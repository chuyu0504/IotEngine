#pragma once

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <openssl/rand.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/command/command.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/edge.service.h"
#include "service/features/edge/gateway/gateway.entity.h"
#include "service/features/edge/gateway/gateway.types.h"
#include "service/features/edge/projection/projection.entity.h"
#include "service/features/edge/serial_debug/serial_debug.service.h"
#include "service/features/edge/session/session.service.h"
#include "service/features/edge/terminal/terminal.service.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"
#include "service/utils/uuid.h"

namespace service::edge::gateway {

inline constexpr std::string_view kSaveLogResultScript = R"lua(
redis.call('SET', KEYS[1], ARGV[1], 'EX', 60)
if ARGV[2] == '1' then
  redis.call('SET', KEYS[2], ARGV[1])
  redis.call('PUBLISH', KEYS[3], ARGV[3])
end
return redis.call('PUBLISH', KEYS[1], '')
)lua";

inline constexpr std::string_view kSaveLogLevelResultScript = R"lua(
redis.call('SET', KEYS[1], ARGV[1], 'EX', 60)
return redis.call('PUBLISH', KEYS[1], '')
)lua";

class GatewayService final {
  public:
    static std::uint64_t newSessionEpoch() {
        std::uint64_t value{};
        if (RAND_bytes(reinterpret_cast<unsigned char*>(&value), static_cast<int>(sizeof(value))) != 1) {
            throw std::runtime_error("edge session epoch generation failed");
        }
        return value == 0 ? 1 : value;
    }

    // The driver owns this scope on the accepting Worker. This resource only
    // registers its business delivery target; it owns no socket or running task.
    class ConnectionRegistration final {
      public:
        ConnectionRegistration(
            ruvia::context& context,
            const ConnectionIdentity& identity,
            std::function<void()> wake,
            std::function<void(std::string_view)> failure
        ) : dispatcher_(context.worker_state<SessionDispatcher>()),
            nodeId_(identity.nodeId), epoch_(identity.epoch) {
            dispatcher_.registerSession(std::string(nodeId_), epoch_, std::move(wake), std::move(failure));
        }

        ConnectionRegistration(const ConnectionRegistration&) = delete;
        ConnectionRegistration& operator=(const ConnectionRegistration&) = delete;

        ~ConnectionRegistration() { dispatcher_.unregisterSession(nodeId_, epoch_); }

      private:
        SessionDispatcher& dispatcher_;
        std::string_view nodeId_;
        std::uint64_t epoch_;
    };

    // The hello is durably handed to this Worker's projector before enrollment
    // is consulted, including for nodes that are still pending approval.
    static ruvia::task<std::optional<Enrollment>> acceptHello(
        ruvia::context& c,
        std::size_t workerIndex,
        std::string_view wire,
        std::string_view imei
    ) {
        const auto& worker = c.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        if (!co_await projector_stream::publishIngress(
                c.redis(),
                worker.channels,
                workerIndex,
                wire,
                service::message::utcNowMilliseconds(),
                worker.instanceId
            )) {
            co_return std::nullopt;
        }
        co_return co_await loadEnrollment(c, imei);
    }

    static ruvia::task<IngressStatus> openSession(ruvia::context& c, const ConnectionIdentity& identity) {
        const auto& worker = c.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        if (!co_await session_state::claim(
                c.redis(),
                worker.channels,
                identity.nodeId,
                identity.epoch,
                identity.protocolVersion,
                identity.workerIndex,
                worker.instanceId
            )) {
            co_return IngressStatus::SessionReplaced;
        }
        // Metadata must precede all subsequent payloads on the same durable
        // Worker-isolated stream so reconnects rebuild the local projection.
        if (!co_await projector_stream::publishMetadata(c.redis(), worker.channels, identity.workerIndex, identity.nodeId, worker.instanceId)) {
            co_await closeSession(c, identity);
            co_return IngressStatus::WorkerLeaseLost;
        }
        co_return IngressStatus::Accepted;
    }

    static ruvia::task<bool> renewSession(ruvia::context& c, const ConnectionIdentity& identity) {
        const auto& worker = c.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        co_return co_await session_state::refresh(
            c.redis(),
            worker.channels,
            identity.nodeId,
            identity.epoch,
            identity.protocolVersion,
            identity.workerIndex,
            worker.instanceId
        );
    }

    static ruvia::task<void> closeSession(ruvia::context& c, const ConnectionIdentity& identity) {
        const auto& worker = c.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        (void)co_await session_state::release(
            c.redis(),
            worker.channels,
            identity.nodeId,
            identity.epoch,
            identity.protocolVersion,
            identity.workerIndex,
            worker.instanceId
        );
    }

    static ruvia::task<IngressStatus> acceptIngress(
        ruvia::context& c,
        const ConnectionIdentity& identity,
        const pb::Envelope& envelope,
        std::string_view wire,
        bool freshTelemetry
    ) {
        const auto& worker = c.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        if (!co_await renewSession(c, identity)) {
            co_return IngressStatus::SessionReplaced;
        }
        if (freshTelemetry && projectedPayload(envelope) &&
            !co_await projector_stream::publishIngress(
                c.redis(),
                c.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                identity.workerIndex,
                wire,
                service::message::utcNowMilliseconds(),
                worker.instanceId
            )) {
            co_return IngressStatus::WorkerLeaseLost;
        }
        co_return IngressStatus::Accepted;
    }

    // Transport acknowledgements are produced only after the corresponding
    // business operation has accepted the payload.
    static ruvia::task<void> acceptPayload(
        ruvia::context& c,
        const ConnectionIdentity& identity,
        const pb::Envelope& input,
        bool capabilitySeen,
        std::uint64_t configSentAtMs
    ) {
        const terminal_state::ConnectionIdentity terminalIdentity{
            identity.nodeId,
            identity.epoch,
            identity.protocolVersion,
            identity.workerIndex
        };
        switch (input.payload_case()) {
            case pb::Envelope::kHeartbeat: {
                constexpr std::uint64_t retryIntervalMs = 30000;
                const auto now = service::message::utcNowMilliseconds();
                if (capabilitySeen && (configSentAtMs == 0 || now - configSentAtMs >= retryIntervalMs)) {
                    (void)co_await ConfigService::requeueIfStale(
                        c,
                        identity.nodeId,
                        input.heartbeat().active_config_version()
                    );
                }
                break;
            }
            case pb::Envelope::kRawPacket:
                if (input.raw_packet().packet_id().size() == 16) {
                    co_await ConfigService::storeDebugPacket(c, identity.nodeId, input.raw_packet());
                }
                break;
            case pb::Envelope::kSerialDebugEvent:
                co_await serial_debug::Service::saveEvent(
                    c,
                    identity.nodeId,
                    session_state::value(identity.epoch, identity.protocolVersion, identity.workerIndex, c.worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId),
                    input.serial_debug_event()
                );
                break;
            case pb::Envelope::kTerminalData:
                co_await terminal_state::TerminalService::saveTerminalData(c, terminalIdentity, input.terminal_data());
                break;
            case pb::Envelope::kTerminalDataAck:
                co_await terminal_state::TerminalService::saveTerminalDataAck(c, terminalIdentity, input.terminal_data_ack());
                break;
            case pb::Envelope::kTerminalOpened:
                co_await terminal_state::TerminalService::saveTerminalOpened(c, terminalIdentity, input.terminal_opened());
                break;
            case pb::Envelope::kTerminalClose:
                co_await terminal_state::TerminalService::saveTerminalClose(c, terminalIdentity, input.terminal_close());
                break;
            case pb::Envelope::kLogResult:
                co_await saveLogResult(c, identity.nodeId, input.log_result());
                break;
            case pb::Envelope::kLogLevelResult:
                co_await saveLogLevelResult(c, input.log_level_result());
                break;
            default:
                break;
        }
    }

    static bool queuedEnvelopeIsCurrent(
        const pb::Envelope& envelope,
        const ConnectionIdentity& identity,
        std::uint64_t now
    ) {
        return !envelope.has_serial_debug_request() ||
            (envelope.session_epoch() == identity.epoch &&
             envelope.protocol_version() == identity.protocolVersion &&
             envelope.created_at_ms() <= now && now - envelope.created_at_ms() <= 15000);
    }

    static EgressQueues egressQueues(std::string_view nodeId) {
        return {
            service::message::edge::configKey(nodeId),
            service::message::terminal::terminalInputKey(nodeId),
            service::message::serial_debug::inputKey(nodeId),
            service::message::edge::commandQueueKey(nodeId),
            service::message::edge::egressKey(nodeId),
        };
    }

    // One pop per call preserves the bounded drain's treatment of invalid and
    // stale entries. Device commands claim their persisted attempt exactly once.
    static ruvia::task<std::optional<QueuedEnvelope>> takeQueuedEnvelope(
        ruvia::context& c,
        const ConnectionIdentity& identity,
        std::string_view key,
        bool supportsCommandStartBefore
    ) {
        auto wire = co_await c.redis().lpop(key);
        if (!wire) {
            co_return std::nullopt;
        }
        QueuedEnvelope result{ std::move(*wire) };
        if (!protocol::decode(result.wire, result.envelope) ||
            !queuedEnvelopeIsCurrent(result.envelope, identity, service::message::utcNowMilliseconds())) {
            co_return result;
        }
        if (result.envelope.has_command_request()) {
            const auto id = service::common::uuidText(result.envelope.command_request().command_id());
            const auto deadlineMs = co_await claimCommand(c, id, identity.nodeId);
            if (!deadlineMs) {
                co_return result;
            }
            applyCommandDeadline(*result.envelope.mutable_command_request(), supportsCommandStartBefore, deadlineMs);
        }
        result.deliver = true;
        co_return result;
    }

    static ruvia::task<void> returnUndelivered(
        ruvia::context& c,
        std::string_view key,
        const QueuedEnvelope& item
    ) {
        // Physical commands cannot be replayed after claiming transmission;
        // serial requests belong to a short-lived session. Other tasks retain
        // their existing reconnect/retry contract and original unbound bytes.
        if (!item.envelope.has_command_request() && !item.envelope.has_serial_debug_request()) {
            (void)co_await c.redis().lpush(key, item.wire);
        }
    }

    static ruvia::task<pb::FirmwareChunk> firmwareChunk(
        ruvia::context& c,
        std::string_view nodeId,
        FirmwareTransfer& transfer,
        const pb::FirmwareChunkRequest& request
    ) {
        const bool sameRequest = transfer.loaded &&
            std::memcmp(transfer.requestId.data(), request.request_id().data(), 16) == 0;
        if (!sameRequest) {
            transfer.loaded = false;
            transfer.path.clear();
            transfer.size = 0;
            const auto requestId = service::common::uuidText(request.request_id());
            if (const auto source = co_await loadFirmwareSource(c, requestId, nodeId)) {
                std::memcpy(transfer.requestId.data(), request.request_id().data(), 16);
                transfer.path = std::filesystem::path(source->storagePath);
                transfer.size = source->sizeBytes;
                transfer.loaded = true;
            }
        }
        pb::FirmwareChunk chunk;
        chunk.set_request_id(request.request_id());
        chunk.set_offset(request.offset());
        if (!transfer.loaded) {
            chunk.set_error("firmware transfer source is unavailable");
            co_return chunk;
        }
        auto value = readFirmwareChunk(transfer.path, transfer.size, request.offset());
        if (!value.error.empty()) {
            chunk.set_error(value.error);
        } else {
            chunk.set_data(std::move(value.data));
            chunk.set_eof(value.eof);
        }
        co_return chunk;
    }

    static ruvia::task<Enrollment> loadEnrollment(ruvia::context& c, std::string_view imei, std::string_view pendingNodeId = {}) {
        const auto value = co_await c.redis().get(service::message::edge::authKey(imei));
        if (value) {
            auto record = EnrollmentRecord::decode(std::string_view(value->data(), value->size()));
            if (record) {
                co_return Enrollment{ std::move(record->nodeId), record->status == "approved" };
            }
        }
        co_return Enrollment{ pendingNodeId.empty() ? service::utils::nextUuid(c.worker_state<service::common::UuidV7State>()) : std::string(pendingNodeId), false };
    }

    static ruvia::task<void> saveLogResult(ruvia::context& c, std::string_view nodeId, const pb::LogResult& result) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        LogResultRecord record;
        if (!result.SerializeToString(&record.protobufBytes)) {
            co_return;
        }
        const auto id = service::common::uuidText(result.request_id());
        const auto key = service::message::edge::logResultKey(id);
        const auto snapshot = service::message::edge::logSnapshotKey(nodeId);
        const auto channel = service::message::live::channel(c.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, service::message::live::topic::kEdgeLogs);
        const std::string_view keys[]{ key, snapshot, channel };
        const std::string_view args[]{ record.protobufBytes, result.success() ? "1" : "0", nodeId };
        const auto saved = co_await c.redis().eval(kSaveLogResultScript, keys, args);
        if (saved.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("persist edge log result", saved);
        }
    }

    static ruvia::task<void> saveLogLevelResult(ruvia::context& c, const pb::LogLevelResult& result) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        LogResultRecord record;
        if (!result.SerializeToString(&record.protobufBytes)) {
            co_return;
        }
        const auto id = service::common::uuidText(result.request_id());
        const auto key = service::message::edge::logLevelResultKey(id);
        const std::string_view keys[]{ key };
        const std::string_view args[]{ record.protobufBytes };
        const auto saved = co_await c.redis().eval(kSaveLogLevelResultScript, keys, args);
        if (saved.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("persist edge log level result", saved);
        }
    }

    template <typename Context>
    static ruvia::task<std::int64_t> databaseTimeMilliseconds(Context& context) {
        const auto rows = co_await context.db().query("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint::text");
        if (rows.empty()) {
            throw std::runtime_error("PostgreSQL clock sample is unavailable");
        }
        const auto sampledValue = rows.front()[0].value();
        if (!sampledValue) {
            throw std::runtime_error("PostgreSQL clock sample is unavailable");
        }
        const auto value = *sampledValue;
        std::int64_t milliseconds{};
        const auto parsed = std::from_chars(value.data(), value.data() + value.size(), milliseconds);
        if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
            throw std::runtime_error("PostgreSQL clock sample is invalid");
        }
        co_return milliseconds;
    }

    template <typename Context>
    static ruvia::task<std::optional<std::int64_t>> claimCommand(
        Context& context,
        std::string_view operationId,
        std::string_view nodeId
    ) {
        using Attempt = service::command::persistence::CommandAttemptEntity;
        using Operation = service::command::persistence::CommandOperationEntity;
        ruvia::db_query claim(context.pool());
        using Binary = ruvia::db_binary_operator;
        const auto deadlineMs = claim.cast(
            claim.call("floor", { claim.binary(claim.extract(ruvia::db_date_part::epoch, claim.column(Attempt::column_name<"deadline">(), "a")), Binary::multiply, claim.value(1000)) }),
            ruvia::db_data_type::big_int
        );
        auto claimBuilder = context.db().template repository<Attempt>().update_builder("a");
        claimBuilder.set(Attempt::column_name<"sent_at">(), claim.call("now")).template update_from<Operation>("o").and_where(claim.binary(claim.column(Attempt::column_name<"operation_id">(), "a"), Binary::equal, claim.cast(claim.value(operationId), ruvia::db_data_type::uuid))).and_where(claim.binary(claim.column(Operation::column_name<"id">(), "o"), Binary::equal, claim.column(Attempt::column_name<"operation_id">(), "a"))).and_where(claim.binary(claim.column(Attempt::column_name<"node_id">(), "a"), Binary::equal, claim.value(nodeId))).and_where(claim.unary(ruvia::db_unary_operator::is_null, claim.column(Attempt::column_name<"sent_at">(), "a"))).and_where(claim.binary(claim.column(Attempt::column_name<"deadline">(), "a"), Binary::greater, claim.call("now"))).and_where(claim.binary(claim.column(Operation::column_name<"status">(), "o"), Binary::in, claim.list({ claim.value("DISPATCHING"), claim.value("AWAITING_RESULT") }))).returning({ { "operation_id", claim.cast(claim.column(Attempt::column_name<"operation_id">(), "a"), ruvia::db_data_type::text) }, { "value1", deadlineMs } });
        const auto claimed = co_await claimBuilder.get_raw_many();
        if (claimed.empty()) {
            co_return std::nullopt;
        }
        // sent_at is already committed. A bad returned value must never reopen the attempt;
        // the command deadline sweep will conservatively transition it to UNKNOWN.
        const auto value = claimed.front()[1].value();
        if (!value) {
            throw std::runtime_error("claimed command attempt has no persisted deadline");
        }
        std::int64_t milliseconds{};
        const auto parsed = std::from_chars(value->data(), value->data() + value->size(), milliseconds);
        if (parsed.ec != std::errc{} || parsed.ptr != value->data() + value->size()) {
            throw std::runtime_error("claimed command attempt deadline is invalid");
        }
        co_return milliseconds;
    }

    static void applyCommandDeadline(pb::CommandRequest& request, bool supportsCommandStartBefore, std::optional<std::int64_t> deadlineMs) {
        if (!supportsCommandStartBefore) {
            if (request.has_start_before_ms()) {
                throw std::invalid_argument("command start deadline was not negotiated");
            }
            return;
        }
        if (request.has_start_before_ms()) {
            throw std::invalid_argument("command start deadline must come from its persisted attempt");
        }
        if (!deadlineMs) {
            throw std::invalid_argument("negotiated command start deadline is unavailable");
        }
        request.set_start_before_ms(*deadlineMs);
    }

    template <typename Context>
    static ruvia::task<std::optional<FirmwareSource>> loadFirmwareSource(
        Context& context,
        std::string_view requestId,
        std::string_view nodeId
    ) {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        using Task = service::edge::persistence::EdgeTaskEntity;
        using Binary = ruvia::db_binary_operator;
        ruvia::db_query firmware(context.pool());
        auto firmwareBuilder = context.db().template repository<Task>().query_builder("task");
        firmwareBuilder.select({ { "storage_path", firmware.column(Firmware::column_name<"storage_path">(), "firmware") }, { "size_bytes", firmware.column(Firmware::column_name<"size_bytes">(), "firmware") } }).template join<Firmware>(ruvia::db_join_type::inner, "firmware", firmware.binary(firmware.cast(firmware.column(Firmware::column_name<"id">(), "firmware"), ruvia::db_data_type::text), Binary::equal, firmware.binary(firmware.column(Task::column_name<"request">(), "task"), Binary::json_get_text, firmware.value("firmware_id")))).and_where(firmware.binary(firmware.column(Task::column_name<"id">(), "task"), Binary::equal, firmware.cast(firmware.value(requestId), ruvia::db_data_type::uuid))).and_where(firmware.binary(firmware.column(Task::column_name<"node_id">(), "task"), Binary::equal, firmware.cast(firmware.value(nodeId), ruvia::db_data_type::uuid))).and_where(firmware.binary(firmware.column(Task::column_name<"task_type">(), "task"), Binary::equal, firmware.value("firmware"))).limit(1);
        const auto rows = co_await firmwareBuilder.get_raw_many();
        if (rows.empty()) {
            co_return std::nullopt;
        }

        FirmwareSource source;
        source.storagePath =
            std::string(rows.front()[0].value().value_or(std::string_view{}));
        const auto sizeText = rows.front()[1].value().value_or(std::string_view{});
        const auto parsed = std::from_chars(sizeText.data(), sizeText.data() + sizeText.size(), source.sizeBytes);
        if (source.storagePath.empty() || source.sizeBytes == 0 || parsed.ec != std::errc{} ||
            parsed.ptr != sizeText.data() + sizeText.size()) {
            co_return std::nullopt;
        }
        co_return source;
    }

    // Reads one bounded chunk of a stored firmware image for the node WebSocket.
    static FirmwareChunk readFirmwareChunk(const std::filesystem::path& path, std::uint64_t expectedSize, std::uint64_t offset) {
        FirmwareChunk result;
        std::error_code error;
        const auto actualSize = std::filesystem::file_size(path, error);
        if (error || actualSize != expectedSize) {
            result.error = "firmware storage size mismatch";
            return result;
        }
        if (offset >= expectedSize) {
            result.error = "firmware chunk offset is outside the image";
            return result;
        }
        const auto remaining = expectedSize - offset;
        const auto size = static_cast<std::size_t>(
            std::min<std::uint64_t>(remaining, kFirmwareChunkSize)
        );
        std::ifstream input(path, std::ios::binary);
        if (!input) {
            result.error = "firmware storage cannot be opened";
            return result;
        }
        input.seekg(static_cast<std::streamoff>(offset));
        result.data.resize(size);
        input.read(result.data.data(), static_cast<std::streamsize>(size));
        if (input.gcount() != static_cast<std::streamsize>(size)) {
            result.data.clear();
            result.error = "firmware storage read failed";
            return result;
        }
        result.eof = offset + size == expectedSize;
        return result;
    }

    // Cleanup only touches files this service names: <uuid>.bin directly inside
    // the firmware directory. Both paths must already be absolute and normalized.
    static bool managedFirmwareFile(const std::filesystem::path& path, const std::filesystem::path& directory) {
        return path.parent_path() == directory && path.extension() == ".bin" &&
            service::common::isUuid(path.stem().string());
    }

    // Records older than the retention with no firmware task that is still running
    // or finished within the retention. Columns: id::text, storage_path.
    template <typename Database>
    static auto expiredFirmwareQuery(Database&& database) {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        using Task = service::edge::persistence::EdgeTaskEntity;
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        ruvia::db_query active;
        const auto cutoff = active.binary(active.call("now"), Op::subtract, active.cast(active.value(kRetentionInterval), Type::interval));
        active.select(active.value(1)).from(Task::table_name(), "task").where(active.binary(active.column(Task::column_name<"task_type">(), "task"), Op::equal, active.value("firmware"))).and_where(active.binary(active.binary(active.column(Task::column_name<"request">(), "task"), Op::json_get_text, active.value("firmware_id")), Op::equal, active.cast(active.column(Firmware::column_name<"id">(), "firmware"), Type::text))).and_where(active.binary(active.binary(active.column(Task::column_name<"status">(), "task"), Op::not_in, active.list({ active.value("succeeded"), active.value("failed") })), Op::or_value, active.binary(active.coalesce({ active.column(Task::column_name<"completed_at">(), "task"), active.column(Task::column_name<"updated_at">(), "task"), active.column(Task::column_name<"created_at">(), "task") }), Op::greater_equal, cutoff)));
        ruvia::db_query candidates;
        auto candidatesBuilder = database.template repository<Firmware>().query_builder("firmware");
        candidatesBuilder.select({ { "id", candidates.cast(candidates.column(Firmware::column_name<"id">(), "firmware"), Type::text) }, { "storage_path", candidates.column(Firmware::column_name<"storage_path">(), "firmware") } }).where(candidates.binary(candidates.column(Firmware::column_name<"created_at">(), "firmware"), Op::less, candidates.binary(candidates.call("now"), Op::subtract, candidates.cast(candidates.value(kRetentionInterval), Type::interval)))).and_where(candidates.unary(ruvia::db_unary_operator::not_value, candidates.exists(active))).order_by(candidates.column(Firmware::column_name<"created_at">(), "firmware"));
        return candidatesBuilder;
    }

    template <typename Database>
    static auto firmwareRemovalQuery(Database&& database, std::string_view id) {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        ruvia::db_query removal;
        auto removalBuilder = database.template repository<service::edge::persistence::EdgeFirmwareEntity>().delete_builder();
        removalBuilder.where(removal.binary(removal.column(Firmware::column_name<"id">()), ruvia::db_binary_operator::equal, removal.cast(removal.value(id), ruvia::db_data_type::uuid)));
        return removalBuilder;
    }

    template <typename Database>
    static auto firmwareReferencesQuery(Database&& database) {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        ruvia::db_query references;
        auto referencesBuilder = database.template repository<Firmware>().query_builder();
        referencesBuilder.select({ { "storage_path", references.column(Firmware::column_name<"storage_path">()) } });
        return referencesBuilder;
    }

    // One row per firmware record: storage_path, the number of its firmware tasks
    // that are not succeeded/failed, and the milliseconds (ceil, relative to the
    // database clock) until it becomes collectable: GREATEST(created_at, latest
    // COALESCE(completed_at, updated_at, created_at) of its tasks) + retention - now().
    template <typename Database>
    static auto firmwareCleanupScheduleQuery(Database&& database) {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        using Task = service::edge::persistence::EdgeTaskEntity;
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        ruvia::db_query schedule;
        const auto activeTasks = schedule.filter(
            schedule.aggregate("count", { schedule.column(Task::column_name<"id">(), "task") }),
            schedule.binary(schedule.column(Task::column_name<"status">(), "task"), Op::not_in, schedule.list({ schedule.value("succeeded"), schedule.value("failed") }))
        );
        const auto lastUse = schedule.aggregate("max", { schedule.coalesce({ schedule.column(Task::column_name<"completed_at">(), "task"), schedule.column(Task::column_name<"updated_at">(), "task"), schedule.column(Task::column_name<"created_at">(), "task") }) });
        const auto collectableAt = schedule.binary(
            schedule.greatest({ schedule.column(Firmware::column_name<"created_at">(), "firmware"), lastUse }),
            Op::add,
            schedule.cast(schedule.value(kRetentionInterval), Type::interval)
        );
        const auto delayMilliseconds = schedule.cast(
            schedule.call("ceil", { schedule.binary(schedule.extract(ruvia::db_date_part::epoch, schedule.binary(collectableAt, Op::subtract, schedule.call("now"))), Op::multiply, schedule.value(std::int64_t{ 1000 })) }),
            Type::big_int
        );
        auto scheduleBuilder = database.template repository<Firmware>().query_builder("firmware");
        scheduleBuilder.select({ { "storage_path", schedule.column(Firmware::column_name<"storage_path">(), "firmware") }, { "value1", activeTasks }, { "value2", delayMilliseconds } }).template join<Task>(ruvia::db_join_type::left, "task", schedule.binary(schedule.binary(schedule.column(Task::column_name<"task_type">(), "task"), Op::equal, schedule.value("firmware")), Op::and_value, schedule.binary(schedule.binary(schedule.column(Task::column_name<"request">(), "task"), Op::json_get_text, schedule.value("firmware_id")), Op::equal, schedule.cast(schedule.column(Firmware::column_name<"id">(), "firmware"), Type::text)))).group_by({ schedule.column(Firmware::column_name<"id">(), "firmware"), schedule.column(Firmware::column_name<"storage_path">(), "firmware"), schedule.column(Firmware::column_name<"created_at">(), "firmware") });
        return scheduleBuilder;
    }

    // Deletes up to kFirmwareCleanupBatch expired records (commit first, then their
    // files), then up to kFirmwareCleanupBatch unreferenced owned files older than
    // the retention. Each phase runs only while its transaction holds the claim.
    template <typename Context>
    static ruvia::task<FirmwareCleanupResult> cleanupFirmwares(Context& context, const std::filesystem::path& directory) {
        FirmwareCleanupResult result;
        auto transaction = co_await context.db().begin_transaction();
        if (!co_await claimFirmwareCleanup(transaction)) {
            co_return result;
        }
        result.claimed = true;
        const auto candidates = expiredFirmwareQuery(transaction);
        const auto rows = co_await candidates.get_raw_many();
        std::vector<std::filesystem::path> removedPaths;
        for (const auto& row : rows) {
            if (removedPaths.size() == kFirmwareCleanupBatch) {
                break;
            }
            const auto path = storedFirmwarePath(row[1].value());
            if (!managedFirmwareFile(path, directory)) {
                continue;
            }
            const auto removal = firmwareRemovalQuery(transaction, row[0].value().value_or(std::string_view{}));
            (void)co_await removal.execute();
            removedPaths.push_back(path);
        }
        // 先提交删除记录；提交结果不确定时不删除文件，下一次按孤立文件回收。
        co_await transaction.commit();
        result.removedRecords = removedPaths.size();
        for (const auto& path : removedPaths) {
            std::error_code error;
            const auto status = std::filesystem::symlink_status(path, error);
            if (error == std::errc::no_such_file_or_directory) {
                continue;
            }
            if (error) {
                throw std::filesystem::filesystem_error("inspect expired firmware", path, error);
            }
            if (std::filesystem::is_regular_file(status)) {
                std::filesystem::remove(path);
            }
        }
        if (!firmwareDirectoryExists(directory)) {
            co_return result;
        }
        // 数据库未引用的最终文件来自中断/不确定的上传提交。只回收自有命名、
        // 超过保留期的普通文件；活动 .upload 文件仍由上传恢复锁保护。
        auto orphanTransaction = co_await context.db().begin_transaction();
        if (!co_await claimFirmwareCleanup(orphanTransaction)) {
            co_return result;
        }
        const auto references = firmwareReferencesQuery(orphanTransaction);
        const auto referenced = co_await references.get_raw_many();
        std::unordered_set<std::string> paths;
        for (const auto& row : referenced) {
            paths.insert(storedFirmwarePath(row[0].value()).string());
        }
        const auto oldest = std::filesystem::file_time_type::clock::now() - kFirmwareRetention;
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            const auto path = entry.path().lexically_normal();
            if (!managedFirmwareFile(path, directory) || paths.contains(path.string()) ||
                !std::filesystem::is_regular_file(entry.symlink_status()) || entry.last_write_time() >= oldest) {
                continue;
            }
            std::filesystem::remove(path);
            if (++result.removedFiles == kFirmwareCleanupBatch) {
                break;
            }
        }
        co_await orphanTransaction.commit();
        co_return result;
    }

    // Time until the earliest cleanup this Worker can perform: the earliest
    // collectable owned record without an unfinished task, or the earliest
    // unreferenced owned file in the directory (mtime + retention). Empty when
    // nothing is pending; never negative.
    template <typename Context>
    static ruvia::task<std::optional<std::chrono::milliseconds>> nextFirmwareCleanupDelay(
        Context& context,
        const std::filesystem::path& directory
    ) {
        std::optional<std::chrono::milliseconds> delay;
        const auto consider = [&delay](std::chrono::milliseconds candidate) {
            candidate = std::max(candidate, std::chrono::milliseconds::zero());
            if (!delay || candidate < *delay) {
                delay = candidate;
            }
        };
        const auto schedule = firmwareCleanupScheduleQuery(context.db());
        const auto rows = co_await schedule.get_raw_many();
        std::unordered_set<std::string> referenced;
        for (const auto& row : rows) {
            const auto path = storedFirmwarePath(row[0].value());
            referenced.insert(path.string());
            const auto activeTasks = service::utils::parseInt64(row[1].value());
            if (!activeTasks) {
                throw std::runtime_error("firmware cleanup task count is invalid");
            }
            if (*activeTasks != 0 || !managedFirmwareFile(path, directory)) {
                continue;
            }
            const auto milliseconds = service::utils::parseInt64(row[2].value());
            if (!milliseconds) {
                throw std::runtime_error("firmware cleanup deadline is invalid");
            }
            consider(std::chrono::milliseconds(*milliseconds));
        }
        if (!firmwareDirectoryExists(directory)) {
            co_return delay;
        }
        const auto now = std::filesystem::file_time_type::clock::now();
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            const auto path = entry.path().lexically_normal();
            if (!managedFirmwareFile(path, directory) || referenced.contains(path.string()) ||
                !std::filesystem::is_regular_file(entry.symlink_status())) {
                continue;
            }
            consider(std::chrono::ceil<std::chrono::milliseconds>(entry.last_write_time() + kFirmwareRetention - now));
        }
        co_return delay;
    }

    static ruvia::task<std::string> scheduleFirmwareCleanup(
        ruvia::web_worker_context& context,
        std::string_view operation,
        std::string_view payload,
        ruvia::stop_token
    ) {
        if (operation != service::message::edge::firmware::kScheduleCleanupOperation || !payload.empty()) {
            service::common::fail(10002, "Invalid firmware cleanup request", 400);
        }
        context.worker_state<FirmwareCleanupSignal>().request();
        co_return std::string{};
    }

  private:
    static bool projectedPayload(const pb::Envelope& input) {
        switch (input.payload_case()) {
            case pb::Envelope::kHeartbeat:
            case pb::Envelope::kCapabilityReport:
            case pb::Envelope::kNetworkConfigResult:
            case pb::Envelope::kVpnConfigResult:
            case pb::Envelope::kFirmwareUpdateResult:
            case pb::Envelope::kModemControlResult:
            case pb::Envelope::kPlatformConfigResult:
            case pb::Envelope::kConfigApplied:
            case pb::Envelope::kConfigRejected:
            case pb::Envelope::kTelemetryBatch:
            case pb::Envelope::kCommandResult:
            case pb::Envelope::kDeviceStatusReport:
            case pb::Envelope::kDtuStatus:
                return true;
            default:
                return false;
        }
    }

    static constexpr std::string_view kRetentionInterval{ "1 hour" };

    template <typename Transaction>
    static ruvia::task<bool> claimFirmwareCleanup(Transaction& transaction) {
        co_return co_await transaction.try_advisory_lock(
            service::message::lock::kFirmwareStorageClass,
            service::message::lock::kFirmwareStorageObject
        );
    }

    // Stored paths are resolved like the upload path that wrote them; an
    // unresolvable value is treated as unowned and never deleted.
    static std::filesystem::path storedFirmwarePath(std::optional<std::string_view> value) {
        std::error_code error;
        auto path = std::filesystem::absolute(std::filesystem::path(std::string(value.value_or(std::string_view{}))), error);
        if (error) {
            return {};
        }
        return path.lexically_normal();
    }

    static bool firmwareDirectoryExists(const std::filesystem::path& directory) {
        std::error_code error;
        if (std::filesystem::exists(directory, error)) {
            return true;
        }
        if (error) {
            throw std::filesystem::filesystem_error("inspect firmware directory", directory, error);
        }
        return false;
    }
};

} // namespace service::edge::gateway
