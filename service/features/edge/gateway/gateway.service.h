#pragma once

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <unordered_set>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/command/command.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/gateway/gateway.entity.h"
#include "service/features/edge/gateway/gateway.types.h"
#include "service/features/edge/projection/projection.entity.h"
#include "service/features/live/live.service.h"
#include "service/utils/number.h"

namespace service::edge::gateway {

class GatewayService final {
  public:
    static ruvia::task<EnrollmentRecord> loadEnrollment(ruvia::context& c, std::string_view imei, std::string_view pendingNodeId = {}) {
        const auto value = co_await c.redis().get(service::message::edge::authKey(imei));
        if (value) {
            const auto record = EnrollmentRecord::decode(std::string_view(value->data(), value->size()));
            if (record) {
                co_return *record;
            }
        }
        co_return EnrollmentRecord{ pendingNodeId.empty() ? c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next() : std::string(pendingNodeId), "pending" };
    }

    static ruvia::task<void> saveLogResult(ruvia::context& c, std::string_view nodeId, const pb::LogResult& result) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        LogResultRecord record;
        if (!result.SerializeToString(&record.protobufBytes)) {
            co_return;
        }
        const auto id = protocol::uuidText(result.request_id());
        ruvia::redis_set_options options;
        options.expiration_ =
            ruvia::redis_set_expiration::expires_after(std::chrono::seconds(60));
        const auto key = service::message::edge::logResultKey(id);
        co_await c.redis().set(key, record.protobufBytes, std::move(options));
        co_await service::live::publishDirected(c.redis(), key);
        if (result.success()) {
            const auto snapshotKey = service::message::edge::logSnapshotKey(nodeId);
            co_await c.redis().set(snapshotKey, record.protobufBytes);
            co_await service::live::publish(c.redis(), service::message::live::topic::kEdgeLogs, std::array{ std::string(nodeId) });
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
        const auto id = protocol::uuidText(result.request_id());
        ruvia::redis_set_options options;
        options.expiration_ =
            ruvia::redis_set_expiration::expires_after(std::chrono::seconds(60));
        const auto key = service::message::edge::logLevelResultKey(id);
        co_await c.redis().set(key, record.protobufBytes, std::move(options));
        co_await service::live::publishDirected(c.redis(), key);
    }

    template <typename Context>
    static ruvia::task<std::int64_t> databaseTimeMilliseconds(Context& context) {
        ruvia::db_query sample(context.pool());
        const auto epochMilliseconds = sample.cast(
            sample.call("floor", { sample.binary(sample.extract(ruvia::db_date_part::epoch, sample.call("clock_timestamp")), ruvia::db_binary_operator::multiply, sample.value(std::int64_t{ 1000 })) }),
            ruvia::db_data_type::big_int
        );
        sample.select(sample.cast(epochMilliseconds, ruvia::db_data_type::text));
        const auto rows = co_await context.db().query(sample);
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
            claim.call("floor", { claim.binary(claim.extract(ruvia::db_date_part::epoch, claim.column(columnName<Attempt, "deadline">(), "a")), Binary::multiply, claim.value(1000)) }),
            ruvia::db_data_type::big_int
        );
        claim.update(Attempt::table_name(), "a")
            .set(columnName<Attempt, "sent_at">(), claim.call("now"))
            .update_from(Operation::table_name(), "o")
            .and_where(claim.binary(claim.column(columnName<Attempt, "operation_id">(), "a"), Binary::equal, claim.cast(claim.value(operationId), ruvia::db_data_type::uuid)))
            .and_where(claim.binary(claim.column(columnName<Operation, "id">(), "o"), Binary::equal, claim.column(columnName<Attempt, "operation_id">(), "a")))
            .and_where(claim.binary(claim.column(columnName<Attempt, "node_id">(), "a"), Binary::equal, claim.value(nodeId)))
            .and_where(claim.unary(ruvia::db_unary_operator::is_null, claim.column(columnName<Attempt, "sent_at">(), "a")))
            .and_where(claim.binary(claim.column(columnName<Attempt, "deadline">(), "a"), Binary::greater, claim.call("now")))
            .and_where(claim.binary(claim.column(columnName<Operation, "status">(), "o"), Binary::in, claim.list({ claim.value("DISPATCHING"), claim.value("AWAITING_RESULT") })))
            .returning({ claim.cast(claim.column(columnName<Attempt, "operation_id">(), "a"), ruvia::db_data_type::text), deadlineMs });
        const auto claimed = co_await context.db().query(claim);
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
        firmware.select({ firmware.column(columnName<Firmware, "storage_path">(), "firmware"), firmware.column(columnName<Firmware, "size_bytes">(), "firmware") })
            .from(Task::table_name(), "task")
            .join(ruvia::db_join_type::inner, Firmware::table_name(), firmware.binary(firmware.cast(firmware.column(columnName<Firmware, "id">(), "firmware"), ruvia::db_data_type::text), Binary::equal, firmware.binary(firmware.column(columnName<Task, "request">(), "task"), Binary::json_get_text, firmware.value("firmware_id"))), "firmware")
            .and_where(firmware.binary(firmware.column(columnName<Task, "id">(), "task"), Binary::equal, firmware.cast(firmware.value(requestId), ruvia::db_data_type::uuid)))
            .and_where(firmware.binary(firmware.column(columnName<Task, "node_id">(), "task"), Binary::equal, firmware.cast(firmware.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(firmware.binary(firmware.column(columnName<Task, "task_type">(), "task"), Binary::equal, firmware.value("firmware")))
            .limit(1);
        const auto rows = co_await context.db().query(firmware);
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

    // Same transaction lock as modules/edge_node firmware upload; try-lock so that
    // exactly one Worker or process performs a due cleanup.
    static ruvia::db_query firmwareCleanupClaimQuery() {
        ruvia::db_query lock;
        lock.select(lock.call("pg_try_advisory_xact_lock", { lock.cast(lock.value(service::message::lock::kFirmwareStorageClass), ruvia::db_data_type::integer), lock.cast(lock.value(service::message::lock::kFirmwareStorageObject), ruvia::db_data_type::integer) }));
        return lock;
    }

    // Records older than the retention with no firmware task that is still running
    // or finished within the retention. Columns: id::text, storage_path.
    static ruvia::db_query expiredFirmwareQuery() {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        using Task = service::edge::persistence::EdgeTaskEntity;
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        ruvia::db_query active;
        const auto cutoff = active.binary(active.call("now"), Op::subtract, active.cast(active.value(kRetentionInterval), Type::interval));
        active.select(active.value(1)).from(Task::table_name(), "task").where(active.binary(active.column(columnName<Task, "task_type">(), "task"), Op::equal, active.value("firmware"))).and_where(active.binary(active.binary(active.column(columnName<Task, "request">(), "task"), Op::json_get_text, active.value("firmware_id")), Op::equal, active.cast(active.column(columnName<Firmware, "id">(), "firmware"), Type::text))).and_where(active.binary(active.binary(active.column(columnName<Task, "status">(), "task"), Op::not_in, active.list({ active.value("succeeded"), active.value("failed") })), Op::or_value, active.binary(active.coalesce({ active.column(columnName<Task, "completed_at">(), "task"), active.column(columnName<Task, "updated_at">(), "task"), active.column(columnName<Task, "created_at">(), "task") }), Op::greater_equal, cutoff)));
        ruvia::db_query candidates;
        candidates.select({ candidates.cast(candidates.column(columnName<Firmware, "id">(), "firmware"), Type::text), candidates.column(columnName<Firmware, "storage_path">(), "firmware") })
            .from(Firmware::table_name(), "firmware")
            .where(candidates.binary(candidates.column(columnName<Firmware, "created_at">(), "firmware"), Op::less, candidates.binary(candidates.call("now"), Op::subtract, candidates.cast(candidates.value(kRetentionInterval), Type::interval))))
            .and_where(candidates.unary(ruvia::db_unary_operator::not_value, candidates.exists(active)))
            .order_by(candidates.column(columnName<Firmware, "created_at">(), "firmware"));
        return candidates;
    }

    static ruvia::db_query firmwareRemovalQuery(std::string_view id) {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        ruvia::db_query removal;
        removal.delete_from(service::edge::persistence::EdgeFirmwareEntity::table_name())
            .where(removal.binary(removal.column(columnName<Firmware, "id">()), ruvia::db_binary_operator::equal, removal.cast(removal.value(id), ruvia::db_data_type::uuid)));
        return removal;
    }

    static ruvia::db_query firmwareReferencesQuery() {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        ruvia::db_query references;
        references.select(references.column(columnName<Firmware, "storage_path">())).from(Firmware::table_name());
        return references;
    }

    // One row per firmware record: storage_path, the number of its firmware tasks
    // that are not succeeded/failed, and the milliseconds (ceil, relative to the
    // database clock) until it becomes collectable: GREATEST(created_at, latest
    // COALESCE(completed_at, updated_at, created_at) of its tasks) + retention - now().
    static ruvia::db_query firmwareCleanupScheduleQuery() {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        using Task = service::edge::persistence::EdgeTaskEntity;
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        ruvia::db_query schedule;
        const auto activeTasks = schedule.filter(
            schedule.aggregate("count", { schedule.column(columnName<Task, "id">(), "task") }),
            schedule.binary(schedule.column(columnName<Task, "status">(), "task"), Op::not_in, schedule.list({ schedule.value("succeeded"), schedule.value("failed") }))
        );
        const auto lastUse = schedule.aggregate("max", { schedule.coalesce({ schedule.column(columnName<Task, "completed_at">(), "task"), schedule.column(columnName<Task, "updated_at">(), "task"), schedule.column(columnName<Task, "created_at">(), "task") }) });
        const auto collectableAt = schedule.binary(
            schedule.greatest({ schedule.column(columnName<Firmware, "created_at">(), "firmware"), lastUse }),
            Op::add,
            schedule.cast(schedule.value(kRetentionInterval), Type::interval)
        );
        const auto delayMilliseconds = schedule.cast(
            schedule.call("ceil", { schedule.binary(schedule.extract(ruvia::db_date_part::epoch, schedule.binary(collectableAt, Op::subtract, schedule.call("now"))), Op::multiply, schedule.value(std::int64_t{ 1000 })) }),
            Type::big_int
        );
        schedule.select({ schedule.column(columnName<Firmware, "storage_path">(), "firmware"), activeTasks, delayMilliseconds })
            .from(Firmware::table_name(), "firmware")
            .join(ruvia::db_join_type::left, Task::table_name(), schedule.binary(schedule.binary(schedule.column(columnName<Task, "task_type">(), "task"), Op::equal, schedule.value("firmware")), Op::and_value, schedule.binary(schedule.binary(schedule.column(columnName<Task, "request">(), "task"), Op::json_get_text, schedule.value("firmware_id")), Op::equal, schedule.cast(schedule.column(columnName<Firmware, "id">(), "firmware"), Type::text))), "task")
            .group_by({ schedule.column(columnName<Firmware, "id">(), "firmware"), schedule.column(columnName<Firmware, "storage_path">(), "firmware"), schedule.column(columnName<Firmware, "created_at">(), "firmware") });
        return schedule;
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
        const auto candidates = expiredFirmwareQuery();
        const auto rows = co_await transaction.query(candidates);
        std::vector<std::filesystem::path> removedPaths;
        for (const auto& row : rows) {
            if (removedPaths.size() == kFirmwareCleanupBatch) {
                break;
            }
            const auto path = storedFirmwarePath(row[1].value());
            if (!managedFirmwareFile(path, directory)) {
                continue;
            }
            const auto removal = firmwareRemovalQuery(row[0].value().value_or(std::string_view{}));
            (void)co_await transaction.execute(removal);
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
        const auto references = firmwareReferencesQuery();
        const auto referenced = co_await orphanTransaction.query(references);
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
        const auto schedule = firmwareCleanupScheduleQuery();
        const auto rows = co_await context.db().query(schedule);
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

  private:
    static constexpr std::string_view kRetentionInterval{ "1 hour" };

    template <typename Entity, ruvia::fixed_string Name>
    static std::string_view columnName() {
        return std::tuple_element_t<Entity::template column_index<Name>(), typename Entity::columns_type>::name.view();
    }

    template <typename Transaction>
    static ruvia::task<bool> claimFirmwareCleanup(Transaction& transaction) {
        const auto claim = firmwareCleanupClaimQuery();
        const auto rows = co_await transaction.query(claim);
        co_return !rows.empty() && rows.front()[0].value().value_or(std::string_view{}) == "t";
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
