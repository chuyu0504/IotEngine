#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <functional>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <iostream>
#include <limits>
#include <map>
#include <openssl/evp.h>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/web/db/db_predicate.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/redis/redis_handle.h>
#include <ruvia/web/web_worker.h>

#include "service/common/derived_point.h"
#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/configuration/configuration.service.h"
#include "service/features/edge/edge.config.h"
#include "service/features/edge/edge.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/session/session.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/packet_log/packet_log.service.h"
#include "service/utils/uuid.h"

namespace service::edge {

// Worker-local socket targets. Redis consumption is driven by the runtime.
class SessionDispatcher final {
  public:
    using SessionWake = std::function<void()>;
    using SessionFailure = std::function<void(std::string_view)>;

    void activate(std::size_t index) {
        if (running_) {
            throw std::runtime_error("edge dispatcher is already running");
        }
        workerIndex_ = index;
        failed_ = false;
        running_ = true;
    }

    void clear() noexcept {
        running_ = false;
        sessions_.clear();
    }

    [[nodiscard]] std::size_t workerIndex() const {
        if (!workerIndex_) {
            throw std::runtime_error("edge dispatcher is not ready");
        }
        return *workerIndex_;
    }

    void registerSession(std::string nodeId, std::uint64_t epoch, SessionWake wake, SessionFailure failure = {}) {
        if (!running_ || failed_ || !wake) {
            throw std::runtime_error("edge dispatcher is not ready");
        }
        sessions_.insert_or_assign(std::move(nodeId), SessionTarget{ epoch, std::move(wake), std::move(failure) });
    }

    void unregisterSession(std::string_view nodeId, std::uint64_t epoch) noexcept {
        const auto current = sessions_.find(nodeId);
        if (current != sessions_.end() && current->second.epoch == epoch) {
            sessions_.erase(current);
        }
    }

    [[nodiscard]] bool failed() const noexcept { return failed_; }

    void failSessions(std::string_view reason) noexcept {
        failed_ = true;
        for (auto& [nodeId, target] : sessions_) {
            (void)nodeId;
            try {
                if (target.failure) {
                    target.failure(reason);
                }
            } catch (const std::exception& error) {
                std::cerr << "edge session failure notification failed: " << error.what() << '\n';
            } catch (...) {
                std::cerr << "edge session failure notification failed: unknown error\n";
            }
        }
    }

    bool deliver(std::string_view nodeId) {
        const auto current = sessions_.find(nodeId);
        if (current == sessions_.end()) {
            return true;
        }
        try {
            current->second.wake();
            return true;
        } catch (const std::exception& error) {
            std::cerr << "edge dispatch worker " << workerIndex() << " wake failed: " << error.what() << '\n';
        } catch (...) {
            std::cerr << "edge dispatch worker " << workerIndex() << " wake failed: unknown error\n";
        }
        return false;
    }

  private:
    struct SessionTarget final {
        std::uint64_t epoch;
        SessionWake wake;
        SessionFailure failure;
    };

    std::map<std::string, SessionTarget, std::less<>> sessions_;
    std::optional<std::size_t> workerIndex_;
    bool running_ = false;
    bool failed_ = false;
};

namespace config::detail {

inline constexpr std::string_view kReplaceQueueScript = R"lua(
local incoming = tonumber(ARGV[1]) or 0
local current = tonumber(redis.call('GET', KEYS[2]) or '0') or 0
if current > incoming then return 0 end
redis.call('DEL', KEYS[1])
for index = 2, #ARGV do redis.call('RPUSH', KEYS[1], ARGV[index]) end
redis.call('EXPIRE', KEYS[1], 604800)
redis.call('SETEX', KEYS[2], 604800, ARGV[1])
return #ARGV - 1
)lua";

// Mapped physical column `Name` of `Entity`, qualified by the query alias `table`.
template <typename Entity, ruvia::fixed_string Name>
ruvia::db_query::expr_type column(ruvia::db_query& query, std::string_view table = {}) {
    return query.column(Entity::template column_name<Name>(), table);
}

inline ruvia::db_query::expr_type jsonGet(ruvia::db_query& query, ruvia::db_query::expr_type value, std::string_view key) {
    return query.binary(std::move(value), ruvia::db_binary_operator::json_get, query.cast(query.value(key), ruvia::db_data_type::text));
}

inline ruvia::db_query::expr_type jsonText(ruvia::db_query& query, ruvia::db_query::expr_type value, std::string_view key) {
    return query.binary(std::move(value), ruvia::db_binary_operator::json_get_text, query.cast(query.value(key), ruvia::db_data_type::text));
}

inline ruvia::db_query::expr_type jsonText(ruvia::db_query& query, std::string_view column, std::string_view table, std::string_view key) {
    return jsonText(query, query.column(column, table), key);
}

inline ruvia::db_query::expr_type jsonPath(ruvia::db_query& query, std::string_view path) {
    return query.cast(
        query.value(path),
        ruvia::db_type_definition{ .data_type_ = ruvia::db_data_type::text,
                                   .array_ = true }
    );
}

inline ruvia::db_query::expr_type toJsonb(ruvia::db_query& query, ruvia::db_query::expr_type value) {
    return query.call("to_jsonb", { std::move(value) });
}

inline ruvia::db_query::expr_type toJsonbText(ruvia::db_query& query, std::string_view value) {
    return toJsonb(query, query.cast(query.value(value), ruvia::db_data_type::text));
}

inline ruvia::db_query::expr_type textDefault(ruvia::db_query& query, ruvia::db_query::expr_type value, std::string_view fallback) {
    return query.coalesce({ query.null_if(std::move(value), query.value(std::string_view{})), query.cast(query.value(fallback), ruvia::db_data_type::text) });
}

inline ruvia::db_query::expr_type booleanText(ruvia::db_query& query, ruvia::db_query::expr_type value) {
    const auto lower = query.call(
        "lower",
        { query.coalesce({ std::move(value), query.cast(query.value(std::string_view{}), ruvia::db_data_type::text) }) }
    );
    return query.case_when(
        { { query.binary(lower, ruvia::db_binary_operator::equal, query.cast(query.value(std::string_view{ "true" }), ruvia::db_data_type::text)),
            query.cast(query.value(true), ruvia::db_data_type::boolean) },
          { query.binary(lower, ruvia::db_binary_operator::equal, query.cast(query.value(std::string_view{ "t" }), ruvia::db_data_type::text)),
            query.cast(query.value(true), ruvia::db_data_type::boolean) },
          { query.binary(lower, ruvia::db_binary_operator::equal, query.cast(query.value(std::string_view{ "1" }), ruvia::db_data_type::text)),
            query.cast(query.value(true), ruvia::db_data_type::boolean) } },
        query.cast(query.value(false), ruvia::db_data_type::boolean)
    );
}

inline ruvia::db_query::expr_type jsonKey(ruvia::db_query& query, std::string_view value) {
    return query.cast(query.value(value), ruvia::db_data_type::text);
}

inline ruvia::db_query::expr_type configVersion(ruvia::db_query& query, ruvia::db_query::expr_type status, std::string_view key) {
    const auto text = jsonText(query, jsonGet(query, std::move(status), "config"), key);
    const auto valid = query.binary(
        text,
        ruvia::db_binary_operator::regex,
        query.cast(query.value(std::string_view{ "^-?[0-9]{1,18}$" }), ruvia::db_data_type::text)
    );
    return query.coalesce(
        { query.case_when({ { valid, query.cast(text, ruvia::db_data_type::big_int) } }),
          query.value(std::int64_t{ 0 }) }
    );
}

inline ruvia::db_query::expr_type configVersion(ruvia::db_query& query, std::string_view table, std::string_view key) {
    return configVersion(query, column<persistence::EdgeNodeEntity, "status">(query, table), key);
}

inline ruvia::db_query::expr_type configState(ruvia::db_query& query, ruvia::db_query::expr_type status) {
    return query.coalesce(
        { jsonText(query, jsonGet(query, std::move(status), "config"), "state"),
          query.cast(query.value(std::string_view{ "idle" }), ruvia::db_data_type::text) }
    );
}

inline constexpr std::string_view kMissingProtocolMessage = "边缘固件尚未声明支持协议 ";

inline bool needsConfigurationReplay(std::uint64_t desiredRevision, std::uint64_t activeRevision, std::string_view state, std::string_view message) {
    if (desiredRevision == 0 || desiredRevision <= activeRevision) {
        return false;
    }
    if (state != "rejected") {
        return true;
    }
    if (!message.starts_with(kMissingProtocolMessage)) {
        return false;
    }
    const auto protocol = message.substr(kMissingProtocolMessage.size());
    return protocol == "MC" || protocol == "FINS" || protocol == "DLT645";
}

inline ruvia::db_query::expr_type capabilityEnabled(ruvia::db_query& query, std::string_view table) {
    return booleanText(query, jsonText(query, column<persistence::EdgeNodeEntity, "capability">(query, table), "deviceConfig"));
}

inline ruvia::db_query queueSnapshotQuery(std::string_view nodeId) {
    ruvia::db_query next;
    const auto desired = configVersion(next, next.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "desiredVersion");
    const auto active = configVersion(next, next.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "activeVersion");
    const auto nowMilliseconds = next.cast(
        next.binary(next.extract(ruvia::db_date_part::epoch, next.call("clock_timestamp")), ruvia::db_binary_operator::multiply, next.value(std::int64_t{ 1000 })),
        ruvia::db_data_type::big_int
    );
    next.select({
                    next.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()),
                    next.alias(next.greatest({ nowMilliseconds, next.binary(desired, ruvia::db_binary_operator::add, next.value(std::int64_t{ 1 })), next.binary(active, ruvia::db_binary_operator::add, next.value(std::int64_t{ 1 })) }), "revision"),
                })
        .from(service::edge::persistence::EdgeNodeEntity::table_name())
        .where(next.binary(next.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, next.cast(next.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(next.binary(next.column(service::edge::persistence::EdgeNodeEntity::column_name<"enrollment_status">()), ruvia::db_binary_operator::equal, next.value(std::string_view{ "approved" })))
        .and_where(capabilityEnabled(next, {}));

    ruvia::db_query update;
    const auto status = update.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), "node");
    const auto statusWithVersion = update.call(
        "jsonb_set",
        { status, jsonPath(update, "{config,desiredVersion}"), toJsonb(update, update.column("revision", "next")), update.cast(update.value(true), ruvia::db_data_type::boolean) }
    );
    const auto statusWithState = update.call(
        "jsonb_set",
        { statusWithVersion, jsonPath(update, "{config,state}"), toJsonbText(update, "pending"), update.cast(update.value(true), ruvia::db_data_type::boolean) }
    );
    const auto statusWithMessage = update.call(
        "jsonb_set",
        { statusWithState, jsonPath(update, "{config,message}"), toJsonbText(update, ""), update.cast(update.value(true), ruvia::db_data_type::boolean) }
    );
    update.with("next", next)
        .update(service::edge::persistence::EdgeNodeEntity::table_name(), "node")
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), statusWithMessage)
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), update.call("now"))
        .update_from("next")
        .where(update.binary(update.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "node"), ruvia::db_binary_operator::equal, update.column("id", "next")))
        .returning({ update.column("revision", "next") });
    return update;
}

inline ruvia::db_query requeueDesiredQuery(std::string_view nodeId) {
    ruvia::db_query query;
    query.select({ configVersion(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "desiredVersion"), configState(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">())), configVersion(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "activeVersion"), query.coalesce({ jsonText(query, jsonGet(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "config"), "message"), query.value(std::string_view{}) }) })
        .from(service::edge::persistence::EdgeNodeEntity::table_name())
        .where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"enrollment_status">()), ruvia::db_binary_operator::equal, query.value(std::string_view{ "approved" })))
        .and_where(capabilityEnabled(query, {}));
    return query;
}

inline ruvia::db_query requeuePendingQuery(std::string_view nodeId, std::uint64_t revision, std::string_view expectedState, std::string_view expectedMessage) {
    ruvia::db_query query;
    const auto status = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
    const auto statusWithState = query.call(
        "jsonb_set",
        { status, jsonPath(query, "{config,state}"), toJsonbText(query, "pending"), query.cast(query.value(true), ruvia::db_data_type::boolean) }
    );
    query.update(service::edge::persistence::EdgeNodeEntity::table_name())
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), query.call("jsonb_set", { statusWithState, jsonPath(query, "{config,message}"), toJsonbText(query, ""), query.cast(query.value(true), ruvia::db_data_type::boolean) }))
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), query.call("now"))
        .where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(configVersion(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "desiredVersion"), ruvia::db_binary_operator::equal, query.cast(query.value(static_cast<std::int64_t>(revision)), ruvia::db_data_type::big_int)))
        .and_where(query.binary(configState(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">())), ruvia::db_binary_operator::equal, query.value(expectedState)))
        .and_where(query.binary(query.coalesce({ jsonText(query, jsonGet(query, status, "config"), "message"), query.value(std::string_view{}) }), ruvia::db_binary_operator::equal, query.value(expectedMessage)));
    return query;
}

inline ruvia::db_query rejectBuildQuery(std::string_view message, std::string_view nodeId, std::uint64_t revision) {
    ruvia::db_query query;
    const auto status = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
    const auto statusWithState = query.call(
        "jsonb_set",
        { status, jsonPath(query, "{config,state}"), toJsonbText(query, "rejected"), query.cast(query.value(true), ruvia::db_data_type::boolean) }
    );
    query.update(service::edge::persistence::EdgeNodeEntity::table_name())
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), query.call("jsonb_set", { statusWithState, jsonPath(query, "{config,message}"), toJsonb(query, query.cast(query.value(message), ruvia::db_data_type::text)), query.cast(query.value(true), ruvia::db_data_type::boolean) }))
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), query.call("now"))
        .where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(configVersion(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "desiredVersion"), ruvia::db_binary_operator::equal, query.cast(query.value(static_cast<std::int64_t>(revision)), ruvia::db_data_type::big_int)));
    return query;
}

} // namespace config::detail

} // namespace service::edge

// Projector Stream entries are written only while the receiving Worker's
// projector lease token is current (fencing); the projection runtime owns the
// lease lifecycle and recovery of dead Streams.
namespace service::edge::projector_stream {

// KEYS[1]=lease, KEYS[2]=data stream, KEYS[3]=registry, KEYS[4]=task wake stream,
// KEYS[5]=data input channel, KEYS[6]=task wake input channel.
inline constexpr std::string_view kFencedPublishScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
-- Data entries may not be trimmed before the projector persists them.  A full
-- queue rejects the device message atomically; the caller then withholds its
-- protocol acknowledgement and retries after reconnecting.
if redis.call('XLEN', KEYS[2]) >= tonumber(ARGV[2]) then return 0 end
local arguments = {'*'}
for index = 5, #ARGV do arguments[#arguments + 1] = ARGV[index] end
local id = redis.call('XADD', KEYS[2], unpack(arguments))
redis.call('SADD', KEYS[3], KEYS[2])
local wake = redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[3], '*', 'task', ARGV[4])
redis.call('PUBLISH', KEYS[5], id)
redis.call('PUBLISH', KEYS[6], wake)
return id
)lua";

inline constexpr std::string_view kMaximumLength{ "100000" };

// Appends `fields` (name/value pairs) to the projector Stream of one Worker.
// Returns false when the lease token is not current or the Stream is full.
template <typename Redis>
ruvia::task<bool> publishFenced(const Redis& redis, const service::message::RedisChannelScope& scope, std::size_t workerIndex, std::string_view instance, std::span<const std::string_view> fields, std::string_view operation) {
    const auto lease = service::message::edge::projector_stream::leaseKey(workerIndex, instance);
    const auto streamName = service::message::edge::projector_stream::stream(workerIndex, instance);
    const std::string registry(service::message::edge::projector_stream::kStreamRegistry);
    const auto wake = service::message::workerWakeStream(workerIndex, instance);
    const auto wakeCapacity = std::to_string(service::message::kWorkerWakeCapacity);
    const auto task = service::message::workerStreamTaskName(service::message::WorkerStreamTask::EdgeProjector);
    const auto token = service::message::edge::projector_stream::ownerToken(workerIndex, instance);
    const auto inputChannel = service::message::consumer::inputChannel(scope, streamName);
    const auto wakeChannel = service::message::consumer::inputChannel(scope, wake);
    const std::string_view keys[]{ lease, streamName, registry, wake, inputChannel, wakeChannel };
    std::vector<std::string_view> arguments{ token, kMaximumLength, wakeCapacity, task };
    arguments.insert(arguments.end(), fields.begin(), fields.end());
    const auto reply = co_await redis.eval(kFencedPublishScript, keys, std::span<const std::string_view>(arguments));
    if (reply.kind() == ruvia::redis_value::kind_type::integer && reply.integer() == 0) {
        co_return false;
    }
    if (reply.kind() != ruvia::redis_value::kind_type::string) {
        service::utils::redis::throwValue(operation, reply);
    }
    co_return true;
}

template <typename Redis>
ruvia::task<bool> publishIngress(const Redis& redis, const service::message::RedisChannelScope& scope, std::size_t workerIndex, std::string_view wire, std::int64_t receivedAtMs, std::string_view instanceId) {
    const auto receivedAt = std::to_string(receivedAtMs);
    const std::string_view fields[]{ "kind", service::message::edge::projector_stream::kIngressKind, "wire", wire, "received_at_ms", receivedAt };
    co_return co_await publishFenced(redis, scope, workerIndex, instanceId, fields, "fenced edge ingress");
}

template <typename Redis>
ruvia::task<bool> publishMetadata(const Redis& redis, const service::message::RedisChannelScope& scope, std::size_t workerIndex, std::string_view nodeId, std::string_view instance) {
    const std::string_view fields[]{ "kind", service::message::edge::projector_stream::kMetadataKind, "node_id", nodeId };
    co_return co_await publishFenced(redis, scope, workerIndex, instance, fields, "fenced edge metadata");
}

} // namespace service::edge::projector_stream

namespace service::edge::metadata {

template <typename Pipeline>
void queueStoreNode(Pipeline& pipeline, std::string_view nodeId, const NodeSnapshot& snapshot) {
    const auto key = service::message::edge::metadataKey(nodeId);
    const auto arguments = storeArguments(snapshot);
    std::vector<std::string_view> command;
    command.reserve(4 + arguments.size());
    command.insert(command.end(), { "EVAL", kStoreNodeScript, "1", key });
    command.insert(command.end(), arguments.begin(), arguments.end());
    pipeline.command(std::span<const std::string_view>(command));
}

template <typename Redis>
ruvia::task<void> storeNode(const Redis& redis, std::string_view nodeId, const NodeSnapshot& snapshot) {
    const auto key = service::message::edge::metadataKey(nodeId);
    const auto arguments = storeArguments(snapshot);
    const std::string_view keys[]{ key };
    const std::vector<std::string_view> argumentViews(arguments.begin(), arguments.end());
    const auto reply = co_await redis.eval(kStoreNodeScript, keys, std::span<const std::string_view>(argumentViews));
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        service::utils::redis::throwValue("store edge metadata", reply);
    }
}

// Returns std::nullopt when the node hash does not exist.
template <typename Redis>
ruvia::task<std::optional<NodeSnapshot>> loadNode(const Redis& redis, std::string_view nodeId) {
    const std::vector<std::string> replyArguments{ "HGETALL", service::message::edge::metadataKey(nodeId) };
    const auto reply =
        co_await redis.command(std::vector<std::string_view>(replyArguments.begin(), replyArguments.end()));
    if (reply.kind() != ruvia::redis_value::kind_type::array) {
        service::utils::redis::throwValue("load edge metadata", reply);
    }
    const auto& fields = reply.array();
    if (fields.empty()) {
        co_return std::nullopt;
    }
    NodeSnapshot snapshot;
    snapshot.reserve(fields.size() / 2);
    for (std::size_t index = 0; index + 1 < fields.size(); index += 2) {
        if (fields[index].kind() != ruvia::redis_value::kind_type::string ||
            fields[index + 1].kind() != ruvia::redis_value::kind_type::string) {
            throw std::runtime_error("edge metadata hash contains a non-string field");
        }
        auto device = decode(fields[index + 1].string());
        if (!device) {
            throw std::runtime_error("edge metadata hash contains an invalid device snapshot");
        }
        snapshot.emplace(std::string(fields[index].string()), std::move(*device));
    }
    co_return snapshot;
}

inline std::int64_t onlineWindowMilliseconds(std::string_view value, std::int64_t fallbackSeconds = 300) noexcept {
    auto seconds = integer(value).value_or(fallbackSeconds);
    if (seconds < 1) {
        seconds = fallbackSeconds;
    }
    if (seconds > std::numeric_limits<std::int64_t>::max() / 1000) {
        return std::numeric_limits<std::int64_t>::max();
    }
    return seconds * 1000;
}

// Device metadata of every approved or pending node, or of one node when `nodeId` is set.
inline ruvia::db_query catalogQuery(std::optional<std::string_view> nodeId) {
    using config::detail::column;
    using Node = service::edge::persistence::EdgeNodeEntity;
    using DeviceRow = service::configuration::persistence::DeviceEntity;
    using LinkRow = service::configuration::persistence::LinkEntity;
    using ModelRow = service::configuration::persistence::DeviceModelEntity;
    using Op = ruvia::db_binary_operator;
    ruvia::db_query query;
    const auto protocolParams = column<DeviceRow, "protocol_params">(query, "d");
    const auto modelConfig = column<ModelRow, "config">(query, "p");
    query
        .select({
            query.cast(column<Node, "id">(query, "n"), ruvia::db_data_type::text),
            query.cast(column<DeviceRow, "id">(query, "d"), ruvia::db_data_type::text),
            query.cast(column<DeviceRow, "link_id">(query, "d"), ruvia::db_data_type::text),
            config::detail::jsonText(query, protocolParams, "device_code"),
            column<ModelRow, "protocol">(query, "p"),
            config::detail::textDefault(query, config::detail::jsonText(query, modelConfig, "storagePolicy"), "report"),
            config::detail::textDefault(query, config::detail::jsonText(query, protocolParams, "online_timeout"), "300"),
        })
        .from(Node::table_name(), "n")
        .join(ruvia::db_join_type::left, LinkRow::table_name(), query.binary(query.binary(column<LinkRow, "edge_node_id">(query, "l"), Op::equal, column<Node, "id">(query, "n")), Op::and_value, query.binary(query.binary(column<LinkRow, "execution">(query, "l"), Op::equal, query.value(std::string_view{ "edge" })), Op::and_value, query.unary(ruvia::db_unary_operator::is_null, column<LinkRow, "deleted_at">(query, "l")))), "l")
        .join(ruvia::db_join_type::left, DeviceRow::table_name(), query.binary(query.binary(column<DeviceRow, "link_id">(query, "d"), Op::equal, column<LinkRow, "id">(query, "l")), Op::and_value, query.unary(ruvia::db_unary_operator::is_null, column<DeviceRow, "deleted_at">(query, "d"))), "d")
        .join(ruvia::db_join_type::left, ModelRow::table_name(), query.binary(query.binary(column<ModelRow, "device_id">(query, "p"), Op::equal, column<DeviceRow, "id">(query, "d")), Op::and_value, query.unary(ruvia::db_unary_operator::is_null, column<ModelRow, "deleted_at">(query, "p"))), "p");
    if (nodeId) {
        query.where(query.binary(column<Node, "id">(query, "n"), Op::equal, query.cast(query.value(*nodeId), ruvia::db_data_type::uuid)));
    }
    query.order_by(column<Node, "id">(query, "n")).add_order_by(column<DeviceRow, "id">(query, "d"));
    return query;
}

template <typename Context>
ruvia::task<Catalog> loadCatalogFromDatabase(Context& context, std::optional<std::string_view> nodeId = std::nullopt) {
    const auto rows = co_await context.db().query(catalogQuery(nodeId));
    Catalog catalog;
    for (const auto& row : rows) {
        auto& snapshot = catalog[std::string(row[0].value().value_or(std::string_view{}))];
        if (!row[1].value().has_value() || !row[2].value().has_value() || !row[3].value().has_value() || !row[4].value().has_value()) {
            continue;
        }
        snapshot.emplace(
            std::string(row[1].value().value_or(std::string_view{})),
            Device{ std::string(row[2].value().value_or(std::string_view{})), std::string(row[3].value().value_or(std::string_view{})), std::string(row[4].value().value_or(std::string_view{})), std::string(row[5].value().value_or(std::string_view{})), onlineWindowMilliseconds(row[6].value().value_or(std::string_view{})) }
        );
    }
    co_return catalog;
}

template <typename Context>
ruvia::task<NodeSnapshot> loadNodeFromDatabase(Context& context, std::string_view nodeId) {
    auto catalog = co_await loadCatalogFromDatabase(context, nodeId);
    const auto node = catalog.find(std::string(nodeId));
    co_return node == catalog.end() ? NodeSnapshot{} : std::move(node->second);
}

// Reloads one node's metadata into Redis and asks the Worker that holds the
// node session to refresh its in-memory catalog from that Redis snapshot.
template <typename Context>
ruvia::task<void> publishNode(Context& context, std::string_view nodeId) {
    const auto snapshot = co_await loadNodeFromDatabase(context, nodeId);
    co_await storeNode(context.redis(), nodeId, snapshot);
    const auto session = co_await context.redis().get(service::message::edge::sessionKey(nodeId));
    if (!session) {
        co_return;
    }
    const auto owner = session_state::parse(std::string_view(session->data(), session->size()));
    if (owner) {
        (void)co_await projector_stream::publishMetadata(context.redis(), context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, owner->workerIndex, nodeId, owner->instanceId);
    }
}

template <typename Context>
ruvia::task<Catalog> hydrate(Context& context) {
    auto catalog = co_await loadCatalogFromDatabase(context);
    if (catalog.empty()) {
        co_return catalog;
    }
    const auto redis = context.redis();
    auto pipeline = redis.pipeline();
    for (const auto& [nodeId, snapshot] : catalog) {
        queueStoreNode(pipeline, nodeId, snapshot);
    }
    const auto replies = co_await std::move(pipeline).exec();
    service::utils::redis::requirePipelineSuccess("hydrate edge metadata", replies);
    co_return catalog;
}

} // namespace service::edge::metadata

namespace service::edge {

// Stateless: every operation receives the Worker context it runs on.
class ConfigService final {
  public:
    template <typename Context>
    static ruvia::task<void> storeDebugPacket(Context& c, std::string_view nodeId, const pb::RawPacket& packet) {
        if (!packet.debug() || packet.endpoint_id().size() != 16 || (packet.payload().empty() && packet.acquisition_state().empty() && !packet.has_parsed_value()) ||
            packet.payload().size() > 4096 || (packet.direction() != "RX" && packet.direction() != "TX" && packet.direction() != "TX_ATTEMPT" && packet.direction() != "RX_DROP")) {
            co_return;
        }
        const auto acquisitionId = protocol::debugAcquisitionId(nodeId, packet);
        const auto linkId = service::common::uuidText(packet.endpoint_id());
        const auto deviceId = packet.device_id().size() == 16 ? service::common::uuidText(packet.device_id()) : std::string{};
        ruvia::db_query node;
        node.select(node.column(persistence::EdgeNodeEntity::column_name<"name">()))
            .from(persistence::EdgeNodeEntity::table_name())
            .where((persistence::EdgeNodeEntity::column<"id">() == nodeId).expression(node));
        ruvia::db_query link;
        link.select({ link.column(service::configuration::persistence::LinkEntity::column_name<"debug_enabled">()), link.subquery(node) })
            .from(service::configuration::persistence::LinkEntity::table_name())
            .where((service::configuration::persistence::LinkEntity::column<"id">() == linkId &&
                    service::configuration::persistence::LinkEntity::column<"edge_node_id">() == nodeId &&
                    service::configuration::persistence::LinkEntity::column<"deleted_at">().is_null())
                       .expression(link));
        const auto links = co_await c.db().query(link);
        if (links.empty()) {
            co_return;
        }
        const bool captureLink = links.front()[0].value().value_or("") == "t" && !packet.device_only();
        bool captureDevice = false;
        if (!deviceId.empty()) {
            ruvia::db_query device;
            device.select(device.column(service::configuration::persistence::DeviceEntity::column_name<"debug_enabled">()))
                .from(service::configuration::persistence::DeviceEntity::table_name())
                .where((service::configuration::persistence::DeviceEntity::column<"id">() == deviceId &&
                        service::configuration::persistence::DeviceEntity::column<"link_id">() == linkId &&
                        service::configuration::persistence::DeviceEntity::column<"deleted_at">().is_null())
                           .expression(device));
            const auto devices = co_await c.db().query(device);
            if (devices.empty()) {
                co_return;
            }
            captureDevice = devices.front()[0].value().value_or("") == "t";
        }
        if (!captureLink && !captureDevice) {
            co_return;
        }
        if (packet.payload().empty() && !packet.has_parsed_value() && packet.acquisition_state() != "running") {
            co_await packet_log::DebugPacketService::finishAcquisition(c.redis(), c.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, acquisitionId, packet.acquisition_state());
            co_return;
        }
        std::string parsedJson;
        if (packet.has_parsed_value()) {
            if (packet.direction() != "RX" || packet.packet_id().size() != 16 ||
                packet.parsed_value().ByteSizeLong() > 12288 || deviceId.empty()) {
                co_return;
            }
            pb::TelemetryRecord decoded;
            auto* value = decoded.add_values();
            *value = packet.parsed_value();
            if (value->element_id().empty()) {
                co_return;
            }
            decoded.set_protocol(pb::PROTOCOL_SL651); // Allows existing binary-value formatting; no wire decoding.
            parsedJson = protocol::TelemetryValues::telemetryJson(decoded);
        }
        co_await packet_log::DebugPacketService::recordPacket(c.redis(), c.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, c.template worker_state<service::common::UuidV7State>(), linkId, deviceId, packet.direction(), "edge", packet.client_address(), std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(packet.payload().data()), packet.payload().size()), packet.observed_at_ms(), captureLink, captureDevice, packet.packet_id().size() == 16 ? std::string(nodeId) + ":" + service::common::uuidText(packet.packet_id()) : std::string{}, packet.status(), packet.reason(), parsedJson, packet.reply_to_packet_id().size() == 16 ? std::string(nodeId) + ":" + service::common::uuidText(packet.reply_to_packet_id()) : std::string{}, packet.has_parsed_value(), packet.payload_offset(), acquisitionId, nodeId, links.front()[1].value().value_or(""));
    }

    static ruvia::task<std::uint64_t> queueSnapshot(ruvia::web_worker_context& c, std::string_view nodeId, std::string_view actorId) {
        const auto version =
            co_await c.db().query(config::detail::queueSnapshotQuery(nodeId));
        if (version.empty()) {
            service::common::fail(17011, "边缘节点未批准或不支持设备配置", 409);
        }
        const auto revision = unsignedInteger(version.front()[0].value().value_or(std::string_view{}));

        auto snapshot = co_await buildSnapshot(c, nodeId, revision);
        if (!snapshot) {
            service::common::fail(17012, "边缘节点配置条目超过 512 条", 409);
        }

        if (!service::common::isUuid(actorId)) {
            service::common::fail(10002, "invalid edge snapshot actor", 400);
        }
        (void)co_await c.db().execute(revisionInsert(nodeId, revision, *snapshot, actorId));
        co_await replaceQueue(c, nodeId, revision, snapshot->wires);
        co_await metadata::publishNode(c, nodeId);
        co_return revision;
    }

    template <typename Context>
    static ruvia::task<bool> requeueIfStale(Context& c, std::string_view nodeId, std::optional<std::uint64_t> reportedRevision = std::nullopt) {
        const auto desired =
            co_await c.db().query(config::detail::requeueDesiredQuery(nodeId));
        if (desired.empty()) {
            co_return false;
        }
        const auto revision = unsignedInteger(desired.front()[0].value().value_or(std::string_view{}));
        const auto state = std::string(desired.front()[1].value().value_or(std::string_view{}));
        const auto activeRevision = reportedRevision.value_or(
            unsignedInteger(desired.front()[2].value().value_or("0"))
        );
        const auto message = std::string(desired.front()[3].value().value_or(std::string_view{}));
        if (!config::detail::needsConfigurationReplay(revision, activeRevision, state, message)) {
            co_return false;
        }

        auto snapshot = co_await buildSnapshot(c, nodeId, revision);
        if (!snapshot) {
            co_return false;
        }
        const auto updated = co_await c.db().execute(config::detail::requeuePendingQuery(nodeId, revision, state, message));
        if (updated.affected_rows() == 0) {
            co_return false;
        }
        auto insert = revisionInsert(nodeId, revision, *snapshot, std::nullopt);
        using Revision = persistence::EdgeConfigRevisionEntity;
        ruvia::db_conflict_options conflict;
        conflict.columns_ = { std::string(Revision::column_name<"node_id">()),
                              std::string(Revision::column_name<"revision">()) };
        conflict.update_ = {
            { std::string(Revision::column_name<"sha256">()), insert.excluded(Revision::column_name<"sha256">()) },
            { std::string(Revision::column_name<"item_count">()), insert.excluded(Revision::column_name<"item_count">()) },
            { std::string(Revision::column_name<"status">()), insert.value(std::string_view{ "pending" }) },
            { std::string(Revision::column_name<"message">()), insert.value(std::string_view{}) },
            { std::string(Revision::column_name<"completed_at">()), insert.null_value() },
        };
        insert.on_conflict(conflict);
        (void)co_await c.db().execute(insert);
        co_await replaceQueue(c, nodeId, revision, snapshot->wires);
        co_await metadata::publishNode(c, nodeId);
        co_return true;
    }

  private:
    struct Snapshot;

    // edge_config_revision row of a built snapshot; background replays have no actor.
    static ruvia::db_query revisionInsert(std::string_view nodeId, std::uint64_t revision, const Snapshot& snapshot, std::optional<std::string_view> actorId) {
        using Revision = persistence::EdgeConfigRevisionEntity;
        ruvia::db_query insert;
        insert
            .insert_into(persistence::EdgeConfigRevisionEntity::table_name(), { Revision::column_name<"node_id">(), Revision::column_name<"revision">(), Revision::column_name<"sha256">(), Revision::column_name<"item_count">(), Revision::column_name<"created_by">() })
            .values({
                insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                insert.value(static_cast<std::int64_t>(revision)),
                insert.value(snapshot.digest),
                insert.value(static_cast<std::int64_t>(snapshot.itemCount)),
                actorId ? insert.cast(insert.value(*actorId), ruvia::db_data_type::uuid) : insert.null_value(),
            });
        return insert;
    }

    struct Snapshot {
        std::string digest;
        std::vector<std::string> wires;
        std::size_t itemCount{};
    };

    static std::uint64_t unsignedInteger(std::string_view value) {
        std::uint64_t result{};
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (error != std::errc{} || end != value.data() + value.size()) {
            throw std::runtime_error("invalid edge config integer");
        }
        return result;
    }

    static pb::Protocol protocolValue(std::string_view value) {
        if (value == "Modbus") {
            return pb::PROTOCOL_MODBUS;
        }
        if (value == "S7") {
            return pb::PROTOCOL_S7;
        }
        if (value == "SL651") {
            return pb::PROTOCOL_SL651;
        }
        if (value == "MC") {
            return pb::PROTOCOL_MC;
        }
        if (value == "FINS") {
            return pb::PROTOCOL_FINS;
        }
        if (value == "DLT645") {
            return pb::PROTOCOL_DLT645;
        }
        return pb::PROTOCOL_UNSPECIFIED;
    }

    static bool setUuid(std::string* field, std::string_view text) {
        std::uint8_t value[16]{};
        if (!service::common::uuidBytes(text, value)) {
            return false;
        }
        field->assign(protocol::bytes(value, sizeof(value)));
        return true;
    }

    static bool encodeItem(const pb::ConfigItem& item, std::string& output) {
        const auto size = item.ByteSizeLong();
        if (size > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
            return false;
        }
        output.assign(size, '\0');
        google::protobuf::io::ArrayOutputStream raw(output.data(), static_cast<int>(size));
        google::protobuf::io::CodedOutputStream coded(&raw);
        coded.SetSerializationDeterministic(true);
        return item.SerializeToCodedStream(&coded) && !coded.HadError();
    }

    static std::array<std::uint8_t, 32> sha256(std::string_view value) {
        std::array<std::uint8_t, 32> output{};
        unsigned size{};
        if (EVP_Digest(value.data(), value.size(), output.data(), &size, EVP_sha256(), nullptr) !=
                1 ||
            size != output.size()) {
            throw std::runtime_error("SHA-256 failed");
        }
        return output;
    }

    static std::array<std::uint8_t, 32>
    digestList(const std::vector<std::array<std::uint8_t, 32>>& values) {
        EVP_MD_CTX* context = EVP_MD_CTX_new();
        if (context == nullptr) {
            throw std::runtime_error("SHA-256 context allocation failed");
        }
        std::array<std::uint8_t, 32> output{};
        unsigned size{};
        bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1;
        for (const auto& value : values) {
            ok = ok && EVP_DigestUpdate(context, value.data(), value.size()) == 1;
        }
        ok = ok && EVP_DigestFinal_ex(context, output.data(), &size) == 1 &&
            size == output.size();
        EVP_MD_CTX_free(context);
        if (!ok) {
            throw std::runtime_error("configuration SHA-256 failed");
        }
        return output;
    }

    static std::string hex(const std::array<std::uint8_t, 32>& value) {
        constexpr char digits[] = "0123456789abcdef";
        std::string result;
        result.reserve(64);
        for (const auto byte : value) {
            result.push_back(digits[byte >> 4U]);
            result.push_back(digits[byte & 0x0fU]);
        }
        return result;
    }

    static void appendWire(std::vector<std::string>& output, const pb::Envelope& envelope) {
        auto wire = protocol::encode(envelope);
        if (wire.empty()) {
            throw std::runtime_error("cannot encode edge configuration envelope");
        }
        output.push_back(std::move(wire));
    }

    template <typename Context>
    static ruvia::task<std::optional<Snapshot>> buildSnapshot(Context& c, std::string_view nodeId, std::uint64_t revision) {
        auto items = co_await loadItems(c, nodeId);
        std::set<std::string> requiredProtocols;
        for (const auto& item : items) {
            if (!item.has_device()) {
                continue;
            }
            switch (item.device().protocol()) {
                case pb::PROTOCOL_MC:
                    requiredProtocols.emplace("MC");
                    break;
                case pb::PROTOCOL_FINS:
                    requiredProtocols.emplace("FINS");
                    break;
                case pb::PROTOCOL_DLT645:
                    requiredProtocols.emplace("DLT645");
                    break;
                default:
                    break;
            }
        }
        for (const auto& name : requiredProtocols) {
            ruvia::db_query capability;
            capability.select(capability.coalesce({ capability.call("jsonb_exists", { config::detail::jsonGet(capability, capability.column(persistence::EdgeNodeEntity::column_name<"capability">()), "protocols"), capability.value(name) }), capability.value(false) }))
                .from(persistence::EdgeNodeEntity::table_name())
                .where(capability.binary(capability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, capability.cast(capability.value(nodeId), ruvia::db_data_type::uuid)));
            const auto rows = co_await c.db().query(capability);
            if (rows.empty() || rows.front()[0].value().value_or("") != "t") {
                co_await rejectBuild(c, nodeId, revision, std::string(config::detail::kMissingProtocolMessage) + name);
                co_return std::nullopt;
            }
        }
        if (items.size() > 512) {
            co_await rejectBuild(c, nodeId, revision, "配置条目超过 nanopb v1 的 512 条上限");
            co_return std::nullopt;
        }

        std::vector<std::array<std::uint8_t, 32>> itemDigests;
        itemDigests.reserve(items.size());
        for (std::size_t index = 0; index < items.size(); ++index) {
            auto& item = items[index];
            item.set_revision(revision);
            item.set_index(static_cast<std::uint32_t>(index));
            std::string canonical;
            if (!encodeItem(item, canonical)) {
                throw std::runtime_error("cannot encode edge config item");
            }
            auto digest = sha256(canonical);
            item.set_sha256(protocol::bytes(digest.data(), digest.size()));
            itemDigests.push_back(digest);
        }
        const auto snapshotDigest = digestList(itemDigests);
        Snapshot snapshot;
        snapshot.digest = hex(snapshotDigest);
        snapshot.itemCount = items.size();
        snapshot.wires.reserve(items.size() + 2);

        auto begin = service::edge::protocol::outbound(service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
        auto* configBegin = begin.mutable_config_begin();
        configBegin->set_revision(revision);
        configBegin->set_item_count(static_cast<std::uint32_t>(items.size()));
        configBegin->set_sha256(
            protocol::bytes(snapshotDigest.data(), snapshotDigest.size())
        );
        appendWire(snapshot.wires, begin);
        for (const auto& item : items) {
            auto envelope = service::edge::protocol::outbound(service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
            *envelope.mutable_config_item() = item;
            appendWire(snapshot.wires, envelope);
        }
        auto commit = service::edge::protocol::outbound(service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
        auto* configCommit = commit.mutable_config_commit();
        configCommit->set_revision(revision);
        configCommit->set_sha256(
            protocol::bytes(snapshotDigest.data(), snapshotDigest.size())
        );
        appendWire(snapshot.wires, commit);
        co_return snapshot;
    }

    template <typename Context>
    static ruvia::task<void> replaceQueue(Context& c, std::string_view nodeId, std::uint64_t revision, const std::vector<std::string>& wires) {
        const std::string key = service::message::edge::configKey(nodeId);
        const std::string revisionKey = service::message::edge::configRevisionKey(nodeId);
        const std::array<std::string_view, 2> keys{ key, revisionKey };
        const auto revisionText = std::to_string(revision);
        std::vector<std::string_view> values;
        values.reserve(wires.size() + 1);
        values.push_back(revisionText);
        for (const auto& wire : wires) {
            values.push_back(wire);
        }
        (void)co_await c.redis().eval(config::detail::kReplaceQueueScript, std::span<const std::string_view>(keys), std::span<const std::string_view>(values));
        co_await dispatch::notifyNode(c.redis(), c.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, nodeId);
    }

    template <typename Context>
    static ruvia::task<void> rejectBuild(Context& c, std::string_view nodeId, std::uint64_t revision, std::string_view message) {
        (void)co_await c.db().execute(
            config::detail::rejectBuildQuery(message, nodeId, revision)
        );
    }

  public:
    template <typename Context>
    static ruvia::task<std::vector<pb::ConfigItem>>
    loadItems(Context& c, std::string_view nodeId, std::map<std::string, std::string>* calculations = nullptr) {
        std::vector<pb::ConfigItem> items;
        ruvia::db_query dtu;
        dtu.select(dtu.column(persistence::EdgeDtuEntity::column_name<"wire_hex">()))
            .from(persistence::EdgeDtuEntity::table_name())
            .where(dtu.binary(dtu.column(persistence::EdgeDtuEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, dtu.cast(dtu.value(nodeId), ruvia::db_data_type::uuid)))
            .order_by(dtu.column(persistence::EdgeDtuEntity::column_name<"channel_id">()));
        const auto channels = co_await c.db().query(dtu);
        if (!channels.empty()) {
            ruvia::db_query capability;
            capability.select(config::detail::jsonText(capability, capability.column(persistence::EdgeNodeEntity::column_name<"capability">()), "dtu"))
                .from(persistence::EdgeNodeEntity::table_name())
                .where(capability.binary(capability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, capability.cast(capability.value(nodeId), ruvia::db_data_type::uuid)));
            const auto supported = co_await c.db().query(capability);
            if (supported.empty() || supported.front()[0].value().value_or("") != "true") {
                throw std::runtime_error("边缘固件尚未声明支持 DTU 透传，不能下发透传配置");
            }
        }
        for (const auto& row : channels) {
            const auto encoded = row[0].value().value_or("");
            std::string wire;
            for (std::size_t i = 0; i + 1 < encoded.size(); i += 2) {
                const int high = service::common::hexDigit(encoded[i]), low = service::common::hexDigit(encoded[i + 1]);
                if (high < 0 || low < 0) {
                    throw std::runtime_error("invalid DTU configuration encoding");
                }
                wire.push_back(static_cast<char>((high << 4) | low));
            }
            pb::ConfigItem item;
            item.set_kind(pb::CONFIG_ITEM_DTU);
            if (!item.mutable_dtu()->ParseFromString(wire)) {
                throw std::runtime_error("invalid DTU configuration");
            }
            items.push_back(std::move(item));
        }
        using Link = service::configuration::persistence::LinkEntity;
        using Model = service::configuration::persistence::DeviceModelEntity;
        auto selection = service::configuration::deviceModelsQuery();
        selection.and_where(selection.binary(Link::column<"execution">().expression(selection, "l"), ruvia::db_binary_operator::equal, selection.value("edge")))
            .and_where(selection.binary(Link::column<"edge_node_id">().expression(selection, "l"), ruvia::db_binary_operator::equal, selection.cast(selection.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(selection.unary(ruvia::db_unary_operator::is_null, Link::column<"deleted_at">().expression(selection, "l")))
            .and_where(selection.unary(ruvia::db_unary_operator::is_null, Model::column<"deleted_at">().expression(selection, "p")));
        auto database = c.db();
        const auto devices = co_await service::configuration::loadDeviceModels(database, selection);
        if (calculations) {
            for (const auto& model : devices) {
                (*calculations)[model.device.id] = model.device.calculationConfig;
            }
        }
        ruvia::db_query capability;
        capability.select(config::detail::jsonText(capability, capability.column(persistence::EdgeNodeEntity::column_name<"capability">()), "derivedPoints"))
            .from(persistence::EdgeNodeEntity::table_name())
            .where(capability.binary(capability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, capability.cast(capability.value(nodeId), ruvia::db_data_type::uuid)));
        const auto supported = co_await c.db().query(capability);
        const bool derivedSupported = !supported.empty() && supported.front()[0].value().value_or("") == "true";
        appendConfiguredDevices(items, devices, derivedSupported);

        co_return items;
    }

    // Edge 保留停用对象，显式发送 enabled=false；所有协议参数均来自 configuration。
    static void appendConfiguredDevices(std::vector<pb::ConfigItem>& items, const std::vector<service::configuration::DeviceModel>& devices, bool derivedSupported) {
        std::set<std::string> endpoints;
        for (const auto& model : devices) {
            const auto& definition = model.device;
            const auto& link = model.endpoint;
            const auto protocol = protocolValue(definition.protocol);
            if (protocol == pb::PROTOCOL_UNSPECIFIED) {
                throw std::invalid_argument("unsupported edge device protocol: " + definition.protocol);
            }
            if (endpoints.emplace(definition.linkId).second) {
                pb::ConfigItem item;
                item.set_kind(pb::CONFIG_ITEM_ENDPOINT);
                auto* value = item.mutable_endpoint();
                if (!setUuid(value->mutable_endpoint_id(), definition.linkId)) {
                    throw std::runtime_error("invalid edge link UUID");
                }
                value->set_name(link.name);
                value->set_interface_name(link.interfaceName);
                value->set_protocol(protocol);
                value->set_debug_enabled(link.debugEnabled);
                value->set_enabled(link.enabled);
                if (link.transport == "serial") {
                    value->set_transport(pb::TRANSPORT_SERIAL);
                    value->set_mode(pb::LINK_MODE_SERIAL);
                    auto* serial = value->mutable_serial();
                    serial->set_channel(link.interfaceName);
                    serial->set_baud_rate(static_cast<std::uint32_t>(link.baudRate));
                    serial->set_data_bits(static_cast<std::uint32_t>(link.dataBits));
                    serial->set_stop_bits(static_cast<std::uint32_t>(link.stopBits));
                    serial->set_parity(link.parity);
                } else {
                    value->set_transport(pb::TRANSPORT_ETHERNET);
                    value->set_mode(link.mode == "TCP Server" ? pb::LINK_MODE_TCP_SERVER : pb::LINK_MODE_TCP_CLIENT);
                    value->set_ip(link.ip);
                    value->set_port(static_cast<std::uint32_t>(link.port));
                }
                items.push_back(std::move(item));
            }
            pb::ConfigItem device;
            device.set_kind(pb::CONFIG_ITEM_DEVICE);
            auto* value = device.mutable_device();
            setUuid(value->mutable_device_id(), definition.id);
            setUuid(value->mutable_endpoint_id(), definition.linkId);
            value->set_device_code(definition.code);
            value->set_name(definition.name);
            value->set_protocol(protocol);
            value->set_debug_enabled(definition.debugEnabled);
            value->set_timezone(definition.timezone);
            if (protocol == pb::PROTOCOL_MC || protocol == pb::PROTOCOL_FINS || protocol == pb::PROTOCOL_DLT645) {
                auto* connection = value->mutable_industrial();
                const auto& mc = definition.mcConnection;
                const auto& fins = definition.finsConnection;
                const auto& dlt = definition.dlt645Connection;
                connection->set_mc_four_e(mc.frame == service::collector::mc::FrameFormat::Binary4E);
                connection->set_mc_network(mc.network);
                connection->set_mc_station(mc.station);
                connection->set_mc_module_io(mc.moduleIo);
                connection->set_mc_multidrop(mc.multidrop);
                connection->set_mc_monitoring_timer(mc.monitoringTimer);
                connection->set_fins_destination_network(fins.destinationNetwork);
                connection->set_fins_destination_node(fins.destinationNode);
                connection->set_fins_destination_unit(fins.destinationUnit);
                connection->set_fins_source_network(fins.sourceNetwork);
                connection->set_fins_source_node(fins.sourceNode);
                connection->set_fins_source_unit(fins.sourceUnit);
                connection->set_dlt645_version(dlt.version == service::collector::dlt645::Version::V1997 ? 1997 : 2007);
                connection->set_dlt645_wakeup_bytes(dlt.wakeupBytes);
                protocol::packet(connection->mutable_dlt645_write_password(), "HEX", dlt.writePassword, "meter password");
                protocol::packet(connection->mutable_dlt645_operator_code(), "HEX", dlt.operatorCode, "meter operator");
            }
            if (protocol == pb::PROTOCOL_SL651) {
                const auto& mode = definition.sl651ResponseMode;
                if (mode.size() != 2 || mode[0] != 'M' || mode[1] < '1' || mode[1] > '4') {
                    throw std::invalid_argument("invalid SL651 response mode");
                }
                value->set_sl651_response_mode(static_cast<std::uint32_t>(mode[1] - '0'));
            }
            // 新固件 0 跟随 report_interval_sec；旧固件保留其兼容的一秒调度。
            value->set_io_interval_ms(0);
            value->set_report_interval_sec(static_cast<std::uint32_t>(definition.readInterval));
            value->set_online_timeout_sec(static_cast<std::uint32_t>(definition.onlineTimeout));
            value->set_modbus_slave_id(definition.slaveId);
            value->set_modbus_mode(definition.modbusMode);
            value->set_modbus_merge_gap(static_cast<std::uint32_t>(definition.modbusMergeGap));
            value->set_modbus_max_quantity(static_cast<std::uint32_t>(definition.modbusMaxQuantity));
            value->set_s7_connection_mode(definition.s7ConnectionMode);
            value->set_s7_connection_type(definition.s7ConnectionType);
            value->set_s7_rack(static_cast<std::uint32_t>(definition.s7Rack));
            value->set_s7_slot(static_cast<std::uint32_t>(definition.s7Slot));
            value->set_s7_local_tsap(definition.s7LocalTsap);
            value->set_s7_remote_tsap(definition.s7RemoteTsap);
            value->set_command_fast_read_duration_sec(static_cast<std::uint32_t>(definition.commandFastReadDuration));
            value->set_command_fast_read_interval_sec(static_cast<std::uint32_t>(definition.commandFastReadInterval));
            value->set_heartbeat_payload(protocol::bytes(definition.heartbeatBytes.data(), definition.heartbeatBytes.size()));
            value->set_enabled(model.enabled);
            items.push_back(std::move(device));
            for (const auto& point : model.derivedPoints) {
                if (!derivedSupported) {
                    throw std::runtime_error("边缘固件尚不支持派生点，请先升级固件");
                }
                pb::ConfigItem item;
                item.set_kind(pb::CONFIG_ITEM_DERIVED_POINT);
                auto* target = item.mutable_derived_point();
                setUuid(target->mutable_device_id(), definition.id);
                target->set_point_id(point.id);
                target->set_name(point.name);
                target->set_unit(point.unit);
                target->set_kind(point.kind);
                target->set_expression(point.expression);
                target->set_boolean_result(point.valueType == "boolean");
                target->set_source_alias(point.sourceAlias);
                target->set_window_seconds(static_cast<std::uint32_t>(point.windowSeconds));
                target->set_max_age_seconds(static_cast<std::uint32_t>(point.maxAgeSeconds));
                target->set_hidden(!point.visible);
                for (const auto& [alias, id] : point.inputs) {
                    auto* input = target->add_inputs();
                    input->set_alias(alias);
                    input->set_point_id(id);
                }
                for (const auto& rule : point.unitRules) {
                    auto* unit = target->add_unit_rules();
                    unit->set_condition(rule.condition);
                    unit->set_unit(rule.unit);
                }
                items.push_back(std::move(item));
            }
            for (const auto& function : model.functions) {
                pb::ConfigItem item;
                item.set_kind(pb::CONFIG_ITEM_SL651_FUNCTION);
                auto* target = item.mutable_sl651_function();
                setUuid(target->mutable_device_id(), definition.id);
                target->set_function_code(function.code);
                target->set_name(function.name);
                target->set_direction(function.direction);
                items.push_back(std::move(item));
            }
            for (const auto& element : definition.elements) {
                pb::ConfigItem item;
                if (protocol == pb::PROTOCOL_MODBUS) {
                    item.set_kind(pb::CONFIG_ITEM_MODBUS_REGISTER);
                    auto* target = item.mutable_modbus_register();
                    setUuid(target->mutable_device_id(), definition.id);
                    target->set_element_id(element.id);
                    target->set_name(element.name);
                    target->set_unit(element.unit);
                    target->set_register_type(element.registerType);
                    target->set_data_type(element.dataType);
                    target->set_byte_order(element.byteOrder);
                    target->set_address(static_cast<std::uint32_t>(element.address));
                    target->set_quantity(static_cast<std::uint32_t>(element.quantity));
                    target->set_scale(1.0);
                    target->set_decimals(static_cast<std::int32_t>(element.decimals));
                    target->set_writable(element.writable);
                } else if (protocol == pb::PROTOCOL_S7) {
                    item.set_kind(pb::CONFIG_ITEM_S7_AREA);
                    auto* target = item.mutable_s7_area();
                    setUuid(target->mutable_device_id(), definition.id);
                    target->set_element_id(element.id);
                    target->set_name(element.name);
                    target->set_unit(element.unit);
                    target->set_area(element.area);
                    target->set_db_number(static_cast<std::uint32_t>(element.dbNumber));
                    target->set_start(static_cast<std::uint32_t>(element.start));
                    target->set_start_bit(static_cast<std::uint32_t>(element.startBit));
                    target->set_size(static_cast<std::uint32_t>(element.size));
                    target->set_data_type(element.dataType);
                    target->set_scale(1.0);
                    target->set_decimals(static_cast<std::int32_t>(element.decimals));
                    target->set_writable(element.writable);
                } else if (protocol == pb::PROTOCOL_SL651) {
                    item.set_kind(pb::CONFIG_ITEM_SL651_ELEMENT);
                    auto* target = item.mutable_sl651_element();
                    setUuid(target->mutable_device_id(), definition.id);
                    target->set_function_code(element.functionCode);
                    target->set_element_id(element.id);
                    target->set_name(element.name);
                    target->set_unit(element.unit);
                    target->set_encoding(element.encoding);
                    target->set_length(static_cast<std::uint32_t>(element.length));
                    target->set_digits(static_cast<std::uint32_t>(element.digits));
                    const auto guide = protocol::packetBytes("HEX", element.guideHex, "sl651_guide");
                    target->set_guide(protocol::bytes(guide.data(), guide.size()));
                    target->set_fixed_position(element.positionMode == "OFFSET");
                    target->set_byte_offset(static_cast<std::uint32_t>(element.byteOffset));
                    target->set_response_element(element.responseElement);
                    target->set_writable(element.writable);
                } else {
                    item.set_kind(pb::CONFIG_ITEM_INDUSTRIAL_POINT);
                    auto* target = item.mutable_industrial_point();
                    setUuid(target->mutable_device_id(), definition.id);
                    target->set_element_id(element.id);
                    target->set_name(element.name);
                    target->set_unit(element.unit);
                    target->set_area(element.area);
                    target->set_data_type(element.dataType);
                    target->set_byte_order(element.byteOrder);
                    target->set_address(static_cast<std::uint32_t>(element.address));
                    target->set_bit(static_cast<std::uint32_t>(element.startBit));
                    target->set_scale(1.0);
                    target->set_decimals(static_cast<std::int32_t>(element.decimals));
                    target->set_identifier(element.guideHex);
                    target->set_length(static_cast<std::uint32_t>(element.length));
                    target->set_digits(static_cast<std::uint32_t>(element.digits));
                    target->set_writable(element.writable);
                }
                items.push_back(std::move(item));
            }
        }
    }
};

class EdgeControlService final {
  public:
    static ruvia::task<std::string> executeOperation(ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        if (stop.stop_requested()) {
            service::common::fail(10004, "Edge operation cancelled", 503);
        }
        if (operation == "queue-snapshot") {
            const auto request = service::message::rpc::decodeLinePair(payload);
            if (!request) {
                service::common::fail(10002, "Invalid edge snapshot payload", 400);
            }
            const auto nodeId = request->first;
            const auto actorId = request->second;
            if (!service::common::isUuid(nodeId) || !service::common::isUuid(actorId)) {
                service::common::fail(10002, "Invalid edge snapshot identifiers", 400);
            }
            const auto revision = co_await ConfigService::queueSnapshot(
                context,
                nodeId,
                actorId
            );
            co_return std::to_string(revision);
        }

        const auto request = service::message::rpc::decodeLinePair(payload);
        if (!request || !service::common::isUuid(request->first)) {
            service::common::fail(10002, "Invalid edge control payload", 400);
        }
        const auto nodeId = request->first;
        const auto requestPayload = request->second;
        if (requestPayload.empty()) {
            service::common::fail(10002, "Empty edge control request", 400);
        }

        pb::Envelope envelope = service::edge::protocol::outbound(service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()), service::message::utcNowMilliseconds(), context.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
        bool parsed = false;
        if (operation == "queue-network") {
            pb::NetworkConfigRequest request;
            parsed = request.ParseFromArray(requestPayload.data(), static_cast<int>(requestPayload.size()));
            if (parsed) {
                *envelope.mutable_network_config_request() = std::move(request);
            }
        } else if (operation == "queue-firmware") {
            pb::FirmwareUpdateRequest request;
            parsed = request.ParseFromArray(requestPayload.data(), static_cast<int>(requestPayload.size()));
            if (parsed) {
                *envelope.mutable_firmware_update_request() = std::move(request);
            }
        } else if (operation == "request-logs") {
            pb::LogRequest request;
            parsed = request.ParseFromArray(requestPayload.data(), static_cast<int>(requestPayload.size()));
            if (parsed) {
                *envelope.mutable_log_request() = std::move(request);
            }
        } else if (operation == "set-log-level") {
            pb::LogLevelRequest request;
            parsed = request.ParseFromArray(requestPayload.data(), static_cast<int>(requestPayload.size()));
            if (parsed) {
                *envelope.mutable_log_level_request() = std::move(request);
            }
        } else {
            service::common::fail(10002, "Unknown edge operation", 400);
        }
        if (!parsed) {
            service::common::fail(10002, "Invalid edge control request", 400);
        }
        const auto wire = protocol::encode(envelope);
        if (wire.empty()) {
            service::common::fail(10002, "Invalid edge control request", 400);
        }
        co_await dispatch::enqueue(context.redis(), context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, nodeId, wire);
        co_return "{}";
    }
};

} // namespace service::edge
