#pragma once

#include <filesystem>
#include <ruvia/web/db/db_predicate.h>
#include <google/protobuf/util/json_util.h>
#include <unordered_set>
#include "service/features/edge/edge.config.h"
#include "service/common/derived_point.h"
#include "service/features/packet_log/packet_log.service.h"

#include "service/features/edge/edge.entity.h"
#include "service/features/edge/edge.types.h"
#include "service/features/edge/acquisition/acquisition.protocol.h"
#include <map>

#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/web/web_worker.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/common/message.h"
#include <chrono>
#include "service/features/edge/session/session.service.h"
#include "service/features/vpn/vpn.service.h"

namespace service::edge::projector_stream {

inline constexpr auto kLeaseTtl = std::chrono::milliseconds(15000);

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
redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[3], '*', 'task', ARGV[4])
return id
)lua";

template <typename Redis>
ruvia::task<bool> publishIngress(const Redis& redis, std::size_t workerIndex, std::string_view wire, std::int64_t receivedAtMs) {
    const auto instance = service::runtime::instanceId();
    const auto lease = leaseKey(workerIndex, instance);
    const auto streamName = stream(workerIndex, instance);
    const auto registry = std::string(kStreamRegistry);
    const auto wake = service::message::workerWakeStream(workerIndex, instance);
    const std::string maxLength = "100000";
    const std::string wakeCapacity = std::to_string(service::message::kWorkerWakeCapacity);
    const std::string task(
        service::message::workerStreamTaskName(service::message::WorkerStreamTask::EdgeProjector)
    );
    const auto token = ownerToken(workerIndex, instance);
    const std::string receivedAt = std::to_string(receivedAtMs);
    const std::string_view keys[]{ lease, streamName, registry, wake };
    const std::string_view arguments[]{ token, maxLength, wakeCapacity, task, "kind", kIngressKind, "wire", wire, "received_at_ms", receivedAt };
    const auto reply = co_await redis.eval(kFencedPublishScript, keys, arguments);
    if (reply.kind() == ruvia::redis_value::kind_type::integer && reply.integer() == 0) {
        co_return false;
    }
    if (reply.kind() != ruvia::redis_value::kind_type::string) {
        service::message::redis::throwValue("fenced edge ingress", reply);
    }
    co_return true;
}

template <typename Redis>
ruvia::task<bool> publishMetadata(const Redis& redis, std::size_t workerIndex, std::string_view nodeId, std::string_view instance = service::runtime::instanceId()) {
    const auto lease = leaseKey(workerIndex, instance);
    const auto streamName = stream(workerIndex, instance);
    const auto registry = std::string(kStreamRegistry);
    const auto wake = service::message::workerWakeStream(workerIndex, instance);
    const std::string maxLength = "100000";
    const std::string wakeCapacity = std::to_string(service::message::kWorkerWakeCapacity);
    const std::string task(
        service::message::workerStreamTaskName(service::message::WorkerStreamTask::EdgeProjector)
    );
    const auto token = ownerToken(workerIndex, instance);
    const std::string_view keys[]{ lease, streamName, registry, wake };
    const std::string_view arguments[]{ token, maxLength, wakeCapacity, task, "kind", kMetadataKind, "node_id", nodeId };
    const auto reply = co_await redis.eval(kFencedPublishScript, keys, arguments);
    if (reply.kind() == ruvia::redis_value::kind_type::integer && reply.integer() == 0) {
        co_return false;
    }
    if (reply.kind() != ruvia::redis_value::kind_type::string) {
        service::message::redis::throwValue("fenced edge metadata", reply);
    }
    co_return true;
}

} // namespace service::edge::projector_stream

namespace service::edge::metadata {

inline constexpr std::string_view kStoreNodeScript = R"lua(
redis.call('DEL', KEYS[1])
for index = 1, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[index], ARGV[index + 1])
end
return #ARGV / 2
)lua";

inline std::int64_t onlineWindowMilliseconds(std::string_view value,
                                             std::int64_t fallbackSeconds = 300) noexcept {
    auto seconds = integer(value).value_or(fallbackSeconds);
    if (seconds < 1)
        seconds = fallbackSeconds;
    if (seconds > std::numeric_limits<std::int64_t>::max() / 1000)
        return std::numeric_limits<std::int64_t>::max();
    return seconds * 1000;
}

template <typename Pipeline>
void queueStoreNode(Pipeline& pipeline, std::string_view nodeId,
                     const NodeSnapshot& snapshot) {
    const std::vector<std::string> keys{key(nodeId)};
    std::vector<std::string> arguments;
    arguments.reserve(snapshot.size() * 2);
    for (const auto& [deviceId, device] : snapshot) {
        arguments.push_back(deviceId);
        arguments.push_back(encode(device));
    }
    const std::vector<std::string_view> keyViews(keys.begin(), keys.end());
    const std::vector<std::string_view> argumentViews(arguments.begin(), arguments.end());
    service::message::redis::queueEval(pipeline, kStoreNodeScript, keyViews, argumentViews);
}

template <typename Context>
ruvia::task<NodeSnapshot> loadNodeFromDatabase(Context& context, std::string_view nodeId) {
    ruvia::db_query query;
    const auto jsonText = [&query](std::string_view column,
                                   std::string_view table,
                                   std::string_view key) {
        return query.binary(query.column(column, table),
                            ruvia::db_binary_operator::json_get_text,
                            query.cast(query.value(key), ruvia::db_data_type::text));
    };
    query
        .select({
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"), ruvia::db_data_type::text),
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d"), ruvia::db_data_type::text),
            jsonText("protocol_params", "d", "device_code"),
            query.column(service::edge::persistence::DeviceModelEntity::column_name<"protocol">(), "p"),
            query.coalesce({
                query.null_if(jsonText("config", "p", "storagePolicy"),
                             query.value(std::string_view{})),
                query.value(std::string_view{"report"})}),
            query.coalesce({
                query.null_if(jsonText("protocol_params", "d", "online_timeout"),
                             query.value(std::string_view{})),
                query.value(std::string_view{"300"})}),
        })
        .from(service::edge::persistence::DeviceEntity::table_name(), "d")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::LinkEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"id">(), "l"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"execution">(), "l"),
                                 ruvia::db_binary_operator::equal,
                                 query.value(std::string_view{"edge"})),
                    ruvia::db_binary_operator::and_value,
                    query.unary(ruvia::db_unary_operator::is_null,
                                query.column(service::edge::persistence::LinkEntity::column_name<"deleted_at">(), "l")))),
            "l")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::DeviceModelEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"device_id">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.unary(ruvia::db_unary_operator::is_null,
                            query.column(service::edge::persistence::DeviceModelEntity::column_name<"deleted_at">(), "p"))),
            "p")
        .where(query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"edge_node_id">(), "l"),
                           ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId),
                                      ruvia::db_data_type::uuid)))
        .and_where(query.unary(ruvia::db_unary_operator::is_null,
                              query.column(service::edge::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
        .order_by(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"));
    const auto rows = co_await context.db().query(query);
    NodeSnapshot snapshot;
    snapshot.reserve(rows.size());
    for (const auto& row : rows) {
        snapshot.emplace(
            std::string(row[0].value().value_or(std::string_view{})),
            Device{std::string(row[1].value().value_or(std::string_view{})), std::string(row[2].value().value_or(std::string_view{})),
                   std::string(row[3].value().value_or(std::string_view{})), std::string(row[4].value().value_or(std::string_view{})),
                   onlineWindowMilliseconds(row[5].value().value_or(std::string_view{}))});
    }
    co_return snapshot;
}

template <typename Context>
ruvia::task<Catalog> loadCatalogFromDatabase(Context& context) {
    ruvia::db_query query;
    const auto jsonText = [&query](std::string_view column,
                                   std::string_view table,
                                   std::string_view key) {
        return query.binary(query.column(column, table),
                            ruvia::db_binary_operator::json_get_text,
                            query.cast(query.value(key), ruvia::db_data_type::text));
    };
    query
        .select({
            query.cast(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "n"), ruvia::db_data_type::text),
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"), ruvia::db_data_type::text),
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d"), ruvia::db_data_type::text),
            jsonText("protocol_params", "d", "device_code"),
            query.column(service::edge::persistence::DeviceModelEntity::column_name<"protocol">(), "p"),
            query.coalesce({
                query.null_if(jsonText("config", "p", "storagePolicy"),
                             query.value(std::string_view{})),
                query.value(std::string_view{"report"})}),
            query.coalesce({
                query.null_if(jsonText("protocol_params", "d", "online_timeout"),
                             query.value(std::string_view{})),
                query.value(std::string_view{"300"})}),
        })
        .from(service::edge::persistence::EdgeNodeEntity::table_name(), "n")
        .join(
            ruvia::db_join_type::left, service::edge::persistence::LinkEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"edge_node_id">(), "l"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "n")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"execution">(), "l"),
                                 ruvia::db_binary_operator::equal,
                                 query.value(std::string_view{"edge"})),
                    ruvia::db_binary_operator::and_value,
                    query.unary(ruvia::db_unary_operator::is_null,
                                query.column(service::edge::persistence::LinkEntity::column_name<"deleted_at">(), "l")))),
            "l")
        .join(
            ruvia::db_join_type::left, service::edge::persistence::DeviceEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::LinkEntity::column_name<"id">(), "l")),
                ruvia::db_binary_operator::and_value,
                query.unary(ruvia::db_unary_operator::is_null,
                            query.column(service::edge::persistence::DeviceEntity::column_name<"deleted_at">(), "d"))),
            "d")
        .join(
            ruvia::db_join_type::left, service::edge::persistence::DeviceModelEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"device_id">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.unary(ruvia::db_unary_operator::is_null,
                            query.column(service::edge::persistence::DeviceModelEntity::column_name<"deleted_at">(), "p"))),
            "p")
        .order_by(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "n"))
        .add_order_by(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"));
    const auto rows = co_await context.db().query(query);
    Catalog catalog;
    for (const auto& row : rows) {
        auto& snapshot = catalog[std::string(row[0].value().value_or(std::string_view{}))];
        if (!row[1].value().has_value() || !row[2].value().has_value() || !row[3].value().has_value() || !row[4].value().has_value())
            continue;
        snapshot.emplace(
            std::string(row[1].value().value_or(std::string_view{})),
            Device{std::string(row[2].value().value_or(std::string_view{})), std::string(row[3].value().value_or(std::string_view{})),
                   std::string(row[4].value().value_or(std::string_view{})), std::string(row[5].value().value_or(std::string_view{})),
                   onlineWindowMilliseconds(row[6].value().value_or(std::string_view{}))});
    }
    co_return catalog;
}

template <typename Redis>
ruvia::task<void> storeNode(const Redis& redis, std::string_view nodeId,
                             const NodeSnapshot& snapshot, bool notify) {
    const std::vector<std::string> keyStore{key(nodeId)};
    std::vector<std::string> argumentStore;
    argumentStore.reserve(snapshot.size() * 2);
    for (const auto& [deviceId, device] : snapshot) {
        argumentStore.push_back(deviceId);
        argumentStore.push_back(encode(device));
    }
    std::vector<std::string_view> keys(keyStore.begin(), keyStore.end());
    std::vector<std::string_view> arguments(argumentStore.begin(), argumentStore.end());
    const auto reply = co_await redis.eval(kStoreNodeScript,
                                           std::span<const std::string_view>(keys),
                                           std::span<const std::string_view>(arguments));
    if (reply.kind() != ruvia::redis_value::kind_type::integer)
        service::message::redis::throwValue("store edge metadata", reply);
    if (!notify)
        co_return;
    const auto session = co_await redis.get(session_state::key(nodeId));
    if (!session)
        co_return;
    const auto owner = session_state::parse(
        std::string_view(session->data(), session->size()));
    if (owner)
        co_await projector_stream::publishMetadata(redis, owner->workerIndex, nodeId, owner->instanceId);
}

template <typename Redis>
ruvia::task<NodeSnapshot> loadNode(const Redis& redis, std::string_view nodeId) {
    const std::vector<std::string> replyArguments{"HGETALL", key(nodeId)};
    const auto reply =
        co_await service::message::redis::command(redis, replyArguments);
    if (reply.kind() != ruvia::redis_value::kind_type::array)
        service::message::redis::throwValue("load edge metadata", reply);
    NodeSnapshot snapshot;
    const auto& fields = reply.array();
    snapshot.reserve(fields.size() / 2);
    for (std::size_t index = 0; index + 1 < fields.size(); index += 2) {
        if (fields[index].kind() != ruvia::redis_value::kind_type::string ||
            fields[index + 1].kind() != ruvia::redis_value::kind_type::string)
            throw std::runtime_error("edge metadata hash contains a non-string field");
        auto device = decode(fields[index + 1].string());
        if (!device)
            throw std::runtime_error("edge metadata hash contains an invalid device snapshot");
        snapshot.emplace(std::string(fields[index].string()), std::move(*device));
    }
    co_return snapshot;
}

template <typename Context>
ruvia::task<void> publishNode(Context& context, std::string_view nodeId) {
    auto snapshot = co_await loadNodeFromDatabase(context, nodeId);
    co_await storeNode(context.redis(), nodeId, snapshot, true);
}

template <typename Context>
ruvia::task<Catalog> hydrate(Context& context) {
    auto catalog = co_await loadCatalogFromDatabase(context);
    if (catalog.empty())
        co_return catalog;
    const auto redis = context.redis();
    auto pipeline = redis.pipeline();
    for (const auto& [nodeId, snapshot] : catalog)
        queueStoreNode(pipeline, nodeId, snapshot);
    const auto replies = co_await std::move(pipeline).exec();
    service::message::redis::requirePipelineSuccess("hydrate edge metadata", replies);
    co_return catalog;
}

} // namespace service::edge::metadata

#include <algorithm>
#include <array>
#include <cmath>
#include <set>

#include <openssl/evp.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <ruvia/web/controller.h>

#include "service/features/edge/edge.protocol.h"

#include "service/utils/number.h"

namespace service::edge {

namespace config::detail {
inline std::vector<std::uint8_t> packetBytes(std::string_view mode, std::string_view content,
                                             std::string_view name) {
    std::vector<std::uint8_t> output;
    if (mode == "OFF" || content.empty())
        return output;
    if (mode == "ASCII") {
        output.assign(content.begin(), content.end());
        return output;
    }
    if (mode != "HEX")
        throw std::runtime_error("invalid edge config packet mode: " + std::string(name));

    int high = -1;
    for (const char character : content) {
        if (character == ' ' || character == '\t' || character == '\r' || character == '\n')
            continue;
        const int digit = protocol::hexDigit(character);
        if (digit < 0)
            throw std::runtime_error("invalid edge config hex: " + std::string(name));
        if (high < 0)
            high = digit;
        else {
            output.push_back(static_cast<std::uint8_t>((high << 4U) | digit));
            high = -1;
        }
    }
    if (high >= 0)
        throw std::runtime_error("invalid edge config hex: " + std::string(name));
    return output;
}

inline void packet(std::string* output, std::string_view mode, std::string_view content,
                   std::string_view name) {
    const auto value = packetBytes(mode, content, name);
    output->assign(protocol::bytes(value.data(), value.size()));
}

inline double number(std::string_view value, double fallback = 0.0) {
    if (value.empty())
        return fallback;
    const auto result = service::utils::decimal(value);
    return result.value_or(fallback);
}

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

inline ruvia::db_query::expr_type jsonGet(ruvia::db_query& query,
                                    ruvia::db_query::expr_type value,
                                    std::string_view key) {
    return query.binary(std::move(value), ruvia::db_binary_operator::json_get,
                        query.cast(query.value(key), ruvia::db_data_type::text));
}

inline ruvia::db_query::expr_type jsonText(ruvia::db_query& query,
                                     ruvia::db_query::expr_type value,
                                     std::string_view key) {
    return query.binary(std::move(value),
                        ruvia::db_binary_operator::json_get_text,
                        query.cast(query.value(key), ruvia::db_data_type::text));
}

inline ruvia::db_query::expr_type jsonText(ruvia::db_query& query,
                                     std::string_view column,
                                     std::string_view table,
                                     std::string_view key) {
    return jsonText(query, query.column(column, table), key);
}

inline ruvia::db_query::expr_type jsonPath(ruvia::db_query& query,
                                     std::string_view path) {
    return query.cast(
        query.value(path),
        ruvia::db_type_definition{.data_type_ = ruvia::db_data_type::text,
                                .array_ = true});
}

inline ruvia::db_query::expr_type toJsonb(ruvia::db_query& query,
                                   ruvia::db_query::expr_type value) {
    return query.call("to_jsonb", {std::move(value)});
}

inline ruvia::db_query::expr_type toJsonbText(ruvia::db_query& query,
                                       std::string_view value) {
    return toJsonb(query, query.cast(query.value(value), ruvia::db_data_type::text));
}

inline ruvia::db_query::expr_type nullableDefault(ruvia::db_query& query,
                                            ruvia::db_query::expr_type value,
                                            std::string_view fallback) {
    return query.coalesce({std::move(value),
                           query.cast(query.value(fallback), ruvia::db_data_type::text)});
}

inline ruvia::db_query::expr_type textDefault(ruvia::db_query& query,
                                        ruvia::db_query::expr_type value,
                                        std::string_view fallback) {
    return query.coalesce({
        query.null_if(std::move(value), query.value(std::string_view{})),
        query.cast(query.value(fallback), ruvia::db_data_type::text)});
}

inline ruvia::db_query::expr_type booleanText(ruvia::db_query& query,
                                        ruvia::db_query::expr_type value) {
    const auto lower = query.call(
        "lower",
        {query.coalesce({std::move(value),
                         query.cast(query.value(std::string_view{}),
                                    ruvia::db_data_type::text)})});
    return query.case_when(
        {{query.binary(lower, ruvia::db_binary_operator::equal,
                       query.cast(query.value(std::string_view{"true"}),
                                  ruvia::db_data_type::text)),
          query.cast(query.value(true), ruvia::db_data_type::boolean)},
         {query.binary(lower, ruvia::db_binary_operator::equal,
                       query.cast(query.value(std::string_view{"t"}),
                                  ruvia::db_data_type::text)),
          query.cast(query.value(true), ruvia::db_data_type::boolean)},
         {query.binary(lower, ruvia::db_binary_operator::equal,
                       query.cast(query.value(std::string_view{"1"}),
                                  ruvia::db_data_type::text)),
          query.cast(query.value(true), ruvia::db_data_type::boolean)}},
        query.cast(query.value(false), ruvia::db_data_type::boolean));
}

inline ruvia::db_query::expr_type jsonKey(ruvia::db_query& query,
                                    std::string_view value) {
    return query.cast(query.value(value), ruvia::db_data_type::text);
}

inline ruvia::db_query::expr_type configVersion(ruvia::db_query& query,
                                          ruvia::db_query::expr_type status,
                                          std::string_view key) {
    const auto text = jsonText(query, jsonGet(query, std::move(status), "config"), key);
    const auto valid = query.binary(
        text, ruvia::db_binary_operator::regex,
        query.cast(query.value(std::string_view{"^-?[0-9]{1,18}$"}),
                   ruvia::db_data_type::text));
    return query.coalesce(
        {query.case_when({{valid, query.cast(text, ruvia::db_data_type::big_int)}}),
         query.value(std::int64_t{0})});
}

inline ruvia::db_query::expr_type configVersion(ruvia::db_query& query,
                                          std::string_view table,
                                          std::string_view key) {
    return configVersion(query, query.column("status", table), key);
}

inline ruvia::db_query::expr_type configState(ruvia::db_query& query,
                                        ruvia::db_query::expr_type status) {
    return query.coalesce(
        {jsonText(query, jsonGet(query, std::move(status), "config"), "state"),
         query.cast(query.value(std::string_view{"idle"}),
                    ruvia::db_data_type::text)});
}

inline ruvia::db_query::expr_type capabilityEnabled(ruvia::db_query& query,
                                              std::string_view table) {
    return booleanText(query, jsonText(query, query.column("capability", table),
                                        "deviceConfig"));
}

inline ruvia::db_query queueSnapshotQuery(std::string_view nodeId) {
    ruvia::db_query next;
    const auto desired = configVersion(next, next.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                                       "desiredVersion");
    const auto active = configVersion(next, next.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                                      "activeVersion");
    const auto nowMilliseconds = next.cast(
        next.binary(next.extract(ruvia::db_date_part::epoch,
                                 next.call("clock_timestamp")),
                    ruvia::db_binary_operator::multiply,
                    next.value(std::int64_t{1000})),
        ruvia::db_data_type::big_int);
    next.select({
            next.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()),
            next.alias(next.greatest({
                           nowMilliseconds,
                           next.binary(desired, ruvia::db_binary_operator::add,
                                       next.value(std::int64_t{1})),
                           next.binary(active, ruvia::db_binary_operator::add,
                                       next.value(std::int64_t{1}))}),
                       "revision"),
        })
        .from(service::edge::persistence::EdgeNodeEntity::table_name())
        .where(next.binary(next.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                           next.cast(next.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(next.binary(next.column(service::edge::persistence::EdgeNodeEntity::column_name<"enrollment_status">()),
                              ruvia::db_binary_operator::equal,
                              next.value(std::string_view{"approved"})))
        .and_where(capabilityEnabled(next, {}));

    ruvia::db_query update;
    const auto status = update.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), "node");
    const auto statusWithVersion = update.call(
        "jsonb_set",
        {status, jsonPath(update, "{config,desiredVersion}"),
         toJsonb(update, update.column("revision", "next")),
         update.cast(update.value(true), ruvia::db_data_type::boolean)});
    const auto statusWithState = update.call(
        "jsonb_set",
        {statusWithVersion, jsonPath(update, "{config,state}"),
         toJsonbText(update, "pending"),
         update.cast(update.value(true), ruvia::db_data_type::boolean)});
    const auto statusWithMessage = update.call(
        "jsonb_set",
        {statusWithState, jsonPath(update, "{config,message}"),
         toJsonbText(update, ""),
         update.cast(update.value(true), ruvia::db_data_type::boolean)});
    update.with("next", next)
        .update(service::edge::persistence::EdgeNodeEntity::table_name(), "node")
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), statusWithMessage)
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), update.call("now"))
        .update_from("next")
        .where(update.binary(update.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "node"),
                            ruvia::db_binary_operator::equal,
                            update.column("id", "next")))
        .returning({update.column("revision", "next")});
    return update;
}

inline ruvia::db_query requeueDesiredQuery(std::string_view nodeId) {
    ruvia::db_query query;
    query.select({configVersion(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "desiredVersion"),
                  configState(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()))})
        .from(service::edge::persistence::EdgeNodeEntity::table_name())
        .where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"enrollment_status">()),
                              ruvia::db_binary_operator::equal,
                              query.value(std::string_view{"approved"})))
        .and_where(capabilityEnabled(query, {}));
    return query;
}

inline ruvia::db_query requeuePendingQuery(std::string_view nodeId,
                                          std::uint64_t revision) {
    ruvia::db_query query;
    const auto status = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
    const auto statusWithState = query.call(
        "jsonb_set",
        {status, jsonPath(query, "{config,state}"),
         toJsonbText(query, "pending"),
         query.cast(query.value(true), ruvia::db_data_type::boolean)});
    query.update(service::edge::persistence::EdgeNodeEntity::table_name())
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), query.call(
                           "jsonb_set",
                           {statusWithState, jsonPath(query, "{config,message}"),
                            toJsonbText(query, ""),
                            query.cast(query.value(true), ruvia::db_data_type::boolean)}))
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), query.call("now"))
        .where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(configVersion(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                                             "desiredVersion"),
                              ruvia::db_binary_operator::equal,
                              query.cast(query.value(static_cast<std::int64_t>(revision)),
                                         ruvia::db_data_type::big_int)))
        .and_where(query.binary(configState(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">())),
                              ruvia::db_binary_operator::not_equal,
                              query.value(std::string_view{"rejected"})));
    return query;
}

inline ruvia::db_query rejectBuildQuery(std::string_view message,
                                       std::string_view nodeId,
                                       std::uint64_t revision) {
    ruvia::db_query query;
    const auto status = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
    const auto statusWithState = query.call(
        "jsonb_set",
        {status, jsonPath(query, "{config,state}"),
         toJsonbText(query, "rejected"),
         query.cast(query.value(true), ruvia::db_data_type::boolean)});
    query.update(service::edge::persistence::EdgeNodeEntity::table_name())
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), query.call(
                           "jsonb_set",
                           {statusWithState, jsonPath(query, "{config,message}"),
                            toJsonb(query, query.cast(query.value(message),
                                                      ruvia::db_data_type::text)),
                            query.cast(query.value(true), ruvia::db_data_type::boolean)}))
        .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), query.call("now"))
        .where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(configVersion(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                                             "desiredVersion"),
                              ruvia::db_binary_operator::equal,
                              query.cast(query.value(static_cast<std::int64_t>(revision)),
                                         ruvia::db_data_type::big_int)));
    return query;
}

inline ruvia::db_query buildItemsQuery(std::string_view nodeId) {
    ruvia::db_query query;
    const auto protocolParams = query.column(service::edge::persistence::DeviceEntity::column_name<"protocol_params">(), "d");
    const auto modelConfig = query.column(service::edge::persistence::DeviceModelEntity::column_name<"config">(), "p");
    const auto endpoint = query.column(service::edge::persistence::LinkEntity::column_name<"endpoint">(), "l");
    const auto packetConfig = jsonGet(query, modelConfig, "packet");
    const auto connectionConfig = jsonGet(query, modelConfig, "connection");
    const auto heartbeatConfig = jsonGet(query, protocolParams, "heartbeat");
    const auto deviceEnabled = query.binary(
        query.column(service::edge::persistence::DeviceEntity::column_name<"status">(), "d"), ruvia::db_binary_operator::equal,
        query.value(std::string_view{"enabled"}));
    const auto linkEnabled = query.binary(
        query.column(service::edge::persistence::LinkEntity::column_name<"status">(), "l"), ruvia::db_binary_operator::equal,
        query.value(std::string_view{"enabled"}));

    query
        .select({
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"), ruvia::db_data_type::text),
            query.column(service::edge::persistence::DeviceEntity::column_name<"name">(), "d"),
            jsonText(query, protocolParams, "device_code"),
            query.column(service::edge::persistence::DeviceModelEntity::column_name<"protocol">(), "p"),
            textDefault(query, jsonText(query, protocolParams, "timezone"), "+08:00"),
            textDefault(query, jsonText(query, modelConfig, "readInterval"), "300"),
            textDefault(query, jsonText(query, protocolParams, "online_timeout"), "300"),
            textDefault(query, jsonText(query, protocolParams, "slave_id"), "1"),
            nullableDefault(query, jsonText(query, protocolParams, "modbus_mode"), "TCP"),
            jsonText(query, endpoint, "transport"),
            jsonText(query, endpoint, "interface"),
            nullableDefault(query, jsonText(query, endpoint, "mode"), ""),
            nullableDefault(query, jsonText(query, endpoint, "ip"), ""),
            textDefault(query, jsonText(query, endpoint, "port"), "0"),
            textDefault(query, jsonText(query, endpoint, "baud_rate"), "9600"),
            textDefault(query, jsonText(query, endpoint, "data_bits"), "8"),
            textDefault(query, jsonText(query, endpoint, "stop_bits"), "1"),
            nullableDefault(query, jsonText(query, endpoint, "parity"), "none"),
            booleanText(query, jsonText(query, endpoint, "rs485")),
            textDefault(query, jsonText(query, packetConfig, "mergeGap"), "0"),
            textDefault(query, jsonText(query, packetConfig, "maxQuantity"), "125"),
            nullableDefault(query, jsonText(query, connectionConfig, "mode"), "RACK_SLOT"),
            nullableDefault(query, jsonText(query, connectionConfig, "connectionType"), "PG"),
            textDefault(query, jsonText(query, connectionConfig, "rack"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "slot"), "1"),
            nullableDefault(query, jsonText(query, connectionConfig, "localTSAP"), ""),
            nullableDefault(query, jsonText(query, connectionConfig, "remoteTSAP"), ""),
            nullableDefault(query, jsonText(query, heartbeatConfig, "mode"), "OFF"),
            nullableDefault(query, jsonText(query, heartbeatConfig, "content"), ""),
            query.binary(query.binary(deviceEnabled, ruvia::db_binary_operator::and_value,
                                      query.column(service::edge::persistence::DeviceModelEntity::column_name<"enabled">(), "p")),
                         ruvia::db_binary_operator::and_value, linkEnabled),
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d"), ruvia::db_data_type::text),
            textDefault(query, jsonText(query, modelConfig, "commandFastReadDuration"), "60"),
            textDefault(query, jsonText(query, modelConfig, "commandFastReadInterval"), "1"),
            query.column(service::edge::persistence::LinkEntity::column_name<"name">(), "l"),
            linkEnabled,
            textDefault(query, jsonText(query, modelConfig, "responseMode"), "M1"),
            query.column(service::edge::persistence::LinkEntity::column_name<"debug_enabled">(), "l"),
            query.column(service::edge::persistence::DeviceEntity::column_name<"debug_enabled">(), "d"),
            textDefault(query, jsonText(query, connectionConfig, "frame"), "3E"),
            textDefault(query, jsonText(query, connectionConfig, "version"), "2007"),
            textDefault(query, jsonText(query, connectionConfig, "network"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "station"), "255"),
            textDefault(query, jsonText(query, connectionConfig, "moduleIo"), "1023"),
            textDefault(query, jsonText(query, connectionConfig, "multidrop"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "monitoringTimer"), "16"),
            textDefault(query, jsonText(query, connectionConfig, "destinationNetwork"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "destinationNode"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "destinationUnit"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "sourceNetwork"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "sourceNode"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "sourceUnit"), "0"),
            textDefault(query, jsonText(query, connectionConfig, "wakeupBytes"), "4"),
            textDefault(query, jsonText(query, connectionConfig, "writePassword"), ""),
            textDefault(query, jsonText(query, connectionConfig, "operatorCode"), ""),
            query.cast(modelConfig, ruvia::db_data_type::text),
        })
        .from(service::edge::persistence::DeviceEntity::table_name(), "d")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::LinkEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"id">(), "l"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"execution">(), "l"),
                                 ruvia::db_binary_operator::equal,
                                 query.value(std::string_view{"edge"})),
                    ruvia::db_binary_operator::and_value,
                    query.unary(ruvia::db_unary_operator::is_null,
                                query.column(service::edge::persistence::LinkEntity::column_name<"deleted_at">(), "l")))),
            "l")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::DeviceModelEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"device_id">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.unary(ruvia::db_unary_operator::is_null,
                            query.column(service::edge::persistence::DeviceModelEntity::column_name<"deleted_at">(), "p"))),
            "p")
        .where(query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"edge_node_id">(), "l"),
                           ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId),
                                      ruvia::db_data_type::uuid)))
        .and_where(query.unary(ruvia::db_unary_operator::is_null,
                              query.column(service::edge::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
        .order_by(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"));
    return query;
}

inline ruvia::db_query appendModbusQuery(std::string_view nodeId) {
    ruvia::db_query query;
    const auto config = query.column(service::edge::persistence::DeviceModelEntity::column_name<"config">(), "p");
    const auto itemSource = query.call(
        "jsonb_array_elements",
        {query.coalesce({jsonGet(query, config, "registers"),
                         query.cast(query.value(std::string_view{"[]"}),
                                    ruvia::db_data_type::jsonb)})});
    const auto item = query.column("item");
    query
        .select({
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"), ruvia::db_data_type::text),
            jsonText(query, item, "id"),
            jsonText(query, item, "name"),
            nullableDefault(query, jsonText(query, item, "unit"), ""),
            jsonText(query, item, "registerType"),
            jsonText(query, item, "dataType"),
            query.coalesce({jsonText(query, item, "byteOrder"),
                           jsonText(query, config, "byteOrder"),
                           query.value(std::string_view{"BIG_ENDIAN"})}),
            textDefault(query, jsonText(query, item, "address"), "0"),
            textDefault(query, jsonText(query, item, "quantity"), "1"),
            textDefault(query, jsonText(query, item, "scale"), "1"),
            textDefault(query, jsonText(query, item, "decimals"), "-1"),
            booleanText(query, jsonText(query, item, "writable")),
        })
        .from(service::edge::persistence::DeviceEntity::table_name(), "d")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::LinkEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"id">(), "l"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"execution">(), "l"),
                                 ruvia::db_binary_operator::equal,
                                 query.value(std::string_view{"edge"})),
                    ruvia::db_binary_operator::and_value,
                    query.unary(ruvia::db_unary_operator::is_null,
                                query.column(service::edge::persistence::LinkEntity::column_name<"deleted_at">(), "l")))),
            "l")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::DeviceModelEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"device_id">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"protocol">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.value(std::string_view{"Modbus"}))),
            "p")
        .join_function(ruvia::db_join_type::cross, itemSource, {}, "item",
                      {.lateral_ = true})
        .where(query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"edge_node_id">(), "l"),
                           ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId),
                                      ruvia::db_data_type::uuid)))
        .and_where(query.unary(ruvia::db_unary_operator::is_null,
                              query.column(service::edge::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
        .order_by(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"))
        .add_order_by(jsonText(query, item, "id"));
    return query;
}

inline ruvia::db_query appendS7Query(std::string_view nodeId) {
    ruvia::db_query query;
    const auto config = query.column(service::edge::persistence::DeviceModelEntity::column_name<"config">(), "p");
    const auto itemSource = query.call(
        "jsonb_array_elements",
        {query.coalesce({jsonGet(query, config, "areas"),
                         query.cast(query.value(std::string_view{"[]"}),
                                    ruvia::db_data_type::jsonb)})});
    const auto item = query.column("item");
    query
        .select({
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"), ruvia::db_data_type::text),
            jsonText(query, item, "id"),
            jsonText(query, item, "name"),
            nullableDefault(query, jsonText(query, item, "unit"), ""),
            jsonText(query, item, "area"),
            textDefault(query, jsonText(query, item, "dbNumber"), "0"),
            textDefault(query, jsonText(query, item, "start"), "0"),
            textDefault(query, jsonText(query, item, "startBit"), "0"),
            textDefault(query, jsonText(query, item, "size"), "1"),
            nullableDefault(query, jsonText(query, item, "dataType"), "BOOL"),
            textDefault(query, jsonText(query, item, "decimals"), "-1"),
            booleanText(query, jsonText(query, item, "writable")),
        })
        .from(service::edge::persistence::DeviceEntity::table_name(), "d")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::LinkEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"id">(), "l"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"execution">(), "l"),
                                 ruvia::db_binary_operator::equal,
                                 query.value(std::string_view{"edge"})),
                    ruvia::db_binary_operator::and_value,
                    query.unary(ruvia::db_unary_operator::is_null,
                                query.column(service::edge::persistence::LinkEntity::column_name<"deleted_at">(), "l")))),
            "l")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::DeviceModelEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"device_id">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"protocol">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.value(std::string_view{"S7"}))),
            "p")
        .join_function(ruvia::db_join_type::cross, itemSource, {}, "item",
                      {.lateral_ = true})
        .where(query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"edge_node_id">(), "l"),
                           ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId),
                                      ruvia::db_data_type::uuid)))
        .and_where(query.unary(ruvia::db_unary_operator::is_null,
                              query.column(service::edge::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
        .order_by(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"))
        .add_order_by(jsonText(query, item, "id"));
    return query;
}

inline ruvia::db_query appendIndustrialQuery(std::string_view nodeId) {
    ruvia::db_query query;
    const auto config = query.column(service::edge::persistence::DeviceModelEntity::column_name<"config">(), "p");
    const auto itemSource = query.call(
        "jsonb_array_elements",
        {query.coalesce({jsonGet(query, config, "points"),
                         query.cast(query.value(std::string_view{"[]"}),
                                    ruvia::db_data_type::jsonb)})});
    const auto item = query.column("item");
    query
        .select({
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"), ruvia::db_data_type::text),
            jsonText(query, item, "id"),
            jsonText(query, item, "name"),
            nullableDefault(query, jsonText(query, item, "unit"), ""),
            textDefault(query, jsonText(query, item, "area"), ""),
            textDefault(query, jsonText(query, item, "dataType"), "UINT16"),
            textDefault(query, jsonText(query, item, "byteOrder"), "BIG_ENDIAN"),
            textDefault(query, jsonText(query, item, "address"), "0"),
            textDefault(query, jsonText(query, item, "bit"), "0"),
            textDefault(query, jsonText(query, item, "scale"), "1"),
            textDefault(query, jsonText(query, item, "decimals"), "-1"),
            textDefault(query, jsonText(query, item, "identifier"), ""),
            textDefault(query, jsonText(query, item, "length"), "4"),
            textDefault(query, jsonText(query, item, "digits"), "2"),
            booleanText(query, jsonText(query, item, "writable")),
        })
        .from(service::edge::persistence::DeviceEntity::table_name(), "d")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::LinkEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"id">(), "l"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"execution">(), "l"),
                                 ruvia::db_binary_operator::equal,
                                 query.value(std::string_view{"edge"})),
                    ruvia::db_binary_operator::and_value,
                    query.unary(ruvia::db_unary_operator::is_null,
                                query.column(service::edge::persistence::LinkEntity::column_name<"deleted_at">(), "l")))),
            "l")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::DeviceModelEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"device_id">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"protocol">(), "p"),
                             ruvia::db_binary_operator::in,
                             query.list({query.value("MC"), query.value("FINS"), query.value("DLT645")}))),
            "p")
        .join_function(ruvia::db_join_type::cross, itemSource, {}, "item",
                      {.lateral_ = true})
        .where(query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"edge_node_id">(), "l"),
                           ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId),
                                      ruvia::db_data_type::uuid)))
        .and_where(query.unary(ruvia::db_unary_operator::is_null,
                              query.column(service::edge::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
        .order_by(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"))
        .add_order_by(jsonText(query, item, "id"));
    return query;
}

inline ruvia::db_query appendSl651FunctionsQuery(std::string_view nodeId) {
    ruvia::db_query query;
    const auto config = query.column(service::edge::persistence::DeviceModelEntity::column_name<"config">(), "p");
    const auto functionSource = query.call(
        "jsonb_array_elements",
        {query.coalesce({jsonGet(query, config, "funcs"),
                         query.cast(query.value(std::string_view{"[]"}),
                                    ruvia::db_data_type::jsonb)})});
    const auto function = query.column("func");
    query
        .select({query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"), ruvia::db_data_type::text),
                 jsonText(query, function, "funcCode"),
                 jsonText(query, function, "name"),
                 jsonText(query, function, "dir")})
        .from(service::edge::persistence::DeviceEntity::table_name(), "d")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::LinkEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"id">(), "l"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"execution">(), "l"),
                                 ruvia::db_binary_operator::equal,
                                 query.value(std::string_view{"edge"})),
                    ruvia::db_binary_operator::and_value,
                    query.unary(ruvia::db_unary_operator::is_null,
                                query.column(service::edge::persistence::LinkEntity::column_name<"deleted_at">(), "l")))),
            "l")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::DeviceModelEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"device_id">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"protocol">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.value(std::string_view{"SL651"}))),
            "p")
        .join_function(ruvia::db_join_type::cross, functionSource, {}, "func",
                      {.lateral_ = true})
        .where(query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"edge_node_id">(), "l"),
                           ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId),
                                      ruvia::db_data_type::uuid)))
        .and_where(query.unary(ruvia::db_unary_operator::is_null,
                              query.column(service::edge::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
        .order_by(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"))
        .add_order_by(jsonText(query, function, "funcCode"));
    return query;
}

inline ruvia::db_query appendSl651ElementsQuery(std::string_view nodeId) {
    ruvia::db_query query;
    const auto config = query.column(service::edge::persistence::DeviceModelEntity::column_name<"config">(), "p");
    const auto functionSource = query.call(
        "jsonb_array_elements",
        {query.coalesce({jsonGet(query, config, "funcs"),
                         query.cast(query.value(std::string_view{"[]"}),
                                    ruvia::db_data_type::jsonb)})});
    const auto function = query.column("func");
    ruvia::db_query elementRows;
    const auto elementSource = elementRows.call(
        "jsonb_array_elements",
        {elementRows.coalesce({
            jsonGet(elementRows, elementRows.column("func"), "elements"),
            elementRows.cast(elementRows.value(std::string_view{"[]"}),
                              ruvia::db_data_type::jsonb)})});
    elementRows
        .select({elementRows.alias(elementRows.column("value", "element_value"),
                                   "element"),
                 elementRows.alias(elementRows.cast(elementRows.value(false),
                                                    ruvia::db_data_type::boolean),
                                   "response_element")})
        .from_function(elementSource, "element_value",
                      {.lateral_ = false, .columns_ = {{.name_ = "value"}}});

    ruvia::db_query responseRows;
    const auto responseElementSource = responseRows.call(
        "jsonb_array_elements",
        {responseRows.coalesce({
            jsonGet(responseRows, responseRows.column("func"),
                    "responseElements"),
            responseRows.cast(responseRows.value(std::string_view{"[]"}),
                               ruvia::db_data_type::jsonb)})});
    responseRows
        .select({responseRows.alias(
                     responseRows.column("value", "response_value"),
                     "element"),
                 responseRows.alias(responseRows.cast(responseRows.value(true),
                                                       ruvia::db_data_type::boolean),
                                    "response_element")})
        .from_function(responseElementSource, "response_value",
                      {.lateral_ = false, .columns_ = {{.name_ = "value"}}});
    elementRows.combine(ruvia::db_set_operation::union_all, responseRows);
    const auto element = query.column("element", "values");
    const auto responseElement = query.column("response_element", "values");
    const auto functionCode = jsonText(query, function, "funcCode");

    query
        .select({
            query.cast(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"), ruvia::db_data_type::text),
            functionCode,
            jsonText(query, element, "id"),
            jsonText(query, element, "name"),
            nullableDefault(query, jsonText(query, element, "unit"), ""),
            jsonText(query, element, "encode"),
            textDefault(query, jsonText(query, element, "length"), "0"),
            textDefault(query, jsonText(query, element, "digits"), "0"),
            nullableDefault(query, jsonText(query, element, "guideHex"), ""),
            responseElement,
            query.binary(jsonText(query, function, "dir"),
                         ruvia::db_binary_operator::equal,
                         query.value(std::string_view{"DOWN"})),
            nullableDefault(query, jsonText(query, element, "positionMode"), "GUIDE"),
            textDefault(query, jsonText(query, element, "byteOffset"), "0"),
        })
        .from(service::edge::persistence::DeviceEntity::table_name(), "d")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::LinkEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"id">(), "l"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"link_id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"execution">(), "l"),
                                 ruvia::db_binary_operator::equal,
                                 query.value(std::string_view{"edge"})),
                    ruvia::db_binary_operator::and_value,
                    query.unary(ruvia::db_unary_operator::is_null,
                                query.column(service::edge::persistence::LinkEntity::column_name<"deleted_at">(), "l")))),
            "l")
        .join(
            ruvia::db_join_type::inner, service::edge::persistence::DeviceModelEntity::table_name(),
            query.binary(
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"device_id">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d")),
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::edge::persistence::DeviceModelEntity::column_name<"protocol">(), "p"),
                             ruvia::db_binary_operator::equal,
                             query.value(std::string_view{"SL651"}))),
            "p")
        .join_function(ruvia::db_join_type::cross, functionSource, {}, "func",
                      {.lateral_ = true})
        .join(ruvia::db_join_type::cross, elementRows, {}, "values",
              {.lateral_ = true})
        .where(query.binary(query.column(service::edge::persistence::LinkEntity::column_name<"edge_node_id">(), "l"),
                           ruvia::db_binary_operator::equal,
                           query.cast(query.value(nodeId),
                                      ruvia::db_data_type::uuid)))
        .and_where(query.unary(ruvia::db_unary_operator::is_null,
                              query.column(service::edge::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
        .order_by(query.column(service::edge::persistence::DeviceEntity::column_name<"id">(), "d"))
        .add_order_by(functionCode)
        .add_order_by(responseElement)
        .add_order_by(jsonText(query, element, "id"));
    return query;
}
} // namespace config::detail

class ConfigService final {
  public:
    template <typename Context>
    static ruvia::task<void> storeDebugPacket(Context& c, std::string_view nodeId, const pb::RawPacket& packet) {
        if (!packet.debug() || packet.endpoint_id().size() != 16 || (packet.payload().empty() && packet.acquisition_state().empty() && !packet.has_parsed_value()) ||
            packet.payload().size() > 4096 || (packet.direction() != "RX" &&
            packet.direction() != "TX" && packet.direction() != "TX_ATTEMPT" && packet.direction() != "RX_DROP")) co_return;
        const auto acquisitionId = protocol::debugAcquisitionId(nodeId, packet);
        const auto linkId = protocol::uuidText(packet.endpoint_id());
        const auto deviceId = packet.device_id().size() == 16 ? protocol::uuidText(packet.device_id()) : std::string{};
        ruvia::db_query node;
        node.select(node.column(persistence::EdgeNodeEntity::column_name<"name">()))
            .from(persistence::EdgeNodeEntity::table_name())
            .where((persistence::EdgeNodeEntity::column<"id">() == nodeId).expression(node));
        ruvia::db_query link;
        link.select({link.column(persistence::LinkEntity::column_name<"debug_enabled">()), link.subquery(node)})
            .from(persistence::LinkEntity::table_name())
            .where((persistence::LinkEntity::column<"id">() == linkId &&
                persistence::LinkEntity::column<"edge_node_id">() == nodeId &&
                persistence::LinkEntity::column<"deleted_at">().is_null()).expression(link));
        const auto links = co_await c.db().query(link);
        if (links.empty()) co_return;
        const bool captureLink = links.front()[0].value().value_or("") == "t" && !packet.device_only();
        bool captureDevice = false;
        if (!deviceId.empty()) {
            ruvia::db_query device;
            device.select(device.column(persistence::DeviceEntity::column_name<"debug_enabled">()))
                .from(persistence::DeviceEntity::table_name())
                .where((persistence::DeviceEntity::column<"id">() == deviceId &&
                    persistence::DeviceEntity::column<"link_id">() == linkId &&
                    persistence::DeviceEntity::column<"deleted_at">().is_null()).expression(device));
            const auto devices = co_await c.db().query(device);
            if (devices.empty()) co_return;
            captureDevice = devices.front()[0].value().value_or("") == "t";
        }
        if (!captureLink && !captureDevice) co_return;
        if (packet.payload().empty() && !packet.has_parsed_value() && packet.acquisition_state() != "running") {
            co_await packet_log::DebugPacketService::finishAcquisition(c.redis(),
                acquisitionId, packet.acquisition_state());
            co_return;
        }
        std::string parsedJson;
        if (packet.has_parsed_value()) {
            if (packet.direction() != "RX" || packet.packet_id().size() != 16 ||
                packet.parsed_value().ByteSizeLong() > 12288 || deviceId.empty()) co_return;
            pb::TelemetryRecord decoded;
            auto* value = decoded.add_values();
            *value = packet.parsed_value();
            if (value->element_id().empty()) co_return;
            decoded.set_protocol(pb::PROTOCOL_SL651); // Allows existing binary-value formatting; no wire decoding.
            parsedJson = protocol::TelemetryValues::telemetryJson(decoded);
        }
        co_await packet_log::DebugPacketService::recordPacket(c.redis(), *c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>(), linkId, deviceId,
            packet.direction(), "edge", packet.client_address(),
            std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(packet.payload().data()), packet.payload().size()),
            packet.observed_at_ms(), captureLink, captureDevice,
            packet.packet_id().size() == 16 ? std::string(nodeId) + ":" + protocol::uuidText(packet.packet_id()) : std::string{},
            packet.status(), packet.reason(), parsedJson,
            packet.reply_to_packet_id().size() == 16 ? std::string(nodeId) + ":" + protocol::uuidText(packet.reply_to_packet_id()) : std::string{},
            packet.has_parsed_value(), packet.payload_offset(), acquisitionId,
            nodeId, links.front()[1].value().value_or(""));
    }

    static ConfigService& instance() {
        static ConfigService value;
        return value;
    }

    ruvia::task<std::uint64_t> queueSnapshot(ruvia::web_worker_context& c,
                                             std::string_view nodeId,
                                             std::string_view actorId) {
        const auto version =
            co_await c.db().query(config::detail::queueSnapshotQuery(nodeId));
        if (version.empty())
            service::common::fail(17011, "边缘节点未批准或不支持设备配置", 409);
        const auto revision = unsignedInteger(version.front()[0].value().value_or(std::string_view{}));

        auto snapshot = co_await buildSnapshot(c, nodeId, revision);
        if (!snapshot) {
            service::common::fail(17012, "边缘节点配置条目超过 512 条", 409);
        }

        if (!service::common::isUuid(actorId))
            service::common::fail(10002, "invalid edge snapshot actor", 400);
        ruvia::db_query insert;
        insert
            .insert_into(service::edge::persistence::EdgeConfigRevisionEntity::table_name(),
                        {"node_id", "revision", "sha256", "item_count",
                         "created_by"})
            .values({
                insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                insert.value(static_cast<std::int64_t>(revision)),
                insert.value(snapshot->digest),
                insert.value(static_cast<std::int64_t>(snapshot->itemCount)),
                insert.cast(insert.value(actorId), ruvia::db_data_type::uuid),
            });
        (void)co_await c.db().execute(insert);
        co_await replaceQueue(c, nodeId, revision, snapshot->wires);
        co_await metadata::publishNode(c, nodeId);
        co_return revision;
    }

    template <typename Context>
    ruvia::task<bool> requeueIfStale(Context& c, std::string_view nodeId,
                                     std::uint64_t activeRevision) {
        const auto desired =
            co_await c.db().query(config::detail::requeueDesiredQuery(nodeId));
        if (desired.empty())
            co_return false;
        const auto revision = unsignedInteger(desired.front()[0].value().value_or(std::string_view{}));
        if (revision == 0 || revision <= activeRevision ||
            desired.front()[1].value().value_or(std::string_view{}) == "rejected")
            co_return false;

        auto snapshot = co_await buildSnapshot(c, nodeId, revision);
        if (!snapshot)
            co_return false;
        ruvia::db_query insert;
        insert
            .insert_into(service::edge::persistence::EdgeConfigRevisionEntity::table_name(),
                        {"node_id", "revision", "sha256", "item_count",
                         "created_by"})
            .values({
                insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                insert.value(static_cast<std::int64_t>(revision)),
                insert.value(snapshot->digest),
                insert.value(static_cast<std::int64_t>(snapshot->itemCount)),
                insert.null_value(),
            });
        ruvia::db_conflict_options conflict;
        conflict.columns_ = {"node_id", "revision"};
        conflict.update_ = {
            {"sha256", insert.excluded(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"sha256">())},
            {"item_count", insert.excluded(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"item_count">())},
            {"status", insert.value(std::string_view{"pending"})},
            {"message", insert.value(std::string_view{})},
            {"completed_at", insert.null_value()},
        };
        insert.on_conflict(conflict);
        (void)co_await c.db().execute(insert);
        (void)co_await c.db().execute(
            config::detail::requeuePendingQuery(nodeId, revision));
        co_await replaceQueue(c, nodeId, revision, snapshot->wires);
        co_await metadata::publishNode(c, nodeId);
        co_return true;
    }

  private:
    struct Snapshot {
        std::string digest;
        std::vector<std::string> wires;
        std::size_t itemCount{};
    };

    static std::uint64_t unsignedInteger(std::string_view value) {
        std::uint64_t result{};
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (error != std::errc{} || end != value.data() + value.size())
            throw std::runtime_error("invalid edge config integer");
        return result;
    }

    static std::int64_t integer(std::string_view value, std::int64_t fallback = 0) {
        if (value.empty())
            return fallback;
        std::int64_t result{};
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        return error == std::errc{} && end == value.data() + value.size() ? result : fallback;
    }

    static std::uint32_t positiveCeil(std::string_view value, double fallback = 300.0) {
        double parsed = config::detail::number(value, fallback);
        if (parsed < 1.0)
            parsed = 1.0;
        const auto maximum = static_cast<double>(std::numeric_limits<std::uint32_t>::max());
        if (parsed > maximum)
            return std::numeric_limits<std::uint32_t>::max();
        return static_cast<std::uint32_t>(std::ceil(parsed));
    }

    static pb::Protocol protocolValue(std::string_view value) {
        if (value == "Modbus")
            return pb::PROTOCOL_MODBUS;
        if (value == "S7")
            return pb::PROTOCOL_S7;
        if (value == "SL651")
            return pb::PROTOCOL_SL651;
        if (value == "MC") return pb::PROTOCOL_MC;
        if (value == "FINS") return pb::PROTOCOL_FINS;
        if (value == "DLT645") return pb::PROTOCOL_DLT645;
        return pb::PROTOCOL_UNSPECIFIED;
    }

    static bool setUuid(std::string* field, std::string_view text) {
        std::uint8_t value[16]{};
        if (!protocol::uuidBytes(text, value))
            return false;
        field->assign(protocol::bytes(value, sizeof(value)));
        return true;
    }

    static void packet(std::string* output, std::string_view mode, std::string_view content,
                       std::string_view name) {
        config::detail::packet(output, mode, content, name);
    }

    static bool encodeItem(const pb::ConfigItem& item, std::string& output) {
        const auto size = item.ByteSizeLong();
        if (size > static_cast<std::size_t>(std::numeric_limits<int>::max()))
            return false;
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
            size != output.size())
            throw std::runtime_error("SHA-256 failed");
        return output;
    }

    static std::array<std::uint8_t, 32>
    digestList(const std::vector<std::array<std::uint8_t, 32>>& values) {
        EVP_MD_CTX* context = EVP_MD_CTX_new();
        if (context == nullptr)
            throw std::runtime_error("SHA-256 context allocation failed");
        std::array<std::uint8_t, 32> output{};
        unsigned size{};
        bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1;
        for (const auto& value : values)
            ok = ok && EVP_DigestUpdate(context, value.data(), value.size()) == 1;
        ok = ok && EVP_DigestFinal_ex(context, output.data(), &size) == 1 &&
             size == output.size();
        EVP_MD_CTX_free(context);
        if (!ok)
            throw std::runtime_error("configuration SHA-256 failed");
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
        if (wire.empty())
            throw std::runtime_error("cannot encode edge configuration envelope");
        output.push_back(std::move(wire));
    }

    template <typename Context>
    static ruvia::task<std::optional<Snapshot>> buildSnapshot(Context& c,
                                                               std::string_view nodeId,
                                                               std::uint64_t revision) {
        auto items = co_await loadItems(c, nodeId);
        std::set<std::string> requiredProtocols;
        for (const auto& item : items) {
            if (!item.has_device()) continue;
            switch (item.device().protocol()) {
            case pb::PROTOCOL_MC: requiredProtocols.emplace("MC"); break;
            case pb::PROTOCOL_FINS: requiredProtocols.emplace("FINS"); break;
            case pb::PROTOCOL_DLT645: requiredProtocols.emplace("DLT645"); break;
            default: break;
            }
        }
        for (const auto& name : requiredProtocols) {
            ruvia::db_query capability;
            capability.select(capability.coalesce({capability.call("jsonb_exists", {
                config::detail::jsonGet(capability, capability.column(persistence::EdgeNodeEntity::column_name<"capability">()), "protocols"),
                capability.value(name)}), capability.value(false)}))
                .from(persistence::EdgeNodeEntity::table_name())
                .where(capability.binary(capability.column(persistence::EdgeNodeEntity::column_name<"id">()),
                    ruvia::db_binary_operator::equal, capability.cast(capability.value(nodeId), ruvia::db_data_type::uuid)));
            const auto rows = co_await c.db().query(capability);
            if (rows.empty() || rows.front()[0].value().value_or("") != "t") {
                co_await rejectBuild(c, nodeId, revision, "边缘固件尚未声明支持协议 " + name);
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
            if (!encodeItem(item, canonical))
                throw std::runtime_error("cannot encode edge config item");
            auto digest = sha256(canonical);
            item.set_sha256(protocol::bytes(digest.data(), digest.size()));
            itemDigests.push_back(digest);
        }
        const auto snapshotDigest = digestList(itemDigests);
        Snapshot snapshot;
        snapshot.digest = hex(snapshotDigest);
        snapshot.itemCount = items.size();
        snapshot.wires.reserve(items.size() + 2);

        auto begin = service::edge::protocol::outbound(c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
        auto* configBegin = begin.mutable_config_begin();
        configBegin->set_revision(revision);
        configBegin->set_item_count(static_cast<std::uint32_t>(items.size()));
        configBegin->set_sha256(
            protocol::bytes(snapshotDigest.data(), snapshotDigest.size()));
        appendWire(snapshot.wires, begin);
        for (const auto& item : items) {
            auto envelope = service::edge::protocol::outbound(c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
            *envelope.mutable_config_item() = item;
            appendWire(snapshot.wires, envelope);
        }
        auto commit = service::edge::protocol::outbound(c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
        auto* configCommit = commit.mutable_config_commit();
        configCommit->set_revision(revision);
        configCommit->set_sha256(
            protocol::bytes(snapshotDigest.data(), snapshotDigest.size()));
        appendWire(snapshot.wires, commit);
        co_return snapshot;
    }

    template <typename Context>
    static ruvia::task<void> replaceQueue(Context& c, std::string_view nodeId,
                                          std::uint64_t revision,
                                          const std::vector<std::string>& wires) {
        const std::string key = "iot:edge:config:" + std::string(nodeId);
        const std::string revisionKey = "iot:edge:config-revision:" + std::string(nodeId);
        const std::array<std::string_view, 2> keys{key, revisionKey};
        const auto revisionText = std::to_string(revision);
        std::vector<std::string_view> values;
        values.reserve(wires.size() + 1);
        values.push_back(revisionText);
        for (const auto& wire : wires)
            values.push_back(wire);
        (void)co_await c.redis().eval(config::detail::kReplaceQueueScript,
                                      std::span<const std::string_view>(keys),
                                      std::span<const std::string_view>(values));
        co_await dispatch::notifyNode(c.redis(), nodeId);
    }

    template <typename Context>
    static ruvia::task<void> rejectBuild(Context& c, std::string_view nodeId,
                                         std::uint64_t revision, std::string_view message) {
        (void)co_await c.db().execute(
            config::detail::rejectBuildQuery(message, nodeId, revision));
    }

  public:
    template <typename Context>
    static ruvia::task<std::vector<pb::ConfigItem>>
    loadItems(Context& c, std::string_view nodeId,
              std::map<std::string, std::string>* calculations = nullptr) {
        std::vector<pb::ConfigItem> items;
        ruvia::db_query dtu;
        dtu.select(dtu.column(persistence::EdgeDtuEntity::column_name<"wire_hex">()))
            .from(persistence::EdgeDtuEntity::table_name()).where(dtu.binary(dtu.column(persistence::EdgeDtuEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                dtu.cast(dtu.value(nodeId), ruvia::db_data_type::uuid))).order_by(dtu.column(persistence::EdgeDtuEntity::column_name<"channel_id">()));
        const auto channels = co_await c.db().query(dtu);
        if (!channels.empty()) {
            ruvia::db_query capability;
            capability.select(config::detail::jsonText(capability, capability.column(persistence::EdgeNodeEntity::column_name<"capability">()), "dtu"))
                .from(persistence::EdgeNodeEntity::table_name()).where(capability.binary(capability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                    capability.cast(capability.value(nodeId), ruvia::db_data_type::uuid)));
            const auto supported = co_await c.db().query(capability);
            if (supported.empty() || supported.front()[0].value().value_or("") != "true")
                throw std::runtime_error("边缘固件尚未声明支持 DTU 透传，不能下发透传配置");
        }
        for (const auto& row : channels) {
            const auto encoded = row[0].value().value_or("");
            std::string wire;
            for (std::size_t i = 0; i + 1 < encoded.size(); i += 2) {
                const int high = service::common::hexDigit(encoded[i]), low = service::common::hexDigit(encoded[i+1]);
                if (high < 0 || low < 0) throw std::runtime_error("invalid DTU configuration encoding");
                wire.push_back(static_cast<char>((high << 4) | low));
            }
            pb::ConfigItem item;
            item.set_kind(pb::CONFIG_ITEM_DTU);
            if (!item.mutable_dtu()->ParseFromString(wire)) throw std::runtime_error("invalid DTU configuration");
            items.push_back(std::move(item));
        }
        const auto devices =
            co_await c.db().query(config::detail::buildItemsQuery(nodeId));
        if (calculations) for (const auto& row : devices)
            (*calculations)[std::string(row[0].value().value_or(""))] =
                std::string(row[54].value().value_or("{}"));
        ruvia::db_query capability;
        capability.select(config::detail::jsonText(capability, capability.column(persistence::EdgeNodeEntity::column_name<"capability">()), "derivedPoints"))
            .from(persistence::EdgeNodeEntity::table_name()).where(capability.binary(capability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                capability.cast(capability.value(nodeId), ruvia::db_data_type::uuid)));
        const auto supported = co_await c.db().query(capability);
        const bool derivedSupported = !supported.empty() && supported.front()[0].value().value_or("") == "true";
        appendConfiguredDevices(items, devices, derivedSupported);

        co_await appendModbus(c, nodeId, items);
        co_await appendS7(c, nodeId, items);
        co_await appendIndustrial(c, nodeId, items);
        co_await appendSl651(c, nodeId, items);
        co_return items;
    }

  private:
    template <typename Rows>
    static void appendConfiguredDevices(std::vector<pb::ConfigItem>& items, const Rows& devices, bool derivedSupported) {
        std::set<std::string> endpoints;
        for (const auto& row : devices) {
            const auto protocol = protocolValue(row[3].value().value_or(std::string_view{}));
            pb::ConfigItem endpoint;
            endpoint.set_kind(pb::CONFIG_ITEM_ENDPOINT);
            auto* endpointValue = endpoint.mutable_endpoint();
            if (!setUuid(endpointValue->mutable_endpoint_id(), row[30].value().value_or(std::string_view{})))
                throw std::runtime_error("invalid edge link UUID");
            endpointValue->set_name(row[33].value().value_or(std::string_view{}));
            endpointValue->set_interface_name(row[10].value().value_or(std::string_view{}));
            endpointValue->set_protocol(protocol);
            endpointValue->set_debug_enabled(row[36].value().value_or("") == "t");
            endpointValue->set_enabled(row[34].value().value_or(std::string_view{}) == "t");
            if (row[9].value().value_or(std::string_view{}) == "serial") {
                endpointValue->set_transport(pb::TRANSPORT_SERIAL);
                endpointValue->set_mode(pb::LINK_MODE_SERIAL);
                auto* serial = endpointValue->mutable_serial();
                serial->set_channel(row[10].value().value_or(std::string_view{}));
                serial->set_baud_rate(
                    static_cast<std::uint32_t>(integer(row[14].value().value_or(std::string_view{}), 9600)));
                serial->set_data_bits(
                    static_cast<std::uint32_t>(integer(row[15].value().value_or(std::string_view{}), 8)));
                serial->set_stop_bits(
                    static_cast<std::uint32_t>(integer(row[16].value().value_or(std::string_view{}), 1)));
                serial->set_parity(row[17].value().value_or(std::string_view{}));
            } else {
                endpointValue->set_transport(pb::TRANSPORT_ETHERNET);
                endpointValue->set_mode(row[11].value().value_or(std::string_view{}) == "TCP Server"
                                            ? pb::LINK_MODE_TCP_SERVER
                                            : pb::LINK_MODE_TCP_CLIENT);
                endpointValue->set_ip(row[12].value().value_or(std::string_view{}));
                endpointValue->set_port(
                    static_cast<std::uint32_t>(integer(row[13].value().value_or(std::string_view{}))));
            }
            if (endpoints.emplace(row[30].value().value_or("")).second)
                items.push_back(std::move(endpoint));

            pb::ConfigItem device;
            device.set_kind(pb::CONFIG_ITEM_DEVICE);
            auto* deviceValue = device.mutable_device();
            setUuid(deviceValue->mutable_device_id(), row[0].value().value_or(std::string_view{}));
            setUuid(deviceValue->mutable_endpoint_id(), row[30].value().value_or(std::string_view{}));
            deviceValue->set_device_code(row[2].value().value_or(std::string_view{}));
            deviceValue->set_name(row[1].value().value_or(std::string_view{}));
            deviceValue->set_protocol(protocol);
            if (protocol == pb::PROTOCOL_MC || protocol == pb::PROTOCOL_FINS || protocol == pb::PROTOCOL_DLT645) {
                auto* connection = deviceValue->mutable_industrial();
                connection->set_mc_four_e(row[38].value().value_or("3E") == "4E");
                connection->set_dlt645_version(static_cast<std::uint32_t>(integer(row[39].value().value_or("2007"), 2007)));
                connection->set_mc_network(static_cast<std::uint32_t>(integer(row[40].value().value_or("0"), 0)));
                connection->set_mc_station(static_cast<std::uint32_t>(integer(row[41].value().value_or("255"), 255)));
                connection->set_mc_module_io(static_cast<std::uint32_t>(integer(row[42].value().value_or("1023"), 1023)));
                connection->set_mc_multidrop(static_cast<std::uint32_t>(integer(row[43].value().value_or("0"), 0)));
                connection->set_mc_monitoring_timer(static_cast<std::uint32_t>(integer(row[44].value().value_or("16"), 16)));
                connection->set_fins_destination_network(static_cast<std::uint32_t>(integer(row[45].value().value_or("0"), 0)));
                connection->set_fins_destination_node(static_cast<std::uint32_t>(integer(row[46].value().value_or("0"), 0)));
                connection->set_fins_destination_unit(static_cast<std::uint32_t>(integer(row[47].value().value_or("0"), 0)));
                connection->set_fins_source_network(static_cast<std::uint32_t>(integer(row[48].value().value_or("0"), 0)));
                connection->set_fins_source_node(static_cast<std::uint32_t>(integer(row[49].value().value_or("0"), 0)));
                connection->set_fins_source_unit(static_cast<std::uint32_t>(integer(row[50].value().value_or("0"), 0)));
                connection->set_dlt645_wakeup_bytes(static_cast<std::uint32_t>(integer(row[51].value().value_or("4"), 4)));
                packet(connection->mutable_dlt645_write_password(), "HEX", row[52].value().value_or(""), "meter password");
                packet(connection->mutable_dlt645_operator_code(), "HEX", row[53].value().value_or(""), "meter operator");
            }
            deviceValue->set_debug_enabled(row[37].value().value_or("") == "t");
            deviceValue->set_timezone(row[4].value().value_or(std::string_view{}));
            if (protocol == pb::PROTOCOL_SL651) {
                const auto mode = row[35].value().value_or(std::string_view{"M1"});
                if (mode.size() != 2 || mode[0] != 'M' || mode[1] < '1' || mode[1] > '4')
                    throw std::invalid_argument("invalid SL651 response mode");
                deviceValue->set_sl651_response_mode(static_cast<std::uint32_t>(mode[1] - '0'));
            }
            // Zero selects the configured read/report interval on updated firmware.
            // Legacy firmware accepts zero and retains its old one-second scheduler,
            // so rollout does not reject the entire device configuration.
            deviceValue->set_io_interval_ms(0);
            deviceValue->set_report_interval_sec(positiveCeil(row[5].value().value_or(std::string_view{})));
            deviceValue->set_online_timeout_sec(
                static_cast<std::uint32_t>(integer(row[6].value().value_or(std::string_view{}), 300)));
            deviceValue->set_modbus_slave_id(
                static_cast<std::uint32_t>(integer(row[7].value().value_or(std::string_view{}), 1)));
            deviceValue->set_modbus_mode(row[8].value().value_or(std::string_view{}));
            deviceValue->set_modbus_merge_gap(
                static_cast<std::uint32_t>(integer(row[19].value().value_or(std::string_view{}))));
            deviceValue->set_modbus_max_quantity(
                static_cast<std::uint32_t>(integer(row[20].value().value_or(std::string_view{}), 125)));
            deviceValue->set_s7_connection_mode(row[21].value().value_or(std::string_view{}));
            deviceValue->set_s7_connection_type(row[22].value().value_or(std::string_view{}));
            deviceValue->set_s7_rack(
                static_cast<std::uint32_t>(integer(row[23].value().value_or(std::string_view{}))));
            deviceValue->set_s7_slot(
                static_cast<std::uint32_t>(integer(row[24].value().value_or(std::string_view{}), 1)));
            deviceValue->set_s7_local_tsap(row[25].value().value_or(std::string_view{}));
            deviceValue->set_s7_remote_tsap(row[26].value().value_or(std::string_view{}));
            deviceValue->set_command_fast_read_duration_sec(static_cast<std::uint32_t>(
                std::clamp<std::int64_t>(integer(row[31].value().value_or(std::string_view{}), 60),
                                         0, 3600)));
            deviceValue->set_command_fast_read_interval_sec(static_cast<std::uint32_t>(
                std::clamp<std::int64_t>(integer(row[32].value().value_or(std::string_view{}), 1),
                                         1, 3600)));
            packet(deviceValue->mutable_heartbeat_payload(), row[27].value().value_or(std::string_view{}), row[28].value().value_or(std::string_view{}),
                   "heartbeat_payload");
            deviceValue->set_enabled(row[29].value().value_or(std::string_view{}) == "t");
            items.push_back(std::move(device));
            const auto configuration = ruvia::json_value::parse(row[54].value().value_or("{}"));
            if (!configuration) throw std::runtime_error("invalid device model configuration");
            for (const auto& point : service::common::orderDerivedPoints(*configuration)) {
                if (!derivedSupported) throw std::runtime_error("边缘固件尚不支持派生点，请先升级固件");
                pb::ConfigItem derived;
                derived.set_kind(pb::CONFIG_ITEM_DERIVED_POINT);
                auto* value = derived.mutable_derived_point();
                setUuid(value->mutable_device_id(), row[0].value().value_or(""));
                value->set_point_id(point.id); value->set_name(point.name); value->set_unit(point.unit);
                value->set_kind(point.kind); value->set_expression(point.expression); value->set_boolean_result(point.valueType == "boolean");
                value->set_source_alias(point.sourceAlias); value->set_window_seconds(static_cast<std::uint32_t>(point.windowSeconds));
                value->set_max_age_seconds(static_cast<std::uint32_t>(point.maxAgeSeconds)); value->set_hidden(!point.visible);
                for (const auto& [alias, id] : point.inputs) { auto* input = value->add_inputs(); input->set_alias(alias); input->set_point_id(id); }
                for (const auto& rule : point.unitRules) { auto* unit = value->add_unit_rules(); unit->set_condition(rule.condition); unit->set_unit(rule.unit); }
                items.push_back(std::move(derived));
            }
        }
    }

    template <typename Context>
    static ruvia::task<void> appendModbus(Context& c, std::string_view nodeId,
                                           std::vector<pb::ConfigItem>& items) {
        const auto rows =
            co_await c.db().query(config::detail::appendModbusQuery(nodeId));
        for (const auto& row : rows) {
            pb::ConfigItem item;
            item.set_kind(pb::CONFIG_ITEM_MODBUS_REGISTER);
            auto* value = item.mutable_modbus_register();
            setUuid(value->mutable_device_id(), row[0].value().value_or(std::string_view{}));
            value->set_element_id(row[1].value().value_or(std::string_view{}));
            value->set_name(row[2].value().value_or(std::string_view{}));
            value->set_unit(row[3].value().value_or(std::string_view{}));
            value->set_register_type(row[4].value().value_or(std::string_view{}));
            value->set_data_type(row[5].value().value_or(std::string_view{}));
            value->set_byte_order(row[6].value().value_or(std::string_view{}));
            value->set_address(static_cast<std::uint32_t>(integer(row[7].value().value_or(std::string_view{}))));
            value->set_quantity(
                static_cast<std::uint32_t>(integer(row[8].value().value_or(std::string_view{}), 1)));
            value->set_scale(config::detail::number(row[9].value().value_or(std::string_view{}), 1.0));
            value->set_decimals(
                static_cast<std::int32_t>(integer(row[10].value().value_or(std::string_view{}), -1)));
            value->set_writable(row[11].value().value_or(std::string_view{}) == "t");
            items.push_back(std::move(item));
        }
    }

    template <typename Context>
    static ruvia::task<void> appendS7(Context& c, std::string_view nodeId,
                                       std::vector<pb::ConfigItem>& items) {
        const auto rows =
            co_await c.db().query(config::detail::appendS7Query(nodeId));
        for (const auto& row : rows) {
            pb::ConfigItem item;
            item.set_kind(pb::CONFIG_ITEM_S7_AREA);
            auto* value = item.mutable_s7_area();
            setUuid(value->mutable_device_id(), row[0].value().value_or(std::string_view{}));
            value->set_element_id(row[1].value().value_or(std::string_view{}));
            value->set_name(row[2].value().value_or(std::string_view{}));
            value->set_unit(row[3].value().value_or(std::string_view{}));
            value->set_area(row[4].value().value_or(std::string_view{}));
            value->set_db_number(
                static_cast<std::uint32_t>(integer(row[5].value().value_or(std::string_view{}))));
            value->set_start(static_cast<std::uint32_t>(integer(row[6].value().value_or(std::string_view{}))));
            value->set_start_bit(
                static_cast<std::uint32_t>(integer(row[7].value().value_or(std::string_view{}))));
            value->set_size(
                static_cast<std::uint32_t>(integer(row[8].value().value_or(std::string_view{}), 1)));
            value->set_data_type(row[9].value().value_or(std::string_view{}));
            value->set_scale(1.0);
            value->set_decimals(
                static_cast<std::int32_t>(integer(row[10].value().value_or(std::string_view{}), -1)));
            value->set_writable(row[11].value().value_or(std::string_view{}) == "t");
            items.push_back(std::move(item));
        }
    }

    template <typename Context>
    static ruvia::task<void> appendIndustrial(Context& c, std::string_view nodeId,
                                             std::vector<pb::ConfigItem>& items) {
        const auto rows = co_await c.db().query(config::detail::appendIndustrialQuery(nodeId));
        for (const auto& row : rows) {
            pb::ConfigItem item;
            item.set_kind(pb::CONFIG_ITEM_INDUSTRIAL_POINT);
            auto* value = item.mutable_industrial_point();
            setUuid(value->mutable_device_id(), row[0].value().value_or(""));
            value->set_element_id(row[1].value().value_or(""));
            value->set_name(row[2].value().value_or(""));
            value->set_unit(row[3].value().value_or(""));
            value->set_area(row[4].value().value_or(""));
            value->set_data_type(row[5].value().value_or(""));
            value->set_byte_order(row[6].value().value_or(""));
            value->set_address(static_cast<std::uint32_t>(integer(row[7].value().value_or("0"))));
            value->set_bit(static_cast<std::uint32_t>(integer(row[8].value().value_or("0"))));
            value->set_scale(config::detail::number(row[9].value().value_or("1"), 1));
            value->set_decimals(static_cast<std::int32_t>(integer(row[10].value().value_or("-1"), -1)));
            value->set_identifier(row[11].value().value_or(""));
            value->set_length(static_cast<std::uint32_t>(integer(row[12].value().value_or("4"), 4)));
            value->set_digits(static_cast<std::uint32_t>(integer(row[13].value().value_or("2"), 2)));
            value->set_writable(row[14].value().value_or("") == "t");
            items.push_back(std::move(item));
        }
    }

    template <typename Context>
    static ruvia::task<void> appendSl651(Context& c, std::string_view nodeId,
                                          std::vector<pb::ConfigItem>& items) {
        const auto functions = co_await c.db().query(
            config::detail::appendSl651FunctionsQuery(nodeId));
        for (const auto& row : functions) {
            pb::ConfigItem item;
            item.set_kind(pb::CONFIG_ITEM_SL651_FUNCTION);
            auto* value = item.mutable_sl651_function();
            setUuid(value->mutable_device_id(), row[0].value().value_or(std::string_view{}));
            value->set_function_code(row[1].value().value_or(std::string_view{}));
            value->set_name(row[2].value().value_or(std::string_view{}));
            value->set_direction(row[3].value().value_or(std::string_view{}));
            items.push_back(std::move(item));
        }

        const auto elements = co_await c.db().query(
            config::detail::appendSl651ElementsQuery(nodeId));
        for (const auto& row : elements) {
            pb::ConfigItem item;
            item.set_kind(pb::CONFIG_ITEM_SL651_ELEMENT);
            auto* value = item.mutable_sl651_element();
            setUuid(value->mutable_device_id(), row[0].value().value_or(std::string_view{}));
            value->set_function_code(row[1].value().value_or(std::string_view{}));
            value->set_element_id(row[2].value().value_or(std::string_view{}));
            value->set_name(row[3].value().value_or(std::string_view{}));
            value->set_unit(row[4].value().value_or(std::string_view{}));
            value->set_encoding(row[5].value().value_or(std::string_view{}));
            value->set_length(
                static_cast<std::uint32_t>(integer(row[6].value().value_or(std::string_view{}))));
            value->set_digits(
                static_cast<std::uint32_t>(integer(row[7].value().value_or(std::string_view{}))));
            const auto guide =
                config::detail::packetBytes("HEX", row[8].value().value_or(std::string_view{}), "sl651_guide");
            value->set_guide(protocol::bytes(guide.data(), guide.size()));
            value->set_fixed_position(row[11].value().value_or(std::string_view{}) == "OFFSET");
            value->set_byte_offset(static_cast<std::uint32_t>(integer(row[12].value().value_or(std::string_view{}))));
            value->set_response_element(row[9].value().value_or(std::string_view{}) == "t");
            value->set_writable(row[10].value().value_or(std::string_view{}) == "t" && !value->response_element());
            items.push_back(std::move(item));
        }
    }
};

inline ConfigService& configService() { return ConfigService::instance(); }

} // namespace service::edge

#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <iostream>
#include <memory>

#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/features/telemetry/telemetry.service.h"

namespace service::edge {

inline constexpr std::string_view kEdgeIngressGroup{"iot-engine:edge-projector"};

inline bool validVpnPublicKey(std::string_view value) noexcept {
    return value.size() == 44 && value.back() == '=' &&
           std::all_of(value.begin(), value.end(), [](unsigned char character) {
               return (character >= 'A' && character <= 'Z') ||
                      (character >= 'a' && character <= 'z') ||
                      (character >= '0' && character <= '9') || character == '+' ||
                      character == '/' || character == '=';
           });
}

class EdgeProjectionService {
  protected:
    struct AcquisitionConfiguration final {
        std::vector<pb::ConfigItem> items;
        std::map<std::string, std::string> calculations;
    };
    std::map<std::string, AcquisitionConfiguration> acquisitionConfigurations_;

    template <typename Transaction>
    static ruvia::task<bool> claimFirmwareCleanup(Transaction& transaction) {
        ruvia::db_query lock;
        lock.select(lock.call("pg_try_advisory_xact_lock", {
            lock.cast(lock.value(17011), ruvia::db_data_type::integer),
            lock.cast(lock.value(1), ruvia::db_data_type::integer)}));
        const auto rows = co_await transaction.query(lock);
        co_return !rows.empty() && rows.front()[0].value().value_or(std::string_view{}) == "t";
    }

    static bool managedFirmwareFile(const std::filesystem::path& path, const std::filesystem::path& directory) {
        return path.parent_path() == directory && path.extension() == ".bin" &&
               service::common::isUuid(path.stem().string());
    }

    static ruvia::task<void> cleanupFirmwares(ruvia::web_worker_context& context, const std::filesystem::path& directory) {
        using Firmware = service::edge::persistence::EdgeFirmwareEntity;
        using Task = service::edge::persistence::EdgeTaskEntity;
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        auto transaction = co_await context.db().begin_transaction();
        if (!co_await claimFirmwareCleanup(transaction))
            co_return;
        ruvia::db_query active;
        const auto cutoff = active.binary(active.call("now"), Op::subtract,
            active.cast(active.value("1 hour"), Type::interval));
        active.select(active.value(1)).from(Task::table_name(), "task")
            .where(active.binary(active.column(Task::column_name<"task_type">(), "task"), Op::equal, active.value("firmware")))
            .and_where(active.binary(
                active.binary(active.column(Task::column_name<"request">(), "task"), Op::json_get_text, active.value("firmware_id")),
                Op::equal, active.cast(active.column(Firmware::column_name<"id">(), "firmware"), Type::text)))
            .and_where(active.binary(
                active.binary(active.column(Task::column_name<"status">(), "task"), Op::not_in,
                    active.list({active.value("succeeded"), active.value("failed")})), Op::or_value,
                active.binary(active.coalesce({active.column(Task::column_name<"completed_at">(), "task"),
                    active.column(Task::column_name<"updated_at">(), "task"), active.column(Task::column_name<"created_at">(), "task")}),
                    Op::greater_equal, cutoff)));
        ruvia::db_query candidates;
        candidates.select({candidates.cast(candidates.column(Firmware::column_name<"id">(), "firmware"), Type::text),
                           candidates.column(Firmware::column_name<"storage_path">(), "firmware")})
            .from(Firmware::table_name(), "firmware")
            .where(candidates.binary(candidates.column(Firmware::column_name<"created_at">(), "firmware"), Op::less,
                candidates.binary(candidates.call("now"), Op::subtract, candidates.cast(candidates.value("1 hour"), Type::interval))))
            .and_where(candidates.unary(ruvia::db_unary_operator::not_value, candidates.exists(active)))
            .order_by(candidates.column(Firmware::column_name<"created_at">(), "firmware"))
            .limit(32);
        const auto rows = co_await transaction.query(candidates);
        std::vector<std::filesystem::path> removedPaths;
        for (const auto& row : rows) {
            const auto id = row[0].value().value_or(std::string_view{});
            const auto path = std::filesystem::absolute(std::filesystem::path(std::string(row[1].value().value_or(std::string_view{})))).lexically_normal();
            if (!managedFirmwareFile(path, directory))
                continue;
            ruvia::db_query removal;
            removal.delete_from(Firmware::table_name()).where(removal.binary(removal.column(Firmware::column_name<"id">()),
                Op::equal, removal.cast(removal.value(id), Type::uuid)));
            (void)co_await transaction.execute(removal);
            removedPaths.push_back(path);
        }
        // 先提交删除记录；提交结果不确定时不删除文件，下一次按孤立文件回收。
        co_await transaction.commit();
        for (const auto& path : removedPaths) {
            std::error_code error;
            const auto status = std::filesystem::symlink_status(path, error);
            if (error == std::errc::no_such_file_or_directory)
                continue;
            if (error)
                throw std::filesystem::filesystem_error("inspect expired firmware", path, error);
            if (std::filesystem::is_regular_file(status))
                std::filesystem::remove(path);
        }
        std::error_code error;
        if (!std::filesystem::exists(directory, error)) {
            if (error)
                throw std::filesystem::filesystem_error("inspect firmware directory", directory, error);
            co_return;
        }
        // 数据库未引用的最终文件来自中断/不确定的上传提交。只回收自有命名、
        // 超过一小时的普通文件；活动 .upload 文件仍由上传恢复锁保护。
        auto orphanTransaction = co_await context.db().begin_transaction();
        if (!co_await claimFirmwareCleanup(orphanTransaction))
            co_return;
        ruvia::db_query references;
        references.select(references.column(Firmware::column_name<"storage_path">())).from(Firmware::table_name());
        const auto referenced = co_await orphanTransaction.query(references);
        std::unordered_set<std::string> paths;
        for (const auto& row : referenced)
            paths.insert(std::filesystem::absolute(std::filesystem::path(std::string(row[0].value().value_or(std::string_view{})))).lexically_normal().string());
        const auto oldest = std::filesystem::file_time_type::clock::now() - std::chrono::hours(1);
        std::size_t removed{};
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            const auto path = entry.path().lexically_normal();
            if (!managedFirmwareFile(path, directory) || paths.contains(path.string()) ||
                !std::filesystem::is_regular_file(entry.symlink_status()) || entry.last_write_time() >= oldest)
                continue;
            std::filesystem::remove(path);
            if (++removed == 32)
                break;
        }
        co_await orphanTransaction.commit();
    }

    static ruvia::task<void> hydrateAuth(ruvia::web_worker_context& context) {
        ruvia::db_query query;
        query.select({query.column(service::edge::persistence::EdgeNodeEntity::column_name<"imei">()),
                      query.cast(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_data_type::text),
                      query.column(service::edge::persistence::EdgeNodeEntity::column_name<"enrollment_status">())})
            .from(service::edge::persistence::EdgeNodeEntity::table_name());
        const auto rows = co_await context.db().query(query);
        if (rows.empty())
            co_return;
        auto pipeline = context.redis().pipeline();
        for (const auto& row : rows) {
            const auto key = protocol::authKey(row[0].value().value_or(std::string_view{}));
            const auto value = std::string(row[1].value().value_or(std::string_view{})) + "|" + std::string(row[2].value().value_or(std::string_view{}));
            pipeline.set(key, value);
        }
        const auto replies = co_await std::move(pipeline).exec();
        service::message::redis::requirePipelineSuccess("hydrate edge authorization", replies);
    }

    ruvia::task<void> project(
        ruvia::web_worker_context& context, metadata::Catalog& catalog,
        std::string_view wire,
        std::string_view receivedAtText,
        std::vector<service::message::StreamMessage>& telemetry,
        std::vector<persistence::TelemetryUploadRecord>& completedUploads,
        std::map<std::string, std::string>& rawCalculations) {
        pb::Envelope envelope;
        if (!protocol::decode(wire, envelope))
            co_return;
        const auto receivedAt = service::utils::parseInt64(
            receivedAtText.empty() ? std::nullopt
                                   : std::optional<std::string_view>(receivedAtText));
        const auto receivedAtMs = receivedAt.value_or(service::message::utcNowMilliseconds());
        if (envelope.payload_case() == pb::Envelope::kHello) {
            co_await saveHello(context, envelope.hello());
            co_return;
        }
        if (envelope.node_id().size() != 16)
            co_return;
        const auto nodeId = protocol::uuidText(envelope.node_id());
        if (envelope.has_telemetry_batch() || envelope.has_command_result())
            catalog[nodeId] = co_await metadata::loadNode(context.redis(),nodeId);
        switch (envelope.payload_case()) {
        case pb::Envelope::kHeartbeat:
            co_await saveHeartbeat(context, nodeId, envelope.heartbeat());
            break;
        case pb::Envelope::kDtuStatus: {
            const auto& status = envelope.dtu_status();
            if (status.channel_id().size() != 16) break;
            std::string json;
            if (!google::protobuf::util::MessageToJsonString(status, &json).ok()) break;
            auto withoutDebug = status;
            withoutDebug.clear_traces();
            withoutDebug.clear_omitted_traces();
            std::string statusOnly;
            if (!google::protobuf::util::MessageToJsonString(withoutDebug, &statusOnly).ok()) break;
            ruvia::db_query update;
            const auto projected = update.case_when({{update.binary(
                config::detail::jsonText(update, update.column(persistence::EdgeDtuEntity::column_name<"config">()), "debugEnabled"),
                ruvia::db_binary_operator::equal, update.value("true")), update.cast(update.value(json), ruvia::db_data_type::jsonb)}},
                update.cast(update.value(statusOnly), ruvia::db_data_type::jsonb));
            update.update(persistence::EdgeDtuEntity::table_name())
                .set(persistence::EdgeDtuEntity::column_name<"status">(), projected)
                .where(update.binary(update.column(persistence::EdgeDtuEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                    update.cast(update.value(nodeId), ruvia::db_data_type::uuid)))
                .and_where(update.binary(update.column(persistence::EdgeDtuEntity::column_name<"channel_id">()), ruvia::db_binary_operator::equal,
                    update.cast(update.value(protocol::uuidText(status.channel_id())), ruvia::db_data_type::uuid)))
                .and_where(update.binary(update.column(persistence::EdgeDtuEntity::column_name<"status">()), ruvia::db_binary_operator::not_equal,
                    projected));
            (void)co_await context.db().execute(update);
            break;
        }
        case pb::Envelope::kCapabilityReport:
            co_await saveCapabilities(context, nodeId, envelope.capability_report());
            break;
        case pb::Envelope::kNetworkConfigResult:
            co_await saveNetworkResult(context, nodeId, envelope.network_config_result());
            break;
        case pb::Envelope::kVpnConfigResult:
            co_await saveVpnResult(context, nodeId, envelope.vpn_config_result());
            break;
        case pb::Envelope::kFirmwareUpdateResult:
            co_await saveFirmwareResult(context, nodeId, envelope.firmware_update_result());
            break;
        case pb::Envelope::kModemControlResult:
            co_await saveModemResult(context, nodeId, envelope.modem_control_result());
            break;
        case pb::Envelope::kPlatformConfigResult:
            co_await savePlatformResult(context, nodeId, envelope.platform_config_result());
            break;
        case pb::Envelope::kConfigApplied:
            co_await saveConfigApplied(context, nodeId, envelope.config_applied());
            break;
        case pb::Envelope::kConfigRejected:
            co_await saveConfigRejected(context, nodeId, envelope.config_rejected());
            break;
        case pb::Envelope::kTelemetryBatch: {
            for (const auto& record : envelope.telemetry_batch().records()) {
                if (record.has_device_status() &&
                    record.device_status().device_id() == record.device_id()) {
                    pb::DeviceStatusReport report;
                    *report.add_devices() = record.device_status();
                    co_await saveDeviceStatus(context, nodeId, report);
                }
            }
            co_await ensureMetadata(context, catalog, nodeId,
                                    envelope.telemetry_batch());
            pb::TelemetryBatch assembled;
            for (const auto& record : envelope.telemetry_batch().records()) {
                if (record.part_count() == 0 && record.part_index() == 0 && record.report_id().empty()) {
                    *assembled.add_records() = record;
                    continue;
                }
                const auto complete = co_await storeTelemetryPart(context.redis(), nodeId, record);
                if (complete) {
                    *assembled.add_records() = *complete;
                    completedUploads.push_back({nodeId, protocol::uuidText(record.device_id()),
                                                 protocol::uuidText(record.report_id())});
                }
            }
            if (std::any_of(assembled.records().begin(), assembled.records().end(),
                           [](const auto& record) { return record.raw_requests_size() != 0; }) &&
                !acquisitionConfigurations_.contains(nodeId)) {
                AcquisitionConfiguration configuration;
                configuration.items = co_await ConfigService::loadItems(context, nodeId, &configuration.calculations);
                acquisitionConfigurations_.emplace(nodeId, std::move(configuration));
            }
            const auto configuration = acquisitionConfigurations_.find(nodeId);
            const std::vector<pb::ConfigItem> emptyItems;
            collectTelemetry(catalog, nodeId, receivedAtMs, assembled, telemetry,
                configuration == acquisitionConfigurations_.end() ? emptyItems : configuration->second.items);
            for (const auto& record : assembled.records())
                if (record.raw_requests_size() && configuration != acquisitionConfigurations_.end()) {
                    const auto calculation = configuration->second.calculations.find(protocol::uuidText(record.device_id()));
                    if (calculation != configuration->second.calculations.end())
                        rawCalculations[protocol::uuidText(record.record_id())] = calculation->second;
                }
            break;
        }
        case pb::Envelope::kCommandResult:
            if (envelope.command_result().device_id().size() == 16)
                co_await ensureMetadata(
                    context, catalog, nodeId,
                    protocol::uuidText(envelope.command_result().device_id()));
            co_await saveCommandResult(context, catalog, nodeId, receivedAtMs,
                                       envelope.command_result());
            break;
        case pb::Envelope::kDeviceStatusReport:
            co_await saveDeviceStatus(context, nodeId, envelope.device_status_report());
            break;
        default:
            break;
        }
    }

    ruvia::task<void> ensureMetadata(ruvia::web_worker_context& context,
                                      metadata::Catalog& catalog,
                                      std::string_view nodeId,
                                      std::string_view deviceId) {
        const auto existingNode = catalog.find(std::string(nodeId));
        if (existingNode != catalog.end() &&
            existingNode->second.contains(std::string(deviceId)))
            co_return;
        auto snapshot = co_await metadata::loadNode(context.redis(), nodeId);
        if (!snapshot.contains(std::string(deviceId))) {
            snapshot = co_await metadata::loadNodeFromDatabase(context, nodeId);
            co_await metadata::storeNode(context.redis(), nodeId, snapshot, false);
        }
        catalog[std::string(nodeId)] = std::move(snapshot);
    }

    ruvia::task<void> ensureMetadata(ruvia::web_worker_context& context,
                                      metadata::Catalog& catalog,
                                      std::string_view nodeId,
                                      const pb::TelemetryBatch& batch) {
        const auto containsAll = [&batch](const metadata::NodeSnapshot& snapshot) {
            for (const auto& record : batch.records()) {
                if (record.device_id().size() == 16 &&
                    !snapshot.contains(protocol::uuidText(record.device_id())))
                    return false;
            }
            return true;
        };
        const auto existingNode = catalog.find(std::string(nodeId));
        if (existingNode != catalog.end() && containsAll(existingNode->second))
            co_return;
        auto snapshot = co_await metadata::loadNode(context.redis(), nodeId);
        if (!containsAll(snapshot)) {
            snapshot = co_await metadata::loadNodeFromDatabase(context, nodeId);
            co_await metadata::storeNode(context.redis(), nodeId, snapshot, false);
        }
        catalog[std::string(nodeId)] = std::move(snapshot);
    }

    static std::string_view simState(pb::ModemSimState state) {
        switch (state) {
        case pb::MODEM_SIM_READY:
            return "ready";
        case pb::MODEM_SIM_NOT_INSERTED:
            return "not_inserted";
        case pb::MODEM_SIM_PIN_REQUIRED:
            return "pin_required";
        case pb::MODEM_SIM_PUK_REQUIRED:
            return "puk_required";
        case pb::MODEM_SIM_BLOCKED:
            return "blocked";
        default:
            return "unknown";
        }
    }

    static ruvia::task<void> saveHello(ruvia::web_worker_context& context,
                                       const pb::Hello& hello) {
        if (!protocol::validImei(hello.imei()))
            co_return;
        const auto candidate = context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        ruvia::db_query query;
        const auto uuid = [&query](std::string_view value) {
            return query.cast(query.value(value), ruvia::db_data_type::uuid);
        };
        const auto text = [&query](const auto& value) {
            return query.cast(query.value(std::string_view(value)),
                              ruvia::db_data_type::text);
        };
        const auto boolean = [&query](bool value) {
            return query.cast(query.value(value), ruvia::db_data_type::boolean);
        };
        const auto integer = [&query](auto value) {
            return query.cast(query.value(static_cast<std::int64_t>(value)),
                              ruvia::db_data_type::big_int);
        };
        const auto jsonKey = [&query](std::string_view value) {
            return config::detail::jsonKey(query, value);
        };
        const auto vpn = query.call(
            "jsonb_build_object",
            {jsonKey("supportsVpn"), boolean(false),
             jsonKey("wireguardVersion"), text(std::string_view{}),
             jsonKey("agentVersion"), text(std::string_view{}),
             jsonKey("publicKey"), text(std::string_view{})});
        const auto capability = query.call(
            "jsonb_build_object",
            {jsonKey("networkConfig"), boolean(hello.supports_network_config()),
             jsonKey("firmwareUpdate"), boolean(hello.supports_firmware_update()),
             jsonKey("firmwareStream"), boolean(hello.supports_firmware_stream()),
             jsonKey("platformConfig"), boolean(hello.supports_platform_config()),
             jsonKey("deviceConfig"), boolean(hello.supports_device_config()),
             jsonKey("networkConfigVersion"), integer(hello.network_config_version()),
             jsonKey("modemControl"), boolean(hello.supports_modem_control()),
             jsonKey("commandStartBefore"), boolean(hello.supports_command_start_before()),
             jsonKey("logs"), boolean(hello.supports_logs()),
             jsonKey("terminal"), boolean(false), jsonKey("serialDebug"), boolean(false),
             jsonKey("vpn"), vpn});
        const auto signal = query.call(
            "jsonb_build_object", {jsonKey("csq"), integer(hello.signal_csq()),
                                    jsonKey("rssiDbm"), integer(hello.signal_rssi_dbm()),
                                    jsonKey("percent"), integer(hello.signal_percent())});
        const auto mobile = query.call(
            "jsonb_build_object",
            {jsonKey("available"), boolean(hello.modem_available()),
             jsonKey("simState"), text(simState(hello.sim_state())),
             jsonKey("iccid"), text(hello.iccid()), jsonKey("signal"), signal,
             jsonKey("registered"), boolean(hello.mobile_registered()),
             jsonKey("registrationStatus"), integer(hello.mobile_registration_status()),
             jsonKey("apn"), text(hello.apn()), jsonKey("operator"), text(hello.mobile_operator()),
             jsonKey("connected"), boolean(hello.mobile_connected()),
             jsonKey("ipv4"), text(hello.mobile_ipv4())});
        const auto config = query.call(
            "jsonb_build_object", {jsonKey("activeVersion"), integer(0),
                                    jsonKey("desiredVersion"), integer(0),
                                    jsonKey("state"), text("idle"),
                                    jsonKey("message"), text(std::string_view{})});
        const auto outbox = query.call(
            "jsonb_build_object", {jsonKey("records"), integer(0),
                                    jsonKey("bytes"), integer(0)});
        const auto log = query.call(
            "jsonb_build_object", {jsonKey("level"),
                                    config::detail::textDefault(
                                        query, text(hello.log_level()), "info")});
        const auto status = query.call(
            "jsonb_build_object", {jsonKey("config"), config,
                                    jsonKey("outbox"), outbox, jsonKey("log"), log});
        query.insert_into(service::edge::persistence::EdgeNodeEntity::table_name(),
                         {"id", "platform_id", "imei", "model", "software_version",
                          "hostname", "architecture", "openwrt_release", "capability",
                          "mobile", "status", "last_seen_at", "updated_at"})
            .values({uuid(std::string_view(candidate)), uuid(context.template worker_state<service::edge::config::PlatformIdentity>().id),
                     query.value(std::string_view(hello.imei())),
                     query.value(std::string_view(hello.model())),
                     query.value(std::string_view(hello.software_version())),
                     query.value(std::string_view(hello.hostname())),
                     query.value(std::string_view(hello.architecture())),
                     query.value(std::string_view(hello.openwrt_release())), capability, mobile,
                     status,
                     query.call("now"), query.call("now")});

        const auto existingCapability = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"capability">(), "edge_node");
        const auto existingMobile = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"mobile">(), "edge_node");
        const auto existingStatus = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), "edge_node");
        const auto excludedMobile = query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"mobile">());
        const auto apn = query.coalesce({
            query.null_if(config::detail::jsonText(query, excludedMobile, "apn"),
                         query.value(std::string_view{})),
            config::detail::jsonText(query, existingMobile, "apn"),
            query.value(std::string_view{})});
        const auto operatorName = query.coalesce({
            query.null_if(config::detail::jsonText(query, excludedMobile, "operator"),
                         query.value(std::string_view{})),
            config::detail::jsonText(query, existingMobile, "operator"),
            query.value(std::string_view{})});
        const auto mobileWithApn = query.call(
            "jsonb_set", {excludedMobile, config::detail::jsonPath(query, "{apn}"),
                           config::detail::toJsonb(query,
                               query.cast(apn, ruvia::db_data_type::text)),
                           query.cast(query.value(true), ruvia::db_data_type::boolean)});
        const auto mobileUpdate = query.call(
            "jsonb_set", {mobileWithApn, config::detail::jsonPath(query, "{operator}"),
                           config::detail::toJsonb(query,
                               query.cast(operatorName, ruvia::db_data_type::text)),
                           query.cast(query.value(true), ruvia::db_data_type::boolean)});
        const auto capabilityUpdate = query.binary(
            query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()), ruvia::db_binary_operator::json_concat,
            query.call("jsonb_build_object",
                       {jsonKey("terminal"),
                        config::detail::booleanText(
                            query, config::detail::jsonText(query, existingCapability, "terminal")),
                        jsonKey("vpn"),
                        query.coalesce({config::detail::jsonGet(query, existingCapability, "vpn"),
                                        config::detail::jsonGet(query, query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()),
                                                                 "vpn")})}));
        const auto statusWithLog = query.call(
            "jsonb_set", {existingStatus, config::detail::jsonPath(query, "{log}"),
                           query.coalesce({config::detail::jsonGet(query, existingStatus, "log"),
                                           query.cast(query.value(std::string_view{"{}"}),
                                                      ruvia::db_data_type::jsonb)}),
                           query.cast(query.value(true), ruvia::db_data_type::boolean)});
        const auto statusUpdate = query.call(
            "jsonb_set", {statusWithLog, config::detail::jsonPath(query, "{log,level}"),
                           config::detail::toJsonb(
                               query, config::detail::textDefault(
                                         query, text(hello.log_level()), "info")),
                           query.cast(query.value(true), ruvia::db_data_type::boolean)});
        ruvia::db_conflict_options conflict;
        conflict.columns_ = {"platform_id", "imei"};
        conflict.update_ = {
            {"model", query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"model">())},
            {"software_version", query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"software_version">())},
            {"hostname", query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"hostname">())},
            {"architecture", query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"architecture">())},
            {"openwrt_release", query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"openwrt_release">())},
            {"capability", capabilityUpdate},
            {"mobile", mobileUpdate},
            {"status", statusUpdate},
            {"last_seen_at", query.call("now")},
            {"updated_at", query.call("now")},
        };
        query.on_conflict(conflict)
            .returning({query.cast(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_data_type::text),
                        query.column(service::edge::persistence::EdgeNodeEntity::column_name<"enrollment_status">())});
        const auto rows = co_await context.db().query(query);
        const auto key = protocol::authKey(hello.imei());
        const auto nodeId = std::string(rows.front()[0].value().value_or(std::string_view{}));
        const auto enrollmentStatus = std::string(rows.front()[1].value().value_or(std::string_view{}));
        const auto value = nodeId + "|" + enrollmentStatus;
        co_await context.redis().set(key, value);
        if (enrollmentStatus == "approved") {
            // The Hello projects deviceConfig before reconciliation. A missing
            // Redis queue must not defer a stale revision until the 900s heartbeat.
            (void)co_await configService().requeueIfStale(
                context, nodeId, hello.last_applied_config_version());
            ruvia::db_query target;
            target
                .select({target.alias(target.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">(), "task"), "task_id"),
                         target.alias(target.column(service::edge::persistence::EdgeFirmwareEntity::column_name<"id">(), "firmware"), "firmware_id")})
                .from(service::edge::persistence::EdgeTaskEntity::table_name(), "task")
                .join(ruvia::db_join_type::inner, service::edge::persistence::EdgeFirmwareEntity::table_name(),
                      target.binary(
                          target.cast(target.column(service::edge::persistence::EdgeFirmwareEntity::column_name<"id">(), "firmware"),
                                      ruvia::db_data_type::text),
                          ruvia::db_binary_operator::equal,
                          config::detail::jsonText(
                              target, target.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">(), "task"), "firmware_id")),
                      "firmware")
                .where(target.binary(
                    target.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">(), "task"), ruvia::db_binary_operator::equal,
                    target.cast(target.value(nodeId), ruvia::db_data_type::uuid)))
                .and_where(target.binary(target.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">(), "task"),
                                        ruvia::db_binary_operator::equal,
                                        target.value(std::string_view{"firmware"})))
                .and_where(target.binary(target.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), "task"),
                                        ruvia::db_binary_operator::equal,
                                        target.value(std::string_view{"running"})))
                .and_where(target.binary(
                    config::detail::jsonText(target, target.column(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), "task"), "state"),
                    ruvia::db_binary_operator::equal,
                    target.value(std::string_view{"flashing"})))
                .order_by(target.column(service::edge::persistence::EdgeTaskEntity::column_name<"created_at">(), "task"), ruvia::db_order_direction::desc)
                .limit(1);

            ruvia::db_query completed;
            const auto rebootedResult = completed.binary(
                completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), "task"), ruvia::db_binary_operator::json_concat,
                completed.call(
                    "jsonb_build_object",
                    {config::detail::jsonKey(completed, "state"),
                     completed.cast(completed.value(std::string_view{"rebooted"}),
                                    ruvia::db_data_type::text),
                     config::detail::jsonKey(completed, "message"),
                     completed.cast(completed.value(std::string_view{"firmware reboot confirmed"}),
                                    ruvia::db_data_type::text),
                     config::detail::jsonKey(completed, "softwareVersion"),
                     completed.cast(completed.value(std::string_view(hello.software_version())),
                                    ruvia::db_data_type::text)}));
            completed.update(service::edge::persistence::EdgeTaskEntity::table_name(), "task")
                .set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), completed.value(std::string_view{"succeeded"}))
                .set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), rebootedResult)
                .set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), completed.call("now"))
                .set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), completed.call("now"))
                .update_from("target")
                .where(completed.binary(completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">(), "task"),
                                        ruvia::db_binary_operator::equal,
                                        completed.column("task_id", "target")))
                .returning({completed.column("firmware_id", "target")});

            ruvia::db_query recovery;
            recovery.with("target", target)
                .with("completed", completed)
                .update(service::edge::persistence::EdgeFirmwareEntity::table_name(), "firmware")
                .set(service::edge::persistence::EdgeFirmwareEntity::column_name<"version">(), recovery.cast(
                                                recovery.value(std::string_view(
                                                    hello.software_version())),
                                                ruvia::db_data_type::text))
                .update_from("completed")
                .where(recovery.binary(recovery.column(service::edge::persistence::EdgeFirmwareEntity::column_name<"id">(), "firmware"),
                                       ruvia::db_binary_operator::equal,
                                       recovery.column("firmware_id", "completed")));
            (void)co_await context.db().execute(recovery);
        }
    }

    static ruvia::task<void> saveHeartbeat(ruvia::web_worker_context& context,
                                           std::string_view nodeId,
                                           const pb::Heartbeat& heartbeat) {
        ruvia::db_query query;
        const auto text = [&query](std::string_view value) {
            return query.cast(query.value(value), ruvia::db_data_type::text);
        };
        const auto integer = [&query](auto value) {
            return query.cast(query.value(static_cast<std::int64_t>(value)),
                              ruvia::db_data_type::big_int);
        };
        const auto boolean = [&query](bool value) {
            return query.cast(query.value(value), ruvia::db_data_type::boolean);
        };
        const auto status = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
        const auto desiredVersion = config::detail::configVersion(
            query, status, "desiredVersion");
        const auto receivedVersion = integer(heartbeat.active_config_version());
        const auto applied = query.binary(
            query.binary(desiredVersion, ruvia::db_binary_operator::equal,
                         receivedVersion),
            ruvia::db_binary_operator::and_value,
            query.binary(receivedVersion, ruvia::db_binary_operator::greater,
                         integer(0)));
        const auto config = query.call(
            "jsonb_build_object",
            {config::detail::jsonKey(query, "activeVersion"),
             query.greatest({config::detail::configVersion(query, status, "activeVersion"),
                             receivedVersion}),
             config::detail::jsonKey(query, "desiredVersion"), desiredVersion,
             config::detail::jsonKey(query, "state"),
             query.case_when({{applied, text("applied")}},
                            config::detail::configState(query, status)),
             config::detail::jsonKey(query, "message"),
             query.case_when(
                 {{applied, text(std::string_view{})}},
                 query.coalesce({config::detail::jsonText(
                                     query, config::detail::jsonGet(query, status, "config"),
                                     "message"),
                                 query.value(std::string_view{})}))});
        const auto signal = query.call(
            "jsonb_build_object", {config::detail::jsonKey(query, "csq"),
                                    integer(heartbeat.signal_csq()),
                                    config::detail::jsonKey(query, "rssiDbm"),
                                    integer(heartbeat.signal_rssi_dbm()),
                                    config::detail::jsonKey(query, "percent"),
                                    integer(heartbeat.signal_percent())});
        const auto existingMobile = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"mobile">());
        const auto apn = query.coalesce({
            query.null_if(text(heartbeat.apn()), query.value(std::string_view{})),
            config::detail::jsonText(query, existingMobile, "apn"),
            query.value(std::string_view{})});
        const auto operatorName = query.coalesce({
            query.null_if(text(heartbeat.mobile_operator()), query.value(std::string_view{})),
            config::detail::jsonText(query, existingMobile, "operator"),
            query.value(std::string_view{})});
        const auto mobile = query.call(
            "jsonb_build_object",
            {config::detail::jsonKey(query, "available"), boolean(heartbeat.modem_available()),
             config::detail::jsonKey(query, "simState"), text(simState(heartbeat.sim_state())),
             config::detail::jsonKey(query, "iccid"), text(heartbeat.iccid()),
             config::detail::jsonKey(query, "signal"), signal,
             config::detail::jsonKey(query, "registered"), boolean(heartbeat.mobile_registered()),
             config::detail::jsonKey(query, "registrationStatus"),
             integer(heartbeat.mobile_registration_status()),
             config::detail::jsonKey(query, "apn"), apn,
             config::detail::jsonKey(query, "operator"), operatorName,
             config::detail::jsonKey(query, "connected"), boolean(heartbeat.mobile_connected()),
             config::detail::jsonKey(query, "ipv4"), text(heartbeat.mobile_ipv4())});
        const auto tcpTrafficText = [&query](std::string_view key) {
            return config::detail::jsonText(
                query,
                config::detail::jsonGet(
                    query,
                    query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                    "tcpTraffic"),
                key);
        };
        const auto shanghaiNow = query.call("timezone", {text("Asia/Shanghai"), query.call("now")});
        const auto currentMonth = query.call("to_char", {shanghaiNow, text("YYYY-MM")});
        const auto currentDay = query.call("to_char", {shanghaiNow, text("YYYY-MM-DD")});
        const auto tcpTraffic = heartbeat.has_tcp_traffic()
            ? [&] {
                const auto& sample = heartbeat.tcp_traffic();
                const auto sameSample = query.binary(
                    tcpTrafficText("sampleId"), ruvia::db_binary_operator::equal,
                    text(std::to_string(sample.sample_id())));
                const auto sameMonth = query.binary(
                    tcpTrafficText("month"), ruvia::db_binary_operator::equal, currentMonth);
                const auto monthMissing = query.unary(
                    ruvia::db_unary_operator::is_null,
                    query.null_if(tcpTrafficText("month"), query.value(std::string_view{})));
                const auto continueMonth = query.binary(
                    sameMonth, ruvia::db_binary_operator::or_value, monthMissing);
                const auto sameDay = query.binary(
                    tcpTrafficText("day"), ruvia::db_binary_operator::equal, currentDay);
                const auto dayMissing = query.unary(
                    ruvia::db_unary_operator::is_null,
                    query.null_if(tcpTrafficText("day"), query.value(std::string_view{})));
                const auto continueDay = query.binary(
                    sameDay, ruvia::db_binary_operator::or_value, dayMissing);
                const auto periodTotal = [&](std::string_view key, const auto& continuePeriod, std::uint64_t bytes) {
                    const auto kept = query.coalesce({
                        query.cast(
                            query.null_if(tcpTrafficText(key), query.value(std::string_view{})),
                            ruvia::db_data_type::big_int),
                        integer(0)});
                    const auto added = query.binary(kept, ruvia::db_binary_operator::add, integer(bytes));
                    return query.cast(
                        query.case_when(
                            {{sameSample, kept},
                             {continuePeriod, added}},
                            integer(bytes)),
                        ruvia::db_data_type::text);
                };
                const auto monthValue = query.case_when(
                    {{sameSample, query.coalesce({tcpTrafficText("month"), currentMonth})}},
                    currentMonth);
                const auto dayValue = query.case_when(
                    {{sameSample, query.coalesce({tcpTrafficText("day"), currentDay})}},
                    currentDay);
                return query.call("jsonb_build_object", {
                    config::detail::jsonKey(query, "uploadBytes"), text(std::to_string(sample.upload_bytes())),
                    config::detail::jsonKey(query, "downloadBytes"), text(std::to_string(sample.download_bytes())),
                    config::detail::jsonKey(query, "intervalMs"), text(std::to_string(sample.interval_ms())),
                    config::detail::jsonKey(query, "sampleId"), text(std::to_string(sample.sample_id())),
                    config::detail::jsonKey(query, "month"), monthValue,
                    config::detail::jsonKey(query, "monthlyUploadBytes"), periodTotal("monthlyUploadBytes", continueMonth, sample.upload_bytes()),
                    config::detail::jsonKey(query, "monthlyDownloadBytes"), periodTotal("monthlyDownloadBytes", continueMonth, sample.download_bytes()),
                    config::detail::jsonKey(query, "day"), dayValue,
                    config::detail::jsonKey(query, "dailyUploadBytes"), periodTotal("dailyUploadBytes", continueDay, sample.upload_bytes()),
                    config::detail::jsonKey(query, "dailyDownloadBytes"), periodTotal("dailyDownloadBytes", continueDay, sample.download_bytes())});
            }()
            : config::detail::jsonGet(
                  query,
                  query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                  "tcpTraffic");
        const auto vpnTrafficText = [&query](std::string_view key) {
            return config::detail::jsonText(
                query,
                config::detail::jsonGet(
                    query,
                    query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                    "vpnTraffic"),
                key);
        };
        const auto vpnTraffic = heartbeat.has_vpn_traffic()
            ? [&] {
                const auto& sample = heartbeat.vpn_traffic();
                const auto sameSample = query.binary(
                    vpnTrafficText("sampleId"), ruvia::db_binary_operator::equal,
                    text(std::to_string(sample.sample_id())));
                const auto sameMonth = query.binary(
                    vpnTrafficText("month"), ruvia::db_binary_operator::equal, currentMonth);
                const auto monthMissing = query.unary(
                    ruvia::db_unary_operator::is_null,
                    query.null_if(vpnTrafficText("month"), query.value(std::string_view{})));
                const auto continueMonth = query.binary(
                    sameMonth, ruvia::db_binary_operator::or_value, monthMissing);
                const auto sameDay = query.binary(
                    vpnTrafficText("day"), ruvia::db_binary_operator::equal, currentDay);
                const auto dayMissing = query.unary(
                    ruvia::db_unary_operator::is_null,
                    query.null_if(vpnTrafficText("day"), query.value(std::string_view{})));
                const auto continueDay = query.binary(
                    sameDay, ruvia::db_binary_operator::or_value, dayMissing);
                const auto periodTotal = [&](std::string_view key, const auto& continuePeriod, std::uint64_t bytes) {
                    const auto kept = query.coalesce({
                        query.cast(
                            query.null_if(vpnTrafficText(key), query.value(std::string_view{})),
                            ruvia::db_data_type::big_int),
                        integer(0)});
                    const auto added = query.binary(kept, ruvia::db_binary_operator::add, integer(bytes));
                    return query.cast(
                        query.case_when(
                            {{sameSample, kept},
                             {continuePeriod, added}},
                            integer(bytes)),
                        ruvia::db_data_type::text);
                };
                const auto monthValue = query.case_when(
                    {{sameSample, query.coalesce({vpnTrafficText("month"), currentMonth})}},
                    currentMonth);
                const auto dayValue = query.case_when(
                    {{sameSample, query.coalesce({vpnTrafficText("day"), currentDay})}},
                    currentDay);
                return query.call("jsonb_build_object", {
                    config::detail::jsonKey(query, "uploadBytes"), text(std::to_string(sample.upload_bytes())),
                    config::detail::jsonKey(query, "downloadBytes"), text(std::to_string(sample.download_bytes())),
                    config::detail::jsonKey(query, "intervalMs"), text(std::to_string(sample.interval_ms())),
                    config::detail::jsonKey(query, "sampleId"), text(std::to_string(sample.sample_id())),
                    config::detail::jsonKey(query, "month"), monthValue,
                    config::detail::jsonKey(query, "monthlyUploadBytes"), periodTotal("monthlyUploadBytes", continueMonth, sample.upload_bytes()),
                    config::detail::jsonKey(query, "monthlyDownloadBytes"), periodTotal("monthlyDownloadBytes", continueMonth, sample.download_bytes()),
                    config::detail::jsonKey(query, "day"), dayValue,
                    config::detail::jsonKey(query, "dailyUploadBytes"), periodTotal("dailyUploadBytes", continueDay, sample.upload_bytes()),
                    config::detail::jsonKey(query, "dailyDownloadBytes"), periodTotal("dailyDownloadBytes", continueDay, sample.download_bytes())});
            }()
            : config::detail::jsonGet(
                  query,
                  query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                  "vpnTraffic");
        query.update(service::edge::persistence::EdgeNodeEntity::table_name())
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), query.call(
                               "jsonb_build_object",
                               {config::detail::jsonKey(query, "config"), config,
                                config::detail::jsonKey(query, "outbox"),
                                query.call("jsonb_build_object",
                                           {config::detail::jsonKey(query, "records"),
                                            integer(heartbeat.outbox_records()),
                                            config::detail::jsonKey(query, "bytes"),
                                            integer(heartbeat.outbox_bytes())}),
                                config::detail::jsonKey(query, "tcpTraffic"), tcpTraffic,
                                config::detail::jsonKey(query, "vpnTraffic"), vpnTraffic,
                                config::detail::jsonKey(query, "log"),
                                query.call("jsonb_build_object",
                                           {config::detail::jsonKey(query, "level"),
                                            config::detail::textDefault(
                                                query, text(heartbeat.log_level()), "info")})}))
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"mobile">(), mobile)
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"capability">(), query.call(
                                  "jsonb_set",
                                  {query.column(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()),
                                   config::detail::jsonPath(query, "{modemControl}"),
                                   config::detail::toJsonb(
                                       query, boolean(heartbeat.supports_modem_control())),
                                   query.cast(query.value(true), ruvia::db_data_type::boolean)}))
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"last_seen_at">(), query.call("now"))
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), query.call("now"))
            .where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                                query.cast(query.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(query);
        if (heartbeat.active_config_version() != 0) {
            ruvia::db_query revision;
            revision.update(service::edge::persistence::EdgeConfigRevisionEntity::table_name(), "revision")
                .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"status">(), revision.value(std::string_view{"applied"}))
                .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"message">(), revision.value(std::string_view{}))
                .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"completed_at">(), revision.coalesce({
                    revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"completed_at">(), "revision"), revision.call("now")}))
                .update_from(service::edge::persistence::EdgeNodeEntity::table_name(), "node")
                .where(revision.binary(
                    revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"node_id">(), "revision"), ruvia::db_binary_operator::equal,
                    revision.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "node")))
                .and_where(revision.binary(
                    revision.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "node"), ruvia::db_binary_operator::equal,
                    revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid)))
                .and_where(revision.binary(
                    revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"revision">(), "revision"),
                    ruvia::db_binary_operator::equal,
                    revision.cast(revision.value(static_cast<std::int64_t>(
                                                  heartbeat.active_config_version())),
                                                ruvia::db_data_type::big_int)))
                .and_where(revision.binary(
                    config::detail::configVersion(
                        revision, revision.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), "node"), "desiredVersion"),
                    ruvia::db_binary_operator::equal,
                    revision.cast(revision.value(static_cast<std::int64_t>(
                                                  heartbeat.active_config_version())),
                                  ruvia::db_data_type::big_int)));
            (void)co_await context.db().execute(revision);
        }
    }

    static std::string mac(const pb::InterfaceCapability& item) {
        if (item.mac().size() != 6)
            return {};
        constexpr char digits[] = "0123456789abcdef";
        std::string output;
        output.reserve(17);
        for (std::size_t index = 0; index < 6; ++index) {
            if (index != 0)
                output.push_back(':');
            const auto byte = static_cast<std::uint8_t>(item.mac()[index]);
            output.push_back(digits[byte >> 4U]);
            output.push_back(digits[byte & 0x0fU]);
        }
        return output;
    }

    template <typename Strings>
    static std::string jsonArray(const Strings& bridgePorts) {
        std::string output{"["};
        bool first = true;
        for (const auto& port : bridgePorts) {
            if (!first)
                output.push_back(',');
            output.push_back('"');
            output += jsonEscape(port);
            output.push_back('"');
            first = false;
        }
        output.push_back(']');
        return output;
    }

    static std::string addressMode(pb::NetworkAddressMode mode) {
        switch (mode) {
        case pb::NETWORK_ADDRESS_DHCP:
            return "dhcp";
        case pb::NETWORK_ADDRESS_STATIC:
            return "static";
        default:
            return "none";
        }
    }

    static ruvia::task<void> saveCapabilities(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::CapabilityReport& report) {
        ruvia::db_query deleteInterfaces;
        deleteInterfaces.delete_from(service::edge::persistence::EdgeNodeInterfaceEntity::table_name())
            .where(deleteInterfaces.binary(
                deleteInterfaces.column(service::edge::persistence::EdgeNodeInterfaceEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                deleteInterfaces.cast(deleteInterfaces.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(deleteInterfaces);
        for (const auto& item : report.interfaces()) {
            const auto macAddress = mac(item);
            const auto ports = jsonArray(item.bridge_ports());
            ruvia::db_query insert;
            insert.insert_into(service::edge::persistence::EdgeNodeInterfaceEntity::table_name(),
                              {"node_id", "name", "display_name", "mac", "is_up",
                               "is_bridge", "ipv4", "prefix_length", "gateway",
                               "bridge_ports"})
                .values({
                    insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                    insert.value(std::string_view(item.name())),
                    insert.value(std::string_view(item.display_name())),
                    insert.null_if(insert.value(std::string_view(macAddress)),
                                  insert.value(std::string_view{})),
                    insert.cast(insert.value(item.up()), ruvia::db_data_type::boolean),
                    insert.cast(insert.value(item.bridge()), ruvia::db_data_type::boolean),
                    insert.null_if(insert.value(std::string_view(item.ipv4())),
                                  insert.value(std::string_view{})),
                    insert.value(item.prefix_length()),
                    insert.null_if(insert.value(std::string_view(item.gateway())),
                                  insert.value(std::string_view{})),
                    insert.cast(insert.value(std::string_view(ports)),
                                 ruvia::db_data_type::jsonb),
                });
            (void)co_await context.db().execute(insert);
        }
        ruvia::db_query deleteNetworks;
        deleteNetworks.delete_from(service::edge::persistence::EdgeNodeNetworkEntity::table_name())
            .where(deleteNetworks.binary(
                deleteNetworks.column(service::edge::persistence::EdgeNodeNetworkEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                deleteNetworks.cast(deleteNetworks.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(deleteNetworks);
        for (const auto& item : report.networks()) {
            const auto ports = jsonArray(item.bridge_ports());
            const auto mode = addressMode(item.mode());
            ruvia::db_query insert;
            insert.insert_into(service::edge::persistence::EdgeNodeNetworkEntity::table_name(),
                              {"node_id", "name", "address_mode", "device", "is_up",
                               "is_bridge", "ipv4", "prefix_length", "gateway",
                               "bridge_ports"})
                .values({
                    insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                    insert.value(std::string_view(item.name())),
                    insert.value(std::string_view(mode)),
                    insert.value(std::string_view(item.device())),
                    insert.cast(insert.value(item.up()), ruvia::db_data_type::boolean),
                    insert.cast(insert.value(item.bridge()), ruvia::db_data_type::boolean),
                    insert.null_if(insert.value(std::string_view(item.ipv4())),
                                  insert.value(std::string_view{})),
                    insert.value(item.prefix_length()),
                    insert.null_if(insert.value(std::string_view(item.gateway())),
                                  insert.value(std::string_view{})),
                    insert.cast(insert.value(std::string_view(ports)),
                                 ruvia::db_data_type::jsonb),
                });
            (void)co_await context.db().execute(insert);
        }
        ruvia::db_query deleteSerial;
        deleteSerial.delete_from(service::edge::persistence::EdgeNodeSerialEntity::table_name())
            .where(deleteSerial.binary(
                deleteSerial.column(service::edge::persistence::EdgeNodeSerialEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                deleteSerial.cast(deleteSerial.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(deleteSerial);
        for (const auto& item : report.serial_ports()) {
            ruvia::db_query insert;
            insert.insert_into(service::edge::persistence::EdgeNodeSerialEntity::table_name(),
                              {"node_id", "path", "display_name", "available", "rs485"})
                .values({
                    insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                    insert.value(std::string_view(item.path())),
                    insert.value(std::string_view(item.display_name())),
                    insert.cast(insert.value(item.available()), ruvia::db_data_type::boolean),
                    insert.cast(insert.value(item.rs485()), ruvia::db_data_type::boolean),
                });
            (void)co_await context.db().execute(insert);
        }
        std::vector<std::string> supported;
        for (const auto protocol : report.supported_protocols()) {
            switch (protocol) {
            case pb::PROTOCOL_MODBUS: supported.emplace_back("Modbus"); break;
            case pb::PROTOCOL_S7: supported.emplace_back("S7"); break;
            case pb::PROTOCOL_SL651: supported.emplace_back("SL651"); break;
            case pb::PROTOCOL_MC: supported.emplace_back("MC"); break;
            case pb::PROTOCOL_FINS: supported.emplace_back("FINS"); break;
            case pb::PROTOCOL_DLT645: supported.emplace_back("DLT645"); break;
            default: break;
            }
        }
        const auto protocols = jsonArray(supported);
        ruvia::db_query terminal;
        terminal.update(service::edge::persistence::EdgeNodeEntity::table_name())
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"capability">(), terminal.call(
                                   "jsonb_set",
                                   {terminal.call("jsonb_set", {
                                        terminal.column(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()),
                                        config::detail::jsonPath(terminal, "{protocols}"),
                                        terminal.cast(terminal.value(std::string_view(protocols)), ruvia::db_data_type::jsonb),
                                        terminal.value(true)}),
                                    config::detail::jsonPath(terminal, "{terminal}"),
                                    config::detail::toJsonb(
                                        terminal, terminal.cast(terminal.value(
                                                                           report.ttyd_available()),
                                                                       ruvia::db_data_type::boolean)),
                                    terminal.cast(terminal.value(true),
                                                  ruvia::db_data_type::boolean)}))
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), terminal.call("now"))
            .where(terminal.binary(terminal.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                                   terminal.cast(terminal.value(nodeId),
                                                 ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(terminal);
        ruvia::db_query serialCapability;
        serialCapability.update(persistence::EdgeNodeEntity::table_name())
            .set(persistence::EdgeNodeEntity::column_name<"capability">(),
                serialCapability.call("jsonb_set", {
                    serialCapability.column(persistence::EdgeNodeEntity::column_name<"capability">()),
                    config::detail::jsonPath(serialCapability, "{serialDebug}"),
                    config::detail::toJsonb(serialCapability, serialCapability.cast(
                        serialCapability.value(report.supports_serial_debug()), ruvia::db_data_type::boolean)),
                    serialCapability.value(true)}))
            .where(serialCapability.binary(serialCapability.column(persistence::EdgeNodeEntity::column_name<"id">()),
                ruvia::db_binary_operator::equal, serialCapability.cast(serialCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(serialCapability);
        ruvia::db_query dtuCapability;
        dtuCapability.update(persistence::EdgeNodeEntity::table_name())
            .set(persistence::EdgeNodeEntity::column_name<"capability">(), dtuCapability.call("jsonb_set", {
                dtuCapability.column(persistence::EdgeNodeEntity::column_name<"capability">()),
                config::detail::jsonPath(dtuCapability, "{dtu}"),
                config::detail::toJsonb(dtuCapability, dtuCapability.cast(dtuCapability.value(report.supports_dtu()), ruvia::db_data_type::boolean)), dtuCapability.value(true)}))
            .where(dtuCapability.binary(dtuCapability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                dtuCapability.cast(dtuCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(dtuCapability);
        ruvia::db_query derivedCapability;
        derivedCapability.update(persistence::EdgeNodeEntity::table_name())
            .set(persistence::EdgeNodeEntity::column_name<"capability">(), derivedCapability.call("jsonb_set", {
                derivedCapability.column(persistence::EdgeNodeEntity::column_name<"capability">()), config::detail::jsonPath(derivedCapability, "{derivedPoints}"),
                config::detail::toJsonb(derivedCapability, derivedCapability.cast(derivedCapability.value(report.supports_derived_points()), ruvia::db_data_type::boolean)), derivedCapability.value(true)}))
            .where(derivedCapability.binary(derivedCapability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, derivedCapability.cast(derivedCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(derivedCapability);
        if (report.has_vpn()) {
            const auto vpnPublicKey = validVpnPublicKey(report.vpn().public_key())
                                          ? std::string(report.vpn().public_key())
                                          : std::string{};
            ruvia::db_query vpn;
            vpn.update(service::edge::persistence::EdgeNodeEntity::table_name())
                .set(service::edge::persistence::EdgeNodeEntity::column_name<"capability">(), vpn.call(
                                       "jsonb_set",
                                       {vpn.column(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()),
                                        config::detail::jsonPath(vpn, "{vpn}"),
                                        vpn.call(
                                            "jsonb_build_object",
                                            {config::detail::jsonKey(vpn, "supportsVpn"),
                                             vpn.cast(vpn.value(report.vpn().supports_vpn()),
                                                      ruvia::db_data_type::boolean),
                                             config::detail::jsonKey(vpn, "wireguardVersion"),
                                             vpn.cast(vpn.value(std::string_view(
                                                                     report.vpn().wireguard_version())),
                                                      ruvia::db_data_type::text),
                                             config::detail::jsonKey(vpn, "agentVersion"),
                                             vpn.cast(vpn.value(std::string_view(
                                                                     report.vpn().agent_version())),
                                                      ruvia::db_data_type::text),
                                             config::detail::jsonKey(vpn, "publicKey"),
                                             vpn.cast(vpn.value(std::string_view(vpnPublicKey)),
                                                      ruvia::db_data_type::text)}),
                                        vpn.cast(vpn.value(true), ruvia::db_data_type::boolean)}))
                .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), vpn.call("now"))
                .where(vpn.binary(vpn.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                                  vpn.cast(vpn.value(nodeId), ruvia::db_data_type::uuid)));
            (void)co_await context.db().execute(vpn);
        }
        if (report.has_vpn() && report.vpn().supports_vpn()) {
            const auto& publicKey = report.vpn().public_key();
            if (validVpnPublicKey(publicKey)) {
                ruvia::db_query activatedQuery;
                activatedQuery.update(service::edge::persistence::VpnPeerEntity::table_name(), "p")
                    .set(service::edge::persistence::VpnPeerEntity::column_name<"public_key">(), activatedQuery.value(std::string_view(publicKey)))
                    .set(service::edge::persistence::VpnPeerEntity::column_name<"status">(), activatedQuery.value(std::string_view{"active"}))
                    .set(service::edge::persistence::VpnPeerEntity::column_name<"updated_at">(), activatedQuery.call("now"))
                    .update_from(service::edge::persistence::VpnNetworkEntity::table_name(), "n")
                    .where(activatedQuery.binary(
                        activatedQuery.column(service::edge::persistence::VpnPeerEntity::column_name<"peer_type">(), "p"),
                        ruvia::db_binary_operator::equal,
                        activatedQuery.value(std::string_view{"edge"})))
                    .and_where(activatedQuery.binary(
                        activatedQuery.column(service::edge::persistence::VpnPeerEntity::column_name<"edge_node_id">(), "p"),
                        ruvia::db_binary_operator::equal,
                        activatedQuery.cast(activatedQuery.value(nodeId),
                                            ruvia::db_data_type::uuid)))
                    .and_where(activatedQuery.binary(
                        activatedQuery.column(service::edge::persistence::VpnPeerEntity::column_name<"status">(), "p"),
                        ruvia::db_binary_operator::not_equal,
                        activatedQuery.value(std::string_view{"revoked"})))
                    .and_where(activatedQuery.binary(
                        activatedQuery.column(service::edge::persistence::VpnNetworkEntity::column_name<"id">(), "n"),
                        ruvia::db_binary_operator::equal,
                        activatedQuery.column(service::edge::persistence::VpnPeerEntity::column_name<"network_id">(), "p")))
                    .returning({activatedQuery.cast(activatedQuery.column(service::edge::persistence::VpnPeerEntity::column_name<"id">(), "p"),
                                                    ruvia::db_data_type::text),
                                activatedQuery.cast(activatedQuery.column(service::edge::persistence::VpnPeerEntity::column_name<"network_id">(), "p"),
                                                    ruvia::db_data_type::text),
                                activatedQuery.cast(activatedQuery.column(service::edge::persistence::VpnNetworkEntity::column_name<"created_by">(), "n"),
                                                    ruvia::db_data_type::text)});
                const auto activated = co_await context.db().query(activatedQuery);
                for (const auto& row : activated) {
                    auto transaction = co_await context.db().begin_transaction();
                    ruvia::db_query lock;
                    lock.select(lock.call(
                        "pg_advisory_xact_lock",
                        {lock.cast(lock.value(std::int64_t{5282804697543808068LL}),
                                   ruvia::db_data_type::big_int)}));
                    (void)co_await transaction.query(lock);
                    co_await service::vpn::feature::syncEdgeBridgeRoutes(
                        transaction, row[0].value().value_or(std::string_view{}),
                        row[1].value().value_or(std::string_view{}), nodeId,
                        row[2].value().value_or(std::string_view{}), *context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>());
                    co_await transaction.commit();
                    co_await service::vpn::queueEdgeConfig(
                        context, row[0].value().value_or(std::string_view{}));
                }
            }
        }
    }

    static ruvia::task<void> saveNetworkResult(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::NetworkConfigResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        const std::string status = result.success() ? "succeeded" : "failed";
        const std::string json = "{\"message\":\"" + jsonEscape(result.message()) +
                                 "\",\"rolled_back\":" +
                                 (result.rolled_back() ? "true" : "false") + "}";
        ruvia::db_query query;
        query.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), query.value(std::string_view(status)))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), query.cast(query.value(std::string_view(json)),
                                       ruvia::db_data_type::jsonb))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), query.call("now"))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), query.call("now"))
            .where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                                query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                                   query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal,
                                   query.value(std::string_view{"network"})))
            .and_where(query.binary(
                query.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in,
                query.list({query.value(std::string_view{"succeeded"}),
                            query.value(std::string_view{"failed"})})));
        (void)co_await context.db().execute(query);
    }

    static ruvia::task<void> saveVpnResult(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::VpnConfigResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        const bool applied = result.applied();
        const std::string status = applied ? "succeeded" : "failed";
        const std::string json = "{\"configVersion\":" +
                                 std::to_string(result.config_version()) +
                                 ",\"errorCode\":" + jsonQuoted(result.error_code()) +
                                 ",\"errorMessage\":" + jsonQuoted(result.error_message()) + "}";
        ruvia::db_query transitioned;
        transitioned.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), transitioned.value(std::string_view(status)))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), transitioned.cast(
                               transitioned.value(std::string_view(json)),
                               ruvia::db_data_type::jsonb))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), transitioned.call("now"))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), transitioned.call("now"))
            .where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(id), ruvia::db_data_type::uuid)))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal,
                transitioned.value(std::string_view{"vpn"})))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in,
                transitioned.list({transitioned.value(std::string_view{"succeeded"}),
                                   transitioned.value(std::string_view{"failed"})})))
            .returning({transitioned.alias(
                            config::detail::jsonText(
                                transitioned, transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">()), "peerId"),
                            "peer_id"),
                        transitioned.alias(
                            config::detail::jsonText(
                                transitioned, transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">()), "enabled"),
                            "enabled"),
                        transitioned.alias(
                            config::detail::jsonText(transitioned,
                                                     transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">()),
                                                     "configVersion"),
                            "config_version")});

        ruvia::db_query update;
        const auto appliedValue = update.cast(update.value(applied), ruvia::db_data_type::boolean);
        const auto enabled = update.coalesce({
            update.cast(update.column("enabled", "task"), ruvia::db_data_type::boolean),
            update.cast(update.value(true), ruvia::db_data_type::boolean)});
        // An on-demand stop changes tunnel runtime only. Preallocated mappings
        // remain available to desktop discovery and the next enabled client.
        const auto activeRoute = update.column(service::edge::persistence::VpnRouteEntity::column_name<"enabled">(), "route");
        const auto routeStatus = update.case_when(
            {{activeRoute,
              update.cast(update.value(std::string_view{"active"}),
                          ruvia::db_data_type::text)}},
            update.cast(update.value(std::string_view{"disabled"}),
                        ruvia::db_data_type::text));
        const auto statusValue = update.case_when(
            {{appliedValue, routeStatus}},
            update.cast(update.value(std::string_view{"error"}),
                        ruvia::db_data_type::text));
        const auto lastError = update.case_when(
            {{appliedValue, update.cast(update.value(std::string_view{}),
                                        ruvia::db_data_type::text)}},
            update.cast(update.value(std::string_view(result.error_message())),
                        ruvia::db_data_type::text));

        ruvia::db_query peer;
        const auto taskPeerId = peer.import_expression(
            update.column("peer_id", "task"), "task", "task");
        peer.select({peer.cast(peer.column(service::edge::persistence::VpnPeerEntity::column_name<"config_revision">(), "peer"),
                               ruvia::db_data_type::text)})
            .from(service::edge::persistence::VpnPeerEntity::table_name(), "peer")
            .where(peer.binary(peer.column(service::edge::persistence::VpnPeerEntity::column_name<"id">(), "peer"),
                               ruvia::db_binary_operator::equal,
                               peer.cast(taskPeerId, ruvia::db_data_type::uuid)));
        update.with("transitioned", transitioned)
            .update(service::edge::persistence::VpnRouteEntity::table_name(), "route")
            .set(service::edge::persistence::VpnRouteEntity::column_name<"status">(), statusValue)
            .set(service::edge::persistence::VpnRouteEntity::column_name<"last_error">(), lastError)
            .set(service::edge::persistence::VpnRouteEntity::column_name<"updated_at">(), update.call("now"))
            .update_from("transitioned", "task")
            .where(update.binary(
                update.column(service::edge::persistence::VpnRouteEntity::column_name<"edge_peer_id">(), "route"), ruvia::db_binary_operator::equal,
                update.cast(update.column("peer_id", "task"), ruvia::db_data_type::uuid)))
            .and_where(update.binary(update.subquery(peer),
                                    ruvia::db_binary_operator::equal,
                                    update.column("config_version", "task")))
            .and_where(enabled);
        (void)co_await context.db().execute(update);
    }

    static ruvia::task<void> saveFirmwareResult(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::FirmwareUpdateResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        std::string status = "running";
        bool completed = false;
        if (result.state() == pb::FIRMWARE_UPDATE_ACCEPTED)
            status = "accepted";
        else if (result.state() == pb::FIRMWARE_UPDATE_FAILED) {
            status = "failed";
            completed = true;
        }
        std::string state = "running";
        if (result.state() == pb::FIRMWARE_UPDATE_ACCEPTED)
            state = "accepted";
        else if (result.state() == pb::FIRMWARE_UPDATE_DOWNLOADING)
            state = "downloading";
        else if (result.state() == pb::FIRMWARE_UPDATE_VERIFYING)
            state = "verifying";
        else if (result.state() == pb::FIRMWARE_UPDATE_FLASHING)
            state = "flashing";
        else if (result.state() == pb::FIRMWARE_UPDATE_FAILED)
            state = "failed";
        const std::string json =
            "{\"state\":\"" + state + "\",\"message\":\"" +
            jsonEscape(result.message()) +
            "\",\"progressPercent\":" + std::to_string(result.progress_percent()) +
            ",\"downloadedBytes\":" + std::to_string(result.downloaded_bytes()) +
            ",\"totalBytes\":" + std::to_string(result.total_bytes()) + "}";
        ruvia::db_query query;
        query.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), query.value(std::string_view(status)))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), query.cast(query.value(std::string_view(json)),
                                       ruvia::db_data_type::jsonb))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), query.call("now"))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), query.case_when(
                                     {{query.cast(query.value(completed),
                                                  ruvia::db_data_type::boolean),
                                       query.call("now")} },
                                     query.null_value()))
            .where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                                query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                                   query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal,
                                   query.value(std::string_view{"firmware"})))
            .and_where(query.binary(
                query.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in,
                query.list({query.value(std::string_view{"succeeded"}),
                            query.value(std::string_view{"failed"})})));
        (void)co_await context.db().execute(query);
    }

    static ruvia::task<void> saveModemResult(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::ModemControlResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        std::string status = "running";
        bool completed = false;
        if (result.state() == pb::MODEM_CONTROL_ACCEPTED)
            status = "accepted";
        else if (result.state() == pb::MODEM_CONTROL_SUCCEEDED) {
            status = "succeeded";
            completed = true;
        } else if (result.state() == pb::MODEM_CONTROL_FAILED) {
            status = "failed";
            completed = true;
        }
        const std::string json = "{\"message\":\"" + jsonEscape(result.message()) +
                                 "\",\"apn\":\"" + jsonEscape(result.apn()) + "\"}";
        ruvia::db_query query;
        query.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), query.value(std::string_view(status)))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), query.cast(query.value(std::string_view(json)),
                                       ruvia::db_data_type::jsonb))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), query.call("now"))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), query.case_when(
                                     {{query.cast(query.value(completed),
                                                  ruvia::db_data_type::boolean),
                                       query.call("now")} },
                                     query.null_value()))
            .where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                                query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                                   query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal,
                                   query.value(std::string_view{"modem"})))
            .and_where(query.binary(
                query.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in,
                query.list({query.value(std::string_view{"succeeded"}),
                            query.value(std::string_view{"failed"})})));
        (void)co_await context.db().execute(query);
    }

    static ruvia::task<void> savePlatformResult(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::PlatformConfigResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        const std::string status = result.success() ? "succeeded" : "failed";
        const std::string json = "{\"message\":\"" + jsonEscape(result.message()) + "\"}";
        ruvia::db_query transitioned;
        transitioned.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), transitioned.value(std::string_view(status)))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), transitioned.cast(
                               transitioned.value(std::string_view(json)),
                               ruvia::db_data_type::jsonb))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), transitioned.call("now"))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), transitioned.call("now"))
            .where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(id), ruvia::db_data_type::uuid)))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::in,
                transitioned.list({transitioned.value(std::string_view{"platform_upsert"}),
                                   transitioned.value(std::string_view{"platform_delete"})})))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in,
                transitioned.list({transitioned.value(std::string_view{"succeeded"}),
                                   transitioned.value(std::string_view{"failed"})})))
            .returning({transitioned.alias(
                            transitioned.cast(
                                config::detail::jsonText(
                                    transitioned, transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">()), "platform_id"),
                                ruvia::db_data_type::uuid),
                            "platform_id"),
                        transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">())});

        ruvia::db_query updated;
        const auto success = updated.cast(updated.value(result.success()),
                                          ruvia::db_data_type::boolean);
        const auto deleteTask = updated.binary(
            updated.column("task_type", "task"), ruvia::db_binary_operator::equal,
            updated.value(std::string_view{"platform_delete"}));
        updated.update(service::edge::persistence::EdgeNodePlatformEntity::table_name(), "target")
            .set(service::edge::persistence::EdgeNodePlatformEntity::column_name<"status">(), updated.call(
                               "jsonb_build_object",
                               {config::detail::jsonKey(updated, "state"),
                                updated.cast(updated.value(std::string_view(
                                                               result.success() ? "applied"
                                                                                : "failed")),
                                             ruvia::db_data_type::text),
                                config::detail::jsonKey(updated, "message"),
                                updated.cast(updated.value(std::string_view(result.message())),
                                             ruvia::db_data_type::text)}))
            .set(service::edge::persistence::EdgeNodePlatformEntity::column_name<"updated_at">(), updated.call("now"))
            .update_from("transitioned", "task")
            .where(updated.binary(
                updated.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"node_id">(), "target"), ruvia::db_binary_operator::equal,
                updated.cast(updated.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(updated.binary(updated.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"platform_id">(), "target"),
                                     ruvia::db_binary_operator::equal,
                                     updated.column("platform_id", "task")))
            .and_where(updated.unary(
                ruvia::db_unary_operator::not_value,
                updated.binary(success, ruvia::db_binary_operator::and_value, deleteTask)))
            .returning({updated.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"platform_id">(), "target")});

        ruvia::db_query cleanup;
        const auto cleanupSuccess = cleanup.import_expression(success);
        const auto cleanupDeleteTask = cleanup.import_expression(deleteTask);
        cleanup.with("transitioned", transitioned)
            .with("updated", updated)
            .delete_from(service::edge::persistence::EdgeNodePlatformEntity::table_name(), "target")
            .delete_using("transitioned", "task")
            .where(cleanup.binary(cleanupSuccess, ruvia::db_binary_operator::and_value,
                                  cleanupDeleteTask))
            .and_where(cleanup.binary(
                cleanup.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"node_id">(), "target"), ruvia::db_binary_operator::equal,
                cleanup.cast(cleanup.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(cleanup.binary(cleanup.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"platform_id">(), "target"),
                                     ruvia::db_binary_operator::equal,
                                     cleanup.column("platform_id", "task")));
        (void)co_await context.db().execute(cleanup);
    }

    static std::string hex(std::string_view value) {
        constexpr char digits[] = "0123456789abcdef";
        std::string output;
        output.reserve(value.size() * 2);
        for (const char item : value) {
            const auto byte = static_cast<std::uint8_t>(item);
            output.push_back(digits[byte >> 4U]);
            output.push_back(digits[byte & 0x0fU]);
        }
        return output;
    }

    static ruvia::task<void> saveConfigApplied(ruvia::web_worker_context& context,
                                                std::string_view nodeId,
                                                const pb::ConfigApplied& result) {
        if (result.revision() == 0 || result.sha256().size() != 32)
            co_return;
        const auto digest = hex(result.sha256());
        ruvia::db_query revision;
        revision.update(service::edge::persistence::EdgeConfigRevisionEntity::table_name())
            .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"status">(), revision.value(std::string_view{"applied"}))
            .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"message">(), revision.value(std::string_view{}))
            .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"completed_at">(), revision.call("now"))
            .where(revision.binary(
                revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(revision.binary(
                revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"revision">()), ruvia::db_binary_operator::equal,
                revision.cast(revision.value(static_cast<std::int64_t>(result.revision())),
                              ruvia::db_data_type::big_int)))
            .and_where(revision.binary(revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"sha256">()),
                                      ruvia::db_binary_operator::equal,
                                      revision.value(std::string_view(digest))));
        (void)co_await context.db().execute(revision);

        ruvia::db_query node;
        const auto status = node.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
        const auto revisionValue = node.cast(
            node.value(static_cast<std::int64_t>(result.revision())),
            ruvia::db_data_type::big_int);
        const auto desired = config::detail::configVersion(node, status, "desiredVersion");
        const auto matches = node.binary(desired, ruvia::db_binary_operator::equal,
                                         revisionValue);
        const auto configWithActive = node.call(
            "jsonb_set",
            {status, config::detail::jsonPath(node, "{config,activeVersion}"),
             config::detail::toJsonb(
                 node, node.greatest({config::detail::configVersion(
                                           node, status, "activeVersion"),
                                      revisionValue})),
             node.cast(node.value(true), ruvia::db_data_type::boolean)});
        const auto configWithState = node.call(
            "jsonb_set",
            {configWithActive, config::detail::jsonPath(node, "{config,state}"),
             config::detail::toJsonb(
                 node, node.case_when(
                           {{matches, node.cast(node.value(std::string_view{"applied"}),
                                                ruvia::db_data_type::text)}},
                           config::detail::configState(node, status))),
             node.cast(node.value(true), ruvia::db_data_type::boolean)});
        const auto configWithMessage = node.call(
            "jsonb_set",
            {configWithState, config::detail::jsonPath(node, "{config,message}"),
             config::detail::toJsonb(
                 node, node.case_when(
                           {{matches, node.cast(node.value(std::string_view{}),
                                                ruvia::db_data_type::text)}},
                           node.coalesce({config::detail::jsonText(
                                              node,
                                              config::detail::jsonGet(node, status, "config"),
                                              "message"),
                                          node.value(std::string_view{})}))),
             node.cast(node.value(true), ruvia::db_data_type::boolean)});
        node.update(service::edge::persistence::EdgeNodeEntity::table_name())
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), configWithMessage)
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), node.call("now"))
            .where(node.binary(node.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                               node.cast(node.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(node);
    }

    static ruvia::task<void> saveConfigRejected(ruvia::web_worker_context& context,
                                                 std::string_view nodeId,
                                                 const pb::ConfigRejected& result) {
        if (result.revision() == 0)
            co_return;
        const std::string message = result.code() + ": " + result.message();
        ruvia::db_query revision;
        revision.update(service::edge::persistence::EdgeConfigRevisionEntity::table_name())
            .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"status">(), revision.value(std::string_view{"rejected"}))
            .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"message">(), revision.value(std::string_view(message)))
            .set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"completed_at">(), revision.call("now"))
            .where(revision.binary(
                revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal,
                revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(revision.binary(
                revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"revision">()), ruvia::db_binary_operator::equal,
                revision.cast(revision.value(static_cast<std::int64_t>(result.revision())),
                              ruvia::db_data_type::big_int)));
        (void)co_await context.db().execute(revision);

        ruvia::db_query node;
        const auto status = node.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
        const auto revisionValue = node.cast(
            node.value(static_cast<std::int64_t>(result.revision())),
            ruvia::db_data_type::big_int);
        const auto matches = node.binary(
            config::detail::configVersion(node, status, "desiredVersion"),
            ruvia::db_binary_operator::equal, revisionValue);
        const auto state = node.case_when(
            {{matches, node.cast(node.value(std::string_view{"rejected"}),
                                 ruvia::db_data_type::text)}},
            config::detail::configState(node, status));
        const auto messageValue = node.case_when(
            {{matches, node.cast(node.value(std::string_view(message)),
                                 ruvia::db_data_type::text)}},
            node.coalesce({config::detail::jsonText(
                               node, config::detail::jsonGet(node, status, "config"),
                               "message"),
                           node.value(std::string_view{})}));
        const auto statusWithState = node.call(
            "jsonb_set", {status, config::detail::jsonPath(node, "{config,state}"),
                           config::detail::toJsonb(node, state),
                           node.cast(node.value(true), ruvia::db_data_type::boolean)});
        node.update(service::edge::persistence::EdgeNodeEntity::table_name())
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), node.call(
                               "jsonb_set",
                               {statusWithState,
                                config::detail::jsonPath(node, "{config,message}"),
                                config::detail::toJsonb(node, messageValue),
                                node.cast(node.value(true),
                                          ruvia::db_data_type::boolean)}))
            .set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), node.call("now"))
            .where(node.binary(node.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                               node.cast(node.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(node);
    }

    static constexpr std::string_view kStoreTelemetryPart = R"lua(
if redis.call('HGET', KEYS[1], 'done') then return 0 end
local signature = redis.call('HGET', KEYS[1], 'signature')
if signature and signature ~= ARGV[1] then return redis.error_reply('telemetry part metadata conflict') end
local field = 'part:' .. ARGV[2]
local previous = redis.call('HGET', KEYS[1], field)
if previous and previous ~= ARGV[4] then return redis.error_reply('telemetry part content conflict') end
local bytes = tonumber(redis.call('HGET', KEYS[1], 'bytes') or '0')
if not previous then
    if bytes + #ARGV[4] > 4194304 then return redis.error_reply('telemetry upload size limit') end
    redis.call('HSET', KEYS[1], 'signature', ARGV[1], 'bytes', bytes + #ARGV[4], field, ARGV[4])
end
local parts = {}
for i = 0, tonumber(ARGV[3]) - 1 do
    local part = redis.call('HGET', KEYS[1], 'part:' .. i)
    if not part then return 0 end
    parts[#parts + 1] = part
end
return parts
)lua";

    static pb::TelemetryRecord assembleTelemetryParts(const std::vector<pb::TelemetryRecord>& parts) {
        if (parts.empty() || parts.size() > 256)
            throw std::runtime_error("invalid telemetry upload part count");
        auto result = parts.front();
        result.set_record_id(result.report_id());
        result.clear_values();
        result.clear_raw_payloads();
        result.clear_raw_requests();
        result.clear_raw_packet_ids();
        std::set<std::string> elements;
        std::size_t rawBytes = 0;
        for (std::size_t index = 0; index < parts.size(); ++index) {
            const auto& part = parts[index];
            if (part.part_index() != index || part.part_count() != parts.size() ||
                part.report_id() != result.report_id() || part.device_id() != result.device_id() ||
                part.endpoint_id() != result.endpoint_id() || part.protocol() != result.protocol() ||
                part.observed_at_ms() != result.observed_at_ms() ||
                part.function_code() != result.function_code() || part.function_name() != result.function_name() ||
                part.direction() != result.direction() || part.model_id() != result.model_id())
                throw std::runtime_error("inconsistent telemetry upload parts");
            for (const auto& value : part.values()) {
                if (!elements.insert(value.element_id()).second)
                    throw std::runtime_error("duplicate element in telemetry upload");
                *result.add_values() = value;
            }
            for (const auto& raw : part.raw_payloads()) {
                rawBytes += raw.size();
                if (raw.empty() || raw.size() > 4112 || rawBytes > 147456 ||
                    result.raw_payloads_size() >= 4095)
                    throw std::runtime_error("invalid telemetry original frame array");
                result.add_raw_payloads(raw);
            }
            if ((parts.front().raw_requests_size() != 0) != (part.raw_requests_size() != 0) ||
                (part.raw_requests_size() != 0 &&
                    (part.raw_requests_size() != part.raw_payloads_size() || part.values_size() != 0)))
                throw std::runtime_error("inconsistent telemetry business exchange parts");
            for (const auto& request : part.raw_requests()) {
                rawBytes += request.size();
                if (request.size() > 4096 || rawBytes > 147456)
                    throw std::runtime_error("invalid telemetry business request");
                result.add_raw_requests(request);
            }
            if (part.raw_packet_ids_size() != 0 && part.raw_packet_ids_size() != part.raw_payloads_size())
                throw std::runtime_error("original packet ID count mismatch");
            for (const auto& id : part.raw_packet_ids()) {
                if (id.size() != 16) throw std::runtime_error("invalid original packet ID");
                result.add_raw_packet_ids(id);
            }
        }
        if (result.raw_payloads().empty())
            throw std::runtime_error("telemetry upload has no original frames");
        result.clear_report_id();
        result.clear_part_count();
        result.clear_part_index();
        return result;
    }

    template <typename Redis>
    static ruvia::task<std::optional<pb::TelemetryRecord>> storeTelemetryPart(
        const Redis& redis, std::string_view nodeId, const pb::TelemetryRecord& record) {
        if (record.report_id().size() != 16 || record.record_id().size() != 16 ||
            record.device_id().size() != 16 || record.part_count() == 0 ||
            record.part_count() > 256 || record.part_index() >= record.part_count() ||
            (record.protocol() != pb::PROTOCOL_SL651 && record.protocol() != pb::PROTOCOL_MODBUS &&
             record.protocol() != pb::PROTOCOL_S7 && record.protocol() != pb::PROTOCOL_MC &&
             record.protocol() != pb::PROTOCOL_FINS && record.protocol() != pb::PROTOCOL_DLT645) ||
            record.ByteSizeLong() > 14000)
            throw std::runtime_error("invalid telemetry upload part");
        const persistence::TelemetryUploadRecord stored{std::string(nodeId),
            protocol::uuidText(record.device_id()), protocol::uuidText(record.report_id())};
        auto metadata = record;
        metadata.clear_record_id();
        metadata.clear_values();
        metadata.clear_raw_payloads();
        metadata.clear_raw_requests();
        metadata.clear_raw_packet_ids();
        metadata.clear_part_index();
        const auto signature = metadata.SerializeAsString();
        const auto wire = record.SerializeAsString();
        const auto index = std::to_string(record.part_index());
        const auto count = std::to_string(record.part_count());
        const auto key = stored.key();
        const std::string_view keys[]{key};
        const std::string_view args[]{signature, index, count, wire};
        const auto reply = co_await redis.eval(kStoreTelemetryPart, keys, args);
        if (reply.kind() == ruvia::redis_value::kind_type::integer && reply.integer() == 0)
            co_return std::nullopt;
        if (reply.kind() != ruvia::redis_value::kind_type::array)
            service::message::redis::throwValue("store telemetry upload", reply);
        std::vector<pb::TelemetryRecord> parts;
        for (const auto& item : reply.array()) {
            if (item.kind() != ruvia::redis_value::kind_type::string)
                throw std::runtime_error("invalid stored telemetry part");
            auto& part = parts.emplace_back();
            if (!part.ParseFromString(std::string(item.string())))
                throw std::runtime_error("corrupt stored telemetry part");
        }
        co_return assembleTelemetryParts(parts);
    }

    template <typename Redis>
    static ruvia::task<void> finishTelemetryUploads(
        const Redis& redis, const std::vector<persistence::TelemetryUploadRecord>& uploads) {
        // 未完成分块保持持久化，不靠 TTL 丢弃已确认的数据。业务消息已进入
        // 持久队列后才回收分块，保留七天去重回执；重放仍受历史记录幂等约束。
        static constexpr std::string_view script = R"lua(
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], 'done', '1')
redis.call('EXPIRE', KEYS[1], 604800)
return 1
)lua";
        for (const auto& upload : uploads) {
            const auto key = upload.key();
            const std::string_view keys[]{key};
            const auto reply = co_await redis.eval(script, keys, std::span<const std::string_view>{});
            if (reply.kind() != ruvia::redis_value::kind_type::integer || reply.integer() != 1)
                service::message::redis::throwValue("finish telemetry upload", reply);
        }
    }

    static void collectTelemetry(const metadata::Catalog& catalog,
                          std::string_view nodeId, std::int64_t receivedAtMs,
                          const pb::TelemetryBatch& batch,
                          std::vector<service::message::StreamMessage>& messages,
                          const std::vector<pb::ConfigItem>& parsingItems = {}) {
        const auto node = catalog.find(std::string(nodeId));
        if (node == catalog.end())
            return;
        for (const auto& record : batch.records()) {
            if (record.record_id().size() != 16 || record.device_id().size() != 16)
                continue;
            const auto deviceId = protocol::uuidText(record.device_id());
            const auto device = node->second.find(deviceId);
            if (device == node->second.end())
                continue;
            message::ParsedDeviceMessage parsed;
            if (record.model_id().size() == 16) {
                parsed.modelId = protocol::uuidText(record.model_id());
            } else if (!record.model_id().empty()) {
                throw std::runtime_error("invalid edge telemetry model reference");
            }
            parsed.messageId = protocol::uuidText(record.record_id());
            parsed.acquisitionId = parsed.messageId;
            parsed.causationId = parsed.messageId;
            parsed.linkId = device->second.linkId;
            parsed.deviceId = deviceId;
            parsed.deviceCode = device->second.deviceCode;
            parsed.protocol = protocol::TelemetryValues::protocolName(record.protocol());
            if (parsed.protocol.empty())
                parsed.protocol = device->second.protocol;
            parsed.connectionId = std::string(nodeId);
            parsed.occurredAtMs = receivedAtMs;
            parsed.observedAtMs = record.observed_at_ms();
            parsed.storagePolicy = device->second.storagePolicy;
            parsed.onlineWindowMs = device->second.onlineWindowMs;
            parsed.source = record.derived_update() ? "derived" : "edge";
            if (record.raw_requests_size() != 0) {
                const auto configured = acquisition::RawAcquisition::device(parsingItems, record.device_id());
                if (configured.linkId != device->second.linkId ||
                    (record.endpoint_id().size() == 16 && protocol::uuidText(record.endpoint_id()) != configured.linkId))
                    throw std::runtime_error("edge acquisition endpoint mismatch");
                const auto decoded = acquisition::RawAcquisition::decode(record, configured);
                parsed.valuesJson = decoded.valuesJson;
                parsed.observedAtMs = decoded.observedAtMs;
            } else parsed.valuesJson = protocol::TelemetryValues::telemetryJson(record);
            if (record.raw_packet_ids_size() != 0 &&
                record.raw_packet_ids_size() != record.raw_payloads_size())
                throw std::runtime_error("original packet ID count mismatch");
            for (int index = 0; index < record.raw_payloads_size(); ++index) {
                std::string packetId;
                if (record.raw_packet_ids_size()) {
                    const auto& id = record.raw_packet_ids(index);
                    if (id.size() != 16) throw std::runtime_error("invalid original packet ID");
                    packetId = std::string(nodeId) + ':' + protocol::uuidText(id);
                } else packetId = std::string(nodeId) + ":legacy-history:" + parsed.messageId + ':' + std::to_string(index);
                if (record.raw_requests_size() && !record.raw_requests(index).empty()) {
                    const auto& request = record.raw_requests(index);
                    parsed.rawPayloads.emplace_back(request.begin(), request.end());
                    parsed.rawPacketIds.push_back(packetId + ":request");
                }
                const auto& response = record.raw_payloads(index);
                parsed.rawPayloads.emplace_back(response.begin(), response.end());
                parsed.rawPacketIds.push_back(std::move(packetId));
            }
            service::message::StreamMessage streamMessage;
            streamMessage.fields = message::parsedFields(parsed);
            messages.push_back(std::move(streamMessage));
        }
    }

    ruvia::task<void> saveCommandResult(ruvia::web_worker_context& context,
                                        const metadata::Catalog& catalog,
                                        std::string_view nodeId,
                                        std::int64_t receivedAtMs,
                                        const pb::CommandResult& result) {
        if (result.command_id().size() != 16 || result.device_id().size() != 16)
            co_return;
        if (!protocol::persistableCommandResult(result.state(), result.write_ack_missing()))
            co_return;
        const auto commandId = protocol::uuidText(result.command_id());
        const auto deviceId = protocol::uuidText(result.device_id());
        const auto node = catalog.find(std::string(nodeId));
        if (node == catalog.end())
            co_return;
        const auto device = node->second.find(deviceId);
        if (device == node->second.end())
            co_return;
        const auto domainState = protocol::commandResultDomainState(
            result.state(), result.write_ack_missing(), result.message());
        const bool success = domainState == "SUCCEEDED";
        const std::string state(domainState);
        std::string reason = result.message();
        if (result.write_ack_missing()) {
            reason = "write_ack_missing";
            if (!result.message().empty())
                reason += ": " + result.message();
        }
        const auto completedAtMs = message::effectiveObservedAt(
            result.completed_at_ms(), receivedAtMs);
        std::vector<message::StreamField> fields{
            {"message_id", context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next()},
            {"causation_id", commandId},
            {"command_id", commandId},
            {"device_id", deviceId},
            {"device_code", device->second.deviceCode},
            {"protocol", device->second.protocol},
            {"attempt", "1"},
            {"success", success ? "1" : "0"},
            {"result_state", state},
            {"reason", reason},
            {"worker_id", "0"},
            {"created_at_ms", std::to_string(message::utcNowMilliseconds())},
            {"completed_at_ms", std::to_string(completedAtMs)},
            {"actual_value_count", std::to_string(result.actual_values_size())}};
        for (int index = 0; index < result.actual_values_size(); ++index) {
            const auto& actual = result.actual_values(index);
            const auto prefix = "actual_value_" + std::to_string(index) + "_";
            fields.push_back({prefix + "element_id", actual.element_id()});
            fields.push_back({prefix + "name", actual.name()});
            fields.push_back({prefix + "kind",
                              actual.has_value() ? protocol::TelemetryValues::scalarKind(actual.value()) : "UNSPECIFIED"});
            fields.push_back({prefix + "value",
                              actual.has_value() ? protocol::TelemetryValues::scalarText(actual.value()) : std::string{}});
            fields.push_back({prefix + "unit", actual.unit()});
        }
        (void)co_await message::redis::publishAndWake(
            context.redis(), message::commandResultStream(), fields,
            std::nullopt,
            service::message::WorkerStreamTask::CommandResult, 10000);
    }

    static std::string deviceStatusKey(std::string_view nodeId, std::string_view deviceId) {
        return "iot:runtime:edge:" + std::string(nodeId) + ":device:" +
               std::string(deviceId);
    }

    static ruvia::task<void> saveDeviceStatus(ruvia::web_worker_context& context,
                                               std::string_view nodeId,
                                               const pb::DeviceStatusReport& report) {
        bool updated = false;
        for (const auto& status : report.devices()) {
            if (status.device_id().size() != 16)
                continue;
            const auto deviceId = protocol::uuidText(status.device_id());
            std::string clients;
            for (const auto& client : status.clients()) {
                if (!clients.empty())
                    clients.push_back(',');
                clients += client;
            }
            const auto key = deviceStatusKey(nodeId, deviceId);
            co_await service::message::redis::eraseHash(context.redis(), key);
            const std::vector<message::StreamField> fields{
                 {"node_id", std::string(nodeId)},
                 {"device_id", deviceId},
                 {"state", status.state()},
                 {"reason", status.reason()},
                 {"client_count", std::to_string(status.client_count())},
                 {"clients", clients},
                 {"last_activity_at_ms", std::to_string(status.last_activity_at_ms())},
                 {"updated_at_ms", std::to_string(service::message::utcNowMilliseconds())}
            };
            co_await service::message::redis::setHash(context.redis(), key, fields);
            const std::vector<std::string> pexpireArguments{"PEXPIRE", key, "900000"};
            (void)co_await service::message::redis::command(
                context.redis(), pexpireArguments);
            updated = true;
        }
        if (updated)
            co_await service::telemetry::latest::publishRealtimeChange(context.redis());
    }

    static std::string jsonEscape(std::string_view value) {
        std::string output;
        output.reserve(value.size());
        for (const char ch : value) {
            if (ch == '"' || ch == '\\')
                output.push_back('\\');
            if (static_cast<unsigned char>(ch) >= 0x20U)
                output.push_back(ch);
        }
        return output;
    }

    static std::string jsonQuoted(std::string_view value) {
        return "\"" + jsonEscape(value) + "\"";
    }

    template <typename Redis>
    static ruvia::task<bool> acquireLease(const Redis& redis, std::size_t index) {
        static constexpr std::string_view script = R"lua(
if redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') then return 1 end
return 0
)lua";
        const auto key = projector_stream::leaseKey(index, service::runtime::instanceId());
        const auto token = projector_stream::ownerToken(index, service::runtime::instanceId());
        const auto ttl = std::to_string(projector_stream::kLeaseTtl.count());
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token, ttl };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("acquire edge projector lease", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<bool> renewLeaseKey(const Redis& redis, std::string_view key, std::string_view token) {
        static constexpr std::string_view script = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
)lua";
        const auto ttl = std::to_string(projector_stream::kLeaseTtl.count());
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token, ttl };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("renew edge projector lease", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<bool> leaseOwned(const Redis& redis, std::string_view key, std::string_view token) {
        static constexpr std::string_view script = R"lua(
if redis.call('GET', KEYS[1]) == ARGV[1] then return 1 end
return 0
)lua";
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("check edge projector lease", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<bool> renewLease(const Redis& redis, std::size_t index) {
        const auto key = projector_stream::leaseKey(index, service::runtime::instanceId());
        const auto token = projector_stream::ownerToken(index, service::runtime::instanceId());
        co_return co_await renewLeaseKey(redis, key, token);
    }

    template <typename Redis>
    static ruvia::task<bool> releaseLeaseKey(const Redis& redis, std::string_view key, std::string_view token) {
        static constexpr std::string_view script = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
)lua";
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("release edge projector lease", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<bool> releaseLease(const Redis& redis, std::size_t index) {
        const auto key = projector_stream::leaseKey(index, service::runtime::instanceId());
        const auto token = projector_stream::ownerToken(index, service::runtime::instanceId());
        co_return co_await releaseLeaseKey(redis, key, token);
    }

    template <typename Redis>
    static ruvia::task<void> registerStream(const Redis& redis, std::string_view streamName) {
        const std::vector<std::string> replyArguments{ "SADD", std::string(projector_stream::kStreamRegistry), std::string(streamName) };
        const auto reply = co_await service::message::redis::command(
            redis,
            replyArguments
        );
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("register edge projector stream", reply);
        }
    }

    static std::optional<std::pair<std::string, std::size_t>> ownerFromStream(
        std::string_view streamName
    ) {
        if (!streamName.starts_with(projector_stream::kStreamPrefix)) {
            return std::nullopt;
        }
        const auto suffix = streamName.substr(projector_stream::kStreamPrefix.size());
        const auto separator = suffix.rfind(':');
        if (separator == std::string_view::npos || separator == 0 ||
            separator + 1 >= suffix.size()) {
            return std::nullopt;
        }
        const auto instance = suffix.substr(0, separator);
        if (instance.find(':') != std::string_view::npos) {
            return std::nullopt;
        }
        std::uint64_t parsedIndex = 0;
        const auto indexText = suffix.substr(separator + 1);
        const auto [end, error] = std::from_chars(
            indexText.data(),
            indexText.data() + indexText.size(),
            parsedIndex
        );
        if (error != std::errc{} || end != indexText.data() + indexText.size() ||
            parsedIndex > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
            return std::nullopt;
        }
        return std::pair{ std::string(instance), static_cast<std::size_t>(parsedIndex) };
    }

    static std::string recoveryToken(std::size_t workerIndex, const std::pair<std::string, std::size_t>& owner) {
        return std::string(service::runtime::instanceId()) + ":recovery:" +
            std::to_string(workerIndex) + ":" + owner.first + ":" +
            std::to_string(owner.second);
    }

    template <typename Redis>
    static ruvia::task<bool> claimRecoveryLease(const Redis& redis, std::string_view key, std::string_view token) {
        static constexpr std::string_view script = R"lua(
if redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') then return 1 end
return 0
)lua";
        const auto ttl = std::to_string(projector_stream::kLeaseTtl.count());
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token, ttl };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("claim dead edge projector lease", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<std::vector<ProjectorRecoveryStream>> discoverDeadStreams(
        const Redis& redis,
        std::string_view currentStream,
        std::size_t workerIndex
    ) {
        std::map<std::string, ProjectorRecoveryStream, std::less<>> dead;
        const auto candidates = co_await service::message::redis::keysMatching(
            redis,
            std::string(projector_stream::kStreamPrefix) + "*"
        );
        for (const auto& candidate : candidates) {
            if (candidate == currentStream) {
                continue;
            }
            const auto owner = ownerFromStream(candidate);
            if (!owner) {
                continue;
            }
            const auto lease = projector_stream::leaseKey(owner->second, owner->first);
            const auto token = recoveryToken(workerIndex, *owner);
            const auto current = co_await redis.get(lease);
            if (current) {
                if (std::string_view(current->data(), current->size()) != token) {
                    // A non-expired owner or another recovery worker still owns
                    // this stream.  Neither case is safe to steal.
                    continue;
                }
                // Seeing our recovery token is not enough to keep reading: the
                // lease may expire between discovery and the first XREADGROUP.
                // Renewal is token-checked and never creates a missing lease.
                if (!co_await renewLeaseKey(redis, lease, token)) {
                    continue;
                }
            } else if (!co_await claimRecoveryLease(redis, lease, token)) {
                continue;
            }
            dead.emplace(candidate, ProjectorRecoveryStream{ candidate, lease, token });
        }
        std::vector<ProjectorRecoveryStream> result;
        result.reserve(dead.size());
        for (auto& [streamName, deadStream] : dead) {
            (void)streamName;
            result.push_back(std::move(deadStream));
        }
        co_return result;
    }

    template <typename Redis>
    static ruvia::task<bool> eraseDeadStream(const Redis& redis, const ProjectorRecoveryStream& deadStream) {
        static constexpr std::string_view script = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) ~= 0 then
  if redis.call('XLEN', KEYS[2]) ~= 0 then return 0 end
  local groups = redis.call('XINFO', 'GROUPS', KEYS[2])
  for _, group in ipairs(groups) do
    for index = 1, #group, 2 do
      if group[index] == 'pending' and tonumber(group[index + 1]) > 0 then
        return 0
      end
    end
  end
  redis.call('DEL', KEYS[2])
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[3], KEYS[2])
return 1
)lua";
        const std::string registry(projector_stream::kStreamRegistry);
        const std::string_view keys[]{ deadStream.lease, deadStream.stream, registry };
        const std::string_view arguments[]{ deadStream.token };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("erase dead edge projector stream", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<bool> acknowledgeAndDeleteFenced(
        const Redis& redis,
        std::string_view lease,
        std::string_view token,
        std::string_view stream,
        std::string_view group,
        const std::vector<service::message::StreamMessage>& messages
    ) {
        if (messages.empty()) {
            co_return true;
        }
        static constexpr std::string_view script = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local group = ARGV[2]
for index = 3, #ARGV do
  redis.call('XACK', KEYS[2], group, ARGV[index])
  redis.call('XDEL', KEYS[2], ARGV[index])
end
return 1
)lua";
        const std::string leaseKey(lease);
        const std::string streamKey(stream);
        const std::string_view keys[]{ leaseKey, streamKey };
        std::vector<std::string> scriptArguments;
        scriptArguments.reserve(messages.size() + 2);
        scriptArguments.emplace_back(token);
        scriptArguments.push_back(std::string(group));
        for (const auto& message : messages) {
            scriptArguments.push_back(message.id);
        }
        std::vector<std::string_view> scriptViews;
        scriptViews.reserve(scriptArguments.size());
        for (const auto& argument : scriptArguments) {
            scriptViews.emplace_back(argument);
        }
        const auto reply = co_await redis.eval(
            script,
            keys,
            std::span<const std::string_view>(scriptViews)
        );
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("fenced edge projector acknowledgement", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<void> cleanupRegistry(const Redis& redis) {
        static constexpr std::string_view script = R"lua(
if redis.call('EXISTS', KEYS[2]) == 0 then
  return redis.call('SREM', KEYS[1], KEYS[2])
end
return 0
)lua";
        const std::vector<std::string> replyArguments{ "SMEMBERS", std::string(projector_stream::kStreamRegistry) };
        const auto reply = co_await service::message::redis::command(
            redis,
            replyArguments
        );
        if (reply.kind() == ruvia::redis_value::kind_type::error &&
            reply.error().starts_with("WRONGTYPE")) {
            service::message::redis::throwValue("list edge projector streams", reply);
        }
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            if (reply.kind() == ruvia::redis_value::kind_type::null) {
                co_return;
            }
            service::message::redis::throwValue("list edge projector streams", reply);
        }
        for (const auto& value : reply.array()) {
            if (value.kind() != ruvia::redis_value::kind_type::string) {
                continue;
            }
            const auto streamName = std::string(value.string());
            const std::string registry(projector_stream::kStreamRegistry);
            const std::string_view keys[]{ registry, streamName };
            const std::span<const std::string_view> arguments;
            const auto removed = co_await redis.eval(
                script,
                keys,
                arguments
            );
            if (removed.kind() != ruvia::redis_value::kind_type::integer) {
                service::message::redis::throwValue("clean edge projector registry", removed);
            }
        }
    }


};

} // namespace service::edge

namespace service::edge {

class EdgeControlService final {
  public:
    static ruvia::task<std::string> executeOperation(ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        if (stop.stop_requested()) {
            service::common::fail(10004, "Edge operation cancelled", 503);
        }
        if (operation == "queue-snapshot") {
            const auto separator = payload.find('\n');
            if (separator == std::string_view::npos) {
                service::common::fail(10002, "Invalid edge snapshot payload", 400);
            }
            const auto nodeId = payload.substr(0, separator);
            const auto actorId = payload.substr(separator + 1);
            if (!service::common::isUuid(nodeId) || !service::common::isUuid(actorId)) {
                service::common::fail(10002, "Invalid edge snapshot identifiers", 400);
            }
            const auto revision = co_await configService().queueSnapshot(
                context,
                nodeId,
                actorId
            );
            co_return std::to_string(revision);
        }

        const auto separator = payload.find('\n');
        if (separator == std::string_view::npos || !service::common::isUuid(payload.substr(0, separator))) {
            service::common::fail(10002, "Invalid edge control payload", 400);
        }
        const auto nodeId = payload.substr(0, separator);
        const auto requestPayload = payload.substr(separator + 1);
        if (requestPayload.empty()) {
            service::common::fail(10002, "Empty edge control request", 400);
        }

        pb::Envelope envelope = service::edge::protocol::outbound(context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), service::message::utcNowMilliseconds(), context.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
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
        co_await dispatch::enqueue(context.redis(), nodeId, wire);
        co_return "{}";
    }
};

} // namespace service::edge
