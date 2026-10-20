#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/access/access.service.h"
#include "service/features/configuration/configuration.entity.h"
#include "service/features/live/live.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/telemetry/latest/latest.entity.h"
#include "service/features/telemetry/latest/latest.service.h"
#include "service/features/telemetry/telemetry.entity.h"
#include "service/features/telemetry/telemetry.protocol.h"
#include "service/utils/json.h"

namespace service::telemetry {

// Dispatch 读取 parsedStream() 并复制到各消费者流；告警复制流由 alert 组件消费。
enum class Consumer { Dispatch,
                      History,
                      Latest,
                      Delivery };

inline std::string consumerStream(Consumer consumer) {
    switch (consumer) {
        case Consumer::History:
            return std::string(message::telemetry::kHistoryStream);
        case Consumer::Latest:
            return std::string(message::telemetry::kLatestStream);
        case Consumer::Delivery:
            return std::string(message::telemetry::kDeliveryStream);
        case Consumer::Dispatch:
            break;
    }
    return message::parsedStream();
}

// 一批遥测的复制回执与四份消费者副本在一个脚本内原子提交；消费者流从不修剪待确认条目，各消费者自行 XACK+XDEL。
// KEYS[1..4]: history、latest、alerts、delivery 消费者流；KEYS[5]: 本实例共享唤醒流；KEYS[6..10]: 对应流的输入通知频道；KEYS[11..]: 每条遥测的复制回执键。
// ARGV[1]: 唤醒流容量；ARGV[2]: 告警消费者的唤醒任务名；之后按条目顺序依次为 <字段名值个数> <name> <value> ...
// 回执已存在时不重复复制；若重复投递携带原始帧身份（raw_packet_ids 非空），只向 history 追加以补记原始帧关联。
// 每次实际追加后在同一脚本发布新条目 ID；无副本追加的重复投递不通知。返回新复制的条目数。
inline constexpr std::string_view kFanoutScript = R"lua(
for i=1,4 do
 local kind = redis.call('TYPE',KEYS[i]).ok
 if kind ~= 'none' and kind ~= 'stream' then
  return redis.error_reply('telemetry consumer key must be a stream')
 end
end
local copied = 0
local cursor = 3
for entry=11,#KEYS do
 local count = tonumber(ARGV[cursor])
 if not count then return redis.error_reply('telemetry fanout field count is missing') end
 local fields = {'*'}
 local replay = false
 for i=cursor+1,cursor+count,2 do
  fields[#fields+1]=ARGV[i]
  fields[#fields+1]=ARGV[i+1]
  if ARGV[i] == 'raw_packet_ids' and ARGV[i+1] ~= '[]' then replay = true end
 end
 cursor = cursor + count + 1
 if redis.call('EXISTS',KEYS[entry]) ~= 0 then
  if replay then
   local id = redis.call('XADD',KEYS[1],unpack(fields))
   redis.call('PUBLISH',KEYS[6],id)
  end
 else
  for i=1,4 do
   local id = redis.call('XADD',KEYS[i],unpack(fields))
   redis.call('PUBLISH',KEYS[i+5],id)
  end
  redis.call('SET',KEYS[entry],'1','EX',604800)
  copied = copied + 1
 end
end
if copied > 0 then
 local id = redis.call('XADD',KEYS[5],'MAXLEN','~',ARGV[1],'*','task',ARGV[2])
 redis.call('PUBLISH',KEYS[10],id)
end
return copied
)lua";

// 每条遥测只解析、规范化与修复 UTF-8 一次，整批一次 EVAL 复制到全部消费者流。
template <class Redis>
ruvia::task<void> fanout(const Redis& redis, const service::message::RedisChannelScope& scope, const std::vector<message::StreamMessage>& messages, std::string_view instanceId) {
    if (messages.empty()) {
        co_return;
    }
    std::vector<std::string> receipts;
    std::vector<std::vector<message::StreamField>> entries;
    std::vector<std::string> counts;
    receipts.reserve(messages.size());
    entries.reserve(messages.size());
    counts.reserve(messages.size());
    std::size_t argumentCount = 0;
    for (const auto& input : messages) {
        auto parsed = message::parsedFrom(input);
        contract::normalize(parsed);
        contract::repairInvalidUtf8(parsed);
        receipts.push_back(std::string(message::telemetry::kFanoutReceiptPrefix) + parsed.messageId);
        auto fields = message::parsedFields(parsed);
        counts.push_back(std::to_string(fields.size() * 2));
        argumentCount += 1 + fields.size() * 2;
        entries.push_back(std::move(fields));
    }
    const auto wakeStream = message::workerWakeStream(std::nullopt, instanceId);
    const std::string inputChannels[]{
        message::consumer::inputChannel(scope, message::telemetry::kHistoryStream),
        message::consumer::inputChannel(scope, message::telemetry::kLatestStream),
        message::consumer::inputChannel(scope, message::telemetry::kAlertsStream),
        message::consumer::inputChannel(scope, message::telemetry::kDeliveryStream),
        message::consumer::inputChannel(scope, wakeStream)
    };
    std::vector<std::string_view> keys;
    keys.reserve(10 + receipts.size());
    keys.insert(keys.end(), { message::telemetry::kHistoryStream, message::telemetry::kLatestStream, message::telemetry::kAlertsStream, message::telemetry::kDeliveryStream, wakeStream });
    for (const auto& channel : inputChannels) {
        keys.push_back(channel);
    }
    for (const auto& receipt : receipts) {
        keys.push_back(receipt);
    }
    const auto wakeCapacity = std::to_string(message::kWorkerWakeCapacity);
    std::vector<std::string_view> arguments{ wakeCapacity, message::workerStreamTaskName(message::WorkerStreamTask::TelemetryAlerts) };
    arguments.reserve(2 + argumentCount);
    for (std::size_t index = 0; index < entries.size(); ++index) {
        arguments.push_back(counts[index]);
        for (const auto& field : entries[index]) {
            arguments.push_back(field.name);
            arguments.push_back(field.value);
        }
    }
    const auto reply = co_await redis.eval(kFanoutScript, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        utils::redis::throwValue("telemetry fanout", reply);
    }
}

class TelemetryService {
  public:
    template <typename Redis>
    static ruvia::task<std::optional<std::int64_t>> retireExpiredFreshness(const Redis& redis, const service::message::RedisChannelScope& scope) {
        co_await latest::expireStale(redis, scope);
        co_await latest::expireConnections(redis, scope);
        co_return co_await latest::nextDeadline(redis);
    }

    static ruvia::task<std::string> executeProjection(ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        if (stop.stop_requested()) {
            service::common::fail(10004, "Telemetry operation cancelled", 503);
        }
        const auto pair = service::message::rpc::decodeLinePair(payload);
        const auto id = pair ? pair->first : payload;
        if (!service::common::isUuid(id)) {
            service::common::fail(10002, "Invalid telemetry object identifier", 400);
        }
        if (operation == "initialize" && pair) {
            co_await latest::initializeDevice(context.redis(), context.worker_state<std::optional<common::WorkerIdentity>>().value().channels, id, pair->second);
            co_await latest::projectDevice(context, id);
        } else if (operation == "project-device") {
            co_await latest::projectDevice(context, id);
        } else if (operation == "project-protocol") {
            co_await latest::projectProtocol(context, id);
        } else if (operation == "erase-device") {
            co_await latest::eraseDevice(context.redis(), context.worker_state<std::optional<common::WorkerIdentity>>().value().channels, id);
        } else {
            service::common::fail(10002, "Unknown telemetry operation", 400);
        }
        co_return "{}";
    }

    static ruvia::task<void> ingest(ruvia::web_worker_context& context, const std::vector<message::StreamMessage>& messages) {
        co_await fanout(context.redis(), context.worker_state<std::optional<common::WorkerIdentity>>().value().channels, messages, context.worker_state<std::optional<common::WorkerIdentity>>().value().instanceId);
    }

    // 消费者副本已由 Dispatch 规范化并修复 UTF-8；这里只解码流字段，并对升级前未修复的副本按需修复（合法文本不复制）。
    static ruvia::task<void> apply(ruvia::web_worker_context& context, Consumer consumer, const std::vector<message::StreamMessage>& messages) {
        if (consumer == Consumer::Dispatch) {
            co_await ingest(context, messages);
            co_return;
        }
        std::vector<message::ParsedDeviceMessage> parsedMessages;
        parsedMessages.reserve(messages.size());
        for (const auto& message : messages) {
            auto parsed = message::parsedFrom(message);
            contract::repairInvalidUtf8(parsed);
            if (consumer != Consumer::Latest && contract::isSl651EmptyReport(parsed)) {
                continue;
            }
            parsedMessages.push_back(std::move(parsed));
        }
        if (parsedMessages.empty()) {
            co_return;
        }
        const auto redis = context.redis();
        const auto& scope = context.worker_state<std::optional<common::WorkerIdentity>>().value().channels;
        switch (consumer) {
            case Consumer::History: {
                co_await persist(context, parsedMessages);
                std::vector<std::string> deviceIds;
                deviceIds.reserve(parsedMessages.size());
                for (const auto& parsed : parsedMessages) {
                    deviceIds.emplace_back(parsed.deviceId);
                }
                std::ranges::sort(deviceIds);
                deviceIds.erase(std::unique(deviceIds.begin(), deviceIds.end()), deviceIds.end());
                // Latest publishes device.realtime for the same samples; History
                // only tells readers of persisted rows (open history stream).
                co_await service::live::publish(redis, scope, service::message::live::topic::kDeviceHistory, deviceIds);
                break;
            }
            case Consumer::Latest:
                co_await latest::update(redis, scope, parsedMessages, context.worker_state<std::optional<common::WorkerIdentity>>().value().instanceId);
                break;
            case Consumer::Delivery:
                co_await service::access::event::publishMany(redis, scope, parsedMessages, context.worker_state<std::optional<common::WorkerIdentity>>().value().instanceId);
                break;
            case Consumer::Dispatch:
                break;
        }
    }

  protected:
    static ruvia::db_query persistenceQuery(std::pmr::memory_resource* resource, const std::vector<message::ParsedDeviceMessage>& messages) {
        std::vector<std::string> rawPayloadArrays;
        rawPayloadArrays.reserve(messages.size());
        for (const auto& message : messages) {
            rawPayloadArrays.push_back(message::rawPayloadsJson(message.rawPayloads));
        }

        using Query = ruvia::db_query;
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        const auto selectColumns = [](Query& query, std::initializer_list<std::string_view> columns, std::string_view table = {}) {
            for (const auto column : columns) {
                query.add_select(query.column(column, table));
            }
        };
        const auto emptyJson = [](Query& query) {
            return query.cast(query.value("{}"), Type::jsonb);
        };
        const auto timestamp = [](Query& query, std::int64_t milliseconds) {
            return query.call("to_timestamp", { query.binary(query.cast(query.value(milliseconds), Type::double_value), Op::divide, query.value(1000.0)) });
        };
        Query incoming(resource);
        for (std::size_t index = 0; index < messages.size(); ++index) {
            const auto& parsed = messages[index];
            incoming.values({ incoming.cast(incoming.value(static_cast<std::int64_t>(index)), Type::big_int), timestamp(incoming, parsed.observedAtMs), incoming.cast(incoming.value(parsed.messageId), Type::uuid), incoming.cast(incoming.value(parsed.deviceId), Type::uuid), incoming.cast(incoming.value(parsed.linkId), Type::uuid), incoming.cast(incoming.value(parsed.connectionId), Type::uuid), incoming.cast(incoming.value(parsed.protocol), Type::text), incoming.cast(incoming.value(parsed.source), Type::text), timestamp(incoming, parsed.occurredAtMs), incoming.cast(incoming.value(parsed.valuesJson), Type::jsonb), incoming.cast(incoming.value(rawPayloadArrays[index]), Type::jsonb), incoming.cast(incoming.value(parsed.storagePolicy), Type::text) });
        }
        Query valid(resource);
        valid.select(valid.star("incoming")).from("incoming").join(ruvia::db_join_type::inner, service::configuration::persistence::DeviceEntity::table_name(), valid.binary(valid.binary(valid.column(service::configuration::persistence::DeviceEntity::column_name<"id">(), "current_device"), Op::equal, valid.column("device_id", "incoming")), Op::and_value, valid.binary(valid.column(service::configuration::persistence::DeviceEntity::column_name<"link_id">(), "current_device"), Op::equal, valid.column("link_id", "incoming"))), "current_device");
        Query requested(resource);
        requested.select(requested.column("device_id")).distinct().from("valid_incoming");
        Query states(resource);
        selectColumns(states, { "device_id", "last_stored_at", "last_observed_at", "last_observed_id", "last_data" }, "state");
        states.from(service::telemetry::persistence::DeviceDataIngestStateEntity::table_name(), "state").join(ruvia::db_join_type::inner, "requested", states.binary(states.column(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"device_id">(), "state"), Op::equal, states.column("device_id", "requested")));
        Query ordered(resource);
        ordered.select({ ordered.star("incoming"), ordered.alias(ordered.column("last_stored_at", "states"), "last_stored") })
            .from("valid_incoming", "incoming")
            .join(ruvia::db_join_type::inner, "states", ordered.binary(ordered.column("device_id", "incoming"), Op::equal, ordered.column("device_id", "states")));
        const auto unpackValue = [](Query& query, ruvia::db_expression value) {
            return query.case_when({ { query.binary(query.binary(query.call("jsonb_typeof", { value }), Op::equal, query.value("object")), Op::and_value, query.binary(value, Op::json_has_key, query.value("value"))), query.binary(value, Op::json_get, query.value("value")) } }, value);
        };
        Query incomingPoints(resource);
        incomingPoints.select({ incomingPoints.column("input_sequence", "ordered"), incomingPoints.column("device_id", "ordered"), incomingPoints.column("report_time", "ordered"), incomingPoints.alias(incomingPoints.column("id", "ordered"), "record_id"), incomingPoints.alias(incomingPoints.column("key", "point"), "element_id"), incomingPoints.alias(unpackValue(incomingPoints, incomingPoints.column("value", "point")), "point_value") })
            .from("ordered")
            .join_function(ruvia::db_join_type::cross, incomingPoints.call("jsonb_each", { incomingPoints.coalesce({ incomingPoints.binary(incomingPoints.column("data", "ordered"), Op::json_get, incomingPoints.value("values")), emptyJson(incomingPoints) }) }), {}, "point", { .lateral_ = true });
        Query timeline(resource);
        timeline.select({ timeline.column("input_sequence"), timeline.column("device_id"), timeline.column("element_id"), timeline.alias(timeline.column("report_time"), "observed_at"), timeline.column("record_id"), timeline.column("point_value"), timeline.alias(timeline.cast(timeline.value(false), Type::boolean), "baseline") }).from("incoming_points");
        Query baseline(resource);
        baseline.select({ baseline.cast(baseline.null_value(), Type::big_int), baseline.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"device_id">(), "latest"), baseline.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"element_id">(), "latest"), baseline.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"observed_at">(), "latest"), baseline.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"record_id">(), "latest"), unpackValue(baseline, baseline.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"value">(), "latest")), baseline.cast(baseline.value(true), Type::boolean) })
            .from(service::telemetry::latest::persistence::DeviceLatestValueEntity::table_name(), "latest")
            .join(ruvia::db_join_type::inner, "requested", baseline.binary(baseline.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"device_id">(), "latest"), Op::equal, baseline.column("device_id", "requested")));
        timeline.combine(ruvia::db_set_operation::union_all, baseline);
        Query pointLagged(resource);
        const ruvia::db_window_options pointOrder{ .partition_by_ = { pointLagged.column("device_id"), pointLagged.column("element_id") },
                                                   .order_by_ = { { pointLagged.column("observed_at") }, { pointLagged.column("record_id") }, { pointLagged.column("baseline"), ruvia::db_order_direction::desc }, { pointLagged.column("input_sequence"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::first } } };
        pointLagged.select({ pointLagged.star("timeline"), pointLagged.alias(pointLagged.over(pointLagged.call("lag", { pointLagged.column("point_value") }), pointOrder), "prior_value"), pointLagged.alias(pointLagged.over(pointLagged.call("lag", { pointLagged.cast(pointLagged.value(true), Type::boolean) }), pointOrder), "prior_present") }).from("point_timeline", "timeline");
        Query changes(resource);
        changes.select({ changes.column("input_sequence"), changes.alias(changes.aggregate("bool_or", { changes.binary(changes.unary(ruvia::db_unary_operator::is_null, changes.column("prior_present")), Op::or_value, changes.binary(changes.column("point_value"), Op::is_distinct_from, changes.column("prior_value"))) }), "changed") })
            .from("point_lagged")
            .and_where(changes.unary(ruvia::db_unary_operator::not_value, changes.column("baseline")))
            .group_by({ changes.column("input_sequence") });
        Query filtered(resource);
        const auto accepted = filtered.binary(filtered.binary(filtered.column("storage_policy"), Op::equal, filtered.value("report")), Op::or_value, filtered.binary(filtered.binary(filtered.column("storage_policy"), Op::equal, filtered.value("change")), Op::and_value, filtered.coalesce({ filtered.column("changed", "point_changes"), filtered.value(false) })));
        filtered.select({ filtered.star("ordered"), filtered.alias(accepted, "accepted") }).from("ordered").join(ruvia::db_join_type::left, "point_changes", filtered.binary(filtered.column("input_sequence", "ordered"), Op::equal, filtered.column("input_sequence", "point_changes")));
        Query records(resource);
        selectColumns(records, { "report_time", "id", "device_id", "link_id", "connection_id", "protocol", "source", "occurred_at", "data", "raw_payload_hex" });
        const auto model = records.binary(records.column("data"), Op::json_get, records.value("model"));
        records.add_select(records.cast(records.binary(model, Op::json_get_text, records.value("id")), Type::uuid)).from("filtered").and_where(records.column("accepted"));
        using Data = service::telemetry::persistence::DeviceDataEntity;
        Query inserted(resource);
        inserted.insert_into(service::telemetry::persistence::DeviceDataEntity::table_name(), { Data::column_name<"report_time">(), Data::column_name<"id">(), Data::column_name<"device_id">(), Data::column_name<"link_id">(), Data::column_name<"connection_id">(), Data::column_name<"protocol">(), Data::column_name<"source">(), Data::column_name<"occurred_at">(), Data::column_name<"data">(), Data::column_name<"raw_payload_hex">(), Data::column_name<"model_id">() })
            .insert_from(records)
            .on_conflict({ .columns_ = { std::string(Data::column_name<"id">()), std::string(Data::column_name<"report_time">()) }, .do_nothing_ = true })
            .returning({ inserted.column(Data::column_name<"device_id">()) });
        Query storage(resource);
        const auto lastStored = storage.aggregate("max", { storage.column("last_stored") });
        const auto lastAccepted = storage.filter(storage.aggregate("max", { storage.column("report_time") }), storage.column("accepted"));
        storage.select({ storage.column("device_id"), storage.alias(storage.coalesce({ storage.greatest({ lastStored, lastAccepted }), lastStored, lastAccepted }), "last_stored_at") })
            .from("filtered")
            .group_by({ storage.column("device_id") });
        Query newObserved(resource);
        const ruvia::db_window_options newestOrder{ .partition_by_ = { newObserved.column("device_id", "incoming") },
                                                    .order_by_ = { { newObserved.column("report_time", "incoming"), ruvia::db_order_direction::desc }, { newObserved.column("id", "incoming"), ruvia::db_order_direction::desc } } };
        newObserved.select({ newObserved.star("incoming"), newObserved.alias(newObserved.over(newObserved.call("row_number"), newestOrder), "newest") })
            .from("valid_incoming", "incoming")
            .join(ruvia::db_join_type::inner, "states", newObserved.binary(newObserved.column("device_id", "incoming"), Op::equal, newObserved.column("device_id", "states")))
            .and_where(newObserved.binary(newObserved.unary(ruvia::db_unary_operator::is_null, newObserved.column("last_observed_at", "states")), Op::or_value, newObserved.binary(newObserved.tuple({ newObserved.column("report_time", "incoming"), newObserved.column("id", "incoming") }), Op::greater, newObserved.tuple({ newObserved.column("last_observed_at", "states"), newObserved.column("last_observed_id", "states") }))));
        Query observed(resource);
        observed.select({ observed.column("device_id", "newest"), observed.alias(observed.column("report_time", "newest"), "last_observed_at"), observed.alias(observed.column("id", "newest"), "last_observed_id"), observed.alias(observed.column("data", "newest"), "last_data"), observed.alias(observed.coalesce({ observed.column("data", "previous"), observed.column("last_data", "states") }), "previous_data") })
            .from("new_observed", "newest")
            .join(ruvia::db_join_type::inner, "states", observed.binary(observed.column("device_id", "newest"), Op::equal, observed.column("device_id", "states")))
            .join(ruvia::db_join_type::left, "new_observed", observed.binary(observed.binary(observed.column("device_id", "previous"), Op::equal, observed.column("device_id", "newest")), Op::and_value, observed.binary(observed.column("newest", "previous"), Op::equal, observed.value(2))), "previous")
            .and_where(observed.binary(observed.column("newest", "newest"), Op::equal, observed.value(1)));
        Query latestElements(resource);
        latestElements.select({ latestElements.column("device_id", "incoming"), latestElements.alias(latestElements.column("key", "point"), "element_id"), latestElements.column("value", "point"), latestElements.alias(latestElements.column("report_time", "incoming"), "observed_at"), latestElements.alias(latestElements.column("id", "incoming"), "record_id") })
            .distinct_on({ latestElements.column("device_id", "incoming"), latestElements.column("key", "point") })
            .from("valid_incoming", "incoming")
            .join(ruvia::db_join_type::inner, "states", latestElements.binary(latestElements.column("device_id", "incoming"), Op::equal, latestElements.column("device_id", "states")))
            .join_function(ruvia::db_join_type::cross, latestElements.call("jsonb_each", { latestElements.coalesce({ latestElements.binary(latestElements.column("data", "incoming"), Op::json_get, latestElements.value("values")), emptyJson(latestElements) }) }), {}, "point", { .lateral_ = true })
            .add_order_by(latestElements.column("device_id", "incoming"))
            .add_order_by(latestElements.column("key", "point"))
            .add_order_by(latestElements.column("report_time", "incoming"), ruvia::db_order_direction::desc)
            .add_order_by(latestElements.column("id", "incoming"), ruvia::db_order_direction::desc);
        Query latestRows(resource);
        selectColumns(latestRows, { "device_id", "element_id", "value", "observed_at", "record_id" });
        latestRows.add_select(latestRows.call("now")).from("latest_elements");
        using Latest = service::telemetry::latest::persistence::DeviceLatestValueEntity;
        Query latestValues(resource);
        latestValues.insert_into(Latest::table_name(), { Latest::column_name<"device_id">(), Latest::column_name<"element_id">(), Latest::column_name<"value">(), Latest::column_name<"observed_at">(), Latest::column_name<"record_id">(), Latest::column_name<"updated_at">() }).insert_from(latestRows).on_conflict({ .columns_ = { std::string(Latest::column_name<"device_id">()), std::string(Latest::column_name<"element_id">()) }, .update_ = { { std::string(Latest::column_name<"value">()), latestValues.excluded(Latest::column_name<"value">()) }, { std::string(Latest::column_name<"observed_at">()), latestValues.excluded(Latest::column_name<"observed_at">()) }, { std::string(Latest::column_name<"record_id">()), latestValues.excluded(Latest::column_name<"record_id">()) }, { std::string(Latest::column_name<"updated_at">()), latestValues.call("now") } }, .update_where_ = latestValues.binary(latestValues.tuple({ latestValues.excluded(Latest::column_name<"observed_at">()), latestValues.excluded(Latest::column_name<"record_id">()) }), Op::greater, latestValues.tuple({ Latest::column<"observed_at">().expression(latestValues), Latest::column<"record_id">().expression(latestValues) })) }).returning({ latestValues.column(Latest::column_name<"device_id">()) });
        const auto barrier = [&](std::string_view table, std::string_view countName) {
            Query query(resource);
            query.select(query.alias(query.aggregate("count", { query.star() }), countName)).from(table);
            return query;
        };
        auto insertedBarrier = barrier("inserted", "inserted_count");
        Query stateUpdated(resource);
        stateUpdated.update(service::telemetry::persistence::DeviceDataIngestStateEntity::table_name(), "state").set(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"last_stored_at">(), stateUpdated.column("last_stored_at", "storage")).set(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"last_observed_at">(), stateUpdated.coalesce({ stateUpdated.column("last_observed_at", "observed"), stateUpdated.column(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"last_observed_at">(), "state") })).set(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"last_observed_id">(), stateUpdated.coalesce({ stateUpdated.column("last_observed_id", "observed"), stateUpdated.column(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"last_observed_id">(), "state") })).set(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"last_data">(), stateUpdated.coalesce({ stateUpdated.column("last_data", "observed"), stateUpdated.column(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"last_data">(), "state") })).set(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"previous_data">(), stateUpdated.case_when({ { stateUpdated.unary(ruvia::db_unary_operator::is_null, stateUpdated.column("device_id", "observed")), stateUpdated.column(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"previous_data">(), "state") } }, stateUpdated.column("previous_data", "observed"))).set(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"updated_at">(), stateUpdated.call("now")).update_from("storage_summary", "storage").join(ruvia::db_join_type::left, "observed_summary", stateUpdated.binary(stateUpdated.column("device_id", "storage"), Op::equal, stateUpdated.column("device_id", "observed")), "observed").join(ruvia::db_join_type::cross, insertedBarrier, {}, "inserted_barrier").and_where(stateUpdated.binary(stateUpdated.column(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"device_id">(), "state"), Op::equal, stateUpdated.column("device_id", "storage"))).and_where(stateUpdated.binary(stateUpdated.column("inserted_count", "inserted_barrier"), Op::greater_equal, stateUpdated.value(0))).returning({ stateUpdated.column(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"device_id">(), "state") });
        auto updateBarrier = barrier("state_updated", "updated_count");
        auto latestBarrier = barrier("latest_values", "latest_count");
        Query persisted(resource);
        const ruvia::db_cte_options materialized{ .materialization_ = ruvia::db_materialization::materialized };
        persisted.with("incoming", incoming, { .columns_ = { "input_sequence", "report_time", "id", "device_id", "link_id", "connection_id", "protocol", "source", "occurred_at", "data", "raw_payload_hex", "storage_policy" } })
            .with("valid_incoming", valid, materialized)
            .with("requested", requested, materialized)
            .with("states", states, materialized)
            .with("ordered", ordered)
            .with("incoming_points", incomingPoints, materialized)
            .with("point_timeline", timeline, materialized)
            .with("point_lagged", pointLagged, materialized)
            .with("point_changes", changes, materialized)
            .with("filtered", filtered, materialized)
            .with("inserted", inserted)
            .with("storage_summary", storage, materialized)
            .with("new_observed", newObserved, materialized)
            .with("observed_summary", observed, materialized)
            .with("latest_elements", latestElements, materialized)
            .with("latest_values", latestValues)
            .with("state_updated", stateUpdated)
            .select({ persisted.column("updated_count", "update_barrier"), persisted.column("latest_count", "latest_barrier") })
            .from(updateBarrier, "update_barrier")
            .join(ruvia::db_join_type::cross, latestBarrier, {}, "latest_barrier");
        return persisted;
    }

    // 写入一批历史：按设备加锁并补齐写入状态后，在一条语句内写 device_data、device_latest_value 与 device_data_ingest_state。
    static ruvia::task<void> persist(ruvia::web_worker_context& context, const std::vector<message::ParsedDeviceMessage>& messages) {
        if (messages.empty()) {
            co_return;
        }
        using Query = ruvia::db_query;
        using Type = ruvia::db_data_type;
        auto persisted = persistenceQuery(context.pool(), messages);
        // Acquire per-device locks before taking the statement snapshot. Seeding
        // in a data-modifying CTE is invisible to sibling SELECTs and drops a
        // newly created device's first sample.
        auto transaction = co_await context.db("telemetry-history").begin_transaction();
        std::set<std::string_view> deviceIds;
        for (const auto& value : messages) {
            deviceIds.insert(value.deviceId);
        }
        Query requestedDevices(context.pool());
        for (const auto deviceId : deviceIds) {
            requestedDevices.values({ requestedDevices.cast(requestedDevices.value(deviceId), Type::uuid) });
        }
        Query locks(context.pool());
        locks.with("requested", requestedDevices, { .columns_ = { "device_id" } })
            .select(locks.call("pg_advisory_xact_lock", { locks.call("hashtextextended", { locks.cast(locks.column("device_id"), Type::text), locks.cast(locks.value(service::message::lock::kTelemetryDeviceSeed), Type::big_int) }) }))
            .from("requested")
            .add_order_by(locks.column("device_id"));
        (void)co_await transaction.query(locks);
        Query deviceRows(context.pool());
        deviceRows.select(deviceRows.column("device_id")).from("requested");
        Query seed(context.pool());
        seed.with("requested", requestedDevices, { .columns_ = { "device_id" } })
            .insert_into(service::telemetry::persistence::DeviceDataIngestStateEntity::table_name(), { service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"device_id">() })
            .insert_from(deviceRows)
            .on_conflict({ .columns_ = { std::string(service::telemetry::persistence::DeviceDataIngestStateEntity::column_name<"device_id">()) }, .do_nothing_ = true });
        (void)co_await transaction.execute(seed);
        (void)co_await transaction.query(persisted);
        co_await transaction.commit();
    }
};

} // namespace service::telemetry
