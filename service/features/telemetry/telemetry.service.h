#pragma once

#include <tuple>

#include <ruvia/core/stop_token.h>

#include "service/utils/number.h"
#include "service/features/telemetry/telemetry.entity.h"
#include <ruvia/web/db/db_query.h>
#include <utility>
#include <spdlog/spdlog.h>
#include "service/features/messaging/messaging.transport.h"
#include "service/common/message.h"
#include "service/features/telemetry/latest/latest.service.h"
#include "service/features/telemetry/telemetry.protocol.h"

namespace service::telemetry {
enum class Consumer { Dispatch,
                      History,
                      Latest,
                      Alerts,
                      Delivery };
inline constexpr std::array<std::string_view, 5> consumerNames{ "dispatch", "history", "latest", "alerts", "delivery" };

inline std::string consumerStream(Consumer consumer) {
    auto stream = service::message::parsedStream();
    if (consumer != Consumer::Dispatch) {
        stream += ':' + std::string(consumerNames[static_cast<std::size_t>(consumer)]);
    }
    return stream;
}

// The ingress receipt and all four durable copies are committed atomically.
// Consumer streams never trim pending entries. Each consumer owns XACK/XDEL.
inline constexpr std::string_view kFanoutScript = R"lua(
if redis.call('EXISTS',KEYS[1]) ~= 0 then
 for i=1,#ARGV,2 do
  if ARGV[i] == 'raw_packet_ids' and ARGV[i+1] ~= '[]' then
   redis.call('XADD',KEYS[2],'*',unpack(ARGV))
   break
  end
 end
 return 0
end
for i=2,5 do
 local kind = redis.call('TYPE',KEYS[i]).ok
 if kind ~= 'none' and kind ~= 'stream' then
  return redis.error_reply('telemetry consumer key must be a stream')
 end
end
local args = {'*'}
for i=1,#ARGV do args[#args+1]=ARGV[i] end
for i=2,5 do redis.call('XADD',KEYS[i],unpack(args)) end
redis.call('SET',KEYS[1],'1','EX',604800)
return 1
)lua";

template <class Redis>
ruvia::task<void> fanout(const Redis& redis, const std::vector<service::message::StreamMessage>& messages) {
    for (const auto& input : messages) {
        auto parsed = service::message::parsedFrom(input);
        contract::normalize(parsed);
        const auto fields = service::message::parsedFields(parsed);
        const std::vector<std::string> store{
            "iot:telemetry:fanout:" + parsed.messageId,
            consumerStream(Consumer::History),
            consumerStream(Consumer::Latest),
            consumerStream(Consumer::Alerts),
            consumerStream(Consumer::Delivery)
        };
        const std::vector<std::string_view> keys(store.begin(), store.end());
        std::vector<std::string_view> args;
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const auto reply = co_await redis.eval(kFanoutScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("telemetry fanout", reply);
        }
    }
}
} // namespace service::telemetry

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>
#include <ruvia/web/db/db.h>

#include "service/features/access/access.service.h"
#include "service/features/alert/alert.service.h"

namespace service::telemetry {

namespace detail {

inline std::string sanitizeJsonUtf8(std::string_view value) {
    static constexpr std::array<char, 16> digits{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    std::string result;
    result.reserve(value.size());
    const auto continuation = [value](std::size_t index) {
        return index < value.size() && (static_cast<unsigned char>(value[index]) & 0xC0U) == 0x80U;
    };
    for (std::size_t index = 0; index < value.size();) {
        const auto byte = static_cast<unsigned char>(value[index]);
        if (byte < 0x80) {
            result.push_back(static_cast<char>(byte));
            ++index;
            continue;
        }
        std::size_t length = 0;
        if (byte >= 0xC2 && byte <= 0xDF && continuation(index + 1)) {
            length = 2;
        } else if (byte == 0xE0 && index + 2 < value.size() &&
                   static_cast<unsigned char>(value[index + 1]) >= 0xA0 &&
                   static_cast<unsigned char>(value[index + 1]) <= 0xBF && continuation(index + 2)) {
            length = 3;
        } else if (((byte >= 0xE1 && byte <= 0xEC) || (byte >= 0xEE && byte <= 0xEF)) &&
                   continuation(index + 1) && continuation(index + 2)) {
            length = 3;
        } else if (byte == 0xED && index + 2 < value.size() &&
                   static_cast<unsigned char>(value[index + 1]) >= 0x80 &&
                   static_cast<unsigned char>(value[index + 1]) <= 0x9F && continuation(index + 2)) {
            length = 3;
        } else if (byte == 0xF0 && index + 3 < value.size() &&
                   static_cast<unsigned char>(value[index + 1]) >= 0x90 &&
                   static_cast<unsigned char>(value[index + 1]) <= 0xBF && continuation(index + 2) &&
                   continuation(index + 3)) {
            length = 4;
        } else if (byte >= 0xF1 && byte <= 0xF3 && continuation(index + 1) &&
                   continuation(index + 2) && continuation(index + 3)) {
            length = 4;
        } else if (byte == 0xF4 && index + 3 < value.size() &&
                   static_cast<unsigned char>(value[index + 1]) >= 0x80 &&
                   static_cast<unsigned char>(value[index + 1]) <= 0x8F && continuation(index + 2) &&
                   continuation(index + 3)) {
            length = 4;
        }
        if (length != 0) {
            result.append(value.substr(index, length));
            index += length;
            continue;
        }
        result += "\\u00";
        result.push_back(digits[byte >> 4U]);
        result.push_back(digits[byte & 0x0FU]);
        ++index;
    }
    return result;
}

} // namespace detail

class TelemetryService {
  public:
    static ruvia::task<std::string> executeProjection(ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        if (stop.stop_requested()) {
            service::common::fail(10004, "Telemetry operation cancelled", 503);
        }
        const auto separator = payload.find('\n');
        const auto id = payload.substr(0, separator);
        if (!service::common::isUuid(id)) {
            service::common::fail(10002, "Invalid telemetry object identifier", 400);
        }
        if (operation == "initialize" && separator != std::string_view::npos) {
            co_await latest::initializeDevice(context.redis(), id, payload.substr(separator + 1));
            co_await latest::projectDevice(context, id);
        } else if (operation == "project-device") {
            co_await latest::projectDevice(context, id);
        } else if (operation == "project-protocol") {
            co_await latest::projectProtocol(context, id);
        } else if (operation == "erase-device") {
            co_await latest::eraseDevice(context.redis(), id);
        } else {
            service::common::fail(10002, "Unknown telemetry operation", 400);
        }
        co_return "{}";
    }

    static ruvia::task<void> ingest(ruvia::web_worker_context& context, const std::vector<message::StreamMessage>& messages) {
        co_await fanout(context.redis(), messages);
    }

    static ruvia::task<void> apply(ruvia::web_worker_context& context, Consumer consumer, const std::vector<message::StreamMessage>& messages) {
        if (consumer == Consumer::Dispatch) {
            co_await ingest(context, messages);
            co_return;
        }
        if (messages.empty()) {
            co_return;
        }
        std::vector<message::ParsedDeviceMessage> parsedMessages;
        parsedMessages.reserve(messages.size());
        for (const auto& message : messages) {
            auto parsed = message::parsedFrom(message);
            parsed.valuesJson = detail::sanitizeJsonUtf8(parsed.valuesJson);
            parsedMessages.push_back(std::move(parsed));
        }
        const auto redis = context.redis();
        if (consumer == Consumer::History || consumer == Consumer::Delivery) {
            std::erase_if(parsedMessages, contract::isSl651EmptyReport);
            if (parsedMessages.empty()) co_return;
        }
        switch (consumer) {
            case Consumer::History: {
                (void)co_await persist(context, parsedMessages, std::vector<bool>(parsedMessages.size(), false));
                co_await latest::publishRealtimeChange(redis);
                break;
            }
            case Consumer::Latest:
                co_await latest::update(redis, parsedMessages);
                break;
            case Consumer::Delivery:
                co_await service::access::event::publishMany(redis, parsedMessages);
                break;
            case Consumer::Alerts: {
                const auto metadata = co_await service::alert::metadata::activity(context, parsedMessages);
                if (metadata.offlineRules) {
                    co_await service::alert::metadata::schedule(redis, parsedMessages);
                }
                auto active = metadata.devices;
                std::vector<std::string> previous;
                for (std::size_t index = 0; index < parsedMessages.size(); ++index) {
                    if (contract::isSl651EmptyReport(parsedMessages[index])) {
                        active[index] = false;
                        previous.emplace_back("{}");
                    } else {
                        previous.push_back(co_await prepareAlert(context, parsedMessages[index]));
                    }
                }
                co_await service::alert::AlertEvaluationService::evaluateTelemetry(context, parsedMessages, previous, active);
                break;
            }
            case Consumer::Dispatch:
                break;
        }
    }

  protected:
    static ruvia::task<std::string> prepareAlert(ruvia::web_worker_context& context, const message::ParsedDeviceMessage& value) {
        ruvia::db_query state(context.pool());
        const auto newer = state.binary(state.tuple({ state.excluded(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"observed_at_ms">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view()), state.excluded(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"message_id">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view()) }), ruvia::db_binary_operator::greater,
            state.tuple({ state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"observed_at_ms">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view(), "alert_input_state"), state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"message_id">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view(), "alert_input_state") }));
        state.insert_into(service::telemetry::persistence::AlertInputStateEntity::table_name(), { "device_id", "observed_at_ms", "message_id", "data", "previous_data" })
            .values({ state.cast(state.value(value.deviceId), ruvia::db_data_type::uuid), state.cast(state.value(value.observedAtMs), ruvia::db_data_type::big_int),
                state.cast(state.value(value.messageId), ruvia::db_data_type::uuid), state.cast(state.value(value.valuesJson), ruvia::db_data_type::jsonb),
                state.cast(state.value("{}"), ruvia::db_data_type::jsonb) })
            .on_conflict({ .columns_ = { "device_id" }, .update_ = {
                { "previous_data", state.case_when({ { newer, state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"data">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view(), "alert_input_state") } }, state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"previous_data">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view(), "alert_input_state")) },
                { "data", state.case_when({ { newer, state.excluded(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"data">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view()) } }, state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"data">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view(), "alert_input_state")) },
                { "observed_at_ms", state.greatest({ state.excluded(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"observed_at_ms">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view()), state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"observed_at_ms">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view(), "alert_input_state") }) },
                { "message_id", state.case_when({ { newer, state.excluded(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"message_id">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view()) } }, state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"message_id">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view(), "alert_input_state")) }
            } })
            .returning({ state.cast(state.case_when({ { state.binary(state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"message_id">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                state.cast(state.value(value.messageId), ruvia::db_data_type::uuid)), state.column(std::tuple_element_t<service::telemetry::persistence::AlertInputStateEntity::column_index<"previous_data">(), service::telemetry::persistence::AlertInputStateEntity::columns_type>::name.view()) } },
                state.cast(state.value("{}"), ruvia::db_data_type::jsonb)), ruvia::db_data_type::text) });
        const auto rows = co_await context.db().query(state);
        co_return std::string(rows.front()[0].value().value_or("{}"));
    }

    static ruvia::db_query persistenceQuery(std::pmr::memory_resource* resource,
        const std::vector<message::ParsedDeviceMessage>& messages, const std::vector<bool>& alertActive) {
        std::vector<std::string> rawPayloadArrays;
        rawPayloadArrays.reserve(messages.size());
        for (const auto& message : messages) {
            rawPayloadArrays.push_back(message::rawPayloadsJson(message.rawPayloads));
        }

        using Query = ruvia::db_query;
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        const auto selectColumns = [](Query& query, std::initializer_list<std::string_view> columns, std::string_view table = {}) {
            for (const auto column : columns) query.add_select(query.column(column, table));
        };
        const auto emptyJson = [](Query& query) { return query.cast(query.value("{}"), Type::jsonb); };
        const auto timestamp = [](Query& query, std::int64_t milliseconds) {
            return query.call("to_timestamp", { query.binary(query.cast(query.value(milliseconds), Type::double_value), Op::divide, query.value(1000.0)) });
        };
        Query incoming(resource);
        for (std::size_t index = 0; index < messages.size(); ++index) {
            const auto& parsed = messages[index];
            incoming.values({ incoming.cast(incoming.value(static_cast<std::int64_t>(index)), Type::big_int), timestamp(incoming, parsed.observedAtMs),
                incoming.cast(incoming.value(parsed.messageId), Type::uuid), incoming.cast(incoming.value(parsed.deviceId), Type::uuid),
                incoming.cast(incoming.value(parsed.linkId), Type::uuid), incoming.cast(incoming.value(parsed.connectionId), Type::uuid),
                incoming.cast(incoming.value(parsed.protocol), Type::text), incoming.cast(incoming.value(parsed.source), Type::text), timestamp(incoming, parsed.occurredAtMs),
                incoming.cast(incoming.value(parsed.valuesJson), Type::jsonb), incoming.cast(incoming.value(rawPayloadArrays[index]), Type::jsonb),
                incoming.cast(incoming.value(parsed.storagePolicy), Type::text), incoming.cast(incoming.value(static_cast<bool>(alertActive[index])), Type::boolean) });
        }
        Query valid(resource);
        valid.select(valid.star("incoming")).from("incoming")
            .join(ruvia::db_join_type::inner, service::telemetry::persistence::DeviceEntity::table_name(), valid.binary(
                valid.binary(valid.column(std::tuple_element_t<service::telemetry::persistence::DeviceEntity::column_index<"id">(), service::telemetry::persistence::DeviceEntity::columns_type>::name.view(), "current_device"), Op::equal, valid.column("device_id", "incoming")), Op::and_value,
                valid.binary(valid.column(std::tuple_element_t<service::telemetry::persistence::DeviceEntity::column_index<"link_id">(), service::telemetry::persistence::DeviceEntity::columns_type>::name.view(), "current_device"), Op::equal, valid.column("link_id", "incoming"))), "current_device");
        Query requested(resource);
        requested.select(requested.column("device_id")).distinct().from("valid_incoming");
        Query states(resource);
        selectColumns(states, { "device_id", "last_stored_at", "last_observed_at", "last_observed_id", "last_data" }, "state");
        states.from(service::telemetry::persistence::DeviceDataIngestStateEntity::table_name(), "state").join(ruvia::db_join_type::inner, "requested",
            states.binary(states.column(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"device_id">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), "state"), Op::equal, states.column("device_id", "requested")));
        Query ordered(resource);
        const ruvia::db_window_options reportOrder{ .partition_by_ = { ordered.column("device_id", "incoming") },
            .order_by_ = { { ordered.column("report_time", "incoming") }, { ordered.column("id", "incoming") } } };
        ordered.select({ ordered.star("incoming"), ordered.alias(ordered.over(ordered.call("row_number"), reportOrder), "sequence"),
                ordered.alias(ordered.column("last_stored_at", "states"), "last_stored"), ordered.alias(ordered.column("last_observed_at", "states"), "baseline_observed_at"),
                ordered.alias(ordered.column("last_observed_id", "states"), "baseline_observed_id"), ordered.alias(ordered.column("last_data", "states"), "baseline_data") })
            .from("valid_incoming", "incoming").join(ruvia::db_join_type::inner, "states",
                ordered.binary(ordered.column("device_id", "incoming"), Op::equal, ordered.column("device_id", "states")));
        Query lagged(resource);
        const ruvia::db_window_options lagOrder{ .partition_by_ = { lagged.column("device_id") }, .order_by_ = { { lagged.column("report_time") }, { lagged.column("id") } } };
        lagged.select(lagged.star("ordered")).from("ordered");
        for (const auto& [column, alias] : { std::pair{ "report_time", "prior_report_time" }, std::pair{ "id", "prior_id" }, std::pair{ "data", "prior_data" } })
            lagged.add_select(lagged.alias(lagged.over(lagged.call("lag", { lagged.column(column) }), lagOrder), alias));
        Query predecessors(resource);
        const auto baselinePresent = predecessors.unary(ruvia::db_unary_operator::is_not_null, predecessors.column("baseline_observed_at"));
        const auto baselineOrder = predecessors.tuple({ predecessors.column("baseline_observed_at"), predecessors.column("baseline_observed_id") });
        const auto stale = predecessors.binary(baselinePresent, Op::and_value, predecessors.binary(
            predecessors.tuple({ predecessors.column("report_time"), predecessors.column("id") }), Op::less_equal, baselineOrder));
        const auto prior = predecessors.binary(predecessors.unary(ruvia::db_unary_operator::is_not_null, predecessors.column("prior_report_time")), Op::and_value,
            predecessors.binary(predecessors.unary(ruvia::db_unary_operator::is_null, predecessors.column("baseline_observed_at")), Op::or_value,
                predecessors.binary(predecessors.tuple({ predecessors.column("prior_report_time"), predecessors.column("prior_id") }), Op::greater, baselineOrder)));
        predecessors.select({ predecessors.column("input_sequence"), predecessors.alias(predecessors.case_when({ { stale, emptyJson(predecessors) },
            { prior, predecessors.column("prior_data") } }, predecessors.coalesce({ predecessors.column("baseline_data"), emptyJson(predecessors) })), "previous_data") }).from("lagged");
        const auto unpackValue = [](Query& query, ruvia::db_expression value) {
            return query.case_when({ { query.binary(query.binary(query.call("jsonb_typeof", { value }), Op::equal, query.value("object")), Op::and_value,
                query.binary(value, Op::json_has_key, query.value("value"))), query.binary(value, Op::json_get, query.value("value")) } }, value);
        };
        Query incomingPoints(resource);
        incomingPoints.select({ incomingPoints.column("input_sequence", "ordered"), incomingPoints.column("device_id", "ordered"), incomingPoints.column("report_time", "ordered"),
                incomingPoints.alias(incomingPoints.column("id", "ordered"), "record_id"), incomingPoints.alias(incomingPoints.column("key", "point"), "element_id"),
                incomingPoints.alias(unpackValue(incomingPoints, incomingPoints.column("value", "point")), "point_value") })
            .from("ordered").join_function(ruvia::db_join_type::cross, incomingPoints.call("jsonb_each", { incomingPoints.coalesce({
                incomingPoints.binary(incomingPoints.column("data", "ordered"), Op::json_get, incomingPoints.value("values")), emptyJson(incomingPoints) }) }), {}, "point", { .lateral_ = true });
        Query timeline(resource);
        timeline.select({ timeline.column("input_sequence"), timeline.column("device_id"), timeline.column("element_id"), timeline.alias(timeline.column("report_time"), "observed_at"),
                timeline.column("record_id"), timeline.column("point_value"), timeline.alias(timeline.cast(timeline.value(false), Type::boolean), "baseline") }).from("incoming_points");
        Query baseline(resource);
        baseline.select({ baseline.cast(baseline.null_value(), Type::big_int), baseline.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"device_id">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view(), "latest"), baseline.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"element_id">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view(), "latest"),
                baseline.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"observed_at">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view(), "latest"), baseline.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"record_id">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view(), "latest"), unpackValue(baseline, baseline.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"value">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view(), "latest")), baseline.cast(baseline.value(true), Type::boolean) })
            .from(service::telemetry::persistence::DeviceLatestValueEntity::table_name(), "latest").join(ruvia::db_join_type::inner, "requested", baseline.binary(baseline.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"device_id">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view(), "latest"), Op::equal, baseline.column("device_id", "requested")));
        timeline.combine(ruvia::db_set_operation::union_all, baseline);
        Query pointLagged(resource);
        const ruvia::db_window_options pointOrder{ .partition_by_ = { pointLagged.column("device_id"), pointLagged.column("element_id") },
            .order_by_ = { { pointLagged.column("observed_at") }, { pointLagged.column("record_id") }, { pointLagged.column("baseline"), ruvia::db_order_direction::desc },
                { pointLagged.column("input_sequence"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::first } } };
        pointLagged.select({ pointLagged.star("timeline"), pointLagged.alias(pointLagged.over(pointLagged.call("lag", { pointLagged.column("point_value") }), pointOrder), "prior_value"),
            pointLagged.alias(pointLagged.over(pointLagged.call("lag", { pointLagged.cast(pointLagged.value(true), Type::boolean) }), pointOrder), "prior_present") }).from("point_timeline", "timeline");
        Query changes(resource);
        changes.select({ changes.column("input_sequence"), changes.alias(changes.aggregate("bool_or", { changes.binary(
                changes.unary(ruvia::db_unary_operator::is_null, changes.column("prior_present")), Op::or_value,
                changes.binary(changes.column("point_value"), Op::is_distinct_from, changes.column("prior_value"))) }), "changed") })
            .from("point_lagged").and_where(changes.unary(ruvia::db_unary_operator::not_value, changes.column("baseline"))).group_by({ changes.column("input_sequence") });
        Query filtered(resource);
        const auto accepted = filtered.binary(filtered.binary(filtered.column("storage_policy"), Op::equal, filtered.value("report")), Op::or_value,
            filtered.binary(filtered.binary(filtered.column("storage_policy"), Op::equal, filtered.value("change")), Op::and_value,
                filtered.coalesce({ filtered.column("changed", "point_changes"), filtered.value(false) })));
        filtered.select({ filtered.star("ordered"), filtered.alias(accepted, "accepted") }).from("ordered")
            .join(ruvia::db_join_type::left, "point_changes", filtered.binary(filtered.column("input_sequence", "ordered"), Op::equal, filtered.column("input_sequence", "point_changes")));
        Query records(resource);
        selectColumns(records, { "report_time", "id", "device_id", "link_id", "connection_id", "protocol", "source", "occurred_at", "data", "raw_payload_hex" });
        const auto model = records.binary(records.column("data"), Op::json_get, records.value("model"));
        records.add_select(records.cast(records.binary(model, Op::json_get_text, records.value("id")), Type::uuid)).from("filtered").and_where(records.column("accepted"));
        Query inserted(resource);
        inserted.insert_into(service::telemetry::persistence::DeviceDataEntity::table_name(), { "report_time", "id", "device_id", "link_id", "connection_id", "protocol", "source", "occurred_at", "data", "raw_payload_hex", "model_id" })
            .insert_from(records).on_conflict({ .columns_ = { "id", "report_time" }, .do_nothing_ = true }).returning({ inserted.column(std::tuple_element_t<service::telemetry::persistence::DeviceDataEntity::column_index<"device_id">(), service::telemetry::persistence::DeviceDataEntity::columns_type>::name.view()) });
        Query storage(resource);
        const auto lastStored = storage.aggregate("max", { storage.column("last_stored") });
        const auto lastAccepted = storage.filter(storage.aggregate("max", { storage.column("report_time") }), storage.column("accepted"));
        storage.select({ storage.column("device_id"), storage.alias(storage.coalesce({ storage.greatest({ lastStored, lastAccepted }), lastStored, lastAccepted }), "last_stored_at") })
            .from("filtered").group_by({ storage.column("device_id") });
        Query newObserved(resource);
        const ruvia::db_window_options newestOrder{ .partition_by_ = { newObserved.column("device_id", "incoming") },
            .order_by_ = { { newObserved.column("report_time", "incoming"), ruvia::db_order_direction::desc }, { newObserved.column("id", "incoming"), ruvia::db_order_direction::desc } } };
        newObserved.select({ newObserved.star("incoming"), newObserved.alias(newObserved.over(newObserved.call("row_number"), newestOrder), "newest") })
            .from("valid_incoming", "incoming").join(ruvia::db_join_type::inner, "states", newObserved.binary(newObserved.column("device_id", "incoming"), Op::equal, newObserved.column("device_id", "states")))
            .and_where(newObserved.binary(newObserved.unary(ruvia::db_unary_operator::is_null, newObserved.column("last_observed_at", "states")), Op::or_value,
                newObserved.binary(newObserved.tuple({ newObserved.column("report_time", "incoming"), newObserved.column("id", "incoming") }), Op::greater,
                    newObserved.tuple({ newObserved.column("last_observed_at", "states"), newObserved.column("last_observed_id", "states" ) }))));
        Query observed(resource);
        observed.select({ observed.column("device_id", "newest"), observed.alias(observed.column("report_time", "newest"), "last_observed_at"),
                observed.alias(observed.column("id", "newest"), "last_observed_id"), observed.alias(observed.column("data", "newest"), "last_data"),
                observed.alias(observed.coalesce({ observed.column("data", "previous"), observed.column("last_data", "states") }), "previous_data") })
            .from("new_observed", "newest").join(ruvia::db_join_type::inner, "states", observed.binary(observed.column("device_id", "newest"), Op::equal, observed.column("device_id", "states")))
            .join(ruvia::db_join_type::left, "new_observed", observed.binary(observed.binary(observed.column("device_id", "previous"), Op::equal, observed.column("device_id", "newest")), Op::and_value,
                observed.binary(observed.column("newest", "previous"), Op::equal, observed.value(2))), "previous")
            .and_where(observed.binary(observed.column("newest", "newest"), Op::equal, observed.value(1)));
        Query latestElements(resource);
        latestElements.select({ latestElements.column("device_id", "incoming"), latestElements.alias(latestElements.column("key", "point"), "element_id"), latestElements.column("value", "point"),
                latestElements.alias(latestElements.column("report_time", "incoming"), "observed_at"), latestElements.alias(latestElements.column("id", "incoming"), "record_id") })
            .distinct_on({ latestElements.column("device_id", "incoming"), latestElements.column("key", "point") }).from("valid_incoming", "incoming")
            .join(ruvia::db_join_type::inner, "states", latestElements.binary(latestElements.column("device_id", "incoming"), Op::equal, latestElements.column("device_id", "states")))
            .join_function(ruvia::db_join_type::cross, latestElements.call("jsonb_each", { latestElements.coalesce({ latestElements.binary(latestElements.column("data", "incoming"), Op::json_get, latestElements.value("values")), emptyJson(latestElements) }) }), {}, "point", { .lateral_ = true })
            .add_order_by(latestElements.column("device_id", "incoming")).add_order_by(latestElements.column("key", "point"))
            .add_order_by(latestElements.column("report_time", "incoming"), ruvia::db_order_direction::desc).add_order_by(latestElements.column("id", "incoming"), ruvia::db_order_direction::desc);
        Query latestRows(resource);
        selectColumns(latestRows, { "device_id", "element_id", "value", "observed_at", "record_id" });
        latestRows.add_select(latestRows.call("now")).from("latest_elements");
        Query latestValues(resource);
        latestValues.insert_into(service::telemetry::persistence::DeviceLatestValueEntity::table_name(), { "device_id", "element_id", "value", "observed_at", "record_id", "updated_at" }).insert_from(latestRows)
            .on_conflict({ .columns_ = { "device_id", "element_id" }, .update_ = { { "value", latestValues.excluded(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"value">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view()) }, { "observed_at", latestValues.excluded(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"observed_at">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view()) },
                { "record_id", latestValues.excluded(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"record_id">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view()) }, { "updated_at", latestValues.call("now") } },
                .update_where_ = latestValues.binary(latestValues.tuple({ latestValues.excluded(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"observed_at">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view()), latestValues.excluded(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"record_id">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view()) }), Op::greater,
                    latestValues.tuple({ latestValues.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"observed_at">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view(), "device_latest_value"), latestValues.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"record_id">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view(), "device_latest_value") })) })
            .returning({ latestValues.column(std::tuple_element_t<service::telemetry::persistence::DeviceLatestValueEntity::column_index<"device_id">(), service::telemetry::persistence::DeviceLatestValueEntity::columns_type>::name.view()) });
        const auto barrier = [&](std::string_view table, std::string_view countName) {
            Query query(resource);
            query.select(query.alias(query.aggregate("count", { query.star() }), countName)).from(table);
            return query;
        };
        auto insertedBarrier = barrier("inserted", "inserted_count");
        Query stateUpdated(resource);
        stateUpdated.update(service::telemetry::persistence::DeviceDataIngestStateEntity::table_name(), "state").set(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"last_stored_at">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), stateUpdated.column("last_stored_at", "storage"))
            .set(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"last_observed_at">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), stateUpdated.coalesce({ stateUpdated.column("last_observed_at", "observed"), stateUpdated.column(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"last_observed_at">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), "state") }))
            .set(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"last_observed_id">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), stateUpdated.coalesce({ stateUpdated.column("last_observed_id", "observed"), stateUpdated.column(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"last_observed_id">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), "state") }))
            .set(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"last_data">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), stateUpdated.coalesce({ stateUpdated.column("last_data", "observed"), stateUpdated.column(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"last_data">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), "state") }))
            .set(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"previous_data">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), stateUpdated.case_when({ { stateUpdated.unary(ruvia::db_unary_operator::is_null, stateUpdated.column("device_id", "observed")), stateUpdated.column(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"previous_data">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), "state") } }, stateUpdated.column("previous_data", "observed")))
            .set(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"updated_at">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), stateUpdated.call("now")).update_from("storage_summary", "storage")
            .join(ruvia::db_join_type::left, "observed_summary", stateUpdated.binary(stateUpdated.column("device_id", "storage"), Op::equal, stateUpdated.column("device_id", "observed")), "observed")
            .join(ruvia::db_join_type::cross, insertedBarrier, {}, "inserted_barrier")
            .and_where(stateUpdated.binary(stateUpdated.column(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"device_id">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), "state"), Op::equal, stateUpdated.column("device_id", "storage")))
            .and_where(stateUpdated.binary(stateUpdated.column("inserted_count", "inserted_barrier"), Op::greater_equal, stateUpdated.value(0))).returning({ stateUpdated.column(std::tuple_element_t<service::telemetry::persistence::DeviceDataIngestStateEntity::column_index<"device_id">(), service::telemetry::persistence::DeviceDataIngestStateEntity::columns_type>::name.view(), "state") });
        auto updateBarrier = barrier("state_updated", "updated_count");
        auto latestBarrier = barrier("latest_values", "latest_count");
        Query persisted(resource);
        const ruvia::db_cte_options materialized{ .materialization_ = ruvia::db_materialization::materialized };
        persisted.with("incoming", incoming, { .columns_ = { "input_sequence", "report_time", "id", "device_id", "link_id", "connection_id", "protocol", "source", "occurred_at", "data", "raw_payload_hex", "storage_policy", "needs_previous" } })
            .with("valid_incoming", valid, materialized).with("requested", requested, materialized).with("states", states, materialized).with("ordered", ordered)
            .with("lagged", lagged, materialized).with("predecessors", predecessors, materialized).with("incoming_points", incomingPoints, materialized)
            .with("point_timeline", timeline, materialized).with("point_lagged", pointLagged, materialized).with("point_changes", changes, materialized)
            .with("filtered", filtered, materialized).with("inserted", inserted).with("storage_summary", storage, materialized).with("new_observed", newObserved, materialized)
            .with("observed_summary", observed, materialized).with("latest_elements", latestElements, materialized).with("latest_values", latestValues).with("state_updated", stateUpdated)
            .select({ persisted.cast(persisted.column("input_sequence", "incoming"), Type::text),
                persisted.cast(persisted.case_when({ { persisted.column("needs_previous", "incoming"), persisted.coalesce({ persisted.column("previous_data", "predecessors"), emptyJson(persisted) }) } }, emptyJson(persisted)), Type::text) })
            .from("incoming").join(ruvia::db_join_type::left, "predecessors", persisted.binary(persisted.column("input_sequence", "incoming"), Op::equal, persisted.column("input_sequence", "predecessors")))
            .join(ruvia::db_join_type::cross, updateBarrier, {}, "update_barrier").join(ruvia::db_join_type::cross, latestBarrier, {}, "latest_barrier")
            .and_where(persisted.binary(persisted.column("updated_count", "update_barrier"), Op::greater_equal, persisted.value(0)))
            .and_where(persisted.binary(persisted.column("latest_count", "latest_barrier"), Op::greater_equal, persisted.value(0))).add_order_by(persisted.column("input_sequence", "incoming"));
        return persisted;
    }

    static ruvia::task<std::vector<std::string>>
    persist(ruvia::web_worker_context& context, const std::vector<message::ParsedDeviceMessage>& messages, const std::vector<bool>& alertActive) {
        if (messages.empty()) {
            co_return std::vector<std::string>{};
        }
        if (messages.size() != alertActive.size()) {
            throw std::invalid_argument("telemetry alert-active batch size mismatch");
        }
        using Query = ruvia::db_query;
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        auto persisted = persistenceQuery(context.pool(), messages, alertActive);
        // Acquire per-device locks before taking the statement snapshot. Seeding
        // in a data-modifying CTE is invisible to sibling SELECTs and drops a
        // newly created device's first sample.
        auto transaction = co_await context.db("telemetry-history").begin_transaction();
        std::set<std::string_view> deviceIds;
        for (const auto& value : messages) {
            deviceIds.insert(value.deviceId);
        }
        Query requestedDevices(context.pool());
        for (const auto deviceId : deviceIds) requestedDevices.values({ requestedDevices.cast(requestedDevices.value(deviceId), Type::uuid) });
        Query locks(context.pool());
        locks.with("requested", requestedDevices, { .columns_ = { "device_id" } })
            .select(locks.call("pg_advisory_xact_lock", { locks.call("hashtextextended", {
                locks.cast(locks.column("device_id"), Type::text), locks.cast(locks.value(734621), Type::big_int) }) }))
            .from("requested").add_order_by(locks.column("device_id"));
        (void)co_await transaction.query(locks);
        Query deviceRows(context.pool());
        deviceRows.select(deviceRows.column("device_id")).from("requested");
        Query seed(context.pool());
        seed.with("requested", requestedDevices, { .columns_ = { "device_id" } })
            .insert_into(service::telemetry::persistence::DeviceDataIngestStateEntity::table_name(), { "device_id" }).insert_from(deviceRows)
            .on_conflict({ .columns_ = { "device_id" }, .do_nothing_ = true });
        (void)co_await transaction.execute(seed);
        const auto rows = co_await transaction.query(persisted);
        co_await transaction.commit();
        std::vector<std::string> previous(messages.size(), "{}");
        for (const auto& row : rows) {
            const auto parsedSequence =
                service::utils::parseInt64(std::optional<std::string_view>{ row[0].value().value_or(std::string_view{}) });
            if (!parsedSequence || *parsedSequence < 0) {
                continue;
            }
            const auto sequence = static_cast<std::size_t>(*parsedSequence);
            if (sequence < previous.size() && row[1].value().has_value()) {
                previous[sequence].assign(row[1].value().value_or(std::string_view{}));
            }
        }
        co_return previous;
    }
};

} // namespace service::telemetry
