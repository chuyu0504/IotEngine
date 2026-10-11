#pragma once

#include "service/utils/number.h"
#include "service/features/telemetry/latest/latest.entity.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/web_worker.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/redis/redis.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/features/messaging/messaging.transport.h"

namespace service::telemetry::latest {

inline constexpr std::string_view kOnlineDeadlinesBase =
    "iot:v2:schedule:device:online-deadlines";
inline constexpr std::string_view kRealtimeChangesStream =
    "iot:live:changes";

template <typename Redis>
ruvia::task<void> publishRealtimeChange(const Redis& redis) {
    const auto reply = co_await service::message::redis::command(
        redis,
        { "XADD", std::string(kRealtimeChangesStream), "MAXLEN", "~", "100000", "*", "topic", "device.realtime" }
    );
    if (reply.kind() != ruvia::redis_value::kind_type::string) {
        service::message::redis::throwValue("publish device change", reply);
    }
}

inline std::string onlineDeadlinesKey() {
    return std::string(kOnlineDeadlinesBase);
}

inline std::string jsonEscape(std::string_view value) {
    std::string output;
    output.reserve(value.size() + 8);
    for (const char ch : value) {
        switch (ch) {
            case '"':
                output += "\\\"";
                break;
            case '\\':
                output += "\\\\";
                break;
            case '\n':
                output += "\\n";
                break;
            case '\r':
                output += "\\r";
                break;
            case '\t':
                output += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(ch) < 0x20) {
                    output += ' ';
                } else {
                    output.push_back(ch);
                }
        }
    }
    return output;
}

inline std::string jsonQuoted(std::string_view value) {
    return "\"" + jsonEscape(value) + "\"";
}

inline std::string jsonKeySet(const std::vector<std::string>& keys) {
    std::string result = "{";
    bool first = true;
    for (const auto& key : keys) {
        if (!first) {
            result.push_back(',');
        }
        first = false;
        result += jsonQuoted(key);
        result += ":true";
    }
    result.push_back('}');
    return result;
}

inline std::string stateJson(std::string_view state, std::string_view reason, std::string_view lastReport, std::string_view onlineUntil, std::string_view updatedAt) {
    std::string result = "{\"state\":" + jsonQuoted(state) +
        ",\"reason\":" + jsonQuoted(reason) + ",\"lastReportAt\":";
    result += lastReport.empty() ? "null" : std::string(lastReport);
    result += ",\"onlineUntil\":";
    result += onlineUntil.empty() ? "null" : std::string(onlineUntil);
    result += ",\"updatedAt\":";
    result += updatedAt.empty() ? "null" : std::string(updatedAt);
    result.push_back('}');
    return result;
}

template <typename Pipeline>
ruvia::task<bool> executeProjectionPipeline(Pipeline pipeline, std::string_view operation) {
    const auto replies = co_await std::move(pipeline).exec();
    service::message::redis::requirePipelineSuccess(operation, replies);
    co_return true;
}

template <typename Redis>
ruvia::task<void> initializeDevice(const Redis& redis, std::string_view deviceId, std::string_view deviceCode) {
    const auto now = std::to_string(service::message::utcNowMilliseconds());
    co_await service::message::redis::setHash(
        redis,
        runtimeKey(deviceId),
        { { "device_id", std::string(deviceId) },
          { "device_code", std::string(deviceCode) },
          { "state", "offline" },
          { "state_reason", "no_connection" },
          { "updated_at_ms", now } }
    );
    co_await service::message::redis::setHash(
        redis,
        latestKey(deviceId),
        { { "_device_id", std::string(deviceId) },
          { "_device_code", std::string(deviceCode) },
          { "_state", stateJson("offline", "no_connection", {}, {}, now) },
          { "_updated_at_ms", now } }
    );
    co_await publishRealtimeChange(redis);
}

template <typename Redis>
ruvia::task<void> eraseDevice(const Redis& redis, std::string_view deviceId) {
    co_await service::message::redis::eraseHash(redis, runtimeKey(deviceId));
    co_await service::message::redis::eraseHash(redis, latestKey(deviceId));
    (void)co_await service::message::redis::command(
        redis,
        { "ZREM", onlineDeadlinesKey(), std::string(deviceId) }
    );
    co_await publishRealtimeChange(redis);
}

template <typename Redis>
ruvia::task<void> signalFreshness(const Redis& redis) {
    co_await service::message::redis::wakeWorker(
        redis,
        std::nullopt,
        service::message::WorkerStreamTask::Freshness
    );
}

template <typename Redis>
ruvia::task<void> update(const Redis& redis, const std::vector<service::message::ParsedDeviceMessage>& messages) {
    if (messages.empty()) {
        co_return;
    }
    static constexpr std::string_view script = R"lua(
local function number_or(value, fallback)
  local number = tonumber(value)
  if number == nil then return fallback end
  return number
end
local function point_value(value, data_type)
  if data_type == 'BOOL' then
    if value == true or value == 1 or value == '1' or value == 'true' then return '1' end
    if value == false or value == 0 or value == '0' or value == 'false' then return '0' end
  end
  return tostring(value)
end
local payload = cjson.decode(ARGV[8])
local runtime_key = 'iot:v2:runtime:device:' .. ARGV[1]
local latest_key = 'iot:v2:device:' .. ARGV[1] .. ':latest'
if redis.call('HGET', runtime_key, 'device_id') ~= ARGV[1] then
  return -1
end
local configured_ids = {}
local has_configured_ids = false
local configured_json = redis.call('HGET', latest_key, '_element_ids')
if configured_json ~= false and configured_json ~= nil and configured_json ~= '' then
  local ok, decoded = pcall(cjson.decode, configured_json)
  if ok and type(decoded) == 'table' then
    configured_ids = decoded
    has_configured_ids = true
  end
end
local observed_at = number_or(ARGV[4], 0)
local online_until = observed_at + number_or(ARGV[7], 300000)
local now = number_or(ARGV[5], observed_at)
local current_report = number_or(redis.call('HGET', runtime_key, 'last_report_at_ms'), -1)
if ARGV[6] ~= 'derived' and observed_at >= current_report then
  local deadlines_key = KEYS[1]
  local earliest = redis.call('ZRANGE', deadlines_key, 0, 0, 'WITHSCORES')
  local wake = #earliest == 0 or online_until < number_or(earliest[2], online_until + 1)
  local state = 'online'
  local reason = ''
  if online_until < now then
    state = 'offline'
    reason = 'data_stale'
  end
  local state_json = cjson.encode({
    state = state,
    reason = reason,
    lastReportAt = observed_at,
    onlineUntil = online_until,
    updatedAt = now
  })
  redis.call('HSET', runtime_key,
    'device_id', ARGV[1], 'device_code', ARGV[2],
    'last_report_at_ms', ARGV[4], 'online_until_ms', tostring(online_until),
    'state', state, 'state_reason', reason, 'updated_at_ms', ARGV[5])
  redis.call('HSET', latest_key,
    '_device_id', ARGV[1], '_device_code', ARGV[2],
    '_state', state_json, '_updated_at_ms', ARGV[5])
  redis.call('ZADD', deadlines_key, online_until, ARGV[1])
  if wake then
    redis.call('XADD', KEYS[2],
               'MAXLEN', '~', '100000', '*', 'task', 'freshness')
  end
end
local count = 0
local touched = false
for element_id, point in pairs(payload.values or {}) do
  if not (has_configured_ids and configured_ids[element_id] == nil) then
    local value = '-'
    local existing = redis.call('HGET', latest_key, element_id)
    local previous = {}
    if existing ~= false and existing ~= nil and existing ~= '' then
      local ok, decoded = pcall(cjson.decode, existing)
      if ok and type(decoded) == 'table' then previous = decoded end
    end
    local data_type = tostring(point.dataType or previous.dataType or '')
    if point.value ~= nil and point.value ~= cjson.null then
      value = point_value(point.value, data_type)
    end
    local current = number_or(previous.observedAt, -1)
    if observed_at >= current then
      local elementName = tostring(point.name or previous.name or element_id)
      local unit = tostring(point.unit or previous.unit or '')
      local scale = previous.scale
      local decimals = previous.decimals
      local group = tostring(previous.group or '')
      local encode = tostring(previous.encode or '')
      local sort = number_or(previous.sort, 0)
      local document = cjson.encode({
        id = element_id,
        name = elementName,
        value = value,
        dataType = data_type,
        unit = unit,
        visible = point.visible ~= false and previous.visible ~= false,
        quality = point.quality or 'good',
        scale = scale,
        decimals = decimals,
        group = group,
        encode = encode,
        sort = sort,
        protocol = ARGV[3],
        observedAt = observed_at,
        updatedAt = number_or(ARGV[5], observed_at),
        source = ARGV[6]
      })
      redis.call('HSET', latest_key, element_id, document)
      count = count + 1
      touched = true
    end
  end
end
if observed_at >= current_report then touched = true end
if touched then redis.call('XADD', KEYS[3], 'MAXLEN', '~', '100000', '*', 'topic', 'device.realtime') end
return count
    )lua";
    const auto scriptSha = co_await redis.script_load(script);
    auto pipeline = redis.pipeline();
    for (const auto& parsed : messages) {
        const auto observedAt = std::to_string(parsed.observedAtMs);
        const auto updatedAt = std::to_string(service::message::utcNowMilliseconds());
        const auto onlineWindow = std::to_string(parsed.onlineWindowMs);
        const auto deadlinesKey = onlineDeadlinesKey();
        const auto wakeStream = service::message::workerWakeStream(std::nullopt, service::runtime::instanceId());
        const std::array<std::string_view, 14> command{
            "EVALSHA",
            scriptSha,
            "3",
            deadlinesKey,
            wakeStream,
            kRealtimeChangesStream,
            parsed.deviceId,
            parsed.deviceCode,
            parsed.protocol,
            observedAt,
            updatedAt,
            parsed.source,
            onlineWindow,
            parsed.valuesJson
        };
        // redis_pipeline copies every argument synchronously.
        pipeline.command(command);
    }
    const auto replies = co_await std::move(pipeline).exec();
    for (const auto& reply : replies) {
        if (reply.kind() == ruvia::redis_value::kind_type::error) {
            service::message::redis::throwValue("device latest update", reply);
        }
    }
}

template <typename Redis>
ruvia::task<void> expireStaleForKey(const Redis& redis, std::string deadlineKey) {
    const auto now = std::to_string(service::message::utcNowMilliseconds());
    const auto due = co_await service::message::redis::command(
        redis,
        { "ZRANGEBYSCORE", deadlineKey, "-inf", now, "LIMIT", "0", "1000" }
    );
    if (due.kind() != ruvia::redis_value::kind_type::array) {
        service::message::redis::throwValue("read device online deadlines", due);
    }
    static constexpr std::string_view script = R"lua(
local function number_or(value, fallback)
  local number = tonumber(value)
  if number == nil then return fallback end
  return number
end
local runtime_key = 'iot:v2:runtime:device:' .. ARGV[1]
local latest_key = 'iot:v2:device:' .. ARGV[1] .. ':latest'
local now = tonumber(ARGV[2])
if redis.call('ZREM', KEYS[1], ARGV[1]) ~= 1 then return 0 end
local expected = tonumber(redis.call('HGET', runtime_key, 'online_until_ms') or '-1')
if expected <= now then
  local state_json = cjson.encode({
    state = 'offline',
    reason = 'data_stale',
    lastReportAt = number_or(redis.call('HGET', runtime_key, 'last_report_at_ms'), 0),
    onlineUntil = expected,
    updatedAt = now
  })
  redis.call('HSET', runtime_key, 'state', 'offline', 'state_reason', 'data_stale',
             'updated_at_ms', ARGV[2])
  redis.call('HSET', latest_key, '_state', state_json, '_updated_at_ms', ARGV[2])
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', '100000', '*', 'topic', 'device.realtime')
  return 1
end
redis.call('ZADD', KEYS[1], expected, ARGV[1])
return 0
)lua";
    if (due.array().empty()) {
        co_return;
    }
    const auto scriptSha = co_await redis.script_load(script);
    auto pipeline = redis.pipeline();
    for (const auto& code : due.array()) {
        if (code.kind() != ruvia::redis_value::kind_type::string) {
            continue;
        }
        const std::array<std::string_view, 7> command{
            "EVALSHA",
            scriptSha,
            "2",
            deadlineKey,
            kRealtimeChangesStream,
            code.string(),
            now
        };
        pipeline.command(command);
    }
    const auto replies = co_await std::move(pipeline).exec();
    for (const auto& reply : replies) {
        if (reply.kind() == ruvia::redis_value::kind_type::error) {
            service::message::redis::throwValue("expire device online deadline", reply);
        }
    }
}

template <typename Redis>
ruvia::task<void> expireStale(const Redis& redis) {
    co_await expireStaleForKey(redis, onlineDeadlinesKey());
}

template <typename Redis>
ruvia::task<std::optional<std::int64_t>> nextDeadlineForKey(
    const Redis& redis,
    std::string deadlineKey
) {
    const auto reply = co_await service::message::redis::command(
        redis,
        { "ZRANGE", deadlineKey, "0", "0", "WITHSCORES" }
    );
    if (reply.kind() != ruvia::redis_value::kind_type::array) {
        service::message::redis::throwValue("read next device online deadline", reply);
    }
    if (reply.array().empty()) {
        co_return std::nullopt;
    }
    if (reply.array().size() != 2 ||
        reply.array()[1].kind() != ruvia::redis_value::kind_type::string) {
        service::message::redis::throwValue("parse next device online deadline", reply);
    }
    const auto score = service::utils::parseInt64(
        std::optional<std::string_view>{ reply.array()[1].string() }
    );
    if (!score) {
        throw std::runtime_error("invalid device online deadline score");
    }
    co_return *score;
}

template <typename Redis>
ruvia::task<std::optional<std::int64_t>> nextDeadline(const Redis& redis) {
    co_return co_await nextDeadlineForKey(redis, onlineDeadlinesKey());
}

inline std::optional<std::chrono::milliseconds>
deadlineWait(std::int64_t now, std::optional<std::int64_t> deadline) {
    if (!deadline.has_value()) {
        return std::nullopt;
    }
    if (*deadline <= now) {
        return std::chrono::milliseconds::zero();
    }
    return std::chrono::milliseconds(*deadline - now);
}

enum class ProjectionScope { Device, Protocol };

template <typename Context>
ruvia::task<void> project(Context& context, ProjectionScope scope, const std::vector<std::string>& ids, bool resetRuntime, bool preserveExisting = false) {
    if (ids.empty()) co_return;
    using Op = ruvia::db_binary_operator;
    using Type = ruvia::db_data_type;
    const auto selected = [&](ruvia::db_query& query) {
        std::vector<ruvia::db_expression> values;
        values.reserve(ids.size());
        for (const auto& id : ids) values.push_back(query.cast(query.value(id), Type::uuid));
        return query.binary(query.column(scope == ProjectionScope::Device ? "id" : "protocol_config_id", "d"), Op::in, query.list(values));
    };
    const auto redis = context.redis();
    const auto nowMs = service::message::utcNowMilliseconds();
    const auto now = std::to_string(nowMs);
    ruvia::db_query deviceQuery;
    const auto timeout = deviceQuery.binary(deviceQuery.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"protocol_params">(), "d"), Op::json_get_text, deviceQuery.value("online_timeout"));
    const auto parsedTimeout = deviceQuery.case_when({ { deviceQuery.binary(
        deviceQuery.coalesce({ timeout, deviceQuery.value("") }), Op::regex, deviceQuery.value("^-?[0-9]{1,18}$")),
        deviceQuery.cast(timeout, Type::big_int) } });
    deviceQuery.select({ deviceQuery.cast(deviceQuery.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"id">(), "d"), Type::text),
        deviceQuery.binary(deviceQuery.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"protocol_params">(), "d"), Op::json_get_text, deviceQuery.value("device_code")),
        deviceQuery.binary(deviceQuery.coalesce({ parsedTimeout, deviceQuery.value(300) }), Op::multiply, deviceQuery.value(1000)) })
        .from(service::telemetry::latest::persistence::DeviceEntity::table_name(), "d")
        .and_where(deviceQuery.unary(ruvia::db_unary_operator::is_null, deviceQuery.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
        .and_where(selected(deviceQuery)).add_order_by(deviceQuery.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"id">(), "d"));
    const auto devices = co_await context.db().query(deviceQuery);
    if (devices.empty()) {
        co_return;
    }

    std::set<std::string, std::less<>> recoveryDeviceIds;
    std::map<std::string, std::string, std::less<>> preservedDeadlines;
    if (preserveExisting) {
        auto existencePipeline = redis.pipeline();
        std::vector<std::vector<std::string>> existenceCommands;
        existenceCommands.reserve(devices.size() * 2);
        for (const auto& row : devices) {
            existenceCommands.push_back(
                { "HMGET", latestKey(row[0].value().value_or(std::string_view{})), "_device_id", "_element_ids" }
            );
            std::vector<std::string_view> views(existenceCommands.back().begin(), existenceCommands.back().end());
            existencePipeline.command(views);
            existenceCommands.push_back(
                { "HMGET", runtimeKey(row[0].value().value_or(std::string_view{})), "device_id", "online_until_ms" }
            );
            std::vector<std::string_view> runtimeViews(existenceCommands.back().begin(), existenceCommands.back().end());
            existencePipeline.command(runtimeViews);
        }
        const auto replies = co_await std::move(existencePipeline).exec();
        for (std::size_t index = 0; index < devices.size(); ++index) {
            const auto& row = devices[index];
            const auto latestIndex = index * 2;
            const auto runtimeIndex = latestIndex + 1;
            const bool matches =
                runtimeIndex < replies.size() &&
                replies[latestIndex].kind() == ruvia::redis_value::kind_type::array &&
                replies[latestIndex].array().size() == 2 &&
                replies[latestIndex].array()[0].kind() == ruvia::redis_value::kind_type::string &&
                replies[latestIndex].array()[0].string() == row[0].value().value_or(std::string_view{}) &&
                replies[latestIndex].array()[1].kind() == ruvia::redis_value::kind_type::string &&
                !replies[latestIndex].array()[1].string().empty() &&
                replies[runtimeIndex].kind() == ruvia::redis_value::kind_type::array &&
                replies[runtimeIndex].array().size() == 2 &&
                replies[runtimeIndex].array()[0].kind() == ruvia::redis_value::kind_type::string &&
                replies[runtimeIndex].array()[0].string() == row[0].value().value_or(std::string_view{});
            if (!matches) {
                recoveryDeviceIds.emplace(row[0].value().value_or(std::string_view{}));
            } else if (replies[runtimeIndex].array()[1].kind() ==
                       ruvia::redis_value::kind_type::string) {
                const auto deadline = service::utils::parseInt64(
                    std::optional<std::string_view>{ replies[runtimeIndex].array()[1].string() }
                );
                if (deadline) {
                    preservedDeadlines.insert_or_assign(std::string(row[0].value().value_or(std::string_view{})), std::to_string(*deadline));
                } else {
                    recoveryDeviceIds.emplace(row[0].value().value_or(std::string_view{}));
                }
            }
        }
    } else {
        for (const auto& row : devices) {
            recoveryDeviceIds.emplace(row[0].value().value_or(std::string_view{}));
        }
    }

    auto metaPipeline = redis.pipeline();
    std::vector<std::vector<std::string>> metaCommands;
    metaCommands.reserve(devices.size() * 6);
    std::map<std::string, std::int64_t, std::less<>> onlineWindows;
    std::map<std::string, std::vector<std::string>, std::less<>> elementIds;
    for (const auto& row : devices) {
        const std::string deviceId(row[0].value().value_or(std::string_view{}));
        const std::string deviceCode(row[1].value().value_or(std::string_view{}));
        onlineWindows.insert_or_assign(
            deviceId,
            service::utils::parseInt64(std::optional<std::string_view>{ row[2].value().value_or(std::string_view{}) })
                .value_or(300000)
        );
        elementIds.insert_or_assign(deviceId, std::vector<std::string>{});
        if (recoveryDeviceIds.contains(deviceId)) {
            metaCommands.push_back({ "DEL", latestKey(deviceId) });
            std::vector<std::string_view> views(metaCommands.back().begin(), metaCommands.back().end());
            metaPipeline.command(views);
            metaCommands.push_back({ "DEL", runtimeKey(deviceId) });
            std::vector<std::string_view> runtimeViews(metaCommands.back().begin(), metaCommands.back().end());
            metaPipeline.command(runtimeViews);
        }
        metaCommands.push_back({ "HSET", latestKey(deviceId), "_device_id", deviceId, "_device_code", deviceCode, "_updated_at_ms", now });
        {
            std::vector<std::string_view> views(metaCommands.back().begin(), metaCommands.back().end());
            metaPipeline.command(views);
        }
        metaCommands.push_back({ "HSETNX", latestKey(deviceId), "_state", stateJson("offline", "no_data", {}, {}, now) });
        {
            std::vector<std::string_view> views(metaCommands.back().begin(), metaCommands.back().end());
            metaPipeline.command(views);
        }
        metaCommands.push_back({ "HSET", runtimeKey(deviceId), "device_id", deviceId, "device_code", deviceCode, "updated_at_ms", now });
        if (resetRuntime || recoveryDeviceIds.contains(deviceId)) {
            metaCommands.back().push_back("state");
            metaCommands.back().push_back("offline");
            metaCommands.back().push_back("state_reason");
            metaCommands.back().push_back(recoveryDeviceIds.contains(deviceId) ? "no_data" : "startup");
        }
        {
            std::vector<std::string_view> views(metaCommands.back().begin(), metaCommands.back().end());
            metaPipeline.command(views);
        }
        if (recoveryDeviceIds.contains(deviceId)) {
            metaCommands.push_back(
                { "ZREM", onlineDeadlinesKey(), deviceId }
            );
            std::vector<std::string_view> views(metaCommands.back().begin(), metaCommands.back().end());
            metaPipeline.command(views);
        } else if (preservedDeadlines.contains(deviceId)) {
            metaCommands.push_back({ "ZADD", onlineDeadlinesKey(), preservedDeadlines.at(deviceId), deviceId });
            std::vector<std::string_view> views(metaCommands.back().begin(), metaCommands.back().end());
            metaPipeline.command(views);
        }
    }
    co_await executeProjectionPipeline(std::move(metaPipeline), "project latest metadata");

    const auto configuredProtocol = [&](std::string_view protocol, std::string_view arrayKey, int order) {
        ruvia::db_query query;
        query.from(service::telemetry::latest::persistence::DeviceEntity::table_name(), "d")
            .join(ruvia::db_join_type::inner, service::telemetry::latest::persistence::DeviceModelEntity::table_name(), query.binary(query.column(service::telemetry::latest::persistence::DeviceModelEntity::column_name<"device_id">(), "p"), Op::equal, query.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"id">(), "d")), "p")
            .and_where(query.binary(query.column(service::telemetry::latest::persistence::DeviceModelEntity::column_name<"protocol">(), "p"), Op::equal, query.value(protocol)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"deleted_at">(), "d")))
            .and_where(selected(query));
        const auto entries = query.call("jsonb_array_elements", { query.coalesce({
            query.binary(query.column(service::telemetry::latest::persistence::DeviceModelEntity::column_name<"config">(), "p"), Op::json_get, query.value(arrayKey)), query.cast(query.value("[]"), Type::jsonb) }) });
        std::vector<ruvia::db_expression> columns{ query.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"id">(), "d"),
            query.binary(query.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"protocol_params">(), "d"), Op::json_get_text, query.value("device_code")),
            query.column(service::telemetry::latest::persistence::DeviceModelEntity::column_name<"protocol">(), "p"), query.column("element"), query.cast(query.value(order), Type::integer) };
        if (protocol == "SL651" && arrayKey == "funcs") {
            query.join_function(ruvia::db_join_type::cross, entries, {}, "functions",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = { { .name_ = "function" }, { .name_ = "function_position" } } });
            const auto fieldArray = [&](std::string_view key) {
                return query.coalesce({ query.binary(query.column("function"), Op::json_get, query.value(key)), query.cast(query.value("[]"), Type::jsonb) });
            };
            query.join_function(ruvia::db_join_type::cross,
                query.call("jsonb_array_elements", { query.binary(fieldArray("elements"), Op::json_concat, fieldArray("responseElements")) }),
                {}, "elements", { .lateral_ = true, .with_ordinality_ = true, .columns_ = { { .name_ = "element" }, { .name_ = "element_position" } } });
            columns.push_back(query.column("function_position"));
            columns.push_back(query.column("element_position"));
        } else {
            query.join_function(ruvia::db_join_type::cross, entries, {}, "entry",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = { { .name_ = "element" }, { .name_ = "position" } } });
            columns.push_back(query.column("position"));
            columns.push_back(query.cast(query.value(0), Type::big_int));
        }
        const auto element = query.column("element");
        const auto configuration = query.column(service::telemetry::latest::persistence::DeviceModelEntity::column_name<"config">(), "p");
        const auto visibility = query.binary(query.binary(configuration, Op::json_get, query.value("pointVisibility")), Op::json_get,
            query.binary(element, Op::json_get_text, query.value("id")));
        columns[3] = query.binary(element, Op::json_concat, query.call("jsonb_build_object", {
            query.cast(query.value("visible"), Type::text), query.coalesce({visibility, query.binary(element, Op::json_get, query.value("visible")), query.cast(query.value("true"), Type::jsonb)})}));
        query.select(columns);
        return query;
    };
    auto configured = configuredProtocol("Modbus", "registers", 1);
    for (const auto protocol : {"Modbus", "S7", "SL651", "MC", "FINS", "DLT645", "MQTT"})
        configured.combine(ruvia::db_set_operation::union_all, configuredProtocol(protocol, "derivedPoints", 5));
    for (const auto protocol : {"MC", "FINS", "DLT645", "MQTT"})
        configured.combine(ruvia::db_set_operation::union_all, configuredProtocol(protocol, "points", 4));
    const auto s7 = configuredProtocol("S7", "areas", 2);
    const auto sl651 = configuredProtocol("SL651", "funcs", 3);
    configured.combine(ruvia::db_set_operation::union_all, s7).combine(ruvia::db_set_operation::union_all, sl651);
    ruvia::db_query numbered;
    const ruvia::db_window_options order{
        .partition_by_ = { numbered.column("device_id", "configured") },
        .order_by_ = { { numbered.column("protocol_order", "configured") }, { numbered.column("function_order", "configured") }, { numbered.column("element_order", "configured") } }
    };
    numbered.select({ numbered.star("configured"), numbered.alias(numbered.binary(
        numbered.over(numbered.call("row_number"), order), Op::subtract, numbered.value(1)), "sort_order") }).from("configured");
    ruvia::db_query points;
    const auto elementText = [&](std::string_view key) {
        return points.binary(points.column("element", "numbered"), Op::json_get_text, points.value(key));
    };
    const auto defaultText = [&](std::string_view key, std::string_view fallback) {
        return points.coalesce({ elementText(key), points.value(fallback) });
    };
    const auto pointValue = points.binary(points.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"value">(), "point"), Op::json_get_text, points.value("value"));
    const auto displayValue = points.case_when({ { points.binary(points.call("jsonb_typeof", {
        points.binary(points.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"value">(), "point"), Op::json_get, points.value("value")) }), Op::equal, points.value("boolean")),
        points.case_when({ { points.cast(pointValue, Type::boolean), points.value("1") } }, points.value("0")) } },
        points.coalesce({ pointValue, points.value("-") }));
    const auto observedAt = points.cast(points.binary(points.extract(ruvia::db_date_part::epoch,
        points.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"observed_at">(), "point")), Op::multiply, points.value(1000)), Type::big_int);
    const auto decimals = points.coalesce({ elementText("decimals"), elementText("digits") });
    const auto numericScale = points.coalesce({ points.case_when({ { points.binary(defaultText("scale", ""), Op::regex,
        points.value(R"(^-?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$)")), points.cast(elementText("scale"), Type::numeric) } }), points.value(1) });
    const auto numericDecimals = points.coalesce({ points.case_when({ { points.binary(
        points.coalesce({ decimals, points.value("") }), Op::regex, points.value("^-?[0-9]{1,18}$")), points.cast(decimals, Type::big_int) } }), points.value(-1) });
    const auto missing = points.unary(ruvia::db_unary_operator::is_null, points.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"observed_at">(), "point"));
    const auto storedPoint = points.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"value">(), "point");
    const auto observedUnit = points.coalesce({points.binary(storedPoint, Op::json_get_text, points.value("unit")), defaultText("unit", "")});
    const auto json = points.call("jsonb_build_object", {
        points.cast(points.value("id"), ruvia::db_data_type::text), elementText("id"), points.cast(points.value("name"), ruvia::db_data_type::text), elementText("name"), points.cast(points.value("value"), ruvia::db_data_type::text), displayValue,
        points.cast(points.value("dataType"), ruvia::db_data_type::text), defaultText("dataType", ""), points.cast(points.value("unit"), ruvia::db_data_type::text), observedUnit,
        points.cast(points.value("visible"), Type::text), points.cast(defaultText("visible", "true"), Type::boolean),
        points.cast(points.value("quality"), Type::text), points.coalesce({points.binary(storedPoint, Op::json_get_text, points.value("quality")), points.value("missing")}),
        points.cast(points.value("scale"), ruvia::db_data_type::text), numericScale, points.cast(points.value("decimals"), ruvia::db_data_type::text), numericDecimals,
        points.cast(points.value("group"), ruvia::db_data_type::text), defaultText("group", ""), points.cast(points.value("encode"), ruvia::db_data_type::text), defaultText("encode", ""),
        points.cast(points.value("sort"), ruvia::db_data_type::text), points.column("sort_order", "numbered"), points.cast(points.value("protocol"), ruvia::db_data_type::text), points.column("protocol", "numbered"),
        points.cast(points.value("observedAt"), ruvia::db_data_type::text), points.case_when({ { missing, points.null_value() } }, observedAt),
        points.cast(points.value("updatedAt"), ruvia::db_data_type::text), points.cast(points.binary(points.extract(ruvia::db_date_part::epoch, points.call("clock_timestamp")), Op::multiply, points.value(1000)), Type::big_int),
        points.cast(points.value("source"), ruvia::db_data_type::text), points.case_when({ { missing, points.cast(points.value("empty"), ruvia::db_data_type::text) } }, points.cast(points.value("database"), ruvia::db_data_type::text)) });
    points.with("configured", configured, { .columns_ = { "device_id", "device_code", "protocol", "element", "protocol_order", "function_order", "element_order" } })
        .with("numbered", numbered)
        .select({ points.cast(points.column("device_id", "numbered"), Type::text), points.column("device_code", "numbered"), points.column("protocol", "numbered"),
            elementText("id"), elementText("name"), observedUnit, displayValue,
            points.coalesce({ points.cast(observedAt, Type::text), points.value("") }),
            points.coalesce({ points.null_if(elementText("scale"), points.value("")), points.value("1") }),
            points.coalesce({ points.null_if(decimals, points.value("")), points.value("-1") }),
            defaultText("group", ""), defaultText("encode", ""), points.cast(points.column("sort_order", "numbered"), Type::text), points.cast(json, Type::text) })
        .from("numbered")
        .join(ruvia::db_join_type::left, service::telemetry::latest::persistence::DeviceLatestValueEntity::table_name(), points.binary(
            points.binary(points.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"device_id">(), "point"), Op::equal, points.column("device_id", "numbered")), Op::and_value,
            points.binary(points.column(service::telemetry::latest::persistence::DeviceLatestValueEntity::column_name<"element_id">(), "point"), Op::equal, elementText("id"))), "point")
        .add_order_by(points.column("device_id", "numbered")).add_order_by(points.column("protocol_order", "numbered"))
        .add_order_by(points.column("function_order", "numbered")).add_order_by(points.column("element_order", "numbered"));
    const auto elements = co_await context.db().query(points);
    static constexpr std::string_view kRefreshElementMetadataScript = R"lua(
local element_id = ARGV[1]
local ok, incoming = pcall(cjson.decode, ARGV[2])
if not ok or type(incoming) ~= 'table' then
  return redis.error_reply('invalid latest element metadata')
end
local existing = redis.call('HGET', KEYS[1], element_id)
if existing ~= false and existing ~= nil and existing ~= '' then
  local decoded_ok, previous = pcall(cjson.decode, existing)
  if decoded_ok and type(previous) == 'table' then
    if previous.value ~= nil then
      incoming.value = previous.value
      if incoming.dataType == 'BOOL' then
        if previous.value == true or previous.value == 1 or previous.value == '1' or previous.value == 'true' then
          incoming.value = '1'
        elseif previous.value == false or previous.value == 0 or previous.value == '0' or previous.value == 'false' then
          incoming.value = '0'
        end
      end
    end
    if previous.observedAt ~= nil then incoming.observedAt = previous.observedAt end
    if previous.updatedAt ~= nil then incoming.updatedAt = previous.updatedAt end
    if previous.source ~= nil then incoming.source = previous.source end
  end
end
redis.call('HSET', KEYS[1], element_id, cjson.encode(incoming))
return 1
)lua";
    auto pipeline = redis.pipeline();
    std::vector<std::vector<std::string>> commands;
    commands.reserve(elements.size() + elementIds.size());
    std::map<std::string, std::int64_t, std::less<>> lastReports;
    for (const auto& row : elements) {
        const std::string deviceId(row[0].value().value_or(std::string_view{}));
        const std::string elementId(row[3].value().value_or(std::string_view{}));
        elementIds[deviceId].push_back(elementId);
        commands.push_back({ "EVAL", std::string(kRefreshElementMetadataScript), "1", latestKey(deviceId), elementId, std::string(row[13].value().value_or(std::string_view{})) });
        std::vector<std::string_view> views;
        views.reserve(commands.back().size());
        for (const auto& argument : commands.back()) {
            views.push_back(argument);
        }
        pipeline.command(views);
        if (!row[7].value().value_or(std::string_view{}).empty()) {
            const auto parsed = service::utils::parseInt64(
                std::optional<std::string_view>{ row[7].value().value_or(std::string_view{}) }
            );
            if (parsed) {
                auto& lastReport = lastReports[deviceId];
                lastReport = std::max(lastReport, *parsed);
            }
        }
    }
    for (const auto& [deviceId, ids] : elementIds) {
        commands.push_back({ "HSET", latestKey(deviceId), "_element_ids", jsonKeySet(ids) });
        std::vector<std::string_view> views;
        views.reserve(commands.back().size());
        for (const auto& argument : commands.back()) {
            views.push_back(argument);
        }
        pipeline.command(views);
    }
    if (!commands.empty()) {
        co_await executeProjectionPipeline(std::move(pipeline), "project latest elements");
    }

    if (!lastReports.empty()) {
        auto reportPipeline = redis.pipeline();
        std::vector<std::vector<std::string>> reportCommands;
        reportCommands.reserve(lastReports.size() * 2);
        for (const auto& [deviceId, lastReport] : lastReports) {
            const auto window =
                onlineWindows.contains(deviceId) ? onlineWindows.at(deviceId) : 300000;
            const auto onlineUntil = lastReport + window;
            const auto online = onlineUntil >= nowMs;
            const auto state = online ? std::string_view("online") : std::string_view("offline");
            const auto reason = online ? std::string_view{} : std::string_view("data_stale");
            const auto lastReportText = std::to_string(lastReport);
            const auto onlineUntilText = std::to_string(onlineUntil);
            reportCommands.push_back(
                { "HSET", runtimeKey(deviceId), "last_report_at_ms", lastReportText, "online_until_ms", onlineUntilText, "state", std::string(state), "state_reason", std::string(reason), "updated_at_ms", now }
            );
            std::vector<std::string_view> views(reportCommands.back().begin(), reportCommands.back().end());
            reportPipeline.command(views);
            reportCommands.push_back({ "HSET", latestKey(deviceId), "_state", stateJson(state, reason, lastReportText, onlineUntilText, now), "_updated_at_ms", now });
            std::vector<std::string_view> latestViews(reportCommands.back().begin(), reportCommands.back().end());
            reportPipeline.command(latestViews);
            reportCommands.push_back({ "ZADD", onlineDeadlinesKey(), std::to_string(onlineUntil), deviceId });
            std::vector<std::string_view> deadlineViews(reportCommands.back().begin(), reportCommands.back().end());
            reportPipeline.command(deadlineViews);
        }
        co_await executeProjectionPipeline(std::move(reportPipeline), "project latest state");
    }
    co_await signalFreshness(redis);
    co_await publishRealtimeChange(redis);
}

template <typename Context>
ruvia::task<void> projectDevice(Context& context, std::string_view id) {
    const std::vector<std::string> ids{ std::string(id) };
    co_await project(context, ProjectionScope::Device, ids, false, true);
}

template <typename Context>
ruvia::task<void> projectProtocol(Context& context, std::string_view id) {
    const std::vector<std::string> ids{ std::string(id) };
    co_await project(context, ProjectionScope::Protocol, ids, false, true);
}

inline ruvia::task<void> hydrate(ruvia::web_worker_context& context, std::size_t workerIndex = 0, std::size_t workerCount = 1) {
    if (workerCount == 0 || workerIndex >= workerCount) {
        throw std::invalid_argument("invalid hydration worker ownership");
    }
    // Redis is the realtime read model. Keep its persisted hashes across a
    // service restart; PostgreSQL repairs metadata and is consulted only for
    // devices whose Redis projection is missing or incomplete.
    // A web worker owns exactly one PostgreSQL connection. Bound recovery result
    // sets so a large protocol definition cannot monopolize that connection for
    // the whole startup or retain every point JSON row in memory at once.
    static constexpr std::size_t kHydrationBatchSize = 32;
    std::string cursor = "00000000-0000-0000-0000-000000000000";
    for (;;) {
        ruvia::db_query page;
        page.select(page.cast(page.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"id">()), ruvia::db_data_type::text)).from(service::telemetry::latest::persistence::DeviceEntity::table_name())
            .and_where(page.unary(ruvia::db_unary_operator::is_null, page.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"deleted_at">())))
            .and_where(page.binary(page.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::greater,
                page.cast(page.value(cursor), ruvia::db_data_type::uuid)))
            .add_order_by(page.column(service::telemetry::latest::persistence::DeviceEntity::column_name<"id">())).limit(kHydrationBatchSize);
        const auto devices = co_await context.db().query(page);
        if (devices.empty()) {
            break;
        }

        std::vector<std::string> ids;
        ids.reserve(devices.size());
        for (const auto& row : devices) {
            ids.emplace_back(row[0].value().value_or(std::string_view{}));
        }
        cursor.assign(devices[devices.size() - 1][0].value().value_or(std::string_view{}));
        if (!ids.empty()) {
            co_await project(context, ProjectionScope::Device, ids, false, true);
        }
        if (devices.size() < kHydrationBatchSize) {
            break;
        }
    }
}

} // namespace service::telemetry::latest
