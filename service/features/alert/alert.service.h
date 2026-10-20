#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
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
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/access/access.entity.h"
#include "service/features/access/access.service.h"
#include "service/features/alert/alert.entity.h"
#include "service/features/configuration/configuration.entity.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/telemetry/telemetry.protocol.h"
#include "service/utils/number.h"
#include "service/utils/uuid.h"

namespace service::alert::metadata {

template <typename Context>
ruvia::task<void> refresh(Context& context);

// KEYS: 设备离线时长哈希、离线截止有序集、共享唤醒流、消费输入通知频道；ARGV[1]: 样本观测时间。
inline constexpr std::string_view kScheduleScript = R"lua(
local values = redis.call('HGETALL', KEYS[1])
if #values == 0 then return 0 end
local earliest = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
local old_earliest = #earliest == 0 and nil or tonumber(earliest[2])
local wake = false
local changed = 0
local observed = tonumber(ARGV[1])
if not observed then return 0 end
for index = 1, #values, 2 do
  local duration = tonumber(values[index + 1])
  if duration then
    local deadline = observed + duration
    local current = redis.call('ZSCORE', KEYS[2], values[index])
    if not current or deadline > tonumber(current) then
      redis.call('ZADD', KEYS[2], deadline, values[index])
      changed = changed + 1
      if not old_earliest or deadline < old_earliest then wake = true end
    end
  end
end
if wake then
  local entry_id = redis.call('XADD', KEYS[3], 'MAXLEN', '~', '100000', '*', 'task', 'freshness-alerts')
  redis.call('PUBLISH', KEYS[4], entry_id)
end
return changed
)lua";

// 在一个脚本内读取并删除到期成员；这是所有 Service Worker 共享的认领点，同一截止只由一个 Worker 评估。
inline constexpr std::string_view kClaimOfflineScript = R"lua(
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1],
                           'LIMIT', '0', ARGV[2])
for _, member in ipairs(members) do
  redis.call('ZREM', KEYS[1], member)
end
return members
)lua";

// KEYS: 规则设备集、离线规则标记、就绪标记、离线时长键集合、离线截止有序集、共享唤醒流、消费输入通知频道。
// ARGV: <设备数> <设备 ID...> <离线条目数> 每条 <时长键> <成员> <时长ms> <截止ms> <设备运行态键>。
inline constexpr std::string_view kRefreshScript = R"lua(
local previous = redis.call('SMEMBERS', KEYS[4])
for _, key in ipairs(previous) do redis.call('DEL', key) end
local had_deadlines = redis.call('ZCARD', KEYS[5]) > 0
redis.call('DEL', KEYS[1], KEYS[4], KEYS[5])
local device_count = tonumber(ARGV[1]) or 0
local cursor = 2
for index = 1, device_count do
  redis.call('SADD', KEYS[1], ARGV[cursor])
  cursor = cursor + 1
end
local offline_count = tonumber(ARGV[cursor]) or 0
cursor = cursor + 1
for index = 1, offline_count do
  local duration_key = ARGV[cursor]
  local member = ARGV[cursor + 1]
  local duration = tonumber(ARGV[cursor + 2])
  local deadline = tonumber(ARGV[cursor + 3])
  local runtime_observed = redis.call('HGET', ARGV[cursor + 4], 'last_report_at_ms')
  local runtime_observed_ms = runtime_observed and tonumber(runtime_observed) or nil
  if runtime_observed_ms then
    deadline = math.max(deadline, runtime_observed_ms + duration)
  end
  redis.call('HSET', duration_key, member, duration)
  redis.call('SADD', KEYS[4], duration_key)
  redis.call('ZADD', KEYS[5], deadline, member)
  cursor = cursor + 5
end
redis.call('SET', KEYS[2], offline_count > 0 and '1' or '0')
redis.call('SET', KEYS[3], '1')
if had_deadlines or offline_count > 0 then
  local entry_id = redis.call('XADD', KEYS[6], 'MAXLEN', '~', '100000', '*',
                              'task', 'freshness-alerts')
  redis.call('PUBLISH', KEYS[7], entry_id)
end
return offline_count
)lua";

inline constexpr std::string_view kRuleDevicesKey{ "iot:alert:rule-devices" };
inline constexpr std::string_view kOfflineRulesKey{ "iot:alert:offline-rules" };
inline constexpr std::string_view kReadyKey{ "iot:alert:metadata-ready" };
inline constexpr std::string_view kOfflineDurationKeys{ "iot:alert:offline-duration-keys" };
inline constexpr std::string_view kOfflineDurationPrefix{ "iot:alert:offline-duration:" };
inline constexpr std::string_view kOfflineDeadlinesBase{
    "iot:schedule:alert:offline-deadlines"
};

inline std::string offlineDeadlinesKey() {
    return std::string(kOfflineDeadlinesBase);
}

inline std::string offlineDurationKey(std::string_view deviceId) {
    return std::string(kOfflineDurationPrefix) + std::string(deviceId);
}

template <typename Redis>
ruvia::task<void> schedule(
    const Redis& redis,
    const service::message::RedisChannelScope& scope,
    const std::vector<service::message::ParsedDeviceMessage>& messages,
    std::string_view instance
) {
    if (messages.empty()) {
        co_return;
    }
    const auto scriptSha = co_await redis.script_load(kScheduleScript);
    const auto deadlinesKey = offlineDeadlinesKey();
    const auto wakeStream = service::message::workerWakeStream(std::nullopt, instance);
    const auto wakeChannel = service::message::consumer::inputChannel(scope, wakeStream);
    auto pipeline = redis.pipeline();
    for (const auto& message : messages) {
        const auto durationKey = offlineDurationKey(message.deviceId);
        const auto observedAt = std::to_string(message.observedAtMs);
        const std::array<std::string_view, 8> command{
            "EVALSHA",
            scriptSha,
            "4",
            durationKey,
            deadlinesKey,
            wakeStream,
            wakeChannel,
            observedAt
        };
        pipeline.command(std::span<const std::string_view>(command));
    }
    const auto replies = co_await std::move(pipeline).exec();
    service::utils::redis::requirePipelineSuccess("schedule offline alerts", replies);
}

template <typename Redis>
ruvia::task<std::optional<std::int64_t>> nextOfflineDeadline(
    const Redis& redis
) {
    const std::vector<std::string> replyArguments{ "ZRANGE", offlineDeadlinesKey(), "0", "0", "WITHSCORES" };
    const auto reply = co_await redis.command(std::vector<std::string_view>(replyArguments.begin(), replyArguments.end()));
    if (reply.kind() != ruvia::redis_value::kind_type::array) {
        service::utils::redis::throwValue("read next offline-alert deadline", reply);
    }
    if (reply.array().empty()) {
        co_return std::nullopt;
    }
    if (reply.array().size() != 2 ||
        reply.array()[1].kind() != ruvia::redis_value::kind_type::string) {
        service::utils::redis::throwValue("parse next offline-alert deadline", reply);
    }
    const auto score = service::utils::parseInt64(
        std::optional<std::string_view>{ reply.array()[1].string() }
    );
    if (!score) {
        throw std::runtime_error("invalid offline-alert deadline score");
    }
    co_return *score;
}

template <typename Redis>
ruvia::task<std::vector<std::string>> dueOffline(
    const Redis& redis,
    std::int64_t nowMs,
    std::size_t maximum = 1000
) {
    const auto key = offlineDeadlinesKey();
    const auto now = std::to_string(nowMs);
    const auto limit = std::to_string(maximum);
    const std::string_view keys[]{ key };
    const std::string_view arguments[]{ now, limit };
    const auto reply = co_await redis.eval(kClaimOfflineScript, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::array) {
        service::utils::redis::throwValue("read due offline-alert deadlines", reply);
    }
    std::vector<std::string> result;
    result.reserve(reply.array().size());
    for (const auto& value : reply.array()) {
        if (value.kind() != ruvia::redis_value::kind_type::string) {
            service::utils::redis::throwValue("parse due offline-alert deadline", reply);
        }
        result.emplace_back(value.string());
    }
    co_return result;
}

template <typename Redis>
ruvia::task<void> removeOfflineDeadlines(
    const Redis& redis,
    const std::vector<std::string>& members
) {
    if (members.empty()) {
        co_return;
    }
    std::vector<std::string> command{ "ZREM", offlineDeadlinesKey() };
    command.insert(command.end(), members.begin(), members.end());
    (void)co_await redis.command(std::vector<std::string_view>(command.begin(), command.end()));
}

struct Activity final {
    std::vector<bool> devices;
    bool offlineRules{ false };
};

template <typename Context>
ruvia::task<Activity> activity(
    Context& context,
    const std::vector<service::message::ParsedDeviceMessage>& messages
) {
    if (messages.empty()) {
        co_return Activity{};
    }
    std::map<std::string, std::vector<std::size_t>, std::less<>> positions;
    for (std::size_t index = 0; index < messages.size(); ++index) {
        positions[messages[index].deviceId].push_back(index);
    }
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto pipeline = context.redis().pipeline();
        pipeline.exists(kReadyKey);
        pipeline.command("GET", kOfflineRulesKey);
        for (const auto& [deviceId, indexes] : positions) {
            (void)indexes;
            pipeline.command("SISMEMBER", kRuleDevicesKey, deviceId);
        }
        const auto replies = co_await std::move(pipeline).exec();
        service::utils::redis::requirePipelineSuccess("read alert metadata", replies);
        const bool ready = !replies.empty() &&
            replies.front().kind() == ruvia::redis_value::kind_type::integer &&
            replies.front().integer() != 0;
        if (!ready) {
            co_await refresh(context);
            continue;
        }
        Activity result;
        result.devices.resize(messages.size(), false);
        result.offlineRules = replies.size() > 1 &&
            replies[1].kind() == ruvia::redis_value::kind_type::string &&
            replies[1].string() == "1";
        std::size_t replyIndex = 2;
        for (const auto& [deviceId, indexes] : positions) {
            (void)deviceId;
            const bool active = replyIndex < replies.size() &&
                replies[replyIndex].kind() == ruvia::redis_value::kind_type::integer &&
                replies[replyIndex].integer() != 0;
            for (const auto index : indexes) {
                result.devices[index] = active;
            }
            ++replyIndex;
        }
        co_return result;
    }
    throw std::runtime_error("alert metadata did not become ready");
}

namespace detail {

inline ruvia::db_query refreshQuery() {
    using Op = ruvia::db_binary_operator;
    using Type = ruvia::db_data_type;
    ruvia::db_query duration;
    const auto text = duration.binary(duration.column("value", "condition"), Op::json_get_text, duration.value("duration"));
    duration.select(duration.alias(duration.least({ duration.greatest({ duration.cast(text, Type::big_int), duration.value(1) }), duration.value(86400) }), "duration_seconds"))
        .and_where(duration.binary(text, Op::regex, duration.value("^[0-9]{1,10}$")));
    ruvia::db_query query;
    const auto deviceId = query.cast(query.column(service::alert::persistence::AlertRuleEntity::column_name<"device_id">(), "rule"), Type::text);
    const auto ruleId = query.cast(query.column(service::alert::persistence::AlertRuleEntity::column_name<"id">(), "rule"), Type::text);
    const auto durationText = query.case_when({ { query.unary(ruvia::db_unary_operator::is_null, query.column("value", "condition")), query.null_value() } }, query.cast(query.coalesce({ query.column("duration_seconds", "offline_duration"), query.value(300) }), Type::text));
    query.select({ deviceId, ruleId, durationText, query.cast(query.coalesce({ query.column(service::alert::persistence::AlertInputStateEntity::column_name<"observed_at_ms">(), "state"), query.value(0) }), Type::text), query.cast(query.column(service::configuration::persistence::DeviceEntity::column_name<"id">(), "device"), Type::text) }).distinct().from(service::alert::persistence::AlertRuleEntity::table_name(), "rule").join(ruvia::db_join_type::inner, service::configuration::persistence::DeviceEntity::table_name(), query.binary(query.column(service::configuration::persistence::DeviceEntity::column_name<"id">(), "device"), Op::equal, query.column(service::alert::persistence::AlertRuleEntity::column_name<"device_id">(), "rule"))).join(ruvia::db_join_type::left, service::alert::persistence::AlertInputStateEntity::table_name(), query.binary(query.column(service::alert::persistence::AlertInputStateEntity::column_name<"device_id">(), "state"), Op::equal, query.column(service::alert::persistence::AlertRuleEntity::column_name<"device_id">(), "rule")), "state").join_function(ruvia::db_join_type::left, query.call("jsonb_array_elements", { query.column(service::alert::persistence::AlertRuleEntity::column_name<"conditions">(), "rule") }), query.binary(query.binary(query.column("value", "condition"), Op::json_get_text, query.value("type")), Op::equal, query.value("offline")), "condition", { .lateral_ = true, .columns_ = { { .name_ = "value" } } }).join(ruvia::db_join_type::left, duration, query.value(true), "offline_duration", { .lateral_ = true }).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::persistence::AlertRuleEntity::column_name<"deleted_at">(), "rule"))).and_where(query.binary(query.column(service::alert::persistence::AlertRuleEntity::column_name<"status">(), "rule"), Op::equal, query.value("enabled"))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::configuration::persistence::DeviceEntity::column_name<"deleted_at">(), "device"))).and_where(query.binary(query.column(service::configuration::persistence::DeviceEntity::column_name<"status">(), "device"), Op::equal, query.value("enabled"))).add_order_by(deviceId).add_order_by(ruleId).add_order_by(durationText);
    return query;
}

} // namespace detail

template <typename Context>
ruvia::task<void> refresh(Context& context) {
    // Serialize the authoritative snapshot and Redis replacement across instances.
    // Telemetry reads never take this cold-path configuration lock.
    auto transaction = co_await context.db().begin_transaction();
    co_await transaction.advisory_lock(service::message::lock::kAlertMetadataProjection);
    const auto rows = co_await transaction.query(detail::refreshQuery());

    struct OfflineEntry final {
        std::string durationKey;
        std::string member;
        std::int64_t durationMs{ 0 };
        std::int64_t deadlineMs{ 0 };
        std::string runtimeKey;
    };

    std::set<std::string, std::less<>> devices;
    std::vector<OfflineEntry> offlineEntries;
    const auto now = service::message::utcNowMilliseconds();
    for (const auto& row : rows) {
        const std::string deviceId(row[0].value().value_or(std::string_view{}));
        devices.emplace(deviceId);
        if (!row[2].value().has_value()) {
            continue;
        }
        const auto durationSeconds =
            service::utils::parseInt64(std::optional<std::string_view>{ row[2].value().value_or(std::string_view{}) });
        if (!durationSeconds || *durationSeconds <= 0) {
            continue;
        }
        const auto observedAtMs =
            service::utils::parseInt64(std::optional<std::string_view>{ row[3].value().value_or(std::string_view{}) })
                .value_or(0);
        const auto durationMs = *durationSeconds * 1000;
        offlineEntries.push_back(
            { offlineDurationKey(deviceId),
              std::string(row[1].value().value_or(std::string_view{})) + ":" + std::to_string(durationMs),
              durationMs,
              observedAtMs > 0 ? observedAtMs + durationMs : now,
              service::message::telemetry::runtimeKey(row[4].value().value_or(std::string_view{})) }
        );
    }

    std::vector<std::string> arguments;
    arguments.reserve(2 + devices.size() + offlineEntries.size() * 5);
    arguments.push_back(std::to_string(devices.size()));
    for (const auto& deviceId : devices) {
        arguments.push_back(deviceId);
    }
    arguments.push_back(std::to_string(offlineEntries.size()));
    for (const auto& entry : offlineEntries) {
        arguments.push_back(entry.durationKey);
        arguments.push_back(entry.member);
        arguments.push_back(std::to_string(entry.durationMs));
        arguments.push_back(std::to_string(entry.deadlineMs));
        arguments.push_back(entry.runtimeKey);
    }
    std::vector<std::string> keys{
        std::string(kRuleDevicesKey),
        std::string(kOfflineRulesKey),
        std::string(kReadyKey),
        std::string(kOfflineDurationKeys),
        offlineDeadlinesKey(),
        service::message::workerWakeStream(std::nullopt, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId)
    };
    keys.push_back(service::message::consumer::inputChannel(context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, keys.back()));
    std::vector<std::string_view> keyViews(keys.begin(), keys.end());
    std::vector<std::string_view> views;
    views.reserve(arguments.size());
    for (const auto& argument : arguments) {
        views.push_back(argument);
    }
    const auto reply = co_await context.redis().eval(kRefreshScript, keyViews, views);
    if (reply.kind() == ruvia::redis_value::kind_type::error) {
        service::utils::redis::throwValue("refresh alert metadata", reply);
    }
    co_await transaction.commit();
}

inline ruvia::task<std::string> executeRefreshOperation(ruvia::web_worker_context& context, std::string_view operation, std::string_view, ruvia::stop_token stop) {
    if (stop.stop_requested()) {
        service::common::fail(10004, "Alert operation cancelled", 503);
    }
    if (operation != "refresh") {
        service::common::fail(10002, "Unknown alert operation", 400);
    }
    co_await refresh(context);
    co_return "{}";
}

} // namespace service::alert::metadata

namespace service::alert {

class AlertEvaluationService final {
    using Query = ruvia::db_query;
    using Op = ruvia::db_binary_operator;
    using Type = ruvia::db_data_type;

  public:
    // 消费 telemetry 告警复制流的一批条目（Dispatch 已规范化并修复 UTF-8，升级前的副本按需修复）：整批一次元数据读取与规则评估。
    // 失败时整批不确认；重放由 alert_input_state.message_id 与 alert_evaluation_receipt 保证幂等。
    static ruvia::task<void> evaluateTelemetry(ruvia::web_worker_context& context, const std::vector<service::message::StreamMessage>& entries) {
        if (entries.empty()) {
            co_return;
        }
        std::vector<service::message::ParsedDeviceMessage> messages;
        messages.reserve(entries.size());
        for (const auto& entry : entries) {
            messages.push_back(service::message::parsedFrom(entry));
            service::telemetry::contract::repairInvalidUtf8(messages.back());
        }
        const auto metadataState = co_await metadata::activity(context, messages);
        if (metadataState.offlineRules) {
            co_await metadata::schedule(context.redis(), context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, messages, context.worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId);
        }
        auto active = metadataState.devices;
        std::vector<std::string> previous;
        previous.reserve(messages.size());
        for (std::size_t index = 0; index < messages.size(); ++index) {
            if (service::telemetry::contract::isSl651EmptyReport(messages[index])) {
                active[index] = false;
                previous.emplace_back("{}");
            } else {
                previous.push_back(co_await recordInput(context, messages[index]));
            }
        }
        co_await evaluateSamples(context, messages, previous, active);
    }

    // 记录设备最新告警输入并返回本条样本的前值；乱序或重放的样本不覆盖更新的输入。
    static ruvia::task<std::string> recordInput(ruvia::web_worker_context& context, const service::message::ParsedDeviceMessage& value) {
        using State = persistence::AlertInputStateEntity;
        Query state(context.pool());
        const auto newer = state.binary(state.tuple({ state.excluded(State::column_name<"observed_at_ms">()), state.excluded(State::column_name<"message_id">()) }), Op::greater, state.tuple({ State::column<"observed_at_ms">().expression(state), State::column<"message_id">().expression(state) }));
        state.insert_into(State::table_name(), { State::column_name<"device_id">(), State::column_name<"observed_at_ms">(), State::column_name<"message_id">(), State::column_name<"data">(), State::column_name<"previous_data">() })
            .values({ state.cast(state.value(value.deviceId), Type::uuid), state.cast(state.value(value.observedAtMs), Type::big_int), state.cast(state.value(value.messageId), Type::uuid), state.cast(state.value(value.valuesJson), Type::jsonb), state.cast(state.value("{}"), Type::jsonb) })
            .on_conflict({ .columns_ = { std::string(State::column_name<"device_id">()) }, .update_ = { { std::string(State::column_name<"previous_data">()), state.case_when({ { newer, State::column<"data">().expression(state) } }, State::column<"previous_data">().expression(state)) }, { std::string(State::column_name<"data">()), state.case_when({ { newer, state.excluded(State::column_name<"data">()) } }, State::column<"data">().expression(state)) }, { std::string(State::column_name<"observed_at_ms">()), state.greatest({ state.excluded(State::column_name<"observed_at_ms">()), State::column<"observed_at_ms">().expression(state) }) }, { std::string(State::column_name<"message_id">()), state.case_when({ { newer, state.excluded(State::column_name<"message_id">()) } }, State::column<"message_id">().expression(state)) } } })
            .returning({ state.cast(state.case_when({ { state.binary(state.column(State::column_name<"message_id">()), Op::equal, state.cast(state.value(value.messageId), Type::uuid)), state.column(State::column_name<"previous_data">()) } }, state.cast(state.value("{}"), Type::jsonb)), Type::text) });
        const auto rows = co_await context.db().query(state);
        co_return std::string(rows.front()[0].value().value_or("{}"));
    }

    static ruvia::task<void> evaluateSamples(
        ruvia::web_worker_context& context,
        const std::vector<service::message::ParsedDeviceMessage>& messages,
        const std::vector<std::string>& previousData,
        const std::vector<bool>& active
    ) {
        if (messages.empty()) {
            co_return;
        }
        if (messages.size() != previousData.size() || messages.size() != active.size()) {
            throw std::invalid_argument("alert telemetry batch size mismatch");
        }
        std::vector<const service::message::ParsedDeviceMessage*> relevant;
        std::vector<std::string_view> relevantPrevious;
        relevant.reserve(messages.size());
        relevantPrevious.reserve(messages.size());
        for (std::size_t index = 0; index < messages.size(); ++index) {
            if (index < active.size() && active[index]) {
                relevant.push_back(&messages[index]);
                relevantPrevious.push_back(previousData[index]);
            }
        }
        if (relevant.empty()) {
            co_return;
        }

        const auto rules = co_await context.db().query(telemetryEvaluationQuery(relevant, relevantPrevious));
        if (rules.empty()) {
            co_await drainOutbox(context);
            co_return;
        }
        std::vector<std::vector<Evaluation>> evaluations(relevant.size());
        for (const auto& row : rules) {
            const auto parsedSequence =
                service::utils::parseInt64(std::optional<std::string_view>{ row[0].value().value_or(std::string_view{}) });
            if (!parsedSequence || *parsedSequence < 0) {
                continue;
            }
            const auto sequence = static_cast<std::size_t>(*parsedSequence);
            if (sequence >= relevant.size()) {
                continue;
            }
            evaluations[sequence].push_back(
                evaluation(row, 1, relevant[sequence]->valuesJson, service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()))
            );
        }
        for (std::size_t sequence = 0; sequence < relevant.size(); ++sequence) {
            co_await applyEvaluations(context, evaluations[sequence], relevant[sequence]->observedAtMs, relevant[sequence]->messageId);
        }
        co_await drainOutbox(context);
    }

    static ruvia::task<void>
    evaluateOfflineDue(ruvia::web_worker_context& context) {
        const auto now = nowMilliseconds();
        const auto due = co_await metadata::dueOffline(context.redis(), now);
        if (due.empty()) {
            co_return;
        }
        std::set<std::string, std::less<>> uniqueRules;
        for (const auto& member : due) {
            const auto separator = member.find(':');
            const auto ruleId = member.substr(0, separator);
            if (separator != std::string::npos && service::common::isUuid(ruleId)) {
                uniqueRules.emplace(ruleId);
            }
        }
        if (!uniqueRules.empty()) {
            const auto rules = co_await context.db().query(offlineEvaluationQuery(uniqueRules));
            co_await apply(context, rules, "{}", now);
        }
        co_await metadata::removeOfflineDeadlines(context.redis(), due);
    }

#ifdef IOT_ENGINE_TESTING
    static std::string evaluationTailForTest() {
        Query query;
        appendEvaluation(query);
        const auto statement = query.compile(ruvia::db_driver::postgresql, nullptr, ruvia::db_parameter_mode::literal);
        return std::string(statement.sql());
    }
#endif
    struct Evaluation {
        std::string ruleId;
        std::string ruleName;
        std::string severity;
        std::string deviceId;
        std::string deviceCode;
        bool matched{ false };
        std::string silence;
        std::string recovery;
        std::string recoveryWait;
        std::string data;
        std::string recordId;
        std::string message;
    };

    template <typename Rows>
    static ruvia::task<void>
    apply(ruvia::web_worker_context& context, const Rows& rows, std::string_view fallbackData, std::int64_t occurredAtMs) {
        std::vector<Evaluation> evaluations;
        evaluations.reserve(rows.size());
        for (const auto& row : rows) {
            evaluations.push_back(evaluation(row, 1, fallbackData, service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>())));
        }
        co_await applyEvaluations(context, evaluations, occurredAtMs, {});
    }

    static ruvia::task<void>
    applyEvaluations(ruvia::web_worker_context& context, const std::vector<Evaluation>& evaluations, std::int64_t occurredAtMs, std::string_view receiptId) {
        constexpr std::size_t kBatchSize = 256;
        for (std::size_t begin = 0; begin < evaluations.size();
             begin += kBatchSize) {
            co_await applyBatch(context, evaluations, begin, std::min(begin + kBatchSize, evaluations.size()), occurredAtMs, begin + kBatchSize >= evaluations.size() ? receiptId : std::string_view{});
        }
    }

    template <typename Row>
    static Evaluation evaluation(const Row& row, std::size_t offset, std::string_view fallbackData, std::string recordId) {
        const auto ruleName = std::string(row[offset + 1].value().value_or(std::string_view{}));
        return Evaluation{
            .ruleId = std::string(row[offset].value().value_or(std::string_view{})),
            .ruleName = ruleName,
            .severity = std::string(row[offset + 2].value().value_or(std::string_view{})),
            .deviceId = std::string(row[offset + 3].value().value_or(std::string_view{})),
            .deviceCode = std::string(row[offset + 4].value().value_or(std::string_view{})),
            .matched =
                row[offset + 5].value().value_or(std::string_view{}) == "t" || row[offset + 5].value().value_or(std::string_view{}) == "true",
            .silence = std::string(row[offset + 6].value().value_or(std::string_view{})),
            .recovery = std::string(row[offset + 7].value().value_or(std::string_view{})),
            .recoveryWait = std::string(row[offset + 8].value().value_or(std::string_view{})),
            .data = !row[offset + 9].value().has_value() ? std::string(fallbackData)
                                                         : std::string(row[offset + 9].value().value_or(std::string_view{})),
            .recordId = std::move(recordId),
            .message = ruleName + " 触发告警",
        };
    }

    static ruvia::task<void>
    applyBatch(ruvia::web_worker_context& context, const std::vector<Evaluation>& evaluations, std::size_t begin, std::size_t end, std::int64_t occurredAtMs, std::string_view receiptId) {
        if (begin >= end) {
            co_return;
        }

        Query incoming(context.pool());
        for (auto index = begin; index < end; ++index) {
            const auto& evaluation = evaluations[index];
            incoming.values({ incoming.cast(incoming.value(std::string_view(evaluation.ruleId)), Type::uuid), incoming.cast(incoming.value(evaluation.matched), Type::boolean), incoming.cast(incoming.value(std::string_view(evaluation.recordId)), Type::uuid), incoming.cast(incoming.value(std::string_view(evaluation.deviceId)), Type::uuid), incoming.cast(incoming.value(std::string_view(evaluation.severity)), Type::text), incoming.cast(incoming.value(std::string_view(evaluation.message)), Type::text), incoming.cast(incoming.value(std::string_view(evaluation.data)), Type::jsonb), incoming.cast(incoming.value(std::string_view(evaluation.silence)), Type::integer), incoming.cast(incoming.value(std::string_view(evaluation.recovery)), Type::text), incoming.cast(incoming.value(std::string_view(evaluation.recoveryWait)), Type::integer), incoming.cast(incoming.value(std::string_view(evaluation.ruleName)), Type::text), incoming.cast(incoming.value(std::string_view(evaluation.deviceCode)), Type::text), incoming.cast(incoming.value(occurredAtMs), Type::big_int), incoming.cast(incoming.value(receiptId), Type::text) });
        }

        Query stateRows(context.pool());
        const auto initialRecovery = stateRows.case_when(
            { { stateRows.column("matched", "incoming"), stateRows.null_value() } },
            stateRows.call("now")
        );
        stateRows
            .select({ stateRows.column("rule_id", "incoming"), stateRows.column("matched", "incoming"), initialRecovery, stateRows.call("now"), stateRows.call("now") })
            .from("incoming", "incoming");

        Query states(context.pool());
        const auto recoveryStarted = states.case_when(
            { { states.excluded(service::alert::persistence::AlertRuleStateEntity::column_name<"matched">()), states.null_value() },
              { states.column(service::alert::persistence::AlertRuleStateEntity::column_name<"matched">(), "alert_rule_state"), states.call("now") } },
            states.coalesce({ states.column(service::alert::persistence::AlertRuleStateEntity::column_name<"recovery_started_at">(), "alert_rule_state"), states.call("now") })
        );
        using RuleState = persistence::AlertRuleStateEntity;
        states
            .insert_into(RuleState::table_name(), { RuleState::column_name<"rule_id">(), RuleState::column_name<"matched">(), RuleState::column_name<"recovery_started_at">(), RuleState::column_name<"last_evaluated_at">(), RuleState::column_name<"updated_at">() })
            .insert_from(stateRows)
            .on_conflict({ .columns_ = { std::string(RuleState::column_name<"rule_id">()) }, .update_ = { { std::string(RuleState::column_name<"recovery_started_at">()), recoveryStarted }, { std::string(RuleState::column_name<"matched">()), states.excluded(RuleState::column_name<"matched">()) }, { std::string(RuleState::column_name<"last_evaluated_at">()), states.call("now") }, { std::string(RuleState::column_name<"updated_at">()), states.call("now") } } })
            .returning({ states.column(service::alert::persistence::AlertRuleStateEntity::column_name<"rule_id">()), states.column(service::alert::persistence::AlertRuleStateEntity::column_name<"recovery_started_at">()) });

        Query activeRecord(context.pool());
        activeRecord
            .select(activeRecord.cast(activeRecord.value(std::int64_t{ 1 }), Type::integer))
            .from(service::alert::persistence::OpenAlertRecordEntity::table_name(), "record")
            .and_where(activeRecord.binary(activeRecord.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"rule_id">(), "record"), Op::equal, activeRecord.column("rule_id", "incoming")))
            .and_where(activeRecord.binary(
                activeRecord.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"status">(), "record"),
                Op::in,
                activeRecord.list({ activeRecord.value("active"), activeRecord.value("acknowledged") })
            ));
        Query recentRecord(context.pool());
        const auto silenceWindow = recentRecord.binary(
            recentRecord.column("silence_duration", "incoming"),
            Op::multiply,
            recentRecord.cast(recentRecord.value("1 second"), Type::interval)
        );
        recentRecord
            .select(recentRecord.cast(recentRecord.value(std::int64_t{ 1 }), Type::integer))
            .from(service::alert::persistence::OpenAlertRecordEntity::table_name(), "record")
            .and_where(recentRecord.binary(recentRecord.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"rule_id">(), "record"), Op::equal, recentRecord.column("rule_id", "incoming")))
            .and_where(recentRecord.binary(
                recentRecord.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"triggered_at">(), "record"),
                Op::greater,
                recentRecord.binary(recentRecord.call("now"), Op::subtract, silenceWindow)
            ));

        Query createdRows(context.pool());
        createdRows
            .select({ createdRows.column("record_id", "incoming"), createdRows.column("rule_id", "incoming"), createdRows.column("device_id", "incoming"), createdRows.column("severity", "incoming"), createdRows.cast(createdRows.value("active"), Type::text), createdRows.column("message", "incoming"), createdRows.column("detail", "incoming"), createdRows.call("now") })
            .from("incoming", "incoming")
            .and_where(createdRows.column("matched", "incoming"))
            .and_where(createdRows.unary(ruvia::db_unary_operator::not_value, createdRows.exists(activeRecord)))
            .and_where(createdRows.unary(ruvia::db_unary_operator::not_value, createdRows.exists(recentRecord)));
        Query created(context.pool());
        using Record = persistence::OpenAlertRecordEntity;
        created
            .insert_into(Record::table_name(), { Record::column_name<"id">(), Record::column_name<"rule_id">(), Record::column_name<"device_id">(), Record::column_name<"severity">(), Record::column_name<"status">(), Record::column_name<"message">(), Record::column_name<"detail">(), Record::column_name<"triggered_at">() })
            .insert_from(createdRows)
            .on_conflict({ .do_nothing_ = true })
            .returning({ created.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"id">()), created.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"rule_id">()) });

        Query resolved(context.pool());
        const auto reverseReady = resolved.binary(
            resolved.binary(resolved.column("recovery_condition", "incoming"), Op::equal, resolved.value("reverse")),
            Op::and_value,
            resolved.binary(
                resolved.unary(ruvia::db_unary_operator::is_not_null, resolved.column("recovery_started_at", "states")),
                Op::and_value,
                resolved.binary(
                    resolved.column("recovery_started_at", "states"),
                    Op::less_equal,
                    resolved.binary(
                        resolved.call("now"),
                        Op::subtract,
                        resolved.binary(
                            resolved.column("recovery_wait_seconds", "incoming"),
                            Op::multiply,
                            resolved.cast(resolved.value("1 second"), Type::interval)
                        )
                    )
                )
            )
        );
        const auto autoSuffix = resolved.call(
            "substring",
            { resolved.column("recovery_condition", "incoming"),
              resolved.cast(resolved.value(6), Type::integer) }
        );
        const auto autoSeconds = resolved.cast(
            resolved.null_if(autoSuffix, resolved.cast(resolved.value(""), Type::text)),
            Type::integer
        );
        const auto autoWait = resolved.coalesce({ autoSeconds, resolved.cast(resolved.value(0), Type::integer) });
        const auto autoWindow = resolved.binary(
            resolved.greatest({ resolved.cast(resolved.value(0), Type::integer), autoWait }),
            Op::multiply,
            resolved.cast(resolved.value("1 second"), Type::interval)
        );
        const auto autoReady = resolved.binary(
            resolved.binary(resolved.column("recovery_condition", "incoming"), Op::like, resolved.value("auto_%")),
            Op::and_value,
            resolved.binary(
                resolved.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"triggered_at">(), "record"),
                Op::less_equal,
                resolved.binary(resolved.call("now"), Op::subtract, autoWindow)
            )
        );
        resolved
            .update(service::alert::persistence::OpenAlertRecordEntity::table_name(), "record")
            .set(service::alert::persistence::OpenAlertRecordEntity::column_name<"status">(), resolved.cast(resolved.value("resolved"), Type::text))
            .set(service::alert::persistence::OpenAlertRecordEntity::column_name<"resolved_at">(), resolved.call("now"))
            .set(service::alert::persistence::OpenAlertRecordEntity::column_name<"updated_at">(), resolved.call("now"))
            .update_from("incoming", "incoming")
            .join(ruvia::db_join_type::inner, "states", resolved.binary(resolved.column("rule_id", "states"), Op::equal, resolved.column("rule_id", "incoming")), "states")
            .and_where(resolved.unary(ruvia::db_unary_operator::not_value, resolved.column("matched", "incoming")))
            .and_where(resolved.binary(resolved.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"rule_id">(), "record"), Op::equal, resolved.column("rule_id", "incoming")))
            .and_where(resolved.binary(
                resolved.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"status">(), "record"),
                Op::in,
                resolved.list({ resolved.value("active"), resolved.value("acknowledged") })
            ))
            .and_where(resolved.binary(reverseReady, Op::or_value, autoReady))
            .returning({ resolved.column(service::alert::persistence::OpenAlertRecordEntity::column_name<"id">(), "record"), resolved.column("rule_id", "incoming") });

        Query changes(context.pool());
        changes
            .select({ changes.alias(changes.cast(changes.value("device.alert.triggered"), Type::text), "event_type"), changes.alias(changes.cast(changes.value("active"), Type::text), "status"), changes.alias(changes.column("id", "created"), "id"), changes.alias(changes.column("rule_id", "created"), "rule_id") })
            .from("created", "created");
        Query resolvedChanges(context.pool());
        resolvedChanges
            .select({ resolvedChanges.cast(resolvedChanges.value("device.alert.resolved"), Type::text), resolvedChanges.cast(resolvedChanges.value("resolved"), Type::text), resolvedChanges.column("id", "resolved"), resolvedChanges.column("rule_id", "resolved") })
            .from("resolved", "resolved");
        changes.combine(ruvia::db_set_operation::union_all, resolvedChanges);

        Query queuedRows(context.pool());
        const auto jsonKey = [&queuedRows](std::string_view key) {
            return queuedRows.cast(queuedRows.value(key), Type::text);
        };
        const auto alertObject = queuedRows.call(
            "jsonb_build_object",
            { jsonKey("id"), queuedRows.column("id", "changes"), jsonKey("ruleId"), queuedRows.column("rule_id", "incoming"), jsonKey("ruleName"), queuedRows.column("rule_name", "incoming"), jsonKey("severity"), queuedRows.column("severity", "incoming"), jsonKey("status"), queuedRows.column("status", "changes") }
        );
        const auto eventData = queuedRows.call(
            "jsonb_build_object",
            { jsonKey("alert"), alertObject, jsonKey("values"), queuedRows.column("detail", "incoming") }
        );
        queuedRows
            .select({ queuedRows.column("id", "changes"), queuedRows.column("event_type", "changes"), queuedRows.column("rule_id", "changes"), queuedRows.column("device_id", "incoming"), queuedRows.column("device_code", "incoming"), queuedRows.column("occurred_at_ms", "incoming"), eventData })
            .from("changes", "changes")
            .join(ruvia::db_join_type::inner, "incoming", queuedRows.binary(queuedRows.column("rule_id", "incoming"), Op::equal, queuedRows.column("rule_id", "changes")), "incoming");
        Query queued(context.pool());
        using Outbox = persistence::AlertEventOutboxEntity;
        queued
            .insert_into(Outbox::table_name(), { Outbox::column_name<"event_id">(), Outbox::column_name<"event_type">(), Outbox::column_name<"rule_id">(), Outbox::column_name<"device_id">(), Outbox::column_name<"device_code">(), Outbox::column_name<"occurred_at_ms">(), Outbox::column_name<"data">() })
            .insert_from(queuedRows)
            .on_conflict({ .columns_ = { std::string(Outbox::column_name<"event_id">()), std::string(Outbox::column_name<"event_type">()) }, .do_nothing_ = true })
            .returning({ queued.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_id">()), queued.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_type">()), queued.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"rule_id">()), queued.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"device_id">()), queued.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"device_code">()), queued.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"occurred_at_ms">()), queued.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"data">()), queued.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"created_at">()) });

        Query queuedBarrier(context.pool());
        queuedBarrier
            .select(queuedBarrier.alias(queuedBarrier.aggregate("count", { queuedBarrier.star() }), "queued_count"))
            .from("queued");
        Query receiptedRows(context.pool());
        receiptedRows
            .select({ receiptedRows.cast(receiptedRows.null_if(receiptedRows.cast(receiptedRows.column("receipt_id", "incoming"), Type::text), receiptedRows.cast(receiptedRows.value(""), Type::text)), Type::uuid), receiptedRows.column("device_id", "incoming") })
            .distinct()
            .from("incoming", "incoming")
            .join(ruvia::db_join_type::cross, queuedBarrier, {}, "queued_barrier")
            .and_where(receiptedRows.binary(
                receiptedRows.column("receipt_id", "incoming"),
                Op::not_equal,
                receiptedRows.cast(receiptedRows.value(""), Type::text)
            ))
            .and_where(receiptedRows.binary(
                receiptedRows.column("queued_count", "queued_barrier"),
                Op::greater_equal,
                receiptedRows.cast(receiptedRows.value(std::int64_t{ 0 }), Type::big_int)
            ));
        Query receipted(context.pool());
        using Receipt = persistence::AlertEvaluationReceiptEntity;
        receipted
            .insert_into(Receipt::table_name(), { Receipt::column_name<"message_id">(), Receipt::column_name<"device_id">() })
            .insert_from(receiptedRows)
            .on_conflict({ .columns_ = { std::string(Receipt::column_name<"message_id">()) }, .do_nothing_ = true })
            .returning({ receipted.column(service::alert::persistence::AlertEvaluationReceiptEntity::column_name<"message_id">()) });

        Query receiptedCount(context.pool());
        receiptedCount
            .select(receiptedCount.aggregate("count", { receiptedCount.star() }))
            .from("receipted");
        Query pruned(context.pool());
        pruned
            .delete_from(service::alert::persistence::AlertEvaluationReceiptEntity::table_name())
            .and_where(pruned.binary(
                pruned.column(service::alert::persistence::AlertEvaluationReceiptEntity::column_name<"created_at">()),
                Op::less,
                pruned.binary(pruned.call("now"), Op::subtract, pruned.cast(pruned.value("7 days"), Type::interval))
            ))
            .and_where(pruned.binary(pruned.subquery(receiptedCount), Op::greater_equal, pruned.cast(pruned.value(std::int64_t{ 0 }), Type::big_int)))
            .returning({ pruned.column(service::alert::persistence::AlertEvaluationReceiptEntity::column_name<"message_id">()) });

        Query relevant(context.pool());
        relevant
            .select(relevant.column("rule_id", "incoming"))
            .distinct()
            .from("incoming", "incoming");
        Query deliverable(context.pool());
        deliverable
            .select({ deliverable.column("event_id", "queued"), deliverable.column("event_type", "queued"), deliverable.column("rule_id", "queued"), deliverable.column("device_id", "queued"), deliverable.column("device_code", "queued"), deliverable.column("occurred_at_ms", "queued"), deliverable.column("data", "queued"), deliverable.column("created_at", "queued") })
            .from("queued", "queued");
        Query existingDeliverable(context.pool());
        existingDeliverable
            .select({ existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_id">(), "outbox"), existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_type">(), "outbox"), existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"rule_id">(), "outbox"), existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"device_id">(), "outbox"), existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"device_code">(), "outbox"), existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"occurred_at_ms">(), "outbox"), existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"data">(), "outbox"), existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"created_at">(), "outbox") })
            .from(service::alert::persistence::AlertEventOutboxEntity::table_name(), "outbox")
            .join(ruvia::db_join_type::inner, "relevant", existingDeliverable.binary(existingDeliverable.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"rule_id">(), "outbox"), Op::equal, existingDeliverable.column("rule_id", "relevant")), "relevant");
        deliverable.combine(ruvia::db_set_operation::union_all, existingDeliverable);

        Query pruneBarrier(context.pool());
        pruneBarrier
            .select(pruneBarrier.alias(pruneBarrier.aggregate("count", { pruneBarrier.star() }), "pruned_count"))
            .from("pruned");

        Query result(context.pool());
        result
            .with("incoming", incoming, { .columns_ = { "rule_id", "matched", "record_id", "device_id", "severity", "message", "detail", "silence_duration", "recovery_condition", "recovery_wait_seconds", "rule_name", "device_code", "occurred_at_ms", "receipt_id" } })
            .with("states", states)
            .with("created", created)
            .with("resolved", resolved)
            .with("changes", changes, { .materialization_ = ruvia::db_materialization::materialized })
            .with("queued", queued)
            .with("receipted", receipted)
            .with("pruned", pruned)
            .with("relevant", relevant)
            .with("deliverable", deliverable)
            .with("prune_barrier", pruneBarrier)
            .select({ result.cast(result.column("event_id", "deliverable"), Type::text), result.column("event_type", "deliverable"), result.cast(result.column("device_id", "deliverable"), Type::text), result.column("device_code", "deliverable"), result.cast(result.column("occurred_at_ms", "deliverable"), Type::text), result.cast(result.column("data", "deliverable"), Type::text) })
            .from("deliverable", "deliverable")
            .join(ruvia::db_join_type::cross, "prune_barrier", {}, "prune_barrier")
            .and_where(result.binary(result.column("pruned_count", "prune_barrier"), Op::greater_equal, result.cast(result.value(std::int64_t{ 0 }), Type::big_int)))
            .order_by(result.column("created_at", "deliverable"))
            .add_order_by(result.column("event_id", "deliverable"));
        const auto events = co_await context.db().query(result);
        co_await publishOutboxRows(context, events);
    }

    template <typename Rows>
    static ruvia::task<void> publishOutboxRows(
        ruvia::web_worker_context& context,
        const Rows& events
    ) {
        if (events.empty()) {
            co_return;
        }
        const auto scriptSha = co_await context.redis().script_load(service::access::persistence::kPublishEventScript);
        auto pipeline = context.redis().pipeline();
        for (const auto& event : events) {
            const auto occurredAt = service::utils::parseInt64(
                std::optional<std::string_view>(event[4].value().value_or(std::string_view{}))
            );
            if (!occurredAt) {
                throw std::runtime_error("invalid alert outbox timestamp");
            }
            service::access::event::queue(
                pipeline,
                context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                scriptSha,
                event[0].value().value_or(std::string_view{}),
                event[1].value().value_or(std::string_view{}),
                event[2].value().value_or(std::string_view{}),
                event[3].value().value_or(std::string_view{}),
                *occurredAt,
                event[5].value().value_or(std::string_view{}),
                context.worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId
            );
        }
        const auto replies = co_await std::move(pipeline).exec();
        service::utils::redis::requirePipelineSuccess("publish alert outbox", replies);

        Query remove(context.pool());
        std::vector<Query::expr_type> keys;
        keys.reserve(events.size());
        for (const auto& event : events) {
            keys.push_back(remove.tuple({ remove.cast(remove.value(event[0].value().value_or(std::string_view{})), Type::uuid), remove.cast(remove.value(event[1].value().value_or(std::string_view{})), Type::text) }));
        }
        remove.delete_from(service::alert::persistence::AlertEventOutboxEntity::table_name())
            .and_where(remove.binary(
                remove.tuple({ remove.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_id">()), remove.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_type">()) }),
                Op::in,
                remove.list(keys)
            ));
        (void)co_await context.db().execute(remove);
    }

    static ruvia::task<void> drainOutbox(ruvia::web_worker_context& context) {
        Query prune(context.pool());
        prune.delete_from(service::alert::persistence::AlertEvaluationReceiptEntity::table_name())
            .and_where(prune.binary(
                prune.column(service::alert::persistence::AlertEvaluationReceiptEntity::column_name<"created_at">()),
                Op::less,
                prune.binary(prune.call("now"), Op::subtract, prune.cast(prune.value("7 days"), Type::interval))
            ));
        (void)co_await context.db().execute(prune);
        while (true) {
            Query eventsQuery(context.pool());
            eventsQuery
                .select({ eventsQuery.cast(eventsQuery.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_id">()), Type::text), eventsQuery.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_type">()), eventsQuery.cast(eventsQuery.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"device_id">()), Type::text), eventsQuery.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"device_code">()), eventsQuery.cast(eventsQuery.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"occurred_at_ms">()), Type::text), eventsQuery.cast(eventsQuery.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"data">()), Type::text) })
                .from(service::alert::persistence::AlertEventOutboxEntity::table_name())
                .order_by(eventsQuery.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"created_at">()))
                .add_order_by(eventsQuery.column(service::alert::persistence::AlertEventOutboxEntity::column_name<"event_id">()))
                .limit(256);
            const auto events = co_await context.db().query(eventsQuery);
            if (events.empty()) {
                co_return;
            }
            co_await publishOutboxRows(context, events);
        }
    }

    static std::int64_t nowMilliseconds() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch()
        )
            .count();
    }

    static Query offlineEvaluationQuery(const std::set<std::string, std::less<>>& ruleIds) {
        Query requested;
        for (const auto& id : ruleIds) {
            requested.values({ requested.cast(requested.value(id), Type::uuid) });
        }
        Query rules;
        rules.select({ rules.alias(rules.cast(rules.value(0), Type::big_int), "input_sequence"), rules.star("rule"), rules.alias(rules.column(service::configuration::persistence::DeviceEntity::column_name<"name">(), "device"), "device_name"), rules.alias(rules.binary(rules.column(service::configuration::persistence::DeviceEntity::column_name<"protocol_params">(), "device"), Op::json_get_text, rules.value("device_code")), "device_code") })
            .from("requested")
            .join(ruvia::db_join_type::inner, service::alert::persistence::AlertRuleEntity::table_name(), rules.binary(rules.column(service::alert::persistence::AlertRuleEntity::column_name<"id">(), "rule"), Op::equal, rules.column("rule_id", "requested")), "rule")
            .join(ruvia::db_join_type::inner, service::configuration::persistence::DeviceEntity::table_name(), rules.binary(rules.column(service::configuration::persistence::DeviceEntity::column_name<"id">(), "device"), Op::equal, rules.column(service::alert::persistence::AlertRuleEntity::column_name<"device_id">(), "rule")));
        activeRules(rules);
        Query samples;
        samples.select({ samples.star("rules"), samples.column(service::alert::persistence::AlertInputStateEntity::column_name<"data">(), "state"), samples.alias(samples.call("to_timestamp", { samples.binary(samples.cast(samples.column(service::alert::persistence::AlertInputStateEntity::column_name<"observed_at_ms">(), "state"), Type::double_value), Op::divide, samples.value(1000.0)) }), "observed_at"), samples.column(service::alert::persistence::AlertInputStateEntity::column_name<"previous_data">(), "state") })
            .from("rules")
            .join(ruvia::db_join_type::left, service::alert::persistence::AlertInputStateEntity::table_name(), samples.binary(samples.column(service::alert::persistence::AlertInputStateEntity::column_name<"device_id">(), "state"), Op::equal, samples.column("device_id", "rules")), "state");
        Query query;
        query.with("requested", requested, { .columns_ = { "rule_id" } }).with("rules", rules).with("samples", samples);
        appendEvaluation(query);
        return query;
    }

    static Query telemetryEvaluationQuery(
        const std::vector<const service::message::ParsedDeviceMessage*>& messages,
        const std::vector<std::string_view>& previousData
    ) {
        Query input;
        for (std::size_t i = 0; i < messages.size(); ++i) {
            input.values({ input.cast(input.value(static_cast<std::int64_t>(i)), Type::big_int), input.cast(input.value(messages[i]->messageId), Type::uuid), input.cast(input.value(messages[i]->deviceId), Type::uuid), input.cast(input.value(messages[i]->valuesJson), Type::jsonb), input.cast(input.value(previousData[i]), Type::jsonb), input.cast(input.value(messages[i]->observedAtMs), Type::big_int) });
        }
        Query rules;
        rules.select({ rules.column("input_sequence", "input"), rules.star("rule"), rules.alias(rules.column(service::configuration::persistence::DeviceEntity::column_name<"name">(), "device"), "device_name"), rules.alias(rules.binary(rules.column(service::configuration::persistence::DeviceEntity::column_name<"protocol_params">(), "device"), Op::json_get_text, rules.value("device_code")), "device_code"), rules.alias(rules.column("data", "input"), "input_data"), rules.column("observed_at_ms", "input"), rules.column("previous_data", "input") })
            .from("input")
            .join(ruvia::db_join_type::inner, service::alert::persistence::AlertRuleEntity::table_name(), rules.binary(rules.column(service::alert::persistence::AlertRuleEntity::column_name<"device_id">(), "rule"), Op::equal, rules.column("device_id", "input")), "rule")
            .join(ruvia::db_join_type::inner, service::configuration::persistence::DeviceEntity::table_name(), rules.binary(rules.column(service::configuration::persistence::DeviceEntity::column_name<"id">(), "device"), Op::equal, rules.column(service::alert::persistence::AlertRuleEntity::column_name<"device_id">(), "rule")))
            .join(ruvia::db_join_type::left, service::alert::persistence::AlertEvaluationReceiptEntity::table_name(), rules.binary(rules.column(service::alert::persistence::AlertEvaluationReceiptEntity::column_name<"message_id">(), "receipt"), Op::equal, rules.column("message_id", "input")), "receipt")
            .and_where(rules.unary(ruvia::db_unary_operator::is_null, rules.column(service::alert::persistence::AlertEvaluationReceiptEntity::column_name<"message_id">(), "receipt")));
        activeRules(rules);
        Query samples;
        samples.select({ samples.star("rules"), samples.alias(samples.column("input_data", "rules"), "data"), samples.alias(samples.call("to_timestamp", { samples.binary(samples.cast(samples.column("observed_at_ms", "rules"), Type::double_value), Op::divide, samples.value(1000.0)) }), "observed_at") }).from("rules");
        Query query;
        query.with("input", input, { .columns_ = { "input_sequence", "message_id", "device_id", "data", "previous_data", "observed_at_ms" } }).with("rules", rules).with("samples", samples);
        appendEvaluation(query);
        return query;
    }

    static void activeRules(Query& query) {
        for (const auto table : { "rule", "device" }) {
            query.and_where(query.unary(ruvia::db_unary_operator::is_null, query.column("deleted_at", table)))
                .and_where(query.binary(query.column("status", table), Op::equal, query.value("enabled")));
        }
    }

    static Query::expr_type conditionText(Query& query, std::string_view key) {
        return query.binary(query.column("value", "condition"), Op::json_get_text, query.value(key));
    }

    static Query::expr_type sampleValue(Query& query, std::string_view column) {
        return query.binary(query.binary(query.binary(query.column(column, "samples"), Op::json_get, query.value("values")), Op::json_get, conditionText(query, "elementKey")), Op::json_get_text, query.value("value"));
    }

    static void appendEvaluation(Query& query) {
        Query numbers;
        const auto duration = conditionText(numbers, "duration");
        const auto threshold = conditionText(numbers, "value");
        const auto rate = conditionText(numbers, "changeRate");
        const auto bit = conditionText(numbers, "bitIndex");
        numbers.select({ numbers.alias(numbers.case_when({ { numbers.binary(duration, Op::regex, numbers.value("^[0-9]{1,10}$")), numbers.cast(numbers.least({ numbers.greatest({ numbers.cast(duration, Type::big_int), numbers.value(1) }), numbers.value(86400) }), Type::integer) } }), "duration_seconds"), numbers.alias(numbers.case_when({ { numbers.binary(numbers.binary(numbers.call("length", { numbers.coalesce({ threshold, numbers.value("") }) }), Op::less_equal, numbers.value(64)), Op::and_value, numbers.binary(threshold, Op::regex, numbers.value("^-?[0-9]+([.][0-9]+)?$"))), numbers.cast(threshold, Type::numeric) } }), "threshold_value"), numbers.alias(numbers.case_when({ { numbers.binary(numbers.unary(ruvia::db_unary_operator::is_null, rate), Op::or_value, numbers.binary(rate, Op::equal, numbers.value(""))), numbers.cast(numbers.value(0), Type::numeric) }, { numbers.binary(numbers.binary(numbers.call("length", { rate }), Op::less_equal, numbers.value(64)), Op::and_value, numbers.binary(rate, Op::regex, numbers.value("^[0-9]+([.][0-9]+)?$"))), numbers.cast(rate, Type::numeric) } }), "change_rate"), numbers.alias(numbers.case_when({ { numbers.binary(bit, Op::regex, numbers.value("^([0-9]|[1-5][0-9]|6[0-2])$")), numbers.cast(bit, Type::integer) } }), "bit_index") });
        Query current;
        const auto raw = sampleValue(current, "data");
        const auto hasBit = current.binary(current.column("value", "condition"), Op::json_has_key, current.value("bitIndex"));
        const auto validBit = current.binary(current.binary(hasBit, Op::and_value, current.unary(ruvia::db_unary_operator::is_not_null, current.column("bit_index", "condition_number"))), Op::and_value, current.binary(raw, Op::regex, current.value("^-?[0-9]+$")));
        current.select({ current.alias(current.case_when({ { validBit, current.cast(current.binary(current.call("int8shr", { current.cast(raw, Type::big_int), current.column("bit_index", "condition_number") }), Op::bit_and, current.cast(current.value(1), Type::big_int)), Type::text) }, { hasBit, current.null_value() } }, raw), "text_value"), current.alias(current.case_when({ { current.binary(raw, Op::regex, current.value("^-?[0-9]+([.][0-9]+)?$")), current.cast(raw, Type::numeric) } }), "numeric_value") });
        Query previous;
        const auto priorRaw = sampleValue(previous, "previous_data");
        previous.select(previous.alias(previous.case_when({ { previous.binary(priorRaw, Op::regex, previous.value("^-?[0-9]+([.][0-9]+)?$")), previous.cast(priorRaw, Type::numeric) } }), "numeric_value"));
        Query conditions;
        const auto numeric = conditions.column("numeric_value", "current_value");
        const auto priorNumeric = conditions.column("numeric_value", "previous_value");
        const auto expected = conditions.column("threshold_value", "condition_number");
        const auto changeRate = conditions.column("change_rate", "condition_number");
        std::vector<ruvia::db_case_branch> thresholdCases;
        for (const auto& [name, op] : std::array<std::pair<std::string_view, Op>, 4>{ { { ">", Op::greater }, { ">=", Op::greater_equal }, { "<", Op::less }, { "<=", Op::less_equal } } }) {
            thresholdCases.push_back({ conditions.binary(conditionText(conditions, "operator"), Op::equal, conditions.value(name)), conditions.coalesce({ conditions.binary(numeric, op, expected), conditions.value(false) }) });
        }
        for (const auto& [name, op] : std::array<std::pair<std::string_view, Op>, 2>{ { { "==", Op::equal }, { "!=", Op::not_equal } } }) {
            thresholdCases.push_back({ conditions.binary(conditionText(conditions, "operator"), Op::equal, conditions.value(name)), conditions.binary(conditions.column("text_value", "current_value"), op, conditionText(conditions, "value")) });
        }
        const auto delta = conditions.binary(conditions.call("abs", { conditions.binary(conditions.binary(conditions.binary(numeric, Op::subtract, priorNumeric), Op::divide, priorNumeric), Op::multiply, conditions.value(100)) }), Op::greater_equal, changeRate);
        const auto missingRate = conditions.binary(conditions.binary(conditions.unary(ruvia::db_unary_operator::is_null, priorNumeric), Op::or_value, conditions.binary(priorNumeric, Op::equal, conditions.value(0))), Op::or_value, conditions.binary(conditions.unary(ruvia::db_unary_operator::is_null, numeric), Op::or_value, conditions.unary(ruvia::db_unary_operator::is_null, changeRate)));
        const auto direction = conditions.coalesce({ conditionText(conditions, "changeDirection"), conditions.value("any") });
        const auto rateMatched = conditions.case_when({ { missingRate, conditions.value(false) }, { conditions.binary(direction, Op::equal, conditions.value("rise")), conditions.binary(conditions.binary(numeric, Op::greater, priorNumeric), Op::and_value, delta) }, { conditions.binary(direction, Op::equal, conditions.value("fall")), conditions.binary(conditions.binary(numeric, Op::less, priorNumeric), Op::and_value, delta) } }, delta);
        const auto offline = conditions.binary(conditions.unary(ruvia::db_unary_operator::is_null, conditions.column("observed_at", "samples")), Op::or_value, conditions.binary(conditions.column("observed_at", "samples"), Op::less, conditions.binary(conditions.call("now"), Op::subtract, conditions.binary(conditions.coalesce({ conditions.column("duration_seconds", "condition_number"), conditions.value(300) }), Op::multiply, conditions.cast(conditions.value("1 second"), Type::interval)))));
        const auto kind = conditionText(conditions, "type");
        conditions.select({ conditions.star("samples"), conditions.alias(conditions.column("value", "condition"), "condition"), conditions.alias(conditions.case_when({ { conditions.binary(kind, Op::equal, conditions.value("offline")), offline }, { conditions.binary(kind, Op::equal, conditions.value("threshold")), conditions.case_when(thresholdCases, conditions.value(false)) }, { conditions.binary(kind, Op::equal, conditions.value("rate_of_change")), rateMatched } }, conditions.value(false)), "condition_matched") })
            .from("samples")
            .join_function(ruvia::db_join_type::cross, conditions.call("jsonb_array_elements", { conditions.column("conditions", "samples") }), {}, "condition", { .lateral_ = true, .columns_ = { { .name_ = "value" } } })
            .join(ruvia::db_join_type::left, numbers, conditions.value(true), "condition_number", { .lateral_ = true })
            .join(ruvia::db_join_type::left, current, conditions.value(true), "current_value", { .lateral_ = true })
            .join(ruvia::db_join_type::left, previous, conditions.value(true), "previous_value", { .lateral_ = true });
        Query evaluated;
        for (const auto name : { "input_sequence", "id", "name", "severity", "device_id", "device_code", "silence_duration", "recovery_condition", "recovery_wait_seconds", "data" }) {
            evaluated.add_select(evaluated.column(name)).add_group_by(evaluated.column(name));
        }
        evaluated.add_select(evaluated.alias(evaluated.case_when({ { evaluated.binary(evaluated.column("logic"), Op::equal, evaluated.value("and")), evaluated.aggregate("bool_and", { evaluated.column("condition_matched") }) } }, evaluated.aggregate("bool_or", { evaluated.column("condition_matched") })), "matched"))
            .from("conditions")
            .add_group_by(evaluated.column("logic"));
        query.with("conditions", conditions).with("evaluated", evaluated).select({ query.column("input_sequence"), query.cast(query.column("id"), Type::text), query.column("name"), query.column("severity"), query.cast(query.column("device_id"), Type::text), query.coalesce({ query.column("device_code"), query.value("") }), query.column("matched"), query.column("silence_duration"), query.column("recovery_condition"), query.column("recovery_wait_seconds"), query.cast(query.coalesce({ query.column("data"), query.cast(query.value("{}"), Type::jsonb) }), Type::text) }).from("evaluated").add_order_by(query.column("input_sequence")).add_order_by(query.column("id"));
    }
};

} // namespace service::alert
