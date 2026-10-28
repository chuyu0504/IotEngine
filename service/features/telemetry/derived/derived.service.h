#pragma once

#include <map>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/redis/redis_handle.h>

#include "service/common/message.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/telemetry/derived/derived.entity.h"
#include "service/features/telemetry/derived/derived.types.h"

namespace service::telemetry::derived {

// 状态与派生样本同一事务提交；重试不会重复计入样本，下游按不可变消息 ID 去重。
// KEYS: 状态、样本流、唤醒流、样本输入频道、唤醒输入频道，以及可选复制回执。
inline constexpr std::string_view kStateTransitionScript = R"lua(
local current=redis.call('GET',KEYS[1]) or ''
if #KEYS>5 and redis.call('EXISTS',KEYS[6])==1 then return 2 end
if current~=ARGV[1] then return 0 end
for i=2,3 do
 local kind=redis.call('TYPE',KEYS[i]).ok
 if kind~='none' and kind~='stream' then return redis.error_reply('derived output must be a stream') end
end
if ARGV[3]=='1' then
 local fields={'*'}
 for i=4,#ARGV do fields[#fields+1]=ARGV[i] end
 local id=redis.call('XADD',KEYS[2],unpack(fields))
 redis.call('PUBLISH',KEYS[4],id)
 local wake=redis.call('XADD',KEYS[3],'MAXLEN','~','100000','*','task','telemetry')
 redis.call('PUBLISH',KEYS[5],wake)
end
redis.call('SET',KEYS[1],ARGV[2],'EX',172800)
if #KEYS>5 then redis.call('SET',KEYS[6],'1','EX',604800) end
return 1
)lua";

// 跨来源的原始采样和设备派生结果使用各自的时间及身份，但必须与状态原子提交。
inline constexpr std::string_view kMultiSourceTransitionScript = R"lua(
local current=redis.call('GET',KEYS[1]) or ''
if #KEYS>5 and redis.call('EXISTS',KEYS[6])==1 then return 2 end
if current~=ARGV[1] then return 0 end
for i=2,3 do
 local kind=redis.call('TYPE',KEYS[i]).ok
 if kind~='none' and kind~='stream' then return redis.error_reply('derived output must be a stream') end
end
local cursor=4
local published=false
for n=1,tonumber(ARGV[3]) do
 local size=tonumber(ARGV[cursor]); cursor=cursor+1
 local fields={'*'}
 for i=1,size do fields[#fields+1]=ARGV[cursor]; cursor=cursor+1 end
 local id=redis.call('XADD',KEYS[2],unpack(fields))
 redis.call('PUBLISH',KEYS[4],id)
 published=true
end
if published then
 local wake=redis.call('XADD',KEYS[3],'MAXLEN','~','100000','*','task','telemetry')
 redis.call('PUBLISH',KEYS[5],wake)
end
redis.call('SET',KEYS[1],ARGV[2],'EX',172800)
if #KEYS>5 then redis.call('SET',KEYS[6],'1','EX',604800) end
return 1
)lua";

struct Result {
    std::optional<std::int64_t> nextDeadline;
    bool changed{};
    bool published{};
};

class DerivedService final {
  public:
    // 原文不是 JSON 或没有派生点时返回空；派生点定义错误抛出 std::invalid_argument。
    [[nodiscard]] static std::optional<DeviceCalculation> prepare(std::string_view deviceId, std::string_view configuration, std::span<const std::string_view> sources = {}, std::string_view snapshotDigest = {}) {
        std::optional<ruvia::json_value> parsed;
        try {
            parsed = ruvia::json_value::parse(configuration);
        } catch (const std::invalid_argument&) {
            return std::nullopt;
        }

        std::vector<ruvia::json_value> sourceValues;
        sourceValues.reserve(sources.size());
        for (const auto source : sources) {
            auto value = ruvia::json_value::parse(source);
            if (!value.is_object()) {
                throw std::invalid_argument("derived source configuration must be an object");
            }
            sourceValues.push_back(std::move(value));
        }
        auto definitions = common::orderDerivedPoints(*parsed, sourceValues);
        if (definitions.empty()) {
            return std::nullopt;
        }
        std::set<std::string, std::less<>> inputIds;
        for (const auto& definition : definitions) {
            for (const auto& [alias, id] : definition.inputs) {
                inputIds.insert(id);
            }
        }
        return DeviceCalculation{
            .configuration = std::string(configuration),
            .stateKey = StateRecord::key(deviceId, configuration, sources, snapshotDigest),
            .calculation = Calculation(std::move(definitions)),
            .inputIds = std::move(inputIds),
            .multipleSources = !sources.empty() || !snapshotDigest.empty(),
        };
    }

    template <class Redis>
    static ruvia::task<Result> append(const Redis& redis, const service::message::RedisChannelScope& scope, const DeviceCalculation& device, message::ParsedDeviceMessage& message, std::string_view instanceId, bool expiration = false, bool deduplicate = false) {
        if (device.multipleSources) {
            co_return co_await appendMultipleSources(redis, scope, device, message, instanceId, expiration, deduplicate);
        }
        const auto& calculation = device.calculation;
        const auto& definitions = calculation.definitions();
        std::map<std::string, Sample> incoming;
        const auto root = ruvia::json_value::parse(message.valuesJson);

        const auto values = root.get<ruvia::json_value>("values");
        if (!values) {
            throw std::runtime_error("sample has no values");
        }
        if (!expiration) {
            (void)values->for_each_field([&](std::string_view id, const ruvia::json_value& wire) {
                if (device.inputIds.contains(id)) {
                    // 输入与本次计算使用同一设备事件时间，避免传输延迟使输入落在计算时间之后。
                    incoming[std::string(id)] = StateRecord::sample(wire, message.occurredAtMs);
                }
                return true;
            });
        }
        const auto stream = message::parsedStream();
        const auto wake = message::workerWakeStream(std::nullopt, instanceId);
        const auto receipt = "iot:derived:receipt:" + message.messageId;
        const auto inputChannel = message::consumer::inputChannel(scope, stream);
        const auto wakeChannel = message::consumer::inputChannel(scope, wake);
        std::vector<std::string_view> keys{ device.stateKey, stream, wake, inputChannel, wakeChannel };
        if (deduplicate) {
            keys.push_back(receipt);
        }
        for (unsigned attempt = 0; attempt < 8; ++attempt) {
            const std::vector<std::string> previousArguments{ "GET", device.stateKey };
            const auto previous = co_await redis.command(std::vector<std::string_view>(previousArguments.begin(), previousArguments.end()));
            if (previous.kind() != ruvia::redis_value::kind_type::null && previous.kind() != ruvia::redis_value::kind_type::string) {
                utils::redis::throwValue("derived state read", previous);
            }
            const std::string before = previous.kind() == ruvia::redis_value::kind_type::string ? std::string(previous.string()) : std::string{};
            auto state = StateRecord::decode(before);
            const auto outputs = calculation.advance(state, incoming, message.occurredAtMs);
            const auto after = StateRecord::encode(state);
            std::map<std::string, std::string> fields;
            if (!expiration) {
                (void)values->for_each_field([&](std::string_view id, const ruvia::json_value& raw) {
                    fields[std::string(id)] = std::string(raw.view());
                    return true;
                });
            }
            for (const auto& definition : definitions) {
                const auto value = state.latest.find(definition.id);
                if (value != state.latest.end() && (!expiration || outputs.contains(definition.id))) {
                    fields[definition.id] = Calculation::pointJson(definition, value->second);
                }
            }
            std::string valuesJson = "{";
            (void)root.for_each_field([&](std::string_view name, const ruvia::json_value& raw) {
                if (name != "values") {
                    valuesJson += utils::jsonQuoted(name) + ':' + std::string(raw.view()) + ',';
                }
                return true;
            });
            valuesJson += "\"values\":{";
            bool first = true;
            for (const auto& [id, value] : fields) {
                if (!first) {
                    valuesJson += ',';
                }
                first = false;
                valuesJson += utils::jsonQuoted(id) + ':' + value;
            }
            valuesJson += "}}";
            std::vector<std::string> store{ before, after, !expiration || !outputs.empty() ? "1" : "0" };
            // 只替换值字段编码派生样本，不复制原始报文。
            std::swap(message.valuesJson, valuesJson);
            auto publication = message::parsedFields(message);
            std::swap(message.valuesJson, valuesJson);
            store.reserve(store.size() + publication.size() * 2);
            for (auto& field : publication) {
                store.push_back(std::move(field.name));
                store.push_back(std::move(field.value));
            }
            const std::vector<std::string_view> args(store.begin(), store.end());
            const auto reply = co_await redis.eval(kStateTransitionScript, keys, args);
            if (reply.kind() != ruvia::redis_value::kind_type::integer) {
                utils::redis::throwValue("derived state CAS", reply);
            }
            if (reply.integer() == 0) {
                continue;
            }
            if (reply.integer() == 2) {
                co_return Result{ calculation.nextDeadline(StateRecord::decode(before), message.occurredAtMs), false, true };
            }
            message.valuesJson = std::move(valuesJson);
            co_return Result{ calculation.nextDeadline(state, message.occurredAtMs), !outputs.empty(), true };
        }
        throw std::runtime_error("derived state contention");
    }

  public:
    static std::string derivedMessageId(std::string_view input, std::string_view stateKey) {
        const auto hash = utils::sha256(std::string(input) + ':' + std::string(stateKey));
        return hash.substr(0, 8) + '-' + hash.substr(8, 4) + "-5" + hash.substr(13, 3) + "-8" + hash.substr(17, 3) + '-' + hash.substr(20, 12);
    }

    static std::map<std::string, Sample> inputSamples(const DeviceCalculation& device, const message::ParsedDeviceMessage& message, bool expiration = false) {
        const auto root = ruvia::json_value::parse(message.valuesJson);
        const auto values = root.get<ruvia::json_value>("values");
        if (!values || !values->is_object()) {
            throw std::runtime_error("sample has no values");
        }
        std::map<std::string, Sample> incoming;
        if (!expiration) {
            const auto at = message::effectiveObservedAt(message.observedAtMs, message.occurredAtMs);
            (void)values->for_each_field([&](std::string_view id, const ruvia::json_value& wire) {
                if (device.inputIds.contains(id)) {
                    auto sample = StateRecord::sample(wire, at);
                    sample.at = at;
                    incoming.emplace(id, std::move(sample));
                }
                return true;
            });
        }
        return incoming;
    }

    static message::ParsedDeviceMessage derivedMessage(const DeviceCalculation& device, const message::ParsedDeviceMessage& input, const std::map<std::string, Sample>& outputs, std::int64_t at) {
        auto result = input;
        result.messageId = derivedMessageId(input.messageId, device.stateKey);
        result.acquisitionId = result.messageId;
        result.causationId = input.messageId;
        result.modelId.clear();
        result.source = "derived";
        result.observedAtMs = at;
        result.occurredAtMs = (std::max)(input.occurredAtMs, at);
        result.rawPayloads.clear();
        result.rawPacketIds.clear();
        result.valuesJson = "{\"values\":{";
        bool first = true;
        for (const auto& definition : device.calculation.definitions()) {
            const auto output = outputs.find(definition.id);
            if (output == outputs.end()) {
                continue;
            }
            if (!first) {
                result.valuesJson += ',';
            }
            first = false;
            result.valuesJson += utils::jsonQuoted(definition.id) + ':' + Calculation::pointJson(definition, output->second);
        }
        result.valuesJson += "}}";
        return result;
    }

    // Immutable journal inputs are ordered by event time and record ID by the caller.
    // Rebuild from an empty state, including deadlines between observations, so an
    // out-of-order input repairs all later affected expressions and windows.
    static std::vector<message::ParsedDeviceMessage> replayHistory(const DeviceCalculation& device, const std::vector<message::ParsedDeviceMessage>& inputs, State* persisted = nullptr, const message::ParsedDeviceMessage* preceding = nullptr) {
        State fresh;
        auto& state = persisted ? *persisted : fresh;
        std::vector<message::ParsedDeviceMessage> result;
        const auto precedingTime = preceding ? message::effectiveObservedAt(preceding->observedAtMs, preceding->occurredAtMs) : 0;
        std::optional<std::int64_t> deadline = preceding ? device.calculation.nextDeadline(state, precedingTime) : std::nullopt;
        const message::ParsedDeviceMessage* previous = preceding;
        for (const auto& input : inputs) {
            const auto at = message::effectiveObservedAt(input.observedAtMs, input.occurredAtMs);
            while (previous && deadline && *deadline < at) {
                const auto expiredAt = *deadline;
                const auto outputs = device.calculation.advance(state, {}, expiredAt);
                if (!outputs.empty()) {
                    auto cause = *previous;
                    cause.messageId = derivedMessageId(previous->messageId, std::to_string(expiredAt));
                    auto output = derivedMessage(device, cause, outputs, expiredAt);
                    output.historyOnly = true;
                    result.push_back(std::move(output));
                }
                deadline = device.calculation.nextDeadline(state, expiredAt);
            }
            const auto outputs = device.calculation.advance(state, inputSamples(device, input), at);
            if (!outputs.empty()) {
                auto output = derivedMessage(device, input, outputs, at);
                output.historyOnly = true;
                result.push_back(std::move(output));
            }
            deadline = device.calculation.nextDeadline(state, at);
            previous = &input;
        }
        return result;
    }

  private:
    template <class Redis>
    static ruvia::task<Result> appendMultipleSources(const Redis& redis, const service::message::RedisChannelScope& scope, const DeviceCalculation& device, message::ParsedDeviceMessage& message, std::string_view instanceId, bool expiration, bool deduplicate) {
        const auto incoming = inputSamples(device, message, expiration);
        const auto stream = message::parsedStream();
        const auto wake = message::workerWakeStream(std::nullopt, instanceId);
        const auto inputChannel = message::consumer::inputChannel(scope, stream);
        const auto wakeChannel = message::consumer::inputChannel(scope, wake);
        const auto receipt = "iot:derived:receipt:" + message.messageId;
        std::vector<std::string_view> keys{ device.stateKey, stream, wake, inputChannel, wakeChannel };
        if (deduplicate) {
            keys.push_back(receipt);
        }
        for (unsigned attempt = 0; attempt < 8; ++attempt) {
            const std::vector<std::string_view> get{ "GET", device.stateKey };
            const auto previous = co_await redis.command(get);
            if (previous.kind() != ruvia::redis_value::kind_type::null && previous.kind() != ruvia::redis_value::kind_type::string) {
                utils::redis::throwValue("derived state read", previous);
            }
            const std::string before = previous.kind() == ruvia::redis_value::kind_type::string ? std::string(previous.string()) : std::string{};
            auto state = StateRecord::decode(before);
            auto evaluationTime = message::effectiveObservedAt(message.observedAtMs, message.occurredAtMs);
            for (const auto& definition : device.calculation.definitions()) {
                if (const auto previousValue = state.latest.find(definition.id); previousValue != state.latest.end()) {
                    evaluationTime = (std::max)(evaluationTime, previousValue->second.at);
                }
            }
            const auto outputs = device.calculation.advance(state, incoming, evaluationTime);
            std::vector<std::vector<message::StreamField>> publications;
            if (!expiration) {
                publications.push_back(message::parsedFields(message));
            }
            if (!outputs.empty()) {
                const auto derived = derivedMessage(device, message, outputs, evaluationTime);
                publications.push_back(message::parsedFields(derived));
            }
            std::vector<std::string> store{ before, StateRecord::encode(state), std::to_string(publications.size()) };
            for (auto& publication : publications) {
                store.push_back(std::to_string(publication.size() * 2));
                for (auto& field : publication) {
                    store.push_back(std::move(field.name));
                    store.push_back(std::move(field.value));
                }
            }
            const std::vector<std::string_view> args(store.begin(), store.end());
            const auto reply = co_await redis.eval(kMultiSourceTransitionScript, keys, args);
            if (reply.kind() != ruvia::redis_value::kind_type::integer) {
                utils::redis::throwValue("derived state CAS", reply);
            }
            if (reply.integer() == 0) {
                continue;
            }
            if (reply.integer() == 2) {
                co_return Result{ device.calculation.nextDeadline(StateRecord::decode(before), evaluationTime), false, true };
            }
            co_return Result{ device.calculation.nextDeadline(state, evaluationTime), !outputs.empty(), true };
        }
        throw std::runtime_error("derived state contention");
    }
};
} // namespace service::telemetry::derived
