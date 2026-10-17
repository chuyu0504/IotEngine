#pragma once

#include <map>
#include <optional>
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
inline constexpr std::string_view kStateTransitionScript = R"lua(
local current=redis.call('GET',KEYS[1]) or ''
if #KEYS>3 and redis.call('EXISTS',KEYS[4])==1 then return 2 end
if current~=ARGV[1] then return 0 end
for i=2,3 do
 local kind=redis.call('TYPE',KEYS[i]).ok
 if kind~='none' and kind~='stream' then return redis.error_reply('derived output must be a stream') end
end
if ARGV[3]=='1' then
 local fields={'*'}
 for i=4,#ARGV do fields[#fields+1]=ARGV[i] end
 redis.call('XADD',KEYS[2],unpack(fields))
 redis.call('XADD',KEYS[3],'MAXLEN','~','100000','*','task','telemetry')
end
redis.call('SET',KEYS[1],ARGV[2],'EX',172800)
if #KEYS>3 then redis.call('SET',KEYS[4],'1','EX',604800) end
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
    [[nodiscard]] static std::optional<DeviceCalculation> prepare(std::string_view deviceId, std::string_view configuration) {
        const auto parsed = ruvia::json_value::parse(configuration);
        if (!parsed) {
            return std::nullopt;
        }
        auto definitions = common::orderDerivedPoints(*parsed);
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
            .stateKey = StateRecord::key(deviceId, configuration),
            .calculation = Calculation(std::move(definitions)),
            .inputIds = std::move(inputIds),
        };
    }

    template <class Redis>
    static ruvia::task<Result> append(const Redis& redis, const DeviceCalculation& device, message::ParsedDeviceMessage& message, std::string_view instanceId, bool expiration = false, bool deduplicate = false) {
        const auto& calculation = device.calculation;
        const auto& definitions = calculation.definitions();
        std::map<std::string, Sample> incoming;
        const auto root = ruvia::json_value::parse(message.valuesJson);
        if (!root) {
            throw std::runtime_error("invalid sample JSON");
        }
        const auto values = root->get<ruvia::json_value>("values");
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
        std::vector<std::string_view> keys{ device.stateKey, stream, wake };
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
            (void)root->for_each_field([&](std::string_view name, const ruvia::json_value& raw) {
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
            if (reply.integer() == 2)
                co_return Result{calculation.nextDeadline(StateRecord::decode(before), message.occurredAtMs), false, true};
            message.valuesJson = std::move(valuesJson);
            co_return Result{ calculation.nextDeadline(state, message.occurredAtMs), !outputs.empty(), true };
        }
        throw std::runtime_error("derived state contention");
    }
};
} // namespace service::telemetry::derived
