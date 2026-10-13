#pragma once

#include <vector>

#include "service/common/message.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/telemetry/derived/derived.entity.h"

namespace service::telemetry::derived {

struct Result {
    std::optional<std::int64_t> nextDeadline;
    bool changed{};
    bool published{};
};

class DerivedService final {
  public:
    template <class Redis>
    static ruvia::task<Result> append(const Redis& redis, std::string_view configWire, message::ParsedDeviceMessage& message, bool expiration = false, bool deduplicate = false) {
        const auto config = ruvia::json_value::parse(configWire);
        if (!config) {
            co_return Result{};
        }
        const auto definitions = common::orderDerivedPoints(*config);
        if (definitions.empty()) {
            co_return Result{};
        }
        const Calculation calculation(definitions);
        const auto key = StateRecord::key(message.deviceId, configWire);
        std::map<std::string, Sample> incoming;
        const auto root = ruvia::json_value::parse(message.valuesJson);
        if (!root) {
            throw std::runtime_error("invalid sample JSON");
        }
        const auto values = root->get<ruvia::json_value>("values");
        if (!values) {
            throw std::runtime_error("sample has no values");
        }
        std::set<std::string> inputIds;
        for (const auto& definition : definitions) {
            for (const auto& [alias, id] : definition.inputs) {
                inputIds.insert(id);
            }
        }
        if (!expiration) {
            (void)values->for_each_field([&](std::string_view id, const ruvia::json_value& wire) {
                if (inputIds.contains(std::string(id))) {
                    incoming[std::string(id)] = StateRecord::sample(wire, message.observedAtMs);
                }
                return true;
            });
        }
        for (unsigned attempt = 0; attempt < 8; ++attempt) {
            const std::vector<std::string> previousArguments{ "GET", key };
            const auto previous = co_await message::redis::command(redis, previousArguments);
            if (previous.kind() != ruvia::redis_value::kind_type::null && previous.kind() != ruvia::redis_value::kind_type::string) {
                message::redis::throwValue("derived state read", previous);
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
            std::string json = "{";
            (void)root->for_each_field([&](std::string_view name, const ruvia::json_value& raw) {
                if (name != "values") {
                    json += utils::jsonQuoted(name) + ':' + std::string(raw.view()) + ',';
                }
                return true;
            });
            json += "\"values\":{";
            bool first = true;
            for (const auto& [id, value] : fields) {
                if (!first) {
                    json += ',';
                }
                first = false;
                json += utils::jsonQuoted(id) + ':' + value;
            }
            auto candidate = message;
            candidate.valuesJson = json + "}}";
            // State and publication commit together. A retry never counts the
            // sample twice; downstream receipts deduplicate its immutable ID.
            static constexpr std::string_view cas = R"lua(
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
            const auto stream = message::parsedStream();
            const auto wake = message::workerWakeStream(std::nullopt, service::runtime::instanceId());
            const auto receipt = "iot:derived:receipt:" + message.messageId;
            std::vector<std::string_view> keys{ key, stream, wake };
            if (deduplicate) keys.push_back(receipt);
            std::vector<std::string> store{ before, after, !expiration || !outputs.empty() ? "1" : "0" };
            for (const auto& field : message::parsedFields(candidate)) {
                store.push_back(field.name);
                store.push_back(field.value);
            }
            const std::vector<std::string_view> args(store.begin(), store.end());
            const auto reply = co_await redis.eval(cas, keys, args);
            if (reply.kind() != ruvia::redis_value::kind_type::integer) {
                message::redis::throwValue("derived state CAS", reply);
            }
            if (reply.integer() == 0) {
                continue;
            }
            if (reply.integer() == 2)
                co_return Result{calculation.nextDeadline(StateRecord::decode(before), message.occurredAtMs), false, true};
            message = std::move(candidate);
            co_return Result{ calculation.nextDeadline(state, message.occurredAtMs), !outputs.empty(), true };
        }
        throw std::runtime_error("derived state contention");
    }
};
} // namespace service::telemetry::derived
