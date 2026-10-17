#pragma once

#include <algorithm>
#include <chrono>
#include <exception>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/features/edge/session/session.service.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"
#include "service/utils/uuid.h"

namespace service::live {

// UI notifications are ephemeral. Runtime configuration additionally keeps its
// durable relay entry so every instance can notify its Collector Workers.
inline constexpr std::string_view kPublishChangeScript = R"lua(
if ARGV[1] == 'runtime-config' then
  local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*',
             'schema_version', '2', 'topic', ARGV[1], 'ids', ARGV[2])
  redis.call('PUBLISH', KEYS[3], id)
end
return redis.call('PUBLISH', KEYS[1], ARGV[2])
)lua";

// Publishes only committed changes; subscribers query authorized persisted data.
template <typename Redis>
ruvia::task<void> publish(const Redis& redis, const service::message::RedisChannelScope& scope, std::string_view topic, std::span<const std::string> ids = {}) {
    const auto channel = service::message::live::channel(scope, topic);
    const auto joined = service::message::live::joinIds(ids);
    const auto inputChannel = service::message::consumer::inputChannel(scope, service::message::live::kChanges);
    const std::string_view keys[]{ channel, service::message::live::kChanges, inputChannel };
    const std::string_view args[]{ topic, joined, service::message::live::kChangesCapacity };
    const auto reply = co_await redis.eval(kPublishChangeScript, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        service::utils::redis::throwValue("publish live change", reply);
    }
}

// Claim and fanout are one transaction: losing the command reply cannot leave
// only some Collector Workers notified, and every append wakes its consumer.
inline constexpr std::string_view kRelayConfigurationScript = R"lua(
for key = 2, #KEYS, 2 do
  local kind = redis.call('TYPE', KEYS[key]).ok
  if kind ~= 'none' and kind ~= 'stream' then
    return redis.error_reply('collector configuration key is not a stream')
  end
end
if not redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then return 0 end
for key = 2, #KEYS, 2 do
  local argument = ((key - 2) / 2) * 2 + 3
  local id = redis.call('XADD', KEYS[key], 'MAXLEN', '~', '10000', '*',
    'message_id', ARGV[argument], 'worker_id', ARGV[argument + 1], 'created_at_ms', ARGV[2])
  redis.call('PUBLISH', KEYS[key + 1], id)
end
return 1
)lua";

template <typename Context>
ruvia::task<void> relayConfiguration(Context& context, std::string_view entryId, std::size_t collectorCount) {
    const auto& identity = context.template worker_state<std::optional<service::common::WorkerIdentity>>().value();
    std::vector<std::string> keys{ "iot:live:relay:" + identity.instanceId + ":" + std::string(entryId) };
    std::vector<std::string> args{ "60", std::to_string(service::message::utcNowMilliseconds()) };
    keys.reserve(1 + collectorCount * 2);
    args.reserve(2 + collectorCount * 2);
    for (std::size_t index = 0; index < collectorCount; ++index) {
        auto stream = service::message::configStream(index, identity.instanceId);
        keys.push_back(stream);
        keys.push_back(service::message::consumer::inputChannel(identity.channels, stream));
        args.push_back(service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()));
        args.push_back(std::to_string(index));
    }
    const std::vector<std::string_view> keyViews(keys.begin(), keys.end());
    const std::vector<std::string_view> argViews(args.begin(), args.end());
    const auto reply = co_await context.redis().eval(kRelayConfigurationScript, keyViews, argViews);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        service::utils::redis::throwValue("relay collector configuration", reply);
    }
}

// Session deadlines can only be inserted at now + 930s. After atomically
// expiring due sessions it is safe to sleep to the earliest remaining deadline,
// capped by that lease duration, without periodic scans.
template <typename Redis>
ruvia::task<std::chrono::milliseconds> expireDueSessions(const Redis& redis, const service::message::RedisChannelScope& scope) {
    constexpr std::chrono::milliseconds lifetime{ 930000 };
    co_await service::edge::session_state::expireSessions(redis, scope);
    const auto deadline = co_await service::edge::session_state::nextDeadline(redis);
    if (!deadline) {
        co_return lifetime;
    }
    const std::string_view args[]{ "TIME" };
    const auto clock = co_await redis.command(args);
    if (clock.kind() != ruvia::redis_value::kind_type::array || clock.array().size() != 2 ||
        clock.array()[0].kind() != ruvia::redis_value::kind_type::string ||
        clock.array()[1].kind() != ruvia::redis_value::kind_type::string) {
        service::utils::redis::throwValue("read Redis clock", clock);
    }
    const auto seconds = service::utils::parseInt64(std::optional<std::string_view>{ clock.array()[0].string() });
    const auto microseconds = service::utils::parseInt64(std::optional<std::string_view>{ clock.array()[1].string() });
    if (!seconds || !microseconds) {
        throw std::runtime_error("invalid Redis clock");
    }
    const auto now = *seconds * 1000 + *microseconds / 1000;
    co_return std::chrono::milliseconds(std::clamp<std::int64_t>(*deadline - now, 0, lifetime.count()));
}

} // namespace service::live
