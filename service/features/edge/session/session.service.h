#pragma once

#include "service/features/edge/session/session.entity.h"

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/redis/redis_types.h>

#include "service/features/messaging/messaging.transport.h"
#include "service/common/uuid.h"

namespace service::edge::session_state {

inline constexpr std::string_view kChanges = "iot:live:changes";
inline constexpr std::string_view kClaimScript = R"lua(
local now=redis.call('TIME')
redis.call('SET',KEYS[1],ARGV[1],'EX',930)
redis.call('ZADD',KEYS[2],now[1]*1000+math.floor(now[2]/1000)+930000,KEYS[1])
redis.call('XADD',KEYS[3],'MAXLEN','~',100000,'*','topic','edge')
return 1
)lua";
inline constexpr std::string_view kRefreshScript = R"lua(
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end
local now=redis.call('TIME')
redis.call('EXPIRE',KEYS[1],930)
redis.call('ZADD',KEYS[2],now[1]*1000+math.floor(now[2]/1000)+930000,KEYS[1])
return 1
)lua";
inline constexpr std::string_view kReleaseScript = R"lua(
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL',KEYS[1])
redis.call('ZREM',KEYS[2],KEYS[1])
redis.call('XADD',KEYS[3],'MAXLEN','~',100000,'*','topic','edge')
return 1
)lua";
// Expiration is a state transition too, even when the owning API process died.
// All instances may service this deadline queue; the atomic removal emits once.
inline constexpr std::string_view kExpireScript = R"lua(
local time=redis.call('TIME')
local now=time[1]*1000+math.floor(time[2]/1000)
local due=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',now,'LIMIT',0,256)
local changed=0
for _,key in ipairs(due) do
 local ttl=redis.call('PTTL',key)
 if ttl == -2 then
  redis.call('ZREM',KEYS[1],key)
  changed=changed+1
 elseif ttl >= 0 then
  redis.call('ZADD',KEYS[1],now+ttl+1,key)
 else
  redis.call('ZREM',KEYS[1],key)
 end
end
if changed > 0 then redis.call('XADD',KEYS[2],'MAXLEN','~',100000,'*','topic','edge') end
return changed
)lua";
template <typename Redis>
ruvia::task<void> expireSessions(const Redis& redis) {
    const std::string_view keys[]{kDeadlines, service::message::live::kChanges};
    (void)co_await redis.eval(kExpireScript, keys, std::span<const std::string_view>{});
}

template <typename Redis>
ruvia::task<bool> mutate(const Redis& redis, std::string_view script,
    std::string_view nodeId, std::uint64_t epoch, std::uint32_t protocolVersion,
    std::size_t workerIndex) {
    const auto sessionKey=key(nodeId);
    const auto expected=value(epoch,protocolVersion,workerIndex,service::runtime::instanceId());
    const std::string_view keys[]{sessionKey,kDeadlines,kChanges};
    const std::string_view args[]{expected};
    const auto reply=co_await redis.eval(script,keys,args);
    if(reply.kind()!=ruvia::redis_value::kind_type::integer)
        service::message::redis::throwValue("edge session transition",reply);
    co_return reply.integer()==1;
}
template <typename Redis>
ruvia::task<bool> claim(const Redis& redis, std::string_view nodeId, std::uint64_t epoch,
                        std::uint32_t protocolVersion, std::size_t workerIndex) {
    co_return co_await mutate(redis,kClaimScript,nodeId,epoch,protocolVersion,workerIndex);
}
template <typename Redis>
ruvia::task<bool> refresh(const Redis& redis, std::string_view nodeId, std::uint64_t epoch,
                          std::uint32_t protocolVersion, std::size_t workerIndex) {
    co_return co_await mutate(redis,kRefreshScript,nodeId,epoch,protocolVersion,workerIndex);
}
template <typename Redis>
ruvia::task<bool> release(const Redis& redis, std::string_view nodeId, std::uint64_t epoch,
                          std::uint32_t protocolVersion, std::size_t workerIndex) {
    co_return co_await mutate(redis,kReleaseScript,nodeId,epoch,protocolVersion,workerIndex);
}

} // namespace service::edge::session_state

namespace service::edge::dispatch {

template <typename Redis>
ruvia::task<void> notifyNode(const Redis& redis, std::string_view nodeId) {
    if (nodeId.empty()) {
        co_return;
    }
    const auto session = co_await redis.get(session_state::key(nodeId));
    if (!session) {
        co_return;
    }
    const auto owner = session_state::parse(
        std::string_view(session->data(), session->size())
    );
    if (!owner) {
        co_return;
    }
    const std::vector<service::message::StreamField> fields{
        { "kind", std::string(kNodeKind) }, { "node_id", std::string(nodeId) }
    };
    (void)co_await service::message::redis::publish(
        redis,
        stream(owner->workerIndex, owner->instanceId),
        fields,
        10000
    );
    const std::vector<service::message::StreamField> wakeFields{{ "task", "edge-dispatcher" }};
    (void)co_await service::message::redis::publish(redis, service::message::workerWakeStream(owner->workerIndex, owner->instanceId), wakeFields, 10000);
}

template <typename Redis>
ruvia::task<void> enqueue(const Redis& redis, std::string_view nodeId, std::string_view wire) {
    if (nodeId.empty() || wire.empty()) {
        co_return;
    }
    const auto key = "iot:edge:egress:" + std::string(nodeId);
    (void)co_await redis.rpush(key, wire);
    (void)co_await redis.ltrim(key, -100, -1);
    co_await notifyNode(redis, nodeId);
}

} // namespace service::edge::dispatch
