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

// Session leases always expire now+930s: every deadline written by claim or
// refresh is later than every deadline already in kDeadlines, so a sleeper
// woken at nextDeadline() never misses an earlier one written meanwhile.
// KEYS: session key, kDeadlines, kChanges. ARGV: session value, node id.
inline constexpr std::string_view kClaimScript = R"lua(
local now=redis.call('TIME')
redis.call('SET',KEYS[1],ARGV[1],'EX',930)
redis.call('ZADD',KEYS[2],now[1]*1000+math.floor(now[2]/1000)+930000,KEYS[1])
redis.call('XADD',KEYS[3],'MAXLEN','~',100000,'*','schema_version','2','topic','edge','ids',ARGV[2])
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
redis.call('XADD',KEYS[3],'MAXLEN','~',100000,'*','schema_version','2','topic','edge','ids',ARGV[2])
return 1
)lua";
// Expiration is a state transition too, even when the owning API process died.
// Every instance may run this; the atomic ZREM claims each due session, so one
// edge change listing all expired node ids is published once.
// KEYS: kDeadlines, kChanges. ARGV: session key prefix.
inline constexpr std::string_view kExpireScript = R"lua(
local time=redis.call('TIME')
local now=time[1]*1000+math.floor(time[2]/1000)
local due=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',now,'LIMIT',0,256)
local expired={}
for _,key in ipairs(due) do
 local ttl=redis.call('PTTL',key)
 if ttl == -2 then
  if redis.call('ZREM',KEYS[1],key) == 1 then
   expired[#expired+1]=string.sub(key,#ARGV[1]+1)
  end
 elseif ttl >= 0 then
  redis.call('ZADD',KEYS[1],now+ttl+1,key)
 else
  redis.call('ZREM',KEYS[1],key)
 end
end
if #expired > 0 then
 redis.call('XADD',KEYS[2],'MAXLEN','~',100000,'*','schema_version','2','topic','edge','ids',table.concat(expired,','))
end
return #expired
)lua";
template <typename Redis>
ruvia::task<void> expireSessions(const Redis& redis) {
    const auto prefix = service::message::edge::sessionKey({});
    const std::string_view keys[]{kDeadlines, service::message::live::kChanges};
    const std::string_view args[]{prefix};
    const auto reply = co_await redis.eval(kExpireScript, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer)
        service::message::redis::throwValue("expire edge sessions", reply);
}

// Earliest session deadline (Redis clock, ms), or nullopt when none is armed.
template <typename Redis>
ruvia::task<std::optional<std::int64_t>> nextDeadline(const Redis& redis) {
    const std::vector<std::string> arguments{"ZRANGE", std::string(kDeadlines), "0", "0", "WITHSCORES"};
    const auto reply = co_await service::message::redis::command(redis, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::array)
        service::message::redis::throwValue("read next edge session deadline", reply);
    if (reply.array().empty())
        co_return std::nullopt;
    if (reply.array().size() != 2 || reply.array()[1].kind() != ruvia::redis_value::kind_type::string)
        service::message::redis::throwValue("parse next edge session deadline", reply);
    const auto text = reply.array()[1].string();
    std::int64_t score{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), score);
    if (error != std::errc{} || end != text.data() + text.size())
        throw std::runtime_error("invalid edge session deadline score");
    co_return score;
}

template <typename Redis>
ruvia::task<bool> mutate(const Redis& redis, std::string_view script,
    std::string_view nodeId, std::uint64_t epoch, std::uint32_t protocolVersion,
    std::size_t workerIndex) {
    const auto sessionKey=service::message::edge::sessionKey(nodeId);
    const auto expected=value(epoch,protocolVersion,workerIndex,service::runtime::instanceId());
    const std::string_view keys[]{sessionKey,kDeadlines,service::message::live::kChanges};
    const std::string_view args[]{expected,nodeId};
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
    const auto session = co_await redis.get(service::message::edge::sessionKey(nodeId));
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
        { "kind", std::string(service::message::edge::dispatch::kNodeKind) }, { "node_id", std::string(nodeId) }
    };
    (void)co_await service::message::redis::publish(
        redis,
        service::message::edge::dispatch::stream(owner->workerIndex, owner->instanceId),
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
    const auto key = service::message::edge::egressKey(nodeId);
    (void)co_await redis.rpush(key, wire);
    (void)co_await redis.ltrim(key, -100, -1);
    co_await notifyNode(redis, nodeId);
}

} // namespace service::edge::dispatch
