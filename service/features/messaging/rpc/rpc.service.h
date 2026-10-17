#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/redis/redis.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/features/messaging/rpc/rpc.entity.h"
#include "service/utils/redis.h"

namespace service::messaging::rpc {

// Persist the reliable result, notify its dedicated channel and acknowledge the
// durable request in one Redis transaction.
// KEYS: reply key, request stream, pending deadline key, deadline channel.
// ARGV: payload, reply lifetime (s), request group, request entry id.
inline constexpr std::string_view kReplyScript = R"lua(
local kind = redis.call('TYPE', KEYS[3]).ok
if kind ~= 'none' and kind ~= 'zset' then
  return redis.error_reply('pending deadline key is not a sorted set')
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('PUBLISH', KEYS[1], '')
redis.call('XACK', KEYS[2], ARGV[3], ARGV[4])
redis.call('XDEL', KEYS[2], ARGV[4])
if redis.call('ZREM', KEYS[3], ARGV[4]) > 0 then
  redis.call('PUBLISH', KEYS[4], ARGV[4])
end
return 1
)lua";

class RpcReceiptService final {
  public:
    static ruvia::task<std::optional<RpcReplyRecord>> beginRequest(const ruvia::redis_handle& redis, const std::string& id) {
        const auto previous = co_await redis.get(service::message::rpc::Contract::reply(id));
        if (previous) {
            co_return RpcReplyRecord{ std::string(*previous) };
        }
        const auto claimed = co_await redis.set(service::message::rpc::Contract::claim(id), "1",
            { .condition_ = ruvia::redis_set_condition::if_absent,
              .expiration_ = ruvia::redis_set_expiration::expires_after(std::chrono::seconds(86400)) });
        if (!claimed.applied()) {
            service::common::fail(10004, "Background operation was interrupted; inspect its state", 503);
        }
        if (co_await redis.exists(service::message::rpc::Contract::cancelled(id))) {
            service::common::fail(10004, "Background operation cancelled", 503);
        }
        co_return std::nullopt;
    }

    static ruvia::task<void> saveAndAcknowledge(const ruvia::redis_handle& redis, const std::string& id, std::string_view requestStream, std::string_view group, std::string_view messageId, const RpcReplyRecord& record) {
        const auto replyKey = service::message::rpc::Contract::reply(id);
        const auto pending = service::message::consumer::deadlineKey(requestStream, group);
        const auto changed = service::message::consumer::deadlineChannel(requestStream, group);
        const std::string_view keys[]{ replyKey, requestStream, pending, changed };
        const std::string_view args[]{
            record.payload,
            service::message::rpc::Contract::replyLifetime,
            group,
            messageId
        };
        const auto reply = co_await redis.eval(kReplyScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("RPC persist reply", reply);
        }
    }
};

} // namespace service::messaging::rpc
