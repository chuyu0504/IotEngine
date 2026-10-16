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

// Stores the reply, notifies the requesting worker on its directed stream and
// removes the request, atomically.
// KEYS: reply key, directed worker stream, request stream.
// ARGV: payload, reply lifetime (s), request group, request entry id,
//       directed stream capacity, directed stream TTL (ms).
inline constexpr std::string_view kReplyScript = R"lua(
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[5], '*', 'topic', KEYS[1])
redis.call('PEXPIRE', KEYS[2], ARGV[6])
redis.call('XACK', KEYS[3], ARGV[3], ARGV[4])
redis.call('XDEL', KEYS[3], ARGV[4])
return 1
)lua";

class RpcReceiptService final {
  public:
    static ruvia::task<std::optional<RpcReplyRecord>> beginRequest(const ruvia::redis_handle& redis, const std::string& id) {
        const std::vector<std::string> previousArguments{ "GET", service::message::rpc::Contract::reply(id) };
        const auto previous = co_await service::message::redis::command(redis, previousArguments);
        if (previous.kind() == ruvia::redis_value::kind_type::string) {
            co_return RpcReplyRecord{ std::string(previous.string()) };
        }
        if (previous.kind() == ruvia::redis_value::kind_type::error) {
            service::message::redis::throwValue("RPC receipt", previous);
        }
        const std::vector<std::string> claimedArguments{ "SET", service::message::rpc::Contract::claim(id), "1", "NX", "EX", "86400" };
        const auto claimed = co_await service::message::redis::command(redis, claimedArguments);
        if (claimed.kind() == ruvia::redis_value::kind_type::error) {
            service::message::redis::throwValue("RPC claim", claimed);
        }
        if (claimed.kind() != ruvia::redis_value::kind_type::string) {
            service::common::fail(10004, "Background operation was interrupted; inspect its state", 503);
        }
        const std::vector<std::string> cancelledArguments{ "EXISTS", service::message::rpc::Contract::cancelled(id) };
        const auto cancelled = co_await service::message::redis::command(redis, cancelledArguments);
        if (cancelled.kind() == ruvia::redis_value::kind_type::error) {
            service::message::redis::throwValue("RPC cancellation", cancelled);
        }
        if (cancelled.kind() == ruvia::redis_value::kind_type::integer && cancelled.integer() != 0) {
            service::common::fail(10004, "Background operation cancelled", 503);
        }
        co_return std::nullopt;
    }

    // `replyStream` is the directed stream of the worker consuming the request
    // stream; the requester's waiter is routed to it by the reply topic.
    static ruvia::task<void> saveAndAcknowledge(const ruvia::redis_handle& redis, const std::string& id, std::string_view replyStream, std::string_view requestStream, std::string_view group, std::string_view messageId, const RpcReplyRecord& record) {
        const auto replyKey = service::message::rpc::Contract::reply(id);
        const std::string_view keys[]{ replyKey, replyStream, requestStream };
        const std::string_view args[]{
            record.payload,
            service::message::rpc::Contract::replyLifetime,
            group,
            messageId,
            service::message::live::kWorkerStreamCapacity,
            service::message::live::kWorkerStreamTtlMilliseconds
        };
        const auto reply = co_await redis.eval(kReplyScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("RPC persist reply", reply);
        }
    }
};

} // namespace service::messaging::rpc
