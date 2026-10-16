#pragma once

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "service/common/message.h"
#include "service/utils/redis.h"

namespace service::live {

// Delivers one directed topic to the worker registered in its route key; a
// missing route means nobody waits any more and the message is dropped.
inline constexpr std::string_view kPublishDirectedScript = R"lua(
local stream = redis.call('GET', KEYS[1])
if not stream then return 0 end
redis.call('XADD', stream, 'MAXLEN', '~', ARGV[2], '*', 'topic', ARGV[1])
redis.call('PEXPIRE', stream, ARGV[3])
return 1
)lua";

// Publishes one committed business change. Empty ids mean the whole topic.
template <typename Redis>
ruvia::task<void> publish(const Redis& redis, std::string_view topic, std::span<const std::string> ids = {}) {
    const std::vector<std::string> args{
        "XADD", std::string(service::message::live::kChanges), "MAXLEN", "~",
        std::string(service::message::live::kChangesCapacity), "*",
        std::string(service::message::live::kSchemaVersionField), std::string(service::message::live::kSchemaVersion),
        std::string(service::message::live::kTopicField), std::string(topic),
        std::string(service::message::live::kIdsField), service::message::live::joinIds(ids)
    };
    const auto reply = co_await service::message::redis::command(redis, args);
    if (reply.kind() != ruvia::redis_value::kind_type::string) {
        service::message::redis::throwValue("XADD live change", reply);
    }
}

template <typename Redis>
ruvia::task<void> publishDirected(const Redis& redis, std::string_view topic) {
    const auto route = service::message::live::routeKey(topic);
    const std::string_view keys[]{ route };
    const std::string_view args[]{ topic, service::message::live::kWorkerStreamCapacity, service::message::live::kWorkerStreamTtlMilliseconds };
    const auto reply = co_await redis.eval(kPublishDirectedScript, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        service::message::redis::throwValue("publish directed live message", reply);
    }
}

} // namespace service::live
