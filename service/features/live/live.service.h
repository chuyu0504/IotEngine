#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "service/common/message.h"
#include "service/utils/redis.h"

namespace service::live {

template <typename Redis>
ruvia::task<void> publish(const Redis& redis, std::string_view topic) {
    const std::vector<std::string> args{
        "XADD", std::string(service::message::live::kChanges), "MAXLEN", "~", "100000", "*",
        std::string(service::message::live::kSchemaVersionField),
        std::string(service::message::live::kSchemaVersion),
        std::string(service::message::live::kTopicField), std::string(topic)};
    const auto reply = co_await service::message::redis::command(redis, args);
    if (reply.kind() != ruvia::redis_value::kind_type::string)
        service::message::redis::throwValue("XADD live change", reply);
}

} // namespace service::live
