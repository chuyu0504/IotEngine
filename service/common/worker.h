#pragma once

#include <cstddef>
#include <string>

#include "service/common/message.h"

namespace service::common {

// Immutable process incarnation and worker position, supplied by the owner.
struct WorkerIdentity final {
    const std::string instanceId;
    const std::size_t index;
    const std::size_t count;
    const service::message::RedisChannelScope channels;
};

} // namespace service::common
