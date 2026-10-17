#pragma once

#include <cstddef>
#include <string>

namespace service::common {

// Immutable process incarnation and worker position, supplied by the owner.
struct WorkerIdentity final {
    const std::string instanceId;
    const std::size_t index;
    const std::size_t count;
};

} // namespace service::common
