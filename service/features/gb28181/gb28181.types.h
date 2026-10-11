#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "service/features/gb28181/device/device.types.h"

namespace service::gb28181 {

enum class ControlClaimResult : std::int64_t {
    kExpired = -3,
    kCancelled = -2,
    kOwnerLost = -1,
    kAlreadyClaimed = 0,
    kClaimed = 1,
};

struct ProjectionPublishResult final {
    std::string order;
    std::string current;
};

struct ProjectionSnapshot final {
    std::vector<Device> devices;
};

} // namespace service::gb28181
