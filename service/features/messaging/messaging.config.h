#pragma once

#include <cstdint>

namespace service::messaging::outbox {

struct Policy final {
    std::int64_t pendingAlertThreshold{ 1000 };
    std::int64_t oldestAgeAlertMs{ 300000 };
    std::int64_t deadLetterAlertThreshold{ 1 };
    std::int64_t receiptRetentionDays{ 30 };
};

} // namespace service::messaging::outbox
