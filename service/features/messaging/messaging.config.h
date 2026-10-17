#pragma once

#include <cstdint>
#include <stdexcept>

#include <ruvia/web/dotenv.h>

namespace service::messaging::outbox {

struct Policy final {
    std::int64_t pendingAlertThreshold{ 1000 };
    std::int64_t oldestAgeAlertMs{ 300000 };
    std::int64_t deadLetterAlertThreshold{ 1 };
    std::int64_t receiptRetentionDays{ 30 };

    static Policy fromEnv(const ruvia::env& env) {
        Policy policy;
        policy.pendingAlertThreshold = env.get<std::int64_t>("OUTBOX_PENDING_ALERT_THRESHOLD").value_or(1000);
        policy.oldestAgeAlertMs = env.get<std::int64_t>("OUTBOX_OLDEST_AGE_ALERT_MS").value_or(300000);
        policy.deadLetterAlertThreshold = env.get<std::int64_t>("OUTBOX_DEAD_LETTER_ALERT_THRESHOLD").value_or(1);
        policy.receiptRetentionDays = env.get<std::int64_t>("OUTBOX_RECEIPT_RETENTION_DAYS").value_or(30);
        if (policy.pendingAlertThreshold < 0 || policy.oldestAgeAlertMs < 0 ||
            policy.deadLetterAlertThreshold < 0 || policy.receiptRetentionDays < 0 ||
            policy.receiptRetentionDays > 3650) {
            throw std::runtime_error("OUTBOX policy values are invalid");
        }
        return policy;
    }
};

} // namespace service::messaging::outbox
