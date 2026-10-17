#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>

namespace service::utils {

// The dedicated connection holds only the claim lock. The operation remains on
// its owning Worker and can use that Worker's ordinary business connections.
// Failed initialization never writes completion; a waiting Worker can retry.
template <typename Context, typename Operation>
ruvia::task<void> initializeOnce(Context& context, std::int64_t lockId, std::string completionKey, Operation operation) {
    auto claim = co_await context.db("startup").begin_transaction();
    ruvia::db_query lock;
    lock.select(lock.call("pg_advisory_xact_lock", { lock.cast(lock.value(lockId), ruvia::db_data_type::big_int) }));
    (void)co_await claim.query(lock);
    const auto redis = context.redis();
    if (!co_await redis.exists(completionKey)) {
        co_await operation(context);
        // Each key names one process incarnation. Expiry only reclaims stale
        // startup metadata; no runtime path re-enters this initialization.
        (void)co_await redis.set(completionKey, "ready");
        (void)co_await redis.expire(completionKey, std::chrono::hours(24));
    }
    co_await claim.commit();
}

} // namespace service::utils
