#pragma once

#include <chrono>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>
#include <ruvia/web/redis/redis_types.h>

namespace service::debug_idle {

inline constexpr auto kIdle = std::chrono::minutes(5);

inline std::string leaseKey(std::string_view kind, std::string_view id) {
    std::string key = "iot:debug:lease:";
    key.append(kind);
    key.push_back(':');
    key.append(id);
    return key;
}

inline std::string watchKey(std::string_view kind, std::string_view id) {
    std::string key = "iot:debug:watch:";
    key.append(kind);
    key.push_back(':');
    key.append(id);
    return key;
}

template <typename Redis>
ruvia::task<void> touch(const Redis& redis, std::string_view kind, std::string_view id) {
    ruvia::redis_set_options options;
    options.expiration_ = ruvia::redis_set_expiration::expires_after(kIdle);
    (void)co_await redis.set(leaseKey(kind, id), "1", options);
}

template <typename Redis>
ruvia::task<void> clear(const Redis& redis, std::string_view kind, std::string_view id) {
    (void)co_await redis.del(leaseKey(kind, id));
    (void)co_await redis.del(watchKey(kind, id));
}

template <typename Redis>
ruvia::task<bool> leased(const Redis& redis, std::string_view kind, std::string_view id) {
    co_return co_await redis.exists(leaseKey(kind, id));
}

template <typename Disable>
void watch(const ruvia::web_worker_handle& worker, std::string kind, std::string id, Disable disable) {
    (void)worker.post([kind = std::move(kind), id = std::move(id), disable = std::move(disable)](
                          ruvia::web_worker_context& ctx) mutable -> ruvia::task<void> {
        const auto watchId = watchKey(kind, id);
        ruvia::redis_set_options claim;
        claim.condition_ = ruvia::redis_set_condition::if_absent;
        claim.expiration_ = ruvia::redis_set_expiration::expires_after(kIdle + std::chrono::seconds(30));
        const auto claimed = co_await ctx.redis().set(watchId, "1", claim);
        if (!claimed.applied()) {
            co_return;
        }
        while (!ctx.get_stop_token().stop_requested()) {
            const auto ttl = co_await ctx.redis().ttl(leaseKey(kind, id));
            if (ttl.state() == ruvia::redis_ttl_state::expiring && ttl.remaining() &&
                *ttl.remaining() > std::chrono::milliseconds::zero()) {
                ruvia::redis_set_options keep;
                keep.expiration_ =
                    ruvia::redis_set_expiration::expires_after(*ttl.remaining() + std::chrono::seconds(30));
                (void)co_await ctx.redis().set(watchId, "1", keep);
                (void)co_await ruvia::sleep_for(
                    ctx.worker(), *ttl.remaining() + std::chrono::seconds(1), ctx.get_stop_token());
                continue;
            }
            if (co_await leased(ctx.redis(), kind, id)) {
                (void)co_await ruvia::sleep_for(ctx.worker(), kIdle, ctx.get_stop_token());
                continue;
            }
            try {
                co_await disable(ctx, id);
            } catch (...) {
            }
            (void)co_await ctx.redis().del(watchId);
            co_return;
        }
    });
}

} // namespace service::debug_idle
