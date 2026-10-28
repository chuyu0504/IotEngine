#pragma once

#include <any>
#include <cstdint>
#include <functional>
#include <map>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/web/context.h>
#include <ruvia/web/db/db_handle.h>
#include <ruvia/web/http_client.h>
#include <ruvia/web/redis/redis_handle.h>

namespace service::middleware {

// Authorization facts belong to one SSE connection, never to its Worker.
// Invalidating also prevents an in-flight query from caching a stale answer.
class AuthorizationCache final {
  public:
    void invalidate() {
        values_.clear();
        ++generation_;
    }

    [[nodiscard]] std::uint64_t generation() const { return generation_; }

    template <typename Value, typename Load>
    ruvia::task<Value> get(std::string key, Load load) {
        for (;;) {
            if (const auto found = values_.find(key); found != values_.end()) {
                co_return std::any_cast<Value>(found->second);
            }
            const auto generation = generation_;
            Value value = co_await load();
            if (generation != generation_) {
                continue;
            }
            values_.insert_or_assign(std::move(key), value);
            co_return value;
        }
    }

  private:
    std::map<std::string, std::any, std::less<>> values_;
    std::uint64_t generation_ = 0;
};

// Each operation owns temporary model storage on the accepting Worker.
// HTTP handlers serialize with c.json(model) before this scope ends; SSE
// snapshots release their model storage after each event.
class RequestContext {
  public:
    RequestContext(ruvia::context& connection, std::string actorId)
        : RequestContext(connection, std::move(actorId), connection.stop_token()) {}

    RequestContext(ruvia::context& connection, std::string actorId, ruvia::stop_token cancellation)
        : userId(std::move(actorId)), stop(std::move(cancellation)), connection_(connection), memory_(connection.pool()) {}

    auto db() const { return connection_.db().with_options({ .stop_token_ = stop }); }

    auto db(std::string_view alias) const { return connection_.db(alias).with_options({ .stop_token_ = stop }); }

    auto redis() const { return connection_.redis().with_options({ .stop_token_ = stop }); }

    auto pool() const { return connection_.pool(); }

    auto arena() { return &memory_; }

    auto http_client(std::string_view alias) const { return connection_.http_client(alias).with_options({ .stop_token_ = stop }); }

    // Borrow the connection's stable capability through this operation's awaits.
    const ruvia::worker_handle& worker() const& noexcept { return connection_.worker(); }

    const ruvia::worker_handle& worker() const&& = delete;

    template <typename State>
    State& worker_state() const { return connection_.worker_state<State>(); }

    auto stop_token() const { return stop; }

    const auto& env() const { return connection_.env(); }

    // Plain requests do not cache; live requests borrow their connection cache.
    void shareAuthorization(AuthorizationCache& cache) { authorizations_ = &cache; }

    template <typename Value, typename Load>
    ruvia::task<Value> authorization(std::string key, Load load) {
        if (authorizations_ == nullptr) {
            co_return co_await load();
        }
        co_return co_await authorizations_->get<Value>(std::move(key), std::move(load));
    }

    std::string userId;
    ruvia::stop_token stop;

  private:
    ruvia::context& connection_;
    std::pmr::monotonic_buffer_resource memory_;
    AuthorizationCache* authorizations_ = nullptr;
};

// Bound by authentication middleware for this request only. The query runs
// on the connection's Worker and owns no cross-request mutable state.
struct AuthenticatedUserSnapshot {
    std::function<ruvia::task<std::string>(RequestContext&)> query;
};

} // namespace service::middleware
