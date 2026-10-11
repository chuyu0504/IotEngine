#pragma once

#include <functional>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/web/context.h>
#include <ruvia/web/http_client_handle.h>
#include <ruvia/web/db/db_handle.h>
#include <ruvia/web/redis/redis_handle.h>

namespace service::middleware {

// Each operation owns temporary model storage on the accepting Worker.
// HTTP handlers serialize with c.json(model) before this scope ends; SSE
// snapshots release their model storage after each event.
class RequestContext {
  public:
    RequestContext(ruvia::context& connection, std::string actorId)
        : RequestContext(connection, std::move(actorId), connection.get_stop_token()) {}

    RequestContext(ruvia::context& connection, std::string actorId, ruvia::stop_token cancellation)
        : userId(std::move(actorId)), stop(std::move(cancellation)), connection_(connection), memory_(connection.pool()) {}

    auto db() const { return connection_.db().with_options({ .stop_token_ = stop }); }

    auto redis() const { return connection_.redis().with_options({ .stop_token_ = stop }); }

    auto pool() const { return connection_.pool(); }

    auto arena() { return &memory_; }

    auto get_http_client(std::string_view alias) const { return connection_.get_http_client(alias).with_options({ .stop_token_ = stop }); }

    auto worker() const { return connection_.worker(); }

    template <typename State>
    State& worker_state() const { return connection_.worker_state<State>(); }

    auto get_stop_token() const { return stop; }

    const auto& env() const { return connection_.env(); }

    std::string userId;
    ruvia::stop_token stop;

  private:
    ruvia::context& connection_;
    std::pmr::monotonic_buffer_resource memory_;
};

// Bound by authentication middleware for this request only. The query runs
// on the connection's Worker and owns no cross-request mutable state.
struct AuthenticatedUserSnapshot {
    std::function<ruvia::task<std::string>(RequestContext&)> query;
};

} // namespace service::middleware
