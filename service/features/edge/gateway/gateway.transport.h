#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string_view>

#include <ruvia/web/controller.h>
#include <ruvia/web/websocket.h>

namespace service::edge::gateway::transport {

// Borrowed by one connection driver on the accepting Service Worker. The native
// socket never leaves this adapter or moves to another Worker.
class Connection final {
  public:
    explicit Connection(ruvia::websocket& socket) : socket_(socket) {}
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    ruvia::scoped_operation<std::optional<ruvia::websocket_message>> read() {
        return socket_.read();
    }

    ruvia::scoped_operation<void> write(std::string_view wire) {
        return socket_.binary(wire, { .compress_ = true });
    }

    ruvia::scoped_operation<void> close(std::uint16_t code, std::string_view reason) {
        return socket_.close(ruvia::websocket_close_options{ .code_ = code, .reason_ = reason });
    }

    void abort() noexcept { socket_.abort(); }

  private:
    ruvia::websocket& socket_;
};

// The assembly binds exactly this connection entry, not a component plugin or
// cross-Worker dispatcher. The binding is immutable for the Worker's lifetime.
struct ConnectionDriver final {
    ruvia::task<void> (*const run)(ruvia::context&, Connection&);

    explicit ConnectionDriver(ruvia::task<void> (*entry)(ruvia::context&, Connection&)) : run(entry) {
        if (!entry) {
            throw std::invalid_argument("gateway connection driver is required");
        }
    }
};

} // namespace service::edge::gateway::transport

namespace service::edge {

class GatewayTransport final {
  public:
    RUVIA_CONTROLLER_GROUP("/edge/v1")
    RUVIA_ROUTES_BEGIN(GatewayTransport)
    const auto webSocketOptions = ruvia::websocket_route_config{
        .lifecycle_ = {
            .heartbeat_ = {
                .ping_interval_ = std::chrono::seconds(300),
                .pong_timeout_ = std::chrono::seconds(60),
            },
            .close_handshake_timeout_ = std::chrono::seconds(5),
        },
        .deflate_ = {
            .enabled_ = true,
            .compression_level_ = 9,
            .context_takeover_ = true,
        },
    };
    RUVIA_GET_WS_OPTIONS("/connect", connect, webSocketOptions);
    RUVIA_ROUTES_END

  private:
    ruvia::task<void> connect(ruvia::context& c) {
        gateway::transport::Connection connection(c.get_websocket());
        co_await c.worker_state<gateway::transport::ConnectionDriver>().run(c, connection);
    }
};

} // namespace service::edge
