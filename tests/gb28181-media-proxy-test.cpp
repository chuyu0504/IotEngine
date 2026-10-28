#include <array>
#include <asio/ip/tcp.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/web/app.h>
#include <ruvia/web/controller.h>
#include <ruvia/web/http_client.h>

#include "service/modules/gb28181/gb28181.service.h"

using namespace std::chrono_literals;
using service::gb28181::MediaProxyService;

namespace {
void require(bool value, const char* message) {
    if (!value) {
        throw std::runtime_error(message);
    }
}

class Origin final {
  public:
    Origin() : acceptor_(io_, { asio::ip::address_v4::loopback(), 0 }) {
        acceptor_.non_blocking(true);
        thread_ = std::jthread([this](std::stop_token stop) {
            run(stop);
        });
    }

    ~Origin() { thread_.request_stop(); }

    std::uint16_t port() const { return acceptor_.local_endpoint().port(); }

    std::atomic_bool cancelled{ false };
    std::atomic_bool failed{ false };

  private:
    bool send(asio::ip::tcp::socket& socket, std::string_view bytes, std::stop_token stop) {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (!bytes.empty() && !stop.stop_requested() && std::chrono::steady_clock::now() < deadline) {
            std::error_code error;
            const auto size = socket.write_some(asio::buffer(bytes.data(), bytes.size()), error);
            if (error == asio::error::would_block || error == asio::error::try_again) {
                std::this_thread::sleep_for(2ms);
                continue;
            }
            if (error) {
                return false;
            }
            bytes.remove_prefix(size);
        }
        return bytes.empty();
    }

    void run(std::stop_token stop) {
        try {
            while (!stop.stop_requested()) {
                asio::ip::tcp::socket socket(io_);
                std::error_code error;
                acceptor_.accept(socket, error);
                if (error == asio::error::would_block || error == asio::error::try_again) {
                    std::this_thread::sleep_for(2ms);
                    continue;
                }
                if (error) {
                    throw std::system_error(error);
                }
                socket.non_blocking(true);
                std::string request;
                std::array<char, 4096> buffer{};
                const auto deadline = std::chrono::steady_clock::now() + 5s;
                while (request.find("\r\n\r\n") == std::string::npos && !stop.stop_requested()) {
                    require(std::chrono::steady_clock::now() < deadline, "origin request timed out");
                    const auto size = socket.read_some(asio::buffer(buffer), error);
                    if (error == asio::error::would_block || error == asio::error::try_again) {
                        std::this_thread::sleep_for(2ms);
                        continue;
                    }
                    if (error) {
                        throw std::system_error(error);
                    }
                    request.append(buffer.data(), size);
                    require(request.size() < 65536, "origin request exceeded limit");
                }
                if (request.find("/rtp/playlist.m3u8?token=test") != std::string::npos) {
                    const std::string body = "#EXTM3U\nsegment.ts\n";
                    (void)send(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\nCache-Control: no-cache\r\nConnection: close\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body, stop);
                } else if (request.find("/rtp/large.live.flv") != std::string::npos) {
                    const std::string body(3U * 1024U * 1024U, 'm');
                    require(send(socket, "HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\nConnection: close\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n", stop), "large response head failed");
                    require(send(socket, body, stop), "large response failed");
                } else if (request.find("/rtp/cancel.live.flv") != std::string::npos) {
                    (void)send(socket, "HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\nConnection: close\r\n\r\n", stop);
                    const std::string body(32U * 1024U, 'c');
                    while (!stop.stop_requested()) {
                        if (!send(socket, body, stop)) {
                            cancelled.store(true);
                            break;
                        }
                        std::this_thread::sleep_for(2ms);
                    }
                } else {
                    throw std::runtime_error("unexpected media proxy target");
                }
            }
        } catch (...) {
            failed.store(true);
        }
    }

    asio::io_context io_;
    asio::ip::tcp::acceptor acceptor_;
    std::jthread thread_;
};

void verifyTargets() {
    require(MediaProxyService::allowedTarget("GET", "/rtp/stream.live.flv"), "media proxy rejected an HTTP-FLV target");
    require(MediaProxyService::allowedTarget("POST", "/index/api/webrtc"), "media proxy rejected WebRTC signaling");
    require(!MediaProxyService::allowedTarget("GET", "/index/api/getServerConfig"), "media proxy exposed an internal ZLM API");
    require(!MediaProxyService::allowedTarget("POST", "/rtp/stream.live.flv"), "media proxy accepted a media POST");
    require(!MediaProxyService::allowedTarget("GET", "/rtp/../index/api/getServerConfig"), "media proxy accepted a traversal target");
    require(!MediaProxyService::allowedTarget("GET", "/rtp/%2e%2e/index/api/getServerConfig") && !MediaProxyService::allowedTarget("GET", "/rtp/..\\index\\api\\getServerConfig"), "media proxy accepted an encoded traversal target");
    require(MediaProxyService::rewriteHlsPlaylist("#EXTM3U\nsegment-1.ts\nsegment-2.ts?part=1\n", "token=x&expires=1") == "#EXTM3U\nsegment-1.ts?token=x&expires=1\nsegment-2.ts?part=1&token=x&expires=1\n", "HLS proxy did not preserve playback authorization on child requests");
    require(MediaProxyService::rewriteHlsPlaylist("#EXTM3U\r\nsegment.ts\r\n", "token=x") == "#EXTM3U\r\nsegment.ts?token=x\r\n", "HLS proxy broke CRLF playlists");
    require(MediaProxyService::rewriteHlsPlaylist("#EXTM3U\nsegment.ts\n", "") == "#EXTM3U\nsegment.ts\n", "HLS proxy rewrote a playlist without a play token");
}

// The production controller first checks the Collector-published media state
// in Redis; this test drives the forwarding service against a local origin.
class TestMediaProxyController final {
  public:
    RUVIA_CONTROLLER_GROUP("/media")
    RUVIA_ROUTES_BEGIN(TestMediaProxyController)
    RUVIA_GET("/*", proxy);
    RUVIA_ROUTES_END

  private:
    ruvia::task<void> proxy(ruvia::context& c) { co_await MediaProxyService::forward(c); }
};

ruvia::task<void> verifyProxy(ruvia::event_loop loop, std::uint16_t port) {
    ruvia::http_client client(loop, { .scheme_ = ruvia::http_scheme::http, .host_ = "127.0.0.1", .port_ = port, .request_timeout_ = 5s, .max_response_bytes_ = 4U * 1024U * 1024U });
    {
        const auto http = client.handle();
        {
            auto response = co_await http.send({ .target_ = "/media/rtp/playlist.m3u8?token=test" });
            require(response.status().value() == 200, "HLS proxy status failed");
            require(response.header("Cache-Control") == "no-cache", "media response headers were lost");
            const auto body = co_await response.body().read_all();
            require(std::string_view(reinterpret_cast<const char*>(body.bytes().data()), body.size()) == "#EXTM3U\nsegment.ts?token=test\n", "HLS child URL did not retain authorization");
        }
        {
            auto response = co_await http.send({ .target_ = "/media/rtp/large.live.flv" });
            const auto body = co_await response.body().read_all(4U * 1024U * 1024U);
            require(body.size() == 3U * 1024U * 1024U && body.bytes().front() == std::byte{ 'm' } && body.bytes().back() == std::byte{ 'm' }, "media stream was truncated at the proxy buffering limit");
        }
        {
            auto response = co_await http.send({ .target_ = "/media/rtp/cancel.live.flv" });
            require((co_await response.body().read()).has_value(), "cancellable media stream did not start");
        }
    }
    co_await client.shutdown();
}
} // namespace

int main() {
    auto& app = ruvia::app();
    std::thread serving;
    std::exception_ptr serverError;
    try {
        verifyTargets();
        Origin origin;
        asio::io_context reserveIo;
        asio::ip::tcp::acceptor reservation(reserveIo, { asio::ip::address_v4::loopback(), 0 });
        const auto port = reservation.local_endpoint().port();
        reservation.close();
        std::promise<void> ready;
        auto started = ready.get_future();
        app.blocking_pool(nullptr).listen({ .address_ = "127.0.0.1", .http_ = port }).server({ .worker_count_ = 2 }).http_client({ .alias_ = "gb-media", .config_ = { .scheme_ = ruvia::http_scheme::http, .host_ = "127.0.0.1", .port_ = origin.port(), .connection_count_ = 2, .request_timeout_ = std::nullopt, .max_response_bytes_ = 2U * 1024U * 1024U } }).on_start([&] {
            ready.set_value();
        });
        serving = std::thread([&] {
            try {
                app.run();
            } catch (...) {
                serverError = std::current_exception();
                try {
                    ready.set_exception(serverError);
                } catch (...) {
                }
            }
        });
        require(started.wait_for(10s) == std::future_status::ready, "media proxy app startup timed out");
        started.get();
        ruvia::event_loop_pool clientPool({ .loop_count_ = 1 });
        clientPool.start();
        clientPool.loop(0).start(verifyProxy(clientPool.loop(0), port)).get();
        clientPool.stop();
        clientPool.join();
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (!origin.cancelled.load() && !origin.failed.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(5ms);
        }
        require(origin.cancelled.load() && !origin.failed.load(), "client disconnect did not close the media upstream");
        app.stop();
        serving.join();
        if (serverError) {
            std::rethrow_exception(serverError);
        }
        std::cout << "media proxy HLS, long stream, and cancellation tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        app.stop();
        if (serving.joinable()) {
            serving.join();
        }
        std::cerr << error.what() << '\n';
        return 1;
    }
}
