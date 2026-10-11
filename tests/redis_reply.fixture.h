#pragma once

#include <asio.hpp>
#include <chrono>
#include <cstdint>
#include <istream>
#include <memory_resource>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/web/redis/redis_client.h>

namespace test {

// Decode controlled RESP replies through the public client; the returned
// value owns a copy in the caller's PMR instead of framework-private storage.
class RedisReplyServer final {
public:
    explicit RedisReplyServer(std::string reply)
        : acceptor_(io_, {asio::ip::address_v4::loopback(), 0}),
          port_(acceptor_.local_endpoint().port()),
          reply_(std::move(reply)), thread_([this] { serve(); }) {}

    ~RedisReplyServer() {
        asio::error_code error;
        asio::ip::tcp::socket wake(io_);
        wake.connect({asio::ip::address_v4::loopback(), port_}, error);
        wake.close(error);
        thread_.join();
    }

    std::uint16_t port() const noexcept { return port_; }

private:
    void serve() noexcept {
        try {
            asio::ip::tcp::socket socket(io_);
            acceptor_.accept(socket);
            asio::streambuf buffer;
            std::istream input(&buffer);
            const auto line = [&] {
                asio::read_until(socket, buffer, "\r\n");
                std::string value;
                std::getline(input, value);
                value.pop_back();
                return value;
            };
            for (;;) {
                const auto array = line();
                if (array.empty() || array.front() != '*') return;
                const auto count = std::stoul(array.substr(1));
                std::string command;
                for (std::size_t index = 0; index < count; ++index) {
                    const auto bulk = line();
                    const auto size = std::stoul(bulk.substr(1));
                    if (buffer.size() < size + 2) {
                        asio::read(socket, buffer, asio::transfer_exactly(size + 2 - buffer.size()));
                    }
                    std::string value(size, '\0');
                    input.read(value.data(), static_cast<std::streamsize>(size));
                    input.ignore(2);
                    if (index == 0) command = std::move(value);
                }
                const std::string_view response = command == "PING"
                    ? std::string_view{"+PONG\r\n"} : std::string_view{reply_};
                asio::write(socket, asio::buffer(response));
            }
        } catch (...) {
            // EOF ends the fixture. Other wire failures reach the client task.
        }
    }

    asio::io_context io_;
    asio::ip::tcp::acceptor acceptor_;
    std::uint16_t port_;
    std::string reply_;
    std::thread thread_;
};

inline ruvia::task<ruvia::redis_value> readRedisReply(
    ruvia::event_loop loop, std::uint16_t port, std::pmr::memory_resource* resource) {
    ruvia::redis_config config;
    config.port_ = port;
    config.tls_.mode_ = ruvia::client_tls_mode::disabled;
    config.pool_size_per_worker_ = 1;
    config.connect_timeout_ = std::chrono::seconds(2);
    config.command_timeout_ = std::chrono::seconds(2);
    ruvia::redis_client client(loop, config);
    co_await client.connect();
    const auto redis = client.handle();
    auto value = co_await redis.command("FIXTURE");
    ruvia::redis_value owned(value, ruvia::redis_value::allocator_type(resource));
    co_await client.shutdown();
    co_return owned;
}

inline ruvia::redis_value redisReply(std::string encoded, std::pmr::memory_resource* resource) {
    RedisReplyServer server(std::move(encoded));
    ruvia::event_loop_pool pool({.loop_count_ = 1});
    pool.start();
    auto result = pool.loop(0).start(readRedisReply(pool.loop(0), server.port(), resource)).get();
    pool.stop();
    pool.join();
    return result;
}

inline std::string encodeRedisReply(const ruvia::redis_value& value) {
    switch (value.kind()) {
    case ruvia::redis_value::kind_type::null: return "$-1\r\n";
    case ruvia::redis_value::kind_type::string:
        return "$" + std::to_string(value.string().size()) + "\r\n" + std::string(value.string()) + "\r\n";
    case ruvia::redis_value::kind_type::error: return "-" + std::string(value.error()) + "\r\n";
    case ruvia::redis_value::kind_type::integer: return ":" + std::to_string(value.integer()) + "\r\n";
    case ruvia::redis_value::kind_type::array: {
        std::string encoded = "*" + std::to_string(value.array().size()) + "\r\n";
        for (const auto& item : value.array()) encoded += encodeRedisReply(item);
        return encoded;
    }
    }
    throw std::logic_error("unknown Redis reply kind");
}

inline ruvia::redis_value redisNull(std::pmr::memory_resource* resource) {
    return redisReply("$-1\r\n", resource);
}
inline ruvia::redis_value redisString(std::string_view value, std::pmr::memory_resource* resource) {
    return redisReply("$" + std::to_string(value.size()) + "\r\n" + std::string(value) + "\r\n", resource);
}
inline ruvia::redis_value redisError(std::string_view value, std::pmr::memory_resource* resource) {
    return redisReply("-" + std::string(value) + "\r\n", resource);
}
inline ruvia::redis_value redisInteger(std::int64_t value, std::pmr::memory_resource* resource) {
    return redisReply(":" + std::to_string(value) + "\r\n", resource);
}
inline ruvia::redis_value redisArray(std::pmr::vector<ruvia::redis_value> values, std::pmr::memory_resource* resource) {
    std::string encoded = "*" + std::to_string(values.size()) + "\r\n";
    for (const auto& value : values) encoded += encodeRedisReply(value);
    return redisReply(std::move(encoded), resource);
}

} // namespace test
