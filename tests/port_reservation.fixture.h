#pragma once

#include <asio.hpp>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>
#include <system_error>

namespace test {

// Servers under test bind fixed ports chosen here. The candidates stay below
// the Linux ephemeral range (32768-60999) and above the integration fixture
// port blocks, so outbound connections of concurrently running tests cannot take
// a port between this check and the server's own bind. Each candidate is
// verified free (UDP and, when requested, TCP on the same number) before use.
inline std::uint16_t reservePort(const asio::ip::address& address, bool withTcp) {
    constexpr std::uint16_t kFirst = 10000;
    constexpr std::uint16_t kLast = 19999;
    static thread_local std::mt19937 random{ std::random_device{}() };
    std::uniform_int_distribution<int> candidates(kFirst, kLast);
    asio::io_context context;
    std::error_code lastError;
    std::uint16_t lastPort = 0;
    for (int attempt = 0; attempt < 64; ++attempt) {
        lastPort = static_cast<std::uint16_t>(candidates(random));
        asio::ip::udp::socket udp(context);
        udp.open(address.is_v6() ? asio::ip::udp::v6() : asio::ip::udp::v4(), lastError);
        if (lastError) {
            continue;
        }
        udp.bind(asio::ip::udp::endpoint(address, lastPort), lastError);
        if (lastError) {
            continue;
        }
        if (!withTcp) {
            return lastPort;
        }
        asio::ip::tcp::acceptor tcp(context);
        tcp.open(address.is_v6() ? asio::ip::tcp::v6() : asio::ip::tcp::v4(), lastError);
        if (lastError) {
            continue;
        }
        tcp.bind(asio::ip::tcp::endpoint(address, lastPort), lastError);
        if (lastError) {
            continue;
        }
        return lastPort;
    }
    throw std::runtime_error("could not reserve a test port; last port=" + std::to_string(lastPort) + ", error=" + lastError.message());
}

} // namespace test
