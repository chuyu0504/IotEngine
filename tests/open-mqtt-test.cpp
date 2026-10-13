#include "service/features/access/access.transport.h"

#include <future>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

template <typename Action>
void rejects(Action action) {
    bool rejected = false;
    try { action(); } catch (const std::exception&) { rejected = true; }
    require(rejected, "invalid MQTT input was accepted");
}

std::pair<unsigned char, std::string> readPacket(asio::ip::tcp::socket& socket) {
    unsigned char header{}, byte{};
    asio::read(socket, asio::buffer(&header, 1));
    std::size_t size = 0, multiplier = 1;
    do {
        asio::read(socket, asio::buffer(&byte, 1));
        size += (byte & 127) * multiplier;
        multiplier *= 128;
        require(multiplier <= 128 * 128 * 128 * 128, "unbounded MQTT length");
    } while (byte & 128);
    require(size <= 1024 * 1024, "oversized MQTT packet");
    std::string body(size, '\0');
    asio::read(socket, asio::buffer(body));
    return {header, body};
}

void exchange(int scenario) {
    asio::io_context io;
    asio::ip::tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
    const auto port = acceptor.local_endpoint().port();
    std::exception_ptr serverError;
    std::jthread server([&] {
        try {
            asio::ip::tcp::socket socket(io);
            acceptor.accept(socket);
            const auto [header, connect] = readPacket(socket);
            require(header == 0x10 && connect.substr(2, 4) == "MQTT" && connect[6] == 4,
                    "invalid CONNECT protocol");
            require(static_cast<unsigned char>(connect[7]) == 0xc2,
                    "CONNECT must carry credentials and clean session");
            require(connect.ends_with(std::string("\0\x04user\0\x04pass", 12)),
                    "MQTT credentials were not encoded independently");
            std::string connack("\x20\x02\0\0", 4);
            if (scenario == 1) connack[3] = 5;
            // Fragmented responses must be reassembled by the transport.
            asio::write(socket, asio::buffer(connack.data(), 1));
            asio::write(socket, asio::buffer(connack.data() + 1, 3));
            if (scenario == 1) return;
            const auto [publishHeader, publish] = readPacket(socket);
            require(publishHeader == 0x32, "PUBLISH must use QoS 1 and retain=false");
            require(publish == std::string("\0\x0b" "events/test\0\x01{\"value\":42}", 27),
                    "topic, packet identifier or payload changed on the wire");
            if (scenario == 3) { std::this_thread::sleep_for(1200ms); return; }
            std::string ack("\x40\x02\0\x01", 4);
            if (scenario == 2) ack[3] = 2;
            asio::write(socket, asio::buffer(ack));
            if (scenario == 0) require(readPacket(socket).first == 0xe0, "missing DISCONNECT");
        } catch (...) { serverError = std::current_exception(); }
    });
    service::access::DeliveryClient client;
    std::promise<service::access::DeliveryResponse> completed;
    auto result = completed.get_future();
    client.publishMqtt({false, "127.0.0.1", std::to_string(port)}, "test-client", "user", "pass",
                       "events/test", "{\"value\":42}", 1s,
                       [&](auto response) { completed.set_value(std::move(response)); });
    require(result.wait_for(4s) == std::future_status::ready, "MQTT operation did not respect its deadline");
    const auto response = result.get();
    server.join();
    if (serverError) std::rethrow_exception(serverError);
    require(response.error.empty() == (scenario == 0), "MQTT acceptance boundary is not PUBACK");
}

int main() {
    try {
        using namespace service::utils::mqtt;
        require(parseBroker("mqtts://[::1]:8884").tls, "TLS endpoint not recognized");
        require(parseBroker("mqtt://localhost").port == "1883", "default MQTT port changed");
        for (const auto url : {"http://host", "mqtt://u:p@host", "mqtt://host/path", "mqtt://host:0", "mqtt://host:65536", "mqtt://host?q=1"})
            rejects([&] { (void)parseBroker(url); });
        for (const auto value : {"", "iot/+", "iot/#", "iot/{unknown}"})
            rejects([&] { (void)publish(value, "{}"); });
        rejects([] { (void)connect("id", "", "password"); });
        rejects([] { (void)publish("topic", std::string(1024 * 1024, 'x')); });
        for (int scenario = 0; scenario < 4; ++scenario) exchange(scenario);
        std::cout << "MQTT publish, authentication, PUBACK and timeout tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
