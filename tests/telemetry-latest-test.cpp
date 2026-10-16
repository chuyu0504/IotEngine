#include <chrono>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/web/redis/redis_client.h>

#include "service/features/telemetry/latest/latest.service.h"

#include "tests/redis_server.fixture.h"

// Executes the telemetry latest Lua scripts against a real Redis: realtime
// changes are published once per batch with the ids of the touched devices
// only, and a flushed script cache is recovered transparently.
namespace {

ruvia::task<ruvia::redis_value> command(const ruvia::redis_handle& redis, std::initializer_list<std::string_view> arguments) {
    co_return co_await redis.command(std::span(arguments.begin(), arguments.size()));
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

// (topic, ids) of every live change entry, oldest first.
ruvia::task<std::vector<std::pair<std::string, std::string>>> changes(const ruvia::redis_handle& redis) {
    const auto reply = co_await command(redis, { "XRANGE", service::message::live::kChanges, "-", "+" });
    require(reply.kind() == ruvia::redis_value::kind_type::array, "XRANGE reply expected");
    std::vector<std::pair<std::string, std::string>> result;
    for (const auto& entry : reply.array()) {
        const auto fields = entry.array()[1].array();
        std::string topic, ids;
        for (std::size_t index = 0; index + 1 < fields.size(); index += 2) {
            if (fields[index].string() == service::message::live::kTopicField) {
                topic = fields[index + 1].string();
            }
            if (fields[index].string() == service::message::live::kIdsField) {
                ids = fields[index + 1].string();
            }
        }
        result.emplace_back(topic, ids);
    }
    co_return result;
}

ruvia::task<std::string> field(const ruvia::redis_handle& redis, std::string_view key, std::string_view name) {
    const auto reply = co_await command(redis, { "HGET", key, name });
    co_return reply.kind() == ruvia::redis_value::kind_type::string ? std::string(reply.string()) : std::string{};
}

service::message::ParsedDeviceMessage report(std::string deviceId, std::int64_t observedAtMs) {
    service::message::ParsedDeviceMessage message;
    message.deviceId = std::move(deviceId);
    message.deviceCode = "code-" + message.deviceId;
    message.protocol = "Modbus";
    message.observedAtMs = observedAtMs;
    message.onlineWindowMs = 300000;
    message.valuesJson = R"({"values":{"pressure":{"value":42,"name":"Pressure","dataType":"FLOAT"}}})";
    return message;
}

ruvia::task<void> verifyLatest(const ruvia::redis_handle& redis) {
    namespace latest = service::telemetry::latest;
    const auto now = service::message::utcNowMilliseconds();
    co_await latest::initializeDevice(redis, "device-a", "code-device-a");
    co_await latest::initializeDevice(redis, "device-b", "code-device-b");
    auto entries = co_await changes(redis);
    require(entries.size() == 2 && entries[0] == std::pair<std::string, std::string>{ "device.realtime", "device-a" } && entries[1] == std::pair<std::string, std::string>{ "device.realtime", "device-b" }, "initialization did not publish its device id");

    // device-c has no runtime projection, so the script leaves it untouched.
    co_await latest::update(redis, { report("device-a", now), report("device-a", now + 1), report("device-c", now) });
    entries = co_await changes(redis);
    require(entries.size() == 3 && entries[2] == std::pair<std::string, std::string>{ "device.realtime", "device-a" }, "a batch must publish one change naming only the touched devices");
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "state")) == "online", "update did not mark the reporting device online");

    // A stale report touches nothing and publishes nothing.
    co_await latest::update(redis, { report("device-a", now - 600000) });
    require((co_await changes(redis)).size() == 3, "an older report published a change");

    // The cached SHA survives a flushed script cache: NOSCRIPT reloads and retries once.
    (void)co_await command(redis, { "SCRIPT", "FLUSH" });
    co_await latest::update(redis, { report("device-b", now + 2) });
    entries = co_await changes(redis);
    require(entries.size() == 4 && entries[3] == std::pair<std::string, std::string>{ "device.realtime", "device-b" }, "update did not recover from a flushed script cache");

    // Due online deadline: claim, mark stale and publish the expired id once.
    (void)co_await command(redis, { "HSET", service::message::telemetry::runtimeKey("device-a"), "online_until_ms", "1" });
    (void)co_await command(redis, { "ZADD", latest::onlineDeadlinesKey(), "1", "device-a" });
    co_await latest::expireStale(redis);
    co_await latest::expireStale(redis);
    entries = co_await changes(redis);
    require(entries.size() == 5 && entries[4] == std::pair<std::string, std::string>{ "device.realtime", "device-a" }, "deadline expiry must publish the expired device exactly once");
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "state_reason")) == "data_stale", "expired device was not marked stale");

    // A renewed deadline is re-armed instead of expired.
    (void)co_await command(redis, { "ZADD", latest::onlineDeadlinesKey(), "1", "device-b" });
    co_await latest::expireStale(redis);
    require((co_await changes(redis)).size() == 5, "a renewed device was published as expired");

    co_await latest::eraseDevice(redis, "device-b");
    entries = co_await changes(redis);
    require(entries.size() == 6 && entries[5] == std::pair<std::string, std::string>{ "device.realtime", "device-b" }, "erase did not publish its device id");
}

ruvia::task<void> run(ruvia::event_loop loop, std::uint16_t port) {
    ruvia::redis_config configuration;
    configuration.host_ = "127.0.0.1";
    configuration.port_ = port;
    configuration.tls_.mode_ = ruvia::client_tls_mode::disabled;
    configuration.pool_size_per_worker_ = 1;
    configuration.command_timeout_ = std::chrono::seconds(3);
    ruvia::redis_client client(loop, configuration);
    co_await client.connect();
    std::exception_ptr error;
    try {
        co_await verifyLatest(client.handle());
    } catch (...) {
        error = std::current_exception();
    }
    co_await client.shutdown();
    if (error) {
        std::rethrow_exception(error);
    }
}

} // namespace

int main() {
    if (std::string_view(IOT_TEST_REDIS_SERVER).empty()) {
        std::cout << "telemetry latest Redis test skipped: redis-server is not installed\n";
        return 77;
    }
    try {
        test::RedisServer redis(IOT_TEST_REDIS_SERVER, IOT_TEST_BUILD_DIRECTORY);
        ruvia::event_loop_pool pool({ .loop_count_ = 1 });
        pool.start();
        pool.loop(0).start(run(pool.loop(0), redis.port())).get();
        pool.stop();
        pool.join();
        std::cout << "telemetry latest tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "telemetry latest test failed: " << error.what() << '\n';
        return 1;
    }
}
