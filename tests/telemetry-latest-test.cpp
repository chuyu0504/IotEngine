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
inline const service::message::RedisChannelScope kChannels{ 0 };

ruvia::task<ruvia::redis_value> command(const ruvia::redis_handle& redis, std::initializer_list<std::string_view> arguments) {
    co_return co_await redis.command(std::span(arguments.begin(), arguments.size()));
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

ruvia::task<void> expectMessage(ruvia::redis_subscription& subscription, std::string_view channel, std::string_view ids) {
    const auto event = co_await subscription.read();
    require(event && event->kind() == ruvia::redis_pubsub_event_kind::message, "live PubSub message expected");
    require(event->channel() == channel && event->payload() == ids, "live PubSub channel or affected device ids differ");
}

// A same-channel marker is ordered after all preceding transitions by Redis.
// Seeing it next proves that a no-op did not emit an extra business message.
ruvia::task<void> expectQuiet(const ruvia::redis_handle& redis, ruvia::redis_subscription& subscription, std::string_view channel) {
    constexpr std::string_view marker = "test:transition-complete";
    require(co_await redis.publish(channel, marker) == 1, "dedicated subscriber is missing");
    co_await expectMessage(subscription, channel, marker);
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
    const auto channel = service::message::live::channel(kChannels, service::message::live::topic::kDeviceRealtime);
    auto subscription = redis.subscription();
    const std::string_view channels[]{ channel };
    co_await subscription.subscribe(channels);
    const auto confirmed = co_await subscription.read();
    require(confirmed && confirmed->kind() == ruvia::redis_pubsub_event_kind::subscribed && confirmed->channel() == channel && confirmed->subscription_count() == 1, "realtime subscription was not confirmed before device transitions");

    const auto now = service::message::utcNowMilliseconds();
    co_await latest::initializeDevice(redis, kChannels, "device-a", "code-device-a");
    co_await expectMessage(subscription, channel, "device-a");
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "state")) == "offline" && (co_await field(redis, service::message::telemetry::latestKey("device-a"), "_device_id")) == "device-a", "initialization published before persisting the device projection");
    co_await latest::initializeDevice(redis, kChannels, "device-b", "code-device-b");
    co_await expectMessage(subscription, channel, "device-b");
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-b"), "device_code")) == "code-device-b", "initialization did not persist the second device");
    co_await expectQuiet(redis, subscription, channel);

    // device-c has no runtime projection, so the batch must leave it untouched.
    co_await latest::update(redis, kChannels, { report("device-a", now), report("device-a", now + 1), report("device-c", now) }, "collector-test-instance");
    co_await expectMessage(subscription, channel, "device-a");
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "state")) == "online" && (co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "last_report_at_ms")) == std::to_string(now + 1) && !(co_await field(redis, service::message::telemetry::latestKey("device-a"), "pressure")).empty(), "update notification did not expose committed latest telemetry");
    require(!(co_await redis.exists(service::message::telemetry::runtimeKey("device-c"))) && !(co_await redis.exists(service::message::telemetry::latestKey("device-c"))), "unconfigured device acquired a realtime projection");
    co_await expectQuiet(redis, subscription, channel);

    // A stale report touches nothing and publishes nothing.
    const auto pressure = co_await field(redis, service::message::telemetry::latestKey("device-a"), "pressure");
    co_await latest::update(redis, kChannels, { report("device-a", now - 600000) }, "collector-test-instance");
    co_await expectQuiet(redis, subscription, channel);
    require((co_await field(redis, service::message::telemetry::latestKey("device-a"), "pressure")) == pressure && (co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "last_report_at_ms")) == std::to_string(now + 1), "older report changed persisted telemetry");

    // The cached SHA survives a flushed script cache: NOSCRIPT reloads and retries once.
    (void)co_await command(redis, { "SCRIPT", "FLUSH" });
    co_await latest::update(redis, kChannels, { report("device-b", now + 2) }, "collector-test-instance");
    co_await expectMessage(subscription, channel, "device-b");
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-b"), "last_report_at_ms")) == std::to_string(now + 2), "script-cache recovery did not persist the report");
    co_await expectQuiet(redis, subscription, channel);

    co_await latest::update(redis, kChannels, { report("device-b", now + 4), report("device-a", now + 3), report("device-a", now + 3) }, "collector-test-instance");
    co_await expectMessage(subscription, channel, "device-a,device-b");
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "last_report_at_ms")) == std::to_string(now + 3) && (co_await field(redis, service::message::telemetry::runtimeKey("device-b"), "last_report_at_ms")) == std::to_string(now + 4), "multi-device batch notification did not expose every persisted report");
    co_await expectQuiet(redis, subscription, channel);

    // Due online deadline: claim, mark stale and publish the expired id once.
    (void)co_await command(redis, { "HSET", service::message::telemetry::runtimeKey("device-a"), "online_until_ms", "1" });
    (void)co_await command(redis, { "ZADD", latest::onlineDeadlinesKey(), "1", "device-a" });
    co_await latest::expireStale(redis, kChannels);
    co_await expectMessage(subscription, channel, "device-a");
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "state")) == "offline" && (co_await field(redis, service::message::telemetry::runtimeKey("device-a"), "state_reason")) == "data_stale", "expiry notification preceded persisted stale state");
    co_await latest::expireStale(redis, kChannels);
    co_await expectQuiet(redis, subscription, channel);

    // A renewed deadline is re-armed instead of expired.
    (void)co_await command(redis, { "ZADD", latest::onlineDeadlinesKey(), "1", "device-b" });
    co_await latest::expireStale(redis, kChannels);
    co_await expectQuiet(redis, subscription, channel);
    require((co_await field(redis, service::message::telemetry::runtimeKey("device-b"), "state")) == "online", "renewed device was incorrectly marked stale");

    co_await latest::eraseDevice(redis, kChannels, "device-b");
    co_await expectMessage(subscription, channel, "device-b");
    require(!(co_await redis.exists(service::message::telemetry::runtimeKey("device-b"))) && !(co_await redis.exists(service::message::telemetry::latestKey("device-b"))) && (co_await command(redis, { "ZSCORE", latest::onlineDeadlinesKey(), "device-b" })).kind() == ruvia::redis_value::kind_type::null, "erase notification preceded removal of persisted device state");
    co_await expectQuiet(redis, subscription, channel);
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
