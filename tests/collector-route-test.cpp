#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <map>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/redis/redis_client.h>

#include "service/features/collector/collector.service.h"
#include "service/features/telemetry/latest/latest.service.h"
#include "service/modules/device/device.entity.h"

#include "tests/redis_server.fixture.h"

namespace {
using service::collector::CollectorStateService;
inline constexpr std::string_view kInstanceId = "collector-route-test";

ruvia::task<ruvia::redis_value> command(const ruvia::redis_handle& redis, std::initializer_list<std::string_view> arguments) {
    co_return co_await redis.command(std::span(arguments.begin(), arguments.size()));
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::int64_t integer(const ruvia::redis_value& reply) {
    require(reply.kind() == ruvia::redis_value::kind_type::integer, "Redis integer reply expected");
    return reply.integer();
}

ruvia::task<std::string> field(const ruvia::redis_handle& redis, std::string_view key, std::string_view name) {
    const auto reply = co_await command(redis, { "HGET", key, name });
    if (reply.kind() == ruvia::redis_value::kind_type::null) {
        co_return std::string{};
    }
    require(reply.kind() == ruvia::redis_value::kind_type::string, "Redis hash reply expected");
    co_return std::string(reply.string());
}

ruvia::task<void> subscribe(ruvia::redis_subscription& subscription, std::string_view channel) {
    const std::string_view channels[]{ channel };
    co_await subscription.subscribe(channels);
    const auto confirmation = co_await subscription.read();
    require(confirmation && confirmation->kind() == ruvia::redis_pubsub_event_kind::subscribed && confirmation->channel() == channel && confirmation->subscription_count() == 1, "device state subscription was not confirmed before the transition");
}

ruvia::task<void> expectMessage(ruvia::redis_subscription& subscription, std::string_view channel, std::string_view ids) {
    const auto event = co_await subscription.read();
    require(event && event->kind() == ruvia::redis_pubsub_event_kind::message && event->channel() == channel && event->payload() == ids, "device state notification channel or affected ids differ");
}

// Redis orders the marker after all preceding transitions on the same channel.
ruvia::task<void> expectQuiet(const ruvia::redis_handle& redis, ruvia::redis_subscription& subscription, std::string_view channel) {
    constexpr std::string_view marker = "test:transition-complete";
    require(co_await redis.publish(channel, marker) == 1, "dedicated state subscriber is missing");
    co_await expectMessage(subscription, channel, marker);
}

ruvia::task<std::pair<std::string, std::string>> readConnection(
    const ruvia::redis_handle& redis,
    std::string_view device
) {
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto route = service::message::collector::connectionKey(device);
    const std::string_view keys[]{ runtime, route };
    const std::string_view args[]{ service::message::collector::kLinkOwnerPrefix };
    const auto reply = co_await redis.eval(service::device::entities::kDeviceConnectionStateScript, keys, args);
    require(reply.kind() == ruvia::redis_value::kind_type::array && reply.array().size() == 4, "connection state reply contract changed");
    require(reply.array()[0].kind() == ruvia::redis_value::kind_type::string && reply.array()[1].kind() == ruvia::redis_value::kind_type::string, "connection state fields are not strings");
    co_return std::pair{ std::string(reply.array()[0].string()), std::string(reply.array()[1].string()) };
}

ruvia::task<void> seedRoute(const ruvia::redis_handle& redis, std::string_view device, std::string_view connection, std::string_view instance = "healthy-remote") {
    const auto route = service::message::collector::connectionKey(device);
    const auto owner = std::string(service::message::collector::kLinkOwnerPrefix) + "read-link";
    (void)co_await command(redis, { "HSET", route, "connection_id", connection, "instance_id", instance, "worker_id", "3", "worker_instance_id", "remote-worker-incarnation", "session_epoch", "11", "link_id", "read-link" });
    (void)co_await command(redis, { "PEXPIRE", route, "3000" });
    (void)co_await command(redis, { "SET", owner, instance, "PX", "5000" });
}

ruvia::task<void> verifyReadLease(const ruvia::redis_handle& redis, const ruvia::worker_handle& worker) {
    constexpr std::string_view device = "read-lease-device";
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto route = service::message::collector::connectionKey(device);
    (void)co_await command(redis, { "HSET", runtime, "connection_id", "legacy-connection", "instance_id", "retired-instance", "worker_id", "0", "last_report_at_ms", "1700000000123" });
    auto state = co_await readConnection(redis, device);
    require(state.first.empty() && state.second == "1700000000123", "legacy permanent connection made an expired device connected or lost its report");
    (void)co_await command(redis, { "HSET", runtime, "state", "offline", "state_reason", "data_stale" });
    {
        const std::string_view keys[]{ runtime, route };
        const std::string_view args[]{ service::message::collector::kLinkOwnerPrefix };
        const auto reply = co_await redis.eval(service::device::entities::kDeviceConnectionStateScript, keys, args);
        require(reply.kind() == ruvia::redis_value::kind_type::array && reply.array().size() == 4 && reply.array()[2].string() == "offline" && reply.array()[3].string() == "data_stale", "connection state omitted the server-computed runtime state");
    }
    co_await seedRoute(redis, device, "remote-connection");
    const auto before = integer(co_await command(redis, { "PTTL", route }));
    state = co_await readConnection(redis, device);
    const auto after = integer(co_await command(redis, { "PTTL", route }));
    require(state.first == "remote-connection" && state.second == "1700000000123", "healthy remote process route was rejected");
    require(before > 0 && after > 0 && after <= before, "reading connection state renewed its lease");
    for (const auto identity : { "connection_id", "instance_id", "worker_id", "worker_instance_id", "session_epoch", "link_id" }) {
        co_await seedRoute(redis, device, "remote-connection");
        (void)co_await command(redis, { "HDEL", route, identity });
        state = co_await readConnection(redis, device);
        require(state.first.empty() && state.second == "1700000000123", "incomplete connection identity was accepted");
    }
    for (const auto& [identity, value] : {
             std::pair<std::string_view, std::string_view>{ "worker_id", "worker-three" },
             { "worker_id", "-1" },
             { "session_epoch", "0" },
             { "session_epoch", "epoch-eleven" } }) {
        co_await seedRoute(redis, device, "remote-connection");
        (void)co_await command(redis, { "HSET", route, identity, value });
        require((co_await readConnection(redis, device)).first.empty(), "malformed worker or session identity was accepted");
    }
    co_await seedRoute(redis, device, "remote-connection");
    (void)co_await command(redis, { "PERSIST", route });
    require((co_await readConnection(redis, device)).first.empty(), "permanent route hash was accepted as a live connection");
    co_await seedRoute(redis, device, "remote-connection");
    const auto owner = std::string(service::message::collector::kLinkOwnerPrefix) + "read-link";
    (void)co_await command(redis, { "SET", owner, "replacement-instance", "PX", "5000" });
    require((co_await readConnection(redis, device)).first.empty(), "link ownership replacement kept an old connection live");
    co_await seedRoute(redis, device, "remote-connection");
    (void)co_await command(redis, { "PEXPIRE", route, "1" });
    (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(40));
    state = co_await readConnection(redis, device);
    require(state.first.empty() && state.second == "1700000000123", "expired connection route survived or telemetry report disappeared");
    require(integer(co_await command(redis, { "PTTL", runtime })) == -1, "connection expiry expired the permanent telemetry runtime");
}

ruvia::task<void> verifyRouteIdentity(const ruvia::redis_handle& redis) {
    constexpr std::string_view device = "route-identity-device";
    constexpr std::string_view link = "route-identity-link";
    const auto route = service::message::collector::connectionKey(device);
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto latest = service::message::telemetry::latestKey(device);
    const auto owner = service::message::collector::linkOwnerKey(link);
    constexpr auto instance = kInstanceId;
    const auto channel = service::message::live::channel(service::message::live::topic::kDeviceState);
    auto subscription = redis.subscription();
    co_await subscribe(subscription, channel);
    (void)co_await command(redis, { "SET", owner, instance, "PX", "60000" });
    (void)co_await command(redis, { "HSET", runtime, "worker_id", "7", "instance_id", "retired-instance", "connection_id", "retired-connection", "link_id", link, "last_report_at_ms", "1700000000456", "online_until_ms", "1700001000456", "state", "online" });
    (void)co_await command(redis, { "HSET", latest, "pressure", "{\"value\":42}", "_state", "retained-telemetry-state" });
    service::collector::ProtocolAction action;
    action.deviceId = device;
    action.deviceCode = "ROUTE-TEST";
    action.connectionId = "connection-a";
    const auto previous = co_await CollectorStateService::bindRoute(redis, action, 11, 3, "worker-a", std::string(link), kInstanceId);
    co_await expectMessage(subscription, channel, device);
    require(previous.connectionId == "retired-connection" && previous.workerId == "7" && previous.instanceId == "retired-instance", "binding lost the previous route owner");
    require((co_await readConnection(redis, device)).first == "connection-a", "bound connection is not visible");
    const auto ttl = integer(co_await command(redis, { "PTTL", route }));
    require(ttl > 0 && ttl <= service::message::collector::kConnectionTtlMilliseconds, "connection route did not receive its bounded lease");
    require((co_await field(redis, runtime, "connection_id")).empty() && (co_await field(redis, runtime, "last_report_at_ms")) == "1700000000456" && (co_await field(redis, latest, "pressure")) == "{\"value\":42}", "binding retained legacy route identity or lost telemetry");
    (void)co_await command(redis, { "PEXPIRE", route, "500" });
    require(co_await CollectorStateService::renewRoute(redis, device, "connection-a", 11, 3, "worker-a", kInstanceId), "current connection could not renew its lease");
    require(integer(co_await command(redis, { "PTTL", route })) > 10000, "successful renewal did not restore the route TTL");
    (void)co_await CollectorStateService::bindRoute(redis, action, 12, 3, "worker-b", std::string(link), kInstanceId);
    co_await expectQuiet(redis, subscription, channel);
    for (const auto& [epoch, workerIndex, incarnation] : {
             std::tuple<std::uint64_t, std::size_t, std::string_view>{ 11, 3, "worker-b" },
             { 12, 4, "worker-b" },
             { 12, 3, "worker-a" } }) {
        require(!(co_await CollectorStateService::renewRoute(redis, device, "connection-a", epoch, workerIndex, incarnation, kInstanceId)), "stale identity renewed a replacement connection");
        co_await CollectorStateService::markDeviceOffline(redis, device, "connection-a", epoch, workerIndex, incarnation, "late_disconnect", kInstanceId);
    }
    require(!(co_await CollectorStateService::renewRoute(redis, device, "old-connection", 12, 3, "worker-b", kInstanceId)), "old connection renewed a replacement connection");
    co_await CollectorStateService::markDeviceOffline(redis, device, "old-connection", 12, 3, "worker-b", "late_disconnect", kInstanceId);
    const std::string_view keys[]{ route, service::message::collector::kConnectionDeadlinesKey };
    const std::string_view arguments[]{ "retired-instance", "3", "worker-b", "connection-a", "12", "15000", service::message::collector::kLinkOwnerPrefix, device };
    require(integer(co_await redis.eval(service::collector::CollectorDeviceRouteRecord::renewScript, keys, arguments)) == 0, "foreign process renewed a live route");
    const std::string_view eraseKeys[]{ route, runtime, channel, service::message::collector::kConnectionDeadlinesKey };
    const std::string_view eraseArguments[]{ "retired-instance", "3", "worker-b", "connection-a", "12", "late_disconnect", "1700000001000", device };
    require(integer(co_await redis.eval(service::collector::CollectorDeviceRouteRecord::eraseScript, eraseKeys, eraseArguments)) == 0, "foreign process erased a live route");
    bool staleBindRejected = false;
    try {
        (void)co_await CollectorStateService::bindRoute(redis, action, 11, 3, "worker-b", std::string(link), kInstanceId);
    } catch (const std::runtime_error&) {
        staleBindRejected = true;
    }
    require((co_await field(redis, route, "session_epoch")) == "12" && (co_await field(redis, route, "worker_instance_id")) == "worker-b" && staleBindRejected, "late bind or cleanup replaced the new epoch");
    co_await expectQuiet(redis, subscription, channel);
    (void)co_await command(redis, { "SET", owner, "replacement-process", "PX", "60000" });
    require(!(co_await CollectorStateService::renewRoute(redis, device, "connection-a", 12, 3, "worker-b", kInstanceId)), "lost link owner continued to renew the route");
    bool rejected = false;
    try {
        (void)co_await CollectorStateService::bindRoute(redis, action, 13, 3, "worker-b", std::string(link), kInstanceId);
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    require(rejected && (co_await field(redis, route, "session_epoch")) == "12", "lost link owner bound a route");
    (void)co_await command(redis, { "SET", owner, instance, "PX", "60000" });
    co_await CollectorStateService::markDeviceOffline(redis, device, "connection-a", 12, 3, "worker-b", "connection_closed", kInstanceId);
    co_await expectMessage(subscription, channel, device);
    require(integer(co_await command(redis, { "EXISTS", route })) == 0 && (co_await readConnection(redis, device)).first.empty(), "disconnect did not erase its owned route");
    require((co_await command(redis, { "ZSCORE", service::message::collector::kConnectionDeadlinesKey, device })).kind() == ruvia::redis_value::kind_type::null, "disconnect left a scheduled connection deadline");
    co_await expectQuiet(redis, subscription, channel);
    co_await CollectorStateService::markDeviceOffline(redis, device, "connection-a", 12, 3, "worker-b", "duplicate_disconnect", kInstanceId);
    co_await expectQuiet(redis, subscription, channel);
    require(!(co_await CollectorStateService::renewRoute(redis, device, "connection-a", 12, 3, "worker-b", kInstanceId)), "disconnected connection was revived by renewal");
    require((co_await field(redis, runtime, "last_report_at_ms")) == "1700000000456" && (co_await field(redis, latest, "pressure")) == "{\"value\":42}" && (co_await field(redis, latest, "_state")) == "retained-telemetry-state", "disconnect altered the latest telemetry");
}

ruvia::task<void> verifyLegacyCleanup(const ruvia::redis_handle& redis) {
    constexpr std::string_view device = "legacy-cleanup-device";
    constexpr std::string_view link = "legacy-cleanup-link";
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto latest = service::message::telemetry::latestKey(device);
    const auto owner = service::message::collector::linkOwnerKey(link);
    const auto channel = service::message::live::channel(service::message::live::topic::kDeviceState);
    auto subscription = redis.subscription();
    co_await subscribe(subscription, channel);
    service::collector::RuntimeSnapshot snapshot;
    service::collector::DeviceDefinition definition;
    definition.id = device;
    definition.linkId = link;
    snapshot.devices.push_back(definition);
    (void)co_await command(redis, { "HSET", runtime, "connection_id", "legacy", "instance_id", "old-process", "worker_id", "0", "worker_instance_id", "old-worker", "session_epoch", "3", "link_id", link, "last_report_at_ms", "1700000000789", "online_until_ms", "1700001000789", "state", "online" });
    (void)co_await command(redis, { "HSET", latest, "temperature", "{\"value\":21}" });
    (void)co_await command(redis, { "SET", owner, "healthy-remote", "PX", "60000" });
    co_await CollectorStateService::eraseLegacyRoutes(redis, snapshot, kInstanceId);
    require((co_await field(redis, runtime, "connection_id")) == "legacy", "another healthy process had its legacy identity erased");
    co_await expectQuiet(redis, subscription, channel);
    (void)co_await command(redis, { "SET", owner, kInstanceId, "PX", "60000" });
    (void)co_await command(redis, { "HSET", runtime, "link_id", "different-link" });
    co_await CollectorStateService::eraseLegacyRoutes(redis, snapshot, kInstanceId);
    require((co_await field(redis, runtime, "connection_id")) == "legacy", "legacy cleanup erased a route assigned to a different link");
    co_await expectQuiet(redis, subscription, channel);
    (void)co_await command(redis, { "HSET", runtime, "link_id", link });
    co_await CollectorStateService::eraseLegacyRoutes(redis, snapshot, kInstanceId);
    co_await expectMessage(subscription, channel, device);
    for (const auto identity : { "instance_id", "worker_id", "worker_instance_id", "connection_id", "session_epoch", "link_id" }) {
        require((co_await field(redis, runtime, identity)).empty(), "retired legacy identity remained permanent");
    }
    require((co_await field(redis, runtime, "last_report_at_ms")) == "1700000000789" && (co_await field(redis, runtime, "online_until_ms")) == "1700001000789" && (co_await field(redis, runtime, "state")) == "online" && (co_await field(redis, latest, "temperature")) == "{\"value\":21}", "legacy cleanup removed retained telemetry");
    co_await expectQuiet(redis, subscription, channel);
    co_await CollectorStateService::eraseLegacyRoutes(redis, snapshot, kInstanceId);
    co_await expectQuiet(redis, subscription, channel);
}

ruvia::task<void> expireOnWorker(ruvia::event_loop loop, std::uint16_t port) {
    ruvia::redis_config configuration;
    configuration.port_ = port;
    configuration.pool_size_per_worker_ = 1;
    configuration.tls_.mode_ = ruvia::client_tls_mode::disabled;
    ruvia::redis_client client(loop, configuration);
    co_await client.connect();
    {
        const auto redis = client.handle();
        co_await service::telemetry::latest::expireConnections(redis);
    }
    co_await client.shutdown();
}

void expireFromTwoWorkers(std::uint16_t port) {
    ruvia::event_loop_pool workers({ .loop_count_ = 2 });
    workers.start();
    auto first = workers.loop(0).start(expireOnWorker(workers.loop(0), port));
    auto second = workers.loop(1).start(expireOnWorker(workers.loop(1), port));
    first.get();
    second.get();
    workers.stop();
    workers.join();
}

ruvia::task<void> verifyExpiryAndSnapshots(const ruvia::redis_handle& redis, const ruvia::worker_handle& worker, std::uint16_t port) {
    constexpr std::string_view device = "natural-expiry-device";
    constexpr std::string_view link = "natural-expiry-link";
    constexpr std::size_t index = 9;
    const auto route = service::message::collector::connectionKey(device);
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto latest = service::message::telemetry::latestKey(device);
    const auto owner = service::message::collector::linkOwnerKey(link);
    const auto channel = service::message::live::channel(service::message::live::topic::kDeviceState);
    auto subscription = redis.subscription();
    co_await subscribe(subscription, channel);
    const auto workerKey = service::collector::CollectorWorkerRecord::key(index, kInstanceId);
    const auto linkKey = service::collector::CollectorLinkRecord::key(link, index, kInstanceId);
    (void)co_await command(redis, { "SET", owner, kInstanceId, "PX", "60000" });
    (void)co_await command(redis, { "HSET", runtime, "last_report_at_ms", "1700000000999" });
    (void)co_await command(redis, { "HSET", latest, "level", "{\"value\":73}" });
    service::collector::ProtocolAction action;
    action.deviceId = device;
    action.connectionId = "natural-expiry-connection";
    (void)co_await CollectorStateService::bindRoute(redis, action, 1, index, "diagnostic-worker", std::string(link), kInstanceId);
    co_await expectMessage(subscription, channel, device);
    service::message::StreamMessage event{ "1-0", { { "link_id", std::string(link) }, { "worker_instance_id", "diagnostic-worker" }, { "state", "connected" }, { "remote_address", "127.0.0.1:1234" } } };
    co_await CollectorStateService::storeLinkEvent(redis, event, index, kInstanceId);
    co_await CollectorStateService::publishWorker(redis, index, "diagnostic-worker", "version-a", kInstanceId);
    require(integer(co_await command(redis, { "PTTL", workerKey })) > 0 && integer(co_await command(redis, { "PTTL", linkKey })) > 0, "collector diagnostics have no bounded lifetime");
    const auto appliedAt = co_await field(redis, workerKey, "applied_at_ms");
    (void)co_await command(redis, { "PEXPIRE", workerKey, "500" });
    co_await CollectorStateService::renewWorkerSnapshot(redis, index, "old-diagnostic-worker", "version-b", kInstanceId);
    require(integer(co_await command(redis, { "PTTL", workerKey })) <= 500 && (co_await field(redis, workerKey, "applied_at_ms")) == appliedAt && (co_await field(redis, workerKey, "version")) == "version-a", "foreign incarnation renewed or replaced current worker metadata");
    co_await CollectorStateService::renewWorkerSnapshot(redis, index, "diagnostic-worker", "version-b", kInstanceId);
    require(integer(co_await command(redis, { "PTTL", workerKey })) > 10000 && (co_await field(redis, workerKey, "applied_at_ms")) == appliedAt && (co_await field(redis, workerKey, "version")) == "version-a", "worker heartbeat changed the applied timestamp or configuration metadata");
    (void)co_await command(redis, { "DEL", workerKey });
    (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(10));
    co_await CollectorStateService::renewWorkerSnapshot(redis, index, "diagnostic-worker", "version-a", kInstanceId);
    require(integer(co_await command(redis, { "PTTL", workerKey })) > 10000 && (co_await field(redis, workerKey, "worker_instance_id")) == "diagnostic-worker" && (co_await field(redis, workerKey, "applied_at_ms")) != appliedAt, "alive worker did not recreate an absent diagnostic snapshot");
    co_await CollectorStateService::eraseWorker(redis, index, "old-diagnostic-worker", kInstanceId);
    co_await CollectorStateService::eraseLink(redis, link, index, "old-diagnostic-worker", kInstanceId);
    require(integer(co_await command(redis, { "EXISTS", workerKey, linkKey })) == 2, "late worker cleanup erased another incarnation's diagnostics");
    const auto linkUpdatedAt = co_await field(redis, linkKey, "updated_at_ms");
    co_await expectQuiet(redis, subscription, channel);
    (void)co_await command(redis, { "PEXPIRE", linkKey, "500" });
    service::message::StreamMessage foreignEvent{ "2-0", { { "link_id", std::string(link) }, { "worker_instance_id", "old-diagnostic-worker" }, { "state", "incorrect-state" }, { "remote_address", "127.0.0.1:9999" } } };
    co_await CollectorStateService::retainLinkSnapshot(redis, foreignEvent, index, kInstanceId);
    require(integer(co_await command(redis, { "PTTL", linkKey })) <= 500 && (co_await field(redis, linkKey, "state")) == "connected" && (co_await field(redis, linkKey, "updated_at_ms")) == linkUpdatedAt, "late worker renewed or replaced another incarnation's link snapshot");
    service::message::StreamMessage sameWorkerOtherFields{ "3-0", { { "link_id", std::string(link) }, { "worker_instance_id", "diagnostic-worker" }, { "state", "incorrect-state" }, { "remote_address", "127.0.0.1:9999" } } };
    co_await CollectorStateService::retainLinkSnapshot(redis, sameWorkerOtherFields, index, kInstanceId);
    require(integer(co_await command(redis, { "PTTL", linkKey })) > 10000 && (co_await field(redis, linkKey, "state")) == "connected" && (co_await field(redis, linkKey, "remote_address")) == "127.0.0.1:1234" && (co_await field(redis, linkKey, "updated_at_ms")) == linkUpdatedAt, "link heartbeat changed state, metadata or the state event timestamp");
    (void)co_await command(redis, { "DEL", linkKey });
    (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(10));
    co_await CollectorStateService::retainLinkSnapshot(redis, event, index, kInstanceId);
    require(integer(co_await command(redis, { "PTTL", linkKey })) > 10000 && (co_await field(redis, linkKey, "state")) == "connected" && (co_await field(redis, linkKey, "remote_address")) == "127.0.0.1:1234" && (co_await field(redis, linkKey, "worker_instance_id")) == "diagnostic-worker" && (co_await field(redis, linkKey, "updated_at_ms")) != linkUpdatedAt, "quiet link did not rebuild an absent snapshot from its actual worker state");
    co_await expectQuiet(redis, subscription, channel);
    (void)co_await command(redis, { "ZADD", service::message::collector::kConnectionDeadlinesKey, "0", device });
    co_await service::telemetry::latest::expireConnections(redis);
    co_await expectQuiet(redis, subscription, channel);
    require((co_await readConnection(redis, device)).first == "natural-expiry-connection", "an old deadline disconnected a renewed route");
    require((co_await service::telemetry::latest::nextDeadline(redis)).has_value(), "connection expiry was not scheduled in the freshness actor");
    std::cout << "waiting for the actual 15 second Redis lease expiry\n";
    (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(service::message::collector::kConnectionTtlMilliseconds + 100));
    require(integer(co_await command(redis, { "EXISTS", route, workerKey, linkKey })) == 0, "abnormal worker exit retained a route or diagnostic snapshot beyond its lease");
    expireFromTwoWorkers(port);
    co_await expectMessage(subscription, channel, device);
    co_await expectQuiet(redis, subscription, channel);
    co_await service::telemetry::latest::expireConnections(redis);
    co_await expectQuiet(redis, subscription, channel);
    require((co_await readConnection(redis, device)).first.empty() && (co_await field(redis, runtime, "last_report_at_ms")) == "1700000000999" && (co_await field(redis, latest, "level")) == "{\"value\":73}" && integer(co_await command(redis, { "PTTL", runtime })) == -1 && integer(co_await command(redis, { "PTTL", latest })) == -1, "natural connection expiry removed permanent telemetry");
    (void)co_await CollectorStateService::bindRoute(redis, action, 2, index, "diagnostic-worker", std::string(link), kInstanceId);
    co_await expectMessage(subscription, channel, device);
    co_await service::telemetry::latest::eraseDevice(redis, device);
    require(integer(co_await command(redis, { "EXISTS", route, runtime, latest })) == 0 && (co_await command(redis, { "ZSCORE", service::message::collector::kConnectionDeadlinesKey, device })).kind() == ruvia::redis_value::kind_type::null, "device deletion retained its connection lease or deadline");
    co_await CollectorStateService::storeLinkEvent(redis, event, index, kInstanceId);
    co_await CollectorStateService::publishWorker(redis, index, "diagnostic-worker", "version-a", kInstanceId);
    co_await CollectorStateService::eraseWorker(redis, index, "diagnostic-worker", kInstanceId);
    require(integer(co_await command(redis, { "EXISTS", workerKey, linkKey })) == 0, "normal worker stop retained its own diagnostics");
    co_await expectQuiet(redis, subscription, channel);
}

ruvia::task<void> verifyConsumerRecovery(const ruvia::redis_handle& redis, const ruvia::worker_handle& worker) {
    using namespace service::utils::redis;
    const std::vector<std::string> streams{ "consumer-recovery-test" };
    constexpr std::string_view group = "consumer-test-group";
    co_await ensureGroup(redis, streams.front(), group);
    require(integer(co_await redis.command("PTTL", streams.front())) == -1, "persistent consumer group unexpectedly acquired stream expiry");
    ruvia::stop_source stop;
    co_await withConsumerNotifications(redis, worker, streams, group, stop.token(), [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
        const auto first = co_await redis.command("XADD", streams.front(), "*", "value", "pending");
        require(first.kind() == ruvia::redis_value::kind_type::string, "cannot seed pending stream entry");
        const auto owned = co_await readGroupMany(redis, streams, group, "old-owner", ">", 1);
        require(owned.size() == 1 && owned.front().messages.size() == 1, "initial consumer did not own entry");
        (void)co_await redis.command("XCLAIM", streams.front(), group, "old-owner", "0", owned.front().messages.front().id, "IDLE", "60001", "JUSTID");
        const auto fresh = co_await readGroupMany(redis, streams, group, "new-owner", ">", 1);
        require(fresh.empty(), "reading new entries implicitly claimed a pending entry");

        bool recovering = true;
        auto claimed = co_await consumeGroup(redis, streams, group, "new-owner", recovering, notifications, stop.token(), std::chrono::milliseconds(0), 1);
        require(claimed.size() == 1 && claimed.front().messages.front().id == owned.front().messages.front().id, "explicit consumer recovery lost pending work");
        co_await acknowledgeAndDeleteMany(redis, streams.front(), group, claimed.front().messages);

        (void)co_await redis.command("XADD", streams.front(), "*", "value", "new");
        recovering = true;
        auto newEntries = co_await consumeGroup(redis, streams, group, "new-owner", recovering, notifications, stop.token(), std::chrono::milliseconds(0), 1);
        require(newEntries.size() == 1 && newEntries.front().messages.front().get("value") == "new", "an empty pending claim hid newly published work");
        co_await acknowledgeAndDeleteMany(redis, streams.front(), group, newEntries.front().messages);

        (void)co_await redis.command("XGROUP", "DESTROY", streams.front(), group);
        (void)co_await redis.command("XADD", streams.front(), "*", "value", "restored");
        recovering = true;
        auto restored = co_await consumeGroup(redis, streams, group, "new-owner", recovering, notifications, stop.token(), std::chrono::milliseconds(0), 1);
        require(restored.size() == 1 && restored.front().messages.front().get("value") == "restored", "deleted consumer group was not recreated and retried");
        co_await acknowledgeAndDeleteMany(redis, streams.front(), group, restored.front().messages);

        (void)co_await redis.command("ACL", "SETUSER", "default", "-xreadgroup");
        bool aclRejected = false;
        std::exception_ptr aclFailure;
        try {
            recovering = true;
            (void)co_await consumeGroup(redis, streams, group, "new-owner", recovering, notifications, stop.token(), std::chrono::milliseconds(0), 1);
        } catch (const ruvia::redis_error& error) {
            aclRejected = error.code() == ruvia::redis_error::code_type::command_error;
        } catch (...) {
            aclFailure = std::current_exception();
        }
        (void)co_await redis.command("ACL", "SETUSER", "default", "+xreadgroup");
        if (aclFailure) {
            std::rethrow_exception(aclFailure);
        }
        require(aclRejected, "consumer recovery swallowed an ACL-denied read");

        (void)co_await redis.command("DEL", streams.front());
        (void)co_await redis.set(streams.front(), "wrong-type");
        recovering = true;
        bool rejected = false;
        try {
            (void)co_await consumeGroup(redis, streams, group, "new-owner", recovering, notifications, stop.token(), std::chrono::milliseconds(0), 1);
        } catch (const ruvia::redis_error& error) {
            rejected = error.code() == ruvia::redis_error::code_type::command_error;
        }
        require(rejected, "consumer recovery swallowed a wrong-type command failure");
        (void)co_await redis.del(streams.front());
        co_await ensureGroup(redis, streams.front(), group);

        // The owner stops after claiming the last item. No later input or polling
        // wakes its peer: the atomic pending deadline alone must recover the item.
        const std::vector<service::message::StreamField> crashFields{ { "value", "owner-crashed" } };
        (void)co_await add(redis, streams.front(), crashFields);
        bool crashedRecovery = false;
        const auto abandoned = co_await consumeGroup(redis, streams, group, "crashed-owner", crashedRecovery, notifications, stop.token(), std::chrono::milliseconds(0), 1, std::chrono::milliseconds(80));
        require(abandoned.size() == 1, "crashing consumer did not acquire the last entry");
        bool peerRecovery = false;
        const auto claimStarted = std::chrono::steady_clock::now();
        const auto recovered = co_await consumeGroup(redis, streams, group, "surviving-peer", peerRecovery, notifications, stop.token(), std::chrono::seconds(2), 1, std::chrono::milliseconds(80));
        require(recovered.size() == 1 && recovered.front().messages.front().id == abandoned.front().messages.front().id && std::chrono::steady_clock::now() - claimStarted < std::chrono::seconds(2), "owner crash without new input stranded durable pending work");
        co_await acknowledgeAndDeleteMany(redis, streams.front(), group, recovered.front().messages);
        require(integer(co_await redis.command("ZCARD", service::message::consumer::deadlineKey(streams.front(), group))) == 0, "acknowledging a recovered entry left a pending deadline");

        // Eight independent consumers must not occupy the bounded blocking command
        // pool. Their healthy Pub/Sub readers stay open while local deadlines fire.
        std::vector<bool> completed(8, false);
        auto idlePeer = [&](std::size_t index) -> ruvia::task<void> {
            ruvia::stop_source peerStop;
            const auto name = "idle-peer-" + std::to_string(index);
            co_await withConsumerNotifications(redis, worker, streams, group, peerStop.token(), [&](ruvia::channel_receiver<std::uint8_t>& peerNotifications) -> ruvia::task<void> {
                bool replay = true;
                const auto empty = co_await consumeGroup(redis, streams, group, name, replay, peerNotifications, peerStop.token(), std::chrono::milliseconds(20));
                require(empty.empty(), "idle peer consumed an already acknowledged item");
                completed[index] = true;
            });
        };
        ruvia::task_scope peers(worker);
        const auto peersStarted = std::chrono::steady_clock::now();
        for (std::size_t index = 0; index < completed.size(); ++index) {
            peers.spawn(idlePeer(index));
        }
        co_await peers.join();
        for (const auto done : completed) {
            require(done, "a consumer stalled behind the blocking command pool");
        }
        require(std::chrono::steady_clock::now() - peersStarted < std::chrono::seconds(2), "idle consumers serialized behind blocked Redis connections");

        const std::vector<std::string> expiringStreams{ "consumer-expiry-repair-test" };
        constexpr auto streamTtl = std::chrono::seconds(60);
        co_await ensureGroup(redis, expiringStreams.front(), group, streamTtl);
        const auto initialTtl = integer(co_await redis.command("PTTL", expiringStreams.front()));
        require(initialTtl > 0 && initialTtl <= 60000, "initial consumer group creation did not atomically set stream expiry");
        (void)co_await redis.persist(expiringStreams.front());
        co_await ensureGroup(redis, expiringStreams.front(), group, streamTtl);
        require(integer(co_await redis.command("PTTL", expiringStreams.front())) > 0, "existing consumer group renewal did not restore stream expiry");
        co_await withConsumerNotifications(redis, worker, expiringStreams, group, stop.token(), [&](ruvia::channel_receiver<std::uint8_t>& expiryNotifications) -> ruvia::task<void> {
            (void)co_await redis.del(expiringStreams.front());
            bool repair = true;
            const auto empty = co_await consumeGroup(redis, expiringStreams, group, "expiring-owner", repair, expiryNotifications, stop.token(), std::chrono::milliseconds(0), 32, std::chrono::minutes(1), false, streamTtl);
            require(empty.empty(), "repaired empty stream returned unexpected work");
            const auto ttl = integer(co_await redis.command("PTTL", expiringStreams.front()));
            require(ttl > 0 && ttl <= 60000, "missing-group repair recreated an immortal consumer stream");
        });

        const std::vector<std::string> backlogStreams{ "consumer-large-pending-test" };
        co_await ensureGroup(redis, backlogStreams.front(), group);
        constexpr std::string_view kSeedLargePendingScript = R"lua(
for index = 1, 300 do redis.call('XADD', KEYS[1], '*', 'value', tostring(index)) end
return 300
)lua";
        const std::string_view backlogKeys[]{ backlogStreams.front() };
        (void)co_await redis.eval(kSeedLargePendingScript, backlogKeys);
        const auto legacy = co_await readGroupMany(redis, backlogStreams, group, "dead-legacy-owner", ">", 300);
        require(legacy.size() == 1 && legacy.front().messages.size() == 300, "legacy owner did not acquire the multi-page pending backlog");
        co_await withConsumerNotifications(redis, worker, backlogStreams, group, stop.token(), [&](ruvia::channel_receiver<std::uint8_t>& backlogNotifications) -> ruvia::task<void> {
            bool bootstrap = true;
            std::size_t recoveredCount = 0;
            while (recoveredCount < 300) {
                const auto pending = co_await consumeGroup(redis, backlogStreams, group, "backlog-peer", bootstrap, backlogNotifications, stop.token(), std::chrono::seconds(2), 32, std::chrono::milliseconds(0));
                require(!pending.empty(), "pending tail was stranded without a later input wake");
                if (recoveredCount == 0) {
                    require(integer(co_await redis.command("ZCARD", service::message::consumer::deadlineKey(backlogStreams.front(), group))) == 300, "pending bootstrap registered only the first claim batch or XPENDING page");
                }
                for (const auto& batch : pending) {
                    recoveredCount += batch.messages.size();
                    co_await acknowledgeAndDeleteMany(redis, batch.stream, group, batch.messages);
                }
            }
            const auto pendingSummary = co_await redis.command("XPENDING", backlogStreams.front(), group);
            require(recoveredCount == 300 && pendingSummary.array()[0].integer() == 0, "multi-page pending recovery duplicated or abandoned entries");
        });

        ruvia::task_scope cancellation(worker);
        auto cancelRead = [&]() -> ruvia::task<void> {
            (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(20));
            stop.request_stop();
        };
        cancellation.spawn(cancelRead());
        recovering = false;
        const auto started = std::chrono::steady_clock::now();
        try {
            const auto cancelled = co_await consumeGroup(redis, streams, group, "new-owner", recovering, notifications, stop.token());
            require(cancelled.empty(), "cancelled empty read returned unexpected work");
        } catch (const ruvia::redis_error& error) {
            require(error.code() == ruvia::redis_error::code_type::cancelled, "cancellation was reported as an unrelated Redis failure");
        }
        co_await cancellation.join();
        require(stop.token().stop_requested() && std::chrono::steady_clock::now() - started < std::chrono::seconds(2), "consumer cancellation waited for the pending recovery timer");
    });
}

ruvia::task<void> verifyOwnershipDeadlines(const ruvia::redis_handle& redis, const ruvia::worker_handle& worker) {
    using namespace service::collector;
    RuntimeSnapshot desired;
    desired.links.push_back({ .id = "ownership-owned", .protocol = "Modbus" });
    desired.links.push_back({ .id = "ownership-foreign", .protocol = "Modbus" });
    desired.devices.push_back({ .id = "owned-device", .linkId = "ownership-owned" });
    desired.devices.push_back({ .id = "foreign-device", .linkId = "ownership-foreign" });
    const auto ownedKey = service::message::collector::linkOwnerKey("ownership-owned");
    const auto foreignKey = service::message::collector::linkOwnerKey("ownership-foreign");
    (void)co_await command(redis, { "SET", foreignKey, "retiring-process", "PX", "1000" });
    std::map<std::string, ownership::LinkLease, std::less<>> leases;
    const auto initial = co_await ownership::retain(redis, desired, kInstanceId, leases);
    require(initial.snapshot.links.size() == 1 && initial.snapshot.devices.size() == 1 && initial.snapshot.devices.front().id == "owned-device" && initial.ownershipChanged, "link ownership did not filter the foreign link and its devices");
    require(initial.nextLeaseWait && initial.nextLeaseWait->count() > 0 && *initial.nextLeaseWait <= std::chrono::milliseconds(1000), "foreign link takeover was not scheduled at its actual Redis deadline");
    const auto foreignDeadline = leases.at("ownership-foreign").nextAttempt;
    (void)co_await command(redis, { "PEXPIRE", ownedKey, "9000" });
    const auto unchanged = co_await ownership::retain(redis, desired, kInstanceId, leases);
    require(!unchanged.ownershipChanged && leases.at("ownership-foreign").nextAttempt == foreignDeadline && integer(co_await command(redis, { "PTTL", ownedKey })) <= 9000, "a pre-deadline ownership check renewed or re-probed an unrelated link");
    (void)co_await ruvia::sleep_for(worker, *initial.nextLeaseWait + std::chrono::milliseconds(40));
    const auto acquired = co_await ownership::retain(redis, desired, kInstanceId, leases);
    require(acquired.snapshot.links.size() == 2 && acquired.snapshot.devices.size() == 2 && acquired.ownershipChanged && acquired.nextLeaseWait && *acquired.nextLeaseWait <= std::chrono::seconds(5), "the expired foreign process link was not acquired on its actual deadline");
    const auto newOwner = co_await command(redis, { "GET", foreignKey });
    require(newOwner.string() == kInstanceId, "takeover did not persist the new process owner");
    const auto removed = co_await ownership::retain(redis, RuntimeSnapshot{}, kInstanceId, leases);
    require(removed.snapshot.links.empty() && !removed.nextLeaseWait && leases.empty() && removed.ownershipChanged, "an empty configuration retained an ownership wake deadline");
    (void)co_await command(redis, { "DEL", ownedKey, foreignKey });

    RuntimeSnapshot persistent;
    persistent.links.push_back({ .id = "ownership-foreign", .protocol = "Modbus" });
    (void)co_await command(redis, { "SET", foreignKey, "permanent-owner" });
    const auto permanent = co_await ownership::retain(redis, persistent, kInstanceId, leases);
    require(permanent.snapshot.links.empty() && !permanent.nextLeaseWait, "a foreign non-expiring lease produced a negative or busy-loop deadline");
    (void)co_await command(redis, { "DEL", foreignKey });
    leases.clear();
    (void)co_await command(redis, { "HSET", foreignKey, "invalid", "type" });
    bool rejected = false;
    try {
        (void)co_await ownership::retain(redis, persistent, kInstanceId, leases);
    } catch (const std::exception&) {
        rejected = true;
    }
    require(rejected, "an invalid ownership storage type was silently accepted");
    (void)co_await command(redis, { "DEL", foreignKey });
    const auto retried = co_await ownership::retain(redis, persistent, kInstanceId, leases);
    require(retried.snapshot.links.size() == 1 && retried.nextLeaseWait, "failed ownership acquisition poisoned the next recovery attempt");
    (void)co_await command(redis, { "DEL", foreignKey });
}

ruvia::task<void> verifyRoutes(ruvia::event_loop loop, std::uint16_t port) {
    ruvia::redis_config configuration;
    configuration.host_ = "127.0.0.1";
    configuration.port_ = port;
    configuration.tls_.mode_ = ruvia::client_tls_mode::disabled;
    configuration.pool_size_per_worker_ = 1;
    configuration.command_timeout_ = std::chrono::seconds(3);
    ruvia::redis_client client(loop, configuration);
    co_await client.connect();
    std::exception_ptr error;
    {
        const auto redis = client.handle();
        const auto worker = loop.handle();
        try {
            co_await verifyConsumerRecovery(redis, worker);
            co_await verifyReadLease(redis, worker);
            co_await verifyRouteIdentity(redis);
            co_await verifyLegacyCleanup(redis);
            co_await verifyOwnershipDeadlines(redis, worker);
            co_await verifyExpiryAndSnapshots(redis, worker, port);
        } catch (...) {
            error = std::current_exception();
        }
    }
    co_await client.shutdown();
    if (error) {
        std::rethrow_exception(error);
    }
}
} // namespace

int main() {
    if (std::string_view(IOT_TEST_REDIS_SERVER).empty()) {
        std::cout << "collector route Redis integration skipped: redis-server is not installed\n";
        return 77;
    }
    try {
        test::RedisServer redis(IOT_TEST_REDIS_SERVER, IOT_TEST_BUILD_DIRECTORY);
        ruvia::event_loop_pool pool({ .loop_count_ = 1 });
        pool.start();
        pool.loop(0).start(verifyRoutes(pool.loop(0), redis.port())).get();
        pool.stop();
        pool.join();
        std::cout << "collector connection lease Redis integration passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "collector route Redis integration failed: " << error.what() << '\n';
        return 1;
    }
}
