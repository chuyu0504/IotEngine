#include <chrono>
#include <cstdint>
#include <iostream>
#include <initializer_list>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/redis/redis_client.h>

#include "tests/redis_server.fixture.h"
#include "service/features/collector/collector.service.h"
#include "service/features/telemetry/latest/latest.service.h"
#include "service/modules/device/device.entity.h"

namespace {
using service::collector::CollectorStateService;

ruvia::task<ruvia::redis_value> command(const ruvia::redis_handle& redis,
                                      std::initializer_list<std::string_view> arguments) {
    co_return co_await redis.command(std::span(arguments.begin(), arguments.size()));
}

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

std::int64_t integer(const ruvia::redis_value& reply) {
    require(reply.kind() == ruvia::redis_value::kind_type::integer, "Redis integer reply expected");
    return reply.integer();
}

ruvia::task<std::string> field(const ruvia::redis_handle& redis,
                              std::string_view key, std::string_view name) {
    const auto reply = co_await command(redis, {"HGET", key, name});
    if (reply.kind() == ruvia::redis_value::kind_type::null) co_return std::string{};
    require(reply.kind() == ruvia::redis_value::kind_type::string, "Redis hash reply expected");
    co_return std::string(reply.string());
}

ruvia::task<std::pair<std::string, std::string>> readConnection(
        const ruvia::redis_handle& redis, std::string_view device) {
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto route = service::message::collector::connectionKey(device);
    const std::string_view keys[]{runtime, route};
    const std::string_view args[]{service::message::collector::kLinkOwnerPrefix};
    const auto reply = co_await redis.eval(service::device::entities::kDeviceConnectionStateScript, keys, args);
    require(reply.kind() == ruvia::redis_value::kind_type::array && reply.array().size() == 4, "connection state reply contract changed");
    require(reply.array()[0].kind() == ruvia::redis_value::kind_type::string &&
                reply.array()[1].kind() == ruvia::redis_value::kind_type::string,
            "connection state fields are not strings");
    co_return std::pair{std::string(reply.array()[0].string()), std::string(reply.array()[1].string())};
}

ruvia::task<void> seedRoute(const ruvia::redis_handle& redis, std::string_view device,
                           std::string_view connection, std::string_view instance = "healthy-remote") {
    const auto route = service::message::collector::connectionKey(device);
    const auto owner = std::string(service::message::collector::kLinkOwnerPrefix) + "read-link";
    (void)co_await command(redis, {"HSET", route, "connection_id", connection, "instance_id", instance,
        "worker_id", "3", "worker_instance_id", "remote-worker-incarnation", "session_epoch", "11",
        "link_id", "read-link"});
    (void)co_await command(redis, {"PEXPIRE", route, "3000"});
    (void)co_await command(redis, {"SET", owner, instance, "PX", "5000"});
}

ruvia::task<void> verifyReadLease(const ruvia::redis_handle& redis, const ruvia::worker_handle& worker) {
    constexpr std::string_view device = "read-lease-device";
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto route = service::message::collector::connectionKey(device);
    (void)co_await command(redis, {"HSET", runtime, "connection_id", "legacy-connection",
        "instance_id", "retired-instance", "worker_id", "0", "last_report_at_ms", "1700000000123"});
    auto state = co_await readConnection(redis, device);
    require(state.first.empty() && state.second == "1700000000123",
            "legacy permanent connection made an expired device connected or lost its report");
    (void)co_await command(redis, { "HSET", runtime, "state", "offline", "state_reason", "data_stale" });
    {
        const std::string_view keys[]{ runtime, route };
        const std::string_view args[]{ service::message::collector::kLinkOwnerPrefix };
        const auto reply = co_await redis.eval(service::device::entities::kDeviceConnectionStateScript, keys, args);
        require(reply.kind() == ruvia::redis_value::kind_type::array && reply.array().size() == 4 && reply.array()[2].string() == "offline" && reply.array()[3].string() == "data_stale", "connection state omitted the server-computed runtime state");
    }
    co_await seedRoute(redis, device, "remote-connection");
    const auto before = integer(co_await command(redis, {"PTTL", route}));
    state = co_await readConnection(redis, device);
    const auto after = integer(co_await command(redis, {"PTTL", route}));
    require(state.first == "remote-connection" && state.second == "1700000000123",
            "healthy remote process route was rejected");
    require(before > 0 && after > 0 && after <= before,
            "reading connection state renewed its lease");
    for (const auto identity : {"connection_id", "instance_id", "worker_id", "worker_instance_id",
                                "session_epoch", "link_id"}) {
        co_await seedRoute(redis, device, "remote-connection");
        (void)co_await command(redis, {"HDEL", route, identity});
        state = co_await readConnection(redis, device);
        require(state.first.empty() && state.second == "1700000000123",
                "incomplete connection identity was accepted");
    }
    for (const auto& [identity, value] : {
            std::pair<std::string_view, std::string_view>{"worker_id", "worker-three"},
            {"worker_id", "-1"}, {"session_epoch", "0"}, {"session_epoch", "epoch-eleven"}}) {
        co_await seedRoute(redis, device, "remote-connection");
        (void)co_await command(redis, {"HSET", route, identity, value});
        require((co_await readConnection(redis, device)).first.empty(),
                "malformed worker or session identity was accepted");
    }
    co_await seedRoute(redis, device, "remote-connection");
    (void)co_await command(redis, {"PERSIST", route});
    require((co_await readConnection(redis, device)).first.empty(),
            "permanent route hash was accepted as a live connection");
    co_await seedRoute(redis, device, "remote-connection");
    const auto owner = std::string(service::message::collector::kLinkOwnerPrefix) + "read-link";
    (void)co_await command(redis, {"SET", owner, "replacement-instance", "PX", "5000"});
    require((co_await readConnection(redis, device)).first.empty(),
            "link ownership replacement kept an old connection live");
    co_await seedRoute(redis, device, "remote-connection");
    (void)co_await command(redis, {"PEXPIRE", route, "1"});
    (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(40));
    state = co_await readConnection(redis, device);
    require(state.first.empty() && state.second == "1700000000123",
            "expired connection route survived or telemetry report disappeared");
    require(integer(co_await command(redis, {"PTTL", runtime})) == -1,
            "connection expiry expired the permanent telemetry runtime");
}

ruvia::task<void> verifyRouteIdentity(const ruvia::redis_handle& redis) {
    constexpr std::string_view device = "route-identity-device";
    constexpr std::string_view link = "route-identity-link";
    const auto route = service::message::collector::connectionKey(device);
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto latest = service::message::telemetry::latestKey(device);
    const auto owner = service::message::collector::linkOwnerKey(link);
    const auto& instance = service::runtime::instanceId();
    (void)co_await command(redis, {"SET", owner, instance, "PX", "60000"});
    (void)co_await command(redis, {"HSET", runtime, "worker_id", "7", "instance_id", "retired-instance",
        "connection_id", "retired-connection", "link_id", link, "last_report_at_ms", "1700000000456",
        "online_until_ms", "1700001000456", "state", "online"});
    (void)co_await command(redis, {"HSET", latest, "pressure", "{\"value\":42}", "_state", "retained-telemetry-state"});
    service::collector::ProtocolAction action;
    action.deviceId = device;
    action.deviceCode = "ROUTE-TEST";
    action.connectionId = "connection-a";
    const auto previous = co_await CollectorStateService::bindRoute(redis, action, 11, 3, "worker-a", std::string(link));
    require(previous.connectionId == "retired-connection" && previous.workerId == "7" &&
                previous.instanceId == "retired-instance", "binding lost the previous route owner");
    require((co_await readConnection(redis, device)).first == "connection-a", "bound connection is not visible");
    const auto ttl = integer(co_await command(redis, {"PTTL", route}));
    require(ttl > 0 && ttl <= service::message::collector::kConnectionTtlMilliseconds,
            "connection route did not receive its bounded lease");
    require((co_await field(redis, runtime, "connection_id")).empty() &&
                (co_await field(redis, runtime, "last_report_at_ms")) == "1700000000456" &&
                (co_await field(redis, latest, "pressure")) == "{\"value\":42}",
            "binding retained legacy route identity or lost telemetry");
    (void)co_await command(redis, {"PEXPIRE", route, "500"});
    require(co_await CollectorStateService::renewRoute(redis, device, "connection-a", 11, 3, "worker-a"),
            "current connection could not renew its lease");
    require(integer(co_await command(redis, {"PTTL", route})) > 10000,
            "successful renewal did not restore the route TTL");
    (void)co_await CollectorStateService::bindRoute(redis, action, 12, 3, "worker-b", std::string(link));
    const auto events = integer(co_await command(redis, {"XLEN", service::message::live::kChanges}));
    for (const auto& [epoch, workerIndex, incarnation] : {
            std::tuple<std::uint64_t, std::size_t, std::string_view>{11, 3, "worker-b"},
            {12, 4, "worker-b"}, {12, 3, "worker-a"}}) {
        require(!(co_await CollectorStateService::renewRoute(redis, device, "connection-a", epoch,
                    workerIndex, incarnation)), "stale identity renewed a replacement connection");
        co_await CollectorStateService::markDeviceOffline(redis, device, "connection-a", epoch,
                                                         workerIndex, incarnation, "late_disconnect");
    }
    require(!(co_await CollectorStateService::renewRoute(redis, device, "old-connection", 12, 3, "worker-b")),
            "old connection renewed a replacement connection");
    co_await CollectorStateService::markDeviceOffline(redis, device, "old-connection", 12, 3,
                                                     "worker-b", "late_disconnect");
    const std::string_view keys[]{route, service::message::collector::kConnectionDeadlinesKey};
    const std::string_view arguments[]{"retired-instance", "3", "worker-b", "connection-a", "12", "15000",
        service::message::collector::kLinkOwnerPrefix, device};
    require(integer(co_await redis.eval(service::collector::CollectorDeviceRouteRecord::renewScript,
                                       keys, arguments)) == 0, "foreign process renewed a live route");
    const std::string_view eraseKeys[]{route, runtime, service::message::live::kChanges,
        service::message::collector::kConnectionDeadlinesKey};
    const std::string_view eraseArguments[]{"retired-instance", "3", "worker-b", "connection-a", "12",
        "late_disconnect", "1700000001000", device};
    require(integer(co_await redis.eval(service::collector::CollectorDeviceRouteRecord::eraseScript,
                                       eraseKeys, eraseArguments)) == 0,
            "foreign process erased a live route");
    bool staleBindRejected = false;
    try { (void)co_await CollectorStateService::bindRoute(redis, action, 11, 3, "worker-b", std::string(link)); }
    catch (const std::runtime_error&) { staleBindRejected = true; }
    require((co_await field(redis, route, "session_epoch")) == "12" &&
                (co_await field(redis, route, "worker_instance_id")) == "worker-b" &&
                staleBindRejected &&
                integer(co_await command(redis, {"XLEN", service::message::live::kChanges})) == events,
            "late bind or cleanup replaced the new epoch or published a false disconnection");
    (void)co_await command(redis, {"SET", owner, "replacement-process", "PX", "60000"});
    require(!(co_await CollectorStateService::renewRoute(redis, device, "connection-a", 12, 3, "worker-b")),
            "lost link owner continued to renew the route");
    bool rejected = false;
    try { (void)co_await CollectorStateService::bindRoute(redis, action, 13, 3, "worker-b", std::string(link)); }
    catch (const std::runtime_error&) { rejected = true; }
    require(rejected && (co_await field(redis, route, "session_epoch")) == "12",
            "lost link owner bound a route");
    (void)co_await command(redis, {"SET", owner, instance, "PX", "60000"});
    co_await CollectorStateService::markDeviceOffline(redis, device, "connection-a", 12, 3,
                                                     "worker-b", "connection_closed");
    require(integer(co_await command(redis, {"EXISTS", route})) == 0 &&
                (co_await readConnection(redis, device)).first.empty(), "disconnect did not erase its owned route");
    require((co_await command(redis, {"ZSCORE", service::message::collector::kConnectionDeadlinesKey, device})).kind() ==
                ruvia::redis_value::kind_type::null, "disconnect left a scheduled connection deadline");
    const auto disconnectedEvents = integer(co_await command(redis, {"XLEN", service::message::live::kChanges}));
    co_await CollectorStateService::markDeviceOffline(redis, device, "connection-a", 12, 3,
                                                     "worker-b", "duplicate_disconnect");
    require(integer(co_await command(redis, {"XLEN", service::message::live::kChanges})) == disconnectedEvents,
            "duplicate disconnect published a duplicate business event");
    require(!(co_await CollectorStateService::renewRoute(redis, device, "connection-a", 12, 3, "worker-b")),
            "disconnected connection was revived by renewal");
    require((co_await field(redis, runtime, "last_report_at_ms")) == "1700000000456" &&
                (co_await field(redis, latest, "pressure")) == "{\"value\":42}" &&
                (co_await field(redis, latest, "_state")) == "retained-telemetry-state",
            "disconnect altered the latest telemetry");
}

ruvia::task<void> verifyLegacyCleanup(const ruvia::redis_handle& redis) {
    constexpr std::string_view device = "legacy-cleanup-device";
    constexpr std::string_view link = "legacy-cleanup-link";
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto latest = service::message::telemetry::latestKey(device);
    const auto owner = service::message::collector::linkOwnerKey(link);
    service::collector::RuntimeSnapshot snapshot;
    service::collector::DeviceDefinition definition;
    definition.id = device;
    definition.linkId = link;
    snapshot.devices.push_back(definition);
    (void)co_await command(redis, {"HSET", runtime, "connection_id", "legacy", "instance_id", "old-process",
        "worker_id", "0", "worker_instance_id", "old-worker", "session_epoch", "3", "link_id", link,
        "last_report_at_ms", "1700000000789", "online_until_ms", "1700001000789", "state", "online"});
    (void)co_await command(redis, {"HSET", latest, "temperature", "{\"value\":21}"});
    (void)co_await command(redis, {"SET", owner, "healthy-remote", "PX", "60000"});
    co_await CollectorStateService::eraseLegacyRoutes(redis, snapshot);
    require((co_await field(redis, runtime, "connection_id")) == "legacy",
            "another healthy process had its legacy identity erased");
    (void)co_await command(redis, {"SET", owner, service::runtime::instanceId(), "PX", "60000"});
    (void)co_await command(redis, {"HSET", runtime, "link_id", "different-link"});
    co_await CollectorStateService::eraseLegacyRoutes(redis, snapshot);
    require((co_await field(redis, runtime, "connection_id")) == "legacy",
            "legacy cleanup erased a route assigned to a different link");
    (void)co_await command(redis, {"HSET", runtime, "link_id", link});
    co_await CollectorStateService::eraseLegacyRoutes(redis, snapshot);
    for (const auto identity : {"instance_id", "worker_id", "worker_instance_id", "connection_id", "session_epoch", "link_id"})
        require((co_await field(redis, runtime, identity)).empty(), "retired legacy identity remained permanent");
    require((co_await field(redis, runtime, "last_report_at_ms")) == "1700000000789" &&
                (co_await field(redis, runtime, "online_until_ms")) == "1700001000789" &&
                (co_await field(redis, runtime, "state")) == "online" &&
                (co_await field(redis, latest, "temperature")) == "{\"value\":21}",
            "legacy cleanup removed retained telemetry");
    const auto events = integer(co_await command(redis, {"XLEN", service::message::live::kChanges}));
    co_await CollectorStateService::eraseLegacyRoutes(redis, snapshot);
    require(integer(co_await command(redis, {"XLEN", service::message::live::kChanges})) == events,
            "repeated legacy cleanup published a duplicate business event");
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
    ruvia::event_loop_pool workers({.loop_count_ = 2});
    workers.start();
    auto first = workers.loop(0).start(expireOnWorker(workers.loop(0), port));
    auto second = workers.loop(1).start(expireOnWorker(workers.loop(1), port));
    first.get();
    second.get();
    workers.stop();
    workers.join();
}

ruvia::task<void> verifyExpiryAndSnapshots(const ruvia::redis_handle& redis,
        const ruvia::worker_handle& worker, std::uint16_t port) {
    constexpr std::string_view device = "natural-expiry-device";
    constexpr std::string_view link = "natural-expiry-link";
    constexpr std::size_t index = 9;
    const auto route = service::message::collector::connectionKey(device);
    const auto runtime = service::message::telemetry::runtimeKey(device);
    const auto latest = service::message::telemetry::latestKey(device);
    const auto owner = service::message::collector::linkOwnerKey(link);
    const auto workerKey = service::collector::CollectorWorkerRecord::key(index, service::runtime::instanceId());
    const auto linkKey = service::collector::CollectorLinkRecord::key(link, index, service::runtime::instanceId());
    (void)co_await command(redis, {"SET", owner, service::runtime::instanceId(), "PX", "60000"});
    (void)co_await command(redis, {"HSET", runtime, "last_report_at_ms", "1700000000999"});
    (void)co_await command(redis, {"HSET", latest, "level", "{\"value\":73}"});
    service::collector::ProtocolAction action;
    action.deviceId = device;
    action.connectionId = "natural-expiry-connection";
    (void)co_await CollectorStateService::bindRoute(redis, action, 1, index, "diagnostic-worker", std::string(link));
    service::message::StreamMessage event{"1-0", {{"link_id", std::string(link)},
        {"worker_instance_id", "diagnostic-worker"}, {"state", "connected"},
        {"remote_address", "127.0.0.1:1234"}}};
    co_await CollectorStateService::storeLinkEvent(redis, event, index);
    co_await CollectorStateService::publishWorker(redis, index, "diagnostic-worker", "version-a");
    require(integer(co_await command(redis, {"PTTL", workerKey})) > 0 &&
                integer(co_await command(redis, {"PTTL", linkKey})) > 0,
            "collector diagnostics have no bounded lifetime");
    const auto appliedAt = co_await field(redis, workerKey, "applied_at_ms");
    (void)co_await command(redis, {"PEXPIRE", workerKey, "500"});
    co_await CollectorStateService::renewWorkerSnapshot(redis, index, "old-diagnostic-worker", "version-b");
    require(integer(co_await command(redis, {"PTTL", workerKey})) <= 500 &&
                (co_await field(redis, workerKey, "applied_at_ms")) == appliedAt &&
                (co_await field(redis, workerKey, "version")) == "version-a",
            "foreign incarnation renewed or replaced current worker metadata");
    co_await CollectorStateService::renewWorkerSnapshot(redis, index, "diagnostic-worker", "version-b");
    require(integer(co_await command(redis, {"PTTL", workerKey})) > 10000 &&
                (co_await field(redis, workerKey, "applied_at_ms")) == appliedAt &&
                (co_await field(redis, workerKey, "version")) == "version-a",
            "worker heartbeat changed the applied timestamp or configuration metadata");
    (void)co_await command(redis, {"DEL", workerKey});
    (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(10));
    co_await CollectorStateService::renewWorkerSnapshot(redis, index, "diagnostic-worker", "version-a");
    require(integer(co_await command(redis, {"PTTL", workerKey})) > 10000 &&
                (co_await field(redis, workerKey, "worker_instance_id")) == "diagnostic-worker" &&
                (co_await field(redis, workerKey, "applied_at_ms")) != appliedAt,
            "alive worker did not recreate an absent diagnostic snapshot");
    co_await CollectorStateService::eraseWorker(redis, index, "old-diagnostic-worker");
    co_await CollectorStateService::eraseLink(redis, link, index, "old-diagnostic-worker");
    require(integer(co_await command(redis, {"EXISTS", workerKey, linkKey})) == 2,
            "late worker cleanup erased another incarnation's diagnostics");
    const auto linkUpdatedAt = co_await field(redis, linkKey, "updated_at_ms");
    const auto linkEvents = integer(co_await command(redis, {"XLEN", service::message::live::kChanges}));
    (void)co_await command(redis, {"PEXPIRE", linkKey, "500"});
    service::message::StreamMessage foreignEvent{"2-0", {{"link_id", std::string(link)},
        {"worker_instance_id", "old-diagnostic-worker"}, {"state", "incorrect-state"},
        {"remote_address", "127.0.0.1:9999"}}};
    co_await CollectorStateService::retainLinkSnapshot(redis, foreignEvent, index);
    require(integer(co_await command(redis, {"PTTL", linkKey})) <= 500 &&
                (co_await field(redis, linkKey, "state")) == "connected" &&
                (co_await field(redis, linkKey, "updated_at_ms")) == linkUpdatedAt,
            "late worker renewed or replaced another incarnation's link snapshot");
    service::message::StreamMessage sameWorkerOtherFields{"3-0", {{"link_id", std::string(link)},
        {"worker_instance_id", "diagnostic-worker"}, {"state", "incorrect-state"},
        {"remote_address", "127.0.0.1:9999"}}};
    co_await CollectorStateService::retainLinkSnapshot(redis, sameWorkerOtherFields, index);
    require(integer(co_await command(redis, {"PTTL", linkKey})) > 10000 &&
                (co_await field(redis, linkKey, "state")) == "connected" &&
                (co_await field(redis, linkKey, "remote_address")) == "127.0.0.1:1234" &&
                (co_await field(redis, linkKey, "updated_at_ms")) == linkUpdatedAt,
            "link heartbeat changed state, metadata or the state event timestamp");
    (void)co_await command(redis, {"DEL", linkKey});
    (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(10));
    co_await CollectorStateService::retainLinkSnapshot(redis, event, index);
    require(integer(co_await command(redis, {"PTTL", linkKey})) > 10000 &&
                (co_await field(redis, linkKey, "state")) == "connected" &&
                (co_await field(redis, linkKey, "remote_address")) == "127.0.0.1:1234" &&
                (co_await field(redis, linkKey, "worker_instance_id")) == "diagnostic-worker" &&
                (co_await field(redis, linkKey, "updated_at_ms")) != linkUpdatedAt,
            "quiet link did not rebuild an absent snapshot from its actual worker state");
    require(integer(co_await command(redis, {"XLEN", service::message::live::kChanges})) == linkEvents,
            "link snapshot renewal or recovery published a duplicate business event");
    (void)co_await command(redis, {"ZADD", service::message::collector::kConnectionDeadlinesKey, "0", device});
    const auto events = integer(co_await command(redis, {"XLEN", service::message::live::kChanges}));
    co_await service::telemetry::latest::expireConnections(redis);
    require(integer(co_await command(redis, {"XLEN", service::message::live::kChanges})) == events &&
                (co_await readConnection(redis, device)).first == "natural-expiry-connection",
            "an old deadline published a false disconnect for a renewed route");
    require((co_await service::telemetry::latest::nextDeadline(redis)).has_value(),
            "connection expiry was not scheduled in the freshness actor");
    std::cout << "waiting for the actual 15 second Redis lease expiry\n";
    (void)co_await ruvia::sleep_for(worker,
        std::chrono::milliseconds(service::message::collector::kConnectionTtlMilliseconds + 100));
    require(integer(co_await command(redis, {"EXISTS", route, workerKey, linkKey})) == 0,
            "abnormal worker exit retained a route or diagnostic snapshot beyond its lease");
    expireFromTwoWorkers(port);
    require(integer(co_await command(redis, {"XLEN", service::message::live::kChanges})) == events + 1,
            "two workers failed to publish exactly one real connection expiry event");
    co_await service::telemetry::latest::expireConnections(redis);
    require(integer(co_await command(redis, {"XLEN", service::message::live::kChanges})) == events + 1,
            "repeated expiry published duplicate disconnect events");
    require((co_await readConnection(redis, device)).first.empty() &&
                (co_await field(redis, runtime, "last_report_at_ms")) == "1700000000999" &&
                (co_await field(redis, latest, "level")) == "{\"value\":73}" &&
                integer(co_await command(redis, {"PTTL", runtime})) == -1 &&
                integer(co_await command(redis, {"PTTL", latest})) == -1,
            "natural connection expiry removed permanent telemetry");
    (void)co_await CollectorStateService::bindRoute(redis, action, 2, index, "diagnostic-worker", std::string(link));
    co_await service::telemetry::latest::eraseDevice(redis, device);
    require(integer(co_await command(redis, {"EXISTS", route, runtime, latest})) == 0 &&
                (co_await command(redis, {"ZSCORE", service::message::collector::kConnectionDeadlinesKey, device})).kind() ==
                    ruvia::redis_value::kind_type::null, "device deletion retained its connection lease or deadline");
    co_await CollectorStateService::storeLinkEvent(redis, event, index);
    co_await CollectorStateService::publishWorker(redis, index, "diagnostic-worker", "version-a");
    co_await CollectorStateService::eraseWorker(redis, index, "diagnostic-worker");
    require(integer(co_await command(redis, {"EXISTS", workerKey, linkKey})) == 0,
            "normal worker stop retained its own diagnostics");
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
            co_await verifyReadLease(redis, worker);
            co_await verifyRouteIdentity(redis);
            co_await verifyLegacyCleanup(redis);
            co_await verifyExpiryAndSnapshots(redis, worker, port);
        } catch (...) {
            error = std::current_exception();
        }
    }
    co_await client.shutdown();
    if (error) std::rethrow_exception(error);
}
} // namespace

int main() {
    if (std::string_view(IOT_TEST_REDIS_SERVER).empty()) {
        std::cout << "collector route Redis integration skipped: redis-server is not installed\n";
        return 77;
    }
    try {
        test::RedisServer redis(IOT_TEST_REDIS_SERVER, IOT_TEST_BUILD_DIRECTORY);
        ruvia::event_loop_pool pool({.loop_count_ = 1});
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
