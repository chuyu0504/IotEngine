#include "service/middleware/api_upload.h"
#include "service/middleware/validation.h"
#include "service/features/edge/edge.config.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <ruvia/web/app.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_migration.h>
#include <ruvia/web/error.h>
#include <ruvia/web/redis/redis.h>

#include "service/common/http.h"
#include "service/common/worker.h"
#include "service/features/packet_log/packet_log.transport.h"
#include "service/config/lifecycle.h"
#include "service/config/schema.h"
#include "service/config/storage.h"
#include "service/features/access/access.runtime.h"
#include "service/features/alert/alert.runtime.h"
#include "service/features/collector/collector.runtime.h"
#include "service/features/command/command.runtime.h"
#include "service/features/command/command.service.h"
#include "service/features/configuration/configuration.runtime.h"
#include "service/features/configuration/configuration.service.h"
#include "service/features/edge/edge.runtime.h"
#include "service/features/edge/gateway/gateway.runtime.h"
#include "service/features/messaging/messaging.runtime.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"
#include "service/features/gb28181/gb28181.runtime.h"
#include "service/features/gb28181/media/media.transport.h"
#include "service/features/live/live.runtime.h"
#include "service/features/telemetry/telemetry.runtime.h"
#include "service/features/telemetry/telemetry.service.h"
#include "service/features/vpn/vpn.runtime.h"
#include "service/middleware/live.h"
#include "service/modules/alert/alert.controller.h"
#include "service/modules/device/device.controller.h"
#include "service/modules/command/command.controller.h"
#include "service/modules/edge_node/edge_node.controller.h"
#include "service/modules/gb28181/gb28181.controller.h"
#include "service/modules/link/link.controller.h"
#include "service/modules/open_access/open_access.controller.h"
#include "service/modules/protocol/protocol.controller.h"
#include "service/modules/system/auth/auth.controller.h"
#include "service/modules/system/dept/dept.controller.h"
#include "service/modules/system/operations/operations.controller.h"
#include "service/modules/system/role/role.controller.h"
#include "service/modules/system/user/user.controller.h"
#include "service/modules/vpn/vpn.controller.h"

namespace {

template <typename String>
void assign(String& target, std::optional<std::string_view> value) {
    if (value) {
        target.assign(*value);
    }
}

ruvia::db_config databaseConfig(const ruvia::env& env) {
    ruvia::db_config config;
    config.driver_ = ruvia::db_driver::postgresql;
    config.tls_.mode_ = ruvia::client_tls_mode::disabled;
    assign(config.host_, env.get("DB_HOST"));
    assign(config.username_, env.get("DB_USERNAME"));
    assign(config.password_, env.get("DB_PASSWORD"));
    assign(config.database_, env.get("DB_DATABASE"));
    config.port_ = env.get<std::uint16_t>("DB_PORT").value_or(5432);
    config.acquire_timeout_ = std::chrono::seconds(2);
    config.connect_timeout_ = std::chrono::seconds(5);
    config.query_timeout_ = std::chrono::seconds(30);
    return config;
}

ruvia::redis_config redisConfig(const ruvia::env& env) {
    ruvia::redis_config config;
    config.tls_.mode_ = ruvia::client_tls_mode::disabled;
    assign(config.host_, env.get("REDIS_HOST"));
    assign(config.password_, env.get("REDIS_PASSWORD"));
    config.port_ = env.get<std::uint16_t>("REDIS_PORT").value_or(6379);
    config.database_ = env.get<std::uint32_t>("REDIS_DATABASE").value_or(0);
    const auto poolSize = env.get<unsigned>("REDIS_POOL_SIZE_PER_WORKER").value_or(2U);
    if (poolSize == 0U || poolSize > 16U) {
        throw std::runtime_error("REDIS_POOL_SIZE_PER_WORKER must be between 1 and 16");
    }
    config.pool_size_per_worker_ = poolSize;
    return config;
}

service::vpn::wireguard::HubConfig vpnHubConfig(const ruvia::env& env) {
    service::vpn::wireguard::HubConfig config;
    assign(config.interfaceName, env.get("VPN_HUB_INTERFACE"));
    assign(config.privateKey, env.get("VPN_HUB_PRIVATE_KEY"));
    assign(config.publicKey, env.get("VPN_HUB_PUBLIC_KEY"));
    assign(config.endpoint, env.get("VPN_HUB_ENDPOINT"));
    config.listenPort = env.get<std::uint16_t>("VPN_HUB_LISTEN_PORT").value_or(51820);
    return config;
}

std::filesystem::path runtimeDirectory(const char* executable) {
    if (!executable || *executable == '\0') {
        return std::filesystem::current_path();
    }
    std::error_code error;
    auto path = std::filesystem::weakly_canonical(std::filesystem::absolute(executable), error);
    if (error) {
        path = std::filesystem::absolute(executable);
    }
    return path.parent_path();
}

service::packet_log::Config packetLogConfig(const ruvia::env& env, const std::filesystem::path& runtime) {
    service::packet_log::Config config;
    config.directory = std::filesystem::path(env.get("PACKET_LOG_DIRECTORY").value_or((runtime / "logs").string()));
    config.level = service::packet_log::parseLevel(env.get("PACKET_LOG_LEVEL").value_or("DEBUG"));
    return config;
}

bool envFlag(const ruvia::env& env, std::string_view name, bool fallback = false) {
    const auto value = env.get(name);
    if (!value) {
        return fallback;
    }
    std::string normalized(*value);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on";
}

AppConfig gb28181Config(const ruvia::env& env) {
    AppConfig config;
    config.enabled = envFlag(env, "GB28181_ENABLED");
    config.sip.domain = std::string(env.get("GB28181_SIP_DOMAIN").value_or(""));
    config.sip.id = std::string(env.get("GB28181_SIP_ID").value_or(""));
    config.sip.host = std::string(env.get("GB28181_SIP_HOST").value_or("0.0.0.0"));
    config.sip.publicIp = std::string(env.get("GB28181_SIP_PUBLIC_IP").value_or(""));
    config.sip.port = env.get<std::uint16_t>("GB28181_SIP_PORT").value_or(5060);
    config.sip.password = std::string(env.get("GB28181_SIP_PASSWORD").value_or(""));
    config.sip.transport = std::string(env.get("GB28181_SIP_TRANSPORT").value_or("udp"));
    config.sip.registrationTimeoutSeconds = env.get<int>("GB28181_REGISTRATION_TIMEOUT_SECONDS").value_or(180);
    config.sip.inviteTimeoutSeconds = env.get<int>("GB28181_INVITE_TIMEOUT_SECONDS").value_or(15);
    config.sip.viewerLeaseTimeoutSeconds = env.get<int>("GB28181_VIEWER_LEASE_TIMEOUT_SECONDS").value_or(90);
    config.sip.nonceTtlSeconds = env.get<int>("GB28181_NONCE_TTL_SECONDS").value_or(300);
    config.sip.logging = envFlag(env, "GB28181_SIP_LOGGING", true);
    config.media.zlmPublicBaseUrl = std::string(env.get("ZLM_PUBLIC_BASE_URL").value_or(""));
    config.media.rtpPublicIp = std::string(env.get("GB28181_RTP_PUBLIC_IP").value_or(""));
    config.media.playTokenSecret = std::string(env.get("GB28181_MEDIA_TOKEN_SECRET").value_or(""));
    config.media.playTokenTtlSeconds = env.get<int>("GB28181_MEDIA_TOKEN_TTL_SECONDS").value_or(300);
    config.media.corsOrigin = std::string(env.get("GB28181_MEDIA_CORS_ORIGIN").value_or(""));
    config.media.workerThreads = env.get<int>("ZLM_WORKER_THREADS").value_or(1);
    config.media.logLevel = env.get<int>("ZLM_LOG_LEVEL").value_or(2);
    config.media.httpPort = env.get<std::uint16_t>("ZLM_HTTP_PORT").value_or(8080);
    config.media.httpsPort = env.get<std::uint16_t>("ZLM_HTTPS_PORT").value_or(8443);
    config.media.rtspPort = env.get<std::uint16_t>("ZLM_RTSP_PORT").value_or(8554);
    config.media.rtspsPort = env.get<std::uint16_t>("ZLM_RTSPS_PORT").value_or(8322);
    config.media.rtmpPort = env.get<std::uint16_t>("ZLM_RTMP_PORT").value_or(1935);
    config.media.rtmpsPort = env.get<std::uint16_t>("ZLM_RTMPS_PORT").value_or(1936);
    config.media.rtcPort = env.get<std::uint16_t>("ZLM_RTC_PORT").value_or(8000);
    config.media.srtPort = env.get<std::uint16_t>("ZLM_SRT_PORT").value_or(9000);
    config.media.rtpPortRangeStart = env.get<std::uint16_t>("GB28181_RTP_PORT_START").value_or(30000);
    config.media.rtpPortRangeEnd = env.get<std::uint16_t>("GB28181_RTP_PORT_END").value_or(30500);
    config.media.tlsEnabled = envFlag(env, "ZLM_TLS_ENABLED");
    config.media.tlsPemPath = std::string(env.get("ZLM_TLS_PEM_PATH").value_or(""));
    config.media.tlsPassword = std::string(env.get("ZLM_TLS_PASSWORD").value_or(""));
    config.media.recordingEnabled = envFlag(env, "GB28181_RECORDING_ENABLED");
    config.media.recordRoot = std::string(env.get("GB28181_RECORD_ROOT").value_or(""));
    config.media.recordMaxSegmentSeconds = env.get<std::uint32_t>("GB28181_RECORD_MAX_SEGMENT_SECONDS").value_or(3600);
    return config;
}

struct CommandLineOptions {
    bool migrateOnly{};
};

CommandLineOptions parseCommandLine(int argc, char* argv[]) {
    const bool migrateOnly = argc == 2 && std::string_view(argv[1]) == "--migrate-only";
    if (argc > 1 && !migrateOnly) {
        throw std::invalid_argument("usage: server [--migrate-only]");
    }
    return CommandLineOptions{ .migrateOnly = migrateOnly };
}

void validateEdgeConfiguration(const ruvia::env& env) {
    const auto platformId = env.get("EDGE_PLATFORM_ID").value_or(service::message::edge::kDefaultPlatformId);
    if (!service::edge::config::validPlatformId(platformId)) {
        throw std::runtime_error("EDGE_PLATFORM_ID is invalid");
    }
    const auto publicBaseUrl = env.get("EDGE_PUBLIC_BASE_URL").value_or(service::message::edge::kDefaultPublicBaseUrl);
    if (!service::edge::config::validPublicBaseUrl(publicBaseUrl)) {
        throw std::runtime_error("EDGE_PUBLIC_BASE_URL is invalid");
    }
}

ruvia::db_config migrateDatabase(
    const ruvia::env& env
) {
    auto db = databaseConfig(env);
    const auto storagePolicy = service::config::deviceDataStoragePolicy(env);
    std::vector<ruvia::db_migration> migrations;
    migrations.reserve(service::config::kSchemaMigrations.size() + 1);
    migrations.insert(migrations.end(), service::config::kSchemaMigrations.begin(), service::config::kSchemaMigrations.end());
    migrations.emplace_back(service::config::deviceDataStoragePolicyMigration(storagePolicy));
    ruvia::db_migrator_options migrationOptions;
    migrationOptions.table_ = "sys_schema_migrations";
    const auto report = ruvia::db_migrator::migrate(db, migrations, std::move(migrationOptions));
    std::cout << "database migrations: applied=" << report.applied().size()
              << ", skipped=" << report.skipped().size() << '\n';
    std::cout << "device_data storage policy: chunk="
              << storagePolicy.chunkIntervalHours << "h, compression="
              << (storagePolicy.compressionEnabled
                      ? std::to_string(storagePolicy.compressionAfterHours) + "h"
                      : "disabled")
              << ", mutable-window=" << storagePolicy.mutableWindowHours << "h\n";
    return db;
}

std::size_t resolveWorkerCount(
    std::optional<unsigned> configured,
    unsigned automatic,
    const char* name
) {
    const auto count = configured.value_or(automatic);
    if (count == 0U || count > 64U) {
        throw std::runtime_error(std::string(name) + " must be between 1 and 64");
    }
    return static_cast<std::size_t>(count);
}

struct WorkerBudget {
    unsigned cpu{};
    unsigned gb28181{};
    std::size_t service{};
    std::size_t collector{};
};

WorkerBudget workerBudget(
    const ruvia::env& env,
    const AppConfig& gb28181
) {
    const auto cpu = std::max(2U, std::thread::hardware_concurrency());
    const auto mediaWorkers = static_cast<unsigned>(std::max(1, gb28181.media.workerThreads));
    // SIP runs inside Collector Workers and HTTP media forwarding inside Service
    // Workers. Only ZLM's internal pools reserve additional threads.
    const auto gb28181WorkerCount = gb28181.enabled ? 2U * mediaWorkers : 0U;
    // Service and Collector each need at least one worker. On a host with
    // fewer CPUs than that hard minimum plus the enabled media runtime,
    // controlled oversubscription is unavoidable and remains explicit.
    const auto businessCpu = std::max(2U, cpu > gb28181WorkerCount ? cpu - gb28181WorkerCount : 0U);
    return WorkerBudget{
        .cpu = cpu,
        .gb28181 = gb28181WorkerCount,
        .service = resolveWorkerCount(
            env.get<unsigned>("SERVICE_WORKERS"),
            (businessCpu + 1U) / 2U,
            "SERVICE_WORKERS"
        ),
        .collector = resolveWorkerCount(
            env.get<unsigned>("COLLECTOR_WORKERS"),
            businessCpu / 2U,
            "COLLECTOR_WORKERS"
        ),
    };
}

service::message::outbox::Policy outboxPolicy(const ruvia::env& env) {
    service::message::outbox::Policy policy;
    policy.pendingAlertThreshold = env.get<std::int64_t>("OUTBOX_PENDING_ALERT_THRESHOLD").value_or(1000);
    policy.oldestAgeAlertMs = env.get<std::int64_t>("OUTBOX_OLDEST_AGE_ALERT_MS").value_or(300000);
    policy.deadLetterAlertThreshold = env.get<std::int64_t>("OUTBOX_DEAD_LETTER_ALERT_THRESHOLD").value_or(1);
    policy.receiptRetentionDays = env.get<std::int64_t>("OUTBOX_RECEIPT_RETENTION_DAYS").value_or(30);
    if (policy.pendingAlertThreshold < 0 || policy.oldestAgeAlertMs < 0 ||
        policy.deadLetterAlertThreshold < 0 || policy.receiptRetentionDays < 0 ||
        policy.receiptRetentionDays > 3650) {
        throw std::runtime_error("OUTBOX policy values are invalid");
    }
    return policy;
}

struct ServiceWorkerComponents {
    std::shared_ptr<service::message::WorkerStreamMultiplexer> multiplexer;
    std::shared_ptr<service::edge::DispatcherRuntime> sessionDispatcherRuntime;
    std::shared_ptr<service::observability::RuntimeDiagnostics> observability;
    std::shared_ptr<service::application::ComponentLifecycle> componentLifecycle;
    std::shared_ptr<service::telemetry::PersistenceRuntime> telemetry;
    std::shared_ptr<service::live::LiveChangeRuntime> liveChanges;
    std::shared_ptr<service::live::QueryRuntime> apiQueries;
    std::shared_ptr<service::rpc::RpcConsumerRuntime> rpcConsumer;
    std::shared_ptr<service::command::CommandProcessingRuntime> commandProcessing;
    std::shared_ptr<service::access::DeliveryRuntime> openDeliveries;
    std::shared_ptr<service::runtime::Reconciler> configReconciler;
    std::shared_ptr<service::edge::EdgeProjectionRuntime> edgeProjection;
    std::shared_ptr<service::vpn::VpnHubRuntime> vpnHubRuntime;
    std::shared_ptr<service::gb28181::GbProjectionRuntime> gbProjection;
    std::shared_ptr<service::alert::AlertBootstrap> alertBootstrap;
    std::shared_ptr<service::message::outbox::OutboxRuntime> outboxRuntime;
};

struct ApplicationComponents {
    AppConfig gb28181;
    ruvia::db_config database;
    ruvia::redis_config serviceRedis;
    ruvia::redis_config collectorRedis;
    std::vector<std::shared_ptr<ServiceWorkerComponents>> workers;
    std::shared_ptr<service::collector::CollectorWorkerPool> collectorWorkerPool;
    std::shared_ptr<service::observability::RuntimeDiagnostics> observability;
    std::shared_ptr<service::application::ComponentLifecycle> applicationLifecycle;
};

void registerRpcHandlers(
    const std::shared_ptr<service::rpc::RpcConsumerRuntime>& rpcConsumer,
    const ruvia::env& env
) {
    rpcConsumer->add("telemetry", service::telemetry::TelemetryService::executeProjection);
    rpcConsumer->add("alert", service::alert::metadata::executeRefreshOperation);
    rpcConsumer->add("access", service::access::AccessOperationService::executeOperation);
    rpcConsumer->add("command", service::command::PreparationService::executeOperation);
    rpcConsumer->add("gb28181", service::gb28181::GbControlService::executeOperation);
    rpcConsumer->add("edge", service::edge::EdgeControlService::executeOperation);
    rpcConsumer->add("edge-serial", service::edge::serial_debug::Service::executeBrowserOperation);
    rpcConsumer->add("edge-terminal", service::edge::terminal_state::TerminalService::executeBrowserOperation);
    auto vpnControl = std::make_shared<service::vpn::VpnControlService>(
        vpnHubConfig(env),
        std::string(env.get("EDGE_PLATFORM_ID").value_or(service::message::edge::kDefaultPlatformId))
    );
    rpcConsumer->add(
        "vpn",
        [vpnControl](ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
            return vpnControl->executeOperation(context, operation, payload, stop);
        }
    );
}

ApplicationComponents createComponents(
    const ruvia::env& env,
    const AppConfig& gb28181,
    const WorkerBudget& budget,
    ruvia::db_config database,
    ruvia::redis_config serviceRedis
) {
    ApplicationComponents components;
    components.gb28181 = gb28181;
    components.database = std::move(database);
    components.serviceRedis = std::move(serviceRedis);
    components.collectorRedis = components.serviceRedis;
    // One worker-local XREAD multiplexes wakeups for all Service Stream tasks.
    // The tasks retain separate consumer groups and use the ordinary pool to drain.
    components.serviceRedis.blocking_pool_size_per_worker_ = 4;

    components.collectorWorkerPool = std::make_shared<service::collector::CollectorWorkerPool>();
    components.observability = std::make_shared<service::observability::RuntimeDiagnostics>();
    for (std::size_t index = 0; index < budget.service; ++index) {
        auto owner = std::make_shared<ServiceWorkerComponents>();
        auto& workerComponents = *owner;
        workerComponents.observability = std::make_shared<service::observability::RuntimeDiagnostics>();
        workerComponents.observability->identifyWorker(index);
        workerComponents.observability->setGauge("iot_engine_service_workers", budget.service);
        workerComponents.observability->setGauge("iot_engine_collector_workers", budget.collector);
        workerComponents.componentLifecycle = std::make_shared<service::application::ComponentLifecycle>(*workerComponents.observability);
        workerComponents.multiplexer = std::make_shared<service::message::WorkerStreamMultiplexer>();
        workerComponents.sessionDispatcherRuntime = std::make_shared<service::edge::DispatcherRuntime>();
        workerComponents.telemetry = std::make_shared<service::telemetry::PersistenceRuntime>();
        workerComponents.liveChanges = std::make_shared<service::live::LiveChangeRuntime>(budget.collector);
        workerComponents.apiQueries = std::make_shared<service::live::QueryRuntime>();
        workerComponents.rpcConsumer = std::make_shared<service::rpc::RpcConsumerRuntime>();
        registerRpcHandlers(workerComponents.rpcConsumer, env);
        workerComponents.commandProcessing = std::make_shared<service::command::CommandProcessingRuntime>();
        workerComponents.openDeliveries = std::make_shared<service::access::DeliveryRuntime>();
        workerComponents.configReconciler = std::make_shared<service::runtime::Reconciler>();
        workerComponents.edgeProjection = std::make_shared<service::edge::EdgeProjectionRuntime>(
            *workerComponents.observability,
            std::filesystem::path(env.get("EDGE_FIRMWARE_DIR").value_or("firmware")));
        const auto enableVpnHub = env.get<bool>("VPN_HUB_ENABLED").value_or(true);
        workerComponents.vpnHubRuntime = enableVpnHub
            ? std::make_shared<service::vpn::VpnHubRuntime>(vpnHubConfig(env))
            : nullptr;
        workerComponents.gbProjection = gb28181.enabled
            ? std::make_shared<service::gb28181::GbProjectionRuntime>()
            : nullptr;
        workerComponents.alertBootstrap = std::make_shared<service::alert::AlertBootstrap>();
        workerComponents.outboxRuntime = std::make_shared<service::message::outbox::OutboxRuntime>(
            *workerComponents.observability,
            budget.collector,
            budget.service,
            components.database,
            outboxPolicy(env)
        );
        components.workers.push_back(std::move(owner));
    }
    components.applicationLifecycle = std::make_shared<service::application::ComponentLifecycle>(*components.observability);
    return components;
}

void configureWeb(ruvia::application& app, const std::filesystem::path& runtime) {
    const auto webRoot = runtime / "web";
    if (!std::filesystem::is_directory(webRoot)) {
        return;
    }
    ruvia::document_root_config config;
    config.root_ = webRoot;
    config.static_options_.index_file_ = "index.html";
    config.static_options_.cache_control_ = "no-cache";
    config.static_options_.file_types_.kind_ = ruvia::static_file_type_policy::kind_type::all;
    app.document_root(std::move(config));
}

ruvia::task<ruvia::http_response> handleError(
    ruvia::context& c,
    ruvia::http_error_info info
) {
    c.status(info.status());
    const auto message = info.message().empty() ? std::string_view("请求失败") : info.message();
    co_return c.json(service::common::error(c, service::middleware::requestErrorCode(c, info), message));
}

// Only the supervisor visits the owner collection. Each posted operation owns one
// worker's components; business workers never receive another worker's handle.
template <typename Operation>
void initializeServiceWorker(ruvia::web_worker_handle worker, Operation operation) {
    auto ready = std::make_shared<std::promise<void>>();
    auto completion = ready->get_future();
    if (!worker.post([operation = std::move(operation), ready](ruvia::web_worker_context& context) mutable -> ruvia::task<void> {
                   try {
                       co_await operation(context);
                       ready->set_value();
                   } catch (...) {
                       ready->set_exception(std::current_exception());
                   }
               })
             .accepted()) {
        throw std::runtime_error("service worker rejected initialization");
    }
    completion.get();
}

void registerServiceWorkerLifecycle(ServiceWorkerComponents& workerComponents, ruvia::web_worker_handle worker, std::size_t index, std::size_t count, std::size_t collectors) {
    auto& componentLifecycle = *workerComponents.componentLifecycle;
    componentLifecycle.add({ .name = "stream-multiplexer", .start = [m = workerComponents.multiplexer, worker, index] {
                       m->start(worker, index);
                   },
                    .stop = [m = workerComponents.multiplexer] {
                        m->stop();
                    } });
    componentLifecycle.add({ .name = "api-live-queries", .start = [r = workerComponents.apiQueries, worker, index] {
                       r->start(worker, index);
                   },
                    .stop = [r = workerComponents.apiQueries] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "live-queries", .start = [r = workerComponents.liveChanges, worker, index, count] {
                       r->start(worker, index, count);
                   },
                    .stop = [r = workerComponents.liveChanges] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "outbox", .start = [r = workerComponents.outboxRuntime, worker] {
                       r->start(worker);
                   },
                    .stop = [r = workerComponents.outboxRuntime] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "telemetry", .start = [r = workerComponents.telemetry, worker, index, count, collectors] {
                       r->start(worker, index, count, collectors);
                   },
                    .stop = [r = workerComponents.telemetry] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "command-results", .start = [r = workerComponents.commandProcessing, worker, index, count, collectors] {
                       r->start(worker, index, count, collectors);
                   },
                    .stop = [r = workerComponents.commandProcessing] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "open-access-delivery", .start = [r = workerComponents.openDeliveries, worker, index, count] {
                       r->start(worker, index, count);
                   },
                    .stop = [r = workerComponents.openDeliveries] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "edge-dispatcher", .start = [r = workerComponents.sessionDispatcherRuntime, worker, index, count] {
                       r->start(worker, index, count);
                   },
                    .stop = [r = workerComponents.sessionDispatcherRuntime] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "edge-projector", .start = [r = workerComponents.edgeProjection, worker, index, count] {
                       r->start(worker, index, count);
                   },
                    .stop = [r = workerComponents.edgeProjection] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "alerts", .start = [r = workerComponents.alertBootstrap, worker, index, count] {
                       r->start(worker, index, count);
                   },
                    .stop = [r = workerComponents.alertBootstrap] {
                        r->stop();
                    } });
    if (workerComponents.vpnHubRuntime) {
        componentLifecycle.add({ .name = "vpn", .start = [r = workerComponents.vpnHubRuntime, worker] {
                           r->start(worker);
                       },
                        .stop = [r = workerComponents.vpnHubRuntime] {
                            r->stop();
                        } });
    }
    componentLifecycle.add({ .name = "config-reconciler", .start = [r = workerComponents.configReconciler, worker, index, count, collectors] {
                       r->start(worker, index, count, collectors);
                   },
                    .stop = [r = workerComponents.configReconciler] {
                        r->stop();
                    } });
    componentLifecycle.add({ .name = "control", .start = [r = workerComponents.rpcConsumer, worker, index] {
                       r->start(worker, index);
                   },
                    .stop = [r = workerComponents.rpcConsumer] {
                        r->stop();
                    } });
    if (workerComponents.gbProjection) {
        componentLifecycle.add({ .name = "gb28181-projector", .start = [r = workerComponents.gbProjection, worker, index, count] {
                           (void)r->start(worker, index, count);
                       },
                        .stop = [r = workerComponents.gbProjection] {
                            r->stop();
                        } });
    }
}

auto makeApplicationStart(ruvia::application& app, ApplicationComponents& components, std::size_t collectors) {
    const auto uploadDirectory = std::filesystem::path(app.env().get("EDGE_FIRMWARE_DIR").value_or("firmware"));
    return [&app, &components, collectors, uploadDirectory] {
        const auto workers = app.workers();
        if (workers.empty() || workers.size() != components.workers.size()) {
            throw std::runtime_error("service worker ownership does not match configuration");
        }
        const auto count = workers.size();
        auto& supervisor = *components.applicationLifecycle;
        std::vector<std::string> preparation;
        for (std::size_t index = 0; index < count; ++index) {
            const auto worker = workers[index];
            const auto owner = components.workers[index];
            const auto name = "prepare-worker-" + std::to_string(index);
            preparation.push_back(name);
            supervisor.add({ .name = name, .start = [owner, worker, index, uploadDirectory] {
                                owner->multiplexer->configure(worker, index);
                                initializeServiceWorker(worker, [owner, index, uploadDirectory, worker](ruvia::web_worker_context& context) -> ruvia::task<void> {
                                    context.worker_state<ruvia::web_worker_handle>() = worker;
                                    context.worker_state<service::ServiceWorkerTopology>().index = index;
                                    service::channel::UploadedFile::recoverAbandoned(uploadDirectory);
                                    (void)co_await service::configuration::ConfigurationService::project(context);
                                });
                            },
                             .stop = [] {
                             } });
        }
        if (components.gb28181.enabled) {
            supervisor.add({ .name = "gb28181-sdk", .dependencies = preparation, .start = [] {
                                if (!sdkSupervisor().started()) {
                                    throw std::runtime_error("ZLMediaKit SDK is not ready");
                                }
                            },
                             .stop = [] {
                                 sdkSupervisor().stop();
                             } });
            preparation.push_back("gb28181-sdk");
        }
        supervisor.add({ .name = "collector", .dependencies = preparation, .start = [collectorWorkerPool = components.collectorWorkerPool, redis = components.collectorRedis, gb28181 = components.gb28181, collectors, owners = components.workers]() mutable {
                            collectorWorkerPool->start(redis, collectors, gb28181);
                            for (const auto& owner : owners) {
                                owner->observability->setComponentStatus("collector", service::observability::ComponentState::Ready);
                            }
                        },
                         .stop = [collectorWorkerPool = components.collectorWorkerPool, owners = components.workers] {
                             collectorWorkerPool->stop();
                             for (const auto& owner : owners) {
                                 owner->observability->setComponentStatus("collector", service::observability::ComponentState::Stopped);
                             }
                         } });
        for (std::size_t index = 0; index < count; ++index) {
            const auto worker = workers[index];
            const auto owner = components.workers[index];
            registerServiceWorkerLifecycle(*owner, worker, index, count, collectors);
            supervisor.add({ .name = "service-worker-" + std::to_string(index), .dependencies = { "collector" }, .start = [owner, worker, index, count] {
                                initializeServiceWorker(worker, [index, count, worker](ruvia::web_worker_context& context) -> ruvia::task<void> {
                                    context.worker_state<ruvia::web_worker_handle>() = worker;
                                    co_await service::telemetry::latest::hydrate(context, index, count);
                                });
                                owner->componentLifecycle->start();
                            },
                             .stop = [owner] {
                                 owner->componentLifecycle->stop();
                             } });
        }
        supervisor.start();
    };
}

void configureServer(
    ruvia::application& app,
    ApplicationComponents& components,
    const WorkerBudget& budget
) {
    auto applicationStart = makeApplicationStart(app, components, budget.collector);
    const auto applicationLifecycle = components.applicationLifecycle;
    const auto observability = components.observability;
    app.database(ruvia::db_registration_config{
        .alias_ = "telemetry-history",
        .config_ = components.database,
    });
    app.database(ruvia::db_registration_config{
        .alias_ = "control",
        .config_ = components.database,
    });
    app.database(ruvia::db_registration_config{
        .alias_ = "vpn-coordination",
        .config_ = components.database,
    });
    app.database(ruvia::db_registration_config{
        .alias_ = "vpn-edge-config",
        .config_ = components.database,
    });
    app.use_worker_state<ruvia::web_worker_handle>()
        .use_worker_state<service::ServiceWorkerTopology>([count = components.workers.size()] {
            return service::ServiceWorkerTopology{count};
        })
        .use_worker_state<std::unique_ptr<service::common::UuidV7Generator>>(
            [] { return std::make_unique<service::common::UuidV7Generator>(); })
        .use_worker_state<service::edge::config::PlatformIdentity>(
            [platformId = std::string(app.env().get("EDGE_PLATFORM_ID").value_or(
                service::message::edge::kDefaultPlatformId))] {
                return service::edge::config::PlatformIdentity{platformId};
            })
        .use_worker_state<service::edge::SessionDispatcher>()
        .use_worker_state<service::device::DeviceCommandPort>([] {
            return service::device::DeviceCommandPort{
                &service::command::CommandService::statuses<service::middleware::RequestContext>,
                &service::command::CommandService::fillOperations<service::middleware::RequestContext>,
            };
        })
        .database(ruvia::db_registration_config{
            .config_ = std::move(components.database),
        })
        .redis(ruvia::redis_registration_config{
            .config_ = std::move(components.serviceRedis),
        })
        .get_http_client({.alias_ = "link-public-ip", .config_ = {
            .scheme_ = ruvia::http_scheme::http,
            .host_ = "ip.sb",
            .port_ = 80,
            .max_response_bytes_ = 64U * 1024U,
            .user_agent_ = "curl/8.0",
        }})
        .on_start(std::move(applicationStart))
        .on_stop([applicationLifecycle, observability] {
            // Keep runtime diagnostics alive until every component has stopped.
            (void)observability;
            applicationLifecycle->stop();
        })
        .on_error(&handleError)
        .listen(ruvia::listen_config{
            .address_ = std::string(app.env().get("HOST").value_or("0.0.0.0")),
            .http_ = app.env().get<std::uint16_t>("PORT").value_or(1102),
        })
        .server(ruvia::server_config{
            .worker_count_ = budget.service,
            .max_stream_body_bytes_ = 129U * 1024U * 1024U,
            // Ruvia applies this limit to both incoming and outgoing frames.
            .max_websocket_message_bytes_ = 16U * 1024U * 1024U,
        })
        .run();
}

} // namespace

int main(int argc, char* argv[]) {
    try {
        const auto commandLine = parseCommandLine(argc, argv);
        auto& app = ruvia::app();
        app.load_dotenv();
        validateEdgeConfiguration(app.env());
        const auto runtime = runtimeDirectory(argc > 0 ? argv[0] : nullptr);
        service::packet_log::initialize(packetLogConfig(app.env(), runtime));
        auto gb28181 = gb28181Config(app.env());

        auto db = migrateDatabase(app.env());
        if (commandLine.migrateOnly) {
            return 0;
        }

        configureWeb(app, runtime);
        if (gb28181.enabled) {
            // Start the SDK before registering the worker-local HTTP origin so
            // port-zero configuration uses the actual bound SDK listener port.
            sdkSupervisor().configure(gb28181.media);
            sdkSupervisor().start();
            app.get_http_client({ .alias_ = "gb-media", .config_ = {
                                                      .scheme_ = ruvia::http_scheme::http,
                                                      .host_ = "127.0.0.1",
                                                      .port_ = sdkSupervisor().ports().http,
                                                      .connection_count_ = 64,
                                                      .request_timeout_ = std::nullopt,
                                                      .max_response_bytes_ = 2U * 1024U * 1024U,
                                                  } });
        }
        const auto budget = workerBudget(app.env(), gb28181);
        std::cout << "worker budget: cpu=" << budget.cpu
                  << ", service=" << budget.service
                  << ", collector=" << budget.collector
                  << ", gb28181=" << budget.gb28181 << '\n';
        auto serviceRedis = redisConfig(app.env());
        auto components = createComponents(
            app.env(),
            gb28181,
            budget,
            std::move(db),
            std::move(serviceRedis)
        );
        configureServer(app, components, budget);
        sdkSupervisor().stop();
        service::packet_log::shutdown();
        return 0;
    } catch (...) {
        sdkSupervisor().stop();
        service::packet_log::shutdown();
        std::cerr << "server failed: " << ruvia::exception_text(std::current_exception()) << '\n';
        return 1;
    }
}
