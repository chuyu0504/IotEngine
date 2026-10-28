#include <algorithm>
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
#include <vector>

#include <ruvia/web/app.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_schema.h>
#include <ruvia/web/error.h>
#include <ruvia/web/redis/redis.h>

#include "service/common/http.h"
#include "service/common/worker.h"
#include "service/config/process.h"
#include "service/config/schema.h"
#include "service/config/storage.h"
#include "service/features/access/access.runtime.h"
#include "service/features/alert/alert.runtime.h"
#include "service/features/collector/collector.runtime.h"
#include "service/features/command/command.runtime.h"
#include "service/features/command/command.service.h"
#include "service/features/configuration/configuration.runtime.h"
#include "service/features/configuration/configuration.service.h"
#include "service/features/edge/edge.config.h"
#include "service/features/edge/edge.runtime.h"
#include "service/features/edge/gateway/gateway.runtime.h"
#include "service/features/edge/gateway/gateway.transport.h"
#include "service/features/edge/projection/projection.runtime.h"
#include "service/features/gb28181/gb28181.runtime.h"
#include "service/features/gb28181/media/media.transport.h"
#include "service/features/live/live.runtime.h"
#include "service/features/messaging/messaging.runtime.h"
#include "service/features/messaging/rpc/rpc.runtime.h"
#include "service/features/observability/observability.runtime.h"
#include "service/features/packet_log/packet_log.transport.h"
#include "service/features/telemetry/telemetry.runtime.h"
#include "service/features/telemetry/telemetry.service.h"
#include "service/features/vpn/vpn.config.h"
#include "service/features/vpn/vpn.runtime.h"
#include "service/middleware/validation.h"
#include "service/modules/alert/alert.controller.h"
#include "service/modules/command/command.controller.h"
#include "service/modules/device/device.controller.h"
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
#include "service/utils/uuid.h"

namespace {

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

ruvia::db_config migrateDatabase(ruvia::db_config db, const ruvia::env& env) {
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

struct ServiceWorkerComponents {
    std::shared_ptr<service::edge::DispatcherRuntime> sessionDispatcherRuntime;
    std::shared_ptr<service::observability::RuntimeDiagnostics> observability;
    std::shared_ptr<service::observability::ComponentLifecycle> componentLifecycle;
    std::shared_ptr<service::telemetry::PersistenceRuntime> telemetry;
    std::shared_ptr<service::live::LiveChangeRuntime> liveChanges;
    std::shared_ptr<service::messaging::rpc::RpcConsumerRuntime> rpcConsumer;
    std::shared_ptr<service::command::CommandProcessingRuntime> commandProcessing;
    std::shared_ptr<service::access::DeliveryRuntime> openDeliveries;
    std::shared_ptr<service::configuration::Reconciler> configReconciler;
    std::shared_ptr<service::edge::projection::ProjectionRuntime> edgeProjection;
    std::shared_ptr<service::edge::gateway::FirmwareCleanupRuntime> firmwareCleanup;
    // Serves the "vpn" RPC component; its background scheduling runs only when VPN_HUB_ENABLED.
    std::shared_ptr<service::vpn::VpnHubService> vpnHubService;
    std::shared_ptr<service::vpn::VpnHubRuntime> vpnHubRuntime;
    std::shared_ptr<service::gb28181::GbProjectionRuntime> gbProjection;
    std::shared_ptr<service::alert::AlertRuntime> alerts;
    std::shared_ptr<service::messaging::outbox::OutboxRuntime> outboxRuntime;
};

// Only the supervisor owns these collections. Each record is created, driven,
// stopped and destroyed on its own Collector event loop.
class CollectorWorkers final {
  public:
    ~CollectorWorkers() { stop(); }

    void start(ruvia::redis_config redis, std::size_t count, service::gb28181::Config gb28181, std::string instance) {
        if (pool_) {
            return;
        }
        redis.pool_size_per_worker_ = 1;
        redis.command_timeout_ = std::nullopt;
        pool_ = std::make_unique<ruvia::event_loop_pool>(
            ruvia::event_loop_pool_options{ .loop_count_ = count, .queue_capacity_ = 8192 }
        );
        try {
            pool_->start();
            owners_.resize(count);
            std::vector<ruvia::root_task<std::unique_ptr<Owner>>> initializing;
            initializing.reserve(count);
            for (std::size_t index = 0; index < count; ++index) {
                initializing.push_back(pool_->loop(index).start(create(
                    pool_->loop(index),
                    redis,
                    gb28181,
                    service::common::WorkerIdentity{ instance, index, count, service::message::RedisChannelScope{ redis.database_ } }
                )));
            }
            std::exception_ptr failure;
            for (std::size_t index = 0; index < count; ++index) {
                try {
                    owners_[index] = initializing[index].get();
                } catch (...) {
                    if (!failure) {
                        failure = std::current_exception();
                    }
                }
            }
            if (failure) {
                std::rethrow_exception(failure);
            }
        } catch (...) {
            stop();
            throw;
        }
    }

    void stop() noexcept {
        if (!pool_) {
            return;
        }
        std::vector<ruvia::root_task<void>> stopping;
        for (std::size_t index = 0; index < owners_.size(); ++index) {
            if (owners_[index]) {
                stopping.push_back(pool_->loop(index).start(shutdown(std::move(owners_[index]))));
            }
        }
        for (auto& task : stopping) {
            try {
                task.get();
            } catch (...) {
                service::collector::logShutdownFailure("worker");
            }
        }
        pool_->stop();
        try {
            pool_->join();
        } catch (...) {
            service::collector::logShutdownFailure("event_loop_pool");
        }
        owners_.clear();
        pool_.reset();
    }

  private:
    struct Owner {
        std::unique_ptr<service::collector::CollectorWorker> collector;
        std::unique_ptr<service::gb28181::CollectorRuntime> gb28181;
    };

    static ruvia::task<void> shutdown(std::unique_ptr<Owner> owner) {
        if (owner->gb28181) {
            try {
                co_await owner->gb28181->shutdown();
            } catch (...) {
                service::collector::logShutdownFailure("gb28181");
            }
        }
        if (owner->collector) {
            try {
                co_await owner->collector->shutdown();
            } catch (...) {
                service::collector::logShutdownFailure("business");
            }
        }
        owner->gb28181.reset();
        if (owner->collector) {
            try {
                co_await owner->collector->shutdownRedis();
            } catch (...) {
                service::collector::logShutdownFailure("redis");
            }
        }
        owner->collector.reset();
    }

    static ruvia::task<std::unique_ptr<Owner>> create(
        ruvia::event_loop loop,
        ruvia::redis_config redisConfig,
        service::gb28181::Config gbConfig,
        service::common::WorkerIdentity identity
    ) {
        auto owner = std::make_unique<Owner>();
        auto redis = std::make_unique<ruvia::redis_client>(loop, redisConfig);
        std::exception_ptr failure;
        try {
            co_await redis->connect();
            owner->collector = std::make_unique<service::collector::CollectorWorker>(
                loop,
                std::move(redis),
                identity
            );
            if (gbConfig.enabled) {
                owner->gb28181 = std::make_unique<service::gb28181::CollectorRuntime>(
                    loop,
                    owner->collector->redisHandle(),
                    std::move(gbConfig),
                    identity
                );
                co_await owner->gb28181->initialize();
            }
            co_await owner->collector->initialize();
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            co_await shutdown(std::move(owner));
            if (redis) {
                try {
                    co_await redis->shutdown();
                } catch (...) {
                    service::collector::logShutdownFailure("startup_redis");
                }
            }
            std::rethrow_exception(failure);
        }
        co_return owner;
    }

    std::unique_ptr<ruvia::event_loop_pool> pool_;
    std::vector<std::unique_ptr<Owner>> owners_;
};

struct ApplicationComponents {
    service::gb28181::Config gb28181;
    ruvia::db_config database;
    ruvia::redis_config serviceRedis;
    ruvia::redis_config collectorRedis;
    service::edge::config::Config edge;
    std::string host;
    std::uint16_t port{};
    std::string instanceId;
    std::vector<std::shared_ptr<ServiceWorkerComponents>> workers;
    std::shared_ptr<CollectorWorkers> collectorWorkers;
    std::shared_ptr<service::observability::RuntimeDiagnostics> observability;
    std::shared_ptr<service::observability::ComponentLifecycle> applicationLifecycle;
};

void registerRpcHandlers(
    const std::shared_ptr<service::messaging::rpc::RpcConsumerRuntime>& rpcConsumer
) {
    rpcConsumer->add("telemetry", service::telemetry::TelemetryService::executeProjection);
    rpcConsumer->add("alert", service::alert::metadata::executeRefreshOperation);
    rpcConsumer->add("access", service::access::AccessOperationService::executeOperation);
    rpcConsumer->add("command", service::command::PreparationService::executeOperation);
    rpcConsumer->add("gb28181", service::gb28181::GbControlService::executeOperation);
    rpcConsumer->add("edge", service::edge::EdgeControlService::executeOperation);
    rpcConsumer->add("edge-serial", service::edge::serial_debug::Service::executeBrowserOperation);
    rpcConsumer->add("edge-terminal", service::edge::terminal_state::TerminalService::executeBrowserOperation);
    rpcConsumer->add(std::string(service::message::edge::firmware::kRpcComponent), service::edge::gateway::GatewayService::scheduleFirmwareCleanup);
}

ApplicationComponents createComponents(
    service::config::ProcessConfig process,
    service::edge::config::Config edge,
    const service::vpn::config::Config& vpn,
    const service::gb28181::Config& gb28181,
    const service::config::WorkerBudget& budget,
    service::messaging::outbox::Policy policy
) {
    ApplicationComponents components;
    components.gb28181 = gb28181;
    components.database = std::move(process.database);
    components.serviceRedis = std::move(process.redis);
    components.collectorRedis = components.serviceRedis;
    components.collectorRedis.blocking_pool_size_per_worker_ = gb28181.enabled ? 3 : 1;
    components.serviceRedis.blocking_pool_size_per_worker_ = 4;
    components.edge = std::move(edge);
    components.host = std::move(process.host);
    components.port = process.port;
    service::common::UuidV7State processUuid;
    components.instanceId = service::utils::nextUuid(processUuid);
    components.collectorWorkers = std::make_shared<CollectorWorkers>();
    components.observability = std::make_shared<service::observability::RuntimeDiagnostics>();
    for (std::size_t index = 0; index < budget.service; ++index) {
        auto owner = std::make_shared<ServiceWorkerComponents>();
        auto& workerComponents = *owner;
        workerComponents.observability = std::make_shared<service::observability::RuntimeDiagnostics>();
        workerComponents.observability->identifyWorker(index);
        workerComponents.observability->setGauge("iot_engine_service_workers", budget.service);
        workerComponents.observability->setGauge("iot_engine_collector_workers", budget.collector);
        workerComponents.componentLifecycle = std::make_shared<service::observability::ComponentLifecycle>(*workerComponents.observability);
        workerComponents.sessionDispatcherRuntime = std::make_shared<service::edge::DispatcherRuntime>();
        workerComponents.telemetry = std::make_shared<service::telemetry::PersistenceRuntime>();
        workerComponents.liveChanges = std::make_shared<service::live::LiveChangeRuntime>(budget.collector);
        workerComponents.rpcConsumer = std::make_shared<service::messaging::rpc::RpcConsumerRuntime>();
        workerComponents.firmwareCleanup = std::make_shared<service::edge::gateway::FirmwareCleanupRuntime>(
            components.edge.firmwareDirectory
        );
        registerRpcHandlers(workerComponents.rpcConsumer);
        workerComponents.commandProcessing = std::make_shared<service::command::CommandProcessingRuntime>();
        workerComponents.openDeliveries = std::make_shared<service::access::DeliveryRuntime>();
        workerComponents.configReconciler = std::make_shared<service::configuration::Reconciler>();
        workerComponents.edgeProjection = std::make_shared<service::edge::projection::ProjectionRuntime>(
            *workerComponents.observability
        );
        workerComponents.vpnHubService = std::make_shared<service::vpn::VpnHubService>(
            vpn.hub,
            components.edge.platformId,
            components.instanceId,
            vpn.enabled
        );
        workerComponents.vpnHubRuntime = std::make_shared<service::vpn::VpnHubRuntime>(
            *workerComponents.vpnHubService
        );
        workerComponents.rpcConsumer->add(
            std::string(service::message::vpn::kRpcComponent),
            [vpnHub = workerComponents.vpnHubService](ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
                return vpnHub->executeOperation(context, operation, payload, stop);
            }
        );
        workerComponents.gbProjection = gb28181.enabled
            ? std::make_shared<service::gb28181::GbProjectionRuntime>(
                  service::common::WorkerIdentity{ components.instanceId, index, budget.service, service::message::RedisChannelScope{ components.serviceRedis.database_ } }
              )
            : nullptr;
        workerComponents.alerts = std::make_shared<service::alert::AlertRuntime>();
        workerComponents.outboxRuntime = std::make_shared<service::messaging::outbox::OutboxRuntime>(
            *workerComponents.observability,
            budget.collector,
            budget.service,
            components.database,
            policy
        );
        components.workers.push_back(std::move(owner));
    }
    components.applicationLifecycle = std::make_shared<service::observability::ComponentLifecycle>(*components.observability);
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
    config.static_options_.file_types_.kind_ = ruvia::static_file_type_policy::kind::all;
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
ruvia::task<void> initializeServiceWorkerTask(
    ruvia::web_worker_context& context,
    Operation operation,
    std::shared_ptr<std::promise<void>> ready
) {
    try {
        co_await operation(context);
        ready->set_value();
    } catch (...) {
        ready->set_exception(std::current_exception());
    }
}

template <typename Operation>
std::future<void> postServiceWorkerInitialization(ruvia::web_worker_handle worker, Operation operation) {
    auto ready = std::make_shared<std::promise<void>>();
    auto completion = ready->get_future();
    if (!worker.post([operation = std::move(operation), ready](ruvia::web_worker_context& context) mutable {
                   return initializeServiceWorkerTask(context, std::move(operation), ready);
               })
             .accepted()) {
        throw std::runtime_error("service worker rejected initialization");
    }
    return completion;
}

void registerServiceWorkerLifecycle(ServiceWorkerComponents& components, ruvia::web_worker_handle worker) {
    auto& lifecycle = *components.componentLifecycle;
    lifecycle.addRuntime("live-queries", components.liveChanges, worker);
    lifecycle.addRuntime("outbox", components.outboxRuntime, worker);
    lifecycle.addRuntime("telemetry", components.telemetry, worker);
    lifecycle.addRuntime("command-results", components.commandProcessing, worker);
    lifecycle.addRuntime("open-access-delivery", components.openDeliveries, worker);
    lifecycle.addRuntime("edge-dispatcher", components.sessionDispatcherRuntime, worker);
    lifecycle.addRuntime("edge-projector", components.edgeProjection, worker);
    lifecycle.addRuntime("firmware-cleanup", components.firmwareCleanup, worker);
    lifecycle.addRuntime("alerts", components.alerts, worker);
    lifecycle.addRuntime("vpn", components.vpnHubRuntime, worker);
    lifecycle.addRuntime("config-reconciler", components.configReconciler, worker);
    lifecycle.addRuntime("control", components.rpcConsumer, worker);
    if (components.gbProjection) {
        lifecycle.addRuntime("gb28181-projector", components.gbProjection, worker);
    }
}

auto makeApplicationStart(ruvia::application& app, ApplicationComponents& components, std::size_t collectors) {
    return [&app, &components, collectors] {
        const auto workers = app.workers();
        if (workers.empty() || workers.size() != components.workers.size()) {
            throw std::runtime_error("service worker ownership does not match configuration");
        }
        auto& supervisor = *components.applicationLifecycle;
        supervisor.add({ .name = "prepare-workers", .start = [workers, &components] {
                            std::vector<std::future<void>> readiness;
                            readiness.reserve(workers.size());
                            std::exception_ptr failure;
                            for (std::size_t index = 0; index < workers.size(); ++index) {
                                try {
                                    readiness.push_back(postServiceWorkerInitialization(
                                        workers[index],
                                        [worker = workers[index],
                                         identity = service::common::WorkerIdentity{
                                             components.instanceId,
                                             index,
                                             workers.size(),
                                             service::message::RedisChannelScope{ components.serviceRedis.database_ } },
                                         directory = components.edge.firmwareDirectory](ruvia::web_worker_context& context) -> ruvia::task<void> {
                                            context.worker_state<ruvia::web_worker_handle>() = worker;
                                            context.worker_state<std::optional<service::common::WorkerIdentity>>().emplace(identity);
                                            (void)co_await service::edge_node::EdgeService::recoverFirmwareUploads(context, directory);
                                            co_await service::configuration::ConfigurationService::initialize(context);
                                            co_await service::telemetry::latest::hydrate(context);
                                        }
                                    ));
                                } catch (...) {
                                    failure = std::current_exception();
                                    break;
                                }
                            }
                            for (auto& ready : readiness) {
                                try {
                                    ready.get();
                                } catch (...) {
                                    if (!failure) {
                                        failure = std::current_exception();
                                    }
                                }
                            }
                            if (failure) {
                                std::rethrow_exception(failure);
                            }
                        },
                         .stop = [] {
                         } });
        std::vector<std::string> dependencies{ "prepare-workers" };
        if (components.gb28181.enabled) {
            supervisor.add({ .name = "gb28181-sdk", .dependencies = dependencies, .start = [] {
                                if (!service::gb28181::media::sdkSupervisor().started()) {
                                    throw std::runtime_error("ZLMediaKit SDK is not ready");
                                }
                            },
                             .stop = [] {
                                 service::gb28181::media::sdkSupervisor().stop();
                             } });
            dependencies.push_back("gb28181-sdk");
        }
        supervisor.add({ .name = "collector", .dependencies = std::move(dependencies), .start = [&components, collectors] {
                            components.collectorWorkers->start(
                                components.collectorRedis,
                                collectors,
                                components.gb28181,
                                components.instanceId
                            );
                            for (const auto& owner : components.workers) {
                                owner->observability->setComponentStatus(
                                    "collector",
                                    service::observability::ComponentState::Ready
                                );
                            }
                        },
                         .stop = [&components] {
                             components.collectorWorkers->stop();
                             for (const auto& owner : components.workers) {
                                 owner->observability->setComponentStatus(
                                     "collector",
                                     service::observability::ComponentState::Stopped
                                 );
                             }
                         } });
        for (std::size_t index = 0; index < workers.size(); ++index) {
            const auto owner = components.workers[index];
            registerServiceWorkerLifecycle(*owner, workers[index]);
            supervisor.add({ .name = "service-worker-" + std::to_string(index), .dependencies = { "collector" }, .start = [owner] {
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
    const service::config::WorkerBudget& budget
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
    app.database(ruvia::db_registration_config{
        .alias_ = "startup",
        .config_ = components.database,
    });
    for (const auto alias : { "acquisition-fence", "edge-configuration-read", "edge-history", "telemetry-alert", "vpn-address-allocation" }) {
        app.database(ruvia::db_registration_config{
            .alias_ = alias,
            .config_ = components.database,
        });
    }
    app.use<service::middleware::ValidationErrorCodeMiddleware<service::common::kValidationErrorCode>>()
        .use_worker_state<ruvia::web_worker_handle>()
        .use_worker_state<std::optional<service::common::WorkerIdentity>>()
        .use_worker_state<service::common::UuidV7State>()
        .use_worker_state<service::edge::config::PlatformIdentity>(
            [platformId = components.edge.platformId] {
                return service::edge::config::PlatformIdentity{ platformId };
            }
        )
        .use_worker_state<service::edge_node::FirmwareStorageConfig>(
            [url = components.edge.publicBaseUrl, directory = components.edge.firmwareDirectory] {
                return service::edge_node::FirmwareStorageConfig{ url, directory };
            }
        )
        .use_worker_state<service::edge::gateway::transport::ConnectionDriver>([] {
            return service::edge::gateway::transport::ConnectionDriver{
                &service::edge::gateway::GatewayRuntime::runConnection
            };
        })
        .use_worker_state<service::edge::SessionDispatcher>()
        .use_worker_state<service::edge::gateway::FirmwareCleanupSignal>()
        .use_worker_state<service::device::DeviceCommandPort>([] {
            return service::device::DeviceCommandPort{
                &service::command::CommandService::statuses<service::middleware::RequestContext>,
                &service::command::CommandService::fillOperations<service::middleware::RequestContext>,
            };
        })
        .use_worker_state<service::middleware::AuthPort>(&service::auth::AuthController::port)
        .database(ruvia::db_registration_config{
            .config_ = std::move(components.database),
        })
        .redis(ruvia::redis_registration_config{
            .config_ = std::move(components.serviceRedis),
        })
        .http_client({ .alias_ = "link-public-ip", .config_ = {
                                                       .scheme_ = ruvia::http_scheme::http,
                                                       .host_ = "ip.sb",
                                                       .port_ = 80,
                                                       .max_response_bytes_ = 64U * 1024U,
                                                       .user_agent_ = "curl/8.0",
                                                   } })
        .on_start(std::move(applicationStart))
        .on_stop([applicationLifecycle, observability] {
            // Keep runtime diagnostics alive until every component has stopped.
            (void)observability;
            applicationLifecycle->stop();
        })
        .on_error(&handleError)
        .listen(ruvia::listen_config{
            .address_ = components.host,
            .http_ = components.port,
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
        auto process = service::config::ProcessConfig::fromEnv(app.env());
        const auto edge = service::edge::config::Config::fromEnv(app.env());
        const auto vpn = service::vpn::config::fromEnv(app.env());
        const auto policy = service::messaging::outbox::Policy::fromEnv(app.env());
        const auto runtime = runtimeDirectory(argc > 0 ? argv[0] : nullptr);
        auto gb28181 = service::gb28181::Config::fromEnv(app.env());

        process.database = migrateDatabase(std::move(process.database), app.env());
        if (commandLine.migrateOnly) {
            return 0;
        }
        service::packet_log::initialize(service::packet_log::fromEnv(app.env(), runtime));

        configureWeb(app, runtime);
        if (gb28181.enabled) {
            // Start the SDK before registering the worker-local HTTP origin so
            // port-zero configuration uses the actual bound SDK listener port.
            service::gb28181::media::sdkSupervisor().configure(gb28181.media);
            service::gb28181::media::sdkSupervisor().start();
            app.http_client({ .alias_ = "gb-media", .config_ = {
                                                        .scheme_ = ruvia::http_scheme::http,
                                                        .host_ = "127.0.0.1",
                                                        .port_ = service::gb28181::media::sdkSupervisor().ports().http,
                                                        .connection_count_ = 64,
                                                        .request_timeout_ = std::nullopt,
                                                        .max_response_bytes_ = 2U * 1024U * 1024U,
                                                    } });
        }
        const auto budget = service::config::WorkerBudget::resolve(
            process,
            gb28181.enabled,
            static_cast<unsigned>(std::max(1, gb28181.media.workerThreads))
        );
        std::cout << "worker budget: cpu=" << budget.cpu
                  << ", service=" << budget.service
                  << ", collector=" << budget.collector
                  << ", gb28181=" << budget.gb28181 << '\n';
        auto components = createComponents(
            std::move(process),
            edge,
            vpn,
            gb28181,
            budget,
            policy
        );
        configureServer(app, components, budget);
        service::gb28181::media::sdkSupervisor().stop();
        service::packet_log::shutdown();
        return 0;
    } catch (...) {
        service::gb28181::media::sdkSupervisor().stop();
        service::packet_log::shutdown();
        std::cerr << "server failed: " << ruvia::exception_text(std::current_exception()) << '\n';
        return 1;
    }
}
