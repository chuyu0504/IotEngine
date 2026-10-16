#pragma once

#include "service/features/observability/observability.service.h"
#include "service/features/telemetry/derived/derived.service.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/features/edge/edge.service.h"
#include "service/features/edge/session/session.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"

namespace service::edge {

// One instance is created by application::use_worker_state for every Service Worker.
// The session table and Redis notification Stream are worker-local: no wakeup,
// callback, or socket is forwarded to another worker.
class SessionDispatcher final {
  public:
    using SessionWake = std::function<void()>;
    using SessionFailure = std::function<void(std::string_view)>;

    SessionDispatcher() = default;
    SessionDispatcher(const SessionDispatcher&) = delete;
    SessionDispatcher& operator=(const SessionDispatcher&) = delete;
    SessionDispatcher(SessionDispatcher&&) noexcept = default;
    SessionDispatcher& operator=(SessionDispatcher&&) = delete;

    [[nodiscard]] std::size_t workerIndex() const {
        if (!workerIndex_) {
            throw std::runtime_error("edge dispatcher is not ready");
        }
        return *workerIndex_;
    }

    void registerSession(
        std::string nodeId,
        std::uint64_t epoch,
        SessionWake wake,
        SessionFailure failure = {}
    ) {
        if (!running_ || failed_ || !wake) {
            throw std::runtime_error("edge dispatcher is not ready");
        }
        sessions_.insert_or_assign(
            std::move(nodeId),
            SessionTarget{ epoch, std::move(wake), std::move(failure) }
        );
    }

    void unregisterSession(std::string_view nodeId, std::uint64_t epoch) noexcept {
        const auto current = sessions_.find(nodeId);
        if (current != sessions_.end() && current->second.epoch == epoch) {
            sessions_.erase(current);
        }
    }

    void requestStop() noexcept {
        if (stopSource_) {
            stopSource_->request_stop();
        }
        service::messaging::workerStreamMultiplexer().signal(
            service::message::WorkerStreamTask::EdgeDispatcher
        );
    }

    // Edge projection lease failure is a worker-local lifecycle event.  Every
    // session registered on this SessionDispatcher is closed on the same Worker so
    // no socket, callback, or mutable session state crosses workers.
    void failSessions(std::string_view reason) noexcept {
        failed_ = true;
        for (auto& [nodeId, target] : sessions_) {
            (void)nodeId;
            try {
                if (target.failure) {
                    target.failure(reason);
                }
            } catch (const std::exception& error) {
                std::cerr << "edge session failure notification failed: "
                          << error.what() << '\n';
            } catch (...) {
                std::cerr << "edge session failure notification failed: unknown error\n";
            }
        }
    }

    [[nodiscard]] bool failed() const noexcept { return failed_; }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t workerIndex, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        bool readySet = false;
        try {
            if (running_) {
                throw std::runtime_error("edge dispatcher is already running");
            }
            workerIndex_ = workerIndex;
            failed_ = false;
            stopSource_ = std::make_unique<ruvia::stop_source>();
            const auto redis = context.redis();
            const auto workerStream = service::message::edge::dispatch::stream(workerIndex, service::runtime::instanceId());
            const auto consumer = "service-edge-dispatcher-" +
                std::to_string(workerIndex);
            co_await service::message::redis::ensureGroup(
                redis,
                workerStream,
                service::message::edge::dispatch::kGroup
            );
            running_ = true;
            ready->set_value();
            readySet = true;
            bool recovering = true;
            const auto stopToken = ruvia::combine_stop_tokens(
                context.get_stop_token(),
                stopSource_->token()
            );
            while (!stopToken.stop_requested()) {
                bool failed = false;
                try {
                    auto messages = recovering
                        ? co_await service::message::redis::readGroup(
                              redis,
                              workerStream,
                              service::message::edge::dispatch::kGroup,
                              consumer,
                              "0",
                              std::chrono::milliseconds(0),
                              256
                          )
                        : co_await service::message::redis::readGroup(
                              redis,
                              workerStream,
                              service::message::edge::dispatch::kGroup,
                              consumer,
                              ">",
                              std::chrono::milliseconds(0),
                              256
                          );
                    if (recovering && messages.empty()) {
                        recovering = false;
                        continue;
                    }
                    if (messages.empty()) {
                        co_await service::messaging::workerStreamMultiplexer().wait(
                            workerIndex,
                            service::message::WorkerStreamTask::EdgeDispatcher,
                            stopToken
                        );
                        continue;
                    }
                    for (const auto& message : messages) {
                        const auto event = service::message::edge::dispatch::eventFrom(message);
                        if (event.kind == service::message::edge::dispatch::kNodeKind &&
                            !event.nodeId.empty() && !deliver(event.nodeId)) {
                            recovering = true;
                            failed = true;
                            break;
                        }
                        co_await service::message::redis::acknowledgeAndDelete(
                            redis,
                            workerStream,
                            service::message::edge::dispatch::kGroup,
                            message.id
                        );
                    }
                } catch (const std::exception& error) {
                    if (stopToken.stop_requested()) {
                        break;
                    }
                    std::cerr << "edge dispatch worker " << workerIndex
                              << " read failed: " << error.what() << '\n';
                    recovering = true;
                    failed = true;
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(
                        context.worker(),
                        std::chrono::milliseconds(250)
                    );
                }
            }
        } catch (...) {
            if (!readySet) {
                try {
                    ready->set_exception(std::current_exception());
                } catch (...) {
                }
            }
        }
        running_ = false;
        sessions_.clear();
        stopSource_.reset();
        try {
            stopped->set_value();
        } catch (...) {
        }
    }

  private:
    struct SessionTarget {
        std::uint64_t epoch{};
        SessionWake wake;
        SessionFailure failure;
    };

    bool deliver(std::string_view nodeId) {
        const auto current = sessions_.find(nodeId);
        if (current == sessions_.end()) {
            return true;
        }
        try {
            current->second.wake();
            return true;
        } catch (const std::exception& error) {
            std::cerr << "edge dispatch worker " << *workerIndex_
                      << " wake failed: " << error.what() << '\n';
            return false;
        } catch (...) {
            std::cerr << "edge dispatch worker " << *workerIndex_
                      << " wake failed: unknown error\n";
            return false;
        }
    }

    std::map<std::string, SessionTarget, std::less<>> sessions_;
    std::unique_ptr<ruvia::stop_source> stopSource_;
    std::optional<std::size_t> workerIndex_;
    bool running_{ false };
    bool failed_{ false };
};

// One supervisor is assembled for each Service Worker. Functional state remains
// in that worker's SessionDispatcher instance above.
class DispatcherRuntime final {
  public:
    DispatcherRuntime() = default;
    DispatcherRuntime(const DispatcherRuntime&) = delete;
    DispatcherRuntime& operator=(const DispatcherRuntime&) = delete;

    ~DispatcherRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex, std::size_t serviceWorkerCount) {
        if (running_.exchange(true)) {
            return;
        }
        worker_ = std::move(worker);
        workerIndex_ = workerIndex;
        serviceWorkerCount_ = serviceWorkerCount;
        if (!worker_.valid() || serviceWorkerCount_ == 0 ||
            workerIndex_ >= serviceWorkerCount_) {
            running_.store(false);
            worker_ = {};
            throw std::runtime_error("edge dispatcher requires service workers");
        }
        auto ready = std::make_shared<std::promise<void>>();
        auto stopped = std::make_shared<std::promise<void>>();
        auto readiness = ready->get_future();
        stopped_ = stopped->get_future().share();
        try {
            const auto posted = worker_.post(
                [workerIndex, ready, stopped](ruvia::web_worker_context& context) {
                    return context.worker_state<SessionDispatcher>().run(
                        context,
                        workerIndex,
                        ready,
                        stopped
                    );
                }
            );
            if (!posted.accepted()) {
                stopped->set_value();
                throw std::runtime_error("service worker rejected edge dispatcher");
            }
            readiness.get();
        } catch (...) {
            stop();
            throw;
        }
    }

    void stop() noexcept {
        if (!running_.exchange(false)) {
            return;
        }
        if (worker_.valid()) {
            const auto worker = worker_;
            const auto wakeAccepted = worker.post([](ruvia::web_worker_context& context) -> ruvia::task<void> {
                                                context.worker_state<SessionDispatcher>().requestStop();
                                                co_return;
                                            })
                                          .accepted();
            if (!wakeAccepted) {
                // A closed worker already propagates its stop token to SessionDispatcher.
            }
        }
        if (stopped_.valid()) {
            stopped_.wait();
        }
        stopped_ = {};
        worker_ = {};
    }

  private:
    ruvia::web_worker_handle worker_;
    std::shared_future<void> stopped_;
    std::size_t workerIndex_ = 0;
    std::size_t serviceWorkerCount_ = 0;
    std::atomic_bool running_{ false };
};

} // namespace service::edge


namespace service::edge {

class EdgeProjectionRuntime final : private EdgeProjectionService {
  public:
    explicit EdgeProjectionRuntime(observability::RuntimeDiagnostics& diagnostics,
                                   std::filesystem::path firmwareDirectory = "firmware")
        : diagnostics_(diagnostics), firmwareDirectory_(std::filesystem::absolute(std::move(firmwareDirectory)).lexically_normal()) {}
    EdgeProjectionRuntime(const EdgeProjectionRuntime&) = delete;
    EdgeProjectionRuntime& operator=(const EdgeProjectionRuntime&) = delete;

    ~EdgeProjectionRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex, std::size_t serviceWorkerCount) {
        if (running_.exchange(true)) {
            return;
        }
        failed_.store(false);
        worker_ = std::move(worker);
        workerIndex_ = workerIndex;
        serviceWorkerCount_ = serviceWorkerCount;
        if (!worker_.valid() || serviceWorkerCount_ == 0 ||
            workerIndex_ >= serviceWorkerCount_) {
            running_.store(false);
            worker_ = {};
            throw std::runtime_error("edge projector requires Service Workers");
        }
        auto ready = std::make_shared<std::promise<void>>();
        auto stopped = std::make_shared<std::promise<void>>();
        auto readiness = ready->get_future();
        stopped_ = stopped->get_future().share();
        const auto posted = worker_.post(
            [this, ready, stopped](ruvia::web_worker_context& context) {
                leaseScope_ = std::make_unique<ruvia::task_scope>(context.worker());
                return run(context, workerIndex_, ready, stopped);
            }
        );
        if (!posted.accepted()) {
            stopped->set_value();
            running_.store(false);
            worker_ = {};
            stopped_ = {};
            leaseScope_.reset();
            throw std::runtime_error("service worker rejected edge projector");
        }
        try {
            readiness.get();
        } catch (...) {
            stop();
            throw;
        }
    }

    void stop() noexcept {
        const bool wasRunning = running_.exchange(false);
        if (!wasRunning && !stopped_.valid()) {
            return;
        }
        if (leaseScope_) {
            leaseScope_->request_stop();
        }
        if (worker_.valid()) {
            const auto worker = worker_;
            const auto wakeAccepted = worker.post([](ruvia::web_worker_context&) -> ruvia::task<void> {
                                                service::messaging::workerStreamMultiplexer().signal(
                                                    service::message::WorkerStreamTask::EdgeProjector
                                                );
                                                co_return;
                                            })
                                          .accepted();
            if (!wakeAccepted) {
                // A closed worker already propagates its stop token to the task.
            }
        }
        if (stopped_.valid()) {
            stopped_.wait();
        }
        stopped_ = {};
        leaseScope_.reset();
        worker_ = {};
    }

    [[nodiscard]] bool failed() const noexcept { return failed_.load(); }

  private:
    static constexpr std::size_t kBatchSize = 512;
    static constexpr auto kLeaseRenewInterval = std::chrono::seconds(5);
    static constexpr auto kRecoverySweepInterval =
        std::chrono::seconds(1);

    void markLeaseLost(ruvia::web_worker_context& context) noexcept {
        failed_.store(true);
        if (leaseLost_.exchange(true)) {
            return;
        }
        try {
            diagnostics_.setComponentStatus("edge-projector", observability::ComponentState::Failed, "worker lease lost");
        } catch (...) {
        }
        try {
            // This is deliberately resolved from the current worker context;
            // no session or callback is routed through another Worker.
            context.worker_state<SessionDispatcher>().failSessions("worker lease lost");
        } catch (const std::exception& error) {
            std::cerr << "edge worker-local session shutdown failed: "
                      << error.what() << '\n';
        } catch (...) {
            std::cerr << "edge worker-local session shutdown failed: unknown error\n";
        }
        try {
            service::messaging::workerStreamMultiplexer().signal(
                service::message::WorkerStreamTask::EdgeProjector
            );
        } catch (...) {
        }
    }

    std::vector<std::pair<std::string, std::string>> recoveryLeaseSnapshot() const {
        std::vector<std::pair<std::string, std::string>> leases;
        leases.reserve(recoveryLeases_.size());
        for (const auto& [key, token] : recoveryLeases_) {
            leases.emplace_back(key, token);
        }
        return leases;
    }

    template <typename Redis>
    ruvia::task<void> maintainLeases(ruvia::web_worker_context& context, const Redis& redis, std::size_t index, ruvia::stop_token scopeStop) {
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), scopeStop);
        while (running_.load() && !stop.stop_requested()) {
            (void)co_await ruvia::sleep_for(
                context.worker(),
                kLeaseRenewInterval,
                stop
            );
            if (stop.stop_requested() || !running_.load()) {
                break;
            }
            try {
                if (!co_await renewLease(redis, index)) {
                    markLeaseLost(context);
                    co_return;
                }
                std::vector<std::string> lostRecoveryLeases;
                for (const auto& [key, token] : recoveryLeaseSnapshot()) {
                    if (!co_await renewLeaseKey(redis, key, token)) {
                        lostRecoveryLeases.push_back(key);
                    }
                }
                for (const auto& key : lostRecoveryLeases) {
                    recoveryLeases_.erase(key);
                    lostRecoveryLeases_.insert(key);
                }
                if (!lostRecoveryLeases.empty()) {
                    service::messaging::workerStreamMultiplexer().signal(
                        service::message::WorkerStreamTask::EdgeProjector
                    );
                }
            } catch (const std::exception& error) {
                markLeaseLost(context);
                std::cerr << "edge projector lease renewal failed: " << error.what() << '\n';
                co_return;
            }
        }
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t index, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        bool readySet = false;
        bool leaseAcquired = false;
        bool heartbeatStarted = false;
        try {
            const auto redis = context.redis();
            const auto currentStream = service::message::edge::projector_stream::stream(index, service::runtime::instanceId());
            const auto currentLease = service::message::edge::projector_stream::leaseKey(index, service::runtime::instanceId());
            const auto currentToken = service::message::edge::projector_stream::ownerToken(index, service::runtime::instanceId());
            recoveryLeases_.clear();
            lostRecoveryLeases_.clear();
            acquisitionConfigurations_.clear();
            leaseLost_.store(false);
            failed_.store(false);

            // Startup acquisition is deliberately separate from renewal.  NX
            // is the only operation allowed to establish this worker's lease;
            // all later heartbeats require the exact original token.
            if (!co_await acquireLease(redis, index)) {
                throw std::runtime_error(
                    "edge projector lease is owned by another worker"
                );
            }
            leaseAcquired = true;

            // The heartbeat has its own task on this same Worker.  Database
            // hydration and a large projection batch therefore cannot starve
            // the lease renewal loop.
            if (!leaseScope_) {
                throw std::runtime_error("edge projector lease scope is not ready");
            }
            leaseScope_->spawn(
                maintainLeases(context, redis, index, leaseScope_->get_stop_token())
            );
            heartbeatStarted = true;

            co_await cleanupRegistry(redis);
            co_await registerStream(redis, currentStream);

            std::vector<std::string> streams{ currentStream };
            std::map<std::string, ProjectorRecoveryStream, std::less<>> deadStreams;

            // A dead v3 owner is recovered only after its lease has expired,
            // and the recovery lease is acquired before its consumer group is
            // created or its first entry is read.
            for (auto& dead : co_await discoverDeadStreams(
                     redis,
                     currentStream,
                     index
                 )) {
                if (!co_await service::message::redis::ensureGroupIfPresent(
                        redis,
                        dead.stream,
                        kEdgeIngressGroup
                    )) {
                    (void)co_await releaseLeaseKey(redis, dead.lease, dead.token);
                    continue;
                }
                if (!co_await renewLeaseKey(redis, dead.lease, dead.token)) {
                    continue;
                }
                streams.push_back(dead.stream);
                recoveryLeases_[dead.lease] = dead.token;
                deadStreams.emplace(dead.stream, std::move(dead));
                co_await registerStream(redis, streams.back());
            }

            co_await service::message::redis::ensureGroup(
                redis,
                currentStream,
                kEdgeIngressGroup
            );
            co_await hydrateAuth(context);
            auto catalog = co_await metadata::hydrate(context);
            ready->set_value();
            readySet = true;

            auto nextFirmwareCleanup = std::chrono::steady_clock::now();

            bool recovering = true;
            const auto consumer = service::runtime::instanceId() + ":service-" +
                std::to_string(index);
            auto nextDiscovery = std::chrono::steady_clock::now();
            auto nextRecoverySweep = nextDiscovery;

            while (running_.load() && !context.get_stop_token().stop_requested() &&
                   !leaseLost_.load()) {
                // A recovery heartbeat loss removes that stream from the read
                // set before another message can be acknowledged for it.
                if (!lostRecoveryLeases_.empty()) {
                    for (auto dead = deadStreams.begin();
                         dead != deadStreams.end();) {
                        if (!lostRecoveryLeases_.contains(dead->second.lease)) {
                            ++dead;
                            continue;
                        }
                        streams.erase(
                            std::remove(streams.begin(), streams.end(), dead->first),
                            streams.end()
                        );
                        recoveryLeases_.erase(dead->second.lease);
                        dead = deadStreams.erase(dead);
                        recovering = true;
                    }
                    lostRecoveryLeases_.clear();
                }

                if (std::chrono::steady_clock::now() >= nextFirmwareCleanup) {
                    nextFirmwareCleanup = std::chrono::steady_clock::now() + std::chrono::minutes(1);
                    try {
                        co_await cleanupFirmwares(context, firmwareDirectory_);
                    } catch (const std::exception& error) {
                        std::cerr << "firmware cleanup failed: " << error.what() << '\n';
                    }
                }
                bool maintenanceFailed = false;
                try {
                    if (std::chrono::steady_clock::now() >= nextDiscovery) {
                        const auto discovered = co_await discoverDeadStreams(
                            redis,
                            currentStream,
                            index
                        );
                        std::set<std::string, std::less<>> discoveredNames;
                        for (const auto& dead : discovered) {
                            discoveredNames.insert(dead.stream);
                        }
                        for (auto dead = deadStreams.begin();
                             dead != deadStreams.end();) {
                            if (discoveredNames.contains(dead->first)) {
                                ++dead;
                                continue;
                            }
                            streams.erase(
                                std::remove(streams.begin(), streams.end(), dead->first),
                                streams.end()
                            );
                            recoveryLeases_.erase(dead->second.lease);
                            dead = deadStreams.erase(dead);
                            recovering = true;
                        }
                        for (auto& dead : discovered) {
                            if (deadStreams.contains(dead.stream)) {
                                continue;
                            }
                            if (!co_await service::message::redis::ensureGroupIfPresent(
                                    redis,
                                    dead.stream,
                                    kEdgeIngressGroup
                                )) {
                                (void)co_await releaseLeaseKey(
                                    redis,
                                    dead.lease,
                                    dead.token
                                );
                                continue;
                            }
                            if (!co_await renewLeaseKey(redis, dead.lease, dead.token)) {
                                continue;
                            }
                            const auto streamName = dead.stream;
                            recoveryLeases_[dead.lease] = dead.token;
                            deadStreams.emplace(streamName, std::move(dead));
                            streams.push_back(streamName);
                            co_await registerStream(redis, streamName);
                            recovering = true;
                            nextRecoverySweep = std::chrono::steady_clock::now();
                        }
                        nextDiscovery = std::chrono::steady_clock::now() +
                            kLeaseRenewInterval;
                    }

                } catch (const std::exception& error) {
                    std::cerr << "edge stream discovery failed: " << error.what() << '\n';
                    maintenanceFailed = true;
                }
                if (!running_.load() || context.get_stop_token().stop_requested() ||
                    leaseLost_.load()) {
                    break;
                }
                if (maintenanceFailed) {
                    (void)co_await ruvia::sleep_for(
                        context.worker(), std::chrono::milliseconds(250)
                    );
                    continue;
                }

                std::vector<service::message::redis::StreamBatch> batches;
                bool readFailed = false;
                bool groupMissing = false;
                try {
                    if (!deadStreams.empty() &&
                        std::chrono::steady_clock::now() >= nextRecoverySweep) {
                        std::vector<std::string> recoveryStreams;
                        recoveryStreams.reserve(deadStreams.size());
                        for (const auto& [stream, dead] : deadStreams) {
                            (void)dead;
                            recoveryStreams.push_back(stream);
                        }
                        // A recovery lease fences the crashed owner, so its
                        // PEL is safe to sweep immediately.  The ordinary
                        // shared read set keeps its normal idle threshold.
                        batches = co_await service::message::redis::claimGroupMany(
                            redis,
                            recoveryStreams,
                            kEdgeIngressGroup,
                            consumer,
                            kBatchSize,
                            std::chrono::milliseconds(0)
                        );
                        nextRecoverySweep = std::chrono::steady_clock::now() +
                            kRecoverySweepInterval;
                    }
                    if (batches.empty()) {
                        batches = recovering
                            ? co_await service::message::redis::claimGroupMany(
                                  redis,
                                  streams,
                                  kEdgeIngressGroup,
                                  consumer,
                                  kBatchSize
                              )
                            : co_await service::message::redis::readGroupMany(
                                  redis,
                                  streams,
                                  kEdgeIngressGroup,
                                  consumer,
                                  ">",
                                  kBatchSize
                              );
                    }
                } catch (const std::exception& error) {
                    if (!running_.load() || context.get_stop_token().stop_requested() ||
                        leaseLost_.load()) {
                        break;
                    }
                    std::cerr << "edge stream read failed: " << error.what() << '\n';
                    recovering = true;
                    readFailed = true;
                    groupMissing = std::string_view(error.what()).find("NOGROUP") !=
                        std::string_view::npos;
                }
                if (groupMissing && running_.load() &&
                    !context.get_stop_token().stop_requested() && !leaseLost_.load()) {
                    try {
                        co_await service::message::redis::ensureGroup(
                            redis,
                            currentStream,
                            kEdgeIngressGroup
                        );
                        for (auto dead = deadStreams.begin();
                             dead != deadStreams.end();) {
                            if (co_await service::message::redis::ensureGroupIfPresent(
                                    redis,
                                    dead->first,
                                    kEdgeIngressGroup
                                )) {
                                ++dead;
                                continue;
                            }
                            streams.erase(
                                std::remove(streams.begin(), streams.end(), dead->first),
                                streams.end()
                            );
                            recoveryLeases_.erase(dead->second.lease);
                            dead = deadStreams.erase(dead);
                            nextRecoverySweep = std::chrono::steady_clock::now();
                        }
                        readFailed = false;
                    } catch (const std::exception& ensureError) {
                        std::cerr << "edge stream group recovery failed: "
                                  << ensureError.what() << '\n';
                    }
                }
                if (!running_.load() || context.get_stop_token().stop_requested() ||
                    leaseLost_.load()) {
                    break;
                }
                if (readFailed) {
                    (void)co_await ruvia::sleep_for(
                        context.worker(),
                        std::chrono::milliseconds(250)
                    );
                    continue;
                }
                if (recovering && batches.empty()) {
                    recovering = false;
                }
                if (batches.empty()) {
                    try {
                        for (auto dead = deadStreams.begin();
                             dead != deadStreams.end();) {
                            if (co_await eraseDeadStream(redis, dead->second)) {
                                streams.erase(
                                    std::remove(streams.begin(), streams.end(), dead->first),
                                    streams.end()
                                );
                                recoveryLeases_.erase(dead->second.lease);
                                dead = deadStreams.erase(dead);
                            } else {
                                ++dead;
                            }
                        }
                    } catch (const std::exception& error) {
                        std::cerr << "edge stream cleanup failed: " << error.what() << '\n';
                        maintenanceFailed = true;
                    }
                    if (!running_.load() || context.get_stop_token().stop_requested() ||
                        leaseLost_.load()) {
                        break;
                    }
                    if (maintenanceFailed) {
                        (void)co_await ruvia::sleep_for(
                            context.worker(),
                            std::chrono::milliseconds(250)
                        );
                        continue;
                    }
                    co_await service::messaging::workerStreamMultiplexer().wait(
                        index,
                        service::message::WorkerStreamTask::EdgeProjector,
                        context.get_stop_token(),
                        kLeaseRenewInterval
                    );
                    continue;
                }

                bool projectionFailed = false;
                try {
                    for (const auto& batch : batches) {
                        if (leaseLost_.load()) {
                            break;
                        }
                        const ProjectorRecoveryStream* owner = nullptr;
                        ProjectorRecoveryStream currentOwner{
                            currentStream,
                            currentLease,
                            currentToken
                        };
                        if (batch.stream == currentStream) {
                            owner = &currentOwner;
                        } else if (const auto dead = deadStreams.find(batch.stream);
                                   dead != deadStreams.end()) {
                            owner = &dead->second;
                        }
                        if (!owner) {
                            throw std::runtime_error(
                                "edge projector batch is outside the owned stream set"
                            );
                        }
                        if (!co_await leaseOwned(redis, owner->lease, owner->token)) {
                            if (batch.stream == currentStream) {
                                markLeaseLost(context);
                            } else {
                                lostRecoveryLeases_.insert(owner->lease);
                                recovering = true;
                            }
                            break;
                        }

                        std::vector<service::message::StreamMessage> telemetry;
                        std::vector<persistence::TelemetryUploadRecord> completedUploads;
                        std::map<std::string, std::string> rawCalculations;
                        telemetry.reserve(batch.messages.size());
                        for (const auto& message : batch.messages) {
                            if (leaseLost_.load()) {
                                break;
                            }
                            const bool metadataEvent =
                                message.get("kind") == service::message::edge::projector_stream::kMetadataKind;
                            if (metadataEvent) {
                                const auto nodeId = message.get("node_id");
                                if (!nodeId.empty()) {
                                    acquisitionConfigurations_.erase(std::string(nodeId));
                                    for (auto it = derivedExpiry_.begin(); it != derivedExpiry_.end();) {
                                        if (it->second.nodeId == nodeId) {
                                            it->second.stop->request_stop();
                                            it = derivedExpiry_.erase(it);
                                        } else ++it;
                                    }
                                    auto snapshot = co_await metadata::loadNodeFromDatabase(
                                        context,
                                        nodeId
                                    );
                                    co_await metadata::storeNode(
                                        redis,
                                        nodeId,
                                        snapshot,
                                        false
                                    );
                                    catalog[std::string(nodeId)] = std::move(snapshot);
                                }
                                continue;
                            }
                            const bool ingressEvent =
                                message.get("kind") == service::message::edge::projector_stream::kIngressKind;
                            if (ingressEvent) {
                                co_await project(
                                    context,
                                    catalog,
                                    message.get("wire"),
                                    message.get("received_at_ms"),
                                    telemetry,
                                    completedUploads,
                                    rawCalculations
                                );
                            }
                        }
                        if (leaseLost_.load()) {
                            break;
                        }
                        for (auto it = telemetry.begin(); it != telemetry.end();) {
                            auto parsed = service::message::parsedFrom(*it);
                            const auto calculation = rawCalculations.find(parsed.messageId);
                            if (calculation == rawCalculations.end()) { ++it; continue; }
                            const auto result = co_await service::telemetry::derived::DerivedService::append(
                                redis, calculation->second, parsed, false, true);
                            scheduleDerivedExpiry(context, parsed, calculation->second, result.nextDeadline);
                            if (result.published) it = telemetry.erase(it);
                            else ++it;
                        }
                        co_await service::telemetry::TelemetryService::ingest(
                            context,
                            telemetry
                        );
                        co_await finishTelemetryUploads(redis, completedUploads);
                        service::messaging::workerStreamMultiplexer().signalTelemetryConsumers();
                        if (leaseLost_.load()) {
                            break;
                        }
                        if (!co_await acknowledgeAndDeleteFenced(
                                redis,
                                owner->lease,
                                owner->token,
                                batch.stream,
                                kEdgeIngressGroup,
                                batch.messages
                            )) {
                            if (batch.stream == currentStream) {
                                markLeaseLost(context);
                            } else {
                                lostRecoveryLeases_.insert(owner->lease);
                                recovering = true;
                            }
                            break;
                        }
                    }
                } catch (const std::exception& error) {
                    recovering = true;
                    projectionFailed = true;
                    std::cerr << "edge projection failed: " << error.what() << '\n';
                }
                if (leaseLost_.load()) {
                    break;
                }
                if (projectionFailed) {
                    (void)co_await ruvia::sleep_for(
                        context.worker(),
                        std::chrono::milliseconds(250)
                    );
                }
            }
        } catch (...) {
            if (!readySet) {
                try {
                    ready->set_exception(std::current_exception());
                } catch (...) {
                }
            }
        }

        for (const auto& [id, expiry] : derivedExpiry_) expiry.stop->request_stop();
        derivedExpiry_.clear();
        if (heartbeatStarted && leaseScope_) {
            leaseScope_->request_stop();
            try {
                co_await leaseScope_->join();
            } catch (const std::exception& error) {
                std::cerr << "edge projector heartbeat stopped with error: "
                          << error.what() << '\n';
            } catch (...) {
                std::cerr << "edge projector heartbeat stopped with unknown error\n";
            }
        }
        if (leaseAcquired) {
            try {
                const auto redis = context.redis();
                for (const auto& [key, token] : recoveryLeaseSnapshot()) {
                    (void)co_await releaseLeaseKey(redis, key, token);
                }
                (void)co_await releaseLease(redis, index);
            } catch (const std::exception& error) {
                std::cerr << "edge projector lease release failed: " << error.what()
                          << '\n';
            } catch (...) {
                std::cerr << "edge projector lease release failed: unknown error\n";
            }
        }
        running_.store(false);
        try {
            stopped->set_value();
        } catch (...) {
        }
    }

    void scheduleDerivedExpiry(ruvia::web_worker_context& context, service::message::ParsedDeviceMessage parsed,
                               std::string config, std::optional<std::int64_t> deadline) {
        const auto id = parsed.deviceId;
        if (const auto old = derivedExpiry_.find(id); old != derivedExpiry_.end()) {
            old->second.stop->request_stop();
            derivedExpiry_.erase(old);
        }
        if (!deadline || !running_.load() || leaseLost_.load()) return;
        auto stop = std::make_shared<ruvia::stop_source>();
        derivedExpiry_[id] = {parsed.connectionId, stop};
        leaseScope_->spawn(expireDerived(context, std::move(parsed), std::move(config), *deadline, stop));
    }

    ruvia::task<void> expireDerived(ruvia::web_worker_context& context, service::message::ParsedDeviceMessage parsed,
                                  std::string config, std::int64_t deadline, std::shared_ptr<ruvia::stop_source> stop) {
        const auto token = ruvia::combine_stop_tokens(
            ruvia::combine_stop_tokens(context.get_stop_token(), leaseScope_->get_stop_token()), stop->token());
        (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(
            (std::max)(std::int64_t{1}, deadline - service::message::utcNowMilliseconds())), token);
        if (token.stop_requested() || !running_.load() || leaseLost_.load()) co_return;
        derivedExpiry_.erase(parsed.deviceId);
        parsed.messageId = derivedUuid_.next();
        parsed.acquisitionId = parsed.causationId = parsed.messageId;
        parsed.source = "derived";
        parsed.rawPayloads.clear(); parsed.rawPacketIds.clear();
        parsed.observedAtMs = parsed.occurredAtMs = service::message::utcNowMilliseconds();
        parsed.valuesJson = "{\"values\":{}}";
        try {
            const auto result = co_await service::telemetry::derived::DerivedService::append(context.redis(), config, parsed, true);
            scheduleDerivedExpiry(context, std::move(parsed), std::move(config), result.nextDeadline);
        } catch (const std::exception& error) {
            std::cerr << "edge derived expiry failed: " << error.what() << '\n';
            scheduleDerivedExpiry(context, std::move(parsed), std::move(config), service::message::utcNowMilliseconds() + 1000);
        }
    }

    struct DerivedExpiry final {
        std::string nodeId;
        std::shared_ptr<ruvia::stop_source> stop;
    };
    std::map<std::string, DerivedExpiry> derivedExpiry_;
    service::common::UuidV7Generator derivedUuid_;
    ruvia::web_worker_handle worker_;
    std::shared_future<void> stopped_;
    std::unique_ptr<ruvia::task_scope> leaseScope_;
    std::size_t workerIndex_ = 0;
    std::size_t serviceWorkerCount_ = 0;
    std::atomic_bool running_{ false };
    std::atomic_bool leaseLost_{ false };
    std::atomic_bool failed_{ false };
    observability::RuntimeDiagnostics& diagnostics_;
    const std::filesystem::path firmwareDirectory_;
    std::map<std::string, std::string, std::less<>> recoveryLeases_;
    std::set<std::string, std::less<>> lostRecoveryLeases_;
};

} // namespace service::edge
