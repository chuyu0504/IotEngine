#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
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

#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/features/edge/projection/projection.service.h"
#include "service/utils/redis.h"
#include "service/utils/worker_task.h"

namespace service::edge::projection {

// Consumes this Worker's projector Stream (and recovers Streams of expired
// owners) under a fenced lease. One instance per Service Worker; its state is
// Worker-local and never shared.
class ProjectionRuntime final {
  public:
    explicit ProjectionRuntime(observability::RuntimeDiagnostics& diagnostics) : diagnostics_(diagnostics) {}

    ProjectionRuntime(const ProjectionRuntime&) = delete;
    ProjectionRuntime& operator=(const ProjectionRuntime&) = delete;

    ~ProjectionRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (running_.exchange(true)) {
            return;
        }
        failed_.store(false);
        if (!worker.valid()) {
            running_.store(false);
            throw std::runtime_error("edge projector requires Service Workers");
        }
        stopSource_.emplace();
        try {
            service::utils::startWorkerTask(std::move(worker), stopSource_->token(), stopped_, [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                stop_ = stop;
                leaseScope_ = std::make_unique<ruvia::task_scope>(context.worker());
                return run(context, ready);
            });
        } catch (...) {
            stop();
            throw;
        }
    }

    void stop() noexcept {
        running_.store(false);
        if (stopSource_) {
            service::utils::stopWorkerTask(*stopSource_, stopped_);
            leaseScope_.reset();
            stopSource_.reset();
        }
    }

    [[nodiscard]] bool failed() const noexcept { return failed_.load(); }

  private:
    static constexpr std::size_t kBatchSize = 512;
    static constexpr auto kLeaseRenewInterval = std::chrono::seconds(5);
    static constexpr auto kRetryDelay = std::chrono::milliseconds(250);

    // Streams read by this Worker: its own plus recovered Streams of expired owners.
    struct ReadSet final {
        ProjectorStreamOwner current;
        std::vector<std::string> streams;
        std::map<std::string, ProjectorStreamOwner, std::less<>> dead;
        bool recovering{ true };
        std::size_t revision = 0;
        std::chrono::steady_clock::time_point nextDiscovery{};

        void add(ProjectorStreamOwner owner) {
            streams.push_back(owner.stream);
            dead.emplace(owner.stream, std::move(owner));
            ++revision;
        }

        std::map<std::string, ProjectorStreamOwner, std::less<>>::iterator
        drop(std::map<std::string, ProjectorStreamOwner, std::less<>>::iterator entry) {
            streams.erase(std::remove(streams.begin(), streams.end(), entry->first), streams.end());
            ++revision;
            return dead.erase(entry);
        }
    };

    struct DerivedExpiry final {
        std::string nodeId;
        std::shared_ptr<ruvia::stop_source> stop;
    };

    bool active(ruvia::web_worker_context& context) const {
        return running_.load() && !stop_.stop_requested() && !leaseLost_.load();
    }

    ruvia::task<void> pause(ruvia::web_worker_context& context) {
        (void)co_await ruvia::sleep_for(context.worker(), kRetryDelay, stop_);
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, service::utils::WorkerReady& ready) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        workerIndex_ = identity.index;
        instance_ = identity.instanceId;
        ReadSet reads{ .current = ProjectorStreams::current(workerIndex_, instance_) };
        std::exception_ptr failure;
        try {
            co_await acquire(context, reads);
            while (active(context)) {
                const auto revision = reads.revision;
                const auto notificationStreams = reads.streams;
                co_await service::utils::redis::withConsumerNotifications(
                    context.redis(),
                    context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                    context.worker(),
                    notificationStreams,
                    kIngressGroup,
                    stop_,
                    [this, &context, &reads, &ready, revision](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
                        ready.set_value();
                        co_await consume(context, reads, notifications, revision);
                    }
                );
            }
        } catch (...) {
            failed_.store(true);
            failure = std::current_exception();
        }
        co_await release(context, reads);
        running_.store(false);
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    // Takes this Worker's lease, starts its renewal, adopts dead Streams and
    // hydrates the Worker-local catalog before the first read.
    ruvia::task<void> acquire(ruvia::web_worker_context& context, ReadSet& reads) {
        const auto redis = context.redis();
        recoveryLeases_.clear();
        lostRecoveryLeases_.clear();
        state_ = {};
        leaseLost_.store(false);
        failed_.store(false);
        // NX is the only operation allowed to establish this Worker's lease; all
        // later renewals require the exact original token.
        if (!co_await ProjectorStreams::claimLease(redis, reads.current.lease, reads.current.token)) {
            throw std::runtime_error("edge projector lease is owned by another worker");
        }
        leaseAcquired_ = true;
        if (!leaseScope_) {
            throw std::runtime_error("edge projector lease scope is not ready");
        }
        // Renewal runs as its own task on this Worker so hydration and large
        // batches cannot starve it.
        leaseScope_->spawn(maintainLeases(context, reads.current, leaseScope_->get_stop_token()));
        heartbeatStarted_ = true;

        co_await ProjectorStreams::registerStream(redis, reads.current.stream);
        reads.streams.push_back(reads.current.stream);
        co_await adoptDeadStreams(redis, reads);
        co_await service::utils::redis::ensureGroup(redis, reads.current.stream, kIngressGroup);
        co_await ProjectionService::initialize(context, state_);
        reads.nextDiscovery = std::chrono::steady_clock::now();
    }

    // A dead owner is recovered only after its lease expired; the recovery lease
    // is held before its consumer group is created or its first entry is read.
    template <typename Redis>
    ruvia::task<void> adoptDeadStreams(const Redis& redis, ReadSet& reads) {
        const auto discovered =
            co_await ProjectorStreams::discoverDeadStreams(redis, reads.current.stream, workerIndex_, instance_);
        for (const auto& dead : discovered) {
            if (reads.dead.contains(dead.stream)) {
                continue;
            }
            if (!co_await service::utils::redis::ensureGroupIfPresent(redis, dead.stream, kIngressGroup)) {
                (void)co_await ProjectorStreams::eraseDeadStream(redis, dead);
                continue;
            }
            if (!co_await ProjectorStreams::renewLease(redis, dead.lease, dead.token)) {
                continue;
            }
            recoveryLeases_[dead.lease] = dead.token;
            co_await ProjectorStreams::registerStream(redis, dead.stream);
            reads.add(dead);
            reads.recovering = true;
        }
    }

    ruvia::task<void> consume(ruvia::web_worker_context& context, ReadSet& reads, ruvia::channel_receiver<std::uint8_t>& notifications, std::size_t revision) {
        const auto redis = context.redis();
        const auto consumer = instance_ + ":service-" + std::to_string(workerIndex_);
        while (active(context)) {
            dropLostRecoveryStreams(reads);
            if (reads.revision != revision) {
                co_return;
            }
            bool discoveryFailed = false;
            try {
                if (std::chrono::steady_clock::now() >= reads.nextDiscovery) {
                    co_await adoptDeadStreams(redis, reads);
                    reads.nextDiscovery = std::chrono::steady_clock::now() + co_await ProjectorStreams::nextRecoveryDelay(redis);
                }
            } catch (const std::exception& error) {
                std::cerr << "edge stream discovery failed: " << error.what() << '\n';
                discoveryFailed = true;
            }
            if (discoveryFailed) {
                if (active(context)) {
                    co_await pause(context);
                }
                continue;
            }
            if (reads.revision != revision) {
                co_return;
            }
            if (!active(context)) {
                break;
            }

            const auto batches = co_await read(context, reads, consumer, notifications);
            if (!batches) {
                continue;
            }
            if (batches->empty()) {
                if (!co_await eraseDrainedStreams(context, reads)) {
                    continue;
                }
                continue;
            }
            bool projectionFailed = false;
            try {
                for (const auto& batch : *batches) {
                    if (!co_await projectBatch(context, reads, batch)) {
                        break;
                    }
                }
            } catch (const std::exception& error) {
                reads.recovering = true;
                std::cerr << "edge projection failed: " << error.what() << '\n';
                projectionFailed = true;
            }
            if (projectionFailed && !leaseLost_.load()) {
                co_await pause(context);
            }
        }
    }

    // Reads the next batches; std::nullopt means the read failed and was retried later.
    ruvia::task<std::optional<std::vector<service::utils::redis::StreamBatch>>>
    read(ruvia::web_worker_context& context, ReadSet& reads, const std::string& consumer, ruvia::channel_receiver<std::uint8_t>& notifications) {
        std::optional<std::vector<service::utils::redis::StreamBatch>> batches;
        try {
            const auto delay = std::max(std::chrono::milliseconds(1), std::chrono::duration_cast<std::chrono::milliseconds>(reads.nextDiscovery - std::chrono::steady_clock::now()));
            batches = co_await service::utils::redis::consumeGroup(
                context.redis(),
                context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                reads.streams,
                kIngressGroup,
                consumer,
                reads.recovering,
                notifications,
                stop_,
                delay,
                kBatchSize,
                std::chrono::milliseconds(0)
            );
        } catch (const std::exception& error) {
            if (active(context)) {
                std::cerr << "edge stream read failed: " << error.what() << '\n';
                reads.recovering = true;
            }
        }
        if (!batches && active(context)) {
            co_await pause(context);
        }
        co_return batches;
    }

    // Releases recovered Streams that are fully drained. Returns false after a failure.
    ruvia::task<bool> eraseDrainedStreams(ruvia::web_worker_context& context, ReadSet& reads) {
        try {
            for (auto dead = reads.dead.begin(); dead != reads.dead.end();) {
                if (co_await ProjectorStreams::eraseDeadStream(context.redis(), dead->second)) {
                    recoveryLeases_.erase(dead->second.lease);
                    dead = reads.drop(dead);
                } else {
                    ++dead;
                }
            }
            co_return true;
        } catch (const std::exception& error) {
            std::cerr << "edge stream cleanup failed: " << error.what() << '\n';
        }
        if (active(context)) {
            co_await pause(context);
        }
        co_return false;
    }

    // A lost recovery lease removes its Stream from the read set before any
    // further entry of that Stream can be acknowledged.
    void dropLostRecoveryStreams(ReadSet& reads) {
        if (lostRecoveryLeases_.empty()) {
            return;
        }
        for (auto dead = reads.dead.begin(); dead != reads.dead.end();) {
            if (!lostRecoveryLeases_.contains(dead->second.lease)) {
                ++dead;
                continue;
            }
            recoveryLeases_.erase(dead->second.lease);
            dead = reads.drop(dead);
            reads.recovering = true;
        }
        lostRecoveryLeases_.clear();
    }

    // Projects and acknowledges one Stream batch under its lease. Returns false
    // when the lease is gone and the remaining batches must not be processed.
    ruvia::task<bool> projectBatch(ruvia::web_worker_context& context, ReadSet& reads, const service::utils::redis::StreamBatch& batch) {
        const auto redis = context.redis();
        if (leaseLost_.load()) {
            co_return false;
        }
        const ProjectorStreamOwner* owner = nullptr;
        if (batch.stream == reads.current.stream) {
            owner = &reads.current;
        } else if (const auto dead = reads.dead.find(batch.stream); dead != reads.dead.end()) {
            owner = &dead->second;
        }
        if (!owner) {
            throw std::runtime_error("edge projector batch is outside the owned stream set");
        }
        if (!co_await ProjectorStreams::leaseOwned(redis, owner->lease, owner->token)) {
            lostLease(context, reads, *owner);
            co_return false;
        }

        ProjectionBatch projected;
        projected.telemetry.reserve(batch.messages.size());
        for (const auto& message : batch.messages) {
            if (leaseLost_.load()) {
                co_return false;
            }
            const auto kind = message.get("kind");
            if (kind == service::message::edge::projector_stream::kMetadataKind) {
                const auto nodeId = message.get("node_id");
                if (!nodeId.empty()) {
                    cancelDerivedExpiries(nodeId);
                    co_await ProjectionService::refreshMetadata(context, state_, nodeId);
                }
            } else if (kind == service::message::edge::projector_stream::kIngressKind) {
                co_await ProjectionService::project(context, workerIndex_, state_, message.get("wire"), message.get("received_at_ms"), projected);
            }
        }
        if (leaseLost_.load()) {
            co_return false;
        }
        for (auto& expiry : co_await ProjectionService::publishTelemetry(context, projected)) {
            scheduleDerivedExpiry(context, std::move(expiry.parsed), std::move(expiry.config), expiry.deadline);
        }
        if (leaseLost_.load()) {
            co_return false;
        }
        if (!co_await ProjectorStreams::acknowledgeFenced(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, *owner, batch.messages)) {
            lostLease(context, reads, *owner);
            co_return false;
        }
        co_return true;
    }

    void lostLease(ruvia::web_worker_context& context, ReadSet& reads, const ProjectorStreamOwner& owner) {
        if (owner.stream == reads.current.stream) {
            markLeaseLost(context);
        } else {
            lostRecoveryLeases_.insert(owner.lease);
            reads.recovering = true;
        }
    }

    // Stops the heartbeat and releases every lease this Worker still holds.
    ruvia::task<void> release(ruvia::web_worker_context& context, const ReadSet& reads) {
        for (const auto& [id, expiry] : derivedExpiry_) {
            expiry.stop->request_stop();
        }
        derivedExpiry_.clear();
        if (heartbeatStarted_ && leaseScope_) {
            leaseScope_->request_stop();
            try {
                co_await leaseScope_->join();
            } catch (const std::exception& error) {
                std::cerr << "edge projector heartbeat stopped with error: " << error.what() << '\n';
            } catch (...) {
                std::cerr << "edge projector heartbeat stopped with unknown error\n";
            }
        }
        heartbeatStarted_ = false;
        if (!leaseAcquired_) {
            co_return;
        }
        leaseAcquired_ = false;
        try {
            const auto redis = context.redis();
            for (const auto& [key, token] : recoveryLeaseSnapshot()) {
                (void)co_await ProjectorStreams::releaseLease(redis, key, token);
            }
            (void)co_await ProjectorStreams::releaseLease(redis, reads.current.lease, reads.current.token);
        } catch (const std::exception& error) {
            std::cerr << "edge projector lease release failed: " << error.what() << '\n';
        } catch (...) {
            std::cerr << "edge projector lease release failed: unknown error\n";
        }
    }

    void markLeaseLost(ruvia::web_worker_context& context) noexcept {
        failed_.store(true);
        if (leaseLost_.exchange(true)) {
            return;
        }
        try {
            ProjectionService::failWorker(context, diagnostics_);
        } catch (const std::exception& error) {
            std::cerr << "edge projector worker failure update failed: " << error.what() << '\n';
        } catch (...) {
            std::cerr << "edge projector worker failure update failed: unknown error\n";
        }
        stopSource_->request_stop();
    }

    std::vector<std::pair<std::string, std::string>> recoveryLeaseSnapshot() const {
        return { recoveryLeases_.begin(), recoveryLeases_.end() };
    }

    ruvia::task<void> maintainLeases(ruvia::web_worker_context& context, ProjectorStreamOwner current, ruvia::stop_token scopeStop) {
        const auto redis = context.redis();
        const auto stop = ruvia::combine_stop_tokens(stop_, scopeStop);
        while (running_.load() && !stop.stop_requested()) {
            (void)co_await ruvia::sleep_for(context.worker(), kLeaseRenewInterval, stop);
            if (stop.stop_requested() || !running_.load()) {
                break;
            }
            try {
                if (!co_await ProjectorStreams::renewLease(redis, current.lease, current.token)) {
                    markLeaseLost(context);
                    co_return;
                }
                std::vector<std::string> lost;
                for (const auto& [key, token] : recoveryLeaseSnapshot()) {
                    if (!co_await ProjectorStreams::renewLease(redis, key, token)) {
                        lost.push_back(key);
                    }
                }
                for (const auto& key : lost) {
                    recoveryLeases_.erase(key);
                    lostRecoveryLeases_.insert(key);
                }
            } catch (const std::exception& error) {
                markLeaseLost(context);
                std::cerr << "edge projector lease renewal failed: " << error.what() << '\n';
                co_return;
            }
        }
    }

    void cancelDerivedExpiries(std::string_view nodeId) {
        for (auto expiry = derivedExpiry_.begin(); expiry != derivedExpiry_.end();) {
            if (expiry->second.nodeId == nodeId) {
                expiry->second.stop->request_stop();
                expiry = derivedExpiry_.erase(expiry);
            } else {
                ++expiry;
            }
        }
    }

    void scheduleDerivedExpiry(ruvia::web_worker_context& context, service::message::ParsedDeviceMessage parsed, std::string config, std::optional<std::int64_t> deadline) {
        const auto id = parsed.deviceId;
        if (const auto old = derivedExpiry_.find(id); old != derivedExpiry_.end()) {
            old->second.stop->request_stop();
            derivedExpiry_.erase(old);
        }
        if (!deadline || !running_.load() || leaseLost_.load()) {
            return;
        }
        auto stop = std::make_shared<ruvia::stop_source>();
        derivedExpiry_[id] = { parsed.connectionId, stop };
        leaseScope_->spawn(expireDerived(context, std::move(parsed), std::move(config), *deadline, stop));
    }

    ruvia::task<void> expireDerived(ruvia::web_worker_context& context, service::message::ParsedDeviceMessage parsed, std::string config, std::int64_t deadline, std::shared_ptr<ruvia::stop_source> stop) {
        const auto token = ruvia::combine_stop_tokens(
            ruvia::combine_stop_tokens(stop_, leaseScope_->get_stop_token()),
            stop->token()
        );
        (void)co_await ruvia::sleep_for(
            context.worker(),
            std::chrono::milliseconds((std::max)(std::int64_t{ 1 }, deadline - service::message::utcNowMilliseconds())),
            token
        );
        if (token.stop_requested() || !running_.load() || leaseLost_.load()) {
            co_return;
        }
        derivedExpiry_.erase(parsed.deviceId);
        try {
            const auto next = co_await ProjectionService::expireDerived(context, parsed, config);
            scheduleDerivedExpiry(context, std::move(parsed), std::move(config), next);
        } catch (const std::exception& error) {
            std::cerr << "edge derived expiry failed: " << error.what() << '\n';
            scheduleDerivedExpiry(context, std::move(parsed), std::move(config), service::message::utcNowMilliseconds() + 1000);
        }
    }

    observability::RuntimeDiagnostics& diagnostics_;
    ProjectionState state_;
    std::map<std::string, DerivedExpiry> derivedExpiry_;
    std::string instance_;
    std::optional<ruvia::stop_source> stopSource_;
    ruvia::stop_token stop_;
    std::future<void> stopped_;
    std::unique_ptr<ruvia::task_scope> leaseScope_;
    std::size_t workerIndex_ = 0;
    std::atomic_bool running_{ false };
    std::atomic_bool leaseLost_{ false };
    std::atomic_bool failed_{ false };
    bool leaseAcquired_{ false };
    bool heartbeatStarted_{ false };
    std::map<std::string, std::string, std::less<>> recoveryLeases_;
    std::set<std::string, std::less<>> lostRecoveryLeases_;
};

} // namespace service::edge::projection
