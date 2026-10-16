#pragma once

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
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

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/edge/edge.runtime.h"
#include "service/features/edge/projection/projection.service.h"
#include "service/features/edge/projection/projection.types.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"
#include "service/features/observability/observability.service.h"
#include "service/features/telemetry/derived/derived.service.h"
#include "service/features/telemetry/telemetry.service.h"
#include "service/utils/redis.h"

namespace service::edge::projection {

inline constexpr std::string_view kIngressGroup{ "iot-engine:edge-projector" };
inline constexpr auto kLeaseTtl = std::chrono::milliseconds(15000);

// SET NX: establishes a Worker lease or a recovery lease of an expired owner.
inline constexpr std::string_view kClaimLeaseScript = R"lua(
if redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') then return 1 end
return 0
)lua";

// Token-checked renewal; never recreates an expired lease.
inline constexpr std::string_view kRenewLeaseScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
)lua";

inline constexpr std::string_view kLeaseOwnedScript = R"lua(
if redis.call('GET', KEYS[1]) == ARGV[1] then return 1 end
return 0
)lua";

inline constexpr std::string_view kReleaseLeaseScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
)lua";

// Deletes a recovered Stream only after it is empty and has no pending entries.
inline constexpr std::string_view kEraseDeadStreamScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) ~= 0 then
  if redis.call('XLEN', KEYS[2]) ~= 0 then return 0 end
  local groups = redis.call('XINFO', 'GROUPS', KEYS[2])
  for _, group in ipairs(groups) do
    for index = 1, #group, 2 do
      if group[index] == 'pending' and tonumber(group[index + 1]) > 0 then
        return 0
      end
    end
  end
  redis.call('DEL', KEYS[2])
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[3], KEYS[2])
return 1
)lua";

// Acknowledges projected entries only while the consuming lease is still owned.
inline constexpr std::string_view kAcknowledgeFencedScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local group = ARGV[2]
for index = 3, #ARGV do
  redis.call('XACK', KEYS[2], group, ARGV[index])
  redis.call('XDEL', KEYS[2], ARGV[index])
end
return 1
)lua";

inline constexpr std::string_view kCleanupRegistryScript = R"lua(
if redis.call('EXISTS', KEYS[2]) == 0 then
  return redis.call('SREM', KEYS[1], KEYS[2])
end
return 0
)lua";

// Lease, fencing and dead-Stream recovery of the projector Streams in Redis.
class ProjectorStreams final {
  public:
    static ProjectorStreamOwner current(std::size_t workerIndex) {
        const auto& instance = service::runtime::instanceId();
        return { service::message::edge::projector_stream::stream(workerIndex, instance),
                 service::message::edge::projector_stream::leaseKey(workerIndex, instance),
                 service::message::edge::projector_stream::ownerToken(workerIndex, instance) };
    }

    template <typename Redis>
    static ruvia::task<bool> claimLease(const Redis& redis, std::string_view key, std::string_view token) {
        co_return co_await leaseScript(redis, kClaimLeaseScript, key, token, true, "claim edge projector lease");
    }

    template <typename Redis>
    static ruvia::task<bool> renewLease(const Redis& redis, std::string_view key, std::string_view token) {
        co_return co_await leaseScript(redis, kRenewLeaseScript, key, token, true, "renew edge projector lease");
    }

    template <typename Redis>
    static ruvia::task<bool> leaseOwned(const Redis& redis, std::string_view key, std::string_view token) {
        co_return co_await leaseScript(redis, kLeaseOwnedScript, key, token, false, "check edge projector lease");
    }

    template <typename Redis>
    static ruvia::task<bool> releaseLease(const Redis& redis, std::string_view key, std::string_view token) {
        co_return co_await leaseScript(redis, kReleaseLeaseScript, key, token, false, "release edge projector lease");
    }

    template <typename Redis>
    static ruvia::task<void> registerStream(const Redis& redis, std::string_view streamName) {
        const std::vector<std::string> arguments{
            "SADD",
            std::string(service::message::edge::projector_stream::kStreamRegistry),
            std::string(streamName)
        };
        const auto reply = co_await service::message::redis::command(redis, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("register edge projector stream", reply);
        }
    }

    // Drops registry members whose Stream no longer exists.
    template <typename Redis>
    static ruvia::task<void> cleanupRegistry(const Redis& redis) {
        const std::string registry(service::message::edge::projector_stream::kStreamRegistry);
        const std::vector<std::string> arguments{ "SMEMBERS", registry };
        const auto reply = co_await service::message::redis::command(redis, arguments);
        if (reply.kind() == ruvia::redis_value::kind_type::null) {
            co_return;
        }
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            service::message::redis::throwValue("list edge projector streams", reply);
        }
        for (const auto& value : reply.array()) {
            if (value.kind() != ruvia::redis_value::kind_type::string) {
                continue;
            }
            const std::string streamName(value.string());
            const std::string_view keys[]{ registry, streamName };
            const auto removed = co_await redis.eval(kCleanupRegistryScript, keys, std::span<const std::string_view>{});
            if (removed.kind() != ruvia::redis_value::kind_type::integer) {
                service::message::redis::throwValue("clean edge projector registry", removed);
            }
        }
    }

    // Owner (instance, Worker index) encoded in a projector Stream name.
    static std::optional<std::pair<std::string, std::size_t>> ownerFromStream(std::string_view streamName) {
        if (!streamName.starts_with(service::message::edge::projector_stream::kStreamPrefix)) {
            return std::nullopt;
        }
        const auto suffix = streamName.substr(service::message::edge::projector_stream::kStreamPrefix.size());
        const auto separator = suffix.rfind(':');
        if (separator == std::string_view::npos || separator == 0 || separator + 1 >= suffix.size()) {
            return std::nullopt;
        }
        const auto instance = suffix.substr(0, separator);
        if (instance.find(':') != std::string_view::npos) {
            return std::nullopt;
        }
        std::uint64_t parsedIndex = 0;
        const auto indexText = suffix.substr(separator + 1);
        const auto [end, error] = std::from_chars(indexText.data(), indexText.data() + indexText.size(), parsedIndex);
        if (error != std::errc{} || end != indexText.data() + indexText.size() ||
            parsedIndex > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
            return std::nullopt;
        }
        return std::pair{ std::string(instance), static_cast<std::size_t>(parsedIndex) };
    }

    static std::string recoveryToken(std::size_t workerIndex, const std::pair<std::string, std::size_t>& owner) {
        return std::string(service::runtime::instanceId()) + ":recovery:" + std::to_string(workerIndex) + ":" +
            owner.first + ":" + std::to_string(owner.second);
    }

    // Streams of expired owners whose recovery lease this Worker now holds.
    template <typename Redis>
    static ruvia::task<std::vector<ProjectorStreamOwner>> discoverDeadStreams(const Redis& redis, std::string_view currentStream, std::size_t workerIndex) {
        std::map<std::string, ProjectorStreamOwner, std::less<>> dead;
        const auto candidates = co_await service::message::redis::keysMatching(
            redis,
            std::string(service::message::edge::projector_stream::kStreamPrefix) + "*"
        );
        for (const auto& candidate : candidates) {
            if (candidate == currentStream) {
                continue;
            }
            const auto owner = ownerFromStream(candidate);
            if (!owner) {
                continue;
            }
            const auto lease = service::message::edge::projector_stream::leaseKey(owner->second, owner->first);
            const auto token = recoveryToken(workerIndex, *owner);
            const auto current = co_await redis.get(lease);
            if (current) {
                // A non-expired owner or another recovery Worker still owns this
                // Stream; neither case is safe to steal. Seeing our own recovery
                // token is not enough either: the lease may expire before the
                // first read, so it must be renewed (token-checked) first.
                if (std::string_view(current->data(), current->size()) != token ||
                    !co_await renewLease(redis, lease, token)) {
                    continue;
                }
            } else if (!co_await claimLease(redis, lease, token)) {
                continue;
            }
            dead.emplace(candidate, ProjectorStreamOwner{ candidate, lease, token });
        }
        std::vector<ProjectorStreamOwner> result;
        result.reserve(dead.size());
        for (auto& [streamName, owner] : dead) {
            result.push_back(std::move(owner));
        }
        co_return result;
    }

    template <typename Redis>
    static ruvia::task<bool> eraseDeadStream(const Redis& redis, const ProjectorStreamOwner& deadStream) {
        const std::string registry(service::message::edge::projector_stream::kStreamRegistry);
        const std::string_view keys[]{ deadStream.lease, deadStream.stream, registry };
        const std::string_view arguments[]{ deadStream.token };
        const auto reply = co_await redis.eval(kEraseDeadStreamScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("erase dead edge projector stream", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<bool> acknowledgeFenced(const Redis& redis, const ProjectorStreamOwner& owner, const std::vector<service::message::StreamMessage>& messages) {
        if (messages.empty()) {
            co_return true;
        }
        const std::string_view keys[]{ owner.lease, owner.stream };
        std::vector<std::string_view> arguments;
        arguments.reserve(messages.size() + 2);
        arguments.push_back(owner.token);
        arguments.push_back(kIngressGroup);
        for (const auto& message : messages) {
            arguments.push_back(message.id);
        }
        const auto reply = co_await redis.eval(kAcknowledgeFencedScript, keys, std::span<const std::string_view>(arguments));
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("fenced edge projector acknowledgement", reply);
        }
        co_return reply.integer() == 1;
    }

  private:
    template <typename Redis>
    static ruvia::task<bool> leaseScript(const Redis& redis, std::string_view script, std::string_view key, std::string_view token, bool withTtl, std::string_view operation) {
        const auto ttl = std::to_string(kLeaseTtl.count());
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token, ttl };
        const auto reply = co_await redis.eval(script, keys, std::span<const std::string_view>(arguments, withTtl ? 2U : 1U));
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue(operation, reply);
        }
        co_return reply.integer() == 1;
    }
};

// Consumes this Worker's projector Stream (and recovers Streams of expired
// owners) under a fenced lease. One instance per Service Worker; its state is
// Worker-local and never shared.
class ProjectionRuntime final {
  public:
    explicit ProjectionRuntime(observability::RuntimeDiagnostics& diagnostics) : diagnostics_(diagnostics) {}

    ProjectionRuntime(const ProjectionRuntime&) = delete;
    ProjectionRuntime& operator=(const ProjectionRuntime&) = delete;

    ~ProjectionRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex, std::size_t serviceWorkerCount) {
        if (running_.exchange(true)) {
            return;
        }
        failed_.store(false);
        worker_ = std::move(worker);
        workerIndex_ = workerIndex;
        if (!worker_.valid() || serviceWorkerCount == 0 || workerIndex_ >= serviceWorkerCount) {
            running_.store(false);
            worker_ = {};
            throw std::runtime_error("edge projector requires Service Workers");
        }
        auto ready = std::make_shared<std::promise<void>>();
        auto stopped = std::make_shared<std::promise<void>>();
        auto readiness = ready->get_future();
        stopped_ = stopped->get_future().share();
        const auto posted = worker_.post([this, ready, stopped](ruvia::web_worker_context& context) {
            leaseScope_ = std::make_unique<ruvia::task_scope>(context.worker());
            return run(context, ready, stopped);
        });
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
            // A rejected post means the Worker is closing; its stop token already
            // reaches the projector task.
            (void)worker_.post([](ruvia::web_worker_context&) -> ruvia::task<void> {
                service::messaging::workerStreamMultiplexer().signal(service::message::WorkerStreamTask::EdgeProjector);
                co_return;
            });
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
    static constexpr auto kRecoverySweepInterval = std::chrono::seconds(1);
    static constexpr auto kRetryDelay = std::chrono::milliseconds(250);

    // Streams read by this Worker: its own plus recovered Streams of expired owners.
    struct ReadSet final {
        ProjectorStreamOwner current;
        std::vector<std::string> streams;
        std::map<std::string, ProjectorStreamOwner, std::less<>> dead;
        bool recovering{ true };
        std::chrono::steady_clock::time_point nextDiscovery{};
        std::chrono::steady_clock::time_point nextRecoverySweep{};

        void add(ProjectorStreamOwner owner) {
            streams.push_back(owner.stream);
            dead.emplace(owner.stream, std::move(owner));
        }

        std::map<std::string, ProjectorStreamOwner, std::less<>>::iterator
        drop(std::map<std::string, ProjectorStreamOwner, std::less<>>::iterator entry) {
            streams.erase(std::remove(streams.begin(), streams.end(), entry->first), streams.end());
            return dead.erase(entry);
        }
    };

    struct DerivedExpiry final {
        std::string nodeId;
        std::shared_ptr<ruvia::stop_source> stop;
    };

    bool active(ruvia::web_worker_context& context) const {
        return running_.load() && !context.get_stop_token().stop_requested() && !leaseLost_.load();
    }

    ruvia::task<void> pause(ruvia::web_worker_context& context) {
        (void)co_await ruvia::sleep_for(context.worker(), kRetryDelay, context.get_stop_token());
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        ReadSet reads{ .current = ProjectorStreams::current(workerIndex_) };
        bool readySet = false;
        try {
            co_await acquire(context, reads);
            ready->set_value();
            readySet = true;
            co_await consume(context, reads);
        } catch (...) {
            if (!readySet) {
                try {
                    ready->set_exception(std::current_exception());
                } catch (...) {
                }
            } else {
                failed_.store(true);
            }
        }
        co_await release(context, reads);
        running_.store(false);
        try {
            stopped->set_value();
        } catch (...) {
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

        co_await ProjectorStreams::cleanupRegistry(redis);
        co_await ProjectorStreams::registerStream(redis, reads.current.stream);
        reads.streams.push_back(reads.current.stream);
        co_await adoptDeadStreams(redis, reads);
        co_await service::message::redis::ensureGroup(redis, reads.current.stream, kIngressGroup);
        co_await ProjectionService::hydrateAuth(context);
        state_.catalog = co_await metadata::hydrate(context);
        reads.nextDiscovery = std::chrono::steady_clock::now();
        reads.nextRecoverySweep = reads.nextDiscovery;
    }

    // A dead owner is recovered only after its lease expired; the recovery lease
    // is held before its consumer group is created or its first entry is read.
    template <typename Redis>
    ruvia::task<void> adoptDeadStreams(const Redis& redis, ReadSet& reads) {
        const auto discovered =
            co_await ProjectorStreams::discoverDeadStreams(redis, reads.current.stream, workerIndex_);
        std::set<std::string, std::less<>> discoveredNames;
        for (const auto& dead : discovered) {
            discoveredNames.insert(dead.stream);
        }
        for (auto dead = reads.dead.begin(); dead != reads.dead.end();) {
            if (discoveredNames.contains(dead->first)) {
                ++dead;
                continue;
            }
            recoveryLeases_.erase(dead->second.lease);
            dead = reads.drop(dead);
            reads.recovering = true;
        }
        for (const auto& dead : discovered) {
            if (reads.dead.contains(dead.stream)) {
                continue;
            }
            if (!co_await service::message::redis::ensureGroupIfPresent(redis, dead.stream, kIngressGroup)) {
                (void)co_await ProjectorStreams::releaseLease(redis, dead.lease, dead.token);
                continue;
            }
            if (!co_await ProjectorStreams::renewLease(redis, dead.lease, dead.token)) {
                continue;
            }
            recoveryLeases_[dead.lease] = dead.token;
            co_await ProjectorStreams::registerStream(redis, dead.stream);
            reads.add(dead);
            reads.recovering = true;
            reads.nextRecoverySweep = std::chrono::steady_clock::now();
        }
    }

    ruvia::task<void> consume(ruvia::web_worker_context& context, ReadSet& reads) {
        const auto redis = context.redis();
        const auto consumer = service::runtime::instanceId() + ":service-" + std::to_string(workerIndex_);
        while (active(context)) {
            dropLostRecoveryStreams(reads);
            bool discoveryFailed = false;
            try {
                if (std::chrono::steady_clock::now() >= reads.nextDiscovery) {
                    co_await adoptDeadStreams(redis, reads);
                    reads.nextDiscovery = std::chrono::steady_clock::now() + kLeaseRenewInterval;
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
            if (!active(context)) {
                break;
            }

            const auto batches = co_await read(context, reads, consumer);
            if (!batches) {
                continue;
            }
            if (batches->empty()) {
                if (!co_await eraseDrainedStreams(context, reads)) {
                    continue;
                }
                co_await service::messaging::workerStreamMultiplexer().wait(
                    workerIndex_,
                    service::message::WorkerStreamTask::EdgeProjector,
                    context.get_stop_token(),
                    kLeaseRenewInterval
                );
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
    ruvia::task<std::optional<std::vector<service::message::redis::StreamBatch>>>
    read(ruvia::web_worker_context& context, ReadSet& reads, const std::string& consumer) {
        const auto redis = context.redis();
        std::vector<service::message::redis::StreamBatch> batches;
        std::optional<std::string> failure;
        try {
            if (!reads.dead.empty() && std::chrono::steady_clock::now() >= reads.nextRecoverySweep) {
                std::vector<std::string> recoveryStreams;
                recoveryStreams.reserve(reads.dead.size());
                for (const auto& [stream, dead] : reads.dead) {
                    recoveryStreams.push_back(stream);
                }
                // A recovery lease fences the crashed owner, so its PEL is safe to
                // sweep immediately; the shared read set keeps its idle threshold.
                batches = co_await service::message::redis::claimGroupMany(
                    redis,
                    recoveryStreams,
                    kIngressGroup,
                    consumer,
                    kBatchSize,
                    std::chrono::milliseconds(0)
                );
                reads.nextRecoverySweep = std::chrono::steady_clock::now() + kRecoverySweepInterval;
            }
            if (batches.empty()) {
                batches = reads.recovering
                    ? co_await service::message::redis::claimGroupMany(redis, reads.streams, kIngressGroup, consumer, kBatchSize)
                    : co_await service::message::redis::readGroupMany(redis, reads.streams, kIngressGroup, consumer, ">", kBatchSize);
            }
        } catch (const std::exception& error) {
            if (active(context)) {
                std::cerr << "edge stream read failed: " << error.what() << '\n';
            }
            failure = std::string(error.what());
        }
        if (failure) {
            if (!active(context)) {
                co_return std::nullopt;
            }
            reads.recovering = true;
            const bool groupMissing = failure->find("NOGROUP") != std::string::npos;
            if (!groupMissing || !co_await recreateGroups(redis, reads)) {
                co_await pause(context);
            }
            co_return std::nullopt;
        }
        if (reads.recovering && batches.empty()) {
            reads.recovering = false;
        }
        co_return batches;
    }

    // Restores the consumer groups after Redis lost them; dead Streams that
    // disappeared meanwhile leave the read set.
    template <typename Redis>
    ruvia::task<bool> recreateGroups(const Redis& redis, ReadSet& reads) {
        try {
            co_await service::message::redis::ensureGroup(redis, reads.current.stream, kIngressGroup);
            for (auto dead = reads.dead.begin(); dead != reads.dead.end();) {
                if (co_await service::message::redis::ensureGroupIfPresent(redis, dead->first, kIngressGroup)) {
                    ++dead;
                    continue;
                }
                recoveryLeases_.erase(dead->second.lease);
                dead = reads.drop(dead);
                reads.nextRecoverySweep = std::chrono::steady_clock::now();
            }
            co_return true;
        } catch (const std::exception& error) {
            std::cerr << "edge stream group recovery failed: " << error.what() << '\n';
            co_return false;
        }
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
    ruvia::task<bool> projectBatch(ruvia::web_worker_context& context, ReadSet& reads, const service::message::redis::StreamBatch& batch) {
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
        co_await publishTelemetry(context, projected);
        co_await ProjectionService::finishTelemetryUploads(redis, projected.completedUploads);
        service::messaging::workerStreamMultiplexer().signalTelemetryConsumers();
        if (leaseLost_.load()) {
            co_return false;
        }
        if (!co_await ProjectorStreams::acknowledgeFenced(redis, *owner, batch.messages)) {
            lostLease(context, reads, *owner);
            co_return false;
        }
        co_return true;
    }

    // Raw-acquisition samples with derived points are published together with
    // their derived values; every other sample is ingested as projected.
    ruvia::task<void> publishTelemetry(ruvia::web_worker_context& context, ProjectionBatch& projected) {
        std::vector<service::message::StreamMessage> messages;
        messages.reserve(projected.telemetry.size());
        for (auto& parsed : projected.telemetry) {
            const auto calculation = projected.rawCalculations.find(parsed.messageId);
            if (calculation != projected.rawCalculations.end()) {
                const auto prepared = service::telemetry::derived::DerivedService::prepare(parsed.deviceId, calculation->second);
                if (!prepared) {
                    scheduleDerivedExpiry(context, parsed, calculation->second, std::nullopt);
                } else {
                    const auto result = co_await service::telemetry::derived::DerivedService::append(
                        context.redis(),
                        *prepared,
                        parsed,
                        false,
                        true
                    );
                    scheduleDerivedExpiry(context, parsed, calculation->second, result.nextDeadline);
                    if (result.published) {
                        continue;
                    }
                }
            }
            service::message::StreamMessage message;
            message.fields = service::message::parsedFields(parsed);
            messages.push_back(std::move(message));
        }
        co_await service::telemetry::TelemetryService::ingest(context, messages);
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
            diagnostics_.setComponentStatus("edge-projector", observability::ComponentState::Failed, "worker lease lost");
        } catch (const std::exception& error) {
            std::cerr << "edge projector status update failed: " << error.what() << '\n';
        } catch (...) {
            std::cerr << "edge projector status update failed: unknown error\n";
        }
        try {
            // Resolved from the current Worker context: no session or callback is
            // routed through another Worker.
            context.worker_state<SessionDispatcher>().failSessions("worker lease lost");
        } catch (const std::exception& error) {
            std::cerr << "edge worker-local session shutdown failed: " << error.what() << '\n';
        } catch (...) {
            std::cerr << "edge worker-local session shutdown failed: unknown error\n";
        }
        try {
            service::messaging::workerStreamMultiplexer().signal(service::message::WorkerStreamTask::EdgeProjector);
        } catch (const std::exception& error) {
            std::cerr << "edge projector wake failed: " << error.what() << '\n';
        } catch (...) {
            std::cerr << "edge projector wake failed: unknown error\n";
        }
    }

    std::vector<std::pair<std::string, std::string>> recoveryLeaseSnapshot() const {
        return { recoveryLeases_.begin(), recoveryLeases_.end() };
    }

    ruvia::task<void> maintainLeases(ruvia::web_worker_context& context, ProjectorStreamOwner current, ruvia::stop_token scopeStop) {
        const auto redis = context.redis();
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), scopeStop);
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
                if (!lost.empty()) {
                    service::messaging::workerStreamMultiplexer().signal(service::message::WorkerStreamTask::EdgeProjector);
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
            ruvia::combine_stop_tokens(context.get_stop_token(), leaseScope_->get_stop_token()),
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
        parsed.messageId = derivedUuid_.next();
        parsed.acquisitionId = parsed.causationId = parsed.messageId;
        parsed.source = "derived";
        parsed.rawPayloads.clear();
        parsed.rawPacketIds.clear();
        parsed.observedAtMs = parsed.occurredAtMs = service::message::utcNowMilliseconds();
        parsed.valuesJson = "{\"values\":{}}";
        try {
            const auto prepared = service::telemetry::derived::DerivedService::prepare(parsed.deviceId, config);
            std::optional<std::int64_t> next;
            if (prepared) {
                next = (co_await service::telemetry::derived::DerivedService::append(context.redis(), *prepared, parsed, true)).nextDeadline;
            }
            scheduleDerivedExpiry(context, std::move(parsed), std::move(config), next);
        } catch (const std::exception& error) {
            std::cerr << "edge derived expiry failed: " << error.what() << '\n';
            scheduleDerivedExpiry(context, std::move(parsed), std::move(config), service::message::utcNowMilliseconds() + 1000);
        }
    }

    observability::RuntimeDiagnostics& diagnostics_;
    ProjectionState state_;
    std::map<std::string, DerivedExpiry> derivedExpiry_;
    service::common::UuidV7Generator derivedUuid_;
    ruvia::web_worker_handle worker_;
    std::shared_future<void> stopped_;
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
