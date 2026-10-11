#pragma once

#include "service/common/uuid.h"

#include <asio/steady_timer.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/core/one_shot.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/web/web_worker.h>
#include <ruvia/web/redis/redis.h>

#include "service/common/message.h"
#include "service/features/gb28181/device/device.runtime.h"
#include "service/features/gb28181/gb28181.config.h"
#include "service/features/gb28181/gb28181.service.h"
#include "service/features/gb28181/media/media.runtime.h"
#include "service/features/gb28181/media/media.transport.h"
#include "service/features/gb28181/sip/sip.transport.h"

namespace service::gb28181 {

// A GB28181 protocol actor is owned by exactly one Collector Worker.  The
// actor borrows that worker's already-driven event_loop and redis_handle; it does
// not create a private thread pool or route SIP packets through another
// worker.  `index` is an ownership label for diagnostics and SDK callback
// routing only, never a device/stream partition key.
class CollectorRuntime final {
  public:
    using OwnerIndex = std::size_t;

    CollectorRuntime(service::common::UuidV7Generator& uuidGenerator, AppConfig config, ruvia::event_loop loop, ruvia::redis_handle redis, OwnerIndex index, OwnerIndex count);
    CollectorRuntime(const CollectorRuntime&) = delete;
    CollectorRuntime& operator=(const CollectorRuntime&) = delete;
    ~CollectorRuntime();

    [[nodiscard]] ruvia::task<void> initialize();
    [[nodiscard]] ruvia::task<void> shutdown();

    [[nodiscard]] bool enabled() const noexcept { return config_.enabled; }

    [[nodiscard]] bool started() const noexcept { return started_; }

    [[nodiscard]] OwnerIndex index() const noexcept { return index_; }

    [[nodiscard]] OwnerIndex count() const noexcept { return count_; }

    [[nodiscard]] ruvia::event_loop loop() const noexcept { return loop_; }

    [[nodiscard]] ruvia::worker_handle worker() const noexcept {
        return worker_;
    }

    [[nodiscard]] DeviceRegistry& devices() noexcept { return devices_; }

    [[nodiscard]] StreamRegistry& streams() noexcept { return streams_; }

    [[nodiscard]] SipServer* sip() noexcept { return sip_.get(); }

    // Called by the Collector command consumer on this same event_loop.  The
    // operation is deliberately local and synchronous with respect to the SIP
    // actor; the returned JSON is sent back through the Redis command reply
    // stream by the caller.
    [[nodiscard]] ruvia::task<std::string>
    execute(std::string operation, std::string payload, ruvia::stop_token stop = {});

  private:
    struct DeviceProjection final {
        Device device;
        DeviceChange change{ DeviceChange::Status };
        std::string ownerToken;
        std::string projectionId;
        std::uint64_t sequence{ 0 };
    };

    struct StreamProjection final {
        StreamStatus stream;
        std::string ownerToken;
        std::string projectionId;
        std::uint64_t sequence{ 0 };
    };

    using ProjectionCompletion = std::function<void(bool)>;

    struct ProjectionBarrier {
        ProjectionCompletion complete;
        std::uint64_t after{ 0 };
        std::uint64_t through{ 0 };
    };

    using ProjectionEvent = std::variant<DeviceProjection, StreamProjection, ProjectionBarrier>;

    service::common::UuidV7Generator& uuidGenerator_;
    AppConfig config_;
    ruvia::event_loop loop_;
    ruvia::worker_handle worker_;
    ruvia::redis_handle redis_;
    OwnerIndex index_{ 0 };
    OwnerIndex count_{ 0 };
    ruvia::task_scope scope_;
    ruvia::task_scope projectionScope_;
    DeviceRegistry devices_;
    StreamRegistry streams_;
    std::shared_ptr<SipServer> sip_;
    std::atomic_bool started_{ false };
    std::atomic_bool stopping_{ false };
    bool sdkRegistered_{ false };
    bool scopeJoined_{ false };
    std::string ownerToken_;
    std::unordered_map<std::string, std::uint64_t> streamGenerations_;
    std::unordered_map<std::string, bool> streamOnline_;
    std::unordered_map<std::string, std::string> previewOwners_;
    std::deque<ProjectionEvent> projectionQueue_;
    bool projectionDrainRunning_{ false };
    bool acceptingProjection_{ true };
    bool currentProjectionSucceeded_{ true };
    std::uint64_t projectionSequence_{ 0 };
    std::uint64_t lastSipBarrierSequence_{ 0 };
    std::deque<std::uint64_t> projectionFailures_;
    std::optional<std::uint64_t> activeControlProjection_;

    [[nodiscard]] ruvia::task<void> publishDevice(const Device& device, DeviceChange change, std::string_view ownerToken, std::string_view projectionId);
    [[nodiscard]] ruvia::task<void>
    publishStream(const StreamStatus& stream, std::string_view ownerToken, std::string_view projectionId);
    [[nodiscard]] ruvia::task<bool> persistProjection(
        std::vector<service::message::StreamField> fields,
        std::string projectionId
    );
    void enqueueBarrier(ProjectionCompletion completion, std::uint64_t after);
    [[nodiscard]] ruvia::task<bool> waitForProjection(ruvia::stop_token stop, std::uint64_t after);
    [[nodiscard]] ruvia::task<void> retainPreview(const SipPreviewStartResult& preview);
    [[nodiscard]] ruvia::task<void> releasePreview(std::string sessionId);
    [[nodiscard]] ruvia::task<void> drainProjection();
    [[nodiscard]] ruvia::task<void> controlLoop();
    [[nodiscard]] ruvia::task<void>
    handleControl(const service::message::StreamMessage& message);
    [[nodiscard]] ruvia::task<void> refreshOwnerLeases();
    [[nodiscard]] ruvia::task<void> clearOwnerLeases();
    void enqueueDeviceProjection(const Device& device, DeviceChange change);
    void enqueueStreamProjection(const StreamStatus& stream);
    [[nodiscard]] std::string deviceOwnerToken(const Device& device) const;
    [[nodiscard]] std::string streamOwnerToken(const StreamStatus& stream);
    void registerSdkRoute();
    void unregisterSdkRoute() noexcept;
    void requireCurrentLoop() const;

    struct OwnerLease final {
        std::string token;
        std::string deviceId;
        std::string streamId;
        std::chrono::steady_clock::time_point expiresAt{};
        std::unique_ptr<asio::steady_timer> timer;
        bool renewing{ false };
    };

    std::unordered_map<std::string, OwnerLease> ownerLeases_;
    std::unordered_map<std::string, std::string> retiredOwnerTokens_;

    [[nodiscard]] ruvia::task<bool>
    retainOwner(std::string key, std::string token, std::string deviceId = {}, std::string streamId = {});
    [[nodiscard]] bool ownsLocal(std::string_view key, std::string_view token) const;
    [[nodiscard]] ruvia::task<void> releaseOwner(std::string key, std::string token);
    [[nodiscard]] ruvia::task<void> renewOwnerLease(std::string key, std::string token);
    void armOwnerLeaseTimer(std::string key);
    void expireOwnerLease(std::string key, std::string token);
    void invalidateOwnerTarget(const std::string& key, const std::string& token, const std::string& deviceId, const std::string& streamId);
};

class GbProjectionRuntime final : public GbProjectionService {
  public:
    using OwnerIndex = std::size_t;
    static constexpr OwnerIndex kUnassignedOwner =
        std::numeric_limits<OwnerIndex>::max();

    GbProjectionRuntime() = default;
    GbProjectionRuntime(const GbProjectionRuntime&) = delete;
    GbProjectionRuntime& operator=(const GbProjectionRuntime&) = delete;

    ~GbProjectionRuntime() { stop(); }

    [[nodiscard]] OwnerIndex workerIndex() const noexcept {
        return workerIndex_;
    }

    [[nodiscard]] ruvia::web_worker_handle worker() const noexcept {
        std::lock_guard lock(lifecycleMutex_);
        return worker_;
    }

    // Worker ids are the only stable link between a request context and the
    // projector that was prepared for that worker.  The index is a startup
    // label; it is never used to partition a device or stream.
    [[nodiscard]] ruvia::worker_id_type workerId() const noexcept {
        std::lock_guard lock(lifecycleMutex_);
        return worker_.id();
    }

    void start(ruvia::web_worker_handle worker, OwnerIndex workerIndex, OwnerIndex serviceWorkerCount);
    void stop() noexcept;

  private:
    struct State final {
        ruvia::stop_source stop;
    };

    [[nodiscard]] ruvia::task<void>
    consume(ruvia::web_worker_context& context, const std::shared_ptr<State>& state);

    std::atomic_bool running_{ false };
    mutable std::mutex lifecycleMutex_;
    ruvia::web_worker_handle worker_;
    OwnerIndex workerIndex_{ 0 };
    OwnerIndex serviceWorkerCount_{ 0 };
    std::shared_ptr<State> state_;
    std::shared_future<void> stopped_;
};

} // namespace service::gb28181
