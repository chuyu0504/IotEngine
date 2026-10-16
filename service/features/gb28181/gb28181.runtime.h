#pragma once

#include <asio/steady_timer.hpp>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/core/one_shot.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/web/redis/redis.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/gb28181/device/device.runtime.h"
#include "service/features/gb28181/gb28181.config.h"
#include "service/features/gb28181/gb28181.service.h"
#include "service/features/gb28181/gb28181.types.h"
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

    CollectorRuntime(service::common::UuidV7Generator& uuidGenerator, Config config, ruvia::event_loop loop, ruvia::redis_handle redis, OwnerIndex index, OwnerIndex count);
    CollectorRuntime(const CollectorRuntime&) = delete;
    CollectorRuntime& operator=(const CollectorRuntime&) = delete;
    ~CollectorRuntime();

    [[nodiscard]] ruvia::task<void> initialize();
    [[nodiscard]] ruvia::task<void> shutdown();

  private:
    // SIP acknowledgements and control commands each wait on one barrier.
    // Beyond this many pending barriers the actor refuses new ones, so the SIP
    // peer or the Service caller retries instead of growing the queue.
    static constexpr std::size_t kProjectionBarrierCapacity = 1024;
    // A directed receipt normally completes the wait.  This bound only
    // re-checks the durable receipt and repairs a lost stream entry.
    static constexpr std::chrono::seconds kProjectionReceiptRecheck{ 5 };
    // Idle Collector streams refresh their incarnation lifetime at this interval.
    static constexpr std::chrono::seconds kCollectorStreamRefresh{ 60 };

    struct OwnerLease final {
        std::string token;
        std::string deviceId;
        std::string streamId;
        std::string deadlineMember;
        std::chrono::steady_clock::time_point expiresAt{};
        std::unique_ptr<asio::steady_timer> timer;
        bool renewing{ false };
    };

    service::common::UuidV7Generator& uuidGenerator_;
    Config config_;
    ruvia::event_loop loop_;
    ruvia::worker_handle worker_;
    ruvia::redis_handle redis_;
    OwnerIndex index_{ 0 };
    ruvia::task_scope scope_;
    ruvia::task_scope projectionScope_;
    device::DeviceRegistry devices_;
    media::StreamRegistry streams_;
    std::shared_ptr<media::ZlmSdk> sdk_;
    std::shared_ptr<sip::SipServer> sip_;
    std::atomic_bool started_{ false };
    std::atomic_bool stopping_{ false };
    bool scopeJoined_{ false };
    std::string ownerToken_;
    std::unordered_map<std::string, std::uint64_t> streamGenerations_;
    std::unordered_map<std::string, bool> streamOnline_;
    std::unordered_map<std::string, std::string> previewOwners_;
    ProjectionQueue projectionQueue_{ kProjectionBarrierCapacity };
    std::unordered_map<std::string, ruvia::one_shot_completion<bool>> projectionReceipts_;
    bool projectionDrainRunning_{ false };
    bool acceptingProjection_{ true };
    bool projectionBackpressure_{ false };
    std::uint64_t lastSipBarrierSequence_{ 0 };
    std::optional<std::uint64_t> activeControlProjection_;
    std::unordered_map<std::string, OwnerLease> ownerLeases_;
    std::unordered_map<std::string, std::string> retiredOwnerTokens_;

    [[nodiscard]] ruvia::task<std::string>
    execute(std::string operation, std::string payload, ruvia::stop_token stop);
    [[nodiscard]] ruvia::task<bool> publishDevice(const DeviceProjection& projection);
    [[nodiscard]] ruvia::task<bool> publishStream(const StreamProjection& projection);
    [[nodiscard]] ruvia::task<bool> persistProjection(
        std::vector<service::message::StreamField> fields,
        std::string projectionId
    );
    void enqueueBarrier(ProjectionCompletion completion, std::uint64_t after);
    [[nodiscard]] ruvia::task<bool> waitForProjection(ruvia::stop_token stop, std::uint64_t after);
    [[nodiscard]] ruvia::task<void> retainPreview(const sip::SipPreviewStartResult& preview);
    [[nodiscard]] ruvia::task<void> releasePreview(std::string sessionId);
    void startProjectionDrain();
    [[nodiscard]] ruvia::task<void> drainProjection();
    [[nodiscard]] ruvia::task<void> controlLoop();
    [[nodiscard]] ruvia::task<void> receiptLoop();
    [[nodiscard]] ruvia::task<void>
    handleControl(const service::message::StreamMessage& message);
    [[nodiscard]] ruvia::task<void> refreshOwnerLeases();
    [[nodiscard]] ruvia::task<void> clearOwnerLeases();
    void enqueueDeviceProjection(const device::Device& device, device::DeviceChange change);
    void enqueueStreamProjection(const media::StreamStatus& stream);
    [[nodiscard]] std::string deviceOwnerToken(const device::Device& device) const;
    [[nodiscard]] std::string streamOwnerToken(const media::StreamStatus& stream);
    void registerSdkRoute();
    void unregisterSdkRoute() noexcept;
    void requireCurrentLoop() const;

    [[nodiscard]] ruvia::task<bool>
    retainOwner(std::string key, std::string token, std::string deviceId = {}, std::string streamId = {});
    [[nodiscard]] bool ownsLocal(std::string_view key, std::string_view token) const;
    [[nodiscard]] ruvia::task<void> releaseOwner(std::string key, std::string token, std::string settledMember = {});
    [[nodiscard]] ruvia::task<void> renewOwnerLease(std::string key, std::string token);
    void armOwnerLeaseTimer(std::string key);
    void expireOwnerLease(std::string key, std::string token);
    void invalidateOwnerTarget(const std::string& key, const std::string& token, const std::string& deviceId, const std::string& streamId);
};

// Every Service Worker runs one projection consumer when GB28181 is enabled.
// The shared consumer group gives each projection to one worker; owner expiry
// is claimed from the deadline set by whichever worker wakes first.
class GbProjectionRuntime final : public GbProjectionService {
  public:
    using OwnerIndex = std::size_t;

    GbProjectionRuntime() = default;
    GbProjectionRuntime(const GbProjectionRuntime&) = delete;
    GbProjectionRuntime& operator=(const GbProjectionRuntime&) = delete;

    ~GbProjectionRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker, OwnerIndex workerIndex);
    void stop() noexcept;

  private:
    static constexpr std::size_t kBatchSize = 64;
    // Entries left pending by a stopped consumer are claimed after this idle
    // time; the sweep runs at the same interval while the worker is idle.
    static constexpr std::chrono::seconds kPendingRecovery{ 60 };

    struct State final {
        ruvia::stop_source stop;
    };

    [[nodiscard]] ruvia::task<void>
    consume(ruvia::web_worker_context& context, const std::shared_ptr<State>& state);
    [[nodiscard]] ruvia::task<void> project(ruvia::web_worker_context& context, const ruvia::redis_handle& redis, std::string_view stream, const service::message::StreamMessage& message);

    std::atomic_bool running_{ false };
    mutable std::mutex lifecycleMutex_;
    ruvia::web_worker_handle worker_;
    OwnerIndex workerIndex_{ 0 };
    std::shared_ptr<State> state_;
    std::shared_future<void> stopped_;
};

} // namespace service::gb28181
