#include "service/features/gb28181/gb28181.runtime.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <exception>
#include <memory_resource>
#include <set>
#include <stdexcept>

#include "service/common/http.h"
#include "service/middleware/log.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/features/gb28181/gb28181.protocol.h"
#include "service/utils/redis.h"

namespace service::gb28181 {



void GbProjectionRuntime::start(ruvia::web_worker_handle worker, OwnerIndex workerIndex, OwnerIndex serviceWorkerCount) {
    {
        std::lock_guard lock(lifecycleMutex_);
        if (running_.load()) {
            if (worker_.id() != worker.id() || workerIndex_ != workerIndex) {
                throw std::logic_error(
                    "GB28181 projection runtime is already bound to another worker"
                );
            }
            return;
        }
        if (!worker.valid() || serviceWorkerCount == 0 ||
            workerIndex >= serviceWorkerCount) {
            throw std::runtime_error(
                "GB28181 projection runtime requires a valid Service Worker"
            );
        }
        worker_ = std::move(worker);
        workerIndex_ = workerIndex;
        serviceWorkerCount_ = serviceWorkerCount;
        state_ = std::make_shared<State>();
        running_.store(true);
    }

    auto ready = std::make_shared<std::promise<void>>();
    auto future = ready->get_future();
    auto stopped = std::make_shared<std::promise<void>>();
    stopped_ = stopped->get_future().share();
    const auto state = state_;
    const auto posted = worker_.post(
        [this, ready, stopped, state](ruvia::web_worker_context& context)
            -> ruvia::task<void> {
            bool readySet = false;
            try {
                const auto redis = context.redis().with_options(
                    { .stop_token_ = state->stop.token() }
                );
                co_await service::message::redis::ensureGroup(
                    redis,
                    control_protocol::stream::kProjection,
                    control_protocol::stream::kProjectionGroup
                );
                ready->set_value();
                readySet = true;
                co_await consume(context, state);
            } catch (...) {
                if (!readySet) {
                    try {
                        ready->set_exception(std::current_exception());
                    } catch (...) {
                    }
                } else {
                    LOG_WARN << "[GB28181][GbProjectionRuntime] consumer stopped";
                }
            }
            try {
                stopped->set_value();
            } catch (...) {
            }
            co_return;
        }
    );
    if (!posted.accepted()) {
        state->stop.request_stop();
        running_.store(false);
        std::lock_guard lock(lifecycleMutex_);
        worker_ = {};
        state_.reset();
        stopped_ = {};
        throw std::runtime_error(
            "service worker rejected GB28181 projection consumer"
        );
    }

    try {
        future.get();
    } catch (...) {
        state->stop.request_stop();
        if (stopped_.valid()) {
            stopped_.wait();
        }
        running_.store(false);
        std::lock_guard lock(lifecycleMutex_);
        worker_ = {};
        state_.reset();
        stopped_ = {};
        throw;
    }
}

void GbProjectionRuntime::stop() noexcept {
    std::shared_ptr<State> state;
    std::shared_future<void> stopped;
    {
        std::lock_guard lock(lifecycleMutex_);
        if (!running_.exchange(false)) {
            return;
        }
        state = state_;
        stopped = stopped_;
    }
    if (state) {
        state->stop.request_stop();
    }
    if (stopped.valid()) {
        stopped.wait();
    }
    std::lock_guard lock(lifecycleMutex_);
    worker_ = {};
    state_.reset();
    stopped_ = {};
}

ruvia::task<void>
GbProjectionRuntime::consume(ruvia::web_worker_context& context, const std::shared_ptr<State>& state) {
    const auto stop =
        ruvia::combine_stop_tokens(context.get_stop_token(), state->stop.token());
    const auto redis = context.redis().with_options({ .stop_token_ = stop });
    const std::string stream(control_protocol::stream::kProjection);
    const auto group = control_protocol::stream::kProjectionGroup;
    const auto consumer = service::runtime::instanceId() + ":service:" +
        std::to_string(workerIndex_);
    const std::vector<std::string> streams{ stream };
    auto nextReconcile = std::chrono::steady_clock::now();

    while (!stop.stop_requested()) {
        bool retry = false;
        std::string failure;
        try {
            co_await service::message::redis::ensureGroup(redis, stream, group);
            if (std::chrono::steady_clock::now() >= nextReconcile) {
                co_await reconcileExpiredOwners(context);
                nextReconcile = std::chrono::steady_clock::now() +
                    std::chrono::seconds(5);
            }
            auto batches = co_await service::message::redis::claimGroupMany(
                redis,
                streams,
                group,
                consumer,
                64
            );
            if (batches.empty()) {
                batches = co_await service::message::redis::readGroupMany(redis, streams, group, consumer, ">", 64);
            }
            if (batches.empty()) {
                (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(100), stop);
                continue;
            }
            for (const auto& batch : batches) {
                for (const auto& message : batch.messages) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    const auto projectionId = message.get("projection_id");
                    const auto ownerToken = message.get("owner_token");
                    if (!service::common::isUuid(projectionId) || !control_protocol::stream::completeOwnerToken(ownerToken)) {
                        LOG_WARN << "[GB28181] invalid projection identity: " << message.id;
                        co_await service::message::redis::acknowledgeAndDelete(redis, batch.stream, group, message.id);
                        continue;
                    }
                    const auto projectionOrder = message.get("projection_order");
                    std::string cursorId(message.id);
                    if (!projectionOrder.empty()) {
                        if (!control_protocol::validProjectionStreamId(projectionOrder)) {
                            LOG_WARN << "[GB28181] invalid projection order: "
                                     << message.id;
                            co_await GbControlService::markProjectionDoneAndAcknowledge(
                                redis,
                                batch.stream,
                                group,
                                message.id,
                                projectionId,
                                false
                            );
                            continue;
                        }
                        cursorId = std::string(projectionOrder);
                    } else if (!control_protocol::validProjectionStreamId(cursorId)) {
                        LOG_WARN << "[GB28181] invalid projection stream ID: "
                                 << message.id;
                        co_await service::message::redis::acknowledgeAndDelete(
                            redis,
                            batch.stream,
                            group,
                            message.id
                        );
                        continue;
                    }
                    const auto eventType = message.get("event_type");
                    std::optional<Device> device;
                    std::optional<StreamStatus> streamStatus;
                    std::optional<DeviceChange> deviceChange;
                    std::string parseError;
                    try {
                        if (eventType == "gb28181.device") {
                            deviceChange =
                                projection_protocol::projectionDeviceChange(message.get("change"));
                            if (!deviceChange) {
                                throw std::runtime_error(
                                    "GB28181 device projection has no valid change"
                                );
                            }
                            device = projection_protocol::deviceFromProjection(message);
                        } else if (eventType == "gb28181.stream") {
                            streamStatus = projection_protocol::streamFromProjection(message);
                        } else {
                            throw std::runtime_error(
                                "unsupported GB28181 projection event type"
                            );
                        }
                    } catch (const std::exception& error) {
                        parseError = error.what();
                    }
                    if (!parseError.empty()) {
                        LOG_WARN << "[GB28181][GbProjectionRuntime] dropping malformed projection: "
                                 << parseError;
                        co_await GbControlService::markProjectionDoneAndAcknowledge(
                            redis,
                            batch.stream,
                            group,
                            message.id,
                            projectionId,
                            false
                        );
                        continue;
                    }
                    bool applied = false;
                    if (device) {
                        if (co_await GbProjectionService::projectionOwnerMatches(
                                redis,
                                device->id,
                                device->online,
                                ownerToken
                            )) {
                            applied = co_await applyDeviceProjection(
                                context,
                                *device,
                                *deviceChange,
                                control_protocol::stream::owner(device->id),
                                std::string(ownerToken),
                                cursorId
                            );
                        }
                    } else if (streamStatus) {
                        const auto identity = StreamStatus::identity(
                            streamStatus->app,
                            streamStatus->stream,
                            streamStatus->schema
                        );
                        if (co_await GbProjectionService::projectionOwnerMatches(
                                redis,
                                identity,
                                streamStatus->online,
                                ownerToken
                            )) {
                            applied = co_await applyStreamProjection(
                                context,
                                *streamStatus,
                                control_protocol::stream::owner(identity),
                                std::string(ownerToken),
                                cursorId
                            );
                        }
                    }
                    co_await GbControlService::markProjectionDoneAndAcknowledge(
                        redis,
                        batch.stream,
                        group,
                        message.id,
                        projectionId,
                        applied
                    );
                }
            }
        } catch (const std::exception& error) {
            if (stop.stop_requested()) {
                break;
            }
            retry = true;
            failure = error.what();
        } catch (...) {
            if (stop.stop_requested()) {
                break;
            }
            retry = true;
        }
        if (retry && !stop.stop_requested()) {
            if (!failure.empty()) {
                LOG_WARN << "[GB28181][GbProjectionRuntime] projection failed: "
                         << failure;
            }
            (void)co_await ruvia::sleep_for(
                context.worker(),
                std::chrono::milliseconds(250),
                stop
            );
        }
    }
    co_return;
}

CollectorRuntime::CollectorRuntime(service::common::UuidV7Generator& uuidGenerator, AppConfig config, ruvia::event_loop loop, ruvia::redis_handle redis, OwnerIndex index, OwnerIndex count)
    : uuidGenerator_(uuidGenerator), config_(std::move(config)),
      loop_(std::move(loop)),
      worker_(loop_.handle()),
      redis_(std::move(redis)),
      index_(index),
      count_(count),
      scope_(worker_),
      projectionScope_(worker_),
      ownerToken_(service::runtime::instanceId() + ":collector:" + std::to_string(index_)),
      devices_([this](const Device& device, DeviceChange change) {
          enqueueDeviceProjection(device, change);
      }),
      streams_([this](const StreamStatus& stream) {
          enqueueStreamProjection(stream);
      }) {
    if (count_ == 0 || index_ >= count_) {
        throw std::invalid_argument(
            "GB28181 Collector owner index is outside worker count"
        );
    }
}

std::string CollectorRuntime::deviceOwnerToken(const Device& device) const {
    return ownerToken_ + ":session:" +
        std::to_string(device.sessionGeneration);
}

std::string CollectorRuntime::streamOwnerToken(const StreamStatus& stream) {
    const auto identity =
        StreamStatus::identity(stream.app, stream.stream, stream.schema);
    auto& generation = streamGenerations_[identity];
    const auto previous = streamOnline_.find(identity);
    if (stream.online && (previous == streamOnline_.end() || !previous->second)) {
        ++generation;
    }
    streamOnline_[identity] = stream.online;
    return ownerToken_ + ":stream:" + std::to_string(generation);
}

CollectorRuntime::~CollectorRuntime() {
    // A task_scope owns coroutine frames that capture this object.  Releasing
    // the object while they are still active would turn a failed shutdown
    // into a use-after-free, so make lifecycle misuse fail closed.
    if (started_.load() || sdkRegistered_ || scope_.size() != 0 || projectionScope_.size() != 0) {
        std::terminate();
    }
}

void CollectorRuntime::requireCurrentLoop() const {
    if (!loop_.valid() || !worker_.valid() || !loop_.is_current()) {
        throw std::logic_error(
            "GB28181 CollectorRuntime operation must run on its owning loop"
        );
    }
    if (count_ == 0 || index_ >= count_) {
        throw std::logic_error("GB28181 CollectorRuntime owner is invalid");
    }
}

void CollectorRuntime::registerSdkRoute() {
    requireCurrentLoop();
    if (sdkRegistered_) {
        return;
    }

    SdkSupervisor::CollectorCallbacks callbacks;
    callbacks.onStreamChanged =
        [this](std::string app, std::string stream, std::string schema, bool online, int readerCount) {
            if (stopping_.load()) {
                return;
            }
            streams_.updateStreamChanged(app, stream, schema, online, readerCount);
            // Shared previews are reused only while their media stream is online.
            if (sip_) {
                sip_->markStreamOnline(stream, online);
            }
        };
    callbacks.onRtpDetached = [this](std::string stream) {
        if (stopping_.load() || !sip_) {
            return;
        }
        // This callback is posted by SdkSupervisor to this Collector's loop.
        // The SIP session and its RTP server therefore remain in the same
        // owner for their entire lifetime.
        (void)sip_->stopPreviewByStream(stream);
    };
    sdkSupervisor().registerCollector(index_, loop_, std::move(callbacks));
    sdkRegistered_ = true;
}

void CollectorRuntime::unregisterSdkRoute() noexcept {
    if (!sdkRegistered_) {
        return;
    }
    sdkSupervisor().unregisterCollector(index_);
    sdkRegistered_ = false;
}

ruvia::task<void> CollectorRuntime::initialize() {
    requireCurrentLoop();
    if (started_.load()) {
        co_return;
    }
    if (stopping_.load()) {
        throw std::logic_error("GB28181 CollectorRuntime cannot restart");
    }
    if (!config_.enabled) {
        co_await GbProjectionService::publishConfig(redis_, config_);
        co_return;
    }

    if (config_.sip.domain.empty() || config_.sip.id.empty() ||
        config_.sip.publicIp.empty() || config_.sip.password.empty() ||
        config_.media.rtpPublicIp.empty() ||
        config_.media.playTokenSecret.size() < 16) {
        throw std::runtime_error(
            "GB28181 configuration requires domain, id, public IP, password, "
            "RTP IP and a media token secret of at least 16 characters"
        );
    }
    const auto randomRtpPort = config_.media.rtpPortRangeStart == 0 &&
        config_.media.rtpPortRangeEnd == 0;
    if (!randomRtpPort &&
        (config_.media.rtpPortRangeStart == 0 ||
         config_.media.rtpPortRangeStart > config_.media.rtpPortRangeEnd)) {
        throw std::runtime_error("GB28181 RTP port range is invalid");
    }
    if (config_.media.workerThreads <= 0) {
        throw std::runtime_error("ZLM worker thread count must be positive");
    }
    if (config_.media.logLevel < 0 || config_.media.logLevel > 4) {
        throw std::runtime_error("ZLM log level must be between 0 and 4");
    }
    if (!sdkSupervisor().started()) {
        throw std::runtime_error(
            "ZLMediaKit SDK supervisor must start before GB28181 Collector"
        );
    }

    std::exception_ptr startupFailure;
    try {
        registerSdkRoute();
        sip_ = std::make_shared<SipServer>(
            uuidGenerator_,
            config_.sip,
            config_.media,
            devices_,
            sdkSupervisor().sdk(),
            loop_,
            [this](const std::string& stream, unsigned int viewerCount) {
                if (!stopping_.load()) {
                    streams_.updateViewerCount(stream, static_cast<int>(viewerCount));
                }
            },
            index_
        );
        sip_->setAcknowledgementHandler([this](ProjectionCompletion completion) {
            enqueueBarrier(std::move(completion), lastSipBarrierSequence_);
            lastSipBarrierSequence_ = projectionSequence_;
        });
        sip_->setStreamClosedHandler([this](const std::string& streamId) {
            for (const auto& stream : streams_.listStreams()) {
                if (stream.stream == streamId && stream.online) {
                    streams_.updateStreamChanged(stream.app, stream.stream, stream.schema, false, 0);
                }
            }
        });
        sip_->setPreviewClosedHandler([this](const std::string& sessionId) {
            if (!projectionScope_.stop_requested()) {
                projectionScope_.spawn(releasePreview(sessionId));
            }
        });
        sip_->start();
        started_.store(true);
        co_await GbProjectionService::publishConfig(redis_, config_);
        scope_.spawn(controlLoop());
        scope_.spawn(refreshOwnerLeases());
    } catch (...) {
        startupFailure = std::current_exception();
    }
    if (startupFailure) {
        co_await shutdown();
        std::rethrow_exception(startupFailure);
    }
    co_return;
}

ruvia::task<void> CollectorRuntime::shutdown() {
    requireCurrentLoop();
    if (scopeJoined_) {
        co_return;
    }
    stopping_.store(true);
    started_.store(false);
    scope_.request_stop();
    if (sip_) {
        sip_->stop();
    }
    unregisterSdkRoute();
    for (const auto& stream : streams_.listStreams()) {
        if (stream.online) {
            streams_.updateStreamChanged(stream.app, stream.stream, stream.schema, false, 0);
        }
    }
    // stop() emits the final local offline snapshots before closing admission.
    acceptingProjection_ = false;
    try {
        co_await scope_.join();
    } catch (const std::exception& error) {
        LOG_WARN << "[GB28181] control shutdown: " << error.what();
    }
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (projectionDrainRunning_ && std::chrono::steady_clock::now() < until) {
        (void)co_await ruvia::sleep_for(worker_, std::chrono::milliseconds(20));
    }
    projectionScope_.request_stop();
    try {
        co_await projectionScope_.join();
    } catch (...) {
    }
    // No producer or renewal coroutine can recreate an owner after this point.
    co_await clearOwnerLeases();
    for (auto& event : projectionQueue_) {
        if (auto* completion = std::get_if<ProjectionBarrier>(&event)) {
            completion->complete(false);
        }
    }
    if (!projectionQueue_.empty()) {
        LOG_WARN << "[GB28181] stopped with unconfirmed projections; no positive SIP acknowledgement was issued";
    }
    projectionQueue_.clear();
    scopeJoined_ = true;
    sip_.reset();
    co_return;
}

void CollectorRuntime::enqueueDeviceProjection(const Device& device, DeviceChange change) {
    if (!acceptingProjection_) {
        return;
    }
    projectionQueue_.push_back(DeviceProjection{ device, change, deviceOwnerToken(device), uuidGenerator_.next(), ++projectionSequence_ });
    if (!projectionDrainRunning_) {
        projectionDrainRunning_ = true;
        projectionScope_.spawn(drainProjection());
    }
}

void CollectorRuntime::enqueueStreamProjection(const StreamStatus& stream) {
    if (!acceptingProjection_) {
        return;
    }
    projectionQueue_.push_back(StreamProjection{ stream, streamOwnerToken(stream), uuidGenerator_.next(), ++projectionSequence_ });
    if (!projectionDrainRunning_) {
        projectionDrainRunning_ = true;
        projectionScope_.spawn(drainProjection());
    }
}

void CollectorRuntime::enqueueBarrier(ProjectionCompletion completion, std::uint64_t after) {
    if (!acceptingProjection_ || projectionScope_.stop_requested()) {
        completion(false);
        return;
    }
    projectionQueue_.push_back(ProjectionBarrier{ std::move(completion), after, projectionSequence_ });
    if (!projectionDrainRunning_) {
        projectionDrainRunning_ = true;
        projectionScope_.spawn(drainProjection());
    }
}

ruvia::task<bool> CollectorRuntime::waitForProjection(ruvia::stop_token stop, std::uint64_t after) {
    auto [completion, receiver] = ruvia::make_one_shot<bool>(worker_);
    auto shared = std::make_shared<ruvia::one_shot_completion<bool>>(std::move(completion));
    enqueueBarrier([shared](bool success) {
        (void)shared->complete(success);
    },
                   after);
    auto result = co_await receiver.wait_for(std::chrono::seconds(30), stop);
    co_return result.has_value() && std::move(result).take_value();
}

ruvia::task<bool> CollectorRuntime::persistProjection(
    std::vector<service::message::StreamField> fields,
    std::string projectionId
) {
    fields.push_back({ "projection_id", projectionId });
    const auto redis = redis_.with_options({ .timeout_ = std::chrono::seconds(3), .stop_token_ = projectionScope_.get_stop_token() });
    std::string projectionOrder;
    std::string currentEntryId;
    while (!projectionScope_.stop_requested()) {
        bool retry = false;
        std::string failure;
        try {
            const auto done = co_await GbControlService::projectionReceipt(redis, projectionId);
            if (done) co_return *done;
            const auto result = co_await GbControlService::publishProjection(
                redis, projectionId, projectionOrder, currentEntryId, fields);
            if (!projectionOrder.empty() &&
                projectionOrder != result.order) {
                throw std::runtime_error(
                    "GB28181 projection ordering ID changed during repair"
                );
            }
            projectionOrder = result.order;
            currentEntryId = result.current;
        } catch (const std::exception& error) {
            retry = true;
            failure = error.what();
        } catch (...) {
            retry = true;
        }
        if (retry) {
            // Keep the ordering IDs in this coroutine across Redis transport
            // failures.  In particular, an XADD may have committed while its
            // response was lost; the sent marker or the local IDs then make
            // the next attempt recoverable without a duplicate event.
            if (!projectionScope_.stop_requested()) {
                if (!failure.empty()) {
                    LOG_WARN << "[GB28181] projection awaits durable storage: "
                             << failure;
                }
                (void)co_await ruvia::sleep_for(
                    worker_,
                    std::chrono::milliseconds(250),
                    projectionScope_.get_stop_token()
                );
            }
            continue;
        }
        // Incremental projections must remain in actor order. A caller may
        // time out, but this queue cannot publish the next snapshot until the
        // preceding DB commit has a definitive receipt (or loses ownership).
        (void)co_await ruvia::sleep_for(worker_, std::chrono::milliseconds(20), projectionScope_.get_stop_token());
    }
    co_return false;
}

ruvia::task<void> CollectorRuntime::publishDevice(const Device& device, DeviceChange change, std::string_view ownerToken, std::string_view projectionId) {
    const auto key = control_protocol::stream::owner(device.id);
    const auto current = devices_.findDevice(device.id);
    if (current && (current->sessionGeneration != device.sessionGeneration || (device.online && !current->online))) {
        currentProjectionSucceeded_ = false;
        co_return;
    }
    if (device.online && !(co_await retainOwner(key, std::string(ownerToken), device.id))) {
        currentProjectionSucceeded_ = false;
        co_return;
    }
    const auto persisted = co_await persistProjection(
        projection_protocol::deviceProjectionFields(device, change, index_, ownerToken),
        std::string(projectionId)
    );
    currentProjectionSucceeded_ = currentProjectionSucceeded_ && persisted;
    if (!device.online) {
        co_await releaseOwner(key, std::string(ownerToken));
    }
}

ruvia::task<void> CollectorRuntime::publishStream(const StreamStatus& stream, std::string_view ownerToken, std::string_view projectionId) {
    const auto identity = StreamStatus::identity(stream.app, stream.stream, stream.schema);
    const auto key = control_protocol::stream::owner(identity);
    if (stream.online && !(co_await retainOwner(key, std::string(ownerToken), {}, stream.stream))) {
        currentProjectionSucceeded_ = false;
        co_return;
    }
    const auto persisted = co_await persistProjection(
        projection_protocol::streamProjectionFields(stream, index_, ownerToken),
        std::string(projectionId)
    );
    currentProjectionSucceeded_ = currentProjectionSucceeded_ && persisted;
    if (!stream.online) {
        co_await releaseOwner(key, std::string(ownerToken));
    }
}

ruvia::task<void> CollectorRuntime::refreshOwnerLeases() {
    requireCurrentLoop();
    while (!scope_.stop_requested()) {
        const auto sleep = co_await ruvia::sleep_for(
            worker_,
            std::chrono::seconds(5),
            scope_.get_stop_token()
        );
        if (sleep == ruvia::timer_sleep_result::stop_requested ||
            scope_.stop_requested()) {
            break;
        }

        const auto now = std::chrono::steady_clock::now();
        std::vector<std::pair<std::string, std::string>> toRenew;
        std::vector<std::pair<std::string, std::string>> expired;
        toRenew.reserve(ownerLeases_.size());
        expired.reserve(ownerLeases_.size());
        for (auto& [key, lease] : ownerLeases_) {
            if (lease.expiresAt <= now) {
                expired.emplace_back(key, lease.token);
                continue;
            }
            if (!lease.renewing) {
                lease.renewing = true;
                toRenew.emplace_back(key, lease.token);
            }
        }
        for (auto& [key, token] : expired) {
            expireOwnerLease(std::move(key), std::move(token));
        }
        for (auto& [key, token] : toRenew) {
            try {
                scope_.spawn(renewOwnerLease(std::move(key), std::move(token)));
            } catch (const std::exception& error) {
                if (const auto found = ownerLeases_.find(key);
                    found != ownerLeases_.end() && found->second.token == token) {
                    found->second.renewing = false;
                }
                LOG_WARN << "[GB28181][Collector] owner lease renewal task could not start: "
                         << error.what();
            }
        }
    }
    co_return;
}

ruvia::task<void> CollectorRuntime::clearOwnerLeases() {
    requireCurrentLoop();
    std::vector<std::pair<std::string, std::string>> leases;
    leases.reserve(ownerLeases_.size());
    for (auto& [key, lease] : ownerLeases_) {
        if (lease.timer) {
            lease.timer->cancel();
        }
        leases.emplace_back(key, lease.token);
    }
    ownerLeases_.clear();

    for (auto& [key, token] : leases) {
        try {
            co_await releaseOwner(std::move(key), std::move(token));
        } catch (const std::exception& error) {
            LOG_WARN << "[GB28181][Collector] owner cleanup failed: "
                     << error.what();
        }
    }
    co_return;
}

ruvia::task<bool> CollectorRuntime::retainOwner(
    std::string key,
    std::string token,
    std::string deviceId,
    std::string streamId
) {
    requireCurrentLoop();
    if (stopping_.load() || key.empty() || token.empty() ||
        !control_protocol::stream::completeOwnerToken(token)) {
        co_return false;
    }

    if (const auto retired = retiredOwnerTokens_.find(key);
        retired != retiredOwnerTokens_.end() && retired->second == token) {
        co_return false;
    }
    constexpr auto kLocalLease = std::chrono::seconds(12);
    const auto operationStarted = std::chrono::steady_clock::now();
    const auto localDeadline = operationStarted + kLocalLease;

    const auto local = ownerLeases_.find(key);
    const bool hasLocal = local != ownerLeases_.end();
    const std::string previousToken =
        hasLocal ? local->second.token : std::string{};
    const auto previousDeadline =
        hasLocal ? local->second.expiresAt
                 : std::chrono::steady_clock::time_point::max();

    bool retained = false;
    try {
        retained = hasLocal && previousToken == token
            ? co_await GbControlService::renewOwnerLease(redis_, key, token)
            : co_await GbControlService::claimOwnerLease(redis_, key, token, previousToken);
    } catch (const std::exception& error) {
        LOG_WARN << "[GB28181][Collector] owner lease claim failed: "
                 << error.what();
    } catch (...) {
        LOG_WARN << "[GB28181][Collector] owner lease claim failed";
    }

    const auto now = std::chrono::steady_clock::now();
    auto current = ownerLeases_.find(key);
    const bool localStillValid =
        !hasLocal || (current != ownerLeases_.end() && current->second.token == previousToken);
    if (!retained || now >= localDeadline || now >= previousDeadline || !localStillValid || scope_.stop_requested()) {
        if (hasLocal && current != ownerLeases_.end() && current->second.token == previousToken) {
            expireOwnerLease(key, previousToken);
        } else if (sip_ && !stopping_.load()) {
            invalidateOwnerTarget(key, token, deviceId, streamId);
        }
        if (key.starts_with(control_protocol::stream::kOwnerPrefix)) {
            retiredOwnerTokens_[key] = token;
        }
        try {
            co_await releaseOwner(key, token);
        } catch (...) {
        }
        co_return false;
    }

    current = ownerLeases_.find(key);
    if (current != ownerLeases_.end() && current->second.token != token &&
        (!hasLocal || current->second.token != previousToken)) {
        try {
            co_await releaseOwner(key, token);
        } catch (...) {
        }
        if (sip_ && !stopping_.load()) {
            invalidateOwnerTarget(key, token, deviceId, streamId);
        }
        co_return false;
    }

    const auto deadline = localDeadline;
    if (deadline <= now) {
        try {
            co_await releaseOwner(key, token);
        } catch (...) {
        }
        if (sip_ && !stopping_.load()) {
            invalidateOwnerTarget(key, token, deviceId, streamId);
        }
        co_return false;
    }

    if (current != ownerLeases_.end() && current->second.token != token) {
        if (current->second.timer) {
            current->second.timer->cancel();
        }
        ownerLeases_.erase(current);
        current = ownerLeases_.end();
    }
    if (current == ownerLeases_.end()) {
        OwnerLease lease;
        lease.token = token;
        lease.deviceId = std::move(deviceId);
        lease.streamId = std::move(streamId);
        lease.expiresAt = deadline;
        current = ownerLeases_.emplace(std::move(key), std::move(lease)).first;
    } else {
        if (!deviceId.empty()) {
            current->second.deviceId = std::move(deviceId);
        }
        if (!streamId.empty()) {
            current->second.streamId = std::move(streamId);
        }
        current->second.expiresAt = deadline;
        current->second.renewing = false;
    }
    armOwnerLeaseTimer(current->first);
    co_return true;
}

bool CollectorRuntime::ownsLocal(std::string_view key, std::string_view token) const {
    if (key.empty() || token.empty()) {
        return false;
    }
    const auto found = ownerLeases_.find(std::string(key));
    return found != ownerLeases_.end() && found->second.token == token &&
        found->second.expiresAt > std::chrono::steady_clock::now();
}

ruvia::task<void> CollectorRuntime::releaseOwner(std::string key, std::string token) {
    requireCurrentLoop();
    if (key.empty() || token.empty()) {
        co_return;
    }

    if (const auto found = ownerLeases_.find(key);
        found != ownerLeases_.end() && found->second.token == token) {
        if (found->second.timer) {
            found->second.timer->cancel();
        }
        ownerLeases_.erase(found);
    }

    co_await GbControlService::releaseOwnerLease(redis_, key, token);
    co_return;
}

ruvia::task<void> CollectorRuntime::renewOwnerLease(std::string key, std::string token) {
    requireCurrentLoop();
    const auto found = ownerLeases_.find(key);
    if (found == ownerLeases_.end() || found->second.token != token) {
        co_return;
    }
    const auto operationStarted = std::chrono::steady_clock::now();
    const auto previousDeadline = found->second.expiresAt;
    if (previousDeadline <= std::chrono::steady_clock::now()) {
        expireOwnerLease(key, token);
        co_return;
    }

    bool renewed = false;
    try {
        renewed = co_await GbControlService::renewOwnerLease(redis_, key, token);
    } catch (const std::exception& error) {
        LOG_WARN << "[GB28181][Collector] owner lease renewal failed: "
                 << error.what();
    } catch (...) {
        LOG_WARN << "[GB28181][Collector] owner lease renewal failed";
    }

    const auto now = std::chrono::steady_clock::now();
    const auto current = ownerLeases_.find(key);
    if (current == ownerLeases_.end() || current->second.token != token) {
        co_return;
    }
    current->second.renewing = false;
    if (!renewed || now >= previousDeadline) {
        expireOwnerLease(key, token);
        try {
            co_await releaseOwner(key, token);
        } catch (const std::exception& error) {
            LOG_WARN << "[GB28181][Collector] expired owner cleanup failed: "
                     << error.what();
        } catch (...) {
            LOG_WARN << "[GB28181][Collector] expired owner cleanup failed";
        }
        co_return;
    }

    current->second.expiresAt = operationStarted + std::chrono::seconds(12);
    armOwnerLeaseTimer(current->first);
    co_return;
}

void CollectorRuntime::armOwnerLeaseTimer(std::string key) {
    if (!loop_.valid() || !loop_.is_current()) {
        return;
    }
    const auto found = ownerLeases_.find(key);
    if (found == ownerLeases_.end()) {
        return;
    }
    auto& lease = found->second;
    if (!lease.timer) {
        lease.timer = std::make_unique<asio::steady_timer>(loop_.io_context());
    }
    lease.timer->expires_at(lease.expiresAt);
    const auto timerToken = lease.token;
    lease.timer->async_wait(
        [this, key = std::move(key), timerToken](const std::error_code& error) {
            if (error) {
                return;
            }
            const auto current = ownerLeases_.find(key);
            if (current == ownerLeases_.end() || current->second.token != timerToken) {
                return;
            }
            if (current->second.expiresAt > std::chrono::steady_clock::now()) {
                armOwnerLeaseTimer(key);
            } else {
                expireOwnerLease(key, timerToken);
            }
        }
    );
}

void CollectorRuntime::expireOwnerLease(std::string key, std::string token) {
    if (!loop_.valid() || !loop_.is_current()) {
        return;
    }
    const auto found = ownerLeases_.find(key);
    if (found == ownerLeases_.end() || found->second.token != token) {
        return;
    }
    if (key.starts_with(control_protocol::stream::kOwnerPrefix)) {
        retiredOwnerTokens_[key] = token;
    }
    auto deviceId = std::move(found->second.deviceId);
    auto streamId = std::move(found->second.streamId);
    ownerLeases_.erase(found);
    if (stopping_.load() || !sip_) {
        return;
    }
    invalidateOwnerTarget(key, token, deviceId, streamId);
}

void CollectorRuntime::invalidateOwnerTarget(const std::string& key, const std::string& token, const std::string& deviceId, const std::string& streamId) {
    if (!sip_ || stopping_.load()) {
        return;
    }
    constexpr auto prefix = control_protocol::stream::kSessionOwnerPrefix;
    if (key.starts_with(prefix)) {
        (void)sip_->stopPreview(key.substr(prefix.size()));
        return;
    }
    if (!deviceId.empty()) {
        const auto device = devices_.findDevice(deviceId);
        if (device && deviceOwnerToken(*device) == token) {
            sip_->invalidateDevice(deviceId, "owner_lease_lost");
        }
    }
    if (!streamId.empty()) {
        const auto current = ownerLeases_.find(key);
        if (current == ownerLeases_.end() || current->second.token == token) {
            (void)sip_->stopPreviewByStream(streamId);
        }
    }
}

ruvia::task<void> CollectorRuntime::drainProjection() {
    requireCurrentLoop();
    while (!projectionQueue_.empty() && !projectionScope_.stop_requested()) {
        auto event = std::move(projectionQueue_.front());
        projectionQueue_.pop_front();
        bool retry = false;
        currentProjectionSucceeded_ = true;
        std::uint64_t eventSequence = 0;
        if (auto* device = std::get_if<DeviceProjection>(&event)) {
            eventSequence = device->sequence;
        }
        if (auto* stream = std::get_if<StreamProjection>(&event)) {
            eventSequence = stream->sequence;
        }
        try {
            if (auto* device = std::get_if<DeviceProjection>(&event)) {
                co_await publishDevice(device->device, device->change, device->ownerToken, device->projectionId);
            } else if (auto* stream = std::get_if<StreamProjection>(&event)) {
                co_await publishStream(stream->stream, stream->ownerToken, stream->projectionId);
            } else {
                const auto& barrier = std::get<ProjectionBarrier>(event);
                const bool failed = std::any_of(projectionFailures_.begin(), projectionFailures_.end(), [&](const auto& failure) {
                    return failure > barrier.after && failure <= barrier.through;
                });
                barrier.complete(!failed);
            }
        } catch (const std::exception& error) {
            retry = true;
            if (!projectionScope_.stop_requested()) {
                LOG_WARN << "[GB28181] projection awaits durable storage: " << error.what();
            }
        } catch (...) {
            retry = true;
        }
        if (!retry && eventSequence && !currentProjectionSucceeded_) {
            projectionFailures_.push_back(eventSequence);
        }
        auto oldestNeeded = std::min(lastSipBarrierSequence_, activeControlProjection_.value_or(projectionSequence_));
        for (const auto& queued : projectionQueue_) {
            if (const auto* barrier = std::get_if<ProjectionBarrier>(&queued)) {
                oldestNeeded = std::min(oldestNeeded, barrier->after);
            }
        }
        while (!projectionFailures_.empty() && projectionFailures_.front() <= oldestNeeded) {
            projectionFailures_.pop_front();
        }
        if (retry) {
            projectionQueue_.push_front(std::move(event));
            (void)co_await ruvia::sleep_for(worker_, std::chrono::milliseconds(250), projectionScope_.get_stop_token());
        }
    }
    projectionDrainRunning_ = false;
}

ruvia::task<void> CollectorRuntime::controlLoop() {
    requireCurrentLoop();
    const auto stream = control_protocol::stream::control(index_, service::runtime::instanceId());
    const auto group = control_protocol::stream::kControlGroup;
    const auto consumer = ownerToken_;
    const auto redis = redis_.with_options({ .stop_token_ = scope_.get_stop_token() });
    const std::vector<std::string> streams{ stream };
    while (!scope_.stop_requested()) {
        bool failed = false;
        try {
            static constexpr std::string_view groupScript = R"lua(
local created=redis.pcall('XGROUP','CREATE',KEYS[1],ARGV[1],'0','MKSTREAM')
if type(created)=='table' and created.err and not string.find(created.err,'BUSYGROUP',1,true) then
 return redis.error_reply(created.err)
end
redis.call('EXPIRE',KEYS[1],600)
return 1
)lua";
            const std::string_view groupKeys[]{ stream };
            const std::string_view groupArgs[]{ group };
            const auto initialized = co_await redis.eval(groupScript, groupKeys, groupArgs);
            if (initialized.kind() != ruvia::redis_value::kind_type::integer) {
                service::message::redis::throwValue("GB28181 control group", initialized);
            }

            // This stream is exclusive to this process incarnation and owner.
            const auto pending = co_await service::message::redis::claimGroupMany(
                redis,
                streams,
                group,
                consumer,
                32,
                std::chrono::milliseconds(0)
            );
            for (const auto& batch : pending) {
                for (const auto& message : batch.messages) {
                    co_await handleControl(message);
                }
            }
            const auto messages = co_await service::message::redis::readGroupBlockingUntil(
                redis,
                stream,
                group,
                consumer,
                scope_.get_stop_token(),
                std::chrono::milliseconds(1000),
                32
            );
            for (const auto& message : messages) {
                co_await handleControl(message);
            }
        } catch (const std::exception& error) {
            failed = true;
            if (!scope_.stop_requested()) {
                LOG_WARN << "[GB28181] control consumer: " << error.what();
            }
        }
        if (failed) {
            (void)co_await ruvia::sleep_for(worker_, std::chrono::milliseconds(250), scope_.get_stop_token());
        }
    }
}

ruvia::task<void> CollectorRuntime::handleControl(
    const service::message::StreamMessage& message
) {
    requireCurrentLoop();
    const auto requestId = std::string(message.get("request_id"));
    if (!service::common::isUuid(requestId)) {
        const std::string id = message.id;
        co_await service::message::redis::acknowledgeAndDelete(redis_, control_protocol::stream::control(index_, service::runtime::instanceId()), control_protocol::stream::kControlGroup, id);
        co_return;
    }
    const auto replyStream = control_protocol::stream::reply(requestId);
    const auto operation = std::string(message.get("operation"));
    const auto payload = std::string(message.get("payload"));
    const auto resultKey = control_protocol::stream::result(requestId);
    const auto claimKey = resultKey + ":claim";
    const auto cancelKey = control_protocol::stream::cancel(requestId);
    const auto key = std::string(message.get("owner_key"));
    const auto token = std::string(message.get("owner_token"));
    const auto deadlineText = message.get("deadline_ms");
    std::int64_t deadlineMs{};
    const auto parsed = std::from_chars(deadlineText.data(), deadlineText.data() + deadlineText.size(), deadlineMs);
    const bool validDeadline = !deadlineText.empty() && parsed.ec == std::errc{} &&
        parsed.ptr == deadlineText.data() + deadlineText.size();
    const auto nowMs = [] {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch()
        )
            .count();
    };
    std::string status, result, claimOwner;
    const auto fail = [&](int code, std::string_view reason) {
        status = "error";
        result = control_protocol::operationJson(code, reason);
    };
    const auto redis = redis_.with_options({ .timeout_ = std::chrono::seconds(3), .stop_token_ = scope_.get_stop_token() });
    const auto cached = co_await GbControlService::loadControlResult(redis, resultKey);
    if (cached) {
        status = cached->status;
        result = cached->payload;
    } else if (!validDeadline || deadlineMs > nowMs() + 60000) {
        fail(400, "invalid command deadline");
    } else if (deadlineMs <= nowMs()) {
        fail(408, "GB28181 command expired");
    } else if (!ownsLocal(key, token)) {
        fail(409, "stale GB28181 connection owner");
    } else {
        const auto claimed = co_await GbControlService::claimControlExecution(
            redis, key, claimKey, cancelKey, token, deadlineText);
        if (claimed != ControlClaimResult::kClaimed) {
            fail(409, claimed == ControlClaimResult::kAlreadyClaimed ? "previous execution outcome is unknown; command will not be replayed" : "GB28181 command expired, cancelled or lost its owner");
        } else {
            claimOwner = token;
            const auto cancelled = co_await GbControlService::controlCancelled(redis, cancelKey);
            if (cancelled || scope_.stop_requested() || deadlineMs <= nowMs() || !ownsLocal(key, token)) {
                fail(408, "GB28181 command cancelled or ownership expired while claiming");
            } else {
                try {
                    const auto beforeProjection = projectionSequence_;
                    activeControlProjection_ = beforeProjection;
                    result = co_await execute(operation, payload, scope_.get_stop_token());
                    if (!(co_await waitForProjection(scope_.get_stop_token(), beforeProjection))) {
                        throw std::runtime_error("GB28181 projection was not durably confirmed");
                    }
                    status = "ok";
                } catch (const std::exception& error) {
                    fail(500, error.what());
                } catch (...) {
                    fail(500, "GB28181 operation failed");
                }
                activeControlProjection_.reset();
            }
        }
    }
    // Preserve the completed result in this coroutine across Redis failures;
    // retry only its durable reply and acknowledgement, never the SDK action.
    while (!scope_.stop_requested()) {
        bool saved = false;
        try {
            co_await GbControlService::publishReplyAndAcknowledge(redis, replyStream, control_protocol::stream::control(index_, service::runtime::instanceId()), control_protocol::stream::kControlGroup, message.id, requestId, operation, status, result, resultKey, claimKey, claimOwner);
            saved = true;
        } catch (const std::exception& error) {
            if (!scope_.stop_requested()) {
                LOG_WARN << "[GB28181] completed result awaits Redis: " << error.what();
            }
        }
        if (saved) {
            co_return;
        }
        (void)co_await ruvia::sleep_for(worker_, std::chrono::milliseconds(250), scope_.get_stop_token());
    }
}

ruvia::task<void> CollectorRuntime::retainPreview(const SipPreviewStartResult& preview) {
    const auto device = devices_.findDevice(preview.deviceId);
    if (!device || !device->online) {
        (void)sip_->stopPreview(preview.sessionId);
        throw std::runtime_error("GB28181 preview device lost its owner");
    }
    const auto token = deviceOwnerToken(*device);
    const auto key = control_protocol::stream::sessionOwner(preview.sessionId);
    std::exception_ptr failure;
    bool retained = false;
    try {
        retained = co_await retainOwner(key, token, preview.deviceId);
        if (retained) {
            previewOwners_[preview.sessionId] = token;
        }
    } catch (...) {
        failure = std::current_exception();
    }
    if (!retained || !ownsLocal(control_protocol::stream::owner(preview.deviceId), token)) {
        (void)sip_->stopPreview(preview.sessionId);
        co_await releaseOwner(key, token);
        if (failure) {
            std::rethrow_exception(failure);
        }
        throw std::runtime_error("GB28181 preview owner could not be retained");
    }
}

ruvia::task<void> CollectorRuntime::releasePreview(std::string sessionId) {
    const auto found = previewOwners_.find(sessionId);
    if (found == previewOwners_.end()) {
        co_return;
    }
    const auto token = found->second;
    previewOwners_.erase(found);
    co_await releaseOwner(control_protocol::stream::sessionOwner(sessionId), token);
}

ruvia::task<std::string>
CollectorRuntime::execute(std::string operation, std::string payload, ruvia::stop_token stop) {
    requireCurrentLoop();
    if (!started_.load()) {
        throw std::runtime_error("GB28181 Collector is not started");
    }
    if (stopping_.load() || stop.stop_requested()) {
        throw std::runtime_error("GB28181 operation was cancelled");
    }
    if (!sip_) {
        throw std::runtime_error("GB28181 SIP actor is unavailable");
    }

    std::pmr::monotonic_buffer_resource resource;
    auto request = ruvia::from_json<service::message::gb28181::ControlRequest>(
        payload.empty() ? std::string_view{ "{}" } : payload,
        { .resource_ = &resource }
    );
    if (!request) {
        throw std::invalid_argument("GB28181 RPC request body is invalid");
    }
    const auto& input = *request;

    if (operation == control_protocol::kCatalogOperation) {
        const auto id = control_protocol::requiredText(input.get<"deviceId">(), "设备编号不能为空");
        co_return control_protocol::jsonAction(sip_->queryCatalog(id), id);
    }
    if (operation == control_protocol::kRenameDeviceOperation) {
        const auto id = control_protocol::requiredText(input.get<"deviceId">(), "设备编号不能为空");
        const auto name = control_protocol::requiredText(input.get<"name">(), "名称不能为空");
        if (!devices_.updateDeviceName(id, name)) {
            throw std::runtime_error("GB28181 device does not exist");
        }
        co_return control_protocol::operationJson(0, "摄像头名称已更新");
    }
    if (operation == control_protocol::kRenameChannelOperation) {
        const auto id = control_protocol::requiredText(input.get<"deviceId">(), "设备编号不能为空");
        const auto channel = control_protocol::requiredText(input.get<"channelId">(), "通道编号不能为空");
        const auto name = control_protocol::requiredText(input.get<"name">(), "名称不能为空");
        if (!devices_.updateChannelName(id, channel, name)) {
            throw std::runtime_error("GB28181 device or channel does not exist");
        }
        co_return control_protocol::operationJson(0, "通道名称已更新");
    }
    if (operation == control_protocol::kPreviewStartOperation) {
        const auto id = control_protocol::requiredText(input.get<"deviceId">(), "设备编号不能为空");
        const auto channel = control_protocol::requiredText(input.get<"channelId">(), "通道编号不能为空");
        const auto result = sip_->startPreview(id, channel);
        if (!result) {
            throw std::runtime_error("GB28181 device or channel unavailable");
        }
        co_await retainPreview(*result);
        co_return control_protocol::jsonPreviewStart(*result);
    }
    if (operation == control_protocol::kPreviewStopOperation) {
        const auto session = control_protocol::requiredText(input.get<"sessionId">(), "会话编号不能为空");
        const auto result = sip_->stopPreview(session);
        if (!result) {
            throw std::runtime_error("GB28181 preview session does not exist");
        }
        co_return control_protocol::jsonPreviewStop(*result);
    }
    if (operation == control_protocol::kPreviewHeartbeatOperation) {
        const auto session = control_protocol::requiredText(input.get<"sessionId">(), "会话编号不能为空");
        co_return control_protocol::jsonAction(sip_->renewPreview(session));
    }
    if (operation == control_protocol::kPtzOperation) {
        const auto id = control_protocol::requiredText(input.get<"deviceId">(), "设备编号不能为空");
        const auto channel = control_protocol::requiredText(input.get<"channelId">(), "通道编号不能为空");
        const auto action = control_protocol::requiredText(input.get<"action">(), "云台动作不能为空");
        control_protocol::requireAction(action);
        const auto speedValue = input.get<"speed">();
        const auto speed = speedValue ? control_protocol::requiredInteger(speedValue, "speed 无效")
                                      : 80;
        if (speed < 0 || speed > 255) {
            throw std::invalid_argument("speed must be between 0 and 255");
        }
        co_return control_protocol::jsonAction(
            sip_->sendPtzControl(id, channel, action, static_cast<std::uint8_t>(speed)),
            id,
            channel
        );
    }
    if (operation == control_protocol::kPtzPositionOperation) {
        const auto id = control_protocol::requiredText(input.get<"deviceId">(), "设备编号不能为空");
        const auto channel = control_protocol::requiredText(input.get<"channelId">(), "通道编号不能为空");
        const auto pan = control_protocol::requiredFinite(input.get<"pan">(), "pan", 0.0, 360.0);
        const auto tilt =
            control_protocol::requiredFinite(input.get<"tilt">(), "tilt", -30.0, 90.0);
        const auto zoom =
            control_protocol::requiredFinite(input.get<"zoom">(), "zoom", 1.0, 1000.0);
        co_return control_protocol::jsonAction(
            sip_->sendPtzPreciseControl(id, channel, pan, tilt, zoom),
            id,
            channel
        );
    }
    if (operation == control_protocol::kRecordingOperation ||
        operation == control_protocol::kRecordingStartOperation ||
        operation == control_protocol::kRecordingStopOperation) {
        const auto streamId = control_protocol::requiredText(input.get<"streamId">(), "流编号不能为空");
        ZlmSdk::OwnerScope owner(index_);
        auto& sdk = sdkSupervisor().sdk();
        if (operation == control_protocol::kRecordingOperation) {
            co_return control_protocol::jsonRecording(sdk.isMp4Recording(streamId));
        }
        const auto changed = operation == control_protocol::kRecordingStartOperation
            ? sdk.startMp4Recording(streamId)
            : sdk.stopMp4Recording(streamId);
        co_return control_protocol::jsonAction(changed);
    }
    if (operation == control_protocol::kHealthOperation ||
        operation == control_protocol::kDevicesOperation) {
        throw std::runtime_error(
            "GB28181 queries must be served from the Service projection"
        );
    }
    throw std::invalid_argument("unsupported GB28181 operation");
}

} // namespace service::gb28181
