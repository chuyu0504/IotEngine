#include "service/features/gb28181/gb28181.runtime.h"

#include <algorithm>
#include <charconv>
#include <exception>
#include <memory_resource>
#include <stdexcept>
#include <utility>

#include <ruvia/web/error.h>

#include "service/common/http.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/features/gb28181/gb28181.protocol.h"
#include "service/utils/log.h"
#include "service/utils/redis.h"
#include "service/utils/uuid.h"

namespace service::gb28181 {

namespace {

// The publishing Collector Worker is encoded in the owner token; its receipt
// stream belongs to that worker's process incarnation.
std::string receiptStreamFor(std::string_view ownerToken) {
    const auto owner = control_protocol::stream::ownerIndex(ownerToken);
    const auto instance = control_protocol::stream::ownerInstance(ownerToken);
    if (!owner || !instance) {
        throw std::runtime_error("GB28181 projection owner token has no Collector Worker");
    }
    return control_protocol::stream::projectionReceipts(*owner, *instance);
}

} // namespace

void GbProjectionRuntime::start(ruvia::web_worker_handle worker) {
    if (stopped_.valid()) {
        throw std::logic_error("GB28181 projection runtime is already started");
    }
    stop_.emplace();
    utils::startWorkerTask(worker, stop_->token(), stopped_, [this](ruvia::web_worker_context& context, utils::WorkerReady& ready, ruvia::stop_token stop) {
        return consume(context, ready, stop);
    });
}

void GbProjectionRuntime::stop() noexcept {
    if (stop_) {
        utils::stopWorkerTask(*stop_, stopped_);
        stop_.reset();
    }
}

ruvia::task<void> GbProjectionRuntime::consume(
    ruvia::web_worker_context& context,
    utils::WorkerReady& ready,
    ruvia::stop_token stop
) {
    const auto redis = context.redis().with_options({ .stop_token_ = stop });
    const std::string stream(control_protocol::stream::kProjection);
    const auto group = control_protocol::stream::kProjectionGroup;
    const auto consumer = identity_.instanceId + ":service:" + std::to_string(identity_.index);
    const std::vector<std::string> streams{
        stream,
        service::message::sharedWakeStream(identity_.instanceId)
    };
    for (const auto& input : streams) {
        co_await service::utils::redis::ensureGroup(redis, input, group);
    }
    co_await service::utils::redis::withConsumerNotifications(
        redis,
        identity_.channels,
        context.worker(),
        streams,
        group,
        stop,
        [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
            co_await seedOwnerDeadlines(context);
            ready.set_value();
            bool recovering = true;
            bool checkOwners = true;
            std::optional<std::chrono::steady_clock::time_point> ownerDeadline;
            while (!stop.stop_requested()) {
                std::string failure;
                try {
                    if (checkOwners || (ownerDeadline && std::chrono::steady_clock::now() >= *ownerDeadline)) {
                        const auto wait = co_await expireDueOwners(context);
                        ownerDeadline = wait ? std::optional(std::chrono::steady_clock::now() + *wait) : std::nullopt;
                        checkOwners = false;
                    }
                    const auto timeout = ownerDeadline
                        ? std::optional(std::max(std::chrono::milliseconds(0), std::chrono::ceil<std::chrono::milliseconds>(*ownerDeadline - std::chrono::steady_clock::now())))
                        : std::nullopt;
                    const auto batches = co_await service::utils::redis::consumeGroup(
                        redis,
                        identity_.channels,
                        streams,
                        group,
                        consumer,
                        recovering,
                        notifications,
                        stop,
                        timeout,
                        kBatchSize
                    );
                    for (const auto& batch : batches) {
                        for (const auto& message : batch.messages) {
                            if (stop.stop_requested()) {
                                co_return;
                            }
                            if (batch.stream == stream) {
                                co_await project(context, redis, batch.stream, message);
                                checkOwners = true;
                            } else {
                                if (message.get("task") == service::message::workerStreamTaskName(service::message::WorkerStreamTask::Gb28181Projector)) {
                                    checkOwners = true;
                                }
                                co_await service::utils::redis::acknowledge(redis, identity_.channels, batch.stream, group, message.id);
                            }
                        }
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        co_return;
                    }
                    failure = error.what();
                }
                if (!failure.empty()) {
                    LOG_WARN << "[GB28181][GbProjectionRuntime] projection failed: " << failure;
                    recovering = true;
                    checkOwners = true;
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(250), stop);
                }
            }
        }
    );
}

ruvia::task<void> GbProjectionRuntime::project(ruvia::web_worker_context& context, const ruvia::redis_handle& redis, std::string_view stream, const service::message::StreamMessage& message) {
    const auto group = control_protocol::stream::kProjectionGroup;
    const auto projectionId = message.get("projection_id");
    const auto ownerToken = message.get("owner_token");
    if (!service::common::isUuid(projectionId) || !control_protocol::stream::completeOwnerToken(ownerToken)) {
        LOG_WARN << "[GB28181] invalid projection identity: " << message.id;
        co_await service::utils::redis::acknowledgeAndDelete(redis, identity_.channels, stream, group, message.id);
        co_return;
    }
    const auto receiptStream = receiptStreamFor(ownerToken);
    const auto projectionOrder = message.get("projection_order");
    std::string cursorId(message.id);
    if (!projectionOrder.empty()) {
        if (!control_protocol::validProjectionStreamId(projectionOrder)) {
            LOG_WARN << "[GB28181] invalid projection order: " << message.id;
            co_await GbControlService::markProjectionDoneAndAcknowledge(redis, identity_.channels, stream, group, message.id, projectionId, receiptStream, false);
            co_return;
        }
        cursorId = std::string(projectionOrder);
    } else if (!control_protocol::validProjectionStreamId(cursorId)) {
        LOG_WARN << "[GB28181] invalid projection stream ID: " << message.id;
        co_await service::utils::redis::acknowledgeAndDelete(redis, identity_.channels, stream, group, message.id);
        co_return;
    }
    const auto eventType = message.get("event_type");
    std::optional<device::Device> device;
    std::optional<media::StreamStatus> streamStatus;
    std::optional<device::DeviceChangeSet> deviceChanges;
    std::string parseError;
    try {
        if (eventType == "gb28181.device") {
            deviceChanges = projection_protocol::projectionDeviceChanges(message.get("change"));
            if (!deviceChanges) {
                throw std::runtime_error("GB28181 device projection has no valid change");
            }
            device = projection_protocol::deviceFromProjection(message);
        } else if (eventType == "gb28181.stream") {
            streamStatus = projection_protocol::streamFromProjection(message);
        } else {
            throw std::runtime_error("unsupported GB28181 projection event type");
        }
    } catch (const std::exception& error) {
        parseError = error.what();
    }
    if (!parseError.empty()) {
        LOG_WARN << "[GB28181][GbProjectionRuntime] dropping malformed projection: " << parseError;
        co_await GbControlService::markProjectionDoneAndAcknowledge(redis, identity_.channels, stream, group, message.id, projectionId, receiptStream, false);
        co_return;
    }
    bool applied = false;
    if (device) {
        if (co_await projectionOwnerMatches(redis, device->id, device->online, ownerToken)) {
            applied = co_await applyDeviceProjection(
                context,
                *device,
                *deviceChanges,
                control_protocol::stream::owner(device->id),
                std::string(ownerToken),
                cursorId
            );
        }
    } else if (streamStatus) {
        const auto identity = media::StreamStatus::identity(streamStatus->app, streamStatus->stream, streamStatus->schema);
        if (co_await projectionOwnerMatches(redis, identity, streamStatus->online, ownerToken)) {
            applied = co_await applyStreamProjection(
                context,
                *streamStatus,
                control_protocol::stream::owner(identity),
                std::string(ownerToken),
                cursorId
            );
        }
    }
    co_await GbControlService::markProjectionDoneAndAcknowledge(redis, identity_.channels, stream, group, message.id, projectionId, receiptStream, applied);
}

CollectorRuntime::CollectorRuntime(ruvia::event_loop loop, ruvia::redis_handle redis, Config config, common::WorkerIdentity identity)
    : identity_(std::move(identity)), config_(std::move(config)),
      loop_(std::move(loop)),
      worker_(loop_.handle()),
      redis_(std::move(redis)),
      index_(identity_.index),
      scope_(worker_),
      projectionScope_(worker_),
      devices_([this](const device::Device& device, device::DeviceChange change) {
          enqueueDeviceProjection(device, change);
      }),
      streams_([this](const media::StreamStatus& stream) {
          enqueueStreamProjection(stream);
      }),
      ownerToken_(identity_.instanceId + ":collector:" + std::to_string(index_)) {
    if (identity_.count == 0 || index_ >= identity_.count) {
        throw std::invalid_argument(
            "GB28181 Collector owner index is outside worker count"
        );
    }
}

std::string CollectorRuntime::deviceOwnerToken(const device::Device& device) const {
    return ownerToken_ + ":session:" +
        std::to_string(device.sessionGeneration);
}

std::string CollectorRuntime::streamOwnerToken(const media::StreamStatus& stream) {
    const auto identity =
        media::StreamStatus::identity(stream.app, stream.stream, stream.schema);
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
    if (started_.load() || sdk_ || scope_.size() != 0 || projectionScope_.size() != 0) {
        std::terminate();
    }
}

void CollectorRuntime::requireCurrentLoop() const {
    if (!loop_.valid() || !worker_.valid() || !loop_.is_current()) {
        throw std::logic_error(
            "GB28181 CollectorRuntime operation must run on its owning loop"
        );
    }
}

void CollectorRuntime::registerSdkRoute() {
    requireCurrentLoop();
    if (sdk_) {
        return;
    }

    media::SdkSupervisor::CollectorCallbacks callbacks;
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
    // The supervisor hands this actor a lifetime-bound SDK handle together with
    // the callback route; it stays valid until the route is unregistered.
    sdk_ = media::sdkSupervisor().registerCollector(index_, loop_, std::move(callbacks));
}

void CollectorRuntime::unregisterSdkRoute() noexcept {
    if (!sdk_) {
        return;
    }
    media::sdkSupervisor().unregisterCollector(index_);
    sdk_.reset();
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
        co_await GbProjectionService::publishConfig(redis_, config_, media::MediaStatus{});
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
    if (!media::sdkSupervisor().started()) {
        throw std::runtime_error(
            "ZLMediaKit SDK supervisor must start before GB28181 Collector"
        );
    }

    std::exception_ptr startupFailure;
    try {
        registerSdkRoute();
        sip_ = std::make_shared<sip::SipServer>(
            uuidState_,
            config_.sip,
            config_.media,
            devices_,
            sdk_,
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
            lastSipBarrierSequence_ = projectionQueue_.sequence();
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
        co_await GbProjectionService::publishConfig(
            redis_,
            config_,
            media::MediaStatus{ .started = sdk_->started(), .ports = sdk_->ports(), .capabilities = sdk_->capabilities() }
        );
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
    } catch (const std::exception& error) {
        LOG_WARN << "[GB28181] projection shutdown: " << error.what();
    }
    // No producer or renewal coroutine can recreate an owner after this point.
    co_await clearOwnerLeases();
    if (!projectionQueue_.empty()) {
        LOG_WARN << "[GB28181] stopped with unconfirmed projections; no positive SIP acknowledgement was issued";
    }
    for (auto& barrier : projectionQueue_.clear()) {
        barrier.complete(false);
    }
    scopeJoined_ = true;
    sip_.reset();
    co_return;
}

void CollectorRuntime::startProjectionDrain() {
    if (!projectionDrainRunning_) {
        projectionDrainRunning_ = true;
        projectionScope_.spawn(drainProjection());
    }
}

void CollectorRuntime::enqueueDeviceProjection(const device::Device& device, device::DeviceChange change) {
    if (!acceptingProjection_) {
        return;
    }
    projectionQueue_.pushDevice(device, change, deviceOwnerToken(device), service::utils::nextUuid(uuidState_));
    startProjectionDrain();
}

void CollectorRuntime::enqueueStreamProjection(const media::StreamStatus& stream) {
    if (!acceptingProjection_) {
        return;
    }
    projectionQueue_.pushStream(stream, streamOwnerToken(stream), service::utils::nextUuid(uuidState_));
    startProjectionDrain();
}

void CollectorRuntime::enqueueBarrier(ProjectionCompletion completion, std::uint64_t after) {
    if (!acceptingProjection_ || projectionScope_.stop_requested()) {
        completion(false);
        return;
    }
    if (projectionQueue_.barriersFull()) {
        // Backpressure: the waiting SIP transaction or control command fails
        // now and its sender retries; queued state projections are kept.
        if (!projectionBackpressure_) {
            projectionBackpressure_ = true;
            LOG_WARN << "[GB28181] projection backlog reached " << kProjectionBarrierCapacity
                     << " pending acknowledgements; refusing new ones until it drains";
        }
        completion(false);
        return;
    }
    if (projectionBackpressure_) {
        projectionBackpressure_ = false;
        LOG_INFO << "[GB28181] projection backlog drained; accepting acknowledgements";
    }
    projectionQueue_.pushBarrier(std::move(completion), after);
    startProjectionDrain();
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
    const auto stop = projectionScope_.get_stop_token();
    const auto redis = redis_.with_options({ .timeout_ = std::chrono::seconds(3), .stop_token_ = stop });
    const auto topic = control_protocol::stream::projectionReceipts(index_, identity_.instanceId);
    std::string projectionOrder;
    std::string currentEntryId;
    while (!stop.stop_requested()) {
        std::string failure;
        try {
            // The dedicated connection has no read timeout. Confirm subscription
            // before inspecting durable state or publishing a projection.
            auto subscription = redis_.with_options({ .stop_token_ = stop }).subscription();
            const std::array<std::string_view, 1> topics{ topic };
            co_await subscription.subscribe(topics);
            while (!stop.stop_requested()) {
                const auto event = co_await subscription.read();
                if (!event) {
                    throw std::runtime_error("GB28181 projection receipt subscription closed");
                }
                if (event->kind() == ruvia::redis_pubsub_event_kind::subscribed) {
                    break;
                }
            }
            if (stop.stop_requested()) {
                break;
            }
            // Reconnecting performs one durable recovery read. It cannot lose a
            // completion between this read and the following subscribed wait.
            if (const auto done = co_await GbControlService::projectionReceipt(redis, projectionId)) {
                co_return *done;
            }
            const auto result = co_await GbControlService::publishProjection(
                redis,
                identity_.channels,
                projectionId,
                projectionOrder,
                currentEntryId,
                fields,
                identity_.instanceId
            );
            if (!projectionOrder.empty() && projectionOrder != result.order) {
                throw std::runtime_error("GB28181 projection ordering ID changed during repair");
            }
            projectionOrder = result.order;
            currentEntryId = result.current;
            while (!stop.stop_requested()) {
                const auto event = co_await subscription.read();
                if (!event) {
                    throw std::runtime_error("GB28181 projection receipt subscription disconnected");
                }
                if (event->kind() != ruvia::redis_pubsub_event_kind::message) {
                    continue;
                }
                if (const auto done = co_await GbControlService::projectionReceipt(redis, projectionId)) {
                    co_return *done;
                }
            }
        } catch (const std::exception& error) {
            failure = error.what();
        }
        if (!stop.stop_requested()) {
            LOG_WARN << "[GB28181] projection awaits durable storage: " << failure;
            (void)co_await ruvia::sleep_for(worker_, std::chrono::milliseconds(250), stop);
        }
    }
    co_return false;
}

ruvia::task<bool> CollectorRuntime::publishDevice(const DeviceProjection& projection) {
    const auto& device = projection.device;
    const auto key = control_protocol::stream::owner(device.id);
    const auto current = devices_.findDevice(device.id);
    if (current && (current->sessionGeneration != device.sessionGeneration || (device.online && !current->online))) {
        co_return false;
    }
    if (device.online && !(co_await retainOwner(key, projection.ownerToken, device.id))) {
        co_return false;
    }
    const auto persisted = co_await persistProjection(
        projection_protocol::deviceProjectionFields(device, projection.changes, index_, projection.ownerToken),
        projection.projectionId
    );
    if (!device.online) {
        co_await releaseOwner(
            key,
            projection.ownerToken,
            persisted ? control_protocol::stream::ownerDeadlineMember(control_protocol::stream::kDeviceDeadlineKind, device.id) : std::string{}
        );
    }
    co_return persisted;
}

ruvia::task<bool> CollectorRuntime::publishStream(const StreamProjection& projection) {
    const auto& stream = projection.stream;
    const auto identity = media::StreamStatus::identity(stream.app, stream.stream, stream.schema);
    const auto key = control_protocol::stream::owner(identity);
    if (stream.online && !(co_await retainOwner(key, projection.ownerToken, {}, stream.stream))) {
        co_return false;
    }
    const auto persisted = co_await persistProjection(
        projection_protocol::streamProjectionFields(stream, index_, projection.ownerToken),
        projection.projectionId
    );
    if (!stream.online) {
        co_await releaseOwner(
            key,
            projection.ownerToken,
            persisted ? control_protocol::stream::ownerDeadlineMember(control_protocol::stream::kStreamDeadlineKind, identity) : std::string{}
        );
    }
    co_return persisted;
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
    // Device and stream owners are tracked in the owner deadline set; preview
    // session owners have no durable aggregate.
    const auto deadlineMember = key.starts_with(control_protocol::stream::kOwnerPrefix)
        ? control_protocol::stream::ownerDeadlineMember(
              deviceId.empty() ? control_protocol::stream::kStreamDeadlineKind : control_protocol::stream::kDeviceDeadlineKind,
              std::string_view(key).substr(control_protocol::stream::kOwnerPrefix.size())
          )
        : std::string{};

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
            ? co_await GbControlService::renewOwnerLease(redis_, key, token, deadlineMember)
            : co_await GbControlService::claimOwnerLease(redis_, identity_.channels, key, token, previousToken, deadlineMember, identity_.instanceId);
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
        } catch (const std::exception& error) {
            LOG_WARN << "[GB28181][Collector] lost owner cleanup failed: " << error.what();
        }
        co_return false;
    }

    current = ownerLeases_.find(key);
    if (current != ownerLeases_.end() && current->second.token != token &&
        (!hasLocal || current->second.token != previousToken)) {
        try {
            co_await releaseOwner(key, token);
        } catch (const std::exception& error) {
            LOG_WARN << "[GB28181][Collector] superseded owner cleanup failed: " << error.what();
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
        } catch (const std::exception& error) {
            LOG_WARN << "[GB28181][Collector] late owner cleanup failed: " << error.what();
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
        lease.deadlineMember = deadlineMember;
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

ruvia::task<void> CollectorRuntime::releaseOwner(std::string key, std::string token, std::string settledMember) {
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

    co_await GbControlService::releaseOwnerLease(redis_, key, token, settledMember);
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
        renewed = co_await GbControlService::renewOwnerLease(redis_, key, token, found->second.deadlineMember);
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
        auto event = projectionQueue_.pop();
        bool retry = false;
        bool succeeded = true;
        std::optional<ProjectionSequenceRange> sequences;
        try {
            if (const auto* device = std::get_if<DeviceProjection>(&event)) {
                sequences = device->sequences;
                succeeded = co_await publishDevice(*device);
            } else if (const auto* stream = std::get_if<StreamProjection>(&event)) {
                sequences = stream->sequences;
                succeeded = co_await publishStream(*stream);
            } else {
                const auto& barrier = std::get<ProjectionBarrier>(event);
                barrier.complete(!projectionQueue_.failedBetween(barrier.after, barrier.through));
            }
        } catch (const std::exception& error) {
            retry = true;
            if (!projectionScope_.stop_requested()) {
                LOG_WARN << "[GB28181] projection awaits durable storage: " << error.what();
            }
        }
        if (!retry && sequences && !succeeded) {
            projectionQueue_.recordFailure(*sequences);
        }
        projectionQueue_.pruneFailures(std::min(lastSipBarrierSequence_, activeControlProjection_.value_or(projectionQueue_.sequence())));
        if (retry) {
            projectionQueue_.retry(std::move(event));
            (void)co_await ruvia::sleep_for(worker_, std::chrono::milliseconds(250), projectionScope_.get_stop_token());
        }
    }
    projectionDrainRunning_ = false;
}

ruvia::task<void> CollectorRuntime::controlLoop() {
    requireCurrentLoop();
    const std::vector<std::string> streams{
        control_protocol::stream::control(index_, identity_.instanceId)
    };
    const auto group = control_protocol::stream::kControlGroup;
    const auto stop = scope_.get_stop_token();
    const auto redis = redis_.with_options({ .stop_token_ = stop });
    co_await GbControlService::ensureCollectorStream(redis, streams[0], group);
    co_await service::utils::redis::withConsumerNotifications(
        redis,
        identity_.channels,
        worker_,
        streams,
        group,
        stop,
        [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
            bool recovering = true;
            auto refreshAt = std::chrono::steady_clock::now() + kCollectorStreamRefresh;
            while (!stop.stop_requested()) {
                std::string failure;
                try {
                    const auto batches = co_await service::utils::redis::consumeGroup(
                        redis,
                        identity_.channels,
                        streams,
                        group,
                        ownerToken_,
                        recovering,
                        notifications,
                        stop,
                        std::max(std::chrono::milliseconds(0), std::chrono::ceil<std::chrono::milliseconds>(refreshAt - std::chrono::steady_clock::now())),
                        32
                    );
                    for (const auto& batch : batches) {
                        for (const auto& message : batch.messages) {
                            co_await handleControl(message);
                        }
                    }
                    if (std::chrono::steady_clock::now() >= refreshAt && !stop.stop_requested()) {
                        co_await GbControlService::refreshCollectorStream(redis, streams[0]);
                        refreshAt = std::chrono::steady_clock::now() + kCollectorStreamRefresh;
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        co_return;
                    }
                    failure = error.what();
                }
                if (!failure.empty()) {
                    LOG_WARN << "[GB28181] control consumer: " << failure;
                    recovering = true;
                    (void)co_await ruvia::sleep_for(worker_, std::chrono::milliseconds(250), stop);
                }
            }
        }
    );
}

ruvia::task<void> CollectorRuntime::handleControl(
    const service::message::StreamMessage& message
) {
    requireCurrentLoop();
    const auto requestId = std::string(message.get("request_id"));
    if (!service::common::isUuid(requestId)) {
        const std::string id = message.id;
        co_await service::utils::redis::acknowledgeAndDelete(redis_, identity_.channels, control_protocol::stream::control(index_, identity_.instanceId), control_protocol::stream::kControlGroup, id);
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
            redis,
            key,
            claimKey,
            cancelKey,
            token,
            deadlineText
        );
        if (claimed != ControlClaimResult::kClaimed) {
            fail(409, claimed == ControlClaimResult::kAlreadyClaimed ? "previous execution outcome is unknown; command will not be replayed" : "GB28181 command expired, cancelled or lost its owner");
        } else {
            claimOwner = token;
            const auto cancelled = co_await GbControlService::controlCancelled(redis, cancelKey);
            if (cancelled || scope_.stop_requested() || deadlineMs <= nowMs() || !ownsLocal(key, token)) {
                fail(408, "GB28181 command cancelled or ownership expired while claiming");
            } else {
                try {
                    const auto beforeProjection = projectionQueue_.sequence();
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
            co_await GbControlService::publishReplyAndAcknowledge(redis, identity_.channels, replyStream, control_protocol::stream::control(index_, identity_.instanceId), control_protocol::stream::kControlGroup, message.id, status, result, resultKey, claimKey, claimOwner);
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

ruvia::task<void> CollectorRuntime::retainPreview(const sip::SipPreviewStartResult& preview) {
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

    const auto& input = request;

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
        if (!sdk_) {
            throw std::runtime_error("ZLMediaKit SDK is unavailable");
        }
        media::ZlmSdk::OwnerScope owner(index_);
        if (operation == control_protocol::kRecordingOperation) {
            co_return control_protocol::jsonRecording(sdk_->isMp4Recording(streamId));
        }
        const auto changed = operation == control_protocol::kRecordingStartOperation
            ? sdk_->startMp4Recording(streamId)
            : sdk_->stopMp4Recording(streamId);
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
