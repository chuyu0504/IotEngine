#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/event_loop.h>
#include <ruvia/core/task.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/web/redis/redis.h>
#include <ruvia/web/redis/redis_client.h>

#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/features/collector/collector.service.h"
#include "service/features/collector/dlt645/dlt645.protocol.h"
#include "service/features/collector/engine/engine.runtime.h"
#include "service/features/collector/fins/fins.protocol.h"
#include "service/features/collector/mc/mc.protocol.h"
#include "service/features/collector/modbus/modbus.protocol.h"
#include "service/features/collector/mqtt/mqtt.protocol.h"
#include "service/features/collector/s7/s7.protocol.h"
#include "service/features/collector/sl651/sl651.protocol.h"
#include "service/features/collector/tcp/tcp.transport.h"
#include "service/middleware/log.h"
#include "service/utils/uuid.h"

namespace service::collector {

// 停机步骤失败只记录后继续后续步骤；只能在 catch 块内调用。
inline void logShutdownFailure(std::string_view step) noexcept {
    try {
        throw;
    } catch (const std::exception& error) {
        LOG_ERROR << "[Collector] shutdown_" << step << "_failed: " << error.what();
    } catch (...) {
        LOG_ERROR << "[Collector] shutdown_" << step << "_failed: unknown error";
    }
}

class CollectorWorker final {
  public:
    CollectorWorker(ruvia::event_loop loop, std::unique_ptr<ruvia::redis_client> redisClient, common::WorkerIdentity identity)
        : identity_(std::move(identity)), loop_(std::move(loop)), workerHandle_(loop_.handle()), resource_(),
          scope_(workerHandle_, ruvia::task_scope_options{ .resource_ = &resource_ }),
          scheduler_(loop_.io_context()),
          redisClient_(std::move(redisClient)),
          redisBlocking_(redisClient_->handle()),
          redis_(redisClient_->with_options({ .timeout_ = std::chrono::seconds(30) })),
          engine_(protocols()),
          tcp_(
              uuidState_,
              loop_.io_context(),
              scheduler_,
              identity_.index,
              identity_.count,
              [this](ProtocolConnectionInfo info) {
                  enqueueConnected(std::move(info));
              },
              [this](IngressPacket packet) {
                  enqueueIngress(std::move(packet));
              },
              [this](std::string connectionId, std::string reason) {
                  enqueueDisconnect(std::move(connectionId), std::move(reason));
              },
              [this](LinkState state) {
                  if (!stopping_) {
                      linkStates_.insert_or_assign(state.linkId, state);
                      scope_.spawn(publishLinkEvent(std::move(state)));
                  }
              },
              [this](std::string linkId, std::string targetId, std::string connectionId, std::function<void(bool)> completed) {
                  if (stopping_) {
                      completed(false);
                      return;
                  }
                  scope_.spawn(claimTarget(std::move(linkId), std::move(targetId), std::move(connectionId), std::move(completed)));
              },
              [this](std::string linkId, std::string targetId, std::string connectionId) {
                  releaseTargetLease(std::move(linkId), std::move(targetId), std::move(connectionId));
              }
          ),
          consumer_("collector-" + std::to_string(identity_.index)),
          commands_(redis_, uuidState_, identity_, collectorGroup(), consumer_, message::deadLetterStream(identity_.index, identity_.instanceId)),
          derived_(redis_, uuidState_, identity_),
          packets_(identity_.index, loadedSnapshot_, linkIndexes_, networkConnections_, routes_) {}

    CollectorWorker(const CollectorWorker&) = delete;
    CollectorWorker& operator=(const CollectorWorker&) = delete;

    [[nodiscard]] ruvia::task<void> initialize() {
        co_await utils::redis::ensureGroup(redis_, configStream(), configGroup());
        co_await utils::redis::ensureGroup(redis_, egressStream(), egressGroup());
        co_await utils::redis::ensureGroup(redis_, linkEventStream(), linkEventGroup());
        co_await utils::redis::ensureGroup(redis_, commandStream(true), commandGroup());
        co_await utils::redis::ensureGroup(redis_, commandStream(false), commandGroup());
        co_await utils::redis::ensureGroup(redis_, controlStream(), collectorGroup());
        loadedConfigVersion_ = co_await config::activeVersion(redis_);
        desiredConfigVersion_ = loadedConfigVersion_;
        desiredSnapshot_ = co_await config::load(redis_, desiredConfigVersion_);
        auto owned = co_await ownership::retain(redis_, desiredSnapshot_, identity_.instanceId, linkLeases_);
        nextOwnershipRefresh_ = owned.nextLeaseWait
            ? std::optional{ std::chrono::steady_clock::now() + *owned.nextLeaseWait }
            : std::nullopt;
        auto snapshot = std::move(owned.snapshot);
        co_await CollectorStateService::eraseLegacyRoutes(redis_, snapshot, identity_.instanceId);
        co_await CollectorStateService::eraseLegacyLinkSnapshots(redis_, snapshot, identity_.instanceId);
        engine_.reload(snapshot);
        tcp_.reload(snapshot);
        replaceLoadedSnapshot(std::move(snapshot));
        co_await CollectorStateService::publishWorker(redis_, identity_.index, workerInstanceId_, loadedConfigVersion_, identity_.instanceId);
        nextStateRefresh_ = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        scheduleTick(std::chrono::milliseconds(0));
    }

    // 停止本组件业务；装配方在所有共用本 Worker Redis 的业务停止后调用 shutdownRedis()。
    [[nodiscard]] ruvia::task<void> shutdown() {
        if (shutdownStarted_) {
            co_return;
        }
        shutdownStarted_ = true;
        stopping_ = true;
        if (tickToken_ != 0) {
            scheduler_.cancel(tickToken_);
        }
        if (targetRenewalToken_ != 0) {
            scheduler_.cancel(targetRenewalToken_);
        }
        for (const auto& [connectionId, deadline] : targetLeaseDeadlines_) {
            scheduler_.cancel(deadline.token);
        }
        targetLeaseDeadlines_.clear();
        try {
            tcp_.stop();
        } catch (...) {
            logShutdownFailure("transport");
        }
        scope_.request_stop();
        try {
            co_await scope_.join();
        } catch (...) {
            logShutdownFailure("task_scope");
        }
        for (const auto& [connectionId, deviceCodes] : routes_) {
            for (const auto& deviceCode : deviceCodes) {
                try {
                    co_await eraseRoute(deviceCode, connectionId, "local_closed");
                } catch (...) {
                    logShutdownFailure("route_cleanup");
                }
            }
        }
        routes_.clear();
        routeEpochs_.clear();
        const auto leases = targetLeases_;
        targetLeases_.clear();
        for (const auto& [target, connectionId] : leases) {
            try {
                co_await ownership::releaseTarget(redis_, target.first, target.second, targetLeaseOwner(connectionId));
            } catch (...) {
                logShutdownFailure("target_release");
            }
        }
        try {
            co_await CollectorStateService::eraseWorker(redis_, identity_.index, workerInstanceId_, identity_.instanceId);
        } catch (...) {
            logShutdownFailure("snapshot_cleanup");
        }
        try {
            scheduler_.stop();
        } catch (...) {
            logShutdownFailure("scheduler");
        }
    }

    [[nodiscard]] std::size_t index() const noexcept { return identity_.index; }

    [[nodiscard]] ruvia::event_loop eventLoop() const { return loop_; }

    [[nodiscard]] ruvia::redis_handle redisHandle() const { return redisClient_->with_options({}); }

    [[nodiscard]] ruvia::task<void> shutdownRedis() {
        co_await redisClient_->shutdown();
    }

  private:
    struct TargetLeaseDeadline {
        DeadlineScheduler::Token token;
        std::chrono::steady_clock::time_point expiresAt;
    };

    std::map<std::string, EgressLogContext> debugPendingSends_;

    struct ConnectionEvent {
        bool connected = false;
        ProtocolConnectionInfo info;
        std::string reason;
    };

    // 传输层回调按到达顺序入队，由同一个排空任务逐项交给协议引擎，保证同一连接的字节与连接事件有序。
    struct IngressWork {
        std::optional<IngressPacket> packet;
        std::optional<ConnectionEvent> connectionEvent;
    };

    static ProtocolSessionFactoryRegistry protocols() {
        ProtocolSessionFactoryRegistry result;
        result.add(std::make_unique<modbus::SessionFactory>());
        result.add(std::make_unique<s7::SessionFactory>());
        result.add(std::make_unique<mc::SessionFactory>());
        result.add(std::make_unique<fins::SessionFactory>());
        result.add(std::make_unique<dlt645::SessionFactory>());
        result.add(std::make_unique<mqtt::Factory>());
        result.add(std::make_unique<sl651::SessionFactory>());
        return result;
    }

    [[nodiscard]] std::string collectorGroup() const { return "iot-engine:collector"; }

    [[nodiscard]] std::string configGroup() const { return collectorGroup(); }

    [[nodiscard]] std::string egressGroup() const { return collectorGroup(); }

    [[nodiscard]] std::string linkEventGroup() const { return collectorGroup(); }

    [[nodiscard]] std::string configStream() const { return message::configStream(identity_.index, identity_.instanceId); }

    [[nodiscard]] std::string commandStream(bool high) const {
        return message::commandStream(identity_.index, high, identity_.instanceId);
    }

    [[nodiscard]] std::string commandGroup() const { return collectorGroup(); }

    [[nodiscard]] std::string controlStream() const { return message::controlStream(identity_.index, identity_.instanceId); }

    [[nodiscard]] std::string egressStream() const { return message::egressStream(identity_.index, identity_.instanceId); }

    [[nodiscard]] std::string linkEventStream() const {
        return message::linkEventStream(identity_.index, identity_.instanceId);
    }

    [[nodiscard]] const DeviceDefinition* findDevice(std::string_view deviceId) const noexcept {
        const auto current = deviceIndexes_.find(deviceId);
        return current == deviceIndexes_.end() ? nullptr : &loadedSnapshot_.devices[current->second];
    }

    [[nodiscard]] std::string remoteAddress(std::string_view connectionId) const {
        const auto network = networkConnections_.find(connectionId);
        return network == networkConnections_.end() ? std::string{} : network->second.remoteAddress;
    }

    // 快照替换时建立 id→下标索引与调试链路集合，并让派生服务按新快照更新已解析配置。
    void replaceLoadedSnapshot(RuntimeSnapshot snapshot) {
        loadedSignature_ = config::signature(snapshot);
        loadedSnapshot_ = std::move(snapshot);
        linkIndexes_.clear();
        deviceIndexes_.clear();
        debugLinks_.clear();
        for (std::size_t index = 0; index < loadedSnapshot_.links.size(); ++index) {
            const auto& link = loadedSnapshot_.links[index];
            linkIndexes_.insert_or_assign(link.id, index);
            if (link.debugEnabled) {
                debugLinks_.insert(link.id);
            }
        }
        for (std::size_t index = 0; index < loadedSnapshot_.devices.size(); ++index) {
            const auto& device = loadedSnapshot_.devices[index];
            deviceIndexes_.insert_or_assign(device.id, index);
            if (device.debugEnabled) {
                debugLinks_.insert(device.linkId);
            }
        }
        derived_.reload(loadedSnapshot_);
    }

    [[nodiscard]] const message::ProtocolTask* taskForCausation(std::string_view causationId) const noexcept {
        return commands_.taskForCausation(causationId);
    }

    [[nodiscard]] std::string targetLeaseOwner(std::string_view connectionId) const {
        return identity_.instanceId + ":worker:" + std::to_string(identity_.index) + ":" + std::string(connectionId);
    }

    ruvia::task<void> claimTarget(std::string linkId, std::string targetId, std::string connectionId, std::function<void(bool)> completed) {
        bool claimed = false;
        const auto began = std::chrono::steady_clock::now();
        try {
            claimed = co_await ownership::retainTarget(redis_.with_options({ .timeout_ = std::chrono::seconds(3) }), linkId, targetId, targetLeaseOwner(connectionId));
            if (claimed && !stopping_ && std::chrono::steady_clock::now() - began < kTargetLeaseValidity &&
                !targetLeases_.contains({ linkId, targetId })) {
                targetLeases_.insert_or_assign({ linkId, targetId }, connectionId);
                armTargetLeaseDeadline({ linkId, targetId }, connectionId, began);
                scheduleTargetRenewal();
            } else if (claimed) {
                claimed = false;
                co_await ownership::releaseTarget(redis_, linkId, targetId, targetLeaseOwner(connectionId));
            }
        } catch (const std::exception& error) {
            LOG_WARN << "[Collector] target_claim_failed: " << error.what();
        } catch (...) {
            LOG_WARN << "[Collector] target_claim_failed: unknown error";
        }
        if (stopping_ && claimed) {
            const auto current = targetLeases_.find({ linkId, targetId });
            if (current != targetLeases_.end() && current->second == connectionId) {
                targetLeases_.erase(current);
            }
            try {
                co_await ownership::releaseTarget(redis_, linkId, targetId, targetLeaseOwner(connectionId));
            } catch (const std::exception& error) {
                LOG_WARN << "[Collector] target_release_failed: " << error.what();
            } catch (...) {
                LOG_WARN << "[Collector] target_release_failed: unknown error";
            }
            claimed = false;
        }
        if (completed) {
            completed(claimed && !stopping_);
        }
    }

    void releaseTargetLease(std::string linkId, std::string targetId, std::string connectionId) {
        const auto current = targetLeases_.find({ linkId, targetId });
        if (current == targetLeases_.end() || current->second != connectionId || stopping_) {
            return;
        }
        targetLeases_.erase(current);
        scheduleTargetRenewal();
        if (const auto deadline = targetLeaseDeadlines_.find(connectionId); deadline != targetLeaseDeadlines_.end()) {
            scheduler_.cancel(deadline->second.token);
            targetLeaseDeadlines_.erase(deadline);
        }
        scope_.spawn(releaseTargetLeaseAsync(std::move(linkId), std::move(targetId), std::move(connectionId)));
    }

    ruvia::task<void> releaseTargetLeaseAsync(std::string linkId, std::string targetId, std::string connectionId) {
        try {
            co_await ownership::releaseTarget(redis_, linkId, targetId, targetLeaseOwner(connectionId));
        } catch (const std::exception& error) {
            LOG_WARN << "[Collector] target_release_failed: " << error.what();
        } catch (...) {
            LOG_WARN << "[Collector] target_release_failed: unknown error";
        }
    }

    ruvia::task<void> renewTargetLease(ClientTargetKey target, std::string connectionId) {
        bool retained = false;
        const auto began = std::chrono::steady_clock::now();
        try {
            retained = co_await ownership::renewTarget(redis_.with_options({ .timeout_ = std::chrono::seconds(3) }), target.first, target.second, targetLeaseOwner(connectionId));
        } catch (const std::exception& error) {
            LOG_WARN << "[Collector] target_renew_failed: " << error.what();
        } catch (...) {
            LOG_WARN << "[Collector] target_renew_failed: unknown error";
        }
        if (stopping_) {
            co_return;
        }
        const auto current = targetLeases_.find(target);
        if (current != targetLeases_.end() && current->second == connectionId) {
            const auto deadline = targetLeaseDeadlines_.find(connectionId);
            const auto now = std::chrono::steady_clock::now();
            if (retained && now - began < kTargetLeaseValidity &&
                deadline != targetLeaseDeadlines_.end() && now < deadline->second.expiresAt) {
                armTargetLeaseDeadline(target, connectionId, began);
                co_return;
            }
            targetLeases_.erase(current);
            scheduleTargetRenewal();
            tcp_.revokeTarget(connectionId, "target_ownership_lost");
        }
    }

    void scheduleTargetRenewal() {
        if (stopping_ || targetLeases_.empty()) {
            if (targetRenewalToken_ != 0) {
                scheduler_.cancel(targetRenewalToken_);
                targetRenewalToken_ = 0;
            }
            return;
        }
        if (targetRenewalToken_ != 0) {
            return;
        }
        targetRenewalToken_ = scheduler_.scheduleAfter(std::chrono::seconds(5), [this] {
            targetRenewalToken_ = 0;
            if (!stopping_) {
                for (const auto& [target, connectionId] : targetLeases_) {
                    scope_.spawn(renewTargetLease(target, connectionId));
                }
                scheduleTargetRenewal();
            }
        });
    }

    void armTargetLeaseDeadline(const ClientTargetKey& target, const std::string& connectionId, std::chrono::steady_clock::time_point began) {
        if (const auto old = targetLeaseDeadlines_.find(connectionId); old != targetLeaseDeadlines_.end()) {
            scheduler_.cancel(old->second.token);
        }
        const auto delay = std::max(std::chrono::milliseconds(1), std::chrono::duration_cast<std::chrono::milliseconds>(began + kTargetLeaseValidity - std::chrono::steady_clock::now()));
        const auto token = scheduler_.scheduleAfter(delay, [this, target, connectionId] {
            targetLeaseDeadlines_.erase(connectionId);
            const auto current = targetLeases_.find(target);
            if (current != targetLeases_.end() && current->second == connectionId) {
                targetLeases_.erase(current);
                scheduleTargetRenewal();
                tcp_.revokeTarget(connectionId, "target_ownership_expired");
            }
        });
        targetLeaseDeadlines_.insert_or_assign(connectionId, TargetLeaseDeadline{ token, began + kTargetLeaseValidity });
    }

    void scheduleTick(std::chrono::milliseconds delay = std::chrono::milliseconds(0)) {
        if (stopping_) {
            return;
        }
        tickToken_ = scheduler_.scheduleAfter(delay, [this] {
            tickToken_ = 0;
            if (!stopping_) {
                scope_.spawn(tick());
            }
        });
    }

    void recoverClaimedMessages(std::string error) {
        LOG_WARN << "[Collector] " << error;
        // A successful XREADGROUP can assign messages to this consumer before a broken
        // connection prevents the reply from reaching us. Every local stream must therefore
        // drain its own PEL after either a blocking or non-blocking read fails.
        configRecovering_ = true;
        needsConfigSnapshot_ = true;
        highRecovering_ = true;
        normalRecovering_ = true;
        controlRecovering_ = true;
        egressRecovering_ = true;
        linkEventRecovering_ = true;
        scheduleTick(kFailureDelay);
    }

    ruvia::task<void> tick() {
        try {
            bool handled = co_await consumeConfig();
            handled = co_await consumeControl() || handled;
            handled = co_await consumeLinkEvent() || handled;
            handled = co_await consumeCommand(true) || handled;
            handled = co_await consumeCommand(false) || handled;
            handled = co_await consumeEgress() || handled;
            if (handled) {
                scheduleTick();
            } else {
                scope_.spawn(waitForWork());
            }
        } catch (const std::exception& error) {
            // XREADGROUP assigns a whole batch to this worker before the individual messages
            // are processed. If one item fails, the remaining items are already in the PEL and
            // a subsequent `>` read cannot see them. Re-enter recovery for every local stream;
            // each stream is partitioned by worker, so this only replays messages already owned
            // by this worker and does not introduce cross-worker claiming or polling.
            recoverClaimedMessages(error.what());
        } catch (...) {
            recoverClaimedMessages("unknown_collector_exception");
        }
    }

    ruvia::task<bool> consumeConfig() {
        const auto stream = configStream();
        const auto group = configGroup();
        const auto messages = co_await readAvailable(stream, group, configRecovering_, 16);
        const auto now = std::chrono::steady_clock::now();
        const bool changed = (!messages.empty() || needsConfigSnapshot_ ||
                              (nextOwnershipRefresh_ && now >= *nextOwnershipRefresh_))
            ? co_await refreshConfigState(!messages.empty() || needsConfigSnapshot_)
            : false;
        if (now >= nextStateRefresh_) {
            co_await renewWorkerState();
        }
        for (const auto& message : messages) {
            co_await utils::redis::acknowledgeAndDelete(redis_, stream, group, message.id);
        }
        co_return changed || !messages.empty();
    }

    ruvia::task<bool> refreshConfigState(bool readSnapshot) {
        const auto version = readSnapshot ? co_await config::activeVersion(redis_) : desiredConfigVersion_;
        if (version != desiredConfigVersion_) {
            auto desired = co_await config::load(redis_, version);
            desiredSnapshot_ = std::move(desired);
            desiredConfigVersion_ = version;
        }
        if (readSnapshot) {
            linkLeases_.clear();
        }
        auto owned = co_await ownership::retain(redis_, desiredSnapshot_, identity_.instanceId, linkLeases_);
        nextOwnershipRefresh_ = owned.nextLeaseWait
            ? std::optional{ std::chrono::steady_clock::now() + *owned.nextLeaseWait }
            : std::nullopt;
        if (!readSnapshot && !owned.ownershipChanged) {
            co_return false;
        }
        auto snapshot = std::move(owned.snapshot);
        bool changed = false;
        if (version != loadedConfigVersion_ || config::signature(snapshot) != loadedSignature_) {
            co_await CollectorStateService::eraseLegacyRoutes(redis_, snapshot, identity_.instanceId);
            co_await CollectorStateService::eraseLegacyLinkSnapshots(redis_, snapshot, identity_.instanceId);
            const auto plan = engine_.planReconcile(loadedSnapshot_, snapshot);
            tcp_.reconcile(snapshot, plan);
            engine_.reloadRetainingSessions(snapshot);
            std::set<std::string_view> nextLinks;
            for (const auto& link : snapshot.links) {
                nextLinks.insert(link.id);
            }
            std::vector<std::string> removedLinks;
            for (const auto& link : loadedSnapshot_.links) {
                if (!nextLinks.contains(link.id)) {
                    removedLinks.push_back(link.id);
                }
            }
            nextLinks.clear();
            // 引擎已同步切换到新快照；在任何 await 之前同步替换 Worker 快照，
            // 让同时运行的入站排空任务与引擎看到同一代配置。
            replaceLoadedSnapshot(std::move(snapshot));
            for (const auto& linkId : removedLinks) {
                linkStates_.erase(linkId);
                co_await CollectorStateService::eraseLink(redis_, linkId, identity_.index, workerInstanceId_, identity_.instanceId);
            }
            auto sessionRefreshes =
                engine_.refreshClientSessions(plan.refreshClientSessions);
            std::erase_if(sessionRefreshes, [this](const auto& refresh) {
                const auto sessionEpoch = tcp_.advanceSessionEpoch(refresh.connectionId);
                if (sessionEpoch == 0) {
                    return true;
                }
                connectionEpochs_[refresh.connectionId] = sessionEpoch;
                if (const auto network = networkConnections_.find(refresh.connectionId);
                    network != networkConnections_.end()) {
                    network->second.sessionEpoch = sessionEpoch;
                }
                cancelProtocolDeadlinesForConnection(refresh.connectionId);
                return false;
            });
            for (auto& refresh : sessionRefreshes) {
                co_await applySessionRefresh(std::move(refresh));
            }
            loadedConfigVersion_ = desiredConfigVersion_;
            co_await CollectorStateService::publishWorker(redis_, identity_.index, workerInstanceId_, loadedConfigVersion_, identity_.instanceId);
            changed = true;
        }
        needsConfigSnapshot_ = false;
        co_return changed;
    }

    ruvia::task<void> renewWorkerState() {
        nextStateRefresh_ = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        co_await renewConnectionState();
        co_await CollectorStateService::renewWorkerSnapshot(redis_, identity_.index, workerInstanceId_, loadedConfigVersion_, identity_.instanceId);
        for (const auto& link : loadedSnapshot_.links) {
            const auto state = linkStates_.find(link.id);
            if (state == linkStates_.end()) {
                continue;
            }
            const message::StreamMessage snapshot{ {}, linkStateFields(state->second) };
            co_await CollectorStateService::retainLinkSnapshot(redis_, snapshot, identity_.index, identity_.instanceId);
        }
    }

    ruvia::task<bool> consumeCommand(bool high) {
        const auto stream = commandStream(high);
        auto& recovering = high ? highRecovering_ : normalRecovering_;
        const auto messages =
            co_await readAvailable(stream, commandGroup(), recovering, kCommandConsumeBatch);
        if (messages.empty()) {
            co_return false;
        }
        for (const auto& message : messages) {
            message::ProtocolTask task;
            std::string parseError;
            try {
                task = message::protocolTaskFrom(message);
            } catch (const std::exception& error) {
                parseError = error.what();
            }
            if (!parseError.empty()) {
                co_await CollectorCommandService::rejectMessage(redis_, identity_.index, identity_.instanceId, consumer_, message, "protocol_task_invalid", parseError, stream, commandGroup());
                continue;
            }
            const auto makeCommand = [&](std::string id) {
                return ProtocolCommand{
                    .id = std::move(id),
                    .deviceId = task.deviceId,
                    .deviceCode = task.deviceCode,
                    .transport = task.transport,
                    .kind = task.kind,
                    .protocol = task.protocol,
                    .payload = message::fromHex(task.payload),
                    .readbackPayload = message::fromHex(task.readbackPayload),
                    .expectedReadbackData = message::fromHex(task.expectedReadbackData),
                    .expectedValue = task.expectedValue,
                    .elements =
                        [&task] {
                            std::vector<CommandElementValue> elements;
                            elements.reserve(task.elements.size());
                            for (const auto& [elementId, value] : task.elements) {
                                elements.push_back({ elementId, value });
                            }
                            return elements;
                        }(),
                    .mqttMessageId = task.mqttMessageId,
                    .highPriority = high,
                    .expectsResponse = task.expectsResponse,
                    .timeout = std::chrono::milliseconds(
                        std::clamp<std::int64_t>(task.responseTimeoutMs, 100, 60000)
                    )
                };
            };
            packets_.discoveryRequested(task);
            if (task.kind == "discovery" && task.connectionId.empty()) {
                const auto connections = tcp_.connectionIds(task.linkId);
                if (connections.empty()) {
                    packets_.discoveryUnavailable(task);
                    co_await failUndeliverable(stream, message.id, task, "discovery_no_connections");
                    continue;
                }
                commands_.startBroadcast(stream, message.id, task, connections.size());
                packets_.broadcastStarted(task, connections.size());
                for (const auto& connectionId : connections) {
                    const auto childId = service::utils::nextUuid(uuidState_);
                    commands_.addBroadcastTarget(childId, task.messageId);
                    packets_.broadcastTarget(task, connectionId, childId);
                    co_await applyActions(connectionId, engine_.execute(connectionId, makeCommand(childId)));
                }
                continue;
            }
            if (task.connectionId.empty()) {
                co_await failUndeliverable(stream, message.id, task, "connection_route_missing");
                continue;
            }
            const auto epoch = connectionEpochs_.find(task.connectionId);
            if (task.sessionEpoch != 0 &&
                (epoch == connectionEpochs_.end() || epoch->second != task.sessionEpoch)) {
                co_await failUndeliverable(stream, message.id, task, "stale_session_epoch");
                continue;
            }
            if (task.kind == "command") {
                const auto reservation = co_await CollectorCommandService::reserveTransmission(
                    redis_,
                    task,
                    message::utcNowMilliseconds()
                );
                if (reservation == TransmissionReservation::Expired) {
                    co_await failUndeliverable(stream, message.id, task, "dispatch_deadline_expired");
                    continue;
                }
                if (reservation == TransmissionReservation::Duplicate) {
                    co_await utils::redis::acknowledgeAndDelete(redis_, stream, commandGroup(), message.id);
                    continue;
                }
            }
            commands_.track(stream, message.id, task);
            auto command = makeCommand(task.messageId);
            packets_.discoveryStarted(task);
            co_await applyActions(task.connectionId, engine_.execute(task.connectionId, std::move(command)));
        }
        co_return true;
    }

    ruvia::task<bool> consumeControl() {
        const auto stream = controlStream();
        const auto messages =
            co_await readAvailable(stream, collectorGroup(), controlRecovering_, 32);
        for (const auto& message : messages) {
            const auto connectionId = message.get("connection_id");
            if (!connectionId.empty()) {
                tcp_.close(connectionId, message.get("reason").empty() ? "device_re_registered" : std::string(message.get("reason")));
            }
            co_await utils::redis::acknowledgeAndDelete(redis_, stream, collectorGroup(), message.id);
        }
        co_return !messages.empty();
    }

    ruvia::task<void> captureDebug(std::string_view linkId, std::string_view connectionId, std::string_view address, std::string_view direction, std::span<const std::uint8_t> bytes, std::int64_t timestamp, std::string_view knownDevice = {}, bool deviceOnly = false, std::string_view eventId = {}, std::string_view status = {}, std::string_view reason = {}, std::string_view parsedJson = {}, std::string_view replyToPacketId = {}, std::string_view acquisitionId = {}) {
        // 只有链路或其设备开启调试时才做设备归属与存储；其余报文不进入调试路径。
        if (!debugLinks_.contains(linkId)) {
            co_return;
        }
        const auto* link = packets_.findLink(linkId);
        if (!link) {
            co_return;
        }
        const auto connection = networkConnections_.find(connectionId);
        const std::string peerAddress = !address.empty() ? std::string(address) : connection == networkConnections_.end() ? std::string{}
                                                                                                                          : connection->second.remoteAddress;
        const auto routes = routes_.find(connectionId);
        const auto* selected = engine_.identifyDebugDevice(loadedSnapshot_, { .linkId = linkId, .targetId = connection == networkConnections_.end() ? std::string_view{} : connection->second.targetId, .direction = direction, .knownDevice = knownDevice, .deviceOnly = deviceOnly, .bytes = bytes, .boundDevices = routes == routes_.end() ? nullptr : &routes->second });
        co_await CollectorPacketService::recordDebugPacket(redis_, uuidState_, *link, selected, direction, peerAddress, bytes, timestamp, deviceOnly, eventId, status, reason, parsedJson, replyToPacketId, acquisitionId);
    }

    void enqueueIngress(IngressPacket packet) {
        if (stopping_) {
            return;
        }
        packets_.ingressReceived(packet);
        if (ingressWork_.size() >= kIngressCapacity) {
            packets_.ingressDropped(packet);
            tcp_.close(packet.connectionId, "raw_ingress_backpressure");
            return;
        }
        ingressWork_.push_back({ .packet = std::move(packet), .connectionEvent = std::nullopt });
        startIngressDrain();
    }

    void enqueueConnected(ProtocolConnectionInfo info) {
        if (stopping_) {
            return;
        }
        networkConnections_.insert_or_assign(info.connectionId, info);
        packets_.connectionOpened(info);
        ingressWork_.push_back({ .packet = std::nullopt, .connectionEvent = ConnectionEvent{ .connected = true, .info = std::move(info), .reason = {} } });
        startIngressDrain();
    }

    void enqueueDisconnect(std::string connectionId, std::string reason) {
        if (stopping_) {
            return;
        }
        const auto current = networkConnections_.find(connectionId);
        if (current == networkConnections_.end()) {
            return;
        }
        auto info = std::move(current->second);
        networkConnections_.erase(current);
        if (!info.targetId.empty()) {
            releaseTargetLease(info.linkId, info.targetId, connectionId);
        }
        packets_.connectionClosed(info, reason);
        ingressWork_.push_back({ .packet = std::nullopt, .connectionEvent = ConnectionEvent{ .connected = false, .info = std::move(info), .reason = std::move(reason) } });
        startIngressDrain();
    }

    void startIngressDrain() {
        if (ingressDraining_ || stopping_) {
            return;
        }
        ingressDraining_ = true;
        scope_.spawn(drainIngress());
    }

    // 单一排空任务按到达顺序逐项处理，保证同一连接的字节与连接事件顺序；处理失败只影响该连接。
    ruvia::task<void> drainIngress() {
        while (!stopping_ && !ingressWork_.empty()) {
            auto work = std::move(ingressWork_.front());
            ingressWork_.pop_front();
            if (work.packet) {
                co_await consumeIngress(std::move(*work.packet));
            } else {
                co_await consumeConnectionEvent(std::move(*work.connectionEvent));
            }
        }
        ingressDraining_ = false;
    }

    ruvia::task<void> consumeConnectionEvent(ConnectionEvent event) {
        const auto& connectionId = event.info.connectionId;
        try {
            const auto current = connectionEpochs_.find(connectionId);
            if (event.connected) {
                if (current != connectionEpochs_.end() && current->second >= event.info.sessionEpoch) {
                    packets_.staleConnection(event.info);
                    co_return;
                }
                connectionEpochs_[connectionId] = event.info.sessionEpoch;
                co_await applyActions(connectionId, engine_.connected(event.info));
            } else if (current != connectionEpochs_.end() && current->second == event.info.sessionEpoch) {
                co_await applyActions(connectionId, engine_.disconnected(connectionId, event.reason), true, event.reason);
            }
        } catch (const std::exception& error) {
            LOG_ERROR << "[Collector] connection_event_failed: connection_id=" << connectionId << " " << error.what();
            if (event.connected) {
                tcp_.close(connectionId, "connection_event_failed");
            }
        } catch (...) {
            LOG_ERROR << "[Collector] connection_event_failed: connection_id=" << connectionId << " unknown error";
            if (event.connected) {
                tcp_.close(connectionId, "connection_event_failed");
            }
        }
    }

    ruvia::task<void> consumeIngress(IngressPacket packet) {
        const auto epoch = connectionEpochs_.find(packet.connectionId);
        if (epoch == connectionEpochs_.end() || epoch->second != packet.sessionEpoch ||
            !engine_.contains(packet.connectionId)) {
            packets_.ingressRejected(packet);
            co_return;
        }
        std::string failure;
        try {
            auto actions = engine_.consume(packet);
            const auto pendingDebug = debugPendingSends_.find(packet.connectionId);
            const auto inputAcquisition = pendingDebug == debugPendingSends_.end()
                ? service::utils::nextUuid(uuidState_)
                : pendingDebug->second.acquisitionId;
            for (auto& action : actions) {
                if (action.kind == ProtocolActionKind::ObserveParsed || action.kind == ProtocolActionKind::PublishParsed) {
                    if (action.parsed.acquisitionId.empty()) {
                        action.parsed.acquisitionId = inputAcquisition;
                    }
                    if (pendingDebug != debugPendingSends_.end() && action.parsed.acquisitionId == pendingDebug->second.acquisitionId) {
                        action.replyToPacketId = pendingDebug->second.messageId;
                    }
                }
            }
            // 完整解析帧在 PublishParsed 中记录；同一网络读取不能再记录一遍。
            // 非完整帧（包括分片、握手和解析失败）仍保留传输层原文。
            if (debugLinks_.contains(packet.linkId)) {
                const bool completeFrame = std::any_of(actions.begin(), actions.end(), [&](const auto& action) {
                    return (action.kind == ProtocolActionKind::PublishParsed ||
                            action.kind == ProtocolActionKind::ObserveParsed) &&
                        std::find(action.parsed.rawPayloads.begin(), action.parsed.rawPayloads.end(), packet.payload) != action.parsed.rawPayloads.end();
                });
                if (!completeFrame) {
                    co_await captureDebug(packet.linkId, packet.connectionId, packet.remoteAddress, "RX", packet.payload, packet.occurredAtMs, {}, false, packet.messageId, "received", {}, {}, {}, inputAcquisition);
                }
            }
            co_await applyActions(packet.connectionId, std::move(actions));
            co_return;
        } catch (const std::exception& error) {
            failure = error.what();
        } catch (...) {
            failure = "unknown_protocol_exception";
        }
        // 协议会话已消费这些字节，不重放；记录后保持连接，需要确认的协议在遥测发布成功前不会确认，
        // 设备会在同一连接上重发（如 SL651 重传上报）。
        LOG_ERROR << "[Collector] ingress_processing_failed: connection_id=" << packet.connectionId << " " << failure;
        packets_.ingressFailed(packet, failure);
    }

    ruvia::task<bool> consumeEgress() {
        if (egressWritePending_) {
            co_return false;
        }
        const auto stream = egressStream();
        const auto group = egressGroup();
        const auto messages =
            co_await readAvailable(stream, group, egressRecovering_, 1);
        if (messages.empty()) {
            co_return false;
        }
        const auto& message = messages.front();
        message::EgressPacket packet;
        std::string parseError;
        try {
            packet = message::egressFrom(message);
        } catch (const std::exception& error) {
            parseError = error.what();
        }
        if (!parseError.empty()) {
            packets_.egressInvalid(message, parseError);
            co_await CollectorCommandService::rejectMessage(redis_, identity_.index, identity_.instanceId, consumer_, message, "socket_egress_invalid", parseError, stream, group);
            co_return true;
        }
        if (packet.workerInstanceId != workerInstanceId_) {
            packets_.egressRejected(packet, "stale_worker_instance");
            co_await CollectorCommandService::rejectMessage(redis_, identity_.index, identity_.instanceId, consumer_, message, "stale_worker_instance", {}, stream, group);
            co_return true;
        }
        const auto connection = networkConnections_.find(packet.connectionId);
        const auto linkId = connection == networkConnections_.end()
            ? std::string_view{}
            : std::string_view(connection->second.linkId);
        if (!co_await ownership::isLinkOwner(redis_, linkId, identity_.instanceId)) {
            tcp_.close(packet.connectionId, "link_ownership_lost");
            co_await utils::redis::acknowledgeAndDelete(redis_, stream, group, message.id);
            co_return true;
        }
        const auto epoch = connectionEpochs_.find(packet.connectionId);
        if (epoch == connectionEpochs_.end() || epoch->second != packet.sessionEpoch) {
            packets_.egressRejected(packet, "stale_session_epoch");
            co_await CollectorCommandService::rejectMessage(redis_, identity_.index, identity_.instanceId, consumer_, message, "stale_session_epoch", {}, stream, group);
            co_return true;
        }
        egressWritePending_ = true;
        const auto entryId = message.id;
        EgressLogContext egressLog;
        egressLog.acquisitionId = packet.acquisitionId;
        egressLog.replyToPacketId = packet.replyToPacketId;
        egressLog.connectionId = packet.connectionId;
        egressLog.messageId = packet.messageId;
        egressLog.causationId = packet.causationId;
        egressLog.sessionEpoch = packet.sessionEpoch;
        if (const auto* task = taskForCausation(packet.causationId)) {
            egressLog.operation = task->kind;
            egressLog.protocol = task->protocol;
            egressLog.linkId = task->linkId;
            egressLog.deviceId = task->deviceId;
            egressLog.deviceCode = task->deviceCode;
        } else {
            egressLog.operation = "protocol";
            if (packets_.captureBytes()) {
                egressLog.deviceCode = packets_.deviceCodesForConnection(packet.connectionId);
            }
        }
        const auto network = networkConnections_.find(packet.connectionId);
        if (network != networkConnections_.end()) {
            if (egressLog.linkId.empty()) {
                egressLog.linkId = network->second.linkId;
            }
            if (egressLog.protocol.empty()) {
                egressLog.protocol = packets_.protocolForLink(network->second.linkId);
            }
            egressLog.remoteAddress = network->second.remoteAddress;
        }
        const bool logBytes = packets_.captureBytes();
        if (logBytes || debugLinks_.contains(egressLog.linkId)) {
            egressLog.debugPayload = engine_.packetForLogging(egressLog.protocol, packet.payload);
        }
        if (logBytes) {
            packets_.sending(egressLog);
        }
        egressLog.debugTime = message::utcNowMilliseconds();
        egressLog.awaitResponse = engine_.expectsResponse(egressLog.protocol, egressLog.causationId);
        if (egressLog.awaitResponse) {
            debugPendingSends_[packet.connectionId] = egressLog;
        }
        co_await captureDebug(egressLog.linkId, packet.connectionId, egressLog.remoteAddress, "TX", egressLog.debugPayload, egressLog.debugTime, egressLog.deviceId, false, egressLog.messageId, "sending", {}, {}, egressLog.replyToPacketId, egressLog.acquisitionId);
        tcp_.send(packet.connectionId, std::move(packet.payload), [this, entryId, egressLog = std::move(egressLog)](bool success) mutable {
            if (!stopping_) {
                scope_.spawn(
                    completeEgress(entryId, success, std::move(egressLog))
                );
            }
        });
        co_return true;
    }

    ruvia::task<void> finishDebugResponse(std::string_view connectionId, bool success, std::string_view reason = {}, std::string_view commandId = {}) {
        const auto found = debugPendingSends_.find(std::string(connectionId));
        if (found == debugPendingSends_.end() ||
            (!commandId.empty() && found->second.causationId != commandId)) {
            co_return;
        }
        auto context = std::move(found->second);
        debugPendingSends_.erase(found);
        co_await captureDebug(context.linkId, context.connectionId, context.remoteAddress, "TX", context.debugPayload, context.debugTime, context.deviceId, false, context.messageId, success ? "success" : "failed", reason, {}, context.replyToPacketId, context.acquisitionId);
    }

    ruvia::task<void> completeEgress(std::string entryId, bool success, EgressLogContext egressLog) {
        const auto pending = debugPendingSends_.find(egressLog.connectionId);
        // A response may arrive before the send callback; never overwrite its final status.
        if (!egressLog.awaitResponse || (pending != debugPendingSends_.end() && pending->second.messageId == egressLog.messageId)) {
            co_await captureDebug(egressLog.linkId, egressLog.connectionId, egressLog.remoteAddress, "TX", egressLog.debugPayload, egressLog.debugTime, egressLog.deviceId, false, egressLog.messageId, success ? (egressLog.awaitResponse ? "waiting" : "sent") : "failed", success ? "" : "socket_write_failed", {}, egressLog.replyToPacketId, egressLog.acquisitionId);
            if (!success) {
                debugPendingSends_.erase(egressLog.connectionId);
            }
        }

        if (!success && egressLog.deviceId.empty() && egressLog.deviceCode.empty()) {
            egressLog.deviceCode = packets_.deviceCodesForConnection(egressLog.connectionId);
        }
        packets_.sent(egressLog, success);
        try {
            const auto stream = egressStream();
            const auto group = egressGroup();
            if (!success) {
                message::StreamMessage message;
                message.id = entryId;
                co_await CollectorCommandService::rejectMessage(redis_, identity_.index, identity_.instanceId, consumer_, message, "socket_write_failed", {}, stream, group);
            } else {
                co_await utils::redis::acknowledgeAndDelete(redis_, stream, group, entryId);
            }
        } catch (const std::exception& error) {
            LOG_WARN << "[Collector] socket_egress_complete_failed: " << error.what();
            egressRecovering_ = true;
        }
        egressWritePending_ = false;
    }

    void spawnActions(std::string connectionId, std::vector<ProtocolAction> actions, bool disconnected = false, std::string reason = {}) {
        if (stopping_) {
            return;
        }
        scope_.spawn(applyActions(std::move(connectionId), std::move(actions), disconnected, std::move(reason)));
    }

    ruvia::task<void> applyActions(std::string connectionId, std::vector<ProtocolAction> actions, bool disconnected = false, std::string reason = {}, std::string replyToPacketId = {}, std::string replyAcquisitionId = {}) {
        // A closed socket must stop being routable before potentially slow command-result
        // publication. Otherwise one Redis failure can leave an offline device advertised as
        // online indefinitely.
        if (disconnected) {
            co_await finishDebugResponse(connectionId, false, reason);
            co_await cleanupConnection(connectionId, reason);
        }
        for (auto& action : actions) {
            if (action.connectionId.empty()) {
                action.connectionId = connectionId;
            }
            if (action.responseSuccess.has_value()) {
                const auto failure = std::find_if(actions.begin(), actions.end(), [](const auto& value) {
                    return value.kind == ProtocolActionKind::FailCommand || value.kind == ProtocolActionKind::Close;
                });
                co_await finishDebugResponse(action.connectionId, *action.responseSuccess, failure == actions.end() ? std::string_view{} : std::string_view(failure->reason));
            }
            switch (action.kind) {
                case ProtocolActionKind::FinishAcquisition:
                    co_await CollectorPacketService::completeAcquisition(redis_, action);
                    break;
                case ProtocolActionKind::Send:
                    if (const auto epoch = connectionEpochs_.find(action.connectionId);
                        epoch != connectionEpochs_.end()) {
                        message::EgressPacket packet{ .messageId = service::utils::nextUuid(uuidState_),
                                                      .workerInstanceId = workerInstanceId_,
                                                      .causationId = action.commandId,
                                                      .connectionId = action.connectionId,
                                                      .sessionEpoch = epoch->second,
                                                      .createdAtMs = message::utcNowMilliseconds(),
                                                      .payload = std::move(action.bytes) };
                        packet.replyToPacketId = replyToPacketId;
                        packet.acquisitionId = !action.acquisitionId.empty() ? action.acquisitionId : !replyAcquisitionId.empty() ? replyAcquisitionId
                            : !action.commandId.empty()                                                                           ? action.commandId
                                                                                                                                  : service::utils::nextUuid(uuidState_);
                        (void)co_await utils::redis::add(redis_, egressStream(), message::egressFields(packet), kEgressStreamCapacity);
                    } else if (!action.commandId.empty()) {
                        co_await finishCommand(action.commandId, false, "stale_session_epoch");
                    }
                    break;
                case ProtocolActionKind::Close: {
                    co_await finishDebugResponse(action.connectionId, false, action.reason);
                    packets_.protocolClosed(action);
                    tcp_.close(action.connectionId, action.reason.empty() ? "protocol_closed" : action.reason);
                    break;
                }
                case ProtocolActionKind::BindDevice:
                    packets_.deviceBound(action);
                    co_await bindRouteIfConnected(action);
                    co_await restoreDerived(action);
                    break;
                case ProtocolActionKind::ObserveParsed:
                case ProtocolActionKind::DiscardCollection:
                case ProtocolActionKind::PublishParsed:
                    if (action.parsed.acquisitionId.empty()) {
                        action.parsed.acquisitionId = action.commandId.empty() ? service::utils::nextUuid(uuidState_) : action.commandId;
                    }
                    action.parsed.messageId = action.parsed.acquisitionId;
                    for (std::size_t index = action.parsed.rawPacketIds.size(); index < action.parsed.rawPayloads.size(); ++index) {
                        action.parsed.rawPacketIds.push_back(service::utils::nextUuid(uuidState_));
                    }
                    if (!action.acquisitionSummary) {
                        std::size_t rawIndex = 0;
                        for (const auto& raw : action.parsed.rawPayloads) {
                            const auto& rawId = action.parsed.rawPacketIds[rawIndex++];
                            co_await captureDebug(action.parsed.linkId, action.connectionId, "", "RX", raw, action.parsed.occurredAtMs, action.parsed.deviceId, false, rawId, action.kind == ProtocolActionKind::DiscardCollection ? "acquisition_partial" : action.kind == ProtocolActionKind::PublishParsed ? "acquisition_success"
                                                                                                                                                                                                                                                                                                              : "received",
                                                  action.reason,
                                                  (rawIndex != action.parsed.rawPayloads.size()) ? std::string_view{} : std::string_view(action.parsed.valuesJson),
                                                  action.replyToPacketId,
                                                  action.parsed.acquisitionId);
                        }
                    }
                    if (action.kind != ProtocolActionKind::PublishParsed) {
                        break;
                    }
                    packets_.parsed(action.parsed);
                    if (!co_await calculateDerived(action.parsed, action.connectionId)) {
                        co_await CollectorTelemetryService::publishParsed(redis_, action.parsed, identity_.instanceId);
                    }
                    co_await applyActions(action.connectionId, engine_.parsedPublished(action.connectionId, action.publicationToken), false, {}, action.parsed.rawPacketIds.empty() ? std::string{} : action.parsed.rawPacketIds.front(), action.parsed.acquisitionId);
                    break;
                case ProtocolActionKind::CompleteCommand: {
                    co_await CollectorPacketService::completeAcquisition(redis_, action);
                    co_await finishDebugResponse(action.connectionId, true, {}, action.commandId);
                    const auto* task = taskForCausation(action.commandId);
                    packets_.commandCompleted(action, task, true);
                    co_await finishCommand(action.commandId, true, action.reason);
                    break;
                }
                case ProtocolActionKind::FailCommand: {
                    co_await CollectorPacketService::completeAcquisition(redis_, action);
                    co_await finishDebugResponse(action.connectionId, false, action.reason, action.commandId);
                    const auto* task = taskForCausation(action.commandId);
                    packets_.commandCompleted(action, task, false);
                    co_await finishCommand(action.commandId, false, action.reason);
                    break;
                }
                case ProtocolActionKind::ScheduleDeadline:
                    scheduleProtocolDeadline(action.connectionId, action.deadlineToken, action.deadlineAfter);
                    break;
                case ProtocolActionKind::CancelDeadline:
                    cancelProtocolDeadline(action.connectionId, action.deadlineToken);
                    break;
            }
        }
        if (disconnected) {
            co_await failPendingForConnection(connectionId, reason);
        }
    }

    ruvia::task<void> restoreDerived(const ProtocolAction& action) {
        const auto* device = findDevice(action.deviceId);
        if (!device) {
            co_return;
        }
        const auto outcome = co_await derived_.expire(*device, action.connectionId, std::nullopt);
        if (outcome.revision != 0) {
            scheduleDerivedExpiry(action.deviceId, action.connectionId, outcome.revision, outcome.nextDeadline);
        }
    }

    ruvia::task<bool> calculateDerived(message::ParsedDeviceMessage& parsed, const std::string& connectionId) {
        const auto outcome = co_await derived_.append(parsed);
        if (outcome.revision == 0) {
            co_return false;
        }
        scheduleDerivedExpiry(parsed.deviceId, connectionId, outcome.revision, outcome.nextDeadline);
        co_return outcome.published;
    }

    // 到期任务只保存设备、连接与配置版本；派生样本由派生服务按当前快照生成。
    void scheduleDerivedExpiry(std::string deviceId, std::string connectionId, std::uint64_t revision, std::optional<std::int64_t> at) {
        if (const auto existing = derivedDeadlines_.find(deviceId); existing != derivedDeadlines_.end()) {
            scheduler_.cancel(existing->second.second);
            derivedDeadlines_.erase(existing);
        }
        if (!at || stopping_) {
            return;
        }
        const auto delay = std::chrono::milliseconds((std::max)(std::int64_t{ 1 }, *at - message::utcNowMilliseconds()));
        const auto token = scheduler_.scheduleAfter(delay, [this, deviceId, connectionId, revision]() mutable {
            derivedDeadlines_.erase(deviceId);
            if (!stopping_) {
                scope_.spawn(expireDerived(std::move(deviceId), std::move(connectionId), revision));
            }
        });
        derivedDeadlines_.insert_or_assign(std::move(deviceId), std::pair{ std::move(connectionId), token });
    }

    ruvia::task<void> expireDerived(std::string deviceId, std::string connectionId, std::uint64_t revision) {
        if (stopping_) {
            co_return;
        }
        const auto* device = findDevice(deviceId);
        if (!device) {
            co_return;
        }
        const auto outcome = co_await derived_.expire(*device, connectionId, revision);
        if (outcome.revision != 0) {
            scheduleDerivedExpiry(std::move(deviceId), std::move(connectionId), outcome.revision, outcome.nextDeadline);
        }
    }

    void scheduleProtocolDeadline(std::string connectionId, std::uint64_t protocolToken, std::chrono::milliseconds delay) {
        const auto key = std::pair{ connectionId, protocolToken };
        cancelProtocolDeadline(connectionId, protocolToken);
        protocolDeadlines_[key] = scheduler_.scheduleAfter(
            delay,
            [this, connectionId = std::move(connectionId), protocolToken, key] {
                protocolDeadlines_.erase(key);
                if (stopping_) {
                    return;
                }
                packets_.deadlineFired(connectionId, protocolToken);
                auto actions = engine_.deadline(connectionId, protocolToken);
                if (actions.empty()) {
                    packets_.deadlineUnresolved(connectionId);
                }
                spawnActions(connectionId, std::move(actions));
            }
        );
    }

    void cancelProtocolDeadline(std::string_view connectionId, std::uint64_t protocolToken) {
        const auto current = protocolDeadlines_.find({ std::string(connectionId), protocolToken });
        if (current == protocolDeadlines_.end()) {
            return;
        }
        scheduler_.cancel(current->second);
        protocolDeadlines_.erase(current);
    }

    ruvia::task<void> waitForWork() {
        try {
            const std::vector<std::string> streams{
                configStream(),
                controlStream(),
                commandStream(true),
                commandStream(false),
                egressStream(),
                linkEventStream()
            };
            while (!stopping_) {
                const auto deadline = nextOwnershipRefresh_
                    ? (std::min)(*nextOwnershipRefresh_, nextStateRefresh_)
                    : nextStateRefresh_;
                const auto until = (std::max)(std::chrono::milliseconds(1),
                                              std::chrono::duration_cast<std::chrono::milliseconds>(
                                                  deadline - std::chrono::steady_clock::now()
                                              ));
                auto batches = co_await utils::redis::readGroupManyBlockingUntil(
                    redisBlocking_,
                    streams,
                    collectorGroup(),
                    consumer_,
                    scope_.get_stop_token(),
                    until,
                    1
                );
                if (stopping_) {
                    co_return;
                }
                if (batches.empty()) {
                    const auto now = std::chrono::steady_clock::now();
                    if (nextOwnershipRefresh_ && now >= *nextOwnershipRefresh_) {
                        (void)co_await refreshConfigState(false);
                    }
                    if (now >= nextStateRefresh_) {
                        co_await renewWorkerState();
                    }
                    continue;
                }
                for (auto& batch : batches) {
                    auto& ready = readyMessages_[batch.stream];
                    ready.insert(ready.end(), std::make_move_iterator(batch.messages.begin()), std::make_move_iterator(batch.messages.end()));
                }
                scheduleTick();
                co_return;
            }
        } catch (const std::exception& error) {
            if (!stopping_) {
                recoverClaimedMessages(error.what());
            }
        } catch (...) {
            if (!stopping_) {
                recoverClaimedMessages("unknown_blocking_read_exception");
            }
        }
    }

    ruvia::task<std::vector<message::StreamMessage>>
    readAvailable(std::string_view stream, std::string_view group, bool& recovering, std::size_t count) {
        if (auto ready = readyMessages_.find(stream); ready != readyMessages_.end()) {
            auto messages = std::move(ready->second);
            readyMessages_.erase(ready);
            co_return messages;
        }
        auto messages = co_await utils::redis::readGroup(
            redis_,
            stream,
            group,
            consumer_,
            recovering ? "0" : ">",
            std::chrono::milliseconds(0),
            count
        );
        if (recovering && messages.empty()) {
            recovering = false;
        }
        co_return messages;
    }

    void cancelProtocolDeadlinesForConnection(std::string_view connectionId, bool cancelDerived = true) {
        for (auto current = derivedDeadlines_.begin(); current != derivedDeadlines_.end();) {
            if (cancelDerived && current->second.first == connectionId) {
                scheduler_.cancel(current->second.second);
                current = derivedDeadlines_.erase(current);
            } else {
                ++current;
            }
        }
        for (auto current = protocolDeadlines_.begin(); current != protocolDeadlines_.end();) {
            if (current->first.first == connectionId) {
                scheduler_.cancel(current->second);
                current = protocolDeadlines_.erase(current);
            } else {
                ++current;
            }
        }
    }

    ruvia::task<void> applySessionRefresh(ProtocolSessionRefresh refresh) {
        std::set<std::string, std::less<>> nextDeviceCodes;
        for (const auto& action : refresh.startedActions) {
            if (action.kind == ProtocolActionKind::BindDevice) {
                nextDeviceCodes.insert(action.deviceId);
            }
        }

        const auto bound = routes_.find(refresh.connectionId);
        if (bound != routes_.end()) {
            std::vector<std::pair<std::string, std::uint64_t>> identities;
            identities.reserve(bound->second.size());
            for (const auto& deviceCode : bound->second) {
                if (const auto identity = routeEpochs_.find({ refresh.connectionId, deviceCode });
                    identity != routeEpochs_.end()) {
                    identities.emplace_back(deviceCode, identity->second);
                }
            }
            for (const auto& [deviceCode, epoch] : identities) {
                if (!nextDeviceCodes.contains(deviceCode)) {
                    co_await CollectorStateService::markDeviceOffline(redis_, deviceCode, refresh.connectionId, epoch, identity_.index, workerInstanceId_, "configuration_reloaded", identity_.instanceId);
                }
                if (const auto identity = routeEpochs_.find({ refresh.connectionId, deviceCode });
                    identity != routeEpochs_.end() && identity->second == epoch) {
                    routeEpochs_.erase(identity);
                    if (const auto current = routes_.find(refresh.connectionId); current != routes_.end()) {
                        current->second.erase(deviceCode);
                    }
                }
            }
            if (const auto current = routes_.find(refresh.connectionId);
                current != routes_.end() && current->second.empty()) {
                routes_.erase(current);
            }
        }

        co_await applyActions(refresh.connectionId, std::move(refresh.retiredActions));
        co_await failPendingForConnection(refresh.connectionId, "configuration_reloaded");
        co_await applyActions(refresh.connectionId, std::move(refresh.startedActions));
    }

    ruvia::task<void> cleanupConnection(std::string_view connectionId, std::string_view reason) {
        // Retained input samples still expire after transport closes. Their
        // bounded deadlines stay with this Worker and release themselves when
        // no valid inputs/window samples remain; no socket or payload is kept.
        cancelProtocolDeadlinesForConnection(connectionId, false);
        connectionEpochs_.erase(std::string(connectionId));
        const auto bound = routes_.find(connectionId);
        if (bound != routes_.end()) {
            const auto devices = bound->second;
            for (const auto& deviceCode : devices) {
                try {
                    co_await eraseRoute(deviceCode, connectionId, reason);
                    routeEpochs_.erase({ std::string(connectionId), deviceCode });
                    if (const auto current = routes_.find(connectionId); current != routes_.end()) {
                        current->second.erase(deviceCode);
                    }
                } catch (const std::exception& error) {
                    // 身份留待停止时重试；真实 socket 已移除，租约不会再续期并在 15 秒内过期。
                    LOG_WARN << "[Collector] disconnected_route_cleanup_failed: " << error.what();
                } catch (...) {
                    LOG_WARN << "[Collector] disconnected_route_cleanup_failed: unknown error";
                }
            }
            if (const auto current = routes_.find(connectionId);
                current != routes_.end() && current->second.empty()) {
                routes_.erase(current);
            }
        }
    }

    ruvia::task<void> failPendingForConnection(std::string_view connectionId, std::string_view reason) {
        const auto commandIds = commands_.pendingForConnection(connectionId);
        const auto failureReason = reason.empty()
            ? std::string_view("connection_closed_result_unknown")
            : reason;
        for (const auto& commandId : commandIds) {
            co_await finishCommand(commandId, false, failureReason);
        }
    }

    [[nodiscard]] bool connectionIsLive(std::string_view connectionId, std::uint64_t epoch) const noexcept {
        const auto network = networkConnections_.find(connectionId);
        return network != networkConnections_.end() && network->second.sessionEpoch == epoch;
    }

    ruvia::task<void> eraseRoute(std::string_view deviceId, std::string_view connectionId, std::string_view reason) {
        const auto identity = routeEpochs_.find({ std::string(connectionId), std::string(deviceId) });
        if (identity == routeEpochs_.end()) {
            co_return;
        }
        const auto epoch = identity->second;
        co_await CollectorStateService::markDeviceOffline(redis_, deviceId, connectionId, epoch, identity_.index, workerInstanceId_, reason, identity_.instanceId);
    }

    ruvia::task<void> renewConnectionState() {
        // 此快照只含本 Worker 已登记的身份；EOF 会同步移除 networkConnections_，不再续租。
        const auto identities = routeEpochs_;
        for (const auto& [identity, epoch] : identities) {
            const auto& [connectionId, deviceId] = identity;
            if (!connectionIsLive(connectionId, epoch)) {
                continue;
            }
            const auto retained = co_await CollectorStateService::renewRoute(
                redis_.with_options({ .timeout_ = std::chrono::seconds(3) }),
                deviceId,
                connectionId,
                epoch,
                identity_.index,
                workerInstanceId_,
                identity_.instanceId
            );
            if (!connectionIsLive(connectionId, epoch) || stopping_) {
                co_await CollectorStateService::markDeviceOffline(redis_, deviceId, connectionId, epoch, identity_.index, workerInstanceId_, "connection_closed_during_renewal", identity_.instanceId);
            } else if (!retained) {
                tcp_.close(connectionId, "connection_route_lease_expired");
            }
        }
    }

    ruvia::task<void> bindRouteIfConnected(const ProtocolAction& action) {
        const auto connected = connectionEpochs_.find(action.connectionId);
        if (connected == connectionEpochs_.end() || !connectionIsLive(action.connectionId, connected->second)) {
            co_return;
        }
        const auto epoch = connected->second;
        // 先登记清理身份：Redis 已执行但回复丢失、或停止取消绑定任务时仍能 CAS 清理。
        routes_[action.connectionId].insert(action.deviceId);
        routeEpochs_.insert_or_assign({ action.connectionId, action.deviceId }, epoch);
        co_await bindRoute(action, epoch);
        // Redis I/O yields to the worker loop. EOF may have removed this connection while the
        // HSET was in flight; compensate before making it locally routable.
        const auto current = connectionEpochs_.find(action.connectionId);
        if (stopping_ || current == connectionEpochs_.end() || current->second != epoch ||
            !connectionIsLive(action.connectionId, epoch)) {
            co_await CollectorStateService::markDeviceOffline(redis_, action.deviceId, action.connectionId, epoch, identity_.index, workerInstanceId_, "connection_closed_during_registration", identity_.instanceId);
            co_return;
        }
    }

    ruvia::task<void> bindRoute(const ProtocolAction& action, std::uint64_t sessionEpoch) {
        const auto connection = networkConnections_.find(action.connectionId);
        if (connection == networkConnections_.end()) {
            co_return;
        }
        const auto owner = co_await CollectorStateService::bindRoute(
            redis_,
            action,
            sessionEpoch,
            identity_.index,
            workerInstanceId_,
            connection->second.linkId,
            identity_.instanceId
        );
        const auto& oldWorker = owner.workerId;
        const auto& oldConnection = owner.connectionId;
        const auto worker = std::to_string(identity_.index);
        if (oldConnection.empty() || oldConnection == action.connectionId || oldWorker.empty()) {
            co_return;
        }
        const auto& oldInstance = owner.instanceId;
        if (oldWorker == worker && oldInstance == identity_.instanceId) {
            tcp_.close(oldConnection, "device_re_registered");
            co_return;
        }
        const std::vector<message::StreamField> fields{
            { "message_id", service::utils::nextUuid(uuidState_) },
            { "connection_id", oldConnection },
            { "device_code", action.deviceCode },
            { "reason", "device_re_registered" },
            { "created_at_ms", std::to_string(message::utcNowMilliseconds()) }
        };
        (void)co_await utils::redis::add(
            redis_,
            std::string(message::kControlStreamPrefix) + std::string(oldInstance) + ":" + oldWorker,
            fields,
            1000
        );
    }

    ruvia::task<void> finishCommand(std::string_view commandId, bool success, std::string_view reason) {
        const auto* task = commands_.taskForCausation(commandId);
        const auto remote = task ? remoteAddress(task->connectionId) : std::string{};
        co_await commands_.finish(commandId, success, reason, remote);
    }

    ruvia::task<void> failUndeliverable(std::string_view stream, std::string_view entryId, const message::ProtocolTask& task, std::string_view reason) {
        co_await commands_.failUndeliverable(stream, entryId, task, reason, remoteAddress(task.connectionId));
    }

    [[nodiscard]] std::vector<message::StreamField> linkStateFields(const LinkState& state) {
        std::string endpoints;
        for (const auto& endpoint : state.remoteEndpoints) {
            if (!endpoints.empty()) {
                endpoints.push_back(',');
            }
            endpoints += endpoint;
        }
        std::vector<message::StreamField> fields{
            { "message_id", service::utils::nextUuid(uuidState_) },
            { "worker_instance_id", workerInstanceId_ },
            { "link_id", state.linkId },
            { "worker_id", std::to_string(state.workerIndex) },
            { "state", state.state },
            { "state_reason", state.reason },
            { "error", state.error },
            { "connection_count", std::to_string(state.remoteEndpoints.size()) },
            { "remote_endpoints", endpoints },
            { "last_activity_at_ms", std::to_string(state.lastActivityAtMs) },
            { "created_at_ms", std::to_string(message::utcNowMilliseconds()) }
        };
        for (const auto& target : state.targets) {
            const auto prefix = "target:" + target.id + ':';
            fields.push_back({ prefix + "state", target.state });
            fields.push_back({ prefix + "reason", target.reason });
            fields.push_back({ prefix + "error", target.error });
            fields.push_back(
                { prefix + "last_activity_at_ms", std::to_string(target.lastActivityAtMs) }
            );
        }
        return fields;
    }

    ruvia::task<void> publishLinkEvent(LinkState state) {
        (void)co_await utils::redis::add(
            redis_,
            linkEventStream(),
            linkStateFields(state),
            kLinkEventStreamCapacity
        );
    }

    ruvia::task<bool> consumeLinkEvent() {
        const auto stream = linkEventStream();
        const auto group = linkEventGroup();
        const auto messages =
            co_await readAvailable(stream, group, linkEventRecovering_, 32);
        for (const auto& message : messages) {
            const auto linkId = message.get("link_id");
            const auto workerId = message.get("worker_id");
            const auto workerInstanceId = message.get("worker_instance_id");
            if (linkId.empty() || workerId != std::to_string(identity_.index) ||
                workerInstanceId != workerInstanceId_) {
                co_await CollectorCommandService::rejectMessage(redis_, identity_.index, identity_.instanceId, consumer_, message, "link_event_invalid", {}, stream, group);
                continue;
            }
            co_await CollectorStateService::storeLinkEvent(redis_, message, identity_.index, identity_.instanceId);
            co_await utils::redis::acknowledgeAndDelete(redis_, stream, group, message.id);
        }
        co_return !messages.empty();
    }

    // 入站内存队列上限沿用原入站流容量；超出时关闭该连接，由设备重连。
    static constexpr std::size_t kIngressCapacity = 100000;
    // Redis retains ownership for 15 seconds. Stop local I/O before that bound,
    // even when a slow Redis command delays renewal on this worker.
    static constexpr auto kTargetLeaseValidity = std::chrono::seconds(12);
    static constexpr auto kFailureDelay = std::chrono::milliseconds(250);
    static constexpr std::size_t kCommandConsumeBatch = 16;
    static constexpr std::size_t kEgressStreamCapacity = 10000;
    static constexpr std::size_t kLinkEventStreamCapacity = 1000;

    const common::WorkerIdentity identity_;
    service::common::UuidV7State uuidState_;
    ruvia::event_loop loop_;
    ruvia::worker_handle workerHandle_;
    std::pmr::unsynchronized_pool_resource resource_;
    ruvia::task_scope scope_;
    DeadlineScheduler scheduler_;
    std::unique_ptr<ruvia::redis_client> redisClient_;
    ruvia::redis_handle redisBlocking_;
    ruvia::redis_handle redis_;
    ProtocolEngine engine_;
    TcpTransport tcp_;
    std::string workerInstanceId_ = service::utils::nextUuid(uuidState_);
    std::string consumer_;
    CollectorCommandService commands_;
    CollectorDerivedService derived_;
    std::map<std::pair<std::string, std::uint64_t>, DeadlineScheduler::Token> protocolDeadlines_;
    std::map<std::string, std::pair<std::string, DeadlineScheduler::Token>> derivedDeadlines_;
    std::map<std::string, std::set<std::string>, std::less<>> routes_;
    std::map<std::pair<std::string, std::string>, std::uint64_t> routeEpochs_;
    std::map<std::string, ProtocolConnectionInfo, std::less<>> networkConnections_;
    std::map<std::string, LinkState, std::less<>> linkStates_;
    std::map<std::string, std::uint64_t, std::less<>> connectionEpochs_;
    std::map<ClientTargetKey, std::string> targetLeases_;
    std::map<std::string, TargetLeaseDeadline, std::less<>> targetLeaseDeadlines_;
    DeadlineScheduler::Token targetRenewalToken_ = 0;
    std::map<std::string, std::vector<message::StreamMessage>, std::less<>> readyMessages_;
    DeadlineScheduler::Token tickToken_ = 0;
    std::string loadedConfigVersion_;
    std::string desiredConfigVersion_;
    std::optional<std::chrono::steady_clock::time_point> nextOwnershipRefresh_;
    std::chrono::steady_clock::time_point nextStateRefresh_{};
    std::map<std::string, ownership::LinkLease, std::less<>> linkLeases_;
    RuntimeSnapshot desiredSnapshot_;
    RuntimeSnapshot loadedSnapshot_;
    std::string loadedSignature_;
    std::map<std::string, std::size_t, std::less<>> linkIndexes_;
    std::map<std::string, std::size_t, std::less<>> deviceIndexes_;
    // 链路本身或其任一设备开启调试的链路。
    std::set<std::string, std::less<>> debugLinks_;
    bool configRecovering_ = true;
    bool needsConfigSnapshot_ = false;
    bool highRecovering_ = true;
    bool normalRecovering_ = true;
    bool controlRecovering_ = true;
    bool egressRecovering_ = true;
    bool linkEventRecovering_ = true;
    bool egressWritePending_ = false;
    bool ingressDraining_ = false;
    std::deque<IngressWork> ingressWork_;
    bool stopping_ = false;
    bool shutdownStarted_ = false;
    CollectorPacketService packets_;
};

} // namespace service::collector
