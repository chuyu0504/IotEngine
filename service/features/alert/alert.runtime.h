#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/features/alert/alert.service.h"
#include "service/utils/log.h"
#include "service/utils/redis.h"
#include "service/utils/worker_task.h"

namespace service::alert {

// Each Worker owns its tasks; consumer groups claim durable telemetry and
// configuration work, while offline evaluations run only at actual deadlines.
class AlertRuntime final {
  public:
    AlertRuntime() = default;
    AlertRuntime(const AlertRuntime&) = delete;
    AlertRuntime& operator=(const AlertRuntime&) = delete;

    ~AlertRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (maintenance_.valid() || telemetry_.valid()) {
            return;
        }
        if (!worker.valid()) {
            throw std::runtime_error("alert runtime requires a Service Worker");
        }
        stop_.emplace();
        try {
            service::utils::startWorkerTask(worker, stop_->token(), maintenance_, [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                return maintain(context, ready, stop);
            });
            service::utils::startWorkerTask(worker, stop_->token(), telemetry_, [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                return consumeTelemetry(context, ready, stop);
            });
        } catch (...) {
            this->stop();
            throw;
        }
    }

    void stop() noexcept {
        if (stop_) {
            service::utils::stopWorkerTask(*stop_, maintenance_);
            service::utils::stopWorkerTask(*stop_, telemetry_);
            stop_.reset();
        }
    }

  private:
    // Preserve deployed consumer groups so upgrades recover pending messages.
    static constexpr std::string_view kTelemetryGroup{ "iot-engine:telemetry-persistence:alerts" };
    static constexpr std::string_view kMetadataGroup{ "iot-engine:alert-metadata" };
    static constexpr auto kFailureBackoff = std::chrono::milliseconds(250);

    static std::string consumerName(const service::common::WorkerIdentity& identity) {
        return identity.instanceId + ":service-" + std::to_string(identity.index);
    }

    ruvia::task<void> consumeTelemetry(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        const auto redis = context.redis();
        const std::vector<std::string> streams{ std::string(service::message::telemetry::kAlertsStream) };
        const auto consumer = consumerName(identity);
        co_await service::utils::redis::ensureGroup(redis, streams.front(), kTelemetryGroup);
        bool recovering = true;
        co_await service::utils::redis::withConsumerNotifications(
            redis,
            context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
            context.worker(),
            streams,
            kTelemetryGroup,
            stop,
            [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
                ready.set_value();
                while (!stop.stop_requested()) {
                    bool failed = false;
                    try {
                        const auto batches = co_await service::utils::redis::consumeGroup(
                            redis,
                            context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                            streams,
                            kTelemetryGroup,
                            consumer,
                            recovering,
                            notifications,
                            stop
                        );
                        for (const auto& batch : batches) {
                            if (stop.stop_requested()) {
                                break;
                            }
                            // Commit the complete evaluation before acknowledging it.
                            co_await AlertEvaluationService::evaluateTelemetry(context, batch.messages);
                            co_await service::utils::redis::acknowledgeAndDeleteMany(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, batch.stream, kTelemetryGroup, batch.messages);
                        }
                    } catch (const std::exception& error) {
                        if (stop.stop_requested()) {
                            break;
                        }
                        LOG_ERROR << "alert telemetry evaluation failed for service worker " << identity.index << ": " << error.what();
                        recovering = true;
                        failed = true;
                    }
                    if (failed) {
                        (void)co_await ruvia::sleep_for(context.worker(), kFailureBackoff, stop);
                    }
                }
            }
        );
    }

    ruvia::task<void> maintain(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        const auto redis = context.redis();
        const std::vector<std::string> streams{
            std::string(service::message::alert::kMetadataChangesStream),
            service::message::workerWakeStream(std::nullopt, identity.instanceId)
        };
        const auto consumer = consumerName(identity);
        for (const auto& stream : streams) {
            co_await service::utils::redis::ensureGroup(redis, stream, kMetadataGroup);
        }
        co_await metadata::refresh(context);
        co_await AlertEvaluationService::drainOutbox(context);
        auto deadline = co_await metadata::nextOfflineDeadline(redis);
        bool recovering = true;
        bool scheduleChanged = false;
        co_await service::utils::redis::withConsumerNotifications(
            redis,
            context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
            context.worker(),
            streams,
            kMetadataGroup,
            stop,
            [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
                ready.set_value();
                while (!stop.stop_requested()) {
                    bool failed = false;
                    try {
                        if (scheduleChanged) {
                            deadline = co_await metadata::nextOfflineDeadline(redis);
                            scheduleChanged = false;
                        }
                        if (deadline && *deadline <= service::message::utcNowMilliseconds()) {
                            // The shared deadline queue atomically claims each due member.
                            co_await AlertEvaluationService::evaluateOfflineDue(context);
                            deadline = co_await metadata::nextOfflineDeadline(redis);
                            continue;
                        }
                        std::optional<std::chrono::milliseconds> wait;
                        if (deadline) {
                            wait = std::chrono::milliseconds(std::max<std::int64_t>(0, *deadline - service::message::utcNowMilliseconds()));
                        }
                        const auto batches = co_await service::utils::redis::consumeGroup(
                            redis,
                            context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                            streams,
                            kMetadataGroup,
                            consumer,
                            recovering,
                            notifications,
                            stop,
                            wait
                        );
                        for (const auto& batch : batches) {
                            if (stop.stop_requested()) {
                                break;
                            }
                            if (batch.stream == streams.front()) {
                                co_await metadata::refresh(context);
                                co_await service::utils::redis::acknowledgeAndDeleteMany(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, batch.stream, kMetadataGroup, batch.messages);
                                scheduleChanged = true;
                            } else {
                                for (const auto& message : batch.messages) {
                                    // Preserve the shared wake entry while completing this group's claim.
                                    co_await service::utils::redis::acknowledge(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, batch.stream, kMetadataGroup, message.id);
                                    if (message.get("task") == "freshness-alerts") {
                                        scheduleChanged = true;
                                    }
                                }
                            }
                        }
                        if (scheduleChanged) {
                            deadline = co_await metadata::nextOfflineDeadline(redis);
                            scheduleChanged = false;
                        }
                    } catch (const std::exception& error) {
                        if (stop.stop_requested()) {
                            break;
                        }
                        LOG_ERROR << "alert maintenance failed for service worker " << identity.index << ": " << error.what();
                        recovering = true;
                        failed = true;
                    }
                    if (failed) {
                        (void)co_await ruvia::sleep_for(context.worker(), kFailureBackoff, stop);
                    }
                }
            }
        );
    }

    std::optional<ruvia::stop_source> stop_;
    std::future<void> maintenance_;
    std::future<void> telemetry_;
};

} // namespace service::alert
