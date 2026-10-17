#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/features/telemetry/telemetry.service.h"
#include "service/utils/log.h"
#include "service/utils/redis.h"
#include "service/utils/worker_task.h"

namespace service::telemetry {

class PersistenceRuntime final {
  public:
    PersistenceRuntime() = default;
    PersistenceRuntime(const PersistenceRuntime&) = delete;
    PersistenceRuntime& operator=(const PersistenceRuntime&) = delete;

    ~PersistenceRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (stop_) {
            return;
        }
        stop_ = std::make_unique<ruvia::stop_source>();
        try {
            for (std::size_t index = 0; index < kRoutes.size(); ++index) {
                utils::startWorkerTask(worker, stop_->token(), stopped_[index], [this, index](ruvia::web_worker_context& context, utils::WorkerReady& ready, ruvia::stop_token stop) {
                    return consume(context, kRoutes[index], ready, stop);
                });
            }
            utils::startWorkerTask(worker, stop_->token(), stopped_.back(), [this](ruvia::web_worker_context& context, utils::WorkerReady& ready, ruvia::stop_token stop) {
                return maintainFreshness(context, ready, stop);
            });
        } catch (...) {
            this->stop();
            throw;
        }
    }

    void stop() noexcept {
        if (!stop_) {
            return;
        }
        stop_->request_stop();
        for (auto& completion : stopped_) {
            utils::stopWorkerTask(*stop_, completion);
        }
        stop_.reset();
    }

  private:
    struct Route final {
        Consumer consumer;
        std::string_view name;
    };

    static constexpr std::string_view kGroup = "iot-engine:telemetry-persistence";
    static constexpr std::array<Route, 4> kRoutes{ { { Consumer::Dispatch, "dispatch" }, { Consumer::History, "history" }, { Consumer::Latest, "latest" }, { Consumer::Delivery, "delivery" } } };

    ruvia::task<void> consume(ruvia::web_worker_context& context, const Route& route, utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto& identity = context.worker_state<std::optional<common::WorkerIdentity>>().value();
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        const auto group = std::string(kGroup) + ":" + std::string(route.name);
        const auto consumer = identity.instanceId + ":service-" + std::to_string(identity.index);
        const std::vector<std::string> streams{ consumerStream(route.consumer) };
        co_await utils::redis::ensureGroup(redis, streams.front(), group);
        co_await utils::redis::withConsumerNotifications(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, context.worker(), streams, group, stop, [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
            ready.set_value();
            bool recovering = true;
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    auto batches = co_await utils::redis::consumeGroup(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, streams, group, consumer, recovering, notifications, stop);
                    for (const auto& batch : batches) {
                        co_await TelemetryService::apply(context, route.consumer, batch.messages);
                        co_await utils::redis::acknowledgeAndDeleteMany(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, batch.stream, group, batch.messages);
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "telemetry " << route.name << " persistence failed: " << error.what();
                    recovering = true;
                    failed = true;
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(250), stop);
                }
            }
        });
    }

    ruvia::task<void> maintainFreshness(ruvia::web_worker_context& context, utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto& identity = context.worker_state<std::optional<common::WorkerIdentity>>().value();
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        const std::vector<std::string> streams{
            message::workerWakeStream(std::nullopt, identity.instanceId),
            message::workerWakeStream(identity.index, identity.instanceId)
        };
        const auto group = std::string(kGroup) + ":freshness";
        const auto consumer = identity.instanceId + ":service-" + std::to_string(identity.index);
        for (const auto& stream : streams) {
            co_await utils::redis::ensureGroup(redis, stream, group);
        }
        co_await utils::redis::withConsumerNotifications(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, context.worker(), streams, group, stop, [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
            auto deadline = co_await TelemetryService::retireExpiredFreshness(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels);
            ready.set_value();
            bool changed = false;
            bool recovering = true;
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    if (changed || (deadline && message::utcNowMilliseconds() >= *deadline)) {
                        deadline = co_await TelemetryService::retireExpiredFreshness(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels);
                        changed = false;
                    }
                    const auto wait = deadline
                        ? std::optional(std::chrono::milliseconds(std::max<std::int64_t>(0, *deadline - message::utcNowMilliseconds())))
                        : std::nullopt;
                    auto batches = co_await utils::redis::consumeGroup(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, streams, group, consumer, recovering, notifications, stop, wait);
                    for (const auto& batch : batches) {
                        for (const auto& item : batch.messages) {
                            changed = changed || item.get("task") == message::workerStreamTaskName(message::WorkerStreamTask::Freshness);
                            // Each consumer group must observe the shared wake entry.
                            co_await utils::redis::acknowledge(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, batch.stream, group, item.id);
                        }
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "telemetry freshness maintenance failed: " << error.what();
                    recovering = true;
                    failed = true;
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(250), stop);
                }
            }
        });
    }

    std::unique_ptr<ruvia::stop_source> stop_;
    std::array<std::future<void>, kRoutes.size() + 1> stopped_;
};

} // namespace service::telemetry
