#pragma once

#include <algorithm>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/features/configuration/configuration.service.h"
#include "service/middleware/log.h"
#include "service/utils/redis.h"
#include "service/utils/worker_task.h"

namespace service::configuration {

class Reconciler final {
  public:
    Reconciler() = default;
    Reconciler(const Reconciler&) = delete;
    Reconciler& operator=(const Reconciler&) = delete;

    ~Reconciler() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (stopped_.valid()) {
            return;
        }
        if (!worker.valid()) {
            throw std::runtime_error("runtime reconciler requires Service and Collector Workers");
        }
        stop_ = std::make_unique<ruvia::stop_source>();
        utils::startWorkerTask(worker, stop_->token(), stopped_, [this](ruvia::web_worker_context& context, utils::WorkerReady& ready, ruvia::stop_token stop) {
            return run(context, ready, stop);
        });
    }

    void stop() noexcept {
        if (stop_) {
            utils::stopWorkerTask(*stop_, stopped_);
            stop_.reset();
        }
    }

  private:
    static constexpr std::string_view kGroup = "iot-engine:runtime-reconciler";
    static constexpr std::size_t kBatchSize = 256;
    static constexpr std::size_t kCoalesceLimit = 4096;
    static constexpr auto kCoalesceWindow = std::chrono::milliseconds(25);

    ruvia::task<void> run(ruvia::web_worker_context& context, utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto& identity = context.worker_state<std::optional<common::WorkerIdentity>>().value();
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        const std::vector<std::string> streams{ message::configuration::runtimeChangesStream() };
        co_await utils::redis::ensureGroup(redis, streams.front(), kGroup);
        co_await utils::redis::withConsumerNotifications(redis, context.worker(), streams, kGroup, stop, [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
            auto cleanupAt = co_await ConfigurationService::retireSnapshots(context);
            ready.set_value();
            const auto consumer = identity.instanceId + ":service-" + std::to_string(identity.index);
            bool recovering = true;
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    if (cleanupAt && message::utcNowMilliseconds() >= *cleanupAt) {
                        cleanupAt = co_await ConfigurationService::retireSnapshots(context);
                    }
                    const auto wait = cleanupAt
                        ? std::optional(std::chrono::milliseconds(std::max<std::int64_t>(0, *cleanupAt - message::utcNowMilliseconds())))
                        : std::nullopt;
                    const auto replaying = recovering;
                    auto batches = co_await utils::redis::consumeGroup(redis, streams, kGroup, consumer, recovering, notifications, stop, wait, kBatchSize);
                    if (batches.empty()) {
                        continue;
                    }
                    if (!replaying) {
                        (void)co_await ruvia::sleep_for(context.worker(), kCoalesceWindow, stop);
                    }
                    std::size_t messageCount = 0;
                    for (const auto& batch : batches) {
                        messageCount += batch.messages.size();
                    }
                    while (!replaying && messageCount < kCoalesceLimit && !stop.stop_requested()) {
                        auto next = co_await utils::redis::consumeGroup(redis, streams, kGroup, consumer, recovering, notifications, stop, std::chrono::milliseconds(0), std::min(kBatchSize, kCoalesceLimit - messageCount), std::chrono::minutes(1), true);
                        if (next.empty()) {
                            break;
                        }
                        for (auto& batch : next) {
                            messageCount += batch.messages.size();
                            batches.push_back(std::move(batch));
                        }
                    }
                    std::vector<message::StreamMessage> received;
                    received.reserve(messageCount);
                    for (const auto& batch : batches) {
                        received.insert(received.end(), batch.messages.begin(), batch.messages.end());
                    }
                    if (co_await ConfigurationService::applyRuntimeChanges(context, kGroup, received)) {
                        cleanupAt = co_await ConfigurationService::retireSnapshots(context);
                    }
                    for (const auto& batch : batches) {
                        co_await utils::redis::acknowledgeAndDeleteMany(redis, batch.stream, kGroup, batch.messages);
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "runtime reconciliation failed: " << error.what();
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
    std::future<void> stopped_;
};

} // namespace service::configuration
