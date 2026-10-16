#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"
#include "service/features/telemetry/latest/latest.service.h"
#include "service/features/telemetry/telemetry.service.h"
#include "service/middleware/log.h"

namespace service::telemetry {
class PersistenceRuntime final : public TelemetryService {
  public:
    PersistenceRuntime() = default;
    PersistenceRuntime(const PersistenceRuntime&) = delete;
    PersistenceRuntime& operator=(const PersistenceRuntime&) = delete;

    ~PersistenceRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex, std::size_t serviceWorkerCount, std::size_t collectorWorkerCount) {
        if (running_.exchange(true)) {
            return;
        }
        worker_ = std::move(worker);
        workerIndex_ = workerIndex;
        serviceWorkerCount_ = serviceWorkerCount;
        collectorWorkerCount_ = collectorWorkerCount;
        if (!worker_.valid() || serviceWorkerCount_ == 0 ||
            workerIndex_ >= serviceWorkerCount_ || collectorWorkerCount_ == 0) {
            running_.store(false);
            throw std::runtime_error("telemetry persistence requires north and collector workers");
        }
        std::vector<std::future<void>> readiness;
        try {
            for (const auto& route : kRoutes) {
                auto ready = std::make_shared<std::promise<void>>();
                auto stopped = std::make_shared<std::promise<void>>();
                readiness.push_back(ready->get_future());
                stopped_.push_back(stopped->get_future().share());
                const auto posted = worker_.post(
                    [this, &route, ready, stopped](ruvia::web_worker_context& context) {
                        return run(context, workerIndex_, route, ready, stopped);
                    }
                );
                if (!posted.accepted()) {
                    stopped->set_value();
                    stop();
                    throw std::runtime_error("service worker rejected telemetry consumer");
                }
            }
            auto ready = std::make_shared<std::promise<void>>();
            auto stopped = std::make_shared<std::promise<void>>();
            readiness.push_back(ready->get_future());
            stopped_.push_back(stopped->get_future().share());
            const auto posted = worker_.post(
                [this, ready, stopped](ruvia::web_worker_context& context) {
                    return maintainFreshness(context, workerIndex_, ready, stopped);
                }
            );
            if (!posted.accepted()) {
                stopped->set_value();
                stop();
                throw std::runtime_error("service worker rejected telemetry freshness task");
            }
            for (auto& ready : readiness) {
                ready.get();
            }
        } catch (...) {
            stop();
            throw;
        }
    }

    void stop() noexcept {
        if (!running_.exchange(false)) {
            return;
        }
        if (worker_.valid()) {
            // A closed worker already propagates its stop token to these tasks.
            (void)worker_.post([](ruvia::web_worker_context&) -> ruvia::task<void> {
                             for (const auto& route : kRoutes) {
                                 message::workerStreamMultiplexer().signal(route.wake);
                             }
                             message::workerStreamMultiplexer().signal(message::WorkerStreamTask::Freshness);
                             co_return;
                         })
                .accepted();
        }
        for (const auto& stopped : stopped_) {
            if (stopped.valid()) {
                stopped.wait();
            }
        }
        stopped_.clear();
        worker_ = {};
    }

  private:
    struct Route final {
        Consumer consumer;
        std::string_view name;
        message::WorkerStreamTask wake;
    };

    static constexpr std::string_view kGroup = "iot-engine:telemetry-persistence";
    static constexpr std::size_t kBatchSize = 256;
    static constexpr std::array<Route, 4> kRoutes{ {
        { Consumer::Dispatch, "dispatch", message::WorkerStreamTask::Telemetry },
        { Consumer::History, "history", message::WorkerStreamTask::TelemetryHistory },
        { Consumer::Latest, "latest", message::WorkerStreamTask::TelemetryLatest },
        { Consumer::Delivery, "delivery", message::WorkerStreamTask::TelemetryDelivery },
    } };

    ruvia::task<void> maintainFreshness(
        ruvia::web_worker_context& context,
        std::size_t index,
        std::shared_ptr<std::promise<void>> ready,
        std::shared_ptr<std::promise<void>> stopped
    ) {
        try {
            const auto redis = context.redis();
            ready->set_value();
            while (running_.load() && !context.get_stop_token().stop_requested()) {
                bool failed = false;
                try {
                    // The current deadline set is shared by all identical
                    // Service Workers and claimed atomically in Redis.
                    co_await latest::expireStale(redis);
                    co_await latest::expireConnections(redis);
                    const auto deadline = co_await latest::nextDeadline(redis);
                    const auto now = deadline
                        ? co_await latest::deadlineTimeMilliseconds(redis)
                        : service::message::utcNowMilliseconds();
                    const auto wait = latest::deadlineWait(now, deadline);
                    if (wait.has_value() && wait->count() == 0) {
                        continue;
                    }
                    co_await service::message::workerStreamMultiplexer().wait(
                        index,
                        service::message::WorkerStreamTask::Freshness,
                        context.get_stop_token(),
                        wait
                    );
                } catch (const std::exception& error) {
                    if (context.get_stop_token().stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "telemetry freshness maintenance failed: " << error.what();
                    failed = true;
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(250));
                }
            }
        } catch (...) {
            try {
                ready->set_exception(std::current_exception());
            } catch (...) {
            }
        }
        try {
            stopped->set_value();
        } catch (...) {
        }
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t index, const Route& route, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        const auto consumerKind = route.consumer;
        const auto group = std::string(kGroup) + ":" + std::string(route.name);
        const auto wakeTask = route.wake;
        try {
            const auto redis = context.redis();
            std::vector<std::string> streams;
            streams.push_back(consumerStream(consumerKind));
            co_await message::redis::ensureGroup(redis, streams.back(), group);
            ready->set_value();
            bool recovering = true;
            const auto consumer = service::runtime::instanceId() + ":service-" + std::to_string(index);
            static constexpr auto kPendingRecoveryInterval =
                std::chrono::milliseconds(5000);
            auto nextRecoverySweep = std::chrono::steady_clock::now();
            while (running_.load() && !context.get_stop_token().stop_requested()) {
                if (streams.empty()) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1));
                    continue;
                }
                std::vector<message::redis::StreamBatch> batches;
                bool readFailed = false;
                bool groupMissing = false;
                try {
                    if (recovering ||
                        std::chrono::steady_clock::now() >= nextRecoverySweep) {
                        batches = co_await message::redis::claimGroupMany(
                            redis,
                            streams,
                            group,
                            consumer,
                            kBatchSize
                        );
                        nextRecoverySweep = std::chrono::steady_clock::now() +
                            kPendingRecoveryInterval;
                        if (batches.empty()) {
                            // A wake may coincide with pending recovery. An empty
                            // pending set must still drain newly published work.
                            recovering = false;
                            batches = co_await message::redis::readGroupMany(
                                redis, streams, group, consumer, ">", kBatchSize);
                        }
                    } else {
                        batches = co_await message::redis::readGroupMany(
                            redis,
                            streams,
                            group,
                            consumer,
                            ">",
                            kBatchSize
                        );
                    }
                } catch (const std::exception& error) {
                    if (!running_.load() || context.get_stop_token().stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "telemetry stream read failed for service worker " << index << ": " << error.what();
                    recovering = true;
                    readFailed = true;
                    groupMissing = std::string_view(error.what()).find("NOGROUP") !=
                        std::string_view::npos;
                }
                if (groupMissing && running_.load() &&
                    !context.get_stop_token().stop_requested()) {
                    try {
                        co_await message::redis::ensureGroup(
                            redis,
                            streams.front(),
                            group
                        );
                        readFailed = false;
                    } catch (const std::exception& ensureError) {
                        LOG_ERROR << "telemetry stream group recovery failed for service worker " << index << ": " << ensureError.what();
                    }
                }
                if (!running_.load() || context.get_stop_token().stop_requested()) {
                    break;
                }
                if (readFailed) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(250));
                    continue;
                }
                if (recovering && batches.empty()) {
                    recovering = false;
                    continue;
                }
                if (batches.empty()) {
                    co_await service::message::workerStreamMultiplexer().wait(
                        index,
                        wakeTask,
                        context.get_stop_token(),
                        kPendingRecoveryInterval
                    );
                    continue;
                }
                bool failed = false;
                for (const auto& batch : batches) {
                    try {
                        co_await apply(context, consumerKind, batch.messages);
                        if (consumerKind == Consumer::Dispatch) {
                            // 本组件消费者在本 Worker 内唤醒；告警消费者由 kFanoutScript 经 Redis 唤醒流通知。
                            for (const auto& consumerRoute : kRoutes) {
                                if (consumerRoute.consumer != Consumer::Dispatch) {
                                    message::workerStreamMultiplexer().signal(consumerRoute.wake);
                                }
                            }
                        }
                        co_await message::redis::acknowledgeAndDeleteMany(redis, batch.stream, group, batch.messages);
                    } catch (const std::exception& error) {
                        LOG_ERROR << "telemetry persistence failed for service worker " << index << ": " << error.what();
                        recovering = true;
                        failed = true;
                    }
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(250));
                }
            }
        } catch (...) {
            try {
                ready->set_exception(std::current_exception());
            } catch (...) {
            }
        }
        try {
            stopped->set_value();
        } catch (...) {
        }
    }

    ruvia::web_worker_handle worker_;
    std::vector<std::shared_future<void>> stopped_;
    std::size_t workerIndex_ = 0;
    std::size_t serviceWorkerCount_ = 0;
    std::size_t collectorWorkerCount_ = 0;
    std::atomic_bool running_{ false };
};

} // namespace service::telemetry
