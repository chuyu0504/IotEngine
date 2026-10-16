#pragma once

#include <algorithm>
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
#include "service/features/alert/alert.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"
#include "service/middleware/log.h"

namespace service::alert {

// 每个 Service Worker 运行相同的两项告警任务，工作经 Redis 消费组与认领脚本分配：
// 1. 消费 telemetry 的告警复制流（service::message::telemetry::kAlertsStream）并评估规则；
// 2. 维护：启动时刷新元数据并投递积压的告警事件，之后消费设备配置变更通知刷新元数据、认领到期离线截止。
class AlertRuntime final {
  public:
    AlertRuntime() = default;
    AlertRuntime(const AlertRuntime&) = delete;
    AlertRuntime& operator=(const AlertRuntime&) = delete;

    ~AlertRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex, std::size_t serviceWorkerCount) {
        if (running_.exchange(true)) {
            return;
        }
        worker_ = std::move(worker);
        workerIndex_ = workerIndex;
        serviceWorkerCount_ = serviceWorkerCount;
        if (!worker_.valid() || serviceWorkerCount_ == 0 || workerIndex_ >= serviceWorkerCount_) {
            running_.store(false);
            worker_ = {};
            throw std::runtime_error("alert runtime requires Service Workers");
        }
        std::vector<std::future<void>> readiness;
        try {
            for (const auto task : { Task::Maintenance, Task::Telemetry }) {
                auto ready = std::make_shared<std::promise<void>>();
                auto stopped = std::make_shared<std::promise<void>>();
                readiness.push_back(ready->get_future());
                stopped_.push_back(stopped->get_future().share());
                const auto posted = worker_.post([this, task, ready, stopped](ruvia::web_worker_context& context) {
                    return task == Task::Maintenance ? maintain(context, ready, stopped) : consumeTelemetry(context, ready, stopped);
                });
                if (!posted.accepted()) {
                    stopped->set_value();
                    stop();
                    throw std::runtime_error("service worker rejected alert runtime");
                }
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
                             service::message::workerStreamMultiplexer().signal(service::message::WorkerStreamTask::TelemetryAlerts);
                             service::message::workerStreamMultiplexer().signal(service::message::WorkerStreamTask::FreshnessAlerts);
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
    enum class Task { Maintenance,
                      Telemetry };

    // 沿用 telemetry 时期的组名，升级时已投递未确认的条目仍由同一消费组认领。
    static constexpr std::string_view kTelemetryGroup{ "iot-engine:telemetry-persistence:alerts" };
    static constexpr std::string_view kMetadataGroup{ "iot-engine:alert-metadata" };
    static constexpr std::size_t kBatchSize = 256;
    static constexpr auto kPendingRecoveryInterval = std::chrono::milliseconds(5000);
    static constexpr auto kFailureBackoff = std::chrono::milliseconds(250);

    struct ReadState final {
        bool recovering{ true };
        std::chrono::steady_clock::time_point nextRecoverySweep{ std::chrono::steady_clock::now() };
    };

    // 恢复期或到达认领周期时先认领本消费者与超时的待确认条目，否则读取新条目。
    template <typename Redis>
    static ruvia::task<std::vector<service::message::redis::StreamBatch>> readBatches(
        const Redis& redis,
        const std::vector<std::string>& streams,
        std::string_view group,
        std::string_view consumer,
        ReadState& state
    ) {
        if (state.recovering || std::chrono::steady_clock::now() >= state.nextRecoverySweep) {
            auto claimed = co_await service::message::redis::claimGroupMany(redis, streams, group, consumer, kBatchSize);
            state.nextRecoverySweep = std::chrono::steady_clock::now() + kPendingRecoveryInterval;
            if (!claimed.empty()) {
                co_return claimed;
            }
            state.recovering = false;
        }
        co_return co_await service::message::redis::readGroupMany(redis, streams, group, consumer, ">", kBatchSize);
    }

    // 读取失败后的恢复：消费组被删除时重建；返回 false 表示仍需退避。
    template <typename Redis>
    ruvia::task<bool> recoverRead(const Redis& redis, std::string_view stream, std::string_view group, bool groupMissing) {
        if (!groupMissing || !running_.load()) {
            co_return false;
        }
        try {
            co_await service::message::redis::ensureGroup(redis, stream, group);
            co_return true;
        } catch (const std::exception& error) {
            LOG_ERROR << "alert stream group recovery failed for " << stream << ": " << error.what();
        }
        co_return false;
    }

    std::string consumerName() const {
        return service::runtime::instanceId() + ":service-" + std::to_string(workerIndex_);
    }

    ruvia::task<void> consumeTelemetry(ruvia::web_worker_context& context, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        try {
            const auto redis = context.redis();
            const std::vector<std::string> streams{ std::string(service::message::telemetry::kAlertsStream) };
            co_await service::message::redis::ensureGroup(redis, streams.front(), kTelemetryGroup);
            ready->set_value();
            const auto consumer = consumerName();
            ReadState state;
            while (running_.load() && !context.get_stop_token().stop_requested()) {
                std::vector<service::message::redis::StreamBatch> batches;
                bool readFailed = false;
                bool groupMissing = false;
                try {
                    batches = co_await readBatches(redis, streams, kTelemetryGroup, consumer, state);
                } catch (const std::exception& error) {
                    if (!running_.load() || context.get_stop_token().stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "alert telemetry stream read failed for service worker " << workerIndex_ << ": " << error.what();
                    state.recovering = true;
                    readFailed = true;
                    groupMissing = std::string_view(error.what()).find("NOGROUP") != std::string_view::npos;
                }
                if (readFailed && co_await recoverRead(redis, streams.front(), kTelemetryGroup, groupMissing)) {
                    readFailed = false;
                }
                if (!running_.load() || context.get_stop_token().stop_requested()) {
                    break;
                }
                if (readFailed) {
                    (void)co_await ruvia::sleep_for(context.worker(), kFailureBackoff);
                    continue;
                }
                if (batches.empty()) {
                    co_await service::message::workerStreamMultiplexer().wait(
                        workerIndex_,
                        service::message::WorkerStreamTask::TelemetryAlerts,
                        context.get_stop_token(),
                        kPendingRecoveryInterval
                    );
                    continue;
                }
                bool failed = false;
                for (const auto& batch : batches) {
                    try {
                        // 整批评估成功后一次 XACK+XDEL；失败时整批保留待确认，由认领重放。
                        co_await AlertEvaluationService::evaluateTelemetry(context, batch.messages);
                        co_await service::message::redis::acknowledgeAndDeleteMany(redis, batch.stream, kTelemetryGroup, batch.messages);
                    } catch (const std::exception& error) {
                        LOG_ERROR << "alert telemetry evaluation failed for service worker " << workerIndex_ << ": " << error.what();
                        state.recovering = true;
                        failed = true;
                    }
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(context.worker(), kFailureBackoff);
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

    ruvia::task<void> maintain(ruvia::web_worker_context& context, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        try {
            const auto redis = context.redis();
            const std::vector<std::string> streams{ std::string(service::message::alert::kMetadataChangesStream) };
            co_await metadata::refresh(context);
            co_await AlertEvaluationService::drainOutbox(context);
            co_await service::message::redis::ensureGroup(redis, streams.front(), kMetadataGroup);
            ready->set_value();
            const auto consumer = consumerName();
            ReadState state;
            while (running_.load() && !context.get_stop_token().stop_requested()) {
                bool failed = false;
                bool groupMissing = false;
                bool changed = false;
                std::optional<std::chrono::milliseconds> wait;
                try {
                    // 一批设备配置变更只刷新一次元数据；刷新提交后才确认通知。
                    const auto batches = co_await readBatches(redis, streams, kMetadataGroup, consumer, state);
                    if (!batches.empty()) {
                        co_await metadata::refresh(context);
                        for (const auto& batch : batches) {
                            co_await service::message::redis::acknowledgeAndDeleteMany(redis, batch.stream, kMetadataGroup, batch.messages);
                        }
                        changed = true;
                    }
                    // 离线截止是共享可用性队列；认领脚本保证每个到期成员只由一个 Worker 评估。
                    co_await AlertEvaluationService::evaluateOfflineDue(context);
                    const auto deadline = co_await metadata::nextOfflineDeadline(redis);
                    wait = kPendingRecoveryInterval;
                    if (deadline) {
                        const auto remaining = *deadline - service::message::utcNowMilliseconds();
                        wait = std::min(*wait, std::chrono::milliseconds(std::max<std::int64_t>(remaining, 0)));
                    }
                } catch (const std::exception& error) {
                    if (!running_.load() || context.get_stop_token().stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "alert maintenance failed for service worker " << workerIndex_ << ": " << error.what();
                    state.recovering = true;
                    failed = true;
                    groupMissing = std::string_view(error.what()).find("NOGROUP") != std::string_view::npos;
                }
                if (failed && co_await recoverRead(redis, streams.front(), kMetadataGroup, groupMissing)) {
                    continue;
                }
                if (!running_.load() || context.get_stop_token().stop_requested()) {
                    break;
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(context.worker(), kFailureBackoff);
                    continue;
                }
                if (changed || wait->count() == 0) {
                    continue;
                }
                co_await service::message::workerStreamMultiplexer().wait(
                    workerIndex_,
                    service::message::WorkerStreamTask::FreshnessAlerts,
                    context.get_stop_token(),
                    wait
                );
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
    std::atomic_bool running_{ false };
};

} // namespace service::alert
