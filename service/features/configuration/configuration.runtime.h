#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/features/configuration/configuration.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"

namespace service::runtime {

// PostgreSQL is the configuration source of truth. Every Service Worker runs
// the same reconciliation loop and independently maintains its local runtime.
class Reconciler final {
  public:
    Reconciler() = default;
    Reconciler(const Reconciler&) = delete;
    Reconciler& operator=(const Reconciler&) = delete;

    ~Reconciler() { stop(); }

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
            worker_ = {};
            throw std::runtime_error(
                "runtime reconciler requires valid Service and Collector Workers"
            );
        }
        auto ready = std::make_shared<std::promise<void>>();
        auto stopped = std::make_shared<std::promise<void>>();
        auto readiness = ready->get_future();
        stopped_ = stopped->get_future().share();
        const auto posted = worker_.post(
            [this, ready, stopped](ruvia::web_worker_context& context) {
                return run(context, workerIndex_, ready, stopped);
            }
        );
        if (!posted.accepted()) {
            stopped->set_value();
            running_.store(false);
            worker_ = {};
            stopped_ = {};
            throw std::runtime_error("service worker rejected runtime reconciler");
        }
        try {
            readiness.get();
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
            const auto worker = worker_;
            const auto wakeAccepted = worker.post([](ruvia::web_worker_context&) -> ruvia::task<void> {
                                                service::message::workerStreamMultiplexer().signal(
                                                    service::message::WorkerStreamTask::Reconciler
                                                );
                                                co_return;
                                            })
                                          .accepted();
            if (!wakeAccepted) {
                // A closed worker already propagates its stop token to the task.
            }
        }
        if (stopped_.valid()) {
            stopped_.wait();
        }
        stopped_ = {};
        worker_ = {};
    }

  private:
    static constexpr std::string_view kGroup{ "iot-engine:runtime-reconciler" };
    static constexpr std::size_t kBatchSize = 256;
    static constexpr std::size_t kCoalesceLimit = 4096;
    static constexpr auto kCoalesceWindow = std::chrono::milliseconds(25);

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t index, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        try {
            const auto redis = context.redis();
            std::vector<std::string> streams;
            streams.push_back(service::message::configuration::runtimeChangesStream());
            co_await service::message::redis::ensureGroup(
                redis,
                streams.back(),
                kGroup
            );
            ready->set_value();
            bool recovering = true;
            const auto consumer = service::runtime::instanceId() + ":service-" + std::to_string(index);
            static constexpr auto kPendingRecoveryInterval =
                std::chrono::milliseconds(5000);
            auto nextRecoverySweep = std::chrono::steady_clock::now();
            std::optional<std::chrono::steady_clock::time_point> cleanupDeadline =
                std::chrono::steady_clock::now();
            while (running_.load() && !context.get_stop_token().stop_requested()) {
                if (!recovering && cleanupDeadline &&
                    std::chrono::steady_clock::now() >= *cleanupDeadline) {
                    try {
                        co_await service::collector::config::cleanupExpiredSnapshots(
                            redis,
                            service::message::utcNowMilliseconds()
                        );
                        cleanupDeadline.reset();
                    } catch (const std::exception& error) {
                        std::cerr << "runtime config cleanup failed: " << error.what() << '\n';
                        cleanupDeadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(250);
                    }
                    continue;
                }
                std::vector<service::message::redis::StreamBatch> batches;
                bool readFailed = false;
                bool groupMissing = false;
                try {
                    if (recovering ||
                        std::chrono::steady_clock::now() >= nextRecoverySweep) {
                        batches = co_await service::message::redis::claimGroupMany(
                            redis,
                            streams,
                            kGroup,
                            consumer,
                            kBatchSize
                        );
                        nextRecoverySweep = std::chrono::steady_clock::now() +
                            kPendingRecoveryInterval;
                    } else {
                        batches = co_await service::message::redis::readGroupMany(
                            redis,
                            streams,
                            kGroup,
                            consumer,
                            ">",
                            kBatchSize
                        );
                    }
                } catch (const std::exception& error) {
                    if (!running_.load() || context.get_stop_token().stop_requested()) {
                        break;
                    }
                    std::cerr << "runtime config stream read failed: " << error.what() << '\n';
                    recovering = true;
                    readFailed = true;
                    groupMissing = std::string_view(error.what()).find("NOGROUP") !=
                        std::string_view::npos;
                }
                if (groupMissing && running_.load() &&
                    !context.get_stop_token().stop_requested()) {
                    try {
                        co_await service::message::redis::ensureGroup(
                            redis,
                            streams.front(),
                            kGroup
                        );
                        readFailed = false;
                    } catch (const std::exception& ensureError) {
                        std::cerr << "runtime config stream group recovery failed: "
                                  << ensureError.what() << '\n';
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
                    std::optional<std::chrono::milliseconds> timeout =
                        kPendingRecoveryInterval;
                    if (cleanupDeadline) {
                        const auto untilCleanup = std::max(
                            std::chrono::milliseconds(1),
                            std::chrono::duration_cast<std::chrono::milliseconds>(
                                *cleanupDeadline - std::chrono::steady_clock::now()
                            )
                        );
                        timeout = std::min(*timeout, untilCleanup);
                    }
                    co_await service::message::workerStreamMultiplexer().wait(
                        index,
                        service::message::WorkerStreamTask::Reconciler,
                        context.get_stop_token(),
                        timeout
                    );
                    continue;
                }
                bool failed = false;
                try {
                    if (!recovering) {
                        // Absorb the rest of a CRUD burst before taking the global projection
                        // lock. Recovery already has a durable backlog and needs no delay.
                        (void)co_await ruvia::sleep_for(context.worker(), kCoalesceWindow);
                    }
                    std::size_t messageCount = 0;
                    for (const auto& batch : batches) {
                        messageCount += batch.messages.size();
                    }
                    while (!recovering && messageCount < kCoalesceLimit) {
                        const auto remaining = kCoalesceLimit - messageCount;
                        auto next = co_await service::message::redis::readGroupMany(
                            redis,
                            streams,
                            kGroup,
                            consumer,
                            ">",
                            std::min(kBatchSize, remaining)
                        );
                        if (next.empty()) {
                            break;
                        }
                        for (auto& batch : next) {
                            messageCount += batch.messages.size();
                            batches.push_back(std::move(batch));
                        }
                    }
                    std::vector<service::message::StreamMessage> received;
                    received.reserve(messageCount);
                    for (const auto& batch : batches) {
                        received.insert(received.end(), batch.messages.begin(), batch.messages.end());
                    }
                    if (co_await service::configuration::ConfigurationService::applyRuntimeChanges(context, kGroup, received)) {
                        cleanupDeadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(service::collector::config::kSnapshotGraceMilliseconds);
                    }
                    for (const auto& batch : batches) {
                        co_await service::message::redis::acknowledgeAndDeleteMany(
                            redis,
                            batch.stream,
                            kGroup,
                            batch.messages
                        );
                    }
                } catch (const std::exception& error) {
                    if (context.get_stop_token().stop_requested()) {
                        break;
                    }
                    std::cerr << "runtime reconciliation failed: " << error.what() << '\n';
                    recovering = true;
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

    ruvia::web_worker_handle worker_;
    std::shared_future<void> stopped_;
    std::size_t workerIndex_ = 0;
    std::size_t serviceWorkerCount_ = 0;
    std::size_t collectorWorkerCount_ = 0;
    std::atomic_bool running_{ false };
};

} // namespace service::runtime
