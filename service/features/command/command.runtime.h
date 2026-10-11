#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/features/command/command.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"

namespace service::command {

class CommandProcessingRuntime final {
  public:
    CommandProcessingRuntime() = default;
    CommandProcessingRuntime(const CommandProcessingRuntime&) = delete;
    CommandProcessingRuntime& operator=(const CommandProcessingRuntime&) = delete;

    ~CommandProcessingRuntime() { stop(); }

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
            throw std::runtime_error("command result runtime requires north and collector workers");
        }
        std::vector<std::future<void>> readiness;
        auto ready = std::make_shared<std::promise<void>>();
        auto stopped = std::make_shared<std::promise<void>>();
        readiness.push_back(ready->get_future());
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
            throw std::runtime_error("service worker rejected command result consumer");
        }
        try {
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
            const auto worker = worker_;
            const auto wakeAccepted = worker.post([](ruvia::web_worker_context&) -> ruvia::task<void> {
                                                service::message::workerStreamMultiplexer().signal(
                                                    service::message::WorkerStreamTask::CommandResult
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
    static constexpr std::string_view kGroup = "iot-engine:command-result";
    static constexpr std::size_t kBatchSize = 256;

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t index, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        try {
            const auto redis = context.redis();
            std::vector<std::string> streams;
            streams.push_back(message::commandResultStream());
            co_await message::redis::ensureGroup(
                redis,
                streams.back(),
                kGroup
            );
            bool recovering = true;
            ready->set_value();
            const auto consumer = service::runtime::instanceId() + ":service-" + std::to_string(index);
            static constexpr auto kPendingRecoveryInterval =
                std::chrono::milliseconds(5000);
            auto nextRecoverySweep = std::chrono::steady_clock::now();
            while (running_.load() && !context.get_stop_token().stop_requested()) {
                bool dispatchFailed = false;
                std::optional<std::chrono::milliseconds> dispatchDelay;
                try {
                    co_await repository::dispatch(context);
                    dispatchDelay = co_await repository::nextDispatchDelay(context);
                } catch (const std::exception& error) {
                    std::cerr << "command dispatch failed: " << error.what() << '\n';
                    dispatchFailed = true;
                }
                if (streams.empty()) {
                    (void)co_await ruvia::sleep_for(
                        context.worker(),
                        dispatchFailed ? std::chrono::milliseconds(250)
                                       : std::chrono::seconds(1)
                    );
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
                            kGroup,
                            consumer,
                            kBatchSize
                        );
                        nextRecoverySweep = std::chrono::steady_clock::now() +
                            kPendingRecoveryInterval;
                    } else {
                        batches = co_await message::redis::readGroupMany(
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
                    std::cerr << "command result stream read failed for service worker " << index
                              << ": " << error.what() << '\n';
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
                            kGroup
                        );
                        readFailed = false;
                    } catch (const std::exception& ensureError) {
                        std::cerr << "command result stream group recovery failed for service worker "
                                  << index << ": " << ensureError.what() << '\n';
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
                    if (dispatchFailed) {
                        (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(250));
                    } else {
                        auto recoveryWait =
                            std::optional<std::chrono::milliseconds>(
                                kPendingRecoveryInterval
                            );
                        if (dispatchDelay && *dispatchDelay < *recoveryWait) {
                            recoveryWait = dispatchDelay;
                        }
                        co_await service::message::workerStreamMultiplexer().wait(
                            index,
                            service::message::WorkerStreamTask::CommandResult,
                            context.get_stop_token(),
                            recoveryWait
                        );
                    }
                    continue;
                }
                bool failed = false;
                for (const auto& batch : batches) {
                    try {
                        if (batch.messages.empty()) {
                            continue;
                        }
                        co_await CommandResultService::project(context, batch.messages);
                        co_await message::redis::acknowledgeAndDeleteMany(
                            context.redis(),
                            batch.stream,
                            kGroup,
                            batch.messages
                        );
                    } catch (const std::exception& error) {
                        std::cerr << "command result projection failed for service worker "
                                  << index << ": " << error.what() << '\n';
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
    std::shared_future<void> stopped_;
    std::size_t workerIndex_ = 0;
    std::size_t serviceWorkerCount_ = 0;
    std::size_t collectorWorkerCount_ = 0;
    std::atomic_bool running_{ false };
};

} // namespace service::command
