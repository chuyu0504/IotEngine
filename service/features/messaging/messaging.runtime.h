#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <optional>

#include <ruvia/core/channel.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/web/web_worker.h>

#include "service/features/messaging/messaging.service.h"
#include "service/features/messaging/postgres_notifier/postgres_notifier.transport.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"
#include "service/middleware/log.h"

namespace service::messaging::outbox {
class OutboxRuntime final : private OutboxService {
  public:
    OutboxRuntime(observability::RuntimeDiagnostics& observability, std::size_t collectorWorkerCount,
            std::size_t serviceWorkerCount,
            ruvia::db_config database,
            Policy policy = {})
        : OutboxService(observability, collectorWorkerCount, serviceWorkerCount, policy),
          notifier_(std::move(database), [this] {
              (void)worker_.post([this](ruvia::web_worker_context&) -> ruvia::task<void> { wake(); co_return; });
          }) {}
    OutboxRuntime(const OutboxRuntime&) = delete;
    OutboxRuntime& operator=(const OutboxRuntime&) = delete;
    ~OutboxRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (running_.exchange(true)) return;
        worker_ = worker;
        stopSource_ = std::make_unique<ruvia::stop_source>();
        auto ready = std::make_shared<std::promise<void>>();
        auto done = std::make_shared<std::promise<void>>();
        auto readiness = ready->get_future();
        stopped_ = done->get_future();
        if (!worker_.post([this, ready, done](ruvia::web_worker_context& context) {
                return run(context, ready, done);
            }).accepted()) {
            done->set_value();
            running_.store(false);
            throw std::runtime_error("service rejected outbox startup");
        }
        try { readiness.get(); notifier_.start(); }
        catch (...) { stop(); throw; }
    }

    void stop() noexcept {
        if (!running_.exchange(false))
            return;
        notifier_.stop();
        if (stopSource_) stopSource_->request_stop();
        if (stopped_.valid()) stopped_.wait();
        wakeChannel_.reset();
        stopSource_.reset();
    }

  private:
    using Clock = std::chrono::steady_clock;
    struct WakeChannel {
        ruvia::channel_sender<int> sender;
        ruvia::channel_receiver<int> receiver;
    };

    void wake() {
        if (!running_.load()) return;
        if (wakeChannel_) (void)wakeChannel_->sender.send(1);
        // Commands use the same committed-work hint, including startup and
        // reconnect catchup; their separate timer tracks actual attempt deadlines.
        service::messaging::workerStreamMultiplexer().signal(
            service::message::WorkerStreamTask::CommandResult);
    }

    ruvia::task<void> run(ruvia::web_worker_context& context,
                          std::shared_ptr<std::promise<void>> ready,
                          std::shared_ptr<std::promise<void>> stopped) {
        try {
            auto [sender, receiver] = ruvia::make_channel<int>(context.worker(), {.capacity_ = 1});
            auto wakeChannel = std::make_shared<WakeChannel>(
                WakeChannel{std::move(sender), std::move(receiver)});
            wakeChannel_ = wakeChannel;
            const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stopSource_->token());
            ready->set_value();
            auto nextMetrics = std::chrono::steady_clock::now();
            auto nextReceiptCleanup = std::chrono::steady_clock::now();
            std::optional<Clock::time_point> nextDispatch = Clock::now();
            while (!stop.stop_requested()) {
                if (policy_.receiptRetentionDays > 0 &&
                    std::chrono::steady_clock::now() >= nextReceiptCleanup) {
                    try {
                        co_await cleanupReceipts(context);
                    } catch (const std::exception& error) {
                        observability_.incrementCounter(
                            "iot_engine_outbox_receipt_cleanup_failures_total");
                        LOG_ERROR << "outbox receipt cleanup failed: " << error.what();
                    }
                    nextReceiptCleanup =
                        std::chrono::steady_clock::now() + std::chrono::hours(1);
                }
                if (std::chrono::steady_clock::now() >= nextMetrics) {
                    observability_.setGauge("iot_engine_outbox_listener_connected", notifier_.connected() ? 1 : 0);
                    try {
                        co_await collectMetrics(context);
                    } catch (const std::exception& error) {
                        observability_.incrementCounter("iot_engine_metrics_collection_failures_total");
                        LOG_ERROR << "metrics collection failed: " << error.what();
                    }
                    nextMetrics = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                }
                if (nextDispatch && Clock::now() >= *nextDispatch) {
                    try {
                        observability_.incrementCounter("iot_engine_outbox_dispatch_checks_total");
                        if (co_await dispatch(context)) {
                            nextDispatch = Clock::now();
                            continue;
                        }
                        nextDispatch = co_await nextAvailable(context);
                    } catch (const std::exception& error) {
                        observability_.incrementCounter("iot_engine_outbox_dispatch_failures_total");
                        LOG_ERROR << "outbox dispatch failed: " << error.what();
                        nextDispatch = Clock::now() + std::chrono::milliseconds(250);
                    }
                }
                auto deadline = nextMetrics;
                if (policy_.receiptRetentionDays > 0)
                    deadline = std::min(deadline, nextReceiptCleanup);
                if (nextDispatch) deadline = std::min(deadline, *nextDispatch);
                const auto delay = std::max(std::chrono::milliseconds(1),
                    std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()));
                const auto notification = co_await wakeChannel->receiver.receive_for(delay, stop);
                if (notification.has_value()) nextDispatch = Clock::now();
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

    PostgresNotifier notifier_;
    std::unique_ptr<ruvia::stop_source> stopSource_;
    std::shared_ptr<WakeChannel> wakeChannel_;
    ruvia::web_worker_handle worker_;
    std::future<void> stopped_;
    std::atomic_bool running_{false};
};

} // namespace service::messaging::outbox
