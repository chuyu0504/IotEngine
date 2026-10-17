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
#include "service/utils/log.h"
#include "service/utils/worker_task.h"

namespace service::messaging::outbox {

class OutboxRuntime final : private OutboxService {
  public:
    OutboxRuntime(observability::RuntimeDiagnostics& observability, std::size_t collectorWorkerCount, std::size_t serviceWorkerCount, ruvia::db_config database, Policy policy = {})
        : OutboxService(observability, collectorWorkerCount, serviceWorkerCount, policy),
          notifier_(std::move(database), [this] {
              (void)worker_.post([this](ruvia::web_worker_context&) -> ruvia::task<void> {
                  wake();
                  co_return;
              });
          }) {}

    OutboxRuntime(const OutboxRuntime&) = delete;
    OutboxRuntime& operator=(const OutboxRuntime&) = delete;

    ~OutboxRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (running_.exchange(true)) {
            return;
        }
        worker_ = worker;
        stopSource_ = std::make_unique<ruvia::stop_source>();
        try {
            service::utils::startWorkerTask(worker, stopSource_->token(), stopped_, [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                return run(context, ready, stop);
            });
            notifier_.start();
        } catch (...) {
            stop();
            throw;
        }
    }

    void stop() noexcept {
        if (!running_.exchange(false)) {
            return;
        }
        notifier_.stop();
        if (stopSource_) {
            service::utils::stopWorkerTask(*stopSource_, stopped_);
        }
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
        if (running_.load() && wakeChannel_) {
            (void)wakeChannel_->sender.send(1);
        }
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        auto [sender, receiver] = ruvia::make_channel<int>(context.worker(), { .capacity_ = 1 });
        wakeChannel_ = std::make_unique<WakeChannel>(WakeChannel{ std::move(sender), std::move(receiver) });
        ready.set_value();
        auto nextLease = Clock::now();
        auto nextReceiptCleanup = Clock::now();
        std::optional<Clock::time_point> nextDispatch = Clock::now();
        std::optional<Clock::time_point> metricsRetry = Clock::now();
        while (!stop.stop_requested()) {
            if (policy_.receiptRetentionDays > 0 && Clock::now() >= nextReceiptCleanup) {
                try {
                    nextReceiptCleanup = co_await cleanupReceipts(context);
                } catch (const std::exception& error) {
                    observability_.incrementCounter("iot_engine_outbox_receipt_cleanup_failures_total");
                    LOG_ERROR << "outbox receipt cleanup failed: " << error.what();
                    nextReceiptCleanup = Clock::now() + std::chrono::seconds(1);
                }
            }
            if (nextDispatch && Clock::now() >= *nextDispatch) {
                try {
                    observability_.incrementCounter("iot_engine_outbox_dispatch_checks_total");
                    const auto dispatched = co_await dispatch(context);
                    nextDispatch = dispatched ? std::optional{ Clock::now() } : co_await nextAvailable(context);
                    metricsRetry = Clock::now();
                } catch (const std::exception& error) {
                    observability_.incrementCounter("iot_engine_outbox_dispatch_failures_total");
                    LOG_ERROR << "outbox dispatch failed: " << error.what();
                    nextDispatch = Clock::now() + std::chrono::milliseconds(250);
                }
            }
            if (metricsRetry && Clock::now() >= *metricsRetry) {
                try {
                    co_await collectMetrics(context);
                    metricsRetry.reset();
                } catch (const std::exception& error) {
                    observability_.incrementCounter("iot_engine_metrics_collection_failures_total");
                    LOG_ERROR << "metrics collection failed: " << error.what();
                    metricsRetry = Clock::now() + std::chrono::seconds(1);
                }
            }
            if (const auto age = nextAgeAlert(); age && Clock::now() >= *age) {
                updateAgeAlert();
                // The threshold crossing is a real state transition, not a
                // periodic count/min query or repeated business snapshot.
                try {
                    co_await publishDiagnostics(context);
                } catch (const std::exception& error) {
                    LOG_ERROR << "outbox age alert: " << error.what();
                }
            }
            if (Clock::now() >= nextLease) {
                observability_.setGauge("iot_engine_outbox_listener_connected", notifier_.connected() ? 1 : 0);
                try {
                    co_await publishDiagnostics(context);
                } catch (const std::exception& error) {
                    LOG_ERROR << "worker health lease: " << error.what();
                }
                nextLease = Clock::now() + std::chrono::seconds(5);
            }
            auto deadline = nextLease;
            if (policy_.receiptRetentionDays > 0) {
                deadline = std::min(deadline, nextReceiptCleanup);
            }
            if (nextDispatch) {
                deadline = std::min(deadline, *nextDispatch);
            }
            if (metricsRetry) {
                deadline = std::min(deadline, *metricsRetry);
            }
            if (const auto age = nextAgeAlert()) {
                deadline = std::min(deadline, *age);
            }
            const auto delay = std::max(std::chrono::milliseconds(1), std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()));
            const auto notification = co_await wakeChannel_->receiver.receive_for(delay, stop);
            if (notification.has_value()) {
                nextDispatch = Clock::now();
            }
        }
        try {
            co_await retireDiagnostics(context);
        } catch (const std::exception& error) {
            LOG_WARN << "worker health lease retirement: " << error.what();
        }
    }

    PostgresNotifier notifier_;
    std::unique_ptr<ruvia::stop_source> stopSource_;
    std::unique_ptr<WakeChannel> wakeChannel_;
    ruvia::web_worker_handle worker_;
    std::future<void> stopped_;
    std::atomic_bool running_{ false };
};

} // namespace service::messaging::outbox
