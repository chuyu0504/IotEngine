#pragma once

#include <algorithm>
#include <chrono>
#include <exception>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/features/edge/gateway/gateway.service.h"
#include "service/features/edge/gateway/gateway.types.h"
#include "service/middleware/log.h"
#include "service/utils/worker_task.h"

namespace service::edge::gateway {

// Firmware storage cleanup driver, one instance per Service Worker. It sleeps
// until the earliest deadline computed by GatewayService (no deadline: until an
// event), then performs the cleanup only if it wins the storage claim. The
// driver, its wakeup source and the RPC handler all run on the owning Worker.
class FirmwareCleanupRuntime final {
  public:
    explicit FirmwareCleanupRuntime(std::filesystem::path directory)
        : directory_(std::filesystem::absolute(std::move(directory)).lexically_normal()) {}

    FirmwareCleanupRuntime(const FirmwareCleanupRuntime&) = delete;
    FirmwareCleanupRuntime& operator=(const FirmwareCleanupRuntime&) = delete;

    ~FirmwareCleanupRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (stop_) {
            return;
        }
        stop_ = std::make_unique<ruvia::stop_source>();
        try {
            service::utils::startWorkerTask(
                worker, stop_->token(), stopped_,
                [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                    return run(context, ready, stop);
                }
            );
        } catch (...) {
            stop_.reset();
            throw;
        }
    }

    void stop() {
        if (!stop_) {
            return;
        }
        service::utils::stopWorkerTask(*stop_, stopped_);
        stop_.reset();
    }

  private:
    // Backoff after a lost claim, an empty claimed run or a failure, before the
    // deadline is recomputed.
    static constexpr auto kRetry = std::chrono::seconds(5);

    ruvia::task<void> run(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        auto& signal = context.worker_state<FirmwareCleanupSignal>();
        signal.bind([this] {
            if (wake_) {
                wake_->request_stop();
            }
        });
        ready.set_value();
        try {
            std::optional<std::chrono::steady_clock::time_point> due;
            bool recompute = true;
            while (!stop.stop_requested()) {
                if (signal.consume() || recompute) {
                    recompute = false;
                    bool scheduled = false;
                    try {
                        const auto delay = co_await GatewayService::nextFirmwareCleanupDelay(context, directory_);
                        due = delay ? std::optional(std::chrono::steady_clock::now() + *delay) : std::nullopt;
                        scheduled = true;
                    } catch (const std::exception& error) {
                        LOG_ERROR << "firmware cleanup schedule failed: " << error.what();
                    }
                    if (!scheduled) {
                        recompute = true;
                        (void)co_await ruvia::sleep_for(context.worker(), kRetry, stop);
                        continue;
                    }
                    if (signal.pending()) {
                        continue;
                    }
                }
                wake_ = std::make_unique<ruvia::stop_source>();
                const auto wait = due ? std::max(*due - std::chrono::steady_clock::now(), std::chrono::steady_clock::duration::zero())
                                      : std::chrono::steady_clock::duration::max();
                (void)co_await ruvia::sleep_for(context.worker(), wait, ruvia::combine_stop_tokens(stop, wake_->token()));
                wake_.reset();
                if (stop.stop_requested()) {
                    break;
                }
                if (signal.pending() || !due || std::chrono::steady_clock::now() < *due) {
                    continue;
                }
                recompute = true;
                bool backoff = true;
                try {
                    const auto result = co_await GatewayService::cleanupFirmwares(context, directory_);
                    // A claimed run that removed nothing means the deadline is not
                    // actually collectable yet; never spin on it.
                    backoff = !result.claimed || result.removedRecords + result.removedFiles == 0;
                } catch (const std::exception& error) {
                    LOG_ERROR << "firmware cleanup failed: " << error.what();
                }
                if (backoff) {
                    (void)co_await ruvia::sleep_for(context.worker(), kRetry, stop);
                }
            }
        } catch (const std::exception& error) {
            LOG_ERROR << "firmware cleanup stopped: " << error.what();
        } catch (...) {
            LOG_ERROR << "firmware cleanup stopped: unknown error";
        }
        signal.bind({});
        wake_.reset();
    }

    const std::filesystem::path directory_;
    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
    std::unique_ptr<ruvia::stop_source> wake_;
};

} // namespace service::edge::gateway
