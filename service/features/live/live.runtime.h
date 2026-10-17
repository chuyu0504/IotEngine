#pragma once

#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/features/live/live.service.h"
#include "service/middleware/log.h"
#include "service/utils/redis.h"
#include "service/utils/worker_task.h"

namespace service::live {

// Durable runtime-config relay and Edge lease deadline driver. API subscribers
// own their Pub/Sub connections; this runtime never reads their notifications.
class LiveChangeRuntime final {
  public:
    explicit LiveChangeRuntime(std::size_t collectorWorkerCount)
        : collectorWorkerCount_(collectorWorkerCount) {
        if (collectorWorkerCount_ == 0) {
            throw std::invalid_argument("live runtime requires collector workers");
        }
    }

    void start(ruvia::web_worker_handle worker) {
        stop_ = std::make_unique<ruvia::stop_source>();
        try {
            service::utils::startWorkerTask(worker, stop_->token(), stopped_,
                [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                    return run(context, ready, stop);
                });
            service::utils::startWorkerTask(worker, stop_->token(), leasesStopped_,
                [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                    return expireSessions(context, ready, stop);
                });
        } catch (...) {
            this->stop();
            throw;
        }
    }

    void stop() {
        if (!stop_) {
            return;
        }
        service::utils::stopWorkerTask(*stop_, stopped_);
        service::utils::stopWorkerTask(*stop_, leasesStopped_);
        stop_.reset();
    }

    ~LiveChangeRuntime() { stop(); }

  private:
    ruvia::task<void> expireSessions(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        ready.set_value();
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        while (!stop.stop_requested()) {
            std::chrono::milliseconds wait;
            try {
                wait = co_await expireDueSessions(redis);
            } catch (const std::exception& error) {
                if (stop.stop_requested()) {
                    break;
                }
                LOG_ERROR << "edge session deadline: " << error.what();
                wait = std::chrono::seconds(1);
            }
            if (wait.count() > 0) {
                (void)co_await ruvia::sleep_for(context.worker(), wait, stop);
            }
        }
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        const std::vector<std::string> streams{ std::string(service::message::live::kChanges) };
        std::vector<std::string> cursors{ co_await service::utils::redis::lastEntryId(redis, streams.front()) };
        ready.set_value();
        while (!stop.stop_requested()) {
            bool failed = false;
            try {
                const auto batches = co_await service::utils::redis::readStreamsBlocking(
                    redis, streams, cursors, stop, std::chrono::seconds(30), 256);
                for (const auto& batch : batches) {
                    for (const auto& change : batch.messages) {
                        if (change.get(service::message::live::kTopicField) == service::message::live::topic::kRuntimeConfig) {
                            co_await relayConfiguration(context, change.id, collectorWorkerCount_);
                        }
                        cursors.front() = change.id;
                    }
                }
            } catch (const std::exception& error) {
                if (stop.stop_requested()) {
                    break;
                }
                LOG_ERROR << "live configuration relay: " << error.what();
                failed = true;
            }
            if (failed) {
                (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1), stop);
            }
        }
    }

    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
    std::future<void> leasesStopped_;
    std::size_t collectorWorkerCount_;
};

} // namespace service::live
