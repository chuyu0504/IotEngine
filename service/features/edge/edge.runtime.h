#pragma once

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
#include "service/features/edge/edge.service.h"
#include "service/utils/log.h"
#include "service/utils/redis.h"
#include "service/utils/worker_task.h"

namespace service::edge {

class DispatcherRuntime final {
  public:
    DispatcherRuntime() = default;
    DispatcherRuntime(const DispatcherRuntime&) = delete;
    DispatcherRuntime& operator=(const DispatcherRuntime&) = delete;

    ~DispatcherRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (stopped_.valid()) {
            return;
        }
        if (!worker.valid()) {
            throw std::runtime_error("edge dispatcher requires service workers");
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
    ruvia::task<void> run(ruvia::web_worker_context& context, utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto& identity = context.worker_state<std::optional<common::WorkerIdentity>>().value();
        auto& dispatcher = context.worker_state<SessionDispatcher>();
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        const std::vector<std::string> streams{ message::edge::dispatch::stream(identity.index, identity.instanceId) };
        const auto consumer = "service-edge-dispatcher-" + std::to_string(identity.index);
        co_await utils::redis::ensureGroup(redis, streams.front(), message::edge::dispatch::kGroup);
        dispatcher.activate(identity.index);

        struct SessionCleanup final {
            SessionDispatcher& dispatcher;

            ~SessionCleanup() { dispatcher.clear(); }
        } cleanup{ dispatcher };

        co_await utils::redis::withConsumerNotifications(redis, context.worker(), streams, message::edge::dispatch::kGroup, stop, [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
            ready.set_value();
            bool recovering = true;
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    auto batches = co_await utils::redis::consumeGroup(redis, streams, message::edge::dispatch::kGroup, consumer, recovering, notifications, stop);
                    for (const auto& batch : batches) {
                        for (const auto& item : batch.messages) {
                            const auto event = message::edge::dispatch::eventFrom(item);
                            if (event.kind == message::edge::dispatch::kNodeKind &&
                                !event.nodeId.empty() && !dispatcher.deliver(event.nodeId)) {
                                throw std::runtime_error("edge session wake failed");
                            }
                            co_await utils::redis::acknowledgeAndDelete(redis, batch.stream, message::edge::dispatch::kGroup, item.id);
                        }
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "edge dispatch failed: " << error.what();
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

} // namespace service::edge
