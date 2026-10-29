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
        const std::vector<std::string> streams{ message::edge::dispatch::stream(identity.index, identity.instanceId), message::workerWakeStream(std::nullopt, identity.instanceId) };
        const auto consumer = "service-edge-dispatcher-" + std::to_string(identity.index);
        for (const auto& stream : streams) {
            co_await utils::redis::ensureGroup(redis, stream, message::edge::dispatch::kGroup);
        }
        dispatcher.activate(identity.index);

        struct SessionCleanup final {
            SessionDispatcher& dispatcher;

            ~SessionCleanup() { dispatcher.clear(); }
        } cleanup{ dispatcher };

        co_await utils::redis::withConsumerNotifications(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, context.worker(), streams, message::edge::dispatch::kGroup, stop, [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
            ready.set_value();
            bool recovering = true;
            auto expiresAt = std::optional(std::chrono::steady_clock::now());
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    if (expiresAt && std::chrono::steady_clock::now() >= *expiresAt) {
                        const auto delay = co_await EdgeControlService::expirePendingIo(context);
                        expiresAt = delay ? std::optional(std::chrono::steady_clock::now() + std::max(std::chrono::milliseconds(1), *delay)) : std::nullopt;
                    }
                    const auto wait = expiresAt ? std::optional(std::max(std::chrono::milliseconds(0), std::chrono::duration_cast<std::chrono::milliseconds>(*expiresAt - std::chrono::steady_clock::now()))) : std::nullopt;
                    auto batches = co_await utils::redis::consumeGroup(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, streams, message::edge::dispatch::kGroup, consumer, recovering, notifications, stop, wait);
                    for (const auto& batch : batches) {
                        for (const auto& item : batch.messages) {
                            if (batch.stream != streams.front()) {
                                if (item.get("task") == message::workerStreamTaskName(message::WorkerStreamTask::EdgeIoDeadline)) {
                                    expiresAt = std::chrono::steady_clock::now();
                                }
                                co_await utils::redis::acknowledge(redis, identity.channels, batch.stream, message::edge::dispatch::kGroup, item.id);
                                continue;
                            }
                            const auto event = message::edge::dispatch::eventFrom(item);
                            if (event.kind == message::edge::dispatch::kNodeKind &&
                                !event.nodeId.empty() && !dispatcher.deliver(event.nodeId)) {
                                throw std::runtime_error("edge session wake failed");
                            }
                            co_await utils::redis::acknowledgeAndDelete(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, batch.stream, message::edge::dispatch::kGroup, item.id);
                        }
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "edge dispatch failed: " << error.what();
                    recovering = true;
                    expiresAt = std::chrono::steady_clock::now();
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
