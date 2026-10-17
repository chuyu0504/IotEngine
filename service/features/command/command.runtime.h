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
#include "service/features/command/command.service.h"
#include "service/utils/log.h"
#include "service/utils/redis.h"
#include "service/utils/worker_task.h"

namespace service::command {

class CommandProcessingRuntime final {
  public:
    CommandProcessingRuntime() = default;
    CommandProcessingRuntime(const CommandProcessingRuntime&) = delete;
    CommandProcessingRuntime& operator=(const CommandProcessingRuntime&) = delete;

    ~CommandProcessingRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (stopped_.valid()) {
            return;
        }
        if (!worker.valid()) {
            throw std::runtime_error("command result runtime requires north and collector workers");
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
    static constexpr std::string_view kGroup = "iot-engine:command-result";

    ruvia::task<void> run(ruvia::web_worker_context& context, utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto& identity = context.worker_state<std::optional<common::WorkerIdentity>>().value();
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        const auto results = message::commandResultStream();
        const std::vector<std::string> streams{ results,
                                                message::workerWakeStream(std::nullopt, identity.instanceId),
                                                message::workerWakeStream(identity.index, identity.instanceId) };
        for (const auto& stream : streams) {
            co_await utils::redis::ensureGroup(redis, stream, kGroup);
        }
        co_await utils::redis::withConsumerNotifications(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, context.worker(), streams, kGroup, stop, [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
            ready.set_value();
            const auto consumer = identity.instanceId + ":service-" + std::to_string(identity.index);
            bool recovering = true;
            auto dispatchAt = std::optional(std::chrono::steady_clock::now());
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    if (dispatchAt && std::chrono::steady_clock::now() >= *dispatchAt) {
                        co_await CommandDispatchService::dispatch(context);
                        const auto delay = co_await CommandDispatchService::nextDispatchDelay(context);
                        dispatchAt = delay ? std::optional(std::chrono::steady_clock::now() + *delay) : std::nullopt;
                    }
                    const auto wait = dispatchAt
                        ? std::optional(std::max(std::chrono::milliseconds(0), std::chrono::duration_cast<std::chrono::milliseconds>(*dispatchAt - std::chrono::steady_clock::now())))
                        : std::nullopt;
                    auto batches = co_await utils::redis::consumeGroup(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, streams, kGroup, consumer, recovering, notifications, stop, wait);
                    for (const auto& batch : batches) {
                        if (batch.stream == results) {
                            co_await CommandResultService::project(context, batch.messages);
                            co_await utils::redis::acknowledgeAndDeleteMany(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, batch.stream, kGroup, batch.messages);
                        } else {
                            for (const auto& item : batch.messages) {
                                if (item.get("task") == message::workerStreamTaskName(message::WorkerStreamTask::CommandResult)) {
                                    dispatchAt = std::chrono::steady_clock::now();
                                }
                                // Wake streams fan out to independent consumer groups.
                                co_await utils::redis::acknowledge(redis, context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, batch.stream, kGroup, item.id);
                            }
                        }
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    LOG_ERROR << "command processing failed: " << error.what();
                    recovering = true;
                    dispatchAt = std::chrono::steady_clock::now();
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

} // namespace service::command
