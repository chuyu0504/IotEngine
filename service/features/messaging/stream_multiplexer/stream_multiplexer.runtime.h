#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <ruvia/core/channel.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/features/messaging/messaging.transport.h"
#include "service/common/message.h"

namespace service::message {

// Each Service Worker consumes one durable wake Stream. Wake entries carry only a
// task name; business payload, consumer groups, pending recovery, and ACK semantics
// remain owned by the seven independent worker-local tasks.
class WorkerStreamMultiplexer;
inline thread_local WorkerStreamMultiplexer* localStreamMultiplexer = nullptr;

class WorkerStreamMultiplexer final {
  public:
    WorkerStreamMultiplexer() = default;
    WorkerStreamMultiplexer(const WorkerStreamMultiplexer&) = delete;
    WorkerStreamMultiplexer& operator=(const WorkerStreamMultiplexer&) = delete;

    ~WorkerStreamMultiplexer() { stop(); }

    void configure(ruvia::web_worker_handle worker, std::size_t index) {
        if (running_.load()) {
            throw std::logic_error("multiplexer is running");
        }
        worker_ = worker;
        index_ = index;
        auto ready = std::make_shared<std::promise<void>>();
        auto completion = ready->get_future();
        if (!worker_.post([this, ready](ruvia::web_worker_context& context) {
                        return initialize(context, index_, ready);
                    })
                 .accepted()) {
            throw std::runtime_error("service rejected multiplexer initialization");
        }
        completion.get();
    }

    ruvia::task<void> wait(std::size_t workerIndex, WorkerStreamTask task, ruvia::stop_token stopToken, std::optional<std::chrono::milliseconds> maximum = std::nullopt) {
        const auto* receiver = &*requireSlot(workerIndex).receivers[taskIndex(task)];
        // The durable work streams remain authoritative when a wake is lost
        // during failover. This is a bounded recovery scan, not a one-second
        // normal-work poll; earlier domain deadlines must retain their meaning.
        constexpr auto recovery = std::chrono::seconds(60);
        if (!maximum || *maximum > recovery) {
            maximum = recovery;
        }
        if (maximum.has_value()) {
            if (maximum->count() <= 0) {
                co_return;
            }
            (void)co_await receiver->receive_for(*maximum, std::move(stopToken));
        } else {
            (void)co_await receiver->receive(std::move(stopToken));
        }
    }

    void signalTelemetryConsumers() noexcept {
        for (const auto task : { WorkerStreamTask::TelemetryHistory,
                                 WorkerStreamTask::TelemetryLatest,
                                 WorkerStreamTask::TelemetryAlerts,
                                 WorkerStreamTask::TelemetryDelivery }) {
            signal(task);
        }
    }

    void signal(WorkerStreamTask task) noexcept {
        if (localStreamMultiplexer == this && slot_) {
            (void)slot_->senders[taskIndex(task)].send(1);
        }
    }

    void start(ruvia::web_worker_handle worker, std::size_t index) {
        if (running_.exchange(true)) {
            return;
        }
        if (!slot_ || index != index_) {
            running_.store(false);
            throw std::logic_error("multiplexer is not configured for this worker");
        }
        worker_ = worker;
        stopSource_ = std::make_unique<ruvia::stop_source>();
        auto ready = std::make_shared<std::promise<void>>();
        auto done = std::make_shared<std::promise<void>>();
        auto readiness = ready->get_future();
        stopped_ = done->get_future();
        if (!worker_.post([this, ready, done](ruvia::web_worker_context& context) {
                        return run(context, index_, ready, done);
                    })
                 .accepted()) {
            done->set_value();
            running_.store(false);
            throw std::runtime_error("service rejected multiplexer startup");
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
        if (stopSource_) {
            stopSource_->request_stop();
        }
        if (stopped_.valid()) {
            stopped_.wait();
        }
        stopSource_.reset();
    }

  private:
    static constexpr std::size_t kTaskCount =
        static_cast<std::size_t>(WorkerStreamTask::Count);
    static constexpr std::string_view kGroup{ "iot-engine:service-worker-wake" };
    static constexpr std::size_t kBatchSize = 256;

    struct WorkerSlot final {
        std::array<ruvia::channel_sender<std::uint8_t>, kTaskCount> senders;
        std::array<std::optional<ruvia::channel_receiver<std::uint8_t>>, kTaskCount>
            receivers;
    };

    [[nodiscard]] static constexpr std::size_t taskIndex(WorkerStreamTask task) {
        return static_cast<std::size_t>(task);
    }

    WorkerSlot& requireSlot(std::size_t workerIndex) {
        if (workerIndex != index_ || !slot_ || localStreamMultiplexer != this) {
            throw std::logic_error("cross-worker multiplexer access");
        }
        return *slot_;
    }

    ruvia::task<void> initialize(ruvia::web_worker_context& context, std::size_t index, std::shared_ptr<std::promise<void>> ready) {
        try {
            auto slot = std::make_unique<WorkerSlot>();
            for (std::size_t task = 0; task < kTaskCount; ++task) {
                auto [sender, receiver] =
                    ruvia::make_channel<std::uint8_t>(context.worker(), { .capacity_ = 1 });
                slot->senders[task] = std::move(sender);
                slot->receivers[task].emplace(std::move(receiver));
            }
            slot_ = std::move(slot);
            localStreamMultiplexer = this;
            ready->set_value();
        } catch (...) {
            try {
                ready->set_exception(std::current_exception());
            } catch (...) {
            }
        }
        co_return;
    }

    void signal(std::size_t workerIndex, WorkerStreamTask task) noexcept {
        if (workerIndex == index_) {
            signal(task);
        }
    }

    void signalAll() noexcept {
        if (slot_) {
            for (const auto& sender : slot_->senders) {
                (void)sender.send(1);
            }
        }
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t index, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        bool readySet = false;
        try {
            const auto redis = context.redis();
            const auto wakeStream = workerWakeStream(index, service::runtime::instanceId());
            const auto consumer = service::runtime::instanceId() + ":service-" + std::to_string(index);
            const auto group = std::string(kGroup) + ":" + std::to_string(index);
            const std::vector<std::string> streams{ wakeStream, sharedWakeStream(service::runtime::instanceId()) };
            for (const auto& stream : streams) {
                co_await service::message::redis::ensureGroup(redis, stream, group);
            }
            ready->set_value();
            readySet = true;
            const auto stopToken = ruvia::combine_stop_tokens(
                context.get_stop_token(),
                stopSource_->token()
            );
            bool recovering = true;
            while (!stopToken.stop_requested()) {
                bool failed = false;
                try {
                    std::vector<service::message::redis::StreamBatch> batches;
                    if (recovering) {
                        batches = co_await service::message::redis::claimGroupMany(
                            redis,
                            streams,
                            group,
                            consumer,
                            kBatchSize
                        );
                    } else {
                        batches = co_await service::message::redis::readGroupManyBlockingUntil(
                            redis,
                            streams,
                            group,
                            consumer,
                            stopToken,
                            std::nullopt,
                            kBatchSize
                        );
                    }
                    if (recovering && batches.empty()) {
                        // A reconnect may have lost/trimmed wake hints while the
                        // corresponding business entries are still durable.
                        signalAll();
                        recovering = false;
                        continue;
                    }
                    for (const auto& batch : batches) {
                        for (const auto& message : batch.messages) {
                            const auto task = workerStreamTask(message.get("task"));
                            if (task) {
                                signal(index, *task);
                            } else {
                                std::cerr << "service worker " << index
                                          << " received an unknown wake task\n";
                            }
                        }
                        if (batch.stream == wakeStream) {
                            co_await service::message::redis::acknowledgeAndDeleteMany(
                                redis,
                                batch.stream,
                                group,
                                batch.messages
                            );
                        } else {
                            // Every independent worker sees this availability hint.
                            // Only the business consumer group claims the payload.
                            for (const auto& entry : batch.messages) {
                                co_await service::message::redis::acknowledge(redis, batch.stream, group, entry.id);
                            }
                        }
                    }
                } catch (const std::exception& error) {
                    if (stopToken.stop_requested()) {
                        break;
                    }
                    std::cerr << "service worker " << index
                              << " wake stream failed: " << error.what() << '\n';
                    recovering = true;
                    failed = true;
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(
                        context.worker(),
                        std::chrono::milliseconds(250)
                    );
                }
            }
        } catch (...) {
            if (!readySet) {
                try {
                    ready->set_exception(std::current_exception());
                } catch (...) {
                }
            }
        }
        if (localStreamMultiplexer == this) {
            localStreamMultiplexer = nullptr;
        }
        try {
            stopped->set_value();
        } catch (...) {
        }
    }

    std::unique_ptr<WorkerSlot> slot_;
    ruvia::web_worker_handle worker_;
    std::size_t index_{};
    std::future<void> stopped_;
    std::unique_ptr<ruvia::stop_source> stopSource_;
    std::atomic_bool running_{ false };
};

inline WorkerStreamMultiplexer& workerStreamMultiplexer() {
    if (!localStreamMultiplexer) {
        throw std::logic_error("worker stream multiplexer is unavailable on this thread");
    }
    return *localStreamMultiplexer;
}

} // namespace service::message
