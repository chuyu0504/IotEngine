#pragma once

#include "service/utils/number.h"
#include "service/features/messaging/messaging.service.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"

namespace service::message::outbox {
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
        service::message::workerStreamMultiplexer().signal(
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
                        std::cerr << "outbox receipt cleanup failed: " << error.what() << '\n';
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
                        std::cerr << "metrics collection failed: " << error.what() << '\n';
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
                        std::cerr << "outbox dispatch failed: " << error.what() << '\n';
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

} // namespace service::message::outbox

#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <ruvia/web/web_worker.h>
#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/common/message.h"
#include "service/features/messaging/messaging.transport.h"

namespace service::rpc {

class RpcConsumerRuntime final {
  public:
    using Handler = std::function<ruvia::task<std::string>(ruvia::web_worker_context&,
        std::string_view, std::string_view, ruvia::stop_token)>;

    void add(std::string component, Handler handler) {
        if (stop_ || !handlers_.emplace(std::move(component), std::move(handler)).second)
            throw std::logic_error("Invalid RPC handler registration");
    }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex) {
        worker_ = worker;
        stream_ = service::message::rpc::Contract::requests(service::runtime::instanceId(), workerIndex);
        stop_ = std::make_unique<ruvia::stop_source>();
        auto ready = std::make_shared<std::promise<void>>();
        auto done = std::make_shared<std::promise<void>>();
        auto started = ready->get_future();
        stopped_ = done->get_future();
        if (!worker.post([this, ready, done](ruvia::web_worker_context& context) {
                return run(context, ready, done);
            }).accepted()) {
            stop_.reset();
            throw std::runtime_error("RPC worker rejected startup");
        }
        started.get();
    }

    void stop() {
        if (!stop_) return;
        stop_->request_stop();
        if (stopped_.valid()) stopped_.wait();
        stop_.reset();
    }
    ~RpcConsumerRuntime() { stop(); }

  private:
    struct Pending {
        ruvia::stop_source stop;
        std::int64_t deadline = 0;
        std::string requestId;
    };

    ruvia::task<void> process(ruvia::web_worker_context& context,
                              service::message::StreamMessage message,
                              std::shared_ptr<Pending> pending) {
        const auto id = std::string(message.get("id"));
        std::string result;
        try {
            if (!service::common::isUuid(id) || message.get("version") != service::message::rpc::Contract::version ||
                message.get("payload").size() > service::message::rpc::Contract::maximumPayload)
                service::common::fail(10002, "Invalid background request", 400);
            if (pending->deadline <= service::message::rpc::Contract::now())
                service::common::fail(10004, "Background operation timed out", 504);
            const auto previous = co_await RpcReceiptService::beginRequest(context.redis(), id);
            if (previous) {
                result = previous->payload;
            } else {
                const auto handler = handlers_.find(std::string(message.get("component")));
                if (handler == handlers_.end())
                    service::common::fail(10004, "Background component is unavailable", 503);
                const auto token = ruvia::combine_stop_tokens(context.get_stop_token(), pending->stop.token());
                result = service::message::rpc::Contract::success(co_await handler->second(
                    context, message.get("operation"), message.get("payload"), token));
            }
        } catch (const ruvia::http_error& error) {
            const auto info = error.info();
            result = service::message::rpc::Contract::failure(info.status().value(), info.code(), info.message());
        } catch (const std::exception& error) {
            result = service::message::rpc::Contract::failure(500, "10004", error.what());
        } catch (...) {
            result = service::message::rpc::Contract::failure(500, "10004", "Background operation failed");
        }
        // Retry only persistence of the completed result, never the operation.
        while (true) {
            bool failed = false;
            try {
                const RpcReplyRecord record{result};
                co_await RpcReceiptService::saveAndAcknowledge(
                    context.redis(), id, stream_, kGroup, message.id, record);
            } catch (const std::exception& error) {
                failed = true;
                if (!stop_->token().stop_requested())
                    std::cerr << "RPC reply persistence: " << error.what() << '\n';
            }
            if (!failed || stop_->token().stop_requested()) break;
            (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1), stop_->token());
        }
        pending_.erase(message.id);
    }

    ruvia::task<void> monitor(ruvia::web_worker_context& context,
                              std::shared_ptr<std::promise<void>> done) {
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stop_->token());
        auto nextExpiryRefresh = std::int64_t{0};
        while (!stop.stop_requested()) {
            std::vector<std::shared_ptr<Pending>> snapshot;
            std::vector<std::string> arguments{"MGET"};
            for (const auto& [entry, pending] : pending_) {
                snapshot.push_back(pending);
                arguments.push_back(service::message::rpc::Contract::cancelled(pending->requestId));
                if (pending->deadline <= service::message::rpc::Contract::now()) pending->stop.request_stop();
            }
            if (!snapshot.empty()) {
                try {
                    const auto cancelled = co_await service::message::redis::command(
                        context.redis(), arguments);
                    if (cancelled.kind() == ruvia::redis_value::kind_type::array &&
                        cancelled.array().size() == snapshot.size())
                        for (std::size_t index = 0; index < snapshot.size(); ++index)
                            if (cancelled.array()[index].kind() == ruvia::redis_value::kind_type::string)
                                snapshot[index]->stop.request_stop();
                } catch (...) {
                    // The local deadline remains effective during Redis outages.
                }
            }
            if (service::message::rpc::Contract::now() >= nextExpiryRefresh) {
                try {
                    const std::vector<std::string> expireArguments{"EXPIRE", stream_, "86400"};
                    (void)co_await service::message::redis::command(
                        context.redis(), expireArguments);
                    nextExpiryRefresh = service::message::rpc::Contract::now() + 60000;
                } catch (...) {
                }
            }
            (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(100), stop);
        }
        for (const auto& [entry, pending] : pending_) pending->stop.request_stop();
        done->set_value();
    }

    ruvia::task<void> run(ruvia::web_worker_context& context,
                          std::shared_ptr<std::promise<void>> ready,
                          std::shared_ptr<std::promise<void>> done) {
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stop_->token());
        bool initialized = false;
        auto monitorDone = std::make_shared<std::promise<void>>();
        auto monitored = monitorDone->get_future();
        bool monitorStarted = false;
        try {
            co_await service::message::redis::ensureGroup(context.redis(), stream_, kGroup);
            monitorStarted = worker_.post([this, monitorDone](ruvia::web_worker_context& current) {
                return monitor(current, monitorDone);
            }).accepted();
            if (!monitorStarted) throw std::runtime_error("RPC monitor rejected startup");
            ready->set_value();
            initialized = true;
            bool recovering = true;
            std::string recoveryId = "0";
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    if (pending_.size() >= 128) {
                        (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(10), stop);
                        continue;
                    }
                    if (recovering)
                        co_await service::message::redis::ensureGroup(context.redis(), stream_, kGroup);
                    auto messages = recovering
                        ? co_await service::message::redis::readGroup(context.redis(), stream_, kGroup,
                            "control", recoveryId, std::chrono::milliseconds(0), 32)
                        : co_await service::message::redis::readGroupBlocking(context.redis(), stream_,
                            kGroup, "control", stop, 32);
                    if (recovering) {
                        if (messages.empty()) recovering = false;
                        else recoveryId = messages.back().id;
                    }
                    for (auto& message : messages) {
                        if (pending_.contains(message.id)) continue;
                        auto pending = std::make_shared<Pending>();
                        pending->deadline = service::utils::parseInt64(message.get("deadline")).value_or(0);
                        pending->requestId = std::string(message.get("id"));
                        const auto entry = message.id;
                        pending_[entry] = pending;
                        if (!worker_.post([this, message = std::move(message), pending](
                                ruvia::web_worker_context& current) mutable {
                                return process(current, std::move(message), pending);
                            }).accepted()) {
                            pending_.erase(entry);
                            throw std::runtime_error("RPC operation rejected");
                        }
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) break;
                    std::cerr << "RPC consumer: " << error.what() << '\n';
                    failed = true;
                    recovering = true;
                    recoveryId = "0";
                }
                if (failed)
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1), stop);
            }
        } catch (...) {
            if (!initialized) ready->set_exception(std::current_exception());
        }
        stop_->request_stop();
        for (const auto& [entry, pending] : pending_) pending->stop.request_stop();
        while (!pending_.empty() || (monitorStarted &&
               monitored.wait_for(std::chrono::seconds(0)) != std::future_status::ready))
            (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(10));
        done->set_value();
    }

    static constexpr std::string_view kGroup = "control";
    std::string stream_;
    std::map<std::string, Handler> handlers_;
    std::map<std::string, std::shared_ptr<Pending>> pending_;
    ruvia::web_worker_handle worker_;
    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
};

} // namespace service::rpc
