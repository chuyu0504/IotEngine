#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/channel.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/features/messaging/rpc/rpc.service.h"
#include "service/utils/log.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"
#include "service/utils/worker_task.h"

namespace service::messaging::rpc {

// Durable requests belong to the accepting Service Worker. The cancellation
// channel is only a wakeup: its persisted keys are checked on initial execution
// and after a lost subscription. Completed results likewise remain persisted.
class RpcConsumerRuntime final {
  public:
    using Handler = std::function<ruvia::task<std::string>(ruvia::web_worker_context&, std::string_view, std::string_view, ruvia::stop_token)>;

    void add(std::string component, Handler handler) {
        if (stop_ || !handlers_.emplace(std::move(component), std::move(handler)).second) {
            throw std::logic_error("Invalid RPC handler registration");
        }
    }

    void start(ruvia::web_worker_handle worker) {
        worker_ = worker;
        stop_ = std::make_unique<ruvia::stop_source>();
        try {
            service::utils::startWorkerTask(worker, stop_->token(), cancellationsStopped_, [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                return cancellations(context, ready, stop);
            });
            service::utils::startWorkerTask(worker, stop_->token(), monitorStopped_, [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                return monitor(context, ready, stop);
            });
            service::utils::startWorkerTask(worker, stop_->token(), stopped_, [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                return run(context, ready, stop);
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
        stop_->request_stop();
        service::utils::stopWorkerTask(*stop_, stopped_);
        service::utils::stopWorkerTask(*stop_, cancellationsStopped_);
        service::utils::stopWorkerTask(*stop_, monitorStopped_);
        stop_.reset();
    }

    ~RpcConsumerRuntime() { stop(); }

  private:
    struct Pending {
        ruvia::stop_source stop;
        std::int64_t deadline = 0;
        std::string requestId;
    };

    struct Wake {
        ruvia::channel_sender<int> sender;
        ruvia::channel_receiver<int> receiver;
    };

    ruvia::task<void> process(ruvia::web_worker_context& context, service::message::StreamMessage message, std::shared_ptr<Pending> pending) {
        const auto id = std::string(message.get("id"));
        std::string result;
        try {
            if (!service::common::isUuid(id) || message.get("version") != service::message::rpc::Contract::version ||
                message.get("payload").size() > service::message::rpc::Contract::maximumPayload) {
                service::common::fail(10002, "Invalid background request", 400);
            }
            if (pending->deadline <= service::message::rpc::Contract::now()) {
                service::common::fail(10004, "Background operation timed out", 504);
            }
            const auto previous = co_await RpcReceiptService::beginRequest(context.redis(), id);
            if (previous) {
                result = previous->payload;
            } else {
                const auto handler = handlers_.find(std::string(message.get("component")));
                if (handler == handlers_.end()) {
                    service::common::fail(10004, "Background component is unavailable", 503);
                }
                const auto token = ruvia::combine_stop_tokens(context.stop_token(), pending->stop.token());
                result = service::message::rpc::Contract::success(co_await handler->second(context, message.get("operation"), message.get("payload"), token));
            }
        } catch (const ruvia::http_error& error) {
            const auto info = error.info();
            result = service::message::rpc::Contract::failure(info.status().value(), info.code(), info.message());
        } catch (const std::exception& error) {
            result = service::message::rpc::Contract::failure(500, "10004", error.what());
        } catch (...) {
            result = service::message::rpc::Contract::failure(500, "10004", "Background operation failed");
        }
        while (true) {
            bool failed = false;
            try {
                co_await RpcReceiptService::saveAndAcknowledge(context.redis(), context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, id, stream_, kGroup, message.id, RpcReplyRecord{ result });
            } catch (const std::exception& error) {
                failed = true;
                if (!stop_->token().stop_requested()) {
                    LOG_ERROR << "RPC reply persistence: " << error.what();
                }
            }
            if (!failed || stop_->token().stop_requested()) {
                break;
            }
            (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1), stop_->token());
        }
        pending_.erase(message.id);
        if (wake_) {
            (void)wake_->sender.send(1);
        }
    }

    ruvia::task<void> cancellations(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        stream_ = service::message::rpc::Contract::requests(identity.instanceId, identity.index);
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        bool initialized = false;
        while (!stop.stop_requested()) {
            bool failed = false;
            try {
                auto subscription = redis.subscription();
                const std::string_view channels[]{ stream_ };
                co_await subscription.subscribe(channels);
                bool confirmed = false;
                while (auto event = co_await subscription.read()) {
                    if (event->kind() == ruvia::redis_pubsub_event_kind::subscribed) {
                        confirmed = true;
                        break;
                    }
                }
                if (!confirmed) {
                    break;
                }
                // Reconcile lost cancellation notifications only on reconnect.
                std::vector<std::shared_ptr<Pending>> active;
                std::vector<std::string> keys;
                for (const auto& [entry, pending] : pending_) {
                    active.push_back(pending);
                    keys.push_back(service::message::rpc::Contract::cancelled(pending->requestId));
                }
                if (!keys.empty()) {
                    const std::vector<std::string_view> views(keys.begin(), keys.end());
                    const auto values = co_await redis.mget(views);
                    for (std::size_t index = 0; index < active.size(); ++index) {
                        if (values[index]) {
                            active[index]->stop.request_stop();
                        }
                    }
                }
                if (!initialized) {
                    // Own the group and its expiry atomically before any
                    // monitor or consumer can report business readiness.
                    co_await service::utils::redis::ensureGroup(redis, stream_, kGroup, kStreamLifetime);
                    ready.set_value();
                    initialized = true;
                }
                while (auto event = co_await subscription.read()) {
                    if (event->kind() != ruvia::redis_pubsub_event_kind::message) {
                        continue;
                    }
                    for (const auto& [entry, pending] : pending_) {
                        if (pending->requestId == event->payload()) {
                            pending->stop.request_stop();
                        }
                    }
                }
            } catch (const std::exception& error) {
                if (!initialized) {
                    throw;
                }
                if (stop.stop_requested()) {
                    break;
                }
                LOG_WARN << "RPC cancellation subscription: " << error.what();
                failed = true;
            }
            if (failed) {
                (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(100), stop);
            }
        }
    }

    ruvia::task<void> monitor(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        auto [sender, receiver] = ruvia::make_channel<int>(context.worker(), { .capacity_ = 1 });
        wake_.emplace(Wake{ std::move(sender), std::move(receiver) });
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        auto nextLeaseAttempt = service::message::rpc::Contract::now() + 60000;
        ready.set_value();
        while (!stop.stop_requested()) {
            const auto now = service::message::rpc::Contract::now();
            if (now >= nextLeaseAttempt) {
                bool leased = false;
                try {
                    co_await service::utils::redis::ensureGroup(redis, stream_, kGroup, kStreamLifetime);
                    leased = true;
                } catch (const std::exception& error) {
                    if (!stop.stop_requested()) {
                        LOG_WARN << "RPC stream lease: " << error.what();
                    }
                }
                nextLeaseAttempt = now + (leased ? 60000 : 1000);
            }
            auto next = nextLeaseAttempt;
            for (const auto& [entry, pending] : pending_) {
                if (pending->deadline <= now) {
                    pending->stop.request_stop();
                } else {
                    next = std::min(next, pending->deadline);
                }
            }
            (void)co_await wake_->receiver.receive_for(std::chrono::milliseconds(std::max<std::int64_t>(1, next - now)), stop);
        }
        for (const auto& [entry, pending] : pending_) {
            pending->stop.request_stop();
        }
        wake_.reset();
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        const std::vector<std::string> streams{ stream_ };
        std::exception_ptr failure;
        try {
            co_await service::utils::redis::withConsumerNotifications(
                redis,
                context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                context.worker(),
                streams,
                kGroup,
                stop,
                [&](ruvia::channel_receiver<std::uint8_t>& notifications) -> ruvia::task<void> {
                    ready.set_value();
                    bool recovering = true;
                    while (!stop.stop_requested()) {
                        bool failed = false;
                        try {
                            if (pending_.size() >= 128) {
                                (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(10), stop);
                                continue;
                            }
                            auto batches = co_await service::utils::redis::consumeGroup(
                                redis,
                                context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                                streams,
                                kGroup,
                                "control",
                                recovering,
                                notifications,
                                stop,
                                std::nullopt,
                                32,
                                std::chrono::minutes(1),
                                false,
                                kStreamLifetime
                            );
                            for (auto& batch : batches) {
                                for (auto& message : batch.messages) {
                                    if (pending_.contains(message.id)) {
                                        continue;
                                    }
                                    auto pending = std::make_shared<Pending>();
                                    pending->deadline = service::utils::parseInt64(message.get("deadline")).value_or(0);
                                    pending->requestId = std::string(message.get("id"));
                                    const auto entry = message.id;
                                    pending_[entry] = pending;
                                    if (wake_) {
                                        (void)wake_->sender.send(1);
                                    }
                                    if (!worker_.post([this, message = std::move(message), pending](ruvia::web_worker_context& current) mutable {
                                                    return process(current, std::move(message), pending);
                                                })
                                             .accepted()) {
                                        pending_.erase(entry);
                                        throw std::runtime_error("RPC operation rejected");
                                    }
                                }
                            }
                        } catch (const std::exception& error) {
                            if (stop.stop_requested()) {
                                break;
                            }
                            LOG_ERROR << "RPC consumer: " << error.what();
                            failed = true;
                            recovering = true;
                        }
                        if (failed) {
                            (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1), stop);
                        }
                    }
                }
            );
        } catch (...) {
            failure = std::current_exception();
        }
        for (const auto& [entry, pending] : pending_) {
            pending->stop.request_stop();
        }
        while (!pending_.empty()) {
            (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(10));
        }
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    static constexpr std::string_view kGroup = "control";
    static constexpr auto kStreamLifetime = std::chrono::seconds(86400);
    std::string stream_;
    std::map<std::string, Handler> handlers_;
    std::map<std::string, std::shared_ptr<Pending>> pending_;
    std::optional<Wake> wake_;
    ruvia::web_worker_handle worker_;
    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
    std::future<void> cancellationsStopped_;
    std::future<void> monitorStopped_;
};

} // namespace service::messaging::rpc
