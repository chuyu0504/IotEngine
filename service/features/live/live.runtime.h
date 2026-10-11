#pragma once

#include "service/common/uuid.h"

#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/features/edge/session/session.service.h"
#include "service/utils/redis.h"

namespace service::live {

// This runtime belongs to the backend feature layer. It relays committed
// configuration changes to every Collector Worker and expires Edge sessions;
// API fanout is owned by service/middleware/live.h in QueryRuntime.
class LiveChangeRuntime final {
  public:
    explicit LiveChangeRuntime(std::size_t collectorWorkerCount)
        : collectorWorkerCount_(collectorWorkerCount) {
        if (collectorWorkerCount_ == 0) {
            throw std::invalid_argument("live runtime requires collector workers");
        }
    }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex, std::size_t serviceWorkerCount) {
        if (serviceWorkerCount == 0 || workerIndex >= serviceWorkerCount) {
            throw std::invalid_argument("live runtime requires a valid Service Worker index");
        }
        stop_ = std::make_unique<ruvia::stop_source>();
        auto ready = std::make_shared<std::promise<void>>();
        auto done = std::make_shared<std::promise<void>>();
        auto started = ready->get_future();
        stopped_ = done->get_future();
        if (!worker.post([this, workerIndex, ready, done](
                             ruvia::web_worker_context& context
                         ) {
                       return run(context, workerIndex, ready, done);
                   })
                  .accepted()) {
            stop_.reset();
            throw std::runtime_error("live feature worker rejected startup");
        }
        try {
            started.get();
        } catch (...) {
            stop();
            throw;
        }
        auto leasesDone = std::make_shared<std::promise<void>>();
        leasesStopped_ = leasesDone->get_future();
        if (!worker.post([this, leasesDone](ruvia::web_worker_context& context) {
                       return expireSessions(context, leasesDone);
                   })
                 .accepted()) {
            leasesDone->set_value();
            stop();
            throw std::runtime_error("live session deadline worker rejected startup");
        }
    }

    void stop() {
        if (!stop_) {
            return;
        }
        stop_->request_stop();
        if (stopped_.valid()) {
            stopped_.wait();
        }
        if (leasesStopped_.valid()) {
            leasesStopped_.wait();
        }
        stop_.reset();
    }

    ~LiveChangeRuntime() { stop(); }

  private:
    ruvia::task<void> relayConfigNotifications(ruvia::web_worker_context& context) {
        const auto createdAt = std::to_string(service::message::utcNowMilliseconds());
        for (std::size_t collectorIndex = 0; collectorIndex < collectorWorkerCount_;
             ++collectorIndex) {
            const std::vector<std::string> args{
                "XADD",
                service::message::configStream(collectorIndex, service::runtime::instanceId()),
                "MAXLEN",
                "~",
                "10000",
                "*",
                "message_id",
                context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(),
                "worker_id",
                std::to_string(collectorIndex),
                "created_at_ms",
                createdAt
            };
            const auto reply = co_await service::message::redis::command(context.redis(), args);
            if (reply.kind() != ruvia::redis_value::kind_type::string) {
                service::message::redis::throwValue("XADD collector config", reply);
            }
        }
    }

    ruvia::task<void> expireSessions(ruvia::web_worker_context& context, std::shared_ptr<std::promise<void>> done) {
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stop_->token());
        while (!stop.stop_requested()) {
            try {
                co_await service::edge::session_state::expireSessions(context.redis());
            } catch (const std::exception& error) {
                if (!stop.stop_requested()) {
                    std::cerr << "edge session deadline: " << error.what() << '\n';
                }
            }
            (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1), stop);
        }
        done->set_value();
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t workerIndex, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> done) {
        const auto group = "live:" + service::runtime::instanceId();
        const auto consumer = "fanout:" + service::runtime::instanceId() + ":" +
            std::to_string(workerIndex);
        const auto redis = context.redis();
        const std::vector<std::string> streams{ std::string(service::message::live::kChanges) };
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stop_->token());
        bool initialized = false;
        try {
            co_await service::message::redis::ensureGroup(redis, streams.front(), group);
            ready->set_value();
            initialized = true;
            bool recovering = true;
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    if (recovering) {
                        co_await service::message::redis::ensureGroup(
                            redis,
                            streams.front(),
                            group
                        );
                        // Claim this worker's own pending entries first, then take
                        // stale entries left by a stopped worker. A live pending
                        // entry remains with its original unique consumer.
                        const auto pending = co_await service::message::redis::claimGroupMany(
                            redis,
                            streams,
                            group,
                            consumer,
                            256
                        );
                        if (!pending.empty()) {
                            for (const auto& batch : pending) {
                                for (const auto& change : batch.messages) {
                                    if (change.get(service::message::live::kTopicField) ==
                                        "runtime-config") {
                                        co_await relayConfigNotifications(context);
                                    }
                                }
                            }
                            for (const auto& batch : pending) {
                                for (const auto& change : batch.messages) {
                                    co_await service::message::redis::acknowledge(
                                        redis,
                                        batch.stream,
                                        group,
                                        change.id
                                    );
                                }
                            }
                            continue;
                        }
                        recovering = false;
                    }
                    const auto changes = co_await service::message::redis::readGroupBlocking(
                        redis,
                        streams.front(),
                        group,
                        consumer,
                        stop,
                        256
                    );
                    for (const auto& change : changes) {
                        if (change.get(service::message::live::kTopicField) == "runtime-config") {
                            co_await relayConfigNotifications(context);
                        }
                        co_await service::message::redis::acknowledge(
                            redis,
                            streams.front(),
                            group,
                            change.id
                        );
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    std::cerr << "live feature stream: " << error.what() << '\n';
                    failed = true;
                    recovering = true;
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1));
                }
            }
        } catch (...) {
            if (!initialized) {
                ready->set_exception(std::current_exception());
            }
        }
        // This consumer group is shared by every Service Worker. Leaving it in
        // Redis avoids revoking another worker's pending notification during a
        // rolling stop or a worker restart.
        done->set_value();
    }

    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
    std::future<void> leasesStopped_;
    std::size_t collectorWorkerCount_ = 0;
};

} // namespace service::live
