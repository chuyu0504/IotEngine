#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/edge/session/session.service.h"
#include "service/middleware/log.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"

namespace service::live {

// Per-instance claim of one kChanges entry: every Service Worker of an instance
// reads the broadcast stream, exactly one of them relays the entry to the
// instance's Collector Workers.
inline constexpr std::string_view kRelayClaimPrefix{ "iot:live:relay:" };
inline constexpr std::string_view kRelayClaimSeconds{ "60" };
// Upper bound of the edge session expiry sleep: every session deadline is
// written as now + 930s, so nothing written later can be due sooner.
inline constexpr std::chrono::milliseconds kSessionLeaseLifetime{ 930000 };

inline std::string relayClaimKey(std::string_view instance, std::string_view entryId) {
    return std::string(kRelayClaimPrefix) + std::string(instance) + ":" + std::string(entryId);
}

// Relays committed runtime configuration changes to every Collector Worker of
// this instance and expires Edge sessions at their nearest deadline. API
// fanout is owned by service/utils/change_feed.h.
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
        if (!worker.post([this, ready, done](ruvia::web_worker_context& context) {
                       return run(context, ready, done);
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
    // Removes consumer groups that older versions created on kChanges; the
    // stream is now read with plain XREAD only. Idempotent across workers.
    template <typename Redis>
    static ruvia::task<void> destroyLegacyGroups(const Redis& redis) {
        const auto stream = std::string(service::message::live::kChanges);
        const std::vector<std::string> infoArguments{ "XINFO", "GROUPS", stream };
        const auto groups = co_await service::message::redis::command(redis, infoArguments);
        if (groups.kind() == ruvia::redis_value::kind_type::error) {
            if (groups.error().find("no such key") != std::string_view::npos) {
                co_return;
            }
            service::message::redis::throwValue("XINFO GROUPS live changes", groups);
        }
        if (groups.kind() != ruvia::redis_value::kind_type::array) {
            service::message::redis::throwValue("XINFO GROUPS live changes", groups);
        }
        for (const auto& group : groups.array()) {
            if (group.kind() != ruvia::redis_value::kind_type::array) {
                continue;
            }
            const auto fields = group.array();
            for (std::size_t index = 0; index + 1 < fields.size(); index += 2) {
                if (fields[index].kind() != ruvia::redis_value::kind_type::string ||
                    fields[index].string() != "name" ||
                    fields[index + 1].kind() != ruvia::redis_value::kind_type::string) {
                    continue;
                }
                const auto name = std::string(fields[index + 1].string());
                if (!name.starts_with("api-live:") && !name.starts_with("live:")) {
                    break;
                }
                const std::vector<std::string> destroyArguments{ "XGROUP", "DESTROY", stream, name };
                const auto reply = co_await service::message::redis::command(redis, destroyArguments);
                if (reply.kind() != ruvia::redis_value::kind_type::integer) {
                    service::message::redis::throwValue("XGROUP DESTROY legacy live group", reply);
                }
                break;
            }
        }
    }

    ruvia::task<void> relayConfigNotifications(ruvia::web_worker_context& context) {
        const auto createdAt = std::to_string(service::message::utcNowMilliseconds());
        for (std::size_t collectorIndex = 0; collectorIndex < collectorWorkerCount_; ++collectorIndex) {
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

    // Claims a runtime-config entry for this instance; false when another
    // Service Worker of the instance already relays it.
    static ruvia::task<bool> claimRelay(ruvia::web_worker_context& context, std::string_view entryId) {
        const std::vector<std::string> args{
            "SET",
            relayClaimKey(service::runtime::instanceId(), entryId),
            "1",
            "NX",
            "EX",
            std::string(kRelayClaimSeconds)
        };
        const auto reply = co_await service::message::redis::command(context.redis(), args);
        if (reply.kind() == ruvia::redis_value::kind_type::error) {
            service::message::redis::throwValue("claim live relay", reply);
        }
        co_return reply.kind() == ruvia::redis_value::kind_type::string;
    }

    static ruvia::task<void> releaseRelay(ruvia::web_worker_context& context, std::string_view entryId) {
        const std::vector<std::string> args{ "DEL", relayClaimKey(service::runtime::instanceId(), entryId) };
        (void)co_await service::message::redis::command(context.redis(), args);
    }

    template <typename Redis>
    static ruvia::task<std::int64_t> redisNowMilliseconds(const Redis& redis) {
        const std::vector<std::string> args{ "TIME" };
        const auto clock = co_await service::message::redis::command(redis, args);
        if (clock.kind() != ruvia::redis_value::kind_type::array || clock.array().size() != 2 ||
            clock.array()[0].kind() != ruvia::redis_value::kind_type::string ||
            clock.array()[1].kind() != ruvia::redis_value::kind_type::string) {
            service::message::redis::throwValue("read Redis clock", clock);
        }
        const auto seconds = service::utils::parseInt64(std::optional<std::string_view>{ clock.array()[0].string() });
        const auto microseconds = service::utils::parseInt64(std::optional<std::string_view>{ clock.array()[1].string() });
        if (!seconds || !microseconds) {
            throw std::runtime_error("invalid Redis clock");
        }
        co_return *seconds * 1000 + *microseconds / 1000;
    }

    // Sleeps until the earliest session deadline, then lets kExpireScript claim
    // (atomic ZREM) and publish the due sessions. No fixed-period scan.
    ruvia::task<void> expireSessions(ruvia::web_worker_context& context, std::shared_ptr<std::promise<void>> done) {
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stop_->token());
        const auto redis = context.redis();
        while (!stop.stop_requested()) {
            auto wait = kSessionLeaseLifetime;
            try {
                co_await service::edge::session_state::expireSessions(redis);
                const auto deadline = co_await service::edge::session_state::nextDeadline(redis);
                if (deadline) {
                    const auto now = co_await redisNowMilliseconds(redis);
                    wait = std::chrono::milliseconds(std::clamp<std::int64_t>(*deadline - now, 0, kSessionLeaseLifetime.count()));
                }
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
        done->set_value();
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> done) {
        const auto redis = context.redis();
        const std::vector<std::string> streams{ std::string(service::message::live::kChanges) };
        std::vector<std::string> cursors{ "0-0" };
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stop_->token());
        try {
            co_await destroyLegacyGroups(redis);
            cursors.front() = co_await service::message::redis::lastEntryId(redis, streams.front());
            ready->set_value();
        } catch (...) {
            ready->set_exception(std::current_exception());
            done->set_value();
            co_return;
        }
        while (!stop.stop_requested()) {
            bool failed = false;
            try {
                const auto batches = co_await service::message::redis::readStreamsBlocking(
                    redis,
                    streams,
                    cursors,
                    stop,
                    std::chrono::seconds(30),
                    256
                );
                for (const auto& batch : batches) {
                    for (const auto& change : batch.messages) {
                        if (change.get(service::message::live::kTopicField) == service::message::live::topic::kRuntimeConfig &&
                            co_await claimRelay(context, change.id)) {
                            std::exception_ptr failure;
                            try {
                                co_await relayConfigNotifications(context);
                            } catch (...) {
                                failure = std::current_exception();
                            }
                            if (failure) {
                                // Let this or another worker retry the entry.
                                co_await releaseRelay(context, change.id);
                                std::rethrow_exception(failure);
                            }
                        }
                        cursors.front() = change.id;
                    }
                }
            } catch (const std::exception& error) {
                if (stop.stop_requested()) {
                    break;
                }
                LOG_ERROR << "live change relay: " << error.what();
                failed = true;
            }
            if (failed) {
                // Resume from the cursor of the last relayed entry.
                (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1), stop);
            }
        }
        done->set_value();
    }

    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
    std::future<void> leasesStopped_;
    std::size_t collectorWorkerCount_ = 0;
};

} // namespace service::live
