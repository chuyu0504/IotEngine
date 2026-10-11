#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/channel.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/controller.h>
#include <ruvia/web/model_json.h>
#include <ruvia/web/streaming.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/middleware/request_context.h"
#include "service/utils/redis.h"

namespace service::live {

// Notifications carry no data or authorization decisions. A subscriber always
// rebuilds its own authorized snapshot. Capacity one intentionally coalesces
// intermediate changes while a slow subscriber is writing its current snapshot.
class Bus final {
  public:
    struct Subscription {
        std::string topic;
        ruvia::channel_sender<int> sender;
        ruvia::channel_receiver<int> receiver;
        std::vector<std::string> topics;
        std::set<std::string> changedTopics;
    };

    void setWorkerIndex(std::size_t workerIndex) noexcept {
        workerIndex_ = workerIndex;
    }

    [[nodiscard]] std::size_t workerIndex() const {
        if (!workerIndex_) {
            throw std::logic_error("live bus worker index is not initialized");
        }
        return *workerIndex_;
    }

    std::shared_ptr<Subscription> subscribe(const ruvia::worker_handle& worker, std::string_view topic) {
        if (!worker.is_current()) {
            throw std::logic_error("live subscriptions must belong to the current worker");
        }
        auto [sender, receiver] = ruvia::make_channel<int>(worker, { .capacity_ = 1 });
        auto subscription = std::make_shared<Subscription>(
            Subscription{ std::string(topic), std::move(sender), std::move(receiver) }
        );
        std::erase_if(subscriptions_, [](const auto& entry) {
            return entry.expired();
        });
        subscriptions_.push_back(subscription);
        return subscription;
    }

    std::shared_ptr<Subscription> subscribeTopics(const ruvia::worker_handle& worker, std::vector<std::string> topics) {
        auto subscription = subscribe(worker, "");
        subscription->topics = std::move(topics);
        return subscription;
    }

    void publish(std::string_view topic) {
        std::erase_if(subscriptions_, [&](const auto& entry) {
            auto subscription = entry.lock();
            if (!subscription) {
                return true;
            }
            const auto& target = subscription->topic;
            for (const auto& watched : subscription->topics) {
                if (affects(watched, topic)) {
                    subscription->changedTopics.insert(watched);
                }
            }
            if (!subscription->changedTopics.empty() || target == "*" || affects(target, topic)) {
                (void)subscription->sender.send(1);
            }
            return false;
        });
    }

  private:
    static bool affects(std::string_view target, std::string_view topic) {
        return topic == "*" || topic == "auth" || target == topic ||
            (target == "command" && (topic == "device" || topic == "protocol")) ||
            (target == "device.realtime" && (topic == "device" || topic == "protocol" || topic == "link" || topic == "edge")) ||
            (target == "device" && (topic == "protocol" || topic == "link" || topic == "edge")) ||
            (target == "access" && (topic == "device.realtime" || topic == "device" || topic == "protocol" || topic == "alert")) ||
            (target == "edge" && (topic == "device" || topic == "link" || topic == "protocol")) ||
            (target == "alert" && (topic == "protocol" || topic == "device")) ||
            (target == "vpn" && topic == "edge");
    }

    std::vector<std::weak_ptr<Subscription>> subscriptions_;
    std::optional<std::size_t> workerIndex_;
};

inline Bus& bus() {
    // Every web_worker owns one event-loop thread. Keeping the bus in thread
    // local storage prevents subscriptions and wakeups from crossing workers;
    // QueryRuntime initializes the worker index before requests are served.
    thread_local Bus value;
    return value;
}

// Each Service Worker owns one API reader. Every worker gets a separate group,
// so each receives every invalidation while the shared stream remains available
// to the feature runtime and other workers/instances.
class QueryRuntime final {
  public:
    void start(ruvia::web_worker_handle worker, std::size_t workerIndex) {
        stop_ = std::make_unique<ruvia::stop_source>();
        auto ready = std::make_shared<std::promise<void>>();
        auto done = std::make_shared<std::promise<void>>();
        auto started = ready->get_future();
        stopped_ = done->get_future();
        if (!worker.post([this, workerIndex, ready, done](ruvia::web_worker_context& context) {
                       return run(context, workerIndex, ready, done);
                   })
                 .accepted()) {
            stop_.reset();
            throw std::runtime_error("live query worker rejected startup");
        }
        try {
            started.get();
        } catch (...) {
            stop();
            throw;
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
        stop_.reset();
    }

    ~QueryRuntime() { stop(); }

  private:
    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t workerIndex, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> done) {
        bus().setWorkerIndex(workerIndex);
        const auto group = "api-live:" + service::runtime::instanceId() + ":" +
            std::to_string(workerIndex);
        const auto consumer = "api-live:" + service::runtime::instanceId() + ":" +
            std::to_string(workerIndex);
        const auto redis = context.redis();
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stop_->token());
        bool initialized = false;
        try {
            co_await service::message::redis::ensureGroup(
                redis,
                service::message::live::kChanges,
                group
            );
            ready->set_value();
            initialized = true;
            bool recovering = true;
            while (!stop.stop_requested()) {
                bool failed = false;
                try {
                    if (recovering) {
                        co_await service::message::redis::ensureGroup(
                            redis,
                            service::message::live::kChanges,
                            group
                        );
                        // A reconnect can miss an invalidation while the reader is
                        // disconnected. Rebuild every local subscription first.
                        bus().publish("*");
                        const auto pending = co_await service::message::redis::readGroup(
                            redis,
                            service::message::live::kChanges,
                            group,
                            consumer,
                            "0",
                            std::chrono::milliseconds(0),
                            256
                        );
                        for (const auto& change : pending) {
                            co_await service::message::redis::acknowledge(
                                redis,
                                service::message::live::kChanges,
                                group,
                                change.id
                            );
                        }
                        if (!pending.empty()) {
                            continue;
                        }
                        recovering = false;
                    }
                    const auto changes = co_await service::message::redis::readGroupBlocking(
                        redis,
                        service::message::live::kChanges,
                        group,
                        consumer,
                        stop,
                        256
                    );
                    std::set<std::string> topics;
                    for (const auto& change : changes) {
                        topics.emplace(change.get(service::message::live::kTopicField));
                    }
                    for (const auto& topic : topics) {
                        bus().publish(topic);
                    }
                    for (const auto& change : changes) {
                        // ACK advances only this process's group. Never remove a
                        // shared entry needed by another group.
                        co_await service::message::redis::acknowledge(
                            redis,
                            service::message::live::kChanges,
                            group,
                            change.id
                        );
                    }
                } catch (const std::exception& error) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    std::cerr << "live API fanout: " << error.what() << '\n';
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
        try {
            (void)co_await service::message::redis::command(
                redis,
                { "XGROUP", "DESTROY", std::string(service::message::live::kChanges), group }
            );
        } catch (...) {
        }
        done->set_value();
    }

    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
};

template <typename Model>
std::string json(const Model& model) {
    return std::string(ruvia::to_json(model));
}

inline std::string data(ruvia::context&, std::string_view value) {
    return "{\"code\":0,\"message\":\"ok\",\"data\":" + std::string(value) + "}";
}

struct SnapshotChannel {
    std::string event;
    std::string topic;
    std::function<ruvia::task<std::string>(service::middleware::RequestContext&)> query;
};

// Channels share one socket, but authorization, change detection and payloads
// remain independent. Telemetry never reloads device metadata or the group tree.
template <typename CheckToken, typename OnKeepalive>
ruvia::task<void> serveSnapshotChannels(ruvia::context& context, std::string userId, std::vector<SnapshotChannel> channels, CheckToken checkToken, OnKeepalive onKeepalive) {
    using namespace std::chrono_literals;
    if (context.req().header("Accept").value_or("").find("text/event-stream") == std::string_view::npos) {
        service::common::fail(10002, "此接口需要 text/event-stream", 406);
    }
    if (const auto* user = context.try_request_state<service::middleware::AuthenticatedUserSnapshot>()) {
        channels.push_back({ "user", "auth", user->query });
    }
    std::vector<std::string> topics;
    for (const auto& channel : channels) {
        topics.push_back(channel.topic);
    }
    auto subscription = bus().subscribeTopics(context.worker(), std::move(topics));
    auto readChannel = [&](const SnapshotChannel& channel) -> ruvia::task<std::string> {
        std::string payload;
        try {
            service::middleware::RequestContext request(context, userId);
            payload = co_await channel.query(request);
        } catch (const ruvia::http_error& failure) {
            const auto info = failure.info();
            payload = json(service::common::error(context, service::common::errorCode(info.code(), info.status().value()), info.message()));
        } catch (const std::exception&) {
            payload = "{\"code\":10004,\"message\":\"实时订阅已中断\"}";
        }
        co_return payload;
    };
    checkToken();
    std::vector<std::string> previous;
    for (const auto& channel : channels) {
        previous.push_back(co_await readChannel(channel));
    }
    context.header("X-Accel-Buffering", "no");
    auto stream = context.stream_sse();
    context.header("Cache-Control", "no-store");
    for (std::size_t index = 0; index < channels.size(); ++index) {
        co_await stream.write({ .data_ = previous[index], .event_ = channels[index].event });
    }
    while (!stream.aborted() && !context.get_stop_token().stop_requested()) {
        const auto notification = co_await subscription->receiver.receive_for(15s, context.get_stop_token());
        if (stream.aborted() || context.get_stop_token().stop_requested()) {
            co_return;
        }
        if (!notification.has_value() && notification.status() != ruvia::worker_wait_status::timed_out) {
            co_return;
        }
        std::string error;
        try {
            checkToken();
        } catch (const ruvia::http_error& failure) {
            const auto info = failure.info();
            error = json(service::common::error(context, service::common::errorCode(info.code(), info.status().value()), info.message()));
        }
        if (!error.empty()) {
            co_await stream.write({ .data_ = error, .event_ = "error" });
            co_return;
        }
        if (notification.has_value()) {
            auto changed = std::exchange(subscription->changedTopics, {});
            for (std::size_t index = 0; index < channels.size(); ++index) {
                if (!changed.contains(channels[index].topic)) {
                    continue;
                }
                auto next = co_await readChannel(channels[index]);
                if (next == previous[index]) {
                    continue;
                }
                previous[index] = std::move(next);
                co_await stream.write({ .data_ = previous[index], .event_ = channels[index].event });
            }
        }
        if (notification.status() == ruvia::worker_wait_status::timed_out) {
            co_await onKeepalive();
            co_await context.stream().write(": keepalive\n\n");
        }
    }
}

template <typename CheckToken>
ruvia::task<void> serveSnapshotChannels(ruvia::context& context, std::string userId, std::vector<SnapshotChannel> channels, CheckToken checkToken) {
    co_await serveSnapshotChannels(context, std::move(userId), std::move(channels), std::move(checkToken), []() -> ruvia::task<void> { co_return; });
}

// Only initial connection and change notifications execute the query. The
// caller's heartbeat check validates token lifetime without database I/O.
template <typename Query, typename CheckToken>
ruvia::task<void> serveSnapshots(ruvia::context& context, std::string_view topic, std::string userId, Query query, CheckToken checkToken) {
    using namespace std::chrono_literals;
    if (context.try_request_state<service::middleware::AuthenticatedUserSnapshot>()) {
        std::vector<SnapshotChannel> channels;
        channels.push_back({ "snapshot", std::string(topic), query });
        co_await serveSnapshotChannels(context, std::move(userId), std::move(channels), checkToken);
        co_return;
    }
    if (context.req().header("Accept").value_or("").find("text/event-stream") == std::string_view::npos) {
        service::common::fail(10002, "此接口需要 text/event-stream", 406);
    }
    auto subscription = bus().subscribe(context.worker(), topic);
    auto readSnapshot = [&]() -> ruvia::task<std::string> {
        service::middleware::RequestContext snapshot(context, userId);
        co_return co_await query(snapshot);
    };
    checkToken();
    auto previous = co_await readSnapshot();
    context.header("X-Accel-Buffering", "no");
    auto stream = context.stream_sse();
    context.header("Cache-Control", "no-store");
    co_await stream.write({ .data_ = previous, .event_ = "snapshot" });
    while (!stream.aborted() && !context.get_stop_token().stop_requested()) {
        const auto notification = co_await subscription->receiver.receive_for(15s, context.get_stop_token());
        if (stream.aborted() || context.get_stop_token().stop_requested()) {
            co_return;
        }
        if (!notification.has_value() && notification.status() != ruvia::worker_wait_status::timed_out) {
            co_return;
        }
        std::string next;
        std::string error;
        try {
            checkToken();
            if (notification.has_value()) {
                next = co_await readSnapshot();
            }
        } catch (const ruvia::http_error& failure) {
            const auto info = failure.info();
            error = json(service::common::error(context, service::common::errorCode(info.code(), info.status().value()), info.message()));
        } catch (const std::exception&) {
            error = "{\"code\":10004,\"message\":\"实时订阅已中断\"}";
        }
        if (!error.empty()) {
            co_await stream.write({ .data_ = error, .event_ = "error" });
            co_return;
        }
        if (notification.has_value() && next != previous) {
            previous = std::move(next);
            co_await stream.write({ .data_ = previous, .event_ = "snapshot" });
        } else if (notification.status() == ruvia::worker_wait_status::timed_out) {
            co_await context.stream().write(": keepalive\n\n");
        }
    }
}

} // namespace service::live
