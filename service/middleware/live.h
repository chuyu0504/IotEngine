#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/channel.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/controller.h>
#include <ruvia/web/model_json.h>
#include <ruvia/web/streaming.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/middleware/request_context.h"
#include "service/utils/json.h"
#include "service/utils/log.h"

namespace service::middleware {

template <typename Model>
std::string modelJson(const Model& model) {
    return std::string(ruvia::to_json(model));
}

// Success envelope around an already serialized JSON value.
inline std::string dataJson(std::string_view value) {
    return "{\"code\":0,\"message\":\"ok\",\"data\":" + std::string(value) + "}";
}

// Entity-level change of one collection channel: rows to insert or replace by
// id, and ids no longer visible. The client merges it into its cached snapshot.
inline std::string deltaJson(std::string_view upsertArray, std::string_view removeArray) {
    return "{\"code\":0,\"message\":\"ok\",\"delta\":{\"upsert\":" + std::string(upsertArray) +
        ",\"remove\":" + std::string(removeArray) + "}}";
}

inline std::string idArrayJson(const std::vector<std::string>& ids) {
    std::string result = "[";
    for (const auto& id : ids) {
        if (result.size() > 1) {
            result.push_back(',');
        }
        result += service::utils::jsonQuoted(id);
    }
    result.push_back(']');
    return result;
}

// One named SSE event stream of a page connection. The owning module declares
// which change topics affect it and how to answer them.
struct LiveChannel {
    std::string event;
    // Change topics (service::message::live::topic) this channel depends on.
    std::vector<std::string> topics;
    // Complete authorized payload: initial subscription, recovery and changes
    // whose scope cannot be narrowed to entities.
    std::function<ruvia::task<std::string>(RequestContext&)> snapshot;
    // Entity-level answer to `changes` (only this channel's topics). Returns the
    // payload to send (usually deltaJson) or nullopt when nothing visible
    // changed. Channels without it resend `snapshot` for every relevant change.
    std::function<ruvia::task<std::optional<std::string>>(RequestContext&, const service::message::live::ChangeSet&)> update;
};

// Entity membership of one collection channel (protocol state, no I/O). The
// channel records the rows of its last snapshot (id and content digest);
// re-queried rows of changed ids become a delta while the visible set stays
// expressible by rows alone, and rows whose content did not change are omitted.
class LiveCollection final {
  public:
    struct Row {
        std::string id;
        std::string json;
    };

    enum class Outcome {
        Unchanged,
        Delta,
        // The visible set gained ids, or a paginated page lost one: rows cannot
        // express the new order or page, so the channel resends its snapshot.
        Snapshot,
    };

    struct Answer {
        Outcome outcome = Outcome::Unchanged;
        std::string payload;
    };

    explicit LiveCollection(bool paginated = false) : paginated_(paginated) {}

    // Rows of the snapshot just sent, serialized like the delta rows.
    void assign(const std::vector<Row>& rows) {
        visible_.clear();
        for (const auto& row : rows) {
            visible_.insert_or_assign(row.id, digest(row.json));
        }
    }

    [[nodiscard]] bool contains(std::string_view id) const { return visible_.contains(id); }

    // `requested`: changed ids; `rows`: those still visible in this channel's
    // scope, serialized in the snapshot row shape.
    [[nodiscard]] Answer answer(const std::set<std::string, std::less<>>& requested, const std::vector<Row>& rows) {
        std::set<std::string_view> present;
        for (const auto& row : rows) {
            if (!visible_.contains(row.id)) {
                return { Outcome::Snapshot, {} };
            }
            present.insert(row.id);
        }
        std::vector<std::string> removed;
        for (const auto& id : requested) {
            if (visible_.contains(id) && !present.contains(id)) {
                removed.push_back(id);
            }
        }
        if (!removed.empty() && paginated_) {
            return { Outcome::Snapshot, {} };
        }
        std::string upsert = "[";
        for (const auto& row : rows) {
            auto& current = visible_.find(row.id)->second;
            const auto next = digest(row.json);
            if (current == next) {
                continue;
            }
            current = next;
            if (upsert.size() > 1) {
                upsert.push_back(',');
            }
            upsert += row.json;
        }
        upsert.push_back(']');
        if (upsert.size() == 2 && removed.empty()) {
            return {};
        }
        for (const auto& id : removed) {
            visible_.erase(visible_.find(id));
        }
        return { Outcome::Delta, deltaJson(upsert, idArrayJson(removed)) };
    }

  private:
    static std::size_t digest(std::string_view json) { return std::hash<std::string_view>{}(json); }

    bool paginated_;
    std::map<std::string, std::size_t, std::less<>> visible_;
};

enum class LiveWait {
    Changed,
    TimedOut,
    Stopped,
};

// Request-owned SSE transport, not a business reader or Worker state. Its only
// I/O is the dedicated subscription; modules retain all query/scope decisions.
class LiveSubscription final {
  public:
    LiveSubscription(ruvia::context& context, std::vector<std::string> topics)
        : context_(context), topics_(std::move(topics)), wake_(ruvia::make_channel<int>(context.worker(), { .capacity_ = 1 })) {
        std::sort(topics_.begin(), topics_.end());
        topics_.erase(std::unique(topics_.begin(), topics_.end()), topics_.end());
        for (const auto& topic : topics_) {
            channels_.push_back(service::message::live::channel(topic));
        }
    }

    [[nodiscard]] bool ready() const { return ready_; }

    [[nodiscard]] auto generation() const { return authorization.generation(); }

    ruvia::task<LiveWait> wait(std::chrono::milliseconds timeout) {
        if (ready_ && !pending_.empty()) {
            co_return LiveWait::Changed;
        }
        const auto result = co_await wake_.second.receive_for(timeout, context_.get_stop_token());
        if (failure_) {
            std::rethrow_exception(failure_);
        }
        if (result.has_value()) {
            co_return LiveWait::Changed;
        }
        co_return result.status() == ruvia::worker_wait_status::timed_out ? LiveWait::TimedOut : LiveWait::Stopped;
    }

    [[nodiscard]] service::message::live::ChangeSet take() { return std::exchange(pending_, {}); }

    ruvia::task<void> read(ruvia::stop_token stop) {
        using namespace std::chrono_literals;
        bool connected = false;
        try {
            std::vector<std::string_view> names;
            for (const auto& channel : channels_) {
                names.push_back(channel);
            }
            while (!stop.stop_requested()) {
                try {
                    auto subscription = context_.redis().with_options({ .stop_token_ = stop }).subscription();
                    co_await subscription.subscribe(names);
                    std::set<std::string_view> confirmed;
                    while (confirmed.size() != names.size()) {
                        const auto event = co_await subscription.read();
                        if (!event) {
                            if (stop.stop_requested()) {
                                co_return;
                            }
                            throw std::runtime_error("SSE subscription closed before confirmation");
                        }
                        if (event->kind() == ruvia::redis_pubsub_event_kind::subscribed) {
                            const auto name = std::find(names.begin(), names.end(), event->channel());
                            if (name != names.end()) {
                                confirmed.insert(*name);
                            }
                        } else if (event->kind() == ruvia::redis_pubsub_event_kind::message) {
                            accept(*event);
                        }
                    }
                    // Queries already in flight may have repopulated the cache
                    // after loss was observed. Auth messages published in that
                    // unpositioned interval were not delivered to this socket.
                    // Re-establish the authorization generation before exposing
                    // readiness or allowing a recovery snapshot to use the cache.
                    authorization.invalidate();
                    ready_ = true;
                    pending_.recovered = connected;
                    connected = true;
                    notify();
                    while (const auto event = co_await subscription.read()) {
                        if (event->kind() == ruvia::redis_pubsub_event_kind::message) {
                            accept(*event);
                        } else if (event->kind() == ruvia::redis_pubsub_event_kind::unsubscribed) {
                            throw std::runtime_error("SSE subscription was removed");
                        }
                    }
                    if (!stop.stop_requested()) {
                        throw std::runtime_error("SSE subscription closed");
                    }
                } catch (const std::exception& failure) {
                    if (stop.stop_requested()) {
                        co_return;
                    }
                    LOG_WARN << "SSE Redis subscription interrupted: " << failure.what();
                    const bool lost = std::exchange(ready_, false);
                    pending_ = {};
                    authorization.invalidate();
                    if (lost) {
                        notify();
                    }
                }
                if (!stop.stop_requested()) {
                    (void)co_await ruvia::sleep_for(context_.worker(), 1s, stop);
                }
            }
        } catch (...) {
            ready_ = false;
            authorization.invalidate();
            failure_ = std::current_exception();
            notify();
        }
    }

    AuthorizationCache authorization;

  private:
    void notify() { (void)wake_.first.send(1); }

    void accept(const ruvia::redis_pubsub_event& event) {
        const auto found = std::find(channels_.begin(), channels_.end(), event.channel());
        if (found == channels_.end()) {
            return;
        }
        const auto& topic = topics_[static_cast<std::size_t>(found - channels_.begin())];
        if (topic == service::message::live::topic::kAuth || topic == service::message::live::topic::kAccess) {
            authorization.invalidate();
        }
        pending_.add(topic, event.payload());
        notify();
    }

    ruvia::context& context_;
    std::vector<std::string> topics_;
    std::vector<std::string> channels_;
    std::pair<ruvia::channel_sender<int>, ruvia::channel_receiver<int>> wake_;
    service::message::live::ChangeSet pending_;
    std::exception_ptr failure_;
    bool ready_ = false;
};

// Always stop and join the reader before its subscription, pending values and
// request context are destroyed, including when a query or socket write fails.
template <typename Serve>
ruvia::task<void> withLiveSubscription(ruvia::context& context, std::vector<std::string> topics, Serve serve) {
    LiveSubscription subscription(context, std::move(topics));
    ruvia::task_scope reader(context.worker(), { .resource_ = context.pool() });
    reader.spawn(subscription.read(ruvia::combine_stop_tokens(reader.get_stop_token(), context.get_stop_token())));
    std::exception_ptr failure;
    try {
        while (!subscription.ready() && !context.get_stop_token().stop_requested()) {
            if (co_await subscription.wait(std::chrono::seconds(15)) == LiveWait::Stopped) {
                break;
            }
        }
        if (subscription.ready() && !context.get_stop_token().stop_requested()) {
            co_await serve(subscription);
        }
    } catch (...) {
        failure = std::current_exception();
    }
    reader.request_stop();
    co_await reader.join();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

namespace live_detail {

inline std::string failurePayload(ruvia::context& context, const ruvia::http_error& failure) {
    const auto info = failure.info();
    return modelJson(service::common::error(context, service::common::errorCode(info.code(), info.status().value()), info.message()));
}

inline constexpr std::string_view kInterrupted{ "{\"code\":10004,\"message\":\"实时订阅已中断\"}" };

inline service::message::live::ChangeSet relevant(const service::message::live::ChangeSet& changes, const LiveChannel& channel) {
    service::message::live::ChangeSet result;
    for (const auto& topic : channel.topics) {
        if (const auto* ids = changes.find(topic)) {
            result.topics.emplace(topic, *ids);
        }
    }
    return result;
}

} // namespace live_detail

// Serves the page SSE connection. Each channel sends one snapshot on subscription
// and on recovery; afterwards only committed changes of its declared topics reach
// it, answered by `update` with the affected entities. Authorization changes
// (topic auth) re-run every channel because they may change the visible scope.
// Authorization facts are cached only by this connection and invalidated as
// soon as an auth notification or subscription interruption is observed.
template <typename CheckToken, typename OnKeepalive>
ruvia::task<void> serveLive(ruvia::context& context, std::string userId, std::vector<LiveChannel> channels, CheckToken checkToken, OnKeepalive onKeepalive) {
    using namespace std::chrono_literals;
    if (context.req().header("Accept").value_or("").find("text/event-stream") == std::string_view::npos) {
        service::common::fail(10002, "此接口需要 text/event-stream", 406);
    }
    if (const auto* user = context.try_request_state<AuthenticatedUserSnapshot>()) {
        channels.push_back({ "user", {}, user->query, {} });
    }
    std::vector<std::string> topics{ std::string(service::message::live::topic::kAuth) };
    for (const auto& channel : channels) {
        for (const auto& topic : channel.topics) {
            if (std::find(topics.begin(), topics.end(), topic) == topics.end()) {
                topics.push_back(topic);
            }
        }
    }
    co_await withLiveSubscription(context, std::move(topics), [&](LiveSubscription& subscription) -> ruvia::task<void> {
        auto snapshot = [&](const LiveChannel& channel) -> ruvia::task<std::string> {
            try {
                RequestContext request(context, userId);
                request.shareAuthorization(subscription.authorization);
                co_return co_await channel.snapshot(request);
            } catch (const ruvia::http_error& failure) {
                co_return live_detail::failurePayload(context, failure);
            } catch (const std::exception& failure) {
                LOG_WARN << "SSE snapshot failed: " << failure.what();
                co_return std::string(live_detail::kInterrupted);
            }
        };
        auto update = [&](const LiveChannel& channel, const service::message::live::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
            try {
                RequestContext request(context, userId);
                request.shareAuthorization(subscription.authorization);
                co_return co_await channel.update(request, changes);
            } catch (const ruvia::http_error& failure) {
                co_return live_detail::failurePayload(context, failure);
            } catch (const std::exception& failure) {
                LOG_WARN << "SSE update failed: " << failure.what();
                co_return std::string(live_detail::kInterrupted);
            }
        };
        checkToken();
        context.header("X-Accel-Buffering", "no");
        context.header("Cache-Control", "no-store");
        auto stream = context.stream_sse();
        std::vector<std::optional<std::size_t>> sent(channels.size());
        bool initial = true;
        bool refresh = true;
        bool recoveryPending = false;
        while (!stream.aborted() && !context.get_stop_token().stop_requested()) {
            auto changes = subscription.take();
            if (!subscription.ready()) {
                refresh = true;
                const auto woke = co_await subscription.wait(15s);
                if (woke == LiveWait::Stopped) {
                    co_return;
                }
                if (woke == LiveWait::TimedOut) {
                    std::string error;
                    try {
                        checkToken();
                    } catch (const ruvia::http_error& failure) {
                        error = live_detail::failurePayload(context, failure);
                    }
                    if (!error.empty()) {
                        co_await stream.write({ .data_ = error, .event_ = "error" });
                        co_return;
                    }
                    co_await context.stream().write(": keepalive\n\n");
                }
                continue;
            }
            recoveryPending = recoveryPending || changes.recovered;
            const bool resend = refresh || recoveryPending || changes.find(service::message::live::topic::kAuth);
            refresh = false;
            const auto generation = subscription.generation();
            std::vector<std::optional<std::string>> payloads(channels.size());
            for (std::size_t index = 0; index < channels.size(); ++index) {
                const auto& channel = channels[index];
                const auto relevant = live_detail::relevant(changes, channel);
                if (!resend && relevant.topics.empty()) {
                    continue;
                }
                if (resend || !channel.update) {
                    payloads[index] = co_await snapshot(channel);
                } else {
                    payloads[index] = co_await update(channel, relevant);
                }
            }
            // An auth event or connection loss during an awaited DB query
            // invalidates its result too, not merely the next cached lookup.
            if (!subscription.ready() || generation != subscription.generation()) {
                refresh = true;
                continue;
            }
            for (std::size_t index = 0; index < channels.size(); ++index) {
                if (!subscription.ready() || generation != subscription.generation()) {
                    refresh = true;
                    break;
                }
                const auto& payload = payloads[index];
                if (!payload) {
                    continue;
                }
                const auto digest = std::hash<std::string>{}(*payload);
                // Recovery is observable even when the persisted state is
                // identical; ordinary/auth duplicates remain suppressed.
                if (!initial && !recoveryPending && sent[index] == digest) {
                    continue;
                }
                sent[index] = digest;
                co_await stream.write({ .data_ = *payload, .event_ = channels[index].event });
            }
            if (refresh) {
                continue;
            }
            recoveryPending = false;
            initial = false;
            const auto woke = co_await subscription.wait(15s);
            if (woke == LiveWait::Stopped) {
                co_return;
            }
            std::string error;
            try {
                checkToken();
            } catch (const ruvia::http_error& failure) {
                error = live_detail::failurePayload(context, failure);
            }
            if (!error.empty()) {
                co_await stream.write({ .data_ = error, .event_ = "error" });
                co_return;
            }
            if (woke == LiveWait::TimedOut && subscription.ready()) {
                co_await onKeepalive();
                co_await context.stream().write(": keepalive\n\n");
            }
        }
    });
}

template <typename CheckToken>
ruvia::task<void> serveLive(ruvia::context& context, std::string userId, std::vector<LiveChannel> channels, CheckToken checkToken) {
    co_await serveLive(context, std::move(userId), std::move(channels), std::move(checkToken), []() -> ruvia::task<void> {
        co_return;
    });
}

} // namespace service::middleware
