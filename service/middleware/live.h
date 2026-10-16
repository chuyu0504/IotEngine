#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/controller.h>
#include <ruvia/web/model_json.h>
#include <ruvia/web/streaming.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/middleware/request_context.h"
#include "service/utils/change_feed.h"
#include "service/utils/json.h"

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
    std::function<ruvia::task<std::optional<std::string>>(RequestContext&, const service::utils::ChangeSet&)> update;
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

namespace live_detail {

inline std::string failurePayload(ruvia::context& context, const ruvia::http_error& failure) {
    const auto info = failure.info();
    return modelJson(service::common::error(context, service::common::errorCode(info.code(), info.status().value()), info.message()));
}

inline constexpr std::string_view kInterrupted{ "{\"code\":10004,\"message\":\"实时订阅已中断\"}" };

inline service::utils::ChangeSet relevant(const service::utils::ChangeSet& changes, const LiveChannel& channel) {
    service::utils::ChangeSet result;
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
// Permission and scope resolution is shared by the worker's live connections
// and reloaded after each auth change (ChangeFeed::authorization).
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
    auto& feed = context.worker_state<service::utils::ChangeFeed>();
    auto subscription = co_await feed.watch(context, std::move(topics));
    auto snapshot = [&](const LiveChannel& channel) -> ruvia::task<std::string> {
        std::string payload;
        try {
            RequestContext request(context, userId);
            request.shareAuthorization(feed);
            payload = co_await channel.snapshot(request);
        } catch (const ruvia::http_error& failure) {
            payload = live_detail::failurePayload(context, failure);
        } catch (const std::exception&) {
            payload = std::string(live_detail::kInterrupted);
        }
        co_return payload;
    };
    auto update = [&](const LiveChannel& channel, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
        std::optional<std::string> payload;
        try {
            RequestContext request(context, userId);
            request.shareAuthorization(feed);
            payload = co_await channel.update(request, changes);
        } catch (const ruvia::http_error& failure) {
            payload = live_detail::failurePayload(context, failure);
        } catch (const std::exception&) {
            payload = std::string(live_detail::kInterrupted);
        }
        co_return payload;
    };
    checkToken();
    std::vector<std::string> initial;
    initial.reserve(channels.size());
    for (const auto& channel : channels) {
        initial.push_back(co_await snapshot(channel));
    }
    context.header("X-Accel-Buffering", "no");
    auto stream = context.stream_sse();
    context.header("Cache-Control", "no-store");
    std::vector<std::optional<std::size_t>> sent;
    sent.reserve(channels.size());
    for (std::size_t index = 0; index < channels.size(); ++index) {
        sent.push_back(std::hash<std::string>{}(initial[index]));
        co_await stream.write({ .data_ = initial[index], .event_ = channels[index].event });
    }
    initial.clear();
    while (!stream.aborted() && !context.get_stop_token().stop_requested()) {
        const auto woke = co_await subscription->wait(15s, context.get_stop_token());
        if (stream.aborted() || context.get_stop_token().stop_requested() || woke == service::utils::ChangeWait::Stopped) {
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
        if (woke == service::utils::ChangeWait::TimedOut) {
            co_await onKeepalive();
            co_await context.stream().write(": keepalive\n\n");
            continue;
        }
        const auto changes = subscription->take();
        const bool resend = changes.recovered || changes.find(service::message::live::topic::kAuth) != nullptr;
        for (std::size_t index = 0; index < channels.size(); ++index) {
            const auto& channel = channels[index];
            std::optional<std::string> payload;
            if (const auto relevant = live_detail::relevant(changes, channel); !resend && relevant.topics.empty()) {
                continue;
            } else if (resend || !channel.update) {
                payload = co_await snapshot(channel);
            } else {
                payload = co_await update(channel, relevant);
            }
            if (!payload) {
                continue;
            }
            // A payload identical to the previous one of this channel (a change
            // already covered by the subscription snapshot, a resend after an
            // unrelated authorization change) tells the client nothing.
            const auto digest = std::hash<std::string>{}(*payload);
            if (sent[index] == digest) {
                continue;
            }
            sent[index] = digest;
            co_await stream.write({ .data_ = *payload, .event_ = channel.event });
        }
    }
}

template <typename CheckToken>
ruvia::task<void> serveLive(ruvia::context& context, std::string userId, std::vector<LiveChannel> channels, CheckToken checkToken) {
    co_await serveLive(context, std::move(userId), std::move(channels), std::move(checkToken), []() -> ruvia::task<void> {
        co_return;
    });
}

} // namespace service::middleware
