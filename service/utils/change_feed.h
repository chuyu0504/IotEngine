#pragma once

#include <any>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/channel.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/core/worker_signal.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/utils/redis.h"

namespace service::utils {

// Changes delivered to one subscription since it last took them. A topic mapped
// to std::nullopt changed as a whole (no entity ids were published).
struct ChangeSet final {
    using Ids = std::set<std::string, std::less<>>;

    std::map<std::string, std::optional<Ids>, std::less<>> topics;
    // The reader lost its position (Redis failure); subscribers resend their
    // authorized state once instead of trusting the missed interval.
    bool recovered = false;

    [[nodiscard]] bool empty() const noexcept { return topics.empty() && !recovered; }

    [[nodiscard]] const std::optional<Ids>* find(std::string_view topic) const {
        const auto found = topics.find(topic);
        return found == topics.end() ? nullptr : &found->second;
    }

    void add(std::string_view topic, std::string_view ids) {
        auto [entry, inserted] = topics.try_emplace(std::string(topic), Ids{});
        if (!entry->second) {
            return;
        }
        const auto split = service::message::live::splitIds(ids);
        if (split.empty()) {
            entry->second.reset();
            return;
        }
        for (const auto id : split) {
            entry->second->emplace(id);
        }
        if (entry->second->size() > service::message::live::kMaximumIds) {
            entry->second.reset();
        }
    }
};

enum class ChangeWait {
    Changed,
    TimedOut,
    Stopped,
};

// One reader per Service Worker for the broadcast change stream and the worker's
// own directed stream. Ruvia rejects Redis Pub/Sub and gives each blocking read
// its own connection from a small per-worker pool, so subscriptions on a worker
// share this single XREAD instead of blocking one connection each. The instance
// lives in Ruvia worker state; the reader starts with the first subscription and
// stops after it has been idle for kLinger.
class ChangeFeed final {
  public:
    class Subscription final {
      public:
        Subscription(const ruvia::worker_handle& worker, std::vector<std::string> topics, std::optional<std::string> directed)
            : topics_(std::move(topics)), directed_(std::move(directed)), wake_(make(worker)) {}

        // Waits for pending changes. Pending changes taken earlier never block.
        ruvia::task<ChangeWait> wait(std::chrono::milliseconds timeout, ruvia::stop_token stop) {
            if (!pending_.empty()) {
                co_return ChangeWait::Changed;
            }
            const auto result = co_await wake_.receiver.receive_for(timeout, std::move(stop));
            if (result.has_value()) {
                co_return ChangeWait::Changed;
            }
            co_return result.status() == ruvia::worker_wait_status::timed_out ? ChangeWait::TimedOut
                                                                              : ChangeWait::Stopped;
        }

        [[nodiscard]] ChangeSet take() { return std::exchange(pending_, {}); }

        // Directed subscriptions may start on every directed message (empty
        // topic) and narrow once the awaited topic is known.
        void retarget(std::string topic) { directed_ = std::move(topic); }

      private:
        friend class ChangeFeed;

        struct Wake {
            ruvia::channel_sender<int> sender;
            ruvia::channel_receiver<int> receiver;
        };

        static Wake make(const ruvia::worker_handle& worker) {
            auto [sender, receiver] = ruvia::make_channel<int>(worker, { .capacity_ = 1 });
            return Wake{ std::move(sender), std::move(receiver) };
        }

        [[nodiscard]] bool watches(std::string_view topic) const {
            for (const auto& watched : topics_) {
                if (watched == topic) {
                    return true;
                }
            }
            return false;
        }

        void deliver(std::string_view topic, std::string_view ids) {
            pending_.add(topic, ids);
            (void)wake_.sender.send(1);
        }

        void recover() {
            pending_.recovered = true;
            (void)wake_.sender.send(1);
        }

        std::vector<std::string> topics_;
        std::optional<std::string> directed_;
        Wake wake_;
        ChangeSet pending_;
    };

    ChangeFeed() = default;
    ChangeFeed(const ChangeFeed&) = delete;
    ChangeFeed& operator=(const ChangeFeed&) = delete;
    // Ruvia constructs worker state from a factory value; never moved after use.
    ChangeFeed(ChangeFeed&&) noexcept = default;
    ChangeFeed& operator=(ChangeFeed&&) = delete;

    // Broadcast business changes for `topics`. Returns once the reader is
    // positioned before every change committed after this call started.
    template <typename Context>
    ruvia::task<std::shared_ptr<Subscription>> watch(Context& context, std::vector<std::string> topics) {
        co_return co_await subscribe(context, std::move(topics), std::nullopt);
    }

    // Directed messages addressed to this worker. An empty topic receives every
    // directed message until Subscription::retarget narrows it.
    template <typename Context>
    ruvia::task<std::shared_ptr<Subscription>> await(Context& context, std::string topic) {
        co_return co_await subscribe(context, {}, std::move(topic));
    }

    // This worker's directed stream; replies and frames for its waiters go here.
    template <typename Context>
    static std::string streamOf(Context& context) {
        const auto& topology = context.template worker_state<service::ServiceWorkerTopology>();
        if (!topology.index) {
            throw std::logic_error("service worker index is not initialized");
        }
        return service::message::live::workerStream(service::runtime::instanceId(), *topology.index);
    }

    // Routes a directed topic to this worker for `lifetime`; producers that
    // cannot know the waiter resolve the stream through this route key.
    template <typename Context>
    static ruvia::task<void> route(Context& context, std::string_view topic, std::chrono::seconds lifetime) {
        const std::vector<std::string> args{ "SET", service::message::live::routeKey(topic), streamOf(context), "EX", std::to_string(lifetime.count()) };
        const auto reply = co_await service::message::redis::command(context.redis(), args);
        if (reply.kind() == ruvia::redis_value::kind_type::error) {
            service::message::redis::throwValue("SET live route", reply);
        }
    }

    // Authorization facts (permission grants, data-scope actors) shared by the
    // live connections of this worker: one load per key, reused until the
    // reader sees an auth change, loses its position or stops. Revocation thus
    // reaches every connection with the auth event that also makes it resend
    // its snapshots. Without a positioned reader nothing is kept.
    template <typename Value, typename Load>
    ruvia::task<Value> authorization(std::string key, Load load) {
        for (;;) {
            if (!ready_) {
                co_return co_await load();
            }
            const auto found = authorizations_.find(key);
            if (found == authorizations_.end()) {
                break;
            }
            if (found->second.has_value()) {
                co_return std::any_cast<Value>(found->second);
            }
            // Another connection is loading this key; it notifies when done.
            co_await positioned_->signal.wait();
        }
        authorizations_.emplace(key, std::any{});
        // Releases the in-flight marker however the load ends (value, error or
        // a cancelled connection destroying this frame) and wakes the waiters.
        PendingAuthorization pending{ *this, key, authorizationGeneration_ };
        Value value = co_await load();
        if (pending.generation == authorizationGeneration_) {
            authorizations_.insert_or_assign(std::move(key), value);
        }
        co_return value;
    }

  private:
    struct PendingAuthorization final {
        ~PendingAuthorization() {
            if (generation == feed.authorizationGeneration_) {
                const auto found = feed.authorizations_.find(key);
                if (found != feed.authorizations_.end() && !found->second.has_value()) {
                    feed.authorizations_.erase(found);
                }
            }
            feed.positioned_->signal.notify();
        }

        ChangeFeed& feed;
        std::string key;
        std::uint64_t generation;
    };

    static constexpr auto kLinger = std::chrono::seconds(60);
    static constexpr std::size_t kBatch = 512;

    template <typename Context>
    ruvia::task<std::shared_ptr<Subscription>> subscribe(Context& context, std::vector<std::string> topics, std::optional<std::string> directed) {
        if (!context.worker().is_current()) {
            throw std::logic_error("change subscriptions must belong to the current worker");
        }
        if (!positioned_) {
            positioned_ = std::make_unique<Positioning>(context.worker());
        }
        auto subscription = std::make_shared<Subscription>(context.worker(), std::move(topics), std::move(directed));
        std::erase_if(subscriptions_, [](const auto& entry) {
            return entry.expired();
        });
        subscriptions_.push_back(subscription);
        if (!running_) {
            start(context);
        }
        while (!ready_) {
            if (failure_) {
                std::rethrow_exception(failure_);
            }
            co_await positioned_->signal.wait();
        }
        co_return subscription;
    }

    template <typename Context>
    void start(Context& context) {
        const auto& topology = context.template worker_state<service::ServiceWorkerTopology>();
        if (!topology.index) {
            throw std::logic_error("service worker index is not initialized");
        }
        const auto worker = context.template worker_state<ruvia::web_worker_handle>();
        running_ = true;
        ready_ = false;
        failure_ = nullptr;
        streams_ = { std::string(service::message::live::kChanges),
                     service::message::live::workerStream(service::runtime::instanceId(), *topology.index) };
        if (!worker.post([this](ruvia::web_worker_context& reader) {
                       return read(reader);
                   })
                 .accepted()) {
            running_ = false;
            throw std::runtime_error("change feed reader rejected");
        }
    }

    [[nodiscard]] bool idle() {
        std::erase_if(subscriptions_, [](const auto& entry) {
            return entry.expired();
        });
        return subscriptions_.empty();
    }

    ruvia::task<void> read(ruvia::web_worker_context& context) {
        const auto redis = context.redis();
        const auto stop = context.get_stop_token();
        try {
            cursors_ = { co_await service::message::redis::lastEntryId(redis, streams_[0]),
                         co_await service::message::redis::lastEntryId(redis, streams_[1]) };
            ready_ = true;
        } catch (...) {
            failure_ = std::current_exception();
        }
        positioned_->signal.notify();
        if (!ready_) {
            running_ = false;
            co_return;
        }
        while (!stop.stop_requested()) {
            bool failed = false;
            try {
                const auto batches = co_await service::message::redis::readStreamsBlocking(
                    redis,
                    streams_,
                    cursors_,
                    stop,
                    kLinger,
                    kBatch
                );
                if (batches.empty() && idle()) {
                    break;
                }
                for (const auto& batch : batches) {
                    const bool broadcast = batch.stream == streams_[0];
                    for (const auto& entry : batch.messages) {
                        deliver(broadcast, entry);
                    }
                    if (!batch.messages.empty()) {
                        cursors_[broadcast ? 0 : 1] = batch.messages.back().id;
                    }
                }
            } catch (const std::exception& error) {
                if (stop.stop_requested()) {
                    break;
                }
                spdlog::warn("live change feed read failed: {}", error.what());
                failed = true;
            }
            if (failed) {
                // Auth changes may have been missed while the position was lost.
                clearAuthorizations();
                for (const auto& entry : subscriptions_) {
                    if (const auto subscription = entry.lock()) {
                        subscription->recover();
                    }
                }
                (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1), stop);
            }
        }
        running_ = false;
        ready_ = false;
        clearAuthorizations();
    }

    void clearAuthorizations() {
        ++authorizationGeneration_;
        authorizations_.clear();
    }

    void deliver(bool broadcast, const service::message::StreamMessage& entry) {
        const auto topic = entry.get(service::message::live::kTopicField);
        if (topic.empty()) {
            return;
        }
        const auto ids = broadcast ? entry.get(service::message::live::kIdsField) : std::string_view{};
        if (broadcast && topic == service::message::live::topic::kAuth) {
            clearAuthorizations();
        }
        std::erase_if(subscriptions_, [&](const auto& weak) {
            const auto subscription = weak.lock();
            if (!subscription) {
                return true;
            }
            if (broadcast ? subscription->watches(topic)
                          : subscription->directed_ && (subscription->directed_->empty() || *subscription->directed_ == topic)) {
                subscription->deliver(topic, ids);
            }
            return false;
        });
    }

    std::vector<std::weak_ptr<Subscription>> subscriptions_;

    // worker_signal borrows its worker handle; both stay at a stable address.
    struct Positioning {
        explicit Positioning(ruvia::worker_handle owner) : worker(std::move(owner)), signal(worker) {}

        ruvia::worker_handle worker;
        ruvia::worker_signal signal;
    };

    std::unique_ptr<Positioning> positioned_;
    std::exception_ptr failure_;
    std::vector<std::string> streams_;
    std::vector<std::string> cursors_;
    bool running_ = false;
    bool ready_ = false;
    // Values of authorization(); an empty std::any marks a load in flight.
    std::map<std::string, std::any, std::less<>> authorizations_;
    std::uint64_t authorizationGeneration_ = 0;
};

} // namespace service::utils
