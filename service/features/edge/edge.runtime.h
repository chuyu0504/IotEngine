#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"
#include "service/utils/redis.h"

namespace service::edge {

// One instance is created by application::use_worker_state for every Service Worker.
// The session table and Redis notification Stream are worker-local: no wakeup,
// callback, or socket is forwarded to another worker.
class SessionDispatcher final {
  public:
    using SessionWake = std::function<void()>;
    using SessionFailure = std::function<void(std::string_view)>;

    SessionDispatcher() = default;
    SessionDispatcher(const SessionDispatcher&) = delete;
    SessionDispatcher& operator=(const SessionDispatcher&) = delete;
    SessionDispatcher(SessionDispatcher&&) noexcept = default;
    SessionDispatcher& operator=(SessionDispatcher&&) = delete;

    [[nodiscard]] std::size_t workerIndex() const {
        if (!workerIndex_) {
            throw std::runtime_error("edge dispatcher is not ready");
        }
        return *workerIndex_;
    }

    void registerSession(
        std::string nodeId,
        std::uint64_t epoch,
        SessionWake wake,
        SessionFailure failure = {}
    ) {
        if (!running_ || failed_ || !wake) {
            throw std::runtime_error("edge dispatcher is not ready");
        }
        sessions_.insert_or_assign(
            std::move(nodeId),
            SessionTarget{ epoch, std::move(wake), std::move(failure) }
        );
    }

    void unregisterSession(std::string_view nodeId, std::uint64_t epoch) noexcept {
        const auto current = sessions_.find(nodeId);
        if (current != sessions_.end() && current->second.epoch == epoch) {
            sessions_.erase(current);
        }
    }

    void requestStop() noexcept {
        if (stopSource_) {
            stopSource_->request_stop();
        }
        service::message::workerStreamMultiplexer().signal(
            service::message::WorkerStreamTask::EdgeDispatcher
        );
    }

    // Edge projection lease failure is a worker-local lifecycle event.  Every
    // session registered on this SessionDispatcher is closed on the same Worker so
    // no socket, callback, or mutable session state crosses workers.
    void failSessions(std::string_view reason) noexcept {
        failed_ = true;
        for (auto& [nodeId, target] : sessions_) {
            (void)nodeId;
            try {
                if (target.failure) {
                    target.failure(reason);
                }
            } catch (const std::exception& error) {
                std::cerr << "edge session failure notification failed: "
                          << error.what() << '\n';
            } catch (...) {
                std::cerr << "edge session failure notification failed: unknown error\n";
            }
        }
    }

    [[nodiscard]] bool failed() const noexcept { return failed_; }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t workerIndex, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        bool readySet = false;
        try {
            if (running_) {
                throw std::runtime_error("edge dispatcher is already running");
            }
            workerIndex_ = workerIndex;
            failed_ = false;
            stopSource_ = std::make_unique<ruvia::stop_source>();
            const auto redis = context.redis();
            const auto workerStream = service::message::edge::dispatch::stream(workerIndex, service::runtime::instanceId());
            const auto consumer = "service-edge-dispatcher-" +
                std::to_string(workerIndex);
            co_await service::message::redis::ensureGroup(
                redis,
                workerStream,
                service::message::edge::dispatch::kGroup
            );
            running_ = true;
            ready->set_value();
            readySet = true;
            bool recovering = true;
            const auto stopToken = ruvia::combine_stop_tokens(
                context.get_stop_token(),
                stopSource_->token()
            );
            while (!stopToken.stop_requested()) {
                bool failed = false;
                try {
                    auto messages = recovering
                        ? co_await service::message::redis::readGroup(
                              redis,
                              workerStream,
                              service::message::edge::dispatch::kGroup,
                              consumer,
                              "0",
                              std::chrono::milliseconds(0),
                              256
                          )
                        : co_await service::message::redis::readGroup(
                              redis,
                              workerStream,
                              service::message::edge::dispatch::kGroup,
                              consumer,
                              ">",
                              std::chrono::milliseconds(0),
                              256
                          );
                    if (recovering && messages.empty()) {
                        recovering = false;
                        continue;
                    }
                    if (messages.empty()) {
                        co_await service::message::workerStreamMultiplexer().wait(
                            workerIndex,
                            service::message::WorkerStreamTask::EdgeDispatcher,
                            stopToken
                        );
                        continue;
                    }
                    for (const auto& message : messages) {
                        const auto event = service::message::edge::dispatch::eventFrom(message);
                        if (event.kind == service::message::edge::dispatch::kNodeKind &&
                            !event.nodeId.empty() && !deliver(event.nodeId)) {
                            recovering = true;
                            failed = true;
                            break;
                        }
                        co_await service::message::redis::acknowledgeAndDelete(
                            redis,
                            workerStream,
                            service::message::edge::dispatch::kGroup,
                            message.id
                        );
                    }
                } catch (const std::exception& error) {
                    if (stopToken.stop_requested()) {
                        break;
                    }
                    std::cerr << "edge dispatch worker " << workerIndex
                              << " read failed: " << error.what() << '\n';
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
        running_ = false;
        sessions_.clear();
        stopSource_.reset();
        try {
            stopped->set_value();
        } catch (...) {
        }
    }

  private:
    struct SessionTarget {
        std::uint64_t epoch{};
        SessionWake wake;
        SessionFailure failure;
    };

    bool deliver(std::string_view nodeId) {
        const auto current = sessions_.find(nodeId);
        if (current == sessions_.end()) {
            return true;
        }
        try {
            current->second.wake();
            return true;
        } catch (const std::exception& error) {
            std::cerr << "edge dispatch worker " << *workerIndex_
                      << " wake failed: " << error.what() << '\n';
            return false;
        } catch (...) {
            std::cerr << "edge dispatch worker " << *workerIndex_
                      << " wake failed: unknown error\n";
            return false;
        }
    }

    std::map<std::string, SessionTarget, std::less<>> sessions_;
    std::unique_ptr<ruvia::stop_source> stopSource_;
    std::optional<std::size_t> workerIndex_;
    bool running_{ false };
    bool failed_{ false };
};

// One supervisor is assembled for each Service Worker. Functional state remains
// in that worker's SessionDispatcher instance above.
class DispatcherRuntime final {
  public:
    DispatcherRuntime() = default;
    DispatcherRuntime(const DispatcherRuntime&) = delete;
    DispatcherRuntime& operator=(const DispatcherRuntime&) = delete;

    ~DispatcherRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex, std::size_t serviceWorkerCount) {
        if (running_.exchange(true)) {
            return;
        }
        worker_ = std::move(worker);
        workerIndex_ = workerIndex;
        serviceWorkerCount_ = serviceWorkerCount;
        if (!worker_.valid() || serviceWorkerCount_ == 0 ||
            workerIndex_ >= serviceWorkerCount_) {
            running_.store(false);
            worker_ = {};
            throw std::runtime_error("edge dispatcher requires service workers");
        }
        auto ready = std::make_shared<std::promise<void>>();
        auto stopped = std::make_shared<std::promise<void>>();
        auto readiness = ready->get_future();
        stopped_ = stopped->get_future().share();
        try {
            const auto posted = worker_.post(
                [workerIndex, ready, stopped](ruvia::web_worker_context& context) {
                    return context.worker_state<SessionDispatcher>().run(
                        context,
                        workerIndex,
                        ready,
                        stopped
                    );
                }
            );
            if (!posted.accepted()) {
                stopped->set_value();
                throw std::runtime_error("service worker rejected edge dispatcher");
            }
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
        if (worker_.valid()) {
            const auto worker = worker_;
            const auto wakeAccepted = worker.post([](ruvia::web_worker_context& context) -> ruvia::task<void> {
                                                context.worker_state<SessionDispatcher>().requestStop();
                                                co_return;
                                            })
                                          .accepted();
            if (!wakeAccepted) {
                // A closed worker already propagates its stop token to SessionDispatcher.
            }
        }
        if (stopped_.valid()) {
            stopped_.wait();
        }
        stopped_ = {};
        worker_ = {};
    }

  private:
    ruvia::web_worker_handle worker_;
    std::shared_future<void> stopped_;
    std::size_t workerIndex_ = 0;
    std::size_t serviceWorkerCount_ = 0;
    std::atomic_bool running_{ false };
};

} // namespace service::edge
