#pragma once

#include <array>
#include <chrono>
#include <exception>
#include <future>
#include <iostream>
#include <string_view>

#include <ruvia/core/channel.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/redis/redis_subscription.h>
#include <ruvia/web/web_worker.h>

#include "service/features/vpn/vpn.service.h"
#include "service/utils/worker_task.h"

namespace service::vpn {

// The service outlives this Worker-local driver. RPC business operations use the
// service directly, including when background synchronization is disabled.
class VpnHubRuntime final {
  public:
    explicit VpnHubRuntime(VpnHubService& service) : service_(service) {}
    VpnHubRuntime(const VpnHubRuntime&) = delete;
    VpnHubRuntime& operator=(const VpnHubRuntime&) = delete;
    ~VpnHubRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (stopped_.valid()) {
            return;
        }
        stop_ = ruvia::stop_source{};
        service::utils::startWorkerTask(worker, stop_.token(), stopped_,
            [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                return run(context, ready, stop);
            });
    }

    void stop() noexcept {
        service::utils::stopWorkerTask(stop_, stopped_);
    }

  private:
    static ruvia::task<void> readNotifications(ruvia::redis_subscription& subscription,
                                              ruvia::channel_sender<bool> sender,
                                              std::exception_ptr& failure) {
        try {
            while (const auto event = co_await subscription.read()) {
                if (event->kind() == ruvia::redis_pubsub_event_kind::message) {
                    // Wake coalescing is safe: all work identities and deadlines
                    // live in Redis, not in this bounded Worker-local channel.
                    (void)sender.send(true);
                }
            }
        } catch (...) {
            failure = std::current_exception();
        }
        sender.close();
    }

    ruvia::task<void> watch(ruvia::web_worker_context& context, ruvia::stop_token stop) {
        ruvia::task_scope reads(context.worker());
        const auto readStop = ruvia::combine_stop_tokens(stop, reads.get_stop_token());
        auto subscription = context.redis().with_options({ .stop_token_ = readStop }).subscription();
        const auto channel = service_.scheduleChannel();
        const std::array<std::string_view, 1> channels{ channel };
        co_await subscription.subscribe(channels);
        // Confirm before reading durable deadlines: an intervening change then
        // appears either in the snapshot or in this connection's message queue.
        bool confirmed = false;
        while (!stop.stop_requested()) {
            const auto event = co_await subscription.read();
            if (!event) {
                co_return;
            }
            if (event->kind() == ruvia::redis_pubsub_event_kind::subscribed && event->channel() == channel) {
                confirmed = true;
                break;
            }
        }
        if (!confirmed) {
            co_return;
        }
        auto [sender, receiver] = ruvia::make_channel<bool>(context.worker(), { .capacity_ = 1 });
        std::exception_ptr readerFailure;
        std::exception_ptr failure;
        reads.spawn(readNotifications(subscription, sender, readerFailure));
        try {
            while (!stop.stop_requested()) {
                co_await service_.executeDue(context, stop);
                const auto deadline = co_await service_.nextDeadline(context);
                const auto wake = deadline ? co_await receiver.receive_for(*deadline, stop)
                                           : co_await receiver.receive(stop);
                if (wake.status() != ruvia::worker_wait_status::value &&
                    wake.status() != ruvia::worker_wait_status::timed_out) {
                    break;
                }
            }
        } catch (...) {
            failure = std::current_exception();
        }
        // Deadline timeouts affect only the local channel. The healthy Pub/Sub
        // connection stays open until cancellation or a real connection failure.
        reads.request_stop();
        subscription.abort();
        co_await reads.join();
        if (failure) {
            std::rethrow_exception(failure);
        }
        if (readerFailure) {
            std::rethrow_exception(readerFailure);
        }
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        co_await service_.initialize(context);
        ready.set_value();
        while (!stop.stop_requested()) {
            bool failed = false;
            try {
                co_await watch(context, stop);
            } catch (const std::exception& error) {
                if (!stop.stop_requested()) {
                    std::cerr << "VPN deadline scheduler failed: " << error.what() << '\n';
                    failed = true;
                }
            }
            if (failed) {
                // Connection/operation recovery only; business retry deadlines
                // stay durable and are claimed rather than rescanned here.
                (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(5), stop);
            }
        }
    }

    VpnHubService& service_;
    ruvia::stop_source stop_;
    std::future<void> stopped_;
};

} // namespace service::vpn
