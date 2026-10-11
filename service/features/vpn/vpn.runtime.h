#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/web/web_worker.h>

#include "service/features/vpn/vpn.service.h"
#include "service/features/vpn/wireguard/wireguard.transport.h"
#include "service/utils/json.h"

namespace service::vpn {

class VpnHubRuntime final {
  public:
    explicit VpnHubRuntime(wireguard::HubConfig config) : hubConfig_(std::move(config)) {}

    VpnHubRuntime(const VpnHubRuntime&) = delete;
    VpnHubRuntime& operator=(const VpnHubRuntime&) = delete;

    ~VpnHubRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (running_.exchange(true)) {
            return;
        }
        worker_ = worker;
        auto ready = std::make_shared<std::promise<void>>();
        auto stopped = std::make_shared<std::promise<void>>();
        stopped_ = stopped->get_future().share();
        const auto posted = worker_.post([this, ready, stopped](ruvia::web_worker_context& context) {
            return run(context, ready, stopped);
        });
        if (!posted.accepted()) {
            running_.store(false);
            worker_ = {};
            stopped_ = {};
            throw std::runtime_error("service rejected VPN runtime");
        }
        ready->get_future().get();
    }

    void stop() noexcept {
        if (!running_.exchange(false)) {
            return;
        }
        if (stopped_.valid()) {
            stopped_.wait();
        }
        stopped_ = {};
        worker_ = {};
    }

  private:
    ruvia::task<void> run(ruvia::web_worker_context& context, const std::shared_ptr<std::promise<void>>& ready, const std::shared_ptr<std::promise<void>>& stopped) {
        try {
            ready->set_value();
            while (running_.load() && !context.get_stop_token().stop_requested()) {
                try {
                    co_await VpnHubService::reconcile(context, hubConfig_, true);
                } catch (const std::exception& error) {
                    std::cerr << "VPN runtime reconciliation failed: " << error.what() << '\n';
                }
                for (int tick = 0; tick < 10 && running_.load() &&
                     !context.get_stop_token().stop_requested();
                     ++tick) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(1));
                }
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

    std::atomic_bool running_{ false };
    wireguard::HubConfig hubConfig_;
    ruvia::web_worker_handle worker_;
    std::shared_future<void> stopped_;
};

} // namespace service::vpn
