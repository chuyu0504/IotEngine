#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/access/access.service.h"
#include "service/features/alert/alert.service.h"

namespace service::alert {

class AlertBootstrap final {
public:
  AlertBootstrap() = default;
  AlertBootstrap(const AlertBootstrap &) = delete;
  AlertBootstrap &operator=(const AlertBootstrap &) = delete;
  ~AlertBootstrap() { stop(); }

  void start(ruvia::web_worker_handle worker, std::size_t workerIndex,
             std::size_t serviceWorkerCount) {
    if (running_.exchange(true))
      return;
    worker_ = std::move(worker);
    workerIndex_ = workerIndex;
    serviceWorkerCount_ = serviceWorkerCount;
    if (!worker_.valid() || serviceWorkerCount_ == 0 ||
        workerIndex_ >= serviceWorkerCount_) {
      running_.store(false);
      worker_ = {};
      throw std::runtime_error("alert runtime requires Service Workers");
    }
    auto ready = std::make_shared<std::promise<void>>();
    auto stopped = std::make_shared<std::promise<void>>();
    auto readiness = ready->get_future();
    stopped_ = stopped->get_future().share();
    const auto posted =
        worker_.post([this, ready, stopped](ruvia::web_worker_context &context) {
          return run(context, ready, stopped);
        });
    if (!posted.accepted()) {
      stopped->set_value();
      running_.store(false);
      worker_ = {};
      stopped_ = {};
      throw std::runtime_error("service worker rejected alert runtime");
    }
    try {
      readiness.get();
    } catch (...) {
      stop();
      throw;
    }
  }

  void stop() noexcept {
    if (!running_.exchange(false))
      return;
    if (stopped_.valid())
      stopped_.wait();
    stopped_ = {};
    worker_ = {};
  }

private:
  ruvia::task<void> run(ruvia::web_worker_context &context,
                        std::shared_ptr<std::promise<void>> ready,
                        std::shared_ptr<std::promise<void>> stopped) {
    try {
      co_await metadata::refresh(context);
      co_await AlertEvaluationService::drainOutbox(context);
      ready->set_value();
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

  ruvia::web_worker_handle worker_;
  std::shared_future<void> stopped_;
  std::size_t workerIndex_ = 0;
  std::size_t serviceWorkerCount_ = 0;
  std::atomic_bool running_{false};
};

} // namespace service::alert
