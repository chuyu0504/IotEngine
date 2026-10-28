#pragma once

#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <utility>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/web/web_worker.h>

namespace service::utils {

// Borrowed startup signal; its storage belongs to runWorkerTask's coroutine.
class WorkerReady final {
  public:
    explicit WorkerReady(std::promise<void>& promise) : promise_(promise) {}

    void set_value() {
        if (!signalled_) {
            promise_.set_value();
            signalled_ = true;
        }
    }

    [[nodiscard]] bool signalled() const noexcept { return signalled_; }

  private:
    std::promise<void>& promise_;
    bool signalled_ = false;
};

template <typename Run>
ruvia::task<void> runWorkerTask(ruvia::web_worker_context& context, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> done, ruvia::stop_token stop, Run run) {
    WorkerReady signal(*ready);
    const auto combined = ruvia::combine_stop_tokens(context.stop_token(), stop);
    try {
        co_await run(context, signal, combined);
        if (!signal.signalled()) {
            throw std::runtime_error("Worker task ended before becoming ready");
        }
    } catch (...) {
        if (!signal.signalled()) {
            ready->set_exception(std::current_exception());
        } else if (!combined.stop_requested()) {
            try {
                throw;
            } catch (const std::exception& error) {
                std::cerr << "Worker task failed: " << error.what() << '\n';
            } catch (...) {
                std::cerr << "Worker task failed: unknown exception\n";
            }
        }
    }
    done->set_value();
}

// All persistent cancellation/completion state is owned explicitly by runtime.
// Startup and shutdown are called off the target Worker's event loop.
template <typename Run>
void startWorkerTask(ruvia::web_worker_handle worker, ruvia::stop_token stop, std::future<void>& completion, Run run) {
    if (completion.valid()) {
        throw std::logic_error("Worker task is already started");
    }
    auto ready = std::make_shared<std::promise<void>>();
    auto done = std::make_shared<std::promise<void>>();
    auto startup = ready->get_future();
    completion = done->get_future();
    if (!worker.post([ready, done, stop, run = std::move(run)](ruvia::web_worker_context& context) mutable {
                   return runWorkerTask(context, ready, done, stop, std::move(run));
               })
             .accepted()) {
        completion = {};
        throw std::runtime_error("Worker rejected task startup");
    }
    try {
        startup.get();
    } catch (...) {
        completion.wait();
        completion = {};
        throw;
    }
}

inline void stopWorkerTask(ruvia::stop_source& stop, std::future<void>& completion) noexcept {
    stop.request_stop();
    if (completion.valid()) {
        completion.wait();
    }
    completion = {};
}

} // namespace service::utils
