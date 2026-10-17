#pragma once

#include <algorithm>
#include <asio.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <queue>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "service/features/collector/tcp/tcp.types.h"

namespace service::collector {

// Worker-affine deadline scheduler. Every Collector Worker owns exactly one instance and therefore
// exactly one underlying asio timer. Protocol sessions only register/cancel deadlines here; they
// must never create their own timers.
class DeadlineScheduler final {
  public:
    using Clock = std::chrono::steady_clock;
    using Deadline = Clock::time_point;
    using Token = std::uint64_t;
    using Callback = std::function<void()>;

    explicit DeadlineScheduler(asio::io_context& ioContext) : timer_(ioContext) {}

    DeadlineScheduler(const DeadlineScheduler&) = delete;
    DeadlineScheduler& operator=(const DeadlineScheduler&) = delete;

    // 仅在装配时创建；返回值借用本调度器，transport 必须先于 stop() 停止。
    [[nodiscard]] TcpDeadlineCallbacks borrowTcpDeadlines() {
        return {
            .scheduleAfter = [this](Clock::duration delay, Callback callback) {
                return scheduleAfter(delay, std::move(callback));
            },
            .cancel = [this](Token token) {
                cancel(token);
            }
        };
    }

    [[nodiscard]] Token schedule(Deadline deadline, Callback callback) {
        const auto token = nextToken_++;
        tasks_.push(Task{ deadline, token, std::move(callback) });
        arm();
        return token;
    }

    template <typename Rep, typename Period>
    [[nodiscard]] Token scheduleAfter(std::chrono::duration<Rep, Period> delay, Callback callback) {
        return schedule(Clock::now() + std::chrono::duration_cast<Clock::duration>(delay), std::move(callback));
    }

    void cancel(Token token) {
        if (token != 0) {
            cancelled_.insert(token);
        }
    }

    void stop() noexcept {
        if (stopped_) {
            return;
        }
        stopped_ = true;
        ++armGeneration_;
        try {
            timer_.cancel();
        } catch (const std::system_error&) {
            // Shutdown is best effort; retain the non-throwing stop contract.
        }
        while (!tasks_.empty()) {
            tasks_.pop();
        }
        cancelled_.clear();
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return tasks_.size() >= cancelled_.size() ? tasks_.size() - cancelled_.size() : 0;
    }

  private:
    struct Task {
        Deadline deadline;
        Token token = 0;
        Callback callback;
    };

    struct LaterDeadline {
        bool operator()(const Task& left, const Task& right) const noexcept {
            if (left.deadline != right.deadline) {
                return left.deadline > right.deadline;
            }
            return left.token > right.token;
        }
    };

    void discardCancelledFront() {
        while (!tasks_.empty() && cancelled_.erase(tasks_.top().token) != 0) {
            tasks_.pop();
        }
    }

    void arm() {
        if (stopped_) {
            return;
        }
        discardCancelledFront();
        if (tasks_.empty()) {
            if (armed_) {
                ++armGeneration_;
                timer_.cancel();
                armed_ = false;
            }
            return;
        }
        const auto deadline = tasks_.top().deadline;
        if (armed_ && deadline >= armedDeadline_) {
            return;
        }
        const auto generation = ++armGeneration_;
        armed_ = true;
        armedDeadline_ = deadline;
        timer_.expires_at(deadline);
        timer_.async_wait([this, generation](const std::error_code& error) {
            if (error == asio::error::operation_aborted || stopped_ || generation != armGeneration_) {
                return;
            }
            armed_ = false;
            runExpired();
        });
    }

    void runExpired() {
        auto now = Clock::now();
        std::size_t processed = 0;
        while (processed < kCallbackBudget) {
            discardCancelledFront();
            if (tasks_.empty() || tasks_.top().deadline > now) {
                break;
            }
            auto task = std::move(const_cast<Task&>(tasks_.top()));
            tasks_.pop();
            if (task.callback) {
                task.callback();
            }
            ++processed;
            now = Clock::now();
        }
        discardCancelledFront();
        if (!tasks_.empty() && tasks_.top().deadline <= Clock::now()) {
            // Explicitly yield after a bounded batch so deadline storms cannot starve socket I/O.
            asio::post(timer_.get_executor(), [this] {
                if (!stopped_) {
                    runExpired();
                }
            });
            return;
        }
        arm();
    }

    static constexpr std::size_t kCallbackBudget = 256;

    asio::steady_timer timer_;
    std::priority_queue<Task, std::vector<Task>, LaterDeadline> tasks_;
    std::unordered_set<Token> cancelled_;
    Token nextToken_ = 1;
    std::uint64_t armGeneration_ = 0;
    Deadline armedDeadline_{};
    bool armed_ = false;
    bool stopped_ = false;
};

} // namespace service::collector
