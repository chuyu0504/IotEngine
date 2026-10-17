#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace service::collector {

// 同 Worker 的截止回调；借用调度器生命周期，不拥有定时器或调度运行对象。
// 所有调用与到期回调均在所属 Worker 执行；须先停止 transport，再停止或销毁调度器。
struct TcpDeadlineCallbacks {
    using Token = std::uint64_t;
    using Callback = std::function<void()>;

    std::function<Token(std::chrono::steady_clock::duration, Callback)> scheduleAfter;
    std::function<void(Token)> cancel;
};

struct LinkState {
    struct Target {
        std::string id;
        std::string state;
        std::string reason;
        std::string error;
        std::int64_t lastActivityAtMs = 0;
    };

    std::string linkId;
    std::size_t workerIndex = 0;
    std::string state;
    std::string reason;
    std::string error;
    std::vector<std::string> remoteEndpoints;
    std::vector<Target> targets;
    std::int64_t lastActivityAtMs = 0;
};

} // namespace service::collector
