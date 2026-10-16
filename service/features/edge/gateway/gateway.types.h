#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>

namespace service::edge::gateway {

// Maximum firmware bytes carried by one FirmwareChunk over the node WebSocket.
inline constexpr std::size_t kFirmwareChunkSize{ 8192 };
// Records and owned files become collectable one hour after their last use.
inline constexpr std::chrono::hours kFirmwareRetention{ 1 };
// Upper bound of records and orphan files removed by one claimed cleanup.
inline constexpr std::size_t kFirmwareCleanupBatch{ 32 };

struct FirmwareSource final {
    std::string storagePath;
    std::uint64_t sizeBytes{};
};

struct FirmwareChunk final {
    std::string data;
    std::string error;
    bool eof{};
};

struct FirmwareCleanupResult final {
    // False when another Worker or process holds the firmware storage lock.
    bool claimed{};
    std::size_t removedRecords{};
    std::size_t removedFiles{};
};

// Worker state: wakes this Worker's firmware cleanup driver to recompute its
// deadline. Producers on the same Worker (edge projection after it committed a
// terminal firmware task, the schedule-cleanup RPC handler) only record the
// request in memory, so it cannot fail after their commit; a request made while
// no driver is bound is kept until the driver binds and consumes it.
class FirmwareCleanupSignal final {
  public:
    void request() {
        requested_ = true;
        if (wake_) {
            wake_();
        }
    }

    // Returns whether a request arrived since the last call, and clears it.
    [[nodiscard]] bool consume() { return std::exchange(requested_, false); }

    [[nodiscard]] bool pending() const { return requested_; }

    // The driver binds its wakeup while it runs and unbinds before it stops.
    void bind(std::function<void()> wake) { wake_ = std::move(wake); }

  private:
    bool requested_{};
    std::function<void()> wake_;
};

} // namespace service::edge::gateway
