#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

#include <edge.pb.h>

namespace service::edge::gateway {

struct Enrollment final {
    std::string nodeId;
    bool approved{};
};

struct ConnectionIdentity final {
    std::string_view nodeId;
    std::uint64_t epoch{};
    std::uint32_t protocolVersion{};
    std::size_t workerIndex{};
};

enum class IngressStatus { Accepted, SessionReplaced, WorkerLeaseLost };

struct EgressQueues final {
    std::string configuration;
    std::string terminal;
    std::string serialDebug;
    std::string command;
    std::string task;
};

struct QueuedEnvelope final {
    explicit QueuedEnvelope(std::pmr::string bytes) : wire(std::move(bytes)) {}
    std::pmr::string wire;
    ::iot::edge::v1::Envelope envelope;
    bool deliver{};
};

struct FirmwareTransfer final {
    std::array<std::uint8_t, 16> requestId{};
    std::filesystem::path path;
    std::uint64_t size{};
    bool loaded{};
};

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
