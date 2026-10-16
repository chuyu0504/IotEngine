#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

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

} // namespace service::edge::gateway
