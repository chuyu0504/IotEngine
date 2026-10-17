#pragma once

#include <chrono>
#include <openssl/rand.h>
#include <stdexcept>

#include "service/common/uuid.h"

namespace service::utils {

// The caller owns this state on a single Worker; borrowers use the same sequence.
[[nodiscard]] inline std::string nextUuid(service::common::UuidV7State& state) {
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch()
    )
                         .count();
    auto timestamp = static_cast<std::uint64_t>(now < 0 ? 0 : now);
    bool freshTimestamp = timestamp > state.timestamp;
    std::size_t carry = state.sequence.size();
    if (!freshTimestamp) {
        timestamp = state.timestamp;
        while (carry > 0) {
            const auto index = carry - 1;
            const auto limit = index == 0 ? 0x0FU : index == 2 ? 0x3FU
                                                               : 0xFFU;
            if (state.sequence[index] < limit) {
                break;
            }
            --carry;
        }
        if (carry == 0) {
            ++timestamp;
            freshTimestamp = true;
        }
    }
    if (freshTimestamp) {
        std::array<std::uint8_t, 10> sequence;
        if (RAND_bytes(sequence.data(), static_cast<int>(sequence.size())) != 1) {
            throw std::runtime_error("UUIDv7 entropy generation failed");
        }
        sequence[0] &= 0x0FU;
        sequence[2] &= 0x3FU;
        state.sequence = sequence;
        state.timestamp = timestamp;
    } else {
        ++state.sequence[carry - 1];
        for (std::size_t index = carry; index < state.sequence.size(); ++index) {
            state.sequence[index] = 0;
        }
    }

    std::array<std::uint8_t, 16> bytes{};
    for (int index = 5; index >= 0; --index) {
        bytes[static_cast<std::size_t>(index)] = static_cast<std::uint8_t>(timestamp & 0xFFU);
        timestamp >>= 8U;
    }
    bytes[6] = static_cast<std::uint8_t>(0x70U | state.sequence[0]);
    bytes[7] = state.sequence[1];
    bytes[8] = static_cast<std::uint8_t>(0x80U | state.sequence[2]);
    for (std::size_t index = 3; index < state.sequence.size(); ++index) {
        bytes[index + 6] = state.sequence[index];
    }
    return service::common::uuidText(bytes.data());
}

} // namespace service::utils
