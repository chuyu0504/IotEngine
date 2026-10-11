#pragma once

#include <ruvia/web/model_types.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <random>
#include <string>
#include <string_view>

namespace service::common {

class UuidV7Generator {
  public:
    UuidV7Generator() : random_(std::random_device{}()) {}

    [[nodiscard]] std::string next() {
        std::lock_guard lock(mutex_);
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        auto timestamp = static_cast<std::uint64_t>(now < 0 ? 0 : now);
        if (timestamp > lastTimestampMs_) {
            lastTimestampMs_ = timestamp;
            randomSequence();
        } else {
            timestamp = lastTimestampMs_;
            if (!incrementSequence()) {
                ++lastTimestampMs_;
                timestamp = lastTimestampMs_;
                randomSequence();
            }
        }

        std::array<std::uint8_t, 16> bytes{};
        for (int index = 5; index >= 0; --index) {
            bytes[static_cast<std::size_t>(index)] = static_cast<std::uint8_t>(timestamp & 0xFFU);
            timestamp >>= 8U;
        }
        bytes[6] = static_cast<std::uint8_t>(0x70U | (sequence_[0] & 0x0FU));
        bytes[7] = sequence_[1];
        bytes[8] = static_cast<std::uint8_t>(0x80U | (sequence_[2] & 0x3FU));
        for (std::size_t index = 3; index < sequence_.size(); ++index)
            bytes[index + 6] = sequence_[index];
        return format(bytes);
    }

  private:
    void randomSequence() {
        for (auto& byte : sequence_)
            byte = static_cast<std::uint8_t>(random_() & 0xFFU);
        sequence_[0] &= 0x0FU;
        sequence_[2] &= 0x3FU;
    }

    bool incrementSequence() noexcept {
        for (std::size_t reverse = 0; reverse < sequence_.size(); ++reverse) {
            const auto index = sequence_.size() - 1 - reverse;
            const auto limit = index == 0 ? 0x0FU : index == 2 ? 0x3FU : 0xFFU;
            if (sequence_[index] < limit) {
                ++sequence_[index];
                return true;
            }
            sequence_[index] = 0;
        }
        return false;
    }

    static std::string format(const std::array<std::uint8_t, 16>& bytes) {
        static constexpr std::array<char, 16> digits{'0', '1', '2', '3', '4', '5', '6', '7',
                                                     '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        std::string value;
        value.reserve(36);
        for (std::size_t index = 0; index < bytes.size(); ++index) {
            if (index == 4 || index == 6 || index == 8 || index == 10)
                value.push_back('-');
            value.push_back(digits[bytes[index] >> 4U]);
            value.push_back(digits[bytes[index] & 0x0FU]);
        }
        return value;
    }

    std::mutex mutex_;
    std::mt19937_64 random_;
    std::uint64_t lastTimestampMs_ = 0;
    std::array<std::uint8_t, 10> sequence_{};
};

inline bool isUuid(std::string_view value) noexcept {
    if (value.size() != 36)
        return false;
    for (std::size_t index = 0; index < value.size(); ++index) {
        if (index == 8 || index == 13 || index == 18 || index == 23) {
            if (value[index] != '-')
                return false;
            continue;
        }
        const auto character = value[index];
        if (!((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f') ||
              (character >= 'A' && character <= 'F')))
            return false;
    }
    return true;
}

inline int hexDigit(char value) {
    if (value >= '0' && value <= '9')
        return value - '0';
    if (value >= 'a' && value <= 'f')
        return value - 'a' + 10;
    if (value >= 'A' && value <= 'F')
        return value - 'A' + 10;
    return -1;
}

inline bool uuidBytes(std::string_view value, std::uint8_t output[16]) {
    if (value.size() != 36 || value[8] != '-' || value[13] != '-' ||
        value[18] != '-' || value[23] != '-')
        return false;
    std::size_t byte = 0;
    for (std::size_t index = 0; index < value.size();) {
        if (value[index] == '-') {
            ++index;
            continue;
        }
        if (index + 1 >= value.size() || byte >= 16)
            return false;
        const int high = hexDigit(value[index]);
        const int low = hexDigit(value[index + 1]);
        if (high < 0 || low < 0)
            return false;
        output[byte++] = static_cast<std::uint8_t>((high << 4U) | low);
        index += 2;
    }
    return byte == 16;
}

inline std::string uuidText(std::string_view value) {
    if (value.size() != 16)
        return {};
    constexpr char digits[] = "0123456789abcdef";
    std::string output;
    output.reserve(36);
    for (std::size_t index = 0; index < 16; ++index) {
        if (index == 4 || index == 6 || index == 8 || index == 10)
            output.push_back('-');
        const auto byte = static_cast<std::uint8_t>(value[index]);
        output.push_back(digits[byte >> 4U]);
        output.push_back(digits[byte & 0x0fU]);
    }
    return output;
}

inline std::string uuidText(const std::uint8_t value[16]) {
    return uuidText(std::string_view(reinterpret_cast<const char*>(value), 16));
}

inline bool isUuidField(const ruvia::string& value) noexcept { return isUuid(value.view()); }

inline bool isOptionalUuidField(const ruvia::string& value) noexcept {
    return value.empty() || isUuid(value.view());
}

} // namespace service::common

namespace service::runtime {

// A process incarnation is never reused after a restart.
inline const std::string& instanceId() {
    static const std::string id = [] {
        service::common::UuidV7Generator generator;
        return generator.next();
    }();
    return id;
}

} // namespace service::runtime
