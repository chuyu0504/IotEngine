#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

#include <ruvia/web/model_types.h>

namespace service::common {

struct UuidV7State {
    std::uint64_t timestamp = 0;
    std::array<std::uint8_t, 10> sequence{};
};

inline bool isUuid(std::string_view value) noexcept {
    if (value.size() != 36) {
        return false;
    }
    for (std::size_t index = 0; index < value.size(); ++index) {
        if (index == 8 || index == 13 || index == 18 || index == 23) {
            if (value[index] != '-') {
                return false;
            }
            continue;
        }
        const auto character = value[index];
        if (!((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f') ||
              (character >= 'A' && character <= 'F'))) {
            return false;
        }
    }
    return true;
}

inline int hexDigit(char value) {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    if (value >= 'A' && value <= 'F') {
        return value - 'A' + 10;
    }
    return -1;
}

inline bool uuidBytes(std::string_view value, std::uint8_t output[16]) {
    if (value.size() != 36 || value[8] != '-' || value[13] != '-' ||
        value[18] != '-' || value[23] != '-') {
        return false;
    }
    std::size_t byte = 0;
    for (std::size_t index = 0; index < value.size();) {
        if (value[index] == '-') {
            ++index;
            continue;
        }
        if (index + 1 >= value.size() || byte >= 16) {
            return false;
        }
        const int high = hexDigit(value[index]);
        const int low = hexDigit(value[index + 1]);
        if (high < 0 || low < 0) {
            return false;
        }
        output[byte++] = static_cast<std::uint8_t>((high << 4U) | low);
        index += 2;
    }
    return byte == 16;
}

inline std::string uuidText(std::string_view value) {
    if (value.size() != 16) {
        return {};
    }
    constexpr char digits[] = "0123456789abcdef";
    std::string output;
    output.reserve(36);
    for (std::size_t index = 0; index < 16; ++index) {
        if (index == 4 || index == 6 || index == 8 || index == 10) {
            output.push_back('-');
        }
        const auto byte = static_cast<std::uint8_t>(value[index]);
        output.push_back(digits[byte >> 4U]);
        output.push_back(digits[byte & 0x0fU]);
    }
    return output;
}

inline std::string uuidText(const std::uint8_t value[16]) {
    return uuidText(std::string_view(reinterpret_cast<const char*>(value), 16));
}

inline bool isUuidField(const ruvia::string& value) noexcept {
    return isUuid(value.view());
}

inline bool isOptionalUuidField(const ruvia::string& value) noexcept {
    return value.empty() || isUuid(value.view());
}

} // namespace service::common
