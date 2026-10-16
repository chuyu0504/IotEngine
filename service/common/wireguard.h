#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace service::common::wireguard {

// WireGuard Curve25519 keys travel as canonical base64 of 32 bytes: 43 symbols
// whose last one carries no padding bits, followed by one '='. This is the only
// key text codec shared by the VPN module, the VPN feature and the Edge projection.
inline constexpr std::size_t kKeySize = 32;
inline constexpr std::size_t kKeyTextSize = 44;

using Key = std::array<std::uint8_t, kKeySize>;

inline constexpr std::string_view kBase64Alphabet{
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
};

inline constexpr int base64Value(char value) noexcept {
    if (value >= 'A' && value <= 'Z') {
        return value - 'A';
    }
    if (value >= 'a' && value <= 'z') {
        return value - 'a' + 26;
    }
    if (value >= '0' && value <= '9') {
        return value - '0' + 52;
    }
    if (value == '+') {
        return 62;
    }
    if (value == '/') {
        return 63;
    }
    return -1;
}

inline std::optional<Key> decodeKey(std::string_view text) noexcept {
    if (text.size() != kKeyTextSize || text.back() != '=') {
        return std::nullopt;
    }
    Key result{};
    std::size_t output = 0;
    std::uint32_t accumulator = 0;
    unsigned bits = 0;
    for (std::size_t index = 0; index + 1 < kKeyTextSize; ++index) {
        const auto value = base64Value(text[index]);
        if (value < 0) {
            return std::nullopt;
        }
        accumulator = (accumulator << 6U) | static_cast<std::uint32_t>(value);
        bits += 6U;
        if (bits >= 8U) {
            bits -= 8U;
            result[output++] = static_cast<std::uint8_t>((accumulator >> bits) & 0xffU);
        }
    }
    // 43 symbols carry 258 bits: 256 key bits and two padding bits that must be zero.
    if (output != kKeySize || bits != 2U || (accumulator & 0x3U) != 0) {
        return std::nullopt;
    }
    return result;
}

inline std::string encodeKey(const Key& key) {
    std::string result;
    result.reserve(kKeyTextSize);
    for (std::size_t index = 0; index < kKeySize; index += 3U) {
        const std::uint32_t first = key[index];
        const std::uint32_t second = index + 1U < kKeySize ? key[index + 1U] : 0U;
        const std::uint32_t third = index + 2U < kKeySize ? key[index + 2U] : 0U;
        result.push_back(kBase64Alphabet[first >> 2U]);
        result.push_back(kBase64Alphabet[((first & 0x03U) << 4U) | (second >> 4U)]);
        result.push_back(index + 1U < kKeySize ? kBase64Alphabet[((second & 0x0fU) << 2U) | (third >> 6U)] : '=');
        result.push_back(index + 2U < kKeySize ? kBase64Alphabet[third & 0x3fU] : '=');
    }
    return result;
}

// Text that the kernel interface can carry. Use it where an existing key must
// still be handled, such as removing a peer already present on the interface.
inline bool validKey(std::string_view text) noexcept {
    return decodeKey(text).has_value();
}

// A key that may be granted Hub access or used as the Hub identity. The all-zero
// key is a low-order Curve25519 point: as a peer key no handshake can ever
// complete, and as a private key it is publicly known.
inline bool validManagedKey(std::string_view text) noexcept {
    const auto key = decodeKey(text);
    return key && std::any_of(key->begin(), key->end(), [](auto byte) {
               return byte != 0;
           });
}

} // namespace service::common::wireguard
