#pragma once

#include <bit>
#include <iomanip>
#include "service/features/collector/collector.protocol.h"

namespace service::collector::register_value {

inline std::size_t width(std::string_view type) {
    if (type == "BOOL") return 1;
    if (type == "INT16" || type == "UINT16" || type == "WORD") return 2;
    if (type == "INT32" || type == "UINT32" || type == "DWORD" || type == "FLOAT" || type == "FLOAT32") return 4;
    if (type == "INT64" || type == "UINT64" || type == "DOUBLE") return 8;
    throw std::invalid_argument("unsupported register data type");
}

inline std::uint16_t readBe16(std::span<const std::uint8_t> bytes, std::size_t offset) {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[offset]) << 8U) |
                                      bytes[offset + 1]);
}

inline std::vector<std::uint8_t> orderedBytes(std::span<const std::uint8_t> bytes,
                                              std::string_view order) {
    std::vector<std::uint8_t> result(bytes.begin(), bytes.end());
    if (order == "LITTLE_ENDIAN")
        std::reverse(result.begin(), result.end());
    else if (order == "BIG_ENDIAN_BYTE_SWAP")
        for (std::size_t index = 0; index + 1 < result.size(); index += 2)
            std::swap(result[index], result[index + 1]);
    else if (order == "LITTLE_ENDIAN_BYTE_SWAP")
        for (std::size_t left = 0, right = result.size() / 2; left < right / 2; ++left) {
            const auto other = right - 1 - left;
            std::swap(result[left * 2], result[other * 2]);
            std::swap(result[left * 2 + 1], result[other * 2 + 1]);
        }
    return result;
}

inline std::uint32_t readBe32(std::span<const std::uint8_t> bytes) {
    return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
           (static_cast<std::uint32_t>(bytes[1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[2]) << 8U) | bytes[3];
}

inline std::uint64_t readBe64(std::span<const std::uint8_t> bytes) {
    std::uint64_t result = 0;
    for (std::size_t index = 0; index < 8; ++index)
        result = (result << 8U) | bytes[index];
    return result;
}

inline std::string decimalJson(double value, const ElementDefinition& element) {
    if (element.decimals >= 0) {
        const auto factor = std::pow(10.0, static_cast<double>(element.decimals));
        value = std::round(value * factor) / factor;
    }
    std::ostringstream output;
    if (element.decimals >= 0)
        output << std::fixed << std::setprecision(element.decimals) << value;
    else
        output << std::setprecision(17) << value;
    return output.str();
}

inline std::optional<std::string> numericJson(std::span<const std::uint8_t> source,
                                              const ElementDefinition& element) {
    const auto bytes = orderedBytes(source, element.byteOrder);
    const auto rounded = element.decimals >= 0;
    if ((element.dataType == "UINT16" || element.dataType == "WORD") && bytes.size() >= 2) {
        const auto value = readBe16(bytes, 0);
        return rounded ? decimalJson(static_cast<double>(value), element) : std::to_string(value);
    }
    if (element.dataType == "INT16" && bytes.size() >= 2) {
        const auto value = static_cast<std::int16_t>(readBe16(bytes, 0));
        return rounded ? decimalJson(static_cast<double>(value), element) : std::to_string(value);
    }
    if ((element.dataType == "UINT32" || element.dataType == "DWORD") && bytes.size() >= 4) {
        const auto value = readBe32(bytes);
        return rounded ? decimalJson(static_cast<double>(value), element) : std::to_string(value);
    }
    if (element.dataType == "INT32" && bytes.size() >= 4) {
        const auto value = static_cast<std::int32_t>(readBe32(bytes));
        return rounded ? decimalJson(static_cast<double>(value), element) : std::to_string(value);
    }
    if (element.dataType == "UINT64" && bytes.size() >= 8) {
        const auto value = readBe64(bytes);
        return rounded ? decimalJson(static_cast<double>(value), element) : std::to_string(value);
    }
    if (element.dataType == "INT64" && bytes.size() >= 8) {
        const auto value = static_cast<std::int64_t>(readBe64(bytes));
        return rounded ? decimalJson(static_cast<double>(value), element) : std::to_string(value);
    }
    if ((element.dataType == "FLOAT" || element.dataType == "FLOAT32") && bytes.size() >= 4)
        return decimalJson(std::bit_cast<float>(readBe32(bytes)), element);
    if (element.dataType == "DOUBLE" && bytes.size() >= 8)
        return decimalJson(std::bit_cast<double>(readBe64(bytes)), element);
    if (element.dataType == "BOOL" && !bytes.empty())
        return bytes[0] != 0 ? "1" : "0";
    return std::nullopt;
}

inline void appendBe(std::vector<std::uint8_t>& bytes, std::uint64_t value, std::size_t width) {
    for (auto offset = width; offset > 0; --offset)
        bytes.push_back(static_cast<std::uint8_t>(value >> ((offset - 1) * 8U)));
}

inline std::vector<std::uint8_t> encodeValue(const ElementDefinition& element,
                                             std::string_view value) {
    std::vector<std::uint8_t> bytes;
    if (element.dataType == "BOOL") {
        bytes.push_back(value == "1" ? 1 : 0);
        return bytes;
    }
    if (element.dataType == "INT16")
        appendBe(
            bytes,
            static_cast<std::uint16_t>(command::integer<std::int64_t>(value, element.name)),
            2);
    else if (element.dataType == "UINT16" || element.dataType == "WORD")
        appendBe(bytes, command::integer<std::uint64_t>(value, element.name), 2);
    else if (element.dataType == "INT32")
        appendBe(
            bytes,
            static_cast<std::uint32_t>(command::integer<std::int64_t>(value, element.name)),
            4);
    else if (element.dataType == "UINT32" || element.dataType == "DWORD")
        appendBe(bytes, command::integer<std::uint64_t>(value, element.name), 4);
    else if (element.dataType == "INT64")
        appendBe(
            bytes,
            static_cast<std::uint64_t>(command::integer<std::int64_t>(value, element.name)),
            8);
    else if (element.dataType == "UINT64")
        appendBe(bytes, command::integer<std::uint64_t>(value, element.name), 8);
    else if (element.dataType == "FLOAT" || element.dataType == "FLOAT32")
        appendBe(bytes,
                 std::bit_cast<std::uint32_t>(
                     static_cast<float>(command::decimal(value, element.name))),
                 4);
    else if (element.dataType == "DOUBLE")
        appendBe(bytes, std::bit_cast<std::uint64_t>(command::decimal(value, element.name)),
                 8);
    if (bytes.empty())
        throw std::invalid_argument("command_invalid: unsupported register data type");
    return orderedBytes(bytes, element.byteOrder);
}

} // namespace service::collector::register_value
