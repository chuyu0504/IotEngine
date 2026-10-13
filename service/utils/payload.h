#pragma once

#include <bit>
#include <charconv>
#include <cmath>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "service/common/timestamp.h"
#include "service/utils/json.h"

namespace service::utils {

using PayloadScalar = std::variant<std::string, double, bool>;

inline std::vector<std::string_view> splitPayload(std::string_view input, std::string_view delimiter) {
    if (delimiter.empty()) {
        throw std::invalid_argument("分隔符不能为空");
    }
    std::vector<std::string_view> result;
    for (;;) {
        const auto position = input.find(delimiter);
        result.push_back(input.substr(0, position));
        if (position == std::string_view::npos) {
            return result;
        }
        input.remove_prefix(position + delimiter.size());
    }
}

inline std::size_t payloadIndex(std::string_view input) {
    std::size_t index = 0;
    const auto parsed = std::from_chars(input.data(), input.data() + input.size(), index);
    if (input.empty() || (input.size() > 1 && input.front() == '0') || parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size() || index > 1048576) {
        throw std::invalid_argument("字段索引或偏移无效");
    }
    return index;
}

struct BinaryPayloadField {
    std::size_t offset, length;
    std::string_view encoding;
    bool littleEndian;

    explicit BinaryPayloadField(std::string_view selector) {
        const auto parts = splitPayload(selector, ":");
        if (parts.size() != 3 && parts.size() != 4) {
            throw std::invalid_argument("二进制字段格式为 偏移:长度:编码:字节序");
        }
        offset = payloadIndex(parts[0]);
        length = payloadIndex(parts[1]);
        encoding = parts[2];
        littleEndian = parts.size() == 4 && parts[3] == "LE";
        if (!length || offset + length > 1048576 ||
            (parts.size() == 4 && parts[3] != "BE" && parts[3] != "LE") ||
            (encoding != "UINT" && encoding != "INT" && encoding != "FLOAT" && encoding != "UTF8" && encoding != "HEX") ||
            ((encoding == "UINT" || encoding == "INT") && length != 1 && length != 2 && length != 4) ||
            (encoding == "FLOAT" && length != 4 && length != 8)) {
            throw std::invalid_argument("二进制字段长度、编码或字节序无效");
        }
    }
};

inline std::string payloadScalarText(const PayloadScalar& value) {
    if (const auto* text = std::get_if<std::string>(&value)) {
        return *text;
    }
    if (const auto* flag = std::get_if<bool>(&value)) {
        return *flag ? "true" : "false";
    }
    char buffer[64];
    const auto parsed = std::to_chars(buffer, buffer + sizeof(buffer), std::get<double>(value));
    if (parsed.ec != std::errc{}) {
        throw std::invalid_argument("数值无法表示");
    }
    return { buffer, parsed.ptr };
}

inline std::string payloadScalarJson(const PayloadScalar& value) {
    return std::holds_alternative<std::string>(value) ? jsonQuoted(std::get<std::string>(value)) : payloadScalarText(value);
}

inline std::optional<double> payloadNumber(const PayloadScalar& value) {
    if (const auto* number = std::get_if<double>(&value)) {
        return std::isfinite(*number) ? std::optional(*number) : std::nullopt;
    }
    const auto* text = std::get_if<std::string>(&value);
    if (!text) {
        return std::nullopt;
    }
    double result = 0;
    const auto parsed = std::from_chars(text->data(), text->data() + text->size(), result);
    return parsed.ec == std::errc{} && parsed.ptr == text->data() + text->size() && std::isfinite(result) ? std::optional(result) : std::nullopt;
}

inline std::optional<PayloadScalar> convertPayloadValue(const PayloadScalar& input, std::string_view type) {
    if (type == "STRING") {
        return PayloadScalar{ payloadScalarText(input) };
    }
    if (type == "BOOL") {
        const auto text = payloadScalarText(input);
        if (text == "true" || text == "1") {
            return PayloadScalar{ true };
        }
        if (text == "false" || text == "0") {
            return PayloadScalar{ false };
        }
        return std::nullopt;
    }
    const auto number = payloadNumber(input);
    if (!number) {
        return std::nullopt;
    }
    return PayloadScalar{ *number };
}

inline std::optional<std::int64_t> payloadTimestamp(const PayloadScalar& value, std::string_view format, std::string_view timezone) {
    if (format != "iso8601") {
        const auto number = payloadNumber(value);
        const auto ms = number ? *number * (format == "unix_s" ? 1000 : 1) : -1;
        if (!std::isfinite(ms) || ms < 0 || ms > 253402300799999.0) {
            return std::nullopt;
        }
        return static_cast<std::int64_t>(ms);
    }
    int zoneMinutes = 0;
    if (timezone.size() == 6 && (timezone[0] == '+' || timezone[0] == '-') && timezone[3] == ':') {
        int hours = 0, minutes = 0;
        const auto h = std::from_chars(timezone.data() + 1, timezone.data() + 3, hours);
        const auto m = std::from_chars(timezone.data() + 4, timezone.data() + 6, minutes);
        if (h.ec != std::errc{} || m.ec != std::errc{} || h.ptr != timezone.data() + 3 || m.ptr != timezone.data() + 6 || hours > 14 || minutes > 59) {
            return std::nullopt;
        }
        zoneMinutes = (hours * 60 + minutes) * (timezone[0] == '-' ? -1 : 1);
    } else {
        return std::nullopt;
    }
    const auto text = payloadScalarText(value);
    const auto time = service::common::parseUtcTimestamp(text, zoneMinutes);
    if (!time) {
        return std::nullopt;
    }
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(time->time_since_epoch()).count();
    if (text.size() > 20 && text[19] == '.') {
        for (std::size_t i = 20, multiplier = 100; i < text.size() && multiplier && text[i] >= '0' && text[i] <= '9'; ++i, multiplier /= 10) {
            ms += (text[i] - '0') * multiplier;
        }
    }
    return ms >= 0 && ms <= 253402300799999LL ? std::optional<std::int64_t>(ms) : std::nullopt;
}

inline std::vector<std::string> payloadRecords(std::string_view input, std::string_view format, std::string_view recordsPath, std::string_view recordDelimiter, std::size_t recordLength) {
    std::vector<std::string> result;
    if (format == "json") {
        const auto parsed = ruvia::json_value::parse(input);
        const auto records = parsed ? jsonField(*parsed, recordsPath) : std::nullopt;
        if (!records) {
            throw std::invalid_argument("JSON 无效或记录路径不存在");
        }
        if (records->is_array()) {
            (void)records->for_each_element([&](const auto& record) {
                if (result.size() >= 256) {
                    throw std::invalid_argument("单条消息最多 256 条设备记录");
                }
                result.emplace_back(record.view());
                return true;
            });
        } else {
            result.emplace_back(records->view());
        }
    } else if (format == "text") {
        for (const auto record : splitPayload(input, recordDelimiter)) {
            if (record.empty()) {
                continue;
            }
            if (result.size() >= 256) {
                throw std::invalid_argument("单条消息最多 256 条设备记录");
            }
            result.emplace_back(record);
        }
    } else if (format == "binary") {
        if (!recordLength) {
            recordLength = input.size();
        }
        if (!recordLength || input.size() % recordLength || input.size() / recordLength > 256) {
            throw std::invalid_argument("二进制记录长度不匹配");
        }
        for (std::size_t i = 0; i < input.size(); i += recordLength) {
            result.emplace_back(input.substr(i, recordLength));
        }
    } else {
        throw std::invalid_argument("负载格式无效");
    }
    return result;
}

class PayloadRecord {
  public:
    PayloadRecord(std::string_view input, std::string_view format, std::string_view delimiter)
        : input_(input), format_(format), json_(format == "json" ? ruvia::json_value::parse(input) : std::nullopt) {
        if (format == "text") {
            columns_ = splitPayload(input, delimiter);
        }
    }

    std::optional<PayloadScalar> field(std::string_view selector) const {
        if (format_ == "json") {
            const auto value = json_ ? jsonField(*json_, selector) : std::nullopt;
            if (!value) {
                return std::nullopt;
            }
            if (value->is_string()) {
                return PayloadScalar{ std::string(value->get<ruvia::string>()->view()) };
            }
            if (value->is_boolean()) {
                return PayloadScalar{ value->get<ruvia::bool_value>()->value_ };
            }
            if (value->is_number()) {
                return PayloadScalar{ value->get<ruvia::double_value>()->value_ };
            }
            return std::nullopt;
        }
        if (format_ == "text") {
            const auto index = payloadIndex(selector);
            return index < columns_.size() ? std::optional(PayloadScalar{ std::string(columns_[index]) }) : std::nullopt;
        }
        const BinaryPayloadField field(selector);
        if (field.offset + field.length > input_.size()) {
            return std::nullopt;
        }
        const auto bytes = input_.substr(field.offset, field.length);
        if (field.encoding == "UTF8") {
            return PayloadScalar{ std::string(bytes.substr(0, bytes.find('\0'))) };
        }
        if (field.encoding == "HEX") {
            std::string result;
            for (const unsigned char byte : bytes) {
                result += "0123456789ABCDEF"[byte >> 4];
                result += "0123456789ABCDEF"[byte & 15];
            }
            return PayloadScalar{ std::move(result) };
        }
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < bytes.size(); ++i) {
            value = (value << 8) | static_cast<unsigned char>(bytes[field.littleEndian ? bytes.size() - i - 1 : i]);
        }
        double number;
        if (field.encoding == "FLOAT") {
            number = field.length == 4 ? static_cast<double>(std::bit_cast<float>(static_cast<std::uint32_t>(value))) : std::bit_cast<double>(value);
        } else if (field.encoding == "INT" && (value & (std::uint64_t{ 1 } << (field.length * 8 - 1)))) {
            number = static_cast<double>(static_cast<std::int64_t>(value) - (std::int64_t{ 1 } << (field.length * 8)));
        } else {
            number = static_cast<double>(value);
        }
        return std::isfinite(number) ? std::optional(PayloadScalar{ number }) : std::nullopt;
    }

  private:
    std::string_view input_, format_;
    std::optional<ruvia::json_value> json_;
    std::vector<std::string_view> columns_;
};

inline void writeBinaryPayloadField(std::string& output, std::vector<bool>& occupied, std::string_view selector, const PayloadScalar& input) {
    const BinaryPayloadField field(selector);
    output.resize(std::max(output.size(), field.offset + field.length), '\0');
    occupied.resize(output.size());
    for (std::size_t i = field.offset; i < field.offset + field.length; ++i) {
        if (occupied[i]) {
            throw std::invalid_argument("指令二进制字段重叠");
        }
        occupied[i] = true;
    }
    const auto text = payloadScalarText(input);
    if (field.encoding == "UTF8") {
        if (text.size() > field.length) {
            throw std::invalid_argument("指令文本超出二进制字段长度");
        }
        output.replace(field.offset, text.size(), text);
        return;
    }
    if (field.encoding == "HEX") {
        if (text.size() != field.length * 2) {
            throw std::invalid_argument("指令 HEX 长度不匹配");
        }
        for (std::size_t i = 0; i < field.length; ++i) {
            unsigned value = 0;
            const auto parsed = std::from_chars(text.data() + i * 2, text.data() + i * 2 + 2, value, 16);
            if (parsed.ec != std::errc{} || parsed.ptr != text.data() + i * 2 + 2) {
                throw std::invalid_argument("指令 HEX 无效");
            }
            output[field.offset + i] = static_cast<char>(value);
        }
        return;
    }
    const auto number = std::holds_alternative<bool>(input) ? std::optional<double>(std::get<bool>(input) ? 1 : 0) : payloadNumber(input);
    if (!number) {
        throw std::invalid_argument("指令数值无效");
    }
    std::uint64_t value;
    if (field.encoding == "FLOAT") {
        if (field.length == 4 && !std::isfinite(static_cast<float>(*number))) {
            throw std::invalid_argument("指令 FLOAT 溢出");
        }
        value = field.length == 4 ? static_cast<std::uint64_t>(std::bit_cast<std::uint32_t>(static_cast<float>(*number))) : std::bit_cast<std::uint64_t>(*number);
    } else {
        const auto bits = field.length * 8;
        const auto minimum = field.encoding == "INT" ? -std::ldexp(1.0, static_cast<int>(bits - 1)) : 0;
        const auto maximum = std::ldexp(1.0, static_cast<int>(bits - (field.encoding == "INT" ? 1 : 0))) - 1;
        if (*number < minimum || *number > maximum || std::trunc(*number) != *number) {
            throw std::invalid_argument("指令整数超出范围或含小数");
        }
        value = static_cast<std::uint64_t>(static_cast<std::int64_t>(*number));
    }
    for (std::size_t i = 0; i < field.length; ++i) {
        output[field.offset + (field.littleEndian ? i : field.length - i - 1)] = static_cast<char>((value >> (i * 8)) & 255);
    }
}

} // namespace service::utils
