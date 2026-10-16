#pragma once

#include <array>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <sstream>
#include <openssl/evp.h>
#include <string>
#include <string_view>
#include <vector>

#include <edge.pb.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <nanopb.pb.h>

#include "service/utils/json.h"
#include "service/utils/crypto.h"
#include "service/utils/number.h"
#include "service/common/uuid.h"
#include "service/common/message.h"

#ifdef GetMessage
#undef GetMessage
#endif

namespace service::edge {
namespace pb = ::iot::edge::v1;
}

namespace service::edge::protocol {

class TelemetryValues final {
public:
    static std::string protocolName(pb::Protocol value) {
        if (value == pb::PROTOCOL_MODBUS)
            return "Modbus";
        if (value == pb::PROTOCOL_S7)
            return "S7";
        if (value == pb::PROTOCOL_SL651)
            return "SL651";
        if (value == pb::PROTOCOL_MC) return "MC";
        if (value == pb::PROTOCOL_FINS) return "FINS";
        if (value == pb::PROTOCOL_DLT645) return "DLT645";
        return {};
    }

    static std::string scalarJson(const pb::ScalarValue& value) {
        switch (value.value_case()) {
        case pb::ScalarValue::kBoolValue:
            return value.bool_value() ? "1" : "0";
        case pb::ScalarValue::kSignedValue:
            return std::to_string(value.signed_value());
        case pb::ScalarValue::kUnsignedValue:
            return std::to_string(value.unsigned_value());
        case pb::ScalarValue::kDoubleValue: {
            std::ostringstream output;
            output.precision(15);
            output << value.double_value();
            return output.str();
        }
        case pb::ScalarValue::kStringValue:
            return "\"" + service::utils::jsonEscape(value.string_value()) + "\"";
        case pb::ScalarValue::kDecimalValue: {
            const auto& text = value.decimal_value();
            std::size_t index = !text.empty() && text[0] == '-' ? 1 : 0;
            if (index == text.size() || text.size() > 32) throw std::invalid_argument("invalid decimal telemetry");
            const auto start = index;
            while (index < text.size() && text[index] >= '0' && text[index] <= '9') ++index;
            if (index == start || (index - start > 1 && text[start] == '0')) throw std::invalid_argument("invalid decimal telemetry");
            if (index < text.size() && text[index] == '.') {
                const auto fraction = ++index;
                while (index < text.size() && text[index] >= '0' && text[index] <= '9') ++index;
                if (fraction == index) throw std::invalid_argument("invalid decimal telemetry");
            }
            if (index != text.size()) throw std::invalid_argument("invalid decimal telemetry");
            return text;
        }
        case pb::ScalarValue::kBytesValue:
            return "\"" + service::utils::hexEncode(reinterpret_cast<const unsigned char*>(value.bytes_value().data()), value.bytes_value().size()) + "\"";
        default:
            return "null";
        }
    }

    static std::string scalarKind(const pb::ScalarValue& value) {
        switch (value.kind()) {
        case pb::VALUE_BOOL:
            return "BOOL";
        case pb::VALUE_SIGNED:
            return "SIGNED";
        case pb::VALUE_UNSIGNED:
            return "UNSIGNED";
        case pb::VALUE_DOUBLE:
            return "DOUBLE";
        case pb::VALUE_STRING:
            return "STRING";
        case pb::VALUE_DECIMAL:
            return "DECIMAL";
        case pb::VALUE_BYTES:
            return "BYTES";
        default:
            return "UNSPECIFIED";
        }
    }

    static std::string scalarText(const pb::ScalarValue& value) {
        switch (value.value_case()) {
        case pb::ScalarValue::kBoolValue:
            return value.bool_value() ? "1" : "0";
        case pb::ScalarValue::kSignedValue:
            return std::to_string(value.signed_value());
        case pb::ScalarValue::kUnsignedValue:
            return std::to_string(value.unsigned_value());
        case pb::ScalarValue::kDoubleValue: {
            std::ostringstream output;
            output.precision(15);
            output << value.double_value();
            return output.str();
        }
        case pb::ScalarValue::kStringValue:
            return value.string_value();
        case pb::ScalarValue::kDecimalValue:
            return scalarJson(value);
        case pb::ScalarValue::kBytesValue:
            return service::utils::hexEncode(reinterpret_cast<const unsigned char*>(value.bytes_value().data()), value.bytes_value().size());
        default:
            return {};
        }
    }

    static std::string telemetryJson(const pb::TelemetryRecord& record) {
        std::string output = "{\"function_code\":\"" +
                             service::utils::jsonEscape(record.function_code()) +
                             "\",\"function_name\":\"" +
                             service::utils::jsonEscape(record.function_name()) + "\",\"direction\":\"" +
                             service::utils::jsonEscape(record.direction()) + "\",\"values\":{";
        bool first = true;
        for (const auto& item : record.values()) {
            std::string valueJson = item.has_value() ? scalarJson(item.value()) : "null";
            std::string dataType = item.has_value() ? scalarKind(item.value()) : "UNSPECIFIED";
            if (!item.encoding().empty()) {
                if (record.protocol() != pb::PROTOCOL_SL651 || item.has_value() ||
                    item.encoded_value().empty() || item.encoded_value().size() > 8192)
                    throw std::runtime_error("invalid edge SL651 binary element");
                const auto& bytes = item.encoded_value();
                std::string decoded;
                if (item.encoding() == "HEX" || item.encoding() == "DICT") {
                    decoded = service::utils::hexEncode(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size());
                    for (char& digit : decoded)
                        if (digit >= 'a' && digit <= 'f') digit = static_cast<char>(digit - 'a' + 'A');
                }
                else if (item.encoding() == "JPEG") {
                    if (bytes.size() <= 2 || static_cast<unsigned char>(bytes[0]) != 0xFF ||
                        static_cast<unsigned char>(bytes[1]) != 0xD8) decoded = "INVALID_JPEG";
                    else {
                        std::string encoded(4 * ((bytes.size() + 2) / 3) + 1, '\0');
                        const auto size = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()),
                            reinterpret_cast<const unsigned char*>(bytes.data()), static_cast<int>(bytes.size()));
                        if (size < 0) throw std::runtime_error("edge JPEG encoding failed");
                        encoded.resize(static_cast<std::size_t>(size));
                        decoded = "data:image/jpeg;base64," + encoded;
                    }
                } else throw std::runtime_error("unsupported edge binary encoding");
                valueJson = "\"" + service::utils::jsonEscape(decoded) + "\"";
                dataType = item.encoding();
            } else if (!item.encoded_value().empty())
                throw std::runtime_error("edge binary element has no encoding");
            if (item.derived() && item.has_value()) {
                if (item.value().value_case() == pb::ScalarValue::kBoolValue) valueJson = item.value().bool_value() ? "true" : "false";
                else if (item.value().value_case() == pb::ScalarValue::kDoubleValue) {
                    char buffer[64];
                    const auto result = std::to_chars(buffer, buffer + sizeof(buffer), item.value().double_value());
                    if (result.ec != std::errc{}) throw std::runtime_error("cannot serialize derived number");
                    valueJson.assign(buffer, result.ptr);
                }
            }
            if (!first)
                output.push_back(',');
            output += "\"" + service::utils::jsonEscape(item.element_id()) + "\":{\"name\":\"" +
                      service::utils::jsonEscape(item.name()) + "\",\"value\":" +
                      valueJson +
                      ",\"dataType\":\"" +
                      service::utils::jsonEscape(dataType) +
                      "\"" +
                      ",\"unit\":\"" + service::utils::jsonEscape(item.unit()) + "\"";
            if (item.derived()) output += ",\"derived\":true,\"visible\":" + std::string(item.hidden() ? "false" : "true") +
                ",\"quality\":" + service::utils::jsonQuoted(item.quality()) + ",\"sample_time_ms\":" + std::to_string(item.sample_time_ms());
            output += '}';
            first = false;
        }
        output += "}}";
        return output;
    }

};

using service::message::edge::kProtocolVersion;
using service::message::edge::kOldestCompatibleProtocolVersion;
using service::message::edge::kMaxMessageSize;

inline constexpr bool supportsProtocolVersion(std::uint32_t version) noexcept {
    return version >= kOldestCompatibleProtocolVersion && version <= kProtocolVersion;
}

inline constexpr bool terminalCommandResultState(pb::CommandState state) noexcept {
    return state == pb::COMMAND_STATE_SUCCEEDED ||
           state == pb::COMMAND_STATE_READBACK_MISMATCH ||
           state == pb::COMMAND_STATE_DEVICE_OFFLINE ||
           state == pb::COMMAND_STATE_TIMED_OUT ||
           state == pb::COMMAND_STATE_REJECTED || state == pb::COMMAND_STATE_FAILED;
}

inline constexpr bool persistableCommandResult(pb::CommandState state,
                                                bool writeAckMissing) noexcept {
    return writeAckMissing || terminalCommandResultState(state);
}

inline bool containsCaseInsensitive(std::string_view text, std::string_view needle) noexcept {
    if (needle.empty())
        return true;
    for (std::size_t start = 0; start + needle.size() <= text.size(); ++start) {
        bool matches = true;
        for (std::size_t offset = 0; offset < needle.size(); ++offset) {
            if (std::tolower(static_cast<unsigned char>(text[start + offset])) !=
                std::tolower(static_cast<unsigned char>(needle[offset]))) {
                matches = false;
                break;
            }
        }
        if (matches)
            return true;
    }
    return false;
}

inline std::string_view commandResultDomainState(pb::CommandState state,
                                                  bool writeAckMissing,
                                                  std::string_view message) noexcept {
    if (writeAckMissing)
        return "UNKNOWN";
    if (state == pb::COMMAND_STATE_READBACK_MISMATCH ||
        (containsCaseInsensitive(message, "readback") &&
         containsCaseInsensitive(message, "mismatch")))
        return "READBACK_MISMATCH";
    if (state == pb::COMMAND_STATE_SUCCEEDED)
        return "SUCCEEDED";
    if (containsCaseInsensitive(message, "readback"))
        return "UNKNOWN";
    if (state == pb::COMMAND_STATE_DEVICE_OFFLINE || state == pb::COMMAND_STATE_REJECTED)
        return "REJECTED";
    if (state == pb::COMMAND_STATE_FAILED)
        return "FAILED";
    return "UNKNOWN";
}

using service::message::edge::authKey;

inline bool validImei(std::string_view imei) {
    if (imei.size() != 15)
        return false;
    unsigned sum = 0;
    for (std::size_t index = 0; index < imei.size(); ++index) {
        if (imei[index] < '0' || imei[index] > '9')
            return false;
        unsigned digit = static_cast<unsigned>(imei[index] - '0');
        if ((index & 1U) != 0U) {
            digit *= 2U;
            if (digit > 9U)
                digit -= 9U;
        }
        sum += digit;
    }
    return sum % 10U == 0U;
}

using service::common::hexDigit;
using service::common::uuidBytes;

inline bool validSessionPlatformId(std::string_view value) noexcept {
    if (value.size() != 16)
        return false;
    for (const unsigned char byte : value)
        if (byte != 0)
            return true;
    return false;
}

using service::common::uuidText;

// 0.3.44/0.3.45 do not report acquisition identities. Keep each original packet
// independently identifiable without inventing collection-round boundaries.
inline std::string debugAcquisitionId(std::string_view nodeId, const pb::RawPacket& packet) {
    if (packet.packet_id().size() != 16)
        throw std::invalid_argument("debug packet ID is required");
    if (packet.acquisition_id().empty())
        return "legacy:" + std::string(nodeId) + ":" + uuidText(packet.packet_id());
    if (packet.acquisition_id().size() != 16)
        throw std::invalid_argument("invalid debug acquisition ID");
    return uuidText(packet.acquisition_id());
}


inline std::string bytes(const std::uint8_t* data, std::size_t size) {
    if (size == 0)
        return {};
    return {reinterpret_cast<const char*>(data), size};
}

// Device heartbeat payload text ("OFF" / "ASCII" / "HEX") as configured in the device model.
inline std::vector<std::uint8_t> packetBytes(std::string_view mode, std::string_view content,
                                             std::string_view name) {
    std::vector<std::uint8_t> output;
    if (mode == "OFF" || content.empty())
        return output;
    if (mode == "ASCII") {
        output.assign(content.begin(), content.end());
        return output;
    }
    if (mode != "HEX")
        throw std::runtime_error("invalid edge config packet mode: " + std::string(name));

    int high = -1;
    for (const char character : content) {
        if (character == ' ' || character == '\t' || character == '\r' || character == '\n')
            continue;
        const int digit = protocol::hexDigit(character);
        if (digit < 0)
            throw std::runtime_error("invalid edge config hex: " + std::string(name));
        if (high < 0)
            high = digit;
        else {
            output.push_back(static_cast<std::uint8_t>((high << 4U) | digit));
            high = -1;
        }
    }
    if (high >= 0)
        throw std::runtime_error("invalid edge config hex: " + std::string(name));
    return output;
}

inline void packet(std::string* output, std::string_view mode, std::string_view content,
                   std::string_view name) {
    const auto value = packetBytes(mode, content, name);
    output->assign(protocol::bytes(value.data(), value.size()));
}

inline double number(std::string_view value, double fallback = 0.0) {
    if (value.empty())
        return fallback;
    const auto result = service::utils::decimal(value);
    return result.value_or(fallback);
}

inline bool fitsNanopbLimits(const google::protobuf::Message& message) {
    const auto* descriptor = message.GetDescriptor();
    const auto* reflection = message.GetReflection();
    for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
        const auto* field = descriptor->field(fieldIndex);
        const auto& options = field->options();
        const NanoPBOptions* limits =
            options.HasExtension(::nanopb) ? &options.GetExtension(::nanopb) : nullptr;
        const int count = field->is_repeated() ? reflection->FieldSize(message, field)
                                                : (reflection->HasField(message, field) ? 1 : 0);
        if (limits && field->is_repeated() && limits->has_max_count() &&
            count > limits->max_count())
            return false;
        for (int index = 0; index < count; ++index) {
            if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING) {
                const auto& value = field->is_repeated()
                                        ? reflection->GetRepeatedStringReference(
                                              message, field, index, nullptr)
                                        : reflection->GetStringReference(message, field, nullptr);
                const auto limit =
                    field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES
                        ? (limits && limits->has_max_size() ? limits->max_size() : -1)
                        : (limits && limits->has_max_length() ? limits->max_length() : -1);
                if (limit >= 0 && value.size() > static_cast<std::size_t>(limit))
                    return false;
            } else if (field->cpp_type() ==
                       google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
                const auto& value =
                    field->is_repeated() ? reflection->GetRepeatedMessage(message, field, index)
                                         : reflection->GetMessage(message, field);
                if (!fitsNanopbLimits(value))
                    return false;
            }
        }
    }
    return true;
}

inline pb::Envelope outbound(std::string_view messageId, std::int64_t createdAtMs,
                             std::string_view platformId, std::string_view nodeId, std::uint64_t epoch = 0,
                             std::uint64_t sequence = 0,
                             std::uint32_t protocolVersion = kProtocolVersion) {
    pb::Envelope result;
    result.set_protocol_version(protocolVersion);
    result.set_session_epoch(epoch);
    result.set_sequence(sequence);
    result.set_created_at_ms(createdAtMs);
    std::uint8_t message[16]{};
    if (uuidBytes(messageId, message))
        result.set_message_id(bytes(message, 16));
    std::uint8_t platform[16]{};
    std::uint8_t node[16]{};
    if (uuidBytes(platformId, platform))
        result.set_platform_id(bytes(platform, 16));
    if (uuidBytes(nodeId, node))
        result.set_node_id(bytes(node, 16));
    return result;
}

inline void bindSession(pb::Envelope& envelope, std::string_view platformBytes,
                        std::string_view nodeBytes, std::uint64_t epoch,
                        std::uint64_t sequence,
                        std::uint32_t protocolVersion = kProtocolVersion) {
    envelope.set_protocol_version(protocolVersion);
    envelope.set_session_epoch(epoch);
    envelope.set_sequence(sequence);
    envelope.set_platform_id(platformBytes);
    envelope.set_node_id(nodeBytes);
}

inline bool decode(std::string_view wire, pb::Envelope& output) {
    output.Clear();
    if (wire.size() > kMaxMessageSize ||
        !output.ParseFromArray(wire.data(), static_cast<int>(wire.size())) ||
        !output.IsInitialized() || !fitsNanopbLimits(output)) {
        output.Clear();
        return false;
    }
    return true;
}

inline std::string encode(const pb::Envelope& input) {
    const auto size = input.ByteSizeLong();
    if (size == 0 || size > kMaxMessageSize || !fitsNanopbLimits(input))
        return {};
    std::string wire;
    if (!input.SerializeToString(&wire))
        return {};
    return wire;
}

} // namespace service::edge::protocol
