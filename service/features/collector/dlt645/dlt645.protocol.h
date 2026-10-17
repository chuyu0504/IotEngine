#pragma once

#include <array>
#include <charconv>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "service/features/collector/collector.protocol.h"
#include "service/features/collector/dlt645/dlt645.types.h"
#include "service/features/collector/polling/polling.protocol.h"

namespace service::collector::dlt645 {

class FrameCodec final {
  public:
    struct Frame {
        std::string address;
        std::uint8_t control = 0;
        std::vector<std::uint8_t> data;
    };

    static std::vector<std::uint8_t> identifier(std::string_view value, Version version) {
        const auto length = version == Version::V2007 ? 4U : 2U;
        if (value.size() != length * 2) {
            throw std::invalid_argument("dlt645_identifier_invalid");
        }
        std::uint32_t identifier = 0;
        const auto result = std::from_chars(value.data(), value.data() + value.size(), identifier, 16);
        if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
            throw std::invalid_argument("dlt645_identifier_invalid");
        }
        std::vector<std::uint8_t> bytes;
        for (std::size_t i = 0; i < length; ++i) {
            bytes.push_back(static_cast<std::uint8_t>(identifier >> (i * 8)));
        }
        return bytes;
    }

    static std::vector<std::uint8_t> read(const Connection& connection, std::string_view address, std::string_view dataIdentifier, std::optional<std::uint8_t> sequence = std::nullopt) {
        auto data = identifier(dataIdentifier, connection.version);
        const auto control = static_cast<std::uint8_t>(connection.version == Version::V2007 ? (sequence ? 0x12 : 0x11) : (sequence ? 0x02 : 0x01));
        if (sequence) {
            if (*sequence == 0) {
                throw std::invalid_argument("dlt645_sequence_invalid");
            }
            data.push_back(*sequence);
        }
        return frame(address, control, data, connection.wakeupBytes);
    }

    static std::vector<std::uint8_t> frame(std::string_view address, std::uint8_t control, std::span<const std::uint8_t> data, std::uint8_t wakeupBytes = 0) {
        if (address.size() != 12 || data.size() > 200 || wakeupBytes > 4) {
            throw std::invalid_argument("dlt645_frame_argument_invalid");
        }
        std::vector<std::uint8_t> bytes(wakeupBytes, 0xfe);
        bytes.push_back(0x68);
        for (std::size_t i = address.size(); i > 0; i -= 2) {
            const auto high = address[i - 2], low = address[i - 1];
            if (high < '0' || high > '9' || low < '0' || low > '9') {
                throw std::invalid_argument("dlt645_address_invalid");
            }
            bytes.push_back(static_cast<std::uint8_t>((high - '0') * 16 + low - '0'));
        }
        bytes.insert(bytes.end(), { 0x68, control, static_cast<std::uint8_t>(data.size()) });
        for (auto value : data) {
            bytes.push_back(static_cast<std::uint8_t>(value + 0x33));
        }
        std::uint8_t checksum = 0;
        for (std::size_t i = wakeupBytes; i < bytes.size(); ++i) {
            checksum += bytes[i];
        }
        bytes.push_back(checksum);
        bytes.push_back(0x16);
        return bytes;
    }

    // 调用方保留 TCP 分片，先跳过唤醒字节；返回从 68H 开始的帧长度。
    static std::optional<std::size_t> frameLength(std::span<const std::uint8_t> bytes) {
        if (bytes.empty()) {
            return std::nullopt;
        }
        if (bytes[0] != 0x68) {
            throw std::invalid_argument("dlt645_frame_start_invalid");
        }
        if (bytes.size() < 10) {
            return std::nullopt;
        }
        if (bytes[7] != 0x68 || bytes[9] > 200) {
            throw std::invalid_argument("dlt645_frame_header_invalid");
        }
        return 12 + bytes[9];
    }

    static Frame parse(std::span<const std::uint8_t> bytes) {
        const auto length = frameLength(bytes);
        if (!length || bytes.size() != *length || bytes.back() != 0x16) {
            throw std::invalid_argument("dlt645_frame_incomplete");
        }
        std::uint8_t checksum = 0;
        for (std::size_t i = 0; i < bytes.size() - 2; ++i) {
            checksum += bytes[i];
        }
        if (checksum != bytes[bytes.size() - 2]) {
            throw std::invalid_argument("dlt645_checksum_invalid");
        }
        Frame result;
        for (std::size_t i = 6; i > 0; --i) {
            const auto high = bytes[i] >> 4, low = bytes[i] & 15;
            if (high > 9 || low > 9) {
                throw std::invalid_argument("dlt645_address_invalid");
            }
            result.address.push_back(static_cast<char>('0' + high));
            result.address.push_back(static_cast<char>('0' + low));
        }
        result.control = bytes[8];
        for (std::size_t i = 10; i < bytes.size() - 2; ++i) {
            result.data.push_back(static_cast<std::uint8_t>(bytes[i] - 0x33));
        }
        return result;
    }

    static std::string bcdValue(std::span<const std::uint8_t> bytes, unsigned decimals, bool signedValue) {
        if (bytes.empty() || bytes.size() > 8 || decimals > 8) {
            throw std::invalid_argument("dlt645_bcd_size_invalid");
        }
        const bool negative = signedValue && (bytes.back() & 0x80) != 0;
        std::string digits;
        for (std::size_t i = bytes.size(); i > 0; --i) {
            const auto value = static_cast<std::uint8_t>(bytes[i - 1] & (signedValue && i == bytes.size() ? 0x7f : 0xff));
            if ((value >> 4) > 9 || (value & 15) > 9) {
                throw std::invalid_argument("dlt645_bcd_digit_invalid");
            }
            digits.push_back(static_cast<char>('0' + (value >> 4)));
            digits.push_back(static_cast<char>('0' + (value & 15)));
        }
        const auto significant = digits.find_first_not_of('0');
        digits = significant == std::string::npos ? "0" : digits.substr(significant);
        if (decimals) {
            if (digits.size() <= decimals) {
                digits.insert(0, decimals + 1 - digits.size(), '0');
            }
            digits.insert(digits.size() - decimals, 1, '.');
        }
        if (negative && significant != std::string::npos) {
            digits.insert(0, 1, '-');
        }
        return digits;
    }

    static std::vector<std::uint8_t> bcdBytes(std::string_view value, std::size_t width, unsigned decimals, bool signedValue) {
        const auto word = command::encodeDlt645Bcd(value, width, decimals, signedValue);
        std::vector<std::uint8_t> result(width);
        for (std::size_t index = 0; index < width; ++index) {
            result[index] = static_cast<std::uint8_t>(word >> (index * 8));
        }
        return result;
    }

    static std::vector<std::uint8_t> hexBytes(std::string_view text) {
        if (text.size() % 2 != 0) {
            throw std::invalid_argument("dlt645_hex_invalid");
        }
        std::vector<std::uint8_t> result;
        for (std::size_t i = 0; i < text.size(); i += 2) {
            unsigned value = 0;
            const auto parsed = std::from_chars(text.data() + i, text.data() + i + 2, value, 16);
            if (parsed.ec != std::errc{} || parsed.ptr != text.data() + i + 2) {
                throw std::invalid_argument("dlt645_hex_invalid");
            }
            result.push_back(static_cast<std::uint8_t>(value));
        }
        return result;
    }

    static std::vector<std::uint8_t> write(const Connection& connection, std::string_view address, std::string_view dataIdentifier, std::span<const std::uint8_t> values) {
        if (connection.writePassword.size() != 8 ||
            (connection.version == Version::V2007 && connection.operatorCode.size() != 8)) {
            throw std::invalid_argument("dlt645_write_credentials_required");
        }
        auto data = identifier(dataIdentifier, connection.version);
        const auto password = hexBytes(connection.writePassword);
        data.insert(data.end(), password.begin(), password.end());
        if (connection.version == Version::V2007) {
            const auto operatorCode = hexBytes(connection.operatorCode);
            data.insert(data.end(), operatorCode.begin(), operatorCode.end());
        }
        data.insert(data.end(), values.begin(), values.end());
        if (data.size() > 50) {
            throw std::invalid_argument("dlt645_write_data_too_large");
        }
        return frame(address, connection.version == Version::V2007 ? 0x14 : 0x04, data, connection.wakeupBytes);
    }
};

class Exchange final {
  public:
    static constexpr bool sharedConnection = true;

    explicit Exchange(const DeviceDefinition& device) : connection_(device.dlt645Connection), address_(device.code) {}

    std::vector<std::uint8_t> handshake() const { return {}; }

    void acceptHandshake(std::span<const std::uint8_t>) { throw std::invalid_argument("dlt645_unexpected_handshake"); }

    void stripPrefix(std::vector<std::uint8_t>& bytes) const {
        while (!bytes.empty() && bytes.front() == 0xfe) {
            bytes.erase(bytes.begin());
        }
    }

    std::optional<std::size_t> frameLength(std::span<const std::uint8_t> bytes) const { return FrameCodec::frameLength(bytes); }

    std::vector<std::uint8_t> read(const ElementDefinition& element) {
        sequence_.reset();
        collected_.clear();
        return FrameCodec::read(connection_, address_, element.guideHex);
    }

    std::vector<std::uint8_t> write(const ElementDefinition& element, std::span<const std::uint8_t> data) const {
        return FrameCodec::write(connection_, address_, element.guideHex, data);
    }

    polling::Reply response(const ElementDefinition& element, bool write, std::span<const std::uint8_t> bytes) {
        const auto frame = FrameCodec::parse(bytes);
        const auto function = connection_.version == Version::V2007
            ? (write ? 0x14 : sequence_ ? 0x12
                                        : 0x11)
            : (write ? 0x04 : sequence_ ? 0x02
                                        : 0x01);
        if (frame.address != address_ || (frame.control & 0x80) == 0 || (frame.control & 0x1f) != function) {
            throw std::invalid_argument("dlt645_response_identity_mismatch");
        }
        if (frame.control & 0x40) {
            if (frame.data.size() != 1 || (frame.control & 0x20)) {
                throw std::invalid_argument("dlt645_error_frame_invalid");
            }
            return { .error = "dlt645_error:" + std::to_string(frame.data.front()) };
        }
        if (write) {
            if (!frame.data.empty() || (frame.control & 0x20)) {
                throw std::invalid_argument("dlt645_write_ack_invalid");
            }
            return {};
        }
        const auto identifier = FrameCodec::identifier(element.guideHex, connection_.version);
        if (frame.data.size() < identifier.size() + (sequence_ ? 1 : 0) ||
            !std::equal(identifier.begin(), identifier.end(), frame.data.begin())) {
            throw std::invalid_argument("dlt645_response_identifier_mismatch");
        }
        if (sequence_ && frame.data.back() != *sequence_) {
            throw std::invalid_argument("dlt645_response_sequence_mismatch");
        }
        const auto end = frame.data.end() - (sequence_ ? 1 : 0);
        collected_.insert(collected_.end(), frame.data.begin() + identifier.size(), end);
        if (element.length <= 0 || collected_.size() > static_cast<std::size_t>(element.length)) {
            throw std::invalid_argument("dlt645_response_data_too_large");
        }
        if (frame.control & 0x20) {
            if (sequence_ == 255 || collected_.size() == static_cast<std::size_t>(element.length)) {
                throw std::invalid_argument("dlt645_response_sequence_overflow");
            }
            sequence_ = static_cast<std::uint8_t>(sequence_.value_or(0) + 1);
            return { .nextRequest = FrameCodec::read(connection_, address_, element.guideHex, sequence_) };
        }
        if (collected_.size() != static_cast<std::size_t>(element.length)) {
            throw std::invalid_argument("dlt645_response_data_size_mismatch");
        }
        return { .data = std::move(collected_) };
    }

    std::vector<std::uint8_t> encode(const ElementDefinition& element, std::string_view value) const {
        if (element.dataType == "HEX") {
            auto bytes = FrameCodec::hexBytes(value);
            if (bytes.size() != static_cast<std::size_t>(element.length)) {
                throw std::invalid_argument("dlt645_point_size_invalid");
            }
            return bytes;
        }
        return FrameCodec::bcdBytes(value, element.length, static_cast<unsigned>(element.digits), element.dataType == "BCD_SIGNED");
    }

    std::string decode(const ElementDefinition& element, std::span<const std::uint8_t> data) const {
        if (element.dataType == "HEX") {
            constexpr char hex[] = "0123456789ABCDEF";
            std::string value;
            for (auto byte : data) {
                value += hex[byte >> 4];
                value += hex[byte & 15];
            }
            return service::utils::jsonQuoted(value);
        }
        return FrameCodec::bcdValue(data, static_cast<unsigned>(element.digits), element.dataType == "BCD_SIGNED");
    }

  private:
    Connection connection_;
    std::string address_;
    std::optional<std::uint8_t> sequence_;
    std::vector<std::uint8_t> collected_;
};

class SessionFactory final : public ProtocolSessionFactory {
  public:
    const ProtocolDefinition& definition() const noexcept override { return kDlt645Protocol; }

    std::vector<std::uint8_t> packetForLogging(std::span<const std::uint8_t> bytes) const override {
        std::vector<std::uint8_t> result(bytes.begin(), bytes.end());
        std::size_t start = 0;
        while (start < bytes.size() && bytes[start] == 0xfe) {
            ++start;
        }
        if (bytes.size() >= start + 10 && bytes[start] == 0x68 && bytes[start + 7] == 0x68 &&
            (bytes[start + 8] == 0x14 || bytes[start + 8] == 0x04)) {
            const auto first = start + 10 + (bytes[start + 8] == 0x14 ? 4 : 2);
            const auto last = std::min(result.size(), first + (bytes[start + 8] == 0x14 ? 8 : 4));
            if (first < last) {
                std::fill(result.begin() + first, result.begin() + last, 0);
            }
        }
        return result;
    }

  protected:
    std::unique_ptr<ProtocolSession> createDeviceSession(const LinkDefinition& link, std::string_view connectionId, std::string_view, const std::shared_ptr<const RuntimeSnapshot>& snapshot, std::vector<const DeviceDefinition*> devices) const override {
        return std::make_unique<polling::Session<Exchange>>(link, std::string(connectionId), snapshot, std::move(devices));
    }
};

} // namespace service::collector::dlt645
