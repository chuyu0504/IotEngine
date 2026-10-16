#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "service/features/collector/collector.protocol.h"
#include "service/utils/json.h"

namespace service::collector::sl651 {

namespace detail {

inline std::uint16_t readBe16(std::span<const std::uint8_t> bytes, std::size_t offset) {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[offset]) << 8U) |
                                      bytes[offset + 1]);
}

inline std::uint16_t crc16Modbus(std::span<const std::uint8_t> bytes) noexcept {
    std::uint16_t crc = 0xFFFFU;
    for (const auto byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc & 1U) != 0 ? static_cast<std::uint16_t>((crc >> 1U) ^ 0xA001U)
                                  : static_cast<std::uint16_t>(crc >> 1U);
    }
    return crc;
}

inline std::optional<std::string> bcd(std::span<const std::uint8_t> bytes) {
    std::string result;
    result.reserve(bytes.size() * 2);
    for (const auto byte : bytes) {
        const auto high = static_cast<std::uint8_t>(byte >> 4U);
        const auto low = static_cast<std::uint8_t>(byte & 0x0FU);
        if (high > 9 || low > 9)
            return std::nullopt;
        result.push_back(static_cast<char>('0' + high));
        result.push_back(static_cast<char>('0' + low));
    }
    return result;
}

inline std::string hexByte(std::uint8_t byte) {
    static constexpr std::array<char, 16> digits{'0', '1', '2', '3', '4', '5', '6', '7',
                                                 '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    return {digits[byte >> 4U], digits[byte & 0x0FU]};
}

inline std::string base64(std::span<const std::uint8_t> bytes) {
    static constexpr std::string_view alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string result;
    result.reserve(((bytes.size() + 2) / 3) * 4);
    std::size_t offset = 0;
    while (offset + 3 <= bytes.size()) {
        const auto value = (static_cast<std::uint32_t>(bytes[offset]) << 16U) |
                           (static_cast<std::uint32_t>(bytes[offset + 1]) << 8U) |
                           bytes[offset + 2];
        result.push_back(alphabet[(value >> 18U) & 0x3FU]);
        result.push_back(alphabet[(value >> 12U) & 0x3FU]);
        result.push_back(alphabet[(value >> 6U) & 0x3FU]);
        result.push_back(alphabet[value & 0x3FU]);
        offset += 3;
    }
    const auto remaining = bytes.size() - offset;
    if (remaining == 1) {
        const auto value = static_cast<std::uint32_t>(bytes[offset]) << 16U;
        result.push_back(alphabet[(value >> 18U) & 0x3FU]);
        result.push_back(alphabet[(value >> 12U) & 0x3FU]);
        result += "==";
    } else if (remaining == 2) {
        const auto value = (static_cast<std::uint32_t>(bytes[offset]) << 16U) |
                           (static_cast<std::uint32_t>(bytes[offset + 1]) << 8U);
        result.push_back(alphabet[(value >> 18U) & 0x3FU]);
        result.push_back(alphabet[(value >> 12U) & 0x3FU]);
        result.push_back(alphabet[(value >> 6U) & 0x3FU]);
        result.push_back('=');
    }
    return result;
}

inline std::vector<std::uint8_t> hexBytes(std::string_view value) { return message::fromHex(value); }

inline std::uint8_t bcdByte(unsigned value) {
    return static_cast<std::uint8_t>(((value / 10U) << 4U) | (value % 10U));
}

inline std::vector<std::uint8_t> bcdAddress(std::string_view value, std::size_t bytes) {
    if (value.size() > bytes * 2 || !std::ranges::all_of(value, [](unsigned char character) {
            return std::isdigit(character);
        }))
        throw std::invalid_argument("command_invalid: SL651 device code is invalid");
    std::string padded(bytes * 2 - value.size(), '0');
    padded.append(value);
    std::vector<std::uint8_t> result;
    result.reserve(bytes);
    for (std::size_t index = 0; index < padded.size(); index += 2)
        result.push_back(
            static_cast<std::uint8_t>(((padded[index] - '0') << 4U) | (padded[index + 1] - '0')));
    return result;
}

inline std::vector<std::uint8_t> encodeValue(const ElementDefinition& element,
                                             std::string_view value) {
    const auto length = static_cast<std::size_t>(std::max<std::int64_t>(1, element.length));
    if (element.encoding == "BCD") {
        const auto parsed = command::decimal(value, element.name);
        if (element.length < 1 || element.length > 31 || element.digits < 0 || element.digits > 7)
            throw std::invalid_argument("command_invalid: SL651 data definition is invalid");
        const auto capacity = length - (parsed < 0 ? 1 : 0);
        std::ostringstream decimal;
        decimal.imbue(std::locale::classic());
        decimal << std::fixed << std::setprecision(static_cast<int>(element.digits)) << std::abs(parsed);
        auto decimalValue = decimal.str();
        std::erase(decimalValue, '.');
        if (decimalValue.size() > capacity * 2)
            throw std::invalid_argument("command_invalid: SL651 BCD is too long");
        auto result = bcdAddress(decimalValue, capacity);
        if (parsed < 0) result.insert(result.begin(), 0xFF);
        return result;
    }
    std::string hex(value);
    if (hex.size() > length * 2)
        throw std::invalid_argument("command_invalid: SL651 HEX is too long");
    hex.insert(hex.begin(), length * 2 - hex.size(), '0');
    auto result = message::fromHex(hex);
    if (result.size() != length)
        throw std::invalid_argument("command_invalid: SL651 HEX is invalid");
    return result;
}

inline std::chrono::seconds timezoneOffset(std::string_view timezone) {
    if (timezone.size() != 6 || (timezone[0] != '+' && timezone[0] != '-') || timezone[3] != ':')
        timezone = service::message::device::kDefaultTimezone;
    const auto hours = (timezone[1] - '0') * 10 + timezone[2] - '0';
    const auto minutes = (timezone[4] - '0') * 10 + timezone[5] - '0';
    const auto seconds = std::chrono::hours(hours) + std::chrono::minutes(minutes);
    return timezone[0] == '-' ? -seconds : seconds;
}

inline std::array<std::uint8_t, 6> reportTime(std::string_view timezone) {
    using namespace std::chrono;
    const auto local = floor<seconds>(system_clock::now()) + timezoneOffset(timezone);
    const auto day = floor<days>(local);
    const year_month_day date{day};
    const hh_mm_ss time{local - day};
    return {bcdByte(static_cast<unsigned>(static_cast<int>(date.year()) % 100)),
            bcdByte(static_cast<unsigned>(date.month())),
            bcdByte(static_cast<unsigned>(date.day())),
            bcdByte(static_cast<unsigned>(time.hours().count())),
            bcdByte(static_cast<unsigned>(time.minutes().count())),
            bcdByte(static_cast<unsigned>(time.seconds().count()))};
}

inline std::string elementValue(std::span<const std::uint8_t> bytes,
                                const ElementDefinition& element) {
    if (element.encoding == "BCD") {
        const bool negative = !bytes.empty() && bytes.front() == 0xFF;
        if (negative) bytes = bytes.subspan(1);
        const auto value = bcd(bytes);
        if (!value)
            return {};
        const std::string sign = negative ? "-" : "";
        if (element.digits <= 0)
            return sign + *value;
        const auto digits = static_cast<std::size_t>(element.digits);
        if (digits >= value->size())
            return sign + "0." + std::string(digits - value->size(), '0') + *value;
        return sign + value->substr(0, value->size() - digits) + '.' +
               value->substr(value->size() - digits);
    }
    if (element.encoding == "TIME_YYMMDDHHMMSS") {
        const auto value = bcd(bytes);
        if (!value || value->size() < 10)
            return {};
        return "20" + value->substr(0, 2) + '-' + value->substr(2, 2) + '-' + value->substr(4, 2) +
               'T' + value->substr(6, 2) + ':' + value->substr(8, 2) + ':' +
               (value->size() >= 12 ? value->substr(10, 2) : "00");
    }
    if (element.encoding == "JPEG") {
        if (bytes.size() <= 2 || bytes[0] != 0xFF || bytes[1] != 0xD8)
            return "INVALID_JPEG";
        return "data:image/jpeg;base64," + base64(bytes);
    }
    return message::toHex(std::vector<std::uint8_t>(bytes.begin(), bytes.end()));
}

inline std::optional<std::int64_t> reportTimeMilliseconds(std::span<const std::uint8_t> body,
                                                          std::string_view timezone) {
    if (body.size() < 8 || timezone.size() != 6 ||
        (timezone.front() != '+' && timezone.front() != '-') || timezone[3] != ':')
        return std::nullopt;
    const auto timestamp = bcd(body.subspan(2, 6));
    if (!timestamp || timestamp->size() != 12)
        return std::nullopt;
    const auto pair = [&](std::size_t offset) {
        return ((*timestamp)[offset] - '0') * 10 + ((*timestamp)[offset + 1] - '0');
    };
    const auto year = 2000 + pair(0);
    const auto month = pair(2);
    const auto day = pair(4);
    const auto hour = pair(6);
    const auto minute = pair(8);
    const auto second = pair(10);
    static constexpr std::array<int, 12> monthDays{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const auto leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if (month < 1 || month > 12 || day < 1 ||
        day > monthDays[static_cast<std::size_t>(month - 1)] + (month == 2 && leap ? 1 : 0) ||
        hour > 23 || minute > 59 || second > 59)
        return std::nullopt;
    const auto daysFromCivil = [](int civilYear, unsigned civilMonth, unsigned civilDay) {
        civilYear -= civilMonth <= 2;
        const auto era = (civilYear >= 0 ? civilYear : civilYear - 399) / 400;
        const auto yearOfEra = static_cast<unsigned>(civilYear - era * 400);
        const auto adjustedMonth = civilMonth > 2 ? civilMonth - 3 : civilMonth + 9;
        const auto dayOfYear = (153 * adjustedMonth + 2) / 5 + civilDay - 1;
        const auto dayOfEra = yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear;
        return static_cast<std::int64_t>(era) * 146097 + dayOfEra - 719468;
    };
    const auto offsetHour = (timezone[1] - '0') * 10 + (timezone[2] - '0');
    const auto offsetMinute = (timezone[4] - '0') * 10 + (timezone[5] - '0');
    if (offsetHour > 14 || offsetMinute > 59)
        return std::nullopt;
    auto offsetSeconds = (offsetHour * 60 + offsetMinute) * 60;
    if (timezone.front() == '-')
        offsetSeconds = -offsetSeconds;
    const auto localSeconds =
        daysFromCivil(year, static_cast<unsigned>(month), static_cast<unsigned>(day)) * 86400 +
        hour * 3600 + minute * 60 + second;
    return (localSeconds - offsetSeconds) * 1000;
}

// 定时报、加报的标准正文：流水号、发报时间、测站地址/分类、观测时间。
// 只按结构读取，不能搜索载荷中的 F0F0（图片或自定义数值也可能包含它）。
inline std::optional<std::int64_t> observationTimeMilliseconds(
    std::span<const std::uint8_t> body, std::uint8_t functionCode, std::string_view timezone) {
    if ((functionCode != 0x32 && functionCode != 0x33) || body.size() < 23 ||
        body[8] != 0xF1 || body[9] != 0xF1 || body[16] != 0xF0 || body[17] != 0xF0)
        return std::nullopt;
    std::array<std::uint8_t, 8> timestamp{};
    std::copy_n(body.begin() + 18, 5, timestamp.begin() + 2);
    return reportTimeMilliseconds(timestamp, timezone);
}

} // namespace detail

class Session final : public ProtocolSession,
                      public CommandCapabilitySession,
                      public DeadlineCapabilitySession {
  public:
    Session(LinkDefinition link, std::string connectionId,
            std::shared_ptr<const RuntimeSnapshot> snapshot,
            std::vector<const DeviceDefinition*> devices)
        : link_(std::move(link)), connectionId_(std::move(connectionId)),
          snapshot_(std::move(snapshot)), devices_(std::move(devices)) {
        for (const auto* device : devices_) {
            if (device->sl651ResponseMode != "M1" && device->sl651ResponseMode != "M2" &&
                device->sl651ResponseMode != "M3" && device->sl651ResponseMode != "M4")
                throw std::invalid_argument("invalid SL651 response mode");
            if (!devicesByCode_.emplace(normalizeCode(device->code), device).second)
                throw std::invalid_argument("duplicate SL651 station address within link");
        }
    }

    // 恢复已完成业务交换的请求上下文，用于平台离线解码，不产生发送动作。
    void restoreAcquisitionRequest(std::span<const std::uint8_t> bytes, std::string id) {
        const auto request = parseFrame(std::vector<std::uint8_t>(bytes.begin(), bytes.end()));
        if (!request || request->upstream || request->ending != 0x05 || request->body.size() < 8)
            throw std::invalid_argument("invalid SL651 acquisition request");
        ProtocolCommand command;
        command.deviceCode = request->deviceCode;
        const auto* device = findDevice(command);
        if (!device || ((device->sl651ResponseMode == "M3") != request->multiPacket))
            throw std::invalid_argument("SL651 acquisition request device mismatch");
        boundDeviceIds_.insert(device->id);
        stationHeaders_[request->deviceCode] = *request;
        pendingCommands_[request->deviceCode] = PendingCommand{.id=std::move(id),
            .deviceCode=request->deviceCode, .functionCode=request->functionCode};
    }

    [[nodiscard]] std::vector<ProtocolAction> consume(const ProtocolInput& input) override {
        std::vector<ProtocolAction> actions;
        std::vector<std::uint8_t> bytes(input.bytes.begin(), input.bytes.end());
        receiveBuffer_.insert(receiveBuffer_.end(), bytes.begin(), bytes.end());
        if (receiveBuffer_.size() > kMaximumReceiveBuffer) {
            receiveBuffer_.clear();
            actions.push_back({.kind = ProtocolActionKind::Close,
                               .connectionId = connectionId_,
                               .reason = "sl651_receive_buffer_overflow"});
            return actions;
        }

        while (true) {
            const auto header = std::search(receiveBuffer_.begin(), receiveBuffer_.end(),
                                            kHeader.begin(), kHeader.end());
            if (header == receiveBuffer_.end()) {
                if (!receiveBuffer_.empty() && receiveBuffer_.back() == 0x7E)
                    receiveBuffer_.erase(receiveBuffer_.begin(), receiveBuffer_.end() - 1);
                else
                    receiveBuffer_.clear();
                break;
            }
            if (header != receiveBuffer_.begin())
                receiveBuffer_.erase(receiveBuffer_.begin(), header);
            if (receiveBuffer_.size() < kHeaderLength)
                break;
            const auto bodyLength =
                static_cast<std::size_t>(detail::readBe16(receiveBuffer_, 11) & 0x0FFFU);
            const auto frameLength = kHeaderLength + 1 + bodyLength + 1 + 2;
            if (bodyLength > kMaximumBodyLength) {
                receiveBuffer_.erase(receiveBuffer_.begin());
                continue;
            }
            if (receiveBuffer_.size() < frameLength)
                break;
            std::vector<std::uint8_t> frame(receiveBuffer_.begin(),
                                            receiveBuffer_.begin() +
                                                static_cast<std::ptrdiff_t>(frameLength));
            receiveBuffer_.erase(receiveBuffer_.begin(),
                                 receiveBuffer_.begin() + static_cast<std::ptrdiff_t>(frameLength));
            auto frameActions = consumeFrame(input, std::move(frame));
            actions.insert(actions.end(), std::make_move_iterator(frameActions.begin()),
                           std::make_move_iterator(frameActions.end()));
        }
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> disconnected(std::string_view reason) override {
        std::vector<ProtocolAction> actions;
        for (const auto& [deviceCode, pending] : pendingCommands_) {
            (void)deviceCode;
            actions.push_back({.kind = ProtocolActionKind::FailCommand,
                               .connectionId = connectionId_,
                               .commandId = pending.id,
                               .reason = std::string(reason)});
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_,
                               .deadlineToken = pending.deadlineToken});
        }
        for (const auto& [key, packet] : multiPackets_) {
            (void)key;
            if (!packet.completed) actions.push_back({.kind = ProtocolActionKind::FinishAcquisition,
                .reason = "partial", .acquisitionId = packet.header.acquisitionId});
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_,
                               .deadlineToken = packet.deadlineToken});
        }
        pendingCommands_.clear();
        boundDeviceIds_.clear();
        multiPackets_.clear();
        stationHeaders_.clear();
        ambiguousDevices_.clear();
        recentResponses_.clear();
        reportAcquisitions_.clear();
        completedResponses_.clear();
        confirmedResponses_.clear();
        unpublishedReports_.clear();
        receiveBuffer_.clear();
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> execute(ProtocolCommand command) override {
        if (command.kind == "discovery")
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .commandId = std::move(command.id),
                     .reason = "sl651_discovery_not_supported"}};
        const auto* device = findDevice(command);
        if (!device || !boundDeviceIds_.contains(device->id))
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .commandId = std::move(command.id),
                     .reason = "sl651_device_offline"}};
        const auto normalizedCode = normalizeCode(device->code);
        if (device->sl651ResponseMode == "M1" || ambiguousDevices_.contains(normalizedCode))
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .commandId = std::move(command.id),
                     .reason = device->sl651ResponseMode == "M1"
                                   ? "sl651_m1_has_no_downlink" : "sl651_reconnect_required"}};
        if (pendingCommands_.contains(normalizedCode))
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .commandId = std::move(command.id),
                     .reason = "sl651_command_busy"}};
        if (command.deviceCode.empty())
            command.deviceCode = device->code;
        if (command.payload.empty() && !command.elements.empty()) {
            try {
                compileElementCommand(*device, command);
            } catch (const std::exception& error) {
                return {{.kind = ProtocolActionKind::FailCommand,
                         .connectionId = connectionId_,
                         .deviceId = device->id,
                         .deviceCode = device->code,
                         .commandId = std::move(command.id),
                         .reason = error.what()}};
            }
        }
        const auto request = parseFrame(command.payload);
        if (!request || request->upstream || request->ending != 0x05 ||
            (request->multiPacket && (request->totalPackets != 1 || request->sequence != 1)) ||
            ((device->sl651ResponseMode == "M3") != request->multiPacket) || request->body.size() < 8 ||
            detail::readBe16(request->body, 0) != 0 ||
            !detail::reportTimeMilliseconds(request->body, device->timezone))
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .commandId = std::move(command.id),
                     .reason = "sl651_command_frame_invalid"}};
        const auto& remoteCode = request->deviceCode;
        const auto& station = stationHeaders_.at(normalizedCode);
        if (remoteCode != normalizedCode || normalizeCode(command.deviceCode) != remoteCode ||
            request->centerCode != station.centerCode || request->password != station.password)
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .commandId = std::move(command.id),
                     .reason = "sl651_command_device_mismatch"}};

        const auto token = nextDeadlineToken_++;
        const auto timeout = std::clamp(command.timeout, std::chrono::milliseconds(100),
                                        std::chrono::milliseconds(60000));
        pendingCommands_[remoteCode] =
            PendingCommand{.id = command.id, .deviceCode = remoteCode,
                           .functionCode = request->functionCode, .deadlineToken = token,
                           .timeout = timeout, .retryFrame = command.payload};
        return {{.kind = ProtocolActionKind::Send,
                 .connectionId = connectionId_,
                 .commandId = command.id,
                 .bytes = std::move(command.payload)},
                {.kind = ProtocolActionKind::ScheduleDeadline,
                 .connectionId = connectionId_,
                 .commandId = command.id,
                 .deadlineToken = token,
                 .deadlineAfter = timeout}};
    }

    [[nodiscard]] std::vector<ProtocolAction> deadline(std::uint64_t token) override {
        for (auto current = pendingCommands_.begin(); current != pendingCommands_.end(); ++current) {
            if (current->second.deadlineToken != token)
                continue;
            auto& pending = current->second;
            const auto assembly = multiPackets_.find(pending.deviceCode + ':' + detail::hexByte(pending.functionCode));
            if (assembly != multiPackets_.end() && !assembly->second.completed) {
                pending.deadlineToken = nextDeadlineToken_++;
                return {{.kind = ProtocolActionKind::ScheduleDeadline, .connectionId = connectionId_,
                         .commandId = pending.id, .deadlineToken = pending.deadlineToken,
                         .deadlineAfter = kMultiPacketIdleTimeout}};
            }
            if (pending.retries++ < 2) {
                pending.deadlineToken = nextDeadlineToken_++;
                return {{.kind = ProtocolActionKind::Send, .connectionId = connectionId_,
                         .commandId = pending.id, .bytes = pending.retryFrame},
                        {.kind = ProtocolActionKind::ScheduleDeadline, .connectionId = connectionId_,
                         .commandId = pending.id, .deadlineToken = pending.deadlineToken,
                         .deadlineAfter = pending.timeout}};
            }
            auto command = std::move(pending);
            // 查询流水号固定为 0，超时后无法区分旧响应与同功能码的新响应。
            // 在该连接重新建立前禁止再次发起有歧义的设备命令。
            ambiguousDevices_.insert(command.deviceCode);
            pendingCommands_.erase(current);
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .commandId = std::move(command.id),
                     .reason = "sl651_command_timeout"}};
        }
        for (auto current = multiPackets_.begin(); current != multiPackets_.end(); ++current) {
            if (current->second.deadlineToken != token)
                continue;
            if (current->second.completed) {
                multiPackets_.erase(current);
                return {};
            }
            auto actions = requestMissingPacket(current->second);
            if (actions.empty()) {
                failAssemblyCommand(actions, current->second.header);
                multiPackets_.erase(current);
            }
            return actions;
        }
        return {};
    }

    [[nodiscard]] std::vector<ProtocolAction> parsedPublished(std::uint64_t publicationToken) override {
        for (auto current = unpublishedReports_.begin(); current != unpublishedReports_.end(); ++current) {
            if (current->first != publicationToken) continue;
            auto report = std::move(current->second);
            unpublishedReports_.erase(current);
            const auto& frame = report.header;
            std::vector<ProtocolAction> actions;
            const auto pending = pendingCommands_.find(frame.deviceCode);
            const bool matchesCommand = pending != pendingCommands_.end() &&
                !report.commandId.empty() && pending->second.id == report.commandId;
            if (matchesCommand) {
                auto& confirmed = confirmedResponses_[frame.deviceCode];
                confirmed.insert(report.identity);
                if (confirmed.size() > 64) confirmed.erase(confirmed.begin());
            }
            if (report.confirm || !report.commandId.empty()) {
                const auto ending = frame.totalPackets > 0 || frame.ending == 0x03 ? 0x04 : 0x06;
                auto response = confirmation(frame, static_cast<std::uint8_t>(ending));
                actions.push_back({.kind = ProtocolActionKind::Send, .connectionId = connectionId_,
                                   .bytes = response, .acquisitionId = frame.acquisitionId});
                if (matchesCommand && ending == 0x06) pending->second.retryFrame = std::move(response);
            }
            auto& recent = recentResponses_[frame.deviceCode];
            if (std::find(recent.begin(), recent.end(), report.identity) == recent.end()) {
                recent.push_back(report.identity);
                if (recent.size() > 64) recent.erase(recent.begin());
            }
            if (!matchesCommand) {
                if (!report.commandId.empty())
                    actions.insert(actions.begin(), {.kind = ProtocolActionKind::CompleteCommand,
                        .connectionId = connectionId_, .commandId = report.commandId});
                return actions;
            }
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline, .connectionId = connectionId_,
                               .deadlineToken = pending->second.deadlineToken});
            if (frame.totalPackets > 0 || frame.ending == 0x03) {
                auto& completed = completedResponses_[frame.deviceCode];
                completed.insert_or_assign(report.identity, pending->second.id);
                if (completed.size() > 64) completed.erase(completed.begin());
                actions.insert(actions.begin(), {.kind = ProtocolActionKind::CompleteCommand,
                                   .connectionId = connectionId_, .commandId = pending->second.id});
                pendingCommands_.erase(pending);
            } else {
                pending->second.retries = 0;
                pending->second.deadlineToken = nextDeadlineToken_++;
                actions.push_back({.kind = ProtocolActionKind::ScheduleDeadline,
                                   .connectionId = connectionId_, .commandId = pending->second.id,
                                   .deadlineToken = pending->second.deadlineToken,
                                   .deadlineAfter = pending->second.timeout});
            }
            return actions;
        }
        return {};
    }

  private:
    struct ParsedFrame {
        std::string acquisitionId;
        std::string deviceCode;
        std::uint8_t centerCode = 0;
        std::array<std::uint8_t, 2> password{};
        std::uint8_t functionCode = 0;
        bool upstream = false;
        bool multiPacket = false;
        std::uint8_t ending = 0;
        std::uint16_t totalPackets = 0;
        std::uint16_t sequence = 0;
        std::vector<std::uint8_t> body;
        std::vector<std::vector<std::uint8_t>> rawFrames;
        std::vector<std::string> rawPacketIds;
    };

    struct PendingCommand {
        std::string id;
        std::string deviceCode;
        std::uint8_t functionCode = 0;
        std::uint64_t deadlineToken = 0;
        std::chrono::milliseconds timeout{3000};
        std::vector<std::uint8_t> retryFrame;
        unsigned retries = 0;
    };

    struct MultiPacket {
        std::uint16_t total = 0;
        std::map<std::uint16_t, std::vector<std::uint8_t>> bodies;
        std::map<std::uint16_t, std::vector<std::uint8_t>> rawFrames;
        std::map<std::uint16_t, std::string> rawPacketIds;
        std::uint64_t deadlineToken = 0;
        ParsedFrame header;
        std::map<std::uint16_t, unsigned> retries;
        std::size_t byteCount = 0;
        bool completed = false;
    };

    struct UnpublishedReport {
        std::string commandId;
        std::string identity;
        ParsedFrame header;
        bool confirm = false;
    };

    [[nodiscard]] std::vector<std::uint8_t> confirmation(const ParsedFrame& frame,
                                                        std::uint8_t ending,
                                                        std::uint16_t sequence = 0) const {
        std::vector<std::uint8_t> body;
        if (frame.totalPackets > 0) {
            const auto packed = (static_cast<std::uint32_t>(frame.totalPackets) << 12U) |
                (sequence == 0 ? frame.totalPackets : sequence);
            body = {static_cast<std::uint8_t>(packed >> 16U),
                    static_cast<std::uint8_t>(packed >> 8U), static_cast<std::uint8_t>(packed)};
        }
        body.push_back(frame.body.size() >= 2 ? frame.body[0] : 0);
        body.push_back(frame.body.size() >= 2 ? frame.body[1] : 0);
        const auto time = detail::reportTime(devicesByCode_.at(frame.deviceCode)->timezone);
        body.insert(body.end(), time.begin(), time.end());
        std::vector<std::uint8_t> response{0x7E, 0x7E};
        const auto station = detail::bcdAddress(frame.deviceCode, 5);
        response.insert(response.end(), station.begin(), station.end());
        response.push_back(frame.centerCode);
        response.insert(response.end(), frame.password.begin(), frame.password.end());
        response.push_back(frame.functionCode);
        response.push_back(0x80);
        response.push_back(static_cast<std::uint8_t>(body.size()));
        response.push_back(frame.totalPackets > 0 ? 0x16 : 0x02);
        response.insert(response.end(), body.begin(), body.end());
        response.push_back(ending);
        const auto crc = detail::crc16Modbus(response);
        response.push_back(static_cast<std::uint8_t>(crc >> 8U));
        response.push_back(static_cast<std::uint8_t>(crc));
        return response;
    }

    void appendReportActions(std::vector<ProtocolAction>& actions, const ProtocolInput& input,
                             const DeviceDefinition& device, const ParsedFrame& frame) {
        if (frame.body.size() < 8 || detail::readBe16(frame.body, 0) == 0 ||
            !detail::reportTimeMilliseconds(frame.body, device.timezone)) return;
        auto action = parsedAction(input, device, frame);
        action.publicationToken = nextPublicationToken_++;
        const auto identity = detail::hexByte(frame.functionCode) + ':' +
            message::toHex(std::vector<std::uint8_t>(frame.body.begin(), frame.body.begin() + 8));
        const auto& recent = recentResponses_[frame.deviceCode];
        const auto pending = pendingCommands_.find(frame.deviceCode);
        const bool matchesCommand = pending != pendingCommands_.end() &&
            pending->second.functionCode == frame.functionCode &&
            std::find(recent.begin(), recent.end(), identity) == recent.end();
        const auto& completed = completedResponses_[frame.deviceCode];
        const auto replay = completed.find(identity);
        auto header = frame;
        header.body.resize(8);
        header.rawFrames.clear();
        if (unpublishedReports_.size() >= 1024 && !unpublishedReports_.contains(action.publicationToken)) {
            actions.push_back({.kind = ProtocolActionKind::Close, .connectionId = connectionId_,
                               .reason = "sl651_publication_backlog"});
            return;
        }
        unpublishedReports_.insert_or_assign(action.publicationToken,
            UnpublishedReport{.commandId = replay != completed.end() ? replay->second :
                                  (matchesCommand ? pending->second.id : std::string{}),
                              .identity = identity, .header = std::move(header),
                              .confirm = frame.functionCode != 0x2F &&
                                  (device.sl651ResponseMode == "M2" || frame.totalPackets > 0 ||
                                   confirmedResponses_[frame.deviceCode].contains(identity))});
        actions.push_back(std::move(action));
    }

    [[nodiscard]] std::vector<ProtocolAction> requestMissingPacket(MultiPacket& packet) {
        for (std::uint16_t sequence = 1; sequence <= packet.total; ++sequence) {
            if (packet.bodies.contains(sequence)) continue;
            auto& attempts = packet.retries[sequence];
            if (attempts >= 2) return {};
            ++attempts;
            packet.deadlineToken = nextDeadlineToken_++;
            return {{.kind = ProtocolActionKind::Send, .connectionId = connectionId_,
                     .bytes = confirmation(packet.header, 0x15, sequence), .acquisitionId = packet.header.acquisitionId},
                    {.kind = ProtocolActionKind::ScheduleDeadline, .connectionId = connectionId_,
                     .deadlineToken = packet.deadlineToken, .deadlineAfter = kMultiPacketIdleTimeout}};
        }
        return {};
    }

    void failAssemblyCommand(std::vector<ProtocolAction>& actions, const ParsedFrame& frame) {
        actions.push_back({.kind = ProtocolActionKind::FinishAcquisition,
            .reason = "partial", .acquisitionId = frame.acquisitionId});
        const auto pending = pendingCommands_.find(frame.deviceCode);
        if (pending == pendingCommands_.end() || pending->second.functionCode != frame.functionCode) return;
        actions.push_back({.kind = ProtocolActionKind::CancelDeadline, .connectionId = connectionId_,
                          .deadlineToken = pending->second.deadlineToken});
        actions.push_back({.kind = ProtocolActionKind::FailCommand, .connectionId = connectionId_,
                          .commandId = pending->second.id, .reason = "sl651_packet_timeout"});
        ambiguousDevices_.insert(frame.deviceCode);
        pendingCommands_.erase(pending);
    }

    [[nodiscard]] const DeviceDefinition*
    findDevice(const ProtocolCommand& command) const noexcept {
        if (!command.deviceId.empty()) {
            const auto current =
                std::find_if(devices_.begin(), devices_.end(),
                             [&](const auto* item) { return item->id == command.deviceId; });
            return current == devices_.end() ? nullptr : *current;
        }
        const auto current = devicesByCode_.find(normalizeCode(command.deviceCode));
        return current == devicesByCode_.end() ? nullptr : current->second;
    }

    void compileElementCommand(const DeviceDefinition& device, ProtocolCommand& command) {
        const auto resolved = command::resolve(device, command.elements);
        const auto function = detail::hexBytes(resolved.functionCode);
        if (function.size() != 1)
            throw std::invalid_argument("command_invalid: SL651 function code is invalid");

        // 6.6.2.4：中心站主动发起的下行报文流水号为 0。
        std::vector<std::uint8_t> body{0, 0};
        const auto time = detail::reportTime(device.timezone);
        body.insert(body.end(), time.begin(), time.end());
        std::vector<bool> occupied(8, true);
        for (const auto& input : resolved.elements) {
            if (input.definition->positionMode != "OFFSET") continue;
            const auto offset = input.definition->byteOffset;
            if (offset < 8 || offset > static_cast<std::int64_t>(kMaximumBodyLength))
                throw std::invalid_argument("command_invalid: fixed element overlaps SL651 serial/time");
            auto value = detail::encodeValue(*input.definition, input.value);
            if (value.size() > kMaximumBodyLength - static_cast<std::size_t>(offset))
                throw std::invalid_argument("command_invalid: SL651 fixed element exceeds body");
            const auto end = static_cast<std::size_t>(offset) + value.size();
            if (occupied.size() < end) { occupied.resize(end, false); body.resize(end, 0); }
            for (std::size_t index = 0; index < value.size(); ++index) {
                const auto position = static_cast<std::size_t>(offset) + index;
                if (occupied[position]) throw std::invalid_argument("command_invalid: overlapping SL651 fixed elements");
                occupied[position] = true;
                body[position] = value[index];
            }
        }
        for (const auto& input : resolved.elements) {
            if (input.definition->positionMode == "OFFSET") continue;
            const auto guide = detail::hexBytes(input.definition->guideHex);
            if (guide.size() != 2 && guide.size() != 3)
                throw std::invalid_argument("command_invalid: SL651 guide is invalid");
            if ((guide.front() == 0xFF) != (guide.size() == 3) ||
                ((guide.front() < 0xF0 || guide.front() == 0xFF) &&
                 (input.definition->length != (guide.back() >> 3U) ||
                  (input.definition->encoding == "BCD" && input.definition->digits != (guide.back() & 7U)))))
                throw std::invalid_argument("command_invalid: SL651 data definition mismatch");
            body.insert(body.end(), guide.begin(), guide.end());
            auto value = detail::encodeValue(*input.definition, input.value);
            body.insert(body.end(), value.begin(), value.end());
        }
        const bool multi = device.sl651ResponseMode == "M3";
        if (multi) body.insert(body.begin(), {0x00, 0x10, 0x01});
        if (body.size() > kMaximumBodyLength)
            throw std::invalid_argument("command_invalid: SL651 command body is too large");

        command.payload = {0x7E, 0x7E};
        const auto remote = detail::bcdAddress(normalizeCode(device.code), 5);
        command.payload.insert(command.payload.end(), remote.begin(), remote.end());
        const auto& station = stationHeaders_.at(normalizeCode(device.code));
        command.payload.push_back(station.centerCode);
        command.payload.insert(command.payload.end(), station.password.begin(), station.password.end());
        command.payload.push_back(function.front());
        const auto length = static_cast<std::uint16_t>(0x8000U | body.size());
        command.payload.push_back(static_cast<std::uint8_t>(length >> 8U));
        command.payload.push_back(static_cast<std::uint8_t>(length));
        command.payload.push_back(multi ? 0x16 : 0x02);
        command.payload.insert(command.payload.end(), body.begin(), body.end());
        command.payload.push_back(0x05);
        const auto crc = detail::crc16Modbus(command.payload);
        command.payload.push_back(static_cast<std::uint8_t>(crc >> 8U));
        command.payload.push_back(static_cast<std::uint8_t>(crc));
    }

    [[nodiscard]] std::vector<ProtocolAction> consumeFrame(const ProtocolInput& input,
                                                           std::vector<std::uint8_t> frame) {
        auto parsed = parseFrame(std::move(frame));
        if (!parsed || !parsed->upstream)
            return {};
        const auto device = devicesByCode_.find(parsed->deviceCode);
        if (device == devicesByCode_.end())
            return {};
        const auto& mode = device->second->sl651ResponseMode;
        if (mode == "M1" && (parsed->multiPacket || parsed->ending != 0x03)) return {};
        if (!parsed->multiPacket &&
            (parsed->body.size() < 8 || detail::readBe16(parsed->body, 0) == 0 ||
             !detail::reportTimeMilliseconds(parsed->body, device->second->timezone)))
            return {};
        // 地址、中心站及密码固定在实际接入的连接内，响应不能跨会话匹配。
        const auto known = stationHeaders_.find(parsed->deviceCode);
        if (known != stationHeaders_.end() &&
            (known->second.centerCode != parsed->centerCode || known->second.password != parsed->password))
            return {};
        if (known == stationHeaders_.end()) {
            auto header = *parsed;
            header.body.clear();
            header.rawFrames.clear();
            stationHeaders_.emplace(parsed->deviceCode, std::move(header));
        }
        std::vector<ProtocolAction> actions;
        if (boundDeviceIds_.insert(device->second->id).second) {
            actions.push_back({.kind = ProtocolActionKind::BindDevice,
                               .connectionId = connectionId_,
                               .deviceId = device->second->id,
                               .deviceCode = device->second->code});
        }

        parsed->acquisitionId = acquisitionIdentity(input.messageId, nextPacketSequence_);
        const auto query = pendingCommands_.find(parsed->deviceCode);
        if (query != pendingCommands_.end() && query->second.functionCode == parsed->functionCode)
            parsed->acquisitionId = query->second.id;
        parsed->rawPacketIds = {std::string(input.messageId) + ":frame:" + std::to_string(nextPacketSequence_++)};
        if (parsed->multiPacket) {
            auto more = consumeMulti(input, *device->second, std::move(*parsed));
            actions.insert(actions.end(), std::make_move_iterator(more.begin()), std::make_move_iterator(more.end()));
            return actions;
        }
        auto reportIdentity = detail::hexByte(parsed->functionCode) + ':' +
            message::toHex(std::vector<std::uint8_t>(parsed->body.begin(), parsed->body.begin() + 8));
        // 补发保留流水号和观测正文，但可更新发报时间。身份独立于连接与接收时间，
        // 同一设备重连后也必须落入同一条历史记录；正文变化则保留为不同记录。
        if ((query == pendingCommands_.end() || query->second.functionCode != parsed->functionCode) && detail::observationTimeMilliseconds(
                parsed->body, parsed->functionCode, device->second->timezone)) {
            auto canonical = parsed->body;
            std::fill(canonical.begin() + 2, canonical.begin() + 8, 0);
            reportIdentity = "sl651:observation:v1:" + link_.id + ':' + device->second->id + ':' +
                detail::hexByte(parsed->functionCode) + ':' + message::toHex(canonical);
            parsed->acquisitionId = acquisitionIdentity(reportIdentity, 0);
        }
        auto& acquisitions = reportAcquisitions_[parsed->deviceCode];
        const auto existing = std::find_if(acquisitions.begin(), acquisitions.end(),
            [&](const auto& entry) { return entry.first == reportIdentity; });
        if (existing != acquisitions.end()) parsed->acquisitionId = existing->second;
        else {
            const auto command = pendingCommands_.find(parsed->deviceCode);
            if (command != pendingCommands_.end() && command->second.functionCode == parsed->functionCode)
                parsed->acquisitionId = command->second.id;
            acquisitions.emplace_back(reportIdentity, parsed->acquisitionId);
            if (acquisitions.size() > 64) acquisitions.erase(acquisitions.begin());
        }
        ProtocolAction received{.kind = ProtocolActionKind::ObserveParsed,
            .connectionId = connectionId_, .deviceId = device->second->id,
            .deviceCode = device->second->code};
        received.parsed.valuesJson.clear();
        received.parsed.linkId = link_.id;
        received.parsed.acquisitionId = parsed->acquisitionId;
        received.parsed.deviceId = device->second->id;
        received.parsed.occurredAtMs = input.receivedAtMs;
        received.parsed.rawPayloads = parsed->rawFrames;
        received.parsed.rawPacketIds = parsed->rawPacketIds;
        actions.push_back(std::move(received));

        const auto key = parsed->deviceCode + ':' + detail::hexByte(parsed->functionCode);
        const auto previous = multiPackets_.find(key);
        if (previous != multiPackets_.end()) {
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                .connectionId = connectionId_, .deadlineToken = previous->second.deadlineToken});
            actions.push_back({.kind = ProtocolActionKind::FinishAcquisition,
                .reason = "partial", .acquisitionId = previous->second.header.acquisitionId});
            multiPackets_.erase(previous);
        }
        appendReportActions(actions, input, *device->second, *parsed);
        return actions;
    }

    [[nodiscard]] std::optional<ParsedFrame> parseFrame(std::vector<std::uint8_t> frame) const {
        if (frame.size() < kHeaderLength + 4 || frame[0] != 0x7E || frame[1] != 0x7E)
            return std::nullopt;
        const auto receivedCrc = detail::readBe16(frame, frame.size() - 2);
        if (detail::crc16Modbus(std::span<const std::uint8_t>(frame).first(frame.size() - 2)) !=
            receivedCrc)
            return std::nullopt;
        const auto lengthField = detail::readBe16(frame, 11);
        if ((lengthField & 0xF000U) != 0 && (lengthField & 0xF000U) != 0x8000U)
            return std::nullopt;
        const auto bodyLength = static_cast<std::size_t>(lengthField & 0x0FFFU);
        const auto stx = frame[13];
        if (stx != 0x02 && stx != 0x16)
            return std::nullopt;
        const auto etxOffset = 14 + bodyLength;
        if (etxOffset + 3 != frame.size())
            return std::nullopt;

        ParsedFrame parsed;
        parsed.upstream = (lengthField & 0xF000U) == 0;
        parsed.ending = frame[etxOffset];
        if (parsed.upstream ? (parsed.ending != 0x03 && parsed.ending != 0x17)
                            : (parsed.ending != 0x05 && parsed.ending != 0x06 &&
                               parsed.ending != 0x04 && parsed.ending != 0x1B && parsed.ending != 0x15))
            return std::nullopt;
        const auto remoteOffset = parsed.upstream ? 3U : 2U;
        const auto code =
            detail::bcd(std::span<const std::uint8_t>(frame).subspan(remoteOffset, 5));
        if (!code)
            return std::nullopt;
        parsed.deviceCode = *code;
        parsed.centerCode = frame[parsed.upstream ? 2U : 7U];
        if (parsed.centerCode == 0) return std::nullopt;
        parsed.password = {frame[8], frame[9]};
        parsed.functionCode = frame[10];
        parsed.multiPacket = stx == 0x16;
        std::size_t bodyOffset = 14;
        auto actualBodyLength = bodyLength;
        if (parsed.multiPacket) {
            if (bodyLength < 3)
                return std::nullopt;
            const auto packed = (static_cast<std::uint32_t>(frame[bodyOffset]) << 16U) |
                                (static_cast<std::uint32_t>(frame[bodyOffset + 1]) << 8U) |
                                frame[bodyOffset + 2];
            parsed.totalPackets = static_cast<std::uint16_t>((packed >> 12U) & 0x0FFFU);
            parsed.sequence = static_cast<std::uint16_t>(packed & 0x0FFFU);
            if (parsed.totalPackets == 0 || parsed.sequence == 0 ||
                parsed.sequence > parsed.totalPackets || parsed.totalPackets > kMaximumPackets)
                return std::nullopt;
            bodyOffset += 3;
            actualBodyLength -= 3;
        }
        parsed.body.assign(frame.begin() + static_cast<std::ptrdiff_t>(bodyOffset),
                           frame.begin() +
                               static_cast<std::ptrdiff_t>(bodyOffset + actualBodyLength));
        parsed.rawFrames.push_back(std::move(frame));
        return parsed;
    }

    [[nodiscard]] std::vector<ProtocolAction>
    consumeMulti(const ProtocolInput& input, const DeviceDefinition& device, ParsedFrame frame) {
        const auto key = frame.deviceCode + ':' + detail::hexByte(frame.functionCode);
        auto current = multiPackets_.find(key);
        std::vector<ProtocolAction> actions;

        // 6.6.2.4：首包携带流水号。相同首包是重发，不能清除已收到的后续包。
        if (current != multiPackets_.end() && frame.sequence == 1 &&
            (current->second.total != frame.totalPackets ||
             (current->second.bodies.contains(1) && current->second.bodies.at(1) != frame.body))) {
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_, .deadlineToken = current->second.deadlineToken});
            actions.push_back({.kind = ProtocolActionKind::FinishAcquisition,
                .reason = "partial", .acquisitionId = current->second.header.acquisitionId});
            multiPackets_.erase(current);
            current = multiPackets_.end();
        }
        if (current == multiPackets_.end()) {
            auto header = frame;
            header.body.clear();
            header.rawFrames.clear();
            current = multiPackets_.emplace(key, MultiPacket{.total = frame.totalPackets,
                .header = std::move(header)}).first;
        } else if (current->second.total != frame.totalPackets) {
            return actions;
        }
        auto& packet = current->second;
        frame.acquisitionId = packet.header.acquisitionId;
        ProtocolAction received{.kind = ProtocolActionKind::ObserveParsed,
            .connectionId = connectionId_, .deviceId = device.id, .deviceCode = device.code};
        received.parsed.acquisitionId = frame.acquisitionId;
        received.parsed.valuesJson.clear();
        received.parsed.linkId = link_.id;
        received.parsed.deviceId = device.id;
        received.parsed.occurredAtMs = input.receivedAtMs;
        received.parsed.rawPayloads = frame.rawFrames;
        received.parsed.rawPacketIds = frame.rawPacketIds;
        actions.push_back(std::move(received));
        const auto existing = packet.bodies.find(frame.sequence);
        if (existing != packet.bodies.end() && existing->second != frame.body) return actions;
        if (existing == packet.bodies.end()) {
            auto bufferedBytes = frame.body.size();
            for (const auto& [packetKey, assembly] : multiPackets_) bufferedBytes += assembly.byteCount;
            if (bufferedBytes > kMaximumAssemblyBytes) {
                actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                    .connectionId = connectionId_, .deadlineToken = packet.deadlineToken});
                failAssemblyCommand(actions, packet.header);
                multiPackets_.erase(current);
                return actions;
            }
            packet.byteCount += frame.body.size();
            packet.bodies.emplace(frame.sequence, frame.body);
            packet.rawFrames.emplace(frame.sequence, frame.rawFrames.front());
        }
        packet.rawPacketIds.insert_or_assign(frame.sequence, frame.rawPacketIds.front());
        if (frame.sequence == 1)
            packet.header.body.assign(frame.body.begin(), frame.body.begin() +
                static_cast<std::ptrdiff_t>(std::min<std::size_t>(8, frame.body.size())));
        if (packet.deadlineToken != 0)
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_, .deadlineToken = packet.deadlineToken});
        if (packet.bodies.size() != packet.total) {
            if (frame.ending == 0x03) {
                auto missing = requestMissingPacket(packet);
                if (missing.empty()) {
                    failAssemblyCommand(actions, packet.header);
                    multiPackets_.erase(current);
                }
                actions.insert(actions.end(), std::make_move_iterator(missing.begin()),
                               std::make_move_iterator(missing.end()));
            } else {
                packet.deadlineToken = nextDeadlineToken_++;
                actions.push_back({.kind = ProtocolActionKind::ScheduleDeadline,
                    .connectionId = connectionId_, .deadlineToken = packet.deadlineToken,
                    .deadlineAfter = kMultiPacketIdleTimeout});
            }
            return actions;
        }
        ParsedFrame combined = packet.header;
        combined.multiPacket = false;
        combined.ending = 0x03;
        combined.body.clear();
        combined.rawFrames.clear();
        combined.rawPacketIds.clear();
        for (std::uint16_t sequence = 1; sequence <= packet.total; ++sequence) {
            const auto& body = packet.bodies.at(sequence);
            combined.body.insert(combined.body.end(), body.begin(), body.end());
            combined.rawFrames.push_back(packet.rawFrames.at(sequence));
            combined.rawPacketIds.push_back(packet.rawPacketIds.at(sequence));
        }
        packet.completed = true;
        packet.deadlineToken = nextDeadlineToken_++;
        actions.push_back({.kind = ProtocolActionKind::ScheduleDeadline,
            .connectionId = connectionId_, .deadlineToken = packet.deadlineToken,
            .deadlineAfter = kMultiPacketIdleTimeout});
        // 保留完整包到空闲超时；确认丢失后的重发必须能重新持久化并再次确认。
        appendReportActions(actions, input, device, combined);
        return actions;
    }

    [[nodiscard]] ProtocolAction parsedAction(const ProtocolInput& input,
                                              const DeviceDefinition& device,
                                              const ParsedFrame& frame) const {
        message::ParsedDeviceMessage message;
        message.acquisitionId = frame.acquisitionId;
        message.causationId = input.messageId;
        message.linkId = link_.id;
        message.deviceId = device.id;
        message.modelId = device.modelId;
        message.deviceCode = device.code;
        message.protocol = "SL651";
        message.connectionId = connectionId_;
        message.occurredAtMs = input.receivedAtMs;
        message.observedAtMs = detail::observationTimeMilliseconds(frame.body, frame.functionCode, device.timezone)
            .value_or(detail::reportTimeMilliseconds(frame.body, device.timezone).value_or(input.receivedAtMs));
        message.storagePolicy = device.storagePolicy;
        message.onlineWindowMs = std::clamp<std::int64_t>(device.onlineTimeout, 1, 86400) * 1000;
        message.source = "push";
        message.rawPayloads = frame.rawFrames;
        message.rawPacketIds = frame.rawPacketIds;
        message.valuesJson = valuesJson(device, frame);
        return {.kind = ProtocolActionKind::PublishParsed,
                .connectionId = connectionId_,
                .deviceId = device.id,
                .deviceCode = device.code,
                .parsed = std::move(message)};
    }

    [[nodiscard]] static std::string valuesJson(const DeviceDefinition& device,
                                                const ParsedFrame& frame) {
        const auto functionCode = detail::hexByte(frame.functionCode);
        const auto downFunction =
            std::any_of(device.elements.begin(), device.elements.end(), [&](const auto& element) {
                return element.functionCode == functionCode && element.direction == "DOWN";
            });
        const auto hasResponseElements =
            std::any_of(device.elements.begin(), device.elements.end(), [&](const auto& element) {
                return element.functionCode == functionCode && element.responseElement;
            });
        const auto useResponseElements = frame.upstream && downFunction && hasResponseElements;
        std::ostringstream json;
        json << "{\"function_code\":\"" << functionCode << "\",\"direction\":\""
             << (frame.upstream ? "UP" : "DOWN") << '"';
        if (frame.totalPackets > 0)
            json << ",\"is_multi_packet\":true,\"total_packets\":" << frame.totalPackets;
        json << ",\"values\":{";
        // 从流水号、发报时间之后逐项读取；未知数值要素也按数据定义跳过。
        // 配置只决定名称和解释方式，不能把载荷内部的字节当成引导符。
        std::map<std::string, std::pair<std::size_t, std::size_t>, std::less<>> fields;
        std::size_t guideStart = 8;
        for (const auto& element : device.elements) {
            if (element.functionCode == functionCode && element.responseElement == useResponseElements &&
                element.positionMode == "OFFSET" && element.byteOffset >= 0 && element.length > 0 &&
                static_cast<std::uint64_t>(element.byteOffset) <= frame.body.size() &&
                static_cast<std::uint64_t>(element.length) <= frame.body.size() - element.byteOffset)
                guideStart = std::max(guideStart, static_cast<std::size_t>(element.byteOffset + element.length));
        }
        for (std::size_t offset = guideStart; offset < frame.body.size();) {
            const auto lead = frame.body[offset];
            const std::size_t guideLength = lead == 0xFF ? 3 : 2;
            if (offset + guideLength > frame.body.size()) break;
            const auto guide = std::span<const std::uint8_t>(frame.body).subspan(offset, guideLength);
            std::size_t length = guide.back() >> 3U;
            if (lead >= 0xF0 && lead != 0xFF) {
                if (guide[1] != lead) break;
                if (lead == 0xF0) length = 5;
                else if (lead == 0xF1) length = 6;
                else if (lead == 0xF2 || lead == 0xF3) length = frame.body.size() - offset - guideLength;
                else {
                    const auto configured = std::find_if(device.elements.begin(), device.elements.end(),
                        [&](const auto& element) {
                            return element.functionCode == functionCode &&
                                element.responseElement == useResponseElements &&
                                detail::hexBytes(element.guideHex) == std::vector<std::uint8_t>(guide.begin(), guide.end());
                        });
                    if (configured == device.elements.end() || configured->length <= 0) break;
                    length = static_cast<std::size_t>(configured->length);
                }
            }
            if (length == 0 || length > frame.body.size() - offset - guideLength) break;
            fields.emplace(message::toHex(std::vector<std::uint8_t>(guide.begin(), guide.end())),
                           std::pair{offset + guideLength, length});
            offset += guideLength + length;
        }
        bool first = true;
        for (const auto& element : device.elements) {
            if (element.functionCode != functionCode ||
                element.responseElement != useResponseElements)
                continue;
            std::size_t valueOffset = 0;
            std::size_t length = 0;
            if (element.positionMode == "OFFSET") {
                if (element.byteOffset < 0 || element.length <= 0) continue;
                valueOffset = static_cast<std::size_t>(element.byteOffset);
                length = static_cast<std::size_t>(element.length);
                if (valueOffset > frame.body.size() || length > frame.body.size() - valueOffset) continue;
            } else {
                const auto guide = detail::hexBytes(element.guideHex);
                if (guide.empty()) continue;
                const auto field = fields.find(message::toHex(guide));
                if (field == fields.end()) continue;
                valueOffset = field->second.first;
                length = field->second.second;
                if (element.length < 0 || (element.length != 0 && static_cast<std::size_t>(element.length) != length)) continue;
                if (element.encoding == "BCD" && (element.digits < 0 || element.digits > 7 ||
                    element.digits != (guide.back() & 7U))) continue;
            }
            const auto value = detail::elementValue(
                std::span<const std::uint8_t>(frame.body).subspan(valueOffset, length), element);
            if (!first)
                json << ',';
            first = false;
            json << '\"' << service::utils::jsonEscape(element.id) << "\":{\"name\":\""
                 << service::utils::jsonEscape(element.name) << "\",\"value\":\""
                 << service::utils::jsonEscape(value) << "\",\"unit\":\""
                 << service::utils::jsonEscape(element.unit) << "\",\"type\":\""
                 << service::utils::jsonEscape(element.encoding) << "\"}";
        }
        json << "}}";
        return json.str();
    }

    static std::string normalizeCode(std::string_view code) {
        return code.size() >= 10 ? std::string(code.substr(code.size() - 10))
                                 : std::string(10 - code.size(), '0') + std::string(code);
    }

    static constexpr std::array<std::uint8_t, 2> kHeader{0x7E, 0x7E};
    static constexpr std::size_t kHeaderLength = 13;
    static constexpr std::size_t kMaximumBodyLength = 4095;
    static constexpr std::size_t kMaximumPackets = 4095;
    static constexpr std::size_t kMaximumAssemblyBytes = 8 * 1024 * 1024;
    static constexpr std::size_t kMaximumReceiveBuffer = 1024 * 1024;
    static constexpr auto kMultiPacketIdleTimeout = std::chrono::seconds(30);

    LinkDefinition link_;
    std::string connectionId_;
    std::shared_ptr<const RuntimeSnapshot> snapshot_;
    std::vector<const DeviceDefinition*> devices_;
    std::map<std::string, const DeviceDefinition*, std::less<>> devicesByCode_;
    std::set<std::string, std::less<>> boundDeviceIds_;
    std::vector<std::uint8_t> receiveBuffer_;
    std::uint64_t nextPacketSequence_ = 0;
    std::map<std::string, PendingCommand, std::less<>> pendingCommands_;
    std::map<std::string, MultiPacket, std::less<>> multiPackets_;
    std::uint64_t nextDeadlineToken_ = 1;
    std::uint64_t nextPublicationToken_ = 1;
    std::map<std::string, ParsedFrame, std::less<>> stationHeaders_;
    std::map<std::uint64_t, UnpublishedReport> unpublishedReports_;
    std::map<std::string, std::vector<std::string>, std::less<>> recentResponses_;
    std::map<std::string, std::vector<std::pair<std::string, std::string>>, std::less<>> reportAcquisitions_;
    std::map<std::string, std::map<std::string, std::string>, std::less<>> completedResponses_;
    std::map<std::string, std::set<std::string>, std::less<>> confirmedResponses_;
    std::set<std::string, std::less<>> ambiguousDevices_;
};

class SessionFactory final : public ProtocolSessionFactory {
  public:
    [[nodiscard]] const ProtocolDefinition& definition() const noexcept override {
        return kSl651Protocol;
    }

    [[nodiscard]] bool packetMatchesDevice(const DeviceDefinition& device, std::string_view direction,
        std::span<const std::uint8_t> bytes) const noexcept override {
        if (bytes.size() < 8 || bytes[0] != 0x7e || bytes[1] != 0x7e) return true;
        const auto offset = direction == "RX" ? 3U : 2U;
        std::uint64_t station = 0;
        for (std::size_t i = 0; i < 5; ++i)
            station = station * 100 + (bytes[offset + i] >> 4) * 10 + (bytes[offset + i] & 15);
        const auto nonzero = device.code.find_first_not_of('0');
        const auto normalized = nonzero == std::string::npos ? std::string_view("0")
            : std::string_view(device.code).substr(nonzero);
        // 不分配内存；完整帧和 BCD 合法性由会话解析验证。
        std::array<char, 20> address{};
        const auto result = std::to_chars(address.data(), address.data() + address.size(), station);
        return result.ec == std::errc{} && std::string_view(address.data(), result.ptr) == normalized;
    }

  protected:
    [[nodiscard]] std::unique_ptr<ProtocolSession>
    createDeviceSession(const LinkDefinition& link, std::string_view connectionId,
                  std::string_view,
                  const std::shared_ptr<const RuntimeSnapshot>& snapshot,
                  std::vector<const DeviceDefinition*> devices) const override {
        return std::make_unique<Session>(link, std::string(connectionId), snapshot,
                                         std::move(devices));
    }
};

} // namespace service::collector::sl651
