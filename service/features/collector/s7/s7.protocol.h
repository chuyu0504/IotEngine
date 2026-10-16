#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iomanip>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "service/features/collector/collector.protocol.h"
#include "service/features/collector/register_value/register_value.protocol.h"
#include "service/features/collector/scheduling/scheduling.protocol.h"
#include "service/utils/json.h"

namespace service::collector::s7 {

namespace detail {

inline std::uint16_t be16(std::span<const std::uint8_t> bytes, std::size_t offset) {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[offset]) << 8U) |
                                      bytes[offset + 1]);
}

inline std::uint32_t be24(std::span<const std::uint8_t> bytes, std::size_t offset) {
    return (static_cast<std::uint32_t>(bytes[offset]) << 16U) |
           (static_cast<std::uint32_t>(bytes[offset + 1]) << 8U) | bytes[offset + 2];
}

inline std::optional<std::uint16_t> parseHex16(std::string_view value) {
    if (value.starts_with("0x") || value.starts_with("0X"))
        value.remove_prefix(2);
    std::uint16_t result = 0;
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), result, 16);
    return error == std::errc{} && end == value.data() + value.size() ? std::optional(result)
                                                                      : std::nullopt;
}

inline std::uint16_t remoteTsap(const DeviceDefinition& device) {
    if (device.s7ConnectionMode == "TSAP")
        return parseHex16(device.s7RemoteTsap).value_or(0x0101);
    const std::uint16_t type = device.s7ConnectionType == "OP"         ? 2
                               : device.s7ConnectionType == "S7_BASIC" ? 3
                                                                       : 1;
    return static_cast<std::uint16_t>((type << 8U) + device.s7Rack * 0x20 + device.s7Slot);
}

inline std::uint16_t localTsap(const DeviceDefinition& device) {
    return device.s7ConnectionMode == "TSAP" ? parseHex16(device.s7LocalTsap).value_or(0x0100)
                                             : 0x0100;
}

inline std::uint32_t be32(std::span<const std::uint8_t> bytes) {
    return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
           (static_cast<std::uint32_t>(bytes[1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[2]) << 8U) | bytes[3];
}

inline std::uint64_t be64(std::span<const std::uint8_t> bytes) {
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

inline std::optional<std::string> decodeJson(std::span<const std::uint8_t> bytes,
                                             const ElementDefinition& element) {
    if (element.dataType == "BOOL") {
        if (bytes.empty())
            return std::nullopt;
        const auto bit = static_cast<unsigned>(std::clamp<std::int64_t>(element.startBit, 0, 7));
        return (bytes[0] & (1U << bit)) != 0 ? "1" : "0";
    }
    if ((element.dataType == "BYTE" || element.dataType == "UINT8") && !bytes.empty())
        return std::to_string(bytes[0]);
    if (element.dataType == "INT8" && !bytes.empty())
        return std::to_string(static_cast<std::int8_t>(bytes[0]));
    if ((element.dataType == "WORD" || element.dataType == "UINT16") && bytes.size() >= 2)
        return std::to_string(be16(bytes, 0));
    if (element.dataType == "INT16" && bytes.size() >= 2)
        return std::to_string(static_cast<std::int16_t>(be16(bytes, 0)));
    if ((element.dataType == "DWORD" || element.dataType == "UINT32") && bytes.size() >= 4)
        return std::to_string(be32(bytes));
    if (element.dataType == "INT32" && bytes.size() >= 4)
        return std::to_string(static_cast<std::int32_t>(be32(bytes)));
    if ((element.dataType == "REAL" || element.dataType == "FLOAT" ||
         element.dataType == "FLOAT32") &&
        bytes.size() >= 4)
        return decimalJson(std::bit_cast<float>(be32(bytes)), element);
    if (element.dataType == "LREAL" && bytes.size() >= 8)
        return decimalJson(std::bit_cast<double>(be64(bytes)), element);
    if (element.dataType == "STRING" && !bytes.empty()) {
        std::string value(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        if (const auto zero = value.find('\0'); zero != std::string::npos)
            value.resize(zero);
        return service::utils::jsonQuoted(value);
    }
    return std::nullopt;
}

inline std::uint8_t areaCode(std::string_view area) {
    return area == "DB" || area == "V" ? 0x84
           : area == "MK"              ? 0x83
           : area == "PA"              ? 0x82
           : area == "PE"              ? 0x81
           : area == "CT"              ? 0x1C
           : area == "TM"              ? 0x1D
                                       : 0;
}

inline std::uint16_t dbNumber(const ElementDefinition& element) {
    return static_cast<std::uint16_t>(
        std::clamp<std::int64_t>(element.area == "V" ? 1 : element.dbNumber, 0, 65535));
}

inline void appendBe(std::vector<std::uint8_t>& bytes, std::uint64_t value, std::size_t width) {
    for (auto offset = width; offset > 0; --offset)
        bytes.push_back(static_cast<std::uint8_t>(value >> ((offset - 1) * 8U)));
}

inline std::vector<std::uint8_t> encodeValue(const ElementDefinition& element,
                                             std::string_view value) {
    std::vector<std::uint8_t> bytes;
    if (element.dataType == "BOOL")
        bytes.push_back(value == "1" ? 1 : 0);
    else if (element.dataType == "INT8")
        bytes.push_back(
            static_cast<std::uint8_t>(command::integer<std::int64_t>(value, element.name)));
    else if (element.dataType == "UINT8" || element.dataType == "BYTE")
        bytes.push_back(
            static_cast<std::uint8_t>(command::integer<std::uint64_t>(value, element.name)));
    else if (element.dataType == "INT16")
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
    else if (element.dataType == "FLOAT" || element.dataType == "FLOAT32" ||
             element.dataType == "REAL")
        appendBe(bytes,
                 std::bit_cast<std::uint32_t>(
                     static_cast<float>(command::decimal(value, element.name))),
                 4);
    else if (element.dataType == "LREAL" || element.dataType == "DOUBLE")
        appendBe(bytes, std::bit_cast<std::uint64_t>(command::decimal(value, element.name)),
                 8);
    else if (element.dataType == "STRING") {
        bytes.assign(value.begin(), value.end());
        bytes.resize(static_cast<std::size_t>(element.size), 0);
    }
    if (bytes.empty())
        throw std::invalid_argument("command_invalid: unsupported S7 data type");
    return bytes;
}

} // namespace detail

class Session final : public ProtocolSession,
                      public CommandCapabilitySession,
                      public DeadlineCapabilitySession {
  public:
    // 使用业务请求中的地址与引用号解析响应，无须重放 COTP/Setup 握手。
    static std::string decodeRead(const DeviceDefinition& device,
                                  std::span<const std::uint8_t> requestWire,
                                  std::span<const std::uint8_t> responseWire) {
        const auto request = requestDescriptor(requestWire);
        if (!request || request->functionCode != kReadFunction || request->items.empty() ||
            detail::be16(requestWire, 2) != requestWire.size() ||
            requestWire.size() != 17U + detail::be16(requestWire, 13) ||
            detail::be16(requestWire, 15) != 0 ||
            !validS7Frame(responseWire) || responseWire.size() < 21 ||
            responseWire[8] != 3 || detail::be16(responseWire, 2) != responseWire.size() ||
            detail::be16(responseWire, 11) != request->pduReference ||
            responseWire[17] != 0 || responseWire[18] != 0 ||
            detail::be16(responseWire, 13) != 2 || responseWire[19] != kReadFunction ||
            responseWire[20] != request->items.size() ||
            responseWire.size() != 21U + detail::be16(responseWire, 15))
            throw std::invalid_argument("invalid S7 acquisition exchange");
        const auto payloads = readResponsePayloads(responseWire, 19, 2);
        if (!payloads || payloads->size() != request->items.size())
            throw std::invalid_argument("invalid S7 acquisition data");
        for (std::size_t index = 0; index < payloads->size(); ++index) {
            const auto& item = request->items[index];
            const auto width = item.wordLength == 1 ? (item.amount + 7U) / 8U
                : item.amount * ((item.wordLength == 0x1c || item.wordLength == 0x1d) ? 2U : 1U);
            if (item.amount == 0 || (item.wordLength != 1 && item.wordLength != 2 &&
                item.wordLength != 0x1c && item.wordLength != 0x1d) || (*payloads)[index].size() != width)
                throw std::invalid_argument("S7 acquisition length mismatch");
        }
        return valuesJson(device, *payloads, *request, true);
    }

    Session(LinkDefinition link, std::string connectionId, std::string targetId,
            std::shared_ptr<const RuntimeSnapshot> snapshot,
            std::vector<const DeviceDefinition*> devices)
        : link_(std::move(link)), connectionId_(std::move(connectionId)),
          targetId_(std::move(targetId)), snapshot_(std::move(snapshot)),
          devices_(std::move(devices)) {
        for (const auto* device : devices_) {
            devicesById_.emplace(device->id, device);
            devicesByCode_.emplace(device->code, device);
            queues_.try_emplace(device->id);
        }
        if (link_.mode == "TCP Client") {
            for (const auto* device : devices_)
                if (device->targetId == targetId_) {
                    device_ = device;
                    state_ = State::Idle;
                    break;
                }
        }
    }

    [[nodiscard]] std::vector<ProtocolAction> connected() override {
        if (!device_)
            return {};
        std::vector<ProtocolAction> actions;
        pollPending_ = !device_->elements.empty();
        appendHandshake(actions);
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> consume(const ProtocolInput& input) override {
        std::vector<ProtocolAction> actions;
        std::vector<std::uint8_t> bytes(input.bytes.begin(), input.bytes.end());
        if (link_.mode == "TCP Server") {
            const auto registration = matchRegistration(bytes);
            if (registration.conflict)
                return {{.kind = ProtocolActionKind::Close,
                         .connectionId = connectionId_,
                         .reason = "s7_registration_conflict"}};
            if (!device_ && registration.device) {
                device_ = registration.device;
                state_ = State::Idle;
                actions.push_back(bindAction(*device_));
                pollPending_ = !device_->elements.empty();
                appendHandshake(actions);
                bytes = std::move(registration.payload);
                if (bytes.empty())
                    return actions;
            } else if (device_ && registration.device) {
                if (registration.device != device_)
                    return {{.kind = ProtocolActionKind::Close,
                             .connectionId = connectionId_,
                             .reason = "s7_registration_conflict"}};
                bytes = std::move(registration.payload);
                if (bytes.empty())
                    return actions;
            } else if (isHeartbeat(bytes)) {
                return actions;
            }
        } else if (isHeartbeat(bytes)) {
            return actions;
        }
        if (!device_)
            return actions;

        receiveBuffer_.insert(receiveBuffer_.end(), bytes.begin(), bytes.end());
        if (receiveBuffer_.size() > kMaximumReceiveBuffer) {
            receiveBuffer_.clear();
            actions.push_back({.kind = ProtocolActionKind::Close,
                               .connectionId = connectionId_,
                               .reason = "s7_receive_buffer_overflow"});
            return actions;
        }
        for (auto& frame : extractFrames()) {
            auto next = consumeFrame(input, std::move(frame));
            if (!next.empty())
                next.front().responseSuccess = std::none_of(next.begin(), next.end(), [](const auto& action) {
                    return action.kind == ProtocolActionKind::FailCommand || action.kind == ProtocolActionKind::Close;
                });
            actions.insert(actions.end(), std::make_move_iterator(next.begin()),
                           std::make_move_iterator(next.end()));
        }
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> disconnected(std::string_view reason) override {
        std::vector<ProtocolAction> actions;
        pollCycle_.fail();
        pollCycle_.finish(actions);
        if (deadlineToken_ != 0)
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_,
                               .deadlineToken = deadlineToken_});
        if (pollDeadlineToken_ != 0)
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_,
                               .deadlineToken = pollDeadlineToken_});
        if (inflight_)
            actions.push_back(failAction(*inflight_, reason));
        if (discovery_) {
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_,
                               .deadlineToken = discovery_->deadlineToken});
            actions.push_back({.kind = ProtocolActionKind::FailCommand,
                               .connectionId = connectionId_,
                               .commandId = discovery_->commandId,
                               .reason = std::string(reason)});
            discovery_.reset();
        }
        for (auto& [deviceId, queue] : queues_) {
            (void)deviceId;
            appendFailures(queue.highWrites, reason, actions);
            appendFailures(queue.highReads, reason, actions);
            appendFailures(queue.normalWrites, reason, actions);
            appendFailures(queue.normalReads, reason, actions);
        }
        inflight_.reset();
        directProbeRequest_.reset();
        receiveBuffer_.clear();
        state_ = State::Closed;
        pollPending_ = false;
        deadlineToken_ = 0;
        pollDeadlineToken_ = 0;
        nextPduReference_ = 0;
        cotpClientReference_ = 0;
        cotpServerReference_ = 0;
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> execute(ProtocolCommand command) override {
        if (command.kind == "discovery") {
            if (link_.mode != "TCP Server")
                return {{.kind = ProtocolActionKind::FailCommand,
                         .connectionId = connectionId_,
                         .commandId = std::move(command.id),
                         .reason = "s7_discovery_requires_server"}};
            if (device_)
                return {{.kind = ProtocolActionKind::CompleteCommand,
                         .connectionId = connectionId_,
                         .deviceId = device_->id,
                         .deviceCode = device_->code,
                         .commandId = std::move(command.id),
                         .reason = "discovery_already_registered"}};
            if (discovery_)
                return {{.kind = ProtocolActionKind::FailCommand,
                         .connectionId = connectionId_,
                         .commandId = std::move(command.id),
                         .reason = "s7_discovery_busy"}};
            if (command.payload.empty())
                return {{.kind = ProtocolActionKind::FailCommand,
                         .connectionId = connectionId_,
                         .commandId = std::move(command.id),
                         .reason = "s7_discovery_payload_empty"}};
            const auto token = nextDeadlineToken_++;
            const auto timeout = std::clamp(command.timeout, std::chrono::milliseconds(100),
                                            std::chrono::milliseconds(60000));
            discovery_ = Discovery{command.id, token};
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
        const auto* device = findDevice(command);
        if (!device || device != device_)
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .deviceId = std::move(command.deviceId),
                     .deviceCode = std::move(command.deviceCode),
                     .commandId = std::move(command.id),
                     .reason = "s7_device_offline"}};
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
        const auto descriptor = requestDescriptor(command.payload);
        if (!descriptor)
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .deviceId = device->id,
                     .deviceCode = device->code,
                     .commandId = std::move(command.id),
                     .reason = "s7_command_frame_invalid"}};
        if (descriptor->functionCode == kWriteFunction && command.readbackPayload.empty())
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .deviceId = device->id,
                     .deviceCode = device->code,
                     .commandId = std::move(command.id),
                     .reason = "s7_write_readback_required"}};
        auto& queue = queues_.at(device->id);
        if (queue.size() >= kMaximumDeviceQueue)
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .deviceId = device->id,
                     .deviceCode = device->code,
                     .commandId = std::move(command.id),
                     .reason = "s7_device_queue_full"}};
        const auto isWrite = descriptor->functionCode == kWriteFunction || !command.elements.empty();
        auto& target = command.highPriority
                           ? (isWrite ? queue.highWrites : queue.highReads)
                           : (isWrite ? queue.normalWrites : queue.normalReads);
        target.push_back(std::move(command));
        std::vector<ProtocolAction> actions;
        appendNext(actions);
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> deadline(std::uint64_t token) override {
        if (discovery_ && token == discovery_->deadlineToken) {
            auto failed = std::move(*discovery_);
            discovery_.reset();
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .commandId = std::move(failed.commandId),
                     .reason = "s7_discovery_window_empty"}};
        }
        if (token != 0 && token == pollDeadlineToken_) {
            pollDeadlineToken_ = 0;
            std::vector<ProtocolAction> actions;
            pollPending_ = device_ && !device_->elements.empty();
            appendNext(actions);
            return actions;
        }
        if (token == 0 || token != deadlineToken_)
            return {};
        deadlineToken_ = 0;
        if (state_ == State::WaitCotp || state_ == State::WaitSetup) {
            if (device_ && probeModeEquals(*device_, "AUTO")) {
                std::vector<ProtocolAction> actions;
                appendDirectProbe(actions);
                return actions;
            }
            return failHandshake("s7_handshake_timeout");
        }
        if (state_ == State::WaitDirectProbe)
            return failHandshake("s7_direct_probe_timeout");
        if (!inflight_)
            return {};
        if (inflight_->phase == Phase::Write) {
            inflight_->writeAckMissing = true;
            return startReadback();
        }
        // A timed-out S7 request leaves the negotiated COTP/S7 session in an unknown state.
        // Complete through the common path so ISO-DR is sent and the next operation handshakes.
        auto failed = std::move(*inflight_);
        inflight_.reset();
        const auto failureReason = failed.writeAckMissing
            ? "write_ack_missing:s7_readback_timeout"
            : failed.phase == Phase::Readback ? "s7_readback_timeout" : "s7_response_timeout";
        std::vector<ProtocolAction> actions{failAction(failed, failureReason)};
        finishCompletedOperation(failed.command, actions);
        return actions;
    }

  private:
    static constexpr std::int64_t kDefaultRecvTimeoutMs = 15000;
    static constexpr std::int64_t kDefaultRetryDelayMs = 1000;
    static constexpr std::int64_t kDefaultPduRequestLength = 480;

    enum class State { AwaitRegistration, Idle, WaitCotp, WaitSetup, WaitDirectProbe, Ready, Closed };
    enum class Phase { Read, PrepareWrite, Write, Readback };

    struct DeviceQueues {
        std::deque<ProtocolCommand> highWrites;
        std::deque<ProtocolCommand> highReads;
        std::deque<ProtocolCommand> normalWrites;
        std::deque<ProtocolCommand> normalReads;

        [[nodiscard]] std::size_t size() const noexcept {
            return highWrites.size() + highReads.size() + normalWrites.size() + normalReads.size();
        }
    };

    struct ReadItem {
        std::uint8_t wordLength = 0;
        std::uint16_t amount = 0;
        std::uint16_t dbNumber = 0;
        std::uint8_t area = 0;
        std::uint32_t bitAddress = 0;
    };

    struct RequestDescriptor {
        std::uint16_t pduReference = 0;
        std::uint8_t functionCode = 0;
        std::vector<ReadItem> items;
    };

    struct Inflight {
        const DeviceDefinition* device = nullptr;
        ProtocolCommand command;
        RequestDescriptor request;
        Phase phase = Phase::Read;
        bool writeAckMissing = false;
        const ElementDefinition* boolElement = nullptr;
        bool desiredBool = false;
    };

    struct RegistrationMatch {
        const DeviceDefinition* device = nullptr;
        std::vector<std::uint8_t> payload;
        bool conflict = false;
    };

    struct Discovery {
        std::string commandId;
        std::uint64_t deadlineToken = 0;
    };

    [[nodiscard]] std::uint16_t nextReference() {
        return nextPduReference_++;
    }

    [[nodiscard]] static std::vector<std::uint8_t> requestHeader(std::uint16_t reference,
                                                                 std::uint16_t parameterLength,
                                                                 std::uint16_t dataLength) {
        const auto totalLength = static_cast<std::uint16_t>(17 + parameterLength + dataLength);
        return {0x03,
                0x00,
                static_cast<std::uint8_t>(totalLength >> 8U),
                static_cast<std::uint8_t>(totalLength),
                0x02,
                0xF0,
                0x80,
                0x32,
                0x01,
                0x00,
                0x00,
                static_cast<std::uint8_t>(reference >> 8U),
                static_cast<std::uint8_t>(reference),
                static_cast<std::uint8_t>(parameterLength >> 8U),
                static_cast<std::uint8_t>(parameterLength),
                static_cast<std::uint8_t>(dataLength >> 8U),
                static_cast<std::uint8_t>(dataLength)};
    }

    [[nodiscard]] static std::vector<std::uint8_t>
    itemParameter(const ElementDefinition& element, bool byteAccess = false) {
        const auto bitAccess = element.dataType == "BOOL" && !byteAccess;
        const auto wordLength = static_cast<std::uint8_t>(bitAccess ? 0x01 : 0x02);
        const auto amount = static_cast<std::uint16_t>(
            bitAccess ? 1 : std::clamp<std::int64_t>(element.size, 1, 65535));
        const auto area = detail::areaCode(element.area);
        if (area == 0)
            throw std::invalid_argument("command_invalid: unsupported S7 area");
        const auto bitAddress = static_cast<std::uint32_t>(std::clamp<std::int64_t>(
            bitAccess ? element.start * 8 + element.startBit : element.start * 8, 0, 0xFFFFFF));
        return {0x12,
                0x0A,
                0x10,
                wordLength,
                static_cast<std::uint8_t>(amount >> 8U),
                static_cast<std::uint8_t>(amount),
                static_cast<std::uint8_t>(detail::dbNumber(element) >> 8U),
                static_cast<std::uint8_t>(detail::dbNumber(element)),
                area,
                static_cast<std::uint8_t>(bitAddress >> 16U),
                static_cast<std::uint8_t>(bitAddress >> 8U),
                static_cast<std::uint8_t>(bitAddress)};
    }

    [[nodiscard]] std::vector<std::uint8_t> buildElementRead(const ElementDefinition& element,
                                                             bool byteAccess = false) {
        auto frame = requestHeader(0, 14, 0);
        frame.push_back(kReadFunction);
        frame.push_back(1);
        const auto item = itemParameter(element, byteAccess);
        frame.insert(frame.end(), item.begin(), item.end());
        return frame;
    }

    [[nodiscard]] std::vector<std::uint8_t> buildElementWrite(const ElementDefinition& element,
                                                              std::span<const std::uint8_t> value,
                                                              bool byteAccess = false) {
        const auto dataLength = static_cast<std::uint16_t>(4 + value.size());
        auto frame = requestHeader(0, 14, dataLength);
        frame.push_back(kWriteFunction);
        frame.push_back(1);
        const auto item = itemParameter(element, byteAccess);
        frame.insert(frame.end(), item.begin(), item.end());
        frame.push_back(0);
        frame.push_back(element.dataType == "BOOL" && !byteAccess ? 0x03 : 0x04);
        const auto encodedLength = static_cast<std::uint16_t>(
            element.dataType == "BOOL" && !byteAccess ? value.size() : value.size() * 8);
        frame.push_back(static_cast<std::uint8_t>(encodedLength >> 8U));
        frame.push_back(static_cast<std::uint8_t>(encodedLength));
        frame.insert(frame.end(), value.begin(), value.end());
        return frame;
    }

    void compileElementCommand(const DeviceDefinition& device, ProtocolCommand& command) {
        const auto resolved = command::resolve(device, command.elements);
        if (resolved.elements.size() != 1)
            throw std::invalid_argument(
                "command_invalid: one S7 task must contain exactly one element");
        const auto& element = *resolved.elements.front().definition;
        const auto& value = resolved.elements.front().value;
        if (element.dataType == "BOOL") {
            command.payload = buildElementRead(element, true);
            command.expectedValue = value;
            return;
        }
        auto encoded = detail::encodeValue(element, value);
        command.payload = buildElementWrite(element, encoded);
        command.readbackPayload = buildElementRead(element);
        command.expectedReadbackData = std::move(encoded);
        command.expectedValue = value;
    }

    static ProtocolAction bindAction(const DeviceDefinition& device) {
        return {.kind = ProtocolActionKind::BindDevice,
                .deviceId = device.id,
                .deviceCode = device.code};
    }

    [[nodiscard]] RegistrationMatch
    matchRegistration(const std::vector<std::uint8_t>& bytes) const {
        RegistrationMatch result;
        const DeviceDefinition* exact = nullptr;
        const DeviceDefinition* prefixed = nullptr;
        std::size_t prefixLength = 0;
        for (const auto* device : devices_) {
            if (device->registrationBytes.empty())
                continue;
            if (bytes == device->registrationBytes) {
                if (exact && exact->id != device->id) {
                    result.conflict = true;
                    return result;
                }
                exact = device;
                continue;
            }
            if (bytes.size() > device->registrationBytes.size() &&
                std::equal(device->registrationBytes.begin(), device->registrationBytes.end(),
                           bytes.begin())) {
                if (prefixed && device->registrationBytes.size() == prefixLength &&
                    prefixed->id != device->id) {
                    result.conflict = true;
                    return result;
                }
                if (!prefixed || device->registrationBytes.size() > prefixLength) {
                    prefixed = device;
                    prefixLength = device->registrationBytes.size();
                }
            }
        }
        if (exact) {
            result.device = exact;
            return result;
        }
        if (prefixed) {
            result.device = prefixed;
            result.payload.assign(bytes.begin() + static_cast<std::ptrdiff_t>(prefixLength),
                                  bytes.end());
        }
        return result;
    }

    [[nodiscard]] bool isHeartbeat(const std::vector<std::uint8_t>& bytes) const {
        if (device_)
            return !device_->heartbeatBytes.empty() && bytes == device_->heartbeatBytes;
        return std::any_of(devices_.begin(), devices_.end(), [&](const auto* device) {
            return !device->heartbeatBytes.empty() && bytes == device->heartbeatBytes;
        });
    }

    void appendHandshake(std::vector<ProtocolAction>& actions) {
        if (!device_ || state_ == State::WaitCotp || state_ == State::WaitSetup ||
            state_ == State::WaitDirectProbe || state_ == State::Ready || state_ == State::Closed)
            return;
        sessionAcquisitionId_ = pollCycle_.id(connectionId_);
        const auto& queue = queues_.at(device_->id);
        for (const auto* commands : {&queue.highWrites, &queue.highReads, &queue.normalWrites, &queue.normalReads}) {
            if (commands->empty()) continue;
            if (commands->front().kind != "poll") sessionAcquisitionId_ = commands->front().id;
            break;
        }
        nextPduReference_ = 0;
        negotiatedPduLength_ = kDefaultPduRequestLength;
        cotpClientReference_ = 0;
        cotpServerReference_ = 0;
        if (probeModeEquals(*device_, "COMPATIBLE") || probeModeEquals(*device_, "DIRECT")) {
            appendDirectProbe(actions);
            return;
        }
        state_ = State::WaitCotp;
        deadlineToken_ = nextDeadlineToken_++;
        actions.push_back({.kind = ProtocolActionKind::Send,
                           .connectionId = connectionId_,
                           .deviceId = device_->id,
                           .deviceCode = device_->code,
                           .bytes = buildCotpRequest(*device_), .acquisitionId = sessionAcquisitionId_});
        actions.push_back({.kind = ProtocolActionKind::ScheduleDeadline,
                           .connectionId = connectionId_,
                           .deadlineToken = deadlineToken_,
                           .deadlineAfter = std::chrono::milliseconds(std::clamp<std::int64_t>(
                               device_->s7HandshakeTimeoutMs, 1000, 30000))});
    }

    [[nodiscard]] static std::vector<std::uint8_t>
    buildCotpRequest(const DeviceDefinition& device) {
        const auto local = detail::localTsap(device);
        const auto remote = detail::remoteTsap(device);
        return {0x03,
                0x00,
                0x00,
                0x16,
                0x11,
                0xE0,
                0x00,
                0x00,
                0x00,
                0x01,
                0x00,
                0xC0,
                0x01,
                0x0A,
                0xC1,
                0x02,
                static_cast<std::uint8_t>(local >> 8U),
                static_cast<std::uint8_t>(local),
                0xC2,
                0x02,
                static_cast<std::uint8_t>(remote >> 8U),
                static_cast<std::uint8_t>(remote)};
    }

    [[nodiscard]] std::vector<std::uint8_t> buildSetupRequest() {
        const auto reference = nextReference();
        const auto requestedPdu = static_cast<std::uint16_t>(kDefaultPduRequestLength);
        return {0x03,
                0x00,
                0x00,
                0x19,
                0x02,
                0xF0,
                0x80,
                0x32,
                0x01,
                0x00,
                0x00,
                static_cast<std::uint8_t>(reference >> 8U),
                static_cast<std::uint8_t>(reference),
                0x00,
                0x08,
                0x00,
                0x00,
                0xF0,
                0x00,
                0x00,
                0x01,
                0x00,
                0x01,
                static_cast<std::uint8_t>(requestedPdu >> 8U),
                static_cast<std::uint8_t>(requestedPdu)};
    }

    void appendDirectProbe(std::vector<ProtocolAction>& actions) {
        if (!device_)
            return;
        auto commands = pollCommands(*device_);
        if (commands.empty()) {
            auto failed = failHandshake("s7_direct_probe_unavailable");
            actions.insert(actions.end(), std::make_move_iterator(failed.begin()),
                           std::make_move_iterator(failed.end()));
            return;
        }
        auto payload = std::move(commands.front().payload);
        if (payload.size() < 31) {
            auto failed = failHandshake("s7_direct_probe_unavailable");
            actions.insert(actions.end(), std::make_move_iterator(failed.begin()),
                           std::make_move_iterator(failed.end()));
            return;
        }
        payload.resize(31);
        payload[2] = 0x00;
        payload[3] = 0x1F;
        payload[13] = 0x00;
        payload[14] = 0x0E;
        payload[18] = 0x01;
        setPduReference(payload, nextReference());
        directProbeRequest_ = requestDescriptor(payload);
        if (!directProbeRequest_) {
            auto failed = failHandshake("s7_direct_probe_unavailable");
            actions.insert(actions.end(), std::make_move_iterator(failed.begin()),
                           std::make_move_iterator(failed.end()));
            return;
        }
        state_ = State::WaitDirectProbe;
        deadlineToken_ = nextDeadlineToken_++;
        actions.push_back({.kind = ProtocolActionKind::Send,
                           .connectionId = connectionId_,
                           .deviceId = device_->id,
                           .deviceCode = device_->code,
                           .bytes = std::move(payload), .acquisitionId = sessionAcquisitionId_});
        actions.push_back({.kind = ProtocolActionKind::ScheduleDeadline,
                           .connectionId = connectionId_,
                           .deadlineToken = deadlineToken_,
                           .deadlineAfter = std::chrono::milliseconds(std::clamp<std::int64_t>(
                               device_->s7DirectProbeTimeoutMs, 1000, 30000))});
    }

    [[nodiscard]] std::vector<std::vector<std::uint8_t>> extractFrames() {
        std::vector<std::vector<std::uint8_t>> frames;
        while (receiveBuffer_.size() >= 4) {
            if (receiveBuffer_[0] != 0x03 || receiveBuffer_[1] != 0x00) {
                receiveBuffer_.erase(receiveBuffer_.begin());
                continue;
            }
            const auto length = detail::be16(receiveBuffer_, 2);
            if (length < 7 || length > negotiatedPduLength_ + 64) {
                receiveBuffer_.erase(receiveBuffer_.begin());
                continue;
            }
            if (receiveBuffer_.size() < length)
                break;
            frames.emplace_back(receiveBuffer_.begin(),
                                receiveBuffer_.begin() + static_cast<std::ptrdiff_t>(length));
            receiveBuffer_.erase(receiveBuffer_.begin(),
                                 receiveBuffer_.begin() + static_cast<std::ptrdiff_t>(length));
        }
        return frames;
    }

    [[nodiscard]] std::vector<ProtocolAction> consumeFrame(const ProtocolInput& input,
                                                           std::vector<std::uint8_t> frame) {
        if (state_ == State::WaitCotp) {
            if (frame.size() < 11 || frame[5] != 0xD0)
                return failHandshake("s7_cotp_confirm_invalid");
            cotpClientReference_ = detail::be16(frame, 6);
            cotpServerReference_ = detail::be16(frame, 8);
            std::vector<ProtocolAction> actions;
            state_ = State::WaitSetup;
            actions.push_back({.kind = ProtocolActionKind::Send,
                               .connectionId = connectionId_,
                               .deviceId = device_->id,
                               .deviceCode = device_->code,
                               .bytes = buildSetupRequest(), .acquisitionId = sessionAcquisitionId_});
            return actions;
        }
        if (state_ == State::WaitSetup) {
            if (!validS7Frame(frame) || frame.size() < 27 || frame[17] != 0 ||
                frame[18] != 0 || frame[19] != 0xF0)
                return failHandshake("s7_setup_response_invalid");
            negotiatedPduLength_ =
                std::clamp<std::size_t>(detail::be16(frame, frame.size() - 2), 240, 4096);
            std::vector<ProtocolAction> actions{{.kind = ProtocolActionKind::CancelDeadline,
                                                 .connectionId = connectionId_,
                                                 .deadlineToken = deadlineToken_}};
            deadlineToken_ = 0;
            state_ = State::Ready;
            if (link_.mode == "TCP Client")
                actions.push_back(bindAction(*device_));
            if (discovery_) {
                actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                                   .connectionId = connectionId_,
                                   .deadlineToken = discovery_->deadlineToken});
                actions.push_back({.kind = ProtocolActionKind::CompleteCommand,
                                   .connectionId = connectionId_,
                                   .deviceId = device_->id,
                                   .deviceCode = device_->code,
                                   .commandId = discovery_->commandId,
                                   .reason = "discovery_device_ready"});
                discovery_.reset();
            }
            preparePendingPoll();
            appendNext(actions);
            return actions;
        }
        if (state_ == State::WaitDirectProbe) {
            if (!directProbeRequest_ || !validS7Frame(frame))
                return failHandshake("s7_direct_probe_response_invalid");
            const auto parameterLength = detail::be16(frame, 13);
            const auto parameterOffset = frame[8] == 0x03 ? 19U : 17U;
            if (detail::be16(frame, 11) != directProbeRequest_->pduReference ||
                parameterLength < 2 || parameterOffset + parameterLength > frame.size() ||
                frame[parameterOffset] != kReadFunction || frame[17] != 0 || frame[18] != 0 ||
                !readResponsePayloads(frame, parameterOffset, parameterLength))
                return failHandshake("s7_direct_probe_response_invalid");
            std::vector<ProtocolAction> actions{{.kind = ProtocolActionKind::CancelDeadline,
                                                 .connectionId = connectionId_,
                                                 .deadlineToken = deadlineToken_}};
            deadlineToken_ = 0;
            directProbeRequest_.reset();
            state_ = State::Ready;
            if (link_.mode == "TCP Client")
                actions.push_back(bindAction(*device_));
            preparePendingPoll();
            appendNext(actions);
            return actions;
        }
        if (state_ != State::Ready || !inflight_ || !validS7Frame(frame))
            return {};
        const auto pduReference = detail::be16(frame, 11);
        const auto parameterLength = detail::be16(frame, 13);
        const auto parameterOffset = frame[8] == 0x03 ? 19U : 17U;
        if (pduReference != inflight_->request.pduReference || parameterLength == 0 ||
            parameterOffset + parameterLength > frame.size() ||
            frame[parameterOffset] != inflight_->request.functionCode)
            return {};

        std::vector<ProtocolAction> actions{{.kind = ProtocolActionKind::CancelDeadline,
                                             .connectionId = connectionId_,
                                             .deadlineToken = deadlineToken_}};
        deadlineToken_ = 0;
        if (frame[17] != 0 || frame[18] != 0) {
            auto failed = std::move(*inflight_);
            inflight_.reset();
            actions.push_back(failAction(failed, "s7_protocol_error"));
            finishCompletedOperation(failed.command, actions);
            return actions;
        }
        if (inflight_->phase == Phase::Write) {
            auto readback = startReadback();
            actions.insert(actions.end(), std::make_move_iterator(readback.begin()),
                           std::make_move_iterator(readback.end()));
            return actions;
        }
        const auto payloads = readResponsePayloads(frame, parameterOffset, parameterLength);
        if (!payloads) {
            auto failed = std::move(*inflight_);
            inflight_.reset();
            actions.push_back(failAction(failed, "s7_read_response_invalid"));
            finishCompletedOperation(failed.command, actions);
            return actions;
        }
        if (inflight_->phase == Phase::PrepareWrite) {
            if (!inflight_->boolElement || payloads->size() != 1 || payloads->front().empty()) {
                auto failed = std::move(*inflight_);
                inflight_.reset();
                actions.push_back(failAction(failed, "s7_bool_prepare_read_invalid"));
                finishCompletedOperation(failed.command, actions);
                return actions;
            }
            auto value = payloads->front();
            const auto bit = static_cast<unsigned>(
                std::clamp<std::int64_t>(inflight_->boolElement->startBit, 0, 7));
            if (inflight_->desiredBool)
                value[0] = static_cast<std::uint8_t>(value[0] | (1U << bit));
            else
                value[0] = static_cast<std::uint8_t>(value[0] & ~(1U << bit));
            inflight_->command.readbackPayload = buildElementRead(*inflight_->boolElement, true);
            auto write = buildElementWrite(*inflight_->boolElement, value, true);
            setPduReference(write, nextReference());
            const auto descriptor = requestDescriptor(write);
            if (!descriptor || descriptor->functionCode != kWriteFunction) {
                auto failed = std::move(*inflight_);
                inflight_.reset();
                actions.push_back(failAction(failed, "s7_bool_write_frame_invalid"));
                finishCompletedOperation(failed.command, actions);
                return actions;
            }
            inflight_->command.payload = std::move(write);
            inflight_->request = *descriptor;
            inflight_->phase = Phase::Write;
            deadlineToken_ = nextDeadlineToken_++;
            actions.push_back({.kind = ProtocolActionKind::Send,
                               .connectionId = connectionId_,
                               .deviceId = inflight_->device->id,
                               .deviceCode = inflight_->device->code,
                               .commandId = inflight_->command.id,
                               .bytes = inflight_->command.payload, .acquisitionId = inflight_->command.kind == "poll" ? pollCycle_.id(connectionId_) : inflight_->command.id});
            actions.push_back({.kind = ProtocolActionKind::ScheduleDeadline,
                               .connectionId = connectionId_,
                               .commandId = inflight_->command.id,
                               .deadlineToken = deadlineToken_,
                               .deadlineAfter = inflight_->command.timeout});
            return actions;
        }
        auto parsed = parsedAction(input, *inflight_->device, frame, *payloads,
                                   inflight_->request, inflight_->command.id);
        if (inflight_->command.kind == "poll")
            pollCycle_.observe(std::move(parsed), actions);
        else
            actions.push_back(std::move(parsed));
        if (inflight_->phase == Phase::Readback && inflight_->boolElement) {
            const auto bit = static_cast<unsigned>(
                std::clamp<std::int64_t>(inflight_->boolElement->startBit, 0, 7));
            const auto valid = payloads->size() == 1 && !payloads->front().empty();
            const auto actual = valid && (payloads->front()[0] & (1U << bit)) != 0;
            if (!valid || actual != inflight_->desiredBool) {
                auto failed = std::move(*inflight_);
                inflight_.reset();
                actions.push_back(failAction(failed, failed.writeAckMissing
                    ? "write_ack_missing:s7_readback_mismatch" : "s7_readback_mismatch"));
                finishCompletedOperation(failed.command, actions);
                return actions;
            }
        } else if (inflight_->phase == Phase::Readback &&
                   !inflight_->command.expectedReadbackData.empty()) {
            std::vector<std::uint8_t> combined;
            for (const auto& payload : *payloads)
                combined.insert(combined.end(), payload.begin(), payload.end());
            if (combined != inflight_->command.expectedReadbackData) {
                auto failed = std::move(*inflight_);
                inflight_.reset();
                actions.push_back(failAction(failed, failed.writeAckMissing
                    ? "write_ack_missing:s7_readback_mismatch" : "s7_readback_mismatch"));
                finishCompletedOperation(failed.command, actions);
                return actions;
            }
        }
        auto completed = std::move(*inflight_);
        inflight_.reset();
        actions.push_back({.kind = completed.writeAckMissing ? ProtocolActionKind::FailCommand
                                                               : ProtocolActionKind::CompleteCommand,
                           .connectionId = connectionId_,
                           .deviceId = completed.device->id,
                           .deviceCode = completed.device->code,
                           .commandId = completed.command.id,
                           .reason = completed.writeAckMissing ? "write_ack_missing" : ""});
        finishCompletedOperation(completed.command, actions);
        return actions;
    }

    [[nodiscard]] static bool validS7Frame(std::span<const std::uint8_t> frame) noexcept {
        return frame.size() >= 19 && frame[0] == 0x03 && frame[1] == 0x00 && frame[4] == 0x02 &&
               frame[5] == 0xF0 && frame[6] == 0x80 && frame[7] == 0x32;
    }

    [[nodiscard]] static std::optional<RequestDescriptor>
    requestDescriptor(std::span<const std::uint8_t> frame) {
        if (!validS7Frame(frame) || frame[8] != 0x01 || frame.size() < 19)
            return std::nullopt;
        const auto parameterLength = detail::be16(frame, 13);
        if (parameterLength == 0 || 17 + parameterLength > frame.size())
            return std::nullopt;
        RequestDescriptor result;
        result.pduReference = detail::be16(frame, 11);
        result.functionCode = frame[17];
        if (result.functionCode != kReadFunction && result.functionCode != kWriteFunction)
            return std::nullopt;
        if (result.functionCode == kReadFunction) {
            const auto count = frame[18];
            if (parameterLength != static_cast<std::size_t>(2 + count * 12))
                return std::nullopt;
            for (std::size_t index = 0; index < count; ++index) {
                const auto offset = 19 + index * 12;
                if (frame[offset] != 0x12 || frame[offset + 1] != 0x0A || frame[offset + 2] != 0x10)
                    return std::nullopt;
                result.items.push_back({.wordLength = frame[offset + 3],
                                        .amount = detail::be16(frame, offset + 4),
                                        .dbNumber = detail::be16(frame, offset + 6),
                                        .area = frame[offset + 8],
                                        .bitAddress = detail::be24(frame, offset + 9)});
            }
        }
        return result;
    }

    static void setPduReference(std::vector<std::uint8_t>& frame, std::uint16_t reference) {
        if (frame.size() < 13)
            return;
        frame[11] = static_cast<std::uint8_t>(reference >> 8U);
        frame[12] = static_cast<std::uint8_t>(reference);
    }

    [[nodiscard]] bool hasQueuedCommands() const {
        return device_ && queues_.at(device_->id).size() != 0;
    }

    void preparePendingPoll() {
        if (!pollPending_ || !device_)
            return;
        pollPending_ = false;
        auto& queue = queues_.at(device_->id);
        if (!queue.normalReads.empty())
            return;
        for (auto& command : pollCommands(*device_)) {
            if (queue.size() >= kMaximumDeviceQueue)
                break;
            queue.normalReads.push_back(std::move(command));
        }
    }

    [[nodiscard]] std::optional<ProtocolCommand> popQueuedCommand() {
        if (!device_)
            return std::nullopt;
        auto& queue = queues_.at(device_->id);
        const auto pop = [](auto& commands) -> std::optional<ProtocolCommand> {
            if (commands.empty())
                return std::nullopt;
            auto command = std::move(commands.front());
            commands.pop_front();
            return command;
        };
        if (auto command = pop(queue.highWrites))
            return command;
        if (auto command = pop(queue.highReads))
            return command;
        if (auto command = pop(queue.normalWrites))
            return command;
        return pop(queue.normalReads);
    }

    void appendDisconnect(std::vector<ProtocolAction>& actions) {
        if (device_ && cotpClientReference_ != 0 && cotpServerReference_ != 0) {
            actions.push_back(
                {.kind = ProtocolActionKind::Send,
                 .connectionId = connectionId_,
                 .deviceId = device_->id,
                 .deviceCode = device_->code,
                 .bytes = {0x03,
                           0x00,
                           0x00,
                           0x0B,
                           0x06,
                           0x80,
                           static_cast<std::uint8_t>(cotpServerReference_ >> 8U),
                           static_cast<std::uint8_t>(cotpServerReference_),
                           static_cast<std::uint8_t>(cotpClientReference_ >> 8U),
                           static_cast<std::uint8_t>(cotpClientReference_),
                           0x00}, .acquisitionId = sessionAcquisitionId_});
        }
        state_ = State::Idle;
        nextPduReference_ = 0;
        cotpClientReference_ = 0;
        cotpServerReference_ = 0;
        directProbeRequest_.reset();
        if (device_)
            negotiatedPduLength_ = kDefaultPduRequestLength;
    }

    void finishCompletedOperation(const ProtocolCommand& completed,
                                  std::vector<ProtocolAction>& actions) {
        if (completed.kind == "poll" && device_) {
            if (std::any_of(actions.begin(), actions.end(), [&](const auto& action) {
                    return action.kind == ProtocolActionKind::FailCommand && action.commandId == completed.id;
                })) pollCycle_.fail();
            auto& queue = queues_.at(device_->id);
            const bool highPriorityWaiting = !queue.highWrites.empty() || !queue.highReads.empty();
            if (highPriorityWaiting) {
                if (std::any_of(queue.normalReads.begin(), queue.normalReads.end(),
                                [](const auto& command) { return command.kind == "poll"; }))
                    pollCycle_.fail();
                std::erase_if(queue.normalReads,
                              [](const auto& command) { return command.kind == "poll"; });
            } else if (!queue.normalReads.empty() && queue.normalReads.front().kind == "poll") {
                appendNext(actions);
                return;
            }
            pollCycle_.finish(actions);
            schedulePoll(actions);
        }
        appendDisconnect(actions);
        appendNext(actions);
    }

    [[nodiscard]] std::vector<ProtocolAction> failHandshake(std::string_view reason) {
        const auto expiredToken = deadlineToken_;
        state_ = State::Idle;
        deadlineToken_ = 0;
        directProbeRequest_.reset();
        nextPduReference_ = 0;
        cotpClientReference_ = 0;
        cotpServerReference_ = 0;
        std::vector<ProtocolAction> actions;
        if (!sessionAcquisitionId_.empty()) actions.push_back({.kind = ProtocolActionKind::FinishAcquisition,
            .reason = "failed", .acquisitionId = sessionAcquisitionId_});
        pollCycle_.fail();
        pollCycle_.finish(actions);
        if (expiredToken != 0)
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_,
                               .deadlineToken = expiredToken});
        if (pollPending_) {
            pollPending_ = false;
            schedulePoll(actions, std::chrono::milliseconds(kDefaultRetryDelayMs));
        } else if (auto command = popQueuedCommand()) {
            actions.push_back({.kind = ProtocolActionKind::FailCommand,
                               .connectionId = connectionId_,
                               .deviceId = command->deviceId,
                               .deviceCode = command->deviceCode,
                               .commandId = command->id,
                               .reason = std::string(reason)});
        }
        appendNext(actions);
        return actions;
    }

    void schedulePoll(
        std::vector<ProtocolAction>& actions,
        std::optional<std::chrono::milliseconds> delay = std::nullopt) {
        if (!device_ || device_->elements.empty() || pollDeadlineToken_ != 0)
            return;
        pollDeadlineToken_ = nextDeadlineToken_++;
        const auto interval = std::chrono::seconds(
            std::clamp<std::int64_t>(device_->readInterval, 1, 86400));
        const auto deadlineAfter =
            delay.value_or(staggeredPollDelay(device_->id, interval));
        actions.push_back({.kind = ProtocolActionKind::ScheduleDeadline,
                           .connectionId = connectionId_,
                           .deviceId = device_->id,
                           .deviceCode = device_->code,
                           .deadlineToken = pollDeadlineToken_,
                           .deadlineAfter = deadlineAfter});
    }

    [[nodiscard]] static bool probeModeEquals(const DeviceDefinition& device,
                                              std::string_view expected) {
        if (device.s7ProbeMode.size() != expected.size())
            return false;
        return std::equal(device.s7ProbeMode.begin(), device.s7ProbeMode.end(), expected.begin(),
                          [](char left, char right) {
                              return std::toupper(static_cast<unsigned char>(left)) ==
                                     std::toupper(static_cast<unsigned char>(right));
                          });
    }

    [[nodiscard]] std::vector<ProtocolCommand> pollCommands(const DeviceDefinition& device) {
        struct ReadSpec {
            std::uint8_t wordLength = 0;
            std::uint16_t amount = 0;
            std::uint16_t db = 0;
            std::uint8_t area = 0;
            std::int64_t start = 0;
            std::uint32_t bitAddress = 0;
            std::size_t responseBytes = 0;
        };
        std::vector<ReadSpec> candidates;
        candidates.reserve(device.elements.size());
        for (const auto& element : device.elements) {
            const auto area = detail::areaCode(element.area);
            if (area == 0)
                continue;
            const auto timerOrCounter = element.area == "TM" || element.area == "CT";
            const auto size =
                static_cast<std::size_t>(std::clamp<std::int64_t>(element.size, 1, 65535));
            const auto amount = static_cast<std::uint16_t>(
                timerOrCounter ? std::max<std::size_t>(1, (size + 1) / 2) : size);
            candidates.push_back(
                {.wordLength = static_cast<std::uint8_t>(element.area == "CT"   ? 0x1C
                                                        : element.area == "TM" ? 0x1D
                                                                               : 0x02),
                 .amount = amount,
                 .db = detail::dbNumber(element),
                 .area = area,
                 .start = element.start,
                 .bitAddress = static_cast<std::uint32_t>(std::clamp<std::int64_t>(
                     timerOrCounter ? element.start : element.start * 8, 0, 0xFFFFFF)),
                 .responseBytes = static_cast<std::size_t>(amount) * (timerOrCounter ? 2U : 1U)});
        }
        std::sort(candidates.begin(), candidates.end(), [](const auto& left, const auto& right) {
            if (left.area != right.area)
                return left.area < right.area;
            if (left.db != right.db)
                return left.db < right.db;
            if (left.wordLength != right.wordLength)
                return left.wordLength < right.wordLength;
            if (left.start != right.start)
                return left.start < right.start;
            return left.amount < right.amount;
        });
        std::vector<ReadSpec> specs;
        for (const auto& candidate : candidates) {
            if (!specs.empty()) {
                auto& current = specs.back();
                const auto currentEnd = current.start + current.amount;
                const auto candidateEnd = candidate.start + candidate.amount;
                if (candidate.area == current.area && candidate.db == current.db &&
                    candidate.wordLength == current.wordLength && candidate.start <= currentEnd) {
                    if (candidateEnd > currentEnd) {
                        current.amount = static_cast<std::uint16_t>(candidateEnd - current.start);
                        current.responseBytes = static_cast<std::size_t>(current.amount) *
                                                (current.wordLength == 0x1C || current.wordLength == 0x1D
                                                     ? 2U
                                                     : 1U);
                    }
                    continue;
                }
            }
            specs.push_back(candidate);
        }

        std::vector<ProtocolCommand> commands;
        std::vector<ReadSpec> batch;
        std::size_t estimatedResponse = 21;
        const auto flush = [&]() {
            if (batch.empty())
                return;
            const auto parameterLength = static_cast<std::uint16_t>(2 + batch.size() * 12);
            const auto totalLength = static_cast<std::uint16_t>(17 + parameterLength);
            std::vector<std::uint8_t> payload{0x03,
                                              0x00,
                                              static_cast<std::uint8_t>(totalLength >> 8U),
                                              static_cast<std::uint8_t>(totalLength),
                                              0x02,
                                              0xF0,
                                              0x80,
                                              0x32,
                                              0x01,
                                              0x00,
                                              0x00,
                                               0x00,
                                               0x00,
                                              static_cast<std::uint8_t>(parameterLength >> 8U),
                                              static_cast<std::uint8_t>(parameterLength),
                                              0x00,
                                              0x00,
                                              kReadFunction,
                                              static_cast<std::uint8_t>(batch.size())};
            for (const auto& spec : batch) {
                payload.insert(payload.end(), {0x12, 0x0A, 0x10, spec.wordLength,
                                               static_cast<std::uint8_t>(spec.amount >> 8U),
                                               static_cast<std::uint8_t>(spec.amount),
                                               static_cast<std::uint8_t>(spec.db >> 8U),
                                               static_cast<std::uint8_t>(spec.db), spec.area,
                                               static_cast<std::uint8_t>(spec.bitAddress >> 16U),
                                               static_cast<std::uint8_t>(spec.bitAddress >> 8U),
                                               static_cast<std::uint8_t>(spec.bitAddress)});
            }
            commands.push_back({.deviceId = device.id,
                                .deviceCode = device.code,
                                 .kind = "poll",
                                 .payload = std::move(payload),
                                 .highPriority = false,
                                 .timeout = std::chrono::milliseconds(kDefaultRecvTimeoutMs)});
            batch.clear();
            estimatedResponse = 21;
        };
        for (const auto& spec : specs) {
            const auto nextRequest = 19 + (batch.size() + 1) * 12;
            const auto padded = spec.responseBytes + (spec.responseBytes & 1U);
            const auto nextResponse = estimatedResponse + 4 + padded;
            if (!batch.empty() && (nextRequest > negotiatedPduLength_ + 7 ||
                                   nextResponse > negotiatedPduLength_ + 7 || batch.size() >= 20))
                flush();
            batch.push_back(spec);
            estimatedResponse += 4 + padded;
        }
        flush();
        return commands;
    }

    [[nodiscard]] static std::optional<std::vector<std::vector<std::uint8_t>>>
    readResponsePayloads(std::span<const std::uint8_t> frame, std::size_t parameterOffset,
                         std::size_t parameterLength) {
        if (frame[parameterOffset] != kReadFunction || parameterLength < 2)
            return std::nullopt;
        const auto count = frame[parameterOffset + 1];
        std::size_t offset = parameterOffset + parameterLength;
        std::vector<std::vector<std::uint8_t>> result;
        result.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            if (offset + 4 > frame.size())
                return std::nullopt;
            if (frame[offset] != 0xFF)
                return std::nullopt;
            const auto transportSize = frame[offset + 1];
            const auto encodedLength = detail::be16(frame, offset + 2);
            const auto byteLength =
                transportSize == 0x09 || transportSize == 0x07 || transportSize == 0x03
                    ? static_cast<std::size_t>(encodedLength)
                    : static_cast<std::size_t>(encodedLength / 8U);
            offset += 4;
            if (offset + byteLength > frame.size())
                return std::nullopt;
            result.emplace_back(frame.begin() + static_cast<std::ptrdiff_t>(offset),
                                frame.begin() + static_cast<std::ptrdiff_t>(offset + byteLength));
            offset += byteLength;
            if ((byteLength & 1U) != 0 && index + 1 < count)
                ++offset;
        }
        return result;
    }

    [[nodiscard]] std::vector<ProtocolAction> dispatchNext() {
        if (state_ != State::Ready || inflight_ || !device_)
            return {};
        auto next = popQueuedCommand();
        if (!next)
            return {};
        auto command = std::move(*next);
        setPduReference(command.payload, nextReference());
        const auto descriptor = requestDescriptor(command.payload);
        if (!descriptor)
            return {};
        const auto timeout = std::chrono::milliseconds(kDefaultRecvTimeoutMs);
        command.timeout = timeout;
        const ElementDefinition* boolElement = nullptr;
        if (command.elements.size() == 1) {
            const auto current = std::find_if(
                device_->elements.begin(), device_->elements.end(), [&](const auto& element) {
                    return element.id == command.elements.front().elementId &&
                           element.dataType == "BOOL";
                });
            if (current != device_->elements.end())
                boolElement = &*current;
        }
        const auto phase = boolElement ? Phase::PrepareWrite
                                      : descriptor->functionCode == kWriteFunction ? Phase::Write
                                                                                   : Phase::Read;
        const auto desiredBool = boolElement && command.expectedValue == "1";
        inflight_ = Inflight{device_,       std::move(command), *descriptor, phase,
                             false,         boolElement,        desiredBool};
        deadlineToken_ = nextDeadlineToken_++;
        return {{.kind = ProtocolActionKind::Send,
                 .connectionId = connectionId_,
                 .deviceId = device_->id,
                 .deviceCode = device_->code,
                 .commandId = inflight_->command.id,
                 .bytes = inflight_->command.payload, .acquisitionId = inflight_->command.kind == "poll" ? pollCycle_.id(connectionId_) : inflight_->command.id},
                {.kind = ProtocolActionKind::ScheduleDeadline,
                 .connectionId = connectionId_,
                 .commandId = inflight_->command.id,
                 .deadlineToken = deadlineToken_,
                 .deadlineAfter = timeout}};
    }

    [[nodiscard]] std::vector<ProtocolAction> startReadback() {
        if (!inflight_)
            return {};
        setPduReference(inflight_->command.readbackPayload, nextReference());
        const auto descriptor = requestDescriptor(inflight_->command.readbackPayload);
        if (!descriptor || descriptor->functionCode != kReadFunction) {
            auto failed = std::move(*inflight_);
            inflight_.reset();
            std::vector<ProtocolAction> actions{failAction(failed, "s7_readback_frame_invalid")};
            finishCompletedOperation(failed.command, actions);
            return actions;
        }
        inflight_->request = *descriptor;
        inflight_->phase = Phase::Readback;
        deadlineToken_ = nextDeadlineToken_++;
        return {{.kind = ProtocolActionKind::Send,
                 .connectionId = connectionId_,
                 .deviceId = inflight_->device->id,
                 .deviceCode = inflight_->device->code,
                 .commandId = inflight_->command.id,
                 .bytes = inflight_->command.readbackPayload, .acquisitionId = inflight_->command.kind == "poll" ? pollCycle_.id(connectionId_) : inflight_->command.id},
                {.kind = ProtocolActionKind::ScheduleDeadline,
                 .connectionId = connectionId_,
                 .commandId = inflight_->command.id,
                 .deadlineToken = deadlineToken_,
                 .deadlineAfter = inflight_->command.timeout}};
    }

    void appendNext(std::vector<ProtocolAction>& actions) {
        if (state_ == State::Ready)
            preparePendingPoll();
        auto next = dispatchNext();
        actions.insert(actions.end(), std::make_move_iterator(next.begin()),
                       std::make_move_iterator(next.end()));
        if (state_ == State::Idle && (pollPending_ || hasQueuedCommands()))
            appendHandshake(actions);
    }

    [[nodiscard]] static ProtocolAction failAction(const Inflight& inflight,
                                                   std::string_view reason) {
        return {.kind = ProtocolActionKind::FailCommand,
                .deviceId = inflight.device->id,
                .deviceCode = inflight.device->code,
                .commandId = inflight.command.id,
                .reason = std::string(reason)};
    }

    static void appendFailures(std::deque<ProtocolCommand>& queue, std::string_view reason,
                               std::vector<ProtocolAction>& actions) {
        while (!queue.empty()) {
            auto command = std::move(queue.front());
            queue.pop_front();
            actions.push_back({.kind = ProtocolActionKind::FailCommand,
                               .deviceId = std::move(command.deviceId),
                               .deviceCode = std::move(command.deviceCode),
                               .commandId = std::move(command.id),
                               .reason = std::string(reason)});
        }
    }

    [[nodiscard]] const DeviceDefinition*
    findDevice(const ProtocolCommand& command) const noexcept {
        if (!command.deviceId.empty()) {
            const auto current = devicesById_.find(command.deviceId);
            if (current != devicesById_.end())
                return current->second;
        }
        const auto current = devicesByCode_.find(command.deviceCode);
        return current == devicesByCode_.end() ? nullptr : current->second;
    }

    [[nodiscard]] ProtocolAction
    parsedAction(const ProtocolInput& input, const DeviceDefinition& device,
                 const std::vector<std::uint8_t>& frame,
                 const std::vector<std::vector<std::uint8_t>>& payloads,
                 const RequestDescriptor& request, std::string_view causationId) const {
        message::ParsedDeviceMessage message;
        message.causationId =
            causationId.empty() ? std::string(input.messageId) : std::string(causationId);
        message.linkId = link_.id;
        message.deviceId = device.id;
        message.modelId = device.modelId;
        message.deviceCode = device.code;
        message.protocol = "S7";
        message.connectionId = connectionId_;
        message.occurredAtMs = input.receivedAtMs;
        message.observedAtMs = input.receivedAtMs;
        message.storagePolicy = device.storagePolicy;
        message.onlineWindowMs =
            std::clamp<std::int64_t>(device.onlineTimeout, 1, 86400) * 1000;
        message.source = "query";
        message.rawPayloads = {frame};
        message.rawPacketIds = {std::string(input.messageId) + ":frame:" + std::to_string(nextPacketSequence_++)};
        message.valuesJson = valuesJson(device, payloads, request);
        return {.kind = ProtocolActionKind::PublishParsed,
                .connectionId = connectionId_,
                .deviceId = device.id,
                .deviceCode = device.code,
                .parsed = std::move(message)};
    }

    [[nodiscard]] static std::string
    valuesJson(const DeviceDefinition& device,
               const std::vector<std::vector<std::uint8_t>>& payloads,
               const RequestDescriptor& request, bool edgeValues = false) {
        std::ostringstream json;
        json << "{\"function_code\":\"READ_VAR\",\"values\":{";
        bool first = true;
        for (std::size_t itemIndex = 0;
             itemIndex < request.items.size() && itemIndex < payloads.size(); ++itemIndex) {
            const auto& item = request.items[itemIndex];
            const auto& payload = payloads[itemIndex];
            for (const auto& element : device.elements) {
                const auto timerOrCounter = element.area == "TM" || element.area == "CT";
                const bool bitAccess = item.wordLength == 1;
                const auto elementAddress =
                    static_cast<std::uint32_t>(timerOrCounter ? element.start : element.start * 8 +
                        (bitAccess ? element.startBit : 0));
                if (!matchesArea(element, item) || elementAddress < item.bitAddress)
                    continue;
                if (bitAccess && (element.dataType != "BOOL" || elementAddress - item.bitAddress >= item.amount))
                    continue;
                const auto byteOffset =
                    timerOrCounter ? static_cast<std::size_t>(elementAddress - item.bitAddress) * 2
                                   : static_cast<std::size_t>(elementAddress - item.bitAddress) / 8;
                const auto width = bitAccess ? 1U :
                    static_cast<std::size_t>(std::max<std::int64_t>(1, element.size));
                if (byteOffset + width > payload.size())
                    continue;
                auto interpretation = element;
                if (bitAccess) interpretation.startBit = (elementAddress - item.bitAddress) % 8;
                const auto bytes = std::span<const std::uint8_t>(payload).subspan(byteOffset, width);
                auto value = detail::decodeJson(bytes, interpretation);
                if (edgeValues && width >= 2 && interpretation.dataType != "BOOL" &&
                    interpretation.dataType != "STRING" && interpretation.dataType != "BYTE" &&
                    interpretation.dataType != "UINT8" && interpretation.dataType != "INT8") {
                    if (interpretation.dataType == "REAL") interpretation.dataType = "FLOAT32";
                    if (interpretation.dataType == "LREAL") interpretation.dataType = "DOUBLE";
                    interpretation.byteOrder = "BIG_ENDIAN";
                    // 固件的 64 位整数保持原始精度，其余数值应用测点倍率与小数位。
                    if (interpretation.dataType == "INT64" || interpretation.dataType == "UINT64") {
                        interpretation.decimals = -1;
                    }
                    value = register_value::numericJson(bytes, interpretation);
                }
                if (!value)
                    continue;
                if (!first)
                    json << ',';
                first = false;
                json << '\"' << service::utils::jsonEscape(element.id) << "\":{\"name\":\""
                     << service::utils::jsonEscape(element.name) << "\",\"value\":" << *value
                     << ",\"unit\":\"" << service::utils::jsonEscape(element.unit) << "\"}";
            }
        }
        json << "}}";
        return json.str();
    }

    [[nodiscard]] static bool matchesArea(const ElementDefinition& element,
                                          const ReadItem& item) noexcept {
        const auto expectedArea = detail::areaCode(element.area);
        return expectedArea == item.area &&
               (item.area != 0x84 || detail::dbNumber(element) == item.dbNumber);
    }

    static constexpr std::uint8_t kReadFunction = 0x04;
    static constexpr std::uint8_t kWriteFunction = 0x05;
    static constexpr std::size_t kMaximumReceiveBuffer = 256 * 1024;
    static constexpr std::size_t kMaximumDeviceQueue = 256;

    LinkDefinition link_;
    std::string connectionId_;
    std::string targetId_;
    std::shared_ptr<const RuntimeSnapshot> snapshot_;
    std::vector<const DeviceDefinition*> devices_;
    std::map<std::string, const DeviceDefinition*, std::less<>> devicesById_;
    std::map<std::string, const DeviceDefinition*, std::less<>> devicesByCode_;
    std::map<std::string, DeviceQueues, std::less<>> queues_;
    AcquisitionCycle pollCycle_;
    std::string sessionAcquisitionId_;
    mutable std::uint64_t nextPacketSequence_ = 0;
    const DeviceDefinition* device_ = nullptr;
    State state_ = State::AwaitRegistration;
    std::vector<std::uint8_t> receiveBuffer_;
    std::optional<Inflight> inflight_;
    std::optional<Discovery> discovery_;
    std::optional<RequestDescriptor> directProbeRequest_;
    std::size_t negotiatedPduLength_ = 480;
    std::uint16_t nextPduReference_ = 0;
    std::uint16_t cotpClientReference_ = 0;
    std::uint16_t cotpServerReference_ = 0;
    bool pollPending_ = false;
    std::uint64_t deadlineToken_ = 0;
    std::uint64_t pollDeadlineToken_ = 0;
    std::uint64_t nextDeadlineToken_ = 1;
};

class SessionFactory final : public ProtocolSessionFactory {
  public:
    [[nodiscard]] const ProtocolDefinition& definition() const noexcept override {
        return kS7Protocol;
    }

  protected:
    [[nodiscard]] std::unique_ptr<ProtocolSession>
    createDeviceSession(const LinkDefinition& link, std::string_view connectionId,
                  std::string_view targetId,
                  const std::shared_ptr<const RuntimeSnapshot>& snapshot,
                  std::vector<const DeviceDefinition*> devices) const override {
        return std::make_unique<Session>(link, std::string(connectionId), std::string(targetId),
                                         snapshot, std::move(devices));
    }
};

} // namespace service::collector::s7
