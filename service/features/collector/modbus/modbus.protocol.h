#pragma once

#include <algorithm>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
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
#include "service/features/collector/register_value/register_value.protocol.h"
#include "service/features/collector/scheduling/scheduling.protocol.h"
#include "service/utils/json.h"

namespace service::collector::modbus {

namespace detail {

inline std::uint16_t readBe16(std::span<const std::uint8_t> bytes, std::size_t offset) {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[offset]) << 8U) |
                                      bytes[offset + 1]);
}

inline std::uint16_t crc16(std::span<const std::uint8_t> bytes) noexcept {
    std::uint16_t crc = 0xFFFFU;
    for (const auto byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc & 1U) != 0 ? static_cast<std::uint16_t>((crc >> 1U) ^ 0xA001U)
                                  : static_cast<std::uint16_t>(crc >> 1U);
    }
    return crc;
}

inline bool validRtu(std::span<const std::uint8_t> frame) noexcept {
    if (frame.size() < 4)
        return false;
    const auto expected = crc16(frame.first(frame.size() - 2));
    return frame[frame.size() - 2] == static_cast<std::uint8_t>(expected & 0xFFU) &&
           frame.back() == static_cast<std::uint8_t>(expected >> 8U);
}

} // namespace detail

class Session final : public ProtocolSession,
                      public CommandCapabilitySession,
                      public DeadlineCapabilitySession {
  public:
    // 解码已完成的业务交换，不创建连接、调度或握手状态。
    static std::string decodeRead(const DeviceDefinition& device,
                                  std::span<const std::uint8_t> requestWire,
                                  std::span<const std::uint8_t> responseWire) {
        const bool tcp = device.modbusMode == "TCP";
        const auto request = requestDescriptor(requestWire, tcp);
        if (!request || requestWire.size() != (tcp ? 12U : 8U) ||
            request->unitId != device.slaveId || request->functionCode < 1 ||
            request->functionCode > 4 || request->quantity == 0 ||
            request->quantity > (request->functionCode <= 2 ? 2000 : 125) ||
            static_cast<std::uint32_t>(request->startAddress) + request->quantity > 65536)
            throw std::invalid_argument("invalid Modbus acquisition request");
        const auto offset = tcp ? 6U : 0U;
        const auto count = request->functionCode <= 2
            ? (request->quantity + 7U) / 8U : request->quantity * 2U;
        if (responseWire.size() != offset + 3U + count + (tcp ? 0U : 2U) ||
            responseWire[offset] != request->unitId ||
            responseWire[offset + 1] != request->functionCode ||
            responseWire[offset + 2] != count ||
            (tcp && (responseWire[2] != 0 || responseWire[3] != 0 ||
                detail::readBe16(responseWire, 0) != request->transactionId ||
                detail::readBe16(responseWire, 4) != responseWire.size() - 6)) ||
            (!tcp && !detail::validRtu(responseWire)))
            throw std::invalid_argument("invalid Modbus acquisition response");
        ResponseFrame frame;
        frame.functionCode = request->functionCode;
        frame.data.assign(responseWire.begin() + offset + 2,
                          responseWire.end() - (tcp ? 0 : 2));
        return valuesJson(device, frame, *request);
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
        if (link_.mode == "TCP Client")
            bindClientTarget();
    }

    void inheritTransportState(const ProtocolSession& previous) override {
        const auto* session = dynamic_cast<const Session*>(&previous);
        if (session)
            nextTransactionId_ = session->nextTransactionId_;
    }

    [[nodiscard]] std::vector<ProtocolAction> connected() override {
        std::vector<ProtocolAction> actions;
        for (const auto* device : boundDevices_) {
            actions.push_back(bindAction(*device));
            enqueuePollNow(*device);
        }
        appendNext(actions);
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
                         .reason = "modbus_registration_conflict"}};
            if (registration.device) {
                if (!boundDevices_.empty()) {
                    if (boundDevices_.front()->registrationBytes !=
                        registration.device->registrationBytes)
                        return {{.kind = ProtocolActionKind::Close,
                                 .connectionId = connectionId_,
                                 .reason = "modbus_registration_conflict"}};
                } else {
                    bindRegistration(*registration.device);
                    for (const auto* device : boundDevices_) {
                        actions.push_back(bindAction(*device));
                        enqueuePollNow(*device);
                    }
                    appendNext(actions);
                }
                bytes = std::move(registration.payload);
                if (bytes.empty())
                    return actions;
            } else if (isHeartbeat(bytes)) {
                return actions;
            }
        } else if (isHeartbeat(bytes)) {
            return actions;
        }

        receiveBuffer_.insert(receiveBuffer_.end(), bytes.begin(), bytes.end());
        if (receiveBuffer_.size() > kMaximumReceiveBuffer) {
            receiveBuffer_.clear();
            actions.push_back({.kind = ProtocolActionKind::Close,
                               .connectionId = connectionId_,
                               .reason = "modbus_receive_buffer_overflow"});
            return actions;
        }

        for (auto& frame : extractFrames(expectedFrameMode())) {
            if (discovery_ && matches(discovery_->request, frame)) {
                const auto* discovered = identifyByUnit(frame.unitId, frame.tcp);
                if (discovered) {
                    discovery_->found = true;
                    if (std::find(boundDevices_.begin(), boundDevices_.end(), discovered) ==
                        boundDevices_.end()) {
                        bindRegistration(*discovered);
                        for (const auto* bound : boundDevices_) {
                            actions.push_back(bindAction(*bound));
                            enqueuePollNow(*bound);
                        }
                        appendNext(actions);
                    }
                    actions.push_back(parsedAction(input, *discovered, frame, discovery_->request,
                                                   discovery_->commandId));
                }
                continue;
            }
            if (boundDevices_.empty()) {
                const auto* identified = identifyByUnit(frame.unitId, frame.tcp);
                if (!identified)
                    continue;
                bindRegistration(*identified);
                for (const auto* device : boundDevices_) {
                    actions.push_back(bindAction(*device));
                    enqueuePollNow(*device);
                }
                appendNext(actions);
            }
            const auto* device = boundByUnit(frame.unitId, frame.tcp);
            if (!device)
                continue;
            auto frameActions = consumeResponse(input, *device, std::move(frame));
            if (!frameActions.empty())
                frameActions.front().responseSuccess = std::none_of(frameActions.begin(), frameActions.end(), [](const auto& action) {
                    return action.kind == ProtocolActionKind::FailCommand || action.kind == ProtocolActionKind::Close;
                });
            actions.insert(actions.end(), std::make_move_iterator(frameActions.begin()),
                           std::make_move_iterator(frameActions.end()));
        }
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> disconnected(std::string_view reason) override {
        std::vector<ProtocolAction> actions;
        for (auto& [id, cycle] : pollCycles_) {
            cycle.fail();
            cycle.finish(actions);
        }
        pollCycles_.clear();
        if (inflight_) {
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_,
                               .deadlineToken = inflight_->deadlineToken});
            actions.push_back({.kind = ProtocolActionKind::FailCommand,
                               .connectionId = connectionId_,
                               .deviceId = inflight_->device->id,
                               .deviceCode = inflight_->device->code,
                               .commandId = inflight_->command.id,
                               .reason = std::string(reason)});
        }
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
            appendQueueFailures(queue.highWrites, reason, actions);
            appendQueueFailures(queue.highReads, reason, actions);
            appendQueueFailures(queue.normalWrites, reason, actions);
            appendQueueFailures(queue.normalReads, reason, actions);
        }
        for (const auto& [token, device] : pollDeadlines_) {
            (void)device;
            actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                               .connectionId = connectionId_,
                               .deadlineToken = token});
        }
        pollDeadlines_.clear();
        pollDevices_.clear();
        inflight_.reset();
        receiveBuffer_.clear();
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> execute(ProtocolCommand command) override {
        if (command.kind == "discovery") {
            if (link_.mode != "TCP Server")
                return {{.kind = ProtocolActionKind::FailCommand,
                         .connectionId = connectionId_,
                         .commandId = std::move(command.id),
                         .reason = "modbus_discovery_requires_server"}};
            if (discovery_)
                return {{.kind = ProtocolActionKind::FailCommand,
                         .connectionId = connectionId_,
                         .commandId = std::move(command.id),
                         .reason = "modbus_discovery_busy"}};
            if (command.transport != "TCP" && command.transport != "RTU")
                return {{.kind = ProtocolActionKind::FailCommand,
                         .connectionId = connectionId_,
                         .commandId = std::move(command.id),
                         .reason = "modbus_discovery_mode_required"}};
            const auto request = requestDescriptor(command.payload, command.transport == "TCP");
            if (!request || isWriteFunction(request->functionCode))
                return {{.kind = ProtocolActionKind::FailCommand,
                         .connectionId = connectionId_,
                         .commandId = std::move(command.id),
                         .reason = "modbus_discovery_frame_invalid"}};
            const auto token = nextDeadlineToken_++;
            const auto timeout = std::clamp(command.timeout, std::chrono::milliseconds(100),
                                            std::chrono::milliseconds(60000));
            discovery_ = Discovery{command.id, *request, token, false};
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
        if (!device ||
            std::find(boundDevices_.begin(), boundDevices_.end(), device) == boundDevices_.end())
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .deviceId = std::move(command.deviceId),
                     .deviceCode = std::move(command.deviceCode),
                     .commandId = std::move(command.id),
                     .reason = "modbus_device_offline"}};
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
        const auto request = requestDescriptor(command.payload, device->modbusMode == "TCP");
        if (!request)
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .deviceId = device->id,
                     .deviceCode = device->code,
                     .commandId = std::move(command.id),
                     .reason = "modbus_command_frame_invalid"}};
        if (isWriteFunction(request->functionCode) && command.readbackPayload.empty())
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .deviceId = device->id,
                     .deviceCode = device->code,
                     .commandId = std::move(command.id),
                     .reason = "modbus_write_readback_required"}};
        auto& queue = queues_.at(device->id);
        if (queue.size() >= kMaximumDeviceQueue)
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = connectionId_,
                     .deviceId = device->id,
                     .deviceCode = device->code,
                     .commandId = std::move(command.id),
                     .reason = "modbus_device_queue_full"}};
        auto& target =
            command.highPriority
                ? (isWriteFunction(request->functionCode) ? queue.highWrites : queue.highReads)
                : (isWriteFunction(request->functionCode) ? queue.normalWrites : queue.normalReads);
        target.push_back(std::move(command));
        return dispatchNext();
    }

    [[nodiscard]] std::vector<ProtocolAction> deadline(std::uint64_t token) override {
        if (discovery_ && token == discovery_->deadlineToken) {
            auto completed = std::move(*discovery_);
            discovery_.reset();
            return {
                {.kind = ProtocolActionKind::CompleteCommand,
                 .connectionId = connectionId_,
                 .commandId = std::move(completed.commandId),
                 .reason = completed.found ? "discovery_devices_found" : "discovery_window_empty"}};
        }
        const auto poll = pollDeadlines_.find(token);
        if (poll != pollDeadlines_.end()) {
            const auto* device = poll->second;
            pollDeadlines_.erase(poll);
            pollDevices_.erase(device->id);
            std::vector<ProtocolAction> actions;
            enqueuePollNow(*device);
            appendNext(actions);
            return actions;
        }
        if (!inflight_ || inflight_->deadlineToken != token)
            return {};
        if (inflight_->phase == Phase::Write) {
            inflight_->writeAckMissing = true;
            return startReadback();
        }
        auto failed = std::move(*inflight_);
        inflight_.reset();
        const auto failureReason = failed.writeAckMissing
            ? "write_ack_missing:modbus_readback_timeout"
            : failed.phase == Phase::Readback ? "modbus_readback_timeout" : "modbus_response_timeout";
        std::vector<ProtocolAction> actions{{.kind = ProtocolActionKind::FailCommand,
                                             .connectionId = connectionId_,
                                             .deviceId = failed.device->id,
                                             .deviceCode = failed.device->code,
                                             .commandId = failed.command.id,
                                             .reason = failureReason}};
        finishCompletedOperation(failed.command, *failed.device, actions);
        return actions;
    }

  private:
    struct DeviceQueues {
        std::deque<ProtocolCommand> highWrites;
        std::deque<ProtocolCommand> highReads;
        std::deque<ProtocolCommand> normalWrites;
        std::deque<ProtocolCommand> normalReads;

        [[nodiscard]] std::size_t size() const noexcept {
            return highWrites.size() + highReads.size() + normalWrites.size() + normalReads.size();
        }
    };

    struct RequestDescriptor {
        bool tcp = false;
        std::uint16_t transactionId = 0;
        std::uint8_t unitId = 0;
        std::uint8_t functionCode = 0;
        std::uint16_t startAddress = 0;
        std::uint16_t quantity = 0;
    };

    struct ResponseFrame {
        bool tcp = false;
        std::uint16_t transactionId = 0;
        std::uint8_t unitId = 0;
        std::uint8_t functionCode = 0;
        bool exception = false;
        std::vector<std::uint8_t> data;
        std::vector<std::uint8_t> raw;
    };

    enum class Phase { Request, Write, Readback };

    struct Inflight {
        const DeviceDefinition* device = nullptr;
        ProtocolCommand command;
        RequestDescriptor request;
        Phase phase = Phase::Request;
        std::uint64_t deadlineToken = 0;
        bool writeAckMissing = false;
    };

    struct RegistrationMatch {
        const DeviceDefinition* device = nullptr;
        std::vector<std::uint8_t> payload;
        bool conflict = false;
    };

    struct Discovery {
        std::string commandId;
        RequestDescriptor request;
        std::uint64_t deadlineToken = 0;
        bool found = false;
    };

    static bool isWriteFunction(std::uint8_t functionCode) noexcept {
        return functionCode == 5 || functionCode == 6 || functionCode == 15 || functionCode == 16;
    }

    [[nodiscard]] std::vector<std::uint8_t> buildFrame(const DeviceDefinition& device,
                                                       std::span<const std::uint8_t> pdu) {
        std::vector<std::uint8_t> frame;
        if (device.modbusMode == "RTU") {
            frame.reserve(pdu.size() + 3);
            frame.push_back(device.slaveId);
            frame.insert(frame.end(), pdu.begin(), pdu.end());
            const auto crc = detail::crc16(frame);
            frame.push_back(static_cast<std::uint8_t>(crc));
            frame.push_back(static_cast<std::uint8_t>(crc >> 8U));
            return frame;
        }
        auto transaction = nextTransactionId_++;
        if (nextTransactionId_ == 0)
            nextTransactionId_ = 1;
        const auto length = static_cast<std::uint16_t>(pdu.size() + 1);
        frame = {static_cast<std::uint8_t>(transaction >> 8U),
                 static_cast<std::uint8_t>(transaction),
                 0,
                 0,
                 static_cast<std::uint8_t>(length >> 8U),
                 static_cast<std::uint8_t>(length),
                 device.slaveId};
        frame.insert(frame.end(), pdu.begin(), pdu.end());
        return frame;
    }

    void compileElementCommand(const DeviceDefinition& device, ProtocolCommand& command) {
        const auto resolved = command::resolve(device, command.elements);
        if (resolved.elements.size() != 1)
            throw std::invalid_argument(
                "command_invalid: one Modbus task must contain exactly one element");
        const auto& element = *resolved.elements.front().definition;
        const auto& value = resolved.elements.front().value;
        const auto address = static_cast<std::uint16_t>(element.address);
        std::vector<std::uint8_t> writePdu;
        std::vector<std::uint8_t> readPdu;
        if (element.registerType == "COIL") {
            writePdu = {5, static_cast<std::uint8_t>(address >> 8U),
                        static_cast<std::uint8_t>(address),
                        static_cast<std::uint8_t>(value == "1" ? 0xFF : 0x00), 0};
            readPdu = {1, static_cast<std::uint8_t>(address >> 8U),
                       static_cast<std::uint8_t>(address), 0, 1};
            command.expectedReadbackData = {static_cast<std::uint8_t>(value == "1" ? 1 : 0)};
        } else if (element.registerType == "HOLDING_REGISTER") {
            auto encoded = register_value::encodeValue(element, value);
            const auto quantity = static_cast<std::uint16_t>(element.quantity);
            if (quantity == 0 || encoded.size() != static_cast<std::size_t>(quantity) * 2)
                throw std::invalid_argument("command_invalid: Modbus register size mismatch");
            if (quantity == 1) {
                writePdu = {6, static_cast<std::uint8_t>(address >> 8U),
                            static_cast<std::uint8_t>(address), encoded[0], encoded[1]};
            } else {
                writePdu = {16,
                            static_cast<std::uint8_t>(address >> 8U),
                            static_cast<std::uint8_t>(address),
                            static_cast<std::uint8_t>(quantity >> 8U),
                            static_cast<std::uint8_t>(quantity),
                            static_cast<std::uint8_t>(encoded.size())};
                writePdu.insert(writePdu.end(), encoded.begin(), encoded.end());
            }
            readPdu = {
                3, static_cast<std::uint8_t>(address >> 8U), static_cast<std::uint8_t>(address),
                static_cast<std::uint8_t>(quantity >> 8U), static_cast<std::uint8_t>(quantity)};
            command.expectedReadbackData = std::move(encoded);
        } else {
            throw std::invalid_argument("command_invalid: Modbus register is read-only");
        }
        command.payload = buildFrame(device, writePdu);
        command.readbackPayload = buildFrame(device, readPdu);
        command.expectedValue = value;
    }

    static ProtocolAction bindAction(const DeviceDefinition& device) {
        return {.kind = ProtocolActionKind::BindDevice,
                .deviceId = device.id,
                .deviceCode = device.code};
    }

    void schedulePoll(const DeviceDefinition& device, std::vector<ProtocolAction>& actions) {
        if (device.elements.empty() || pollDevices_.contains(device.id))
            return;
        const auto token = nextDeadlineToken_++;
        pollDeadlines_[token] = &device;
        pollDevices_.insert(device.id);
        const auto interval = std::chrono::seconds(
            std::clamp<std::int64_t>(device.readInterval, 1, 86400));
        actions.push_back({.kind = ProtocolActionKind::ScheduleDeadline,
                           .connectionId = connectionId_,
                           .deviceId = device.id,
                           .deviceCode = device.code,
                           .deadlineToken = token,
                           .deadlineAfter = staggeredPollDelay(device.id, interval)});
    }

    void enqueuePollNow(const DeviceDefinition& device) {
        if (device.elements.empty())
            return;
        auto& queue = queues_.at(device.id);
        const auto alreadyQueued = std::any_of(queue.normalReads.begin(), queue.normalReads.end(),
                                               [](const auto& command) {
                                                   return command.kind == "poll";
                                               });
        if (alreadyQueued)
            return;
        for (auto& command : pollCommands(device)) {
            if (queue.size() >= kMaximumDeviceQueue)
                break;
            queue.normalReads.push_back(std::move(command));
        }
    }

    [[nodiscard]] std::vector<ProtocolCommand> pollCommands(const DeviceDefinition& device) {
        std::vector<ProtocolCommand> commands;
        struct ReadGroup {
            std::uint8_t function = 0;
            std::uint16_t address = 0;
            std::uint16_t quantity = 0;
        };
        std::map<std::string, std::vector<const ElementDefinition*>, std::less<>> byType;
        for (const auto& element : device.elements)
            byType[element.registerType].push_back(&element);
        std::vector<ReadGroup> groups;
        for (auto& [registerType, elements] : byType) {
            const auto function = registerType == "COIL"               ? 1U
                                  : registerType == "DISCRETE_INPUT"   ? 2U
                                  : registerType == "HOLDING_REGISTER" ? 3U
                                  : registerType == "INPUT_REGISTER"   ? 4U
                                                                       : 0U;
            if (function == 0 || elements.empty())
                continue;
            std::ranges::sort(elements, {}, &ElementDefinition::address);
            const auto maximum = static_cast<std::uint16_t>(
                function <= 2 ? 2000 : std::clamp<std::int64_t>(device.modbusMaxQuantity, 1, 125));
            const auto mergeGap = std::clamp<std::int64_t>(device.modbusMergeGap, 0, 2000);
            for (const auto* element : elements) {
                const auto address = static_cast<std::uint16_t>(
                    std::clamp<std::int64_t>(element->address, 0, 65535));
                const auto quantity = static_cast<std::uint16_t>(
                    std::clamp<std::int64_t>(element->quantity, 1, maximum));
                if (!groups.empty() && groups.back().function == function) {
                    auto& current = groups.back();
                    const auto currentEnd =
                        static_cast<std::uint32_t>(current.address) + current.quantity;
                    const auto nextEnd = static_cast<std::uint32_t>(address) + quantity;
                    const auto gap = static_cast<std::int64_t>(address) - currentEnd;
                    const auto merged = nextEnd - current.address;
                    if (gap <= mergeGap && merged <= maximum) {
                        current.quantity = static_cast<std::uint16_t>(
                            std::max<std::uint32_t>(current.quantity, merged));
                        continue;
                    }
                }
                groups.push_back({static_cast<std::uint8_t>(function), address, quantity});
            }
        }
        for (const auto& group : groups) {
            const auto function = group.function;
            const auto address = group.address;
            const auto quantity = group.quantity;
            std::vector<std::uint8_t> payload;
            if (device.modbusMode == "RTU") {
                payload = {device.slaveId,
                           static_cast<std::uint8_t>(function),
                           static_cast<std::uint8_t>(address >> 8U),
                           static_cast<std::uint8_t>(address),
                           static_cast<std::uint8_t>(quantity >> 8U),
                           static_cast<std::uint8_t>(quantity)};
                const auto crc = detail::crc16(payload);
                payload.push_back(static_cast<std::uint8_t>(crc & 0xFFU));
                payload.push_back(static_cast<std::uint8_t>(crc >> 8U));
            } else {
                auto transaction = nextTransactionId_++;
                if (nextTransactionId_ == 0)
                    nextTransactionId_ = 1;
                payload = {static_cast<std::uint8_t>(transaction >> 8U),
                           static_cast<std::uint8_t>(transaction),
                           0,
                           0,
                           0,
                           6,
                           device.slaveId,
                           static_cast<std::uint8_t>(function),
                           static_cast<std::uint8_t>(address >> 8U),
                           static_cast<std::uint8_t>(address),
                           static_cast<std::uint8_t>(quantity >> 8U),
                           static_cast<std::uint8_t>(quantity)};
            }
            commands.push_back({.deviceId = device.id,
                                .deviceCode = device.code,
                                .kind = "poll",
                                .payload = std::move(payload),
                                .highPriority = false,
                                .timeout = std::chrono::seconds(3)});
        }
        return commands;
    }

    void bindClientTarget() {
        for (const auto* device : devices_)
            if (device->targetId == targetId_)
                boundDevices_.push_back(device);
    }

    void bindRegistration(const DeviceDefinition& matched) {
        boundDevices_.clear();
        for (const auto* device : devices_) {
            if (!matched.registrationBytes.empty() &&
                device->registrationBytes == matched.registrationBytes)
                boundDevices_.push_back(device);
        }
        if (boundDevices_.empty())
            boundDevices_.push_back(&matched);
    }

    [[nodiscard]] RegistrationMatch
    matchRegistration(const std::vector<std::uint8_t>& bytes) const {
        RegistrationMatch result;
        std::size_t matchedLength = 0;
        for (const auto* device : devices_) {
            const auto& registration = device->registrationBytes;
            if (registration.empty() || bytes.size() < registration.size() ||
                !std::equal(registration.begin(), registration.end(), bytes.begin()))
                continue;
            if (result.device && registration.size() == matchedLength &&
                registration != result.device->registrationBytes) {
                result.conflict = true;
                return result;
            }
            if (!result.device || registration.size() > matchedLength) {
                result.device = device;
                matchedLength = registration.size();
            }
        }
        if (result.device)
            result.payload.assign(bytes.begin() + static_cast<std::ptrdiff_t>(matchedLength),
                                  bytes.end());
        return result;
    }

    [[nodiscard]] bool isHeartbeat(const std::vector<std::uint8_t>& bytes) const {
        return std::any_of(devices_.begin(), devices_.end(), [&](const auto* device) {
            return !device->heartbeatBytes.empty() && device->heartbeatBytes == bytes &&
                   (boundDevices_.empty() || std::find(boundDevices_.begin(), boundDevices_.end(),
                                                       device) != boundDevices_.end());
        });
    }

    [[nodiscard]] const DeviceDefinition* identifyByUnit(std::uint8_t unit,
                                                         bool tcp) const noexcept {
        const DeviceDefinition* matched = nullptr;
        for (const auto* device : devices_) {
            if (device->slaveId != unit || (device->modbusMode == "TCP") != tcp)
                continue;
            if (matched)
                return nullptr;
            matched = device;
        }
        return matched;
    }

    [[nodiscard]] const DeviceDefinition* boundByUnit(std::uint8_t unit, bool tcp) const noexcept {
        for (const auto* device : boundDevices_)
            if (device->slaveId == unit && (device->modbusMode == "TCP") == tcp)
                return device;
        return nullptr;
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

    [[nodiscard]] static std::optional<RequestDescriptor>
    requestDescriptor(std::span<const std::uint8_t> bytes, bool tcp) {
        RequestDescriptor result;
        std::size_t pdu = 0;
        if (tcp) {
            if (bytes.size() < 8 || bytes[2] != 0 || bytes[3] != 0)
                return std::nullopt;
            const auto length = detail::readBe16(bytes, 4);
            if (length < 2 || length > 254 || bytes.size() != static_cast<std::size_t>(6 + length))
                return std::nullopt;
            result.tcp = true;
            result.transactionId = detail::readBe16(bytes, 0);
            result.unitId = bytes[6];
            result.functionCode = bytes[7];
            pdu = 8;
        } else {
            if (bytes.size() < 4 || !detail::validRtu(bytes))
                return std::nullopt;
            result.unitId = bytes[0];
            result.functionCode = bytes[1];
            pdu = 2;
        }
        if (bytes.size() >= pdu + 4) {
            result.startAddress = detail::readBe16(bytes, pdu);
            result.quantity = detail::readBe16(bytes, pdu + 2);
        }
        return result.functionCode == 0 ? std::nullopt : std::optional(result);
    }

    [[nodiscard]] std::optional<bool> expectedFrameMode() const noexcept {
        if (inflight_)
            return inflight_->request.tcp;
        if (discovery_)
            return discovery_->request.tcp;
        if (boundDevices_.empty())
            return std::nullopt;
        const auto tcp = boundDevices_.front()->modbusMode == "TCP";
        if (std::any_of(boundDevices_.begin(), boundDevices_.end(),
                        [tcp](const auto* device) { return (device->modbusMode == "TCP") != tcp; }))
            return std::nullopt;
        return tcp;
    }

    [[nodiscard]] std::vector<ResponseFrame> extractFrames(std::optional<bool> expectedTcp) {
        std::vector<ResponseFrame> frames;
        while (!receiveBuffer_.empty()) {
            if (expectedTcp && *expectedTcp && receiveBuffer_.size() < 6)
                break;
            bool tcp = expectedTcp.value_or(false);
            if (!expectedTcp && receiveBuffer_.size() >= 6 && receiveBuffer_[2] == 0 &&
                receiveBuffer_[3] == 0) {
                const auto payloadLength = detail::readBe16(receiveBuffer_, 4);
                tcp = payloadLength >= 2 && payloadLength <= 254;
            }
            std::size_t length = 0;
            if (tcp) {
                if (receiveBuffer_[2] != 0 || receiveBuffer_[3] != 0) {
                    receiveBuffer_.erase(receiveBuffer_.begin());
                    continue;
                }
                const auto payloadLength = detail::readBe16(receiveBuffer_, 4);
                if (payloadLength < 2 || payloadLength > 254) {
                    receiveBuffer_.erase(receiveBuffer_.begin());
                    continue;
                }
                length = 6 + payloadLength;
            } else {
                if (receiveBuffer_.size() < 3)
                    break;
                const auto functionCode = receiveBuffer_[1];
                if ((functionCode & 0x80U) != 0)
                    length = 5;
                else if (functionCode >= 1 && functionCode <= 4)
                    length = static_cast<std::size_t>(receiveBuffer_[2]) + 5;
                else if (isWriteFunction(functionCode))
                    length = 8;
                else {
                    receiveBuffer_.erase(receiveBuffer_.begin());
                    continue;
                }
            }
            if (receiveBuffer_.size() < length)
                break;
            std::vector<std::uint8_t> raw(receiveBuffer_.begin(),
                                          receiveBuffer_.begin() +
                                              static_cast<std::ptrdiff_t>(length));
            if (!tcp && !detail::validRtu(raw)) {
                receiveBuffer_.erase(receiveBuffer_.begin());
                continue;
            }
            receiveBuffer_.erase(receiveBuffer_.begin(),
                                 receiveBuffer_.begin() + static_cast<std::ptrdiff_t>(length));
            ResponseFrame frame;
            frame.tcp = tcp;
            frame.transactionId = tcp ? detail::readBe16(raw, 0) : 0;
            frame.unitId = tcp ? raw[6] : raw[0];
            const auto functionOffset = tcp ? 7U : 1U;
            frame.exception = (raw[functionOffset] & 0x80U) != 0;
            frame.functionCode = static_cast<std::uint8_t>(raw[functionOffset] & 0x7FU);
            const auto payloadOffset = functionOffset + 1;
            const auto payloadEnd = raw.size() - (tcp ? 0 : 2);
            if (payloadEnd > payloadOffset)
                frame.data.assign(raw.begin() + static_cast<std::ptrdiff_t>(payloadOffset),
                                  raw.begin() + static_cast<std::ptrdiff_t>(payloadEnd));
            frame.raw = std::move(raw);
            frames.push_back(std::move(frame));
        }
        return frames;
    }

    [[nodiscard]] static bool matches(const Inflight& inflight,
                                      const ResponseFrame& response) noexcept {
        return inflight.request.tcp == response.tcp &&
               (!response.tcp || inflight.request.transactionId == response.transactionId) &&
               inflight.request.unitId == response.unitId &&
               inflight.request.functionCode == response.functionCode;
    }

    [[nodiscard]] static bool matches(const RequestDescriptor& request,
                                      const ResponseFrame& response) noexcept {
        return request.tcp == response.tcp &&
               (!response.tcp || request.transactionId == response.transactionId) &&
               request.functionCode == response.functionCode;
    }

    [[nodiscard]] std::vector<ProtocolAction> consumeResponse(const ProtocolInput& input,
                                                              const DeviceDefinition& device,
                                                              ResponseFrame frame) {
        std::vector<ProtocolAction> actions;
        if (!inflight_ || inflight_->device != &device || !matches(*inflight_, frame))
            return actions;
        actions.push_back({.kind = ProtocolActionKind::CancelDeadline,
                           .connectionId = connectionId_,
                           .deadlineToken = inflight_->deadlineToken});
        if (frame.exception) {
            auto failed = std::move(*inflight_);
            inflight_.reset();
            actions.push_back({.kind = ProtocolActionKind::FailCommand,
                               .connectionId = connectionId_,
                               .deviceId = device.id,
                               .deviceCode = device.code,
                               .commandId = failed.command.id,
                               .reason = "modbus_exception_response"});
            finishCompletedOperation(failed.command, *failed.device, actions);
            return actions;
        }
        if (inflight_->phase == Phase::Write) {
            auto readback = startReadback();
            actions.insert(actions.end(), std::make_move_iterator(readback.begin()),
                           std::make_move_iterator(readback.end()));
            return actions;
        }

        const auto parsed =
            parsedAction(input, device, frame, inflight_->request, inflight_->command.id);
        if (inflight_->command.kind == "poll")
            pollCycles_[device.id].observe(parsed, actions);
        else
            actions.push_back(parsed);
        if (inflight_->phase == Phase::Readback &&
            !inflight_->command.expectedReadbackData.empty()) {
            const auto values = frame.data.empty()
                                    ? std::span<const std::uint8_t>{}
                                    : std::span<const std::uint8_t>(frame.data).subspan(1);
            if (!std::equal(values.begin(), values.end(),
                            inflight_->command.expectedReadbackData.begin(),
                            inflight_->command.expectedReadbackData.end())) {
                auto failed = std::move(*inflight_);
                inflight_.reset();
                actions.push_back({.kind = ProtocolActionKind::FailCommand,
                                   .connectionId = connectionId_,
                                   .deviceId = device.id,
                                   .deviceCode = device.code,
                                   .commandId = failed.command.id,
                                   .reason = failed.writeAckMissing
                                       ? "write_ack_missing:modbus_readback_mismatch"
                                       : "modbus_readback_mismatch"});
                finishCompletedOperation(failed.command, *failed.device, actions);
                return actions;
            }
        }
        auto completed = std::move(*inflight_);
        inflight_.reset();
        actions.push_back({.kind = completed.writeAckMissing ? ProtocolActionKind::FailCommand
                                                               : ProtocolActionKind::CompleteCommand,
                           .connectionId = connectionId_,
                           .deviceId = device.id,
                           .deviceCode = device.code,
                           .commandId = completed.command.id,
                           .reason = completed.writeAckMissing ? "write_ack_missing" : ""});
        finishCompletedOperation(completed.command, *completed.device, actions);
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> startReadback() {
        if (!inflight_ || inflight_->command.readbackPayload.empty())
            return {};
        const auto descriptor = requestDescriptor(inflight_->command.readbackPayload,
                                                  inflight_->device->modbusMode == "TCP");
        if (!descriptor) {
            auto failed = std::move(*inflight_);
            inflight_.reset();
            std::vector<ProtocolAction> actions{{.kind = ProtocolActionKind::FailCommand,
                                                 .connectionId = connectionId_,
                                                 .deviceId = failed.device->id,
                                                  .deviceCode = failed.device->code,
                                                  .commandId = failed.command.id,
                                                  .reason = "modbus_readback_frame_invalid"}};
            finishCompletedOperation(failed.command, *failed.device, actions);
            return actions;
        }
        inflight_->phase = Phase::Readback;
        inflight_->request = *descriptor;
        inflight_->deadlineToken = nextDeadlineToken_++;
        return {{.kind = ProtocolActionKind::Send,
                 .connectionId = connectionId_,
                 .deviceId = inflight_->device->id,
                 .deviceCode = inflight_->device->code,
                 .commandId = inflight_->command.id,
                 .bytes = inflight_->command.readbackPayload, .acquisitionId = inflight_->command.kind == "poll" ? pollCycles_[inflight_->device->id].id(connectionId_ + inflight_->device->id) : inflight_->command.id},
                {.kind = ProtocolActionKind::ScheduleDeadline,
                 .connectionId = connectionId_,
                 .commandId = inflight_->command.id,
                 .deadlineToken = inflight_->deadlineToken,
                 .deadlineAfter = inflight_->command.timeout}};
    }

    [[nodiscard]] std::vector<ProtocolAction> dispatchNext() {
        if (inflight_ || boundDevices_.empty())
            return {};
        for (const bool high : {true, false}) {
            for (std::size_t attempt = 0; attempt < boundDevices_.size(); ++attempt) {
                const auto index = (roundRobinIndex_ + attempt) % boundDevices_.size();
                const auto* device = boundDevices_[index];
                auto& queue = queues_.at(device->id);
                auto& writes = high ? queue.highWrites : queue.normalWrites;
                auto& reads = high ? queue.highReads : queue.normalReads;
                ProtocolCommand command;
                if (!writes.empty()) {
                    command = std::move(writes.front());
                    writes.pop_front();
                } else if (!reads.empty()) {
                    command = std::move(reads.front());
                    reads.pop_front();
                } else {
                    continue;
                }
                roundRobinIndex_ = (index + 1) % boundDevices_.size();
                const auto descriptor =
                    requestDescriptor(command.payload, device->modbusMode == "TCP");
                if (!descriptor)
                    continue;
                const auto phase =
                    isWriteFunction(descriptor->functionCode) ? Phase::Write : Phase::Request;
                const auto token = nextDeadlineToken_++;
                const auto timeout = std::clamp(command.timeout, std::chrono::milliseconds(100),
                                                std::chrono::milliseconds(60000));
                command.timeout = timeout;
                inflight_ = Inflight{device, std::move(command), *descriptor, phase, token, false};
                return {{.kind = ProtocolActionKind::Send,
                         .connectionId = connectionId_,
                         .deviceId = device->id,
                         .deviceCode = device->code,
                         .commandId = inflight_->command.id,
                         .bytes = inflight_->command.payload, .acquisitionId = inflight_->command.kind == "poll" ? pollCycles_[inflight_->device->id].id(connectionId_ + inflight_->device->id) : inflight_->command.id},
                        {.kind = ProtocolActionKind::ScheduleDeadline,
                         .connectionId = connectionId_,
                         .commandId = inflight_->command.id,
                         .deadlineToken = token,
                         .deadlineAfter = timeout}};
            }
        }
        return {};
    }

    void appendNext(std::vector<ProtocolAction>& actions) {
        auto next = dispatchNext();
        actions.insert(actions.end(), std::make_move_iterator(next.begin()),
                       std::make_move_iterator(next.end()));
    }

    void finishCompletedOperation(const ProtocolCommand& completed,
                                  const DeviceDefinition& device,
                                  std::vector<ProtocolAction>& actions) {
        if (completed.kind == "poll") {
            const auto& queue = queues_.at(device.id);
            const auto pollRemaining = std::any_of(queue.normalReads.begin(),
                                                   queue.normalReads.end(),
                                                   [](const auto& command) {
                                                       return command.kind == "poll";
                                                   });
            const bool failed = std::any_of(actions.begin(), actions.end(), [&](const auto& action) {
                return action.kind == ProtocolActionKind::FailCommand && action.commandId == completed.id;
            });
            if (failed) pollCycles_[device.id].fail();
            if (!pollRemaining) {
                pollCycles_[device.id].finish(actions);
                schedulePoll(device, actions);
            }
        }
        appendNext(actions);
    }

    static void appendQueueFailures(std::deque<ProtocolCommand>& queue, std::string_view reason,
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

    [[nodiscard]] ProtocolAction parsedAction(const ProtocolInput& input,
                                              const DeviceDefinition& device,
                                              const ResponseFrame& frame,
                                              const RequestDescriptor& request,
                                              std::string_view causationId) const {
        message::ParsedDeviceMessage message;
        message.causationId =
            causationId.empty() ? std::string(input.messageId) : std::string(causationId);
        message.linkId = link_.id;
        message.deviceId = device.id;
        message.modelId = device.modelId;
        message.deviceCode = device.code;
        message.protocol = "Modbus";
        message.connectionId = connectionId_;
        message.occurredAtMs = input.receivedAtMs;
        message.observedAtMs = input.receivedAtMs;
        message.storagePolicy = device.storagePolicy;
        message.onlineWindowMs =
            std::clamp<std::int64_t>(device.onlineTimeout, 1, 86400) * 1000;
        message.source = "query";
        message.rawPayloads = {frame.raw};
        message.rawPacketIds = {std::string(input.messageId) + ":frame:" + std::to_string(nextPacketSequence_++)};
        message.valuesJson = valuesJson(device, frame, request);
        return {.kind = ProtocolActionKind::PublishParsed,
                .connectionId = connectionId_,
                .deviceId = device.id,
                .deviceCode = device.code,
                .parsed = std::move(message)};
    }

    [[nodiscard]] static std::string valuesJson(const DeviceDefinition& device,
                                                const ResponseFrame& frame,
                                                const RequestDescriptor& request) {
        std::ostringstream json;
        json << "{\"function_code\":" << static_cast<unsigned>(frame.functionCode)
             << ",\"values\":{";
        const auto data = frame.data.empty() ? std::span<const std::uint8_t>{}
                                             : std::span<const std::uint8_t>(frame.data).subspan(1);
        bool first = true;
        for (const auto& element : device.elements) {
            const bool sameType =
                (frame.functionCode == 1 && element.registerType == "COIL") ||
                (frame.functionCode == 2 && element.registerType == "DISCRETE_INPUT") ||
                (frame.functionCode == 3 && element.registerType == "HOLDING_REGISTER") ||
                (frame.functionCode == 4 && element.registerType == "INPUT_REGISTER");
            if (!sameType)
                continue;
            if (element.address < request.startAddress ||
                element.address >= static_cast<std::int64_t>(request.startAddress) + request.quantity)
                continue;
            std::optional<std::string> numeric;
            if (frame.functionCode <= 2) {
                const auto bitOffset =
                    static_cast<std::size_t>(element.address - request.startAddress);
                if (bitOffset / 8 >= data.size())
                    continue;
                numeric = (data[bitOffset / 8] & (1U << (bitOffset % 8))) != 0 ? "1" : "0";
            } else {
                const auto offset =
                    static_cast<std::size_t>(element.address - request.startAddress) * 2;
                const auto width =
                    static_cast<std::size_t>(std::max<std::int64_t>(1, element.quantity)) * 2;
                if (offset + width > data.size())
                    continue;
                numeric = register_value::numericJson(data.subspan(offset, width), element);
            }
            if (!numeric)
                continue;
            if (!first)
                json << ',';
            first = false;
            json << '\"' << service::utils::jsonEscape(element.id) << "\":{\"name\":\""
                 << service::utils::jsonEscape(element.name) << "\",\"value\":" << *numeric
                 << ",\"unit\":\"" << service::utils::jsonEscape(element.unit) << "\"}";
        }
        json << "}}";
        return json.str();
    }

    static constexpr std::size_t kMaximumReceiveBuffer = 64 * 1024;
    static constexpr std::size_t kMaximumDeviceQueue = 256;

    LinkDefinition link_;
    std::string connectionId_;
    std::string targetId_;
    std::shared_ptr<const RuntimeSnapshot> snapshot_;
    std::vector<const DeviceDefinition*> devices_;
    std::map<std::string, const DeviceDefinition*, std::less<>> devicesById_;
    std::map<std::string, const DeviceDefinition*, std::less<>> devicesByCode_;
    std::map<std::string, DeviceQueues, std::less<>> queues_;
    std::map<std::string, AcquisitionCycle, std::less<>> pollCycles_;
    mutable std::uint64_t nextPacketSequence_ = 0;
    std::vector<const DeviceDefinition*> boundDevices_;
    std::vector<std::uint8_t> receiveBuffer_;
    std::optional<Inflight> inflight_;
    std::optional<Discovery> discovery_;
    std::map<std::uint64_t, const DeviceDefinition*> pollDeadlines_;
    std::set<std::string, std::less<>> pollDevices_;
    std::size_t roundRobinIndex_ = 0;
    std::uint64_t nextDeadlineToken_ = 1;
    std::uint16_t nextTransactionId_ = 1;
};

class SessionFactory final : public ProtocolSessionFactory {
  public:
    [[nodiscard]] const ProtocolDefinition& definition() const noexcept override {
        return kModbusProtocol;
    }

    [[nodiscard]] bool canRefreshTransport(const DeviceDefinition& device) const noexcept override {
        return device.modbusMode == "TCP";
    }

    [[nodiscard]] bool packetMatchesDevice(const DeviceDefinition& device, std::string_view,
        std::span<const std::uint8_t> bytes) const noexcept override {
        if (bytes.empty()) return true;
        if (device.modbusMode == "RTU") return bytes[0] == device.slaveId;
        return bytes.size() < 7 || bytes[2] != 0 || bytes[3] != 0 || bytes[6] == device.slaveId;
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

} // namespace service::collector::modbus
