#pragma once
#include "service/features/collector/collector.entity.h"
#include <set>
#include <utility>
#include <functional>
#include "service/common/message.h"

#include <chrono>
#include <span>
#include <string_view>
#include <cstdint>
#include <string>
#include <vector>

namespace service::collector {

enum class ProtocolCapability : std::uint32_t {
    TcpServer = 1U << 0U,
    TcpClient = 1U << 1U,
    Registration = 1U << 2U,
    Heartbeat = 1U << 3U,
    Polling = 1U << 4U,
    Discovery = 1U << 5U,
    Commands = 1U << 6U,
    UnsolicitedReports = 1U << 7U,
};

class ProtocolCapabilities final {
  public:
    constexpr ProtocolCapabilities() = default;
    constexpr explicit ProtocolCapabilities(std::uint32_t bits) : bits_(bits) {}

    [[nodiscard]] constexpr bool has(ProtocolCapability capability) const noexcept {
        return (bits_ & static_cast<std::uint32_t>(capability)) != 0;
    }

    [[nodiscard]] constexpr std::uint32_t bits() const noexcept { return bits_; }

  private:
    std::uint32_t bits_ = 0;
};

[[nodiscard]] constexpr ProtocolCapabilities operator|(ProtocolCapability left,
                                                       ProtocolCapability right) noexcept {
    return ProtocolCapabilities(static_cast<std::uint32_t>(left) |
                                static_cast<std::uint32_t>(right));
}

[[nodiscard]] constexpr ProtocolCapabilities operator|(ProtocolCapabilities left,
                                                       ProtocolCapability right) noexcept {
    return ProtocolCapabilities(left.bits() | static_cast<std::uint32_t>(right));
}

enum class ProtocolActionKind {
    Send,
    Close,
    BindDevice,
    ObserveParsed,
    DiscardCollection,
    FinishAcquisition,
    PublishParsed,
    CompleteCommand,
    FailCommand,
    ScheduleDeadline,
    CancelDeadline,
};

// 协议能力描述不持有连接、任务或其他可变运行状态。
enum class CommandLayout { WritableElements, CompleteFunction, WritableMessage };
enum class CommandTransport { Raw, DeviceConfigured };
enum class ResponseTracking { EverySend, CommandsOnly };
enum class PacketAttribution { Connection, ParsedFrame };

struct ProtocolDefinition {
    std::string_view name;
    ProtocolCapabilities capabilities;
    CommandLayout commandLayout;
    CommandTransport commandTransport;
    ResponseTracking responseTracking;
    PacketAttribution packetAttribution;
};

struct ProtocolAction {
    ProtocolActionKind kind = ProtocolActionKind::PublishParsed;
    std::string connectionId;
    std::string deviceId;
    std::string deviceCode;
    std::string commandId;
    std::string reason;
    std::uint64_t deadlineToken = 0;
    std::chrono::milliseconds deadlineAfter{0};
    std::vector<std::uint8_t> bytes;
    std::uint64_t publicationToken = 0;
    message::ParsedDeviceMessage parsed;
    std::optional<bool> responseSuccess;
    std::string acquisitionId;
    std::string replyToPacketId;
    bool acquisitionSummary = false;
};

struct ProtocolInput {
    std::string_view messageId;
    std::string_view linkId;
    std::string_view connectionId;
    std::string_view remoteAddress;
    std::int64_t receivedAtMs = 0;
    std::span<const std::uint8_t> bytes;
};

struct CommandElementValue {
    std::string elementId;
    std::string value;
};

struct ProtocolCommand {
    std::string id;
    std::string deviceId;
    std::string deviceCode;
    std::string transport;
    std::string kind;
    std::string protocol;
    std::vector<std::uint8_t> payload;
    std::vector<std::uint8_t> readbackPayload;
    std::vector<std::uint8_t> expectedReadbackData;
    std::string expectedValue;
    std::vector<CommandElementValue> elements;
    std::string mqttMessageId;
    bool highPriority = true;
    bool expectsResponse = true;
    std::chrono::milliseconds timeout{3000};
};

enum class TransmissionReservation { Expired, Duplicate, Reserved };

using RealtimePointDefinition = service::message::realtime::Point;
using RealtimeDeviceDefinition = service::message::realtime::Device;

struct RuntimeSnapshot {
    std::vector<LinkDefinition> links;
    std::vector<DeviceDefinition> devices;
    std::vector<RealtimeDeviceDefinition> realtimeDevices;
};

} // namespace service::collector

namespace service::collector {
using ClientTargetKey = std::pair<std::string, std::string>;

struct RuntimeReconcilePlan {
    std::set<std::string, std::less<>> affectedLinks;
    std::set<std::string, std::less<>> restartLinks;
    std::set<ClientTargetKey> refreshClientSessions;
    std::set<ClientTargetKey> restartClientTargets;
};

} // namespace service::collector

namespace service::collector::command {

struct ResolvedElement {
    const ElementDefinition* definition = nullptr;
    std::string value;
};

struct ResolvedCommand {
    std::string functionCode;
    std::vector<ResolvedElement> elements;
};

} // namespace service::collector::command
