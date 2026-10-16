#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <set>
#include <optional>
#include <memory_resource>
#include <ruvia/web/model.h>
#include <ruvia/web/model_json.h>
#include <ruvia/web/model_object.h>

#include "service/common/uuid.h"
#include "service/utils/json.h"
#include "service/utils/network.h"

namespace service::message::serial_debug {
inline std::string inputKey(std::string_view nodeId) {
    return "iot:edge:serial:input:" + std::string(nodeId);
}
inline std::string outputKey(std::string_view nodeId, std::string_view sessionId) {
    return "iot:edge:serial:output:" + std::string(nodeId) + ":" + std::string(sessionId);
}
inline std::string eventTopic(std::string_view nodeId, std::string_view sessionId) {
    return "edge-serial:" + std::string(nodeId) + ":" + std::string(sessionId);
}
} // namespace service::message::serial_debug

namespace service::message::vpn {

inline constexpr std::int64_t kDesktopSessionLeaseMs{45000};
inline std::string desktopSessionKey(std::string_view peerId) {
    return "iot:vpn:desktop:sessions:" + std::string(peerId);
}

// Shared VPN address contract. It contains only immutable values and pure mapping logic.
using service::utils::network::Ipv4Cidr;

inline constexpr Ipv4Cidr kOverlayPool{0x64600000U, 11}; // 100.96.0.0/11
inline constexpr Ipv4Cidr kVirtualLanPool{0xac100000U, 12}; // 172.16.0.0/12

inline std::optional<Ipv4Cidr> mappedVirtualCidr(const Ipv4Cidr& real) noexcept {
    if (real.prefix < kVirtualLanPool.prefix)
        return std::nullopt;
    const auto slotBits = static_cast<unsigned>(real.prefix - kVirtualLanPool.prefix);
    const auto slotCount = std::uint64_t{1} << slotBits;
    const auto slot = (static_cast<std::uint64_t>(real.network) >> (32U - real.prefix)) &
                      (slotCount - 1U);
    const auto network = static_cast<std::uint64_t>(kVirtualLanPool.network) +
                         slot * static_cast<std::uint64_t>(real.size());
    return network <= 0xffffffffU
               ? std::optional<Ipv4Cidr>(Ipv4Cidr{static_cast<std::uint32_t>(network), real.prefix})
               : std::nullopt;
}

} // namespace service::message::vpn

namespace service::message::device {
// Device timezone used when protocol parameters omit `timezone`.
inline constexpr std::string_view kDefaultTimezone{"+08:00"};
} // namespace service::message::device

namespace service::message {

namespace edge {
inline constexpr std::uint32_t kProtocolVersion = 6;
inline constexpr std::uint32_t kOldestCompatibleProtocolVersion = 2;
inline constexpr std::string_view kDefaultPlatformId{
    "00000000-0000-7000-8000-000000000001"};
inline constexpr std::string_view kDefaultPublicBaseUrl{"https://i.a-z.xin"};
inline constexpr std::size_t kMaxMessageSize{16U * 1024U};

inline std::string authKey(std::string_view imei) {
    return "iot:edge:auth:" + std::string(imei);
}

// Node session lease written by the edge gateway and read by management modules.
inline std::string sessionKey(std::string_view nodeId) {
    return "iot:edge:session:" + std::string(nodeId);
}

inline std::string metadataKey(std::string_view nodeId) {
    return "iot:edge:metadata:" + std::string(nodeId);
}

inline std::string egressKey(std::string_view nodeId) {
    return "iot:edge:egress:" + std::string(nodeId);
}

inline std::string configKey(std::string_view nodeId) {
    return "iot:edge:config:" + std::string(nodeId);
}

inline std::string configRevisionKey(std::string_view nodeId) {
    return "iot:edge:config-revision:" + std::string(nodeId);
}

inline std::string commandQueueKey(std::string_view nodeId) {
    return "iot:v2:edge:commands:" + std::string(nodeId);
}

inline std::string deviceStatusKey(std::string_view nodeId, std::string_view deviceId) {
    return "iot:runtime:edge:" + std::string(nodeId) + ":device:" + std::string(deviceId);
}

inline std::string logResultKey(std::string_view requestId) {
    return "iot:edge:logs:" + std::string(requestId);
}

inline std::string logSnapshotKey(std::string_view nodeId) {
    return "iot:edge:logs:snapshot:" + std::string(nodeId);
}

inline std::string logLevelResultKey(std::string_view requestId) {
    return "iot:edge:logs:level:" + std::string(requestId);
}
} // namespace edge

inline constexpr std::array<std::string_view, 6> kWebhookEvents{
    "device.data.reported", "device.image.reported", "device.command.accepted",
    "device.command.updated", "device.alert.triggered", "device.alert.resolved",
};

inline bool supportedEvent(std::string_view value) {
    return std::find(kWebhookEvents.begin(), kWebhookEvents.end(), value) != kWebhookEvents.end();
}

inline std::string openMqttTopic(std::string_view deviceId) {
    return "iot/" + std::string(deviceId);
}

inline std::string webhookEnvelope(std::string_view eventType, std::string_view occurredAt,
                                    std::string_view deliveryId, std::string_view dataJson) {
    return "{\"event\":" + service::utils::jsonQuoted(eventType) +
           ",\"time\":" + service::utils::jsonQuoted(occurredAt) +
           ",\"deliveryId\":" + service::utils::jsonQuoted(deliveryId) +
           ",\"data\":" + std::string(dataJson) + "}";
}

inline constexpr std::string_view kMessageSchemaVersion{"2"};

inline constexpr std::string_view kConfigStreamPrefix = "iot:channel:config:worker:";
inline constexpr std::string_view kIngressStreamPrefix = "iot:channel:packet:raw:worker:";
inline constexpr std::string_view kEgressStreamPrefix = "iot:channel:socket:egress:worker:";
inline constexpr std::string_view kCommandStreamPrefix = "iot:channel:command:worker:";
inline constexpr std::string_view kLinkEventStreamPrefix = "iot:channel:link:event:worker:";
inline constexpr std::string_view kControlStreamPrefix = "iot:channel:control:worker:";
inline constexpr std::string_view kDeadLetterStreamPrefix = "iot:channel:dead-letter:worker:";

inline std::string workerStream(std::string_view prefix, std::size_t workerIndex,
                                std::string_view suffix,
                                std::string_view instance) {
    return std::string(prefix) + std::string(instance) + ":" + std::to_string(workerIndex) + std::string(suffix);
}

inline std::string configStream(std::size_t workerIndex, std::string_view instance) {
    return workerStream(kConfigStreamPrefix, workerIndex, {}, instance);
}

inline std::string ingressStream(std::size_t workerIndex, std::string_view instance) {
    return workerStream(kIngressStreamPrefix, workerIndex, {}, instance);
}

inline std::string parsedStream() { return "iot:v3:telemetry"; }

inline std::string egressStream(std::size_t workerIndex, std::string_view instance) {
    return workerStream(kEgressStreamPrefix, workerIndex, {}, instance);
}

inline std::string commandStream(std::size_t workerIndex, bool highPriority,
                                  std::string_view instance) {
    return workerStream(kCommandStreamPrefix, workerIndex, highPriority ? ":high" : ":normal", instance);
}

inline std::string commandResultStream() { return "iot:v3:command-result"; }

inline std::string linkEventStream(std::size_t workerIndex, std::string_view instance) {
    return workerStream(kLinkEventStreamPrefix, workerIndex, {}, instance);
}

inline std::string controlStream(std::size_t workerIndex, std::string_view instance) {
    return workerStream(kControlStreamPrefix, workerIndex, {}, instance);
}

inline std::string deadLetterStream(std::size_t workerIndex, std::string_view instance) {
    return workerStream(kDeadLetterStreamPrefix, workerIndex, {}, instance);
}

struct StreamField {
    std::string name;
    std::string value;
};

struct StreamMessage {
    std::string id;
    std::vector<StreamField> fields;

    [[nodiscard]] std::string_view get(std::string_view name) const noexcept {
        for (const auto& field : fields)
            if (field.name == name)
                return field.value;
        return {};
    }
};

struct IngressPacket {
    std::string messageId;
    std::string workerInstanceId;
    std::string linkId;
    std::string connectionId;
    std::string remoteAddress;
    std::uint64_t sessionEpoch = 0;
    std::int64_t occurredAtMs = 0;
    std::vector<std::uint8_t> payload;
};

struct ConnectionEvent {
    std::string messageId;
    std::string workerInstanceId;
    std::string eventType;
    std::string linkId;
    std::string connectionId;
    std::string remoteAddress;
    std::string targetId;
    std::string reason;
    std::uint64_t sessionEpoch = 0;
    std::int64_t occurredAtMs = 0;
};

struct EgressPacket {
    std::string messageId;
    std::string workerInstanceId;
    std::string causationId;
    std::string connectionId;
    std::uint64_t sessionEpoch = 0;
    std::int64_t createdAtMs = 0;
    std::vector<std::uint8_t> payload;
    std::string replyToPacketId;
    std::string acquisitionId;
};

struct ParsedDeviceMessage {
    std::string acquisitionId;
    // 原始帧身份独立于历史幂等 ID，按 rawPayloads 相同顺序排列。
    std::vector<std::string> rawPacketIds;
    std::string eventKind = "sample";
    std::string messageId;
    std::string causationId;
    std::string linkId;
    std::string deviceId;
    std::string modelId;
    std::string deviceCode;
    std::string protocol;
    std::string connectionId;
    std::int64_t occurredAtMs = 0;
    std::int64_t observedAtMs = 0;
    std::string storagePolicy = "report";
    std::int64_t onlineWindowMs = 300000;
    std::string source = "push";
    std::string valuesJson;
    std::vector<std::vector<std::uint8_t>> rawPayloads;
};

struct ProtocolTask {
    std::string messageId;
    std::string causationId;
    std::string groupKey;
    std::string protocol;
    std::string transport;
    std::string kind;
    std::string linkId;
    std::string deviceId;
    std::string deviceCode;
    std::string connectionId;
    std::string payload;
    std::string readbackPayload;
    std::string expectedReadbackData;
    std::string expectedValue;
    std::vector<std::pair<std::string, std::string>> elements;
    std::string mqttMessageId;
    bool expectsResponse = true;
    std::int64_t responseTimeoutMs = 3000;
    std::int64_t createdAtMs = 0;
    std::int64_t attempt = 1;
    std::int64_t maxAttempts = 3;
    std::uint64_t sessionEpoch = 0;
};

inline std::int64_t utcNowMilliseconds() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}


inline std::int64_t effectiveObservedAt(std::int64_t observedAtMs,
                                        std::int64_t occurredAtMs) noexcept {
    if (occurredAtMs <= 0)
        return std::max<std::int64_t>(observedAtMs, 0);
    if (observedAtMs <= 0 || observedAtMs > occurredAtMs)
        return occurredAtMs;
    return observedAtMs;
}

inline std::string toHex(const std::vector<std::uint8_t>& bytes) {
    static constexpr std::array<char, 16> digits{'0', '1', '2', '3', '4', '5', '6', '7',
                                                 '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    std::string result;
    result.reserve(bytes.size() * 2);
    for (const auto byte : bytes) {
        result.push_back(digits[(byte >> 4U) & 0x0FU]);
        result.push_back(digits[byte & 0x0FU]);
    }
    return result;
}

inline std::vector<std::uint8_t> fromHex(std::string_view value) {
    auto nibble = [](char character) -> int {
        if (character >= '0' && character <= '9')
            return character - '0';
        if (character >= 'a' && character <= 'f')
            return character - 'a' + 10;
        if (character >= 'A' && character <= 'F')
            return character - 'A' + 10;
        return -1;
    };
    if (value.size() % 2 != 0)
        return {};
    std::vector<std::uint8_t> result;
    result.reserve(value.size() / 2);
    for (std::size_t index = 0; index < value.size(); index += 2) {
        const auto high = nibble(value[index]);
        const auto low = nibble(value[index + 1]);
        if (high < 0 || low < 0)
            return {};
        result.push_back(static_cast<std::uint8_t>((high << 4) | low));
    }
    return result;
}

inline std::string rawPayloadsJson(const std::vector<std::vector<std::uint8_t>>& payloads) {
    std::string result{"["};
    for (std::size_t index = 0; index < payloads.size(); ++index) {
        if (index != 0)
            result.push_back(',');
        result.push_back('"');
        result += toHex(payloads[index]);
        result.push_back('"');
    }
    result.push_back(']');
    return result;
}

inline std::vector<std::vector<std::uint8_t>> rawPayloadsFromJson(std::string_view value) {
    std::vector<std::vector<std::uint8_t>> result;
    std::size_t offset = 0;
    const auto skipWhitespace = [&] {
        while (offset < value.size() && (value[offset] == ' ' || value[offset] == '\n' ||
                                         value[offset] == '\r' || value[offset] == '\t'))
            ++offset;
    };
    skipWhitespace();
    if (offset == value.size() || value[offset++] != '[')
        return {};
    skipWhitespace();
    if (offset < value.size() && value[offset] == ']') {
        ++offset;
        skipWhitespace();
        return offset == value.size() ? result : std::vector<std::vector<std::uint8_t>>{};
    }
    while (offset < value.size()) {
        if (value[offset++] != '"')
            return {};
        const auto end = value.find('"', offset);
        if (end == std::string_view::npos)
            return {};
        const auto hex = value.substr(offset, end - offset);
        auto payload = fromHex(hex);
        if (payload.empty() && !hex.empty())
            return {};
        result.push_back(std::move(payload));
        offset = end + 1;
        skipWhitespace();
        if (offset == value.size())
            return {};
        if (value[offset] == ']') {
            ++offset;
            skipWhitespace();
            return offset == value.size() ? result : std::vector<std::vector<std::uint8_t>>{};
        }
        if (value[offset++] != ',')
            return {};
        skipWhitespace();
    }
    return {};
}

inline std::vector<StreamField> ingressFields(const IngressPacket& packet) {
    return {{"event_type", "packet"},
            {"event_id", packet.messageId},
            {"schema_version", std::string(kMessageSchemaVersion)},
            {"message_id", packet.messageId},
            {"worker_instance_id", packet.workerInstanceId},
            {"link_id", packet.linkId},
            {"connection_id", packet.connectionId},
            {"remote_address", packet.remoteAddress},
            {"session_epoch", std::to_string(packet.sessionEpoch)},
            {"occurred_at_ms", std::to_string(packet.occurredAtMs)},
            {"payload_hex", toHex(packet.payload)}};
}

inline IngressPacket ingressFrom(const StreamMessage& message) {
    const auto require = [&message](std::string_view name) {
        const auto value = message.get(name);
        if (value.empty())
            throw std::runtime_error("Missing ingress field: " + std::string(name));
        return value;
    };
    const auto eventType = message.get("event_type");
    if (!eventType.empty() && eventType != "packet")
        throw std::runtime_error("Ingress message is not a packet");
    const auto integer = [&require](std::string_view name) {
        const auto value = require(name);
        std::int64_t result = 0;
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (error != std::errc{} || end != value.data() + value.size())
            throw std::runtime_error("Invalid ingress integer field: " + std::string(name));
        return result;
    };
    IngressPacket packet;
    packet.messageId = std::string(require("message_id"));
    packet.workerInstanceId = std::string(require("worker_instance_id"));
    packet.linkId = std::string(require("link_id"));
    packet.connectionId = std::string(require("connection_id"));
    packet.remoteAddress = std::string(message.get("remote_address"));
    packet.sessionEpoch = static_cast<std::uint64_t>(integer("session_epoch"));
    packet.occurredAtMs = integer("occurred_at_ms");
    const auto payload = require("payload_hex");
    packet.payload = fromHex(payload);
    if (packet.payload.empty() && !payload.empty())
        throw std::runtime_error("Invalid ingress HEX payload");
    return packet;
}

inline std::vector<StreamField> connectionEventFields(const ConnectionEvent& event) {
    return {{"event_type", event.eventType},
            {"event_id", event.messageId},
            {"schema_version", std::string(kMessageSchemaVersion)},
            {"message_id", event.messageId},
            {"worker_instance_id", event.workerInstanceId},
            {"link_id", event.linkId},
            {"connection_id", event.connectionId},
            {"remote_address", event.remoteAddress},
            {"target_id", event.targetId},
            {"reason", event.reason},
            {"session_epoch", std::to_string(event.sessionEpoch)},
            {"occurred_at_ms", std::to_string(event.occurredAtMs)}};
}

inline ConnectionEvent connectionEventFrom(const StreamMessage& message) {
    const auto require = [&message](std::string_view name) {
        const auto value = message.get(name);
        if (value.empty())
            throw std::runtime_error("Missing connection event field: " + std::string(name));
        return value;
    };
    const auto integer = [&require](std::string_view name) {
        const auto value = require(name);
        std::int64_t result = 0;
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (error != std::errc{} || end != value.data() + value.size() || result < 0)
            throw std::runtime_error("Invalid connection event integer: " + std::string(name));
        return result;
    };
    ConnectionEvent event;
    event.eventType = std::string(require("event_type"));
    if (event.eventType != "connected" && event.eventType != "disconnected")
        throw std::runtime_error("Invalid connection event type");
    event.messageId = std::string(require("message_id"));
    event.workerInstanceId = std::string(require("worker_instance_id"));
    event.linkId = std::string(require("link_id"));
    event.connectionId = std::string(require("connection_id"));
    event.remoteAddress = std::string(message.get("remote_address"));
    event.targetId = std::string(message.get("target_id"));
    event.reason = std::string(message.get("reason"));
    event.sessionEpoch = static_cast<std::uint64_t>(integer("session_epoch"));
    event.occurredAtMs = integer("occurred_at_ms");
    return event;
}

inline std::vector<StreamField> egressFields(const EgressPacket& packet) {
    return {{"event_id", packet.messageId},
            {"acquisition_id", packet.acquisitionId},
            {"reply_to_packet_id", packet.replyToPacketId},
            {"event_type", "packet.egress"},
            {"schema_version", std::string(kMessageSchemaVersion)},
            {"message_id", packet.messageId},
            {"worker_instance_id", packet.workerInstanceId},
            {"causation_id", packet.causationId},
            {"connection_id", packet.connectionId},
            {"session_epoch", std::to_string(packet.sessionEpoch)},
            {"created_at_ms", std::to_string(packet.createdAtMs)},
            {"payload_hex", toHex(packet.payload)}};
}

inline EgressPacket egressFrom(const StreamMessage& message) {
    const auto require = [&message](std::string_view name) {
        const auto value = message.get(name);
        if (value.empty())
            throw std::runtime_error("Missing egress field: " + std::string(name));
        return value;
    };
    const auto integer = [&require](std::string_view name) {
        const auto value = require(name);
        std::int64_t result = 0;
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (error != std::errc{} || end != value.data() + value.size() || result < 0)
            throw std::runtime_error("Invalid egress integer field: " + std::string(name));
        return result;
    };
    EgressPacket packet;
    packet.acquisitionId = std::string(require("acquisition_id"));
    packet.replyToPacketId = std::string(message.get("reply_to_packet_id"));
    packet.messageId = std::string(require("message_id"));
    packet.workerInstanceId = std::string(require("worker_instance_id"));
    packet.causationId = std::string(message.get("causation_id"));
    packet.connectionId = std::string(require("connection_id"));
    packet.sessionEpoch = static_cast<std::uint64_t>(integer("session_epoch"));
    packet.createdAtMs = integer("created_at_ms");
    const auto payload = require("payload_hex");
    packet.payload = fromHex(payload);
    if (packet.payload.empty())
        throw std::runtime_error("Invalid or empty egress HEX payload");
    return packet;
}

inline std::vector<StreamField> parsedFields(const ParsedDeviceMessage& message) {
    std::string packetIds = "[";
    for (const auto& id : message.rawPacketIds) {
        if (packetIds.size() > 1) packetIds += ',';
        packetIds += service::utils::jsonQuoted(id);
    }
    packetIds += ']';
    return {{"event_id", message.messageId},
            {"acquisition_id", message.acquisitionId},
            {"raw_packet_ids", packetIds},
            {"event_type", "device.data.parsed"},
            {"schema_version", std::string(kMessageSchemaVersion)},
            {"aggregate_id", message.deviceId},
            {"model_id", message.modelId},
             {"event_kind", message.eventKind},
            {"message_id", message.messageId},
            {"causation_id", message.causationId},
            {"link_id", message.linkId},
            {"device_id", message.deviceId},
            {"device_code", message.deviceCode},
            {"protocol", message.protocol},
            {"connection_id", message.connectionId},
            {"occurred_at_ms", std::to_string(message.occurredAtMs)},
            {"observed_at_ms", std::to_string(message.observedAtMs)},
            {"storage_policy", message.storagePolicy},
            {"online_window_ms", std::to_string(message.onlineWindowMs)},
            {"source", message.source},
            {"values_json", message.valuesJson},
            {"raw_payload_hex", rawPayloadsJson(message.rawPayloads)}};
}

inline ParsedDeviceMessage parsedFrom(const StreamMessage& message) {
    const auto require = [&message](std::string_view name) {
        const auto value = message.get(name);
        if (value.empty())
            throw std::runtime_error("Missing parsed packet field: " + std::string(name));
        return value;
    };
    const auto integer = [&require](std::string_view name) {
        const auto value = require(name);
        std::int64_t result = 0;
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (error != std::errc{} || end != value.data() + value.size())
            throw std::runtime_error("Invalid parsed packet integer: " + std::string(name));
        return result;
    };
    ParsedDeviceMessage parsed;
    parsed.acquisitionId = std::string(require("acquisition_id"));
    const auto packetIds = message.get("raw_packet_ids");
    if (!packetIds.empty()) {
        const auto array = ruvia::json_value::parse(packetIds);
        if (!array || !array->is_array()) throw std::runtime_error("Invalid raw packet IDs");
        const std::string document = "{\"ids\":" + std::string(packetIds) + "}";
        const auto object = ruvia::json_value::parse(document);
        const auto ids = object ? object->get<ruvia::array<ruvia::string>>("ids") : std::nullopt;
        if (!ids) throw std::runtime_error("Invalid raw packet IDs");
        for (const auto& id : *ids) parsed.rawPacketIds.emplace_back(id.view());
    }
    parsed.eventKind = message.get("event_kind").empty() ? "sample" : std::string(message.get("event_kind"));
    if (parsed.eventKind != "sample" && parsed.eventKind != "image")
        throw std::runtime_error("Invalid telemetry event kind");
    parsed.modelId = std::string(message.get("model_id"));
    if (!parsed.modelId.empty() && !service::common::isUuid(parsed.modelId))
        throw std::runtime_error("Invalid telemetry model UUID");
    parsed.messageId = std::string(require("message_id"));
    parsed.causationId = std::string(require("causation_id"));
    parsed.linkId = std::string(require("link_id"));
    parsed.deviceId = std::string(require("device_id"));
    parsed.deviceCode = std::string(require("device_code"));
    parsed.protocol = std::string(require("protocol"));
    parsed.connectionId = std::string(require("connection_id"));
    parsed.occurredAtMs = integer("occurred_at_ms");
    parsed.observedAtMs = integer("observed_at_ms");
    parsed.observedAtMs = effectiveObservedAt(parsed.observedAtMs, parsed.occurredAtMs);
    parsed.storagePolicy = std::string(require("storage_policy"));
    if (parsed.storagePolicy != "report" && parsed.storagePolicy != "change")
        throw std::runtime_error("Invalid parsed packet storage policy");
    const auto onlineWindow = message.get("online_window_ms");
    if (!onlineWindow.empty()) {
        std::int64_t result = 0;
        const auto [end, error] =
            std::from_chars(onlineWindow.data(), onlineWindow.data() + onlineWindow.size(), result);
        if (error != std::errc{} || end != onlineWindow.data() + onlineWindow.size())
            throw std::runtime_error("Invalid parsed packet online window");
        parsed.onlineWindowMs = std::clamp<std::int64_t>(result, 1000, 2592000000LL);
    }
    parsed.source = std::string(require("source"));
    parsed.valuesJson = std::string(require("values_json"));
    const auto payload = require("raw_payload_hex");
    parsed.rawPayloads = rawPayloadsFromJson(payload);
    if (parsed.rawPayloads.empty() && payload != "[]")
        throw std::runtime_error("Invalid parsed packet HEX payload array");
    return parsed;
}

inline std::vector<StreamField> protocolTaskFields(const ProtocolTask& task) {
    std::vector<StreamField> fields{{"event_id", task.messageId},
                                    {"event_type", "protocol.task"},
                                    {"schema_version", std::string(kMessageSchemaVersion)},
                                    {"aggregate_id", task.deviceId},
                                    {"message_id", task.messageId},
                                    {"causation_id", task.causationId},
                                    {"group_key", task.groupKey},
                                    {"protocol", task.protocol},
                                    {"transport", task.transport},
                                    {"kind", task.kind},
                                    {"link_id", task.linkId},
                                    {"device_id", task.deviceId},
                                    {"device_code", task.deviceCode},
                                    {"connection_id", task.connectionId},
                                    {"payload_hex", task.payload},
                                    {"readback_payload_hex", task.readbackPayload},
                                    {"expected_readback_hex", task.expectedReadbackData},
                                    {"expected_value", task.expectedValue},
                                    {"expects_response", task.expectsResponse ? "1" : "0"},
                                    {"response_timeout_ms", std::to_string(task.responseTimeoutMs)},
                                    {"created_at_ms", std::to_string(task.createdAtMs)},
                                    {"attempt", std::to_string(task.attempt)},
                                    {"max_attempts", std::to_string(task.maxAttempts)},
                                    {"session_epoch", std::to_string(task.sessionEpoch)}};
    if (!task.mqttMessageId.empty()) fields.push_back({"mqtt_message_id", task.mqttMessageId});
    fields.reserve(fields.size() + task.elements.size());
    for (const auto& [elementId, value] : task.elements)
        fields.push_back({"element:" + elementId, value});
    return fields;
}

inline ProtocolTask protocolTaskFrom(const StreamMessage& message) {
    const auto require = [&message](std::string_view name) {
        const auto value = message.get(name);
        if (value.empty())
            throw std::runtime_error("Missing protocol task field: " + std::string(name));
        return value;
    };
    const auto integer = [&message, &require](std::string_view name, bool required = true) {
        const auto value = message.get(name);
        if (value.empty()) {
            if (required)
                (void)require(name);
            return std::int64_t{0};
        }
        std::int64_t result = 0;
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (error != std::errc{} || end != value.data() + value.size())
            throw std::runtime_error("Invalid protocol task integer field: " + std::string(name));
        return result;
    };
    ProtocolTask task;
    task.messageId = std::string(require("message_id"));
    task.causationId = std::string(message.get("causation_id"));
    task.groupKey = std::string(require("group_key"));
    task.protocol = std::string(require("protocol"));
    task.transport = std::string(require("transport"));
    if (task.transport != "TCP" && task.transport != "RTU" && task.transport != "RAW")
        throw std::runtime_error("Invalid protocol task transport");
    task.kind = std::string(require("kind"));
    task.linkId = std::string(require("link_id"));
    task.deviceId = std::string(message.get("device_id"));
    task.deviceCode = std::string(message.get("device_code"));
    task.connectionId = std::string(message.get("connection_id"));
    task.payload = std::string(message.get("payload_hex"));
    task.readbackPayload = std::string(message.get("readback_payload_hex"));
    task.expectedReadbackData = std::string(message.get("expected_readback_hex"));
    task.expectedValue = std::string(message.get("expected_value"));
    task.mqttMessageId = std::string(message.get("mqtt_message_id"));
    for (const auto& current : message.fields) {
        if (!current.name.starts_with("element:"))
            continue;
        const auto elementId = std::string_view(current.name).substr(8);
        if (elementId.empty())
            throw std::runtime_error("Invalid protocol task element field");
        task.elements.emplace_back(elementId, current.value);
    }
    if (task.payload.empty() && task.elements.empty())
        throw std::runtime_error("Protocol task requires payload_hex or elements");
    const auto expectsResponse = message.get("expects_response");
    task.expectsResponse = expectsResponse.empty() || expectsResponse == "1";
    task.responseTimeoutMs = integer("response_timeout_ms", false);
    if (task.responseTimeoutMs == 0)
        task.responseTimeoutMs = 3000;
    task.createdAtMs = integer("created_at_ms");
    task.attempt = integer("attempt", false);
    if (task.attempt == 0)
        task.attempt = 1;
    task.maxAttempts = integer("max_attempts", false);
    if (task.maxAttempts == 0)
        task.maxAttempts = 3;
    task.attempt = std::clamp<std::int64_t>(task.attempt, 1, 100);
    task.maxAttempts = std::clamp<std::int64_t>(task.maxAttempts, task.attempt, 100);
    task.sessionEpoch = static_cast<std::uint64_t>(integer("session_epoch", false));
    return task;
}

} // namespace service::message

namespace service::message::live {

// The live stream carries invalidation notices. Consumers rebuild their own
// authorized snapshot and never treat a stream entry as response data.
inline constexpr std::string_view kChanges{"iot:live:changes"};
inline constexpr std::string_view kTopicField{"topic"};
inline constexpr std::string_view kSchemaVersionField{"schema_version"};
inline constexpr std::string_view kSchemaVersion{"1"};

} // namespace service::message::live

namespace service::message::rpc {

struct Contract final {
    static constexpr std::string_view version = "1";
    static constexpr std::size_t maximumPayload = 1024 * 1024;
    static constexpr auto timeout = std::chrono::seconds(120);
    static constexpr std::string_view replyLifetime = "600";

    static std::string requests(std::string_view instance, std::size_t workerIndex) {
        return "iot:rpc:requests:" + std::string(instance) + ":worker:" + std::to_string(workerIndex);
    }
    static std::string reply(std::string_view id) {
        return "iot:rpc:reply:" + std::string(id);
    }
    static std::string cancelled(std::string_view id) {
        return "iot:rpc:cancelled:" + std::string(id);
    }
    static std::string claim(std::string_view id) {
        return "iot:rpc:claim:" + std::string(id);
    }
    static std::int64_t now() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
    static std::string success(std::string_view payload) {
        return "OK\n" + std::string(payload);
    }
    static std::string failure(std::uint16_t status, std::string_view code,
                               std::string_view message) {
        return "ERR\n" + std::to_string(status) + "\n" + std::string(code) + "\n" +
               std::string(message);
    }
    // XADD argument vector of one request entry on `requests(instance, worker)`.
    static std::vector<std::string> requestArguments(std::string_view stream, std::string_view id,
                                                     std::string_view component,
                                                     std::string_view operation,
                                                     std::string payload,
                                                     std::int64_t deadline) {
        return {"XADD", std::string(stream), "*", "version", std::string(version),
                "id", std::string(id), "component", std::string(component),
                "operation", std::string(operation), "payload", std::move(payload),
                "deadline", std::to_string(deadline)};
    }
};

// Serializes an RPC request or reply model. Every JSON RPC payload is a model below.
template <typename Model>
std::string encode(const Model& model, std::pmr::memory_resource* resource) {
    const auto encoded = ruvia::to_json(model, {.resource_ = resource});
    return {encoded.data(), encoded.size()};
}

// Text payload "<first>\n<second>"; the first part never contains a newline.
struct LinePair final {
    std::string_view first;
    std::string_view second;
};

inline std::string encodeLinePair(std::string_view first, std::string_view second) {
    std::string result;
    result.reserve(first.size() + second.size() + 1);
    result.append(first).push_back('\n');
    result.append(second);
    return result;
}

inline std::optional<LinePair> decodeLinePair(std::string_view payload) {
    const auto separator = payload.find('\n');
    if (separator == std::string_view::npos)
        return std::nullopt;
    return LinePair{payload.substr(0, separator), payload.substr(separator + 1)};
}

} // namespace service::message::rpc

namespace service::message::lock {

// PostgreSQL advisory lock identities shared by every Service Worker and process.
// Values are runtime contracts: changing one breaks mutual exclusion during upgrades.
inline constexpr std::int64_t kConfigurationProjection{5282804697543808067};
// Default VPN network row: Hub key initialization and default network provisioning.
inline constexpr std::int64_t kVpnDefaultNetwork{5282804697543808073};
inline constexpr std::int64_t kVpnAddressAllocation{5282804697543808068};
inline constexpr std::int64_t kVpnReconciliation{5282804697543808071};
inline constexpr std::int64_t kAccessSessionProjection{734622};
inline constexpr std::int64_t kAlertMetadataProjection{734623};
// Two-key form pg_advisory_xact_lock(int4, int4) guarding firmware storage.
inline constexpr std::int32_t kFirmwareStorageClass{17011};
inline constexpr std::int32_t kFirmwareStorageObject{1};
// Seeds for hashtextextended(<resource>, seed), which derives per-resource lock keys.
inline constexpr std::int64_t kVpnEdgeConfigSeed{5282804697543808072};
inline constexpr std::int64_t kTelemetryDeviceSeed{734621};
inline constexpr std::int64_t kGb28181OwnerSeed{28181};
inline constexpr std::int64_t kCommandIdempotencySeed{0};

} // namespace service::message::lock

namespace service::message::configuration {

inline std::string runtimeChangesStream() {
    return "iot:channel:runtime:config-work";
}

} // namespace service::message::configuration

namespace service::message::packet_log {

// Bounded debug packet indexes written by the packet log feature and read by modules.
inline std::string linkIndexKey(std::string_view linkId) {
    return "iot:debug:v4:link:" + std::string(linkId);
}

inline std::string deviceIndexKey(std::string_view deviceId) {
    return "iot:debug:v4:device:" + std::string(deviceId);
}

} // namespace service::message::packet_log

namespace service::message::collector {

inline constexpr std::int64_t kConnectionTtlMilliseconds = 15000;
inline constexpr std::string_view kConnectionPrefix{"iot:v3:device:connection:"};
inline constexpr std::string_view kConnectionDeadlinesKey{"iot:v3:device:connection:deadlines"};
inline constexpr std::string_view kLinkOwnerPrefix{"iot:v2:owner:link:"};

inline std::string connectionKey(std::string_view deviceId) {
    return std::string(kConnectionPrefix) + std::string(deviceId);
}

inline std::string linkOwnerKey(std::string_view linkId) {
    return std::string(kLinkOwnerPrefix) + std::string(linkId);
}

// Per-Worker link runtime snapshots: <prefix><instance>:<worker index>.
inline std::string linkRuntimePrefix(std::string_view linkId) {
    return "iot:runtime:link:" + std::string(linkId) + ":worker:";
}

} // namespace service::message::collector

namespace service::message::telemetry {

inline std::string latestKey(std::string_view deviceId) {
    return "iot:v2:device:" + std::string(deviceId) + ":latest";
}

inline std::string runtimeKey(std::string_view deviceId) {
    return "iot:v2:runtime:device:" + std::string(deviceId);
}

inline std::string canonicalPointText(std::string_view value, std::string_view dataType) {
    if (dataType != "BOOL") return std::string(value);
    if (value == "true" || value == "1") return "1";
    if (value == "false" || value == "0") return "0";
    return std::string(value);
}

inline std::string canonicalPointJson(std::string_view value, std::string_view dataType) {
    if (dataType != "BOOL") return std::string(value);
    if (value == "true" || value == "1") return "1";
    if (value == "false" || value == "0") return "0";
    if (value == "\"true\"" || value == "\"1\"") return "\"1\"";
    if (value == "\"false\"" || value == "\"0\"") return "\"0\"";
    return std::string(value);
}

} // namespace service::message::telemetry

namespace service::message::access::session {

inline constexpr std::string_view kActiveVersionKey{"iot:open-access:session:active"};
inline constexpr std::string_view kVersionPrefix{"iot:open-access:session:version:"};

inline std::string encode(std::string_view id, std::string_view name,
                          std::string_view status, std::string_view expiresAtMs,
                          std::string_view scopes, std::string_view deviceIds) {
    std::string result;
    result.reserve(id.size() + name.size() + status.size() + expiresAtMs.size() +
                   scopes.size() + deviceIds.size() + 5);
    for (const auto field : {id, name, status, expiresAtMs, scopes, deviceIds}) {
        if (!result.empty())
            result.push_back('\0');
        result.append(field);
    }
    return result;
}

struct Entry final {
    std::string id;
    std::string name;
    std::string status;
    std::int64_t expiresAtMs{0};
    std::set<std::string, std::less<>> scopes;
    std::set<std::string, std::less<>> deviceIds;
};

inline std::vector<std::string> parseStringArray(std::string_view json) {
    std::vector<std::string> result;
    const auto array = ruvia::json_value::parse(json);
    if (!array || !array->is_array()) throw std::runtime_error("invalid projected access-session array");
    const std::string document = "{\"values\":" + std::string(json) + "}";
    const auto object = ruvia::json_value::parse(document);
    const auto parsed = object ? object->get<ruvia::array<ruvia::string>>("values") : std::nullopt;
    if (!parsed)
        throw std::runtime_error("invalid projected access-session array");
    result.reserve(parsed->size());
    for (const auto& item : *parsed)
        result.emplace_back(item.view());
    return result;
}

inline Entry decode(std::string_view encoded) {
    std::vector<std::string_view> fields;
    fields.reserve(6);
    std::size_t begin = 0;
    while (fields.size() < 5) {
        const auto end = encoded.find('\0', begin);
        if (end == std::string_view::npos)
            throw std::runtime_error("invalid projected access-session entry");
        fields.push_back(encoded.substr(begin, end - begin));
        begin = end + 1;
    }
    fields.push_back(encoded.substr(begin));
    Entry result;
    result.id.assign(fields[0]);
    result.name.assign(fields[1]);
    result.status.assign(fields[2]);
    const auto expiration = fields[3];
    const auto [end, error] =
        std::from_chars(expiration.data(), expiration.data() + expiration.size(), result.expiresAtMs);
    if (expiration.empty() || error != std::errc{} || end != expiration.data() + expiration.size())
        throw std::runtime_error("invalid projected access-session expiration");
    for (auto& scope : parseStringArray(fields[4]))
        result.scopes.emplace(std::move(scope));
    for (auto& device : parseStringArray(fields[5]))
        result.deviceIds.emplace(std::move(device));
    return result;
}

inline bool expired(const Entry& entry, std::int64_t nowMs) noexcept {
    return entry.expiresAtMs > 0 && entry.expiresAtMs <= nowMs;
}

} // namespace service::message::access::session

namespace service::message::realtime {

struct Point final {
    std::string id;
    std::string name;
    std::string unit;
    bool operator==(const Point&) const = default;
};

struct Device final {
    std::string id;
    std::string code;
    std::string name;
    std::vector<Point> points;
    bool operator==(const Device&) const = default;
};

inline std::string encodePoint(const Point& point) {
    std::string value;
    value.reserve(point.id.size() + point.name.size() + point.unit.size() + 2);
    value.append(point.id);
    value.push_back('\0');
    value.append(point.name);
    value.push_back('\0');
    value.append(point.unit);
    return value;
}

inline std::optional<Point> decodePoint(std::string_view value) {
    const auto first = value.find('\0');
    if (first == std::string_view::npos)
        return std::nullopt;
    const auto second = value.find('\0', first + 1);
    if (second == std::string_view::npos)
        return std::nullopt;
    return Point{std::string(value.substr(0, first)),
                 std::string(value.substr(first + 1, second - first - 1)),
                 std::string(value.substr(second + 1))};
}

} // namespace realtime

namespace service::message::projection {
inline constexpr std::string_view kRuntimeActiveVersionKey = "iot:config:runtime:active-version";
inline constexpr std::string_view kRuntimeVersionsKey = "iot:config:runtime:versions";
inline constexpr std::string_view kRuntimeVersionPrefix = "iot:config:runtime:";
inline std::string realtimeDeviceKey(std::string_view version, std::string_view deviceId) {
    return std::string(kRuntimeVersionPrefix) + std::string(version) + ":realtime-device:" + std::string(deviceId);
}

inline std::string realtimeDevicePointsKey(std::string_view version,
                                           std::string_view deviceId) {
    return realtimeDeviceKey(version, deviceId) + ":points";
}

} // namespace service::message::projection

namespace service::message::worker_metrics {

// One atomic, expiring entity per Worker; readers use the same wire columns.
inline constexpr char kSnapshotTable[] = "iot_worker_snapshot";
inline constexpr std::chrono::milliseconds kSnapshotTtl{15000};
inline std::string snapshotId(std::size_t workerIndex,
    std::string_view instance) {
    return std::string(instance) + ":worker:" + std::to_string(workerIndex);
}

} // namespace service::message::worker_metrics

namespace service::message {

enum class WorkerStreamTask : std::uint8_t {
    Telemetry,
    TelemetryHistory,
    TelemetryLatest,
    TelemetryAlerts,
    TelemetryDelivery,
    Freshness,
    FreshnessAlerts,
    CommandResult,
    Webhook,
    Reconciler,
    EdgeProjector,
    EdgeDispatcher,
    Count,
};

inline constexpr std::string_view kWorkerWakeStreamPrefix{ "iot:service:worker:" };
inline constexpr std::size_t kWorkerWakeCapacity = 100000;

inline std::string workerWakeStream(std::size_t workerIndex, std::string_view instance) {
    return std::string(kWorkerWakeStreamPrefix) + std::string(instance) + ":" + std::to_string(workerIndex) + ":wake";
}

inline std::string sharedWakeStream(std::string_view instance) {
    return "iot:service:" + std::string(instance) + ":work-available";
}

inline std::string workerWakeStream(std::optional<std::size_t> workerIndex, std::string_view instance) {
    return workerIndex ? workerWakeStream(*workerIndex, instance) : sharedWakeStream(instance);
}

inline constexpr std::array<std::string_view, static_cast<std::size_t>(WorkerStreamTask::Count)>
    kWorkerStreamTaskNames{ "telemetry", "telemetry-history", "telemetry-latest", "telemetry-alerts", "telemetry-delivery", "freshness", "freshness-alerts", "command-result", "webhook", "reconciler", "edge-projector", "edge-dispatcher" };

inline constexpr std::string_view workerStreamTaskName(WorkerStreamTask task) {
    return kWorkerStreamTaskNames.at(static_cast<std::size_t>(task));
}

inline std::optional<WorkerStreamTask> workerStreamTask(std::string_view name) {
    for (std::size_t index = 0; index < kWorkerStreamTaskNames.size(); ++index) {
        if (kWorkerStreamTaskNames[index] == name) {
            return static_cast<WorkerStreamTask>(index);
        }
    }
    return std::nullopt;
}

} // namespace service::message

namespace service::message::edge::dispatch {

inline constexpr std::string_view kGroup{ "iot-engine:edge-dispatch" };
inline constexpr std::string_view kNodeKind{ "node" };

inline std::string stream(std::size_t workerIndex, std::string_view instance) {
    return "iot:v2:edge:dispatch:" + std::string(instance) + ":" + std::to_string(workerIndex);
}

struct Event final {
    std::string kind;
    std::string nodeId;
};

inline Event eventFrom(const service::message::StreamMessage& message) {
    return {
        .kind = std::string(message.get("kind")),
        .nodeId = std::string(message.get("node_id")),
    };
}

} // namespace service::message::edge::dispatch

namespace service::message::terminal {

inline std::string terminalEventTopic(std::string_view nodeId, std::string_view terminalId) {
    return "edge-terminal:" + std::string(nodeId) + ":" + std::string(terminalId);
}
inline std::string terminalAckTopic(std::string_view nodeId, std::string_view terminalId) {
    return terminalEventTopic(nodeId, terminalId) + ":input-ack";
}

inline std::string terminalInputKey(std::string_view nodeId) {
        return "iot:edge:terminal:in:" + std::string(nodeId);
    }
inline std::string terminalOutputKey(std::string_view nodeId, std::string_view terminalId) {
        return "iot:edge:terminal:out:" + std::string(nodeId) + ":" +
            std::string(terminalId);
    }
} // namespace service::message::terminal

namespace service::message::access::stream {

inline constexpr std::string_view kEventBase{ "iot:channel:open-access:event" };
inline constexpr std::string_view kAuditBase{ "iot:channel:open-access:audit" };
inline constexpr std::string_view kDeliveryResultBase{
    "iot:channel:open-access:delivery-result"
};
inline constexpr std::string_view kCatalogChangesBase{
    "iot:channel:open-access:config-change"
};
inline constexpr std::string_view kSessionChangesBase{
    "iot:channel:open-access:session-change"
};

inline std::string event() {
    return std::string(kEventBase);
}

inline std::string audit() {
    return std::string(kAuditBase);
}

inline std::string deliveryResult() {
    return std::string(kDeliveryResultBase);
}

inline std::string catalogChanges(std::size_t workerIndex) {
    return std::string(kCatalogChangesBase) + ":" + std::to_string(workerIndex);
}

inline std::string sessionChanges() {
    return std::string(kSessionChangesBase);
}

} // namespace service::message::access::stream

namespace service::message::edge::projector_stream {

inline constexpr std::string_view kIngressKind{ "ingress" };
inline constexpr std::string_view kMetadataKind{ "metadata" };
inline constexpr std::string_view kStreamPrefix{ "iot:v3:edge:projector:" };
inline constexpr std::string_view kLeasePrefix{ "iot:v3:edge:projector:lease:" };
inline constexpr std::string_view kStreamRegistry{ "iot:v3:edge:projector:streams" };

inline std::string stream(std::size_t workerIndex, std::string_view instance) {
    return std::string(kStreamPrefix) + std::string(instance) + ":" + std::to_string(workerIndex);
}

inline std::string leaseKey(std::size_t workerIndex, std::string_view instance) {
    return std::string(kLeasePrefix) + std::string(instance) + ":" +
        std::to_string(workerIndex);
}

inline std::string ownerToken(std::size_t workerIndex, std::string_view instance) {
    return std::string(instance) + ":" + std::to_string(workerIndex);
}

} // namespace service::message::edge::projector_stream

namespace service::message::edge::firmware {

// Firmware storage cleanup is scheduled per Service Worker from its earliest
// deadline (edge_firmware/edge_task plus its own firmware directory). This RPC
// asks the receiving Worker to recompute that deadline after a change that can
// make it earlier. It is appended to the caller's own Worker request stream
// (service::message::rpc::Contract::requests), the payload is empty and the
// reply body is empty.
inline constexpr std::string_view kRpcComponent{ "edge-firmware" };
// Senders: modules/edge_node after a firmware upload or EdgeNode deletion
// finishes (service::rpc::call), and the Edge projector after a firmware
// edge_task transitions to succeeded/failed (appended without waiting).
inline constexpr std::string_view kScheduleCleanupOperation{ "schedule-cleanup" };

} // namespace service::message::edge::firmware


namespace service::message::command {

// command.prepare: Service module request and feature reply.
RUVIA_MODEL(PrepareRequest,
    RUVIA_REQUIRED_FIELD(deviceId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(mqttMessageId, ruvia::string),
    RUVIA_REQUIRED_FIELD(elements, ruvia::array<ruvia::array<ruvia::string>>));

RUVIA_MODEL(PreparedElement,
    RUVIA_REQUIRED_FIELD(elementId, ruvia::string),
    RUVIA_REQUIRED_FIELD(value, ruvia::string));

RUVIA_MODEL(PreparedCommand,
    RUVIA_REQUIRED_FIELD(id, ruvia::string),
    RUVIA_REQUIRED_FIELD(deviceId, ruvia::string),
    RUVIA_REQUIRED_FIELD(deviceCode, ruvia::string),
    RUVIA_REQUIRED_FIELD(protocol, ruvia::string),
    RUVIA_REQUIRED_FIELD(elements, ruvia::boxed_array<PreparedElement>),
    RUVIA_REQUIRED_FIELD(payload, ruvia::array<ruvia::string>));

RUVIA_MODEL(PreparedBatch,
    RUVIA_REQUIRED_FIELD(queue, ruvia::string),
    RUVIA_REQUIRED_FIELD(kind, ruvia::string),
    RUVIA_REQUIRED_FIELD(maximum, ruvia::int64),
    RUVIA_REQUIRED_FIELD(nodeId, ruvia::string),
    RUVIA_REQUIRED_FIELD(commands, ruvia::boxed_array<PreparedCommand>));

} // namespace service::message::command

namespace service::message::gb28181 {

// The GB28181 module sends ControlRequest; the feature forwards it unchanged to the
// owning Collector. Replies carry code, message and an operation-specific data model.
RUVIA_MODEL(ControlRequest,
    RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("channel_id", channelId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("stream_id", streamId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("session_id", sessionId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(action, ruvia::string),
    RUVIA_OPTIONAL_FIELD(speed, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(pan, ruvia::double_value),
    RUVIA_OPTIONAL_FIELD(tilt, ruvia::double_value),
    RUVIA_OPTIONAL_FIELD(zoom, ruvia::double_value));

RUVIA_MODEL(MediaPorts,
    RUVIA_OPTIONAL_FIELD(http, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(https, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtsp, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtsps, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtmp, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtmps, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtc, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(srt, ruvia::int64));

RUVIA_MODEL(MediaCapabilities,
    RUVIA_OPTIONAL_FIELD(faac, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(ffmpeg, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(hls, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(mp4, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("rtp_proxy", rtpProxy, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(srt, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(sctp, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("web_rtc", webRtc, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(x264, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("video_stack", videoStack, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(tls, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(recording, ruvia::bool_value));

RUVIA_MODEL(Health,
    RUVIA_OPTIONAL_FIELD(status, ruvia::string),
    RUVIA_OPTIONAL_FIELD(service, ruvia::string),
    RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(started, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(error, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("media_ports", mediaPorts, MediaPorts),
    RUVIA_OPTIONAL_FIELD_NAME("media_capabilities", mediaCapabilities, MediaCapabilities));

RUVIA_MODEL(Channel,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("reported_name", reportedName, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("custom_name", customName, ruvia::string),
    RUVIA_OPTIONAL_FIELD(manufacturer, ruvia::string),
    RUVIA_OPTIONAL_FIELD(online, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("ptz_type", ptzType, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("ptz_capable", ptzCapable, ruvia::bool_value));

RUVIA_MODEL(Device,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("reported_name", reportedName, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("custom_name", customName, ruvia::string),
    RUVIA_OPTIONAL_FIELD(manufacturer, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("remote_address", remoteAddress, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("remote_ip", remoteIp, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("remote_port", remotePort, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("registration_source", registrationSource, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("last_seen_at", lastSeenAt, ruvia::string),
    RUVIA_OPTIONAL_FIELD(online, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(channels, ruvia::boxed_array<Channel>));

RUVIA_MODEL(DeviceList, RUVIA_OPTIONAL_FIELD(items, ruvia::boxed_array<Device>));

RUVIA_MODEL(PlayUrls,
    RUVIA_OPTIONAL_FIELD_NAME("http_flv", httpFlv, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("ws_flv", wsFlv, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("http_ts", httpTs, ruvia::string),
    RUVIA_OPTIONAL_FIELD(hls, ruvia::string),
    RUVIA_OPTIONAL_FIELD(webrtc, ruvia::string),
    RUVIA_OPTIONAL_FIELD(rtsp, ruvia::string),
    RUVIA_OPTIONAL_FIELD(rtmp, ruvia::string));

RUVIA_MODEL(PreviewStart,
    RUVIA_OPTIONAL_FIELD(sent, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("session_id", sessionId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("channel_id", channelId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("stream_id", streamId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(ssrc, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("rtp_port", rtpPort, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("lease_timeout_seconds", leaseTimeoutSeconds, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("play_urls", playUrls, PlayUrls));

RUVIA_MODEL(PreviewStop,
    RUVIA_OPTIONAL_FIELD(stopped, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("session_id", sessionId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("stream_id", streamId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("bye_sent", byeSent, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("rtp_server_closed", rtpServerClosed, ruvia::bool_value));

RUVIA_MODEL(Action,
    RUVIA_OPTIONAL_FIELD(sent, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("channel_id", channelId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(recording, ruvia::bool_value));

RUVIA_MODEL(OperationResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string));

RUVIA_MODEL(HealthResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, Health));

RUVIA_MODEL(DeviceListResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, DeviceList));

RUVIA_MODEL(PreviewStartResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, PreviewStart));

RUVIA_MODEL(PreviewStopResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, PreviewStop));

RUVIA_MODEL(ActionResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, Action));

} // namespace service::message::gb28181

namespace service::message::access {

// access.audit: open-access request audit forwarded by the module to the feature.
RUVIA_MODEL(AuditRequest,
    RUVIA_OPTIONAL_FIELD(action, ruvia::string),
    RUVIA_OPTIONAL_FIELD(accessKeyId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(method, ruvia::string),
    RUVIA_OPTIONAL_FIELD(target, ruvia::string),
    RUVIA_OPTIONAL_FIELD(requestIp, ruvia::string),
    RUVIA_OPTIONAL_FIELD(httpStatus, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(deviceId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(requestPayload, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(responsePayload, ruvia::json_value));

} // namespace service::message::access

namespace service::message::serial_debug {

// edge-serial: browser serial debug operations relayed by the edge node module.
RUVIA_MODEL(Request,
    RUVIA_REQUIRED_FIELD(nodeId, ruvia::string),
    RUVIA_REQUIRED_FIELD(userId, ruvia::string),
    RUVIA_REQUIRED_FIELD(connectionId, ruvia::string),
    RUVIA_REQUIRED_FIELD(sessionId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(path, ruvia::string),
    RUVIA_OPTIONAL_FIELD(after, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(command, ruvia::json_value));

// The browser command carried in Request.command.
RUVIA_MODEL(Command,
    RUVIA_OPTIONAL_FIELD(action, ruvia::string),
    RUVIA_OPTIONAL_FIELD(requestId, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(baudRate, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(dataBits, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(stopBits, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(parity, ruvia::string),
    RUVIA_OPTIONAL_FIELD(rs485, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(hex, ruvia::string));

RUVIA_MODEL(OpenReply, RUVIA_REQUIRED_FIELD(id, ruvia::string));

RUVIA_MODEL(Settings,
    RUVIA_REQUIRED_FIELD(path, ruvia::string),
    RUVIA_REQUIRED_FIELD(baudRate, ruvia::int64),
    RUVIA_REQUIRED_FIELD(dataBits, ruvia::int64),
    RUVIA_REQUIRED_FIELD(stopBits, ruvia::int64),
    RUVIA_REQUIRED_FIELD(parity, ruvia::string),
    RUVIA_REQUIRED_FIELD(rs485, ruvia::bool_value));

// One event stored in outputKey() and returned to the browser.
RUVIA_MODEL(Event,
    RUVIA_REQUIRED_FIELD(kind, ruvia::string),
    RUVIA_REQUIRED_FIELD(requestId, ruvia::int64),
    RUVIA_REQUIRED_FIELD(manual, ruvia::bool_value),
    RUVIA_REQUIRED_FIELD(direction, ruvia::string),
    RUVIA_REQUIRED_FIELD(hex, ruvia::string),
    RUVIA_REQUIRED_FIELD(timestamp, ruvia::int64),
    RUVIA_REQUIRED_FIELD(sequence, ruvia::int64),
    RUVIA_REQUIRED_FIELD(droppedBytes, ruvia::int64),
    RUVIA_REQUIRED_FIELD(message, ruvia::string),
    RUVIA_REQUIRED_FIELD(settings, Settings));

RUVIA_MODEL(EventsReply,
    RUVIA_REQUIRED_FIELD(events, ruvia::boxed_array<Event>),
    RUVIA_REQUIRED_FIELD(cursor, ruvia::int64));

} // namespace service::message::serial_debug

namespace service::message::terminal {

// edge-terminal: browser terminal operations relayed by the edge node module.
RUVIA_MODEL(Request,
    RUVIA_REQUIRED_FIELD(nodeId, ruvia::string),
    RUVIA_REQUIRED_FIELD(sessionId, ruvia::string),
    RUVIA_REQUIRED_FIELD(connectionId, ruvia::string),
    RUVIA_REQUIRED_FIELD(userId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(columns, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rows, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(content, ruvia::string),
    RUVIA_OPTIONAL_FIELD(sequence, ruvia::int64));

RUVIA_MODEL(OpenReply,
    RUVIA_REQUIRED_FIELD(id, ruvia::string),
    RUVIA_REQUIRED_FIELD(protocolVersion, ruvia::int64));

RUVIA_MODEL(Event,
    RUVIA_REQUIRED_FIELD(kind, ruvia::string),
    RUVIA_OPTIONAL_FIELD(reason, ruvia::string),
    RUVIA_OPTIONAL_FIELD(content, ruvia::string),
    RUVIA_OPTIONAL_FIELD(sequence, ruvia::int64));

RUVIA_MODEL(EventsReply,
    RUVIA_REQUIRED_FIELD(events, ruvia::boxed_array<Event>),
    RUVIA_REQUIRED_FIELD(more, ruvia::bool_value));

RUVIA_MODEL(InputStatusReply, RUVIA_REQUIRED_FIELD(acknowledged, ruvia::bool_value));

} // namespace service::message::terminal

namespace service::message::vpn {

// vpn.reconcile reply.
RUVIA_MODEL(RuntimeStatus,
    RUVIA_REQUIRED_FIELD(platformSupported, ruvia::bool_value),
    RUVIA_REQUIRED_FIELD(configured, ruvia::bool_value),
    RUVIA_REQUIRED_FIELD(code, ruvia::string),
    RUVIA_REQUIRED_FIELD(message, ruvia::string),
    RUVIA_REQUIRED_FIELD(runtimePeerCount, ruvia::int64));

inline constexpr std::string_view kRpcComponent{"vpn"};

// VPN module -> VPN feature operations.
// queue-edge-config: payload rpc::encodeLinePair(peerId, actorId).
inline constexpr std::string_view kQueueEdgeConfigOperation{"queue-edge-config"};
// reconcile: empty payload; replies RuntimeStatus.
inline constexpr std::string_view kReconcileOperation{"reconcile"};
// wireguard-remove-peer: payload is the WireGuard public key text.
inline constexpr std::string_view kRemovePeerOperation{"wireguard-remove-peer"};
// sync-edge-routes: payload EdgeRouteSync. The feature takes
// lock::kVpnAddressAllocation in its own transaction, so the caller must not
// hold that lock (or any transaction) while it waits for the reply.
inline constexpr std::string_view kSyncEdgeRoutesOperation{"sync-edge-routes"};

// Reconcile vpn_route with the Edge bridge inventory of one edge peer.
// actorId becomes vpn_route.created_by of newly mapped routes.
RUVIA_MODEL(EdgeRouteSync,
    RUVIA_REQUIRED_FIELD(peerId, ruvia::string),
    RUVIA_REQUIRED_FIELD(networkId, ruvia::string),
    RUVIA_REQUIRED_FIELD(edgeNodeId, ruvia::string),
    RUVIA_REQUIRED_FIELD(actorId, ruvia::string));

// Edge projection -> VPN feature notifications. The Edge projector appends them
// to its own Worker's RPC request stream and does not wait for the reply; the
// VPN handler logs failures itself because nobody reads the stored reply.
inline constexpr std::string_view kEdgeCapabilityOperation{"edge-capability"};
inline constexpr std::string_view kEdgeConfigResultOperation{"edge-config-result"};

// An approved EdgeNode reported VPN support with a managed WireGuard public key
// (common/wireguard.h validManagedKey). The VPN feature activates the edge peer,
// synchronizes bridge routes, queues the VPN config and reconciles the Hub.
RUVIA_MODEL(EdgeCapability,
    RUVIA_REQUIRED_FIELD(nodeId, ruvia::string),
    RUVIA_REQUIRED_FIELD(publicKey, ruvia::string));

// A VPN config task reached a terminal state on the Edge side. edge_task has
// already been transitioned by the Edge projector; the VPN feature projects the
// result onto vpn_route when configVersion still equals vpn_peer.config_revision.
RUVIA_MODEL(EdgeConfigResult,
    RUVIA_REQUIRED_FIELD(peerId, ruvia::string),
    RUVIA_REQUIRED_FIELD(configVersion, ruvia::int64),
    RUVIA_REQUIRED_FIELD(enabled, ruvia::bool_value),
    RUVIA_REQUIRED_FIELD(applied, ruvia::bool_value),
    RUVIA_REQUIRED_FIELD(errorMessage, ruvia::string));

} // namespace service::message::vpn