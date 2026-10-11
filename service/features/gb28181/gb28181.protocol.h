#pragma once

#include <cmath>
#include <set>
#include <utility>
#include <algorithm>
#include <stdexcept>
#include <vector>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

#include <ruvia/web/model.h>
#include <ruvia/web/model_json.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/features/gb28181/device/device.types.h"
#include "service/features/gb28181/media/media.types.h"
#include "service/features/gb28181/sip/sip.types.h"
#include "service/common/uuid.h"
#include "service/utils/json.h"

namespace service::gb28181::control_protocol {

namespace stream {

// Control streams are scoped to a process incarnation.  This prevents two
// processes with the same worker index from consuming each other's commands.
inline constexpr std::string_view kProjection{ "iot:gb28181:projection" };
inline constexpr std::string_view kProjectionGroup{ "iot-engine:gb28181-projector" };
inline constexpr std::string_view kControlPrefix{ "iot:gb28181:control:worker:" };
inline constexpr std::string_view kControlGroup{ "iot-engine:gb28181-control" };
inline constexpr std::string_view kConfigKey{ "iot:state:gb28181:config" };

// New control entries always carry request_id, owner_key, owner_token,
// operation, payload, reply_stream, result_key, deadline_ms and cancel_key.
// The owner token is a process/session fence; worker index only selects the
// already owning Collector stream and never partitions a device.

inline std::string control(
    std::size_t workerIndex,
    std::string_view instance
) {
    return std::string(kControlPrefix) + std::string(instance) + ":" +
        std::to_string(workerIndex);
}

inline std::string reply(std::string_view requestId) {
    return "iot:gb28181:reply:" + std::string(requestId);
}

inline std::string result(std::string_view requestId) {
    return "iot:gb28181:result:" + std::string(requestId);
}

inline std::string cancel(std::string_view requestId) {
    return "iot:gb28181:cancel:" + std::string(requestId);
}

inline std::string owner(std::string_view deviceId) {
    return "iot:gb28181:owner:" + std::string(deviceId);
}

inline std::string sessionOwner(std::string_view sessionId) {
    return "iot:gb28181:session-owner:" + std::string(sessionId);
}

inline std::string projectionDone(std::string_view projectionId) {
    return "iot:gb28181:projection:done:" + std::string(projectionId);
}

// Owner tokens are process-incarnation fences.  The suffix is the Collector
// index and is used only to select the already owning control stream; the
// process UUID and the session suffix prevent a restarted or stale connection
// from replaying a command for a new owner.
inline std::optional<std::size_t> ownerIndex(std::string_view token) {
    constexpr std::string_view marker{ ":collector:" };
    const auto markerPosition = token.rfind(marker);
    if (markerPosition == std::string_view::npos) {
        return std::nullopt;
    }
    auto indexText = token.substr(markerPosition + marker.size());
    if (const auto separator = indexText.find(':');
        separator != std::string_view::npos) {
        indexText = indexText.substr(0, separator);
    }
    if (indexText.empty()) {
        return std::nullopt;
    }
    std::size_t index{};
    const auto [end, error] =
        std::from_chars(indexText.data(), indexText.data() + indexText.size(), index);
    if (error != std::errc{} || end != indexText.data() + indexText.size()) {
        return std::nullopt;
    }
    return index;
}

inline std::optional<std::string_view> ownerInstance(std::string_view token) {
    constexpr std::string_view marker{ ":collector:" };
    const auto markerPosition = token.rfind(marker);
    if (markerPosition == std::string_view::npos || markerPosition == 0) {
        return std::nullopt;
    }
    const auto instance = token.substr(0, markerPosition);
    if (instance.empty()) {
        return std::nullopt;
    }
    return instance;
}

inline bool completeOwnerToken(std::string_view token) {
    const auto instance = ownerInstance(token);
    if (!instance || !service::common::isUuid(*instance)) {
        return false;
    }
    constexpr std::string_view marker{ ":collector:" };
    const auto markerPosition = token.rfind(marker);
    auto suffix = token.substr(markerPosition + marker.size());
    const auto indexSeparator = suffix.find(':');
    if (indexSeparator == std::string_view::npos) {
        return false;
    }
    suffix = suffix.substr(indexSeparator + 1);
    const auto generationSeparator = suffix.find(':');
    if (generationSeparator == std::string_view::npos) {
        return false;
    }
    const auto kind = suffix.substr(0, generationSeparator);
    if (kind != "session" && kind != "stream") {
        return false;
    }
    const auto generationText = suffix.substr(generationSeparator + 1);
    if (generationText.empty()) {
        return false;
    }
    std::uint64_t generation{};
    const auto [end, error] = std::from_chars(
        generationText.data(),
        generationText.data() + generationText.size(),
        generation
    );
    return error == std::errc{} &&
        end == generationText.data() + generationText.size() &&
        generation != 0;
}

} // namespace stream

// The northbound module and the GB28181 feature communicate with a small,
// deliberately boring JSON contract.  These models are feature-owned: the
// module keeps its HTTP DTOs independent and only parses the wire shape.
RUVIA_MODEL(
    ControlRequest,
    RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("channel_id", channelId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("stream_id", streamId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("session_id", sessionId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(action, ruvia::string),
    RUVIA_OPTIONAL_FIELD(speed, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(pan, ruvia::double_value),
    RUVIA_OPTIONAL_FIELD(tilt, ruvia::double_value),
    RUVIA_OPTIONAL_FIELD(zoom, ruvia::double_value)
);

RUVIA_MODEL(
    MediaPortsJson,
    RUVIA_OPTIONAL_FIELD(http, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(https, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtsp, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtsps, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtmp, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtmps, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(rtc, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(srt, ruvia::int64)
);

RUVIA_MODEL(
    MediaCapabilitiesJson,
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
    RUVIA_OPTIONAL_FIELD(recording, ruvia::bool_value)
);

RUVIA_MODEL(
    HealthJson,
    RUVIA_OPTIONAL_FIELD(status, ruvia::string),
    RUVIA_OPTIONAL_FIELD(service, ruvia::string),
    RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(started, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(error, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("media_ports", mediaPorts, MediaPortsJson),
    RUVIA_OPTIONAL_FIELD_NAME("media_capabilities", mediaCapabilities, MediaCapabilitiesJson)
);

RUVIA_MODEL(
    ChannelJson,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("reported_name", reportedName, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("custom_name", customName, ruvia::string),
    RUVIA_OPTIONAL_FIELD(manufacturer, ruvia::string),
    RUVIA_OPTIONAL_FIELD(online, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("ptz_type", ptzType, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("ptz_capable", ptzCapable, ruvia::bool_value)
);

RUVIA_MODEL(
    DeviceJson,
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
    RUVIA_OPTIONAL_FIELD(channels, ruvia::boxed_array<ChannelJson>)
);

RUVIA_MODEL(DeviceListJson, RUVIA_OPTIONAL_FIELD(items, ruvia::boxed_array<DeviceJson>));

inline std::optional<ControlRequest> parseRequest(
    std::string_view body,
    std::pmr::memory_resource* resource
) {
    return ruvia::from_json<ControlRequest>(body, { .resource_ = resource });
}

inline constexpr std::string_view kHealthOperation{ "health" };
inline constexpr std::string_view kDevicesOperation{ "devices" };
inline constexpr std::string_view kCatalogOperation{ "catalog" };
inline constexpr std::string_view kRenameDeviceOperation{ "rename_device" };
inline constexpr std::string_view kRenameChannelOperation{ "rename_channel" };
inline constexpr std::string_view kPreviewStartOperation{ "preview.start" };
inline constexpr std::string_view kPreviewStopOperation{ "preview.stop" };
inline constexpr std::string_view kPreviewHeartbeatOperation{
    "preview.heartbeat"
};
inline constexpr std::string_view kPtzOperation{ "ptz" };
inline constexpr std::string_view kPtzPositionOperation{ "ptz.position" };
inline constexpr std::string_view kRecordingOperation{ "recording" };
inline constexpr std::string_view kRecordingStartOperation{ "recording.start" };
inline constexpr std::string_view kRecordingStopOperation{ "recording.stop" };

inline std::string successJson(const auto& model, std::pmr::memory_resource* resource) {
    const auto encoded = ruvia::to_json(model, { .resource_ = resource });
    std::string result{ "{\"code\":0,\"message\":\"ok\",\"data\":" };
    result.append(encoded.data(), encoded.size());
    result.push_back('}');
    return result;
}

inline std::string operationJson(std::string_view message) {
    return "{\"code\":0,\"message\":" + service::utils::jsonQuoted(message) +
        "}";
}

} // namespace service::gb28181::control_protocol

namespace service::gb28181::control_protocol {

inline bool validProjectionStreamId(std::string_view value) {
    const auto separator = value.find('-');
    if (separator == std::string_view::npos || separator == 0 ||
        separator + 1 == value.size() ||
        value.find('-', separator + 1) != std::string_view::npos) {
        return false;
    }
    const auto unsignedInteger = [](std::string_view part) {
        std::uint64_t parsed{};
        const auto [end, error] = std::from_chars(
            part.data(),
            part.data() + part.size(),
            parsed
        );
        return error == std::errc{} && end == part.data() + part.size();
    };
    return unsignedInteger(value.substr(0, separator)) &&
        unsignedInteger(value.substr(separator + 1)) && value != "0-0";
}

} // namespace service::gb28181::control_protocol

namespace service::gb28181::projection_protocol {

inline std::string projectionChangeName(DeviceChange change) {
    switch (change) {
        case DeviceChange::Status:
            return "status";
        case DeviceChange::Catalog:
            return "catalog";
        case DeviceChange::DeviceName:
            return "device_name";
        case DeviceChange::ChannelName:
            return "channel_name";
    }
    return "status";
}

inline std::vector<service::message::StreamField>
deviceProjectionFields(const Device& device, DeviceChange change, std::size_t owner, std::string_view ownerToken) {
    using service::message::StreamField;
    std::vector<StreamField> fields{
        { "event_type", "gb28181.device" },
        { "schema_version", "1" },
        { "change", projectionChangeName(change) },
        { "aggregate_id", device.id },
        { "device_id", device.id },
        { "name", device.name },
        { "custom_name", device.customName },
        { "manufacturer", device.manufacturer },
        { "remote_address", device.remoteAddress },
        { "registration_source", device.registrationSource },
        { "online", device.online ? "1" : "0" },
        { "last_seen_at", service::common::utcTimestamp(device.lastSeen) },
        { "session_generation", std::to_string(device.sessionGeneration) },
        { "owner_index", std::to_string(owner) },
        { "owner_token", std::string(ownerToken) },
        { "channel_count", std::to_string(device.channels.size()) },
    };
    fields.reserve(fields.size() + device.channels.size() * 6U);
    for (std::size_t index = 0; index < device.channels.size(); ++index) {
        const auto& channel = device.channels[index];
        const auto prefix = "channel." + std::to_string(index) + ".";
        fields.emplace_back(prefix + "id", channel.id);
        fields.emplace_back(prefix + "name", channel.name);
        fields.emplace_back(prefix + "custom_name", channel.customName);
        fields.emplace_back(prefix + "manufacturer", channel.manufacturer);
        fields.emplace_back(prefix + "online", channel.online ? "1" : "0");
        fields.emplace_back(prefix + "ptz_type", std::to_string(channel.ptzType));
    }
    return fields;
}

inline std::vector<service::message::StreamField>
streamProjectionFields(const StreamStatus& stream, std::size_t owner, std::string_view ownerToken) {
    using service::message::StreamField;
    return {
        { "event_type", "gb28181.stream" },
        { "schema_version", "1" },
        { "aggregate_id", StreamStatus::identity(stream.app, stream.stream, stream.schema) },
        { "app", stream.app },
        { "stream", stream.stream },
        { "schema", stream.schema },
        { "online", stream.online ? "1" : "0" },
        { "reader_count", std::to_string(stream.readerCount) },
        { "owner_index", std::to_string(owner) },
        { "owner_token", std::string(ownerToken) },
    };
}

inline std::optional<std::int64_t> projectionInteger(std::string_view value) {
    std::int64_t parsed{};
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size()) {
        return std::nullopt;
    }
    return parsed;
}

inline bool projectionBoolean(std::string_view value) {
    return value == "t" || value == "true" || value == "1";
}

inline std::optional<DeviceChange> projectionDeviceChange(std::string_view value) {
    if (value == "status") {
        return DeviceChange::Status;
    }
    if (value == "catalog") {
        return DeviceChange::Catalog;
    }
    if (value == "device_name") {
        return DeviceChange::DeviceName;
    }
    if (value == "channel_name") {
        return DeviceChange::ChannelName;
    }
    return std::nullopt;
}

inline std::size_t projectionCount(std::string_view value, std::string_view field) {
    const auto parsed = projectionInteger(value);
    if (!parsed || *parsed < 0 || *parsed > 100000) {
        throw std::runtime_error("invalid GB28181 projection " + std::string(field));
    }
    return static_cast<std::size_t>(*parsed);
}

inline Device deviceFromProjection(const service::message::StreamMessage& message) {
    if (message.get("schema_version") != "1" ||
        message.get("event_type") != "gb28181.device") {
        throw std::runtime_error("unsupported GB28181 device projection");
    }
    Device device;
    device.id = std::string(message.get("device_id"));
    if (device.id.empty()) {
        throw std::runtime_error("GB28181 device projection has no id");
    }
    device.name = std::string(message.get("name"));
    device.customName = std::string(message.get("custom_name"));
    device.manufacturer = std::string(message.get("manufacturer"));
    device.remoteAddress = std::string(message.get("remote_address"));
    device.registrationSource = std::string(message.get("registration_source"));
    device.online = projectionBoolean(message.get("online"));
    if (const auto lastSeen = service::common::parseUtcTimestamp(
            message.get("last_seen_at")
        )) {
        device.lastSeen = *lastSeen;
    }
    if (const auto generation = projectionInteger(message.get("session_generation"))) {
        device.sessionGeneration = static_cast<std::uint64_t>(
            std::max<std::int64_t>(0, *generation)
        );
    }

    const auto channelCount = projectionCount(message.get("channel_count"), "channel_count");
    device.channels.reserve(channelCount);
    for (std::size_t index = 0; index < channelCount; ++index) {
        const auto prefix = "channel." + std::to_string(index) + ".";
        Channel channel;
        channel.id = std::string(message.get(prefix + "id"));
        channel.name = std::string(message.get(prefix + "name"));
        channel.customName = std::string(message.get(prefix + "custom_name"));
        channel.manufacturer =
            std::string(message.get(prefix + "manufacturer"));
        channel.online = projectionBoolean(message.get(prefix + "online"));
        channel.ptzType = static_cast<int>(projectionInteger(
                                               message.get(prefix + "ptz_type")
        )
                                               .value_or(-1));
        if (channel.id.empty()) {
            throw std::runtime_error("GB28181 projection channel has no id");
        }
        device.channels.push_back(std::move(channel));
    }
    return device;
}

inline StreamStatus streamFromProjection(const service::message::StreamMessage& message) {
    if (message.get("schema_version") != "1" ||
        message.get("event_type") != "gb28181.stream") {
        throw std::runtime_error("unsupported GB28181 stream projection");
    }
    StreamStatus stream;
    stream.app = std::string(message.get("app"));
    stream.stream = std::string(message.get("stream"));
    stream.schema = std::string(message.get("schema"));
    if (stream.app.empty() || stream.stream.empty() || stream.schema.empty()) {
        throw std::runtime_error("GB28181 stream projection is incomplete");
    }
    stream.online = projectionBoolean(message.get("online"));
    stream.readerCount = static_cast<int>(projectionInteger(
                                              message.get("reader_count")
    )
                                              .value_or(0));
    stream.readerCount = std::max(0, stream.readerCount);
    return stream;
}

} // namespace service::gb28181::projection_protocol

namespace service::gb28181::control_protocol {

inline std::string requiredText(const std::optional<ruvia::string>& value, std::string_view message) {
    if (!value || value->empty()) {
        service::common::fail(10001, std::string(message), 400);
    }
    return std::string(value->view());
}

inline std::int64_t requiredInteger(const std::optional<ruvia::int64>& value, std::string_view message) {
    if (!value) {
        service::common::fail(10001, std::string(message), 400);
    }
    return static_cast<std::int64_t>(*value);
}

inline double requiredFinite(const std::optional<ruvia::double_value>& value, std::string_view name, double minimum, double maximum) {
    if (!value || !std::isfinite(static_cast<double>(*value)) ||
        static_cast<double>(*value) < minimum ||
        static_cast<double>(*value) > maximum) {
        service::common::fail(10001, std::string(name) + " 超出允许范围", 400);
    }
    return static_cast<double>(*value);
}

inline void requireAction(std::string_view action) {
    static const std::set<std::string_view> actions{
        "left",
        "right",
        "up",
        "down",
        "zoomin",
        "zoomout",
        "stop"
    };
    if (!actions.contains(action)) {
        service::common::fail(10001, "不支持的云台动作", 400);
    }
}

inline std::pair<std::string, std::string> splitRemoteAddress(std::string_view address) {
    if (address.starts_with('[')) {
        const auto end = address.find(']');
        if (end != std::string_view::npos) {
            auto port = end + 1 < address.size() && address[end + 1] == ':'
                ? std::string(address.substr(end + 2))
                : std::string{};
            return { std::string(address.substr(1, end - 1)), std::move(port) };
        }
    }
    const auto colon = address.rfind(':');
    if (colon == std::string_view::npos || address.find(':') != colon) {
        return { std::string(address), {} };
    }
    return { std::string(address.substr(0, colon)),
             std::string(address.substr(colon + 1)) };
}

} // namespace service::gb28181::control_protocol

namespace service::gb28181::control_protocol {

inline MediaPortsJson mediaPortsJson(std::pmr::memory_resource* resource, const MediaServerPorts& ports) {
    MediaPortsJson result(ruvia::model_options{.resource_ = resource});
    result.set<"http">(ports.http)
        .set<"https">(ports.https)
        .set<"rtsp">(ports.rtsp)
        .set<"rtsps">(ports.rtsps)
        .set<"rtmp">(ports.rtmp)
        .set<"rtmps">(ports.rtmps)
        .set<"rtc">(ports.rtc)
        .set<"srt">(ports.srt);
    return result;
}

inline MediaCapabilitiesJson mediaCapabilitiesJson(
    std::pmr::memory_resource* resource,
    const MediaCapabilities& capabilities
) {
    MediaCapabilitiesJson result(ruvia::model_options{.resource_ = resource});
    result.set<"faac">(capabilities.faac)
        .set<"ffmpeg">(capabilities.ffmpeg)
        .set<"hls">(capabilities.hls)
        .set<"mp4">(capabilities.mp4)
        .set<"rtpProxy">(capabilities.rtpProxy)
        .set<"srt">(capabilities.srt)
        .set<"sctp">(capabilities.sctp)
        .set<"webRtc">(capabilities.webRtc)
        .set<"x264">(capabilities.x264)
        .set<"videoStack">(capabilities.videoStack)
        .set<"tls">(capabilities.tls)
        .set<"recording">(capabilities.recording);
    return result;
}

inline ChannelJson channelJson(std::pmr::memory_resource* resource, const Channel& channel) {
    ChannelJson result(ruvia::model_options{.resource_ = resource});
    result.set<"id">(channel.id)
        .set<"name">(channel.displayName())
        .set<"reportedName">(channel.name)
        .set<"customName">(channel.customName)
        .set<"manufacturer">(channel.manufacturer)
        .set<"online">(channel.online)
        .set<"ptzType">(channel.ptzType)
        .set<"ptzCapable">(channel.ptzType > 0);
    return result;
}

inline DeviceJson deviceJson(std::pmr::memory_resource* resource, const Device& device) {
    const auto [remoteIp, remotePort] = control_protocol::splitRemoteAddress(device.remoteAddress);
    ruvia::boxed_array<ChannelJson> channels(
        ruvia::model_options{ .resource_ = resource }
    );
    for (const auto& channel : device.channels) {
        channels.emplace(channelJson(resource, channel));
    }

    DeviceJson result(ruvia::model_options{.resource_ = resource});
    result.set<"id">(device.id)
        .set<"name">(device.displayName())
        .set<"reportedName">(device.name)
        .set<"customName">(device.customName)
        .set<"manufacturer">(device.manufacturer)
        .set<"remoteAddress">(device.remoteAddress)
        .set<"remoteIp">(remoteIp)
        .set<"remotePort">(remotePort)
        .set<"registrationSource">(device.registrationSource)
        .set<"lastSeenAt">(service::common::utcTimestamp(device.lastSeen))
        .set<"online">(device.online)
        .set<"channels">(std::move(channels));
    return result;
}

inline std::string jsonData(std::string value) {
    return "{\"code\":0,\"message\":\"ok\",\"data\":" +
        std::move(value) + "}";
}

inline std::string jsonAction(bool sent, std::string_view deviceId = {}, std::string_view channelId = {}) {
    std::string result = "{\"sent\":";
    result += sent ? "true" : "false";
    if (!deviceId.empty()) {
        result += ",\"device_id\":" + service::utils::jsonQuoted(deviceId);
    }
    if (!channelId.empty()) {
        result += ",\"channel_id\":" + service::utils::jsonQuoted(channelId);
    }
    result.push_back('}');
    return jsonData(std::move(result));
}

inline std::string jsonPreviewStart(const SipPreviewStartResult& value) {
    std::string result = "{\"sent\":true,\"session_id\":" +
        service::utils::jsonQuoted(value.sessionId) +
        ",\"device_id\":" +
        service::utils::jsonQuoted(value.deviceId) +
        ",\"channel_id\":" +
        service::utils::jsonQuoted(value.channelId) +
        ",\"stream_id\":" +
        service::utils::jsonQuoted(value.streamId) +
        ",\"ssrc\":" +
        service::utils::jsonQuoted(value.ssrc) +
        ",\"rtp_port\":" + std::to_string(value.rtpPort) +
        ",\"lease_timeout_seconds\":" +
        std::to_string(value.leaseTimeoutSeconds) +
        ",\"play_urls\":{\"http_flv\":" +
        service::utils::jsonQuoted(value.playUrls.httpFlv) +
        ",\"ws_flv\":" +
        service::utils::jsonQuoted(value.playUrls.wsFlv) +
        ",\"http_ts\":" +
        service::utils::jsonQuoted(value.playUrls.httpTs) +
        ",\"hls\":" +
        service::utils::jsonQuoted(value.playUrls.hls) +
        ",\"webrtc\":" +
        service::utils::jsonQuoted(value.playUrls.webRtc) +
        ",\"rtsp\":" +
        service::utils::jsonQuoted(value.playUrls.rtsp) +
        ",\"rtmp\":" +
        service::utils::jsonQuoted(value.playUrls.rtmp) +
        "}}";
    return jsonData(std::move(result));
}

inline std::string jsonPreviewStop(const SipPreviewStopResult& value) {
    return jsonData(
        "{\"stopped\":true,\"session_id\":" +
        service::utils::jsonQuoted(value.sessionId) + ",\"stream_id\":" +
        service::utils::jsonQuoted(value.streamId) + ",\"bye_sent\":" +
        (value.byeSent ? "true" : "false") +
        ",\"rtp_server_closed\":" +
        (value.rtpServerClosed ? "true" : "false") + "}"
    );
}

} // namespace service::gb28181::control_protocol
