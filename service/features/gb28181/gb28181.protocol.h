#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/model.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/features/gb28181/device/device.types.h"
#include "service/features/gb28181/media/media.types.h"
#include "service/features/gb28181/sip/sip.types.h"
#include "service/utils/json.h"

namespace service::gb28181::control_protocol {

namespace stream {

// Control streams are scoped to a process incarnation.  This prevents two
// processes with the same worker index from consuming each other's commands.
inline constexpr std::string_view kProjection{ "iot:gb28181:projection" };
inline constexpr std::string_view kProjectionGroup{ "iot-engine:gb28181-projector" };
inline constexpr std::string_view kControlPrefix{ "iot:gb28181:control:worker:" };
inline constexpr std::string_view kControlGroup{ "iot-engine:gb28181-control" };
inline constexpr std::string_view kReceiptPrefix{ "iot:gb28181:projection-receipt:worker:" };
// Sorted set of owned aggregates scored by their Redis owner lease expiry in
// Redis milliseconds.  Members are `device:<id>` or `stream:<app:schema:stream>`.
inline constexpr std::string_view kOwnerDeadlines{ "iot:gb28181:owner-deadlines" };
inline constexpr std::string_view kOwnerDeadlineSeedPrefix{ "iot:gb28181:owner-deadlines:seed:" };
inline constexpr std::string_view kDeviceDeadlineKind{ "device" };
inline constexpr std::string_view kStreamDeadlineKind{ "stream" };

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

// Projection receipts are directed to the Collector Worker that published the
// projection.  The stream is scoped to that worker's process incarnation.
inline std::string projectionReceipts(
    std::size_t workerIndex,
    std::string_view instance
) {
    return std::string(kReceiptPrefix) + std::string(instance) + ":" +
        std::to_string(workerIndex);
}

inline std::string ownerDeadlineSeed(std::string_view instance) {
    return std::string(kOwnerDeadlineSeedPrefix) + std::string(instance);
}

inline std::string ownerDeadlineMember(std::string_view kind, std::string_view aggregateId) {
    return std::string(kind) + ":" + std::string(aggregateId);
}

struct OwnerDeadlineMember final {
    std::string_view kind;
    std::string_view aggregateId;
};

inline std::optional<OwnerDeadlineMember> parseOwnerDeadlineMember(std::string_view member) {
    const auto separator = member.find(':');
    if (separator == std::string_view::npos || separator + 1 == member.size()) {
        return std::nullopt;
    }
    const auto kind = member.substr(0, separator);
    if (kind != kDeviceDeadlineKind && kind != kStreamDeadlineKind) {
        return std::nullopt;
    }
    return OwnerDeadlineMember{ kind, member.substr(separator + 1) };
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

inline constexpr std::string_view kOwnerPrefix{ "iot:gb28181:owner:" };
inline constexpr std::string_view kSessionOwnerPrefix{ "iot:gb28181:session-owner:" };

inline std::string owner(std::string_view deviceId) {
    return std::string(kOwnerPrefix) + std::string(deviceId);
}

inline std::string sessionOwner(std::string_view sessionId) {
    return std::string(kSessionOwnerPrefix) + std::string(sessionId);
}

inline std::string projectionDone(std::string_view projectionId) {
    return "iot:gb28181:projection:done:" + std::string(projectionId);
}

inline std::string projectionSent(std::string_view projectionId) {
    return "iot:gb28181:projection:sent:" + std::string(projectionId);
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

inline constexpr std::array<std::pair<device::DeviceChange, std::string_view>, 4> kDeviceChangeNames{ {
    { device::DeviceChange::Status, "status" },
    { device::DeviceChange::Catalog, "catalog" },
    { device::DeviceChange::DeviceName, "device_name" },
    { device::DeviceChange::ChannelName, "channel_name" },
} };

// A coalesced projection lists its change kinds separated by commas.
inline std::string projectionChangeNames(device::DeviceChangeSet changes) {
    std::string names;
    for (const auto& [change, name] : kDeviceChangeNames) {
        if (changes.contains(change)) {
            if (!names.empty()) {
                names.push_back(',');
            }
            names.append(name);
        }
    }
    return names;
}

inline std::vector<service::message::StreamField>
deviceProjectionFields(const device::Device& device, device::DeviceChangeSet changes, std::size_t owner, std::string_view ownerToken) {
    using service::message::StreamField;
    std::vector<StreamField> fields{
        { "event_type", "gb28181.device" },
        { "schema_version", "1" },
        { "change", projectionChangeNames(changes) },
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
streamProjectionFields(const media::StreamStatus& stream, std::size_t owner, std::string_view ownerToken) {
    using service::message::StreamField;
    return {
        { "event_type", "gb28181.stream" },
        { "schema_version", "1" },
        { "aggregate_id", media::StreamStatus::identity(stream.app, stream.stream, stream.schema) },
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

// Every comma-separated name must be a known change; empty names are malformed.
inline std::optional<device::DeviceChangeSet> projectionDeviceChanges(std::string_view value) {
    device::DeviceChangeSet changes;
    for (;;) {
        const auto separator = value.find(',');
        const auto name = value.substr(0, separator);
        const auto found = std::ranges::find(kDeviceChangeNames, name, &std::pair<device::DeviceChange, std::string_view>::second);
        if (found == kDeviceChangeNames.end()) {
            return std::nullopt;
        }
        changes.add(found->first);
        if (separator == std::string_view::npos) {
            return changes;
        }
        value.remove_prefix(separator + 1);
    }
}

// Inverse of media::StreamStatus::identity; app and schema never contain ':'.
inline std::optional<media::StreamStatus> streamFromIdentity(std::string_view identity) {
    const auto first = identity.find(':');
    if (first == std::string_view::npos || first == 0) {
        return std::nullopt;
    }
    const auto second = identity.find(':', first + 1);
    if (second == std::string_view::npos || second == first + 1 || second + 1 == identity.size()) {
        return std::nullopt;
    }
    media::StreamStatus stream;
    stream.app = std::string(identity.substr(0, first));
    stream.schema = std::string(identity.substr(first + 1, second - first - 1));
    stream.stream = std::string(identity.substr(second + 1));
    return stream;
}

inline std::size_t projectionCount(std::string_view value, std::string_view field) {
    const auto parsed = projectionInteger(value);
    if (!parsed || *parsed < 0 || *parsed > 100000) {
        throw std::runtime_error("invalid GB28181 projection " + std::string(field));
    }
    return static_cast<std::size_t>(*parsed);
}

inline device::Device deviceFromProjection(const service::message::StreamMessage& message) {
    if (message.get("schema_version") != "1" ||
        message.get("event_type") != "gb28181.device") {
        throw std::runtime_error("unsupported GB28181 device projection");
    }
    device::Device device;
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
        device::Channel channel;
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

inline media::StreamStatus streamFromProjection(const service::message::StreamMessage& message) {
    if (message.get("schema_version") != "1" ||
        message.get("event_type") != "gb28181.stream") {
        throw std::runtime_error("unsupported GB28181 stream projection");
    }
    media::StreamStatus stream;
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

inline service::message::gb28181::MediaPorts mediaPortsJson(std::pmr::memory_resource* resource, const media::MediaServerPorts& ports) {
    service::message::gb28181::MediaPorts result(ruvia::model_options{ .resource_ = resource });
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

inline service::message::gb28181::MediaCapabilities mediaCapabilitiesJson(
    std::pmr::memory_resource* resource,
    const media::MediaCapabilities& capabilities
) {
    service::message::gb28181::MediaCapabilities result(ruvia::model_options{ .resource_ = resource });
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

inline service::message::gb28181::Channel channelJson(std::pmr::memory_resource* resource, const device::Channel& channel) {
    service::message::gb28181::Channel result(ruvia::model_options{ .resource_ = resource });
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

inline service::message::gb28181::Device deviceJson(std::pmr::memory_resource* resource, const device::Device& device) {
    const auto [remoteIp, remotePort] = control_protocol::splitRemoteAddress(device.remoteAddress);
    ruvia::boxed_array<service::message::gb28181::Channel> channels(
        ruvia::model_options{ .resource_ = resource }
    );
    for (const auto& channel : device.channels) {
        channels.emplace_back(channelJson(resource, channel));
    }

    service::message::gb28181::Device result(ruvia::model_options{ .resource_ = resource });
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

template <typename Response, typename Data>
std::string successJson(Data data, std::pmr::memory_resource* resource) {
    Response response(ruvia::model_options{ .resource_ = resource });
    response.template set<"code">(0).template set<"message">("ok").template set<"data">(std::move(data));
    return service::message::rpc::encode(response, resource);
}

inline std::string operationJson(std::int64_t code, std::string_view message) {
    service::message::gb28181::OperationResponse response;
    response.set<"code">(code).set<"message">(message);
    return service::message::rpc::encode(response, nullptr);
}

inline std::string jsonAction(bool sent, std::string_view deviceId = {}, std::string_view channelId = {}) {
    service::message::gb28181::Action action;
    action.set<"sent">(sent);
    if (!deviceId.empty()) {
        action.set<"deviceId">(deviceId);
    }
    if (!channelId.empty()) {
        action.set<"channelId">(channelId);
    }
    return successJson<service::message::gb28181::ActionResponse>(std::move(action), nullptr);
}

inline std::string jsonRecording(bool recording) {
    service::message::gb28181::Action action;
    action.set<"recording">(recording);
    return successJson<service::message::gb28181::ActionResponse>(std::move(action), nullptr);
}

inline std::string jsonPreviewStart(const sip::SipPreviewStartResult& value) {
    service::message::gb28181::PlayUrls urls;
    urls.set<"httpFlv">(value.playUrls.httpFlv)
        .set<"wsFlv">(value.playUrls.wsFlv)
        .set<"httpTs">(value.playUrls.httpTs)
        .set<"hls">(value.playUrls.hls)
        .set<"webrtc">(value.playUrls.webRtc)
        .set<"rtsp">(value.playUrls.rtsp)
        .set<"rtmp">(value.playUrls.rtmp);
    service::message::gb28181::PreviewStart preview;
    preview.set<"sent">(true)
        .set<"sessionId">(value.sessionId)
        .set<"deviceId">(value.deviceId)
        .set<"channelId">(value.channelId)
        .set<"streamId">(value.streamId)
        .set<"ssrc">(value.ssrc)
        .set<"rtpPort">(value.rtpPort)
        .set<"leaseTimeoutSeconds">(value.leaseTimeoutSeconds)
        .set<"playUrls">(std::move(urls));
    return successJson<service::message::gb28181::PreviewStartResponse>(std::move(preview), nullptr);
}

inline std::string jsonPreviewStop(const sip::SipPreviewStopResult& value) {
    service::message::gb28181::PreviewStop preview;
    preview.set<"stopped">(true)
        .set<"sessionId">(value.sessionId)
        .set<"streamId">(value.streamId)
        .set<"byeSent">(value.byeSent)
        .set<"rtpServerClosed">(value.rtpServerClosed);
    return successJson<service::message::gb28181::PreviewStopResponse>(std::move(preview), nullptr);
}

} // namespace service::gb28181::control_protocol
