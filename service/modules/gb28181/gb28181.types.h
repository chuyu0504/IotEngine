#pragma once
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <ruvia/web/model.h>
#include <ruvia/web/validation.h>

#include "service/common/http.h"
#include "service/utils/text.h"

namespace service::gb28181 {

inline constexpr auto kDefaultSpeed = RUVIA_DEFAULT(80){};

inline bool isGbIdentifier(const ruvia::string& value) {
    return !service::utils::trim(value.view()).empty();
}

RUVIA_MODEL(GbDeviceParams, RUVIA_REQUIRED_FIELD(deviceId, ruvia::string, RUVIA_MAX(128, "deviceId 长度超出限制"), RUVIA_CUSTOM("deviceId 不能为空白", isGbIdentifier)));

RUVIA_MODEL(GbChannelParams, RUVIA_REQUIRED_FIELD(deviceId, ruvia::string, RUVIA_MAX(128, "deviceId 长度超出限制"), RUVIA_CUSTOM("deviceId 不能为空白", isGbIdentifier)), RUVIA_REQUIRED_FIELD(channelId, ruvia::string, RUVIA_MAX(128, "channelId 长度超出限制"), RUVIA_CUSTOM("channelId 不能为空白", isGbIdentifier)));

RUVIA_MODEL(GbStreamParams, RUVIA_REQUIRED_FIELD(streamId, ruvia::string, RUVIA_MAX(128, "streamId 长度超出限制"), RUVIA_CUSTOM("streamId 不能为空白", isGbIdentifier)));

RUVIA_MODEL(GbSessionParams, RUVIA_REQUIRED_FIELD(sessionId, ruvia::string, RUVIA_MAX(128, "sessionId 长度超出限制"), RUVIA_CUSTOM("sessionId 不能为空白", isGbIdentifier)));

RUVIA_MODEL(GbPtzParams, RUVIA_REQUIRED_FIELD(deviceId, ruvia::string, RUVIA_MAX(128, "deviceId 长度超出限制"), RUVIA_CUSTOM("deviceId 不能为空白", isGbIdentifier)), RUVIA_REQUIRED_FIELD(channelId, ruvia::string, RUVIA_MAX(128, "channelId 长度超出限制"), RUVIA_CUSTOM("channelId 不能为空白", isGbIdentifier)), RUVIA_REQUIRED_FIELD(action, ruvia::string, RUVIA_MAX(128, "action 长度超出限制"), RUVIA_CUSTOM("action 不能为空白", isGbIdentifier), RUVIA_ONE_OF("不支持的云台动作", "left", "right", "up", "down", "zoomin", "zoomout", "stop")));

inline bool isGbFiniteNumber(const ruvia::double_value& value) {
    return std::isfinite(static_cast<double>(value));
}

RUVIA_MODEL(GbNameBody, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MAX(255, "name 长度超出限制"), RUVIA_CUSTOM("name 不能为空白", isGbIdentifier)));

RUVIA_MODEL(GbPtzBody, RUVIA_OPTIONAL_FIELD(speed, ruvia::int64, RUVIA_NULLABLE, decltype(kDefaultSpeed), RUVIA_MIN(0, "speed 必须在 0 - 255 之间"), RUVIA_MAX(255, "speed 必须在 0 - 255 之间")));

RUVIA_MODEL(GbPositionBody, RUVIA_REQUIRED_FIELD(pan, ruvia::double_value, RUVIA_MIN(0, "pan 必须在 0 - 360 之间"), RUVIA_MAX(360, "pan 必须在 0 - 360 之间"), RUVIA_CUSTOM("pan 必须是有限数字", isGbFiniteNumber)), RUVIA_REQUIRED_FIELD(tilt, ruvia::double_value, RUVIA_MIN(-30, "tilt 必须在 -30 - 90 之间"), RUVIA_MAX(90, "tilt 必须在 -30 - 90 之间"), RUVIA_CUSTOM("tilt 必须是有限数字", isGbFiniteNumber)), RUVIA_REQUIRED_FIELD(zoom, ruvia::double_value, RUVIA_MIN(1, "zoom 必须在 1 - 1000 之间"), RUVIA_MAX(1000, "zoom 必须在 1 - 1000 之间"), RUVIA_CUSTOM("zoom 必须是有限数字", isGbFiniteNumber)));

RUVIA_MODEL(GbMediaPortsDto, RUVIA_OPTIONAL_FIELD(http, ruvia::int64), RUVIA_OPTIONAL_FIELD(https, ruvia::int64), RUVIA_OPTIONAL_FIELD(rtsp, ruvia::int64), RUVIA_OPTIONAL_FIELD(rtsps, ruvia::int64), RUVIA_OPTIONAL_FIELD(rtmp, ruvia::int64), RUVIA_OPTIONAL_FIELD(rtmps, ruvia::int64), RUVIA_OPTIONAL_FIELD(rtc, ruvia::int64), RUVIA_OPTIONAL_FIELD(srt, ruvia::int64));

RUVIA_MODEL(
    GbMediaCapabilitiesDto,
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

RUVIA_MODEL(GbHealthDto, RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD(service, ruvia::string), RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(started, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(error, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("media_ports", mediaPorts, GbMediaPortsDto), RUVIA_OPTIONAL_FIELD_NAME("media_capabilities", mediaCapabilities, GbMediaCapabilitiesDto));

RUVIA_MODEL(
    GbChannelDto,
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
    GbDeviceDto,
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
    RUVIA_OPTIONAL_FIELD(channels, ruvia::boxed_array<GbChannelDto>)
);

RUVIA_MODEL(GbDeviceListDto, RUVIA_OPTIONAL_FIELD(items, ruvia::boxed_array<GbDeviceDto>));

RUVIA_MODEL(GbPlayUrlsDto, RUVIA_OPTIONAL_FIELD_NAME("http_flv", httpFlv, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("ws_flv", wsFlv, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("http_ts", httpTs, ruvia::string), RUVIA_OPTIONAL_FIELD(hls, ruvia::string), RUVIA_OPTIONAL_FIELD(webrtc, ruvia::string), RUVIA_OPTIONAL_FIELD(rtsp, ruvia::string), RUVIA_OPTIONAL_FIELD(rtmp, ruvia::string));

RUVIA_MODEL(
    GbPreviewStartDto,
    RUVIA_OPTIONAL_FIELD(sent, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("session_id", sessionId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("channel_id", channelId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("stream_id", streamId, ruvia::string),
    RUVIA_OPTIONAL_FIELD(ssrc, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("rtp_port", rtpPort, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("lease_timeout_seconds", leaseTimeoutSeconds, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("play_urls", playUrls, GbPlayUrlsDto)
);

RUVIA_MODEL(
    GbPreviewStopDto,
    RUVIA_OPTIONAL_FIELD(stopped, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("session_id", sessionId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("stream_id", streamId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("bye_sent", byeSent, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD_NAME("rtp_server_closed", rtpServerClosed, ruvia::bool_value)
);

RUVIA_MODEL(GbActionDto, RUVIA_OPTIONAL_FIELD(registered, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(sent, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("channel_id", channelId, ruvia::string), RUVIA_OPTIONAL_FIELD(action, ruvia::string), RUVIA_OPTIONAL_FIELD(speed, ruvia::int64), RUVIA_OPTIONAL_FIELD(pan, ruvia::double_value), RUVIA_OPTIONAL_FIELD(tilt, ruvia::double_value), RUVIA_OPTIONAL_FIELD(zoom, ruvia::double_value), RUVIA_OPTIONAL_FIELD(recording, ruvia::bool_value));

RUVIA_MODEL(GbActionResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, GbActionDto));

RUVIA_MODEL(GbPreviewStartResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, GbPreviewStartDto));

RUVIA_MODEL(GbPreviewStopResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, GbPreviewStopDto));

RUVIA_MODEL(GbDeviceListResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, GbDeviceListDto));

RUVIA_MODEL(GbHealthResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, GbHealthDto));

} // namespace service::gb28181
