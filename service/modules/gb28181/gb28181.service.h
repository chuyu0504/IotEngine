#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/web/context.h>
#include <ruvia/web/model_json.h>

#include "service/common/http.h"
#include "service/middleware/rpc.h"
#include "service/modules/gb28181/gb28181.types.h"

namespace service::gb28181 {

namespace detail {

template <typename Context>
rpc_wire::ControlRequest controlRequest(Context& c) {
    return rpc_wire::ControlRequest({ .resource_ = c.arena() });
}

template <typename Context>
std::string encodeRequest(Context& c, const rpc_wire::ControlRequest& request) {
    const auto encoded = ruvia::to_json(request, { .resource_ = c.arena() });
    return { encoded.data(), encoded.size() };
}

template <typename Response, typename Context>
Response parseResponse(Context& c, std::string_view body) {
    auto parsed =
        ruvia::from_json<Response>(body, { .resource_ = c.arena() });
    if (!parsed || !parsed->template get<"data">()) {
        service::common::fail(10004, "GB28181 RPC 响应无效", 502);
    }
    return std::move(*parsed);
}

template <typename Context>

GbMediaPortsDto mediaPorts(Context& c, const rpc_wire::MediaPorts& value) {
    GbMediaPortsDto result(ruvia::model_options{ .resource_ = c.arena() });
    if (const auto& field = value.template get<"http">()) {
        result.set<"http">(*field);
    }
    if (const auto& field = value.template get<"https">()) {
        result.set<"https">(*field);
    }
    if (const auto& field = value.template get<"rtsp">()) {
        result.set<"rtsp">(*field);
    }
    if (const auto& field = value.template get<"rtsps">()) {
        result.set<"rtsps">(*field);
    }
    if (const auto& field = value.template get<"rtmp">()) {
        result.set<"rtmp">(*field);
    }
    if (const auto& field = value.template get<"rtmps">()) {
        result.set<"rtmps">(*field);
    }
    if (const auto& field = value.template get<"rtc">()) {
        result.set<"rtc">(*field);
    }
    if (const auto& field = value.template get<"srt">()) {
        result.set<"srt">(*field);
    }
    return result;
}

template <typename Context>

GbMediaCapabilitiesDto mediaCapabilities(
    Context& c,
    const rpc_wire::MediaCapabilities& value
) {
    GbMediaCapabilitiesDto result(ruvia::model_options{ .resource_ = c.arena() });
    if (const auto& field = value.template get<"faac">()) {
        result.set<"faac">(*field);
    }
    if (const auto& field = value.template get<"ffmpeg">()) {
        result.set<"ffmpeg">(*field);
    }
    if (const auto& field = value.template get<"hls">()) {
        result.set<"hls">(*field);
    }
    if (const auto& field = value.template get<"mp4">()) {
        result.set<"mp4">(*field);
    }
    if (const auto& field = value.template get<"rtpProxy">()) {
        result.set<"rtpProxy">(*field);
    }
    if (const auto& field = value.template get<"srt">()) {
        result.set<"srt">(*field);
    }
    if (const auto& field = value.template get<"sctp">()) {
        result.set<"sctp">(*field);
    }
    if (const auto& field = value.template get<"webRtc">()) {
        result.set<"webRtc">(*field);
    }
    if (const auto& field = value.template get<"x264">()) {
        result.set<"x264">(*field);
    }
    if (const auto& field = value.template get<"videoStack">()) {
        result.set<"videoStack">(*field);
    }
    if (const auto& field = value.template get<"tls">()) {
        result.set<"tls">(*field);
    }
    if (const auto& field = value.template get<"recording">()) {
        result.set<"recording">(*field);
    }
    return result;
}

template <typename Context>

GbHealthDto health(Context& c, const rpc_wire::Health& value) {
    GbHealthDto result(ruvia::model_options{ .resource_ = c.arena() });
    if (const auto& field = value.template get<"status">()) {
        result.set<"status">(field->view());
    }
    if (const auto& field = value.template get<"service">()) {
        result.set<"service">(field->view());
    }
    if (const auto& field = value.template get<"enabled">()) {
        result.set<"enabled">(*field);
    }
    if (const auto& field = value.template get<"started">()) {
        result.set<"started">(*field);
    }
    if (const auto& field = value.template get<"error">()) {
        result.set<"error">(field->view());
    }
    if (const auto& field = value.template get<"mediaPorts">()) {
        result.set<"mediaPorts">(mediaPorts(c, *field));
    }
    if (const auto& field = value.template get<"mediaCapabilities">()) {
        result.set<"mediaCapabilities">(mediaCapabilities(c, *field));
    }
    return result;
}

template <typename Context>

GbChannelDto channel(Context& c, const rpc_wire::Channel& value) {
    GbChannelDto result(ruvia::model_options{ .resource_ = c.arena() });
    if (const auto& field = value.template get<"id">()) {
        result.set<"id">(field->view());
    }
    if (const auto& field = value.template get<"name">()) {
        result.set<"name">(field->view());
    }
    if (const auto& field = value.template get<"reportedName">()) {
        result.set<"reportedName">(field->view());
    }
    if (const auto& field = value.template get<"customName">()) {
        result.set<"customName">(field->view());
    }
    if (const auto& field = value.template get<"manufacturer">()) {
        result.set<"manufacturer">(field->view());
    }
    if (const auto& field = value.template get<"online">()) {
        result.set<"online">(*field);
    }
    if (const auto& field = value.template get<"ptzType">()) {
        result.set<"ptzType">(*field);
    }
    if (const auto& field = value.template get<"ptzCapable">()) {
        result.set<"ptzCapable">(*field);
    }
    return result;
}

template <typename Context>

GbDeviceDto device(Context& c, const rpc_wire::Device& value) {
    GbDeviceDto result(ruvia::model_options{ .resource_ = c.arena() });
    if (const auto& field = value.template get<"id">()) {
        result.set<"id">(field->view());
    }
    if (const auto& field = value.template get<"name">()) {
        result.set<"name">(field->view());
    }
    if (const auto& field = value.template get<"reportedName">()) {
        result.set<"reportedName">(field->view());
    }
    if (const auto& field = value.template get<"customName">()) {
        result.set<"customName">(field->view());
    }
    if (const auto& field = value.template get<"manufacturer">()) {
        result.set<"manufacturer">(field->view());
    }
    if (const auto& field = value.template get<"remoteAddress">()) {
        result.set<"remoteAddress">(field->view());
    }
    if (const auto& field = value.template get<"remoteIp">()) {
        result.set<"remoteIp">(field->view());
    }
    if (const auto& field = value.template get<"remotePort">()) {
        result.set<"remotePort">(field->view());
    }
    if (const auto& field = value.template get<"registrationSource">()) {
        result.set<"registrationSource">(field->view());
    }
    if (const auto& field = value.template get<"lastSeenAt">()) {
        result.set<"lastSeenAt">(field->view());
    }
    if (const auto& field = value.template get<"online">()) {
        result.set<"online">(*field);
    }

    if (const auto& source = value.template get<"channels">()) {
        ruvia::boxed_array<GbChannelDto> channels(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& item : *source) {
            channels.emplace(channel(c, item));
        }
        result.set<"channels">(std::move(channels));
    }
    return result;
}

template <typename Context>

GbPlayUrlsDto playUrls(Context& c, const rpc_wire::PlayUrls& value) {
    GbPlayUrlsDto result(ruvia::model_options{ .resource_ = c.arena() });
    if (const auto& field = value.template get<"httpFlv">()) {
        result.set<"httpFlv">(field->view());
    }
    if (const auto& field = value.template get<"wsFlv">()) {
        result.set<"wsFlv">(field->view());
    }
    if (const auto& field = value.template get<"httpTs">()) {
        result.set<"httpTs">(field->view());
    }
    if (const auto& field = value.template get<"hls">()) {
        result.set<"hls">(field->view());
    }
    if (const auto& field = value.template get<"webrtc">()) {
        result.set<"webrtc">(field->view());
    }
    if (const auto& field = value.template get<"rtsp">()) {
        result.set<"rtsp">(field->view());
    }
    if (const auto& field = value.template get<"rtmp">()) {
        result.set<"rtmp">(field->view());
    }
    return result;
}

template <typename Context>

GbPreviewStartDto previewStart(Context& c, const rpc_wire::PreviewStart& value) {
    GbPreviewStartDto result(ruvia::model_options{ .resource_ = c.arena() });
    if (const auto& field = value.template get<"sent">()) {
        result.set<"sent">(*field);
    }
    if (const auto& field = value.template get<"sessionId">()) {
        result.set<"sessionId">(field->view());
    }
    if (const auto& field = value.template get<"deviceId">()) {
        result.set<"deviceId">(field->view());
    }
    if (const auto& field = value.template get<"channelId">()) {
        result.set<"channelId">(field->view());
    }
    if (const auto& field = value.template get<"streamId">()) {
        result.set<"streamId">(field->view());
    }
    if (const auto& field = value.template get<"ssrc">()) {
        result.set<"ssrc">(field->view());
    }
    if (const auto& field = value.template get<"rtpPort">()) {
        result.set<"rtpPort">(*field);
    }
    if (const auto& field = value.template get<"leaseTimeoutSeconds">()) {
        result.set<"leaseTimeoutSeconds">(*field);
    }
    if (const auto& field = value.template get<"playUrls">()) {
        result.set<"playUrls">(playUrls(c, *field));
    }
    return result;
}

template <typename Context>

GbPreviewStopDto previewStop(Context& c, const rpc_wire::PreviewStop& value) {
    GbPreviewStopDto result(ruvia::model_options{ .resource_ = c.arena() });
    if (const auto& field = value.template get<"stopped">()) {
        result.set<"stopped">(*field);
    }
    if (const auto& field = value.template get<"sessionId">()) {
        result.set<"sessionId">(field->view());
    }
    if (const auto& field = value.template get<"streamId">()) {
        result.set<"streamId">(field->view());
    }
    if (const auto& field = value.template get<"byeSent">()) {
        result.set<"byeSent">(*field);
    }
    if (const auto& field = value.template get<"rtpServerClosed">()) {
        result.set<"rtpServerClosed">(*field);
    }
    return result;
}

} // namespace detail

class Gb28181Service final {
  public:
    template <typename Context>
    [[nodiscard]] ruvia::task<GbHealthDto> health(Context& c) const {
        const auto raw = co_await call(c, "health", "{}");
        auto response = detail::parseResponse<rpc_wire::HealthResponse>(c, raw);
        co_return detail::health(c, *response.template get<"data">());
    }

    template <typename Context>

    [[nodiscard]] ruvia::task<GbDeviceListDto>
    devices(Context& c) const {
        const auto raw = co_await call(c, "devices", "{}");
        auto response = detail::parseResponse<rpc_wire::DeviceListResponse>(c, raw);
        const auto& source = *response.template get<"data">();
        ruvia::boxed_array<GbDeviceDto> items(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        if (const auto& values = source.template get<"items">()) {
            for (const auto& item : *values) {
                items.emplace(detail::device(c, item));
            }
        }
        GbDeviceListDto result(ruvia::model_options{ .resource_ = c.arena() });
        result.set<"items">(std::move(items));
        co_return result;
    }

    template <typename Context>

    ruvia::task<void> queryCatalog(Context& c, std::string deviceId) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"deviceId">(deviceId);
        (void)co_await call(c, "catalog", detail::encodeRequest(c, payload));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> renameDevice(Context& c, std::string deviceId, std::string name) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"deviceId">(deviceId);
        payload.template set<"name">(name);
        (void)co_await call(c, "rename_device", detail::encodeRequest(c, payload));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> renameChannel(Context& c, std::string deviceId, std::string channelId, std::string name) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"deviceId">(deviceId);
        payload.template set<"channelId">(channelId);
        payload.template set<"name">(name);
        (void)co_await call(c, "rename_channel", detail::encodeRequest(c, payload));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> ptz(Context& c, std::string deviceId, std::string channelId, std::string action, std::uint8_t speed) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"deviceId">(deviceId);
        payload.template set<"channelId">(channelId);
        payload.template set<"action">(action);
        payload.template set<"speed">(speed);
        (void)co_await call(c, "ptz", detail::encodeRequest(c, payload));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> ptzPosition(Context& c, std::string deviceId, std::string channelId, double pan, double tilt, double zoom) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"deviceId">(deviceId);
        payload.template set<"channelId">(channelId);
        payload.template set<"pan">(pan);
        payload.template set<"tilt">(tilt);
        payload.template set<"zoom">(zoom);
        (void)co_await call(c, "ptz.position", detail::encodeRequest(c, payload));
        co_return;
    }

    template <typename Context>

    ruvia::task<GbPreviewStartDto>
    startPreview(Context& c, std::string deviceId, std::string channelId) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"deviceId">(deviceId);
        payload.template set<"channelId">(channelId);
        const auto raw =
            co_await call(c, "preview.start", detail::encodeRequest(c, payload));
        auto response =
            detail::parseResponse<rpc_wire::PreviewStartResponse>(c, raw);
        co_return detail::previewStart(c, *response.template get<"data">());
    }

    template <typename Context>

    ruvia::task<GbPreviewStopDto>
    stopPreview(Context& c, std::string sessionId) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"sessionId">(sessionId);
        const auto raw =
            co_await call(c, "preview.stop", detail::encodeRequest(c, payload));
        auto response =
            detail::parseResponse<rpc_wire::PreviewStopResponse>(c, raw);
        co_return detail::previewStop(c, *response.template get<"data">());
    }

    template <typename Context>

    ruvia::task<void> renewPreview(Context& c, std::string sessionId) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"sessionId">(sessionId);
        (void)co_await call(c, "preview.heartbeat", detail::encodeRequest(c, payload));
        co_return;
    }

    template <typename Context>

    ruvia::task<bool> recording(Context& c, std::string streamId) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"streamId">(streamId);
        const auto raw =
            co_await call(c, "recording", detail::encodeRequest(c, payload));
        auto response = detail::parseResponse<rpc_wire::ActionResponse>(c, raw);
        const auto& value = response.template get<"data">()->template get<"recording">();
        co_return value&& static_cast<bool>(*value);
    }

    template <typename Context>

    ruvia::task<void> startRecording(Context& c, std::string streamId) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"streamId">(streamId);
        (void)co_await call(c, "recording.start", detail::encodeRequest(c, payload));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> stopRecording(Context& c, std::string streamId) const {
        auto payload = detail::controlRequest(c);
        payload.template set<"streamId">(streamId);
        (void)co_await call(c, "recording.stop", detail::encodeRequest(c, payload));
        co_return;
    }

  private:
    template <typename Context>
    static ruvia::task<std::string> call(Context& c, std::string_view operation, std::string payload) {
        co_return co_await service::rpc::call(c, "gb28181", operation, std::move(payload));
    }
};

inline Gb28181Service& gb28181Service() {
    static thread_local Gb28181Service value;
    return value;
}

} // namespace service::gb28181
