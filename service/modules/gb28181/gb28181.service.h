#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/context.h>
#include <ruvia/web/context_request.h>
#include <ruvia/web/http_client_handle.h>
#include <ruvia/web/model_json.h>
#include <ruvia/web/streaming.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/modules/gb28181/gb28181.entity.h"
#include "service/utils/rpc_client.h"
#include "service/modules/gb28181/gb28181.types.h"

namespace service::gb28181 {

namespace detail {


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

GbMediaPortsDto mediaPorts(Context& c, const service::message::gb28181::MediaPorts& value) {
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
    const service::message::gb28181::MediaCapabilities& value
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

GbHealthDto health(Context& c, const service::message::gb28181::Health& value) {
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

GbChannelDto channel(Context& c, const service::message::gb28181::Channel& value) {
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

GbDeviceDto device(Context& c, const service::message::gb28181::Device& value) {
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

GbPlayUrlsDto playUrls(Context& c, const service::message::gb28181::PlayUrls& value) {
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

GbPreviewStartDto previewStart(Context& c, const service::message::gb28181::PreviewStart& value) {
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

GbPreviewStopDto previewStop(Context& c, const service::message::gb28181::PreviewStop& value) {
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
        auto response = detail::parseResponse<service::message::gb28181::HealthResponse>(c, raw);
        co_return detail::health(c, *response.template get<"data">());
    }

    template <typename Context>

    [[nodiscard]] ruvia::task<GbDeviceListDto>
    devices(Context& c) const {
        const auto raw = co_await call(c, "devices", "{}");
        auto response = detail::parseResponse<service::message::gb28181::DeviceListResponse>(c, raw);
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
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"deviceId">(deviceId);
        (void)co_await call(c, "catalog", service::message::rpc::encode(payload, c.arena()));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> renameDevice(Context& c, std::string deviceId, std::string name) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"deviceId">(deviceId);
        payload.template set<"name">(name);
        (void)co_await call(c, "rename_device", service::message::rpc::encode(payload, c.arena()));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> renameChannel(Context& c, std::string deviceId, std::string channelId, std::string name) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"deviceId">(deviceId);
        payload.template set<"channelId">(channelId);
        payload.template set<"name">(name);
        (void)co_await call(c, "rename_channel", service::message::rpc::encode(payload, c.arena()));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> ptz(Context& c, std::string deviceId, std::string channelId, std::string action, std::uint8_t speed) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"deviceId">(deviceId);
        payload.template set<"channelId">(channelId);
        payload.template set<"action">(action);
        payload.template set<"speed">(speed);
        (void)co_await call(c, "ptz", service::message::rpc::encode(payload, c.arena()));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> ptzPosition(Context& c, std::string deviceId, std::string channelId, double pan, double tilt, double zoom) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"deviceId">(deviceId);
        payload.template set<"channelId">(channelId);
        payload.template set<"pan">(pan);
        payload.template set<"tilt">(tilt);
        payload.template set<"zoom">(zoom);
        (void)co_await call(c, "ptz.position", service::message::rpc::encode(payload, c.arena()));
        co_return;
    }

    template <typename Context>

    ruvia::task<GbPreviewStartDto>
    startPreview(Context& c, std::string deviceId, std::string channelId) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"deviceId">(deviceId);
        payload.template set<"channelId">(channelId);
        const auto raw =
            co_await call(c, "preview.start", service::message::rpc::encode(payload, c.arena()));
        auto response =
            detail::parseResponse<service::message::gb28181::PreviewStartResponse>(c, raw);
        co_return detail::previewStart(c, *response.template get<"data">());
    }

    template <typename Context>

    ruvia::task<GbPreviewStopDto>
    stopPreview(Context& c, std::string sessionId) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"sessionId">(sessionId);
        const auto raw =
            co_await call(c, "preview.stop", service::message::rpc::encode(payload, c.arena()));
        auto response =
            detail::parseResponse<service::message::gb28181::PreviewStopResponse>(c, raw);
        co_return detail::previewStop(c, *response.template get<"data">());
    }

    template <typename Context>

    ruvia::task<void> renewPreview(Context& c, std::string sessionId) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"sessionId">(sessionId);
        (void)co_await call(c, "preview.heartbeat", service::message::rpc::encode(payload, c.arena()));
        co_return;
    }

    template <typename Context>

    ruvia::task<bool> recording(Context& c, std::string streamId) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"streamId">(streamId);
        const auto raw =
            co_await call(c, "recording", service::message::rpc::encode(payload, c.arena()));
        auto response = detail::parseResponse<service::message::gb28181::ActionResponse>(c, raw);
        const auto& value = response.template get<"data">()->template get<"recording">();
        co_return value&& static_cast<bool>(*value);
    }

    template <typename Context>

    ruvia::task<void> startRecording(Context& c, std::string streamId) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"streamId">(streamId);
        (void)co_await call(c, "recording.start", service::message::rpc::encode(payload, c.arena()));
        co_return;
    }

    template <typename Context>

    ruvia::task<void> stopRecording(Context& c, std::string streamId) const {
        auto payload = service::message::gb28181::ControlRequest({ .resource_ = c.arena() });
        payload.template set<"streamId">(streamId);
        (void)co_await call(c, "recording.stop", service::message::rpc::encode(payload, c.arena()));
        co_return;
    }

  private:
    template <typename Context>
    static ruvia::task<std::string> call(Context& c, std::string_view operation, std::string payload) {
        co_return co_await service::utils::callRpc(c, "gb28181", operation, std::move(payload));
    }
};

inline Gb28181Service& gb28181Service() {
    static thread_local Gb28181Service value;
    return value;
}

// Proxies player requests under /media to the process-local ZLMediaKit HTTP
// listener through the worker's "gb-media" client.  Playback is authorized by
// the signed play token, which ZLMediaKit verifies for every play request.
class MediaProxyService final {
  public:
    // Media readiness is published by the Collector that holds the SDK handle.
    static ruvia::task<bool> mediaStarted(ruvia::context& c) {
        const auto started = co_await c.redis().hget(MediaStateRecord::kKey, MediaStateRecord::kStartedField);
        co_return started&& MediaStateRecord::started(std::string_view(*started));
    }

    static bool allowedTarget(std::string_view method, std::string_view path) noexcept {
        if (method == "POST") {
            return path == "/index/api/webrtc";
        }
        if (method != "GET" || !path.starts_with("/rtp/") ||
            path.find("..") != std::string_view::npos) {
            return false;
        }
        return std::ranges::none_of(path, [](unsigned char character) {
            return character <= 0x20U || character == 0x7FU || character == '\\' ||
                character == '%' || character == '?' || character == '#';
        });
    }

    // HLS child URLs carry the playlist's play token.
    static std::string rewriteHlsPlaylist(std::string_view playlist, std::string_view query) {
        if (query.empty()) {
            return std::string(playlist);
        }
        std::string output;
        output.reserve(playlist.size() + query.size() * 8U);
        while (!playlist.empty()) {
            const auto end = playlist.find('\n');
            auto line =
                end == std::string_view::npos ? playlist : playlist.substr(0, end);
            const auto carriage = line.ends_with('\r');
            if (carriage) {
                line.remove_suffix(1);
            }
            if (!line.empty() && !line.starts_with('#')) {
                output.append(line);
                output.push_back(line.find('?') == std::string_view::npos ? '?' : '&');
                output.append(query);
            } else {
                output.append(line);
            }
            if (carriage) {
                output.push_back('\r');
            }
            if (end == std::string_view::npos) {
                break;
            }
            output.push_back('\n');
            playlist.remove_prefix(end + 1);
        }
        return output;
    }

    static ruvia::task<void> forward(ruvia::context& context) {
        const auto request = context.req();
        const auto path = request.path();
        if (!path.starts_with("/media")) {
            service::common::fail(10001, "媒体代理路径无效", 400);
        }
        const auto upstreamPath = path.substr(6);
        if (!allowedTarget(request.method(), upstreamPath)) {
            service::common::fail(10003, "媒体资源不存在", 404);
        }

        const auto query = queryString(request);
        std::string target(upstreamPath);
        if (!query.empty()) {
            target += '?' + query;
        }
        std::vector<ruvia::http_header_view> headers;
        for (const auto name : { "Content-Type", "Accept", "Range" }) {
            if (const auto value = request.header(name)) {
                headers.emplace_back(name, *value);
            }
        }
        std::string body;
        if (request.method() == "POST") {
            body.assign(co_await request.text());
        }
        const auto content = request.method() == "POST"
            ? ruvia::http_client_request_content_view::bytes(body)
            : ruvia::http_client_request_content_view::none();
        // The registered client, upstream socket, and response body all belong to
        // the accepting Service Worker. Ruvia supplies cancellation and backpressure.
        auto upstream = co_await context.get_http_client("gb-media").send({ .method_ = request.method(), .target_ = target, .headers_ = headers, .content_ = content });
        if (upstream.status().value() < 200 || upstream.status().value() > 599) {
            service::common::fail(10002, "媒体服务返回了无效状态", 502);
        }
        context.status(upstream.status());
        for (const auto& header : upstream.headers()) {
            if (forwardedResponseHeader(lower(std::string(header.name())))) {
                context.header(header.name(), header.value());
            }
        }

        auto& writer = context.stream();
        if (upstreamPath.ends_with(".m3u8")) {
            const auto playlist = co_await upstream.body().read_all(2U * 1024U * 1024U);
            const std::string_view playlistText(reinterpret_cast<const char*>(playlist.bytes().data()), playlist.size());
            const auto rewritten = rewriteHlsPlaylist(playlistText, query);
            co_await writer.write(rewritten);
        } else {
            co_await upstream.body().pipe_to(writer);
        }
        if (!writer.aborted()) {
            co_await writer.end();
        }
    }

  private:
    static std::string lower(std::string value) {
        std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return value;
    }

    static bool forwardedResponseHeader(std::string_view name) noexcept {
        return name == "content-type" || name == "cache-control" ||
            name == "content-range" || name == "accept-ranges" || name == "etag" ||
            name == "last-modified" || name == "access-control-allow-origin" ||
            name == "access-control-expose-headers";
    }

    static std::string percentEncode(std::string_view value) {
        static constexpr char digits[] = "0123456789ABCDEF";
        std::string result;
        result.reserve(value.size());
        for (const auto ch : value) {
            const auto byte = static_cast<unsigned char>(ch);
            if (std::isalnum(byte) != 0 || ch == '-' || ch == '_' || ch == '.' ||
                ch == '~') {
                result.push_back(ch);
            } else {
                result.push_back('%');
                result.push_back(digits[byte >> 4U]);
                result.push_back(digits[byte & 0x0FU]);
            }
        }
        return result;
    }

    static std::string queryString(const ruvia::context_request& request) {
        std::string result;
        for (const auto& field : request.query_fields()) {
            if (!result.empty()) {
                result.push_back('&');
            }
            result.append(percentEncode(field.name()));
            result.push_back('=');
            result.append(percentEncode(field.value()));
        }
        return result;
    }
};

} // namespace service::gb28181
