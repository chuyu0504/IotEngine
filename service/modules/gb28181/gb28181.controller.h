#pragma once

#include <ruvia/web/controller.h>
#include <ruvia/web/model_json.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/permission.h"
#include "service/modules/gb28181/gb28181.service.h"
#include "service/modules/gb28181/gb28181.types.h"

namespace service::gb28181 {
class Gb28181Controller final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/gb28181", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(Gb28181Controller)
    RUVIA_GET("/health", health, service::middleware::PermissionMiddleware<"iot:gb28181:query">);
    RUVIA_GET_SSE("/devices/events", devicesEvents, service::middleware::PermissionMiddleware<"iot:gb28181:query">);
    RUVIA_GET("/streams/:streamId/recording", recording, service::middleware::PermissionMiddleware<"iot:gb28181:record">, ruvia::path_model<GbStreamParams>);
    RUVIA_POST("/devices/:deviceId/catalog/query", catalog, service::middleware::PermissionMiddleware<"iot:gb28181:control">, ruvia::path_model<GbDeviceParams>);
    RUVIA_PUT("/devices/:deviceId/name", renameDevice, service::middleware::PermissionMiddleware<"iot:gb28181:control">, ruvia::path_model<GbDeviceParams>, ruvia::json_body<GbNameBody>);
    RUVIA_PUT("/devices/:deviceId/channels/:channelId/name", renameChannel, service::middleware::PermissionMiddleware<"iot:gb28181:control">, ruvia::path_model<GbChannelParams>, ruvia::json_body<GbNameBody>);
    RUVIA_POST("/devices/:deviceId/channels/:channelId/preview/start", startPreview, service::middleware::PermissionMiddleware<"iot:gb28181:control">, ruvia::path_model<GbChannelParams>);
    RUVIA_POST("/previews/:sessionId/stop", stopPreview, service::middleware::PermissionMiddleware<"iot:gb28181:control">, ruvia::path_model<GbSessionParams>);
    RUVIA_POST("/previews/:sessionId/heartbeat", heartbeatPreview, service::middleware::PermissionMiddleware<"iot:gb28181:control">, ruvia::path_model<GbSessionParams>);
    RUVIA_POST("/devices/:deviceId/channels/:channelId/ptz/position/set", ptzPosition, service::middleware::PermissionMiddleware<"iot:gb28181:control">, ruvia::path_model<GbChannelParams>, ruvia::json_body<GbPositionBody>);
    RUVIA_POST("/devices/:deviceId/channels/:channelId/ptz/:action", ptz, service::middleware::PermissionMiddleware<"iot:gb28181:control">, ruvia::path_model<GbPtzParams>, ruvia::json_body<GbPtzBody>);
    RUVIA_POST("/streams/:streamId/recording/start", startRecording, service::middleware::PermissionMiddleware<"iot:gb28181:record">, ruvia::path_model<GbStreamParams>);
    RUVIA_POST("/streams/:streamId/recording/stop", stopRecording, service::middleware::PermissionMiddleware<"iot:gb28181:record">, ruvia::path_model<GbStreamParams>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> health(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        auto result = co_await gb28181Service().health(request);
        co_return c.json(service::common::ok<GbHealthResponse>(request, std::move(result)));
    }

    ruvia::task<void> devicesEvents(ruvia::context& c) {
        std::vector<service::middleware::LiveChannel> channels;
        channels.push_back({ "snapshot", { std::string(service::message::live::topic::kGb28181) },
            [](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                // 连接建立后权限仍可能被撤销，每次事件查询前重新校验。
                co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:gb28181:query");
                auto result = co_await gb28181Service().devices(request);
                co_return service::middleware::modelJson(service::common::ok<GbDeviceListResponse>(request, std::move(result)));
            },
            {} });
        co_await service::middleware::serveLive(c, service::middleware::requireAuth(c).userId, std::move(channels),
            [&c] { (void)service::middleware::requireAuth(c); });
    }

    ruvia::task<ruvia::http_response> recording(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        GbActionDto result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"recording">(co_await gb28181Service().recording(request, std::string(c.req().validated<GbStreamParams>().get<"streamId">().view())));
        co_return c.json(service::common::ok<GbActionResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> catalog(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& params = c.req().validated<GbDeviceParams>();
        co_await gb28181Service().queryCatalog(request, std::string(params.get<"deviceId">().view()));
        GbActionDto result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"sent">(true);
        result.set<"deviceId">(params.get<"deviceId">().view());
        co_return c.json(service::common::ok<GbActionResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> renameDevice(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await gb28181Service().renameDevice(request, std::string(c.req().validated<GbDeviceParams>().get<"deviceId">().view()), service::utils::trim(c.req().validated<GbNameBody>().get<"name">().view()));
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> renameChannel(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& params = c.req().validated<GbChannelParams>();
        co_await gb28181Service().renameChannel(request, std::string(params.get<"deviceId">().view()), std::string(params.get<"channelId">().view()), service::utils::trim(c.req().validated<GbNameBody>().get<"name">().view()));
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> startPreview(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& params = c.req().validated<GbChannelParams>();
        auto result = co_await gb28181Service().startPreview(request, std::string(params.get<"deviceId">().view()), std::string(params.get<"channelId">().view()));
        co_return c.json(service::common::ok<GbPreviewStartResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> stopPreview(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        auto result = co_await gb28181Service().stopPreview(request, std::string(c.req().validated<GbSessionParams>().get<"sessionId">().view()));
        co_return c.json(service::common::ok<GbPreviewStopResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> heartbeatPreview(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await gb28181Service().renewPreview(request, std::string(c.req().validated<GbSessionParams>().get<"sessionId">().view()));
        GbActionDto result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"sent">(true);
        result.set<"action">("heartbeat");
        co_return c.json(service::common::ok<GbActionResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> ptzPosition(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& params = c.req().validated<GbChannelParams>();
        const auto& body = c.req().validated<GbPositionBody>();
        co_await gb28181Service().ptzPosition(request, std::string(params.get<"deviceId">().view()), std::string(params.get<"channelId">().view()), body.get<"pan">().value_, body.get<"tilt">().value_, body.get<"zoom">().value_);
        GbActionDto result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"pan">(body.get<"pan">());
        result.set<"tilt">(body.get<"tilt">());
        result.set<"zoom">(body.get<"zoom">());
        result.set<"sent">(true);
        co_return c.json(service::common::ok<GbActionResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> ptz(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& params = c.req().validated<GbPtzParams>();
        const auto speed = static_cast<std::uint8_t>(c.req().validated<GbPtzBody>().get<"speed">().value_or(ruvia::int64{ 80 }).value_);
        co_await gb28181Service().ptz(request, std::string(params.get<"deviceId">().view()), std::string(params.get<"channelId">().view()), std::string(params.get<"action">().view()), speed);
        GbActionDto result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"deviceId">(params.get<"deviceId">().view());
        result.set<"channelId">(params.get<"channelId">().view());
        result.set<"action">(params.get<"action">().view());
        result.set<"speed">(speed);
        result.set<"sent">(true);
        co_return c.json(service::common::ok<GbActionResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> startRecording(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await gb28181Service().startRecording(request, std::string(c.req().validated<GbStreamParams>().get<"streamId">().view()));
        GbActionDto result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"recording">(true);
        co_return c.json(service::common::ok<GbActionResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> stopRecording(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await gb28181Service().stopRecording(request, std::string(c.req().validated<GbStreamParams>().get<"streamId">().view()));
        GbActionDto result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"recording">(false);
        co_return c.json(service::common::ok<GbActionResponse>(request, std::move(result)));
    }
};

// Player URLs embed a signed play token instead of a session, so the media
// routes carry no authentication middleware; ZLMediaKit rejects unsigned or
// expired playback.
class MediaProxyController final {
  public:
    RUVIA_CONTROLLER_GROUP("/media")
    RUVIA_ROUTES_BEGIN(MediaProxyController)
    RUVIA_GET_STREAM("/*", proxy);
    ruvia_controller_access_type::add_response_stream_route(
        ruvia_route_scope,
        ruvia::http_known_method::post,
        "/*",
        ruvia_controller_access_type::template bind_stream<&ruvia_controller_type::proxy>(
            this
        ),
        ruvia_controller_access_type::template make_middlewares<>()
    );
    RUVIA_ROUTES_END

  private:
    ruvia::task<void> proxy(ruvia::context& c) {
        if (!(co_await MediaProxyService::mediaStarted(c))) {
            service::common::fail(10002, "GB28181 媒体服务未启用", 503);
        }
        co_await MediaProxyService::forward(c);
    }
};
} // namespace service::gb28181
