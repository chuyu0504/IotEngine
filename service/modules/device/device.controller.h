#pragma once

#include <chrono>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/permission.h"
#include "service/modules/device/device.types.h"
#include "service/modules/device/device.service.h"

namespace service::device {

class DeviceController final : public ruvia::controller<DeviceController> {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/device", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN
    // 共享流按频道独立鉴权，不能把设备、分组及用户频道改成整体 AND 权限。
    RUVIA_GET_SSE("/events", deviceEvents, ruvia::query_model<DeviceEventsQuery>);
    RUVIA_PUT("/:id/debug", setDebug, service::middleware::PermissionMiddleware<"iot:device:edit">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<DeviceDebugBody>);
    // 设备
    RUVIA_GET("/options", options, service::middleware::PermissionMiddleware<"iot:device:query">);
    // 设备分组（统一收编到 /v1/device/groups）
    RUVIA_GET("/groups/:id/shares", groupShares, service::middleware::PermissionMiddleware<"iot:device-group:share">, ruvia::path_model<DeviceIdParams>);
    RUVIA_GET("/groups/:id/share-targets", groupShareTargets, service::middleware::PermissionMiddleware<"iot:device-group:share">, ruvia::path_model<DeviceIdParams>);
    RUVIA_PUT("/groups/:id/shares", replaceGroupShares, service::middleware::PermissionMiddleware<"iot:device-group:share">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<ReplaceDeviceSharesBody>);
    RUVIA_POST("/groups", groupCreate, service::middleware::PermissionMiddleware<"iot:device-group:add">, ruvia::json_body<CreateDeviceGroupBody>);
    RUVIA_PUT("/groups/:id", groupUpdate, service::middleware::PermissionMiddleware<"iot:device-group:edit">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<UpdateDeviceGroupBody>);
    RUVIA_DELETE("/groups/:id", groupRemove, service::middleware::PermissionMiddleware<"iot:device-group:delete">, ruvia::path_model<DeviceIdParams>);
    RUVIA_GET("/:id/history", history, service::middleware::PermissionMiddleware<"iot:device:query">, ruvia::path_model<DeviceIdParams>, ruvia::query_model<DeviceHistoryQuery>);
    RUVIA_GET("/:id/shares", shares, service::middleware::PermissionMiddleware<"iot:device:share">, ruvia::path_model<DeviceIdParams>);
    RUVIA_GET("/:id/share-targets", shareTargets, service::middleware::PermissionMiddleware<"iot:device:share">, ruvia::path_model<DeviceIdParams>);
    RUVIA_PUT("/:id/shares", replaceShares, service::middleware::PermissionMiddleware<"iot:device:share">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<ReplaceDeviceSharesBody>);
    // 设备（带参数的通配路由放在静态路由之后）
    RUVIA_GET("/:id", detail, service::middleware::PermissionMiddleware<"iot:device:query">, ruvia::path_model<DeviceIdParams>);
    RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"iot:device:query">);
    RUVIA_POST("/", create, service::middleware::PermissionMiddleware<"iot:device:add">, ruvia::json_body<CreateDeviceBody>);
    RUVIA_PUT("/:id", update, service::middleware::PermissionMiddleware<"iot:device:edit">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<UpdateDeviceBody>);
    RUVIA_DELETE("/:id", remove, service::middleware::PermissionMiddleware<"iot:device:delete">, ruvia::path_model<DeviceIdParams>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<DeviceCommandStatusesResponse> queryCommandStatuses(service::middleware::RequestContext& request, std::string_view ids) {
        co_return service::common::ok<DeviceCommandStatusesResponse>(request, co_await deviceService().commandStatuses(request, ids));
    }

    ruvia::task<DeviceDebugPacketsResponse> queryDebugPackets(service::middleware::RequestContext& request, std::string_view deviceId) {
        co_return service::common::ok<DeviceDebugPacketsResponse>(request, co_await deviceService().debugPackets(request, deviceId));
    }

    ruvia::task<ruvia::http_response> setDebug(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().setDebug(request, id(c), c.req().validated<DeviceDebugBody>().get<"enabled">());
        co_return c.json(service::common::operation(c, "调试设置已保存"));
    }

    static std::string id(ruvia::context& c) {
        return std::string(c.req().validated<DeviceIdParams>().get<"id">().view());
    }

    // ---- 设备 ----
    ruvia::task<ruvia::http_response> list(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(co_await queryList(c, request));
    }

    ruvia::task<void> deviceEvents(ruvia::context& c) {

        std::vector<service::live::SnapshotChannel> channels{
            { "devices", "device", [this, &c](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                 co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:device:query");
                 co_return service::live::json(co_await queryList(c, request));
             } },
            { "groups", "device", [this, &c](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                 co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:device-group:query");
                 co_return service::live::json(co_await queryGroupTreeCount(c, request));
             } },
            { "realtime", "device.realtime", [this, &c](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                 co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:device:query");
                 co_return service::live::json(co_await queryRealtime(c, request));
             } }
        };
        const auto& debugDeviceId = c.req().validated<DeviceEventsQuery>().get<"debugDeviceId">();
        if (debugDeviceId) {
            channels.push_back({ "packets", "packet-debug", [this, deviceId = std::string(debugDeviceId->view())](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                    co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:device:edit");
                                    co_return service::live::json(co_await queryDebugPackets(request, deviceId));
                                } });
        }
        const auto& commandIds = c.req().validated<DeviceEventsQuery>().get<"commandIds">();
        if (commandIds) {
            channels.push_back({ "commands", "command", [this, ids = std::string(commandIds->view())](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                    co_return service::live::json(co_await queryCommandStatuses(request, ids));
                                } });
        }
        const std::string debugId = debugDeviceId ? std::string(debugDeviceId->view()) : std::string();
        co_await service::live::serveSnapshotChannels(
            c,
            service::middleware::requireAuth(c).userId,
            std::move(channels),
            [&c] { (void)service::middleware::requireAuth(c); },
            [&c, debugId]() -> ruvia::task<void> {
                if (!debugId.empty()) {
                    co_await service::debug_idle::touch(c.redis(), "device", debugId);
                }
                co_return;
            });
    }

    ruvia::task<DevicePageResponse> queryList(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::common::ok<DevicePageResponse>(request, co_await deviceService().list(request));
    }

    ruvia::task<DeviceRealtimeResponse> queryRealtime(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::common::ok<DeviceRealtimeResponse>(request, co_await deviceService().realtime(request));
    }

    ruvia::task<ruvia::http_response> options(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceOptionsResponse>(request, co_await deviceService().options(request)));
    }

    ruvia::task<ruvia::http_response> detail(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceDetailResponse>(request, co_await deviceService().detail(request, id(c))));
    }

    ruvia::task<ruvia::http_response> history(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = co_await historySnapshot(c, request);
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<std::string> historySnapshot(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::live::data(c, co_await deviceService().history(request, id(c), c.req().validated<DeviceHistoryQuery>()));
    }

    ruvia::task<ruvia::http_response> create(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().create(request, c.req().validated<CreateDeviceBody>());
        co_return c.json(service::common::operation(c, "创建成功"));
    }

    ruvia::task<ruvia::http_response> update(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().update(request, id(c), c.req().validated<UpdateDeviceBody>());
        co_return c.json(service::common::operation(c, "更新成功"));
    }

    ruvia::task<ruvia::http_response> remove(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().remove(request, id(c));
        co_return c.json(service::common::operation(c, "删除成功"));
    }

    ruvia::task<ruvia::http_response> shares(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceSharesResponse>(request, co_await deviceShareService().list(request, id(c))));
    }

    ruvia::task<ruvia::http_response> shareTargets(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceShareTargetsResponse>(request, co_await deviceShareService().targets(request, id(c))));
    }

    ruvia::task<ruvia::http_response> replaceShares(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceShareService().replace(request, id(c), c.req().validated<ReplaceDeviceSharesBody>());
        co_return c.json(service::common::operation(c, "设备分享已更新"));
    }

    // ---- 设备分组 ----
    ruvia::task<DeviceGroupListResponse> queryGroupTreeCount(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::common::ok<DeviceGroupListResponse>(request, co_await deviceService().listGroups(request));
    }

    ruvia::task<ruvia::http_response> groupShares(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceSharesResponse>(request, co_await deviceShareService().listGroup(request, id(c))));
    }

    ruvia::task<ruvia::http_response> groupShareTargets(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceShareTargetsResponse>(request, co_await deviceShareService().groupTargets(request, id(c))));
    }

    ruvia::task<ruvia::http_response> replaceGroupShares(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceShareService().replaceGroup(request, id(c), c.req().validated<ReplaceDeviceSharesBody>());
        co_return c.json(service::common::operation(c, "设备分组分享已更新"));
    }

    ruvia::task<ruvia::http_response> groupCreate(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().createGroup(request, c.req().validated<CreateDeviceGroupBody>());
        co_return c.json(service::common::operation(c, "创建成功"));
    }

    ruvia::task<ruvia::http_response> groupUpdate(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().updateGroup(request, id(c), c.req().validated<UpdateDeviceGroupBody>());
        co_return c.json(service::common::operation(c, "更新成功"));
    }

    ruvia::task<ruvia::http_response> groupRemove(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().removeGroup(request, id(c));
        co_return c.json(service::common::operation(c, "删除成功"));
    }
};

} // namespace service::device
