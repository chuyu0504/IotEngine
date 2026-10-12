#pragma once

#include <chrono>
#include <optional>
#include <string>

#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/permission.h"
#include "service/middleware/validation.h"
#include "service/modules/link/link.types.h"
#include "service/modules/link/link.service.h"

namespace service::link {

class LinkController final : public ruvia::controller<LinkController> {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/link", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN
    RUVIA_GET_SSE("/events", linkEvents, service::middleware::PermissionMiddleware<"iot:link:query">, ruvia::query_model<LinkListQuery>, ruvia::query_model<LinkEventsQuery>);
    RUVIA_PUT("/:id/debug", setDebug, service::middleware::PermissionMiddleware<"iot:link:edit">, ruvia::path_model<LinkIdParams>, ruvia::json_body<LinkDebugBody>);
    RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"iot:link:query">, ruvia::query_model<LinkListQuery>);
    RUVIA_GET("/enums", enums, service::middleware::PermissionMiddleware<"iot:link:query">);
    RUVIA_GET("/public-ip", publicIp, service::middleware::PermissionMiddleware<"iot:link:query">);
    RUVIA_POST("/", create, service::middleware::PermissionMiddleware<"iot:link:add">, service::middleware::ValidationErrorCodeMiddleware<10001, "name", 15002, "too_small">, ruvia::json_body<SaveLinkBody>);
    RUVIA_PUT("/:id", update, service::middleware::PermissionMiddleware<"iot:link:edit">, ruvia::path_model<LinkIdParams>, service::middleware::ValidationErrorCodeMiddleware<10001, "name", 15002, "too_small">, ruvia::json_body<SaveLinkBody>);
    RUVIA_DELETE("/:id", remove, service::middleware::PermissionMiddleware<"iot:link:delete">, ruvia::path_model<LinkIdParams>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<LinkDebugPacketsResponse> queryDebugPackets(ruvia::context& c, service::middleware::RequestContext& request, std::string_view linkId) {
        co_return service::common::ok<LinkDebugPacketsResponse>(request, co_await linkService().debugPackets(request, linkId));
    }

    ruvia::task<ruvia::http_response> setDebug(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await linkService().setDebug(request, id(c), c.req().validated<LinkDebugBody>().get<"enabled">());
        co_return c.json(service::common::operation(c, "调试设置已保存"));
    }

    static std::string id(ruvia::context& c) {
        return std::string(c.req().validated<LinkIdParams>().get<"id">().view());
    }

    static std::optional<std::string> text(const std::optional<ruvia::string>& value) {
        return value ? std::optional<std::string>(std::string(value->view())) : std::nullopt;
    }

    ruvia::task<ruvia::http_response> list(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(co_await queryList(c, request));
    }

    ruvia::task<void> linkEvents(ruvia::context& c) {

        service::middleware::RequestContext access(c, service::middleware::requireAuth(c).userId);
        std::vector<service::live::SnapshotChannel> channels;
        channels.push_back({ "links", "link", [this, &c](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:link:query");
                                co_return service::live::json(co_await queryList(c, request));
                            } });
        std::string debugId;
        bool debugAuthorized = false;
        if (const auto& value = c.req().validated<LinkEventsQuery>().get<"debugLinkId">()) {
            debugId = std::string(value->view());
            channels.push_back({ "packets", "packet-debug", [this, &c, &debugAuthorized, linkId = debugId](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                    debugAuthorized = false;
                                    co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:link:edit");
                                    const auto payload = service::live::json(co_await queryDebugPackets(c, request, linkId));
                                    debugAuthorized = true;
                                    co_return payload;
                                } });
        }
        co_await service::live::serveSnapshotChannels(
            c,
            access.userId,
            std::move(channels),
            [&c] { (void)service::middleware::requireAuth(c); },
            [&c, &debugAuthorized, debugId]() -> ruvia::task<void> {
                if (!debugId.empty() && debugAuthorized) {
                    (void)co_await c.redis().expire(service::debug_idle::leaseKey("link", debugId),
                        std::chrono::duration_cast<std::chrono::seconds>(service::debug_idle::kIdle));
                }
                co_return;
            });
    }

    ruvia::task<LinkPageResponse> queryList(ruvia::context& c, service::middleware::RequestContext& request) {
        const auto& query = c.req().validated<LinkListQuery>();
        co_return service::common::ok<LinkPageResponse>(request, co_await linkService().list(request, static_cast<std::int64_t>(*query.get<"page">()), static_cast<std::int64_t>(*query.get<"pageSize">()), text(query.get<"keyword">()), text(query.get<"mode">()), text(query.get<"protocol">()), text(query.get<"status">())));
    }

    ruvia::task<ruvia::http_response> enums(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<LinkEnumsResponse>(request, linkService().enums(request)));
    }

    ruvia::task<ruvia::http_response> publicIp(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        PublicIpDto result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"ip">(co_await linkService().publicIp(request));
        co_return c.json(service::common::ok<PublicIpResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> create(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await linkService().create(request, c.req().validated<SaveLinkBody>());
        co_return c.json(service::common::operation(c, "创建成功"));
    }

    ruvia::task<ruvia::http_response> update(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await linkService().update(request, id(c), c.req().validated<SaveLinkBody>());
        co_return c.json(service::common::operation(c, "更新成功"));
    }

    ruvia::task<ruvia::http_response> remove(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await linkService().remove(request, id(c));
        co_return c.json(service::common::operation(c, "删除成功"));
    }
};

} // namespace service::link
