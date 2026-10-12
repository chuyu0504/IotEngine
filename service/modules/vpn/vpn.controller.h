#pragma once
#include <ruvia/web/controller.h>
#include <ruvia/core/task_scope.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/permission.h"
#include "service/middleware/validation.h"
#include "service/modules/vpn/vpn.types.h"
#include "service/modules/vpn/vpn.service.h"
#include "service/utils/json.h"

namespace service::vpn {
class VpnController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/vpn", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(VpnController)
    RUVIA_GET("/networks", listNetworks, service::middleware::PermissionMiddleware<"iot:vpn:query">, ruvia::query_model<VpnListQuery>);
    RUVIA_PATCH("/routes/:id", updateRoute, service::middleware::PermissionMiddleware<"iot:vpn:edit">, ruvia::path_model<VpnIdParams>, service::middleware::ValidationErrorCodeMiddleware<21001>, ruvia::json_body<VpnRoutePatchBody>);
    RUVIA_POST("/peers", createPeer, service::middleware::PermissionMiddleware<"iot:vpn:add">, service::middleware::ValidationErrorCodeMiddleware<21001>, ruvia::json_body<VpnPeerBody>);
    RUVIA_POST("/peers/:id/revoke", revokePeer, service::middleware::PermissionMiddleware<"iot:vpn:revoke">, ruvia::path_model<VpnIdParams>);
    RUVIA_POST("/peers/:id/sync", syncPeer, service::middleware::PermissionMiddleware<"iot:vpn:edit">, ruvia::path_model<VpnIdParams>);
    RUVIA_GET("/desktop/devices", desktopDevices, service::middleware::PermissionMiddleware<"iot:vpn:query">, service::middleware::PermissionMiddleware<"iot:vpn:enroll">, service::middleware::PermissionMiddleware<"iot:edge:query">);
    RUVIA_POST("/desktop/peers", desktopCreatePeer, service::middleware::PermissionMiddleware<"iot:vpn:query">, service::middleware::PermissionMiddleware<"iot:vpn:enroll">, service::middleware::PermissionMiddleware<"iot:edge:query">, service::middleware::ValidationErrorCodeMiddleware<21001>, ruvia::json_body<VpnDesktopPeerBody>);
    RUVIA_PATCH("/desktop/peers/:id", desktopUpdatePeer, service::middleware::PermissionMiddleware<"iot:vpn:query">, service::middleware::PermissionMiddleware<"iot:vpn:enroll">, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<VpnIdParams>, service::middleware::ValidationErrorCodeMiddleware<21001>, ruvia::json_body<VpnDesktopSelectionBody>);
    RUVIA_GET_SSE("/desktop/peers/:id/config/events", desktopPeerConfigEvents, service::middleware::PermissionMiddleware<"iot:vpn:query">, service::middleware::PermissionMiddleware<"iot:vpn:enroll">, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<VpnIdParams>);
    RUVIA_GET("/desktop/peers/:id/config", desktopPeerConfig, service::middleware::PermissionMiddleware<"iot:vpn:query">, service::middleware::PermissionMiddleware<"iot:vpn:enroll">, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<VpnIdParams>);
    RUVIA_DELETE("/desktop/peers/:id", desktopDeletePeer, service::middleware::PermissionMiddleware<"iot:vpn:enroll">, ruvia::path_model<VpnIdParams>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> listNetworks(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<VpnListQuery>();
        const auto keyword = body.get<"keyword">() ? std::optional<std::string>(body.get<"keyword">()->view()) : std::nullopt;
        const auto status = body.get<"status">() ? std::optional<std::string>(body.get<"status">()->view()) : std::nullopt;
        const auto payload = service::live::data(c, co_await vpnService().networks(request, *body.get<"page">(), *body.get<"pageSize">(), keyword, status));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<ruvia::http_response> updateRoute(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<VpnRoutePatchBody>();
        co_await vpnService().updateRoute(request, c.req().validated<VpnIdParams>().get<"id">().view(), body);
        const auto payload = service::live::data(c, "null");
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<ruvia::http_response> createPeer(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<VpnPeerBody>();
        const auto id = co_await vpnService().createPeer(request, body);
        const auto payload = service::live::data(c, "{\"id\":" + service::utils::jsonQuoted(id) + "}");
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<ruvia::http_response> revokePeer(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await vpnService().revokePeer(request, c.req().validated<VpnIdParams>().get<"id">().view());
        const auto payload = service::live::data(c, "null");
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<ruvia::http_response> syncPeer(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await vpnService().syncPeer(request, c.req().validated<VpnIdParams>().get<"id">().view());
        const auto payload = service::live::data(c, "null");
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<ruvia::http_response> desktopDevices(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = service::live::data(c, co_await vpnService().desktopDevices(request));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<ruvia::http_response> desktopCreatePeer(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<VpnDesktopPeerBody>();
        const auto payload = service::live::data(c, co_await vpnService().desktopCreatePeer(request, body));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<ruvia::http_response> desktopUpdatePeer(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<VpnDesktopSelectionBody>();
        const auto payload = service::live::data(c, co_await vpnService().desktopUpdatePeer(request, c.req().validated<VpnIdParams>().get<"id">().view(), body));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<ruvia::http_response> desktopPeerConfig(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = service::live::data(c, co_await vpnService().desktopPeerConfig(request, c.req().validated<VpnIdParams>().get<"id">().view()));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
    ruvia::task<void> desktopPeerConfigEvents(ruvia::context& c) {
        const auto userId = service::middleware::requireAuth(c).userId;
        const std::string peerId(c.req().validated<VpnIdParams>().get<"id">().view());
        if (c.req().header("Accept").value_or("").find("text/event-stream") == std::string_view::npos) {
            service::common::fail(10002, "此接口需要 text/event-stream", 406);
        }
        service::middleware::RequestContext request(c, userId);
        co_await service::auth::AuthService::requirePermission(request, userId, "iot:vpn:enroll");
        co_await service::auth::AuthService::requirePermission(request, userId, "iot:edge:query");
        const auto connectionId = co_await vpnService().openDesktopSession(request, peerId);
        ruvia::task_scope leases(c.worker(), ruvia::task_scope_options{.resource_ = c.pool()});
        std::exception_ptr failure;
        std::exception_ptr leaseFailure;
        auto renewLease = [&]() -> ruvia::task<void> {
            try {
                while (!leases.stop_requested() && !c.get_stop_token().stop_requested()) {
                    (void)co_await ruvia::sleep_for(c.worker(), std::chrono::seconds(15), leases.get_stop_token());
                    if (leases.stop_requested() || c.get_stop_token().stop_requested()) break;
                    service::middleware::RequestContext renewal(c, userId, leases.get_stop_token());
                    // Redis resource maintenance only: no business query or edge traffic.
                    co_await vpnService().renewDesktopSession(renewal, peerId, connectionId);
                }
            } catch (...) { leaseFailure = std::current_exception(); }
        };
        leases.spawn(renewLease());
        std::vector<service::live::SnapshotChannel> channels;
        channels.push_back({"snapshot", "vpn", [&c, &peerId](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
            co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:vpn:query");
            co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:vpn:enroll");
            co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
            co_return service::live::data(c, co_await vpnService().desktopPeerConfig(request, peerId));
        }});
        try {
            co_await service::live::serveSnapshotChannels(c, userId, std::move(channels), [&] {
                if (leaseFailure) std::rethrow_exception(leaseFailure);
                (void)service::middleware::requireAuth(c);
            });
        } catch (...) { failure = std::current_exception(); }
        leases.request_stop();
        co_await leases.join();
        // Cleanup must survive cancellation of the HTTP connection. Expiry is
        // the fallback if Redis or the API process itself is unavailable.
        try {
            service::middleware::RequestContext cleanup(c, userId, {});
            co_await vpnService().closeDesktopSession(cleanup, peerId, connectionId);
        } catch (...) { if (!failure) failure = std::current_exception(); }
        if (failure) std::rethrow_exception(failure);
    }
    ruvia::task<ruvia::http_response> desktopDeletePeer(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await vpnService().desktopDeletePeer(request, c.req().validated<VpnIdParams>().get<"id">().view());
        const auto payload = service::live::data(c, "null");
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }
};
} // namespace service::vpn
