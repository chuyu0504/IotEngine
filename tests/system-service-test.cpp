#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/web/controller.h>
#include <ruvia/web/testing.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/middleware/auth.h"
#include "service/middleware/permission.h"
#include "service/modules/system/role/role.types.h"

namespace {

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::atomic<int> permissionChecks{ 0 };

service::middleware::AccessTokenCheck verifyProbeToken(ruvia::context&, std::string_view token) {
    if (token == "valid") {
        return { service::middleware::AccessTokenState::valid, "user-1" };
    }
    if (token == "expired") {
        return { service::middleware::AccessTokenState::expired, {} };
    }
    return { service::middleware::AccessTokenState::invalid, {} };
}

ruvia::task<void> requireProbePermission(service::middleware::RequestContext&, std::string_view userId, std::string_view permission) {
    ++permissionChecks;
    if (userId != "user-1" || permission != "probe:allowed") {
        service::common::fail(service::common::kPermissionDeniedErrorCode, "无权限", 403);
    }
    co_return;
}

ruvia::task<std::string> probeUserSnapshot(service::middleware::RequestContext& request) {
    co_return "{\"user\":\"" + request.userId + "\"}";
}

ruvia::task<ruvia::http_response> renderError(ruvia::context& c, ruvia::http_error_info info) {
    c.status(info.status());
    const auto body = std::string(info.code()) + ":" + std::string(info.message());
    co_return c.text(std::string_view(body));
}

class AuthProbeController final {
  public:
    RUVIA_CONTROLLER_GROUP("/probe", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(AuthProbeController)
    RUVIA_GET("/me", me);
    RUVIA_GET("/allowed", me, service::middleware::PermissionMiddleware<"probe:allowed">);
    RUVIA_GET("/denied", me, service::middleware::PermissionMiddleware<"probe:denied">);
    RUVIA_GET("/snapshot", snapshot);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> me(ruvia::context& c) {
        const auto user = service::middleware::requireAuth(c);
        co_return c.text(std::string_view(user.userId));
    }

    ruvia::task<ruvia::http_response> snapshot(ruvia::context& c) {
        const auto* user = &c.request_state<service::middleware::AuthenticatedUserSnapshot>();
        if (user->query == nullptr) {
            co_return c.text(std::string_view("none"));
        }
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = co_await user->query(request);
        co_return c.text(std::string_view(payload));
    }
};

ruvia::test_request probe(std::string_view path, std::optional<std::string_view> token) {
    auto request = ruvia::test_request::get(path);
    if (token) {
        request.header("Authorization", "Bearer " + std::string(*token));
    }
    return request;
}

void requireResponse(ruvia::test_app& app, ruvia::test_request request, std::uint16_t status, std::string_view body, const char* message) {
    const auto response = app.request(request);
    if (response.status().value() != status || response.body() != body) {
        std::cerr << "status=" << response.status().value() << " body=" << response.body() << '\n';
        throw std::runtime_error(message);
    }
}

void requireInjectedAuthentication() {
    ruvia::test_app app;
    app.use_worker_state<service::middleware::AuthPort>([] {
        return service::middleware::AuthPort{ &verifyProbeToken, &requireProbePermission, &probeUserSnapshot };
    });
    app.on_error(&renderError);

    requireResponse(app, probe("/probe/me", std::nullopt), 401, "11004:未登录", "missing token must be rejected as not logged in");
    requireResponse(app, probe("/probe/me", "expired"), 401, "11005:Token 已过期", "expired token must be rejected");
    requireResponse(app, probe("/probe/me", "forged"), 401, "11006:Token 无效", "invalid token must be rejected");
    requireResponse(app, probe("/probe/me", "valid"), 200, "user-1", "valid token must reach the handler");

    permissionChecks = 0;
    requireResponse(app, probe("/probe/denied", std::nullopt), 401, "11004:未登录", "authentication must run before permission");
    requireResponse(app, probe("/probe/allowed", "forged"), 401, "11006:Token 无效", "invalid token must not reach permission");
    require(permissionChecks == 0, "permission ran for an unauthenticated request");
    requireResponse(app, probe("/probe/denied", "valid"), 403, "11007:无权限", "missing permission must be forbidden");
    requireResponse(app, probe("/probe/allowed", "valid"), 200, "user-1", "granted permission must reach the handler");
    require(permissionChecks == 2, "permission middleware did not check each protected request once");

    requireResponse(app, probe("/probe/snapshot", "valid"), 200, "none", "user snapshot bound without X-SSE-User");
    auto sse = probe("/probe/snapshot", "valid");
    sse.header("X-SSE-User", "1");
    requireResponse(app, sse, 200, "{\"user\":\"user-1\"}", "X-SSE-User did not bind the injected user snapshot");
}

void requireUnwiredAuthenticationFailsClosed() {
    ruvia::test_app app;
    app.use_worker_state<service::middleware::AuthPort>();
    app.on_error(&renderError);
    requireResponse(app, probe("/probe/me", std::nullopt), 401, "11004:未登录", "missing token must still be rejected");
    requireResponse(app, probe("/probe/me", "valid"), 500, "10004:认证能力未装配", "unwired authentication must fail closed");
    requireResponse(app, probe("/probe/allowed", "valid"), 500, "10004:认证能力未装配", "unwired permission must fail closed");
}

ruvia::array<ruvia::string> permissionList(std::initializer_list<std::string_view> values) {
    ruvia::array<ruvia::string> result;
    for (const auto value : values) {
        result.emplace_back(value);
    }
    return result;
}

void requirePermissionRejected(std::initializer_list<std::string_view> values, std::string_view message) {
    try {
        (void)service::role::permissionsJson(permissionList(values));
    } catch (const ruvia::http_error& error) {
        const auto info = error.info();
        require(info.status().value() == 400 && info.code() == "13005" && info.message() == message, "role permission rejection changed code or message");
        return;
    }
    throw std::runtime_error("role permission validation accepted an invalid code");
}

void requireRolePermissionValidation() {
    require(service::role::permissionsJson(std::nullopt) == "[]", "missing permissions must store an empty array");
    require(service::role::permissionsJson(permissionList({ "iot:device:query", "a\"b" })) == R"(["iot:device:query","a\"b"])", "role permissions are not encoded as a JSON string array");
    const std::string longest(128, 'p');
    require(service::role::permissionsJson(permissionList({ longest })) == "[\"" + longest + "\"]", "128-character permission code rejected");
    requirePermissionRejected({ "" }, "权限编码不能为空且不能超过 128 个字符");
    requirePermissionRejected({ std::string(129, 'p') }, "权限编码不能为空且不能超过 128 个字符");
    requirePermissionRejected({ "iot:device:query", "bad,permission" }, "权限编码不能包含逗号");
}

void requireWorkerMetricSnapshotContract() {
    const auto worker0 = service::message::worker_metrics::snapshotId(0, "test");
    const auto worker1 = service::message::worker_metrics::snapshotId(1, "test");
    require(worker0 != worker1, "worker snapshots share an identity");
    require(worker0 == "test:worker:0", "snapshot omits process and worker identity");
    require(worker0 != service::message::worker_metrics::snapshotId(0, "other"), "snapshots from different processes share an identity");
}

} // namespace

int main() {
    try {
        requireInjectedAuthentication();
        requireUnwiredAuthenticationFailsClosed();
        requireRolePermissionValidation();
        requireWorkerMetricSnapshotContract();
        std::cout << "system service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "system service test failed: " << error.what() << '\n';
        return 1;
    }
}
