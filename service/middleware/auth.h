#pragma once

#include <string>
#include <string_view>
#include <utility>

#include <ruvia/web/auth/jwt.h>
#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/request_context.h"

namespace service::middleware {

enum class AccessTokenState { valid,
                              expired,
                              invalid };

struct AccessTokenCheck {
    AccessTokenState state = AccessTokenState::invalid;
    std::string userId;
};

struct AuthenticatedUser {
    std::string userId;
};

// 认证与权限能力由应用入口按 Worker 注入，middleware 不依赖业务模块。
// 任一能力未装配时拒绝请求。
struct AuthPort final {
    // 过期返回 expired；签名、类型、载荷或密钥配置无效返回 invalid。
    AccessTokenCheck (*verifyAccessToken)(ruvia::context&, std::string_view token) = nullptr;
    // 无权限时以 403「无权限」失败。
    ruvia::task<void> (*requirePermission)(RequestContext&, std::string_view userId, std::string_view permission) = nullptr;
    // X-SSE-User 订阅的当前用户快照（统一响应 JSON）。
    ruvia::task<std::string> (*currentUserSnapshot)(RequestContext&) = nullptr;
};

template <typename Context>
const AuthPort& authPort(Context& c) {
    const auto& port = c.template worker_state<AuthPort>();
    if (port.verifyAccessToken == nullptr || port.requirePermission == nullptr || port.currentUserSnapshot == nullptr) {
        service::common::fail(service::common::kServerErrorCode, "认证能力未装配", 500);
    }
    return port;
}

inline AuthenticatedUser requireAuth(ruvia::context& c) {
    const auto token = ruvia::jwt_bearer_token(c.req().header("Authorization").value_or(""));
    if (!token) {
        service::common::fail(service::common::kUnauthorizedErrorCode, "未登录", 401);
    }
    auto check = authPort(c).verifyAccessToken(c, *token);
    switch (check.state) {
        case AccessTokenState::valid:
            return AuthenticatedUser{ std::move(check.userId) };
        case AccessTokenState::expired:
            service::common::fail(service::common::kTokenExpiredErrorCode, "Token 已过期", 401);
        case AccessTokenState::invalid:
            break;
    }
    service::common::fail(service::common::kTokenInvalidErrorCode, "Token 无效", 401);
}

class AuthMiddleware final {
  public:
    ruvia::task<void> handle(ruvia::context& c, ruvia::next& next) {
        (void)requireAuth(c);
        const AuthenticatedUserSnapshot snapshot{
            c.req().header("X-SSE-User").value_or("") == "1" ? authPort(c).currentUserSnapshot : nullptr
        };
        const auto binding = c.bind_request_state(snapshot);
        co_await next();
    }
};

} // namespace service::middleware
