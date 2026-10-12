#pragma once

#include <string>
#include <string_view>

#include <ruvia/web/controller.h>
#include <ruvia/web/model_json.h>

#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/middleware/request_context.h"
#include "service/modules/system/auth/auth.service.h"

namespace service::middleware {

inline service::auth::JwtPayload requireAuth(ruvia::context& c) {
    const auto token = ruvia::jwt_bearer_token(c.req().header("Authorization").value_or(""));
    if (!token) {
        service::common::fail(service::common::kUnauthorizedErrorCode, "未登录", 401);
    }
    try {
        return service::auth::AuthTokenService::verifyAccessToken(c, *token);
    } catch (const service::auth::JwtExpiredError&) {
        service::common::fail(service::common::kTokenExpiredErrorCode, "Token 已过期", 401);
    } catch (...) {
        service::common::fail(service::common::kTokenInvalidErrorCode, "Token 无效", 401);
    }
}

class AuthMiddleware final {
  public:
    ruvia::task<void> handle(ruvia::context& c, ruvia::next& next) {
        (void)requireAuth(c);
        if (c.req().header("X-SSE-User").value_or("") == "1") {
            const AuthenticatedUserSnapshot snapshot{ [](RequestContext& request) -> ruvia::task<std::string> {
                const auto response = service::common::ok<service::auth::CurrentUserResponse>(
                    request,
                    co_await service::auth::authService().current(request, request.userId)
                );
                const auto encoded = ruvia::to_json(response);
                co_return std::string(encoded.data(), encoded.size());
            } };
            const auto binding = c.bind_request_state(snapshot);
            co_await next();
        } else {
            co_await next();
        }
    }
};

} // namespace service::middleware
