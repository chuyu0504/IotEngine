#pragma once

#include <string>
#include <string_view>

#include <ruvia/web/controller.h>
#include <ruvia/web/model_json.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/modules/system/auth/auth.service.h"
#include "service/modules/system/auth/auth.types.h"

namespace service::auth {

class AuthController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/auth")
    RUVIA_ROUTES_BEGIN(AuthController)
    RUVIA_POST("/login", login, ruvia::json_body<LoginBody>);
    RUVIA_POST("/refresh", refresh, ruvia::json_body<RefreshBody>);
    RUVIA_POST("/logout", logout);
    RUVIA_GET("/me", me, service::middleware::AuthMiddleware);
    RUVIA_ROUTES_END

  public:
    // 由应用入口注入 service::middleware::AuthPort。
    static service::middleware::AuthPort port() {
        return service::middleware::AuthPort{
            &verifyAccessToken,
            &AuthService::requirePermission<service::middleware::RequestContext>,
            &currentUserSnapshot,
        };
    }

  private:
    static service::middleware::AccessTokenCheck verifyAccessToken(ruvia::context& c, std::string_view token) {
        try {
            return { service::middleware::AccessTokenState::valid, AuthTokenService::verifyAccessToken(c, token).userId };
        } catch (const JwtExpiredError&) {
            return { service::middleware::AccessTokenState::expired, {} };
        } catch (...) {
            return { service::middleware::AccessTokenState::invalid, {} };
        }
    }

    static ruvia::task<std::string> currentUserSnapshot(service::middleware::RequestContext& request) {
        const auto response = service::common::ok<CurrentUserResponse>(request, co_await AuthService::instance().current(request, request.userId));
        const auto encoded = ruvia::to_json(response);
        co_return std::string(encoded.data(), encoded.size());
    }

    ruvia::task<ruvia::http_response> login(ruvia::context& c) {

        co_return c.json(service::common::ok<LoginResponse>(c, co_await AuthService::instance().login(c, c.req().validated<LoginBody>())));
    }

    ruvia::task<ruvia::http_response> refresh(ruvia::context& c) {

        co_return c.json(service::common::ok<LoginResponse>(c, co_await AuthService::instance().refresh(c, c.req().validated<RefreshBody>())));
    }

    ruvia::task<ruvia::http_response> logout(ruvia::context& c) {
        co_return c.json(service::common::operation(c, "退出成功"));
    }

    ruvia::task<ruvia::http_response> me(ruvia::context& c) {
        const auto principal = service::middleware::requireAuth(c);
        co_return c.json(service::common::ok<CurrentUserResponse>(c, co_await AuthService::instance().current(c, principal.userId)));
    }
};

} // namespace service::auth
