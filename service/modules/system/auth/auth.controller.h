#pragma once

#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/modules/system/auth/auth.types.h"
#include "service/modules/system/auth/auth.service.h"

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

  private:
    ruvia::task<ruvia::http_response> login(ruvia::context& c) {

        co_return c.json(service::common::ok<LoginResponse>(
            c, co_await authService().login(c, c.req().validated<LoginBody>())));
    }
    ruvia::task<ruvia::http_response> refresh(ruvia::context& c) {

        co_return c.json(service::common::ok<LoginResponse>(
            c, co_await authService().refresh(c, c.req().validated<RefreshBody>())));
    }
    ruvia::task<ruvia::http_response> logout(ruvia::context& c) {
        co_return c.json(service::common::operation(c, "退出成功"));
    }
    ruvia::task<ruvia::http_response> me(ruvia::context& c) {
        const auto principal = service::middleware::requireAuth(c);
        co_return c.json(service::common::ok<CurrentUserResponse>(
            c, co_await authService().current(c, principal.userId)));
    }
};

} // namespace service::auth
