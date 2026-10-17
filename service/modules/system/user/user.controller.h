#pragma once

#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/permission.h"
#include "service/modules/system/user/user.types.h"
#include "service/modules/system/user/user.service.h"

namespace service::user {

class UserController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/users", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(UserController)
    RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"system:user:query">, ruvia::query_model<UserListQuery>);
    RUVIA_GET("/options", options, service::middleware::PermissionMiddleware<"system:user:query">, ruvia::query_model<UserOptionsQuery>);
    RUVIA_POST("/", create, service::middleware::PermissionMiddleware<"system:user:add">, ruvia::json_body<CreateUserBody>);
    RUVIA_PUT("/:id", update, service::middleware::PermissionMiddleware<"system:user:edit">, ruvia::path_model<UserIdParams>, ruvia::json_body<UpdateUserBody>);
    RUVIA_DELETE("/:id", remove, service::middleware::PermissionMiddleware<"system:user:delete">, ruvia::path_model<UserIdParams>);
    RUVIA_ROUTES_END

  private:
    static std::string id(ruvia::context& c) {
        return std::string(c.req().validated<UserIdParams>().get<"id">().view());
    }

    ruvia::task<ruvia::http_response> list(ruvia::context& c) {
        const auto& query = c.req().validated<UserListQuery>();
        const auto page = static_cast<std::int64_t>(*query.get<"page">());
        const auto pageSize = static_cast<std::int64_t>(*query.get<"pageSize">());
        const auto keyword = query.get<"keyword">()
                                 ? std::optional<std::string>(std::string(query.get<"keyword">()->view()))
                                 : std::nullopt;
        const auto status = query.get<"status">()
                                ? std::optional<std::string>(std::string(query.get<"status">()->view()))
                                : std::nullopt;
        co_return c.json(service::common::ok<UserPageResponse>(
            c, co_await UserService::instance().list(c, page, pageSize, keyword, status)));
    }

    ruvia::task<ruvia::http_response> options(ruvia::context& c) {
        const auto& query = c.req().validated<UserOptionsQuery>();
        const auto keyword = query.get<"keyword">()
                                 ? std::optional<std::string>(std::string(query.get<"keyword">()->view()))
                                 : std::nullopt;
        co_return c.json(service::common::ok<UserOptionsResponse>(
            c, co_await UserService::instance().options(c, keyword)));
    }

    ruvia::task<ruvia::http_response> create(ruvia::context& c) {
        co_await UserService::instance().create(c, c.req().validated<CreateUserBody>());
        co_return c.json(service::common::operation(c, "创建成功"));
    }

    ruvia::task<ruvia::http_response> update(ruvia::context& c) {
        co_await UserService::instance().update(c, id(c), c.req().validated<UpdateUserBody>());
        co_return c.json(service::common::operation(c, "更新成功"));
    }

    ruvia::task<ruvia::http_response> remove(ruvia::context& c) {
        const auto principal = service::middleware::requireAuth(c);
        co_await UserService::instance().remove(c, id(c), principal.userId);
        co_return c.json(service::common::operation(c, "删除成功"));
    }
};

} // namespace service::user
