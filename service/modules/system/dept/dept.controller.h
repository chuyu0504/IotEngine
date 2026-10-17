#pragma once

#include <optional>
#include <string>

#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/permission.h"
#include "service/modules/system/dept/dept.types.h"
#include "service/modules/system/dept/dept.service.h"

namespace service::dept {

class DeptController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/departments", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(DeptController)
    RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"system:dept:query">, ruvia::query_model<DeptListQuery>);
    RUVIA_GET("/options", options, service::middleware::PermissionMiddleware<"system:dept:query">);
    RUVIA_POST("/", create, service::middleware::PermissionMiddleware<"system:dept:add">, ruvia::json_body<CreateDeptBody>);
    RUVIA_PUT("/:id", update, service::middleware::PermissionMiddleware<"system:dept:edit">, ruvia::path_model<DeptIdParams>, ruvia::json_body<UpdateDeptBody>);
    RUVIA_DELETE("/:id", remove, service::middleware::PermissionMiddleware<"system:dept:delete">, ruvia::path_model<DeptIdParams>);
    RUVIA_ROUTES_END

  private:
    static std::string id(ruvia::context& c) {
        return std::string(c.req().validated<DeptIdParams>().get<"id">().view());
    }

    ruvia::task<ruvia::http_response> list(ruvia::context& c) {
        const auto& query = c.req().validated<DeptListQuery>();
        const auto keyword = query.get<"keyword">()
                                 ? std::optional<std::string>(std::string(query.get<"keyword">()->view()))
                                 : std::nullopt;
        const auto status = query.get<"status">()
                                ? std::optional<std::string>(std::string(query.get<"status">()->view()))
                                : std::nullopt;
        const auto parentId =
            query.get<"parentId">() ? std::optional<std::string>(std::string(query.get<"parentId">()->view()))
                             : std::nullopt;
        co_return c.json(service::common::ok<DeptPageResponse>(
            c, co_await DeptService::instance().list(c, static_cast<std::int64_t>(*query.get<"page">()),
                                           static_cast<std::int64_t>(*query.get<"pageSize">()), keyword,
                                           status, parentId)));
    }

    ruvia::task<ruvia::http_response> options(ruvia::context& c) {
        co_return c.json(service::common::ok<DeptOptionsResponse>(c, co_await DeptService::instance().options(c)));
    }

    ruvia::task<ruvia::http_response> create(ruvia::context& c) {
        co_await DeptService::instance().create(c, c.req().validated<CreateDeptBody>());
        co_return c.json(service::common::operation(c, "创建成功"));
    }

    ruvia::task<ruvia::http_response> update(ruvia::context& c) {
        co_await DeptService::instance().update(c, id(c), c.req().validated<UpdateDeptBody>());
        co_return c.json(service::common::operation(c, "更新成功"));
    }

    ruvia::task<ruvia::http_response> remove(ruvia::context& c) {
        co_await DeptService::instance().remove(c, id(c));
        co_return c.json(service::common::operation(c, "删除成功"));
    }
};

} // namespace service::dept
