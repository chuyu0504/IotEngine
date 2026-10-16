#pragma once

#include <string_view>

#include <ruvia/web/controller.h>
#include <ruvia/web/fixed_string.h>

#include "service/middleware/auth.h"
#include "service/middleware/request_context.h"

namespace service::middleware {

// 已认证请求或 SSE 事件查询按当前用户复核权限码；无权限时 403。
inline ruvia::task<void> requirePermission(RequestContext& request, std::string_view permission) {
    co_await authPort(request).requirePermission(request, request.userId, permission);
}

template <ruvia::fixed_string Permission>
class PermissionMiddleware final {
  public:
    ruvia::task<void> handle(ruvia::context& c, ruvia::next& next) {
        {
            RequestContext request(c, requireAuth(c).userId);
            co_await requirePermission(request, Permission.view());
        }
        co_await next();
    }
};

} // namespace service::middleware
