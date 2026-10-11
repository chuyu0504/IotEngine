#pragma once

#include <string_view>

#include <ruvia/web/controller.h>
#include <ruvia/web/fixed_string.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/modules/system/auth/auth.service.h"

namespace service::middleware {

inline ruvia::task<void> requirePermission(ruvia::context& c, std::string_view permission) {
    const auto principal = requireAuth(c);
    co_await service::auth::AuthService::requirePermission(c, principal.userId, permission);
}

template <ruvia::fixed_string Permission>
class PermissionMiddleware final : public ruvia::middleware {
  public:
    ruvia::task<void> handle(ruvia::context& c, ruvia::next& next) {
        co_await requirePermission(c, Permission.view());
        co_await next();
    }
};

} // namespace service::middleware
