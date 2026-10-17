#pragma once
#include <ruvia/web/controller.h>

#include "service/middleware/auth.h"
#include "service/middleware/permission.h"
#include "service/modules/command/command.service.h"
#include "service/modules/command/command.types.h"

namespace service::command {
class CommandController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/device", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(CommandController)
    RUVIA_POST("/:id/commands", create, service::middleware::PermissionMiddleware<"iot:device:command">, ruvia::path_model<CommandStatusQuery>, ruvia::json_body<SubmitCommandBody>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> create(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& id = c.req().validated<CommandStatusQuery>().get<"id">();
        co_return c.json(service::common::ok<service::device::DeviceCommandCreateResponse>(request, co_await CommandService::instance().create(request, id.view(), c.req().validated<SubmitCommandBody>())));
    }
};
} // namespace service::command
