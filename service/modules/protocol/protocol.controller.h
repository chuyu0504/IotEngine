#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <ruvia/web/controller.h>
#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/permission.h"
#include "service/middleware/validation.h"
#include "service/modules/protocol/protocol.types.h"
#include "service/modules/protocol/protocol.service.h"

namespace service::protocol {

class ProtocolController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/protocol/configs", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(ProtocolController)
    RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"iot:protocol:query">, ruvia::query_model<ProtocolListQuery>);
    RUVIA_GET("/options", options, service::middleware::PermissionMiddleware<"iot:protocol:query">, ruvia::query_model<ProtocolListQuery>);
    RUVIA_GET("/:id", detail, service::middleware::PermissionMiddleware<"iot:protocol:query">, ruvia::path_model<ProtocolIdParams>);
    RUVIA_POST("/", create, service::middleware::PermissionMiddleware<"iot:protocol:add">, service::middleware::ValidationErrorCodeMiddleware<16002, "enabled", 16004>, ruvia::json_body<CreateProtocolBody>);
    RUVIA_POST("/test-expression", testExpression, service::middleware::PermissionMiddleware<"iot:protocol:query">, ruvia::json_body<ExpressionTestBody>);
    RUVIA_POST("/preview-mqtt", previewMqtt, service::middleware::PermissionMiddleware<"iot:protocol:query">, ruvia::json_body<MqttPreviewBody>);
    RUVIA_PUT("/:id", update, service::middleware::PermissionMiddleware<"iot:protocol:edit">, ruvia::path_model<ProtocolIdParams>, service::middleware::ValidationErrorCodeMiddleware<16002, "enabled", 16004>, ruvia::json_body<UpdateProtocolBody>);
    RUVIA_DELETE("/:id", remove, service::middleware::PermissionMiddleware<"iot:protocol:delete">, ruvia::path_model<ProtocolIdParams>);
    RUVIA_ROUTES_END

  private:
    static std::string id(ruvia::context& c) {
        return std::string(c.req().validated<ProtocolIdParams>().get<"id">().view());
    }

    ruvia::task<ruvia::http_response> list(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& query = c.req().validated<ProtocolListQuery>();
        const auto& protocolValue = query.get<"protocol">();
        const auto protocol = protocolValue ? std::optional<std::string>(protocolValue->view()) : std::nullopt;
        const auto data = co_await protocolService().list(request, static_cast<std::int64_t>(*query.get<"page">()), static_cast<std::int64_t>(*query.get<"pageSize">()), protocol);
        c.header("Content-Type", "application/json");
        const auto payload = service::live::data(c, data);
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<ruvia::http_response> options(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& query = c.req().validated<ProtocolListQuery>();
        const auto& protocol = query.get<"protocol">();
        if (!protocol) service::common::fail(16003, "protocol 不能为空", 400);
        const auto data = co_await protocolService().options(request, std::string(protocol->view()), static_cast<std::int64_t>(*query.get<"page">()), static_cast<std::int64_t>(*query.get<"pageSize">()));
        c.header("Content-Type", "application/json");
        const auto payload = service::live::data(c, data);
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<ruvia::http_response> detail(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto data = co_await protocolService().detail(request, id(c));
        c.header("Content-Type", "application/json");
        const auto payload = service::live::data(c, data);
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<ruvia::http_response> testExpression(ruvia::context& c) {
        const auto result = ProtocolService::testExpression(c.req().validated<ExpressionTestBody>());
        co_return c.json(service::common::ok<ExpressionTestResponse>(c, result));
    }

    ruvia::task<ruvia::http_response> previewMqtt(ruvia::context& c) {
        const auto result = ProtocolService::previewMqtt(c.req().validated<MqttPreviewBody>());
        co_return c.json(service::common::ok<MqttPreviewResponse>(c, result));
    }

    ruvia::task<ruvia::http_response> create(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await protocolService().create(request, c.req().validated<CreateProtocolBody>());
        co_return c.json(service::common::operation(c, "创建成功"));
    }

    ruvia::task<ruvia::http_response> update(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await protocolService().update(request, id(c), c.req().validated<UpdateProtocolBody>());
        co_return c.json(service::common::operation(c, "更新成功"));
    }

    ruvia::task<ruvia::http_response> remove(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await protocolService().remove(request, id(c));
        co_return c.json(service::common::operation(c, "删除成功"));
    }
};

} // namespace service::protocol
