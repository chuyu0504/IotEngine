#pragma once

#include <string>
#include <string_view>
#include <memory_resource>

#include <ruvia/http/http_status.h>
#include <ruvia/web/controller.h>

#include "service/modules/system/operations/operations.service.h"

namespace service::system {

class OperationsController final {
  public:
    RUVIA_CONTROLLER_GROUP("/internal")
    RUVIA_ROUTES_BEGIN(OperationsController)
    RUVIA_GET("/health/live", live);
    RUVIA_GET("/health/ready", ready);
    RUVIA_GET("/metrics", metrics);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> live(ruvia::context& context) {
        std::pmr::string body("{\"status\":\"alive\"}", context.arena());
        auto response = context.body(std::move(body));
        response.header("Content-Type", "application/json; charset=UTF-8");
        co_return response;
    }

    ruvia::task<ruvia::http_response> ready(ruvia::context& context) {
        const auto result = co_await OperationsService::readiness(context);
        if (!result.ready)
            context.status(ruvia::http_status::service_unavailable);
        std::pmr::string body(result.json, context.arena());
        auto response = context.body(std::move(body));
        response.header("Content-Type", "application/json; charset=UTF-8");
        co_return response;
    }

    ruvia::task<ruvia::http_response> metrics(ruvia::context& context) {
        const auto text = co_await OperationsService::metrics(context);
        std::pmr::string body(text, context.arena());
        auto response = context.body(std::move(body));
        response.header("Content-Type", "text/plain; version=0.0.4; charset=UTF-8");
        co_return response;
    }
};

} // namespace service::system
