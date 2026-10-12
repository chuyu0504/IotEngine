#include <iostream>
#include <stdexcept>
#include <string>
#include <type_traits>

#include <ruvia/web/testing.h>
#include <ruvia/web/controller.h>

#include "service/modules/device/device.types.h"
#include "service/modules/edge_node/edge_node.types.h"
#include "service/modules/gb28181/gb28181.types.h"
#include "service/modules/system/user/user.types.h"
#include "service/utils/json.h"
#include "service/middleware/request_context.h"

namespace {
static_assert(std::is_same_v<ruvia::task<ruvia::http_response>::value_type, ruvia::http_response>);
RUVIA_MODEL(CreatedResponse,
    RUVIA_REQUIRED_FIELD(created, ruvia::bool_value),
    RUVIA_REQUIRED_FIELD(name, ruvia::string));

class ValidationRoutes final {
  public:
    RUVIA_CONTROLLER_GROUP("/validation")
    RUVIA_ROUTES_BEGIN(ValidationRoutes)
    RUVIA_POST("/typed-response", typedResponse);
    RUVIA_POST("/devices", createDevice, ruvia::json_body<service::device::CreateDeviceBody>);
    RUVIA_PUT("/devices", updateDevice, ruvia::json_body<service::device::UpdateDeviceBody>);
    RUVIA_POST("/debug", debug, ruvia::json_body<service::device::DeviceDebugBody>);
    RUVIA_POST("/network", network, ruvia::json_body<service::edge::NetworkBody>);
    RUVIA_POST("/user", createUser, ruvia::json_body<service::user::CreateUserBody>);
    RUVIA_GET("/users", query, ruvia::query_model<service::user::UserListQuery>);
    RUVIA_GET("/device/:deviceId", device, ruvia::path_model<service::gb28181::GbDeviceParams>);
    RUVIA_GET("/device/:deviceId/channel/:channelId", channel, ruvia::path_model<service::gb28181::GbChannelParams>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> typedResponse(ruvia::context& c) {
        service::middleware::RequestContext request(c, "test-user");
        if (request.arena() == c.arena()) throw std::runtime_error("业务操作未隔离临时内存");
        service::middleware::RequestContext snapshot(c, "test-user");
        if (snapshot.arena() == c.arena()) throw std::runtime_error("事件快照未隔离临时内存");
        CreatedResponse response(ruvia::model_options{ .resource_ = request.arena() });
        response.set<"created">(true).set<"name">(std::string(8192, 'x') + "设备\"一");
        c.status(ruvia::http_status::created);
        co_return c.json(response);
    }

    ruvia::task<ruvia::http_response> createDevice(ruvia::context& c) {

        co_return c.text("accepted");
    }
    ruvia::task<ruvia::http_response> updateDevice(ruvia::context& c) {

        co_return c.text("accepted");
    }
    ruvia::task<ruvia::http_response> network(ruvia::context& c) {

        co_return c.text("accepted");
    }
    ruvia::task<ruvia::http_response> createUser(ruvia::context& c) {

        co_return c.text("accepted");
    }
    ruvia::task<ruvia::http_response> device(ruvia::context& c) {

        co_return c.text("accepted");
    }
    ruvia::task<ruvia::http_response> channel(ruvia::context& c) {

        co_return c.text("accepted");
    }
    ruvia::task<ruvia::http_response> debug(ruvia::context& c) {

        co_return c.text(std::string_view(c.req().validated<service::device::DeviceDebugBody>().get<"enabled">() ? "on" : "off"));
    }
    ruvia::task<ruvia::http_response> query(ruvia::context& c) {
        const auto& body = c.req().validated<service::user::UserListQuery>();
        const auto page = std::to_string(body.get<"page">()->value_);
        co_return c.text(std::string_view(page));
    }
};

void expect(ruvia::test_app& app, const ruvia::test_request& request, int status,
            std::string_view message = {}) {
    const auto response = app.request(request);
    if (response.status().value() != status ||
        (!message.empty() && response.body().find(message) == std::string_view::npos)) {
        throw std::runtime_error("请求校验结果不符: " + std::to_string(response.status().value()) +
                                 " " + std::string(response.body()));
    }
}
} // namespace

int main() {
    try {
        ruvia::test_app app;
        const auto typed = app.request(ruvia::test_request::post("/validation/typed-response"));
        if (typed.status().value() != 201 ||
            !typed.header("Content-Type").value_or("").starts_with("application/json") ||
            typed.body() != std::string(R"({"created":true,"name":")") + std::string(8192, 'x') + R"(设备\"一"})") {
            throw std::runtime_error("类型化响应未保持状态码或 JSON 序列化");
        }
        expect(app, ruvia::test_request::post("/validation/devices").json("{}"), 400, "name");
        expect(app, ruvia::test_request::put("/validation/devices").json("{}"), 200);
        expect(app, ruvia::test_request::put("/validation/devices").json(R"({"name":""})"), 400);
        expect(app, ruvia::test_request::put("/validation/devices").json(R"({"protocol_config_id":""})"), 200);
        expect(app, ruvia::test_request::post("/validation/devices").json(R"({"name":"device","device_code":"device","protocol_config_id":""})"), 400);
        expect(app, ruvia::test_request::post("/validation/devices").json(R"({"name":"device","device_code":"device","protocol_config_id":"01994172-9ad0-7000-8000-000000000001"})"), 200);
        expect(app, ruvia::test_request::post("/validation/debug").json(R"({"enabled":false})"), 200, "off");
        for (const auto body : {"{}", R"({"enabled":null})", R"({"enabled":"false"})", R"({"enabled":true,"enabled":false})", "{"}) {
            expect(app, ruvia::test_request::post("/validation/debug").json(body), 400);
        }
        expect(app, ruvia::test_request::post("/validation/debug").body(R"({"enabled":false})", "text/plain"), 415);
        expect(app, ruvia::test_request::post("/validation/debug").body(R"({"enabled":false})", "Application/JSON; charset=utf-8"), 200);
        expect(app, ruvia::test_request::post("/validation/network").json(R"({"interfaces":[{"operation":"upsert","name":"lan"}]})"), 200);
        expect(app, ruvia::test_request::post("/validation/network").json(R"({"interfaces":[]})"), 400);
        expect(app, ruvia::test_request::post("/validation/network").json(R"({"interfaces":[{"operation":"upsert","name":"bad-name"}]})"), 400, "interfaces[0].name");
        expect(app, ruvia::test_request::post("/validation/network").json(R"({"interfaces":[{"operation":"upsert","name":"lan","prefixLength":31}]})"), 400, "interfaces[0].prefixLength");
        expect(app, ruvia::test_request::post("/validation/network").json(R"({"interfaces":[null]})"), 400);
        expect(app, ruvia::test_request::post("/validation/user").json(R"({"username":"tester","password":"123456","role_ids":["role"],"email":"invalid"})"), 400, "email");
        expect(app, ruvia::test_request::get("/validation/users"), 200, "1");
        expect(app, ruvia::test_request::get("/validation/users?page=2&pageSize=100"), 200, "2");
        expect(app, ruvia::test_request::get("/validation/users?pageSize=101"), 400);
        expect(app, ruvia::test_request::get("/validation/users?page=invalid"), 400);
        expect(app, ruvia::test_request::get("/validation/device/camera"), 200);
        expect(app, ruvia::test_request::get("/validation/device/camera/channel/channel1"), 200);
        expect(app, ruvia::test_request::get("/validation/device/%20"), 400);
        std::cout << "HTTP 请求绑定、字段约束、操作差异及嵌套校验通过\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
