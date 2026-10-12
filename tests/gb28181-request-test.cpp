#include <cstdio>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/web/controller.h>
#include <ruvia/web/testing.h>

#include "service/modules/gb28181/gb28181.types.h"

namespace {
using namespace service::gb28181;

class GbRequestRoutes final {
  public:
    RUVIA_CONTROLLER_GROUP("/gb-request")
    RUVIA_ROUTES_BEGIN(GbRequestRoutes)
    RUVIA_POST("/name", name, ruvia::json_body<GbNameBody>);
    RUVIA_POST("/channel-name", channelName, ruvia::json_body<GbNameBody>);
    RUVIA_POST("/device/:deviceId/channel/:channelId/ptz/:action", ptz, ruvia::path_model<GbPtzParams>, ruvia::json_body<GbPtzBody>);
    RUVIA_POST("/position", position, ruvia::json_body<GbPositionBody>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> name(ruvia::context& c) {
        const auto name = service::utils::trim(c.req().validated<GbNameBody>().get<"name">().view());
        co_return c.text(std::string_view(name));
    }
    ruvia::task<ruvia::http_response> channelName(ruvia::context& c) {
        const auto name = service::utils::trim(c.req().validated<GbNameBody>().get<"name">().view());
        co_return c.text(std::string_view(name));
    }
    ruvia::task<ruvia::http_response> ptz(ruvia::context& c) {
        const auto speed = std::to_string(c.req().validated<GbPtzBody>().get<"speed">().value_or(ruvia::int64{80}).value_);
        co_return c.text(std::string_view(speed));
    }
    ruvia::task<ruvia::http_response> position(ruvia::context& c) {
        const auto& input = c.req().validated<GbPositionBody>();
        if (input.get<"pan">().value_ < 0 || input.get<"tilt">().value_ < -30 || input.get<"zoom">().value_ < 1) throw std::runtime_error("invalid position reached handler");
        co_return c.text("accepted");
    }
};

void expect(ruvia::test_app& app, const ruvia::test_request& request, int status, std::string_view body = {}) {
    const auto response = app.request(request);
    if (response.status().value() != status || (!body.empty() && response.body() != body)) {
        throw std::runtime_error("unexpected GB request result: " + std::to_string(response.status().value()) + " " + std::string(response.body()));
    }
}

void testPtzValidation(ruvia::test_app& app) {
    const auto path = "/gb-request/device/device/channel/channel/ptz/left";
    expect(app, ruvia::test_request::post(path).json("{}"), 200, "80");
    for (const auto value : {"0", "255", "80"}) {
        expect(app, ruvia::test_request::post(path).json(std::string("{\"speed\":") + value + "}"), 200, value);
    }
    expect(app, ruvia::test_request::post(path).json(R"({"speed":null})"), 200, "80");
    for (const auto value : {"-1", "256", "12.5", "\"80\"", "true", "{}", "[]"}) {
        expect(app, ruvia::test_request::post(path).json(std::string("{\"speed\":") + value + "}"), 400);
    }
    expect(app, ruvia::test_request::post(path).json(R"({"speed":1,"speed":2})"), 400);
    expect(app, ruvia::test_request::post("/gb-request/device/device/channel/channel/ptz/arbitrary").json("{}"), 400);
    expect(app, ruvia::test_request::post("/gb-request/position").json(R"({"pan":360,"tilt":-30,"zoom":1000})"), 200);
    expect(app, ruvia::test_request::post("/gb-request/position").json(R"({"pan":null,"tilt":0,"zoom":1})"), 400);
    for (const auto body : {R"({"pan":361,"tilt":0,"zoom":1})", R"({"pan":0,"tilt":91,"zoom":1})", R"({"pan":0,"tilt":0,"zoom":0})", R"({"pan":"1","tilt":0,"zoom":1})", R"({"pan":1e999,"tilt":0,"zoom":1})", R"({"pan":0,"tilt":0})"}) {
        expect(app, ruvia::test_request::post("/gb-request/position").json(body), 400);
    }
}

void testIdentityValidation(ruvia::test_app& app) {
    expect(app, ruvia::test_request::post("/gb-request/name").json(R"({"name":"  camera  "})"), 200, "camera");
    expect(app, ruvia::test_request::post("/gb-request/channel-name").json(R"({"name":"  channel  "})"), 200, "channel");
    for (const auto body : {"{}", R"({"name":" \t "})", R"({"name":null})", R"({"name":12})", "[]", "{"}) {
        expect(app, ruvia::test_request::post("/gb-request/name").json(body), 400);
    }
    expect(app, ruvia::test_request::post("/gb-request/name").json("{\"name\":\"" + std::string(256, 'x') + "\"}"), 400);
    expect(app, ruvia::test_request::post("/gb-request/name").body(R"({"name":"camera"})", "text/plain"), 415);
    expect(app, ruvia::test_request::post("/gb-request/name").body(R"({"name":"camera"})", "Application/JSON; charset=utf-8"), 200, "camera");
}
} // namespace

int main() {
    try {
        ruvia::test_app app;
        testPtzValidation(app);
        testIdentityValidation(app);
    } catch (const std::exception& error) {
        std::fprintf(stderr, "gb28181-request-test failed: %s\n", error.what());
        return 1;
    }
    return 0;
}
