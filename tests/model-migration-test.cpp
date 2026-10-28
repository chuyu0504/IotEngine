#include <algorithm>
#include <array>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/web/controller.h>
#include <ruvia/web/testing.h>

#include "service/middleware/validation.h"
#include "service/modules/alert/alert.types.h"
#include "service/modules/link/link.types.h"
#include "service/modules/open_access/open_access.types.h"
#include "service/modules/protocol/protocol.types.h"
#include "service/modules/vpn/vpn.types.h"

namespace {
RUVIA_MODEL(PresenceBody, RUVIA_REQUIRED_FIELD(requiredText, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value, RUVIA_DEFAULT(true)), RUVIA_OPTIONAL_FIELD(nullableDefault, ruvia::int64, RUVIA_NULLABLE, RUVIA_DEFAULT(7)));

class ModelMigrationRoutes final {
  public:
    RUVIA_CONTROLLER_GROUP("/model-migration")
    RUVIA_ROUTES_BEGIN(ModelMigrationRoutes)
    RUVIA_POST("/presence", presence, ruvia::json_body<PresenceBody>);
    RUVIA_PUT("/errors/:id", accept, ruvia::path_model<service::protocol::ProtocolIdParams>, service::middleware::ValidationErrorCodeMiddleware<16002, "enabled", 16004>, ruvia::json_body<service::protocol::UpdateProtocolBody>);
    RUVIA_POST("/protocol", createProtocol, ruvia::json_body<service::protocol::CreateProtocolBody>);
    RUVIA_PUT("/protocol", updateProtocol, ruvia::json_body<service::protocol::UpdateProtocolBody>);
    RUVIA_POST("/rule", rule, ruvia::json_body<service::alert::RuleInput>);
    RUVIA_POST("/template", accept, ruvia::json_body<service::alert::TemplateInput>);
    RUVIA_POST("/batch", accept, ruvia::json_body<service::alert::AlertBatchBody>);
    RUVIA_POST("/key", accept, ruvia::json_body<service::open_access::CreateAccessKeyBody>);
    RUVIA_PUT("/key", updateKey, ruvia::json_body<service::open_access::UpdateAccessKeyBody>);
    RUVIA_POST("/webhook", webhook, ruvia::json_body<service::open_access::CreateWebhookBody>);
    RUVIA_PUT("/webhook", updateWebhook, ruvia::json_body<service::open_access::UpdateWebhookBody>);
    RUVIA_POST("/vpn", vpn, ruvia::json_body<service::vpn::VpnDesktopSelectionBody>);
    RUVIA_POST("/link", accept, service::middleware::ValidationErrorCodeMiddleware<10001, "name", 15002, "too_small">, ruvia::json_body<service::link::SaveLinkBody>);
    RUVIA_ROUTES_END
  private:
    ruvia::task<ruvia::http_response> accept(ruvia::context& c) { co_return c.text("accepted"); }

    ruvia::task<ruvia::http_response> presence(ruvia::context& c) {
        const auto& body = c.req().validated<PresenceBody>();
        if (!body.is_present<"requiredText">()) {
            throw std::runtime_error("required presence lost");
        }
        if (!body.is_present<"enabled">() && (!body.get<"enabled">() || !body.get<"enabled">()->value_)) {
            throw std::runtime_error("missing default lost");
        }
        if (body.is_null<"nullableDefault">()) {
            if (!body.is_present<"nullableDefault">() || body.get<"nullableDefault">()) {
                throw std::runtime_error("null replaced by default");
            }
            co_return c.text("null");
        }
        if (body.is_present<"nullableDefault">()) {
            co_return c.text("present");
        }
        if (!body.get<"nullableDefault">() || body.get<"nullableDefault">()->value_ != 7) {
            throw std::runtime_error("missing nullable default lost");
        }
        co_return c.text("default");
    }

    ruvia::task<ruvia::http_response> createProtocol(ruvia::context& c) {
        const auto& enabled = c.req().validated<service::protocol::CreateProtocolBody>().get<"enabled">();
        co_return c.text(std::string_view(enabled ? (enabled->value_ ? "true" : "false") : "null"));
    }

    ruvia::task<ruvia::http_response> updateProtocol(ruvia::context& c) {
        const auto& body = c.req().validated<service::protocol::UpdateProtocolBody>();
        const auto& config = body.get<"config">();
        if (config) {
            co_return c.text(config->view());
        }
        if (!body.is_present<"remark">()) {
            co_return c.text("unchanged");
        }
        if (body.is_null<"remark">()) {
            co_return c.text("cleared");
        }
        const auto& remark = body.get<"remark">();
        co_return c.text(remark ? remark->view() : "cleared");
    }

    ruvia::task<ruvia::http_response> rule(ruvia::context& c) {
        const auto& body = c.req().validated<service::alert::RuleInput>();
        if (body.get<"silenceDuration">()->value_ != 300 || body.get<"recoveryWaitSeconds">()->value_ != 60 || body.is_present<"silenceDuration">()) {
            throw std::runtime_error("alert defaults or input presence changed");
        }
        service::alert::alertRequest::validateConditions(body.get<"conditions">().view());
        co_return c.text(body.get<"severity">()->view());
    }

    ruvia::task<ruvia::http_response> updateKey(ruvia::context& c) {
        const auto& body = c.req().validated<service::open_access::UpdateAccessKeyBody>();
        if (!body.is_present<"remark">()) {
            co_return c.text("unchanged");
        }
        if (body.is_null<"remark">()) {
            co_return c.text("cleared");
        }
        const auto& remark = body.get<"remark">();
        co_return c.text(remark ? remark->view() : "cleared");
    }

    ruvia::task<ruvia::http_response> webhook(ruvia::context& c) {
        const auto& body = c.req().validated<service::open_access::CreateWebhookBody>();
        if (body.get<"timeoutSeconds">()->value_ != 5 || body.get<"skipTlsVerify">()->value_) {
            throw std::runtime_error("webhook defaults changed");
        }
        const auto& headers = body.get<"headers">();
        const bool validHeaders = !headers || service::open_access::isWebhookHeaders(*headers);
        if (!validHeaders) {
            service::common::fail(19002, "非法 Header", 400);
        }
        co_return c.text("accepted");
    }

    ruvia::task<ruvia::http_response> updateWebhook(ruvia::context& c) {
        const auto& body = c.req().validated<service::open_access::UpdateWebhookBody>();
        const auto& headers = body.get<"headers">();
        const bool validHeaders = !headers || service::open_access::isWebhookHeaders(*headers);
        if (!validHeaders) {
            service::common::fail(19002, "非法 Header", 400);
        }
        co_return c.text("accepted");
    }

    ruvia::task<ruvia::http_response> vpn(ruvia::context& c) {
        const auto ids = service::vpn::normalizeVpnNodeIds(c.req().validated<service::vpn::VpnDesktopSelectionBody>().get<"edgeNodeIds">());
        co_return c.text(ids.empty() ? std::string_view("empty") : std::string_view(ids.front()));
    }
};

void expect(ruvia::test_app& app, const ruvia::test_request& request, int status, std::string_view text = {}) {
    const auto result = app.request(request);
    if (result.status().value() != status || (!text.empty() && result.body() != text)) {
        throw std::runtime_error("model request: " + std::to_string(result.status().value()) + " " + std::string(result.body()));
    }
}

void testValidationCodes(ruvia::test_app& app) {
    const auto path = "/model-migration/errors/00000000-0000-7000-8000-000000000001";
    expect(app, ruvia::test_request::put(path).json(R"({"name":42})"), 400, "16002");
    expect(app, ruvia::test_request::put(path).json(R"({"enabled":"false"})"), 400, "16004");
    expect(app, ruvia::test_request::put("/model-migration/errors/invalid").json(R"({"name":42})"), 400, "10001");
    expect(app, ruvia::test_request::put(path).json("{}"), 200, "accepted");
    expect(app, ruvia::test_request::post("/model-migration/presence").json("{}"), 400, "10001");
}

void testPresence(ruvia::test_app& app) {
    expect(app, ruvia::test_request::post("/model-migration/presence").json("{}"), 400);
    expect(app, ruvia::test_request::post("/model-migration/presence").json(R"({"requiredText":null})"), 200, "default");
    expect(app, ruvia::test_request::post("/model-migration/presence").json(R"({"requiredText":"ok","nullableDefault":null})"), 200, "null");
    expect(app, ruvia::test_request::post("/model-migration/presence").json(R"({"requiredText":"ok","nullableDefault":3})"), 200, "present");
    expect(app, ruvia::test_request::post("/model-migration/presence").json(R"({"requiredText":"ok","enabled":null})"), 400);
    expect(app, ruvia::test_request::post("/model-migration/presence").json(R"({"requiredText":42})"), 400);
}

void testProtocol(ruvia::test_app& app) {
    expect(app, ruvia::test_request::post("/model-migration/protocol").json(R"({"protocol":"Modbus","name":"pump"})"), 200, "true");
    expect(app, ruvia::test_request::post("/model-migration/protocol").json(R"({"protocol":"Modbus","name":"pump","enabled":false})"), 200, "false");
    expect(app, ruvia::test_request::post("/model-migration/protocol").json(R"({"protocol":"Modbus","name":"pump","enabled":null})"), 200, "null");
    for (const auto invalid : { R"({"protocol":null,"name":"x"})", R"({"protocol":"Modbus","name":null})", R"({"protocol":"Modbus","name":"x","enabled":"false"})", R"({"protocol":"Modbus","name":"x","remark":3})" }) {
        expect(app, ruvia::test_request::post("/model-migration/protocol").json(invalid), 400);
    }
    expect(app, ruvia::test_request::put("/model-migration/protocol").json("{}"), 200, "unchanged");
    expect(app, ruvia::test_request::put("/model-migration/protocol").json(R"({"remark":null})"), 200, "cleared");
    expect(app, ruvia::test_request::put("/model-migration/protocol").json(R"({"remark":"note"})"), 200, "note");
    expect(app, ruvia::test_request::put("/model-migration/protocol").json(R"({"config":{ "vendor": [null, true, {"x":1}] }})"), 200, R"({ "vendor": [null, true, {"x":1}] })");
    for (const auto invalid : { R"({"remark":false})", R"({"name":4})", R"({"protocol":[]})", R"({"enabled":"false"})", R"({"remark":"one","remark":"two"})", "[]" }) {
        expect(app, ruvia::test_request::put("/model-migration/protocol").json(invalid), 400);
    }
}

void testAlert(ruvia::test_app& app) {
    const std::string fields = R"("name":"alarm","device_id":"00000000-0000-7000-8000-000000000001","conditions":[{"type":"offline"}])";
    expect(app, ruvia::test_request::post("/model-migration/rule").json("{" + fields + "}"), 200, "warning");
    for (const auto invalid : { R"("remark":null)", R"("remark":42)", R"("silence_duration":null)", R"("silence_duration":"300")", R"("silence_duration":-1)", R"("status":"invalid")" }) {
        expect(app, ruvia::test_request::post("/model-migration/rule").json("{" + fields + "," + invalid + "}"), 400);
    }
    expect(app, ruvia::test_request::post("/model-migration/template").json(R"({"name":"alarm","conditions":[{"type":"offline"}]})"), 200);
    for (const auto invalid : { "{}", R"({"ids":[]})", R"({"ids":["bad"]})", R"({"ids":[1]})" }) {
        expect(app, ruvia::test_request::post("/model-migration/batch").json(invalid), 400);
    }
}

void testAccess(ruvia::test_app& app) {
    const std::string key = R"({"name":"integration","scopes":["device:realtime"],"deviceIds":["00000000-0000-7000-8000-000000000001"]})";
    expect(app, ruvia::test_request::post("/model-migration/key").json(key), 200);
    expect(app, ruvia::test_request::post("/model-migration/key").json("{}"), 400);
    expect(app, ruvia::test_request::put("/model-migration/key").json("{}"), 200, "unchanged");
    expect(app, ruvia::test_request::put("/model-migration/key").json(R"({"remark":null})"), 200, "cleared");
    for (const auto invalid : { R"({"name":" "})", R"({"scopes":["admin"]})", R"({"deviceIds":["bad"]})", R"({"remark":false})" }) {
        expect(app, ruvia::test_request::put("/model-migration/key").json(invalid), 400);
    }
    const std::string fields = R"("accessKeyId":"00000000-0000-7000-8000-000000000001","name":"sink","url":"https://example.test/hook")";
    const std::string webhookWithoutHeaders = "{" + fields + "}";
    const auto noHeadersResponse = app.request(ruvia::test_request::post("/model-migration/webhook").json(webhookWithoutHeaders));
    const int noHeadersStatus = noHeadersResponse.status().value();
    if (noHeadersStatus != 200) {
        throw std::runtime_error("webhook without headers: expected status=200; actual status=" + std::to_string(noHeadersStatus) + " body=" + std::string(noHeadersResponse.body()));
    }
    const std::string webhookWithHeaders = "{" + fields + R"(,"headers":{"X-Example":"ok"}})";
    expect(app, ruvia::test_request::post("/model-migration/webhook").json(webhookWithHeaders), 200, "accepted");
    expect(app, ruvia::test_request::put("/model-migration/webhook").json(R"({"headers":{"X-Example":"ok"}})"), 200, "accepted");
    for (const auto headers : {
             R"({})",
             R"({"X-One":"1","X-Two":"2"})",
             R"({"X-!#$%&'*+.^_`|~":"ok"})",
             R"({"X-Long":"a value longer than the small string buffer"})",
             R"({"X-Empty":""})",
             R"({"X-\u0045xample":"line\tvalue"})",
             R"({"X-Test":"ok","X-Test":"again"})" }) {
        expect(app, ruvia::test_request::post("/model-migration/webhook").json("{" + fields + ",\"headers\":" + headers + "}"), 200, "accepted");
        expect(app, ruvia::test_request::put("/model-migration/webhook").json("{\"headers\":" + std::string(headers) + "}"), 200, "accepted");
    }
    for (const auto name : {
             "host",
             "content-length",
             "connection",
             "x-iot-event",
             "x-iot-timestamp",
             "x-iot-delivery",
             "x-iot-signature",
             "content-type",
             "user-agent",
             "transfer-encoding",
             "trailer",
             "te",
             "upgrade",
             "expect",
             "proxy-connection" }) {
        std::string headerName(name);
        for (int casing = 0; casing < 2; ++casing) {
            const std::string headers = "{\"" + headerName + "\":\"custom\"}";
            expect(app, ruvia::test_request::post("/model-migration/webhook").json("{" + fields + ",\"headers\":" + headers + "}"), 400);
            expect(app, ruvia::test_request::put("/model-migration/webhook").json("{\"headers\":" + headers + "}"), 400);
            for (auto& ch : headerName) {
                if (ch >= 'a' && ch <= 'z') {
                    ch = static_cast<char>(ch - ('a' - 'A'));
                }
            }
        }
    }
    for (const auto invalidHeaders : {
             R"({"Host":"attacker"})",
             R"({"X-IoT-Event":"custom"})",
             R"({"Ho\u0073t":"attacker"})",
             R"({"X-Test":"x\r\ny"})",
             R"({"X-Test":3})",
             R"({"bad name":"x"})",
             R"({"":"x"})",
             R"({"X-Test:":"x"})",
             R"({"X-\u0009Test":"x"})",
             R"({"X-\u00e9":"x"})",
             R"({"X-Test":null})",
             R"({"X-Test":{}})",
             R"({"X-Test":3,"X-Test":"ok"})",
             R"({"X-Test":"ok","X-Test":3})" }) {
        expect(app, ruvia::test_request::post("/model-migration/webhook").json("{" + fields + ",\"headers\":" + invalidHeaders + "}"), 400);
        expect(app, ruvia::test_request::put("/model-migration/webhook").json("{\"headers\":" + std::string(invalidHeaders) + "}"), 400);
    }
    for (const auto invalid : {
             R"({"url":"file:///tmp/x"})",
             R"({"url":"https://user@example.test"})",
             R"({"timeoutSeconds":31})",
             R"({"skipTlsVerify":"false"})",
             R"({"eventTypes":[]})",
             R"({"eventTypes":["bogus"]})" }) {
        expect(app, ruvia::test_request::put("/model-migration/webhook").json(invalid), 400);
    }
}

void testLink(ruvia::test_app& app) {
    expect(app, ruvia::test_request::post("/model-migration/link").json("{}"), 400, "10001");
    expect(app, ruvia::test_request::post("/model-migration/link").json(R"({"name":"","protocol":"Modbus","endpoint":{}})"), 400, "15002");
    expect(app, ruvia::test_request::post("/model-migration/link").json(R"({"name":"valid","protocol":"invalid","endpoint":{}})"), 400, "10001");
}

void testVpn(ruvia::test_app& app) {
    expect(app, ruvia::test_request::post("/model-migration/vpn").json(R"({"edgeNodeIds":[]})"), 200, "empty");
    expect(app, ruvia::test_request::post("/model-migration/vpn").json(R"({"edgeNodeIds":["AAAAAAAA-AAAA-7AAA-8AAA-AAAAAAAAAAAA"]})"), 200, "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa");
    for (const auto invalid : { "{}", R"({"edgeNodeIds":null})", R"({"edgeNodeIds":["bad"]})", R"({"edgeNodeIds":["AAAAAAAA-AAAA-7AAA-8AAA-AAAAAAAAAAAA","aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"]})" }) {
        expect(app, ruvia::test_request::post("/model-migration/vpn").json(invalid), 400);
    }
}
} // namespace

int main() {
    try {
        ruvia::test_app app;
        app.use<service::middleware::ValidationErrorCodeMiddleware<service::common::kValidationErrorCode>>();
        app.on_error([](ruvia::context& context, ruvia::http_error_info error) -> ruvia::task<ruvia::http_response> {
            context.status(error.status());
            std::pmr::string code(context.arena());
            code = std::to_string(service::middleware::requestErrorCode(context, error));
            co_return context.text(std::move(code));
        });
        testValidationCodes(app);
        testPresence(app);
        testProtocol(app);
        testAlert(app);
        testAccess(app);
        testVpn(app);
        testLink(app);
        std::cout << "model migration tests passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
