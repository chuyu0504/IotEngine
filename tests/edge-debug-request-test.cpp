#include <iostream>

#include <ruvia/web/controller.h>
#include <ruvia/web/testing.h>

#include "service/modules/edge_node/edge_node.controller.h"

namespace {
void require(bool condition, const char* reason) {
    if (!condition) {
        throw std::runtime_error(reason);
    }
}

RUVIA_MODEL(EchoBody, RUVIA_REQUIRED_FIELD(value, ruvia::string, RUVIA_MIN(2, "short")));

class EventProbe final {
  public:
    RUVIA_CONTROLLER_GROUP("/probe")
    RUVIA_ROUTES_BEGIN(EventProbe)
    RUVIA_POST("/", probe);

    RUVIA_ROUTES_END
    void registerEvents(service::edge_node::debug::EventRegistry& registry) {
        registry.add<EventProbe, &EventProbe::echo, EchoBody>("echo.read", *this);
    }

  private:
    ruvia::task<service::edge_node::debug::EventResult> echo(service::edge_node::debug::EventContext& c, const EchoBody& body) {
        require(c.connection.worker().is_current(), "event migrated to another worker");
        require(c.arena() != c.connection.pool(), "debug invocation reused the connection allocation arena");
        require(c.request.event == "echo.read", "event identity changed");
        auto& workerGenerator = *c.connection.worker_state<std::unique_ptr<service::common::UuidV7Generator>>();
        require(c.worker_state<std::unique_ptr<service::common::UuidV7Generator>>().get() == &workerGenerator, "debug event did not borrow its accepting worker's UUID generator");
        const auto firstId = c.worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        service::middleware::RequestContext nextRequest(c.connection, "another-actor");
        require(nextRequest.worker_state<std::unique_ptr<service::common::UuidV7Generator>>().get() == &workerGenerator, "request scope replaced worker UUID state");
        require(firstId < nextRequest.worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), "UUID order was reset between request scopes");
        co_return service::edge_node::debug::EventResult{ service::utils::jsonQuoted(body.get<"value">().view()) };
    }

    ruvia::task<ruvia::http_response> probe(ruvia::context& c) {
        const auto wire = co_await c.req().text();
        auto request = service::edge_node::debug::Request::parse(wire);
        service::edge_node::debug::SessionIdentity identity{
            c.worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next()};
        service::edge_node::debug::ConnectionResources resources;
        std::any state;
        service::edge_node::debug::EventContext context(c, identity, request, c.get_stop_token(), resources, state);
        service::edge_node::debug::EventRegistry registry;
        registerEvents(registry);
        bool duplicate = false;
        try {
            registerEvents(registry);
        } catch (const std::logic_error&) {
            duplicate = true;
        }
        require(duplicate, "duplicate event accepted");
        registry.seal();
        require(registry.policy("echo.read").authenticated, "events must require authentication by default");
        bool sealed = false;
        try {
            registerEvents(registry);
        } catch (const std::logic_error&) {
            sealed = true;
        }
        require(sealed, "registration changed after sealing");
        const auto result = co_await registry.invoke(context);
        co_return c.text(std::string_view(result.data));
    }
};
} // namespace

int main() {
    service::common::UuidV7Generator uuidGenerator;
    try {
        const auto id = uuidGenerator.next();
        const auto wire = "{\"id\":\"" + id + "\",\"event\":\"echo.read\",\"data\":{\"value\":\"typed\"}}";
        const auto parsed = service::edge_node::debug::Request::parse(wire);
        require(parsed.id == id && parsed.data == "{\"value\":\"typed\"}", "request ownership failed");
        const auto lexical = "{\"id\":\"" + id + "\",\"event\":\"echo.read\",\"d\\u0061ta\":{\"value\":[1e3,{\"x\":\"a\\\"},b\"}]}}";
        require(service::edge_node::debug::Request::parse(lexical).data == "{\"value\":[1e3,{\"x\":\"a\\\"},b\"}]}", "raw JSON or escaped field name changed");
        for (const auto extra : { ",\"id\":\"other\"}", ",\"method\":\"GET\"}", ",\"unknown\":null}" }) {
            auto malformed = wire.substr(0, wire.size() - 1) + extra;
            bool rejected = false;
            try {
                (void)service::edge_node::debug::Request::parse(malformed);
            } catch (const service::edge_node::debug::EventError&) {
                rejected = true;
            }
            require(rejected, "duplicate or unknown envelope field accepted");
        }
        const auto duplicates = ruvia::json_value::parse(R"({"x":1,"x":{"nested":["comma,brace}",2]}})");
        require(duplicates && duplicates->get<ruvia::json_value>("x")->view() == R"({"nested":["comma,brace}",2]})", "last duplicate field semantics changed");
        for (const auto bad : { "{}", "[]", "{\"id\":\"1\",\"event\":\"echo.read\",\"data\":{}}" }) {
            bool rejected = false;
            try {
                (void)service::edge_node::debug::Request::parse(bad);
            } catch (const service::edge_node::debug::EventError&) {
                rejected = true;
            }
            require(rejected, "invalid request accepted");
        }
        ruvia::test_app app;
        app.use_worker_state<std::unique_ptr<service::common::UuidV7Generator>>(
            [] { return std::make_unique<service::common::UuidV7Generator>(); });
        const auto good = app.request(ruvia::test_request::post("/probe").json(wire));
        require(good.status().value() == 200 && good.body() == "\"typed\"", "typed event invocation failed");
        const auto invalid = "{\"id\":\"" + id + "\",\"event\":\"echo.read\",\"data\":{\"value\":\"x\"}}";
        require(app.request(ruvia::test_request::post("/probe").json(invalid)).status().value() == 400, "validator did not run");
        std::cout << "Debug request parsing, typed dispatch, validation and worker ownership passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
