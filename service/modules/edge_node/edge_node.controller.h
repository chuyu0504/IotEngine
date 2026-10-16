#pragma once

#include <any>
#include <chrono>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ruvia/core/task_scope.h>
#include <ruvia/web/controller.h>
#include <ruvia/web/validation.h>
#include <ruvia/web/db/db_handle.h>
#include <ruvia/web/redis/redis_handle.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/log.h"
#include "service/middleware/permission.h"
#include "service/middleware/request_context.h"
#include "service/modules/edge_node/edge_node.types.h"
#include "service/modules/edge_node/edge_node.service.h"

namespace service::edge_node::debug {

class EventError final : public std::runtime_error {
  public:
    EventError(std::int64_t code, std::string message)
        : std::runtime_error(std::move(message)), code(code) {}

    const std::int64_t code;
};

struct Request final {
    std::string id;
    std::string event;
    std::string data;

    static Request parse(std::string_view wire) {
        if (wire.size() > 64U * 1024U) {
            throw EventError(10002, "消息过大");
        }
        const auto value = ruvia::json_value::parse(wire);
        if (!value || !value->is_object()) {
            throw EventError(10002, "消息格式无效");
        }
        unsigned fields = 0;
        const auto validFields = value->for_each_field([&](std::string_view key, const ruvia::json_value&) {
            const unsigned bit = key == "id" ? 1U : key == "event" ? 2U
                : key == "data"                                    ? 4U
                                                                   : 0U;
            if (!bit || (fields & bit)) {
                return false;
            }
            fields |= bit;
            return true;
        });
        const auto id = value->get<ruvia::string>("id");
        const auto event = value->get<ruvia::string>("event");
        const auto data = value->get<ruvia::json_value>("data");
        if (!id || !service::common::isUuid(id->view()) || !event || event->empty() ||
            event->view().size() > 128 || !data || !validFields || fields != 7U) {
            throw EventError(10002, "事件编号、名称或参数无效");
        }
        for (const auto c : event->view()) {
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_')) {
                throw EventError(10002, "事件名称无效");
            }
        }
        return { std::string(id->view()), std::string(event->view()), std::string(data->view()) };
    }
};

struct SessionIdentity final {
    const std::string connectionId;
    std::string accessToken;
    std::uint64_t revision{};
};

class EventContext;

// Resource implementations retain only state belonging to this connection.
// Their close operation uses the accepting Worker's business context.
class ConnectionResource {
  public:
    ConnectionResource(std::string owner, std::string permission)
        : owner(std::move(owner)), permission(std::move(permission)) {}

    virtual ~ConnectionResource() = default;
    virtual ruvia::task<void> close(EventContext& context) = 0;
    const std::string owner;
    const std::string permission;
    ruvia::stop_source lifetime;
};

using ConnectionResources = std::unordered_map<std::string, std::shared_ptr<ConnectionResource>>;

// Each invocation owns its allocation arena. Connection resources are borrowed
// only on the accepting Worker; event arguments never mutate its HTTP request.
class EventContext final : public service::middleware::RequestContext {
  public:
    EventContext(ruvia::context& connection, SessionIdentity& identity, const Request& request, ruvia::stop_token cancellation, ConnectionResources& resources, std::any& state)
        : RequestContext(connection, {}, std::move(cancellation)), connection(connection), identity(identity), request(request), resources(resources), state(state) {}

    ruvia::context& connection;
    SessionIdentity& identity;
    const Request& request;
    ConnectionResources& resources;
    std::any& state;
};

struct EventResult final {
    std::string data{ "null" };
    std::string topic;
    bool identityChanged{};
    ruvia::stop_token lifetime;
    bool more{};
    bool incremental{};
};

struct EventPolicy final {
    bool authenticated{ true };
    std::string_view permission;
};

class EventRegistry final {
  public:
    template <typename Controller, auto Handler, typename Body>
    void add(std::string_view name, Controller& controller, EventPolicy policy = {}) {
        if (sealed_) {
            throw std::logic_error("event registration is sealed");
        }
        if (name.empty()) {
            throw std::invalid_argument("empty event name");
        }
        if (!policy.authenticated && !policy.permission.empty()) {
            throw std::logic_error("anonymous event cannot require a permission");
        }
        const auto [entry, inserted] = handlers_.emplace(std::string(name), Entry{ &controller, &invokeHandler<Controller, Handler, Body>, policy });
        if (!inserted) {
            throw std::logic_error("duplicate event: " + std::string(name));
        }
    }

    void seal() noexcept { sealed_ = true; }

    EventPolicy policy(std::string_view name) const {
        const auto found = handlers_.find(std::string(name));
        if (found == handlers_.end()) {
            throw EventError(10003, "事件不存在");
        }
        return found->second.policy;
    }

    [[nodiscard]] ruvia::task<EventResult> invoke(EventContext& context) const {
        if (!sealed_) {
            throw std::logic_error("event registration is not sealed");
        }
        const auto found = handlers_.find(context.request.event);
        if (found == handlers_.end()) {
            throw EventError(10003, "事件不存在");
        }
        return found->second.invoke(found->second.controller, context);
    }

  private:
    template <typename Controller, auto Handler, typename Body>
    static ruvia::task<EventResult> invokeHandler(void* instance, EventContext& context) {
        if (!context.connection.worker().is_current()) {
            throw std::logic_error("event invoked outside its accepting worker");
        }
        if (context.stop.stop_requested()) {
            throw EventError(10002, "操作已取消");
        }
        auto body = ruvia::from_json<Body>(context.request.data, { .resource_ = context.arena() });
        if (!body) {
            throw EventError(10001, "事件参数格式无效");
        }
        ruvia::validator validation({ .resource_ = context.arena() });
        ruvia::json_body<Body> validator;
        validator.validate(*body, validation);
        validation.throw_if_invalid();
        co_return co_await (static_cast<Controller*>(instance)->*Handler)(context, *body);
    }

    struct Entry {
        void* controller;
        ruvia::task<EventResult> (*invoke)(void*, EventContext&);
        EventPolicy policy;
    };

    std::unordered_map<std::string, Entry> handlers_;
    bool sealed_{};
};

} // namespace service::edge_node::debug

namespace service::edge_node {

class DebugConnection final {
  public:
    DebugConnection(ruvia::context& context, service::edge_node::debug::EventRegistry& registry)
        : context_(context), socket_(context.get_websocket()), tasks_(context.worker(), { .resource_ = context.pool() }),
          registry_(registry),
          identity_{ context.worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next() } {}

    ruvia::task<void> run() {
        tasks_.spawn(watchResourceAuthorization());
        bool malformed = false;
        try {
            while (auto frame = co_await socket_.read()) {
                if (!frame->text()) {
                    malformed = true;
                    break;
                }
                co_await accept(service::edge_node::debug::Request::parse(frame->payload()));
            }
        } catch (...) {
            malformed = true;
        }
        closing_ = true;
        for (auto& [id, operation] : operations_) {
            operation->stop.request_stop();
        }
        tasks_.request_stop();
        try {
            co_await socket_.close({ .code_ = static_cast<std::uint16_t>(malformed ? 1002 : 1000), .reason_ = "debug session ended" });
        } catch (...) {
        }
        try {
            co_await tasks_.join();
        } catch (...) {
        }
        operations_.clear();
        co_await closeResources({});
        identity_.accessToken.clear();
    }

  private:
    struct Operation {
        explicit Operation(const ruvia::worker_handle& worker)
            : completion(ruvia::make_channel<int>(worker, { .capacity_ = 1 })) {}

        service::edge_node::debug::Request request;
        ruvia::stop_source stop;
        std::any state;
        std::pair<ruvia::channel_sender<int>, ruvia::channel_receiver<int>> completion;
    };

    ruvia::task<void> reply(std::string_view id, std::string_view data, bool error = false) {
        if (closing_) {
            co_return;
        }
        const auto wire = "{\"id\":" + service::utils::jsonQuoted(id) +
            (error ? ",\"error\":" : ",\"data\":") + std::string(data) + "}";
        co_await socket_.text(wire);
    }

    ruvia::task<void> reject(std::string_view id, std::int64_t code, std::string_view message) {
        const auto data = "{\"code\":" + std::to_string(code) + ",\"message\":" + service::utils::jsonQuoted(message) + "}";
        co_await reply(id, data, true);
    }

    ruvia::task<void> accept(service::edge_node::debug::Request request) {
        if (seen_.size() >= 100000) {
            co_await reject(request.id, 10002, "当前会话请求数量已达上限，请重新连接");
            co_return;
        }
        if (!seen_.insert(request.id).second) {
            co_await reject(request.id, 10002, "每次请求必须使用新的 UUID");
            co_return;
        }
        if (request.event == "subscription.cancel") {
            const auto data = ruvia::json_value::parse(request.data);
            const auto target = data ? data->get<ruvia::string>("subscriptionId") : std::nullopt;
            if (!target || !service::common::isUuid(target->view())) {
                co_await reject(request.id, 10001, "订阅编号无效");
                co_return;
            }
            const auto found = operations_.find(std::string(target->view()));
            if (found != operations_.end()) {
                const auto operation = found->second;
                if (!operation->request.event.ends_with(".subscribe")) {
                    co_await reject(request.id, 10001, "只能取消订阅");
                    co_return;
                }
                operation->stop.request_stop();
                // Acknowledge only after the subscription has released its
                // notification receiver and finished any in-flight response.
                (void)co_await operation->completion.second.receive(tasks_.get_stop_token());
            }
            co_await reply(request.id, "null");
            co_return;
        }
        if (operations_.size() >= 128) {
            co_await reject(request.id, 10002, "并发请求过多");
            co_return;
        }
        auto operation = std::make_shared<Operation>(context_.worker());
        operation->request = std::move(request);
        operations_.emplace(operation->request.id, operation);
        tasks_.spawn(execute(std::move(operation)));
    }

    ruvia::task<void> execute(std::shared_ptr<Operation> operation) {
        using namespace std::chrono_literals;
        const auto operationStop = ruvia::combine_stop_tokens(tasks_.get_stop_token(), operation->stop.token());
        auto stop = operationStop;
        const auto& request = operation->request;
        const bool subscription = request.event.ends_with(".subscribe");
        auto notification = subscription ? service::live::bus().subscribe(context_.worker(), "*") : nullptr;
        std::int64_t code = 0;
        std::string message;
        std::string previous;
        bool first = true;
        try {
            do {
                const auto revision = identity_.revision;
                service::edge_node::debug::EventContext event(context_, identity_, request, stop, resources_, operation->state);
                const auto policy = registry_.policy(request.event);
                if (policy.authenticated) {
                    if (identity_.accessToken.empty()) {
                        throw service::edge_node::debug::EventError(11004, "未登录");
                    }
                    const auto principal = service::auth::AuthTokenService::verifyAccessToken(event, identity_.accessToken);
                    event.userId = principal.userId;
                    if (!policy.permission.empty()) {
                        co_await service::auth::AuthService::requirePermission(event, event.userId, policy.permission);
                    }
                    if (stop.stop_requested() || identity_.revision != revision) {
                        break;
                    }
                }
                const auto result = co_await registry_.invoke(event);
                stop = ruvia::combine_stop_tokens(operationStop, result.lifetime);
                if (stop.stop_requested()) {
                    break;
                }
                if (identity_.revision != revision) {
                    if (!result.identityChanged) {
                        break;
                    }
                    // A successful identity change invalidates all earlier work.
                    for (auto& [id, pending] : operations_) {
                        if (id != request.id) {
                            pending->stop.request_stop();
                        }
                    }
                    const auto owner = identity_.accessToken.empty() ? std::string{} : service::auth::AuthTokenService::verifyAccessToken(event, identity_.accessToken).userId;
                    co_await closeResources(owner);
                }
                if (first || result.incremental || previous != result.data) {
                    co_await reply(request.id, result.data);
                }
                first = false;
                previous = result.data;
                if (!subscription || result.topic.empty()) {
                    break;
                }
                notification->topic = result.topic;
                if (result.more) {
                    continue;
                }
                while (!stop.stop_requested()) {
                    const auto change = co_await notification->receiver.receive_for(15s, stop);
                    if (stop.stop_requested()) {
                        break;
                    }
                    if (identity_.accessToken.empty()) {
                        throw service::edge_node::debug::EventError(11004, "未登录");
                    }
                    (void)service::auth::AuthTokenService::verifyAccessToken(event, identity_.accessToken);
                    if (change.has_value()) {
                        break;
                    }
                }
            } while (!stop.stop_requested());
        } catch (const service::edge_node::debug::EventError& error) {
            code = error.code;
            message = error.what();
        } catch (const ruvia::validation_error&) {
            code = 10001;
            message = "事件参数校验失败";
        } catch (const ruvia::http_error& error) {
            const auto info = error.info();
            code = service::common::errorCode(info.code(), info.status().value());
            message = info.message();
        } catch (const service::auth::JwtExpiredError&) {
            code = 11005;
            message = "登录已过期";
        } catch (const service::auth::JwtInvalidError&) {
            code = 11006;
            message = "登录令牌无效";
        } catch (...) {
            code = 10004;
            message = "操作失败";
        }
        if (code && !stop.stop_requested() && !closing_) {
            try {
                co_await reject(request.id, code, message);
            } catch (...) {
            }
        }
        notification.reset();
        operations_.erase(request.id);
        operation->completion.first.close();
    }

    ruvia::task<void> closeResources(std::string_view retainedOwner) {
        std::vector<std::pair<std::string, std::shared_ptr<service::edge_node::debug::ConnectionResource>>> closing;
        for (const auto& [id, resource] : resources_) {
            if (resource->owner != retainedOwner || resource->lifetime.stop_requested()) {
                resource->lifetime.request_stop();
                closing.emplace_back(id, resource);
            }
        }
        for (const auto& [id, resource] : closing) {
            const service::edge_node::debug::Request request;
            std::any state;
            service::edge_node::debug::EventContext event(context_, identity_, request, {}, resources_, state);
            event.userId = resource->owner;
            try {
                co_await resource->close(event);
                resources_.erase(id);
            } catch (...) {
                // Retain the stopped identity for a later cleanup attempt.
                // Collector leases also bound resources after process failure.
            }
        }
    }

    ruvia::task<void> watchResourceAuthorization() {
        using namespace std::chrono_literals;
        const auto notification = service::live::bus().subscribe(context_.worker(), "auth");
        const auto stop = tasks_.get_stop_token();
        while (!stop.stop_requested()) {
            const auto changed = co_await notification->receiver.receive_for(15s, stop);
            if (stop.stop_requested()) {
                break;
            }
            if (resources_.empty()) {
                continue;
            }
            const auto revision = identity_.revision;
            const service::edge_node::debug::Request request;
            std::any state;
            service::edge_node::debug::EventContext event(context_, identity_, request, stop, resources_, state);
            bool denied = false;
            try {
                event.userId = service::auth::AuthTokenService::verifyAccessToken(event, identity_.accessToken).userId;
                // Timeouts only verify token expiry. Permission data is queried
                // after an authorization-change notification, never on a timer.
                if (changed.has_value()) {
                    std::unordered_set<std::string> permissions;
                    for (const auto& [id, resource] : resources_) {
                        permissions.insert(resource->permission);
                    }
                    for (const auto& permission : permissions) {
                        co_await service::auth::AuthService::requirePermission(event, event.userId, permission);
                        if (identity_.revision != revision || stop.stop_requested()) {
                            break;
                        }
                    }
                }
            } catch (...) {
                denied = true;
            }
            if (denied && identity_.revision == revision) {
                co_await closeResources({});
            } else if (identity_.revision == revision) {
                co_await closeResources(event.userId);
            }
        }
    }

    ruvia::context& context_;
    ruvia::websocket& socket_;
    ruvia::task_scope tasks_;
    service::edge_node::debug::EventRegistry& registry_;
    service::edge_node::debug::ConnectionResources resources_;
    service::edge_node::debug::SessionIdentity identity_;
    std::unordered_map<std::string, std::shared_ptr<Operation>> operations_;
    std::unordered_set<std::string> seen_;
    bool closing_{};
};

class EdgeController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/edge")
    RUVIA_ROUTES_BEGIN(EdgeController)
    const auto options = ruvia::websocket_route_config{ .lifecycle_ = {
                                                          .heartbeat_ = { .ping_interval_ = std::chrono::seconds(30), .pong_timeout_ = std::chrono::seconds(15) },
                                                          .close_handshake_timeout_ = std::chrono::seconds(5) } };
    RUVIA_GET_WS_OPTIONS("/debug", debugConnection, options);
    RUVIA_ROUTES_END

  private:
    void registerDebugOperations(service::edge_node::debug::EventRegistry& registry) {
        registry.add<EdgeController, &EdgeController::authenticateDebugConnection, DebugAuthenticationBody>("edge.debug.authenticate", *this, { .authenticated = false });
        registry.add<EdgeController, &EdgeController::openSerial, NodeSerialOpenInput>("edge.serial.open", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::sendSerialCommand, NodeSerialCommandInput>("edge.serial.command", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::serialEvents, NodeSerialSessionInput>("edge.serial.events.subscribe", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::closeSerial, NodeSerialSessionInput>("edge.serial.close", *this);
        registry.add<EdgeController, &EdgeController::openTerminal, TerminalOpenInput>("edge.terminal.open", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::terminalEvents, TerminalSessionInput>("edge.terminal.events.subscribe", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::writeTerminal, TerminalWriteInput>("edge.terminal.write", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::resizeTerminal, TerminalResizeInput>("edge.terminal.resize", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::keepTerminalAlive, TerminalSessionInput>("edge.terminal.keepalive", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::acknowledgeTerminalOutput, TerminalAckInput>("edge.terminal.output.ack", *this, { .permission = "iot:edge:terminal" });
        registry.add<EdgeController, &EdgeController::closeTerminal, TerminalSessionInput>("edge.terminal.close", *this);
    }

    ruvia::task<void> debugConnection(ruvia::context& context) {
        service::edge_node::debug::EventRegistry registry;
        registerDebugOperations(registry);
        registry.seal();
        DebugConnection session(context, registry);
        co_await session.run();
    }

    ruvia::task<service::edge_node::debug::EventResult> authenticateDebugConnection(service::edge_node::debug::EventContext& c, const DebugAuthenticationBody& body) {
        const auto revision = c.identity.revision;
        const auto token = body.get<"token">().view();
        co_await edgeService().authenticateDebugConnection(c, token);
        if (c.stop.stop_requested() || c.identity.revision != revision) {
            throw service::edge_node::debug::EventError(10002, "认证操作已取消或会话已变更");
        }
        c.identity.accessToken.assign(token);
        ++c.identity.revision;
        co_return service::edge_node::debug::EventResult{ "null", {}, true };
    }

    class TerminalSession final : public service::edge_node::debug::ConnectionResource {
      public:
        TerminalSession(std::string id, std::string owner, std::string node, std::string connection, unsigned columns, unsigned rows)
            : ConnectionResource(std::move(owner), "iot:edge:terminal"), id(std::move(id)), node(std::move(node)), connection(std::move(connection)), columns(columns), rows(rows) {}

        ruvia::task<void> close(service::edge_node::debug::EventContext& c) override {
            (void)co_await edgeService().terminalOperation(c, node, id, connection, "terminal-close");
        }

        const std::string id;
        const std::string node;
        const std::string connection;
        unsigned columns;
        unsigned rows;
        unsigned protocolVersion{};
        std::uint64_t inputSequence{};
        bool writing{};
        std::weak_ptr<std::string> outputSubscription;
    };

    template <typename Body>
    static std::shared_ptr<TerminalSession> terminalSession(service::edge_node::debug::EventContext& c, const Body& body) {
        const auto found = c.resources.find("terminal:" + std::string(body.template get<"sessionId">().view()));
        const auto session = found == c.resources.end() ? nullptr : std::dynamic_pointer_cast<TerminalSession>(found->second);
        if (!session || session->owner != c.userId || session->node != body.template get<"id">().view() || session->lifetime.stop_requested()) {
            throw service::edge_node::debug::EventError(17018, "终端已结束或不属于当前连接");
        }
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        return session;
    }

    ruvia::task<service::edge_node::debug::EventResult> openTerminal(service::edge_node::debug::EventContext& c, const TerminalOpenInput& body) {
        if (c.resources.size() >= 8) {
            throw service::edge_node::debug::EventError(17018, "当前连接的调试会话过多");
        }
        const auto session = std::make_shared<TerminalSession>(c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), c.userId, std::string(body.get<"id">().view()), c.identity.connectionId, static_cast<unsigned>(body.get<"columns">().value_), static_cast<unsigned>(body.get<"rows">().value_));
        const auto key = "terminal:" + session->id;
        c.resources.emplace(key, session);
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        std::exception_ptr failure;
        try {
            const auto reply = co_await edgeService().openTerminal(c, session->node, session->id, session->connection, session->columns, session->rows);
            const auto parsed = ruvia::from_json<service::message::terminal::OpenReply>(reply, { .resource_ = c.arena() });
            const auto version = parsed ? parsed->get<"protocolVersion">().value_ : 0;
            if (c.stop.stop_requested() || !parsed || parsed->get<"id">().view() != session->id || version < 1 || version > 100) {
                throw service::edge_node::debug::EventError(17018, "终端打开已取消或响应无效");
            }
            session->protocolVersion = static_cast<unsigned>(version);
            co_return service::edge_node::debug::EventResult{ "{\"id\":" + service::utils::jsonQuoted(session->id) + "}" };
        } catch (...) {
            failure = std::current_exception();
        }
        session->lifetime.request_stop();
        c.stop = {};
        try {
            co_await session->close(c);
            c.resources.erase(key);
        } catch (...) {
        }
        std::rethrow_exception(failure);
    }

    ruvia::task<service::edge_node::debug::EventResult> terminalEvents(service::edge_node::debug::EventContext& c, const TerminalSessionInput& body) {
        const auto session = terminalSession(c, body);
        if (!c.state.has_value()) {
            if (!session->outputSubscription.expired()) {
                throw service::edge_node::debug::EventError(17018, "终端已有输出订阅");
            }
            const auto subscription = std::make_shared<std::string>(c.request.id);
            session->outputSubscription = subscription;
            c.state = subscription;
        }
        const auto reply = co_await edgeService().terminalOperation(c, session->node, session->id, session->connection, "terminal-events");
        const auto parsed = ruvia::from_json<service::message::terminal::EventsReply>(reply, { .resource_ = c.arena() });
        if (!parsed) {
            throw service::edge_node::debug::EventError(17018, "终端输出响应无效");
        }
        co_return service::edge_node::debug::EventResult{ reply, service::message::terminal::terminalEventTopic(session->node, session->id), false, session->lifetime.token(), parsed->get<"more">().value_, true };
    }

    ruvia::task<service::edge_node::debug::EventResult> writeTerminal(service::edge_node::debug::EventContext& c, const TerminalWriteInput& body) {
        const auto session = terminalSession(c, body);
        if (session->writing) {
            throw service::edge_node::debug::EventError(17018, "上一笔终端输入尚未确认");
        }
        session->writing = true;
        std::exception_ptr failure;
        try {
            co_await edgeService().writeTerminal(c, session->node, session->id, session->connection, session->protocolVersion, session->inputSequence, body.get<"content">().view());
        } catch (...) {
            failure = std::current_exception();
        }
        session->writing = false;
        if (failure) {
            session->lifetime.request_stop();
            c.stop = {};
            try {
                co_await session->close(c);
                c.resources.erase("terminal:" + session->id);
            } catch (...) {
            }
            std::rethrow_exception(failure);
        }
        co_return service::edge_node::debug::EventResult{};
    }

    ruvia::task<service::edge_node::debug::EventResult> resizeTerminal(service::edge_node::debug::EventContext& c, const TerminalResizeInput& body) {
        const auto session = terminalSession(c, body);
        session->columns = static_cast<unsigned>(body.get<"columns">().value_);
        session->rows = static_cast<unsigned>(body.get<"rows">().value_);
        (void)co_await edgeService().terminalOperation(c, session->node, session->id, session->connection, "terminal-resize", [&session](service::message::terminal::Request& request) {
            request.set<"columns">(session->columns).set<"rows">(session->rows);
        });
        co_return service::edge_node::debug::EventResult{};
    }

    ruvia::task<service::edge_node::debug::EventResult> keepTerminalAlive(service::edge_node::debug::EventContext& c, const TerminalSessionInput& body) {
        const auto session = terminalSession(c, body);
        (void)co_await edgeService().terminalOperation(c, session->node, session->id, session->connection, "terminal-keepalive", [&session](service::message::terminal::Request& request) {
            request.set<"columns">(session->columns).set<"rows">(session->rows);
        });
        co_return service::edge_node::debug::EventResult{};
    }

    ruvia::task<service::edge_node::debug::EventResult> acknowledgeTerminalOutput(service::edge_node::debug::EventContext& c, const TerminalAckInput& body) {
        const auto session = terminalSession(c, body);
        (void)co_await edgeService().terminalOperation(c, session->node, session->id, session->connection, "terminal-output-ack", [&body](service::message::terminal::Request& request) {
            request.set<"sequence">(body.get<"sequence">().value_);
        });
        co_return service::edge_node::debug::EventResult{};
    }

    ruvia::task<service::edge_node::debug::EventResult> closeTerminal(service::edge_node::debug::EventContext& c, const TerminalSessionInput& body) {
        const auto key = "terminal:" + std::string(body.get<"sessionId">().view());
        if (!c.resources.contains(key)) {
            co_return service::edge_node::debug::EventResult{};
        }
        const auto session = terminalSession(c, body);
        session->lifetime.request_stop();
        c.stop = {};
        co_await session->close(c);
        c.resources.erase(key);
        co_return service::edge_node::debug::EventResult{};
    }

    class SerialSession final : public service::edge_node::debug::ConnectionResource {
      public:
        SerialSession(std::string id, std::string owner, std::string node, std::string connection)
            : ConnectionResource(std::move(owner), "iot:edge:terminal"), id(std::move(id)), node(std::move(node)), connection(std::move(connection)) {}

        ruvia::task<void> close(service::edge_node::debug::EventContext& c) override {
            (void)co_await edgeService().serialOperation(c, node, id, connection, "serial-close");
        }

        const std::string id;
        const std::string node;
        const std::string connection;
    };

    static std::shared_ptr<SerialSession> serialSession(service::edge_node::debug::EventContext& c, std::string_view node, std::string_view id) {
        const auto found = c.resources.find("serial:" + std::string(id));
        const auto session = found == c.resources.end() ? nullptr : std::dynamic_pointer_cast<SerialSession>(found->second);
        if (!session || session->owner != c.userId || session->node != node || session->lifetime.stop_requested()) {
            throw service::edge_node::debug::EventError(17021, "串口会话已结束或不属于当前连接");
        }
        return session;
    }

    ruvia::task<service::edge_node::debug::EventResult> openSerial(service::edge_node::debug::EventContext& c, const NodeSerialOpenInput& body) {
        if (c.resources.size() >= 8) {
            throw service::edge_node::debug::EventError(17021, "当前连接的调试会话过多");
        }
        const auto session = std::make_shared<SerialSession>(c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), c.userId, std::string(body.get<"id">().view()), c.identity.connectionId);
        const auto key = "serial:" + session->id;
        c.resources.emplace(key, session);
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        std::exception_ptr failure;
        try {
            SerialDebugOpenRequest configuration;
            configuration.set<"path">(body.get<"path">().view());
            const auto result = co_await edgeService().openSerialDebug(c, std::string(body.get<"id">().view()), configuration, session->connection, session->id);
            if (c.stop.stop_requested()) {
                throw service::edge_node::debug::EventError(17021, "串口打开已取消");
            }
            co_return service::edge_node::debug::EventResult{ result };
        } catch (...) {
            failure = std::current_exception();
        }
        session->lifetime.request_stop();
        c.stop = {};
        try {
            co_await session->close(c);
            c.resources.erase(key);
        } catch (...) {
            // The stopped resource remains registered for cleanup retry.
        }
        std::rethrow_exception(failure);
    }

    ruvia::task<service::edge_node::debug::EventResult> sendSerialCommand(service::edge_node::debug::EventContext& c, const NodeSerialCommandInput& body) {
        const auto payload = ruvia::json_value::parse(c.request.data);
        const auto command = payload->get<ruvia::json_value>("command");
        if (!command || command->view().size() > 4096) {
            throw service::edge_node::debug::EventError(10001, "串口指令过大");
        }
        const auto session = serialSession(c, std::string(body.get<"id">().view()), std::string(body.get<"sessionId">().view()));
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        (void)co_await edgeService().serialOperation(c, session->node, session->id, session->connection, "serial-command", [&command](service::message::serial_debug::Request& request) {
            request.set<"command">(*command);
        });
        co_return service::edge_node::debug::EventResult{};
    }

    ruvia::task<service::edge_node::debug::EventResult> serialEvents(service::edge_node::debug::EventContext& c, const NodeSerialSessionInput& body) {
        const auto session = serialSession(c, body.get<"id">().view(), body.get<"sessionId">().view());
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        if (!c.state.has_value()) {
            c.state.emplace<std::int64_t>(0);
        }
        auto& cursor = std::any_cast<std::int64_t&>(c.state);
        const auto result = co_await edgeService().serialEvents(c, session->node, session->id, session->connection, cursor);
        co_return service::edge_node::debug::EventResult{ result, service::message::serial_debug::eventTopic(session->node, session->id), false, session->lifetime.token() };
    }

    ruvia::task<service::edge_node::debug::EventResult> closeSerial(service::edge_node::debug::EventContext& c, const NodeSerialSessionInput& body) {
        const auto key = "serial:" + std::string(body.get<"sessionId">().view());
        if (!c.resources.contains(key)) {
            co_return service::edge_node::debug::EventResult{};
        }
        const auto session = serialSession(c, body.get<"id">().view(), body.get<"sessionId">().view());
        session->lifetime.request_stop();
        co_await session->close(c);
        c.resources.erase(key);
        co_return service::edge_node::debug::EventResult{};
    }
};

class EdgeManagementController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/edge", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(EdgeManagementController)
    RUVIA_GET_SSE("/events", pageEvents, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::query_model<EdgeEventsQuery>, ruvia::query_model<LogsQuery>);
    RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::query_model<EdgeListQuery>);
    RUVIA_GET("/:id", detail, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<EdgeIdParams>);
    RUVIA_GET("/groups", groups, service::middleware::PermissionMiddleware<"iot:edge:query">);
    RUVIA_POST("/groups", createGroup, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::json_body<EdgeGroupBody>);
    RUVIA_PUT("/groups/:id", updateGroup, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<EdgeGroupBody>);
    RUVIA_DELETE("/groups/:id", removeGroup, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>);
    RUVIA_PUT("/:id/enrollment", enrollment, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<EnrollmentBody>);
    RUVIA_DELETE("/:id", removeEnrollment, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>);
    RUVIA_PUT("/:id/name", renameNode, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<NodeNameBody>);
    RUVIA_PUT("/:id/group", setNodeGroup, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<NodeGroupBody>);
    RUVIA_POST("/:id/network", network, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<NetworkBody>);
    RUVIA_GET("/:id/dtu", dtuChannels, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<EdgeIdParams>);
    RUVIA_PUT("/:id/dtu", saveDtuChannel, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<DtuChannelBody>);
    RUVIA_DELETE("/:id/dtu/:channelId", deleteDtuChannel, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<DtuChannelParams>);
    RUVIA_POST("/:id/sync", sync, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<EdgeIdParams>);
    RUVIA_POST_STREAM("/:id/firmware", uploadFirmware, service::middleware::PermissionMiddleware<"iot:edge:firmware">, ruvia::path_model<EdgeIdParams>, ruvia::query_model<FirmwareUploadBody>);
    RUVIA_POST("/:id/firmware/reuse", reuseFirmware, service::middleware::PermissionMiddleware<"iot:edge:firmware">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<FirmwareReuseBody>);
    RUVIA_POST("/:id/logs/capture", captureLogs, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<EdgeIdParams>);
    RUVIA_PUT("/:id/logs/level", logLevel, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<LogLevelBody>);
    RUVIA_ROUTES_END
  private:
    static std::optional<std::string> text(const std::optional<ruvia::string>& value) {
        return value ? std::optional<std::string>(std::string(value->view())) : std::nullopt;
    }

    template <typename Model>
    static std::string arraySnapshot(const ruvia::boxed_array<Model>& values) {
        std::string json{ "[" };
        for (const auto& value : values) {
            if (json.size() > 1) {
                json += ',';
            }
            json += ruvia::to_json(value);
        }
        return json + "]";
    }

    ruvia::task<void> pageEvents(ruvia::context& c) {

        const auto& scope = c.req().validated<EdgeEventsQuery>();
        const auto nodeId = text(scope.get<"nodeId">());
        if ((*scope.get<"logs">() || *scope.get<"vpn">()) && !nodeId) {
            service::common::fail(17003, "实时范围缺少节点 ID", 400);
        }
        std::vector<service::live::SnapshotChannel> channels{
            { "nodes", "edge", [this, &c](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                 co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                 co_return service::live::data(c, arraySnapshot(co_await edgeService().inventory(request)));
             } }
        };
        if (nodeId) {
            channels.push_back({ "detail", "edge", [this, &c, id = *nodeId](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                    co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                                    co_return service::live::data(c, std::string(ruvia::to_json(co_await edgeService().detail(request, id))));
                                } });
            if (*scope.get<"dtu">()) {
                channels.push_back({ "dtu", "edge-dtu:" + *nodeId, [this, &c, id = *nodeId](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                    co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                    co_return service::live::data(c, co_await edgeService().dtuChannels(request, id));
                } });
            }
            if (*scope.get<"logs">()) {
                channels.push_back({ "logs", service::message::edge::logSnapshotKey(*nodeId), [this, &c, id = *nodeId](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                        co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                                        co_return service::live::data(c, std::string(ruvia::to_json(co_await edgeService().logSnapshot(request, id, c.req().validated<LogsQuery>()))));
                                    } });
            }
            if (*scope.get<"vpn">()) {
                channels.push_back({ "vpn", "vpn", [this, &c, id = *nodeId](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                        co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                                        co_return service::live::data(c, co_await edgeService().vpnState(request, id));
                                    } });
            }
        }
        co_await service::live::serveSnapshotChannels(c, service::middleware::requireAuth(c).userId, std::move(channels), [&c] {
            (void)service::middleware::requireAuth(c);
        });
    }

    ruvia::task<ruvia::http_response> list(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& query = c.req().validated<EdgeListQuery>();
        auto result = co_await edgeService().list(request, *query.get<"page">(), *query.get<"pageSize">(), text(query.get<"keyword">()), text(query.get<"status">()), text(query.get<"groupId">()));
        co_return c.json(service::common::ok<EdgePageResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> detail(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        auto result = co_await edgeService().detail(request, body.get<"id">().view());
        co_return c.json(service::common::ok<EdgeNodeResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> groups(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<EdgeGroupsResponse>(request, co_await edgeService().groups(request)));
    }

    ruvia::task<ruvia::http_response> createGroup(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeGroupBody>();
        co_await edgeService().createGroup(request, body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> updateGroup(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeGroupBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().updateGroup(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> removeGroup(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        co_await edgeService().removeGroup(request, body.get<"id">().view());
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> enrollment(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EnrollmentBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().setEnrollment(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> removeEnrollment(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        co_await edgeService().removeEnrollment(request, body.get<"id">().view());
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> renameNode(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<NodeNameBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().renameNode(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> setNodeGroup(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<NodeGroupBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().setNodeGroup(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> network(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<NetworkBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().queueNetwork(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> dtuChannels(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto result = service::live::data(c, co_await edgeService().dtuChannels(request, c.req().validated<EdgeIdParams>().get<"id">().view()));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(result));
    }
    ruvia::task<ruvia::http_response> saveDtuChannel(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto body = c.req().validated_json<DtuChannelBody>();
        co_await edgeService().saveDtuChannel(request, c.req().validated<EdgeIdParams>().get<"id">().view(), body.value(), body.raw());
        co_return c.json(service::common::operation(c, "ok"));
    }
    ruvia::task<ruvia::http_response> deleteDtuChannel(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<DtuChannelParams>();
        co_await edgeService().deleteDtuChannel(request, body.get<"id">().view(), body.get<"channelId">().view());
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> sync(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        (void)co_await EdgeService::queueSnapshot(request, body.get<"id">().view(), request.userId);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> reuseFirmware(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        const auto& body = c.req().validated<FirmwareReuseBody>();
        FirmwareReuseResult result(ruvia::model_options{.resource_ = c.arena()});
        result.set<"reused">(co_await edgeService().reuseFirmware(request, id, body));
        co_return c.json(result);
    }

    ruvia::task<ruvia::http_response> uploadFirmware(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        if (c.req().header("Content-Type").value_or("") != "application/octet-stream") {
            service::common::fail(17017, "固件上传需要 application/octet-stream", 415);
        }
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        const auto& configuration = c.req().validated<FirmwareUploadBody>();
        try {
            const auto directory = co_await edgeService().prepareFirmwareUpload(request, id);
            service::edge_node::FirmwareUpload file(request.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), directory, static_cast<std::uint64_t>(configuration.get<"sizeBytes">().value_));
            auto& reader = c.req().get_body_reader();
            while (auto chunk = co_await reader.read()) {
                auto remaining = *chunk;
                while (!remaining.empty()) {
                    const auto bytes = remaining.first(std::min(remaining.size(), service::edge_node::FirmwareUpload::kChunkBytes));
                    try {
                        file.appendBytes(std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
                    } catch (const std::invalid_argument&) {
                        service::common::fail(17017, "固件内容超出声明大小", 400);
                    }
                    remaining = remaining.subspan(bytes.size());
                }
            }
            std::string hash;
            try {
                hash = file.finish();
            } catch (const std::invalid_argument&) {
                service::common::fail(17017, "固件上传尚未完成", 400);
            }
            (void)service::middleware::requireAuth(c);
            co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:firmware");
            co_await edgeService().finishFirmwareUpload(request, id, configuration, file.path, hash, static_cast<std::int64_t>(file.received));
        } catch (const ruvia::http_error&) {
            throw;
        } catch (const std::exception& error) {
            // 业务错误已映射为 http_error；其余异常以 500 返回，在请求边界记录原因。
            LOG_ERROR << "edge firmware upload failed: node_id=" << id << " error=" << error.what();
            throw;
        }
        co_return c.json(service::common::operation(c, "固件已上传，刷写任务已下发"));
    }

    ruvia::task<ruvia::http_response> captureLogs(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        const LogsQuery query;
        (void)co_await edgeService().logs(request, body.get<"id">().view(), query);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> logLevel(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<LogLevelBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().setLogLevel(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }
};

class EdgePublicController final {
  public:
    RUVIA_CONTROLLER_GROUP("/edge/v1/firmware")
    RUVIA_ROUTES_BEGIN(EdgePublicController)
    RUVIA_GET("/:id/download", download, ruvia::path_model<EdgeIdParams>, ruvia::query_model<FirmwareDownloadQuery>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> download(ruvia::context& c) {

        const auto& id = c.req().validated<EdgeIdParams>();
        const auto& query = c.req().validated<FirmwareDownloadQuery>();
        auto [path, fileName] = co_await edgeService().firmwareDownload(
            c,
            id.get<"id">().view(),
            query.get<"token">().view()
        );
        if (!std::filesystem::is_regular_file(path)) {
            service::common::fail(17009, "固件文件不存在", 404);
        }
        c.header("Content-Disposition", "attachment; filename=firmware.bin");
        co_return c.file(ruvia::file_response_options{
            .path_ = path,
            .content_type_ = "application/octet-stream",
        });
    }
};

} // namespace service::edge_node
