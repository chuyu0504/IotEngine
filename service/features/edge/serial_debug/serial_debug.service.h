#pragma once

#include <array>
#include <chrono>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include <ruvia/web/context.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/edge/edge.config.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/serial_debug/serial_debug.entity.h"
#include "service/features/edge/serial_debug/serial_debug.protocol.h"
#include "service/features/edge/session/session.service.h"
#include "service/utils/change_feed.h"

namespace service::edge::serial_debug {

// Serial events are directed messages: they reach only the Service Worker that
// holds the browser connection, through the route it registered on open.

// KEYS: node session, owner, closed marker. ARGV: node session.
inline constexpr std::string_view kRegisterSessionScript = R"lua(
if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('EXISTS',KEYS[3]) == 1 then return 0 end
if not redis.call('SET',KEYS[2],ARGV[1],'EX',60,'NX') then return 0 end
return 1
)lua";

// KEYS: node session, owner, node input queue, closed marker.
// ARGV: node session, envelope, action.
inline constexpr std::string_view kEnqueueScript = R"lua(
if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('GET',KEYS[2]) ~= ARGV[1] then return 0 end
if ARGV[3] ~= 'close' and redis.call('EXISTS',KEYS[4]) == 1 then return 0 end
if redis.call('LLEN',KEYS[3]) >= 64 then return 0 end
redis.call('EXPIRE',KEYS[2],60)
redis.call('RPUSH',KEYS[3],ARGV[2])
redis.call('EXPIRE',KEYS[3],15)
return 1
)lua";

// KEYS: browser binding, request sequence, owner.
// ARGV: binding, request sequence, node session.
inline constexpr std::string_view kAcceptCommandScript = R"lua(
if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('GET',KEYS[3]) ~= ARGV[3] then return 0 end
local previous=tonumber(redis.call('GET',KEYS[2]) or '0')
if tonumber(ARGV[2]) <= previous then return 0 end
redis.call('SET',KEYS[2],ARGV[2],'EX',60)
redis.call('EXPIRE',KEYS[1],60)
return 1
)lua";

// KEYS: owner, output, node session, event route.
// ARGV: node session, event JSON, event topic, stream capacity, stream lifetime (ms).
inline constexpr std::string_view kSaveSerialEventScript = R"lua(
if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('GET',KEYS[3]) ~= ARGV[1] then return 0 end
redis.call('RPUSH',KEYS[2],ARGV[2])
redis.call('LTRIM',KEYS[2],-256,-1)
redis.call('EXPIRE',KEYS[2],60)
local stream = redis.call('GET', KEYS[4])
if stream then
  redis.call('XADD', stream, 'MAXLEN', '~', ARGV[4], '*', 'topic', ARGV[3])
  redis.call('PEXPIRE', stream, ARGV[5])
end
return 1
)lua";

// KEYS: owner, output. ARGV: node session.
inline constexpr std::string_view kReleaseScript = R"lua(
if redis.call('GET',KEYS[1]) == ARGV[1] then redis.call('DEL',KEYS[1],KEYS[2]) end
return 1
)lua";

inline constexpr std::string_view kDeleteKeysScript = R"lua(
return redis.call('DEL',unpack(KEYS))
)lua";

// Lifetime of the browser binding and its event route; commands renew it.
inline constexpr auto kBrowserLifetime = std::chrono::seconds(60);

class Service final {
  public:
    static ruvia::task<std::string> executeBrowserOperation(ruvia::web_worker_context& c, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        if (stop.stop_requested()) {
            service::common::fail(10004, "串口操作已取消", 503);
        }
        const auto input = ruvia::from_json<service::message::serial_debug::Request>(payload, { .resource_ = c.pool() });
        if (!input) {
            service::common::fail(17021, "串口请求格式无效", 400);
        }
        const auto user = std::string(input->get<"userId">().view()), connection = std::string(input->get<"connectionId">().view()), node = std::string(input->get<"nodeId">().view());
        if (!service::common::isUuid(user) || !service::common::isUuid(connection) || !service::common::isUuid(node)) {
            service::common::fail(17021, "串口会话身份无效", 400);
        }
        const auto id = std::string(input->get<"sessionId">().view());
        if (!service::common::isUuid(id)) {
            service::common::fail(17021, "串口会话无效", 400);
        }
        const auto closedKey = BrowserBindingRecord::closedKey(id);
        const auto topic = service::message::serial_debug::eventTopic(node, id);
        const auto route = service::message::live::routeKey(topic);
        if (operation == "serial-open") {
            const auto current = co_await c.redis().get(service::message::edge::sessionKey(node));
            if (!current) {
                service::common::fail(17019, "节点当前离线", 409);
            }
            const auto& path = input->get<"path">();
            auto ticket = SerialTargetRecord::decode(node + "\n" + std::string(*current) + "\n" + (path ? std::string(path->view()) : std::string{}));
            if (!ticket || !co_await registerSession(c, *ticket, id)) {
                service::common::fail(17021, "节点不支持串口调试或已重新连接", 409);
            }
            // This RPC runs on the worker holding the browser connection; route
            // the session's events there before the node can answer.
            co_await service::utils::ChangeFeed::route(c, topic, kBrowserLifetime);
            const BrowserBindingRecord binding{ user, connection, *ticket };
            co_await c.redis().set(BrowserBindingRecord::key(id), binding.encode(), { .expiration_ = ruvia::redis_set_expiration::expires_after(kBrowserLifetime) });
            co_await c.redis().set(BrowserBindingRecord::sequenceKey(id), "1", { .expiration_ = ruvia::redis_set_expiration::expires_after(kBrowserLifetime) });
            wire::SerialDebugRequest request;
            request.set_action("open");
            request.set_request_sequence(1);
            request.mutable_settings()->set_channel(ticket->path);
            if (!co_await enqueue(c, *ticket, id, std::move(request))) {
                co_await release(c, *ticket, id);
                co_await deleteKeys(c, std::array<std::string_view, 3>{ BrowserBindingRecord::key(id), BrowserBindingRecord::sequenceKey(id), route });
                service::common::fail(17021, "串口打开指令未入队", 409);
            }
            service::message::serial_debug::OpenReply reply(ruvia::model_options{ .resource_ = c.pool() });
            reply.set<"id">(id);
            co_return service::message::rpc::encode(reply, c.pool());
        }
        const auto bindingKey = BrowserBindingRecord::key(id);
        const auto stored = co_await c.redis().get(bindingKey);
        const auto binding = stored ? BrowserBindingRecord::decode(*stored) : std::nullopt;
        if (!stored && operation == "serial-close") {
            co_await c.redis().set(closedKey, "1", { .expiration_ = ruvia::redis_set_expiration::expires_after(std::chrono::seconds(120)) });
            co_await deleteKeys(c, std::array<std::string_view, 1>{ route });
            co_return "{}";
        }
        if (!binding || binding->userId != user || binding->connectionId != connection || binding->ticket.nodeId != node) {
            service::common::fail(17021, "串口会话已结束或不属于当前连接", 409);
        }
        const auto& ticket = binding->ticket;
        if (operation == "serial-events") {
            const auto& after = input->get<"after">();
            if (!after || after->value_ < 0) {
                service::common::fail(17021, "串口事件游标无效", 400);
            }
            const auto current = co_await c.redis().get(service::message::edge::sessionKey(node));
            if (!current || std::string_view(*current) != ticket.nodeSession) {
                service::common::fail(17019, "节点已断开或重新连接", 409);
            }
            const auto items = co_await c.redis().lrange(service::message::serial_debug::outputKey(node, id), 0, -1);
            ruvia::boxed_array<service::message::serial_debug::Event> events(ruvia::model_options{ .resource_ = c.pool() });
            auto cursor = after->value_;
            for (const auto& item : items) {
                auto event = ruvia::from_json<service::message::serial_debug::Event>(item, { .resource_ = c.pool() });
                if (!event || event->get<"sequence">().value_ <= cursor) {
                    continue;
                }
                const auto sequence = event->get<"sequence">().value_;
                if (sequence != cursor + 1) {
                    service::common::fail(17021, "串口接收缓冲区已溢出，请重新打开调试", 409);
                }
                events.emplace(std::move(*event));
                cursor = sequence;
            }
            service::message::serial_debug::EventsReply reply(ruvia::model_options{ .resource_ = c.pool() });
            reply.set<"events">(std::move(events)).set<"cursor">(cursor);
            co_return service::message::rpc::encode(reply, c.pool());
        }
        const auto sequenceKey = BrowserBindingRecord::sequenceKey(id);
        if (operation == "serial-close") {
            co_await c.redis().set(closedKey, "1", { .expiration_ = ruvia::redis_set_expiration::expires_after(std::chrono::seconds(120)) });
            const auto next = co_await c.redis().incr(sequenceKey);
            wire::SerialDebugRequest request;
            request.set_action("close");
            request.set_request_sequence(static_cast<std::uint64_t>(next));
            request.mutable_settings()->set_channel(ticket.path);
            if (!co_await enqueue(c, ticket, id, std::move(request))) {
                const auto current = co_await c.redis().get(service::message::edge::sessionKey(node));
                const auto owner = co_await c.redis().get(SessionRecord::key(node, id));
                if (current && owner && std::string_view(*current) == ticket.nodeSession && std::string_view(*owner) == ticket.nodeSession) {
                    // Preserve the owner and binding so cleanup can retry after
                    // backpressure subsides. A ended node connection needs no command.
                    service::common::fail(17021, "串口关闭指令暂未入队，将重试清理", 409);
                }
            }
            co_await release(c, ticket, id);
            co_await deleteKeys(c, std::array<std::string_view, 3>{ bindingKey, sequenceKey, route });
            co_return "{}";
        }
        if (operation != "serial-command") {
            service::common::fail(17021, "未知串口操作", 400);
        }
        const auto& command = input->get<"command">();
        auto request = command ? decodeRequest(command->view(), ticket.path, 1) : std::nullopt;
        if (!request) {
            service::common::fail(17021, "串口指令或参数无效", 400);
        }
        const auto sequence = std::to_string(request->request_sequence());
        const auto ownerKey = SessionRecord::key(node, id);
        const std::string_view keys[]{ bindingKey, sequenceKey, ownerKey };
        const std::string_view args[]{ *stored, sequence, ticket.nodeSession };
        const auto accepted = co_await c.redis().eval(kAcceptCommandScript, keys, args);
        if (accepted.kind() != ruvia::redis_value::kind_type::integer || accepted.integer() != 1) {
            service::common::fail(17021, "串口指令重复或会话已结束", 409);
        }
        co_await service::utils::ChangeFeed::route(c, topic, kBrowserLifetime);
        if (!co_await enqueue(c, ticket, id, std::move(*request))) {
            service::common::fail(17021, "串口指令未入队，请检查节点连接", 409);
        }
        co_return "{}";
    }

    template <typename Context>
    static ruvia::task<bool> registerSession(Context& c, const SerialTargetRecord& ticket, std::string_view id) {
        const auto parsed = session_state::parse(ticket.nodeSession);
        if (!parsed || parsed->protocolVersion < 5) {
            co_return false;
        }
        const SessionRecord record{ ticket.nodeSession };
        const auto currentKey = service::message::edge::sessionKey(ticket.nodeId);
        const auto ownerKey = SessionRecord::key(ticket.nodeId, id);
        const auto closedKey = BrowserBindingRecord::closedKey(id);
        const std::string_view keys[]{ currentKey, ownerKey, closedKey };
        const std::string_view args[]{ record.nodeSession };
        const auto reply = co_await c.redis().eval(kRegisterSessionScript, keys, args);
        co_return reply.kind() == ruvia::redis_value::kind_type::integer&& reply.integer() == 1;
    }

    template <typename Context>
    static ruvia::task<bool> enqueue(Context& c, const SerialTargetRecord& ticket, std::string_view id, wire::SerialDebugRequest request) {
        const auto parsed = session_state::parse(ticket.nodeSession);
        if (!parsed) {
            co_return false;
        }
        std::array<std::uint8_t, 16> bytes{};
        if (!service::common::uuidBytes(id, bytes.data())) {
            co_return false;
        }
        request.set_session_id(bytes.data(), bytes.size());
        auto envelope = protocol::outbound(c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, ticket.nodeId);
        envelope.set_session_epoch(parsed->epoch);
        envelope.set_protocol_version(parsed->protocolVersion);
        *envelope.mutable_serial_debug_request() = std::move(request);
        const auto encoded = protocol::encode(envelope);
        const auto currentKey = service::message::edge::sessionKey(ticket.nodeId);
        const auto ownerKey = SessionRecord::key(ticket.nodeId, id);
        const auto inputKey = service::message::serial_debug::inputKey(ticket.nodeId);
        const auto closedKey = BrowserBindingRecord::closedKey(id);
        const std::string_view keys[]{ currentKey, ownerKey, inputKey, closedKey };
        const std::string_view args[]{ ticket.nodeSession, encoded, envelope.serial_debug_request().action() };
        const auto reply = co_await c.redis().eval(kEnqueueScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer || reply.integer() != 1) {
            co_return false;
        }
        co_await dispatch::notifyNode(c.redis(), ticket.nodeId);
        co_return true;
    }

    static ruvia::task<void> saveEvent(ruvia::context& c, std::string_view nodeId, std::string_view nodeSession, const wire::SerialDebugEvent& event) {
        if (event.session_id().size() != 16 || event.data().size() > 1024 ||
            event.sequence() == 0) {
            co_return;
        }
        const auto id = protocol::uuidText(event.session_id());
        const auto ownerKey = SessionRecord::key(nodeId, id);
        const auto outputKey = service::message::serial_debug::outputKey(nodeId, id);
        const auto currentKey = service::message::edge::sessionKey(nodeId);
        const auto json = eventJson(event);
        const auto topic = service::message::serial_debug::eventTopic(nodeId, id);
        const auto route = service::message::live::routeKey(topic);
        const std::string_view keys[]{ ownerKey, outputKey, currentKey, route };
        const std::string_view args[]{ nodeSession, json, topic, service::message::live::kWorkerStreamCapacity, service::message::live::kWorkerStreamTtlMilliseconds };
        const auto reply = co_await c.redis().eval(kSaveSerialEventScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("save edge serial event", reply);
        }
    }

    template <typename Context>
    static ruvia::task<void> release(Context& c, const SerialTargetRecord& ticket, std::string_view id) {
        const auto ownerKey = SessionRecord::key(ticket.nodeId, id);
        const auto outputKey = service::message::serial_debug::outputKey(ticket.nodeId, id);
        const std::string_view keys[]{ ownerKey, outputKey };
        const std::string_view args[]{ ticket.nodeSession };
        const auto reply = co_await c.redis().eval(kReleaseScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("release edge serial session", reply);
        }
    }

    template <typename Context>
    static ruvia::task<void> deleteKeys(Context& c, std::span<const std::string_view> keys) {
        const auto reply = co_await c.redis().eval(kDeleteKeysScript, keys, std::span<const std::string_view>{});
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("delete edge serial browser state", reply);
        }
    }
};
} // namespace service::edge::serial_debug
