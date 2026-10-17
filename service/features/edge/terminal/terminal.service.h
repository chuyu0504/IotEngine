#pragma once

#include <array>
#include <chrono>
#include <memory>
#include <memory_resource>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <terminal.pb.h>

#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/edge/edge.config.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/session/session.service.h"
#include "service/features/edge/terminal/terminal.entity.h"
#include "service/features/edge/terminal/terminal.types.h"
#include "service/utils/base64.h"
#include "service/utils/json.h"
#include "service/utils/uuid.h"

namespace service::edge::terminal_state {

// Directed Pub/Sub only wakes browser waiters; frames and acknowledgements
// remain in the session's Redis state.

// KEYS: node session, owner, browser binding, closed marker.
// ARGV: node session, encoded binding.
inline constexpr std::string_view kOpenTerminalScript = R"lua(
if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('EXISTS',KEYS[4]) == 1 or redis.call('EXISTS',KEYS[2]) == 1 then return 0 end
redis.call('SET',KEYS[2],ARGV[1],'EX',120)
redis.call('SET',KEYS[3],ARGV[2],'EX',120)
return 1
)lua";

// KEYS: node session, owner, browser binding, closed marker, node input queue,
// input sequence, input ACK, output ACK, output sequence, ready marker, output.
// ARGV: node session, encoded binding, envelope, kind, sequence, acknowledged frame.
inline constexpr std::string_view kEnqueueBrowserInputScript = R"lua(
if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('GET',KEYS[2]) ~= ARGV[1] or redis.call('GET',KEYS[3]) ~= ARGV[2] then return 0 end
if ARGV[4] ~= 'close' and redis.call('EXISTS',KEYS[4]) == 1 then return 0 end
if redis.call('LLEN',KEYS[5]) >= 4096 then return 0 end
local sequence=tonumber(ARGV[5])
if ARGV[4] == 'input' then
 if redis.call('EXISTS',KEYS[10]) == 0 then return 0 end
 if sequence > 0 then
  local previous=tonumber(redis.call('GET',KEYS[6]) or '0')
  if sequence ~= previous + 1 or tonumber(redis.call('GET',KEYS[7]) or '0') ~= previous then return 0 end
  redis.call('SET',KEYS[6],ARGV[5],'EX',120)
 end
elseif ARGV[4] == 'ack' then
 local previous=tonumber(redis.call('GET',KEYS[8]) or '0')
 if sequence <= previous then return 1 end
 if sequence ~= previous + 1 or sequence > tonumber(redis.call('GET',KEYS[9]) or '0') then return 0 end
 if ARGV[6] == '' or redis.call('LREM',KEYS[11],1,ARGV[6]) ~= 1 then return 0 end
 redis.call('SET',KEYS[8],ARGV[5],'EX',120)
end
redis.call('RPUSH',KEYS[5],ARGV[3])
redis.call('EXPIRE',KEYS[5],120)
return 1
)lua";

inline constexpr std::string_view kDeleteKeysScript = R"lua(
return redis.call('DEL',unpack(KEYS))
)lua";

// ARGV: lifetime in seconds of every key.
inline constexpr std::string_view kExpireKeysScript = R"lua(
for _,key in ipairs(KEYS) do redis.call('EXPIRE',key,ARGV[1]) end
return 1
)lua";

// KEYS: node input queue. ARGV: envelope, queue limit, lifetime.
inline constexpr std::string_view kEnqueueInputScript = R"lua(
if redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
local length = redis.call('RPUSH', KEYS[1], ARGV[1])
if length == 1 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end
return length
)lua";

// KEYS: owner, output, input ACK, output sequence. ARGV: node session.
inline constexpr std::string_view kReleaseTerminalSessionScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])
return 1
)lua";

// Close only this terminal. Retain its final browser frame for the output pump.
// KEYS: owner, output, input ACK, output sequence.
// ARGV: node session, close frame, event topic, ACK topic.
inline constexpr std::string_view kFailScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('EXPIRE', KEYS[2], 120)
redis.call('PUBLISH', ARGV[3], '')
redis.call('PUBLISH', ARGV[4], '')
return 1
)lua";

// KEYS: node session, terminal owner, output queue, input ACK, output sequence.
// Sequence state must live as long as the idle terminal, not just its last I/O.
// Check both owners atomically so an old browser cannot extend a replaced session.
inline constexpr std::string_view kRefreshScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
for i = 2, 5 do
    redis.call('EXPIRE', KEYS[i], ARGV[2])
end
return 1
)lua";

// Ready, close and legacy data frames.
// KEYS: owner, output, ready marker, remote-closed marker.
// ARGV: node session, frame, lifetime, kind (ready|close|data), event topic, ACK topic.
inline constexpr std::string_view kSaveTerminalFrameScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('LLEN',KEYS[2]) >= 64 then return -1 end
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
if ARGV[4] == 'ready' then redis.call('SET',KEYS[3],'1','EX',ARGV[3]) end
if ARGV[4] == 'close' then
 redis.call('SET',KEYS[4],'1','EX',ARGV[3])
 redis.call('PUBLISH', ARGV[6], '')
end
redis.call('PUBLISH', ARGV[5], '')
return 1
)lua";

// Sequenced output (protocol 5+). Returns 2 for a duplicate, -1 for a gap or a
// full backlog. KEYS: owner, output, output sequence.
// ARGV: node session, frame, sequence, lifetime, event topic.
inline constexpr std::string_view kSaveTerminalDataScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local previous = tonumber(redis.call('GET', KEYS[3]) or '0')
local sequence = tonumber(ARGV[3])
if sequence == previous then return 2 end
if sequence ~= previous + 1 then return -1 end
if redis.call('LLEN',KEYS[2]) >= 64 then return -1 end
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])
redis.call('PUBLISH', ARGV[5], '')
return 1
)lua";

// Input acknowledgement (protocol 5+). KEYS: owner, input ACK.
// ARGV: node session, sequence, lifetime, ACK topic.
inline constexpr std::string_view kSaveTerminalAckScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local previous = tonumber(redis.call('GET', KEYS[2]) or '0')
local sequence = tonumber(ARGV[2])
if sequence == previous then return 1 end
if sequence ~= previous + 1 then return -1 end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('PUBLISH', ARGV[4], '')
return 1
)lua";

// Lifetime of browser bindings; keepalive and resize renew it.
inline constexpr auto kBrowserLifetime = std::chrono::seconds(120);

namespace webpb = ::iot::edge::terminal::v1;

class TerminalService final {
  public:
    static ruvia::task<std::string> executeBrowserOperation(ruvia::web_worker_context& c, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        if (stop.stop_requested()) {
            service::common::fail(17018, "终端操作已取消", 409);
        }
        const auto input = ruvia::from_json<service::message::terminal::Request>(payload, { .resource_ = c.pool() });
        if (!input) {
            service::common::fail(17018, "终端请求无效", 400);
        }
        const auto node = std::string(input->get<"nodeId">().view()), id = std::string(input->get<"sessionId">().view()), user = std::string(input->get<"userId">().view()), connection = std::string(input->get<"connectionId">().view());
        if (!service::common::isUuid(node) || !service::common::isUuid(id) || !service::common::isUuid(user) || !service::common::isUuid(connection)) {
            service::common::fail(17018, "终端会话身份无效", 400);
        }
        const auto bindingKey = TerminalBrowserRecord::key(id);
        const auto closedKey = TerminalBrowserRecord::closedKey(id);
        const auto ownerKey = TerminalSessionRecord::terminalSessionKey(node, id);
        const auto nodeKey = service::message::edge::sessionKey(node);
        const auto readyKey = TerminalBrowserRecord::readyKey(id);
        const auto dimensions = [&] {
            const auto& columns = input->get<"columns">();
            const auto& rows = input->get<"rows">();
            if (!columns || !rows || columns->value_ < 1 || columns->value_ > 1000 || rows->value_ < 1 || rows->value_ > 1000) {
                service::common::fail(17018, "终端尺寸无效", 400);
            }
            return std::pair(static_cast<unsigned>(columns->value_), static_cast<unsigned>(rows->value_));
        };
        std::array<std::uint8_t, 16> bytes{};
        (void)service::common::uuidBytes(id, bytes.data());
        const auto terminalBytes = protocol::bytes(bytes.data(), bytes.size());
        const auto envelope = [&] {
            return protocol::outbound(service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, node);
        };
        if (operation == "terminal-open") {
            const auto [columns, rows] = dimensions();
            const auto current = co_await c.redis().get(nodeKey);
            const auto state = current ? session_state::parse(*current) : std::nullopt;
            if (!state || !protocol::supportsProtocolVersion(state->protocolVersion)) {
                service::common::fail(17019, "节点当前离线", 409);
            }
            const TerminalBrowserRecord binding{ user, connection, node, std::string(*current) };
            const auto encoded = binding.encode();
            const std::string_view keys[]{ nodeKey, ownerKey, bindingKey, closedKey };
            const std::string_view args[]{ binding.nodeSession, encoded };
            const auto registered = co_await c.redis().eval(kOpenTerminalScript, keys, args);
            if (registered.kind() != ruvia::redis_value::kind_type::integer || registered.integer() != 1) {
                service::common::fail(17018, "终端打开已取消或节点已重连", 409);
            }
            auto open = envelope();
            auto* message = open.mutable_terminal_open();
            message->set_terminal_id(terminalBytes);
            message->set_columns(columns);
            message->set_rows(rows);
            if (state->protocolVersion <= 3) {
                message->set_ticket(id);
            }
            co_await enqueueBrowserInput(c, binding, id, open);
            if (state->protocolVersion <= 3) {
                co_await c.redis().set(readyKey, "1", { .expiration_ = ruvia::redis_set_expiration::expires_after(std::chrono::seconds(120)) });
                webpb::WebTerminalFrame ready;
                ready.mutable_ready();
                co_await saveTerminalFrame(c, ConnectionIdentity{ node, state->epoch, state->protocolVersion, state->workerIndex }, id, ready);
            }
            service::message::terminal::OpenReply reply(ruvia::model_options{ .resource_ = c.pool() });
            reply.set<"id">(id).set<"protocolVersion">(static_cast<std::int64_t>(state->protocolVersion));
            co_return service::message::rpc::encode(reply, c.pool());
        }
        const auto stored = co_await c.redis().get(bindingKey);
        if (!stored && operation == "terminal-close") {
            co_await c.redis().set(closedKey, "1", { .expiration_ = ruvia::redis_set_expiration::expires_after(std::chrono::seconds(600)) });
            co_return "{}";
        }
        const auto binding = stored ? TerminalBrowserRecord::decode(*stored) : std::nullopt;
        if (!binding || binding->userId != user || binding->connectionId != connection || binding->nodeId != node) {
            service::common::fail(17018, "终端不属于当前连接或已结束", 409);
        }
        const auto state = session_state::parse(binding->nodeSession);
        if (!state) {
            service::common::fail(17018, "终端节点状态无效", 409);
        }
        if (operation == "terminal-close") {
            co_await c.redis().set(closedKey, "1", { .expiration_ = ruvia::redis_set_expiration::expires_after(std::chrono::seconds(600)) });
            const auto current = co_await c.redis().get(nodeKey);
            const auto owner = co_await c.redis().get(ownerKey);
            const auto remoteClosed = co_await c.redis().get(TerminalBrowserRecord::remoteClosedKey(id));
            if (!remoteClosed && current && owner && std::string_view(*current) == binding->nodeSession && std::string_view(*owner) == binding->nodeSession) {
                auto close = envelope();
                close.mutable_terminal_close()->set_terminal_id(terminalBytes);
                close.mutable_terminal_close()->set_reason("browser closed");
                co_await enqueueBrowserInput(c, *binding, id, close);
            }
            co_await releaseTerminalSession(c, node, id, binding->nodeSession);
            const auto inputSequence = TerminalBrowserRecord::inputSequenceKey(id), outputAck = TerminalBrowserRecord::outputAckKey(id);
            const auto remoteClosedKey = TerminalBrowserRecord::remoteClosedKey(id);
            co_await deleteKeys(c, std::array<std::string_view, 5>{ bindingKey, readyKey, inputSequence, outputAck, remoteClosedKey });
            co_return "{}";
        }
        if (operation == "terminal-events") {
            ruvia::boxed_array<service::message::terminal::Event> events(ruvia::model_options{ .resource_ = c.pool() });
            const auto outputKey = service::message::terminal::terminalOutputKey(node, id);
            const bool reliable = state->protocolVersion >= 5;
            const auto& after = input->get<"after">();
            const auto& ready = input->get<"ready">();
            const auto& closed = input->get<"closed">();
            const auto cursor = after ? after->value_ : 0;
            if (cursor < 0) service::common::fail(17018, "终端输出游标无效", 400);
            bool more = false;
            const auto items = co_await c.redis().lrange(outputKey, 0, -1);
            for (const auto& storedFrame : items) {
                if (!reliable && events.size() == 32) { more = true; break; }
                std::optional<std::pmr::string> consumed;
                if (!reliable) {
                    consumed = co_await c.redis().lpop(outputKey);
                    if (!consumed) break;
                }
                const std::string_view wire = reliable ? std::string_view(storedFrame) : std::string_view(*consumed);
                webpb::WebTerminalFrame frame;
                if (!frame.ParseFromArray(wire.data(), static_cast<int>(wire.size()))) {
                    service::common::fail(17018, "终端输出格式无效", 502);
                }
                if (reliable && ((frame.has_data() && frame.data().sequence() <= static_cast<std::uint64_t>(cursor)) ||
                    (frame.has_ready() && ready && ready->value_) || (frame.has_close() && closed && closed->value_))) continue;
                if (events.size() == 32) { more = true; break; }
                auto& event = events.emplace();
                if (frame.has_ready()) {
                    event.set<"kind">("ready");
                } else if (frame.has_close()) {
                    event.set<"kind">("close").set<"reason">(frame.close().reason());
                } else if (frame.has_data()) {
                    event.set<"kind">("data")
                        .set<"content">(service::utils::encode_base64(frame.data().data()))
                        .set<"sequence">(static_cast<std::int64_t>(frame.data().sequence()));
                } else {
                    service::common::fail(17018, "终端输出类型无效", 502);
                }
            }
            service::message::terminal::EventsReply reply(ruvia::model_options{ .resource_ = c.pool() });
            reply.set<"events">(std::move(events)).set<"more">(more);
            co_return service::message::rpc::encode(reply, c.pool());
        }
        if (operation == "terminal-input-status") {
            const auto& sequence = input->get<"sequence">();
            if (!sequence || sequence->value_ <= 0) {
                service::common::fail(17018, "终端确认序号无效", 400);
            }
            const auto status = co_await inputAckStatus(c, node, id, binding->nodeSession, static_cast<std::uint64_t>(sequence->value_));
            if (status == InputAckStatus::OwnershipLost) {
                service::common::fail(17019, "终端连接已结束", 409);
            }
            if (status != InputAckStatus::Acknowledged && co_await c.redis().get(TerminalBrowserRecord::remoteClosedKey(id))) {
                service::common::fail(17019, "节点已关闭终端", 409);
            }
            service::message::terminal::InputStatusReply reply(ruvia::model_options{ .resource_ = c.pool() });
            reply.set<"acknowledged">(status == InputAckStatus::Acknowledged);
            co_return service::message::rpc::encode(reply, c.pool());
        }
        auto command = envelope();
        if (operation == "terminal-write") {
            std::string content;
            try {
                const auto& encoded = input->get<"content">();
                content = service::utils::decode_base64(encoded ? encoded->view() : std::string_view{}, 4096);
            } catch (const service::utils::base64_decode_error&) {
                service::common::fail(17018, "终端输入无效", 400);
            }
            const auto& sequence = input->get<"sequence">();
            if (!sequence || sequence->value_ < 0 || sequence->value_ > 9007199254740991LL || (state->protocolVersion >= 5 && sequence->value_ == 0)) {
                service::common::fail(17018, "终端输入无效", 400);
            }
            auto* data = command.mutable_terminal_data();
            data->set_terminal_id(terminalBytes);
            data->set_data(content);
            if (state->protocolVersion >= 5) {
                data->set_sequence(static_cast<std::uint64_t>(sequence->value_));
            }
        } else if (operation == "terminal-output-ack") {
            const auto& sequence = input->get<"sequence">();
            if (!sequence || sequence->value_ <= 0 || sequence->value_ > 9007199254740991LL || state->protocolVersion < 5) {
                service::common::fail(17018, "终端确认无效", 400);
            }
            auto* ack = command.mutable_terminal_data_ack();
            ack->set_terminal_id(terminalBytes);
            ack->set_sequence(static_cast<std::uint64_t>(sequence->value_));
        } else if (operation == "terminal-resize" || operation == "terminal-keepalive") {
            const auto [columns, rows] = dimensions();
            if (co_await refreshSession(c, node, id, binding->nodeSession) != 1) {
                service::common::fail(17019, "终端连接已结束", 409);
            }
            const auto inputSequence = TerminalBrowserRecord::inputSequenceKey(id), outputAck = TerminalBrowserRecord::outputAckKey(id);
            const std::string_view keys[]{ bindingKey, readyKey, inputSequence, outputAck };
            const auto lifetime = std::to_string(kBrowserLifetime.count());
            const std::string_view args[]{ lifetime };
            const auto renewed = co_await c.redis().eval(kExpireKeysScript, keys, args);
            if (renewed.kind() != ruvia::redis_value::kind_type::integer) {
                service::message::redis::throwValue("renew edge terminal browser state", renewed);
            }
            auto* resize = command.mutable_terminal_resize();
            resize->set_terminal_id(terminalBytes);
            resize->set_columns(columns);
            resize->set_rows(rows);
        } else {
            service::common::fail(17018, "未知终端操作", 400);
        }
        co_await enqueueBrowserInput(c, *binding, id, command);
        co_return "{}";
    }

    template <typename Context>
    static ruvia::task<void> deleteKeys(Context& c, std::span<const std::string_view> keys) {
        const auto reply = co_await c.redis().eval(kDeleteKeysScript, keys, std::span<const std::string_view>{});
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("delete edge terminal browser state", reply);
        }
    }

    template <typename Context>
    static ruvia::task<void> enqueueBrowserInput(Context& c, const TerminalBrowserRecord& binding, std::string_view id, const pb::Envelope& envelope) {
        const auto currentKey = service::message::edge::sessionKey(binding.nodeId), ownerKey = TerminalSessionRecord::terminalSessionKey(binding.nodeId, id);
        const auto browserKey = TerminalBrowserRecord::key(id), closedKey = TerminalBrowserRecord::closedKey(id), queueKey = service::message::terminal::terminalInputKey(binding.nodeId);
        const auto sequenceKey = TerminalBrowserRecord::inputSequenceKey(id), inputAckKey = TerminalSessionRecord::terminalInputAckKey(binding.nodeId, id);
        const auto outputAckKey = TerminalBrowserRecord::outputAckKey(id), outputSequenceKey = TerminalSessionRecord::terminalOutputSequenceKey(binding.nodeId, id), readyKey = TerminalBrowserRecord::readyKey(id);
        const auto encoded = protocol::encode(envelope), owner = binding.encode();
        const auto kind = envelope.has_terminal_close() ? "close" : envelope.has_terminal_data() ? "input"
            : envelope.has_terminal_data_ack()                                                   ? "ack"
                                                                                                 : "control";
        const auto sequence = std::to_string(envelope.has_terminal_data() ? envelope.terminal_data().sequence() : envelope.has_terminal_data_ack() ? envelope.terminal_data_ack().sequence()
                                                                                                                                                   : 0);
        const auto outputKey = service::message::terminal::terminalOutputKey(binding.nodeId, id);
        std::string acknowledged;
        if (envelope.has_terminal_data_ack()) {
            for (const auto& item : co_await c.redis().lrange(outputKey, 0, -1)) {
                webpb::WebTerminalFrame frame;
                if (!frame.ParseFromArray(item.data(), static_cast<int>(item.size()))) service::common::fail(17018, "终端输出格式无效", 502);
                if (frame.has_data() && frame.data().sequence() == envelope.terminal_data_ack().sequence()) {
                    acknowledged.assign(item.data(), item.size());
                    break;
                }
            }
        }
        const std::string_view keys[]{ currentKey, ownerKey, browserKey, closedKey, queueKey, sequenceKey, inputAckKey, outputAckKey, outputSequenceKey, readyKey, outputKey };
        const std::string_view args[]{ binding.nodeSession, owner, encoded, kind, sequence, acknowledged };
        const auto result = co_await c.redis().eval(kEnqueueBrowserInputScript, keys, args);
        if (result.kind() != ruvia::redis_value::kind_type::integer || result.integer() != 1) {
            service::common::fail(17018, "终端已关闭、尚未就绪或指令队列已满", 409);
        }
        co_await dispatch::notifyNode(c.redis(), binding.nodeId);
    }


    template <typename Context>
    static ruvia::task<InputAckStatus> inputAckStatus(Context& c, std::string_view nodeId, std::string_view terminalId, std::string_view nodeSession, std::uint64_t sequence) {
        const auto value = co_await c.redis().get(TerminalSessionRecord::terminalInputAckKey(nodeId, terminalId));
        if (value && std::string_view(value->data(), value->size()) == std::to_string(sequence)) {
            co_return InputAckStatus::Acknowledged;
        }
        const auto owner = co_await c.redis().get(TerminalSessionRecord::terminalSessionKey(nodeId, terminalId));
        if (!owner) {
            co_return InputAckStatus::OwnershipLost;
        }
        const TerminalSessionRecord record{ std::string(owner->data(), owner->size()) };
        if (record.nodeSession != nodeSession) {
            co_return InputAckStatus::OwnershipLost;
        }
        co_return InputAckStatus::Pending;
    }

    template <typename Context>
    static ruvia::task<std::int64_t> refreshSession(Context& c, std::string_view nodeId, std::string_view terminalId, std::string_view nodeSession) {
        const auto nodeKey = service::message::edge::sessionKey(nodeId);
        const auto owner = TerminalSessionRecord::terminalSessionKey(nodeId, terminalId);
        const auto output = service::message::terminal::terminalOutputKey(nodeId, terminalId);
        const auto inputAck = TerminalSessionRecord::terminalInputAckKey(nodeId, terminalId);
        const auto outputSequence = TerminalSessionRecord::terminalOutputSequenceKey(nodeId, terminalId);
        const std::string_view keys[]{ nodeKey, owner, output, inputAck, outputSequence };
        const std::string_view arguments[]{ nodeSession, "120" };
        const auto result = co_await c.redis().eval(kRefreshScript, keys, arguments);
        if (result.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("refresh edge terminal state", result);
        }
        co_return result.integer();
    }

    template <typename Context>
    static ruvia::task<void> enqueueInput(Context& c, std::string_view nodeId, const pb::Envelope& envelope) {
        const auto wire = protocol::encode(envelope);
        if (wire.empty()) {
            throw std::runtime_error("edge terminal envelope encode failed");
        }
        // Terminal input gets its own downstream key: the shared egress list is
        // head-trimmed by command dispatch, which would hand the node a terminal_data
        // whose terminal_open it never saw. Overflow here drops the newest envelope
        // instead, so whatever the node does receive stays a valid prefix.
        const auto key = service::message::terminal::terminalInputKey(nodeId);
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ wire, "4096", "120" };
        const auto reply = co_await c.redis().eval(kEnqueueInputScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("enqueue edge terminal input", reply);
        }
        if (reply.integer() == 0) {
            throw std::runtime_error("edge terminal input backlog exceeded");
        }
        co_await dispatch::notifyNode(c.redis(), nodeId);
    }

    template <typename Context>
    static ruvia::task<void> releaseTerminalSession(Context& c, std::string_view nodeId, std::string_view terminalId, std::string_view nodeSession) {
        const auto ownershipKey = TerminalSessionRecord::terminalSessionKey(nodeId, terminalId);
        const auto outputKey = service::message::terminal::terminalOutputKey(nodeId, terminalId);
        const auto inputAckKey = TerminalSessionRecord::terminalInputAckKey(nodeId, terminalId);
        const auto outputSequenceKey = TerminalSessionRecord::terminalOutputSequenceKey(nodeId, terminalId);
        const std::string_view keys[]{ ownershipKey, outputKey, inputAckKey, outputSequenceKey };
        const std::string_view arguments[]{ nodeSession };
        const auto reply = co_await c.redis().eval(kReleaseTerminalSessionScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("release edge terminal session", reply);
        }
    }

    template <typename Context>
    static ruvia::task<void> saveTerminalFrame(Context& c, const ConnectionIdentity& session, std::string_view terminalId, const webpb::WebTerminalFrame& frame) {
        std::string wire;
        if (!frame.SerializeToString(&wire)) {
            co_return;
        }
        const auto ownershipKey = TerminalSessionRecord::terminalSessionKey(session.nodeId, terminalId);
        const auto outputKey = service::message::terminal::terminalOutputKey(session.nodeId, terminalId);
        const auto epoch = session_state::value(
            session.epoch,
            session.protocolVersion,
            session.workerIndex,
            c.template worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId
        );
        const std::string ttl = "120";
        const auto readyKey = TerminalBrowserRecord::readyKey(terminalId), remoteClosedKey = TerminalBrowserRecord::remoteClosedKey(terminalId);
        const auto topic = service::message::terminal::terminalEventTopic(session.nodeId, terminalId), ackTopic = service::message::terminal::terminalAckTopic(session.nodeId, terminalId);
        const std::string_view keys[]{ ownershipKey, outputKey, readyKey, remoteClosedKey };
        const std::string_view arguments[]{ epoch, wire, ttl, frame.has_ready() ? "ready" : frame.has_close() ? "close"
                                                                                                              : "data",
                                            topic,
                                            ackTopic };
        const auto reply = co_await c.redis().eval(kSaveTerminalFrameScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("append edge terminal output", reply);
        }
        if (reply.integer() < 0) {
            co_await failTerminal(c, session, terminalId, "terminal output backlog exceeded");
        }
    }

    template <typename Context>
    static ruvia::task<void> failTerminal(Context& c, const ConnectionIdentity& session, std::string_view terminalId, std::string_view reason) {
        std::array<std::uint8_t, 16> terminalBytes{};
        if (!service::common::uuidBytes(terminalId, terminalBytes.data())) {
            co_return;
        }
        webpb::WebTerminalFrame frame;
        frame.mutable_close()->set_reason(std::string(reason));
        std::string wire;
        if (!frame.SerializeToString(&wire)) {
            co_return;
        }
        const auto owner = TerminalSessionRecord::terminalSessionKey(session.nodeId, terminalId);
        const auto output = service::message::terminal::terminalOutputKey(session.nodeId, terminalId);
        const auto inputAck = TerminalSessionRecord::terminalInputAckKey(session.nodeId, terminalId);
        const auto outputSequence = TerminalSessionRecord::terminalOutputSequenceKey(session.nodeId, terminalId);
        const auto epoch = session_state::value(session.epoch, session.protocolVersion, session.workerIndex, c.template worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId);
        const auto topic = service::message::terminal::terminalEventTopic(session.nodeId, terminalId), ackTopic = service::message::terminal::terminalAckTopic(session.nodeId, terminalId);
        const std::string_view keys[]{ owner, output, inputAck, outputSequence };
        const std::string_view args[]{ epoch, wire, topic, ackTopic };
        const auto result = co_await c.redis().eval(kFailScript, keys, args);
        if (result.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("close failed terminal", result);
        }
        if (result.integer() == 1) {
            auto close = service::edge::protocol::outbound(service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, session.nodeId);
            close.mutable_terminal_close()->set_terminal_id(
                protocol::bytes(terminalBytes.data(), terminalBytes.size())
            );
            close.mutable_terminal_close()->set_reason(std::string(reason));
            co_await enqueueInput(c, session.nodeId, close);
        }
    }

    static ruvia::task<void> saveTerminalData(ruvia::context& c, const ConnectionIdentity& session, const pb::TerminalData& data) {
        if (data.terminal_id().size() != 16 || data.data().empty()) {
            co_return;
        }
        const auto id = service::common::uuidText(data.terminal_id());
        if (data.data().size() > 16384 || data.sequence() > 9007199254740991ULL) {
            co_await failTerminal(c, session, id, "terminal output exceeds limits");
            co_return;
        }
        if (session.protocolVersion < 5) {
            webpb::WebTerminalFrame legacyFrame;
            legacyFrame.mutable_data()->set_data(data.data());
            co_await saveTerminalFrame(c, session, id, legacyFrame);
            co_return;
        }
        if (data.sequence() == 0) {
            co_await failTerminal(c, session, id, "terminal output sequence missing");
            co_return;
        }
        webpb::WebTerminalFrame frame;
        frame.mutable_data()->set_data(data.data());
        frame.mutable_data()->set_sequence(data.sequence());
        std::string wire;
        if (!frame.SerializeToString(&wire)) {
            co_return;
        }
        const auto ownershipKey = TerminalSessionRecord::terminalSessionKey(session.nodeId, id);
        const auto outputKey = service::message::terminal::terminalOutputKey(session.nodeId, id);
        const auto sequenceKey = TerminalSessionRecord::terminalOutputSequenceKey(session.nodeId, id);
        const auto epoch = session_state::value(
            session.epoch,
            session.protocolVersion,
            session.workerIndex,
            c.template worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId
        );
        const auto sequence = std::to_string(data.sequence());
        const std::string ttl = "120";
        const auto topic = service::message::terminal::terminalEventTopic(session.nodeId, id);
        const std::string_view keys[]{ ownershipKey, outputKey, sequenceKey };
        const std::string_view arguments[]{ epoch, wire, sequence, ttl, topic };
        const auto reply = co_await c.redis().eval(kSaveTerminalDataScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("append sequenced terminal output", reply);
        }
        if (reply.integer() < 0) {
            co_await failTerminal(c, session, id, "terminal output sequence mismatch");
        }
    }

    static ruvia::task<void> saveTerminalDataAck(
        ruvia::context& c,
        const ConnectionIdentity& session,
        const pb::TerminalDataAck& ack
    ) {
        if (session.protocolVersion < 5) {
            co_return;
        }
        if (ack.terminal_id().size() != 16) {
            co_return;
        }
        const auto id = service::common::uuidText(ack.terminal_id());
        if (ack.sequence() == 0) {
            co_await failTerminal(c, session, id, "terminal input sequence missing");
            co_return;
        }
        const auto ownershipKey = TerminalSessionRecord::terminalSessionKey(session.nodeId, id);
        const auto ackKey = TerminalSessionRecord::terminalInputAckKey(session.nodeId, id);
        const auto epoch = session_state::value(
            session.epoch,
            session.protocolVersion,
            session.workerIndex,
            c.template worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId
        );
        const auto sequence = std::to_string(ack.sequence());
        const std::string ttl = "120";
        const auto topic = service::message::terminal::terminalAckTopic(session.nodeId, id);
        const std::string_view keys[]{ ownershipKey, ackKey };
        const std::string_view arguments[]{ epoch, sequence, ttl, topic };
        const auto reply = co_await c.redis().eval(kSaveTerminalAckScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("advance terminal input ack", reply);
        }
        if (reply.integer() < 0) {
            co_await failTerminal(c, session, id, "terminal input acknowledgement mismatch");
        }
    }

    static ruvia::task<void> saveTerminalOpened(ruvia::context& c, const ConnectionIdentity& session, const pb::TerminalOpened& opened) {
        if (opened.terminal_id().size() != 16) {
            co_return;
        }
        const auto id = service::common::uuidText(opened.terminal_id());
        webpb::WebTerminalFrame frame;
        frame.mutable_ready();
        co_await saveTerminalFrame(c, session, id, frame);
    }

    static ruvia::task<void> saveTerminalClose(ruvia::context& c, const ConnectionIdentity& session, const pb::TerminalClose& close) {
        if (close.terminal_id().size() != 16) {
            co_return;
        }
        const auto id = service::common::uuidText(close.terminal_id());
        webpb::WebTerminalFrame frame;
        auto* terminalClose = frame.mutable_close();
        terminalClose->set_exit_code(close.exit_code());
        terminalClose->set_reason(close.reason());
        co_await saveTerminalFrame(c, session, id, frame);
    }
};

} // namespace service::edge::terminal_state
