#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/controller.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/gateway/gateway.service.h"
#include "service/features/edge/gateway/gateway.types.h"
#include "service/middleware/log.h"
#include "service/utils/uuid.h"

namespace service::edge {
class GatewayTransport final {
  public:
    RUVIA_CONTROLLER_GROUP("/edge/v1")
    RUVIA_ROUTES_BEGIN(GatewayTransport)
    const auto webSocketOptions = ruvia::websocket_route_config{
        .lifecycle_ = {
            .heartbeat_ = {
                .ping_interval_ = std::chrono::seconds(300),
                .pong_timeout_ = std::chrono::seconds(60),
            },
            .close_handshake_timeout_ = std::chrono::seconds(5),
        },
        .deflate_ = {
            .enabled_ = true,
            .compression_level_ = 9,
            .context_takeover_ = true,
        },
    };
    RUVIA_GET_WS_OPTIONS("/connect", connect, webSocketOptions);
    RUVIA_ROUTES_END

  private:
    struct TelemetryReceipt {
        std::array<std::uint8_t, 16> recordId{};
        std::uint64_t acknowledgedAtMs{};
        bool occupied{};
    };

    struct Session {
        std::string nodeId;
        std::array<std::uint8_t, 16> nodeBytes{};
        std::array<std::uint8_t, 16> platformBytes{};
        std::uint32_t protocolVersion{ service::message::edge::kProtocolVersion };
        std::size_t workerIndex{};
        std::uint64_t epoch{};
        std::uint64_t inboundSequence{};
        std::uint64_t outboundSequence{};
        std::uint64_t configSentAtMs{};
        std::deque<std::string> outbound;
        std::array<TelemetryReceipt, 64> telemetryReceipts{};
        std::size_t nextTelemetryReceipt{};
        gateway::FirmwareTransfer firmwareTransfer;
        bool capabilitySeen{};
        bool sparseHeartbeat{};
        bool statusReporting{};
        bool supportsCommandStartBefore{};
        std::chrono::steady_clock::time_point lastInbound{ std::chrono::steady_clock::now() };
    };

    struct LiveSession {
        ruvia::context* context{};
        ruvia::websocket* socket{};
        Session* session{};
        ruvia::task_scope* scope{};
        bool active{ true };
        bool flushPending{};
        bool flushing{};
    };

    ruvia::task<void> connect(ruvia::context& c) {
        auto& socket = c.get_websocket();
        auto& dispatcher = c.worker_state<SessionDispatcher>();
        const auto workerIndex = dispatcher.workerIndex();
        auto first = co_await socket.read();
        if (!first || !first->binary()) {
            co_await socket.close(
                ruvia::websocket_close_options{ .code_ = 1002, .reason_ = "binary hello required" }
            );
            co_return;
        }
        pb::Envelope input;
        if (!protocol::decode(first->payload(), input) ||
            !protocol::supportsProtocolVersion(input.protocol_version()) ||
            input.payload_case() != pb::Envelope::kHello ||
            !protocol::validSessionPlatformId(input.platform_id()) ||
            !protocol::validImei(input.hello().imei())) {
            co_await socket.close(
                ruvia::websocket_close_options{ .code_ = 1002, .reason_ = "invalid hello" }
            );
            co_return;
        }
        const auto enrollment = co_await gateway::GatewayService::acceptHello(
            c, workerIndex, first->payload(), input.hello().imei()
        );
        if (!enrollment) {
            co_await socket.close(ruvia::websocket_close_options{
                .code_ = 1008,
                .reason_ = "worker lease unavailable",
            });
            co_return;
        }
        const std::string imei(input.hello().imei());
        const bool sparseHeartbeat = input.hello().supports_sparse_heartbeat();
        const bool statusReporting = input.hello().supports_status_reporting();
        const bool supportsCommandStartBefore = input.hello().supports_command_start_before();
        std::string nodeId = enrollment->nodeId;
        bool approved = enrollment->approved;
        auto session = makeSession(
            nodeId,
            input.protocol_version(),
            input.platform_id(),
            workerIndex
        );
        if (!approved) {
            co_await sendEnrollment(c, socket, session);

            session.inboundSequence = input.sequence();
            while (auto message = co_await socket.read()) {
                if (!message->binary() || !protocol::decode(message->payload(), input) ||
                    input.protocol_version() != session.protocolVersion ||
                    input.payload_case() != pb::Envelope::kHeartbeat ||
                    (supportsCommandStartBefore &&
                     (!input.heartbeat().has_nonce() || input.heartbeat().nonce() == 0)) ||
                    !input.node_id().empty() || input.session_epoch() != 0 ||
                    input.sequence() <= session.inboundSequence ||
                    input.platform_id() !=
                        protocol::bytes(session.platformBytes.data(), session.platformBytes.size())) {
                    co_await socket.close(ruvia::websocket_close_options{
                        .code_ = 1002,
                        .reason_ = "invalid pending heartbeat",
                    });
                    co_return;
                }
                session.inboundSequence = input.sequence();
                session.lastInbound = std::chrono::steady_clock::now();

                const auto refreshed = co_await gateway::GatewayService::loadEnrollment(c, imei, nodeId);
                nodeId = refreshed.nodeId;
                approved = refreshed.approved;
                if (approved) {
                    break;
                }

                auto heartbeat = makeEnvelope(c, session);
                auto* heartbeatAck = heartbeat.mutable_heartbeat_ack();
                heartbeatAck->set_platform_time_ms(service::message::utcNowMilliseconds());
                if (input.heartbeat().has_nonce()) {
                    heartbeatAck->set_nonce(input.heartbeat().nonce());
                }
                if (supportsCommandStartBefore) {
                    bool sampleAvailable = false;
                    try {
                        heartbeatAck->set_database_time_ms(
                            co_await gateway::GatewayService::databaseTimeMilliseconds(c)
                        );
                        sampleAvailable = true;
                    } catch (const std::exception& error) {
                        LOG_ERROR << "pending edge heartbeat database time sample failed: " << error.what();
                    }
                    if (!sampleAvailable) {
                        co_await socket.close(ruvia::websocket_close_options{
                            .code_ = 1011,
                            .reason_ = "database time sample unavailable",
                        });
                        co_return;
                    }
                }
                co_await send(socket, heartbeat);
            }
            if (!approved) {
                co_return;
            }
            session = makeSession(
                nodeId,
                session.protocolVersion,
                protocol::bytes(session.platformBytes.data(), session.platformBytes.size()),
                workerIndex
            );
        }

        session.sparseHeartbeat = sparseHeartbeat;
        session.statusReporting = statusReporting;
        session.supportsCommandStartBefore = supportsCommandStartBefore;
        const auto opened = co_await gateway::GatewayService::openSession(c, connectionIdentity(session));
        if (opened != gateway::IngressStatus::Accepted) {
            co_await socket.close(ruvia::websocket_close_options{
                .code_ = 1008,
                .reason_ = opened == gateway::IngressStatus::SessionReplaced
                    ? "session replaced" : "worker lease lost",
            });
            co_return;
        }
        auto ack = makeEnvelope(c, session);
        auto* helloAck = ack.mutable_hello_ack();
        helloAck->set_assigned_node_id(
            protocol::bytes(session.nodeBytes.data(), session.nodeBytes.size())
        );
        helloAck->set_session_epoch(session.epoch);
        helloAck->set_negotiated_protocol_version(session.protocolVersion);
        helloAck->set_raw_telemetry(input.hello().supports_raw_telemetry());
        // The wire field name is retained for deployed firmware.
        helloAck->set_heartbeat_interval_sec(statusReporting ? 900 : 300);
        helloAck->set_max_message_size(static_cast<std::uint32_t>(service::message::edge::kMaxMessageSize));
        helloAck->set_platform_time_ms(service::message::utcNowMilliseconds());
        if (supportsCommandStartBefore) {
            bool sampleAvailable = false;
            try {
                helloAck->set_database_time_ms(
                    co_await gateway::GatewayService::databaseTimeMilliseconds(c)
                );
                sampleAvailable = true;
            } catch (const std::exception& error) {
                LOG_ERROR << "edge hello database time sample failed: " << error.what();
            }
            if (!sampleAvailable) {
                co_await gateway::GatewayService::closeSession(c, connectionIdentity(session));
                co_await socket.close(ruvia::websocket_close_options{
                    .code_ = 1011,
                    .reason_ = "database time sample unavailable",
                });
                co_return;
            }
        }
        co_await send(socket, ack);
        std::exception_ptr sessionFailure;
        // Egress must not wait for the node to speak first. This worker's own
        // dispatcher wakes only sessions accepted by this worker.
        ruvia::task_scope egressScope(
            c.worker(),
            ruvia::task_scope_options{ .resource_ = c.pool() }
        );
        auto live = std::make_shared<LiveSession>(LiveSession{
            .context = &c,
            .socket = &socket,
            .session = &session,
            .scope = &egressScope,
        });
        bool registered = false;
        try {
            std::weak_ptr<LiveSession> weak = live;
            dispatcher.registerSession(
                session.nodeId,
                session.epoch,
                [weak] {
                    if (const auto current = weak.lock()) {
                        requestFlush(current);
                    }
                },
                [weak](std::string_view) {
                    if (const auto current = weak.lock()) {
                        if (!current->active) {
                            return;
                        }
                        current->active = false;
                        current->scope->request_stop();
                        // SessionDispatcher::failSessions is invoked by this same
                        // Service Worker. Abort wakes the connection's read
                        // loop without moving the socket across workers.
                        current->socket->abort();
                    }
                }
            );
            registered = true;
            egressScope.spawn(maintainSession(live));
            // Reliable per-node queues may already contain work from while the
            // node was offline; connection establishment is itself a wakeup.
            requestFlush(live);
            while (auto message = co_await socket.read()) {
                if (!message->binary() || !protocol::decode(message->payload(), input) ||
                    !validInbound(input, session)) {
                    co_await socket.close(ruvia::websocket_close_options{
                        .code_ = 1002,
                        .reason_ = "invalid envelope",
                    });
                    break;
                }
                const auto telemetry = telemetryDecision(session, input);
                const auto accepted = co_await gateway::GatewayService::acceptIngress(
                    c, connectionIdentity(session), input, message->payload(), telemetry.publish
                );
                if (accepted != gateway::IngressStatus::Accepted) {
                    co_await socket.close(ruvia::websocket_close_options{
                        .code_ = 1008,
                        .reason_ = accepted == gateway::IngressStatus::SessionReplaced
                            ? "session replaced" : "worker lease lost",
                    });
                    break;
                }
                session.inboundSequence = input.sequence();
                session.lastInbound = std::chrono::steady_clock::now();
                if (telemetry.acknowledge) {
                    co_await handle(c, session, input, telemetry.publish);
                }
                requestFlush(live);
            }
        } catch (...) {
            sessionFailure = std::current_exception();
        }
        live->active = false;
        if (registered) {
            dispatcher.unregisterSession(session.nodeId, session.epoch);
        }
        egressScope.request_stop();
        try {
            co_await egressScope.join();
        } catch (...) {
            if (!sessionFailure) {
                sessionFailure = std::current_exception();
            }
        }
        try {
            co_await gateway::GatewayService::closeSession(c, connectionIdentity(session));
        } catch (...) {
            if (!sessionFailure) {
                sessionFailure = std::current_exception();
            }
        }
        if (sessionFailure) {
            std::rethrow_exception(sessionFailure);
        }
    }

    static Session makeSession(std::string nodeId, std::uint32_t protocolVersion, std::string_view platformId, std::size_t workerIndex) {
        Session result;
        result.nodeId = std::move(nodeId);
        result.protocolVersion = protocolVersion;
        result.workerIndex = workerIndex;
        service::common::uuidBytes(result.nodeId, result.nodeBytes.data());
        std::memcpy(result.platformBytes.data(), platformId.data(), result.platformBytes.size());
        result.epoch = randomEpoch();
        return result;
    }

    static ruvia::task<void> sendEnrollment(ruvia::context& c, ruvia::websocket& socket, Session& session) {
        auto reply = makeEnvelope(c, session);
        auto* enrollment = reply.mutable_enrollment_pending();
        enrollment->set_code("pending");
        enrollment->set_message("registration pending approval");
        co_await send(socket, reply);
    }

    static std::uint64_t randomEpoch() {
        static thread_local std::mt19937_64 random(std::random_device{}());
        auto value = random();
        return value == 0 ? 1 : value;
    }

    static bool validInbound(const pb::Envelope& input, const Session& session) {
        if (input.protocol_version() != session.protocolVersion ||
            input.node_id().size() != 16 || input.platform_id().size() != 16 ||
            input.session_epoch() != session.epoch ||
            input.sequence() <= session.inboundSequence ||
            input.node_id() != protocol::bytes(session.nodeBytes.data(), 16) ||
            input.platform_id() != protocol::bytes(session.platformBytes.data(), 16)) {
            return false;
        }
        return !session.supportsCommandStartBefore ||
            input.payload_case() != pb::Envelope::kHeartbeat ||
            (input.heartbeat().has_nonce() && input.heartbeat().nonce() != 0);
    }

    struct TelemetryDecision {
        bool publish{ true };
        bool acknowledge{ true };
    };

    static TelemetryDecision telemetryDecision(Session& session, const pb::Envelope& input) {
        if (input.payload_case() != pb::Envelope::kTelemetryBatch ||
            input.telemetry_batch().records().empty() ||
            input.telemetry_batch().records(0).record_id().size() != 16) {
            return {};
        }
        constexpr std::uint64_t retryAckIntervalMs = 1000;
        const auto recordId = input.telemetry_batch().records(0).record_id();
        const auto now = service::message::utcNowMilliseconds();
        for (auto& receipt : session.telemetryReceipts) {
            if (!receipt.occupied ||
                std::memcmp(receipt.recordId.data(), recordId.data(), 16) != 0) {
                continue;
            }
            if (now - receipt.acknowledgedAtMs < retryAckIntervalMs) {
                return { .publish = false, .acknowledge = false };
            }
            receipt.acknowledgedAtMs = now;
            return { .publish = false, .acknowledge = true };
        }
        auto& receipt = session.telemetryReceipts[session.nextTelemetryReceipt];
        std::memcpy(receipt.recordId.data(), recordId.data(), 16);
        receipt.acknowledgedAtMs = now;
        receipt.occupied = true;
        session.nextTelemetryReceipt =
            (session.nextTelemetryReceipt + 1) % session.telemetryReceipts.size();
        return {};
    }

    static pb::Envelope makeEnvelope(ruvia::context& c, Session& session) {
        auto result = service::edge::protocol::outbound(service::utils::nextUuid(c.worker_state<service::common::UuidV7State>()), service::message::utcNowMilliseconds(), service::common::uuidText(protocol::bytes(session.platformBytes.data(), session.platformBytes.size())), session.nodeId, session.epoch, ++session.outboundSequence, session.protocolVersion);
        return result;
    }

    static ruvia::task<void> send(ruvia::websocket& socket, const pb::Envelope& envelope) {
        const auto wire = protocol::encode(envelope);
        if (wire.empty()) {
            throw std::runtime_error("edge envelope encode failed");
        }
        co_await socket.binary(wire, { .compress_ = true });
    }

    static ruvia::task<void> sendSessionEnvelope(
        ruvia::context& c,
        ruvia::websocket& socket,
        Session& session,
        pb::Envelope& envelope
    ) {
        if (envelope.has_heartbeat_ack() && session.supportsCommandStartBefore &&
            envelope.heartbeat_ack().has_nonce()) {
            if (envelope.heartbeat_ack().nonce() == 0) {
                throw std::runtime_error("correlated heartbeat nonce is invalid");
            }
            envelope.mutable_heartbeat_ack()->set_database_time_ms(
                co_await gateway::GatewayService::databaseTimeMilliseconds(c)
            );
        }
        co_await send(socket, envelope);
    }

    static void enqueue(Session& session, const pb::Envelope& envelope) {
        auto wire = protocol::encode(envelope);
        if (wire.empty()) {
            throw std::runtime_error("edge envelope encode failed");
        }
        session.outbound.emplace_back(std::move(wire));
    }

    // This worker's SessionDispatcher wakes the worker-local session when one of its
    // reliable queues changes. Redis reads are non-blocking and reuse this
    // worker's ordinary pool; no callback or socket crosses worker boundaries.
    static void requestFlush(const std::shared_ptr<LiveSession>& live) {
        if (!live->active || live->scope->stop_requested()) {
            return;
        }
        live->flushPending = true;
        if (live->flushing) {
            return;
        }
        live->flushing = true;
        try {
            live->scope->spawn(flushEgress(live));
        } catch (...) {
            live->flushing = false;
            throw;
        }
    }

    static ruvia::task<void> maintainSession(std::shared_ptr<LiveSession> live) {
        // 旧固件按真实应用消息维护时序；新固件明确声明状态上报能力后，
        // 由原生 WS 探活维护连接，本任务只续租 Worker 的路由记录。
        const auto probeInterval = (live->session->statusReporting || live->session->sparseHeartbeat)
            ? std::chrono::seconds(300)
            : std::chrono::seconds(20);
        const auto idleTimeout = live->session->sparseHeartbeat
            ? std::chrono::seconds(900)
            : std::chrono::seconds(60);
        try {
            while (live->active && !live->scope->stop_requested()) {
                (void)co_await ruvia::sleep_for(live->context->worker(), probeInterval, live->scope->get_stop_token());
                if (!live->active || live->scope->stop_requested()) {
                    break;
                }
                if (live->session->statusReporting) {
                    // Native WebSocket Ping/Pong owns transport liveness. Refresh
                    // this Worker's routing lease without sending application traffic.
                    if (!co_await gateway::GatewayService::renewSession(
                            *live->context, connectionIdentity(*live->session)
                        )) {
                        live->socket->abort();
                        break;
                    }
                    continue;
                }
                const auto idle = std::chrono::steady_clock::now() - live->session->lastInbound;
                if (idle >= idleTimeout) {
                    live->socket->abort();
                    break;
                }
                // 新固件主动心跳并接收确认；此任务仅检查超时，不主动发送应用 Ping。
                if (live->session->sparseHeartbeat) {
                    continue;
                }
                auto keepalive = makeEnvelope(*live->context, *live->session);
                keepalive.mutable_ping()->set_nonce(keepalive.sequence());
                enqueue(*live->session, keepalive);
                requestFlush(live);
            }
        } catch (const std::exception& error) {
            LOG_ERROR << "edge session maintenance failed: " << error.what();
            live->socket->abort();
        } catch (...) {
            LOG_ERROR << "edge session maintenance failed: unknown error";
            live->socket->abort();
        }
    }

    static ruvia::task<void> flushEgress(std::shared_ptr<LiveSession> live) {
        auto& c = *live->context;
        auto& socket = *live->socket;
        auto& session = *live->session;
        const auto queues = gateway::GatewayService::egressQueues(session.nodeId);
        std::exception_ptr failure;
        try {
            while (live->active && !live->scope->stop_requested() &&
                   live->flushPending) {
                live->flushPending = false;
                for (;;) {
                    int replies = 0;
                    while (!session.outbound.empty() && replies < 64) {
                        auto wire = std::move(session.outbound.front());
                        session.outbound.pop_front();
                        pb::Envelope envelope;
                        if (!protocol::decode(wire, envelope)) {
                            throw std::runtime_error("edge envelope decode failed");
                        }
                        co_await sendSessionEnvelope(c, socket, session, envelope);
                        ++replies;
                    }
                    const auto configs =
                        co_await drainQueue(c, socket, session, queues.configuration, 64);
                    if (configs != 0) {
                        session.configSentAtMs = service::message::utcNowMilliseconds();
                    }
                    const auto keystrokes =
                        co_await drainQueue(c, socket, session, queues.terminal, 64);
                    const auto serialRequests =
                        co_await drainQueue(c, socket, session, queues.serialDebug, 64);
                    const auto commands =
                        co_await drainQueue(c, socket, session, queues.command, 64);
                    const auto tasks =
                        co_await drainQueue(c, socket, session, queues.task, 64);
                    if (replies + configs + keystrokes + serialRequests + commands + tasks == 0) {
                        break;
                    }
                }
            }
        } catch (...) {
            failure = std::current_exception();
        }
        live->flushing = false;
        if (failure) {
            live->active = false;
            try {
                co_await socket.close(
                    ruvia::websocket_close_options{ .code_ = 1011,
                                                    .reason_ = "edge egress failed" }
                );
            } catch (...) {
                socket.abort();
            }
            std::rethrow_exception(failure);
        }
    }

    static ruvia::task<void> deliverQueuedItem(
        ruvia::context& c,
        ruvia::websocket& socket,
        Session& session,
        std::string_view key,
        gateway::QueuedEnvelope& item
    ) {
        auto& envelope = item.envelope;
        protocol::bindSession(
            envelope,
            protocol::bytes(session.platformBytes.data(), session.platformBytes.size()),
            protocol::bytes(session.nodeBytes.data(), session.nodeBytes.size()),
            session.epoch,
            ++session.outboundSequence,
            session.protocolVersion
        );
        std::exception_ptr failure;
        try {
            co_await sendSessionEnvelope(c, socket, session, envelope);
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            co_await gateway::GatewayService::returnUndelivered(c, key, item);
            std::rethrow_exception(failure);
        }
    }

    static ruvia::task<int> drainQueue(ruvia::context& c, ruvia::websocket& socket, Session& session, std::string_view key, int limit) {
        int sent = 0;
        for (int count = 0; count < limit; ++count) {
            auto item = co_await gateway::GatewayService::takeQueuedEnvelope(
                c, connectionIdentity(session), key, session.supportsCommandStartBefore
            );
            if (!item) {
                break;
            }
            if (!item->deliver) {
                continue;
            }
            co_await deliverQueuedItem(c, socket, session, key, *item);
            ++sent;
        }
        co_return sent;
    }

    static gateway::ConnectionIdentity connectionIdentity(const Session& session) {
        return { session.nodeId, session.epoch, session.protocolVersion, session.workerIndex };
    }

    static ruvia::task<void> handle(ruvia::context& c, Session& session, const pb::Envelope& input, bool freshTelemetry) {
        co_await gateway::GatewayService::acceptPayload(
            c, connectionIdentity(session), input, session.capabilitySeen, session.configSentAtMs
        );
        switch (input.payload_case()) {
            case pb::Envelope::kHeartbeat: {
                auto reply = makeEnvelope(c, session);
                auto* heartbeatAck = reply.mutable_heartbeat_ack();
                heartbeatAck->set_platform_time_ms(service::message::utcNowMilliseconds());
                if (input.heartbeat().has_nonce()) {
                    heartbeatAck->set_nonce(input.heartbeat().nonce());
                }
                heartbeatAck->set_request_capability_report(!session.capabilitySeen);
                heartbeatAck->set_request_device_status(false);
                // The ingress stream accepted this heartbeat before handle() is called.
                if (input.heartbeat().has_tcp_traffic()) {
                    heartbeatAck->set_traffic_sample_id(input.heartbeat().tcp_traffic().sample_id());
                }
                if (input.heartbeat().has_vpn_traffic()) {
                    heartbeatAck->set_vpn_traffic_sample_id(input.heartbeat().vpn_traffic().sample_id());
                }
                enqueue(session, reply);
                break;
            }
            case pb::Envelope::kCapabilityReport:
                session.capabilitySeen = true;
                break;
            case pb::Envelope::kTelemetryBatch: {
                // Legacy firmware cannot attach status to telemetry. Request it at
                // report time using the existing acknowledgement it understands.
                if (!session.sparseHeartbeat && freshTelemetry && std::any_of(input.telemetry_batch().records().begin(), input.telemetry_batch().records().end(), [](const auto& record) {
                        return !record.has_device_status() && !record.derived_update();
                    })) {
                    auto statusRequest = makeEnvelope(c, session);
                    statusRequest.mutable_heartbeat_ack()->set_request_device_status(true);
                    statusRequest.mutable_heartbeat_ack()->set_platform_time_ms(service::message::utcNowMilliseconds());
                    enqueue(session, statusRequest);
                }
                auto reply = makeEnvelope(c, session);
                auto* telemetryAck = reply.mutable_telemetry_ack();
                for (const auto& record : input.telemetry_batch().records()) {
                    if (record.record_id().size() != 16) {
                        continue;
                    }
                    telemetryAck->add_accepted_record_ids(record.record_id());
                    break;
                }
                enqueue(session, reply);
                break;
            }
            case pb::Envelope::kRawPacket: {
                const auto& packet = input.raw_packet();
                if (packet.packet_id().size() != 16) {
                    break;
                }
                auto reply = makeEnvelope(c, session);
                reply.mutable_raw_packet_ack()->set_packet_id(packet.packet_id());
                enqueue(session, reply);
                break;
            }
            case pb::Envelope::kCommandResult: {
                const auto& result = input.command_result();
                if (result.command_id().size() != 16) {
                    break;
                }
                auto reply = makeEnvelope(c, session);
                reply.mutable_command_result_ack()->set_command_id(result.command_id());
                enqueue(session, reply);
                break;
            }
            case pb::Envelope::kFirmwareChunkRequest:
                co_await sendFirmwareChunk(c, session, input.firmware_chunk_request());
                break;
            case pb::Envelope::kPing: {
                // Older nodes send zero-nonce application pings while a remote
                // terminal is open. They still use the pong as their application
                // liveness signal, so ignoring nonce zero forces a reconnect after
                // the negotiated watchdog interval and strands the browser terminal.
                auto reply = makeEnvelope(c, session);
                reply.mutable_pong()->set_nonce(input.ping().nonce());
                enqueue(session, reply);
                break;
            }
            default:
                break;
        }
    }

    static ruvia::task<void> sendFirmwareChunk(
        ruvia::context& c,
        Session& session,
        const pb::FirmwareChunkRequest& request
    ) {
        if (session.protocolVersion < 6 || request.request_id().size() != 16) {
            co_return;
        }
        auto chunk = co_await gateway::GatewayService::firmwareChunk(
            c, session.nodeId, session.firmwareTransfer, request
        );
        auto reply = makeEnvelope(c, session);
        *reply.mutable_firmware_chunk() = std::move(chunk);
        enqueue(session, reply);
    }
};

} // namespace service::edge
