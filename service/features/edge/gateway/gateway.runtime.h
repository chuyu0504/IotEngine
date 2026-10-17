#pragma once

#include <algorithm>
#include <chrono>
#include <exception>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/edge/gateway/gateway.protocol.h"
#include "service/features/edge/gateway/gateway.service.h"
#include "service/features/edge/gateway/gateway.transport.h"
#include "service/features/edge/gateway/gateway.types.h"
#include "service/middleware/log.h"
#include "service/utils/uuid.h"
#include "service/utils/worker_task.h"

namespace service::edge::gateway {

class GatewayRuntime final {
  private:
    struct LiveSession {
        ruvia::context* context{};
        transport::Connection* socket{};
        GatewayProtocol::Session* session{};
        ruvia::task_scope* scope{};
        bool active{ true };
        bool flushPending{};
        bool flushing{};
    };

  public:
    static ruvia::task<void> runConnection(ruvia::context& c, transport::Connection& socket) {
        const auto workerIndex = c.worker_state<std::optional<service::common::WorkerIdentity>>().value().index;
        auto first = co_await socket.read();
        if (!first || !first->binary()) {
            co_await socket.close(1002, "binary hello required");
            co_return;
        }
        pb::Envelope input;
        if (!GatewayProtocol::decodeHello(first->payload(), input)) {
            co_await socket.close(1002, "invalid hello");
            co_return;
        }
        const auto enrollment = co_await gateway::GatewayService::acceptHello(
            c,
            workerIndex,
            first->payload(),
            input.hello().imei()
        );
        if (!enrollment) {
            co_await socket.close(1008, "worker lease unavailable");
            co_return;
        }
        const std::string imei(input.hello().imei());
        const bool sparseHeartbeat = input.hello().supports_sparse_heartbeat();
        const bool statusReporting = input.hello().supports_status_reporting();
        const bool supportsCommandStartBefore = input.hello().supports_command_start_before();
        std::string nodeId = enrollment->nodeId;
        bool approved = enrollment->approved;
        auto session = GatewayProtocol::makeSession(
            nodeId,
            input.protocol_version(),
            input.platform_id(),
            workerIndex,
            GatewayService::newSessionEpoch(),
            std::chrono::steady_clock::now()
        );
        if (!approved) {
            co_await sendEnrollment(c, socket, session);

            session.inboundSequence = input.sequence();
            while (auto message = co_await socket.read()) {
                if (!message->binary() ||
                    !GatewayProtocol::decodePendingHeartbeat(message->payload(), input, session, supportsCommandStartBefore)) {
                    co_await socket.close(1002, "invalid pending heartbeat");
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
                        co_await socket.close(1011, "database time sample unavailable");
                        co_return;
                    }
                }
                co_await send(socket, heartbeat);
            }
            if (!approved) {
                co_return;
            }
            session = GatewayProtocol::approveSession(
                session,
                nodeId,
                GatewayService::newSessionEpoch(),
                std::chrono::steady_clock::now()
            );
        }

        session.sparseHeartbeat = sparseHeartbeat;
        session.statusReporting = statusReporting;
        session.supportsCommandStartBefore = supportsCommandStartBefore;
        const auto opened = co_await gateway::GatewayService::openSession(c, connectionIdentity(session));
        if (opened != gateway::IngressStatus::Accepted) {
            co_await socket.close(
                1008,
                opened == gateway::IngressStatus::SessionReplaced
                    ? "session replaced"
                    : "worker lease lost"
            );
            co_return;
        }
        auto ack = makeEnvelope(c, session);
        GatewayProtocol::helloAcknowledgement(session, input, ack, service::message::utcNowMilliseconds());
        auto* helloAck = ack.mutable_hello_ack();
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
                co_await socket.close(1011, "database time sample unavailable");
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
        try {
            std::weak_ptr<LiveSession> weak = live;
            gateway::GatewayService::ConnectionRegistration registration(
                c,
                connectionIdentity(session),
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
                        // The business registration invokes this callback only
                        // on the same Service Worker that accepted the socket.
                        current->socket->abort();
                    }
                }
            );
            egressScope.spawn(maintainSession(live));
            // Reliable per-node queues may already contain work from while the
            // node was offline; connection establishment is itself a wakeup.
            requestFlush(live);
            while (auto message = co_await socket.read()) {
                if (!message->binary() || !GatewayProtocol::decodeInbound(message->payload(), input, session)) {
                    co_await socket.close(1002, "invalid envelope");
                    break;
                }
                const auto telemetry = GatewayProtocol::telemetryDecision(session, input, service::message::utcNowMilliseconds());
                const auto accepted = co_await gateway::GatewayService::acceptIngress(
                    c,
                    connectionIdentity(session),
                    input,
                    message->payload(),
                    telemetry.publish
                );
                if (accepted != gateway::IngressStatus::Accepted) {
                    co_await socket.close(
                        1008,
                        accepted == gateway::IngressStatus::SessionReplaced
                            ? "session replaced"
                            : "worker lease lost"
                    );
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

  private:
    static ruvia::task<void> sendEnrollment(ruvia::context& c, transport::Connection& socket, GatewayProtocol::Session& session) {
        auto reply = makeEnvelope(c, session);
        GatewayProtocol::enrollmentPending(reply);
        co_await send(socket, reply);
    }

    static pb::Envelope makeEnvelope(ruvia::context& c, GatewayProtocol::Session& session) {
        pb::Envelope result;
        GatewayProtocol::stampEnvelope(
            session,
            result,
            service::utils::nextUuid(c.worker_state<service::common::UuidV7State>()),
            service::message::utcNowMilliseconds()
        );
        return result;
    }

    static ruvia::task<void> send(transport::Connection& socket, const pb::Envelope& envelope) {
        const auto wire = GatewayProtocol::serialize(envelope);
        co_await socket.write(wire);
    }

    static ruvia::task<void> sendSessionEnvelope(
        ruvia::context& c,
        transport::Connection& socket,
        GatewayProtocol::Session& session,
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
                            *live->context,
                            connectionIdentity(*live->session)
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
                GatewayProtocol::enqueue(*live->session, keepalive);
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
                    while (replies < 64) {
                        auto envelope = GatewayProtocol::nextOutbound(session);
                        if (!envelope) {
                            break;
                        }
                        co_await sendSessionEnvelope(c, socket, session, *envelope);
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
                co_await socket.close(1011, "edge egress failed");
            } catch (...) {
                socket.abort();
            }
            std::rethrow_exception(failure);
        }
    }

    static ruvia::task<void> deliverQueuedItem(
        ruvia::context& c,
        transport::Connection& socket,
        GatewayProtocol::Session& session,
        std::string_view key,
        gateway::QueuedEnvelope& item
    ) {
        auto& envelope = item.envelope;
        GatewayProtocol::bindQueuedEnvelope(session, envelope);
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

    static ruvia::task<int> drainQueue(ruvia::context& c, transport::Connection& socket, GatewayProtocol::Session& session, std::string_view key, int limit) {
        int sent = 0;
        for (int count = 0; count < limit; ++count) {
            auto item = co_await gateway::GatewayService::takeQueuedEnvelope(
                c,
                connectionIdentity(session),
                key,
                session.supportsCommandStartBefore
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

    static gateway::ConnectionIdentity connectionIdentity(const GatewayProtocol::Session& session) {
        return { session.nodeId, session.epoch, session.protocolVersion, session.workerIndex };
    }

    static ruvia::task<void> handle(ruvia::context& c, GatewayProtocol::Session& session, const pb::Envelope& input, bool freshTelemetry) {
        co_await gateway::GatewayService::acceptPayload(
            c,
            connectionIdentity(session),
            input,
            session.capabilitySeen,
            session.configSentAtMs
        );
        if (input.has_firmware_chunk_request()) {
            co_await sendFirmwareChunk(c, session, input.firmware_chunk_request());
            co_return;
        }
        if (GatewayProtocol::needsLegacyStatus(session, input, freshTelemetry)) {
            auto statusRequest = makeEnvelope(c, session);
            statusRequest.mutable_heartbeat_ack()->set_request_device_status(true);
            statusRequest.mutable_heartbeat_ack()->set_platform_time_ms(service::message::utcNowMilliseconds());
            GatewayProtocol::enqueue(session, statusRequest);
        }
        pb::Envelope reply;
        if (GatewayProtocol::acknowledgePayload(session, input, reply, service::message::utcNowMilliseconds())) {
            GatewayProtocol::stampEnvelope(
                session,
                reply,
                service::utils::nextUuid(c.worker_state<service::common::UuidV7State>()),
                service::message::utcNowMilliseconds()
            );
            GatewayProtocol::enqueue(session, reply);
        }
    }

    static ruvia::task<void> sendFirmwareChunk(
        ruvia::context& c,
        GatewayProtocol::Session& session,
        const pb::FirmwareChunkRequest& request
    ) {
        if (session.protocolVersion < 6 || request.request_id().size() != 16) {
            co_return;
        }
        auto chunk = co_await gateway::GatewayService::firmwareChunk(
            c,
            session.nodeId,
            session.firmwareTransfer,
            request
        );
        auto reply = makeEnvelope(c, session);
        *reply.mutable_firmware_chunk() = std::move(chunk);
        GatewayProtocol::enqueue(session, reply);
    }
};

// Firmware storage cleanup driver, one instance per Service Worker. It sleeps
// until the earliest deadline computed by GatewayService (no deadline: until an
// event), then performs the cleanup only if it wins the storage claim. The
// driver, its wakeup source and the RPC handler all run on the owning Worker.
class FirmwareCleanupRuntime final {
  public:
    explicit FirmwareCleanupRuntime(std::filesystem::path directory)
        : directory_(std::filesystem::absolute(std::move(directory)).lexically_normal()) {}

    FirmwareCleanupRuntime(const FirmwareCleanupRuntime&) = delete;
    FirmwareCleanupRuntime& operator=(const FirmwareCleanupRuntime&) = delete;

    ~FirmwareCleanupRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (stop_) {
            return;
        }
        stop_ = std::make_unique<ruvia::stop_source>();
        try {
            service::utils::startWorkerTask(
                worker,
                stop_->token(),
                stopped_,
                [this](ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
                    return run(context, ready, stop);
                }
            );
        } catch (...) {
            stop_.reset();
            throw;
        }
    }

    void stop() {
        if (!stop_) {
            return;
        }
        service::utils::stopWorkerTask(*stop_, stopped_);
        stop_.reset();
    }

  private:
    // Backoff after a lost claim, an empty claimed run or a failure, before the
    // deadline is recomputed.
    static constexpr auto kRetry = std::chrono::seconds(5);

    ruvia::task<void> run(ruvia::web_worker_context& context, service::utils::WorkerReady& ready, ruvia::stop_token stop) {
        auto& signal = context.worker_state<FirmwareCleanupSignal>();
        signal.bind([this] {
            if (wake_) {
                wake_->request_stop();
            }
        });
        ready.set_value();
        try {
            std::optional<std::chrono::steady_clock::time_point> due;
            bool recompute = true;
            while (!stop.stop_requested()) {
                if (signal.consume() || recompute) {
                    recompute = false;
                    bool scheduled = false;
                    try {
                        const auto delay = co_await GatewayService::nextFirmwareCleanupDelay(context, directory_);
                        due = delay ? std::optional(std::chrono::steady_clock::now() + *delay) : std::nullopt;
                        scheduled = true;
                    } catch (const std::exception& error) {
                        LOG_ERROR << "firmware cleanup schedule failed: " << error.what();
                    }
                    if (!scheduled) {
                        recompute = true;
                        (void)co_await ruvia::sleep_for(context.worker(), kRetry, stop);
                        continue;
                    }
                    if (signal.pending()) {
                        continue;
                    }
                }
                wake_ = std::make_unique<ruvia::stop_source>();
                const auto wait = due ? std::max(*due - std::chrono::steady_clock::now(), std::chrono::steady_clock::duration::zero())
                                      : std::chrono::steady_clock::duration::max();
                (void)co_await ruvia::sleep_for(context.worker(), wait, ruvia::combine_stop_tokens(stop, wake_->token()));
                wake_.reset();
                if (stop.stop_requested()) {
                    break;
                }
                if (signal.pending() || !due || std::chrono::steady_clock::now() < *due) {
                    continue;
                }
                recompute = true;
                bool backoff = true;
                try {
                    const auto result = co_await GatewayService::cleanupFirmwares(context, directory_);
                    // A claimed run that removed nothing means the deadline is not
                    // actually collectable yet; never spin on it.
                    backoff = !result.claimed || result.removedRecords + result.removedFiles == 0;
                } catch (const std::exception& error) {
                    LOG_ERROR << "firmware cleanup failed: " << error.what();
                }
                if (backoff) {
                    (void)co_await ruvia::sleep_for(context.worker(), kRetry, stop);
                }
            }
        } catch (const std::exception& error) {
            LOG_ERROR << "firmware cleanup stopped: " << error.what();
        } catch (...) {
            LOG_ERROR << "firmware cleanup stopped: unknown error";
        }
        signal.bind({});
        wake_.reset();
    }

    const std::filesystem::path directory_;
    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
    std::unique_ptr<ruvia::stop_source> wake_;
};

} // namespace service::edge::gateway
