#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/gateway/gateway.types.h"

namespace service::edge::gateway {

class GatewayProtocol final {
  public:
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
        std::chrono::steady_clock::time_point lastInbound{};
    };

    static Session makeSession(std::string nodeId, std::uint32_t protocolVersion, std::string_view platformId, std::size_t workerIndex, std::uint64_t epoch, std::chrono::steady_clock::time_point now) {
        Session result;
        result.nodeId = std::move(nodeId);
        result.protocolVersion = protocolVersion;
        result.workerIndex = workerIndex;
        service::common::uuidBytes(result.nodeId, result.nodeBytes.data());
        std::memcpy(result.platformBytes.data(), platformId.data(), result.platformBytes.size());
        result.epoch = epoch;
        result.lastInbound = now;
        return result;
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

    static TelemetryDecision telemetryDecision(Session& session, const pb::Envelope& input, std::uint64_t now) {
        if (input.payload_case() != pb::Envelope::kTelemetryBatch ||
            input.telemetry_batch().records().empty() ||
            input.telemetry_batch().records(0).record_id().size() != 16) {
            return {};
        }
        constexpr std::uint64_t retryAckIntervalMs = 1000;
        const auto recordId = input.telemetry_batch().records(0).record_id();
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

    static std::string serialize(const pb::Envelope& envelope) {
        auto wire = protocol::encode(envelope);
        if (wire.empty()) {
            throw std::runtime_error("edge envelope encode failed");
        }
        return wire;
    }

    static void enqueue(Session& session, const pb::Envelope& envelope) {
        session.outbound.emplace_back(serialize(envelope));
    }

    static std::optional<pb::Envelope> nextOutbound(Session& session) {
        if (session.outbound.empty()) {
            return std::nullopt;
        }
        auto wire = std::move(session.outbound.front());
        session.outbound.pop_front();
        pb::Envelope envelope;
        if (!protocol::decode(wire, envelope)) {
            throw std::runtime_error("edge envelope decode failed");
        }
        return envelope;
    }

    static Session approveSession(const Session& pending, std::string nodeId, std::uint64_t epoch, std::chrono::steady_clock::time_point now) {
        return makeSession(
            std::move(nodeId),
            pending.protocolVersion,
            std::string_view(reinterpret_cast<const char*>(pending.platformBytes.data()), pending.platformBytes.size()),
            pending.workerIndex,
            epoch,
            now
        );
    }

    static void bindQueuedEnvelope(Session& session, pb::Envelope& envelope) {
        protocol::bindSession(
            envelope,
            protocol::bytes(session.platformBytes.data(), session.platformBytes.size()),
            protocol::bytes(session.nodeBytes.data(), session.nodeBytes.size()),
            session.epoch,
            ++session.outboundSequence,
            session.protocolVersion
        );
    }

    static bool decodeHello(std::string_view wire, pb::Envelope& input) {
        return protocol::decode(wire, input) && validHello(input);
    }

    static bool decodePendingHeartbeat(std::string_view wire, pb::Envelope& input, const Session& session, bool supportsCommandStartBefore) {
        return protocol::decode(wire, input) && validPendingHeartbeat(input, session, supportsCommandStartBefore);
    }

    static bool decodeInbound(std::string_view wire, pb::Envelope& input, const Session& session) {
        return protocol::decode(wire, input) && validInbound(input, session);
    }

    static bool validHello(const pb::Envelope& input) {
        return protocol::supportsProtocolVersion(input.protocol_version()) &&
            input.payload_case() == pb::Envelope::kHello &&
            protocol::validSessionPlatformId(input.platform_id()) &&
            protocol::validImei(input.hello().imei());
    }

    static bool validPendingHeartbeat(const pb::Envelope& input, const Session& session, bool supportsCommandStartBefore) {
        return input.protocol_version() == session.protocolVersion &&
            input.payload_case() == pb::Envelope::kHeartbeat &&
            (!supportsCommandStartBefore || (input.heartbeat().has_nonce() && input.heartbeat().nonce() != 0)) &&
            input.node_id().empty() && input.session_epoch() == 0 &&
            input.sequence() > session.inboundSequence &&
            input.platform_id() == protocol::bytes(session.platformBytes.data(), session.platformBytes.size());
    }

    static void stampEnvelope(Session& session, pb::Envelope& envelope, std::string_view messageId, std::int64_t now) {
        std::uint8_t messageBytes[16]{};
        if (service::common::uuidBytes(messageId, messageBytes)) {
            envelope.set_message_id(protocol::bytes(messageBytes, sizeof(messageBytes)));
        }
        envelope.set_created_at_ms(now);
        envelope.set_protocol_version(session.protocolVersion);
        envelope.set_session_epoch(session.epoch);
        envelope.set_sequence(++session.outboundSequence);
        envelope.set_platform_id(protocol::bytes(session.platformBytes.data(), session.platformBytes.size()));
        std::uint8_t nodeBytes[16]{};
        if (service::common::uuidBytes(session.nodeId, nodeBytes)) {
            envelope.set_node_id(protocol::bytes(nodeBytes, sizeof(nodeBytes)));
        }
    }

    static void enrollmentPending(pb::Envelope& reply) {
        reply.mutable_enrollment_pending()->set_code("pending");
        reply.mutable_enrollment_pending()->set_message("registration pending approval");
    }

    static void helloAcknowledgement(const Session& session, const pb::Envelope& input, pb::Envelope& reply, std::int64_t now) {
        auto* ack = reply.mutable_hello_ack();
        ack->set_assigned_node_id(protocol::bytes(session.nodeBytes.data(), session.nodeBytes.size()));
        ack->set_session_epoch(session.epoch);
        ack->set_negotiated_protocol_version(session.protocolVersion);
        ack->set_raw_telemetry(input.hello().supports_raw_telemetry());
        // Keep the deployed wire field, including old firmware's 300s cadence.
        ack->set_heartbeat_interval_sec(session.statusReporting ? 900 : 300);
        ack->set_max_message_size(static_cast<std::uint32_t>(service::message::edge::kMaxMessageSize));
        ack->set_platform_time_ms(now);
    }

    static bool needsLegacyStatus(const Session& session, const pb::Envelope& input, bool freshTelemetry) {
        return input.has_telemetry_batch() && !session.sparseHeartbeat && freshTelemetry &&
            std::any_of(input.telemetry_batch().records().begin(), input.telemetry_batch().records().end(), [](const auto& record) {
                   return !record.has_device_status() && !record.derived_update();
               });
    }

    // Builds only the protocol payload. The driver stamps headers after this
    // returns true, so payloads with no response never consume a sequence number.
    static bool acknowledgePayload(Session& session, const pb::Envelope& input, pb::Envelope& reply, std::int64_t now) {
        switch (input.payload_case()) {
            case pb::Envelope::kHeartbeat: {
                auto* ack = reply.mutable_heartbeat_ack();
                ack->set_platform_time_ms(now);
                if (input.heartbeat().has_nonce()) {
                    ack->set_nonce(input.heartbeat().nonce());
                }
                ack->set_request_capability_report(!session.capabilitySeen);
                ack->set_request_device_status(false);
                if (input.heartbeat().has_tcp_traffic()) {
                    ack->set_traffic_sample_id(input.heartbeat().tcp_traffic().sample_id());
                }
                if (input.heartbeat().has_vpn_traffic()) {
                    ack->set_vpn_traffic_sample_id(input.heartbeat().vpn_traffic().sample_id());
                }
                return true;
            }
            case pb::Envelope::kCapabilityReport:
                session.capabilitySeen = true;
                return false;
            case pb::Envelope::kTelemetryBatch: {
                auto* ack = reply.mutable_telemetry_ack();
                for (const auto& record : input.telemetry_batch().records()) {
                    if (record.record_id().size() == 16) {
                        ack->add_accepted_record_ids(record.record_id());
                        break;
                    }
                }
                return true;
            }
            case pb::Envelope::kRawPacket:
                if (input.raw_packet().packet_id().size() != 16) {
                    return false;
                }
                reply.mutable_raw_packet_ack()->set_packet_id(input.raw_packet().packet_id());
                return true;
            case pb::Envelope::kCommandResult:
                if (input.command_result().command_id().size() != 16) {
                    return false;
                }
                reply.mutable_command_result_ack()->set_command_id(input.command_result().command_id());
                return true;
            case pb::Envelope::kPing:
                // Deployed terminal firmware also uses nonce zero for liveness.
                reply.mutable_pong()->set_nonce(input.ping().nonce());
                return true;
            default:
                return false;
        }
    }
};

} // namespace service::edge::gateway
