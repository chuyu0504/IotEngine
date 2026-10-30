#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory_resource>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "service/features/edge/gateway/gateway.entity.h"
#include "service/features/edge/gateway/gateway.protocol.h"
#include "service/features/edge/gateway/gateway.service.h"
#include "service/modules/edge_node/edge_node.service.h"

#include "tests/database.fixture.h"

int main() {
    try {
        const auto approved = service::edge::gateway::EnrollmentRecord::decode("node-1|approved");
        if (!approved || approved->nodeId != "node-1" || approved->status != "approved") {
            throw std::runtime_error("gateway enrollment record lost node or approval status");
        }
        if (service::edge::gateway::EnrollmentRecord::decode("node-1").has_value()) {
            throw std::runtime_error("gateway accepted an incomplete enrollment record");
        }
        const auto pending = service::edge::gateway::EnrollmentRecord::decode("node-1|pending");
        if (!pending || pending->status != "pending") {
            throw std::runtime_error("gateway enrollment record changed pending status");
        }
        {
            const service::edge::gateway::ConnectionIdentity session{ "node-1", 42, 6, 3 };
            service::edge::pb::Envelope envelope;
            envelope.mutable_serial_debug_request();
            envelope.set_session_epoch(session.epoch);
            envelope.set_protocol_version(session.protocolVersion);
            envelope.set_created_at_ms(100000);
            const auto current = [&](std::uint64_t now) {
                return service::edge::gateway::GatewayService::queuedEnvelopeIsCurrent(envelope, session, now);
            };
            if (!current(100000) || !current(115000) || current(115001) || current(99999)) {
                throw std::runtime_error("serial debug queue freshness boundary changed");
            }
            envelope.set_session_epoch(session.epoch + 1);
            if (current(100000)) {
                throw std::runtime_error("serial debug request from a replaced session was accepted");
            }
            envelope.set_session_epoch(session.epoch);
            envelope.set_protocol_version(session.protocolVersion - 1);
            if (current(100000)) {
                throw std::runtime_error("serial debug request from a different protocol session was accepted");
            }
            envelope.mutable_ping();
            if (!current(115001)) {
                throw std::runtime_error("ordinary queued task incorrectly acquired serial debug expiry");
            }
        }
        {
            auto session = service::edge::gateway::GatewayProtocol::makeSession(
                "00000000-0000-7000-8000-000000000002",
                6,
                std::string(16, '\x01'),
                3,
                42,
                std::chrono::steady_clock::time_point{}
            );
            service::edge::pb::Envelope input;
            input.set_protocol_version(6);
            input.set_platform_id(std::string(16, '\x01'));
            input.mutable_hello()->set_imei("490154203237518");
            if (!service::edge::gateway::GatewayProtocol::validHello(input)) {
                throw std::runtime_error("gateway rejected a valid deployed hello");
            }
            input.mutable_hello()->set_imei("490154203237519");
            if (service::edge::gateway::GatewayProtocol::validHello(input)) {
                throw std::runtime_error("gateway accepted an invalid IMEI");
            }
            input.mutable_heartbeat()->set_nonce(9);
            input.set_sequence(1);
            if (!service::edge::gateway::GatewayProtocol::validPendingHeartbeat(input, session, true)) {
                throw std::runtime_error("gateway rejected a correlated pending heartbeat");
            }
            input.mutable_heartbeat()->set_nonce(0);
            if (service::edge::gateway::GatewayProtocol::validPendingHeartbeat(input, session, true) ||
                !service::edge::gateway::GatewayProtocol::validPendingHeartbeat(input, session, false)) {
                throw std::runtime_error("gateway changed negotiated pending-heartbeat nonce rules");
            }
            input.Clear();
            auto* record = input.mutable_telemetry_batch()->add_records();
            record->set_record_id(std::string(16, '\x04'));
            const auto fresh = service::edge::gateway::GatewayProtocol::telemetryDecision(session, input, 1000);
            const auto duplicate = service::edge::gateway::GatewayProtocol::telemetryDecision(session, input, 1999);
            const auto retry = service::edge::gateway::GatewayProtocol::telemetryDecision(session, input, 2000);
            if (!fresh.publish || !fresh.acknowledge || duplicate.publish || duplicate.acknowledge ||
                retry.publish || !retry.acknowledge) {
                throw std::runtime_error("gateway telemetry duplicate/retry acknowledgement semantics changed");
            }
            service::edge::pb::Envelope reply;
            if (!service::edge::gateway::GatewayProtocol::acknowledgePayload(session, input, reply, 3000) ||
                reply.telemetry_ack().accepted_record_ids_size() != 1 ||
                reply.telemetry_ack().accepted_record_ids(0) != record->record_id()) {
                throw std::runtime_error("gateway lost the telemetry acknowledgement record identity");
            }
            service::edge::gateway::GatewayProtocol::stampEnvelope(
                session,
                reply,
                "00000000-0000-7000-8000-000000000003",
                3000
            );
            service::edge::gateway::GatewayProtocol::enqueue(session, reply);
            service::edge::pb::Envelope decoded;
            if (session.outbound.size() != 1 ||
                !service::edge::protocol::decode(session.outbound.front(), decoded) ||
                decoded.session_epoch() != 42 || decoded.sequence() != 1 ||
                decoded.protocol_version() != 6 || decoded.created_at_ms() != 3000 ||
                decoded.telemetry_ack().accepted_record_ids(0) != std::string(16, '\x04')) {
                throw std::runtime_error("gateway changed acknowledgement wire/session binding");
            }
            input.Clear();
            input.mutable_capability_report();
            reply.Clear();
            if (service::edge::gateway::GatewayProtocol::acknowledgePayload(session, input, reply, 4000) ||
                !session.capabilitySeen || session.outboundSequence != 1) {
                throw std::runtime_error("capability reporting consumed an acknowledgement sequence");
            }
            input.mutable_ping()->set_nonce(0);
            if (!service::edge::gateway::GatewayProtocol::acknowledgePayload(session, input, reply, 4000) ||
                !reply.has_pong() || reply.pong().nonce() != 0) {
                throw std::runtime_error("gateway no longer acknowledges legacy zero-nonce terminal pings");
            }
            session.statusReporting = true;
            input.mutable_hello()->set_supports_raw_telemetry(true);
            reply.Clear();
            service::edge::gateway::GatewayProtocol::helloAcknowledgement(session, input, reply, 5000);
            if (reply.hello_ack().heartbeat_interval_sec() != 900 ||
                !reply.hello_ack().raw_telemetry() || reply.hello_ack().negotiated_protocol_version() != 6 ||
                reply.hello_ack().session_epoch() != 42 ||
                reply.hello_ack().max_message_size() != service::message::edge::kMaxMessageSize) {
                throw std::runtime_error("gateway changed the deployed hello acknowledgement contract");
            }
            input.mutable_hello()->set_supports_external_io(true);
            input.mutable_hello()->set_supports_node_io(true);
            reply.Clear();
            service::edge::gateway::GatewayProtocol::helloAcknowledgement(session, input, reply, 5000);
            if (reply.hello_ack().external_io() || reply.hello_ack().node_io()) {
                throw std::runtime_error("gateway enabled the old external IO wire format");
            }
            input.mutable_hello()->set_supports_external_io_milliamps(true);
            reply.Clear();
            service::edge::gateway::GatewayProtocol::helloAcknowledgement(session, input, reply, 5000);
            if (!reply.hello_ack().external_io() || !reply.hello_ack().node_io()) {
                throw std::runtime_error("gateway failed to negotiate mA external IO");
            }
        }
        service::edge::pb::CommandRequest capableCommand;
        capableCommand.set_timeout_ms(5000);
        constexpr std::int64_t persistedDeadlineMs = 1780000000123;
        service::edge::gateway::GatewayService::applyCommandDeadline(
            capableCommand,
            true,
            persistedDeadlineMs
        );
        if (!capableCommand.has_start_before_ms() ||
            capableCommand.start_before_ms() != persistedDeadlineMs ||
            capableCommand.timeout_ms() != 5000) {
            throw std::runtime_error("negotiated command deadline did not preserve the persisted millisecond value");
        }
        service::edge::pb::CommandRequest legacyCommand;
        legacyCommand.set_timeout_ms(5000);
        service::edge::gateway::GatewayService::applyCommandDeadline(
            legacyCommand,
            false,
            persistedDeadlineMs
        );
        if (legacyCommand.has_start_before_ms() || legacyCommand.timeout_ms() != 5000) {
            throw std::runtime_error("legacy command wire behavior changed");
        }
        bool rejectedUnnegotiatedField = false;
        legacyCommand.set_start_before_ms(persistedDeadlineMs);
        try {
            service::edge::gateway::GatewayService::applyCommandDeadline(
                legacyCommand,
                false,
                persistedDeadlineMs
            );
        } catch (const std::invalid_argument&) {
            rejectedUnnegotiatedField = true;
        }
        if (!rejectedUnnegotiatedField) {
            throw std::runtime_error("legacy session accepted an unnegotiated command deadline field");
        }
        bool rejectedMissingDeadline = false;
        try {
            service::edge::pb::CommandRequest invalidCommand;
            service::edge::gateway::GatewayService::applyCommandDeadline(
                invalidCommand,
                true,
                std::nullopt
            );
        } catch (const std::invalid_argument&) {
            rejectedMissingDeadline = true;
        }
        if (!rejectedMissingDeadline) {
            throw std::runtime_error("negotiated command deadline silently proceeded without its database value");
        }
        {
            constexpr std::string_view nodeId = "00000000-0000-4000-8000-0000000000aa";
            const auto statement = test::compileDatabaseStatement([nodeId](auto& database) {
                return service::edge_node::EdgeService::pendingEnrollmentRemoval(database, std::pmr::new_delete_resource(), nodeId);
            });
            const auto& sql = statement.sql;
            const auto where = sql.find("WHERE");
            if (sql.find("DELETE FROM") == std::string::npos || sql.find("edge_node") == std::string::npos) {
                throw std::runtime_error("pending enrollment removal is not a DELETE on edge_node");
            }
            if (where == std::string::npos || sql.find("enrollment_status", where) == std::string::npos ||
                statement.parameterCount != 2 ||
                sql.find(" AND ", where) == std::string::npos) {
                throw std::runtime_error("pending enrollment removal does not restrict deletion to the pending node");
            }
            if (sql.find("RETURNING") == std::string::npos) {
                throw std::runtime_error("pending enrollment removal cannot detect a concurrent status change");
            }
        }
        std::cout << "edge service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "edge service test failed: " << error.what() << '\n';
        return 1;
    }
}
