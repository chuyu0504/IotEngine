#include "service/features/edge/edge.config.h"
#include "service/common/uuid.h"
#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>

#include "service/features/edge/edge.protocol.h"
#include <terminal.pb.h>

namespace {

void require(bool condition, std::string_view message) {
    if (condition)
        return;
    std::cerr << "edge protocol test failed: " << message << '\n';
    std::exit(1);
}

void testNetworkNanopbWireContract() {
    service::edge::pb::NetworkConfigRequest request;
    const std::array<std::uint8_t, 16> requestId{
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    };
    request.set_request_id(
        service::edge::protocol::bytes(requestId.data(), requestId.size()));
    auto* interface = request.add_interfaces();
    interface->set_name("eth0");
    interface->set_mode(service::edge::pb::NETWORK_ADDRESS_DHCP);
    interface->set_operation(service::edge::pb::NETWORK_CONFIG_UPSERT);
    interface->set_logical_name("lan");
    interface->set_device("eth0");
    interface->set_previous_logical_name("old");
    request.set_rollback_timeout_sec(30);

    std::string wire;
    require(request.SerializeToString(&wire), "network request serialization failed");
    const std::array<std::uint8_t, 48> nanopbWire{
        0x0a, 0x10, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x12, 0x1a, 0x0a, 0x04,
        0x65, 0x74, 0x68, 0x30, 0x10, 0x01, 0x48, 0x01, 0x52, 0x03, 0x6c,
        0x61, 0x6e, 0x5a, 0x04, 0x65, 0x74, 0x68, 0x30, 0x62, 0x03, 0x6f,
        0x6c, 0x64, 0x18, 0x1e,
    };
    require(wire == std::string_view(reinterpret_cast<const char*>(nanopbWire.data()),
                                     nanopbWire.size()),
            "C++ Protobuf wire differs from the nanopb golden vector");
}

void testConfigItemNanopbWireContract() {
    service::edge::pb::ConfigItem item;
    item.set_revision(7);
    item.set_kind(service::edge::pb::CONFIG_ITEM_ENDPOINT);
    auto* endpoint = item.mutable_endpoint();
    const std::array<std::uint8_t, 16> id{
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    };
    endpoint->set_endpoint_id(service::edge::protocol::bytes(id.data(), id.size()));
    endpoint->set_name("x");
    endpoint->set_transport(service::edge::pb::TRANSPORT_ETHERNET);
    endpoint->set_mode(service::edge::pb::LINK_MODE_TCP_CLIENT);
    endpoint->set_protocol(service::edge::pb::PROTOCOL_MODBUS);
    endpoint->set_ip("1.2.3.4");
    endpoint->set_port(502);
    endpoint->set_enabled(true);
    endpoint->set_interface_name("eth0");

    std::string wire;
    require(item.SerializeToString(&wire), "config item serialization failed");
    const std::array<std::uint8_t, 53> nanopbWire{
        0x08, 0x07, 0x18, 0x01, 0x52, 0x2f, 0x0a, 0x10, 0x00, 0x01, 0x02,
        0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
        0x0e, 0x0f, 0x12, 0x01, 0x78, 0x18, 0x01, 0x20, 0x02, 0x28, 0x02,
        0x32, 0x07, 0x31, 0x2e, 0x32, 0x2e, 0x33, 0x2e, 0x34, 0x38, 0xf6,
        0x03, 0x48, 0x01, 0x52, 0x04, 0x65, 0x74, 0x68, 0x30,
    };
    require(wire == std::string_view(reinterpret_cast<const char*>(nanopbWire.data()),
                                     nanopbWire.size()),
            "config item wire differs from the nanopb digest contract");
}

void testEnvelopeRoundTrip() {
    service::common::UuidV7Generator uuidGenerator;
    auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000002", 42, 7);
    envelope.mutable_ping()->set_nonce(1234);
    const auto wire = service::edge::protocol::encode(envelope);
    require(!wire.empty(), "envelope serialization failed");

    service::edge::pb::Envelope decoded;
    require(service::edge::protocol::decode(wire, decoded), "envelope parse failed");
    require(decoded.protocol_version() == service::edge::protocol::kProtocolVersion,
            "protocol version changed");
    require(decoded.session_epoch() == 42 && decoded.sequence() == 7,
            "session identity changed");
    require(decoded.payload_case() == service::edge::pb::Envelope::kPing &&
                decoded.ping().nonce() == 1234,
            "oneof payload changed");

    std::string oversized(service::edge::protocol::kMaxMessageSize + 1, '\0');
    require(!service::edge::protocol::decode(oversized, decoded),
            "oversized envelope was accepted");
}

void testCompatibleProtocolVersionContract() {
    service::common::UuidV7Generator uuidGenerator;
    require(service::edge::protocol::supportsProtocolVersion(2) &&
                service::edge::protocol::supportsProtocolVersion(3) &&
                service::edge::protocol::supportsProtocolVersion(4) &&
                service::edge::protocol::supportsProtocolVersion(5) &&
                service::edge::protocol::supportsProtocolVersion(6),
            "supported edgenode protocol range changed");
    require(!service::edge::protocol::supportsProtocolVersion(1) &&
                !service::edge::protocol::supportsProtocolVersion(7),
            "unsupported protocol version was accepted");
    require(service::edge::protocol::kProtocolVersion == 6,
            "firmware streaming did not advance the wire protocol");

    auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000002");
    envelope.mutable_heartbeat_ack()->set_platform_time_ms(1234);
    const std::string platform(16, '\x01');
    const std::string node(16, '\x02');
    envelope.set_protocol_version(2);
    service::edge::protocol::bindSession(envelope, platform, node, 42, 7);
    const auto wire = service::edge::protocol::encode(envelope);
    require(!wire.empty(), "strict protocol envelope serialization failed");

    service::edge::pb::Envelope decoded;
    require(service::edge::protocol::decode(wire, decoded),
            "strict protocol envelope parse failed");
    require(decoded.protocol_version() == service::edge::protocol::kProtocolVersion,
            "session binding preserved a stale protocol version");
    require(decoded.platform_id() == platform && decoded.node_id() == node &&
                decoded.session_epoch() == 42 && decoded.sequence() == 7,
            "strict protocol session binding changed identity");
    require(decoded.payload_case() == service::edge::pb::Envelope::kHeartbeatAck &&
                decoded.heartbeat_ack().platform_time_ms() == 1234 &&
                !decoded.heartbeat_ack().has_nonce() &&
                !decoded.heartbeat_ack().has_database_time_ms(),
            "strict protocol session binding changed legacy payload");
    auto* sampledAck = decoded.mutable_heartbeat_ack();
    sampledAck->set_nonce(123456789);
    sampledAck->set_database_time_ms(1700000000123);
    service::edge::pb::Envelope sampledDecoded;
    require(service::edge::protocol::decode(service::edge::protocol::encode(decoded), sampledDecoded) &&
                sampledDecoded.heartbeat_ack().has_nonce() &&
                sampledDecoded.heartbeat_ack().nonce() == 123456789 &&
                sampledDecoded.heartbeat_ack().has_database_time_ms() &&
                sampledDecoded.heartbeat_ack().database_time_ms() == 1700000000123,
            "correlated PostgreSQL time sample did not survive wire serialization");

    service::edge::protocol::bindSession(envelope, platform, node, 43, 8, 3);
    require(envelope.protocol_version() == 3 && envelope.session_epoch() == 43 &&
                envelope.sequence() == 8,
            "legacy session binding did not preserve the negotiated protocol");
}

void testTerminalOpenedContract() {
    service::common::UuidV7Generator uuidGenerator;
    auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000002", 42, 7);
    const std::string terminalId(16, '\x03');
    envelope.mutable_terminal_opened()->set_terminal_id(terminalId);
    const auto wire = service::edge::protocol::encode(envelope);
    require(!wire.empty(), "terminal opened acknowledgement was not encoded");

    service::edge::pb::Envelope decoded;
    require(service::edge::protocol::decode(wire, decoded),
            "terminal opened acknowledgement was not decoded");
    require(decoded.payload_case() == service::edge::pb::Envelope::kTerminalOpened &&
                decoded.terminal_opened().terminal_id() == terminalId,
            "terminal opened acknowledgement changed identity");
}

void testTerminalFlowControlContract() {
    service::common::UuidV7Generator uuidGenerator;
    auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000002", 42, 8);
    const std::string terminalId(16, '\x04');
    auto* data = envelope.mutable_terminal_data();
    data->set_terminal_id(terminalId);
    data->set_data("input");
    data->set_sequence(9);
    const auto wire = service::edge::protocol::encode(envelope);
    require(!wire.empty(), "sequenced terminal data was not encoded");
    service::edge::pb::Envelope decoded;
    require(service::edge::protocol::decode(wire, decoded) &&
                decoded.terminal_data().sequence() == 9,
            "terminal data sequence did not round trip");

    auto ack = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000002", 42, 9);
    ack.mutable_terminal_data_ack()->set_terminal_id(terminalId);
    ack.mutable_terminal_data_ack()->set_sequence(9);
    const auto ackWire = service::edge::protocol::encode(ack);
    require(service::edge::protocol::decode(ackWire, decoded) &&
                decoded.payload_case() ==
                    service::edge::pb::Envelope::kTerminalDataAck &&
                decoded.terminal_data_ack().sequence() == 9,
            "terminal data acknowledgement did not round trip");
}

void testVpnConfigContract() {
    service::common::UuidV7Generator uuidGenerator;
    auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000002", 42, 10);
    auto* request = envelope.mutable_vpn_config_request();
    const std::string requestId(16, '\x06');
    request->set_request_id(requestId);
    request->set_config_version(7);
    request->set_hub_public_key("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq=");
    request->set_hub_endpoint("vpn.example.com");
    request->set_hub_listen_port(51820);
    request->set_edge_address("100.96.0.10/32");
    request->set_enabled(true);
    auto* route = request->add_routes();
    route->set_route_id("00000000-0000-7000-8000-000000000003");
    route->set_virtual_cidr("172.31.10.0/24");
    route->set_target_cidr("192.168.10.0/24");
    route->set_mode("nat");
    route->set_nat_mode("masquerade");
    route->set_enabled(true);

    const auto wire = service::edge::protocol::encode(envelope);
    require(!wire.empty(), "VPN config request was not encoded");
    service::edge::pb::Envelope decoded;
    require(service::edge::protocol::decode(wire, decoded),
            "VPN config request was not decoded");
    require(decoded.payload_case() == service::edge::pb::Envelope::kVpnConfigRequest &&
                decoded.vpn_config_request().config_version() == 7 &&
                decoded.vpn_config_request().enabled() &&
                decoded.vpn_config_request().routes_size() == 1 &&
                decoded.vpn_config_request().routes(0).virtual_cidr() == "172.31.10.0/24",
            "VPN config request changed during round trip");
}

void testLegacyEdgenodeContract() {
    service::common::UuidV7Generator uuidGenerator;
    constexpr std::uint32_t legacyVersion = 3;
    const std::string nodeId = "00000000-0000-7000-8000-000000000002";
    const std::string platform(16, '\x01');
    const std::string node(16, '\x02');
    const std::string terminalId(16, '\x05');

    auto helloAck = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId, nodeId, 77, 1, legacyVersion);
    auto* negotiated = helloAck.mutable_hello_ack();
    negotiated->set_assigned_node_id(node);
    negotiated->set_session_epoch(77);
    negotiated->set_negotiated_protocol_version(legacyVersion);
    service::edge::protocol::bindSession(helloAck, platform, node, 77, 1,
                                         legacyVersion);
    auto wire = service::edge::protocol::encode(helloAck);
    service::edge::pb::Envelope decoded;
    require(service::edge::protocol::decode(wire, decoded) &&
                decoded.protocol_version() == legacyVersion &&
                decoded.hello_ack().negotiated_protocol_version() == legacyVersion,
            "legacy hello negotiation did not round trip");

    auto open = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId, nodeId, 77, 2, legacyVersion);
    auto* terminalOpen = open.mutable_terminal_open();
    terminalOpen->set_terminal_id(terminalId);
    terminalOpen->set_ticket("legacy-browser-ticket");
    terminalOpen->set_columns(120);
    terminalOpen->set_rows(30);
    wire = service::edge::protocol::encode(open);
    require(service::edge::protocol::decode(wire, decoded) &&
                decoded.protocol_version() == legacyVersion &&
                decoded.terminal_open().ticket().size() >= 16,
            "legacy terminal ticket contract was not preserved");

    auto data = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId, nodeId, 77, 3, legacyVersion);
    data.mutable_terminal_data()->set_terminal_id(terminalId);
    data.mutable_terminal_data()->set_data("legacy input");
    wire = service::edge::protocol::encode(data);
    require(service::edge::protocol::decode(wire, decoded) &&
                decoded.terminal_data().sequence() == 0,
            "legacy unsequenced terminal data contract changed");
}

void testTelemetryRawFrameArray() {
    service::edge::pb::TelemetryRecord input;
    input.set_record_id(std::string(16, '\x01'));
    input.set_device_id(std::string(16, '\x02'));
    input.set_observed_at_ms(123);
    input.add_raw_payloads(std::string("\x01\x03", 2));
    input.add_raw_packet_ids(std::string(16, '\x03'));
    service::edge::pb::TelemetryRecord decoded;
    require(decoded.ParseFromString(input.SerializeAsString()), "telemetry array wire was rejected");
    require(decoded.record_id() == input.record_id() && decoded.raw_payloads_size() == 1 &&
        decoded.raw_payloads(0) == input.raw_payloads(0) && decoded.raw_packet_ids(0) == input.raw_packet_ids(0),
        "telemetry wire lost acquisition or packet identity");
}

void testPlatformIdConfiguration() {
    service::common::UuidV7Generator uuidGenerator;
    constexpr std::string_view secondaryPlatformId{
        "00000000-0000-7000-8000-000000000002"};
    require(!service::edge::config::validPlatformId("invalid"),
            "invalid platform id was accepted");
    require(!service::edge::config::validPlatformId(
                "00000000-0000-0000-0000-000000000000"),
            "nil platform id was accepted");
    require(service::edge::config::validPlatformId(secondaryPlatformId),
            "valid platform id was rejected");
    const service::edge::config::PlatformIdentity secondary{std::string(secondaryPlatformId)};
    const service::edge::config::PlatformIdentity primary{
        std::string(service::message::edge::kDefaultPlatformId)};
    require(secondary.id == secondaryPlatformId && primary.id != secondary.id,
            "platform configurations do not retain independent identities");
    std::uint8_t expected[16]{};
    require(service::edge::protocol::uuidBytes(secondary.id, expected),
            "test platform id is invalid");
    const auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), secondaryPlatformId,
        "00000000-0000-7000-8000-000000000003");
    require(envelope.platform_id() == service::edge::protocol::bytes(expected, 16),
            "outbound envelope ignored the configured platform id");
    require(service::edge::config::validPlatformId(
                service::message::edge::kDefaultPlatformId),
            "default platform id is invalid");
    const auto defaultEnvelope = service::edge::protocol::outbound(
        uuidGenerator.next(), service::message::utcNowMilliseconds(),
        service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000003");
    require(defaultEnvelope.platform_id() != envelope.platform_id(),
            "explicit platform identities affected another envelope");
}

void testSessionPlatformIdentityIsInternal() {
    require(!service::edge::protocol::validSessionPlatformId({}),
            "empty session platform id was accepted");
    require(!service::edge::protocol::validSessionPlatformId(std::string(16, '\0')),
            "zero session platform id was accepted");
    require(service::edge::protocol::validSessionPlatformId(std::string(16, '\1')),
            "generated session platform id was rejected");
}

void testModemProfileRoundTrip() {
    service::common::UuidV7Generator uuidGenerator;
    auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000002");
    auto* request = envelope.mutable_modem_control_request();
    request->set_request_id(std::string(16, '\1'));
    request->set_action(service::edge::pb::MODEM_CONTROL_APPLY_PROFILE);
    request->set_apn("private.mnc001.mcc460.gprs");
    request->set_automatic_apn(false);
    request->set_username("edge-user");
    request->set_password("secret");
    request->set_pdp_type(service::edge::pb::MODEM_PDP_IPV4V6);
    request->set_auth_type(service::edge::pb::MODEM_AUTH_PAP_OR_CHAP);
    request->set_pin_code("1234");
    request->set_redial_after_apply(true);

    const auto wire = service::edge::protocol::encode(envelope);
    require(!wire.empty(), "modem profile serialization failed");
    service::edge::pb::Envelope decoded;
    require(service::edge::protocol::decode(wire, decoded),
            "modem profile parse failed");
    const auto& profile = decoded.modem_control_request();
    require(profile.action() == service::edge::pb::MODEM_CONTROL_APPLY_PROFILE &&
                profile.apn() == "private.mnc001.mcc460.gprs" &&
                !profile.automatic_apn() && profile.username() == "edge-user" &&
                profile.password() == "secret" &&
                profile.pdp_type() == service::edge::pb::MODEM_PDP_IPV4V6 &&
                profile.auth_type() == service::edge::pb::MODEM_AUTH_PAP_OR_CHAP &&
                profile.pin_code() == "1234" && profile.redial_after_apply(),
            "modem profile fields did not round-trip");

    request->set_apn(std::string(101, 'a'));
    require(service::edge::protocol::encode(envelope).empty(),
            "oversized APN escaped the nanopb wire limit");
}

void testNanopbBounds() {
    service::common::UuidV7Generator uuidGenerator;
    auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000002");
    auto* request = envelope.mutable_network_config_request();
    request->set_request_id(std::string(16, '\1'));
    request->add_interfaces()->set_logical_name(std::string(33, 'x'));
    require(service::edge::protocol::encode(envelope).empty(),
            "nanopb max_length annotation was ignored");

    std::string uncheckedWire;
    require(envelope.SerializeToString(&uncheckedWire),
            "unchecked envelope serialization failed");
    service::edge::pb::Envelope decoded;
    require(!service::edge::protocol::decode(uncheckedWire, decoded),
            "nanopb-incompatible envelope was accepted");
    require(decoded.payload_case() == service::edge::pb::Envelope::PAYLOAD_NOT_SET,
            "failed decode retained an invalid payload");
}

void testWebTerminalProtobuf() {
    ::iot::edge::terminal::v1::WebTerminalFrame frame;
    frame.mutable_resize()->set_columns(120);
    frame.mutable_resize()->set_rows(30);

    std::string wire;
    require(frame.SerializeToString(&wire), "web terminal frame serialization failed");
    const std::array<std::uint8_t, 6> expected{
        0x1a, 0x04, 0x08, 0x78, 0x10, 0x1e,
    };
    require(wire == std::string_view(reinterpret_cast<const char*>(expected.data()),
                                     expected.size()),
            "web terminal frame differs from the browser golden vector");

    ::iot::edge::terminal::v1::WebTerminalFrame decoded;
    require(decoded.ParseFromString(wire), "web terminal frame parse failed");
    require(decoded.payload_case() ==
                ::iot::edge::terminal::v1::WebTerminalFrame::kResize &&
                decoded.resize().columns() == 120 && decoded.resize().rows() == 30,
            "web terminal resize payload changed");
}

void testLegacyDebugAcquisitionIdentity() {
    service::edge::pb::RawPacket packet;
    packet.set_packet_id(std::string(16, '\x03'));
    const auto legacy = service::edge::protocol::debugAcquisitionId("node-a", packet);
    require(legacy.starts_with("legacy:node-a:"), "legacy debug packet has no fallback identity");
    require(legacy == service::edge::protocol::debugAcquisitionId("node-a", packet),
            "legacy replay changed debug identity");
    require(legacy != service::edge::protocol::debugAcquisitionId("node-b", packet),
            "legacy packets from different nodes collided");
    packet.set_acquisition_id(std::string(16, '\x04'));
    require(service::edge::protocol::debugAcquisitionId("node-a", packet) ==
                service::edge::protocol::uuidText(packet.acquisition_id()),
            "current firmware acquisition identity changed");
    packet.set_acquisition_id("malformed");
    bool rejected = false;
    try { (void)service::edge::protocol::debugAcquisitionId("node-a", packet); }
    catch (const std::invalid_argument&) { rejected = true; }
    require(rejected, "malformed nonempty acquisition ID was accepted as legacy");
}

void testCommandResultProjectionState() {
    using State = service::edge::pb::CommandState;
    const std::array lostAckResults{
        std::pair{"FINS", State::COMMAND_STATE_DEVICE_OFFLINE},
        std::pair{"S7", State::COMMAND_STATE_FAILED},
        std::pair{"Modbus", State::COMMAND_STATE_FAILED},
    };
    for (const auto& [protocol, state] : lostAckResults) {
        require(service::edge::protocol::commandResultDomainState(state, true, "") == "UNKNOWN",
                protocol);
    }
    require(service::edge::protocol::commandResultDomainState(
                State::COMMAND_STATE_SUCCEEDED, true, "") == "UNKNOWN",
            "missing write acknowledgement was overridden by a success state");
    require(service::edge::protocol::commandResultDomainState(
                State::COMMAND_STATE_SUCCEEDED, false, "readback verified") == "SUCCEEDED",
            "verified success was misclassified just because its message mentions readback");
    require(service::edge::protocol::commandResultDomainState(
                State::COMMAND_STATE_DEVICE_OFFLINE, false,
                "write acknowledged; readback failed or unavailable") == "UNKNOWN",
            "acknowledged write with offline readback was marked never executed");
    for (const auto reason : {"fins_readback_timeout", "s7_readback_timeout",
                              "modbus_readback_timeout"}) {
        require(service::edge::protocol::commandResultDomainState(
                    State::COMMAND_STATE_FAILED, false, reason) == "UNKNOWN",
                "failed readback was misrepresented as a confirmed non-execution");
    }
    require(service::edge::protocol::commandResultDomainState(
                State::COMMAND_STATE_FAILED, false, "s7_readback_mismatch") ==
                "READBACK_MISMATCH",
            "explicit readback mismatch lost its distinct result state");

    service::edge::pb::CommandResult legacyOffline;
    legacyOffline.set_state(State::COMMAND_STATE_DEVICE_OFFLINE);
    require(!legacyOffline.write_ack_missing() &&
                service::edge::protocol::commandResultDomainState(
                    legacyOffline.state(), legacyOffline.write_ack_missing(), "") == "REJECTED",
            "legacy result without the new flag changed its existing state mapping");
    legacyOffline.set_state(State::COMMAND_STATE_FAILED);
    require(service::edge::protocol::commandResultDomainState(
                legacyOffline.state(), legacyOffline.write_ack_missing(), "") == "FAILED",
            "legacy failed result without the new flag changed its existing state mapping");

    auto envelope = service::edge::protocol::outbound(
        "00000000-0000-7000-8000-000000000010", 123456789,
        service::message::edge::kDefaultPlatformId,
        "00000000-0000-7000-8000-000000000011");
    auto* result = envelope.mutable_command_result();
    result->set_command_id(std::string(16, '\x01'));
    result->set_device_id(std::string(16, '\x02'));
    result->set_state(State::COMMAND_STATE_DEVICE_OFFLINE);
    result->set_write_ack_missing(true);
    service::edge::pb::Envelope decoded;
    require(service::edge::protocol::decode(service::edge::protocol::encode(envelope), decoded) &&
                decoded.command_result().write_ack_missing() &&
                service::edge::protocol::commandResultDomainState(
                    decoded.command_result().state(), decoded.command_result().write_ack_missing(),
                    decoded.command_result().message()) == "UNKNOWN",
            "write acknowledgement ambiguity did not survive Edge protobuf serialization");
}

void testCommandResultRequiresTerminalState() {
    require(!service::edge::protocol::persistableCommandResult(
                service::edge::pb::COMMAND_STATE_RUNNING, false) &&
                service::edge::protocol::persistableCommandResult(
                    service::edge::pb::COMMAND_STATE_RUNNING, true),
            "missing write acknowledgement was discarded with a non-terminal firmware state");
    require(!service::edge::protocol::terminalCommandResultState(
                service::edge::pb::COMMAND_STATE_UNSPECIFIED) &&
                !service::edge::protocol::terminalCommandResultState(
                    service::edge::pb::COMMAND_STATE_ACCEPTED) &&
                !service::edge::protocol::terminalCommandResultState(
                    service::edge::pb::COMMAND_STATE_RUNNING),
            "non-terminal command progress completed the command");
    require(service::edge::protocol::terminalCommandResultState(
                service::edge::pb::COMMAND_STATE_SUCCEEDED) &&
                service::edge::protocol::terminalCommandResultState(
                    service::edge::pb::COMMAND_STATE_TIMED_OUT) &&
                service::edge::protocol::terminalCommandResultState(
                    service::edge::pb::COMMAND_STATE_FAILED),
            "terminal command result state was rejected");
}

} // namespace

int main() {
    const auto explicitEnvelope = service::edge::protocol::outbound(
        "00000000-0000-7000-8000-000000000004", 123456789,
        "00000000-0000-7000-8000-000000000005",
        "00000000-0000-7000-8000-000000000006", 12, 34);
    require(explicitEnvelope.created_at_ms() == 123456789 &&
                explicitEnvelope.session_epoch() == 12 && explicitEnvelope.sequence() == 34,
            "outbound protocol changed supplied time or session identity");
    require(service::common::uuidText(explicitEnvelope.message_id()) ==
                "00000000-0000-7000-8000-000000000004" &&
                service::common::uuidText(explicitEnvelope.platform_id()) ==
                "00000000-0000-7000-8000-000000000005",
            "outbound protocol ignored explicit message or platform identity");
    require(service::edge::protocol::validImei("490154203237518"), "valid IMEI rejected");
    require(!service::edge::protocol::validImei("490154203237519"), "bad IMEI accepted");
    testNetworkNanopbWireContract();
    testConfigItemNanopbWireContract();
    testEnvelopeRoundTrip();
    testCompatibleProtocolVersionContract();
    testTerminalOpenedContract();
    testTerminalFlowControlContract();
    testVpnConfigContract();
    testLegacyEdgenodeContract();
    testTelemetryRawFrameArray();
    testPlatformIdConfiguration();
    testSessionPlatformIdentityIsInternal();
    testModemProfileRoundTrip();
    testNanopbBounds();
    testWebTerminalProtobuf();
    testLegacyDebugAcquisitionIdentity();
    testCommandResultProjectionState();
    testCommandResultRequiresTerminalState();
    std::cout << "edge protocol tests passed\n";
}
