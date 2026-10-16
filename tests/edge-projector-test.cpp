#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "service/features/edge/edge.protocol.h"

namespace {

namespace pb = service::edge::pb;
using service::edge::protocol::TelemetryValues;

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

// BOOL readings are projected as 0/1 like every other numeric point.
void testBooleanProjection() {
    pb::ScalarValue value;
    value.set_bool_value(true);
    require(TelemetryValues::scalarText(value) == "1", "edge BOOL true is not projected as 1");
    value.set_bool_value(false);
    require(TelemetryValues::scalarText(value) == "0", "edge BOOL false is not projected as 0");
    require(TelemetryValues::scalarJson(value) == "0", "edge BOOL JSON is not projected as 0");
}

// Projected telemetry retains each point's reported data type.
void testTelemetryDataType() {
    pb::TelemetryRecord record;
    record.set_protocol(pb::PROTOCOL_MODBUS);
    auto* item = record.add_values();
    item->set_element_id("temperature");
    item->mutable_value()->set_double_value(21.5);
    const auto json = TelemetryValues::telemetryJson(record);
    const auto expected = ",\"dataType\":\"" + TelemetryValues::scalarKind(item->value()) + "\"";
    require(json.find("\"temperature\"") != std::string::npos, "telemetry JSON omits the element");
    require(json.find(expected) != std::string::npos, "edge telemetry does not retain point data types");
}

// Heartbeat payload encodings accepted by EdgeNode configuration.
void testPacketBytes() {
    const auto hex = service::edge::protocol::packetBytes("HEX", "0a FF", "heartbeat_payload");
    require(hex.size() == 2 && hex[0] == 0x0a && hex[1] == 0xff, "HEX heartbeat payload decoded incorrectly");
    require(service::edge::protocol::packetBytes("OFF", "AA", "heartbeat_payload").empty(), "OFF heartbeat payload must be empty");
    const auto ascii = service::edge::protocol::packetBytes("ASCII", "ok", "heartbeat_payload");
    require(ascii.size() == 2 && ascii[0] == 'o', "ASCII heartbeat payload decoded incorrectly");
}

} // namespace

int main() {
    try {
        testBooleanProjection();
        testTelemetryDataType();
        testPacketBytes();
        std::cout << "edge projector tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "edge projector test failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
