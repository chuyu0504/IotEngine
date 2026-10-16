#include <cstdlib>
#include <iostream>
#include "service/features/edge/serial_debug/serial_debug.protocol.h"
#include "service/features/edge/serial_debug/serial_debug.entity.h"

static void require(bool value, const char* message) {
    if (!value) { std::cerr << message << '\n'; std::exit(1); }
}
int main() {
    using namespace service::edge::serial_debug;
    const auto request = decodeRequest(R"({"action":"write","requestId":2,"hex":"00FF0A80"})", "/dev/ttyS1", 1);
    require(request && request->data() == std::string("\0\xff\x0a\x80", 4), "binary bytes changed");
    require(!decodeRequest(R"({"action":"write","requestId":2,"hex":"FF"})", "/dev/ttyS1", 2), "replayed write accepted");
    require(!decodeRequest(R"({"action":"write","requestId":3,"hex":"0G"})", "/dev/ttyS1", 2), "invalid hex accepted");
    require(!decodeRequest(R"({"action":"open","requestId":3})", "/dev/ttyS1", 2), "browser can replace session");
    require(!decodeRequest(R"({"action":"write","requestId":9007199254740992,"hex":"FF"})", "/dev/ttyS1", 2), "unsafe sequence accepted");
    const auto manual = decodeRequest(R"({"action":"manual","requestId":3,"baudRate":115200,"dataBits":8,"stopBits":1,"parity":"none","rs485":true})", "/dev/ttyS1", 2);
    require(manual && manual->settings().channel() == "/dev/ttyS1" && manual->settings().rs485(), "manual settings lost");
    require(!decodeRequest(R"({"action":"manual","requestId":3,"baudRate":4294976896,"dataBits":8,"stopBits":1,"parity":"none"})", "/dev/ttyS1", 2), "baud overflow accepted");
    require(!decodeHex(std::string(2050, '0')), "oversize manual payload accepted");
    require(!decodeHex("0"), "half byte accepted");
    wire::SerialDebugEvent event;
    event.set_kind("data"); event.set_direction("RX"); event.set_data("\0\xff", 2);
    event.set_sequence(8); event.mutable_settings()->set_channel("/dev/ttyS1");
    const auto encoded = eventJson(event);
    require(encoded.find("\"hex\":\"00FF\"") != std::string::npos, "event lost binary bytes");
    require(ruvia::json_value::parse(encoded).has_value(), "event JSON is invalid");
    require(encoded == R"({"kind":"data","requestId":0,"manual":false,"direction":"RX","hex":"00FF","timestamp":0,"sequence":8,"droppedBytes":0,"message":"","settings":{"path":"/dev/ttyS1","baudRate":0,"dataBits":0,"stopBits":0,"parity":"","rs485":false}})",
        "serial event wire shape changed");
    const auto ticket = SerialTargetRecord::decode("00000000-0000-7000-8000-000000000001\n1|6|2|owner\n/dev/ttyS1");
    require(ticket && ticket->path == "/dev/ttyS1", "ticket mapping failed");
    require(!SerialTargetRecord::decode("00000000-0000-7000-8000-000000000001\nowner\n/dev/ttyS1\nextra"), "multiline path accepted");
    const BrowserBindingRecord binding{
        "00000000-0000-7000-8000-000000000002",
        "00000000-0000-7000-8000-000000000003", *ticket};
    const auto restored = BrowserBindingRecord::decode(binding.encode());
    require(restored && restored->userId == binding.userId && restored->connectionId == binding.connectionId &&
        restored->ticket.nodeSession == binding.ticket.nodeSession && restored->ticket.path == binding.ticket.path,
        "browser binding lost connection ownership");
    require(!BrowserBindingRecord::decode("other-user\n" + binding.connectionId + "\n" + binding.encode()),
        "invalid browser owner accepted");
    std::cout << "serial debug protocol tests passed\n";
}
