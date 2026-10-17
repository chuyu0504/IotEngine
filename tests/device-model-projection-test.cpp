#include <algorithm>
#include <array>
#include <cstdint>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <iostream>
#include <limits>
#include <memory>
#include <openssl/evp.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "service/features/collector/dlt645/dlt645.protocol.h"
#include "service/features/collector/sl651/sl651.protocol.h"
#include "service/features/configuration/configuration.protocol.h"
#include "service/features/edge/edge.service.h"

namespace {
namespace pb = iot::edge::v1;
using service::configuration::decodeModelElements;
using service::configuration::DeviceModel;

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

DeviceModel project(std::string_view protocol, std::string_view config) {
    DeviceModel model;
    model.device.protocol = protocol;
    model.device.calculationConfig = config;
    decodeModelElements(model);
    return model;
}

std::vector<DeviceModel> protocolFixtures() {
    std::vector<DeviceModel> models;
    models.push_back(project("Modbus", R"({"byteOrder":"LITTLE_ENDIAN","registers":[
        {"id":"z","address":90},
        {"id":"m","address":"50","name":null,"unit":null,"dataType":null,"byteOrder":null,"quantity":null,"decimals":null,"writable":null},
        {"id":"a","name":"pressure","unit":"kPa","registerType":"HOLDING","address":1,"dataType":"FLOAT32","byteOrder":"BIG_ENDIAN_SWAP","quantity":"2","decimals":3,"writable":true}]})"));
    models.push_back(project("S7", R"({"areas":[
        {"id":"z","start":90},
        {"id":"m","start":"50","dataType":null,"size":null,"dbNumber":null,"startBit":null,"decimals":null},
        {"id":"a","name":"switch","unit":"state","area":"DB","dbNumber":"7","start":1,"startBit":5,"size":"2","dataType":"BOOL","decimals":0,"writable":true}]})"));
    for (const auto protocol : { "MC", "FINS" }) {
        models.push_back(project(protocol, R"({"points":[
            {"id":"z","address":90},
            {"id":"m","address":"50","dataType":null,"byteOrder":null,"bit":null,"decimals":null},
            {"id":"a","name":"counter","unit":"count","area":"D","address":1,"bit":"3","dataType":"INT32","byteOrder":"BIG_ENDIAN_SWAP","decimals":"2","writable":true}]})"));
    }
    models.push_back(project("DLT645", R"({"points":[
        {"id":"z","address":90,"identifier":"00010000"},
        {"id":"m","address":"50","identifier":null,"dataType":null,"length":null,"digits":null,"decimals":null},
        {"id":"a","name":"energy","unit":"kWh","address":1,"identifier":"01020304","dataType":"HEX","length":"6","digits":0,"decimals":4,"writable":true}]})"));
    models.push_back(project("SL651", R"({"funcs":[
        {"funcCode":"99","name":"empty","dir":"UP","elements":[],"responseElements":[]},
        {"funcCode":null,"name":null,"dir":null,"elements":null,"responseElements":null},
        {"funcCode":"32","name":"report","dir":"UP","elements":[{"id":"z","writable":true}],"responseElements":[{"id":"m","writable":true}]},
        {"funcCode":"01","name":"command","dir":"DOWN","elements":[
            {"id":"b","name":"level","unit":"m","encode":"BCD","guideHex":"01AF","length":"4","digits":"2","positionMode":"OFFSET","byteOffset":"9","writable":false},
            {"id":"a","encode":null,"length":null,"digits":null,"positionMode":null,"byteOffset":null}],
            "responseElements":[{"id":"0","encode":"HEX","guideHex":"FF","length":1,"digits":0,"writable":true}]}]})"));
    return models;
}

void testDecoderDefaultsAndOrder(const std::vector<DeviceModel>& models) {
    for (std::size_t index = 0; index < 5; ++index) {
        const auto& elements = models[index].device.elements;
        require(elements.size() == 3, "native protocol lost elements");
        require(elements[0].id == "z" && elements[1].id == "m" && elements[2].id == "a", "native array order was sorted by ID");
        require((index == 1 ? elements[0].start : elements[0].address) == 90 && (index == 1 ? elements[1].start : elements[1].address) == 50 && (index == 1 ? elements[2].start : elements[2].address) == 1, "native array order or numeric-string addresses changed");
        for (std::size_t n = 0; n < 2; ++n) {
            const auto& element = elements[n];
            require(element.configKey == "element:" + element.id && element.name.empty() && element.unit.empty() && element.decimals == -1 && !element.writable, "absent/null common point defaults diverged");
        }
        require(elements[2].writable && !elements[2].name.empty() && !elements[2].unit.empty(), "explicit point metadata was discarded");
    }
    const auto& modbus = models[0].device.elements;
    require(modbus[0].quantity == 1 && modbus[1].quantity == 1 && modbus[2].quantity == 2, "Modbus quantity defaults or explicit numeric string changed");
    require(modbus[0].dataType.empty() && modbus[1].dataType.empty() && modbus[2].dataType == "FLOAT32", "Modbus introduced an implicit data type");
    require(modbus[0].byteOrder == "LITTLE_ENDIAN" && modbus[1].byteOrder == "LITTLE_ENDIAN" && modbus[2].byteOrder == "BIG_ENDIAN_SWAP", "Modbus point/model byte-order precedence changed");
    for (const auto json : { R"({"registers":[{"id":"x"}]})", R"({"byteOrder":null,"registers":[{"id":"x","byteOrder":null}]})" }) {
        require(project("Modbus", json).device.elements[0].byteOrder == "BIG_ENDIAN", "Modbus missing model byte order must be big endian");
    }
    const auto& s7 = models[1].device.elements;
    require(s7[0].dataType == "UINT8" && s7[1].dataType == "UINT8" && s7[0].size == 1 && s7[1].size == 1, "S7 absent/null type and size defaults changed");
    require(s7[2].dataType == "BOOL" && s7[2].size == 2 && s7[2].dbNumber == 7 && s7[2].startBit == 5 && s7[2].area == "DB", "S7 explicit BOOL location changed");
    require(models[1].device.s7LocalTsap == "0100" && models[1].device.s7RemoteTsap == "0101", "S7 TSAP defaults changed");
    for (const auto index : { 2, 3 }) {
        const auto& elements = models[index].device.elements;
        const auto endian = index == 2 ? "LITTLE_ENDIAN" : "BIG_ENDIAN";
        require(elements[0].byteOrder == endian && elements[1].byteOrder == endian && elements[0].dataType == "UINT16" && elements[1].dataType == "UINT16", "MC/FINS absent/null endian or type defaults changed");
        require(elements[2].dataType == "INT32" && elements[2].byteOrder == "BIG_ENDIAN_SWAP" && elements[2].startBit == 3 && elements[2].decimals == 2 && elements[2].area == "D", "industrial explicit values changed");
    }
    const auto& meter = models[4].device.elements;
    for (std::size_t n = 0; n < 2; ++n) {
        require(meter[n].length == 4 && meter[n].digits == 2 && meter[n].dataType == "BCD", "DLT645 absent/null BCD length/digits changed");
    }
    require(meter[0].guideHex == "00010000" && meter[1].guideHex.empty() && meter[2].guideHex == "01020304" && meter[2].dataType == "HEX" && meter[2].encoding == "HEX" && meter[2].length == 6 && meter[2].digits == 0, "DLT645 explicit identifier/HEX/zero digits changed");
    const auto boundedBcd = project("DLT645", R"({"points":[{"id":"bounded","identifier":"00010000","dataType":"BCD","length":1,"digits":0,"writable":true}]})");
    const std::array<service::collector::CommandElementValue, 1> inRange{ { { "bounded", "99" } } };
    require(service::collector::command::resolve(boundedBcd.device, inRange).elements.size() == 1, "shared DLT645 model rejected a representable single-byte BCD command");
    bool overflowRejected = false;
    try {
        const std::array<service::collector::CommandElementValue, 1> overflow{ { { "bounded", "100" } } };
        (void)service::collector::command::resolve(boundedBcd.device, overflow);
    } catch (const std::invalid_argument&) {
        overflowRejected = true;
    }
    require(overflowRejected, "shared DLT645 model accepted an overflowing single-byte BCD command");
    require(boundedBcd.device.elements[0].encoding == "BCD", "canonical DLT645 model lost BCD encoding");
    require(service::collector::dlt645::FrameCodec::bcdBytes("99", 1, 0, false) == std::vector<std::uint8_t>{ 0x99 }, "single-byte BCD wire boundary changed");
    const auto signedBcd = project("DLT645", R"({"points":[{"id":"signed","dataType":"BCD_SIGNED","length":1,"digits":0,"writable":true}]})");
    const std::array<service::collector::CommandElementValue, 1> signedRequest{ { { "signed", "-79" } } };
    require(service::collector::command::resolve(signedBcd.device, signedRequest).elements.size() == 1, "DLT645 signed BCD incorrectly reserved a whole sign byte");
    require(service::collector::dlt645::FrameCodec::bcdBytes("-79", 1, 0, true) == std::vector<std::uint8_t>{ 0xf9 }, "DLT645 signed BCD wire sign changed");
    const auto preciseBcd = project("DLT645", R"({"points":[{"id":"precise","dataType":"BCD","length":5,"digits":8,"writable":true}]})");
    const std::array<service::collector::CommandElementValue, 1> preciseRequest{ { { "precise", "0.00000001" } } };
    require(service::collector::command::resolve(preciseBcd.device, preciseRequest).elements.size() == 1, "DLT645 legal eight-digit precision was subjected to SL651 limits");
    require(service::collector::dlt645::FrameCodec::bcdBytes("0.00000001", 5, 8, false) == std::vector<std::uint8_t>({ 1, 0, 0, 0, 0 }), "DLT645 eight-digit BCD wire precision changed");
    for (const auto& [device, value] : std::array<std::pair<const DeviceModel*, std::string_view>, 3>{ { { &signedBcd, "-80" }, { &boundedBcd, "-1" }, { &preciseBcd, "0.000000011" } } }) {
        bool rejected = false;
        try {
            const std::array<service::collector::CommandElementValue, 1> request{ { { device->device.elements[0].id, std::string(value) } } };
            (void)service::collector::command::resolve(device->device, request);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        require(rejected, "DLT645 command preparation disagreed with actual BCD sign/precision limits");
    }
}

void testSl651(const DeviceModel& model) {
    require(model.functions.size() == 4 && model.functions[0].code == "99" && model.functions[0].name == "empty" && model.functions[0].direction == "UP" && model.functions[1].code.empty() && model.functions[1].name.empty() && model.functions[1].direction.empty() && model.functions[2].code == "32" && model.functions[3].code == "01", "SL651 lost empty functions, null metadata, or function order");
    const auto& e = model.device.elements;
    require(e.size() == 5 && e[0].id == "z" && e[1].id == "m" && e[2].id == "b" && e[3].id == "a" && e[4].id == "0", "SL651 normal/response array order changed");
    require(!e[0].writable && !e[1].writable && e[2].writable && e[3].writable && !e[4].writable, "SL651 writability must derive from DOWN and nonresponse, not explicit writable");
    require(!e[0].responseElement && e[1].responseElement && !e[2].responseElement && !e[3].responseElement && e[4].responseElement && e[1].configKey == "response:m" && e[2].configKey == "element:b" && e[0].functionCode == "32" && e[2].functionCode == "01" && e[0].direction == "UP" && e[2].direction == "DOWN", "SL651 function/response identity changed");
    require(e[2].encoding == "BCD" && e[2].guideHex == "01AF" && e[2].length == 4 && e[2].digits == 2 && e[2].positionMode == "OFFSET" && e[2].byteOffset == 9 && e[3].encoding.empty() && e[3].length == 0 && e[3].digits == 0 && e[3].positionMode == "GUIDE" && e[3].byteOffset == 0, "SL651 explicit/null positioning or encoding changed");
    const auto empty = project("SL651", R"({"funcs":[{}]})");
    require(empty.functions.size() == 1 && empty.functions[0].code.empty() && empty.device.elements.empty(), "SL651 absent function metadata must retain the empty function");
}

void testMqtt() {
    const auto model = project("MQTT", R"({"points":[
        {"id":"z","name":"first","field":"values.high","dataType":"NUMBER"},
        {"id":"a","name":"second","field":"values.low","dataType":"BOOLEAN","unit":"state","writable":true}]})");
    const auto& e = model.device.elements;
    require(e.size() == 2 && e[0].id == "z" && e[1].id == "a" && e[0].name == "first" && e[0].mqttField == "values.high" && e[0].dataType == "NUMBER" && e[0].unit.empty() && !e[0].writable && e[0].size == 65535 && e[0].decimals == -1 && e[0].configKey == "element:z" && e[1].name == "second" && e[1].mqttField == "values.low" && e[1].dataType == "BOOLEAN" && e[1].unit == "state" && e[1].writable && e[1].size == 65535, "MQTT point required fields, optional defaults, or native order changed");
    for (const auto field : { "id", "name", "field", "dataType" }) {
        for (const bool null : { false, true }) {
            std::string json = "{\"points\":[{";
            bool separator = false;
            for (const auto key : { "id", "name", "field", "dataType" }) {
                if (std::string_view(key) == field && !null) {
                    continue;
                }
                if (separator) {
                    json += ',';
                }
                separator = true;
                json += '"';
                json += key;
                json += "\":";
                json += std::string_view(key) == field ? "null" : "\"value\"";
            }
            json += "}]}";
            bool rejected = false;
            try {
                (void)project("MQTT", json);
            } catch (const std::invalid_argument&) {
                rejected = true;
            }
            require(rejected, "MQTT accepted a missing/null required point field");
        }
    }
    std::vector<pb::ConfigItem> items;
    bool rejected = false;
    try {
        service::edge::ConfigService::appendConfiguredDevices(items, { model }, false);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    require(rejected && items.empty(), "MQTT must be rejected before emitting unspecified wire protocol");
}

std::string uuidBytes(unsigned char suffix) {
    std::string result(16, '\0');
    result[6] = '\x40';
    result[8] = static_cast<char>(0x80);
    result[15] = static_cast<char>(suffix);
    return result;
}

void configureIdentity(DeviceModel& model, std::size_t index) {
    auto& d = model.device;
    const char digit = static_cast<char>('1' + index);
    d.id = "00000000-0000-4000-8000-00000000000" + std::string(1, digit);
    d.linkId = "00000000-0000-4000-8000-00000000001" + std::string(1, digit);
    d.code = "device-" + std::to_string(index);
    d.name = "device name";
    d.debugEnabled = true;
    d.timezone = "Asia/Shanghai";
    d.heartbeatBytes = { 0x00, 0x01, 0xff };
    auto& e = model.endpoint;
    e.name = "endpoint";
    e.interfaceName = index % 2 ? "/dev/ttyS1" : "eth1";
    e.transport = index % 2 ? "serial" : "ethernet";
    e.mode = index == 0 ? "TCP Server" : "TCP Client";
    e.ip = "192.0.2.10";
    e.port = 502;
    e.enabled = index != 1;
    e.debugEnabled = true;
    model.enabled = index % 2 == 0;
}

void testNumericPointValues() {
    const auto model = project("Modbus", R"({"registers":[
        {"id":"z","address":1e3,"quantity":2.0,"decimals":"0"},
        {"id":"a","address":"9junk","quantity":1.5,"decimals":null}]})");
    require(model.device.elements[0].address == 1000 && model.device.elements[0].quantity == 2 && model.device.elements[0].decimals == 0 && model.device.elements[1].address == 0 && model.device.elements[1].quantity == 1 && model.device.elements[1].decimals == -1, "integer JSON fields lost exponent/integral/string support or accepted fractional/trailing input");
    require(service::configuration::configIntegerText("1.00000000000000001", 17) == 17 && service::configuration::configIntegerText("9223372036854775807.0", 17) == std::numeric_limits<std::int64_t>::max() && service::configuration::configIntegerText("-9223372036854775808.0", 17) == std::numeric_limits<std::int64_t>::min() && service::configuration::configIntegerText("9223372036854775808.0", 17) == 17, "numeric projection rounded fractional tokens or overflowed int64");
}

void testRealtimeReadModel() {
    const auto model = project("Modbus", R"({"registers":[
        {"id":"z","writable":"TRUE"},{"id":"a","name":"","unit":null,"writable":1}],
        "derivedPoints":[
            {"id":"d2","name":"second","kind":"expression","expression":"x","inputs":[{"alias":"x","pointId":"d1"}]},
            {"id":"d1","kind":"expression","expression":"x","inputs":[{"alias":"x","pointId":"z"}]}]})");
    require(model.points.size() == 4 && model.points[0].id == "z" && model.points[0].name == "z" && model.points[1].name.empty() && model.points[1].unit.empty() && model.points[2].id == "d2" && model.points[3].id == "d1", "realtime projection lost original point order or missing versus empty names");
    require(model.derivedPoints.size() == 2 && model.derivedPoints[0].id == "d1" && model.derivedPoints[1].id == "d2", "edge derived projection lost dependency order");
    require(model.device.elements[0].writable && model.device.elements[1].writable, "legacy imported writable text/numeric values no longer match old SQL projection");
    const auto sl651 = project("SL651", R"({"funcs":[
        {"funcCode":"32","dir":"UP","elements":[{"id":"image","encode":"JPEG"},{"id":"up","encode":"BCD"}],
         "responseElements":[{"id":"response"}]},
        {"funcCode":"01","dir":"DOWN","elements":[{"id":"down"}]}]})");
    require(sl651.points.size() == 1 && sl651.points[0].id == "up", "realtime SL651 catalog exposed image, DOWN or response-only fields");
}

// Build independent expected protobufs, then compare the complete message, including
// default-valued fields, oneof presence, and fields that must remain absent.
std::vector<pb::ConfigItem> expectedItems(const DeviceModel& m, std::size_t index) {
    constexpr std::array protocols{ pb::PROTOCOL_MODBUS, pb::PROTOCOL_S7, pb::PROTOCOL_MC, pb::PROTOCOL_FINS, pb::PROTOCOL_DLT645, pb::PROTOCOL_SL651 };
    const auto protocol = protocols[index];
    const auto id = uuidBytes(static_cast<unsigned char>(index + 1));
    const auto linkId = uuidBytes(static_cast<unsigned char>(0x11 + index));
    const auto& d = m.device;
    const auto& link = m.endpoint;
    std::vector<pb::ConfigItem> items;
    auto& endpointItem = items.emplace_back();
    endpointItem.set_kind(pb::CONFIG_ITEM_ENDPOINT);
    auto* endpoint = endpointItem.mutable_endpoint();
    endpoint->set_endpoint_id(linkId);
    endpoint->set_name(link.name);
    endpoint->set_interface_name(link.interfaceName);
    endpoint->set_protocol(protocol);
    endpoint->set_enabled(link.enabled);
    endpoint->set_debug_enabled(link.debugEnabled);
    if (index % 2) {
        endpoint->set_transport(pb::TRANSPORT_SERIAL);
        endpoint->set_mode(pb::LINK_MODE_SERIAL);
        auto* serial = endpoint->mutable_serial();
        serial->set_channel(link.interfaceName);
        serial->set_baud_rate(9600);
        serial->set_data_bits(8);
        serial->set_stop_bits(1);
        serial->set_parity("none");
    } else {
        endpoint->set_transport(pb::TRANSPORT_ETHERNET);
        endpoint->set_mode(index == 0 ? pb::LINK_MODE_TCP_SERVER : pb::LINK_MODE_TCP_CLIENT);
        endpoint->set_ip(link.ip);
        endpoint->set_port(502);
    }
    auto& deviceItem = items.emplace_back();
    deviceItem.set_kind(pb::CONFIG_ITEM_DEVICE);
    auto* device = deviceItem.mutable_device();
    device->set_device_id(id);
    device->set_endpoint_id(linkId);
    device->set_device_code(d.code);
    device->set_name(d.name);
    device->set_protocol(protocol);
    device->set_timezone("Asia/Shanghai");
    device->set_debug_enabled(true);
    device->set_enabled(m.enabled);
    device->set_io_interval_ms(0);
    device->set_report_interval_sec(300);
    device->set_online_timeout_sec(300);
    device->set_modbus_slave_id(1);
    device->set_modbus_mode("");
    device->set_modbus_merge_gap(100);
    device->set_modbus_max_quantity(125);
    device->set_s7_connection_mode("RACK_SLOT");
    device->set_s7_connection_type("PG");
    device->set_s7_rack(0);
    device->set_s7_slot(1);
    device->set_s7_local_tsap("0100");
    device->set_s7_remote_tsap("0101");
    device->set_command_fast_read_duration_sec(60);
    device->set_command_fast_read_interval_sec(1);
    device->set_heartbeat_payload(std::string("\x00\x01\xff", 3));
    if (index >= 2 && index <= 4) {
        auto* c = device->mutable_industrial();
        c->set_mc_four_e(false);
        c->set_mc_network(0);
        c->set_mc_station(255);
        c->set_mc_module_io(1023);
        c->set_mc_multidrop(0);
        c->set_mc_monitoring_timer(16);
        c->set_fins_source_network(0);
        c->set_fins_source_node(0);
        c->set_fins_source_unit(0);
        c->set_fins_destination_network(0);
        c->set_fins_destination_node(0);
        c->set_fins_destination_unit(0);
        c->set_dlt645_version(2007);
        c->set_dlt645_wakeup_bytes(4);
        c->set_dlt645_write_password("");
        c->set_dlt645_operator_code("");
    }
    if (index == 5) {
        device->set_sl651_response_mode(1);
    }
    for (const auto& f : m.functions) {
        auto& item = items.emplace_back();
        item.set_kind(pb::CONFIG_ITEM_SL651_FUNCTION);
        auto* target = item.mutable_sl651_function();
        target->set_device_id(id);
        target->set_function_code(f.code);
        target->set_name(f.name);
        target->set_direction(f.direction);
    }
    for (const auto& e : d.elements) {
        auto& item = items.emplace_back();
        if (index == 0) {
            item.set_kind(pb::CONFIG_ITEM_MODBUS_REGISTER);
            auto* t = item.mutable_modbus_register();
            t->set_device_id(id);
            t->set_element_id(e.id);
            t->set_name(e.name);
            t->set_unit(e.unit);
            t->set_register_type(e.registerType);
            t->set_data_type(e.dataType);
            t->set_byte_order(e.byteOrder);
            t->set_address(e.address);
            t->set_quantity(e.quantity);
            t->set_scale(1);
            t->set_decimals(e.decimals);
            t->set_writable(e.writable);
        } else if (index == 1) {
            item.set_kind(pb::CONFIG_ITEM_S7_AREA);
            auto* t = item.mutable_s7_area();
            t->set_device_id(id);
            t->set_element_id(e.id);
            t->set_name(e.name);
            t->set_unit(e.unit);
            t->set_area(e.area);
            t->set_db_number(e.dbNumber);
            t->set_start(e.start);
            t->set_start_bit(e.startBit);
            t->set_size(e.size);
            t->set_data_type(e.dataType);
            t->set_scale(1);
            t->set_decimals(e.decimals);
            t->set_writable(e.writable);
        } else if (index == 5) {
            item.set_kind(pb::CONFIG_ITEM_SL651_ELEMENT);
            auto* t = item.mutable_sl651_element();
            t->set_device_id(id);
            t->set_function_code(e.functionCode);
            t->set_element_id(e.id);
            t->set_name(e.name);
            t->set_unit(e.unit);
            t->set_encoding(e.encoding);
            t->set_length(e.length);
            t->set_digits(e.digits);
            // Fixture bytes are intentionally independent of the production HEX parser.
            t->set_guide(e.id == "b" ? std::string("\x01\xaf", 2) : e.id == "0" ? std::string("\xff", 1)
                                                                                : "");
            t->set_response_element(e.responseElement);
            t->set_writable(e.writable);
            t->set_fixed_position(e.positionMode == "OFFSET");
            t->set_byte_offset(e.byteOffset);
        } else {
            item.set_kind(pb::CONFIG_ITEM_INDUSTRIAL_POINT);
            auto* t = item.mutable_industrial_point();
            t->set_device_id(id);
            t->set_element_id(e.id);
            t->set_name(e.name);
            t->set_unit(e.unit);
            t->set_data_type(e.dataType);
            t->set_area(e.area);
            t->set_address(e.address);
            t->set_bit(e.startBit);
            t->set_byte_order(e.byteOrder);
            t->set_scale(1);
            t->set_decimals(e.decimals);
            t->set_identifier(e.guideHex);
            t->set_length(e.length);
            t->set_digits(e.digits);
            t->set_writable(e.writable);
        }
    }
    return items;
}

std::string deterministicBytes(const std::vector<pb::ConfigItem>& items) {
    std::string result;
    for (const auto& item : items) {
        std::string bytes;
        {
            google::protobuf::io::StringOutputStream output(&bytes);
            google::protobuf::io::CodedOutputStream coded(&output);
            coded.SetSerializationDeterministic(true);
            require(item.SerializeToCodedStream(&coded) && !coded.HadError(), "protobuf serialization failed");
        }
        const auto size = static_cast<std::uint64_t>(bytes.size());
        for (unsigned shift = 0; shift < 64; shift += 8) {
            result.push_back(static_cast<char>(size >> shift));
        }
        result += bytes;
    }
    return result;
}

std::string digest(std::string_view bytes) {
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    require(static_cast<bool>(context), "allocating SHA-256 context failed");
    std::array<unsigned char, EVP_MAX_MD_SIZE> output{};
    unsigned size = 0;
    require(EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) == 1 && EVP_DigestUpdate(context.get(), bytes.data(), bytes.size()) == 1 && EVP_DigestFinal_ex(context.get(), output.data(), &size) == 1 && size == 32, "SHA-256 calculation failed");
    return std::string(reinterpret_cast<const char*>(output.data()), size);
}

std::vector<pb::ConfigItem> append(const std::vector<DeviceModel>& models) {
    std::vector<pb::ConfigItem> result;
    service::edge::ConfigService::appendConfiguredDevices(result, models, false);
    return result;
}

void testEdgeProjectionAndDigest(std::vector<DeviceModel> models) {
    std::vector<pb::ConfigItem> expected;
    for (std::size_t i = 0; i < models.size(); ++i) {
        configureIdentity(models[i], i);
        auto next = expectedItems(models[i], i);
        expected.insert(expected.end(), next.begin(), next.end());
    }
    const auto actual = append(models);
    require(actual.size() == expected.size(), "edge projection omitted disabled models/functions/points");
    for (std::size_t i = 0; i < actual.size(); ++i) {
        require(actual[i].SerializeAsString() == expected[i].SerializeAsString(), "edge projection changed a representable protobuf field or item order");
    }
    const auto bytes = deterministicBytes(actual);
    const auto repeated = deterministicBytes(append(models));
    require(bytes == repeated && digest(bytes) == digest(repeated), "identical input must produce identical deterministic serialization and SHA-256");
    auto changed = models;
    changed[0].device.elements[0].quantity = 2;
    require(digest(bytes) != digest(deterministicBytes(append(changed))), "semantic change to default quantity did not change configuration digest");
    changed = models;
    std::swap(changed[0].device.elements[0], changed[0].device.elements[1]);
    require(digest(bytes) != digest(deterministicBytes(append(changed))), "native point order change did not change configuration digest");
    changed = models;
    changed[0].enabled = !changed[0].enabled;
    require(digest(bytes) != digest(deterministicBytes(append(changed))), "device enabled state did not change configuration digest");
}

void testExplicitWireSettings(std::vector<DeviceModel> models) {
    for (std::size_t index = 0; index < models.size(); ++index) {
        auto& model = models[index];
        configureIdentity(model, index);
        auto expected = expectedItems(model, index);
        auto& d = model.device;
        d.readInterval = 17;
        d.onlineTimeout = 91;
        d.slaveId = 12;
        d.modbusMode = "RTU";
        d.modbusMergeGap = 7;
        d.modbusMaxQuantity = 33;
        d.s7ConnectionMode = "TSAP";
        d.s7ConnectionType = "OP";
        d.s7Rack = 2;
        d.s7Slot = 4;
        d.s7LocalTsap = "0200";
        d.s7RemoteTsap = "0301";
        d.commandFastReadDuration = 23;
        d.commandFastReadInterval = 3;
        d.sl651ResponseMode = "M4";
        auto* device = expected[1].mutable_device();
        device->set_report_interval_sec(17);
        device->set_online_timeout_sec(91);
        device->set_modbus_slave_id(12);
        device->set_modbus_mode("RTU");
        device->set_modbus_merge_gap(7);
        device->set_modbus_max_quantity(33);
        device->set_s7_connection_mode("TSAP");
        device->set_s7_connection_type("OP");
        device->set_s7_rack(2);
        device->set_s7_slot(4);
        device->set_s7_local_tsap("0200");
        device->set_s7_remote_tsap("0301");
        device->set_command_fast_read_duration_sec(23);
        device->set_command_fast_read_interval_sec(3);
        if (index == 5) {
            device->set_sl651_response_mode(4);
        }
        if (index % 2) {
            model.endpoint.baudRate = 19200;
            model.endpoint.dataBits = 7;
            model.endpoint.stopBits = 2;
            model.endpoint.parity = "even";
            auto* serial = expected[0].mutable_endpoint()->mutable_serial();
            serial->set_baud_rate(19200);
            serial->set_data_bits(7);
            serial->set_stop_bits(2);
            serial->set_parity("even");
        }
        if (index >= 2 && index <= 4) {
            d.mcConnection.frame = service::collector::mc::FrameFormat::Binary4E;
            d.mcConnection.network = 1;
            d.mcConnection.station = 2;
            d.mcConnection.moduleIo = 3;
            d.mcConnection.multidrop = 4;
            d.mcConnection.monitoringTimer = 5;
            d.finsConnection.sourceNetwork = 6;
            d.finsConnection.sourceNode = 7;
            d.finsConnection.sourceUnit = 8;
            d.finsConnection.destinationNetwork = 9;
            d.finsConnection.destinationNode = 10;
            d.finsConnection.destinationUnit = 11;
            d.dlt645Connection.version = service::collector::dlt645::Version::V1997;
            d.dlt645Connection.wakeupBytes = 2;
            d.dlt645Connection.writePassword = "01020304";
            d.dlt645Connection.operatorCode = "05060708";
            auto* connection = device->mutable_industrial();
            connection->set_mc_four_e(true);
            connection->set_mc_network(1);
            connection->set_mc_station(2);
            connection->set_mc_module_io(3);
            connection->set_mc_multidrop(4);
            connection->set_mc_monitoring_timer(5);
            connection->set_fins_source_network(6);
            connection->set_fins_source_node(7);
            connection->set_fins_source_unit(8);
            connection->set_fins_destination_network(9);
            connection->set_fins_destination_node(10);
            connection->set_fins_destination_unit(11);
            connection->set_dlt645_version(1997);
            connection->set_dlt645_wakeup_bytes(2);
            connection->set_dlt645_write_password(std::string("\x01\x02\x03\x04", 4));
            connection->set_dlt645_operator_code(std::string("\x05\x06\x07\x08", 4));
        }
        const auto actual = append({ model });
        require(deterministicBytes(actual) == deterministicBytes(expected), "explicit endpoint/device/industrial settings changed or zero IO/identity scale was lost");
    }
}

DeviceModel repeatedSl651Model() {
    return project("SL651", R"({"funcs":[
        {"funcCode":"32","name":"first report","dir":"UP","elements":[
            {"id":"shared","name":"report 32","unit":"cm","encode":"BCD","guideHex":"3912","length":2,"digits":2},
            {"id":"anchor","name":"anchor","unit":"m","encode":"BCD","guideHex":"3810","length":2,"digits":0}],
         "responseElements":[{"id":"shared","name":"response 32","unit":"response32","encode":"HEX","guideHex":"3718","length":3,"digits":0}]},
        {"funcCode":"35","name":"later report","dir":"UP","elements":[
            {"id":"shared","name":"report 35","unit":"mm","encode":"BCD","guideHex":"3621","length":4,"digits":1,"positionMode":"OFFSET","byteOffset":12},
            {"id":"tail","name":"tail","unit":"s","encode":"BCD","guideHex":"3510","length":2,"digits":0},
            {"id":"image","name":"image","encode":"JPEG"}]},
        {"funcCode":"4C","name":"first command","dir":"DOWN","elements":[
            {"id":"shared","name":"command 4C","unit":"command4C","encode":"BCD","guideHex":"3411","length":2,"digits":1},
            {"id":"other","name":"other 4C","unit":"other4C","encode":"BCD","guideHex":"3310","length":2,"digits":0}],
         "responseElements":[
            {"id":"shared","name":"response 4C","unit":"response4C","encode":"HEX","guideHex":"3220","length":4,"digits":0},
            {"id":"response-only","name":"response only","encode":"HEX","length":1}]},
        {"funcCode":"4D","name":"different set","dir":"DOWN","elements":[
            {"id":"shared","name":"command 4D","unit":"command4D","encode":"BCD","guideHex":"3112","length":2,"digits":2},
            {"id":"third","name":"third 4D","encode":"BCD","guideHex":"3010","length":2,"digits":0}]},
        {"funcCode":"4E","name":"identical set","dir":"DOWN","elements":[
            {"id":"shared","name":"command 4E","unit":"command4E","encode":"BCD","guideHex":"2910","length":2,"digits":0},
            {"id":"other","name":"other 4E","encode":"BCD","guideHex":"2810","length":2,"digits":0}]}]})");
}

void testRepeatedSl651Projection() {
    auto model = repeatedSl651Model();
    const auto& elements = model.device.elements;
    const std::array<std::string_view, 14> ids{
        "shared",
        "anchor",
        "shared",
        "shared",
        "tail",
        "image",
        "shared",
        "other",
        "shared",
        "response-only",
        "shared",
        "third",
        "shared",
        "other"
    };
    const std::array<std::string_view, 14> names{
        "report 32",
        "anchor",
        "response 32",
        "report 35",
        "tail",
        "image",
        "command 4C",
        "other 4C",
        "response 4C",
        "response only",
        "command 4D",
        "third 4D",
        "command 4E",
        "other 4E"
    };
    const std::array<std::string_view, 14> codes{
        "32",
        "32",
        "32",
        "35",
        "35",
        "35",
        "4C",
        "4C",
        "4C",
        "4C",
        "4D",
        "4D",
        "4E",
        "4E"
    };
    require(elements.size() == ids.size(), "repeated SL651 IDs lost source occurrences");
    require(model.functions.size() == 5, "repeated SL651 IDs lost functions");
    const std::array<std::string_view, 5> functionCodes{ "32", "35", "4C", "4D", "4E" };
    const std::array<std::string_view, 5> functionNames{
        "first report",
        "later report",
        "first command",
        "different set",
        "identical set"
    };
    for (std::size_t index = 0; index < functionCodes.size(); ++index) {
        require(model.functions[index].code == functionCodes[index] && model.functions[index].name == functionNames[index] && model.functions[index].direction == (index < 2 ? "UP" : "DOWN"), "repeated SL651 function metadata or source order changed");
    }
    for (std::size_t index = 0; index < elements.size(); ++index) {
        const auto& element = elements[index];
        const bool response = index == 2 || index == 8 || index == 9;
        require(element.id == ids[index] && element.name == names[index] && element.functionCode == codes[index] && element.responseElement == response && element.direction == (index < 6 ? "UP" : "DOWN") && element.writable == (index >= 6 && !response) && element.configKey == std::string(response ? "response:" : "element:") + std::string(ids[index]), "repeated SL651 occurrence identity, metadata or function association changed");
    }
    require(elements[0].unit == "cm" && elements[0].guideHex == "3912" && elements[0].length == 2 && elements[0].digits == 2 && elements[2].unit == "response32" && elements[2].encoding == "HEX" && elements[2].length == 3 && elements[3].unit == "mm" && elements[3].guideHex == "3621" && elements[3].length == 4 && elements[3].digits == 1 && elements[3].positionMode == "OFFSET" && elements[3].byteOffset == 12 && elements[6].unit == "command4C" && elements[6].guideHex == "3411" && elements[6].digits == 1 && elements[8].unit == "response4C" && elements[8].guideHex == "3220" && elements[8].length == 4 && elements[10].unit == "command4D" && elements[10].guideHex == "3112" && elements[10].digits == 2 && elements[12].unit == "command4E" && elements[12].guideHex == "2910" && elements[12].digits == 0, "repeated SL651 occurrences overwrote each other's encoding or metadata");
    require(model.points.size() == 3 && model.points[0].id == "shared" && model.points[0].name == "report 35" && model.points[0].unit == "mm" && model.points[1].id == "anchor" && model.points[2].id == "tail", "realtime repeated SL651 IDs must keep first position and last UP metadata only");

    configureIdentity(model, 5);
    const auto wire = append({ model });
    std::size_t functionIndex = 0;
    std::size_t elementIndex = 0;
    for (const auto& item : wire) {
        if (item.kind() == pb::CONFIG_ITEM_SL651_FUNCTION) {
            require(functionIndex < model.functions.size(), "wire added an SL651 function");
            const auto& source = model.functions[functionIndex++];
            const auto& target = item.sl651_function();
            require(target.function_code() == source.code && target.name() == source.name && target.direction() == source.direction && target.device_id() == uuidBytes(6), "wire lost repeated SL651 function metadata or association");
        } else if (item.kind() == pb::CONFIG_ITEM_SL651_ELEMENT) {
            require(elementIndex < elements.size(), "wire added an SL651 occurrence");
            const auto& source = elements[elementIndex++];
            const auto& target = item.sl651_element();
            const auto guide = service::collector::sl651::detail::hexBytes(source.guideHex);
            const std::string guideBytes(guide.begin(), guide.end());
            require(target.element_id() == source.id && target.function_code() == source.functionCode && target.name() == source.name && target.unit() == source.unit && target.encoding() == source.encoding && target.length() == source.length && target.digits() == source.digits && target.guide() == guideBytes && target.fixed_position() == (source.positionMode == "OFFSET") && target.byte_offset() == source.byteOffset && target.response_element() == source.responseElement && target.writable() == source.writable && target.device_id() == uuidBytes(6), "wire collapsed repeated SL651 IDs or mixed per-function occurrence metadata");
        }
    }
    require(functionIndex == 5 && elementIndex == elements.size(), "wire omitted repeated SL651 functions or occurrences");
}

void testRepeatedSl651Commands() {
    namespace collector = service::collector;
    const auto model = repeatedSl651Model();
    const std::vector<collector::CommandElementValue> firstRequest{ { "other", " 7 " }, { "shared", "12.3" } };
    const auto first = collector::command::resolve(model.device, firstRequest);
    require(first.functionCode == "4C" && first.elements.size() == 2 && first.elements[0].definition == &model.device.elements[7] && first.elements[0].value == "7" && first.elements[1].definition == &model.device.elements[6] && first.elements[1].value == "12.3", "UP/response occurrences hid DOWN records, request order changed, or identical sets did not choose first function");
    const std::vector<collector::CommandElementValue> secondRequest{ { "shared", "12.34" }, { "third", "8" } };
    const auto second = collector::command::resolve(model.device, secondRequest);
    require(second.functionCode == "4D" && second.elements.size() == 2 && second.elements[0].definition == &model.device.elements[10] && second.elements[0].value == "12.34" && second.elements[1].definition == &model.device.elements[11] && second.elements[1].value == "8", "complete SL651 ID set did not select the matching later DOWN function");
    auto reordered = model.device;
    std::rotate(reordered.elements.begin() + 6, reordered.elements.begin() + 12, reordered.elements.end());
    const auto reorderedResult = collector::command::resolve(reordered, firstRequest);
    require(reorderedResult.functionCode == "4E" && reorderedResult.elements[0].definition->name == "other 4E" && reorderedResult.elements[1].definition->name == "command 4E", "identical SL651 command sets used code sorting instead of first source function");
    auto withImage = model.device;
    auto image = withImage.elements[5];
    image.functionCode = "4C";
    image.direction = "DOWN";
    withImage.elements.insert(withImage.elements.begin() + 8, image);
    require(collector::command::resolve(withImage, firstRequest).functionCode == "4E", "a DOWN function containing an uncommandable JPEG input hid the next complete function");
    bool imageRejected = false;
    try {
        const std::vector<collector::CommandElementValue> request{
            { "other", "7" },
            { "shared", "12.3" },
            { "image", "FFD8FFD9" }
        };
        (void)collector::command::resolve(withImage, request);
    } catch (const std::invalid_argument&) {
        imageRejected = true;
    }
    require(imageRejected, "SL651 command accepted a JPEG occurrence as a writable input");
    for (const auto& invalid : {
             std::vector<collector::CommandElementValue>{ { "shared", "1" } },
             std::vector<collector::CommandElementValue>{ { "shared", "1" }, { "missing", "2" } },
             std::vector<collector::CommandElementValue>{ { "shared", "1" }, { "anchor", "2" } },
             std::vector<collector::CommandElementValue>{ { "shared", "1" }, { "response-only", "2" } },
             std::vector<collector::CommandElementValue>{ { "other", "1" }, { "third", "2" } },
             std::vector<collector::CommandElementValue>{ { "shared", "1" }, { "shared", "2" } },
             std::vector<collector::CommandElementValue>{} }) {
        bool rejected = false;
        try {
            (void)collector::command::resolve(model.device, invalid);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        require(rejected, "SL651 accepted a partial, wrong, response-only, duplicate or empty command ID set");
    }
}

void testRepeatedSl651CommandFrame() {
    namespace collector = service::collector;
    auto model = repeatedSl651Model();
    model.device.id = "repeated-device";
    model.device.code = "0000000001";
    model.device.linkId = "repeated-link";
    model.device.timezone = "+08:00";
    model.device.sl651ResponseMode = "M2";
    auto snapshot = std::make_shared<collector::RuntimeSnapshot>();
    snapshot->devices.push_back(model.device);
    collector::LinkDefinition link;
    link.id = "repeated-link";
    link.protocol = "SL651";
    link.mode = "TCP Server";
    collector::sl651::Session session(link, "repeated-connection", snapshot, { &snapshot->devices[0] });

    // Independent CRC and report bytes establish a real station header before encoding.
    const auto crc16 = [](const auto& bytes) {
        std::uint16_t crc = 0xFFFF;
        for (const auto byte : bytes) {
            crc ^= byte;
            for (int bit = 0; bit < 8; ++bit) {
                crc = static_cast<std::uint16_t>((crc >> 1U) ^ ((crc & 1U) ? 0xA001U : 0U));
            }
        }
        return crc;
    };
    std::vector<std::uint8_t> report{
        0x7E,
        0x7E,
        0x01,
        0x00,
        0x00,
        0x00,
        0x00,
        0x01,
        0x00,
        0x00,
        0x32,
        0x00,
        0x0C,
        0x02,
        0x00,
        0x01,
        0x24,
        0x01,
        0x02,
        0x03,
        0x04,
        0x05,
        0x39,
        0x12,
        0x12,
        0x34,
        0x03
    };
    const auto reportCrc = crc16(report);
    report.push_back(static_cast<std::uint8_t>(reportCrc >> 8U));
    report.push_back(static_cast<std::uint8_t>(reportCrc));
    const auto received = session.consume({ .messageId = "report", .linkId = "repeated-link", .connectionId = "repeated-connection", .receivedAtMs = 1704135845000, .bytes = report });
    require(std::any_of(received.begin(), received.end(), [](const auto& action) {
                return action.kind == collector::ProtocolActionKind::PublishParsed;
            }),
            "SL651 command-frame fixture did not bind its station from an actual report");

    collector::ProtocolCommand command;
    command.id = "repeated-command";
    command.deviceId = model.device.id;
    command.deviceCode = model.device.code;
    command.kind = "control";
    command.elements = { { "other", "7" }, { "shared", "12.3" } };
    const auto actions = session.execute(std::move(command));
    const auto sent = std::find_if(actions.begin(), actions.end(), [](const auto& action) {
        return action.kind == collector::ProtocolActionKind::Send;
    });
    require(sent != actions.end(), "repeated SL651 ID command failed actual frame encoding");
    const auto& frame = sent->bytes;
    const std::vector<std::uint8_t> header{
        0x7E,
        0x7E,
        0x00,
        0x00,
        0x00,
        0x00,
        0x01,
        0x01,
        0x00,
        0x00,
        0x4C,
        0x80,
        0x10,
        0x02
    };
    const std::vector<std::uint8_t> values{ 0x33, 0x10, 0x00, 0x07, 0x34, 0x11, 0x01, 0x23 };
    require(frame.size() == 33 && std::equal(header.begin(), header.end(), frame.begin()) && frame[14] == 0 && frame[15] == 0 && std::equal(values.begin(), values.end(), frame.begin() + 22) && frame[30] == 0x05, "SL651 frame lost selected DOWN function, request-order guides or per-occurrence BCD precision");
    const auto expectedCrc = crc16(std::span<const std::uint8_t>(frame.data(), frame.size() - 2));
    require(frame[31] == static_cast<std::uint8_t>(expectedCrc >> 8U) && frame[32] == static_cast<std::uint8_t>(expectedCrc), "generated repeated-ID SL651 command frame CRC is invalid");
    // This covers collector encoding only. The edge request has no function selector,
    // so it does not establish how firmware resolves repeated IDs across functions.
}

void testElementFieldRoundtrip() {
    using service::collector::ElementDefinition;
    ElementDefinition element;
    element.configKey = "response:shared";
    element.id = "shared";
    element.name = "field name";
    element.unit = "field unit";
    element.dataType = "INT32";
    element.byteOrder = "LITTLE_ENDIAN";
    element.registerType = "HOLDING";
    element.mqttField = "mqtt.field";
    element.address = 101;
    element.quantity = 2;
    element.decimals = -1;
    element.writable = true;
    element.area = "DB";
    element.dbNumber = 3;
    element.start = 104;
    element.startBit = 5;
    element.size = 6;
    element.functionCode = "4C";
    element.direction = "DOWN";
    element.guideHex = "3912";
    element.positionMode = "OFFSET";
    element.byteOffset = 108;
    element.encoding = "BCD";
    element.length = 9;
    element.digits = 7;
    element.responseElement = true;
    const std::array<std::string_view, 26> names{
        "config_key",
        "id",
        "name",
        "unit",
        "data_type",
        "byte_order",
        "register_type",
        "mqtt_field",
        "address",
        "quantity",
        "decimals",
        "writable",
        "area",
        "db_number",
        "start",
        "start_bit",
        "size",
        "function_code",
        "direction",
        "guide_hex",
        "position_mode",
        "byte_offset",
        "encoding",
        "length",
        "digits",
        "response_element"
    };
    const std::array<std::string, 26> expected{
        "response:shared",
        "shared",
        "field name",
        "field unit",
        "INT32",
        "LITTLE_ENDIAN",
        "HOLDING",
        "mqtt.field",
        "101",
        "2",
        "-1",
        "1",
        "DB",
        "3",
        "104",
        "5",
        "6",
        "4C",
        "DOWN",
        "3912",
        "OFFSET",
        "108",
        "BCD",
        "9",
        "7",
        "1"
    };
    require(ElementDefinition::kFieldNames == names && element.fieldValues() == expected, "element snapshot field names and values are incomplete or misaligned");
    std::vector<service::message::StreamField> fields;
    for (std::size_t index = 0; index < names.size(); ++index) {
        fields.push_back({ std::string(names[index]), expected[index] });
    }
    require(service::collector::config::detail::element(fields) == element, "element snapshot field mapping failed complete roundtrip");
    element.configKey.clear();
    require(element.fieldValues()[0] == element.id, "empty element configKey no longer serializes external ID");
    element.writable = false;
    element.responseElement = false;
    require(element.fieldValues()[11] == "0" && element.fieldValues()[25] == "0", "element false flags did not serialize as zero");
}

} // namespace

int main() {
    try {
        const auto models = protocolFixtures();
        testDecoderDefaultsAndOrder(models);
        testSl651(models.back());
        testMqtt();
        testNumericPointValues();
        testRealtimeReadModel();
        testEdgeProjectionAndDigest(models);
        testExplicitWireSettings(models);
        testRepeatedSl651Projection();
        testRepeatedSl651Commands();
        testElementFieldRoundtrip();
        testRepeatedSl651CommandFrame();
        std::cout << "device model projection tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "device model projection test failed: " << error.what() << '\n';
        return 1;
    }
}
