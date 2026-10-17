#pragma once

// 数据库配置读取与 Collector 配置投影编排。

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_handle.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/collector/collector.service.h"
#include "service/features/collector/collector.types.h"
#include "service/features/collector/mqtt/mqtt.types.h"
#include "service/features/configuration/configuration.entity.h"
#include "service/features/live/live.service.h"
#include "service/features/messaging/messaging.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/utils/number.h"

namespace service::configuration {

using service::collector::DeviceDefinition;
using service::collector::ElementDefinition;
using service::collector::LinkDefinition;
using service::collector::LinkTargetDefinition;
using service::collector::RealtimeDeviceDefinition;
using service::collector::RealtimePointDefinition;
using service::collector::RuntimeSnapshot;

// DTU 注册包/心跳包内容 → 字节。
inline std::vector<std::uint8_t> packetBytes(std::string_view mode, std::string_view content) {
    if (mode == "OFF" || content.empty())
        return {};
    if (mode == "ASCII") {
        std::vector<std::uint8_t> bytes;
        bytes.reserve(content.size());
        for (std::size_t index = 0; index < content.size(); ++index) {
            if (content[index] != '\\' || index + 1 >= content.size()) {
                bytes.push_back(static_cast<std::uint8_t>(content[index]));
                continue;
            }
            switch (content[index + 1]) {
            case 'r':
                bytes.push_back(0x0D);
                ++index;
                break;
            case 'n':
                bytes.push_back(0x0A);
                ++index;
                break;
            case 't':
                bytes.push_back(0x09);
                ++index;
                break;
            case '\\':
                bytes.push_back('\\');
                ++index;
                break;
            default:
                bytes.push_back(static_cast<std::uint8_t>(content[index]));
                break;
            }
        }
        return bytes;
    }
    if (mode != "HEX")
        throw std::runtime_error("Unsupported DTU registration mode: " + std::string(mode));
    std::string normalized;
    normalized.reserve(content.size());
    for (const auto character : content)
        if (!std::isspace(static_cast<unsigned char>(character)))
            normalized.push_back(character);
    const auto bytes = message::fromHex(normalized);
    if (bytes.empty() && !normalized.empty())
        throw std::runtime_error("Invalid HEX DTU registration content");
    return bytes;
}

namespace detail {
inline std::string_view trim(std::string_view value) noexcept {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())))
        value.remove_prefix(1);
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())))
        value.remove_suffix(1);
    return value;
}

inline std::int64_t integer(std::string_view value, std::int64_t fallback = 0) noexcept {
    value = trim(value);
    if (value.empty())
        return fallback;
    std::int64_t result = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    return error == std::errc{} && end == value.data() + value.size() ? result : fallback;
}

inline std::int64_t integerInRange(std::string_view value, std::int64_t minimum,
                                   std::int64_t maximum, std::int64_t fallback) noexcept {
    const auto parsed = integer(value, fallback);
    return parsed < minimum || parsed > maximum ? fallback : parsed;
}

template <typename Row> std::string cell(const Row& row, std::size_t column) {
    return std::string(row[column].value().value_or(std::string_view{}));
}
template <typename Row>
std::int64_t cellInt(const Row& row, std::size_t column, std::int64_t fallback = 0) {
    return integer(row[column].value().value_or(std::string_view{}), fallback);
}
template <typename Row>
std::uint16_t cellPort(const Row& row, std::size_t column, std::uint16_t fallback = 0) {
    return static_cast<std::uint16_t>(integerInRange(
        row[column].value().value_or(std::string_view{}), 0, std::numeric_limits<std::uint16_t>::max(), fallback));
}
template <typename Row>
std::uint8_t cellUInt8(const Row& row, std::size_t column, std::uint8_t fallback = 0) {
    return static_cast<std::uint8_t>(integerInRange(
        row[column].value().value_or(std::string_view{}), 0, std::numeric_limits<std::uint8_t>::max(), fallback));
}
template <typename Row> bool cellBool(const Row& row, std::size_t column) {
    const auto value = row[column].value().value_or(std::string_view{});
    return value == "t" || value == "true" || value == "1";
}

// 以 d（device）、l（link）、p（device_model）联表，限定未删除且均已启用、由 Collector 执行的设备；
// deviceId 非空时只取该设备。调用方追加投影与要素展开。
inline void enabledCollectorDevices(ruvia::db_query& query, std::string_view deviceId) {
    using Op = ruvia::db_binary_operator;
    using Device = persistence::DeviceEntity;
    using Link = persistence::LinkEntity;
    using Model = persistence::DeviceModelEntity;
    query.from(Device::table_name(), "d")
        .join(ruvia::db_join_type::inner, Link::table_name(), query.binary(Link::column<"id">().expression(query, "l"), Op::equal, Device::column<"link_id">().expression(query, "d")), "l")
        .join(ruvia::db_join_type::inner, Model::table_name(), query.binary(Model::column<"device_id">().expression(query, "p"), Op::equal, Device::column<"id">().expression(query, "d")), "p")
        .and_where(query.unary(ruvia::db_unary_operator::is_null, Device::column<"deleted_at">().expression(query, "d")))
        .and_where(query.binary(Device::column<"status">().expression(query, "d"), Op::equal, query.value("enabled")))
        .and_where(query.unary(ruvia::db_unary_operator::is_null, Link::column<"deleted_at">().expression(query, "l")))
        .and_where(query.binary(Link::column<"status">().expression(query, "l"), Op::equal, query.value("enabled")))
        .and_where(query.binary(Link::column<"execution">().expression(query, "l"), Op::equal, query.value("collector")))
        .and_where(query.unary(ruvia::db_unary_operator::is_null, Model::column<"deleted_at">().expression(query, "p")))
        .and_where(query.binary(Model::column<"enabled">().expression(query, "p"), Op::equal, query.value(true)));
    if (!deviceId.empty()) {
        query.and_where(query.binary(Device::column<"id">().expression(query, "d"), Op::equal, query.cast(query.value(deviceId), ruvia::db_data_type::uuid)));
    }
}

// 填充 devices 的要素定义：只为 devices 中出现的协议执行对应的要素展开查询，结果按设备 ID 归入；
// MQTT 要素来自已加载的 calculationConfig。
template <typename Database>
ruvia::task<void> loadDeviceElements(Database& db, std::string_view deviceId, std::vector<DeviceDefinition>& devices) {
    using Op = ruvia::db_binary_operator;
    using Type = ruvia::db_data_type;
    using Model = persistence::DeviceModelEntity;

    std::unordered_map<std::string_view, std::size_t> deviceIndexes;
    deviceIndexes.reserve(devices.size());
    std::set<std::string_view, std::less<>> protocols;
    for (std::size_t index = 0; index < devices.size(); ++index) {
        deviceIndexes.emplace(devices[index].id, index);
        protocols.insert(devices[index].protocol);
    }
    const auto findDevice = [&devices, &deviceIndexes](std::string_view id) -> DeviceDefinition* {
        const auto device = deviceIndexes.find(id);
        return device == deviceIndexes.end() ? nullptr : &devices[device->second];
    };

    const auto protocolElements = [deviceId](std::string_view protocol, std::string_view arrayKey, std::string_view alias) {
        ruvia::db_query query;
        enabledCollectorDevices(query, deviceId);
        query.join_function(ruvia::db_join_type::cross, query.call("jsonb_array_elements", { query.coalesce({ query.binary(Model::column<"config">().expression(query, "p"), Op::json_get, query.value(arrayKey)), query.cast(query.value("[]"), Type::jsonb) }) }), {}, alias, { .lateral_ = true })
            .and_where(query.binary(Model::column<"protocol">().expression(query, "p"), Op::equal, query.value(protocol)));
        return query;
    };
    const auto elementText = [](ruvia::db_query& query, std::string_view key) {
        return query.binary(query.column("element"), Op::json_get_text, query.value(key));
    };
    const auto elementDefault = [&](ruvia::db_query& query, std::string_view key, std::string_view fallback) {
        return query.coalesce({ elementText(query, key), query.value(fallback) });
    };
    const auto writableElement = [&](ruvia::db_query& query) {
        return query.binary(query.call("lower", { elementDefault(query, "writable", "false") }), Op::in, query.list({ query.value("true"), query.value("t"), query.value("1") }));
    };
    const auto orderElementAddress = [&](ruvia::db_query& query, std::string_view key) {
        query.add_order_by(query.column("id", "d")).add_order_by(query.case_when({ { query.binary(elementDefault(query, key, ""), Op::regex, query.value("^-?[0-9]{1,18}$")), query.cast(elementText(query, key), Type::big_int) } }, query.value(0)));
    };

    if (protocols.contains("Modbus")) {
        auto modbusQuery = protocolElements("Modbus", "registers", "element");
        modbusQuery.select({ modbusQuery.cast(modbusQuery.column("id", "d"), Type::text), elementText(modbusQuery, "id"), elementText(modbusQuery, "name"), elementDefault(modbusQuery, "unit", ""), elementText(modbusQuery, "dataType"), modbusQuery.coalesce({ elementText(modbusQuery, "byteOrder"), modbusQuery.binary(modbusQuery.column("config", "p"), Op::json_get_text, modbusQuery.value("byteOrder")), modbusQuery.value("BIG_ENDIAN") }), elementText(modbusQuery, "registerType"), elementText(modbusQuery, "address"), elementText(modbusQuery, "quantity"), modbusQuery.value("1"), elementDefault(modbusQuery, "decimals", "-1"), writableElement(modbusQuery) });
        orderElementAddress(modbusQuery, "address");
        const auto modbusElements = co_await db.query(modbusQuery);
        for (const auto& row : modbusElements) {
            auto* device = findDevice(row[0].value().value_or(std::string_view{}));
            if (!device) {
                continue;
            }
            ElementDefinition element;
            element.configKey = "element:" + cell(row, 1);
            element.id = cell(row, 1);
            element.name = cell(row, 2);
            element.unit = cell(row, 3);
            element.dataType = cell(row, 4);
            element.byteOrder = cell(row, 5);
            element.registerType = cell(row, 6);
            element.address = cellInt(row, 7);
            element.quantity = cellInt(row, 8);
            element.decimals = cellInt(row, 10);
            element.writable = cellBool(row, 11);
            device->elements.push_back(std::move(element));
        }
    }

    for (const auto protocol : { "MC", "FINS", "DLT645" }) {
        if (!protocols.contains(protocol)) {
            continue;
        }
        auto query = protocolElements(protocol, "points", "element");
        query.select({ query.cast(query.column("id", "d"), Type::text), elementText(query, "id"), elementText(query, "name"), elementDefault(query, "unit", ""), elementText(query, "dataType"), elementDefault(query, "byteOrder", std::string_view(protocol) == "MC" ? "LITTLE_ENDIAN" : "BIG_ENDIAN"), elementDefault(query, "area", ""), elementDefault(query, "address", "0"), elementDefault(query, "bit", "0"), query.value("1"), elementDefault(query, "decimals", "-1"), writableElement(query), elementDefault(query, "identifier", ""), elementDefault(query, "length", "0"), elementDefault(query, "digits", "0") });
        orderElementAddress(query, "address");
        const auto rows = co_await db.query(query);
        for (const auto& row : rows) {
            auto* device = findDevice(row[0].value().value_or(std::string_view{}));
            if (!device) {
                continue;
            }
            ElementDefinition element;
            element.configKey = "element:" + cell(row, 1);
            element.id = cell(row, 1);
            element.name = cell(row, 2);
            element.unit = cell(row, 3);
            element.dataType = cell(row, 4);
            element.byteOrder = cell(row, 5);
            element.area = cell(row, 6);
            element.address = cellInt(row, 7);
            element.startBit = cellInt(row, 8);
            element.decimals = cellInt(row, 10);
            element.writable = cellBool(row, 11);
            element.guideHex = cell(row, 12);
            element.length = cellInt(row, 13);
            element.digits = cellInt(row, 14);
            if (element.dataType == "HEX") {
                element.encoding = "HEX";
            }
            device->elements.push_back(std::move(element));
        }
    }

    for (auto& device : devices) {
        if (device.protocol != "MQTT") {
            continue;
        }
        const auto config = ruvia::from_json<service::collector::mqtt::Config>(device.calculationConfig);
        if (!config) {
            throw std::invalid_argument("invalid MQTT configuration projection");
        }
        for (const auto& point : config->get<"points">()) {
            ElementDefinition element;
            element.configKey = "element:" + std::string(point.get<"id">().view());
            element.id = point.get<"id">().view();
            element.name = point.get<"name">().view();
            element.dataType = point.get<"dataType">().view();
            element.mqttField = point.get<"field">().view();
            const auto& unit = point.get<"unit">();
            element.unit = unit ? std::string(unit->view()) : "";
            element.writable = point.get<"writable">().value_or(ruvia::bool_value{ false }).value_;
            element.size = 65535;
            device.elements.push_back(std::move(element));
        }
    }

    if (protocols.contains("S7")) {
        auto s7Query = protocolElements("S7", "areas", "element");
        s7Query.select({ s7Query.cast(s7Query.column("id", "d"), Type::text), elementText(s7Query, "id"), elementText(s7Query, "name"), elementDefault(s7Query, "unit", ""), elementDefault(s7Query, "dataType", "UINT8"), elementText(s7Query, "area"), elementDefault(s7Query, "dbNumber", "0"), elementText(s7Query, "start"), elementDefault(s7Query, "startBit", "0"), elementText(s7Query, "size"), elementDefault(s7Query, "decimals", "-1"), writableElement(s7Query) });
        orderElementAddress(s7Query, "start");
        const auto s7Elements = co_await db.query(s7Query);
        for (const auto& row : s7Elements) {
            auto* device = findDevice(row[0].value().value_or(std::string_view{}));
            if (!device) {
                continue;
            }
            ElementDefinition element;
            element.configKey = "element:" + cell(row, 1);
            element.id = cell(row, 1);
            element.name = cell(row, 2);
            element.unit = cell(row, 3);
            element.dataType = cell(row, 4);
            element.area = cell(row, 5);
            element.dbNumber = cellInt(row, 6);
            element.start = cellInt(row, 7);
            element.startBit = cellInt(row, 8);
            element.size = cellInt(row, 9);
            element.decimals = cellInt(row, 10);
            element.writable = cellBool(row, 11);
            device->elements.push_back(std::move(element));
        }
    }

    if (protocols.contains("SL651")) {
        const auto sl651Fields = [](std::string_view key, bool response) {
            ruvia::db_query query;
            query.select({ query.column("element"), query.alias(query.cast(query.value(response), Type::boolean), "response_element") })
                .from_function(query.call("jsonb_array_elements", { query.coalesce({ query.binary(query.column("func"), Op::json_get, query.value(key)), query.cast(query.value("[]"), Type::jsonb) }) }), "element");
            return query;
        };
        auto fields = sl651Fields("elements", false);
        const auto responseFields = sl651Fields("responseElements", true);
        fields.combine(ruvia::db_set_operation::union_all, responseFields);
        auto sl651Query = protocolElements("SL651", "funcs", "func");
        const auto sl651Text = [&](std::string_view key) {
            return sl651Query.binary(sl651Query.column("element", "configured"), Op::json_get_text, sl651Query.value(key));
        };
        const auto functionCode = sl651Query.binary(sl651Query.column("func"), Op::json_get_text, sl651Query.value("funcCode"));
        sl651Query.join(ruvia::db_join_type::cross, fields, {}, "configured", { .lateral_ = true })
            .select({ sl651Query.cast(sl651Query.column("id", "d"), Type::text), sl651Text("id"), sl651Text("name"), sl651Query.coalesce({ sl651Text("unit"), sl651Query.value("") }), functionCode, sl651Query.binary(sl651Query.column("func"), Op::json_get_text, sl651Query.value("dir")), sl651Text("guideHex"), sl651Text("encode"), sl651Text("length"), sl651Query.coalesce({ sl651Text("digits"), sl651Query.value("0") }), sl651Query.column("response_element", "configured"), sl651Query.coalesce({ sl651Text("positionMode"), sl651Query.value("GUIDE") }), sl651Query.coalesce({ sl651Text("byteOffset"), sl651Query.value("0") }) })
            .add_order_by(sl651Query.column("id", "d"))
            .add_order_by(functionCode)
            .add_order_by(sl651Query.column("response_element", "configured"))
            .add_order_by(sl651Text("id"));
        const auto sl651Elements = co_await db.query(sl651Query);
        for (const auto& row : sl651Elements) {
            auto* device = findDevice(row[0].value().value_or(std::string_view{}));
            if (!device) {
                continue;
            }
            ElementDefinition element;
            element.configKey = (cellBool(row, 10) ? "response:" : "element:") + cell(row, 1);
            element.id = cell(row, 1);
            element.name = cell(row, 2);
            element.unit = cell(row, 3);
            element.functionCode = cell(row, 4);
            element.direction = cell(row, 5);
            element.guideHex = cell(row, 6);
            element.encoding = cell(row, 7);
            element.length = cellInt(row, 8);
            element.digits = cellInt(row, 9);
            element.responseElement = cellBool(row, 10);
            element.positionMode = cell(row, 11);
            element.byteOffset = cellInt(row, 12);
            device->elements.push_back(std::move(element));
        }
    }
}

} // namespace detail

// 加载已启用 Collector 设备的运行定义（连接参数、协议参数与要素），按 link_id、id 排序；
// deviceId 非空时只加载该设备，设备、链路或设备模型未启用时结果为空。
template <typename Database>
ruvia::task<std::vector<DeviceDefinition>> loadDeviceDefinitions(Database& db, std::string_view deviceId = {}) {
    using detail::cell;
    using detail::cellBool;
    using detail::cellInt;
    using Op = ruvia::db_binary_operator;
    using Type = ruvia::db_data_type;
    using Device = persistence::DeviceEntity;
    using Link = persistence::LinkEntity;
    using Model = persistence::DeviceModelEntity;

    ruvia::db_query deviceQuery;
    const auto parameters = Device::column<"protocol_params">().expression(deviceQuery, "d");
    const auto config = Model::column<"config">().expression(deviceQuery, "p");
    const auto jsonText = [&](ruvia::db_expression object, std::string_view key) {
        return deviceQuery.binary(object, Op::json_get_text, deviceQuery.value(key));
    };
    const auto defaultText = [&](ruvia::db_expression object, std::string_view key, std::string_view fallback) {
        return deviceQuery.coalesce({ jsonText(object, key), deviceQuery.value(fallback) });
    };
    const auto heartbeat = deviceQuery.binary(parameters, Op::json_get, deviceQuery.value("heartbeat"));
    const auto registration = deviceQuery.binary(parameters, Op::json_get, deviceQuery.value("registration"));
    const auto connection = deviceQuery.binary(config, Op::json_get, deviceQuery.value("connection"));
    const auto packet = deviceQuery.binary(config, Op::json_get, deviceQuery.value("packet"));
    deviceQuery.select({ deviceQuery.cast(Device::column<"id">().expression(deviceQuery, "d"), Type::text), jsonText(parameters, "device_code"), Device::column<"name">().expression(deviceQuery, "d"), deviceQuery.cast(Device::column<"link_id">().expression(deviceQuery, "d"), Type::text), jsonText(Link::column<"endpoint">().expression(deviceQuery, "l"), "mode"), defaultText(parameters, "target_id", ""), Model::column<"protocol">().expression(deviceQuery, "p"), defaultText(parameters, "timezone", service::message::device::kDefaultTimezone), deviceQuery.coalesce({ deviceQuery.null_if(jsonText(parameters, "online_timeout"), deviceQuery.value("")), deviceQuery.value("300") }), defaultText(heartbeat, "mode", "OFF"), defaultText(heartbeat, "content", ""), defaultText(registration, "mode", "OFF"), defaultText(registration, "content", ""), defaultText(parameters, "modbus_mode", ""), deviceQuery.coalesce({ deviceQuery.null_if(jsonText(parameters, "slave_id"), deviceQuery.value("")), deviceQuery.value("1") }), defaultText(connection, "mode", "RACK_SLOT"), defaultText(connection, "connectionType", "PG"), defaultText(connection, "rack", "0"), defaultText(connection, "slot", "1"), defaultText(connection, "localTSAP", "0100"), defaultText(connection, "remoteTSAP", "0101"), deviceQuery.coalesce({ jsonText(connection, "handshakeTimeout"), jsonText(config, "handshakeTimeout"), deviceQuery.value("5000") }), deviceQuery.coalesce({ jsonText(connection, "directProbeTimeout"), jsonText(config, "directProbeTimeout"), deviceQuery.value("5000") }), deviceQuery.coalesce({ jsonText(connection, "probeMode"), jsonText(config, "probeMode"), deviceQuery.value("STANDARD") }), deviceQuery.coalesce({ deviceQuery.null_if(jsonText(config, "readInterval"), deviceQuery.value("")), deviceQuery.value("300") }), defaultText(config, "storagePolicy", "report"), defaultText(config, "commandFastReadDuration", "60"), defaultText(config, "commandFastReadInterval", "1"), defaultText(packet, "mergeGap", "100"), defaultText(packet, "maxQuantity", "125"), deviceQuery.cast(Model::column<"id">().expression(deviceQuery, "p"), Type::text), defaultText(config, "responseMode", "M1"), Device::column<"debug_enabled">().expression(deviceQuery, "d"), defaultText(connection, "frame", "3E"), defaultText(connection, "version", "2007"), defaultText(connection, "network", "0"), defaultText(connection, "station", "255"), defaultText(connection, "moduleIo", "1023"), defaultText(connection, "multidrop", "0"), defaultText(connection, "monitoringTimer", "16"), defaultText(connection, "destinationNetwork", "0"), defaultText(connection, "destinationNode", "0"), defaultText(connection, "destinationUnit", "0"), defaultText(connection, "sourceNetwork", "0"), defaultText(connection, "sourceNode", "0"), defaultText(connection, "sourceUnit", "0"), defaultText(connection, "wakeupBytes", "4"), defaultText(connection, "writePassword", ""), defaultText(connection, "operatorCode", ""), deviceQuery.cast(config, Type::text), defaultText(parameters, "parameters", "{}") });
    detail::enabledCollectorDevices(deviceQuery, deviceId);
    deviceQuery.add_order_by(Device::column<"link_id">().expression(deviceQuery, "d")).add_order_by(Device::column<"id">().expression(deviceQuery, "d"));
    const auto rows = co_await db.query(deviceQuery);
    std::vector<DeviceDefinition> devices;
    devices.reserve(rows.size());
    for (const auto& row : rows) {
        DeviceDefinition device;
        device.id = cell(row, 0);
        device.code = cell(row, 1);
        device.name = cell(row, 2);
        device.linkId = cell(row, 3);
        device.linkMode = cell(row, 4);
        device.targetId = cell(row, 5);
        device.protocol = cell(row, 6);
        device.timezone = cell(row, 7);
        device.onlineTimeout = cellInt(row, 8, 300);
        device.heartbeatMode = cell(row, 9);
        device.heartbeatBytes = packetBytes(device.heartbeatMode, row[10].value().value_or(std::string_view{}));
        device.registrationMode = cell(row, 11);
        device.registrationBytes = packetBytes(device.registrationMode, row[12].value().value_or(std::string_view{}));
        if (device.linkMode != "TCP Server" || device.protocol == "SL651") {
            device.heartbeatMode = "OFF";
            device.heartbeatBytes.clear();
            device.registrationMode = "OFF";
            device.registrationBytes.clear();
        }
        device.modbusMode = cell(row, 13);
        device.slaveId = detail::cellUInt8(row, 14, 1);
        device.s7ConnectionMode = cell(row, 15);
        device.s7ConnectionType = cell(row, 16);
        device.s7Rack = cellInt(row, 17);
        device.s7Slot = cellInt(row, 18);
        device.s7LocalTsap = cell(row, 19);
        device.s7RemoteTsap = cell(row, 20);
        device.s7HandshakeTimeoutMs = cellInt(row, 21);
        device.s7DirectProbeTimeoutMs = cellInt(row, 22);
        device.s7ProbeMode = cell(row, 23);
        device.readInterval = cellInt(row, 24);
        device.calculationConfig = cell(row, 49);
        device.parameters = cell(row, 50);
        device.storagePolicy = cell(row, 25);
        device.commandFastReadDuration = cellInt(row, 26);
        device.commandFastReadInterval = cellInt(row, 27);
        device.modbusMergeGap = cellInt(row, 28);
        device.modbusMaxQuantity = cellInt(row, 29);
        device.modelId = cell(row, 30);
        device.sl651ResponseMode = cell(row, 31);
        device.debugEnabled = cellBool(row, 32);
        service::collector::ConnectionConfig::apply(device, {
            {"mc_frame", cell(row, 33)},
            {"dlt645_version", cell(row, 34)},
            {"mc_network", cell(row, 35)},
            {"mc_station", cell(row, 36)},
            {"mc_module_io", cell(row, 37)},
            {"mc_multidrop", cell(row, 38)},
            {"mc_monitoring_timer", cell(row, 39)},
            {"fins_destination_network", cell(row, 40)},
            {"fins_destination_node", cell(row, 41)},
            {"fins_destination_unit", cell(row, 42)},
            {"fins_source_network", cell(row, 43)},
            {"fins_source_node", cell(row, 44)},
            {"fins_source_unit", cell(row, 45)},
            {"dlt645_wakeup_bytes", cell(row, 46)},
            {"dlt645_write_password", cell(row, 47)},
            {"dlt645_operator_code", cell(row, 48)}
        });
        devices.push_back(std::move(device));
    }
    co_await detail::loadDeviceElements(db, deviceId, devices);
    co_return devices;
}

// Collector 运行快照：collector 链路与目标、启用的 collector 设备，以及全部未删除设备的实时点位读模型。
template <typename Database>
ruvia::task<RuntimeSnapshot> loadRuntimeSnapshot(Database& db) {
    using detail::cell;
    using detail::cellBool;
    using detail::cellInt;
    RuntimeSnapshot snapshot;

    using Op = ruvia::db_binary_operator;
    using Type = ruvia::db_data_type;
    ruvia::db_query linkQuery;
    const auto endpointText = [&](std::string_view key) {
        return linkQuery.binary(linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"endpoint">(), service::configuration::persistence::LinkEntity::columns_type>::name.view()), Op::json_get_text, linkQuery.value(key));
    };
    linkQuery.select({ linkQuery.cast(linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"id">(), service::configuration::persistence::LinkEntity::columns_type>::name.view()), Type::text), linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"name">(), service::configuration::persistence::LinkEntity::columns_type>::name.view()), endpointText("mode"), linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"protocol">(), service::configuration::persistence::LinkEntity::columns_type>::name.view()), linkQuery.coalesce({ endpointText("ip"), linkQuery.value("") }), linkQuery.coalesce({ linkQuery.null_if(endpointText("port"), linkQuery.value("")), linkQuery.value("0") }), linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"status">(), service::configuration::persistence::LinkEntity::columns_type>::name.view()), linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"debug_enabled">(), service::configuration::persistence::LinkEntity::columns_type>::name.view()) })
        .from(service::configuration::persistence::LinkEntity::table_name())
        .and_where(linkQuery.unary(ruvia::db_unary_operator::is_null, linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"deleted_at">(), service::configuration::persistence::LinkEntity::columns_type>::name.view())))
        .and_where(linkQuery.binary(linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"execution">(), service::configuration::persistence::LinkEntity::columns_type>::name.view()), Op::equal, linkQuery.value("collector")))
        .add_order_by(linkQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"id">(), service::configuration::persistence::LinkEntity::columns_type>::name.view()));
    const auto links = co_await db.query(linkQuery);
    for (const auto& row : links) {
        LinkDefinition link;
        link.id = cell(row, 0);
        link.name = cell(row, 1);
        link.mode = cell(row, 2);
        link.protocol = cell(row, 3);
        link.ip = cell(row, 4);
        link.port = detail::cellPort(row, 5);
        link.status = cell(row, 6);
        link.debugEnabled = cellBool(row, 7);
        snapshot.links.push_back(std::move(link));
    }
    std::unordered_map<std::string_view, std::size_t> linkIndexes;
    linkIndexes.reserve(snapshot.links.size());
    for (std::size_t index = 0; index < snapshot.links.size(); ++index) {
        linkIndexes.emplace(snapshot.links[index].id, index);
    }

    ruvia::db_query targetQuery;
    const auto targetText = [&](std::string_view key) {
        return targetQuery.binary(targetQuery.column("target"), Op::json_get_text, targetQuery.value(key));
    };
    targetQuery.select({ targetQuery.cast(targetQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"id">(), service::configuration::persistence::LinkEntity::columns_type>::name.view(), "l"), Type::text), targetText("id"), targetText("name"), targetText("ip"), targetQuery.coalesce({ targetQuery.null_if(targetText("port"), targetQuery.value("")), targetQuery.value("0") }), targetQuery.coalesce({ targetText("status"), targetQuery.value("enabled") }), targetQuery.coalesce({ targetText("mqtt"), targetQuery.value("{}") }) })
        .from(service::configuration::persistence::LinkEntity::table_name(), "l")
        .join_function(ruvia::db_join_type::cross, targetQuery.call("jsonb_array_elements", { targetQuery.coalesce({ targetQuery.binary(targetQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"endpoint">(), service::configuration::persistence::LinkEntity::columns_type>::name.view(), "l"), Op::json_get, targetQuery.value("targets")), targetQuery.cast(targetQuery.value("[]"), Type::jsonb) }) }), {}, "target", { .lateral_ = true })
        .and_where(targetQuery.unary(ruvia::db_unary_operator::is_null, targetQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"deleted_at">(), service::configuration::persistence::LinkEntity::columns_type>::name.view(), "l")))
        .and_where(targetQuery.binary(targetQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"execution">(), service::configuration::persistence::LinkEntity::columns_type>::name.view(), "l"), Op::equal, targetQuery.value("collector")))
        .and_where(targetQuery.binary(targetQuery.binary(targetQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"endpoint">(), service::configuration::persistence::LinkEntity::columns_type>::name.view(), "l"), Op::json_get_text, targetQuery.value("mode")), Op::equal, targetQuery.value("TCP Client")))
        .add_order_by(targetQuery.column(std::tuple_element_t<service::configuration::persistence::LinkEntity::column_index<"id">(), service::configuration::persistence::LinkEntity::columns_type>::name.view(), "l"));
    const auto targets = co_await db.query(targetQuery);
    for (const auto& row : targets) {
        const auto linkId = cell(row, 0);
        const auto link = linkIndexes.find(linkId);
        if (link == linkIndexes.end()) {
            continue;
        }
        LinkTargetDefinition target;
        target.id = cell(row, 1);
        target.name = cell(row, 2);
        target.ip = cell(row, 3);
        target.port = detail::cellPort(row, 4);
        target.status = cell(row, 5);
        target.mqttConfig = cell(row, 6);
        snapshot.links[link->second].targets.push_back(std::move(target));
    }

    snapshot.devices = co_await loadDeviceDefinitions(db);

    // This read model covers every non-deleted device, including edge-executed and disabled
    // devices. It replaces request-time PostgreSQL lookups in the Open Access realtime API.
    const auto configuredProtocol = [](std::string_view protocol, std::string_view arrayKey, int order) {
        ruvia::db_query query;
        query.from(service::configuration::persistence::DeviceEntity::table_name(), "d")
            .join(ruvia::db_join_type::inner, service::configuration::persistence::DeviceModelEntity::table_name(),
                query.binary(query.column(std::tuple_element_t<service::configuration::persistence::DeviceModelEntity::column_index<"device_id">(), service::configuration::persistence::DeviceModelEntity::columns_type>::name.view(), "p"), Op::equal, query.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"id">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d")), "p")
            .and_where(query.binary(query.column(std::tuple_element_t<service::configuration::persistence::DeviceModelEntity::column_index<"protocol">(), service::configuration::persistence::DeviceModelEntity::columns_type>::name.view(), "p"), Op::equal, query.value(protocol)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"deleted_at">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d")));
        const auto entries = query.call("jsonb_array_elements", { query.coalesce({
            query.binary(query.column(std::tuple_element_t<service::configuration::persistence::DeviceModelEntity::column_index<"config">(), service::configuration::persistence::DeviceModelEntity::columns_type>::name.view(), "p"), Op::json_get, query.value(arrayKey)), query.cast(query.value("[]"), Type::jsonb) }) });
        if (protocol == "SL651" && arrayKey == "funcs") {
            query.join_function(ruvia::db_join_type::cross, entries, {}, "functions",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = { { .name_ = "function" }, { .name_ = "function_position" } } })
                .join_function(ruvia::db_join_type::cross, query.call("jsonb_array_elements", { query.coalesce({
                    query.binary(query.column("function"), Op::json_get, query.value("elements")), query.cast(query.value("[]"), Type::jsonb) }) }),
                    {}, "elements", { .lateral_ = true, .with_ordinality_ = true, .columns_ = { { .name_ = "element" }, { .name_ = "element_position" } } })
                .and_where(query.binary(query.binary(query.column("function"), Op::json_get_text, query.value("dir")), Op::equal, query.value("UP")))
                .select({ query.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"id">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d"), query.column("element"), query.cast(query.value(order), Type::integer), query.column("function_position"), query.column("element_position") });
        } else {
            query.join_function(ruvia::db_join_type::cross, entries, {}, "entry",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = { { .name_ = "element" }, { .name_ = "position" } } })
                .select({ query.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"id">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d"), query.column("element"), query.cast(query.value(order), Type::integer), query.column("position"), query.cast(query.value(0), Type::big_int) });
        }
        query.and_where(query.binary(query.coalesce({ query.binary(query.column("element"), Op::json_get_text, query.value("encode")), query.value("") }),
            Op::not_equal, query.value("JPEG")));
        return query;
    };
    auto configured = configuredProtocol("Modbus", "registers", 1);
    for (const auto protocol : {"Modbus", "S7", "SL651", "MC", "FINS", "DLT645", "MQTT"})
        configured.combine(ruvia::db_set_operation::union_all, configuredProtocol(protocol, "derivedPoints", 5));
    for (const auto protocol : {"MC", "FINS", "DLT645", "MQTT"})
        configured.combine(ruvia::db_set_operation::union_all, configuredProtocol(protocol, "points", 4));
    const auto s7Configured = configuredProtocol("S7", "areas", 2);
    const auto sl651Configured = configuredProtocol("SL651", "funcs", 3);
    configured.combine(ruvia::db_set_operation::union_all, s7Configured).combine(ruvia::db_set_operation::union_all, sl651Configured);
    ruvia::db_query realtime;
    const auto realtimeText = [&](std::string_view key) {
        return realtime.binary(realtime.column("element", "configured"), Op::json_get_text, realtime.value(key));
    };
    realtime.with("configured", configured, { .columns_ = { "device_id", "element", "protocol_order", "function_order", "element_order" } })
        .select({ realtime.cast(realtime.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"id">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d"), Type::text),
            realtime.coalesce({ realtime.binary(realtime.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"protocol_params">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d"), Op::json_get_text, realtime.value("device_code")), realtime.value("") }),
            realtime.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"name">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d"), realtimeText("id"), realtime.coalesce({ realtimeText("name"), realtimeText("id") }),
            realtime.coalesce({ realtimeText("unit"), realtime.value("") }) })
        .from(service::configuration::persistence::DeviceEntity::table_name(), "d")
        .join(ruvia::db_join_type::left, "configured", realtime.binary(realtime.column("device_id", "configured"), Op::equal, realtime.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"id">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d")))
        .and_where(realtime.unary(ruvia::db_unary_operator::is_null, realtime.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"deleted_at">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d")))
        .add_order_by(realtime.column(std::tuple_element_t<service::configuration::persistence::DeviceEntity::column_index<"id">(), service::configuration::persistence::DeviceEntity::columns_type>::name.view(), "d")).add_order_by(realtime.column("protocol_order", "configured"))
        .add_order_by(realtime.column("function_order", "configured")).add_order_by(realtime.column("element_order", "configured"));
    const auto realtimeRows = co_await db.query(realtime);
    RealtimeDeviceDefinition* realtimeDevice = nullptr;
    for (const auto& row : realtimeRows) {
        const auto deviceId = cell(row, 0);
        if (!realtimeDevice || realtimeDevice->id != deviceId) {
            RealtimeDeviceDefinition device;
            device.id = deviceId;
            device.code = cell(row, 1);
            device.name = cell(row, 2);
            snapshot.realtimeDevices.push_back(std::move(device));
            realtimeDevice = &snapshot.realtimeDevices.back();
        }
        if (row[3].value().has_value())
            realtimeDevice->points.push_back(
                RealtimePointDefinition{cell(row, 3), cell(row, 4), cell(row, 5)});
    }

    co_return snapshot;
}

} // namespace service::configuration

namespace service::configuration {

class ConfigurationService final {
  public:
    template <typename Context>
    static ruvia::task<std::string> project(Context& context, bool notify = false) {
        auto transaction = co_await context.db().begin_transaction();
        // Every Service Worker shares this transaction-scoped lock. The snapshot is loaded only
        // after earlier projections finish, so a slower request can never overwrite a newer DB
        // state.
        ruvia::db_query lock;
        lock.select(lock.call("pg_advisory_xact_lock", {
            lock.cast(lock.value(service::message::lock::kConfigurationProjection), ruvia::db_data_type::big_int) }));
        (void)co_await transaction.query(lock);
        auto snapshot =
            co_await service::configuration::loadRuntimeSnapshot(transaction);
        auto version =
            co_await service::collector::config::project(context.redis(), snapshot, *context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>());
        co_await transaction.commit();
        if (notify)
            co_await service::live::publish(context.redis(), service::message::live::topic::kRuntimeConfig);
        co_return version;
    }

    // 处理一批运行配置变更流条目：按持久回执跳过已处理事件；link/device/protocol 变更重新投影运行配置，
    // device 变更另向告警元数据变更流发布一条通知（alert 运行时消费后刷新元数据），最后写入回执。
    // 返回是否执行了投影；回执写入后才由调用方确认（XACK/XDEL）这些条目。
    template <typename Context>
    static ruvia::task<bool> applyRuntimeChanges(Context& context, std::string_view consumer, const std::vector<service::message::StreamMessage>& messages) {
        const auto eventIds = service::messaging::idempotency::eventIds(messages);
        const auto pendingIds = co_await service::messaging::idempotency::pending(context, consumer, eventIds);
        bool projectionRequired = false;
        bool deviceChanged = false;
        for (const auto& message : messages) {
            if (!service::messaging::idempotency::shouldProcess(message, pendingIds)) {
                continue;
            }
            const auto aggregate = message.get("aggregate");
            projectionRequired = projectionRequired || aggregate == "link" || aggregate == "device" || aggregate == "protocol";
            deviceChanged = deviceChanged || aggregate == "device";
        }
        if (projectionRequired) {
            (void)co_await project(context, true);
        }
        if (deviceChanged) {
            const std::vector<service::message::StreamField> fields{ { "aggregate", "device" } };
            (void)co_await service::messaging::stream::publishAndWake(context.redis(), service::message::alert::kMetadataChangesStream, fields, std::nullopt, service::message::WorkerStreamTask::FreshnessAlerts);
        }
        co_await service::messaging::idempotency::markProcessed(context, consumer, eventIds);
        co_return projectionRequired;
    }
};

} // namespace service::configuration
