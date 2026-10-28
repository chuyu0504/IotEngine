#pragma once

// 数据库配置读取与 Collector 配置投影编排。

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_handle.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/redis/redis_handle.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/collector/collector.service.h"
#include "service/features/collector/collector.types.h"
#include "service/features/configuration/configuration.entity.h"
#include "service/features/configuration/configuration.protocol.h"
#include "service/features/live/live.service.h"
#include "service/features/messaging/messaging.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/utils/number.h"
#include "service/utils/startup.h"
#include "service/utils/uuid.h"

namespace service::configuration {

// Configuration writers take the exclusive transaction lock in database triggers.
// Consumers retain this shared lock through their final database/Redis side effects.
template <typename Context>
ruvia::task<ruvia::db_transaction> lockAcquisition(Context& context) {
    auto transaction = co_await context.db("acquisition-fence").begin_transaction();
    ruvia::db_query q, singleton;
    singleton.values({ singleton.value(1) });
    auto lock = transaction.template repository<persistence::AcquisitionEpochEntity>().query_builder();
    lock.with("singleton", singleton).from_cte("singleton").select({ { "locked", q.call("pg_advisory_xact_lock_shared", { q.cast(q.value(service::message::lock::kAcquisitionConfiguration), ruvia::db_data_type::big_int) }) } });
    (void)co_await lock.get_raw_many();
    co_return transaction;
}

template <typename Database>
ruvia::task<std::int64_t> acquisitionEpoch(Database& database, std::string_view nodeId) {
    ruvia::db_find_options options;
    options.where_ = persistence::AcquisitionEpochEntity::column<"node_id">() == nodeId;
    const auto row = co_await database.template repository<persistence::AcquisitionEpochEntity>().find_one(options);
    co_return row ? row->template get<"epoch">() : -1;
}

template <typename Context>
ruvia::task<std::optional<ruvia::db_transaction>> fenceAcquisition(Context& context, std::vector<service::message::ParsedDeviceMessage>& messages) {
    if (std::ranges::none_of(messages, [](const auto& value) {
            return !value.configurationDigest.empty();
        })) {
        co_return std::nullopt;
    }
    auto transaction = co_await lockAcquisition(context);
    std::map<std::string, std::int64_t, std::less<>> epochs;
    for (auto& value : messages) {
        if (value.configurationDigest.empty() || value.historyOnly) {
            continue;
        }
        auto found = epochs.find(value.connectionId);
        if (found == epochs.end()) {
            found = epochs.emplace(value.connectionId, co_await acquisitionEpoch(transaction, value.connectionId)).first;
        }
        value.historyOnly = value.configurationEpoch < 0 || value.configurationEpoch != found->second;
    }
    co_return std::move(transaction);
}

// DTU 注册包/心跳包内容 → 字节。
inline std::vector<std::uint8_t> packetBytes(std::string_view mode, std::string_view content) {
    if (mode == "OFF" || content.empty()) {
        return {};
    }
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
    if (mode != "HEX") {
        throw std::runtime_error("Unsupported DTU registration mode: " + std::string(mode));
    }
    std::string normalized;
    normalized.reserve(content.size());
    for (const auto character : content) {
        if (!std::isspace(static_cast<unsigned char>(character))) {
            normalized.push_back(character);
        }
    }
    const auto bytes = message::fromHex(normalized);
    if (bytes.empty() && !normalized.empty()) {
        throw std::runtime_error("Invalid HEX DTU registration content");
    }
    return bytes;
}

namespace detail {

inline std::int64_t integerInRange(std::string_view value, std::int64_t minimum, std::int64_t maximum, std::int64_t fallback) noexcept {
    const auto parsed = configIntegerText(value, fallback);
    return parsed < minimum || parsed > maximum ? fallback : parsed;
}

template <typename Row>
std::string cell(const Row& row, std::size_t column) {
    return std::string(row[column].value().value_or(std::string_view{}));
}

template <typename Row>
std::int64_t cellInt(const Row& row, std::size_t column, std::int64_t fallback = 0) {
    return configIntegerText(row[column].value().value_or(std::string_view{}), fallback);
}

template <typename Row>
std::uint16_t cellPort(const Row& row, std::size_t column, std::uint16_t fallback = 0) {
    return static_cast<std::uint16_t>(integerInRange(
        row[column].value().value_or(std::string_view{}),
        0,
        std::numeric_limits<std::uint16_t>::max(),
        fallback
    ));
}

template <typename Row>
std::uint8_t cellUInt8(const Row& row, std::size_t column, std::uint8_t fallback = 0) {
    return static_cast<std::uint8_t>(integerInRange(
        row[column].value().value_or(std::string_view{}),
        0,
        std::numeric_limits<std::uint8_t>::max(),
        fallback
    ));
}

template <typename Row>
bool cellBool(const Row& row, std::size_t column) {
    const auto value = row[column].value().value_or(std::string_view{});
    return value == "t" || value == "true" || value == "1";
}

// 所有未删除设备的同一模型联表；关联对象的删除/启停与执行位置由调用边界选择。
inline void joinDeviceModels(auto& target, ruvia::db_query& query) {
    using Op = ruvia::db_binary_operator;
    using Device = persistence::DeviceEntity;
    using Link = persistence::LinkEntity;
    using Model = persistence::DeviceModelEntity;
    target.template join<Link>(ruvia::db_join_type::inner, "l", query.binary(Link::column<"id">().expression(query, "l"), Op::equal, Device::column<"link_id">().expression(query, "d"))).template join<Model>(ruvia::db_join_type::inner, "p", query.binary(Model::column<"device_id">().expression(query, "p"), Op::equal, Device::column<"id">().expression(query, "d"))).and_where(query.unary(ruvia::db_unary_operator::is_null, Device::column<"deleted_at">().expression(query, "d")));
}

} // namespace detail

// 设备参数与协议默认值的唯一数据库投影；调用方追加真实执行上下文的筛选。
template <typename Database>
auto deviceModelsQuery(Database&& database) {
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
    auto deviceQueryBuilder = database.template repository<Device>().query_builder("d");
    deviceQueryBuilder.select({ { "id", deviceQuery.cast(Device::column<"id">().expression(deviceQuery, "d"), Type::text) }, { "value1", jsonText(parameters, "device_code") }, { "name", Device::column<"name">().expression(deviceQuery, "d") }, { "link_id", deviceQuery.cast(Device::column<"link_id">().expression(deviceQuery, "d"), Type::text) }, { "endpoint", jsonText(Link::column<"endpoint">().expression(deviceQuery, "l"), "mode") }, { "value5", defaultText(parameters, "target_id", "") }, { "protocol", Model::column<"protocol">().expression(deviceQuery, "p") }, { "value7", defaultText(parameters, "timezone", service::message::device::kDefaultTimezone) }, { "value8", deviceQuery.coalesce({ deviceQuery.null_if(jsonText(parameters, "online_timeout"), deviceQuery.value("")), deviceQuery.value("300") }) }, { "value9", defaultText(heartbeat, "mode", "OFF") }, { "value10", defaultText(heartbeat, "content", "") }, { "value11", defaultText(registration, "mode", "OFF") }, { "value12", defaultText(registration, "content", "") }, { "value13", defaultText(parameters, "modbus_mode", "TCP") }, { "value14", deviceQuery.coalesce({ deviceQuery.null_if(jsonText(parameters, "slave_id"), deviceQuery.value("")), deviceQuery.value("1") }) }, { "value15", defaultText(connection, "mode", "RACK_SLOT") }, { "value16", defaultText(connection, "connectionType", "PG") }, { "value17", defaultText(connection, "rack", "0") }, { "value18", defaultText(connection, "slot", "1") }, { "value19", defaultText(connection, "localTSAP", "0100") }, { "value20", defaultText(connection, "remoteTSAP", "0101") }, { "value21", deviceQuery.coalesce({ jsonText(connection, "handshakeTimeout"), jsonText(config, "handshakeTimeout"), deviceQuery.value("5000") }) }, { "value22", deviceQuery.coalesce({ jsonText(connection, "directProbeTimeout"), jsonText(config, "directProbeTimeout"), deviceQuery.value("5000") }) }, { "value23", deviceQuery.coalesce({ jsonText(connection, "probeMode"), jsonText(config, "probeMode"), deviceQuery.value("STANDARD") }) }, { "value24", deviceQuery.coalesce({ deviceQuery.null_if(jsonText(config, "readInterval"), deviceQuery.value("")), deviceQuery.value("300") }) }, { "value25", defaultText(config, "storagePolicy", "report") }, { "value26", defaultText(config, "commandFastReadDuration", "60") }, { "value27", defaultText(config, "commandFastReadInterval", "1") }, { "value28", defaultText(packet, "mergeGap", "100") }, { "value29", defaultText(packet, "maxQuantity", "125") }, { "value30", deviceQuery.cast(Model::column<"id">().expression(deviceQuery, "p"), Type::text) }, { "value31", defaultText(config, "responseMode", "M1") }, { "debug_enabled", Device::column<"debug_enabled">().expression(deviceQuery, "d") }, { "value33", defaultText(connection, "frame", "3E") }, { "value34", defaultText(connection, "version", "2007") }, { "value35", defaultText(connection, "network", "0") }, { "value36", defaultText(connection, "station", "255") }, { "value37", defaultText(connection, "moduleIo", "1023") }, { "value38", defaultText(connection, "multidrop", "0") }, { "value39", defaultText(connection, "monitoringTimer", "16") }, { "value40", defaultText(connection, "destinationNetwork", "0") }, { "value41", defaultText(connection, "destinationNode", "0") }, { "value42", defaultText(connection, "destinationUnit", "0") }, { "value43", defaultText(connection, "sourceNetwork", "0") }, { "value44", defaultText(connection, "sourceNode", "0") }, { "value45", defaultText(connection, "sourceUnit", "0") }, { "value46", defaultText(connection, "wakeupBytes", "4") }, { "value47", defaultText(connection, "writePassword", "") }, { "value48", defaultText(connection, "operatorCode", "") }, { "value49", deviceQuery.cast(config, Type::text) }, { "value50", defaultText(parameters, "parameters", "{}") }, { "value51", deviceQuery.cast(Link::column<"endpoint">().expression(deviceQuery, "l"), Type::text) }, { "value52", Link::column<"name">().expression(deviceQuery, "l") }, { "status", Link::column<"status">().expression(deviceQuery, "l") }, { "value54", Link::column<"debug_enabled">().expression(deviceQuery, "l") }, { "value55", Device::column<"status">().expression(deviceQuery, "d") }, { "enabled", Model::column<"enabled">().expression(deviceQuery, "p") }, { "execution", Link::column<"execution">().expression(deviceQuery, "l") }, { "deleted_at", deviceQuery.binary(deviceQuery.unary(ruvia::db_unary_operator::is_null, Link::column<"deleted_at">().expression(deviceQuery, "l")), Op::and_value, deviceQuery.unary(ruvia::db_unary_operator::is_null, Model::column<"deleted_at">().expression(deviceQuery, "p"))) } });
    detail::joinDeviceModels(deviceQueryBuilder, deviceQuery);
    deviceQueryBuilder.add_order_by(Device::column<"link_id">().expression(deviceQuery, "d")).add_order_by(Device::column<"id">().expression(deviceQuery, "d"));

    return deviceQueryBuilder;
}

template <typename Database, typename Query>
ruvia::task<std::vector<DeviceModel>> loadDeviceModels(Database& database, const Query& deviceQuery) {
    using detail::cell;
    using detail::cellBool;
    using detail::cellInt;
    const auto rows = co_await deviceQuery.get_raw_many();
    std::vector<DeviceModel> devices;
    devices.reserve(rows.size());
    for (const auto& row : rows) {
        DeviceModel model;
        auto& device = model.device;
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
        device.s7Slot = cellInt(row, 18, 1);
        device.s7LocalTsap = cell(row, 19);
        device.s7RemoteTsap = cell(row, 20);
        device.s7HandshakeTimeoutMs = cellInt(row, 21);
        device.s7DirectProbeTimeoutMs = cellInt(row, 22);
        device.s7ProbeMode = cell(row, 23);
        device.readInterval = std::clamp<std::int64_t>(cellInt(row, 24, 300), 1, std::numeric_limits<std::uint32_t>::max());
        device.calculationConfig = cell(row, 49);
        device.parameters = cell(row, 50);
        device.storagePolicy = cell(row, 25);
        device.commandFastReadDuration = std::clamp<std::int64_t>(cellInt(row, 26, 60), 0, 3600);
        device.commandFastReadInterval = std::clamp<std::int64_t>(cellInt(row, 27, 1), 1, 3600);
        device.modbusMergeGap = cellInt(row, 28, 100);
        device.modbusMaxQuantity = cellInt(row, 29, 125);
        device.modelId = cell(row, 30);
        device.sl651ResponseMode = cell(row, 31);
        device.debugEnabled = cellBool(row, 32);
        service::collector::ConnectionConfig::apply(device, { { "mc_frame", cell(row, 33) }, { "dlt645_version", cell(row, 34) }, { "mc_network", cell(row, 35) }, { "mc_station", cell(row, 36) }, { "mc_module_io", cell(row, 37) }, { "mc_multidrop", cell(row, 38) }, { "mc_monitoring_timer", cell(row, 39) }, { "fins_destination_network", cell(row, 40) }, { "fins_destination_node", cell(row, 41) }, { "fins_destination_unit", cell(row, 42) }, { "fins_source_network", cell(row, 43) }, { "fins_source_node", cell(row, 44) }, { "fins_source_unit", cell(row, 45) }, { "dlt645_wakeup_bytes", cell(row, 46) }, { "dlt645_write_password", cell(row, 47) }, { "dlt645_operator_code", cell(row, 48) } });
        const std::string_view endpointJson = row[51].value().value_or(std::string_view{});
        const auto endpoint = ruvia::from_json<EndpointConfig>(endpointJson);

        model.endpoint.name = cell(row, 52);
        model.endpoint.enabled = cell(row, 53) == "enabled";
        model.endpoint.debugEnabled = cellBool(row, 54);
        model.enabled = model.endpoint.enabled && cell(row, 55) == "enabled" && cellBool(row, 56);
        model.execution = cell(row, 57);
        model.available = cellBool(row, 58);
        model.endpoint.transport = configText(endpoint.get<"transport">());
        model.endpoint.interfaceName = configText(endpoint.get<"interfaceName">());
        model.endpoint.mode = configText(endpoint.get<"mode">());
        model.endpoint.ip = configText(endpoint.get<"ip">());
        model.endpoint.port = configInteger(endpoint.get<"port">());
        model.endpoint.baudRate = configInteger(endpoint.get<"baud_rate">(), 9600);
        model.endpoint.dataBits = configInteger(endpoint.get<"data_bits">(), 8);
        model.endpoint.stopBits = configInteger(endpoint.get<"stop_bits">(), 1);
        model.endpoint.parity = configText(endpoint.get<"parity">(), "none");
        decodeModelElements(model);
        model.calculation.config = device.calculationConfig;
        devices.push_back(std::move(model));
    }
    if (devices.empty()) {
        co_return devices;
    }
    using Source = persistence::ExternalIoSourceEntity;
    using Binding = persistence::ExternalIoBindingEntity;
    using Protocol = persistence::ProtocolConfigEntity;
    using Op = ruvia::db_binary_operator;
    ruvia::db_query ioQuery;
    std::vector<ruvia::db_expression> ids;
    std::map<std::string, DeviceModel*, std::less<>> byId;
    for (auto& model : devices) {
        ids.push_back(ioQuery.cast(ioQuery.value(model.device.id), ruvia::db_data_type::uuid));
        byId.emplace(model.device.id, &model);
    }
    auto ioBuilder = database.template repository<Source>().query_builder("s");
    ioBuilder.select({ { "device", ioQuery.cast(Source::column<"device_id">().expression(ioQuery, "s"), ruvia::db_data_type::text) }, { "model", ioQuery.cast(Source::column<"protocol_config_id">().expression(ioQuery, "s"), ruvia::db_data_type::text) }, { "derived", Source::column<"derived_config">().expression(ioQuery, "s") }, { "config", Protocol::column<"config">().expression(ioQuery, "p") }, { "point", ioQuery.cast(Binding::column<"point_id">().expression(ioQuery, "b"), ruvia::db_data_type::text) }, { "kind", Binding::column<"kind">().expression(ioQuery, "b") }, { "channel", Binding::column<"channel">().expression(ioQuery, "b") } })
        .template join<Protocol>(ruvia::db_join_type::inner, "p", ioQuery.binary(Protocol::column<"id">().expression(ioQuery, "p"), Op::equal, Source::column<"protocol_config_id">().expression(ioQuery, "s")))
        .template join<Binding>(ruvia::db_join_type::inner, "b", ioQuery.binary(Binding::column<"device_id">().expression(ioQuery, "b"), Op::equal, Source::column<"device_id">().expression(ioQuery, "s")))
        .where(ioQuery.binary(Source::column<"device_id">().expression(ioQuery, "s"), Op::in, ioQuery.list(ids)))
        .add_order_by(Source::column<"device_id">().expression(ioQuery, "s"))
        .add_order_by(Binding::column<"point_id">().expression(ioQuery, "b"));
    for (const auto& row : co_await ioBuilder.get_raw_many()) {
        auto& model = *byId.at(cell(row, 0));
        model.externalIoModelId = cell(row, 1);
        model.deviceDerivedConfig = cell(row, 2);
        model.externalIoConfig = cell(row, 3);
        model.externalIo.push_back({ .id = cell(row, 4), .channel = cell(row, 6), .kind = cell(row, 5) });
    }
    for (auto& model : devices) {
        if (model.externalIo.empty()) {
            continue;
        }
        const auto io = ruvia::json_value::parse(model.externalIoConfig);
        const auto ioPoints = io.get<ruvia::json_value>("points");
        if (!ioPoints) {
            throw std::runtime_error("external IO model has no points");
        }
        (void)ioPoints->for_each_element([&](const ruvia::json_value& point) {
            const auto id = point.get<ruvia::string>("id");
            for (auto& bound : model.externalIo) {
                if (id && bound.id == id->view()) {
                    const auto name = point.get<ruvia::string>("name");
                    const auto unit = point.get<ruvia::string>("unit");
                    bound.name = name ? std::string(name->view()) : bound.id;
                    bound.unit = bound.kind == "AI" ? "uA" : unit ? std::string(unit->view())
                                                                  : std::string{};
                    bound.visible = static_cast<bool>(point.get<ruvia::bool_value>("visible").value_or(ruvia::bool_value{ true }));
                    model.points.push_back({ bound.id, bound.name, bound.unit });
                }
            }
            return true;
        });
        model.calculation.config = model.deviceDerivedConfig;
        model.calculation.sources = { model.device.calculationConfig, model.externalIoConfig };
        const std::array sources{ ruvia::json_value::parse(model.device.calculationConfig), ruvia::json_value::parse(model.externalIoConfig) };
        const auto combined = service::common::orderDerivedPoints(ruvia::json_value::parse(model.deviceDerivedConfig), sources);
        for (const auto& point : combined) {
            if (std::ranges::none_of(model.points, [&](const auto& existing) {
                    return existing.id == point.id;
                })) {
                model.points.push_back({ point.id, point.name, point.unit });
            }
        }
    }
    co_return devices;
}

// Collector 执行边界：停用设备/链路/模型不参与采集；Edge 则必须下发 enabled=false。
template <typename Database>
ruvia::task<std::vector<service::collector::DeviceDefinition>> loadDeviceDefinitions(Database& db, std::string_view deviceId = {}) {
    using Device = persistence::DeviceEntity;
    using Link = persistence::LinkEntity;
    using Model = persistence::DeviceModelEntity;
    using Op = ruvia::db_binary_operator;
    ruvia::db_query queryExpressions;
    auto query = deviceModelsQuery(db);
    query.and_where(queryExpressions.binary(Link::column<"execution">().expression(queryExpressions, "l"), Op::equal, queryExpressions.value("collector")))
        .and_where(queryExpressions.binary(Device::column<"status">().expression(queryExpressions, "d"), Op::equal, queryExpressions.value("enabled")))
        .and_where(queryExpressions.binary(Link::column<"status">().expression(queryExpressions, "l"), Op::equal, queryExpressions.value("enabled")))
        .and_where(Model::column<"enabled">().expression(queryExpressions, "p"))
        .and_where(queryExpressions.unary(ruvia::db_unary_operator::is_null, Link::column<"deleted_at">().expression(queryExpressions, "l")))
        .and_where(queryExpressions.unary(ruvia::db_unary_operator::is_null, Model::column<"deleted_at">().expression(queryExpressions, "p")));
    if (!deviceId.empty()) {
        query.and_where(queryExpressions.binary(Device::column<"id">().expression(queryExpressions, "d"), Op::equal, queryExpressions.cast(queryExpressions.value(deviceId), ruvia::db_data_type::uuid)));
    }
    auto models = co_await loadDeviceModels(db, query);
    std::vector<service::collector::DeviceDefinition> devices;
    devices.reserve(models.size());
    for (auto& model : models) {
        devices.push_back(std::move(model.device));
    }
    co_return devices;
}

// Collector 运行快照：collector 链路与目标、启用的 collector 设备，以及全部未删除设备的实时点位读模型。
template <typename Database>
ruvia::task<service::collector::RuntimeSnapshot> loadRuntimeSnapshot(Database& db) {
    using detail::cell;
    using detail::cellBool;
    using detail::cellInt;
    service::collector::RuntimeSnapshot snapshot;

    using Op = ruvia::db_binary_operator;
    using Type = ruvia::db_data_type;
    ruvia::db_query linkQuery;
    const auto endpointText = [&](std::string_view key) {
        return linkQuery.binary(linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"endpoint">()), Op::json_get_text, linkQuery.value(key));
    };
    auto linkQueryBuilder = db.template repository<service::configuration::persistence::LinkEntity>().query_builder();
    linkQueryBuilder.select({ { "id", linkQuery.cast(linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"id">()), Type::text) }, { "name", linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"name">()) }, { "value2", endpointText("mode") }, { "protocol", linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"protocol">()) }, { "value4", linkQuery.coalesce({ endpointText("ip"), linkQuery.value("") }) }, { "value5", linkQuery.coalesce({ linkQuery.null_if(endpointText("port"), linkQuery.value("")), linkQuery.value("0") }) }, { "status", linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"status">()) }, { "debug_enabled", linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"debug_enabled">()) } }).and_where(linkQuery.unary(ruvia::db_unary_operator::is_null, linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"deleted_at">()))).and_where(linkQuery.binary(linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"execution">()), Op::equal, linkQuery.value("collector"))).add_order_by(linkQuery.column(service::configuration::persistence::LinkEntity::column_name<"id">()));
    const auto links = co_await linkQueryBuilder.get_raw_many();
    for (const auto& row : links) {
        service::collector::LinkDefinition link;
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
    auto targetQueryBuilder = db.template repository<service::configuration::persistence::LinkEntity>().query_builder("l");
    targetQueryBuilder.select({ { "id", targetQuery.cast(targetQuery.column(service::configuration::persistence::LinkEntity::column_name<"id">(), "l"), Type::text) }, { "value1", targetText("id") }, { "value2", targetText("name") }, { "value3", targetText("ip") }, { "value4", targetQuery.coalesce({ targetQuery.null_if(targetText("port"), targetQuery.value("")), targetQuery.value("0") }) }, { "value5", targetQuery.coalesce({ targetText("status"), targetQuery.value("enabled") }) }, { "value6", targetQuery.coalesce({ targetText("mqtt"), targetQuery.value("{}") }) } }).join_function(ruvia::db_join_type::cross, targetQuery.call("jsonb_array_elements", { targetQuery.coalesce({ targetQuery.binary(targetQuery.column(service::configuration::persistence::LinkEntity::column_name<"endpoint">(), "l"), Op::json_get, targetQuery.value("targets")), targetQuery.cast(targetQuery.value("[]"), Type::jsonb) }) }), "target", {}, { .lateral_ = true }).and_where(targetQuery.unary(ruvia::db_unary_operator::is_null, targetQuery.column(service::configuration::persistence::LinkEntity::column_name<"deleted_at">(), "l"))).and_where(targetQuery.binary(targetQuery.column(service::configuration::persistence::LinkEntity::column_name<"execution">(), "l"), Op::equal, targetQuery.value("collector"))).and_where(targetQuery.binary(targetQuery.binary(targetQuery.column(service::configuration::persistence::LinkEntity::column_name<"endpoint">(), "l"), Op::json_get_text, targetQuery.value("mode")), Op::equal, targetQuery.value("TCP Client"))).add_order_by(targetQuery.column(service::configuration::persistence::LinkEntity::column_name<"id">(), "l"));
    const auto targets = co_await targetQueryBuilder.get_raw_many();
    for (const auto& row : targets) {
        const auto linkId = cell(row, 0);
        const auto link = linkIndexes.find(linkId);
        if (link == linkIndexes.end()) {
            continue;
        }
        service::collector::LinkTargetDefinition target;
        target.id = cell(row, 1);
        target.name = cell(row, 2);
        target.ip = cell(row, 3);
        target.port = detail::cellPort(row, 4);
        target.status = cell(row, 5);
        target.mqttConfig = cell(row, 6);
        snapshot.links[link->second].targets.push_back(std::move(target));
    }

    // 同一次模型投影分出采集执行上下文与完整实时读模型；不再另行展开协议 JSON。
    auto models = co_await loadDeviceModels(db, deviceModelsQuery(db));
    snapshot.devices.reserve(models.size());
    snapshot.realtimeDevices.reserve(models.size());
    for (auto& model : models) {
        service::collector::RealtimeDeviceDefinition realtime;
        realtime.id = model.device.id;
        realtime.code = model.device.code;
        realtime.name = model.device.name;
        realtime.points = std::move(model.points);
        snapshot.realtimeDevices.push_back(std::move(realtime));
        if (model.execution == "collector" && model.available && model.enabled) {
            snapshot.devices.push_back(std::move(model.device));
        }
    }
    std::ranges::sort(snapshot.realtimeDevices, {}, &service::collector::RealtimeDeviceDefinition::id);

    co_return snapshot;
}

} // namespace service::configuration

namespace service::configuration {

class ConfigurationService final {
  public:
    template <typename Context>
    static ruvia::task<void> initialize(Context& context) {
        const auto& identity = context.template worker_state<std::optional<service::common::WorkerIdentity>>().value();
        co_await service::utils::initializeOnce(
            context,
            service::message::lock::kConfigurationStartup,
            service::message::configuration::startupProjectionKey(identity.instanceId),
            [](Context& owner) -> ruvia::task<void> {
                (void)co_await project(owner);
            }
        );
    }

    template <typename Context>
    static ruvia::task<std::string> project(Context& context, bool notify = false) {
        auto transaction = co_await context.db().begin_transaction();
        // Every Service Worker shares this transaction-scoped lock. The snapshot is loaded only
        // after earlier projections finish, so a slower request can never overwrite a newer DB
        // state.
        co_await transaction.advisory_lock(service::message::lock::kConfigurationProjection);
        auto snapshot =
            co_await service::configuration::loadRuntimeSnapshot(transaction);
        auto version =
            co_await service::collector::config::project(context.redis(), snapshot, context.template worker_state<service::common::UuidV7State>());
        co_await transaction.commit();
        if (notify) {
            co_await service::live::publish(context.redis(), context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, service::message::live::topic::kRuntimeConfig);
        }
        co_return version;
    }

    // Serialize retirement with publication, so an active generation cannot be
    // removed while another Worker switches the pointer. Return the next real
    // inactive generation deadline instead of scheduling a periodic scan.
    template <typename Context>
    static ruvia::task<std::optional<std::int64_t>> retireSnapshots(Context& context) {
        auto transaction = co_await context.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kConfigurationProjection);
        const auto redis = context.redis();
        const auto active = co_await redis.get(service::message::projection::kRuntimeActiveVersionKey);
        if (!active) {
            co_await transaction.commit();
            co_return std::nullopt;
        }
        co_await service::collector::config::eraseExpiredSnapshots(redis, service::message::utcNowMilliseconds(), *active);
        const auto generations = co_await redis.command("ZRANGE", service::message::projection::kRuntimeVersionsKey, "0", "1", "WITHSCORES");
        if (generations.kind() != ruvia::redis_value::kind_type::array) {
            service::utils::redis::throwValue("runtime snapshot deadlines", generations);
        }
        std::optional<std::int64_t> deadline;
        const auto values = generations.array();
        for (std::size_t index = 0; index + 1 < values.size(); index += 2) {
            if (values[index].kind() != ruvia::redis_value::kind_type::string ||
                values[index + 1].kind() != ruvia::redis_value::kind_type::string) {
                service::utils::redis::throwValue("runtime snapshot deadline entry", generations);
            }
            if (values[index].string() == *active) {
                continue;
            }
            const auto text = values[index + 1].string();
            std::int64_t createdAt{};
            const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), createdAt);
            if (error != std::errc{} || end != text.data() + text.size()) {
                throw std::runtime_error("invalid runtime snapshot deadline");
            }
            deadline = createdAt + service::collector::config::kSnapshotGraceMilliseconds;
            break;
        }
        co_await transaction.commit();
        co_return deadline;
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
            (void)co_await service::messaging::stream::publishAndWake(
                context.redis(),
                context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                service::message::alert::kMetadataChangesStream,
                fields,
                std::nullopt,
                service::message::WorkerStreamTask::FreshnessAlerts,
                10000,
                context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId
            );
        }
        co_await service::messaging::idempotency::markProcessed(context, consumer, eventIds);
        co_return projectionRequired;
    }
};

} // namespace service::configuration
