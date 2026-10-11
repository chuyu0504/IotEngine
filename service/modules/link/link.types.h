#pragma once

#include "service/common/uuid.h"

#include <ruvia/web/validation.h>

namespace service::link {
RUVIA_MODEL(LinkDebugPacketDto, RUVIA_REQUIRED_FIELD_NAME("acquisition_id", acquisitionId, ruvia::string), RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD(direction, ruvia::string), RUVIA_OPTIONAL_FIELD(source, ruvia::string), RUVIA_OPTIONAL_FIELD(address, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_node_id", edgeNodeId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_node_name", edgeNodeName, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("payload_hex", payloadHex, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("time_ms", timeMs, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("transport_status", transportStatus, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("response_status", responseStatus, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("parse_status", parseStatus, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("revision", revision, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("reply_to_packet_id", replyToPacketId, ruvia::string), RUVIA_OPTIONAL_FIELD(reason, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("parsed_json", parsedJson, ruvia::string));
RUVIA_MODEL(LinkDebugAcquisitionDto, RUVIA_REQUIRED_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("started_at_ms", startedAtMs, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("finished_at_ms", finishedAtMs, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("last_packet_at_ms", lastPacketAtMs, ruvia::string), RUVIA_OPTIONAL_FIELD(state, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD(packets, ruvia::boxed_array<LinkDebugPacketDto>));

RUVIA_MODEL(LinkDebugBody, RUVIA_REQUIRED_FIELD(enabled, ruvia::bool_value));

RUVIA_MODEL(MqttConnectionBody,
    RUVIA_REQUIRED_FIELD(clientId, ruvia::string, RUVIA_MIN(1, "MQTT Client ID 不能为空"), RUVIA_MAX(128, "MQTT Client ID 过长")),
    RUVIA_OPTIONAL_FIELD(username, ruvia::string, RUVIA_MAX(1024, "MQTT 用户名过长")),
    RUVIA_OPTIONAL_FIELD(password, ruvia::string, RUVIA_MAX(1024, "MQTT 密码过长")),
    RUVIA_OPTIONAL_FIELD(keepAliveSeconds, ruvia::int64, RUVIA_DEFAULT(60), RUVIA_MIN(1, "MQTT 保活时间必须大于 0"), RUVIA_MAX(65535, "MQTT 保活时间过长")));

RUVIA_MODEL(LinkTargetBody, RUVIA_OPTIONAL_FIELD(mqtt, MqttConnectionBody), RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_MAX(64, "目标 ID 不能超过 64 个字符")), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MAX(100, "目标名称不能超过 100 个字符")), RUVIA_REQUIRED_FIELD(ip, ruvia::string, RUVIA_MAX(253, "目标地址不能超过 253 个字符")), RUVIA_REQUIRED_FIELD(port, ruvia::int64, RUVIA_MIN(1, "目标端口必须在 1 - 65535 之间"), RUVIA_MAX(65535, "目标端口必须在 1 - 65535 之间")), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("目标状态无效", "enabled", "disabled")));

RUVIA_MODEL(LinkEndpointBody, RUVIA_OPTIONAL_FIELD(transport, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("interface", interfaceName, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("baud_rate", baudRate, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("data_bits", dataBits, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("stop_bits", stopBits, ruvia::int64), RUVIA_OPTIONAL_FIELD(parity, ruvia::string), RUVIA_OPTIONAL_FIELD(rs485, ruvia::bool_value),

                    RUVIA_OPTIONAL_FIELD(mode, ruvia::string),
                    RUVIA_OPTIONAL_FIELD(ip, ruvia::string),
                    RUVIA_OPTIONAL_FIELD(port, ruvia::int64),
                    RUVIA_OPTIONAL_FIELD(targets, ruvia::array<LinkTargetBody>));

RUVIA_MODEL(SaveLinkBody, RUVIA_OPTIONAL_FIELD(execution, ruvia::string, RUVIA_ONE_OF("采集位置无效", "collector", "edge")), RUVIA_OPTIONAL_FIELD_NAME("edge_node_id", edgeNodeId, ruvia::string), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "链路名称不能为空"), RUVIA_MAX(100, "链路名称不能超过 100 个字符")), RUVIA_REQUIRED_FIELD(protocol, ruvia::string, RUVIA_ONE_OF("协议无效", "SL651", "Modbus", "S7", "MC", "FINS", "DLT645", "MQTT")), RUVIA_REQUIRED_FIELD(endpoint, LinkEndpointBody), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("状态无效", "enabled", "disabled")));

RUVIA_MODEL(LinkEventsQuery, RUVIA_OPTIONAL_FIELD_NAME("debugLinkId", debugLinkId, ruvia::string, RUVIA_CUSTOM("调试链路 ID 必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(LinkListQuery, RUVIA_OPTIONAL_FIELD(page, ruvia::int64, RUVIA_DEFAULT(1), RUVIA_MIN(1, "page 必须大于 0")), RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64, RUVIA_DEFAULT(10), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD(keyword, ruvia::string), RUVIA_OPTIONAL_FIELD(mode, ruvia::string, RUVIA_ONE_OF("链路模式无效", "TCP Server", "TCP Client")), RUVIA_OPTIONAL_FIELD(protocol, ruvia::string, RUVIA_ONE_OF("协议无效", "SL651", "Modbus", "S7", "MC", "FINS", "DLT645", "MQTT")), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("状态无效", "enabled", "disabled")));

RUVIA_MODEL(LinkIdParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(RuntimeDto, RUVIA_OPTIONAL_FIELD(state, ruvia::string), RUVIA_OPTIONAL_FIELD(reason, ruvia::string), RUVIA_OPTIONAL_FIELD(error, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("clientCount", clientCount, ruvia::int64), RUVIA_OPTIONAL_FIELD(clients, ruvia::boxed_array<ruvia::string>), RUVIA_OPTIONAL_FIELD_NAME("lastActivityAt", lastActivityAt, ruvia::string));

RUVIA_MODEL(LinkTargetDto, RUVIA_OPTIONAL_FIELD(mqtt, MqttConnectionBody), RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD(ip, ruvia::string), RUVIA_OPTIONAL_FIELD(port, ruvia::int64), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD(runtime, RuntimeDto));

RUVIA_MODEL(LinkEndpointDto, RUVIA_OPTIONAL_FIELD(transport, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("interface", interfaceName, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("baud_rate", baudRate, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("data_bits", dataBits, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("stop_bits", stopBits, ruvia::int64), RUVIA_OPTIONAL_FIELD(parity, ruvia::string), RUVIA_OPTIONAL_FIELD(rs485, ruvia::bool_value),

                     RUVIA_OPTIONAL_FIELD(mode, ruvia::string),
                     RUVIA_OPTIONAL_FIELD(ip, ruvia::string),
                     RUVIA_OPTIONAL_FIELD(port, ruvia::int64),
                     RUVIA_OPTIONAL_FIELD(targets, ruvia::boxed_array<LinkTargetDto>));

RUVIA_MODEL(LinkItemDto, RUVIA_OPTIONAL_FIELD_NAME("debug_enabled", debugEnabled, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(execution, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_node_id", edgeNodeId, ruvia::string), RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD(protocol, ruvia::string), RUVIA_OPTIONAL_FIELD(endpoint, LinkEndpointDto), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD(runtime, RuntimeDto), RUVIA_OPTIONAL_FIELD_NAME("created_by", createdBy, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("created_at", createdAt, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("updated_at", updatedAt, ruvia::string));

RUVIA_MODEL(LinkEnumsDto, RUVIA_OPTIONAL_FIELD(modes, ruvia::boxed_array<ruvia::string>), RUVIA_OPTIONAL_FIELD(protocols, ruvia::boxed_array<ruvia::string>), RUVIA_OPTIONAL_FIELD(statuses, ruvia::boxed_array<ruvia::string>));

RUVIA_MODEL(LinkPageDataDto, RUVIA_OPTIONAL_FIELD(list, ruvia::boxed_array<LinkItemDto>), RUVIA_OPTIONAL_FIELD(total, ruvia::int64), RUVIA_OPTIONAL_FIELD(page, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("totalPages", totalPages, ruvia::int64));

RUVIA_MODEL(PublicIpDto, RUVIA_OPTIONAL_FIELD(ip, ruvia::string));

RUVIA_MODEL(LinkDebugPacketsResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, ruvia::boxed_array<LinkDebugAcquisitionDto>));
RUVIA_MODEL(LinkPageResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, LinkPageDataDto));
RUVIA_MODEL(LinkEnumsResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, LinkEnumsDto));
RUVIA_MODEL(PublicIpResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, PublicIpDto));
} // namespace service::link
