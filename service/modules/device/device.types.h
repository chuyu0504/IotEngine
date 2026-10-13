#pragma once
#include "service/modules/link/link.types.h"

#include "service/common/uuid.h"

#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>

#include <ruvia/web/validation.h>

namespace service::device {

inline constexpr auto kDefaultPage = RUVIA_DEFAULT(1){};
inline constexpr auto kDefaultPageSize = RUVIA_DEFAULT(20){};

RUVIA_MODEL(DeviceDebugPacketDto, RUVIA_REQUIRED_FIELD_NAME("acquisition_id", acquisitionId, ruvia::string), RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD(direction, ruvia::string), RUVIA_OPTIONAL_FIELD(source, ruvia::string), RUVIA_OPTIONAL_FIELD(address, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_node_id", edgeNodeId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_node_name", edgeNodeName, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("payload_hex", payloadHex, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("time_ms", timeMs, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("transport_status", transportStatus, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("response_status", responseStatus, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("parse_status", parseStatus, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("revision", revision, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("reply_to_packet_id", replyToPacketId, ruvia::string), RUVIA_OPTIONAL_FIELD(reason, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("parsed_json", parsedJson, ruvia::string));
RUVIA_MODEL(DeviceDebugAcquisitionDto, RUVIA_REQUIRED_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("started_at_ms", startedAtMs, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("finished_at_ms", finishedAtMs, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("last_packet_at_ms", lastPacketAtMs, ruvia::string), RUVIA_OPTIONAL_FIELD(state, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD(packets, ruvia::boxed_array<DeviceDebugPacketDto>));

RUVIA_MODEL(DeviceEventsQuery, RUVIA_OPTIONAL_FIELD_NAME("debugDeviceId", debugDeviceId, ruvia::string, RUVIA_CUSTOM("调试设备 ID 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD_NAME("commandIds", commandIds, ruvia::string, RUVIA_MIN(1, "至少指定一条指令"), RUVIA_MAX(9471, "最多指定 256 条指令")));
RUVIA_MODEL(DeviceDebugBody, RUVIA_REQUIRED_FIELD(enabled, ruvia::bool_value));

enum class DeviceAccessLevel : std::int64_t {
    none = 0,
    view = 1,
    operate = 2,
    owner = 4,
};

struct DeviceActor final {
    std::string userId;
    std::string departmentId;
    bool superadmin{};
    bool canEdit{};
    bool canDelete{};
    bool canShare{};
    bool canCommand{};
    bool canGroupShare{};
};

struct DeviceAccessDecision final {
    DeviceActor actor;
    DeviceAccessLevel level{ DeviceAccessLevel::none };
};

struct DeviceCapabilities final {
    bool canEdit{};
    bool canDelete{};
    bool canShare{};
    bool canCommand{};
    std::string_view accessLevel{ "none" };
};

// 设备时区格式校验：[+-]HH:MM，范围 -14:00 .. +14:00（空视为合法，可选字段）。
inline bool isDeviceTimezone(const ruvia::string& value) {
    const std::string_view v = value.view();
    if (v.empty()) {
        return true;
    }
    if (v.size() != 6 || (v[0] != '+' && v[0] != '-') || v[3] != ':') {
        return false;
    }
    for (const std::size_t i : { std::size_t{ 1 }, std::size_t{ 2 }, std::size_t{ 4 }, std::size_t{ 5 } }) {
        if (!std::isdigit(static_cast<unsigned char>(v[i]))) {
            return false;
        }
    }
    const int hh = (v[1] - '0') * 10 + (v[2] - '0');
    const int mm = (v[4] - '0') * 10 + (v[5] - '0');
    if (hh == 14) {
        return mm == 0;
    }
    return hh <= 13 && mm <= 59;
}

RUVIA_MODEL(DeviceIdParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

// ===== 设备分享 =====

RUVIA_MODEL(DeviceShareBodyItem, RUVIA_OPTIONAL_FIELD_NAME("subject_type", subjectType, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("subject_id", subjectId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("access_level", accessLevel, ruvia::string));

RUVIA_MODEL(ReplaceDeviceSharesBody, RUVIA_REQUIRED_FIELD(shares, ruvia::array<DeviceShareBodyItem>, RUVIA_MAX(500, "单次最多设置 500 个分享对象")));

RUVIA_MODEL(DeviceShareItemDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("subject_type", subjectType, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("subject_id", subjectId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("subject_name", subjectName, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("access_level", accessLevel, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("source_type", sourceType, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("source_group_id", sourceGroupId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("source_group_name", sourceGroupName, ruvia::string), RUVIA_OPTIONAL_FIELD(inherited, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("created_at", createdAt, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("updated_at", updatedAt, ruvia::string));

RUVIA_MODEL(DeviceShareTargetDto, RUVIA_OPTIONAL_FIELD_NAME("subject_type", subjectType, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("subject_id", subjectId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("subject_name", subjectName, ruvia::string));

// ===== 设备写侧 Body =====

RUVIA_MODEL(DevicePacketBody, RUVIA_OPTIONAL_FIELD(mode, ruvia::string), RUVIA_OPTIONAL_FIELD(content, ruvia::string));

RUVIA_MODEL(CreateDeviceBody, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "设备名称长度必须在 1 - 100 之间"), RUVIA_MAX(100, "设备名称长度必须在 1 - 100 之间")), RUVIA_REQUIRED_FIELD_NAME("device_code", deviceCode, ruvia::string, RUVIA_MIN(1, "设备编码长度必须在 1 - 100 之间"), RUVIA_MAX(100, "设备编码长度必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD_NAME("link_id", linkId, ruvia::string, RUVIA_CUSTOM("链路必须是 UUID", service::common::isOptionalUuidField)), RUVIA_OPTIONAL_FIELD_NAME("target_id", targetId, ruvia::string), RUVIA_REQUIRED_FIELD_NAME("protocol_config_id", protocolConfigId, ruvia::string, RUVIA_CUSTOM("链路和设备类型必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD_NAME("group_id", groupId, ruvia::string, RUVIA_CUSTOM("设备分组必须是 UUID", service::common::isOptionalUuidField)), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("设备参数无效", "enabled", "disabled")), RUVIA_OPTIONAL_FIELD_NAME("online_timeout", onlineTimeout, ruvia::int64, RUVIA_MIN(1, "设备参数无效"), RUVIA_MAX(86400, "设备参数无效")), RUVIA_OPTIONAL_FIELD_NAME("remote_control", remoteControl, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("modbus_mode", modbusMode, ruvia::string, RUVIA_ONE_OF("设备参数无效", "TCP", "RTU")), RUVIA_OPTIONAL_FIELD_NAME("slave_id", slaveId, ruvia::int64, RUVIA_MIN(1, "设备参数无效"), RUVIA_MAX(247, "设备参数无效")), RUVIA_OPTIONAL_FIELD(timezone, ruvia::string, RUVIA_CUSTOM("设备参数无效", isDeviceTimezone)), RUVIA_OPTIONAL_FIELD(heartbeat, DevicePacketBody), RUVIA_OPTIONAL_FIELD(registration, DevicePacketBody), RUVIA_OPTIONAL_FIELD(remark, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_connection", edgeConnection, service::link::SaveLinkBody));

RUVIA_MODEL(UpdateDeviceBody, RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(1, "设备名称长度必须在 1 - 100 之间"), RUVIA_MAX(100, "设备名称长度必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD_NAME("device_code", deviceCode, ruvia::string, RUVIA_MIN(1, "设备编码长度必须在 1 - 100 之间"), RUVIA_MAX(100, "设备编码长度必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD_NAME("link_id", linkId, ruvia::string, RUVIA_CUSTOM("链路必须是 UUID", service::common::isOptionalUuidField)), RUVIA_OPTIONAL_FIELD_NAME("target_id", targetId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("protocol_config_id", protocolConfigId, ruvia::string, RUVIA_CUSTOM("链路和设备类型必须是 UUID", service::common::isOptionalUuidField)), RUVIA_OPTIONAL_FIELD_NAME("group_id", groupId, ruvia::string, RUVIA_CUSTOM("设备分组必须是 UUID", service::common::isOptionalUuidField)), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("设备参数无效", "enabled", "disabled")), RUVIA_OPTIONAL_FIELD_NAME("online_timeout", onlineTimeout, ruvia::int64, RUVIA_MIN(1, "设备参数无效"), RUVIA_MAX(86400, "设备参数无效")), RUVIA_OPTIONAL_FIELD_NAME("remote_control", remoteControl, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("modbus_mode", modbusMode, ruvia::string, RUVIA_ONE_OF("设备参数无效", "TCP", "RTU")), RUVIA_OPTIONAL_FIELD_NAME("slave_id", slaveId, ruvia::int64, RUVIA_MIN(1, "设备参数无效"), RUVIA_MAX(247, "设备参数无效")), RUVIA_OPTIONAL_FIELD(timezone, ruvia::string, RUVIA_CUSTOM("设备参数无效", isDeviceTimezone)), RUVIA_OPTIONAL_FIELD(heartbeat, DevicePacketBody), RUVIA_OPTIONAL_FIELD(registration, DevicePacketBody), RUVIA_OPTIONAL_FIELD(remark, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_connection", edgeConnection, service::link::SaveLinkBody));

// ===== 设备读侧 DTO =====

// 心跳包 / 注册包（透传 DB jsonb 的 mode + 可选 content）
RUVIA_MODEL(DevicePacketDto, RUVIA_OPTIONAL_FIELD(mode, ruvia::string), RUVIA_OPTIONAL_FIELD(content, ruvia::string));

RUVIA_MODEL(DeviceCommandElementBody, RUVIA_OPTIONAL_FIELD_NAME("elementId", elementId, ruvia::string), RUVIA_OPTIONAL_FIELD(value, ruvia::string));

RUVIA_MODEL(DeviceCommandBody, RUVIA_OPTIONAL_FIELD(mqttMessageId, ruvia::string, RUVIA_CUSTOM("控制消息 ID 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD_NAME("idempotency_key", idempotencyKey, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("deviceId", deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD(elements, ruvia::array<DeviceCommandElementBody>, RUVIA_MIN(1, "请至少选择一个下发要素"), RUVIA_MAX(256, "单次最多下发 256 个要素")));

RUVIA_MODEL(DeviceCommandCreateDto, RUVIA_OPTIONAL_FIELD_NAME("command_ids", commandIds, ruvia::boxed_array<ruvia::string>), RUVIA_OPTIONAL_FIELD(status, ruvia::string));

RUVIA_MODEL(DeviceCommandActualValueDto, RUVIA_OPTIONAL_FIELD_NAME("element_id", elementId, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD(kind, ruvia::string), RUVIA_OPTIONAL_FIELD(value, ruvia::string), RUVIA_OPTIONAL_FIELD(unit, ruvia::string));

RUVIA_MODEL(DeviceCommandStatusDto, RUVIA_OPTIONAL_FIELD_NAME("command_id", commandId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_id", deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_code", deviceCode, ruvia::string), RUVIA_OPTIONAL_FIELD(protocol, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD(reason, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("created_at_ms", createdAtMs, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("completed_at_ms", completedAtMs, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("actual_values", actualValues, ruvia::boxed_array<DeviceCommandActualValueDto>));

RUVIA_MODEL(DeviceCommandStatusesDto, RUVIA_OPTIONAL_FIELD(complete, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(statuses, ruvia::boxed_array<DeviceCommandStatusDto>));

RUVIA_MODEL(DeviceElementDto, RUVIA_OPTIONAL_FIELD(visible, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(quality, ruvia::string), RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD(value, ruvia::string), RUVIA_OPTIONAL_FIELD(unit, ruvia::string), RUVIA_OPTIONAL_FIELD(scale, ruvia::double_value), RUVIA_OPTIONAL_FIELD(decimals, ruvia::int64), RUVIA_OPTIONAL_FIELD(group, ruvia::string), RUVIA_OPTIONAL_FIELD(encode, ruvia::string));

RUVIA_MODEL(DeviceCommandOptionDto, RUVIA_OPTIONAL_FIELD(label, ruvia::string), RUVIA_OPTIONAL_FIELD(value, ruvia::string));

RUVIA_MODEL(DeviceCommandOperationElementDto, RUVIA_OPTIONAL_FIELD(required, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("elementId", elementId, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD(value, ruvia::string), RUVIA_OPTIONAL_FIELD(unit, ruvia::string), RUVIA_OPTIONAL_FIELD(options, ruvia::boxed_array<DeviceCommandOptionDto>), RUVIA_OPTIONAL_FIELD_NAME("registerType", registerType, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("dataType", dataType, ruvia::string), RUVIA_OPTIONAL_FIELD(size, ruvia::int64), RUVIA_OPTIONAL_FIELD(encode, ruvia::string), RUVIA_OPTIONAL_FIELD(length, ruvia::int64), RUVIA_OPTIONAL_FIELD(digits, ruvia::int64));

RUVIA_MODEL(DeviceCommandOperationDto, RUVIA_OPTIONAL_FIELD(mqttMessageId, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD(elements, ruvia::boxed_array<DeviceCommandOperationElementDto>));

RUVIA_MODEL(EdgeStatusDto, RUVIA_OPTIONAL_FIELD(state, ruvia::string), RUVIA_OPTIONAL_FIELD(reason, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("clientCount", clientCount, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("lastActivityAt", lastActivityAt, ruvia::string));

RUVIA_MODEL(DeviceItemDto, RUVIA_OPTIONAL_FIELD_NAME("debug_enabled", debugEnabled, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("link_debug_enabled", linkDebugEnabled, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_code", deviceCode, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("link_id", linkId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_node_id", edgeNodeId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_node_name", edgeNodeName, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_node_imei", edgeNodeImei, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_transport", edgeTransport, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_interface", edgeInterface, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_mode", edgeMode, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_ip", edgeIp, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_port", edgePort, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("serial_baud_rate", serialBaudRate, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("serial_data_bits", serialDataBits, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("serial_stop_bits", serialStopBits, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("serial_parity", serialParity, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("target_id", targetId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("protocol_config_id", protocolConfigId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("group_id", groupId, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("online_timeout", onlineTimeout, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("remote_control", remoteControl, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("modbus_mode", modbusMode, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("slave_id", slaveId, ruvia::int64), RUVIA_OPTIONAL_FIELD(timezone, ruvia::string), RUVIA_OPTIONAL_FIELD(heartbeat, DevicePacketDto), RUVIA_OPTIONAL_FIELD(registration, DevicePacketDto), RUVIA_OPTIONAL_FIELD(remark, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("created_by", createdBy, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("created_at", createdAt, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("updated_at", updatedAt, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("link_name", linkName, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("link_mode", linkMode, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("link_protocol", linkProtocol, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("protocol_name", protocolName, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("protocol_type", protocolType, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("read_interval", readInterval, ruvia::double_value), RUVIA_OPTIONAL_FIELD_NAME("storage_policy", storagePolicy, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("element_count", elementCount, ruvia::int64), RUVIA_OPTIONAL_FIELD(connected, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("connectionState", connectionState, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edgeStatus", edgeStatus, EdgeStatusDto), RUVIA_OPTIONAL_FIELD_NAME("reportTime", reportTime, ruvia::string), RUVIA_OPTIONAL_FIELD(elements, ruvia::boxed_array<DeviceElementDto>), RUVIA_OPTIONAL_FIELD_NAME("commandOperations", commandOperations, ruvia::boxed_array<DeviceCommandOperationDto>), RUVIA_OPTIONAL_FIELD_NAME("can_edit", canEdit, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_delete", canDelete, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_share", canShare, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_command", canCommand, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("access_level", accessLevel, ruvia::string));

RUVIA_MODEL(DeviceOptionDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_code", deviceCode, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("can_edit", canEdit, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_delete", canDelete, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_share", canShare, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_command", canCommand, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("access_level", accessLevel, ruvia::string));

RUVIA_MODEL(DeviceRealtimeDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("device_code", deviceCode, ruvia::string), RUVIA_OPTIONAL_FIELD(connected, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("connectionState", connectionState, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("reportTime", reportTime, ruvia::string), RUVIA_OPTIONAL_FIELD(elements, ruvia::boxed_array<DeviceElementDto>), RUVIA_OPTIONAL_FIELD_NAME("edge_node_id", edgeNodeId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edge_transport", edgeTransport, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("edgeStatus", edgeStatus, EdgeStatusDto), RUVIA_OPTIONAL_FIELD_NAME("can_edit", canEdit, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_delete", canDelete, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_share", canShare, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("can_command", canCommand, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("access_level", accessLevel, ruvia::string));

RUVIA_MODEL(DevicePageDataDto, RUVIA_OPTIONAL_FIELD(list, ruvia::boxed_array<DeviceItemDto>), RUVIA_OPTIONAL_FIELD(total, ruvia::int64));
RUVIA_MODEL(DeviceRealtimePageDto, RUVIA_OPTIONAL_FIELD(list, ruvia::boxed_array<DeviceRealtimeDto>), RUVIA_OPTIONAL_FIELD(total, ruvia::int64));

RUVIA_MODEL(DeviceCommandCreateResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, DeviceCommandCreateDto));

// ===== 设备分组（合并入 device 模块，子资源）=====

RUVIA_MODEL(CreateDeviceGroupBody, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "分组名称长度必须在 1 - 100 之间"), RUVIA_MAX(100, "分组名称长度必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD_NAME("parent_id", parentId, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("设备分组参数无效", "enabled", "disabled")), RUVIA_OPTIONAL_FIELD_NAME("sort_order", sortOrder, ruvia::int64, RUVIA_MIN(0, "设备分组参数无效")), RUVIA_OPTIONAL_FIELD(remark, ruvia::string));

RUVIA_MODEL(UpdateDeviceGroupBody, RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(1, "分组名称长度必须在 1 - 100 之间"), RUVIA_MAX(100, "分组名称长度必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD_NAME("parent_id", parentId, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("设备分组参数无效", "enabled", "disabled")), RUVIA_OPTIONAL_FIELD_NAME("sort_order", sortOrder, ruvia::int64, RUVIA_MIN(0, "设备分组参数无效")), RUVIA_OPTIONAL_FIELD(remark, ruvia::string));

RUVIA_MODEL(DeviceGroupItemDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("parent_id", parentId, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("sort_order", sortOrder, ruvia::int64), RUVIA_OPTIONAL_FIELD(remark, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("deviceCount", deviceCount, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("created_at", createdAt, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("updated_at", updatedAt, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("can_share", canShare, ruvia::bool_value));

RUVIA_MODEL(DeviceHistoryQuery, RUVIA_REQUIRED_FIELD(startTime, ruvia::string, RUVIA_MIN(1, "startTime 不能为空")), RUVIA_REQUIRED_FIELD(endTime, ruvia::string, RUVIA_MIN(1, "endTime 不能为空")), RUVIA_OPTIONAL_FIELD(page, ruvia::int64, decltype(kDefaultPage), RUVIA_MIN(1, "page 必须大于 0")), RUVIA_OPTIONAL_FIELD(pageSize, ruvia::int64, decltype(kDefaultPageSize), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")));

RUVIA_MODEL(DeviceDebugPacketsResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, ruvia::boxed_array<DeviceDebugAcquisitionDto>));
RUVIA_MODEL(DeviceSharesResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, ruvia::boxed_array<DeviceShareItemDto>));
RUVIA_MODEL(DeviceShareTargetsResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, ruvia::boxed_array<DeviceShareTargetDto>));
RUVIA_MODEL(DevicePageResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, DevicePageDataDto));
RUVIA_MODEL(DeviceDetailResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, DeviceItemDto));
RUVIA_MODEL(DeviceCommandStatusesResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, DeviceCommandStatusesDto));
RUVIA_MODEL(DeviceOptionsResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, ruvia::boxed_array<DeviceOptionDto>));
RUVIA_MODEL(DeviceRealtimeResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, DeviceRealtimePageDto));
RUVIA_MODEL(DeviceGroupListResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, ruvia::boxed_array<DeviceGroupItemDto>));

} // namespace service::device
