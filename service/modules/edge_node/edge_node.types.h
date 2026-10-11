#pragma once

#include <cctype>

#include "service/common/uuid.h"

#include <string>

#include <ruvia/web/validation.h>

namespace service::edge {

inline bool isUciSectionName(const ruvia::string& value) {
    const auto text = value.view();
    if (text.empty() || text.size() > 15) {
        return false;
    }
    for (const unsigned char character : text) {
        if (!std::isalnum(character) && character != '_') {
            return false;
        }
    }
    return true;
}

inline bool isOptionalNetworkDevice(const ruvia::string& value) {
    const auto text = value.view();
    if (text.size() > 32) {
        return false;
    }
    for (const unsigned char character : text) {
        if (!std::isalnum(character) && character != '_' && character != '-' &&
            character != '.' && character != ':') {
            return false;
        }
    }
    return true;
}

inline bool isEdgeGroupFilter(const ruvia::string& value) {
    return value.view() == "ungrouped" || service::common::isOptionalUuidField(value);
}

RUVIA_MODEL(DebugAuthenticationBody, RUVIA_REQUIRED_FIELD(token, ruvia::string, RUVIA_MIN(1, "令牌不能为空")));

RUVIA_MODEL(EdgeEventsQuery, RUVIA_OPTIONAL_FIELD(dtu, ruvia::bool_value, RUVIA_DEFAULT(false)), RUVIA_OPTIONAL_FIELD(nodeId, ruvia::string, RUVIA_CUSTOM("nodeId 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(logs, ruvia::bool_value, RUVIA_DEFAULT(false)), RUVIA_OPTIONAL_FIELD(vpn, ruvia::bool_value, RUVIA_DEFAULT(false)));

RUVIA_MODEL(EdgeListQuery, RUVIA_OPTIONAL_FIELD(page, ruvia::int64, RUVIA_DEFAULT(1), RUVIA_MIN(1, "page 必须大于 0")), RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64, RUVIA_DEFAULT(20), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD(keyword, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("注册状态无效", "pending", "approved")), RUVIA_OPTIONAL_FIELD_NAME("groupId", groupId, ruvia::string, RUVIA_CUSTOM("节点分组筛选无效", isEdgeGroupFilter)));

RUVIA_MODEL(EdgeIdParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(EnrollmentBody, RUVIA_REQUIRED_FIELD(status, ruvia::string, RUVIA_ONE_OF("注册状态无效", "approved")), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MAX(100, "节点名称不能超过 100 个字符")));

RUVIA_MODEL(NodeNameBody, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MAX(100, "节点名称不能超过 100 个字符")));

RUVIA_MODEL(NodeGroupBody, RUVIA_OPTIONAL_FIELD_NAME("groupId", groupId, ruvia::string, RUVIA_CUSTOM("节点分组 ID 无效", service::common::isOptionalUuidField)));

RUVIA_MODEL(EdgeGroupBody, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "分组名称不能为空"), RUVIA_MAX(100, "分组名称不能超过 100 个字符")), RUVIA_OPTIONAL_FIELD_NAME("parentId", parentId, ruvia::string, RUVIA_CUSTOM("上级分组 ID 无效", service::common::isOptionalUuidField)), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("分组状态无效", "enabled", "disabled")), RUVIA_OPTIONAL_FIELD_NAME("sortOrder", sortOrder, ruvia::int64, RUVIA_DEFAULT(0), RUVIA_MIN(0, "分组排序不能小于 0")), RUVIA_OPTIONAL_FIELD(remark, ruvia::string, RUVIA_MAX(500, "分组备注不能超过 500 个字符")));

RUVIA_MODEL(NetworkInterfaceBody, RUVIA_REQUIRED_FIELD(operation, ruvia::string, RUVIA_ONE_OF("网络操作无效", "upsert", "delete")), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_CUSTOM("逻辑接口名称只能包含字母、数字和下划线", isUciSectionName)), RUVIA_OPTIONAL_FIELD_NAME("previousName", previousName, ruvia::string, RUVIA_CUSTOM("原逻辑接口名称只能包含字母、数字和下划线", isUciSectionName)), RUVIA_OPTIONAL_FIELD(mode, ruvia::string, RUVIA_ONE_OF("地址协议无效", "dhcp", "static")), RUVIA_OPTIONAL_FIELD(device, ruvia::string, RUVIA_CUSTOM("设备名称包含非法字符", isOptionalNetworkDevice)), RUVIA_OPTIONAL_FIELD(bridge, ruvia::bool_value, RUVIA_DEFAULT(false)), RUVIA_OPTIONAL_FIELD_NAME("bridgePorts", bridgePorts, ruvia::array<ruvia::string>, RUVIA_MAX(8, "网桥最多包含 8 个成员")), RUVIA_OPTIONAL_FIELD(ip, ruvia::string, RUVIA_MAX(15, "IPv4 地址格式无效")), RUVIA_OPTIONAL_FIELD_NAME("prefixLength", prefixLength, ruvia::int64, RUVIA_DEFAULT(0), RUVIA_MIN(0, "IPv4 前缀必须在 0 - 30 之间"), RUVIA_MAX(30, "IPv4 前缀必须在 0 - 30 之间")), RUVIA_OPTIONAL_FIELD(gateway, ruvia::string, RUVIA_MAX(15, "网关格式无效")));

RUVIA_MODEL(NetworkBody, RUVIA_REQUIRED_FIELD(interfaces, ruvia::array<NetworkInterfaceBody>, RUVIA_MIN(1, "请至少配置一个逻辑接口"), RUVIA_MAX(8, "单次最多配置 8 个逻辑接口")), RUVIA_OPTIONAL_FIELD_NAME("rollbackTimeoutSec", rollbackTimeoutSec, ruvia::int64, RUVIA_DEFAULT(60), RUVIA_MIN(30, "回滚等待时间必须在 30 - 300 秒之间"), RUVIA_MAX(300, "回滚等待时间必须在 30 - 300 秒之间")));

RUVIA_MODEL(FirmwareUploadBody, RUVIA_REQUIRED_FIELD_NAME("fileName", fileName, ruvia::string, RUVIA_MIN(1, "固件文件名不能为空"), RUVIA_MAX(255, "固件文件名过长")), RUVIA_REQUIRED_FIELD_NAME("sizeBytes", sizeBytes, ruvia::int64, RUVIA_MIN(1, "固件文件不能为空"), RUVIA_MAX(134217728, "固件不能超过 128 MiB")), RUVIA_OPTIONAL_FIELD_NAME("keepSettings", keepSettings, ruvia::bool_value, RUVIA_DEFAULT(true)));
RUVIA_MODEL(FirmwareReuseBody, RUVIA_REQUIRED_FIELD(sha256, ruvia::string, RUVIA_MIN(64, "固件摘要无效"), RUVIA_MAX(64, "固件摘要无效")), RUVIA_REQUIRED_FIELD_NAME("sizeBytes", sizeBytes, ruvia::int64, RUVIA_MIN(1, "固件文件不能为空"), RUVIA_MAX(134217728, "固件不能超过 128 MiB")), RUVIA_OPTIONAL_FIELD_NAME("keepSettings", keepSettings, ruvia::bool_value, RUVIA_DEFAULT(true)));
RUVIA_MODEL(FirmwareReuseResult, RUVIA_OPTIONAL_FIELD(reused, ruvia::bool_value));

RUVIA_MODEL(FirmwareDownloadQuery, RUVIA_REQUIRED_FIELD(token, ruvia::string, RUVIA_MAX(64, "下载凭据无效")));

RUVIA_MODEL(LogsQuery, RUVIA_OPTIONAL_FIELD(limit, ruvia::int64, RUVIA_DEFAULT(48), RUVIA_MIN(1, "日志条数必须在 1 - 48 之间"), RUVIA_MAX(48, "日志条数必须在 1 - 48 之间")), RUVIA_OPTIONAL_FIELD(level, ruvia::string, RUVIA_ONE_OF("日志级别无效", "debug", "info", "warn", "error")), RUVIA_OPTIONAL_FIELD(source, ruvia::string, RUVIA_MAX(16, "日志来源不能超过 16 个字符")));

RUVIA_MODEL(DtuChannelBody,
    RUVIA_REQUIRED_FIELD(channelId, ruvia::string, RUVIA_CUSTOM("通道 ID 无效", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "请输入通道名称"), RUVIA_MAX(100, "通道名称过长")),
    RUVIA_REQUIRED_FIELD(enabled, ruvia::bool_value), RUVIA_REQUIRED_FIELD(southMode, ruvia::string, RUVIA_ONE_OF("南向模式无效", "serial", "tcp_client", "tcp_server")),
    RUVIA_OPTIONAL_FIELD(southHost, ruvia::string, RUVIA_MAX(253, "南向地址过长")), RUVIA_OPTIONAL_FIELD(southPort, ruvia::int64, RUVIA_MIN(1, "端口无效"), RUVIA_MAX(65535, "端口无效")),
    RUVIA_REQUIRED_FIELD(northHost, ruvia::string, RUVIA_MIN(1, "请输入北向服务器"), RUVIA_MAX(253, "服务器地址过长")), RUVIA_REQUIRED_FIELD(northPort, ruvia::int64, RUVIA_MIN(1, "端口无效"), RUVIA_MAX(65535, "端口无效")),
    RUVIA_OPTIONAL_FIELD(serialPath, ruvia::string, RUVIA_MAX(96, "串口路径过长")), RUVIA_OPTIONAL_FIELD(baudRate, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(dataBits, ruvia::int64), RUVIA_OPTIONAL_FIELD(stopBits, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(parity, ruvia::string), RUVIA_OPTIONAL_FIELD(rs485, ruvia::bool_value),
    RUVIA_REQUIRED_FIELD(maxClients, ruvia::int64, RUVIA_MIN(1, "客户端数至少为 1"), RUVIA_MAX(16, "最多 16 个客户端")), RUVIA_REQUIRED_FIELD(queueBytes, ruvia::int64, RUVIA_MIN(4096, "缓冲至少 4096 字节"), RUVIA_MAX(65536, "缓冲最多 65536 字节")),
    RUVIA_REQUIRED_FIELD(serialFrameMs, ruvia::int64, RUVIA_MIN(0, "组帧间隔无效"), RUVIA_MAX(1000, "组帧间隔最多 1000 ms")), RUVIA_OPTIONAL_FIELD(uplinkOnly, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(registrationHex, ruvia::string, RUVIA_MAX(512, "注册包最多 256 字节")), RUVIA_OPTIONAL_FIELD(debugEnabled, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(heartbeatHex, ruvia::string, RUVIA_MAX(512, "心跳包最多 256 字节")), RUVIA_OPTIONAL_FIELD(heartbeatIntervalSec, ruvia::int64, RUVIA_MIN(0, "心跳间隔无效"), RUVIA_MAX(86400, "心跳间隔最多 86400 秒")));
RUVIA_MODEL(DtuChannelParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点 ID 无效", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(channelId, ruvia::string, RUVIA_CUSTOM("通道 ID 无效", service::common::isUuidField)));

RUVIA_MODEL(LogLevelBody, RUVIA_REQUIRED_FIELD(level, ruvia::string, RUVIA_ONE_OF("日志级别无效", "silent", "debug", "info", "warn", "error")));

RUVIA_MODEL(InterfaceDto, RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("displayName", displayName, ruvia::string), RUVIA_OPTIONAL_FIELD(mac, ruvia::string), RUVIA_OPTIONAL_FIELD(up, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(bridge, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(ipv4, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("prefixLength", prefixLength, ruvia::int64), RUVIA_OPTIONAL_FIELD(gateway, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("bridgePorts", bridgePorts, ruvia::boxed_array<ruvia::string>));

RUVIA_MODEL(NetworkDto, RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD(mode, ruvia::string), RUVIA_OPTIONAL_FIELD(device, ruvia::string), RUVIA_OPTIONAL_FIELD(up, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(bridge, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("bridgePorts", bridgePorts, ruvia::boxed_array<ruvia::string>), RUVIA_OPTIONAL_FIELD(ipv4, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("prefixLength", prefixLength, ruvia::int64), RUVIA_OPTIONAL_FIELD(gateway, ruvia::string));

RUVIA_MODEL(SerialDto, RUVIA_OPTIONAL_FIELD(path, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("displayName", displayName, ruvia::string), RUVIA_OPTIONAL_FIELD(available, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(rs485, ruvia::bool_value));

RUVIA_MODEL(ConfigStatusDto, RUVIA_OPTIONAL_FIELD_NAME("activeVersion", activeVersion, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("desiredVersion", desiredVersion, ruvia::int64), RUVIA_OPTIONAL_FIELD(state, ruvia::string), RUVIA_OPTIONAL_FIELD(message, ruvia::string));

RUVIA_MODEL(OutboxStatusDto, RUVIA_OPTIONAL_FIELD(records, ruvia::int64), RUVIA_OPTIONAL_FIELD(bytes, ruvia::int64));

RUVIA_MODEL(TcpTrafficDto, RUVIA_OPTIONAL_FIELD(uploadBytes, ruvia::string), RUVIA_OPTIONAL_FIELD(downloadBytes, ruvia::string), RUVIA_OPTIONAL_FIELD(intervalMs, ruvia::string), RUVIA_OPTIONAL_FIELD(sampleId, ruvia::string), RUVIA_OPTIONAL_FIELD(month, ruvia::string), RUVIA_OPTIONAL_FIELD(monthlyUploadBytes, ruvia::string), RUVIA_OPTIONAL_FIELD(monthlyDownloadBytes, ruvia::string), RUVIA_OPTIONAL_FIELD(day, ruvia::string), RUVIA_OPTIONAL_FIELD(dailyUploadBytes, ruvia::string), RUVIA_OPTIONAL_FIELD(dailyDownloadBytes, ruvia::string));

RUVIA_MODEL(LogStatusDto, RUVIA_OPTIONAL_FIELD(level, ruvia::string));

RUVIA_MODEL(NodeStatusDto, RUVIA_OPTIONAL_FIELD(online, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("lastSeenAt", lastSeenAt, ruvia::string), RUVIA_OPTIONAL_FIELD(config, ConfigStatusDto), RUVIA_OPTIONAL_FIELD(outbox, OutboxStatusDto), RUVIA_OPTIONAL_FIELD(log, LogStatusDto), RUVIA_OPTIONAL_FIELD(tcpTraffic, TcpTrafficDto), RUVIA_OPTIONAL_FIELD(vpnTraffic, TcpTrafficDto));

RUVIA_MODEL(VpnCapabilityDto, RUVIA_OPTIONAL_FIELD_NAME("supportsVpn", supportsVpn, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("wireguardVersion", wireguardVersion, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("agentVersion", agentVersion, ruvia::string), RUVIA_OPTIONAL_FIELD(publicKey, ruvia::string));

RUVIA_MODEL(CapabilityDto, RUVIA_OPTIONAL_FIELD(serialDebug, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("networkConfig", networkConfig, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("networkConfigVersion", networkConfigVersion, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("firmwareUpdate", firmwareUpdate, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("deviceConfig", deviceConfig, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("modemControl", modemControl, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(terminal, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(logs, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(vpn, VpnCapabilityDto));

RUVIA_MODEL(SerialDebugOpenRequest, RUVIA_REQUIRED_FIELD(path, ruvia::string, RUVIA_MAX(96, "串口路径过长")));

RUVIA_MODEL(SerialDebugCommandRequest, RUVIA_REQUIRED_FIELD(action, ruvia::string, RUVIA_ONE_OF("串口操作无效", "manual", "monitor", "write", "keepalive")), RUVIA_REQUIRED_FIELD(requestId, ruvia::int64, RUVIA_MIN(2, "指令序号无效"), RUVIA_MAX(9007199254740991LL, "指令序号无效")), RUVIA_OPTIONAL_FIELD(baudRate, ruvia::int64), RUVIA_OPTIONAL_FIELD(dataBits, ruvia::int64), RUVIA_OPTIONAL_FIELD(stopBits, ruvia::int64), RUVIA_OPTIONAL_FIELD(parity, ruvia::string), RUVIA_OPTIONAL_FIELD(rs485, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(hex, ruvia::string, RUVIA_MAX(2048, "每次最多发送 1024 字节")));

RUVIA_MODEL(NodeSerialOpenInput, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(path, ruvia::string, RUVIA_MAX(96, "串口路径过长")));

RUVIA_MODEL(TerminalOpenInput, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(columns, ruvia::int64, RUVIA_MIN(1, "终端参数无效"), RUVIA_MAX(1000LL, "终端参数过大")), RUVIA_REQUIRED_FIELD(rows, ruvia::int64, RUVIA_MIN(1, "终端参数无效"), RUVIA_MAX(1000LL, "终端参数过大")));
RUVIA_MODEL(TerminalSessionInput, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(sessionId, ruvia::string, RUVIA_CUSTOM("会话编号必须是 UUID", service::common::isUuidField)));
RUVIA_MODEL(TerminalResizeInput, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(sessionId, ruvia::string, RUVIA_CUSTOM("会话编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(columns, ruvia::int64, RUVIA_MIN(1, "终端参数无效"), RUVIA_MAX(1000LL, "终端参数过大")), RUVIA_REQUIRED_FIELD(rows, ruvia::int64, RUVIA_MIN(1, "终端参数无效"), RUVIA_MAX(1000LL, "终端参数过大")));
RUVIA_MODEL(TerminalWriteInput, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(sessionId, ruvia::string, RUVIA_CUSTOM("会话编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(content, ruvia::string, RUVIA_MIN(4, "终端内容为空"), RUVIA_MAX(21848, "终端输入过大")));
RUVIA_MODEL(TerminalAckInput, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(sessionId, ruvia::string, RUVIA_CUSTOM("会话编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(sequence, ruvia::int64, RUVIA_MIN(1, "终端参数无效"), RUVIA_MAX(9007199254740991LL, "终端参数过大")));

RUVIA_MODEL(NodeSerialSessionInput, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(sessionId, ruvia::string, RUVIA_CUSTOM("会话编号必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(NodeSerialCommandInput, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("节点编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(sessionId, ruvia::string, RUVIA_CUSTOM("会话编号必须是 UUID", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(command, SerialDebugCommandRequest));

RUVIA_MODEL(SignalDto, RUVIA_OPTIONAL_FIELD(csq, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("rssiDbm", rssiDbm, ruvia::int64), RUVIA_OPTIONAL_FIELD(percent, ruvia::int64));

RUVIA_MODEL(MobileDto, RUVIA_OPTIONAL_FIELD(available, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("simState", simState, ruvia::string), RUVIA_OPTIONAL_FIELD(iccid, ruvia::string), RUVIA_OPTIONAL_FIELD(signal, SignalDto), RUVIA_OPTIONAL_FIELD(registered, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("registrationStatus", registrationStatus, ruvia::int64), RUVIA_OPTIONAL_FIELD(apn, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("operator", operatorName, ruvia::string), RUVIA_OPTIONAL_FIELD(connected, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(ipv4, ruvia::string));

RUVIA_MODEL(FirmwareStatusDto, RUVIA_OPTIONAL_FIELD(state, ruvia::string), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("progressPercent", progressPercent, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("downloadedBytes", downloadedBytes, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("totalBytes", totalBytes, ruvia::int64));

RUVIA_MODEL(TaskDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("taskType", taskType, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("progressPercent", progressPercent, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("downloadedBytes", downloadedBytes, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("totalBytes", totalBytes, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("createdAt", createdAt, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("updatedAt", updatedAt, ruvia::string));

RUVIA_MODEL(EdgeNodeDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(imei, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("groupId", groupId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("groupName", groupName, ruvia::string), RUVIA_OPTIONAL_FIELD(model, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("softwareVersion", softwareVersion, ruvia::string), RUVIA_OPTIONAL_FIELD(hostname, ruvia::string), RUVIA_OPTIONAL_FIELD(architecture, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("openwrtRelease", openwrtRelease, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("enrollmentStatus", enrollmentStatus, ruvia::string), RUVIA_OPTIONAL_FIELD(status, NodeStatusDto), RUVIA_OPTIONAL_FIELD(capability, CapabilityDto), RUVIA_OPTIONAL_FIELD(mobile, MobileDto), RUVIA_OPTIONAL_FIELD(firmware, FirmwareStatusDto), RUVIA_OPTIONAL_FIELD_NAME("vpnVirtualCidrs", vpnVirtualCidrs, ruvia::boxed_array<ruvia::string>), RUVIA_OPTIONAL_FIELD_NAME("createdAt", createdAt, ruvia::string), RUVIA_OPTIONAL_FIELD(interfaces, ruvia::boxed_array<InterfaceDto>), RUVIA_OPTIONAL_FIELD(networks, ruvia::boxed_array<NetworkDto>), RUVIA_OPTIONAL_FIELD_NAME("serialPorts", serialPorts, ruvia::boxed_array<SerialDto>), RUVIA_OPTIONAL_FIELD(tasks, ruvia::boxed_array<TaskDto>));

RUVIA_MODEL(EdgeGroupDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("parentId", parentId, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("sortOrder", sortOrder, ruvia::int64), RUVIA_OPTIONAL_FIELD(remark, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("nodeCount", nodeCount, ruvia::int64));

RUVIA_MODEL(EdgePageDto, RUVIA_OPTIONAL_FIELD(list, ruvia::boxed_array<EdgeNodeDto>), RUVIA_OPTIONAL_FIELD(total, ruvia::int64), RUVIA_OPTIONAL_FIELD(page, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("totalPages", totalPages, ruvia::int64));

RUVIA_MODEL(LogLineDto, RUVIA_OPTIONAL_FIELD(time, ruvia::string), RUVIA_OPTIONAL_FIELD(level, ruvia::string), RUVIA_OPTIONAL_FIELD(source, ruvia::string), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(detail, ruvia::string));

RUVIA_MODEL(LogsDto, RUVIA_OPTIONAL_FIELD(lines, ruvia::boxed_array<LogLineDto>));

RUVIA_MODEL(EdgeGroupsResponse,
    RUVIA_REQUIRED_FIELD(code, ruvia::int64),
    RUVIA_REQUIRED_FIELD(message, ruvia::string),
    RUVIA_REQUIRED_FIELD(data, ruvia::boxed_array<EdgeGroupDto>));

RUVIA_MODEL(EdgeNodeResponse,
    RUVIA_REQUIRED_FIELD(code, ruvia::int64),
    RUVIA_REQUIRED_FIELD(message, ruvia::string),
    RUVIA_REQUIRED_FIELD(data, EdgeNodeDto));

RUVIA_MODEL(EdgePageResponse,
    RUVIA_REQUIRED_FIELD(code, ruvia::int64),
    RUVIA_REQUIRED_FIELD(message, ruvia::string),
    RUVIA_REQUIRED_FIELD(data, EdgePageDto));

} // namespace service::edge
