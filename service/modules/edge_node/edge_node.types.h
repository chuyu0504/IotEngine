#pragma once

#include <any>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <ruvia/core/channel.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/worker_handle.h>
#include <ruvia/web/model.h>
#include <ruvia/web/validation.h>

#include "service/common/message.h"
#include "service/common/uuid.h"

namespace service::edge_node {

struct FirmwareStorageConfig final {
    const std::string publicBaseUrl;
    const std::filesystem::path directory;
};

inline constexpr auto kDefaultDtu = RUVIA_DEFAULT(false){};
inline constexpr auto kDefaultIo = RUVIA_DEFAULT(false){};
inline constexpr auto kDefaultLogs = RUVIA_DEFAULT(false){};
inline constexpr auto kDefaultVpn = RUVIA_DEFAULT(false){};
inline constexpr auto kDefaultPage = RUVIA_DEFAULT(1){};
inline constexpr auto kDefaultPageSize = RUVIA_DEFAULT(20){};
inline constexpr auto kDefaultSortOrder = RUVIA_DEFAULT(0){};
inline constexpr auto kDefaultBridge = RUVIA_DEFAULT(false){};
inline constexpr auto kDefaultPrefixLength = RUVIA_DEFAULT(0){};
inline constexpr auto kDefaultRollbackTimeoutSec = RUVIA_DEFAULT(60){};
inline constexpr auto kDefaultKeepSettings = RUVIA_DEFAULT(true){};
inline constexpr auto kDefaultLimit = RUVIA_DEFAULT(48){};

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

RUVIA_MODEL(EdgeEventsQuery, RUVIA_OPTIONAL_FIELD(io, ruvia::bool_value, decltype(kDefaultIo)), RUVIA_OPTIONAL_FIELD(dtu, ruvia::bool_value, decltype(kDefaultDtu)), RUVIA_OPTIONAL_FIELD(nodeId, ruvia::string, RUVIA_CUSTOM("nodeId 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(logs, ruvia::bool_value, decltype(kDefaultLogs)), RUVIA_OPTIONAL_FIELD(vpn, ruvia::bool_value, decltype(kDefaultVpn)));

RUVIA_MODEL(EdgeListQuery, RUVIA_OPTIONAL_FIELD(page, ruvia::int64, decltype(kDefaultPage), RUVIA_MIN(1, "page 必须大于 0")), RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64, decltype(kDefaultPageSize), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD(keyword, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("注册状态无效", "pending", "approved")), RUVIA_OPTIONAL_FIELD_NAME("groupId", groupId, ruvia::string, RUVIA_CUSTOM("节点分组筛选无效", isEdgeGroupFilter)));

RUVIA_MODEL(EdgeIdParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(EnrollmentBody, RUVIA_REQUIRED_FIELD(status, ruvia::string, RUVIA_ONE_OF("注册状态无效", "approved")), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MAX(100, "节点名称不能超过 100 个字符")));

RUVIA_MODEL(NodeNameBody, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MAX(100, "节点名称不能超过 100 个字符")));

RUVIA_MODEL(NodeGroupBody, RUVIA_OPTIONAL_FIELD_NAME("groupId", groupId, ruvia::string, RUVIA_CUSTOM("节点分组 ID 无效", service::common::isOptionalUuidField)));

RUVIA_MODEL(EdgeGroupBody, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "分组名称不能为空"), RUVIA_MAX(100, "分组名称不能超过 100 个字符")), RUVIA_OPTIONAL_FIELD_NAME("parentId", parentId, ruvia::string, RUVIA_CUSTOM("上级分组 ID 无效", service::common::isOptionalUuidField)), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("分组状态无效", "enabled", "disabled")), RUVIA_OPTIONAL_FIELD_NAME("sortOrder", sortOrder, ruvia::int64, decltype(kDefaultSortOrder), RUVIA_MIN(0, "分组排序不能小于 0")), RUVIA_OPTIONAL_FIELD(remark, ruvia::string, RUVIA_MAX(500, "分组备注不能超过 500 个字符")));

RUVIA_MODEL(NetworkInterfaceBody, RUVIA_REQUIRED_FIELD(operation, ruvia::string, RUVIA_ONE_OF("网络操作无效", "upsert", "delete")), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_CUSTOM("逻辑接口名称只能包含字母、数字和下划线", isUciSectionName)), RUVIA_OPTIONAL_FIELD_NAME("previousName", previousName, ruvia::string, RUVIA_CUSTOM("原逻辑接口名称只能包含字母、数字和下划线", isUciSectionName)), RUVIA_OPTIONAL_FIELD(mode, ruvia::string, RUVIA_ONE_OF("地址协议无效", "dhcp", "static")), RUVIA_OPTIONAL_FIELD(device, ruvia::string, RUVIA_CUSTOM("设备名称包含非法字符", isOptionalNetworkDevice)), RUVIA_OPTIONAL_FIELD(bridge, ruvia::bool_value, decltype(kDefaultBridge)), RUVIA_OPTIONAL_FIELD_NAME("bridgePorts", bridgePorts, ruvia::array<ruvia::string>, RUVIA_MAX(8, "网桥最多包含 8 个成员")), RUVIA_OPTIONAL_FIELD(ip, ruvia::string, RUVIA_MAX(15, "IPv4 地址格式无效")), RUVIA_OPTIONAL_FIELD_NAME("prefixLength", prefixLength, ruvia::int64, decltype(kDefaultPrefixLength), RUVIA_MIN(0, "IPv4 前缀必须在 0 - 30 之间"), RUVIA_MAX(30, "IPv4 前缀必须在 0 - 30 之间")), RUVIA_OPTIONAL_FIELD(gateway, ruvia::string, RUVIA_MAX(15, "网关格式无效")));

RUVIA_MODEL(NetworkBody, RUVIA_REQUIRED_FIELD(interfaces, ruvia::array<NetworkInterfaceBody>, RUVIA_MIN(1, "请至少配置一个逻辑接口"), RUVIA_MAX(8, "单次最多配置 8 个逻辑接口")), RUVIA_OPTIONAL_FIELD_NAME("rollbackTimeoutSec", rollbackTimeoutSec, ruvia::int64, decltype(kDefaultRollbackTimeoutSec), RUVIA_MIN(30, "回滚等待时间必须在 30 - 300 秒之间"), RUVIA_MAX(300, "回滚等待时间必须在 30 - 300 秒之间")));

RUVIA_MODEL(FirmwareUploadBody, RUVIA_REQUIRED_FIELD_NAME("fileName", fileName, ruvia::string, RUVIA_MIN(1, "固件文件名不能为空"), RUVIA_MAX(255, "固件文件名过长")), RUVIA_REQUIRED_FIELD_NAME("sizeBytes", sizeBytes, ruvia::int64, RUVIA_MIN(1, "固件文件不能为空"), RUVIA_MAX(134217728, "固件不能超过 128 MiB")), RUVIA_OPTIONAL_FIELD_NAME("keepSettings", keepSettings, ruvia::bool_value, decltype(kDefaultKeepSettings)));
RUVIA_MODEL(FirmwareReuseBody, RUVIA_REQUIRED_FIELD(sha256, ruvia::string, RUVIA_MIN(64, "固件摘要无效"), RUVIA_MAX(64, "固件摘要无效")), RUVIA_REQUIRED_FIELD_NAME("sizeBytes", sizeBytes, ruvia::int64, RUVIA_MIN(1, "固件文件不能为空"), RUVIA_MAX(134217728, "固件不能超过 128 MiB")), RUVIA_OPTIONAL_FIELD_NAME("keepSettings", keepSettings, ruvia::bool_value, decltype(kDefaultKeepSettings)));
RUVIA_MODEL(FirmwareReuseResult, RUVIA_OPTIONAL_FIELD(reused, ruvia::bool_value));

RUVIA_MODEL(FirmwareDownloadQuery, RUVIA_REQUIRED_FIELD(token, ruvia::string, RUVIA_MAX(64, "下载凭据无效")));

RUVIA_MODEL(LogsQuery, RUVIA_OPTIONAL_FIELD(limit, ruvia::int64, decltype(kDefaultLimit), RUVIA_MIN(1, "日志条数必须在 1 - 48 之间"), RUVIA_MAX(48, "日志条数必须在 1 - 48 之间")), RUVIA_OPTIONAL_FIELD(level, ruvia::string, RUVIA_ONE_OF("日志级别无效", "debug", "info", "warn", "error")), RUVIA_OPTIONAL_FIELD(source, ruvia::string, RUVIA_MAX(16, "日志来源不能超过 16 个字符")));

RUVIA_MODEL(DtuChannelBody, RUVIA_REQUIRED_FIELD(channelId, ruvia::string, RUVIA_CUSTOM("通道 ID 无效", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "请输入通道名称"), RUVIA_MAX(100, "通道名称过长")), RUVIA_REQUIRED_FIELD(enabled, ruvia::bool_value), RUVIA_REQUIRED_FIELD(southMode, ruvia::string, RUVIA_ONE_OF("南向模式无效", "serial", "tcp_client", "tcp_server")), RUVIA_OPTIONAL_FIELD(southHost, ruvia::string, RUVIA_MAX(253, "南向地址过长")), RUVIA_OPTIONAL_FIELD(southPort, ruvia::int64, RUVIA_MIN(1, "端口无效"), RUVIA_MAX(65535, "端口无效")), RUVIA_REQUIRED_FIELD(northHost, ruvia::string, RUVIA_MIN(1, "请输入北向服务器"), RUVIA_MAX(253, "服务器地址过长")), RUVIA_REQUIRED_FIELD(northPort, ruvia::int64, RUVIA_MIN(1, "端口无效"), RUVIA_MAX(65535, "端口无效")), RUVIA_OPTIONAL_FIELD(serialPath, ruvia::string, RUVIA_MAX(96, "串口路径过长")), RUVIA_OPTIONAL_FIELD(baudRate, ruvia::int64), RUVIA_OPTIONAL_FIELD(dataBits, ruvia::int64), RUVIA_OPTIONAL_FIELD(stopBits, ruvia::int64), RUVIA_OPTIONAL_FIELD(parity, ruvia::string), RUVIA_OPTIONAL_FIELD(rs485, ruvia::bool_value), RUVIA_REQUIRED_FIELD(maxClients, ruvia::int64, RUVIA_MIN(1, "客户端数至少为 1"), RUVIA_MAX(16, "最多 16 个客户端")), RUVIA_REQUIRED_FIELD(queueBytes, ruvia::int64, RUVIA_MIN(4096, "缓冲至少 4096 字节"), RUVIA_MAX(65536, "缓冲最多 65536 字节")), RUVIA_REQUIRED_FIELD(serialFrameMs, ruvia::int64, RUVIA_MIN(0, "组帧间隔无效"), RUVIA_MAX(1000, "组帧间隔最多 1000 ms")), RUVIA_OPTIONAL_FIELD(uplinkOnly, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(registrationHex, ruvia::string, RUVIA_MAX(512, "注册包最多 256 字节")), RUVIA_OPTIONAL_FIELD(debugEnabled, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(heartbeatHex, ruvia::string, RUVIA_MAX(512, "心跳包最多 256 字节")), RUVIA_OPTIONAL_FIELD(heartbeatIntervalSec, ruvia::int64, RUVIA_MIN(0, "心跳间隔无效"), RUVIA_MAX(86400, "心跳间隔最多 86400 秒")));
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

RUVIA_MODEL(ExternalIoChannelDto, RUVIA_OPTIONAL_FIELD(channel, ruvia::string), RUVIA_OPTIONAL_FIELD(kind, ruvia::string), RUVIA_OPTIONAL_FIELD(available, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(controllable, ruvia::bool_value));
RUVIA_MODEL(ExternalIoCapabilityDto, RUVIA_OPTIONAL_FIELD(supported, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(channels, ruvia::array<ExternalIoChannelDto>));
RUVIA_MODEL(CapabilityDto, RUVIA_OPTIONAL_FIELD(nodeIoControl, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(serialDebug, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("networkConfig", networkConfig, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("networkConfigVersion", networkConfigVersion, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("firmwareUpdate", firmwareUpdate, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("deviceConfig", deviceConfig, ruvia::bool_value), RUVIA_OPTIONAL_FIELD_NAME("modemControl", modemControl, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(terminal, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(logs, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(vpn, VpnCapabilityDto), RUVIA_OPTIONAL_FIELD(externalIo, ExternalIoCapabilityDto));

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

RUVIA_MODEL(TaskDto, RUVIA_OPTIONAL_FIELD(uncertain, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("taskType", taskType, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("progressPercent", progressPercent, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("downloadedBytes", downloadedBytes, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("totalBytes", totalBytes, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("createdAt", createdAt, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("updatedAt", updatedAt, ruvia::string));

RUVIA_MODEL(IoBindingDto, RUVIA_REQUIRED_FIELD(deviceId, ruvia::string), RUVIA_REQUIRED_FIELD(deviceName, ruvia::string), RUVIA_REQUIRED_FIELD(pointId, ruvia::string));
RUVIA_MODEL(IoChannelDto, RUVIA_REQUIRED_FIELD(state, service::message::edge::IoChannelState), RUVIA_REQUIRED_FIELD(bindings, ruvia::array<IoBindingDto>), RUVIA_OPTIONAL_FIELD(pending, TaskDto), RUVIA_OPTIONAL_FIELD(lastControl, TaskDto));
RUVIA_MODEL(IoStateDto, RUVIA_REQUIRED_FIELD(online, ruvia::bool_value), RUVIA_REQUIRED_FIELD(current, ruvia::bool_value), RUVIA_REQUIRED_FIELD(controlSupported, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(observedAtMs, ruvia::int64), RUVIA_REQUIRED_FIELD(channels, ruvia::array<IoChannelDto>));
RUVIA_MODEL(IoControlBody, RUVIA_REQUIRED_FIELD(channel, ruvia::string, RUVIA_MIN(3, "请选择输出通道"), RUVIA_MAX(16, "IO 通道无效")), RUVIA_REQUIRED_FIELD(value, ruvia::bool_value), RUVIA_REQUIRED_FIELD(idempotencyKey, ruvia::string, RUVIA_CUSTOM("控制操作标识必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(EdgeNodeDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(imei, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("groupId", groupId, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("groupName", groupName, ruvia::string), RUVIA_OPTIONAL_FIELD(model, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("softwareVersion", softwareVersion, ruvia::string), RUVIA_OPTIONAL_FIELD(hostname, ruvia::string), RUVIA_OPTIONAL_FIELD(architecture, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("openwrtRelease", openwrtRelease, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("enrollmentStatus", enrollmentStatus, ruvia::string), RUVIA_OPTIONAL_FIELD(status, NodeStatusDto), RUVIA_OPTIONAL_FIELD(capability, CapabilityDto), RUVIA_OPTIONAL_FIELD(mobile, MobileDto), RUVIA_OPTIONAL_FIELD(firmware, FirmwareStatusDto), RUVIA_OPTIONAL_FIELD_NAME("vpnVirtualCidrs", vpnVirtualCidrs, ruvia::boxed_array<ruvia::string>), RUVIA_OPTIONAL_FIELD_NAME("createdAt", createdAt, ruvia::string), RUVIA_OPTIONAL_FIELD(interfaces, ruvia::boxed_array<InterfaceDto>), RUVIA_OPTIONAL_FIELD(networks, ruvia::boxed_array<NetworkDto>), RUVIA_OPTIONAL_FIELD_NAME("serialPorts", serialPorts, ruvia::boxed_array<SerialDto>), RUVIA_OPTIONAL_FIELD(tasks, ruvia::boxed_array<TaskDto>));

RUVIA_MODEL(EdgeGroupDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(name, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("parentId", parentId, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("sortOrder", sortOrder, ruvia::int64), RUVIA_OPTIONAL_FIELD(remark, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("nodeCount", nodeCount, ruvia::int64));

RUVIA_MODEL(EdgePageDto, RUVIA_OPTIONAL_FIELD(list, ruvia::boxed_array<EdgeNodeDto>), RUVIA_OPTIONAL_FIELD(total, ruvia::int64), RUVIA_OPTIONAL_FIELD(page, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64), RUVIA_OPTIONAL_FIELD_NAME("totalPages", totalPages, ruvia::int64));

RUVIA_MODEL(LogLineDto, RUVIA_OPTIONAL_FIELD(time, ruvia::string), RUVIA_OPTIONAL_FIELD(level, ruvia::string), RUVIA_OPTIONAL_FIELD(source, ruvia::string), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(detail, ruvia::string));

RUVIA_MODEL(LogsDto, RUVIA_OPTIONAL_FIELD(lines, ruvia::boxed_array<LogLineDto>));

RUVIA_MODEL(EdgeGroupsResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, ruvia::boxed_array<EdgeGroupDto>));

RUVIA_MODEL(EdgeNodeResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, EdgeNodeDto));

RUVIA_MODEL(IoStateResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, IoStateDto));
RUVIA_MODEL(IoControlResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, TaskDto));

RUVIA_MODEL(EdgePageResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, EdgePageDto));

} // namespace service::edge_node

// Edge debug WebSocket protocol: request envelope, per-connection state and
// typed event dispatch. No I/O; the session loop lives in the service.
namespace service::edge_node::debug {

class EventError final : public std::runtime_error {
  public:
    EventError(std::int64_t code, std::string message)
        : std::runtime_error(std::move(message)), code(code) {}

    const std::int64_t code;
};

struct Request final {
    std::string id;
    std::string event;
    std::string data;

    static Request parse(std::string_view wire) {
        if (wire.size() > 64U * 1024U) {
            throw EventError(10002, "消息过大");
        }
        const auto value = ruvia::json_value::parse(wire);
        if (!value.is_object()) {
            throw EventError(10002, "消息格式无效");
        }
        unsigned fields = 0;
        const auto validFields = value.for_each_field([&](std::string_view key, const ruvia::json_value&) {
            const unsigned bit = key == "id" ? 1U : key == "event" ? 2U
                : key == "data"                                    ? 4U
                                                                   : 0U;
            if (!bit || (fields & bit)) {
                return false;
            }
            fields |= bit;
            return true;
        });
        const auto id = value.get<ruvia::string>("id");
        const auto event = value.get<ruvia::string>("event");
        const auto data = value.get<ruvia::json_value>("data");
        if (!id || !service::common::isUuid(id->view()) || !event || event->empty() ||
            event->view().size() > 128 || !data || !validFields || fields != 7U) {
            throw EventError(10002, "事件编号、名称或参数无效");
        }
        for (const auto c : event->view()) {
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_')) {
                throw EventError(10002, "事件名称无效");
            }
        }
        return { std::string(id->view()), std::string(event->view()), std::string(data->view()) };
    }
};

struct SessionIdentity final {
    const std::string connectionId;
    std::string accessToken;
    std::uint64_t revision{};
};

class EventContext;

// Resource implementations retain only state belonging to this connection.
// Their close operation uses the accepting Worker's business context.
class ConnectionResource {
  public:
    ConnectionResource(std::string owner, std::string permission)
        : owner(std::move(owner)), permission(std::move(permission)) {}

    virtual ~ConnectionResource() = default;
    virtual ruvia::task<void> close(EventContext& context) = 0;
    const std::string owner;
    const std::string permission;
    ruvia::stop_source lifetime;
};

using ConnectionResources = std::unordered_map<std::string, std::shared_ptr<ConnectionResource>>;

struct EventResult final {
    std::string data{ "null" };
    // Directed live topic that re-invokes a subscription (empty: one reply).
    std::string topic;
    bool identityChanged{};
    ruvia::stop_token lifetime;
    bool more{};
    bool incremental{};
};

struct EventPolicy final {
    bool authenticated{ true };
    std::string_view permission;
};

// One accepted request of a connection until it completes or is cancelled.
struct Operation final {
    explicit Operation(const ruvia::worker_handle& worker)
        : completion(ruvia::make_channel<int>(worker, { .capacity_ = 1 })) {}

    Request request;
    ruvia::stop_source stop;
    std::any state;
    std::pair<ruvia::channel_sender<int>, ruvia::channel_receiver<int>> completion;
};

// Typed event table of one connection. `Context` is the per-invocation event
// context (service::edge_node::debug::EventContext in production); `Invoker` is the
// service adapter that decodes the body and runs the handler.
template <typename Context>
class EventRegistry final {
  public:
    using Invoker = ruvia::task<EventResult> (*)(void*, Context&);

    void add(std::string_view name, void* handlers, Invoker invoke, EventPolicy policy = {}) {
        if (sealed_) {
            throw std::logic_error("event registration is sealed");
        }
        if (name.empty()) {
            throw std::invalid_argument("empty event name");
        }
        if (!policy.authenticated && !policy.permission.empty()) {
            throw std::logic_error("anonymous event cannot require a permission");
        }
        const auto [entry, inserted] = handlers_.emplace(std::string(name), Entry{ handlers, invoke, policy });
        if (!inserted) {
            throw std::logic_error("duplicate event: " + std::string(name));
        }
    }

    void seal() noexcept { sealed_ = true; }

    EventPolicy policy(std::string_view name) const {
        const auto found = handlers_.find(std::string(name));
        if (found == handlers_.end()) {
            throw EventError(10003, "事件不存在");
        }
        return found->second.policy;
    }

    [[nodiscard]] ruvia::task<EventResult> invoke(Context& context) const {
        if (!sealed_) {
            throw std::logic_error("event registration is not sealed");
        }
        const auto found = handlers_.find(context.request.event);
        if (found == handlers_.end()) {
            throw EventError(10003, "事件不存在");
        }
        return found->second.invoke(found->second.handlers, context);
    }

  private:
    struct Entry {
        void* handlers;
        Invoker invoke;
        EventPolicy policy;
    };

    std::unordered_map<std::string, Entry> handlers_;
    bool sealed_{};
};

} // namespace service::edge_node::debug
