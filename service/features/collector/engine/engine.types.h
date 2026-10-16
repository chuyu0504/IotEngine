#pragma once

#include <cstdint>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "service/features/collector/collector.types.h"

namespace service::collector {

struct ProtocolConnectionInfo {
    std::string connectionId;
    std::string linkId;
    std::string remoteAddress;
    std::string targetId;
    std::uint64_t sessionEpoch = 0;
};

// 一次网络读取的原始字节；只在所属 Worker 内从传输层交给协议引擎，不经过 Redis。
struct IngressPacket {
    std::string messageId;
    std::string linkId;
    std::string connectionId;
    std::string remoteAddress;
    std::uint64_t sessionEpoch = 0;
    std::int64_t occurredAtMs = 0;
    std::vector<std::uint8_t> payload;
};

struct ProtocolSessionRefresh {
    std::string connectionId;
    std::vector<ProtocolAction> retiredActions;
    std::vector<ProtocolAction> startedActions;
};

struct DebugPacketIdentity {
    std::string_view linkId;
    std::string_view targetId;
    std::string_view direction;
    std::string_view knownDevice;
    bool deviceOnly = false;
    std::span<const std::uint8_t> bytes;
    const std::set<std::string>* boundDevices = nullptr;
};

} // namespace service::collector
