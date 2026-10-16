#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "service/common/message.h"

namespace service::command {

// 设备当前 Collector 连接的路由身份，来自 Redis 动态连接键。
struct DeviceRoute {
    std::size_t workerIndex = 0;
    std::string instanceId;
    std::string connectionId;
    std::uint64_t sessionEpoch = 0;
};

enum class PendingQueueKind { Stream,
                              List };

// 已持久化、待写入物理下发队列的一条命令：Stream 使用字段，List 使用编码后的负载。
struct PendingDispatch {
    message::ProtocolTask task;
    std::vector<message::StreamField> streamFields;
    std::string listPayload;
};

} // namespace service::command
