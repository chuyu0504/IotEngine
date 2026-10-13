#pragma once

#include <cstddef>
#include <string_view>
#include "service/common/message.h"
#include "service/features/collector/mc/mc.types.h"
#include "service/features/collector/fins/fins.types.h"
#include "service/features/collector/dlt645/dlt645.types.h"

#include <cstdint>
#include <string>
#include <vector>

namespace service::collector {

// Redis 配置快照采用版本化键、动态元素字段和原子发布；Ruvia Redis ORM
// 的固定 Hash 实体不能映射此多键协议。以下类型是实际读写的存储映射。
struct LinkTargetDefinition {
    std::string id;
    std::string name;
    std::string ip;
    std::uint16_t port = 0;
    std::string status;
    std::string mqttConfig = "{}";
    bool operator==(const LinkTargetDefinition&) const = default;
};

struct LinkDefinition {
    bool debugEnabled = false;
    std::string id;
    std::string name;
    std::string mode;
    std::string protocol;
    std::string ip;
    std::uint16_t port = 0;
    std::string status;
    std::vector<LinkTargetDefinition> targets;
    bool operator==(const LinkDefinition&) const = default;
};

struct ElementDefinition {
    std::string configKey;
    std::string id;
    std::string name;
    std::string unit;
    std::string dataType;
    std::string byteOrder;
    std::string registerType;
    std::string mqttField;
    std::int64_t address = 0;
    std::int64_t quantity = 0;
    std::int64_t decimals = -1;
    std::string area;
    std::int64_t dbNumber = 0;
    std::int64_t start = 0;
    std::int64_t startBit = 0;
    std::int64_t size = 0;
    std::string functionCode;
    std::string direction;
    std::string guideHex;
    std::string encoding;
    std::int64_t length = 0;
    std::int64_t digits = 0;
    bool writable = false;
    bool responseElement = false;
    std::string positionMode = "GUIDE";
    std::int64_t byteOffset = 0;
    bool operator==(const ElementDefinition&) const = default;
};

struct DeviceDefinition {
    std::string calculationConfig;
    std::string parameters = "{}";
    bool debugEnabled = false;
    std::string id;
    std::string modelId;
    std::string code;
    std::string name;
    std::string linkId;
    std::string linkMode;
    std::string targetId;
    std::string protocol;
    std::string timezone = "+08:00";
    std::string sl651ResponseMode = "M1";
    std::int64_t onlineTimeout = 300;
    std::string heartbeatMode = "OFF";
    std::vector<std::uint8_t> heartbeatBytes;
    std::string registrationMode = "OFF";
    std::vector<std::uint8_t> registrationBytes;
    std::string modbusMode;
    std::uint8_t slaveId = 1;
    std::int64_t modbusMergeGap = 100;
    std::int64_t modbusMaxQuantity = 125;
    std::string s7ConnectionMode = "RACK_SLOT";
    std::string s7ConnectionType = "PG";
    std::int64_t s7Rack = 0;
    std::int64_t s7Slot = 1;
    std::string s7LocalTsap = "0100";
    std::string s7RemoteTsap = "0101";
    std::int64_t s7HandshakeTimeoutMs = 5000;
    std::int64_t s7DirectProbeTimeoutMs = 5000;
    std::string s7ProbeMode = "STANDARD";
    mc::Connection mcConnection;
    fins::Connection finsConnection;
    dlt645::Connection dlt645Connection;
    std::int64_t readInterval = 300;
    std::string storagePolicy = "report";
    std::int64_t commandFastReadDuration = 60;
    std::int64_t commandFastReadInterval = 1;
    std::vector<ElementDefinition> elements;
    bool operator==(const DeviceDefinition&) const = default;
};

} // namespace service::collector

namespace service::collector {

// 现有 Hash 使用原始键，Redis ORM 的固定键前缀不能直接映射。
struct CollectorWorkerRecord final {
    static constexpr std::string_view storeScript = R"lua(
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], unpack(ARGV, 2))
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
)lua";
    static constexpr std::string_view retainScript = R"lua(
local current = redis.call('HGET', KEYS[1], 'worker_instance_id')
if current == ARGV[2] then return redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], unpack(ARGV, 3))
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
)lua";
    static constexpr std::string_view eraseScript = R"lua(
if redis.call('HGET', KEYS[1], 'worker_instance_id') ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
)lua";
    std::size_t workerIndex{};
    std::string version;
    std::string state;
    std::int64_t appliedAtMs{};
    std::string workerInstanceId;

    [[nodiscard]] std::vector<message::StreamField> fields() const {
        return {{"worker_id", std::to_string(workerIndex)}, {"version", version},
                {"state", state}, {"applied_at_ms", std::to_string(appliedAtMs)},
                {"worker_instance_id", workerInstanceId}};
    }

    static std::string key(std::size_t index, std::string_view instance) {
        return "iot:runtime:collector:" + std::string(instance) + ":" + std::to_string(index);
    }
};

// 链路快照保留事件携带的动态字段；消息 ID 和创建时间不属于存储快照。
struct CollectorLinkRecord final {
    static constexpr std::string_view eraseLegacyScript = R"lua(
local owner = redis.call('GET', KEYS[2])
if owner ~= ARGV[1] or owner == ARGV[2] then return 0 end
if redis.call('PTTL', KEYS[1]) ~= -1 then return 0 end
return redis.call('DEL', KEYS[1])
)lua";
    std::vector<message::StreamField> fields;

    static CollectorLinkRecord fromEvent(const message::StreamMessage& event,
                                        std::int64_t updatedAtMs) {
        CollectorLinkRecord record;
        record.fields.reserve(event.fields.size() + 1);
        for (const auto& field : event.fields) {
            if (field.name != "message_id" && field.name != "created_at_ms")
                record.fields.push_back(field);
        }
        record.fields.push_back({"updated_at_ms", std::to_string(updatedAtMs)});
        return record;
    }

    static std::string key(std::string_view id, std::size_t index, std::string_view instance) {
        return "iot:runtime:link:" + std::string(id) + ":worker:" +
            std::string(instance) + ":" + std::to_string(index);
    }
};

struct DeviceRouteOwner final {
    std::string workerId;
    std::string connectionId;
    std::string instanceId;
};

// 连接身份使用独立 Hash 租约；遥测投影的永久 Hash 不保存路由。
struct CollectorDeviceRouteRecord final {
    static constexpr std::string_view bindScript = R"lua(
if redis.call('GET', KEYS[4]) ~= ARGV[7] then return 0 end
local current = redis.call('HMGET', KEYS[1], 'instance_id', 'worker_id', 'worker_instance_id', 'connection_id', 'session_epoch')
if current[1] == ARGV[7] and current[2] == ARGV[3] and current[3] == ARGV[9] and current[4] == ARGV[4]
  and tonumber(current[5] or '0') > tonumber(ARGV[5]) then return 0 end
local previous_worker = redis.call('HGET', KEYS[1], 'worker_id') or redis.call('HGET', KEYS[2], 'worker_id') or ''
local previous_instance = redis.call('HGET', KEYS[1], 'instance_id') or redis.call('HGET', KEYS[2], 'instance_id') or ''
local previous_connection = redis.call('HGET', KEYS[1], 'connection_id') or redis.call('HGET', KEYS[2], 'connection_id') or ''
redis.call('HSET', KEYS[1], 'device_id', ARGV[1], 'device_code', ARGV[2],
  'worker_id', ARGV[3], 'connection_id', ARGV[4], 'session_epoch', ARGV[5],
  'instance_id', ARGV[7], 'link_id', ARGV[8], 'worker_instance_id', ARGV[9])
redis.call('PEXPIRE', KEYS[1], ARGV[10])
local now = redis.call('TIME')
redis.call('ZADD', KEYS[5], tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) + tonumber(ARGV[10]), ARGV[1])
redis.call('HDEL', KEYS[2], 'instance_id', 'worker_id', 'worker_instance_id', 'connection_id', 'session_epoch', 'link_id')
redis.call('HSET', KEYS[2], 'updated_at_ms', ARGV[6])
if previous_connection ~= ARGV[4] then redis.call('XADD', KEYS[3], 'MAXLEN', '~', '100000', '*', 'topic', 'device') end
return {previous_worker, previous_connection, previous_instance}
)lua";

    static constexpr std::string_view renewScript = R"lua(
if redis.call('PTTL', KEYS[1]) <= 0 then return 0 end
local route = redis.call('HMGET', KEYS[1], 'instance_id', 'worker_id', 'worker_instance_id', 'connection_id', 'session_epoch', 'link_id')
for index = 1, 5 do if route[index] ~= ARGV[index] then return 0 end end
if not route[6] or redis.call('GET', ARGV[7] .. route[6]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[6])
local now = redis.call('TIME')
redis.call('ZADD', KEYS[2], tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) + tonumber(ARGV[6]), ARGV[8])
return 1
)lua";

    static constexpr std::string_view eraseScript = R"lua(
local route = redis.call('HMGET', KEYS[1], 'instance_id', 'worker_id', 'worker_instance_id', 'connection_id', 'session_epoch')
for index = 1, 5 do if route[index] ~= ARGV[index] then return 0 end end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[4], ARGV[8])
if not redis.call('HGET', KEYS[2], 'last_report_at_ms') then
  redis.call('HSET', KEYS[2], 'state', 'offline', 'state_reason', ARGV[6])
end
redis.call('HSET', KEYS[2], 'updated_at_ms', ARGV[7])
redis.call('XADD', KEYS[3], 'MAXLEN', '~', '100000', '*', 'topic', 'device')
return 1
)lua";

    static constexpr std::string_view eraseLegacyScript = R"lua(
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local connection = redis.call('HGET', KEYS[1], 'connection_id')
if not connection or connection == '' then return 0 end
local link = redis.call('HGET', KEYS[1], 'link_id')
if link and link ~= '' and link ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[1], 'instance_id', 'worker_id', 'worker_instance_id', 'connection_id', 'session_epoch', 'link_id')
redis.call('HSET', KEYS[1], 'updated_at_ms', ARGV[3])
redis.call('XADD', KEYS[3], 'MAXLEN', '~', '100000', '*', 'topic', 'device')
return 1
)lua";
};

} // namespace service::collector
