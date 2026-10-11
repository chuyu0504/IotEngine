#pragma once
#include <string>
#include <memory_resource>
#include <string_view>
#include <ruvia/web/redis/redis_entity.h>

namespace service::packet_log {
// 轮次与报文的原子更新使用 Redis Lua；公开 ORM 无法表达多 Hash/ZSET 事务。
struct DebugAcquisitionStorage {
    static constexpr std::string_view prefix = "iot:debug:v4:acquisition:";
};
RUVIA_REDIS_ENTITY(DebugAcquisitionHash, "iot:debug:v4:acquisition",
    RUVIA_REDIS_COLUMN(acquisition_id, ruvia::string, ruvia::redis_column_options{.primary_key_=true}),
    RUVIA_REDIS_COLUMN(link_id, ruvia::string),
    RUVIA_REDIS_COLUMN(device_id, ruvia::string),
    RUVIA_REDIS_COLUMN(source, ruvia::string),
    RUVIA_REDIS_COLUMN(state, ruvia::string),
    RUVIA_REDIS_COLUMN(started_at_ms, ruvia::string),
    RUVIA_REDIS_COLUMN(finished_at_ms, ruvia::string),
    RUVIA_REDIS_COLUMN(last_packet_at_ms, ruvia::string),
    RUVIA_REDIS_COLUMN(created_ms, ruvia::string),
    RUVIA_REDIS_COLUMN(link_index, ruvia::string),
    RUVIA_REDIS_COLUMN(device_index, ruvia::string));

// 同一报文 Hash 由设备和链路 ZSET 引用。公开 ORM 不支持跨 Hash/ZSET 的
// 原子状态推进、双索引裁剪及引用回收，因此这些操作由 service 中的 Lua 完成。
RUVIA_REDIS_ENTITY(DebugPacketHash, "iot:debug:v4:packet",
    RUVIA_REDIS_COLUMN(event_id, ruvia::string, ruvia::redis_column_options{.primary_key_=true}),
    RUVIA_REDIS_COLUMN(acquisition_id, ruvia::string),
    RUVIA_REDIS_COLUMN(link_id, ruvia::string),
    RUVIA_REDIS_COLUMN(device_id, ruvia::string),
    RUVIA_REDIS_COLUMN(direction, ruvia::string),
    RUVIA_REDIS_COLUMN(source, ruvia::string),
    RUVIA_REDIS_COLUMN(address, ruvia::string),
    RUVIA_REDIS_COLUMN(edge_node_id, ruvia::string),
    RUVIA_REDIS_COLUMN(edge_node_name, ruvia::string),
    RUVIA_REDIS_COLUMN(payload_hex, ruvia::string),
    RUVIA_REDIS_COLUMN(time_ms, ruvia::string),
    RUVIA_REDIS_COLUMN(offset, ruvia::string),
    RUVIA_REDIS_COLUMN(transport_status, ruvia::string),
    RUVIA_REDIS_COLUMN(response_status, ruvia::string),
    RUVIA_REDIS_COLUMN(parse_status, ruvia::string),
    RUVIA_REDIS_COLUMN(reason, ruvia::string),
    RUVIA_REDIS_COLUMN(parsed_json, ruvia::string),
    RUVIA_REDIS_COLUMN(reply_to_packet_id, ruvia::string),
    RUVIA_REDIS_COLUMN(revision, ruvia::string),
    RUVIA_REDIS_COLUMN(created_ms, ruvia::string),
    RUVIA_REDIS_COLUMN(link_index, ruvia::string),
    RUVIA_REDIS_COLUMN(device_index, ruvia::string));

struct DebugPacketStorage {
    static std::string key(std::string_view scope, std::string_view id) {
        return "iot:debug:v4:" + std::string(scope) + ':' + std::string(id);
    }
};
}
