#pragma once

#include <string>
#include <string_view>

#include <ruvia/web/db/db_entity.h>

namespace service::edge::persistence {

RUVIA_DB_ENTITY(EdgeNodeInterfaceEntity, "edge_node_interface", RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 32 }), RUVIA_DB_COLUMN(display_name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(mac, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 17 }), RUVIA_DB_COLUMN(is_up, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(is_bridge, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(ipv4, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15 }), RUVIA_DB_COLUMN(prefix_length, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .nullable_ = true }), RUVIA_DB_COLUMN(gateway, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15 }), RUVIA_DB_COLUMN(bridge_ports, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'[]'::jsonb" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

RUVIA_DB_ENTITY(EdgeNodeNetworkEntity, "edge_node_network", RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 32 }), RUVIA_DB_COLUMN(address_mode, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 12 }), RUVIA_DB_COLUMN(device, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(is_up, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(is_bridge, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(ipv4, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15 }), RUVIA_DB_COLUMN(prefix_length, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .nullable_ = true }), RUVIA_DB_COLUMN(gateway, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15 }), RUVIA_DB_COLUMN(bridge_ports, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'[]'::jsonb" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

RUVIA_DB_ENTITY(EdgeNodePlatformEntity, "edge_node_platform", RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(platform_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 32 }), RUVIA_DB_COLUMN(base_url, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 255 }), RUVIA_DB_COLUMN(enabled, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "true" } }), RUVIA_DB_COLUMN(priority, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "100" } }), RUVIA_DB_COLUMN(reconnect_interval_sec, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "5" } }), RUVIA_DB_COLUMN(outbox_max_bytes, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "262144" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{\"state\": \"pending\", \"message\": \"\"}'::jsonb" } }))

RUVIA_DB_ENTITY(EdgeNodeSerialEntity, "edge_node_serial", RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(path, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 96 }), RUVIA_DB_COLUMN(display_name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(available, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(rs485, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

RUVIA_DB_ENTITY(EdgeTaskEntity, "edge_task", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(task_type, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 30 }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 20, .default_expression_ = ruvia::fixed_string{ "'pending'::character varying" } }), RUVIA_DB_COLUMN(request, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(result, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(created_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(completed_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }))

// 分块字段是动态序号，值为原始 Protobuf。Ruvia Redis ORM 无法映射
// 动态字段及原子完整性检查，使用本映射和下面的 Lua 维护。
struct TelemetryUploadRecord final {
    std::string nodeId;
    std::string deviceId;
    std::string reportId;

    [[nodiscard]] std::string key() const {
        return "iot:edge:telemetry-upload:" + nodeId + ':' + deviceId + ':' + reportId;
    }
};

// KEYS[1] upload record; ARGV: metadata signature, part index, part count, part wire.
// Returns 0 while parts are missing (or after completion), else every part in order.
inline constexpr std::string_view kStoreTelemetryPartScript = R"lua(
if redis.call('HGET', KEYS[1], 'done') then return 0 end
local signature = redis.call('HGET', KEYS[1], 'signature')
if signature and signature ~= ARGV[1] then return redis.error_reply('telemetry part metadata conflict') end
local field = 'part:' .. ARGV[2]
local previous = redis.call('HGET', KEYS[1], field)
if previous and previous ~= ARGV[4] then return redis.error_reply('telemetry part content conflict') end
local bytes = tonumber(redis.call('HGET', KEYS[1], 'bytes') or '0')
if not previous then
    if bytes + #ARGV[4] > 4194304 then return redis.error_reply('telemetry upload size limit') end
    redis.call('HSET', KEYS[1], 'signature', ARGV[1], 'bytes', bytes + #ARGV[4], field, ARGV[4])
end
local parts = {}
for i = 0, tonumber(ARGV[3]) - 1 do
    local part = redis.call('HGET', KEYS[1], 'part:' .. i)
    if not part then return 0 end
    parts[#parts + 1] = part
end
return parts
)lua";

// 未完成分块保持持久化，不靠 TTL 丢弃已确认的数据。业务消息已进入
// 持久队列后才回收分块，保留七天去重回执；重放仍受历史记录幂等约束。
inline constexpr std::string_view kFinishTelemetryUploadScript = R"lua(
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], 'done', '1')
redis.call('EXPIRE', KEYS[1], 604800)
return 1
)lua";

} // namespace service::edge::persistence
