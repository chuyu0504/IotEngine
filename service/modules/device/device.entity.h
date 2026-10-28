#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

#include <ruvia/web/db/db_entity.h>

namespace service::device::entities {
// 连接租约与永久遥测投影分别存储。跨键有效性读取需要 Redis Lua，
// ORM 无法原子读取连接 TTL、链路归属、最后上报时间和遥测运行状态。
// 返回 {connection_id, last_report_at_ms, state, state_reason}；连接无效时 connection_id 为空。
inline constexpr std::string_view kDeviceConnectionStateScript = R"lua(
local runtime = redis.call('HMGET', KEYS[1], 'last_report_at_ms', 'state', 'state_reason')
local report = runtime[1] or ''
local state = runtime[2] or ''
local reason = runtime[3] or ''
local route = redis.call('HMGET', KEYS[2],
  'connection_id', 'instance_id', 'worker_id', 'worker_instance_id', 'session_epoch', 'link_id')
if redis.call('PTTL', KEYS[2]) <= 0 then return {'', report, state, reason} end
for index = 1, 6 do
  if not route[index] or route[index] == '' then return {'', report, state, reason} end
end
if not string.match(route[3], '^%d+$') or not string.match(route[5], '^[1-9]%d*$') then
  return {'', report, state, reason}
end
if redis.call('GET', ARGV[1] .. route[6]) ~= route[2] then return {'', report, state, reason} end
return {route[1], report, state, reason}
)lua";

// 设备调试空闲租约/看守键；字符串值为外部契约，不可变更。
inline std::string debugLeaseKey(std::string_view deviceId) {
    std::string key = "iot:debug:lease:device:";
    key.append(deviceId);
    return key;
}

inline std::string debugWatchKey(std::string_view deviceId) {
    std::string key = "iot:debug:watch:device:";
    key.append(deviceId);
    return key;
}

// protocol_address 为 NOT NULL 且没有列默认值：由 device_channel_binding 触发器（bind_device_channel，
// service/config/channels.h）在 BEFORE INSERT/UPDATE 时按所属链路协议与 protocol_params 计算写入。
// db_column_options 无法表达触发器生成值，写入路径不提交此列，读取时视为非空。
RUVIA_DB_ENTITY(DeviceEntity, "device", RUVIA_DB_COLUMN(debug_enabled, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 100 }), RUVIA_DB_COLUMN(link_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(protocol_config_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(group_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .enum_name_ = ruvia::fixed_string{ "status_enum" }, .default_expression_ = ruvia::fixed_string{ "'enabled'::status_enum" } }), RUVIA_DB_COLUMN(protocol_params, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(remark, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(created_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(deleted_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(protocol_address, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }))

RUVIA_DB_ENTITY(DeviceAccessGrantEntity, "device_access_grant", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(user_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(department_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(access_level, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 20 }), RUVIA_DB_COLUMN(granted_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

RUVIA_DB_ENTITY(DeviceDataEntity, "device_data", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(link_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(protocol, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(data, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(report_time, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .primary_key_ = true }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(connection_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(source, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(occurred_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz }), RUVIA_DB_COLUMN(raw_payload_hex, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'[]'::jsonb" } }), RUVIA_DB_COLUMN(model_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }))

RUVIA_DB_ENTITY(DeviceGroupEntity, "device_group", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 100 }), RUVIA_DB_COLUMN(parent_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .enum_name_ = ruvia::fixed_string{ "status_enum" }, .default_expression_ = ruvia::fixed_string{ "'enabled'::status_enum" } }), RUVIA_DB_COLUMN(sort_order, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "0" } }), RUVIA_DB_COLUMN(remark, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(created_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(deleted_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }))

RUVIA_DB_ENTITY(DeviceGroupAccessGrantEntity, "device_group_access_grant", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(group_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(user_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(department_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(access_level, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 20 }), RUVIA_DB_COLUMN(granted_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

RUVIA_DB_ENTITY(DeviceModelEntity, "device_model", RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(protocol, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 20 }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 64 }), RUVIA_DB_COLUMN(config, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .nullable_ = true }), RUVIA_DB_COLUMN(remark, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(created_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(enabled, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .nullable_ = true }), RUVIA_DB_COLUMN(deleted_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }))

// 设备与设备分组共享授权变更的安全审计记录；modules 内唯一写入方为设备共享，故由 device 映射。
RUVIA_DB_ENTITY(SecurityAuditLogEntity, "security_audit_log",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(actor_user_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(action, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(resource_type, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 50}),
    RUVIA_DB_COLUMN(resource_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(outcome, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 20}),
    RUVIA_DB_COLUMN(reason, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .nullable_ = true}),
    RUVIA_DB_COLUMN(details, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(occurred_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}))
RUVIA_DB_ENTITY(ExternalIoSourceEntity, "device_external_io",
    RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }),
    RUVIA_DB_COLUMN(link_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }),
    RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }),
    RUVIA_DB_COLUMN(protocol_config_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }),
    RUVIA_DB_COLUMN(derived_config, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }))

RUVIA_DB_ENTITY(ExternalIoBindingEntity, "device_external_io_binding",
    RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }),
    RUVIA_DB_COLUMN(point_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }),
    RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }),
    RUVIA_DB_COLUMN(kind, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 2 }),
    RUVIA_DB_COLUMN(channel, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 16 }))
} // namespace service::device::entities
