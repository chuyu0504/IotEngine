#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

#include <ruvia/web/db/db_entity.h>
#include <ruvia/web/db/db_query.h>

namespace service::command::persistence {

// Redis 动态连接键使用完整身份和 TTL；命令读取不延长租约。
// KEYS[1]=设备连接键，ARGV[1]=链路属主键前缀；属主实例不符或任一字段缺失时返回空数组。
inline constexpr std::string_view kDeviceRouteReadScript = R"lua(
if redis.call('PTTL', KEYS[1]) <= 0 then return {} end
local route = redis.call('HMGET', KEYS[1], 'worker_id', 'instance_id', 'connection_id', 'session_epoch', 'worker_instance_id', 'link_id')
for index = 1, 6 do if not route[index] or route[index] == '' then return {} end end
if redis.call('GET', ARGV[1] .. route[6]) ~= route[2] then return {} end
return route
)lua";

// 待下发命令批量入队（Stream 或 List）：容量不足时整体拒绝并返回 0，已接受的命令不裁剪。
// KEYS[1]=队列，KEYS[2]=Stream 输入通知频道（List 不发布）；ARGV[1]=stream|list，ARGV[2]=最大深度，ARGV[3]=命令数，其后每条命令为“值个数 + 值”。
inline constexpr std::string_view kPendingBatchDispatchScript = R"lua(
local mode=ARGV[1]
local count=tonumber(ARGV[3])
local depth=mode=='stream' and redis.call('XLEN',KEYS[1]) or redis.call('LLEN',KEYS[1])
if depth+count>tonumber(ARGV[2]) then return 0 end
local offset=4
for task=1,count do
  local size=tonumber(ARGV[offset])
  offset=offset+1
  local values={}
  if mode=='stream' then values[1]='*' end
  for index=1,size do values[#values+1]=ARGV[offset]; offset=offset+1 end
  if mode=='stream' then
    local id=redis.call('XADD',KEYS[1],unpack(values))
    redis.call('PUBLISH',KEYS[2],id)
  else redis.call('RPUSH',KEYS[1],unpack(values)) end
end
return count
)lua";

RUVIA_DB_ENTITY(CommandAttemptEntity, "command_attempt", RUVIA_DB_COLUMN(operation_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(queue_key, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(queue_kind, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(payload, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb }), RUVIA_DB_COLUMN(submitted_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .default_expression_ = ruvia::fixed_string{ "''::text" } }), RUVIA_DB_COLUMN(max_length, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer }), RUVIA_DB_COLUMN(claimed_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(dispatched_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(sent_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(deadline, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "(now() + '00:01:00'::interval)" } }))

RUVIA_DB_ENTITY(CommandOperationEntity, "command_operation", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(request_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(ordinal, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer }), RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(device_code, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(protocol, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(reason, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .default_expression_ = ruvia::fixed_string{ "''::text" } }), RUVIA_DB_COLUMN(elements, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'[]'::jsonb" } }), RUVIA_DB_COLUMN(actual_values, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'[]'::jsonb" } }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(completed_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(model_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }))

} // namespace service::command::persistence
