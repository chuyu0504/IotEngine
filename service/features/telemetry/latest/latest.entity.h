#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

#include <ruvia/web/db/db_entity.h>
#include <ruvia/web/db/db_predicate.h>

#include "service/common/message.h"
#include "service/features/configuration/configuration.entity.h"

namespace service::telemetry::latest::persistence {

// 连接截止索引只保存设备 ID 与 Redis 时钟截止值；跨键 TTL 检查及
// 到期认领需要 Lua，ORM 无法原子认领索引并发布一次状态变化。
struct DeviceConnectionDeadline final {
    static std::string key() {
        return std::string(service::message::collector::kConnectionDeadlinesKey);
    }

    static constexpr std::string_view eraseScript = R"lua(
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
)lua";

    static constexpr std::string_view expireScript = R"lua(
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1000)
local expired = {}
for _, device_id in ipairs(due) do
  local ttl = redis.call('PTTL', ARGV[1] .. device_id)
  if ttl > 0 then
    redis.call('ZADD', KEYS[1], now + ttl, device_id)
  elseif redis.call('ZREM', KEYS[1], device_id) == 1 then
    expired[#expired + 1] = device_id
  end
end
if #expired > 0 then
  redis.call('PUBLISH', KEYS[2], table.concat(expired, ','))
end
return #expired
)lua";
};

RUVIA_DB_ENTITY(DeviceLatestValueEntity, "device_latest_value", RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(element_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .primary_key_ = true }), RUVIA_DB_COLUMN(value, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb }), RUVIA_DB_COLUMN(observed_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz }), RUVIA_DB_COLUMN(record_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

} // namespace service::telemetry::latest::persistence
