#pragma once

#include <cstdint>
#include <map>
#include <memory_resource>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/web/db/db_entity.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/redis/redis_types.h>

#include "service/common/message.h"
#include "service/utils/json.h"

namespace service::access::persistence {

// 访问会话投影激活：KEYS[1]=当前版本指针，KEYS[2]=新版本 Hash；ARGV[1]=新版本，ARGV[2]=版本键前缀，
// ARGV[3]=旧版本保留秒数。切换指针、持久化新版本并让旧版本按宽限期过期，返回旧版本或空串。
inline constexpr std::string_view kSessionActivateScript = R"lua(
local previous = redis.call('GET', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1])
redis.call('PERSIST', KEYS[2])
if previous and previous ~= ARGV[1] then
  redis.call('EXPIRE', ARGV[2] .. previous, ARGV[3])
end
return previous or ''
)lua";

// 开放接入事件发布契约（Webhook/开放 MQTT 消费 event() 流）：同一 eventType + eventId 只写入一次，
// 幂等键保留 kEventPublicationTtlSeconds；写入后唤醒发布实例的共享 Webhook 任务。
// KEYS：1=事件流，2=幂等键，3=唤醒流，4=事件输入频道，5=唤醒输入频道；
// ARGV：1=事件流长度上限，2=幂等键 TTL 秒，3=唤醒流长度上限，4=唤醒任务名，5.. 为事件字段名/值。已发布时返回 false。
inline constexpr std::string_view kPublishEventScript = R"lua(
if redis.call('EXISTS', KEYS[2]) ~= 0 then return false end
local arguments = {'MAXLEN', '~', ARGV[1], '*'}
for index = 5, #ARGV do arguments[#arguments + 1] = ARGV[index] end
local id = redis.call('XADD', KEYS[1], unpack(arguments))
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
local wake = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', 'task', ARGV[4])
redis.call('PUBLISH', KEYS[4], id)
redis.call('PUBLISH', KEYS[5], wake)
return id
)lua";

// 投递结果入队与 DeliveryProgressRecord 进度原子写入：KEYS[1]=结果流，KEYS[2]=进度 Hash，KEYS[3]=唤醒流；
// KEYS[4]=结果流输入通知频道，KEYS[5]=唤醒流输入通知频道。
// ARGV[1]=目标 ID，ARGV[2]=结果流长度上限，ARGV[3]=进度 TTL 秒，ARGV[4]=唤醒流长度上限，ARGV[5]=唤醒任务名，
// ARGV[6]='1' 时记录目标已完成，ARGV[7..]=结果字段名/值。目标已完成时返回 false。
inline constexpr std::string_view kDeliveryResultScript = R"lua(
if redis.call('HEXISTS', KEYS[2], ARGV[1]) ~= 0 then return false end
local arguments = {'MAXLEN', '~', ARGV[2], '*'}
for index = 7, #ARGV do arguments[#arguments + 1] = ARGV[index] end
local id = redis.call('XADD', KEYS[1], unpack(arguments))
if ARGV[6] == '1' then
redis.call('HSET', KEYS[2], ARGV[1], '1')
redis.call('EXPIRE', KEYS[2], ARGV[3])
end
local wake = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[4], '*', 'task', ARGV[5])
redis.call('PUBLISH', KEYS[4], id)
redis.call('PUBLISH', KEYS[5], wake)
return id
)lua";

RUVIA_DB_ENTITY(OpenAccessKeyEntity, "open_access_key", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64 }), RUVIA_DB_COLUMN(access_key_prefix, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 16 }), RUVIA_DB_COLUMN(access_key_hash, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64 }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .enum_name_ = ruvia::fixed_string{ "status_enum" }, .default_expression_ = ruvia::fixed_string{ "'enabled'::status_enum" } }), RUVIA_DB_COLUMN(scopes, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'[]'::jsonb" } }), RUVIA_DB_COLUMN(expires_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(last_used_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(last_used_ip, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 64 }), RUVIA_DB_COLUMN(remark, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 200 }), RUVIA_DB_COLUMN(created_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(deleted_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }))

RUVIA_DB_ENTITY(OpenAccessKeyDeviceEntity, "open_access_key_device", RUVIA_DB_COLUMN(access_key_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

RUVIA_DB_ENTITY(OpenAccessLogEntity, "open_access_log", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(access_key_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(webhook_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(mqtt_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(direction, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 20 }), RUVIA_DB_COLUMN(action, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 50 }), RUVIA_DB_COLUMN(event_type, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 100 }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 20 }), RUVIA_DB_COLUMN(http_method, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 10 }), RUVIA_DB_COLUMN(target, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(request_ip, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 64 }), RUVIA_DB_COLUMN(http_status, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .nullable_ = true }), RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(device_code, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 100 }), RUVIA_DB_COLUMN(message, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(request_payload, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(response_payload, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

RUVIA_DB_ENTITY(OpenWebhookEntity, "open_webhook", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(access_key_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64 }), RUVIA_DB_COLUMN(url, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .enum_name_ = ruvia::fixed_string{ "status_enum" }, .default_expression_ = ruvia::fixed_string{ "'enabled'::status_enum" } }), RUVIA_DB_COLUMN(secret, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 255 }), RUVIA_DB_COLUMN(headers, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(event_types, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'[\"device.data.reported\"]'::jsonb" } }), RUVIA_DB_COLUMN(timeout_seconds, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "5" } }), RUVIA_DB_COLUMN(last_triggered_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(last_success_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(last_failure_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(last_http_status, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .nullable_ = true }), RUVIA_DB_COLUMN(last_error, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(deleted_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(skip_tls_verify, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }))

RUVIA_DB_ENTITY(OpenMqttEntity, "open_mqtt", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(access_key_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64 }), RUVIA_DB_COLUMN(broker_url, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(username, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(password, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .default_expression_ = ruvia::fixed_string{ "'enabled'" } }), RUVIA_DB_COLUMN(event_types, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'[\"device.data.reported\"]'::jsonb" } }), RUVIA_DB_COLUMN(timeout_seconds, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "5" } }), RUVIA_DB_COLUMN(last_triggered_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(last_success_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(last_failure_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(last_error, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text, .nullable_ = true }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(deleted_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }))

} // namespace service::access::persistence

namespace service::access::delivery {

// Existing Hash stores completed target IDs as dynamic fields, with value 1.
// Ruvia Redis ORM cannot map this key format and dynamic fields; the result message
// and progress must be written atomically by the same Lua operation.
struct DeliveryProgressRecord final {
    std::set<std::string, std::less<>> completedTargets;

    static std::string key(std::string_view eventType, std::string_view eventId) {
        return "iot:open-access:delivery-progress:" + std::string(eventType) + ":" + std::string(eventId);
    }

    static DeliveryProgressRecord decode(const ruvia::redis_value& reply) {
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            throw std::runtime_error("read webhook delivery progress: expected array");
        }
        DeliveryProgressRecord record;
        for (const auto& value : reply.array()) {
            if (value.kind() != ruvia::redis_value::kind_type::string) {
                throw std::runtime_error("webhook delivery progress contains a non-string target");
            }
            record.completedTargets.emplace(value.string());
        }
        return record;
    }
};

// Existing telemetry Hash uses dynamic point fields and JSON values. Redis ORM
// cannot map this fixed key format and heterogeneous fields. Preserve missing
// configuration, null values, and malformed-field handling when decoding.
struct LatestValuesRecord final {
    struct Point final {
        std::int64_t sort{ 0 };
        std::int64_t observedAt{ 0 };
        std::string id;
        std::string name;
        std::string value{ "null" };
        std::string unit;
        std::string dataType;
        std::string encode;
        std::string access{ "ro" };
    };

    std::set<std::string, std::less<>> configured;
    std::map<std::string, Point, std::less<>> latest;
    bool hasConfigured{};

    static LatestValuesRecord decode(const ruvia::redis_value& reply) {
        LatestValuesRecord record;
        auto& configured = record.configured;
        auto& latest = record.latest;
        auto& hasConfigured = record.hasConfigured;
        if (reply.kind() == ruvia::redis_value::kind_type::array) {
            const auto& entries = reply.array();
            for (std::size_t index = 0; index + 1 < entries.size(); index += 2) {
                if (entries[index].kind() != ruvia::redis_value::kind_type::string ||
                    entries[index + 1].kind() != ruvia::redis_value::kind_type::string) {
                    continue;
                }
                const auto field = entries[index].string();
                const auto raw = entries[index + 1].string();
                if (field == "_element_ids") {
                    hasConfigured = true;
                    const auto parsed = ruvia::json_value::parse(raw);
                    if (parsed.is_object()) {
                        (void)parsed.for_each_field(
                            [&](std::string_view name, const ruvia::json_value&) {
                                if (!name.empty()) {
                                    configured.emplace(name);
                                }
                                return true;
                            }
                        );
                    }
                    continue;
                }
                if (field.empty() || field.front() == '_') {
                    continue;
                }
                const auto parsed = ruvia::json_value::parse(raw);
                if (!parsed.is_object()) {
                    continue;
                }
                Point point;
                point.id.assign(field);
                if (const auto value = parsed.get<ruvia::string>("id")) {
                    point.id.assign(value->view());
                }
                point.name = point.id;
                if (const auto value = parsed.get<ruvia::string>("name")) {
                    point.name.assign(value->view());
                }
                if (const auto value = parsed.get<ruvia::string>("unit")) {
                    point.unit.assign(value->view());
                }
                if (const auto value = parsed.get<ruvia::string>("dataType")) {
                    point.dataType.assign(value->view());
                }
                if (const auto value = parsed.get<ruvia::string>("encode")) {
                    point.encode.assign(value->view());
                }
                if (const auto value = parsed.get<ruvia::int64>("sort")) {
                    point.sort = static_cast<std::int64_t>(*value);
                }
                if (const auto value = parsed.get<ruvia::int64>("observedAt")) {
                    point.observedAt = static_cast<std::int64_t>(*value);
                }
                if (const auto value = parsed.get<ruvia::json_value>("value")) {
                    point.value = service::message::telemetry::canonicalPointJson(
                        value->view(),
                        point.dataType
                    );
                }
                if (const auto value = parsed.get<ruvia::string>("access"); value && value->view() == "rw") {
                    point.access = "rw";
                }
                latest.insert_or_assign(point.id, std::move(point));
            }
        }
        return record;
    }
};

} // namespace service::access::delivery
