#pragma once
#include <ruvia/web/db/db_entity.h>
#include <ruvia/web/redis/redis_entity.h>
#include "service/common/message.h"
namespace service::message {
RUVIA_REDIS_ENTITY(WorkerSnapshotEntity, service::message::worker_metrics::kSnapshotTable,
    RUVIA_REDIS_COLUMN(id, ruvia::string, ruvia::redis_column_options{.primary_key_ = true}),
    RUVIA_REDIS_COLUMN(metrics, ruvia::string),
    RUVIA_REDIS_COLUMN(ready, bool),
    RUVIA_REDIS_COLUMN(health, ruvia::string));
}

namespace service::messaging::persistence {

RUVIA_DB_ENTITY(OutboxConsumerReceiptEntity, "outbox_consumer_receipt",
    RUVIA_DB_COLUMN(consumer_name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 150}),
    RUVIA_DB_COLUMN(event_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(processed_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}))

RUVIA_DB_ENTITY(OutboxEventEntity, "outbox_event",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(event_type, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(aggregate_type, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(aggregate_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text}),
    RUVIA_DB_COLUMN(action, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 50}),
    RUVIA_DB_COLUMN(schema_version, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer}),
    RUVIA_DB_COLUMN(payload, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(occurred_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(available_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(attempts, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"0"}}),
    RUVIA_DB_COLUMN(last_error, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .nullable_ = true}),
    RUVIA_DB_COLUMN(published_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(dead_lettered_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}))

} // namespace service::messaging::persistence

namespace service::rpc {
// RPC 结果以原始 JSON 存入固定 Redis String 键；与通知和消息确认原子写入。
// Redis ORM 的自动键前缀不能表达既有 Contract::reply 键，service 使用此映射。
struct RpcReplyRecord final {
    std::string payload;
};
} // namespace service::rpc
