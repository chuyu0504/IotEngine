#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>

#include <ruvia/web/db/db_entity.h>

namespace service::system {

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

} // namespace service::system
