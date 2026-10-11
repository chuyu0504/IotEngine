#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <ruvia/web/db/db_entity.h>

namespace service::telemetry::latest::persistence {

RUVIA_DB_ENTITY(DeviceEntity, "device",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(link_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(protocol_config_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(group_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .enum_name_ = ruvia::fixed_string{"status_enum"}, .default_expression_ = ruvia::fixed_string{"'enabled'::status_enum"}}),
    RUVIA_DB_COLUMN(protocol_params, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(remark, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .nullable_ = true}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(protocol_address, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text}))

RUVIA_DB_ENTITY(DeviceLatestValueEntity, "device_latest_value",
    RUVIA_DB_COLUMN(device_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(element_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .primary_key_ = true}),
    RUVIA_DB_COLUMN(value, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb}),
    RUVIA_DB_COLUMN(observed_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz}),
    RUVIA_DB_COLUMN(record_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}))

RUVIA_DB_ENTITY(DeviceModelEntity, "device_model",
    RUVIA_DB_COLUMN(device_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(protocol, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 20}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 64}),
    RUVIA_DB_COLUMN(config, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .nullable_ = true}),
    RUVIA_DB_COLUMN(remark, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .nullable_ = true}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(enabled, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .nullable_ = true}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}))

} // namespace service::telemetry::latest::persistence
