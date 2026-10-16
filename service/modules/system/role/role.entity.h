#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <ruvia/web/db/db_entity.h>
#include <ruvia/web/db/db_projection.h>

namespace service::role::entities {

RUVIA_DB_ENTITY(RoleEntity, "sys_role",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 50}),
    RUVIA_DB_COLUMN(code, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 50}),
    RUVIA_DB_COLUMN(description, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 255}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .enum_name_ = ruvia::fixed_string{"status_enum"}, .default_expression_ = ruvia::fixed_string{"'enabled'::status_enum"}}),
    RUVIA_DB_COLUMN(sort_order, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"0"}}),
    RUVIA_DB_COLUMN(permissions, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'[]'::jsonb"}}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}))

RUVIA_DB_PROJECTION(RolePermission, RUVIA_DB_COLUMN(permission, std::pmr::string))

} // namespace service::role::entities
