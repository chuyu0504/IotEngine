#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <ruvia/web/db/db_entity.h>
#include <ruvia/web/db/db_projection.h>

namespace service::dept {

RUVIA_DB_ENTITY(
    DeptEntity, "sys_department",
    RUVIA_DB_COLUMN(id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(code, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 50}),
    RUVIA_DB_COLUMN(parent_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(leader_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(sort_order, std::int64_t, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"0"}}), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{.enum_name_ = ruvia::fixed_string{"status_enum"}, .default_expression_ = ruvia::fixed_string{"'enabled'"}}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz,
                                           .nullable_ = true}))

RUVIA_DB_PROJECTION(DepartmentDetails,
    RUVIA_DB_COLUMN(id, std::pmr::string),
    RUVIA_DB_COLUMN(name, std::pmr::string),
    RUVIA_DB_COLUMN(code, std::pmr::string),
    RUVIA_DB_COLUMN(parent_id, std::pmr::string),
    RUVIA_DB_COLUMN(parent_name, std::pmr::string),
    RUVIA_DB_COLUMN(leader_id, std::pmr::string),
    RUVIA_DB_COLUMN(leader_name, std::pmr::string),
    RUVIA_DB_COLUMN(sort_order, std::int64_t),
    RUVIA_DB_COLUMN(status, std::pmr::string),
    RUVIA_DB_COLUMN(created_at, std::pmr::string),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string))

} // namespace service::dept
