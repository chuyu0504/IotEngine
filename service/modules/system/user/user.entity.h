#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <ruvia/web/db/db_entity.h>
#include <ruvia/web/db/db_projection.h>

namespace service::user {

RUVIA_DB_ENTITY(
    UserEntity, "sys_user",
    RUVIA_DB_COLUMN(id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(username, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 50}), RUVIA_DB_COLUMN(password_hash, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255}),
    RUVIA_DB_COLUMN(nickname, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 100}),
    RUVIA_DB_COLUMN(phone, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 20}),
    RUVIA_DB_COLUMN(email, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 100}),
    RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{.enum_name_ = ruvia::fixed_string{"status_enum"}, .default_expression_ = ruvia::fixed_string{"'enabled'"}}),
    RUVIA_DB_COLUMN(department_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz,
                                           .nullable_ = true}))

RUVIA_DB_ENTITY(UserRoleEntity, "sys_user_role",
                RUVIA_DB_COLUMN(id, std::pmr::string,
                                ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                                       .primary_key_ = true}),
                RUVIA_DB_COLUMN(user_id, std::pmr::string,
                                ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
                RUVIA_DB_COLUMN(role_id, std::pmr::string,
                                ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}))

RUVIA_DB_PROJECTION(UserDetails,
    RUVIA_DB_COLUMN(id, std::pmr::string),
    RUVIA_DB_COLUMN(username, std::pmr::string),
    RUVIA_DB_COLUMN(nickname, std::pmr::string),
    RUVIA_DB_COLUMN(phone, std::pmr::string),
    RUVIA_DB_COLUMN(email, std::pmr::string),
    RUVIA_DB_COLUMN(status, std::pmr::string),
    RUVIA_DB_COLUMN(department_id, std::pmr::string),
    RUVIA_DB_COLUMN(department_name, std::pmr::string),
    RUVIA_DB_COLUMN(created_at, std::pmr::string),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string))

} // namespace service::user
