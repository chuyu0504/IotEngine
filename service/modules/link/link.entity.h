#pragma once

#include <cstdint>
#include <map>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

#include <ruvia/web/db/db_entity.h>

#include "service/utils/number.h"

namespace service::link::entities {

// Collector 快照含 target:<UUID>:* 动态字段，固定字段 Redis ORM 无法直接映射。
// 键为 service::message::collector 的 linkOwnerKey 与 linkRuntimePrefix。
struct LinkRuntimeRecord {
    std::map<std::string, std::string> fields;

    [[nodiscard]] std::string text(std::string_view name, std::string_view fallback = {}) const {
        const auto current = fields.find(std::string(name));
        return current == fields.end() ? std::string(fallback) : current->second;
    }

    [[nodiscard]] std::int64_t integer(std::string_view name) const {
        const auto value = text(name);
        return service::utils::parseInt64(std::optional<std::string_view>{value}).value_or(0);
    }
};

// 链路调试空闲租约与看守认领的 Redis 键，值为 "1"，过期由 DebugIdle 管理。
inline std::string debugLeaseKey(std::string_view linkId) {
    std::string key = "iot:debug:lease:link:";
    key.append(linkId);
    return key;
}

inline std::string debugWatchKey(std::string_view linkId) {
    std::string key = "iot:debug:watch:link:";
    key.append(linkId);
    return key;
}

RUVIA_DB_ENTITY(LinkEntity, "link",
    RUVIA_DB_COLUMN(debug_enabled, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(protocol, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 20}),
    RUVIA_DB_COLUMN(endpoint, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb}),
    RUVIA_DB_COLUMN(execution, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 16}),
    RUVIA_DB_COLUMN(edge_node_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .enum_name_ = ruvia::fixed_string{"status_enum"}, .default_expression_ = ruvia::fixed_string{"'enabled'::status_enum"}}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}))

} // namespace service::link::entities
