#pragma once

#include <vector>
#include "service/common/message.h"

#include <cstdint>
#include <memory_resource>
#include <string>
#include <ruvia/web/db/db_entity.h>

namespace service::gb28181::persistence {

RUVIA_DB_ENTITY(Gb28181ChannelEntity, "gb28181_channel",
    RUVIA_DB_COLUMN(device_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 128}),
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 128}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(manufacturer, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(online, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(ptz_type, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"'-1'::integer"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(custom_name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 255}))

RUVIA_DB_ENTITY(Gb28181DeviceEntity, "gb28181_device",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 128}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(manufacturer, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(remote_address, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(registration_source, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{"'sip'::character varying"}}),
    RUVIA_DB_COLUMN(online, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(last_seen_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz}),
    RUVIA_DB_COLUMN(mapped_device_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(custom_name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 255}),
    RUVIA_DB_COLUMN(projection_cursor, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::numeric, .precision_ = 40, .scale_ = 0, .default_expression_ = ruvia::fixed_string{"0"}}))

RUVIA_DB_ENTITY(Gb28181StreamEntity, "gb28181_stream",
    RUVIA_DB_COLUMN(app, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 128}),
    RUVIA_DB_COLUMN(stream, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 255}),
    RUVIA_DB_COLUMN(schema, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 32}),
    RUVIA_DB_COLUMN(online, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(reader_count, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"0"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(projection_cursor, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::numeric, .precision_ = 40, .scale_ = 0, .default_expression_ = ruvia::fixed_string{"0"}}))

} // namespace service::gb28181::persistence

namespace service::gb28181 {

// Fixed Redis String stores status and payload separated by the first newline.
// Redis ORM cannot preserve this existing scalar key/value format.
struct ControlResultRecord final {
    std::string status;
    std::string payload;

    static ControlResultRecord decode(std::string_view value) {
        const auto separator = value.find('\n');
        if (separator == std::string_view::npos) {
            return {"ok", std::string(value)};
        }
        return {std::string(value.substr(0, separator)), std::string(value.substr(separator + 1))};
    }

    std::string encode() const { return status + "\n" + payload; }
};

// 固定 Hash 使用既有配置键；Ruvia Redis ORM 总会附加 ruvia:orm: 前缀及实体 ID，
// 无法映射该原键。Service Worker 只读取启用状态。
struct ConfigProjectionRecord final {
    bool enabled{};

    static bool enabledValue(std::string_view value) {
        return value == "t" || value == "true" || value == "1";
    }

    std::vector<service::message::StreamField> encode() const {
        return {{"enabled", enabled ? "1" : "0"}};
    }
};

} // namespace service::gb28181
