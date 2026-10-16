#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

#include <ruvia/web/db/db_entity.h>

#include "service/common/message.h"

namespace service::gb28181::persistence {

RUVIA_DB_ENTITY(Gb28181ChannelEntity, "gb28181_channel", RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 128 }), RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 128 }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(manufacturer, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(online, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(ptz_type, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "'-1'::integer" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(custom_name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 255 }))

RUVIA_DB_ENTITY(Gb28181DeviceEntity, "gb28181_device", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 128 }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(manufacturer, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(remote_address, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(registration_source, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{ "'sip'::character varying" } }), RUVIA_DB_COLUMN(online, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(last_seen_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz }), RUVIA_DB_COLUMN(mapped_device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(custom_name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 255 }), RUVIA_DB_COLUMN(projection_cursor, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::numeric, .precision_ = 40, .scale_ = 0, .default_expression_ = ruvia::fixed_string{ "0" } }))

RUVIA_DB_ENTITY(Gb28181StreamEntity, "gb28181_stream", RUVIA_DB_COLUMN(app, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 128 }), RUVIA_DB_COLUMN(stream, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 255 }), RUVIA_DB_COLUMN(schema, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 32 }), RUVIA_DB_COLUMN(online, bool, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{ "false" } }), RUVIA_DB_COLUMN(reader_count, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "0" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(projection_cursor, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::numeric, .precision_ = 40, .scale_ = 0, .default_expression_ = ruvia::fixed_string{ "0" } }))

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
            return { "ok", std::string(value) };
        }
        return { std::string(value.substr(0, separator)), std::string(value.substr(separator + 1)) };
    }

    std::string encode() const { return status + "\n" + payload; }
};

// 固定 Hash service::message::gb28181::kStateKey；Ruvia Redis ORM 总会附加
// ruvia:orm: 前缀及实体 ID，无法映射该原键。Collector 启动时写入，Service Worker
// 读取启用状态与媒体健康；媒体端口与能力以 message.h 中的模型 JSON 存储。
struct ConfigProjectionRecord final {
    bool enabled{};
    bool mediaStarted{};
    std::string mediaPorts;
    std::string mediaCapabilities;

    static bool flag(std::string_view value) {
        return value == "t" || value == "true" || value == "1";
    }

    static ConfigProjectionRecord decode(const std::vector<service::message::StreamField>& fields) {
        namespace state = service::message::gb28181;
        ConfigProjectionRecord record;
        for (const auto& field : fields) {
            if (field.name == state::kStateEnabledField) {
                record.enabled = flag(field.value);
            } else if (field.name == state::kStateMediaStartedField) {
                record.mediaStarted = flag(field.value);
            } else if (field.name == state::kStateMediaPortsField) {
                record.mediaPorts = field.value;
            } else if (field.name == state::kStateMediaCapabilitiesField) {
                record.mediaCapabilities = field.value;
            }
        }
        return record;
    }

    std::vector<service::message::StreamField> encode() const {
        namespace state = service::message::gb28181;
        return {
            { std::string(state::kStateEnabledField), enabled ? "1" : "0" },
            { std::string(state::kStateMediaStartedField), mediaStarted ? "1" : "0" },
            { std::string(state::kStateMediaPortsField), mediaPorts },
            { std::string(state::kStateMediaCapabilitiesField), mediaCapabilities },
        };
    }
};

} // namespace service::gb28181
