#pragma once

#include <memory_resource>
#include <string>

#include <ruvia/web/db/db_entity.h>

namespace service::link {
// 有界 ZSET 索引与报文 Hash 的一致性读取需要 Lua，ORM 无原子跨键读取接口。
struct LinkDebugIndex {
    static std::string key(std::string_view id) { return "iot:debug:v4:link:" + std::string(id); }
};


RUVIA_DB_ENTITY(
    LinkEntity, "link",
    RUVIA_DB_COLUMN(debug_enabled, bool, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}), RUVIA_DB_COLUMN(protocol, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 20}),
    RUVIA_DB_COLUMN(endpoint, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb}),
    RUVIA_DB_COLUMN(execution, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 16}),
    RUVIA_DB_COLUMN(edge_node_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .nullable_ = true}),
    RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{.enum_name_ = ruvia::fixed_string{"status_enum"}, .default_expression_ = ruvia::fixed_string{"'enabled'::status_enum"}}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz,
                                           .nullable_ = true}))

} // namespace service::link

namespace service::link::entities {

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

RUVIA_DB_ENTITY(EdgeNodeEntity, "edge_node",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(platform_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(imei, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 15}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 100}),
    RUVIA_DB_COLUMN(model, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 128, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(software_version, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(hostname, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(architecture, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(openwrt_release, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(enrollment_status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 20, .default_expression_ = ruvia::fixed_string{"'pending'::character varying"}}),
    RUVIA_DB_COLUMN(last_seen_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(approved_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(approved_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(network_config_version, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"0"}}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{\"log\": {\"level\": \"info\"}, \"config\": {\"state\": \"idle\", \"message\": \"\", \"activeVersion\": 0, \"desiredVersion\": 0}, \"outbox\": {\"bytes\": 0, \"records\": 0}}'::jsonb"}}),
    RUVIA_DB_COLUMN(capability, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(mobile, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(group_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}))

RUVIA_DB_ENTITY(EdgeNodeInterfaceEntity, "edge_node_interface",
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 32}),
    RUVIA_DB_COLUMN(display_name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(mac, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 17}),
    RUVIA_DB_COLUMN(is_up, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(is_bridge, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(ipv4, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15}),
    RUVIA_DB_COLUMN(prefix_length, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .nullable_ = true}),
    RUVIA_DB_COLUMN(gateway, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15}),
    RUVIA_DB_COLUMN(bridge_ports, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'[]'::jsonb"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}))

RUVIA_DB_ENTITY(EdgeNodeSerialEntity, "edge_node_serial",
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(path, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 96}),
    RUVIA_DB_COLUMN(display_name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(available, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(rs485, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}))

} // namespace service::link::entities
