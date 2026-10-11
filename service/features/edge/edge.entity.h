#pragma once

#include <charconv>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <ruvia/web/db/db_entity.h>

namespace service::edge::persistence {

// 分块字段是动态序号，值为原始 Protobuf。Ruvia Redis ORM 无法映射
// 动态字段及原子完整性检查，使用本映射和 service 中的 Lua 维护。
struct TelemetryUploadRecord final {
    std::string nodeId;
    std::string deviceId;
    std::string reportId;

    [[nodiscard]] std::string key() const {
        return "iot:edge:telemetry-upload:" + nodeId + ':' + deviceId + ':' + reportId;
    }
};

RUVIA_DB_ENTITY(
    EdgeDtuEntity, "edge_dtu",
    RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{.data_type_=ruvia::db_data_type::uuid, .primary_key_=true}),
    RUVIA_DB_COLUMN(channel_id, std::pmr::string, ruvia::db_column_options{.data_type_=ruvia::db_data_type::uuid, .primary_key_=true}),
    RUVIA_DB_COLUMN(config, std::pmr::string, ruvia::db_column_options{.data_type_=ruvia::db_data_type::jsonb}),
    RUVIA_DB_COLUMN(wire_hex, std::pmr::string),
    RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{.data_type_=ruvia::db_data_type::jsonb, .default_expression_=ruvia::fixed_string{"'{}'::jsonb"}}));

RUVIA_DB_ENTITY(DeviceEntity, "device",
    RUVIA_DB_COLUMN(debug_enabled, bool, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
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

RUVIA_DB_ENTITY(EdgeConfigRevisionEntity, "edge_config_revision",
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(revision, std::int64_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::big_int, .primary_key_ = true}),
    RUVIA_DB_COLUMN(sha256, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64}),
    RUVIA_DB_COLUMN(item_count, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 20, .default_expression_ = ruvia::fixed_string{"'pending'::character varying"}}),
    RUVIA_DB_COLUMN(message, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 256, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(completed_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}))

RUVIA_DB_ENTITY(EdgeFirmwareEntity, "edge_firmware",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(version, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64}),
    RUVIA_DB_COLUMN(file_name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255}),
    RUVIA_DB_COLUMN(storage_path, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text}),
    RUVIA_DB_COLUMN(sha256, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64}),
    RUVIA_DB_COLUMN(size_bytes, std::int64_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::big_int}),
    RUVIA_DB_COLUMN(download_token, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}))

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

RUVIA_DB_ENTITY(EdgeNodeNetworkEntity, "edge_node_network",
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 32}),
    RUVIA_DB_COLUMN(address_mode, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 12}),
    RUVIA_DB_COLUMN(device, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
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

RUVIA_DB_ENTITY(EdgeNodePlatformEntity, "edge_node_platform",
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(platform_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32}),
    RUVIA_DB_COLUMN(base_url, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255}),
    RUVIA_DB_COLUMN(enabled, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"true"}}),
    RUVIA_DB_COLUMN(priority, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"100"}}),
    RUVIA_DB_COLUMN(reconnect_interval_sec, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"5"}}),
    RUVIA_DB_COLUMN(outbox_max_bytes, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"262144"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{\"state\": \"pending\", \"message\": \"\"}'::jsonb"}}))

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

RUVIA_DB_ENTITY(EdgeTaskEntity, "edge_task",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(task_type, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 30}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 20, .default_expression_ = ruvia::fixed_string{"'pending'::character varying"}}),
    RUVIA_DB_COLUMN(request, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(result, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(completed_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}))

RUVIA_DB_ENTITY(LinkEntity, "link",
    RUVIA_DB_COLUMN(debug_enabled, bool, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}),
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

RUVIA_DB_ENTITY(VpnNetworkEntity, "vpn_network",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(overlay_cidr, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 18}),
    RUVIA_DB_COLUMN(hub_public_key, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(hub_endpoint, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(hub_listen_port, std::int32_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"51820"}}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .enum_name_ = ruvia::fixed_string{"status_enum"}, .default_expression_ = ruvia::fixed_string{"'enabled'::status_enum"}}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(hub_private_key, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 44, .default_expression_ = ruvia::fixed_string{"''::character varying"}}))

RUVIA_DB_ENTITY(VpnPeerEntity, "vpn_peer",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(network_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(peer_type, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 16}),
    RUVIA_DB_COLUMN(edge_node_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(user_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .nullable_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(public_key, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(assigned_ipv4, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::inet}),
    RUVIA_DB_COLUMN(allowed_routes, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'[]'::jsonb"}}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 16, .default_expression_ = ruvia::fixed_string{"'active'::character varying"}}),
    RUVIA_DB_COLUMN(config_revision, std::int64_t,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::big_int, .default_expression_ = ruvia::fixed_string{"1"}}),
    RUVIA_DB_COLUMN(last_handshake_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(revoked_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true}),
    RUVIA_DB_COLUMN(client_private_key, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 44, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(client_managed, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"false"}}))

RUVIA_DB_ENTITY(VpnRouteEntity, "vpn_route",
    RUVIA_DB_COLUMN(id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true}),
    RUVIA_DB_COLUMN(network_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(edge_peer_id, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(lan_interface, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32}),
    RUVIA_DB_COLUMN(target_cidr, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 18}),
    RUVIA_DB_COLUMN(virtual_cidr, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 18}),
    RUVIA_DB_COLUMN(mode, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 16, .default_expression_ = ruvia::fixed_string{"'nat'::character varying"}}),
    RUVIA_DB_COLUMN(nat_mode, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 16, .default_expression_ = ruvia::fixed_string{"'masquerade'::character varying"}}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 16, .default_expression_ = ruvia::fixed_string{"'active'::character varying"}}),
    RUVIA_DB_COLUMN(enabled, bool,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::boolean, .default_expression_ = ruvia::fixed_string{"true"}}),
    RUVIA_DB_COLUMN(last_error, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::text, .default_expression_ = ruvia::fixed_string{"''::text"}}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
        ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}))

} // namespace service::edge::persistence

namespace service::edge::metadata {

// Redis Hash 以设备 ID 为动态字段，值为五个长度前缀字段的记录。
// Ruvia Redis ORM 的固定字段与键前缀不能表达此现有映射；整节点替换
// 仍由 service 中的 DEL/HSET 原子 Lua 执行，避免部分目录可见。
struct Device final {
    std::string linkId;
    std::string deviceCode;
    std::string protocol;
    std::string storagePolicy{"report"};
    std::int64_t onlineWindowMs{300000};
};

using NodeSnapshot = std::unordered_map<std::string, Device>;
using Catalog = std::unordered_map<std::string, NodeSnapshot>;

inline std::string key(std::string_view nodeId) {
    return "iot:edge:metadata:" + std::string(nodeId);
}

inline void appendField(std::string& output, std::string_view value) {
    output += std::to_string(value.size());
    output.push_back(':');
    output.append(value);
}

inline std::string encode(const Device& device) {
    std::string output;
    output.reserve(device.linkId.size() + device.deviceCode.size() + device.protocol.size() + 48);
    appendField(output, device.linkId);
    appendField(output, device.deviceCode);
    appendField(output, device.protocol);
    appendField(output, device.storagePolicy);
    appendField(output, std::to_string(device.onlineWindowMs));
    return output;
}

inline std::optional<std::string_view> takeField(std::string_view value,
                                                 std::size_t& offset) noexcept {
    const auto colon = value.find(':', offset);
    if (colon == std::string_view::npos)
        return std::nullopt;
    std::size_t size{};
    const auto [end, error] =
        std::from_chars(value.data() + offset, value.data() + colon, size);
    if (error != std::errc{} || end != value.data() + colon || size > value.size() - colon - 1)
        return std::nullopt;
    const auto begin = colon + 1;
    offset = begin + size;
    return value.substr(begin, size);
}

inline std::optional<std::int64_t> integer(std::string_view value) noexcept {
    std::int64_t result{};
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size())
        return std::nullopt;
    return result;
}

inline bool validStoragePolicy(std::string_view value) noexcept {
    return value == "report" || value == "change";
}

inline std::optional<Device> decode(std::string_view value) {
    std::size_t offset{};
    const auto linkId = takeField(value, offset);
    const auto deviceCode = takeField(value, offset);
    const auto protocol = takeField(value, offset);
    const auto storagePolicy = takeField(value, offset);
    const auto onlineWindowMs = takeField(value, offset);
    if (!linkId || !deviceCode || !protocol || !storagePolicy || !onlineWindowMs ||
        offset != value.size())
        return std::nullopt;
    const auto online = integer(*onlineWindowMs);
    if (!validStoragePolicy(*storagePolicy) || !online || *online < 1000)
        return std::nullopt;
    return Device{std::string(*linkId), std::string(*deviceCode), std::string(*protocol),
                  std::string(*storagePolicy), *online};
}

} // namespace service::edge::metadata
