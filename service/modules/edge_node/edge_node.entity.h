#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

#include <ruvia/web/db/db_entity.h>

namespace service::edge_node::entities {

// 固件上传暂存目录恢复的 Redis 认领键（SET NX）：每个进程实例只由认领的 Service Worker 恢复一次。
inline std::string firmwareUploadRecoveryKey(std::string_view instanceId) {
    return "iot:edge:firmware:upload-recovery:" + std::string(instanceId);
}

RUVIA_DB_ENTITY(
    EdgeDtuEntity, "edge_dtu",
    RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{.data_type_=ruvia::db_data_type::uuid, .primary_key_=true}),
    RUVIA_DB_COLUMN(channel_id, std::pmr::string, ruvia::db_column_options{.data_type_=ruvia::db_data_type::uuid, .primary_key_=true}),
    RUVIA_DB_COLUMN(config, std::pmr::string, ruvia::db_column_options{.data_type_=ruvia::db_data_type::jsonb}),
    RUVIA_DB_COLUMN(wire_hex, std::pmr::string),
    RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{.data_type_=ruvia::db_data_type::jsonb, .default_expression_=ruvia::fixed_string{"'{}'::jsonb"}}));

RUVIA_DB_ENTITY(
    EdgeNodeEntity, "edge_node",
    RUVIA_DB_COLUMN(id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(platform_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(imei, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 15}), RUVIA_DB_COLUMN(name, std::pmr::string,
                                                              ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 100}),
    RUVIA_DB_COLUMN(model, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 128, .default_expression_ = ruvia::fixed_string{"''::character varying"}}), RUVIA_DB_COLUMN(software_version, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(hostname, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}), RUVIA_DB_COLUMN(architecture, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(openwrt_release, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(enrollment_status, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 20, .default_expression_ = ruvia::fixed_string{"'pending'::character varying"}}),
    RUVIA_DB_COLUMN(group_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .nullable_ = true}),
    RUVIA_DB_COLUMN(status, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{\"log\": {\"level\": \"info\"}, \"config\": {\"state\": \"idle\", \"message\": \"\", \"activeVersion\": 0, \"desiredVersion\": 0}, \"outbox\": {\"bytes\": 0, \"records\": 0}}'::jsonb"}}),
    RUVIA_DB_COLUMN(capability, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(mobile, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'{}'::jsonb"}}),
    RUVIA_DB_COLUMN(last_seen_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz,
                                           .nullable_ = true}),
    RUVIA_DB_COLUMN(approved_by, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .nullable_ = true}),
    RUVIA_DB_COLUMN(approved_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz,
                                           .nullable_ = true}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(network_config_version, std::int32_t,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{"0"}}));

RUVIA_DB_ENTITY(
    EdgeNodeGroupEntity, "edge_node_group",
    RUVIA_DB_COLUMN(id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 100}),
    RUVIA_DB_COLUMN(parent_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .nullable_ = true}),
    RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{.enum_name_ = ruvia::fixed_string{"status_enum"}, .default_expression_ = ruvia::fixed_string{"'enabled'::status_enum"}}), RUVIA_DB_COLUMN(sort_order, std::int32_t, ruvia::db_column_options{.default_expression_ = ruvia::fixed_string{"0"}}),
    RUVIA_DB_COLUMN(remark, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 500}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}),
    RUVIA_DB_COLUMN(deleted_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz,
                                           .nullable_ = true}));

RUVIA_DB_ENTITY(
    EdgeNodeInterfaceEntity, "edge_node_interface",
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 32}),
    RUVIA_DB_COLUMN(display_name, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(mac, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 17}),
    RUVIA_DB_COLUMN(is_up, bool, ruvia::db_column_options{.default_expression_ = ruvia::fixed_string{"false"}}), RUVIA_DB_COLUMN(is_bridge, bool, ruvia::db_column_options{.default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(ipv4, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15}),
    RUVIA_DB_COLUMN(prefix_length, std::int32_t,
                    ruvia::db_column_options{.nullable_ = true}),
    RUVIA_DB_COLUMN(gateway, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15}),
    RUVIA_DB_COLUMN(bridge_ports, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'[]'::jsonb"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}));

RUVIA_DB_ENTITY(
    EdgeNodeNetworkEntity, "edge_node_network",
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 32}),
    RUVIA_DB_COLUMN(address_mode, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 12}), RUVIA_DB_COLUMN(device, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{"''::character varying"}}),
    RUVIA_DB_COLUMN(is_up, bool, ruvia::db_column_options{.default_expression_ = ruvia::fixed_string{"false"}}), RUVIA_DB_COLUMN(is_bridge, bool, ruvia::db_column_options{.default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(ipv4, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15}),
    RUVIA_DB_COLUMN(prefix_length, std::int32_t,
                    ruvia::db_column_options{.nullable_ = true}),
    RUVIA_DB_COLUMN(gateway, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 15}),
    RUVIA_DB_COLUMN(bridge_ports, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{"'[]'::jsonb"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}));

RUVIA_DB_ENTITY(
    EdgeNodeSerialEntity, "edge_node_serial",
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(path, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 96}),
    RUVIA_DB_COLUMN(display_name, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{"''::character varying"}}), RUVIA_DB_COLUMN(available, bool, ruvia::db_column_options{.default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(rs485, bool, ruvia::db_column_options{.default_expression_ = ruvia::fixed_string{"false"}}),
    RUVIA_DB_COLUMN(updated_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}));

RUVIA_DB_ENTITY(
    EdgeFirmwareEntity, "edge_firmware",
    RUVIA_DB_COLUMN(id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(version, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64}), RUVIA_DB_COLUMN(file_name, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 255}),
    RUVIA_DB_COLUMN(storage_path, std::pmr::string), RUVIA_DB_COLUMN(sha256, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64}),
    RUVIA_DB_COLUMN(size_bytes, std::int64_t), RUVIA_DB_COLUMN(download_token, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 64}),
    RUVIA_DB_COLUMN(created_by, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(created_at, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{"now()"}}));

RUVIA_DB_ENTITY(
    EdgeTaskEntity, "edge_task",
    RUVIA_DB_COLUMN(id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid,
                                           .primary_key_ = true}),
    RUVIA_DB_COLUMN(node_id, std::pmr::string,
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::uuid}),
    RUVIA_DB_COLUMN(task_type, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 30}), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{.data_type_ = ruvia::db_data_type::varchar, .length_ = 20, .default_expression_ = ruvia::fixed_string{"'pending'::character varying"}}),
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
                    ruvia::db_column_options{.data_type_ = ruvia::db_data_type::timestamp_tz,
                                           .nullable_ = true}));

} // namespace service::edge_node::entities
