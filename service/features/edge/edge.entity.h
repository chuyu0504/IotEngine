#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <ruvia/web/db/db_entity.h>

#include "service/common/message.h"

namespace service::edge::persistence {

RUVIA_DB_ENTITY(
    EdgeDtuEntity,
    "edge_dtu",
    RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }),
    RUVIA_DB_COLUMN(channel_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }),
    RUVIA_DB_COLUMN(config, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb }),
    RUVIA_DB_COLUMN(wire_hex, std::pmr::string),
    RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } })
);

RUVIA_DB_ENTITY(EdgeConfigRevisionEntity, "edge_config_revision", RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(revision, std::int64_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::big_int, .primary_key_ = true }), RUVIA_DB_COLUMN(sha256, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64 }), RUVIA_DB_COLUMN(item_count, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 20, .default_expression_ = ruvia::fixed_string{ "'pending'::character varying" } }), RUVIA_DB_COLUMN(message, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 256, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(created_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(completed_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }))

RUVIA_DB_ENTITY(AcquisitionCalculationEntity, "edge_acquisition_calculation", RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(digest, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 64 }), RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(record_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(observed_at_ms, std::int64_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::big_int }), RUVIA_DB_COLUMN(state, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb }))

RUVIA_DB_ENTITY(AcquisitionSampleEntity, "edge_acquisition_sample", RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(digest, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 64 }), RUVIA_DB_COLUMN(device_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(record_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(observed_at_ms, std::int64_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::big_int }), RUVIA_DB_COLUMN(payload, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb }))

RUVIA_DB_ENTITY(EdgeAcquisitionSnapshotEntity, "edge_acquisition_snapshot", RUVIA_DB_COLUMN(node_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(digest, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .primary_key_ = true, .length_ = 64 }), RUVIA_DB_COLUMN(revision, std::int64_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::big_int }), RUVIA_DB_COLUMN(payload, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

RUVIA_DB_ENTITY(EdgeNodeEntity, "edge_node", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(platform_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(imei, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 15 }), RUVIA_DB_COLUMN(name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .nullable_ = true, .length_ = 100 }), RUVIA_DB_COLUMN(model, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 128, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(software_version, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(hostname, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(architecture, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 32, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(openwrt_release, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64, .default_expression_ = ruvia::fixed_string{ "''::character varying" } }), RUVIA_DB_COLUMN(enrollment_status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 20, .default_expression_ = ruvia::fixed_string{ "'pending'::character varying" } }), RUVIA_DB_COLUMN(last_seen_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(approved_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }), RUVIA_DB_COLUMN(approved_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .nullable_ = true }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(updated_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }), RUVIA_DB_COLUMN(network_config_version, std::int32_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::integer, .default_expression_ = ruvia::fixed_string{ "0" } }), RUVIA_DB_COLUMN(status, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{\"log\": {\"level\": \"info\"}, \"config\": {\"state\": \"idle\", \"message\": \"\", \"activeVersion\": 0, \"desiredVersion\": 0}, \"outbox\": {\"bytes\": 0, \"records\": 0}}'::jsonb" } }), RUVIA_DB_COLUMN(capability, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(mobile, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::jsonb, .default_expression_ = ruvia::fixed_string{ "'{}'::jsonb" } }), RUVIA_DB_COLUMN(group_id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .nullable_ = true }))

} // namespace service::edge::persistence

namespace service::edge::metadata {

// Redis Hash 以设备 ID 为动态字段，值为五个长度前缀字段的记录。
// Ruvia Redis ORM 的固定字段与键前缀不能表达此现有映射；整节点替换
// 仍由 service 中的 DEL/HSET 原子 Lua 执行，避免部分目录可见。
struct Device final {
    std::string linkId;
    std::string deviceCode;
    std::string protocol;
    std::string storagePolicy{ "report" };
    std::int64_t onlineWindowMs{ 300000 };
};

using NodeSnapshot = std::unordered_map<std::string, Device>;
using Catalog = std::unordered_map<std::string, NodeSnapshot>;

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

inline std::optional<std::string_view> takeField(std::string_view value, std::size_t& offset) noexcept {
    const auto colon = value.find(':', offset);
    if (colon == std::string_view::npos) {
        return std::nullopt;
    }
    std::size_t size{};
    const auto [end, error] =
        std::from_chars(value.data() + offset, value.data() + colon, size);
    if (error != std::errc{} || end != value.data() + colon || size > value.size() - colon - 1) {
        return std::nullopt;
    }
    const auto begin = colon + 1;
    offset = begin + size;
    return value.substr(begin, size);
}

inline std::optional<std::int64_t> integer(std::string_view value) noexcept {
    std::int64_t result{};
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size()) {
        return std::nullopt;
    }
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
        offset != value.size()) {
        return std::nullopt;
    }
    const auto online = integer(*onlineWindowMs);
    if (!validStoragePolicy(*storagePolicy) || !online || *online < 1000) {
        return std::nullopt;
    }
    return Device{ std::string(*linkId), std::string(*deviceCode), std::string(*protocol), std::string(*storagePolicy), *online };
}

// Replaces one node hash atomically so readers never observe a partial catalog.
inline constexpr std::string_view kStoreNodeScript = R"lua(
redis.call('DEL', KEYS[1])
for index = 1, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[index], ARGV[index + 1])
end
return #ARGV / 2
)lua";

// HSET field/value pairs of kStoreNodeScript for one node snapshot.
inline std::vector<std::string> storeArguments(const NodeSnapshot& snapshot) {
    std::vector<std::string> arguments;
    arguments.reserve(snapshot.size() * 2);
    for (const auto& [deviceId, device] : snapshot) {
        arguments.push_back(deviceId);
        arguments.push_back(encode(device));
    }
    return arguments;
}

} // namespace service::edge::metadata
