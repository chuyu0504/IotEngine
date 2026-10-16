#pragma once

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

#include <ruvia/web/db/db_entity.h>

// edge_firmware 由 gateway 拥有（固件分块与回收）；edge_task 复用
// projection.entity.h（主写方为投影），command_attempt/command_operation 复用
// command.entity.h（service::command::persistence）。
namespace service::edge::persistence {

RUVIA_DB_ENTITY(EdgeFirmwareEntity, "edge_firmware", RUVIA_DB_COLUMN(id, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid, .primary_key_ = true }), RUVIA_DB_COLUMN(version, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64 }), RUVIA_DB_COLUMN(file_name, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 255 }), RUVIA_DB_COLUMN(storage_path, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::text }), RUVIA_DB_COLUMN(sha256, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64 }), RUVIA_DB_COLUMN(size_bytes, std::int64_t, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::big_int }), RUVIA_DB_COLUMN(download_token, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::varchar, .length_ = 64 }), RUVIA_DB_COLUMN(created_by, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::uuid }), RUVIA_DB_COLUMN(created_at, std::pmr::string, ruvia::db_column_options{ .data_type_ = ruvia::db_data_type::timestamp_tz, .default_expression_ = ruvia::fixed_string{ "now()" } }))

} // namespace service::edge::persistence

namespace service::edge::gateway {

// 认证投影使用 Redis String 存储 nodeId|status；固定 Hash ORM 无法读取此标量格式。
struct EnrollmentRecord final {
    std::string nodeId;
    std::string status;

    static std::optional<EnrollmentRecord> decode(std::string_view value) {
        const auto separator = value.find('|');
        if (separator == std::string_view::npos) {
            return std::nullopt;
        }
        return EnrollmentRecord{ std::string(value.substr(0, separator)),
                                 std::string(value.substr(separator + 1)) };
    }
};

// Redis String 保存设备返回的 Protobuf 字节；固定 Hash ORM 不能映射此标量格式。
// 键为 service::message::edge 的 logResultKey、logSnapshotKey、logLevelResultKey；
// 请求结果带过期时间，成功快照持续保留，期限和写入顺序由 GatewayService 管理。
struct LogResultRecord final {
    std::string protobufBytes;
};

} // namespace service::edge::gateway
