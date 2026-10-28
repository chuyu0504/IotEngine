#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/web/db/db_schema.h>
#include <ruvia/web/dotenv.h>

namespace service::config {

struct DeviceDataStoragePolicy final {
    bool compressionEnabled{ true };
    std::int64_t chunkIntervalHours{ 7 * 24 };
    std::int64_t compressionAfterHours{ 7 * 24 };
    std::int64_t mutableWindowHours{ 2 * 24 };
};

inline constexpr std::int64_t kMaximumStoragePolicyHours = 10 * 365 * 24;

inline bool parseStoragePolicyFlag(std::string_view name, std::string_view value) {
    std::string normalized(value);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (normalized == "1" || normalized == "true" || normalized == "yes" ||
        normalized == "on") {
        return true;
    }
    if (normalized == "0" || normalized == "false" || normalized == "no" ||
        normalized == "off") {
        return false;
    }
    throw std::invalid_argument(std::string(name) + " must be true/false, yes/no, on/off, or 1/0");
}

inline std::int64_t parseStoragePolicyHours(std::string_view name, std::string_view value, bool allowZero = false) {
    std::int64_t hours{};
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), hours);
    const auto minimum = allowZero ? 0 : 1;
    if (error != std::errc{} || end != value.data() + value.size() ||
        hours < minimum || hours > kMaximumStoragePolicyHours) {
        throw std::invalid_argument(std::string(name) + " must be an integer between " + std::to_string(minimum) + " and " + std::to_string(kMaximumStoragePolicyHours));
    }
    return hours;
}

inline void validateDeviceDataStoragePolicy(const DeviceDataStoragePolicy& policy) {
    if (policy.chunkIntervalHours <= 0 ||
        policy.chunkIntervalHours > kMaximumStoragePolicyHours) {
        throw std::invalid_argument(
            "DEVICE_DATA_CHUNK_INTERVAL_HOURS is outside the supported range"
        );
    }
    if (policy.compressionAfterHours <= 0 ||
        policy.compressionAfterHours > kMaximumStoragePolicyHours) {
        throw std::invalid_argument(
            "DEVICE_DATA_COMPRESSION_AFTER_HOURS is outside the supported range"
        );
    }
    if (policy.mutableWindowHours < 0 ||
        policy.mutableWindowHours > kMaximumStoragePolicyHours) {
        throw std::invalid_argument(
            "DEVICE_DATA_MUTABLE_WINDOW_HOURS is outside the supported range"
        );
    }
    if (policy.compressionEnabled &&
        policy.compressionAfterHours <= policy.mutableWindowHours) {
        throw std::invalid_argument(
            "DEVICE_DATA_COMPRESSION_AFTER_HOURS must be greater than "
            "DEVICE_DATA_MUTABLE_WINDOW_HOURS while compression is enabled"
        );
    }
}

inline DeviceDataStoragePolicy deviceDataStoragePolicy(const ruvia::env& env) {
    DeviceDataStoragePolicy policy;
    if (const auto value = env.get("DEVICE_DATA_COMPRESSION_POLICY_ENABLED")) {
        policy.compressionEnabled =
            parseStoragePolicyFlag("DEVICE_DATA_COMPRESSION_POLICY_ENABLED", *value);
    }
    if (const auto value = env.get("DEVICE_DATA_CHUNK_INTERVAL_HOURS")) {
        policy.chunkIntervalHours =
            parseStoragePolicyHours("DEVICE_DATA_CHUNK_INTERVAL_HOURS", *value);
    }
    if (const auto value = env.get("DEVICE_DATA_COMPRESSION_AFTER_HOURS")) {
        policy.compressionAfterHours =
            parseStoragePolicyHours("DEVICE_DATA_COMPRESSION_AFTER_HOURS", *value);
    }
    if (const auto value = env.get("DEVICE_DATA_MUTABLE_WINDOW_HOURS")) {
        policy.mutableWindowHours =
            parseStoragePolicyHours("DEVICE_DATA_MUTABLE_WINDOW_HOURS", *value, true);
    }
    validateDeviceDataStoragePolicy(policy);
    return policy;
}

inline ruvia::db_migration
deviceDataStoragePolicyMigration(const DeviceDataStoragePolicy& policy) {
    validateDeviceDataStoragePolicy(policy);

    std::string migrationId = "runtime_device_data_storage_policy_v2_";
    migrationId += policy.compressionEnabled ? "on" : "off";
    migrationId += "_chunk_" + std::to_string(policy.chunkIntervalHours);
    migrationId += "_after_" + std::to_string(policy.compressionAfterHours);
    migrationId += "_mutable_" + std::to_string(policy.mutableWindowHours);

    ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
    ruvia::db_query cleanup;
    const auto migrationPrefix = cleanup.value(
        "runtime_device_data_storage_policy_"
    );
    cleanup.delete_from("sys_schema_migrations")
        .where(cleanup.call("starts_with", { cleanup.column("migration_id"), migrationPrefix }));
    schema.execute(cleanup);

    ruvia::db_query expressions;
    const std::array chunkIntervalArguments{ ruvia::db_named_argument{
        "hours",
        expressions.value(policy.chunkIntervalHours) } };
    const auto chunkInterval = expressions.call("make_interval", std::span<const ruvia::db_expression>{}, chunkIntervalArguments);
    schema.remove_compression_policy("device_data", true);
    schema.set_chunk_time_interval("device_data", chunkInterval);

    if (policy.compressionEnabled) {
        const std::array compressionAfterArguments{ ruvia::db_named_argument{
            "hours",
            expressions.value(policy.compressionAfterHours) } };
        const auto compressionAfter = expressions.call("make_interval", std::span<const ruvia::db_expression>{}, compressionAfterArguments);
        schema.add_compression_policy("device_data", compressionAfter, true);
    }

    auto migrations = schema.compile(migrationId);
    if (migrations.size() != 1) {
        throw std::logic_error(
            "device_data storage policy must compile to one migration"
        );
    }
    return std::move(migrations.front());
}

} // namespace service::config
