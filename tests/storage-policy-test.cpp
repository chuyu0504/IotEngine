#include <algorithm>
#include <asio.hpp>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <ruvia/web/db/db_schema.h>

#include "service/config/schema.h"
#include "service/config/storage.h"

namespace storage = service::config;

// The public migrator validates the complete plan before attempting a connection.
// Reserve a non-listening port so that a valid plan deterministically reaches
// connection failure, while invalid SQL or duplicate IDs still fail validation.
void validateMigrations(std::span<const ruvia::db_migration> migrations) {
    asio::io_context io;
    asio::ip::tcp::acceptor reserved(io);
    reserved.open(asio::ip::tcp::v4());
    reserved.bind({ asio::ip::address_v4::loopback(), 0 });
    ruvia::db_config config;
    config.driver_ = ruvia::db_driver::postgresql;
    config.tls_.mode_ = ruvia::client_tls_mode::disabled;
    config.port_ = reserved.local_endpoint().port();
    config.connect_timeout_ = std::chrono::milliseconds(10);
    try {
        (void)ruvia::db_migrator::migrate(config, migrations);
    } catch (const ruvia::db_error& error) {
        if (error.code() == ruvia::db_error::code_type::connect_failed ||
            error.code() == ruvia::db_error::code_type::timeout) {
            return;
        }
        throw;
    }
    throw std::runtime_error("reserved database port unexpectedly accepted a connection");
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <typename Action>
void requireInvalid(Action&& action, const char* message) {
    try {
        action();
    } catch (const std::invalid_argument&) {
        return;
    }
    throw std::runtime_error(message);
}

int main() {
    try {
        storage::DeviceDataStoragePolicy defaults;
        storage::validateDeviceDataStoragePolicy(defaults);
        require(defaults.compressionEnabled, "compression must default to enabled");
        require(defaults.chunkIntervalHours == 168, "device_data chunks must default to seven days");
        require(defaults.compressionAfterHours == 168, "device_data compression must default to seven days");
        require(defaults.mutableWindowHours == 48, "device_data mutable window must default to two days");

        require(storage::parseStoragePolicyFlag("flag", "ON"), "case-insensitive true flag parsing failed");
        require(!storage::parseStoragePolicyFlag("flag", "false"), "false flag parsing failed");
        require(storage::parseStoragePolicyHours("hours", "168") == 168, "hour parsing failed");
        requireInvalid(
            [] {
                (void)storage::parseStoragePolicyHours("hours", "7d");
            },
            "hour parsing must reject unit suffixes"
        );

        auto unsafe = defaults;
        unsafe.compressionAfterHours = unsafe.mutableWindowHours;
        requireInvalid(
            [&unsafe] {
                storage::validateDeviceDataStoragePolicy(unsafe);
            },
            "compression must not overlap the mutable write-back window"
        );

        unsafe.compressionEnabled = false;
        storage::validateDeviceDataStoragePolicy(unsafe);

        auto enabled = storage::deviceDataStoragePolicyMigration(defaults);
        require(enabled.id().starts_with("runtime_device_data_storage_policy_v2_"), "storage policy identity must use the v2 namespace");
        require(enabled.id().find("runtime_device_data_storage_policy_v1_") == std::string_view::npos, "storage policy identity must not reuse the v1 namespace");
        require(enabled.id().find("_on_chunk_168_after_168_mutable_48") != std::string::npos, "enabled policy identity is incomplete");
        require(enabled.atomicity() == ruvia::db_migration_atomicity::transactional, "storage policy migration must be transactional");
        require(enabled.sql().starts_with("DO "), "storage policy operations must compile into one transaction");
        require(enabled.sql().find("remove_compression_policy") != std::string::npos, "policy replacement must remove the previous job");
        require(enabled.sql().find("set_chunk_time_interval") != std::string::npos, "configured chunk interval was not rendered");
        require(enabled.sql().find("make_interval") != std::string::npos && enabled.sql().find("168") != std::string::npos, "configured policy intervals were not rendered");
        require(enabled.sql().find("add_compression_policy") != std::string::npos, "enabled policy must add the compression job");
        require(enabled.sql().find("DELETE FROM") != std::string::npos && enabled.sql().find("starts_with") != std::string::npos, "policy changes must clean prior policy migrations by prefix");
        require(enabled.sql().find("runtime_device_data_storage_policy_") != std::string::npos, "policy cleanup must cover the shared migration namespace");
        require(enabled.sql().find("runtime_device_data_storage_policy_v1_") == std::string::npos, "policy cleanup must not be limited to the v1 namespace");

        auto disabled = defaults;
        disabled.compressionEnabled = false;
        auto disabledMigration =
            storage::deviceDataStoragePolicyMigration(disabled);
        require(disabledMigration.id().starts_with("runtime_device_data_storage_policy_v2_off_"), "disabled policy identity must use the v2 off namespace");
        require(disabledMigration.atomicity() == ruvia::db_migration_atomicity::transactional, "disabled storage policy migration must be transactional");
        require(disabledMigration.sql().find("remove_compression_policy") != std::string::npos, "disabled policy must remove the compression job");
        require(disabledMigration.sql().find("set_chunk_time_interval") != std::string::npos, "disabled policy must still configure the chunk interval");
        require(disabledMigration.sql().find("add_compression_policy") == std::string::npos, "disabled policy must not add a compression job");

        auto alternate = defaults;
        alternate.chunkIntervalHours = 24;
        alternate.compressionAfterHours = 72;
        alternate.mutableWindowHours = 12;
        const auto alternateMigration =
            storage::deviceDataStoragePolicyMigration(alternate);
        require(alternateMigration.id().find("_chunk_24_after_72_mutable_12") != std::string::npos, "storage policy identity must include configured times");
        require(alternateMigration.sql().find("make_interval") != std::string::npos && alternateMigration.sql().find("24") != std::string::npos && alternateMigration.sql().find("72") != std::string::npos, "storage policy SQL must use configured interval values");
        require(alternateMigration.sql().find("runtime_device_data_storage_policy_") != std::string::npos, "alternate policy must clean the shared migration namespace");

        std::vector<ruvia::db_migration> migrations;
        migrations.reserve(storage::kSchemaMigrations.size() + 1);
        migrations.insert(migrations.end(), storage::kSchemaMigrations.begin(), storage::kSchemaMigrations.end());
        migrations.emplace_back(std::move(enabled));
        for (const auto& migration : migrations) {
            try {
                validateMigrations(
                    std::span<const ruvia::db_migration>(&migration, 1)
                );
            } catch (const std::invalid_argument& error) {
                throw std::runtime_error("invalid migration " + std::string(migration.id()) + ": " + error.what());
            }
        }
        validateMigrations(migrations);
        const std::vector<ruvia::db_migration> duplicateMigrations{
            ruvia::db_migration({ .id_ = "duplicate", .sql_ = "SELECT 1" }),
            ruvia::db_migration({ .id_ = "DUPLICATE", .sql_ = "SELECT 2" })
        };
        requireInvalid([&] {
            validateMigrations(duplicateMigrations);
        },
                       "public migrator accepted case-insensitive duplicate IDs");
        const std::vector<ruvia::db_migration> multiStatementMigration{
            ruvia::db_migration({ .id_ = "multiple", .sql_ = "SELECT 1; SELECT 2" })
        };
        requireInvalid([&] {
            validateMigrations(multiStatementMigration);
        },
                       "public migrator accepted multiple SQL statements");
        const auto latestValueMigration = std::find_if(
            storage::kSchemaMigrations.begin(),
            storage::kSchemaMigrations.end(),
            [](const auto& migration) {
                return migration.id() == "0021_device_latest_value";
            }
        );
        require(latestValueMigration != storage::kSchemaMigrations.end(), "latest-value projection migration is missing");
        require(latestValueMigration->sql().find("PRIMARY KEY (device_id, element_id)") != std::string::npos, "latest-value projection must enforce one row per device element");
        require(latestValueMigration->sql().find("ORDER BY history.device_id, point.key") != std::string::npos, "latest-value backfill must preserve per-element ordering");

        std::cout << "storage policy tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "storage policy test failed: " << error.what() << '\n';
        return 1;
    }
}
