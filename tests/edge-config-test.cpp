#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory_resource>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "service/config/schema.h"
#include "service/features/edge/edge.config.h"
#include "service/features/edge/edge.service.h"

#include "tests/database.fixture.h"

namespace {

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

void testPacketBytesRejectInvalidHex() {
    const auto bytes =
        service::edge::protocol::packetBytes("HEX", "01 0a FF", "heartbeat_payload");
    require(bytes == std::vector<std::uint8_t>{ 0x01, 0x0a, 0xff }, "edge config HEX packet changed valid bytes");

    bool rejectedBadDigit = false;
    try {
        (void)service::edge::protocol::packetBytes("HEX", "0G", "heartbeat_payload");
    } catch (const std::runtime_error& error) {
        rejectedBadDigit =
            std::string_view(error.what()).find("invalid edge config hex: heartbeat_payload") !=
            std::string_view::npos;
    }
    require(rejectedBadDigit, "edge config accepted a HEX packet with a bad digit");

    bool rejectedOddNibble = false;
    try {
        (void)service::edge::protocol::packetBytes("HEX", "ABC", "heartbeat_payload");
    } catch (const std::runtime_error& error) {
        rejectedOddNibble =
            std::string_view(error.what()).find("invalid edge config hex: heartbeat_payload") !=
            std::string_view::npos;
    }
    require(rejectedOddNibble, "edge config accepted a HEX packet with an odd nibble");
}

void testPacketBytesRejectUnknownMode() {
    bool rejected = false;
    try {
        (void)service::edge::protocol::packetBytes("BASE64", "AA==", "heartbeat_payload");
    } catch (const std::runtime_error& error) {
        rejected =
            std::string_view(error.what()).find("invalid edge config packet mode: heartbeat_payload") != std::string_view::npos;
    }
    require(rejected, "edge config accepted an unsupported packet mode");
}

void testReplaceQueueToleratesCorruptRevisionKey() {
    constexpr std::string_view script = service::edge::config::detail::kReplaceQueueScript;
    require(script.find("tonumber(redis.call('GET', KEYS[2]) or '0') or 0") != std::string_view::npos, "edge config replaceQueue can compare nil when Redis revision key is corrupt");
}

void testNumberRejectsTrailingGarbage() {
    require(service::edge::protocol::number("1.25", -1.0) == 1.25, "edge config decimal parser changed valid decimal");
    require(service::edge::protocol::number("1.25x", -1.0) == -1.0, "edge config decimal parser accepted trailing garbage");
}

void testRevisionSqlGuardsCorruptNodeJson() {
    using namespace service::edge::config::detail;
    constexpr std::string_view node = "019fd9f6-4be5-7272-a194-9e571bce848d";
    const auto queue = test::compileDatabaseStatement([node](auto& database) {
        return queueSnapshotQuery(database, node);
    });
    const auto requeue = test::compileDatabaseStatement([node](auto& database) {
        return requeueDesiredQuery(database, node);
    });
    const auto pending = test::compileDatabaseStatement([node](auto& database) {
        return requeuePendingQuery(database, node, 12, "rejected", "边缘固件尚未声明支持协议 DLT645");
    });
    const auto reject = test::compileDatabaseStatement([node](auto& database) {
        return rejectBuildQuery(database, "rejected", node, 12);
    });
    require(queue.sql.find("CASE WHEN") != std::string::npos && queue.sql.find("AS BIGINT") != std::string::npos, "edge config queueSnapshot does not guard numeric revisions");
    require(requeue.sql.find("CASE WHEN") != std::string::npos && pending.sql.find("CASE WHEN") != std::string::npos && reject.sql.find("CASE WHEN") != std::string::npos, "edge config revision updates do not guard numeric revisions");
    require(queue.parameterCount > 0, "edge config queueSnapshot no longer binds its input parameters");
}

void testConfigurationReplayAfterCapabilityRefresh() {
    using service::edge::config::detail::needsConfigurationReplay;
    require(needsConfigurationReplay(12, 0, "applied", ""), "a restarted node without its saved configuration was not restored");
    for (const auto protocol : { "MC", "FINS", "DLT645" }) {
        const auto reason = std::string("边缘固件尚未声明支持协议 ") + protocol;
        require(needsConfigurationReplay(12, 0, "rejected", reason), "a capability rejection remained permanent after fresh capabilities");
    }
    require(!needsConfigurationReplay(12, 0, "rejected", "invalid serial endpoint"), "a device configuration rejection was retried automatically");
    require(!needsConfigurationReplay(12, 0, "rejected", "边缘固件尚未声明支持协议 UNKNOWN"), "an unknown rejection was treated as a recoverable protocol capability");
    require(!needsConfigurationReplay(12, 12, "applied", ""), "an unchanged configuration was replayed");
    require(!needsConfigurationReplay(12, 13, "applied", ""), "an older server configuration replaced a newer node configuration");
    require(!needsConfigurationReplay(0, 0, "idle", ""), "an empty desired configuration triggered restoration");
    require(needsConfigurationReplay(12, 11, "pending", ""), "a pending newer configuration was not restored");
}

void testReadIntervalMigrationRemovesLegacyField() {
    const auto migration = std::find_if(
        service::config::kSchemaMigrations.begin(),
        service::config::kSchemaMigrations.end(),
        [](const auto& value) {
            return value.id() == "0026_unify_protocol_read_interval";
        }
    );
    require(migration != service::config::kSchemaMigrations.end(), "readInterval migration is missing");
    require(migration->sql().find("config - 'pollInterval'") != std::string_view::npos, "readInterval migration does not remove pollInterval");
    require(migration->sql().find("'{readInterval}'") != std::string_view::npos, "readInterval migration does not write the canonical field");
    require(migration->sql().find("ck_protocol_config_no_poll_interval") != std::string_view::npos, "schema does not prevent pollInterval from returning");
}

void testStoragePolicyMigrationRemovesLegacyField() {
    const auto migration = std::find_if(
        service::config::kSchemaMigrations.begin(),
        service::config::kSchemaMigrations.end(),
        [](const auto& value) {
            return value.id() == "0027_unify_protocol_storage_policy";
        }
    );
    require(migration != service::config::kSchemaMigrations.end(), "storagePolicy migration is missing");
    require(migration->sql().find("config - 'storageInterval'") != std::string_view::npos && migration->sql().find("'{storagePolicy}'") != std::string_view::npos, "storagePolicy migration does not replace the retired interval field");
    require(migration->sql().find("ELSE 'report'") != std::string_view::npos, "storagePolicy migration can silently reduce stored history");
    require(migration->sql().find("ck_protocol_config_storage_policy") != std::string_view::npos && migration->sql().find("COALESCE(config->>'storagePolicy' IN") != std::string_view::npos, "schema does not enforce a canonical storage policy");
}

void testEnrollmentMigrationRemovesRejectedState() {
    const auto migration = std::find_if(
        service::config::kSchemaMigrations.begin(),
        service::config::kSchemaMigrations.end(),
        [](const auto& value) {
            return value.id() == "0028_remove_edge_enrollment_rejection";
        }
    );
    require(migration != service::config::kSchemaMigrations.end(), "enrollment migration is missing");
    require(migration->sql().find("WHERE enrollment_status = 'rejected'") != std::string_view::npos, "enrollment migration does not migrate rejected registrations");
    require(migration->sql().find("IN ('pending', 'approved')") != std::string_view::npos, "schema still permits rejected registrations");
}

void testPublicBaseUrlConfiguration() {
    for (const auto value : { "", "ftp://secondary.example", "https://", "https:///host", "https://host/path with space" }) {
        require(!service::edge::config::validPublicBaseUrl(value), "invalid public URL accepted");
    }
    for (const auto value : { "https://secondary.example/", "http://127.0.0.1:55132", "https://host/base/" }) {
        require(service::edge::config::validPublicBaseUrl(value), "valid public URL rejected");
    }
    require(!service::edge::config::validPublicBaseUrl("https://host/" + std::string(256, 'a')), "oversized public URL accepted");
}

} // namespace

int main() {
    try {
        testPublicBaseUrlConfiguration();
        testPacketBytesRejectInvalidHex();
        testPacketBytesRejectUnknownMode();
        testReplaceQueueToleratesCorruptRevisionKey();
        testNumberRejectsTrailingGarbage();
        testRevisionSqlGuardsCorruptNodeJson();
        testConfigurationReplayAfterCapabilityRefresh();
        testReadIntervalMigrationRemovesLegacyField();
        testStoragePolicyMigrationRemovesLegacyField();
        testEnrollmentMigrationRemovesRejectedState();
        std::cout << "edge config tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "edge config test failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
