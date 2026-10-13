#include "service/features/edge/edge.config.h"
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
#include "service/features/edge/edge.service.h"

namespace {

void require(bool condition, std::string_view message) {
    if (!condition)
        throw std::runtime_error(std::string(message));
}

std::string compileSql(const ruvia::db_query& query) {
    auto statement = query.compile(ruvia::db_driver::postgresql,
                                   std::pmr::new_delete_resource(),
                                   ruvia::db_parameter_mode::literal);
    return std::string(statement.sql());
}

void testPacketBytesRejectInvalidHex() {
    const auto bytes =
        service::edge::config::detail::packetBytes("HEX", "01 0a FF", "heartbeat_payload");
    require(bytes == std::vector<std::uint8_t>{0x01, 0x0a, 0xff},
            "edge config HEX packet changed valid bytes");

    bool rejectedBadDigit = false;
    try {
        (void)service::edge::config::detail::packetBytes("HEX", "0G", "heartbeat_payload");
    } catch (const std::runtime_error& error) {
        rejectedBadDigit =
            std::string_view(error.what()).find("invalid edge config hex: heartbeat_payload") !=
            std::string_view::npos;
    }
    require(rejectedBadDigit, "edge config accepted a HEX packet with a bad digit");

    bool rejectedOddNibble = false;
    try {
        (void)service::edge::config::detail::packetBytes("HEX", "ABC", "heartbeat_payload");
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
        (void)service::edge::config::detail::packetBytes("BASE64", "AA==", "heartbeat_payload");
    } catch (const std::runtime_error& error) {
        rejected =
            std::string_view(error.what()).find(
                "invalid edge config packet mode: heartbeat_payload") != std::string_view::npos;
    }
    require(rejected, "edge config accepted an unsupported packet mode");
}

void testReplaceQueueToleratesCorruptRevisionKey() {
    constexpr std::string_view script = service::edge::config::detail::kReplaceQueueScript;
    require(script.find("tonumber(redis.call('GET', KEYS[2]) or '0') or 0") !=
                std::string_view::npos,
            "edge config replaceQueue can compare nil when Redis revision key is corrupt");
}

void testNumberRejectsTrailingGarbage() {
    require(service::edge::config::detail::number("1.25", -1.0) == 1.25,
            "edge config decimal parser changed valid decimal");
    require(service::edge::config::detail::number("1.25x", -1.0) == -1.0,
            "edge config decimal parser accepted trailing garbage");
}

void testRevisionSqlGuardsCorruptNodeJson() {
    using namespace service::edge::config::detail;
    const auto queue = compileSql(queueSnapshotQuery("019fd9f6-4be5-7272-a194-9e571bce848d"));
    const auto requeue = compileSql(requeueDesiredQuery("019fd9f6-4be5-7272-a194-9e571bce848d"));
    const auto pending = compileSql(
        requeuePendingQuery("019fd9f6-4be5-7272-a194-9e571bce848d", 12));
    const auto reject = compileSql(
        rejectBuildQuery("rejected", "019fd9f6-4be5-7272-a194-9e571bce848d", 12));
    require(queue.find("CASE WHEN") != std::string_view::npos &&
                queue.find("AS BIGINT") != std::string_view::npos,
            "edge config queueSnapshot does not guard numeric revisions");
    require(requeue.find("CASE WHEN") != std::string_view::npos &&
                pending.find("CASE WHEN") != std::string_view::npos &&
                reject.find("CASE WHEN") != std::string_view::npos,
            "edge config revision updates do not guard numeric revisions");
    require(queue.find("^-?[0-9]{1,18}$") !=
                std::string_view::npos,
            "edge config queueSnapshot does not guard desiredVersion");
    require(queue.find("deviceConfig") !=
                std::string_view::npos,
            "edge config queueSnapshot does not guard deviceConfig capability");
}

void testBuildItemSqlAvoidsJsonCasts() {
    using namespace service::edge::config::detail;
    const auto items = compileSql(buildItemsQuery("019fd9f6-4be5-7272-a194-9e571bce848d"));
    const auto modbus = compileSql(appendModbusQuery("019fd9f6-4be5-7272-a194-9e571bce848d"));
    const auto s7 = compileSql(appendS7Query("019fd9f6-4be5-7272-a194-9e571bce848d"));
    const auto sl651 = compileSql(
        appendSl651ElementsQuery("019fd9f6-4be5-7272-a194-9e571bce848d"));
    require(items.find("AS NUMERIC") ==
                std::string_view::npos,
            "edge config buildItems directly casts readInterval");
    require(items.find("AS INTEGER") ==
                std::string_view::npos,
            "edge config buildItems directly casts online_timeout");
    require(items.find("E'port') AS") == std::string_view::npos,
            "edge config buildItems directly casts endpoint port");
    require(items.find("mergeGap") != std::string_view::npos,
            "edge config buildItems directly casts packet mergeGap");
    require(items.find("registration") == std::string_view::npos,
            "edge config still exports a device registration payload");
    require(items.find("readInterval") != std::string_view::npos,
            "edge config buildItems does not use the unified readInterval for reporting");
    require(items.find("pollInterval") == std::string_view::npos,
            "edge config buildItems still reads the retired pollInterval field");
    require(items.find("commandFastReadDuration") !=
                std::string_view::npos,
            "edge config buildItems ignores the configured fast-read window");
    require(items.find("commandFastReadInterval") !=
                std::string_view::npos,
            "edge config buildItems ignores the configured fast-read interval");
    require(modbus.find("scale") ==
                std::string_view::npos,
            "edge config Modbus query still reads retired native scale");
    require(s7.find("start") != std::string_view::npos,
            "edge config S7 query does not leave start for strict C++ parsing");
    require(sl651.find("length") != std::string_view::npos,
            "edge config SL651 query does not leave length for strict C++ parsing");
}

void testReadIntervalMigrationRemovesLegacyField() {
    const auto migration = std::find_if(
        service::config::kSchemaMigrations.begin(), service::config::kSchemaMigrations.end(),
        [](const auto& value) { return value.id() == "0026_unify_protocol_read_interval"; });
    require(migration != service::config::kSchemaMigrations.end(),
            "readInterval migration is missing");
    require(migration->sql().find("config - 'pollInterval'") != std::string_view::npos,
            "readInterval migration does not remove pollInterval");
    require(migration->sql().find("'{readInterval}'") != std::string_view::npos,
            "readInterval migration does not write the canonical field");
    require(migration->sql().find("ck_protocol_config_no_poll_interval") !=
                std::string_view::npos,
            "schema does not prevent pollInterval from returning");
}

void testStoragePolicyMigrationRemovesLegacyField() {
    const auto migration = std::find_if(
        service::config::kSchemaMigrations.begin(), service::config::kSchemaMigrations.end(),
        [](const auto& value) { return value.id() == "0027_unify_protocol_storage_policy"; });
    require(migration != service::config::kSchemaMigrations.end(),
            "storagePolicy migration is missing");
    require(migration->sql().find("config - 'storageInterval'") != std::string_view::npos &&
                migration->sql().find("'{storagePolicy}'") != std::string_view::npos,
            "storagePolicy migration does not replace the retired interval field");
    require(migration->sql().find("ELSE 'report'") != std::string_view::npos,
            "storagePolicy migration can silently reduce stored history");
    require(migration->sql().find("ck_protocol_config_storage_policy") !=
                std::string_view::npos &&
                migration->sql().find("COALESCE(config->>'storagePolicy' IN") !=
                    std::string_view::npos,
            "schema does not enforce a canonical storage policy");
}

void testEnrollmentMigrationRemovesRejectedState() {
    const auto migration = std::find_if(
        service::config::kSchemaMigrations.begin(), service::config::kSchemaMigrations.end(),
        [](const auto& value) { return value.id() == "0028_remove_edge_enrollment_rejection"; });
    require(migration != service::config::kSchemaMigrations.end(),
            "enrollment migration is missing");
    require(migration->sql().find("WHERE enrollment_status = 'rejected'") !=
                std::string_view::npos,
            "enrollment migration does not migrate rejected registrations");
    require(migration->sql().find("IN ('pending', 'approved')") !=
                std::string_view::npos,
            "schema still permits rejected registrations");
}

void testPublicBaseUrlConfiguration() {
    for (const auto value : {"", "ftp://secondary.example", "https://", "https:///host", "https://host/path with space"})
        require(!service::edge::config::validPublicBaseUrl(value), "invalid public URL accepted");
    for (const auto value : {"https://secondary.example/", "http://127.0.0.1:55132", "https://host/base/"})
        require(service::edge::config::validPublicBaseUrl(value), "valid public URL rejected");
    require(!service::edge::config::validPublicBaseUrl("https://host/" + std::string(256, 'a')),
            "oversized public URL accepted");
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
        testBuildItemSqlAvoidsJsonCasts();
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
