#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory_resource>
#include <stdexcept>
#include <string>

#include "service/modules/open_access/open_access.service.h"

namespace {

void require(bool condition, const char* message) {
    if (!condition)
        throw std::runtime_error(message);
}

std::string accessSource(std::string_view file) {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
                "service/modules/open_access" / file;
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open access service source");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

std::string accessFeatureSource() {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
                "service/features/access/access.service.h";
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open access feature service source");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void requireNoUnsafeParsers(std::string_view source) {
    require(source.find("COALESCE((protocol_params->>'remote_control')::boolean") ==
                std::string_view::npos,
            "access service directly casts remote_control");
    require(source.find("std::stoll(") == std::string_view::npos,
            "access service uses unsafe/partial stoll parsing");
}

void requireStrictPresentFieldTypes(std::string_view source) {
    require(source.find("RUVIA_OPTIONAL_FIELD(name, ruvia::string,") != std::string_view::npos,
            "access request does not validate optional string fields through Model");
    require(source.find("RUVIA_OPTIONAL_FIELD(timeoutSeconds, ruvia::int64,") != std::string_view::npos,
            "access request does not validate optional integer fields through Model");
    require(source.find(
                "const auto scopes = payload.get<ruvia::array<ruvia::string>>(\"scopes\")") ==
                std::string_view::npos,
            "access key update ignores present non-array scopes");
    require(source.find(
                "const auto accessKeyId = payload.get<ruvia::string>(\"accessKeyId\")") ==
                std::string_view::npos,
            "webhook update ignores present non-string accessKeyId");
    require(source.find(
                "const auto events = payload.get<ruvia::array<ruvia::string>>(\"eventTypes\")") ==
                std::string_view::npos,
            "webhook update ignores present non-array eventTypes");
    require(source.find("RUVIA_OPTIONAL_FIELD(skipTlsVerify, ruvia::bool_value)") != std::string_view::npos,
            "webhook TLS verification flag is not strictly parsed as boolean");
    for (const auto field : {"expiresAt", "remark", "secret"}) {
        require(source.find(std::string("payload.is_present<\"") + field + "\">()") != std::string_view::npos,
                "nullable update field lost its original input presence");
    }
}

void requirePublicHistoryQueryShape() {
    const auto query = service::open_access::AccessService::publicHistoryQuery(
        std::pmr::new_delete_resource(), "00000000-0000-0000-0000-000000000001",
        "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", 20, 40, 3);
    const auto statement = query.compile(ruvia::db_driver::postgresql,
                                         std::pmr::new_delete_resource(),
                                         ruvia::db_parameter_mode::literal);
    const std::string sql(statement.sql());
    for (const auto* fragment : {"device_data", "jsonb_typeof", "to_jsonb", "jsonb_set",
                                 "'JPEG'", "'points'", "'device'", "'totalPages'",
                                 "iot_utc_timestamp", "deleted_at"}) {
        require(sql.find(fragment) != std::string::npos,
                "public device history SQL lost BOOL 0/1 canonicalization or /api output shape");
    }
    require(sql.find("LIMIT 20") != std::string::npos && sql.find("OFFSET 40") != std::string::npos,
            "public device history SQL lost pagination");
}

void requireAccessQueriesUseOrm(std::string_view source) {
    require(source.find("#include <ruvia/web/db/db_query.h>") != std::string_view::npos,
            "access feature service does not include the pinned db_query API");
    require(source.find("R\"sql") == std::string_view::npos &&
                source.find("transaction.query(\"") == std::string_view::npos &&
                source.find("context.db().query(\"") == std::string_view::npos &&
                source.find("context.db().execute(\"") == std::string_view::npos,
            "access feature service still embeds raw SQL");
    require(source.find("transaction.query(advisoryLock)") != std::string_view::npos,
            "access session refresh does not use a db_query advisory lock");
    require(source.find("snapshot.filter(") != std::string_view::npos && source.find(".join_function(ruvia::db_join_type::cross") != std::string_view::npos, "access projections lost aggregate FILTER or LATERAL JSON semantics");
    require(source.find(".distinct_on({latest.column(\"target_id\", \"incoming\")})") !=
                std::string_view::npos &&
                source.find(".distinct_on({latestUsage.column(\"access_key_id\", \"incoming\")})") !=
                    std::string_view::npos,
            "access batch persistence lost DISTINCT ON selection");
    require(source.find(".with(\"updated\", updated)") != std::string_view::npos && source.find(".with(\"usage_updated\", usageUpdated)") != std::string_view::npos, "access batch persistence lost data-changing CTEs");
}

} // namespace

int main() {
    try {
        const auto source = accessSource("open_access.service.h");
        const auto types = accessSource("open_access.types.h");
        const auto featureSource = accessFeatureSource();
        requireNoUnsafeParsers(source);
        requireStrictPresentFieldTypes(source + types);
        require(source.find("static void validateWebhookUrl") == std::string::npos,
                "access service still defines URL structure validation");
        require(source.find("open_access.schema.h") == std::string::npos,
                "access service still depends on request schema");
        require(types.find("RUVIA_CUSTOM(\"Webhook 地址必须是有效的 HTTP(S) URL\", isWebhookUrl)") != std::string::npos,
                "webhook input does not validate its URL before business execution");
        requirePublicHistoryQueryShape();
        requireAccessQueriesUseOrm(featureSource);
        std::cout << "access service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "access service test failed: " << error.what() << '\n';
        return 1;
    }
}
