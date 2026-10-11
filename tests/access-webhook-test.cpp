#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

void require(bool condition, const char* message) {
    if (!condition)
        throw std::runtime_error(message);
}

std::string webhookSource(std::string_view relativePath) {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
                std::filesystem::path(relativePath);
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open access webhook source");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void requireNoUnsafeStoll(std::string_view source) {
    require(source.find("std::stoll(") == std::string_view::npos,
            "access webhook uses unsafe/partial stoll parsing");
}

void requireUnambiguousTlsVerificationFlag(std::string_view source) {
    require(source.find("catalog.case_when(") != std::string_view::npos &&
                source.find("catalog.column(service::access::persistence::OpenWebhookEntity::column_name<\"skip_tls_verify\">(), \"webhook\")") !=
                    std::string_view::npos &&
                source.find("catalog.value(\"1\")") != std::string_view::npos &&
                source.find("catalog.value(\"0\")") != std::string_view::npos &&
                source.find("skip_tls_verify::text") == std::string_view::npos,
            "webhook runtime does not project the PostgreSQL boolean explicitly");
}

void requireCanonicalBooleanPointValues(std::string_view source) {
    require(source.find("parsed->get<ruvia::string>(\"dataType\")") !=
                    std::string_view::npos &&
                source.find("telemetry::latest::canonicalPointJson(") !=
                    std::string_view::npos,
            "webhook payloads do not canonicalize BOOL points to 0/1");
}

} // namespace

int main() {
    try {
        const auto serviceSource = webhookSource("service/features/access/access.service.h");
        const auto entitySource = webhookSource("service/features/access/access.entity.h");
        requireNoUnsafeStoll(serviceSource);
        requireUnambiguousTlsVerificationFlag(serviceSource);
        requireCanonicalBooleanPointValues(entitySource);
        std::cout << "access webhook tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "access webhook test failed: " << error.what() << '\n';
        return 1;
    }
}
