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

std::string persistenceSource() {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
                "service/features/telemetry/telemetry.service.h";
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open telemetry persistence source");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void requireNoUnsafeUnsignedParsing(std::string_view source) {
    require(source.find("std::stoull(") == std::string_view::npos,
            "telemetry persistence uses unsafe/partial stoull parsing");
}

void requireStoragePolicySemantics(std::string_view source) {
    require(source.find("storage_interval") == std::string_view::npos,
            "telemetry persistence still applies interval-based history filtering");
    require(source.find("filtered.column(\"storage_policy\"), Op::equal, filtered.value(\"report\")") != std::string_view::npos &&
                source.find("filtered.column(\"storage_policy\"), Op::equal, filtered.value(\"change\")") != std::string_view::npos,
            "telemetry persistence does not implement both storage policies");
    require(source.find(".from(service::telemetry::persistence::DeviceLatestValueEntity::table_name(), \"latest\")") != std::string_view::npos &&
                source.find("unpackValue(incomingPoints, incomingPoints.column(\"value\", \"point\"))") != std::string_view::npos,
            "change storage does not compare point values with the latest read model");
    require(source.find("filtered.coalesce({ filtered.column(\"changed\", \"point_changes\"), filtered.value(false) })") != std::string_view::npos,
            "change storage accepts unchanged or empty telemetry");
    require(source.find(".from(\"valid_incoming\", \"incoming\")") != std::string_view::npos,
            "latest values are not refreshed for every valid report");
}

} // namespace

int main() {
    try {
        const auto source = persistenceSource();
        requireNoUnsafeUnsignedParsing(source);
        requireStoragePolicySemantics(source);
        std::cout << "telemetry persistence tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "telemetry persistence test failed: " << error.what() << '\n';
        return 1;
    }
}
