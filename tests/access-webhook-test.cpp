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

void requireCanonicalBooleanPointValues(std::string_view source) {
    require(source.find("parsed->get<ruvia::string>(\"dataType\")") !=
                    std::string_view::npos,
            "webhook payloads do not canonicalize BOOL points to 0/1");
}

} // namespace

int main() {
    try {
        const auto serviceSource = webhookSource("service/features/access/access.service.h");
        const auto entitySource = webhookSource("service/features/access/access.entity.h");
        requireNoUnsafeStoll(serviceSource);
        requireCanonicalBooleanPointValues(entitySource);
        std::cout << "access webhook tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "access webhook test failed: " << error.what() << '\n';
        return 1;
    }
}
