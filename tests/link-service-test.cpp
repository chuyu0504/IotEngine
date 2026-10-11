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

std::string linkSource(std::string_view file) {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
                "service/modules/link" / file;
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open link source");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void requireNoUnsafeParsing(std::string_view source) {
    require(source.find("std::stoll") == std::string_view::npos,
            "link service uses unsafe/partial stoll parsing");
    require(source.find("COALESCE((endpoint->>'port')::integer") == std::string_view::npos,
            "link service directly casts endpoint port");
    require(source.find("R\"sql") == std::string_view::npos,
            "link service still embeds runtime SQL");
    require(source.find("c.db().query(\"") == std::string_view::npos,
            "link service still executes a raw SQL string");
    require(source.find("transaction.execute(\"") == std::string_view::npos,
            "link service still executes a raw transaction SQL string");
    require(source.find("ruvia::db_query") != std::string_view::npos,
            "link service does not use the public db_query API");
    require(source.find("LinkEntity") != std::string_view::npos,
            "link service does not use its ORM entity");
    require(source.find("c.pool()") != std::string_view::npos,
            "link service does not allocate queries from Context.pool()");
}

void requireInputValidation(std::string_view source) {
    require(source.find("if (!value || value->view().empty())") != std::string_view::npos,
            "link service accepts empty required strings");
    require(source.find("mode != \"TCP Server\" && mode != \"TCP Client\"") !=
                std::string_view::npos,
            "link service accepts unknown mode as TCP Client");
    require(source.find("protocol != \"SL651\" && protocol != \"Modbus\" && protocol != \"S7\"") !=
                std::string_view::npos,
            "link service accepts unknown protocol");
    require(source.find("status != \"enabled\" && status != \"disabled\"") !=
                std::string_view::npos,
            "link service accepts unknown link status");
    require(source.find("targetStatus != \"enabled\" && targetStatus != \"disabled\"") !=
                std::string_view::npos,
            "link service accepts unknown target status");
}

} // namespace

int main() {
    try {
        const auto source = linkSource("link.service.h");
        const auto types = linkSource("link.types.h");
        requireNoUnsafeParsing(source);
        requireInputValidation(source + types);
        require(source.find("static void validateConfiguration") != std::string::npos,
                "link service must enforce protocol and endpoint combinations");
        require(types.find("RUVIA_REQUIRED_FIELD(name") != std::string::npos,
                "link request must declare its required fields");
        std::cout << "link service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "link service test failed: " << error.what() << '\n';
        return 1;
    }
}
