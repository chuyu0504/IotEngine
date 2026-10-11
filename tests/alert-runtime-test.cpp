#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "service/features/alert/alert.runtime.h"
#include "service/modules/alert/alert.types.h"

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void requireAbsent(const std::string& haystack, const char* needle, const char* message) {
    require(haystack.find(needle) == std::string::npos, message);
}

std::string readSource(const char* relative) {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() / relative;
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open alert source");
    return { std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>() };
}

void expectInvalidConditions(std::string_view raw, const char* message) {
    bool rejected = false;
    try {
        service::alert::alertRequest::validateConditions(raw);
    } catch (const std::exception&) {
        rejected = true;
    }
    require(rejected, message);
}

int main() {
    try {
        const auto sql = service::alert::AlertEvaluationService::evaluationTailForTest();
        require(sql.find("^[0-9]{1,10}$") != std::string::npos, "offline duration must be guarded before casting");
        require(sql.find("^-?[0-9]+([.][0-9]+)?$") != std::string::npos, "numeric conditions must be guarded before casting");
        require(sql.find("^([0-9]|[1-5][0-9]|6[0-2])$") != std::string::npos, "bit index must be bounded before shifting");
        require(sql.find("int8shr") != std::string::npos, "bit conditions must use the parsed index");
        require(sql.find("CASE WHEN") != std::string::npos, "numeric casts must retain guarded CASE expressions");
        const auto refreshSql = service::alert::metadata::detail::refreshQuery().compile(ruvia::db_driver::postgresql, nullptr, ruvia::db_parameter_mode::literal);
        require(refreshSql.sql().find("^[0-9]{1,10}$") != std::string_view::npos, "metadata refresh must guard offline duration");
        const auto metadataSource = readSource("service/features/alert/alert.service.h");
        requireAbsent(metadataSource, "std::stoll(", "alert metadata uses unsafe/partial stoll parsing");
        const auto runtimeSource = readSource("service/features/alert/alert.runtime.h");
        requireAbsent(runtimeSource, "std::stoull(", "alert runtime uses unsafe/partial stoull parsing");
        const auto serviceSource = readSource("service/modules/alert/alert.types.h");
        require(serviceSource.find("RUVIA_OPTIONAL_FIELD(remark, ruvia::string") != std::string::npos, "alert remark must have a typed model field");
        require(serviceSource.find("silenceDuration, ruvia::int64") != std::string::npos, "alert silence duration must have an integer model field");

        service::alert::alertRequest::validateConditions(
            R"([{"type":"threshold","elementKey":"temperature","operator":">","value":"12.5"}])"
        );
        service::alert::alertRequest::validateConditions(
            R"([{"type":"offline","duration":300}])"
        );
        service::alert::alertRequest::validateConditions(
            R"([{"type":"rate_of_change","elementKey":"flow","changeRate":"15","changeDirection":"rise"}])"
        );
        service::alert::alertRequest::validateConditions(
            R"([{"type":"threshold","elementKey":"a,]\"b","operator":"==","value":"quoted\\value","extra":{"nested":[{},[1,2]]}},{"type":"offline","duration":60}])"
        );
        expectInvalidConditions("[ ]", "empty spaced array was accepted");
        expectInvalidConditions(R"([{"type":"offline"},])", "trailing comma was accepted");
        expectInvalidConditions(R"([{"type":"offline"}] {})", "trailing JSON was accepted");
        expectInvalidConditions(R"({"type":"offline"})", "non-array conditions were accepted");
        expectInvalidConditions(
            R"([{"type":"threshold","elementKey":"temperature","operator":">","value":"abc"}])",
            "alert rule accepted a non-numeric threshold for a numeric operator"
        );
        expectInvalidConditions(
            R"([{"type":"rate_of_change","elementKey":"flow","changeRate":"abc"}])",
            "alert rule accepted a non-numeric rate-of-change threshold"
        );
        expectInvalidConditions(
            R"([{"type":"threshold","elementKey":"status","operator":"==","value":"1","bitIndex":"bad"}])",
            "alert rule accepted a malformed bitIndex"
        );
        expectInvalidConditions(R"([{"type":"threshold","operator":">","value":"1"}])", "alert rule accepted a condition without elementKey");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "alert runtime test failed: " << error.what() << '\n';
        return 1;
    }
}
