#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

void require(bool condition, std::string_view message) {
    if (!condition)
        throw std::runtime_error(std::string(message));
}

std::string readEdgeProjectionSource(std::string_view relativePath) {
    const auto sourcePath = std::filesystem::path(__FILE__).parent_path().parent_path() /
                            std::filesystem::path(relativePath);
    std::ifstream input(sourcePath, std::ios::binary);
    require(input.good(), "cannot open edge projector source");
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

} // namespace

int main() {
    try {
        const auto serviceSource =
            readEdgeProjectionSource("service/features/edge/edge.service.h");
        const auto protocolSource =
            readEdgeProjectionSource("service/features/edge/edge.protocol.h");
        const auto runtimeSource =
            readEdgeProjectionSource("service/features/edge/edge.runtime.h");
        require(serviceSource.find("COALESCE((edge_node.capability->>'terminal')::boolean") ==
                    std::string::npos,
                "edge projector directly casts terminal capability");
        require(serviceSource.find("COALESCE((status->'config'->>'activeVersion')::bigint") ==
                    std::string::npos,
                "edge projector directly casts activeVersion");
        require(serviceSource.find("COALESCE((status->'config'->>'desiredVersion')::bigint") ==
                    std::string::npos,
                "edge projector directly casts desiredVersion");
        require(serviceSource.find("COALESCE((node.status->'config'->>'desiredVersion')::bigint") ==
                    std::string::npos,
                "edge projector directly casts node desiredVersion");
        require(serviceSource.find("config::detail::jsonText(query, existingCapability, \"terminal\")") !=
                    std::string::npos,
                "edge projector does not guard terminal capability");
        require(serviceSource.find("query.case_when({{valid, query.cast(text, ruvia::db_data_type::big_int)}})") !=
                    std::string::npos,
                "edge projector does not guard desiredVersion");
        require(serviceSource.find("\"status\", \"last_seen_at\", \"updated_at\"") != std::string::npos,
                "edge hello insert does not record presence");
        require(serviceSource.find("\"last_seen_at\", query.call(\"now\")") != std::string::npos,
                "edge hello update does not refresh node presence");
        require(serviceSource.find("edge_node.enrollment_status IN ('pending', 'approved')") ==
                    std::string::npos,
                "edge hello update still carries rejected enrollment compatibility");
        require(serviceSource.find("result.actual_values_size()") != std::string::npos &&
                    serviceSource.find("actual_value_count") != std::string::npos &&
                    serviceSource.find("actual.has_value()") != std::string::npos &&
                    serviceSource.find("scalarText(actual.value())") != std::string::npos,
                "edge command projector drops physical readback values");
        require(protocolSource.find("return value.bool_value() ? \"true\" : \"false\";") ==
                    std::string::npos &&
                    protocolSource.find("return value.bool_value() ? \"1\" : \"0\";") !=
                    std::string::npos,
                "edge BOOL values are not projected as 0/1");
        require(protocolSource.find(",\\\"dataType\\\":\\\"") != std::string::npos &&
                    protocolSource.find("scalarKind(item.value())") != std::string::npos,
                "edge telemetry does not retain point data types");
        require(runtimeSource.find("projector_stream::stream(index, service::runtime::instanceId())") !=
                    std::string::npos,
                "edge projector does not consume its accepting worker's stream");
        std::cout << "edge projector tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "edge projector test failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
