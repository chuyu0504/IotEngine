#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::string commandSource() {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
        "service/modules/command/command.service.h";
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open command service source");
    return { std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>() };
}

std::string projectSource(std::string_view relative) {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
        std::filesystem::path(relative);
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open command integration source");
    return { std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>() };
}

void requireNoUnsafeJsonCasts(std::string_view source) {
    require(source.find("COALESCE((protocol_params->>'remote_control')::boolean") == std::string_view::npos, "command service directly casts remote_control");
    require(source.find("COALESCE((d.protocol_params->>'remote_control')::boolean") == std::string_view::npos, "command service directly casts device remote_control");
    require(source.find("COALESCE((n.capability->>'deviceConfig')::boolean") == std::string_view::npos, "command service directly casts edge deviceConfig capability");
    require(source.find("COALESCE((n.status->'config'->>'activeVersion')::bigint") == std::string_view::npos, "command service directly casts edge activeVersion");
    require(source.find("COALESCE((item->>'writable')::boolean") == std::string_view::npos, "command service directly casts item writable");
    require(source.find("COALESCE((item->>'size')::integer") == std::string_view::npos, "command service directly casts item size");
    require(source.find("COALESCE((item->>'length')::integer") == std::string_view::npos, "command service directly casts item length");
    require(source.find("COALESCE((item->>'digits')::integer") == std::string_view::npos, "command service directly casts item digits");
}

void requireDuplicateElementRejected(std::string_view source) {
    require(source.find("seenElementIds") != std::string_view::npos, "command service does not track duplicate command elements");
    require(source.find("下发要素不能重复") != std::string_view::npos, "command service does not reject duplicate command elements");
}

} // namespace

int main() {
    try {
        const auto source = commandSource();
        const auto preparation = projectSource("service/features/command/command.service.h");
        requireNoUnsafeJsonCasts(source);
        requireNoUnsafeJsonCasts(preparation);
        requireDuplicateElementRejected(source);
        require(preparation.find("command->set_fast_read_duration_sec(10)") == std::string::npos, "edge commands still hard-code the fast-read window");
        require(preparation.find("command->set_fast_read_interval_sec(1)") == std::string::npos, "edge commands still hard-code the fast-read interval");
        require(preparation.find("text(routeQuery.column(std::tuple_element_t<service::command::persistence::DeviceModelEntity::column_index<\"config\">(), service::command::persistence::DeviceModelEntity::columns_type>::name.view(), \"p\"), \"commandFastReadDuration\")") != std::string::npos && preparation.find("text(routeQuery.column(std::tuple_element_t<service::command::persistence::DeviceModelEntity::column_index<\"config\">(), service::command::persistence::DeviceModelEntity::columns_type>::name.view(), \"p\"), \"commandFastReadInterval\")") != std::string::npos, "edge commands do not load the protocol fast-read policy");
        const auto statusSource = projectSource("service/modules/device/device.service.h");
        require(statusSource.find("DeviceCommandStatusesDto") != std::string::npos && statusSource.find("milliseconds(100)") == std::string::npos, "command status snapshots must not poll for results");
        require(statusSource.find("DeviceCommandActualValueDto") != std::string::npos && statusSource.find("actual_value_count") != std::string::npos && statusSource.find("result.template set<\"actualValues\">") != std::string::npos, "command status API omits readback values");
        const auto resultProjectionSource = projectSource("service/features/command/command.service.h");
        require(resultProjectionSource.find("actualValuesJson(message)") != std::string::npos && resultProjectionSource.find("actual_value_count") != std::string::npos, "command result projection omits readback values");
        require(resultProjectionSource.find("kCommandResultAcceptingStatuses") != std::string::npos && resultProjectionSource.find("expiredDispatchState(false)") != std::string::npos && resultProjectionSource.find("expiredDispatchState(true)") != std::string::npos, "command result projection permits late terminal-state downgrades or changes expired dispatch semantics");
        const auto collectorRuntime = projectSource("service/features/collector/collector.runtime.h");
        require(collectorRuntime.find("service::command::retryableFailure(reason)") != std::string::npos, "collector does not use command-domain retry policy");
        const auto pollingProtocol = projectSource("service/features/collector/polling/polling.protocol.h");
        require(pollingProtocol.find("polling_readback_timeout") != std::string::npos, "FINS readback timeout does not retain its ambiguous write context");
        const auto types = projectSource("service/modules/device/device.types.h");
        require(types.find("\"actual_values\", actualValues") != std::string::npos, "command response contract omits actual_values");
        std::cout << "command service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "command service test failed: " << error.what() << '\n';
        return 1;
    }
}
