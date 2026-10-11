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

std::string protocolSource(std::string_view file) {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
                "service/modules/protocol" / file;
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot open protocol service source");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void requireNoUnsafeParsing(std::string_view source) {
    require(source.find("std::stoll") == std::string_view::npos,
            "protocol service uses unsafe/partial stoll parsing");
    require(source.find("COALESCE((value->>'enabled')::boolean") == std::string_view::npos,
            "protocol service directly casts create enabled");
    require(source.find("THEN (body.value->>'enabled')::boolean") == std::string_view::npos,
            "protocol service directly casts update enabled");
    require(source.find("R\"sql") == std::string_view::npos,
            "protocol service still embeds runtime SQL");
    require(source.find("c.db().query(\"") == std::string_view::npos,
            "protocol service still executes a raw SQL string");
    require(source.find("transaction.execute(\"") == std::string_view::npos,
            "protocol service still executes a raw transaction SQL string");
    require(source.find("ruvia::db_query") != std::string_view::npos,
            "protocol service does not use the public db_query API");
    require(source.find("ProtocolConfigEntity") != std::string_view::npos,
            "protocol service does not use its ORM entity");
    require(source.find("c.pool()") != std::string_view::npos,
            "protocol service does not allocate queries from Context.pool()");
    require(source.find("canonicalNumericText(value.view())") != std::string_view::npos,
            "protocol integer validation does not preserve PostgreSQL exponent normalization");
    require(source.find("compareNumeric(*text, \"1000000000\")") != std::string_view::npos,
            "protocol decimal validation does not compare numeric values exactly");
    require(source.find("std::strtod") == std::string_view::npos,
            "protocol numeric validation rounds JSON numbers through double precision");
    require(source.find("1e3 becomes 1000") != std::string_view::npos,
            "protocol regression coverage does not document exponent normalization");
    require(source.find("1000000000.000000000001") != std::string_view::npos,
            "protocol regression coverage does not document exact decimal bounds");
}

void requirePartialModbusUpdateValidation(std::string_view source) {
    require(source.find("(required && !hasField(*config, \"byteOrder\"))") !=
                std::string_view::npos,
            "protocol service requires Modbus byteOrder only on create");
    require(source.find("(required && !registers)") != std::string_view::npos,
            "protocol service requires Modbus registers only on create");
    require(source.find("if (packetValue && (!packetValue->is_object() || !packet))") != std::string_view::npos,
            "protocol service does not validate Modbus packet objects");
}

void requireStrictUpdateFieldTypes(std::string_view source) {
    require(source.find("if (const auto requested = payload.get<ruvia::string>(\"protocol\");") ==
                std::string_view::npos,
            "protocol update ignores present non-string protocol fields");
    require(source.find("if (const auto name = payload.get<ruvia::string>(\"name\"))") ==
                std::string_view::npos,
            "protocol update writes present non-string name fields through SQL");
    require(source.find("RUVIA_OPTIONAL_FIELD(protocol, ruvia::string") != std::string_view::npos,
            "protocol update does not declare a typed protocol field");
    require(source.find("RUVIA_OPTIONAL_FIELD(name, ruvia::string") != std::string_view::npos,
            "protocol update does not declare a typed name field");
    require(source.find("RUVIA_OPTIONAL_FIELD(remark, ruvia::string") != std::string_view::npos,
            "protocol update does not declare a nullable string remark field");
}

void requireModbusRuntimeFieldValidation(std::string_view source) {
    require(source.find("Modbus 配置的 readInterval 无效") != std::string_view::npos,
            "protocol service does not validate Modbus readInterval");
    require(source.find("Modbus 配置的 packet.mergeGap 无效") != std::string_view::npos,
            "protocol service does not validate Modbus packet mergeGap");
    require(source.find("Modbus 配置的 packet.maxQuantity 无效") != std::string_view::npos,
            "protocol service does not validate Modbus packet maxQuantity");
    require(source.find("Modbus 寄存器 scale 无效") != std::string_view::npos,
            "protocol service does not validate Modbus register scale");
    require(source.find("Modbus 寄存器 decimals 无效") != std::string_view::npos,
            "protocol service does not validate Modbus register decimals");
    require(source.find("Modbus 寄存器 writable 必须是布尔值") != std::string_view::npos,
            "protocol service does not validate Modbus register writable");
}

void requireUnifiedReadIntervalValidation(std::string_view source) {
    require(source.find("S7 配置的 readInterval 无效") != std::string_view::npos,
            "protocol service does not validate S7 readInterval");
    require(source.find("S7 配置不允许 pollInterval，请使用 readInterval") !=
                std::string_view::npos,
            "protocol service still accepts S7 pollInterval");
    require(source.find("Modbus 配置不允许 pollInterval，请使用 readInterval") !=
                std::string_view::npos,
            "protocol service still accepts Modbus pollInterval");
}

void requireUnifiedStoragePolicyValidation(std::string_view source) {
    require(source.find("配置不允许 storageInterval，请使用 storagePolicy") !=
                std::string_view::npos,
            "protocol service still accepts the retired storage interval");
    require(source.find("\"report\", \"change\"") !=
                std::string_view::npos,
            "protocol service does not validate canonical storage policies");
    require(source.find("配置的 storagePolicy 不能为空") != std::string_view::npos,
            "protocol create does not require a storage policy");
}

void requireQualifiedJsonArrayValidation(std::string_view source) {
    require(source.find("jsonb_array_elements") ==
                std::string_view::npos,
            "protocol validation still runs JSON array SQL");
    require(source.find("ruvia::array<S7Area>") !=
                std::string_view::npos,
            "S7 validation does not walk arrays through the JSON API");
    require(source.find("ruvia::array<ModbusRegister>") !=
                std::string_view::npos,
            "Modbus validation does not walk arrays through the JSON API");
}

void requireEdgeSyncDoesNotLeakAsUpdateFailure(std::string_view source) {
    require(source.find("db_unary_operator::is_not_null") != std::string_view::npos,
            "protocol update can queue empty edge node IDs");
    require(source.find("protocol edge config sync failed") != std::string_view::npos,
            "protocol update does not isolate edge config sync failures");
    require(source.find("catch (const std::exception& error)") != std::string_view::npos,
            "protocol update does not catch edge config sync exceptions");
    require(source.find("enqueueConfigEvent(transaction, \"protocol\", \"updated\"") !=
                std::string_view::npos,
            "protocol update does not enqueue its event transactionally");
    require(source.find("publishConfigEvent") == std::string_view::npos,
            "protocol update still performs a database/Redis dual write");
    require(source.find("logPostUpdateFailure(\"edge-sync\"") != std::string_view::npos,
            "protocol update does not isolate edge sync query failures");
    require(source.find("protocol post-update ") != std::string_view::npos,
            "protocol update does not log post-update failures");
    require(source.find("catch (...)") != std::string_view::npos,
            "protocol update does not catch non-std post-update exceptions");
    require(source.find("error=unknown exception") != std::string_view::npos,
            "protocol update does not log non-std edge sync exceptions");
    require(source.find(".distinct()") != std::string_view::npos,
            "protocol edge sync does not deduplicate edge nodes");
}

} // namespace

int main() {
    try {
        const auto service = protocolSource("protocol.service.h");
        const auto types = protocolSource("protocol.types.h");
        const auto source = service + types;
        require(service.find("protocol.schema.h") == std::string::npos,
                "protocol service depends on request schema");
        require(types.find("RUVIA_MODEL(UpdateProtocolBody,") != std::string::npos,
                "protocol update value has no request parser");
        require(service.find("ProtocolConfigurationRules::validateConfig") != std::string::npos,
                "protocol service does not enforce persisted protocol configuration rules");
        require(source.find("ruvia::detail::") == std::string::npos,
                "protocol uses private framework APIs");
        requireNoUnsafeParsing(source);
        requirePartialModbusUpdateValidation(service);
        requireStrictUpdateFieldTypes(source);
        requireModbusRuntimeFieldValidation(service);
        requireUnifiedReadIntervalValidation(service);
        requireUnifiedStoragePolicyValidation(service);
        requireQualifiedJsonArrayValidation(service);
        requireEdgeSyncDoesNotLeakAsUpdateFailure(service);
        std::cout << "protocol service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "protocol service test failed: " << error.what() << '\n';
        return 1;
    }
}
