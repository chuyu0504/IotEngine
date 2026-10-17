#include <iostream>
#include <stdexcept>
#include "service/features/telemetry/telemetry.protocol.h"
#include "service/utils/uuid.h"

void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
int main() {
    service::common::UuidV7State uuidState;
    try {
        service::message::ParsedDeviceMessage input;
        input.acquisitionId = service::utils::nextUuid(uuidState);
        input.observedAtMs=123;input.occurredAtMs=456;
        input.valuesJson=R"({"values":{"n":{"value":12.5,"unit":"C"},"b":{"value":true},"s":{"value":"0012"},"nil":{"value":null},"text":{"value":"\"type\":\"JPEG\""}}})";
        service::telemetry::contract::normalize(input);
        const auto decoded=ruvia::json_value::parse(input.valuesJson);
        require(decoded.has_value(),"normalized telemetry is invalid JSON");
        require(input.eventKind=="sample","text accidentally classified as media");
        require(input.valuesJson.find("\"model\":null")!=std::string::npos,"absent model provenance was fabricated");
        require(input.valuesJson.find("\"value_type\":\"number\"")!=std::string::npos,"numeric type lost");
        require(input.valuesJson.find("\"value_type\":\"boolean\"")!=std::string::npos,"boolean type lost");
        require(input.valuesJson.find("\"value\":\"0012\"")!=std::string::npos,"string precision changed");
        require(input.valuesJson.find("\"quality\":\"missing\"")!=std::string::npos,"null quality not represented");
        require(input.valuesJson.find("\"sample_time_ms\":123")!=std::string::npos,"sample time changed");
        require(input.valuesJson.find("\"received_time_ms\":456")!=std::string::npos,"receipt time changed");
        const auto once=input.valuesJson;
        service::telemetry::contract::normalize(input);
        require(input.valuesJson==once,"normalization is not idempotent");
        input.modelId="00000000-0000-7000-8000-000000000002";
        input.valuesJson=R"({"values":{"image":{"type":"JPEG","value":"INVALID_JPEG"}}})";
        service::telemetry::contract::normalize(input);
        require(input.eventKind=="image","JPEG was not classified as media");
        require(input.valuesJson.find("\"quality\":\"invalid\"")!=std::string::npos,"invalid media quality lost");
        require(input.valuesJson.find("\"revision\"")==std::string::npos,"removed model revision was emitted");
        input.valuesJson=R"({"values":{"bad":{"unit":"C"}}})";
        bool rejected=false;
        try {service::telemetry::contract::normalize(input);}catch(const std::runtime_error&){rejected=true;}
        require(rejected,"missing values silently accepted");
        input.protocol = "SL651";
        input.valuesJson = R"({"function_code":"2F","values":{ }})";
        service::telemetry::contract::normalize(input);
        require(service::telemetry::contract::isSl651EmptyReport(input), "SL651 heartbeat was treated as an element sample");
        input.valuesJson = R"({"function_code":"32","values":{}})";
        require(service::telemetry::contract::isSl651EmptyReport(input), "SL651 empty report would create an empty history row");
        input.valuesJson = R"({"function_code":"32","values":{"flow":{"value":0}}})";
        require(!service::telemetry::contract::isSl651EmptyReport(input), "SL651 zero measurement was discarded");
        input.valuesJson = R"({"values":{"flow":{"value":null}}})";
        require(!service::telemetry::contract::isSl651EmptyReport(input), "SL651 missing-quality point was discarded");
        input.protocol = "Modbus";
        input.valuesJson = R"({"values":{}})";
        require(!service::telemetry::contract::isSl651EmptyReport(input), "SL651 empty report rule leaked to another protocol");
        input.protocol = "SL651";
        input.deviceId = "00000000-0000-7000-8000-000000000003";
        input.messageId = service::utils::nextUuid(uuidState);
        input.rawPayloads = {{0x7e, 0x7e, 0x01}, {0x02, 0x03}};
        input.valuesJson = R"({"values":{"flow":{"value":1}}})";
        service::telemetry::contract::normalize(input);
        const auto report = input;
        require(service::common::isUuid(report.messageId), "report identity is not a UUID");
        input.messageId = service::utils::nextUuid(uuidState);
        input.connectionId = service::utils::nextUuid(uuidState);
        input.causationId = service::utils::nextUuid(uuidState);
        input.occurredAtMs += 5000;
        service::telemetry::contract::normalize(input);
        require(input.messageId == report.messageId, "reconnected retransmission changed identity");
        auto nextRound = report;
        nextRound.acquisitionId = service::utils::nextUuid(uuidState);
        service::telemetry::contract::normalize(nextRound);
        require(nextRound.messageId != report.messageId, "identical values collapsed independent scans");
        auto missingIdentity = report;
        missingIdentity.acquisitionId.clear();
        bool missingRejected = false;
        try { service::telemetry::contract::normalize(missingIdentity); }
        catch (const std::invalid_argument&) { missingRejected = true; }
        require(missingRejected, "telemetry without acquisition identity was accepted");
        std::cout<<"telemetry contract tests passed\n";
        return 0;
    } catch(const std::exception& error){std::cerr<<error.what()<<'\n';return 1;}
}
