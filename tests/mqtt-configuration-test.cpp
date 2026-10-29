#include <iostream>
#include <utility>

#include "service/modules/protocol/protocol.service.h"

namespace {
void require(bool value, const char* reason) {
    if (!value) {
        throw std::runtime_error(reason);
    }
}

constexpr std::string_view kPoint = "00000000-0000-7000-8000-000000000001";

service::protocol::MqttPreviewResult preview(std::string_view config, std::string_view topic, std::string_view payload, std::string_view code = "") {
    const auto raw = "{\"config\":" + std::string(config) + ",\"topic\":" + service::utils::jsonQuoted(topic) + ",\"payload\":" + service::utils::jsonQuoted(payload) + ",\"deviceCode\":" + service::utils::jsonQuoted(code) + ",\"timezone\":\"+08:00\"}";
    const auto body = ruvia::from_json<service::protocol::MqttPreviewBody>(raw);
    return service::protocol::ProtocolService::previewMqtt(body);
}

std::string configuration(std::string_view options, std::string_view pointOptions, std::string_view field = "temperature", std::string_view type = "DOUBLE") {
    return "{\"identityProperty\":\"serial\",\"parameters\":[{\"name\":\"serial\",\"dataType\":\"STRING\"},{\"name\":\"uuid\",\"dataType\":\"STRING\"},{\"name\":\"route\",\"dataType\":\"STRING\"}],\"storagePolicy\":\"report\",\"topic\":\"devices\",\"qos\":1," + std::string(options) + "\"points\":[{\"id\":\"" + std::string(kPoint) + "\",\"name\":\"温度\",\"field\":" + service::utils::jsonQuoted(field) + ",\"dataType\":" + service::utils::jsonQuoted(type) + std::string(pointOptions) + "}]}";
}

void testFormatsAndTime() {
    const auto json = configuration(R"("recordsPath":"/devices","identitySource":"payload","deviceCodeField":"/code","timeField":"/time","timeFormat":"iso8601",)", "");
    const auto result = preview(json, "devices", R"({"devices":[{"code":"D001","temperature":256,"time":"2026-01-01T08:00:00.123"},{"code":"D002","temperature":"bad"}]})");
    require(result.get<"records">().size() == 2, "nested records lost");
    const auto& record = result.get<"records">()[0];
    require(record.get<"deviceCode">().view() == "D001" && record.get<"time">().view() == "2026-01-01T00:00:00Z", "identity or timezone incorrect");
    require(std::abs(std::stod(std::string(record.get<"points">()[0].get<"value">().view())) - 256.0) < 0.00001, "native MQTT value must remain unscaled");
    require(result.get<"records">()[1].get<"errors">().size() == 1, "missing point diagnostic");
    const auto text = configuration(R"("payloadFormat":"text","identitySource":"payload","deviceCodeField":"0","delimiter":",","recordDelimiter":";",)", "", "1");
    const auto rows = preview(text, "devices", "D001,12.5;D002,18");
    require(rows.get<"records">().size() == 2 && rows.get<"records">()[1].get<"points">()[0].get<"value">().view() == "18", "text records or conversion failed");
    const auto binary = configuration(R"("payloadFormat":"binary","identitySource":"payload","deviceCodeField":"0:4:UTF8","recordLength":6,)", "", "4:2:UINT:LE");
    const auto bytes = preview(binary, "devices", "44 30 30 31 00 01 44 30 30 32 10 00");
    require(bytes.get<"records">().size() == 2 && bytes.get<"records">()[0].get<"points">()[0].get<"value">().view() == "256", "binary batch or byte order failed");
    const auto enums = configuration("", R"(,"enumValues":[{"input":"on","output":"true"},{"input":"off","output":"false"}])", "temperature", "BOOL");
    const auto enumResult = preview(enums, "devices", R"({"temperature":"on"})", "D001");
    require(enumResult.get<"records">()[0].get<"points">()[0].get<"value">().view() == "true", "enum conversion failed");
    auto topic = configuration(R"("identitySource":"topic","topicDeviceSegment":1,)", "");
    topic.replace(topic.find("\"topic\":\"devices\""), 17, "\"topic\":\"devices/+/telemetry\"");
    const auto topicResult = preview(topic, "devices/D002/telemetry", R"({"temperature":8})");
    require(topicResult.get<"records">()[0].get<"deviceCode">().view() == "D002", "topic identity failed");
}

void testDataTimeSelection() {
    const auto templ = service::utils::jsonQuoted(R"({"temperature":"$point:温度","a/b":{"~clock":["$time"]}})");
    const auto timed = configuration("\"timeSource\":\"payload\",\"timeFormat\":\"unix_s\",\"reportTemplate\":" + templ + ",", "");
    const auto result = preview(timed, "devices", R"({"temperature":8,"a/b":{"~clock":[1767225600]}})", "D001");
    require(result.get<"records">().front().get<"time">().view() == "2026-01-01T00:00:00Z", "$time path was not derived for preview");
    const auto bare = configuration("\"timeSource\":\"payload\",\"timeField\":\"clock\",\"reportTemplate\":" + service::utils::jsonQuoted(R"({"temperature":"$point:温度","clock":"$time"})") + ",", "");
    (void)preview(bare, "devices", R"({"temperature":8,"clock":1767225600000})", "D001");
    for (const auto& invalid : {
             configuration(R"("timeSource":"payload","timeField":"clock",)", ""),
             configuration(R"("timeSource":"received","timeField":"clock",)", ""),
             configuration("\"timeSource\":\"received\",\"reportTemplate\":" + templ + ",", ""),
             configuration("\"timeSource\":\"payload\",\"timeField\":\"wrong\",\"reportTemplate\":" + templ + ",", ""),
             configuration("\"timeSource\":\"payload\",\"reportTemplate\":" + service::utils::jsonQuoted(R"({"temperature":"$point:温度"})") + ",", ""),
             configuration(R"("timeSource":"invalid",)", "") }) {
        bool rejected = false;
        try {
            (void)preview(invalid, "devices", R"({"temperature":8})", "D001");
        } catch (const std::exception&) {
            rejected = true;
        }
        require(rejected, "inconsistent data time selection was accepted");
    }
}

void testRejectedConfiguration() {
    for (const auto config : {
             configuration(R"("identitySource":"payload",)", ""),
             configuration(R"("payloadFormat":"binary",)", "", "0:8:UINT"),
             configuration(R"("payloadFormat":"text",)", "", "-1"),
             configuration(R"("commandTopic":"commands",)", R"(,"writable":true,"scale":0)"),
             configuration("", R"(,"enumValues":[{"input":"a","output":"1"},{"input":"b","output":"1.0"}])") }) {
        bool rejected = false;
        try {
            (void)preview(config, "devices", "{}");
        } catch (const std::exception&) {
            rejected = true;
        }
        require(rejected, "invalid configuration accepted");
    }
    bool rejected = false;
    try {
        (void)preview(configuration("", ""), "different", "{}");
    } catch (const std::exception&) {
        rejected = true;
    }
    require(rejected, "unmatched sample topic accepted");
    auto wildcard = configuration(R"("identitySource":"topic","topicDeviceSegment":1,)", "");
    wildcard.replace(wildcard.find("\"topic\":\"devices\""), 17, "\"topic\":\"devices/+\"");
    rejected = false;
    try {
        (void)preview(wildcard, "devices/+", R"({"temperature":1})");
    } catch (const std::exception&) {
        rejected = true;
    }
    require(rejected, "wildcard sample topic accepted as a concrete publication topic");
}

void testTemplates() {
    const auto withTemplate = [](std::string_view value, bool writable = true) {
        return configuration("\"commandTopic\":\"commands\",\"commandTemplate\":" + service::utils::jsonQuoted(value) + ",", writable ? R"(,"writable":true)" : "");
    };
    const auto valid = withTemplate(R"({"device":"$param:serial","set":{"target":"$point:温度"},"again":"$point:温度","constant":false,"$point:温度":"key remains literal"})");
    const auto namedResult = preview(valid, "devices", R"({"temperature":8})", "D001");
    require(namedResult.get<"records">()[0].get<"points">()[0].get<"value">().view() == "8", "named template altered report value");
    const auto legacyResult = preview(withTemplate(R"({"values":{"temperature":"$point:温度"}})"), "devices", R"({"temperature":8})", "D001");
    require(legacyResult.get<"records">()[0].get<"points">()[0].get<"value">().view() == "8", "legacy values template altered report value");

    const std::string report = " {\"code\":\"$param:serial\",\"temperature\":\"$point:温度\",\"at\":0,\"constant\":42} ";
    const auto metadata = configuration("\"reportTemplate\":" + service::utils::jsonQuoted(report) + ",", "");
    const auto typed = ruvia::from_json<service::protocol::MqttConfig>(metadata);
    require(typed.get<"reportTemplate">() && typed.get<"reportTemplate">()->view() == report, "report template metadata was dropped or normalized");
    const auto serializedText = ruvia::to_json(typed);
    const auto serialized = ruvia::json_value::parse(serializedText);
    const auto storedReport = serialized.get<ruvia::string>("reportTemplate");
    require(storedReport && storedReport->view() == report, "preview serialization lost report template");
    const auto metadataResult = preview(metadata, "devices", R"({"temperature":8})", "D001");
    require(metadataResult.get<"records">()[0].get<"points">()[0].get<"value">().view() == "8", "report metadata altered canonical preview");

    std::string deep = "\"$point:温度\"";
    for (int i = 0; i < 33; ++i) {
        deep = "{\"nested\":" + deep + "}";
    }
    std::vector<std::string> invalid{
        withTemplate(R"({"target":"$point:不存在"})"),
        withTemplate(R"({"target":"$unknown"})"),
        withTemplate(R"({"target":"$time"})"),
        withTemplate(R"({"target":"$point:"})"),
        withTemplate(R"({"target":"$point:温度"})", false),
        withTemplate(R"({"target":1,"target":"$point:温度"})"),
        withTemplate(R"(["$point:温度"])"),
        withTemplate(deep),
        withTemplate("{\"constant\":" + service::utils::jsonQuoted(std::string(16384, 'x')) + "}"),
        configuration("\"reportTemplate\":" + service::utils::jsonQuoted(R"({"a":"$point:温度","b":"$point:温度"})") + ",", ""),
        configuration("\"reportTemplate\":" + service::utils::jsonQuoted(R"({"a":"$values"})") + ",", ""),
        configuration("\"reportTemplate\":" + service::utils::jsonQuoted(R"({"a":"$point:不存在"})") + ",", ""),
        configuration("\"reportTemplate\":" + service::utils::jsonQuoted(R"({"a":"$point:温度","b":"$param:serial","c":"$param:serial"})") + ",", ""),
        configuration("\"reportTemplate\":" + service::utils::jsonQuoted(R"({"a":"$point:温度","b":"$time","c":"$time"})") + ",", ""),
        configuration("\"reportTemplate\":" + service::utils::jsonQuoted(deep) + ",", ""),
        configuration("\"reportTemplate\":" + service::utils::jsonQuoted("[]") + ",", "")
    };
    auto duplicateName = configuration("", "");
    duplicateName.replace(duplicateName.size() - 2, 1, ",{\"id\":\"00000000-0000-7000-8000-000000000002\",\"name\":\"温度\",\"field\":\"other\",\"dataType\":\"DOUBLE\"}]");
    invalid.push_back(std::move(duplicateName));
    for (const auto& config : invalid) {
        bool rejected = false;
        try {
            (void)preview(config, "devices", R"({"temperature":8})", "D001");
        } catch (const std::exception&) {
            rejected = true;
        }
        require(rejected, "invalid template accepted by module");
    }
}

void testMultipleControlMessages() {
    const auto command = [](std::string_view id, std::string_view name, std::string_view topic, std::string_view payload, std::string_view required) {
        return "{\"id\":" + service::utils::jsonQuoted(id) + ",\"name\":" + service::utils::jsonQuoted(name) +
            ",\"topic\":" + service::utils::jsonQuoted(topic) + ",\"template\":" + service::utils::jsonQuoted(payload) +
            ",\"requiredPointIds\":" + std::string(required) + "}";
    };
    constexpr std::string_view firstId = "00000000-0000-7000-8000-000000000010";
    constexpr std::string_view secondId = "00000000-0000-7000-8000-000000000011";
    const auto required = "[" + service::utils::jsonQuoted(kPoint) + "]";
    const auto first = command(firstId, "设置温度", "commands/{param:serial}", R"({"set":"$point:温度"})", required);
    const auto second = command(secondId, "温度校准", "calibrate/{param:serial}", R"({"calibrate":"$point:温度"})", "[]");
    const auto valid = configuration("\"commands\":[" + first + "," + second + "],", R"(,"writable":true)");
    const auto parsed = ruvia::from_json<service::protocol::MqttConfig>(valid);
    require(parsed.get<"commands">() && parsed.get<"commands">()->size() == 2, "multiple command configuration lost");
    const auto encoded = ruvia::to_json(parsed);
    require(encoded.find("requiredPointIds") != std::string::npos && encoded.find("templateText") == std::string::npos, "command wire field names changed");
    const auto previewed = preview(valid, "devices", R"({"temperature":8})", "D001");
    require(previewed.get<"records">()[0].get<"points">().size() == 1, "multiple commands changed reports");
    const auto onlyControl = configuration("\"reportTemplate\":\"{}\",\"commands\":[" + first + "],", R"(,"writable":true)", "");
    const auto report = preview(onlyControl, "devices", "{}", "D001");
    require(report.get<"records">()[0].get<"points">().empty() && report.get<"records">()[0].get<"errors">().empty(), "control-only attribute was parsed as a report field");
    const auto topic = command(firstId, "Topic", "commands/{point:温度}", "{}", required);
    (void)preview(configuration("\"commands\":[" + topic + "],", R"(,"writable":true)"), "devices", "{}", "D001");
    const std::vector<std::string> invalidLists{
        first + "," + first,
        first + "," + command(secondId, "设置温度", "commands", R"({"set":"$point:温度"})", "[]"),
        command(firstId, "设置", "commands", R"({"set":"$point:温度"})", "[\"missing\"]"),
        command(firstId, "设置", "commands", R"({"set":"$point:温度"})", "[" + service::utils::jsonQuoted(kPoint) + "," + service::utils::jsonQuoted(kPoint) + "]"),
        command(firstId, "设置", "commands/+", R"({"set":"$point:温度"})", required),
        command(firstId, "设置", "commands", R"({"set":"$point:不存在"})", "[]"),
        command(firstId, "设置", "commands", R"({"set":["$point:温度"]})", "[]"),
        command(firstId, "设置", "commands/{point:温度}", "{}", "[]"),
        command(firstId, "设置", "commands/prefix{param:serial}", R"({"set":"$point:温度"})", required),
        command(firstId, "设置", "commands", R"({"set":"$values"})", required),
    };
    for (const auto& list : invalidLists) {
        bool rejected = false;
        try {
            (void)preview(configuration("\"commands\":[" + list + "],", R"(,"writable":true)"), "devices", "{}", "D001");
        } catch (const std::exception&) {
            rejected = true;
        }
        require(rejected, "invalid control message accepted");
    }
}

void testTemplateValueSemantics() {
    using Template = service::utils::JsonValueTemplate;
    const auto compile = [] {
        const std::string input = R"({"z":{"items":[{"target":"$point:温度","optional":"$optional"},[true,null,{}]]},"a":[],"escaped\"key":"$param:serial"})";
        return Template(input, { "$point:温度", "$optional", "$param:serial" }, { "$optional" });
    };
    const Template::Values values{ { "$point:温度", "12.5" }, { "$param:serial", R"("D001")" } };
    constexpr std::string_view expected = R"({"z":{"items":[{"target":12.5},[true,null,{}]]},"a":[],"escaped\"key":"D001"})";
    const auto verify = [&](const Template& value) {
        require(value.render(values) == expected, "template ownership, member order, or nested rendering changed");
        require(value.references() == Template::Tokens({ "$point:温度", "$optional", "$param:serial" }), "template references lost after copy or move");
    };

    auto original = compile();
    verify(original);
    auto copied = original;
    verify(copied);
    auto moved = std::move(original);
    verify(moved);
    original = copied;
    verify(original);
    original = std::move(moved);
    verify(original);
    verify(copied);
}

void testCommandResponseConfiguration() {
    const auto make = [](std::string_view fields, std::string_view topic = "devices/{param:serial}/result", int timeout = 10000) {
        const auto command = "{\"id\":\"00000000-0000-7000-8000-000000000010\",\"name\":\"设置\",\"topic\":\"devices/{param:serial}/down\",\"template\":" + service::utils::jsonQuoted(R"({"id":"$point:温度","route":"$param:route","uuid":"$param:uuid","value":"$point:温度"})") +
            ",\"requiredPointIds\":[" + service::utils::jsonQuoted(kPoint) + "],\"response\":{\"topic\":" + service::utils::jsonQuoted(topic) + ",\"matchFields\":" + std::string(fields) + ",\"codeField\":\"/code\",\"successCode\":0,\"timeoutMs\":" + std::to_string(timeout) + "}}";
        return configuration("\"commands\":[" + command + "],", R"(,"writable":true)");
    };
    const auto valid = make(R"(["/id","/route"])");
    (void)preview(valid, "devices", R"({"temperature":8})", "D001");
    const auto parsed = ruvia::from_json<service::protocol::MqttConfig>(valid);
    require(parsed.get<"commands">()->front().get<"response">().has_value(), "generic result configuration lost");
    for (const auto& invalid : { make(R"(["/route"])"), make(R"(["/id","/unknown"])"), make(R"(["/id","/id"])"), make(R"(["/id"])", "devices/+/result"), make(R"(["/id"])", "devices/{param:serial}/result", 0) }) {
        bool rejected = false;
        try {
            (void)preview(invalid, "devices", "{}", "D001");
        } catch (const std::exception&) {
            rejected = true;
        }
        require(rejected, "invalid result correlation accepted");
    }
}

void testMultipleReports() {
    const auto report = [](std::string_view id, std::string_view name, std::string_view topic, std::string_view field) {
        return "{\"id\":" + service::utils::jsonQuoted(id) + ",\"name\":" + service::utils::jsonQuoted(name) + ",\"topic\":" + service::utils::jsonQuoted(topic) +
            ",\"qos\":1,\"reportTemplate\":\"{\\\"value\\\":\\\"$point:温度\\\"}\",\"pointFields\":[{\"pointId\":" + service::utils::jsonQuoted(kPoint) + ",\"field\":" + service::utils::jsonQuoted(field) + "}]}";
    };
    const auto first = report("00000000-0000-4000-8000-000000000002", "温度", "devices/{param:serial}/temperature", "/value");
    const auto second = report("00000000-0000-4000-8000-000000000003", "另一映射", "devices/{param:serial}/alternate", "/value");
    const auto config = configuration("\"timeSource\":\"payload\",\"timeField\":\"ignored-root-clock\",\"reports\":[" + first + ',' + second + "],", "");
    const auto value = preview(config, "devices/D001/alternate", R"({"value":42})", "D001");
    require(value.get<"records">().size() == 1 && value.get<"records">()[0].get<"points">()[0].get<"value">().view() == "42", "preview did not select second report");
    for (const auto& bad : { configuration("\"reports\":[],", ""), configuration("\"reports\":[" + first + ',' + first + "],", ""), configuration("\"reports\":[" + first + ',' + report("00000000-0000-4000-8000-000000000003", "重叠", "devices/+/temperature", "/value") + "],", "") }) {
        bool rejected = false;
        try {
            (void)preview(bad, "devices/D001/temperature", "{}", "D001");
        } catch (const std::exception&) {
            rejected = true;
        }
        require(rejected, "invalid report list or overlapping topics accepted");
    }
}
} // namespace

int main() {
    try {
        testMultipleReports();
        testFormatsAndTime();
        testDataTimeSelection();
        testRejectedConfiguration();
        testTemplates();
        testMultipleControlMessages();
        testTemplateValueSemantics();
        testCommandResponseConfiguration();
        std::cout << "MQTT configuration tests passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
