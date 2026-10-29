#include <iostream>

#include "service/features/collector/mqtt/mqtt.protocol.h"

using namespace service::collector;

namespace {
void require(bool value, const char* message) {
    if (!value) {
        throw std::runtime_error(message);
    }
}

std::unique_ptr<ProtocolSession> userSession(const std::shared_ptr<RuntimeSnapshot>& snapshot) {
    for (auto& device : snapshot->devices) {
        const auto config = ruvia::json_value::parse(std::string_view(device.calculationConfig));
        if (!config.get<ruvia::string>("identityProperty")) {
            device.calculationConfig.insert(1, "\"identityProperty\":\"serial\",");
        }
        if (device.calculationConfig.find("$point:请求编号") != std::string::npos && std::none_of(device.elements.begin(), device.elements.end(), [](const auto& point) {
                return point.id == "request";
            })) {
            device.elements.push_back({ .id = "request", .name = "请求编号", .dataType = "DOUBLE", .writable = true });
            const auto offset = device.calculationConfig.find("\"points\":[") + 10;
            device.calculationConfig.insert(offset, R"({"id":"request","name":"请求编号","field":"","dataType":"DOUBLE","writable":true},)");
        }
        const auto properties = ruvia::json_value::parse(std::string_view(device.parameters));
        std::string updated = "{";
        (void)properties.for_each_field([&](std::string_view name, const ruvia::json_value& value) {
            if (name != "serial" && name != "uuid") {
                updated += service::utils::jsonQuoted(name) + ':' + std::string(value.view()) + ',';
            }
            return true;
        });
        updated += "\"serial\":" + service::utils::jsonQuoted(device.code) + ",\"uuid\":" + service::utils::jsonQuoted(device.id) + '}';
        device.parameters = std::move(updated);
    }
    return mqtt::Factory{}.createSession(snapshot->links.front(), "connection", "broker", snapshot);
}

struct Fixture {
    std::shared_ptr<RuntimeSnapshot> snapshot = std::make_shared<RuntimeSnapshot>();
    std::unique_ptr<ProtocolSession> session;
    unsigned packetSequence = 0;
    unsigned requestSequence = 1000;

    explicit Fixture(bool shared = false) {
        LinkDefinition link{ .id = "link", .mode = "TCP Client", .protocol = "MQTT", .status = "enabled" };
        link.targets.push_back({ .id = "broker", .ip = "127.0.0.1", .port = 1883, .status = "enabled", .mqttConfig = R"({"clientId":"platform-test","keepAliveSeconds":60})" });
        snapshot->links.push_back(link);
        for (int i = 0; i < (shared ? 2 : 1); ++i) {
            DeviceDefinition device{ .calculationConfig = R"({"topic":"devices", "deviceCodeField":"deviceCode", "commandTopic":"commands","qos":2,"points":[{"id":"temperature","name":"温度","field":"temperature","dataType":"DOUBLE"}]})",
                                     .id = std::to_string(i),
                                     .code = std::to_string(i),
                                     .linkId = "link",
                                     .targetId = "broker",
                                     .protocol = "MQTT" };
            if (!shared) {
                device.calculationConfig = R"({"topic":"devices/0","commandTopic":"commands/0","qos":2,"points":[{"id":"temperature","name":"温度","field":"temperature","dataType":"DOUBLE"}]})";
            }
            device.elements.push_back({ .id = "temperature", .name = "温度", .dataType = "DOUBLE", .mqttField = "temperature", .writable = true });
            snapshot->devices.push_back(std::move(device));
        }
        session = userSession(snapshot);
    }

    std::vector<ProtocolAction> receive(const mqtt::Bytes& bytes) {
        const auto id = "packet-" + std::to_string(++packetSequence);
        return session->consume({ .messageId = id, .connectionId = "connection", .receivedAtMs = 1000, .bytes = bytes });
    }

    std::vector<ProtocolAction> execute(ProtocolCommand command) {
        if (snapshot->devices.front().calculationConfig.find("$point:请求编号") != std::string::npos && std::none_of(command.elements.begin(), command.elements.end(), [](const auto& point) {
                return point.elementId == "request";
            })) {
            command.elements.push_back({ "request", std::to_string(++requestSequence) });
        }
        return dynamic_cast<CommandCapabilitySession*>(session.get())->execute(std::move(command));
    }

    void start() {
        const auto connect = session->connected();
        require(connect.front().bytes == mqtt::Bytes({ 0x10, 25, 0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 60, 0, 13, 'p', 'l', 'a', 't', 'f', 'o', 'r', 'm', '-', 't', 'e', 's', 't' }), "CONNECT golden frame mismatch");
        const auto subscribe = receive({ 0x20, 2, 0, 0 });
        require(subscribe.front().bytes.front() == 0x82, "missing SUBSCRIBE");
        const auto suback = receive({ 0x90, 3, 0, 1, 2 });
        require(!suback.empty() && suback.front().kind == ProtocolActionKind::CancelDeadline, "SUBACK rejected");
    }
};

mqtt::Bytes publication(std::string_view topic, std::string_view payload, unsigned qos, std::uint16_t id = 7, bool duplicate = false) {
    mqtt::Bytes body;
    mqtt::text(body, topic);
    if (qos) {
        mqtt::word(body, id);
    }
    body.insert(body.end(), payload.begin(), payload.end());
    return mqtt::packet(0x30 | (qos << 1) | (duplicate ? 8 : 0), body);
}

void testFramingAndRouting() {
    Fixture fixture(true);
    fixture.start();
    const auto packet = publication("devices", R"({"deviceCode":"1","temperature":23.5})", 1);
    std::vector<ProtocolAction> actions;
    for (const auto byte : packet) {
        const auto next = fixture.receive({ byte });
        actions.insert(actions.end(), next.begin(), next.end());
    }
    require(actions.size() == 2 && actions.front().kind == ProtocolActionKind::PublishParsed && actions.front().deviceId == "1", "shared topic routing failed");
    require(actions.front().parsed.valuesJson.find("23.5") != std::string::npos, "mapped value lost");
    require(fixture.receive(publication("devices", R"({"deviceCode":"1","temperature":23.5})", 1, 7, true)).empty(), "pending QoS1 duplicate was published");
    const auto ack = fixture.session->parsedPublished(actions.front().publicationToken);
    require(ack.size() == 1 && ack.front().bytes == mqtt::Bytes({ 0x40, 2, 0, 7 }), "PUBACK before/after publication incorrect");
    require(fixture.receive(publication("devices", R"({"deviceCode":"unknown","temperature":25})", 1, 8)).front().bytes == mqtt::Bytes({ 0x40, 2, 0, 8 }), "unknown device not acknowledged");
    auto coalesced = publication("devices", R"({"deviceCode":"0","temperature":10})", 0);
    const auto second = publication("devices", R"({"deviceCode":"1","temperature":11})", 0);
    coalesced.insert(coalesced.end(), second.begin(), second.end());
    require(fixture.receive(coalesced).size() == 4, "coalesced publications lost");
}

void testQos2AndCommand() {
    Fixture fixture;
    fixture.start();
    const auto packet = publication("devices/0", R"({"temperature":8})", 2);
    require(fixture.receive(packet).front().bytes == mqtt::Bytes({ 0x50, 2, 0, 7 }), "QoS2 missing PUBREC");
    const auto released = fixture.receive({ 0x62, 2, 0, 7 });
    require(released.front().kind == ProtocolActionKind::PublishParsed, "QoS2 publication missing");
    require(fixture.receive({ 0x62, 2, 0, 7 }).empty(), "QoS2 duplicate release republished");
    require(fixture.session->parsedPublished(released.front().publicationToken).front().bytes == mqtt::Bytes({ 0x70, 2, 0, 7 }), "QoS2 missing PUBCOMP");
    require(fixture.receive({ 0x62, 2, 0, 7 }).front().bytes == mqtt::Bytes({ 0x70, 2, 0, 7 }), "completed QoS2 release not acknowledged");
    const auto command = dynamic_cast<CommandCapabilitySession*>(fixture.session.get())->execute({ .id = "command", .deviceId = "0", .elements = { { "temperature", "12.5" } } });
    require(command.front().bytes.front() == 0x34, "outbound QoS2 missing PUBLISH");
    require(fixture.receive({ 0x50, 2, 0, 2 }).front().bytes == mqtt::Bytes({ 0x62, 2, 0, 2 }), "outbound QoS2 missing PUBREL");
    const auto complete = fixture.receive({ 0x70, 2, 0, 2 });
    require(complete.back().kind == ProtocolActionKind::CompleteCommand, "outbound QoS2 did not complete");
}

void testFailuresAndKeepAlive() {
    Fixture fixture;
    fixture.start();
    const auto ping = dynamic_cast<DeadlineCapabilitySession*>(fixture.session.get())->deadline(2);
    require(ping.front().bytes == mqtt::Bytes({ 0xc0, 0 }), "missing keep alive PINGREQ");
    require(fixture.receive({ 0xd0, 0 }).size() == 2, "PINGRESP did not rearm keep alive");
    require(dynamic_cast<DeadlineCapabilitySession*>(fixture.session.get())->deadline(3).back().kind == ProtocolActionKind::Close, "ping timeout did not close");
    for (const mqtt::Bytes malformed : { mqtt::Bytes{ 0x20, 2, 0, 5 }, mqtt::Bytes{ 0x21, 2, 0, 0 }, mqtt::Bytes{ 0x20, 0xff, 0xff, 0xff, 0xff } }) {
        Fixture rejected;
        (void)rejected.session->connected();
        require(rejected.receive(malformed).back().kind == ProtocolActionKind::Close, "malformed MQTT accepted");
    }
    require(!mqtt::validText(std::string("a\0b", 3)) && !mqtt::validText("\xed\xa0\x80"), "invalid UTF-8 accepted");
    require(mqtt::Factory{}.packetForLogging({ mqtt::Bytes{ 0x10, 3, 'x', 'y', 'z' } }) == mqtt::Bytes({ 0x10, 0 }), "CONNECT credentials leaked");
}

void testBatchPathsAndRetries() {
    Fixture fixture(true);
    for (auto& device : fixture.snapshot->devices) {
        device.calculationConfig = R"({"topic":"devices","recordsPath":"/payload/devices","deviceCodeField":"/meta/code","commandTopic":"commands","qos":2,"points":[{"id":"temperature","name":"温度","field":"/metrics/temperature","dataType":"DOUBLE"}]})";
        device.elements.front().mqttField = "/metrics/temperature";
    }
    fixture.session = userSession(fixture.snapshot);
    fixture.start();
    const auto bytes = publication("devices", R"({"payload":{"devices":[{"meta":{"code":"0"},"metrics":{"temperature":20}},{"meta":{"code":"1"},"metrics":{"temperature":21}}]}})", 1);
    const ProtocolInput input{ .messageId = "durable-input", .receivedAtMs = 1000, .bytes = bytes };
    const auto actions = fixture.session->consume(input);
    require(actions.size() == 4 && actions[0].deviceId == "0" && actions[2].deviceId == "1", "nested batch route failed");
    require(actions[0].parsed.acquisitionId != actions[2].parsed.acquisitionId, "batch records share a history identity");
    require(fixture.session->parsedPublished(actions[0].publicationToken).empty(), "batch acknowledged before all devices were published");
    const auto retry = fixture.session->consume(input);
    require(retry[0].parsed.acquisitionId == actions[0].parsed.acquisitionId && retry[2].publicationToken == actions[2].publicationToken, "Redis retry advanced MQTT state");
    const auto ack = fixture.session->parsedPublished(actions[2].publicationToken);
    require(ack.front().bytes == mqtt::Bytes({ 0x40, 2, 0, 7 }), "batch did not acknowledge after publication");
    require(fixture.session->parsedPublished(actions[2].publicationToken).front().bytes == ack.front().bytes, "ack publication retry lost acknowledgement");
    const auto request = dynamic_cast<CommandCapabilitySession*>(fixture.session.get())->execute({ .id = "nested-command", .deviceId = "1", .elements = { { "temperature", "12" } } });
    const std::string encoded(request.front().bytes.begin(), request.front().bytes.end());
    require(encoded.find(R"({"meta":{"code":"1"},"metrics":{"temperature":12}})") != std::string::npos, "command did not encode nested paths");
    Fixture array(true);
    array.start();
    require(array.receive(publication("devices", R"([{"deviceCode":"0","temperature":1},{"deviceCode":"1","temperature":2}])", 0)).size() == 4, "top-level array failed");
    const std::string json = R"({"a/b":{"~name":[false,42]}})";
    const auto object = ruvia::json_value::parse(json);
    const auto selected = service::utils::jsonField(object, "/a~1b/~0name/1");
    require(selected && selected->view() == "42", "JSON Pointer escape or index failed");
}

void testTextAndBinaryCommands() {
    for (const bool binary : { false, true }) {
        Fixture fixture;
        auto& device = fixture.snapshot->devices.front();
        device.calculationConfig = binary
            ? R"({"topic":"devices/0","identitySource":"payload","deviceCodeField":"0:4:UTF8","payloadFormat":"binary","recordLength":6,"commandTopic":"commands/0","qos":2,"points":[{"id":"temperature","name":"温度","field":"4:2:UINT:LE","dataType":"DOUBLE"}]})"
            : R"({"topic":"devices/0","identitySource":"payload","deviceCodeField":"0","payloadFormat":"text","delimiter":",","recordDelimiter":";","commandTopic":"commands/0","qos":2,"points":[{"id":"temperature","name":"温度","field":"1","dataType":"DOUBLE"}]})";
        device.elements.front().mqttField = binary ? "4:2:UINT:LE" : "1";
        fixture.session = userSession(fixture.snapshot);
        fixture.start();
        const auto payload = binary ? std::string("0\0\0\0\0\1", 6) : "0,256;unknown,100";
        const auto actions = fixture.receive(publication("devices/0", payload, 0));
        require(actions.size() == 2 && actions.front().kind == ProtocolActionKind::PublishParsed && actions.front().parsed.valuesJson.find("256") != std::string::npos, "configured text/binary report failed");
        const auto command = dynamic_cast<CommandCapabilitySession*>(fixture.session.get())->execute({ .id = "encoded-command", .deviceId = "0", .elements = { { "temperature", "15" } } });
        require(command.front().kind == ProtocolActionKind::Send, "configured text/binary command rejected");
        const auto expected = binary ? std::string("0\0\0\0\x0f\0", 6) : "0,15";
        require(command.front().bytes.size() >= expected.size() && std::equal(expected.begin(), expected.end(), command.front().bytes.end() - expected.size(), [](char left, std::uint8_t right) {
                    return static_cast<std::uint8_t>(left) == right;
                }),
                "text/binary command must preserve raw values");
    }
}

std::string publishedPayload(const std::vector<ProtocolAction>& actions) {
    if (actions.empty() || actions.front().kind != ProtocolActionKind::Send) {
        throw std::runtime_error("command did not publish: " + (actions.empty() ? std::string("no actions") : actions.front().reason));
    }
    const auto& bytes = actions.front().bytes;
    require((bytes.front() >> 4) == 3, "command frame is not PUBLISH");
    std::size_t offset = 1;
    while (bytes.at(offset++) & 128) {
    }
    const auto topicLength = (bytes.at(offset) << 8) | bytes.at(offset + 1);
    offset += 2 + topicLength;
    if ((bytes.front() >> 1) & 3) {
        offset += 2;
    }
    return std::string(bytes.begin() + static_cast<std::ptrdiff_t>(offset), bytes.end());
}

void configureNamed(Fixture& fixture, std::string_view commandTemplate, std::string_view temperatureOptions = "") {
    auto& device = fixture.snapshot->devices.front();
    device.code = "device\"\\code";
    device.calculationConfig = R"({"topic":"devices/0","commandTopic":"commands/0","qos":2,"identitySource":"payload","deviceCodeField":"/report/code","commandTemplate":)" +
        service::utils::jsonQuoted(commandTemplate) +
        R"(,"points":[{"id":"temperature","name":"温度","field":"/report/temperature","dataType":"DOUBLE","writable":true)" + std::string(temperatureOptions) +
        R"(},{"id":"enabled","name":"开关","field":"/report/enabled","dataType":"BOOL","writable":true},{"id":"label","name":"名称","field":"/report/label","dataType":"STRING","writable":true},{"id":"mode","name":"模式","field":"/report/mode","dataType":"BOOL","writable":true,"enumValues":[{"input":"ON","output":"true"},{"input":"OFF","output":"false"}]},{"id":"other","name":"其他","field":"/report/other","dataType":"DOUBLE","writable":true}]})";
    device.elements = {
        { .id = "temperature", .name = "温度", .dataType = "DOUBLE", .mqttField = "/report/temperature", .writable = true },
        { .id = "enabled", .name = "开关", .dataType = "BOOL", .mqttField = "/report/enabled", .writable = true },
        { .id = "label", .name = "名称", .dataType = "STRING", .mqttField = "/report/label", .size = 256, .writable = true },
        { .id = "mode", .name = "模式", .dataType = "BOOL", .mqttField = "/report/mode", .writable = true },
        { .id = "other", .name = "其他", .dataType = "DOUBLE", .mqttField = "/report/other", .writable = true }
    };
    fixture.session = userSession(fixture.snapshot);
}

void requireCommandFailure(const std::vector<ProtocolAction>& actions) {
    require(actions.size() == 1 && actions.front().kind == ProtocolActionKind::FailCommand && actions.front().bytes.empty(), "failed command sent an MQTT frame");
}

void testNamedTemplates() {
    constexpr std::string_view templateJson = R"({"device":"$param:serial","set":{"temperature":"$point:温度","enabled":"$point:开关","label":"$point:名称","mode":"$point:模式"},"again":"$point:温度","constant":[7,false,null,"prefix $point:温度"],"$param:serial":"literal"})";
    Fixture fixture;
    configureNamed(fixture, templateJson);
    fixture.start();
    const auto report = fixture.receive(publication("devices/0", R"({"report":{"code":"device\"\\code","temperature":7,"enabled":true,"label":"x","mode":"ON","other":1}})", 0));
    require(report.front().kind == ProtocolActionKind::PublishParsed && report.front().parsed.valuesJson.find("7") != std::string::npos, "named command changed canonical report mapping");
    const std::string label = "quote\" slash\\ line\nnext";
    const auto command = fixture.execute({ .id = "named", .deviceId = "0", .elements = { { "temperature", "15" }, { "enabled", "1" }, { "label", label }, { "mode", "1" } } });
    const auto payload = publishedPayload(command);
    const auto expected = R"({"device":"device\"\\code","set":{"temperature":15,"enabled":true,"label":)" + service::utils::jsonQuoted(label) + R"(,"mode":"ON"},"again":15,"constant":[7,false,null,"prefix $point:温度"],"$param:serial":"literal"})";
    require(payload == expected, "named template typed raw values, escaping or constants incorrect");
    require(payload.find("\"report\"") == std::string::npos && payload.find("\"other\"") == std::string::npos, "report paths leaked into command");
    const auto partial = publishedPayload(fixture.execute({ .id = "partial", .deviceId = "0", .elements = { { "enabled", "0" } } }));
    require(partial == R"({"device":"device\"\\code","set":{"enabled":false},"constant":[7,false,null,"prefix $point:温度"],"$param:serial":"literal"})", "partial command filled absent fields or unresolved placeholders");
    requireCommandFailure(fixture.execute({ .id = "unrepresented", .deviceId = "0", .elements = { { "other", "1" } } }));
    requireCommandFailure(fixture.execute({ .id = "invalid-value", .deviceId = "0", .elements = { { "enabled", "invalid" } } }));
    fixture.snapshot->devices.front().elements[1].writable = false;
    requireCommandFailure(fixture.execute({ .id = "permission", .deviceId = "0", .elements = { { "enabled", "1" } } }));

    Fixture array;
    configureNamed(array, R"({"set":["$point:温度","$point:开关"]})");
    array.start();
    requireCommandFailure(array.execute({ .id = "missing-slot", .deviceId = "0", .elements = { { "temperature", "15" } } }));
    require(publishedPayload(array.execute({ .id = "all-slots", .deviceId = "0", .elements = { { "temperature", "15" }, { "enabled", "0" } } })) == R"({"set":[15,false]})", "fixed array template shifted slots");

    Fixture legacy;
    configureNamed(legacy, R"({"device":"$param:serial","body":{"report":{"temperature":"$point:温度"}}})");
    legacy.start();
    const auto legacyPayloadText = publishedPayload(legacy.execute({ .id = "legacy", .deviceId = "0", .elements = { { "temperature", "15" } } }));
    const auto legacyPayload = ruvia::json_value::parse(legacyPayloadText);
    const auto legacyTemperature = service::utils::jsonField(legacyPayload, "/body/report/temperature");
    require(legacyTemperature && legacyTemperature->view() == "15", "legacy $values API behavior regressed");

    for (const auto invalid : { R"({"x":"$unknown"})", R"({"x":"$time"})", R"({"x":"$point:不存在"})", R"({"x":1,"x":"$point:温度"})", R"(["$point:温度"])" }) {
        bool rejected = false;
        try {
            Fixture bad;
            configureNamed(bad, invalid);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        require(rejected, "invalid named template accepted by runtime");
    }
}

void testMultipleMessagesAndRequiredPoints() {
    Fixture fixture;
    auto& device = fixture.snapshot->devices.front();
    device.calculationConfig = R"({"topic":"devices/0","qos":2,"reportTemplate":"{\"temperature\":\"$point:温度\"}","commands":[{"id":"set","name":"设置","topic":"devices/{param:serial}/settings/{point:通道}","template":"{\"action\":\"set\",\"values\":{\"temperature\":\"$point:温度\",\"enabled\":\"$point:开关\"}}","requiredPointIds":["temperature","channel"]},{"id":"switch","name":"切换","topic":"devices/{param:serial}/switch","template":"{\"switch\":\"$point:开关\"}","requiredPointIds":["enabled"]}],"points":[{"id":"temperature","name":"温度","field":"temperature","dataType":"DOUBLE","writable":true},{"id":"enabled","name":"开关","field":"","dataType":"BOOL","writable":true},{"id":"channel","name":"通道","field":"","dataType":"STRING","writable":true}]})";
    device.elements = {
        { .id = "temperature", .name = "温度", .dataType = "DOUBLE", .mqttField = "temperature", .writable = true },
        { .id = "enabled", .name = "开关", .dataType = "BOOL", .writable = true },
        { .id = "channel", .name = "通道", .dataType = "STRING", .size = 256, .writable = true },
    };
    fixture.session = userSession(fixture.snapshot);
    fixture.start();
    const auto selected = fixture.execute({ .id = "set-1", .deviceId = "0", .elements = { { "temperature", "15" }, { "channel", "zone-a" } }, .mqttMessageId = "set" });
    require(publishedPayload(selected) == R"({"action":"set","values":{"temperature":15}})", "optional property was filled or required value not converted");
    const std::string wire(selected.front().bytes.begin(), selected.front().bytes.end());
    require(wire.find("devices/0/settings/zone-a") != std::string::npos, "selected command Topic was not rendered from properties");
    require(publishedPayload(fixture.execute({ .id = "switch-1", .deviceId = "0", .elements = { { "enabled", "0" } }, .mqttMessageId = "switch" })) == R"({"switch":false})", "other message payload was used or false value omitted");
    require(publishedPayload(fixture.execute({ .id = "set-2", .deviceId = "0", .elements = { { "temperature", "0" }, { "enabled", "0" }, { "channel", "zone-b" } }, .mqttMessageId = "set" })) == R"({"action":"set","values":{"temperature":0,"enabled":false}})", "zero or false optional control value was lost");
    for (const auto& command : std::vector<ProtocolCommand>{
             { .id = "missing-choice", .deviceId = "0", .elements = { { "temperature", "15" }, { "channel", "zone-a" } } },
             { .id = "unknown-choice", .deviceId = "0", .elements = { { "enabled", "1" } }, .mqttMessageId = "unknown" },
             { .id = "missing-required", .deviceId = "0", .elements = { { "enabled", "1" }, { "channel", "zone-a" } }, .mqttMessageId = "set" },
             { .id = "missing-topic", .deviceId = "0", .elements = { { "temperature", "15" } }, .mqttMessageId = "set" },
             { .id = "wrong-membership", .deviceId = "0", .elements = { { "temperature", "15" }, { "enabled", "1" } }, .mqttMessageId = "switch" },
             { .id = "blank-required", .deviceId = "0", .elements = { { "temperature", " " }, { "channel", "zone-a" } }, .mqttMessageId = "set" },
             { .id = "topic-injection", .deviceId = "0", .elements = { { "temperature", "15" }, { "channel", "zone/#" } }, .mqttMessageId = "set" },
         }) {
        requireCommandFailure(fixture.execute(command));
    }
    // 准备阶段也必须拒绝缺少必填值，不能仅依靠实际发送阶段。
    bool rejected = false;
    try {
        (void)command::resolve(device, std::vector<CommandElementValue>{ { "enabled", "1" } }, "set");
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    require(rejected, "command preparation did not enforce required points");
    service::message::ProtocolTask task;
    task.messageId = "command";
    task.groupKey = "device:0";
    task.protocol = "MQTT";
    task.transport = "RAW";
    task.kind = "command";
    task.linkId = "link";
    task.deviceId = "0";
    task.createdAtMs = 1;
    task.elements = { { "enabled", "0" } };
    task.mqttMessageId = "switch";
    service::message::StreamMessage envelope;
    envelope.fields = service::message::protocolTaskFields(task);
    const auto restored = service::message::protocolTaskFrom(envelope);
    require(restored.mqttMessageId == "switch" && restored.elements == task.elements, "Redis command contract lost message selection");
}
} // namespace

void testConfiguredCommandResponses() {
    Fixture fixture(true);
    const std::string config = R"({"topic":"devices/{param:serial}/up","qos":1,"reportTemplate":"{\"value\":\"$point:温度\"}","points":[{"id":"temperature","name":"温度","field":"value","dataType":"DOUBLE","writable":true}],"commands":[{"id":"set","name":"设置","topic":"devices/{param:serial}/down","template":"{\"id\":\"$point:请求编号\",\"route\":\"$param:route\",\"uuid\":\"$param:uuid\",\"code\":\"$param:serial\",\"channel\":\"$param:channel\",\"enabled\":\"$param:enabled\",\"meta\":\"$param:meta\",\"operation\":\"set\",\"value\":\"$point:温度\"}","requiredPointIds":["temperature"],"response":{"topic":"devices/{param:serial}/result","matchFields":["/id","/route","/operation"],"codeField":"/result/code","successCode":0,"timeoutMs":10000}}]})";
    for (auto& device : fixture.snapshot->devices) {
        device.calculationConfig = config;
        device.parameters = "{\"route\":\"A" + device.code + "\",\"channel\":2,\"enabled\":true,\"meta\":{\"type\":\"water\"}}";
    }
    fixture.session = userSession(fixture.snapshot);
    (void)fixture.session->connected();
    const auto subscriptions = fixture.receive({ 0x20, 2, 0, 0 });
    const std::string subscriptionBytes(subscriptions.front().bytes.begin(), subscriptions.front().bytes.end());
    require(subscriptionBytes.find("devices/0/result") != std::string::npos && subscriptionBytes.find("devices/1/result") != std::string::npos, "device result topics were not subscribed");
    (void)fixture.receive({ 0x90, 6, 0, 1, 1, 1, 1, 1 });
    auto issue = [&](std::string id, std::string device = "0") {
        return fixture.execute({ .id = std::move(id), .deviceId = std::move(device), .elements = { { "temperature", "12.5" } }, .mqttMessageId = "set" });
    };
    auto requestId = [](const std::vector<ProtocolAction>& actions) {
        const std::string frame(actions.front().bytes.begin(), actions.front().bytes.end());
        require((actions.front().bytes.front() & 1) == 0, "control message must never be retained");
        const auto payload = frame.substr(frame.find('{'));
        const service::utils::PayloadRecord sent(payload, "json", ",");
        const auto id = sent.field("id");
        require(id && std::holds_alternative<double>(*id) && std::get<double>(*id) > 0 && std::get<double>(*id) <= 9007199254740991., "command ID is not a safe positive JSON integer");
        require(sent.field("uuid") == service::utils::PayloadScalar{ "0" } && sent.field("code") == service::utils::PayloadScalar{ "0" }, "device identity was not substituted");
        require(sent.field("channel") == service::utils::PayloadScalar{ 2.0 } && sent.field("enabled") == service::utils::PayloadScalar{ true }, "parameter JSON types were changed");
        require(sent.field("route") == service::utils::PayloadScalar{ "A0" }, "device parameter was not substituted");
        return service::utils::payloadScalarJson(*id);
    };
    const auto first = issue("business-1");
    const auto firstId = requestId(first);
    const auto ack = fixture.receive({ 0x40, 2, 0, 2 });
    require(std::none_of(ack.begin(), ack.end(), [](const auto& action) {
                return action.kind == ProtocolActionKind::CompleteCommand;
            }),
            "broker PUBACK reported device success");
    auto result = [&](const std::string& id, std::string route = "A0", std::string operation = "set", int code = 0) {
        return "{\"id\":" + id + ",\"route\":\"" + route + "\",\"operation\":\"" + operation + "\",\"result\":{\"code\":" + std::to_string(code) + ",\"response_hex\":null}}";
    };
    auto completed = [](const auto& actions) {
        return std::count_if(actions.begin(), actions.end(), [](const auto& action) {
            return action.kind == ProtocolActionKind::CompleteCommand || action.kind == ProtocolActionKind::FailCommand;
        });
    };
    for (const auto& payload : { result("1"), result(firstId, "A1"), result(firstId, "A0", "other") }) {
        require(completed(fixture.receive(publication("devices/0/result", payload, 1))) == 0, "unmatched result completed command");
    }
    require(completed(fixture.receive(publication("devices/1/result", result(firstId), 1))) == 0, "other device completed command");
    auto retained = publication("devices/0/result", result(firstId), 1);
    retained.front() |= 1;
    require(completed(fixture.receive(retained)) == 0, "retained result completed command");
    const auto success = fixture.receive(publication("devices/0/result", result(firstId), 1));
    require(completed(success) == 1 && success.front().commandId == "business-1" && success.front().kind == ProtocolActionKind::CompleteCommand, "matched device success missing");
    require(completed(fixture.receive(publication("devices/0/result", result(firstId), 1))) == 0, "duplicate result completed command twice");
    const auto second = issue("business-2");
    const auto secondId = requestId(second);
    require(secondId != firstId, "new command reused numeric ID");
    const auto failure = fixture.receive(publication("devices/0/result", result(secondId, "A0", "set", 1), 1));
    require(completed(failure) == 1 && failure.front().kind == ProtocolActionKind::FailCommand && failure.front().reason.find("response_hex") != std::string::npos, "device failure or raw result lost");
    require(completed(fixture.receive({ 0x40, 2, 0, 3 })) == 0, "late PUBACK completed failed command");
    const auto third = issue("business-3");
    (void)fixture.receive({ 0x40, 2, 0, 4 });
    const auto timeout = dynamic_cast<DeadlineCapabilitySession*>(fixture.session.get())->deadline(third.back().deadlineToken);
    require(timeout.size() == 1 && timeout.front().kind == ProtocolActionKind::FailCommand && timeout.front().reason == "mqtt_device_response_timeout", "response timeout retried or closed unrelated devices");
    const auto other = issue("device-two", "1");
    const std::string frame(other.front().bytes.begin(), other.front().bytes.end());
    const auto payload = frame.substr(frame.find('{'));
    const service::utils::PayloadRecord otherSent(payload, "json", ",");
    require(otherSent.field("route") == service::utils::PayloadScalar{ "A1" } && otherSent.field("uuid") == service::utils::PayloadScalar{ "1" }, "another device reused parameters or identity");
    fixture.snapshot->devices.front().parameters = "{}";
    fixture.session = userSession(fixture.snapshot);
    (void)fixture.session->connected();
    (void)fixture.receive({ 0x20, 2, 0, 0 });
    (void)fixture.receive({ 0x90, 6, 0, 1, 1, 1, 1, 1 });
    const auto missing = issue("missing-parameter");
    require(missing.size() == 1 && missing.front().kind == ProtocolActionKind::FailCommand && missing.front().reason.find("$param:") != std::string::npos, "missing parameter published an incomplete command");
}

void testMultipleReports() {
    Fixture fixture;
    auto& device = fixture.snapshot->devices.front();
    device.calculationConfig = R"json({"reports":[{"id":"00000000-0000-4000-8000-000000000001","name":"温度","topic":"devices/{param:serial}/temperature","qos":1,"reportTemplate":"{\"value\":\"$point:温度\"}","pointFields":[{"pointId":"temperature","field":"/value"}]},{"id":"00000000-0000-4000-8000-000000000002","name":"状态","topic":"devices/{param:serial}/state","qos":1,"reportTemplate":"{\"enabled\":\"$point:开关\"}","pointFields":[{"pointId":"enabled","field":"/enabled"}]}],"points":[{"id":"temperature","name":"温度","field":"","dataType":"DOUBLE"},{"id":"enabled","name":"开关","field":"","dataType":"BOOL"}]})json";
    fixture.session = userSession(fixture.snapshot);
    (void)fixture.session->connected();
    const auto subscribe = fixture.receive({ 0x20, 2, 0, 0 });
    require(subscribe.front().bytes.front() == 0x82, "multiple report topics not subscribed");
    const auto subscribed = fixture.receive({ 0x90, 4, 0, 1, 1, 1 });
    require(subscribed.front().kind == ProtocolActionKind::CancelDeadline, "multiple report SUBACK rejected");
    const auto temperature = fixture.receive(publication("devices/0/temperature", R"({"value":25,"enabled":false})", 0));
    require(temperature.size() == 2 && temperature.front().parsed.valuesJson.find("25") != std::string::npos && temperature.front().parsed.valuesJson.find("enabled") == std::string::npos, "first report used another message mapping");
    const auto state = fixture.receive(publication("devices/0/state", R"({"enabled":true,"value":100})", 0));
    require(state.size() == 2 && state.front().parsed.valuesJson.find("true") != std::string::npos && state.front().parsed.valuesJson.find("temperature") == std::string::npos, "second report used another message mapping");
}

void testUserDefinedIdentity() {
    Fixture fixture;
    auto& device = fixture.snapshot->devices.front();
    device.code = "management-code";
    device.parameters = R"({"硬件序列号":"hardware-7"})";
    device.calculationConfig = R"({"identityProperty":"硬件序列号","topic":"factory/{param:硬件序列号}/up","qos":1,"commandTopic":"factory/{param:硬件序列号}/set","commandTemplate":"{\"my_device\":\"$param:硬件序列号\",\"value\":\"$point:温度\"}","points":[{"id":"temperature","name":"温度","field":"reading","dataType":"DOUBLE","writable":true}]})";
    fixture.session = mqtt::Factory{}.createSession(fixture.snapshot->links.front(), "connection", "broker", fixture.snapshot);
    (void)fixture.session->connected();
    (void)fixture.receive({ 0x20, 2, 0, 0 });
    (void)fixture.receive({ 0x90, 3, 0, 1, 1 });
    require(fixture.receive(publication("factory/management-code/up", R"({"reading":8})", 0)).empty(), "management code identified a custom device");
    const auto report = fixture.receive(publication("factory/hardware-7/up", R"({"reading":8})", 0));
    require(report.size() == 2 && report.front().deviceId == device.id && report.front().parsed.deviceCode == "management-code", "custom identifier failed or overwrote platform metadata");
    const auto command = fixture.execute({ .id = "custom", .deviceId = device.id, .elements = { { "temperature", "12" } } });
    require(publishedPayload(command) == R"({"my_device":"hardware-7","value":12})", "user property was not encoded");
    device.parameters = R"({"硬件序列号":null})";
    bool rejected = false;
    try {
        (void)mqtt::Factory{}.createSession(fixture.snapshot->links.front(), "connection", "broker", fixture.snapshot);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    require(rejected, "missing identifier fell back to a platform field");
}

int main() {
    try {
        testUserDefinedIdentity();
        testMultipleReports();
        testConfiguredCommandResponses();
        testFramingAndRouting();
        testQos2AndCommand();
        testFailuresAndKeepAlive();
        testBatchPathsAndRetries();
        testTextAndBinaryCommands();
        testNamedTemplates();
        testMultipleMessagesAndRequiredPoints();
        std::cout << "MQTT tests passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
