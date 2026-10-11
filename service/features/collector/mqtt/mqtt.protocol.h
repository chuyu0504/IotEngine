#pragma once

#include <deque>
#include <set>

#include "service/features/collector/collector.protocol.h"
#include "service/features/collector/mqtt/mqtt.types.h"
#include "service/utils/json.h"
#include "service/utils/payload.h"

namespace service::collector::mqtt {

using Bytes = std::vector<std::uint8_t>;
inline constexpr std::size_t kMaxPacket = 1024 * 1024;
inline constexpr std::size_t kMaxInflight = 256;

// MQTT 3.1.1 UTF-8 字段不能含 U+0000、代理项或非最短编码。
inline bool validText(std::string_view text) {
    if (text.size() > 65535) {
        return false;
    }
    for (std::size_t i = 0; i < text.size();) {
        const auto first = static_cast<unsigned char>(text[i++]);
        std::uint32_t value = first;
        unsigned continuation = 0;
        if (first < 0x80) {
            if (!first) {
                return false;
            }
            continue;
        }
        if (first >= 0xc2 && first <= 0xdf) {
            value &= 0x1f;
            continuation = 1;
        } else if (first >= 0xe0 && first <= 0xef) {
            value &= 0x0f;
            continuation = 2;
        } else if (first >= 0xf0 && first <= 0xf4) {
            value &= 7;
            continuation = 3;
        } else {
            return false;
        }
        const auto count = continuation;
        while (continuation--) {
            if (i == text.size()) {
                return false;
            }
            const auto next = static_cast<unsigned char>(text[i++]);
            if ((next & 0xc0) != 0x80) {
                return false;
            }
            value = (value << 6) | (next & 0x3f);
        }
        if ((count == 1 && value < 0x80) || (count == 2 && value < 0x800) ||
            (count == 3 && value < 0x10000) || value > 0x10ffff ||
            (value >= 0xd800 && value <= 0xdfff)) {
            return false;
        }
    }
    return true;
}

inline std::string deviceTopic(std::string topic, std::string_view code, bool filter = false) {
    constexpr std::string_view marker = "{deviceCode}";
    for (auto position = topic.find(marker); position != std::string::npos;
         position = topic.find(marker, position + code.size())) {
        topic.replace(position, marker.size(), code);
    }
    if (topic.empty() || !validText(topic) || (!filter && topic.find_first_of("+#") != std::string::npos)) {
        throw std::invalid_argument("MQTT topic must be a concrete UTF-8 topic");
    }
    if (filter) {
        const auto levels = service::utils::splitPayload(topic, "/");
        for (std::size_t i = 0; i < levels.size(); ++i) {
            if ((levels[i].find('+') != std::string_view::npos && levels[i] != "+") ||
                (levels[i].find('#') != std::string_view::npos && (levels[i] != "#" || i + 1 != levels.size()))) {
                throw std::invalid_argument("invalid MQTT topic filter");
            }
        }
    }
    return topic;
}

inline bool topicMatches(std::string_view filter, std::string_view topic) {
    if (topic.starts_with('$') && !filter.starts_with('$')) {
        return false;
    }
    const auto pattern = service::utils::splitPayload(filter, "/"), levels = service::utils::splitPayload(topic, "/");
    std::size_t i = 0;
    for (; i < pattern.size(); ++i) {
        if (pattern[i] == "#") {
            return true;
        }
        if (i >= levels.size() || (pattern[i] != "+" && pattern[i] != levels[i])) {
            return false;
        }
    }
    return i == levels.size();
}

inline void word(Bytes& bytes, std::uint16_t value) {
    bytes.push_back(static_cast<std::uint8_t>(value >> 8));
    bytes.push_back(static_cast<std::uint8_t>(value));
}

inline void text(Bytes& bytes, std::string_view value) {
    if (!validText(value)) {
        throw std::invalid_argument("invalid MQTT UTF-8 string");
    }
    word(bytes, static_cast<std::uint16_t>(value.size()));
    bytes.insert(bytes.end(), value.begin(), value.end());
}

inline Bytes packet(std::uint8_t header, const Bytes& body) {
    if (body.size() > kMaxPacket) {
        throw std::invalid_argument("MQTT packet exceeds 1 MiB limit");
    }
    Bytes result{ header };
    auto length = body.size();
    do {
        auto digit = static_cast<std::uint8_t>(length % 128);
        length /= 128;
        result.push_back(digit | (length ? 0x80 : 0));
    } while (length);
    result.insert(result.end(), body.begin(), body.end());
    return result;
}

inline Bytes acknowledgement(std::uint8_t header, std::uint16_t id) {
    Bytes body;
    word(body, id);
    return packet(header, body);
}

class Session final : public ProtocolSession, public CommandCapabilitySession, public DeadlineCapabilitySession {
  public:
    Session(const LinkDefinition& link, std::string connectionId, std::string_view targetId, std::shared_ptr<const RuntimeSnapshot> snapshot, std::vector<const DeviceDefinition*> devices)
        : linkId_(link.id), connectionId_(std::move(connectionId)), snapshot_(std::move(snapshot)) {
        const auto target = std::find_if(link.targets.begin(), link.targets.end(), [&](const auto& item) {
            return item.id == targetId;
        });
        if (target == link.targets.end()) {
            throw std::invalid_argument("MQTT broker configuration is missing");
        }
        auto connection = ruvia::from_json<Connection>(target->mqttConfig);
        if (!connection) {
            throw std::invalid_argument("invalid MQTT broker configuration");
        }
        connection_ = std::move(*connection);
        const auto clientId = connection_.get<"clientId">().view();
        keepAlive_ = connection_.get<"keepAliveSeconds">().value_or(ruvia::int64{ 60 }).value_;
        if (clientId.empty() || !validText(clientId) || keepAlive_ < 1 || keepAlive_ > 65535) {
            throw std::invalid_argument("invalid MQTT client ID or keep alive");
        }
        for (const auto* device : devices) {
            auto parsed = ruvia::from_json<Config>(device->calculationConfig);
            if (!parsed) {
                throw std::invalid_argument("invalid MQTT device configuration");
            }
            const auto& config = *parsed;
            const auto qos = config.get<"qos">().value_or(ruvia::int64{ 1 }).value_;
            if (qos < 0 || qos > 2) {
                throw std::invalid_argument("invalid MQTT QoS");
            }
            const auto& codeField = config.get<"deviceCodeField">();
            const auto& recordsPath = config.get<"recordsPath">();
            Subscription subscription{ .device = device, .topic = deviceTopic(std::string(config.get<"topic">().view()), device->code, true), .deviceCodeField = codeField ? std::string(codeField->view()) : "", .recordsPath = recordsPath ? std::string(recordsPath->view()) : "", .qos = static_cast<std::uint8_t>(qos) };
            const auto stringOption = [](const auto& value, std::string fallback) {
                return value ? std::string(value->view()) : fallback;
            };
            subscription.identitySource = stringOption(config.get<"identitySource">(), subscription.deviceCodeField.empty() ? "bound" : "payload");
            subscription.format = stringOption(config.get<"payloadFormat">(), "json");
            subscription.delimiter = stringOption(config.get<"delimiter">(), ",");
            subscription.recordDelimiter = stringOption(config.get<"recordDelimiter">(), "\n");
            subscription.timeField = stringOption(config.get<"timeField">(), "");
            subscription.timeFormat = stringOption(config.get<"timeFormat">(), "unix_ms");
            subscription.topicSegment = static_cast<std::size_t>(config.get<"topicDeviceSegment">().value_or(ruvia::int64{ 1 }).value_);
            subscription.recordLength = static_cast<std::size_t>(config.get<"recordLength">().value_or(ruvia::int64{ 0 }).value_);
            if (const auto& topic = config.get<"commandTopic">(); topic && !topic->view().empty()) {
                subscription.commandTopic = deviceTopic(std::string(topic->view()), device->code);
            }
            std::map<std::string, bool, std::less<>> templatePoints;
            for (const auto& point : config.get<"points">()) {
                const auto& unit = point.get<"unit">();
                if (!templatePoints.emplace(std::string(point.get<"name">().view()), point.get<"writable">().value_or(ruvia::bool_value{ false }).value_).second) {
                    throw std::invalid_argument("MQTT 点位名称重复");
                }
                subscription.points.push_back({ std::string(point.get<"id">().view()), std::string(point.get<"name">().view()), std::string(point.get<"field">().view()), std::string(point.get<"dataType">().view()), unit ? std::string(unit->view()) : "", point.get<"scale">().value_or(ruvia::double_value{ 1 }).value_, point.get<"offset">().value_or(ruvia::double_value{ 0 }).value_ });
                if (const auto& values = point.get<"enumValues">(); values) {
                    for (const auto& value : *values) {
                        subscription.points.back().enums.emplace_back(value.get<"input">().view(), value.get<"output">().view());
                    }
                }
            }
            const auto compileTemplate = [&](std::string_view input, bool report = false, const std::set<std::string, std::less<>>& required = {}, bool legacyValues = true) {
                service::utils::JsonValueTemplate::Tokens allowed{ "$deviceCode", report ? "$time" : "$values" }, sparse;
                if (!report && !legacyValues) allowed.erase("$values");
                for (const auto& [name, writable] : templatePoints) {
                    const auto token = "$point:" + name;
                    allowed.insert(token);
                    if (!required.contains(token)) sparse.insert(token);
                }
                service::utils::JsonValueTemplate result(input, allowed, sparse, report);
                for (const auto& [name, writable] : templatePoints) {
                    if (!result.references("$point:" + name)) continue;
                    if (!report && !writable) throw std::invalid_argument("指令模板引用不可写点位: " + name);
                }
                return result;
            };
            if (const auto& value = config.get<"commandTemplate">(); value && !value->view().empty()) {
                if (subscription.format != "json") throw std::invalid_argument("指令模板仅用于 JSON");
                subscription.commandTemplate = compileTemplate(value->view());
            }
            if (const auto& commands = config.get<"commands">(); commands) {
                if (subscription.format != "json" || commands->size() > 32 || !subscription.commandTopic.empty() || subscription.commandTemplate)
                    throw std::invalid_argument("MQTT 控制消息配置冲突或格式无效");
                for (const auto& message : *commands) {
                    std::set<std::string, std::less<>> required, topicTokens;
                    for (const auto& id : message.get<"requiredPointIds">()) {
                        const auto point = std::find_if(subscription.points.begin(), subscription.points.end(), [&](const auto& point) { return point.id == id.view(); });
                        if (point == subscription.points.end() || !required.emplace("$point:" + point->name).second)
                            throw std::invalid_argument("必填控制点未知或重复");
                    }
                    const auto topic = std::string(message.get<"topic">().view());
                    (void)deviceTopic(topic, device->code);
                    for (const auto level : service::utils::splitPayload(topic, "/")) {
                        if (level == "{deviceCode}") continue;
                        if (level.starts_with("{point:") && level.ends_with('}')) {
                            const auto token = "$point:" + std::string(level.substr(7, level.size() - 8));
                            if (!required.contains(token)) throw std::invalid_argument("Topic 属性必须设为必填");
                            topicTokens.emplace(token);
                        } else if (level.find_first_of("{}") != std::string_view::npos) {
                            throw std::invalid_argument("Topic 占位符须独占一层");
                        }
                    }
                    auto compiled = compileTemplate(message.get<"templateText">().view(), false, required, false);
                    service::utils::JsonValueTemplate::Values requiredValues{{"$deviceCode", "\"device\""}};
                    for (const auto& token : required) {
                        if (!compiled.references(token) && !topicTokens.contains(token)) throw std::invalid_argument("必填控制点不属于当前消息");
                        requiredValues.emplace(token, "null");
                    }
                    (void)compiled.render(requiredValues);
                    if (!subscription.commands.emplace(std::string(message.get<"id">().view()), CompiledCommand{topic, std::move(compiled), std::move(topicTokens)}).second)
                        throw std::invalid_argument("控制消息标识重复");
                }
            }
            if (const auto& value = config.get<"reportTemplate">(); value) {
                if (subscription.format != "json") throw std::invalid_argument("上报模板仅用于 JSON");
                (void)compileTemplate(value->view(), true);
            }
            for (const auto& previous : subscriptions_) {
                if (previous.topic == subscription.topic && previous.recordsPath == subscription.recordsPath &&
                    (subscription.identitySource == "bound" || previous.identitySource == "bound" ||
                     (previous.deviceCodeField == subscription.deviceCodeField && previous.device->code == device->code))) {
                    throw std::invalid_argument("shared MQTT topic requires a consistent device code field and unique device codes");
                }
            }
            topics_[subscription.topic] = std::max(topics_[subscription.topic], subscription.qos);
            subscriptions_.push_back(std::move(subscription));
        }
    }

    std::vector<ProtocolAction> connected() override {
        Bytes body;
        text(body, "MQTT");
        body.push_back(4);
        const auto& username = connection_.get<"username">();
        const auto& password = connection_.get<"password">();
        if (password && !password->view().empty() && (!username || username->view().empty())) {
            throw std::invalid_argument("MQTT password requires a username");
        }
        const bool user = username && !username->view().empty(), pass = password && !password->view().empty();
        body.push_back(2 | (user ? 128 : 0) | (pass ? 64 : 0)); // Clean Session，重连后重新订阅。
        word(body, static_cast<std::uint16_t>(keepAlive_));
        text(body, connection_.get<"clientId">().view());
        if (user) {
            text(body, username->view());
        }
        if (pass) {
            if (password->view().size() > 65535) {
                throw std::invalid_argument("MQTT password too long");
            }
            word(body, static_cast<std::uint16_t>(password->view().size()));
            body.insert(body.end(), password->view().begin(), password->view().end());
        }
        return { send(packet(0x10, body)), timer(kHandshake, 10000) };
    }

    std::vector<ProtocolAction> consume(const ProtocolInput& input) override {
        if (closed_) {
            return {};
        }
        // Redis 消费重试不能再次推进状态机；重放同一批动作并保留遥测幂等 ID。
        if (!input.messageId.empty() && cachedInputId_ == input.messageId) {
            return cachedActions_;
        }
        cachedInputId_ = input.messageId;
        publishedAcks_.clear();
        std::vector<ProtocolAction> actions;
        try {
            if (buffer_.size() + input.bytes.size() > 2 * kMaxPacket + 10) {
                throw std::invalid_argument("MQTT receive buffer overflow");
            }
            buffer_.insert(buffer_.end(), input.bytes.begin(), input.bytes.end());
            while (buffer_.size() >= 2) {
                std::size_t length = 0, offset = 1, multiplier = 1;
                bool complete = false;
                for (unsigned i = 0; i < 4; ++i) {
                    if (offset == buffer_.size()) {
                        break;
                    }
                    const auto digit = buffer_[offset++];
                    length += (digit & 127) * multiplier;
                    if (!(digit & 128)) {
                        if (i && digit == 0) {
                            throw std::invalid_argument("noncanonical MQTT length");
                        }
                        complete = true;
                        break;
                    }
                    if (i == 3) {
                        throw std::invalid_argument("invalid MQTT remaining length");
                    }
                    multiplier *= 128;
                }
                if (!complete) {
                    break;
                }
                if (length > kMaxPacket) {
                    throw std::invalid_argument("MQTT frame too large");
                }
                if (buffer_.size() < offset + length) {
                    break;
                }
                Bytes frame(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(offset + length));
                buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(offset + length));
                receive(frame[0], std::span(frame).subspan(offset), frame, input, actions);
            }
        } catch (const std::exception& error) {
            auto failure = disconnected(error.what());
            actions.insert(actions.end(), failure.begin(), failure.end());
            actions.push_back({ .kind = ProtocolActionKind::Close, .connectionId = connectionId_, .reason = "mqtt_protocol_error: " + std::string(error.what()) });
        }
        cachedActions_ = actions;
        return actions;
    }

    std::vector<ProtocolAction> parsedPublished(std::uint64_t token) override {
        if (const auto completed = publishedAcks_.find(token); completed != publishedAcks_.end()) {
            return completed->second;
        }
        const auto found = publications_.find(token);
        if (found == publications_.end() || closed_) {
            return {};
        }
        const auto id = found->second;
        publications_.erase(found);
        auto pending = inbound_.find(id);
        if (pending == inbound_.end() || --pending->second.remaining) {
            return {};
        }
        const auto qos = pending->second.qos;
        inbound_.erase(pending);
        auto actions = std::vector<ProtocolAction>{ send(acknowledgement(qos == 1 ? 0x40 : 0x70, id)) };
        publishedAcks_[token] = actions;
        return actions;
    }

    std::vector<ProtocolAction> execute(ProtocolCommand request) override {
        const auto fail = [&](std::string reason) {
            return std::vector<ProtocolAction>{ { .kind = ProtocolActionKind::FailCommand,
                                                  .connectionId = connectionId_,
                                                  .deviceId = request.deviceId,
                                                  .commandId = request.id,
                                                  .reason = std::move(reason) } };
        };
        if (!ready_ || closed_) {
            return fail("mqtt_not_ready");
        }
        const auto subscription = std::find_if(subscriptions_.begin(), subscriptions_.end(), [&](const auto& item) {
            return item.device->id == request.deviceId;
        });
        if (subscription == subscriptions_.end() || (subscription->commandTopic.empty() && subscription->commands.empty())) {
            return fail("mqtt_command_topic_missing");
        }
        if (outbound_.size() >= kMaxInflight) {
            return fail("mqtt_inflight_limit");
        }
        try {
            const auto elements = command::resolve(*subscription->device, request.elements, request.mqttMessageId);
            const CompiledCommand* selected = nullptr;
            if (!request.mqttMessageId.empty()) {
                const auto found = subscription->commands.find(request.mqttMessageId);
                if (found == subscription->commands.end()) throw std::invalid_argument("command_invalid: 控制消息不存在");
                selected = &found->second;
            }
            const auto* commandTemplate = selected ? &selected->payload : (subscription->commandTemplate ? &*subscription->commandTemplate : nullptr);
            PayloadObject payloadObject;
            service::utils::JsonValueTemplate::Values namedValues;
            std::map<std::string, std::string, std::less<>> topicValues;
            const bool fieldPayload = !commandTemplate || commandTemplate->references("$values");
            std::map<std::size_t, std::string> columns;
            std::string payload;
            std::vector<bool> occupied;
            const auto assign = [&](std::string_view field, const service::utils::PayloadScalar& value) {
                if (subscription->format == "json") {
                    payloadObject.assign(field, service::utils::payloadScalarJson(value));
                } else if (subscription->format == "text") {
                    const auto text = service::utils::payloadScalarText(value);
                    if (text.find(subscription->delimiter) != std::string::npos || text.find(subscription->recordDelimiter) != std::string::npos) {
                        throw std::invalid_argument("指令文本包含分隔符");
                    }
                    if (!columns.emplace(service::utils::payloadIndex(field), text).second) {
                        throw std::invalid_argument("指令文本列重复");
                    }
                } else {
                    service::utils::writeBinaryPayloadField(payload, occupied, field, value);
                }
            };
            for (const auto& element : elements.elements) {
                const auto& point = *element.definition;
                const auto mapping = std::find_if(subscription->points.begin(), subscription->points.end(), [&](const auto& item) {
                    return item.id == point.id;
                });
                if (mapping == subscription->points.end()) {
                    throw std::invalid_argument("指令点位映射不存在");
                }
                auto value = service::utils::convertPayloadValue(service::utils::PayloadScalar{ element.value }, point.dataType);
                if (!value) {
                    throw std::invalid_argument("指令值无效");
                }
                if (auto* number = std::get_if<double>(&*value)) {
                    if (!mapping->scale) {
                        throw std::invalid_argument("倍率为零的点位不可写");
                    }
                    *number = (*number - mapping->offset) / mapping->scale;
                    if (!std::isfinite(*number)) {
                        throw std::invalid_argument("指令数值溢出");
                    }
                }
                if (!mapping->enums.empty()) {
                    bool found = false;
                    for (const auto& [input, output] : mapping->enums) {
                        const auto converted = service::utils::convertPayloadValue(service::utils::PayloadScalar{ output }, point.dataType);
                        if (converted && *converted == *value) {
                            value = service::utils::PayloadScalar{ input };
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw std::invalid_argument("指令值没有对应的枚举原值");
                    }
                }
                const auto token = "$point:" + mapping->name;
                if (commandTemplate && !commandTemplate->references("$values") && !commandTemplate->references(token) && !(selected && selected->topicTokens.contains(token))) {
                    throw std::invalid_argument("请求点位未出现在指令模板中: " + mapping->name);
                }
                if (commandTemplate && commandTemplate->references(token)) {
                    namedValues.emplace(token, service::utils::payloadScalarJson(*value));
                }
                if (selected && selected->topicTokens.contains(token)) {
                    auto textValue = service::utils::payloadScalarText(*value);
                    if (textValue.empty() || !validText(textValue) || textValue.find_first_of("/+#") != std::string::npos)
                        throw std::invalid_argument("command_invalid: Topic 属性值必须是非空且不含 /、+、# 的单层值");
                    topicValues.emplace(token, std::move(textValue));
                }
                if (fieldPayload) assign(point.mqttField, *value);
            }
            if (fieldPayload && subscription->identitySource == "payload") {
                assign(subscription->deviceCodeField, service::utils::PayloadScalar{ subscription->device->code });
            }
            if (subscription->format == "json") {
                if (commandTemplate) {
                    if (commandTemplate->references("$values")) namedValues.emplace("$values", payloadObject.json(true));
                    if (commandTemplate->references("$deviceCode")) namedValues.emplace("$deviceCode", service::utils::jsonQuoted(subscription->device->code));
                    payload = commandTemplate->render(namedValues);
                } else {
                    payload = payloadObject.json(true);
                }
            } else if (subscription->format == "text" && !columns.empty()) {
                if (columns.rbegin()->first > 4096) {
                    throw std::invalid_argument("指令文本列超出范围");
                }
                for (std::size_t i = 0; i <= columns.rbegin()->first; ++i) {
                    if (i) {
                        payload += subscription->delimiter;
                    }
                    if (columns.contains(i)) {
                        payload += columns.at(i);
                    }
                }
            } else if (subscription->format == "binary" && subscription->recordLength) {
                if (payload.size() > subscription->recordLength) {
                    throw std::invalid_argument("指令字段超出记录长度");
                }
                payload.resize(subscription->recordLength, '\0');
            }
            const auto id = nextId();
            Bytes body;
            std::string topic = subscription->commandTopic;
            if (selected) {
                bool first = true;
                for (const auto level : service::utils::splitPayload(selected->topic, "/")) {
                    if (!first) topic += '/';
                    first = false;
                    if (level == "{deviceCode}") topic += subscription->device->code;
                    else if (level.starts_with("{point:") && level.ends_with('}')) topic += topicValues.at("$point:" + std::string(level.substr(7, level.size() - 8)));
                    else topic += level;
                }
                if (topic.size() > 1024) throw std::invalid_argument("command_invalid: 控制 Topic 超过 1024 字节");
            }
            text(body, topic);
            word(body, id);
            body.insert(body.end(), payload.begin(), payload.end());
            const auto qos = std::max<std::uint8_t>(1, subscription->qos);
            auto publication = send(packet(0x30 | (qos << 1), body));
            outbound_.emplace(id, Outbound{ request.id, request.deviceId, qos, false });
            publication.commandId = request.id;
            publication.deviceId = request.deviceId;
            return { std::move(publication), timer(kCommandBase + id, std::max<std::int64_t>(1, request.timeout.count())) };
        } catch (const std::invalid_argument& error) {
            return fail(error.what());
        }
    }

    std::vector<ProtocolAction> deadline(std::uint64_t token) override {
        if (closed_) {
            return {};
        }
        if (token == kKeepAlive) {
            return { send({ 0xc0, 0 }), timer(kPing, std::min<std::int64_t>(10000, keepAlive_ * 1000)) };
        }
        if (token == kHandshake || token == kPing || (token >= kCommandBase && outbound_.contains(static_cast<std::uint16_t>(token - kCommandBase)))) {
            auto actions = disconnected("mqtt_response_timeout");
            actions.push_back({ .kind = ProtocolActionKind::Close, .connectionId = connectionId_, .reason = "mqtt_response_timeout" });
            return actions;
        }
        return {};
    }

    std::vector<ProtocolAction> disconnected(std::string_view reason) override {
        closed_ = true;
        ready_ = false;
        buffer_.clear();
        inbound_.clear();
        publications_.clear();
        std::vector<ProtocolAction> actions{ cancel(kHandshake), cancel(kKeepAlive), cancel(kPing) };
        for (const auto& [id, request] : outbound_) {
            actions.push_back(cancel(kCommandBase + id));
            actions.push_back({ .kind = ProtocolActionKind::FailCommand, .connectionId = connectionId_, .deviceId = request.deviceId, .commandId = request.commandId, .reason = std::string(reason) });
        }
        outbound_.clear();
        return actions;
    }

  private:
    struct PayloadObject {
        std::string value;
        std::map<std::string, PayloadObject> fields;

        void assign(std::string_view field, std::string jsonValue) {
            auto* node = this;
            for (const auto& key : service::utils::jsonFieldSegments(field)) {
                if (!node->value.empty()) {
                    throw std::invalid_argument("MQTT command JSON paths overlap");
                }
                node = &node->fields[key];
            }
            if (!node->fields.empty() || !node->value.empty()) {
                throw std::invalid_argument("MQTT command JSON paths overlap");
            }
            node->value = std::move(jsonValue);
        }

        std::string json(bool object = false) const {
            if (!value.empty()) {
                return value;
            }
            std::map<std::size_t, const PayloadObject*> indexes;
            if (!object && !fields.empty()) {
                for (const auto& [key, child] : fields) {
                    std::size_t index = 0;
                    const auto parsed = std::from_chars(key.data(), key.data() + key.size(), index);
                    if (key.empty() || (key.size() > 1 && key.front() == '0') || parsed.ec != std::errc{} || parsed.ptr != key.data() + key.size()) {
                        object = true;
                        break;
                    }
                    if (index > 4096) {
                        throw std::invalid_argument("MQTT command array index too large");
                    }
                    indexes.emplace(index, &child);
                }
            }
            if (!object && !indexes.empty()) {
                std::string result = "[";
                for (std::size_t i = 0; i <= indexes.rbegin()->first; ++i) {
                    if (i) {
                        result += ',';
                    }
                    const auto child = indexes.find(i);
                    result += child == indexes.end() ? "null" : child->second->json();
                }
                return result + ']';
            }
            std::string result = "{";
            for (const auto& [key, child] : fields) {
                if (result.size() > 1) {
                    result += ',';
                }
                result += service::utils::jsonQuoted(key) + ':' + child.json();
            }
            return result + '}';
        }
    };

    struct Mapping {
        std::string id, name, field, type, unit;
        double scale = 1, offset = 0;
        std::vector<std::pair<std::string, std::string>> enums;
    };

    struct CompiledCommand {
        std::string topic;
        service::utils::JsonValueTemplate payload;
        std::set<std::string, std::less<>> topicTokens;
    };

    struct Subscription {
        const DeviceDefinition* device;
        std::string topic, commandTopic, deviceCodeField, recordsPath;
        std::uint8_t qos;
        std::vector<Mapping> points;
        std::string identitySource, format, delimiter, recordDelimiter, timeField, timeFormat;
        std::optional<service::utils::JsonValueTemplate> commandTemplate;
        std::map<std::string, CompiledCommand, std::less<>> commands;
        std::size_t topicSegment = 1, recordLength = 0;
    };

    struct Inbound {
        std::uint8_t qos;
        bool released = false;
        std::size_t remaining = 0;
        Bytes frame;
        std::string topic, payload, messageId;
        std::int64_t time;
    };

    struct Outbound {
        std::string commandId, deviceId;
        std::uint8_t qos;
        bool released;
    };

    static constexpr std::uint64_t kHandshake = 1, kKeepAlive = 2, kPing = 3, kCommandBase = 65536;

    ProtocolAction send(Bytes bytes) const { return { .kind = ProtocolActionKind::Send, .connectionId = connectionId_, .bytes = std::move(bytes) }; }

    ProtocolAction timer(std::uint64_t token, std::int64_t ms) const { return { .kind = ProtocolActionKind::ScheduleDeadline,
                                                                                .connectionId = connectionId_,
                                                                                .deadlineToken = token,
                                                                                .deadlineAfter = std::chrono::milliseconds(ms) }; }

    ProtocolAction cancel(std::uint64_t token) const { return { .kind = ProtocolActionKind::CancelDeadline,
                                                                .connectionId = connectionId_,
                                                                .deadlineToken = token }; }

    std::uint16_t nextId() {
        do {
            if (++packetId_ == 0) {
                ++packetId_;
            }
        } while (outbound_.contains(packetId_) || subscribeId_ == packetId_);
        return packetId_;
    }

    static std::uint16_t readId(std::span<const std::uint8_t> bytes) {
        if (bytes.size() < 2 || (!bytes[0] && !bytes[1])) {
            throw std::invalid_argument("invalid MQTT packet identifier");
        }
        return static_cast<std::uint16_t>((bytes[0] << 8) | bytes[1]);
    }

    void receive(std::uint8_t header, std::span<const std::uint8_t> body, const Bytes& frame, const ProtocolInput& input, std::vector<ProtocolAction>& actions) {
        const auto type = header >> 4;
        if (type != 3 && header != 0x20 && header != 0x40 && header != 0x50 && header != 0x62 &&
            header != 0x70 && header != 0x90 && header != 0xd0) {
            throw std::invalid_argument("unexpected MQTT control packet");
        }
        if (!connack_ && type != 2) {
            throw std::invalid_argument("MQTT expected CONNACK");
        }
        if (type == 2) {
            if (connack_ || body.size() != 2 || body[0] != 0 || body[1] != 0) {
                throw std::invalid_argument("MQTT broker rejected CONNECT");
            }
            connack_ = true;
            if (subscriptions_.empty()) {
                ready_ = true;
                actions.push_back(cancel(kHandshake));
            } else {
                subscribeId_ = nextId();
                Bytes bytes;
                word(bytes, subscribeId_);
                for (const auto& [topic, qos] : topics_) {
                    text(bytes, topic);
                    bytes.push_back(qos);
                }
                actions.push_back(send(packet(0x82, bytes)));
            }
            actions.push_back(timer(kKeepAlive, keepAlive_ * 500));
            return;
        }
        if (type == 9) {
            if (!subscribeId_ || readId(body) != subscribeId_ || body.size() != topics_.size() + 2) {
                throw std::invalid_argument("invalid MQTT SUBACK");
            }
            std::size_t i = 2;
            for (const auto& [topic, qos] : topics_) {
                if (body[i++] > qos) {
                    throw std::invalid_argument("MQTT subscription rejected");
                }
            }
            subscribeId_ = 0;
            ready_ = true;
            actions.push_back(cancel(kHandshake));
            return;
        }
        if (type == 13) {
            if (!body.empty()) {
                throw std::invalid_argument("invalid MQTT PINGRESP");
            }
            actions.push_back(cancel(kPing));
            actions.push_back(timer(kKeepAlive, keepAlive_ * 500));
            return;
        }
        if (type == 3) {
            const auto qos = static_cast<std::uint8_t>((header >> 1) & 3);
            if (qos == 3 || (qos == 0 && (header & 8)) || body.size() < 2) {
                throw std::invalid_argument("invalid MQTT PUBLISH");
            }
            const auto length = static_cast<std::size_t>((body[0] << 8) | body[1]);
            if (!length || body.size() < 2 + length + (qos ? 2 : 0)) {
                throw std::invalid_argument("truncated MQTT topic");
            }
            const std::string topic(reinterpret_cast<const char*>(body.data() + 2), length);
            if (!validText(topic) || topic.find_first_of("+#") != std::string::npos) {
                throw std::invalid_argument("invalid MQTT publish topic");
            }
            const auto offset = 2 + length;
            const auto id = qos ? readId(body.subspan(offset)) : 0;
            Inbound message{ .qos = qos, .frame = frame, .topic = topic, .payload = std::string(reinterpret_cast<const char*>(body.data() + offset + (qos ? 2 : 0)), body.size() - offset - (qos ? 2 : 0)), .messageId = std::string(input.messageId), .time = input.receivedAtMs };
            if (!qos) {
                publish(message, 0, actions);
                return;
            }
            if (auto previous = inbound_.find(id); previous != inbound_.end()) {
                if (!(header & 8) || previous->second.topic != message.topic || previous->second.payload != message.payload || previous->second.qos != qos) {
                    throw std::invalid_argument("MQTT packet ID reused before acknowledgement");
                }
                if (qos == 2) {
                    actions.push_back(send(acknowledgement(0x50, id)));
                }
                return;
            }
            if (inbound_.size() >= kMaxInflight) {
                throw std::invalid_argument("MQTT inflight limit exceeded");
            }
            std::size_t pendingBytes = message.frame.size();
            for (const auto& [pendingId, pending] : inbound_) {
                pendingBytes += pending.frame.size();
            }
            if (pendingBytes > 16 * kMaxPacket) {
                throw std::invalid_argument("MQTT pending receive bytes exceeded");
            }
            auto& pending = inbound_.emplace(id, std::move(message)).first->second;
            if (qos == 2) {
                actions.push_back(send(acknowledgement(0x50, id)));
            } else {
                publish(pending, id, actions);
            }
            return;
        }
        if (body.size() != 2) {
            throw std::invalid_argument("invalid MQTT acknowledgement length");
        }
        const auto id = readId(body);
        if (type == 6) {
            const auto found = inbound_.find(id);
            if (found == inbound_.end()) {
                actions.push_back(send(acknowledgement(0x70, id)));
                return;
            }
            if (found->second.qos != 2) {
                throw std::invalid_argument("unexpected MQTT PUBREL");
            }
            if (!found->second.released) {
                found->second.released = true;
                publish(found->second, id, actions);
            }
            return;
        }
        const auto found = outbound_.find(id);
        if (found == outbound_.end()) {
            throw std::invalid_argument("unknown MQTT acknowledgement");
        }
        if (type == 5 && found->second.qos == 2) {
            found->second.released = true;
            actions.push_back(send(acknowledgement(0x62, id)));
            return;
        }
        if ((type == 4 && found->second.qos == 1) || (type == 7 && found->second.qos == 2 && found->second.released)) {
            actions.push_back(cancel(kCommandBase + id));
            actions.push_back({ .kind = ProtocolActionKind::CompleteCommand, .connectionId = connectionId_, .deviceId = found->second.deviceId, .commandId = found->second.commandId, .reason = "mqtt_broker_acknowledged" });
            outbound_.erase(found);
            return;
        }
        throw std::invalid_argument("unexpected MQTT acknowledgement");
    }

    void publish(Inbound& message, std::uint16_t id, std::vector<ProtocolAction>& actions) {
        for (const auto& item : subscriptions_) {
            if (!topicMatches(item.topic, message.topic)) {
                continue;
            }
            if (item.identitySource == "topic") {
                const auto levels = service::utils::splitPayload(message.topic, "/");
                if (item.topicSegment >= levels.size() || levels[item.topicSegment] != item.device->code) {
                    continue;
                }
            }
            try {
                const auto records = service::utils::payloadRecords(message.payload, item.format, item.recordsPath, item.recordDelimiter, item.recordLength);
                for (const auto& record : records) {
                    service::utils::PayloadRecord parsed(record, item.format, item.delimiter);
                    publishRecord(item, parsed, message, id, actions);
                }
            } catch (const std::invalid_argument&) { /* 无效业务负载不推进遥测，但仍确认 MQTT。 */
            }
        }
        // 无匹配点位的消息不进入业务流，但必须完成 MQTT 确认，避免毒消息反复重连。
        if (id && !message.remaining) {
            const auto qos = message.qos;
            inbound_.erase(id);
            actions.push_back(send(acknowledgement(qos == 1 ? 0x40 : 0x70, id)));
        }
    }

    void publishRecord(const Subscription& item, const service::utils::PayloadRecord& record, Inbound& message, std::uint16_t id, std::vector<ProtocolAction>& actions) {
        if (item.identitySource == "payload") {
            const auto code = record.field(item.deviceCodeField);
            if (!code || service::utils::payloadScalarText(*code) != item.device->code) {
                return;
            }
        }
        auto occurredAt = message.time;
        if (!item.timeField.empty()) {
            if (const auto time = record.field(item.timeField)) {
                const auto timestamp = service::utils::payloadTimestamp(*time, item.timeFormat, item.device->timezone);
                if (!timestamp) {
                    return;
                }
                occurredAt = *timestamp;
            }
        }
        std::string values = "{";
        for (const auto& point : item.points) {
            if (point.field.empty()) continue;
            auto input = record.field(point.field);
            if (input && !point.enums.empty()) {
                const auto key = service::utils::payloadScalarText(*input);
                const auto found = std::find_if(point.enums.begin(), point.enums.end(), [&](const auto& item) {
                    return item.first == key;
                });
                input = found == point.enums.end() ? std::nullopt : std::optional(service::utils::PayloadScalar{ found->second });
            }
            const auto value = input ? service::utils::convertPayloadValue(*input, point.type, point.scale, point.offset) : std::nullopt;
            if (!value) {
                continue;
            }
            if ((actions.size() / 2 + 1) * message.frame.size() > 16 * kMaxPacket) {
                throw std::length_error("MQTT batch exceeds receive budget");
            }
            if (values.size() > 1) {
                values += ',';
            }
            values += service::utils::jsonQuoted(point.id) + ":{\"name\":" + service::utils::jsonQuoted(point.name) +
                ",\"unit\":" + service::utils::jsonQuoted(point.unit) + ",\"value\":" + service::utils::payloadScalarJson(*value) + '}';
        }
        values += '}';
        if (values == "{}") {
            return;
        }
        ProtocolAction action{ .kind = ProtocolActionKind::PublishParsed, .connectionId = connectionId_, .deviceId = item.device->id, .deviceCode = item.device->code, .publicationToken = id ? ++publicationId_ : 0 };
        action.parsed = { .rawPacketIds = { message.messageId }, .causationId = message.messageId, .linkId = linkId_, .deviceId = item.device->id, .modelId = item.device->modelId, .deviceCode = item.device->code, .protocol = "MQTT", .connectionId = connectionId_, .occurredAtMs = occurredAt, .observedAtMs = message.time, .storagePolicy = item.device->storagePolicy, .onlineWindowMs = item.device->onlineTimeout * 1000, .valuesJson = "{\"function_code\":\"PUBLISH\",\"values\":" + values + '}', .rawPayloads = { message.frame } };
        action.parsed.acquisitionId = acquisitionIdentity(connectionId_, ++acquisitionSequence_);
        if (id) {
            publications_.emplace(action.publicationToken, id);
            ++message.remaining;
        }
        actions.push_back(std::move(action));
        actions.push_back({ .kind = ProtocolActionKind::BindDevice, .connectionId = connectionId_, .deviceId = item.device->id, .deviceCode = item.device->code });
    }

    std::string linkId_, connectionId_;
    std::shared_ptr<const RuntimeSnapshot> snapshot_;
    Connection connection_;
    std::vector<Subscription> subscriptions_;
    std::map<std::string, std::uint8_t> topics_;
    Bytes buffer_;
    std::map<std::uint16_t, Inbound> inbound_;
    std::map<std::uint16_t, Outbound> outbound_;
    std::map<std::uint64_t, std::uint16_t> publications_;
    std::string cachedInputId_;
    std::vector<ProtocolAction> cachedActions_;
    std::map<std::uint64_t, std::vector<ProtocolAction>> publishedAcks_;
    std::uint16_t packetId_ = 0, subscribeId_ = 0;
    std::uint64_t publicationId_ = 0;
    std::uint64_t acquisitionSequence_ = 0;
    std::int64_t keepAlive_ = 60;
    bool connack_ = false, ready_ = false, closed_ = false;
};

class Factory final : public ProtocolSessionFactory {
  public:
    const ProtocolDefinition& definition() const noexcept override { return kMqttProtocol; }

    std::vector<std::uint8_t> packetForLogging(std::span<const std::uint8_t> bytes) const override {
        // CONNECT 可能携带认证信息；日志只保留固定头，不记录其负载。
        if (!bytes.empty() && (bytes[0] >> 4) == 1) {
            return { 0x10, 0 };
        }
        return { bytes.begin(), bytes.end() };
    }

  protected:
    std::unique_ptr<ProtocolSession> createDeviceSession(const LinkDefinition& link, std::string_view connectionId, std::string_view targetId, const std::shared_ptr<const RuntimeSnapshot>& snapshot, std::vector<const DeviceDefinition*> devices) const override {
        return std::make_unique<Session>(link, std::string(connectionId), targetId, snapshot, std::move(devices));
    }
};

} // namespace service::collector::mqtt
