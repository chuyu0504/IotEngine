#pragma once

#include <memory>
#include <map>
#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <memory_resource>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/web/model_object.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/middleware/rpc.h"
#include "service/modules/edge_node/edge_node.service.h"
#include "service/modules/link/link.entity.h"
#include "service/modules/protocol/protocol.entity.h"
#include "service/modules/protocol/protocol.types.h"
#include "service/modules/system/outbox/outbox.service.h"
#include "service/modules/system/role/role.entity.h"
#include "service/modules/system/user/user.entity.h"
#include "service/utils/json.h"
#include "service/utils/payload.h"
#include "service/common/derived_point.h"
#include "service/common/mqtt_report.h"

namespace service::protocol {

class ProtocolConfigurationRules final {
  private:
    static std::string trimJson(std::string_view value) {
        const auto first = value.find_first_not_of(" \t\r\n");
        value = first == std::string_view::npos ? std::string_view{} : value.substr(first);
        while (!value.empty()) {
            const auto ch = value.back();
            if (ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n') {
                break;
            }
            value.remove_suffix(1);
        }
        return std::string(value);
    }

    static std::optional<std::string> stringValue(const ruvia::json_value& value) {
        const auto decoded = value.get<ruvia::string>();
        return decoded ? std::optional<std::string>(decoded->view()) : std::nullopt;
    }

  public:
    static void validateProtocol(std::string_view protocol) {
        if (protocol != "SL651" && protocol != "Modbus" && protocol != "S7" &&
            protocol != "MC" && protocol != "FINS" && protocol != "DLT645" && protocol != "MQTT") {
            service::common::fail(16003, "不支持的协议类型", 400);
        }
    }

    static void validateName(std::string_view name) {
        if (name.empty() || name.size() > 64) {
            service::common::fail(16002, "配置名称长度必须在 1 - 64 之间", 400);
        }
    }

  private:
    static bool hasField(const ruvia::json_value& object, std::string_view field) {
        return static_cast<bool>(object.get<ruvia::json_value>(field));
    }

    static bool oneOf(std::string_view value, std::initializer_list<std::string_view> values) {
        return std::find(values.begin(), values.end(), value) != values.end();
    }

    static bool enumFieldValid(const ruvia::json_value& object, std::string_view field, std::initializer_list<std::string_view> values) {
        const auto raw = object.get<ruvia::json_value>(field);
        if (!raw) {
            return true;
        }
        const auto value = stringValue(*raw);
        if (!value) {
            return false;
        }
        return oneOf(*value, values);
    }

    static std::optional<std::string> scalarText(const ruvia::json_value& value, bool allowString) {
        if (value.is_number()) {
            return canonicalNumericText(value.view());
        }
        if (allowString && value.is_string()) {
            return stringValue(value);
        }
        return std::nullopt;
    }

    // PostgreSQL jsonb stores numbers as numeric values, so ->> emits a
    // decimal token even when the request used an exponent (for example,
    // 1e3 becomes 1000). Keep that normalization local to numeric JSON
    // values; JSON strings must retain their original token for the integer
    // regex checks below.
    static std::optional<std::string> canonicalNumericText(std::string_view value, std::size_t maximum = 4096) {
        const auto trimmed = trimJson(value);
        std::string_view token = trimmed;
        if (token.empty()) {
            return std::nullopt;
        }

        bool negative = false;
        if (token.front() == '-' || token.front() == '+') {
            negative = token.front() == '-';
            token.remove_prefix(1);
        }
        if (token.empty()) {
            return std::nullopt;
        }

        const auto isDigit = [](char value) {
            return value >= '0' && value <= '9';
        };
        std::size_t position = 0;
        const auto integerStart = position;
        while (position < token.size() && isDigit(token[position])) {
            ++position;
        }
        const auto integerDigits = position - integerStart;
        std::size_t fractionDigits = 0;
        if (position < token.size() && token[position] == '.') {
            ++position;
            const auto fractionStart = position;
            while (position < token.size() && isDigit(token[position])) {
                ++position;
            }
            fractionDigits = position - fractionStart;
        }
        if (integerDigits == 0 && fractionDigits == 0) {
            return std::nullopt;
        }

        std::int64_t exponent = 0;
        if (position < token.size() && (token[position] == 'e' || token[position] == 'E')) {
            ++position;
            bool exponentNegative = false;
            if (position < token.size() && (token[position] == '-' || token[position] == '+')) {
                exponentNegative = token[position] == '-';
                ++position;
            }
            const auto exponentStart = position;
            while (position < token.size() && isDigit(token[position])) {
                ++position;
            }
            if (position == exponentStart) {
                return std::nullopt;
            }
            std::int64_t parsedExponent = 0;
            const auto [end, error] = std::from_chars(token.data() + exponentStart, token.data() + position, parsedExponent);
            if (error != std::errc{} || end != token.data() + position ||
                (exponentNegative && parsedExponent == (std::numeric_limits<std::int64_t>::max)())) {
                return std::nullopt;
            }
            exponent = exponentNegative ? -parsedExponent : parsedExponent;
        }
        if (position != token.size() || integerDigits > maximum ||
            fractionDigits > maximum || integerDigits + fractionDigits > maximum) {
            return std::nullopt;
        }

        std::string digits;
        digits.reserve(integerDigits + fractionDigits);
        digits.append(token, integerStart, integerDigits);
        if (fractionDigits != 0) {
            digits.append(token, integerStart + integerDigits + 1, fractionDigits);
        }

        const auto expansionLimit = static_cast<std::int64_t>(maximum - digits.size());
        if (exponent > expansionLimit || exponent < -expansionLimit) {
            return std::nullopt;
        }
        const auto integerCount = static_cast<std::int64_t>(integerDigits);
        if ((exponent > 0 && integerCount > (std::numeric_limits<std::int64_t>::max)() - exponent) ||
            (exponent < 0 && integerCount < (std::numeric_limits<std::int64_t>::min)() - exponent)) {
            return std::nullopt;
        }
        const auto decimalPosition = integerCount + exponent;

        std::size_t fractionLength = 0;
        if (decimalPosition <= 0) {
            const auto leadingFractionZeros = static_cast<std::size_t>(-decimalPosition);
            if (leadingFractionZeros > maximum - digits.size()) {
                return std::nullopt;
            }
            fractionLength = leadingFractionZeros + digits.size();
        } else if (decimalPosition < static_cast<std::int64_t>(digits.size())) {
            fractionLength = digits.size() - static_cast<std::size_t>(decimalPosition);
        }
        const auto integerLength = decimalPosition <= 0
            ? std::size_t{ 1 }
            : std::max(digits.size(), static_cast<std::size_t>(decimalPosition));
        const auto outputLength = integerLength + (fractionLength == 0 ? 0 : 1 + fractionLength);
        if (outputLength > maximum) {
            return std::nullopt;
        }

        std::string output;
        output.reserve(outputLength + (negative ? 1 : 0));
        if (decimalPosition <= 0) {
            output = "0.";
            output.append(static_cast<std::size_t>(-decimalPosition), '0');
            output += digits;
        } else if (decimalPosition >= static_cast<std::int64_t>(digits.size())) {
            output = digits;
            output.append(static_cast<std::size_t>(decimalPosition) - digits.size(), '0');
        } else {
            const auto split = static_cast<std::size_t>(decimalPosition);
            output.assign(digits.data(), split);
            output.push_back('.');
            output.append(digits.data() + split, digits.size() - split);
        }

        const auto dot = output.find('.');
        const auto integerEnd = dot == std::string::npos ? output.size() : dot;
        const auto firstNonZero = output.find_first_not_of('0', 0);
        if (firstNonZero == std::string::npos || firstNonZero >= integerEnd) {
            output.erase(0, integerEnd > 0 ? integerEnd - 1 : 0);
        } else if (firstNonZero > 0) {
            output.erase(0, firstNonZero);
        }

        bool nonZero = false;
        for (const auto digit : digits) {
            if (digit != '0') {
                nonZero = true;
                break;
            }
        }
        if (negative && nonZero) {
            output.insert(output.begin(), '-');
        }
        return output;
    }

    static int compareAbsoluteNumeric(std::string_view lhs, std::string_view rhs) {
        const auto stripLeadingZeros = [](std::string_view value) {
            const auto first = value.find_first_not_of('0');
            if (first == std::string_view::npos) {
                return std::string_view{ "0" };
            }
            return value.substr(first);
        };
        const auto integerPart = [](std::string_view value) {
            const auto dot = value.find('.');
            return value.substr(0, dot == std::string_view::npos ? value.size() : dot);
        };
        const auto fractionPart = [](std::string_view value) {
            const auto dot = value.find('.');
            return dot == std::string_view::npos ? std::string_view{} : value.substr(dot + 1);
        };
        const auto lhsInteger = stripLeadingZeros(integerPart(lhs));
        const auto rhsInteger = stripLeadingZeros(integerPart(rhs));
        if (lhsInteger.size() != rhsInteger.size()) {
            return lhsInteger.size() < rhsInteger.size() ? -1 : 1;
        }
        if (lhsInteger != rhsInteger) {
            return lhsInteger < rhsInteger ? -1 : 1;
        }

        const auto lhsFraction = fractionPart(lhs);
        const auto rhsFraction = fractionPart(rhs);
        const auto length = std::max(lhsFraction.size(), rhsFraction.size());
        for (std::size_t index = 0; index < length; ++index) {
            const auto left = index < lhsFraction.size() ? lhsFraction[index] : '0';
            const auto right = index < rhsFraction.size() ? rhsFraction[index] : '0';
            if (left != right) {
                return left < right ? -1 : 1;
            }
        }
        return 0;
    }

    static int compareNumeric(std::string_view lhs, std::string_view rhs) {
        const auto left = canonicalNumericText(lhs);
        const auto right = canonicalNumericText(rhs);
        if (!left || !right) {
            return 0;
        }
        const bool leftNegative = !left->empty() && left->front() == '-';
        const bool rightNegative = !right->empty() && right->front() == '-';
        if (leftNegative != rightNegative) {
            return leftNegative ? -1 : 1;
        }
        const auto leftAbsolute = leftNegative ? std::string_view(*left).substr(1) : std::string_view(*left);
        const auto rightAbsolute = rightNegative ? std::string_view(*right).substr(1) : std::string_view(*right);
        const auto result = compareAbsoluteNumeric(leftAbsolute, rightAbsolute);
        return leftNegative ? -result : result;
    }

    static bool unsignedDigits(std::string_view value, std::size_t maximum) {
        if (value.empty() || value.size() > maximum) {
            return false;
        }
        return std::all_of(value.begin(), value.end(), [](char ch) {
            return ch >= '0' && ch <= '9';
        });
    }

    static bool signedDigits(std::string_view value, std::size_t maximum) {
        if (!value.empty() && value.front() == '-') {
            value.remove_prefix(1);
        }
        return unsignedDigits(value, maximum);
    }

    static std::optional<std::int64_t> integerFieldValue(
        const std::optional<ruvia::json_value>& raw,
        bool allowString,
        std::size_t maximum,
        std::int64_t lower,
        std::int64_t upper
    ) {
        if (!raw) {
            return std::nullopt;
        }
        const auto text = scalarText(*raw, allowString);
        if (!text || !unsignedDigits(*text, maximum)) {
            return std::nullopt;
        }
        std::int64_t value{};
        const auto [end, error] =
            std::from_chars(text->data(), text->data() + text->size(), value);
        if (error != std::errc{} || end != text->data() + text->size() || value < lower ||
            value > upper) {
            return std::nullopt;
        }
        return value;
    }

    static std::optional<std::int64_t> integerFieldValue(
        const ruvia::json_value& object,
        std::string_view field,
        bool allowString,
        std::size_t maximum,
        std::int64_t lower,
        std::int64_t upper
    ) {
        return integerFieldValue(object.get<ruvia::json_value>(field), allowString, maximum, lower, upper);
    }

    static bool integerFieldValid(const std::optional<ruvia::json_value>& raw, bool allowString, std::size_t maximum, std::int64_t lower, std::int64_t upper, bool required = false) {
        if (!raw) {
            return !required;
        }
        return integerFieldValue(raw, allowString, maximum, lower, upper).has_value();
    }

    static bool integerFieldValid(const ruvia::json_value& object, std::string_view field, bool allowString, std::size_t maximum, std::int64_t lower, std::int64_t upper, bool required = false) {
        return integerFieldValid(object.get<ruvia::json_value>(field), allowString, maximum, lower, upper, required);
    }

    static bool signedIntegerFieldValid(const std::optional<ruvia::json_value>& raw, bool allowString, std::size_t maximum, std::int64_t lower, std::int64_t upper) {
        if (!raw) {
            return true;
        }
        const auto text = scalarText(*raw, allowString);
        if (!text || !signedDigits(*text, maximum)) {
            return false;
        }
        std::int64_t value{};
        const auto [end, error] =
            std::from_chars(text->data(), text->data() + text->size(), value);
        return error == std::errc{} && end == text->data() + text->size() && value >= lower &&
            value <= upper;
    }

    static bool signedIntegerFieldValid(const ruvia::json_value& object, std::string_view field, bool allowString, std::size_t maximum, std::int64_t lower, std::int64_t upper) {
        return signedIntegerFieldValid(object.get<ruvia::json_value>(field), allowString, maximum, lower, upper);
    }

    static bool numberFieldValid(const std::optional<ruvia::json_value>& raw, bool allowString, std::string_view lower, std::string_view upper) {
        if (!raw) {
            return true;
        }
        const auto text = scalarText(*raw, allowString);
        if (!text || text->empty()) {
            return false;
        }
        return compareNumeric(*text, lower) >= 0 && compareNumeric(*text, upper) <= 0;
    }

    static bool numberFieldValid(const ruvia::json_value& object, std::string_view field, bool allowString, std::string_view lower, std::string_view upper) {
        return numberFieldValid(object.get<ruvia::json_value>(field), allowString, lower, upper);
    }

    template <typename T>
    static bool modelArray(const std::optional<ruvia::json_value>& raw, std::optional<ruvia::array<T>>& parsed) {
        if (!raw) {
            parsed.reset();
            return true;
        }
        if (!raw->is_array()) {
            return false;
        }
        parsed = raw->template get<ruvia::array<T>>();
        return parsed.has_value();
    }

    static bool enumValueValid(const std::optional<ruvia::string>& value, std::initializer_list<std::string_view> values) {
        return !value || oneOf(value->view(), values);
    }

  public:
    static void validateConfig(const std::optional<ruvia::json_value>& config, const std::string& protocol, bool required) {
        if (!config) {
            if (required) {
                service::common::fail(16004, "config 不能为空", 400);
            }
            return;
        }
        if (!config->is_object()) {
            service::common::fail(16004, "config 必须是对象", 400);
        }
        for (const auto field : {"registers", "areas", "points"}) {
            if (const auto points = config->get<ruvia::json_value>(field); points && points->is_array()) {
                (void)points->for_each_element([&](const ruvia::json_value& point) {
                    if (hasField(point, "scale") || hasField(point, "offset")) {
                        service::common::fail(16004, "原生点位不支持倍率和偏移，请使用派生值表达式", 400);
                    }
                    return true;
                });
            }
        }
        if (hasField(*config, "storageInterval")) {
            service::common::fail(16004, "配置不允许 storageInterval，请使用 storagePolicy", 400);
        }
        if (!enumFieldValid(*config, "storagePolicy", { "report", "change" })) {
            service::common::fail(16004, "配置的 storagePolicy 无效", 400);
        }
        if (required && !hasField(*config, "storagePolicy")) {
            service::common::fail(16004, "配置的 storagePolicy 不能为空", 400);
        }

        if (hasField(*config, "deviceParameters")) service::common::fail(16004, "设备参数请在设备管理中配置", 400);

        const auto parameterConfig = ruvia::from_json<service::common::DeviceParameterConfiguration>(config->view());
        if (!parameterConfig) service::common::fail(16004, "设备参数定义须为数组", 400);
        try {
            if (const auto& definitions = parameterConfig->get<"parameters">(); definitions)
                service::common::validateDeviceParameterDefinitions(*definitions);
        } catch (const std::invalid_argument& error) { service::common::fail(16004, error.what(), 400); }

        // MQTT 属性与消息的跨字段约束在加锁后的合并配置上统一校验。
        if (protocol == "MQTT" && !required) return;
        if (protocol == "MQTT" && config->get<ruvia::json_value>("reports")) {
            try {
                for (const auto& wire : service::common::mqttReportConfigurations(*config)) {
                    const auto single = ruvia::json_value::parse(std::string_view(wire));
                    validateConfig(single, protocol, required);
                }
            } catch (const std::invalid_argument& error) { service::common::fail(16004, error.what(), 400); }
            return;
        }
        if (protocol == "MQTT") {
            const auto typed = ruvia::from_json<MqttConfig>(config->view());
            if (!typed) service::common::fail(16004, "MQTT 配置字段类型无效", 400);
            const auto topicValid = [](std::string_view topic, bool filter = false) {
                if (topic.empty() || topic.size() > 1024 || topic.find('\0') != std::string_view::npos) return false;
                const auto levels = service::utils::splitPayload(topic, "/");
                for (std::size_t i = 0; i < levels.size(); ++i) {
                    if (levels[i].find_first_of("+#") == std::string_view::npos) continue;
                    if (!filter || (levels[i] != "+" && (levels[i] != "#" || i + 1 != levels.size()))) return false;
                }
                return true;
            };
            const auto& topic = typed->get<"topic">();
            if ((required && !topic) || (topic && !topicValid(topic->view(), true)))
                service::common::fail(16004, "MQTT Topic 过滤器无效", 400);
            const auto& commandTopic = typed->get<"commandTopic">();
            const auto& commands = typed->get<"commands">();
            if (commands && (commands->size() > 32 || (commandTopic && !commandTopic->view().empty()) ||
                (typed->get<"commandTemplate">() && !typed->get<"commandTemplate">()->view().empty())))
                service::common::fail(16004, "控制消息最多 32 条，且不能与旧版单条控制配置同时使用", 400);
            if (commandTopic && !commandTopic->view().empty() && !topicValid(commandTopic->view()))
                service::common::fail(16004, "MQTT 指令 Topic 无效", 400);
            const auto& codeField = typed->get<"deviceCodeField">();
            const auto& recordsPath = typed->get<"recordsPath">();
            const auto format = typed->get<"payloadFormat">() ? typed->get<"payloadFormat">()->view() : std::string_view("json");
            const auto identity = typed->get<"identitySource">() ? typed->get<"identitySource">()->view() : (codeField && !codeField->view().empty() ? std::string_view("payload") : std::string_view("bound"));
            if (!oneOf(format, {"json", "text", "binary"}) || !oneOf(identity, {"bound", "payload", "topic"})) service::common::fail(16004, "MQTT 负载格式或设备识别方式无效", 400);
            if (identity == "payload" && (!codeField || codeField->view().empty())) service::common::fail(16004, "负载识别需要设备标识字段", 400);
            if (identity == "bound" && topic && topic->view().find_first_of("+#") != std::string_view::npos) service::common::fail(16004, "通配 Topic 需要从 Topic 或负载识别设备", 400);
            const auto segment = typed->get<"topicDeviceSegment">().value_or(ruvia::int64{1}).value_;
            if (segment < 0 || segment > 1024) service::common::fail(16004, "Topic 设备标识段号无效", 400);
            const auto recordLength = typed->get<"recordLength">().value_or(ruvia::int64{0}).value_;
            if (recordLength < 0 || recordLength > 1048576) service::common::fail(16004, "二进制记录长度无效", 400);
            for (const auto* delimiter : {&typed->get<"delimiter">(), &typed->get<"recordDelimiter">()})
                if (*delimiter && ((*delimiter)->view().empty() || (*delimiter)->view().size() > 8)) service::common::fail(16004, "分隔符长度须为 1 到 8 字节", 400);
            const auto validateSelector = [&](std::string_view selector) {
                if (selector.size() > 256) throw std::invalid_argument("字段路径过长");
                if (format == "json") (void)service::utils::jsonFieldSegments(selector);
                else if (format == "text") { if (service::utils::payloadIndex(selector) > 4096) throw std::invalid_argument("文本列号超出范围"); }
                else {
                    const service::utils::BinaryPayloadField field(selector);
                    if (recordLength && field.offset + field.length > static_cast<std::size_t>(recordLength)) throw std::invalid_argument("二进制字段超出记录长度");
                }
            };
            try {
                if (identity == "payload") validateSelector(codeField->view());
                if (recordsPath && !recordsPath->view().empty()) {
                    if (format != "json" || recordsPath->view().size() > 256) throw std::invalid_argument("记录路径仅用于 JSON");
                    (void)service::utils::jsonFieldSegments(recordsPath->view());
                }
                if (const auto& timeField = typed->get<"timeField">(); timeField && !timeField->view().empty()) validateSelector(timeField->view());
            } catch (const std::invalid_argument& error) { service::common::fail(16004, error.what(), 400); }
            if (const auto& timeFormat = typed->get<"timeFormat">(); timeFormat && !oneOf(timeFormat->view(), {"unix_ms", "unix_s", "iso8601"})) service::common::fail(16004, "时间格式无效", 400);
            const auto qos = typed->get<"qos">().value_or(ruvia::int64{1}).value_;
            if (qos < 0 || qos > 2) service::common::fail(16004, "MQTT QoS 必须是 0、1 或 2", 400);
            const auto& points = typed->get<"points">();
            if ((required && !points) || (points && points->size() > 256)) service::common::fail(16004, "MQTT 点位配置无效", 400);
            std::set<std::string> ids, fields;
            std::map<std::string, bool, std::less<>> templatePoints;
            if (points) for (const auto& point : *points) {
                const auto name = point.get<"name">().view(), field = point.get<"field">().view();
                if (!service::common::isUuidField(point.get<"id">()) || !ids.emplace(point.get<"id">().view()).second ||
                    name.empty() || name.size() > 100 || field.size() > 256 ||
                    (field.empty() && (format != "json" || !typed->get<"reportTemplate">())) ||
                    (!field.empty() && (!fields.emplace(field).second || (codeField && codeField->view() == field))) ||
                    !oneOf(point.get<"dataType">().view(), {"BOOL", "STRING", "DOUBLE"}))
                    service::common::fail(16004, "MQTT 点位名称、标识、JSON 字段或类型无效", 400);
                if (!templatePoints.emplace(std::string(name), point.get<"writable">().value_or(ruvia::bool_value{false}).value_).second)
                    service::common::fail(16004, "MQTT 点位名称重复", 400);
                try { if (!field.empty()) validateSelector(field); }
                catch (const std::invalid_argument& error) { service::common::fail(16004, error.what(), 400); }
                if (const auto& enums = point.get<"enumValues">(); enums) {
                    if (enums->size() > 64) service::common::fail(16004, "最多 64 条枚举映射", 400);
                    std::set<std::string> inputs, outputs;
                    for (const auto& value : *enums) {
                        const auto input = value.get<"input">().view(), output = value.get<"output">().view();
                        const auto converted = service::utils::convertPayloadValue(service::utils::PayloadScalar{std::string(output)}, point.get<"dataType">().view());
                        if (input.size() > 256 || output.size() > 256 || !inputs.emplace(input).second || !converted || !outputs.emplace(service::utils::payloadScalarJson(*converted)).second) service::common::fail(16004, "枚举原值、转换值须唯一且符合点位类型", 400);
                    }
                }
                if (point.get<"unit">() && point.get<"unit">()->view().size() > 32)
                    service::common::fail(16004, "MQTT 点位单位过长", 400);
                if (required && point.get<"writable">().value_or(ruvia::bool_value{false}).value_ &&
                    !commands && (!commandTopic || commandTopic->view().empty())) service::common::fail(16004, "可写 MQTT 点位需要指令 Topic", 400);
            }
            try {
                const auto validateTemplate = [&](std::string_view input, bool report = false, bool legacyValues = true) {
                    service::utils::JsonValueTemplate::Tokens allowed{ "$deviceCode", report ? "$time" : "$values" }, sparse;
                    if (!report) { allowed.insert("$deviceId"); allowed.insert("$commandId"); }
                    if (!report && !legacyValues) allowed.erase("$values");
                    for (const auto& [name, writable] : templatePoints) {
                        const auto token = "$point:" + name;
                        allowed.insert(token);
                        sparse.insert(token);
                    }
                    const service::utils::JsonValueTemplate parsed(input, allowed, sparse, report, 16384, 32, report ? utils::JsonValueTemplate::Tokens{} : utils::JsonValueTemplate::Tokens{"$param:"});
                    for (const auto& [name, writable] : templatePoints) {
                        if (!parsed.references("$point:" + name)) continue;
                        if (!report && !writable) throw std::invalid_argument("指令模板引用不可写点位: " + name);
                    }
                    return parsed;
                };
                if (commands) {
                    if (format != "json") throw std::invalid_argument("多条控制消息仅用于 JSON");
                    std::set<std::string, std::less<>> messageIds, messageNames, writableIds;
                    for (const auto& message : *commands) {
                        const auto name = message.get<"name">().view();
                        if (!common::isUuidField(message.get<"id">()) || !messageIds.emplace(message.get<"id">().view()).second ||
                            name.empty() || name.size() > 100 || !messageNames.emplace(name).second || !topicValid(message.get<"topic">().view()))
                            throw std::invalid_argument("控制消息名称、标识或 Topic 无效或重复");
                        const auto parsed = validateTemplate(message.get<"templateText">().view(), false, false);
                        std::set<std::string, std::less<>> referenced, topicNames, requiredIds;
                        for (const auto level : utils::splitPayload(message.get<"topic">().view(), "/")) {
                            if (level == "{deviceCode}") continue;
                            if (level.starts_with("{point:") && level.ends_with('}')) {
                                const auto pointName = level.substr(7, level.size() - 8);
                                const auto point = templatePoints.find(pointName);
                                if (point == templatePoints.end() || !point->second) throw std::invalid_argument("控制 Topic 引用了未知或不可写属性");
                                topicNames.emplace(pointName);
                            } else if (level.find_first_of("{}") != std::string_view::npos) {
                                throw std::invalid_argument("控制 Topic 占位符须独占一层");
                            }
                        }
                        if (points) for (const auto& point : *points) {
                            if (parsed.references("$point:" + std::string(point.get<"name">().view())) || topicNames.contains(point.get<"name">().view()))
                                referenced.emplace(point.get<"id">().view());
                        }
                        if (referenced.empty()) throw std::invalid_argument("控制消息至少需要引用一个属性");
                        if (const auto& response = message.get<"response">(); response) {
                            if (!topicValid(response->get<"topic">().view()) || response->get<"topic">().view().find("{point:") != std::string_view::npos || !parsed.references("$commandId"))
                                throw std::invalid_argument("设备回执需要固定设备 Topic 及 $commandId");
                            const auto timeout = response->get<"timeoutMs">().value_or(ruvia::int64{10000}).value_;
                            const auto successCode = response->get<"successCode">().value_;
                            if (successCode < -9007199254740991LL || successCode > 9007199254740991LL) throw std::invalid_argument("回执成功码须为 JSON 安全整数");
                            if (timeout < 1000 || timeout > 120000) throw std::invalid_argument("回执超时须为 1000–120000 毫秒");
                            const auto& matchFields = response->get<"matchFields">();
                            if (matchFields.empty() || matchFields.size() > 16) throw std::invalid_argument("回执须有 1–16 个关联字段");
                            bool matchesId = false;
                            std::set<std::string> selectors;
                            const utils::PayloadRecord source(message.get<"templateText">().view(), "json", ",");
                            for (const auto& field : matchFields) {
                                (void)utils::jsonFieldSegments(field.view());
                                if (field.view().empty() || field.view().size() > 256 || !selectors.emplace(field.view()).second)
                                    throw std::invalid_argument("回执关联字段无效或重复");
                                const auto value = source.field(field.view());
                                if (!value) throw std::invalid_argument("回执关联字段须出现在指令模板中");
                                if (const auto* text = std::get_if<std::string>(&*value); text && *text == "$commandId") matchesId = true;
                            }
                            if (!matchesId) throw std::invalid_argument("回执关联字段须包含 $commandId 所在路径");
                            if (response->get<"codeField">().view().empty() || response->get<"codeField">().view().size() > 256)
                                throw std::invalid_argument("回执状态路径无效");
                            (void)utils::jsonFieldSegments(response->get<"codeField">().view());
                        }
                        utils::JsonValueTemplate::Values requiredValues{{"$deviceCode", "\"device\""}, {"$deviceId", "\"device-id\""}, {"$commandId", "1"}};
                        for (const auto& token : parsed.references()) if (token.starts_with("$param:")) requiredValues.emplace(token, "null");
                        for (const auto& id : message.get<"requiredPointIds">()) {
                            if (!referenced.contains(id.view()) || !requiredIds.emplace(id.view()).second)
                                throw std::invalid_argument("必填控制点必须属于当前消息且不能重复");
                            if (points) for (const auto& point : *points) {
                                if (point.get<"id">().view() == id.view()) requiredValues.emplace("$point:" + std::string(point.get<"name">().view()), "null");
                            }
                        }
                        if (points) for (const auto& point : *points) {
                            if (topicNames.contains(point.get<"name">().view()) && !requiredIds.contains(point.get<"id">().view()))
                                throw std::invalid_argument("控制 Topic 中的属性必须设为必填");
                        }
                        // 数组不能省略下标；对象字段可以省略，且不补造值。
                        (void)parsed.render(requiredValues);
                        writableIds.insert(referenced.begin(), referenced.end());
                    }
                    if (points) for (const auto& point : *points) {
                        if (point.get<"writable">().value_or(ruvia::bool_value{false}).value_ != writableIds.contains(point.get<"id">().view()))
                            throw std::invalid_argument("属性可写状态与控制消息引用不一致");
                    }
                }
                if (const auto& value = typed->get<"commandTemplate">(); value && !value->view().empty()) {
                    if (format != "json") throw std::invalid_argument("指令模板仅用于 JSON");
                    validateTemplate(value->view());
                }
                if (const auto& value = typed->get<"reportTemplate">(); value) {
                    if (format != "json") throw std::invalid_argument("上报模板仅用于 JSON");
                    validateTemplate(value->view(), true);
                }
            } catch (const std::invalid_argument& error) { service::common::fail(16004, error.what(), 400); }
            return;
        }
        if (protocol == "SL651") {
            if (!enumFieldValid(*config, "responseMode", { "M1", "M2", "M3", "M4" }) ||
                (hasField(*config, "funcs") &&
                 !config->get<ruvia::json_value>("funcs")->is_array()) ||
                (required && (!hasField(*config, "responseMode") || !hasField(*config, "funcs")))) {
                service::common::fail(16004, "SL651 配置无效", 400);
            }
            std::optional<ruvia::array<Sl651Func>> funcs;
            if (!modelArray(config->get<ruvia::json_value>("funcs"), funcs)) {
                service::common::fail(16004, "SL651 配置无效", 400);
            }
            const auto validElement = [](const Sl651Element& element) {
                if (!enumValueValid(element.get<"positionMode">(), { "GUIDE", "OFFSET" })) {
                    return false;
                }
                const auto position = element.get<"positionMode">() ? std::string(element.get<"positionMode">()->view()) : std::string("GUIDE");
                if (position == "OFFSET") {
                    const auto offset = element.is_present<"byteOffset">()
                        ? integerFieldValue(element.get<"byteOffset">(), false, 7, 0, 8388607)
                        : std::optional<std::int64_t>{ 0 };
                    const auto length = integerFieldValue(element.get<"length">(), false, 7, 1, 8388608);
                    return offset && length && *offset + *length <= 8388608 &&
                        integerFieldValid(element.get<"digits">(), false, 1, 0, 7, true) &&
                        enumValueValid(element.get<"encode">(), { "BCD", "HEX", "DICT", "JPEG", "TIME_YYMMDDHHMMSS" }) &&
                        element.is_present<"encode">();
                }
                if (position != "GUIDE") {
                    return false;
                }
                const auto guide = element.get<"guideHex">() ? std::optional<std::string>(element.get<"guideHex">()->view()) : std::nullopt;
                const auto encoding = element.get<"encode">() ? std::optional<std::string>(element.get<"encode">()->view()) : std::nullopt;
                if (!guide || (guide->size() != 4 && guide->size() != 6) ||
                    !std::all_of(guide->begin(), guide->end(), [](unsigned char ch) {
                        return std::isxdigit(ch) != 0;
                    }) ||
                    !encoding || !oneOf(*encoding, { "BCD", "HEX", "DICT", "JPEG", "TIME_YYMMDDHHMMSS" }) || !integerFieldValid(element.get<"length">(), false, 7, 0, 8388608, true) || !integerFieldValid(element.get<"digits">(), false, 1, 0, 7, true)) {
                    return false;
                }
                const auto lead = std::stoi(guide->substr(0, 2), nullptr, 16);
                if ((lead == 0xFF) != (guide->size() == 6)) {
                    return false;
                }
                const auto definition = std::stoi(guide->substr(guide->size() - 2), nullptr, 16);
                const auto length = std::stoi(*scalarText(*element.get<"length">(), false));
                const auto digits = std::stoi(*scalarText(*element.get<"digits">(), false));
                if (lead < 0xF0 || lead == 0xFF) {
                    return length > 0 && length == (definition >> 3) &&
                        (*encoding != "BCD" || digits == (definition & 7));
                }
                if (definition != lead) {
                    return false;
                }
                if (lead == 0xF0) {
                    return length == 5;
                }
                if (lead == 0xF1) {
                    return length == 6;
                }
                return lead == 0xF2 || lead == 0xF3 || length > 0;
            };
            if (funcs) {
                for (const auto& function : *funcs) {
                    const auto code = function.get<"funcCode">() ? std::optional<std::string>(function.get<"funcCode">()->view()) : std::nullopt;
                    if (!code || code->size() != 2 || !std::all_of(code->begin(), code->end(), [](unsigned char ch) {
                            return std::isxdigit(ch) != 0;
                        }) ||
                        !enumValueValid(function.get<"dir">(), { "UP", "DOWN" })) {
                        service::common::fail(16004, "SL651 引导符、长度或小数位不符合数据定义", 400);
                    }
                    for (const auto* elements : { &function.get<"elements">(), &function.get<"responseElements">() }) {
                        if (!*elements) {
                            continue;
                        }
                        for (const auto& element : **elements) {
                            if (!validElement(element)) {
                                service::common::fail(16004, "SL651 引导符、长度或小数位不符合数据定义", 400);
                            }
                        }
                    }
                }
            }
            return;
        }

        if (protocol == "MC" || protocol == "FINS" || protocol == "DLT645") {
            const auto connectionValue = config->get<ruvia::json_value>("connection");
            const auto pointsValue = config->get<ruvia::json_value>("points");
            const auto connection = connectionValue ? connectionValue->get<IndustrialConnection>() : std::optional<IndustrialConnection>{};
            std::optional<ruvia::array<IndustrialPoint>> points;
            if (!connectionValue || !connectionValue->is_object() || !connection ||
                !modelArray(pointsValue, points) || !points ||
                !integerFieldValid(*config, "readInterval", true, 5, 1, 3600) ||
                !integerFieldValid(*config, "commandFastReadDuration", true, 5, 0, 3600) ||
                !integerFieldValid(*config, "commandFastReadInterval", true, 5, 1, 3600)) {
                service::common::fail(16004, "协议连接、点位或采集周期配置无效", 400);
            }
            if (protocol == "MC") {
                if (!enumValueValid(connection->get<"frame">(), { "3E", "4E" }) ||
                    !integerFieldValid(connection->get<"network">(), false, 3, 0, 255) ||
                    !integerFieldValid(connection->get<"station">(), false, 3, 0, 255) ||
                    !integerFieldValid(connection->get<"moduleIo">(), false, 5, 0, 65535) ||
                    !integerFieldValid(connection->get<"multidrop">(), false, 3, 0, 255) ||
                    !integerFieldValid(connection->get<"monitoringTimer">(), false, 5, 1, 65535)) {
                    service::common::fail(16004, "MC/SLMP 连接参数无效", 400);
                }
            } else if (protocol == "FINS") {
                const std::pair<std::string_view, std::int64_t> routes[] = {
                    { "destinationNetwork", 127 }, { "sourceNetwork", 127 },
                    { "destinationNode", 254 }, { "sourceNode", 254 },
                    { "destinationUnit", 255 }, { "sourceUnit", 255 },
                };
                const auto validRoute = [&](std::string_view field, std::int64_t maximum) {
                    if (field == "destinationNetwork") return integerFieldValid(connection->get<"destinationNetwork">(), false, 3, 0, maximum);
                    if (field == "sourceNetwork") return integerFieldValid(connection->get<"sourceNetwork">(), false, 3, 0, maximum);
                    if (field == "destinationNode") return integerFieldValid(connection->get<"destinationNode">(), false, 3, 0, maximum);
                    if (field == "sourceNode") return integerFieldValid(connection->get<"sourceNode">(), false, 3, 0, maximum);
                    if (field == "destinationUnit") return integerFieldValid(connection->get<"destinationUnit">(), false, 3, 0, maximum);
                    return integerFieldValid(connection->get<"sourceUnit">(), false, 3, 0, maximum);
                };
                for (const auto& [field, maximum] : routes) {
                    if (!validRoute(field, maximum)) {
                        service::common::fail(16004, "FINS 路由参数无效", 400);
                    }
                }
            } else {
                if (!enumValueValid(connection->get<"version">(), { "1997", "2007" }) ||
                    !integerFieldValid(connection->get<"wakeupBytes">(), false, 1, 0, 4)) {
                    service::common::fail(16004, "DL/T645 版本或唤醒字节数无效", 400);
                }
                for (const auto* field : { &connection->get<"writePassword">(), &connection->get<"operatorCode">() }) {
                    if (!*field) {
                        continue;
                    }
                    const auto text = std::string((*field)->view());
                    if (!text.empty() && (text.size() != 8 || !std::all_of(text.begin(), text.end(), [](unsigned char c) {
                                                         return std::isxdigit(c) != 0;
                                                     }))) {
                        service::common::fail(16004, "DL/T645 写入认证字段必须为空或 8 位十六进制", 400);
                    }
                }
            }
            std::set<std::string> ids;
            if (points->size() > 256) {
                service::common::fail(16004, "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证", 400);
            }
            for (const auto& point : *points) {
                const auto id = point.get<"id">() ? std::optional<std::string>(point.get<"id">()->view()) : std::nullopt;
                const auto name = point.get<"name">() ? std::optional<std::string>(point.get<"name">()->view()) : std::nullopt;
                const auto type = point.get<"dataType">() ? std::optional<std::string>(point.get<"dataType">()->view()) : std::nullopt;
                const auto unit = point.get<"unit">() ? std::optional<std::string>(point.get<"unit">()->view()) : std::nullopt;
                if (!id || !service::common::isUuid(*id) || !ids.insert(*id).second || !name || name->empty() || name->size() > 100 || !type ||
                    (unit && unit->size() > 32) ||
                    (point.is_present<"writable">() && !point.get<"writable">())) {
                    service::common::fail(16004, "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证", 400);
                }
                if (protocol == "DLT645") {
                    const auto identifier = point.get<"identifier">() ? std::optional<std::string>(point.get<"identifier">()->view()) : std::nullopt;
                    const auto version = connection->get<"version">() ? std::string(connection->get<"version">()->view()) : std::string("2007");
                    if (!identifier || identifier->size() != (version == "1997" ? 4 : 8) ||
                        !std::all_of(identifier->begin(), identifier->end(), [](unsigned char c) {
                            return std::isxdigit(c) != 0;
                        }) ||
                        !oneOf(*type, { "BCD", "BCD_SIGNED", "HEX" }) || !integerFieldValid(point.get<"length">(), false, 3, 1, *type == "HEX" ? 200 : 8, true) || !integerFieldValid(point.get<"digits">(), false, 1, 0, 8)) {
                        service::common::fail(16004, "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证", 400);
                    }
                    if (point.get<"writable">() && static_cast<bool>(*point.get<"writable">())) {
                        const auto password = connection->get<"writePassword">() ? std::string(connection->get<"writePassword">()->view()) : std::string{};
                        const auto operatorCode = connection->get<"operatorCode">() ? std::string(connection->get<"operatorCode">()->view()) : std::string{};
                        if (password.size() != 8 || (version == "2007" && operatorCode.size() != 8)) {
                            service::common::fail(16004, "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证", 400);
                        }
                        const auto length = integerFieldValue(point.get<"length">(), false, 3, 1, 200);
                        if (!length || *length > (version == "2007" ? 38 : 44)) {
                            service::common::fail(16004, "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证", 400);
                        }
                    }
                    continue;
                }
                const auto area = point.get<"area">() ? std::optional<std::string>(point.get<"area">()->view()) : std::nullopt;
                const auto bits = *type == "BOOL";
                if (!area || !oneOf(*type, { "BOOL", "INT16", "UINT16", "INT32", "UINT32", "FLOAT32", "INT64", "UINT64", "DOUBLE" }) ||
                    !enumValueValid(point.get<"byteOrder">(), { "BIG_ENDIAN", "LITTLE_ENDIAN", "BIG_ENDIAN_BYTE_SWAP", "LITTLE_ENDIAN_BYTE_SWAP" }) ||
                    !signedIntegerFieldValid(point.get<"decimals">(), false, 2, -1, 8)) {
                    service::common::fail(16004, "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证", 400);
                }
                const auto address = integerFieldValue(point.get<"address">(), false, 8, 0, protocol == "MC" ? 16777215 : 65535);
                const auto width = bits ? 1 : (*type == "INT16" || *type == "UINT16") ? 1
                    : (*type == "INT64" || *type == "UINT64" || *type == "DOUBLE")    ? 4
                                                                                      : 2;
                if (!address) {
                    service::common::fail(16004, "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证", 400);
                }
                bool ok = false;
                if (protocol == "MC") {
                    const bool bitArea = oneOf(*area, { "M", "X", "Y", "B", "L", "F", "V", "S", "TS", "CS" });
                    ok = (bitArea || oneOf(*area, { "D", "W", "R", "ZR", "TN", "CN" })) && (!bits || bitArea) &&
                        *address + width * (bitArea && !bits ? 16 : 1) - 1 <= 16777215;
                } else {
                    ok = oneOf(*area, { "D", "CIO", "W", "H", "A" }) && *address + (bits ? 0 : width - 1) <= 65535 &&
                        integerFieldValid(point.get<"bit">(), false, 2, 0, bits ? 15 : 0);
                }
                if (!ok) {
                    service::common::fail(16004, "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证", 400);
                }
            }
            return;
        }

        if (protocol == "S7") {
            const auto connectionValue = config->get<ruvia::json_value>("connection");
            const auto areasValue = config->get<ruvia::json_value>("areas");
            const auto connection = connectionValue && connectionValue->is_object() ? connectionValue->get<S7Connection>() : std::optional<S7Connection>{};
            std::optional<ruvia::array<S7Area>> areas;
            if (!enumFieldValid(*config, "plcModel", { "S7-200", "S7-300", "S7-400", "S7-1200", "S7-1500" }) ||
                (connectionValue && (!connectionValue->is_object() || !connection)) ||
                (areasValue && !modelArray(areasValue, areas)) ||
                (required && (!hasField(*config, "plcModel") || !connection || !areas))) {
                service::common::fail(16004, "S7 配置无效", 400);
            }
            if (connection &&
                (!enumValueValid(connection->get<"probeMode">(), { "STANDARD", "COMPATIBLE", "AUTO" }) ||
                 !numberFieldValid(connection->get<"handshakeTimeout">(), false, "1000", "30000") ||
                 !numberFieldValid(connection->get<"directProbeTimeout">(), false, "1000", "30000"))) {
                service::common::fail(16004, "S7 配置无效", 400);
            }
            if (hasField(*config, "pollInterval")) {
                service::common::fail(16004, "S7 配置不允许 pollInterval，请使用 readInterval", 400);
            }
            if (!integerFieldValid(*config, "readInterval", true, 5, 1, 3600)) {
                service::common::fail(16004, "S7 配置的 readInterval 无效", 400);
            }
            if (areas) {
                for (const auto& area : *areas) {
                    const auto id = area.get<"id">() ? std::optional<std::string>(area.get<"id">()->view()) : std::nullopt;
                    const auto name = area.get<"name">() ? std::optional<std::string>(area.get<"name">()->view()) : std::nullopt;
                    const auto areaType = area.get<"area">() ? std::optional<std::string>(area.get<"area">()->view()) : std::nullopt;
                    if (!id || id->empty() || !name || name->empty() || !areaType ||
                        !oneOf(*areaType, { "DB", "V", "MK", "PE", "PA", "CT", "TM" })) {
                        service::common::fail(16004, "S7 寄存器配置无效", 400);
                    }
                    if (!enumValueValid(area.get<"dataType">(), { "BOOL", "INT8", "UINT8", "INT16", "UINT16", "INT32", "UINT32", "FLOAT", "LREAL", "STRING" }) ||
                        !integerFieldValid(area.get<"start">(), false, 10, 0, 2147483647, true) ||
                        !integerFieldValid(area.get<"size">(), false, 5, 1, 65535, true) ||
                        !integerFieldValid(area.get<"startBit">(), false, 1, 0, 7) ||
                        !signedIntegerFieldValid(area.get<"decimals">(), false, 2, -1, 8) ||
                        (area.is_present<"writable">() && !area.get<"writable">())) {
                        service::common::fail(16004, "S7 寄存器配置无效", 400);
                    }
                    if (*areaType == "DB" && !integerFieldValid(area.get<"dbNumber">(), false, 5, 1, 65535, true)) {
                        service::common::fail(16004, "S7 寄存器配置无效", 400);
                    }
                }
            }
            return;
        }

        if (protocol != "Modbus") {
            return;
        }
        const auto packetValue = config->get<ruvia::json_value>("packet");
        const auto registersValue = config->get<ruvia::json_value>("registers");
        const auto packet = packetValue && packetValue->is_object() ? packetValue->get<ModbusPacket>() : std::optional<ModbusPacket>{};
        std::optional<ruvia::array<ModbusRegister>> registers;
        if (!enumFieldValid(*config, "byteOrder", { "BIG_ENDIAN", "LITTLE_ENDIAN", "BIG_ENDIAN_BYTE_SWAP", "LITTLE_ENDIAN_BYTE_SWAP" }) ||
            (required && !hasField(*config, "byteOrder"))) {
            service::common::fail(16004, "Modbus 配置的 byteOrder 无效", 400);
        }
        if ((registersValue && !modelArray(registersValue, registers)) || (required && !registers)) {
            service::common::fail(16004, "Modbus 配置的 registers 必须是数组", 400);
        }
        if (packetValue && (!packetValue->is_object() || !packet)) {
            service::common::fail(16004, "Modbus 配置的 packet 必须是对象", 400);
        }
        if (!integerFieldValid(*config, "readInterval", true, 5, 1, 3600)) {
            service::common::fail(16004, "Modbus 配置的 readInterval 无效", 400);
        }
        if (packet && !integerFieldValid(packet->get<"mergeGap">(), true, 5, 0, 2000)) {
            service::common::fail(16004, "Modbus 配置的 packet.mergeGap 无效", 400);
        }
        if (packet && !integerFieldValid(packet->get<"maxQuantity">(), true, 5, 1, 125)) {
            service::common::fail(16004, "Modbus 配置的 packet.maxQuantity 无效", 400);
        }
        if (hasField(*config, "pollInterval")) {
            service::common::fail(16004, "Modbus 配置不允许 pollInterval，请使用 readInterval", 400);
        }
        if (!registers) {
            return;
        }
        for (const auto& entry : *registers) {
            const auto id = entry.get<"id">() ? std::optional<std::string>(entry.get<"id">()->view()) : std::nullopt;
            const auto name = entry.get<"name">() ? std::optional<std::string>(entry.get<"name">()->view()) : std::nullopt;
            const auto registerType = entry.get<"registerType">() ? std::optional<std::string>(entry.get<"registerType">()->view()) : std::nullopt;
            const auto dataType = entry.get<"dataType">() ? std::optional<std::string>(entry.get<"dataType">()->view()) : std::nullopt;
            if (!id || id->empty() || !name || name->empty() || !registerType || !dataType ||
                !oneOf(*registerType, { "COIL", "DISCRETE_INPUT", "HOLDING_REGISTER", "INPUT_REGISTER" }) ||
                !oneOf(*dataType, { "BOOL", "INT16", "UINT16", "INT32", "UINT32", "FLOAT32", "INT64", "UINT64", "DOUBLE" }) ||
                !integerFieldValid(entry.get<"address">(), false, 5, 0, 65535, true) ||
                !integerFieldValid(entry.get<"quantity">(), false, 1, 1, 4, true)) {
                service::common::fail(16004, "Modbus 寄存器配置无效", 400);
            }
            if (!enumValueValid(entry.get<"byteOrder">(), { "BIG_ENDIAN", "LITTLE_ENDIAN", "BIG_ENDIAN_BYTE_SWAP", "LITTLE_ENDIAN_BYTE_SWAP" })) {
                service::common::fail(16004, "Modbus 寄存器 byteOrder 无效", 400);
            }
            if (!signedIntegerFieldValid(entry.get<"decimals">(), true, 2, -1, 8)) {
                service::common::fail(16004, "Modbus 寄存器 decimals 无效", 400);
            }
            if (entry.is_present<"writable">() && !entry.get<"writable">()) {
                service::common::fail(16004, "Modbus 寄存器 writable 必须是布尔值", 400);
            }
        }
    }
};


class ProtocolService {
    static void validateDerivedConfig(const ruvia::json_value& config) {
        try { (void)service::common::orderDerivedPoints(config); }
        catch (const std::invalid_argument& error) { service::common::fail(16004, error.what(), 400); }
    }
  public:
    static ExpressionTestResult testExpression(const ExpressionTestBody& body) {
        try {
            std::map<std::string, double> inputs;
            for (const auto& input : body.get<"inputs">()) {
                const std::string alias(input.get<"alias">().view());
                const double value = input.get<"value">().value_;
                const utils::Expression variable(alias);
                if (variable.variables().size() != 1 || variable.variables().front() != alias ||
                    !std::isfinite(value) || !inputs.emplace(alias, value).second) {
                    throw std::invalid_argument("变量名无效、重复或测试值不是有限数值");
                }
            }
            const auto compile = [&](std::string_view text) {
                utils::Expression expression(text);
                for (const auto& alias : expression.variables()) {
                    if (!inputs.contains(alias)) throw std::invalid_argument("未提供变量测试值：" + alias);
                }
                return expression;
            };
            const auto expression = compile(body.get<"expression">().view());
            std::vector<utils::Expression> conditions;
            for (const auto& rule : body.get<"unitRules">()) conditions.push_back(compile(rule.get<"condition">().view()));
            const auto resolve = [&](std::string_view alias) { return inputs.at(std::string(alias)); };
            const auto value = expression.evaluate(resolve);
            std::string unit(body.get<"unit">().view());
            std::int64_t matched{};
            for (std::size_t i = 0; i < conditions.size(); ++i) {
                if (conditions[i].evaluate(resolve) != 0) {
                    unit = body.get<"unitRules">()[i].get<"unit">().view();
                    matched = static_cast<std::int64_t>(i + 1);
                    break;
                }
            }
            ExpressionTestResult result;
            result.set<"value">(value).set<"unit">(unit).set<"matchedRule">(matched);
            return result;
        } catch (const std::invalid_argument& error) {
            common::fail(16004, error.what(), 400);
        } catch (const std::domain_error& error) {
            common::fail(16004, error.what(), 400);
        }
    }

    static MqttPreviewResult previewMqtt(const MqttPreviewBody& body) {
        try {
            const auto& rawConfig = body.get<"config">();
            const auto parsedConfig = ruvia::json_value::parse(rawConfig.view());
            ProtocolConfigurationRules::validateConfig(parsedConfig, "MQTT", true);
            validateDerivedConfig(rawConfig);
            const auto reports = service::common::mqttReportConfigurations(rawConfig);
            const auto requestedTopic = body.get<"topic">().view();
            const auto& requestedCode = body.get<"deviceCode">();
            const auto selected = std::find_if(reports.begin(), reports.end(), [&](const std::string& wire) {
                const auto report = ruvia::json_value::parse(std::string_view(wire));
                const auto topic = report ? report->get<ruvia::string>("topic") : std::optional<ruvia::string>{};
                return topic && service::common::mqttReportTopicMatches(std::string(topic->view()), requestedTopic, requestedCode ? requestedCode->view() : std::string_view{});
            });
            if (selected == reports.end()) throw std::invalid_argument("示例 Topic 未匹配上报消息");
            const auto decodedConfig = ruvia::from_json<MqttConfig>(*selected);
            if (!decodedConfig) throw std::invalid_argument("MQTT 配置字段类型无效");
            const auto& config = *decodedConfig;
            const auto topic = body.get<"topic">().view();
            if (topic.empty() || topic.find_first_of("+#") != std::string_view::npos || topic.find('\0') != std::string_view::npos)
                throw std::invalid_argument("示例 Topic 须为不含通配符的具体 Topic");
            const auto option = [](const auto& value, std::string fallback) { return value ? std::string(value->view()) : fallback; };
            const auto format = option(config.get<"payloadFormat">(), "json");
            const auto codeField = option(config.get<"deviceCodeField">(), "");
            const auto identity = option(config.get<"identitySource">(), codeField.empty() ? "bound" : "payload");
            const auto deviceCode = option(body.get<"deviceCode">(), "");
            auto filter = std::string(config.get<"topic">()->view());
            constexpr std::string_view marker = "{deviceCode}";
            for (auto i = filter.find(marker); i != std::string::npos; i = filter.find(marker, i + deviceCode.size())) filter.replace(i, marker.size(), deviceCode);
            const auto levels = utils::splitPayload(body.get<"topic">().view(), "/"), pattern = utils::splitPayload(filter, "/");
            bool matched = !(body.get<"topic">().view().starts_with('$') && !filter.starts_with('$'));
            std::size_t index = 0;
            for (; matched && index < pattern.size(); ++index) {
                if (pattern[index] == "#") break;
                matched = index < levels.size() && (pattern[index] == "+" || pattern[index] == levels[index]);
            }
            if (!matched || (index == pattern.size() && index != levels.size())) throw std::invalid_argument("示例 Topic 未匹配订阅配置");
            auto payload = std::string(body.get<"payload">().view());
            if (format == "binary") {
                std::string hex;
                for (const unsigned char ch : payload) if (!std::isspace(ch)) hex += static_cast<char>(ch);
                if (hex.size() % 2) throw std::invalid_argument("二进制示例须为完整的 HEX 字节");
                payload.clear();
                for (std::size_t i = 0; i < hex.size(); i += 2) {
                    unsigned value = 0; const auto parsed = std::from_chars(hex.data() + i, hex.data() + i + 2, value, 16);
                    if (parsed.ec != std::errc{} || parsed.ptr != hex.data() + i + 2) throw std::invalid_argument("二进制示例 HEX 无效");
                    payload += static_cast<char>(value);
                }
            }
            if (payload.size() > 1048576) throw std::invalid_argument("示例负载超过 1 MiB");
            const auto records = utils::payloadRecords(payload, format, option(config.get<"recordsPath">(), ""), option(config.get<"recordDelimiter">(), "\n"), static_cast<std::size_t>(config.get<"recordLength">().value_or(ruvia::int64{0}).value_));
            MqttPreviewResult result;
            ruvia::array<MqttPreviewRecord> output;
            const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            for (const auto& input : records) {
                utils::PayloadRecord record(input, format, option(config.get<"delimiter">(), ","));
                MqttPreviewRecord row; ruvia::array<ruvia::string> errors; ruvia::array<MqttPreviewPoint> points;
                auto code = deviceCode;
                if (identity == "payload") {
                    const auto value = record.field(codeField); code = value ? utils::payloadScalarText(*value) : "";
                } else if (identity == "topic") {
                    const auto segment = static_cast<std::size_t>(config.get<"topicDeviceSegment">().value_or(ruvia::int64{1}).value_);
                    code = segment < levels.size() ? std::string(levels[segment]) : "";
                }
                if (code.empty()) errors.push_back(ruvia::string("未提取到设备编码"));
                else if (!deviceCode.empty() && code != deviceCode) errors.push_back(ruvia::string("设备编码与测试设备不匹配，实际接入将跳过此记录"));
                auto occurredAt = now;
                const auto timeField = option(config.get<"timeField">(), "");
                if (!timeField.empty()) if (const auto time = record.field(timeField)) {
                    const auto timestamp = utils::payloadTimestamp(*time, option(config.get<"timeFormat">(), "unix_ms"), option(body.get<"timezone">(), "+08:00"));
                    if (timestamp) occurredAt = *timestamp; else errors.push_back(ruvia::string("设备时间无效，实际接入将跳过此记录"));
                }
                for (const auto& point : *config.get<"points">()) {
                    if (point.get<"field">().view().empty()) continue;
                    auto inputValue = record.field(point.get<"field">().view());
                    if (inputValue) if (const auto& enums = point.get<"enumValues">(); enums && !enums->empty()) {
                        const auto key = utils::payloadScalarText(*inputValue); bool found = false;
                        for (const auto& value : *enums) if (value.get<"input">().view() == key) { inputValue = utils::PayloadScalar{std::string(value.get<"output">().view())}; found = true; break; }
                        if (!found) inputValue.reset();
                    }
                    const auto value = inputValue ? utils::convertPayloadValue(*inputValue, point.get<"dataType">().view()) : std::nullopt;
                    if (!value) { errors.push_back(ruvia::string(std::string(point.get<"name">().view()) + "：字段缺失或类型转换失败")); continue; }
                    MqttPreviewPoint preview;
                    preview.set<"id">(point.get<"id">().view()).set<"name">(point.get<"name">().view()).set<"value">(utils::payloadScalarText(*value)).set<"unit">(option(point.get<"unit">(), ""));
                    points.push_back(std::move(preview));
                }
                row.set<"deviceCode">(code).set<"time">(common::utcTimestampFromMilliseconds(occurredAt)).set<"points">(std::move(points)).set<"errors">(std::move(errors));
                output.push_back(std::move(row));
            }
            result.set<"records">(std::move(output)); return result;
        } catch (const std::invalid_argument& error) { common::fail(16004, error.what(), 400); }
    }

    static ProtocolService& instance() {
        static thread_local ProtocolService service;
        return service;
    }

    template <typename Context>
    ruvia::task<std::string> list(Context& c, std::int64_t page, std::int64_t pageSize, std::optional<std::string> protocol) {
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 1000);
        ruvia::db_find_options countOptions;
        countOptions.where_ = ProtocolConfigEntity::column<"deleted_at">().is_null();
        if (protocol && !protocol->empty()) {
            countOptions.where_ = std::move(countOptions.where_) &&
                ProtocolConfigEntity::column<"protocol">() == *protocol;
        }
        const auto total = co_await c.db().template get_repository<ProtocolConfigEntity>().count(countOptions);

        auto query = protocolSelect(c.pool());
        if (protocol && !protocol->empty()) {
            query.and_where(query.binary(query.column("protocol"), ruvia::db_binary_operator::equal, query.value(*protocol)));
        }
        query.order_by(query.column("id"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>((page - 1) * pageSize));
        const auto rows = co_await c.db().query(query);

        std::string result = "{\"list\":[";
        bool first = true;
        for (const auto& row : rows) {
            if (!first) {
                result.push_back(',');
            }
            first = false;
            result += itemJson(row);
        }
        result += "],\"total\":" + std::to_string(total) + ",\"page\":" + std::to_string(page) +
            ",\"pageSize\":" + std::to_string(pageSize) + ",\"totalPages\":" +
            std::to_string(total == 0 ? 0 : (total + pageSize - 1) / pageSize) + "}";
        co_return result;
    }

    template <typename Context>
    ruvia::task<std::string> detail(Context& c, std::string_view id) {
        co_return co_await detailData(c, id);
    }

    template <typename Context>
    ruvia::task<std::string> options(Context& c, const std::string& protocol, std::int64_t page, std::int64_t pageSize) {
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 1000);
        ruvia::db_find_options options;
        options.where_ = ProtocolConfigEntity::column<"deleted_at">().is_null() &&
            ProtocolConfigEntity::column<"protocol">() == protocol;
        const auto total = co_await c.db().template get_repository<ProtocolConfigEntity>().count(options);
        options.where_ = std::move(options.where_) && ProtocolConfigEntity::column<"enabled">() == true;
        options.order_ = { { "name" } };
        options.take_ = static_cast<std::uint64_t>(pageSize);
        options.skip_ = static_cast<std::uint64_t>((page - 1) * pageSize);
        const auto rows = co_await c.db().template get_repository<ProtocolConfigEntity>().find(options);
        std::string result = "{\"list\":[";
        bool first = true;
        for (const auto& row : rows) {
            if (!first) {
                result.push_back(',');
            }
            first = false;
            result += "{\"id\":" + service::utils::jsonQuoted(row.template get<"id">()) +
                ",\"name\":" + service::utils::jsonQuoted(row.template get<"name">());
            const auto config = ruvia::from_json<service::common::DeviceParameterConfiguration>(row.template get<"config">());
            result += ",\"parameters\":";
            if (config && config->get<"parameters">()) {
                const auto json = ruvia::to_json(*config->get<"parameters">());
                result.append(json.data(), json.size());
            } else result += "[]";
            result += "}";
        }
        result += "],\"total\":" + std::to_string(total) + ",\"page\":" + std::to_string(page) +
            ",\"pageSize\":" + std::to_string(pageSize) + "}";
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> create(Context& c, const CreateProtocolBody& body) {
        const std::string protocol(body.get<"protocol">().view());
        const std::string name(body.get<"name">().view());
        ProtocolConfigurationRules::validateProtocol(protocol);
        ProtocolConfigurationRules::validateName(name);
        if (body.is_null<"config">()) service::common::fail(16004, "config 必须是对象", 400);
        ProtocolConfigurationRules::validateConfig(body.get<"config">(), protocol, true);
        validateDerivedConfig(*body.get<"config">());
        co_await ensureNameAvailable(c, name, std::nullopt);
        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        const auto& config = body.get<"config">();
        const auto& remark = body.get<"remark">();
        const bool enabled = body.get<"enabled">().value_or(ruvia::bool_value{true}).value_;
        auto transaction = co_await c.db().begin_transaction();
        ProtocolConfigEntity configuration(c.pool());
        configuration.set<"id">(id);
        configuration.set<"protocol">(protocol);
        configuration.set<"name">(name);
        configuration.set<"enabled">(enabled);
        configuration.set<"config">(config->view());
        configuration.set<"created_by">(c.userId);
        if (remark && !remark->view().empty()) {
            configuration.set<"remark">(remark->view());
        } else {
            configuration.set_null<"remark">();
        }
        (void)co_await transaction.template get_repository<ProtocolConfigEntity>().insert(configuration);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "protocol", "created", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> update(Context& c, std::string_view id, const UpdateProtocolBody& body) {
        ruvia::db_find_options options;
        options.where_ = service::common::database::activeId<ProtocolConfigEntity>(id);
        const auto existing = co_await c.db().template get_repository<ProtocolConfigEntity>().find_one(options);
        if (!existing) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        co_await requireOwner(c, existing->template get<"created_by">());
        const std::string protocol(existing->template get<"protocol">());
        if (const auto& requested = body.get<"protocol">()) {
            if (requested->view().empty()) {
                service::common::fail(16003, "protocol 不能为空", 400);
            }
            if (requested->view() != protocol) {
                service::common::fail(16006, "协议类型不可修改", 409);
            }
        }
        if (const auto& name = body.get<"name">()) {
            ProtocolConfigurationRules::validateName(name->view());
            co_await ensureNameAvailable(c, std::string(name->view()), std::string(id));
        }
        auto transaction = co_await c.db().begin_transaction();
        options.lock_ = ruvia::db_lock_options{};
        const auto locked = co_await transaction.template get_repository<ProtocolConfigEntity>().find_one(options);
        if (!locked) service::common::fail(16001, "协议配置不存在", 404);
        if (body.is_null<"config">()) service::common::fail(16004, "config 必须是对象", 400);
        ProtocolConfigurationRules::validateConfig(body.get<"config">(), protocol, false);
        if (body.get<"config">()) {
            const auto previous = ruvia::json_value::parse(locked->template get<"config">());
            std::map<std::string, std::string> fields;
            if (!previous) throw std::runtime_error("invalid stored protocol configuration");
            (void)previous->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
                fields[std::string(key)] = std::string(value.view());
                return true;
            });
            (void)body.get<"config">()->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
                fields[std::string(key)] = std::string(value.view());
                return true;
            });
            std::string merged = "{";
            for (const auto& [key, value] : fields) {
                if (merged.size() > 1) merged += ',';
                merged += service::utils::jsonQuoted(key) + ':' + value;
            }
            merged += '}';
            const auto mergedConfig = ruvia::json_value::parse(merged);
            if (protocol == "MQTT") ProtocolConfigurationRules::validateConfig(mergedConfig, protocol, true);
            validateDerivedConfig(*mergedConfig);
            if (!service::common::orderDerivedPoints(*mergedConfig).empty()) co_await requireDerivedSupport(transaction, c.pool(), id);
        }
        const auto& name = body.get<"name">();
        const auto& remark = body.get<"remark">();
        const auto& config = body.get<"config">();
        const auto& enabled = body.get<"enabled">();
        ruvia::db_expressions expressions(c.pool());
        std::vector<ruvia::db_assignment> changes{ { "updated_at", expressions.call("now") } };
        if (name) {
            changes.push_back({ "name", expressions.value(name->view()) });
        }
        if (enabled) {
            changes.push_back({ "enabled", expressions.value(enabled->value_) });
        }
        if (config) {
            changes.push_back({ "config", expressions.binary(expressions.column("config"), ruvia::db_binary_operator::json_concat, expressions.cast(expressions.value(config->view()), ruvia::db_data_type::jsonb)) });
        }
        if (body.is_present<"remark">()) {
            changes.push_back({ "remark", remark && !remark->view().empty() ? expressions.value(remark->view()) : expressions.null_value() });
        }
        (void)co_await transaction.template get_repository<ProtocolConfigEntity>().update(
            service::common::database::activeId<ProtocolConfigEntity>(id),
            changes
        );
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "protocol", "updated", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
        try {
            (void)co_await service::rpc::call(c, "telemetry", "project-protocol", std::string(id));
        } catch (...) {
            // Startup hydration repairs Redis read models if Redis is temporarily unavailable.
        }
        try {
            co_await syncEdgeNodes(c, id);
        } catch (const std::exception& error) {
            logPostUpdateFailure("edge-sync", id, error.what());
        } catch (...) {
            logPostUpdateFailure("edge-sync", id, "unknown exception");
        }
    }

    template <typename Context>
    ruvia::task<void> createDlt645Point(Context& c, std::string_view id, const CreateDlt645PointBody& body) {
        ruvia::db_find_options options;
        options.where_ = service::common::database::activeId<ProtocolConfigEntity>(id);
        const auto existing = co_await c.db().template get_repository<ProtocolConfigEntity>().find_one(options);
        if (!existing) service::common::fail(16001, "协议配置不存在", 404);
        co_await requireOwner(c, existing->template get<"created_by">());
        auto transaction = co_await c.db().begin_transaction();
        options.lock_ = ruvia::db_lock_options{};
        const auto configuration = co_await transaction.template get_repository<ProtocolConfigEntity>().find_one(options);
        if (!configuration) service::common::fail(16001, "协议配置不存在", 404);
        if (configuration->template get<"protocol">() != "DLT645") {
            service::common::fail(16004, "此点位接口仅支持 DL/T645", 400);
        }
        const auto previous = ruvia::json_value::parse(configuration->template get<"config">());
        if (!previous) throw std::runtime_error("invalid stored protocol configuration");
        const auto points = previous->template get<ruvia::json_value>("points");
        if (!points || !points->is_array()) throw std::runtime_error("invalid stored protocol points");
        const auto pointId = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        const auto requestJson = ruvia::to_json(body);
        const auto request = ruvia::json_value::parse(requestJson);
        if (!request) throw std::runtime_error("invalid serialized point request");
        std::string pointJson = "{\"id\":" + service::utils::jsonQuoted(pointId);
        (void)request->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
            if (key != "visible") pointJson += ',' + service::utils::jsonQuoted(key) + ':' + std::string(value.view());
            return true;
        });
        pointJson += '}';
        std::string pointArray = "[";
        (void)points->for_each_element([&](const ruvia::json_value& point) {
            if (pointArray.size() > 1) pointArray += ',';
            pointArray += point.view();
            return true;
        });
        if (pointArray.size() > 1) pointArray += ',';
        pointArray += pointJson + ']';
        std::map<std::string, std::string> fields;
        (void)previous->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
            fields.emplace(key, value.view());
            return true;
        });
        fields["points"] = std::move(pointArray);
        if (!static_cast<bool>(body.get<"visible">().value_or(ruvia::bool_value{true}))) {
            std::string visibility = "{";
            if (const auto existingVisibility = previous->template get<ruvia::json_value>("pointVisibility")) {
                if (!existingVisibility->is_object()) throw std::runtime_error("invalid stored point visibility");
                (void)existingVisibility->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
                    if (visibility.size() > 1) visibility += ',';
                    visibility += service::utils::jsonQuoted(key) + ':' + std::string(value.view());
                    return true;
                });
            }
            if (visibility.size() > 1) visibility += ',';
            fields["pointVisibility"] = visibility + service::utils::jsonQuoted(pointId) + ":false}";
        }
        std::string merged = "{";
        for (const auto& [key, value] : fields) {
            if (merged.size() > 1) merged += ',';
            merged += service::utils::jsonQuoted(key) + ':' + value;
        }
        merged += '}';
        const auto parsed = ruvia::json_value::parse(merged);
        ProtocolConfigurationRules::validateConfig(parsed, "DLT645", true);
        validateDerivedConfig(*parsed);
        ruvia::db_expressions expressions(c.pool());
        const ruvia::db_assignment configurationChanges[]{
            { "config", expressions.cast(expressions.value(merged), ruvia::db_data_type::jsonb) },
            { "updated_at", expressions.call("now") }
        };
        (void)co_await transaction.template get_repository<ProtocolConfigEntity>().update(
            service::common::database::activeId<ProtocolConfigEntity>(id), configurationChanges);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "protocol", "updated", id,
            c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
        try {
            (void)co_await service::rpc::call(c, "telemetry", "project-protocol", std::string(id));
        } catch (...) {
            // Startup hydration repairs Redis read models if Redis is temporarily unavailable.
        }
        try {
            co_await syncEdgeNodes(c, id);
        } catch (const std::exception& error) {
            logPostUpdateFailure("edge-sync", id, error.what());
        } catch (...) {
            logPostUpdateFailure("edge-sync", id, "unknown exception");
        }
    }

    template <typename Context>
    ruvia::task<void> remove(Context& c, std::string_view id) {
        ruvia::db_find_options options;
        options.where_ = service::common::database::activeId<ProtocolConfigEntity>(id);
        const auto existing = co_await c.db().template get_repository<ProtocolConfigEntity>().find_one(options);
        if (!existing) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        co_await requireOwner(c, existing->template get<"created_by">());
        ruvia::db_find_options used;
        used.where_ = entities::DeviceEntity::column<"protocol_config_id">() == id &&
            entities::DeviceEntity::column<"deleted_at">().is_null();
        if (co_await c.db().template get_repository<entities::DeviceEntity>().exists(used)) {
            service::common::fail(16008, "协议配置已被设备使用，请先删除关联设备", 409);
        }
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_expressions expressions(c.pool());
        const ruvia::db_assignment deletionChanges[]{
            { "deleted_at", expressions.call("now") }, { "updated_at", expressions.call("now") }
        };
        (void)co_await transaction.template get_repository<ProtocolConfigEntity>().update(
            service::common::database::activeId<ProtocolConfigEntity>(id), deletionChanges
        );
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "protocol", "deleted", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
    }

  private:
    template <typename Context>
    static ruvia::task<std::string> detailData(Context& c, std::string_view id) {
        auto query = protocolSelect(c.pool());
        query.where(ProtocolConfigEntity::column<"deleted_at">().is_null().expression(query))
            .and_where(query.binary(query.column("id"), ruvia::db_binary_operator::equal, query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        co_return itemJson(rows.front());
    }

    static ruvia::db_query edgeNodeReferences(std::pmr::memory_resource* resource, std::string_view configId) {
        ruvia::db_query query(resource);
        const auto edgeNodeId =
            query.cast(query.column("edge_node_id", "l"), ruvia::db_data_type::text);
        query.select(edgeNodeId)
            .from(service::protocol::entities::DeviceEntity::table_name(), "d")
            .join(ruvia::db_join_type::inner, service::link::LinkEntity::table_name(), query.binary(query.column("id", "l"), ruvia::db_binary_operator::equal, query.column(service::protocol::entities::DeviceEntity::column_name<"link_id">(), "d")), "l")
            .where(query.binary(query.column(service::protocol::entities::DeviceEntity::column_name<"protocol_config_id">(), "d"), ruvia::db_binary_operator::equal, query.cast(query.value(configId), ruvia::db_data_type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::protocol::entities::DeviceEntity::column_name<"deleted_at">(), "d")))
            .and_where(query.binary(query.column("execution", "l"), ruvia::db_binary_operator::equal, query.value("edge")))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column("deleted_at", "l")))
            .and_where(query.unary(ruvia::db_unary_operator::is_not_null, query.column("edge_node_id", "l")))
            .distinct()
            .order_by(edgeNodeId);
        return query;
    }

    static ruvia::task<void> requireDerivedSupport(ruvia::db_transaction& transaction, std::pmr::memory_resource* resource, std::string_view configId) {
        const auto rows = co_await transaction.query(edgeNodeReferences(resource, configId));
        for (const auto& row : rows) {
            const auto nodeId = row[0].value().value_or(std::string_view{});
            ruvia::db_find_options nodeOptions;
            nodeOptions.where_ = service::edge::EdgeNodeEntity::column<"id">() == nodeId;
            const auto node = co_await transaction.template get_repository<service::edge::EdgeNodeEntity>().find_one(nodeOptions);
            const auto capability = node ? ruvia::json_value::parse(node->template get<"capability">()) : std::optional<ruvia::json_value>{};
            const auto supported = capability ? capability->template get<ruvia::bool_value>("derivedPoints") : std::optional<ruvia::bool_value>{};
            if (!supported || !static_cast<bool>(*supported)) service::common::fail(16009, "关联边缘节点尚不支持派生点，请先升级固件", 409);
        }
    }

    template <typename Context>
    static ruvia::task<void> syncEdgeNodes(Context& c, std::string_view configId) {
        const auto rows = co_await c.db().query(edgeNodeReferences(c.pool(), configId));
        for (const auto& row : rows) {
            const auto nodeId = row[0].value().value_or(std::string_view{});
            if (nodeId.empty()) continue;
            try {
                (void)co_await service::edge::EdgeService::queueSnapshot(c, nodeId, c.userId);
            } catch (const std::exception& error) {
                std::cerr << "protocol edge config sync failed: node=" << nodeId
                          << " config=" << configId << " error=" << error.what() << '\n';
            } catch (...) {
                std::cerr << "protocol edge config sync failed: node=" << nodeId
                          << " config=" << configId << " error=unknown exception\n";
            }
        }
    }

    static void logPostUpdateFailure(std::string_view stage, std::string_view configId, std::string_view message) {
        std::cerr << "protocol post-update " << stage << " failed: config=" << configId
                  << " error=" << message << '\n';
    }

    static ruvia::db_query protocolSelect(std::pmr::memory_resource* resource) {
        ruvia::db_query query(resource);
        query.select({ query.cast(query.column("id"), ruvia::db_data_type::text), query.column("protocol"), query.column("name"), query.column("enabled"), query.column("config"), service::common::database::emptyText(query, "remark"), query.cast(query.call("iot_utc_timestamp", { query.column("created_at") }), ruvia::db_data_type::text), query.cast(query.call("iot_utc_timestamp", { query.column("updated_at") }), ruvia::db_data_type::text) })
            .from(ProtocolConfigEntity::table_name())
            .where(ProtocolConfigEntity::column<"deleted_at">().is_null().expression(query));
        return query;
    }

    static std::string itemJson(const ruvia::db_row& row) {
        const auto enabled = row[3].as<bool>().value_or(false);
        std::string result = "{\"id\":" + service::utils::jsonQuoted(row[0].value().value_or("")) +
            ",\"protocol\":" +
            service::utils::jsonQuoted(row[1].value().value_or("")) +
            ",\"name\":" + service::utils::jsonQuoted(row[2].value().value_or("")) +
            ",\"enabled\":" + (enabled ? "true" : "false") +
            ",\"config\":" + std::string(row[4].value().value_or("{}")) +
            ",\"remark\":" + service::utils::jsonQuoted(row[5].value().value_or("")) +
            ",\"created_at\":" +
            service::utils::jsonQuoted(row[6].value().value_or("")) +
            ",\"updated_at\":" +
            service::utils::jsonQuoted(row[7].value().value_or("")) + "}";
        return result;
    }

    template <typename Context>
    ruvia::task<void> ensureNameAvailable(Context& c, const std::string& name, std::optional<std::string> excludedId) {
        ruvia::db_find_options options;
        options.where_ = ProtocolConfigEntity::column<"deleted_at">().is_null() &&
            ProtocolConfigEntity::column<"name">() == name;
        if (excludedId) {
            options.where_ = std::move(options.where_) && ProtocolConfigEntity::column<"id">() != *excludedId;
        }
        if (co_await c.db().template get_repository<ProtocolConfigEntity>().exists(options)) {
            service::common::fail(16005, "配置名称已存在", 409);
        }
    }

    template <typename Context>
    ruvia::task<void> requireOwner(Context& c, std::string_view ownerId) {
        if (c.userId == ownerId) {
            co_return;
        }
        ruvia::db_query query(c.pool());
        query.select(query.cast(query.value(1), ruvia::db_data_type::integer))
            .from(service::user::UserRoleEntity::table_name(), "ur")
            .join(ruvia::db_join_type::inner, service::role::RoleEntity::table_name(), query.binary(query.column("role_id", "ur"), ruvia::db_binary_operator::equal, query.column("id", "r")), "r")
            .where(query.binary(query.column("user_id", "ur"), ruvia::db_binary_operator::equal, query.value(c.userId)))
            .and_where(query.binary(query.column("code", "r"), ruvia::db_binary_operator::equal, query.value("superadmin")))
            .and_where(query.binary(query.column("status", "r"), ruvia::db_binary_operator::equal, query.value("enabled")))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column("deleted_at", "r")))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(16007, "只能修改或删除自己创建的协议配置", 403);
        }
    }
};

inline ProtocolService& protocolService() {
    return ProtocolService::instance();
}

} // namespace service::protocol
