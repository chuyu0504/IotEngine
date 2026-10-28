#pragma once

#include <cctype>
#include <charconv>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

#include <ruvia/web/model.h>

#include "service/features/collector/mqtt/mqtt.types.h"
#include "service/features/configuration/configuration.types.h"

namespace service::configuration {

inline std::string configText(const std::optional<ruvia::string>& value, std::string_view fallback = {}) {
    return value ? std::string(value->view()) : std::string(fallback);
}

inline std::int64_t configIntegerText(std::string_view value, std::int64_t fallback = 0) noexcept {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    if (value.empty()) {
        return fallback;
    }
    std::int64_t result{};
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error == std::errc{} && end == value.data() + value.size()) {
        return result;
    }
    // 配置允许整数形式的十进制/指数 token；不经 double 舍入把小数误认成整数。
    const bool negative = value.front() == '-';
    if (negative) {
        value.remove_prefix(1);
    }
    const auto exponentPosition = value.find_first_of("eE");
    std::int64_t exponent{};
    if (exponentPosition != std::string_view::npos) {
        auto text = value.substr(exponentPosition + 1);
        if (!text.empty() && text.front() == '+') {
            text.remove_prefix(1);
        }
        const auto [parsedEnd, parsedError] = std::from_chars(text.data(), text.data() + text.size(), exponent);
        if (text.empty() || parsedError != std::errc{} || parsedEnd != text.data() + text.size() ||
            exponent < -4096 || exponent > 4096) {
            return fallback;
        }
        value = value.substr(0, exponentPosition);
    }
    std::int64_t digits{};
    std::int64_t decimalPosition{ -1 };
    std::int64_t firstNonzero{ -1 };
    std::int64_t lastNonzero{ -1 };
    for (const auto character : value) {
        if (character == '.' && decimalPosition < 0) {
            decimalPosition = digits;
        } else if (character >= '0' && character <= '9') {
            if (character != '0') {
                if (firstNonzero < 0) {
                    firstNonzero = digits;
                }
                lastNonzero = digits;
            }
            ++digits;
        } else {
            return fallback;
        }
    }
    if (digits == 0) {
        return fallback;
    }
    if (firstNonzero < 0) {
        return 0;
    }
    decimalPosition = (decimalPosition < 0 ? digits : decimalPosition) + exponent;
    if (lastNonzero >= decimalPosition || decimalPosition - firstNonzero > 19) {
        return fallback;
    }
    const auto limit = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) + (negative ? 1U : 0U);
    std::uint64_t magnitude{};
    std::int64_t position{};
    for (const auto character : value) {
        if (character == '.') {
            continue;
        }
        if (position >= firstNonzero && position < decimalPosition) {
            const auto digit = static_cast<unsigned>(character - '0');
            if (magnitude > (limit - digit) / 10) {
                return fallback;
            }
            magnitude = magnitude * 10 + digit;
        }
        ++position;
    }
    while (position++ < decimalPosition) {
        if (magnitude > limit / 10) {
            return fallback;
        }
        magnitude *= 10;
    }
    if (negative && magnitude == limit) {
        return std::numeric_limits<std::int64_t>::min();
    }
    return negative ? -static_cast<std::int64_t>(magnitude) : static_cast<std::int64_t>(magnitude);
}

inline std::int64_t configInteger(const std::optional<ruvia::json_value>& value, std::int64_t fallback = 0) {
    if (!value || value->is_null()) {
        return fallback;
    }
    if (value->is_number()) {
        return configIntegerText(value->view(), fallback);
    }
    if (const auto text = value->get<ruvia::string>()) {
        return configIntegerText(text->view(), fallback);
    }
    return fallback;
}

inline bool configWritable(const std::optional<ruvia::json_value>& value) {
    if (!value || value->is_null()) {
        return false;
    }
    const auto trueText = [](std::string_view text) {
        if (text == "1" || text == "t" || text == "T") {
            return true;
        }
        return text.size() == 4 && std::tolower(static_cast<unsigned char>(text[0])) == 't' &&
            std::tolower(static_cast<unsigned char>(text[1])) == 'r' &&
            std::tolower(static_cast<unsigned char>(text[2])) == 'u' &&
            std::tolower(static_cast<unsigned char>(text[3])) == 'e';
    };
    if (const auto text = value->get<ruvia::string>()) {
        return trueText(text->view());
    }
    return trueText(value->view());
}

// 配置数组顺序就是协议要素顺序；尤其 SL651 的无标识/定长字段不能按 id 或地址重排。
inline void decodeModelElements(DeviceModel& model) {
    auto& device = model.device;
    const auto config = ruvia::json_value::parse(device.calculationConfig);

    const auto append = [&](const ElementConfig& source, std::string_view functionCode = {}, std::string_view direction = {}, bool response = false) {
        service::collector::ElementDefinition element;
        element.id = configText(source.get<"id">());
        element.configKey = (response ? "response:" : "element:") + element.id;
        element.name = configText(source.get<"name">());
        element.unit = configText(source.get<"unit">());
        const auto defaultType = device.protocol == "S7" ? "UINT8" : device.protocol == "DLT645" ? "BCD"
            : (device.protocol == "MC" || device.protocol == "FINS")                             ? "UINT16"
                                                                                                 : "";
        element.dataType = configText(source.get<"dataType">(), defaultType);
        if (device.protocol == "Modbus" || device.protocol == "MC" || device.protocol == "FINS" || device.protocol == "DLT645") {
            element.byteOrder = configText(source.get<"byteOrder">(), device.protocol == "MC" ? "LITTLE_ENDIAN" : "BIG_ENDIAN");
        }
        if (device.protocol == "Modbus" && !source.get<"byteOrder">()) {
            const auto byteOrder = config.get<ruvia::string>("byteOrder");
            element.byteOrder = byteOrder ? std::string(byteOrder->view()) : "BIG_ENDIAN";
        }
        element.registerType = configText(source.get<"registerType">());
        element.address = configInteger(source.get<"address">());
        element.quantity = configInteger(source.get<"quantity">(), device.protocol == "Modbus" ? 1 : 0);
        element.decimals = configInteger(source.get<"decimals">(), -1);
        element.writable = configWritable(source.get<"writable">());
        element.area = configText(source.get<"area">());
        element.dbNumber = configInteger(source.get<"dbNumber">());
        element.start = configInteger(source.get<"start">());
        element.startBit = configInteger(device.protocol == "S7" ? source.get<"startBit">() : source.get<"bit">());
        element.size = configInteger(source.get<"size">(), device.protocol == "S7" ? 1 : 0);
        element.functionCode = functionCode;
        element.direction = direction;
        element.responseElement = response;
        element.guideHex = configText(source.get<"guideHex">());
        element.encoding = configText(source.get<"encode">());
        element.length = configInteger(source.get<"length">(), device.protocol == "DLT645" ? 4 : 0);
        element.digits = configInteger(source.get<"digits">(), device.protocol == "DLT645" ? 2 : 0);
        element.positionMode = configText(source.get<"positionMode">(), "GUIDE");
        element.byteOffset = configInteger(source.get<"byteOffset">());
        if (device.protocol == "SL651") {
            element.writable = direction == "DOWN" && !response;
        }
        if (device.protocol == "DLT645") {
            element.guideHex = configText(source.get<"identifier">());
            element.encoding = element.dataType;
        }
        if (source.get<"id">() && element.encoding != "JPEG" && (device.protocol != "SL651" || (direction == "UP" && !response))) {
            model.points.push_back({ element.id, configText(source.get<"name">(), element.id), element.unit });
        }
        device.elements.push_back(std::move(element));
    };
    if (device.protocol == "MQTT") {
        const auto mqtt = ruvia::from_json<service::collector::mqtt::Config>(device.calculationConfig);

        for (const auto& point : mqtt.get<"points">()) {
            service::collector::ElementDefinition element;
            element.id = point.get<"id">().view();
            element.configKey = "element:" + element.id;
            element.name = point.get<"name">().view();
            element.unit = configText(point.get<"unit">());
            element.dataType = point.get<"dataType">().view();
            element.mqttField = point.get<"field">().view();
            element.writable = point.get<"writable">().value_or(ruvia::bool_value{ false }).value_;
            element.size = 65535;
            model.points.push_back({ element.id, element.name, element.unit });
            device.elements.push_back(std::move(element));
        }
    } else if (device.protocol == "SL651") {
        if (const auto raw = config.get<ruvia::json_value>("funcs")) {
            const auto functions = raw->get<ruvia::array<FunctionConfig>>();
            if (!functions) {
                throw std::invalid_argument("invalid SL651 functions");
            }
            for (const auto& function : *functions) {
                auto& definition = model.functions.emplace_back(FunctionDefinition{ configText(function.get<"funcCode">()), configText(function.get<"name">()), configText(function.get<"dir">()) });
                if (const auto& elements = function.get<"elements">()) {
                    for (const auto& element : *elements) {
                        append(element, definition.code, definition.direction);
                    }
                }
                if (const auto& elements = function.get<"responseElements">()) {
                    for (const auto& element : *elements) {
                        append(element, definition.code, definition.direction, true);
                    }
                }
            }
        }
    } else {
        const auto arrayKey = device.protocol == "Modbus" ? "registers" : device.protocol == "S7" ? "areas"
                                                                                                  : "points";
        if (const auto raw = config.get<ruvia::json_value>(arrayKey)) {
            const auto elements = raw->get<ruvia::array<ElementConfig>>();
            if (!elements) {
                throw std::invalid_argument("invalid protocol elements");
            }
            device.elements.reserve(elements->size());
            for (const auto& element : *elements) {
                append(element);
            }
        }
    }
    if (const auto raw = config.get<ruvia::json_value>("derivedPoints")) {
        const auto points = raw->get<ruvia::array<service::common::DerivedPointConfig>>();
        if (!points) {
            throw std::invalid_argument("invalid derived points");
        }
        for (const auto& point : *points) {
            const auto id = configText(point.get<"id">());
            model.points.push_back({ id, configText(point.get<"name">(), id), configText(point.get<"unit">()) });
        }
    }
    model.derivedPoints = service::common::orderDerivedPoints(config);
    // 外部 pointId 是最新值聚合身份，而不是函数 occurrence。SL651 目录只展示 UP，
    // 位置取首次出现、名称/单位取最后可展示定义；latest 的控制能力视图另含 DOWN/响应。
    if (model.points.size() < 2) {
        return;
    }
    std::unordered_map<std::string_view, std::size_t> pointIndexes;
    pointIndexes.reserve(model.points.size());
    std::size_t pointCount{};
    for (std::size_t index = 0; index < model.points.size(); ++index) {
        auto& point = model.points[index];
        const auto existing = pointIndexes.find(point.id);
        if (existing != pointIndexes.end()) {
            auto& target = model.points[existing->second];
            target.name = std::move(point.name);
            target.unit = std::move(point.unit);
        } else {
            if (index != pointCount) {
                model.points[pointCount] = std::move(point);
            }
            pointIndexes.emplace(model.points[pointCount].id, pointCount);
            ++pointCount;
        }
    }
    model.points.resize(pointCount);
}

} // namespace service::configuration
