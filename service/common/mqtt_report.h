#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/model.h>
#include <ruvia/web/model_json.h>

#include "service/common/uuid.h"
#include "service/utils/json.h"
#include "service/utils/payload.h"

namespace service::common {

// MQTT 3.1.1 Topic 过滤器匹配；filter 中的 {deviceCode} 须已替换。
inline bool mqttTopicMatches(std::string_view filter, std::string_view topic) {
    if (topic.starts_with('$') && !filter.starts_with('$')) return false;
    const auto pattern = utils::splitPayload(filter,"/"), levels = utils::splitPayload(topic,"/");
    std::size_t index = 0;
    for (; index < pattern.size(); ++index) {
        if (pattern[index] == "#") return true;
        if (index == levels.size() || (pattern[index] != "+" && pattern[index] != levels[index])) return false;
    }
    return index == levels.size();
}

inline bool mqttReportTopicMatches(std::string filter, std::string_view topic, std::string_view code) {
    constexpr std::string_view marker = "{deviceCode}";
    for (auto index = filter.find(marker); index != std::string::npos; index = filter.find(marker, index + code.size())) {
        filter.replace(index, marker.size(), code);
    }
    return mqttTopicMatches(filter, topic);
}

inline bool mqttReportTopicsOverlap(std::string_view left, std::string_view right) {
    const auto a = utils::splitPayload(left,"/"), b = utils::splitPayload(right,"/");
    for (std::size_t index=0; index<a.size() && index<b.size(); ++index) {
        if (a[index]=="#" || b[index]=="#") return true;
        if (a[index]!=b[index] && a[index]!="+" && b[index]!="+" && a[index]!="{deviceCode}" && b[index]!="{deviceCode}") return false;
    }
    if (a.size()==b.size()) return true;
    return a.size()<b.size() ? b[a.size()]=="#" : a[b.size()]=="#";
}

RUVIA_MODEL(MqttReportField,
    RUVIA_REQUIRED_FIELD(pointId, ruvia::string),
    RUVIA_REQUIRED_FIELD(field, ruvia::string));
RUVIA_MODEL(MqttReport,
    RUVIA_REQUIRED_FIELD(id, ruvia::string),
    RUVIA_REQUIRED_FIELD(name, ruvia::string),
    RUVIA_REQUIRED_FIELD(topic, ruvia::string),
    RUVIA_REQUIRED_FIELD(reportTemplate, ruvia::string),
    RUVIA_REQUIRED_FIELD(pointFields, ruvia::array<MqttReportField>),
    RUVIA_OPTIONAL_FIELD(qos, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(recordsPath, ruvia::string),
    RUVIA_OPTIONAL_FIELD(deviceCodeField, ruvia::string),
    RUVIA_OPTIONAL_FIELD(identitySource, ruvia::string),
    RUVIA_OPTIONAL_FIELD(topicDeviceSegment, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(timeField, ruvia::string),
    RUVIA_OPTIONAL_FIELD(timeFormat, ruvia::string));

// 将每条上报配置投影到同一解码契约。旧单消息配置同样只经过这一入口。
inline std::vector<std::string> mqttReportConfigurations(const ruvia::json_value& config) {
    const auto reports = config.get<ruvia::json_value>("reports");
    if (!reports) return {std::string(config.view())};
    const auto decoded = reports->get<ruvia::array<MqttReport>>();
    const auto points = config.get<ruvia::json_value>("points");
    if (!decoded || decoded->empty() || decoded->size() > 32 || !points || !points->is_array())
        throw std::invalid_argument("MQTT 上报消息须为 1–32 条，并定义设备属性");
    std::set<std::string> pointIds, reportIds, names, topics;
    (void)points->for_each_element([&](const ruvia::json_value& point) {
        if (const auto id = point.get<ruvia::string>("id")) pointIds.emplace(id->view());
        return true;
    });
    std::vector<std::string> result;
    for (const auto& report : *decoded) {
        for (const auto& topic : topics) {
            if (mqttReportTopicsOverlap(topic,report.get<"topic">().view()))
                throw std::invalid_argument("同一设备的上报 Topic 不能重复或交叉匹配");
        }
        if (!isUuidField(report.get<"id">()) || !reportIds.emplace(report.get<"id">().view()).second ||
            report.get<"name">().view().empty() || report.get<"name">().view().size() > 100 ||
            !names.emplace(report.get<"name">().view()).second || !topics.emplace(report.get<"topic">().view()).second)
            throw std::invalid_argument("上报消息标识、名称和 Topic 必须有效且唯一");
        std::map<std::string,std::string> fields;
        for (const auto& field : report.get<"pointFields">()) {
            const std::string id(field.get<"pointId">().view());
            if (!pointIds.contains(id) || field.get<"field">().view().empty() ||
                !fields.emplace(id, field.get<"field">().view()).second)
                throw std::invalid_argument("上报消息引用了未知或重复属性");
        }
        if (fields.empty()) throw std::invalid_argument("每条上报消息至少绑定一个属性");
        std::map<std::string,std::string> projected;
        (void)config.for_each_field([&](std::string_view name,const ruvia::json_value& value) {
            if (name != "reports") projected.emplace(name,value.view());
            return true;
        });
        for (const auto name : {"topic","recordsPath","deviceCodeField","identitySource","topicDeviceSegment","payloadFormat","delimiter","recordDelimiter","recordLength","timeField","timeFormat","reportTemplate","qos"})
            projected.erase(name);
        // to_json 的内容必须覆盖视图使用期。
        const auto reportWire = ruvia::to_json(report);
        const auto parsedReport = ruvia::json_value::parse(std::string_view(reportWire));
        if (!parsedReport) throw std::invalid_argument("上报消息字段无效");
        (void)parsedReport->for_each_field([&](std::string_view name,const ruvia::json_value& value) {
            if (name != "id" && name != "name" && name != "pointFields") projected[std::string(name)] = value.view();
            return true;
        });
        projected["payloadFormat"] = "\"json\"";
        std::string mapped = "[";
        (void)points->for_each_element([&](const ruvia::json_value& point) {
            const auto id = point.get<ruvia::string>("id");
            if (!id) throw std::invalid_argument("设备属性标识无效");
            if (mapped.size()>1) mapped += ',';
            mapped += '{';
            (void)point.for_each_field([&](std::string_view name,const ruvia::json_value& value) {
                if (name != "field") mapped += utils::jsonQuoted(name) + ':' + std::string(value.view()) + ',';
                return true;
            });
            const auto match = fields.find(std::string(id->view()));
            mapped += "\"field\":" + utils::jsonQuoted(match == fields.end() ? "" : match->second) + '}';
            return true;
        });
        projected["points"] = mapped + ']';
        std::string json = "{";
        for (const auto& [name,value] : projected) {
            if (json.size()>1) json += ',';
            json += utils::jsonQuoted(name) + ':' + value;
        }
        result.push_back(json + '}');
    }
    return result;
}

// 上报消息中一个已绑定载荷字段的设备属性。
struct MqttReportPoint {
    std::string id, name, field, dataType, unit;
    std::vector<std::pair<std::string, std::string>> enums;
};

// 单条上报消息的载荷解码参数，缺省值与实际接入一致。
struct MqttReportLayout {
    std::string format, delimiter, recordsPath, recordDelimiter, identitySource, deviceCodeField, timeField, timeFormat;
    std::size_t topicDeviceSegment = 1, recordLength = 0;
    std::vector<MqttReportPoint> points;
};

// 一条设备记录的解码结果；values 与 MqttReportLayout::points 同序，缺失或转换失败为空。
struct MqttReportRecord {
    std::string deviceCode;
    std::int64_t occurredAtMs = 0;
    bool timeValid = true;
    std::vector<std::optional<utils::PayloadScalar>> values;
};

template <typename Points>
void appendMqttReportPoints(std::vector<MqttReportPoint>& output, const Points& points) {
    for (const auto& point : points) {
        if (point.template get<"field">().view().empty()) {
            continue;
        }
        const auto& unit = point.template get<"unit">();
        auto& mapped = output.emplace_back(MqttReportPoint{ .id = std::string(point.template get<"id">().view()), .name = std::string(point.template get<"name">().view()), .field = std::string(point.template get<"field">().view()), .dataType = std::string(point.template get<"dataType">().view()), .unit = unit ? std::string(unit->view()) : std::string{} });
        if (const auto& mappings = point.template get<"enumValues">(); mappings) {
            for (const auto& mapping : *mappings) {
                mapped.enums.emplace_back(mapping.template get<"input">().view(), mapping.template get<"output">().view());
            }
        }
    }
}

template <typename Points>
void appendMqttReportPoints(std::vector<MqttReportPoint>& output, const std::optional<Points>& points) {
    if (points) {
        appendMqttReportPoints(output, *points);
    }
}

// 由 mqttReportConfigurations 投影出的单条上报配置模型构造解码参数。
template <typename Config>
MqttReportLayout mqttReportLayout(const Config& config) {
    const auto option = [](const auto& value, std::string_view fallback) {
        return value ? std::string(value->view()) : std::string(fallback);
    };
    MqttReportLayout layout;
    layout.format = option(config.template get<"payloadFormat">(), "json");
    layout.delimiter = option(config.template get<"delimiter">(), ",");
    layout.recordsPath = option(config.template get<"recordsPath">(), "");
    layout.recordDelimiter = option(config.template get<"recordDelimiter">(), "\n");
    layout.deviceCodeField = option(config.template get<"deviceCodeField">(), "");
    layout.identitySource = option(config.template get<"identitySource">(), layout.deviceCodeField.empty() ? "bound" : "payload");
    layout.timeField = option(config.template get<"timeField">(), "");
    layout.timeFormat = option(config.template get<"timeFormat">(), "unix_ms");
    layout.topicDeviceSegment = static_cast<std::size_t>(config.template get<"topicDeviceSegment">().value_or(ruvia::int64{ 1 }).value_);
    layout.recordLength = static_cast<std::size_t>(config.template get<"recordLength">().value_or(ruvia::int64{ 0 }).value_);
    appendMqttReportPoints(layout.points, config.template get<"points">());
    return layout;
}

// identitySource=topic 时 Topic 指定层即设备编码；层不存在时为空。
inline std::string_view mqttReportTopicDeviceCode(const MqttReportLayout& layout, std::string_view topic) {
    const auto levels = utils::splitPayload(topic, "/");
    return layout.topicDeviceSegment < levels.size() ? levels[layout.topicDeviceSegment] : std::string_view{};
}

// 按上报配置解码一条消息：拆分记录、提取设备编码、解析设备时间、映射枚举并转换类型。
// boundDeviceCode 为 identitySource=bound 时的设备编码；记录无设备时间时使用 receivedAtMs，
// 设备时间无效时 timeValid=false。载荷或字段选择器无效时抛出 std::invalid_argument。
inline std::vector<MqttReportRecord> decodeMqttReport(const MqttReportLayout& layout, std::string_view topic, std::string_view payload, std::string_view boundDeviceCode, std::string_view timezone, std::int64_t receivedAtMs) {
    const auto records = utils::payloadRecords(payload, layout.format, layout.recordsPath, layout.recordDelimiter, layout.recordLength);
    std::vector<MqttReportRecord> result;
    result.reserve(records.size());
    for (const auto& input : records) {
        const utils::PayloadRecord record(input, layout.format, layout.delimiter);
        auto& decoded = result.emplace_back();
        if (layout.identitySource == "payload") {
            const auto code = record.field(layout.deviceCodeField);
            decoded.deviceCode = code ? utils::payloadScalarText(*code) : std::string{};
        } else {
            decoded.deviceCode = layout.identitySource == "topic" ? mqttReportTopicDeviceCode(layout, topic) : boundDeviceCode;
        }
        decoded.occurredAtMs = receivedAtMs;
        if (!layout.timeField.empty()) {
            if (const auto time = record.field(layout.timeField)) {
                const auto timestamp = utils::payloadTimestamp(*time, layout.timeFormat, timezone);
                decoded.timeValid = timestamp.has_value();
                if (timestamp) {
                    decoded.occurredAtMs = *timestamp;
                }
            }
        }
        decoded.values.reserve(layout.points.size());
        for (const auto& point : layout.points) {
            auto value = record.field(point.field);
            if (value && !point.enums.empty()) {
                const auto key = utils::payloadScalarText(*value);
                const auto found = std::find_if(point.enums.begin(), point.enums.end(), [&](const auto& mapping) {
                    return mapping.first == key;
                });
                value = found == point.enums.end() ? std::nullopt : std::optional(utils::PayloadScalar{ found->second });
            }
            decoded.values.push_back(value ? utils::convertPayloadValue(*value, point.dataType) : std::nullopt);
        }
    }
    return result;
}

} // namespace service::common
