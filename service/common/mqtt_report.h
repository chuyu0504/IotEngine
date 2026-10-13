#pragma once

#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <ruvia/web/model.h>
#include <ruvia/web/model_json.h>

#include "service/common/uuid.h"
#include "service/utils/json.h"
#include "service/utils/payload.h"

namespace service::common {

inline bool mqttReportTopicMatches(std::string filter, std::string_view topic, std::string_view code) {
    constexpr std::string_view marker = "{deviceCode}";
    for (auto index = filter.find(marker); index != std::string::npos; index = filter.find(marker, index + code.size()))
        filter.replace(index, marker.size(), code);
    if (topic.starts_with('$') && !filter.starts_with('$')) return false;
    const auto pattern = utils::splitPayload(filter,"/"), levels = utils::splitPayload(topic,"/");
    std::size_t index = 0;
    for (; index < pattern.size(); ++index) {
        if (pattern[index] == "#") return true;
        if (index == levels.size() || (pattern[index] != "+" && pattern[index] != levels[index])) return false;
    }
    return index == levels.size();
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

} // namespace service::common
