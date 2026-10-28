#pragma once

#include <algorithm>
#include <charconv>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <ruvia/web/model.h>
#include <ruvia/web/validation.h>

#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/utils/json.h"
#include "service/utils/text.h"

namespace service::alert {

inline constexpr auto kDefaultSeverity = RUVIA_DEFAULT("warning"){};
inline constexpr auto kDefaultLogic = RUVIA_DEFAULT("and"){};
inline constexpr auto kDefaultSilenceDuration = RUVIA_DEFAULT(300){};
inline constexpr auto kDefaultRecoveryCondition = RUVIA_DEFAULT("reverse"){};
inline constexpr auto kDefaultRecoveryWaitSeconds = RUVIA_DEFAULT(60){};
inline constexpr auto kDefaultStatus = RUVIA_DEFAULT("enabled"){};
inline constexpr auto kDefaultRemark = RUVIA_DEFAULT(""){};
inline constexpr auto kDefaultCategory = RUVIA_DEFAULT(""){};
inline constexpr auto kDefaultDescription = RUVIA_DEFAULT(""){};
inline constexpr auto kDefaultProtocolConfigId = RUVIA_DEFAULT(""){};
inline constexpr auto kDefaultPage = RUVIA_DEFAULT(1){};
inline constexpr auto kDefaultPageSize = RUVIA_DEFAULT(20){};

RUVIA_MODEL(AlertCondition, RUVIA_REQUIRED_FIELD(type, ruvia::string), RUVIA_OPTIONAL_FIELD(elementKey, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("operator", comparison, ruvia::string), RUVIA_OPTIONAL_FIELD(changeDirection, ruvia::string), RUVIA_OPTIONAL_FIELD(value, ruvia::json_value), RUVIA_OPTIONAL_FIELD(changeRate, ruvia::json_value), RUVIA_OPTIONAL_FIELD(duration, ruvia::json_value), RUVIA_OPTIONAL_FIELD(bitIndex, ruvia::json_value));

namespace alertRequest {
inline bool decimalText(std::string_view raw, bool allowNegative) {
    const auto value = service::utils::trim(raw);
    if (value.empty() || value.size() > 64) {
        return false;
    }
    std::size_t index = 0;
    if (allowNegative && value[index] == '-') {
        if (++index == value.size()) {
            return false;
        }
    }
    bool beforeDot = false, afterDot = false, dot = false;
    for (; index < value.size(); ++index) {
        const char c = value[index];
        if (c >= '0' && c <= '9') {
            if (dot) {
                afterDot = true;
            } else {
                beforeDot = true;
            }
        } else if (c == '.' && !dot) {
            dot = true;
        } else {
            return false;
        }
    }
    return beforeDot && (!dot || afterDot);
}

inline std::optional<std::string> scalarText(const ruvia::json_value& object, std::string_view field) {
    const auto value = object.get<ruvia::json_value>(field);
    if (!value || value->is_null()) {
        return std::nullopt;
    }
    if (value->is_string()) {
        const auto decoded = object.get<ruvia::string>(field);
        auto text = service::utils::trim(decoded->view());
        if (text.size() > 128) {
            service::common::fail(17002, "告警条件值过长", 400);
        }
        return text;
    }
    if (value->is_number() || value->is_boolean()) {
        return service::utils::trim(value->view());
    }
    service::common::fail(17002, "告警条件值必须是标量", 400);
}

inline std::optional<std::int64_t> integerValue(const ruvia::json_value& object, std::string_view field) {
    const auto text = scalarText(object, field);
    if (!text || text->empty()) {
        return std::nullopt;
    }
    std::int64_t result{};
    const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), result);
    if (error != std::errc{} || end != text->data() + text->size()) {
        service::common::fail(17002, "告警条件值必须是整数", 400);
    }
    return result;
}

inline void validateConditions(std::string_view raw) {
    std::optional<ruvia::json_value> parsed;
    try {
        parsed = ruvia::json_value::parse(raw);
    } catch (const std::invalid_argument&) {
        service::common::fail(17002, "至少配置一个告警条件", 400);
    }
    std::size_t count = 0;
    const bool valid = parsed->for_each_element([&](const ruvia::json_value& value) {
        const auto condition = value.get<AlertCondition>();
        if (!condition) {
            service::common::fail(17002, "告警条件字段类型无效", 400);
        }

        ++count;
        const auto type = service::utils::trim(condition->get<"type">().view());
        if (type == "offline") {
            const auto duration = integerValue(value, "duration");
            if (duration && (*duration < 1 || *duration > 86400)) {
                service::common::fail(17002, "离线检测时长超出允许范围", 400);
            }
            return true;
        }
        const auto& element = condition->get<"elementKey">();
        if (!element || service::utils::trim(element->view()).empty() || service::utils::trim(element->view()).size() > 128) {
            service::common::fail(17002, "请选择告警要素", 400);
        }
        if (type == "threshold") {
            const auto& comparison = condition->get<"comparison">();
            const auto op = comparison ? service::utils::trim(comparison->view()) : "";
            if (op != ">" && op != ">=" && op != "<" && op != "<=" && op != "==" && op != "!=") {
                service::common::fail(17002, "告警比较符无效", 400);
            }
            const auto threshold = scalarText(value, "value");
            if (!threshold || threshold->empty()) {
                service::common::fail(17002, "告警阈值不能为空", 400);
            }
            if (op != "==" && op != "!=" && !decimalText(*threshold, true)) {
                service::common::fail(17002, "告警阈值必须是数字", 400);
            }
        } else if (type == "rate_of_change") {
            const auto& requested = condition->get<"changeDirection">();
            const auto direction = requested ? service::utils::trim(requested->view()) : "any";
            if (direction != "any" && direction != "rise" && direction != "fall") {
                service::common::fail(17002, "变化方向无效", 400);
            }
            const auto rate = scalarText(value, "changeRate");
            if (rate && !rate->empty() && !decimalText(*rate, false)) {
                service::common::fail(17002, "变化率必须是非负数字", 400);
            }
        } else {
            service::common::fail(17002, "告警条件类型无效", 400);
        }
        const auto bit = integerValue(value, "bitIndex");
        if (bit && (*bit < 0 || *bit > 62)) {
            service::common::fail(17002, "位索引超出允许范围", 400);
        }
        return true;
    });
    if (!valid || count == 0) {
        service::common::fail(17002, "至少配置一个告警条件", 400);
    }
}
} // namespace alertRequest

inline bool isAlertName(const ruvia::string& value) {
    const auto name = service::utils::trim(value.view());
    return !name.empty() && name.size() <= 128;
}

inline bool isAlertUuidList(const ruvia::array<ruvia::string>& values) {
    return !values.empty() && values.size() <= 1000 && std::ranges::all_of(values, service::common::isUuidField);
}

inline std::vector<std::string> uniqueAlertIds(const ruvia::array<ruvia::string>& values) {
    std::set<std::string> ids;
    for (const auto& value : values) {
        ids.emplace(value.view());
    }
    return { ids.begin(), ids.end() };
}

inline bool isAlertOptionalUuid(const ruvia::string& value) {
    return value.view().empty() || service::common::isUuidField(value);
}

RUVIA_MODEL(ApplyTemplateInput, RUVIA_REQUIRED_FIELD_NAME("template_id", templateId, ruvia::string, RUVIA_CUSTOM("请选择告警模板", service::common::isUuidField)), RUVIA_REQUIRED_FIELD_NAME("device_ids", deviceIds, ruvia::array<ruvia::string>, RUVIA_CUSTOM("请选择有效的目标设备", isAlertUuidList)));
RUVIA_MODEL(AlertBatchBody, RUVIA_REQUIRED_FIELD(ids, ruvia::array<ruvia::string>, RUVIA_CUSTOM("请选择有效的操作对象", isAlertUuidList)));
// 条件和扩展协议列表保留原文；条件的跨字段业务约束在 Service 中校验。
RUVIA_MODEL(RuleInput, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_CUSTOM("规则名称无效", isAlertName)), RUVIA_REQUIRED_FIELD_NAME("device_id", deviceId, ruvia::string, RUVIA_CUSTOM("请选择关联设备", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(severity, ruvia::string, decltype(kDefaultSeverity), RUVIA_ONE_OF("severity 无效", "critical", "warning", "info")), RUVIA_REQUIRED_FIELD(conditions, ruvia::json_value), RUVIA_OPTIONAL_FIELD(logic, ruvia::string, decltype(kDefaultLogic), RUVIA_ONE_OF("logic 无效", "and", "or")), RUVIA_OPTIONAL_FIELD_NAME("silence_duration", silenceDuration, ruvia::int64, decltype(kDefaultSilenceDuration), RUVIA_MIN(0, "静默时长不能为负"), RUVIA_MAX(86400, "静默时长过长")), RUVIA_OPTIONAL_FIELD_NAME("recovery_condition", recoveryCondition, ruvia::string, decltype(kDefaultRecoveryCondition), RUVIA_MAX(32, "恢复条件过长")), RUVIA_OPTIONAL_FIELD_NAME("recovery_wait_seconds", recoveryWaitSeconds, ruvia::int64, decltype(kDefaultRecoveryWaitSeconds), RUVIA_MIN(0, "恢复等待不能为负"), RUVIA_MAX(86400, "恢复等待过长")), RUVIA_OPTIONAL_FIELD(status, ruvia::string, decltype(kDefaultStatus), RUVIA_ONE_OF("status 无效", "enabled", "disabled")), RUVIA_OPTIONAL_FIELD(remark, ruvia::string, decltype(kDefaultRemark), RUVIA_MAX(500, "备注过长")));
RUVIA_MODEL(TemplateInput, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_CUSTOM("模板名称无效", isAlertName)), RUVIA_OPTIONAL_FIELD(category, ruvia::string, decltype(kDefaultCategory), RUVIA_MAX(64, "分类过长")), RUVIA_OPTIONAL_FIELD(description, ruvia::string, decltype(kDefaultDescription), RUVIA_MAX(500, "描述过长")), RUVIA_OPTIONAL_FIELD(severity, ruvia::string, decltype(kDefaultSeverity), RUVIA_ONE_OF("severity 无效", "critical", "warning", "info")), RUVIA_REQUIRED_FIELD(conditions, ruvia::json_value), RUVIA_OPTIONAL_FIELD(logic, ruvia::string, decltype(kDefaultLogic), RUVIA_ONE_OF("logic 无效", "and", "or")), RUVIA_OPTIONAL_FIELD_NAME("silence_duration", silenceDuration, ruvia::int64, decltype(kDefaultSilenceDuration), RUVIA_MIN(0, "静默时长不能为负"), RUVIA_MAX(86400, "静默时长过长")), RUVIA_OPTIONAL_FIELD_NAME("recovery_condition", recoveryCondition, ruvia::string, decltype(kDefaultRecoveryCondition), RUVIA_MAX(32, "恢复条件过长")), RUVIA_OPTIONAL_FIELD_NAME("recovery_wait_seconds", recoveryWaitSeconds, ruvia::int64, decltype(kDefaultRecoveryWaitSeconds), RUVIA_MIN(0, "恢复等待不能为负"), RUVIA_MAX(86400, "恢复等待过长")), RUVIA_OPTIONAL_FIELD_NAME("applicable_protocols", applicableProtocols, ruvia::json_value), RUVIA_OPTIONAL_FIELD_NAME("protocol_config_id", protocolConfigId, ruvia::string, decltype(kDefaultProtocolConfigId), RUVIA_CUSTOM("协议配置无效", isAlertOptionalUuid)));

RUVIA_MODEL(AlertIdParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));
RUVIA_MODEL(AlertListQuery, RUVIA_OPTIONAL_FIELD(page, ruvia::int64, decltype(kDefaultPage), RUVIA_MIN(1, "page 必须大于 0")), RUVIA_OPTIONAL_FIELD(pageSize, ruvia::int64, decltype(kDefaultPageSize), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD(keyword, ruvia::string), RUVIA_OPTIONAL_FIELD(deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD(ruleId, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD(severity, ruvia::string), RUVIA_OPTIONAL_FIELD(category, ruvia::string));

// One serialized record in the records page row shape.
struct AlertRecordRow {
    std::string id;
    std::string json;
};

// Serialized records page and the rows it shows (live records channel).
struct AlertRecordPage {
    std::string json;
    std::vector<AlertRecordRow> rows;
};

} // namespace service::alert
