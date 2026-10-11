#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <ruvia/web/model.h>

#include "service/common/uuid.h"
#include "service/utils/expression.h"
#include "service/utils/json.h"

namespace service::common {

struct DerivedPoint {
    struct UnitRule {
        std::string condition;
        std::string unit;
    };

    std::string id;
    std::string name;
    std::string unit;
    std::string unitMode{ "fixed" };
    std::vector<UnitRule> unitRules;
    std::string sourceAlias;
    std::string kind;
    std::string expression;
    std::string valueType;
    std::vector<std::pair<std::string, std::string>> inputs;
    std::int64_t windowSeconds{};
    std::int64_t maxAgeSeconds{ 300 };
    bool visible{ true };
};

RUVIA_MODEL(DerivedPointInputConfig,
    RUVIA_OPTIONAL_FIELD(alias, ruvia::string),
    RUVIA_OPTIONAL_FIELD(pointId, ruvia::string));

RUVIA_MODEL(DerivedPointUnitRuleConfig,
    RUVIA_OPTIONAL_FIELD(condition, ruvia::string),
    RUVIA_OPTIONAL_FIELD(unit, ruvia::string));

RUVIA_MODEL(DerivedPointConfig,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(unit, ruvia::string),
    RUVIA_OPTIONAL_FIELD(unitMode, ruvia::string),
    RUVIA_OPTIONAL_FIELD(unitRules, ruvia::array<DerivedPointUnitRuleConfig>),
    RUVIA_OPTIONAL_FIELD(sourceAlias, ruvia::string),
    RUVIA_OPTIONAL_FIELD(kind, ruvia::string),
    RUVIA_OPTIONAL_FIELD(expression, ruvia::string),
    RUVIA_OPTIONAL_FIELD(valueType, ruvia::string),
    RUVIA_OPTIONAL_FIELD(inputs, ruvia::array<DerivedPointInputConfig>),
    RUVIA_OPTIONAL_FIELD(windowSeconds, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(maxAgeSeconds, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(visible, ruvia::bool_value));

// Shared configuration contract; no runtime state or I/O.
inline std::vector<DerivedPoint> decodeDerivedPoints(const ruvia::json_value& config) {
    const auto raw = config.get<ruvia::json_value>("derivedPoints");
    if (!raw) {
        return {};
    }
    if (!raw->is_array()) {
        throw std::invalid_argument("derivedPoints 必须是数组");
    }
    std::vector<DerivedPoint> points;
    const auto text = [](const auto& value, std::string_view key, std::size_t maximum, bool required = true) {
        if (!value && required) {
            throw std::invalid_argument("派生点字段无效: " + std::string(key));
        }
        if (!value) {
            return std::string{};
        }
        if (value->view().size() > maximum || (required && value->view().empty())) {
            throw std::invalid_argument("派生点字段无效: " + std::string(key));
        }
        return std::string(value->view());
    };
    const auto integer = [](const std::optional<ruvia::int64>& value, bool present, std::string_view key, std::int64_t fallback, std::int64_t minimum) {
        if (!present) {
            return fallback;
        }
        if (!value || value->value_ < minimum || value->value_ > 86400) {
            throw std::invalid_argument("派生点时间参数无效: " + std::string(key));
        }
        return value->value_;
    };
    std::set<std::string> ids;
    if (!raw->for_each_element([&](const ruvia::json_value& item) {
            if (!item.is_object() || points.size() == 32) {
                throw std::invalid_argument("派生点最多 32 个");
            }
            const auto parsed = item.get<DerivedPointConfig>();
            if (!parsed) {
                throw std::invalid_argument("派生点字段无效");
            }
            if (parsed->is_present<"unit">() && !parsed->get<"unit">()) {
                throw std::invalid_argument("派生点字段无效: unit");
            }
            if (parsed->is_present<"unitMode">() && !parsed->get<"unitMode">()) {
                throw std::invalid_argument("派生点字段无效: unitMode");
            }
            if (parsed->is_present<"expression">() && !parsed->get<"expression">()) {
                throw std::invalid_argument("派生点字段无效: expression");
            }
            if (parsed->is_present<"sourceAlias">() && !parsed->get<"sourceAlias">()) {
                throw std::invalid_argument("派生点字段无效: sourceAlias");
            }
            if (parsed->is_present<"visible">() && !parsed->get<"visible">()) {
                throw std::invalid_argument("visible 必须是布尔值");
            }
            DerivedPoint point;
            point.id = text(parsed->get<"id">(), "id", 36);
            if (!isUuid(point.id) || !ids.insert(point.id).second) {
                throw std::invalid_argument("派生点标识无效或重复");
            }
            point.name = text(parsed->get<"name">(), "name", 100);
            point.unit = text(parsed->get<"unit">(), "unit", 32, false);
            if (parsed->is_present<"unitMode">()) {
                point.unitMode = text(parsed->get<"unitMode">(), "unitMode", 16);
            }
            if (point.unitMode != "fixed" && point.unitMode != "conditional") {
                throw std::invalid_argument("单位模式无效");
            }
            point.kind = text(parsed->get<"kind">(), "kind", 16);
            point.valueType = text(parsed->get<"valueType">(), "valueType", 8);
            if (point.valueType != "number" && point.valueType != "boolean") {
                throw std::invalid_argument("派生点结果类型无效");
            }
            if (point.kind != "expression" && point.kind != "average" && point.kind != "minimum" && point.kind != "maximum") {
                throw std::invalid_argument("派生点计算类型无效");
            }
            point.expression = text(parsed->get<"expression">(), "expression", 512, point.kind == "expression");
            point.windowSeconds = integer(parsed->get<"windowSeconds">(), parsed->is_present<"windowSeconds">(), "windowSeconds", 0, 1);
            point.maxAgeSeconds = integer(parsed->get<"maxAgeSeconds">(), parsed->is_present<"maxAgeSeconds">(), "maxAgeSeconds", 300, 1);
            if (const auto& visible = parsed->get<"visible">()) {
                point.visible = static_cast<bool>(*visible);
            }
            if (point.kind != "expression" && (!point.windowSeconds || point.valueType != "number")) {
                throw std::invalid_argument("窗口派生点需要时间窗口和数值结果类型");
            }
            const auto& inputs = parsed->get<"inputs">();
            std::set<std::string> names;
            if (!inputs || inputs->empty()) {
                throw std::invalid_argument("派生点必须绑定输入点");
            }
            if (inputs->size() > 16) {
                throw std::invalid_argument("派生点输入绑定无效");
            }
            for (const auto& input : *inputs) {
                const auto alias = text(input.get<"alias">(), "alias", 32);
                const auto id = text(input.get<"pointId">(), "pointId", 36);
                const utils::Expression variable(alias);
                if (variable.variables() != std::vector<std::string>{ alias } || !isUuid(id) || !names.insert(alias).second) {
                    throw std::invalid_argument("派生点输入绑定无效");
                }
                point.inputs.emplace_back(alias, id);
            }
            if (point.kind == "expression") {
                const utils::Expression expression(point.expression);
                for (const auto& name : expression.variables()) {
                    if (!names.contains(name)) {
                        throw std::invalid_argument("公式包含未绑定变量: " + name);
                    }
                }
            } else {
                point.sourceAlias = text(parsed->get<"sourceAlias">(), "sourceAlias", 32);
                if (!names.contains(point.sourceAlias)) {
                    throw std::invalid_argument("窗口计算的输入点未绑定");
                }
            }
            if (parsed->is_present<"unitRules">()) {
                const auto& rules = parsed->get<"unitRules">();
                if (!rules) {
                    throw std::invalid_argument("单位条件必须是数组");
                }
                if (rules->size() > 8) {
                    throw std::invalid_argument("单位条件最多 8 条");
                }
                for (const auto& rule : *rules) {
                    auto condition = text(rule.get<"condition">(), "condition", 512);
                    auto unit = text(rule.get<"unit">(), "unit", 32, false);
                    const utils::Expression expression(condition);
                    for (const auto& name : expression.variables()) {
                        if (!names.contains(name)) {
                            throw std::invalid_argument("单位条件包含未绑定变量: " + name);
                        }
                    }
                    point.unitRules.push_back({ std::move(condition), std::move(unit) });
                }
            }
            if ((point.unitMode == "conditional") != !point.unitRules.empty()) {
                throw std::invalid_argument("条件单位必须设置规则，固定单位不得保留条件");
            }
            points.push_back(std::move(point));
            return true;
        })) {
        throw std::invalid_argument("derivedPoints 必须是数组");
    }
    return points;
}

inline std::vector<DerivedPoint> orderDerivedPoints(const ruvia::json_value& config) {
    auto points = decodeDerivedPoints(config);
    std::set<std::string> physical;
    const auto collect = [&](const ruvia::json_value& array) {
        (void)array.for_each_element([&](const ruvia::json_value& value) {
            if (const auto id = value.get<ruvia::string>("id")) {
                physical.emplace(id->view());
            }
            return true;
        });
    };
    for (auto name : { "registers", "areas", "points" }) {
        if (auto array = config.get<ruvia::json_value>(name)) {
            collect(*array);
        }
    }
    if (const auto functions = config.get<ruvia::json_value>("funcs")) {
        (void)functions->for_each_element([&](const ruvia::json_value& function) {
            const auto direction = function.get<ruvia::string>("dir");
            if (direction && direction->view() == "UP") {
                if (auto elements = function.get<ruvia::json_value>("elements")) {
                    collect(*elements);
                }
            }
            return true;
        });
    }
    std::map<std::string, std::size_t> index;
    for (std::size_t i = 0; i < points.size(); ++i) {
        if (physical.contains(points[i].id)) {
            throw std::invalid_argument("派生点标识不能与采集点重复");
        }
        index.emplace(points[i].id, i);
    }
    std::vector<DerivedPoint> ordered;
    std::vector<unsigned> visited(points.size());
    const auto visit = [&](auto&& self, std::size_t i) -> void {
        if (visited[i] == 2) {
            return;
        }
        if (visited[i] == 1) {
            throw std::invalid_argument("派生点存在循环依赖");
        }
        visited[i] = 1;
        for (const auto& [alias, id] : points[i].inputs) {
            const auto found = index.find(id);
            if (found != index.end()) {
                self(self, found->second);
            } else if (!physical.contains(id)) {
                throw std::invalid_argument("派生点引用的采集点不存在或不是上行点");
            }
        }
        visited[i] = 2;
        ordered.push_back(points[i]);
    };
    for (std::size_t i = 0; i < points.size(); ++i) {
        visit(visit, i);
    }
    if (const auto visibility = config.get<ruvia::json_value>("pointVisibility")) {
        if (!visibility->is_object() || !visibility->for_each_field([&](std::string_view id, const ruvia::json_value& raw) {
                return isUuid(id) && (raw.view() == "true" || raw.view() == "false");
            })) {
            throw std::invalid_argument("点位显示设置必须是标识到布尔值的映射");
        }
    }
    return ordered;
}
} // namespace service::common
