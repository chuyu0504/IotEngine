#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/model.h>
#include <ruvia/web/model_object.h>

namespace service::common {

// 派生点配置契约：结构解码与依赖排序，不含字段业务校验（由写入方 protocol 模块负责）。
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

enum class DerivedPointErrorCode {
    malformed,          // derivedPoints 不是派生点对象数组
    duplicateId,        // 派生点标识重复
    physicalIdConflict, // 派生点标识与采集点重复
    unknownInput,       // 输入既不是派生点也不是采集点（含非上行点）
    cycle,              // 派生点之间存在循环依赖
};

class DerivedPointError final : public std::runtime_error {
  public:
    DerivedPointError(DerivedPointErrorCode code, std::string pointId)
        : std::runtime_error(describe(code, pointId)), code_(code), pointId_(std::move(pointId)) {}

    DerivedPointErrorCode code() const noexcept {
        return code_;
    }

    // 出错的派生点标识；malformed 时为空。
    const std::string& pointId() const noexcept {
        return pointId_;
    }

  private:
    static std::string describe(DerivedPointErrorCode code, std::string_view pointId) {
        std::string_view reason;
        switch (code) {
            case DerivedPointErrorCode::malformed:
                reason = "malformed derived points";
                break;
            case DerivedPointErrorCode::duplicateId:
                reason = "duplicate derived point id";
                break;
            case DerivedPointErrorCode::physicalIdConflict:
                reason = "derived point id conflicts with a collected point";
                break;
            case DerivedPointErrorCode::unknownInput:
                reason = "derived point input is not an uplink point";
                break;
            case DerivedPointErrorCode::cycle:
                reason = "derived point dependency cycle";
                break;
        }
        return pointId.empty() ? std::string(reason) : std::string(reason) + ": " + std::string(pointId);
    }

    DerivedPointErrorCode code_;
    std::string pointId_;
};

inline std::vector<DerivedPoint> decodeDerivedPoints(const ruvia::json_value& config) {
    const auto raw = config.get<ruvia::json_value>("derivedPoints");
    if (!raw) {
        return {};
    }
    const auto decoded = raw->get<ruvia::array<DerivedPointConfig>>();
    if (!decoded) {
        throw DerivedPointError(DerivedPointErrorCode::malformed, {});
    }
    const auto text = [](const std::optional<ruvia::string>& value, std::string_view fallback = {}) {
        return value ? std::string(value->view()) : std::string(fallback);
    };
    std::vector<DerivedPoint> points;
    points.reserve(decoded->size());
    for (const auto& item : *decoded) {
        DerivedPoint point;
        point.id = text(item.get<"id">());
        point.name = text(item.get<"name">());
        point.unit = text(item.get<"unit">());
        point.unitMode = text(item.get<"unitMode">(), "fixed");
        point.kind = text(item.get<"kind">());
        point.valueType = text(item.get<"valueType">());
        point.expression = text(item.get<"expression">());
        // sourceAlias 只对窗口计算有意义，公式派生点不携带。
        if (point.kind != "expression") {
            point.sourceAlias = text(item.get<"sourceAlias">());
        }
        point.windowSeconds = item.get<"windowSeconds">().value_or(ruvia::int64{ 0 }).value_;
        point.maxAgeSeconds = item.get<"maxAgeSeconds">().value_or(ruvia::int64{ 300 }).value_;
        point.visible = static_cast<bool>(item.get<"visible">().value_or(ruvia::bool_value{ true }));
        if (const auto& inputs = item.get<"inputs">()) {
            for (const auto& input : *inputs) {
                point.inputs.emplace_back(text(input.get<"alias">()), text(input.get<"pointId">()));
            }
        }
        if (const auto& rules = item.get<"unitRules">()) {
            for (const auto& rule : *rules) {
                point.unitRules.push_back({ text(rule.get<"condition">()), text(rule.get<"unit">()) });
            }
        }
        points.push_back(std::move(point));
    }
    return points;
}

// 按依赖拓扑排序：输入为派生点时先计算其依赖；采集点取 registers/areas/points 及 SL651 上行要素。
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
            throw DerivedPointError(DerivedPointErrorCode::physicalIdConflict, points[i].id);
        }
        if (!index.emplace(points[i].id, i).second) {
            throw DerivedPointError(DerivedPointErrorCode::duplicateId, points[i].id);
        }
    }
    std::vector<DerivedPoint> ordered;
    ordered.reserve(points.size());
    std::vector<unsigned> visited(points.size());
    const auto visit = [&](auto&& self, std::size_t i) -> void {
        if (visited[i] == 2) {
            return;
        }
        if (visited[i] == 1) {
            throw DerivedPointError(DerivedPointErrorCode::cycle, points[i].id);
        }
        visited[i] = 1;
        for (const auto& [alias, id] : points[i].inputs) {
            const auto found = index.find(id);
            if (found != index.end()) {
                self(self, found->second);
            } else if (!physical.contains(id)) {
                throw DerivedPointError(DerivedPointErrorCode::unknownInput, points[i].id);
            }
        }
        visited[i] = 2;
        ordered.push_back(points[i]);
    };
    for (std::size_t i = 0; i < points.size(); ++i) {
        visit(visit, i);
    }
    return ordered;
}

} // namespace service::common
