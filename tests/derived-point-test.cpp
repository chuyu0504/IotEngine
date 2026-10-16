#include <iostream>
#include <optional>
#include <stdexcept>
#include <string_view>

#include "service/features/telemetry/derived/derived.entity.h"

using namespace service::telemetry::derived;

void require(bool value, const char* reason) {
    if (!value) {
        throw std::runtime_error(reason);
    }
}

int main() {
    try {
        using service::utils::Expression;
        const auto configuration = ruvia::json_value::parse(R"json({"registers":[{"id":"00000000-0000-4000-8000-000000000001"}],"derivedPoints":[{"id":"00000000-0000-4000-8000-000000000002","name":"pressure","kind":"expression","valueType":"number","expression":"if(p > 100, p / 1000, p)","inputs":[{"alias":"p","pointId":"00000000-0000-4000-8000-000000000001"}],"unit":"kPa","unitMode":"conditional","unitRules":[{"condition":"p > 100","unit":"MPa"}],"maxAgeSeconds":10,"visible":true}],"pointVisibility":{"00000000-0000-4000-8000-000000000001":false}})json");
        require(configuration && service::common::orderDerivedPoints(*configuration).size() == 1, "configuration must decode and validate");
        const auto structuralError = [](std::string_view json) -> std::optional<service::common::DerivedPointError> {
            const auto value = ruvia::json_value::parse(json);
            require(value.has_value(), "fixture JSON must parse");
            try {
                (void)service::common::orderDerivedPoints(*value);
            } catch (const service::common::DerivedPointError& error) {
                return error;
            }
            return std::nullopt;
        };
        using Code = service::common::DerivedPointErrorCode;
        const auto cycle = structuralError(R"json({"derivedPoints":[{"id":"a","kind":"expression","inputs":[{"alias":"x","pointId":"b"}]},{"id":"b","kind":"expression","inputs":[{"alias":"y","pointId":"a"}]}]})json");
        require(cycle && cycle->code() == Code::cycle && cycle->pointId() == "a", "dependency cycle must report the revisited point");
        const auto unknown = structuralError(R"json({"registers":[{"id":"r"}],"derivedPoints":[{"id":"a","inputs":[{"alias":"x","pointId":"missing"}]}]})json");
        require(unknown && unknown->code() == Code::unknownInput && unknown->pointId() == "a", "unknown input must be typed");
        const auto downlink = structuralError(R"json({"funcs":[{"dir":"DOWN","elements":[{"id":"r"}]}],"derivedPoints":[{"id":"a","inputs":[{"alias":"x","pointId":"r"}]}]})json");
        require(downlink && downlink->code() == Code::unknownInput, "downlink elements are not inputs");
        const auto conflict = structuralError(R"json({"areas":[{"id":"a"}],"derivedPoints":[{"id":"a","inputs":[]}]})json");
        require(conflict && conflict->code() == Code::physicalIdConflict && conflict->pointId() == "a", "physical id conflict must be typed");
        const auto duplicate = structuralError(R"json({"derivedPoints":[{"id":"a"},{"id":"a"}]})json");
        require(duplicate && duplicate->code() == Code::duplicateId && duplicate->pointId() == "a", "duplicate id must be typed");
        const auto malformed = structuralError(R"json({"derivedPoints":[{"id":5}]})json");
        require(malformed && malformed->code() == Code::malformed && malformed->pointId().empty(), "malformed derived points must be typed");
        // 结构排序不做业务校验：字段取值由 protocol 写入时校验，这里只解码。
        const auto ordered = ruvia::json_value::parse(R"json({"points":[{"id":"p"}],"derivedPoints":[{"id":"b","kind":"sum","expression":"x","inputs":[{"alias":"x","pointId":"a"}]},{"id":"a","kind":"average","sourceAlias":"x","windowSeconds":5,"inputs":[{"alias":"x","pointId":"p"}]}]})json");
        const auto points = service::common::orderDerivedPoints(*ordered);
        require(points.size() == 2 && points[0].id == "a" && points[1].id == "b" && points[1].kind == "sum", "dependencies must be ordered first without business validation");
        require(points[0].sourceAlias == "x" && points[0].windowSeconds == 5 && points[0].maxAgeSeconds == 300 && points[0].unitMode == "fixed" && points[0].visible, "decoded defaults changed");
        require(Expression("if(x > 10, x * 2, 1 / 0)").evaluate([](auto) {
            return 12;
        }) == 24,
                "conditional must short circuit");
        require(Expression("2 + 3 * 4 == 14 && !false").evaluate([](auto) {
            return 0;
        }) == 1,
                "precedence");
        bool failed = false;
        try {
            (void)Expression("1 / 0").evaluate([](auto) {
                return 0;
            });
        } catch (const std::domain_error&) {
            failed = true;
        }
        require(failed, "division by zero must fail");
        service::common::DerivedPoint point;
        point.id = "derived";
        point.name = "压力";
        point.kind = "expression";
        point.valueType = "number";
        point.expression = "if(mode == 1, p / 1000, p)";
        point.inputs = { { "p", "pressure" }, { "mode", "mode" } };
        point.unitMode = "conditional";
        point.unit = "kPa";
        point.unitRules = { { "mode == 1", "MPa" } };
        point.maxAgeSeconds = 5;
        Calculation expression({ point });
        State state;
        auto values = expression.advance(state, { { "pressure", { 2500, "kPa", 1000, "good" } }, { "mode", { 0, "", 1000, "good" } } }, 1000);
        require(values.at("derived").value == 2500 && values.at("derived").unit == "kPa", "default unit");
        values = expression.advance(state, { { "mode", { 1, "", 2000, "good" } } }, 2000);
        require(values.at("derived").value == 2.5 && values.at("derived").unit == "MPa", "unit-only dependency must recompute value and unit atomically");
        require(Calculation::pointJson(point, values.at("derived")).find("\"unit\":\"MPa\"") != std::string::npos, "result must retain unit");
        require(expression.advance(state, { { "mode", { 0, "", 1000, "good" } } }, 2000).empty(), "late input must not regress result");
        values = expression.advance(state, {}, 6000);
        require(!values.at("derived").value && values.at("derived").unit.empty() && values.at("derived").quality == "stale", "stale unit must not be guessed");
        point.kind = "average";
        point.sourceAlias = "p";
        point.windowSeconds = 10;
        point.maxAgeSeconds = 30;
        point.unitMode = "fixed";
        point.unitRules.clear();
        point.inputs = { { "p", "pressure" } };
        Calculation average({ point });
        State window;
        average.advance(window, { { "pressure", { 10, "kPa", 1000, "good" } } }, 1000);
        average.advance(window, { { "pressure", { 10, "kPa", 2000, "good" } } }, 2000);
        values = average.advance(window, { { "pressure", { 40, "kPa", 3000, "good" } } }, 3000);
        require(values.at("derived").value == 20, "unchanged samples still count in arithmetic mean");
        values = average.advance(window, {}, 11000);
        require(values.at("derived").value == 25, "window expires without new sample");
        values = average.advance(window, { { "pressure", { 2, "MPa", 12000, "good" } } }, 12000);
        require(values.at("derived").value == 2, "different source units must not mix in window");
        const auto restored = StateRecord::decode(StateRecord::encode(window));
        require(restored.windows.at("derived").samples == window.windows.at("derived").samples && restored.latest.at("derived").unit == "kPa", "state persistence retains window and unit");
        require(average.nextDeadline(window, 12000) == 22000, "next expiry must be event based");
        for (const auto kind : { "minimum", "maximum" }) {
            point.kind = kind;
            Calculation extrema({ point });
            State samples;
            extrema.advance(samples, { { "pressure", { 10, "", 1000, "good" } } }, 1000);
            values = extrema.advance(samples, { { "pressure", { 40, "", 2000, "good" } } }, 2000);
            require(values.at("derived").value == (point.kind == "minimum" ? 10 : 40), "window extrema");
        }
        std::cout << "派生公式、窗口、条件单位和数据有效性检查通过\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
