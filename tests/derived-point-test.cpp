#include <array>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string_view>

#include "service/features/telemetry/derived/derived.service.h"

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
        require(service::common::orderDerivedPoints(configuration).size() == 1, "configuration must decode and validate");
        const auto structuralError = [](std::string_view json) -> std::optional<service::common::DerivedPointError> {
            const auto value = ruvia::json_value::parse(json);
            require(value.is_object(), "fixture JSON must parse");
            try {
                (void)service::common::orderDerivedPoints(value);
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
        const auto points = service::common::orderDerivedPoints(ordered);
        require(points.size() == 2 && points[0].id == "a" && points[1].id == "b" && points[1].kind == "sum", "dependencies must be ordered first without business validation");
        require(points[0].sourceAlias == "x" && points[0].windowSeconds == 5 && points[0].maxAgeSeconds == 300 && points[0].unitMode == "fixed" && points[0].visible, "decoded defaults changed");
        const auto deviceDerived = ruvia::json_value::parse(R"json({"derivedPoints":[{"id":"combined","kind":"expression","valueType":"number","expression":"if(io, m * 2, 0)","maxAgeSeconds":5,"inputs":[{"alias":"m","pointId":"converted"},{"alias":"io","pointId":"input"}]}]})json");
        const std::array sources{
            ruvia::json_value::parse(R"json({"registers":[{"id":"register"}],"derivedPoints":[{"id":"converted","kind":"expression","valueType":"number","expression":"x + 1","inputs":[{"alias":"x","pointId":"register"}]}]})json"),
            ruvia::json_value::parse(R"json({"points":[{"id":"input"}]})json"),
        };
        const auto combined = service::common::orderDerivedPoints(deviceDerived, sources);
        require(combined.size() == 2 && combined[0].id == "converted" && combined[1].id == "combined", "cross-source dependencies must be ordered before device calculations");
        const std::array<std::string_view, 2> sourceTexts{ sources[0].view(), sources[1].view() };
        const auto prepared = DerivedService::prepare("device", deviceDerived.view(), sourceTexts);
        require(prepared && prepared->calculation.definitions().size() == 2 && prepared->inputIds.contains("input"), "device calculation must prepare all source definitions");
        const std::array<std::string_view, 2> changedSources{ sources[0].view(), R"json({"points":[{"id":"input","name":"new"}]})json" };
        const auto changed = DerivedService::prepare("device", deviceDerived.view(), changedSources);
        require(changed && changed->stateKey != prepared->stateKey, "changing a source must isolate prior calculation state");
        const auto historicalInput = [](std::string id, std::string values, std::int64_t at) {
            service::message::ParsedDeviceMessage value;
            value.messageId = value.acquisitionId = std::move(id);
            value.observedAtMs = at;
            value.occurredAtMs = 900000; // transport time must not expire historical inputs
            value.valuesJson = "{\"values\":" + values + '}';
            value.configurationDigest = "snapshot";
            return value;
        };
        std::vector<service::message::ParsedDeviceMessage> historical{
            historicalInput("a", R"({"register":{"value":12}})", 1000),
            historicalInput("c", R"({"input":{"value":1}})", 3000),
        };
        auto replayed = DerivedService::replayHistory(*prepared, historical);
        require(replayed.back().observedAtMs == 3000 && replayed.back().valuesJson.find("\"value\":26") != std::string::npos, "historical calculation uses event time, not reception time");
        const auto stableId = replayed.back().messageId;
        historical.insert(historical.begin() + 1, historicalInput("b", R"({"register":{"value":14}})", 2000));
        replayed = DerivedService::replayHistory(*prepared, historical);
        require(replayed.back().messageId == stableId && replayed.back().valuesJson.find("\"value\":30") != std::string::npos, "late input must repair later chained cross-source results with stable identities");
        require(replayed.back().historyOnly, "historical recomputation cannot update current values or alerts");
        const auto repeatedHistory = DerivedService::replayHistory(*prepared, historical);
        require(repeatedHistory.size() == replayed.size() && repeatedHistory.back().valuesJson == replayed.back().valuesJson, "history recomputation is independent of prior Redis state and process lifetime");
        State checkpointState;
        const std::vector<service::message::ParsedDeviceMessage> firstPage{ historical.front() };
        const auto firstHistory = DerivedService::replayHistory(*prepared, firstPage, &checkpointState);
        auto persistedState = StateRecord::decode(StateRecord::encode(checkpointState));
        const std::vector<service::message::ParsedDeviceMessage> nextPage(historical.begin() + 1, historical.end());
        const auto incrementalHistory = DerivedService::replayHistory(*prepared, nextPage, &persistedState, &firstPage.back());
        require(firstHistory.size() + incrementalHistory.size() == repeatedHistory.size() && incrementalHistory.back().valuesJson == repeatedHistory.back().valuesJson && incrementalHistory.back().messageId == repeatedHistory.back().messageId, "durable incremental calculation must match an ordered journal replay after restart");
        Calculation crossSource(combined);
        State crossState;
        auto crossValues = crossSource.advance(crossState, { { "register", { 12, "", 1000, "good" } } }, 1000);
        require(!crossValues.at("combined").value && crossValues.at("combined").quality == "missing", "a missing IO source must not produce a derived value");
        crossValues = crossSource.advance(crossState, { { "input", { 1, "", 2000, "good" } } }, 2000);
        require(crossValues.at("combined").value == 26 && crossValues.at("combined").quality == "good", "IO input must recompute the value using the latest protocol input");
        crossValues = crossSource.advance(crossState, {}, 6000);
        require(!crossValues.at("combined").value && crossValues.at("combined").quality == "stale", "an expired protocol input must invalidate the cross-source value");
        crossValues = crossSource.advance(crossState, { { "register", { 14, "", 7000, "good" } } }, 7000);
        require(!crossValues.at("combined").value && crossValues.at("combined").quality == "stale", "a stale IO input must not be used after a protocol update");
        crossValues = crossSource.advance(crossState, { { "input", { 0, "", 7100, "good" } } }, 7100);
        require(crossValues.at("combined").value == 0 && crossValues.at("combined").quality == "good", "a new IO input must recover the cross-source value");
        const std::array conflictingSources{
            ruvia::json_value::parse(R"json({"registers":[{"id":"same"}]})json"),
            ruvia::json_value::parse(R"json({"points":[{"id":"same"}]})json"),
        };
        try {
            (void)service::common::orderDerivedPoints(deviceDerived, conflictingSources);
            throw std::runtime_error("ambiguous source point must be rejected");
        } catch (const service::common::DerivedPointError& error) {
            require(error.code() == Code::sourceIdConflict && error.pointId() == "same", "ambiguous source point must identify the duplicate");
        }
        const std::array caseConflictingSources{
            ruvia::json_value::parse(R"json({"registers":[{"id":"aaaaaaaa-0000-4000-8000-000000000001"}]})json"),
            ruvia::json_value::parse(R"json({"points":[{"id":"AAAAAAAA-0000-4000-8000-000000000001"}]})json"),
        };
        try {
            (void)service::common::orderDerivedPoints(ruvia::json_value::parse("{}"), caseConflictingSources);
            throw std::runtime_error("UUID case variants across sources must be rejected");
        } catch (const service::common::DerivedPointError& error) {
            require(error.code() == Code::sourceIdConflict, "UUID case variants must identify a source conflict");
        }
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
