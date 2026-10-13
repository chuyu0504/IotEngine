#pragma once

#include "service/features/telemetry/derived/derived.protocol.h"
#include "service/utils/crypto.h"

namespace service::telemetry::derived {
// Dynamic device/configuration keys and a CAS transition are not expressible by
// Redis ORM. The stored value is the complete bounded calculation state.
struct StateRecord final {
    static std::string key(std::string_view deviceId, std::string_view configuration) {
        // 时间基准升级后不复用按接收时间保存的输入；原状态保留原有 TTL 自动回收。
        return "iot:derived:state:" + std::string(deviceId) + ':' + utils::sha256("event-time-v1:" + std::string(configuration));
    }

    static std::string encode(const State& state) {
        std::string result = "{\"latest\":{";
        bool first = true;
        common::DerivedPoint definition;
        definition.valueType = "number";
        for (const auto& [id, sample] : state.latest) {
            if (!first) {
                result += ',';
            }
            first = false;
            result += utils::jsonQuoted(id) + ':' + Calculation::pointJson(definition, sample);
        }
        result += "},\"windows\":{";
        first = true;
        for (const auto& [id, window] : state.windows) {
            if (!first) {
                result += ',';
            }
            first = false;
            result += utils::jsonQuoted(id) + ":{\"unit\":" + utils::jsonQuoted(window.unit) + ",\"overflowUntil\":" + std::to_string(window.overflowUntil) + ",\"samples\":[";
            bool firstSample = true;
            for (const auto& [at, value] : window.samples) {
                if (!firstSample) {
                    result += ',';
                }
                firstSample = false;
                result += Calculation::pointJson(definition, Sample{ value, "", at, "good" });
            }
            result += "]}";
        }
        return result + "}}";
    }

    static Sample sample(const ruvia::json_value& value, std::int64_t timestamp) {
        Sample result;
        result.at = timestamp;
        if (const auto at = value.get<ruvia::int64>("sample_time_ms")) {
            result.at = at->value_;
        }
        if (const auto number = value.get<ruvia::double_value>("value"); number && std::isfinite(number->value_)) {
            result.value = number->value_;
        } else if (const auto boolean = value.get<ruvia::bool_value>("value")) {
            result.value = static_cast<bool>(*boolean) ? 1 : 0;
        }
        if (const auto unit = value.get<ruvia::string>("unit")) {
            result.unit = unit->view();
        }
        result.quality = result.value ? "good" : "missing";
        if (const auto quality = value.get<ruvia::string>("quality")) {
            result.quality = quality->view();
        }
        return result;
    }

    static State decode(std::string_view wire) {
        State state;
        if (wire.empty()) {
            return state;
        }
        const auto value = ruvia::json_value::parse(wire);
        if (!value) {
            throw std::runtime_error("invalid derived state");
        }
        if (const auto latest = value->get<ruvia::json_value>("latest")) {
            (void)latest->for_each_field([&](std::string_view id, const ruvia::json_value& raw) {
                state.latest[std::string(id)] = sample(raw, 0);
                return true;
            });
        }
        if (const auto windows = value->get<ruvia::json_value>("windows")) {
            (void)windows->for_each_field([&](std::string_view id, const ruvia::json_value& item) {
                auto& window = state.windows[std::string(id)];
                if (const auto unit = item.get<ruvia::string>("unit")) {
                    window.unit = unit->view();
                }
                if (const auto overflow = item.get<ruvia::int64>("overflowUntil")) {
                    window.overflowUntil = overflow->value_;
                }
                if (const auto samples = item.get<ruvia::json_value>("samples")) {
                    (void)samples->for_each_element([&](const ruvia::json_value& point) {
                        const auto parsed = sample(point, 0);
                        if (parsed.value) {
                            window.samples.emplace_back(parsed.at, *parsed.value);
                        }
                        if (window.samples.size() > 4096) {
                            throw std::runtime_error("derived window capacity exceeded");
                        }
                        return true;
                    });
                }
                return true;
            });
        }
        return state;
    }
};
} // namespace service::telemetry::derived
