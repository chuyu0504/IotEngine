#pragma once

#include <charconv>
#include <deque>
#include <map>
#include <optional>
#include <set>
#include <string>

#include "service/common/derived_point.h"
#include "service/utils/expression.h"
#include "service/utils/json.h"

namespace service::telemetry::derived {

struct Sample {
    std::optional<double> value;
    std::string unit;
    std::int64_t at{};
    std::string quality{ "missing" };
};

struct Window {
    std::deque<std::pair<std::int64_t, double>> samples;
    std::string unit;
    std::int64_t overflowUntil{};
};

struct State {
    std::map<std::string, Sample> latest;
    std::map<std::string, Window> windows;
};

// Pure calculation, used with one immutable input snapshot and configuration.
class Calculation final {
  public:
    explicit Calculation(std::vector<common::DerivedPoint> definitions) : definitions_(std::move(definitions)) {}

    std::map<std::string, Sample> advance(State& state, const std::map<std::string, Sample>& incoming, std::int64_t now) const {
        const auto windowCount = std::count_if(definitions_.begin(), definitions_.end(), [](const auto& point) {
            return point.kind != "expression";
        });
        const std::size_t windowCapacity = 4096 / (std::max)(std::ptrdiff_t{ 1 }, windowCount);
        std::set<std::string> updated;
        for (const auto& [id, sample] : incoming) {
            const auto found = state.latest.find(id);
            // Replays and older observations cannot change a completed window.
            if (found != state.latest.end() && sample.at <= found->second.at) {
                continue;
            }
            state.latest[id] = sample;
            updated.insert(id);
        }
        std::map<std::string, Sample> outputs;
        for (const auto& definition : definitions_) {
            const auto previous = state.latest.find(definition.id);
            Sample result;
            result.at = now;
            result.unit = definition.unit;
            const auto resolve = [&](std::string_view alias) -> double {
                const auto binding = std::find_if(definition.inputs.begin(), definition.inputs.end(), [&](const auto& value) {
                    return value.first == alias;
                });
                if (binding == definition.inputs.end()) {
                    throw std::domain_error("unbound");
                }
                const auto found = state.latest.find(binding->second);
                if (found == state.latest.end() || !found->second.value || found->second.quality != "good") {
                    throw std::domain_error("missing");
                }
                if (found->second.at > now || now - found->second.at >= definition.maxAgeSeconds * 1000) {
                    throw std::domain_error("stale");
                }
                return *found->second.value;
            };
            bool changed = previous == state.latest.end();
            for (const auto& [alias, id] : definition.inputs) {
                changed = changed || updated.contains(id);
            }
            if (previous != state.latest.end()) {
                for (const auto& [alias, id] : definition.inputs) {
                    const auto input = state.latest.find(id);
                    if (input != state.latest.end() && input->second.at + definition.maxAgeSeconds * 1000 <= now && previous->second.quality == "good") {
                        changed = true;
                    }
                }
            }
            if (definition.kind != "expression") {
                const auto source = std::find_if(definition.inputs.begin(), definition.inputs.end(), [&](const auto& value) {
                    return value.first == definition.sourceAlias;
                });
                auto& window = state.windows[definition.id];
                const auto sourceValue = source == definition.inputs.end() ? state.latest.end() : state.latest.find(source->second);
                if (sourceValue != state.latest.end() && updated.contains(source->second) && sourceValue->second.value && sourceValue->second.quality == "good") {
                    if (window.unit != sourceValue->second.unit) {
                        window = {};
                        window.unit = sourceValue->second.unit;
                    }
                    const auto at = sourceValue->second.at;
                    while (!window.samples.empty() && window.samples.front().first <= now - definition.windowSeconds * 1000) {
                        window.samples.pop_front();
                    }
                    if (window.samples.size() == windowCapacity) {
                        window.overflowUntil = window.samples.front().first + definition.windowSeconds * 1000;
                        window.samples.pop_front();
                    }
                    window.samples.emplace_back(at, *sourceValue->second.value);
                }
                while (!window.samples.empty() && window.samples.front().first <= now - definition.windowSeconds * 1000) {
                    window.samples.pop_front();
                    changed = true;
                }
            }
            if (!changed) {
                continue;
            }
            try {
                // Check every declared dependency, including unit-only inputs.
                for (const auto& [alias, id] : definition.inputs) {
                    (void)resolve(alias);
                }
                for (const auto& rule : definition.unitRules) {
                    if (utils::Expression(rule.condition).evaluate(resolve) != 0) {
                        result.unit = rule.unit;
                        break;
                    }
                }
                double value{};
                if (definition.kind == "expression") {
                    value = utils::Expression(definition.expression).evaluate(resolve);
                } else {
                    const auto& window = state.windows.at(definition.id);
                    if (window.overflowUntil > now) {
                        throw std::domain_error("capacity_exceeded");
                    }
                    if (window.samples.empty()) {
                        throw std::domain_error("missing");
                    }
                    value = window.samples.front().second;
                    if (definition.kind == "average") {
                        // Incremental mean avoids overflowing a finite-value sum.
                        long double mean{};
                        std::size_t count{};
                        for (const auto& [at, number] : window.samples) {
                            ++count;
                            mean += (static_cast<long double>(number) - mean) / count;
                        }
                        value = static_cast<double>(mean);
                    } else {
                        for (const auto& [at, number] : window.samples) {
                            value = definition.kind == "minimum" ? (std::min)(value, number) : (std::max)(value, number);
                        }
                    }
                }
                if (!std::isfinite(value)) {
                    throw std::domain_error("invalid");
                }
                result.value = definition.valueType == "boolean" ? double(value != 0) : value;
                result.quality = "good";
            } catch (const std::domain_error& error) {
                const std::string_view reason(error.what());
                result.quality = reason == "missing" || reason == "stale" || reason == "capacity_exceeded" ? reason : "invalid";
                // Do not display a guessed unit when its inputs are invalid.
                if (definition.unitMode == "conditional") {
                    result.unit.clear();
                }
            }
            state.latest[definition.id] = result;
            outputs[definition.id] = result;
            updated.insert(definition.id);
        }
        return outputs;
    }

    std::optional<std::int64_t> nextDeadline(const State& state, std::int64_t now) const {
        std::optional<std::int64_t> deadline;
        const auto add = [&](std::int64_t value) {
            if (value > now && (!deadline || value < *deadline)) {
                deadline = value;
            }
        };
        for (const auto& definition : definitions_) {
            for (const auto& [alias, id] : definition.inputs) {
                if (const auto found = state.latest.find(id); found != state.latest.end()) {
                    add(found->second.at + definition.maxAgeSeconds * 1000);
                }
            }
            if (const auto window = state.windows.find(definition.id); window != state.windows.end() && !window->second.samples.empty()) {
                add(window->second.samples.front().first + definition.windowSeconds * 1000);
            }
        }
        return deadline;
    }

    static std::string pointJson(const common::DerivedPoint& definition, const Sample& sample) {
        std::string value = "null";
        if (sample.value) {
            if (definition.valueType == "boolean") {
                value = *sample.value != 0 ? "true" : "false";
            } else {
                char buffer[64];
                const auto result = std::to_chars(buffer, buffer + sizeof(buffer), *sample.value);
                if (result.ec != std::errc{}) {
                    throw std::runtime_error("cannot encode derived value");
                }
                value.assign(buffer, result.ptr);
            }
        }
        return "{\"name\":" + utils::jsonQuoted(definition.name) + ",\"value\":" + value +
            ",\"unit\":" + utils::jsonQuoted(sample.unit) + ",\"quality\":" + utils::jsonQuoted(sample.quality) +
            ",\"sample_time_ms\":" + std::to_string(sample.at) + ",\"derived\":true,\"visible\":" + (definition.visible ? "true" : "false") + "}";
    }

  private:
    std::vector<common::DerivedPoint> definitions_;
};
} // namespace service::telemetry::derived
