#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>

#include "service/features/observability/observability.types.h"

namespace service::observability {

class RuntimeDiagnostics final {
  public:
    void identifyWorker(std::size_t index) {
        workerIndex_ = std::to_string(index);
    }

    [[nodiscard]] std::string_view workerIndex() const noexcept { return workerIndex_; }

    [[nodiscard]] std::uint64_t revision() const {
        std::lock_guard lock(mutex_);
        return revision_;
    }

    void setComponentStatus(std::string name, ComponentState state, std::string detail = {}) {
        std::lock_guard lock(mutex_);
        const auto previous = components_.find(name);
        if (previous != components_.end() && previous->second.state == state && previous->second.detail == detail) {
            return;
        }
        ++revision_;
        components_.insert_or_assign(
            std::move(name),
            ComponentStatus{ state, std::move(detail), nowMilliseconds() }
        );
    }

    void incrementCounter(std::string_view name, std::uint64_t amount = 1) {
        std::lock_guard lock(mutex_);
        if (amount == 0) {
            return;
        }
        ++revision_;
        counters_[std::string(name)] += amount;
    }

    void setGauge(std::string_view name, std::int64_t value) {
        std::lock_guard lock(mutex_);
        const auto previous = gauges_.find(name);
        if (previous != gauges_.end() && previous->second == value) {
            return;
        }
        ++revision_;
        gauges_.insert_or_assign(std::string(name), value);
    }

    void setCounter(std::string_view name, std::uint64_t value) {
        std::lock_guard lock(mutex_);
        const auto previous = counters_.find(name);
        if (previous != counters_.end() && previous->second == value) {
            return;
        }
        ++revision_;
        counters_.insert_or_assign(std::string(name), value);
    }

    bool setAlertState(std::string name, bool active, std::string detail = {}) {
        std::lock_guard lock(mutex_);
        const auto current = alerts_.find(name);
        if (current != alerts_.end() && current->second.active == active) {
            if (current->second.detail != detail) {
                ++revision_;
            }
            current->second.detail = std::move(detail);
            return false;
        }
        ++revision_;
        alerts_.insert_or_assign(
            std::move(name),
            AlertStatus{ active, std::move(detail), nowMilliseconds() }
        );
        return true;
    }

    [[nodiscard]] bool areComponentsReady() const {
        std::lock_guard lock(mutex_);
        if (components_.empty()) {
            return false;
        }
        for (const auto& [_, component] : components_) {
            if (component.state != ComponentState::Ready) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] std::string healthJson() const {
        std::lock_guard lock(mutex_);
        std::ostringstream output;
        output << "{\"status\":\""
               << (!areAllComponentsReadyLocked() ? "not_ready" : hasActiveAlertLocked() ? "degraded"
                                                                                         : "ready")
               << "\",\"components\":{";
        bool first = true;
        for (const auto& [name, component] : components_) {
            if (!first) {
                output << ',';
            }
            first = false;
            output << '\"' << escape(name) << "\":{\"state\":\""
                   << stateName(component.state) << "\",\"detail\":\""
                   << escape(component.detail) << "\",\"changed_at_ms\":"
                   << component.changedAtMs << '}';
        }
        output << "},\"alerts\":{";
        first = true;
        for (const auto& [name, alert] : alerts_) {
            if (!first) {
                output << ',';
            }
            first = false;
            output << '\"' << escape(name) << "\":{\"active\":"
                   << (alert.active ? "true" : "false") << ",\"detail\":\""
                   << escape(alert.detail) << "\",\"changed_at_ms\":"
                   << alert.changedAtMs << '}';
        }
        output << "}}";
        return output.str();
    }

    [[nodiscard]] std::string prometheus() const {
        std::lock_guard lock(mutex_);
        std::ostringstream output;
        output << "# TYPE iot_engine_ready gauge\n"
               << "iot_engine_ready " << (areAllComponentsReadyLocked() ? 1 : 0) << "\n";
        output << "# TYPE iot_engine_component_ready gauge\n";
        for (const auto& [name, component] : components_) {
            output << "iot_engine_component_ready{component=\"" << escape(name) << "\"} "
                   << (component.state == ComponentState::Ready ? 1 : 0) << "\n";
        }
        output << "# TYPE iot_engine_alert_active gauge\n";
        std::size_t activeAlerts = 0;
        for (const auto& [name, alert] : alerts_) {
            output << "iot_engine_alert_active{alert=\"" << escape(name) << "\"} "
                   << (alert.active ? 1 : 0) << "\n";
            activeAlerts += alert.active ? 1 : 0;
        }
        output << "# TYPE iot_engine_active_alerts gauge\n"
               << "iot_engine_active_alerts " << activeAlerts << "\n";
        for (const auto& [name, value] : counters_) {
            output << sanitizeMetric(name) << " " << value << "\n";
        }
        for (const auto& [name, value] : gauges_) {
            output << sanitizeMetric(name) << " " << value << "\n";
        }
        const auto raw = output.str();
        if (workerIndex_.empty()) {
            return raw;
        }
        std::istringstream lines(raw);
        std::ostringstream labelled;
        for (std::string line; std::getline(lines, line);) {
            if (!line.empty() && line.front() != '#') {
                const auto space = line.find(' ');
                const auto brace = line.find('{');
                if (brace != std::string::npos && brace < space) {
                    line.insert(brace + 1, "worker=\"" + workerIndex_ + "\",");
                } else if (space != std::string::npos) {
                    line.insert(space, "{worker=\"" + workerIndex_ + "\"}");
                }
            }
            labelled << line << '\n';
        }
        return labelled.str();
    }

  private:
    std::string workerIndex_;
    std::uint64_t revision_ = 0;

    static std::int64_t nowMilliseconds() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch()
        )
            .count();
    }

    [[nodiscard]] bool areAllComponentsReadyLocked() const {
        if (components_.empty()) {
            return false;
        }
        for (const auto& [_, component] : components_) {
            if (component.state != ComponentState::Ready) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool hasActiveAlertLocked() const {
        for (const auto& [_, alert] : alerts_) {
            if (alert.active) {
                return true;
            }
        }
        return false;
    }

    static std::string_view stateName(ComponentState state) {
        switch (state) {
            case ComponentState::Stopped:
                return "stopped";
            case ComponentState::Starting:
                return "starting";
            case ComponentState::Ready:
                return "ready";
            case ComponentState::Failed:
                return "failed";
        }
        return "unknown";
    }

    static std::string escape(std::string_view input) {
        std::string output;
        output.reserve(input.size());
        for (const char ch : input) {
            if (ch == '\\' || ch == '\"') {
                output.push_back('\\');
            }
            if (ch == '\n') {
                output += "\\n";
                continue;
            }
            output.push_back(ch);
        }
        return output;
    }

    static std::string sanitizeMetric(std::string_view input) {
        std::string output;
        output.reserve(input.size());
        for (const char ch : input) {
            output.push_back((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == ':' ? ch : '_');
        }
        return output;
    }

    mutable std::mutex mutex_;
    std::map<std::string, ComponentStatus, std::less<>> components_;
    std::map<std::string, std::uint64_t, std::less<>> counters_;
    std::map<std::string, std::int64_t, std::less<>> gauges_;
    std::map<std::string, AlertStatus, std::less<>> alerts_;
};

} // namespace service::observability
