#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/context.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/redis/redis_repository.h>

#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/modules/system/operations/operations.entity.h"
#include "service/modules/system/operations/operations.types.h"

namespace service::system {

class OperationsService final {
  public:
    static ruvia::task<OperationsReadiness> readiness(ruvia::context& context) {
        const auto expectedWorkers = workerCount(context);
        bool databaseReady = false;
        bool redisReady = false;
        try {
            ruvia::db_query query(context.pool());
            query.select(query.value(1));
            const auto queryRows = co_await context.db().query(query);
            databaseReady = !queryRows.empty();
            const std::vector<std::string_view> command{ "PING" };
            const auto reply = co_await context.redis().command(command);
            redisReady = reply.kind() == ruvia::redis_value::kind_type::string &&
                reply.string() == "PONG";
        } catch (...) {
        }

        std::vector<WorkerDiagnosticSnapshot> snapshots;
        bool snapshotsAvailable = expectedWorkers != 0;
        if (snapshotsAvailable) {
            try {
                snapshots = co_await loadSnapshots(context, expectedWorkers);
            } catch (...) {
                snapshotsAvailable = false;
            }
        }

        bool workersReady = snapshotsAvailable && snapshots.size() == expectedWorkers;
        bool degraded = false;
        for (const auto& snapshot : snapshots) {
            if (!snapshot.metricsPresent || !snapshot.readinessPresent || !snapshot.ready ||
                snapshot.healthJson.empty()) {
                workersReady = false;
            }
            if (snapshot.healthJson.find("\"status\":\"degraded\"") != std::string::npos) {
                degraded = true;
            }
        }

        const bool ready = databaseReady && redisReady && workersReady;
        const auto status = !ready ? std::string_view{ "not_ready" }
            : degraded             ? std::string_view{ "degraded" }
                                   : std::string_view{ "ready" };
        co_return OperationsReadiness{ ready, aggregateHealthJson(snapshots, status) };
    }

    static ruvia::task<std::string> metrics(ruvia::context& context) {
        const auto expectedWorkers = workerCount(context);
        if (expectedWorkers == 0) {
            co_return fallbackMetrics();
        }
        try {
            const auto snapshots = co_await loadSnapshots(context, expectedWorkers);
            std::set<std::string, std::less<>> typeLines;
            std::string output;
            for (const auto& snapshot : snapshots) {
                if (!snapshot.metricsPresent) {
                    continue;
                }
                std::istringstream lines(snapshot.metrics);
                for (std::string line; std::getline(lines, line);) {
                    if (line.starts_with("# TYPE ") && !typeLines.emplace(line).second) {
                        continue;
                    }
                    output.append(line);
                    output.push_back('\n');
                }
            }
            co_return output.empty() ? fallbackMetrics() : std::move(output);
        } catch (...) {
            co_return fallbackMetrics();
        }
    }

  private:
    static std::size_t workerCount(const ruvia::context& context) noexcept {
        try {
            const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>();
            return identity ? identity->count : 0;
        } catch (const std::logic_error&) {
            return 0;
        }
    }

    static ruvia::task<std::vector<WorkerDiagnosticSnapshot>>
    loadSnapshots(ruvia::context& context, std::size_t count) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        auto snapshots = context.redis().get_repository<entities::WorkerSnapshotEntity>();
        std::vector<WorkerDiagnosticSnapshot> result(count);
        for (std::size_t index = 0; index < count; ++index) {
            const ruvia::redis_find_options options{
                .where_ = entities::WorkerSnapshotEntity::field<"id">() ==
                    service::message::worker_metrics::snapshotId(index, identity.instanceId)
            };
            const auto snapshot = co_await snapshots.find_one(options);
            if (!snapshot) {
                continue;
            }
            auto& value = result[index];
            value.metrics.assign(snapshot->get<"metrics">().view());
            // Age advances at read time from the last real queue observation;
            // health lease renewal never performs a business aggregate query.
            const auto oldestAt = snapshot->get<"outbox_oldest_at_ms">();
            const auto age = oldestAt == 0 ? 0 : std::max<std::int64_t>(0, service::message::utcNowMilliseconds() - oldestAt);
            constexpr std::string_view ageMetric{ "iot_engine_outbox_oldest_age_ms{" };
            if (const auto start = value.metrics.find(ageMetric); start != std::string::npos) {
                const auto space = value.metrics.find(' ', start);
                const auto end = value.metrics.find('\n', space);
                if (space != std::string::npos && end != std::string::npos) {
                    value.metrics.replace(space + 1, end - space - 1, std::to_string(age));
                }
            }
            value.healthJson.assign(snapshot->get<"health">().view());
            value.metricsPresent = true;
            value.readinessPresent = true;
            value.ready = snapshot->get<"ready">();
        }
        co_return result;
    }

    static std::string aggregateHealthJson(const std::vector<WorkerDiagnosticSnapshot>& snapshots, std::string_view status) {
        std::string result;
        for (const auto& snapshot : snapshots) {
            if (!snapshot.healthJson.empty()) {
                result = snapshot.healthJson;
                break;
            }
        }
        if (result.empty()) {
            result = "{\"status\":\"not_ready\",\"components\":{},\"alerts\":{}}";
        }

        constexpr std::string_view prefix{ "\"status\":\"" };
        if (const auto start = result.find(prefix); start != std::string::npos) {
            const auto valueStart = start + prefix.size();
            if (const auto valueEnd = result.find('"', valueStart);
                valueEnd != std::string::npos) {
                result.replace(valueStart, valueEnd - valueStart, status);
            }
        }

        if (result.back() == '}') {
            result.pop_back();
        }
        result += ",\"workers\":{";
        for (std::size_t index = 0; index < snapshots.size(); ++index) {
            if (index != 0) {
                result.push_back(',');
            }
            result += '"' + std::to_string(index) + "\":";
            if (snapshots[index].readinessPresent && !snapshots[index].healthJson.empty()) {
                result += snapshots[index].healthJson;
            } else {
                result += "{\"status\":\"not_ready\",\"components\":{}}";
            }
        }
        result += "}}";
        return result;
    }

    static std::string fallbackMetrics() {
        return "# TYPE iot_engine_ready gauge\n"
               "iot_engine_ready 0\n";
    }
};

} // namespace service::system
