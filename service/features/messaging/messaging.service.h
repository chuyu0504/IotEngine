#pragma once

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <ruvia/web/context.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/redis/redis_repository.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/live/live.service.h"
#include "service/features/messaging/messaging.config.h"
#include "service/features/messaging/messaging.entity.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/observability/observability.service.h"
#include "service/middleware/log.h"

namespace service::messaging::outbox {

template <typename Redis>
ruvia::task<void> publishConfigEnvelope(
    const Redis& redis,
    std::string_view eventId,
    std::string_view eventType,
    std::string_view aggregate,
    std::string_view aggregateId,
    std::string_view action,
    std::string_view schemaVersion,
    std::string_view occurredAtMs,
    std::size_t serviceWorkerCount,
    std::string_view instance
) {
    const bool runtimeChange =
        aggregate == "link" || aggregate == "device" || aggregate == "protocol";
    const bool webhookChange = aggregate == "access_key" || aggregate == "webhook" || aggregate == "mqtt" ||
        aggregate == "device" || aggregate == "protocol";
    const bool accessSessionChange = aggregate == "access_key" || aggregate == "device";
    if (!runtimeChange && !webhookChange && !accessSessionChange) {
        co_return;
    }
    if (serviceWorkerCount == 0) {
        throw std::invalid_argument("invalid Service Worker count for config event");
    }
    const std::vector<service::message::StreamField> fields{
        { "message_id", std::string(eventId) },
        { "event_id", std::string(eventId) },
        { "event_type", std::string(eventType) },
        { "schema_version", std::string(schemaVersion) },
        { "aggregate", std::string(aggregate) },
        { "aggregate_type", std::string(aggregate) },
        { "action", std::string(action) },
        { "aggregate_id", std::string(aggregateId) },
        { "created_at_ms", std::string(occurredAtMs) },
        { "occurred_at_ms", std::string(occurredAtMs) }
    };
    auto pipeline = redis.pipeline();
    if (runtimeChange) {
        service::messaging::stream::queueAddAndWake(
            pipeline,
            service::message::configuration::runtimeChangesStream(),
            fields,
            std::nullopt,
            service::message::WorkerStreamTask::Reconciler,
            10000,
            instance
        );
    }
    if (webhookChange) {
        // Every Worker owns an independent in-memory catalog, so each receives one
        // refresh event. Data events are claimed independently from their shared work queue.
        for (std::size_t workerIndex = 0; workerIndex < serviceWorkerCount;
             ++workerIndex) {
            service::messaging::stream::queueAddAndWake(
                pipeline,
                service::message::access::stream::catalogChanges(workerIndex),
                fields,
                workerIndex,
                service::message::WorkerStreamTask::Webhook,
                10000,
                instance
            );
        }
    }
    if (accessSessionChange) {
        service::messaging::stream::queueAddAndWake(
            pipeline,
            service::message::access::stream::sessionChanges(),
            fields,
            std::nullopt,
            service::message::WorkerStreamTask::Webhook,
            10000,
            instance
        );
    }
    const auto replies = co_await std::move(pipeline).exec();
    service::utils::redis::requirePipelineSuccess("publish config change", replies);
    co_return;
}

} // namespace service::messaging::outbox

namespace service::messaging::idempotency {

inline std::vector<std::string>
eventIds(const std::vector<service::message::StreamMessage>& messages) {
    std::set<std::string, std::less<>> unique;
    for (const auto& message : messages) {
        const auto id = message.get("event_id");
        if (service::common::isUuid(id)) {
            unique.emplace(id);
        }
    }
    return { unique.begin(), unique.end() };
}

inline ruvia::task<std::set<std::string, std::less<>>>
pending(ruvia::web_worker_context& context, std::string_view consumer, const std::vector<std::string>& eventIds) {
    std::set<std::string, std::less<>> result(eventIds.begin(), eventIds.end());
    if (eventIds.empty()) {
        co_return result;
    }

    ruvia::db_query receipts(context.pool());
    std::vector<ruvia::db_expression> ids;
    ids.reserve(eventIds.size());
    for (const auto& id : eventIds) {
        ids.push_back(receipts.cast(receipts.value(id), ruvia::db_data_type::uuid));
    }
    receipts.select(receipts.cast(receipts.column(std::tuple_element_t<service::messaging::persistence::OutboxConsumerReceiptEntity::column_index<"event_id">(), service::messaging::persistence::OutboxConsumerReceiptEntity::columns_type>::name.view()), ruvia::db_data_type::text))
        .from(service::messaging::persistence::OutboxConsumerReceiptEntity::table_name())
        .and_where(receipts.binary(receipts.column(std::tuple_element_t<service::messaging::persistence::OutboxConsumerReceiptEntity::column_index<"consumer_name">(), service::messaging::persistence::OutboxConsumerReceiptEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, receipts.value(consumer)))
        .and_where(receipts.binary(receipts.column(std::tuple_element_t<service::messaging::persistence::OutboxConsumerReceiptEntity::column_index<"event_id">(), service::messaging::persistence::OutboxConsumerReceiptEntity::columns_type>::name.view()), ruvia::db_binary_operator::in, receipts.list(ids)));
    const auto rows = co_await context.db().query(receipts);
    for (const auto& row : rows) {
        result.erase(std::string(row[0].value().value_or(std::string_view{})));
    }
    co_return result;
}

inline bool
shouldProcess(const service::message::StreamMessage& message, const std::set<std::string, std::less<>>& pendingIds) {
    const auto id = message.get("event_id");
    return !service::common::isUuid(id) || pendingIds.contains(id);
}

inline ruvia::task<void>
markProcessed(ruvia::web_worker_context& context, std::string_view consumer, const std::vector<std::string>& eventIds) {
    if (eventIds.empty()) {
        co_return;
    }
    ruvia::db_query receipts(context.pool());
    receipts.insert_into(service::messaging::persistence::OutboxConsumerReceiptEntity::table_name(), { "consumer_name", "event_id" });
    for (const auto& id : eventIds) {
        receipts.values({ receipts.value(consumer), receipts.cast(receipts.value(id), ruvia::db_data_type::uuid) });
    }
    receipts.on_conflict({ .columns_ = { "consumer_name", "event_id" }, .do_nothing_ = true });
    (void)co_await context.db().execute(receipts);
}

} // namespace service::messaging::idempotency

namespace service::messaging::outbox {

class OutboxService {
  protected:
    struct Event {
        std::string id;
        std::string type;
        std::string aggregate;
        std::string aggregateId;
        std::string action;
        std::string schemaVersion;
        std::string occurredAtMs;
        std::string deviceCode;
        std::string data;
    };

    using Clock = std::chrono::steady_clock;

    OutboxService(observability::RuntimeDiagnostics& diagnostics, std::size_t collectors, std::size_t workers, Policy policy)
        : observability_(diagnostics), collectorWorkerCount_(collectors), serviceWorkerCount_(workers), policy_(policy) {}

    ruvia::task<std::optional<Clock::time_point>> nextAvailable(ruvia::web_worker_context& context) {
        // No periodic empty-queue scan. A timer is armed only for a durable
        // retry, or for eligible rows currently locked by another dispatcher.
        ruvia::db_query available(context.pool());
        const auto delayMs = available.binary(
            available.extract(ruvia::db_date_part::epoch, available.binary(available.aggregate("min", { available.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"available_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()) }), ruvia::db_binary_operator::subtract, available.call("clock_timestamp"))),
            ruvia::db_binary_operator::multiply,
            available.value(1000)
        );
        available.select(available.cast(available.cast(available.call("ceil", { delayMs }), ruvia::db_data_type::big_int), ruvia::db_data_type::text))
            .from(service::messaging::persistence::OutboxEventEntity::table_name())
            .and_where(available.unary(ruvia::db_unary_operator::is_null, available.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"published_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())))
            .and_where(available.unary(ruvia::db_unary_operator::is_null, available.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())));
        const auto rows = co_await context.db().query(available);
        if (rows.empty() || !rows.front()[0].value()) {
            co_return std::nullopt;
        }
        const auto delay = std::chrono::milliseconds(std::max<std::int64_t>(25, integer(*rows.front()[0].value())));
        co_return Clock::now() + delay;
    }

    ruvia::task<bool> dispatch(ruvia::web_worker_context& context) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        auto transaction = co_await context.db().begin_transaction();
        ruvia::db_query pending(context.pool());
        const auto occurredAtMs = pending.call("floor", { pending.binary(pending.extract(ruvia::db_date_part::epoch, pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"occurred_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())), ruvia::db_binary_operator::multiply, pending.value(1000)) });
        pending.select(pending.cast(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_data_type::text))
            .add_select(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"event_type">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .add_select(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"aggregate_type">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .add_select(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"aggregate_id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .add_select(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"action">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .add_select(pending.cast(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"schema_version">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_data_type::text))
            .add_select(pending.cast(pending.cast(occurredAtMs, ruvia::db_data_type::big_int), ruvia::db_data_type::text))
            .add_select(pending.coalesce({ pending.binary(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"payload">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::json_get_text, pending.value("device_code")), pending.value("") }))
            .add_select(pending.cast(pending.coalesce({ pending.binary(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"payload">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::json_get, pending.value("data")), pending.cast(pending.value("{}"), ruvia::db_data_type::jsonb) }), ruvia::db_data_type::text))
            .from(service::messaging::persistence::OutboxEventEntity::table_name())
            .and_where(pending.unary(ruvia::db_unary_operator::is_null, pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"published_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())))
            .and_where(pending.unary(ruvia::db_unary_operator::is_null, pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())))
            .and_where(pending.binary(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"available_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::less_equal, pending.call("now")))
            .add_order_by(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"occurred_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .add_order_by(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .lock({ .mode_ = ruvia::db_row_lock::update, .skip_locked_ = true })
            .limit(100);
        const auto rows = co_await transaction.query(pending);
        if (rows.empty()) {
            co_await transaction.commit();
            observability_.setGauge("iot_engine_outbox_last_batch_size", 0);
            co_return false;
        }

        std::vector<Event> events;
        events.reserve(rows.size());
        for (const auto& row : rows) {
            Event event;
            event.id = std::string(row[0].value().value_or(std::string_view{}));
            event.type = std::string(row[1].value().value_or(std::string_view{}));
            event.aggregate = std::string(row[2].value().value_or(std::string_view{}));
            event.aggregateId = std::string(row[3].value().value_or(std::string_view{}));
            event.action = std::string(row[4].value().value_or(std::string_view{}));
            event.schemaVersion = std::string(row[5].value().value_or(std::string_view{ "1" }));
            event.occurredAtMs = std::string(row[6].value().value_or(std::string_view{ "0" }));
            event.deviceCode = row[7].value().value_or(std::string_view{});
            event.data = row[8].value().value_or(std::string_view{});
            events.push_back(std::move(event));
        }

        for (const auto& event : events) {
            std::string publishError;
            try {
                if (event.type == "query.changed") {
                    // Schema 2 rows carry the changed entity ids; older rows
                    // carried table names and mean the whole topic changed.
                    std::vector<std::string> ids;
                    if (event.schemaVersion == "2") {
                        for (const auto id : service::message::live::splitIds(event.aggregateId)) {
                            ids.emplace_back(id);
                        }
                    }
                    co_await service::live::publish(context.redis(), event.aggregate, ids);
                } else if (event.aggregate == "command") {
                    const auto publication = service::message::access::stream::eventPublication(
                        event.id,
                        event.type,
                        event.aggregateId,
                        event.deviceCode,
                        integer(event.occurredAtMs),
                        event.data,
                        identity.instanceId
                    );
                    const std::vector<std::string_view> keys(publication.keys.begin(), publication.keys.end());
                    const std::vector<std::string_view> arguments(publication.arguments.begin(), publication.arguments.end());
                    const auto reply = co_await context.redis().eval(service::message::access::stream::kPublishEventScript, keys, arguments);
                    if (!reply.null() && reply.kind() != ruvia::redis_value::kind_type::string) {
                        service::utils::redis::throwValue("publish open-access event", reply);
                    }
                    co_await service::messaging::stream::wakeWorker(context.redis(), std::nullopt, service::message::WorkerStreamTask::CommandResult, identity.instanceId);
                } else {
                    co_await publishConfigEnvelope(context.redis(), event.id, event.type, event.aggregate, event.aggregateId, event.action, event.schemaVersion, event.occurredAtMs, serviceWorkerCount_, identity.instanceId);
                }
            } catch (const std::exception& error) {
                publishError = error.what();
            } catch (...) {
                publishError = "unknown publish failure";
            }
            if (!publishError.empty()) {
                if (publishError.size() > 2000) {
                    publishError.resize(2000);
                }
                ruvia::db_query retry(context.pool());
                const auto attempts = retry.binary(retry.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::add, retry.value(1));
                const auto seconds = retry.least({ retry.value(300), retry.cast(retry.call("int8shl", { retry.cast(retry.value(1), ruvia::db_data_type::big_int), retry.least({ retry.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), retry.value(8) }) }), ruvia::db_data_type::integer) });
                const std::vector<ruvia::db_named_argument> intervalArgs{ { "secs", seconds } };
                retry.update(service::messaging::persistence::OutboxEventEntity::table_name())
                    .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), attempts)
                    .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"last_error">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), retry.value(publishError))
                    .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"available_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), retry.binary(retry.call("now"), ruvia::db_binary_operator::add, retry.call("make_interval", {}, intervalArgs)))
                    .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), retry.case_when({ { retry.binary(attempts, ruvia::db_binary_operator::greater_equal, retry.value(20)), retry.call("now") } }, retry.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())))
                    .and_where(retry.binary(retry.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, retry.cast(retry.value(event.id), ruvia::db_data_type::uuid)));
                (void)co_await transaction.execute(retry);
                co_await transaction.commit();
                observability_.incrementCounter("iot_engine_outbox_publish_retries_total");
                co_return true;
            }
            ruvia::db_query published(context.pool());
            published.update(service::messaging::persistence::OutboxEventEntity::table_name()).set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"published_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), published.call("now")).set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), published.binary(published.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::add, published.value(1))).set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"last_error">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), published.null_value()).and_where(published.binary(published.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, published.cast(published.value(event.id), ruvia::db_data_type::uuid)));
            (void)co_await transaction.execute(published);
        }
        co_await transaction.commit();
        observability_.incrementCounter("iot_engine_outbox_published_total", events.size());
        observability_.setGauge("iot_engine_outbox_last_batch_size", static_cast<std::int64_t>(events.size()));
        co_return true;
    }

    ruvia::task<void> collectMetrics(ruvia::web_worker_context& context) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        ruvia::db_query metrics(context.pool());
        const auto pending = metrics.unary(ruvia::db_unary_operator::is_null, metrics.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()));
        const auto count = metrics.aggregate("count", { metrics.star() });
        const auto oldest = metrics.filter(metrics.aggregate("min", { metrics.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"occurred_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()) }), pending);
        const auto ageMs = metrics.call("floor", { metrics.binary(metrics.extract(ruvia::db_date_part::epoch, metrics.binary(metrics.call("now"), ruvia::db_binary_operator::subtract, oldest)), ruvia::db_binary_operator::multiply, metrics.value(1000)) });
        metrics.select(metrics.cast(metrics.filter(count, pending), ruvia::db_data_type::text))
            .add_select(metrics.cast(metrics.cast(metrics.coalesce({ ageMs, metrics.value(0) }), ruvia::db_data_type::big_int), ruvia::db_data_type::text))
            .add_select(metrics.cast(metrics.filter(count, metrics.unary(ruvia::db_unary_operator::is_not_null, metrics.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))), ruvia::db_data_type::text))
            .from(service::messaging::persistence::OutboxEventEntity::table_name())
            .and_where(metrics.unary(ruvia::db_unary_operator::is_null, metrics.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"published_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())));
        metrics.add_select(metrics.cast(metrics.cast(metrics.coalesce({ metrics.binary(metrics.extract(ruvia::db_date_part::epoch, oldest), ruvia::db_binary_operator::multiply, metrics.value(1000)), metrics.value(0) }), ruvia::db_data_type::big_int), ruvia::db_data_type::text));
        const auto rows = co_await context.db().query(metrics);
        if (!rows.empty()) {
            const auto pending =
                integer(rows.front()[0].value().value_or(std::string_view{}));
            const auto oldestAge =
                integer(rows.front()[1].value().value_or(std::string_view{}));
            const auto deadLettered =
                integer(rows.front()[2].value().value_or(std::string_view{}));
            oldestOutboxAtMs_ = integer(rows.front()[3].value().value_or(std::string_view{}));
            observability_.setGauge("iot_engine_outbox_pending", pending);
            observability_.setGauge("iot_engine_outbox_oldest_age_ms", oldestAge);
            observability_.setGauge("iot_engine_outbox_dead_lettered", deadLettered);
            updateAlert("outbox_pending", pending, policy_.pendingAlertThreshold);
            updateAgeAlert();
            updateAlert("outbox_dead_lettered", deadLettered, policy_.deadLetterAlertThreshold);
        }
        observability_.setGauge("iot_engine_outbox_receipt_retention_days", policy_.receiptRetentionDays);

        co_await collectStream(context, "runtime_config", service::message::configuration::runtimeChangesStream(), "iot-engine:runtime-reconciler");
        co_await collectStream(context, "access_session", service::message::access::stream::sessionChanges(), "iot-engine:open-webhook");
        co_await collectStream(context, "telemetry", message::parsedStream(), "iot-engine:telemetry-persistence");
        co_await collectStream(context, "command_result", message::commandResultStream(), "iot-engine:command-result");

        for (std::size_t index = 0; index < serviceWorkerCount_; ++index) {
            co_await collectStream(context, "webhook_config_" + std::to_string(index), service::message::access::stream::catalogChanges(index), "iot-engine:open-webhook");
        }
        for (std::size_t index = 0; index < collectorWorkerCount_; ++index) {
            co_await collectStream(context, "dead_letter_" + std::to_string(index), message::deadLetterStream(index, identity.instanceId), {});
        }

        co_await publishDiagnostics(context);
    }

    // No aggregate business query on the health lease. Unchanged diagnostics
    // only extend the existing record; a real change or Redis recovery writes it.
    ruvia::task<void> publishDiagnostics(ruvia::web_worker_context& context) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        const auto id = service::message::worker_metrics::snapshotId(identity.index, identity.instanceId);
        auto snapshots = context.redis().get_repository<WorkerSnapshotEntity>();
        const auto predicate = WorkerSnapshotEntity::field<"id">() == id;
        const auto revision = observability_.revision();
        if (snapshotRevision_ == revision && snapshotOldestAtMs_ == oldestOutboxAtMs_ &&
            co_await snapshots.expire(predicate, std::chrono::duration_cast<std::chrono::seconds>(service::message::worker_metrics::kSnapshotTtl))) {
            co_return;
        }
        WorkerSnapshotEntity snapshot(context.pool());
        snapshot.set<"id">(id);
        snapshot.set<"metrics">(observability_.prometheus());
        snapshot.set<"ready">(observability_.areComponentsReady());
        snapshot.set<"health">(observability_.healthJson());
        snapshot.set<"outbox_oldest_at_ms">(oldestOutboxAtMs_);
        const ruvia::redis_write_options expiration{ .ttl_ = service::message::worker_metrics::kSnapshotTtl };
        (void)co_await snapshots.upsert(snapshot, expiration);
        snapshotRevision_ = revision;
        snapshotOldestAtMs_ = oldestOutboxAtMs_;
    }

    ruvia::task<void> retireDiagnostics(ruvia::web_worker_context& context) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        const auto id = service::message::worker_metrics::snapshotId(identity.index, identity.instanceId);
        auto snapshots = context.redis().get_repository<WorkerSnapshotEntity>();
        (void)co_await snapshots.delete_by(WorkerSnapshotEntity::field<"id">() == id);
        snapshotRevision_.reset();
    }

    [[nodiscard]] std::optional<Clock::time_point> nextAgeAlert() const {
        if (oldestOutboxAtMs_ == 0 || policy_.oldestAgeAlertMs <= 0 || ageAlertActive_) {
            return std::nullopt;
        }
        const auto delay = oldestOutboxAtMs_ + policy_.oldestAgeAlertMs - service::message::utcNowMilliseconds();
        return Clock::now() + std::chrono::milliseconds(std::max<std::int64_t>(0, delay));
    }

    void updateAgeAlert() {
        const auto age = oldestOutboxAtMs_ == 0 ? 0 : std::max<std::int64_t>(0, service::message::utcNowMilliseconds() - oldestOutboxAtMs_);
        ageAlertActive_ = policy_.oldestAgeAlertMs > 0 && age >= policy_.oldestAgeAlertMs;
        updateAlert("outbox_oldest_age", age, policy_.oldestAgeAlertMs);
    }

    ruvia::task<void> collectStream(ruvia::web_worker_context& context, std::string metricSuffix, std::string stream, std::string_view group) {
        const std::string_view lengthArguments[]{ "XLEN", stream };
        const auto length = co_await context.redis().command(lengthArguments);
        if (length.kind() == ruvia::redis_value::kind_type::integer) {
            observability_.setGauge("iot_engine_stream_" + metricSuffix + "_entries", length.integer());
        }
        if (group.empty()) {
            co_return;
        }
        const std::string_view pendingArguments[]{ "XPENDING", stream, group };
        const auto pending = co_await context.redis().command(pendingArguments);
        if (pending.kind() == ruvia::redis_value::kind_type::array &&
            !pending.array().empty() &&
            pending.array().front().kind() == ruvia::redis_value::kind_type::integer) {
            observability_.setGauge("iot_engine_stream_" + metricSuffix + "_pending", pending.array().front().integer());
        }
    }

    static std::int64_t integer(std::string_view value) {
        std::int64_t result{};
        const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
        return error == std::errc{} && end == value.data() + value.size() ? result : 0;
    }

    ruvia::task<Clock::time_point> cleanupReceipts(ruvia::web_worker_context& context) {
        auto transaction = co_await context.db().begin_transaction();
        ruvia::db_query ownership(context.pool());
        ownership.select(ownership.call("pg_try_advisory_xact_lock", { ownership.cast(ownership.value(service::message::lock::kOutboxReceiptCleanup), ruvia::db_data_type::big_int) }));
        const auto claimed = co_await transaction.query(ownership);
        if (claimed.empty() || claimed.front()[0].value().value_or("") != "t") {
            co_await transaction.commit();
            co_return Clock::now() + std::chrono::milliseconds(100);
        }
        using Receipt = service::messaging::persistence::OutboxConsumerReceiptEntity;
        constexpr auto processedAt = std::tuple_element_t<Receipt::column_index<"processed_at">(), Receipt::columns_type>::name.view();
        ruvia::db_query expired(context.pool());
        const std::vector<ruvia::db_named_argument> intervalArgs{
            { "days", expired.cast(expired.value(policy_.receiptRetentionDays), ruvia::db_data_type::integer) }
        };
        expired.delete_from(Receipt::table_name())
            .and_where(expired.binary(expired.column(processedAt), ruvia::db_binary_operator::less_equal, expired.binary(expired.call("clock_timestamp"), ruvia::db_binary_operator::subtract, expired.call("make_interval", {}, intervalArgs))));
        (void)co_await transaction.execute(expired);
        ruvia::db_query next(context.pool());
        const std::vector<ruvia::db_named_argument> retentionArgs{
            { "days", next.cast(next.value(policy_.receiptRetentionDays), ruvia::db_data_type::integer) }
        };
        const auto deadline = next.binary(next.aggregate("min", { next.column(processedAt) }), ruvia::db_binary_operator::add, next.call("make_interval", {}, retentionArgs));
        const auto delay = next.binary(next.extract(ruvia::db_date_part::epoch, next.binary(deadline, ruvia::db_binary_operator::subtract, next.call("clock_timestamp"))), ruvia::db_binary_operator::multiply, next.value(1000));
        next.select(next.cast(next.cast(next.call("ceil", { delay }), ruvia::db_data_type::big_int), ruvia::db_data_type::text)).from(Receipt::table_name());
        const auto rows = co_await transaction.query(next);
        co_await transaction.commit();
        // New receipts cannot expire before now + retention. This is a bound
        // derived from their lifetime, not a fixed-period cleanup scan.
        const auto lifetime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::hours(24 * policy_.receiptRetentionDays));
        const auto wait = rows.empty() || !rows.front()[0].value()
            ? lifetime
            : std::chrono::milliseconds(std::clamp<std::int64_t>(integer(*rows.front()[0].value()), 1, lifetime.count()));
        co_return Clock::now() + wait;
    }

    void updateAlert(std::string name, std::int64_t value, std::int64_t threshold) {
        const bool active = threshold > 0 && value >= threshold;
        const auto detail = "value=" + std::to_string(value) +
            ", threshold=" + std::to_string(threshold);
        if (observability_.setAlertState(name, active, detail)) {
            LOG_WARN << "operational alert " << (active ? "active" : "cleared")
                     << ": " << name << " (" << detail << ")";
        }
    }

    observability::RuntimeDiagnostics& observability_;
    std::size_t collectorWorkerCount_{};
    std::size_t serviceWorkerCount_{};
    Policy policy_;
    std::optional<std::uint64_t> snapshotRevision_;
    std::int64_t snapshotOldestAtMs_ = 0;
    std::int64_t oldestOutboxAtMs_ = 0;
    bool ageAlertActive_ = false;
};

} // namespace service::messaging::outbox
