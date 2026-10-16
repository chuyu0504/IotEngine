#pragma once

#include <charconv>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <ruvia/web/context.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/redis/redis_repository.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/features/messaging/messaging.entity.h"
#include "service/features/messaging/messaging.transport.h"

namespace service::message {

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
    std::size_t serviceWorkerCount = 1
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
        service::message::redis::queueAddAndWake(
            pipeline,
            service::message::configuration::runtimeChangesStream(),
            fields,
            std::nullopt,
            WorkerStreamTask::Reconciler,
            10000
        );
    }
    if (webhookChange) {
        // Every Worker owns an independent in-memory catalog, so each receives one
        // refresh event. Data events are claimed independently from their shared work queue.
        for (std::size_t workerIndex = 0; workerIndex < serviceWorkerCount;
             ++workerIndex) {
            service::message::redis::queueAddAndWake(
                pipeline,
                service::message::access::stream::catalogChanges(workerIndex),
                fields,
                workerIndex,
                WorkerStreamTask::Webhook,
                10000
            );
        }
    }
    if (accessSessionChange) {
        service::message::redis::queueAddAndWake(
            pipeline,
            service::message::access::stream::sessionChanges(),
            fields,
            std::nullopt,
            WorkerStreamTask::Webhook,
            10000
        );
    }
    const auto replies = co_await std::move(pipeline).exec();
    service::message::redis::requirePipelineSuccess("publish config change", replies);
    co_return;
}

} // namespace service::message

#include <set>

#include <ruvia/web/web_worker.h>

#include "service/common/uuid.h"

namespace service::message::idempotency {

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

} // namespace service::message::idempotency

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>

#include <ruvia/core/channel.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/timer.h>

#include "service/features/observability/observability.service.h"
#include "service/features/messaging/postgres_notifier/postgres_notifier.transport.h"
#include "service/features/live/live.service.h"
#include "service/features/messaging/messaging.config.h"

namespace service::message::outbox {

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
            available.extract(ruvia::db_date_part::epoch,
                available.binary(available.aggregate("min", { available.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"available_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()) }),
                    ruvia::db_binary_operator::subtract, available.call("clock_timestamp"))),
            ruvia::db_binary_operator::multiply, available.value(1000));
        available.select(available.cast(available.cast(available.call("ceil", { delayMs }),
                ruvia::db_data_type::big_int), ruvia::db_data_type::text))
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
        auto transaction = co_await context.db().begin_transaction();
        ruvia::db_query pending(context.pool());
        const auto occurredAtMs = pending.call("floor", { pending.binary(
            pending.extract(ruvia::db_date_part::epoch, pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"occurred_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())),
            ruvia::db_binary_operator::multiply, pending.value(1000)) });
        pending.select(pending.cast(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_data_type::text))
            .add_select(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"event_type">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())).add_select(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"aggregate_type">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .add_select(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"aggregate_id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())).add_select(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"action">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .add_select(pending.cast(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"schema_version">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_data_type::text))
            .add_select(pending.cast(pending.cast(occurredAtMs, ruvia::db_data_type::big_int), ruvia::db_data_type::text))
            .add_select(pending.coalesce({ pending.binary(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"payload">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()),
                ruvia::db_binary_operator::json_get_text, pending.value("device_code")), pending.value("") }))
            .add_select(pending.cast(pending.coalesce({ pending.binary(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"payload">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()),
                ruvia::db_binary_operator::json_get, pending.value("data")),
                pending.cast(pending.value("{}"), ruvia::db_data_type::jsonb) }), ruvia::db_data_type::text))
            .from(service::messaging::persistence::OutboxEventEntity::table_name())
            .and_where(pending.unary(ruvia::db_unary_operator::is_null, pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"published_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())))
            .and_where(pending.unary(ruvia::db_unary_operator::is_null, pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())))
            .and_where(pending.binary(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"available_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::less_equal, pending.call("now")))
            .add_order_by(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"occurred_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())).add_order_by(pending.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))
            .lock({ .mode_ = ruvia::db_row_lock::update, .skip_locked_ = true }).limit(100);
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
                    co_await service::live::publish(context.redis(), event.aggregate);
                } else if (event.aggregate == "command") {
                    const auto publication = service::message::access::stream::eventPublication(
                        event.id,
                        event.type,
                        event.aggregateId,
                        event.deviceCode,
                        integer(event.occurredAtMs),
                        event.data,
                        service::runtime::instanceId()
                    );
                    const std::vector<std::string_view> keys(publication.keys.begin(), publication.keys.end());
                    const std::vector<std::string_view> arguments(publication.arguments.begin(), publication.arguments.end());
                    const auto reply = co_await context.redis().eval(service::message::access::stream::kPublishEventScript, keys, arguments);
                    if (!reply.null() && reply.kind() != ruvia::redis_value::kind_type::string) {
                        service::message::redis::throwValue("publish open-access event", reply);
                    }
                } else {
                    co_await publishConfigEnvelope(context.redis(), event.id, event.type, event.aggregate, event.aggregateId, event.action, event.schemaVersion, event.occurredAtMs, serviceWorkerCount_);
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
                const auto seconds = retry.least({ retry.value(300), retry.cast(
                    retry.call("int8shl", { retry.cast(retry.value(1), ruvia::db_data_type::big_int),
                        retry.least({ retry.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), retry.value(8) }) }), ruvia::db_data_type::integer) });
                const std::vector<ruvia::db_named_argument> intervalArgs{ { "secs", seconds } };
                retry.update(service::messaging::persistence::OutboxEventEntity::table_name())
                    .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), attempts).set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"last_error">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), retry.value(publishError))
                    .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"available_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), retry.binary(retry.call("now"), ruvia::db_binary_operator::add,
                        retry.call("make_interval", {}, intervalArgs)))
                    .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), retry.case_when({ { retry.binary(attempts,
                        ruvia::db_binary_operator::greater_equal, retry.value(20)), retry.call("now") } },
                        retry.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())))
                    .and_where(retry.binary(retry.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                        retry.cast(retry.value(event.id), ruvia::db_data_type::uuid)));
                (void)co_await transaction.execute(retry);
                co_await transaction.commit();
                observability_.incrementCounter("iot_engine_outbox_publish_retries_total");
                co_return true;
            }
            ruvia::db_query published(context.pool());
            published.update(service::messaging::persistence::OutboxEventEntity::table_name()).set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"published_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), published.call("now"))
                .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), published.binary(published.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"attempts">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::add, published.value(1)))
                .set(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"last_error">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view(), published.null_value())
                .and_where(published.binary(published.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"id">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                    published.cast(published.value(event.id), ruvia::db_data_type::uuid)));
            (void)co_await transaction.execute(published);
        }
        co_await transaction.commit();
        observability_.incrementCounter("iot_engine_outbox_published_total", events.size());
        observability_.setGauge("iot_engine_outbox_last_batch_size", static_cast<std::int64_t>(events.size()));
        co_return true;
    }

    ruvia::task<void> collectMetrics(ruvia::web_worker_context& context) {
        const auto workerText = observability_.workerIndex();
        if (workerText.empty())
            co_return;
        std::size_t workerIndex{};
        const auto parsed = std::from_chars(
            workerText.data(), workerText.data() + workerText.size(), workerIndex);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != workerText.data() + workerText.size()) {
            co_return;
        }
        ruvia::db_query metrics(context.pool());
        const auto pending = metrics.unary(ruvia::db_unary_operator::is_null, metrics.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()));
        const auto count = metrics.aggregate("count", { metrics.star() });
        const auto oldest = metrics.filter(metrics.aggregate("min", { metrics.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"occurred_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()) }), pending);
        const auto ageMs = metrics.call("floor", { metrics.binary(metrics.extract(ruvia::db_date_part::epoch,
            metrics.binary(metrics.call("now"), ruvia::db_binary_operator::subtract, oldest)),
            ruvia::db_binary_operator::multiply, metrics.value(1000)) });
        metrics.select(metrics.cast(metrics.filter(count, pending), ruvia::db_data_type::text))
            .add_select(metrics.cast(metrics.cast(metrics.coalesce({ ageMs, metrics.value(0) }),
                ruvia::db_data_type::big_int), ruvia::db_data_type::text))
            .add_select(metrics.cast(metrics.filter(count, metrics.unary(ruvia::db_unary_operator::is_not_null,
                metrics.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"dead_lettered_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view()))), ruvia::db_data_type::text))
            .from(service::messaging::persistence::OutboxEventEntity::table_name())
            .and_where(metrics.unary(ruvia::db_unary_operator::is_null, metrics.column(std::tuple_element_t<service::messaging::persistence::OutboxEventEntity::column_index<"published_at">(), service::messaging::persistence::OutboxEventEntity::columns_type>::name.view())));
        const auto rows = co_await context.db().query(metrics);
        if (!rows.empty()) {
            const auto pending =
                integer(rows.front()[0].value().value_or(std::string_view{}));
            const auto oldestAge =
                integer(rows.front()[1].value().value_or(std::string_view{}));
            const auto deadLettered =
                integer(rows.front()[2].value().value_or(std::string_view{}));
            observability_.setGauge("iot_engine_outbox_pending", pending);
            observability_.setGauge("iot_engine_outbox_oldest_age_ms", oldestAge);
            observability_.setGauge("iot_engine_outbox_dead_lettered", deadLettered);
            updateAlert("outbox_pending", pending, policy_.pendingAlertThreshold);
            updateAlert("outbox_oldest_age", oldestAge, policy_.oldestAgeAlertMs);
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
            co_await collectStream(context, "dead_letter_" + std::to_string(index), message::deadLetterStream(index, service::runtime::instanceId()), {});
        }

        // The operations endpoints run on any Service Worker. Publish this
        // worker's complete snapshot so those endpoints can aggregate every
        // worker without reading another worker's in-memory runtime diagnostics.
        WorkerSnapshotEntity snapshot(context.pool());
        snapshot.set<"id">(service::message::worker_metrics::snapshotId(workerIndex, service::runtime::instanceId()));
        snapshot.set<"metrics">(observability_.prometheus());
        snapshot.set<"ready">(observability_.areComponentsReady());
        snapshot.set<"health">(observability_.healthJson());
        auto snapshots = context.redis().get_repository<WorkerSnapshotEntity>();
        const ruvia::redis_write_options expiration{
            .ttl_ = service::message::worker_metrics::kSnapshotTtl};
        (void)co_await snapshots.upsert(snapshot, expiration);
    }

    ruvia::task<void> collectStream(ruvia::web_worker_context& context, std::string metricSuffix, std::string stream, std::string_view group) {
        const std::vector<std::string> lengthArguments{ "XLEN", stream };
        const auto length = co_await service::message::redis::command(
            context.redis(),
            lengthArguments
        );
        if (length.kind() == ruvia::redis_value::kind_type::integer) {
            observability_.setGauge("iot_engine_stream_" + metricSuffix + "_entries", length.integer());
        }
        if (group.empty()) {
            co_return;
        }
        const std::vector<std::string> pendingArguments{ "XPENDING", stream, std::string(group) };
        const auto pending = co_await service::message::redis::command(
            context.redis(),
            pendingArguments
        );
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

    ruvia::task<void> cleanupReceipts(ruvia::web_worker_context& context) {
        ruvia::db_query expired(context.pool());
        const std::vector<ruvia::db_named_argument> intervalArgs{
            { "days", expired.cast(expired.value(policy_.receiptRetentionDays), ruvia::db_data_type::integer) }
        };
        expired.delete_from(service::messaging::persistence::OutboxConsumerReceiptEntity::table_name())
            .and_where(expired.binary(expired.column(std::tuple_element_t<service::messaging::persistence::OutboxConsumerReceiptEntity::column_index<"processed_at">(), service::messaging::persistence::OutboxConsumerReceiptEntity::columns_type>::name.view()), ruvia::db_binary_operator::less,
                expired.binary(expired.call("now"), ruvia::db_binary_operator::subtract,
                    expired.call("make_interval", {}, intervalArgs))));
        (void)co_await context.db().execute(expired);
    }

    void updateAlert(std::string name, std::int64_t value, std::int64_t threshold) {
        const bool active = threshold > 0 && value >= threshold;
        const auto detail = "value=" + std::to_string(value) +
            ", threshold=" + std::to_string(threshold);
        if (observability_.setAlertState(name, active, detail)) {
            std::cerr << "operational alert " << (active ? "active" : "cleared")
                      << ": " << name << " (" << detail << ")\n";
        }
    }

    observability::RuntimeDiagnostics& observability_;
    std::size_t collectorWorkerCount_{};
    std::size_t serviceWorkerCount_{};
    Policy policy_;
};

} // namespace service::message::outbox

namespace service::rpc {

class RpcReceiptService final {
  public:
    static ruvia::task<std::optional<RpcReplyRecord>> beginRequest(
        const ruvia::redis_handle& redis, const std::string& id) {
        const std::vector<std::string> previousArguments{"GET", service::message::rpc::Contract::reply(id)};
        const auto previous = co_await service::message::redis::command(redis, previousArguments);
        if (previous.kind() == ruvia::redis_value::kind_type::string)
            co_return RpcReplyRecord{std::string(previous.string())};
        if (previous.kind() == ruvia::redis_value::kind_type::error)
            service::message::redis::throwValue("RPC receipt", previous);
        const std::vector<std::string> claimedArguments{"SET", service::message::rpc::Contract::claim(id), "1", "NX", "EX", "86400"};
        const auto claimed = co_await service::message::redis::command(redis,
            claimedArguments);
        if (claimed.kind() == ruvia::redis_value::kind_type::error)
            service::message::redis::throwValue("RPC claim", claimed);
        if (claimed.kind() != ruvia::redis_value::kind_type::string)
            service::common::fail(10004, "Background operation was interrupted; inspect its state", 503);
        const std::vector<std::string> cancelledArguments{"EXISTS", service::message::rpc::Contract::cancelled(id)};
        const auto cancelled = co_await service::message::redis::command(redis,
            cancelledArguments);
        if (cancelled.kind() == ruvia::redis_value::kind_type::error)
            service::message::redis::throwValue("RPC cancellation", cancelled);
        if (cancelled.kind() == ruvia::redis_value::kind_type::integer && cancelled.integer() != 0)
            service::common::fail(10004, "Background operation cancelled", 503);
        co_return std::nullopt;
    }

    static ruvia::task<void> saveAndAcknowledge(const ruvia::redis_handle& redis,
        const std::string& id, const std::string& stream, std::string_view group,
        const std::string& messageId, const RpcReplyRecord& record) {
    static constexpr std::string_view kReplyScript = R"lua(
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', 100000, '*', 'topic', KEYS[1], 'schema_version', '1')
redis.call('XACK', KEYS[3], ARGV[3], ARGV[4])
redis.call('XDEL', KEYS[3], ARGV[4])
return 1
)lua";

                const auto replyKey = service::message::rpc::Contract::reply(id);
                const std::string_view keys[]{replyKey, service::message::live::kChanges, stream};
                const std::string_view args[]{record.payload, service::message::rpc::Contract::replyLifetime, group, messageId};
                const auto reply = co_await redis.eval(kReplyScript, keys, args);
                if (reply.kind() == ruvia::redis_value::kind_type::error)
                    service::message::redis::throwValue("RPC persist reply", reply);
    }
};

} // namespace service::rpc
