#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/model.h>
#include <ruvia/web/redis/redis_handle.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/access/access.entity.h"
#include "service/features/access/access.types.h"
#include "service/features/configuration/configuration.entity.h"
#include "service/features/live/live.service.h"
#include "service/features/messaging/messaging.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/utils/json.h"
#include "service/utils/mqtt.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"
#include "service/utils/text.h"
#include "service/utils/uuid.h"

namespace service::access::session {

template <typename Context>
ruvia::task<void> refresh(Context& context, bool onlyIfMissing = false) {
    // Serialize the database snapshot and Redis pointer swap across service instances.
    // The lock is held only by cold-path configuration projection, never by API reads.
    auto transaction = co_await context.db().begin_transaction();
    co_await transaction.advisory_lock(service::message::lock::kAccessSessionProjection);
    if (onlyIfMissing) {
        const std::vector<std::string> activeArguments{ "GET", std::string(service::message::access::session::kActiveVersionKey) };
        const auto active = co_await context.redis().command(std::vector<std::string_view>(activeArguments.begin(), activeArguments.end()));
        if (!active.null()) {
            if (active.kind() != ruvia::redis_value::kind_type::string) {
                service::utils::redis::throwValue(
                    "read active access-session projection",
                    active
                );
            }
            co_await transaction.commit();
            co_return;
        }
    }
    ruvia::db_query snapshot(context.pool());
    const auto expiresAtMs = snapshot.cast(
        snapshot.coalesce({ snapshot.cast(snapshot.binary(snapshot.extract(ruvia::db_date_part::epoch, service::access::persistence::OpenAccessKeyEntity::column<"expires_at">().expression(snapshot, "key")), ruvia::db_binary_operator::multiply, snapshot.value(std::int64_t{ 1000 })), ruvia::db_data_type::big_int), snapshot.value(std::int64_t{ 0 }) }),
        ruvia::db_data_type::text
    );
    const std::array bindingOrder{
        ruvia::db_order_term{ service::access::persistence::OpenAccessKeyDeviceEntity::column<"device_id">().expression(snapshot, "binding") }
    };
    const auto bindingIds = snapshot.aggregate(
        "jsonb_agg",
        { snapshot.cast(service::access::persistence::OpenAccessKeyDeviceEntity::column<"device_id">().expression(snapshot, "binding"), ruvia::db_data_type::text) },
        false,
        bindingOrder
    );
    const auto bindingIdsOrEmpty = snapshot.coalesce({ snapshot.filter(bindingIds, snapshot.unary(ruvia::db_unary_operator::is_not_null, service::access::persistence::OpenAccessKeyDeviceEntity::column<"device_id">().expression(snapshot, "binding"))), snapshot.cast(snapshot.value("[]"), ruvia::db_data_type::jsonb) });
    auto snapshotBuilder = transaction.template repository<service::access::persistence::OpenAccessKeyEntity>().query_builder("key");
    snapshotBuilder.select({ { "access_key_hash", service::access::persistence::OpenAccessKeyEntity::column<"access_key_hash">().expression(snapshot, "key") }, { "id", snapshot.cast(service::access::persistence::OpenAccessKeyEntity::column<"id">().expression(snapshot, "key"), ruvia::db_data_type::text) }, { "name", service::access::persistence::OpenAccessKeyEntity::column<"name">().expression(snapshot, "key") }, { "status", snapshot.cast(service::access::persistence::OpenAccessKeyEntity::column<"status">().expression(snapshot, "key"), ruvia::db_data_type::text) }, { "value4", expiresAtMs }, { "scopes", snapshot.cast(service::access::persistence::OpenAccessKeyEntity::column<"scopes">().expression(snapshot, "key"), ruvia::db_data_type::text) }, { "value6", snapshot.cast(bindingIdsOrEmpty, ruvia::db_data_type::text) } }).template join<service::access::persistence::OpenAccessKeyDeviceEntity>(ruvia::db_join_type::left, "binding", snapshot.binary(service::access::persistence::OpenAccessKeyDeviceEntity::column<"access_key_id">().expression(snapshot, "binding"), ruvia::db_binary_operator::equal, service::access::persistence::OpenAccessKeyEntity::column<"id">().expression(snapshot, "key"))).where(snapshot.unary(ruvia::db_unary_operator::is_null, service::access::persistence::OpenAccessKeyEntity::column<"deleted_at">().expression(snapshot, "key"))).group_by({ service::access::persistence::OpenAccessKeyEntity::column<"id">().expression(snapshot, "key") }).order_by(service::access::persistence::OpenAccessKeyEntity::column<"id">().expression(snapshot, "key"));
    const auto rows = co_await snapshotBuilder.get_raw_many();

    const auto version = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
    const auto versionKey = std::string(service::message::access::session::kVersionPrefix) + version;
    constexpr std::size_t chunkSize = 128;
    for (std::size_t offset = 0; offset < rows.size(); offset += chunkSize) {
        const auto end = std::min(rows.size(), offset + chunkSize);
        std::vector<std::string> command{ "HSET", versionKey };
        command.reserve(2 + (end - offset) * 2);
        for (std::size_t index = offset; index < end; ++index) {
            const auto& row = rows[index];
            command.emplace_back(row[0].value().value_or(std::string_view{}));
            command.push_back(service::message::access::session::encode(row[1].value().value_or(std::string_view{}), row[2].value().value_or(std::string_view{}), row[3].value().value_or(std::string_view{}), row[4].value().value_or(std::string_view{}), row[5].value().value_or(std::string_view{}), row[6].value().value_or(std::string_view{})));
        }
        (void)co_await context.redis().command(std::vector<std::string_view>(command.begin(), command.end()));
    }
    if (rows.empty()) {
        const std::vector<std::string> readyArguments{ "HSET", versionKey, "__ready", "1" };
        (void)co_await context.redis().command(std::vector<std::string_view>(readyArguments.begin(), readyArguments.end()));
    }
    const std::vector<std::string> expireArguments{ "EXPIRE", versionKey, "600" };
    (void)co_await context.redis().command(std::vector<std::string_view>(expireArguments.begin(), expireArguments.end()));

    const std::string activeKey(service::message::access::session::kActiveVersionKey);
    const std::string_view keys[]{ activeKey, versionKey };
    const std::string prefix(service::message::access::session::kVersionPrefix);
    const std::string grace = "120";
    const std::string_view args[]{ version, prefix, grace };
    const auto reply = co_await context.redis().eval(persistence::kSessionActivateScript, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::string) {
        service::utils::redis::throwValue("activate access-session projection", reply);
    }
    co_await transaction.commit();
    co_await service::live::publish(context.redis(), context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, service::message::live::topic::kAccess);
}

template <typename Context>
ruvia::task<void> ensure(Context& context) {
    const std::vector<std::string> activeArguments{ "GET", std::string(service::message::access::session::kActiveVersionKey) };
    const auto active = co_await context.redis().command(std::vector<std::string_view>(activeArguments.begin(), activeArguments.end()));
    if (!active.null()) {
        if (active.kind() != ruvia::redis_value::kind_type::string) {
            service::utils::redis::throwValue(
                "read active access-session projection",
                active
            );
        }
        co_return;
    }
    co_await refresh(context, true);
}

// 消费访问会话变更：存在未处理且影响会话的事件（access_key、device）时重建会话投影，随后持久化消费回执。
inline ruvia::task<void> applyChanges(ruvia::web_worker_context& context, std::string_view receiptConsumer, const std::vector<message::StreamMessage>& messages) {
    const auto eventIds = service::messaging::idempotency::eventIds(messages);
    const auto pendingIds = co_await service::messaging::idempotency::pending(context, receiptConsumer, eventIds);
    const bool changed = std::ranges::any_of(messages, [&pendingIds](const message::StreamMessage& message) {
        const auto aggregate = message.get("aggregate");
        return service::messaging::idempotency::shouldProcess(message, pendingIds) &&
            (aggregate == "access_key" || aggregate == "device");
    });
    if (changed) {
        co_await refresh(context);
    }
    co_await service::messaging::idempotency::markProcessed(context, receiptConsumer, eventIds);
}

} // namespace service::access::session

namespace service::access::delivery {

template <typename Entity>
ruvia::task<void> loadTargets(ruvia::web_worker_context& context, Catalog& result) {
    constexpr bool mqtt = std::is_same_v<Entity, service::access::persistence::OpenMqttEntity>;
    ruvia::db_query catalog(context.pool());
    const auto option = [&]<bool enabled>(std::string_view column) {
        if constexpr (enabled) {
            return catalog.coalesce({ catalog.column(column, "webhook"), catalog.value("") });
        } else {
            return catalog.value("");
        }
    };
    const auto url = [&] {
        if constexpr (mqtt) {
            return Entity::template column<"broker_url">().expression(catalog, "webhook");
        } else {
            return Entity::template column<"url">().expression(catalog, "webhook");
        }
    }();
    const auto skipTls = [&] {
        if constexpr (mqtt) {
            return catalog.value("0");
        } else {
            return catalog.case_when({ { Entity::template column<"skip_tls_verify">().expression(catalog, "webhook"), catalog.value("1") } }, catalog.value("0"));
        }
    }();
    const auto headers = [&] {
        if constexpr (mqtt) {
            return catalog.value("{}");
        } else {
            return catalog.cast(Entity::template column<"headers">().expression(catalog, "webhook"), ruvia::db_data_type::text);
        }
    }();
    const auto expiresAtMs = catalog.cast(
        catalog.coalesce({ catalog.cast(catalog.binary(catalog.extract(ruvia::db_date_part::epoch, service::access::persistence::OpenAccessKeyEntity::column<"expires_at">().expression(catalog, "key")), ruvia::db_binary_operator::multiply, catalog.value(std::int64_t{ 1000 })), ruvia::db_data_type::big_int), catalog.value(std::int64_t{ 0 }) }),
        ruvia::db_data_type::text
    );
    const auto eventTypes = catalog.call(
        "jsonb_array_elements_text",
        { catalog.case_when(
            { { catalog.binary(
                    catalog.call("jsonb_typeof", { Entity::template column<"event_types">().expression(catalog, "webhook") }),
                    ruvia::db_binary_operator::equal,
                    catalog.value("array")
                ),
                Entity::template column<"event_types">().expression(catalog, "webhook") } },
            catalog.cast(catalog.value("[]"), ruvia::db_data_type::jsonb)
        ) }
    );
    auto catalogBuilder = context.db().template repository<Entity>().query_builder("webhook");

    const std::array<ruvia::db_source_column, 1> sourceColumns1{ { { .name_ = "value" } } };
    catalogBuilder.select({ { "device_id", catalog.cast(service::access::persistence::OpenAccessKeyDeviceEntity::column<"device_id">().expression(catalog, "binding"), ruvia::db_data_type::text) }, { "name", service::configuration::persistence::DeviceEntity::column<"name">().expression(catalog, "device") }, { "protocol_params", catalog.coalesce({ catalog.binary(service::configuration::persistence::DeviceEntity::column<"protocol_params">().expression(catalog, "device"), ruvia::db_binary_operator::json_get_text, catalog.value("device_code")), catalog.value("") }) }, { "id", catalog.cast(Entity::template column<"id">().expression(catalog, "webhook"), ruvia::db_data_type::text) }, { "access_key_id", catalog.cast(Entity::template column<"access_key_id">().expression(catalog, "webhook"), ruvia::db_data_type::text) }, { "value5", url }, { "value6", option.template operator()<!mqtt>("secret") }, { "value7", headers }, { "timeout_seconds", catalog.cast(Entity::template column<"timeout_seconds">().expression(catalog, "webhook"), ruvia::db_data_type::text) }, { "value9", skipTls }, { "value10", expiresAtMs }, { "value", catalog.column("value", "event_type") }, { "value12", option.template operator()<mqtt>("username") }, { "value13", option.template operator()<mqtt>("password") } }).template join<service::access::persistence::OpenAccessKeyEntity>(ruvia::db_join_type::inner, "key", catalog.binary(service::access::persistence::OpenAccessKeyEntity::column<"id">().expression(catalog, "key"), ruvia::db_binary_operator::equal, Entity::template column<"access_key_id">().expression(catalog, "webhook"))).template join<service::access::persistence::OpenAccessKeyDeviceEntity>(ruvia::db_join_type::inner, "binding", catalog.binary(service::access::persistence::OpenAccessKeyDeviceEntity::column<"access_key_id">().expression(catalog, "binding"), ruvia::db_binary_operator::equal, service::access::persistence::OpenAccessKeyEntity::column<"id">().expression(catalog, "key"))).template join<service::configuration::persistence::DeviceEntity>(ruvia::db_join_type::inner, "device", catalog.binary(service::configuration::persistence::DeviceEntity::column<"id">().expression(catalog, "device"), ruvia::db_binary_operator::equal, service::access::persistence::OpenAccessKeyDeviceEntity::column<"device_id">().expression(catalog, "binding"))).join_function(ruvia::db_join_type::cross, eventTypes, "event_type", {}, { .lateral_ = true, .columns_ = sourceColumns1 }).where(catalog.unary(ruvia::db_unary_operator::is_null, Entity::template column<"deleted_at">().expression(catalog, "webhook"))).and_where(catalog.binary(Entity::template column<"status">().expression(catalog, "webhook"), ruvia::db_binary_operator::equal, catalog.value("enabled"))).and_where(catalog.unary(ruvia::db_unary_operator::is_null, service::access::persistence::OpenAccessKeyEntity::column<"deleted_at">().expression(catalog, "key"))).and_where(catalog.binary(service::access::persistence::OpenAccessKeyEntity::column<"status">().expression(catalog, "key"), ruvia::db_binary_operator::equal, catalog.value("enabled"))).and_where(catalog.binary(catalog.unary(ruvia::db_unary_operator::is_null, service::access::persistence::OpenAccessKeyEntity::column<"expires_at">().expression(catalog, "key")), ruvia::db_binary_operator::or_value, catalog.binary(service::access::persistence::OpenAccessKeyEntity::column<"expires_at">().expression(catalog, "key"), ruvia::db_binary_operator::greater, catalog.call("now")))).and_where(catalog.unary(ruvia::db_unary_operator::is_null, service::configuration::persistence::DeviceEntity::column<"deleted_at">().expression(catalog, "device"))).order_by(service::access::persistence::OpenAccessKeyDeviceEntity::column<"device_id">().expression(catalog, "binding")).add_order_by(catalog.column("value", "event_type")).add_order_by(Entity::template column<"id">().expression(catalog, "webhook"));
    if constexpr (mqtt) {
        const auto scope = catalog.case_when({ { catalog.binary(catalog.column("value", "event_type"), ruvia::db_binary_operator::in, catalog.list({ catalog.value("device.alert.triggered"), catalog.value("device.alert.resolved") })), catalog.value("alert:read") }, { catalog.binary(catalog.column("value", "event_type"), ruvia::db_binary_operator::in, catalog.list({ catalog.value("device.command.accepted"), catalog.value("device.command.updated") })), catalog.value("device:command") } }, catalog.value("device:realtime"));
        catalogBuilder.and_where(catalog.call("jsonb_exists", { service::access::persistence::OpenAccessKeyEntity::column<"scopes">().expression(catalog, "key"), scope }));
    }
    const auto rows = co_await catalogBuilder.get_raw_many();
    for (const auto& row : rows) {
        const std::string deviceId(row[0].value().value_or(std::string_view{}));
        auto& device = result[deviceId];
        device.name.assign(row[1].value().value_or(std::string_view{}));
        device.code.assign(row[2].value().value_or(std::string_view{}));
        const auto timeout =
            service::utils::parseInt64(std::optional<std::string_view>{ row[8].value().value_or(std::string_view{}) })
                .value_or(5);
        const auto expiresAtMs =
            service::utils::parseInt64(std::optional<std::string_view>{ row[10].value().value_or(std::string_view{}) })
                .value_or(0);
        device.targets[std::string(row[11].value().value_or(std::string_view{}))].push_back(
            { std::string(row[3].value().value_or(std::string_view{})), std::string(row[4].value().value_or(std::string_view{})), std::string(row[5].value().value_or(std::string_view{})), std::string(row[6].value().value_or(std::string_view{})), std::string(row[7].value().value_or(std::string_view{})), timeout > 0 ? timeout : std::int64_t{ 5 }, row[9].value().value_or(std::string_view{}) == "1", expiresAtMs, std::nullopt }
        );
        if constexpr (mqtt) {
            auto& target = device.targets[std::string(row[11].value().value_or(std::string_view{}))].back();
            target.mqtt = MqttDestination{ std::string(row[12].value().value_or(std::string_view{})),
                                           std::string(row[13].value().value_or(std::string_view{})) };
        }
    }
    co_return;
}

inline ruvia::task<Catalog> loadCatalog(ruvia::web_worker_context& context) {
    Catalog result;
    co_await loadTargets<service::access::persistence::OpenWebhookEntity>(context, result);
    co_await loadTargets<service::access::persistence::OpenMqttEntity>(context, result);
    co_return result;
}

// 每批消费都按当前授权重新加载投递目录（各实例独立消费共享数据），随后持久化目录变更事件的消费回执。
inline ruvia::task<Catalog> reloadCatalog(ruvia::web_worker_context& context, std::string_view receiptConsumer, const std::vector<message::StreamMessage>& messages) {
    const auto eventIds = service::messaging::idempotency::eventIds(messages);
    auto catalog = co_await loadCatalog(context);
    co_await service::messaging::idempotency::markProcessed(context, receiptConsumer, eventIds);
    co_return catalog;
}

template <typename Entity>
ruvia::task<void> persistTargetResults(
    ruvia::web_worker_context& context,
    const std::vector<message::StreamMessage>& messages
) {
    constexpr bool mqtt = std::is_same_v<Entity, service::access::persistence::OpenMqttEntity>;
    using Log = service::access::persistence::OpenAccessLogEntity;
    if (messages.empty()) {
        co_return;
    }
    ruvia::db_query incoming(context.pool());
    for (std::size_t index = 0; index < messages.size(); ++index) {
        const auto httpStatus = service::utils::parseInt64(
            std::optional<std::string_view>(messages[index].get("http_status"))
        );
        const auto completedAt = service::utils::parseInt64(
            std::optional<std::string_view>(messages[index].get("completed_at_ms"))
        );
        incoming.values({ incoming.cast(incoming.value(static_cast<std::int64_t>(index)), ruvia::db_data_type::big_int), incoming.cast(incoming.value(messages[index].get("log_id")), ruvia::db_data_type::uuid), incoming.cast(incoming.value(messages[index].get("access_key_id")), ruvia::db_data_type::uuid), incoming.cast(incoming.value(messages[index].get(mqtt ? "mqtt_id" : "webhook_id")), ruvia::db_data_type::uuid), incoming.cast(incoming.value(messages[index].get("event_type")), ruvia::db_data_type::text), incoming.cast(incoming.value(messages[index].get("status")), ruvia::db_data_type::text), incoming.cast(incoming.value(messages[index].get("target")), ruvia::db_data_type::text), incoming.cast(incoming.value(httpStatus.value_or(0)), ruvia::db_data_type::big_int), incoming.cast(incoming.value(messages[index].get("device_id")), ruvia::db_data_type::uuid), incoming.cast(incoming.value(messages[index].get("device_code")), ruvia::db_data_type::text), incoming.cast(incoming.value(messages[index].get("message")), ruvia::db_data_type::text), incoming.cast(incoming.value(messages[index].get("request_payload")), ruvia::db_data_type::jsonb), incoming.cast(incoming.value(messages[index].get("response_payload")), ruvia::db_data_type::jsonb), incoming.cast(incoming.value(completedAt.value_or(service::message::utcNowMilliseconds())), ruvia::db_data_type::big_int) });
    }
    ruvia::db_query latest(context.pool());
    latest
        .select(latest.star())
        .distinct_on({ latest.column("target_id", "incoming") })
        .from("incoming", "incoming")
        .order_by(latest.column("target_id", "incoming"))
        .add_order_by(latest.column("completed_at_ms", "incoming"), ruvia::db_order_direction::desc)
        .add_order_by(latest.column("sequence", "incoming"), ruvia::db_order_direction::desc);

    ruvia::db_query summary(context.pool());
    const auto successfulAt = summary.filter(
        summary.aggregate("max", { summary.column("completed_at_ms", "incoming") }),
        summary.binary(summary.column("status", "incoming"), ruvia::db_binary_operator::equal, summary.value("success"))
    );
    const auto failedAt = summary.filter(
        summary.aggregate("max", { summary.column("completed_at_ms", "incoming") }),
        summary.binary(summary.column("status", "incoming"), ruvia::db_binary_operator::equal, summary.value("failed"))
    );
    summary
        .select({ summary.column("target_id", "incoming"), summary.alias(successfulAt, "last_success_ms"), summary.alias(failedAt, "last_failure_ms") })
        .from("incoming", "incoming")
        .group_by({ summary.column("target_id", "incoming") });

    ruvia::db_query updated(context.pool());
    const auto asTimestamp = [&updated](ruvia::db_expression milliseconds) {
        return updated.call(
            "to_timestamp",
            { updated.binary(
                updated.cast(milliseconds, ruvia::db_data_type::double_value),
                ruvia::db_binary_operator::divide,
                updated.value(1000.0)
            ) }
        );
    };
    const auto epoch = updated.call("to_timestamp", { updated.value(0.0) });
    const auto latestTimestamp = asTimestamp(
        updated.column("completed_at_ms", "latest")
    );
    const auto successTimestamp = asTimestamp(
        updated.column("last_success_ms", "summary")
    );
    const auto failureTimestamp = asTimestamp(
        updated.column("last_failure_ms", "summary")
    );
    const auto latestIsNewer = updated.binary(
        updated.unary(ruvia::db_unary_operator::is_null, Entity::template column<"last_triggered_at">().expression(updated, "webhook")),
        ruvia::db_binary_operator::or_value,
        updated.binary(Entity::template column<"last_triggered_at">().expression(updated, "webhook"), ruvia::db_binary_operator::less_equal, latestTimestamp)
    );
    updated
        .update(Entity::table_name(), "webhook")
        .set(Entity::template column_name<"last_triggered_at">(), updated.greatest({ updated.coalesce({ Entity::template column<"last_triggered_at">().expression(updated, "webhook"), epoch }), latestTimestamp }))
        .set(Entity::template column_name<"last_success_at">(), updated.case_when({ { updated.unary(ruvia::db_unary_operator::is_null, updated.column("last_success_ms", "summary")), Entity::template column<"last_success_at">().expression(updated, "webhook") } }, updated.greatest({ updated.coalesce({ Entity::template column<"last_success_at">().expression(updated, "webhook"), epoch }), successTimestamp })))
        .set(Entity::template column_name<"last_failure_at">(), updated.case_when({ { updated.unary(ruvia::db_unary_operator::is_null, updated.column("last_failure_ms", "summary")), Entity::template column<"last_failure_at">().expression(updated, "webhook") } }, updated.greatest({ updated.coalesce({ Entity::template column<"last_failure_at">().expression(updated, "webhook"), epoch }), failureTimestamp })))
        .set(Entity::template column_name<"last_error">(), updated.case_when({ { latestIsNewer, updated.null_if(updated.column("message", "latest"), updated.value("")) } }, Entity::template column<"last_error">().expression(updated, "webhook")))
        .set(Entity::template column_name<"updated_at">(), updated.call("now"))
        .update_from("summary", "summary")
        .join(ruvia::db_join_type::inner, "latest", updated.binary(updated.column("target_id", "latest"), ruvia::db_binary_operator::equal, updated.column("target_id", "summary")), "latest")
        .where(updated.binary(Entity::template column<"id">().expression(updated, "webhook"), ruvia::db_binary_operator::equal, updated.column("target_id", "summary")))
        .and_where(updated.unary(ruvia::db_unary_operator::is_null, Entity::template column<"deleted_at">().expression(updated, "webhook")))
        .returning({ Entity::template column<"id">().expression(updated, "webhook") });
    if constexpr (!mqtt) {
        updated.set(Entity::template column_name<"last_http_status">(), updated.case_when({ { latestIsNewer, updated.null_if(updated.column("http_status", "latest"), updated.value(std::int64_t{ 0 })) } }, Entity::template column<"last_http_status">().expression(updated, "webhook")));
    }

    ruvia::db_query logRows(context.pool());
    logRows
        .select({ logRows.column("log_id", "incoming"), logRows.column("access_key_id", "incoming"), mqtt ? logRows.null_value() : logRows.column("target_id", "incoming"), mqtt ? logRows.column("target_id", "incoming") : logRows.null_value(), logRows.value("push"), logRows.value(mqtt ? "mqtt" : "webhook"), logRows.column("event_type", "incoming"), logRows.column("status", "incoming"), logRows.value(mqtt ? "PUBLISH" : "POST"), logRows.column("target", "incoming"), logRows.null_if(logRows.column("http_status", "incoming"), logRows.value(std::int64_t{ 0 })), logRows.column("device_id", "incoming"), logRows.null_if(logRows.column("device_code", "incoming"), logRows.value("")), logRows.null_if(logRows.column("message", "incoming"), logRows.value("")), logRows.column("request_payload", "incoming"), logRows.column("response_payload", "incoming") })
        .from("incoming", "incoming");

    const std::array<std::string_view, 16> writeColumns{ Log::column_name<"id">(), Log::column_name<"access_key_id">(), Log::column_name<"webhook_id">(), Log::column_name<"mqtt_id">(), Log::column_name<"direction">(), Log::column_name<"action">(), Log::column_name<"event_type">(), Log::column_name<"status">(), Log::column_name<"http_method">(), Log::column_name<"target">(), Log::column_name<"http_status">(), Log::column_name<"device_id">(), Log::column_name<"device_code">(), Log::column_name<"message">(), Log::column_name<"request_payload">(), Log::column_name<"response_payload">() };
    auto writeBuilder = context.db().template repository<service::access::persistence::OpenAccessLogEntity>().insert_builder();

    const std::array<ruvia::borrowed_text, 14> sourceColumns2{ { "sequence", "log_id", "access_key_id", "target_id", "event_type", "status", "target", "http_status", "device_id", "device_code", "message", "request_payload", "response_payload", "completed_at_ms" } };
    const std::array<ruvia::borrowed_text, 1> conflictColumns3{ { Log::column_name<"id">() } };
    writeBuilder.with("incoming", incoming, { .columns_ = sourceColumns2 }).with("latest", latest).with("summary", summary).with("updated", updated).insert_from(writeColumns, logRows).on_conflict({ .conflict_paths_ = conflictColumns3, .do_nothing_ = true });
    (void)co_await writeBuilder.execute();
}

inline ruvia::task<void> persistResults(ruvia::web_worker_context& context, const std::vector<message::StreamMessage>& messages) {
    std::vector<message::StreamMessage> webhooks, mqtt;
    for (const auto& message : messages) {
        if (message.get("mqtt_id").empty()) {
            webhooks.push_back(message);
        } else {
            mqtt.push_back(message);
        }
    }
    co_await persistTargetResults<service::access::persistence::OpenWebhookEntity>(context, webhooks);
    co_await persistTargetResults<service::access::persistence::OpenMqttEntity>(context, mqtt);
}

inline ruvia::task<void> persistAudits(
    ruvia::web_worker_context& context,
    const std::vector<message::StreamMessage>& messages
) {
    using Log = service::access::persistence::OpenAccessLogEntity;
    if (messages.empty()) {
        co_return;
    }
    ruvia::db_query incoming(context.pool());
    for (std::size_t index = 0; index < messages.size(); ++index) {
        const auto httpStatus = service::utils::parseInt64(
            std::optional<std::string_view>(messages[index].get("http_status"))
        );
        const auto usedAt = service::utils::parseInt64(
            std::optional<std::string_view>(messages[index].get("used_at_ms"))
        );
        incoming.values({ incoming.cast(incoming.value(static_cast<std::int64_t>(index)), ruvia::db_data_type::big_int), incoming.cast(incoming.value(messages[index].get("log_id")), ruvia::db_data_type::uuid), incoming.cast(incoming.value(messages[index].get("access_key_id")), ruvia::db_data_type::uuid), incoming.cast(incoming.value(messages[index].get("action")), ruvia::db_data_type::text), incoming.cast(incoming.value(messages[index].get("http_method")), ruvia::db_data_type::text), incoming.cast(incoming.value(messages[index].get("target")), ruvia::db_data_type::text), incoming.cast(incoming.value(messages[index].get("request_ip")), ruvia::db_data_type::text), incoming.cast(incoming.value(httpStatus.value_or(0)), ruvia::db_data_type::integer), incoming.cast(incoming.null_if(incoming.value(messages[index].get("device_id")), incoming.value("")), ruvia::db_data_type::uuid), incoming.cast(incoming.value(messages[index].get("request_payload")), ruvia::db_data_type::jsonb), incoming.cast(incoming.value(messages[index].get("response_payload")), ruvia::db_data_type::jsonb), incoming.cast(incoming.value(usedAt.value_or(service::message::utcNowMilliseconds())), ruvia::db_data_type::big_int) });
    }
    ruvia::db_query latestUsage(context.pool());
    latestUsage
        .select({ latestUsage.column("access_key_id", "incoming"), latestUsage.column("request_ip", "incoming"), latestUsage.column("used_at_ms", "incoming") })
        .distinct_on({ latestUsage.column("access_key_id", "incoming") })
        .from("incoming", "incoming")
        .order_by(latestUsage.column("access_key_id", "incoming"))
        .add_order_by(latestUsage.column("used_at_ms", "incoming"), ruvia::db_order_direction::desc)
        .add_order_by(latestUsage.column("sequence", "incoming"), ruvia::db_order_direction::desc);

    ruvia::db_query usageUpdated(context.pool());
    const auto latestTimestamp = usageUpdated.call(
        "to_timestamp",
        { usageUpdated.binary(
            usageUpdated.cast(usageUpdated.column("used_at_ms", "latest"), ruvia::db_data_type::double_value),
            ruvia::db_binary_operator::divide,
            usageUpdated.value(1000.0)
        ) }
    );
    usageUpdated
        .update(service::access::persistence::OpenAccessKeyEntity::table_name(), "key")
        .set(service::access::persistence::OpenAccessKeyEntity::column_name<"last_used_at">(), latestTimestamp)
        .set(service::access::persistence::OpenAccessKeyEntity::column_name<"last_used_ip">(), usageUpdated.null_if(usageUpdated.column("request_ip", "latest"), usageUpdated.value("")))
        .update_from("latest_usage", "latest")
        .where(usageUpdated.binary(service::access::persistence::OpenAccessKeyEntity::column<"id">().expression(usageUpdated, "key"), ruvia::db_binary_operator::equal, usageUpdated.column("access_key_id", "latest")))
        .and_where(usageUpdated.binary(
            usageUpdated.unary(ruvia::db_unary_operator::is_null, service::access::persistence::OpenAccessKeyEntity::column<"last_used_at">().expression(usageUpdated, "key")),
            ruvia::db_binary_operator::or_value,
            usageUpdated.binary(service::access::persistence::OpenAccessKeyEntity::column<"last_used_at">().expression(usageUpdated, "key"), ruvia::db_binary_operator::less_equal, latestTimestamp)
        ))
        .returning({ service::access::persistence::OpenAccessKeyEntity::column<"id">().expression(usageUpdated, "key") });

    ruvia::db_query updateBarrier(context.pool());
    updateBarrier
        .select(updateBarrier.alias(
            updateBarrier.aggregate("count", { updateBarrier.star() }),
            "updated_count"
        ))
        .from("usage_updated");

    ruvia::db_query logRows(context.pool());
    logRows
        .select({ logRows.column("log_id", "incoming"), logRows.column("access_key_id", "incoming"), logRows.value("pull"), logRows.column("action", "incoming"), logRows.value("success"), logRows.null_if(logRows.column("http_method", "incoming"), logRows.value("")), logRows.null_if(logRows.column("target", "incoming"), logRows.value("")), logRows.null_if(logRows.column("request_ip", "incoming"), logRows.value("")), logRows.null_if(logRows.column("http_status", "incoming"), logRows.value(std::int64_t{ 0 })), logRows.column("device_id", "incoming"), logRows.column("request_payload", "incoming"), logRows.column("response_payload", "incoming") })
        .from("incoming", "incoming")
        .join(ruvia::db_join_type::cross, updateBarrier, {}, "update_barrier")
        .where(logRows.binary(logRows.column("updated_count", "update_barrier"), ruvia::db_binary_operator::greater_equal, logRows.value(std::int64_t{ 0 })));

    const std::array<std::string_view, 12> writeColumns{ Log::column_name<"id">(), Log::column_name<"access_key_id">(), Log::column_name<"direction">(), Log::column_name<"action">(), Log::column_name<"status">(), Log::column_name<"http_method">(), Log::column_name<"target">(), Log::column_name<"request_ip">(), Log::column_name<"http_status">(), Log::column_name<"device_id">(), Log::column_name<"request_payload">(), Log::column_name<"response_payload">() };
    auto writeBuilder = context.db().template repository<service::access::persistence::OpenAccessLogEntity>().insert_builder();

    const std::array<ruvia::borrowed_text, 12> sourceColumns4{ { "sequence", "log_id", "access_key_id", "action", "http_method", "target", "request_ip", "http_status", "device_id", "request_payload", "response_payload", "used_at_ms" } };
    const std::array<ruvia::borrowed_text, 1> conflictColumns5{ { Log::column_name<"id">() } };
    writeBuilder.with("incoming", incoming, { .columns_ = sourceColumns4 }).with("latest_usage", latestUsage, { .materialization_ = ruvia::db_materialization::materialized }).with("usage_updated", usageUpdated).insert_from(writeColumns, logRows).on_conflict({ .conflict_paths_ = conflictColumns5, .do_nothing_ = true });
    (void)co_await writeBuilder.execute();
}

class DeliveryService final {
  public:
    static std::string deviceReference(std::string_view id, std::string_view code, std::string_view name) {
        return "{\"id\":" + service::utils::jsonQuoted(id) + ",\"code\":" + service::utils::jsonQuoted(code) +
            ",\"name\":" + service::utils::jsonQuoted(name) + "}";
    }

    static std::string jsonFieldOr(const ruvia::json_value& object, std::string_view field, std::string_view fallback) {
        const auto value = object.get<ruvia::json_value>(field);
        return value ? std::string(value->view()) : std::string(fallback);
    }

    static std::string mergeEventData(std::string_view deviceJson, std::string_view rawData) {
        std::string result = "{\"device\":" + std::string(deviceJson);
        std::optional<ruvia::json_value> parsed;
        try {
            parsed = ruvia::json_value::parse(rawData);
        } catch (const std::invalid_argument&) {
            // 历史事件正文损坏时仍保留可信设备身份。
        }
        if (parsed && parsed->is_object()) {
            (void)parsed->for_each_field(
                [&](std::string_view name, const ruvia::json_value& value) {
                    if (name != "device") {
                        result += "," + service::utils::jsonQuoted(name) + ":" + std::string(value.view());
                    }
                    return true;
                }
            );
        }
        result.push_back('}');
        return result;
    }

    static std::string imageEventData(std::string_view deviceJson, std::string_view rawData, std::string_view occurredAt) {
        std::optional<ruvia::json_value> parsed;
        try {
            parsed = ruvia::json_value::parse(rawData);
        } catch (const std::invalid_argument&) {
            return mergeEventData(deviceJson, "{}");
        }
        if (!parsed->is_object()) {
            return mergeEventData(deviceJson, "{}");
        }
        const auto values = parsed->get<ruvia::json_value>("values");
        if (!values || !values->is_object()) {
            return mergeEventData(deviceJson, rawData);
        }

        std::string image;
        (void)values->for_each_field(
            [&](std::string_view id, const ruvia::json_value& item) {
                if (!image.empty()) {
                    return true;
                }
                if (!item.is_object()) {
                    return true;
                }
                const auto type = item.get<ruvia::string>("type");
                const auto value = item.get<ruvia::json_value>("value");
                const auto text = item.get<ruvia::string>("value");
                const bool jpeg = type && type->view() == "JPEG";
                const bool dataUrl = text && text->view().starts_with("data:image/");
                if (!value || (!jpeg && !dataUrl)) {
                    return true;
                }
                const auto name = item.get<ruvia::string>("name");
                image = "{\"id\":" + service::utils::jsonQuoted(id) + ",\"name\":" +
                    service::utils::jsonQuoted(name ? name->view() : std::string_view("image")) +
                    ",\"data\":" + std::string(value->view()) +
                    ",\"time\":" + service::utils::jsonQuoted(occurredAt) + "}";
                return true;
            }
        );
        if (image.empty()) {
            return mergeEventData(deviceJson, rawData);
        }
        return "{\"device\":" + std::string(deviceJson) + ",\"image\":" + image + "}";
    }

    static std::string commandEventData(std::string_view deviceJson, const ruvia::json_value& payload) {
        return "{\"device\":" + std::string(deviceJson) + ",\"command\":{\"id\":" +
            jsonFieldOr(payload, "commandId", "null") + ",\"status\":" +
            jsonFieldOr(payload, "status", "null") + ",\"reason\":" +
            jsonFieldOr(payload, "reason", "null") + ",\"elements\":" +
            jsonFieldOr(payload, "elements", "[]") + ",\"actual_values\":" +
            jsonFieldOr(payload, "actualValues", "[]") + "}}";
    }

    static ruvia::task<std::string> realtimeData(ruvia::web_worker_context& context, std::string_view deviceId, const DeviceCatalog& device) {
        const std::vector<std::string> arguments{ "HGETALL", service::message::telemetry::latestKey(deviceId) };
        const auto reply = co_await context.redis().command(std::vector<std::string_view>(arguments.begin(), arguments.end()));
        auto record = LatestValuesRecord::decode(reply);
        std::vector<LatestValuesRecord::Point> points;
        points.reserve(record.latest.size());
        for (auto& [id, point] : record.latest) {
            if ((!record.hasConfigured || record.configured.contains(id)) && point.encode != "JPEG") {
                points.push_back(std::move(point));
            }
        }
        std::ranges::sort(points, [](const LatestValuesRecord::Point& left, const LatestValuesRecord::Point& right) {
            return left.sort == right.sort ? left.id < right.id : left.sort < right.sort;
        });
        std::string body = "{\"device\":" + deviceReference(deviceId, device.code, device.name) +
            ",\"points\":[";
        for (std::size_t index = 0; index < points.size(); ++index) {
            if (index != 0) {
                body.push_back(',');
            }
            const auto time = points[index].observedAt > 0
                ? service::utils::jsonQuoted(service::common::utcTimestampFromMilliseconds(points[index].observedAt))
                : std::string("null");
            body += "{\"id\":" + service::utils::jsonQuoted(points[index].id) +
                ",\"name\":" + service::utils::jsonQuoted(points[index].name) +
                ",\"access\":" + service::utils::jsonQuoted(points[index].access) +
                ",\"value\":" + points[index].value +
                ",\"unit\":" + service::utils::jsonQuoted(points[index].unit) +
                ",\"time\":" + time + "}";
        }
        body += "]}";
        co_return body;
    }

    static ruvia::task<Delivery> buildDelivery(ruvia::web_worker_context& context, const message::StreamMessage& message, const DeviceCatalog& catalog) {
        Delivery delivery;
        delivery.id = message.get("event_id").empty() ? service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>())
                                                      : std::string(message.get("event_id"));
        delivery.eventType = std::string(message.get("event_type"));
        delivery.deviceId = std::string(message.get("device_id"));
        delivery.deviceCode = std::string(message.get("device_code"));
        const auto occurredAt = service::utils::parseInt64(
            std::optional<std::string_view>(message.get("occurred_at_ms"))
        );
        delivery.occurredAt = occurredAt ? service::common::utcTimestampFromMilliseconds(*occurredAt) : service::common::utcTimestampNow();

        const auto device =
            deviceReference(delivery.deviceId, delivery.deviceCode, catalog.name);
        const auto rawData = message.get("data_json");
        std::string data;
        if (delivery.eventType == "device.data.reported") {
            data = co_await realtimeData(context, delivery.deviceId, catalog);
        } else if (delivery.eventType == "device.image.reported") {
            data = imageEventData(device, rawData, delivery.occurredAt);
        } else if (delivery.eventType == "device.command.accepted" || delivery.eventType == "device.command.updated") {
            const auto payload = ruvia::json_value::parse(rawData);
            data = payload.is_object()
                ? commandEventData(device, payload)
                : mergeEventData(device, rawData);
        } else {
            data = mergeEventData(device, rawData);
        }
        delivery.body =
            service::message::webhookEnvelope(delivery.eventType, delivery.occurredAt, delivery.id, data);
        co_return delivery;
    }

    static constexpr std::int64_t kDeliveryProgressTtlSeconds = 7 * 24 * 60 * 60;

    template <typename Redis>
    static ruvia::task<std::set<std::string, std::less<>>>
    completedTargets(const Redis& redis, const Delivery& delivery) {
        const std::vector<std::string> replyArguments{ "HKEYS", DeliveryProgressRecord::key(delivery.eventType, delivery.id) };
        const auto reply = co_await redis.command(std::vector<std::string_view>(replyArguments.begin(), replyArguments.end()));
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            utils::redis::throwValue("read webhook delivery progress", reply);
        }
        co_return DeliveryProgressRecord::decode(reply).completedTargets;
    }

    static ruvia::task<void> enqueueResult(ruvia::web_worker_context& context, const Target& target, const Delivery& delivery, const DeliveryResponse& response, bool success) {
        const auto error = service::utils::sanitize(response.error, 1000);
        const auto status = success ? "success" : "failed";
        const auto logId = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
        const auto responseJson = (target.mqtt ? std::string("{\"qos\":1,\"acknowledged\":") + (success ? "true" : "false") : "{\"httpStatus\":" + std::to_string(response.status)) +
            ",\"body\":" + service::utils::jsonQuoted(service::utils::sanitize(response.body, 2000)) +
            (error.empty() ? "" : ",\"error\":" + service::utils::jsonQuoted(error)) + "}";
        const auto resultStream = service::message::access::stream::deliveryResult();
        const auto wakeStream = service::message::workerWakeStream(std::nullopt, context.worker_state<std::optional<common::WorkerIdentity>>().value().instanceId);
        const std::vector<std::string> keyStore{
            resultStream,
            DeliveryProgressRecord::key(delivery.eventType, delivery.id),
            wakeStream,
            service::message::consumer::inputChannel(context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, resultStream),
            service::message::consumer::inputChannel(context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, wakeStream)
        };
        const std::vector<std::string> argumentStore{
            target.id,
            "100000",
            std::to_string(kDeliveryProgressTtlSeconds),
            std::to_string(service::message::kWorkerWakeCapacity),
            std::string(service::message::workerStreamTaskName(service::message::WorkerStreamTask::Webhook)),
            (success || !target.mqtt) ? "1" : "0",
            "log_id",
            logId,
            "access_key_id",
            target.accessKeyId,
            target.mqtt ? "mqtt_id" : "webhook_id",
            target.id,
            "event_type",
            delivery.eventType,
            "status",
            status,
            "target",
            target.mqtt ? target.url + "/" + service::message::openMqttTopic(delivery.deviceId) : target.url,
            "http_status",
            std::to_string(response.status),
            "device_id",
            delivery.deviceId,
            "device_code",
            delivery.deviceCode,
            "message",
            error,
            "request_payload",
            delivery.body,
            "response_payload",
            responseJson,
            "completed_at_ms",
            std::to_string(service::message::utcNowMilliseconds()),
        };
        const std::vector<std::string_view> keys(keyStore.begin(), keyStore.end());
        const std::vector<std::string_view> arguments(argumentStore.begin(), argumentStore.end());
        const auto reply = co_await context.redis().eval(service::access::persistence::kDeliveryResultScript, keys, arguments);
        if (!reply.null() && reply.kind() != ruvia::redis_value::kind_type::string) {
            utils::redis::throwValue("enqueue webhook result", reply);
        }
    }
};

} // namespace service::access::delivery

namespace service::access::event {

// 将一条开放接入事件按 service::message::access::stream 契约加入 pipeline（scriptSha 为 kPublishEventScript）。
template <typename Pipeline>
void queue(Pipeline& pipeline, const service::message::RedisChannelScope& scope, std::string_view scriptSha, std::string_view eventId, std::string_view eventType, std::string_view deviceId, std::string_view deviceCode, std::int64_t occurredAtMs, std::string_view dataJson, std::string_view instanceId) {
    const auto publication = service::message::access::stream::eventPublication(
        scope,
        eventId,
        eventType,
        deviceId,
        deviceCode,
        occurredAtMs,
        dataJson,
        instanceId
    );
    std::array<std::string_view, 24> command{ "EVALSHA", scriptSha, "5" };
    std::ranges::copy(publication.keys, command.begin() + 3);
    std::ranges::copy(publication.arguments, command.begin() + 8);
    pipeline.command(std::span<const std::string_view>(command));
}

template <typename Redis>
ruvia::task<void> publishMany(
    const Redis& redis,
    const service::message::RedisChannelScope& scope,
    const std::vector<message::ParsedDeviceMessage>& messages,
    std::string_view instanceId
) {
    if (messages.empty()) {
        co_return;
    }
    const auto scriptSha = co_await redis.script_load(service::access::persistence::kPublishEventScript);
    auto pipeline = redis.pipeline();
    for (const auto& parsed : messages) {
        const auto eventType =
            parsed.eventKind == "image"
            ? "device.image.reported"
            : "device.data.reported";
        queue(pipeline, scope, scriptSha, parsed.messageId, eventType, parsed.deviceId, parsed.deviceCode, parsed.observedAtMs, parsed.valuesJson, instanceId);
    }
    const auto replies = co_await std::move(pipeline).exec();
    utils::redis::requirePipelineSuccess("publish open-access events", replies);
}

} // namespace service::access::event

namespace service::access::audit {

inline constexpr std::size_t kCapacity = 100000;

template <typename Redis>
ruvia::task<void> publish(const Redis& redis, const service::message::RedisChannelScope& scope, std::string_view logId, std::string_view action, std::string_view accessKeyId, std::string_view method, std::string_view target, std::string_view requestIp, std::int64_t httpStatus, std::string_view instanceId, std::string_view deviceId = {}, std::string_view requestPayload = "{}", std::string_view responsePayload = "{}") {
    const std::vector<service::message::StreamField> fields{
        { "log_id", std::string(logId) },
        { "access_key_id", std::string(accessKeyId) },
        { "action", std::string(action) },
        { "http_method", std::string(method) },
        { "target", std::string(target) },
        { "request_ip", std::string(requestIp) },
        { "http_status", std::to_string(httpStatus) },
        { "device_id", std::string(deviceId) },
        { "request_payload", std::string(requestPayload) },
        { "response_payload", std::string(responsePayload) },
        { "used_at_ms", std::to_string(service::message::utcNowMilliseconds()) },
    };
    (void)co_await service::messaging::stream::addAndWake(
        redis,
        scope,
        service::message::access::stream::audit(),
        fields,
        std::nullopt,
        service::message::WorkerStreamTask::Webhook,
        kCapacity,
        instanceId
    );
}

} // namespace service::access::audit

namespace service::access {

class AccessOperationService final {
  public:
    static ruvia::task<std::string> executeOperation(ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        if (stop.stop_requested()) {
            service::common::fail(10004, "Access projection cancelled", 503);
        }
        if (operation == "refresh") {
            co_await session::refresh(context);
            co_return "{}";
        }
        if (operation == "audit") {
            const auto request = ruvia::from_json<service::message::access::AuditRequest>(payload, { .resource_ = context.pool() });

            const auto text = [](const std::optional<ruvia::string>& value) {
                return value ? std::string(value->view()) : std::string{};
            };
            const auto json = [](const std::optional<ruvia::json_value>& value) {
                return value ? std::string(value->view()) : std::string("{}");
            };
            const auto& httpStatus = request.get<"httpStatus">();
            const std::int64_t status = httpStatus ? static_cast<std::int64_t>(*httpStatus) : 200;
            const auto action = text(request.get<"action">());
            const auto accessKeyId = text(request.get<"accessKeyId">());
            const auto method = text(request.get<"method">());
            const auto target = text(request.get<"target">());
            const auto requestIp = text(request.get<"requestIp">());
            const auto deviceId = text(request.get<"deviceId">());
            const auto requestPayload = json(request.get<"requestPayload">());
            const auto responsePayload = json(request.get<"responsePayload">());
            co_await audit::publish(context.redis(), context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()), action, accessKeyId, method, target, requestIp, status, context.worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId, deviceId, requestPayload, responsePayload);
            co_return "{}";
        }
        service::common::fail(10002, "Unknown access operation", 400);
    }
};

} // namespace service::access
