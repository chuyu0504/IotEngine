#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <memory_resource>
#include <openssl/rand.h>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/model.h>
#include <ruvia/web/redis/redis_handle.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/modules/alert/alert.entity.h"
#include "service/modules/command/command.service.h"
#include "service/modules/device/device.entity.h"
#include "service/modules/device/device.service.h"
#include "service/modules/open_access/open_access.entity.h"
#include "service/modules/open_access/open_access.types.h"
#include "service/modules/system/outbox/outbox.service.h"
#include "service/utils/crypto.h"
#include "service/utils/json.h"
#include "service/utils/number.h"
#include "service/utils/rpc_client.h"
#include "service/utils/text.h"
#include "service/utils/uuid.h"

namespace service::open_access {

namespace db = service::common::database;

inline std::string generateAccessKey() {
    std::array<unsigned char, 24> random{};
    if (RAND_bytes(random.data(), static_cast<int>(random.size())) != 1) {
        throw std::runtime_error("access-key random generation failed");
    }
    return "ak_" + service::utils::hexEncode(random.data(), random.size());
}

class AccessService final {
  public:
    static AccessService& instance() {
        static thread_local AccessService service;
        return service;
    }

    template <typename Context>
    ruvia::task<std::string> listKeys(Context& c) {
        ruvia::db_query listed(c.pool());
        const auto keyId = listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key");
        const auto bindingId = listed.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">(), "binding");
        const auto item = listed.call(
            "jsonb_build_object",
            { db::text(listed, "id"), db::text(listed, keyId), db::text(listed, "name"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), "key"), db::text(listed, "accessKeyPrefix"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"access_key_prefix">(), "key"), db::text(listed, "status"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"status">(), "key"), db::text(listed, "scopes"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"scopes">(), "key"), db::text(listed, "expiresAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"expires_at">(), "key") }), db::text(listed, "lastUsedAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"last_used_at">(), "key") }), db::text(listed, "lastUsedIp"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"last_used_ip">(), "key"), db::text(listed, "remark"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"remark">(), "key"), db::text(listed, "createdAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"created_at">(), "key") }), db::text(listed, "updatedAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"updated_at">(), "key") }), db::text(listed, "webhookCount"), listed.aggregate("count", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">(), "webhook") }, true), db::text(listed, "deviceIds"), listed.coalesce({ listed.filter(listed.aggregate("jsonb_agg", { bindingId }, true), listed.unary(ruvia::db_unary_operator::is_not_null, bindingId)), emptyJsonArray(listed) }) }
        );
        listed.select({ listed.alias(item, "item"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"created_at">(), "key") })
            .from(service::open_access::entities::OpenAccessKeyEntity::table_name(), "key")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), listed.binary(listed.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">(), "binding"), ruvia::db_binary_operator::equal, keyId), "binding")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenWebhookEntity::table_name(), db::andAll(listed, listed.binary(listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), "webhook"), ruvia::db_binary_operator::equal, keyId), listed.unary(ruvia::db_unary_operator::is_null, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">(), "webhook"))), "webhook")
            .where(listed.unary(ruvia::db_unary_operator::is_null, listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">(), "key")))
            .group_by({ keyId });
        ruvia::db_query query(c.pool());
        const std::array<ruvia::db_order_term, 1> keyOrder{ { ruvia::db_order_term{ query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"created_at">(), "listed"),
                                                                                    ruvia::db_order_direction::desc,
                                                                                    ruvia::db_nulls_order::default_value } } };
        auto queryBuilder = c.db().template repository<service::open_access::entities::OpenAccessKeyEntity>().query_builder("listed");
        queryBuilder.select({ { "item", db::text(query, query.coalesce({ query.aggregate("jsonb_agg", { query.column("item", "listed") }, false, keyOrder), emptyJsonArray(query) })) } }).with("listed", listed).from_cte("listed");
        co_return firstJson(co_await queryBuilder.get_raw_many());
    }

    template <typename Context>
    ruvia::task<std::string> deviceOptions(Context& c) {
        const auto actor = co_await service::device::DeviceAccessService::instance().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("scoped_device");
        service::device::DeviceAccessService::addScopedDevicesCtes(queryBuilder, query, actor);
        const auto params = query.column(service::device::entities::DeviceEntity::column_name<"protocol_params">());
        const auto key = db::text(query, "remote_control");
        const auto hasKey = query.binary(params, ruvia::db_binary_operator::json_has_key, key);
        const auto type = query.call(
            "jsonb_typeof",
            { db::jsonValue(query, params, "remote_control") }
        );
        const auto remoteText = db::jsonText(
            query,
            params,
            "remote_control"
        );
        const auto booleanRemote = query.binary(remoteText, ruvia::db_binary_operator::equal, query.value("true"));
        const auto stringRemote = query.binary(
            query.call("lower", { query.call("btrim", { remoteText }) }),
            ruvia::db_binary_operator::equal,
            query.value("true")
        );
        const auto safeRemote = query.case_when(
            { { query.unary(ruvia::db_unary_operator::is_null, params),
                db::boolean(query, true) },
              { query.unary(ruvia::db_unary_operator::not_value, hasKey),
                db::boolean(query, true) },
              { query.binary(type, ruvia::db_binary_operator::equal, query.value("boolean")),
                booleanRemote },
              { query.binary(type, ruvia::db_binary_operator::equal, query.value("string")),
                stringRemote } },
            db::boolean(query, false)
        );
        const auto canCommand = query.binary(
            query.binary(query.column("access_rank"), ruvia::db_binary_operator::greater_equal, db::integer(query, 2)),
            ruvia::db_binary_operator::and_value,
            safeRemote
        );
        const auto item = query.call(
            "jsonb_build_object",
            { db::text(query, "id"), db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"id">())), db::text(query, "name"), query.column(service::device::entities::DeviceEntity::column_name<"name">()), db::text(query, "deviceCode"), db::jsonText(query, params, "device_code"), db::text(query, "canCommand"), canCommand }
        );
        const std::array<ruvia::db_order_term, 2> deviceOrder{ { ruvia::db_order_term{ query.column(service::device::entities::DeviceEntity::column_name<"name">()), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value },
                                                                 ruvia::db_order_term{ query.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value } } };

        queryBuilder.select({ { "value0", db::text(query, query.coalesce({ query.aggregate("jsonb_agg", { item }, false, deviceOrder), emptyJsonArray(query) })) } }).from_cte("scoped_device").where(query.binary(query.column("access_rank"), ruvia::db_binary_operator::greater, db::integer(query, 0)));
        co_return firstJson(co_await queryBuilder.get_raw_many());
    }

    template <typename Context>
    ruvia::task<std::string> createKey(Context& c, const CreateAccessKeyBody& payload) {
        const auto name = service::utils::trim(payload.get<"name">().view());
        const auto status = payload.get<"status">()->view();
        const auto scopes = accessStringSet(payload.get<"scopes">());
        const auto devices = accessDeviceIds(payload.get<"deviceIds">());
        co_await ensureDevicesAccessible(c, devices, scopes.contains(kScopeCommand));
        const auto expiresAt = trimAccessText(payload.get<"expiresAt">());
        const auto remark = trimAccessText(payload.get<"remark">());
        co_await ensureKeyNameAvailable(c, name, std::nullopt);

        const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        const auto rawKey = service::open_access::generateAccessKey();
        const auto prefix = rawKey.substr(0, 14);
        const auto keyHash = service::utils::sha256(rawKey);
        const auto scopeJson = stringArrayJson(scopes);
        const auto expiresAtValue = expiresAt.value_or("");
        const auto remarkValue = remark.value_or("");
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query insert(c.pool());
        const std::array<std::string_view, 9> insertColumns{ service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), service::open_access::entities::OpenAccessKeyEntity::column_name<"access_key_prefix">(), service::open_access::entities::OpenAccessKeyEntity::column_name<"access_key_hash">(), service::open_access::entities::OpenAccessKeyEntity::column_name<"status">(), service::open_access::entities::OpenAccessKeyEntity::column_name<"scopes">(), service::open_access::entities::OpenAccessKeyEntity::column_name<"expires_at">(), service::open_access::entities::OpenAccessKeyEntity::column_name<"remark">(), service::open_access::entities::OpenAccessKeyEntity::column_name<"created_by">() };
        auto insertBuilder = transaction.template repository<service::open_access::entities::OpenAccessKeyEntity>().insert_builder();
        ruvia::db_values_builder insertValues(insertColumns);
        insertValues.row({ db::uuid(insert, id), insert.value(name), insert.value(prefix), insert.value(keyHash), insert.value(status), insert.cast(insert.value(scopeJson), ruvia::db_data_type::jsonb), insert.cast(insert.null_if(insert.value(expiresAtValue), insert.value("")), ruvia::db_data_type::timestamp_tz), insert.null_if(insert.value(remarkValue), insert.value("")), db::uuid(insert, c.userId) });
        insertBuilder.insert_from(insertColumns, insertValues.query());
        (void)co_await insertBuilder.execute();
        co_await replaceDevices(transaction, id, devices);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "access_key", "created", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        co_await refreshProjection(c);

        co_return "{\"id\":" + service::utils::jsonQuoted(id) + ",\"name\":" + service::utils::jsonQuoted(name) +
            ",\"status\":" + service::utils::jsonQuoted(status) + ",\"scopes\":" + scopeJson +
            ",\"expiresAt\":" + (expiresAt ? service::utils::jsonQuoted(*expiresAt) : std::string("null")) +
            ",\"deviceIds\":" + stringArrayJson(devices) + ",\"accessKey\":" + service::utils::jsonQuoted(rawKey) +
            ",\"accessKeyPrefix\":" + service::utils::jsonQuoted(prefix) + "}";
    }

    template <typename Context>
    ruvia::task<void> updateKey(Context& c, std::string_view id, const UpdateAccessKeyBody& payload) {
        service::common::requireUuid(19002, id, "调用配置 ID 无效");
        const auto existing = co_await requireKey(c, id);
        const auto name = trimAccessText(payload.get<"name">()).value_or(existing.name);
        const auto status = trimAccessText(payload.get<"status">()).value_or(existing.status);
        const auto scopes = payload.get<"scopes">() ? accessStringSet(*payload.get<"scopes">()) : existing.scopes;
        const auto devices = payload.get<"deviceIds">() ? accessDeviceIds(*payload.get<"deviceIds">()) : existing.deviceIds;
        co_await ensureDevicesAccessible(c, devices, scopes.contains(kScopeCommand));
        co_await ensureKeyNameAvailable(c, name, std::string(id));
        const auto expiresAt = payload.is_present<"expiresAt">() ? trimAccessText(payload.get<"expiresAt">()) : existing.expiresAt;
        const auto remark = payload.is_present<"remark">() ? trimAccessText(payload.get<"remark">()) : existing.remark;
        const auto scopeJson = stringArrayJson(scopes);
        const auto expiresAtValue = expiresAt.value_or("");
        const auto remarkValue = remark.value_or("");
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query update(c.pool());
        auto updateBuilder = transaction.template repository<service::open_access::entities::OpenAccessKeyEntity>().update_builder();
        updateBuilder.set(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), update.value(name)).set(service::open_access::entities::OpenAccessKeyEntity::column_name<"status">(), update.value(status)).set(service::open_access::entities::OpenAccessKeyEntity::column_name<"scopes">(), update.cast(update.value(scopeJson), ruvia::db_data_type::jsonb)).set(service::open_access::entities::OpenAccessKeyEntity::column_name<"expires_at">(), update.cast(update.null_if(update.value(expiresAtValue), update.value("")), ruvia::db_data_type::timestamp_tz)).set(service::open_access::entities::OpenAccessKeyEntity::column_name<"remark">(), update.null_if(update.value(remarkValue), update.value(""))).set(service::open_access::entities::OpenAccessKeyEntity::column_name<"updated_at">(), update.call("now")).where(db::andAll(update, update.binary(update.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(update, id)), update.unary(ruvia::db_unary_operator::is_null, update.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))));
        (void)co_await updateBuilder.execute();
        if (payload.get<"deviceIds">()) {
            co_await replaceDevices(transaction, id, devices);
        }
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "access_key", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        co_await refreshProjection(c);
    }

    template <typename Context>
    ruvia::task<std::string> rotateKey(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "调用配置 ID 无效");
        const auto existing = co_await requireKey(c, id);
        const auto rawKey = service::open_access::generateAccessKey();
        const auto prefix = rawKey.substr(0, 14);
        const auto keyHash = service::utils::sha256(rawKey);
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query update(c.pool());
        auto updateBuilder = transaction.template repository<service::open_access::entities::OpenAccessKeyEntity>().update_builder();
        updateBuilder.set(service::open_access::entities::OpenAccessKeyEntity::column_name<"access_key_hash">(), update.value(keyHash)).set(service::open_access::entities::OpenAccessKeyEntity::column_name<"access_key_prefix">(), update.value(prefix)).set(service::open_access::entities::OpenAccessKeyEntity::column_name<"updated_at">(), update.call("now")).where(db::andAll(update, update.binary(update.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(update, id)), update.unary(ruvia::db_unary_operator::is_null, update.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))));
        (void)co_await updateBuilder.execute();
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "access_key", "rotated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        co_await refreshProjection(c);
        co_return "{\"id\":" + service::utils::jsonQuoted(id) + ",\"name\":" + service::utils::jsonQuoted(existing.name) +
            ",\"accessKey\":" + service::utils::jsonQuoted(rawKey) + ",\"accessKeyPrefix\":" + service::utils::jsonQuoted(prefix) +
            "}";
    }

    template <typename Context>
    ruvia::task<void> removeKey(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "调用配置 ID 无效");
        (void)co_await requireKey(c, id);
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query webhookUpdate(c.pool());
        auto webhookUpdateBuilder = transaction.template repository<service::open_access::entities::OpenWebhookEntity>().update_builder();
        webhookUpdateBuilder.set(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">(), webhookUpdate.call("now")).set(service::open_access::entities::OpenWebhookEntity::column_name<"updated_at">(), webhookUpdate.call("now")).where(db::andAll(webhookUpdate, webhookUpdate.binary(webhookUpdate.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, db::uuid(webhookUpdate, id)), webhookUpdate.unary(ruvia::db_unary_operator::is_null, webhookUpdate.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))));
        (void)co_await webhookUpdateBuilder.execute();
        ruvia::db_query mqttUpdate(c.pool());
        auto mqttUpdateBuilder = transaction.template repository<service::open_access::entities::OpenMqttEntity>().update_builder();
        mqttUpdateBuilder.set(service::open_access::entities::OpenMqttEntity::column_name<"deleted_at">(), mqttUpdate.call("now")).set(service::open_access::entities::OpenMqttEntity::column_name<"updated_at">(), mqttUpdate.call("now")).where(db::andAll(mqttUpdate, mqttUpdate.binary(mqttUpdate.column(service::open_access::entities::OpenMqttEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, db::uuid(mqttUpdate, id)), mqttUpdate.unary(ruvia::db_unary_operator::is_null, mqttUpdate.column(service::open_access::entities::OpenMqttEntity::column_name<"deleted_at">()))));
        (void)co_await mqttUpdateBuilder.execute();
        ruvia::db_query keyUpdate(c.pool());
        auto keyUpdateBuilder = transaction.template repository<service::open_access::entities::OpenAccessKeyEntity>().update_builder();
        keyUpdateBuilder.set(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">(), keyUpdate.call("now")).set(service::open_access::entities::OpenAccessKeyEntity::column_name<"updated_at">(), keyUpdate.call("now")).where(db::andAll(keyUpdate, keyUpdate.binary(keyUpdate.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(keyUpdate, id)), keyUpdate.unary(ruvia::db_unary_operator::is_null, keyUpdate.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))));
        (void)co_await keyUpdateBuilder.execute();
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "access_key", "deleted", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        co_await refreshProjection(c);
    }

    template <typename Context>
    ruvia::task<std::string> listWebhooks(Context& c, const WebhookQueryParams& filters) {
        ruvia::db_query listed(c.pool());
        const auto webhookId = listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">(), "webhook");
        const auto bindingDevice = listed.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">(), "binding");
        const auto item = listed.call(
            "jsonb_build_object",
            { db::text(listed, "id"), db::text(listed, webhookId), db::text(listed, "accessKeyId"), db::text(listed, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), "webhook")), db::text(listed, "accessKeyName"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), "key"), db::text(listed, "name"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"name">(), "webhook"), db::text(listed, "url"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"url">(), "webhook"), db::text(listed, "status"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"status">(), "webhook"), db::text(listed, "timeoutSeconds"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"timeout_seconds">(), "webhook"), db::text(listed, "skipTlsVerify"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"skip_tls_verify">(), "webhook"), db::text(listed, "headers"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"headers">(), "webhook"), db::text(listed, "eventTypes"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"event_types">(), "webhook"), db::text(listed, "deviceIds"), listed.coalesce({ listed.filter(listed.aggregate("jsonb_agg", { bindingDevice }), listed.unary(ruvia::db_unary_operator::is_not_null, bindingDevice)), emptyJsonArray(listed) }), db::text(listed, "hasSecret"), listed.unary(ruvia::db_unary_operator::is_not_null, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"secret">(), "webhook")), db::text(listed, "lastTriggeredAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_triggered_at">(), "webhook") }), db::text(listed, "lastSuccessAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_success_at">(), "webhook") }), db::text(listed, "lastFailureAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_failure_at">(), "webhook") }), db::text(listed, "lastHttpStatus"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_http_status">(), "webhook"), db::text(listed, "lastError"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_error">(), "webhook"), db::text(listed, "createdAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"created_at">(), "webhook") }), db::text(listed, "updatedAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"updated_at">(), "webhook") }) }
        );
        listed.select({ listed.alias(item, "item"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"created_at">(), "webhook") })
            .from(service::open_access::entities::OpenWebhookEntity::table_name(), "webhook")
            .join(ruvia::db_join_type::inner, service::open_access::entities::OpenAccessKeyEntity::table_name(), db::andAll(listed, listed.binary(listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key"), ruvia::db_binary_operator::equal, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), "webhook")), listed.unary(ruvia::db_unary_operator::is_null, listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">(), "key"))), "key")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), listed.binary(listed.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">(), "binding"), ruvia::db_binary_operator::equal, listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key")), "binding")
            .where(listed.unary(ruvia::db_unary_operator::is_null, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">(), "webhook")))
            .group_by({ webhookId, listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key") });
        if (const auto& key = filters.get<"accessKeyId">(); key && !key->view().empty()) {
            service::common::requireUuid(19002, key->view(), "调用配置 ID 无效");
            listed.and_where(listed.binary(listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), "webhook"), ruvia::db_binary_operator::equal, db::uuid(listed, key->view())));
        }
        ruvia::db_query query(c.pool());
        const std::array<ruvia::db_order_term, 1> webhookOrder{ { ruvia::db_order_term{ query.column(service::open_access::entities::OpenWebhookEntity::column_name<"created_at">(), "listed"),
                                                                                        ruvia::db_order_direction::desc,
                                                                                        ruvia::db_nulls_order::default_value } } };
        auto queryBuilder = c.db().template repository<service::open_access::entities::OpenWebhookEntity>().query_builder("listed");
        queryBuilder.select({ { "item", db::text(query, query.coalesce({ query.aggregate("jsonb_agg", { query.column("item", "listed") }, false, webhookOrder), emptyJsonArray(query) })) } }).with("listed", listed).from_cte("listed");
        co_return firstJson(co_await queryBuilder.get_raw_many());
    }

    template <typename Context>
    ruvia::task<std::string> createWebhook(Context& c, const CreateWebhookBody& payload) {
        if (const auto& headers = payload.get<"headers">(); headers && !isWebhookHeaders(*headers)) {
            service::common::fail(19002, "自定义 Header 包含非法或保留字段", 400);
        }
        const auto accessKeyId = payload.get<"accessKeyId">().view();
        (void)co_await requireKey(c, accessKeyId);
        const auto name = service::utils::trim(payload.get<"name">().view());
        const auto url = service::utils::trim(payload.get<"url">().view());
        const auto status = payload.get<"status">()->view();
        const auto timeout = payload.get<"timeoutSeconds">()->value_;
        const auto skipTlsVerify = payload.get<"skipTlsVerify">()->value_;
        const std::string headers(payload.get<"headers">() ? payload.get<"headers">()->view() : "{}");
        const auto events = payload.get<"eventTypes">() ? accessStringSet(*payload.get<"eventTypes">()) : std::set<std::string, std::less<>>{ "device.data.reported" };
        const auto secret = trimAccessText(payload.get<"secret">());
        co_await ensureWebhookNameAvailable(c, accessKeyId, name, std::nullopt);
        const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        const auto eventJson = stringArrayJson(events);
        const auto secretValue = secret.value_or("");
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query insert(c.pool());
        const std::array<std::string_view, 10> insertColumns{ service::open_access::entities::OpenWebhookEntity::column_name<"id">(), service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), service::open_access::entities::OpenWebhookEntity::column_name<"name">(), service::open_access::entities::OpenWebhookEntity::column_name<"url">(), service::open_access::entities::OpenWebhookEntity::column_name<"status">(), service::open_access::entities::OpenWebhookEntity::column_name<"timeout_seconds">(), service::open_access::entities::OpenWebhookEntity::column_name<"skip_tls_verify">(), service::open_access::entities::OpenWebhookEntity::column_name<"headers">(), service::open_access::entities::OpenWebhookEntity::column_name<"event_types">(), service::open_access::entities::OpenWebhookEntity::column_name<"secret">() };
        auto insertBuilder = transaction.template repository<service::open_access::entities::OpenWebhookEntity>().insert_builder();
        ruvia::db_values_builder insertValues(insertColumns);
        insertValues.row({ db::uuid(insert, id), db::uuid(insert, accessKeyId), insert.value(name), insert.value(url), insert.value(status), insert.value(timeout), insert.value(skipTlsVerify), insert.cast(insert.value(headers), ruvia::db_data_type::jsonb), insert.cast(insert.value(eventJson), ruvia::db_data_type::jsonb), insert.null_if(insert.value(secretValue), insert.value("")) });
        insertBuilder.insert_from(insertColumns, insertValues.query());
        (void)co_await insertBuilder.execute();
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "webhook", "created", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        co_return "{\"id\":" + service::utils::jsonQuoted(id) + ",\"accessKeyId\":" + service::utils::jsonQuoted(accessKeyId) +
            ",\"name\":" + service::utils::jsonQuoted(name) + ",\"url\":" + service::utils::jsonQuoted(url) +
            ",\"status\":" + service::utils::jsonQuoted(status) + ",\"timeoutSeconds\":" + std::to_string(timeout) +
            ",\"skipTlsVerify\":" + (skipTlsVerify ? "true" : "false") +
            ",\"headers\":" + headers + ",\"eventTypes\":" + stringArrayJson(events) +
            ",\"hasSecret\":" + (secret && !secret->empty() ? "true" : "false") + "}";
    }

    template <typename Context>
    ruvia::task<void> updateWebhook(Context& c, std::string_view id, const UpdateWebhookBody& payload) {
        if (const auto& headers = payload.get<"headers">(); headers && !isWebhookHeaders(*headers)) {
            service::common::fail(19002, "自定义 Header 包含非法或保留字段", 400);
        }
        service::common::requireUuid(19002, id, "Webhook ID 无效");
        const auto existing = co_await requireWebhook(c, id);
        const auto accessKeyId = trimAccessText(payload.get<"accessKeyId">()).value_or(existing.accessKeyId);
        (void)co_await requireKey(c, accessKeyId);
        const auto name = trimAccessText(payload.get<"name">()).value_or(existing.name);
        const auto url = trimAccessText(payload.get<"url">()).value_or(existing.url);
        const auto status = trimAccessText(payload.get<"status">()).value_or(existing.status);
        const auto timeout = payload.get<"timeoutSeconds">() ? payload.get<"timeoutSeconds">()->value_ : existing.timeout;
        const auto skipTlsVerify = payload.get<"skipTlsVerify">() ? payload.get<"skipTlsVerify">()->value_ : existing.skipTlsVerify;
        const std::string headers(payload.get<"headers">() ? payload.get<"headers">()->view() : existing.headers);
        const auto events = payload.get<"eventTypes">() ? accessStringSet(*payload.get<"eventTypes">()) : existing.events;
        const auto secret = payload.is_present<"secret">() ? trimAccessText(payload.get<"secret">()) : existing.secret;
        co_await ensureWebhookNameAvailable(c, accessKeyId, name, std::string(id));
        const auto eventJson = stringArrayJson(events);
        const auto secretValue = secret.value_or("");
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query update(c.pool());
        auto updateBuilder = transaction.template repository<service::open_access::entities::OpenWebhookEntity>().update_builder();
        updateBuilder.set(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), db::uuid(update, accessKeyId)).set(service::open_access::entities::OpenWebhookEntity::column_name<"name">(), update.value(name)).set(service::open_access::entities::OpenWebhookEntity::column_name<"url">(), update.value(url)).set(service::open_access::entities::OpenWebhookEntity::column_name<"status">(), update.value(status)).set(service::open_access::entities::OpenWebhookEntity::column_name<"timeout_seconds">(), update.value(timeout)).set(service::open_access::entities::OpenWebhookEntity::column_name<"skip_tls_verify">(), update.value(skipTlsVerify)).set(service::open_access::entities::OpenWebhookEntity::column_name<"headers">(), update.cast(update.value(headers), ruvia::db_data_type::jsonb)).set(service::open_access::entities::OpenWebhookEntity::column_name<"event_types">(), update.cast(update.value(eventJson), ruvia::db_data_type::jsonb)).set(service::open_access::entities::OpenWebhookEntity::column_name<"secret">(), update.null_if(update.value(secretValue), update.value(""))).set(service::open_access::entities::OpenWebhookEntity::column_name<"updated_at">(), update.call("now")).where(db::andAll(update, update.binary(update.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(update, id)), update.unary(ruvia::db_unary_operator::is_null, update.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))));
        (void)co_await updateBuilder.execute();
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "webhook", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> removeWebhook(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "Webhook ID 无效");
        (void)co_await requireWebhook(c, id);
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query update(c.pool());
        auto updateBuilder = transaction.template repository<service::open_access::entities::OpenWebhookEntity>().update_builder();
        updateBuilder.set(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">(), update.call("now")).set(service::open_access::entities::OpenWebhookEntity::column_name<"updated_at">(), update.call("now")).where(db::andAll(update, update.binary(update.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(update, id)), update.unary(ruvia::db_unary_operator::is_null, update.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))));
        (void)co_await updateBuilder.execute();
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "webhook", "deleted", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<MqttItem>> listMqtt(Context& c, const WebhookQueryParams& filters) {
        ruvia::db_query q(c.pool());
        auto qBuilder = c.db().template repository<service::open_access::entities::OpenMqttEntity>().query_builder("m");
        qBuilder.select({ { "id", db::text(q, q.column(service::open_access::entities::OpenMqttEntity::column_name<"id">(), "m")) }, { "access_key_id", db::text(q, q.column(service::open_access::entities::OpenMqttEntity::column_name<"access_key_id">(), "m")) }, { "name", q.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), "k") }, { "value3", q.column(service::open_access::entities::OpenMqttEntity::column_name<"name">(), "m") }, { "broker_url", q.column(service::open_access::entities::OpenMqttEntity::column_name<"broker_url">(), "m") }, { "status", q.column(service::open_access::entities::OpenMqttEntity::column_name<"status">(), "m") }, { "timeout_seconds", q.column(service::open_access::entities::OpenMqttEntity::column_name<"timeout_seconds">(), "m") }, { "event_types", db::text(q, q.column(service::open_access::entities::OpenMqttEntity::column_name<"event_types">(), "m")) }, { "username", q.column(service::open_access::entities::OpenMqttEntity::column_name<"username">(), "m") }, { "password", q.case_when({ { q.unary(ruvia::db_unary_operator::is_not_null, q.column(service::open_access::entities::OpenMqttEntity::column_name<"password">(), "m")), q.value("1") } }, q.value("0")) }, { "last_triggered_at", q.call("iot_utc_timestamp", { q.column(service::open_access::entities::OpenMqttEntity::column_name<"last_triggered_at">(), "m") }) }, { "last_success_at", q.call("iot_utc_timestamp", { q.column(service::open_access::entities::OpenMqttEntity::column_name<"last_success_at">(), "m") }) }, { "last_failure_at", q.call("iot_utc_timestamp", { q.column(service::open_access::entities::OpenMqttEntity::column_name<"last_failure_at">(), "m") }) }, { "last_error", q.column(service::open_access::entities::OpenMqttEntity::column_name<"last_error">(), "m") }, { "created_at", q.call("iot_utc_timestamp", { q.column(service::open_access::entities::OpenMqttEntity::column_name<"created_at">(), "m") }) }, { "updated_at", q.call("iot_utc_timestamp", { q.column(service::open_access::entities::OpenMqttEntity::column_name<"updated_at">(), "m") }) } }).template join<service::open_access::entities::OpenAccessKeyEntity>(ruvia::db_join_type::inner, "k", q.binary(q.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "k"), ruvia::db_binary_operator::equal, q.column(service::open_access::entities::OpenMqttEntity::column_name<"access_key_id">(), "m"))).where(q.unary(ruvia::db_unary_operator::is_null, q.column(service::open_access::entities::OpenMqttEntity::column_name<"deleted_at">(), "m"))).and_where(q.unary(ruvia::db_unary_operator::is_null, q.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">(), "k"))).order_by(q.column(service::open_access::entities::OpenMqttEntity::column_name<"created_at">(), "m"), ruvia::db_order_direction::desc);
        if (const auto& key = filters.get<"accessKeyId">(); key) {
            qBuilder.and_where(q.binary(q.column(service::open_access::entities::OpenMqttEntity::column_name<"access_key_id">(), "m"), ruvia::db_binary_operator::equal, db::uuid(q, key->view())));
        }
        const auto rows = co_await qBuilder.get_raw_many();
        ruvia::boxed_array<MqttItem> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            const auto field = [&row](std::size_t i) {
                return row[i].value().value_or(std::string_view{});
            };
            item.set<"id">(field(0)).template set<"accessKeyId">(field(1)).template set<"accessKeyName">(field(2)).template set<"name">(field(3)).template set<"brokerUrl">(field(4)).template set<"status">(field(5)).template set<"timeoutSeconds">(service::utils::parseInt64(row[6].value()).value_or(5)).template set<"hasPassword">(field(9) == "1").template set<"createdAt">(field(14)).template set<"updatedAt">(field(15));
            ruvia::array<ruvia::string> events(ruvia::model_options{ .resource_ = c.arena() });
            for (const auto& event : parseStringArray(field(7))) {
                events.push_back(ruvia::string(event));
            }
            item.set<"eventTypes">(std::move(events));
            const auto nullable = [&]<ruvia::fixed_string Name>(std::size_t index) {
                if (row[index].value()) {
                    item.template set<Name>(*row[index].value());
                } else {
                    item.template set<Name>(nullptr);
                }
            };
            nullable.template operator()<"username">(8);
            nullable.template operator()<"lastTriggeredAt">(10);
            nullable.template operator()<"lastSuccessAt">(11);
            nullable.template operator()<"lastFailureAt">(12);
            nullable.template operator()<"lastError">(13);
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> saveMqtt(Context& c, std::optional<std::string> id, const MqttSaveBody& payload) {
        const auto keyId = payload.get<"accessKeyId">().view();
        const auto key = co_await requireKey(c, keyId);
        co_await ensureDevicesAccessible(c, key.deviceIds, false);
        const auto events = accessStringSet(payload.get<"eventTypes">());
        for (const auto& event : events) {
            const auto scope = event.starts_with("device.alert.") ? kScopeAlert : event.starts_with("device.command.") ? kScopeCommand
                                                                                                                       : kScopeRealtime;
            if (!key.scopes.contains(scope)) {
                service::common::fail(19011, "事件超出调用配置的开放权限", 403);
            }
        }
        std::string password;
        if (id) {
            ruvia::db_query current(c.pool());
            auto currentBuilder = c.db().template repository<service::open_access::entities::OpenMqttEntity>().query_builder();
            currentBuilder.select({ { "password", current.column(service::open_access::entities::OpenMqttEntity::column_name<"password">()) } }).where(db::andAll(current, current.binary(current.column(service::open_access::entities::OpenMqttEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(current, *id)), current.unary(ruvia::db_unary_operator::is_null, current.column(service::open_access::entities::OpenMqttEntity::column_name<"deleted_at">()))));
            const auto rows = co_await currentBuilder.get_raw_many();
            if (rows.empty()) {
                service::common::fail(19001, "MQTT 配置不存在", 404);
            }
            password = rows.front()[0].value().value_or(std::string_view{});
        }
        if (payload.template is_present<"password">()) {
            password = payload.get<"password">() ? std::string(payload.get<"password">()->view()) : "";
        }
        const auto username = payload.get<"username">() ? std::string(payload.get<"username">()->view()) : "";
        if (!service::utils::mqtt::validText(username) || password.size() > 65535 || (!password.empty() && username.empty())) {
            service::common::fail(19002, "MQTT 凭据无效；设置密码时必须提供用户名", 400);
        }
        const auto name = service::utils::trim(payload.get<"name">().view());
        ruvia::db_query duplicate(c.pool());
        auto duplicateBuilder = c.db().template repository<service::open_access::entities::OpenMqttEntity>().query_builder();
        duplicateBuilder.select({ { "value0", duplicate.value(1) } }).where(db::andAll(duplicate, duplicate.binary(duplicate.column(service::open_access::entities::OpenMqttEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, db::uuid(duplicate, keyId)), duplicate.binary(duplicate.column(service::open_access::entities::OpenMqttEntity::column_name<"name">()), ruvia::db_binary_operator::equal, duplicate.value(name)), duplicate.unary(ruvia::db_unary_operator::is_null, duplicate.column(service::open_access::entities::OpenMqttEntity::column_name<"deleted_at">()))));
        if (id) {
            duplicateBuilder.and_where(duplicate.binary(duplicate.column(service::open_access::entities::OpenMqttEntity::column_name<"id">()), ruvia::db_binary_operator::not_equal, db::uuid(duplicate, *id)));
        }
        const auto duplicateRows = co_await duplicateBuilder.get_raw_many();
        if (!duplicateRows.empty()) {
            service::common::fail(19003, "同一调用配置下的 MQTT 名称已存在", 409);
        }
        const auto targetId = id.value_or(service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        const auto eventJson = stringArrayJson(events);
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query q(c.pool());
        const auto userValue = q.null_if(q.value(username), q.value(""));
        const auto passwordValue = q.null_if(q.value(password), q.value(""));
        const auto eventValue = q.cast(q.value(eventJson), ruvia::db_data_type::jsonb);
        if (id) {
            auto update = transaction.template repository<service::open_access::entities::OpenMqttEntity>().update_builder();
            update.set(service::open_access::entities::OpenMqttEntity::column_name<"access_key_id">(), db::uuid(q, keyId)).set(service::open_access::entities::OpenMqttEntity::column_name<"name">(), q.value(name)).set(service::open_access::entities::OpenMqttEntity::column_name<"broker_url">(), q.value(payload.get<"brokerUrl">().view())).set(service::open_access::entities::OpenMqttEntity::column_name<"username">(), userValue).set(service::open_access::entities::OpenMqttEntity::column_name<"password">(), passwordValue).set(service::open_access::entities::OpenMqttEntity::column_name<"status">(), q.value(payload.get<"status">()->view())).set(service::open_access::entities::OpenMqttEntity::column_name<"timeout_seconds">(), q.value(payload.get<"timeoutSeconds">()->value_)).set(service::open_access::entities::OpenMqttEntity::column_name<"event_types">(), eventValue).set(service::open_access::entities::OpenMqttEntity::column_name<"updated_at">(), q.call("now")).where(q.binary(q.column(service::open_access::entities::OpenMqttEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(q, targetId)));
            (void)co_await update.execute();
        } else {
            const std::array<std::string_view, 9> qColumns{ service::open_access::entities::OpenMqttEntity::column_name<"id">(), service::open_access::entities::OpenMqttEntity::column_name<"access_key_id">(), service::open_access::entities::OpenMqttEntity::column_name<"name">(), service::open_access::entities::OpenMqttEntity::column_name<"broker_url">(), service::open_access::entities::OpenMqttEntity::column_name<"username">(), service::open_access::entities::OpenMqttEntity::column_name<"password">(), service::open_access::entities::OpenMqttEntity::column_name<"status">(), service::open_access::entities::OpenMqttEntity::column_name<"timeout_seconds">(), service::open_access::entities::OpenMqttEntity::column_name<"event_types">() };
            auto qBuilder = transaction.template repository<service::open_access::entities::OpenMqttEntity>().insert_builder();
            ruvia::db_values_builder qValues(qColumns);
            qValues.row({ db::uuid(q, targetId), db::uuid(q, keyId), q.value(name), q.value(payload.get<"brokerUrl">().view()), userValue, passwordValue, q.value(payload.get<"status">()->view()), q.value(payload.get<"timeoutSeconds">()->value_), eventValue });
            qBuilder.insert_from(qColumns, qValues.query());
            (void)co_await qBuilder.execute();
        }
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "mqtt", id ? "updated" : "created", targetId, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> removeMqtt(Context& c, std::string_view id) {
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query q(c.pool());
        auto qBuilder = transaction.template repository<service::open_access::entities::OpenMqttEntity>().update_builder();
        qBuilder.set(service::open_access::entities::OpenMqttEntity::column_name<"deleted_at">(), q.call("now")).set(service::open_access::entities::OpenMqttEntity::column_name<"updated_at">(), q.call("now")).where(db::andAll(q, q.binary(q.column(service::open_access::entities::OpenMqttEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(q, id)), q.unary(ruvia::db_unary_operator::is_null, q.column(service::open_access::entities::OpenMqttEntity::column_name<"deleted_at">())))).returning({ { "id", q.column(service::open_access::entities::OpenMqttEntity::column_name<"id">()) } });
        const auto existingRows = co_await qBuilder.get_raw_many();
        if (existingRows.empty()) {
            service::common::fail(19001, "MQTT 配置不存在", 404);
        }
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "mqtt", "deleted", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<std::string> listLogs(Context& c, const AccessLogParams& filters) {
        const auto pageNumber = filters.get<"page">()->value_;
        const auto pageSize = filters.get<"pageSize">()->value_;
        if (pageNumber - 1 > std::numeric_limits<std::int64_t>::max() / pageSize) {
            service::common::fail(19002, "分页超出允许范围", 400);
        }
        const service::common::Page pagination{ pageNumber, pageSize, (pageNumber - 1) * pageSize };
        const auto accessKeyId = trimAccessText(filters.get<"accessKeyId">());
        const auto webhookId = trimAccessText(filters.get<"webhookId">());
        const auto mqttId = trimAccessText(filters.get<"mqttId">());
        const auto deviceId = trimAccessText(filters.get<"deviceId">());
        const auto direction = trimAccessText(filters.get<"direction">());
        const auto action = trimAccessText(filters.get<"action">());
        const auto status = trimAccessText(filters.get<"status">());
        const auto eventType = trimAccessText(filters.get<"eventType">());
        const auto applyFilters = [&](ruvia::db_query& query, std::string_view alias) {
            auto predicate = db::boolean(query, true);
            const auto addUuid = [&](std::string_view value, std::string_view column) {
                if (!value.empty()) {
                    predicate = query.binary(
                        predicate,
                        ruvia::db_binary_operator::and_value,
                        query.binary(query.column(column, alias), ruvia::db_binary_operator::equal, db::uuid(query, value))
                    );
                }
            };
            const auto addText = [&](const std::optional<std::string_view>& value,
                                     std::string_view column) {
                if (value && !value->empty()) {
                    predicate = query.binary(
                        predicate,
                        ruvia::db_binary_operator::and_value,
                        query.binary(query.column(column, alias), ruvia::db_binary_operator::equal, query.value(*value))
                    );
                }
            };
            addUuid(accessKeyId.value_or(""), "access_key_id");
            addUuid(webhookId.value_or(""), "webhook_id");
            addUuid(mqttId.value_or(""), "mqtt_id");
            addUuid(deviceId.value_or(""), "device_id");
            addText(direction, "direction");
            addText(action, "action");
            addText(status, "status");
            addText(eventType, "event_type");
            query.where(predicate);
        };
        ruvia::db_query counted(c.pool());
        counted.select(counted.aggregate("count", { counted.star() }))
            .from(service::open_access::entities::OpenAccessLogEntity::table_name(), "log");
        applyFilters(counted, "log");
        ruvia::db_query pageRows(c.pool());
        pageRows.select(pageRows.star("log")).from(service::open_access::entities::OpenAccessLogEntity::table_name(), "log");
        applyFilters(pageRows, "log");
        pageRows.order_by(pageRows.column(service::open_access::entities::OpenAccessLogEntity::column_name<"created_at">(), "log"), ruvia::db_order_direction::desc)
            .add_order_by(pageRows.column(service::open_access::entities::OpenAccessLogEntity::column_name<"id">(), "log"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pagination.pageSize))
            .offset(static_cast<std::uint64_t>(pagination.offset));
        ruvia::db_query page(c.pool());
        page.select({ page.star("page_rows"), page.alias(page.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), "key"), "access_key_name"), page.alias(page.column(service::open_access::entities::OpenWebhookEntity::column_name<"name">(), "webhook"), "webhook_name"), page.alias(page.column(service::open_access::entities::OpenMqttEntity::column_name<"name">(), "mqtt"), "mqtt_name") })
            .from(pageRows, "page_rows")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenAccessKeyEntity::table_name(), page.binary(page.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key"), ruvia::db_binary_operator::equal, page.column(service::open_access::entities::OpenAccessLogEntity::column_name<"access_key_id">(), "page_rows")), "key")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenWebhookEntity::table_name(), page.binary(page.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">(), "webhook"), ruvia::db_binary_operator::equal, page.column(service::open_access::entities::OpenAccessLogEntity::column_name<"webhook_id">(), "page_rows")), "webhook")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenMqttEntity::table_name(), page.binary(page.column(service::open_access::entities::OpenMqttEntity::column_name<"id">(), "mqtt"), ruvia::db_binary_operator::equal, page.column(service::open_access::entities::OpenAccessLogEntity::column_name<"mqtt_id">(), "page_rows")), "mqtt");
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::open_access::entities::OpenAccessLogEntity>().query_builder("page");
        queryBuilder.with("counted", counted).with("page_rows", pageRows).with("page", page);
        const auto total = query.subquery(counted);
        const auto item = query.call(
            "jsonb_build_object",
            { db::text(query, "id"), db::text(query, query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"id">(), "page")), db::text(query, "accessKeyId"), db::text(query, query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"access_key_id">(), "page")), db::text(query, "accessKeyName"), query.column("access_key_name", "page"), db::text(query, "webhookId"), db::text(query, query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"webhook_id">(), "page")), db::text(query, "webhookName"), query.column("webhook_name", "page"), db::text(query, "mqttId"), db::text(query, query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"mqtt_id">(), "page")), db::text(query, "mqttName"), query.column("mqtt_name", "page"), db::text(query, "direction"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"direction">(), "page"), db::text(query, "action"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"action">(), "page"), db::text(query, "eventType"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"event_type">(), "page"), db::text(query, "status"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"status">(), "page"), db::text(query, "httpMethod"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"http_method">(), "page"), db::text(query, "target"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"target">(), "page"), db::text(query, "requestIp"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"request_ip">(), "page"), db::text(query, "httpStatus"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"http_status">(), "page"), db::text(query, "deviceId"), db::text(query, query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"device_id">(), "page")), db::text(query, "deviceCode"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"device_code">(), "page"), db::text(query, "message"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"message">(), "page"), db::text(query, "requestPayload"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"request_payload">(), "page"), db::text(query, "responsePayload"), query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"response_payload">(), "page"), db::text(query, "createdAt"), query.call("iot_utc_timestamp", { query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"created_at">(), "page") }) }
        );
        const std::array<ruvia::db_order_term, 2> logOrder{ { ruvia::db_order_term{ query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"created_at">(), "page"),
                                                                                    ruvia::db_order_direction::desc,
                                                                                    ruvia::db_nulls_order::default_value },
                                                              ruvia::db_order_term{ query.column(service::open_access::entities::OpenAccessLogEntity::column_name<"id">(), "page"), ruvia::db_order_direction::desc, ruvia::db_nulls_order::default_value } } };
        const auto list = query.coalesce(
            { query.aggregate("jsonb_agg", { item }, false, logOrder),
              emptyJsonArray(query) }
        );
        const auto totalPages = query.cast(
            query.call("ceil", { query.binary(query.cast(total, ruvia::db_data_type::numeric), ruvia::db_binary_operator::divide, query.cast(query.value(pagination.pageSize), ruvia::db_data_type::numeric)) }),
            ruvia::db_data_type::big_int
        );
        queryBuilder.select({ { "value0", query.cast(query.call("jsonb_build_object", { db::text(query, "list"), list, db::text(query, "total"), total, db::text(query, "page"), query.cast(query.value(pagination.page), ruvia::db_data_type::big_int), db::text(query, "pageSize"), query.cast(query.value(pagination.pageSize), ruvia::db_data_type::big_int), db::text(query, "totalPages"), totalPages }), ruvia::db_data_type::text) } }).from_cte("page");
        co_return firstJson(co_await queryBuilder.get_raw_many());
    }

    ruvia::task<AccessSession> authenticate(ruvia::context& c, std::string_view requiredScope) {
        const auto raw = requestAccessKey(c.req());
        if (raw.empty()) {
            service::common::fail(19010, "缺少 X-Access-Key", 401);
        }
        const auto projected = co_await loadSession(c, raw);
        if (!projected) {
            service::common::fail(19010, "AccessKey 无效", 401);
        }
        if (projected->status != "enabled") {
            service::common::fail(19011, "AccessKey 已被禁用", 403);
        }
        if (service::message::access::session::expired(*projected, service::message::utcNowMilliseconds())) {
            service::common::fail(19010, "AccessKey 已过期", 401);
        }
        AccessSession session;
        session.id = projected->id;
        session.name = projected->name;
        session.scopes = projected->scopes;
        session.expiresAtMs = projected->expiresAtMs;
        if (!requiredScope.empty() && !session.allows(requiredScope)) {
            service::common::fail(19011, "AccessKey 未开通所需权限", 403);
        }
        session.deviceIds = projected->deviceIds;
        if (session.deviceIds.empty()) {
            service::common::fail(19011, "AccessKey 未配置可访问设备", 403);
        }
        co_return session;
    }

    ruvia::task<void> audit(ruvia::context& c, std::string_view action, const AccessSession& session, std::string_view deviceId = {}, std::string_view requestPayload = "{}", std::string_view responsePayload = "{}") {
        try {
            service::message::access::AuditRequest payload(ruvia::model_options{ .resource_ = c.arena() });
            payload.set<"action">(action)
                .set<"accessKeyId">(session.id)
                .set<"method">(c.req().method())
                .set<"target">(c.req().path())
                .set<"requestIp">(service::common::clientIp(c))
                .set<"httpStatus">(200)
                .set<"deviceId">(deviceId);
            payload.set<"requestPayload">(ruvia::json_value::parse(requestPayload, { .resource_ = c.arena() }));
            payload.set<"responsePayload">(ruvia::json_value::parse(responsePayload, { .resource_ = c.arena() }));
            (void)co_await service::utils::callRpc(c, "access", "audit", service::message::rpc::encode(payload, c.arena()));
        } catch (...) {
            // Open-access audit is best effort.
        }
    }

    ruvia::task<std::string> publicDevices(ruvia::context& c, const AccessSession& session) {
        const auto pagination = service::common::page(c.req());
        const auto keyword = service::utils::trim(c.req().query("keyword").value_or(""));
        ruvia::db_query visible(c.pool());
        const auto deviceId = visible.column(service::device::entities::DeviceEntity::column_name<"id">(), "device");
        const auto code = db::jsonText(
            visible,
            visible.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "device"),
            "device_code"
        );
        const auto pattern = visible.binary(
            visible.binary(db::text(visible, "%"), ruvia::db_binary_operator::concat, db::text(visible, keyword)),
            ruvia::db_binary_operator::concat,
            db::text(visible, "%")
        );
        const auto keywordFilter = visible.binary(
            visible.binary(db::text(visible, keyword), ruvia::db_binary_operator::equal, db::text(visible, "")),
            ruvia::db_binary_operator::or_value,
            visible.binary(
                visible.binary(
                    visible.binary(db::text(visible, deviceId), ruvia::db_binary_operator::i_like, pattern),
                    ruvia::db_binary_operator::or_value,
                    visible.binary(visible.column(service::device::entities::DeviceEntity::column_name<"name">(), "device"), ruvia::db_binary_operator::i_like, pattern)
                ),
                ruvia::db_binary_operator::or_value,
                visible.binary(visible.coalesce({ code, visible.value("") }), ruvia::db_binary_operator::i_like, pattern)
            )
        );
        visible.select({ deviceId, visible.column(service::device::entities::DeviceEntity::column_name<"name">(), "device"), visible.alias(code, "code") })
            .from(service::device::entities::DeviceEntity::table_name(), "device")
            .join(ruvia::db_join_type::inner, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), visible.binary(visible.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">(), "binding"), ruvia::db_binary_operator::equal, deviceId), "binding")
            .where(db::andAll(visible, visible.binary(visible.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">(), "binding"), ruvia::db_binary_operator::equal, db::uuid(visible, session.id)), visible.unary(ruvia::db_unary_operator::is_null, visible.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "device")), keywordFilter));
        ruvia::db_query counted(c.pool());
        counted.select(counted.aggregate("count", { counted.star() })).from(visible, "visible");
        ruvia::db_query page(c.pool());
        page.select(page.star("visible"))
            .from(visible, "visible")
            .order_by(page.column(service::device::entities::DeviceEntity::column_name<"name">(), "visible"))
            .add_order_by(page.column(service::device::entities::DeviceEntity::column_name<"id">(), "visible"))
            .limit(static_cast<std::uint64_t>(pagination.pageSize))
            .offset(static_cast<std::uint64_t>(pagination.offset));
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("page");
        queryBuilder.with("visible", visible).with("counted", counted).with("page", page);
        const auto item = query.call(
            "jsonb_build_object",
            { db::text(query, "id"), db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"id">(), "page")), db::text(query, "code"), query.column("code", "page"), db::text(query, "name"), query.column(service::device::entities::DeviceEntity::column_name<"name">(), "page") }
        );
        const auto total = query.subquery(counted);
        const std::array<ruvia::db_order_term, 2> deviceListOrder{ { ruvia::db_order_term{ query.column(service::device::entities::DeviceEntity::column_name<"name">(), "page"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value },
                                                                     ruvia::db_order_term{ query.column(service::device::entities::DeviceEntity::column_name<"id">(), "page"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value } } };
        const auto list = query.coalesce(
            { query.aggregate("jsonb_agg", { item }, false, deviceListOrder),
              emptyJsonArray(query) }
        );
        const auto totalPages = query.cast(
            query.call("ceil", { query.binary(query.cast(total, ruvia::db_data_type::numeric), ruvia::db_binary_operator::divide, query.cast(query.value(pagination.pageSize), ruvia::db_data_type::numeric)) }),
            ruvia::db_data_type::big_int
        );
        queryBuilder.select({ { "value0", query.cast(query.call("jsonb_build_object", { db::text(query, "list"), list, db::text(query, "total"), total, db::text(query, "page"), query.cast(query.value(pagination.page), ruvia::db_data_type::big_int), db::text(query, "pageSize"), query.cast(query.value(pagination.pageSize), ruvia::db_data_type::big_int), db::text(query, "totalPages"), totalPages }), ruvia::db_data_type::text) } }).from_cte("page");
        const auto rows = co_await queryBuilder.get_raw_many();
        co_return firstJson(rows);
    }

    ruvia::task<std::string> publicRealtime(ruvia::context& c, const AccessSession& session, std::string_view deviceId) {
        service::common::requireUuid(19002, deviceId, "设备 ID 无效");
        if (!session.allowsDevice(deviceId)) {
            service::common::fail(19011, "AccessKey 无权访问该设备", 403);
        }
        co_return co_await realtimeData(c, deviceId);
    }

    ruvia::task<std::string> publicHistory(ruvia::context& c, const AccessSession& session, std::string_view deviceId) {
        co_await requireSessionDevice(c, session, deviceId);
        const auto start = service::utils::trim(c.req().query("startTime").value_or(""));
        const auto end = service::utils::trim(c.req().query("endTime").value_or(""));
        if (start.empty() || end.empty()) {
            service::common::fail(19002, "startTime 和 endTime 不能为空", 400);
        }
        const auto pagination = service::common::page(c.req());
        try {
            const auto query = publicHistoryQuery(c.db(), c.pool(), deviceId, start, end, pagination.pageSize, pagination.offset, pagination.page);
            const auto rows = co_await query.get_raw_many();
            co_return firstJson(rows);
        } catch (const std::exception&) {
            service::common::fail(19002, "时间范围格式错误", 400);
        }
    }

    ruvia::task<service::device::DeviceCommandCreateDto> publicCommand(ruvia::context& c, const AccessSession& session, const service::device::DeviceCommandBody& body) {
        const auto id = body.get<"deviceId">() ? std::string(body.get<"deviceId">()->view()) : std::string{};
        service::common::requireUuid(19002, id, "设备 ID 无效");
        if (!session.allowsDevice(id)) {
            service::common::fail(19011, "AccessKey 无权控制该设备", 403);
        }
        auto result = co_await service::command::CommandService::instance().createExternal(c, id, body, session.id);
        co_await audit(c, "command", session, id);
        co_return result;
    }

    ruvia::task<std::string> publicAlerts(ruvia::context& c, const AccessSession& session) {
        const auto pagination = service::common::page(c.req());
        std::optional<std::string> deviceFilter;
        if (const auto device = c.req().query("deviceId"); device && !device->empty()) {
            service::common::requireUuid(19002, *device, "设备 ID 无效");
            if (!session.allowsDevice(*device)) {
                service::common::fail(19011, "AccessKey 无权访问该设备", 403);
            }
            deviceFilter = std::string(*device);
        }
        const auto status = c.req().query("status");
        const auto severity = c.req().query("severity");
        const auto applyFilters = [&](ruvia::db_query& query, std::string_view recordAlias, std::string_view bindingAlias) {
            auto predicate = query.binary(query.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">(), bindingAlias), ruvia::db_binary_operator::equal, db::uuid(query, session.id));
            if (deviceFilter) {
                predicate = query.binary(
                    predicate,
                    ruvia::db_binary_operator::and_value,
                    query.binary(query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"device_id">(), recordAlias), ruvia::db_binary_operator::equal, db::uuid(query, *deviceFilter))
                );
            }
            if (status && !status->empty()) {
                predicate = query.binary(
                    predicate,
                    ruvia::db_binary_operator::and_value,
                    query.binary(query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">(), recordAlias), ruvia::db_binary_operator::equal, query.value(*status))
                );
            }
            if (severity && !severity->empty()) {
                predicate = query.binary(
                    predicate,
                    ruvia::db_binary_operator::and_value,
                    query.binary(query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"severity">(), recordAlias), ruvia::db_binary_operator::equal, query.value(*severity))
                );
            }
            query.where(predicate);
        };
        ruvia::db_query counted(c.pool());
        counted.select(counted.aggregate("count", { counted.star() }))
            .from(service::alert::entities::OpenAlertRecordEntity::table_name(), "record")
            .join(ruvia::db_join_type::inner, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), counted.binary(counted.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">(), "binding"), ruvia::db_binary_operator::equal, counted.column(service::alert::entities::OpenAlertRecordEntity::column_name<"device_id">(), "record")), "binding");
        applyFilters(counted, "record", "binding");
        ruvia::db_query pageRows(c.pool());
        pageRows.select(pageRows.star("record"))
            .from(service::alert::entities::OpenAlertRecordEntity::table_name(), "record")
            .join(ruvia::db_join_type::inner, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), pageRows.binary(pageRows.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">(), "binding"), ruvia::db_binary_operator::equal, pageRows.column(service::alert::entities::OpenAlertRecordEntity::column_name<"device_id">(), "record")), "binding");
        applyFilters(pageRows, "record", "binding");
        pageRows.order_by(pageRows.column(service::alert::entities::OpenAlertRecordEntity::column_name<"triggered_at">(), "record"), ruvia::db_order_direction::desc)
            .add_order_by(pageRows.column(service::alert::entities::OpenAlertRecordEntity::column_name<"id">(), "record"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pagination.pageSize))
            .offset(static_cast<std::uint64_t>(pagination.offset));
        ruvia::db_query page(c.pool());
        page.select({ page.star("page_rows"), page.alias(page.column(service::device::entities::DeviceEntity::column_name<"name">(), "device"), "device_name"), page.alias(db::jsonText(page, page.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "device"), "device_code"), "device_code") })
            .from(pageRows, "page_rows")
            .join(ruvia::db_join_type::inner, service::device::entities::DeviceEntity::table_name(), page.binary(page.column(service::device::entities::DeviceEntity::column_name<"id">(), "device"), ruvia::db_binary_operator::equal, page.column(service::alert::entities::OpenAlertRecordEntity::column_name<"device_id">(), "page_rows")), "device");
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::alert::entities::OpenAlertRecordEntity>().query_builder("page");
        queryBuilder.with("counted", counted).with("page_rows", pageRows).with("page", page);
        const auto item = query.call(
            "jsonb_build_object",
            { db::text(query, "id"), db::text(query, query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"id">(), "page")), db::text(query, "device"), query.call("jsonb_build_object", { db::text(query, "id"), db::text(query, query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"device_id">(), "page")), db::text(query, "code"), query.column("device_code", "page"), db::text(query, "name"), query.column("device_name", "page") }), db::text(query, "ruleId"), db::text(query, query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"rule_id">(), "page")), db::text(query, "severity"), query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"severity">(), "page"), db::text(query, "status"), query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">(), "page"), db::text(query, "message"), query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"message">(), "page"), db::text(query, "time"), query.call("iot_utc_timestamp", { query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"triggered_at">(), "page") }) }
        );
        const auto total = query.subquery(counted);
        const std::array<ruvia::db_order_term, 2> alertOrder{ { ruvia::db_order_term{ query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"triggered_at">(), "page"),
                                                                                      ruvia::db_order_direction::desc,
                                                                                      ruvia::db_nulls_order::default_value },
                                                                ruvia::db_order_term{ query.column(service::alert::entities::OpenAlertRecordEntity::column_name<"id">(), "page"), ruvia::db_order_direction::desc, ruvia::db_nulls_order::default_value } } };
        const auto list = query.coalesce(
            { query.aggregate("jsonb_agg", { item }, false, alertOrder),
              emptyJsonArray(query) }
        );
        const auto totalPages = query.cast(
            query.call("ceil", { query.binary(query.cast(total, ruvia::db_data_type::numeric), ruvia::db_binary_operator::divide, query.cast(query.value(pagination.pageSize), ruvia::db_data_type::numeric)) }),
            ruvia::db_data_type::big_int
        );
        queryBuilder.select({ { "value0", query.cast(query.call("jsonb_build_object", { db::text(query, "list"), list, db::text(query, "total"), total, db::text(query, "page"), query.cast(query.value(pagination.page), ruvia::db_data_type::big_int), db::text(query, "pageSize"), query.cast(query.value(pagination.pageSize), ruvia::db_data_type::big_int), db::text(query, "totalPages"), totalPages }), ruvia::db_data_type::text) } }).from_cte("page");
        const auto rows = co_await queryBuilder.get_raw_many();
        co_return firstJson(rows);
    }

    template <typename Context>
    ruvia::task<std::string> realtimeData(Context& c, std::string_view deviceId) {
        const auto device = co_await loadRealtimeDevice(c, deviceId);
        if (!device) {
            service::common::fail(19001, "设备不存在", 404);
        }
        const auto latestKey = service::message::telemetry::latestKey(device->id);
        const std::vector<std::string> latestArguments{ "HGETALL", latestKey };
        const auto latest = co_await c.redis().command(std::vector<std::string_view>(latestArguments.begin(), latestArguments.end()));
        std::map<std::string_view, std::string_view, std::less<>> latestFields;
        if (latest.kind() == ruvia::redis_value::kind_type::array) {
            const auto values = latest.array();
            for (std::size_t index = 0; index + 1 < values.size(); index += 2) {
                if (values[index].kind() == ruvia::redis_value::kind_type::string &&
                    values[index + 1].kind() == ruvia::redis_value::kind_type::string) {
                    latestFields.insert_or_assign(values[index].string(), values[index + 1].string());
                }
            }
        } else {
            service::utils::redis::throwValue("read realtime device values", latest);
        }
        std::string body = "{\"device\":{\"id\":" + service::utils::jsonQuoted(deviceId) +
            ",\"code\":" + service::utils::jsonQuoted(device->code) +
            ",\"name\":" + service::utils::jsonQuoted(device->name) + "},\"points\":[";
        for (std::size_t index = 0; index < device->points.size(); ++index) {
            if (index != 0) {
                body.push_back(',');
            }
            std::string value = "null";
            std::string time = "null";
            if (const auto data = latestFields.find(device->points[index].id);
                data != latestFields.end()) {
                {
                    const auto json = ruvia::json_value::parse(data->second);
                    const auto dataType = json.template get<ruvia::string>("dataType");
                    if (const auto current = json.template get<ruvia::json_value>("value")) {
                        value = service::message::telemetry::canonicalPointJson(
                            current->view(),
                            dataType ? dataType->view() : std::string_view{}
                        );
                    }
                    if (const auto observed =
                            json.template get<ruvia::int64>("observedAt")) {
                        time = service::utils::jsonQuoted(service::common::utcTimestampFromMilliseconds(static_cast<std::int64_t>(*observed)));
                    }
                }
            }
            body += "{\"id\":" + service::utils::jsonQuoted(device->points[index].id) +
                ",\"name\":" + service::utils::jsonQuoted(device->points[index].name) +
                ",\"value\":" + value +
                ",\"unit\":" + service::utils::jsonQuoted(device->points[index].unit) +
                ",\"time\":" + time + "}";
        }
        body += "]}";
        co_return body;
    }

  private:
    static ruvia::db_expression emptyJsonArray(ruvia::db_query& query) {
        return query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
    }

    template <typename Context>
    static ruvia::task<void> refreshProjection(Context& c) {
        (void)co_await service::utils::callRpc(c, "access", "refresh", "{}");
    }

    static ruvia::task<std::optional<service::message::access::session::Entry>> loadSession(
        ruvia::context& c,
        std::string_view rawKey
    ) {
        static constexpr std::string_view script = R"lua(
local version = redis.call('GET', KEYS[1])
if not version then return nil end
return redis.call('HGET', ARGV[2] .. version, ARGV[1])
)lua";
        const auto keyHash = service::utils::sha256(rawKey);
        const std::string activeKey(service::message::access::session::kActiveVersionKey);
        const std::string_view keys[]{ activeKey };
        const std::string prefix(service::message::access::session::kVersionPrefix);
        const std::string_view args[]{ keyHash, prefix };
        const auto reply = co_await c.redis().eval(script, keys, args);
        if (reply.null()) {
            co_return std::nullopt;
        }
        if (reply.kind() != ruvia::redis_value::kind_type::string) {
            service::utils::redis::throwValue("load projected access session", reply);
        }
        co_return service::message::access::session::decode(reply.string());
    }

    static std::string_view projectionField(
        const ruvia::redis_value& value,
        std::string_view operation,
        std::string_view field
    ) {
        if (value.kind() != ruvia::redis_value::kind_type::array) {
            service::utils::redis::throwValue(operation, value);
        }
        const auto& values = value.array();
        if (values.size() % 2 != 0) {
            service::utils::redis::throwValue(operation, value);
        }
        for (std::size_t index = 0; index < values.size(); index += 2) {
            if (values[index].kind() != ruvia::redis_value::kind_type::string ||
                values[index + 1].kind() != ruvia::redis_value::kind_type::string) {
                service::utils::redis::throwValue(operation, value);
            }
            if (values[index].string() == field) {
                return values[index + 1].string();
            }
        }
        return {};
    }

    static ruvia::task<std::optional<service::message::realtime::Device>> loadRealtimeDevice(
        ruvia::context& c,
        std::string_view deviceId
    ) {
        const std::vector<std::string> versionReplyArguments{ "GET", std::string(service::message::projection::kRuntimeActiveVersionKey) };
        const auto versionReply = co_await c.redis().command(std::vector<std::string_view>(versionReplyArguments.begin(), versionReplyArguments.end()));
        if (versionReply.null()) {
            throw std::runtime_error("runtime is not ready");
        }
        if (versionReply.kind() != ruvia::redis_value::kind_type::string) {
            service::utils::redis::throwValue("GET active runtime", versionReply);
        }
        const auto version = versionReply.string();
        const std::vector<std::vector<std::string>> commands{
            { "HGETALL", service::message::projection::realtimeDeviceKey(version, deviceId) },
            { "LRANGE", service::message::projection::realtimeDevicePointsKey(version, deviceId), "0", "-1" }
        };
        auto pipeline = c.redis().pipeline();
        for (const auto& command : commands) {
            const std::vector<std::string_view> views(command.begin(), command.end());
            pipeline.command(views);
        }
        const auto replies = co_await std::move(pipeline).exec();
        if (replies.size() != 2) {
            throw std::runtime_error("incomplete realtime device projection reply");
        }
        if (replies[0].kind() != ruvia::redis_value::kind_type::array ||
            replies[0].array().empty()) {
            co_return std::nullopt;
        }

        service::message::realtime::Device result;
        result.id.assign(projectionField(replies[0], "read realtime device metadata", "id"));
        result.code.assign(projectionField(replies[0], "read realtime device metadata", "code"));
        result.name.assign(projectionField(replies[0], "read realtime device metadata", "name"));
        if (result.id.empty()) {
            co_return std::nullopt;
        }
        if (replies[1].kind() != ruvia::redis_value::kind_type::array) {
            service::utils::redis::throwValue("read realtime device points", replies[1]);
        }
        const auto& points = replies[1].array();
        result.points.reserve(points.size());
        for (const auto& point : points) {
            if (point.kind() != ruvia::redis_value::kind_type::string) {
                service::utils::redis::throwValue("read realtime device points", replies[1]);
            }
            const auto decoded = service::message::realtime::decodePoint(point.string());
            if (!decoded) {
                throw std::runtime_error("invalid realtime point projection");
            }
            result.points.push_back(*decoded);
        }
        co_return result;
    }

    struct KeyState final {
        std::string name;
        std::string status;
        std::set<std::string, std::less<>> scopes;
        std::vector<std::string> deviceIds;
        std::optional<std::string> expiresAt;
        std::optional<std::string> remark;
    };

    struct WebhookState final {
        std::string accessKeyId;
        std::string name;
        std::string url;
        std::string status;
        std::int64_t timeout{ 5 };
        bool skipTlsVerify{ false };
        std::string headers{ "{}" };
        std::set<std::string, std::less<>> events;
        std::optional<std::string> secret;
    };

    static std::string requestAccessKey(const ruvia::context_request& request) {
        return service::utils::trim(request.header("X-Access-Key").value_or(""));
    }

    template <typename Rows>
    static std::string firstJson(const Rows& rows) {
        if (rows.empty() || rows.front().empty() || !rows.front()[0].value().has_value()) {
            return "[]";
        }
        return std::string(rows.front()[0].value().value_or(std::string_view{}));
    }

    template <typename Range>
    static std::string stringArrayJson(const Range& values) {
        std::string result{ "[" };
        bool first = true;
        for (const auto& value : values) {
            if (!first) {
                result.push_back(',');
            }
            first = false;
            result += service::utils::jsonQuoted(value);
        }
        result.push_back(']');
        return result;
    }

    static std::set<std::string, std::less<>> parseStringArray(std::string_view json) {
        std::set<std::string, std::less<>> result;
        const auto value = ruvia::json_value::parse(json);
        if (!value.is_array()) {
            return result;
        }
        const std::string document = "{\"values\":" + std::string(json) + "}";
        const auto object = ruvia::json_value::parse(document);
        const auto parsed = object.get<ruvia::array<ruvia::string>>("values");
        if (!parsed) {
            return result;
        }
        for (const auto& item : *parsed) {
            result.emplace(item.view());
        }
        return result;
    }

    template <typename Context>
    ruvia::task<void> ensureDevicesAccessible(Context& c, const std::vector<std::string>& ids, bool requireOperate) {
        const auto actor = co_await service::device::DeviceAccessService::instance().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("scoped_device");
        service::device::DeviceAccessService::addScopedDevicesCtes(queryBuilder, query, actor);
        std::vector<ruvia::db_expression> deviceIds;
        deviceIds.reserve(ids.size());
        for (const auto& id : ids) {
            deviceIds.push_back(db::uuid(query, id));
        }

        queryBuilder.select({ { "value0", query.aggregate("count", { query.star() }) } }).from_cte("scoped_device").where(db::andAll(query, query.binary(query.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::in, query.list(deviceIds)), query.binary(query.column("access_rank"), ruvia::db_binary_operator::greater_equal, db::integer(query, requireOperate ? 2 : 1))));
        const auto rows = co_await queryBuilder.get_raw_many();
        const auto visible =
            service::utils::parseInt64(
                std::optional<std::string_view>{ rows.front()[0].value().value_or(std::string_view{}) }
            )
                .value_or(-1);
        if (visible != static_cast<std::int64_t>(ids.size())) {
            service::common::fail(19011, requireOperate ? "所选设备中包含无控制权限的设备" : "所选设备中包含无访问权限的设备", 403);
        }
    }

    static ruvia::task<void> replaceDevices(ruvia::db_transaction& transaction, std::string_view keyId, const std::vector<std::string>& devices) {
        ruvia::db_query remove;
        auto removeBuilder = transaction.template repository<service::open_access::entities::OpenAccessKeyDeviceEntity>().delete_builder();
        removeBuilder.where(remove.binary(remove.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, db::uuid(remove, keyId)));
        (void)co_await removeBuilder.execute();
        for (const auto& device : devices) {
            ruvia::db_query insert;
            const std::array<std::string_view, 2> insertColumns{ service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">(), service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">() };
            auto insertBuilder = transaction.template repository<service::open_access::entities::OpenAccessKeyDeviceEntity>().insert_builder();
            ruvia::db_values_builder insertValues(insertColumns);
            insertValues.row({ db::uuid(insert, keyId), db::uuid(insert, device) });
            insertBuilder.insert_from(insertColumns, insertValues.query());
            (void)co_await insertBuilder.execute();
        }
    }

    template <typename Context>
    ruvia::task<void> ensureKeyNameAvailable(Context& c, std::string_view name, std::optional<std::string> except) {
        ruvia::db_query query(c.pool());
        auto predicate = db::andAll(
            query,
            query.binary(query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">()), ruvia::db_binary_operator::equal, query.value(name)),
            query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))
        );
        if (except) {
            predicate = query.binary(
                predicate,
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::not_equal, db::uuid(query, *except))
            );
        }
        auto queryBuilder = c.db().template repository<service::open_access::entities::OpenAccessKeyEntity>().query_builder();
        queryBuilder.select({ { "value0", db::integer(query, 1) } }).where(predicate).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (!rows.empty()) {
            service::common::fail(19003, "调用配置名称已存在", 409);
        }
    }

    template <typename Context>
    ruvia::task<void> ensureWebhookNameAvailable(Context& c, std::string_view accessKeyId, std::string_view name, std::optional<std::string> except) {
        ruvia::db_query query(c.pool());
        auto predicate = db::andAll(
            query,
            query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, db::uuid(query, accessKeyId)),
            query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"name">()), ruvia::db_binary_operator::equal, query.value(name)),
            query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))
        );
        if (except) {
            predicate = query.binary(
                predicate,
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">()), ruvia::db_binary_operator::not_equal, db::uuid(query, *except))
            );
        }
        auto queryBuilder = c.db().template repository<service::open_access::entities::OpenWebhookEntity>().query_builder();
        queryBuilder.select({ { "value0", db::integer(query, 1) } }).where(predicate).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (!rows.empty()) {
            service::common::fail(19003, "同一调用配置下的 Webhook 名称已存在", 409);
        }
    }

    template <typename Context>
    ruvia::task<KeyState> requireKey(Context& c, std::string_view id) {
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::open_access::entities::OpenAccessKeyEntity>().query_builder();
        queryBuilder.select({ { "name", query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">()) }, { "status", db::text(query, query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"status">())) }, { "scopes", db::text(query, query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"scopes">())) }, { "expires_at", query.call("iot_utc_timestamp", { query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"expires_at">()) }) }, { "remark", query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"remark">()) } }).where(db::andAll(query, query.binary(query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(query, id)), query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">())))).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(19001, "调用配置不存在", 404);
        }
        const auto& row = rows.front();
        KeyState state;
        state.name = std::string(row[0].value().value_or(std::string_view{}));
        state.status = std::string(row[1].value().value_or(std::string_view{}));
        state.scopes = parseStringArray(row[2].value().value_or(std::string_view{}));
        if (row[3].value().has_value()) {
            state.expiresAt = std::string(row[3].value().value_or(std::string_view{}));
        }
        if (row[4].value().has_value()) {
            state.remark = std::string(row[4].value().value_or(std::string_view{}));
        }
        ruvia::db_query devicesQuery(c.pool());
        auto devicesQueryBuilder = c.db().template repository<service::open_access::entities::OpenAccessKeyDeviceEntity>().query_builder();
        devicesQueryBuilder.select({ { "device_id", db::text(devicesQuery, devicesQuery.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">())) } }).where(devicesQuery.binary(devicesQuery.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, db::uuid(devicesQuery, id))).order_by(devicesQuery.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">()));
        const auto devices = co_await devicesQueryBuilder.get_raw_many();
        for (const auto& device : devices) {
            state.deviceIds.emplace_back(device[0].value().value_or(std::string_view{}));
        }
        co_return state;
    }

    template <typename Context>
    ruvia::task<WebhookState> requireWebhook(Context& c, std::string_view id) {
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::open_access::entities::OpenWebhookEntity>().query_builder();
        queryBuilder.select({ { "access_key_id", db::text(query, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">())) }, { "name", query.column(service::open_access::entities::OpenWebhookEntity::column_name<"name">()) }, { "url", query.column(service::open_access::entities::OpenWebhookEntity::column_name<"url">()) }, { "status", db::text(query, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"status">())) }, { "timeout_seconds", query.column(service::open_access::entities::OpenWebhookEntity::column_name<"timeout_seconds">()) }, { "skip_tls_verify", query.case_when({ { query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"skip_tls_verify">()), ruvia::db_binary_operator::equal, db::boolean(query, true)), db::text(query, "1") } }, db::text(query, "0")) }, { "headers", db::text(query, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"headers">())) }, { "event_types", db::text(query, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"event_types">())) }, { "secret", query.column(service::open_access::entities::OpenWebhookEntity::column_name<"secret">()) } }).where(db::andAll(query, query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(query, id)), query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">())))).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(19001, "Webhook 不存在", 404);
        }
        const auto& row = rows.front();
        WebhookState state;
        state.accessKeyId = std::string(row[0].value().value_or(std::string_view{}));
        state.name = std::string(row[1].value().value_or(std::string_view{}));
        state.url = std::string(row[2].value().value_or(std::string_view{}));
        state.status = std::string(row[3].value().value_or(std::string_view{}));
        state.timeout =
            service::utils::parseInt64(std::optional<std::string_view>{ row[4].value().value_or(std::string_view{}) })
                .value_or(state.timeout);
        state.skipTlsVerify = row[5].value().value_or(std::string_view{}) == "1";
        state.headers = std::string(row[6].value().value_or(std::string_view{}));
        state.events = parseStringArray(row[7].value().value_or(std::string_view{}));
        if (row[8].value().has_value()) {
            state.secret = std::string(row[8].value().value_or(std::string_view{}));
        }
        co_return state;
    }

    static ruvia::task<void> requireSessionDevice(ruvia::context& c, const AccessSession& session, std::string_view deviceId) {
        service::common::requireUuid(19002, deviceId, "设备 ID 无效");
        if (!session.allowsDevice(deviceId)) {
            service::common::fail(19011, "AccessKey 无权访问该设备", 403);
        }
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder();
        queryBuilder.select({ { "value0", db::integer(query, 1) } }).where(db::andAll(query, query.binary(query.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(query, deviceId)), query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceEntity::column_name<"deleted_at">())))).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(19001, "设备不存在", 404);
        }
    }

  public:
    // 第三方 /api 历史输出形状（device + points，过滤 JPEG）与 DeviceService::historyQuery 不同，不可复用。
    template <typename Database>
    static auto publicHistoryQuery(Database&& database, std::pmr::memory_resource* resource, std::string_view deviceId, std::string_view start, std::string_view end, std::int64_t pageSize, std::int64_t offset, std::int64_t page)
        -> decltype(database.template repository<service::device::entities::DeviceDataEntity>().query_builder()) {
        const auto timestamp = [](ruvia::db_query& query, std::string_view value) {
            return query.cast(query.value(value), ruvia::db_data_type::timestamp_tz);
        };
        const auto emptyObject = [](ruvia::db_query& query) {
            return query.cast(query.value("{}"), ruvia::db_data_type::jsonb);
        };
        const auto emptyArray = [](ruvia::db_query& query) {
            return query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
        };

        ruvia::db_query deviceRef(resource);
        const auto deviceParams = deviceRef.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "device");
        deviceRef
            .select({ deviceRef.column(service::device::entities::DeviceEntity::column_name<"id">(), "device"), deviceRef.alias(db::jsonText(deviceRef, deviceParams, "device_code"), "code"), deviceRef.column(service::device::entities::DeviceEntity::column_name<"name">(), "device") })
            .from(service::device::entities::DeviceEntity::table_name(), "device")
            .where(db::andAll(deviceRef, deviceRef.binary(deviceRef.column(service::device::entities::DeviceEntity::column_name<"id">(), "device"), ruvia::db_binary_operator::equal, db::uuid(deviceRef, deviceId)), deviceRef.unary(ruvia::db_unary_operator::is_null, deviceRef.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "device"))));

        ruvia::db_query counted(resource);
        const auto countedData = counted.column(service::device::entities::DeviceDataEntity::column_name<"data">(), "record");
        counted
            .select(counted.aggregate("count", { counted.star() }))
            .from(service::device::entities::DeviceDataEntity::table_name(), "record")
            .where(db::andAll(counted, counted.binary(counted.column(service::device::entities::DeviceDataEntity::column_name<"device_id">(), "record"), ruvia::db_binary_operator::equal, db::uuid(counted, deviceId)), counted.binary(counted.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::greater_equal, timestamp(counted, start)), counted.binary(counted.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::less_equal, timestamp(counted, end)), counted.binary(counted.call("jsonb_typeof", { db::jsonValue(counted, countedData, "values") }), ruvia::db_binary_operator::equal, db::text(counted, "object"))));

        ruvia::db_query filtered(resource);
        const auto filteredData = filtered.column(service::device::entities::DeviceDataEntity::column_name<"data">(), "record");
        filtered
            .select({ filtered.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "record"), filtered.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), filteredData })
            .from(service::device::entities::DeviceDataEntity::table_name(), "record")
            .where(db::andAll(filtered, filtered.binary(filtered.column(service::device::entities::DeviceDataEntity::column_name<"device_id">(), "record"), ruvia::db_binary_operator::equal, db::uuid(filtered, deviceId)), filtered.binary(filtered.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::greater_equal, timestamp(filtered, start)), filtered.binary(filtered.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::less_equal, timestamp(filtered, end)), filtered.binary(filtered.call("jsonb_typeof", { db::jsonValue(filtered, filteredData, "values") }), ruvia::db_binary_operator::equal, db::text(filtered, "object"))))
            .order_by(filtered.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_order_direction::desc)
            .add_order_by(filtered.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "record"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>(offset));

        ruvia::db_query normalizedValues(resource);
        const auto normalizedPoint = normalizedValues.column("value", "point");
        const auto normalizedPointValue = db::jsonValue(
            normalizedValues,
            normalizedPoint,
            "value"
        );
        const auto pointObject = db::andAll(
            normalizedValues,
            normalizedValues.binary(normalizedValues.call("jsonb_typeof", { normalizedPoint }), ruvia::db_binary_operator::equal, db::text(normalizedValues, "object")),
            normalizedValues.binary(normalizedValues.call("jsonb_typeof", { normalizedPointValue }), ruvia::db_binary_operator::equal, db::text(normalizedValues, "boolean"))
        );
        const auto pointBoolean = normalizedValues.cast(
            db::jsonText(normalizedValues, normalizedPoint, "value"),
            ruvia::db_data_type::boolean
        );
        const auto pointNumber = normalizedValues.case_when(
            { { pointBoolean,
                normalizedValues.cast(normalizedValues.value(1), ruvia::db_data_type::integer) } },
            normalizedValues.cast(normalizedValues.value(0), ruvia::db_data_type::integer)
        );
        const auto pointPath = normalizedValues.cast(
            normalizedValues.array({ db::text(normalizedValues, "value") }),
            ruvia::db_type_definition{ .data_type_ = ruvia::db_data_type::text, .array_ = true }
        );
        const auto normalizedPointValueExpr = normalizedValues.case_when(
            { { pointObject,
                normalizedValues.call(
                    "jsonb_set",
                    { normalizedPoint, pointPath, normalizedValues.call("to_jsonb", { pointNumber }), normalizedValues.cast(normalizedValues.value(false), ruvia::db_data_type::boolean) }
                ) } },
            normalizedPoint
        );
        const std::array<ruvia::db_source_column, 2> sourceColumns1{ { { .name_ = "key" }, { .name_ = "value" } } };
        normalizedValues
            .select(normalizedValues.aggregate(
                "jsonb_object_agg",
                { normalizedValues.column("key", "point"), normalizedPointValueExpr }
            ))

            .from_function(
                normalizedValues.call(
                    "jsonb_each",
                    { normalizedValues.coalesce(
                        { db::jsonValue(
                              normalizedValues,
                              normalizedValues.column(service::device::entities::DeviceDataEntity::column_name<"data">(), "filtered"),
                              "values"
                          ),
                          emptyObject(normalizedValues) }
                    ) }
                ),
                "point",
                { .lateral_ = true,
                  .columns_ = sourceColumns1 }
            );
        ruvia::db_query normalized(resource);
        normalized
            .select({ normalized.star("filtered"), normalized.alias(normalized.coalesce({ normalized.subquery(normalizedValues), emptyObject(normalized) }), "normalized_values") })
            .from(filtered, "filtered");

        ruvia::db_query items(resource);
        const auto point = items.column("value", "point");
        const auto pointValue = db::jsonValue(
            items,
            point,
            "value"
        );
        const auto normalizedData = items.column(service::device::entities::DeviceDataEntity::column_name<"data">(), "normalized");
        const auto itemPointBoolean = items.cast(
            db::jsonText(items, point, "value"),
            ruvia::db_data_type::boolean
        );
        const auto itemPointNumber = items.case_when(
            { { itemPointBoolean, items.cast(items.value(1), ruvia::db_data_type::integer) } },
            items.cast(items.value(0), ruvia::db_data_type::integer)
        );
        const auto itemPoint = items.call(
            "jsonb_build_object",
            { db::text(items, "id"), items.column("key", "point"), db::text(items, "name"), items.coalesce({ db::jsonText(items, point, "name"), items.column("key", "point") }), db::text(items, "value"), items.case_when({ { items.binary(items.call("jsonb_typeof", { pointValue }), ruvia::db_binary_operator::equal, db::text(items, "boolean")), items.call("to_jsonb", { itemPointNumber }) } }, pointValue), db::text(items, "unit"), items.coalesce({ db::jsonText(items, point, "unit"), db::text(items, "") }), db::text(items, "time"), items.call("iot_utc_timestamp", { items.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "normalized") }) }
        );
        const std::array<ruvia::db_order_term, 1> pointOrder{ { ruvia::db_order_term{ items.column("key", "point"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value } } };
        const auto points = items.coalesce(
            { items.filter(
                  items.aggregate("jsonb_agg", { itemPoint }, false, pointOrder),
                  items.unary(ruvia::db_unary_operator::is_not_null, items.column("key", "point"))
              ),
              emptyArray(items) }
        );
        const auto item = items.call(
            "jsonb_build_object",
            { db::text(items, "device"),
              items.call("jsonb_build_object", { db::text(items, "id"), items.cast(items.column(service::device::entities::DeviceEntity::column_name<"id">(), "device_ref"), ruvia::db_data_type::uuid), db::text(items, "code"), items.column("code", "device_ref"), db::text(items, "name"), items.column(service::device::entities::DeviceEntity::column_name<"name">(), "device_ref") }),
              db::text(items, "points"),
              points }
        );
        const std::array<ruvia::db_source_column, 2> sourceColumns2{ { { .name_ = "key" }, { .name_ = "value" } } };
        items
            .select({ items.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "normalized"), items.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "normalized"), items.alias(item, "item") })
            .from(normalized, "normalized")
            .join(ruvia::db_join_type::cross, "device_ref", {}, "device_ref")

            .join_function(
                ruvia::db_join_type::left,
                items.call("jsonb_each", { db::jsonValue(items, normalizedData, "values") }),
                items.binary(
                    items.coalesce({ db::jsonText(items, point, "type"), db::text(items, "") }),
                    ruvia::db_binary_operator::not_equal,
                    db::text(items, "JPEG")
                ),
                "point",
                { .lateral_ = true,
                  .columns_ = sourceColumns2 }
            )
            .group_by({ items.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "normalized"), items.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "normalized"), items.column(service::device::entities::DeviceEntity::column_name<"id">(), "device_ref"), items.column("code", "device_ref"), items.column(service::device::entities::DeviceEntity::column_name<"name">(), "device_ref") });

        ruvia::db_query query(resource);
        auto queryBuilder = database.template repository<service::device::entities::DeviceDataEntity>().query_builder("items");
        queryBuilder.with("device_ref", deviceRef).with("counted", counted).with("filtered", filtered).with("normalized", normalized).with("items", items);
        const auto total = query.coalesce(
            { query.subquery(counted), query.cast(query.value(0), ruvia::db_data_type::big_int) }
        );
        const std::array<ruvia::db_order_term, 2> historyOrder{ { ruvia::db_order_term{ query.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "items"),
                                                                                        ruvia::db_order_direction::desc,
                                                                                        ruvia::db_nulls_order::default_value },
                                                                  ruvia::db_order_term{ query.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "items"), ruvia::db_order_direction::desc, ruvia::db_nulls_order::default_value } } };
        const auto list = query.coalesce(
            { query.aggregate(
                  "jsonb_agg",
                  { query.column("item", "items") },
                  false,
                  historyOrder
              ),
              emptyArray(query) }
        );
        const auto totalPages = query.cast(
            query.call("ceil", { query.binary(query.cast(total, ruvia::db_data_type::numeric), ruvia::db_binary_operator::divide, query.cast(query.value(pageSize), ruvia::db_data_type::numeric)) }),
            ruvia::db_data_type::big_int
        );
        queryBuilder.select({ { "value0", query.cast(query.call("jsonb_build_object", { db::text(query, "list"), list, db::text(query, "total"), total, db::text(query, "page"), query.cast(query.value(page), ruvia::db_data_type::big_int), db::text(query, "pageSize"), query.cast(query.value(pageSize), ruvia::db_data_type::big_int), db::text(query, "totalPages"), totalPages }), ruvia::db_data_type::text) } }).from_cte("items");
        return queryBuilder;
    }
};

} // namespace service::open_access
