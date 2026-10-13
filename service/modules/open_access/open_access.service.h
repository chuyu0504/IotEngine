#pragma once

#include <memory>
#include <algorithm>
#include <array>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory_resource>
#include <openssl/rand.h>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/model_object.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/middleware/rpc.h"
#include "service/modules/device/device.service.h"
#include "service/modules/open_access/open_access.entity.h"
#include "service/modules/open_access/open_access.types.h"
#include "service/modules/system/outbox/outbox.service.h"
#include "service/utils/crypto.h"
#include "service/utils/json.h"
#include "service/utils/number.h"
#include "service/utils/text.h"

namespace service::access {

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
            { textKey(listed, "id"), text(listed, keyId), textKey(listed, "name"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), "key"), textKey(listed, "accessKeyPrefix"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"access_key_prefix">(), "key"), textKey(listed, "status"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"status">(), "key"), textKey(listed, "scopes"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"scopes">(), "key"), textKey(listed, "expiresAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"expires_at">(), "key") }), textKey(listed, "lastUsedAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"last_used_at">(), "key") }), textKey(listed, "lastUsedIp"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"last_used_ip">(), "key"), textKey(listed, "remark"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"remark">(), "key"), textKey(listed, "createdAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"created_at">(), "key") }), textKey(listed, "updatedAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"updated_at">(), "key") }), textKey(listed, "webhookCount"), listed.aggregate("count", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">(), "webhook") }, true), textKey(listed, "deviceIds"), listed.coalesce({ listed.filter(listed.aggregate("jsonb_agg", { bindingId }, true), listed.unary(ruvia::db_unary_operator::is_not_null, bindingId)), emptyJson(listed) }) }
        );
        listed.select({ listed.alias(item, "item"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"created_at">(), "key") })
            .from(service::open_access::entities::OpenAccessKeyEntity::table_name(), "key")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), listed.binary(listed.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">(), "binding"), ruvia::db_binary_operator::equal, keyId), "binding")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenWebhookEntity::table_name(), andAll(listed, listed.binary(listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), "webhook"), ruvia::db_binary_operator::equal, keyId), listed.unary(ruvia::db_unary_operator::is_null, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">(), "webhook"))), "webhook")
            .where(listed.unary(ruvia::db_unary_operator::is_null, listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">(), "key")))
            .group_by({ keyId });
        ruvia::db_query query(c.pool());
        const std::array<ruvia::db_order_term, 1> keyOrder{ { ruvia::db_order_term{ query.column("created_at", "listed"),
                                                                                ruvia::db_order_direction::desc,
                                                                                ruvia::db_nulls_order::default_value } } };
        query.select(text(query, query.coalesce({ query.aggregate("jsonb_agg", { query.column("item", "listed") }, false, keyOrder), emptyJson(query) })))
            .from(listed, "listed");
        co_return firstJson(co_await c.db().query(query));
    }

    template <typename Context>
    ruvia::task<std::string> deviceOptions(Context& c) {
        const auto actor = co_await service::device::deviceAccessService().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        service::device::DeviceAccessService::addScopedDevicesCtes(query, actor);
        const auto params = query.column("protocol_params");
        const auto key = service::device::DeviceAccessService::textKey(query, "remote_control");
        const auto hasKey = query.binary(params, ruvia::db_binary_operator::json_has_key, key);
        const auto type = query.call(
            "jsonb_typeof",
            { service::device::DeviceAccessService::jsonValue(query, params, "remote_control") }
        );
        const auto remoteText = service::device::DeviceAccessService::jsonText(
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
                service::device::DeviceAccessService::boolean(query, true) },
              { query.unary(ruvia::db_unary_operator::not_value, hasKey),
                service::device::DeviceAccessService::boolean(query, true) },
              { query.binary(type, ruvia::db_binary_operator::equal, query.value("boolean")),
                booleanRemote },
              { query.binary(type, ruvia::db_binary_operator::equal, query.value("string")),
                stringRemote } },
            service::device::DeviceAccessService::boolean(query, false)
        );
        const auto canCommand = query.binary(
            query.binary(query.column("access_rank"), ruvia::db_binary_operator::greater_equal, service::device::DeviceAccessService::integer(query, 2)),
            ruvia::db_binary_operator::and_value,
            safeRemote
        );
        const auto item = query.call(
            "jsonb_build_object",
            { textKey(query, "id"), service::device::DeviceAccessService::text(query, query.column("id")), textKey(query, "name"), query.column("name"), textKey(query, "deviceCode"), service::device::DeviceAccessService::jsonText(query, params, "device_code"), textKey(query, "canCommand"), canCommand }
        );
        const std::array<ruvia::db_order_term, 2> deviceOrder{ { ruvia::db_order_term{ query.column("name"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value },
                                                               ruvia::db_order_term{ query.column("id"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value } } };
        query.select(text(query, query.coalesce({ query.aggregate("jsonb_agg", { item }, false, deviceOrder), emptyJson(query) })))
            .from("scoped_device")
            .where(query.binary(query.column("access_rank"), ruvia::db_binary_operator::greater, service::device::DeviceAccessService::integer(query, 0)));
        co_return firstJson(co_await c.db().query(query));
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

        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        const auto rawKey = service::access::generateAccessKey();
        const auto prefix = rawKey.substr(0, 14);
        const auto keyHash = service::utils::sha256(rawKey);
        const auto scopeJson = stringArrayJson(scopes);
        const auto expiresAtValue = expiresAt.value_or("");
        const auto remarkValue = remark.value_or("");
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query insert(c.pool());
        insert.insert_into(service::open_access::entities::OpenAccessKeyEntity::table_name(), { "id", "name", "access_key_prefix", "access_key_hash", "status", "scopes", "expires_at", "remark", "created_by" })
            .values({ uuid(insert, id), insert.value(name), insert.value(prefix), insert.value(keyHash), insert.value(status), insert.cast(insert.value(scopeJson), ruvia::db_data_type::jsonb), insert.cast(insert.null_if(insert.value(expiresAtValue), insert.value("")), ruvia::db_data_type::timestamp_tz), insert.null_if(insert.value(remarkValue), insert.value("")), uuid(insert, c.userId) });
        (void)co_await transaction.execute(insert);
        co_await replaceDevices(transaction, id, devices);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "access_key", "created", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
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
        update.update(service::open_access::entities::OpenAccessKeyEntity::table_name())
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), update.value(name))
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"status">(), update.value(status))
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"scopes">(), update.cast(update.value(scopeJson), ruvia::db_data_type::jsonb))
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"expires_at">(), update.cast(update.null_if(update.value(expiresAtValue), update.value("")), ruvia::db_data_type::timestamp_tz))
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"remark">(), update.null_if(update.value(remarkValue), update.value("")))
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"updated_at">(), update.call("now"))
            .where(andAll(update, update.binary(update.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::equal, uuid(update, id)), update.unary(ruvia::db_unary_operator::is_null, update.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))));
        (void)co_await transaction.execute(update);
        if (payload.get<"deviceIds">()) {
            co_await replaceDevices(transaction, id, devices);
        }
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "access_key", "updated", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
        co_await refreshProjection(c);
    }

    template <typename Context>
    ruvia::task<std::string> rotateKey(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "调用配置 ID 无效");
        const auto existing = co_await requireKey(c, id);
        const auto rawKey = service::access::generateAccessKey();
        const auto prefix = rawKey.substr(0, 14);
        const auto keyHash = service::utils::sha256(rawKey);
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query update(c.pool());
        update.update(service::open_access::entities::OpenAccessKeyEntity::table_name())
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"access_key_hash">(), update.value(keyHash))
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"access_key_prefix">(), update.value(prefix))
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"updated_at">(), update.call("now"))
            .where(andAll(update, update.binary(update.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::equal, uuid(update, id)), update.unary(ruvia::db_unary_operator::is_null, update.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))));
        (void)co_await transaction.execute(update);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "access_key", "rotated", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
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
        webhookUpdate.update(service::open_access::entities::OpenWebhookEntity::table_name())
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">(), webhookUpdate.call("now"))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"updated_at">(), webhookUpdate.call("now"))
            .where(andAll(webhookUpdate, webhookUpdate.binary(webhookUpdate.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, uuid(webhookUpdate, id)), webhookUpdate.unary(ruvia::db_unary_operator::is_null, webhookUpdate.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))));
        (void)co_await transaction.execute(webhookUpdate);
        ruvia::db_query mqttUpdate(c.pool());
        using M = service::open_access::entities::OpenMqttEntity;
        mqttUpdate.update(M::table_name()).set(M::column_name<"deleted_at">(),mqttUpdate.call("now"))
            .set(M::column_name<"updated_at">(),mqttUpdate.call("now"))
            .where(andAll(mqttUpdate,mqttUpdate.binary(mqttUpdate.column(M::column_name<"access_key_id">()),ruvia::db_binary_operator::equal,uuid(mqttUpdate,id)),mqttUpdate.unary(ruvia::db_unary_operator::is_null,mqttUpdate.column(M::column_name<"deleted_at">()))));
        (void)co_await transaction.execute(mqttUpdate);
        ruvia::db_query keyUpdate(c.pool());
        keyUpdate.update(service::open_access::entities::OpenAccessKeyEntity::table_name())
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">(), keyUpdate.call("now"))
            .set(service::open_access::entities::OpenAccessKeyEntity::column_name<"updated_at">(), keyUpdate.call("now"))
            .where(andAll(keyUpdate, keyUpdate.binary(keyUpdate.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::equal, uuid(keyUpdate, id)), keyUpdate.unary(ruvia::db_unary_operator::is_null, keyUpdate.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))));
        (void)co_await transaction.execute(keyUpdate);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "access_key", "deleted", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
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
            { textKey(listed, "id"), text(listed, webhookId), textKey(listed, "accessKeyId"), text(listed, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), "webhook")), textKey(listed, "accessKeyName"), listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">(), "key"), textKey(listed, "name"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"name">(), "webhook"), textKey(listed, "url"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"url">(), "webhook"), textKey(listed, "status"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"status">(), "webhook"), textKey(listed, "timeoutSeconds"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"timeout_seconds">(), "webhook"), textKey(listed, "skipTlsVerify"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"skip_tls_verify">(), "webhook"), textKey(listed, "headers"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"headers">(), "webhook"), textKey(listed, "eventTypes"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"event_types">(), "webhook"), textKey(listed, "deviceIds"), listed.coalesce({ listed.filter(listed.aggregate("jsonb_agg", { bindingDevice }), listed.unary(ruvia::db_unary_operator::is_not_null, bindingDevice)), emptyJson(listed) }), textKey(listed, "hasSecret"), listed.unary(ruvia::db_unary_operator::is_not_null, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"secret">(), "webhook")), textKey(listed, "lastTriggeredAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_triggered_at">(), "webhook") }), textKey(listed, "lastSuccessAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_success_at">(), "webhook") }), textKey(listed, "lastFailureAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_failure_at">(), "webhook") }), textKey(listed, "lastHttpStatus"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_http_status">(), "webhook"), textKey(listed, "lastError"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"last_error">(), "webhook"), textKey(listed, "createdAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"created_at">(), "webhook") }), textKey(listed, "updatedAt"), listed.call("iot_utc_timestamp", { listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"updated_at">(), "webhook") }) }
        );
        listed.select({ listed.alias(item, "item"), listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"created_at">(), "webhook") })
            .from(service::open_access::entities::OpenWebhookEntity::table_name(), "webhook")
            .join(ruvia::db_join_type::inner, service::open_access::entities::OpenAccessKeyEntity::table_name(), andAll(listed, listed.binary(listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key"), ruvia::db_binary_operator::equal, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), "webhook")), listed.unary(ruvia::db_unary_operator::is_null, listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">(), "key"))), "key")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), listed.binary(listed.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">(), "binding"), ruvia::db_binary_operator::equal, listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key")), "binding")
            .where(listed.unary(ruvia::db_unary_operator::is_null, listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">(), "webhook")))
            .group_by({ webhookId, listed.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key") });
        if (const auto& key = filters.get<"accessKeyId">(); key && !key->view().empty()) {
            service::common::requireUuid(19002, key->view(), "调用配置 ID 无效");
            listed.and_where(listed.binary(listed.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), "webhook"), ruvia::db_binary_operator::equal, uuid(listed, key->view())));
        }
        ruvia::db_query query(c.pool());
        const std::array<ruvia::db_order_term, 1> webhookOrder{ { ruvia::db_order_term{ query.column("created_at", "listed"),
                                                                                    ruvia::db_order_direction::desc,
                                                                                    ruvia::db_nulls_order::default_value } } };
        query.select(text(query, query.coalesce({ query.aggregate("jsonb_agg", { query.column("item", "listed") }, false, webhookOrder), emptyJson(query) })))
            .from(listed, "listed");
        co_return firstJson(co_await c.db().query(query));
    }

    template <typename Context>
    ruvia::task<std::string> createWebhook(Context& c, const CreateWebhookBody& payload) {
        if (const auto& headers = payload.get<"headers">(); headers && !isWebhookHeaders(*headers)) service::common::fail(19002, "自定义 Header 包含非法或保留字段", 400);
        const auto accessKeyId = payload.get<"accessKeyId">().view();
        (void)co_await requireKey(c, accessKeyId);
        const auto name = service::utils::trim(payload.get<"name">().view());
        const auto url = service::utils::trim(payload.get<"url">().view());
        const auto status = payload.get<"status">()->view();
        const auto timeout = payload.get<"timeoutSeconds">()->value_;
        const auto skipTlsVerify = payload.get<"skipTlsVerify">()->value_;
        const std::string headers(payload.get<"headers">() ? payload.get<"headers">()->view() : "{}");
        const auto events = payload.get<"eventTypes">() ? accessStringSet(*payload.get<"eventTypes">()) : std::set<std::string, std::less<>>{"device.data.reported"};
        const auto secret = trimAccessText(payload.get<"secret">());
        co_await ensureWebhookNameAvailable(c, accessKeyId, name, std::nullopt);
        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        const auto eventJson = stringArrayJson(events);
        const auto secretValue = secret.value_or("");
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query insert(c.pool());
        insert.insert_into(service::open_access::entities::OpenWebhookEntity::table_name(), { "id", "access_key_id", "name", "url", "status", "timeout_seconds", "skip_tls_verify", "headers", "event_types", "secret" })
            .values({ uuid(insert, id), uuid(insert, accessKeyId), insert.value(name), insert.value(url), insert.value(status), insert.value(timeout), insert.value(skipTlsVerify), insert.cast(insert.value(headers), ruvia::db_data_type::jsonb), insert.cast(insert.value(eventJson), ruvia::db_data_type::jsonb), insert.null_if(insert.value(secretValue), insert.value("")) });
        (void)co_await transaction.execute(insert);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "webhook", "created", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
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
        if (const auto& headers = payload.get<"headers">(); headers && !isWebhookHeaders(*headers)) service::common::fail(19002, "自定义 Header 包含非法或保留字段", 400);
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
        update.update(service::open_access::entities::OpenWebhookEntity::table_name())
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">(), uuid(update, accessKeyId))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"name">(), update.value(name))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"url">(), update.value(url))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"status">(), update.value(status))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"timeout_seconds">(), update.value(timeout))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"skip_tls_verify">(), update.value(skipTlsVerify))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"headers">(), update.cast(update.value(headers), ruvia::db_data_type::jsonb))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"event_types">(), update.cast(update.value(eventJson), ruvia::db_data_type::jsonb))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"secret">(), update.null_if(update.value(secretValue), update.value("")))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"updated_at">(), update.call("now"))
            .where(andAll(update, update.binary(update.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">()), ruvia::db_binary_operator::equal, uuid(update, id)), update.unary(ruvia::db_unary_operator::is_null, update.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))));
        (void)co_await transaction.execute(update);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "webhook", "updated", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> removeWebhook(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "Webhook ID 无效");
        (void)co_await requireWebhook(c, id);
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query update(c.pool());
        update.update(service::open_access::entities::OpenWebhookEntity::table_name())
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">(), update.call("now"))
            .set(service::open_access::entities::OpenWebhookEntity::column_name<"updated_at">(), update.call("now"))
            .where(andAll(update, update.binary(update.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">()), ruvia::db_binary_operator::equal, uuid(update, id)), update.unary(ruvia::db_unary_operator::is_null, update.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))));
        (void)co_await transaction.execute(update);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "webhook", "deleted", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<MqttItem>> listMqtt(Context& c, const WebhookQueryParams& filters) {
        using E = service::open_access::entities::OpenMqttEntity;
        using K = service::open_access::entities::OpenAccessKeyEntity;
        ruvia::db_query q(c.pool());
        q.select({text(q,q.column(E::column_name<"id">(),"m")), text(q,q.column(E::column_name<"access_key_id">(),"m")),
            q.column(K::column_name<"name">(),"k"), q.column(E::column_name<"name">(),"m"),
            q.column(E::column_name<"broker_url">(),"m"),
            q.column(E::column_name<"status">(),"m"), q.column(E::column_name<"timeout_seconds">(),"m"),
            text(q,q.column(E::column_name<"event_types">(),"m")), q.column(E::column_name<"username">(),"m"),
            q.case_when({{q.unary(ruvia::db_unary_operator::is_not_null,q.column(E::column_name<"password">(),"m")),q.value("1")}},q.value("0")),
            q.call("iot_utc_timestamp",{q.column(E::column_name<"last_triggered_at">(),"m")}),
            q.call("iot_utc_timestamp",{q.column(E::column_name<"last_success_at">(),"m")}),
            q.call("iot_utc_timestamp",{q.column(E::column_name<"last_failure_at">(),"m")}),
            q.column(E::column_name<"last_error">(),"m"),
            q.call("iot_utc_timestamp",{q.column(E::column_name<"created_at">(),"m")}),
            q.call("iot_utc_timestamp",{q.column(E::column_name<"updated_at">(),"m")})})
            .from(E::table_name(),"m")
            .join(ruvia::db_join_type::inner,K::table_name(),q.binary(q.column(K::column_name<"id">(),"k"),ruvia::db_binary_operator::equal,q.column(E::column_name<"access_key_id">(),"m")),"k")
            .where(q.unary(ruvia::db_unary_operator::is_null,q.column(E::column_name<"deleted_at">(),"m")))
            .and_where(q.unary(ruvia::db_unary_operator::is_null,q.column(K::column_name<"deleted_at">(),"k")))
            .order_by(q.column(E::column_name<"created_at">(),"m"),ruvia::db_order_direction::desc);
        if (const auto& key = filters.get<"accessKeyId">(); key)
            q.and_where(q.binary(q.column(E::column_name<"access_key_id">(),"m"),ruvia::db_binary_operator::equal,uuid(q,key->view())));
        const auto rows = co_await c.db().query(q);
        ruvia::boxed_array<MqttItem> result(ruvia::model_options{.resource_=c.arena()});
        for (const auto& row : rows) {
            auto& item = result.emplace(ruvia::model_options{.resource_=c.arena()});
            const auto field = [&row](std::size_t i) { return row[i].value().value_or(std::string_view{}); };
            item.set<"id">(field(0)).template set<"accessKeyId">(field(1)).template set<"accessKeyName">(field(2))
                .template set<"name">(field(3)).template set<"brokerUrl">(field(4))
                .template set<"status">(field(5)).template set<"timeoutSeconds">(service::utils::parseInt64(row[6].value()).value_or(5))
                .template set<"hasPassword">(field(9)=="1").template set<"createdAt">(field(14)).template set<"updatedAt">(field(15));
            ruvia::array<ruvia::string> events(ruvia::model_options{.resource_=c.arena()});
            for (const auto& event : parseStringArray(field(7))) events.push_back(ruvia::string(event));
            item.set<"eventTypes">(std::move(events));
            const auto nullable = [&]<ruvia::fixed_string Name>(std::size_t index) {
                if (row[index].value()) item.template set<Name>(*row[index].value());
                else item.template set<Name>(nullptr);
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
        using E = service::open_access::entities::OpenMqttEntity;
        const auto keyId = payload.get<"accessKeyId">().view();
        const auto key = co_await requireKey(c,keyId);
        co_await ensureDevicesAccessible(c,key.deviceIds,false);
        const auto events = accessStringSet(payload.get<"eventTypes">());
        for (const auto& event : events) {
            const auto scope = event.starts_with("device.alert.") ? kScopeAlert :
                event.starts_with("device.command.") ? kScopeCommand : kScopeRealtime;
            if (!key.scopes.contains(scope)) service::common::fail(19011,"事件超出调用配置的开放权限",403);
        }
        std::string password;
        if (id) {
            ruvia::db_query current(c.pool());
            current.select(current.column(E::column_name<"password">())).from(E::table_name())
                .where(andAll(current,current.binary(current.column(E::column_name<"id">()),ruvia::db_binary_operator::equal,uuid(current,*id)),current.unary(ruvia::db_unary_operator::is_null,current.column(E::column_name<"deleted_at">()))));
            const auto rows = co_await c.db().query(current);
            if (rows.empty()) service::common::fail(19001,"MQTT 配置不存在",404);
            password = rows.front()[0].value().value_or(std::string_view{});
        }
        if (payload.template is_present<"password">())
            password = payload.get<"password">() ? std::string(payload.get<"password">()->view()) : "";
        const auto username = payload.get<"username">() ? std::string(payload.get<"username">()->view()) : "";
        if (!service::utils::mqtt::validText(username) || password.size()>65535 || (!password.empty() && username.empty()))
            service::common::fail(19002,"MQTT 凭据无效；设置密码时必须提供用户名",400);
        const auto name = service::utils::trim(payload.get<"name">().view());
        ruvia::db_query duplicate(c.pool());
        duplicate.select(duplicate.value(1)).from(E::table_name())
            .where(andAll(duplicate,duplicate.binary(duplicate.column(E::column_name<"access_key_id">()),ruvia::db_binary_operator::equal,uuid(duplicate,keyId)),
                duplicate.binary(duplicate.column(E::column_name<"name">()),ruvia::db_binary_operator::equal,duplicate.value(name)),
                duplicate.unary(ruvia::db_unary_operator::is_null,duplicate.column(E::column_name<"deleted_at">()))));
        if (id) duplicate.and_where(duplicate.binary(duplicate.column(E::column_name<"id">()),ruvia::db_binary_operator::not_equal,uuid(duplicate,*id)));
        const auto duplicateRows = co_await c.db().query(duplicate);
        if (!duplicateRows.empty()) service::common::fail(19003,"同一调用配置下的 MQTT 名称已存在",409);
        const auto targetId = id.value_or(c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        const auto eventJson = stringArrayJson(events);
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query q(c.pool());
        const auto userValue = q.null_if(q.value(username),q.value(""));
        const auto passwordValue = q.null_if(q.value(password),q.value(""));
        const auto eventValue = q.cast(q.value(eventJson),ruvia::db_data_type::jsonb);
        if (id) {
            q.update(E::table_name())
                .set(E::column_name<"access_key_id">(),uuid(q,keyId)).set(E::column_name<"name">(),q.value(name))
                .set(E::column_name<"broker_url">(),q.value(payload.get<"brokerUrl">().view()))
                .set(E::column_name<"username">(),userValue).set(E::column_name<"password">(),passwordValue)
                .set(E::column_name<"status">(),q.value(payload.get<"status">()->view()))
                .set(E::column_name<"timeout_seconds">(),q.value(payload.get<"timeoutSeconds">()->value_))
                .set(E::column_name<"event_types">(),eventValue).set(E::column_name<"updated_at">(),q.call("now"))
                .where(q.binary(q.column(E::column_name<"id">()),ruvia::db_binary_operator::equal,uuid(q,targetId)));
        } else {
            q.insert_into(E::table_name(),{"id","access_key_id","name","broker_url","username","password","status","timeout_seconds","event_types"})
                .values({uuid(q,targetId),uuid(q,keyId),q.value(name),q.value(payload.get<"brokerUrl">().view()),userValue,passwordValue,
                    q.value(payload.get<"status">()->view()),q.value(payload.get<"timeoutSeconds">()->value_),eventValue});
        }
        (void)co_await transaction.execute(q);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction,"mqtt",id ? "updated" : "created",targetId,c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> removeMqtt(Context& c, std::string_view id) {
        using E = service::open_access::entities::OpenMqttEntity;
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query q(c.pool());
        q.update(E::table_name()).set(E::column_name<"deleted_at">(),q.call("now")).set(E::column_name<"updated_at">(),q.call("now"))
            .where(andAll(q,q.binary(q.column(E::column_name<"id">()),ruvia::db_binary_operator::equal,uuid(q,id)),q.unary(ruvia::db_unary_operator::is_null,q.column(E::column_name<"deleted_at">()))))
            .returning({q.column(E::column_name<"id">())});
        const auto existingRows = co_await transaction.query(q);
        if (existingRows.empty()) service::common::fail(19001,"MQTT 配置不存在",404);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction,"mqtt","deleted",id,c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<std::string> listLogs(Context& c, const AccessLogParams& filters) {
        const auto pageNumber = filters.get<"page">()->value_;
        const auto pageSize = filters.get<"pageSize">()->value_;
        if (pageNumber - 1 > std::numeric_limits<std::int64_t>::max() / pageSize) service::common::fail(19002, "分页超出允许范围", 400);
        const service::common::Page pagination{pageNumber, pageSize, (pageNumber - 1) * pageSize};
        const auto accessKeyId = trimAccessText(filters.get<"accessKeyId">());
        const auto webhookId = trimAccessText(filters.get<"webhookId">());
        const auto mqttId = trimAccessText(filters.get<"mqttId">());
        const auto deviceId = trimAccessText(filters.get<"deviceId">());
        const auto direction = trimAccessText(filters.get<"direction">());
        const auto action = trimAccessText(filters.get<"action">());
        const auto status = trimAccessText(filters.get<"status">());
        const auto eventType = trimAccessText(filters.get<"eventType">());
        const auto applyFilters = [&](ruvia::db_query& query, std::string_view alias) {
            auto predicate = service::device::DeviceAccessService::boolean(query, true);
            const auto addUuid = [&](std::string_view value, std::string_view column) {
                if (!value.empty()) {
                    predicate = query.binary(
                        predicate,
                        ruvia::db_binary_operator::and_value,
                        query.binary(query.column(column, alias), ruvia::db_binary_operator::equal, uuid(query, value))
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
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenAccessKeyEntity::table_name(), page.binary(page.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">(), "key"), ruvia::db_binary_operator::equal, page.column("access_key_id", "page_rows")), "key")
            .join(ruvia::db_join_type::left, service::open_access::entities::OpenWebhookEntity::table_name(), page.binary(page.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">(), "webhook"), ruvia::db_binary_operator::equal, page.column("webhook_id", "page_rows")), "webhook")
            .join(ruvia::db_join_type::left,service::open_access::entities::OpenMqttEntity::table_name(),page.binary(page.column(service::open_access::entities::OpenMqttEntity::column_name<"id">(),"mqtt"),ruvia::db_binary_operator::equal,page.column("mqtt_id","page_rows")),"mqtt");
        ruvia::db_query query(c.pool());
        query.with("counted", counted).with("page_rows", pageRows).with("page", page);
        const auto total = query.subquery(counted);
        const auto item = query.call(
            "jsonb_build_object",
            { textKey(query, "id"), text(query, query.column("id", "page")), textKey(query, "accessKeyId"), text(query, query.column("access_key_id", "page")), textKey(query, "accessKeyName"), query.column("access_key_name", "page"), textKey(query, "webhookId"), text(query, query.column("webhook_id", "page")), textKey(query, "webhookName"), query.column("webhook_name", "page"), textKey(query,"mqttId"),text(query,query.column("mqtt_id","page")),textKey(query,"mqttName"),query.column("mqtt_name","page"), textKey(query, "direction"), query.column("direction", "page"), textKey(query, "action"), query.column("action", "page"), textKey(query, "eventType"), query.column("event_type", "page"), textKey(query, "status"), query.column("status", "page"), textKey(query, "httpMethod"), query.column("http_method", "page"), textKey(query, "target"), query.column("target", "page"), textKey(query, "requestIp"), query.column("request_ip", "page"), textKey(query, "httpStatus"), query.column("http_status", "page"), textKey(query, "deviceId"), text(query, query.column("device_id", "page")), textKey(query, "deviceCode"), query.column("device_code", "page"), textKey(query, "message"), query.column("message", "page"), textKey(query, "requestPayload"), query.column("request_payload", "page"), textKey(query, "responsePayload"), query.column("response_payload", "page"), textKey(query, "createdAt"), query.call("iot_utc_timestamp", { query.column("created_at", "page") }) }
        );
        const std::array<ruvia::db_order_term, 2> logOrder{ { ruvia::db_order_term{ query.column("created_at", "page"),
                                                                                ruvia::db_order_direction::desc,
                                                                                ruvia::db_nulls_order::default_value },
                                                            ruvia::db_order_term{ query.column("id", "page"), ruvia::db_order_direction::desc, ruvia::db_nulls_order::default_value } } };
        const auto list = query.coalesce(
            { query.aggregate("jsonb_agg", { item }, false, logOrder),
              emptyJson(query) }
        );
        const auto totalPages = query.cast(
            query.call("ceil", { query.binary(query.cast(total, ruvia::db_data_type::numeric), ruvia::db_binary_operator::divide, query.cast(query.value(pagination.pageSize), ruvia::db_data_type::numeric)) }),
            ruvia::db_data_type::big_int
        );
        query.select(query.cast(
                         query.call("jsonb_build_object", { textKey(query, "list"), list, textKey(query, "total"), total, textKey(query, "page"), query.cast(query.value(pagination.page), ruvia::db_data_type::big_int), textKey(query, "pageSize"), query.cast(query.value(pagination.pageSize), ruvia::db_data_type::big_int), textKey(query, "totalPages"), totalPages }),
                         ruvia::db_data_type::text
                     ))
            .from("page");
        co_return firstJson(co_await c.db().query(query));
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
        if (session::expired(*projected, service::message::utcNowMilliseconds())) {
            service::common::fail(19010, "AccessKey 已过期", 401);
        }
        AccessSession session;
        session.id = projected->id;
        session.name = projected->name;
        session.scopes = projected->scopes;
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
            std::string payload =
                "{\"action\":" + service::utils::jsonQuoted(action) +
                ",\"accessKeyId\":" + service::utils::jsonQuoted(session.id) +
                ",\"method\":" + service::utils::jsonQuoted(c.req().method()) +
                ",\"target\":" + service::utils::jsonQuoted(c.req().path()) +
                ",\"requestIp\":" +
                service::utils::jsonQuoted(service::common::clientIp(c)) +
                ",\"httpStatus\":200,\"deviceId\":" +
                service::utils::jsonQuoted(deviceId) +
                ",\"requestPayload\":" + std::string(requestPayload) +
                ",\"responsePayload\":" + std::string(responsePayload) + "}";
            (void)co_await service::rpc::call(c, "access", "audit", std::move(payload));
        } catch (...) {
            // Open-access audit is best effort.
        }
    }

    ruvia::task<std::string> publicDevices(ruvia::context& c, const AccessSession& session) {
        const auto pagination = service::common::page(c.req());
        const auto keyword = service::utils::trim(c.req().query("keyword").value_or(""));
        ruvia::db_query visible(c.pool());
        const auto deviceId = visible.column(service::open_access::entities::DeviceEntity::column_name<"id">(), "device");
        const auto code = service::device::DeviceAccessService::jsonText(
            visible,
            visible.column(service::open_access::entities::DeviceEntity::column_name<"protocol_params">(), "device"),
            "device_code"
        );
        const auto pattern = visible.binary(
            visible.binary(service::device::DeviceAccessService::text(visible, "%"), ruvia::db_binary_operator::concat, service::device::DeviceAccessService::text(visible, keyword)),
            ruvia::db_binary_operator::concat,
            service::device::DeviceAccessService::text(visible, "%")
        );
        const auto keywordFilter = visible.binary(
            visible.binary(service::device::DeviceAccessService::text(visible, keyword), ruvia::db_binary_operator::equal, service::device::DeviceAccessService::text(visible, "")),
            ruvia::db_binary_operator::or_value,
            visible.binary(
                visible.binary(
                    visible.binary(service::device::DeviceAccessService::text(visible, deviceId), ruvia::db_binary_operator::i_like, pattern),
                    ruvia::db_binary_operator::or_value,
                    visible.binary(visible.column(service::open_access::entities::DeviceEntity::column_name<"name">(), "device"), ruvia::db_binary_operator::i_like, pattern)
                ),
                ruvia::db_binary_operator::or_value,
                visible.binary(visible.coalesce({ code, visible.value("") }), ruvia::db_binary_operator::i_like, pattern)
            )
        );
        visible.select({ deviceId, visible.column(service::open_access::entities::DeviceEntity::column_name<"name">(), "device"), visible.alias(code, "code") })
            .from(service::open_access::entities::DeviceEntity::table_name(), "device")
            .join(ruvia::db_join_type::inner, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), visible.binary(visible.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">(), "binding"), ruvia::db_binary_operator::equal, deviceId), "binding")
            .where(andAll(visible, visible.binary(visible.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">(), "binding"), ruvia::db_binary_operator::equal, uuid(visible, session.id)), visible.unary(ruvia::db_unary_operator::is_null, visible.column(service::open_access::entities::DeviceEntity::column_name<"deleted_at">(), "device")), keywordFilter));
        ruvia::db_query counted(c.pool());
        counted.select(counted.aggregate("count", { counted.star() })).from(visible, "visible");
        ruvia::db_query page(c.pool());
        page.select(page.star("visible"))
            .from(visible, "visible")
            .order_by(page.column("name", "visible"))
            .add_order_by(page.column("id", "visible"))
            .limit(static_cast<std::uint64_t>(pagination.pageSize))
            .offset(static_cast<std::uint64_t>(pagination.offset));
        ruvia::db_query query(c.pool());
        query.with("visible", visible).with("counted", counted).with("page", page);
        const auto item = query.call(
            "jsonb_build_object",
            { textKey(query, "id"), text(query, query.column("id", "page")), textKey(query, "code"), query.column("code", "page"), textKey(query, "name"), query.column("name", "page") }
        );
        const auto total = query.subquery(counted);
        const std::array<ruvia::db_order_term, 2> deviceListOrder{ { ruvia::db_order_term{ query.column("name", "page"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value },
                                                                   ruvia::db_order_term{ query.column("id", "page"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value } } };
        const auto list = query.coalesce(
            { query.aggregate("jsonb_agg", { item }, false, deviceListOrder),
              emptyJson(query) }
        );
        const auto totalPages = query.cast(
            query.call("ceil", { query.binary(query.cast(total, ruvia::db_data_type::numeric), ruvia::db_binary_operator::divide, query.cast(query.value(pagination.pageSize), ruvia::db_data_type::numeric)) }),
            ruvia::db_data_type::big_int
        );
        query.select(query.cast(
                         query.call("jsonb_build_object", { textKey(query, "list"), list, textKey(query, "total"), total, textKey(query, "page"), query.cast(query.value(pagination.page), ruvia::db_data_type::big_int), textKey(query, "pageSize"), query.cast(query.value(pagination.pageSize), ruvia::db_data_type::big_int), textKey(query, "totalPages"), totalPages }),
                         ruvia::db_data_type::text
                     ))
            .from("page");
        const auto rows = co_await c.db().query(query);
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
            const auto query = historyQuery(c.pool(), deviceId, start, end, pagination.pageSize, pagination.offset, pagination.page);
            const auto rows = co_await c.db().query(query);
            co_return firstJson(rows);
        } catch (const std::exception&) {
            service::common::fail(19002, "时间范围格式错误", 400);
        }
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
            auto predicate = query.binary(query.column("access_key_id", bindingAlias), ruvia::db_binary_operator::equal, uuid(query, session.id));
            if (deviceFilter) {
                predicate = query.binary(
                    predicate,
                    ruvia::db_binary_operator::and_value,
                    query.binary(query.column("device_id", recordAlias), ruvia::db_binary_operator::equal, uuid(query, *deviceFilter))
                );
            }
            if (status && !status->empty()) {
                predicate = query.binary(
                    predicate,
                    ruvia::db_binary_operator::and_value,
                    query.binary(query.column("status", recordAlias), ruvia::db_binary_operator::equal, query.value(*status))
                );
            }
            if (severity && !severity->empty()) {
                predicate = query.binary(
                    predicate,
                    ruvia::db_binary_operator::and_value,
                    query.binary(query.column("severity", recordAlias), ruvia::db_binary_operator::equal, query.value(*severity))
                );
            }
            query.where(predicate);
        };
        ruvia::db_query counted(c.pool());
        counted.select(counted.aggregate("count", { counted.star() }))
            .from(service::open_access::entities::OpenAlertRecordEntity::table_name(), "record")
            .join(ruvia::db_join_type::inner, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), counted.binary(counted.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">(), "binding"), ruvia::db_binary_operator::equal, counted.column(service::open_access::entities::OpenAlertRecordEntity::column_name<"device_id">(), "record")), "binding");
        applyFilters(counted, "record", "binding");
        ruvia::db_query pageRows(c.pool());
        pageRows.select(pageRows.star("record"))
            .from(service::open_access::entities::OpenAlertRecordEntity::table_name(), "record")
            .join(ruvia::db_join_type::inner, service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), pageRows.binary(pageRows.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">(), "binding"), ruvia::db_binary_operator::equal, pageRows.column(service::open_access::entities::OpenAlertRecordEntity::column_name<"device_id">(), "record")), "binding");
        applyFilters(pageRows, "record", "binding");
        pageRows.order_by(pageRows.column(service::open_access::entities::OpenAlertRecordEntity::column_name<"triggered_at">(), "record"), ruvia::db_order_direction::desc)
            .add_order_by(pageRows.column(service::open_access::entities::OpenAlertRecordEntity::column_name<"id">(), "record"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pagination.pageSize))
            .offset(static_cast<std::uint64_t>(pagination.offset));
        ruvia::db_query page(c.pool());
        page.select({ page.star("page_rows"), page.alias(page.column(service::open_access::entities::DeviceEntity::column_name<"name">(), "device"), "device_name"), page.alias(service::device::DeviceAccessService::jsonText(page, page.column(service::open_access::entities::DeviceEntity::column_name<"protocol_params">(), "device"), "device_code"), "device_code") })
            .from(pageRows, "page_rows")
            .join(ruvia::db_join_type::inner, service::open_access::entities::DeviceEntity::table_name(), page.binary(page.column(service::open_access::entities::DeviceEntity::column_name<"id">(), "device"), ruvia::db_binary_operator::equal, page.column("device_id", "page_rows")), "device");
        ruvia::db_query query(c.pool());
        query.with("counted", counted).with("page_rows", pageRows).with("page", page);
        const auto item = query.call(
            "jsonb_build_object",
            { textKey(query, "id"), text(query, query.column("id", "page")), textKey(query, "device"), query.call("jsonb_build_object", { textKey(query, "id"), text(query, query.column("device_id", "page")), textKey(query, "code"), query.column("device_code", "page"), textKey(query, "name"), query.column("device_name", "page") }), textKey(query, "ruleId"), text(query, query.column("rule_id", "page")), textKey(query, "severity"), query.column("severity", "page"), textKey(query, "status"), query.column("status", "page"), textKey(query, "message"), query.column("message", "page"), textKey(query, "time"), query.call("iot_utc_timestamp", { query.column("triggered_at", "page") }) }
        );
        const auto total = query.subquery(counted);
        const std::array<ruvia::db_order_term, 2> alertOrder{ { ruvia::db_order_term{ query.column("triggered_at", "page"),
                                                                                  ruvia::db_order_direction::desc,
                                                                                  ruvia::db_nulls_order::default_value },
                                                              ruvia::db_order_term{ query.column("id", "page"), ruvia::db_order_direction::desc, ruvia::db_nulls_order::default_value } } };
        const auto list = query.coalesce(
            { query.aggregate("jsonb_agg", { item }, false, alertOrder),
              emptyJson(query) }
        );
        const auto totalPages = query.cast(
            query.call("ceil", { query.binary(query.cast(total, ruvia::db_data_type::numeric), ruvia::db_binary_operator::divide, query.cast(query.value(pagination.pageSize), ruvia::db_data_type::numeric)) }),
            ruvia::db_data_type::big_int
        );
        query.select(query.cast(
                         query.call("jsonb_build_object", { textKey(query, "list"), list, textKey(query, "total"), total, textKey(query, "page"), query.cast(query.value(pagination.page), ruvia::db_data_type::big_int), textKey(query, "pageSize"), query.cast(query.value(pagination.pageSize), ruvia::db_data_type::big_int), textKey(query, "totalPages"), totalPages }),
                         ruvia::db_data_type::text
                     ))
            .from("page");
        const auto rows = co_await c.db().query(query);
        co_return firstJson(rows);
    }

    template <typename Context>
    ruvia::task<std::string> realtimeData(Context& c, std::string_view deviceId) {
        const auto device = co_await loadRealtimeDevice(c, deviceId);
        if (!device) {
            service::common::fail(19001, "设备不存在", 404);
        }
        const auto latestKey = service::telemetry::latest::latestKey(device->id);
        const std::vector<std::string> latestArguments{ "HGETALL", latestKey };
        const auto latest = co_await service::message::redis::command(
            c.redis(),
            latestArguments
        );
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
            service::message::redis::throwValue("read realtime device values", latest);
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
                if (const auto json = ruvia::json_value::parse(data->second)) {
                    const auto dataType = json->template get<ruvia::string>("dataType");
                    if (const auto current = json->template get<ruvia::json_value>("value")) {
                        value = service::telemetry::latest::canonicalPointJson(
                            current->view(),
                            dataType ? dataType->view() : std::string_view{}
                        );
                    }
                    if (const auto observed =
                            json->template get<ruvia::int64>("observedAt")) {
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
    static ruvia::db_query::expr_type text(ruvia::db_query& query, ruvia::db_query::expr_type value) {
        return service::device::DeviceAccessService::text(query, value);
    }

    static ruvia::db_query::expr_type text(ruvia::db_query& query, std::string_view value) {
        return service::device::DeviceAccessService::text(query, value);
    }

    static ruvia::db_query::expr_type textKey(ruvia::db_query& query, std::string_view value) {
        return service::device::DeviceAccessService::textKey(query, value);
    }

    static ruvia::db_query::expr_type uuid(ruvia::db_query& query, std::string_view value) {
        return service::device::DeviceAccessService::uuid(query, value);
    }

    static ruvia::db_query::expr_type emptyJson(ruvia::db_query& query) {
        return query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
    }

    static ruvia::db_query::expr_type andAll(ruvia::db_query& query, ruvia::db_query::expr_type first, ruvia::db_query::expr_type second) {
        return service::device::andAll(query, first, second);
    }

    template <typename... Expressions>
    static ruvia::db_query::expr_type andAll(ruvia::db_query& query, ruvia::db_query::expr_type first, ruvia::db_query::expr_type second, Expressions... rest) {
        return service::device::andAll(query, first, second, rest...);
    }

    template <typename Context>
    static ruvia::task<void> refreshProjection(Context& c) {
        (void)co_await service::rpc::call(c, "access", "refresh", "{}");
    }

    static ruvia::task<std::optional<session::Entry>> loadSession(
        ruvia::context& c,
        std::string_view rawKey
    ) {
        static constexpr std::string_view script = R"lua(
local version = redis.call('GET', KEYS[1])
if not version then return nil end
return redis.call('HGET', ARGV[2] .. version, ARGV[1])
)lua";
        const auto keyHash = service::utils::sha256(rawKey);
        const std::string activeKey(session::kActiveVersionKey);
        const std::string_view keys[]{ activeKey };
        const std::string prefix(session::kVersionPrefix);
        const std::string_view args[]{ keyHash, prefix };
        const auto reply = co_await c.redis().eval(script, keys, args);
        if (reply.null()) {
            co_return std::nullopt;
        }
        if (reply.kind() != ruvia::redis_value::kind_type::string) {
            service::message::redis::throwValue("load projected access session", reply);
        }
        co_return session::decode(reply.string());
    }

    static std::string_view projectionField(
        const ruvia::redis_value& value,
        std::string_view operation,
        std::string_view field
    ) {
        if (value.kind() != ruvia::redis_value::kind_type::array) {
            service::message::redis::throwValue(operation, value);
        }
        const auto& values = value.array();
        if (values.size() % 2 != 0) {
            service::message::redis::throwValue(operation, value);
        }
        for (std::size_t index = 0; index < values.size(); index += 2) {
            if (values[index].kind() != ruvia::redis_value::kind_type::string ||
                values[index + 1].kind() != ruvia::redis_value::kind_type::string) {
                service::message::redis::throwValue(operation, value);
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
        const auto versionReply = co_await service::message::redis::command(
            c.redis(),
            versionReplyArguments
        );
        if (versionReply.null()) {
            throw std::runtime_error("runtime is not ready");
        }
        if (versionReply.kind() != ruvia::redis_value::kind_type::string) {
            service::message::redis::throwValue("GET active runtime", versionReply);
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
            service::message::redis::throwValue("read realtime device points", replies[1]);
        }
        const auto& points = replies[1].array();
        result.points.reserve(points.size());
        for (const auto& point : points) {
            if (point.kind() != ruvia::redis_value::kind_type::string) {
                service::message::redis::throwValue("read realtime device points", replies[1]);
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
        if (!value || !value->is_array()) {
            return result;
        }
        const std::string document = "{\"values\":" + std::string(json) + "}";
        const auto object = ruvia::json_value::parse(document);
        const auto parsed = object ? object->get<ruvia::array<ruvia::string>>("values") : std::nullopt;
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
        const auto actor = co_await service::device::deviceAccessService().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        service::device::DeviceAccessService::addScopedDevicesCtes(query, actor);
        std::vector<ruvia::db_query::expr_type> deviceIds;
        deviceIds.reserve(ids.size());
        for (const auto& id : ids) {
            deviceIds.push_back(uuid(query, id));
        }
        query.select(query.aggregate("count", { query.star() }))
            .from("scoped_device")
            .where(andAll(query, query.binary(query.column("id"), ruvia::db_binary_operator::in, query.list(deviceIds)), query.binary(query.column("access_rank"), ruvia::db_binary_operator::greater_equal, service::device::DeviceAccessService::integer(query, requireOperate ? 2 : 1))));
        const auto rows = co_await c.db().query(query);
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
        remove.delete_from(service::open_access::entities::OpenAccessKeyDeviceEntity::table_name())
            .where(remove.binary(remove.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, uuid(remove, keyId)));
        (void)co_await transaction.execute(remove);
        for (const auto& device : devices) {
            ruvia::db_query insert;
            insert.insert_into(service::open_access::entities::OpenAccessKeyDeviceEntity::table_name(), { "access_key_id", "device_id" })
                .values({ uuid(insert, keyId), uuid(insert, device) });
            (void)co_await transaction.execute(insert);
        }
    }

    template <typename Context>
    ruvia::task<void> ensureKeyNameAvailable(Context& c, std::string_view name, std::optional<std::string> except) {
        ruvia::db_query query(c.pool());
        auto predicate = andAll(
            query,
            query.binary(query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">()), ruvia::db_binary_operator::equal, query.value(name)),
            query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))
        );
        if (except) {
            predicate = query.binary(
                predicate,
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::not_equal, uuid(query, *except))
            );
        }
        query.select(service::device::DeviceAccessService::integer(query, 1))
            .from(service::open_access::entities::OpenAccessKeyEntity::table_name())
            .where(predicate)
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (!rows.empty()) {
            service::common::fail(19003, "调用配置名称已存在", 409);
        }
    }

    template <typename Context>
    ruvia::task<void> ensureWebhookNameAvailable(Context& c, std::string_view accessKeyId, std::string_view name, std::optional<std::string> except) {
        ruvia::db_query query(c.pool());
        auto predicate = andAll(
            query,
            query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, uuid(query, accessKeyId)),
            query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"name">()), ruvia::db_binary_operator::equal, query.value(name)),
            query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))
        );
        if (except) {
            predicate = query.binary(
                predicate,
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">()), ruvia::db_binary_operator::not_equal, uuid(query, *except))
            );
        }
        query.select(service::device::DeviceAccessService::integer(query, 1))
            .from(service::open_access::entities::OpenWebhookEntity::table_name())
            .where(predicate)
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (!rows.empty()) {
            service::common::fail(19003, "同一调用配置下的 Webhook 名称已存在", 409);
        }
    }

    template <typename Context>
    ruvia::task<KeyState> requireKey(Context& c, std::string_view id) {
        ruvia::db_query query(c.pool());
        query.select({ query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"name">()), text(query, query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"status">())), text(query, query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"scopes">())), query.call("iot_utc_timestamp", { query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"expires_at">()) }), query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"remark">()) })
            .from(service::open_access::entities::OpenAccessKeyEntity::table_name())
            .where(andAll(query, query.binary(query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"id">()), ruvia::db_binary_operator::equal, uuid(query, id)), query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::OpenAccessKeyEntity::column_name<"deleted_at">()))))
            .limit(1);
        const auto rows = co_await c.db().query(query);
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
        devicesQuery
            .select(text(devicesQuery, devicesQuery.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">())))
            .from(service::open_access::entities::OpenAccessKeyDeviceEntity::table_name())
            .where(devicesQuery.binary(devicesQuery.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"access_key_id">()), ruvia::db_binary_operator::equal, uuid(devicesQuery, id)))
            .order_by(devicesQuery.column(service::open_access::entities::OpenAccessKeyDeviceEntity::column_name<"device_id">()));
        const auto devices = co_await c.db().query(devicesQuery);
        for (const auto& device : devices) {
            state.deviceIds.emplace_back(device[0].value().value_or(std::string_view{}));
        }
        co_return state;
    }

    template <typename Context>
    ruvia::task<WebhookState> requireWebhook(Context& c, std::string_view id) {
        ruvia::db_query query(c.pool());
        query.select({ text(query, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"access_key_id">())), query.column(service::open_access::entities::OpenWebhookEntity::column_name<"name">()), query.column(service::open_access::entities::OpenWebhookEntity::column_name<"url">()), text(query, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"status">())), query.column(service::open_access::entities::OpenWebhookEntity::column_name<"timeout_seconds">()), query.case_when({ { query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"skip_tls_verify">()), ruvia::db_binary_operator::equal, service::device::DeviceAccessService::boolean(query, true)), text(query, "1") } }, text(query, "0")), text(query, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"headers">())), text(query, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"event_types">())), query.column(service::open_access::entities::OpenWebhookEntity::column_name<"secret">()) })
            .from(service::open_access::entities::OpenWebhookEntity::table_name())
            .where(andAll(query, query.binary(query.column(service::open_access::entities::OpenWebhookEntity::column_name<"id">()), ruvia::db_binary_operator::equal, uuid(query, id)), query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::OpenWebhookEntity::column_name<"deleted_at">()))))
            .limit(1);
        const auto rows = co_await c.db().query(query);
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
        query.select(service::device::DeviceAccessService::integer(query, 1))
            .from(service::open_access::entities::DeviceEntity::table_name())
            .where(andAll(query, query.binary(query.column(service::open_access::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, uuid(query, deviceId)), query.unary(ruvia::db_unary_operator::is_null, query.column(service::open_access::entities::DeviceEntity::column_name<"deleted_at">()))))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(19001, "设备不存在", 404);
        }
    }

    static ruvia::db_query historyQuery(std::pmr::memory_resource* resource, std::string_view deviceId, std::string_view start, std::string_view end, std::int64_t pageSize, std::int64_t offset, std::int64_t page) {
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
        const auto deviceParams = deviceRef.column(service::open_access::entities::DeviceEntity::column_name<"protocol_params">(), "device");
        deviceRef
            .select({ deviceRef.column(service::open_access::entities::DeviceEntity::column_name<"id">(), "device"), deviceRef.alias(service::device::DeviceAccessService::jsonText(deviceRef, deviceParams, "device_code"), "code"), deviceRef.column(service::open_access::entities::DeviceEntity::column_name<"name">(), "device") })
            .from(service::open_access::entities::DeviceEntity::table_name(), "device")
            .where(andAll(deviceRef, deviceRef.binary(deviceRef.column(service::open_access::entities::DeviceEntity::column_name<"id">(), "device"), ruvia::db_binary_operator::equal, uuid(deviceRef, deviceId)), deviceRef.unary(ruvia::db_unary_operator::is_null, deviceRef.column(service::open_access::entities::DeviceEntity::column_name<"deleted_at">(), "device"))));

        ruvia::db_query counted(resource);
        const auto countedData = counted.column(service::open_access::entities::DeviceDataEntity::column_name<"data">(), "record");
        counted
            .select(counted.aggregate("count", { counted.star() }))
            .from(service::open_access::entities::DeviceDataEntity::table_name(), "record")
            .where(andAll(counted, counted.binary(counted.column(service::open_access::entities::DeviceDataEntity::column_name<"device_id">(), "record"), ruvia::db_binary_operator::equal, uuid(counted, deviceId)), counted.binary(counted.column(service::open_access::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::greater_equal, timestamp(counted, start)), counted.binary(counted.column(service::open_access::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::less_equal, timestamp(counted, end)), counted.binary(counted.call("jsonb_typeof", { service::device::DeviceAccessService::jsonValue(counted, countedData, "values") }), ruvia::db_binary_operator::equal, text(counted, "object"))));

        ruvia::db_query filtered(resource);
        const auto filteredData = filtered.column(service::open_access::entities::DeviceDataEntity::column_name<"data">(), "record");
        filtered
            .select({ filtered.column(service::open_access::entities::DeviceDataEntity::column_name<"id">(), "record"), filtered.column(service::open_access::entities::DeviceDataEntity::column_name<"report_time">(), "record"), filteredData })
            .from(service::open_access::entities::DeviceDataEntity::table_name(), "record")
            .where(andAll(filtered, filtered.binary(filtered.column(service::open_access::entities::DeviceDataEntity::column_name<"device_id">(), "record"), ruvia::db_binary_operator::equal, uuid(filtered, deviceId)), filtered.binary(filtered.column(service::open_access::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::greater_equal, timestamp(filtered, start)), filtered.binary(filtered.column(service::open_access::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::less_equal, timestamp(filtered, end)), filtered.binary(filtered.call("jsonb_typeof", { service::device::DeviceAccessService::jsonValue(filtered, filteredData, "values") }), ruvia::db_binary_operator::equal, text(filtered, "object"))))
            .order_by(filtered.column(service::open_access::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_order_direction::desc)
            .add_order_by(filtered.column(service::open_access::entities::DeviceDataEntity::column_name<"id">(), "record"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>(offset));

        ruvia::db_query normalizedValues(resource);
        const auto normalizedPoint = normalizedValues.column("value", "point");
        const auto normalizedPointValue = service::device::DeviceAccessService::jsonValue(
            normalizedValues,
            normalizedPoint,
            "value"
        );
        const auto pointObject = andAll(
            normalizedValues,
            normalizedValues.binary(normalizedValues.call("jsonb_typeof", { normalizedPoint }), ruvia::db_binary_operator::equal, text(normalizedValues, "object")),
            normalizedValues.binary(normalizedValues.call("jsonb_typeof", { normalizedPointValue }), ruvia::db_binary_operator::equal, text(normalizedValues, "boolean"))
        );
        const auto pointBoolean = normalizedValues.cast(
            service::device::DeviceAccessService::jsonText(normalizedValues, normalizedPoint, "value"),
            ruvia::db_data_type::boolean
        );
        const auto pointNumber = normalizedValues.case_when(
            { { pointBoolean,
                normalizedValues.cast(normalizedValues.value(1), ruvia::db_data_type::integer) } },
            normalizedValues.cast(normalizedValues.value(0), ruvia::db_data_type::integer)
        );
        const auto pointPath = normalizedValues.cast(
            normalizedValues.array({ textKey(normalizedValues, "value") }),
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
        normalizedValues
            .select(normalizedValues.aggregate(
                "jsonb_object_agg",
                { normalizedValues.column("key", "point"), normalizedPointValueExpr }
            ))
            .from_function(
                normalizedValues.call(
                    "jsonb_each",
                    { normalizedValues.coalesce(
                        { service::device::DeviceAccessService::jsonValue(
                              normalizedValues,
                              normalizedValues.column("data", "filtered"),
                              "values"
                          ),
                          emptyObject(normalizedValues) }
                    ) }
                ),
                "point",
                { .lateral_ = true,
                  .columns_ = { { .name_ = "key" }, { .name_ = "value" } } }
            );
        ruvia::db_query normalized(resource);
        normalized
            .select({ normalized.star("filtered"), normalized.alias(normalized.coalesce({ normalized.subquery(normalizedValues), emptyObject(normalized) }), "normalized_values") })
            .from(filtered, "filtered");

        ruvia::db_query items(resource);
        const auto point = items.column("value", "point");
        const auto pointValue = service::device::DeviceAccessService::jsonValue(
            items,
            point,
            "value"
        );
        const auto normalizedData = items.column("data", "normalized");
        const auto itemPointBoolean = items.cast(
            service::device::DeviceAccessService::jsonText(items, point, "value"),
            ruvia::db_data_type::boolean
        );
        const auto itemPointNumber = items.case_when(
            { { itemPointBoolean, items.cast(items.value(1), ruvia::db_data_type::integer) } },
            items.cast(items.value(0), ruvia::db_data_type::integer)
        );
        const auto itemPoint = items.call(
            "jsonb_build_object",
            { textKey(items, "id"), items.column("key", "point"), textKey(items, "name"), items.coalesce({ service::device::DeviceAccessService::jsonText(items, point, "name"), items.column("key", "point") }), textKey(items, "value"), items.case_when({ { items.binary(items.call("jsonb_typeof", { pointValue }), ruvia::db_binary_operator::equal, text(items, "boolean")), items.call("to_jsonb", { itemPointNumber }) } }, pointValue), textKey(items, "unit"), items.coalesce({ service::device::DeviceAccessService::jsonText(items, point, "unit"), text(items, "") }), textKey(items, "time"), items.call("iot_utc_timestamp", { items.column("report_time", "normalized") }) }
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
            { textKey(items, "device"),
              items.call("jsonb_build_object", { textKey(items, "id"), items.cast(items.column("id", "device_ref"), ruvia::db_data_type::uuid), textKey(items, "code"), items.column("code", "device_ref"), textKey(items, "name"), items.column("name", "device_ref") }),
              textKey(items, "points"),
              points }
        );
        items
            .select({ items.column("report_time", "normalized"), items.column("id", "normalized"), items.alias(item, "item") })
            .from(normalized, "normalized")
            .join(ruvia::db_join_type::cross, "device_ref", {}, "device_ref")
            .join_function(
                ruvia::db_join_type::left,
                items.call("jsonb_each", { service::device::DeviceAccessService::jsonValue(items, normalizedData, "values") }),
                items.binary(
                    items.coalesce({ service::device::DeviceAccessService::jsonText(items, point, "type"), text(items, "") }),
                    ruvia::db_binary_operator::not_equal,
                    text(items, "JPEG")
                ),
                "point",
                { .lateral_ = true,
                  .columns_ = { { .name_ = "key" }, { .name_ = "value" } } }
            )
            .group_by({ items.column("report_time", "normalized"), items.column("id", "normalized"), items.column("id", "device_ref"), items.column("code", "device_ref"), items.column("name", "device_ref") });

        ruvia::db_query query(resource);
        query.with("device_ref", deviceRef)
            .with("counted", counted)
            .with("filtered", filtered)
            .with("normalized", normalized)
            .with("items", items);
        const auto total = query.coalesce(
            { query.subquery(counted), query.cast(query.value(0), ruvia::db_data_type::big_int) }
        );
        const std::array<ruvia::db_order_term, 2> historyOrder{ { ruvia::db_order_term{ query.column("report_time", "items"),
                                                                                    ruvia::db_order_direction::desc,
                                                                                    ruvia::db_nulls_order::default_value },
                                                                ruvia::db_order_term{ query.column("id", "items"), ruvia::db_order_direction::desc, ruvia::db_nulls_order::default_value } } };
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
        query.select(query.cast(
                         query.call("jsonb_build_object", { textKey(query, "list"), list, textKey(query, "total"), total, textKey(query, "page"), query.cast(query.value(page), ruvia::db_data_type::big_int), textKey(query, "pageSize"), query.cast(query.value(pageSize), ruvia::db_data_type::big_int), textKey(query, "totalPages"), totalPages }),
                         ruvia::db_data_type::text
                     ))
            .from("items");
        return query;
    }
};

inline AccessService& accessService() {
    return AccessService::instance();
}

} // namespace service::access
