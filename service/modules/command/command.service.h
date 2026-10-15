#pragma once

#include <tuple>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/timer.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/middleware/rpc.h"
#include "service/modules/command/command.entity.h"
#include "service/modules/command/command.types.h"
#include "service/modules/device/device.service.h"
#include "service/modules/device/device.types.h"
#include "service/utils/json.h"

namespace service::command {

class CommandService final {
  public:
    explicit CommandService(service::device::DeviceAccessService& accessService)
        : accessService_(accessService) {}

    template <typename Context>
    ruvia::task<service::device::DeviceCommandCreateDto>
    create(Context& context, std::string_view deviceId, const SubmitCommandBody& body) {
        const auto access = co_await accessService_.require(
            context,
            deviceId,
            service::device::DeviceAccessLevel::operate,
            context.userId
        );
        const auto deviceRows = co_await context.db().query(
            deviceRemoteControlQuery(context.pool(), deviceId)
        );
        if (deviceRows.empty()) {
            service::common::fail(18001, "设备不存在", 404);
        }
        const auto capabilities = service::device::DeviceAccessService::capabilities(
            access.actor,
            access.level,
            deviceRows.front()[0].value().value_or(std::string_view{}) == "t"
        );
        if (!capabilities.canCommand) {
            service::common::fail(18005, "设备未开启远程控制或当前账号无下发权限", 403);
        }

        co_return co_await enqueueDevice(context, deviceId, body, access.actor.userId);
    }

    template <typename Context>
    ruvia::task<service::device::DeviceCommandCreateDto>
    createExternal(Context& context, std::string_view deviceId, const service::device::DeviceCommandBody& body, std::string_view accessKeyId) {
        const auto deviceRows = co_await context.db().query(
            deviceRemoteControlQuery(context.pool(), deviceId)
        );
        if (deviceRows.empty()) {
            service::common::fail(18001, "设备不存在", 404);
        }
        if (deviceRows.front()[0].value().value_or(std::string_view{}) != "t") {
            service::common::fail(18005, "设备未开启远程控制", 403);
        }
        co_return co_await enqueueDevice(context, deviceId, body, "access-key:" + std::string(accessKeyId));
    }

  private:
    static ruvia::db_query deviceRemoteControlQuery(std::pmr::memory_resource* resource, std::string_view deviceId) {
        ruvia::db_query query(resource);
        const auto protocolParams = query.column(std::tuple_element_t<service::command::entities::DeviceEntity::column_index<"protocol_params">(), service::command::entities::DeviceEntity::columns_type>::name.view());
        const auto remoteControlKey = query.cast(
            query.value("remote_control"),
            ruvia::db_data_type::text
        );
        const auto hasRemoteControl = query.binary(
            protocolParams,
            ruvia::db_binary_operator::json_has_key,
            remoteControlKey
        );
        const auto remoteControl = query.binary(
            protocolParams,
            ruvia::db_binary_operator::json_get_text,
            remoteControlKey
        );
        const auto normalizedRemoteControl = query.call(
            "lower",
            { query.coalesce({ remoteControl, query.value("") }) }
        );
        const auto enabled = query.case_when(
            { { query.binary(normalizedRemoteControl, ruvia::db_binary_operator::equal, query.value("true")),
                query.cast(query.value(true), ruvia::db_data_type::boolean) },
              { query.binary(normalizedRemoteControl, ruvia::db_binary_operator::equal, query.value("t")),
                query.cast(query.value(true), ruvia::db_data_type::boolean) },
              { query.binary(normalizedRemoteControl, ruvia::db_binary_operator::equal, query.value("1")),
                query.cast(query.value(true), ruvia::db_data_type::boolean) },
              { query.binary(normalizedRemoteControl, ruvia::db_binary_operator::equal, query.value("yes")),
                query.cast(query.value(true), ruvia::db_data_type::boolean) },
              { query.binary(normalizedRemoteControl, ruvia::db_binary_operator::equal, query.value("y")),
                query.cast(query.value(true), ruvia::db_data_type::boolean) },
              { query.binary(normalizedRemoteControl, ruvia::db_binary_operator::equal, query.value("on")),
                query.cast(query.value(true), ruvia::db_data_type::boolean) } },
            query.cast(query.value(false), ruvia::db_data_type::boolean)
        );
        query
            .select(query.case_when({ { hasRemoteControl, enabled } }, query.cast(query.value(true), ruvia::db_data_type::boolean)))
            .from(service::command::entities::DeviceEntity::table_name())
            .where(query.binary(query.column(std::tuple_element_t<service::command::entities::DeviceEntity::column_index<"id">(), service::command::entities::DeviceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, query.cast(query.value(deviceId), ruvia::db_data_type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(std::tuple_element_t<service::command::entities::DeviceEntity::column_index<"deleted_at">(), service::command::entities::DeviceEntity::columns_type>::name.view())))
            .limit(1);
        return query;
    }

    template <typename Context, typename Body>
    ruvia::task<service::device::DeviceCommandCreateDto>
    enqueueDevice(Context& context, std::string_view deviceId, const Body& body, std::string submittedBy) {
        const auto& keyField = body.template get<"idempotencyKey">();
        if (!keyField || !service::common::isUuid(keyField->view())) {
            service::common::fail(18010, "idempotency_key 必须是 UUID", 400);
        }
        const std::string key(keyField->view());
        const auto& mqttMessage = body.template get<"mqttMessageId">();
        if (mqttMessage && !service::common::isUuid(mqttMessage->view()))
            service::common::fail(18010, "控制消息 ID 必须是 UUID", 400);
        const std::string mqttSelection = mqttMessage
            ? ",\"mqttMessageId\":" + service::utils::jsonQuoted(mqttMessage->view()) : "";
        const auto elements = normalize(body);
        std::string payload = "[";
        for (const auto& [elementId, value] : elements) {
            if (payload.size() > 1) {
                payload += ',';
            }
            payload += "[" + service::utils::jsonQuoted(elementId) + "," +
                service::utils::jsonQuoted(value) + "]";
        }
        payload += ']';
        const auto requestPayload = mqttMessage
            ? "{\"elements\":" + payload + mqttSelection + "}" : payload;
        auto transaction = co_await context.db().begin_transaction();
        const std::string lockKey = submittedBy + ":" + key;
        ruvia::db_query advisory(context.pool());
        advisory.select(advisory.call(
            "pg_advisory_xact_lock",
            { advisory.call("hashtextextended", { advisory.value(lockKey), advisory.value(std::int64_t{ 0 }) }) }
        ));
        (void)co_await transaction.query(advisory);

        ruvia::db_query priorQuery(context.pool());
        const auto priorDevice = priorQuery.binary(
            priorQuery.column(std::tuple_element_t<service::command::entities::CommandRequestEntity::column_index<"device_id">(), service::command::entities::CommandRequestEntity::columns_type>::name.view()),
            ruvia::db_binary_operator::equal,
            priorQuery.cast(priorQuery.value(deviceId), ruvia::db_data_type::uuid)
        );
        const auto priorPayload = priorQuery.binary(
            priorQuery.column(std::tuple_element_t<service::command::entities::CommandRequestEntity::column_index<"payload">(), service::command::entities::CommandRequestEntity::columns_type>::name.view()),
            ruvia::db_binary_operator::equal,
            priorQuery.cast(priorQuery.value(requestPayload), ruvia::db_data_type::jsonb)
        );
        priorQuery
            .select({ priorQuery.cast(priorQuery.column(std::tuple_element_t<service::command::entities::CommandRequestEntity::column_index<"id">(), service::command::entities::CommandRequestEntity::columns_type>::name.view()), ruvia::db_data_type::text), priorQuery.binary(priorDevice, ruvia::db_binary_operator::and_value, priorPayload) })
            .from(service::command::entities::CommandRequestEntity::table_name())
            .where(priorQuery.binary(
                priorQuery.binary(priorQuery.column(std::tuple_element_t<service::command::entities::CommandRequestEntity::column_index<"actor">(), service::command::entities::CommandRequestEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, priorQuery.value(submittedBy)),
                ruvia::db_binary_operator::and_value,
                priorQuery.binary(priorQuery.column(std::tuple_element_t<service::command::entities::CommandRequestEntity::column_index<"idempotency_key">(), service::command::entities::CommandRequestEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, priorQuery.cast(priorQuery.value(key), ruvia::db_data_type::uuid))
            ));
        const auto prior = co_await transaction.query(priorQuery);
        if (!prior.empty()) {
            if (prior.front()[1].value().value_or(std::string_view{}) != "t") {
                common::fail(18014, "幂等键已用于不同的指令请求", 409);
            }
            ruvia::db_query commandQuery(context.pool());
            commandQuery
                .select(commandQuery.cast(commandQuery.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"id">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()), ruvia::db_data_type::text))
                .from(service::command::entities::CommandOperationEntity::table_name())
                .where(commandQuery.binary(
                    commandQuery.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"request_id">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    commandQuery.cast(
                        commandQuery.value(prior.front()[0].value().value_or(std::string_view{})),
                        ruvia::db_data_type::uuid
                    )
                ))
                .order_by(commandQuery.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"ordinal">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()));
            const auto commands = co_await transaction.query(commandQuery);
            ruvia::boxed_array<ruvia::string> ids(ruvia::model_options{ .resource_ = context.arena() });
            for (const auto& row : commands) {
                ids.emplace(row[0].value().value_or(std::string_view{}), ruvia::model_options{ .resource_ = context.arena() });
            }
            co_await transaction.commit();
            service::device::DeviceCommandCreateDto result(ruvia::model_options{ .resource_ = context.arena() });
            result.template set<"commandIds">(std::move(ids)).template set<"status">("ACCEPTED");
            co_return result;
        }
        const auto requestId = context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        ruvia::db_query requestQuery(context.pool());
        requestQuery
            .insert_into(service::command::entities::CommandRequestEntity::table_name(), { "id", "actor", "idempotency_key", "device_id", "payload" })
            .values({ requestQuery.cast(requestQuery.value(requestId), ruvia::db_data_type::uuid), requestQuery.value(submittedBy), requestQuery.cast(requestQuery.value(key), ruvia::db_data_type::uuid), requestQuery.cast(requestQuery.value(deviceId), ruvia::db_data_type::uuid), requestQuery.cast(requestQuery.value(requestPayload), ruvia::db_data_type::jsonb) });
        (void)co_await transaction.execute(requestQuery);

        ruvia::db_query deviceLock(context.pool());
        deviceLock
            .select(deviceLock.column(std::tuple_element_t<service::command::entities::DeviceEntity::column_index<"id">(), service::command::entities::DeviceEntity::columns_type>::name.view()))
            .from(service::command::entities::DeviceEntity::table_name())
            .where(deviceLock.binary(deviceLock.column(std::tuple_element_t<service::command::entities::DeviceEntity::column_index<"id">(), service::command::entities::DeviceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, deviceLock.cast(deviceLock.value(deviceId), ruvia::db_data_type::uuid)))
            .lock({ .mode_ = ruvia::db_row_lock::share });
        (void)co_await transaction.query(deviceLock);
        const auto prepared = co_await service::rpc::call(context, "command", "prepare", "{\"deviceId\":" + service::utils::jsonQuoted(deviceId) + mqttSelection + ",\"elements\":" + payload + "}");
        auto result = co_await appendPrepared(context, transaction, requestId, deviceId, submittedBy, prepared);
        co_await transaction.commit();
        co_return result;
    }

    template <typename Context>
    static ruvia::task<service::device::DeviceCommandCreateDto> appendPrepared(
        Context& context,
        ruvia::db_transaction& transaction,
        std::string_view requestId,
        std::string_view deviceId,
        std::string_view submittedBy,
        std::string_view preparedJson
    ) {
        const auto batch = ruvia::from_json<PreparedBatch>(preparedJson, { .resource_ = context.arena() });
        if (!batch || !batch->template get<"queue">() || !batch->template get<"kind">() ||
            !batch->template get<"maximum">() || !batch->template get<"nodeId">() || !batch->template get<"commands">() ||
            batch->template get<"commands">()->empty() || batch->template get<"commands">()->size() > 256) {
            common::fail(10004, "Invalid prepared command batch", 502);
        }
        const auto kind = batch->template get<"kind">()->view();
        const auto maximum = static_cast<std::int64_t>(*batch->template get<"maximum">());
        if ((kind != "list" && kind != "stream") || maximum <= 0 || maximum > 10000) {
            common::fail(10004, "Invalid prepared command queue", 502);
        }
        ruvia::boxed_array<ruvia::string> commandIds(ruvia::model_options{ .resource_ = context.arena() });
        std::int64_t ordinal = 0;
        for (const auto& command : *batch->template get<"commands">()) {
            if (!command.template get<"id">() || !common::isUuid(command.template get<"id">()->view()) ||
                !command.template get<"deviceId">() || command.template get<"deviceId">()->view() != deviceId ||
                !command.template get<"deviceCode">() || !command.template get<"protocol">() ||
                !command.template get<"elements">() || !command.template get<"payload">()) {
                common::fail(10004, "Invalid prepared command", 502);
            }
            std::string elements = "[";
            for (const auto& element : *command.template get<"elements">()) {
                if (!element.template get<"elementId">() || !element.template get<"value">()) {
                    common::fail(10004, "Invalid prepared command element", 502);
                }
                if (elements.size() > 1) {
                    elements += ',';
                }
                elements += "{\"elementId\":" + service::utils::jsonQuoted(element.template get<"elementId">()->view()) +
                    ",\"value\":" + service::utils::jsonQuoted(element.template get<"value">()->view()) + "}";
            }
            elements += ']';
            std::string payload = "[";
            for (const auto& value : *command.template get<"payload">()) {
                if (payload.size() > 1) {
                    payload += ',';
                }
                payload += service::utils::jsonQuoted(value.view());
            }
            payload += ']';
            const auto id = command.template get<"id">()->view();
            const auto operationOrdinal = ordinal++;
            ruvia::db_query operationSource(context.pool());
            operationSource
                .select({ operationSource.cast(operationSource.value(id), ruvia::db_data_type::uuid), operationSource.cast(operationSource.value(requestId), ruvia::db_data_type::uuid), operationSource.value(static_cast<std::int32_t>(operationOrdinal)), operationSource.cast(operationSource.value(deviceId), ruvia::db_data_type::uuid), operationSource.value(command.template get<"deviceCode">()->view()), operationSource.value(command.template get<"protocol">()->view()), operationSource.value("ACCEPTED"), operationSource.cast(operationSource.value(elements), ruvia::db_data_type::jsonb), operationSource.column(std::tuple_element_t<service::command::entities::DeviceEntity::column_index<"protocol_config_id">(), service::command::entities::DeviceEntity::columns_type>::name.view()) })
                .from(service::command::entities::DeviceEntity::table_name())
                .where(operationSource.binary(
                    operationSource.column(std::tuple_element_t<service::command::entities::DeviceEntity::column_index<"id">(), service::command::entities::DeviceEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    operationSource.cast(operationSource.value(deviceId), ruvia::db_data_type::uuid)
                ));
            ruvia::db_query operationQuery(context.pool());
            operationQuery
                .insert_into(service::command::entities::CommandOperationEntity::table_name(), { "id", "request_id", "ordinal", "device_id", "device_code", "protocol", "status", "elements", "model_id" })
                .insert_from(operationSource);
            (void)co_await transaction.execute(operationQuery);

            ruvia::db_query attemptQuery(context.pool());
            attemptQuery
                .insert_into(service::command::entities::CommandAttemptEntity::table_name(), { "operation_id", "queue_key", "queue_kind", "payload", "submitted_by", "node_id", "max_length" })
                .values({ attemptQuery.cast(attemptQuery.value(id), ruvia::db_data_type::uuid), attemptQuery.value(batch->template get<"queue">()->view()), attemptQuery.value(kind), attemptQuery.cast(attemptQuery.value(payload), ruvia::db_data_type::jsonb), attemptQuery.value(submittedBy), attemptQuery.value(batch->template get<"nodeId">()->view()), attemptQuery.value(static_cast<std::int32_t>(maximum)) });
            (void)co_await transaction.execute(attemptQuery);
            const auto eventId = context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
            ruvia::db_query eventSource(context.pool());
            const auto jsonKey = [&](std::string_view key) {
                return eventSource.cast(eventSource.value(key), ruvia::db_data_type::text);
            };
            eventSource
                .select({ eventSource.cast(eventSource.value(eventId), ruvia::db_data_type::uuid), eventSource.value("device.command.accepted"), eventSource.value("command"), eventSource.cast(eventSource.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"device_id">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()), ruvia::db_data_type::text), eventSource.value("updated"), eventSource.value(std::int32_t{ 2 }), eventSource.call("jsonb_build_object", { jsonKey("device_code"), eventSource.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"device_code">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()), jsonKey("data"), eventSource.call("jsonb_build_object", { jsonKey("commandId"), eventSource.cast(eventSource.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"id">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()), ruvia::db_data_type::text), jsonKey("status"), eventSource.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"status">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()), jsonKey("reason"), eventSource.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"reason">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()), jsonKey("elements"), eventSource.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"elements">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()), jsonKey("actualValues"), eventSource.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"actual_values">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()) }) }) })
                .from(service::command::entities::CommandOperationEntity::table_name())
                .where(eventSource.binary(
                    eventSource.column(std::tuple_element_t<service::command::entities::CommandOperationEntity::column_index<"id">(), service::command::entities::CommandOperationEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    eventSource.cast(eventSource.value(id), ruvia::db_data_type::uuid)
                ));
            ruvia::db_query eventQuery(context.pool());
            eventQuery
                .insert_into(service::command::entities::OutboxEventEntity::table_name(), { "id", "event_type", "aggregate_type", "aggregate_id", "action", "schema_version", "payload" })
                .insert_from(eventSource);
            (void)co_await transaction.execute(eventQuery);
            commandIds.emplace(id, ruvia::model_options{ .resource_ = context.arena() });
        }
        service::device::DeviceCommandCreateDto result(ruvia::model_options{ .resource_ = context.arena() });
        result.template set<"commandIds">(std::move(commandIds)).template set<"status">("ACCEPTED");
        co_return result;
    }

    template <typename Body>
    static std::vector<std::pair<std::string, std::string>> normalize(const Body& body) {
        const auto* elements = [&]() -> const ruvia::array<service::device::DeviceCommandElementBody>* {
            if constexpr (std::is_same_v<Body, SubmitCommandBody>) {
                return &body.template get<"elements">();
            } else {
                const auto& values = body.template get<"elements">();
                return values ? &*values : nullptr;
            }
        }();
        if (!elements || elements->empty() || elements->size() > 256) {
            service::common::fail(18010, "下发要素数量必须在 1 - 256 之间", 400);
        }
        std::vector<std::pair<std::string, std::string>> result;
        std::set<std::string, std::less<>> seenElementIds;
        result.reserve(elements->size());
        for (const auto& element : *elements) {
            if (!element.template get<"elementId">() || !element.template get<"value">()) {
                service::common::fail(18010, "下发要素参数不完整", 400);
            }
            const auto id = element.template get<"elementId">()->view();
            const auto value = element.template get<"value">()->view();
            if (!service::common::isUuid(id)) {
                service::common::fail(18010, "下发要素 ID 必须是 UUID", 400);
            }
            if (!seenElementIds.emplace(id).second) {
                service::common::fail(18010, "下发要素不能重复", 400);
            }
            if (value.empty() || value.size() > 4096) {
                service::common::fail(18010, "下发要素值长度必须在 1 - 4096 之间", 400);
            }
            result.push_back({ std::string(id), std::string(value) });
        }
        return result;
    }

    service::device::DeviceAccessService& accessService_;
};

inline CommandService& commandService() {
    static thread_local CommandService service(service::device::deviceAccessService());
    return service;
}

} // namespace service::command
