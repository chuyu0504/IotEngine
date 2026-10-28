#pragma once
#include <algorithm>
#include <array>
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
#include <tuple>
#include <utility>
#include <vector>

#include <ruvia/core/timer.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/modules/command/command.entity.h"
#include "service/modules/command/command.types.h"
#include "service/modules/device/device.entity.h"
#include "service/modules/device/device.service.h"
#include "service/modules/device/device.types.h"
#include "service/modules/protocol/protocol.types.h"
#include "service/modules/protocol/protocol.entity.h"
#include "service/modules/system/outbox/outbox.entity.h"
#include "service/utils/json.h"
#include "service/utils/payload.h"
#include "service/utils/rpc_client.h"
#include "service/utils/uuid.h"

namespace service::command {

namespace db = service::common::database;

class CommandService final {
  public:
    explicit CommandService(service::device::DeviceAccessService& accessService)
        : accessService_(accessService) {}

    static CommandService& instance() {
        static thread_local CommandService service(service::device::DeviceAccessService::instance());
        return service;
    }

    template <typename Context>
    ruvia::task<service::device::DeviceCommandCreateDto>
    create(Context& context, std::string_view deviceId, const SubmitCommandBody& body) {
        const auto access = co_await accessService_.require(
            context,
            deviceId,
            service::device::DeviceAccessLevel::operate,
            context.userId
        );
        const auto deviceRows = co_await deviceRemoteControlQuery(context.db(), context.pool(), deviceId).get_raw_many();
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
        const auto deviceRows = co_await deviceRemoteControlQuery(context.db(), context.pool(), deviceId).get_raw_many();
        if (deviceRows.empty()) {
            service::common::fail(18001, "设备不存在", 404);
        }
        if (deviceRows.front()[0].value().value_or(std::string_view{}) != "t") {
            service::common::fail(18005, "设备未开启远程控制", 403);
        }
        co_return co_await enqueueDevice(context, deviceId, body, "access-key:" + std::string(accessKeyId));
    }

    // 设备事件流 commands 频道：按 ID 读取命令状态，逐条校验设备操作权限。
    template <typename Context>
    static ruvia::task<service::device::DeviceCommandStatusesDto>
    statuses(Context& context, std::string_view ids) {
        co_await service::auth::AuthService::requirePermission(context, context.userId, "iot:device:command");
        ruvia::boxed_array<service::device::DeviceCommandStatusDto> statuses(
            ruvia::model_options{ .resource_ = context.arena() }
        );
        bool complete = true;
        std::size_t offset = 0, count = 0;
        for (;;) {
            const auto end = ids.find(',', offset);
            const auto id = ids.substr(offset, end == std::string_view::npos ? end : end - offset);
            if (!service::common::isUuid(id) || ++count > 256) {
                service::common::fail(18012, "Invalid command ID list", 400);
            }
            auto result = co_await readCommandStatus(context, id);
            complete = complete && commandTerminalState(result.template get<"status">()->view());
            statuses.emplace_back(std::move(result));
            if (end == std::string_view::npos) {
                break;
            }
            offset = end + 1;
        }
        service::device::DeviceCommandStatusesDto result(ruvia::model_options{ .resource_ = context.arena() });
        result.template set<"complete">(complete).template set<"statuses">(std::move(statuses));
        co_return result;
    }

  private:
    template <typename Context>
    static ruvia::task<service::device::DeviceCommandStatusDto> readCommandStatus(Context& context, std::string_view commandId) {
        const auto fields =
            co_await loadCommandStatus(context, commandId);
        if (fields.empty()) {
            service::common::fail(18012, "下发记录不存在", 404);
        }
        const auto deviceId = commandField(fields, "device_id");
        if (deviceId.empty()) {
            service::common::fail(18012, "下发状态数据无效", 500);
        }
        (void)co_await service::device::DeviceAccessService::instance().require(
            context,
            deviceId,
            service::device::DeviceAccessLevel::operate,
            context.userId
        );

        service::device::DeviceCommandStatusDto result(ruvia::model_options{ .resource_ = context.arena() });
        fillCommandStatus(result, commandId, fields);
        co_return result;
    }

    static std::string commandActualValueField(std::size_t index, std::string_view name) {
        return "actual_value_" + std::to_string(index) + "_" + std::string(name);
    }

    static void fillCommandStatus(service::device::DeviceCommandStatusDto& result, std::string_view commandId, const std::vector<message::StreamField>& fields) {
        result.template set<"commandId">(commandId)
            .template set<"deviceId">(commandField(fields, "device_id"))
            .template set<"deviceCode">(commandField(fields, "device_code"))
            .template set<"protocol">(commandField(fields, "protocol"))
            .template set<"status">(commandField(fields, "status"));
        const auto reason = commandField(fields, "reason");
        if (!reason.empty()) {
            result.template set<"reason">(reason);
        }
        const auto createdAt = commandInteger(fields, "created_at_ms");
        if (createdAt != 0) {
            result.template set<"createdAtMs">(createdAt);
        }
        const auto completedAt = commandInteger(fields, "completed_at_ms");
        if (completedAt != 0) {
            result.template set<"completedAtMs">(completedAt);
        }
        const auto actualCount =
            std::clamp<std::int64_t>(commandInteger(fields, "actual_value_count"), 0, 8);
        if (actualCount != 0) {
            ruvia::boxed_array<service::device::DeviceCommandActualValueDto> actualValues(
                ruvia::model_options{ .resource_ = result.resource() }
            );
            for (std::int64_t index = 0; index < actualCount; ++index) {
                auto& actual = actualValues.emplace_back();
                actual.template set<"elementId">(
                          commandField(fields, commandActualValueField(index, "element_id"))
                )
                    .template set<"name">(commandField(fields, commandActualValueField(index, "name")))
                    .template set<"kind">(commandField(fields, commandActualValueField(index, "kind")))
                    .template set<"value">(commandField(fields, commandActualValueField(index, "value")))
                    .template set<"unit">(commandField(fields, commandActualValueField(index, "unit")));
            }
            result.template set<"actualValues">(std::move(actualValues));
        }
    }

    static std::string_view commandField(const std::vector<message::StreamField>& fields, std::string_view name) {
        for (const auto& value : fields) {
            if (value.name == name) {
                return value.value;
            }
        }
        return {};
    }

    static bool commandTerminalState(std::string_view state) {
        return state == "SUCCEEDED" || state == "REJECTED" || state == "UNKNOWN" ||
            state == "READBACK_MISMATCH" || state == "FAILED";
    }

    template <typename Context>
    static ruvia::task<std::vector<message::StreamField>> loadCommandStatus(Context& context, std::string_view id) {
        ruvia::db_query statusQuery(context.pool());
        const auto createdAtMs = statusQuery.cast(
            statusQuery.binary(statusQuery.extract(ruvia::db_date_part::epoch, statusQuery.column(service::command::entities::CommandOperationEntity::column_name<"created_at">())), ruvia::db_binary_operator::multiply, statusQuery.value(std::int64_t{ 1000 })),
            ruvia::db_data_type::big_int
        );
        const auto completedAtMs = statusQuery.cast(
            statusQuery.binary(statusQuery.extract(ruvia::db_date_part::epoch, statusQuery.column(service::command::entities::CommandOperationEntity::column_name<"completed_at">())), ruvia::db_binary_operator::multiply, statusQuery.value(std::int64_t{ 1000 })),
            ruvia::db_data_type::big_int
        );
        auto statusQueryBuilder = context.db().template repository<service::command::entities::CommandOperationEntity>().query_builder();
        statusQueryBuilder.select({ { "device_id", statusQuery.cast(statusQuery.column(service::command::entities::CommandOperationEntity::column_name<"device_id">()), ruvia::db_data_type::text) }, { "device_code", statusQuery.column(service::command::entities::CommandOperationEntity::column_name<"device_code">()) }, { "protocol", statusQuery.column(service::command::entities::CommandOperationEntity::column_name<"protocol">()) }, { "status", statusQuery.column(service::command::entities::CommandOperationEntity::column_name<"status">()) }, { "reason", statusQuery.column(service::command::entities::CommandOperationEntity::column_name<"reason">()) }, { "value5", statusQuery.cast(createdAtMs, ruvia::db_data_type::text) }, { "value6", statusQuery.coalesce({ statusQuery.cast(completedAtMs, ruvia::db_data_type::text), statusQuery.value("0") }) } }).where(statusQuery.binary(statusQuery.column(service::command::entities::CommandOperationEntity::column_name<"id">()), ruvia::db_binary_operator::equal, statusQuery.cast(statusQuery.value(id), ruvia::db_data_type::uuid)));
        const auto rows = co_await statusQueryBuilder.get_raw_many();
        std::vector<message::StreamField> fields;
        if (rows.empty()) {
            co_return fields;
        }
        const std::string_view names[]{ "device_id", "device_code", "protocol", "status", "reason", "created_at_ms", "completed_at_ms" };
        for (std::size_t index = 0; index < 7; ++index) {
            fields.push_back({ std::string(names[index]), std::string(rows.front()[index].value().value_or(std::string_view{})) });
        }
        ruvia::db_query actualQuery(context.pool());
        const auto jsonKey = [&](std::string_view key) {
            return actualQuery.cast(actualQuery.value(key), ruvia::db_data_type::text);
        };
        auto actualQueryBuilder = context.db().template repository<service::command::entities::CommandOperationEntity>().query_builder("operation");

        const std::array<ruvia::db_source_column, 2> sourceColumns1{ { { .name_ = "value" }, { .name_ = "idx" } } };
        actualQueryBuilder.select({ { "value", actualQuery.binary(actualQuery.column("value", "a"), ruvia::db_binary_operator::json_get_text, jsonKey("elementId")) }, { "value1", actualQuery.binary(actualQuery.column("value", "a"), ruvia::db_binary_operator::json_get_text, jsonKey("name")) }, { "value2", actualQuery.binary(actualQuery.column("value", "a"), ruvia::db_binary_operator::json_get_text, jsonKey("kind")) }, { "value3", actualQuery.binary(actualQuery.column("value", "a"), ruvia::db_binary_operator::json_get_text, jsonKey("value")) }, { "value4", actualQuery.binary(actualQuery.column("value", "a"), ruvia::db_binary_operator::json_get_text, jsonKey("unit")) } }).join_function(ruvia::db_join_type::cross, actualQuery.call("jsonb_array_elements", { actualQuery.column(service::command::entities::CommandOperationEntity::column_name<"actual_values">(), "operation") }), "a", {}, { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns1 }).where(actualQuery.binary(actualQuery.column(service::command::entities::CommandOperationEntity::column_name<"id">(), "operation"), ruvia::db_binary_operator::equal, actualQuery.cast(actualQuery.value(id), ruvia::db_data_type::uuid))).order_by(actualQuery.column("idx", "a"));
        const auto actual = co_await actualQueryBuilder.get_raw_many();
        fields.push_back({ "actual_value_count", std::to_string(actual.size()) });
        const std::string_view actualNames[]{ "element_id", "name", "kind", "value", "unit" };
        for (std::size_t index = 0; index < actual.size(); ++index) {
            for (std::size_t col = 0; col < 5; ++col) {
                fields.push_back({ "actual_value_" + std::to_string(index) + "_" + std::string(actualNames[col]), std::string(actual[index][col].value().value_or(std::string_view{})) });
            }
        }
        co_return fields;
    }

    static std::int64_t commandInteger(const std::vector<message::StreamField>& fields, std::string_view name) {
        const auto value = commandField(fields, name);
        std::int64_t result = 0;
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        return error == std::errc{} && end == value.data() + value.size() ? result : 0;
    }

  public:
    // 设备详情的可写命令目录：按设备模型中的可写要素与 MQTT 控制消息生成命令操作。
    template <typename Context>
    static ruvia::task<void>
    fillOperations(Context& c, const service::device::DeviceItems& items, std::optional<std::string_view> onlyDevice) {
        if (items.empty()) {
            co_return;
        }
        const auto makeWritable = [](ruvia::db_query& query,
                                     ruvia::db_expression value) {
            const auto normalized = query.call("lower", { query.coalesce({ value, query.value("") }) });
            return query.case_when(
                { { query.binary(normalized, ruvia::db_binary_operator::equal, query.value("true")),
                    db::boolean(query, true) },
                  { query.binary(normalized, ruvia::db_binary_operator::equal, query.value("t")),
                    db::boolean(query, true) },
                  { query.binary(normalized, ruvia::db_binary_operator::equal, query.value("1")),
                    db::boolean(query, true) },
                  { query.binary(normalized, ruvia::db_binary_operator::equal, query.value("yes")),
                    db::boolean(query, true) },
                  { query.binary(normalized, ruvia::db_binary_operator::equal, query.value("y")),
                    db::boolean(query, true) },
                  { query.binary(normalized, ruvia::db_binary_operator::equal, query.value("on")),
                    db::boolean(query, true) } },
                db::boolean(query, false)
            );
        };
        const auto emptyJson = [](ruvia::db_query& query) {
            return query.cast(query.value("{}"), ruvia::db_data_type::jsonb);
        };
        const auto emptyArray = [](ruvia::db_query& query) {
            return query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
        };
        const auto addDeviceFilters = [&](ruvia::db_query& query) {
            auto predicate = db::andAll(
                query,
                query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "d")),
                query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceModelEntity::column_name<"deleted_at">(), "p"))
            );
            predicate = query.binary(
                predicate,
                ruvia::db_binary_operator::and_value,
                query.binary(query.column(service::device::entities::DeviceModelEntity::column_name<"enabled">(), "p"), ruvia::db_binary_operator::equal, db::boolean(query, true))
            );
            if (onlyDevice) {
                predicate = query.binary(
                    predicate,
                    ruvia::db_binary_operator::and_value,
                    query.binary(query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), ruvia::db_binary_operator::equal, db::uuid(query, *onlyDevice))
                );
            }
            query.where(predicate);
        };

        ruvia::db_query modbus(c.pool());
        const auto modbusElement = modbus.column("element", "elements");
        const auto modbusRegisterType = db::jsonText(
            modbus,
            modbusElement,
            "registerType"
        );
        ruvia::db_query modbusLabelOne(c.pool());
        const auto modbusLabelOneElement = modbusLabelOne.column("element", "elements");
        const std::array<ruvia::db_source_column, 1> sourceColumns2{ { { .name_ = "value" } } };
        modbusLabelOne
            .select(db::jsonText(modbusLabelOne, modbusLabelOne.column("value", "mapping"), "label"))

            .from_function(
                modbusLabelOne.call(
                    "jsonb_array_elements",
                    { modbusLabelOne.coalesce(
                        { db::jsonValue(
                              modbusLabelOne,
                              db::jsonValue(modbusLabelOne, modbusLabelOneElement, "dictConfig"),
                              "items"
                          ),
                          emptyArray(modbusLabelOne) }
                    ) }
                ),
                "mapping",
                { .columns_ = sourceColumns2 }
            )
            .where(modbusLabelOne.binary(
                db::jsonText(modbusLabelOne, modbusLabelOne.column("value", "mapping"), "key"),
                ruvia::db_binary_operator::equal,
                modbusLabelOne.value("1")
            ))
            .limit(1);
        ruvia::db_query modbusLabelZero(c.pool());
        const auto modbusLabelZeroElement = modbusLabelZero.column("element", "elements");
        const std::array<ruvia::db_source_column, 1> sourceColumns3{ { { .name_ = "value" } } };
        modbusLabelZero
            .select(db::jsonText(modbusLabelZero, modbusLabelZero.column("value", "mapping"), "label"))

            .from_function(
                modbusLabelZero.call(
                    "jsonb_array_elements",
                    { modbusLabelZero.coalesce(
                        { db::jsonValue(
                              modbusLabelZero,
                              db::jsonValue(modbusLabelZero, modbusLabelZeroElement, "dictConfig"),
                              "items"
                          ),
                          emptyArray(modbusLabelZero) }
                    ) }
                ),
                "mapping",
                { .columns_ = sourceColumns3 }
            )
            .where(modbusLabelZero.binary(
                db::jsonText(modbusLabelZero, modbusLabelZero.column("value", "mapping"), "key"),
                ruvia::db_binary_operator::equal,
                modbusLabelZero.value("0")
            ))
            .limit(1);
        const auto modbusPreset = modbus.case_when(
            { { modbus.binary(modbusRegisterType, ruvia::db_binary_operator::equal, modbus.value("COIL")),
                modbus.call(
                    "jsonb_build_array",
                    { modbus.call("jsonb_build_object", { db::text(modbus, "label"), modbus.coalesce({ modbus.subquery(modbusLabelOne), db::text(modbus, "1") }), db::text(modbus, "value"), db::text(modbus, "1") }),
                      modbus.call("jsonb_build_object", { db::text(modbus, "label"), modbus.coalesce({ modbus.subquery(modbusLabelZero), db::text(modbus, "0") }), db::text(modbus, "value"), db::text(modbus, "0") }) }
                ) } },
            emptyArray(modbus)
        );
        const std::array<ruvia::db_source_column, 2> sourceColumns4{ { { .name_ = "element" }, { .name_ = "element_position" } } };
        const std::array<ruvia::db_source_column, 2> sourceColumns5{ { { .name_ = "preset" }, { .name_ = "preset_position" } } };
        modbus
            .select({ modbus.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), db::text(modbus, "MODBUS_WRITE"), db::text(modbus, "写寄存器"), modbusElement, db::integer(modbus, 1), modbus.column("element_position", "elements"), modbus.column("preset", "presets"), modbus.column("preset_position", "presets") })
            .from(service::device::entities::DeviceEntity::table_name(), "d")
            .join(ruvia::db_join_type::inner, service::device::entities::DeviceModelEntity::table_name(), db::andAll(modbus, modbus.binary(modbus.column(service::device::entities::DeviceModelEntity::column_name<"device_id">(), "p"), ruvia::db_binary_operator::equal, modbus.column(service::device::entities::DeviceEntity::column_name<"id">(), "d")), modbus.binary(modbus.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, modbus.value("Modbus"))), "p")

            .join_function(
                ruvia::db_join_type::cross,
                modbus.call("jsonb_array_elements", { modbus.coalesce({ db::jsonValue(modbus, modbus.column(service::device::entities::DeviceModelEntity::column_name<"config">(), "p"), "registers"), emptyArray(modbus) }) }),
                {},
                "elements",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns4 }
            )

            .join_function(
                ruvia::db_join_type::left,
                modbus.call("jsonb_array_elements", { modbusPreset }),
                db::boolean(modbus, true),
                "presets",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns5 }
            );
        addDeviceFilters(modbus);
        modbus.and_where(makeWritable(
            modbus,
            db::jsonText(modbus, modbusElement, "writable")
        ));

        ruvia::db_query s7(c.pool());
        const auto s7Element = s7.column("element", "elements");
        const auto s7Preset = s7.case_when(
            { { s7.binary(db::jsonText(s7, s7Element, "dataType"), ruvia::db_binary_operator::equal, s7.value("BOOL")),
                s7.cast(s7.value("[{\"label\":\"1\",\"value\":\"1\"},"
                                 "{\"label\":\"0\",\"value\":\"0\"}]"),
                        ruvia::db_data_type::jsonb) } },
            emptyArray(s7)
        );
        const auto commandName = s7.case_when(
            { { s7.binary(s7.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, s7.value("MQTT")),
                s7.value("发送控制消息") } },
            s7.value("写寄存器")
        );
        const std::array<ruvia::db_source_column, 2> sourceColumns6{ { { .name_ = "element" }, { .name_ = "element_position" } } };
        const std::array<ruvia::db_source_column, 2> sourceColumns7{ { { .name_ = "preset" }, { .name_ = "preset_position" } } };
        s7.select({ s7.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), s7.binary(s7.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::concat, s7.value("_WRITE")), commandName, s7Element, db::integer(s7, 2), s7.column("element_position", "elements"), s7.column("preset", "presets"), s7.column("preset_position", "presets") })
            .from(service::device::entities::DeviceEntity::table_name(), "d")
            .join(ruvia::db_join_type::inner, service::device::entities::DeviceModelEntity::table_name(), db::andAll(s7, s7.binary(s7.column(service::device::entities::DeviceModelEntity::column_name<"device_id">(), "p"), ruvia::db_binary_operator::equal, s7.column(service::device::entities::DeviceEntity::column_name<"id">(), "d")), s7.binary(s7.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::in, s7.list({ s7.value("S7"), s7.value("MC"), s7.value("FINS"), s7.value("DLT645"), s7.value("MQTT") }))), "p")

            .join_function(
                ruvia::db_join_type::cross,
                s7.call("jsonb_array_elements", { s7.coalesce({ db::jsonValue(s7, s7.column(service::device::entities::DeviceModelEntity::column_name<"config">(), "p"), "areas"), db::jsonValue(s7, s7.column(service::device::entities::DeviceModelEntity::column_name<"config">(), "p"), "points"), emptyArray(s7) }) }),
                {},
                "elements",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns6 }
            )

            .join_function(ruvia::db_join_type::left, s7.call("jsonb_array_elements", { s7Preset }), db::boolean(s7, true), "presets", { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns7 });
        addDeviceFilters(s7);
        s7.and_where(makeWritable(s7, db::jsonText(s7, s7Element, "writable")));

        ruvia::db_query sl651(c.pool());
        const auto function = sl651.column("function", "functions");
        const auto sl651Element = sl651.column("element", "elements");
        const std::array<ruvia::db_source_column, 2> sourceColumns8{ { { .name_ = "function" }, { .name_ = "function_position" } } };
        const std::array<ruvia::db_source_column, 2> sourceColumns9{ { { .name_ = "element" }, { .name_ = "element_position" } } };
        const std::array<ruvia::db_source_column, 2> sourceColumns10{ { { .name_ = "preset" }, { .name_ = "preset_position" } } };
        sl651.select({ sl651.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), db::jsonText(sl651, function, "funcCode"), sl651.coalesce({ sl651.null_if(db::jsonText(sl651, function, "name"), sl651.value("")), db::jsonText(sl651, function, "funcCode") }), sl651Element, sl651.binary(sl651.column("function_position", "functions"), ruvia::db_binary_operator::add, db::integer(sl651, 2)), sl651.column("element_position", "elements"), sl651.column("preset", "presets"), sl651.column("preset_position", "presets") })
            .from(service::device::entities::DeviceEntity::table_name(), "d")
            .join(ruvia::db_join_type::inner, service::device::entities::DeviceModelEntity::table_name(), db::andAll(sl651, sl651.binary(sl651.column(service::device::entities::DeviceModelEntity::column_name<"device_id">(), "p"), ruvia::db_binary_operator::equal, sl651.column(service::device::entities::DeviceEntity::column_name<"id">(), "d")), sl651.binary(sl651.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, sl651.value("SL651"))), "p")

            .join_function(
                ruvia::db_join_type::cross,
                sl651.call("jsonb_array_elements", { sl651.coalesce({ db::jsonValue(sl651, sl651.column(service::device::entities::DeviceModelEntity::column_name<"config">(), "p"), "funcs"), emptyArray(sl651) }) }),
                {},
                "functions",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns8 }
            )

            .join_function(
                ruvia::db_join_type::cross,
                sl651.call("jsonb_array_elements", { sl651.coalesce({ db::jsonValue(sl651, function, "elements"), emptyArray(sl651) }) }),
                {},
                "elements",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns9 }
            )

            .join_function(
                ruvia::db_join_type::left,
                sl651.call("jsonb_array_elements", { sl651.coalesce({ db::jsonValue(sl651, sl651Element, "options"), emptyArray(sl651) }) }),
                db::boolean(sl651, true),
                "presets",
                { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns10 }
            );
        addDeviceFilters(sl651);
        sl651.and_where(sl651.binary(db::jsonText(sl651, function, "dir"), ruvia::db_binary_operator::equal, sl651.value("DOWN")));
        sl651.and_where(sl651.binary(
            sl651.coalesce({ db::jsonText(sl651, sl651Element, "encode"), sl651.value("") }),
            ruvia::db_binary_operator::not_equal,
            sl651.value("JPEG")
        ));

        modbus.combine(ruvia::db_set_operation::union_all, s7)
            .combine(ruvia::db_set_operation::union_all, sl651);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("command_element");

        const std::array<ruvia::borrowed_text, 8> sourceColumns11{ { "device_id", "operation_key", "operation_name", "element", "operation_position", "element_position", "preset", "preset_position" } };
        queryBuilder.with("command_element", modbus, { .columns_ = sourceColumns11 });
        const auto element = query.column("element", "command_element");
        const auto preset = query.column("preset", "command_element");
        queryBuilder.select({ { "device_id", db::text(query, query.column("device_id", "command_element")) }, { "operation_key", query.column("operation_key", "command_element") }, { "operation_name", query.column("operation_name", "command_element") }, { "value3", db::jsonText(query, element, "id") }, { "value4", db::jsonText(query, element, "name") }, { "value5", query.coalesce({ db::jsonText(query, element, "unit"), query.value("") }) }, { "value6", query.coalesce({ db::jsonText(query, element, "registerType"), query.value("") }) }, { "value7", query.coalesce({ db::jsonText(query, element, "dataType"), query.value("") }) }, { "value8", db::jsonText(query, element, "size") }, { "value9", query.coalesce({ db::jsonText(query, element, "encode"), query.value("") }) }, { "value10", db::jsonText(query, element, "length") }, { "value11", db::jsonText(query, element, "digits") }, { "value12", db::jsonText(query, preset, "label") }, { "value13", db::jsonText(query, preset, "value") }, { "operation_position", query.column("operation_position", "command_element") }, { "element_position", query.column("element_position", "command_element") }, { "preset_position", query.column("preset_position", "command_element") } }).from_cte("command_element").order_by(query.column("device_id", "command_element")).add_order_by(query.column("operation_position", "command_element")).add_order_by(query.column("operation_key", "command_element")).add_order_by(query.column("element_position", "command_element")).add_order_by(query.column("preset_position", "command_element"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::last);
        const auto rows = co_await queryBuilder.get_raw_many();

        struct OptionData {
            std::string label;
            std::string value;
        };

        struct ElementData {
            std::string id;
            std::string name;
            std::string unit;
            std::string registerType;
            std::string dataType;
            std::optional<std::int64_t> size;
            std::string encode;
            std::optional<std::int64_t> length;
            std::optional<std::int64_t> digits;
            std::vector<OptionData> options;
        };

        struct OperationData {
            std::string key;
            std::string name;
            std::vector<ElementData> elements;
        };

        std::map<std::string, std::vector<OperationData>, std::less<>> configured;
        for (const auto& row : rows) {
            const auto deviceId = std::string(row[0].value().value_or(std::string_view{}));
            if (!items.contains(deviceId)) {
                continue;
            }
            auto& operations = configured[deviceId];
            const auto operationKey = std::string(row[1].value().value_or(std::string_view{}));
            auto operation =
                std::find_if(operations.begin(), operations.end(), [&](const auto& value) {
                    return value.key == operationKey;
                });
            if (operation == operations.end()) {
                operations.push_back({ operationKey, std::string(row[2].value().value_or(std::string_view{})), {} });
                operation = std::prev(operations.end());
            }
            const auto elementId = std::string(row[3].value().value_or(std::string_view{}));
            auto element = std::find_if(operation->elements.begin(), operation->elements.end(), [&](const auto& value) {
                return value.id == elementId;
            });
            if (element == operation->elements.end()) {
                ElementData data;
                data.id = elementId;
                data.name = std::string(row[4].value().value_or(std::string_view{}));
                data.unit = std::string(row[5].value().value_or(std::string_view{}));
                data.registerType = std::string(row[6].value().value_or(std::string_view{}));
                data.dataType = std::string(row[7].value().value_or(std::string_view{}));
                if (row[8].value().has_value()) {
                    data.size = service::utils::parseInt64(row[8].value().value_or(std::string_view{})).value_or(0);
                }
                data.encode = std::string(row[9].value().value_or(std::string_view{}));
                if (row[10].value().has_value()) {
                    data.length = service::utils::parseInt64(row[10].value().value_or(std::string_view{})).value_or(0);
                }
                if (row[11].value().has_value()) {
                    data.digits = service::utils::parseInt64(row[11].value().value_or(std::string_view{})).value_or(0);
                }
                operation->elements.push_back(std::move(data));
                element = std::prev(operation->elements.end());
            }
            if (row[12].value().has_value() && row[13].value().has_value()) {
                element->options.push_back(
                    { std::string(row[12].value().value_or(std::string_view{})), std::string(row[13].value().value_or(std::string_view{})) }
                );
            }
        }

        if (onlyDevice) {
            using Source = service::device::entities::ExternalIoSourceEntity;
            using Binding = service::device::entities::ExternalIoBindingEntity;
            using Model = service::protocol::entities::ProtocolConfigEntity;
            ruvia::db_query ioQuery(c.pool());
            auto ioBuilder = c.db().template repository<Source>().query_builder("s");
            ioBuilder.select({ { "point", db::text(ioQuery, Binding::column<"point_id">().expression(ioQuery, "b")) }, { "config", Model::column<"config">().expression(ioQuery, "p") } })
                .template join<Binding>(ruvia::db_join_type::inner, "b", ioQuery.binary(Binding::column<"device_id">().expression(ioQuery, "b"), ruvia::db_binary_operator::equal, Source::column<"device_id">().expression(ioQuery, "s")))
                .template join<Model>(ruvia::db_join_type::inner, "p", ioQuery.binary(Model::column<"id">().expression(ioQuery, "p"), ruvia::db_binary_operator::equal, Source::column<"protocol_config_id">().expression(ioQuery, "s")))
                .where(db::andAll(ioQuery, ioQuery.binary(Source::column<"device_id">().expression(ioQuery, "s"), ruvia::db_binary_operator::equal, db::uuid(ioQuery, *onlyDevice)), ioQuery.binary(Binding::column<"kind">().expression(ioQuery, "b"), ruvia::db_binary_operator::equal, ioQuery.value("DO"))));
            for (const auto& row : co_await ioBuilder.get_raw_many()) {
                const std::string pointId(row[0].value().value_or(""));
                const auto config = ruvia::from_json<service::protocol::ExternalIoConfig>(row[1].value().value_or("{}"));
                if (!config.template get<"points">()) continue;
                for (const auto& point : *config.template get<"points">()) {
                    if (point.template get<"id">().view() != pointId) continue;
                    ElementData element;
                    element.id = pointId;
                    element.name = point.template get<"name">().view();
                    element.dataType = "BOOL";
                    element.options = { { "关闭", "0" }, { "开启", "1" } };
                    configured[std::string(*onlyDevice)].push_back({ "EXTERNAL_IO_" + pointId, "外部 IO：" + element.name, { std::move(element) } });
                    break;
                }
            }
        }

        for (auto& [deviceId, operations] : configured) {
            const auto item = items.find(deviceId);
            if (item == items.end()) {
                continue;
            }
            ruvia::boxed_array<service::device::DeviceCommandOperationDto> operationDtos(
                ruvia::model_options{ .resource_ = c.arena() }
            );
            for (const auto& operation : operations) {
                auto& operationDto = operationDtos.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
                operationDto.template set<"name">(operation.name);
                ruvia::boxed_array<service::device::DeviceCommandOperationElementDto> elementDtos(
                    ruvia::model_options{ .resource_ = c.arena() }
                );
                for (const auto& element : operation.elements) {
                    auto& elementDto = elementDtos.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
                    elementDto.template set<"elementId">(element.id).template set<"name">(element.name).template set<"value">("");
                    if (!element.unit.empty()) {
                        elementDto.template set<"unit">(element.unit);
                    }
                    if (!element.registerType.empty()) {
                        elementDto.template set<"registerType">(element.registerType);
                    }
                    if (!element.dataType.empty()) {
                        elementDto.template set<"dataType">(element.dataType);
                    }
                    if (element.size) {
                        elementDto.template set<"size">(*element.size);
                    }
                    if (!element.encode.empty()) {
                        elementDto.template set<"encode">(element.encode);
                    }
                    if (element.length) {
                        elementDto.template set<"length">(*element.length);
                    }
                    if (element.digits) {
                        elementDto.template set<"digits">(*element.digits);
                    }
                    if (!element.options.empty()) {
                        ruvia::boxed_array<service::device::DeviceCommandOptionDto> optionDtos(
                            ruvia::model_options{ .resource_ = c.arena() }
                        );
                        for (const auto& option : element.options) {
                            optionDtos.emplace_back(ruvia::model_options{ .resource_ = c.arena() }).template set<"label">(option.label).template set<"value">(option.value);
                        }
                        elementDto.template set<"options">(std::move(optionDtos));
                    }
                }
                operationDto.template set<"elements">(std::move(elementDtos));
            }
            item->second->template set<"commandOperations">(std::move(operationDtos));
        }
        ruvia::db_query mqttQuery(c.pool());
        auto mqttQueryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("d");
        mqttQueryBuilder.select({ { "id", mqttQuery.column(service::device::entities::DeviceEntity::column_name<"id">(), "d") }, { "config", mqttQuery.column(service::device::entities::DeviceModelEntity::column_name<"config">(), "p") } }).template join<service::device::entities::DeviceModelEntity>(ruvia::db_join_type::inner, "p", db::andAll(mqttQuery, mqttQuery.binary(mqttQuery.column(service::device::entities::DeviceModelEntity::column_name<"device_id">(), "p"), ruvia::db_binary_operator::equal, mqttQuery.column(service::device::entities::DeviceEntity::column_name<"id">(), "d")), mqttQuery.binary(mqttQuery.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, mqttQuery.value("MQTT"))));
        addDeviceFilters(mqttQuery);
        const auto mqttRows = co_await mqttQueryBuilder.get_raw_many();
        for (const auto& row : mqttRows) {
            const auto item = items.find(std::string(row[0].value().value_or(std::string_view{})));
            if (item == items.end()) {
                continue;
            }
            const auto config = ruvia::from_json<service::protocol::MqttConfig>(row[1].value().value_or(std::string_view{}));
            if (!config.template get<"commands">() || !config.template get<"points">()) {
                continue;
            }
            ruvia::boxed_array<service::device::DeviceCommandOperationDto> operations(ruvia::model_options{ .resource_ = c.arena() });
            service::utils::JsonValueTemplate::Tokens allowed{ "$deviceCode", "$deviceId", "$commandId" };
            for (const auto& point : *config.template get<"points">()) {
                allowed.emplace("$point:" + std::string(point.template get<"name">().view()));
            }
            for (const auto& command : *config.template get<"commands">()) {
                const service::utils::JsonValueTemplate message(command.template get<"templateText">().view(), allowed, {}, false, 16384, 32, { "$param:" });
                std::set<std::string, std::less<>> topicNames, requiredIds;
                for (const auto level : service::utils::splitPayload(command.template get<"topic">().view(), "/")) {
                    if (level.starts_with("{point:") && level.ends_with('}')) {
                        topicNames.emplace(level.substr(7, level.size() - 8));
                    }
                }
                for (const auto& id : command.template get<"requiredPointIds">()) {
                    requiredIds.emplace(id.view());
                }
                auto& operation = operations.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
                operation.template set<"name">(command.template get<"name">().view()).template set<"mqttMessageId">(command.template get<"id">().view());
                ruvia::boxed_array<service::device::DeviceCommandOperationElementDto> elements(ruvia::model_options{ .resource_ = c.arena() });
                for (const auto& point : *config.template get<"points">()) {
                    if (!message.references("$point:" + std::string(point.template get<"name">().view())) && !topicNames.contains(point.template get<"name">().view())) {
                        continue;
                    }
                    auto& element = elements.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
                    element.template set<"elementId">(point.template get<"id">().view()).template set<"name">(point.template get<"name">().view()).template set<"value">("").template set<"dataType">(point.template get<"dataType">().view()).template set<"required">(requiredIds.contains(point.template get<"id">().view()));
                    if (point.template get<"unit">()) {
                        element.template set<"unit">(point.template get<"unit">()->view());
                    }
                    if (point.template get<"dataType">().view() == "BOOL") {
                        ruvia::boxed_array<service::device::DeviceCommandOptionDto> options(ruvia::model_options{ .resource_ = c.arena() });
                        for (const auto value : { "1", "0" }) {
                            options.emplace_back(ruvia::model_options{ .resource_ = c.arena() }).template set<"label">(value).template set<"value">(value);
                        }
                        element.template set<"options">(std::move(options));
                    }
                }
                operation.template set<"elements">(std::move(elements));
            }
            item->second->template set<"commandOperations">(std::move(operations));
        }
    }

  private:
    template <typename Database>
    static auto deviceRemoteControlQuery(Database&& database, std::pmr::memory_resource* resource, std::string_view deviceId) {
        ruvia::db_query query(resource);
        const auto protocolParams = query.column(service::device::entities::DeviceEntity::column_name<"protocol_params">());
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
        auto queryBuilder = database.template repository<service::device::entities::DeviceEntity>().query_builder();
        queryBuilder.select({ { "value0", query.case_when({ { hasRemoteControl, enabled } }, query.cast(query.value(true), ruvia::db_data_type::boolean)) } }).where(query.binary(query.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, query.cast(query.value(deviceId), ruvia::db_data_type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceEntity::column_name<"deleted_at">()))).limit(1);
        return queryBuilder;
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
        if (mqttMessage && !service::common::isUuid(mqttMessage->view())) {
            service::common::fail(18010, "控制消息 ID 必须是 UUID", 400);
        }
        const std::string mqttSelection = mqttMessage
            ? ",\"mqttMessageId\":" + service::utils::jsonQuoted(mqttMessage->view())
            : "";
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
            ? "{\"elements\":" + payload + mqttSelection + "}"
            : payload;
        auto transaction = co_await context.db().begin_transaction();
        const std::string lockKey = submittedBy + ":" + key;
        co_await transaction.advisory_lock(lockKey, service::message::lock::kCommandIdempotencySeed);

        ruvia::db_query priorQuery(context.pool());
        const auto priorDevice = priorQuery.binary(
            priorQuery.column(service::command::entities::CommandRequestEntity::column_name<"device_id">()),
            ruvia::db_binary_operator::equal,
            priorQuery.cast(priorQuery.value(deviceId), ruvia::db_data_type::uuid)
        );
        const auto priorPayload = priorQuery.binary(
            priorQuery.column(service::command::entities::CommandRequestEntity::column_name<"payload">()),
            ruvia::db_binary_operator::equal,
            priorQuery.cast(priorQuery.value(requestPayload), ruvia::db_data_type::jsonb)
        );
        auto priorQueryBuilder = transaction.template repository<service::command::entities::CommandRequestEntity>().query_builder();
        priorQueryBuilder.select({ { "id", priorQuery.cast(priorQuery.column(service::command::entities::CommandRequestEntity::column_name<"id">()), ruvia::db_data_type::text) }, { "value1", priorQuery.binary(priorDevice, ruvia::db_binary_operator::and_value, priorPayload) } }).where(priorQuery.binary(priorQuery.binary(priorQuery.column(service::command::entities::CommandRequestEntity::column_name<"actor">()), ruvia::db_binary_operator::equal, priorQuery.value(submittedBy)), ruvia::db_binary_operator::and_value, priorQuery.binary(priorQuery.column(service::command::entities::CommandRequestEntity::column_name<"idempotency_key">()), ruvia::db_binary_operator::equal, priorQuery.cast(priorQuery.value(key), ruvia::db_data_type::uuid))));
        const auto prior = co_await priorQueryBuilder.get_raw_many();
        if (!prior.empty()) {
            if (prior.front()[1].value().value_or(std::string_view{}) != "t") {
                common::fail(18014, "幂等键已用于不同的指令请求", 409);
            }
            ruvia::db_query commandQuery(context.pool());
            auto commandQueryBuilder = transaction.template repository<service::command::entities::CommandOperationEntity>().query_builder();
            commandQueryBuilder.select({ { "id", commandQuery.cast(commandQuery.column(service::command::entities::CommandOperationEntity::column_name<"id">()), ruvia::db_data_type::text) } }).where(commandQuery.binary(commandQuery.column(service::command::entities::CommandOperationEntity::column_name<"request_id">()), ruvia::db_binary_operator::equal, commandQuery.cast(commandQuery.value(prior.front()[0].value().value_or(std::string_view{})), ruvia::db_data_type::uuid))).order_by(commandQuery.column(service::command::entities::CommandOperationEntity::column_name<"ordinal">()));
            const auto commands = co_await commandQueryBuilder.get_raw_many();
            ruvia::boxed_array<ruvia::string> ids(ruvia::model_options{ .resource_ = context.arena() });
            for (const auto& row : commands) {
                ids.emplace_back(row[0].value().value_or(std::string_view{}), ruvia::model_options{ .resource_ = context.arena() });
            }
            co_await transaction.commit();
            service::device::DeviceCommandCreateDto result(ruvia::model_options{ .resource_ = context.arena() });
            result.template set<"commandIds">(std::move(ids)).template set<"status">("ACCEPTED");
            co_return result;
        }
        const auto requestId = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
        ruvia::db_query requestQuery(context.pool());
        const std::array<std::string_view, 5> requestQueryColumns{ service::command::entities::CommandRequestEntity::column_name<"id">(), service::command::entities::CommandRequestEntity::column_name<"actor">(), service::command::entities::CommandRequestEntity::column_name<"idempotency_key">(), service::command::entities::CommandRequestEntity::column_name<"device_id">(), service::command::entities::CommandRequestEntity::column_name<"payload">() };
        auto requestQueryBuilder = transaction.template repository<service::command::entities::CommandRequestEntity>().insert_builder();
        ruvia::db_values_builder requestQueryValues(requestQueryColumns);
        requestQueryValues.row({ requestQuery.cast(requestQuery.value(requestId), ruvia::db_data_type::uuid), requestQuery.value(submittedBy), requestQuery.cast(requestQuery.value(key), ruvia::db_data_type::uuid), requestQuery.cast(requestQuery.value(deviceId), ruvia::db_data_type::uuid), requestQuery.cast(requestQuery.value(requestPayload), ruvia::db_data_type::jsonb) });
        requestQueryBuilder.insert_from(requestQueryColumns, requestQueryValues.query());
        (void)co_await requestQueryBuilder.execute();

        ruvia::db_query deviceLock(context.pool());
        auto deviceLockBuilder = transaction.template repository<service::device::entities::DeviceEntity>().query_builder();
        deviceLockBuilder.select({ { "id", deviceLock.column(service::device::entities::DeviceEntity::column_name<"id">()) } }).where(deviceLock.binary(deviceLock.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, deviceLock.cast(deviceLock.value(deviceId), ruvia::db_data_type::uuid))).set_lock({ .mode_ = ruvia::db_row_lock::share });
        (void)co_await deviceLockBuilder.get_raw_many();
        service::message::command::PrepareRequest prepareRequest(ruvia::model_options{ .resource_ = context.arena() });
        prepareRequest.template set<"deviceId">(deviceId);
        if (mqttMessage) {
            prepareRequest.template set<"mqttMessageId">(mqttMessage->view());
        }
        ruvia::array<ruvia::array<ruvia::string>> preparedElements(ruvia::model_options{ .resource_ = context.arena() });
        for (const auto& [elementId, value] : elements) {
            auto& pair = preparedElements.emplace_back();
            pair.emplace_back(std::string_view(elementId));
            pair.emplace_back(std::string_view(value));
        }
        prepareRequest.template set<"elements">(std::move(preparedElements));
        const auto prepared = co_await service::utils::callRpc(context, "command", "prepare", service::message::rpc::encode(prepareRequest, context.arena()));
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
        const auto batch = ruvia::from_json<service::message::command::PreparedBatch>(preparedJson, { .resource_ = context.arena() });
        if (batch.template get<"commands">().empty() || batch.template get<"commands">().size() > 256) {
            common::fail(10004, "Invalid prepared command batch", 502);
        }
        const auto kind = batch.template get<"kind">().view();
        const auto maximum = static_cast<std::int64_t>(batch.template get<"maximum">());
        if ((kind != "list" && kind != "stream") || maximum <= 0 || maximum > 10000) {
            common::fail(10004, "Invalid prepared command queue", 502);
        }
        ruvia::boxed_array<ruvia::string> commandIds(ruvia::model_options{ .resource_ = context.arena() });
        std::int64_t ordinal = 0;
        for (const auto& command : batch.template get<"commands">()) {
            if (!common::isUuid(command.template get<"id">().view()) ||
                command.template get<"deviceId">().view() != deviceId) {
                common::fail(10004, "Invalid prepared command", 502);
            }
            std::string elements = "[";
            for (const auto& element : command.template get<"elements">()) {
                if (elements.size() > 1) {
                    elements += ',';
                }
                elements += "{\"elementId\":" + service::utils::jsonQuoted(element.template get<"elementId">().view()) +
                    ",\"value\":" + service::utils::jsonQuoted(element.template get<"value">().view()) + "}";
            }
            elements += ']';
            std::string payload = "[";
            for (const auto& value : command.template get<"payload">()) {
                if (payload.size() > 1) {
                    payload += ',';
                }
                payload += service::utils::jsonQuoted(value.view());
            }
            payload += ']';
            const auto id = command.template get<"id">().view();
            const auto operationOrdinal = ordinal++;
            ruvia::db_query operationSource(context.pool());
            ruvia::db_query ioModel(context.pool());
            using IoSource = service::device::entities::ExternalIoSourceEntity;
            ioModel.select({ IoSource::column<"protocol_config_id">().expression(ioModel) })
                .from(IoSource::table_name())
                .where((IoSource::column<"device_id">() == deviceId).expression(ioModel));
            const auto modelId = command.template get<"protocol">().view() == "ExternalIO"
                ? operationSource.subquery(ioModel)
                : operationSource.column(service::device::entities::DeviceEntity::column_name<"protocol_config_id">());
            operationSource
                .select({ operationSource.cast(operationSource.value(id), ruvia::db_data_type::uuid), operationSource.cast(operationSource.value(requestId), ruvia::db_data_type::uuid), operationSource.value(static_cast<std::int32_t>(operationOrdinal)), operationSource.cast(operationSource.value(deviceId), ruvia::db_data_type::uuid), operationSource.value(command.template get<"deviceCode">().view()), operationSource.value(command.template get<"protocol">().view()), operationSource.value("ACCEPTED"), operationSource.cast(operationSource.value(elements), ruvia::db_data_type::jsonb), modelId })
                .from(service::device::entities::DeviceEntity::table_name())
                .where(operationSource.binary(
                    operationSource.column(service::device::entities::DeviceEntity::column_name<"id">()),
                    ruvia::db_binary_operator::equal,
                    operationSource.cast(operationSource.value(deviceId), ruvia::db_data_type::uuid)
                ));
            const std::array<std::string_view, 9> operationQueryColumns{ service::command::entities::CommandOperationEntity::column_name<"id">(), service::command::entities::CommandOperationEntity::column_name<"request_id">(), service::command::entities::CommandOperationEntity::column_name<"ordinal">(), service::command::entities::CommandOperationEntity::column_name<"device_id">(), service::command::entities::CommandOperationEntity::column_name<"device_code">(), service::command::entities::CommandOperationEntity::column_name<"protocol">(), service::command::entities::CommandOperationEntity::column_name<"status">(), service::command::entities::CommandOperationEntity::column_name<"elements">(), service::command::entities::CommandOperationEntity::column_name<"model_id">() };
            auto operationQueryBuilder = transaction.template repository<service::command::entities::CommandOperationEntity>().insert_builder();
            operationQueryBuilder.insert_from(operationQueryColumns, operationSource);
            (void)co_await operationQueryBuilder.execute();

            ruvia::db_query attemptQuery(context.pool());
            const std::array<std::string_view, 7> attemptQueryColumns{ service::command::entities::CommandAttemptEntity::column_name<"operation_id">(), service::command::entities::CommandAttemptEntity::column_name<"queue_key">(), service::command::entities::CommandAttemptEntity::column_name<"queue_kind">(), service::command::entities::CommandAttemptEntity::column_name<"payload">(), service::command::entities::CommandAttemptEntity::column_name<"submitted_by">(), service::command::entities::CommandAttemptEntity::column_name<"node_id">(), service::command::entities::CommandAttemptEntity::column_name<"max_length">() };
            auto attemptQueryBuilder = transaction.template repository<service::command::entities::CommandAttemptEntity>().insert_builder();
            ruvia::db_values_builder attemptQueryValues(attemptQueryColumns);
            attemptQueryValues.row({ attemptQuery.cast(attemptQuery.value(id), ruvia::db_data_type::uuid), attemptQuery.value(batch.template get<"queue">().view()), attemptQuery.value(kind), attemptQuery.cast(attemptQuery.value(payload), ruvia::db_data_type::jsonb), attemptQuery.value(submittedBy), attemptQuery.value(batch.template get<"nodeId">().view()), attemptQuery.value(static_cast<std::int32_t>(maximum)) });
            attemptQueryBuilder.insert_from(attemptQueryColumns, attemptQueryValues.query());
            (void)co_await attemptQueryBuilder.execute();
            const auto eventId = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
            ruvia::db_query eventSource(context.pool());
            const auto jsonKey = [&](std::string_view key) {
                return eventSource.cast(eventSource.value(key), ruvia::db_data_type::text);
            };
            eventSource
                .select({ eventSource.cast(eventSource.value(eventId), ruvia::db_data_type::uuid), eventSource.value("device.command.accepted"), eventSource.value("command"), eventSource.cast(eventSource.column(service::command::entities::CommandOperationEntity::column_name<"device_id">()), ruvia::db_data_type::text), eventSource.value("updated"), eventSource.value(std::int32_t{ 2 }), eventSource.call("jsonb_build_object", { jsonKey("device_code"), eventSource.column(service::command::entities::CommandOperationEntity::column_name<"device_code">()), jsonKey("data"), eventSource.call("jsonb_build_object", { jsonKey("commandId"), eventSource.cast(eventSource.column(service::command::entities::CommandOperationEntity::column_name<"id">()), ruvia::db_data_type::text), jsonKey("status"), eventSource.column(service::command::entities::CommandOperationEntity::column_name<"status">()), jsonKey("reason"), eventSource.column(service::command::entities::CommandOperationEntity::column_name<"reason">()), jsonKey("elements"), eventSource.column(service::command::entities::CommandOperationEntity::column_name<"elements">()), jsonKey("actualValues"), eventSource.column(service::command::entities::CommandOperationEntity::column_name<"actual_values">()) }) }) })
                .from(service::command::entities::CommandOperationEntity::table_name())
                .where(eventSource.binary(
                    eventSource.column(service::command::entities::CommandOperationEntity::column_name<"id">()),
                    ruvia::db_binary_operator::equal,
                    eventSource.cast(eventSource.value(id), ruvia::db_data_type::uuid)
                ));
            const std::array<std::string_view, 7> eventQueryColumns{ service::system::entities::OutboxEventEntity::column_name<"id">(), service::system::entities::OutboxEventEntity::column_name<"event_type">(), service::system::entities::OutboxEventEntity::column_name<"aggregate_type">(), service::system::entities::OutboxEventEntity::column_name<"aggregate_id">(), service::system::entities::OutboxEventEntity::column_name<"action">(), service::system::entities::OutboxEventEntity::column_name<"schema_version">(), service::system::entities::OutboxEventEntity::column_name<"payload">() };
            auto eventQueryBuilder = transaction.template repository<service::system::entities::OutboxEventEntity>().insert_builder();
            eventQueryBuilder.insert_from(eventQueryColumns, eventSource);
            (void)co_await eventQueryBuilder.execute();
            commandIds.emplace_back(id, ruvia::model_options{ .resource_ = context.arena() });
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

} // namespace service::command
