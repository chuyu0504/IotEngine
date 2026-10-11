#pragma once

#include <memory>
#include <algorithm>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/context.h>
#include <ruvia/web/model_object.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/middleware/rpc.h"
#include "service/modules/alert/alert.entity.h"
#include "service/modules/alert/alert.types.h"
#include "service/utils/json.h"
#include "service/utils/number.h"
#include "service/utils/text.h"

namespace service::alert {

class AlertService final {
    using Query = ruvia::db_query;
    using Op = ruvia::db_binary_operator;
    using Type = ruvia::db_data_type;

  public:
    static AlertService& instance() {
        static thread_local AlertService service;
        return service;
    }

    template <typename Context>
    ruvia::task<std::string> listRules(Context& c, const AlertListQuery& parameters) {
        Query filtered(c.pool());
        filtered.select({ filtered.star("rule"), filtered.alias(filtered.column(service::alert::entities::DeviceEntity::column_name<"name">(), "device"), "device_name") })
            .from(service::alert::entities::AlertRuleEntity::table_name(), "rule")
            .join(ruvia::db_join_type::inner, service::alert::entities::DeviceEntity::table_name(), filtered.binary(filtered.column(service::alert::entities::DeviceEntity::column_name<"id">(), "device"), Op::equal, filtered.column(service::alert::entities::AlertRuleEntity::column_name<"device_id">(), "rule")))
            .and_where(filtered.unary(ruvia::db_unary_operator::is_null, filtered.column(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">(), "rule")));
        appendTextFilter(parameters.get<"keyword">(), "keyword", filtered, filtered.column(service::alert::entities::AlertRuleEntity::column_name<"name">(), "rule"), true);
        appendUuidFilter(parameters.get<"deviceId">(), "deviceId", filtered, filtered.column(service::alert::entities::AlertRuleEntity::column_name<"device_id">(), "rule"));
        appendTextFilter(parameters.get<"severity">(), "severity", filtered, filtered.column(service::alert::entities::AlertRuleEntity::column_name<"severity">(), "rule"));
        appendTextFilter(parameters.get<"status">(), "status", filtered, filtered.cast(filtered.column(service::alert::entities::AlertRuleEntity::column_name<"status">(), "rule"), Type::text));
        Query listed(c.pool());
        listed.select(listed.star()).from("page_rows");
        co_return co_await pageResult(c, parameters, filtered, listed, { "id", "name", "device_id", "device_name", "severity", "conditions", "logic", "silence_duration", "recovery_condition", "recovery_wait_seconds", "status", "remark", "created_at", "updated_at" }, "created_at");
    }

    template <typename Context>
    ruvia::task<void> createRule(Context& c, const RuleInput& input) {
        alertRequest::validateConditions(input.get<"conditions">().view());
        co_await ensureDevice(c, input.get<"deviceId">().view());
        co_await ensureRuleName(c, service::utils::trim(input.get<"name">().view()), input.get<"deviceId">().view(), std::nullopt);
        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        Query query(c.pool());
        query.insert_into(service::alert::entities::AlertRuleEntity::table_name(), { "id", "name", "device_id", "severity", "conditions", "logic", "silence_duration", "recovery_condition", "recovery_wait_seconds", "status", "remark", "created_by" })
            .values({ query.cast(query.value(id), Type::uuid), query.value(service::utils::trim(input.get<"name">().view())), query.cast(query.value(input.get<"deviceId">().view()), Type::uuid), query.value(input.get<"severity">()->view()), query.cast(query.value(input.get<"conditions">().view()), Type::jsonb), query.value(input.get<"logic">()->view()), query.cast(query.value(input.get<"silenceDuration">()->value_), Type::integer), query.value(service::utils::trim(input.get<"recoveryCondition">()->view())), query.cast(query.value(input.get<"recoveryWaitSeconds">()->value_), Type::integer), query.cast(query.value(input.get<"status">()->view()), { .custom_name_ = "status_enum" }), query.null_if(query.value(service::utils::trim(input.get<"remark">()->view())), query.value("")), query.cast(query.value(c.userId), Type::uuid) });
        (void)co_await c.db().execute(query);
        (void)co_await service::rpc::call(c, "alert", "refresh", "{}");
    }

    template <typename Context>
    ruvia::task<void> updateRule(Context& c, std::string_view id, const RuleInput& input) {
        alertRequest::validateConditions(input.get<"conditions">().view());
        service::common::requireUuid(19002, id, "告警规则 ID 无效");
        co_await requireRule(c, id);
        co_await ensureDevice(c, input.get<"deviceId">().view());
        co_await ensureRuleName(c, service::utils::trim(input.get<"name">().view()), input.get<"deviceId">().view(), std::string(id));
        Query query(c.pool());
        query.update(service::alert::entities::AlertRuleEntity::table_name())
            .set(service::alert::entities::AlertRuleEntity::column_name<"name">(), query.value(service::utils::trim(input.get<"name">().view())))
            .set(service::alert::entities::AlertRuleEntity::column_name<"device_id">(), query.cast(query.value(input.get<"deviceId">().view()), Type::uuid))
            .set(service::alert::entities::AlertRuleEntity::column_name<"severity">(), query.value(input.get<"severity">()->view()))
            .set(service::alert::entities::AlertRuleEntity::column_name<"conditions">(), query.cast(query.value(input.get<"conditions">().view()), Type::jsonb))
            .set(service::alert::entities::AlertRuleEntity::column_name<"logic">(), query.value(input.get<"logic">()->view()))
            .set(service::alert::entities::AlertRuleEntity::column_name<"silence_duration">(), query.cast(query.value(input.get<"silenceDuration">()->value_), Type::integer))
            .set(service::alert::entities::AlertRuleEntity::column_name<"recovery_condition">(), query.value(service::utils::trim(input.get<"recoveryCondition">()->view())))
            .set(service::alert::entities::AlertRuleEntity::column_name<"recovery_wait_seconds">(), query.cast(query.value(input.get<"recoveryWaitSeconds">()->value_), Type::integer))
            .set(service::alert::entities::AlertRuleEntity::column_name<"status">(), query.cast(query.value(input.get<"status">()->view()), { .custom_name_ = "status_enum" }))
            .set(service::alert::entities::AlertRuleEntity::column_name<"remark">(), query.null_if(query.value(service::utils::trim(input.get<"remark">()->view())), query.value("")))
            .set(service::alert::entities::AlertRuleEntity::column_name<"updated_at">(), query.call("now"))
            .and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column_name<"id">()), Op::equal, query.cast(query.value(id), Type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">())));
        (void)co_await c.db().execute(query);
        (void)co_await service::rpc::call(c, "alert", "refresh", "{}");
    }

    template <typename Context>
    ruvia::task<void> removeRule(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "告警规则 ID 无效");
        co_await requireRule(c, id);
        auto transaction = co_await c.db().begin_transaction();
        Query records(c.pool());
        records.update(service::alert::entities::OpenAlertRecordEntity::table_name()).set(service::alert::entities::OpenAlertRecordEntity::column_name<"status">(), records.value("resolved")).set(service::alert::entities::OpenAlertRecordEntity::column_name<"resolved_at">(), records.call("now")).set(service::alert::entities::OpenAlertRecordEntity::column_name<"updated_at">(), records.call("now")).and_where(records.binary(records.column(service::alert::entities::OpenAlertRecordEntity::column_name<"rule_id">()), Op::equal, records.cast(records.value(id), Type::uuid))).and_where(records.binary(records.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">()), Op::in, records.list({ records.value("active"), records.value("acknowledged") })));
        (void)co_await transaction.execute(records);
        Query rules(c.pool());
        rules.update(service::alert::entities::AlertRuleEntity::table_name()).set(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">(), rules.call("now")).set(service::alert::entities::AlertRuleEntity::column_name<"updated_at">(), rules.call("now")).and_where(rules.binary(rules.column(service::alert::entities::AlertRuleEntity::column_name<"id">()), Op::equal, rules.cast(rules.value(id), Type::uuid))).and_where(rules.unary(ruvia::db_unary_operator::is_null, rules.column(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">())));
        (void)co_await transaction.execute(rules);
        co_await transaction.commit();
        (void)co_await service::rpc::call(c, "alert", "refresh", "{}");
    }

    template <typename Context>
    ruvia::task<void> batchRemoveRules(Context& c, const std::vector<std::string>& ids) {
        auto transaction = co_await c.db().begin_transaction();
        Query records(c.pool());
        records.update(service::alert::entities::OpenAlertRecordEntity::table_name()).set(service::alert::entities::OpenAlertRecordEntity::column_name<"status">(), records.value("resolved")).set(service::alert::entities::OpenAlertRecordEntity::column_name<"resolved_at">(), records.call("now")).set(service::alert::entities::OpenAlertRecordEntity::column_name<"updated_at">(), records.call("now")).and_where(records.binary(records.column(service::alert::entities::OpenAlertRecordEntity::column_name<"rule_id">()), Op::in, uuidList(records, ids))).and_where(records.binary(records.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">()), Op::in, records.list({ records.value("active"), records.value("acknowledged") })));
        (void)co_await transaction.execute(records);
        Query rules(c.pool());
        rules.update(service::alert::entities::AlertRuleEntity::table_name()).set(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">(), rules.call("now")).set(service::alert::entities::AlertRuleEntity::column_name<"updated_at">(), rules.call("now")).and_where(rules.binary(rules.column(service::alert::entities::AlertRuleEntity::column_name<"id">()), Op::in, uuidList(rules, ids))).and_where(rules.unary(ruvia::db_unary_operator::is_null, rules.column(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">())));
        (void)co_await transaction.execute(rules);
        co_await transaction.commit();
        (void)co_await service::rpc::call(c, "alert", "refresh", "{}");
    }

    template <typename Context>
    ruvia::task<std::string> listTemplates(Context& c, const AlertListQuery& parameters) {
        Query filtered(c.pool());
        filtered.select({ filtered.star("template"), filtered.alias(filtered.column(service::alert::entities::ProtocolConfigEntity::column_name<"name">(), "config"), "config_name"), filtered.alias(filtered.column(service::alert::entities::ProtocolConfigEntity::column_name<"protocol">(), "config"), "protocol_type") })
            .from(service::alert::entities::AlertRuleTemplateEntity::table_name(), "template")
            .join(ruvia::db_join_type::left, service::alert::entities::ProtocolConfigEntity::table_name(), filtered.binary(filtered.column(service::alert::entities::ProtocolConfigEntity::column_name<"id">(), "config"), Op::equal, filtered.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"protocol_config_id">(), "template")), "config")
            .and_where(filtered.unary(ruvia::db_unary_operator::is_null, filtered.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"deleted_at">(), "template")));
        appendTextFilter(parameters.get<"category">(), "category", filtered, filtered.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"category">(), "template"));
        Query listed(c.pool());
        listed.select(listed.star()).from("page_rows");
        co_return co_await pageResult(c, parameters, filtered, listed, { "id", "name", "category", "description", "severity", "logic", "silence_duration", "protocol_config_id", "config_name", "protocol_type", "created_at" }, "created_at");
    }

    template <typename Context>
    ruvia::task<std::string> templateDetail(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "告警模板 ID 无效");
        Query query(c.pool());
        query.select(query.cast(fieldJson(query, { "id", "name", "category", "description", "severity", "conditions", "logic", "silence_duration", "recovery_condition", "recovery_wait_seconds", "applicable_protocols", "protocol_config_id", "created_by", "created_at", "updated_at" }), Type::text))
            .from(service::alert::entities::AlertRuleTemplateEntity::table_name())
            .and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"id">()), Op::equal, query.cast(query.value(id), Type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"deleted_at">())));
        co_return firstObject(co_await c.db().query(query), "告警模板不存在");
    }

    template <typename Context>
    ruvia::task<void> createTemplate(Context& c, const TemplateInput& input) {
        alertRequest::validateConditions(input.get<"conditions">().view());
        if (const auto& protocols = input.get<"applicableProtocols">(); protocols && !protocols->is_array()) service::common::fail(17002, "applicable_protocols 必须是数组", 400);
        co_await ensureTemplateName(c, service::utils::trim(input.get<"name">().view()), std::nullopt);
        if (!input.get<"protocolConfigId">()->view().empty()) {
            co_await ensureProtocolConfig(c, input.get<"protocolConfigId">()->view());
        }
        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        Query query(c.pool());
        query.insert_into(service::alert::entities::AlertRuleTemplateEntity::table_name(), { "id", "name", "category", "description", "severity", "conditions", "logic", "silence_duration", "recovery_condition", "recovery_wait_seconds", "applicable_protocols", "protocol_config_id", "created_by" })
            .values({ query.cast(query.value(id), Type::uuid), query.value(service::utils::trim(input.get<"name">().view())), query.null_if(query.value(service::utils::trim(input.get<"category">()->view())), query.value("")), query.null_if(query.value(service::utils::trim(input.get<"description">()->view())), query.value("")), query.value(input.get<"severity">()->view()), query.cast(query.value(input.get<"conditions">().view()), Type::jsonb), query.value(input.get<"logic">()->view()), query.cast(query.value(input.get<"silenceDuration">()->value_), Type::integer), query.value(service::utils::trim(input.get<"recoveryCondition">()->view())), query.cast(query.value(input.get<"recoveryWaitSeconds">()->value_), Type::integer), query.cast(query.value(input.get<"applicableProtocols">() ? input.get<"applicableProtocols">()->view() : "[]"), Type::jsonb), query.cast(query.null_if(query.value(input.get<"protocolConfigId">()->view()), query.value("")), Type::uuid), query.cast(query.value(c.userId), Type::uuid) });
        (void)co_await c.db().execute(query);
    }

    template <typename Context>
    ruvia::task<void> updateTemplate(Context& c, std::string_view id, const TemplateInput& input) {
        alertRequest::validateConditions(input.get<"conditions">().view());
        if (const auto& protocols = input.get<"applicableProtocols">(); protocols && !protocols->is_array()) service::common::fail(17002, "applicable_protocols 必须是数组", 400);
        service::common::requireUuid(19002, id, "告警模板 ID 无效");
        co_await requireTemplate(c, id);
        co_await ensureTemplateName(c, service::utils::trim(input.get<"name">().view()), std::string(id));
        if (!input.get<"protocolConfigId">()->view().empty()) {
            co_await ensureProtocolConfig(c, input.get<"protocolConfigId">()->view());
        }
        Query query(c.pool());
        query.update(service::alert::entities::AlertRuleTemplateEntity::table_name())
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"name">(), query.value(service::utils::trim(input.get<"name">().view())))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"category">(), query.null_if(query.value(service::utils::trim(input.get<"category">()->view())), query.value("")))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"description">(), query.null_if(query.value(service::utils::trim(input.get<"description">()->view())), query.value("")))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"severity">(), query.value(input.get<"severity">()->view()))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"conditions">(), query.cast(query.value(input.get<"conditions">().view()), Type::jsonb))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"logic">(), query.value(input.get<"logic">()->view()))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"silence_duration">(), query.cast(query.value(input.get<"silenceDuration">()->value_), Type::integer))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"recovery_condition">(), query.value(service::utils::trim(input.get<"recoveryCondition">()->view())))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"recovery_wait_seconds">(), query.cast(query.value(input.get<"recoveryWaitSeconds">()->value_), Type::integer))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"applicable_protocols">(), query.cast(query.value(input.get<"applicableProtocols">() ? input.get<"applicableProtocols">()->view() : "[]"), Type::jsonb))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"protocol_config_id">(), query.cast(query.null_if(query.value(input.get<"protocolConfigId">()->view()), query.value("")), Type::uuid))
            .set(service::alert::entities::AlertRuleTemplateEntity::column_name<"updated_at">(), query.call("now"))
            .and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"id">()), Op::equal, query.cast(query.value(id), Type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"deleted_at">())));
        (void)co_await c.db().execute(query);
    }

    template <typename Context>
    ruvia::task<void> removeTemplate(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "告警模板 ID 无效");
        co_await requireTemplate(c, id);
        Query removal(c.pool());
        removal.update(service::alert::entities::AlertRuleTemplateEntity::table_name()).set(service::alert::entities::AlertRuleTemplateEntity::column_name<"deleted_at">(), removal.call("now")).set(service::alert::entities::AlertRuleTemplateEntity::column_name<"updated_at">(), removal.call("now")).and_where(removal.binary(removal.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"id">()), Op::equal, removal.cast(removal.value(id), Type::uuid))).and_where(removal.unary(ruvia::db_unary_operator::is_null, removal.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"deleted_at">())));
        (void)co_await c.db().execute(removal);
    }

    template <typename Context>
    ruvia::task<std::string> applyTemplate(Context& c, const ApplyTemplateInput& input) {
        const auto templateId = input.get<"templateId">().view();
        const auto deviceIds = uniqueAlertIds(input.get<"deviceIds">());
        co_await requireTemplate(c, templateId);
        Query selected(c.pool());
        selected.select(selected.star()).from(service::alert::entities::AlertRuleTemplateEntity::table_name()).and_where(selected.binary(selected.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"id">()), Op::equal, selected.cast(selected.value(templateId), Type::uuid))).and_where(selected.unary(ruvia::db_unary_operator::is_null, selected.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"deleted_at">())));
        Query requested(c.pool());
        for (const auto& id : deviceIds) {
            requested.values({ requested.cast(requested.value(id), Type::uuid) });
        }
        Query existing(c.pool());
        const auto existingName = existing.binary(existing.binary(existing.column("name", "selected"), Op::concat, existing.value(" - ")), Op::concat, existing.column(service::alert::entities::DeviceEntity::column_name<"name">(), "device"));
        existing.select(existing.cast(existing.value(1), Type::integer)).from(service::alert::entities::AlertRuleEntity::table_name(), "existing").and_where(existing.binary(existing.column(service::alert::entities::AlertRuleEntity::column_name<"device_id">(), "existing"), Op::equal, existing.column(service::alert::entities::DeviceEntity::column_name<"id">(), "device"))).and_where(existing.binary(existing.column(service::alert::entities::AlertRuleEntity::column_name<"name">(), "existing"), Op::equal, existingName)).and_where(existing.unary(ruvia::db_unary_operator::is_null, existing.column(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">(), "existing")));
        Query source(c.pool());
        const auto name = source.binary(source.binary(source.column("name", "selected"), Op::concat, source.value(" - ")), Op::concat, source.column(service::alert::entities::DeviceEntity::column_name<"name">(), "device"));
        source.select({ source.call("gen_random_uuid"), name, source.column(service::alert::entities::DeviceEntity::column_name<"id">(), "device"), source.column("severity", "selected"), source.column("conditions", "selected"), source.column("logic", "selected"), source.column("silence_duration", "selected"), source.column("recovery_condition", "selected"), source.column("recovery_wait_seconds", "selected"), source.cast(source.value("enabled"), { .custom_name_ = "status_enum" }), source.column("description", "selected"), source.cast(source.value(c.userId), Type::uuid) })
            .from("selected")
            .join(ruvia::db_join_type::cross, "requested")
            .join(ruvia::db_join_type::inner, service::alert::entities::DeviceEntity::table_name(), source.binary(source.column(service::alert::entities::DeviceEntity::column_name<"id">(), "device"), Op::equal, source.column("device_id", "requested")))
            .and_where(source.unary(ruvia::db_unary_operator::is_null, source.column(service::alert::entities::DeviceEntity::column_name<"deleted_at">(), "device")))
            .and_where(source.unary(ruvia::db_unary_operator::not_value, source.exists(existing)));
        Query created(c.pool());
        created.insert_into(service::alert::entities::AlertRuleEntity::table_name(), { "id", "name", "device_id", "severity", "conditions", "logic", "silence_duration", "recovery_condition", "recovery_wait_seconds", "status", "remark", "created_by" })
            .insert_from(source)
            .returning({ created.column(service::alert::entities::AlertRuleEntity::column_name<"id">()) });
        Query count(c.pool());
        count.select(count.aggregate("count", { count.star() })).from("created");
        Query ids(c.pool());
        ids.select(ids.aggregate("jsonb_agg", { ids.column("id") })).from("created");
        Query response(c.pool());
        response.with("selected", selected).with("requested", requested, { .columns_ = { "device_id" } }).with("created", created).select(response.cast(response.call("jsonb_build_object", { response.cast(response.value("success"), Type::text), response.subquery(count), response.cast(response.value("total"), Type::text), response.cast(response.value(static_cast<std::int64_t>(deviceIds.size())), Type::integer), response.cast(response.value("createdIds"), Type::text), response.coalesce({ response.subquery(ids), response.cast(response.value("[]"), Type::jsonb) }) }), Type::text));
        const auto result = co_await c.db().query(response);
        (void)co_await service::rpc::call(c, "alert", "refresh", "{}");
        co_return firstJson(result);
    }

    template <typename Context>
    ruvia::task<std::string> listRecords(Context& c, const AlertListQuery& parameters) {
        Query filtered(c.pool());
        filtered.select(filtered.star("record")).from(service::alert::entities::OpenAlertRecordEntity::table_name(), "record");
        appendUuidFilter(parameters.get<"deviceId">(), "deviceId", filtered, filtered.column(service::alert::entities::OpenAlertRecordEntity::column_name<"device_id">(), "record"));
        appendUuidFilter(parameters.get<"ruleId">(), "ruleId", filtered, filtered.column(service::alert::entities::OpenAlertRecordEntity::column_name<"rule_id">(), "record"));
        appendTextFilter(parameters.get<"status">(), "status", filtered, filtered.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">(), "record"));
        appendTextFilter(parameters.get<"severity">(), "severity", filtered, filtered.column(service::alert::entities::OpenAlertRecordEntity::column_name<"severity">(), "record"));
        Query listed(c.pool());
        listed.select({ listed.star("page_rows"), listed.alias(listed.column(service::alert::entities::AlertRuleEntity::column_name<"name">(), "rule"), "rule_name"), listed.alias(listed.column(service::alert::entities::DeviceEntity::column_name<"name">(), "device"), "device_name") })
            .from("page_rows")
            .join(ruvia::db_join_type::left, service::alert::entities::AlertRuleEntity::table_name(), listed.binary(listed.column(service::alert::entities::AlertRuleEntity::column_name<"id">(), "rule"), Op::equal, listed.column("rule_id", "page_rows")), "rule")
            .join(ruvia::db_join_type::inner, service::alert::entities::DeviceEntity::table_name(), listed.binary(listed.column(service::alert::entities::DeviceEntity::column_name<"id">(), "device"), Op::equal, listed.column("device_id", "page_rows")));
        co_return co_await pageResult(c, parameters, filtered, listed, { "id", "rule_id", "rule_name", "device_id", "device_name", "severity", "status", "message", "detail", "triggered_at", "acknowledged_at", "acknowledged_by", "resolved_at" }, "triggered_at");
    }

    template <typename Context>
    ruvia::task<void> acknowledge(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "告警记录 ID 无效");
        Query acknowledgement(c.pool());
        acknowledgement.update(service::alert::entities::OpenAlertRecordEntity::table_name()).set(service::alert::entities::OpenAlertRecordEntity::column_name<"status">(), acknowledgement.value("acknowledged")).set(service::alert::entities::OpenAlertRecordEntity::column_name<"acknowledged_at">(), acknowledgement.call("now")).set(service::alert::entities::OpenAlertRecordEntity::column_name<"acknowledged_by">(), acknowledgement.cast(acknowledgement.value(c.userId), Type::uuid)).set(service::alert::entities::OpenAlertRecordEntity::column_name<"updated_at">(), acknowledgement.call("now")).and_where(acknowledgement.binary(acknowledgement.column(service::alert::entities::OpenAlertRecordEntity::column_name<"id">()), Op::equal, acknowledgement.cast(acknowledgement.value(id), Type::uuid))).and_where(acknowledgement.binary(acknowledgement.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">()), Op::equal, acknowledgement.value("active")));
        const auto result = co_await c.db().execute(acknowledgement);
        if (result.affected_rows() == 0) {
            service::common::fail(17003, "告警记录不存在或已处理", 404);
        }
    }

    template <typename Context>
    ruvia::task<void> batchAcknowledge(Context& c, const std::vector<std::string>& ids) {
        Query acknowledgement(c.pool());
        acknowledgement.update(service::alert::entities::OpenAlertRecordEntity::table_name()).set(service::alert::entities::OpenAlertRecordEntity::column_name<"status">(), acknowledgement.value("acknowledged")).set(service::alert::entities::OpenAlertRecordEntity::column_name<"acknowledged_at">(), acknowledgement.call("now")).set(service::alert::entities::OpenAlertRecordEntity::column_name<"acknowledged_by">(), acknowledgement.cast(acknowledgement.value(c.userId), Type::uuid)).set(service::alert::entities::OpenAlertRecordEntity::column_name<"updated_at">(), acknowledgement.call("now")).and_where(acknowledgement.binary(acknowledgement.column(service::alert::entities::OpenAlertRecordEntity::column_name<"id">()), Op::in, uuidList(acknowledgement, ids))).and_where(acknowledgement.binary(acknowledgement.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">()), Op::equal, acknowledgement.value("active")));
        (void)co_await c.db().execute(acknowledgement);
    }

    template <typename Context>
    ruvia::task<std::string> stats(Context& c) {
        Query unresolved(c.pool());
        const auto count = unresolved.aggregate("count", { unresolved.star() });
        unresolved.select(unresolved.alias(count, "total"));
        for (const auto severity : { "critical", "warning", "info" }) {
            unresolved.add_select(unresolved.alias(unresolved.filter(count, unresolved.binary(unresolved.column(service::alert::entities::OpenAlertRecordEntity::column_name<"severity">()), Op::equal, unresolved.value(severity))), severity));
        }
        unresolved.add_select(unresolved.alias(unresolved.aggregate("count", { unresolved.column(service::alert::entities::OpenAlertRecordEntity::column_name<"device_id">()) }, true), "affected_devices"))
            .from(service::alert::entities::OpenAlertRecordEntity::table_name())
            .and_where(unresolved.binary(unresolved.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">()), Op::in, unresolved.list({ unresolved.value("active"), unresolved.value("acknowledged") })));
        Query todayNew(c.pool());
        todayNew.select(todayNew.alias(todayNew.aggregate("count", { todayNew.star() }), "total")).from(service::alert::entities::OpenAlertRecordEntity::table_name()).and_where(todayNew.binary(todayNew.column(service::alert::entities::OpenAlertRecordEntity::column_name<"triggered_at">()), Op::greater_equal, todayNew.cast(todayNew.call("now"), Type::date)));
        Query acknowledged(c.pool());
        acknowledged.select(acknowledged.alias(acknowledged.aggregate("count", { acknowledged.star() }), "total")).from(service::alert::entities::OpenAlertRecordEntity::table_name()).and_where(acknowledged.binary(acknowledged.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">()), Op::equal, acknowledged.value("acknowledged")));
        Query todayResolved(c.pool());
        todayResolved.select(todayResolved.alias(todayResolved.aggregate("count", { todayResolved.star() }), "total")).from(service::alert::entities::OpenAlertRecordEntity::table_name()).and_where(todayResolved.binary(todayResolved.column(service::alert::entities::OpenAlertRecordEntity::column_name<"status">()), Op::equal, todayResolved.value("resolved"))).and_where(todayResolved.binary(todayResolved.column(service::alert::entities::OpenAlertRecordEntity::column_name<"resolved_at">()), Op::greater_equal, todayResolved.cast(todayResolved.call("now"), Type::date)));
        Query result(c.pool());
        std::vector<ruvia::db_expression> values;
        for (const auto field : { "total", "critical", "warning", "info", "affected_devices" }) {
            values.push_back(result.cast(result.value(field), Type::text));
            values.push_back(result.column(field, "unresolved"));
        }
        for (const auto& [key, table] : { std::pair{ "today_new", "today_new" }, std::pair{ "acknowledged", "acknowledged_summary" }, std::pair{ "today_resolved", "today_resolved" } }) {
            values.push_back(result.cast(result.value(key), Type::text));
            values.push_back(result.column("total", table));
        }
        result.with("unresolved", unresolved).with("today_new", todayNew).with("acknowledged_summary", acknowledged).with("today_resolved", todayResolved).select(result.cast(result.call("jsonb_build_object", values), Type::text)).from("unresolved").join(ruvia::db_join_type::cross, "today_new").join(ruvia::db_join_type::cross, "acknowledged_summary").join(ruvia::db_join_type::cross, "today_resolved");
        co_return firstJson(co_await c.db().query(result));
    }

  private:
    static ruvia::db_expression uuidList(Query& query, const std::vector<std::string>& ids) {
        std::vector<ruvia::db_expression> values;
        values.reserve(ids.size());
        for (const auto& id : ids) {
            values.push_back(query.cast(query.value(id), Type::uuid));
        }
        return query.list(values);
    }

    template <typename Rows>
    static std::string firstJson(const Rows& rows) {
        if (rows.empty() || rows.front().empty() || !rows.front()[0].value().has_value()) {
            return "{}";
        }
        return std::string(rows.front()[0].value().value_or(std::string_view{}));
    }

    template <typename Rows>
    static std::string firstObject(const Rows& rows, std::string_view notFound) {
        if (rows.empty()) {
            service::common::fail(17003, std::string(notFound), 404);
        }
        return std::string(rows.front()[0].value().value_or(std::string_view{}));
    }

    static ruvia::db_expression fieldJson(Query& query, const std::vector<std::string_view>& fields) {
        std::vector<ruvia::db_expression> values;
        values.reserve(fields.size() * 2);
        for (const auto field : fields) {
            values.push_back(query.cast(query.value(field), Type::text));
            auto value = query.column(field);
            if (field.ends_with("_at") || field == "latest_trigger_time") {
                value = query.call("iot_utc_timestamp", { value });
            }
            values.push_back(value);
        }
        return query.call("jsonb_build_object", values);
    }

    template <typename Context>
    static ruvia::task<std::string> pageResult(Context& c, const AlertListQuery& parameters, const Query& filtered, const Query& listed, std::vector<std::string_view> fields, std::string_view timeColumn) {
        service::common::Page pagination;
        pagination.page = static_cast<std::int64_t>(*parameters.get<"page">());
        pagination.pageSize = static_cast<std::int64_t>(*parameters.get<"pageSize">());
        if (pagination.page > INT64_MAX / pagination.pageSize) {
            service::common::fail(17002, "分页超出范围", 400);
        }
        pagination.offset = (pagination.page - 1) * pagination.pageSize;
        Query counted(c.pool());
        counted.select(counted.alias(counted.aggregate("count", { counted.star() }), "total")).from("filtered");
        Query page(c.pool());
        page.select(page.star()).from("filtered").add_order_by(page.column(timeColumn), ruvia::db_order_direction::desc).add_order_by(page.column("id"), ruvia::db_order_direction::desc).limit(static_cast<std::uint64_t>(pagination.pageSize)).offset(static_cast<std::uint64_t>(pagination.offset));
        Query rows(c.pool());
        const std::vector<ruvia::db_order_term> order{ { rows.column(timeColumn), ruvia::db_order_direction::desc }, { rows.column("id"), ruvia::db_order_direction::desc } };
        rows.select(rows.aggregate("jsonb_agg", { fieldJson(rows, fields) }, false, order)).from("listed");
        Query count(c.pool());
        count.select(count.column("total")).from("counted");
        Query result(c.pool());
        const auto total = result.coalesce({ result.subquery(count), result.value(0) });
        const auto pageSize = result.cast(result.value(pagination.pageSize), Type::big_int);
        const auto pages = result.case_when({ { result.binary(pageSize, Op::equal, result.value(0)), result.value(0) } }, result.cast(result.call("ceil", { result.binary(result.cast(total, Type::numeric), Op::divide, pageSize) }), Type::big_int));
        result.with("filtered", filtered).with("counted", counted).with("page_rows", page).with("listed", listed).select(result.cast(result.call("jsonb_build_object", { result.cast(result.value("list"), Type::text), result.coalesce({ result.subquery(rows), result.cast(result.value("[]"), Type::jsonb) }), result.cast(result.value("total"), Type::text), total, result.cast(result.value("page"), Type::text), result.cast(result.value(pagination.page), Type::big_int), result.cast(result.value("pageSize"), Type::text), pageSize, result.cast(result.value("totalPages"), Type::text), pages }), Type::text));
        co_return firstJson(co_await c.db().query(result));
    }

    static void appendTextFilter(const std::optional<ruvia::string>& value, std::string_view, Query& query, ruvia::db_expression column, bool contains = false) {
        if (!value || value->view().empty()) {
            return;
        }
        query.and_where(query.binary(column, contains ? Op::i_like : Op::equal, query.value(contains ? "%" + std::string(value->view()) + "%" : std::string(value->view()))));
    }

    static void appendUuidFilter(const std::optional<ruvia::string>& value, std::string_view parameter, Query& query, ruvia::db_expression column) {
        if (!value || value->view().empty()) {
            return;
        }
        service::common::requireUuid(19002, value->view(), std::string(parameter) + " 无效");
        query.and_where(query.binary(column, Op::equal, query.cast(query.value(value->view()), Type::uuid)));
    }

    template <typename Context>
    static ruvia::task<void> ensureDevice(Context& c, std::string_view id) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::DeviceEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::DeviceEntity::column_name<"id">()), Op::equal, query.cast(query.value(id), Type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::DeviceEntity::column_name<"deleted_at">())));
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17003, "关联设备不存在", 404);
        }
    }

    template <typename Context>
    static ruvia::task<void> ensureProtocolConfig(Context& c, std::string_view id) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::ProtocolConfigEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::ProtocolConfigEntity::column_name<"id">()), Op::equal, query.cast(query.value(id), Type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::ProtocolConfigEntity::column_name<"deleted_at">())));
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17003, "协议配置不存在", 404);
        }
    }

    template <typename Context>
    static ruvia::task<void> requireRule(Context& c, std::string_view id) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::AlertRuleEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column_name<"id">()), Op::equal, query.cast(query.value(id), Type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">())));
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17003, "告警规则不存在", 404);
        }
    }

    template <typename Context>
    static ruvia::task<void> requireTemplate(Context& c, std::string_view id) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::AlertRuleTemplateEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"id">()), Op::equal, query.cast(query.value(id), Type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"deleted_at">())));
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17003, "告警模板不存在", 404);
        }
    }

    template <typename Context>
    static ruvia::task<void> ensureRuleName(Context& c, std::string_view name, std::string_view deviceId, const std::optional<std::string>& excluded) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::AlertRuleEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column_name<"name">()), Op::equal, query.value(name))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleEntity::column_name<"deleted_at">())));
        query.and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column_name<"device_id">()), Op::equal, query.cast(query.value(deviceId), Type::uuid)));
        if (excluded && !excluded->empty()) {
            query.and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column_name<"id">()), Op::not_equal, query.cast(query.value(*excluded), Type::uuid)));
        }
        const auto rows = co_await c.db().query(query);
        if (!rows.empty()) {
            service::common::fail(17009, "该设备已存在同名告警规则", 409);
        }
    }

    template <typename Context>
    static ruvia::task<void> ensureTemplateName(Context& c, std::string_view name, const std::optional<std::string>& excluded) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::AlertRuleTemplateEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"name">()), Op::equal, query.value(name))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"deleted_at">())));
        if (excluded && !excluded->empty()) {
            query.and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column_name<"id">()), Op::not_equal, query.cast(query.value(*excluded), Type::uuid)));
        }
        const auto rows = co_await c.db().query(query);
        if (!rows.empty()) {
            service::common::fail(17009, "告警模板名称已存在", 409);
        }
    }
};

inline AlertService& alertService() {
    return AlertService::instance();
}

} // namespace service::alert
