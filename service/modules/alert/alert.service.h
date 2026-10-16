#pragma once

#include <tuple>

#include <memory>
#include <algorithm>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <set>
#include <stdexcept>
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
#include "service/modules/alert/alert.entity.h"
#include "service/modules/alert/alert.types.h"
#include "service/modules/device/device.entity.h"
#include "service/modules/protocol/protocol.entity.h"
#include "service/utils/json.h"
#include "service/utils/number.h"
#include "service/utils/rpc_client.h"
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
        filtered.select({ filtered.star("rule"), filtered.alias(filtered.column(service::device::entities::DeviceEntity::column<"name">().name(), "device"), "device_name") })
            .from(service::alert::entities::AlertRuleEntity::table_name(), "rule")
            .join(ruvia::db_join_type::inner, service::device::entities::DeviceEntity::table_name(), filtered.binary(filtered.column(service::device::entities::DeviceEntity::column<"id">().name(), "device"), Op::equal, filtered.column(service::alert::entities::AlertRuleEntity::column<"device_id">().name(), "rule")))
            .and_where(filtered.unary(ruvia::db_unary_operator::is_null, filtered.column(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name(), "rule")));
        appendTextFilter(parameters.get<"keyword">(), "keyword", filtered, filtered.column(service::alert::entities::AlertRuleEntity::column<"name">().name(), "rule"), true);
        appendUuidFilter(parameters.get<"deviceId">(), "deviceId", filtered, filtered.column(service::alert::entities::AlertRuleEntity::column<"device_id">().name(), "rule"));
        appendTextFilter(parameters.get<"severity">(), "severity", filtered, filtered.column(service::alert::entities::AlertRuleEntity::column<"severity">().name(), "rule"));
        appendTextFilter(parameters.get<"status">(), "status", filtered, filtered.cast(filtered.column(service::alert::entities::AlertRuleEntity::column<"status">().name(), "rule"), Type::text));
        Query listed(c.pool());
        listed.select(listed.star()).from("page_rows");
        const std::vector<std::string_view> fields{ service::alert::entities::AlertRuleEntity::column<"id">().name(), service::alert::entities::AlertRuleEntity::column<"name">().name(), service::alert::entities::AlertRuleEntity::column<"device_id">().name(), "device_name", service::alert::entities::AlertRuleEntity::column<"severity">().name(), service::alert::entities::AlertRuleEntity::column<"conditions">().name(), service::alert::entities::AlertRuleEntity::column<"logic">().name(), service::alert::entities::AlertRuleEntity::column<"silence_duration">().name(), service::alert::entities::AlertRuleEntity::column<"recovery_condition">().name(), service::alert::entities::AlertRuleEntity::column<"recovery_wait_seconds">().name(), service::alert::entities::AlertRuleEntity::column<"status">().name(), service::alert::entities::AlertRuleEntity::column<"remark">().name(), service::alert::entities::AlertRuleEntity::column<"created_at">().name(), service::alert::entities::AlertRuleEntity::column<"updated_at">().name() };
        co_return co_await pageResult(c, parameters, filtered, listed, fields, service::alert::entities::AlertRuleEntity::column<"created_at">().name());
    }

    template <typename Context>
    ruvia::task<void> createRule(Context& c, const RuleInput& input) {
        alertRequest::validateConditions(input.get<"conditions">().view());
        co_await ensureDevice(c, input.get<"deviceId">().view());
        co_await ensureRuleName(c, service::utils::trim(input.get<"name">().view()), input.get<"deviceId">().view(), std::nullopt);
        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        Query query(c.pool());
        query.insert_into(service::alert::entities::AlertRuleEntity::table_name(), { service::alert::entities::AlertRuleEntity::column<"id">().name(), service::alert::entities::AlertRuleEntity::column<"name">().name(), service::alert::entities::AlertRuleEntity::column<"device_id">().name(), service::alert::entities::AlertRuleEntity::column<"severity">().name(), service::alert::entities::AlertRuleEntity::column<"conditions">().name(), service::alert::entities::AlertRuleEntity::column<"logic">().name(), service::alert::entities::AlertRuleEntity::column<"silence_duration">().name(), service::alert::entities::AlertRuleEntity::column<"recovery_condition">().name(), service::alert::entities::AlertRuleEntity::column<"recovery_wait_seconds">().name(), service::alert::entities::AlertRuleEntity::column<"status">().name(), service::alert::entities::AlertRuleEntity::column<"remark">().name(), service::alert::entities::AlertRuleEntity::column<"created_by">().name() })
            .values({ query.cast(query.value(id), Type::uuid), query.value(service::utils::trim(input.get<"name">().view())), query.cast(query.value(input.get<"deviceId">().view()), Type::uuid), query.value(input.get<"severity">()->view()), query.cast(query.value(input.get<"conditions">().view()), Type::jsonb), query.value(input.get<"logic">()->view()), query.cast(query.value(input.get<"silenceDuration">()->value_), Type::integer), query.value(service::utils::trim(input.get<"recoveryCondition">()->view())), query.cast(query.value(input.get<"recoveryWaitSeconds">()->value_), Type::integer), query.cast(query.value(input.get<"status">()->view()), { .custom_name_ = "status_enum" }), query.null_if(query.value(service::utils::trim(input.get<"remark">()->view())), query.value("")), query.cast(query.value(c.userId), Type::uuid) });
        (void)co_await c.db().execute(query);
        (void)co_await service::utils::callRpc(c, "alert", "refresh", "{}");
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
            .set(service::alert::entities::AlertRuleEntity::column<"name">().name(), query.value(service::utils::trim(input.get<"name">().view())))
            .set(service::alert::entities::AlertRuleEntity::column<"device_id">().name(), query.cast(query.value(input.get<"deviceId">().view()), Type::uuid))
            .set(service::alert::entities::AlertRuleEntity::column<"severity">().name(), query.value(input.get<"severity">()->view()))
            .set(service::alert::entities::AlertRuleEntity::column<"conditions">().name(), query.cast(query.value(input.get<"conditions">().view()), Type::jsonb))
            .set(service::alert::entities::AlertRuleEntity::column<"logic">().name(), query.value(input.get<"logic">()->view()))
            .set(service::alert::entities::AlertRuleEntity::column<"silence_duration">().name(), query.cast(query.value(input.get<"silenceDuration">()->value_), Type::integer))
            .set(service::alert::entities::AlertRuleEntity::column<"recovery_condition">().name(), query.value(service::utils::trim(input.get<"recoveryCondition">()->view())))
            .set(service::alert::entities::AlertRuleEntity::column<"recovery_wait_seconds">().name(), query.cast(query.value(input.get<"recoveryWaitSeconds">()->value_), Type::integer))
            .set(service::alert::entities::AlertRuleEntity::column<"status">().name(), query.cast(query.value(input.get<"status">()->view()), { .custom_name_ = "status_enum" }))
            .set(service::alert::entities::AlertRuleEntity::column<"remark">().name(), query.null_if(query.value(service::utils::trim(input.get<"remark">()->view())), query.value("")))
            .set(service::alert::entities::AlertRuleEntity::column<"updated_at">().name(), query.call("now"))
            .and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column<"id">().name()), Op::equal, query.cast(query.value(id), Type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name())));
        (void)co_await c.db().execute(query);
        (void)co_await service::utils::callRpc(c, "alert", "refresh", "{}");
    }

    template <typename Context>
    ruvia::task<void> removeRule(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "告警规则 ID 无效");
        co_await requireRule(c, id);
        auto transaction = co_await c.db().begin_transaction();
        Query records(c.pool());
        records.update(service::alert::entities::OpenAlertRecordEntity::table_name()).set(service::alert::entities::OpenAlertRecordEntity::column<"status">().name(), records.value("resolved")).set(service::alert::entities::OpenAlertRecordEntity::column<"resolved_at">().name(), records.call("now")).set(service::alert::entities::OpenAlertRecordEntity::column<"updated_at">().name(), records.call("now")).and_where(records.binary(records.column(service::alert::entities::OpenAlertRecordEntity::column<"rule_id">().name()), Op::equal, records.cast(records.value(id), Type::uuid))).and_where(records.binary(records.column(service::alert::entities::OpenAlertRecordEntity::column<"status">().name()), Op::in, records.list({ records.value("active"), records.value("acknowledged") })));
        (void)co_await transaction.execute(records);
        Query rules(c.pool());
        rules.update(service::alert::entities::AlertRuleEntity::table_name()).set(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name(), rules.call("now")).set(service::alert::entities::AlertRuleEntity::column<"updated_at">().name(), rules.call("now")).and_where(rules.binary(rules.column(service::alert::entities::AlertRuleEntity::column<"id">().name()), Op::equal, rules.cast(rules.value(id), Type::uuid))).and_where(rules.unary(ruvia::db_unary_operator::is_null, rules.column(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name())));
        (void)co_await transaction.execute(rules);
        co_await transaction.commit();
        (void)co_await service::utils::callRpc(c, "alert", "refresh", "{}");
    }

    template <typename Context>
    ruvia::task<void> batchRemoveRules(Context& c, const std::vector<std::string>& ids) {
        auto transaction = co_await c.db().begin_transaction();
        Query records(c.pool());
        records.update(service::alert::entities::OpenAlertRecordEntity::table_name()).set(service::alert::entities::OpenAlertRecordEntity::column<"status">().name(), records.value("resolved")).set(service::alert::entities::OpenAlertRecordEntity::column<"resolved_at">().name(), records.call("now")).set(service::alert::entities::OpenAlertRecordEntity::column<"updated_at">().name(), records.call("now")).and_where(records.binary(records.column(service::alert::entities::OpenAlertRecordEntity::column<"rule_id">().name()), Op::in, uuidList(records, ids))).and_where(records.binary(records.column(service::alert::entities::OpenAlertRecordEntity::column<"status">().name()), Op::in, records.list({ records.value("active"), records.value("acknowledged") })));
        (void)co_await transaction.execute(records);
        Query rules(c.pool());
        rules.update(service::alert::entities::AlertRuleEntity::table_name()).set(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name(), rules.call("now")).set(service::alert::entities::AlertRuleEntity::column<"updated_at">().name(), rules.call("now")).and_where(rules.binary(rules.column(service::alert::entities::AlertRuleEntity::column<"id">().name()), Op::in, uuidList(rules, ids))).and_where(rules.unary(ruvia::db_unary_operator::is_null, rules.column(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name())));
        (void)co_await transaction.execute(rules);
        co_await transaction.commit();
        (void)co_await service::utils::callRpc(c, "alert", "refresh", "{}");
    }

    template <typename Context>
    ruvia::task<std::string> listTemplates(Context& c, const AlertListQuery& parameters) {
        Query filtered(c.pool());
        filtered.select({ filtered.star("template"), filtered.alias(filtered.column(service::protocol::entities::ProtocolConfigEntity::column<"name">().name(), "config"), "config_name"), filtered.alias(filtered.column(service::protocol::entities::ProtocolConfigEntity::column<"protocol">().name(), "config"), "protocol_type") })
            .from(service::alert::entities::AlertRuleTemplateEntity::table_name(), "template")
            .join(ruvia::db_join_type::left, service::protocol::entities::ProtocolConfigEntity::table_name(), filtered.binary(filtered.column(service::protocol::entities::ProtocolConfigEntity::column<"id">().name(), "config"), Op::equal, filtered.column(service::alert::entities::AlertRuleTemplateEntity::column<"protocol_config_id">().name(), "template")), "config")
            .and_where(filtered.unary(ruvia::db_unary_operator::is_null, filtered.column(service::alert::entities::AlertRuleTemplateEntity::column<"deleted_at">().name(), "template")));
        appendTextFilter(parameters.get<"category">(), "category", filtered, filtered.column(service::alert::entities::AlertRuleTemplateEntity::column<"category">().name(), "template"));
        Query listed(c.pool());
        listed.select(listed.star()).from("page_rows");
        const std::vector<std::string_view> fields{ service::alert::entities::AlertRuleTemplateEntity::column<"id">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"name">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"category">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"description">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"severity">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"logic">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"silence_duration">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"protocol_config_id">().name(), "config_name", "protocol_type", service::alert::entities::AlertRuleTemplateEntity::column<"created_at">().name() };
        co_return co_await pageResult(c, parameters, filtered, listed, fields, service::alert::entities::AlertRuleTemplateEntity::column<"created_at">().name());
    }

    template <typename Context>
    ruvia::task<std::string> templateDetail(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "告警模板 ID 无效");
        Query query(c.pool());
        query.select(query.cast(fieldJson(query, { service::alert::entities::AlertRuleTemplateEntity::column<"id">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"name">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"category">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"description">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"severity">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"conditions">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"logic">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"silence_duration">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"recovery_condition">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"recovery_wait_seconds">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"applicable_protocols">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"protocol_config_id">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"created_by">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"created_at">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"updated_at">().name() }), Type::text))
            .from(service::alert::entities::AlertRuleTemplateEntity::table_name())
            .and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column<"id">().name()), Op::equal, query.cast(query.value(id), Type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleTemplateEntity::column<"deleted_at">().name())));
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
        query.insert_into(service::alert::entities::AlertRuleTemplateEntity::table_name(), { service::alert::entities::AlertRuleTemplateEntity::column<"id">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"name">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"category">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"description">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"severity">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"conditions">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"logic">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"silence_duration">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"recovery_condition">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"recovery_wait_seconds">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"applicable_protocols">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"protocol_config_id">().name(), service::alert::entities::AlertRuleTemplateEntity::column<"created_by">().name() })
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
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"name">().name(), query.value(service::utils::trim(input.get<"name">().view())))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"category">().name(), query.null_if(query.value(service::utils::trim(input.get<"category">()->view())), query.value("")))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"description">().name(), query.null_if(query.value(service::utils::trim(input.get<"description">()->view())), query.value("")))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"severity">().name(), query.value(input.get<"severity">()->view()))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"conditions">().name(), query.cast(query.value(input.get<"conditions">().view()), Type::jsonb))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"logic">().name(), query.value(input.get<"logic">()->view()))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"silence_duration">().name(), query.cast(query.value(input.get<"silenceDuration">()->value_), Type::integer))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"recovery_condition">().name(), query.value(service::utils::trim(input.get<"recoveryCondition">()->view())))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"recovery_wait_seconds">().name(), query.cast(query.value(input.get<"recoveryWaitSeconds">()->value_), Type::integer))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"applicable_protocols">().name(), query.cast(query.value(input.get<"applicableProtocols">() ? input.get<"applicableProtocols">()->view() : "[]"), Type::jsonb))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"protocol_config_id">().name(), query.cast(query.null_if(query.value(input.get<"protocolConfigId">()->view()), query.value("")), Type::uuid))
            .set(service::alert::entities::AlertRuleTemplateEntity::column<"updated_at">().name(), query.call("now"))
            .and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column<"id">().name()), Op::equal, query.cast(query.value(id), Type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleTemplateEntity::column<"deleted_at">().name())));
        (void)co_await c.db().execute(query);
    }

    template <typename Context>
    ruvia::task<void> removeTemplate(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "告警模板 ID 无效");
        co_await requireTemplate(c, id);
        Query removal(c.pool());
        removal.update(service::alert::entities::AlertRuleTemplateEntity::table_name()).set(service::alert::entities::AlertRuleTemplateEntity::column<"deleted_at">().name(), removal.call("now")).set(service::alert::entities::AlertRuleTemplateEntity::column<"updated_at">().name(), removal.call("now")).and_where(removal.binary(removal.column(service::alert::entities::AlertRuleTemplateEntity::column<"id">().name()), Op::equal, removal.cast(removal.value(id), Type::uuid))).and_where(removal.unary(ruvia::db_unary_operator::is_null, removal.column(service::alert::entities::AlertRuleTemplateEntity::column<"deleted_at">().name())));
        (void)co_await c.db().execute(removal);
    }

    template <typename Context>
    ruvia::task<std::string> applyTemplate(Context& c, const ApplyTemplateInput& input) {
        const auto templateId = input.get<"templateId">().view();
        const auto deviceIds = uniqueAlertIds(input.get<"deviceIds">());
        co_await requireTemplate(c, templateId);
        Query selected(c.pool());
        selected.select(selected.star()).from(service::alert::entities::AlertRuleTemplateEntity::table_name()).and_where(selected.binary(selected.column(service::alert::entities::AlertRuleTemplateEntity::column<"id">().name()), Op::equal, selected.cast(selected.value(templateId), Type::uuid))).and_where(selected.unary(ruvia::db_unary_operator::is_null, selected.column(service::alert::entities::AlertRuleTemplateEntity::column<"deleted_at">().name())));
        Query requested(c.pool());
        for (const auto& id : deviceIds) {
            requested.values({ requested.cast(requested.value(id), Type::uuid) });
        }
        Query existing(c.pool());
        const auto existingName = existing.binary(existing.binary(existing.column(service::alert::entities::AlertRuleTemplateEntity::column<"name">().name(), "selected"), Op::concat, existing.value(" - ")), Op::concat, existing.column(service::device::entities::DeviceEntity::column<"name">().name(), "device"));
        existing.select(existing.cast(existing.value(1), Type::integer)).from(service::alert::entities::AlertRuleEntity::table_name(), "existing").and_where(existing.binary(existing.column(service::alert::entities::AlertRuleEntity::column<"device_id">().name(), "existing"), Op::equal, existing.column(service::device::entities::DeviceEntity::column<"id">().name(), "device"))).and_where(existing.binary(existing.column(service::alert::entities::AlertRuleEntity::column<"name">().name(), "existing"), Op::equal, existingName)).and_where(existing.unary(ruvia::db_unary_operator::is_null, existing.column(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name(), "existing")));
        Query source(c.pool());
        const auto name = source.binary(source.binary(source.column(service::alert::entities::AlertRuleTemplateEntity::column<"name">().name(), "selected"), Op::concat, source.value(" - ")), Op::concat, source.column(service::device::entities::DeviceEntity::column<"name">().name(), "device"));
        source.select({ source.call("gen_random_uuid"), name, source.column(service::device::entities::DeviceEntity::column<"id">().name(), "device"), source.column(service::alert::entities::AlertRuleTemplateEntity::column<"severity">().name(), "selected"), source.column(service::alert::entities::AlertRuleTemplateEntity::column<"conditions">().name(), "selected"), source.column(service::alert::entities::AlertRuleTemplateEntity::column<"logic">().name(), "selected"), source.column(service::alert::entities::AlertRuleTemplateEntity::column<"silence_duration">().name(), "selected"), source.column(service::alert::entities::AlertRuleTemplateEntity::column<"recovery_condition">().name(), "selected"), source.column(service::alert::entities::AlertRuleTemplateEntity::column<"recovery_wait_seconds">().name(), "selected"), source.cast(source.value("enabled"), { .custom_name_ = "status_enum" }), source.column(service::alert::entities::AlertRuleTemplateEntity::column<"description">().name(), "selected"), source.cast(source.value(c.userId), Type::uuid) })
            .from("selected")
            .join(ruvia::db_join_type::cross, "requested")
            .join(ruvia::db_join_type::inner, service::device::entities::DeviceEntity::table_name(), source.binary(source.column(service::device::entities::DeviceEntity::column<"id">().name(), "device"), Op::equal, source.column("device_id", "requested")))
            .and_where(source.unary(ruvia::db_unary_operator::is_null, source.column(service::device::entities::DeviceEntity::column<"deleted_at">().name(), "device")))
            .and_where(source.unary(ruvia::db_unary_operator::not_value, source.exists(existing)));
        Query created(c.pool());
        created.insert_into(service::alert::entities::AlertRuleEntity::table_name(), { service::alert::entities::AlertRuleEntity::column<"id">().name(), service::alert::entities::AlertRuleEntity::column<"name">().name(), service::alert::entities::AlertRuleEntity::column<"device_id">().name(), service::alert::entities::AlertRuleEntity::column<"severity">().name(), service::alert::entities::AlertRuleEntity::column<"conditions">().name(), service::alert::entities::AlertRuleEntity::column<"logic">().name(), service::alert::entities::AlertRuleEntity::column<"silence_duration">().name(), service::alert::entities::AlertRuleEntity::column<"recovery_condition">().name(), service::alert::entities::AlertRuleEntity::column<"recovery_wait_seconds">().name(), service::alert::entities::AlertRuleEntity::column<"status">().name(), service::alert::entities::AlertRuleEntity::column<"remark">().name(), service::alert::entities::AlertRuleEntity::column<"created_by">().name() })
            .insert_from(source)
            .returning({ created.column(service::alert::entities::AlertRuleEntity::column<"id">().name()) });
        Query count(c.pool());
        count.select(count.aggregate("count", { count.star() })).from("created");
        Query ids(c.pool());
        ids.select(ids.aggregate("jsonb_agg", { ids.column(service::alert::entities::AlertRuleEntity::column<"id">().name()) })).from("created");
        Query response(c.pool());
        response.with("selected", selected).with("requested", requested, { .columns_ = { "device_id" } }).with("created", created).select(response.cast(response.call("jsonb_build_object", { response.cast(response.value("success"), Type::text), response.subquery(count), response.cast(response.value("total"), Type::text), response.cast(response.value(static_cast<std::int64_t>(deviceIds.size())), Type::integer), response.cast(response.value("createdIds"), Type::text), response.coalesce({ response.subquery(ids), response.cast(response.value("[]"), Type::jsonb) }) }), Type::text));
        const auto result = co_await c.db().query(response);
        (void)co_await service::utils::callRpc(c, "alert", "refresh", "{}");
        co_return firstJson(result);
    }

    template <typename Context>
    ruvia::task<std::string> listRecords(Context& c, const AlertListQuery& parameters) {
        co_return co_await pageResult(c, parameters, recordFilter(c, parameters), recordListing(c), recordFields(), "triggered_at");
    }

    // Records page plus its rows (membership and content of the live records
    // channel); each row is the record's text exactly as it appears in the page.
    template <typename Context>
    ruvia::task<AlertRecordPage> recordPage(Context& c, const AlertListQuery& parameters) {
        AlertRecordPage page{ co_await listRecords(c, parameters), {} };
        const auto parsed = ruvia::json_value::parse(page.json);
        const auto list = parsed ? parsed->template get<ruvia::json_value>("list") : std::nullopt;
        if (!list) {
            throw std::runtime_error("alert record page has no list");
        }
        (void)list->for_each_element([&page](const ruvia::json_value& record) {
            const auto id = record.template get<ruvia::string>("id");
            if (!id) {
                throw std::runtime_error("alert record without id");
            }
            page.rows.push_back({ std::string(id->view()), std::string(record.view()) });
            return true;
        });
        co_return page;
    }

    // Records matching `parameters` filters among `recordIds`, plus records of
    // `visibleIds` that show a changed rule or device; rows have the page row shape.
    template <typename Context>
    ruvia::task<std::vector<AlertRecordRow>> recordRows(Context& c, const AlertListQuery& parameters, const std::vector<std::string>& recordIds, const std::vector<std::string>& ruleIds, const std::vector<std::string>& deviceIds, const std::vector<std::string>& visibleIds) {
        using Record = service::alert::entities::OpenAlertRecordEntity;
        const auto records = validIds(recordIds);
        const auto rules = validIds(ruleIds);
        const auto devices = validIds(deviceIds);
        const auto visible = validIds(visibleIds);
        std::vector<AlertRecordRow> result;
        auto filtered = recordFilter(c, parameters);
        const auto id = filtered.column(Record::column<"id">().name(), "record");
        std::optional<ruvia::db_expression> matches;
        if (!records.empty()) {
            matches = filtered.binary(id, Op::in, uuidList(filtered, records));
        }
        std::optional<ruvia::db_expression> related;
        if (!rules.empty()) {
            related = filtered.binary(filtered.column(Record::column<"rule_id">().name(), "record"), Op::in, uuidList(filtered, rules));
        }
        if (!devices.empty()) {
            const auto device = filtered.binary(filtered.column(Record::column<"device_id">().name(), "record"), Op::in, uuidList(filtered, devices));
            related = related ? filtered.binary(*related, Op::or_value, device) : device;
        }
        if (related && !visible.empty()) {
            const auto shown = filtered.binary(*related, Op::and_value, filtered.binary(id, Op::in, uuidList(filtered, visible)));
            matches = matches ? filtered.binary(*matches, Op::or_value, shown) : shown;
        }
        if (!matches) {
            co_return result;
        }
        filtered.and_where(*matches);
        Query rows(c.pool());
        rows.with("page_rows", filtered)
            .with("listed", recordListing(c))
            .select({ rows.cast(rows.column("id"), Type::text), rows.cast(fieldJson(rows, recordFields()), Type::text) })
            .from("listed");
        for (const auto& row : co_await c.db().query(rows)) {
            result.push_back({ std::string(row[0].value().value_or(std::string_view{})), std::string(row[1].value().value_or(std::string_view{})) });
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> acknowledge(Context& c, std::string_view id) {
        service::common::requireUuid(19002, id, "告警记录 ID 无效");
        Query acknowledgement(c.pool());
        acknowledgement.update(service::alert::entities::OpenAlertRecordEntity::table_name()).set(service::alert::entities::OpenAlertRecordEntity::column<"status">().name(), acknowledgement.value("acknowledged")).set(service::alert::entities::OpenAlertRecordEntity::column<"acknowledged_at">().name(), acknowledgement.call("now")).set(service::alert::entities::OpenAlertRecordEntity::column<"acknowledged_by">().name(), acknowledgement.cast(acknowledgement.value(c.userId), Type::uuid)).set(service::alert::entities::OpenAlertRecordEntity::column<"updated_at">().name(), acknowledgement.call("now")).and_where(acknowledgement.binary(acknowledgement.column(service::alert::entities::OpenAlertRecordEntity::column<"id">().name()), Op::equal, acknowledgement.cast(acknowledgement.value(id), Type::uuid))).and_where(acknowledgement.binary(acknowledgement.column(service::alert::entities::OpenAlertRecordEntity::column<"status">().name()), Op::equal, acknowledgement.value("active")));
        const auto result = co_await c.db().execute(acknowledgement);
        if (result.affected_rows() == 0) {
            service::common::fail(17003, "告警记录不存在或已处理", 404);
        }
    }

    template <typename Context>
    ruvia::task<void> batchAcknowledge(Context& c, const std::vector<std::string>& ids) {
        Query acknowledgement(c.pool());
        acknowledgement.update(service::alert::entities::OpenAlertRecordEntity::table_name()).set(service::alert::entities::OpenAlertRecordEntity::column<"status">().name(), acknowledgement.value("acknowledged")).set(service::alert::entities::OpenAlertRecordEntity::column<"acknowledged_at">().name(), acknowledgement.call("now")).set(service::alert::entities::OpenAlertRecordEntity::column<"acknowledged_by">().name(), acknowledgement.cast(acknowledgement.value(c.userId), Type::uuid)).set(service::alert::entities::OpenAlertRecordEntity::column<"updated_at">().name(), acknowledgement.call("now")).and_where(acknowledgement.binary(acknowledgement.column(service::alert::entities::OpenAlertRecordEntity::column<"id">().name()), Op::in, uuidList(acknowledgement, ids))).and_where(acknowledgement.binary(acknowledgement.column(service::alert::entities::OpenAlertRecordEntity::column<"status">().name()), Op::equal, acknowledgement.value("active")));
        (void)co_await c.db().execute(acknowledgement);
    }

    template <typename Context>
    ruvia::task<std::string> stats(Context& c) {
        Query unresolved(c.pool());
        const auto count = unresolved.aggregate("count", { unresolved.star() });
        unresolved.select(unresolved.alias(count, "total"));
        for (const auto severity : { "critical", "warning", "info" }) {
            unresolved.add_select(unresolved.alias(unresolved.filter(count, unresolved.binary(unresolved.column(service::alert::entities::OpenAlertRecordEntity::column<"severity">().name()), Op::equal, unresolved.value(severity))), severity));
        }
        unresolved.add_select(unresolved.alias(unresolved.aggregate("count", { unresolved.column(service::alert::entities::OpenAlertRecordEntity::column<"device_id">().name()) }, true), "affected_devices"))
            .from(service::alert::entities::OpenAlertRecordEntity::table_name())
            .and_where(unresolved.binary(unresolved.column(service::alert::entities::OpenAlertRecordEntity::column<"status">().name()), Op::in, unresolved.list({ unresolved.value("active"), unresolved.value("acknowledged") })));
        Query todayNew(c.pool());
        todayNew.select(todayNew.alias(todayNew.aggregate("count", { todayNew.star() }), "total")).from(service::alert::entities::OpenAlertRecordEntity::table_name()).and_where(todayNew.binary(todayNew.column(service::alert::entities::OpenAlertRecordEntity::column<"triggered_at">().name()), Op::greater_equal, todayNew.cast(todayNew.call("now"), Type::date)));
        Query acknowledged(c.pool());
        acknowledged.select(acknowledged.alias(acknowledged.aggregate("count", { acknowledged.star() }), "total")).from(service::alert::entities::OpenAlertRecordEntity::table_name()).and_where(acknowledged.binary(acknowledged.column(service::alert::entities::OpenAlertRecordEntity::column<"status">().name()), Op::equal, acknowledged.value("acknowledged")));
        Query todayResolved(c.pool());
        todayResolved.select(todayResolved.alias(todayResolved.aggregate("count", { todayResolved.star() }), "total")).from(service::alert::entities::OpenAlertRecordEntity::table_name()).and_where(todayResolved.binary(todayResolved.column(service::alert::entities::OpenAlertRecordEntity::column<"status">().name()), Op::equal, todayResolved.value("resolved"))).and_where(todayResolved.binary(todayResolved.column(service::alert::entities::OpenAlertRecordEntity::column<"resolved_at">().name()), Op::greater_equal, todayResolved.cast(todayResolved.call("now"), Type::date)));
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
    template <typename Context>
    static Query recordFilter(Context& c, const AlertListQuery& parameters) {
        using Record = service::alert::entities::OpenAlertRecordEntity;
        Query filtered(c.pool());
        filtered.select(filtered.star("record")).from(Record::table_name(), "record");
        appendUuidFilter(parameters.get<"deviceId">(), "deviceId", filtered, filtered.column(Record::column<"device_id">().name(), "record"));
        appendUuidFilter(parameters.get<"ruleId">(), "ruleId", filtered, filtered.column(Record::column<"rule_id">().name(), "record"));
        appendTextFilter(parameters.get<"status">(), "status", filtered, filtered.column(Record::column<"status">().name(), "record"));
        appendTextFilter(parameters.get<"severity">(), "severity", filtered, filtered.column(Record::column<"severity">().name(), "record"));
        return filtered;
    }

    // Rows of the `page_rows` CTE with the rule and device names they display.
    template <typename Context>
    static Query recordListing(Context& c) {
        using Record = service::alert::entities::OpenAlertRecordEntity;
        using Rule = service::alert::entities::AlertRuleEntity;
        using Device = service::device::entities::DeviceEntity;
        Query listed(c.pool());
        listed.select({ listed.star("page_rows"), listed.alias(listed.column(Rule::column<"name">().name(), "rule"), "rule_name"), listed.alias(listed.column(Device::column<"name">().name(), "device"), "device_name") })
            .from("page_rows")
            .join(ruvia::db_join_type::left, Rule::table_name(), listed.binary(listed.column(Rule::column<"id">().name(), "rule"), Op::equal, listed.column(Record::column<"rule_id">().name(), "page_rows")), "rule")
            .join(ruvia::db_join_type::inner, Device::table_name(), listed.binary(listed.column(Device::column<"id">().name(), "device"), Op::equal, listed.column(Record::column<"device_id">().name(), "page_rows")));
        return listed;
    }

    static std::vector<std::string_view> recordFields() {
        using Record = service::alert::entities::OpenAlertRecordEntity;
        return { Record::column<"id">().name(), Record::column<"rule_id">().name(), "rule_name", Record::column<"device_id">().name(), "device_name", Record::column<"severity">().name(), Record::column<"status">().name(), Record::column<"message">().name(), Record::column<"detail">().name(), Record::column<"triggered_at">().name(), Record::column<"acknowledged_at">().name(), Record::column<"acknowledged_by">().name(), Record::column<"resolved_at">().name() };
    }

    // Change-feed ids come from producers outside this request; only UUIDs reach SQL casts.
    static std::vector<std::string> validIds(const std::vector<std::string>& ids) {
        std::vector<std::string> result;
        result.reserve(ids.size());
        for (const auto& id : ids) {
            if (service::common::isUuid(id)) {
                result.push_back(id);
            }
        }
        return result;
    }

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
        query.select(query.cast(query.value(1), Type::integer)).from(service::device::entities::DeviceEntity::table_name()).and_where(query.binary(query.column(service::device::entities::DeviceEntity::column<"id">().name()), Op::equal, query.cast(query.value(id), Type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceEntity::column<"deleted_at">().name())));
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17003, "关联设备不存在", 404);
        }
    }

    template <typename Context>
    static ruvia::task<void> ensureProtocolConfig(Context& c, std::string_view id) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::protocol::entities::ProtocolConfigEntity::table_name()).and_where(query.binary(query.column(service::protocol::entities::ProtocolConfigEntity::column<"id">().name()), Op::equal, query.cast(query.value(id), Type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::protocol::entities::ProtocolConfigEntity::column<"deleted_at">().name())));
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17003, "协议配置不存在", 404);
        }
    }

    template <typename Context>
    static ruvia::task<void> requireRule(Context& c, std::string_view id) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::AlertRuleEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column<"id">().name()), Op::equal, query.cast(query.value(id), Type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name())));
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17003, "告警规则不存在", 404);
        }
    }

    template <typename Context>
    static ruvia::task<void> requireTemplate(Context& c, std::string_view id) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::AlertRuleTemplateEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column<"id">().name()), Op::equal, query.cast(query.value(id), Type::uuid))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleTemplateEntity::column<"deleted_at">().name())));
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17003, "告警模板不存在", 404);
        }
    }

    template <typename Context>
    static ruvia::task<void> ensureRuleName(Context& c, std::string_view name, std::string_view deviceId, const std::optional<std::string>& excluded) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::AlertRuleEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column<"name">().name()), Op::equal, query.value(name))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleEntity::column<"deleted_at">().name())));
        query.and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column<"device_id">().name()), Op::equal, query.cast(query.value(deviceId), Type::uuid)));
        if (excluded && !excluded->empty()) {
            query.and_where(query.binary(query.column(service::alert::entities::AlertRuleEntity::column<"id">().name()), Op::not_equal, query.cast(query.value(*excluded), Type::uuid)));
        }
        const auto rows = co_await c.db().query(query);
        if (!rows.empty()) {
            service::common::fail(17009, "该设备已存在同名告警规则", 409);
        }
    }

    template <typename Context>
    static ruvia::task<void> ensureTemplateName(Context& c, std::string_view name, const std::optional<std::string>& excluded) {
        Query query(c.pool());
        query.select(query.cast(query.value(1), Type::integer)).from(service::alert::entities::AlertRuleTemplateEntity::table_name()).and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column<"name">().name()), Op::equal, query.value(name))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::alert::entities::AlertRuleTemplateEntity::column<"deleted_at">().name())));
        if (excluded && !excluded->empty()) {
            query.and_where(query.binary(query.column(service::alert::entities::AlertRuleTemplateEntity::column<"id">().name()), Op::not_equal, query.cast(query.value(*excluded), Type::uuid)));
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
