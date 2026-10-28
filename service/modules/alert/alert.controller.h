#pragma once
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/permission.h"
#include "service/middleware/validation.h"
#include "service/modules/alert/alert.service.h"
#include "service/modules/alert/alert.types.h"

namespace service::alert {
class AlertController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/alert", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(AlertController)
    RUVIA_GET("/rules", rules, service::middleware::PermissionMiddleware<"iot:alert:query">, ruvia::query_model<AlertListQuery>);
    RUVIA_GET("/templates", templates, service::middleware::PermissionMiddleware<"iot:alert:query">, ruvia::query_model<AlertListQuery>);
    RUVIA_GET("/events", alertEvents, service::middleware::PermissionMiddleware<"iot:alert:query">, ruvia::query_model<AlertListQuery>);
    RUVIA_GET("/templates/:id", templateDetail, service::middleware::PermissionMiddleware<"iot:alert:query">, ruvia::path_model<AlertIdParams>);
    RUVIA_POST("/rules", createRule, service::middleware::PermissionMiddleware<"iot:alert:add">, service::middleware::ValidationErrorCodeMiddleware<17002>, ruvia::json_body<RuleInput>);
    RUVIA_DELETE("/rules", batchRemoveRules, service::middleware::PermissionMiddleware<"iot:alert:delete">, service::middleware::ValidationErrorCodeMiddleware<17002>, ruvia::json_body<AlertBatchBody>);
    RUVIA_POST("/rules/apply-template", applyTemplate, service::middleware::PermissionMiddleware<"iot:alert:add">, service::middleware::ValidationErrorCodeMiddleware<17002>, ruvia::json_body<ApplyTemplateInput>);
    RUVIA_POST("/templates", createTemplate, service::middleware::PermissionMiddleware<"iot:alert:add">, service::middleware::ValidationErrorCodeMiddleware<17002>, ruvia::json_body<TemplateInput>);
    RUVIA_POST("/records/batch-ack", batchAcknowledge, service::middleware::PermissionMiddleware<"iot:alert:ack">, service::middleware::ValidationErrorCodeMiddleware<17002>, ruvia::json_body<AlertBatchBody>);
    RUVIA_PUT("/rules/:id", updateRule, service::middleware::PermissionMiddleware<"iot:alert:edit">, ruvia::path_model<AlertIdParams>, service::middleware::ValidationErrorCodeMiddleware<17002>, ruvia::json_body<RuleInput>);
    RUVIA_DELETE("/rules/:id", removeRule, service::middleware::PermissionMiddleware<"iot:alert:delete">, ruvia::path_model<AlertIdParams>);
    RUVIA_PUT("/templates/:id", updateTemplate, service::middleware::PermissionMiddleware<"iot:alert:edit">, ruvia::path_model<AlertIdParams>, service::middleware::ValidationErrorCodeMiddleware<17002>, ruvia::json_body<TemplateInput>);
    RUVIA_DELETE("/templates/:id", removeTemplate, service::middleware::PermissionMiddleware<"iot:alert:delete">, ruvia::path_model<AlertIdParams>);
    RUVIA_POST("/records/:id/ack", acknowledge, service::middleware::PermissionMiddleware<"iot:alert:ack">, ruvia::path_model<AlertIdParams>);
    RUVIA_ROUTES_END

  private:
    static std::string id(ruvia::context& c) { return std::string(c.req().validated<AlertIdParams>().get<"id">().view()); }

    ruvia::task<ruvia::http_response> rules(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = service::middleware::dataJson(co_await AlertService::instance().listRules(request, c.req().validated<AlertListQuery>()));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<ruvia::http_response> templates(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = service::middleware::dataJson(co_await AlertService::instance().listTemplates(request, c.req().validated<AlertListQuery>()));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<ruvia::http_response> templateDetail(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = service::middleware::dataJson(co_await AlertService::instance().templateDetail(request, id(c)));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<void> alertEvents(ruvia::context& c) {
        namespace topic = service::message::live::topic;

        // 分页记录：成员关系由 LiveCollection 判定；visible 保存本页记录，用于把规则、设备变化映射到本页行。
        struct RecordsState {
            service::middleware::LiveCollection collection{ true };
            std::vector<std::string> visible;
        };

        auto records = std::make_shared<RecordsState>();
        auto recordsSnapshot = [&c, records](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
            co_await service::middleware::requirePermission(request, "iot:alert:query");
            auto page = co_await AlertService::instance().recordPage(request, c.req().validated<AlertListQuery>());
            std::vector<service::middleware::LiveCollection::Row> rows;
            records->visible.clear();
            for (auto& row : page.rows) {
                records->visible.push_back(row.id);
                rows.push_back({ std::move(row.id), std::move(row.json) });
            }
            records->collection.assign(rows);
            co_return service::middleware::dataJson(page.json);
        };
        auto recordsUpdate = [&c, records, recordsSnapshot](service::middleware::RequestContext& request, const service::message::live::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
            co_await service::middleware::requirePermission(request, "iot:alert:query");
            std::vector<std::string> recordIds;
            std::vector<std::string> ruleIds;
            std::vector<std::string> deviceIds;
            for (const auto& [name, ids] : changes.topics) {
                if (!ids) {
                    co_return co_await recordsSnapshot(request);
                }
                auto& target = name == topic::kAlert ? recordIds : name == topic::kAlertRule ? ruleIds
                                                                                             : deviceIds;
                target.insert(target.end(), ids->begin(), ids->end());
            }
            const auto rows = co_await AlertService::instance().recordRows(request, c.req().validated<AlertListQuery>(), recordIds, ruleIds, deviceIds, records->visible);
            service::message::live::ChangeSet::Ids requested(recordIds.begin(), recordIds.end());
            std::vector<service::middleware::LiveCollection::Row> changed;
            changed.reserve(rows.size());
            for (const auto& row : rows) {
                requested.insert(row.id);
                changed.push_back({ row.id, row.json });
            }
            auto answer = records->collection.answer(requested, changed);
            if (answer.outcome == service::middleware::LiveCollection::Outcome::Snapshot) {
                co_return co_await recordsSnapshot(request);
            }
            if (answer.outcome == service::middleware::LiveCollection::Outcome::Unchanged) {
                co_return std::nullopt;
            }
            co_return std::move(answer.payload);
        };
        std::vector<service::middleware::LiveChannel> channels;
        // 记录行显示规则名与设备名，规则、设备变化只刷新本页引用它们的记录。
        channels.push_back({ "records", { std::string(topic::kAlert), std::string(topic::kAlertRule), std::string(topic::kDevice) }, recordsSnapshot, recordsUpdate });
        channels.push_back({ "stats", { std::string(topic::kAlert) }, [](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                co_await service::middleware::requirePermission(request, "iot:alert:query");
                                co_return service::middleware::dataJson(co_await AlertService::instance().stats(request));
                            },
                             {} });
        co_await service::middleware::serveLive(c, service::middleware::requireAuth(c).userId, std::move(channels), [&c] {
            (void)service::middleware::requireAuth(c);
        });
    }

    ruvia::task<ruvia::http_response> createRule(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().createRule(request, c.req().validated<RuleInput>());
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> updateRule(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().updateRule(request, id(c), c.req().validated<RuleInput>());
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> removeRule(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().removeRule(request, id(c));
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> batchRemoveRules(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().batchRemoveRules(request, uniqueAlertIds(c.req().validated<AlertBatchBody>().get<"ids">()));
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> applyTemplate(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = service::middleware::dataJson(co_await AlertService::instance().applyTemplate(request, c.req().validated<ApplyTemplateInput>()));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<ruvia::http_response> createTemplate(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().createTemplate(request, c.req().validated<TemplateInput>());
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> updateTemplate(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().updateTemplate(request, id(c), c.req().validated<TemplateInput>());
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> removeTemplate(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().removeTemplate(request, id(c));
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> acknowledge(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().acknowledge(request, id(c));
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> batchAcknowledge(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await AlertService::instance().batchAcknowledge(request, uniqueAlertIds(c.req().validated<AlertBatchBody>().get<"ids">()));
        co_return c.json(service::common::operation(c, "操作成功"));
    }
};
} // namespace service::alert
