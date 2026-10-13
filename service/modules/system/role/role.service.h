#pragma once

#include <memory>
#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/modules/system/role/role.entity.h"
#include "service/modules/system/role/role.types.h"
#include "service/modules/system/user/user.entity.h"
#include "service/utils/json.h"

namespace service::role {

namespace db = service::common::database;

class RoleService {
  public:
    static RoleService& instance() {
        thread_local RoleService service;
        return service;
    }

    template <typename Context>
    ruvia::task<RolePageDataDto> list(Context& c, std::int64_t page, std::int64_t pageSize, std::optional<std::string> keyword, std::optional<std::string> status) {
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 100);
        auto where = RoleEntity::column<"deleted_at">().is_null();
        if (keyword && !keyword->empty()) {
            const auto pattern = "%" + *keyword + "%";
            where = std::move(where) && (RoleEntity::column<"name">().ilike(pattern) || RoleEntity::column<"code">().ilike(pattern));
        }
        if (status && (*status == "enabled" || *status == "disabled")) {
            where = std::move(where) && RoleEntity::column<"status">() == *status;
        }
        ruvia::db_find_options countOptions;
        countOptions.where_ = std::move(where);
        const auto total = static_cast<std::int64_t>(
            co_await c.db().template get_repository<RoleEntity>().count(countOptions)
        );
        auto query = roleSelect(c);
        query.where(countOptions.where_)
            .order_by("id", ruvia::db_order_direction::desc)
            .take(pageSize)
            .skip((page - 1) * pageSize);
        const auto rows = co_await query.get_many();

        ruvia::boxed_array<RoleItemDto> roles(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& role = roles.emplace(ruvia::model_options{ .resource_ = c.arena() });
            fillBase(role, row);
            role.set<"permissions">(co_await loadPermissions(c, role.get<"id">()->view()));
        }
        RolePageDataDto result(ruvia::model_options{ .resource_ = c.arena() });
        result.set<"list">(std::move(roles))
            .template set<"total">(total)
            .template set<"page">(page)
            .template set<"pageSize">(pageSize)
            .template set<"totalPages">(total == 0 ? 0 : (total + pageSize - 1) / pageSize);
        co_return result;
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<RoleOptionDto>> options(Context& c) {
        ruvia::db_find_options options;

        options.where_ = RoleEntity::column<"status">() == "enabled" &&
            RoleEntity::column<"deleted_at">().is_null();
        options.order_ = { { "id" } };
        const auto rows = co_await c.db().template get_repository<RoleEntity>().find(options);
        ruvia::boxed_array<RoleOptionDto> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = result.emplace(ruvia::model_options{ .resource_ = c.arena() });
            item.set<"id">(row.template get<"id">())
                .template set<"name">(row.template get<"name">())
                .template set<"code">(row.template get<"code">());
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> create(Context& c, const CreateRoleBody& body) {
        const std::string code(body.get<"code">().view());
        co_await ensureCodeAvailable(c, code, std::nullopt);
        const std::string name(body.get<"name">().view());
        const std::string description =
            body.get<"description">() ? std::string(body.get<"description">()->view()) : "";
        const std::string status =
            body.get<"status">() ? std::string(body.get<"status">()->view()) : "enabled";
        const std::string permissions = permissionsJson(body.get<"permissions">());
        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        RoleEntity role(c.pool());
        role.set<"id">(id);
        role.set<"name">(name);
        role.set<"code">(code);
        role.set<"status">(status);
        role.set<"permissions">(permissions);
        if (description.empty()) {
            role.set_null<"description">();
        } else {
            role.set<"description">(description);
        }
        (void)co_await c.db().template get_repository<RoleEntity>().insert(role);
    }

    template <typename Context>
    ruvia::task<void> update(Context& c, std::string_view id, const UpdateRoleBody& body) {
        ruvia::db_find_options existingOptions;

        existingOptions.where_ = db::activeId<RoleEntity>(id);
        const auto existing = co_await c.db().template get_repository<RoleEntity>().find_one(existingOptions);
        if (!existing) {
            service::common::fail(13001, "角色不存在", 404);
        }
        if (existing->template get<"code">() == service::role::kSuperAdminRoleCode) {
            service::common::fail(13003, "内置超级管理员角色不能修改", 400);
        }
        if (body.get<"code">()) {
            co_await ensureCodeAvailable(c, std::string(body.get<"code">()->view()), std::string(id));
        }

        ruvia::db_expressions query(c.pool());
        std::vector<ruvia::db_assignment> changes;
        bool changed = false;
        auto append = [&](std::string_view column, std::string_view value) {
            changes.push_back({ std::string(column), query.value(value) });
            changed = true;
        };
        if (body.get<"name">()) {
            append("name", body.get<"name">()->view());
        }
        if (body.get<"code">()) {
            append("code", body.get<"code">()->view());
        }
        if (body.get<"description">()) {
            append("description", body.get<"description">()->view());
        }
        if (body.get<"status">()) {
            append("status", body.get<"status">()->view());
        }
        if (body.get<"permissions">()) {
            changes.push_back({ "permissions", query.cast(query.value(permissionsJson(body.get<"permissions">())), ruvia::db_data_type::jsonb) });
            changed = true;
        }
        if (!changed) {
            co_return;
        }
        changes.push_back({ "updated_at", query.call("now") });
        (void)co_await c.db().template get_repository<RoleEntity>().update(RoleEntity::column<"id">() == id, changes);
    }

    template <typename Context>
    ruvia::task<void> remove(Context& c, std::string_view id) {
        ruvia::db_find_options options;

        options.where_ = db::activeId<RoleEntity>(id);
        const auto role = co_await c.db().template get_repository<RoleEntity>().find_one(options);
        if (!role) {
            service::common::fail(13001, "角色不存在", 404);
        }
        if (role->template get<"code">() == service::role::kSuperAdminRoleCode) {
            service::common::fail(13003, "内置超级管理员角色不能删除", 400);
        }
        ruvia::db_expressions expressions(c.pool());
        auto assigned = c.db().template get_repository<service::user::UserRoleEntity>().create_query_builder("ur");
        assigned.template join<service::user::UserEntity>(ruvia::db_join_type::inner, "u", expressions.binary(expressions.column("id", "u"), ruvia::db_binary_operator::equal, expressions.column("user_id", "ur")))
            .where(service::user::UserRoleEntity::column<"role_id">() == id)
            .and_where(expressions.unary(ruvia::db_unary_operator::is_null, expressions.column("deleted_at", "u")));
        if (co_await assigned.get_exists()) {
            service::common::fail(13004, "角色仍有用户使用，不能删除", 409);
        }
        const ruvia::db_assignment deletionChanges[]{
            { "deleted_at", expressions.call("now") }, { "updated_at", expressions.call("now") }
        };
        (void)co_await c.db().template get_repository<RoleEntity>().update(
            RoleEntity::column<"id">() == id, deletionChanges
        );
    }

  private:
    template <typename Context>
    static ruvia::db_query_builder<RoleEntity, ruvia::db_handle> roleSelect(Context& c) {
        ruvia::db_expressions expressions(c.pool());
        auto roles = c.db().template get_repository<RoleEntity>().create_query_builder("role");
        roles.select({ { "id" }, { "name" }, { "code" }, { "description", expressions.coalesce({ expressions.column("description", "role"), expressions.value("") }) }, { "status" }, { "created_at", expressions.call("iot_utc_timestamp", { expressions.column("created_at", "role") }) }, { "updated_at", expressions.call("iot_utc_timestamp", { expressions.column("updated_at", "role") }) } });
        return roles;
    }

    static void fillBase(RoleItemDto& item, const RoleEntity& row) {
        item.set<"id">(row.template get<"id">());
        item.set<"name">(row.template get<"name">());
        item.set<"code">(row.template get<"code">());
        item.set<"description">(row.template get<"description">());
        item.set<"status">(row.template get<"status">());
        item.set<"createdAt">(row.template get<"created_at">());
        item.set<"updatedAt">(row.template get<"updated_at">());
    }

    static std::string
    permissionsJson(const std::optional<ruvia::array<ruvia::string>>& permissions) {
        if (!permissions) {
            return "[]";
        }
        std::string result{ "[" };
        for (const auto& permission : *permissions) {
            const auto value = permission.view();
            if (value.empty() || value.size() > 128) {
                service::common::fail(13005, "权限编码不能为空且不能超过 128 个字符", 400);
            }
            if (value.find(',') != std::string_view::npos) {
                service::common::fail(13005, "权限编码不能包含逗号", 400);
            }
            if (result.size() > 1) {
                result += ',';
            }
            result += service::utils::jsonQuoted(value);
        }
        return result + "]";
    }

    template <typename Context>
    ruvia::task<ruvia::array<ruvia::string>> loadPermissions(Context& c, std::string_view id) {
        ruvia::db_expressions expressions(c.pool());
        auto query = c.db().template get_repository<RoleEntity>().create_query_builder("role");
        query.select({ { "permission", expressions.column("permission", "values") } })
            .join_function(ruvia::db_join_type::cross, expressions.call("jsonb_array_elements_text", { expressions.column("permissions", "role") }), "values", {}, { .lateral_ = true, .columns_ = { { .name_ = "permission" } } })
            .where(RoleEntity::column<"id">() == id)
            .order_by(expressions.column("permission", "values"));
        const auto rows = co_await query.template get_many<RolePermission>();
        ruvia::array<ruvia::string> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            result.emplace_back(row.template get<"permission">(), ruvia::model_options{ .resource_ = c.arena() });
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> ensureCodeAvailable(Context& c, const std::string& code, std::optional<std::string> excludedId) {
        ruvia::db_find_options options;
        options.where_ = RoleEntity::column<"code">() == code;
        if (excludedId) {
            options.where_ = std::move(options.where_) && RoleEntity::column<"id">() != *excludedId;
        }
        if (co_await c.db().template get_repository<RoleEntity>().exists(options)) {
            service::common::fail(13002, "角色编码已存在", 409);
        }
    }
};

inline RoleService& roleService() {
    return RoleService::instance();
}

} // namespace service::role
