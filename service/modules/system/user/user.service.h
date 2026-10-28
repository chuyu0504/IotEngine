#pragma once
#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/web/db/db_query.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/modules/system/dept/dept.entity.h"
#include "service/modules/system/role/role.entity.h"
#include "service/modules/system/role/role.types.h"
#include "service/modules/system/user/user.entity.h"
#include "service/modules/system/user/user.types.h"
#include "service/utils/password.h"
#include "service/utils/uuid.h"

namespace service::user {

namespace db = service::common::database;

class UserService {
  public:
    static UserService& instance() {
        thread_local UserService service;
        return service;
    }

    template <typename Context>
    ruvia::task<UserPageDataDto> list(Context& c, std::int64_t page, std::int64_t pageSize, std::optional<std::string> keyword, std::optional<std::string> status) {
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 100);
        ruvia::db_find_options options;
        options.where_ = entities::UserEntity::column<"deleted_at">().is_null();
        if (keyword && !keyword->empty()) {
            const auto pattern = "%" + *keyword + "%";
            options.where_ =
                std::move(options.where_) && (entities::UserEntity::column<"username">().ilike(pattern) || entities::UserEntity::column<"nickname">().ilike(pattern) || entities::UserEntity::column<"email">().ilike(pattern));
        }
        if (status && (*status == "enabled" || *status == "disabled")) {
            options.where_ = std::move(options.where_) && entities::UserEntity::column<"status">() == *status;
        }
        const auto total = static_cast<std::int64_t>(
            co_await c.db().template repository<entities::UserEntity>().count(options)
        );
        auto query = userSelect(c);
        query.where(options.where_)
            .order_by(entities::UserEntity::column_name<"id">(), ruvia::db_order_direction::desc)
            .limit(pageSize)
            .offset((page - 1) * pageSize);
        const auto rows = co_await query.template get_many<entities::UserDetails>();

        ruvia::boxed_array<UserItemDto> users(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = users.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            fillBase(item, row);
            item.set<"roles">(co_await loadRoles(c, item.get<"id">()->view()));
        }
        UserPageDataDto result(ruvia::model_options{ .resource_ = c.arena() });
        result.set<"list">(std::move(users))
            .template set<"total">(total)
            .template set<"page">(page)
            .template set<"pageSize">(pageSize)
            .template set<"totalPages">(total == 0 ? 0 : (total + pageSize - 1) / pageSize);
        co_return result;
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<UserOptionDto>> options(Context& c, std::optional<std::string> keyword) {
        auto where = entities::UserEntity::column<"deleted_at">().is_null() &&
            entities::UserEntity::column<"status">() == "enabled";
        if (keyword && !keyword->empty()) {
            const auto pattern = "%" + *keyword + "%";
            where = std::move(where) && (entities::UserEntity::column<"username">().ilike(pattern) || entities::UserEntity::column<"nickname">().ilike(pattern));
        }
        ruvia::db_find_options options;
        options.where_ = std::move(where);

        const std::array<ruvia::db_find_order, 1> findOrder1{ { { "username" } } };
        options.order_ = findOrder1;
        options.take_ = 100;
        const auto rows = co_await c.db().template repository<entities::UserEntity>().find(options);
        ruvia::boxed_array<UserOptionDto> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            item.set<"id">(row.template get<"id">())
                .template set<"username">(row.template get<"username">())
                .template set<"nickname">(row.template is_null<"nickname">() ? std::string_view{} : row.template get<"nickname">());
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> create(Context& c, const CreateUserBody& body) {
        const std::string username(body.get<"username">().view());
        ruvia::db_find_options exists;
        exists.where_ = entities::UserEntity::column<"username">() == username &&
            entities::UserEntity::column<"deleted_at">().is_null();
        if (co_await c.db().template repository<entities::UserEntity>().exists(exists)) {
            service::common::fail(12002, "用户名已存在", 409);
        }
        co_await validateRoles(c, body.get<"roleIds">());
        co_await validateDepartment(c, body.get<"departmentId">());

        const std::string passwordHash =
            service::utils::hashPassword(body.get<"password">().view());
        const std::string nickname =
            body.get<"nickname">() ? std::string(body.get<"nickname">()->view()) : "";
        const std::string phone =
            body.get<"phone">() ? std::string(body.get<"phone">()->view()) : "";
        const std::string email =
            body.get<"email">() ? std::string(body.get<"email">()->view()) : "";
        const std::string status =
            body.get<"status">() ? std::string(body.get<"status">()->view()) : "enabled";
        const std::string departmentId =
            body.get<"departmentId">() ? std::string(body.get<"departmentId">()->view()) : "";
        const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        auto tx = co_await c.db().begin_transaction();
        entities::UserEntity user(c.pool());
        user.set<"id">(id);
        user.set<"username">(username);
        user.set<"password_hash">(passwordHash);
        user.set<"status">(status);
        if (nickname.empty()) {
            user.set_null<"nickname">();
        } else {
            user.set<"nickname">(nickname);
        }
        if (phone.empty()) {
            user.set_null<"phone">();
        } else {
            user.set<"phone">(phone);
        }
        if (email.empty()) {
            user.set_null<"email">();
        } else {
            user.set<"email">(email);
        }
        if (departmentId.empty()) {
            user.set_null<"department_id">();
        } else {
            user.set<"department_id">(departmentId);
        }
        (void)co_await tx.template repository<entities::UserEntity>().insert(user);
        co_await replaceRoles(tx, id, body.get<"roleIds">(), c.pool(), c.template worker_state<service::common::UuidV7State>());
        co_await tx.commit();
    }

    template <typename Context>
    ruvia::task<void> update(Context& c, std::string_view id, const UpdateUserBody& body) {
        ruvia::db_find_options options;
        options.where_ = db::activeId<entities::UserEntity>(id);
        const auto existing = co_await c.db().template repository<entities::UserEntity>().find_one(options);
        if (!existing) {
            service::common::fail(12001, "用户不存在", 404);
        }
        const std::string username(existing->template get<"username">());
        if (username == "admin" && body.get<"status">() &&
            body.get<"status">()->view() != "enabled") {
            service::common::fail(12003, "内置管理员不能被禁用", 400);
        }
        if (body.get<"roleIds">()) {
            co_await validateRoles(c, *body.get<"roleIds">());
            if (username == "admin" && !co_await containsSuperadmin(c, *body.get<"roleIds">())) {
                service::common::fail(12003, "不能移除内置管理员的超级管理员角色", 400);
            }
        }
        if (body.get<"departmentId">()) {
            co_await validateDepartment(c, body.get<"departmentId">());
        }

        ruvia::db_query query(c.pool());
        std::vector<ruvia::db_assignment> changes;
        bool changed = false;
        auto append = [&](std::string_view column, std::string_view value) {
            changes.push_back({ column, query.value(value) });
            changed = true;
        };
        if (body.get<"nickname">()) {
            append(entities::UserEntity::column_name<"nickname">(), body.get<"nickname">()->view());
        }
        if (body.get<"phone">()) {
            append(entities::UserEntity::column_name<"phone">(), body.get<"phone">()->view());
        }
        if (body.get<"email">()) {
            append(entities::UserEntity::column_name<"email">(), body.get<"email">()->view());
        }
        if (body.get<"status">()) {
            append(entities::UserEntity::column_name<"status">(), body.get<"status">()->view());
        }
        if (body.get<"password">()) {
            append(entities::UserEntity::column_name<"password_hash">(), service::utils::hashPassword(body.get<"password">()->view()));
        }
        if (body.get<"departmentId">()) {
            changes.push_back({ entities::UserEntity::column_name<"department_id">(), query.cast(query.null_if(query.value(body.get<"departmentId">()->view()), query.value("")), ruvia::db_data_type::uuid) });
            changed = true;
        }
        auto tx = co_await c.db().begin_transaction();
        if (changed) {
            changes.push_back({ entities::UserEntity::column_name<"updated_at">(), query.call("now") });
            (void)co_await tx.template repository<entities::UserEntity>().update(entities::UserEntity::column<"id">() == id, changes);
        }
        if (body.get<"roleIds">()) {
            co_await replaceRoles(tx, id, *body.get<"roleIds">(), c.pool(), c.template worker_state<service::common::UuidV7State>());
        }
        co_await tx.commit();
    }

    template <typename Context>
    ruvia::task<void> remove(Context& c, std::string_view id, std::string_view operatorId) {
        if (id == operatorId) {
            service::common::fail(12004, "不能删除当前登录用户", 400);
        }
        ruvia::db_find_options options;
        options.where_ = db::activeId<entities::UserEntity>(id);
        const auto user = co_await c.db().template repository<entities::UserEntity>().find_one(options);
        if (!user) {
            service::common::fail(12001, "用户不存在", 404);
        }
        if (user->template get<"username">() == "admin") {
            service::common::fail(12004, "内置管理员不能删除", 400);
        }
        ruvia::db_query expressions(c.pool());
        const ruvia::db_assignment deletionChanges[]{
            { "deleted_at", expressions.call("now") },
            { "updated_at", expressions.call("now") }
        };
        (void)co_await c.db().template repository<entities::UserEntity>().update(
            entities::UserEntity::column<"id">() == id,
            deletionChanges
        );
    }

  private:
    template <typename Context>
    static ruvia::db_query_builder<entities::UserEntity, ruvia::db_handle> userSelect(Context& c) {
        using namespace ruvia;
        using entities::UserEntity;
        db_query expressions(c.pool());
        auto query = c.db().template repository<UserEntity>().query_builder("u");
        query.select({ { "id", UserEntity::column<"id">().expression(expressions, "u") }, { "username", UserEntity::column<"username">().expression(expressions, "u") }, { "nickname", expressions.coalesce({ expressions.cast(UserEntity::column<"nickname">().expression(expressions, "u"), db_data_type::text), expressions.value("") }) }, { "phone", expressions.coalesce({ expressions.cast(UserEntity::column<"phone">().expression(expressions, "u"), db_data_type::text), expressions.value("") }) }, { "email", expressions.coalesce({ expressions.cast(UserEntity::column<"email">().expression(expressions, "u"), db_data_type::text), expressions.value("") }) }, { "status", UserEntity::column<"status">().expression(expressions, "u") }, { "department_id", expressions.coalesce({ expressions.cast(UserEntity::column<"department_id">().expression(expressions, "u"), db_data_type::text), expressions.value("") }) }, { "department_name", expressions.coalesce({ expressions.cast(service::dept::entities::DeptEntity::column<"name">().expression(expressions, "d"), db_data_type::text), expressions.value("") }) }, { "created_at", expressions.call("iot_utc_timestamp", { UserEntity::column<"created_at">().expression(expressions, "u") }) }, { "updated_at", expressions.call("iot_utc_timestamp", { UserEntity::column<"updated_at">().expression(expressions, "u") }) } });
        query.template join<service::dept::entities::DeptEntity>(db_join_type::left, "d", expressions.binary(service::dept::entities::DeptEntity::column<"id">().expression(expressions, "d"), db_binary_operator::equal, UserEntity::column<"department_id">().expression(expressions, "u")));
        return query;
    }

    static void fillBase(UserItemDto& item, const entities::UserDetails& row) {
        item.set<"id">(row.template get<"id">());
        item.set<"username">(row.template get<"username">());
        item.set<"nickname">(row.template get<"nickname">());
        item.set<"phone">(row.template get<"phone">());
        item.set<"email">(row.template get<"email">());
        item.set<"status">(row.template get<"status">());
        item.set<"departmentId">(row.template get<"department_id">());
        item.set<"departmentName">(row.template get<"department_name">());
        item.set<"createdAt">(row.template get<"created_at">());
        item.set<"updatedAt">(row.template get<"updated_at">());
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<service::role::RoleOptionDto>>
    loadRoles(Context& c, std::string_view userId) {
        ruvia::db_query expressions(c.pool());
        auto query = c.db().template repository<service::role::entities::RoleEntity>().query_builder("r");
        query.select({ { "id" }, { "name" }, { "code" } })
            .template join<entities::UserRoleEntity>(ruvia::db_join_type::inner, "ur", expressions.binary(entities::UserRoleEntity::column<"role_id">().expression(expressions, "ur"), ruvia::db_binary_operator::equal, service::role::entities::RoleEntity::column<"id">().expression(expressions, "r")))
            .where(expressions.binary(entities::UserRoleEntity::column<"user_id">().expression(expressions, "ur"), ruvia::db_binary_operator::equal, expressions.value(userId)))
            .and_where(service::role::entities::RoleEntity::column<"deleted_at">().is_null())
            .order_by(service::role::entities::RoleEntity::column_name<"id">());
        const auto rows = co_await query.get_many();
        ruvia::boxed_array<service::role::RoleOptionDto> roles(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& role = roles.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            role.set<"id">(row.template get<"id">())
                .template set<"name">(row.template get<"name">())
                .template set<"code">(row.template get<"code">());
        }
        co_return roles;
    }

    template <typename Context>
    ruvia::task<void> validateRoles(Context& c, const ruvia::array<ruvia::string>& roleIds) {
        std::set<std::string, std::less<>> seenRoleIds;
        for (const auto& roleId : roleIds) {
            if (!service::common::isUuid(roleId.view())) {
                service::common::fail(12005, "角色 ID 必须是 UUID", 400);
            }
            const std::string roleIdValue(roleId.view());
            if (!seenRoleIds.emplace(roleIdValue).second) {
                service::common::fail(12005, "角色不能重复", 400);
            }
            ruvia::db_find_options options;
            options.where_ = db::activeId<service::role::entities::RoleEntity>(roleIdValue) &&
                service::role::entities::RoleEntity::column<"status">() == "enabled";
            if (!co_await c.db().template repository<service::role::entities::RoleEntity>().exists(options)) {
                service::common::fail(12005, "包含无效角色", 400);
            }
        }
    }

    template <typename Context>
    ruvia::task<void> validateDepartment(Context& c, const std::optional<ruvia::string>& departmentId) {
        if (!departmentId || departmentId->view().empty()) {
            co_return;
        }
        ruvia::db_find_options options;
        options.where_ = db::activeId<service::dept::entities::DeptEntity>(departmentId->view()) &&
            service::dept::entities::DeptEntity::column<"status">() == "enabled";
        if (!co_await c.db().template repository<service::dept::entities::DeptEntity>().exists(options)) {
            service::common::fail(12006, "部门不存在或已禁用", 400);
        }
    }

    template <typename Context>
    ruvia::task<bool> containsSuperadmin(Context& c, const ruvia::array<ruvia::string>& roleIds) {
        for (const auto& roleId : roleIds) {
            ruvia::db_find_options options;
            options.where_ =
                db::activeId<service::role::entities::RoleEntity>(roleId.view()) &&
                service::role::entities::RoleEntity::column<"code">() == service::role::kSuperAdminRoleCode;
            if (co_await c.db().template repository<service::role::entities::RoleEntity>().exists(options)) {
                co_return true;
            }
        }
        co_return false;
    }

    static ruvia::task<void> replaceRoles(ruvia::db_transaction& tx, std::string_view userId, const ruvia::array<ruvia::string>& roleIds, std::pmr::memory_resource* resource, service::common::UuidV7State& uuidState) {
        (void)co_await tx.template repository<entities::UserRoleEntity>().delete_by(
            entities::UserRoleEntity::column<"user_id">() == userId
        );
        for (const auto& roleId : roleIds) {
            entities::UserRoleEntity binding(resource);
            binding.set<"id">(service::utils::nextUuid(uuidState));
            binding.set<"user_id">(userId);
            binding.set<"role_id">(roleId.view());
            (void)co_await tx.template repository<entities::UserRoleEntity>().upsert(binding, { .do_nothing_ = true });
        }
    }
};

} // namespace service::user
