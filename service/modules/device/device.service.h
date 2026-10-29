#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/model.h>
#include <ruvia/web/redis/redis.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/modules/device/device.entity.h"
#include "service/modules/device/device.types.h"
#include "service/modules/edge_node/edge_node.entity.h"
#include "service/modules/edge_node/edge_node.service.h"
#include "service/modules/link/link.entity.h"
#include "service/modules/link/link.service.h"
#include "service/modules/protocol/protocol.entity.h"
#include "service/modules/protocol/protocol.service.h"
#include "service/modules/protocol/protocol.types.h"
#include "service/modules/system/auth/auth.entity.h"
#include "service/modules/system/auth/auth.service.h"
#include "service/modules/system/dept/dept.entity.h"
#include "service/modules/system/outbox/outbox.service.h"
#include "service/modules/system/role/role.entity.h"
#include "service/modules/system/user/user.entity.h"
#include "service/utils/json.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"
#include "service/utils/rpc_client.h"
#include "service/utils/uuid.h"

namespace service::device {

namespace db = service::common::database;

// CTE/派生列名（非实体列）。
inline constexpr std::string_view kAccessRankColumn = "access_rank";
inline constexpr std::string_view kNormalizedValuesColumn = "normalized_values";
inline constexpr std::string_view kSubjectTypeColumn = "subject_type";
inline constexpr std::string_view kSubjectIdColumn = "subject_id";
inline constexpr std::string_view kSubjectNameColumn = "subject_name";
inline constexpr std::string_view kInheritedColumn = "inherited";
inline constexpr std::string_view kJsonEachKeyColumn = "key";
inline constexpr std::string_view kJsonEachValueColumn = "value";

class DeviceAccessService {
  public:
    static DeviceAccessService& instance() {
        static thread_local DeviceAccessService service;
        return service;
    }

    // The caller's data scope. Live connections share it until the next
    // authorization change; other requests resolve it per call.
    template <typename Context>
    ruvia::task<DeviceActor> actor(Context& c, std::string_view userId) const {
        auto load = [this, &c, userId]() {
            return resolveActor(c, userId);
        };
        if constexpr (requires { c.template authorization<DeviceActor>(std::string{}, load); }) {
            co_return co_await c.template authorization<DeviceActor>("device-actor:" + std::string(userId), load);
        } else {
            co_return co_await load();
        }
    }

    template <typename Context>
    ruvia::task<DeviceActor> resolveActor(Context& c, std::string_view userId) const {
        ruvia::db_query query(c.pool());
        const auto roleCode = query.column(service::role::entities::RoleEntity::column_name<"code">(), "role");
        const auto isSuperadmin = query.binary(roleCode, ruvia::db_binary_operator::equal, query.value("superadmin"));
        const auto permissions = query.column(service::role::entities::RoleEntity::column_name<"permissions">(), "role");
        const auto hasWildcard = query.binary(
            permissions,
            ruvia::db_binary_operator::json_has_key,
            db::text(query, "*")
        );
        const auto permission = [&](std::string_view value) {
            return query.binary(permissions, ruvia::db_binary_operator::json_has_key, db::text(query, value));
        };
        const auto capability = [&](std::string_view value) {
            return query.binary(
                query.binary(isSuperadmin, ruvia::db_binary_operator::or_value, hasWildcard),
                ruvia::db_binary_operator::or_value,
                permission(value)
            );
        };
        const auto boolOr = [&](ruvia::db_expression expression) {
            return query.coalesce({ query.aggregate("bool_or", { expression }), db::boolean(query, false) });
        };
        auto queryBuilder = c.db().template repository<service::user::entities::UserEntity>().query_builder("actor");
        queryBuilder.select({ { "id", query.coalesce({ db::text(query, query.column(service::dept::entities::DeptEntity::column_name<"id">(), "department")), query.value("") }) }, { "value1", boolOr(isSuperadmin) }, { "value2", boolOr(capability("iot:device:edit")) }, { "value3", boolOr(capability("iot:device:delete")) }, { "value4", boolOr(capability("iot:device:share")) }, { "value5", boolOr(capability("iot:device:command")) }, { "value6", boolOr(capability("iot:device-group:share")) } }).template join<service::dept::entities::DeptEntity>(ruvia::db_join_type::left, "department", db::andAll(query, query.binary(query.column(service::dept::entities::DeptEntity::column_name<"id">(), "department"), ruvia::db_binary_operator::equal, query.column(service::user::entities::UserEntity::column_name<"department_id">(), "actor")), query.binary(query.column(service::dept::entities::DeptEntity::column_name<"status">(), "department"), ruvia::db_binary_operator::equal, query.value("enabled")), query.unary(ruvia::db_unary_operator::is_null, query.column(service::dept::entities::DeptEntity::column_name<"deleted_at">(), "department")))).template join<service::user::entities::UserRoleEntity>(ruvia::db_join_type::left, "user_role", query.binary(query.column(service::user::entities::UserRoleEntity::column_name<"user_id">(), "user_role"), ruvia::db_binary_operator::equal, query.column(service::user::entities::UserEntity::column_name<"id">(), "actor"))).template join<service::role::entities::RoleEntity>(ruvia::db_join_type::left, "role", db::andAll(query, query.binary(query.column(service::role::entities::RoleEntity::column_name<"id">(), "role"), ruvia::db_binary_operator::equal, query.column(service::user::entities::UserRoleEntity::column_name<"role_id">(), "user_role")), query.binary(query.column(service::role::entities::RoleEntity::column_name<"status">(), "role"), ruvia::db_binary_operator::equal, query.value("enabled")), query.unary(ruvia::db_unary_operator::is_null, query.column(service::role::entities::RoleEntity::column_name<"deleted_at">(), "role")))).where(db::andAll(query, query.binary(query.column(service::user::entities::UserEntity::column_name<"id">(), "actor"), ruvia::db_binary_operator::equal, db::uuid(query, userId)), query.binary(query.column(service::user::entities::UserEntity::column_name<"status">(), "actor"), ruvia::db_binary_operator::equal, query.value("enabled")), query.unary(ruvia::db_unary_operator::is_null, query.column(service::user::entities::UserEntity::column_name<"deleted_at">(), "actor")))).group_by({ query.column(service::user::entities::UserEntity::column_name<"id">(), "actor"), query.column(service::dept::entities::DeptEntity::column_name<"id">(), "department") });
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(service::common::kTokenInvalidErrorCode, "用户状态无效", 401);
        }
        const auto& row = rows.front();
        DeviceActor result;
        result.userId = userId;
        result.departmentId = std::string(row[0].value().value_or(std::string_view{}));
        result.superadmin = isTrue(row[1].value().value_or(std::string_view{}));
        result.canEdit = isTrue(row[2].value().value_or(std::string_view{}));
        result.canDelete = isTrue(row[3].value().value_or(std::string_view{}));
        result.canShare = isTrue(row[4].value().value_or(std::string_view{}));
        result.canCommand = isTrue(row[5].value().value_or(std::string_view{}));
        result.canGroupShare = isTrue(row[6].value().value_or(std::string_view{}));
        co_return result;
    }

    template <typename Context>
    ruvia::task<DeviceActor> requireGroupOwner(Context& c, std::string_view groupId, std::string_view userId) const {
        auto currentActor = co_await actor(c, userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceGroupEntity>().query_builder();
        queryBuilder.select({ { "created_by", db::text(query, query.column(service::device::entities::DeviceGroupEntity::column_name<"created_by">())) } }).where(db::andAll(query, query.binary(query.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(query, groupId)), query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">())))).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(17001, "设备分组不存在", 404);
        }
        if (!currentActor.superadmin && rows.front()[0].value().value_or(std::string_view{}) != currentActor.userId) {
            service::common::fail(17005, "只能分享自己创建的设备分组", 403);
        }
        co_return currentActor;
    }

    template <typename Context>
    ruvia::task<DeviceAccessDecision> require(Context& c, std::string_view deviceId, DeviceAccessLevel minimum, std::string_view userId) const {
        auto currentActor = co_await actor(c, userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("device");
        addScopedDevicesCtes(queryBuilder, query, currentActor);

        queryBuilder.select({ { "value0", query.column(kAccessRankColumn, "device") } }).from_cte("scoped_device").where(query.binary(query.column(service::device::entities::DeviceEntity::column_name<"id">(), "device"), ruvia::db_binary_operator::equal, db::uuid(query, deviceId))).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(18001, "设备不存在", 404);
        }
        const auto level = rank(rows.front()[0].value().value_or(std::string_view{}));
        if (level == DeviceAccessLevel::none) {
            service::common::fail(18001, "设备不存在", 404);
        }
        if (level < minimum) {
            service::common::fail(18005, "设备权限不足", 403);
        }
        co_return DeviceAccessDecision{ std::move(currentActor), level };
    }

    // Append the actor-scoped device relations to a query.  The CTEs are deliberately public
    // so callers can compose their own projection, ordering and pagination without embedding SQL.
    static void addScopedDevicesCtes(auto& target, ruvia::db_query& query, const DeviceActor& actor) {
        ruvia::db_query shared(query.resource());
        ruvia::db_query sharedRecursive(query.resource());
        shared.select({ shared.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), "access_grant"), accessLevelRank(shared, shared.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"access_level">(), "access_grant")) })
            .from(service::device::entities::DeviceGroupAccessGrantEntity::table_name(), "access_grant")
            .join(ruvia::db_join_type::inner, service::device::entities::DeviceGroupEntity::table_name(), db::andAll(shared, shared.binary(shared.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "granted_group"), ruvia::db_binary_operator::equal, shared.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), "access_grant")), shared.unary(ruvia::db_unary_operator::is_null, shared.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), "granted_group"))), "granted_group")
            .where(db::andAll(shared, shared.binary(shared.binary(shared.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), "access_grant"), ruvia::db_binary_operator::equal, db::uuid(shared, actor.userId)), ruvia::db_binary_operator::or_value, shared.binary(shared.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"department_id">(), "access_grant"), ruvia::db_binary_operator::equal, db::nullableUuid(shared, actor.departmentId))), shared.unary(ruvia::db_unary_operator::not_value, db::boolean(shared, actor.superadmin))));
        sharedRecursive.select({ sharedRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "child"), sharedRecursive.column(kAccessRankColumn, "shared") })
            .from(service::device::entities::DeviceGroupEntity::table_name(), "child")
            .join(ruvia::db_join_type::inner, "shared_group_access", sharedRecursive.binary(sharedRecursive.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), "shared"), ruvia::db_binary_operator::equal, sharedRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), "child")), "shared")
            .where(sharedRecursive.unary(ruvia::db_unary_operator::is_null, sharedRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), "child")));
        shared.combine(ruvia::db_set_operation::union_value, sharedRecursive);

        const std::array<ruvia::borrowed_text, 2> sourceColumns1{ { "group_id", "access_rank" } };
        target.with("shared_group_access", shared, { .recursive_ = true, .columns_ = sourceColumns1 });

        ruvia::db_query groupAccess(query.resource());
        groupAccess.select(
                       { groupAccess.column(service::device::entities::DeviceEntity::column_name<"group_id">()),
                         groupAccess.alias(
                             groupAccess.aggregate("max", { groupAccess.column(kAccessRankColumn) }),
                             "access_rank"
                         ) }
        )
            .from("shared_group_access")
            .group_by({ groupAccess.column(service::device::entities::DeviceEntity::column_name<"group_id">()) });
        target.with("group_access", groupAccess);

        ruvia::db_query deviceAccess(query.resource());
        deviceAccess
            .select({ deviceAccess.column(service::device::entities::DeviceAccessGrantEntity::column_name<"device_id">()), deviceAccess.alias(deviceAccess.aggregate("max", { accessLevelRank(deviceAccess, deviceAccess.column(service::device::entities::DeviceAccessGrantEntity::column_name<"access_level">(), "access_grant")) }), "access_rank") })
            .from(service::device::entities::DeviceAccessGrantEntity::table_name(), "access_grant")
            .where(db::andAll(deviceAccess, deviceAccess.binary(deviceAccess.binary(deviceAccess.column(service::device::entities::DeviceAccessGrantEntity::column_name<"user_id">(), "access_grant"), ruvia::db_binary_operator::equal, db::uuid(deviceAccess, actor.userId)), ruvia::db_binary_operator::or_value, deviceAccess.binary(deviceAccess.column(service::device::entities::DeviceAccessGrantEntity::column_name<"department_id">(), "access_grant"), ruvia::db_binary_operator::equal, db::nullableUuid(deviceAccess, actor.departmentId))), deviceAccess.unary(ruvia::db_unary_operator::not_value, db::boolean(deviceAccess, actor.superadmin))))
            .group_by({ deviceAccess.column(service::device::entities::DeviceAccessGrantEntity::column_name<"device_id">()) });
        target.with("device_access", deviceAccess);

        ruvia::db_query scoped(query.resource());
        const auto owned = scoped.binary(
            scoped.binary(db::boolean(scoped, actor.superadmin), ruvia::db_binary_operator::or_value, scoped.binary(scoped.column(service::device::entities::DeviceEntity::column_name<"created_by">(), "source"), ruvia::db_binary_operator::equal, db::uuid(scoped, actor.userId))),
            ruvia::db_binary_operator::equal,
            db::boolean(scoped, true)
        );
        const auto inherited = scoped.greatest(
            { scoped.coalesce({ scoped.column(kAccessRankColumn, "device_access"), db::integer(scoped, 0) }),
              scoped.coalesce({ scoped.column(kAccessRankColumn, "group_access"), db::integer(scoped, 0) }) }
        );
        scoped.select({ scoped.column(service::device::entities::DeviceEntity::column_name<"id">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"name">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"protocol_config_id">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"group_id">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"status">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"remark">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"created_by">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"created_at">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"updated_at">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"protocol_address">(), "source"), scoped.column(service::device::entities::DeviceEntity::column_name<"debug_enabled">(), "source"), scoped.case_when({ { owned, db::integer(scoped, 4) } }, inherited) })
            .from(service::device::entities::DeviceEntity::table_name(), "source")
            .join(ruvia::db_join_type::left, "device_access", scoped.binary(scoped.column(service::device::entities::DeviceAccessGrantEntity::column_name<"device_id">(), "device_access"), ruvia::db_binary_operator::equal, scoped.column(service::device::entities::DeviceEntity::column_name<"id">(), "source")), "device_access")
            .join(ruvia::db_join_type::left, "group_access", scoped.binary(scoped.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), "group_access"), ruvia::db_binary_operator::equal, scoped.column(service::device::entities::DeviceEntity::column_name<"group_id">(), "source")), "group_access")
            .where(scoped.unary(ruvia::db_unary_operator::is_null, scoped.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "source")));

        const std::array<ruvia::borrowed_text, 15> sourceColumns2{ { "id", "name", "link_id", "protocol_config_id", "group_id", "status", "protocol_params", "remark", "created_by", "created_at", "updated_at", "deleted_at", "protocol_address", "debug_enabled", "access_rank" } };
        target.with("scoped_device", scoped, { .columns_ = sourceColumns2 });
    }

    static void addVisibleGroupsCtes(auto& target, ruvia::db_query& query, const DeviceActor& actor) {
        addScopedDevicesCtes(target, query, actor);
        ruvia::db_query shared(query.resource());
        ruvia::db_query sharedRecursive(query.resource());
        shared.select(shared.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), "access_grant"))
            .from(service::device::entities::DeviceGroupAccessGrantEntity::table_name(), "access_grant")
            .where(shared.binary(
                shared.binary(shared.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), "access_grant"), ruvia::db_binary_operator::equal, db::uuid(shared, actor.userId)),
                ruvia::db_binary_operator::or_value,
                shared.binary(shared.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"department_id">(), "access_grant"), ruvia::db_binary_operator::equal, db::nullableUuid(shared, actor.departmentId))
            ));
        sharedRecursive.select(sharedRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "child"))
            .from(service::device::entities::DeviceGroupEntity::table_name(), "child")
            .join(ruvia::db_join_type::inner, "shared_group_tree", sharedRecursive.binary(sharedRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "parent"), ruvia::db_binary_operator::equal, sharedRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), "child")), "parent")
            .where(sharedRecursive.unary(ruvia::db_unary_operator::is_null, sharedRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), "child")));
        shared.combine(ruvia::db_set_operation::union_value, sharedRecursive);

        const std::array<ruvia::borrowed_text, 1> sourceColumns3{ { "id" } };
        target.with("shared_group_tree", shared, { .recursive_ = true, .columns_ = sourceColumns3 });

        ruvia::db_query scopedGroups(query.resource());
        scopedGroups.select(scopedGroups.column(service::device::entities::DeviceEntity::column_name<"group_id">(), "scoped"))
            .from("scoped_device", "scoped")
            .where(db::andAll(scopedGroups, scopedGroups.binary(scopedGroups.column(kAccessRankColumn, "scoped"), ruvia::db_binary_operator::greater, db::integer(scopedGroups, 0)), scopedGroups.unary(ruvia::db_unary_operator::is_not_null, scopedGroups.column(service::device::entities::DeviceEntity::column_name<"group_id">(), "scoped"))));
        ruvia::db_query ownedGroups(query.resource());
        ownedGroups.select(ownedGroups.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "owned"))
            .from(service::device::entities::DeviceGroupEntity::table_name(), "owned")
            .where(db::andAll(ownedGroups, ownedGroups.unary(ruvia::db_unary_operator::is_null, ownedGroups.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), "owned")), ownedGroups.binary(ownedGroups.binary(db::boolean(ownedGroups, actor.superadmin), ruvia::db_binary_operator::or_value, ownedGroups.binary(ownedGroups.column(service::device::entities::DeviceGroupEntity::column_name<"created_by">(), "owned"), ruvia::db_binary_operator::equal, db::uuid(ownedGroups, actor.userId))), ruvia::db_binary_operator::equal, db::boolean(ownedGroups, true))));
        ruvia::db_query sharedGroups(query.resource());
        sharedGroups.select(sharedGroups.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "shared"))
            .from("shared_group_tree", "shared");
        scopedGroups.combine(ruvia::db_set_operation::union_value, ownedGroups)
            .combine(ruvia::db_set_operation::union_value, sharedGroups);

        ruvia::db_query parents(query.resource());
        parents.select(parents.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), "parent"))
            .from(service::device::entities::DeviceGroupEntity::table_name(), "parent")
            .join(ruvia::db_join_type::inner, "visible_group", parents.binary(parents.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "visible"), ruvia::db_binary_operator::equal, parents.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "parent")), "visible")
            .where(db::andAll(parents, parents.unary(ruvia::db_unary_operator::is_not_null, parents.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), "parent")), parents.unary(ruvia::db_unary_operator::is_null, parents.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), "parent"))));
        scopedGroups.combine(ruvia::db_set_operation::union_value, parents);

        const std::array<ruvia::borrowed_text, 1> sourceColumns4{ { "id" } };
        target.with("visible_group", scopedGroups, { .recursive_ = true, .columns_ = sourceColumns4 });
    }

    static ruvia::db_expression accessLevelRank(ruvia::db_query& query, ruvia::db_expression level) {
        return query.case_when(
            { { query.binary(level, ruvia::db_binary_operator::equal, query.value("operate")),
                db::integer(query, 2) },
              { query.binary(level, ruvia::db_binary_operator::equal, query.value("view")),
                db::integer(query, 1) } },
            db::integer(query, 0)
        );
    }

    static ruvia::db_expression remoteControlEnabled(ruvia::db_query& query, ruvia::db_expression params) {
        const auto key = db::text(query, "remote_control");
        const auto hasKey = query.binary(params, ruvia::db_binary_operator::json_has_key, key);
        const auto value = query.call(
            "lower",
            { query.coalesce({ query.binary(params, ruvia::db_binary_operator::json_get_text, key), query.value("") }) }
        );
        return query.case_when(
            { { hasKey,
                query.case_when(
                    { { query.binary(value, ruvia::db_binary_operator::equal, query.value("true")),
                        db::boolean(query, true) },
                      { query.binary(value, ruvia::db_binary_operator::equal, query.value("t")),
                        db::boolean(query, true) },
                      { query.binary(value, ruvia::db_binary_operator::equal, query.value("1")),
                        db::boolean(query, true) },
                      { query.binary(value, ruvia::db_binary_operator::equal, query.value("yes")),
                        db::boolean(query, true) },
                      { query.binary(value, ruvia::db_binary_operator::equal, query.value("y")),
                        db::boolean(query, true) },
                      { query.binary(value, ruvia::db_binary_operator::equal, query.value("on")),
                        db::boolean(query, true) } },
                    db::boolean(query, false)
                ) } },
            db::boolean(query, true)
        );
    }

    static DeviceCapabilities capabilities(const DeviceActor& actor, DeviceAccessLevel level, bool remoteControl) {
        const auto rankValue = static_cast<std::int64_t>(level);
        DeviceCapabilities result;
        result.canEdit = actor.canEdit && level == DeviceAccessLevel::owner;
        result.canDelete = actor.canDelete && level == DeviceAccessLevel::owner;
        result.canShare = actor.canShare && level == DeviceAccessLevel::owner;
        result.canCommand = actor.canCommand && remoteControl &&
            rankValue >= rankValueOf(DeviceAccessLevel::operate);
        result.accessLevel = name(level);
        return result;
    }

    static DeviceAccessLevel rank(std::string_view value) {
        const auto parsed =
            service::utils::parseInt64(std::optional<std::string_view>{ value }).value_or(0);
        if (parsed >= rankValueOf(DeviceAccessLevel::owner)) {
            return DeviceAccessLevel::owner;
        }
        if (parsed == rankValueOf(DeviceAccessLevel::operate)) {
            return DeviceAccessLevel::operate;
        }
        if (parsed == rankValueOf(DeviceAccessLevel::view)) {
            return DeviceAccessLevel::view;
        }
        return DeviceAccessLevel::none;
    }

  private:
    static bool isTrue(std::string_view value) { return value == "t" || value == "true"; }

    static constexpr std::int64_t rankValueOf(DeviceAccessLevel level) {
        return static_cast<std::int64_t>(level);
    }

    static constexpr std::string_view name(DeviceAccessLevel level) {
        switch (level) {
            case DeviceAccessLevel::owner:
                return "owner";
            case DeviceAccessLevel::operate:
                return "operate";
            case DeviceAccessLevel::view:
                return "view";
            case DeviceAccessLevel::none:
                return "none";
        }
        return "none";
    }
};

using DeviceItems = std::map<std::string, DeviceItemDto*, std::less<>>;

// 命令状态与可写命令目录归 command 模块（command 依赖 device）。设备详情与设备事件流的
// commands 频道经应用入口按 Worker 注入的端口读取，device 不反向依赖 command。
struct DeviceCommandPort final {
    ruvia::task<DeviceCommandStatusesDto> (*statuses)(service::middleware::RequestContext&, std::string_view commandIds) = nullptr;
    ruvia::task<void> (*fillOperations)(service::middleware::RequestContext&, const DeviceItems&, std::optional<std::string_view> onlyDevice) = nullptr;
};

template <typename Context>
const DeviceCommandPort& deviceCommandPort(Context& c) {
    const auto& port = c.template worker_state<DeviceCommandPort>();
    if (port.statuses == nullptr || port.fillOperations == nullptr) {
        service::common::fail(service::common::kServerErrorCode, "设备命令能力未装配", 500);
    }
    return port;
}

class DeviceService {
  public:
    static DeviceService& instance() {
        static thread_local DeviceService service;
        return service;
    }

    static service::link::DebugIdleKeys debugIdleKeys(std::string_view id) {
        return { service::device::entities::debugLeaseKey(id), service::device::entities::debugWatchKey(id) };
    }

    // 续期设备调试空闲租约（调试事件流心跳）。
    template <typename Redis>
    static ruvia::task<void> touchDebug(const Redis& redis, std::string_view id) {
        co_await service::link::DebugIdle::touch(redis, debugIdleKeys(id));
    }

    // `onlyIds` re-reads just those devices through the same scoped query (live
    // updates), so rows keep exactly the snapshot row shape.
    template <typename Context>
    ruvia::task<DevicePageDataDto> list(Context& c, const std::vector<std::string>* onlyIds = nullptr) {
        DevicePageDataDto result(ruvia::model_options{ .resource_ = c.arena() });
        const auto ids = onlyIds ? validIds(*onlyIds) : std::vector<std::string>{};
        if (onlyIds && ids.empty()) {
            result.template set<"list">(ruvia::boxed_array<DeviceItemDto>(ruvia::model_options{ .resource_ = c.arena() })).template set<"total">(0);
            co_return result;
        }
        const auto actor = co_await DeviceAccessService::instance().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("d");
        DeviceAccessService::addScopedDevicesCtes(queryBuilder, query, actor);
        selectItemColumns(queryBuilder, query);

        queryBuilder.from_cte("scoped_device").template join<service::link::entities::LinkEntity>(ruvia::db_join_type::left, "l", query.binary(query.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, query.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d"))).template join<service::edge_node::entities::EdgeNodeEntity>(ruvia::db_join_type::left, "en", query.binary(query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "en"), ruvia::db_binary_operator::equal, query.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l"))).template join<service::device::entities::DeviceModelEntity>(ruvia::db_join_type::inner, "p", query.binary(query.column(service::device::entities::DeviceModelEntity::column_name<"device_id">(), "p"), ruvia::db_binary_operator::equal, query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"))).where(query.binary(query.column(kAccessRankColumn, "d"), ruvia::db_binary_operator::greater, db::integer(query, 0))).order_by(query.column(service::device::entities::DeviceEntity::column_name<"group_id">(), "d"), ruvia::db_order_direction::asc, ruvia::db_nulls_order::last).add_order_by(query.column(service::device::entities::DeviceEntity::column_name<"created_at">(), "d")).add_order_by(query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"));
        if (onlyIds) {
            restrictIds(query, query, query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), ids);
        }
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<DeviceItemDto> items(ruvia::model_options{ .resource_ = c.arena() });
        std::map<std::string, DeviceItemDto*, std::less<>> itemsById;
        for (const auto& row : rows) {
            auto& item = items.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            fillItem(c, item, row, actor);
            itemsById.emplace(std::string(row[0].value().value_or(std::string_view{})), &item);
        }
        co_await fillLatest(c, itemsById);
        result.template set<"list">(std::move(items)).template set<"total">(static_cast<std::int64_t>(rows.size()));
        co_return result;
    }

    template <typename Context>
    ruvia::task<DeviceRealtimePageDto> realtime(Context& c, const std::vector<std::string>* onlyIds = nullptr) {
        DeviceRealtimePageDto result(ruvia::model_options{ .resource_ = c.arena() });
        const auto ids = onlyIds ? validIds(*onlyIds) : std::vector<std::string>{};
        if (onlyIds && ids.empty()) {
            result.template set<"list">(ruvia::boxed_array<DeviceRealtimeDto>(ruvia::model_options{ .resource_ = c.arena() })).template set<"total">(0);
            co_return result;
        }
        const auto actor = co_await DeviceAccessService::instance().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("d");
        DeviceAccessService::addScopedDevicesCtes(queryBuilder, query, actor);
        const auto executionIsEdge = query.binary(query.column(service::link::entities::LinkEntity::column_name<"execution">(), "l"), ruvia::db_binary_operator::equal, query.value("edge"));

        queryBuilder.select({ { "id", db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d")) }, { "protocol_params", db::jsonText(query, query.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "d"), "device_code") }, { "value2", DeviceAccessService::remoteControlEnabled(query, query.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "d")) }, { "value3", query.column(kAccessRankColumn, "d") }, { "edge_node_id", query.case_when({ { executionIsEdge, db::text(query, query.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l")) } }) }, { "endpoint", query.case_when({ { executionIsEdge, db::jsonText(query, query.column(service::link::entities::LinkEntity::column_name<"endpoint">(), "l"), "transport") } }) } }).from_cte("scoped_device").template join<service::link::entities::LinkEntity>(ruvia::db_join_type::left, "l", query.binary(query.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, query.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d"))).where(query.binary(query.column(kAccessRankColumn, "d"), ruvia::db_binary_operator::greater, db::integer(query, 0))).order_by(query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"));
        if (onlyIds) {
            restrictIds(query, query, query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), ids);
        }
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<DeviceRealtimeDto> items(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        std::map<std::string, DeviceRealtimeDto*, std::less<>> itemsById;
        for (const auto& row : rows) {
            const auto capabilities = DeviceAccessService::capabilities(
                actor,
                DeviceAccessService::rank(row[3].value().value_or(std::string_view{})),
                row[2].value().value_or(std::string_view{}) == "t"
            );
            auto& item = items.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"id">(row[0].value().value_or(std::string_view{}))
                .template set<"deviceCode">(row[1].value().value_or(std::string_view{}))
                .template set<"connected">(false)
                .template set<"connectionState">("disconnected")
                .template set<"runtimeState">("offline")
                .template set<"elements">(ruvia::boxed_array<DeviceElementDto>(
                    ruvia::model_options{ .resource_ = c.arena() }
                ))
                .template set<"canEdit">(capabilities.canEdit)
                .template set<"canDelete">(capabilities.canDelete)
                .template set<"canShare">(capabilities.canShare)
                .template set<"canCommand">(capabilities.canCommand)
                .template set<"accessLevel">(capabilities.accessLevel);
            if (row[4].value().has_value()) {
                item.template set<"edgeNodeId">(row[4].value().value_or(std::string_view{}));
            }
            if (row[5].value().has_value()) {
                item.template set<"edgeTransport">(row[5].value().value_or(std::string_view{}));
            }
            itemsById.emplace(std::string(row[0].value().value_or(std::string_view{})), &item);
        }
        co_await fillLatest(c, itemsById);
        result.template set<"list">(std::move(items)).template set<"total">(static_cast<std::int64_t>(rows.size()));
        co_return result;
    }

    // Devices whose list/realtime rows show data of the given links, protocol
    // configurations, edge nodes or groups (including descendants of a group:
    // shared access inherits down the tree). Visibility is checked afterwards
    // by the scoped list query.
    template <typename Context>
    ruvia::task<std::vector<std::string>> devicesReferencing(Context& c, const std::vector<std::string>& linkIds, const std::vector<std::string>& protocolConfigIds, const std::vector<std::string>& edgeNodeIds, const std::vector<std::string>& groupIds) {
        using Device = service::device::entities::DeviceEntity;
        using Group = service::device::entities::DeviceGroupEntity;
        const auto links = validIds(linkIds);
        const auto protocols = validIds(protocolConfigIds);
        const auto nodes = validIds(edgeNodeIds);
        const auto groups = validIds(groupIds);
        std::vector<std::string> result;
        if (links.empty() && protocols.empty() && nodes.empty() && groups.empty()) {
            co_return result;
        }
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<Device>().query_builder("d");
        std::vector<ruvia::db_expression> conditions;
        if (!links.empty()) {
            conditions.push_back(query.binary(query.column(Device::column_name<"link_id">(), "d"), ruvia::db_binary_operator::in, uuidList(query, links)));
        }
        if (!protocols.empty()) {
            conditions.push_back(query.binary(query.column(Device::column_name<"protocol_config_id">(), "d"), ruvia::db_binary_operator::in, uuidList(query, protocols)));
        }
        if (!nodes.empty()) {
            conditions.push_back(query.binary(query.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l"), ruvia::db_binary_operator::in, uuidList(query, nodes)));
        }
        if (!groups.empty()) {
            ruvia::db_query roots(query.resource());
            roots.select(roots.column(Group::column_name<"id">(), "root"))
                .from(Group::table_name(), "root")
                .where(roots.binary(roots.column(Group::column_name<"id">(), "root"), ruvia::db_binary_operator::in, uuidList(roots, groups)));
            ruvia::db_query children(query.resource());
            children.select(children.column(Group::column_name<"id">(), "child"))
                .from(Group::table_name(), "child")
                .join(ruvia::db_join_type::inner, "changed_group", children.binary(children.column("id", "parent"), ruvia::db_binary_operator::equal, children.column(Group::column_name<"parent_id">(), "child")), "parent");
            roots.combine(ruvia::db_set_operation::union_value, children);

            const std::array<ruvia::borrowed_text, 1> sourceColumns5{ { "id" } };
            queryBuilder.with("changed_group", roots, { .recursive_ = true, .columns_ = sourceColumns5 });
            ruvia::db_query subtree(query.resource());
            subtree.select(subtree.column("id")).from("changed_group");
            conditions.push_back(query.binary(query.column(Device::column_name<"group_id">(), "d"), ruvia::db_binary_operator::in, query.subquery(subtree)));
        }
        auto matches = conditions.front();
        for (std::size_t index = 1; index < conditions.size(); ++index) {
            matches = query.binary(matches, ruvia::db_binary_operator::or_value, conditions[index]);
        }
        queryBuilder.select({ { "id", db::text(query, query.column(Device::column_name<"id">(), "d")) } }).template join<service::link::entities::LinkEntity>(ruvia::db_join_type::left, "l", query.binary(query.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, query.column(Device::column_name<"link_id">(), "d"))).where(db::andAll(query, query.unary(ruvia::db_unary_operator::is_null, query.column(Device::column_name<"deleted_at">(), "d")), matches));
        const auto rows = co_await queryBuilder.get_raw_many();
        result.reserve(rows.size());
        for (const auto& row : rows) {
            result.emplace_back(row[0].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<DeviceItemDto> detail(Context& c, std::string_view id) {
        const auto actor = co_await DeviceAccessService::instance().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("d");
        DeviceAccessService::addScopedDevicesCtes(queryBuilder, query, actor);
        selectItemColumns(queryBuilder, query);

        queryBuilder.from_cte("scoped_device").template join<service::link::entities::LinkEntity>(ruvia::db_join_type::left, "l", query.binary(query.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, query.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d"))).template join<service::edge_node::entities::EdgeNodeEntity>(ruvia::db_join_type::left, "en", query.binary(query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "en"), ruvia::db_binary_operator::equal, query.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l"))).template join<service::device::entities::DeviceModelEntity>(ruvia::db_join_type::inner, "p", query.binary(query.column(service::device::entities::DeviceModelEntity::column_name<"device_id">(), "p"), ruvia::db_binary_operator::equal, query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"))).where(db::andAll(query, query.binary(query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), ruvia::db_binary_operator::equal, db::uuid(query, id)), query.binary(query.column(kAccessRankColumn, "d"), ruvia::db_binary_operator::greater, db::integer(query, 0)))).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(18001, "设备不存在", 404);
        }
        DeviceItemDto item(ruvia::model_options{ .resource_ = c.arena() });
        fillItem(c, item, rows.front(), actor);
        DeviceItems itemById{ { std::string(id), &item } };
        if (auto io = co_await loadExternalIo(c, c.db(), id)) {
            item.template set<"externalIo">(std::move(*io));
        }
        co_await fillLatest(c, itemById);
        co_await deviceCommandPort(c).fillOperations(c, itemById, id);
        co_return item;
    }

    template <typename Context>
    ruvia::task<std::string> history(Context& c, std::string_view id, const DeviceHistoryQuery& body) {
        (void)co_await DeviceAccessService::instance().require(c, id, DeviceAccessLevel::view, c.userId);
        const auto start = body.template get<"startTime">().view();
        const auto end = body.template get<"endTime">().view();
        const auto requestedPage = static_cast<std::int64_t>(*body.template get<"page">());
        const auto requestedPageSize = static_cast<std::int64_t>(*body.template get<"pageSize">());
        const auto page = requestedPage > 0 ? requestedPage : std::int64_t{ 1 };
        const auto pageSize =
            requestedPageSize < 1 ? std::int64_t{ 20 }
                                  : std::min<std::int64_t>(requestedPageSize, 100);
        const auto offset = (page - 1) * pageSize;

        try {
            const auto rows = co_await historyQuery(c.db(), c.pool(), id, start, end, pageSize, offset, page).get_raw_many();
            co_return rows.empty() ? std::string{ "{\"list\":[],\"total\":0}" }
                                   : std::string{ rows.front()[0].value().value_or(std::string_view{}) };
        } catch (const std::exception&) {
            service::common::fail(18002, "时间范围格式错误", 400);
        }
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<DeviceOptionDto>> options(Context& c) {
        const auto actor = co_await DeviceAccessService::instance().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("scoped_device");
        DeviceAccessService::addScopedDevicesCtes(queryBuilder, query, actor);

        queryBuilder.select({ { "id", db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"id">())) }, { "name", query.column(service::device::entities::DeviceEntity::column_name<"name">()) }, { "protocol_params", db::jsonText(query, query.column(service::device::entities::DeviceEntity::column_name<"protocol_params">()), "device_code") }, { "value3", DeviceAccessService::remoteControlEnabled(query, query.column(service::device::entities::DeviceEntity::column_name<"protocol_params">())) }, { "value4", query.column(kAccessRankColumn) } }).from_cte("scoped_device").where(db::andAll(query, query.binary(query.column(kAccessRankColumn), ruvia::db_binary_operator::greater, db::integer(query, 0)), query.binary(query.column(service::device::entities::DeviceEntity::column_name<"status">()), ruvia::db_binary_operator::equal, query.value("enabled")))).order_by(query.column(service::device::entities::DeviceEntity::column_name<"name">()));
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<DeviceOptionDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            const auto capabilities = DeviceAccessService::capabilities(
                actor,
                DeviceAccessService::rank(row[4].value().value_or(std::string_view{})),
                row[3].value().value_or(std::string_view{}) == "t"
            );
            auto& item = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"id">(row[0].value().value_or(std::string_view{}))
                .template set<"name">(row[1].value().value_or(std::string_view{}))
                .template set<"deviceCode">(row[2].value().value_or(std::string_view{}))
                .template set<"canEdit">(capabilities.canEdit)
                .template set<"canDelete">(capabilities.canDelete)
                .template set<"canShare">(capabilities.canShare)
                .template set<"canCommand">(capabilities.canCommand)
                .template set<"accessLevel">(capabilities.accessLevel);
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> create(Context& c, const CreateDeviceBody& input) {
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        const auto& body = input;
        std::string resolvedLinkId = str(body.template get<"linkId">());
        if (body.template get<"edgeConnection">()) {
            if (!resolvedLinkId.empty()) {
                service::common::fail(18002, "本地链路和边缘连接不能同时配置", 400);
            }
            const auto endpointId = co_await service::link::LinkService::instance().configureDeviceEndpoint(c, transaction, *body.template get<"edgeConnection">());
            resolvedLinkId = endpointId;
        }

        co_await validate(c, transaction, resolvedLinkId, body, true);
        co_await ensureUnique(c, transaction, resolvedLinkId, body, std::nullopt);
        co_await validateRuntimeIdentity(c, transaction, resolvedLinkId, body, std::nullopt);
        const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        const std::string name(body.template get<"name">().view());
        const std::string deviceCode(body.template get<"deviceCode">().view());
        const std::string linkId = resolvedLinkId;
        ruvia::db_query channelQuery(c.pool());
        auto channelQueryBuilder = transaction.template repository<service::link::entities::LinkEntity>().query_builder();
        channelQueryBuilder.select({ { "edge_node_id", channelQuery.coalesce({ db::text(channelQuery, channelQuery.column(service::link::entities::LinkEntity::column_name<"edge_node_id">())), channelQuery.value("") }) }, { "protocol", service::link::entities::LinkEntity::column<"protocol">().expression(channelQuery) } }).where(db::andAll(channelQuery, channelQuery.binary(channelQuery.column(service::link::entities::LinkEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(channelQuery, linkId)), channelQuery.unary(ruvia::db_unary_operator::is_null, channelQuery.column(service::link::entities::LinkEntity::column_name<"deleted_at">()))));
        const auto channel = co_await channelQueryBuilder.get_raw_many();
        if (channel.empty()) {
            service::common::fail(18003, "通道不存在", 400);
        }
        const std::string edgeNodeId(channel.front()[0].value().value_or(""));
        const std::string targetId = str(body.template get<"targetId">());
        const std::string protocolConfigId(body.template get<"protocolConfigId">().view());
        const std::string groupId = str(body.template get<"groupId">());
        const std::string status = body.template get<"status">() ? std::string(body.template get<"status">()->view()) : "enabled";
        const std::int64_t onlineTimeout =
            body.template get<"onlineTimeout">() ? static_cast<std::int64_t>(*body.template get<"onlineTimeout">()) : 300;
        const std::string remoteControl =
            (!body.template get<"remoteControl">() || *body.template get<"remoteControl">()) ? "true" : "false";
        const std::string modbusMode = str(body.template get<"modbusMode">());
        const std::string slaveId =
            body.template get<"slaveId">() ? std::to_string(static_cast<std::int64_t>(*body.template get<"slaveId">())) : "";
        const std::string timezone = (body.template get<"timezone">() && !body.template get<"timezone">()->view().empty())
            ? std::string(body.template get<"timezone">()->view())
            : std::string(service::message::device::kDefaultTimezone);
        const std::string heartbeat = packetJson(body.template get<"heartbeat">());
        const std::string registration =
            edgeNodeId.empty() ? packetJson(body.template get<"registration">()) : R"({"mode":"OFF"})";
        const std::string remark = str(body.template get<"remark">());

        ruvia::db_query insert(c.pool());
        const auto jsonb = [&](std::string_view value) {
            return insert.cast(insert.value(value), ruvia::db_data_type::jsonb);
        };
        const auto emptyPacket = jsonb(R"({"mode":"OFF"})");
        auto protocolParams = insert.call(
            "jsonb_strip_nulls",
            { insert.call("jsonb_build_object", { db::text(insert, "device_code"), db::text(insert, insert.value(deviceCode)), db::text(insert, "target_id"), insert.null_if(db::text(insert, insert.value(targetId)), insert.value("")), db::text(insert, "online_timeout"), insert.cast(insert.value(onlineTimeout), ruvia::db_data_type::integer), db::text(insert, "remote_control"), insert.cast(insert.value(remoteControl == "true"), ruvia::db_data_type::boolean), db::text(insert, "modbus_mode"), insert.null_if(db::text(insert, insert.value(modbusMode)), insert.value("")), db::text(insert, "slave_id"), insert.cast(insert.null_if(db::text(insert, insert.value(slaveId)), insert.value("")), ruvia::db_data_type::integer), db::text(insert, "timezone"), db::text(insert, insert.value(timezone)), db::text(insert, "heartbeat"), insert.coalesce({ insert.cast(insert.null_if(db::text(insert, insert.value(heartbeat)), insert.value("")), ruvia::db_data_type::jsonb), emptyPacket }), db::text(insert, "registration"), insert.coalesce({ insert.cast(insert.null_if(db::text(insert, insert.value(registration)), insert.value("")), ruvia::db_data_type::jsonb), emptyPacket }) }) }
        );
        protocolParams = insert.binary(protocolParams, ruvia::db_binary_operator::json_concat, insert.call("jsonb_build_object", { db::text(insert, "parameters"), jsonb(body.template get<"parameters">() ? body.template get<"parameters">()->view() : "{}") }));
        const std::array<std::string_view, 9> insertColumns{ service::device::entities::DeviceEntity::column_name<"id">(), service::device::entities::DeviceEntity::column_name<"name">(), service::device::entities::DeviceEntity::column_name<"link_id">(), service::device::entities::DeviceEntity::column_name<"protocol_config_id">(), service::device::entities::DeviceEntity::column_name<"group_id">(), service::device::entities::DeviceEntity::column_name<"status">(), service::device::entities::DeviceEntity::column_name<"protocol_params">(), service::device::entities::DeviceEntity::column_name<"remark">(), service::device::entities::DeviceEntity::column_name<"created_by">() };
        auto insertBuilder = transaction.template repository<service::device::entities::DeviceEntity>().insert_builder();
        ruvia::db_values_builder insertValues(insertColumns);
        insertValues.row({ db::uuid(insert, id), insert.value(name), db::uuid(insert, linkId), db::uuid(insert, protocolConfigId), db::nullableUuid(insert, groupId), insert.value(status), protocolParams, insert.null_if(db::text(insert, insert.value(remark)), insert.value("")), db::uuid(insert, c.userId) });
        insertBuilder.insert_from(insertColumns, insertValues.query());
        (void)co_await insertBuilder.execute();
        if (channel.front()[1].value().value_or("") == "ExternalIO") {
            DeviceExternalIoBody io(ruvia::model_options{ .resource_ = c.arena() });
            io.set<"protocolConfigId">(protocolConfigId);
            validateStandaloneIo(body, protocolConfigId);
            co_await saveExternalIo(c, transaction, id, linkId, protocolConfigId, io);
        } else if (body.template get<"externalIo">()) {
            co_await saveExternalIo(c, transaction, id, linkId, protocolConfigId, *body.template get<"externalIo">());
        }
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "device", "created", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        try {
            (void)co_await service::utils::callRpc(c, "telemetry", "initialize", service::message::rpc::encodeLinePair(id, deviceCode));
        } catch (...) {
            // PostgreSQL is authoritative; startup hydration or the first report repairs Redis.
        }
        if (!edgeNodeId.empty()) {
            (void)co_await service::edge_node::EdgeService::queueSnapshot(c, edgeNodeId, c.userId);
        }
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<DeviceDebugAcquisitionDto>> debugPackets(Context& c, std::string_view id) {
        (void)co_await DeviceAccessService::instance().require(c, id, DeviceAccessLevel::owner, c.userId);
        co_await touchDebug(c.redis(), id);
        // 有界 ZSET 索引与报文 Hash 的一致性读取需要 Lua，ORM 无原子跨键读取接口。
        const auto key = service::message::packet_log::deviceIndexKey(id);
        static constexpr std::string_view readPackets = R"lua(
local result={}
local count=0
for _,acquisition in ipairs(redis.call('ZREVRANGE',KEYS[1],0,99)) do
    local round='iot:debug:v4:acquisition:'..acquisition
    local metadata=redis.call('HGETALL',round)
    if #metadata==0 then
        redis.call('ZREM',KEYS[1],acquisition)
    else
        local ids=redis.call('ZRANGE',round..':packets',0,-1)
        if count+#ids>4096 then break end
        local packets={}
        for _,id in ipairs(ids) do
            local fields=redis.call('HGETALL','iot:debug:v4:packet:'..id)
            if #fields>0 then packets[#packets+1]={id,fields} end
        end
        result[#result+1]={acquisition,metadata,packets}
        count=count+#packets
    end
end
return result
)lua";
        const std::vector<std::string_view> keys{ key };
        const std::vector<std::string_view> args;
        const auto reply = co_await c.redis().eval(readPackets, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            service::utils::redis::throwValue("read debug packets", reply);
        }
        ruvia::boxed_array<DeviceDebugAcquisitionDto> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : reply.array()) {
            if (row.kind() != ruvia::redis_value::kind_type::array || row.array().size() != 3) {
                continue;
            }
            auto& acquisition = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            acquisition.template set<"id">(row.array()[0].string());
            const auto metadata = row.array()[1].array();
            for (std::size_t index = 0; index + 1 < metadata.size(); index += 2) {
                const auto field = metadata[index].string();
                const auto value = metadata[index + 1].string();
                if (field == "started_at_ms") {
                    acquisition.template set<"startedAtMs">(value);
                }
                if (field == "finished_at_ms") {
                    acquisition.template set<"finishedAtMs">(value);
                }
                if (field == "last_packet_at_ms") {
                    acquisition.template set<"lastPacketAtMs">(value);
                }
                if (field == "state") {
                    acquisition.template set<"state">(value);
                }
                if (field == "device_id") {
                    acquisition.template set<"deviceId">(value);
                }
            }
            ruvia::boxed_array<DeviceDebugPacketDto> packets(ruvia::model_options{ .resource_ = c.arena() });
            for (const auto& packetRow : row.array()[2].array()) {
                const auto fields = packetRow.array()[1].array();
                std::string eventId(packetRow.array()[0].string());
                auto& packet = packets.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
                packet.template set<"id">(eventId);
                for (std::size_t index = 0; index + 1 < fields.size(); index += 2) {
                    const auto name = fields[index].string();
                    const auto value = fields[index + 1].string();
                    if (name == "acquisition_id") {
                        packet.template set<"acquisitionId">(value);
                    } else if (name == "device_id") {
                        packet.template set<"deviceId">(value);
                    } else if (name == "direction") {
                        packet.template set<"direction">(value);
                    } else if (name == "source") {
                        packet.template set<"source">(value);
                    } else if (name == "address") {
                        packet.template set<"address">(value);
                    } else if (name == "edge_node_id") {
                        packet.template set<"edgeNodeId">(value);
                    } else if (name == "edge_node_name") {
                        packet.template set<"edgeNodeName">(value);
                    } else if (name == "payload_hex") {
                        packet.template set<"payloadHex">(value);
                    } else if (name == "time_ms") {
                        packet.template set<"timeMs">(value);
                    } else if (name == "transport_status") {
                        packet.template set<"transportStatus">(value);
                    } else if (name == "response_status") {
                        packet.template set<"responseStatus">(value);
                    } else if (name == "parse_status") {
                        packet.template set<"parseStatus">(value);
                    } else if (name == "revision") {
                        packet.template set<"revision">(value);
                    } else if (name == "reply_to_packet_id") {
                        packet.template set<"replyToPacketId">(value);
                    } else if (name == "reason") {
                        packet.template set<"reason">(value);
                    } else if (name == "parsed_json") {
                        packet.template set<"parsedJson">(value);
                    }
                }
            }
            acquisition.template set<"packets">(std::move(packets));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> setDebug(Context& c, std::string_view id, bool enabled) {
        (void)co_await DeviceAccessService::instance().require(c, id, DeviceAccessLevel::owner, c.userId);
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        ruvia::db_query query(c.pool());
        auto queryBuilder = transaction.template repository<service::device::entities::DeviceEntity>().update_builder();
        queryBuilder.set(service::device::entities::DeviceEntity::column_name<"debug_enabled">(), query.value(enabled)).set(service::device::entities::DeviceEntity::column_name<"updated_at">(), query.call("now")).where((service::device::entities::DeviceEntity::column<"id">() == id && service::device::entities::DeviceEntity::column<"deleted_at">().is_null()).expression(query));
        (void)co_await queryBuilder.execute();
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "device", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        if (enabled) {
            co_await touchDebug(c.redis(), id);
            service::link::DebugIdle::watch(
                c.template worker_state<ruvia::web_worker_handle>(),
                debugIdleKeys(id),
                std::string(id),
                [](ruvia::web_worker_context& ctx, std::string_view deviceId) -> ruvia::task<void> {
                    co_await DeviceService::instance().disableExpiredDebug(ctx, deviceId);
                }
            );
        } else {
            co_await service::link::DebugIdle::clear(c.redis(), debugIdleKeys(id));
        }
        const auto device = co_await detail(c, id);
        if (device.template get<"edgeNodeId">() && !device.template get<"edgeNodeId">()->view().empty()) {
            (void)co_await service::edge_node::EdgeService::queueSnapshot(c, device.template get<"edgeNodeId">()->view(), c.userId);
        }
    }

    template <typename Context>
    ruvia::task<void> disableExpiredDebug(Context& c, std::string_view id) {
        if (co_await service::link::DebugIdle::leased(c.redis(), debugIdleKeys(id))) {
            co_return;
        }
        ruvia::db_query lookup(c.pool());
        auto lookupBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("d");
        lookupBuilder.select({ { "debug_enabled", lookup.column(service::device::entities::DeviceEntity::column_name<"debug_enabled">(), "d") }, { "created_by", lookup.column(service::device::entities::DeviceEntity::column_name<"created_by">(), "d") }, { "edge_node_id", lookup.coalesce({ lookup.cast(lookup.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l"), ruvia::db_data_type::text), lookup.value("") }) } }).template join<service::link::entities::LinkEntity>(ruvia::db_join_type::left, "l", lookup.binary(lookup.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, lookup.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d"))).where(db::andAll(lookup, lookup.binary(lookup.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), ruvia::db_binary_operator::equal, db::uuid(lookup, id)), lookup.unary(ruvia::db_unary_operator::is_null, lookup.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "d"))));
        const auto rows = co_await lookupBuilder.get_raw_many();
        if (rows.empty() || rows.front()[0].value().value_or("") != "t") {
            co_return;
        }
        const auto actor = std::string(rows.front()[1].value().value_or(""));
        const auto edgeNodeId = std::string(rows.front()[2].value().value_or(""));
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        ruvia::db_query query(c.pool());
        auto queryBuilder = transaction.template repository<service::device::entities::DeviceEntity>().update_builder();
        queryBuilder.set(service::device::entities::DeviceEntity::column_name<"debug_enabled">(), query.value(false)).set(service::device::entities::DeviceEntity::column_name<"updated_at">(), query.call("now")).where((service::device::entities::DeviceEntity::column<"id">() == id && service::device::entities::DeviceEntity::column<"deleted_at">().is_null()).expression(query));
        (void)co_await queryBuilder.execute();
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "device", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        if (!edgeNodeId.empty() && service::common::isUuid(actor)) {
            (void)co_await service::edge_node::EdgeService::queueSnapshot(c, edgeNodeId, actor);
        }
    }

    template <typename Context>
    ruvia::task<void> update(Context& c, std::string_view id, const UpdateDeviceBody& input) {
        const auto& body = input;
        std::string resolvedLinkId = str(body.template get<"linkId">());
        (void)co_await DeviceAccessService::instance().require(c, id, DeviceAccessLevel::owner, c.userId);
        ruvia::db_query currentQuery(c.pool());
        auto currentQueryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("d");
        currentQueryBuilder.select({ { "link_id", db::text(currentQuery, currentQuery.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d")) }, { "edge_node_id", currentQuery.coalesce({ db::text(currentQuery, currentQuery.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l")), currentQuery.value("") }) }, { "protocol_config_id", db::text(currentQuery, currentQuery.column(service::device::entities::DeviceEntity::column_name<"protocol_config_id">(), "d")) }, { "protocol_params", db::jsonText(currentQuery, currentQuery.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "d"), "device_code") }, { "execution", currentQuery.column(service::link::entities::LinkEntity::column_name<"execution">(), "l") }, { "status", db::text(currentQuery, currentQuery.column(service::device::entities::DeviceEntity::column_name<"status">(), "d")) } }).template join<service::link::entities::LinkEntity>(ruvia::db_join_type::inner, "l", currentQuery.binary(currentQuery.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, currentQuery.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d"))).where(db::andAll(currentQuery, currentQuery.binary(currentQuery.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), ruvia::db_binary_operator::equal, db::uuid(currentQuery, id)), currentQuery.unary(ruvia::db_unary_operator::is_null, currentQuery.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "d"))));
        const auto rows = co_await currentQueryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(18001, "设备不存在", 404);
        }
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        co_await lockConnectionNodes(c, transaction, rows.front()[1].value().value_or(""), body.template get<"edgeConnection">() ? str(body.template get<"edgeConnection">()->template get<"edgeNodeId">()) : std::string{});
        if (body.template get<"edgeConnection">()) {
            if (!resolvedLinkId.empty()) {
                service::common::fail(18002, "本地链路和边缘连接不能同时配置", 400);
            }
            const auto endpointId = co_await service::link::LinkService::instance().configureDeviceEndpoint(c, transaction, *body.template get<"edgeConnection">(), id);
            resolvedLinkId = endpointId;
        }
        const std::string validationLink = resolvedLinkId.empty() ? std::string(rows.front()[0].value().value_or("")) : resolvedLinkId;
        co_await validate(c, transaction, validationLink, body, false, rows.front()[2].value().value_or(""));

        const auto& current = rows.front();
        const std::string currentLinkId(
            current[0].value().value_or(std::string_view{})
        );
        const std::string currentEdgeNodeId(
            current[1].value().value_or(std::string_view{})
        );
        const std::string currentProtocolConfigId(
            current[2].value().value_or(std::string_view{})
        );
        const std::string currentExecution(
            current[4].value().value_or(std::string_view{})
        );
        const std::string requestedLinkId = resolvedLinkId;
        const std::string targetLinkId = requestedLinkId.empty() ? currentLinkId : requestedLinkId;
        const std::string targetProtocolConfigId = body.template get<"protocolConfigId">() ? str(body.template get<"protocolConfigId">()) : currentProtocolConfigId;
        ruvia::db_query targetQuery(c.pool());
        auto targetQueryBuilder = transaction.template repository<service::link::entities::LinkEntity>().query_builder("l");
        targetQueryBuilder.select({ { "edge_node_id", targetQuery.coalesce({ db::text(targetQuery, targetQuery.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l")), targetQuery.value("") }) }, { "protocol", service::link::entities::LinkEntity::column<"protocol">().expression(targetQuery, "l") } }).template join<service::protocol::entities::ProtocolConfigEntity>(ruvia::db_join_type::inner, "p", targetQuery.binary(targetQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, targetQuery.column(service::link::entities::LinkEntity::column_name<"protocol">(), "l"))).where(db::andAll(targetQuery, targetQuery.binary(targetQuery.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, db::uuid(targetQuery, targetLinkId)), targetQuery.binary(targetQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"id">(), "p"), ruvia::db_binary_operator::equal, db::uuid(targetQuery, targetProtocolConfigId)), targetQuery.unary(ruvia::db_unary_operator::is_null, targetQuery.column(service::link::entities::LinkEntity::column_name<"deleted_at">(), "l")), targetQuery.unary(ruvia::db_unary_operator::is_null, targetQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"deleted_at">(), "p"))));
        const auto target = co_await targetQueryBuilder.get_raw_many();
        if (target.empty()) {
            service::common::fail(18003, "通道或设备类型不存在，或协议不一致", 400);
        }
        const std::string targetEdgeNodeId(target.front()[0].value().value_or(""));
        const bool targetEdge = !targetEdgeNodeId.empty();
        const bool connectionChanged = targetLinkId != currentLinkId || targetProtocolConfigId != currentProtocolConfigId;
        auto externalIo = co_await loadExternalIo(c, transaction, id);
        const DeviceExternalIoBody* ioConfiguration = externalIo ? &*externalIo : nullptr;
        const bool standaloneIo = target.front()[1].value().value_or("") == "ExternalIO";
        DeviceExternalIoBody standaloneConfiguration(ruvia::model_options{ .resource_ = c.arena() });
        if (standaloneIo) {
            validateStandaloneIo(body, targetProtocolConfigId);
            standaloneConfiguration.set<"protocolConfigId">(targetProtocolConfigId);
            ioConfiguration = &standaloneConfiguration;
        } else if (body.template is_present<"externalIo">()) {
            ioConfiguration = body.template get<"externalIo">() ? &*body.template get<"externalIo">() : nullptr;
        }
        const bool ioChanged = connectionChanged || body.template is_present<"externalIo">() || (standaloneIo && !externalIo);
        if (ioChanged && externalIo) {
            co_await clearExternalIo(c, transaction, id);
        }
        co_await ensureUnique(c, transaction, resolvedLinkId, body, std::string(id));
        co_await validateRuntimeIdentity(c, transaction, resolvedLinkId, body, std::string(id));

        ruvia::db_query update(c.pool());
        auto updateBuilder = transaction.template repository<service::device::entities::DeviceEntity>().update_builder();

        bool changed = false;
        const auto assign = [&](std::string_view column, ruvia::db_expression value) {
            updateBuilder.set(column, value);
            changed = true;
        };
        if (body.template get<"name">()) {
            assign("name", update.value(body.template get<"name">()->view()));
        }
        if (targetLinkId != currentLinkId) {
            assign("link_id", db::uuid(update, targetLinkId));
        }
        if (body.template get<"protocolConfigId">()) {
            assign("protocol_config_id", db::uuid(update, targetProtocolConfigId));
        }
        if (body.template get<"groupId">()) {
            assign("group_id", db::nullableUuid(update, body.template get<"groupId">()->view()));
        }
        if (body.template get<"status">()) {
            assign("status", update.value(body.template get<"status">()->view()));
        }
        auto protocolParams = update.column(service::device::entities::DeviceEntity::column_name<"protocol_params">());
        const auto jsonPath = [&](std::string_view key) {
            return update.cast(update.array({ db::text(update, key) }), ruvia::db_type_definition{ .data_type_ = ruvia::db_data_type::text, .array_ = true });
        };
        const auto jsonValue = [&](std::string_view key, ruvia::db_expression value) {
            protocolParams = update.call(
                "jsonb_set",
                { protocolParams, jsonPath(key), update.call("to_jsonb", { value }), db::boolean(update, true) }
            );
            changed = true;
        };
        const auto jsonDocument = [&](std::string_view key, std::string_view value) {
            protocolParams = update.call(
                "jsonb_set",
                { protocolParams, jsonPath(key), update.cast(update.value(value), ruvia::db_data_type::jsonb), db::boolean(update, true) }
            );
            changed = true;
        };
        if (body.template get<"parameters">()) {
            jsonDocument("parameters", body.template get<"parameters">()->view());
        }
        if (body.template get<"deviceCode">()) {
            jsonValue("device_code", db::text(update, update.value(body.template get<"deviceCode">()->view())));
        }
        if (body.template get<"targetId">()) {
            jsonValue("target_id", db::text(update, update.value(body.template get<"targetId">()->view())));
        } else if (connectionChanged) {
            protocolParams = update.binary(protocolParams, ruvia::db_binary_operator::json_delete, db::text(update, "target_id")),
            changed = true;
        }
        if (body.template get<"onlineTimeout">()) {
            jsonValue("online_timeout", update.cast(update.value(static_cast<std::int64_t>(*body.template get<"onlineTimeout">())), ruvia::db_data_type::big_int));
        }
        if (body.template get<"remoteControl">()) {
            jsonValue("remote_control", update.cast(update.value(static_cast<bool>(*body.template get<"remoteControl">())), ruvia::db_data_type::boolean));
        }
        if (body.template get<"modbusMode">()) {
            jsonValue("modbus_mode", db::text(update, update.value(body.template get<"modbusMode">()->view())));
        }
        if (body.template get<"slaveId">()) {
            jsonValue("slave_id", update.cast(update.value(static_cast<std::int64_t>(*body.template get<"slaveId">())), ruvia::db_data_type::big_int));
        }
        if (body.template get<"timezone">()) {
            jsonValue("timezone", db::text(update, update.value(body.template get<"timezone">()->view())));
        }
        std::string heartbeat;
        if (body.template get<"heartbeat">()) {
            heartbeat = packetJson(body.template get<"heartbeat">());
            jsonDocument("heartbeat", heartbeat);
        }
        std::string registration;
        if (targetEdge) {
            registration = R"({"mode":"OFF"})";
            jsonDocument("registration", registration);
        } else if (body.template get<"registration">()) {
            registration = packetJson(body.template get<"registration">());
            jsonDocument("registration", registration);
        }
        if (changed) {
            assign("protocol_params", protocolParams);
        }
        if (body.template get<"remark">()) {
            assign("remark", update.null_if(db::text(update, update.value(body.template get<"remark">()->view())), update.value("")));
        }

        {
            if (changed) {
                assign("updated_at", update.call("now"));
            }
            if (changed) {
                updateBuilder.where(update.binary(update.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(update, id)));
            }
            if (changed) {
                (void)co_await updateBuilder.execute();
            }
            if (ioConfiguration && ioChanged) {
                co_await saveExternalIo(c, transaction, id, targetLinkId, targetProtocolConfigId, *ioConfiguration);
            }
            co_await service::system::OutboxService::enqueueConfigEvent(transaction, "device", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
            if (connectionChanged && currentExecution == "edge") {
                co_await service::link::LinkService::instance().retireUnusedDeviceEndpoint(c, transaction, currentLinkId);
            }
            co_await transaction.commit();
        }
        try {
            (void)co_await service::utils::callRpc(c, "telemetry", "project-device", std::string(id));
        } catch (...) {
            // PostgreSQL remains authoritative; startup hydration repairs Redis read models.
        }
        if (!currentEdgeNodeId.empty()) {
            (void)co_await service::edge_node::EdgeService::queueSnapshot(
                c,
                currentEdgeNodeId,
                c.userId
            );
        }
        if (!targetEdgeNodeId.empty() && targetEdgeNodeId != currentEdgeNodeId) {
            (void)co_await service::edge_node::EdgeService::queueSnapshot(
                c,
                targetEdgeNodeId,
                c.userId
            );
        }
    }

    template <typename Context>
    ruvia::task<void> remove(Context& c, std::string_view id) {
        (void)co_await DeviceAccessService::instance().require(c, id, DeviceAccessLevel::owner, c.userId);
        ruvia::db_query currentQuery(c.pool());
        auto currentQueryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder("d");
        currentQueryBuilder.select({ { "protocol_params", db::jsonText(currentQuery, currentQuery.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "d"), "device_code") }, { "edge_node_id", currentQuery.coalesce({ db::text(currentQuery, currentQuery.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l")), currentQuery.value("") }) }, { "link_id", db::text(currentQuery, currentQuery.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d")) }, { "execution", currentQuery.column(service::link::entities::LinkEntity::column_name<"execution">(), "l") } }).template join<service::link::entities::LinkEntity>(ruvia::db_join_type::inner, "l", currentQuery.binary(currentQuery.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, currentQuery.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d"))).where(db::andAll(currentQuery, currentQuery.binary(currentQuery.column(service::device::entities::DeviceEntity::column_name<"id">(), "d"), ruvia::db_binary_operator::equal, db::uuid(currentQuery, id)), currentQuery.unary(ruvia::db_unary_operator::is_null, currentQuery.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "d"))));
        const auto rows = co_await currentQueryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(18001, "设备不存在", 404);
        }
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        co_await lockConnectionNodes(c, transaction, rows.front()[1].value().value_or(""), {});
        ruvia::db_query removeQuery(c.pool());
        auto removeQueryBuilder = transaction.template repository<service::device::entities::DeviceEntity>().update_builder();
        removeQueryBuilder.set(service::device::entities::DeviceEntity::column_name<"deleted_at">(), removeQuery.call("now")).set(service::device::entities::DeviceEntity::column_name<"updated_at">(), removeQuery.call("now")).where(removeQuery.binary(removeQuery.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(removeQuery, id)));
        (void)co_await removeQueryBuilder.execute();
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "device", "deleted", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        if (!rows.front()[1].value().value_or("").empty()) {
            co_await service::link::LinkService::instance().retireUnusedDeviceEndpoint(c, transaction, rows.front()[2].value().value_or(""));
        }
        co_await transaction.commit();
        try {
            (void)co_await service::utils::callRpc(c, "telemetry", "erase-device", std::string(id));
        } catch (...) {
            // The next startup hydration removes stale Redis state for deleted devices.
        }
        if (!rows.front()[1].value().value_or(std::string_view{}).empty()) {
            (void)co_await service::edge_node::EdgeService::queueSnapshot(
                c,
                rows.front()[1].value().value_or(std::string_view{}),
                c.userId
            );
        }
    }

    // ===== 设备分组（合并入同一 DeviceService 类）=====

    template <typename Context>
    ruvia::task<ruvia::boxed_array<DeviceGroupItemDto>> listGroups(Context& c) {
        const auto actor = co_await DeviceAccessService::instance().actor(c, c.userId);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceGroupEntity>().query_builder("g");
        DeviceAccessService::addVisibleGroupsCtes(queryBuilder, query, actor);
        ruvia::db_query visible(query.resource());
        visible.select(visible.column(service::device::entities::DeviceEntity::column_name<"id">())).from("visible_group");
        ruvia::db_query count(query.resource());
        count.select(count.aggregate("count", { count.star() }))
            .from("scoped_device", "scoped")
            .where(db::andAll(count, count.binary(count.column(service::device::entities::DeviceEntity::column_name<"group_id">(), "scoped"), ruvia::db_binary_operator::equal, count.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "g")), count.binary(count.column(kAccessRankColumn, "scoped"), ruvia::db_binary_operator::greater, db::integer(count, 0))));

        queryBuilder.select({ { "id", db::text(query, query.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "g")) }, { "name", query.column(service::device::entities::DeviceGroupEntity::column_name<"name">(), "g") }, { "parent_id", query.coalesce({ db::text(query, query.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), "g")), query.value("") }) }, { "status", query.column(service::device::entities::DeviceGroupEntity::column_name<"status">(), "g") }, { "sort_order", query.column(service::device::entities::DeviceGroupEntity::column_name<"sort_order">(), "g") }, { "remark", query.coalesce({ query.column(service::device::entities::DeviceGroupEntity::column_name<"remark">(), "g"), query.value("") }) }, { "value6", query.subquery(count) }, { "created_at", query.call("iot_utc_timestamp", { query.column(service::device::entities::DeviceGroupEntity::column_name<"created_at">(), "g") }) }, { "updated_at", query.call("iot_utc_timestamp", { query.column(service::device::entities::DeviceGroupEntity::column_name<"updated_at">(), "g") }) }, { "created_by", db::text(query, query.column(service::device::entities::DeviceGroupEntity::column_name<"created_by">(), "g")) } }).where(db::andAll(query, query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), "g")), query.binary(query.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "g"), ruvia::db_binary_operator::in, query.subquery(visible)))).order_by(query.column(service::device::entities::DeviceGroupEntity::column_name<"sort_order">(), "g")).add_order_by(query.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "g"));
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<DeviceGroupItemDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            fillGroup(result.emplace_back(ruvia::model_options{ .resource_ = c.arena() }), row, actor);
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> createGroup(Context& c, const CreateDeviceGroupBody& body) {
        co_await validateParent(c, body, std::nullopt);
        const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        const std::string name(body.template get<"name">().view());
        const std::string parentId = body.template get<"parentId">() ? std::string(body.template get<"parentId">()->view()) : "";
        const std::string status = body.template get<"status">() ? std::string(body.template get<"status">()->view()) : "enabled";
        const std::int64_t sortOrder =
            body.template get<"sortOrder">() ? static_cast<std::int64_t>(*body.template get<"sortOrder">()) : 0;
        const std::string remark = body.template get<"remark">() ? std::string(body.template get<"remark">()->view()) : "";
        ruvia::db_query insert(c.pool());
        const std::array<std::string_view, 7> insertColumns{ service::device::entities::DeviceGroupEntity::column_name<"id">(), service::device::entities::DeviceGroupEntity::column_name<"name">(), service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), service::device::entities::DeviceGroupEntity::column_name<"status">(), service::device::entities::DeviceGroupEntity::column_name<"sort_order">(), service::device::entities::DeviceGroupEntity::column_name<"remark">(), service::device::entities::DeviceGroupEntity::column_name<"created_by">() };
        auto insertBuilder = c.db().template repository<service::device::entities::DeviceGroupEntity>().insert_builder();
        ruvia::db_values_builder insertValues(insertColumns);
        insertValues.row({ db::uuid(insert, id), insert.value(name), db::nullableUuid(insert, parentId), insert.value(status), insert.value(sortOrder), insert.null_if(db::text(insert, insert.value(remark)), insert.value("")), db::uuid(insert, c.userId) });
        insertBuilder.insert_from(insertColumns, insertValues.query());
        (void)co_await insertBuilder.execute();
    }

    template <typename Context>
    ruvia::task<void> updateGroup(Context& c, std::string_view id, const UpdateDeviceGroupBody& body) {
        ruvia::db_query currentQuery(c.pool());
        auto currentQueryBuilder = c.db().template repository<service::device::entities::DeviceGroupEntity>().query_builder();
        currentQueryBuilder.select({ { "created_by", currentQuery.column(service::device::entities::DeviceGroupEntity::column_name<"created_by">()) } }).where(db::andAll(currentQuery, currentQuery.binary(currentQuery.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(currentQuery, id)), currentQuery.unary(ruvia::db_unary_operator::is_null, currentQuery.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">()))));
        const auto rows = co_await currentQueryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(17001, "设备分组不存在", 404);
        }
        co_await requireGroupOwner(c, rows.front()[0].value().value_or(std::string_view{}));
        co_await validateParent(c, body, std::string(id));

        ruvia::db_query update(c.pool());
        auto updateBuilder = c.db().template repository<service::device::entities::DeviceGroupEntity>().update_builder();

        bool changed = false;
        const auto assign = [&](std::string_view column, ruvia::db_expression value) {
            updateBuilder.set(column, value);
            changed = true;
        };
        if (body.template get<"name">()) {
            assign("name", update.value(body.template get<"name">()->view()));
        }
        if (body.template get<"parentId">()) {
            assign("parent_id", db::nullableUuid(update, body.template get<"parentId">()->view()));
        }
        if (body.template get<"status">()) {
            assign("status", update.value(body.template get<"status">()->view()));
        }
        if (body.template get<"sortOrder">()) {
            assign("sort_order", update.value(static_cast<std::int64_t>(*body.template get<"sortOrder">())));
        }
        if (body.template get<"remark">()) {
            assign("remark", update.null_if(db::text(update, update.value(body.template get<"remark">()->view())), update.value("")));
        }
        if (!changed) {
            co_return;
        }
        updateBuilder.set(service::device::entities::DeviceGroupEntity::column_name<"updated_at">(), update.call("now")).where(update.binary(update.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(update, id)));
        (void)co_await updateBuilder.execute();
    }

    template <typename Context>
    ruvia::task<void> removeGroup(Context& c, std::string_view id) {
        ruvia::db_query currentQuery(c.pool());
        auto currentQueryBuilder = c.db().template repository<service::device::entities::DeviceGroupEntity>().query_builder();
        currentQueryBuilder.select({ { "created_by", currentQuery.column(service::device::entities::DeviceGroupEntity::column_name<"created_by">()) } }).where(db::andAll(currentQuery, currentQuery.binary(currentQuery.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(currentQuery, id)), currentQuery.unary(ruvia::db_unary_operator::is_null, currentQuery.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">()))));
        const auto rows = co_await currentQueryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(17001, "设备分组不存在", 404);
        }
        co_await requireGroupOwner(c, rows.front()[0].value().value_or(std::string_view{}));
        ruvia::db_query childGroups(c.pool());
        childGroups.select(db::integer(childGroups, 1))
            .from(service::device::entities::DeviceGroupEntity::table_name())
            .where(db::andAll(childGroups, childGroups.binary(childGroups.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">()), ruvia::db_binary_operator::equal, db::uuid(childGroups, id)), childGroups.unary(ruvia::db_unary_operator::is_null, childGroups.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">()))));
        ruvia::db_query childDevices(c.pool());
        childDevices.select(db::integer(childDevices, 1))
            .from(service::device::entities::DeviceEntity::table_name())
            .where(db::andAll(childDevices, childDevices.binary(childDevices.column(service::device::entities::DeviceEntity::column_name<"group_id">()), ruvia::db_binary_operator::equal, db::uuid(childDevices, id)), childDevices.unary(ruvia::db_unary_operator::is_null, childDevices.column(service::device::entities::DeviceEntity::column_name<"deleted_at">()))));
        ruvia::db_query used(c.pool());
        auto usedBuilder = c.db().template repository<entities::DeviceGroupEntity>().query_builder();
        usedBuilder.from_nothing().select({ { "used", used.binary(used.exists(childGroups), ruvia::db_binary_operator::or_value, used.exists(childDevices)) } });
        const auto usedRows = co_await usedBuilder.get_raw_many();
        if (usedRows.front()[0].value().value_or(std::string_view{}) == "t") {
            service::common::fail(17004, "请先移除子分组和设备", 409);
        }
        ruvia::db_query removeQuery(c.pool());
        auto removeQueryBuilder = c.db().template repository<service::device::entities::DeviceGroupEntity>().update_builder();
        removeQueryBuilder.set(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), removeQuery.call("now")).set(service::device::entities::DeviceGroupEntity::column_name<"updated_at">(), removeQuery.call("now")).where(removeQuery.binary(removeQuery.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(removeQuery, id)));
        (void)co_await removeQueryBuilder.execute();
    }

  private:
    static std::int64_t toInt(std::string_view value, std::int64_t fallback = 0) {
        return service::utils::parseInt64(std::optional<std::string_view>{ value })
            .value_or(fallback);
    }

    static std::optional<double> parseDouble(std::string_view value) {
        while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
            value.remove_prefix(1);
        }
        while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
            value.remove_suffix(1);
        }
        if (value.empty()) {
            return std::nullopt;
        }
        return service::utils::decimal(value);
    }

    static double toDouble(std::string_view value, double fallback = 0) {
        return parseDouble(value).value_or(fallback);
    }

    template <typename Database>
    static auto historyQuery(Database&& database, std::pmr::memory_resource* resource, std::string_view deviceId, std::string_view start, std::string_view end, std::int64_t pageSize, std::int64_t offset, std::int64_t page) {
        const auto timestamp = [](ruvia::db_query& query, std::string_view value) {
            return query.cast(query.value(value), ruvia::db_data_type::timestamp_tz);
        };
        ruvia::db_query counted(resource);
        const auto countedData = counted.column(service::device::entities::DeviceDataEntity::column_name<"data">(), "record");
        counted.select(counted.aggregate("count", { counted.star() }))
            .from(service::device::entities::DeviceDataEntity::table_name(), "record")
            .where(db::andAll(counted, counted.binary(counted.column(service::device::entities::DeviceDataEntity::column_name<"device_id">(), "record"), ruvia::db_binary_operator::equal, db::uuid(counted, deviceId)), counted.binary(counted.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::greater_equal, timestamp(counted, start)), counted.binary(counted.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::less_equal, timestamp(counted, end)), counted.binary(counted.call("jsonb_typeof", { db::jsonValue(counted, countedData, "values") }), ruvia::db_binary_operator::equal, counted.value("object"))));

        ruvia::db_query filtered(resource);
        const auto filteredData = filtered.column(service::device::entities::DeviceDataEntity::column_name<"data">(), "record");
        filtered
            .select({ filtered.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "record"), filtered.column(service::device::entities::DeviceDataEntity::column_name<"protocol">(), "record"), filtered.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), filtered.column(service::device::entities::DeviceDataEntity::column_name<"source">(), "record"), filtered.column(service::device::entities::DeviceDataEntity::column_name<"raw_payload_hex">(), "record"), filteredData })
            .from(service::device::entities::DeviceDataEntity::table_name(), "record")
            .where(db::andAll(filtered, filtered.binary(filtered.column(service::device::entities::DeviceDataEntity::column_name<"device_id">(), "record"), ruvia::db_binary_operator::equal, db::uuid(filtered, deviceId)), filtered.binary(filtered.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::greater_equal, timestamp(filtered, start)), filtered.binary(filtered.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_binary_operator::less_equal, timestamp(filtered, end)), filtered.binary(filtered.call("jsonb_typeof", { db::jsonValue(filtered, filteredData, "values") }), ruvia::db_binary_operator::equal, filtered.value("object"))))
            .order_by(filtered.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "record"), ruvia::db_order_direction::desc)
            .add_order_by(filtered.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "record"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>(offset));

        ruvia::db_query normalized(resource);
        ruvia::db_query normalizedValues(resource);
        const auto point = normalizedValues.column(kJsonEachValueColumn, "point");
        const auto pointValue = db::jsonValue(
            normalizedValues,
            point,
            "value"
        );
        const auto pointObject = db::andAll(
            normalizedValues,
            normalizedValues.binary(normalizedValues.call("jsonb_typeof", { point }), ruvia::db_binary_operator::equal, normalizedValues.value("object")),
            normalizedValues.binary(
                normalizedValues.call("jsonb_typeof", { pointValue }),
                ruvia::db_binary_operator::equal,
                normalizedValues.value("boolean")
            )
        );
        const auto pointBoolean = normalizedValues.cast(
            db::jsonText(normalizedValues, point, "value"),
            ruvia::db_data_type::boolean
        );
        const auto pointNumber = normalizedValues.case_when(
            { { pointBoolean, db::integer(normalizedValues, 1) } },
            db::integer(normalizedValues, 0)
        );
        const auto pointPath = normalizedValues.cast(
            normalizedValues.array({ db::text(normalizedValues, "value") }),
            ruvia::db_type_definition{ .data_type_ = ruvia::db_data_type::text, .array_ = true }
        );
        const auto normalizedPoint = normalizedValues.case_when(
            { { pointObject,
                normalizedValues.call("jsonb_set", { point, pointPath, normalizedValues.call("to_jsonb", { pointNumber }), db::boolean(normalizedValues, false) }) } },
            point
        );
        const std::array<ruvia::db_source_column, 2> sourceColumns6{ { { .name_ = "key" }, { .name_ = "value" } } };
        normalizedValues
            .select(normalizedValues.aggregate("jsonb_object_agg", { normalizedValues.column(kJsonEachKeyColumn, "point"), normalizedPoint }))

            .from_function(
                normalizedValues.call(
                    "jsonb_each",
                    { normalizedValues.coalesce(
                        { db::jsonValue(
                              normalizedValues,
                              normalizedValues.column(service::device::entities::DeviceDataEntity::column_name<"data">(), "filtered"),
                              "values"
                          ),
                          normalizedValues.cast(normalizedValues.value("{}"), ruvia::db_data_type::jsonb) }
                    ) }
                ),
                "point",
                { .lateral_ = true,
                  .columns_ = sourceColumns6 }
            );
        normalized
            .select({ normalized.star("filtered"), normalized.alias(normalized.coalesce({ normalized.subquery(normalizedValues), normalized.cast(normalized.value("{}"), ruvia::db_data_type::jsonb) }), "normalized_values") })
            .from(filtered, "filtered");

        ruvia::db_query query(resource);
        auto queryBuilder = database.template repository<service::device::entities::DeviceDataEntity>().query_builder("normalized");
        queryBuilder.with("counted", counted).with("filtered", filtered).with("normalized", normalized);
        const auto total = query.coalesce({ query.subquery(counted), db::integer(query, 0) });
        const auto item = query.call(
            "jsonb_build_object",
            { db::text(query, "id"), query.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "normalized"), db::text(query, "protocol"), query.column(service::device::entities::DeviceDataEntity::column_name<"protocol">(), "normalized"), db::text(query, "reportTime"), query.call("iot_utc_timestamp", { query.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "normalized") }), db::text(query, "source"), query.column(service::device::entities::DeviceDataEntity::column_name<"source">(), "normalized"), db::text(query, "functionCode"), db::jsonText(query, query.column(service::device::entities::DeviceDataEntity::column_name<"data">(), "normalized"), "function_code"), db::text(query, "values"), query.column(kNormalizedValuesColumn, "normalized"), db::text(query, "rawPayloadHex"), query.coalesce({ query.column(service::device::entities::DeviceDataEntity::column_name<"raw_payload_hex">(), "normalized"), query.cast(query.value("[]"), ruvia::db_data_type::jsonb) }) }
        );
        const std::array<ruvia::db_order_term, 2> historyOrder{ { ruvia::db_order_term{ query.column(service::device::entities::DeviceDataEntity::column_name<"report_time">(), "normalized"),
                                                                                        ruvia::db_order_direction::desc,
                                                                                        ruvia::db_nulls_order::default_value },
                                                                  ruvia::db_order_term{ query.column(service::device::entities::DeviceDataEntity::column_name<"id">(), "normalized"),
                                                                                        ruvia::db_order_direction::desc,
                                                                                        ruvia::db_nulls_order::default_value } } };
        const auto list = query.coalesce(
            { query.aggregate("jsonb_agg", { item }, false, historyOrder),
              query.cast(query.value("[]"), ruvia::db_data_type::jsonb) }
        );
        const auto totalPages = query.cast(
            query.call("ceil", { query.binary(query.cast(total, ruvia::db_data_type::numeric), ruvia::db_binary_operator::divide, query.cast(query.value(pageSize), ruvia::db_data_type::numeric)) }),
            ruvia::db_data_type::big_int
        );
        queryBuilder.select({ { "value0", query.cast(query.call("jsonb_build_object", { db::text(query, "list"), list, db::text(query, "total"), total, db::text(query, "page"), query.cast(query.value(page), ruvia::db_data_type::big_int), db::text(query, "pageSize"), query.cast(query.value(pageSize), ruvia::db_data_type::big_int), db::text(query, "totalPages"), totalPages }), ruvia::db_data_type::text) } }).from_cte("normalized");
        return queryBuilder;
    }

    // 列顺序必须与 fillItem 的 row 下标严格对应。
    static void selectItemColumns(auto& target, ruvia::db_query& query) {
        const auto protocolParams = query.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "d");
        const auto endpoint = query.column(service::link::entities::LinkEntity::column_name<"endpoint">(), "l");
        const auto protocolConfig = query.column(service::device::entities::DeviceModelEntity::column_name<"config">(), "p");
        const auto emptyJson = query.cast(query.value("{}"), ruvia::db_data_type::jsonb);
        const auto emptyArray = query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
        const auto nullableJsonText = [&](std::string_view key) {
            return query.null_if(db::jsonText(query, protocolParams, key), query.value(""));
        };
        const auto numericText = db::jsonText(query, protocolParams, "online_timeout");
        const auto numericOnlineTimeout = query.case_when(
            { { query.binary(query.coalesce({ numericText, query.value("") }), ruvia::db_binary_operator::regex, query.value("^-?[0-9]{1,18}$")),
                query.cast(numericText, ruvia::db_data_type::integer) } }
        );
        const auto onlineTimeout = query.coalesce({ numericOnlineTimeout, db::integer(query, 300) });
        const auto timezone = query.coalesce(
            { query.null_if(db::jsonText(query, protocolParams, "timezone"), query.value("")),
              query.value(service::message::device::kDefaultTimezone) }
        );
        const auto edgeExecution = query.binary(query.column(service::link::entities::LinkEntity::column_name<"execution">(), "l"), ruvia::db_binary_operator::equal, query.value("edge"));

        ruvia::db_query functionCount(query.resource());
        const auto functions = functionCount.coalesce(
            { db::jsonValue(functionCount, functionCount.column(service::device::entities::DeviceModelEntity::column_name<"config">(), "p"), "funcs"),
              functionCount.cast(functionCount.value("[]"), ruvia::db_data_type::jsonb) }
        );
        const auto function = functionCount.column(kJsonEachValueColumn, "function");
        const auto elements = functionCount.coalesce(
            { db::jsonValue(functionCount, function, "elements"),
              functionCount.cast(functionCount.value("[]"), ruvia::db_data_type::jsonb) }
        );
        const auto responseElements = functionCount.coalesce(
            { db::jsonValue(functionCount, function, "responseElements"),
              functionCount.cast(functionCount.value("[]"), ruvia::db_data_type::jsonb) }
        );
        const auto elementCount = functionCount.call(
            "jsonb_array_length",
            { elements }
        );
        const auto responseElementCount = functionCount.call(
            "jsonb_array_length",
            { responseElements }
        );
        const std::array<ruvia::db_source_column, 1> sourceColumns7{ { { .name_ = "value" } } };
        functionCount
            .select(functionCount.coalesce(
                { functionCount.aggregate(
                      "sum",
                      { functionCount.binary(elementCount, ruvia::db_binary_operator::add, responseElementCount) }
                  ),
                  db::integer(functionCount, 0) }
            ))

            .from_function(functionCount.call("jsonb_array_elements", { functions }), "function", { .columns_ = sourceColumns7 });
        const auto protocolElementCount = query.case_when(
            { { query.binary(query.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, query.value("Modbus")),
                query.call("jsonb_array_length", { query.coalesce({ db::jsonValue(query, protocolConfig, "registers"), emptyArray }) }) },
              { query.binary(query.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, query.value("S7")),
                query.call("jsonb_array_length", { query.coalesce({ db::jsonValue(query, protocolConfig, "areas"), emptyArray }) }) } },
            query.case_when({ { query.binary(query.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, query.value("SL651")), query.coalesce({ query.subquery(functionCount), db::integer(query, 0) }) } }, query.call("jsonb_array_length", { query.coalesce({ db::jsonValue(query, protocolConfig, "points"), emptyArray }) }))
        );

        target.select({ { "id", db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"id">(), "d")) }, { "name", query.column(service::device::entities::DeviceEntity::column_name<"name">(), "d") }, { "value2", db::jsonText(query, protocolParams, "device_code") }, { "link_id", db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "d")) }, { "value4", nullableJsonText("target_id") }, { "protocol_config_id", db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"protocol_config_id">(), "d")) }, { "group_id", db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"group_id">(), "d")) }, { "status", query.column(service::device::entities::DeviceEntity::column_name<"status">(), "d") }, { "value8", onlineTimeout }, { "value9", DeviceAccessService::remoteControlEnabled(query, protocolParams) }, { "value10", nullableJsonText("modbus_mode") }, { "value11", nullableJsonText("slave_id") }, { "value12", timezone }, { "value13", db::jsonText(query, db::jsonValue(query, protocolParams, "heartbeat"), "mode") }, { "value14", db::jsonText(query, db::jsonValue(query, protocolParams, "heartbeat"), "content") }, { "value15", db::jsonText(query, db::jsonValue(query, protocolParams, "registration"), "mode") }, { "value16", db::jsonText(query, db::jsonValue(query, protocolParams, "registration"), "content") }, { "remark", query.coalesce({ query.column(service::device::entities::DeviceEntity::column_name<"remark">(), "d"), query.value("") }) }, { "created_by", db::text(query, query.column(service::device::entities::DeviceEntity::column_name<"created_by">(), "d")) }, { "created_at", query.call("iot_utc_timestamp", { query.column(service::device::entities::DeviceEntity::column_name<"created_at">(), "d") }) }, { "updated_at", query.call("iot_utc_timestamp", { query.column(service::device::entities::DeviceEntity::column_name<"updated_at">(), "d") }) }, { "value21", query.coalesce({ query.column(service::link::entities::LinkEntity::column_name<"name">(), "l"), query.value("") }) }, { "value22", query.coalesce({ db::jsonText(query, endpoint, "mode"), query.value("") }) }, { "protocol", query.coalesce({ query.column(service::link::entities::LinkEntity::column_name<"protocol">(), "l"), query.value("") }) }, { "value24", query.column(service::device::entities::DeviceModelEntity::column_name<"name">(), "p") }, { "value25", query.column(service::device::entities::DeviceModelEntity::column_name<"protocol">(), "p") }, { "value26", query.null_if(db::jsonText(query, protocolConfig, "readInterval"), query.value("")) }, { "value27", query.null_if(db::jsonText(query, protocolConfig, "storagePolicy"), query.value("")) }, { "value28", protocolElementCount }, { "value29", query.column(kAccessRankColumn, "d") }, { "edge_node_id", query.case_when({ { edgeExecution, db::text(query, query.column(service::link::entities::LinkEntity::column_name<"edge_node_id">(), "l")) } }) }, { "value31", query.coalesce({ query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"name">(), "en"), query.value("") }) }, { "imei", query.coalesce({ query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"imei">(), "en"), query.value("") }) }, { "value33", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "transport"), query.value("")) } }) }, { "value34", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "interface"), query.value("")) } }) }, { "value35", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "mode"), query.value("")) } }) }, { "value36", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "ip"), query.value("")) } }) }, { "value37", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "port"), query.value("")) } }) }, { "value38", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "baud_rate"), query.value("")) } }) }, { "value39", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "data_bits"), query.value("")) } }) }, { "value40", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "stop_bits"), query.value("")) } }) }, { "value41", query.case_when({ { edgeExecution, query.null_if(db::jsonText(query, endpoint, "parity"), query.value("")) } }) }, { "debug_enabled", query.column(service::device::entities::DeviceEntity::column_name<"debug_enabled">(), "d") }, { "value43", query.column(service::link::entities::LinkEntity::column_name<"debug_enabled">(), "l") }, { "value44", query.coalesce({ db::jsonValue(query, protocolParams, "parameters"), query.cast(query.value("{}"), ruvia::db_data_type::jsonb) }) }, { "config", query.column(service::device::entities::DeviceModelEntity::column_name<"config">(), "p") } });
    }

    static std::vector<std::string> mqttCardPointIds(std::string_view wire) {
        const auto config = ruvia::from_json<DeviceMqttCardConfiguration>(wire);

        std::set<std::string, std::less<>> reported;
        const auto& reports = config.get<"reports">();
        if (reports) {
            for (const auto& report : *reports) {
                for (const auto& field : report.get<"pointFields">()) {
                    reported.emplace(field.get<"pointId">().view());
                }
            }
        }
        const auto visible = [&](const DeviceCardPoint& point) {
            if (const auto& visibility = config.get<"pointVisibility">(); visibility) {
                if (const auto overrideValue = visibility->get<ruvia::bool_value>(point.get<"id">().view())) {
                    return static_cast<bool>(*overrideValue);
                }
            }
            return static_cast<bool>(point.get<"visible">().value_or(ruvia::bool_value{ true }));
        };
        std::vector<std::string> result;
        for (const auto& point : config.get<"points">()) {
            const bool mapped = reports ? reported.contains(point.get<"id">().view())
                                        : point.get<"field">() && !point.get<"field">()->view().empty();
            if (mapped && visible(point)) {
                result.emplace_back(point.get<"id">().view());
            }
        }
        if (const auto& derived = config.get<"derivedPoints">(); derived) {
            for (const auto& point : *derived) {
                if (visible(point)) {
                    result.emplace_back(point.get<"id">().view());
                }
            }
        }
        return result;
    }

    template <typename Context, typename Row>
    static void fillItem(Context& c, DeviceItemDto& item, Row&& row, const DeviceActor& actor) {
        auto parameters = ruvia::json_object::parse(row[44].value().value_or("{}"));

        ruvia::json_object ownedParameters({ .resource_ = c.arena() });
        ownedParameters = std::move(parameters);
        item.template set<"parameters">(std::move(ownedParameters));
        auto parameterConfig = ruvia::from_json<service::common::DeviceParameterConfiguration>(row[45].value().value_or("{}"), { .resource_ = c.arena() });
        if (parameterConfig.template get<"identityProperty">()) {
            item.template set<"identityProperty">(*parameterConfig.template get<"identityProperty">());
        }
        if (parameterConfig.template get<"parameters">()) {
            item.template set<"parameterDefinitions">(std::move(*parameterConfig.template get<"parameters">()));
        }
        item.template set<"id">(row[0].value().value_or(std::string_view{}));
        item.template set<"name">(row[1].value().value_or(std::string_view{}));
        item.template set<"debugEnabled">(row[42].value().value_or("") == "t");
        item.template set<"linkDebugEnabled">(row[43].value().value_or("") == "t");
        item.template set<"deviceCode">(row[2].value().value_or(std::string_view{}));
        if (row[3].value().has_value()) {
            item.template set<"linkId">(row[3].value().value_or(std::string_view{}));
        }
        if (row[4].value().has_value()) {
            item.template set<"targetId">(row[4].value().value_or(std::string_view{}));
        }
        item.template set<"protocolConfigId">(row[5].value().value_or(std::string_view{}));
        if (row[6].value().has_value()) {
            item.template set<"groupId">(row[6].value().value_or(std::string_view{}));
        }
        item.template set<"status">(row[7].value().value_or(std::string_view{}));
        item.template set<"onlineTimeout">(toInt(row[8].value().value_or(std::string_view{})));
        item.template set<"remoteControl">(row[9].value().value_or(std::string_view{}) == "t");
        if (row[10].value().has_value()) {
            item.template set<"modbusMode">(row[10].value().value_or(std::string_view{}));
        }
        if (row[11].value().has_value()) {
            item.template set<"slaveId">(toInt(row[11].value().value_or(std::string_view{})));
        }
        item.template set<"timezone">(row[12].value().value_or(std::string_view{}));
        {
            DevicePacketDto heartbeat(ruvia::model_options{ .resource_ = c.arena() });
            if (row[13].value().has_value()) {
                heartbeat.template set<"mode">(row[13].value().value_or(std::string_view{}));
            }
            if (row[14].value().has_value()) {
                heartbeat.template set<"content">(row[14].value().value_or(std::string_view{}));
            }
            item.template set<"heartbeat">(std::move(heartbeat));
        }
        if (!row[30].value().has_value()) {
            DevicePacketDto registration(ruvia::model_options{ .resource_ = c.arena() });
            if (row[15].value().has_value()) {
                registration.template set<"mode">(row[15].value().value_or(std::string_view{}));
            }
            if (row[16].value().has_value()) {
                registration.template set<"content">(row[16].value().value_or(std::string_view{}));
            }
            item.template set<"registration">(std::move(registration));
        }
        item.template set<"remark">(row[17].value().value_or(std::string_view{}));
        item.template set<"createdBy">(row[18].value().value_or(std::string_view{}));
        item.template set<"createdAt">(row[19].value().value_or(std::string_view{}));
        item.template set<"updatedAt">(row[20].value().value_or(std::string_view{}));
        item.template set<"linkName">(row[21].value().value_or(std::string_view{}));
        item.template set<"linkMode">(row[22].value().value_or(std::string_view{}));
        item.template set<"linkProtocol">(row[23].value().value_or(std::string_view{}));
        item.template set<"protocolName">(row[24].value().value_or(std::string_view{}));
        item.template set<"protocolType">(row[25].value().value_or(std::string_view{}));
        if (row[26].value().has_value()) {
            if (const auto value = parseDouble(row[26].value().value_or(std::string_view{}))) {
                item.template set<"readInterval">(*value);
            }
        }
        if (row[27].value().has_value()) {
            item.template set<"storagePolicy">(row[27].value().value_or(std::string_view{}));
        }
        const auto capabilities = DeviceAccessService::capabilities(
            actor,
            DeviceAccessService::rank(row[29].value().value_or(std::string_view{})),
            row[9].value().value_or(std::string_view{}) == "t"
        );
        item.template set<"elementCount">(toInt(row[28].value().value_or(std::string_view{})));
        if (row[25].value().value_or("") == "MQTT") {
            const auto points = mqttCardPointIds(row[45].value().value_or("{}"));
            ruvia::array<ruvia::string> ids(ruvia::model_options{ .resource_ = c.arena() });
            for (const auto& id : points) {
                ids.emplace_back(id);
            }
            item.template set<"cardPointIds">(std::move(ids));
            item.template set<"elementCount">(static_cast<std::int64_t>(points.size()));
        }
        item.template set<"connected">(false);
        item.template set<"connectionState">("disconnected");
        item.template set<"runtimeState">("offline");
        item.template set<"elements">(ruvia::boxed_array<DeviceElementDto>(
            ruvia::model_options{ .resource_ = c.arena() }
        ));
        item.template set<"canEdit">(capabilities.canEdit);
        item.template set<"canDelete">(capabilities.canDelete);
        item.template set<"canShare">(capabilities.canShare);
        item.template set<"canCommand">(capabilities.canCommand);
        item.template set<"accessLevel">(capabilities.accessLevel);
        if (row[30].value().has_value()) {
            item.template set<"edgeNodeId">(row[30].value().value_or(std::string_view{}));
            item.template set<"edgeNodeName">(row[31].value().value_or(std::string_view{}));
            item.template set<"edgeNodeImei">(row[32].value().value_or(std::string_view{}));
        }
        if (row[33].value().has_value()) {
            item.template set<"edgeTransport">(row[33].value().value_or(std::string_view{}));
        }
        if (row[34].value().has_value()) {
            item.template set<"edgeInterface">(row[34].value().value_or(std::string_view{}));
        }
        if (row[35].value().has_value()) {
            item.template set<"edgeMode">(row[35].value().value_or(std::string_view{}));
        }
        if (row[36].value().has_value()) {
            item.template set<"edgeIp">(row[36].value().value_or(std::string_view{}));
        }
        if (row[37].value().has_value()) {
            item.template set<"edgePort">(toInt(row[37].value().value_or(std::string_view{})));
        }
        if (row[38].value().has_value()) {
            item.template set<"serialBaudRate">(toInt(row[38].value().value_or(std::string_view{})));
        }
        if (row[39].value().has_value()) {
            item.template set<"serialDataBits">(toInt(row[39].value().value_or(std::string_view{})));
        }
        if (row[40].value().has_value()) {
            item.template set<"serialStopBits">(toInt(row[40].value().value_or(std::string_view{})));
        }
        if (row[41].value().has_value()) {
            item.template set<"serialParity">(row[41].value().value_or(std::string_view{}));
        }
    }

    template <typename Context, typename Item>
    static ruvia::task<void>
    fillLatest(Context& c, const std::map<std::string, Item*, std::less<>>& items) {
        if (items.empty()) {
            co_return;
        }
        auto pipeline = c.redis().pipeline();
        enum class ReplyKind { runtime,
                               latest,
                               edgeDevice };

        struct ReplyBinding {
            ReplyKind kind;
            Item* item;
        };

        std::vector<ReplyBinding> bindings;
        bindings.reserve(items.size() * 3);
        for (const auto& [id, item] : items) {
            (void)id;
            if (!item->template get<"deviceCode">()) {
                continue;
            }
            // 原子核验连接租约与链路归属；读取不会延长租约，也不读取历史连接身份。
            pipeline.command(
                "EVAL",
                entities::kDeviceConnectionStateScript,
                "2",
                service::message::telemetry::runtimeKey(id),
                service::message::collector::connectionKey(id),
                service::message::collector::kLinkOwnerPrefix
            );
            bindings.push_back({ ReplyKind::runtime, item });
            pipeline.hget_all(service::message::telemetry::latestKey(id));
            bindings.push_back({ ReplyKind::latest, item });
            if (item->template get<"edgeNodeId">() &&
                item->template get<"edgeTransport">() &&
                item->template get<"edgeTransport">()->view() == "tcp") {
                pipeline.command(
                    "HMGET",
                    service::message::edge::deviceStatusKey(
                        item->template get<"edgeNodeId">()->view(),
                        item->template get<"id">()->view()
                    ),
                    "state",
                    "reason",
                    "client_count",
                    "last_activity_at_ms"
                );
                bindings.push_back({ ReplyKind::edgeDevice, item });
            }
        }
        const auto replies = co_await std::move(pipeline).exec();
        for (std::size_t index = 0; index < bindings.size() && index < replies.size(); ++index) {
            const auto& binding = bindings[index];
            if (binding.kind == ReplyKind::runtime) {
                applyRuntime(*binding.item, replies[index]);
            } else if (binding.kind == ReplyKind::latest) {
                applyLatestElements(c, *binding.item, replies[index]);
            } else {
                applyEdgeRuntime(c, *binding.item, replies[index]);
            }
        }
    }

    static std::string redisArrayField(const ruvia::redis_value& value, std::size_t index) {
        if (value.kind() != ruvia::redis_value::kind_type::array) {
            return {};
        }
        const auto entries = value.array();
        if (index >= entries.size() || entries[index].kind() != ruvia::redis_value::kind_type::string) {
            return {};
        }
        return std::string(entries[index].string());
    }

    static std::optional<std::string> jsonString(const ruvia::json_value& object, std::string_view field) {
        const auto value = object.template get<ruvia::string>(field);
        if (!value) {
            return std::nullopt;
        }
        return std::string(value->view());
    }

    static std::int64_t jsonInt(const ruvia::json_value& object, std::string_view field, std::int64_t fallback) {
        if (const auto value = object.template get<ruvia::int64>(field)) {
            return static_cast<std::int64_t>(*value);
        }
        const auto raw = object.get<ruvia::json_value>(field);
        if (!raw) {
            return fallback;
        }
        return service::utils::parseInt64(std::optional<std::string_view>{ raw->view() })
            .value_or(fallback);
    }

    static double jsonDouble(const ruvia::json_value& object, std::string_view field, double fallback) {
        const auto raw = object.get<ruvia::json_value>(field);
        if (!raw) {
            return fallback;
        }
        return toDouble(raw->view(), fallback);
    }

    template <typename Item>
    static void applyRuntime(Item& item, const ruvia::redis_value& reply) {
        const auto reportTime = redisArrayField(reply, 1);
        if (!reportTime.empty()) {
            const auto milliseconds = toInt(reportTime);
            if (milliseconds > 0) {
                item.template set<"reportTime">(
                    service::common::utcTimestampFromMilliseconds(milliseconds)
                );
            }
        }
        const bool connected = !redisArrayField(reply, 0).empty();
        item.template set<"connected">(connected);
        item.template set<"connectionState">(
            connected ? "connected" : "disconnected"
        );
        // 在线状态由遥测运行投影按设备在线窗口计算并在到期时推送，客户端不再自行推导。
        const auto state = redisArrayField(reply, 2);
        item.template set<"runtimeState">(state == "online" ? "online" : "offline");
        const auto reason = redisArrayField(reply, 3);
        if (!reason.empty()) {
            item.template set<"runtimeReason">(reason);
        }
    }

    template <typename Context, typename Item>
    static void applyEdgeRuntime(Context& c, Item& item, const ruvia::redis_value& reply) {
        const auto state = redisArrayField(reply, 0);
        if (state.empty()) {
            return;
        }
        const bool connected = state == "connected" || state == "online";
        item.template set<"connected">(connected);
        item.template set<"connectionState">(
            connected ? "connected" : "disconnected"
        );
        EdgeStatusDto status(ruvia::model_options{ .resource_ = c.arena() });
        status.template set<"state">(state);
        const auto reason = redisArrayField(reply, 1);
        if (!reason.empty()) {
            status.template set<"reason">(reason);
        }
        const auto clientCount = redisArrayField(reply, 2);
        if (!clientCount.empty()) {
            status.template set<"clientCount">(toInt(clientCount));
        }
        const auto lastActivity = redisArrayField(reply, 3);
        if (!lastActivity.empty()) {
            const auto milliseconds = toInt(lastActivity);
            if (milliseconds > 0) {
                status.template set<"lastActivityAt">(
                    service::common::utcTimestampFromMilliseconds(milliseconds)
                );
            }
        }
        item.template set<"edgeStatus">(std::move(status));
    }

    struct LatestElement final {
        bool visible = true;
        std::string quality;
        std::int64_t sort = 0;
        std::int64_t observedAt = 0;
        double scale = 1.0;
        std::int64_t decimals = -1;
        std::string id;
        std::string name;
        std::string value{ "-" };
        std::string unit;
        std::string dataType;
        std::string group;
        std::string encode;
    };

    template <typename Context, typename Item>
    static void applyLatestElements(Context& c, Item& item, const ruvia::redis_value& reply) {
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            return;
        }
        bool hasElementIds = false;
        std::set<std::string, std::less<>> elementIds;
        std::map<std::string, LatestElement, std::less<>> latest;

        const auto& entries = reply.array();
        for (std::size_t index = 0; index + 1 < entries.size(); index += 2) {
            if (entries[index].kind() != ruvia::redis_value::kind_type::string ||
                entries[index + 1].kind() != ruvia::redis_value::kind_type::string) {
                continue;
            }
            const auto field = entries[index].string();
            if (field == "_element_ids") {
                hasElementIds = true;
                const auto parsed = ruvia::json_value::parse(entries[index + 1].string());
                if (parsed.is_object()) {
                    (void)parsed.for_each_field(
                        [&](std::string_view key, const ruvia::json_value&) {
                            if (!key.empty()) {
                                elementIds.emplace(key);
                            }
                            return true;
                        }
                    );
                }
                continue;
            }
            if (field.empty() || field.front() == '_') {
                continue;
            }
            const auto parsed = ruvia::json_value::parse(entries[index + 1].string());
            if (!parsed.is_object()) {
                continue;
            }
            LatestElement element;
            element.id = jsonString(parsed, "id").value_or(std::string(field));
            element.name = jsonString(parsed, "name").value_or(element.id);
            element.dataType = jsonString(parsed, "dataType").value_or("");
            element.value = service::message::telemetry::canonicalPointText(
                jsonString(parsed, "value").value_or("-"),
                element.dataType
            );
            element.unit = jsonString(parsed, "unit").value_or("");
            if (const auto visible = parsed.template get<ruvia::bool_value>("visible")) {
                element.visible = static_cast<bool>(*visible);
            }
            element.quality = jsonString(parsed, "quality").value_or("missing");
            element.group = jsonString(parsed, "group").value_or("");
            element.encode = jsonString(parsed, "encode").value_or("");
            element.scale = jsonDouble(parsed, "scale", 1.0);
            element.decimals = jsonInt(parsed, "decimals", -1);
            element.sort = jsonInt(parsed, "sort", 0);
            element.observedAt = jsonInt(parsed, "observedAt", 0);
            const auto elementId = element.id;
            latest.insert_or_assign(elementId, std::move(element));
        }

        std::vector<LatestElement> elements;
        elements.reserve(latest.size());
        for (auto& [id, element] : latest) {
            if (hasElementIds && !elementIds.contains(id)) {
                continue;
            }
            elements.push_back(std::move(element));
        }
        std::sort(elements.begin(), elements.end(), [](const auto& left, const auto& right) {
            if (left.sort != right.sort) {
                return left.sort < right.sort;
            }
            return left.id < right.id;
        });
        ruvia::boxed_array<DeviceElementDto> dtos(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        std::int64_t reportTime = 0;
        for (const auto& element : elements) {
            auto& dto = dtos.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            dto.template set<"id">(element.id)
                .template set<"name">(element.name)
                .template set<"value">(element.value)
                .template set<"unit">(element.unit)
                .template set<"visible">(element.visible)
                .template set<"quality">(element.quality)
                .template set<"scale">(element.scale)
                .template set<"decimals">(element.decimals);
            if (!element.group.empty()) {
                dto.template set<"group">(element.group);
            }
            if (!element.encode.empty()) {
                dto.template set<"encode">(element.encode);
            }
            reportTime = std::max(reportTime, element.observedAt);
        }
        item.template set<"elements">(std::move(dtos));
        if (reportTime > 0 && !item.template get<"reportTime">()) {
            item.template set<"reportTime">(
                service::common::utcTimestampFromMilliseconds(reportTime)
            );
        }
    }

    static std::string str(const ruvia::string& value) { return std::string(value.view()); }

    static std::string str(const std::optional<ruvia::string>& value) {
        return value ? std::string(value->view()) : std::string{};
    }

    // 未提供返回空串（SQL 里以 NULLIF 回退到默认或当前值）；提供则序列化为 {"mode":..,"content":..}
    static std::string packetJson(const std::optional<DevicePacketBody>& packet) {
        if (!packet) {
            return "";
        }
        std::string out = "{\"mode\":";
        out += service::utils::jsonQuoted(packet->template get<"mode">() ? packet->template get<"mode">()->view() : std::string_view("OFF"));
        if (packet->template get<"content">()) {
            out += ",\"content\":";
            out += service::utils::jsonQuoted(packet->template get<"content">()->view());
        }
        out.push_back('}');
        return out;
    }

    template <typename Context>
    ruvia::task<void> lockConnectionNodes(Context& c, ruvia::db_transaction& transaction, std::string_view prior, std::string_view next) {
        std::set<std::string> nodes;
        if (!prior.empty()) {
            nodes.emplace(prior);
        }
        if (!next.empty()) {
            nodes.emplace(next);
        }
        for (const auto& id : nodes) {
            if (!service::common::isUuid(id)) {
                service::common::fail(18002, "边缘节点 ID 无效", 400);
            }
            ruvia::db_query query(c.pool());
            auto queryBuilder = transaction.template repository<service::edge_node::entities::EdgeNodeEntity>().query_builder();
            queryBuilder.select({ { "id", query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">()) } }).where((service::edge_node::entities::EdgeNodeEntity::column<"id">() == id).expression(query)).set_lock(ruvia::db_lock_options{});
            (void)co_await queryBuilder.get_raw_many();
        }
    }

    template <typename Context, typename Database>
    static ruvia::task<std::optional<DeviceExternalIoBody>> loadExternalIo(Context& c, Database&& database, std::string_view id) {
        using Source = entities::ExternalIoSourceEntity;
        ruvia::db_query query(c.pool());
        auto builder = database.template repository<Source>().query_builder();
        builder.select({ { "model", db::text(query, Source::column<"protocol_config_id">().expression(query)) }, { "derived", Source::column<"derived_config">().expression(query) } })
            .where((Source::column<"device_id">() == id).expression(query));
        const auto rows = co_await builder.get_raw_many();
        if (rows.empty()) {
            co_return std::nullopt;
        }
        DeviceExternalIoBody result(ruvia::model_options{ .resource_ = c.arena() });
        result.template set<"protocolConfigId">(rows.front()[0].value().value_or(""));
        const auto derived = ruvia::from_json<service::protocol::DerivedPointsConfig>(rows.front()[1].value().value_or("{}"));
        if (derived.template get<"derivedPoints">()) {
            result.template set<"derivedPoints">(*derived.template get<"derivedPoints">());
        }
        co_return result;
    }

    template <typename Body>
    static void validateStandaloneIo(const Body& body, std::string_view modelId) {
        if (body.template is_present<"externalIo">() &&
            (!body.template get<"externalIo">() || body.template get<"externalIo">()->template get<"protocolConfigId">().view() != modelId ||
             (body.template get<"externalIo">()->template get<"derivedPoints">() && !body.template get<"externalIo">()->template get<"derivedPoints">()->empty()))) {
            service::common::fail(18002, "独立外部 IO 只使用所选 IO 方案", 400);
        }
    }

    template <typename Context>
    static ruvia::task<void> clearExternalIo(Context& c, ruvia::db_transaction& transaction, std::string_view id) {
        using Source = entities::ExternalIoSourceEntity;
        ruvia::db_query query(c.pool());
        auto builder = transaction.template repository<Source>().delete_builder();
        builder.where((Source::column<"device_id">() == id).expression(query));
        (void)co_await builder.execute();
    }

    template <typename Context>
    static ruvia::task<void> saveExternalIo(Context& c, ruvia::db_transaction& transaction, std::string_view id, std::string_view linkId, std::string_view primaryModelId, const DeviceExternalIoBody& input) {
        using Link = service::link::entities::LinkEntity;
        using Node = service::edge_node::entities::EdgeNodeEntity;
        using Model = service::protocol::entities::ProtocolConfigEntity;
        using Source = entities::ExternalIoSourceEntity;
        using Op = ruvia::db_binary_operator;
        ruvia::db_query query(c.pool());
        auto builder = transaction.template repository<Link>().query_builder("l");
        builder.select({ { "node", db::text(query, Link::column<"edge_node_id">().expression(query, "l")) }, { "capability", Node::column<"capability">().expression(query, "n") }, { "model", Node::column<"model">().expression(query, "n") } })
            .template join<Node>(ruvia::db_join_type::inner, "n", query.binary(Node::column<"id">().expression(query, "n"), Op::equal, Link::column<"edge_node_id">().expression(query, "l")))
            .where(db::andAll(query, query.binary(Link::column<"id">().expression(query, "l"), Op::equal, db::uuid(query, linkId)), query.binary(Link::column<"execution">().expression(query, "l"), Op::equal, query.value("edge")), query.binary(Node::column<"enrollment_status">().expression(query, "n"), Op::equal, query.value("approved"))))
            .set_lock(ruvia::db_lock_options{});
        const auto nodes = co_await builder.get_raw_many();
        if (nodes.empty()) {
            service::common::fail(18002, "外部 IO 必须与设备采集链路属于同一边缘节点", 400);
        }
        const std::string nodeId(nodes.front()[0].value().value_or(""));
        const auto capability = ruvia::json_value::parse(nodes.front()[1].value().value_or("{}"));
        const auto ioCapability = capability.template get<ruvia::json_value>("externalIo");
        if (!ioCapability || !static_cast<bool>(ioCapability->template get<ruvia::bool_value>("supported").value_or(ruvia::bool_value{ false }))) {
            service::common::fail(18002, "该边缘节点固件未声明外部 IO 能力，请先升级固件", 400);
        }
        ruvia::db_query modelsQuery(c.pool());
        auto modelsBuilder = transaction.template repository<Model>().query_builder();
        const std::string ioModelId(input.template get<"protocolConfigId">().view());
        modelsBuilder.select({ { "id", db::text(modelsQuery, Model::column<"id">().expression(modelsQuery)) }, { "protocol", Model::column<"protocol">().expression(modelsQuery) }, { "config", Model::column<"config">().expression(modelsQuery) } })
            .where(db::andAll(modelsQuery, modelsQuery.binary((Model::column<"id">() == primaryModelId).expression(modelsQuery), Op::or_value, (Model::column<"id">() == std::string_view(ioModelId)).expression(modelsQuery)), modelsQuery.unary(ruvia::db_unary_operator::is_null, Model::column<"deleted_at">().expression(modelsQuery)), (Model::column<"enabled">() == true).expression(modelsQuery)))
            .set_lock(ruvia::db_lock_options{});
        const auto models = co_await modelsBuilder.get_raw_many();
        std::string primaryConfig = primaryModelId == ioModelId ? "{}" : "", ioConfig;
        for (const auto& row : models) {
            if (row[0].value().value_or("") == ioModelId && row[1].value().value_or("") == "ExternalIO") {
                ioConfig = row[2].value().value_or("{}");
            } else if (row[0].value().value_or("") == primaryModelId) {
                primaryConfig = row[2].value().value_or("{}");
            }
        }
        if (primaryConfig.empty() || ioConfig.empty()) {
            service::common::fail(18002, "采集协议或外部 IO 配置不存在或未启用", 400);
        }
        if (primaryModelId == ioModelId) {
            const auto protocols = capability.template get<ruvia::array<ruvia::string>>("protocols");
            if (!protocols || std::ranges::none_of(*protocols, [](const auto& protocol) {
                    return protocol.view() == "ExternalIO";
                })) {
                service::common::fail(18002, "节点固件不支持独立外部 IO，请先升级固件", 400);
            }
        }
        const auto io = ruvia::from_json<service::protocol::ExternalIoConfig>(ioConfig);
        service::protocol::ProtocolService::validateExternalIoCapability(io, nodes.front()[2].value().value_or(""), capability);
        co_await service::protocol::ProtocolService::requireExternalIoOutputs(transaction, nodeId, id, ioModelId, io);
        service::protocol::DerivedPointsConfig derived(ruvia::model_options{ .resource_ = c.arena() });
        if (input.template get<"derivedPoints">()) {
            derived.template set<"derivedPoints">(*input.template get<"derivedPoints">());
        }
        const auto derivedJson = ruvia::to_json(derived);
        const std::array sources{ ruvia::json_value::parse(primaryConfig), ruvia::json_value::parse(ioConfig) };
        (void)service::protocol::ProtocolService::validateDerivedPoints(ruvia::json_value::parse(std::string_view(derivedJson.data(), derivedJson.size())), sources);
        Source source;
        source.template set<"device_id">(id).template set<"link_id">(linkId).template set<"node_id">(nodeId).template set<"protocol_config_id">(ioModelId).template set<"derived_config">(std::string_view(derivedJson.data(), derivedJson.size()));
        (void)co_await transaction.template repository<Source>().insert(source);
    }

    // 扁平字段（必填/长度/枚举/范围/UUID/timezone）由声明式校验器保证；
    // 此处只做跨字段、依赖 DB 与协议相关的校验（保留 18002/18003 域码）。
    template <typename Context, typename Body>
    ruvia::task<void> validate(Context& c, ruvia::db_transaction& transaction, std::string_view resolvedLinkId, const Body& body, bool required, std::string_view currentConfigId = {}) {
        if (const auto& parameters = body.template get<"parameters">(); parameters) {
            if (parameters->view().size() > 262144) {
                service::common::fail(18002, "固有属性最多 256 KiB", 400);
            }
            std::set<std::string> names;
            const bool valid = parameters->for_each_field([&](std::string_view name, const ruvia::json_value&) {
                return !name.empty() && name.size() <= 100 && name.front() != ' ' && name.back() != ' ' &&
                    name.find_first_of("\r\n\t") == std::string_view::npos && names.emplace(name).second && names.size() <= 256;
            });
            if (!valid) {
                service::common::fail(18002, "固有属性名须为 1–100 字节且不能重复，最多 256 项", 400);
            }
        }
        service::device::validatePacket(body.template get<"heartbeat">());
        service::device::validatePacket(body.template get<"registration">());
        const auto linkId = std::string(resolvedLinkId);
        const auto requestedConfigId = str(body.template get<"protocolConfigId">());
        const auto configId = requestedConfigId.empty() ? std::string(currentConfigId) : requestedConfigId;
        if (required && linkId.empty()) {
            service::common::fail(18003, "请选择通道", 400);
        }
        if (required && configId.empty()) {
            service::common::fail(18003, "请选择设备类型", 400);
        }

        const ruvia::string* code = [&]() -> const ruvia::string* {
            if constexpr (std::is_same_v<Body, CreateDeviceBody>) {
                return &body.template get<"deviceCode">();
            } else {
                const auto& value = body.template get<"deviceCode">();
                return value ? &*value : nullptr;
            }
        }();
        if (code) {
            if (code->view().empty() || code->view().size() > 100) {
                service::common::fail(18002, "设备编码长度必须在 1 - 100 之间", 400);
            }
            for (const auto character : code->view()) {
                if (!std::isalnum(static_cast<unsigned char>(character)) && character != '-' && character != '_') {
                    service::common::fail(18002, "设备编码只能包含字母、数字、连字符和下划线", 400);
                }
            }
        }
        if (body.template get<"groupId">() && !body.template get<"groupId">()->view().empty()) {
            ruvia::db_query groupQuery(c.pool());
            auto groupQueryBuilder = transaction.template repository<service::device::entities::DeviceGroupEntity>().query_builder();
            groupQueryBuilder.select({ { "value0", db::integer(groupQuery, 1) } }).where(db::andAll(groupQuery, groupQuery.binary(groupQuery.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(groupQuery, body.template get<"groupId">()->view())), groupQuery.unary(ruvia::db_unary_operator::is_null, groupQuery.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">()))));
            const auto group = co_await groupQueryBuilder.get_raw_many();
            if (group.empty()) {
                service::common::fail(18003, "设备分组不存在", 400);
            }
        }
        if (configId.empty() || linkId.empty()) {
            co_return;
        }

        ruvia::db_query relationQuery(c.pool());
        auto relationQueryBuilder = transaction.template repository<service::link::entities::LinkEntity>().query_builder("l");
        relationQueryBuilder.select({ { "protocol", relationQuery.column(service::link::entities::LinkEntity::column_name<"protocol">(), "l") }, { "config", relationQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"config">(), "p") } }).template join<service::protocol::entities::ProtocolConfigEntity>(ruvia::db_join_type::inner, "p", relationQuery.binary(relationQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"protocol">(), "p"), ruvia::db_binary_operator::equal, relationQuery.column(service::link::entities::LinkEntity::column_name<"protocol">(), "l"))).where(db::andAll(relationQuery, relationQuery.binary(relationQuery.column(service::link::entities::LinkEntity::column_name<"id">(), "l"), ruvia::db_binary_operator::equal, db::uuid(relationQuery, linkId)), relationQuery.binary(relationQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"id">(), "p"), ruvia::db_binary_operator::equal, db::uuid(relationQuery, configId)), relationQuery.unary(ruvia::db_unary_operator::is_null, relationQuery.column(service::link::entities::LinkEntity::column_name<"deleted_at">(), "l")), relationQuery.unary(ruvia::db_unary_operator::is_null, relationQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"deleted_at">(), "p"))));
        const auto relation = co_await relationQueryBuilder.get_raw_many();
        if (relation.empty()) {
            service::common::fail(18003, "通道或设备类型不存在，或协议不一致", 400);
        }
        const std::string configProtocol(relation.front()[0].value().value_or(""));
        if (const auto& values = body.template get<"parameters">(); values && configProtocol != "MQTT") {
            const auto definition = ruvia::from_json<service::common::DeviceParameterConfiguration>(relation.front()[1].value().value_or("{}"));

            if (const auto& fields = definition.template get<"parameters">(); fields) {
                for (const auto& field : *fields) {
                    const auto name = field.template get<"name">().view();
                    const auto value = values->template get<ruvia::json_value>(name);
                    if (value && !service::common::deviceParameterValueMatches(field.template get<"dataType">().view(), *value)) {
                        service::common::fail(18002, "固有属性类型不匹配: " + std::string(name), 400);
                    }
                }
            }
        }
        if (configProtocol == "MQTT" && (required || body.template get<"parameters">())) {
            try {
                if (!body.template get<"parameters">()) {
                    throw std::invalid_argument("请填写设备标识固有属性");
                }
                const auto values = ruvia::json_value::parse(body.template get<"parameters">()->view());
                const auto config = ruvia::json_value::parse(relation.front()[1].value().value_or("{}"));
                service::protocol::ProtocolService::validateMqttDeviceValues(config, values);
            } catch (const std::invalid_argument& error) {
                service::common::fail(18002, error.what(), 400);
            }
        }
        if (configProtocol == "SL651" &&
            (packetEnabled(body.template get<"heartbeat">()) || packetEnabled(body.template get<"registration">()))) {
            service::common::fail(18002, "SL651 设备不支持配置注册包或心跳包", 400);
        }
        if (configProtocol == "DLT645" && code) {
            if (code->view().size() != 12 || !std::all_of(code->view().begin(), code->view().end(), [](unsigned char c) {
                    return std::isdigit(c) != 0;
                })) {
                service::common::fail(18002, "DL/T645 表地址必须是 12 位数字，不足时左侧补零", 400);
            }
        }
        if (configProtocol == "SL651" && code) {
            if (code->view().size() != 10) {
                service::common::fail(18002, "SL651 遥测站地址必须是 10 位数字，不足时左侧补零", 400);
            }
            for (const auto character : code->view()) {
                if (!std::isdigit(static_cast<unsigned char>(character))) {
                    service::common::fail(18002, "SL651 设备编码必须是数字遥测站地址", 400);
                }
            }
        }
    }

    static bool packetEnabled(const std::optional<DevicePacketBody>& packet) {
        return packet && packet->template get<"mode">() && packet->template get<"mode">()->view() != "OFF";
    }

    template <typename Context, typename Body>
    ruvia::task<void> validateRuntimeIdentity(Context& c, ruvia::db_transaction& transaction, std::string_view resolvedLinkId, const Body& body, std::optional<std::string> excludedId) {
        const std::string excluded = excludedId.value_or(std::string(kNilUuid));
        const std::string inLinkId = std::string(resolvedLinkId);
        const std::string inTargetId = str(body.template get<"targetId">());
        const std::string inConfigId = str(body.template get<"protocolConfigId">());
        const std::string inSlaveId =
            body.template get<"slaveId">() ? std::to_string(static_cast<std::int64_t>(*body.template get<"slaveId">())) : "";
        const std::string inRegistration = packetJson(body.template get<"registration">());
        const std::string inHeartbeat = packetJson(body.template get<"heartbeat">());
        ruvia::db_query currentDevice(c.pool());
        currentDevice.select({ currentDevice.column(service::device::entities::DeviceEntity::column_name<"link_id">()), currentDevice.column(service::device::entities::DeviceEntity::column_name<"protocol_config_id">()), currentDevice.column(service::device::entities::DeviceEntity::column_name<"protocol_params">()) })
            .from(service::device::entities::DeviceEntity::table_name())
            .where(db::andAll(currentDevice, currentDevice.binary(currentDevice.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(currentDevice, excluded)), currentDevice.unary(ruvia::db_unary_operator::is_null, currentDevice.column(service::device::entities::DeviceEntity::column_name<"deleted_at">()))));
        ruvia::db_query candidateQuery(c.pool());
        const auto currentParams = candidateQuery.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "current");
        const auto candidateLink = candidateQuery.coalesce(
            { db::nullableUuid(candidateQuery, inLinkId),
              candidateQuery.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "current") }
        );
        const auto candidateTarget = candidateQuery.coalesce(
            { candidateQuery.null_if(db::text(candidateQuery, candidateQuery.value(inTargetId)), candidateQuery.value("")),
              db::jsonText(candidateQuery, currentParams, "target_id"),
              candidateQuery.value("") }
        );
        const auto candidateConfig = candidateQuery.coalesce(
            { db::nullableUuid(candidateQuery, inConfigId),
              candidateQuery.column(service::device::entities::DeviceEntity::column_name<"protocol_config_id">(), "current") }
        );
        const auto inputSlave = candidateQuery.null_if(
            db::text(candidateQuery, candidateQuery.value(inSlaveId)),
            candidateQuery.value("")
        );
        const auto storedSlave = db::jsonText(candidateQuery, currentParams, "slave_id");
        const auto parseSlave = [&](ruvia::db_expression value) {
            return candidateQuery.case_when(
                { { candidateQuery.binary(candidateQuery.coalesce({ value, candidateQuery.value("") }), ruvia::db_binary_operator::regex, candidateQuery.value("^-?[0-9]{1,18}$")),
                    candidateQuery.cast(value, ruvia::db_data_type::integer) } }
            );
        };
        const auto candidateSlave = candidateQuery.coalesce(
            { parseSlave(inputSlave), parseSlave(storedSlave), db::integer(candidateQuery, 1) }
        );
        const auto defaultPacket = candidateQuery.cast(
            candidateQuery.value(R"({"mode":"OFF"})"),
            ruvia::db_data_type::jsonb
        );
        const auto candidateRegistration = candidateQuery.coalesce(
            { candidateQuery.cast(candidateQuery.null_if(db::text(candidateQuery, candidateQuery.value(inRegistration)), candidateQuery.value("")), ruvia::db_data_type::jsonb),
              db::jsonValue(candidateQuery, currentParams, "registration"),
              defaultPacket }
        );
        const auto candidateHeartbeat = candidateQuery.coalesce(
            { candidateQuery.cast(candidateQuery.null_if(db::text(candidateQuery, candidateQuery.value(inHeartbeat)), candidateQuery.value("")), ruvia::db_data_type::jsonb),
              db::jsonValue(candidateQuery, currentParams, "heartbeat"),
              defaultPacket }
        );
        const auto registrationModeExpr = candidateQuery.call(
            "upper",
            { candidateQuery.coalesce({ db::jsonText(candidateQuery, candidateRegistration, "mode"), candidateQuery.value("OFF") }) }
        );
        const auto registrationContent = candidateQuery.coalesce(
            { db::jsonText(candidateQuery, candidateRegistration, "content"),
              candidateQuery.value("") }
        );
        const auto registrationKeyExpr = candidateQuery.case_when(
            { { candidateQuery.binary(registrationModeExpr, ruvia::db_binary_operator::equal, candidateQuery.value("OFF")),
                candidateQuery.value("OFF:") },
              { candidateQuery.binary(registrationModeExpr, ruvia::db_binary_operator::equal, candidateQuery.value("HEX")),
                candidateQuery.binary(
                    db::text(candidateQuery, "HEX:"),
                    ruvia::db_binary_operator::concat,
                    candidateQuery.call(
                        "upper",
                        { candidateQuery.call(
                            "regexp_replace",
                            { registrationContent, candidateQuery.value("\\s"), candidateQuery.value(""), candidateQuery.value("g") }
                        ) }
                    )
                ) } },
            candidateQuery.binary(db::text(candidateQuery, "ASCII:"), ruvia::db_binary_operator::concat, registrationContent)
        );
        const auto heartbeatModeExpr = candidateQuery.call(
            "upper",
            { candidateQuery.coalesce({ db::jsonText(candidateQuery, candidateHeartbeat, "mode"), candidateQuery.value("OFF") }) }
        );
        auto candidateBuilder = transaction.template repository<service::link::entities::LinkEntity>().query_builder("link");
        candidateBuilder.with("current_device", currentDevice);
        candidateBuilder
            .select({ { "link_id", candidateLink }, { "mode", db::jsonText(candidateQuery, candidateQuery.column(service::link::entities::LinkEntity::column_name<"endpoint">(), "link"), "mode") }, { "protocol", candidateQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"protocol">(), "protocol") }, { "target", candidateTarget }, { "slave", candidateSlave }, { "registration_mode", registrationModeExpr }, { "registration_key", registrationKeyExpr }, { "heartbeat_mode", heartbeatModeExpr } })
            .from_function(candidateQuery.call("generate_series", { db::integer(candidateQuery, 1), db::integer(candidateQuery, 1) }), "base")
            .join_cte(ruvia::db_join_type::left, "current_device", "current", db::boolean(candidateQuery, true))
            .template join<service::link::entities::LinkEntity>(ruvia::db_join_type::inner, "link", db::andAll(candidateQuery, candidateQuery.binary(candidateQuery.column(service::link::entities::LinkEntity::column_name<"id">(), "link"), ruvia::db_binary_operator::equal, candidateLink), candidateQuery.unary(ruvia::db_unary_operator::is_null, candidateQuery.column(service::link::entities::LinkEntity::column_name<"deleted_at">(), "link"))))
            .template join<service::protocol::entities::ProtocolConfigEntity>(ruvia::db_join_type::inner, "protocol", db::andAll(candidateQuery, candidateQuery.binary(candidateQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"id">(), "protocol"), ruvia::db_binary_operator::equal, candidateConfig), candidateQuery.unary(ruvia::db_unary_operator::is_null, candidateQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"deleted_at">(), "protocol"))))
            .limit(1);
        const auto candidate = co_await candidateBuilder.get_raw_many();
        if (candidate.empty()) {
            co_return;
        }

        const auto& current = candidate.front();
        const std::string linkId(current[0].value().value_or(std::string_view{}));
        const std::string linkMode(current[1].value().value_or(std::string_view{}));
        const std::string protocol(current[2].value().value_or(std::string_view{}));
        const std::string targetId(current[3].value().value_or(std::string_view{}));
        const auto slaveId = toInt(current[4].value().value_or(std::string_view{}));
        const std::string registrationMode(current[5].value().value_or(std::string_view{}));
        const std::string registrationKey(current[6].value().value_or(std::string_view{}));
        const std::string heartbeatMode(current[7].value().value_or(std::string_view{}));
        if (linkMode != "TCP Server" || protocol == "SL651") {
            if (registrationMode != "OFF" || heartbeatMode != "OFF") {
                service::common::fail(18002, protocol == "SL651" ? "SL651 设备不支持配置注册包或心跳包" : "仅 TCP Server 设备支持配置注册包或心跳包", 400);
            }
        }
        if (protocol != "Modbus" && protocol != "S7" && protocol != "MC" && protocol != "FINS") {
            co_return;
        }

        ruvia::db_query siblingsQuery(c.pool());
        const auto siblingParams = siblingsQuery.column(service::device::entities::DeviceEntity::column_name<"protocol_params">(), "device");
        const auto siblingSlaveText = db::jsonText(
            siblingsQuery,
            siblingParams,
            "slave_id"
        );
        const auto siblingSlave = siblingsQuery.coalesce(
            { siblingsQuery.case_when(
                  { { siblingsQuery.binary(siblingsQuery.coalesce({ siblingSlaveText, siblingsQuery.value("") }), ruvia::db_binary_operator::regex, siblingsQuery.value("^-?[0-9]{1,18}$")),
                      siblingsQuery.cast(siblingSlaveText, ruvia::db_data_type::integer) } }
              ),
              db::integer(siblingsQuery, 1) }
        );
        const auto siblingRegistration = db::jsonValue(
            siblingsQuery,
            siblingParams,
            "registration"
        );
        const auto siblingMode = siblingsQuery.call(
            "upper",
            { siblingsQuery.coalesce({ db::jsonText(siblingsQuery, siblingRegistration, "mode"), siblingsQuery.value("OFF") }) }
        );
        const auto siblingContent = siblingsQuery.coalesce(
            { db::jsonText(siblingsQuery, siblingRegistration, "content"),
              siblingsQuery.value("") }
        );
        const auto siblingKey = siblingsQuery.case_when(
            { { siblingsQuery.binary(siblingMode, ruvia::db_binary_operator::equal, siblingsQuery.value("OFF")),
                siblingsQuery.value("OFF:") },
              { siblingsQuery.binary(siblingMode, ruvia::db_binary_operator::equal, siblingsQuery.value("HEX")),
                siblingsQuery.binary(
                    db::text(siblingsQuery, "HEX:"),
                    ruvia::db_binary_operator::concat,
                    siblingsQuery.call(
                        "upper",
                        { siblingsQuery.call(
                            "regexp_replace",
                            { siblingContent, siblingsQuery.value("\\s"), siblingsQuery.value(""), siblingsQuery.value("g") }
                        ) }
                    )
                ) } },
            siblingsQuery.binary(db::text(siblingsQuery, "ASCII:"), ruvia::db_binary_operator::concat, siblingContent)
        );
        auto siblingsQueryBuilder = transaction.template repository<service::device::entities::DeviceEntity>().query_builder("device");
        siblingsQueryBuilder.select({ { "name", siblingsQuery.column(service::device::entities::DeviceEntity::column_name<"name">(), "device") }, { "value1", siblingSlave }, { "value2", siblingsQuery.coalesce({ db::jsonText(siblingsQuery, siblingParams, "target_id"), siblingsQuery.value("") }) }, { "value3", siblingMode }, { "value4", siblingKey } }).template join<service::protocol::entities::ProtocolConfigEntity>(ruvia::db_join_type::inner, "config", db::andAll(siblingsQuery, siblingsQuery.binary(siblingsQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"id">(), "config"), ruvia::db_binary_operator::equal, siblingsQuery.column(service::device::entities::DeviceEntity::column_name<"protocol_config_id">(), "device")), siblingsQuery.unary(ruvia::db_unary_operator::is_null, siblingsQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"deleted_at">(), "config")))).where(db::andAll(siblingsQuery, siblingsQuery.binary(siblingsQuery.column(service::device::entities::DeviceEntity::column_name<"link_id">(), "device"), ruvia::db_binary_operator::equal, db::uuid(siblingsQuery, linkId)), siblingsQuery.binary(siblingsQuery.column(service::device::entities::DeviceEntity::column_name<"id">(), "device"), ruvia::db_binary_operator::not_equal, db::uuid(siblingsQuery, excluded)), siblingsQuery.unary(ruvia::db_unary_operator::is_null, siblingsQuery.column(service::device::entities::DeviceEntity::column_name<"deleted_at">(), "device")), siblingsQuery.binary(siblingsQuery.column(service::protocol::entities::ProtocolConfigEntity::column_name<"protocol">(), "config"), ruvia::db_binary_operator::equal, siblingsQuery.value(protocol)))).order_by(siblingsQuery.column(service::device::entities::DeviceEntity::column_name<"id">(), "device"));
        const auto siblings = co_await siblingsQueryBuilder.get_raw_many();

        if (linkMode == "TCP Client") {
            if (targetId.empty()) {
                co_return;
            }
            for (const auto& sibling : siblings) {
                if (sibling[2].value().value_or(std::string_view{}) != targetId) {
                    continue;
                }
                const std::string name(sibling[0].value().value_or(std::string_view{}));
                if (protocol == "S7" || protocol == "MC" || protocol == "FINS") {
                    service::common::fail(
                        18006,
                        protocol + " 同一目标地址只能关联一个设备，冲突设备: " + name,
                        409
                    );
                }
                if (toInt(sibling[1].value().value_or(std::string_view{})) == slaveId) {
                    service::common::fail(
                        18006,
                        "Modbus 同一目标地址下 Slave ID 重复，冲突设备: " + name,
                        409
                    );
                }
            }
            co_return;
        }
        if (linkMode != "TCP Server") {
            co_return;
        }

        if (protocol == "Modbus" && !siblings.empty()) {
            if (registrationMode == "OFF") {
                service::common::fail(18006, "Modbus TCP Server 链路存在多个设备时必须配置注册包", 409);
            }
            for (const auto& sibling : siblings) {
                const std::string name(sibling[0].value().value_or(std::string_view{}));
                if (sibling[3].value().value_or(std::string_view{}) == "OFF") {
                    service::common::fail(
                        18006,
                        "Modbus TCP Server 链路存在未配置注册包的设备: " + name,
                        409
                    );
                }
                if (sibling[4].value().value_or(std::string_view{}) == registrationKey && toInt(sibling[1].value().value_or(std::string_view{})) == slaveId) {
                    service::common::fail(
                        18006,
                        "Modbus 同一链路和注册码下 Slave ID 重复，冲突设备: " + name,
                        409
                    );
                }
            }
            co_return;
        }

        if (protocol == "S7" || protocol == "MC" || protocol == "FINS") {
            for (const auto& sibling : siblings) {
                if (sibling[4].value().value_or(std::string_view{}) == registrationKey) {
                    service::common::fail(18006, protocol + " TCP Server 同一链路下注册码重复，冲突设备: " + std::string(sibling[0].value().value_or(std::string_view{})), 409);
                }
            }
        }
    }

    template <typename Context, typename Body>
    ruvia::task<void> ensureUnique(Context& c, ruvia::db_transaction& transaction, std::string_view resolvedLinkId, const Body& body, std::optional<std::string> excludedId) {
        const auto& name = body.template get<"name">();
        const auto& code = body.template get<"deviceCode">();
        if constexpr (std::is_same_v<Body, UpdateDeviceBody>) {
            if (!name && !code) {
                co_return;
            }
        }
        const std::string nameValue = str(name);
        const std::string codeValue = str(code);
        const std::string linkValue = std::string(resolvedLinkId);
        const std::string excluded = excludedId.value_or(std::string(kNilUuid));
        ruvia::db_query query(c.pool());
        ruvia::db_query currentLink(c.pool());
        currentLink.select(currentLink.column(service::device::entities::DeviceEntity::column_name<"link_id">()))
            .from(service::device::entities::DeviceEntity::table_name())
            .where(currentLink.binary(currentLink.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(currentLink, excluded)))
            .limit(1);
        const auto nameMatch = db::andAll(
            query,
            query.binary(db::text(query, nameValue), ruvia::db_binary_operator::not_equal, db::text(query, "")),
            query.binary(query.column(service::device::entities::DeviceEntity::column_name<"name">()), ruvia::db_binary_operator::equal, query.value(nameValue))
        );
        const auto codeMatch = db::andAll(
            query,
            query.binary(db::text(query, codeValue), ruvia::db_binary_operator::not_equal, db::text(query, "")),
            query.binary(
                db::jsonText(query, query.column(service::device::entities::DeviceEntity::column_name<"protocol_params">()), "device_code"),
                ruvia::db_binary_operator::equal,
                query.value(codeValue)
            ),
            query.binary(query.column(service::device::entities::DeviceEntity::column_name<"link_id">()), ruvia::db_binary_operator::equal, query.coalesce({ db::nullableUuid(query, linkValue), query.subquery(currentLink) }))
        );
        auto queryBuilder = transaction.template repository<service::device::entities::DeviceEntity>().query_builder();
        queryBuilder.select({ { "value0", db::integer(query, 1) } }).where(db::andAll(query, query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceEntity::column_name<"deleted_at">())), query.binary(query.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::not_equal, db::uuid(query, excluded)), query.binary(nameMatch, ruvia::db_binary_operator::or_value, codeMatch))).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (!rows.empty()) {
            service::common::fail(18004, "设备名称已存在或同一链路的设备编码重复", 409);
        }
    }

    // ----- 设备分组私有工具 -----

    template <typename Row>
    static void fillGroup(DeviceGroupItemDto& item, const Row& row, const DeviceActor& actor) {
        item.template set<"id">(row[0].value().value_or(std::string_view{}));
        item.template set<"name">(row[1].value().value_or(std::string_view{}));
        item.template set<"parentId">(row[2].value().value_or(std::string_view{}));
        item.template set<"status">(row[3].value().value_or(std::string_view{}));
        item.template set<"sortOrder">(toInt(row[4].value().value_or(std::string_view{})));
        item.template set<"remark">(row[5].value().value_or(std::string_view{}));
        item.template set<"deviceCount">(toInt(row[6].value().value_or(std::string_view{})));
        item.template set<"createdAt">(row[7].value().value_or(std::string_view{}));
        item.template set<"updatedAt">(row[8].value().value_or(std::string_view{}));
        item.template set<"canShare">(
            actor.canGroupShare &&
            (actor.superadmin ||
             row[9].value().value_or(std::string_view{}) == actor.userId)
        );
    }

    template <typename Context, typename Body>
    ruvia::task<void> validateParent(Context& c, const Body& body, std::optional<std::string> currentId) {
        const auto& parent = body.template get<"parentId">();
        if (!parent || parent->view().empty()) {
            co_return;
        }
        if (!service::common::isUuid(parent->view())) {
            service::common::fail(17002, "上级分组必须是 UUID", 400);
        }
        if (currentId && parent->view() == *currentId) {
            service::common::fail(17003, "上级分组不能是自身", 409);
        }
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceGroupEntity>().query_builder();
        queryBuilder.select({ { "value0", db::integer(query, 1) } }).where(db::andAll(query, query.binary(query.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(query, parent->view())), query.unary(ruvia::db_unary_operator::is_null, query.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">())))).limit(1);
        const auto exists = co_await queryBuilder.get_raw_many();
        if (exists.empty()) {
            service::common::fail(17003, "上级分组不存在", 400);
        }
    }

    template <typename Context>
    ruvia::task<void> requireGroupOwner(Context& c, std::string_view ownerId) {
        if (c.userId == ownerId) {
            co_return;
        }
        ruvia::db_query roles(c.pool());
        roles.select(db::integer(roles, 1))
            .from(service::user::entities::UserRoleEntity::table_name(), "ur")
            .join(ruvia::db_join_type::inner, service::role::entities::RoleEntity::table_name(), db::andAll(roles, roles.binary(roles.column(service::role::entities::RoleEntity::column_name<"id">(), "r"), ruvia::db_binary_operator::equal, roles.column(service::user::entities::UserRoleEntity::column_name<"role_id">(), "ur")), roles.binary(roles.column(service::role::entities::RoleEntity::column_name<"code">(), "r"), ruvia::db_binary_operator::equal, roles.value("superadmin")), roles.binary(roles.column(service::role::entities::RoleEntity::column_name<"status">(), "r"), ruvia::db_binary_operator::equal, roles.value("enabled")), roles.unary(ruvia::db_unary_operator::is_null, roles.column(service::role::entities::RoleEntity::column_name<"deleted_at">(), "r"))), "r")
            .where(roles.binary(roles.column(service::user::entities::UserRoleEntity::column_name<"user_id">(), "ur"), ruvia::db_binary_operator::equal, db::uuid(roles, c.userId)))
            .limit(1);
        ruvia::db_query superadmin(c.pool());
        auto superadminBuilder = c.db().template repository<service::user::entities::UserRoleEntity>().query_builder();
        superadminBuilder.from_nothing().select({ { "exists", superadmin.exists(roles) } });
        const auto rows = co_await superadminBuilder.get_raw_many();
        if (rows.front()[0].value().value_or(std::string_view{}) != "t") {
            service::common::fail(17005, "只能管理自己创建的设备分组", 403);
        }
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

    static ruvia::db_expression uuidList(ruvia::db_query& query, const std::vector<std::string>& ids) {
        std::vector<ruvia::db_expression> values;
        values.reserve(ids.size());
        for (const auto& id : ids) {
            values.push_back(db::uuid(query, id));
        }
        return query.list(values);
    }

    static void restrictIds(auto& target, ruvia::db_query& query, ruvia::db_expression column, const std::vector<std::string>& ids) {
        target.and_where(query.binary(column, ruvia::db_binary_operator::in, uuidList(query, ids)));
    }

    static constexpr std::string_view kNilUuid = "00000000-0000-0000-0000-000000000000";
};

class DeviceShareService {
  public:
    static DeviceShareService& instance() {
        static thread_local DeviceShareService service;
        return service;
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<DeviceShareItemDto>> list(Context& c, std::string_view deviceId) {
        (void)co_await DeviceAccessService::instance().require(c, deviceId, DeviceAccessLevel::owner, c.userId);
        ruvia::db_query currentDevice(c.pool());
        currentDevice.select(currentDevice.column(service::device::entities::DeviceEntity::column_name<"group_id">()))
            .from(service::device::entities::DeviceEntity::table_name())
            .where(currentDevice.binary(currentDevice.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(currentDevice, deviceId)));
        ruvia::db_query ancestor(c.pool());
        ruvia::db_query ancestorRecursive(c.pool());
        ancestor.select({ ancestor.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "device_group"), ancestor.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), "device_group"), ancestor.column(service::device::entities::DeviceGroupEntity::column_name<"name">(), "device_group") })
            .from(service::device::entities::DeviceGroupEntity::table_name(), "device_group")
            .join(ruvia::db_join_type::inner, "current_device", ancestor.binary(ancestor.column(service::device::entities::DeviceEntity::column_name<"group_id">(), "current_device"), ruvia::db_binary_operator::equal, ancestor.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "device_group")), "current_device")
            .where(ancestor.unary(ruvia::db_unary_operator::is_null, ancestor.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), "device_group")));
        ancestorRecursive
            .select({ ancestorRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "parent"), ancestorRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), "parent"), ancestorRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"name">(), "parent") })
            .from(service::device::entities::DeviceGroupEntity::table_name(), "parent")
            .join(ruvia::db_join_type::inner, "ancestor_group", ancestorRecursive.binary(ancestorRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"parent_id">(), "child"), ruvia::db_binary_operator::equal, ancestorRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "parent")), "child")
            .where(ancestorRecursive.unary(ruvia::db_unary_operator::is_null, ancestorRecursive.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">(), "parent")));
        ancestor.combine(ruvia::db_set_operation::union_all, ancestorRecursive);

        ruvia::db_query direct(c.pool());
        const auto directUser = direct.unary(ruvia::db_unary_operator::is_not_null, direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"user_id">(), "access_grant"));
        const auto directSubjectType = direct.case_when(
            { { directUser, db::text(direct, direct.value("user")) } },
            db::text(direct, direct.value("department"))
        );
        const auto directName = direct.case_when(
            { { directUser,
                direct.coalesce({ direct.null_if(direct.column(service::user::entities::UserEntity::column_name<"nickname">(), "target_user"), direct.value("")), direct.column(service::user::entities::UserEntity::column_name<"username">(), "target_user"), direct.value("已删除用户") }) } },
            direct.coalesce({ direct.column(service::dept::entities::DeptEntity::column_name<"name">(), "target_department"), direct.value("已删除部门") })
        );
        direct
            .select({ db::text(direct, direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"id">(), "access_grant")), directSubjectType, direct.coalesce({ db::text(direct, direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"user_id">(), "access_grant")), db::text(direct, direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"department_id">(), "access_grant")) }), directName, direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"access_level">(), "access_grant"), db::text(direct, direct.value("device")), db::text(direct, direct.value("")), db::text(direct, direct.value("")), db::boolean(direct, false), direct.call("iot_utc_timestamp", { direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"created_at">(), "access_grant") }), direct.call("iot_utc_timestamp", { direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"updated_at">(), "access_grant") }) })
            .from(service::device::entities::DeviceAccessGrantEntity::table_name(), "access_grant")
            .join(ruvia::db_join_type::left, service::user::entities::UserEntity::table_name(), direct.binary(direct.column(service::user::entities::UserEntity::column_name<"id">(), "target_user"), ruvia::db_binary_operator::equal, direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"user_id">(), "access_grant")), "target_user")
            .join(ruvia::db_join_type::left, service::dept::entities::DeptEntity::table_name(), direct.binary(direct.column(service::dept::entities::DeptEntity::column_name<"id">(), "target_department"), ruvia::db_binary_operator::equal, direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"department_id">(), "access_grant")), "target_department")
            .where(direct.binary(direct.column(service::device::entities::DeviceAccessGrantEntity::column_name<"device_id">(), "access_grant"), ruvia::db_binary_operator::equal, db::uuid(direct, deviceId)));

        ruvia::db_query inherited(c.pool());
        const auto inheritedUser = inherited.unary(
            ruvia::db_unary_operator::is_not_null,
            inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), "group_access")
        );
        const auto inheritedSubjectType = inherited.case_when(
            { { inheritedUser, db::text(inherited, inherited.value("user")) } },
            db::text(inherited, inherited.value("department"))
        );
        const auto inheritedName = inherited.case_when(
            { { inheritedUser,
                inherited.coalesce({ inherited.null_if(inherited.column(service::user::entities::UserEntity::column_name<"nickname">(), "inherited_user"), inherited.value("")), inherited.column(service::user::entities::UserEntity::column_name<"username">(), "inherited_user"), inherited.value("已删除用户") }) } },
            inherited.coalesce({ inherited.column(service::dept::entities::DeptEntity::column_name<"name">(), "inherited_department"), inherited.value("已删除部门") })
        );
        inherited
            .select({ db::text(inherited, inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"id">(), "group_access")), inheritedSubjectType, inherited.coalesce({ db::text(inherited, inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), "group_access")), db::text(inherited, inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"department_id">(), "group_access")) }), inheritedName, inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"access_level">(), "group_access"), db::text(inherited, inherited.value("group")), db::text(inherited, inherited.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "ancestor")), inherited.column(service::device::entities::DeviceGroupEntity::column_name<"name">(), "ancestor"), db::boolean(inherited, true), inherited.call("iot_utc_timestamp", { inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"created_at">(), "group_access") }), inherited.call("iot_utc_timestamp", { inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"updated_at">(), "group_access") }) })
            .from(service::device::entities::DeviceGroupAccessGrantEntity::table_name(), "group_access")
            .join(ruvia::db_join_type::inner, "ancestor_group", inherited.binary(inherited.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "ancestor"), ruvia::db_binary_operator::equal, inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), "group_access")), "ancestor")
            .join(ruvia::db_join_type::left, service::user::entities::UserEntity::table_name(), inherited.binary(inherited.column(service::user::entities::UserEntity::column_name<"id">(), "inherited_user"), ruvia::db_binary_operator::equal, inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), "group_access")), "inherited_user")
            .join(ruvia::db_join_type::left, service::dept::entities::DeptEntity::table_name(), inherited.binary(inherited.column(service::dept::entities::DeptEntity::column_name<"id">(), "inherited_department"), ruvia::db_binary_operator::equal, inherited.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"department_id">(), "group_access")), "inherited_department");
        direct.combine(ruvia::db_set_operation::union_all, inherited);
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceAccessGrantEntity>().query_builder("device_share");

        const std::array<ruvia::borrowed_text, 3> sourceColumns8{ { "id", "parent_id", "name" } };
        const std::array<ruvia::borrowed_text, 11> sourceColumns9{ { "id", "subject_type", "subject_id", "subject_name", "access_level", "source_type", "source_group_id", "source_group_name", "inherited", "created_at", "updated_at" } };
        queryBuilder.with("current_device", currentDevice).with("ancestor_group", ancestor, { .recursive_ = true, .columns_ = sourceColumns8 }).with("device_share", direct, { .columns_ = sourceColumns9 });
        queryBuilder.select({ { "id", query.column("id", "device_share") }, { "subject_type", query.column("subject_type", "device_share") }, { "subject_id", query.column("subject_id", "device_share") }, { "subject_name", query.column("subject_name", "device_share") }, { "access_level", query.column("access_level", "device_share") }, { "source_type", query.column("source_type", "device_share") }, { "source_group_id", query.column("source_group_id", "device_share") }, { "source_group_name", query.column("source_group_name", "device_share") }, { "inherited", query.column("inherited", "device_share") }, { "created_at", query.column("created_at", "device_share") }, { "updated_at", query.column("updated_at", "device_share") } }).from_cte("device_share").order_by(query.column(kSubjectTypeColumn, "device_share")).add_order_by(query.column(kSubjectNameColumn, "device_share")).add_order_by(query.column(kInheritedColumn, "device_share")).add_order_by(query.column(service::device::entities::DeviceAccessGrantEntity::column_name<"id">(), "device_share"));
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<DeviceShareItemDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& item = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"id">(row[0].value().value_or(std::string_view{}))
                .template set<"subjectType">(row[1].value().value_or(std::string_view{}))
                .template set<"subjectId">(row[2].value().value_or(std::string_view{}))
                .template set<"subjectName">(row[3].value().value_or(std::string_view{}))
                .template set<"accessLevel">(row[4].value().value_or(std::string_view{}))
                .template set<"sourceType">(row[5].value().value_or(std::string_view{}))
                .template set<"sourceGroupId">(row[6].value().value_or(std::string_view{}))
                .template set<"sourceGroupName">(row[7].value().value_or(std::string_view{}))
                .template set<"inherited">(row[8].value().value_or(std::string_view{}) == "t")
                .template set<"createdAt">(row[9].value().value_or(std::string_view{}))
                .template set<"updatedAt">(row[10].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<DeviceShareTargetDto>> targets(Context& c, std::string_view deviceId) {
        (void)co_await DeviceAccessService::instance().require(c, deviceId, DeviceAccessLevel::owner, c.userId);
        ruvia::db_query owner(c.pool());
        owner.select(owner.column(service::device::entities::DeviceEntity::column_name<"created_by">()))
            .from(service::device::entities::DeviceEntity::table_name())
            .where(owner.binary(owner.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(owner, deviceId)))
            .limit(1);
        ruvia::db_query users(c.pool());
        users.select({ db::text(users, users.value("user")), db::text(users, users.column(service::user::entities::UserEntity::column_name<"id">(), "target")), users.coalesce({ users.null_if(users.column(service::user::entities::UserEntity::column_name<"nickname">(), "target"), users.value("")), users.column(service::user::entities::UserEntity::column_name<"username">(), "target") }) })
            .from(service::user::entities::UserEntity::table_name(), "target")
            .where(db::andAll(users, users.binary(users.column(service::user::entities::UserEntity::column_name<"status">(), "target"), ruvia::db_binary_operator::equal, users.value("enabled")), users.unary(ruvia::db_unary_operator::is_null, users.column(service::user::entities::UserEntity::column_name<"deleted_at">(), "target")), users.binary(users.column(service::user::entities::UserEntity::column_name<"id">(), "target"), ruvia::db_binary_operator::not_equal, users.subquery(owner))));
        ruvia::db_query departments(c.pool());
        departments.select({ db::text(departments, departments.value("department")), db::text(departments, departments.column(service::dept::entities::DeptEntity::column_name<"id">(), "department")), departments.column(service::dept::entities::DeptEntity::column_name<"name">(), "department") })
            .from(service::dept::entities::DeptEntity::table_name(), "department")
            .where(db::andAll(departments, departments.binary(departments.column(service::dept::entities::DeptEntity::column_name<"status">(), "department"), ruvia::db_binary_operator::equal, departments.value("enabled")), departments.unary(ruvia::db_unary_operator::is_null, departments.column(service::dept::entities::DeptEntity::column_name<"deleted_at">(), "department"))));
        users.combine(ruvia::db_set_operation::union_all, departments);
        ruvia::db_query query(c.pool());

        const std::array<ruvia::borrowed_text, 3> sourceColumns10{ { "subject_type", "subject_id", "subject_name" } };
        auto queryBuilder = c.db().template repository<service::user::entities::UserEntity>().query_builder("share_target");
        queryBuilder.with("share_target", users, { .columns_ = sourceColumns10 })
            .select({ { "subject_type", query.column("subject_type", "share_target") }, { "subject_id", query.column("subject_id", "share_target") }, { "subject_name", query.column("subject_name", "share_target") } })
            .from_cte("share_target")
            .order_by(query.column(kSubjectTypeColumn, "share_target"))
            .add_order_by(query.column(kSubjectNameColumn, "share_target"))
            .add_order_by(query.column(kSubjectIdColumn, "share_target"));
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<DeviceShareTargetDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& item = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"subjectType">(row[0].value().value_or(std::string_view{})).template set<"subjectId">(row[1].value().value_or(std::string_view{})).template set<"subjectName">(row[2].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> replace(Context& c, std::string_view deviceId, const ReplaceDeviceSharesBody& body) {
        auto decision =
            co_await DeviceAccessService::instance().require(c, deviceId, DeviceAccessLevel::owner, c.userId);
        auto shares = normalize(body);

        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        ruvia::db_query deviceQuery(c.pool());
        auto deviceQueryBuilder = transaction.template repository<service::device::entities::DeviceEntity>().query_builder();
        deviceQueryBuilder.select({ { "created_by", db::text(deviceQuery, deviceQuery.column(service::device::entities::DeviceEntity::column_name<"created_by">())) } }).where(db::andAll(deviceQuery, deviceQuery.binary(deviceQuery.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(deviceQuery, deviceId)), deviceQuery.unary(ruvia::db_unary_operator::is_null, deviceQuery.column(service::device::entities::DeviceEntity::column_name<"deleted_at">())))).set_lock({ .mode_ = ruvia::db_row_lock::update });
        const auto deviceRows = co_await deviceQueryBuilder.get_raw_many();
        if (deviceRows.empty()) {
            service::common::fail(18001, "设备不存在", 404);
        }
        const std::string ownerId(deviceRows.front()[0].value().value_or(std::string_view{}));

        co_await validateTargets(transaction, shares, ownerId);

        ruvia::db_query remove(c.pool());
        auto removeBuilder = transaction.template repository<service::device::entities::DeviceAccessGrantEntity>().delete_builder();
        removeBuilder.where(remove.binary(remove.column(service::device::entities::DeviceAccessGrantEntity::column_name<"device_id">()), ruvia::db_binary_operator::equal, db::uuid(remove, deviceId)));
        (void)co_await removeBuilder.execute();
        for (const auto& share : shares) {
            const std::string userId = share.subjectType == "user" ? share.subjectId : "";
            const std::string departmentId =
                share.subjectType == "department" ? share.subjectId : "";
            const auto grantId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
            ruvia::db_query insert(c.pool());
            const std::array<std::string_view, 6> insertColumns{ service::device::entities::DeviceAccessGrantEntity::column_name<"id">(), service::device::entities::DeviceAccessGrantEntity::column_name<"device_id">(), service::device::entities::DeviceAccessGrantEntity::column_name<"user_id">(), service::device::entities::DeviceAccessGrantEntity::column_name<"department_id">(), service::device::entities::DeviceAccessGrantEntity::column_name<"access_level">(), service::device::entities::DeviceAccessGrantEntity::column_name<"granted_by">() };
            auto insertBuilder = transaction.template repository<service::device::entities::DeviceAccessGrantEntity>().insert_builder();
            ruvia::db_values_builder insertValues(insertColumns);
            insertValues.row({ db::uuid(insert, grantId), db::uuid(insert, deviceId), db::nullableUuid(insert, userId), db::nullableUuid(insert, departmentId), insert.value(share.accessLevel), db::uuid(insert, decision.actor.userId) });
            insertBuilder.insert_from(insertColumns, insertValues.query());
            (void)co_await insertBuilder.execute();
        }
        const auto auditId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        const auto shareCount = static_cast<std::int64_t>(shares.size());
        ruvia::db_query audit(c.pool());
        const std::array<std::string_view, 7> auditColumns{ service::device::entities::SecurityAuditLogEntity::column_name<"id">(), service::device::entities::SecurityAuditLogEntity::column_name<"actor_user_id">(), service::device::entities::SecurityAuditLogEntity::column_name<"action">(), service::device::entities::SecurityAuditLogEntity::column_name<"resource_type">(), service::device::entities::SecurityAuditLogEntity::column_name<"resource_id">(), service::device::entities::SecurityAuditLogEntity::column_name<"outcome">(), service::device::entities::SecurityAuditLogEntity::column_name<"details">() };
        auto auditBuilder = transaction.template repository<service::device::entities::SecurityAuditLogEntity>().insert_builder();
        ruvia::db_values_builder auditValues(auditColumns);
        auditValues.row({ db::uuid(audit, auditId), db::uuid(audit, decision.actor.userId), audit.value("device.share.replace"), audit.value("device"), db::uuid(audit, deviceId), audit.value("success"), audit.call("jsonb_build_object", { db::text(audit, "share_count"), audit.cast(audit.value(shareCount), ruvia::db_data_type::integer) }) });
        auditBuilder.insert_from(auditColumns, auditValues.query());
        (void)co_await auditBuilder.execute();
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<DeviceShareItemDto>> listGroup(Context& c, std::string_view groupId) {
        (void)co_await DeviceAccessService::instance().requireGroupOwner(c, groupId, c.userId);
        ruvia::db_query query(c.pool());
        const auto isUser = query.unary(ruvia::db_unary_operator::is_not_null, query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), "access_grant"));
        const auto subjectType = query.case_when(
            { { isUser, db::text(query, query.value("user")) } },
            db::text(query, query.value("department"))
        );
        const auto subjectName = query.case_when(
            { { isUser,
                query.coalesce({ query.null_if(query.column(service::user::entities::UserEntity::column_name<"nickname">(), "target_user"), query.value("")), query.column(service::user::entities::UserEntity::column_name<"username">(), "target_user"), query.value("已删除用户") }) } },
            query.coalesce({ query.column(service::dept::entities::DeptEntity::column_name<"name">(), "target_department"), query.value("已删除部门") })
        );
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceGroupAccessGrantEntity>().query_builder("access_grant");
        queryBuilder.select({ { "id", db::text(query, query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"id">(), "access_grant")) }, { "value1", subjectType }, { "user_id", query.coalesce({ db::text(query, query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), "access_grant")), db::text(query, query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"department_id">(), "access_grant")) }) }, { "value3", subjectName }, { "access_level", query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"access_level">(), "access_grant") }, { "value5", db::text(query, query.value("group")) }, { "value6", db::text(query, query.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "target_group")) }, { "name", query.column(service::device::entities::DeviceGroupEntity::column_name<"name">(), "target_group") }, { "value8", db::boolean(query, false) }, { "created_at", query.call("iot_utc_timestamp", { query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"created_at">(), "access_grant") }) }, { "updated_at", query.call("iot_utc_timestamp", { query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"updated_at">(), "access_grant") }) } }).template join<service::device::entities::DeviceGroupEntity>(ruvia::db_join_type::inner, "target_group", query.binary(query.column(service::device::entities::DeviceGroupEntity::column_name<"id">(), "target_group"), ruvia::db_binary_operator::equal, query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), "access_grant"))).template join<service::user::entities::UserEntity>(ruvia::db_join_type::left, "target_user", query.binary(query.column(service::user::entities::UserEntity::column_name<"id">(), "target_user"), ruvia::db_binary_operator::equal, query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), "access_grant"))).template join<service::dept::entities::DeptEntity>(ruvia::db_join_type::left, "target_department", query.binary(query.column(service::dept::entities::DeptEntity::column_name<"id">(), "target_department"), ruvia::db_binary_operator::equal, query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"department_id">(), "access_grant"))).where(query.binary(query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), "access_grant"), ruvia::db_binary_operator::equal, db::uuid(query, groupId))).order_by(subjectType).add_order_by(subjectName).add_order_by(query.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"id">(), "access_grant"));
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<DeviceShareItemDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& item = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"id">(row[0].value().value_or(std::string_view{}))
                .template set<"subjectType">(row[1].value().value_or(std::string_view{}))
                .template set<"subjectId">(row[2].value().value_or(std::string_view{}))
                .template set<"subjectName">(row[3].value().value_or(std::string_view{}))
                .template set<"accessLevel">(row[4].value().value_or(std::string_view{}))
                .template set<"sourceType">(row[5].value().value_or(std::string_view{}))
                .template set<"sourceGroupId">(row[6].value().value_or(std::string_view{}))
                .template set<"sourceGroupName">(row[7].value().value_or(std::string_view{}))
                .template set<"inherited">(false)
                .template set<"createdAt">(row[9].value().value_or(std::string_view{}))
                .template set<"updatedAt">(row[10].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<DeviceShareTargetDto>> groupTargets(Context& c, std::string_view groupId) {
        (void)co_await DeviceAccessService::instance().requireGroupOwner(c, groupId, c.userId);
        ruvia::db_query owner(c.pool());
        owner.select(owner.column(service::device::entities::DeviceGroupEntity::column_name<"created_by">()))
            .from(service::device::entities::DeviceGroupEntity::table_name())
            .where(owner.binary(owner.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(owner, groupId)))
            .limit(1);
        ruvia::db_query users(c.pool());
        users.select({ db::text(users, users.value("user")), db::text(users, users.column(service::user::entities::UserEntity::column_name<"id">(), "target")), users.coalesce({ users.null_if(users.column(service::user::entities::UserEntity::column_name<"nickname">(), "target"), users.value("")), users.column(service::user::entities::UserEntity::column_name<"username">(), "target") }) })
            .from(service::user::entities::UserEntity::table_name(), "target")
            .where(db::andAll(users, users.binary(users.column(service::user::entities::UserEntity::column_name<"status">(), "target"), ruvia::db_binary_operator::equal, users.value("enabled")), users.unary(ruvia::db_unary_operator::is_null, users.column(service::user::entities::UserEntity::column_name<"deleted_at">(), "target")), users.binary(users.column(service::user::entities::UserEntity::column_name<"id">(), "target"), ruvia::db_binary_operator::not_equal, users.subquery(owner))));
        ruvia::db_query departments(c.pool());
        departments.select({ db::text(departments, departments.value("department")), db::text(departments, departments.column(service::dept::entities::DeptEntity::column_name<"id">(), "department")), departments.column(service::dept::entities::DeptEntity::column_name<"name">(), "department") })
            .from(service::dept::entities::DeptEntity::table_name(), "department")
            .where(db::andAll(departments, departments.binary(departments.column(service::dept::entities::DeptEntity::column_name<"status">(), "department"), ruvia::db_binary_operator::equal, departments.value("enabled")), departments.unary(ruvia::db_unary_operator::is_null, departments.column(service::dept::entities::DeptEntity::column_name<"deleted_at">(), "department"))));
        users.combine(ruvia::db_set_operation::union_all, departments);
        ruvia::db_query query(c.pool());

        const std::array<ruvia::borrowed_text, 3> sourceColumns11{ { "subject_type", "subject_id", "subject_name" } };
        auto queryBuilder = c.db().template repository<service::user::entities::UserEntity>().query_builder("share_target");
        queryBuilder.with("share_target", users, { .columns_ = sourceColumns11 })
            .select({ { "subject_type", query.column("subject_type", "share_target") }, { "subject_id", query.column("subject_id", "share_target") }, { "subject_name", query.column("subject_name", "share_target") } })
            .from_cte("share_target")
            .order_by(query.column(kSubjectTypeColumn, "share_target"))
            .add_order_by(query.column(kSubjectNameColumn, "share_target"))
            .add_order_by(query.column(kSubjectIdColumn, "share_target"));
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<DeviceShareTargetDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& item = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"subjectType">(row[0].value().value_or(std::string_view{})).template set<"subjectId">(row[1].value().value_or(std::string_view{})).template set<"subjectName">(row[2].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> replaceGroup(Context& c, std::string_view groupId, const ReplaceDeviceSharesBody& body) {
        auto actor = co_await DeviceAccessService::instance().requireGroupOwner(c, groupId, c.userId);
        auto shares = normalize(body);
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        ruvia::db_query groupQuery(c.pool());
        auto groupQueryBuilder = transaction.template repository<service::device::entities::DeviceGroupEntity>().query_builder();
        groupQueryBuilder.select({ { "created_by", db::text(groupQuery, groupQuery.column(service::device::entities::DeviceGroupEntity::column_name<"created_by">())) } }).where(db::andAll(groupQuery, groupQuery.binary(groupQuery.column(service::device::entities::DeviceGroupEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(groupQuery, groupId)), groupQuery.unary(ruvia::db_unary_operator::is_null, groupQuery.column(service::device::entities::DeviceGroupEntity::column_name<"deleted_at">())))).set_lock({ .mode_ = ruvia::db_row_lock::update });
        const auto groupRows = co_await groupQueryBuilder.get_raw_many();
        if (groupRows.empty()) {
            service::common::fail(17001, "设备分组不存在", 404);
        }
        co_await validateTargets(transaction, shares, groupRows.front()[0].value().value_or(std::string_view{}));

        ruvia::db_query remove(c.pool());
        auto removeBuilder = transaction.template repository<service::device::entities::DeviceGroupAccessGrantEntity>().delete_builder();
        removeBuilder.where(remove.binary(remove.column(service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">()), ruvia::db_binary_operator::equal, db::uuid(remove, groupId)));
        (void)co_await removeBuilder.execute();
        for (const auto& share : shares) {
            const std::string userId = share.subjectType == "user" ? share.subjectId : "";
            const std::string departmentId =
                share.subjectType == "department" ? share.subjectId : "";
            const auto grantId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
            ruvia::db_query insert(c.pool());
            const std::array<std::string_view, 6> insertColumns{ service::device::entities::DeviceGroupAccessGrantEntity::column_name<"id">(), service::device::entities::DeviceGroupAccessGrantEntity::column_name<"group_id">(), service::device::entities::DeviceGroupAccessGrantEntity::column_name<"user_id">(), service::device::entities::DeviceGroupAccessGrantEntity::column_name<"department_id">(), service::device::entities::DeviceGroupAccessGrantEntity::column_name<"access_level">(), service::device::entities::DeviceGroupAccessGrantEntity::column_name<"granted_by">() };
            auto insertBuilder = transaction.template repository<service::device::entities::DeviceGroupAccessGrantEntity>().insert_builder();
            ruvia::db_values_builder insertValues(insertColumns);
            insertValues.row({ db::uuid(insert, grantId), db::uuid(insert, groupId), db::nullableUuid(insert, userId), db::nullableUuid(insert, departmentId), insert.value(share.accessLevel), db::uuid(insert, actor.userId) });
            insertBuilder.insert_from(insertColumns, insertValues.query());
            (void)co_await insertBuilder.execute();
        }
        const auto auditId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        const auto shareCount = static_cast<std::int64_t>(shares.size());
        ruvia::db_query audit(c.pool());
        const std::array<std::string_view, 7> auditColumns{ service::device::entities::SecurityAuditLogEntity::column_name<"id">(), service::device::entities::SecurityAuditLogEntity::column_name<"actor_user_id">(), service::device::entities::SecurityAuditLogEntity::column_name<"action">(), service::device::entities::SecurityAuditLogEntity::column_name<"resource_type">(), service::device::entities::SecurityAuditLogEntity::column_name<"resource_id">(), service::device::entities::SecurityAuditLogEntity::column_name<"outcome">(), service::device::entities::SecurityAuditLogEntity::column_name<"details">() };
        auto auditBuilder = transaction.template repository<service::device::entities::SecurityAuditLogEntity>().insert_builder();
        ruvia::db_values_builder auditValues(auditColumns);
        auditValues.row({ db::uuid(audit, auditId), db::uuid(audit, actor.userId), audit.value("device_group.share.replace"), audit.value("device_group"), db::uuid(audit, groupId), audit.value("success"), audit.call("jsonb_build_object", { db::text(audit, "share_count"), audit.cast(audit.value(shareCount), ruvia::db_data_type::integer) }) });
        auditBuilder.insert_from(auditColumns, auditValues.query());
        (void)co_await auditBuilder.execute();
        co_await transaction.commit();
    }

  private:
    struct NormalizedShare final {
        std::string subjectType;
        std::string subjectId;
        std::string accessLevel;
    };

    static std::vector<NormalizedShare> normalize(const ReplaceDeviceSharesBody& body) {
        std::vector<NormalizedShare> shares;
        shares.reserve(body.template get<"shares">().size());
        std::set<std::string, std::less<>> uniqueSubjects;
        for (const auto& item : body.template get<"shares">()) {
            if (!item.template get<"subjectType">() || !item.template get<"subjectId">() || !item.template get<"accessLevel">()) {
                service::common::fail(18010, "分享对象参数不完整", 400);
            }
            NormalizedShare share{ std::string(item.template get<"subjectType">()->view()),
                                   std::string(item.template get<"subjectId">()->view()),
                                   std::string(item.template get<"accessLevel">()->view()) };
            if (share.subjectType != "user" && share.subjectType != "department") {
                service::common::fail(18010, "分享对象类型无效", 400);
            }
            if (!service::common::isUuid(share.subjectId)) {
                service::common::fail(18010, "分享对象 ID 必须是 UUID", 400);
            }
            if (share.accessLevel != "view" && share.accessLevel != "operate") {
                service::common::fail(18010, "设备访问级别无效", 400);
            }
            if (!uniqueSubjects.emplace(share.subjectType + ":" + share.subjectId).second) {
                service::common::fail(18010, "分享对象不能重复", 400);
            }
            shares.emplace_back(std::move(share));
        }
        return shares;
    }

    static ruvia::task<void> validateTargets(ruvia::db_transaction& transaction, const std::vector<NormalizedShare>& shares, std::string_view ownerId) {
        for (const auto& share : shares) {
            if (share.subjectType == "user") {
                if (share.subjectId == ownerId) {
                    service::common::fail(18010, "不能向资源所有者重复授权", 400);
                }
                ruvia::db_query targetQuery;
                auto targetQueryBuilder = transaction.template repository<service::user::entities::UserEntity>().query_builder();
                targetQueryBuilder.select({ { "value0", db::integer(targetQuery, 1) } }).where(db::andAll(targetQuery, targetQuery.binary(targetQuery.column(service::user::entities::UserEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(targetQuery, share.subjectId)), targetQuery.binary(targetQuery.column(service::user::entities::UserEntity::column_name<"status">()), ruvia::db_binary_operator::equal, targetQuery.value("enabled")), targetQuery.unary(ruvia::db_unary_operator::is_null, targetQuery.column(service::user::entities::UserEntity::column_name<"deleted_at">())))).limit(1);
                const auto target = co_await targetQueryBuilder.get_raw_many();
                if (target.empty()) {
                    service::common::fail(18010, "包含不存在或已禁用的用户", 400);
                }
            } else {
                ruvia::db_query targetQuery;
                auto targetQueryBuilder = transaction.template repository<service::dept::entities::DeptEntity>().query_builder();
                targetQueryBuilder.select({ { "value0", db::integer(targetQuery, 1) } }).where(db::andAll(targetQuery, targetQuery.binary(targetQuery.column(service::dept::entities::DeptEntity::column_name<"id">()), ruvia::db_binary_operator::equal, db::uuid(targetQuery, share.subjectId)), targetQuery.binary(targetQuery.column(service::dept::entities::DeptEntity::column_name<"status">()), ruvia::db_binary_operator::equal, targetQuery.value("enabled")), targetQuery.unary(ruvia::db_unary_operator::is_null, targetQuery.column(service::dept::entities::DeptEntity::column_name<"deleted_at">())))).limit(1);
                const auto target = co_await targetQueryBuilder.get_raw_many();
                if (target.empty()) {
                    service::common::fail(18010, "包含不存在或已禁用的部门", 400);
                }
            }
        }
    }
};

} // namespace service::device
