#pragma once

#include "service/common/uuid.h"

#include <ruvia/web/validation.h>

namespace service::role {

inline constexpr auto kDefaultPage = RUVIA_DEFAULT(1){};
inline constexpr auto kDefaultPageSize = RUVIA_DEFAULT(10){};

inline constexpr std::string_view kSuperAdminRoleCode{"superadmin"};

RUVIA_MODEL(CreateRoleBody,
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(2, "角色名称长度需在 2 - 128 之间"), RUVIA_MAX(128, "角色名称长度需在 2 - 128 之间")),
    RUVIA_REQUIRED_FIELD(code, ruvia::string, RUVIA_MIN(2, "角色编码长度需在 2 - 64 之间"), RUVIA_MAX(64, "角色编码长度需在 2 - 64 之间")),
    RUVIA_OPTIONAL_FIELD(description, ruvia::string, RUVIA_MAX(500, "角色描述不能超过 500 个字符")),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("状态无效", "enabled", "disabled")),
    RUVIA_OPTIONAL_FIELD(permissions, ruvia::array<ruvia::string>));

RUVIA_MODEL(UpdateRoleBody,
    RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(2, "角色名称长度需在 2 - 128 之间"), RUVIA_MAX(128, "角色名称长度需在 2 - 128 之间")),
    RUVIA_OPTIONAL_FIELD(code, ruvia::string, RUVIA_MIN(2, "角色编码长度需在 2 - 64 之间"), RUVIA_MAX(64, "角色编码长度需在 2 - 64 之间")),
    RUVIA_OPTIONAL_FIELD(description, ruvia::string, RUVIA_MAX(500, "角色描述不能超过 500 个字符")),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("状态无效", "enabled", "disabled")),
    RUVIA_OPTIONAL_FIELD(permissions, ruvia::array<ruvia::string>));

RUVIA_MODEL(RoleListQuery,
    RUVIA_OPTIONAL_FIELD(page, ruvia::int64, decltype(kDefaultPage), RUVIA_MIN(1, "page 必须大于 0")),
    RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64, decltype(kDefaultPageSize), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")),
    RUVIA_OPTIONAL_FIELD(keyword, ruvia::string),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("状态无效", "enabled", "disabled")));

RUVIA_MODEL(RoleIdParams,
    RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(RoleOptionDto,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(code, ruvia::string));

RUVIA_MODEL(RoleItemDto,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(code, ruvia::string),
    RUVIA_OPTIONAL_FIELD(description, ruvia::string, RUVIA_OMIT_EMPTY),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string),
    RUVIA_OPTIONAL_FIELD(permissions, ruvia::array<ruvia::string>),
    RUVIA_OPTIONAL_FIELD_NAME("created_at", createdAt, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("updated_at", updatedAt, ruvia::string));

RUVIA_MODEL(RolePageDataDto,
    RUVIA_OPTIONAL_FIELD(list, ruvia::boxed_array<RoleItemDto>),
    RUVIA_OPTIONAL_FIELD(total, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(page, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("totalPages", totalPages, ruvia::int64));

RUVIA_MODEL(RolePageResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, RolePageDataDto));

RUVIA_MODEL(RoleOptionsResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, ruvia::boxed_array<RoleOptionDto>));

} // namespace service::role
