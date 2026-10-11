#pragma once

#include "service/common/uuid.h"

#include <ruvia/web/validation.h>

namespace service::dept {

RUVIA_MODEL(CreateDeptBody,
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(2, "部门名称长度需在 2 - 128 之间"), RUVIA_MAX(128, "部门名称长度需在 2 - 128 之间")),
    RUVIA_OPTIONAL_FIELD(code, ruvia::string, RUVIA_MAX(64, "部门编码不能超过 64 个字符")),
    RUVIA_OPTIONAL_FIELD_NAME("parent_id", parentId, ruvia::string, RUVIA_CUSTOM("上级部门必须是 UUID", service::common::isOptionalUuidField)),
    RUVIA_OPTIONAL_FIELD_NAME("leader_id", leaderId, ruvia::string, RUVIA_CUSTOM("负责人必须是 UUID", service::common::isOptionalUuidField)),
    RUVIA_OPTIONAL_FIELD_NAME("sort_order", sortOrder, ruvia::int64, RUVIA_MIN(0, "排序不能小于 0")),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("状态无效", "enabled", "disabled")));

RUVIA_MODEL(UpdateDeptBody,
    RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(2, "部门名称长度需在 2 - 128 之间"), RUVIA_MAX(128, "部门名称长度需在 2 - 128 之间")),
    RUVIA_OPTIONAL_FIELD(code, ruvia::string, RUVIA_MAX(64, "部门编码不能超过 64 个字符")),
    RUVIA_OPTIONAL_FIELD_NAME("parent_id", parentId, ruvia::string, RUVIA_CUSTOM("上级部门必须是 UUID", service::common::isOptionalUuidField)),
    RUVIA_OPTIONAL_FIELD_NAME("leader_id", leaderId, ruvia::string, RUVIA_CUSTOM("负责人必须是 UUID", service::common::isOptionalUuidField)),
    RUVIA_OPTIONAL_FIELD_NAME("sort_order", sortOrder, ruvia::int64, RUVIA_MIN(0, "排序不能小于 0")),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("状态无效", "enabled", "disabled")));

RUVIA_MODEL(DeptListQuery,
    RUVIA_OPTIONAL_FIELD(page, ruvia::int64, RUVIA_DEFAULT(1), RUVIA_MIN(1, "page 必须大于 0")),
    RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64, RUVIA_DEFAULT(10), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")),
    RUVIA_OPTIONAL_FIELD(keyword, ruvia::string),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("状态无效", "enabled", "disabled")),
    RUVIA_OPTIONAL_FIELD_NAME("parent_id", parentId, ruvia::string, RUVIA_CUSTOM("上级部门必须是 UUID", service::common::isOptionalUuidField)));

RUVIA_MODEL(DeptIdParams,
    RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(DeptOptionDto,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("parent_id", parentId, ruvia::string));

RUVIA_MODEL(DeptItemDto,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(code, ruvia::string, RUVIA_OMIT_EMPTY),
    RUVIA_OPTIONAL_FIELD_NAME("parent_id", parentId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("parent_name", parentName, ruvia::string, RUVIA_OMIT_EMPTY),
    RUVIA_OPTIONAL_FIELD_NAME("leader_id", leaderId, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("leader_name", leaderName, ruvia::string, RUVIA_OMIT_EMPTY),
    RUVIA_OPTIONAL_FIELD_NAME("sort_order", sortOrder, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("created_at", createdAt, ruvia::string),
    RUVIA_OPTIONAL_FIELD_NAME("updated_at", updatedAt, ruvia::string));

RUVIA_MODEL(DeptPageDataDto,
    RUVIA_OPTIONAL_FIELD(list, ruvia::boxed_array<DeptItemDto>),
    RUVIA_OPTIONAL_FIELD(total, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(page, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64),
    RUVIA_OPTIONAL_FIELD_NAME("totalPages", totalPages, ruvia::int64));

RUVIA_MODEL(DeptPageResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, DeptPageDataDto));

RUVIA_MODEL(DeptOptionsResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string),
    RUVIA_OPTIONAL_FIELD(data, ruvia::boxed_array<DeptOptionDto>));

} // namespace service::dept
