#pragma once

#include <string>

#include <ruvia/web/validation.h>

#include "service/modules/system/role/role.types.h"

namespace service::auth {

struct JwtPayload {
    std::string userId;
    std::string username;
};

RUVIA_MODEL(LoginBody, RUVIA_REQUIRED_FIELD(username, ruvia::string, RUVIA_MIN(1, "用户名不能为空")), RUVIA_REQUIRED_FIELD(password, ruvia::string, RUVIA_MIN(1, "密码不能为空")));

RUVIA_MODEL(RefreshBody, RUVIA_REQUIRED_FIELD_NAME("refresh_token", refreshToken, ruvia::string, RUVIA_MIN(1, "刷新令牌不能为空")));

RUVIA_MODEL(AuthUserInfoDto, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(username, ruvia::string), RUVIA_OPTIONAL_FIELD(nickname, ruvia::string, RUVIA_OMIT_EMPTY), RUVIA_OPTIONAL_FIELD(status, ruvia::string), RUVIA_OPTIONAL_FIELD(roles, ruvia::array<service::role::RoleOptionDto>), RUVIA_OPTIONAL_FIELD(permissions, ruvia::array<ruvia::string>));

RUVIA_MODEL(LoginResultDto, RUVIA_OPTIONAL_FIELD(token, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("refresh_token", refreshToken, ruvia::string), RUVIA_OPTIONAL_FIELD(user, AuthUserInfoDto));

RUVIA_MODEL(LoginResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, LoginResultDto));

RUVIA_MODEL(CurrentUserResponse, RUVIA_OPTIONAL_FIELD(code, ruvia::int64), RUVIA_OPTIONAL_FIELD(message, ruvia::string), RUVIA_OPTIONAL_FIELD(data, AuthUserInfoDto));

} // namespace service::auth
