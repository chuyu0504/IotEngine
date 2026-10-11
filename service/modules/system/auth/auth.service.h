#pragma once

#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>

#include <ruvia/web/app.h>
#include <ruvia/web/auth/jwt.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/modules/system/auth/auth.entity.h"
#include "service/modules/system/auth/auth.error.h"
#include "service/modules/system/auth/auth.types.h"
#include "service/modules/system/role/role.entity.h"
#include "service/modules/system/user/user.entity.h"
#include "service/utils/password.h"
#include "service/utils/redis.h"

namespace service::auth {

class AuthTokenService final {
  public:
    static std::string requiredSecret(const ruvia::env& env, std::string_view name) {
        const auto value = env.get(name);
        if (!value || value->size() < 32) {
            throw std::runtime_error(std::string(name) + " must contain at least 32 characters");
        }
        return std::string(*value);
    }

    static std::chrono::seconds duration(std::string_view value, std::chrono::seconds fallback) {
        if (value.empty()) {
            return fallback;
        }
        std::int64_t multiplier = 1;
        const char suffix = value.back();
        if (suffix == 'm') {
            multiplier = 60;
        } else if (suffix == 'h') {
            multiplier = 3600;
        } else if (suffix == 'd') {
            multiplier = 86400;
        }
        const bool hasSuffix = suffix == 's' || suffix == 'm' || suffix == 'h' || suffix == 'd';
        const auto number = hasSuffix ? value.substr(0, value.size() - 1) : value;
        std::int64_t amount = 0;
        const auto [end, error] =
            std::from_chars(number.data(), number.data() + number.size(), amount);
        if (number.empty() || error != std::errc{} || end != number.data() + number.size() ||
            amount <= 0 || amount > std::numeric_limits<std::int64_t>::max() / multiplier) {
            return fallback;
        }
        return std::chrono::seconds(amount * multiplier);
    }

    template <typename Context>
    static std::string sign(Context& c, const JwtPayload& payload, std::string_view secretName, std::string_view type, std::string_view durationName, std::chrono::seconds fallback) {
        const auto secret = requiredSecret(c.env(), secretName);
        ruvia::jwt_sign_options options;
        options.secret_ = secret;
        options.issuer_.assign("iot-engine");
        options.audience_.assign("iot-engine-web");
        options.subject_.assign(payload.userId);
        options.expires_in_ = duration(c.env().get(durationName).value_or(""), fallback);
        options.claims_.emplace_back(
            ruvia::jwt_claim_options{ .name_ = "user_id", .value_ = payload.userId }
        );
        options.claims_.emplace_back(
            ruvia::jwt_claim_options{ .name_ = "username", .value_ = payload.username }
        );
        options.claims_.emplace_back(ruvia::jwt_claim_options{ .name_ = "token_type", .value_ = type });
        options.resource_ = c.pool();
        const auto token = ruvia::jwt_sign(options);
        return std::string(token.data(), token.size());
    }

    template <typename Context>
    static JwtPayload verify(Context& c, std::string_view token, std::string_view secretName, std::string_view expectedType) {
        try {
            const auto secret = requiredSecret(c.env(), secretName);
            ruvia::jwt_verify_options options;
            options.token_ = token;
            options.secret_ = secret;
            options.issuer_.assign("iot-engine");
            options.audience_.assign("iot-engine-web");
            options.leeway_ = std::chrono::seconds(15);
            options.resource_ = c.pool();
            const auto decoded = ruvia::jwt_verify(options);
            const auto id = decoded.claim("user_id");
            const auto username = decoded.claim("username");
            const auto type = decoded.claim("token_type");
            if (!id || !username || !type || *type != expectedType) {
                throw JwtInvalidError("invalid token payload");
            }
            JwtPayload result;
            result.userId = std::string(*id);
            result.username = std::string(*username);
            if (!service::common::isUuid(result.userId) || result.username.empty()) {
                throw JwtInvalidError("invalid token payload");
            }
            return result;
        } catch (const JwtInvalidError&) {
            throw;
        } catch (const std::exception& error) {
            const std::string message(error.what());
            if (message.find("expired") != std::string::npos) {
                throw JwtExpiredError(message);
            }
            throw JwtInvalidError(message);
        }
    }

    template <typename Context>
    static std::string signAccessToken(Context& c, const JwtPayload& payload) {
        return sign(c, payload, "JWT_SECRET", "access", "JWT_EXPIRES_IN", std::chrono::hours(1));
    }

    template <typename Context>
    static std::string signRefreshToken(Context& c, const JwtPayload& payload) {
        return sign(c, payload, "JWT_REFRESH_SECRET", "refresh", "JWT_REFRESH_EXPIRES_IN", std::chrono::hours(24 * 30));
    }

    template <typename Context>
    static JwtPayload verifyAccessToken(Context& c, std::string_view token) {
        return verify(c, token, "JWT_SECRET", "access");
    }

    template <typename Context>
    static JwtPayload verifyRefreshToken(Context& c, std::string_view token) {
        return verify(c, token, "JWT_REFRESH_SECRET", "refresh");
    }
};

class LoginRateLimiter {
  public:
    static constexpr int limit = 5;
    static constexpr std::string_view windowMilliseconds = "900000";

    template <typename Context>
    ruvia::task<bool> locked(Context& context, std::string_view username) {
        const auto reply =
            co_await service::message::redis::command(context.redis(), { "GET", LoginFailureCounter::key(username) });
        if (reply.null()) {
            co_return false;
        }
        if (reply.kind() != ruvia::redis_value::kind_type::string) {
            throw std::runtime_error("invalid login rate limit state");
        }
        co_return LoginFailureCounter::decode(reply.string()).failures >= limit;
    }

    template <typename Context>
    ruvia::task<int> failure(Context& context, std::string_view username) {
        static constexpr std::string_view script = R"lua(
local failures = redis.call('INCR', KEYS[1])
if failures == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return failures
)lua";
        const auto reply = co_await service::message::redis::command(
            context.redis(),
            { "EVAL", std::string(script), "1", LoginFailureCounter::key(username), std::string(windowMilliseconds) }
        );
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            throw std::runtime_error("invalid login rate limit increment");
        }
        co_return static_cast<int>(reply.integer());
    }

    template <typename Context>
    ruvia::task<void> clear(Context& context, std::string_view username) {
        (void)co_await service::message::redis::command(context.redis(), { "DEL", LoginFailureCounter::key(username) });
    }
};

class AuthService {
  public:
    explicit AuthService(LoginRateLimiter& limiter) : limiter_(limiter) {}

    template <typename Context>
    static ruvia::task<void> requirePermission(Context& c, std::string_view userId, std::string_view permission) {
        ruvia::db_expressions expressions(c.pool());
        auto query = enabledRoles(c, userId);
        query.template join<service::user::UserEntity>(ruvia::db_join_type::inner, "u", expressions.binary(expressions.column("id", "u"), ruvia::db_binary_operator::equal, expressions.column("user_id", "ur")))
            .and_where(expressions.binary(
                expressions.binary(expressions.column("status", "u"), ruvia::db_binary_operator::equal, expressions.value("enabled")),
                ruvia::db_binary_operator::and_value,
                expressions.unary(ruvia::db_unary_operator::is_null, expressions.column("deleted_at", "u"))
            ))
            .and_where(expressions.binary(
                expressions.binary(expressions.column("code", "r"), ruvia::db_binary_operator::equal, expressions.value("superadmin")),
                ruvia::db_binary_operator::or_value,
                expressions.binary(
                    expressions.binary(expressions.column("permissions", "r"), ruvia::db_binary_operator::json_has_key, expressions.value("*")),
                    ruvia::db_binary_operator::or_value,
                    expressions.binary(expressions.column("permissions", "r"), ruvia::db_binary_operator::json_has_key, expressions.value(permission))
                )
            ));
        if (!co_await query.get_exists()) {
            service::common::fail(service::common::kPermissionDeniedErrorCode, "无权限", 403);
        }
    }

    template <typename Context>
    ruvia::task<LoginResultDto> login(Context& c, const LoginBody& body) {
        const std::string username(body.get<"username">().view());
        const std::string password(body.get<"password">().view());
        if (co_await limiter_.locked(c, username)) {
            service::common::fail(11003, "登录失败次数过多，请 15 分钟后再试", 429);
        }

        ruvia::db_find_options options;
        options.where_ = service::user::UserEntity::column<"username">() == username &&
            service::user::UserEntity::column<"deleted_at">().is_null();
        const auto user = co_await c.db().template get_repository<service::user::UserEntity>().find_one(options);

        if (!user || !service::utils::comparePassword(password, user->template get<"password_hash">())) {
            const int remaining = LoginRateLimiter::limit - co_await limiter_.failure(c, username);
            const auto message = remaining > 0 ? "用户名或密码错误，还剩 " +
                    std::to_string(remaining) + " 次尝试机会"
                                               : "登录失败次数过多，请 15 分钟后再试";
            service::common::fail(11001, message, remaining > 0 ? 401 : 429);
        }

        if (user->template get<"status">() != "enabled") {
            service::common::fail(11002, "用户已被禁用", 403);
        }
        co_await limiter_.clear(c, username);

        const std::string userId(user->template get<"id">());
        const std::string nickname(user->template is_null<"nickname">() ? std::string_view{} : user->template get<"nickname">());
        const std::string status(user->template get<"status">());
        JwtPayload payload{ userId, username };
        LoginResultDto result(ruvia::model_options{ .resource_ = c.arena() });
        result.set<"token">(AuthTokenService::signAccessToken(c, payload))
            .template set<"refreshToken">(AuthTokenService::signRefreshToken(c, payload))
            .template set<"user">(co_await buildUser(c, userId, username, nickname, status));
        co_return result;
    }

    template <typename Context>
    ruvia::task<LoginResultDto> refresh(Context& c, const RefreshBody& body) {
        JwtPayload payload;
        try {
            payload = AuthTokenService::verifyRefreshToken(c, body.get<"refreshToken">().view());
        } catch (...) {
            service::common::fail(service::common::kTokenInvalidErrorCode, "刷新令牌无效", 401);
        }
        ruvia::db_find_options options;
        options.where_ = service::common::database::activeId<service::user::UserEntity>(payload.userId);
        const auto user = co_await c.db().template get_repository<service::user::UserEntity>().find_one(options);
        if (!user) {
            service::common::fail(11008, "用户不存在", 404);
        }
        if (user->template get<"status">() != "enabled") {
            service::common::fail(11002, "用户已被禁用", 403);
        }

        const std::string userId(user->template get<"id">());
        const std::string username(user->template get<"username">());
        const std::string nickname(user->template is_null<"nickname">() ? std::string_view{} : user->template get<"nickname">());
        const std::string status(user->template get<"status">());
        JwtPayload next{ userId, username };
        LoginResultDto result(ruvia::model_options{ .resource_ = c.arena() });
        result.set<"token">(AuthTokenService::signAccessToken(c, next))
            .template set<"refreshToken">(AuthTokenService::signRefreshToken(c, next))
            .template set<"user">(co_await buildUser(c, userId, username, nickname, status));
        co_return result;
    }

    template <typename Context>
    ruvia::task<AuthUserInfoDto> current(Context& c, std::string_view userId) {
        ruvia::db_find_options options;
        options.where_ = service::common::database::activeId<service::user::UserEntity>(userId);
        const auto user = co_await c.db().template get_repository<service::user::UserEntity>().find_one(options);
        if (!user) {
            service::common::fail(11008, "用户不存在", 404);
        }
        if (user->template get<"status">() != "enabled") {
            service::common::fail(11002, "用户已被禁用", 403);
        }
        co_return co_await buildUser(c, userId, std::string(user->template get<"username">()), std::string(user->template is_null<"nickname">() ? std::string_view{} : user->template get<"nickname">()), std::string(user->template get<"status">()));
    }

  private:
    template <typename Context>
    static ruvia::db_query_builder<service::role::RoleEntity, ruvia::db_handle>
    enabledRoles(Context& c, std::string_view userId) {
        ruvia::db_expressions expressions(c.pool());
        auto query = c.db().template get_repository<service::role::RoleEntity>().create_query_builder("r");
        query.template join<service::user::UserRoleEntity>(ruvia::db_join_type::inner, "ur", expressions.binary(expressions.column("role_id", "ur"), ruvia::db_binary_operator::equal, expressions.column("id", "r")))
            .where(expressions.binary(expressions.column("user_id", "ur"), ruvia::db_binary_operator::equal, expressions.value(userId)))
            .and_where(service::role::RoleEntity::column<"status">() == "enabled" && service::role::RoleEntity::column<"deleted_at">().is_null());
        return query;
    }

    template <typename Context>
    ruvia::task<AuthUserInfoDto> buildUser(Context& c, std::string_view userId, const std::string& username, const std::string& nickname, const std::string& status) {
        AuthUserInfoDto user(ruvia::model_options{ .resource_ = c.arena() });
        user.set<"id">(userId).set<"username">(username).set<"nickname">(nickname).set<"status">(
            status
        );

        auto roleQuery = enabledRoles(c, userId);
        roleQuery.select({ { "id" }, { "name" }, { "code" } }).order_by("id");
        const auto roles = co_await roleQuery.get_many();
        auto& roleItems = user.ensure<"roles">();
        for (const auto& row : roles) {
            auto& role = roleItems.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            role.set<"id">(row.template get<"id">())
                .template set<"name">(row.template get<"name">())
                .template set<"code">(row.template get<"code">());
        }

        ruvia::db_expressions expressions(c.pool());
        auto permissionQuery = enabledRoles(c, userId);
        permissionQuery.select({ { "permission", expressions.column("permission", "p") } })
            .distinct()
            .join_function(ruvia::db_join_type::cross, expressions.call("jsonb_array_elements_text", { expressions.column("permissions", "r") }), "p", {}, { .lateral_ = true, .columns_ = { { .name_ = "permission" } } })
            .order_by(expressions.column("permission", "p"));
        const auto permissions = co_await permissionQuery.template get_many<service::role::RolePermission>();
        auto& permissionItems = user.ensure<"permissions">();
        for (const auto& row : permissions) {
            permissionItems.emplace_back(row.template get<"permission">(), ruvia::model_options{ .resource_ = c.arena() });
        }
        co_return user;
    }

  private:
    LoginRateLimiter& limiter_;
};

inline AuthService& authService() {
    thread_local LoginRateLimiter limiter;
    thread_local AuthService service(limiter);
    return service;
}

} // namespace service::auth
