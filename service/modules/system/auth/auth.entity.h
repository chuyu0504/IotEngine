#pragma once

#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>

namespace service::auth::entities {

// 首次失败设置过期时间，后续原子递增不延长窗口；Redis ORM 不支持
// INCR 与条件 PEXPIRE 的原子组合，使用此映射维护同一条计数记录。
struct LoginFailureCounter final {
    std::int64_t failures{};

    static LoginFailureCounter decode(std::string_view value) {
        std::int64_t failures{};
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), failures);
        if (value.empty() || error != std::errc{} || end != value.data() + value.size())
            return {};
        return {failures};
    }

    static std::string key(std::string_view username) {
        return "iot:auth:login-failures:" + std::string(username);
    }
};

} // namespace service::auth::entities
