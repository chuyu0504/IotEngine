#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/web/context.h>
#include <ruvia/web/conn_info.h>
#include "service/utils/text.h"
#include "service/utils/number.h"
#include <ruvia/web/error.h>

#include <ruvia/web/model.h>
#include "service/common/uuid.h"

namespace service::common {

RUVIA_MODEL(OperationResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string));

inline constexpr std::int64_t kValidationErrorCode{10001};
inline constexpr std::int64_t kBadRequestErrorCode{10002};
inline constexpr std::int64_t kNotFoundErrorCode{10003};
inline constexpr std::int64_t kServerErrorCode{10004};
inline constexpr std::int64_t kUnauthorizedErrorCode{11004};
inline constexpr std::int64_t kTokenExpiredErrorCode{11005};
inline constexpr std::int64_t kTokenInvalidErrorCode{11006};
inline constexpr std::int64_t kPermissionDeniedErrorCode{11007};

RUVIA_MODEL(ErrorResponse,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(message, ruvia::string));


struct Page final {
    std::int64_t page{1};
    std::int64_t pageSize{20};
    std::int64_t offset{0};
};

inline Page page(const ruvia::context_request& request) {
    const auto integer = [&request](std::string_view name, std::int64_t fallback) {
        return service::utils::parseInt64(request.query(name)).value_or(fallback);
    };
    Page result;
    result.page = std::max<std::int64_t>(1, integer("page", 1));
    result.pageSize = std::clamp<std::int64_t>(integer("pageSize", 20), 1, 100);
    result.offset = (result.page - 1) * result.pageSize;
    return result;
}


[[noreturn]] inline void fail(std::int64_t code, std::string message, std::uint16_t status) {
    const auto codeText = std::to_string(code);
    throw ruvia::http_error(ruvia::http_error_info_options{
        .status_ = ruvia::http_status_code::from_value(status),
        .code_ = codeText,
        .message_ = message,
    });
}

inline std::int64_t errorCode(std::string_view code, std::uint16_t status) {
    if (!code.empty()) {
        std::int64_t value{};
        const auto [ptr, ec] = std::from_chars(code.data(), code.data() + code.size(), value);
        if (ec == std::errc{} && ptr == code.data() + code.size())
            return value;
        if (code == "validation_failed")
            return kValidationErrorCode;
    }
    if (status == 401)
        return kUnauthorizedErrorCode;
    if (status == 403)
        return kPermissionDeniedErrorCode;
    if (status == 404)
        return kNotFoundErrorCode;
    return status >= 500 ? kServerErrorCode : kBadRequestErrorCode;
}

template <typename Response, typename Context, typename Data> Response ok(Context& c, Data&& data) {
    Response response(ruvia::model_options{.resource_ = c.arena()});
    response.template set<"code">(0)
        .template set<"message">("ok")
        .template set<"data">(std::forward<Data>(data));
    return response;
}

inline OperationResponse operation(ruvia::context& c, std::string_view message) {
    OperationResponse response(ruvia::model_options{.resource_ = c.arena()});
    response.set<"code">(0).set<"message">(message);
    return response;
}

inline ErrorResponse error(ruvia::context& c, std::int64_t code, std::string_view message) {
    ErrorResponse response(ruvia::model_options{.resource_ = c.arena()});
    response.set<"code">(code).set<"message">(message);
    return response;
}

inline void requireUuid(std::int64_t code, std::string_view value, std::string_view message) {
    if (!service::common::isUuid(value))
        service::common::fail(code, std::string(message), 400);
}

inline std::string clientIp(const ruvia::context& context) {
    if (const auto value = context.req().header("X-Real-IP")) {
        const auto resolved = service::utils::trim(*value);
        if (!resolved.empty())
            return resolved;
    }
    return std::string(context.conn().remote().address());
}

} // namespace service::common
