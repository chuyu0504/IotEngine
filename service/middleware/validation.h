#pragma once

#include <cstdint>
#include <string_view>
#include <ruvia/web/error.h>
#include <ruvia/web/fixed_string.h>
#include <ruvia/web/middleware.h>
#include <ruvia/web/next.h>
#include "service/common/http.h"

namespace service::middleware {

struct RequestValidationErrorCodes final {
    std::int64_t code;
    std::string_view field;
    std::int64_t fieldCode;
    std::string_view issueCode;
};

inline std::int64_t requestErrorCode(const ruvia::context& context, const ruvia::http_error_info& error) {
    if (error.code() == "validation_failed") {
        if (const auto* codes = context.try_request_state<RequestValidationErrorCodes>()) {
            const auto issues = error.validation_issues();
            if (!codes->field.empty() && !issues.empty() && issues.front().field() == codes->field && (codes->issueCode.empty() || issues.front().code() == codes->issueCode)) return codes->fieldCode;
            return codes->code;
        }
    }
    return service::common::errorCode(error.code(), error.status().value());
}

// json_body 的校验失败直接进入 on_error，并不向 next() 抛出异常。
// 用请求作用域绑定声明模块错误码，统一错误响应读取它；不重复解析或校验。
// 放在 path_model 之后、json_body 之前，保持认证、权限及路径校验错误不变。
template <std::int64_t Code, ruvia::fixed_string Field = "", std::int64_t FieldCode = Code, ruvia::fixed_string IssueCode = "">
class ValidationErrorCodeMiddleware final : public ruvia::middleware {
  public:
    ruvia::task<void> handle(ruvia::context& context, ruvia::next& next) {
        const RequestValidationErrorCodes codes{Code, Field.view(), FieldCode, IssueCode.view()};
        const auto binding = context.bind_request_state(codes);
        co_await next();
    }
};

} // namespace service::middleware
