#include <iostream>
#include <stdexcept>
#include <string>

#include "service/modules/protocol/protocol.service.h"

namespace {
void require(bool value, const char* reason) {
    if (!value) {
        throw std::runtime_error(reason);
    }
}

void validate(std::string_view raw) {
    const auto body = ruvia::from_json<service::protocol::ExpressionValidateBody>(raw);
    service::protocol::ProtocolService::validateExpression(body);
}

std::string rejection(std::string_view raw) {
    try {
        validate(raw);
    } catch (const ruvia::http_error& error) {
        const auto info = error.info();
        require(info.code() == "16004" && info.status().value() == 400, "expression rejection used the wrong error code");
        return std::string(info.message());
    }
    throw std::runtime_error("invalid expression accepted");
}
} // namespace

int main() {
    try {
        validate(R"json({"expression":"if(p > 100, p / 1000, q)","variables":["p","q","unused"]})json");
        // 只编译不求值：除零等运行时错误不影响校验结果。
        validate(R"({"expression":"p / 0","variables":["p"]})");
        require(!rejection(R"({"expression":"p +","variables":["p"]})").empty(), "syntax error message empty");
        require(rejection(R"({"expression":"p + unknown","variables":["p"]})") == "未提供变量测试值：unknown", "unbound variable message changed");
        std::cout << "protocol expression validation passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
