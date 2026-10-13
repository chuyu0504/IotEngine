if(NOT DEFINED SOURCE_DIR)
    message(FATAL_ERROR "SOURCE_DIR is required")
endif()

# 仅修复固定归档 eb7a4100 的编译诊断；不改变公开 API 或关闭警告。
# 文本必须精确匹配，重复配置可重入，依赖源码漂移时拒绝继续构建。
function(patch_ruvia_source RELATIVE_PATH ORIGINAL_TEXT PATCHED_TEXT)
    set(SOURCE_FILE "${SOURCE_DIR}/${RELATIVE_PATH}")
    file(READ "${SOURCE_FILE}" SOURCE_CONTENT)
    string(FIND "${SOURCE_CONTENT}" "${ORIGINAL_TEXT}" ORIGINAL_INDEX)
    if(ORIGINAL_INDEX EQUAL -1)
        string(FIND "${SOURCE_CONTENT}" "${PATCHED_TEXT}" PATCHED_INDEX)
        if(PATCHED_INDEX EQUAL -1)
            message(FATAL_ERROR "Unexpected pinned Ruvia source: ${RELATIVE_PATH}")
        endif()
        return()
    endif()
    string(REPLACE "${ORIGINAL_TEXT}" "${PATCHED_TEXT}" PATCHED_CONTENT "${SOURCE_CONTENT}")
    file(WRITE "${SOURCE_FILE}" "${PATCHED_CONTENT}")
endfunction()

# 保留四字节并行检查和非短路求值，通过整数操作明确按位与的用途。
patch_ruvia_source(
    "ruvia-http/include/ruvia/http/http_field_syntax.h"
    [=[        if (!(is_http_token_char(static_cast<unsigned char>(bytes_value[0])) &
                is_http_token_char(static_cast<unsigned char>(bytes_value[1])) &
                is_http_token_char(static_cast<unsigned char>(bytes_value[2])) &
                is_http_token_char(static_cast<unsigned char>(bytes_value[3])))) {]=]
    [=[        if (!(static_cast<unsigned>(is_http_token_char(static_cast<unsigned char>(bytes_value[0]))) &
                static_cast<unsigned>(is_http_token_char(static_cast<unsigned char>(bytes_value[1]))) &
                static_cast<unsigned>(is_http_token_char(static_cast<unsigned char>(bytes_value[2]))) &
                static_cast<unsigned>(is_http_token_char(static_cast<unsigned char>(bytes_value[3]))))) {]=])

# 泛型 lambda 显式通过捕获的对象调用，避免 Clang 误报隐式成员访问未使用捕获。
patch_ruvia_source(
    "ruvia-web/src/server/http1_response_output.h"
    "[this](auto handler) { write_remaining(std::move(handler)); }"
    "[this](auto handler) { this->write_remaining(std::move(handler)); }")

# Model 的比较运算符是可选能力，匿名命名空间内的响应 Model 也可不使用它。
patch_ruvia_source(
    "ruvia-web/include/ruvia/web/detail/model/macro/macros.h"
    "        friend bool operator==(const t_type&, const t_type&) = default;"
    "        [[maybe_unused]] friend bool operator==(const t_type&, const t_type&) = default;")
