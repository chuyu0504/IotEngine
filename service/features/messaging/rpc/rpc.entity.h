#pragma once

#include <string>

namespace service::messaging::rpc {

// RPC 结果以原始 JSON 存入固定 Redis String 键 Contract::reply(id)，与回执通知和
// 请求确认原子写入；Redis ORM 的自动键前缀无法表达既有键名，service 使用此映射。
struct RpcReplyRecord final {
    std::string payload;
};

} // namespace service::messaging::rpc
