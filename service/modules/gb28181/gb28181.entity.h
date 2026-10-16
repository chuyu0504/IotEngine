#pragma once

#include <string_view>

#include "service/common/message.h"

namespace service::gb28181 {

// 固定 Hash service::message::gb28181::kStateKey 的只读视图。持有 SDK 句柄的
// Collector 写入该 Hash；/media 代理只读取媒体启动标志，Ruvia Redis ORM 的
// ruvia:orm: 前缀无法映射该原键。
struct MediaStateRecord final {
    static constexpr std::string_view kKey = service::message::gb28181::kStateKey;
    static constexpr std::string_view kStartedField = service::message::gb28181::kStateMediaStartedField;

    static bool started(std::string_view value) noexcept { return value == "1"; }
};

} // namespace service::gb28181
