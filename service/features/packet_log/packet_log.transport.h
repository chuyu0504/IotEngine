#pragma once

#include <cstdint>
#include <span>
#include <string_view>

#include "service/features/packet_log/packet_log.config.h"

namespace service::packet_log {

void initialize(Config config);
void shutdown() noexcept;

// 调用方在构造日志上下文或载荷前先判定级别，未启用的级别不产生任何开销。
[[nodiscard]] bool enabled(Level level) noexcept;

void write(Level level, std::string_view event, const Context& context = {},
           std::span<const std::uint8_t> bytes = {}, std::string_view reason = {}) noexcept;

} // namespace service::packet_log
