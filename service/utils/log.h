#pragma once

#include <spdlog/spdlog.h>
#include <sstream>
#include <utility>

namespace service::utils {

class LogLine final {
  public:
    explicit LogLine(spdlog::level::level_enum level) noexcept : level_(level) {}

    ~LogLine() {
        try {
            spdlog::log(level_, "{}", output_.str());
        } catch (...) {
        }
    }

    LogLine(const LogLine&) = delete;
    LogLine& operator=(const LogLine&) = delete;
    LogLine(LogLine&&) = delete;
    LogLine& operator=(LogLine&&) = delete;

    template <typename T>
    LogLine& operator<<(T&& value) {
        output_ << std::forward<T>(value);
        return *this;
    }

  private:
    spdlog::level::level_enum level_;
    std::ostringstream output_;
};

} // namespace service::utils

#define LOG_TRACE ::service::utils::LogLine(spdlog::level::trace)
#define LOG_DEBUG ::service::utils::LogLine(spdlog::level::debug)
#define LOG_INFO ::service::utils::LogLine(spdlog::level::info)
#define LOG_WARN ::service::utils::LogLine(spdlog::level::warn)
#define LOG_ERROR ::service::utils::LogLine(spdlog::level::err)
