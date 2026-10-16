#include "service/features/packet_log/packet_log.transport.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <spdlog/async.h>
#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string>
#include <utility>

#include "service/middleware/log.h"

namespace service::packet_log {
namespace {

// 进程内唯一的报文日志 sink：一个 spdlog 线程池和一个异步 logger。它只序列化已格式化的
// 文本行，不持有连接、会话或任何业务状态，因此各 Worker 共用它不会转交 Worker 的工作。
struct Sink final {
    std::shared_ptr<spdlog::details::thread_pool> pool;
    std::shared_ptr<spdlog::async_logger> logger;

    ~Sink() {
        if (logger) {
            try {
                logger->flush();
            } catch (const std::exception& error) {
                LOG_ERROR << "[PacketLog] flush_failed: " << error.what();
            } catch (...) {
                LOG_ERROR << "[PacketLog] flush_failed: unknown error";
            }
        }
        // 先释放 logger，再由线程池析构处理剩余队列并回收线程。
        logger.reset();
        pool.reset();
    }
};

inline constexpr std::string_view kLoggerName = "iot-engine-packet";
inline constexpr std::size_t kQueueSize = 65536;
inline constexpr std::size_t kHexLimitBytes = 64U * 1024U;
inline constexpr std::uint16_t kRetentionDays = 14;
// 低流量调试记录最迟在该间隔内落盘；错误记录立即落盘。
inline constexpr auto kFlushInterval = std::chrono::seconds(1);

std::mutex lifecycle;
std::atomic<Level> threshold{ Level::Off };
std::atomic<std::shared_ptr<const Sink>> sink;

[[nodiscard]] spdlog::level::level_enum spdLevel(Level level) noexcept {
    switch (level) {
        case Level::Trace:
            return spdlog::level::trace;
        case Level::Debug:
            return spdlog::level::debug;
        case Level::Info:
            return spdlog::level::info;
        case Level::Warn:
            return spdlog::level::warn;
        case Level::Error:
            return spdlog::level::err;
        case Level::Off:
            return spdlog::level::off;
    }
    return spdlog::level::debug;
}

void appendEscaped(std::string& output, std::string_view value) {
    output.push_back('"');
    for (const unsigned char character : value) {
        switch (character) {
            case '\\':
                output += "\\\\";
                break;
            case '"':
                output += "\\\"";
                break;
            case '\n':
                output += "\\n";
                break;
            case '\r':
                output += "\\r";
                break;
            case '\t':
                output += "\\t";
                break;
            default:
                output.push_back(character < 0x20 ? '?' : static_cast<char>(character));
                break;
        }
    }
    output.push_back('"');
}

void appendField(std::string& output, std::string_view name, std::string_view value) {
    if (value.empty()) {
        return;
    }
    output.push_back(' ');
    output.append(name);
    output.push_back('=');
    appendEscaped(output, value);
}

void appendInteger(std::string& output, std::string_view name, std::uint64_t value) {
    output.push_back(' ');
    output.append(name);
    output.push_back('=');
    output += std::to_string(value);
}

[[nodiscard]] std::string hex(std::span<const std::uint8_t> bytes, std::size_t limit) {
    static constexpr char digits[] = "0123456789ABCDEF";
    const auto count = std::min(bytes.size(), limit);
    std::string result;
    result.reserve(count == 0 ? 0 : count * 3 - 1);
    for (std::size_t index = 0; index < count; ++index) {
        if (index != 0) {
            result.push_back(' ');
        }
        result.push_back(digits[bytes[index] >> 4U]);
        result.push_back(digits[bytes[index] & 0x0FU]);
    }
    return result;
}

} // namespace

void initialize(Config config) {
    shutdown();
    if (config.level == Level::Off) {
        return;
    }
    if (config.directory.empty()) {
        throw std::invalid_argument("packet log directory is empty");
    }

    std::filesystem::create_directories(config.directory);
    // 历史版本按线程命名日志文件；保留期清理同时覆盖这些旧文件名。
    const auto oldest = std::filesystem::file_time_type::clock::now() - std::chrono::days(kRetentionDays);
    for (const auto& entry : std::filesystem::directory_iterator(config.directory)) {
        const auto name = entry.path().filename().string();
        if (entry.is_regular_file() && name.starts_with("packet-") &&
            entry.path().extension() == ".log" && entry.last_write_time() < oldest) {
            std::filesystem::remove(entry.path());
        }
    }

    auto next = std::make_shared<Sink>();
    next->pool = std::make_shared<spdlog::details::thread_pool>(kQueueSize, 1);
    auto file = std::make_shared<spdlog::sinks::daily_file_sink_mt>(
        (config.directory / "packet-collector.log").string(),
        0,
        0,
        false,
        kRetentionDays
    );
    next->logger = std::make_shared<spdlog::async_logger>(
        std::string(kLoggerName),
        std::move(file),
        next->pool,
        spdlog::async_overflow_policy::overrun_oldest
    );
    next->logger->set_level(spdLevel(config.level));
    next->logger->flush_on(spdlog::level::err);
    next->logger->set_formatter(std::make_unique<spdlog::pattern_formatter>("%Y-%m-%dT%H:%M:%S.%eZ [%l] %v", spdlog::pattern_time_type::utc));
    {
        const std::lock_guard guard(lifecycle);
        // 周期刷新由 spdlog registry 驱动，只作用于已注册的 logger。
        spdlog::register_logger(next->logger);
        spdlog::flush_every(kFlushInterval);
        sink.store(std::move(next), std::memory_order_release);
        threshold.store(config.level, std::memory_order_release);
    }
    write(Level::Info, "PACKET_LOG_STARTED", {}, {}, "async_daily_14d_overrun_oldest");
}

void shutdown() noexcept {
    std::shared_ptr<const Sink> previous;
    try {
        const std::lock_guard guard(lifecycle);
        threshold.store(Level::Off, std::memory_order_release);
        previous = sink.exchange({}, std::memory_order_acq_rel);
        if (previous) {
            spdlog::flush_every(std::chrono::seconds(0));
            spdlog::drop(std::string(kLoggerName));
        }
    } catch (const std::exception& error) {
        LOG_ERROR << "[PacketLog] shutdown_failed: " << error.what();
    } catch (...) {
        LOG_ERROR << "[PacketLog] shutdown_failed: unknown error";
    }
    // 正在写入的线程仍持有 Sink 时，由最后一个持有者完成刷新与线程回收。
    previous.reset();
}

bool enabled(Level level) noexcept {
    const auto current = threshold.load(std::memory_order_acquire);
    return level != Level::Off && current != Level::Off && level >= current;
}

void write(Level level, std::string_view event, const Context& context, std::span<const std::uint8_t> bytes, std::string_view reason) noexcept {
    if (!enabled(level)) {
        return;
    }
    try {
        const auto current = sink.load(std::memory_order_acquire);
        if (!current) {
            return;
        }

        std::string line("event=");
        appendEscaped(line, event);
        appendField(line, "direction", context.direction);
        appendField(line, "operation", context.operation);
        appendField(line, "protocol", context.protocol);
        if (context.workerIndex != std::numeric_limits<std::size_t>::max()) {
            appendInteger(line, "worker_id", context.workerIndex);
        }
        appendField(line, "link_id", context.linkId);
        appendField(line, "device_id", context.deviceId);
        appendField(line, "device_code", context.deviceCode);
        appendField(line, "connection_id", context.connectionId);
        if (context.sessionEpoch != 0) {
            appendInteger(line, "session_epoch", context.sessionEpoch);
        }
        appendField(line, "remote", context.remoteAddress);
        appendField(line, "message_id", context.messageId);
        appendField(line, "causation_id", context.causationId);
        appendField(line, "reason", reason);
        if (!bytes.empty()) {
            appendInteger(line, "bytes", bytes.size());
            const auto encoded = hex(bytes, kHexLimitBytes);
            appendField(line, "hex", encoded);
            if (bytes.size() > kHexLimitBytes) {
                appendInteger(line, "truncated_bytes", bytes.size() - kHexLimitBytes);
            }
        }
        current->logger->log(spdLevel(level), "{}", line);
    } catch (const std::exception& error) {
        LOG_ERROR << "[PacketLog] write_failed: " << error.what();
    } catch (...) {
        LOG_ERROR << "[PacketLog] write_failed: unknown error";
    }
}

} // namespace service::packet_log
