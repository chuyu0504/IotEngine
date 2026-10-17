#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include <ruvia/web/dotenv.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/redis/redis.h>

namespace service::config {

struct ProcessConfig final {
    ruvia::db_config database;
    ruvia::redis_config redis;
    std::string host{"0.0.0.0"};
    std::uint16_t port{1102};
    std::optional<unsigned> serviceWorkers;
    std::optional<unsigned> collectorWorkers;

    static ProcessConfig fromEnv(const ruvia::env& env) {
        ProcessConfig config;
        config.database.driver_ = ruvia::db_driver::postgresql;
        config.database.tls_.mode_ = ruvia::client_tls_mode::disabled;
        if (const auto value = env.get("DB_HOST")) config.database.host_.assign(*value);
        if (const auto value = env.get("DB_USERNAME")) config.database.username_.assign(*value);
        if (const auto value = env.get("DB_PASSWORD")) config.database.password_.assign(*value);
        if (const auto value = env.get("DB_DATABASE")) config.database.database_.assign(*value);
        config.database.port_ = env.get<std::uint16_t>("DB_PORT").value_or(5432);
        config.database.acquire_timeout_ = std::chrono::seconds(2);
        config.database.connect_timeout_ = std::chrono::seconds(5);
        config.database.query_timeout_ = std::chrono::seconds(30);
        config.redis.tls_.mode_ = ruvia::client_tls_mode::disabled;
        if (const auto value = env.get("REDIS_HOST")) config.redis.host_.assign(*value);
        if (const auto value = env.get("REDIS_PASSWORD")) config.redis.password_.assign(*value);
        config.redis.port_ = env.get<std::uint16_t>("REDIS_PORT").value_or(6379);
        config.redis.database_ = env.get<std::uint32_t>("REDIS_DATABASE").value_or(0);
        const auto poolSize = env.get<unsigned>("REDIS_POOL_SIZE_PER_WORKER").value_or(2U);
        if (poolSize == 0 || poolSize > 16)
            throw std::invalid_argument("REDIS_POOL_SIZE_PER_WORKER must be between 1 and 16");
        config.redis.pool_size_per_worker_ = poolSize;
        config.host = std::string(env.get("HOST").value_or("0.0.0.0"));
        config.port = env.get<std::uint16_t>("PORT").value_or(1102);
        config.serviceWorkers = env.get<unsigned>("SERVICE_WORKERS");
        config.collectorWorkers = env.get<unsigned>("COLLECTOR_WORKERS");
        return config;
    }
};

struct WorkerBudget final {
    unsigned cpu{};
    unsigned gb28181{};
    std::size_t service{};
    std::size_t collector{};

    static WorkerBudget resolve(const ProcessConfig& config, bool mediaEnabled, unsigned mediaThreads) {
        const auto cpu = std::max(2U, std::thread::hardware_concurrency());
        const auto reserved = mediaEnabled ? 2U * std::max(1U, mediaThreads) : 0U;
        const auto business = std::max(2U, cpu > reserved ? cpu - reserved : 0U);
        const auto count = [](std::optional<unsigned> configured, unsigned automatic, const char* name) {
            const auto value = configured.value_or(automatic);
            if (value == 0 || value > 64)
                throw std::invalid_argument(std::string(name) + " must be between 1 and 64");
            return static_cast<std::size_t>(value);
        };
        return {cpu, reserved,
                count(config.serviceWorkers, (business + 1U) / 2U, "SERVICE_WORKERS"),
                count(config.collectorWorkers, business / 2U, "COLLECTOR_WORKERS")};
    }
};

} // namespace service::config
