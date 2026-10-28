#include <chrono>
#include <cstdint>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/event_loop_pool.h>

#include "service/modules/link/link.service.h"

namespace {

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

// ===== 假 Redis：记录 DebugIdle 发出的命令 =====

struct FakeRedis {
    struct SetCall {
        std::string key;
        std::optional<ruvia::redis_set_condition> condition;
        std::optional<std::chrono::milliseconds> expiration;
    };

    struct SetResult {
        [[nodiscard]] bool applied() const { return true; }
    };

    mutable std::vector<SetCall> sets;
    mutable std::vector<std::pair<std::string, std::chrono::seconds>> expires;
    mutable std::vector<std::string> deletes;
    mutable std::vector<std::string> existsChecks;
    bool present = true;

    ruvia::task<SetResult> set(std::string_view key, std::string_view, const ruvia::redis_set_options& options) const {
        SetCall call{ std::string(key), options.condition_, std::nullopt };
        if (options.expiration_ && options.expiration_->duration()) {
            call.expiration = *options.expiration_->duration();
        }
        sets.push_back(std::move(call));
        co_return SetResult{};
    }

    ruvia::task<bool> expire(std::string_view key, std::chrono::seconds ttl) const {
        expires.emplace_back(std::string(key), ttl);
        co_return present;
    }

    ruvia::task<std::int64_t> del(std::string_view key) const {
        deletes.emplace_back(key);
        co_return 1;
    }

    ruvia::task<bool> exists(std::string_view key) const {
        existsChecks.emplace_back(key);
        co_return present;
    }
};

// ===== 运行与错误捕获 =====

ruvia::task<void> capture(ruvia::task<void> operation, std::string& error) {
    try {
        co_await std::move(operation);
    } catch (const ruvia::http_error& failure) {
        error = std::string(failure.info().code()) + ":" + std::string(failure.info().message());
    }
}

class Runner {
  public:
    Runner() { pool_.start(); }

    ~Runner() {
        pool_.stop();
        pool_.join();
    }

    // 返回 "code:message"；成功时为空。
    std::string run(ruvia::task<void> operation) {
        std::string error;
        pool_.loop(0).start(capture(std::move(operation), error)).get();
        return error;
    }

  private:
    ruvia::event_loop_pool pool_{ { .loop_count_ = 1 } };
};

// ===== 用例 =====

void verifyDebugKeys() {
    namespace entities = service::link::entities;
    require(entities::debugLeaseKey("abc") == "iot:debug:lease:link:abc", "link debug lease key changed");
    require(entities::debugWatchKey("abc") == "iot:debug:watch:link:abc", "link debug watch key changed");
}

void verifyDebugIdleCommands(Runner& runner) {
    using service::link::DebugIdle;
    const service::link::DebugIdleKeys keys{ "lease-key", "watch-key" };
    FakeRedis redis;

    require(runner.run(DebugIdle::touch(redis, keys)).empty(), "touch failed");
    require(redis.sets.size() == 1 && redis.sets[0].key == "lease-key" && !redis.sets[0].condition && redis.sets[0].expiration == std::chrono::milliseconds(service::link::kDebugIdle), "touch must SET the lease with the idle expiration");

    require(runner.run(DebugIdle::extend(redis, keys)).empty(), "extend failed");
    require(redis.expires.size() == 1 && redis.expires[0].first == "lease-key" && redis.expires[0].second == std::chrono::duration_cast<std::chrono::seconds>(service::link::kDebugIdle), "extend must only EXPIRE the existing lease");
    require(redis.sets.size() == 1, "extend must not recreate a cleared lease");

    require(runner.run(DebugIdle::clear(redis, keys)).empty(), "clear failed");
    require(redis.deletes == std::vector<std::string>{ "lease-key", "watch-key" }, "clear must delete lease and watch keys");

    bool leased = false;
    redis.present = false;
    require(runner.run([](const FakeRedis& source, const service::link::DebugIdleKeys& idle, bool& result) -> ruvia::task<void> {
                      result = co_await DebugIdle::leased(source, idle);
                  }(redis, keys, leased))
                .empty(),
            "leased failed");
    require(!leased && redis.existsChecks == std::vector<std::string>{ "lease-key" }, "leased must check the lease key");
}

} // namespace

int main() {
    try {
        verifyDebugKeys();
        Runner runner;
        verifyDebugIdleCommands(runner);
        std::cout << "link service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "link service test failed: " << error.what() << '\n';
        return 1;
    }
}
