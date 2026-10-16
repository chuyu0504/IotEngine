#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <ruvia/core/event_loop_pool.h>

#include "service/modules/link/link.service.h"

namespace {

void require(bool condition, std::string_view message) {
    if (!condition)
        throw std::runtime_error(std::string(message));
}

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
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

// ===== 假数据库：编译 LinkService 发出的 db_query，按顺序返回预设结果 =====

struct FakeCell {
    std::optional<std::string> text;
    [[nodiscard]] std::optional<std::string_view> value() const {
        return text ? std::optional<std::string_view>(*text) : std::nullopt;
    }
};
using FakeRow = std::vector<FakeCell>;
using FakeRows = std::vector<FakeRow>;

struct FakeExecution {
    std::uint64_t rows = 0;
    [[nodiscard]] std::uint64_t affected_rows() const { return rows; }
};

struct InsertedLink {
    std::string name;
    std::string protocol;
    std::string endpoint;
    std::string status;
    std::string createdBy;
    std::string execution;
};

struct Recorder {
    std::vector<std::string> queries;
    std::vector<std::string> executions;
    std::vector<FakeRows> queryResults;
    std::uint64_t affectedRows = 1;
    std::optional<InsertedLink> inserted;
    int commits = 0;

    static std::string compile(const ruvia::db_query& query) {
        const auto statement = query.compile(ruvia::db_driver::postgresql, std::pmr::new_delete_resource(), ruvia::db_parameter_mode::literal);
        return std::string(statement.sql());
    }

    FakeRows nextRows(const ruvia::db_query& query) {
        queries.push_back(compile(query));
        if (queryResults.empty())
            return {};
        auto rows = std::move(queryResults.front());
        queryResults.erase(queryResults.begin());
        return rows;
    }
};

struct FakeLinkRepository {
    Recorder* recorder;
    ruvia::task<void> insert(const service::link::entities::LinkEntity& link) {
        const auto text = [](const auto& value) { return std::string(std::string_view(value)); };
        recorder->inserted = InsertedLink{ text(link.get<"name">()), text(link.get<"protocol">()), text(link.get<"endpoint">()),
                                           text(link.get<"status">()), text(link.get<"created_by">()), text(link.get<"execution">()) };
        co_return;
    }
};

struct FakeTransaction {
    Recorder* recorder;

    template <typename Entity>
    FakeLinkRepository get_repository() {
        static_assert(std::is_same_v<Entity, service::link::entities::LinkEntity>);
        return { recorder };
    }

    ruvia::task<FakeRows> query(const ruvia::db_query& query) { co_return recorder->nextRows(query); }

    ruvia::task<FakeExecution> execute(const ruvia::db_query& query) {
        recorder->executions.push_back(Recorder::compile(query));
        co_return FakeExecution{ recorder->affectedRows };
    }

    ruvia::task<void> commit() {
        ++recorder->commits;
        co_return;
    }
};

struct FakeDatabase {
    Recorder* recorder;
    ruvia::task<FakeRows> query(const ruvia::db_query& query) { co_return recorder->nextRows(query); }
    ruvia::task<FakeTransaction> begin_transaction() { co_return FakeTransaction{ recorder }; }
};

struct FakeContext {
    std::string userId = "01970000-0000-7000-8000-000000000001";
    Recorder recorder;
    FakeDatabase database{ &recorder };
    std::unique_ptr<service::common::UuidV7Generator> uuid = std::make_unique<service::common::UuidV7Generator>();

    std::pmr::memory_resource* pool() { return std::pmr::new_delete_resource(); }
    FakeDatabase& db() { return database; }

    template <typename State>
    State& worker_state() {
        static_assert(std::is_same_v<State, std::unique_ptr<service::common::UuidV7Generator>>);
        return uuid;
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

service::link::SaveLinkBody body(std::string_view json) {
    auto parsed = ruvia::from_json<service::link::SaveLinkBody>(json);
    require(parsed.has_value(), "link request body did not parse");
    return std::move(*parsed);
}

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
    require(redis.sets.size() == 1 && redis.sets[0].key == "lease-key" && !redis.sets[0].condition &&
                redis.sets[0].expiration == std::chrono::milliseconds(service::link::kDebugIdle),
            "touch must SET the lease with the idle expiration");

    require(runner.run(DebugIdle::extend(redis, keys)).empty(), "extend failed");
    require(redis.expires.size() == 1 && redis.expires[0].first == "lease-key" &&
                redis.expires[0].second == std::chrono::duration_cast<std::chrono::seconds>(service::link::kDebugIdle),
            "extend must only EXPIRE the existing lease");
    require(redis.sets.size() == 1, "extend must not recreate a cleared lease");

    require(runner.run(DebugIdle::clear(redis, keys)).empty(), "clear failed");
    require(redis.deletes == std::vector<std::string>{ "lease-key", "watch-key" }, "clear must delete lease and watch keys");

    bool leased = false;
    redis.present = false;
    require(runner.run([](const FakeRedis& source, const service::link::DebugIdleKeys& idle, bool& result) -> ruvia::task<void> {
                result = co_await DebugIdle::leased(source, idle);
            }(redis, keys, leased)).empty(),
            "leased failed");
    require(!leased && redis.existsChecks == std::vector<std::string>{ "lease-key" }, "leased must check the lease key");
}

void verifyCreateValidation(Runner& runner) {
    struct Case {
        std::string_view json;
        std::string_view error;
    };
    const Case cases[] = {
        { R"({"execution":"edge","name":"n","protocol":"Modbus","endpoint":{"mode":"TCP Client","targets":[]}})", "15002:边缘连接参数请在设备表单中配置" },
        { R"({"name":"n","protocol":"Modbus","endpoint":{"targets":[]}})", "15002:链路模式不能为空" },
        { R"({"name":"n","protocol":"Modbus","endpoint":{"mode":"","targets":[]}})", "15002:链路模式不能为空" },
        { R"({"name":"n","protocol":"Modbus","status":"paused","endpoint":{"mode":"TCP Client","targets":[]}})", "15002:状态无效" },
        { R"({"name":"n","protocol":"Modbus","endpoint":{"mode":"TCP Client"}})", "15002:目标列表不能为空" },
        { R"({"name":"n","protocol":"Modbus","endpoint":{"mode":"UDP","targets":[]}})", "15003:链路模式无效" },
        { R"({"name":"n","protocol":"HTTP","endpoint":{"mode":"TCP Client","targets":[]}})", "15003:协议无效" },
        { R"({"name":"n","protocol":"SL651","endpoint":{"mode":"TCP Client","targets":[{"id":"a","name":"a","ip":"10.0.0.1","port":1}]}})", "15003:SL651 只支持 TCP Server 模式" },
        { R"({"name":"n","protocol":"Modbus","endpoint":{"mode":"TCP Server","ip":"10.0.0.1","port":502,"targets":[]}})", "15003:TCP Server 监听 IP 必须是 0.0.0.0" },
        { R"({"name":"n","protocol":"Modbus","endpoint":{"mode":"TCP Client","targets":[]}})", "15003:TCP Client 至少需要一个目标地址" },
        { R"({"name":"n","protocol":"Modbus","endpoint":{"mode":"TCP Client","targets":[{"id":"a","name":"a","ip":"10.0.0.1","port":1,"status":"paused"}]}})", "15003:目标状态无效" },
        { R"({"name":"n","protocol":"Modbus","endpoint":{"mode":"TCP Client","targets":[{"id":"a","name":"a","ip":"10.0.0.1","port":1},{"id":"a","name":"b","ip":"10.0.0.2","port":1}]}})", "15004:同一链路内目标 ID 不能重复" },
    };
    for (const auto& item : cases) {
        FakeContext context;
        service::link::LinkService service;
        const auto request = body(item.json);
        const auto error = runner.run(service.create(context, request));
        require(error == item.error, std::string("unexpected create validation result: ") + error + " for " + std::string(item.json));
        require(context.recorder.queries.empty() && context.recorder.executions.empty() && !context.recorder.inserted,
                "invalid link request reached the database");
    }
}

void verifyCreateSerializesEscapedEndpoint(Runner& runner) {
    FakeContext context;
    service::link::LinkService service;
    const auto request = body(R"({"name":"plant","protocol":"Modbus","endpoint":{"mode":"TCP Client","targets":[{"id":"t1","name":"A \"q\" \\ \n\u0001","ip":"192.168.1.10","port":502}]}})");
    require(runner.run(service.create(context, request)).empty(), "valid link create failed");
    const auto& inserted = context.recorder.inserted;
    require(inserted.has_value(), "link entity was not inserted");
    require(inserted->endpoint ==
                R"({"transport":"tcp","mode":"TCP Client","ip":"","port":0,"targets":[{"id":"t1","name":"A \"q\" \\ \n\u0001","ip":"192.168.1.10","port":502,"status":"enabled"}]})",
            "stored endpoint JSON is not escaped or shaped correctly: " + inserted->endpoint);
    require(ruvia::json_value::parse(inserted->endpoint).has_value(), "stored endpoint is not valid JSON");
    require(inserted->name == "plant" && inserted->protocol == "Modbus" && inserted->status == "enabled" &&
                inserted->execution == "collector" && inserted->createdBy == context.userId,
            "inserted link fields changed");
    require(context.recorder.queries.size() == 1, "TCP Client create must run one availability query");
    const auto& availability = context.recorder.queries.front();
    require(contains(availability, "\"link\".\"name\" = E'plant'"), "availability must check the link name");
    require(!contains(availability, "^[0-9]{1,5}$"), "TCP Client availability must not compare listen ports");
    require(context.recorder.executions.size() == 1 && contains(context.recorder.executions.front(), "config.changed") &&
                contains(context.recorder.executions.front(), "E'created'"),
            "create must enqueue one link created config event");
    require(context.recorder.commits == 1, "create must commit once");
}

void verifyServerAvailabilityGuardsPort(Runner& runner) {
    FakeContext context;
    service::link::LinkService service;
    const auto request = body(R"({"name":"listener","protocol":"Modbus","endpoint":{"mode":"TCP Server","ip":"0.0.0.0","port":1502,"targets":[]}})");
    context.recorder.queryResults.push_back(FakeRows{ FakeRow{ FakeCell{ "1" } } });
    require(runner.run(service.create(context, request)) == "15005:链路名称或监听地址已存在", "taken listen address must be rejected");
    require(context.recorder.queries.size() == 1, "TCP Server create must run one availability query");
    const auto& availability = context.recorder.queries.front();
    require(contains(availability, "^[0-9]{1,5}$") && contains(availability, "CASE WHEN"),
            "stored endpoint port must be cast only after a numeric guard");
    require(contains(availability, "\"link\".\"endpoint\" ->> "), "availability must read endpoint JSON fields");
    require(contains(availability, "\"link\".\"execution\" = E'collector'"), "availability must only compare collector listeners");
    require(!context.recorder.inserted && context.recorder.executions.empty(), "rejected create must not write");
}

void verifyUpdate(Runner& runner) {
    const auto storedRow = [](std::string_view owner) {
        return FakeRows{ FakeRow{ FakeCell{ "TCP Server" }, FakeCell{ "Modbus" }, FakeCell{ std::string(owner) } } };
    };
    constexpr std::string_view id = "01970000-0000-7000-8000-0000000000aa";
    {
        FakeContext context;
        service::link::LinkService service;
        context.recorder.queryResults.push_back(storedRow(context.userId));
        const auto request = body(R"({"name":"n","protocol":"S7","endpoint":{"mode":"TCP Server","ip":"0.0.0.0","port":102,"targets":[]}})");
        require(runner.run(service.update(context, id, request)) == "15006:链路模式和协议创建后不能修改", "protocol change must be rejected");
        require(contains(context.recorder.queries.front(), "\"link\".\"endpoint\" ->> "), "update lookup must read the stored mode");
        require(context.recorder.executions.empty(), "rejected update must not write");
    }
    for (const std::uint64_t affected : { std::uint64_t{ 1 }, std::uint64_t{ 0 } }) {
        FakeContext context;
        service::link::LinkService service;
        context.recorder.queryResults.push_back(storedRow(context.userId));
        context.recorder.affectedRows = affected;
        const auto request = body(R"({"name":"renamed","protocol":"Modbus","endpoint":{"mode":"TCP Server","ip":"0.0.0.0","port":502,"targets":[]}})");
        require(runner.run(service.update(context, id, request)).empty(), "valid link update failed");
        require(!context.recorder.executions.empty(), "update was not executed");
        const auto& update = context.recorder.executions.front();
        require(contains(update, "\"link\".\"name\" IS DISTINCT FROM E'renamed'") &&
                    contains(update, "\"link\".\"status\" IS DISTINCT FROM E'enabled'") &&
                    contains(update, "\"link\".\"endpoint\" IS DISTINCT FROM"),
                "update must only write changed links");
        require(context.recorder.executions.size() == (affected == 0 ? 1U : 2U), "config event must follow only an effective update");
    }
}

} // namespace

int main() {
    try {
        verifyDebugKeys();
        Runner runner;
        verifyDebugIdleCommands(runner);
        verifyCreateValidation(runner);
        verifyCreateSerializesEscapedEndpoint(runner);
        verifyServerAvailabilityGuardsPort(runner);
        verifyUpdate(runner);
        std::cout << "link service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "link service test failed: " << error.what() << '\n';
        return 1;
    }
}
