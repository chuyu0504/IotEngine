#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory_resource>
#include <span>
#include <string>
#include <thread>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/core/memory/memory_pool.h>
#include <ruvia/web/app.h>
#include <ruvia/web/controller.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/http_client.h>
#include <ruvia/web/redis/redis.h>

#include "service/middleware/request_context.h"

#include "tests/database.fixture.h"
#include "tests/redis_reply.fixture.h"

namespace {

RUVIA_MODEL(OperationMemoryResult, RUVIA_REQUIRED_FIELD(value, ruvia::string));

void check(bool condition, const char* message) {
    if (!condition) {
        std::fprintf(stderr, "%s\n", message);
        std::abort();
    }
}

class CountingResource final : public std::pmr::memory_resource {
  public:
    std::size_t live() const noexcept { return live_; }

  private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        auto* value = std::pmr::new_delete_resource()->allocate(bytes, alignment);
        ++live_;
        return value;
    }

    void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) override {
        --live_;
        std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }

    std::size_t live_{};
};

bool adjacent(const void* first, const void* second) {
    const auto left = reinterpret_cast<std::uintptr_t>(first);
    const auto right = reinterpret_cast<std::uintptr_t>(second);
    return left + 1 == right || right + 1 == left;
}

ruvia::task<void> coldOperationsUseClientStorage(ruvia::event_loop loop, std::uint16_t databasePort, std::uint16_t redisPort) {
    ruvia::worker_memory worker;
    std::array<std::byte, 64 * 1024> buffer{};
    ruvia::request_memory request(worker, buffer);
    check(request.upstream_resource() == worker.resource(), "request memory does not borrow its worker pool");

    ruvia::db_config databaseConfig;
    databaseConfig.driver_ = ruvia::db_driver::postgresql;
    databaseConfig.tls_.mode_ = ruvia::client_tls_mode::disabled;
    databaseConfig.port_ = databasePort;
    databaseConfig.connect_timeout_ = std::chrono::seconds(2);
    databaseConfig.query_timeout_ = std::chrono::seconds(2);
    ruvia::db_client database(loop, databaseConfig);
    ruvia::redis_config redisConfig;
    redisConfig.tls_.mode_ = ruvia::client_tls_mode::disabled;
    redisConfig.port_ = redisPort;
    redisConfig.pool_size_per_worker_ = 1;
    ruvia::redis_client redis(loop, redisConfig);
    co_await database.connect();
    co_await redis.connect();
    {
        const auto databaseHandle = database.handle();
        const auto redisHandle = redis.handle();
        for (int index = 0; index != 2000; ++index) {
            const auto* before = request.resource()->allocate(1, 1);
            {
                auto operation = databaseHandle.query("SELECT ?", std::string(2048, char('a' + index % 26)));
                (void)operation;
            }
            {
                auto operation = redisHandle.command("SET", "key", std::string(2048, char('a' + index % 26)));
                (void)operation;
            }
            const auto* after = request.resource()->allocate(1, 1);
            check(adjacent(before, after), "long lived operation advanced request arena");
        }
        const auto* before = request.resource()->allocate(1, 1);
        (void)request.resource()->allocate(2048, 1);
        const auto* after = request.resource()->allocate(1, 1);
        check(!adjacent(before, after), "arena probe missed an injected request allocation");
        const auto retained = co_await databaseHandle.query("SELECT $1", std::string(2048, 'r'));
        check(retained.front()["value"].value() == std::string(2048, 'r'), "retained DB result changed");
        for (int index = 0; index != 100; ++index) {
            const auto transient = co_await databaseHandle.query("SELECT $1", std::string(2048, 'x'));
            check(transient.front()[0].value()->size() == 2048, "DB result value mismatch");
            check(retained.front()["value"].value() == std::string(2048, 'r'), "later DB query invalidated a retained result");
        }
        for (const bool query : { true, false }) {
            bool rejected = false;
            try {
                if (query) {
                    (void)co_await databaseHandle.query("SELECT fixture_failure");
                } else {
                    (void)co_await databaseHandle.execute("SELECT fixture_failure");
                }
            } catch (const ruvia::db_error& error) {
                rejected = error.code() == ruvia::db_error::code_type::statement_failed;
            }
            check(rejected, "PostgreSQL statement failure lost its original error");
            const auto recovered = co_await databaseHandle.query("SELECT $1", std::string(2048, 'r'));
            check(recovered.front()["value"].value() == std::string(2048, 'r'), "PostgreSQL statement failure left its connection unusable");
        }
    }
    co_await redis.shutdown();
    co_await database.shutdown();
}

ruvia::db_statement ownedStatement(std::pmr::memory_resource* resource, char fill) {
    ruvia::db_query query(resource);
    query.select(query.value(std::string(2048, fill)));
    return query.compile(ruvia::db_driver::postgresql, resource, ruvia::db_parameter_mode::literal);
}

void resultStorageIsReclaimed() {
    {
        CountingResource resource;
        {
            const auto retained = ownedStatement(&resource, 'r');
            const auto expected = std::string(retained.sql());
            const auto baseline = resource.live();
            check(expected.find(std::string(2048, 'r')) != std::string::npos, "compiled DB result lost owned input");
            for (int index = 0; index != 100; ++index) {
                {
                    const auto transient = ownedStatement(&resource, char('a' + index % 26));
                    check(resource.live() > baseline, "DB statement did not allocate");
                    check(!transient.sql().empty(), "DB statement result is empty");
                }
                check(resource.live() == baseline, "DB statement storage accumulated");
                check(retained.sql() == expected, "retained DB result changed");
            }
        }
        check(resource.live() == 0, "DB statement storage was not released");
    }
    {
        CountingResource resource;
        {
            const auto retained = test::redisString("retained", &resource);
            const auto baseline = resource.live();
            for (int index = 0; index != 100; ++index) {
                {
                    const auto transient = test::redisString(
                        std::string(2048, char('a' + index % 26)),
                        &resource
                    );
                    check(transient.string().size() == 2048, "Redis result value mismatch");
                }
                check(resource.live() == baseline, "Redis result storage accumulated");
            }
            check(retained.string() == "retained", "retained Redis result changed");
        }
        check(resource.live() == 0, "Redis result storage was not released");
    }
}

class OperationMemoryController final {
  public:
    RUVIA_CONTROLLER_GROUP("/operation-memory")
    RUVIA_ROUTES_BEGIN(OperationMemoryController)
    RUVIA_GET("", probe);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> probe(ruvia::context& connection) {
        for (int index = 0; index != 2000; ++index) {
            const auto* before = connection.arena()->allocate(1, 1);
            {
                auto operation = connection.db().query("SELECT ?", std::string(2048, 'd'));
                (void)operation;
            }
            {
                auto operation = connection.redis().command("SET", "key", std::string(2048, 'r'));
                (void)operation;
            }
            {
                service::middleware::RequestContext request(connection, "memory-test");
                check(request.pool() == connection.pool(), "operation pool is not worker owned");
                (void)request.arena()->allocate(2048, 1);
            }
            const auto* after = connection.arena()->allocate(1, 1);
            check(adjacent(before, after), "operation storage advanced the HTTP request arena");
        }
        const auto* before = connection.arena()->allocate(1, 1);
        (void)connection.arena()->allocate(2048, 1);
        const auto* after = connection.arena()->allocate(1, 1);
        check(!adjacent(before, after), "HTTP arena probe missed an injected request allocation");
        service::middleware::RequestContext request(connection, "memory-test");
        OperationMemoryResult result(ruvia::model_options{ .resource_ = request.arena() });
        result.set<"value">("operation storage released");
        co_return connection.json(result);
    }
};

ruvia::task<void> verifyHttpRequest(ruvia::event_loop loop, std::uint16_t port) {
    ruvia::http_client client(loop, { .scheme_ = ruvia::http_scheme::http, .host_ = "127.0.0.1", .port_ = port, .request_timeout_ = std::chrono::seconds(5) });
    {
        const auto http = client.handle();
        auto response = co_await http.send({ .target_ = "/operation-memory" });
        const auto body = co_await response.body().read_all();
        check(response.status() == ruvia::http_status::ok && std::string_view(reinterpret_cast<const char*>(body.bytes().data()), body.size()) == R"({"value":"operation storage released"})", "serialized result did not survive operation storage release");
    }
    co_await client.shutdown();
}

void requestStorageSurvives() {
    test::PostgreSqlReplyServer database;
    test::RedisReplyServer redis("+PONG\r\n");
    ruvia::db_config databaseConfig;
    databaseConfig.driver_ = ruvia::db_driver::postgresql;
    databaseConfig.port_ = database.port();
    databaseConfig.tls_.mode_ = ruvia::client_tls_mode::disabled;
    ruvia::redis_config redisConfig;
    redisConfig.port_ = redis.port();
    redisConfig.pool_size_per_worker_ = 1;
    redisConfig.tls_.mode_ = ruvia::client_tls_mode::disabled;
    asio::io_context io;
    asio::ip::tcp::acceptor reservation(io, { asio::ip::address_v4::loopback(), 0 });
    const auto port = reservation.local_endpoint().port();
    reservation.close();
    auto& app = ruvia::app();
    std::promise<void> ready;
    auto started = ready.get_future();
    app.blocking_pool(nullptr).listen({ .address_ = "127.0.0.1", .http_ = port }).server({ .worker_count_ = 1 }).database({ .config_ = databaseConfig }).redis({ .config_ = redisConfig }).on_start([&] {
        ready.set_value();
    });
    std::exception_ptr serverError;
    std::thread serving([&] {
        try {
            app.run();
        } catch (...) {
            serverError = std::current_exception();
            try {
                ready.set_exception(serverError);
            } catch (...) {
            }
        }
    });
    try {
        if (started.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            throw std::runtime_error("operation memory app startup timed out");
        }
        started.get();
        ruvia::event_loop_pool clients({ .loop_count_ = 1 });
        clients.start();
        clients.loop(0).start(verifyHttpRequest(clients.loop(0), port)).get();
        clients.stop();
        clients.join();
    } catch (...) {
        app.stop();
        serving.join();
        throw;
    }
    app.stop();
    serving.join();
    if (serverError) {
        std::rethrow_exception(serverError);
    }
}

} // namespace

int main() {
    try {
        ruvia::event_loop_pool pool({ .loop_count_ = 1 });
        test::PostgreSqlReplyServer database;
        test::RedisReplyServer redis("+PONG\r\n");
        pool.start();
        pool.loop(0).start(coldOperationsUseClientStorage(pool.loop(0), database.port(), redis.port())).get();
        pool.stop();
        pool.join();
        resultStorageIsReclaimed();
        requestStorageSurvives();
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "%s\n", error.what());
        return 1;
    }
}
