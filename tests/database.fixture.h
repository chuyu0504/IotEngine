#pragma once

#include <array>
#include <asio.hpp>
#include <chrono>
#include <cstddef>
#include <exception>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/web/db/db_handle.h>
#include <ruvia/web/db/db_repository.h>

namespace test {

// Minimal PostgreSQL peer for public-client ownership checks. It completes the
// startup and extended query exchange, returning one owned text field or a
// statement failure followed by ReadyForQuery on the same connection.
class PostgreSqlReplyServer final {
  public:
    PostgreSqlReplyServer()
        : acceptor_(io_, { asio::ip::address_v4::loopback(), 0 }),
          port_(acceptor_.local_endpoint().port()), thread_([this] {
              serve();
          }) {}

    ~PostgreSqlReplyServer() {
        asio::error_code error;
        asio::ip::tcp::socket wake(io_);
        wake.connect({ asio::ip::address_v4::loopback(), port_ }, error);
        wake.close(error);
        thread_.join();
    }

    std::uint16_t port() const noexcept { return port_; }

  private:
    static std::string number(std::uint32_t value, unsigned bytes = 4) {
        std::string output(bytes, '\0');
        for (unsigned index = 0; index < bytes; ++index) {
            output[index] = static_cast<char>(value >> (8 * (bytes - index - 1)));
        }
        return output;
    }

    static std::uint32_t length(const std::array<unsigned char, 4>& value) {
        return (std::uint32_t(value[0]) << 24) | (std::uint32_t(value[1]) << 16) |
            (std::uint32_t(value[2]) << 8) | value[3];
    }

    void serve() noexcept {
        try {
            asio::ip::tcp::socket socket(io_);
            acceptor_.accept(socket);
            const auto payload = [&] {
                std::array<unsigned char, 4> size{};
                asio::read(socket, asio::buffer(size));
                const auto count = length(size);
                if (count < 4 || count > 1024 * 1024) {
                    throw std::runtime_error("invalid PostgreSQL fixture frame");
                }
                std::string body(count - 4, '\0');
                asio::read(socket, asio::buffer(body));
                return body;
            };
            const auto send = [&](char type, std::string_view body) {
                const auto frame = std::string(1, type) + number(static_cast<std::uint32_t>(body.size() + 4)) + std::string(body);
                asio::write(socket, asio::buffer(frame));
            };
            (void)payload();
            send('R', number(0));
            send('S', std::string("client_encoding\0UTF8\0", 21));
            send('S', std::string("server_version\00018.0\0", 20));
            send('K', number(1) + number(2));
            send('Z', "I");
            bool statementFailure = false;
            std::unordered_map<std::string, bool> preparedFailures;
            for (;;) {
                char type{};
                asio::read(socket, asio::buffer(&type, 1));
                const auto input = payload();
                if (type == 'X') {
                    return;
                }
                if (type == 'C') {
                    if (!input.empty() && input.front() == 'S') {
                        preparedFailures.erase(input.c_str() + 1);
                    }
                    send('3', {});
                }
                if (type == 'P') {
                    statementFailure = input.find("fixture_failure") != std::string::npos;
                    preparedFailures[std::string(input.c_str())] = statementFailure;
                    send('1', {});
                }
                if (type == 'B') {
                    const auto statementName = input.find('\0') + 1;
                    statementFailure = preparedFailures.at(input.c_str() + statementName);
                    send('2', {});
                }
                if (type == 'D' || type == 'Q') {
                    send('T', number(1, 2) + std::string("value\0", 6) + number(0) + number(0, 2) + number(25) + number(0xffff, 2) + number(0xffffffff) + number(0, 2));
                }
                if (type == 'E' || type == 'Q') {
                    if (statementFailure) {
                        constexpr char error[] = "SERROR\0C22012\0Mfixture statement failed\0\0";
                        send('E', std::string_view(error, sizeof(error) - 1));
                    } else {
                        send('D', number(1, 2) + number(2048) + std::string(2048, 'r'));
                        send('C', std::string("SELECT 1\0", 9));
                    }
                }
                if (type == 'S' || type == 'Q') {
                    send('Z', "I");
                }
            }
        } catch (...) {
            // Client closure ends this fixture; protocol failures reach its task.
        }
    }

    asio::io_context io_;
    asio::ip::tcp::acceptor acceptor_;
    std::uint16_t port_;
    std::thread thread_;
};

struct CompiledDatabaseStatement {
    std::string sql;
    std::size_t parameterCount{};
};

template <typename QueryFactory>
ruvia::task<CompiledDatabaseStatement> compileOnLoop(ruvia::event_loop loop, std::uint16_t port, QueryFactory makeQuery, ruvia::db_parameter_mode parameterMode) {
    ruvia::db_config configuration;
    configuration.driver_ = ruvia::db_driver::postgresql;
    configuration.port_ = port;
    configuration.tls_.mode_ = ruvia::client_tls_mode::disabled;
    configuration.connect_timeout_ = std::chrono::seconds(2);
    ruvia::db_client database(loop, configuration);
    CompiledDatabaseStatement result;
    std::exception_ptr failure;
    try {
        co_await database.connect();
        auto handle = database.handle();
        const auto query = makeQuery(handle);
        const auto statement = parameterMode == ruvia::db_parameter_mode::literal
            ? query.query().compile(ruvia::db_driver::postgresql, nullptr, parameterMode)
            : query.get_query_and_parameters();
        result.sql = statement.sql();
        result.parameterCount = statement.params().size();
    } catch (...) {
        failure = std::current_exception();
    }
    co_await database.shutdown();
    if (failure) {
        std::rethrow_exception(failure);
    }
    co_return result;
}

template <typename QueryFactory>
CompiledDatabaseStatement compileDatabaseStatement(QueryFactory makeQuery, ruvia::db_parameter_mode parameterMode = ruvia::db_parameter_mode::bound) {
    PostgreSqlReplyServer peer;
    ruvia::event_loop_pool loops({ .loop_count_ = 1 });
    loops.start();
    try {
        auto result = loops.loop(0).start(compileOnLoop(loops.loop(0), peer.port(), std::move(makeQuery), parameterMode)).get();
        loops.stop();
        loops.join();
        return result;
    } catch (...) {
        loops.stop();
        loops.join();
        throw;
    }
}

} // namespace test
