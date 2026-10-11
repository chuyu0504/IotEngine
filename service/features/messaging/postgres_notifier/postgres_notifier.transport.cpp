#include "service/features/messaging/postgres_notifier/postgres_notifier.transport.h"

#include <libpq-fe.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <utility>
#include <string>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <cerrno>
#include <poll.h>
#endif

namespace service::message::outbox {
namespace {

constexpr auto kReadWait = std::chrono::milliseconds(1000);
constexpr auto kInitialBackoff = std::chrono::milliseconds(100);
constexpr auto kMaximumBackoff = std::chrono::seconds(5);

void notifyWake(const std::function<void()> &wake) noexcept {
    try {
        wake();
    } catch (...) {
        std::clog << "postgres notifier wake callback failed\n";
    }
}

bool waitForSocket(int socket, bool writable, std::stop_token stopToken,
                   std::chrono::steady_clock::time_point deadline) noexcept {
    if (socket < 0)
        return false;
    while (!stopToken.stop_requested()) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining <= std::chrono::milliseconds::zero())
            return false;

        const auto bounded = (std::min)(remaining, kReadWait);
#ifdef _WIN32
        fd_set readSet;
        fd_set writeSet;
        FD_ZERO(&readSet);
        FD_ZERO(&writeSet);
        if (writable) FD_SET(socket, &writeSet); else FD_SET(socket, &readSet);
        timeval timeout{};
        timeout.tv_sec = static_cast<long>(bounded.count() / 1000);
        timeout.tv_usec = static_cast<long>((bounded.count() % 1000) * 1000);
        const int result = select(0, &readSet, &writeSet,
                                  nullptr, &timeout);
#else
        pollfd descriptor{socket, static_cast<short>(writable ? POLLOUT : POLLIN), 0};
        const int result = poll(&descriptor, 1, static_cast<int>(bounded.count()));
#endif
        if (result > 0)
            return true;
        if (result == 0)
            continue;
#ifdef _WIN32
        if (WSAGetLastError() == WSAEINTR)
#else
        if (errno == EINTR)
#endif
            continue;
        return false;
    }
    return false;
}

std::string timeoutSeconds(std::optional<std::chrono::milliseconds> timeout) {
    if (!timeout || timeout->count() <= 0)
        return {};
    const auto seconds = (timeout->count() + 999) / 1000;
    return std::to_string(seconds > 1 ? seconds : 1);
}

PGconn *connect(const ruvia::db_config &config, std::stop_token stopToken) {
    const std::string port = config.port_ ? std::to_string(*config.port_) : "5432";
    const std::string connectTimeout = timeoutSeconds(config.connect_timeout_);
    const char *keywords[] = {"host", "port", "user", "password", "dbname", "application_name",
                              "connect_timeout", "keepalives_idle", "keepalives_interval",
                              "keepalives_count", nullptr};
    const char *values[] = {config.host_.c_str(), port.c_str(), config.username_.c_str(),
                            config.password_.c_str(), config.database_.c_str(), "iot-engine-outbox-listener",
                            connectTimeout.empty() ? nullptr : connectTimeout.c_str(), "60", "10", "3", nullptr};
    PGconn *connection = PQconnectStartParams(keywords, values, 0);
    if (!connection)
        return nullptr;
    const auto timeout = config.connect_timeout_.value_or(std::chrono::seconds(5));
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const PostgresPollingStatusType status = PQconnectPoll(connection);
        if (status == PGRES_POLLING_OK) {
            if (PQsetnonblocking(connection, 1) != 0) {
                PQfinish(connection);
                return nullptr;
            }
            return connection;
        }
        if (status == PGRES_POLLING_ACTIVE)
            continue;
        if (status == PGRES_POLLING_FAILED || !waitForSocket(PQsocket(connection),
                                                               status == PGRES_POLLING_WRITING,
                                                               stopToken, deadline)) {
            PQfinish(connection);
            return nullptr;
        }
    }
}

bool listen(PGconn *connection, std::stop_token stopToken) {
    if (PQsendQuery(connection, "LISTEN iot_outbox_pending") == 0)
        return false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    int flushStatus = PQflush(connection);
    while (flushStatus == 1) {
        if (!waitForSocket(PQsocket(connection), true, stopToken, deadline))
            return false;
        flushStatus = PQflush(connection);
    }
    if (flushStatus == -1)
        return false;
    for (;;) {
        if (!waitForSocket(PQsocket(connection), false, stopToken, deadline) ||
            PQconsumeInput(connection) == 0)
            return false;
        if (PQisBusy(connection))
            continue;
        bool committed = false;
        for (;;) {
            if (PQisBusy(connection)) {
                if (!waitForSocket(PQsocket(connection), false, stopToken, deadline) ||
                    PQconsumeInput(connection) == 0)
                    return false;
                continue;
            }
            PGresult *result = PQgetResult(connection);
            if (!result)
                break;
            committed = PQresultStatus(result) == PGRES_COMMAND_OK;
            const bool failed = !committed;
            PQclear(result);
            if (failed)
                return false;
        }
        return committed;
    }
}

bool drain(PGconn *connection, const std::function<void()> &wake,
           std::stop_token stopToken) {
    if (PQconsumeInput(connection) == 0)
        return false;
    bool received = false;
    while (PGnotify *notification = PQnotifies(connection)) {
        received = true;
        PQfreemem(notification);
    }
    if (received)
        notifyWake(wake);
    return !stopToken.stop_requested();
}

void waitBeforeRetry(std::chrono::milliseconds delay, std::stop_token stopToken) noexcept {
    while (delay > std::chrono::milliseconds::zero() && !stopToken.stop_requested()) {
        const auto pause = (std::min)(delay, kReadWait);
        std::this_thread::sleep_for(pause);
        delay -= pause;
    }
}

} // namespace

PostgresNotifier::PostgresNotifier(ruvia::db_config config, std::function<void()> wake)
    : config_(std::move(config)), wake_(std::move(wake)) {}

PostgresNotifier::~PostgresNotifier() { stop(); }

void PostgresNotifier::start() {
    std::scoped_lock lock(lifecycleMutex_);
    if (thread_.joinable())
        return;
    thread_ = std::jthread([this](std::stop_token token) { run(token); });
}

void PostgresNotifier::stop() {
    std::scoped_lock lock(lifecycleMutex_);
    if (!thread_.joinable()) {
        connected_.store(false, std::memory_order_release);
        return;
    }
    thread_.request_stop();
    thread_.join();
    connected_.store(false, std::memory_order_release);
}

bool PostgresNotifier::connected() const noexcept {
    return connected_.load(std::memory_order_acquire);
}

void PostgresNotifier::run(std::stop_token stopToken) {
    auto backoff = kInitialBackoff;
    while (!stopToken.stop_requested()) {
        try {
        std::unique_ptr<PGconn, decltype(&PQfinish)> connection(connect(config_, stopToken), &PQfinish);
        if (!connection || !listen(connection.get(), stopToken)) {
            connected_.store(false, std::memory_order_release);
            std::clog << "postgres notifier disconnected; retrying\n";
            waitBeforeRetry(backoff, stopToken);
            const auto doubled = backoff * 2;
            backoff = doubled < std::chrono::duration_cast<std::chrono::milliseconds>(kMaximumBackoff)
                          ? doubled
                          : std::chrono::duration_cast<std::chrono::milliseconds>(kMaximumBackoff);
            continue;
        }
        backoff = kInitialBackoff;
        connected_.store(true, std::memory_order_release);
        notifyWake(wake_);
        while (!stopToken.stop_requested()) {
            (void)waitForSocket(PQsocket(connection.get()), false, stopToken,
                                std::chrono::steady_clock::now() + kReadWait);
            if (stopToken.stop_requested())
                break;
            // A one-second timeout bounds stop latency; libpq remains nonblocking, so
            // probing it after the timeout also detects an orderly peer close.
            if (!drain(connection.get(), wake_, stopToken))
                break;
        }
        connected_.store(false, std::memory_order_release);
        } catch (...) {
            connected_.store(false, std::memory_order_release);
            std::clog << "postgres notifier failed; retrying\n";
            waitBeforeRetry(backoff, stopToken);
        }
    }
    connected_.store(false, std::memory_order_release);
}

} // namespace service::message::outbox
