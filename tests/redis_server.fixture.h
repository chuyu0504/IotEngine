#pragma once

#include <asio.hpp>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <sstream>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#else
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace test {

// 独立纯内存 Redis；仅监听随机 loopback 端口，所有临时产物归根 build。
class RedisServer final {
  public:
    RedisServer(std::filesystem::path executable, std::filesystem::path buildDirectory) {
        asio::io_context io;
        asio::ip::tcp::acceptor reservation(io, {asio::ip::address_v4::loopback(), 0});
        port_ = reservation.local_endpoint().port();
        reservation.close();
        directory_ = std::move(buildDirectory) / ("redis-route-test-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directory(directory_);
        const auto configuration = directory_ / "redis.conf";
        std::ofstream file(configuration);
        file << "bind 127.0.0.1\nport " << port_
             << "\nprotected-mode yes\nsave \"\"\nappendonly no\nlogfile redis.log\n";
        file.close();
        if (!file) throw std::runtime_error("cannot write isolated Redis configuration");
        try {
            start(std::move(executable), configuration);
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < deadline) {
                asio::ip::tcp::socket socket(io);
                asio::error_code error;
                socket.connect({asio::ip::address_v4::loopback(), port_}, error);
                if (!error) return;
                if (!running()) {
                    std::ifstream log(directory_ / "redis.log");
                    std::ostringstream reason;
                    reason << log.rdbuf();
                    throw std::runtime_error("isolated Redis exited during startup: " + reason.str());
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            throw std::runtime_error("isolated Redis startup timed out");
        } catch (...) {
            stop();
            std::filesystem::remove_all(directory_);
            throw;
        }
    }

    RedisServer(const RedisServer&) = delete;
    RedisServer& operator=(const RedisServer&) = delete;

    ~RedisServer() {
        stop();
        std::error_code ignored;
        std::filesystem::remove_all(directory_, ignored);
    }

    std::uint16_t port() const noexcept { return port_; }

  private:
    void start(std::filesystem::path executable, const std::filesystem::path& configuration) {
#ifdef _WIN32
        std::wstring command = L"\"" + executable.wstring() + L"\" \"" +
            configuration.filename().wstring() + L"\"";
        STARTUPINFOW startup{};
        startup.cb = sizeof(startup);
        PROCESS_INFORMATION process{};
        if (!CreateProcessW(executable.c_str(), command.data(), nullptr, nullptr, FALSE,
                            CREATE_NO_WINDOW, nullptr, directory_.c_str(), &startup, &process))
            throw std::runtime_error("cannot launch isolated Redis");
        process_ = process.hProcess;
        CloseHandle(process.hThread);
#else
        process_ = fork();
        if (process_ < 0) throw std::runtime_error("cannot fork isolated Redis");
        if (process_ == 0) {
            if (chdir(directory_.c_str()) != 0) _exit(126);
            execl(executable.c_str(), executable.c_str(), configuration.filename().c_str(), nullptr);
            _exit(127);
        }
#endif
    }

    bool running() const {
#ifdef _WIN32
        return process_ && WaitForSingleObject(process_, 0) == WAIT_TIMEOUT;
#else
        return process_ > 0 && kill(process_, 0) == 0;
#endif
    }

    void stop() noexcept {
#ifdef _WIN32
        if (!process_) return;
        TerminateProcess(process_, 0);
        WaitForSingleObject(process_, 5000);
        CloseHandle(process_);
        process_ = nullptr;
#else
        if (process_ <= 0) return;
        kill(process_, SIGTERM);
        int status{};
        while (waitpid(process_, &status, 0) < 0 && errno == EINTR) {}
        process_ = -1;
#endif
    }

    std::uint16_t port_{};
    std::filesystem::path directory_;
#ifdef _WIN32
    HANDLE process_{};
#else
    pid_t process_ = -1;
#endif
};

} // namespace test
