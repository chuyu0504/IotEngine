#include <asio.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include <ruvia/web/app.h>
#include <ruvia/web/controller.h>

namespace {

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

class FirmwareDownloadRoutes final {
  public:
    RUVIA_CONTROLLER_GROUP("/firmware")
    RUVIA_ROUTES_BEGIN(FirmwareDownloadRoutes)
    RUVIA_GET("/download", download);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> download(ruvia::context& c) {
        // The response must keep its file after this local path is destroyed.
        const auto path = c.worker_state<std::filesystem::path>();
        c.header("Content-Disposition", "attachment; filename=firmware.bin");
        co_return c.file({ .path_ = path, .content_type_ = "application/octet-stream" });
    }
};

std::string request(std::uint16_t port, std::string_view method, std::string_view headers = {}) {
    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    socket.connect({ asio::ip::address_v4::loopback(), port });
    const auto wire = std::string(method) + " /firmware/download HTTP/1.1\r\nHost: localhost\r\n" +
        "Connection: close\r\n" + std::string(headers) + "\r\n";
    asio::write(socket, asio::buffer(wire));
    std::string response;
    std::error_code error;
    asio::read(socket, asio::dynamic_buffer(response), error);
    require(error == asio::error::eof, "firmware connection did not close normally");
    return response;
}

std::string_view body(const std::string& response) {
    const auto end = response.find("\r\n\r\n");
    require(end != std::string::npos, "firmware response has no header terminator");
    return std::string_view(response).substr(end + 4);
}

void verifyDownloads(std::uint16_t port, const std::string& content) {
    const auto full = request(port, "GET");
    require(full.starts_with("HTTP/1.1 200 "), "full firmware download failed");
    require(full.find("attachment; filename=firmware.bin") != std::string::npos, "firmware attachment header changed");
    require(full.find("application/octet-stream") != std::string::npos, "firmware content type changed");
    require(body(full) == content, "firmware bytes changed after the handler returned");

    const auto partial = request(port, "GET", "Range: bytes=32760-32800\r\n");
    require(partial.starts_with("HTTP/1.1 206 "), "firmware byte range was not honored");
    require(partial.find("bytes 32760-32800/" + std::to_string(content.size())) != std::string::npos, "firmware content range changed");
    require(body(partial) == std::string_view(content).substr(32760, 41), "firmware resume bytes changed");

    const auto suffix = request(port, "GET", "Range: bytes=-17\r\n");
    require(suffix.starts_with("HTTP/1.1 206 ") && body(suffix) == std::string_view(content).substr(content.size() - 17), "firmware suffix range failed");

    const auto invalid = request(port, "GET", "Range: bytes=999999-\r\n");
    require(invalid.starts_with("HTTP/1.1 416 "), "unsatisfiable firmware range was accepted");
    const auto head = request(port, "HEAD");
    require(head.starts_with("HTTP/1.1 200 ") && body(head).empty(), "firmware HEAD returned a body or an error");
    require(body(request(port, "GET")) == content, "range requests damaged a subsequent download");
}

void serveAndVerify(const std::filesystem::path& path, const std::string& content) {
    asio::io_context io;
    asio::ip::tcp::acceptor reservation(io, { asio::ip::address_v4::loopback(), 0 });
    const auto port = reservation.local_endpoint().port();
    reservation.close();

    auto& app = ruvia::app();
    std::promise<void> ready;
    auto started = ready.get_future();
    app.blocking_pool(nullptr)
        .listen({ .address_ = "127.0.0.1", .http_ = port })
        .server({ .worker_count_ = 1 })
        .use_worker_state<std::filesystem::path>([path] {
            return path;
        })
        .on_start([&] {
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
        require(started.wait_for(std::chrono::seconds(10)) == std::future_status::ready, "firmware fixture startup timed out");
        started.get();
        verifyDownloads(port, content);
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
    const auto directory = std::filesystem::current_path() /
        ("firmware-download-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto path = directory / std::filesystem::path(u8"固件.bin");
    try {
        std::filesystem::create_directory(directory);
        std::string content(300 * 1024 + 17, '\0');
        for (std::size_t index = 0; index < content.size(); ++index) {
            content[index] = static_cast<char>(index % 256);
        }
        {
            std::ofstream output(path, std::ios::binary);
            output.write(content.data(), static_cast<std::streamsize>(content.size()));
            require(output.good(), "failed to write firmware fixture");
        }
        serveAndVerify(path, content);
        std::filesystem::remove(path);
        std::filesystem::remove(directory);
        std::cout << "Firmware file lifetime, Unicode path, GET, HEAD and Range passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        std::filesystem::remove(directory, ignored);
        return 1;
    }
}
