#pragma once

#include "service/utils/number.h"
#include "service/utils/mqtt.h"
#include "service/features/access/access.types.h"

#include <algorithm>
#include <array>
#include <asio.hpp>
#include <asio/ssl.hpp>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <openssl/ssl.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include <vector>

#include <ruvia/core/task.h>


namespace service::access {

inline bool webhookHeaderNameEquals(std::string_view left, std::string_view right) {
    return left.size() == right.size() &&
        std::equal(left.begin(), left.end(), right.begin(), [](char a, char b) {
               return std::tolower(static_cast<unsigned char>(a)) ==
                   std::tolower(static_cast<unsigned char>(b));
           });
}

inline bool validWebhookHeaderSyntax(std::string_view name, std::string_view value) {
    if (name.empty()) {
        return false;
    }
    for (const auto ch : name) {
        const auto token = std::isalnum(static_cast<unsigned char>(ch)) || ch == '!' ||
            ch == '#' || ch == '$' || ch == '%' || ch == '&' || ch == '\'' ||
            ch == '*' || ch == '+' || ch == '-' || ch == '.' || ch == '^' ||
            ch == '_' || ch == '`' || ch == '|' || ch == '~';
        if (!token) {
            return false;
        }
    }
    return std::none_of(value.begin(), value.end(), [](char ch) {
        const auto byte = static_cast<unsigned char>(ch);
        return byte < 0x20 || byte == 0x7f;
    });
}

inline bool managedWebhookHeader(std::string_view name, bool includeApplicationHeaders) {
    static constexpr std::array<std::string_view, 11> transportHeaders{
        "host",
        "content-length",
        "transfer-encoding",
        "connection",
        "proxy-connection",
        "trailer",
        "te",
        "upgrade",
        "expect",
        "content-type",
        "user-agent"
    };
    if (std::any_of(transportHeaders.begin(), transportHeaders.end(), [&](auto reserved) {
            return webhookHeaderNameEquals(name, reserved);
        })) {
        return true;
    }
    if (!includeApplicationHeaders) {
        return false;
    }
    static constexpr std::array<std::string_view, 4> applicationHeaders{
        "x-iot-event",
        "x-iot-timestamp",
        "x-iot-delivery",
        "x-iot-signature"
    };
    return std::any_of(applicationHeaders.begin(), applicationHeaders.end(), [&](auto reserved) {
        return webhookHeaderNameEquals(name, reserved);
    });
}

inline void validateWebhookHeader(std::string_view name, std::string_view value, bool customHeader) {
    if (!validWebhookHeaderSyntax(name, value) || managedWebhookHeader(name, customHeader)) {
        throw std::invalid_argument("Webhook header is invalid or reserved");
    }
}

inline WebhookUrl parseWebhookUrl(std::string_view value) {
    if (value.find_first_of("\r\n") != std::string_view::npos ||
        value.find('#') != std::string_view::npos) {
        throw std::invalid_argument("Webhook URL contains invalid request characters");
    }
    WebhookUrl result;
    if (value.starts_with("https://")) {
        result.tls = true;
        value.remove_prefix(8);
        result.port = "443";
    } else if (value.starts_with("http://")) {
        value.remove_prefix(7);
        result.port = "80";
    } else {
        throw std::invalid_argument("Webhook URL scheme is invalid");
    }
    const auto path = value.find_first_of("/?");
    auto authority = value.substr(0, path);
    result.target = path == std::string_view::npos ? "/" : std::string(value.substr(path));
    if (result.target.starts_with('?')) {
        result.target.insert(result.target.begin(), '/');
    }
    if (authority.empty() || authority.find('@') != std::string_view::npos) {
        throw std::invalid_argument("Webhook URL authority is invalid");
    }
    if (authority.front() == '[') {
        const auto closing = authority.find(']');
        if (closing == std::string_view::npos) {
            throw std::invalid_argument("Webhook IPv6 host is invalid");
        }
        result.host = std::string(authority.substr(1, closing - 1));
        if (closing + 1 < authority.size()) {
            if (authority[closing + 1] != ':') {
                throw std::invalid_argument("Webhook URL port is invalid");
            }
            result.port = std::string(authority.substr(closing + 2));
        }
    } else if (const auto colon = authority.rfind(':'); colon != std::string_view::npos) {
        result.host = std::string(authority.substr(0, colon));
        result.port = std::string(authority.substr(colon + 1));
    } else {
        result.host = std::string(authority);
    }
    if (result.host.empty() || result.port.empty()) {
        throw std::invalid_argument("Webhook URL host or port is invalid");
    }
    return result;
}

class DeliveryClient final {
  public:
    DeliveryClient()
        : tls_(asio::ssl::context::tls_client), work_(asio::make_work_guard(io_)),
          thread_([this] {
              io_.run();
          }) {
        tls_.set_default_verify_paths();
        tls_.set_verify_mode(asio::ssl::verify_peer);
    }

    ~DeliveryClient() {
        work_.reset();
        io_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    DeliveryClient(const DeliveryClient&) = delete;
    DeliveryClient& operator=(const DeliveryClient&) = delete;

    void post(WebhookUrl url, std::string request, std::chrono::seconds timeout, bool skipTlsVerify, std::function<void(DeliveryResponse)> done) {
        asio::co_spawn(
            io_,
            [this, url = std::move(url), request = std::move(request), timeout, skipTlsVerify]() -> asio::awaitable<DeliveryResponse> {
                if (url.tls) {
                    co_return co_await exchangeTls(url, request, timeout, skipTlsVerify);
                }
                co_return co_await exchangePlain(url, request, timeout);
            },
            [done = std::move(done)](std::exception_ptr error, DeliveryResponse response) {
                if (error) {
                    try {
                        std::rethrow_exception(error);
                    } catch (const std::exception& exception) {
                        response.error = exception.what();
                    } catch (...) {
                        response.error = "unknown outbound HTTP error";
                    }
                }
                done(std::move(response));
            }
        );
    }

  private:
    template <typename Stream>
    static asio::awaitable<std::string> readResponse(Stream& stream) {
        std::string response;
        std::array<char, 8192> buffer{};
        while (response.size() < 65536) {
            std::error_code error;
            const auto size = co_await stream.async_read_some(
                asio::buffer(buffer),
                asio::redirect_error(asio::use_awaitable, error)
            );
            if (error == asio::error::eof || error == asio::ssl::error::stream_truncated ||
                size == 0) {
                break;
            }
            if (error) {
                throw std::system_error(error);
            }
            response.append(buffer.data(), std::min<std::size_t>(size, 65536 - response.size()));
        }
        co_return response;
    }

  public:
    static asio::ssl::verify_mode tlsVerifyMode(bool skipTlsVerify) noexcept {
        return skipTlsVerify ? asio::ssl::verify_none : asio::ssl::verify_peer;
    }

    static DeliveryResponse parseResponse(std::string response) {
        std::size_t offset = 0;
        for (int informational = 0; informational < 8; ++informational) {
            const auto body = response.find("\r\n\r\n", offset);
            if (body == std::string::npos) {
                throw std::runtime_error("Webhook returned incomplete HTTP headers");
            }
            const auto lineEnd = response.find("\r\n", offset);
            if (lineEnd == std::string::npos || lineEnd > body) {
                throw std::runtime_error("Webhook returned an invalid HTTP response");
            }
            const auto line = std::string_view(response).substr(offset, lineEnd - offset);
            const auto firstSpace = line.find(' ');
            const auto version = line.substr(0, firstSpace);
            if ((version != "HTTP/1.0" && version != "HTTP/1.1") ||
                firstSpace == std::string_view::npos || firstSpace + 4 > line.size() ||
                (line.size() > firstSpace + 4 && line[firstSpace + 4] != ' ')) {
                throw std::runtime_error("Webhook returned an invalid HTTP status");
            }
            const auto statusText = line.substr(firstSpace + 1, 3);
            if (!std::ranges::all_of(statusText, [](unsigned char character) {
                    return std::isdigit(character) != 0;
                })) {
                throw std::runtime_error("Webhook returned an invalid HTTP status");
            }
            const auto status = service::utils::parseInt64(
                std::optional<std::string_view>(statusText)
            );
            if (!status || *status < 100 || *status > 599) {
                throw std::runtime_error("Webhook returned an invalid HTTP status");
            }
            if (*status >= 100 && *status < 200 && *status != 101) {
                offset = body + 4;
                continue;
            }
            DeliveryResponse result;
            result.status = *status;
            result.body = response.substr(body + 4, 8192);
            return result;
        }
        throw std::runtime_error("Webhook returned too many informational responses");
    }

  private:
    static std::string hostHeader(const WebhookUrl& url) {
        const bool defaultPort = (url.tls && url.port == "443") || (!url.tls && url.port == "80");
        const auto host = url.host.find(':') == std::string::npos ? url.host : '[' + url.host + ']';
        return host + (defaultPort ? "" : ":" + url.port);
    }

    asio::awaitable<DeliveryResponse>
    exchangePlain(const WebhookUrl& url, const std::string& request, std::chrono::seconds timeout) {
        auto executor = co_await asio::this_coro::executor;
        auto socket = std::make_shared<asio::ip::tcp::socket>(executor);
        auto resolver = std::make_shared<asio::ip::tcp::resolver>(executor);
        auto timer = std::make_shared<asio::steady_timer>(executor, timeout);
        timer->async_wait([resolver, socket](const std::error_code& error) {
            if (!error) {
                resolver->cancel();
                std::error_code ignored;
                socket->cancel(ignored);
            }
        });
        const auto endpoints =
            co_await resolver->async_resolve(url.host, url.port, asio::use_awaitable);
        co_await asio::async_connect(*socket, endpoints, asio::use_awaitable);
        co_await asio::async_write(*socket, asio::buffer(request), asio::use_awaitable);
        auto response = co_await readResponse(*socket);
        timer->cancel();
        co_return parseResponse(std::move(response));
    }

    asio::awaitable<DeliveryResponse>
    exchangeTls(const WebhookUrl& url, const std::string& request, std::chrono::seconds timeout, bool skipTlsVerify) {
        auto executor = co_await asio::this_coro::executor;
        using Stream = asio::ssl::stream<asio::ip::tcp::socket>;
        auto stream = std::make_shared<Stream>(executor, tls_);
        auto resolver = std::make_shared<asio::ip::tcp::resolver>(executor);
        if (SSL_set_tlsext_host_name(stream->native_handle(), url.host.c_str()) != 1) {
            throw std::runtime_error("Webhook TLS SNI setup failed");
        }
        stream->set_verify_mode(tlsVerifyMode(skipTlsVerify));
        if (!skipTlsVerify) {
            stream->set_verify_callback(asio::ssl::host_name_verification(url.host));
        }
        auto timer = std::make_shared<asio::steady_timer>(executor, timeout);
        timer->async_wait([resolver, stream](const std::error_code& error) {
            if (!error) {
                resolver->cancel();
                std::error_code ignored;
                stream->next_layer().cancel(ignored);
            }
        });
        const auto endpoints =
            co_await resolver->async_resolve(url.host, url.port, asio::use_awaitable);
        co_await asio::async_connect(stream->next_layer(), endpoints, asio::use_awaitable);
        co_await stream->async_handshake(asio::ssl::stream_base::client, asio::use_awaitable);
        co_await asio::async_write(*stream, asio::buffer(request), asio::use_awaitable);
        auto response = co_await readResponse(*stream);
        timer->cancel();
        co_return parseResponse(std::move(response));
    }

  public:
    static std::string request(const WebhookUrl& url, std::string_view body, const std::vector<std::pair<std::string, std::string>>& headers) {
        std::string result = "POST " + url.target + " HTTP/1.1\r\nHost: " + hostHeader(url) +
            "\r\nUser-Agent: iot-engine-webhook/1.0\r\n";
        for (const auto& [name, value] : headers) {
            validateWebhookHeader(name, value, false);
            result += name + ": " + value + "\r\n";
        }
        result +=
            "Content-Type: application/json\r\nContent-Length: " + std::to_string(body.size()) +
            "\r\nConnection: close\r\n\r\n";
        result.append(body);
        return result;
    }

  public:
    void publishMqtt(service::utils::mqtt::BrokerAddress address, std::string clientId,
                     std::string username, std::string password, std::string topic,
                     std::string payload, std::chrono::seconds timeout,
                     std::function<void(DeliveryResponse)> done) {
        auto connectPacket = service::utils::mqtt::connect(clientId, username, password);
        auto publishPacket = service::utils::mqtt::publish(topic, payload);
        asio::co_spawn(io_,
            [this, address = std::move(address), connectPacket = std::move(connectPacket),
             publishPacket = std::move(publishPacket), timeout]() -> asio::awaitable<void> {
                if (address.tls) {
                    using Stream = asio::ssl::stream<asio::ip::tcp::socket>;
                    auto stream = std::make_shared<Stream>(io_, tls_);
                    stream->set_verify_mode(asio::ssl::verify_peer);
                    stream->set_verify_callback(asio::ssl::host_name_verification(address.host));
                    if (SSL_set_tlsext_host_name(stream->native_handle(), address.host.c_str()) != 1)
                        throw std::runtime_error("MQTT TLS SNI setup failed");
                    co_await connectMqtt(stream, address, connectPacket, publishPacket, timeout);
                } else {
                    auto stream = std::make_shared<asio::ip::tcp::socket>(io_);
                    co_await connectMqtt(stream, address, connectPacket, publishPacket, timeout);
                }
            },
            [done = std::move(done)](std::exception_ptr error) {
                DeliveryResponse response;
                if (error) {
                    try { std::rethrow_exception(error); }
                    catch (const std::exception& exception) { response.error = exception.what(); }
                    catch (...) { response.error = "unknown MQTT publish failure"; }
                }
                done(std::move(response));
            });
    }

  private:
    template <typename Stream>
    asio::awaitable<void> connectMqtt(std::shared_ptr<Stream> stream,
        const service::utils::mqtt::BrokerAddress& address, const std::string& connectPacket,
        const std::string& publishPacket, std::chrono::seconds timeout) {
        auto resolver = std::make_shared<asio::ip::tcp::resolver>(io_);
        auto timer = std::make_shared<asio::steady_timer>(io_, timeout);
        timer->async_wait([resolver, stream](const std::error_code& error) {
            if (!error) {
                resolver->cancel();
                std::error_code ignored;
                stream->lowest_layer().close(ignored);
            }
        });
        try {
            const auto endpoints = co_await resolver->async_resolve(address.host, address.port, asio::use_awaitable);
            co_await asio::async_connect(stream->lowest_layer(), endpoints, asio::use_awaitable);
            if constexpr (requires { stream->async_handshake(asio::ssl::stream_base::client, asio::use_awaitable); })
                co_await stream->async_handshake(asio::ssl::stream_base::client, asio::use_awaitable);
            co_await asio::async_write(*stream, asio::buffer(connectPacket), asio::use_awaitable);
            std::array<char, 4> acknowledgement{};
            co_await asio::async_read(*stream, asio::buffer(acknowledgement), asio::use_awaitable);
            service::utils::mqtt::checkConnack({acknowledgement.data(), acknowledgement.size()});
            co_await asio::async_write(*stream, asio::buffer(publishPacket), asio::use_awaitable);
            co_await asio::async_read(*stream, asio::buffer(acknowledgement), asio::use_awaitable);
            service::utils::mqtt::checkPuback({acknowledgement.data(), acknowledgement.size()});
            // PUBACK is the delivery boundary; a failed best-effort DISCONNECT cannot undo it.
            std::error_code ignored;
            const std::array<char, 2> disconnect{static_cast<char>(0xe0), 0};
            co_await asio::async_write(*stream, asio::buffer(disconnect), asio::redirect_error(asio::use_awaitable, ignored));
            timer->cancel();
            stream->lowest_layer().close(ignored);
        } catch (...) {
            timer->cancel();
            std::error_code ignored;
            stream->lowest_layer().close(ignored);
            throw;
        }
    }

  private:
    asio::io_context io_;
    asio::ssl::context tls_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    std::thread thread_;
};

} // namespace service::access
