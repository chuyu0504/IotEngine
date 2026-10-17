#include <array>
#include <asio/ip/tcp.hpp>
#include <asio/ip/udp.hpp>
#include <asio/write.hpp>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <openssl/evp.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <ruvia/core/event_loop_pool.h>

#include "service/common/timestamp.h"
#include "service/features/gb28181/device/device.protocol.h"
#include "service/features/gb28181/media/media.transport.h"
#include "service/features/gb28181/sip/sip.protocol.h"
#include "service/features/gb28181/sip/sip.transport.h"
#include "service/utils/uuid.h"

#include "tests/port_reservation.fixture.h"

namespace {

using service::gb28181::MediaConfig;
using service::gb28181::SipConfig;
using service::gb28181::device::Channel;
using service::gb28181::device::Device;
using service::gb28181::device::DeviceChange;
using service::gb28181::device::DeviceRegistry;
using service::gb28181::media::ZlmSdk;
using service::gb28181::sip::SipMessage;
using service::gb28181::sip::SipRequestHead;
using service::gb28181::sip::SipServer;
using service::gb28181::sip::SipUnsupportedRequestGuard;

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

// SIP binds UDP and TCP on the same number.
std::uint16_t reserveLocalPort() {
    return test::reservePort(asio::ip::address_v4::loopback(), true);
}

std::uint16_t reserveLocalUdpPort(const asio::ip::address& address) {
    return test::reservePort(address, false);
}

std::string sipOptions(std::uint16_t clientPort, std::string_view transport) {
    return "OPTIONS sip:34020000002000000001@127.0.0.1 SIP/2.0\r\n"
           "Via: SIP/2.0/" +
        std::string(transport) + " 127.0.0.1:" + std::to_string(clientPort) +
        ";branch=z9hG4bK-test\r\n"
        "From: <sip:34020000001320000001@3402000000>;tag=test\r\n"
        "To: <sip:34020000002000000001@3402000000>\r\n"
        "Call-ID: gb28181-sip-worker-test\r\n"
        "CSeq: 1 OPTIONS\r\n"
        "Content-Length: 0\r\n\r\n";
}

std::string md5Hex(const std::string& input) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int length{};
    auto* context = EVP_MD_CTX_new();
    EVP_DigestInit_ex(context, EVP_md5(), nullptr);
    EVP_DigestUpdate(context, input.data(), input.size());
    EVP_DigestFinal_ex(context, digest.data(), &length);
    EVP_MD_CTX_free(context);
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (unsigned int index = 0; index < length; ++index) {
        output << std::setw(2) << static_cast<unsigned int>(digest[index]);
    }
    return output.str();
}

std::string challengeNonce(std::string_view response) {
    constexpr std::string_view marker{ "nonce=\"" };
    const auto begin = response.find(marker);
    if (begin == std::string_view::npos) {
        return {};
    }
    const auto value = begin + marker.size();
    const auto end = response.find('"', value);
    return end == std::string_view::npos
        ? std::string{}
        : std::string(response.substr(value, end - value));
}

std::string xmlValue(std::string_view body, std::string_view name) {
    const auto open = "<" + std::string(name) + ">";
    const auto close = "</" + std::string(name) + ">";
    const auto begin = body.find(open);
    if (begin == std::string_view::npos) {
        return {};
    }
    const auto value = begin + open.size();
    const auto end = body.find(close, value);
    return end == std::string_view::npos
        ? std::string{}
        : std::string(body.substr(value, end - value));
}

std::string sipRegister(std::uint16_t clientPort, unsigned int cseq, std::string_view nonce = {}, std::string_view fromDevice = "34020000001320000001", std::string_view authUsername = {}, std::string_view nonceCount = "00000001", std::string_view cnonce = "test-cnonce") {
    constexpr std::string_view realm{ "3402000000" };
    constexpr std::string_view password{ "test" };
    constexpr std::string_view uri{ "sip:3402000000" };
    const auto digestUser =
        authUsername.empty() ? std::string(fromDevice) : std::string(authUsername);
    std::string authorization;
    if (!nonce.empty()) {
        const auto ha1 = md5Hex(digestUser + ":" + std::string(realm) + ":" + std::string(password));
        const auto ha2 = md5Hex("REGISTER:" + std::string(uri));
        const auto response = md5Hex(ha1 + ":" + std::string(nonce) + ":" + std::string(nonceCount) + ":" + std::string(cnonce) + ":auth:" + ha2);
        authorization = "Authorization: Digest username=\"" + digestUser +
            "\", realm=\"" + std::string(realm) + "\", nonce=\"" +
            std::string(nonce) + "\", uri=\"" + std::string(uri) +
            "\", response=\"" + response +
            "\", algorithm=MD5, qop=auth, nc=" +
            std::string(nonceCount) + ", cnonce=\"" +
            std::string(cnonce) + "\"\r\n";
    }
    return "REGISTER " + std::string(uri) +
        " SIP/2.0\r\n"
        "Via: SIP/2.0/UDP 127.0.0.1:" +
        std::to_string(clientPort) + ";branch=z9hG4bK-register-" +
        std::to_string(cseq) +
        "\r\n"
        "From: <sip:" +
        std::string(fromDevice) + "@" + std::string(realm) +
        ">;tag=test\r\n"
        "To: <sip:" +
        std::string(fromDevice) + "@" + std::string(realm) +
        ">\r\n"
        "Contact: <sip:" +
        std::string(fromDevice) + "@127.0.0.1:" + std::to_string(clientPort) +
        ">\r\n"
        "Call-ID: gb28181-register-test\r\n"
        "CSeq: " +
        std::to_string(cseq) +
        " REGISTER\r\n"
        "Expires: 60\r\n" +
        authorization + "Content-Length: 0\r\n\r\n";
}

std::string sipKeepalive(
    std::uint16_t clientPort,
    std::string_view deviceId = "34020000001320000001",
    unsigned int cseq = 4,
    std::string_view callId = "gb28181-keepalive-test"
) {
    const std::string body =
        "<?xml version=\"1.0\"?>\r\n"
        "<Notify><CmdType>Keepalive</CmdType><SN>1</SN>"
        "<DeviceID>" +
        std::string(deviceId) +
        "</DeviceID><Status>OK</Status></Notify>";
    return "MESSAGE sip:34020000002000000001@3402000000 SIP/2.0\r\n"
           "Via: SIP/2.0/UDP 127.0.0.1:" +
        std::to_string(clientPort) +
        ";branch=z9hG4bK-keepalive\r\n"
        "From: <sip:" +
        std::string(deviceId) + "@3402000000>;tag=test\r\n"
                                "To: <sip:34020000002000000001@3402000000>\r\n"
                                "Call-ID: " +
        std::string(callId) + "\r\n"
                              "CSeq: " +
        std::to_string(cseq) + " MESSAGE\r\n"
                               "Content-Type: Application/MANSCDP+xml\r\n"
                               "Content-Length: " +
        std::to_string(body.size()) + "\r\n\r\n" + body;
}

std::string catalogMessage(
    std::string_view ptzType,
    std::string_view deviceId = "34020000001320000001"
) {
    const auto body =
        "<?xml version=\"1.0\"?>\r\n"
        "<Response><CmdType>Catalog</CmdType><SN>5</SN>"
        "<DeviceID>" +
        std::string(deviceId) + "</DeviceID>"
                                "<DeviceList Num=\"1\"><Item>"
                                "<DeviceID>" +
        std::string(deviceId) + "</DeviceID>"
                                "<Name>Main channel</Name>"
                                "<Manufacturer>test</Manufacturer>"
                                "<Status>ON</Status>"
                                "<Info><PTZType>" +
        std::string(ptzType) +
        "</PTZType></Info>"
        "</Item></DeviceList></Response>";
    return "MESSAGE sip:34020000002000000001@3402000000 SIP/2.0\r\n"
           "Via: SIP/2.0/UDP 127.0.0.1:0;branch=z9hG4bK-catalog\r\n"
           "From: <sip:" +
        std::string(deviceId) + "@3402000000>;tag=catalog\r\n"
                                "To: <sip:34020000002000000001@3402000000>\r\n"
                                "Call-ID: gb28181-catalog-test\r\n"
                                "CSeq: 10 MESSAGE\r\n"
                                "Content-Type: Application/MANSCDP+xml\r\n"
                                "Content-Length: " +
        std::to_string(body.size()) + "\r\n\r\n" + body;
}

template <typename Receive>
std::string receiveUntil(Receive receive, std::chrono::milliseconds timeout = std::chrono::seconds(3)) {
    std::string response;
    std::array<char, 4096> buffer{};
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        std::error_code error;
        const auto size = receive(buffer, error);
        if (!error) {
            response.append(buffer.data(), size);
            if (response.find("\r\n\r\n") != std::string::npos) {
                return response;
            }
        } else if (error != asio::error::would_block &&
                   error != asio::error::try_again) {
            throw std::runtime_error("SIP client receive failed: " + error.message());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return response;
}

template <typename Receive, typename Predicate>
std::optional<SipMessage>
receiveSipMatching(Receive receive, Predicate predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()
        );
        auto raw = receiveUntil(receive, std::max(std::chrono::milliseconds(1), remaining));
        if (raw.empty()) {
            return std::nullopt;
        }
        auto message = SipMessage::parse(raw);
        if (message.has_value() && predicate(*message)) {
            return message;
        }
    }
    return std::nullopt;
}

std::string exchangeUdp(asio::ip::udp::socket& client, const asio::ip::udp::endpoint& server, std::string_view request) {
    client.send_to(asio::buffer(request), server);
    asio::ip::udp::endpoint remote;
    return receiveUntil(
        [&client, &remote](auto& buffer, std::error_code& error) {
            return client.receive_from(asio::buffer(buffer), remote, 0, error);
        }
    );
}

void verifySipRegistrationGeneration(
    const SipConfig& sip,
    DeviceRegistry& devices,
    asio::io_context& clientContext
) {
    constexpr std::string_view deviceId{ "34020000001320000009" };
    const asio::ip::udp::endpoint endpoint(
        asio::ip::address_v4::loopback(),
        sip.port
    );
    asio::ip::udp::socket firstClient(
        clientContext,
        asio::ip::udp::endpoint(asio::ip::udp::v4(), 0)
    );
    firstClient.non_blocking(true);
    const auto firstPort = firstClient.local_endpoint().port();
    const auto challenge = exchangeUdp(
        firstClient,
        endpoint,
        sipRegister(firstPort, 30, {}, deviceId)
    );
    const auto nonce = challengeNonce(challenge);
    require(
        challenge.starts_with("SIP/2.0 401 Unauthorized") && !nonce.empty(),
        "same-peer REGISTER generation challenge was not received"
    );
    const auto firstRegistration = exchangeUdp(
        firstClient,
        endpoint,
        sipRegister(firstPort, 31, nonce, deviceId)
    );
    require(
        firstRegistration.starts_with("SIP/2.0 200 OK"),
        "same-peer REGISTER generation registration was rejected"
    );
    const auto first = devices.findDevice(std::string(deviceId));
    require(first.has_value(), "same-peer REGISTER did not create a device");

    const auto samePeerRegistration = exchangeUdp(
        firstClient,
        endpoint,
        sipRegister(firstPort, 32, nonce, deviceId, {}, "00000002")
    );
    require(
        samePeerRegistration.starts_with("SIP/2.0 200 OK"),
        "same-peer REGISTER renewal was rejected"
    );
    const auto samePeer = devices.findDevice(std::string(deviceId));
    require(
        samePeer.has_value() &&
            samePeer->sessionGeneration == first->sessionGeneration,
        "same-peer REGISTER changed the session generation"
    );

    asio::ip::udp::socket replacementClient(
        clientContext,
        asio::ip::udp::endpoint(asio::ip::udp::v4(), 0)
    );
    replacementClient.non_blocking(true);
    const auto replacementPort = replacementClient.local_endpoint().port();
    const auto replacementChallenge = exchangeUdp(
        replacementClient,
        endpoint,
        sipRegister(replacementPort, 40, {}, deviceId)
    );
    const auto replacementNonce = challengeNonce(replacementChallenge);
    require(
        replacementChallenge.starts_with("SIP/2.0 401 Unauthorized") &&
            !replacementNonce.empty(),
        "different-peer REGISTER generation challenge was not received"
    );
    const auto replacementRegistration = exchangeUdp(
        replacementClient,
        endpoint,
        sipRegister(replacementPort, 41, replacementNonce, deviceId)
    );
    require(
        replacementRegistration.starts_with("SIP/2.0 200 OK"),
        "different-peer REGISTER generation registration was rejected"
    );
    const auto replacement = devices.findDevice(std::string(deviceId));
    require(
        replacement.has_value() &&
            replacement->sessionGeneration != samePeer->sessionGeneration &&
            replacement->remoteAddress ==
                "127.0.0.1:" + std::to_string(replacementPort),
        "different-peer REGISTER did not replace the session generation and route"
    );
}

void verifyKeepaliveExpiry(service::common::UuidV7State& uuidState, std::shared_ptr<SipServer>& server, SipConfig sip, const MediaConfig& media, DeviceRegistry& devices, const std::shared_ptr<ZlmSdk>& zlm, const ruvia::event_loop& loop, asio::io_context& clientContext) {
    server->stop();
    server.reset();

    sip.port = reserveLocalPort();
    sip.transport = "udp";
    sip.registrationTimeoutSeconds = 1;
    server = std::make_shared<SipServer>(uuidState, sip, media, devices, zlm, loop);
    server->start();

    const asio::ip::udp::endpoint endpoint(asio::ip::address_v4::loopback(), sip.port);
    asio::ip::udp::socket client(
        clientContext,
        asio::ip::udp::endpoint(asio::ip::udp::v4(), 0)
    );
    client.non_blocking(true);

    const auto challenge = exchangeUdp(
        client,
        endpoint,
        sipRegister(client.local_endpoint().port(), 20)
    );
    const auto nonce = challengeNonce(challenge);
    require(challenge.starts_with("SIP/2.0 401 Unauthorized") && !nonce.empty(), "expiry test REGISTER challenge was not received");

    const auto registration = exchangeUdp(
        client,
        endpoint,
        sipRegister(client.local_endpoint().port(), 21, nonce)
    );
    require(registration.starts_with("SIP/2.0 200 OK"), "expiry test REGISTER was rejected");

    const auto keepalive = exchangeUdp(
        client,
        endpoint,
        sipKeepalive(client.local_endpoint().port())
    );
    require(keepalive.starts_with("SIP/2.0 200 OK"), "expiry test keepalive was not acknowledged");

    std::this_thread::sleep_for(std::chrono::milliseconds(1250));
    server->stop();
    const auto registered = devices.findDevice("34020000001320000001");
    require(registered.has_value(), "registered device was not retained");
    require(!registered->online, "device did not expire after keepalive timeout");
}

void verifyDualUdpWorkerOwnership(const SipConfig& baseSip, const MediaConfig& media, const std::shared_ptr<ZlmSdk>& zlm) {
    ruvia::event_loop_pool dualPool(
        ruvia::event_loop_pool_options{ .loop_count_ = 2, .queue_capacity_ = 128 }
    );
    dualPool.start();

    SipConfig sip = baseSip;
    sip.transport = "udp";
    sip.port = reserveLocalPort();
    sip.registrationTimeoutSeconds = 30;
    DeviceRegistry firstDevices;
    DeviceRegistry secondDevices;
    service::common::UuidV7State firstIds, secondIds;
    std::shared_ptr<SipServer> first;
    std::shared_ptr<SipServer> second;
    try {
        first = std::make_shared<SipServer>(firstIds, sip, media, firstDevices, zlm, dualPool.loop(0), SipServer::ViewerCountObserver{}, 0);
        second = std::make_shared<SipServer>(secondIds, sip, media, secondDevices, zlm, dualPool.loop(1), SipServer::ViewerCountObserver{}, 1);
        first->start();
        second->start();

        const asio::ip::udp::endpoint serverEndpoint(
            asio::ip::address_v4::loopback(),
            sip.port
        );
        std::size_t firstFlowCount = 0;
        std::size_t secondFlowCount = 0;
        for (unsigned int attempt = 0; attempt < 32; ++attempt) {
            const auto deviceId =
                "3402000000132" + std::to_string(1000000 + attempt);
            asio::io_context clientContext;
            asio::ip::udp::socket client(
                clientContext,
                asio::ip::udp::endpoint(asio::ip::udp::v4(), 0)
            );
            client.non_blocking(true);
            const auto clientPort = client.local_endpoint().port();

            const auto challenge = exchangeUdp(
                client,
                serverEndpoint,
                sipRegister(clientPort, 100 + attempt, {}, deviceId)
            );
            const auto nonce = challengeNonce(challenge);
            require(challenge.starts_with("SIP/2.0 401 Unauthorized") && !nonce.empty(), "dual UDP worker REGISTER challenge was not received");
            const auto registration = exchangeUdp(
                client,
                serverEndpoint,
                sipRegister(clientPort, 200 + attempt, nonce, deviceId)
            );
            require(registration.starts_with("SIP/2.0 200 OK"), "dual UDP worker REGISTER was rejected");

            std::size_t owner = std::numeric_limits<std::size_t>::max();
            const auto ownerDeadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while (std::chrono::steady_clock::now() < ownerDeadline) {
                const bool inFirst = firstDevices.findDevice(deviceId).has_value();
                const bool inSecond = secondDevices.findDevice(deviceId).has_value();
                if (inFirst || inSecond) {
                    require(!(inFirst && inSecond), "dual UDP worker REGISTER was accepted by two owners");
                    owner = inFirst ? 0 : 1;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            require(owner != std::numeric_limits<std::size_t>::max(), "dual UDP worker REGISTER owner was not observed");
            if (owner == 0) {
                ++firstFlowCount;
            } else {
                ++secondFlowCount;
            }

            const auto keepalive = sipKeepalive(clientPort, deviceId);
            client.send_to(asio::buffer(keepalive), serverEndpoint);
            asio::ip::udp::endpoint remote;
            const auto keepaliveAck = receiveSipMatching(
                [&client, &remote](auto& buffer, std::error_code& error) {
                    return client.receive_from(asio::buffer(buffer), remote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.statusCode == 200;
                },
                std::chrono::seconds(3)
            );
            require(keepaliveAck.has_value(), "dual UDP worker Keepalive was not acknowledged");

            const bool inFirstAfterKeepalive =
                firstDevices.findDevice(deviceId).has_value();
            const bool inSecondAfterKeepalive =
                secondDevices.findDevice(deviceId).has_value();
            require(!(inFirstAfterKeepalive && inSecondAfterKeepalive) && ((owner == 0 && inFirstAfterKeepalive) || (owner == 1 && inSecondAfterKeepalive)), "dual UDP worker Keepalive changed its owning worker");

            const auto catalog = catalogMessage("2", deviceId);
            client.send_to(asio::buffer(catalog), serverEndpoint);
            const auto catalogAck = receiveSipMatching(
                [&client, &remote](auto& buffer, std::error_code& error) {
                    return client.receive_from(asio::buffer(buffer), remote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.statusCode == 200;
                },
                std::chrono::seconds(3)
            );
            require(catalogAck.has_value(), "dual UDP worker Catalog was not acknowledged");

            const auto firstAfterCatalog = firstDevices.findDevice(deviceId);
            const auto secondAfterCatalog = secondDevices.findDevice(deviceId);
            require(!(firstAfterCatalog.has_value() && secondAfterCatalog.has_value()), "dual UDP worker Catalog was accepted by two owners");
            const auto& ownedDevice = owner == 0 ? firstAfterCatalog
                                                 : secondAfterCatalog;
            require(ownedDevice.has_value() && ownedDevice->channels.size() == 1 && ownedDevice->channels.front().id == deviceId, "dual UDP worker Catalog changed ownership or was not persisted");
        }
        std::cout << "dual UDP worker flow distribution: worker0=" << firstFlowCount
                  << ", worker1=" << secondFlowCount << '\n';

        first->stop();
        second->stop();
        first.reset();
        second.reset();
        dualPool.stop();
        dualPool.join();
    } catch (...) {
        if (first) {
            first->stop();
        }
        if (second) {
            second->stop();
        }
        first.reset();
        second.reset();
        dualPool.stop();
        dualPool.join();
        throw;
    }
}

void verifyAcknowledgementBarrier(
    const std::shared_ptr<SipServer>& server,
    std::uint16_t serverPort,
    asio::ip::udp::socket& client
) {
    std::mutex completionsMutex;
    std::condition_variable completionsChanged;
    std::deque<std::function<void(bool)>> completions;
    server->setAcknowledgementHandler(
        [&completionsMutex, &completionsChanged, &completions](
            std::function<void(bool)> completion
        ) {
            {
                std::lock_guard lock(completionsMutex);
                completions.push_back(std::move(completion));
            }
            completionsChanged.notify_one();
        }
    );

    const auto clientPort = client.local_endpoint().port();
    const asio::ip::udp::endpoint serverEndpoint(
        asio::ip::address_v4::loopback(),
        serverPort
    );
    asio::ip::udp::endpoint remote;
    const auto request = sipKeepalive(clientPort, "34020000001320000001", 44, "gb28181-ack-barrier-success");
    client.send_to(asio::buffer(request), serverEndpoint);

    {
        std::unique_lock lock(completionsMutex);
        require(completionsChanged.wait_for(lock, std::chrono::seconds(3), [&completions] {
            return !completions.empty();
        }),
                "SIP acknowledgement barrier was not invoked");
    }
    const auto beforePersistence = receiveSipMatching(
        [&client, &remote](auto& buffer, std::error_code& error) {
            return client.receive_from(asio::buffer(buffer), remote, 0, error);
        },
        [](const SipMessage& message) {
            return message.statusCode > 0 &&
                message.header("Call-ID") == "gb28181-ack-barrier-success";
        },
        std::chrono::milliseconds(100)
    );
    require(!beforePersistence.has_value(), "SIP 200 was sent before the persistence barrier completed");

    std::function<void(bool)> complete;
    {
        std::lock_guard lock(completionsMutex);
        require(!completions.empty(), "SIP acknowledgement completion disappeared before release");
        complete = std::move(completions.front());
        completions.pop_front();
    }
    complete(true);
    const auto persisted = receiveSipMatching(
        [&client, &remote](auto& buffer, std::error_code& error) {
            return client.receive_from(asio::buffer(buffer), remote, 0, error);
        },
        [](const SipMessage& message) {
            return message.statusCode == 200 &&
                message.header("Call-ID") == "gb28181-ack-barrier-success";
        },
        std::chrono::seconds(3)
    );
    require(persisted.has_value(), "SIP acknowledgement barrier did not send 200 after persistence");

    const auto failureRequest = sipKeepalive(
        clientPort,
        "34020000001320000001",
        45,
        "gb28181-ack-barrier-failure"
    );
    client.send_to(asio::buffer(failureRequest), serverEndpoint);
    {
        std::unique_lock lock(completionsMutex);
        require(completionsChanged.wait_for(lock, std::chrono::seconds(3), [&completions] {
            return !completions.empty();
        }),
                "SIP failure acknowledgement barrier was not invoked");
        complete = std::move(completions.front());
        completions.pop_front();
    }
    complete(false);
    const auto failed = receiveSipMatching(
        [&client, &remote](auto& buffer, std::error_code& error) {
            return client.receive_from(asio::buffer(buffer), remote, 0, error);
        },
        [](const SipMessage& message) {
            return message.statusCode == 503 &&
                message.header("Call-ID") == "gb28181-ack-barrier-failure";
        },
        std::chrono::seconds(3)
    );
    require(failed.has_value(), "SIP acknowledgement barrier did not send 503 on persistence failure");

    server->setAcknowledgementHandler({});
}

bool socketClosesWithin(asio::ip::tcp::socket& socket, std::chrono::milliseconds timeout) {
    socket.non_blocking(true);
    std::array<char, 256> buffer{};
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        std::error_code error;
        (void)socket.read_some(asio::buffer(buffer), error);
        if (error == asio::error::eof ||
            error == asio::error::connection_reset ||
            error == asio::error::connection_aborted ||
            error == asio::error::operation_aborted) {
            return true;
        }
        if (error != asio::error::would_block && error != asio::error::try_again) {
            throw std::runtime_error("SIP client close probe failed: " + error.message());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
}

std::string inviteOkWithCSeqMethod(const SipMessage& invite, std::string_view cseqMethod) {
    const auto inviteCSeq = invite.header("CSeq");
    const auto space = inviteCSeq.find(' ');
    const auto cseqNumber =
        space == std::string::npos ? inviteCSeq : inviteCSeq.substr(0, space);
    return "SIP/2.0 200 OK\r\n"
           "Via: " +
        invite.header("Via") +
        "\r\n"
        "From: " +
        invite.header("From") +
        "\r\n"
        "To: " +
        invite.header("To") +
        ";tag=device-ok\r\n"
        "Call-ID: " +
        invite.header("Call-ID") +
        "\r\n"
        "CSeq: " +
        cseqNumber + " " + std::string(cseqMethod) +
        "\r\n"
        "Content-Length: 0\r\n\r\n";
}

// Expected bytes are the actual output of the string concatenation that
// eb3a676 used for each outgoing request; serialize() must not change them.
void verifySipRequestHeadBytes() {
    constexpr std::string_view platformId{ "34020000002000000001" };
    constexpr std::string_view domain{ "3402000000" };
    constexpr std::string_view channelId{ "34020000001320000001" };
    const SipRequestHead ack{
        .method = "ACK",
        .targetUser = channelId,
        .targetHost = "192.168.1.64",
        .targetPort = 5061,
        .transport = "UDP",
        .platformId = platformId,
        .platformHost = "203.0.113.7",
        .platformPort = 5060,
        .domain = domain,
        .branch = "z9hG4bK-ack-1",
        .fromTag = "from-1",
        .echoedTo = "<sip:34020000001320000001@3402000000>;tag=device-tag",
        .callId = "invite-1@3402000000",
        .cseq = 7,
        .contact = true,
    };
    require(ack.serialize({}) == "ACK sip:34020000001320000001@192.168.1.64:5061 SIP/2.0\r\n"
                                 "Via: SIP/2.0/UDP 203.0.113.7:5060;branch=z9hG4bK-ack-1\r\n"
                                 "From: <sip:34020000002000000001@3402000000>;tag=from-1\r\n"
                                 "To: <sip:34020000001320000001@3402000000>;tag=device-tag\r\n"
                                 "Call-ID: invite-1@3402000000\r\n"
                                 "CSeq: 7 ACK\r\n"
                                 "Contact: <sip:34020000002000000001@203.0.113.7:5060>\r\n"
                                 "Max-Forwards: 70\r\n"
                                 "User-Agent: gb28181-platform-cpp\r\n"
                                 "Content-Length: 0\r\n"
                                 "\r\n",
            "ACK bytes changed");

    const std::string catalogBody =
        "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
        "<Query>\r\n"
        "<CmdType>Catalog</CmdType>\r\n"
        "<SN>11</SN>\r\n"
        "<DeviceID>34020000001110000001</DeviceID>\r\n"
        "</Query>\r\n";
    const SipRequestHead catalog{
        .method = "MESSAGE",
        .targetUser = "34020000001110000001",
        .targetHost = "192.168.1.64",
        .targetPort = 5061,
        .transport = "UDP",
        .platformId = platformId,
        .platformHost = "203.0.113.7",
        .platformPort = 5060,
        .domain = domain,
        .branch = "z9hG4bK-branch-1",
        .fromTag = "tag-1",
        .callId = "catalog-1@3402000000",
        .cseq = 11,
        .contentType = "Application/MANSCDP+xml",
    };
    require(catalog.serialize(catalogBody) == "MESSAGE sip:34020000001110000001@192.168.1.64:5061 SIP/2.0\r\n"
                                              "Via: SIP/2.0/UDP 203.0.113.7:5060;branch=z9hG4bK-branch-1\r\n"
                                              "From: <sip:34020000002000000001@3402000000>;tag=tag-1\r\n"
                                              "To: <sip:34020000001110000001@3402000000>\r\n"
                                              "Call-ID: catalog-1@3402000000\r\n"
                                              "CSeq: 11 MESSAGE\r\n"
                                              "Max-Forwards: 70\r\n"
                                              "User-Agent: gb28181-platform-cpp\r\n"
                                              "Content-Type: Application/MANSCDP+xml\r\n"
                                              "Content-Length: 144\r\n"
                                              "\r\n"
                                              "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
                                              "<Query>\r\n"
                                              "<CmdType>Catalog</CmdType>\r\n"
                                              "<SN>11</SN>\r\n"
                                              "<DeviceID>34020000001110000001</DeviceID>\r\n"
                                              "</Query>\r\n",
            "Catalog MESSAGE bytes changed");

    const std::string ptzBody =
        "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
        "<Control>\r\n"
        "<CmdType>DeviceControl</CmdType>\r\n"
        "<SN>12</SN>\r\n"
        "<DeviceID>34020000001320000001</DeviceID>\r\n"
        "<PTZCmd>A50F010800FA00B7</PTZCmd>\r\n"
        "<Info>\r\n"
        "<ControlPriority>5</ControlPriority>\r\n"
        "</Info>\r\n"
        "</Control>\r\n";
    const SipRequestHead ptz{
        .method = "MESSAGE",
        .targetUser = channelId,
        .targetHost = "fd00::64",
        .targetPort = 5062,
        .transport = "TCP",
        .platformId = platformId,
        .platformHost = "fd00::1",
        .platformPort = 15060,
        .domain = domain,
        .branch = "z9hG4bK-ptz-1",
        .fromTag = "tag-2",
        .callId = "ptz-1@3402000000",
        .cseq = 12,
        .contentType = "Application/MANSCDP+xml",
    };
    require(ptz.serialize(ptzBody) == "MESSAGE sip:34020000001320000001@[fd00::64]:5062 SIP/2.0\r\n"
                                      "Via: SIP/2.0/TCP [fd00::1]:15060;branch=z9hG4bK-ptz-1\r\n"
                                      "From: <sip:34020000002000000001@3402000000>;tag=tag-2\r\n"
                                      "To: <sip:34020000001320000001@3402000000>\r\n"
                                      "Call-ID: ptz-1@3402000000\r\n"
                                      "CSeq: 12 MESSAGE\r\n"
                                      "Max-Forwards: 70\r\n"
                                      "User-Agent: gb28181-platform-cpp\r\n"
                                      "Content-Type: Application/MANSCDP+xml\r\n"
                                      "Content-Length: 244\r\n"
                                      "\r\n"
                                      "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
                                      "<Control>\r\n"
                                      "<CmdType>DeviceControl</CmdType>\r\n"
                                      "<SN>12</SN>\r\n"
                                      "<DeviceID>34020000001320000001</DeviceID>\r\n"
                                      "<PTZCmd>A50F010800FA00B7</PTZCmd>\r\n"
                                      "<Info>\r\n"
                                      "<ControlPriority>5</ControlPriority>\r\n"
                                      "</Info>\r\n"
                                      "</Control>\r\n",
            "PTZ MESSAGE bytes changed");

    const std::string preciseBody =
        "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
        "<Control>\r\n"
        "<CmdType>DeviceControl</CmdType>\r\n"
        "<SN>13</SN>\r\n"
        "<DeviceID>34020000001320000001</DeviceID>\r\n"
        "<PTZPreciseCtrl>\r\n"
        "<Pan>90.5</Pan>\r\n"
        "<Tilt>10</Tilt>\r\n"
        "<Zoom>2</Zoom>\r\n"
        "</PTZPreciseCtrl>\r\n"
        "</Control>\r\n";
    const SipRequestHead precise{
        .method = "MESSAGE",
        .targetUser = channelId,
        .targetHost = "192.168.1.64",
        .targetPort = 5061,
        .transport = "UDP",
        .platformId = platformId,
        .platformHost = "10.0.0.1",
        .platformPort = 5060,
        .domain = domain,
        .branch = "z9hG4bK-ptz-precise-1",
        .fromTag = "tag-3",
        .callId = "ptz-precise-1@3402000000",
        .cseq = 13,
        .contentType = "Application/MANSCDP+xml",
    };
    require(precise.serialize(preciseBody) == "MESSAGE sip:34020000001320000001@192.168.1.64:5061 SIP/2.0\r\n"
                                              "Via: SIP/2.0/UDP 10.0.0.1:5060;branch=z9hG4bK-ptz-precise-1\r\n"
                                              "From: <sip:34020000002000000001@3402000000>;tag=tag-3\r\n"
                                              "To: <sip:34020000001320000001@3402000000>\r\n"
                                              "Call-ID: ptz-precise-1@3402000000\r\n"
                                              "CSeq: 13 MESSAGE\r\n"
                                              "Max-Forwards: 70\r\n"
                                              "User-Agent: gb28181-platform-cpp\r\n"
                                              "Content-Type: Application/MANSCDP+xml\r\n"
                                              "Content-Length: 241\r\n"
                                              "\r\n"
                                              "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
                                              "<Control>\r\n"
                                              "<CmdType>DeviceControl</CmdType>\r\n"
                                              "<SN>13</SN>\r\n"
                                              "<DeviceID>34020000001320000001</DeviceID>\r\n"
                                              "<PTZPreciseCtrl>\r\n"
                                              "<Pan>90.5</Pan>\r\n"
                                              "<Tilt>10</Tilt>\r\n"
                                              "<Zoom>2</Zoom>\r\n"
                                              "</PTZPreciseCtrl>\r\n"
                                              "</Control>\r\n",
            "PTZ precise MESSAGE bytes changed");

    const std::string inviteBody =
        "v=0\r\n"
        "o=34020000001320000001 0 0 IN IP4 203.0.113.7\r\n"
        "s=Play\r\n"
        "c=IN IP4 203.0.113.7\r\n"
        "t=0 0\r\n"
        "m=video 30000 TCP/RTP/AVP 96\r\n"
        "a=recvonly\r\n"
        "a=setup:passive\r\n"
        "a=connection:new\r\n"
        "a=rtpmap:96 PS/90000\r\n"
        "y=0100000001\r\n";
    const SipRequestHead invite{
        .method = "INVITE",
        .targetUser = channelId,
        .targetHost = "fd00::64",
        .targetPort = 5062,
        .transport = "TCP",
        .platformId = platformId,
        .platformHost = "fd00::1",
        .platformPort = 15060,
        .domain = domain,
        .branch = "z9hG4bK-invite-1",
        .fromTag = "from-2",
        .callId = "invite-2@3402000000",
        .cseq = 21,
        .contact = true,
        .subject = "34020000001320000001:0100000001,34020000002000000001:0",
        .contentType = "application/sdp",
    };
    require(invite.serialize(inviteBody) == "INVITE sip:34020000001320000001@[fd00::64]:5062 SIP/2.0\r\n"
                                            "Via: SIP/2.0/TCP [fd00::1]:15060;branch=z9hG4bK-invite-1\r\n"
                                            "From: <sip:34020000002000000001@3402000000>;tag=from-2\r\n"
                                            "To: <sip:34020000001320000001@3402000000>\r\n"
                                            "Call-ID: invite-2@3402000000\r\n"
                                            "CSeq: 21 INVITE\r\n"
                                            "Contact: <sip:34020000002000000001@[fd00::1]:15060>\r\n"
                                            "Max-Forwards: 70\r\n"
                                            "Subject: 34020000001320000001:0100000001,34020000002000000001:0\r\n"
                                            "User-Agent: gb28181-platform-cpp\r\n"
                                            "Content-Type: application/sdp\r\n"
                                            "Content-Length: 202\r\n"
                                            "\r\n"
                                            "v=0\r\n"
                                            "o=34020000001320000001 0 0 IN IP4 203.0.113.7\r\n"
                                            "s=Play\r\n"
                                            "c=IN IP4 203.0.113.7\r\n"
                                            "t=0 0\r\n"
                                            "m=video 30000 TCP/RTP/AVP 96\r\n"
                                            "a=recvonly\r\n"
                                            "a=setup:passive\r\n"
                                            "a=connection:new\r\n"
                                            "a=rtpmap:96 PS/90000\r\n"
                                            "y=0100000001\r\n",
            "INVITE bytes changed");

    const SipRequestHead bye{
        .method = "BYE",
        .targetUser = channelId,
        .targetHost = "192.168.1.64",
        .targetPort = 5061,
        .transport = "UDP",
        .platformId = platformId,
        .platformHost = "203.0.113.7",
        .platformPort = 5060,
        .domain = domain,
        .branch = "z9hG4bK-bye-1",
        .fromTag = "from-1",
        .toTag = "device-tag",
        .callId = "invite-1@3402000000",
        .cseq = 22,
        .contact = true,
    };
    require(bye.serialize({}) == "BYE sip:34020000001320000001@192.168.1.64:5061 SIP/2.0\r\n"
                                 "Via: SIP/2.0/UDP 203.0.113.7:5060;branch=z9hG4bK-bye-1\r\n"
                                 "From: <sip:34020000002000000001@3402000000>;tag=from-1\r\n"
                                 "To: <sip:34020000001320000001@3402000000>;tag=device-tag\r\n"
                                 "Call-ID: invite-1@3402000000\r\n"
                                 "CSeq: 22 BYE\r\n"
                                 "Contact: <sip:34020000002000000001@203.0.113.7:5060>\r\n"
                                 "Max-Forwards: 70\r\n"
                                 "User-Agent: gb28181-platform-cpp\r\n"
                                 "Content-Length: 0\r\n"
                                 "\r\n",
            "BYE bytes changed");

    const SipRequestHead cancel{
        .method = "CANCEL",
        .targetUser = channelId,
        .targetHost = "fd00::64",
        .targetPort = 5062,
        .transport = "TCP",
        .platformId = platformId,
        .platformHost = "fd00::1",
        .platformPort = 15060,
        .domain = domain,
        .branch = "z9hG4bK-invite-1",
        .fromTag = "from-2",
        .callId = "invite-2@3402000000",
        .cseq = 21,
    };
    require(cancel.serialize({}) == "CANCEL sip:34020000001320000001@[fd00::64]:5062 SIP/2.0\r\n"
                                    "Via: SIP/2.0/TCP [fd00::1]:15060;branch=z9hG4bK-invite-1\r\n"
                                    "From: <sip:34020000002000000001@3402000000>;tag=from-2\r\n"
                                    "To: <sip:34020000001320000001@3402000000>\r\n"
                                    "Call-ID: invite-2@3402000000\r\n"
                                    "CSeq: 21 CANCEL\r\n"
                                    "Max-Forwards: 70\r\n"
                                    "User-Agent: gb28181-platform-cpp\r\n"
                                    "Content-Length: 0\r\n"
                                    "\r\n",
            "CANCEL bytes changed");
}

} // namespace

int main() {
    service::common::UuidV7State uuidState;
    ruvia::event_loop_pool pool(
        ruvia::event_loop_pool_options{ .loop_count_ = 1, .queue_capacity_ = 128 }
    );
    std::shared_ptr<SipServer> server;
    std::unique_ptr<DeviceRegistry> devices;
    std::shared_ptr<ZlmSdk> zlm;
    std::vector<std::pair<std::string, unsigned int>> viewerCounts;
    std::mutex viewerCountsMutex;
    std::condition_variable viewerCountsChanged;
    std::vector<std::string> closedStreams;
    std::mutex closedStreamsMutex;
    std::condition_variable closedStreamsChanged;
    try {
        {
            SipUnsupportedRequestGuard guard(std::chrono::seconds(2));
            const auto started = SipUnsupportedRequestGuard::Clock::time_point{};
            require(!SipUnsupportedRequestGuard::requiresGuard("REGISTER sip:test SIP/2.0\r\n\r\n"), "SIP flood guard throttled REGISTER");
            require(!SipUnsupportedRequestGuard::requiresGuard("MESSAGE sip:test SIP/2.0\r\n\r\n"), "SIP flood guard throttled MESSAGE");
            require(!SipUnsupportedRequestGuard::requiresGuard("SIP/2.0 200 OK\r\n\r\n"), "SIP flood guard throttled a response");
            require(!SipUnsupportedRequestGuard::requiresGuard("OPTIONS sip:test SIP/2.0\r\n\r\n"), "SIP flood guard changed unsupported method compatibility");
            require(!guard.inspect("INVITE sip:test SIP/2.0\r\n\r\n", started).allowed, "SIP flood guard processed the first INVITE request");
            require(!guard.inspect("INVITE sip:test SIP/2.0\r\n\r\n", started).allowed, "SIP flood guard processed a repeated INVITE request");
            const auto suppressed = guard.inspect(
                "INVITE sip:test SIP/2.0\r\n\r\n",
                started + std::chrono::seconds(1)
            );
            require(!suppressed.allowed && suppressed.suppressedToReport == 0, "SIP flood guard reported before its aggregation interval");
            const auto report = guard.inspect("INVITE sip:test SIP/2.0\r\n\r\n", started + std::chrono::seconds(2));
            require(!report.allowed && report.suppressedToReport == 4, "SIP flood guard did not aggregate suppressed requests");
        }

        require(!SipMessage::parse("MESSAGE sip:platform@example.test SIP/2.0\r\n"
                                   "Content-Length: 0\r\n\r\n<Notify>smuggled</Notify>")
                     .has_value(),
                "SIP parser accepted bytes beyond the declared Content-Length");
        require(!SipMessage::parse("MESSAGE sip:platform@example.test SIP/2.0\r\n"
                                   "Content-Length: 8\r\n\r\nshort")
                     .has_value(),
                "SIP parser accepted a truncated declared body");
        require(!SipMessage::parse("SIP/2.0 200x OK\r\nContent-Length: 0\r\n\r\n").has_value(), "SIP parser accepted a non-decimal response status");
        require(!SipMessage::parse("REGISTER sip:platform@example.test HTTP/1.1\r\nContent-Length: 0\r\n\r\n").has_value(), "SIP parser accepted a request with a non-SIP protocol version");
        verifySipRequestHeadBytes();
        std::vector<DeviceChange> changes;
        DeviceRegistry observed(
            [&changes](const Device&, DeviceChange change) {
                changes.push_back(change);
            }
        );
        observed.upsertRegistration("device", "127.0.0.1:5060");
        observed.updateCatalog("device", {});
        observed.markOffline("device");
        require(changes == std::vector<DeviceChange>{
                               DeviceChange::Status,
                               DeviceChange::Catalog,
                               DeviceChange::Status,
                           },
                "GB28181 projection changes were not classified");

        DeviceRegistry generationRegistry;
        generationRegistry.upsertRegistration("generation-device", "127.0.0.1:5060");
        const auto firstGeneration =
            generationRegistry.findDevice("generation-device");
        require(firstGeneration.has_value() && firstGeneration->sessionGeneration != 0, "GB28181 registration did not allocate a session generation");
        generationRegistry.upsertRegistration("generation-device", "127.0.0.1:5060");
        const auto samePeerGeneration =
            generationRegistry.findDevice("generation-device");
        require(samePeerGeneration.has_value() && samePeerGeneration->sessionGeneration == firstGeneration->sessionGeneration, "GB28181 same-peer REGISTER changed the session generation");
        generationRegistry.upsertRegistration("generation-device", "127.0.0.1:5061");
        const auto changedPeerGeneration =
            generationRegistry.findDevice("generation-device");
        require(changedPeerGeneration.has_value() && changedPeerGeneration->sessionGeneration != samePeerGeneration->sessionGeneration, "GB28181 different-peer REGISTER did not change the session generation");

        pool.start();
        auto loop = pool.loop(0);
        require(loop.valid(), "Ruvia SIP worker did not start");

        SipConfig sip;
        sip.domain = "3402000000";
        sip.id = "34020000002000000001";
        sip.host = "127.0.0.1";
        sip.publicIp = "127.0.0.1";
        sip.port = reserveLocalPort();
        sip.password = "test";
        sip.transport = "both";
        sip.registrationTimeoutSeconds = 30;
        sip.viewerLeaseTimeoutSeconds = 1;
        sip.logging = false;

        MediaConfig media;
        media.rtpPublicIp = "127.0.0.1";
        media.playTokenSecret = "gb28181-sip-test-secret";
        media.logLevel = 4;
        media.httpPort = 0;
        media.rtspPort = 0;
        media.rtmpPort = 0;
        // ZLMediaKit binds RTC UDP on a random port and then TCP on the same
        // number, which can already be taken; use a port free for both.
        media.rtcPort = reserveLocalPort();
        media.srtPort = 0;
        media.rtpPortRangeStart = 0;
        media.rtpPortRangeEnd = 0;
        devices = std::make_unique<DeviceRegistry>();
        zlm = std::make_shared<ZlmSdk>(media);
        zlm->start();
        server = std::make_shared<SipServer>(
            uuidState,
            sip,
            media,
            *devices,
            zlm,
            loop,
            [&viewerCounts, &viewerCountsMutex, &viewerCountsChanged](const std::string& stream, unsigned int count) {
                std::lock_guard lock(viewerCountsMutex);
                viewerCounts.emplace_back(stream, count);
                viewerCountsChanged.notify_all();
            }
        );
        server->setStreamClosedHandler(
            [&closedStreams, &closedStreamsMutex, &closedStreamsChanged](
                const std::string& streamId
            ) {
                std::lock_guard lock(closedStreamsMutex);
                closedStreams.push_back(streamId);
                closedStreamsChanged.notify_all();
            }
        );
        server->start();

        asio::io_context clientContext;
        const asio::ip::udp::endpoint serverUdp(asio::ip::address_v4::loopback(), sip.port);
        asio::ip::udp::socket udp(clientContext, asio::ip::udp::endpoint(asio::ip::udp::v4(), 0));
        const auto udpRequest = sipOptions(udp.local_endpoint().port(), "UDP");
        udp.send_to(asio::buffer(udpRequest), serverUdp);
        udp.non_blocking(true);
        asio::ip::udp::endpoint udpRemote;
        const auto udpResponse =
            receiveUntil([&udp, &udpRemote](auto& buffer, std::error_code& error) {
                return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
            });
        require(udpResponse.starts_with("SIP/2.0 405 Method Not Allowed"), "Ruvia SIP worker did not answer UDP");

        const auto registerChallenge = sipRegister(udp.local_endpoint().port(), 2);
        udp.send_to(asio::buffer(registerChallenge), serverUdp);
        const auto challengeResponse =
            receiveUntil([&udp, &udpRemote](auto& buffer, std::error_code& error) {
                return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
            });
        require(challengeResponse.starts_with("SIP/2.0 401 Unauthorized"), "REGISTER without digest was accepted");
        const auto nonce = challengeNonce(challengeResponse);
        require(!nonce.empty(), "REGISTER challenge did not include a nonce");

        const auto authorizedRegister =
            sipRegister(udp.local_endpoint().port(), 3, nonce);
        udp.send_to(asio::buffer(authorizedRegister), serverUdp);
        const auto registerResponse =
            receiveUntil([&udp, &udpRemote](auto& buffer, std::error_code& error) {
                return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
            });
        require(registerResponse.starts_with("SIP/2.0 200 OK"), "valid REGISTER digest was rejected");

        verifySipRegistrationGeneration(sip, *devices, clientContext);

        udp.send_to(asio::buffer(authorizedRegister), serverUdp);
        const auto replayResponse =
            receiveUntil([&udp, &udpRemote](auto& buffer, std::error_code& error) {
                return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
            });
        require(replayResponse.starts_with("SIP/2.0 401 Unauthorized"), "replayed REGISTER digest was accepted");

        constexpr std::string_view spoofedDevice{ "34020000001320009999" };
        const auto spoofedRegister =
            sipRegister(udp.local_endpoint().port(), 4, nonce, spoofedDevice, "34020000001320000001", "00000002", "spoof-cnonce");
        udp.send_to(asio::buffer(spoofedRegister), serverUdp);
        const auto spoofedRegisterResponse =
            receiveUntil([&udp, &udpRemote](auto& buffer, std::error_code& error) {
                return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
            });
        require(spoofedRegisterResponse.starts_with("SIP/2.0 403 Forbidden"), "REGISTER accepted a Digest username that did not match From");
        require(!devices->findDevice(std::string(spoofedDevice)).has_value(), "REGISTER created a device from an unauthenticated From identity");

        asio::ip::udp::socket spoofUdp(clientContext, asio::ip::udp::endpoint(asio::ip::udp::v4(), 0));
        spoofUdp.non_blocking(true);
        asio::ip::udp::endpoint spoofRemote;
        const auto spoofKeepalive = sipKeepalive(spoofUdp.local_endpoint().port());
        spoofUdp.send_to(asio::buffer(spoofKeepalive), serverUdp);
        const auto spoofKeepaliveAck =
            receiveSipMatching(
                [&spoofUdp, &spoofRemote](auto& buffer, std::error_code& error) {
                    return spoofUdp.receive_from(asio::buffer(buffer), spoofRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.statusCode == 403;
                },
                std::chrono::seconds(3)
            );
        require(spoofKeepaliveAck.has_value(), "GB28181 spoofed Keepalive was not rejected");
        const auto afterSpoofKeepalive = devices->findDevice("34020000001320000001");
        require(afterSpoofKeepalive.has_value() && afterSpoofKeepalive->remoteAddress == "127.0.0.1:" + std::to_string(udp.local_endpoint().port()), "GB28181 Keepalive from an unexpected remote hijacked registration");

        const auto keepalive = sipKeepalive(udp.local_endpoint().port());
        udp.send_to(asio::buffer(keepalive), serverUdp);
        const auto keepaliveResponse =
            receiveUntil([&udp, &udpRemote](auto& buffer, std::error_code& error) {
                return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
            });
        require(keepaliveResponse.starts_with("SIP/2.0 200 OK"), "GB28181 keepalive was not acknowledged");

        udp.send_to(asio::buffer(catalogMessage("1x")), serverUdp);
        const auto badCatalogAck =
            receiveSipMatching(
                [&udp, &udpRemote](auto& buffer, std::error_code& error) {
                    return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.statusCode == 200;
                },
                std::chrono::seconds(3)
            );
        require(badCatalogAck.has_value(), "GB28181 malformed Catalog was not acknowledged");
        auto afterBadCatalog = devices->findDevice("34020000001320000001");
        require(afterBadCatalog.has_value() && afterBadCatalog->channels.size() == 1 && afterBadCatalog->channels.front().ptzType == -1, "GB28181 Catalog accepted a non-decimal PTZType");

        udp.send_to(asio::buffer(catalogMessage("2")), serverUdp);
        const auto goodCatalogAck =
            receiveSipMatching(
                [&udp, &udpRemote](auto& buffer, std::error_code& error) {
                    return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.statusCode == 200;
                },
                std::chrono::seconds(3)
            );
        require(goodCatalogAck.has_value(), "GB28181 valid Catalog was not acknowledged");
        auto afterGoodCatalog = devices->findDevice("34020000001320000001");
        require(afterGoodCatalog.has_value() && afterGoodCatalog->channels.size() == 1 && afterGoodCatalog->channels.front().ptzType == 2, "GB28181 valid Catalog PTZType was not stored");

        verifyAcknowledgementBarrier(server, sip.port, udp);

        asio::ip::tcp::socket tcp(clientContext);
        tcp.connect(
            asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), sip.port)
        );
        const auto tcpRequest = sipOptions(tcp.local_endpoint().port(), "TCP");
        asio::write(tcp, asio::buffer(tcpRequest));
        tcp.non_blocking(true);
        const auto tcpResponse =
            receiveUntil([&tcp](auto& buffer, std::error_code& error) {
                return tcp.read_some(asio::buffer(buffer), error);
            });
        require(tcpResponse.starts_with("SIP/2.0 405 Method Not Allowed"), "Ruvia SIP worker did not answer TCP");

        asio::ip::tcp::socket oversizedHeaderTcp(clientContext);
        oversizedHeaderTcp.connect(
            asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), sip.port)
        );
        const std::string oversizedHeader(72 * 1024, 'A');
        asio::write(oversizedHeaderTcp, asio::buffer(oversizedHeader));
        require(socketClosesWithin(oversizedHeaderTcp, std::chrono::seconds(1)), "SIP TCP framing kept an oversized unterminated header open");

        asio::ip::tcp::socket invalidLengthTcp(clientContext);
        invalidLengthTcp.connect(
            asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), sip.port)
        );
        const auto invalidLengthRequest =
            "OPTIONS sip:34020000002000000001@127.0.0.1 SIP/2.0\r\n"
            "Via: SIP/2.0/TCP 127.0.0.1:" +
            std::to_string(invalidLengthTcp.local_endpoint().port()) +
            ";branch=z9hG4bK-invalid-length\r\n"
            "From: <sip:34020000001320000001@3402000000>;tag=test\r\n"
            "To: <sip:34020000002000000001@3402000000>\r\n"
            "Call-ID: gb28181-invalid-length-test\r\n"
            "CSeq: 2 OPTIONS\r\n"
            "Content-Length: -1\r\n\r\n";
        asio::write(invalidLengthTcp, asio::buffer(invalidLengthRequest));
        require(socketClosesWithin(invalidLengthTcp, std::chrono::seconds(1)), "SIP TCP framing kept an invalid Content-Length connection open");

        devices->updateCatalog("34020000001320000001", { Channel{ .id = "34020000001320000001", .name = "Main channel", .manufacturer = "test", .online = true, .ptzType = 1 } });
        const auto preview =
            server->startPreview("34020000001320000001", "34020000001320000001");
        require(preview.has_value(), "GB28181 preview INVITE was not started");
        const auto invite =
            receiveSipMatching(
                [&udp, &udpRemote](auto& buffer, std::error_code& error) {
                    return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.method == "INVITE";
                },
                std::chrono::seconds(3)
            );
        require(invite.has_value(), "GB28181 preview INVITE was not received");

        const auto notInviteOk = inviteOkWithCSeqMethod(*invite, "NOTINVITE");
        udp.send_to(asio::buffer(notInviteOk), serverUdp);
        const auto badAck =
            receiveSipMatching(
                [&udp, &udpRemote](auto& buffer, std::error_code& error) {
                    return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.method == "ACK";
                },
                std::chrono::milliseconds(300)
            );
        require(!badAck.has_value(), "SIP accepted a 200 OK whose CSeq method was not INVITE");
        require(server->stopPreview(preview->sessionId).has_value(), "GB28181 pending preview session was not stopped");
        {
            std::unique_lock lock(closedStreamsMutex);
            require(closedStreamsChanged.wait_for(lock, std::chrono::seconds(3), [&closedStreams] {
                return !closedStreams.empty();
            }),
                    "GB28181 stream close callback was not delivered for the removed session");
            require(closedStreams == std::vector<std::string>{ preview->streamId }, "GB28181 stream close callback did not identify the removed media stream");
            closedStreams.clear();
        }
        {
            std::lock_guard lock(viewerCountsMutex);
            require(viewerCounts.size() >= 2 && viewerCounts[viewerCounts.size() - 2] == std::pair{ preview->streamId, 1U } && viewerCounts.back() == std::pair{ preview->streamId, 0U }, "GB28181 pending preview viewer count was not released");
            viewerCounts.clear();
        }
        const auto sharedPreview =
            server->startPreview("34020000001320000001", "34020000001320000001");
        require(sharedPreview.has_value(), "GB28181 shared preview INVITE was not started");
        const auto sharedInvite =
            receiveSipMatching(
                [&udp, &udpRemote](auto& buffer, std::error_code& error) {
                    return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.method == "INVITE";
                },
                std::chrono::seconds(3)
            );
        require(sharedInvite.has_value(), "GB28181 shared preview INVITE was not received");
        const auto sharedInviteOk = inviteOkWithCSeqMethod(*sharedInvite, "INVITE");
        udp.send_to(asio::buffer(sharedInviteOk), serverUdp);
        const auto sharedAck =
            receiveSipMatching(
                [&udp, &udpRemote](auto& buffer, std::error_code& error) {
                    return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.method == "ACK";
                },
                std::chrono::seconds(3)
            );
        require(sharedAck.has_value(), "GB28181 shared preview INVITE was not acknowledged");
        server->markStreamOnline(sharedPreview->streamId, true);

        const auto secondViewer =
            server->startPreview("34020000001320000001", "34020000001320000001");
        require(secondViewer.has_value() && secondViewer->streamId == sharedPreview->streamId && secondViewer->sessionId != sharedPreview->sessionId, "GB28181 concurrent preview did not reuse the camera stream");
        {
            std::unique_lock lock(viewerCountsMutex);
            require(viewerCountsChanged.wait_for(lock, std::chrono::seconds(3), [&viewerCounts] {
                return viewerCounts.size() >= 3;
            }),
                    "GB28181 expired viewer lease did not reduce the viewer count");
            require(viewerCounts.back() == std::pair{ sharedPreview->streamId, 1U }, "GB28181 viewer lease expiry did not release exactly one viewer");
        }
        require(server->renewPreview(secondViewer->sessionId), "GB28181 active viewer lease could not be renewed");
        {
            std::lock_guard lock(closedStreamsMutex);
            require(closedStreams.empty(), "GB28181 stream close callback fired while another viewer was active");
        }
        const auto prematureBye =
            receiveSipMatching(
                [&udp, &udpRemote](auto& buffer, std::error_code& error) {
                    return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.method == "BYE";
                },
                std::chrono::milliseconds(300)
            );
        require(!prematureBye.has_value(), "GB28181 stream closed while another viewer was still active");
        require(server->stopPreview(secondViewer->sessionId).has_value(), "GB28181 second preview viewer was not released");
        const auto lastViewerBye =
            receiveSipMatching(
                [&udp, &udpRemote](auto& buffer, std::error_code& error) {
                    return udp.receive_from(asio::buffer(buffer), udpRemote, 0, error);
                },
                [](const SipMessage& message) {
                    return message.method == "BYE";
                },
                std::chrono::seconds(3)
            );
        require(lastViewerBye.has_value(), "GB28181 stream was not closed after its last viewer disappeared");
        {
            std::unique_lock lock(closedStreamsMutex);
            require(closedStreamsChanged.wait_for(lock, std::chrono::seconds(3), [&closedStreams] {
                return !closedStreams.empty();
            }),
                    "GB28181 stream close callback was not delivered after the last viewer disappeared");
            require(closedStreams == std::vector<std::string>{ sharedPreview->streamId }, "GB28181 stream close callback fired more than once for a multi-viewer stream");
        }
        require(!server->stopPreview(sharedPreview->sessionId).has_value(), "GB28181 preview session remained after its last viewer left");
        {
            std::lock_guard lock(viewerCountsMutex);
            require(viewerCounts == std::vector<std::pair<std::string, unsigned int>>{
                                        { sharedPreview->streamId, 1U },
                                        { sharedPreview->streamId, 2U },
                                        { sharedPreview->streamId, 1U },
                                        { sharedPreview->streamId, 0U },
                                    },
                    "GB28181 shared preview viewer count did not follow 1-2-1-0");
        }

        devices->upsertRegistration(
            "bad-port-device",
            "127.0.0.1:" + std::to_string(udp.local_endpoint().port()) + "x"
        );
        require(!server->queryCatalog("bad-port-device"), "GB28181 accepted a remote address with a non-decimal port");

        {
            DeviceRegistry ipv6Devices;
            const auto ipv6Address = asio::ip::address_v6::loopback();
            SipConfig ipv6Sip = sip;
            ipv6Sip.host = "::1";
            ipv6Sip.transport = "udp";
            ipv6Sip.port = reserveLocalUdpPort(ipv6Address);
            auto ipv6Server = std::make_shared<SipServer>(
                uuidState,
                ipv6Sip,
                media,
                ipv6Devices,
                zlm,
                loop
            );
            ipv6Server->start();

            asio::ip::udp::socket ipv6Client(
                clientContext,
                asio::ip::udp::endpoint(ipv6Address, 0)
            );
            ipv6Client.non_blocking(true);
            asio::ip::udp::endpoint ipv6Remote;
            ipv6Devices.upsertRegistration(
                "ipv6-device",
                "[::1]:" + std::to_string(ipv6Client.local_endpoint().port())
            );
            require(ipv6Server->queryCatalog("ipv6-device"), "GB28181 IPv6 UDP route was not resolved");
            const auto ipv6Packet = receiveUntil(
                [&ipv6Client, &ipv6Remote](auto& buffer, std::error_code& error) {
                    return ipv6Client.receive_from(asio::buffer(buffer), ipv6Remote, 0, error);
                },
                std::chrono::seconds(3)
            );
            require(!ipv6Packet.empty(), "GB28181 IPv6 UDP catalog datagram was not delivered");
            const auto ipv6Catalog = SipMessage::parse(ipv6Packet);
            require(ipv6Catalog.has_value(), "GB28181 IPv6 UDP catalog datagram could not be parsed");
            require(ipv6Catalog->method == "MESSAGE" && xmlValue(ipv6Catalog->body, "CmdType") == "Catalog", "GB28181 IPv6 UDP datagram is not a catalog query");
            ipv6Server->stop();
        }

        verifyDualUdpWorkerOwnership(sip, media, zlm);
        verifyKeepaliveExpiry(uuidState, server, sip, media, *devices, zlm, loop, clientContext);
        zlm->stop();
        require(service::common::canonicalUtcTimestamp("2026-07-29 08:40:45+00") == "2026-07-29T08:40:45Z", "offset timestamp was not normalized");
        require(service::common::canonicalUtcTimestamp("2026-07-29T08:40:45", 480) == "2026-07-29T00:40:45Z", "GB28181 local timestamp was not normalized with device timezone");
        require(service::common::canonicalUtcTimestamp("Tue Jul 28 18:24:54 CST 2026", 480).empty(), "untyped locale timestamp was silently accepted");
        require(
            service::common::canonicalUtcTimestamp("2026-02-31T08:40:45Z").empty(),
            "invalid calendar date was silently normalized"
        );
        pool.stop();
        pool.join();
        server.reset();
        std::cout << "gb28181 Ruvia SIP worker tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        try {
            if (server) {
                server->stop();
            }
            server.reset();
            if (zlm) {
                zlm->stop();
            }
            zlm.reset();
            devices.reset();
            pool.stop();
            pool.join();
        } catch (...) {
        }
        std::cerr << error.what() << '\n';
        return 1;
    }
}
