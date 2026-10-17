#include "service/features/gb28181/sip/sip.transport.h"

#include <algorithm>
#include <array>
#include <asio/write.hpp>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstring>
#include <future>
#include <iomanip>
#include <iostream>
#include <locale>
#include <pugixml.hpp>
#include <random>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "service/features/gb28181/sip/sip.protocol.h"
#include "service/middleware/log.h"

#if !defined(_WIN32)
#include <sys/socket.h>

#include "service/utils/uuid.h"
#endif

namespace service::gb28181::sip {

namespace {

constexpr std::size_t kMaxSipHeaderBytes = 64 * 1024;
constexpr std::size_t kMaxSipBodyBytes = 8 * 1024 * 1024;

struct RemoteEndpoint {
    std::string host;
    uint16_t port{ 0 };
};

template <typename Endpoint>
std::string endpointKey(const Endpoint& endpoint) {
    const auto address = endpoint.address().to_string();
    return endpoint.address().is_v6()
        ? "[" + address + "]:" + std::to_string(endpoint.port())
        : address + ":" + std::to_string(endpoint.port());
}

std::string extractUserFromSipUri(const std::string& value) {
    const auto sip = value.find("sip:");
    if (sip == std::string::npos) {
        return {};
    }
    const auto begin = sip + 4;
    const auto end = value.find_first_of("@;>", begin);
    if (end == std::string::npos) {
        return value.substr(begin);
    }
    return value.substr(begin, end - begin);
}

std::string extractTag(const std::string& value) {
    const auto tag = value.find("tag=");
    if (tag == std::string::npos) {
        return {};
    }
    const auto begin = tag + 4;
    const auto end = value.find_first_of(";> \t\r\n", begin);
    if (end == std::string::npos) {
        return value.substr(begin);
    }
    return value.substr(begin, end - begin);
}

std::optional<int> registrationExpires(const SipMessage& message) {
    auto value = message.header("Expires");
    if (value.empty()) {
        const auto contact = message.header("Contact");
        auto marker = contact;
        std::transform(marker.begin(), marker.end(), marker.begin(), [](unsigned char character) {
            return static_cast<char>(std::tolower(character));
        });
        const auto position = marker.find("expires=");
        if (position != std::string::npos) {
            const auto begin = position + 8;
            const auto end = marker.find_first_of(";> \t\r\n", begin);
            value = marker.substr(begin, end - begin);
        }
    }
    if (value.empty()) {
        return std::nullopt;
    }
    try {
        std::size_t consumed{};
        const auto result = std::stoi(value, &consumed);
        if (consumed != value.size() || result < 0) {
            return std::nullopt;
        }
        return result;
    } catch (...) {
        return std::nullopt;
    }
}

struct CSeqValue {
    unsigned int number{ 0 };
    std::string method;
};

std::optional<CSeqValue> parseCSeq(const std::string& value) {
    std::istringstream input(value);
    std::string numberText;
    std::string method;
    std::string extra;
    if (!(input >> numberText >> method) || (input >> extra)) {
        return std::nullopt;
    }
    unsigned int number = 0;
    const auto [end, error] =
        std::from_chars(numberText.data(), numberText.data() + numberText.size(), number);
    if (numberText.empty() || error != std::errc{} ||
        end != numberText.data() + numberText.size()) {
        return std::nullopt;
    }
    return CSeqValue{ .number = number, .method = std::move(method) };
}

std::string peerToString(const SipServer::SipPeer& peer) {
    const auto host = peer.address.find(':') != std::string::npos &&
            !peer.address.starts_with('[')
        ? "[" + peer.address + "]"
        : peer.address;
    return host + ":" + std::to_string(peer.port);
}

const char* transportName(SipServer::SipTransport transport) {
    return transport == SipServer::SipTransport::Tcp ? "TCP" : "UDP";
}

bool samePeer(const SipServer::SipPeer& expected, const SipServer::SipPeer& actual) {
    if (expected.transport != actual.transport || expected.address != actual.address ||
        expected.port != actual.port) {
        return false;
    }
    if (expected.transport == SipServer::SipTransport::Tcp && expected.tcp && actual.tcp &&
        expected.tcp != actual.tcp) {
        return false;
    }
    return true;
}

bool registeredRemoteMatches(const device::DeviceRegistry& deviceRegistry, const std::string& deviceId, const SipServer::SipPeer& remote) {
    if (deviceId.empty()) {
        return false;
    }
    const auto device = deviceRegistry.findDevice(deviceId);
    return device.has_value() && device->remoteAddress == peerToString(remote);
}

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string trim(std::string value) {
    const auto notSpace = [](unsigned char c) {
        return !std::isspace(c);
    };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), notSpace));
    value.erase(std::find_if(value.rbegin(), value.rend(), notSpace).base(), value.end());
    return value;
}

std::optional<unsigned int> parseUnsigned(std::string value) {
    value = trim(std::move(value));
    unsigned int parsed = 0;
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (value.empty() || error != std::errc{} || end != value.data() + value.size()) {
        return std::nullopt;
    }
    return parsed;
}

std::optional<std::size_t> contentLengthOf(const std::string& headerText) {
    std::istringstream lines(headerText);
    std::string line;
    std::optional<std::size_t> contentLength;
    while (std::getline(lines, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        const auto colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        const auto name = lower(trim(line.substr(0, colon)));
        if (name == "content-length" || name == "l") {
            const auto value = trim(line.substr(colon + 1));
            std::size_t parsed{};
            const auto [end, error] =
                std::from_chars(value.data(), value.data() + value.size(), parsed);
            if (value.empty() || error != std::errc{} ||
                end != value.data() + value.size() ||
                (contentLength && *contentLength != parsed)) {
                return std::nullopt;
            }
            contentLength = parsed;
        }
    }
    return contentLength.value_or(0);
}

std::string compactForLog(std::string value, std::size_t limit = 512) {
    const auto truncated = value.size() > limit;
    if (truncated) {
        value.resize(limit);
    }

    std::string output;
    output.reserve(value.size());
    for (const auto ch : value) {
        if (ch == '\r') {
            output += "\\r";
        } else if (ch == '\n') {
            output += "\\n";
        } else if (ch == '\t') {
            output += "\\t";
        } else {
            output.push_back(ch);
        }
    }
    if (truncated) {
        output += "...";
    }
    return output;
}

std::optional<RemoteEndpoint> parseRemoteEndpoint(const std::string& remoteAddress) {
    const auto colon = remoteAddress.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= remoteAddress.size()) {
        return std::nullopt;
    }

    const auto port = parseUnsigned(remoteAddress.substr(colon + 1));
    if (!port || *port == 0 || *port > 65535) {
        return std::nullopt;
    }
    auto host = remoteAddress.substr(0, colon);
    if (host.front() == '[') {
        if (host.size() < 3 || host.back() != ']') {
            return std::nullopt;
        }
        host = host.substr(1, host.size() - 2);
    } else if (host.find(':') != std::string::npos) {
        return std::nullopt;
    }
    return RemoteEndpoint{ std::move(host), static_cast<uint16_t>(*port) };
}

std::string routeUnavailableReason(const std::optional<device::DeviceRouteSnapshot>& route) {
    if (!route.has_value()) {
        return "device_not_registered";
    }
    if (!route->online) {
        return "device_offline";
    }
    if (route->remoteAddress.empty()) {
        return "remote_address_empty";
    }
    return {};
}

void logSipPacket(
    const char* direction,
    const SipMessage& message,
    const SipServer::SipPeer& remote,
    std::size_t bytes,
    bool includeBody
) {
    LOG_DEBUG << "[GB28181][SIP][" << direction << "] " << transportName(remote.transport)
              << " " << peerToString(remote)
              << ", start_line=\"" << message.startLine << "\""
              << ", call_id=" << message.header("Call-ID")
              << ", cseq=\"" << message.header("CSeq") << "\""
              << ", bytes=" << bytes
              << ", body_bytes=" << message.body.size();

    if (includeBody && !message.body.empty()) {
        LOG_TRACE << "[GB28181][SIP][" << direction << "_BODY] " << transportName(remote.transport)
                  << " " << peerToString(remote)
                  << ", call_id=" << message.header("Call-ID")
                  << ", body=\"" << compactForLog(message.body, 2048) << "\"";
    }
}

void logSipSend(const std::string& packet, const SipServer::SipPeer& remote, bool includeBody) {
    const auto message = SipMessage::parse(packet);
    if (!message.has_value()) {
        LOG_DEBUG << "[GB28181][SIP][TX] " << transportName(remote.transport)
                  << " " << peerToString(remote)
                  << ", bytes=" << packet.size()
                  << ", first_bytes=\"" << compactForLog(packet, 160) << "\"";
        return;
    }
    logSipPacket("TX", *message, remote, packet.size(), includeBody);
}

std::string xmlText(const pugi::xml_node& node, const char* name) {
    return node.child(name).text().as_string();
}

int xmlInt(const pugi::xml_node& node, const char* name, int fallback = -1) {
    auto text = xmlText(node, name);
    if (text.empty()) {
        text = xmlText(node.child("Info"), name);
    }
    text = trim(std::move(text));
    if (text.empty()) {
        return fallback;
    }
    int parsed = 0;
    const auto [end, error] =
        std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (error != std::errc{} || end != text.data() + text.size()) {
        return fallback;
    }
    return parsed;
}

bool statusOnline(const std::string& status) {
    return status == "ON" || status == "ONLINE" || status == "OK";
}

std::string makeToken(const char* prefix) {
    static thread_local std::mt19937_64 rng{ std::random_device{}() };
    std::ostringstream output;
    output << prefix << std::chrono::steady_clock::now().time_since_epoch().count() << rng();
    return output.str();
}

std::string ptzCommand(const std::string& action, uint8_t speed) {
    const auto clamped = static_cast<uint8_t>(std::min<unsigned int>(speed, 255));
    uint8_t command = 0x00;
    uint8_t panSpeed = 0x00;
    uint8_t tiltSpeed = 0x00;
    uint8_t zoomSpeed = 0x00;

    if (action == "left") {
        command = 0x02;
        panSpeed = clamped;
    } else if (action == "right") {
        command = 0x01;
        panSpeed = clamped;
    } else if (action == "up") {
        command = 0x08;
        tiltSpeed = clamped;
    } else if (action == "down") {
        command = 0x04;
        tiltSpeed = clamped;
    } else if (action == "zoomin") {
        command = 0x10;
        zoomSpeed = clamped;
    } else if (action == "zoomout") {
        command = 0x20;
        zoomSpeed = clamped;
    }

    std::array<uint8_t, 8> bytes{ 0xA5, 0x0F, 0x01, command, panSpeed, tiltSpeed, zoomSpeed, 0x00 };
    unsigned int checksum = 0;
    for (std::size_t i = 0; i < bytes.size() - 1; ++i) {
        checksum += bytes[i];
    }
    bytes.back() = static_cast<uint8_t>(checksum & 0xFF);

    std::ostringstream output;
    output << std::uppercase << std::hex << std::setfill('0');
    for (const auto byte : bytes) {
        output << std::setw(2) << static_cast<unsigned int>(byte);
    }
    return output.str();
}

void runOnLoopAndWait(const ruvia::event_loop& loop, std::function<void()> work) {
    // Supervisor-only lifecycle barrier used before HTTP serving starts and
    // after it stops. Request/Worker paths use Runtime::invoke + one_shot.
    if (!loop.valid()) {
        throw std::runtime_error("GB28181 SIP IO loop is not configured");
    }
    if (loop.is_current()) {
        work();
        return;
    }
    auto completion = std::make_shared<std::promise<void>>();
    auto future = completion->get_future();
    const auto posted = loop.post([completion, work = std::move(work)]() mutable {
        try {
            work();
            completion->set_value();
        } catch (...) {
            completion->set_exception(std::current_exception());
        }
    });
    if (!posted.accepted()) {
        throw std::runtime_error("GB28181 SIP worker rejected operation");
    }
    future.get();
}

template <typename Socket>
void enablePortReuse(Socket& socket, std::error_code& error) {
#if defined(SO_REUSEPORT) && !defined(_WIN32)
    int enabled = 1;
    if (::setsockopt(socket.native_handle(), SOL_SOCKET, SO_REUSEPORT, &enabled, sizeof(enabled)) != 0) {
        error = std::error_code(errno, std::generic_category());
        return;
    }
#else
    (void)socket;
#endif
    error.clear();
}

} // namespace

SipServer::SipServer(service::common::UuidV7State& uuidState, SipConfig sipConfig, MediaConfig mediaConfig, device::DeviceRegistry& deviceRegistry, std::shared_ptr<media::ZlmSdk> zlmSdk, ruvia::event_loop ioLoop, ViewerCountObserver viewerCountObserver, media::ZlmSdk::OwnerIndex owner)
    : uuidState_(uuidState),
      sipConfig_(std::move(sipConfig)),
      mediaConfig_(std::move(mediaConfig)),
      deviceRegistry_(deviceRegistry),
      zlmSdk_(std::move(zlmSdk)),
      viewerCountObserver_(std::move(viewerCountObserver)),
      owner_(owner),
      ioLoop_(std::move(ioLoop)) {}

void SipServer::setAcknowledgementHandler(
    std::function<void(std::function<void(bool)>)> handler
) {
    if (running_.load() && ioLoop_.valid() && !ioLoop_.is_current()) {
        runOnLoopAndWait(ioLoop_, [this, handler = std::move(handler)]() mutable {
            acknowledgementHandler_ = std::move(handler);
        });
        return;
    }
    acknowledgementHandler_ = std::move(handler);
}

void SipServer::setPreviewClosedHandler(
    std::function<void(const std::string& sessionId)> handler
) {
    if (running_.load() && ioLoop_.valid() && !ioLoop_.is_current()) {
        runOnLoopAndWait(ioLoop_, [this, handler = std::move(handler)]() mutable {
            previewClosedHandler_ = std::move(handler);
        });
        return;
    }
    previewClosedHandler_ = std::move(handler);
}

void SipServer::setStreamClosedHandler(
    std::function<void(const std::string& streamId)> handler
) {
    if (running_.load() && ioLoop_.valid() && !ioLoop_.is_current()) {
        runOnLoopAndWait(ioLoop_, [this, handler = std::move(handler)]() mutable {
            streamClosedHandler_ = std::move(handler);
        });
        return;
    }
    streamClosedHandler_ = std::move(handler);
}

void SipServer::invalidateDevice(const std::string& deviceId, std::string reason) {
    if (deviceId.empty()) {
        return;
    }

    if (!ioLoop_.valid()) {
        return;
    }

    auto operation = [weak = weak_from_this(), deviceId, reason = std::move(reason)]() mutable {
        const auto self = weak.lock();
        if (!self || !self->running_.load()) {
            return;
        }
        self->invalidateDeviceInLoop(deviceId, std::move(reason), true);
    };
    if (ioLoop_.is_current()) {
        operation();
        return;
    }
    const auto posted = ioLoop_.post(std::move(operation));
    if (!posted.accepted()) {
        LOG_WARN << "[GB28181][SIP] Device invalidation rejected by Ruvia worker, device="
                 << deviceId;
    }
}

SipServer::~SipServer() {
    if (!running_.load()) {
        return;
    }
    if (ioLoop_.valid() && ioLoop_.is_current()) {
        running_.store(false);
        stopInLoop();
    } else {
        LOG_WARN << "[GB28181][SIP] SipServer destroyed while running; call stop() before destruction";
    }
}

void SipServer::start() {
    if (weak_from_this().expired()) {
        throw std::logic_error(
            "SipServer must be owned by std::shared_ptr before start()"
        );
    }
    if (running_.exchange(true)) {
        LOG_DEBUG << "[GB28181][SIP] Start skipped: server already running";
        return;
    }

    LOG_DEBUG << "[GB28181][SIP] Starting, listen=" << sipConfig_.host << ":" << sipConfig_.port
              << ", domain=" << sipConfig_.domain
              << ", sip_id=" << sipConfig_.id
              << ", public_ip=" << sipConfig_.publicIp
              << ", media_rtp_ip=" << mediaConfig_.rtpPublicIp
              << ", rtp_port_range=" << mediaConfig_.rtpPortRangeStart << "-" << mediaConfig_.rtpPortRangeEnd
              << ", zlm_media_workers=" << mediaConfig_.workerThreads;

    if (!ioLoop_.valid()) {
        running_.store(false);
        throw std::runtime_error("GB28181 SIP IO loop is not configured");
    }
    const auto transport = lower(sipConfig_.transport);
    if (transport != "udp" && transport != "tcp" && transport != "both") {
        running_.store(false);
        throw std::runtime_error(
            "GB28181 SIP transport must be udp, tcp or both"
        );
    }
    if (sipConfig_.registrationTimeoutSeconds <= 0 ||
        sipConfig_.inviteTimeoutSeconds <= 0 ||
        sipConfig_.viewerLeaseTimeoutSeconds <= 0 ||
        sipConfig_.nonceTtlSeconds <= 0) {
        running_.store(false);
        throw std::runtime_error("GB28181 SIP timeouts must be positive");
    }
    auto startWork = [this]() {
        try {
            startInLoop();
        } catch (...) {
            // startInLoop may have bound one transport before another
            // transport failed (for example TCP after UDP).  Clear every
            // partially-created socket and timer while still on the owning
            // event_loop so a failed Collector initialization cannot leak a
            // listener into the next worker incarnation.
            stopInLoop();
            running_.store(false);
            throw;
        }
    };
    runOnLoopAndWait(ioLoop_, std::move(startWork));
}

void SipServer::stop() {
    if (!running_.load()) {
        LOG_DEBUG << "[GB28181][SIP] Stop skipped: server is not running";
        return;
    }

    if (!ioLoop_.valid()) {
        LOG_DEBUG << "[GB28181][SIP] Stop skipped: IO loop is not available";
        return;
    }
    LOG_DEBUG << "[GB28181][SIP] Stopping";
    runOnLoopAndWait(ioLoop_, [this]() {
        stopInLoop();
        running_.store(false);
    });
}

void SipServer::startInLoop() {
    if (!ioLoop_.valid() || !ioLoop_.is_current()) {
        throw std::runtime_error("GB28181 SIP IO loop is not available");
    }

    std::error_code error;
    const auto address = asio::ip::make_address(sipConfig_.host, error);
    if (error) {
        throw std::runtime_error("Invalid GB28181 SIP listen address: " + error.message());
    }

    const auto transport = lower(sipConfig_.transport);
    if (transport == "udp" || transport == "both") {
        const asio::ip::udp::endpoint endpoint(address, sipConfig_.port);
        udpSocket_ = std::make_unique<asio::ip::udp::socket>(ioLoop_.io_context());
        udpSocket_->open(endpoint.protocol(), error);
        if (error) {
            throw std::runtime_error("Could not open GB28181 UDP socket: " + error.message());
        }
        udpSocket_->set_option(asio::socket_base::reuse_address(true), error);
        if (error) {
            throw std::runtime_error("Could not configure GB28181 UDP socket: " + error.message());
        }
        enablePortReuse(*udpSocket_, error);
        if (error) {
            throw std::runtime_error("Could not configure GB28181 UDP port reuse: " + error.message());
        }
        udpSocket_->bind(endpoint, error);
        if (error) {
            throw std::runtime_error("Could not bind GB28181 UDP socket: " + error.message());
        }
        receiveUdp();
        LOG_DEBUG << "[GB28181][SIP] UDP listening on " << sipConfig_.host << ":"
                  << sipConfig_.port << " via Ruvia core worker " << ioLoop_.id();
    }

    if (transport == "tcp" || transport == "both") {
        const asio::ip::tcp::endpoint endpoint(address, sipConfig_.port);
        tcpAcceptor_ =
            std::make_unique<asio::ip::tcp::acceptor>(ioLoop_.io_context());
        tcpAcceptor_->open(endpoint.protocol(), error);
        if (error) {
            throw std::runtime_error("Could not open GB28181 TCP acceptor: " + error.message());
        }
        tcpAcceptor_->set_option(asio::socket_base::reuse_address(true), error);
        if (error) {
            throw std::runtime_error("Could not configure GB28181 TCP acceptor: " + error.message());
        }
        enablePortReuse(*tcpAcceptor_, error);
        if (error) {
            throw std::runtime_error("Could not configure GB28181 TCP port reuse: " + error.message());
        }
        tcpAcceptor_->bind(endpoint, error);
        if (error) {
            throw std::runtime_error("Could not bind GB28181 TCP acceptor: " + error.message());
        }
        tcpAcceptor_->listen(asio::socket_base::max_listen_connections, error);
        if (error) {
            throw std::runtime_error("Could not listen on GB28181 TCP acceptor: " + error.message());
        }
        acceptTcp();
        LOG_DEBUG << "[GB28181][SIP] TCP listening on " << sipConfig_.host << ":"
                  << sipConfig_.port << " via Ruvia core worker " << ioLoop_.id();
    }
    deadlineTimer_ =
        std::make_unique<asio::steady_timer>(ioLoop_.io_context());
}

void SipServer::stopInLoop() {
    const auto sessionCount = previewSessions_.size();
    const auto viewerCount = previewViewers_.size();
    std::vector<std::string> sessions;
    sessions.reserve(sessionCount);
    for (const auto& [id, _] : previewSessions_) {
        sessions.push_back(id);
    }
    for (const auto& id : sessions) {
        (void)stopPreview(id);
    }

    for (const auto& device : deviceRegistry_.listDevices()) {
        if (!device.online) {
            continue;
        }
        invalidatedDevices_.insert(device.id);
        deviceRegistry_.markOffline(device.id);
        cancelDeadline(DeadlineKind::Registration, device.id);
    }

    std::error_code error;
    if (deadlineTimer_) {
        deadlineTimer_->cancel();
        deadlineTimer_.reset();
    }
    deadlines_ = {};
    deadlineGenerations_.clear();
    digestNonceCounts_.clear();
    invalidatedDevices_.clear();
    if (udpSocket_) {
        udpSocket_->cancel(error);
        udpSocket_->close(error);
        udpSocket_.reset();
    }
    if (tcpAcceptor_) {
        tcpAcceptor_->cancel(error);
        tcpAcceptor_->close(error);
        tcpAcceptor_.reset();
    }

    std::size_t tcpConnectionCount = 0;
    tcpConnectionCount = tcpConnections_.size();
    for (auto& [_, connection] : tcpConnections_) {
        if (!connection) {
            continue;
        }
        connection->socket.cancel(error);
        connection->socket.shutdown(asio::ip::tcp::socket::shutdown_both, error);
        connection->socket.close(error);
    }
    tcpConnections_.clear();

    LOG_DEBUG << "[GB28181][SIP] Stopped, active_sessions=" << sessionCount
              << ", active_viewers=" << viewerCount
              << ", tcp_connections=" << tcpConnectionCount;
}

void SipServer::notifyViewerCountChanged(const std::string& streamId, unsigned int viewerCount) const noexcept {
    if (!viewerCountObserver_) {
        return;
    }
    try {
        viewerCountObserver_(streamId, viewerCount);
    } catch (const std::exception& error) {
        LOG_WARN << "[GB28181][Preview] Viewer count observer failed, stream_id="
                 << streamId << ", viewers=" << viewerCount
                 << ", error=" << error.what();
    } catch (...) {
        LOG_WARN << "[GB28181][Preview] Viewer count observer failed, stream_id="
                 << streamId << ", viewers=" << viewerCount
                 << ", error=unknown";
    }
}

void SipServer::notifyPreviewClosed(const std::string& sessionId) noexcept {
    if (!previewClosedHandler_) {
        return;
    }
    try {
        previewClosedHandler_(sessionId);
    } catch (const std::exception& error) {
        LOG_WARN << "[GB28181][Preview] Closed observer failed, session="
                 << sessionId << ", error=" << error.what();
    } catch (...) {
        LOG_WARN << "[GB28181][Preview] Closed observer failed, session="
                 << sessionId << ", error=unknown";
    }
}

void SipServer::notifyStreamClosed(const std::string& streamId) noexcept {
    if (!streamClosedHandler_ || streamId.empty()) {
        return;
    }
    try {
        streamClosedHandler_(streamId);
    } catch (const std::exception& error) {
        LOG_WARN << "[GB28181][Preview] Stream closed observer failed, stream_id="
                 << streamId << ", error=" << error.what();
    } catch (...) {
        LOG_WARN << "[GB28181][Preview] Stream closed observer failed, stream_id="
                 << streamId << ", error=unknown";
    }
}

void SipServer::receiveUdp() {
    if (!running_.load() || !udpSocket_ || !udpSocket_->is_open()) {
        return;
    }
    const auto weak = weak_from_this();
    udpSocket_->async_receive_from(
        asio::buffer(udpBuffer_),
        udpRemote_,
        [weak](const std::error_code& error, std::size_t size) {
            const auto self = weak.lock();
            if (!self || !self->running_.load()) {
                return;
            }
            if (error) {
                if (error != asio::error::operation_aborted) {
                    LOG_WARN << "[GB28181][SIP] UDP receive failed: " << error.message();
                }
                self->receiveUdp();
                return;
            }
            const auto floodDecision = self->unsupportedRequestGuard_.inspect(
                std::string_view(self->udpBuffer_.data(), size)
            );
            if (!floodDecision.allowed) {
                if (self->sipConfig_.logging && floodDecision.suppressedToReport > 0) {
                    LOG_WARN << "[GB28181][SIP] Unsupported request flood suppressed, count="
                             << floodDecision.suppressedToReport
                             << ", last_remote=UDP "
                             << self->udpRemote_.address().to_string() << ':'
                             << self->udpRemote_.port();
                }
                self->receiveUdp();
                return;
            }
            SipPeer peer;
            peer.transport = SipTransport::Udp;
            peer.udp = self->udpRemote_;
            peer.address = self->udpRemote_.address().to_string();
            peer.port = self->udpRemote_.port();
            if (self->sipConfig_.logging) {
                LOG_DEBUG << "[GB28181][SIP][UDP_RX] remote=" << peerToString(peer)
                          << ", bytes=" << size;
            }
            self->handlePacket(std::string(self->udpBuffer_.data(), size), peer);
            self->receiveUdp();
        }
    );
}

void SipServer::acceptTcp() {
    if (!running_.load() || !tcpAcceptor_ || !tcpAcceptor_->is_open()) {
        return;
    }
    const auto weak = weak_from_this();
    tcpAcceptor_->async_accept([weak](const std::error_code& error, asio::ip::tcp::socket socket) {
        const auto self = weak.lock();
        if (!self || !self->running_.load()) {
            return;
        }
        if (error) {
            if (error != asio::error::operation_aborted) {
                LOG_WARN << "[GB28181][SIP] TCP accept failed: " << error.message();
            }
            self->acceptTcp();
            return;
        }
        std::error_code endpointError;
        const auto endpoint = socket.remote_endpoint(endpointError);
        if (endpointError) {
            LOG_WARN << "[GB28181][SIP] TCP remote endpoint failed: "
                     << endpointError.message();
            socket.close(endpointError);
            self->acceptTcp();
            return;
        }
        auto connection = std::make_shared<TcpConnection>(std::move(socket));
        connection->key = endpointKey(endpoint);
        connection->address = endpoint.address().to_string();
        connection->port = endpoint.port();
        self->tcpConnections_[connection->key] = connection;
        LOG_DEBUG << "[GB28181][SIP] TCP connected from " << connection->key;
        self->readTcp(connection);
        self->acceptTcp();
    });
}

void SipServer::readTcp(const TcpConnectionPtr& connection) {
    if (!running_.load() || !connection || !connection->socket.is_open()) {
        return;
    }
    const auto weak = weak_from_this();
    connection->socket.async_read_some(
        asio::buffer(connection->readBuffer),
        [weak, connection](const std::error_code& error, std::size_t size) {
            const auto self = weak.lock();
            if (!self) {
                return;
            }
            if (error) {
                if (error != asio::error::operation_aborted &&
                    error != asio::error::eof) {
                    LOG_WARN << "[GB28181][SIP] TCP receive failed from "
                             << connection->key << ": " << error.message();
                }
                self->removeTcpConnection(connection);
                return;
            }
            connection->pending.append(connection->readBuffer.data(), size);
            self->processTcpPending(connection);
            self->readTcp(connection);
        }
    );
}

void SipServer::removeTcpConnection(const TcpConnectionPtr& connection) {
    if (!connection) {
        return;
    }
    std::error_code ignored;
    connection->socket.close(ignored);
    bool removed = false;
    const auto found = tcpConnections_.find(connection->key);
    if (found != tcpConnections_.end() && found->second == connection) {
        tcpConnections_.erase(found);
        removed = true;
    }
    if (!removed) {
        return;
    }

    LOG_DEBUG << "[GB28181][SIP] TCP disconnected from " << connection->key;
    std::vector<std::string> deviceIds;
    for (const auto& device : deviceRegistry_.listDevices()) {
        if (device.online && device.remoteAddress == connection->key) {
            deviceIds.push_back(device.id);
        }
    }
    for (const auto& deviceId : deviceIds) {
        invalidateDeviceInLoop(deviceId, "tcp_connection_closed", false);
    }
}

void SipServer::processTcpPending(const TcpConnectionPtr& connection) {
    if (!connection) {
        return;
    }
    if (sipConfig_.logging) {
        LOG_DEBUG << "[GB28181][SIP][TCP_RX] remote=" << connection->key
                  << ", pending_bytes=" << connection->pending.size();
    }
    while (true) {
        const auto headerEnd = connection->pending.find("\r\n\r\n");
        if (headerEnd == std::string::npos) {
            if (connection->pending.size() > kMaxSipHeaderBytes) {
                LOG_WARN << "[GB28181][SIP] Closing TCP connection with oversized SIP header from "
                         << connection->key << ", pending_bytes=" << connection->pending.size();
                removeTcpConnection(connection);
                return;
            }
            break;
        }
        if (headerEnd > kMaxSipHeaderBytes) {
            LOG_WARN << "[GB28181][SIP] Closing TCP connection with oversized SIP header from "
                     << connection->key << ", header_bytes=" << headerEnd;
            removeTcpConnection(connection);
            return;
        }
        const auto contentLength =
            contentLengthOf(connection->pending.substr(0, headerEnd));
        if (!contentLength.has_value()) {
            LOG_WARN << "[GB28181][SIP] Closing TCP connection with invalid Content-Length from "
                     << connection->key << ", header=\""
                     << compactForLog(connection->pending.substr(0, headerEnd), 300)
                     << "\"";
            removeTcpConnection(connection);
            return;
        }
        if (*contentLength > kMaxSipBodyBytes) {
            LOG_WARN << "[GB28181][SIP] Closing TCP connection with oversized SIP body from "
                     << connection->key << ", content_length=" << *contentLength;
            removeTcpConnection(connection);
            return;
        }
        const auto packetSize = headerEnd + 4 + *contentLength;
        if (connection->pending.size() < packetSize) {
            break;
        }
        auto packet = connection->pending.substr(0, packetSize);
        connection->pending.erase(0, packetSize);
        SipPeer peer;
        peer.transport = SipTransport::Tcp;
        peer.tcp = connection;
        peer.address = connection->address;
        peer.port = connection->port;
        handlePacket(packet, peer);
    }
}

void SipServer::handlePacket(const std::string& packet, const SipPeer& remote) {
    const auto floodDecision = unsupportedRequestGuard_.inspect(packet);
    if (!floodDecision.allowed) {
        if (sipConfig_.logging && floodDecision.suppressedToReport > 0) {
            LOG_WARN << "[GB28181][SIP] Unsupported request flood suppressed, count="
                     << floodDecision.suppressedToReport
                     << ", last_remote=" << transportName(remote.transport) << " "
                     << peerToString(remote);
        }
        return;
    }

    const auto message = SipMessage::parse(packet);
    if (!message.has_value()) {
        LOG_WARN << "[GB28181][SIP] Ignored malformed packet from " << transportName(remote.transport)
                 << " " << peerToString(remote)
                 << ", bytes=" << packet.size()
                 << ", first_bytes=\"" << compactForLog(packet, 200) << "\"";
        return;
    }

    if (sipConfig_.logging) {
        logSipPacket("RX", *message, remote, packet.size(), true);
    }

    if (message->statusCode > 0) {
        handleResponse(*message, remote);
        return;
    }

    if (message->method == "REGISTER") {
        handleRegister(*message, remote);
        return;
    }

    if (message->method == "MESSAGE") {
        handleMessage(*message, remote);
        return;
    }

    if (sipConfig_.logging) {
        LOG_WARN << "[GB28181][SIP] Unsupported method, method=" << message->method
                 << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
                 << ", call_id=" << message->header("Call-ID")
                 << ", cseq=\"" << message->header("CSeq") << "\"";
    }
    sendResponse(*message, remote, 405, "Method Not Allowed");
}

void SipServer::handleResponse(const SipMessage& message, const SipPeer& remote) {
    const auto cseqHeader = message.header("CSeq");
    const auto cseq = parseCSeq(cseqHeader);
    if (message.statusCode == 200 && cseq && cseq->method == "INVITE") {
        handleInviteOk(message, remote);
        return;
    }

    if (message.statusCode >= 300) {
        LOG_WARN << "[GB28181][SIP] Error response, status=" << message.statusCode
                 << ", reason=\"" << message.reasonPhrase << "\""
                 << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
                 << ", call_id=" << message.header("Call-ID")
                 << ", cseq=\"" << cseqHeader << "\""
                 << ", body=\"" << compactForLog(message.body, 300) << "\"";
        if (cseq && cseq->method == "INVITE") {
            const auto callId = message.header("Call-ID");
            std::optional<std::string> sessionToStop;
            for (const auto& [sessionId, session] : previewSessions_) {
                if (session.callId == callId && session.inviteCseq == cseq->number &&
                    samePeer(session.remote, remote)) {
                    sessionToStop = sessionId;
                    break;
                }
            }
            if (sessionToStop) {
                (void)stopPreview(*sessionToStop);
            }
        }
        return;
    }

    LOG_DEBUG << "[GB28181][SIP] Unhandled response, status=" << message.statusCode
              << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
              << ", call_id=" << message.header("Call-ID")
              << ", cseq=\"" << cseqHeader << "\"";
}

void SipServer::handleInviteOk(const SipMessage& message, const SipPeer& remote) {
    const auto callId = message.header("Call-ID");
    if (callId.empty()) {
        LOG_WARN << "[GB28181][Invite] 200 OK without Call-ID from "
                 << transportName(remote.transport) << " " << peerToString(remote);
        return;
    }

    const auto cseq = parseCSeq(message.header("CSeq"));
    if (!cseq || cseq->method != "INVITE") {
        LOG_WARN << "[GB28181][Invite] 200 OK with non-INVITE CSeq ignored, call_id="
                 << callId
                 << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
                 << ", cseq=\"" << message.header("CSeq") << "\"";
        return;
    }

    PreviewSession* session = nullptr;
    for (auto& [_, candidate] : previewSessions_) {
        if (candidate.callId == callId) {
            session = &candidate;
            break;
        }
    }

    if (!session) {
        LOG_WARN << "[GB28181][Invite] 200 OK for unknown Call-ID, call_id=" << callId
                 << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
                 << ", cseq=\"" << message.header("CSeq") << "\"";
        return;
    }
    if (session->inviteCseq != cseq->number || !samePeer(session->remote, remote)) {
        LOG_WARN << "[GB28181][Invite] 200 OK correlation mismatch, session="
                 << session->sessionId
                 << ", call_id=" << callId
                 << ", expected_cseq=" << session->inviteCseq
                 << ", actual_cseq=" << cseq->number
                 << ", expected_remote=" << transportName(session->remote.transport) << " "
                 << peerToString(session->remote)
                 << ", actual_remote=" << transportName(remote.transport) << " "
                 << peerToString(remote);
        return;
    }

    const auto to = message.header("To");
    const auto toTag = extractTag(to);
    if (!message.body.empty()) {
        LOG_DEBUG << "[GB28181][Invite] 200 OK SDP received, session=" << session->sessionId
                  << ", device=" << session->deviceId
                  << ", channel=" << session->channelId
                  << ", stream_id=" << session->streamId
                  << ", call_id=" << callId
                  << ", body=\"" << compactForLog(message.body, 1200) << "\"";
    }
    session->toTag = toTag;
    session->established = true;
    cancelDeadline(DeadlineKind::Invite, session->sessionId);
    for (const auto& [viewerId, streamSessionId] : previewViewers_) {
        if (streamSessionId == session->sessionId) {
            scheduleDeadline(
                DeadlineKind::ViewerLease,
                viewerId,
                std::chrono::seconds(sipConfig_.viewerLeaseTimeoutSeconds)
            );
        }
    }

    const auto publicHost = sipConfig_.publicIp.empty() || sipConfig_.publicIp == "YOUR_PUBLIC_SERVER_IP" ? sipConfig_.host : sipConfig_.publicIp;
    const auto branch = "z9hG4bK-" + makeToken("ack");

    const SipRequestHead ack{
        .method = "ACK",
        .targetUser = session->channelId,
        .targetHost = remote.address,
        .targetPort = remote.port,
        .transport = transportName(remote.transport),
        .platformId = sipConfig_.id,
        .platformHost = publicHost,
        .platformPort = sipConfig_.port,
        .domain = sipConfig_.domain,
        .branch = branch,
        .fromTag = session->fromTag,
        .echoedTo = to,
        .callId = callId,
        .cseq = cseq->number,
        .contact = true,
    };

    sendRequest(ack.serialize({}), remote);
    LOG_DEBUG << "[GB28181][Invite] ACK sent, session=" << session->sessionId
              << ", device=" << session->deviceId
              << ", channel=" << session->channelId
              << ", stream_id=" << session->streamId
              << ", call_id=" << callId
              << ", cseq=" << cseq->number
              << ", to_tag=" << toTag
              << ", remote=" << transportName(remote.transport) << " " << peerToString(remote);
}

void SipServer::handleRegister(const SipMessage& message, const SipPeer& remote) {
    const auto proof = DigestAuth::verifyRegister(
        message,
        sipConfig_.domain,
        sipConfig_.password,
        sipConfig_.password,
        sipConfig_.nonceTtlSeconds
    );
    bool replay = false;
    std::string deviceId;
    if (proof) {
        deviceId = extractUserFromSipUri(message.header("From"));
        if (deviceId.empty()) {
            deviceId = extractUserFromSipUri(message.header("Contact"));
        }
        if (deviceId.empty()) {
            deviceId = proof->username;
        }
        if (proof->username.empty() || deviceId != proof->username) {
            sendResponse(message, remote, 403, "Forbidden");
            LOG_WARN << "[GB28181][Register] Authenticated username mismatch, username="
                     << proof->username
                     << ", declared_device=" << deviceId
                     << ", remote=" << transportName(remote.transport) << " "
                     << peerToString(remote)
                     << ", call_id=" << message.header("Call-ID")
                     << ", cseq=\"" << message.header("CSeq") << "\"";
            return;
        }
        const auto replayKey = proof->nonce + "\n" + proof->username;
        const auto previous = digestNonceCounts_.find(replayKey);
        replay = previous != digestNonceCounts_.end() &&
            proof->nonceCount <= previous->second;
        if (!replay) {
            digestNonceCounts_[replayKey] = proof->nonceCount;
            scheduleDeadline(
                DeadlineKind::AuthNonce,
                replayKey,
                std::chrono::seconds(sipConfig_.nonceTtlSeconds)
            );
        }
    }
    if (!proof || replay) {
        const auto nonce = DigestAuth::makeNonce(sipConfig_.password);
        std::ostringstream auth;
        auth << "WWW-Authenticate: Digest realm=\"" << sipConfig_.domain
             << "\", nonce=\"" << nonce
             << "\", algorithm=MD5, qop=\"auth\"\r\n";
        sendResponse(message, remote, 401, "Unauthorized", auth.str());
        if (sipConfig_.logging) {
            LOG_DEBUG << "[GB28181][Register] Auth challenge sent, device_hint="
                      << extractUserFromSipUri(message.header("From"))
                      << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
                      << ", call_id=" << message.header("Call-ID")
                      << ", cseq=\"" << message.header("CSeq") << "\"";
        }
        return;
    }

    const auto expires = registrationExpires(message);
    if (expires && *expires == 0) {
        invalidateDeviceInLoop(deviceId, "device_unregistered", false);
        acknowledge(message, remote);
        LOG_INFO << "[GB28181][Register] Device unregistered, device=" << deviceId
                 << ", remote=" << transportName(remote.transport) << " "
                 << peerToString(remote);
        return;
    }

    const auto remoteAddress = peerToString(remote);
    if (const auto previous = deviceRegistry_.findDevice(deviceId);
        previous && previous->online && previous->remoteAddress != remoteAddress) {
        invalidateDeviceInLoop(deviceId, "registration_remote_changed", true);
    }
    invalidatedDevices_.erase(deviceId);
    deviceRegistry_.upsertRegistration(deviceId, remoteAddress);
    const auto lifetime =
        expires ? std::min(*expires, sipConfig_.registrationTimeoutSeconds)
                : sipConfig_.registrationTimeoutSeconds;
    scheduleDeadline(DeadlineKind::Registration, deviceId, std::chrono::seconds(std::max(1, lifetime)));
    LOG_INFO << "[GB28181][Register] Device registered, device=" << deviceId
             << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
             << ", contact=\"" << message.header("Contact") << "\""
             << ", expires=\"" << message.header("Expires") << "\""
             << ", call_id=" << message.header("Call-ID")
             << ", cseq=\"" << message.header("CSeq") << "\"";

    acknowledge(message, remote);

    scheduleCatalogQuery(deviceId);
}

void SipServer::handleMessage(const SipMessage& message, const SipPeer& remote) {
    pugi::xml_document document;
    const auto result = document.load_string(message.body.c_str());
    if (!result) {
        LOG_WARN << "[GB28181][Message] Ignored invalid XML from " << transportName(remote.transport)
                 << " " << peerToString(remote)
                 << ", error=" << result.description()
                 << ", body=\"" << compactForLog(message.body, 500) << "\"";
        sendResponse(message, remote, 200, "OK");
        return;
    }

    auto root = document.first_child();
    const auto cmdType = xmlText(root, "CmdType");
    auto deviceId = xmlText(root, "DeviceID");
    if (deviceId.empty()) {
        LOG_WARN << "[GB28181][Message] Ignored MESSAGE without DeviceID, cmd_type=" << cmdType
                 << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
                 << ", body=\"" << compactForLog(message.body, 500) << "\"";
        sendResponse(message, remote, 200, "OK");
        return;
    }
    const auto snText = xmlText(root, "SN");
    LOG_DEBUG << "[GB28181][Message] Received, cmd_type=" << cmdType
              << ", device=" << deviceId
              << ", sn=" << snText
              << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
              << ", body_bytes=" << message.body.size();

    auto rejectUnexpectedRemote = [&](std::string_view context,
                                      const std::string& targetDeviceId) {
        if (!invalidatedDevices_.contains(targetDeviceId) &&
            registeredRemoteMatches(deviceRegistry_, targetDeviceId, remote)) {
            return false;
        }
        LOG_WARN << "[GB28181][" << context
                 << "] Ignored MESSAGE from unexpected remote, device="
                 << targetDeviceId
                 << ", remote=" << transportName(remote.transport) << " "
                 << peerToString(remote);
        return true;
    };

    if (cmdType == "Keepalive") {
        if (rejectUnexpectedRemote("Keepalive", deviceId)) {
            sendResponse(message, remote, 403, "Forbidden");
            return;
        }
        const auto status = xmlText(root, "Status");
        bool shouldQueryCatalog = false;
        if (statusOnline(status)) {
            shouldQueryCatalog = deviceRegistry_.updateKeepaliveAndNeedsCatalog(deviceId, peerToString(remote));
            scheduleDeadline(
                DeadlineKind::Registration,
                deviceId,
                std::chrono::seconds(sipConfig_.registrationTimeoutSeconds)
            );
            if (shouldQueryCatalog) {
                scheduleCatalogQuery(deviceId);
            }
        } else {
            invalidateDeviceInLoop(deviceId, "device_keepalive_offline", false);
        }
        LOG_DEBUG << "[GB28181][Keepalive] device=" << deviceId
                  << ", status=" << status
                  << ", online=" << statusOnline(status)
                  << ", catalog_scheduled=" << shouldQueryCatalog
                  << ", remote=" << transportName(remote.transport) << " " << peerToString(remote);
        acknowledge(message, remote);
        return;
    }

    if (cmdType == "Catalog") {
        if (rejectUnexpectedRemote("Catalog", deviceId)) {
            sendResponse(message, remote, 403, "Forbidden");
            return;
        }
        std::vector<device::Channel> channels;
        std::size_t onlineCount = 0;
        for (auto item : root.child("DeviceList").children("Item")) {
            device::Channel channel;
            channel.id = xmlText(item, "DeviceID");
            channel.name = xmlText(item, "Name");
            channel.manufacturer = xmlText(item, "Manufacturer");
            channel.online = statusOnline(xmlText(item, "Status"));
            channel.ptzType = xmlInt(item, "PTZType");
            if (!channel.id.empty()) {
                if (channel.online) {
                    ++onlineCount;
                }
                LOG_DEBUG << "[GB28181][Catalog] Channel, device=" << deviceId
                          << ", channel=" << channel.id
                          << ", name=\"" << channel.name << "\""
                          << ", manufacturer=\"" << channel.manufacturer << "\""
                          << ", online=" << channel.online
                          << ", ptz_type=" << channel.ptzType;
                channels.push_back(std::move(channel));
            }
        }
        const auto channelCount = channels.size();
        deviceRegistry_.updateCatalog(deviceId, std::move(channels));
        LOG_DEBUG << "[GB28181][Catalog] Updated, device=" << deviceId
                  << ", sn=" << snText
                  << ", channels=" << channelCount
                  << ", online_channels=" << onlineCount
                  << ", remote=" << transportName(remote.transport) << " " << peerToString(remote);
        acknowledge(message, remote);
        return;
    }

    LOG_WARN << "[GB28181][Message] Unhandled CmdType, cmd_type=" << cmdType
             << ", device=" << deviceId
             << ", sn=" << snText
             << ", remote=" << transportName(remote.transport) << " " << peerToString(remote);
    sendResponse(message, remote, 200, "OK");
}

bool SipServer::queryCatalog(const std::string& deviceId) {
    LOG_DEBUG << "[GB28181][Catalog] Query requested, device=" << deviceId;

    const auto route = deviceRegistry_.findRouteSnapshot(deviceId);
    const auto unavailableReason = routeUnavailableReason(route);
    if (!unavailableReason.empty()) {
        LOG_WARN << "[GB28181][Catalog] Query skipped, device=" << deviceId
                 << ", reason=" << unavailableReason;
        return false;
    }

    const auto endpoint = parseRemoteEndpoint(route->remoteAddress);
    if (!endpoint.has_value()) {
        LOG_WARN << "[GB28181][Catalog] Query skipped, device=" << deviceId
                 << ", reason=invalid_remote_address"
                 << ", remote_address=" << route->remoteAddress;
        return false;
    }

    auto remote = peerFromAddress(route->remoteAddress);
    if (!remote.has_value()) {
        LOG_WARN << "[GB28181][Catalog] Query skipped, device=" << deviceId
                 << ", reason=peer_unavailable"
                 << ", remote_address=" << route->remoteAddress;
        return false;
    }

    const auto sn = cseq_.fetch_add(1);
    std::ostringstream body;
    body << "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
         << "<Query>\r\n"
         << "<CmdType>Catalog</CmdType>\r\n"
         << "<SN>" << sn << "</SN>\r\n"
         << "<DeviceID>" << deviceId << "</DeviceID>\r\n"
         << "</Query>\r\n";

    const auto bodyText = body.str();
    const auto publicHost = sipConfig_.publicIp.empty() || sipConfig_.publicIp == "YOUR_PUBLIC_SERVER_IP" ? sipConfig_.host : sipConfig_.publicIp;
    const auto branch = "z9hG4bK-" + makeToken("branch");
    const auto tag = makeToken("tag");
    const auto callId = makeToken("catalog") + "@" + sipConfig_.domain;

    const SipRequestHead request{
        .method = "MESSAGE",
        .targetUser = deviceId,
        .targetHost = endpoint->host,
        .targetPort = endpoint->port,
        .transport = transportName(remote->transport),
        .platformId = sipConfig_.id,
        .platformHost = publicHost,
        .platformPort = sipConfig_.port,
        .domain = sipConfig_.domain,
        .branch = branch,
        .fromTag = tag,
        .callId = callId,
        .cseq = sn,
        .contentType = "Application/MANSCDP+xml",
    };

    sendRequest(request.serialize(bodyText), *remote);
    LOG_DEBUG << "[GB28181][Catalog] Query sent, device=" << deviceId
              << ", sn=" << sn
              << ", call_id=" << callId
              << ", branch=" << branch
              << ", remote=" << transportName(remote->transport) << " " << peerToString(*remote)
              << ", body_bytes=" << bodyText.size();
    return true;
}

bool SipServer::sendPtzControl(const std::string& deviceId, const std::string& channelId, const std::string& action, uint8_t speed) {
    LOG_DEBUG << "[GB28181][PTZ] Control requested, device=" << deviceId
              << ", channel=" << channelId
              << ", action=" << action
              << ", speed=" << static_cast<unsigned int>(speed);

    const auto route = deviceRegistry_.findRouteSnapshot(deviceId);
    const auto unavailableReason = routeUnavailableReason(route);
    if (!unavailableReason.empty()) {
        LOG_WARN << "[GB28181][PTZ] Control skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", action=" << action
                 << ", reason=" << unavailableReason;
        return false;
    }

    const auto endpoint = parseRemoteEndpoint(route->remoteAddress);
    if (!endpoint.has_value()) {
        LOG_WARN << "[GB28181][PTZ] Control skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", action=" << action
                 << ", reason=invalid_remote_address"
                 << ", remote_address=" << route->remoteAddress;
        return false;
    }

    auto remote = peerFromAddress(route->remoteAddress);
    if (!remote.has_value()) {
        LOG_WARN << "[GB28181][PTZ] Control skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", action=" << action
                 << ", reason=peer_unavailable"
                 << ", remote_address=" << route->remoteAddress;
        return false;
    }

    const auto sn = cseq_.fetch_add(1);
    const auto command = ptzCommand(action, speed);
    std::ostringstream body;
    body << "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
         << "<Control>\r\n"
         << "<CmdType>DeviceControl</CmdType>\r\n"
         << "<SN>" << sn << "</SN>\r\n"
         << "<DeviceID>" << channelId << "</DeviceID>\r\n"
         << "<PTZCmd>" << command << "</PTZCmd>\r\n"
         << "<Info>\r\n"
         << "<ControlPriority>5</ControlPriority>\r\n"
         << "</Info>\r\n"
         << "</Control>\r\n";

    const auto bodyText = body.str();
    const auto publicHost = sipConfig_.publicIp.empty() || sipConfig_.publicIp == "YOUR_PUBLIC_SERVER_IP" ? sipConfig_.host : sipConfig_.publicIp;
    const auto branch = "z9hG4bK-" + makeToken("ptz");
    const auto tag = makeToken("tag");
    const auto callId = makeToken("ptz") + "@" + sipConfig_.domain;

    const SipRequestHead request{
        .method = "MESSAGE",
        .targetUser = channelId,
        .targetHost = endpoint->host,
        .targetPort = endpoint->port,
        .transport = transportName(remote->transport),
        .platformId = sipConfig_.id,
        .platformHost = publicHost,
        .platformPort = sipConfig_.port,
        .domain = sipConfig_.domain,
        .branch = branch,
        .fromTag = tag,
        .callId = callId,
        .cseq = sn,
        .contentType = "Application/MANSCDP+xml",
    };

    sendRequest(request.serialize(bodyText), *remote);
    LOG_DEBUG << "[GB28181][PTZ] Control sent, device=" << deviceId
              << ", channel=" << channelId
              << ", action=" << action
              << ", speed=" << static_cast<unsigned int>(speed)
              << ", command=" << command
              << ", sn=" << sn
              << ", call_id=" << callId
              << ", branch=" << branch
              << ", remote=" << transportName(remote->transport) << " " << peerToString(*remote)
              << ", body_bytes=" << bodyText.size();
    return true;
}

bool SipServer::sendPtzPreciseControl(
    const std::string& deviceId,
    const std::string& channelId,
    double pan,
    double tilt,
    double zoom
) {
    LOG_DEBUG << "[GB28181][PTZ] Precise control requested, device=" << deviceId
              << ", channel=" << channelId
              << ", pan=" << pan
              << ", tilt=" << tilt
              << ", zoom=" << zoom;

    const auto route = deviceRegistry_.findRouteSnapshot(deviceId);
    const auto unavailableReason = routeUnavailableReason(route);
    if (!unavailableReason.empty()) {
        LOG_WARN << "[GB28181][PTZ] Precise control skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", reason=" << unavailableReason;
        return false;
    }

    const auto endpoint = parseRemoteEndpoint(route->remoteAddress);
    if (!endpoint.has_value()) {
        LOG_WARN << "[GB28181][PTZ] Precise control skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", reason=invalid_remote_address"
                 << ", remote_address=" << route->remoteAddress;
        return false;
    }

    auto remote = peerFromAddress(route->remoteAddress);
    if (!remote.has_value()) {
        LOG_WARN << "[GB28181][PTZ] Precise control skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", reason=peer_unavailable"
                 << ", remote_address=" << route->remoteAddress;
        return false;
    }

    const auto sn = cseq_.fetch_add(1);
    std::ostringstream body;
    body.imbue(std::locale::classic());
    body << std::fixed << std::setprecision(2)
         << "<?xml version=\"1.0\" encoding=\"GB2312\"?>\r\n"
         << "<Control>\r\n"
         << "<CmdType>DeviceControl</CmdType>\r\n"
         << "<SN>" << sn << "</SN>\r\n"
         << "<DeviceID>" << channelId << "</DeviceID>\r\n"
         << "<PTZPreciseCtrl>\r\n"
         << "<Pan>" << pan << "</Pan>\r\n"
         << "<Tilt>" << tilt << "</Tilt>\r\n"
         << "<Zoom>" << zoom << "</Zoom>\r\n"
         << "</PTZPreciseCtrl>\r\n"
         << "</Control>\r\n";

    const auto bodyText = body.str();
    const auto publicHost = sipConfig_.publicIp.empty() || sipConfig_.publicIp == "YOUR_PUBLIC_SERVER_IP" ? sipConfig_.host : sipConfig_.publicIp;
    const auto branch = "z9hG4bK-" + makeToken("ptz-precise");
    const auto tag = makeToken("tag");
    const auto callId = makeToken("ptz-precise") + "@" + sipConfig_.domain;

    const SipRequestHead request{
        .method = "MESSAGE",
        .targetUser = channelId,
        .targetHost = endpoint->host,
        .targetPort = endpoint->port,
        .transport = transportName(remote->transport),
        .platformId = sipConfig_.id,
        .platformHost = publicHost,
        .platformPort = sipConfig_.port,
        .domain = sipConfig_.domain,
        .branch = branch,
        .fromTag = tag,
        .callId = callId,
        .cseq = sn,
        .contentType = "Application/MANSCDP+xml",
    };

    sendRequest(request.serialize(bodyText), *remote);
    LOG_DEBUG << "[GB28181][PTZ] Precise control sent, device=" << deviceId
              << ", channel=" << channelId
              << ", pan=" << pan
              << ", tilt=" << tilt
              << ", zoom=" << zoom
              << ", sn=" << sn
              << ", call_id=" << callId
              << ", branch=" << branch
              << ", remote=" << transportName(remote->transport) << " " << peerToString(*remote)
              << ", body_bytes=" << bodyText.size();
    return true;
}

std::optional<SipPreviewStartResult>
SipServer::startPreview(const std::string& deviceId, const std::string& channelId) {
    // Every SDK call made by this SIP actor is attributed to the Collector
    // that owns its listening socket.  The marker is thread-local and the
    // whole method runs on ioLoop_, so SDK callbacks can route back without
    // hashing or handing the session to another worker.
    media::ZlmSdk::OwnerScope sdkOwner(owner_);
    LOG_DEBUG << "[GB28181][Preview] Start requested, device=" << deviceId
              << ", channel=" << channelId;

    const auto route = deviceRegistry_.findRouteSnapshot(deviceId, channelId);
    const auto unavailableReason = routeUnavailableReason(route);
    if (!unavailableReason.empty()) {
        LOG_WARN << "[GB28181][Preview] Start skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", reason=" << unavailableReason;
        return std::nullopt;
    }
    if (route->hasChannels && !route->channelExists) {
        LOG_WARN << "[GB28181][Preview] Start skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", reason=channel_not_found";
        return std::nullopt;
    }

    const auto endpoint = parseRemoteEndpoint(route->remoteAddress);
    if (!endpoint.has_value()) {
        LOG_WARN << "[GB28181][Preview] Start skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", reason=invalid_remote_address"
                 << ", remote_address=" << route->remoteAddress;
        return std::nullopt;
    }

    auto remote = peerFromAddress(route->remoteAddress);
    if (!remote.has_value()) {
        LOG_WARN << "[GB28181][Preview] Start skipped, device=" << deviceId
                 << ", channel=" << channelId
                 << ", reason=peer_unavailable"
                 << ", remote_address=" << route->remoteAddress;
        return std::nullopt;
    }

    std::vector<std::string> staleSessionIds;
    for (auto& [_, session] : previewSessions_) {
        if (session.deviceId == deviceId && session.channelId == channelId) {
            if (!session.mediaOnline) {
                staleSessionIds.push_back(session.sessionId);
                continue;
            }
            const auto viewerId = makeToken("viewer");
            ++session.viewerCount;
            previewViewers_[viewerId] = session.sessionId;
            scheduleDeadline(
                DeadlineKind::ViewerLease,
                viewerId,
                std::chrono::seconds(sipConfig_.viewerLeaseTimeoutSeconds)
            );
            notifyViewerCountChanged(session.streamId, session.viewerCount);
            LOG_DEBUG << "[GB28181][Preview] Stream reused, device=" << deviceId
                      << ", channel=" << channelId
                      << ", viewer_session=" << viewerId
                      << ", stream_session=" << session.sessionId
                      << ", stream_id=" << session.streamId
                      << ", viewers=" << session.viewerCount;
            return SipPreviewStartResult{
                viewerId,
                session.deviceId,
                session.channelId,
                session.streamId,
                session.ssrc,
                session.rtpPort,
                session.playUrls,
                static_cast<unsigned int>(sipConfig_.viewerLeaseTimeoutSeconds),
            };
        }
    }

    for (const auto& staleSessionId : staleSessionIds) {
        const auto result = stopPreview(staleSessionId);
        if (result.has_value()) {
            LOG_DEBUG << "[GB28181][Preview] Closed stale session before restart, session=" << staleSessionId
                      << ", stream_id=" << result->streamId
                      << ", bye_sent=" << result->byeSent
                      << ", rtp_server_closed=" << result->rtpServerClosed;
        }
    }

    const auto cseq = cseq_.fetch_add(1);
    const auto publicHost = sipConfig_.publicIp.empty() || sipConfig_.publicIp == "YOUR_PUBLIC_SERVER_IP" ? sipConfig_.host : sipConfig_.publicIp;
    const auto mediaHost = mediaConfig_.rtpPublicIp.empty() || mediaConfig_.rtpPublicIp == "YOUR_PUBLIC_SERVER_IP" ? publicHost : mediaConfig_.rtpPublicIp;
    const auto sessionId = makeToken("preview");
    const auto branch = "z9hG4bK-" + makeToken("branch");
    const auto fromTag = makeToken("tag");
    const auto callId = sessionId + "@" + sipConfig_.domain;
    const auto nowMs = static_cast<unsigned long long>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    const auto ssrcNumber = 1000000000ULL + ((nowMs + cseq) % 899999999ULL);
    const auto ssrc = std::to_string(ssrcNumber);
    LOG_DEBUG << "[GB28181][Preview] Opening ZLM RTP server, device=" << deviceId
              << ", channel=" << channelId
              << ", ssrc=" << ssrc;
    const auto rtpServer = zlmSdk_->openRtpServer(deviceId, channelId, ssrc, service::utils::nextUuid(uuidState_));
    if (!rtpServer.has_value()) {
        LOG_WARN << "[GB28181][Preview] Start failed, device=" << deviceId
                 << ", channel=" << channelId
                 << ", reason=zlm_open_rtp_failed"
                 << ", ssrc=" << ssrc;
        return std::nullopt;
    }
    LOG_DEBUG << "[GB28181][Preview] ZLM RTP server opened, stream_id=" << rtpServer->streamId
              << ", rtp_port=" << rtpServer->port
              << ", ssrc=" << ssrc;

    std::ostringstream body;
    body << "v=0\r\n"
         << "o=" << channelId << " 0 0 IN IP4 " << mediaHost << "\r\n"
         << "s=Play\r\n"
         << "c=IN IP4 " << mediaHost << "\r\n"
         << "t=0 0\r\n"
         << "m=video " << rtpServer->port << " TCP/RTP/AVP 96\r\n"
         << "a=recvonly\r\n"
         << "a=setup:passive\r\n"
         << "a=connection:new\r\n"
         << "a=rtpmap:96 PS/90000\r\n"
         << "y=" << ssrc << "\r\n";

    const auto bodyText = body.str();
    const auto subject = channelId + ":" + ssrc + "," + sipConfig_.id + ":0";
    const SipRequestHead invite{
        .method = "INVITE",
        .targetUser = channelId,
        .targetHost = endpoint->host,
        .targetPort = endpoint->port,
        .transport = transportName(remote->transport),
        .platformId = sipConfig_.id,
        .platformHost = publicHost,
        .platformPort = sipConfig_.port,
        .domain = sipConfig_.domain,
        .branch = branch,
        .fromTag = fromTag,
        .callId = callId,
        .cseq = cseq,
        .contact = true,
        .subject = subject,
        .contentType = "application/sdp",
    };

    PreviewSession session;
    session.sessionId = sessionId;
    session.deviceId = deviceId;
    session.channelId = channelId;
    session.streamId = rtpServer->streamId;
    session.callId = callId;
    session.fromTag = fromTag;
    session.branch = branch;
    session.ssrc = ssrc;
    session.inviteCseq = cseq;
    session.rtpPort = rtpServer->port;
    session.playUrls = rtpServer->playUrls;
    session.remote = *remote;
    session.viewerCount = 1;

    const auto viewerId = makeToken("viewer");

    previewSessions_.emplace(sessionId, session);
    previewViewers_.emplace(viewerId, sessionId);
    notifyViewerCountChanged(session.streamId, session.viewerCount);
    scheduleDeadline(DeadlineKind::Invite, sessionId, std::chrono::seconds(sipConfig_.inviteTimeoutSeconds));
    scheduleDeadline(DeadlineKind::ViewerLease, viewerId, std::chrono::seconds(sipConfig_.viewerLeaseTimeoutSeconds));
    LOG_DEBUG << "[GB28181][Preview] Session stored, viewer_session=" << viewerId
              << ", stream_session=" << sessionId
              << ", stream_id=" << session.streamId
              << ", call_id=" << callId
              << ", cseq=" << cseq;

    sendRequest(invite.serialize(bodyText), *remote);
    LOG_DEBUG << "[GB28181][Preview] INVITE sent, device=" << deviceId
              << ", channel=" << channelId
              << ", viewer_session=" << viewerId
              << ", stream_session=" << sessionId
              << ", stream_id=" << rtpServer->streamId
              << ", rtp_port=" << rtpServer->port
              << ", ssrc=" << ssrc
              << ", call_id=" << callId
              << ", branch=" << branch
              << ", remote=" << transportName(remote->transport) << " " << peerToString(*remote)
              << ", sdp=\"" << compactForLog(bodyText, 700) << "\"";

    return SipPreviewStartResult{
        viewerId,
        session.deviceId,
        session.channelId,
        session.streamId,
        session.ssrc,
        session.rtpPort,
        session.playUrls,
        static_cast<unsigned int>(sipConfig_.viewerLeaseTimeoutSeconds),
    };
}

void SipServer::markStreamOnline(const std::string& streamId, bool online) {
    bool found = false;
    for (auto& [_, session] : previewSessions_) {
        if (session.streamId == streamId) {
            found = true;
            session.mediaOnline = online;
            LOG_DEBUG << "[GB28181][Media] Session media state changed, session=" << session.sessionId
                      << ", device=" << session.deviceId
                      << ", channel=" << session.channelId
                      << ", stream_id=" << streamId
                      << ", online=" << online
                      << ", viewers=" << session.viewerCount;
        }
    }
    if (!found) {
        LOG_DEBUG << "[GB28181][Media] Stream state ignored because session was not found, stream_id="
                  << streamId << ", online=" << online;
    }
}

std::optional<SipPreviewStopResult>
SipServer::stopPreview(const std::string& sessionId) {
    media::ZlmSdk::OwnerScope sdkOwner(owner_);
    LOG_DEBUG << "[GB28181][Preview] Stop requested, session=" << sessionId;

    PreviewSession session;
    std::string streamSessionId = sessionId;
    std::vector<std::string> closedSessionIds;
    const auto viewerIter = previewViewers_.find(sessionId);
    const bool viewerRequest = viewerIter != previewViewers_.end();
    if (viewerRequest) {
        streamSessionId = viewerIter->second;
        closedSessionIds.push_back(sessionId);
        cancelDeadline(DeadlineKind::ViewerLease, sessionId);
        previewViewers_.erase(viewerIter);
    }

    const auto iter = previewSessions_.find(streamSessionId);
    if (iter == previewSessions_.end()) {
        LOG_WARN << "[GB28181][Preview] Stop skipped, session=" << sessionId
                 << ", resolved_stream_session=" << streamSessionId
                 << ", reason=session_not_found";
        for (const auto& closedSessionId : closedSessionIds) {
            notifyPreviewClosed(closedSessionId);
        }
        return std::nullopt;
    }

    if (viewerRequest && iter->second.viewerCount > 0) {
        --iter->second.viewerCount;
        notifyViewerCountChanged(iter->second.streamId, iter->second.viewerCount);
        if (iter->second.viewerCount > 0) {
            LOG_DEBUG << "[GB28181][Preview] Viewer released, viewer_session=" << sessionId
                      << ", stream_session=" << streamSessionId
                      << ", stream_id=" << iter->second.streamId
                      << ", viewers=" << iter->second.viewerCount;
            for (const auto& closedSessionId : closedSessionIds) {
                notifyPreviewClosed(closedSessionId);
            }
            return SipPreviewStopResult{
                sessionId,
                iter->second.streamId,
                false,
                false,
            };
        }
    }

    session = iter->second;
    for (auto viewer = previewViewers_.begin(); viewer != previewViewers_.end();) {
        if (viewer->second == streamSessionId) {
            cancelDeadline(DeadlineKind::ViewerLease, viewer->first);
            if (!viewerRequest || viewer->first != sessionId) {
                closedSessionIds.push_back(viewer->first);
            }
            viewer = previewViewers_.erase(viewer);
        } else {
            ++viewer;
        }
    }
    previewSessions_.erase(iter);
    for (const auto& closedSessionId : closedSessionIds) {
        notifyPreviewClosed(closedSessionId);
    }
    cancelDeadline(DeadlineKind::Invite, streamSessionId);
    if (!viewerRequest && session.viewerCount > 0) {
        notifyViewerCountChanged(session.streamId, 0);
    }

    LOG_DEBUG << "[GB28181][Preview] Stream session removed, requested_session=" << sessionId
              << ", stream_session=" << streamSessionId
              << ", device=" << session.deviceId
              << ", channel=" << session.channelId
              << ", stream_id=" << session.streamId
              << ", established=" << session.established
              << ", media_online=" << session.mediaOnline
              << ", viewers=" << session.viewerCount;

    bool byeSent = false;
    if (session.established) {
        const auto cseq = cseq_.fetch_add(1);
        const auto publicHost = sipConfig_.publicIp.empty() || sipConfig_.publicIp == "YOUR_PUBLIC_SERVER_IP" ? sipConfig_.host : sipConfig_.publicIp;
        const auto branch = "z9hG4bK-" + makeToken("bye");

        const SipRequestHead bye{
            .method = "BYE",
            .targetUser = session.channelId,
            .targetHost = session.remote.address,
            .targetPort = session.remote.port,
            .transport = transportName(session.remote.transport),
            .platformId = sipConfig_.id,
            .platformHost = publicHost,
            .platformPort = sipConfig_.port,
            .domain = sipConfig_.domain,
            .branch = branch,
            .fromTag = session.fromTag,
            .toTag = session.toTag,
            .callId = session.callId,
            .cseq = cseq,
            .contact = true,
        };

        sendRequest(bye.serialize({}), session.remote);
        byeSent = true;
        LOG_DEBUG << "[GB28181][Preview] BYE sent, session=" << streamSessionId
                  << ", stream_id=" << session.streamId
                  << ", call_id=" << session.callId
                  << ", cseq=" << cseq
                  << ", remote=" << transportName(session.remote.transport) << " " << peerToString(session.remote);
    } else {
        const auto publicHost =
            sipConfig_.publicIp.empty() ||
                sipConfig_.publicIp == "YOUR_PUBLIC_SERVER_IP"
            ? sipConfig_.host
            : sipConfig_.publicIp;
        const SipRequestHead cancel{
            .method = "CANCEL",
            .targetUser = session.channelId,
            .targetHost = session.remote.address,
            .targetPort = session.remote.port,
            .transport = transportName(session.remote.transport),
            .platformId = sipConfig_.id,
            .platformHost = publicHost,
            .platformPort = sipConfig_.port,
            .domain = sipConfig_.domain,
            .branch = session.branch,
            .fromTag = session.fromTag,
            .callId = session.callId,
            .cseq = session.inviteCseq,
        };
        sendRequest(cancel.serialize({}), session.remote);
        LOG_DEBUG << "[GB28181][Preview] CANCEL sent for pending session, session="
                  << streamSessionId << ", stream_id=" << session.streamId
                  << ", call_id=" << session.callId;
    }

    const bool rtpServerClosed = zlmSdk_->closeRtpServer(session.streamId);
    notifyStreamClosed(session.streamId);
    LOG_DEBUG << "[GB28181][Preview] RTP server close finished, session=" << streamSessionId
              << ", stream_id=" << session.streamId
              << ", closed=" << rtpServerClosed
              << ", bye_sent=" << byeSent;

    return SipPreviewStopResult{
        sessionId,
        session.streamId,
        byeSent,
        rtpServerClosed,
    };
}

bool SipServer::renewPreview(const std::string& sessionId) {
    const auto viewer = previewViewers_.find(sessionId);
    if (viewer == previewViewers_.end()) {
        LOG_DEBUG << "[GB28181][Preview] Viewer lease renewal skipped, viewer_session="
                  << sessionId << ", reason=viewer_not_found";
        return false;
    }
    const auto session = previewSessions_.find(viewer->second);
    if (session == previewSessions_.end()) {
        cancelDeadline(DeadlineKind::ViewerLease, sessionId);
        previewViewers_.erase(viewer);
        notifyPreviewClosed(sessionId);
        LOG_WARN << "[GB28181][Preview] Orphan viewer lease removed, viewer_session="
                 << sessionId;
        return false;
    }
    scheduleDeadline(DeadlineKind::ViewerLease, sessionId, std::chrono::seconds(sipConfig_.viewerLeaseTimeoutSeconds));
    LOG_DEBUG << "[GB28181][Preview] Viewer lease renewed, viewer_session="
              << sessionId << ", stream_session=" << session->second.sessionId
              << ", stream_id=" << session->second.streamId
              << ", timeout_seconds=" << sipConfig_.viewerLeaseTimeoutSeconds;
    return true;
}

std::optional<SipPreviewStopResult>
SipServer::stopPreviewByStream(const std::string& streamId) {
    LOG_DEBUG << "[GB28181][Preview] Stop by stream requested, stream_id=" << streamId;

    std::string sessionId;
    for (const auto& [candidateSessionId, session] : previewSessions_) {
        if (session.streamId == streamId) {
            sessionId = candidateSessionId;
            break;
        }
    }

    if (sessionId.empty()) {
        LOG_WARN << "[GB28181][Preview] Stop by stream skipped, stream_id=" << streamId
                 << ", reason=session_not_found";
        return std::nullopt;
    }
    return stopPreview(sessionId);
}

void SipServer::sendResponse(const SipMessage& request, const SipPeer& remote, int statusCode, const std::string& reason, const std::string& extraHeaders) {
    std::ostringstream response;
    response << "SIP/2.0 " << statusCode << ' ' << reason << "\r\n";

    const auto via = request.header("Via");
    const auto from = request.header("From");
    const auto to = request.header("To");
    const auto callId = request.header("Call-ID");
    const auto cseq = request.header("CSeq");

    if (!via.empty()) {
        response << "Via: " << via << "\r\n";
    }
    if (!from.empty()) {
        response << "From: " << from << "\r\n";
    }
    if (!to.empty()) {
        response << "To: " << to << "\r\n";
    }
    if (!callId.empty()) {
        response << "Call-ID: " << callId << "\r\n";
    }
    if (!cseq.empty()) {
        response << "CSeq: " << cseq << "\r\n";
    }

    response << "User-Agent: gb28181-platform-cpp\r\n";
    response << extraHeaders;
    response << "Content-Length: 0\r\n\r\n";

    const auto data = response.str();
    sendRequest(data, remote);
    if (sipConfig_.logging) {
        LOG_DEBUG << "[GB28181][SIP] Response sent, status=" << statusCode
                  << ", reason=\"" << reason << "\""
                  << ", remote=" << transportName(remote.transport) << " " << peerToString(remote)
                  << ", call_id=" << callId
                  << ", cseq=\"" << cseq << "\"";
    }
}

void SipServer::acknowledge(const SipMessage& request, const SipPeer& remote) {
    const auto handler = acknowledgementHandler_;
    if (!handler) {
        sendResponse(request, remote, 200, "OK");
        return;
    }

    const auto completed = std::make_shared<std::atomic_bool>(false);
    const auto weak = weak_from_this();
    auto completion = [weak, request, remote, completed](bool persisted) mutable {
        if (completed->exchange(true)) {
            return;
        }

        const auto self = weak.lock();
        if (!self || !self->ioLoop_.valid() || !self->running_.load()) {
            return;
        }

        if (self->ioLoop_.is_current()) {
            if (persisted) {
                self->sendResponse(request, remote, 200, "OK");
            } else {
                self->sendResponse(request, remote, 503, "Service Unavailable");
            }
            return;
        }

        const auto posted = self->ioLoop_.post(
            [weak, request, remote, persisted]() mutable {
                const auto self = weak.lock();
                if (!self || !self->running_.load()) {
                    return;
                }
                if (persisted) {
                    self->sendResponse(request, remote, 200, "OK");
                } else {
                    self->sendResponse(request, remote, 503, "Service Unavailable");
                }
            }
        );
        if (!posted.accepted()) {
            LOG_WARN << "[GB28181][SIP] Acknowledgement response rejected by Ruvia worker, remote="
                     << transportName(remote.transport) << " " << peerToString(remote);
        }
    };

    try {
        handler(std::move(completion));
    } catch (const std::exception& error) {
        LOG_WARN << "[GB28181][SIP] Acknowledgement handler failed: "
                 << error.what();
        if (!completed->exchange(true)) {
            if (running_.load()) {
                sendResponse(request, remote, 503, "Service Unavailable");
            }
        }
    } catch (...) {
        LOG_WARN << "[GB28181][SIP] Acknowledgement handler failed: unknown error";
        if (!completed->exchange(true)) {
            if (running_.load()) {
                sendResponse(request, remote, 503, "Service Unavailable");
            }
        }
    }
}

void SipServer::sendRequest(const std::string& request, const SipPeer& remote) {
    if (sipConfig_.logging) {
        logSipSend(request, remote, true);
    }

    auto data = std::make_shared<std::string>(request);
    const auto weak = weak_from_this();
    if (remote.transport == SipTransport::Tcp) {
        if (!remote.tcp) {
            LOG_WARN << "[GB28181][SIP] TCP send failed: connection is closed, remote="
                     << peerToString(remote);
            return;
        }
        const auto connection = remote.tcp;
        const auto posted = ioLoop_.post(
            [weak, connection, data] {
                if (const auto self = weak.lock()) {
                    self->writeTcp(connection, data);
                }
            }
        );
        if (!posted.accepted()) {
            LOG_WARN << "[GB28181][SIP] TCP send rejected by Ruvia worker, remote="
                     << peerToString(remote);
        }
        return;
    }

    if (!ioLoop_.valid()) {
        LOG_WARN << "[GB28181][SIP] UDP send failed: socket is not ready, remote="
                 << peerToString(remote);
        return;
    }

    const auto posted = ioLoop_.post([weak, data, remoteAddress = remote.udp]() {
        const auto self = weak.lock();
        if (!self || !self->running_.load() || !self->udpSocket_ ||
            !self->udpSocket_->is_open()) {
            return;
        }
        self->udpSocket_->async_send_to(
            asio::buffer(*data),
            remoteAddress,
            [weak, data](const std::error_code& error, std::size_t sent) {
                const auto self = weak.lock();
                if (!self) {
                    return;
                }
                if (error) {
                    if (error != asio::error::operation_aborted) {
                        LOG_WARN << "[GB28181][SIP] UDP send failed: "
                                 << error.message();
                    }
                } else if (self->sipConfig_.logging) {
                    LOG_DEBUG << "[GB28181][SIP][UDP_TX] bytes=" << sent;
                }
            }
        );
    });
    if (!posted.accepted()) {
        LOG_WARN << "[GB28181][SIP] UDP send rejected by Ruvia worker, remote="
                 << peerToString(remote);
    }
}

void SipServer::writeTcp(const TcpConnectionPtr& connection, std::shared_ptr<std::string> data) {
    if (!running_.load() || !connection || !connection->socket.is_open()) {
        return;
    }
    connection->writes.push_back(std::move(data));
    if (!connection->writeInProgress) {
        continueTcpWrite(connection);
    }
}

void SipServer::continueTcpWrite(const TcpConnectionPtr& connection) {
    if (!connection || !connection->socket.is_open() || connection->writes.empty()) {
        if (connection) {
            connection->writeInProgress = false;
        }
        return;
    }
    connection->writeInProgress = true;
    const auto data = connection->writes.front();
    const auto weak = weak_from_this();
    asio::async_write(
        connection->socket,
        asio::buffer(*data),
        [weak, connection, data](const std::error_code& error, std::size_t sent) {
            const auto self = weak.lock();
            if (!self) {
                return;
            }
            if (error) {
                if (error != asio::error::operation_aborted) {
                    LOG_WARN << "[GB28181][SIP] TCP send failed to "
                             << connection->key << ": " << error.message();
                }
                connection->writes.clear();
                connection->writeInProgress = false;
                self->removeTcpConnection(connection);
                return;
            }
            connection->writes.pop_front();
            if (self->sipConfig_.logging) {
                LOG_DEBUG << "[GB28181][SIP][TCP_TX] remote=" << connection->key
                          << ", bytes=" << sent;
            }
            self->continueTcpWrite(connection);
        }
    );
}

std::optional<SipServer::SipPeer> SipServer::peerFromAddress(const std::string& remoteAddress) const {
    const auto endpoint = parseRemoteEndpoint(remoteAddress);
    if (!endpoint.has_value()) {
        return std::nullopt;
    }

    SipPeer peer;
    peer.address = endpoint->host;
    peer.port = endpoint->port;
    const auto iter = tcpConnections_.find(remoteAddress);
    if (iter != tcpConnections_.end() && iter->second) {
        peer.transport = SipTransport::Tcp;
        peer.tcp = iter->second;
        return peer;
    }
    if (lower(sipConfig_.transport) == "tcp") {
        return std::nullopt;
    }

    peer.transport = SipTransport::Udp;
    std::error_code error;
    const auto address = asio::ip::make_address(peer.address, error);
    if (error) {
        return std::nullopt;
    }
    peer.udp = asio::ip::udp::endpoint(address, peer.port);
    return peer;
}

void SipServer::scheduleCatalogQuery(const std::string& deviceId) {
    if (!ioLoop_.valid() || !ioLoop_.is_current()) {
        LOG_WARN << "[GB28181][Catalog] Schedule skipped, device=" << deviceId
                 << ", reason=io_loop_unavailable";
        return;
    }
    LOG_DEBUG << "[GB28181][Catalog] Query scheduled, device=" << deviceId
              << ", delay_seconds=0.5";
    scheduleDeadline(DeadlineKind::Catalog, deviceId, std::chrono::milliseconds(500));
}

std::string SipServer::deadlineKey(DeadlineKind kind, const std::string& key) {
    return std::to_string(static_cast<int>(kind)) + ":" + key;
}

void SipServer::scheduleDeadline(DeadlineKind kind, std::string key, std::chrono::milliseconds delay) {
    if (!deadlineTimer_ || !ioLoop_.is_current()) {
        return;
    }
    const auto composite = deadlineKey(kind, key);
    const auto generation = ++deadlineGenerations_[composite];
    deadlines_.push(Deadline{
        .at = std::chrono::steady_clock::now() + delay,
        .kind = kind,
        .key = std::move(key),
        .generation = generation,
    });
    armDeadlineTimer();
}

void SipServer::cancelDeadline(DeadlineKind kind, const std::string& key) {
    ++deadlineGenerations_[deadlineKey(kind, key)];
    armDeadlineTimer();
}

void SipServer::armDeadlineTimer() {
    if (!deadlineTimer_ || !running_.load()) {
        return;
    }
    while (!deadlines_.empty()) {
        const auto& next = deadlines_.top();
        const auto generation =
            deadlineGenerations_.find(deadlineKey(next.kind, next.key));
        if (generation != deadlineGenerations_.end() &&
            generation->second == next.generation) {
            break;
        }
        deadlines_.pop();
    }
    deadlineTimer_->cancel();
    if (deadlines_.empty()) {
        return;
    }
    deadlineTimer_->expires_at(deadlines_.top().at);
    const auto weak = weak_from_this();
    deadlineTimer_->async_wait([weak](const std::error_code& error) {
        if (const auto self = weak.lock()) {
            self->processDeadlines(error);
        }
    });
}

void SipServer::processDeadlines(const std::error_code& error) {
    if (error || !running_.load()) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    while (!deadlines_.empty() && deadlines_.top().at <= now) {
        auto deadline = deadlines_.top();
        deadlines_.pop();
        const auto generation =
            deadlineGenerations_.find(deadlineKey(deadline.kind, deadline.key));
        if (generation == deadlineGenerations_.end() ||
            generation->second != deadline.generation) {
            continue;
        }
        ++generation->second;
        expireDeadline(deadline);
    }
    armDeadlineTimer();
}

void SipServer::expireDeadline(const Deadline& deadline) {
    switch (deadline.kind) {
        case DeadlineKind::Catalog:
            if (queryCatalog(deadline.key)) {
                LOG_DEBUG << "[GB28181][Catalog] Scheduled query sent, device="
                          << deadline.key;
            } else {
                LOG_WARN << "[GB28181][Catalog] Scheduled query failed, device="
                         << deadline.key;
            }
            break;
        case DeadlineKind::Registration:
            invalidateDeviceInLoop(deadline.key, "registration_expired", true);
            LOG_WARN << "[GB28181][Register] Device expired, device=" << deadline.key;
            break;
        case DeadlineKind::Invite:
            LOG_WARN << "[GB28181][Invite] Session timed out, session="
                     << deadline.key;
            (void)stopPreview(deadline.key);
            break;
        case DeadlineKind::ViewerLease: {
            const auto viewer = previewViewers_.find(deadline.key);
            if (viewer == previewViewers_.end()) {
                break;
            }
            const auto session = previewSessions_.find(viewer->second);
            LOG_INFO << "[GB28181][Preview] Viewer lease expired, viewer_session="
                     << deadline.key << ", stream_session=" << viewer->second
                     << ", stream_id="
                     << (session == previewSessions_.end()
                             ? std::string{ "unknown" }
                             : session->second.streamId)
                     << ", viewers_before="
                     << (session == previewSessions_.end() ? 0U
                                                           : session->second.viewerCount);
            (void)stopPreview(deadline.key);
            break;
        }
        case DeadlineKind::AuthNonce:
            digestNonceCounts_.erase(deadline.key);
            break;
    }
}

void SipServer::closeDeviceSessions(const std::string& deviceId) {
    std::vector<std::string> sessions;
    for (const auto& [id, session] : previewSessions_) {
        if (session.deviceId == deviceId) {
            sessions.push_back(id);
        }
    }
    for (const auto& id : sessions) {
        (void)stopPreview(id);
    }
}

void SipServer::closeDeviceTransport(const std::string& deviceId, const std::string& remoteAddress) {
    if (remoteAddress.empty()) {
        return;
    }

    std::vector<TcpConnectionPtr> connections;
    for (const auto& [key, connection] : tcpConnections_) {
        if (key == remoteAddress && connection) {
            connections.push_back(connection);
        }
    }
    for (const auto& connection : connections) {
        removeTcpConnection(connection);
        LOG_DEBUG << "[GB28181][SIP] Device TCP session closed, device="
                  << deviceId << ", remote=" << remoteAddress;
    }
}

void SipServer::invalidateDeviceInLoop(const std::string& deviceId, std::string reason, bool closeTransport) {
    if (!ioLoop_.is_current()) {
        LOG_WARN << "[GB28181][SIP] Device invalidation ignored outside owning loop, device="
                 << deviceId;
        return;
    }

    const auto device = deviceRegistry_.findDevice(deviceId);
    const auto remoteAddress = device ? device->remoteAddress : std::string{};
    invalidatedDevices_.insert(deviceId);
    deviceRegistry_.markOffline(deviceId);
    cancelDeadline(DeadlineKind::Registration, deviceId);
    closeDeviceSessions(deviceId);
    if (closeTransport) {
        closeDeviceTransport(deviceId, remoteAddress);
    }

    LOG_INFO << "[GB28181][SIP] Device session invalidated, device=" << deviceId
             << ", reason=" << (reason.empty() ? "unspecified" : reason)
             << ", transport_closed=" << closeTransport;
}

} // namespace service::gb28181::sip
