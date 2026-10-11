#pragma once

#include <array>
#include <asio/ip/tcp.hpp>
#include <asio/ip/udp.hpp>
#include <asio/steady_timer.hpp>
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ruvia/core/event_loop_pool.h>

#include "service/common/uuid.h"
#include "service/features/gb28181/device/device.runtime.h"
#include "service/features/gb28181/gb28181.config.h"
#include "service/features/gb28181/media/media.transport.h"
#include "service/features/gb28181/sip/sip.protocol.h"
#include "service/features/gb28181/sip/sip.types.h"

#ifdef GetMessage
#undef GetMessage
#endif

class SipServer : public std::enable_shared_from_this<SipServer> {
  public:
    using ViewerCountObserver =
        std::function<void(const std::string& streamId, unsigned int viewerCount)>;

    SipServer(service::common::UuidV7Generator& uuidGenerator, SipConfig sipConfig, MediaConfig mediaConfig, DeviceRegistry& deviceRegistry, ZlmSdk& zlmSdk, ruvia::event_loop ioLoop, ViewerCountObserver viewerCountObserver = {}, ZlmSdk::OwnerIndex owner = ZlmSdk::kUnassignedOwner);
    ~SipServer();

    void start();
    void stop();

    // The Collector installs a local Redis projection barrier.  The handler
    // is invoked on this SipServer's event_loop after the local registry event
    // has been emitted.  It must eventually call the supplied completion with
    // true only after that event and every preceding local projection are
    // durable.  The completion may run from another thread; SipServer posts
    // the final response back to this event_loop.
    void setAcknowledgementHandler(
        std::function<void(std::function<void(bool)>)> handler
    );
    // The callback receives one public viewer session id for each session
    // removed from the local preview registry.  It runs on this event_loop.
    void setPreviewClosedHandler(
        std::function<void(const std::string& sessionId)> handler
    );
    // The callback receives one stream id when its local preview session is
    // removed and the SDK RTP server is closed.  It runs on this event_loop;
    // releasing an individual viewer while other viewers remain does not
    // invoke it.
    void setStreamClosedHandler(
        std::function<void(const std::string& streamId)> handler
    );

    // Invalidate only this Collector's local SIP ownership.  A UDP socket is
    // shared by the local server, so invalidating a UDP session blocks its old
    // endpoint until a fresh authenticated REGISTER; it never forwards the
    // device to another Worker.
    void invalidateDevice(const std::string& deviceId, std::string reason = {});

    bool queryCatalog(const std::string& deviceId);
    bool sendPtzControl(const std::string& deviceId, const std::string& channelId, const std::string& action, uint8_t speed);
    bool sendPtzPreciseControl(const std::string& deviceId, const std::string& channelId, double pan, double tilt, double zoom);
    std::optional<SipPreviewStartResult> startPreview(const std::string& deviceId, const std::string& channelId);
    std::optional<SipPreviewStopResult> stopPreview(const std::string& sessionId);
    bool renewPreview(const std::string& sessionId);
    std::optional<SipPreviewStopResult> stopPreviewByStream(const std::string& streamId);
    void markStreamOnline(const std::string& streamId, bool online);

    enum class SipTransport {
        Udp,
        Tcp,
    };

    struct TcpConnection final {
        explicit TcpConnection(asio::ip::tcp::socket value)
            : socket(std::move(value)) {}

        asio::ip::tcp::socket socket;
        std::string key;
        std::string address;
        std::uint16_t port{ 0 };
        std::array<char, 8192> readBuffer{};
        std::string pending;
        std::deque<std::shared_ptr<std::string>> writes;
        bool writeInProgress{ false };
    };

    using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

    struct SipPeer {
        SipTransport transport{ SipTransport::Udp };
        asio::ip::udp::endpoint udp;
        TcpConnectionPtr tcp;
        std::string address;
        uint16_t port{ 0 };
    };

  private:
    enum class DeadlineKind {
        Catalog,
        Registration,
        Invite,
        ViewerLease,
        AuthNonce,
    };

    struct Deadline {
        std::chrono::steady_clock::time_point at;
        DeadlineKind kind{ DeadlineKind::Catalog };
        std::string key;
        std::uint64_t generation{ 0 };
    };

    struct DeadlineLater {
        bool operator()(const Deadline& left, const Deadline& right) const noexcept {
            return left.at > right.at;
        }
    };

    struct PreviewSession {
        std::string sessionId;
        std::string deviceId;
        std::string channelId;
        std::string streamId;
        std::string callId;
        std::string fromTag;
        std::string toTag;
        std::string branch;
        std::string ssrc;
        unsigned int inviteCseq{ 0 };
        uint16_t rtpPort{ 0 };
        PlayUrls playUrls;
        SipPeer remote;
        bool established{ false };
        bool mediaOnline{ false };
        unsigned int viewerCount{ 0 };
    };

    service::common::UuidV7Generator& uuidGenerator_;
    SipConfig sipConfig_;
    MediaConfig mediaConfig_;
    DeviceRegistry& deviceRegistry_;
    ZlmSdk& zlmSdk_;
    ViewerCountObserver viewerCountObserver_;
    ZlmSdk::OwnerIndex owner_{ ZlmSdk::kUnassignedOwner };
    std::atomic_bool running_{ false };
    ruvia::event_loop ioLoop_;
    std::unique_ptr<asio::ip::udp::socket> udpSocket_;
    std::unique_ptr<asio::ip::tcp::acceptor> tcpAcceptor_;
    std::unique_ptr<asio::steady_timer> deadlineTimer_;
    std::array<char, 8192> udpBuffer_{};
    asio::ip::udp::endpoint udpRemote_;
    SipUnsupportedRequestGuard unsupportedRequestGuard_;
    std::atomic_uint cseq_{ 1 };
    std::unordered_map<std::string, TcpConnectionPtr> tcpConnections_;
    std::unordered_set<std::string> invalidatedDevices_;
    std::map<std::string, PreviewSession> previewSessions_;
    std::map<std::string, std::string> previewViewers_;
    std::unordered_map<std::string, std::uint32_t> digestNonceCounts_;
    std::unordered_map<std::string, std::uint64_t> deadlineGenerations_;
    std::priority_queue<Deadline, std::vector<Deadline>, DeadlineLater> deadlines_;

    void startInLoop();
    void stopInLoop();
    void notifyViewerCountChanged(const std::string& streamId, unsigned int viewerCount) const noexcept;
    void notifyPreviewClosed(const std::string& sessionId) noexcept;
    void notifyStreamClosed(const std::string& streamId) noexcept;
    void receiveUdp();
    void acceptTcp();
    void readTcp(const TcpConnectionPtr& connection);
    void removeTcpConnection(const TcpConnectionPtr& connection);
    void processTcpPending(const TcpConnectionPtr& connection);
    void writeTcp(const TcpConnectionPtr& connection, std::shared_ptr<std::string> data);
    void continueTcpWrite(const TcpConnectionPtr& connection);
    void handlePacket(const std::string& packet, const SipPeer& remote);
    void handleRegister(const SipMessage& message, const SipPeer& remote);
    void handleMessage(const SipMessage& message, const SipPeer& remote);
    void handleResponse(const SipMessage& message, const SipPeer& remote);
    void handleInviteOk(const SipMessage& message, const SipPeer& remote);
    void sendResponse(const SipMessage& request, const SipPeer& remote, int statusCode, const std::string& reason, const std::string& extraHeaders = {});
    void acknowledge(const SipMessage& request, const SipPeer& remote);
    void sendRequest(const std::string& request, const SipPeer& remote);
    std::optional<SipPeer> peerFromAddress(const std::string& remoteAddress) const;
    void scheduleCatalogQuery(const std::string& deviceId);
    void scheduleDeadline(DeadlineKind kind, std::string key, std::chrono::milliseconds delay);
    void cancelDeadline(DeadlineKind kind, const std::string& key);
    void armDeadlineTimer();
    void processDeadlines(const std::error_code& error);
    void expireDeadline(const Deadline& deadline);
    [[nodiscard]] static std::string deadlineKey(DeadlineKind kind, const std::string& key);
    void closeDeviceSessions(const std::string& deviceId);
    void invalidateDeviceInLoop(const std::string& deviceId, std::string reason, bool closeTransport);
    void closeDeviceTransport(const std::string& deviceId, const std::string& remoteAddress);

    std::function<void(std::function<void(bool)>)> acknowledgementHandler_;
    std::function<void(const std::string& sessionId)> previewClosedHandler_;
    std::function<void(const std::string& streamId)> streamClosedHandler_;
};
