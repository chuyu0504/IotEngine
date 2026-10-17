#pragma once

#include "service/common/uuid.h"
#include "service/utils/uuid.h"

#include <array>
#include <asio.hpp>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#if defined(SO_REUSEPORT) && !defined(_WIN32)
#include <sys/socket.h>
#endif
#include "service/common/message.h"
#include "service/features/collector/collector.types.h"
#include "service/features/collector/tcp/tcp.types.h"
#include "service/features/collector/engine/engine.types.h"
#include "service/features/collector/scheduling/scheduling.runtime.h"

namespace service::collector {

class TcpTransport final {
  public:
    using ConnectedHandler = std::function<void(ProtocolConnectionInfo)>;
    using PacketHandler = std::function<void(IngressPacket)>;
    using DisconnectedHandler = std::function<void(std::string, std::string)>;
    using StateHandler = std::function<void(LinkState)>;
    using WriteHandler = std::function<void(bool)>;
    using TargetClaimHandler =
        std::function<void(std::string, std::string, std::string, std::function<void(bool)>)>;
    using TargetReleaseHandler = std::function<void(std::string, std::string, std::string)>;

    TcpTransport(service::common::UuidV7State& uuidState, asio::io_context& ioContext, DeadlineScheduler& scheduler, std::size_t workerIndex, std::size_t workerCount, ConnectedHandler onConnected, PacketHandler onPacket, DisconnectedHandler onDisconnected, StateHandler onState, TargetClaimHandler onTargetClaim = {}, TargetReleaseHandler onTargetRelease = {})
        : uuidState_(uuidState), ioContext_(ioContext), scheduler_(scheduler), workerIndex_(workerIndex),
          workerCount_(workerCount), onConnected_(std::move(onConnected)),
          onPacket_(std::move(onPacket)), onDisconnected_(std::move(onDisconnected)),
          onState_(std::move(onState)), onTargetClaim_(std::move(onTargetClaim)),
          onTargetRelease_(std::move(onTargetRelease)) {}

    TcpTransport(const TcpTransport&) = delete;
    TcpTransport& operator=(const TcpTransport&) = delete;

    ~TcpTransport() { stop(); }

    void reload(const RuntimeSnapshot& snapshot) {
        stopTransports();
        stopped_ = false;
        definitions_.clear();
        for (const auto& link : snapshot.links) {
            definitions_.emplace(link.id, link);
            startDefinition(link);
        }
    }

    void stopLinks(const std::set<std::string, std::less<>>& linkIds) {
        for (auto current = listeners_.begin(); current != listeners_.end();) {
            if (!linkIds.contains(current->first)) {
                ++current;
                continue;
            }
            current->second->stop();
            current = listeners_.erase(current);
        }
        for (auto current = clients_.begin(); current != clients_.end();) {
            if (!linkIds.contains(current->second->link.id)) {
                ++current;
                continue;
            }
            current->second->stop();
            current = clients_.erase(current);
        }
        std::vector<std::string> closing;
        for (const auto& [connectionId, linkId] : connectionLinks_) {
            if (linkIds.contains(linkId)) {
                closing.push_back(connectionId);
            }
        }
        for (const auto& connectionId : closing) {
            const auto current = connections_.find(connectionId);
            if (current != connections_.end()) {
                current->second->close("config_reloaded");
            }
        }
        for (const auto& linkId : linkIds) {
            lastActivity_.erase(linkId);
        }
    }

    void reconcile(const RuntimeSnapshot& snapshot, const RuntimeReconcilePlan& plan) {
        stopped_ = false;
        std::map<std::string, const LinkDefinition*, std::less<>> nextLinks;
        for (const auto& link : snapshot.links) {
            if (plan.affectedLinks.contains(link.id)) {
                nextLinks.emplace(link.id, &link);
            }
        }

        for (const auto& linkId : plan.affectedLinks) {
            const auto old = definitions_.find(linkId);
            const auto next = nextLinks.find(linkId);
            const auto* nextLink = next == nextLinks.end() ? nullptr : next->second;
            const bool incrementalClient =
                !plan.restartLinks.contains(linkId) &&
                old != definitions_.end() && nextLink &&
                old->second.mode == "TCP Client" &&
                nextLink->mode == "TCP Client" &&
                old->second.protocol == nextLink->protocol;

            if (incrementalClient) {
                definitions_.insert_or_assign(linkId, *nextLink);
                reconcileClientTargets(*nextLink, plan.restartClientTargets);
                continue;
            }

            stopLinks({ linkId });
            definitions_.erase(linkId);
            if (nextLink) {
                definitions_.emplace(linkId, *nextLink);
                startDefinition(*nextLink);
            }
        }
    }

    void stop() noexcept {
        if (stopped_) {
            return;
        }
        stopped_ = true;
        stopTransports();
        definitions_.clear();
    }

    void close(std::string_view connectionId, std::string reason = "local_closed") {
        const auto current = connections_.find(connectionId);
        if (current != connections_.end()) {
            auto connection = current->second;
            connection->close(std::move(reason));
        }
    }

    [[nodiscard]] std::uint64_t advanceSessionEpoch(std::string_view connectionId) {
        if (!connections_.contains(connectionId)) {
            return 0;
        }
        const auto current = connectionEpochs_.find(connectionId);
        if (current == connectionEpochs_.end()) {
            return 0;
        }
        current->second = ++sessionEpoch_;
        return current->second;
    }

    void send(std::string_view connectionId, std::vector<std::uint8_t> bytes, WriteHandler onDone = {}) {
        const auto current = connections_.find(connectionId);
        if (current == connections_.end()) {
            if (onDone) {
                onDone(false);
            }
            return;
        }
        current->second->send(std::move(bytes), std::move(onDone));
    }

    [[nodiscard]] std::vector<std::string> connectionIds(std::string_view linkId) const {
        std::vector<std::string> result;
        for (const auto& [connectionId, ownerLinkId] : connectionLinks_) {
            if (ownerLinkId == linkId) {
                result.push_back(connectionId);
            }
        }
        return result;
    }

    [[nodiscard]] std::string connectionId(std::string_view linkId, std::string_view targetId) const {
        const auto current = clients_.find(std::string(linkId) + ':' + std::string(targetId));
        return current == clients_.end() ? std::string{} : current->second->connectionId;
    }

    // Revoke only the exact target connection generation identified by
    // connectionId.  The token is also used while a lease claim is pending,
    // so lease loss cannot leave a physical socket connected or allow a late
    // callback from this generation to affect a replacement.
    bool revokeTarget(std::string_view connectionId, std::string reason = "target_ownership_lost") {
        for (const auto& [key, client] : clients_) {
            (void)key;
            if (client->connectionId == connectionId) {
                client->revoke(std::move(reason));
                return true;
            }
        }
        return false;
    }

  private:
    struct Failure {
        std::string_view reason;
        std::string_view message;
        bool retry = true;
    };

    static Failure classify(const std::error_code& error) {
        if (error == asio::error::eof) {
            return { "remote_closed", "远端已关闭连接", true };
        }
        if (error == asio::error::connection_refused) {
            return { "connection_refused", "目标拒绝连接", true };
        }
        if (error == asio::error::connection_reset) {
            return { "connection_reset", "连接被远端重置", true };
        }
        if (error == asio::error::timed_out) {
            return { "timed_out", "连接超时", true };
        }
        if (error == asio::error::host_unreachable) {
            return { "host_unreachable", "目标主机不可达", true };
        }
        if (error == asio::error::network_unreachable) {
            return { "network_unreachable", "目标网络不可达", true };
        }
        if (error == asio::error::invalid_argument) {
            return { "invalid_address", "目标地址无效", false };
        }
        if (error == asio::error::operation_aborted) {
            return { "local_closed", "本地已关闭连接", false };
        }
        return { "transport_error", "链路传输异常", true };
    }

    class Connection final : public std::enable_shared_from_this<Connection> {
      public:
        using BytesHandler =
            std::function<void(std::string_view, std::string_view, std::vector<std::uint8_t>)>;
        using CloseHandler = std::function<void(std::string_view, std::string_view)>;

        Connection(asio::ip::tcp::socket socket, std::string id, std::string remote, BytesHandler onBytes, CloseHandler onClose)
            : socket_(std::move(socket)), id_(std::move(id)), remote_(std::move(remote)),
              onBytes_(std::move(onBytes)), onClose_(std::move(onClose)) {}

        [[nodiscard]] const std::string& id() const noexcept { return id_; }

        [[nodiscard]] const std::string& remote() const noexcept { return remote_; }

        void start() { read(); }

        void close(std::string reason) { finish(std::move(reason)); }

        void send(std::vector<std::uint8_t> bytes, WriteHandler handler) {
            if (closed_) {
                if (handler) {
                    handler(false);
                }
                return;
            }
            writes_.push_back({ std::move(bytes), std::move(handler) });
            if (!writing_) {
                writeNext();
            }
        }

      private:
        struct PendingWrite {
            std::vector<std::uint8_t> bytes;
            WriteHandler handler;
        };

        void read() {
            socket_.async_read_some(asio::buffer(readBuffer_), [self = shared_from_this()](const std::error_code& error, std::size_t size) {
                if (self->closed_) {
                    return;
                }
                if (error) {
                    self->finish(std::string(classify(error).reason));
                    return;
                }
                std::vector<std::uint8_t> bytes(
                    self->readBuffer_.begin(),
                    self->readBuffer_.begin() + static_cast<std::ptrdiff_t>(size)
                );
                self->onBytes_(self->id_, self->remote_, std::move(bytes));
                self->read();
            });
        }

        void writeNext() {
            if (closed_ || writes_.empty()) {
                writing_ = false;
                return;
            }
            writing_ = true;
            asio::async_write(socket_, asio::buffer(writes_.front().bytes), [self = shared_from_this()](const std::error_code& error, std::size_t) {
                if (self->writes_.empty()) {
                    self->writing_ = false;
                    return;
                }
                auto handler = std::move(self->writes_.front().handler);
                self->writes_.pop_front();
                if (handler) {
                    handler(!error && !self->closed_);
                }
                self->writing_ = false;
                if (self->closed_) {
                    return;
                }
                if (error) {
                    self->finish(std::string(classify(error).reason));
                    return;
                }
                self->writeNext();
            });
        }

        void finish(std::string reason) {
            if (closed_) {
                return;
            }
            closed_ = true;
            std::error_code ignored;
            socket_.cancel(ignored);
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
            socket_.close(ignored);
            // The active async_write borrows the first buffer until its completion callback.
            // Keep that entry alive and let the callback fail it exactly once; queued entries
            // after it have not started and can be failed immediately.
            const auto firstQueued = writing_ && !writes_.empty() ? std::next(writes_.begin())
                                                                  : writes_.begin();
            for (auto pending = firstQueued; pending != writes_.end(); ++pending) {
                if (pending->handler) {
                    pending->handler(false);
                }
            }
            writes_.erase(firstQueued, writes_.end());
            onClose_(id_, reason);
        }

        asio::ip::tcp::socket socket_;
        std::string id_;
        std::string remote_;
        BytesHandler onBytes_;
        CloseHandler onClose_;
        std::array<std::uint8_t, 16384> readBuffer_{};
        std::deque<PendingWrite> writes_;
        bool writing_ = false;
        bool closed_ = false;
    };

    class Listener final : public std::enable_shared_from_this<Listener> {
      public:
        using AcceptHandler = std::function<void(asio::ip::tcp::socket, std::string)>;

        Listener(asio::io_context& ioContext, const LinkDefinition& link, AcceptHandler onAccept)
            : acceptor_(ioContext), onAccept_(std::move(onAccept)) {
            const auto address = asio::ip::make_address("0.0.0.0");
            const asio::ip::tcp::endpoint endpoint(address, link.port);
            acceptor_.open(endpoint.protocol());
            acceptor_.set_option(asio::socket_base::reuse_address(true));
#if defined(SO_REUSEPORT) && !defined(_WIN32)
            const int enabled = 1;
            if (::setsockopt(acceptor_.native_handle(), SOL_SOCKET, SO_REUSEPORT, &enabled, sizeof(enabled)) != 0) {
                throw std::system_error(errno, std::generic_category(), "SO_REUSEPORT");
            }
#endif
            acceptor_.bind(endpoint);
            acceptor_.listen(asio::socket_base::max_listen_connections);
        }

        void start() { accept(); }

        void stop() noexcept {
            std::error_code ignored;
            acceptor_.cancel(ignored);
            acceptor_.close(ignored);
        }

      private:
        void accept() {
            acceptor_.async_accept([self = shared_from_this()](const std::error_code& error, asio::ip::tcp::socket socket) {
                if (!error) {
                    std::string remote;
                    std::error_code endpointError;
                    const auto endpoint = socket.remote_endpoint(endpointError);
                    if (!endpointError) {
                        remote = endpoint.address().to_string() + ':' +
                            std::to_string(endpoint.port());
                    }
                    self->onAccept_(std::move(socket), std::move(remote));
                }
                if (self->acceptor_.is_open()) {
                    self->accept();
                }
            });
        }

        asio::ip::tcp::acceptor acceptor_;
        AcceptHandler onAccept_;
    };

    struct ClientTarget : std::enable_shared_from_this<ClientTarget> {
        struct Attempt final {
            std::string token;
            std::shared_ptr<asio::ip::tcp::socket> socket;
            std::shared_ptr<asio::ip::tcp::resolver> resolver;
            bool claimResponseReceived = false;
            bool leaseHeld = false;
            bool releaseRequested = false;
            bool cancelled = false;
        };

        TcpTransport& owner;
        LinkDefinition link;
        LinkTargetDefinition target;
        DeadlineScheduler::Token reconnectToken = 0;
        std::string connectionId;
        std::shared_ptr<Attempt> attempt;
        bool stopped = false;
        bool failed = false;
        std::string state = "connecting";
        std::string reason;
        std::string error;
        std::int64_t lastActivityAtMs = 0;

        ClientTarget(TcpTransport& ownerValue, LinkDefinition linkValue, LinkTargetDefinition targetValue)
            : owner(ownerValue), link(std::move(linkValue)), target(std::move(targetValue)) {}

        [[nodiscard]] std::string key() const { return link.id + ':' + target.id; }

        [[nodiscard]] std::string endpoint() const {
            return target.ip + ':' + std::to_string(target.port);
        }

        void start() { connect(); }

        void stop() {
            stopped = true;
            if (reconnectToken != 0) {
                owner.scheduler_.cancel(reconnectToken);
                reconnectToken = 0;
            }
            const auto currentAttempt = attempt;
            if (currentAttempt) {
                currentAttempt->cancelled = true;
                closeSocket(currentAttempt);
                release(currentAttempt);
            }
            const auto activeConnection = std::exchange(connectionId, {});
            if (!activeConnection.empty()) {
                owner.close(activeConnection, "local_closed");
            }
            attempt.reset();
        }

        void connect() {
            if (stopped || attempt || !connectionId.empty()) {
                return;
            }
            state = failed ? "reconnecting" : "connecting";
            reason.clear();
            error.clear();
            owner.publish(link.id, failed ? "reconnecting" : "connecting");
            std::error_code addressError;
            const auto address = asio::ip::make_address(target.ip, addressError);
            if (addressError && link.protocol != "MQTT") {
                fail(addressError);
                return;
            }

            // Claim the target before opening a physical socket. Two Collector
            // Workers may race here, but only the lease owner is allowed to
            // reach the device and become its active connection.
            const auto currentAttempt = std::make_shared<Attempt>();
            currentAttempt->token = service::utils::nextUuid(owner.uuidState_);
            attempt = currentAttempt;
            connectionId = currentAttempt->token;
            const auto claimCompleted = [self = shared_from_this(), currentAttempt](bool claimed) {
                self->onClaimCompleted(currentAttempt, claimed);
            };
            if (owner.onTargetClaim_) {
                owner.onTargetClaim_(link.id, target.id, currentAttempt->token, std::move(claimCompleted));
            } else {
                claimCompleted(true);
            }
        }

        void onClaimCompleted(const std::shared_ptr<Attempt>& currentAttempt, bool claimed) {
            if (currentAttempt->claimResponseReceived) {
                return;
            }
            currentAttempt->claimResponseReceived = true;
            currentAttempt->leaseHeld = claimed;
            const bool current = attempt == currentAttempt && connectionId == currentAttempt->token;
            if (!current) {
                if (claimed) {
                    release(currentAttempt, true);
                }
                return;
            }
            if (stopped || currentAttempt->cancelled) {
                if (claimed) {
                    release(currentAttempt, true);
                }
                clearAttempt(currentAttempt);
                return;
            }
            if (!claimed) {
                clearAttempt(currentAttempt);
                failed = true;
                state = "reconnecting";
                reason = "target_owned";
                error = "目标已由其他 Collector Worker 持有";
                owner.publish(link.id, "reconnecting", reason, error);
                scheduleReconnect();
                return;
            }

            state = "connecting";
            reason.clear();
            error.clear();
            currentAttempt->socket = std::make_shared<asio::ip::tcp::socket>(owner.ioContext_);
            std::error_code addressError;
            const auto address = asio::ip::make_address(target.ip, addressError);
            if (addressError && link.protocol != "MQTT") {
                fail(currentAttempt, addressError);
                return;
            }
            const auto connected = [self = shared_from_this(), currentAttempt](const std::error_code& connectError) {
                if (self->attempt != currentAttempt || self->connectionId != currentAttempt->token) {
                    self->closeSocket(currentAttempt);
                    if (currentAttempt->leaseHeld) {
                        self->release(currentAttempt);
                    }
                    return;
                }
                if (connectError) {
                    self->fail(currentAttempt, connectError);
                    return;
                }
                if (self->stopped || currentAttempt->cancelled) {
                    self->closeSocket(currentAttempt);
                    self->release(currentAttempt);
                    self->clearAttempt(currentAttempt);
                    return;
                }
                self->state = "connected";
                self->reason.clear();
                self->error.clear();
                auto socket = std::move(*currentAttempt->socket);
                currentAttempt->socket.reset();
                self->owner.addConnection(self->link, self->target.id, currentAttempt->token, self->endpoint(), std::move(socket));
                self->owner.publish(self->link.id, "connected");
            };
            if (!addressError) {
                currentAttempt->socket->async_connect({address, target.port}, connected);
            } else {
                currentAttempt->resolver = std::make_shared<asio::ip::tcp::resolver>(owner.ioContext_);
                currentAttempt->resolver->async_resolve(target.ip, std::to_string(target.port), [self = shared_from_this(), currentAttempt, connected](const std::error_code& error, asio::ip::tcp::resolver::results_type endpoints) {
                    if (self->attempt != currentAttempt || self->stopped || currentAttempt->cancelled) return;
                    if (error) { self->fail(currentAttempt, error); return; }
                    asio::async_connect(*currentAttempt->socket, endpoints, [connected](const std::error_code& error, const auto&) { connected(error); });
                });
            }
        }

        void fail(const std::error_code& error) {
            fail({}, error);
        }

        void fail(const std::shared_ptr<Attempt>& currentAttempt, const std::error_code& error) {
            if (stopped || (currentAttempt && (attempt != currentAttempt || connectionId != currentAttempt->token))) {
                return;
            }
            closeSocket(currentAttempt);
            if (currentAttempt) {
                release(currentAttempt);
                clearAttempt(currentAttempt);
            }
            const auto failure = classify(error);
            failed = true;
            state = failure.retry ? "reconnecting" : "error";
            reason = failure.reason;
            this->error = failure.message;
            owner.publish(link.id, failure.retry ? "reconnecting" : "error", failure.reason, failure.message);
            if (!failure.retry) {
                return;
            }
            scheduleReconnect();
        }

        void revoke(std::string reason) {
            if (stopped || !attempt) {
                return;
            }
            const auto currentAttempt = attempt;
            currentAttempt->cancelled = true;
            release(currentAttempt);
            const bool physicalConnection = owner.connections_.contains(currentAttempt->token);
            if (physicalConnection) {
                owner.close(currentAttempt->token, std::move(reason));
                return;
            }
            closeSocket(currentAttempt);
            clearAttempt(currentAttempt);
            failed = true;
            state = "reconnecting";
            this->reason = std::move(reason);
            error.clear();
            owner.publish(link.id, "reconnecting", this->reason);
            scheduleReconnect();
        }

      private:
        static void closeSocket(const std::shared_ptr<Attempt>& currentAttempt) noexcept {
            if (currentAttempt && currentAttempt->resolver) currentAttempt->resolver->cancel();
            if (!currentAttempt || !currentAttempt->socket) {
                return;
            }
            std::error_code ignored;
            currentAttempt->socket->cancel(ignored);
            currentAttempt->socket->close(ignored);
        }

        void release(const std::shared_ptr<Attempt>& currentAttempt, bool force = false) {
            if (!currentAttempt || (!force && currentAttempt->releaseRequested)) {
                return;
            }
            currentAttempt->releaseRequested = true;
            if (owner.onTargetRelease_) {
                owner.onTargetRelease_(link.id, target.id, currentAttempt->token);
            }
        }

        void clearAttempt(const std::shared_ptr<Attempt>& currentAttempt) {
            if (attempt != currentAttempt) {
                return;
            }
            attempt.reset();
            connectionId.clear();
        }

        void scheduleReconnect() {
            if (stopped || reconnectToken != 0) {
                return;
            }
            reconnectToken = owner.scheduler_.scheduleAfter(std::chrono::seconds(2), [self = shared_from_this()] {
                self->reconnectToken = 0;
                self->connect();
            });
        }
    };

    void startServer(const LinkDefinition& link) {
        try {
            auto listener = std::make_shared<Listener>(
                ioContext_,
                link,
                [this, link](asio::ip::tcp::socket socket, std::string remote) {
                    const auto definition = definitions_.find(link.id);
                    if (stopped_ || definition == definitions_.end() ||
                        definition->second.mode != "TCP Server" ||
                        definition->second.status != "enabled") {
                        return;
                    }
                    addConnection(definition->second, {}, service::utils::nextUuid(uuidState_), std::move(remote), std::move(socket));
                }
            );
            listeners_.emplace(link.id, listener);
            listener->start();
            publish(link.id, "listening");
        } catch (const std::system_error& error) {
            std::string_view reason = "listener_start_failed";
#if defined(_WIN32)
            // Windows has no SO_REUSEPORT equivalent. Every worker still attempts to
            // own its listener, and workers that cannot bind report the platform limit
            // instead of silently becoming idle.
            if (workerCount_ > 1 && error.code() == asio::error::address_in_use) {
                reason = "listener_port_reuse_unavailable";
            }
#endif
            publish(link.id, "error", reason, error.what());
        } catch (const std::exception& error) {
            publish(link.id, "error", "listener_start_failed", error.what());
        }
    }

    void startDefinition(const LinkDefinition& link) {
        if (link.status != "enabled") {
            publish(link.id, "stopped");
            return;
        }
        if (link.mode == "TCP Server") {
            startServer(link);
        } else if (link.mode == "TCP Client") {
            startClients(link);
        } else {
            publish(link.id, "error", "unsupported_link_mode", "不支持的链路模式");
        }
    }

    void startClients(const LinkDefinition& link) {
        bool started = false;
        for (const auto& target : link.targets) {
            if (target.status != "enabled") {
                continue;
            }
            started = true;
            startClient(link, target);
        }
        if (!started) {
            publish(link.id, "idle");
        }
    }

    void startClient(const LinkDefinition& link, const LinkTargetDefinition& target) {
        auto client = std::make_shared<ClientTarget>(*this, link, target);
        clients_.insert_or_assign(client->key(), client);
        client->start();
    }

    void reconcileClientTargets(
        const LinkDefinition& link,
        const std::set<ClientTargetKey>& restartClientTargets
    ) {
        std::map<std::string, LinkTargetDefinition, std::less<>> desired;
        if (link.status == "enabled") {
            for (const auto& target : link.targets) {
                if (target.status != "enabled") {
                    continue;
                }
                desired.insert_or_assign(target.id, target);
            }
        }

        for (auto current = clients_.begin(); current != clients_.end();) {
            auto& client = current->second;
            if (client->link.id != link.id) {
                ++current;
                continue;
            }
            const auto target = desired.find(client->target.id);
            const bool forced =
                restartClientTargets.contains({ link.id, client->target.id });
            const bool endpointUnchanged =
                target != desired.end() &&
                target->second.ip == client->target.ip &&
                target->second.port == client->target.port;
            if (forced || !endpointUnchanged) {
                client->stop();
                current = clients_.erase(current);
                continue;
            }

            client->link = link;
            client->target = target->second;
            desired.erase(target);
            ++current;
        }

        for (const auto& [targetId, target] : desired) {
            (void)targetId;
            startClient(link, target);
        }

        bool hasAssignedTarget = false;
        bool hasConnectedTarget = false;
        bool hasFailedTarget = false;
        for (const auto& [key, client] : clients_) {
            (void)key;
            if (client->link.id != link.id) {
                continue;
            }
            hasAssignedTarget = true;
            hasConnectedTarget = hasConnectedTarget || connections_.contains(client->connectionId);
            hasFailedTarget = hasFailedTarget || client->failed;
        }
        if (link.status != "enabled") {
            publish(link.id, "stopped");
        } else if (!hasAssignedTarget) {
            publish(link.id, "idle");
        } else if (hasConnectedTarget) {
            publish(link.id, "connected");
        } else {
            publish(link.id, hasFailedTarget ? "reconnecting" : "connecting");
        }
    }

    void addConnection(const LinkDefinition& link, std::string targetId, std::string id, std::string remote, asio::ip::tcp::socket socket) {
        const auto connectionId = id;
        const auto sessionEpoch = ++sessionEpoch_;
        connectionEpochs_[connectionId] = sessionEpoch;
        auto connection = std::make_shared<Connection>(
            std::move(socket),
            id,
            remote,
            [this, linkId = link.id](std::string_view currentId, std::string_view currentRemote, std::vector<std::uint8_t> bytes) {
                auto& activity = lastActivity_[linkId];
                activity = message::utcNowMilliseconds();
                const auto targetId = connectionTargets_.find(std::string(currentId));
                if (targetId != connectionTargets_.end() && !targetId->second.empty()) {
                    const auto client = clients_.find(linkId + ':' + targetId->second);
                    if (client != clients_.end()) {
                        client->second->lastActivityAtMs = activity;
                    }
                }
                IngressPacket packet{ .messageId = service::utils::nextUuid(uuidState_),
                                      .linkId = linkId,
                                      .connectionId = std::string(currentId),
                                      .remoteAddress = std::string(currentRemote),
                                      .sessionEpoch = connectionEpochs_.at(
                                          std::string(currentId)
                                      ),
                                      .occurredAtMs = activity,
                                      .payload = std::move(bytes) };
                onPacket_(std::move(packet));
                publish(linkId, definitions_[linkId].mode == "TCP Server" ? "listening" : "connected");
            },
            [this](std::string_view currentId, std::string_view reason) {
                removeConnection(currentId, reason, true);
            }
        );
        connections_.emplace(connectionId, connection);
        connectionLinks_[connectionId] = link.id;
        connectionTargets_[connectionId] = targetId;
        onConnected_({ .connectionId = connectionId, .linkId = link.id, .remoteAddress = remote, .targetId = std::move(targetId), .sessionEpoch = sessionEpoch });
        publish(link.id, link.mode == "TCP Server" ? "listening" : "connected");
        connection->start();
    }

    void removeConnection(std::string_view id, std::string_view reason, bool notifyClient) {
        const auto current = connections_.find(id);
        if (current == connections_.end()) {
            return;
        }
        const auto linkId = connectionLinks_[current->first];
        const auto targetId = connectionTargets_[current->first];
        connections_.erase(current);
        connectionLinks_.erase(std::string(id));
        connectionTargets_.erase(std::string(id));
        onDisconnected_(std::string(id), std::string(reason));
        connectionEpochs_.erase(std::string(id));
        publish(linkId, definitions_[linkId].mode == "TCP Server" ? "listening" : "reconnecting", reason);
        if (notifyClient && !targetId.empty()) {
            const auto client = clients_.find(linkId + ':' + targetId);
            if (client != clients_.end()) {
                if (client->second->attempt && client->second->attempt->token == id) {
                    client->second->attempt->cancelled = true;
                    if (!client->second->attempt->releaseRequested) {
                        client->second->attempt->releaseRequested = true;
                        if (onTargetRelease_) {
                            onTargetRelease_(linkId, targetId, std::string(id));
                        }
                    }
                    client->second->attempt.reset();
                }
                client->second->connectionId.clear();
                client->second->failed = true;
                if (!client->second->stopped && client->second->reconnectToken == 0) {
                    client->second->reconnectToken = scheduler_.scheduleAfter(
                        std::chrono::seconds(2),
                        [target = client->second] {
                            target->reconnectToken = 0;
                            target->connect();
                        }
                    );
                }
            }
        }
    }

    void publish(std::string_view linkId, std::string state, std::string_view reason = {}, std::string_view error = {}) {
        if (!onState_) {
            return;
        }
        LinkState status{ .linkId = std::string(linkId),
                          .workerIndex = workerIndex_,
                          .state = std::move(state),
                          .reason = std::string(reason),
                          .error = std::string(error),
                          .lastActivityAtMs = lastActivity_[std::string(linkId)] };
        for (const auto& [id, connection] : connections_) {
            const auto owner = connectionLinks_.find(id);
            if (owner != connectionLinks_.end() && owner->second == linkId) {
                status.remoteEndpoints.push_back(connection->remote());
            }
        }
        for (const auto& [key, client] : clients_) {
            (void)key;
            if (client->link.id != linkId) {
                continue;
            }
            status.targets.push_back({ .id = client->target.id, .state = client->state, .reason = client->reason, .error = client->error, .lastActivityAtMs = client->lastActivityAtMs });
        }
        onState_(std::move(status));
    }

    void stopTransports() noexcept {
        for (auto& [id, listener] : listeners_) {
            (void)id;
            listener->stop();
        }
        listeners_.clear();
        for (auto& [id, client] : clients_) {
            (void)id;
            client->stop();
        }
        clients_.clear();
        while (!connections_.empty()) {
            connections_.begin()->second->close("local_closed");
        }
        connectionLinks_.clear();
        connectionTargets_.clear();
    }

    service::common::UuidV7State& uuidState_;
    asio::io_context& ioContext_;
    DeadlineScheduler& scheduler_;
    std::size_t workerIndex_ = 0;
    std::size_t workerCount_ = 1;
    ConnectedHandler onConnected_;
    PacketHandler onPacket_;
    DisconnectedHandler onDisconnected_;
    StateHandler onState_;
    TargetClaimHandler onTargetClaim_;
    TargetReleaseHandler onTargetRelease_;
    std::map<std::string, LinkDefinition, std::less<>> definitions_;
    std::map<std::string, std::shared_ptr<Listener>, std::less<>> listeners_;
    std::map<std::string, std::shared_ptr<ClientTarget>, std::less<>> clients_;
    std::map<std::string, std::shared_ptr<Connection>, std::less<>> connections_;
    std::map<std::string, std::string, std::less<>> connectionLinks_;
    std::map<std::string, std::string, std::less<>> connectionTargets_;
    std::map<std::string, std::uint64_t, std::less<>> connectionEpochs_;
    std::map<std::string, std::int64_t, std::less<>> lastActivity_;
    std::uint64_t sessionEpoch_ = 0;
    bool stopped_ = true;
};

} // namespace service::collector
