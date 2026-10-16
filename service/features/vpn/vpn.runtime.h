#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/model_json.h>
#include <ruvia/web/redis/redis_repository.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/wireguard.h"
#include "service/features/vpn/firewall/firewall.transport.h"
#include "service/features/vpn/vpn.entity.h"
#include "service/features/vpn/vpn.service.h"
#include "service/features/vpn/wireguard/wireguard.transport.h"
#include "service/middleware/log.h"

namespace service::vpn {

// One instance per Service Worker. It serves the "vpn" RPC component on its
// Worker and, when background synchronization is enabled, schedules the two
// background duties without fixed-period polling:
// - desktop session expiry: wake at the nearest session deadline reported by the
//   last demand computation; one Worker claims each deadline;
// - drift correction of the host's WireGuard/nftables state: at most once per
//   kDriftCorrectionPeriod per instance, claimed through the reconciliation lock
//   and the per-instance ReconciliationSchedule lease.
// All members except stop_/stopped_ are touched only on the owning Worker.
class VpnHubRuntime final {
  public:
    static constexpr auto kDriftCorrectionPeriod = std::chrono::minutes(5);
    // Wake slightly after a deadline so the Redis clock has passed it.
    static constexpr auto kExpiryMargin = std::chrono::milliseconds(250);

    VpnHubRuntime(wireguard::HubConfig fallback, std::string platformId)
        : fallback_(std::move(fallback)), platformId_(std::move(platformId)) {}

    VpnHubRuntime(const VpnHubRuntime&) = delete;
    VpnHubRuntime& operator=(const VpnHubRuntime&) = delete;

    ~VpnHubRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker) {
        if (stop_) {
            return;
        }
        stop_ = std::make_unique<ruvia::stop_source>();
        auto ready = std::make_shared<std::promise<void>>();
        auto done = std::make_shared<std::promise<void>>();
        auto started = ready->get_future();
        stopped_ = done->get_future();
        if (!worker.post([this, ready, done](ruvia::web_worker_context& context) {
                       return run(context, ready, done);
                   })
                 .accepted()) {
            stop_.reset();
            stopped_ = {};
            throw std::runtime_error("service rejected VPN runtime");
        }
        started.get();
    }

    void stop() noexcept {
        if (!stop_) {
            return;
        }
        stop_->request_stop();
        if (stopped_.valid()) {
            stopped_.wait();
        }
        stopped_ = {};
        stop_.reset();
    }

    ruvia::task<std::string> executeOperation(ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        namespace contract = service::message::vpn;
        if (stop.stop_requested()) {
            service::common::fail(10004, "VPN background operation cancelled", 503);
        }

        if (operation == contract::kQueueEdgeConfigOperation) {
            const auto request = service::message::rpc::decodeLinePair(payload);
            if (!request || !service::common::isUuid(request->first) || !service::common::isUuid(request->second)) {
                service::common::fail(10002, "VPN Edge 配置请求无效", 400);
            }
            co_await VpnEdgeService::queueEdgeConfig(context, request->first, request->second, platformId_);
            co_return "{}";
        }

        if (operation == contract::kSyncEdgeRoutesOperation) {
            const auto request = ruvia::from_json<contract::EdgeRouteSync>(payload, { .resource_ = context.pool() });
            if (!request || !service::common::isUuid(request->get<"peerId">().view()) ||
                !service::common::isUuid(request->get<"networkId">().view()) ||
                !service::common::isUuid(request->get<"edgeNodeId">().view()) ||
                !service::common::isUuid(request->get<"actorId">().view())) {
                service::common::fail(10002, "VPN 路由同步请求无效", 400);
            }
            co_await VpnEdgeService::syncEdgeRoutes(context, request->get<"peerId">().view(), request->get<"networkId">().view(), request->get<"edgeNodeId">().view(), request->get<"actorId">().view());
            co_return "{}";
        }

        if (operation == contract::kReconcileOperation) {
            const auto result = co_await reconcile(context, Trigger::event);
            if (result.supported && !result.configured && result.code != "hub_config_missing") {
                service::common::fail(21005, "VPN Hub reconciliation failed: " + result.message, 503);
            }
            co_return runtimeStatusJson(result, context.pool());
        }

        if (operation == contract::kRemovePeerOperation) {
            co_await removePeer(context, payload);
            co_return "{}";
        }

        if (operation == contract::kEdgeCapabilityOperation) {
            const auto request = ruvia::from_json<contract::EdgeCapability>(payload, { .resource_ = context.pool() });
            if (!request || !service::common::isUuid(request->get<"nodeId">().view()) ||
                !service::common::wireguard::validManagedKey(request->get<"publicKey">().view())) {
                service::common::fail(10002, "VPN Edge 能力通知无效", 400);
            }
            const std::string nodeId(request->get<"nodeId">().view());
            try {
                co_await activateEdge(context, nodeId, request->get<"publicKey">().view());
            } catch (const std::exception& error) {
                LOG_ERROR << "VPN edge capability for node " << nodeId << " failed: " << error.what();
                throw;
            }
            co_return "{}";
        }

        if (operation == contract::kEdgeConfigResultOperation) {
            const auto request = ruvia::from_json<contract::EdgeConfigResult>(payload, { .resource_ = context.pool() });
            if (!request || !service::common::isUuid(request->get<"peerId">().view())) {
                service::common::fail(10002, "VPN Edge 配置结果无效", 400);
            }
            try {
                co_await VpnEdgeService::applyConfigResult(context, *request);
            } catch (const std::exception& error) {
                LOG_ERROR << "VPN edge config result for peer " << request->get<"peerId">().view()
                          << " failed: " << error.what();
                throw;
            }
            co_return "{}";
        }

        service::common::fail(10002, "Unknown VPN background operation", 400);
    }

  private:
    enum class Trigger { event,
                         drift };

    // Activation is committed first, so the Hub must learn the reported key even
    // when a later route synchronization fails; that failure is rethrown after.
    ruvia::task<void> activateEdge(ruvia::web_worker_context& context, std::string_view nodeId, std::string_view publicKey) {
        const auto peers = co_await VpnEdgeService::activateEdgePeers(context, nodeId, publicKey);
        std::exception_ptr failure;
        try {
            for (const auto& peer : peers) {
                co_await VpnEdgeService::syncEdgeRoutes(context, peer.peerId, peer.networkId, nodeId, peer.networkOwner);
                co_await VpnEdgeService::queueEdgeConfig(context, peer.peerId);
            }
        } catch (...) {
            failure = std::current_exception();
        }
        if (!peers.empty()) {
            const auto status = co_await reconcile(context, Trigger::event);
            if (status.supported && !status.configured && status.code != "hub_config_missing") {
                LOG_WARN << "VPN Hub reconciliation after edge capability of node " << nodeId
                         << " is incomplete: " << status.code << ' ' << status.message;
            }
        }
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    // The kernel interface is a per-host singleton shared by every Worker and
    // every instance on the host. kVpnReconciliation serializes all changes to
    // it; drift correction additionally skips when this instance reconciled
    // within kDriftCorrectionPeriod. The lock runs on a separate Worker-local
    // connection so business queries keep their ordinary connection.
    ruvia::task<wireguard::RuntimeStatus> reconcile(ruvia::web_worker_context& context, Trigger trigger) {
        const bool drift = trigger == Trigger::drift;
        if (drift && co_await VpnRuntimeService::reconciliationCurrent(context)) {
            co_return wireguard::RuntimeStatus{ .code = "reconciliation_current" };
        }
        auto ownership = co_await VpnRuntimeService::claimHub(context, !drift);
        if (!ownership) {
            co_return wireguard::RuntimeStatus{ .code = "reconciliation_in_progress" };
        }
        if (drift && co_await VpnRuntimeService::reconciliationCurrent(context)) {
            co_return wireguard::RuntimeStatus{ .code = "reconciliation_current" };
        }
        // Keep address allocation behind the complete kernel reconciliation.
        // Otherwise an older snapshot could install a revoked key after its
        // address has already been returned to a newly enrolled client.
        co_await VpnRuntimeService::lockAddressAllocation(*ownership, context.pool());
        scheduleDesktopExpiry(co_await VpnEdgeService::synchronizeEdgeDemand(context));
        auto result = co_await applyHub(context);
        if (result.configured) {
            co_await VpnRuntimeService::recordReconciliation(
                context,
                std::chrono::duration_cast<std::chrono::milliseconds>(kDriftCorrectionPeriod)
            );
        }
        co_await ownership->commit();
        co_return result;
    }

    ruvia::task<void> removePeer(ruvia::web_worker_context& context, std::string_view publicKey) {
        // Format-only validation: a key already on the interface must stay removable.
        if (!service::common::wireguard::validKey(publicKey)) {
            service::common::fail(10002, "WireGuard Peer 公钥无效", 400);
        }
        auto ownership = co_await VpnRuntimeService::claimHub(context, true);
        const auto config = co_await VpnHubKeyService::loadOrInitialize(context, fallback_);
        if (config) {
            const auto result = wireguard::controller().removePeer(*config, publicKey);
            if (result.supported && !result.configured) {
                service::common::fail(21005, "VPN Hub peer removal failed: " + result.message, 503);
            }
        }
        co_await ownership->commit();
    }

    static void appendList(std::string_view text, std::vector<std::string>& output) {
        while (!text.empty()) {
            const auto separator = text.find(',');
            auto item = text.substr(0, separator);
            text = separator == std::string_view::npos ? std::string_view{} : text.substr(separator + 1);
            if (!item.empty() && item.front() == ' ') {
                item.remove_prefix(1);
            }
            if (!item.empty()) {
                output.emplace_back(item);
            }
        }
    }

    // Apply the database view of active peers to the kernel interface and the
    // firewall, then record the handshakes the interface reports.
    ruvia::task<wireguard::RuntimeStatus> applyHub(ruvia::web_worker_context& context) {
        const auto config = co_await VpnHubKeyService::loadOrInitialize(context, fallback_);
        if (!config) {
            co_return wireguard::RuntimeStatus{ .supported = true,
                                                .configured = false,
                                                .code = "hub_config_missing",
                                                .message = "WireGuard Hub 配置尚未初始化",
                                                .peerCount = 0 };
        }
        auto& controller = wireguard::controller();
        auto result = controller.configure(*config);
        if (!result.configured) {
            co_return result;
        }
        const auto peers = co_await VpnRuntimeService::loadActivePeers(context);
        // Remove obsolete keys before assigning a recycled address to a new peer.
        // A failed inventory or removal must not grant access to a stale key.
        const auto currentPeers = controller.peerKeys(*config);
        if (!currentPeers) {
            co_return wireguard::RuntimeStatus{ .supported = true,
                                                .configured = false,
                                                .code = "peer_inventory_failed",
                                                .message = "Unable to read WireGuard peers before reconciliation" };
        }
        // Only managed keys are granted Hub access: an all-zero key can never
        // complete a handshake and would only hold an address and its routes.
        const auto grantable = [](const VpnRuntimeService::Peer& peer) {
            return service::common::wireguard::validManagedKey(peer.publicKey) && parseIpv4(peer.assignedIpv4);
        };
        std::unordered_set<std::string> expected;
        for (const auto& peer : peers) {
            if (grantable(peer)) {
                expected.insert(peer.publicKey);
            }
        }
        for (const auto& publicKey : *currentPeers) {
            if (!expected.contains(publicKey)) {
                const auto removed = controller.removePeer(*config, publicKey);
                if (!removed.configured) {
                    co_return removed;
                }
            }
        }
        std::vector<firewall::ClientAccess> clients;
        std::vector<wireguard::Peer> configured;
        std::vector<std::string> expectedRoutes;
        for (const auto& record : peers) {
            if (!grantable(record)) {
                continue;
            }
            wireguard::Peer peer;
            peer.publicKey = record.publicKey;
            peer.allowedIps.emplace_back(record.assignedIpv4 + "/32");
            if (record.peerType == "edge") {
                firewall::ClientAccess client{ .assignedIpv4 = record.assignedIpv4 };
                appendList(record.sourceRoutes, client.sourceRoutes);
                peer.allowedIps.insert(peer.allowedIps.end(), client.sourceRoutes.begin(), client.sourceRoutes.end());
                appendList(record.allowedRoutes, client.allowedRoutes);
                clients.push_back(std::move(client));
            } else if (record.peerType == "windows") {
                firewall::ClientAccess client{ .assignedIpv4 = record.assignedIpv4 };
                appendList(record.allowedRoutes, client.allowedRoutes);
                appendList(record.edgeAddresses, client.edgeAddresses);
                clients.push_back(std::move(client));
            }
            expectedRoutes.insert(expectedRoutes.end(), peer.allowedIps.begin(), peer.allowedIps.end());
            configured.push_back(std::move(peer));
        }

        const auto firewallResult = firewall::apply(config->interfaceName, clients);
        if (!firewallResult.configured) {
            co_return wireguard::RuntimeStatus{ .supported = true,
                                                .configured = false,
                                                .code = "firewall_configure_failed",
                                                .message = firewallResult.message,
                                                .peerCount = 0 };
        }
        // Install the new address permissions before enabling its new key.
        // Recycled addresses must never inherit a previous client's firewall access.
        std::size_t configuredPeers = 0;
        for (const auto& peer : configured) {
            const auto peerResult = controller.upsertPeer(*config, peer);
            if (!peerResult.configured) {
                co_return peerResult;
            }
            ++configuredPeers;
        }
        const auto routeResult = controller.reconcileRoutes(*config, expectedRoutes);
        if (!routeResult.configured) {
            co_return routeResult;
        }
        if (const auto handshakes = controller.peerHandshakes(*config)) {
            for (const auto& [publicKey, seconds] : *handshakes) {
                if (seconds == 0 || seconds > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
                    continue;
                }
                co_await VpnRuntimeService::updatePeerHandshake(context, publicKey, static_cast<std::int64_t>(seconds));
            }
        }
        result.peerCount = configuredPeers;
        result.message = "WireGuard hub is configured";
        co_return result;
    }

    // Adopt the nearest desktop deadline of a fresh, complete demand view and
    // let the scheduler re-arm its sleep.
    void scheduleDesktopExpiry(const DesktopDemand& demand) {
        if (demand.nextExpiryMs) {
            desktopExpiryMs_ = *demand.nextExpiryMs;
            desktopExpiryAt_ = std::chrono::steady_clock::now() +
                std::chrono::milliseconds(*demand.nextExpiryMs - demand.redisNowMs) + kExpiryMargin;
        } else {
            desktopExpiryMs_.reset();
        }
        if (wake_) {
            wake_->request_stop();
        }
    }

    // A lapsed desktop session can shrink the Edge demand. Exactly one Worker
    // claims each deadline and keeps tracking the next one; the others stop
    // tracking until an event or a drift run hands them a new view.
    ruvia::task<void> expireDesktopSessions(ruvia::web_worker_context& context) {
        const auto deadline = *desktopExpiryMs_;
        desktopExpiryMs_.reset();
        if (!(co_await VpnRuntimeService::claimDesktopExpiry(context, deadline))) {
            co_return;
        }
        auto ownership = co_await VpnRuntimeService::claimHub(context, true);
        scheduleDesktopExpiry(co_await VpnEdgeService::synchronizeEdgeDemand(context));
        co_await ownership->commit();
    }

    ruvia::task<void> run(ruvia::web_worker_context& context, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> done) {
        const auto stop = ruvia::combine_stop_tokens(context.get_stop_token(), stop_->token());
        ready->set_value();
        // Reconcile at start-up: the interface may not exist yet on this host.
        auto nextDrift = std::chrono::steady_clock::now();
        while (!stop.stop_requested()) {
            if (std::chrono::steady_clock::now() >= nextDrift) {
                nextDrift = std::chrono::steady_clock::now() + kDriftCorrectionPeriod;
                try {
                    const auto status = co_await reconcile(context, Trigger::drift);
                    if (status.supported && !status.configured && status.code != "hub_config_missing") {
                        LOG_WARN << "VPN drift correction is incomplete: " << status.code << ' ' << status.message;
                    }
                } catch (const std::exception& error) {
                    LOG_ERROR << "VPN drift correction failed: " << error.what();
                }
            }
            if (desktopExpiryMs_ && std::chrono::steady_clock::now() >= desktopExpiryAt_) {
                try {
                    co_await expireDesktopSessions(context);
                } catch (const std::exception& error) {
                    LOG_ERROR << "VPN desktop session expiry failed: " << error.what();
                }
            }
            if (stop.stop_requested()) {
                break;
            }
            auto wakeAt = nextDrift;
            if (desktopExpiryMs_) {
                wakeAt = std::min(wakeAt, desktopExpiryAt_);
            }
            wake_ = std::make_unique<ruvia::stop_source>();
            const auto delay = std::max(wakeAt - std::chrono::steady_clock::now(), std::chrono::steady_clock::duration::zero());
            (void)co_await ruvia::sleep_for(context.worker(), delay, ruvia::combine_stop_tokens(stop, wake_->token()));
        }
        wake_.reset();
        done->set_value();
    }

    static std::string runtimeStatusJson(const wireguard::RuntimeStatus& result, std::pmr::memory_resource* resource) {
        service::message::vpn::RuntimeStatus status(ruvia::model_options{ .resource_ = resource });
        status.set<"platformSupported">(result.supported)
            .set<"configured">(result.configured)
            .set<"code">(result.code)
            .set<"message">(result.message)
            .set<"runtimePeerCount">(static_cast<std::int64_t>(result.peerCount));
        return service::message::rpc::encode(status, resource);
    }

    const wireguard::HubConfig fallback_;
    const std::string platformId_;
    std::unique_ptr<ruvia::stop_source> stop_;
    std::future<void> stopped_;
    std::unique_ptr<ruvia::stop_source> wake_;
    std::optional<std::int64_t> desktopExpiryMs_;
    std::chrono::steady_clock::time_point desktopExpiryAt_{};
};

} // namespace service::vpn
