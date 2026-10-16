#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <edge.pb.h>
#include <memory>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_transaction.h>
#include <ruvia/web/redis/redis_repository.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/wireguard.h"
#include "service/features/edge/edge.config.h"
#include "service/features/edge/edge.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/projection/projection.entity.h"
#include "service/features/edge/session/session.service.h"
#include "service/features/vpn/vpn.entity.h"
#include "service/features/vpn/wireguard/wireguard.config.h"
#include "service/utils/network.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"

namespace service::vpn {

using service::message::vpn::kVirtualLanPool;
using service::message::vpn::mappedVirtualCidr;
using service::utils::network::hostAddress;
using service::utils::network::Ipv4Cidr;
using service::utils::network::isPrivateIpv4;
using service::utils::network::networkCidr;
using service::utils::network::parseCidr;
using service::utils::network::parseIpv4;

// Newest connection deadline of one desktop client, measured on the Redis clock.
inline constexpr std::string_view kDesktopSessionDeadlineScript = R"lua(
local time=redis.call('TIME')
local now=time[1]*1000+math.floor(time[2]/1000)
local last=redis.call('ZRANGE',KEYS[1],-1,-1,'WITHSCORES')
if last[2]==nil then return {now,0} end
return {now,tonumber(last[2])}
)lua";

// Claim one desktop-session deadline: only the first Worker to present a deadline
// later than every deadline already handled processes the expiry.
inline constexpr std::string_view kDesktopExpiryClaimScript = R"lua(
local handled=tonumber(redis.call('GET',KEYS[1]) or '0')
if tonumber(ARGV[1])<=handled then return 0 end
redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2])
return 1
)lua";

namespace feature_detail {

inline std::string rowText(const auto& row, std::size_t index) {
    return std::string(row[index].value().value_or(std::string_view{}));
}

inline std::int64_t integer(std::string_view value, std::int64_t fallback = 0) {
    return service::utils::parseInt64(
               value.empty() ? std::nullopt : std::optional<std::string_view>(value)
    )
        .value_or(fallback);
}

} // namespace feature_detail

// Edge peers demanded by live desktop sessions, and when that demand may shrink.
struct DesktopDemand final {
    std::unordered_set<std::string> edgePeers;
    // Earliest Redis-clock millisecond at which an active desktop client's
    // newest session lapses. Empty when no desktop session is active.
    std::optional<std::int64_t> nextExpiryMs;
    std::int64_t redisNowMs{};
};

class VpnRuntimeService final {
  public:
    template <typename Context>
    static ruvia::task<DesktopDemand> desktopDemand(Context& context) {
        using Op = ruvia::db_binary_operator;
        using Access = persistence::VpnEffectiveEdgeAccessEntity;
        using Peer = persistence::VpnPeerEntity;
        ruvia::db_query selections;
        selections
            .select({ selections.cast(selections.column(std::tuple_element_t<Access::column_index<"edge_peer_id">(), Access::columns_type>::name.view(), "access"), ruvia::db_data_type::text), selections.cast(selections.column(std::tuple_element_t<Access::column_index<"peer_id">(), Access::columns_type>::name.view(), "access"), ruvia::db_data_type::text) })
            .from(persistence::VpnEffectiveEdgeAccessEntity::table_name(), "access")
            .join(ruvia::db_join_type::inner, persistence::VpnPeerEntity::table_name(), selections.binary(selections.column(std::tuple_element_t<Peer::column_index<"id">(), Peer::columns_type>::name.view(), "peer"), Op::equal, selections.column(std::tuple_element_t<Access::column_index<"peer_id">(), Access::columns_type>::name.view(), "access")), "peer")
            .and_where(selections.column(std::tuple_element_t<Peer::column_index<"client_managed">(), Peer::columns_type>::name.view(), "peer"))
            .and_where(selections.binary(
                selections.column(std::tuple_element_t<Peer::column_index<"peer_type">(), Peer::columns_type>::name.view(), "peer"),
                Op::equal,
                selections.value("windows")
            ));
        const auto rows = co_await context.db().query(selections);
        DesktopDemand demand;
        std::unordered_set<std::string> activeClients, inactiveClients;
        for (const auto& row : rows) {
            const auto edgePeerId = feature_detail::rowText(row, 0);
            const auto clientPeerId = feature_detail::rowText(row, 1);
            if (!activeClients.contains(clientPeerId) && !inactiveClients.contains(clientPeerId)) {
                const auto key = persistence::DesktopSessionSet::key(clientPeerId);
                const std::string_view keys[]{ key };
                const auto reply = co_await context.redis().eval(
                    kDesktopSessionDeadlineScript,
                    keys,
                    std::span<const std::string_view>{}
                );
                if (reply.kind() != ruvia::redis_value::kind_type::array || reply.array().size() != 2 ||
                    reply.array()[0].kind() != ruvia::redis_value::kind_type::integer ||
                    reply.array()[1].kind() != ruvia::redis_value::kind_type::integer) {
                    service::message::redis::throwValue("desktop VPN demand", reply);
                }
                const auto now = reply.array()[0].integer();
                const auto deadline = reply.array()[1].integer();
                demand.redisNowMs = now;
                if (deadline > now) {
                    activeClients.insert(clientPeerId);
                    demand.nextExpiryMs = demand.nextExpiryMs ? std::min(*demand.nextExpiryMs, deadline) : deadline;
                } else {
                    inactiveClients.insert(clientPeerId);
                }
            }
            if (activeClients.contains(clientPeerId)) {
                demand.edgePeers.insert(edgePeerId);
            }
        }
        co_return demand;
    }

    // True for exactly one caller per deadline: the deadline is newer than every
    // deadline already handled by any Service Worker of any instance.
    template <typename Context>
    static ruvia::task<bool> claimDesktopExpiry(Context& context, std::int64_t deadlineMs) {
        const std::string_view keys[]{ persistence::DesktopExpiryClaim::kKey };
        const auto deadline = std::to_string(deadlineMs);
        const auto lifetime = std::to_string(std::chrono::milliseconds(std::chrono::minutes(10)).count());
        const std::string_view arguments[]{ deadline, lifetime };
        const auto reply = co_await context.redis().eval(kDesktopExpiryClaimScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("desktop VPN expiry claim", reply);
        }
        co_return reply.integer() == 1;
    }

    // Ownership of the host's kernel interface: kVpnReconciliation held by a
    // transaction on the Worker-local "vpn-coordination" connection, so business
    // queries keep their ordinary connection without pool self-deadlock. With
    // wait=false the claim fails immediately when another owner holds it.
    // Committing (or destroying) the returned transaction releases ownership.
    template <typename Context>
    static ruvia::task<std::optional<ruvia::db_transaction>> claimHub(Context& context, bool wait) {
        auto ownership = co_await context.db("vpn-coordination").begin_transaction();
        ruvia::db_query lock(context.pool());
        lock.select(lock.call(wait ? "pg_advisory_xact_lock" : "pg_try_advisory_xact_lock", { lock.cast(lock.value(service::message::lock::kVpnReconciliation), ruvia::db_data_type::big_int) }));
        const auto rows = co_await ownership.query(lock);
        if (!wait && (rows.empty() || rows[0][0].value().value_or("") != "t")) {
            co_return std::nullopt;
        }
        co_return std::optional<ruvia::db_transaction>(std::move(ownership));
    }

    // True while this instance's Hub was reconciled within the last lease.
    template <typename Context>
    static ruvia::task<bool> reconciliationCurrent(Context& context) {
        auto schedules = context.redis().template get_repository<persistence::ReconciliationSchedule>();
        const ruvia::redis_find_options current{ .where_ = persistence::ReconciliationSchedule::field<"id">() == service::runtime::instanceId() };
        co_return co_await schedules.exists(current);
    }

    template <typename Context>
    static ruvia::task<void> recordReconciliation(Context& context, std::chrono::milliseconds lease) {
        auto schedules = context.redis().template get_repository<persistence::ReconciliationSchedule>();
        persistence::ReconciliationSchedule schedule(context.pool());
        schedule.set<"id">(service::runtime::instanceId());
        (void)co_await schedules.upsert(schedule, ruvia::redis_write_options{ .ttl_ = lease });
    }

    template <typename Transaction>
    static ruvia::task<void> lockAddressAllocation(Transaction& transaction, std::pmr::memory_resource* resource) {
        ruvia::db_query lock(resource);
        lock.select(lock.call("pg_advisory_xact_lock", { lock.cast(lock.value(service::message::lock::kVpnAddressAllocation), ruvia::db_data_type::big_int) }));
        (void)co_await transaction.query(lock);
    }

    struct Peer final {
        std::string publicKey;
        std::string assignedIpv4;
        std::string peerType;
        std::string sourceRoutes;
        std::string allowedRoutes;
        std::string edgeAddresses;
    };

    template <typename Context>
    static ruvia::task<std::vector<Peer>> loadActivePeers(Context& context) {
        using Op = ruvia::db_binary_operator;
        using Route = persistence::VpnRouteEntity;
        using RouteAccess = persistence::VpnEffectiveRouteAccessEntity;
        using EdgeAccess = persistence::VpnEffectiveEdgeAccessEntity;
        using PeerRow = persistence::VpnPeerEntity;
        using Network = persistence::VpnNetworkEntity;
        ruvia::db_query routes;
        const std::vector<ruvia::db_order_term> routeOrder{
            { routes.column(std::tuple_element_t<Route::column_index<"virtual_cidr">(), Route::columns_type>::name.view(), "r") }
        };
        routes
            .select(routes.aggregate(
                "string_agg",
                { routes.column(std::tuple_element_t<Route::column_index<"virtual_cidr">(), Route::columns_type>::name.view(), "r"),
                  routes.value(", ") },
                false,
                routeOrder
            ))
            .from(persistence::VpnRouteEntity::table_name(), "r")
            .and_where(routes.binary(
                routes.column(std::tuple_element_t<Route::column_index<"edge_peer_id">(), Route::columns_type>::name.view(), "r"),
                Op::equal,
                routes.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "p")
            ))
            .and_where(routes.column(std::tuple_element_t<Route::column_index<"enabled">(), Route::columns_type>::name.view(), "r"));
        ruvia::db_query allowed;
        const std::vector<ruvia::db_order_term> allowedOrder{
            { allowed.column(std::tuple_element_t<RouteAccess::column_index<"virtual_cidr">(), RouteAccess::columns_type>::name.view(), "access") }
        };
        allowed
            .select(allowed.aggregate(
                "string_agg",
                { allowed.column(std::tuple_element_t<RouteAccess::column_index<"virtual_cidr">(), RouteAccess::columns_type>::name.view(), "access"),
                  allowed.value(", ") },
                false,
                allowedOrder
            ))
            .from(persistence::VpnEffectiveRouteAccessEntity::table_name(), "access")
            .and_where(allowed.binary(
                allowed.column(std::tuple_element_t<RouteAccess::column_index<"peer_id">(), RouteAccess::columns_type>::name.view(), "access"),
                Op::equal,
                allowed.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "p")
            ));
        ruvia::db_query addresses;
        const std::vector<ruvia::db_order_term> addressOrder{
            { addresses.column(std::tuple_element_t<EdgeAccess::column_index<"edge_address">(), EdgeAccess::columns_type>::name.view(), "access") }
        };
        addresses
            .select(addresses.aggregate(
                "string_agg",
                { addresses.column(std::tuple_element_t<EdgeAccess::column_index<"edge_address">(), EdgeAccess::columns_type>::name.view(), "access"),
                  addresses.value(", ") },
                false,
                addressOrder
            ))
            .from(persistence::VpnEffectiveEdgeAccessEntity::table_name(), "access")
            .and_where(addresses.binary(
                addresses.column(std::tuple_element_t<EdgeAccess::column_index<"peer_id">(), EdgeAccess::columns_type>::name.view(), "access"),
                Op::equal,
                addresses.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "p")
            ));
        ruvia::db_query peersQuery;
        peersQuery.select(peersQuery.column(std::tuple_element_t<PeerRow::column_index<"public_key">(), PeerRow::columns_type>::name.view(), "p"))
            .add_select(peersQuery.call(
                "host",
                { peersQuery.column(std::tuple_element_t<PeerRow::column_index<"assigned_ipv4">(), PeerRow::columns_type>::name.view(), "p") }
            ))
            .add_select(peersQuery.column(std::tuple_element_t<PeerRow::column_index<"peer_type">(), PeerRow::columns_type>::name.view(), "p"))
            .add_select(peersQuery.coalesce({ peersQuery.subquery(routes), peersQuery.value("") }))
            .add_select(peersQuery.coalesce({ peersQuery.subquery(allowed), peersQuery.value("") }))
            .add_select(peersQuery.coalesce({ peersQuery.subquery(addresses), peersQuery.value("") }))
            .from(persistence::VpnPeerEntity::table_name(), "p")
            .join(ruvia::db_join_type::inner, persistence::VpnNetworkEntity::table_name(), peersQuery.binary(peersQuery.column(std::tuple_element_t<Network::column_index<"id">(), Network::columns_type>::name.view(), "n"), Op::equal, peersQuery.column(std::tuple_element_t<PeerRow::column_index<"network_id">(), PeerRow::columns_type>::name.view(), "p")), "n")
            .and_where(peersQuery.binary(
                peersQuery.column(std::tuple_element_t<PeerRow::column_index<"status">(), PeerRow::columns_type>::name.view(), "p"),
                Op::equal,
                peersQuery.value("active")
            ))
            .and_where(peersQuery.binary(
                peersQuery.column(std::tuple_element_t<Network::column_index<"status">(), Network::columns_type>::name.view(), "n"),
                Op::equal,
                peersQuery.value("enabled")
            ))
            .and_where(peersQuery.unary(
                ruvia::db_unary_operator::is_null,
                peersQuery.column(std::tuple_element_t<Network::column_index<"deleted_at">(), Network::columns_type>::name.view(), "n")
            ))
            .and_where(peersQuery.binary(
                peersQuery.column(std::tuple_element_t<PeerRow::column_index<"public_key">(), PeerRow::columns_type>::name.view(), "p"),
                Op::not_equal,
                peersQuery.value("")
            ))
            .and_where(peersQuery.binary(
                peersQuery.unary(
                    ruvia::db_unary_operator::not_value,
                    peersQuery.column(std::tuple_element_t<PeerRow::column_index<"client_managed">(), PeerRow::columns_type>::name.view(), "p")
                ),
                Op::or_value,
                peersQuery.call(
                    "vpn_desktop_user_authorized",
                    { peersQuery.column(std::tuple_element_t<PeerRow::column_index<"user_id">(), PeerRow::columns_type>::name.view(), "p") }
                )
            ))
            .add_order_by(peersQuery.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "p"));
        const auto rows = co_await context.db().query(peersQuery);
        std::vector<Peer> peers;
        peers.reserve(rows.size());
        for (const auto& row : rows) {
            peers.push_back(Peer{
                feature_detail::rowText(row, 0),
                feature_detail::rowText(row, 1),
                feature_detail::rowText(row, 2),
                feature_detail::rowText(row, 3),
                feature_detail::rowText(row, 4),
                feature_detail::rowText(row, 5),
            });
        }
        co_return peers;
    }

    template <typename Context>
    static ruvia::task<void> updatePeerHandshake(Context& context, std::string_view publicKey, std::int64_t seconds) {
        using PeerRow = persistence::VpnPeerEntity;
        ruvia::db_query handshake;
        const auto timestamp =
            handshake.call("to_timestamp", { handshake.cast(handshake.value(seconds), ruvia::db_data_type::double_value) });
        handshake.update(persistence::VpnPeerEntity::table_name())
            .set(std::tuple_element_t<PeerRow::column_index<"last_handshake_at">(), PeerRow::columns_type>::name.view(), timestamp)
            .set(std::tuple_element_t<PeerRow::column_index<"updated_at">(), PeerRow::columns_type>::name.view(), handshake.call("now"))
            .and_where(handshake.binary(
                handshake.column(std::tuple_element_t<PeerRow::column_index<"public_key">(), PeerRow::columns_type>::name.view()),
                ruvia::db_binary_operator::equal,
                handshake.value(publicKey)
            ))
            .and_where(handshake.binary(
                handshake.column(std::tuple_element_t<PeerRow::column_index<"status">(), PeerRow::columns_type>::name.view()),
                ruvia::db_binary_operator::equal,
                handshake.value("active")
            ))
            .and_where(handshake.binary(
                handshake.unary(ruvia::db_unary_operator::is_null, handshake.column(std::tuple_element_t<PeerRow::column_index<"last_handshake_at">(), PeerRow::columns_type>::name.view())),
                ruvia::db_binary_operator::or_value,
                handshake.binary(
                    handshake.column(std::tuple_element_t<PeerRow::column_index<"last_handshake_at">(), PeerRow::columns_type>::name.view()),
                    ruvia::db_binary_operator::less,
                    timestamp
                )
            ));
        (void)co_await context.db().execute(handshake);
    }
};

// Hub identity stored on the default network row. Key derivation is local
// computation; the row is the shared source for every Worker and instance.
class VpnHubKeyService final {
  public:
    static constexpr std::string_view kDefaultNetworkId{ "00000000-0000-7000-8000-000000000004" };
    static constexpr std::string_view kDefaultNetworkName{ "iot-server" };

    static bool derivePublicKey(std::string_view privateKey, std::string& publicKey) {
        const auto privateBytes = service::common::wireguard::decodeKey(privateKey);
        if (!privateBytes) {
            return false;
        }
        using KeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
        KeyPtr key(EVP_PKEY_new_raw_private_key(EVP_PKEY_X25519, nullptr, privateBytes->data(), privateBytes->size()), &EVP_PKEY_free);
        if (!key) {
            return false;
        }
        service::common::wireguard::Key publicBytes{};
        std::size_t publicSize = publicBytes.size();
        if (EVP_PKEY_get_raw_public_key(key.get(), publicBytes.data(), &publicSize) != 1 || publicSize != publicBytes.size()) {
            return false;
        }
        publicKey = service::common::wireguard::encodeKey(publicBytes);
        return true;
    }

    static bool generateKeyPair(std::string& privateKey, std::string& publicKey) {
        service::common::wireguard::Key privateBytes{};
        if (RAND_bytes(privateBytes.data(), static_cast<int>(privateBytes.size())) != 1) {
            return false;
        }
        privateKey = service::common::wireguard::encodeKey(privateBytes);
        return service::common::wireguard::validManagedKey(privateKey) && derivePublicKey(privateKey, publicKey);
    }

    // The Hub private key must be a managed key: an all-zero key is public
    // knowledge and would let anyone impersonate the Hub.
    template <typename Context>
    static ruvia::task<std::optional<wireguard::HubConfig>> loadOrInitialize(Context& context, const wireguard::HubConfig& fallback) {
        using Network = persistence::VpnNetworkEntity;
        auto transaction = co_await context.db().begin_transaction();
        ruvia::db_query lock;
        lock.select(lock.call("pg_advisory_xact_lock", { lock.cast(lock.value(service::message::lock::kVpnDefaultNetwork), ruvia::db_data_type::big_int) }));
        (void)co_await transaction.query(lock);
        ruvia::db_query network;
        network
            .select({ network.column(std::tuple_element_t<Network::column_index<"hub_private_key">(), Network::columns_type>::name.view()), network.column(std::tuple_element_t<Network::column_index<"hub_public_key">(), Network::columns_type>::name.view()), network.column(std::tuple_element_t<Network::column_index<"hub_endpoint">(), Network::columns_type>::name.view()), network.column(std::tuple_element_t<Network::column_index<"hub_listen_port">(), Network::columns_type>::name.view()) })
            .from(persistence::VpnNetworkEntity::table_name())
            .and_where(network.binary(network.column(std::tuple_element_t<Network::column_index<"id">(), Network::columns_type>::name.view()), ruvia::db_binary_operator::equal, network.cast(network.value(kDefaultNetworkId), ruvia::db_data_type::uuid)))
            .and_where(network.binary(network.column(std::tuple_element_t<Network::column_index<"name">(), Network::columns_type>::name.view()), ruvia::db_binary_operator::equal, network.value(kDefaultNetworkName)))
            .and_where(network.binary(network.column(std::tuple_element_t<Network::column_index<"status">(), Network::columns_type>::name.view()), ruvia::db_binary_operator::equal, network.value("enabled")))
            .and_where(network.unary(ruvia::db_unary_operator::is_null, network.column(std::tuple_element_t<Network::column_index<"deleted_at">(), Network::columns_type>::name.view())))
            .limit(1)
            .lock({ .mode_ = ruvia::db_row_lock::update });
        const auto rows = co_await transaction.query(network);
        if (rows.empty()) {
            co_await transaction.commit();
            if (!service::common::wireguard::validManagedKey(fallback.privateKey)) {
                co_return std::nullopt;
            }
            co_return fallback;
        }

        auto config = fallback;
        const auto storedPrivateKey = feature_detail::rowText(rows.front(), 0);
        const auto storedPublicKey = feature_detail::rowText(rows.front(), 1);
        const auto storedEndpoint = feature_detail::rowText(rows.front(), 2);
        const auto storedPortText = feature_detail::rowText(rows.front(), 3);
        const auto storedPort = service::utils::parseInt64(std::optional<std::string_view>(storedPortText));
        config.endpoint = storedEndpoint.empty() ? fallback.endpoint : storedEndpoint;
        config.listenPort = storedPort && *storedPort > 0 && *storedPort <= 65535 ? static_cast<std::uint16_t>(*storedPort)
                                                                                  : fallback.listenPort;

        bool persist = false;
        if (storedPrivateKey.empty()) {
            if (service::common::wireguard::validManagedKey(fallback.privateKey) &&
                derivePublicKey(fallback.privateKey, config.publicKey)) {
                config.privateKey = fallback.privateKey;
            } else if (!generateKeyPair(config.privateKey, config.publicKey)) {
                co_return std::nullopt;
            }
            persist = true;
        } else {
            if (!service::common::wireguard::validManagedKey(storedPrivateKey) ||
                !derivePublicKey(storedPrivateKey, config.publicKey)) {
                co_return std::nullopt;
            }
            config.privateKey = storedPrivateKey;
            persist = storedPublicKey != config.publicKey;
        }
        if (storedEndpoint.empty() && !fallback.endpoint.empty()) {
            persist = true;
        }
        if (!storedPort || *storedPort != config.listenPort) {
            persist = true;
        }

        if (persist) {
            ruvia::db_query update;
            update.update(persistence::VpnNetworkEntity::table_name())
                .set(std::tuple_element_t<Network::column_index<"hub_private_key">(), Network::columns_type>::name.view(), update.value(config.privateKey))
                .set(std::tuple_element_t<Network::column_index<"hub_public_key">(), Network::columns_type>::name.view(), update.value(config.publicKey))
                .set(std::tuple_element_t<Network::column_index<"hub_endpoint">(), Network::columns_type>::name.view(), update.value(config.endpoint))
                .set(std::tuple_element_t<Network::column_index<"hub_listen_port">(), Network::columns_type>::name.view(), update.value(static_cast<int>(config.listenPort)))
                .set(std::tuple_element_t<Network::column_index<"updated_at">(), Network::columns_type>::name.view(), update.call("now"))
                .and_where(update.binary(update.column(std::tuple_element_t<Network::column_index<"id">(), Network::columns_type>::name.view()), ruvia::db_binary_operator::equal, update.cast(update.value(kDefaultNetworkId), ruvia::db_data_type::uuid)));
            (void)co_await transaction.execute(update);
        }
        co_await transaction.commit();
        co_return config;
    }
};

// VPN state of Edge peers: bridge route mapping, versioned Edge configuration,
// desktop demand and the Edge-reported capability and configuration results.
class VpnEdgeService final {
  public:
    // A virtual mapping must not collide with another virtual mapping or with a
    // real target network. Real target networks themselves may repeat: each one
    // is translated behind its owning Edge peer.
    static bool virtualMappingConflicts(const Ipv4Cidr& candidate, const std::optional<Ipv4Cidr>& realTarget, const std::optional<Ipv4Cidr>& existingVirtual) noexcept {
        return (realTarget && candidate.overlaps(*realTarget)) || (existingVirtual && candidate.overlaps(*existingVirtual));
    }

    static bool realTargetConflictsVirtual(const Ipv4Cidr& candidate, const std::optional<Ipv4Cidr>& existingVirtual) noexcept {
        return existingVirtual && candidate.overlaps(*existingVirtual);
    }

    // Reconcile the Edge bridge inventory with its VPN routes in one transaction
    // that holds lock::kVpnAddressAllocation, so route allocation is serialized
    // with peer address allocation and with the Hub kernel reconciliation.
    // The caller must not hold that lock or wait for this operation inside a
    // transaction that does.
    template <typename Context>
    static ruvia::task<void> syncEdgeRoutes(Context& context, std::string_view peerId, std::string_view networkId, std::string_view edgeNodeId, std::string_view actorId) {
        auto transaction = co_await context.db().begin_transaction();
        co_await VpnRuntimeService::lockAddressAllocation(transaction, context.pool());
        co_await syncEdgeBridgeRoutes(transaction, peerId, networkId, edgeNodeId, actorId, *context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>());
        co_await transaction.commit();
    }

    struct ActivatedPeer final {
        std::string peerId;
        std::string networkId;
        std::string networkOwner;
    };

    // Install the key an approved EdgeNode reported on its non-revoked edge peers.
    template <typename Context>
    static ruvia::task<std::vector<ActivatedPeer>> activateEdgePeers(Context& context, std::string_view nodeId, std::string_view publicKey) {
        using Op = ruvia::db_binary_operator;
        using PeerRow = persistence::VpnPeerEntity;
        using Network = persistence::VpnNetworkEntity;
        ruvia::db_query activated;
        activated.update(persistence::VpnPeerEntity::table_name(), "p")
            .set(std::tuple_element_t<PeerRow::column_index<"public_key">(), PeerRow::columns_type>::name.view(), activated.value(publicKey))
            .set(std::tuple_element_t<PeerRow::column_index<"status">(), PeerRow::columns_type>::name.view(), activated.value(std::string_view{ "active" }))
            .set(std::tuple_element_t<PeerRow::column_index<"updated_at">(), PeerRow::columns_type>::name.view(), activated.call("now"))
            .update_from(persistence::VpnNetworkEntity::table_name(), "n")
            .where(activated.binary(
                activated.column(std::tuple_element_t<PeerRow::column_index<"peer_type">(), PeerRow::columns_type>::name.view(), "p"),
                Op::equal,
                activated.value(std::string_view{ "edge" })
            ))
            .and_where(activated.binary(
                activated.column(std::tuple_element_t<PeerRow::column_index<"edge_node_id">(), PeerRow::columns_type>::name.view(), "p"),
                Op::equal,
                activated.cast(activated.value(nodeId), ruvia::db_data_type::uuid)
            ))
            .and_where(activated.binary(
                activated.column(std::tuple_element_t<PeerRow::column_index<"status">(), PeerRow::columns_type>::name.view(), "p"),
                Op::not_equal,
                activated.value(std::string_view{ "revoked" })
            ))
            .and_where(activated.binary(
                activated.column(std::tuple_element_t<Network::column_index<"id">(), Network::columns_type>::name.view(), "n"),
                Op::equal,
                activated.column(std::tuple_element_t<PeerRow::column_index<"network_id">(), PeerRow::columns_type>::name.view(), "p")
            ))
            .returning(
                { activated.cast(activated.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "p"), ruvia::db_data_type::text),
                  activated.cast(
                      activated.column(std::tuple_element_t<PeerRow::column_index<"network_id">(), PeerRow::columns_type>::name.view(), "p"),
                      ruvia::db_data_type::text
                  ),
                  activated.cast(
                      activated.column(std::tuple_element_t<Network::column_index<"created_by">(), Network::columns_type>::name.view(), "n"),
                      ruvia::db_data_type::text
                  ) }
            );
        const auto rows = co_await context.db().query(activated);
        std::vector<ActivatedPeer> peers;
        peers.reserve(rows.size());
        for (const auto& row : rows) {
            peers.push_back(ActivatedPeer{ feature_detail::rowText(row, 0), feature_detail::rowText(row, 1), feature_detail::rowText(row, 2) });
        }
        co_return peers;
    }

    // Project a terminal Edge VPN configuration result onto the peer's routes.
    // Only the newest revision of an enabled configuration changes route state:
    // an on-demand stop changes tunnel runtime only, and preallocated mappings
    // remain available to desktop discovery and the next enabled client.
    template <typename Context>
    static ruvia::task<void> applyConfigResult(Context& context, const service::message::vpn::EdgeConfigResult& result) {
        if (!result.get<"enabled">().value_) {
            co_return;
        }
        using Op = ruvia::db_binary_operator;
        using Route = persistence::VpnRouteEntity;
        using PeerRow = persistence::VpnPeerEntity;
        const auto peerId = result.get<"peerId">().view();
        ruvia::db_query revision;
        revision.select(revision.column(std::tuple_element_t<PeerRow::column_index<"config_revision">(), PeerRow::columns_type>::name.view()))
            .from(persistence::VpnPeerEntity::table_name())
            .where(revision.binary(revision.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view()), Op::equal, revision.cast(revision.value(peerId), ruvia::db_data_type::uuid)));
        ruvia::db_query update;
        const auto applied = result.get<"applied">().value_;
        const auto status =
            applied ? update.case_when(
                          { { update.column(std::tuple_element_t<Route::column_index<"enabled">(), Route::columns_type>::name.view(), "route"),
                              update.cast(update.value(std::string_view{ "active" }), ruvia::db_data_type::text) } },
                          update.cast(update.value(std::string_view{ "disabled" }), ruvia::db_data_type::text)
                      )
                    : update.cast(update.value(std::string_view{ "error" }), ruvia::db_data_type::text);
        const auto lastError = applied ? std::string_view{} : result.get<"errorMessage">().view();
        update.update(persistence::VpnRouteEntity::table_name(), "route")
            .set(std::tuple_element_t<Route::column_index<"status">(), Route::columns_type>::name.view(), status)
            .set(std::tuple_element_t<Route::column_index<"last_error">(), Route::columns_type>::name.view(), update.cast(update.value(lastError), ruvia::db_data_type::text))
            .set(std::tuple_element_t<Route::column_index<"updated_at">(), Route::columns_type>::name.view(), update.call("now"))
            .where(update.binary(
                update.column(std::tuple_element_t<Route::column_index<"edge_peer_id">(), Route::columns_type>::name.view(), "route"),
                Op::equal,
                update.cast(update.value(peerId), ruvia::db_data_type::uuid)
            ))
            .and_where(update.binary(update.subquery(revision), Op::equal, update.cast(update.value(result.get<"configVersion">().value_), ruvia::db_data_type::big_int)));
        (void)co_await context.db().execute(update);
    }

    // Queue a complete, versioned VPN configuration for an Edge peer. The actor
    // is optional for background projection: in that case the VPN network owner
    // is used so edge_task.created_by remains a valid audit subject.
    template <typename Context>
    static ruvia::task<void> queueEdgeConfig(Context& c, std::string_view peerId, std::string_view actorId = {}, std::string_view platformId = {}) {
        using Op = ruvia::db_binary_operator;
        using PeerRow = persistence::VpnPeerEntity;
        using Network = persistence::VpnNetworkEntity;
        using Route = persistence::VpnRouteEntity;
        using Node = service::edge::persistence::EdgeNodeEntity;
        using Task = service::edge::persistence::EdgeTaskEntity;
        // Capability reports and desktop demand changes may arrive on different
        // Workers. Serialize revisions without sharing their connections or state.
        auto configuration = co_await c.db("vpn-edge-config").begin_transaction();
        ruvia::db_query lock;
        lock.select(lock.call("pg_advisory_xact_lock", { lock.call("hashtextextended", { lock.cast(lock.value(peerId), ruvia::db_data_type::text), lock.value(service::message::lock::kVpnEdgeConfigSeed) }) }));
        (void)co_await configuration.query(lock);
        ruvia::db_query peer;
        const auto capability = peer.call(
            "lower",
            { peer.coalesce({ peer.binary(peer.binary(peer.column(std::tuple_element_t<Node::column_index<"capability">(), Node::columns_type>::name.view(), "e"), Op::json_get, peer.value("vpn")), Op::json_get_text, peer.value("supportsVpn")), peer.value("") }) }
        );
        peer.select({ peer.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "p"), peer.column(std::tuple_element_t<PeerRow::column_index<"network_id">(), PeerRow::columns_type>::name.view(), "p"), peer.column(std::tuple_element_t<PeerRow::column_index<"edge_node_id">(), PeerRow::columns_type>::name.view(), "p"), peer.call("host", { peer.column(std::tuple_element_t<PeerRow::column_index<"assigned_ipv4">(), PeerRow::columns_type>::name.view(), "p") }), peer.column(std::tuple_element_t<PeerRow::column_index<"config_revision">(), PeerRow::columns_type>::name.view(), "p"), peer.column(std::tuple_element_t<PeerRow::column_index<"status">(), PeerRow::columns_type>::name.view(), "p"), peer.column(std::tuple_element_t<Network::column_index<"hub_public_key">(), Network::columns_type>::name.view(), "n"), peer.column(std::tuple_element_t<Network::column_index<"hub_endpoint">(), Network::columns_type>::name.view(), "n"), peer.column(std::tuple_element_t<Network::column_index<"hub_listen_port">(), Network::columns_type>::name.view(), "n"), peer.cast(peer.column(std::tuple_element_t<Network::column_index<"created_by">(), Network::columns_type>::name.view(), "n"), ruvia::db_data_type::text), peer.column(std::tuple_element_t<Network::column_index<"status">(), Network::columns_type>::name.view(), "n") })
            .from(persistence::VpnPeerEntity::table_name(), "p")
            .join(ruvia::db_join_type::inner, Network::table_name(), peer.binary(peer.column(std::tuple_element_t<Network::column_index<"id">(), Network::columns_type>::name.view(), "n"), Op::equal, peer.column(std::tuple_element_t<PeerRow::column_index<"network_id">(), PeerRow::columns_type>::name.view(), "p")), "n")
            .join(ruvia::db_join_type::inner, Node::table_name(), peer.binary(peer.column(std::tuple_element_t<Node::column_index<"id">(), Node::columns_type>::name.view(), "e"), Op::equal, peer.column(std::tuple_element_t<PeerRow::column_index<"edge_node_id">(), PeerRow::columns_type>::name.view(), "p")), "e")
            .and_where(peer.binary(peer.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "p"), Op::equal, peer.cast(peer.value(peerId), ruvia::db_data_type::uuid)))
            .and_where(peer.binary(peer.column(std::tuple_element_t<PeerRow::column_index<"peer_type">(), PeerRow::columns_type>::name.view(), "p"), Op::equal, peer.value("edge")))
            .and_where(peer.binary(peer.column(std::tuple_element_t<Node::column_index<"enrollment_status">(), Node::columns_type>::name.view(), "e"), Op::equal, peer.value("approved")))
            .and_where(peer.binary(capability, Op::in, peer.list({ peer.value("true"), peer.value("t"), peer.value("1") })))
            .limit(1);
        const auto rows = co_await configuration.query(peer);
        if (rows.empty()) {
            co_return;
        }

        const auto nodeId = feature_detail::rowText(rows.front(), 2);
        ruvia::db_query routes;
        routes
            .select({ routes.cast(routes.column(std::tuple_element_t<Route::column_index<"id">(), Route::columns_type>::name.view()), ruvia::db_data_type::text), routes.column(std::tuple_element_t<Route::column_index<"virtual_cidr">(), Route::columns_type>::name.view()), routes.column(std::tuple_element_t<Route::column_index<"target_cidr">(), Route::columns_type>::name.view()), routes.column(std::tuple_element_t<Route::column_index<"mode">(), Route::columns_type>::name.view()), routes.column(std::tuple_element_t<Route::column_index<"nat_mode">(), Route::columns_type>::name.view()), routes.column(std::tuple_element_t<Route::column_index<"enabled">(), Route::columns_type>::name.view()) })
            .from(persistence::VpnRouteEntity::table_name())
            .and_where(routes.binary(routes.column(std::tuple_element_t<Route::column_index<"edge_peer_id">(), Route::columns_type>::name.view()), Op::equal, routes.cast(routes.value(peerId), ruvia::db_data_type::uuid)))
            .add_order_by(routes.column(std::tuple_element_t<Route::column_index<"virtual_cidr">(), Route::columns_type>::name.view()))
            .limit(16);
        const auto routeRows = co_await configuration.query(routes);
        auto& uuidGenerator = *c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>();
        const auto requestId = uuidGenerator.next();
        std::uint8_t requestBytes[16]{};
        if (!service::edge::protocol::uuidBytes(requestId, requestBytes)) {
            co_return;
        }
        auto envelope = service::edge::protocol::outbound(uuidGenerator.next(), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
        if (!platformId.empty()) {
            std::uint8_t platformBytes[16]{};
            if (service::edge::protocol::uuidBytes(platformId, platformBytes)) {
                envelope.set_platform_id(service::edge::protocol::bytes(platformBytes, sizeof(platformBytes)));
            }
        }
        auto* request = envelope.mutable_vpn_config_request();
        request->set_request_id(service::edge::protocol::bytes(requestBytes, sizeof(requestBytes)));
        const auto nextVersion = feature_detail::integer(feature_detail::rowText(rows.front(), 4)) + 1;
        request->set_config_version(static_cast<std::uint64_t>(nextVersion));
        const auto revoked = feature_detail::rowText(rows.front(), 5) == "revoked";
        const auto networkDisabled = feature_detail::rowText(rows.front(), 10) != "enabled";
        const auto demand = co_await VpnRuntimeService::desktopDemand(c);
        request->set_enabled(!revoked && !networkDisabled && demand.edgePeers.contains(std::string(peerId)));
        request->set_hub_public_key(feature_detail::rowText(rows.front(), 6));
        request->set_hub_endpoint(feature_detail::rowText(rows.front(), 7));
        request->set_hub_listen_port(static_cast<std::uint32_t>(feature_detail::integer(feature_detail::rowText(rows.front(), 8), 51820)));
        request->set_edge_address(feature_detail::rowText(rows.front(), 3) + "/32");
        if (request->enabled()) {
            for (const auto& row : routeRows) {
                auto* route = request->add_routes();
                route->set_route_id(feature_detail::rowText(row, 0));
                route->set_virtual_cidr(feature_detail::rowText(row, 1));
                route->set_target_cidr(feature_detail::rowText(row, 2));
                route->set_mode(feature_detail::rowText(row, 3));
                route->set_nat_mode(feature_detail::rowText(row, 4));
                route->set_enabled(feature_detail::rowText(row, 5) == "t");
            }
        }
        const auto wire = service::edge::protocol::encode(envelope);
        if (wire.empty()) {
            co_return;
        }
        const auto createdBy = actorId.empty() ? feature_detail::rowText(rows.front(), 9) : std::string(actorId);
        if (createdBy.empty()) {
            co_return;
        }
        ruvia::db_query superseded;
        superseded.update(Task::table_name())
            .set(std::tuple_element_t<Task::column_index<"status">(), Task::columns_type>::name.view(), superseded.value("failed"))
            .set(std::tuple_element_t<Task::column_index<"result">(), Task::columns_type>::name.view(), superseded.call("jsonb_build_object", { superseded.cast(superseded.value("configVersion"), ruvia::db_data_type::text), superseded.binary(superseded.column(std::tuple_element_t<Task::column_index<"request">(), Task::columns_type>::name.view()), Op::json_get_text, superseded.cast(superseded.value("configVersion"), ruvia::db_data_type::text)), superseded.cast(superseded.value("errorCode"), ruvia::db_data_type::text), superseded.cast(superseded.value("superseded"), ruvia::db_data_type::text), superseded.cast(superseded.value("errorMessage"), ruvia::db_data_type::text), superseded.cast(superseded.value("superseded by newer VPN configuration"), ruvia::db_data_type::text) }))
            .set(std::tuple_element_t<Task::column_index<"updated_at">(), Task::columns_type>::name.view(), superseded.call("now"))
            .set(std::tuple_element_t<Task::column_index<"completed_at">(), Task::columns_type>::name.view(), superseded.call("now"))
            .and_where(superseded.binary(superseded.column(std::tuple_element_t<Task::column_index<"node_id">(), Task::columns_type>::name.view()), Op::equal, superseded.cast(superseded.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(superseded.binary(superseded.column(std::tuple_element_t<Task::column_index<"task_type">(), Task::columns_type>::name.view()), Op::equal, superseded.value("vpn")))
            .and_where(superseded.binary(superseded.column(std::tuple_element_t<Task::column_index<"status">(), Task::columns_type>::name.view()), Op::not_in, superseded.list({ superseded.value("succeeded"), superseded.value("failed") })))
            .and_where(superseded.binary(
                superseded.binary(superseded.column(std::tuple_element_t<Task::column_index<"request">(), Task::columns_type>::name.view()), Op::json_get_text, superseded.value("peerId")),
                Op::equal,
                superseded.cast(superseded.value(peerId), ruvia::db_data_type::text)
            ));
        ruvia::db_query task;
        task.with("superseded", superseded)
            .insert_into(Task::table_name(), { std::tuple_element_t<Task::column_index<"id">(), Task::columns_type>::name.view(), std::tuple_element_t<Task::column_index<"node_id">(), Task::columns_type>::name.view(), std::tuple_element_t<Task::column_index<"task_type">(), Task::columns_type>::name.view(), std::tuple_element_t<Task::column_index<"request">(), Task::columns_type>::name.view(), std::tuple_element_t<Task::column_index<"created_by">(), Task::columns_type>::name.view() })
            .values({ task.cast(task.value(requestId), ruvia::db_data_type::uuid), task.cast(task.value(nodeId), ruvia::db_data_type::uuid), task.value("vpn"), task.call("jsonb_build_object", { task.cast(task.value("peerId"), ruvia::db_data_type::text), task.cast(task.value(peerId), ruvia::db_data_type::text), task.cast(task.value("configVersion"), ruvia::db_data_type::text), task.cast(task.value(nextVersion), ruvia::db_data_type::big_int), task.cast(task.value("enabled"), ruvia::db_data_type::text), task.cast(task.value(request->enabled()), ruvia::db_data_type::boolean) }), task.cast(task.value(createdBy), ruvia::db_data_type::uuid) });
        (void)co_await configuration.execute(task);
        ruvia::db_query revision;
        revision.update(persistence::VpnPeerEntity::table_name())
            .set(std::tuple_element_t<PeerRow::column_index<"config_revision">(), PeerRow::columns_type>::name.view(), revision.value(nextVersion))
            .set(std::tuple_element_t<PeerRow::column_index<"updated_at">(), PeerRow::columns_type>::name.view(), revision.call("now"))
            .and_where(revision.binary(revision.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view()), Op::equal, revision.cast(revision.value(peerId), ruvia::db_data_type::uuid)));
        (void)co_await configuration.execute(revision);
        co_await configuration.commit();
        co_await service::edge::dispatch::enqueue(c.redis(), nodeId, wire);
    }

    // Queue a configuration for every edge peer whose last requested enabled
    // state differs from the current desktop demand. An unchanged demand sends
    // nothing, so repeated calls are idempotent.
    template <typename Context>
    static ruvia::task<DesktopDemand> synchronizeEdgeDemand(Context& context) {
        using Op = ruvia::db_binary_operator;
        using PeerRow = persistence::VpnPeerEntity;
        using Task = service::edge::persistence::EdgeTaskEntity;
        auto demand = co_await VpnRuntimeService::desktopDemand(context);
        ruvia::db_query latest;
        latest
            .select(latest.binary(latest.column(std::tuple_element_t<Task::column_index<"request">(), Task::columns_type>::name.view(), "task"), Op::json_get_text, latest.value("enabled")))
            .from(Task::table_name(), "task")
            .and_where(latest.binary(latest.column(std::tuple_element_t<Task::column_index<"task_type">(), Task::columns_type>::name.view(), "task"), Op::equal, latest.value("vpn")))
            .and_where(latest.binary(
                latest.binary(latest.column(std::tuple_element_t<Task::column_index<"request">(), Task::columns_type>::name.view(), "task"), Op::json_get_text, latest.value("peerId")),
                Op::equal,
                latest.cast(latest.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "peer"), ruvia::db_data_type::text)
            ))
            .add_order_by(latest.column(std::tuple_element_t<Task::column_index<"created_at">(), Task::columns_type>::name.view(), "task"), ruvia::db_order_direction::desc)
            .add_order_by(latest.column(std::tuple_element_t<Task::column_index<"id">(), Task::columns_type>::name.view(), "task"), ruvia::db_order_direction::desc)
            .limit(1);
        ruvia::db_query peers;
        peers
            .select(peers.cast(peers.column(std::tuple_element_t<PeerRow::column_index<"id">(), PeerRow::columns_type>::name.view(), "peer"), ruvia::db_data_type::text))
            .add_select(peers.subquery(latest))
            .from(persistence::VpnPeerEntity::table_name(), "peer")
            .and_where(peers.binary(peers.column(std::tuple_element_t<PeerRow::column_index<"peer_type">(), PeerRow::columns_type>::name.view(), "peer"), Op::equal, peers.value("edge")));
        const auto rows = co_await context.db().query(peers);
        for (const auto& row : rows) {
            const auto peerId = feature_detail::rowText(row, 0);
            const auto previous = feature_detail::rowText(row, 1);
            const auto desired = demand.edgePeers.contains(peerId) ? "true" : "false";
            if (previous != desired) {
                co_await queueEdgeConfig(context, peerId);
            }
        }
        co_return demand;
    }

  private:
    // Reconcile the Edge bridge inventory with its VPN routes. The operation
    // keeps a user-selected virtual network while the bridge prefix remains
    // compatible. A prefix change necessarily selects a new equally-sized
    // virtual network. Real LAN prefixes are local to each Edge peer and may
    // repeat across sites; virtual prefixes remain globally unique so the Hub
    // can route them unambiguously.
    template <typename Db>
    static ruvia::task<void> syncEdgeBridgeRoutes(Db& db, std::string_view peerId, std::string_view networkId, std::string_view edgeNodeId, std::string_view actorId, service::common::UuidV7Generator& uuidGenerator) {
        struct RouteRecord final {
            std::string id;
            std::string lanInterface;
            std::optional<Ipv4Cidr> target;
            std::optional<Ipv4Cidr> virtualNetwork;
        };

        struct BridgeRecord final {
            std::string lanInterface;
            Ipv4Cidr target;
        };

        using Op = ruvia::db_binary_operator;
        using Bridge = service::edge::persistence::EdgeNodeNetworkEntity;
        using Route = persistence::VpnRouteEntity;
        ruvia::db_query bridgeQuery;
        bridgeQuery
            .select({ bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"name">(), Bridge::columns_type>::name.view()), bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"device">(), Bridge::columns_type>::name.view()), bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"ipv4">(), Bridge::columns_type>::name.view()), bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"prefix_length">(), Bridge::columns_type>::name.view()) })
            .from(Bridge::table_name())
            .and_where(bridgeQuery.binary(bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"node_id">(), Bridge::columns_type>::name.view()), Op::equal, bridgeQuery.cast(bridgeQuery.value(edgeNodeId), ruvia::db_data_type::uuid)))
            .and_where(bridgeQuery.binary(bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"is_bridge">(), Bridge::columns_type>::name.view()), Op::equal, bridgeQuery.value(true)))
            .and_where(bridgeQuery.binary(
                bridgeQuery.coalesce({ bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"ipv4">(), Bridge::columns_type>::name.view()), bridgeQuery.value("") }),
                Op::not_equal,
                bridgeQuery.value("")
            ))
            .and_where(bridgeQuery.between(
                bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"prefix_length">(), Bridge::columns_type>::name.view()),
                bridgeQuery.value(1),
                bridgeQuery.value(30)
            ))
            .add_order_by(bridgeQuery.column(std::tuple_element_t<Bridge::column_index<"name">(), Bridge::columns_type>::name.view()));
        const auto bridgeRows = co_await db.query(bridgeQuery);
        std::vector<BridgeRecord> bridges;
        for (const auto& row : bridgeRows) {
            const auto address = feature_detail::rowText(row, 2);
            const auto prefix = feature_detail::integer(feature_detail::rowText(row, 3));
            const auto target = networkCidr(address, static_cast<std::uint8_t>(prefix));
            if (!target || !isPrivateIpv4(*target)) {
                continue;
            }
            const auto name = feature_detail::rowText(row, 1).empty() ? feature_detail::rowText(row, 0) : feature_detail::rowText(row, 1);
            if (!name.empty()) {
                bridges.push_back(BridgeRecord{ name, *target });
            }
        }
        if (bridges.empty()) {
            service::common::fail(21008, "EdgeNode 尚未上报可映射的私有桥接 LAN 网段", 409);
        }

        const auto routeColumns = [](ruvia::db_query& query) {
            return std::vector<ruvia::db_query::expr_type>{
                query.cast(query.column(std::tuple_element_t<Route::column_index<"id">(), Route::columns_type>::name.view()), ruvia::db_data_type::text),
                query.column(std::tuple_element_t<Route::column_index<"lan_interface">(), Route::columns_type>::name.view()),
                query.column(std::tuple_element_t<Route::column_index<"target_cidr">(), Route::columns_type>::name.view()),
                query.column(std::tuple_element_t<Route::column_index<"virtual_cidr">(), Route::columns_type>::name.view())
            };
        };
        const auto routeRecord = [](const auto& row) {
            return RouteRecord{ feature_detail::rowText(row, 0), feature_detail::rowText(row, 1), parseCidr(feature_detail::rowText(row, 2), 1, 30), parseCidr(feature_detail::rowText(row, 3), 1, 30) };
        };
        ruvia::db_query currentQuery;
        const auto currentColumns = routeColumns(currentQuery);
        currentQuery.select(currentColumns)
            .from(persistence::VpnRouteEntity::table_name())
            .and_where(currentQuery.binary(currentQuery.column(std::tuple_element_t<Route::column_index<"edge_peer_id">(), Route::columns_type>::name.view()), Op::equal, currentQuery.cast(currentQuery.value(peerId), ruvia::db_data_type::uuid)))
            .add_order_by(currentQuery.column(std::tuple_element_t<Route::column_index<"id">(), Route::columns_type>::name.view()));
        const auto currentRows = co_await db.query(currentQuery);
        std::vector<RouteRecord> current;
        current.reserve(currentRows.size());
        for (const auto& row : currentRows) {
            current.push_back(routeRecord(row));
        }

        ruvia::db_query allRoutesQuery;
        const auto allColumns = routeColumns(allRoutesQuery);
        allRoutesQuery.select(allColumns).from(persistence::VpnRouteEntity::table_name());
        const auto allRows = co_await db.query(allRoutesQuery);
        std::vector<RouteRecord> allRoutes;
        allRoutes.reserve(allRows.size());
        for (const auto& row : allRows) {
            allRoutes.push_back(routeRecord(row));
        }

        const auto conflicts = [&](const Ipv4Cidr& candidate, std::string_view excludedId) {
            for (const auto& route : allRoutes) {
                if (route.id == excludedId) {
                    continue;
                }
                if (virtualMappingConflicts(candidate, route.target, route.virtualNetwork)) {
                    return true;
                }
            }
            return false;
        };
        const auto chooseVirtual = [&](const Ipv4Cidr& target, std::string_view excludedId) -> std::optional<Ipv4Cidr> {
            if (const auto preferred = mappedVirtualCidr(target);
                preferred && !preferred->overlaps(target) && !conflicts(*preferred, excludedId)) {
                return preferred;
            }
            if (target.prefix < kVirtualLanPool.prefix) {
                return std::nullopt;
            }
            const auto blockSize = static_cast<std::uint64_t>(target.size());
            for (std::uint64_t offset = 0; offset < kVirtualLanPool.size(); offset += blockSize) {
                const Ipv4Cidr candidate{ static_cast<std::uint32_t>(kVirtualLanPool.network + offset), target.prefix };
                if (!candidate.overlaps(target) && !conflicts(candidate, excludedId)) {
                    return candidate;
                }
            }
            return std::nullopt;
        };

        std::unordered_set<std::string> claimed;
        for (const auto& bridge : bridges) {
            auto existing = std::find_if(current.begin(), current.end(), [&](const auto& route) {
                return !claimed.contains(route.id) &&
                    ((route.target && route.target->network == bridge.target.network && route.target->prefix == bridge.target.prefix) ||
                     route.lanInterface == bridge.lanInterface);
            });
            std::string routeId;
            std::optional<Ipv4Cidr> virtualNetwork;
            if (existing != current.end()) {
                routeId = existing->id;
                claimed.emplace(routeId);
                if (existing->virtualNetwork && existing->virtualNetwork->prefix == bridge.target.prefix &&
                    kVirtualLanPool.contains(existing->virtualNetwork->network) &&
                    kVirtualLanPool.contains(existing->virtualNetwork->network + existing->virtualNetwork->size() - 1U) &&
                    !existing->virtualNetwork->overlaps(bridge.target) && !conflicts(*existing->virtualNetwork, routeId)) {
                    virtualNetwork = existing->virtualNetwork;
                } else {
                    virtualNetwork = chooseVirtual(bridge.target, routeId);
                }
            } else {
                virtualNetwork = chooseVirtual(bridge.target, {});
            }
            const auto excludedId = existing != current.end() ? existing->id : std::string{};
            for (const auto& route : allRoutes) {
                if (route.id == excludedId) {
                    continue;
                }
                if (realTargetConflictsVirtual(bridge.target, route.virtualNetwork)) {
                    service::common::fail(21002, "真实 LAN 与已有虚拟网段重叠", 409);
                }
            }
            if (!virtualNetwork) {
                service::common::fail(21009, "没有可用的全局唯一虚拟网段", 409);
            }
            const auto targetCidr = bridge.target.text();
            const auto virtualCidr = virtualNetwork->text();
            if (existing != current.end()) {
                ruvia::db_query route;
                route.update(persistence::VpnRouteEntity::table_name())
                    .set(std::tuple_element_t<Route::column_index<"network_id">(), Route::columns_type>::name.view(), route.cast(route.value(networkId), ruvia::db_data_type::uuid))
                    .set(std::tuple_element_t<Route::column_index<"lan_interface">(), Route::columns_type>::name.view(), route.value(bridge.lanInterface))
                    .set(std::tuple_element_t<Route::column_index<"target_cidr">(), Route::columns_type>::name.view(), route.value(targetCidr))
                    .set(std::tuple_element_t<Route::column_index<"virtual_cidr">(), Route::columns_type>::name.view(), route.value(virtualCidr))
                    .set(std::tuple_element_t<Route::column_index<"mode">(), Route::columns_type>::name.view(), route.value("nat"))
                    .set(std::tuple_element_t<Route::column_index<"nat_mode">(), Route::columns_type>::name.view(), route.value("masquerade"))
                    .set(std::tuple_element_t<Route::column_index<"enabled">(), Route::columns_type>::name.view(), route.value(true))
                    .set(std::tuple_element_t<Route::column_index<"status">(), Route::columns_type>::name.view(), route.value("active"))
                    .set(std::tuple_element_t<Route::column_index<"last_error">(), Route::columns_type>::name.view(), route.value(""))
                    .set(std::tuple_element_t<Route::column_index<"updated_at">(), Route::columns_type>::name.view(), route.call("now"))
                    .and_where(route.binary(route.column(std::tuple_element_t<Route::column_index<"id">(), Route::columns_type>::name.view()), Op::equal, route.cast(route.value(routeId), ruvia::db_data_type::uuid)));
                (void)co_await db.execute(route);
                for (auto& route : allRoutes) {
                    if (route.id == routeId) {
                        route.lanInterface = bridge.lanInterface;
                        route.target = bridge.target;
                        route.virtualNetwork = virtualNetwork;
                    }
                }
            } else {
                const auto id = uuidGenerator.next();
                ruvia::db_query route;
                route
                    .insert_into(Route::table_name(), { std::tuple_element_t<Route::column_index<"id">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"network_id">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"edge_peer_id">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"lan_interface">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"target_cidr">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"virtual_cidr">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"mode">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"nat_mode">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"enabled">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"status">(), Route::columns_type>::name.view(), std::tuple_element_t<Route::column_index<"created_by">(), Route::columns_type>::name.view() })
                    .values({ route.cast(route.value(id), ruvia::db_data_type::uuid), route.cast(route.value(networkId), ruvia::db_data_type::uuid), route.cast(route.value(peerId), ruvia::db_data_type::uuid), route.value(bridge.lanInterface), route.value(targetCidr), route.value(virtualCidr), route.value("nat"), route.value("masquerade"), route.value(true), route.value("active"), route.cast(route.value(actorId), ruvia::db_data_type::uuid) });
                (void)co_await db.execute(route);
                allRoutes.push_back(RouteRecord{ id, bridge.lanInterface, bridge.target, virtualNetwork });
            }
        }
    }
};

} // namespace service::vpn
