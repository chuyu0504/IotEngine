#pragma once
#include "service/features/edge/edge.config.h"

#include <ruvia/core/stop_token.h>

#include "service/utils/number.h"
#include <chrono>
#include <limits>
#include <sstream>
#include <ruvia/web/redis/redis_repository.h>
#include <ruvia/web/web_worker.h>
#include "service/features/vpn/firewall/firewall.transport.h"

#include "service/features/vpn/vpn.entity.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <edge.pb.h>
#include <google/protobuf/message.h>

#include "service/common/message.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/session/session.service.h"
#include "service/utils/network.h"

namespace service::vpn::client_config {

inline std::string render(std::string_view privateKey, std::string_view address,
                          std::string_view hubPublicKey, std::string_view hubEndpoint,
                          std::uint16_t hubPort,
                          const std::vector<std::string>& allowedRoutes) {
    std::string config{"[Interface]\nPrivateKey = "};
    config += privateKey;
    config += "\nAddress = ";
    config += address;
    config += "/32\nMTU = 1280\n\n[Peer]\nPublicKey = ";
    config += hubPublicKey;
    config += "\nEndpoint = ";
    config += hubEndpoint;
    config += ":" + std::to_string(hubPort);
    config += "\nAllowedIPs = ";
    for (std::size_t index = 0; index < allowedRoutes.size(); ++index) {
        if (index != 0)
            config += ", ";
        config += allowedRoutes[index];
    }
    config += "\nPersistentKeepalive = 120\n";
    return config;
}

} // namespace service::vpn::client_config

#include <cstddef>
#include <optional>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/utils/redis.h"

namespace service::vpn {

using service::message::vpn::kOverlayPool;
using service::message::vpn::kVirtualLanPool;
using service::message::vpn::mappedVirtualCidr;
using service::utils::network::Ipv4Cidr;
using service::utils::network::hostAddress;
using service::utils::network::isPrivateIpv4;
using service::utils::network::networkCidr;
using service::utils::network::parseCidr;
using service::utils::network::parseIpv4;

class VpnRuntimeService final {
  public:
    template <typename Context>
    static ruvia::task<std::unordered_set<std::string>> desktopDemand(Context& context) {
        using Op = ruvia::db_binary_operator;
        ruvia::db_query selections;
        selections.select({
                selections.cast(selections.column(persistence::VpnEffectiveEdgeAccessEntity::column_name<"edge_peer_id">(), "access"), ruvia::db_data_type::text),
                selections.cast(selections.column(persistence::VpnEffectiveEdgeAccessEntity::column_name<"peer_id">(), "access"), ruvia::db_data_type::text)})
            .from(persistence::VpnEffectiveEdgeAccessEntity::table_name(), "access")
            .join(ruvia::db_join_type::inner, persistence::VpnPeerEntity::table_name(),
                selections.binary(selections.column(persistence::VpnPeerEntity::column_name<"id">(), "peer"), Op::equal,
                    selections.column(persistence::VpnEffectiveEdgeAccessEntity::column_name<"peer_id">(), "access")), "peer")
            .and_where(selections.column(persistence::VpnPeerEntity::column_name<"client_managed">(), "peer"))
            .and_where(selections.binary(selections.column(persistence::VpnPeerEntity::column_name<"peer_type">(), "peer"), Op::equal, selections.value("windows")));
        const auto rows = co_await context.db().query(selections);
        std::unordered_set<std::string> activeClients, inactiveClients, demand;
        for (const auto& row : rows) {
            const std::string edgePeerId(row[0].value().value_or(""));
            const std::string clientPeerId(row[1].value().value_or(""));
            if (!activeClients.contains(clientPeerId) && !inactiveClients.contains(clientPeerId)) {
                const auto key = persistence::DesktopSessionSet::key(clientPeerId);
                const std::string_view keys[]{key};
                const auto reply = co_await context.redis().eval(R"lua(
local time=redis.call('TIME')
local now=time[1]*1000+math.floor(time[2]/1000)
return redis.call('ZCOUNT',KEYS[1],'('..now,'+inf')
)lua", keys, std::span<const std::string_view>{});
                if (reply.kind() != ruvia::redis_value::kind_type::integer)
                    service::message::redis::throwValue("desktop VPN demand", reply);
                (reply.integer() > 0 ? activeClients : inactiveClients).insert(clientPeerId);
            }
            if (activeClients.contains(clientPeerId)) demand.insert(edgePeerId);
        }
        co_return demand;
    }

    template <typename Transaction>
    static ruvia::task<bool> acquireReconciliation(Transaction& transaction,
        std::pmr::memory_resource* resource, bool background) {
        ruvia::db_query lock(resource);
        lock.select(lock.call(background ? "pg_try_advisory_xact_lock" : "pg_advisory_xact_lock",
            { lock.cast(lock.value(std::int64_t{5282804697543808071}), ruvia::db_data_type::big_int) }));
        const auto rows = co_await transaction.query(lock);
        co_return !background || (!rows.empty() && rows[0][0].value().value_or("") == "t");
    }

    template <typename Transaction>
    static ruvia::task<void> lockAddressAllocation(Transaction& transaction,
        std::pmr::memory_resource* resource) {
        ruvia::db_query lock(resource);
        lock.select(lock.call("pg_advisory_xact_lock",
            { lock.cast(lock.value(std::int64_t{5282804697543808068}), ruvia::db_data_type::big_int) }));
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
        ruvia::db_query routes;
        const std::vector<ruvia::db_order_term> routeOrder{ { routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"virtual_cidr">(), "r") } };
        routes.select(routes.aggregate("string_agg", { routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"virtual_cidr">(), "r"), routes.value(", ") }, false, routeOrder))
            .from(service::vpn::persistence::VpnRouteEntity::table_name(), "r")
            .and_where(routes.binary(routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"edge_peer_id">(), "r"), Op::equal, routes.column(service::vpn::persistence::VpnPeerEntity::column_name<"id">(), "p")))
            .and_where(routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"enabled">(), "r"));
        ruvia::db_query allowed;
        const std::vector<ruvia::db_order_term> allowedOrder{ { allowed.column(service::vpn::persistence::VpnEffectiveRouteAccessEntity::column_name<"virtual_cidr">(), "access") } };
        allowed.select(allowed.aggregate("string_agg", { allowed.column(service::vpn::persistence::VpnEffectiveRouteAccessEntity::column_name<"virtual_cidr">(), "access"), allowed.value(", ") }, false, allowedOrder))
            .from(service::vpn::persistence::VpnEffectiveRouteAccessEntity::table_name(), "access")
            .and_where(allowed.binary(allowed.column(service::vpn::persistence::VpnEffectiveRouteAccessEntity::column_name<"peer_id">(), "access"), Op::equal, allowed.column(service::vpn::persistence::VpnPeerEntity::column_name<"id">(), "p")));
        ruvia::db_query addresses;
        const std::vector<ruvia::db_order_term> addressOrder{ { addresses.column(service::vpn::persistence::VpnEffectiveEdgeAccessEntity::column_name<"edge_address">(), "access") } };
        addresses.select(addresses.aggregate("string_agg", { addresses.column(service::vpn::persistence::VpnEffectiveEdgeAccessEntity::column_name<"edge_address">(), "access"), addresses.value(", ") }, false, addressOrder))
            .from(service::vpn::persistence::VpnEffectiveEdgeAccessEntity::table_name(), "access")
            .and_where(addresses.binary(addresses.column(service::vpn::persistence::VpnEffectiveEdgeAccessEntity::column_name<"peer_id">(), "access"), Op::equal, addresses.column(service::vpn::persistence::VpnPeerEntity::column_name<"id">(), "p")));
        ruvia::db_query peersQuery;
        peersQuery.select(peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"public_key">(), "p"))
            .add_select(peersQuery.call("host", { peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"assigned_ipv4">(), "p") }))
            .add_select(peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"peer_type">(), "p"))
            .add_select(peersQuery.coalesce({ peersQuery.subquery(routes), peersQuery.value("") }))
            .add_select(peersQuery.coalesce({ peersQuery.subquery(allowed), peersQuery.value("") }))
            .add_select(peersQuery.coalesce({ peersQuery.subquery(addresses), peersQuery.value("") }))
            .from(service::vpn::persistence::VpnPeerEntity::table_name(), "p")
            .join(ruvia::db_join_type::inner, service::vpn::persistence::VpnNetworkEntity::table_name(),
                peersQuery.binary(peersQuery.column(service::vpn::persistence::VpnNetworkEntity::column_name<"id">(), "n"), Op::equal, peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"network_id">(), "p")), "n")
            .and_where(peersQuery.binary(peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"status">(), "p"), Op::equal, peersQuery.value("active")))
            .and_where(peersQuery.binary(peersQuery.column(service::vpn::persistence::VpnNetworkEntity::column_name<"status">(), "n"), Op::equal, peersQuery.value("enabled")))
            .and_where(peersQuery.unary(ruvia::db_unary_operator::is_null, peersQuery.column(service::vpn::persistence::VpnNetworkEntity::column_name<"deleted_at">(), "n")))
            .and_where(peersQuery.binary(peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"public_key">(), "p"), Op::not_equal, peersQuery.value("")))
            .and_where(peersQuery.binary(peersQuery.unary(ruvia::db_unary_operator::not_value, peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"client_managed">(), "p")),
                Op::or_value, peersQuery.call("vpn_desktop_user_authorized", { peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"user_id">(), "p") })))
            .add_order_by(peersQuery.column(service::vpn::persistence::VpnPeerEntity::column_name<"id">(), "p"));
        const auto rows = co_await context.db().query(peersQuery);
        std::vector<Peer> peers;
        peers.reserve(rows.size());
        for (const auto& row : rows)
            peers.push_back(Peer{
                std::string(row[0].value().value_or(std::string_view{})),
                std::string(row[1].value().value_or(std::string_view{})),
                std::string(row[2].value().value_or(std::string_view{})),
                std::string(row[3].value().value_or(std::string_view{})),
                std::string(row[4].value().value_or(std::string_view{})),
                std::string(row[5].value().value_or(std::string_view{})),
            });
        co_return peers;
    }

    template <typename Context>
    static ruvia::task<void> updatePeerHandshake(Context& context,
                                                 std::string_view publicKey,
                                                 std::int64_t seconds) {
        ruvia::db_query handshake;
        const auto timestamp = handshake.call("to_timestamp", { handshake.cast(handshake.value(seconds), ruvia::db_data_type::double_value) });
        handshake.update(service::vpn::persistence::VpnPeerEntity::table_name()).set(service::vpn::persistence::VpnPeerEntity::column_name<"last_handshake_at">(), timestamp).set(service::vpn::persistence::VpnPeerEntity::column_name<"updated_at">(), handshake.call("now"))
            .and_where(handshake.binary(handshake.column(service::vpn::persistence::VpnPeerEntity::column_name<"public_key">()), ruvia::db_binary_operator::equal, handshake.value(publicKey)))
            .and_where(handshake.binary(handshake.column(service::vpn::persistence::VpnPeerEntity::column_name<"status">()), ruvia::db_binary_operator::equal, handshake.value("active")))
            .and_where(handshake.binary(handshake.unary(ruvia::db_unary_operator::is_null, handshake.column(service::vpn::persistence::VpnPeerEntity::column_name<"last_handshake_at">())),
                ruvia::db_binary_operator::or_value, handshake.binary(handshake.column(service::vpn::persistence::VpnPeerEntity::column_name<"last_handshake_at">()), ruvia::db_binary_operator::less, timestamp)));
        (void)co_await context.db().execute(handshake);
    }
};

namespace detail {

inline std::string edgeConfigRowValue(const auto& row, std::size_t index) {
    return std::string(row[index].value().value_or(std::string_view{}));
}

} // namespace detail

namespace feature {

namespace route_sync_detail {

inline std::string rowValue(const auto& row, std::size_t index) {
    return std::string(row[index].value().value_or(std::string_view{}));
}

inline std::int64_t integer(std::string_view value, std::int64_t fallback = 0) {
    return service::utils::parseInt64(
               value.empty() ? std::nullopt : std::optional<std::string_view>(value))
        .value_or(fallback);
}

} // namespace route_sync_detail

// A virtual mapping must not collide with another virtual mapping or with a
// real target network. Real target networks themselves may repeat: each one is
// translated behind its owning Edge peer.
inline bool virtualMappingConflicts(
    const Ipv4Cidr& candidate, const std::optional<Ipv4Cidr>& realTarget,
    const std::optional<Ipv4Cidr>& existingVirtual) noexcept {
    return (realTarget && candidate.overlaps(*realTarget)) ||
           (existingVirtual && candidate.overlaps(*existingVirtual));
}

inline bool realTargetConflictsVirtual(
    const Ipv4Cidr& candidate, const std::optional<Ipv4Cidr>& existingVirtual) noexcept {
    return existingVirtual && candidate.overlaps(*existingVirtual);
}

// Reconcile the Edge bridge inventory with its VPN routes. The operation keeps
// a user-selected virtual network while the bridge prefix remains compatible.
// A prefix change necessarily selects a new equally-sized virtual network. Real
// LAN prefixes are local to each Edge peer and may repeat across sites; virtual
// prefixes remain globally unique so the Hub can route them unambiguously.
template <typename Db>
ruvia::task<void> syncEdgeBridgeRoutes(Db& db, std::string_view peerId,
                                       std::string_view networkId,
                                       std::string_view edgeNodeId,
                                       std::string_view actorId, service::common::UuidV7Generator& uuidGenerator) {
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
    ruvia::db_query bridgeQuery;
    bridgeQuery.select({ bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"name">()), bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"device">()),
            bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"ipv4">()), bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"prefix_length">()) })
        .from(service::vpn::persistence::EdgeNodeNetworkEntity::table_name())
        .and_where(bridgeQuery.binary(bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"node_id">()), Op::equal,
            bridgeQuery.cast(bridgeQuery.value(edgeNodeId), ruvia::db_data_type::uuid)))
        .and_where(bridgeQuery.binary(bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"is_bridge">()), Op::equal, bridgeQuery.value(true)))
        .and_where(bridgeQuery.binary(bridgeQuery.coalesce({ bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"ipv4">()), bridgeQuery.value("") }), Op::not_equal, bridgeQuery.value("")))
        .and_where(bridgeQuery.between(bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"prefix_length">()), bridgeQuery.value(1), bridgeQuery.value(30)))
        .add_order_by(bridgeQuery.column(service::vpn::persistence::EdgeNodeNetworkEntity::column_name<"name">()));
    const auto bridgeRows = co_await db.query(bridgeQuery);
    std::vector<BridgeRecord> bridges;
    for (const auto& row : bridgeRows) {
        const auto address = route_sync_detail::rowValue(row, 2);
        const auto prefix = route_sync_detail::integer(route_sync_detail::rowValue(row, 3));
        const auto target = networkCidr(address, static_cast<std::uint8_t>(prefix));
        if (!target || !isPrivateIpv4(*target))
            continue;
        const auto name = route_sync_detail::rowValue(row, 1).empty()
                              ? route_sync_detail::rowValue(row, 0)
                              : route_sync_detail::rowValue(row, 1);
        if (!name.empty())
            bridges.push_back(BridgeRecord{name, *target});
    }
    if (bridges.empty())
        service::common::fail(21008, "EdgeNode 尚未上报可映射的私有桥接 LAN 网段", 409);

    ruvia::db_query currentQuery;
    currentQuery.select({ currentQuery.cast(currentQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"id">()), ruvia::db_data_type::text),
            currentQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"lan_interface">()), currentQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"target_cidr">()), currentQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"virtual_cidr">()) })
        .from(service::vpn::persistence::VpnRouteEntity::table_name())
        .and_where(currentQuery.binary(currentQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"edge_peer_id">()), Op::equal,
            currentQuery.cast(currentQuery.value(peerId), ruvia::db_data_type::uuid)))
        .add_order_by(currentQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"id">()));
    const auto currentRows = co_await db.query(currentQuery);
    std::vector<RouteRecord> current;
    current.reserve(currentRows.size());
    for (const auto& row : currentRows)
        current.push_back(RouteRecord{
            route_sync_detail::rowValue(row, 0), route_sync_detail::rowValue(row, 1),
            parseCidr(route_sync_detail::rowValue(row, 2), 1, 30),
            parseCidr(route_sync_detail::rowValue(row, 3), 1, 30)});

    ruvia::db_query allRoutesQuery;
    allRoutesQuery.select({ allRoutesQuery.cast(allRoutesQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"id">()), ruvia::db_data_type::text),
        allRoutesQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"lan_interface">()), allRoutesQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"target_cidr">()), allRoutesQuery.column(service::vpn::persistence::VpnRouteEntity::column_name<"virtual_cidr">()) }).from(service::vpn::persistence::VpnRouteEntity::table_name());
    const auto allRows = co_await db.query(allRoutesQuery);
    std::vector<RouteRecord> allRoutes;
    allRoutes.reserve(allRows.size());
    for (const auto& row : allRows)
        allRoutes.push_back(RouteRecord{
            route_sync_detail::rowValue(row, 0), route_sync_detail::rowValue(row, 1),
            parseCidr(route_sync_detail::rowValue(row, 2), 1, 30),
            parseCidr(route_sync_detail::rowValue(row, 3), 1, 30)});

    const auto conflicts = [&](const Ipv4Cidr& candidate, std::string_view excludedId) {
        for (const auto& route : allRoutes) {
            if (route.id == excludedId)
                continue;
            if (virtualMappingConflicts(candidate, route.target, route.virtualNetwork))
                return true;
        }
        return false;
    };
    const auto chooseVirtual = [&](const Ipv4Cidr& target,
                                   std::string_view excludedId) -> std::optional<Ipv4Cidr> {
        if (const auto preferred = mappedVirtualCidr(target);
            preferred && !preferred->overlaps(target) && !conflicts(*preferred, excludedId))
            return preferred;
        if (target.prefix < kVirtualLanPool.prefix)
            return std::nullopt;
        const auto blockSize = static_cast<std::uint64_t>(target.size());
        for (std::uint64_t offset = 0; offset < kVirtualLanPool.size(); offset += blockSize) {
            const Ipv4Cidr candidate{
                static_cast<std::uint32_t>(kVirtualLanPool.network + offset), target.prefix};
            if (!candidate.overlaps(target) && !conflicts(candidate, excludedId))
                return candidate;
        }
        return std::nullopt;
    };

    std::unordered_set<std::string> claimed;
    for (const auto& bridge : bridges) {
        auto existing = std::find_if(current.begin(), current.end(), [&](const auto& route) {
            return !claimed.contains(route.id) &&
                   ((route.target && route.target->network == bridge.target.network &&
                     route.target->prefix == bridge.target.prefix) ||
                    route.lanInterface == bridge.lanInterface);
        });
        std::string routeId;
        std::optional<Ipv4Cidr> virtualNetwork;
        if (existing != current.end()) {
            routeId = existing->id;
            claimed.emplace(routeId);
            if (existing->virtualNetwork &&
                existing->virtualNetwork->prefix == bridge.target.prefix &&
                kVirtualLanPool.contains(existing->virtualNetwork->network) &&
                kVirtualLanPool.contains(existing->virtualNetwork->network +
                                         existing->virtualNetwork->size() - 1U) &&
                !existing->virtualNetwork->overlaps(bridge.target) &&
                !conflicts(*existing->virtualNetwork, routeId))
                virtualNetwork = existing->virtualNetwork;
            else
                virtualNetwork = chooseVirtual(bridge.target, routeId);
        } else {
            virtualNetwork = chooseVirtual(bridge.target, {});
        }
        const auto excludedId = existing != current.end() ? existing->id : std::string{};
        for (const auto& route : allRoutes) {
            if (route.id == excludedId)
                continue;
            if (realTargetConflictsVirtual(bridge.target, route.virtualNetwork))
                service::common::fail(21002, "真实 LAN 与已有虚拟网段重叠", 409);
        }
        if (!virtualNetwork)
            service::common::fail(21009, "没有可用的全局唯一虚拟网段", 409);
        const auto targetCidr = bridge.target.text();
        const auto virtualCidr = virtualNetwork->text();
        if (existing != current.end()) {
            ruvia::db_query route;
            route.update(service::vpn::persistence::VpnRouteEntity::table_name())
                .set(service::vpn::persistence::VpnRouteEntity::column_name<"network_id">(), route.cast(route.value(networkId), ruvia::db_data_type::uuid))
                .set(service::vpn::persistence::VpnRouteEntity::column_name<"lan_interface">(), route.value(bridge.lanInterface)).set(service::vpn::persistence::VpnRouteEntity::column_name<"target_cidr">(), route.value(targetCidr))
                .set(service::vpn::persistence::VpnRouteEntity::column_name<"virtual_cidr">(), route.value(virtualCidr)).set(service::vpn::persistence::VpnRouteEntity::column_name<"mode">(), route.value("nat"))
                .set(service::vpn::persistence::VpnRouteEntity::column_name<"nat_mode">(), route.value("masquerade")).set(service::vpn::persistence::VpnRouteEntity::column_name<"enabled">(), route.value(true))
                .set(service::vpn::persistence::VpnRouteEntity::column_name<"status">(), route.value("active")).set(service::vpn::persistence::VpnRouteEntity::column_name<"last_error">(), route.value(""))
                .set(service::vpn::persistence::VpnRouteEntity::column_name<"updated_at">(), route.call("now"))
                .and_where(route.binary(route.column(service::vpn::persistence::VpnRouteEntity::column_name<"id">()), Op::equal, route.cast(route.value(routeId), ruvia::db_data_type::uuid)));
            (void)co_await db.execute(route);
            for (auto& route : allRoutes)
                if (route.id == routeId) {
                    route.lanInterface = bridge.lanInterface;
                    route.target = bridge.target;
                    route.virtualNetwork = virtualNetwork;
                }
        } else {
            const auto id = uuidGenerator.next();
            ruvia::db_query route;
            route.insert_into(service::vpn::persistence::VpnRouteEntity::table_name(), { "id", "network_id", "edge_peer_id", "lan_interface", "target_cidr",
                "virtual_cidr", "mode", "nat_mode", "enabled", "status", "created_by" })
                .values({ route.cast(route.value(id), ruvia::db_data_type::uuid),
                    route.cast(route.value(networkId), ruvia::db_data_type::uuid),
                    route.cast(route.value(peerId), ruvia::db_data_type::uuid), route.value(bridge.lanInterface),
                    route.value(targetCidr), route.value(virtualCidr), route.value("nat"), route.value("masquerade"),
                    route.value(true), route.value("active"), route.cast(route.value(actorId), ruvia::db_data_type::uuid) });
            (void)co_await db.execute(route);
            allRoutes.push_back(
                RouteRecord{id, bridge.lanInterface, bridge.target, virtualNetwork});
        }
    }
}

} // namespace feature

// Queue a complete, versioned VPN configuration for an Edge peer. The actor is
// optional for background projection: in that case the VPN network owner is
// used so edge_task.created_by remains a valid audit subject.
template <typename Context>
ruvia::task<void> queueEdgeConfig(Context& c, std::string_view peerId,
                                  std::string_view actorId = {},
                                  std::string_view platformId = {}) {
    using Op = ruvia::db_binary_operator;
    // Capability reports and desktop demand changes may arrive on different
    // Workers. Serialize revisions without sharing their connections or state.
    auto configuration = co_await c.db("vpn-edge-config").begin_transaction();
    ruvia::db_query lock;
    lock.select(lock.call("pg_advisory_xact_lock", {
        lock.call("hashtextextended", {lock.cast(lock.value(peerId), ruvia::db_data_type::text), lock.value(std::int64_t{5282804697543808072})})}));
    (void)co_await configuration.query(lock);
    ruvia::db_query peer;
    const auto capability = peer.call("lower", { peer.coalesce({ peer.binary(
        peer.binary(peer.column(service::vpn::persistence::EdgeNodeEntity::column_name<"capability">(), "e"), Op::json_get, peer.value("vpn")),
        Op::json_get_text, peer.value("supportsVpn")), peer.value("") }) });
    peer.select({ peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"id">(), "p"), peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"network_id">(), "p"), peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"edge_node_id">(), "p"),
            peer.call("host", { peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"assigned_ipv4">(), "p") }), peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"config_revision">(), "p"),
            peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"status">(), "p"), peer.column(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_public_key">(), "n"), peer.column(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_endpoint">(), "n"),
            peer.column(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_listen_port">(), "n"), peer.cast(peer.column(service::vpn::persistence::VpnNetworkEntity::column_name<"created_by">(), "n"), ruvia::db_data_type::text), peer.column(service::vpn::persistence::VpnNetworkEntity::column_name<"status">(), "n") })
        .from(service::vpn::persistence::VpnPeerEntity::table_name(), "p")
        .join(ruvia::db_join_type::inner, service::vpn::persistence::VpnNetworkEntity::table_name(), peer.binary(peer.column(service::vpn::persistence::VpnNetworkEntity::column_name<"id">(), "n"), Op::equal, peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"network_id">(), "p")), "n")
        .join(ruvia::db_join_type::inner, service::vpn::persistence::EdgeNodeEntity::table_name(), peer.binary(peer.column(service::vpn::persistence::EdgeNodeEntity::column_name<"id">(), "e"), Op::equal, peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"edge_node_id">(), "p")), "e")
        .and_where(peer.binary(peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"id">(), "p"), Op::equal, peer.cast(peer.value(peerId), ruvia::db_data_type::uuid)))
        .and_where(peer.binary(peer.column(service::vpn::persistence::VpnPeerEntity::column_name<"peer_type">(), "p"), Op::equal, peer.value("edge")))
        .and_where(peer.binary(peer.column(service::vpn::persistence::EdgeNodeEntity::column_name<"enrollment_status">(), "e"), Op::equal, peer.value("approved")))
        .and_where(peer.binary(capability, Op::in, peer.list({ peer.value("true"), peer.value("t"), peer.value("1") })))
        .limit(1);
    const auto rows = co_await configuration.query(peer);
    if (rows.empty())
        co_return;

    const auto nodeId = detail::edgeConfigRowValue(rows.front(), 2);
    ruvia::db_query routes;
    routes.select({ routes.cast(routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"id">()), ruvia::db_data_type::text), routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"virtual_cidr">()),
            routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"target_cidr">()), routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"mode">()), routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"nat_mode">()), routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"enabled">()) })
        .from(service::vpn::persistence::VpnRouteEntity::table_name())
        .and_where(routes.binary(routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"edge_peer_id">()), Op::equal, routes.cast(routes.value(peerId), ruvia::db_data_type::uuid)))
        .add_order_by(routes.column(service::vpn::persistence::VpnRouteEntity::column_name<"virtual_cidr">())).limit(16);
    const auto routeRows = co_await configuration.query(routes);
    const auto requestId = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
    std::uint8_t requestBytes[16]{};
    if (!service::edge::protocol::uuidBytes(requestId, requestBytes))
        co_return;
    auto envelope = service::edge::protocol::outbound(c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), service::message::utcNowMilliseconds(), c.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
    if (!platformId.empty()) {
        std::uint8_t platformBytes[16]{};
        if (service::edge::protocol::uuidBytes(platformId, platformBytes))
            envelope.set_platform_id(
                service::edge::protocol::bytes(platformBytes, sizeof(platformBytes)));
    }
    auto* request = envelope.mutable_vpn_config_request();
    request->set_request_id(service::edge::protocol::bytes(requestBytes, sizeof(requestBytes)));
    const auto revisionText = detail::edgeConfigRowValue(rows.front(), 4);
    const auto nextVersion = service::utils::parseInt64(
                                 std::optional<std::string_view>(revisionText))
                                 .value_or(0) +
                             1;
    request->set_config_version(static_cast<std::uint64_t>(nextVersion));
    const auto revoked = detail::edgeConfigRowValue(rows.front(), 5) == "revoked";
    const auto networkDisabled = detail::edgeConfigRowValue(rows.front(), 10) != "enabled";
    const auto demand = co_await VpnRuntimeService::desktopDemand(c);
    request->set_enabled(!revoked && !networkDisabled && demand.contains(std::string(peerId)));
    request->set_hub_public_key(detail::edgeConfigRowValue(rows.front(), 6));
    request->set_hub_endpoint(detail::edgeConfigRowValue(rows.front(), 7));
    const auto portText = detail::edgeConfigRowValue(rows.front(), 8);
    request->set_hub_listen_port(static_cast<std::uint32_t>(
        service::utils::parseInt64(std::optional<std::string_view>(portText))
            .value_or(51820)));
    request->set_edge_address(detail::edgeConfigRowValue(rows.front(), 3) + "/32");
    if (request->enabled()) {
        for (const auto& row : routeRows) {
            auto* route = request->add_routes();
            route->set_route_id(detail::edgeConfigRowValue(row, 0));
            route->set_virtual_cidr(detail::edgeConfigRowValue(row, 1));
            route->set_target_cidr(detail::edgeConfigRowValue(row, 2));
            route->set_mode(detail::edgeConfigRowValue(row, 3));
            route->set_nat_mode(detail::edgeConfigRowValue(row, 4));
            route->set_enabled(detail::edgeConfigRowValue(row, 5) == "t");
        }
    }
    const auto wire = service::edge::protocol::encode(envelope);
    if (wire.empty())
        co_return;
    const auto createdBy = actorId.empty()
                               ? detail::edgeConfigRowValue(rows.front(), 9)
                               : std::string(actorId);
    if (createdBy.empty())
        co_return;
    ruvia::db_query superseded;
    superseded.update(service::vpn::persistence::EdgeTaskEntity::table_name()).set(service::vpn::persistence::EdgeTaskEntity::column_name<"status">(), superseded.value("failed"))
        .set(service::vpn::persistence::EdgeTaskEntity::column_name<"result">(), superseded.call("jsonb_build_object", {
            superseded.cast(superseded.value("configVersion"), ruvia::db_data_type::text), superseded.binary(superseded.column(service::vpn::persistence::EdgeTaskEntity::column_name<"request">()), Op::json_get_text, superseded.cast(superseded.value("configVersion"), ruvia::db_data_type::text)),
            superseded.cast(superseded.value("errorCode"), ruvia::db_data_type::text), superseded.cast(superseded.value("superseded"), ruvia::db_data_type::text),
            superseded.cast(superseded.value("errorMessage"), ruvia::db_data_type::text), superseded.cast(superseded.value("superseded by newer VPN configuration"), ruvia::db_data_type::text) }))
        .set(service::vpn::persistence::EdgeTaskEntity::column_name<"updated_at">(), superseded.call("now")).set(service::vpn::persistence::EdgeTaskEntity::column_name<"completed_at">(), superseded.call("now"))
        .and_where(superseded.binary(superseded.column(service::vpn::persistence::EdgeTaskEntity::column_name<"node_id">()), Op::equal, superseded.cast(superseded.value(nodeId), ruvia::db_data_type::uuid)))
        .and_where(superseded.binary(superseded.column(service::vpn::persistence::EdgeTaskEntity::column_name<"task_type">()), Op::equal, superseded.value("vpn")))
        .and_where(superseded.binary(superseded.column(service::vpn::persistence::EdgeTaskEntity::column_name<"status">()), Op::not_in, superseded.list({ superseded.value("succeeded"), superseded.value("failed") })))
        .and_where(superseded.binary(superseded.binary(superseded.column(service::vpn::persistence::EdgeTaskEntity::column_name<"request">()), Op::json_get_text, superseded.value("peerId")),
            Op::equal, superseded.cast(superseded.value(peerId), ruvia::db_data_type::text)));
    ruvia::db_query task;
    task.with("superseded", superseded)
        .insert_into(service::vpn::persistence::EdgeTaskEntity::table_name(), { "id", "node_id", "task_type", "request", "created_by" })
        .values({ task.cast(task.value(requestId), ruvia::db_data_type::uuid), task.cast(task.value(nodeId), ruvia::db_data_type::uuid),
            task.value("vpn"), task.call("jsonb_build_object", {
                task.cast(task.value("peerId"), ruvia::db_data_type::text), task.cast(task.value(peerId), ruvia::db_data_type::text),
                task.cast(task.value("configVersion"), ruvia::db_data_type::text), task.cast(task.value(nextVersion), ruvia::db_data_type::big_int),
                task.cast(task.value("enabled"), ruvia::db_data_type::text), task.cast(task.value(request->enabled()), ruvia::db_data_type::boolean) }),
            task.cast(task.value(createdBy), ruvia::db_data_type::uuid) });
    (void)co_await configuration.execute(task);
    ruvia::db_query revision;
    revision.update(service::vpn::persistence::VpnPeerEntity::table_name()).set(service::vpn::persistence::VpnPeerEntity::column_name<"config_revision">(), revision.value(nextVersion)).set(service::vpn::persistence::VpnPeerEntity::column_name<"updated_at">(), revision.call("now"))
        .and_where(revision.binary(revision.column(service::vpn::persistence::VpnPeerEntity::column_name<"id">()), Op::equal, revision.cast(revision.value(peerId), ruvia::db_data_type::uuid)));
    (void)co_await configuration.execute(revision);
    co_await configuration.commit();
    co_await service::edge::dispatch::enqueue(c.redis(), nodeId, wire);
}

// The existing reconciliation job also services expired desktop resources.
// Compare the last requested state so an idle job never sends duplicate configs.
template <typename Context>
ruvia::task<void> synchronizeEdgeDemand(Context& context) {
    using Op = ruvia::db_binary_operator;
    const auto demand = co_await VpnRuntimeService::desktopDemand(context);
    ruvia::db_query latest;
    latest.select(latest.binary(latest.column(persistence::EdgeTaskEntity::column_name<"request">(), "task"), Op::json_get_text, latest.value("enabled")))
        .from(persistence::EdgeTaskEntity::table_name(), "task")
        .and_where(latest.binary(latest.column(persistence::EdgeTaskEntity::column_name<"task_type">(), "task"), Op::equal, latest.value("vpn")))
        .and_where(latest.binary(latest.binary(latest.column(persistence::EdgeTaskEntity::column_name<"request">(), "task"), Op::json_get_text, latest.value("peerId")), Op::equal,
            latest.cast(latest.column(persistence::VpnPeerEntity::column_name<"id">(), "peer"), ruvia::db_data_type::text)))
        .add_order_by(latest.column(persistence::EdgeTaskEntity::column_name<"created_at">(), "task"), ruvia::db_order_direction::desc)
        .add_order_by(latest.column(persistence::EdgeTaskEntity::column_name<"id">(), "task"), ruvia::db_order_direction::desc)
        .limit(1);
    ruvia::db_query peers;
    peers.select(peers.cast(peers.column(persistence::VpnPeerEntity::column_name<"id">(), "peer"), ruvia::db_data_type::text))
        .add_select(peers.subquery(latest))
        .from(persistence::VpnPeerEntity::table_name(), "peer")
        .and_where(peers.binary(peers.column(persistence::VpnPeerEntity::column_name<"peer_type">(), "peer"), Op::equal, peers.value("edge")));
    const auto rows = co_await context.db().query(peers);
    for (const auto& row : rows) {
        const std::string peerId(row[0].value().value_or(""));
        const auto previous = row[1].value().value_or("");
        const auto desired = demand.contains(peerId) ? "true" : "false";
        if (previous != desired) co_await queueEdgeConfig(context, peerId);
    }
}

} // namespace service::vpn

#include <array>
#include <memory>

#include <openssl/evp.h>
#include <openssl/rand.h>

#include "service/features/vpn/wireguard/wireguard.transport.h"

namespace service::vpn::hub_config {

inline constexpr std::string_view kDefaultNetworkId{
    "00000000-0000-7000-8000-000000000004"};
inline constexpr std::string_view kDefaultNetworkName{"iot-server"};

inline std::string rowValue(const auto& row, std::size_t index) {
    return std::string(row[index].value().value_or(std::string_view{}));
}

inline int base64Value(char value) noexcept {
    if (value >= 'A' && value <= 'Z')
        return value - 'A';
    if (value >= 'a' && value <= 'z')
        return value - 'a' + 26;
    if (value >= '0' && value <= '9')
        return value - '0' + 52;
    if (value == '+')
        return 62;
    if (value == '/')
        return 63;
    return -1;
}

inline bool decodeKey(std::string_view input, std::array<unsigned char, 32>& output) noexcept {
    if (input.size() != 44 || input.back() != '=')
        return false;
    std::size_t outputIndex = 0;
    for (std::size_t index = 0; index < input.size(); index += 4) {
        const auto first = base64Value(input[index]);
        const auto second = base64Value(input[index + 1]);
        const auto third = input[index + 2] == '=' ? 0 : base64Value(input[index + 2]);
        const auto fourth = input[index + 3] == '=' ? 0 : base64Value(input[index + 3]);
        if (first < 0 || second < 0 || third < 0 || fourth < 0 ||
            (index + 2 == input.size() - 1 && input[index + 2] != '=') ||
            (index + 3 == input.size() - 1 && input[index + 3] != '='))
            return false;
        if (outputIndex < output.size())
            output[outputIndex++] = static_cast<unsigned char>((first << 2) | (second >> 4));
        if (index + 2 < input.size() - 1 && outputIndex < output.size())
            output[outputIndex++] = static_cast<unsigned char>((second << 4) | (third >> 2));
        if (index + 3 < input.size() - 1 && outputIndex < output.size())
            output[outputIndex++] = static_cast<unsigned char>((third << 6) | fourth);
    }
    return outputIndex == output.size();
}

inline std::string encodeKey(const std::array<unsigned char, 32>& input) {
    constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string output;
    output.resize(44, '=');
    std::size_t out = 0;
    for (std::size_t index = 0; index < input.size(); index += 3) {
        const auto left = input.size() - index;
        const auto value = (static_cast<unsigned>(input[index]) << 16) |
                           (left > 1 ? static_cast<unsigned>(input[index + 1]) << 8 : 0U) |
                           (left > 2 ? static_cast<unsigned>(input[index + 2]) : 0U);
        output[out++] = alphabet[(value >> 18) & 0x3fU];
        output[out++] = alphabet[(value >> 12) & 0x3fU];
        if (left > 1)
            output[out++] = alphabet[(value >> 6) & 0x3fU];
        if (left > 2)
            output[out++] = alphabet[value & 0x3fU];
    }
    return output;
}

inline bool derivePublicKey(std::string_view privateKey, std::string& publicKey) {
    std::array<unsigned char, 32> privateBytes{};
    if (!decodeKey(privateKey, privateBytes))
        return false;
    using KeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
    KeyPtr key(EVP_PKEY_new_raw_private_key(EVP_PKEY_X25519, nullptr, privateBytes.data(),
                                            privateBytes.size()),
              &EVP_PKEY_free);
    if (!key)
        return false;
    std::array<unsigned char, 32> publicBytes{};
    std::size_t publicSize = publicBytes.size();
    if (EVP_PKEY_get_raw_public_key(key.get(), publicBytes.data(), &publicSize) != 1 ||
        publicSize != publicBytes.size())
        return false;
    publicKey = encodeKey(publicBytes);
    return true;
}

inline bool generateKeyPair(std::string& privateKey, std::string& publicKey) {
    std::array<unsigned char, 32> privateBytes{};
    if (RAND_bytes(privateBytes.data(), static_cast<int>(privateBytes.size())) != 1)
        return false;
    privateKey = encodeKey(privateBytes);
    return derivePublicKey(privateKey, publicKey);
}

template <typename Context>
ruvia::task<std::optional<wireguard::HubConfig>> loadOrInitialize(
    Context& context, const wireguard::HubConfig& fallback) {
    auto transaction = co_await context.db().begin_transaction();
    ruvia::db_query lock;
    lock.select(lock.call("pg_advisory_xact_lock", {
        lock.cast(lock.value(std::int64_t{5282804697543808067}), ruvia::db_data_type::big_int) }));
    (void)co_await transaction.query(lock);
    ruvia::db_query network;
    network.select({ network.column(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_private_key">()), network.column(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_public_key">()),
            network.column(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_endpoint">()), network.column(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_listen_port">()) })
        .from(service::vpn::persistence::VpnNetworkEntity::table_name())
        .and_where(network.binary(network.column(service::vpn::persistence::VpnNetworkEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
            network.cast(network.value(kDefaultNetworkId), ruvia::db_data_type::uuid)))
        .and_where(network.binary(network.column(service::vpn::persistence::VpnNetworkEntity::column_name<"name">()), ruvia::db_binary_operator::equal, network.value(kDefaultNetworkName)))
        .and_where(network.binary(network.column(service::vpn::persistence::VpnNetworkEntity::column_name<"status">()), ruvia::db_binary_operator::equal, network.value("enabled")))
        .and_where(network.unary(ruvia::db_unary_operator::is_null, network.column(service::vpn::persistence::VpnNetworkEntity::column_name<"deleted_at">())))
        .limit(1).lock({ .mode_ = ruvia::db_row_lock::update });
    const auto rows = co_await transaction.query(network);
    if (rows.empty()) {
        co_await transaction.commit();
        if (!wireguard::validKey(fallback.privateKey))
            co_return std::nullopt;
        co_return fallback;
    }

    auto config = fallback;
    const auto storedPrivateKey = rowValue(rows.front(), 0);
    const auto storedPublicKey = rowValue(rows.front(), 1);
    const auto storedEndpoint = rowValue(rows.front(), 2);
    const auto storedPortText = rowValue(rows.front(), 3);
    const auto storedPort = service::utils::parseInt64(
        std::optional<std::string_view>(storedPortText));
    config.endpoint = storedEndpoint.empty() ? fallback.endpoint : storedEndpoint;
    config.listenPort = storedPort && *storedPort > 0 && *storedPort <= 65535
                            ? static_cast<std::uint16_t>(*storedPort)
                            : fallback.listenPort;

    bool persist = false;
    if (storedPrivateKey.empty()) {
        if (wireguard::validKey(fallback.privateKey) &&
            derivePublicKey(fallback.privateKey, config.publicKey)) {
            config.privateKey = fallback.privateKey;
        } else if (!generateKeyPair(config.privateKey, config.publicKey)) {
            co_return std::nullopt;
        }
        persist = true;
    } else {
        if (!wireguard::validKey(storedPrivateKey) ||
            !derivePublicKey(storedPrivateKey, config.publicKey))
            co_return std::nullopt;
        config.privateKey = storedPrivateKey;
        persist = storedPublicKey != config.publicKey;
    }
    if (storedEndpoint.empty() && !fallback.endpoint.empty())
        persist = true;
    if (!storedPort || *storedPort != config.listenPort)
        persist = true;

    if (persist) {
        ruvia::db_query update;
        update.update(service::vpn::persistence::VpnNetworkEntity::table_name()).set(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_private_key">(), update.value(config.privateKey))
            .set(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_public_key">(), update.value(config.publicKey)).set(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_endpoint">(), update.value(config.endpoint))
            .set(service::vpn::persistence::VpnNetworkEntity::column_name<"hub_listen_port">(), update.value(static_cast<int>(config.listenPort)))
            .set(service::vpn::persistence::VpnNetworkEntity::column_name<"updated_at">(), update.call("now"))
            .and_where(update.binary(update.column(service::vpn::persistence::VpnNetworkEntity::column_name<"id">()), ruvia::db_binary_operator::equal,
                update.cast(update.value(kDefaultNetworkId), ruvia::db_data_type::uuid)));
        (void)co_await transaction.execute(update);
    }
    co_await transaction.commit();
    co_return config;
}

} // namespace service::vpn::hub_config

namespace service::vpn {

class VpnHubService final {
  public:
    static ruvia::task<wireguard::RuntimeStatus> reconcile(
        ruvia::web_worker_context& context,
        const wireguard::HubConfig& fallback,
        bool background = false
    ) {
        // The interface is an external singleton. All workers run the same job,
        // but only the transaction owner applies a particular reconciliation.
        // A separate worker-local connection keeps the lock while business
        // queries use their ordinary connection, without pool self-deadlock.
        auto ownership = co_await context.db("vpn-coordination").begin_transaction();
        if (!(co_await VpnRuntimeService::acquireReconciliation(ownership, context.pool(), background))) {
            co_return wireguard::RuntimeStatus{ .code = "reconciliation_in_progress" };
        }
        const auto instance = service::runtime::instanceId();
        auto schedules = context.redis().get_repository<ReconciliationSchedule>();
        if (background) {
            const ruvia::redis_find_options options{
                .where_ = ReconciliationSchedule::field<"id">() == instance};
            if (co_await schedules.exists(options)) {
                co_return wireguard::RuntimeStatus{ .code = "reconciliation_current" };
            }
        }
        // Keep address allocation behind the complete kernel reconciliation.
        // Otherwise an older snapshot could install a revoked key after its
        // address has already been returned to a newly enrolled client.
        co_await VpnRuntimeService::lockAddressAllocation(ownership, context.pool());
        co_await synchronizeEdgeDemand(context);
        auto result = co_await reconcileLocal(context, fallback);
        if (result.configured) {
            ReconciliationSchedule schedule(context.pool());
            schedule.set<"id">(instance);
            const ruvia::redis_write_options expiration{.ttl_ = std::chrono::seconds(10)};
            (void)co_await schedules.upsert(schedule, expiration);
        }
        co_await ownership.commit();
        co_return result;
    }

    static ruvia::task<void> removePeer(ruvia::web_worker_context& context,
        const wireguard::HubConfig& fallback, std::string_view payload) {
            if (!wireguard::validKey(payload)) {
                service::common::fail(10002, "WireGuard Peer 公钥无效", 400);
            }
            auto ownership = co_await context.db("vpn-coordination").begin_transaction();
            (void)co_await VpnRuntimeService::acquireReconciliation(ownership, context.pool(), false);
            const auto config = co_await hub_config::loadOrInitialize(context, fallback);
            if (config) {
                const auto result = wireguard::controller().removePeer(*config, payload);
                if (result.supported && !result.configured) {
                    service::common::fail(21005, "VPN Hub peer removal failed: " + result.message, 503);
                }
            }
            co_await ownership.commit();
    }

  private:
    static ruvia::task<wireguard::RuntimeStatus> reconcileLocal(
        ruvia::web_worker_context& context,
        const wireguard::HubConfig& fallback
    ) {
        const auto config = co_await hub_config::loadOrInitialize(context, fallback);
        if (!config) {
            co_return wireguard::RuntimeStatus{
                .supported = true,
                .configured = false,
                .code = "hub_config_missing",
                .message = "WireGuard Hub 配置尚未初始化",
                .peerCount = 0
            };
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
            co_return wireguard::RuntimeStatus{
                .supported = true,
                .configured = false,
                .code = "peer_inventory_failed",
                .message = "Unable to read WireGuard peers before reconciliation"
            };
        }
        std::unordered_set<std::string> expected;
        for (const auto& peerRecord : peers) {
            if (wireguard::validKey(peerRecord.publicKey) && parseIpv4(peerRecord.assignedIpv4)) {
                expected.insert(peerRecord.publicKey);
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
        std::size_t configuredPeers = 0;
        for (const auto& peerRecord : peers) {
            const auto& publicKey = peerRecord.publicKey;
            const auto& assigned = peerRecord.assignedIpv4;
            const auto& peerType = peerRecord.peerType;
            if (!wireguard::validKey(publicKey) || !parseIpv4(assigned)) {
                continue;
            }
            wireguard::Peer peer;
            peer.publicKey = publicKey;
            peer.allowedIps.emplace_back(assigned + "/32");
            if (peerType == "edge") {
                firewall::ClientAccess client{ .assignedIpv4 = assigned };
                std::stringstream routes(peerRecord.sourceRoutes);
                std::string route;
                while (std::getline(routes, route, ',')) {
                    if (!route.empty() && route.front() == ' ') {
                        route.erase(route.begin());
                    }
                    if (!route.empty()) {
                        peer.allowedIps.push_back(route);
                        client.sourceRoutes.push_back(std::move(route));
                    }
                }
                std::stringstream allowedRoutes(peerRecord.allowedRoutes);
                while (std::getline(allowedRoutes, route, ',')) {
                    if (!route.empty() && route.front() == ' ') {
                        route.erase(route.begin());
                    }
                    if (!route.empty()) {
                        client.allowedRoutes.push_back(std::move(route));
                    }
                }
                clients.push_back(std::move(client));
            } else if (peerType == "windows") {
                firewall::ClientAccess client{ .assignedIpv4 = assigned };
                std::stringstream routes(peerRecord.allowedRoutes);
                std::string route;
                while (std::getline(routes, route, ',')) {
                    if (!route.empty() && route.front() == ' ') {
                        route.erase(route.begin());
                    }
                    if (!route.empty()) {
                        client.allowedRoutes.push_back(std::move(route));
                    }
                }
                std::stringstream edgeAddresses(peerRecord.edgeAddresses);
                std::string edgeAddress;
                while (std::getline(edgeAddresses, edgeAddress, ',')) {
                    if (!edgeAddress.empty() && edgeAddress.front() == ' ') {
                        edgeAddress.erase(edgeAddress.begin());
                    }
                    if (!edgeAddress.empty()) {
                        client.edgeAddresses.push_back(std::move(edgeAddress));
                    }
                }
                clients.push_back(std::move(client));
            }
            expectedRoutes.insert(expectedRoutes.end(), peer.allowedIps.begin(), peer.allowedIps.end());
            configured.push_back(std::move(peer));
        }

        const auto firewallResult = firewall::apply(config->interfaceName, clients);
        if (!firewallResult.configured) {
            co_return wireguard::RuntimeStatus{
                .supported = true,
                .configured = false,
                .code = "firewall_configure_failed",
                .message = firewallResult.message,
                .peerCount = configuredPeers
            };
        }
        // Install the new address permissions before enabling its new key.
        // Recycled addresses must never inherit a previous client's firewall access.
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
                co_await VpnRuntimeService::updatePeerHandshake(
                    context,
                    publicKey,
                    static_cast<std::int64_t>(seconds)
                );
            }
        }
        result.peerCount = configuredPeers;
        result.message = "WireGuard hub is configured";
        co_return result;
    }

};

} // namespace service::vpn

namespace service::vpn {

class VpnControlService final {
  public:
    explicit VpnControlService(
        wireguard::HubConfig fallback,
        std::string platformId =
            std::string(service::edge::protocol::kDefaultPlatformId)
    )
        : fallback_(std::move(fallback)), platformId_(std::move(platformId)) {}

    ruvia::task<std::string> executeOperation(ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) const {
        if (stop.stop_requested()) {
            service::common::fail(10004, "VPN background operation cancelled", 503);
        }

        if (operation == "queue-edge-config") {
            const auto separator = payload.find('\n');
            if (separator == std::string_view::npos || separator == 0 ||
                separator + 1 >= payload.size() ||
                !service::common::isUuid(payload.substr(0, separator)) ||
                !service::common::isUuid(payload.substr(separator + 1))) {
                service::common::fail(10002, "VPN Edge 配置请求无效", 400);
            }
            co_await queueEdgeConfig(context, payload.substr(0, separator), payload.substr(separator + 1), platformId_);
            co_return "{}";
        }

        if (operation == "reconcile" || operation == "wireguard-reconcile" ||
            operation == "firewall-reconcile") {
            const auto result = co_await VpnHubService::reconcile(context, fallback_);
            if (operation == "reconcile" && result.supported && !result.configured &&
                result.code != "hub_config_missing") {
                service::common::fail(21005, "VPN Hub reconciliation failed: " + result.message, 503);
            }
            co_return runtimeStatusJson(result);
        }

        if (operation == "wireguard-remove-peer") {
            co_await VpnHubService::removePeer(context, fallback_, payload);
            co_return "{}";
        }

        service::common::fail(10002, "Unknown VPN background operation", 400);
    }

  private:
    static std::string runtimeStatusJson(const wireguard::RuntimeStatus& result) {
    return "{\"platformSupported\":" +
        std::string(result.supported ? "true" : "false") +
        ",\"configured\":" + std::string(result.configured ? "true" : "false") +
        ",\"code\":" + service::utils::jsonQuoted(result.code) +
        ",\"message\":" + service::utils::jsonQuoted(result.message) +
        ",\"runtimePeerCount\":" + std::to_string(result.peerCount) + "}";
}

    const wireguard::HubConfig fallback_;
    const std::string platformId_;
};

} // namespace service::vpn
