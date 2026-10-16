#pragma once

#include <tuple>

#include <memory>
#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <ruvia/web/context.h>
#include <ruvia/web/model_object.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/middleware/rpc.h"
#include "service/modules/vpn/vpn.entity.h"
#include "service/modules/vpn/vpn.types.h"
#include "service/utils/json.h"
#include "service/utils/number.h"

namespace service::vpn {

namespace cidr = module;

inline constexpr std::string_view kDefaultNetworkId{
    "00000000-0000-7000-8000-000000000004"
};
inline constexpr std::string_view kDefaultNetworkName{ "iot-server" };
inline constexpr std::string_view kDefaultOverlayCidr{ "100.96.0.0/16" };

namespace detail {

inline std::string jsonArray(const std::vector<std::string>& values) {
    std::string result{ "[" };
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (index != 0) {
            result.push_back(',');
        }
        result += service::utils::jsonQuoted(values[index]);
    }
    result.push_back(']');
    return result;
}

inline ruvia::db_expression jsonKey(ruvia::db_query& query, std::string_view key) {
    return query.cast(query.value(key), ruvia::db_data_type::text);
}

inline bool validKey(std::string_view value) noexcept {
    if (value.size() != 44 || value.back() != '=') {
        return false;
    }
    for (const auto character : value) {
        if (!((character >= 'A' && character <= 'Z') ||
              (character >= 'a' && character <= 'z') ||
              (character >= '0' && character <= '9') || character == '+' || character == '/' ||
              character == '=')) {
            return false;
        }
    }
    return true;
}

inline int base64Value(char value) noexcept {
    if (value >= 'A' && value <= 'Z') {
        return value - 'A';
    }
    if (value >= 'a' && value <= 'z') {
        return value - 'a' + 26;
    }
    if (value >= '0' && value <= '9') {
        return value - '0' + 52;
    }
    if (value == '+') {
        return 62;
    }
    if (value == '/') {
        return 63;
    }
    return -1;
}

inline bool decodeKey(std::string_view input, std::array<unsigned char, 32>& output) noexcept {
    if (input.size() != 44 || input.back() != '=') {
        return false;
    }
    std::size_t outputIndex = 0;
    for (std::size_t index = 0; index < input.size(); index += 4) {
        const auto first = base64Value(input[index]);
        const auto second = base64Value(input[index + 1]);
        const auto third = input[index + 2] == '=' ? 0 : base64Value(input[index + 2]);
        const auto fourth = input[index + 3] == '=' ? 0 : base64Value(input[index + 3]);
        if (first < 0 || second < 0 || third < 0 || fourth < 0 ||
            (index + 2 == input.size() - 1 && input[index + 2] != '=') ||
            (index + 3 == input.size() - 1 && input[index + 3] != '=')) {
            return false;
        }
        if (outputIndex < output.size()) {
            output[outputIndex++] = static_cast<unsigned char>((first << 2) | (second >> 4));
        }
        if (index + 2 < input.size() - 1 && outputIndex < output.size()) {
            output[outputIndex++] = static_cast<unsigned char>((second << 4) | (third >> 2));
        }
        if (index + 3 < input.size() - 1 && outputIndex < output.size()) {
            output[outputIndex++] = static_cast<unsigned char>((third << 6) | fourth);
        }
    }
    return outputIndex == output.size();
}

inline bool validManagedKey(std::string_view value) noexcept {
    if (value.size() != 44 || value.back() != '=') {
        return false;
    }
    for (std::size_t index = 0; index < 43; ++index) {
        if (base64Value(value[index]) < 0) {
            return false;
        }
    }
    if ((base64Value(value[42]) & 3) != 0) {
        return false;
    }
    std::array<unsigned char, 32> decoded{};
    return decodeKey(value, decoded) &&
        std::any_of(decoded.begin(), decoded.end(), [](auto byte) {
               return byte != 0;
           });
}

template <typename Db>
ruvia::task<void> validateSelectedEdges(Db& db, std::string_view networkId, const std::vector<std::string>& ids) {
    if (ids.empty()) {
        co_return;
    }
    using Op = ruvia::db_binary_operator;
    ruvia::db_query query;
    std::vector<ruvia::db_query::expr_type> selectedIds;
    selectedIds.reserve(ids.size());
    for (const auto& id : ids) {
        selectedIds.push_back(query.cast(query.value(id), ruvia::db_data_type::uuid));
    }
    query.select(query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"id">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"))
        .distinct()
        .from(service::vpn::entities::EdgeNodeEntity::table_name(), "edge")
        .join(ruvia::db_join_type::inner, service::vpn::entities::VpnPeerEntity::table_name(), query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"id">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge")), "peer")
        .join(ruvia::db_join_type::inner, service::vpn::entities::VpnNetworkEntity::table_name(), query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), Op::equal, query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer")), "network")
        .where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"id">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"), Op::in, query.list(selectedIds)))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"enrollment_status">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"), Op::equal, query.value("approved")))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.value("edge")))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.value("active")))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.cast(query.value(networkId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), Op::equal, query.value("enabled")))
        .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network")));
    const auto rows = co_await db.query(query);
    if (rows.size() != ids.size()) {
        service::common::fail(21001, "所选 Edge 节点尚未批准或没有可用的 VPN 网络", 400);
    }
}

inline std::string hostText(std::uint32_t address) {
    return std::to_string((address >> 24U) & 0xffU) + "." +
        std::to_string((address >> 16U) & 0xffU) + "." +
        std::to_string((address >> 8U) & 0xffU) + "." +
        std::to_string(address & 0xffU);
}

inline std::string rowValue(const auto& row, std::size_t index) {
    return std::string(row[index].value().value_or(std::string_view{}));
}

template <typename Db>
ruvia::task<void> advisoryLock(Db& db, std::pmr::memory_resource* resource, std::int64_t key) {
    ruvia::db_query lock(resource);
    lock.select(lock.call("pg_advisory_xact_lock", { lock.cast(lock.value(key), ruvia::db_data_type::big_int) }));
    (void)co_await db.query(lock);
}

} // namespace detail

namespace route_sync_detail {

inline std::int64_t integer(std::string_view value, std::int64_t fallback = 0) {
    return service::utils::parseInt64(
               value.empty() ? std::nullopt : std::optional<std::string_view>(value)
    )
        .value_or(fallback);
}

} // namespace route_sync_detail

// Reconcile the Edge bridge inventory with its VPN routes. This database
// projection belongs to the management module; the VPN feature only receives
// the resulting edge configuration through its Redis control operation.
template <typename Db>
ruvia::task<void> syncEdgeBridgeRoutes(Db& db, std::string_view peerId, std::string_view networkId, std::string_view edgeNodeId, std::string_view actorId, service::common::UuidV7Generator& uuidGenerator) {
    struct RouteRecord final {
        std::string id;
        std::string lanInterface;
        std::optional<cidr::Ipv4Cidr> target;
        std::optional<cidr::Ipv4Cidr> virtualNetwork;
    };

    struct BridgeRecord final {
        std::string lanInterface;
        cidr::Ipv4Cidr target;
    };

    using Op = ruvia::db_binary_operator;
    ruvia::db_query bridgeQuery;
    bridgeQuery
        .select({ bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"name">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()), bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"device">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()), bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"ipv4">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()), bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"prefix_length">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()) })
        .from(service::vpn::entities::EdgeNodeNetworkEntity::table_name())
        .where(bridgeQuery.binary(
            bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"node_id">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()),
            Op::equal,
            bridgeQuery.cast(bridgeQuery.value(edgeNodeId), ruvia::db_data_type::uuid)
        ))
        .and_where(bridgeQuery.binary(bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"is_bridge">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()), Op::equal, bridgeQuery.value(true)))
        .and_where(bridgeQuery.binary(
            bridgeQuery.coalesce({ bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"ipv4">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()), bridgeQuery.value("") }),
            Op::not_equal,
            bridgeQuery.value("")
        ))
        .and_where(bridgeQuery.between(bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"prefix_length">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()), bridgeQuery.value(1), bridgeQuery.value(30)))
        .order_by(bridgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeNetworkEntity::column_index<"name">(), service::vpn::entities::EdgeNodeNetworkEntity::columns_type>::name.view()));
    const auto bridgeRows = co_await db.query(bridgeQuery);
    std::vector<BridgeRecord> bridges;
    for (const auto& row : bridgeRows) {
        const auto address = detail::rowValue(row, 2);
        const auto prefix = route_sync_detail::integer(detail::rowValue(row, 3));
        const auto target = cidr::networkCidr(address, static_cast<std::uint8_t>(prefix));
        if (!target || !cidr::isPrivateIpv4(*target)) {
            continue;
        }
        const auto name = detail::rowValue(row, 1).empty()
            ? detail::rowValue(row, 0)
            : detail::rowValue(row, 1);
        if (!name.empty()) {
            bridges.push_back(BridgeRecord{ name, *target });
        }
    }
    if (bridges.empty()) {
        service::common::fail(21008, "EdgeNode 尚未上报可映射的私有桥接 LAN 网段", 409);
    }

    ruvia::db_query currentQuery;
    currentQuery
        .select({ currentQuery.cast(currentQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), ruvia::db_data_type::text), currentQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"lan_interface">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), currentQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"target_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), currentQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()) })
        .from(service::vpn::entities::VpnRouteEntity::table_name())
        .where(currentQuery.binary(
            currentQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"edge_peer_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()),
            Op::equal,
            currentQuery.cast(currentQuery.value(peerId), ruvia::db_data_type::uuid)
        ))
        .order_by(currentQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()));
    const auto currentRows = co_await db.query(currentQuery);
    std::vector<RouteRecord> current;
    current.reserve(currentRows.size());
    for (const auto& row : currentRows) {
        current.push_back(RouteRecord{ detail::rowValue(row, 0), detail::rowValue(row, 1), cidr::parseCidr(detail::rowValue(row, 2), 1, 30), cidr::parseCidr(detail::rowValue(row, 3), 1, 30) });
    }

    ruvia::db_query allRoutesQuery;
    allRoutesQuery
        .select({ allRoutesQuery.cast(allRoutesQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), ruvia::db_data_type::text), allRoutesQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"lan_interface">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), allRoutesQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"target_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), allRoutesQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()) })
        .from(service::vpn::entities::VpnRouteEntity::table_name());
    const auto allRows = co_await db.query(allRoutesQuery);
    std::vector<RouteRecord> allRoutes;
    allRoutes.reserve(allRows.size());
    for (const auto& row : allRows) {
        allRoutes.push_back(RouteRecord{ detail::rowValue(row, 0), detail::rowValue(row, 1), cidr::parseCidr(detail::rowValue(row, 2), 1, 30), cidr::parseCidr(detail::rowValue(row, 3), 1, 30) });
    }

    const auto conflicts = [&](const cidr::Ipv4Cidr& candidate, std::string_view excludedId) {
        for (const auto& route : allRoutes) {
            if (route.id == excludedId) {
                continue;
            }
            if (cidr::virtualMappingConflicts(candidate, route.target, route.virtualNetwork)) {
                return true;
            }
        }
        return false;
    };
    const auto chooseVirtual = [&](const cidr::Ipv4Cidr& target,
                                   std::string_view excludedId) -> std::optional<cidr::Ipv4Cidr> {
        if (const auto preferred = cidr::mappedVirtualCidr(target);
            preferred && !preferred->overlaps(target) && !conflicts(*preferred, excludedId)) {
            return preferred;
        }
        if (target.prefix < cidr::kVirtualLanPool.prefix) {
            return std::nullopt;
        }
        const auto blockSize = static_cast<std::uint64_t>(target.size());
        for (std::uint64_t offset = 0; offset < cidr::kVirtualLanPool.size(); offset += blockSize) {
            const cidr::Ipv4Cidr candidate{
                static_cast<std::uint32_t>(cidr::kVirtualLanPool.network + offset),
                target.prefix
            };
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
                ((route.target && route.target->network == bridge.target.network &&
                  route.target->prefix == bridge.target.prefix) ||
                 route.lanInterface == bridge.lanInterface);
        });
        std::string routeId;
        std::optional<cidr::Ipv4Cidr> virtualNetwork;
        if (existing != current.end()) {
            routeId = existing->id;
            claimed.emplace(routeId);
            if (existing->virtualNetwork &&
                existing->virtualNetwork->prefix == bridge.target.prefix &&
                cidr::kVirtualLanPool.contains(existing->virtualNetwork->network) &&
                cidr::kVirtualLanPool.contains(existing->virtualNetwork->network + existing->virtualNetwork->size() - 1U) &&
                !existing->virtualNetwork->overlaps(bridge.target) &&
                !conflicts(*existing->virtualNetwork, routeId)) {
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
            if (cidr::realTargetConflictsVirtual(bridge.target, route.virtualNetwork)) {
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
            route.update(service::vpn::entities::VpnRouteEntity::table_name())
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"network_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.cast(route.value(networkId), ruvia::db_data_type::uuid))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"lan_interface">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.value(bridge.lanInterface))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"target_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.value(targetCidr))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.value(virtualCidr))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"mode">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.value("nat"))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"nat_mode">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.value("masquerade"))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"enabled">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.value(true))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"status">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.value("active"))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"last_error">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.value(""))
                .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"updated_at">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), route.call("now"))
                .where(route.binary(route.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), Op::equal, route.cast(route.value(routeId), ruvia::db_data_type::uuid)));
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
            route.insert_into(service::vpn::entities::VpnRouteEntity::table_name(), { "id", "network_id", "edge_peer_id", "lan_interface", "target_cidr", "virtual_cidr", "mode", "nat_mode", "enabled", "status", "created_by" })
                .values({ route.cast(route.value(id), ruvia::db_data_type::uuid), route.cast(route.value(networkId), ruvia::db_data_type::uuid), route.cast(route.value(peerId), ruvia::db_data_type::uuid), route.value(bridge.lanInterface), route.value(targetCidr), route.value(virtualCidr), route.value("nat"), route.value("masquerade"), route.value(true), route.value("active"), route.cast(route.value(actorId), ruvia::db_data_type::uuid) });
            (void)co_await db.execute(route);
            allRoutes.push_back(
                RouteRecord{ id, bridge.lanInterface, bridge.target, virtualNetwork }
            );
        }
    }
}

class VpnService final {
  private:
    static std::int64_t integer(std::string_view, std::int64_t = 0);
    static void requireUuid(std::string_view, std::string_view);
    template <typename Context>
    static ruvia::task<std::string> ensureDefaultNetwork(Context&);
    template <typename Context>
    static ruvia::task<void> audit(Context&, std::string_view, std::string_view, std::string_view, std::string_view, std::string_view, std::string_view);
    template <typename Context>
    static ruvia::task<std::string> control(Context&, std::string_view, std::string);
    template <typename Context>
    static ruvia::task<void> queueEdgeConfig(Context&, std::string_view);
    template <typename Db>
    static ruvia::task<std::optional<std::uint32_t>> allocateAddressFromDb(
        Db&,
        std::string_view,
        const cidr::Ipv4Cidr&
    );
    template <typename Context>
    static ruvia::task<void> validateAllowedRoutes(Context&, std::string_view, const std::vector<std::string>&);
    template <typename Context>
    static ruvia::task<void> reconcileHub(Context&);

  public:
    static VpnService& instance() {
        static thread_local VpnService value;
        return value;
    }

    template <typename Context>
    ruvia::task<std::string> networks(Context& c, std::int64_t page, std::int64_t pageSize, std::optional<std::string> keyword, std::optional<std::string> status) {
        using Op = ruvia::db_binary_operator;
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 100);
        if (page - 1 > std::numeric_limits<std::int64_t>::max() / pageSize) {
            service::common::fail(21001, "分页超出允许范围", 400);
        }
        const auto defaultNetworkId = co_await ensureDefaultNetwork(c);
        ruvia::db_query count(c.pool());
        count.select(count.aggregate("count", { count.star() }))
            .from(service::vpn::entities::VpnNetworkEntity::table_name(), "n")
            .where(count.binary(count.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), Op::equal, count.cast(count.value(defaultNetworkId), ruvia::db_data_type::uuid)))
            .and_where(count.unary(ruvia::db_unary_operator::is_null, count.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n")));
        if (keyword && !keyword->empty()) {
            const auto pattern = "%" + *keyword + "%";
            count.and_where(count.binary(count.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"name">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), Op::i_like, count.value(pattern)));
        }
        if (status && !status->empty()) {
            count.and_where(count.binary(count.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), Op::equal, count.value(*status)));
        }
        const auto countRows = co_await c.db().query(count);
        const auto total = integer(detail::rowValue(countRows.front(), 0));
        ruvia::db_query peerCount(c.pool());
        peerCount.select(peerCount.aggregate("count", { peerCount.star() }))
            .from(service::vpn::entities::VpnPeerEntity::table_name(), "p")
            .where(peerCount.binary(peerCount.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), Op::equal, peerCount.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n")))
            .and_where(peerCount.binary(peerCount.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), Op::not_equal, peerCount.value("revoked")));
        ruvia::db_query routeCount(c.pool());
        routeCount.select(routeCount.aggregate("count", { routeCount.star() }))
            .from(service::vpn::entities::VpnRouteEntity::table_name(), "r")
            .where(routeCount.binary(routeCount.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"network_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), Op::equal, routeCount.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n")))
            .and_where(routeCount.binary(routeCount.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"enabled">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), Op::equal, routeCount.value(true)));
        ruvia::db_query listQuery(c.pool());
        listQuery
            .select(listQuery.call(
                "jsonb_build_object",
                { detail::jsonKey(listQuery, "id"), listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), detail::jsonKey(listQuery, "name"), listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"name">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), detail::jsonKey(listQuery, "overlayCidr"), listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"overlay_cidr">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), detail::jsonKey(listQuery, "hubPublicKey"), listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"hub_public_key">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), detail::jsonKey(listQuery, "hubEndpoint"), listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"hub_endpoint">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), detail::jsonKey(listQuery, "hubListenPort"), listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"hub_listen_port">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), detail::jsonKey(listQuery, "status"), listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), detail::jsonKey(listQuery, "peerCount"), listQuery.subquery(peerCount), detail::jsonKey(listQuery, "routeCount"), listQuery.subquery(routeCount), detail::jsonKey(listQuery, "createdAt"), listQuery.call("iot_utc_timestamp", { listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"created_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n") }), detail::jsonKey(listQuery, "updatedAt"), listQuery.call("iot_utc_timestamp", { listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"updated_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n") }) }
            ))
            .from(service::vpn::entities::VpnNetworkEntity::table_name(), "n")
            .where(listQuery.binary(listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), Op::equal, listQuery.cast(listQuery.value(defaultNetworkId), ruvia::db_data_type::uuid)))
            .and_where(listQuery.unary(ruvia::db_unary_operator::is_null, listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n")));
        if (keyword && !keyword->empty()) {
            const auto pattern = "%" + *keyword + "%";
            listQuery.and_where(listQuery.binary(listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"name">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), Op::i_like, listQuery.value(pattern)));
        }
        if (status && !status->empty()) {
            listQuery.and_where(listQuery.binary(listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), Op::equal, listQuery.value(*status)));
        }
        listQuery.order_by(listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"created_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), ruvia::db_order_direction::desc)
            .add_order_by(listQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "n"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>((page - 1) * pageSize));
        const auto rows = co_await c.db().query(listQuery);
        std::string list{ "[" };
        for (std::size_t index = 0; index < rows.size(); ++index) {
            if (index != 0) {
                list.push_back(',');
            }
            list += detail::rowValue(rows[index], 0);
        }
        list.push_back(']');
        co_return "{\"list\":" + list + ",\"total\":" + std::to_string(total) +
            ",\"page\":" + std::to_string(page) + ",\"pageSize\":" +
            std::to_string(pageSize) + ",\"totalPages\":" +
            std::to_string(total == 0 ? 0 : (total + pageSize - 1) / pageSize) + "}";
    }

    template <typename Context>
    ruvia::task<std::string> routes(Context& c, std::optional<std::string> networkId, std::optional<std::string> edgeNodeId = std::nullopt) {
        using Op = ruvia::db_binary_operator;
        ruvia::db_query query(c.pool());
        query
            .select(query.call(
                "jsonb_build_object",
                { detail::jsonKey(query, "id"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "networkId"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"network_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "edgePeerId"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"edge_peer_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "edgeNodeId"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "lanInterface"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"lan_interface">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "targetCidr"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"target_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "virtualCidr"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "mode"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"mode">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "natMode"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"nat_mode">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "status"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"status">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "enabled"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"enabled">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "lastError"), query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"last_error">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), detail::jsonKey(query, "createdAt"), query.call("iot_utc_timestamp", { query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"created_at">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r") }), detail::jsonKey(query, "updatedAt"), query.call("iot_utc_timestamp", { query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"updated_at">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r") }) }
            ))
            .from(service::vpn::entities::VpnRouteEntity::table_name(), "r")
            .join(ruvia::db_join_type::inner, service::vpn::entities::VpnPeerEntity::table_name(), query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), Op::equal, query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"edge_peer_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r")), "p");
        if (networkId && !networkId->empty()) {
            requireUuid(*networkId, "VPN 网络 ID 无效");
            query.where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"network_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), Op::equal, query.cast(query.value(*networkId), ruvia::db_data_type::uuid)));
        }
        if (edgeNodeId && !edgeNodeId->empty()) {
            requireUuid(*edgeNodeId, "Edge 节点 ID 无效");
            query.and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), Op::equal, query.cast(query.value(*edgeNodeId), ruvia::db_data_type::uuid)));
        }
        query.order_by(query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"created_at">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "r"), ruvia::db_order_direction::desc)
            .limit(1000);
        const auto rows = co_await c.db().query(query);
        std::string result{ "[" };
        for (std::size_t index = 0; index < rows.size(); ++index) {
            if (index != 0) {
                result.push_back(',');
            }
            result += detail::rowValue(rows[index], 0);
        }
        result.push_back(']');
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> updateRoute(Context& c, std::string_view id, const VpnRoutePatchBody& payload) {
        using Op = ruvia::db_binary_operator;
        requireUuid(id, "VPN 路由 ID 无效");
        ruvia::db_query current(c.pool());
        current.select({ current.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"edge_peer_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), current.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"network_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), current.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"lan_interface">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), current.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"target_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), current.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), current.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"mode">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), current.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"enabled">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()) })
            .from(service::vpn::entities::VpnRouteEntity::table_name())
            .where(current.binary(current.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), Op::equal, current.cast(current.value(id), ruvia::db_data_type::uuid)));
        const auto rows = co_await c.db().query(current);
        if (rows.empty()) {
            service::common::fail(21004, "VPN 路由不存在", 404);
        }
        const auto edgePeerId = detail::rowValue(rows.front(), 0);
        const auto currentInterface = detail::rowValue(rows.front(), 2);
        const auto currentTarget = detail::rowValue(rows.front(), 3);
        const auto currentVirtual = detail::rowValue(rows.front(), 4);
        const auto currentMode = detail::rowValue(rows.front(), 5);
        const auto currentEnabled = detail::rowValue(rows.front(), 6) == "t";
        const auto requestedTarget = optionalVpnText(payload.get<"targetCidr">());
        const auto requestedInterface = optionalVpnText(payload.get<"lanInterface">());
        const auto requestedMode = optionalVpnText(payload.get<"mode">());
        const auto& requestedEnabled = payload.get<"enabled">();
        if ((requestedTarget && *requestedTarget != currentTarget) ||
            (requestedInterface && *requestedInterface != currentInterface) ||
            (requestedMode && *requestedMode != currentMode) ||
            (requestedEnabled && requestedEnabled->value_ != currentEnabled)) {
            service::common::fail(21003, "VPN 映射中只有虚拟网段可以修改", 409);
        }
        const auto targetText = currentTarget;
        const auto virtualText = optionalVpnText(payload.get<"virtualCidr">()).value_or(currentVirtual);
        const auto target = cidr::parseCidr(targetText, 1, 30);
        const auto virtualNetwork = cidr::parseCidr(virtualText, 1, 30);
        if (!target || !virtualNetwork || target->prefix != virtualNetwork->prefix ||
            !cidr::isPrivateIpv4(*target) || !cidr::kVirtualLanPool.contains(virtualNetwork->network) ||
            !cidr::kVirtualLanPool.contains(virtualNetwork->network + virtualNetwork->size() - 1U) ||
            virtualNetwork->overlaps(*target)) {
            service::common::fail(21001, "VPN 路由 CIDR 无效或不是等长私网映射", 400);
        }
        if (currentMode != "nat" || !currentEnabled) {
            service::common::fail(21003, "VPN 自动映射只能保持启用的 NAT 模式", 409);
        }
        auto transaction = co_await c.db().begin_transaction();
        co_await detail::advisoryLock(transaction, c.pool(), service::message::lock::kVpnAddressAllocation);
        ruvia::db_query conflictsQuery(c.pool());
        conflictsQuery
            .select({ conflictsQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"target_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), conflictsQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()) })
            .from(service::vpn::entities::VpnRouteEntity::table_name())
            .where(conflictsQuery.binary(
                conflictsQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()),
                Op::not_equal,
                conflictsQuery.cast(conflictsQuery.value(id), ruvia::db_data_type::uuid)
            ));
        const auto conflicts = co_await transaction.query(conflictsQuery);
        for (const auto& row : conflicts) {
            const auto otherTarget = cidr::parseCidr(detail::rowValue(row, 0), 1, 30);
            const auto otherVirtual = cidr::parseCidr(detail::rowValue(row, 1), 1, 30);
            if ((otherTarget && virtualNetwork->overlaps(*otherTarget)) ||
                (otherVirtual && virtualNetwork->overlaps(*otherVirtual))) {
                service::common::fail(21002, "虚拟网段与全局已有网段重叠", 409);
            }
        }
        const auto virtualCidr = virtualNetwork->text();
        ruvia::db_query update(c.pool());
        update.update(service::vpn::entities::VpnRouteEntity::table_name())
            .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), update.value(virtualCidr))
            .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"last_error">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), update.value(""))
            .set(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"updated_at">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), update.call("now"))
            .where(update.binary(update.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), Op::equal, update.cast(update.value(id), ruvia::db_data_type::uuid)));
        (void)co_await transaction.execute(update);
        co_await transaction.commit();
        co_await audit(c, c.userId, "vpn.route.update", "vpn_route", id, "success", "{}");
        co_await queueEdgeConfig(c, edgePeerId);
        (void)co_await reconcileHub(c);
    }

    template <typename Context>
    ruvia::task<std::string> peers(Context& c, std::optional<std::string> networkId, std::optional<std::string> edgeNodeId = std::nullopt) {
        using Op = ruvia::db_binary_operator;
        ruvia::db_query query(c.pool());
        query
            .select(query.call(
                "jsonb_build_object",
                { detail::jsonKey(query, "id"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "networkId"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "peerType"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "edgeNodeId"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "userId"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"user_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "name"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"name">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "publicKey"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"public_key">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "assignedIpv4"), query.call("host", { query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"assigned_ipv4">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p") }), detail::jsonKey(query, "allowedRoutes"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"allowed_routes">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "status"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "configRevision"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"config_revision">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), detail::jsonKey(query, "lastHandshakeAt"), query.call("iot_utc_timestamp", { query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"last_handshake_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p") }), detail::jsonKey(query, "createdAt"), query.call("iot_utc_timestamp", { query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"created_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p") }) }
            ))
            .from(service::vpn::entities::VpnPeerEntity::table_name(), "p");
        if (networkId && !networkId->empty()) {
            requireUuid(*networkId, "VPN 网络 ID 无效");
            query.where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), Op::equal, query.cast(query.value(*networkId), ruvia::db_data_type::uuid)));
        }
        if (edgeNodeId && !edgeNodeId->empty()) {
            requireUuid(*edgeNodeId, "Edge 节点 ID 无效");
            query.and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), Op::equal, query.cast(query.value(*edgeNodeId), ruvia::db_data_type::uuid)));
        }
        query.order_by(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"created_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "p"), ruvia::db_order_direction::desc)
            .limit(1000);
        const auto rows = co_await c.db().query(query);
        std::string result{ "[" };
        for (std::size_t index = 0; index < rows.size(); ++index) {
            if (index != 0) {
                result.push_back(',');
            }
            result += detail::rowValue(rows[index], 0);
        }
        result.push_back(']');
        co_return result;
    }

    template <typename Context>
    ruvia::task<std::string> createPeer(Context& c, const VpnPeerBody& payload) {
        const auto defaultNetworkId = co_await ensureDefaultNetwork(c);
        const auto requestedNetworkId = optionalVpnText(payload.get<"networkId">());
        if (requestedNetworkId && !service::common::isUuid(*requestedNetworkId)) {
            service::common::fail(21001, "VPN 网络 ID 无效", 400);
        }
        if (requestedNetworkId && *requestedNetworkId != defaultNetworkId) {
            service::common::fail(21003, "VPN 仅使用默认 iot-server 网络", 409);
        }
        const auto networkId = defaultNetworkId;
        const std::string peerType(payload.get<"peerType">().view());
        if (peerType != "windows" && peerType != "edge") {
            service::common::fail(21001, "Peer 类型只支持 windows 或 edge", 400);
        }
        const std::string name(payload.get<"name">().view());
        auto publicKey = optionalVpnText(payload.get<"publicKey">()).value_or("");
        if (!publicKey.empty() && !detail::validKey(publicKey)) {
            service::common::fail(21001, "WireGuard 公钥格式无效", 400);
        }
        std::string edgeNodeId;
        std::string userId = c.userId;
        std::string reusableEdgePeerId;
        if (peerType == "edge") {
            edgeNodeId = optionalVpnText(payload.get<"edgeNodeId">()).value_or("");
            if (!service::common::isUuid(edgeNodeId)) {
                service::common::fail(21001, "Edge 节点 ID 无效", 400);
            }
        } else if (publicKey.empty()) {
            service::common::fail(21001, "Windows Peer 必须提供公钥", 400);
        }
        using Op = ruvia::db_binary_operator;
        ruvia::db_query networkQuery(c.pool());
        networkQuery
            .select({ networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"overlay_cidr">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()) })
            .from(service::vpn::entities::VpnNetworkEntity::table_name())
            .where(networkQuery.binary(networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), Op::equal, networkQuery.cast(networkQuery.value(networkId), ruvia::db_data_type::uuid)))
            .and_where(networkQuery.unary(ruvia::db_unary_operator::is_null, networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view())));
        const auto networkRows = co_await c.db().query(networkQuery);
        if (networkRows.empty()) {
            service::common::fail(21004, "VPN 网络不存在", 404);
        }
        if (detail::rowValue(networkRows.front(), 1) != "enabled") {
            service::common::fail(21003, "VPN 网络已停用", 409);
        }
        if (peerType == "edge") {
            ruvia::db_query edgeQuery(c.pool());
            const auto vpnCapability = edgeQuery.binary(
                edgeQuery.binary(edgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"capability">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view()), Op::json_get, edgeQuery.cast(edgeQuery.value("vpn"), ruvia::db_data_type::text)),
                Op::json_get_text,
                edgeQuery.cast(edgeQuery.value("publicKey"), ruvia::db_data_type::text)
            );
            edgeQuery
                .select(edgeQuery.coalesce({ vpnCapability, edgeQuery.value("") }))
                .from(service::vpn::entities::EdgeNodeEntity::table_name())
                .where(edgeQuery.binary(edgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"id">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view()), Op::equal, edgeQuery.cast(edgeQuery.value(edgeNodeId), ruvia::db_data_type::uuid)))
                .and_where(edgeQuery.binary(edgeQuery.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"enrollment_status">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view()), Op::equal, edgeQuery.value("approved")));
            const auto edgeRows = co_await c.db().query(edgeQuery);
            if (edgeRows.empty()) {
                service::common::fail(21004, "Edge 节点不存在或尚未批准", 404);
            }
            const auto reportedKey = detail::rowValue(edgeRows.front(), 0);
            publicKey = detail::validKey(reportedKey) ? reportedKey : std::string{};
            ruvia::db_query existingQuery(c.pool());
            const auto revokedFirst = existingQuery.case_when(
                { { existingQuery.binary(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, existingQuery.value("revoked")),
                    existingQuery.cast(existingQuery.value(1), ruvia::db_data_type::integer) } },
                existingQuery.cast(existingQuery.value(0), ruvia::db_data_type::integer)
            );
            existingQuery
                .select({ existingQuery.cast(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), ruvia::db_data_type::text), existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()) })
                .from(service::vpn::entities::VpnPeerEntity::table_name())
                .where(existingQuery.binary(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, existingQuery.cast(existingQuery.value(networkId), ruvia::db_data_type::uuid)))
                .and_where(existingQuery.binary(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, existingQuery.cast(existingQuery.value(edgeNodeId), ruvia::db_data_type::uuid)))
                .order_by(revokedFirst)
                .add_order_by(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"created_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), ruvia::db_order_direction::desc)
                .limit(1);
            const auto existing = co_await c.db().query(existingQuery);
            if (!existing.empty()) {
                if (detail::rowValue(existing.front(), 1) != "revoked") {
                    service::common::fail(21002, "该 Edge 节点已经加入 VPN 网络", 409);
                }
                reusableEdgePeerId = detail::rowValue(existing.front(), 0);
            }
            userId.clear();
        }
        const auto overlay = cidr::parseCidr(detail::rowValue(networkRows.front(), 0), 16, 30);
        if (!overlay) {
            service::common::fail(21005, "VPN 网络 Overlay 配置损坏", 500);
        }
        std::vector<std::string> allowedRoutes;
        if (const auto& routes = payload.get<"allowedRoutes">()) {
            for (const auto& route : *routes) allowedRoutes.emplace_back(route.view());
        }
        if (allowedRoutes.size() > 64) {
            service::common::fail(21001, "Peer 最多授权 64 条路由", 400);
        }
        co_await validateAllowedRoutes(c, networkId, allowedRoutes);
        const auto id = reusableEdgePeerId.empty() ? c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next()
                                                   : reusableEdgePeerId;
        const auto status = publicKey.empty() ? "pending" : "active";
        const auto allowedRoutesJson = detail::jsonArray(allowedRoutes);
        auto transaction = co_await c.db().begin_transaction();
        co_await detail::advisoryLock(transaction, c.pool(), service::message::lock::kVpnAddressAllocation);
        if (reusableEdgePeerId.empty()) {
            const auto assigned = co_await allocateAddressFromDb(transaction, networkId, *overlay);
            const auto assignedText = detail::hostText(*assigned);
            ruvia::db_query insert(c.pool());
            insert.insert_into(service::vpn::entities::VpnPeerEntity::table_name(), { "id", "network_id", "peer_type", "edge_node_id", "user_id", "name", "public_key", "assigned_ipv4", "allowed_routes", "status" })
                .values({ insert.cast(insert.value(id), ruvia::db_data_type::uuid), insert.cast(insert.value(networkId), ruvia::db_data_type::uuid), insert.value(peerType), insert.cast(insert.null_if(insert.cast(insert.value(edgeNodeId), ruvia::db_data_type::text), insert.cast(insert.value(""), ruvia::db_data_type::text)), ruvia::db_data_type::uuid), insert.cast(insert.null_if(insert.cast(insert.value(userId), ruvia::db_data_type::text), insert.cast(insert.value(""), ruvia::db_data_type::text)), ruvia::db_data_type::uuid), insert.value(name), insert.value(publicKey), insert.cast(insert.value(assignedText), ruvia::db_data_type::inet), insert.cast(insert.value(allowedRoutesJson), ruvia::db_data_type::jsonb), insert.value(status) });
            (void)co_await transaction.execute(insert);
        } else {
            ruvia::db_query reactivation(c.pool());
            reactivation
                .update(service::vpn::entities::VpnPeerEntity::table_name())
                .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"name">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), reactivation.value(name))
                .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"public_key">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), reactivation.value(publicKey))
                .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"allowed_routes">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), reactivation.cast(reactivation.value(allowedRoutesJson), ruvia::db_data_type::jsonb))
                .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), reactivation.value(status))
                .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"revoked_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), reactivation.null_value())
                .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"updated_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), reactivation.call("now"))
                .where(reactivation.binary(reactivation.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, reactivation.cast(reactivation.value(id), ruvia::db_data_type::uuid)))
                .and_where(reactivation.binary(reactivation.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, reactivation.value("edge")))
                .and_where(reactivation.binary(reactivation.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, reactivation.value("revoked")))
                .returning({ reactivation.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()) });
            const auto reactivated = co_await transaction.query(reactivation);
            if (reactivated.empty()) {
                service::common::fail(21002, "该 Edge 节点已经加入 VPN 网络", 409);
            }
        }
        if (peerType == "edge") {
            co_await syncEdgeBridgeRoutes(transaction, id, networkId, edgeNodeId, c.userId, *c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>());
        }
        co_await transaction.commit();
        co_await audit(c, c.userId, reusableEdgePeerId.empty() ? "vpn.peer.create" : "vpn.peer.reactivate", "vpn_peer", id, "success", "{}");
        if (peerType == "edge") {
            co_await queueEdgeConfig(c, id);
        }
        (void)co_await reconcileHub(c);
        co_return id;
    }

    template <typename Context>
    ruvia::task<void> revokePeer(Context& c, std::string_view id) {
        requireUuid(id, "VPN Peer ID 无效");
        using Op = ruvia::db_binary_operator;
        ruvia::db_query update(c.pool());
        update.update(service::vpn::entities::VpnPeerEntity::table_name())
            .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), update.value("revoked"))
            .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"revoked_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), update.call("now"))
            .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"updated_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), update.call("now"))
            .where(update.binary(update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, update.cast(update.value(id), ruvia::db_data_type::uuid)))
            .and_where(update.binary(update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::not_equal, update.value("revoked")))
            .returning({ update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"public_key">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()) });
        const auto rows = co_await c.db().query(update);
        if (rows.empty()) {
            service::common::fail(21004, "VPN Peer 不存在或已撤销", 404);
        }
        const auto key = detail::rowValue(rows.front(), 0);
        if (detail::validKey(key)) {
            (void)co_await control(c, "wireguard-remove-peer", key);
        }
        const auto edgeNode = detail::rowValue(rows.front(), 1);
        if (!edgeNode.empty()) {
            co_await queueEdgeConfig(c, id);
        }
        co_await audit(c, c.userId, "vpn.peer.revoke", "vpn_peer", id, "success", "{}");
        (void)co_await reconcileHub(c);
    }

    template <typename Context>
    ruvia::task<void> syncPeer(Context& c, std::string_view id) {
        requireUuid(id, "VPN Peer ID 无效");
        using Op = ruvia::db_binary_operator;
        ruvia::db_query current(c.pool());
        current
            .select({ current.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), current.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), current.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), current.cast(current.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), ruvia::db_data_type::text) })
            .from(service::vpn::entities::VpnPeerEntity::table_name())
            .where(current.binary(current.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, current.cast(current.value(id), ruvia::db_data_type::uuid)));
        const auto rows = co_await c.db().query(current);
        if (rows.empty()) {
            service::common::fail(21004, "VPN Peer 不存在", 404);
        }
        if (detail::rowValue(rows.front(), 1) != "edge") {
            service::common::fail(21001, "只有 Edge Peer 支持重新下发", 400);
        }
        if (detail::rowValue(rows.front(), 2) == "revoked") {
            service::common::fail(21003, "VPN Peer 已撤销", 409);
        }
        const auto edgeNodeId = detail::rowValue(rows.front(), 0);
        const auto networkId = detail::rowValue(rows.front(), 3);
        auto transaction = co_await c.db().begin_transaction();
        co_await detail::advisoryLock(transaction, c.pool(), service::message::lock::kVpnAddressAllocation);
        co_await syncEdgeBridgeRoutes(transaction, id, networkId, edgeNodeId, c.userId, *c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>());
        co_await transaction.commit();
        co_await queueEdgeConfig(c, id);
        (void)co_await reconcileHub(c);
    }

    template <typename Context>
    ruvia::task<std::string> desktopDevices(Context& c);
    template <typename Context>
    ruvia::task<std::string> desktopCreatePeer(Context& c, const VpnDesktopPeerBody& payload);
    template <typename Context>
    ruvia::task<std::string> desktopUpdatePeer(Context& c, std::string_view id, const VpnDesktopSelectionBody& payload);
    template <typename Context>
    ruvia::task<std::string> desktopPeerConfig(Context& c, std::string_view id);
    template <typename Context>
    ruvia::task<void> desktopDeletePeer(Context& c, std::string_view id);

    template <typename Context>
    ruvia::task<std::string> openDesktopSession(Context& c, std::string_view peerId) {
        // Ownership, selections and current authorization must be valid before
        // the connection can create demand. A saved Peer alone creates none.
        (void)co_await desktopPeerConfig(c, peerId);
        const auto connectionId = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        co_await renewDesktopSession(c, peerId, connectionId);
        std::exception_ptr failure;
        try { co_await reconcileHub(c); } catch (...) { failure = std::current_exception(); }
        if (failure) {
            co_await closeDesktopSession(c, peerId, connectionId);
            std::rethrow_exception(failure);
        }
        co_return connectionId;
    }

    template <typename Context>
    ruvia::task<void> renewDesktopSession(Context& c, std::string_view peerId, std::string_view connectionId) {
        const auto key = entities::DesktopSessionSet::key(peerId);
        const auto duration = std::to_string(service::message::vpn::kDesktopSessionLeaseMs);
        const std::string_view keys[]{key};
        const std::string_view arguments[]{connectionId, duration};
        const auto reply = co_await c.redis().eval(R"lua(
local time=redis.call('TIME')
local now=time[1]*1000+math.floor(time[2]/1000)
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
redis.call('ZADD',KEYS[1],now+tonumber(ARGV[2]),ARGV[1])
redis.call('PEXPIRE',KEYS[1],ARGV[2])
return 1
)lua", keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer || reply.integer() != 1)
            service::message::redis::throwValue("renew desktop VPN session", reply);
    }

    template <typename Context>
    ruvia::task<void> closeDesktopSession(Context& c, std::string_view peerId, std::string_view connectionId) {
        const auto key = entities::DesktopSessionSet::key(peerId);
        const std::string_view keys[]{key};
        const std::string_view arguments[]{connectionId};
        // A late close removes only its own connection, including after reconnect.
        const auto reply = co_await c.redis().eval("return redis.call('ZREM',KEYS[1],ARGV[1])", keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer)
            service::message::redis::throwValue("close desktop VPN session", reply);
        co_await reconcileHub(c);
    }

  private:
};

inline std::int64_t VpnService::integer(std::string_view value, std::int64_t fallback) {
    std::int64_t result{};
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    return error == std::errc{} && end == value.data() + value.size() ? result : fallback;
}

inline void VpnService::requireUuid(std::string_view id, std::string_view message) {
    if (!service::common::isUuid(id)) {
        service::common::fail(21001, std::string(message), 400);
    }
}

template <typename Context>
ruvia::task<std::string> VpnService::ensureDefaultNetwork(Context& c) {
    const auto hubPublicKey =
        std::string(c.env().get("VPN_HUB_PUBLIC_KEY").value_or(""));
    const auto hubEndpoint =
        std::string(c.env().get("VPN_HUB_ENDPOINT").value_or(""));
    const auto hubListenPort = c.env().template get<std::uint16_t>("VPN_HUB_LISTEN_PORT").value_or(51820);
    auto transaction = co_await c.db().begin_transaction();
    co_await detail::advisoryLock(transaction, c.pool(), service::message::lock::kVpnDefaultNetwork);
    using Op = ruvia::db_binary_operator;
    ruvia::db_query existingQuery(c.pool());
    existingQuery
        .select(existingQuery.cast(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), ruvia::db_data_type::text))
        .from(service::vpn::entities::VpnNetworkEntity::table_name())
        .where(existingQuery.binary(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"name">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), Op::equal, existingQuery.value(kDefaultNetworkName)))
        .and_where(existingQuery.unary(ruvia::db_unary_operator::is_null, existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view())))
        .limit(1);
    const auto existing = co_await transaction.query(existingQuery);
    std::string networkId = std::string(kDefaultNetworkId);
    if (!existing.empty()) {
        networkId = detail::rowValue(existing.front(), 0);
    } else {
        ruvia::db_query insert(c.pool());
        insert
            .insert_into(service::vpn::entities::VpnNetworkEntity::table_name(), { "id", "name", "overlay_cidr", "hub_public_key", "hub_endpoint", "hub_listen_port", "created_by" })
            .values({ insert.cast(insert.value(kDefaultNetworkId), ruvia::db_data_type::uuid), insert.value(kDefaultNetworkName), insert.value(kDefaultOverlayCidr), insert.value(hubPublicKey), insert.value(hubEndpoint), insert.value(static_cast<int>(hubListenPort)), insert.cast(insert.value(c.userId), ruvia::db_data_type::uuid) })
            .on_conflict({ .columns_ = { "id" }, .update_ = { { "name", insert.excluded(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"name">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()) }, { "overlay_cidr", insert.excluded(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"overlay_cidr">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()) }, { "deleted_at", insert.null_value() }, { "updated_at", insert.call("now") } } })
            .returning({ insert.cast(insert.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), ruvia::db_data_type::text) });
        const auto inserted = co_await transaction.query(insert);
        if (inserted.empty()) {
            service::common::fail(21005, "默认 iot-server VPN 网络创建失败", 500);
        }
        networkId = detail::rowValue(inserted.front(), 0);
    }
    ruvia::db_query update(c.pool());
    update
        .update(service::vpn::entities::VpnNetworkEntity::table_name())
        .set(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"name">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), update.value(kDefaultNetworkName))
        .set(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"overlay_cidr">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), update.value(kDefaultOverlayCidr))
        .set(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), update.value("enabled"))
        .set(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), update.null_value())
        .set(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"updated_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), update.call("now"))
        .where(update.binary(update.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), Op::equal, update.cast(update.value(networkId), ruvia::db_data_type::uuid)));
    (void)co_await transaction.execute(update);
    co_await transaction.commit();
    co_return networkId;
}

template <typename Db>
ruvia::task<std::optional<std::uint32_t>> VpnService::allocateAddressFromDb(
    Db& db,
    std::string_view networkId,
    const cidr::Ipv4Cidr& overlay
) {
    using Op = ruvia::db_binary_operator;
    ruvia::db_query query;
    const auto reusable = query.binary(
        query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::not_equal, query.value("windows")),
        Op::or_value,
        query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::not_equal, query.value("revoked"))
    );
    query
        .select(query.call("host", { query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"assigned_ipv4">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()) }))
        .from(service::vpn::entities::VpnPeerEntity::table_name())
        .where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, query.cast(query.value(networkId), ruvia::db_data_type::uuid)))
        .and_where(reusable);
    const auto rows = co_await db.query(query);
    std::unordered_set<std::uint32_t> used;
    for (const auto& row : rows) {
        if (const auto value = cidr::parseIpv4(detail::rowValue(row, 0))) {
            used.emplace(*value);
        }
    }
    for (std::uint32_t offset = 2; offset + 1 < overlay.size(); ++offset) {
        const auto candidate = *cidr::hostAddress(overlay, offset);
        if (!used.contains(candidate)) {
            co_return candidate;
        }
    }
    service::common::fail(21007, "VPN Overlay 地址池已耗尽", 409);
}

template <typename Context>
ruvia::task<void> VpnService::validateAllowedRoutes(
    Context& c,
    std::string_view networkId,
    const std::vector<std::string>& routes
) {
    if (routes.empty()) {
        co_return;
    }
    for (const auto& route : routes) {
        if (!cidr::parseCidr(route, 1, 30)) {
            service::common::fail(21001, "Peer 授权路由 CIDR 无效", 400);
        }
    }
    using Op = ruvia::db_binary_operator;
    ruvia::db_query query(c.pool());
    query
        .select(query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()))
        .from(service::vpn::entities::VpnRouteEntity::table_name())
        .where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"network_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), Op::equal, query.cast(query.value(networkId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"enabled">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view()), Op::equal, query.value(true)));
    const auto rows = co_await c.db().query(query);
    std::unordered_set<std::string> allowed;
    for (const auto& row : rows) {
        allowed.emplace(detail::rowValue(row, 0));
    }
    for (const auto& route : routes) {
        if (!allowed.contains(route)) {
            service::common::fail(21003, "Peer 授权路由不属于该 VPN 网络", 403);
        }
    }
}

template <typename Context>
ruvia::task<std::string> VpnService::desktopDevices(Context& c) {
    using Op = ruvia::db_binary_operator;
    ruvia::db_query routeQuery(c.pool());
    const std::array routeOrder{
        ruvia::db_order_term{ routeQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "route") }
    };
    routeQuery
        .select(routeQuery.aggregate("jsonb_agg", { routeQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "route") }, false, routeOrder))
        .from(service::vpn::entities::VpnRouteEntity::table_name(), "route")
        .where(routeQuery.binary(routeQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"edge_peer_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "route"), Op::equal, routeQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer")))
        .and_where(routeQuery.binary(routeQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"network_id">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "route"), Op::equal, routeQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network")))
        .and_where(routeQuery.binary(routeQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"enabled">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "route"), Op::equal, routeQuery.value(true)))
        .and_where(routeQuery.binary(routeQuery.column(std::tuple_element_t<service::vpn::entities::VpnRouteEntity::column_index<"status">(), service::vpn::entities::VpnRouteEntity::columns_type>::name.view(), "route"), Op::equal, routeQuery.value("active")));
    ruvia::db_query query(c.pool());
    const auto emptyRoutes = query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
    query
        .select({ query.call("jsonb_build_object", { detail::jsonKey(query, "id"), query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"id">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"), detail::jsonKey(query, "name"), query.coalesce({ query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"name">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"), query.value("") }), detail::jsonKey(query, "imei"), query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"imei">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"), detail::jsonKey(query, "virtualCidrs"), query.coalesce({ query.subquery(routeQuery), emptyRoutes }), detail::jsonKey(query, "assignedIpv4"), query.call("host", { query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"assigned_ipv4">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer") }) }), query.cast(query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"id">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"), ruvia::db_data_type::text) })
        .from(service::vpn::entities::EdgeNodeEntity::table_name(), "edge")
        .join(ruvia::db_join_type::inner, service::vpn::entities::VpnPeerEntity::table_name(), query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"id">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge")), "peer")
        .join(ruvia::db_join_type::inner, service::vpn::entities::VpnNetworkEntity::table_name(), query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), Op::equal, query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer")), "network")
        .where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.value("edge")))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.value("active")))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"enrollment_status">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"), Op::equal, query.value("approved")))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"name">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), Op::equal, query.value(kDefaultNetworkName)))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), Op::equal, query.value("enabled")))
        .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network")))
        .order_by(query.coalesce({ query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"name">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"), query.value("") }))
        .add_order_by(query.column(std::tuple_element_t<service::vpn::entities::EdgeNodeEntity::column_index<"id">(), service::vpn::entities::EdgeNodeEntity::columns_type>::name.view(), "edge"));
    const auto rows = co_await c.db().query(query);
    std::string result{ "[" };
    for (std::size_t index = 0; index < rows.size(); ++index) {
        if (index) {
            result += ',';
        }
        // Use the same live session source as EdgeService::fillNode. Database
        // telemetry timestamps do not represent the lifetime of a WebSocket.
        const auto session = co_await c.redis().get(service::message::edge::sessionKey(detail::rowValue(rows[index], 1)));
        const auto device = detail::rowValue(rows[index], 0);
        result += std::string("{\"online\":") + (session.has_value() ? "true," : "false,") + device.substr(1);
    }
    result += ']';
    co_return result;
}

template <typename Context>
ruvia::task<std::string> VpnService::desktopCreatePeer(Context& c, const VpnDesktopPeerBody& payload) {
    const std::string name(payload.get<"name">().view());
    const std::string publicKey(payload.get<"publicKey">().view());
    if (!detail::validManagedKey(publicKey)) {
        service::common::fail(21001, "WireGuard 公钥格式无效", 400);
    }
    const auto ids = normalizeVpnNodeIds(payload.get<"edgeNodeIds">());
    auto tx = co_await c.db().begin_transaction();
    // Serialize allocation and the public-key retry lookup together. A lost POST
    // response can safely be retried with the locally persisted keypair.
    co_await detail::advisoryLock(tx, c.pool(), service::message::lock::kVpnAddressAllocation);
    using Op = ruvia::db_binary_operator;
    ruvia::db_query existingQuery(c.pool());
    existingQuery
        .select({ existingQuery.cast(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), ruvia::db_data_type::text), existingQuery.cast(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"user_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), ruvia::db_data_type::text), existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"client_managed">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()) })
        .from(service::vpn::entities::VpnPeerEntity::table_name())
        .where(existingQuery.binary(existingQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"public_key">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, existingQuery.value(publicKey)))
        .lock({ .mode_ = ruvia::db_row_lock::update });
    const auto existing = co_await tx.query(existingQuery);
    if (!existing.empty()) {
        const auto& row = existing.front();
        if (detail::rowValue(row, 1) != c.userId || detail::rowValue(row, 3) != "t") {
            service::common::fail(21002, "该 WireGuard 公钥已经被使用", 409);
        }
        if (detail::rowValue(row, 2) == "revoked") {
            service::common::fail(21009, "该 Windows VPN 注册已撤销", 410);
        }
        if (detail::rowValue(row, 2) != "active") {
            service::common::fail(21002, "该 WireGuard 公钥已经被使用", 409);
        }
        const auto id = detail::rowValue(row, 0);
        co_await tx.commit();
        co_await reconcileHub(c);
        // POST retries recover the original registration even if an Edge has
        // since been removed. Selection changes use the dedicated PATCH route.
        co_return co_await desktopPeerConfig(c, id);
    }
    ruvia::db_query networkQuery(c.pool());
    networkQuery
        .select({ networkQuery.cast(networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), ruvia::db_data_type::text), networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"overlay_cidr">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()) })
        .from(service::vpn::entities::VpnNetworkEntity::table_name())
        .where(networkQuery.binary(networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"name">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), Op::equal, networkQuery.value(kDefaultNetworkName)))
        .and_where(networkQuery.binary(networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view()), Op::equal, networkQuery.value("enabled")))
        .and_where(networkQuery.unary(ruvia::db_unary_operator::is_null, networkQuery.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view())))
        .lock({ .mode_ = ruvia::db_row_lock::share });
    const auto network = co_await tx.query(networkQuery);
    if (network.empty()) {
        service::common::fail(21004, "VPN 网络不存在或已停用", 404);
    }
    const auto networkId = detail::rowValue(network.front(), 0);
    const auto overlay = cidr::parseCidr(detail::rowValue(network.front(), 1), 16, 30);
    if (!overlay) {
        service::common::fail(21005, "VPN 网络 Overlay 配置损坏", 500);
    }
    co_await detail::validateSelectedEdges(tx, networkId, ids);
    const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
    const auto assigned = co_await allocateAddressFromDb(tx, networkId, *overlay);
    if (!assigned) {
        service::common::fail(21002, "VPN 网络地址已用尽", 409);
    }
    const auto assignedText = detail::hostText(*assigned);
    ruvia::db_query insert(c.pool());
    insert
        .insert_into(service::vpn::entities::VpnPeerEntity::table_name(), { "id", "network_id", "peer_type", "user_id", "name", "public_key", "assigned_ipv4", "allowed_routes", "status", "client_managed" })
        .values({ insert.cast(insert.value(id), ruvia::db_data_type::uuid), insert.cast(insert.value(networkId), ruvia::db_data_type::uuid), insert.value("windows"), insert.cast(insert.value(c.userId), ruvia::db_data_type::uuid), insert.value(name), insert.value(publicKey), insert.cast(insert.value(assignedText), ruvia::db_data_type::inet), insert.cast(insert.value("[]"), ruvia::db_data_type::jsonb), insert.value("active"), insert.value(true) });
    (void)co_await tx.execute(insert);
    for (const auto& edge : ids) {
        ruvia::db_query selection(c.pool());
        selection
            .insert_into(service::vpn::entities::VpnPeerEdgeSelectionEntity::table_name(), { "peer_id", "edge_node_id" })
            .values({ selection.cast(selection.value(id), ruvia::db_data_type::uuid), selection.cast(selection.value(edge), ruvia::db_data_type::uuid) });
        (void)co_await tx.execute(selection);
    }
    co_await tx.commit();
    co_await audit(c, c.userId, "vpn.desktop.enroll", "vpn_peer", id, "success", "{}");
    (void)co_await reconcileHub(c);
    co_return co_await desktopPeerConfig(c, id);
}

template <typename Context>
ruvia::task<std::string> VpnService::desktopUpdatePeer(Context& c, std::string_view id, const VpnDesktopSelectionBody& payload) {
    requireUuid(id, "VPN Peer ID 无效");
    const auto ids = normalizeVpnNodeIds(payload.get<"edgeNodeIds">());
    auto tx = co_await c.db().begin_transaction();
    // Lock the parent before replacing children so concurrent PATCH requests
    // cannot merge two selections or race revocation.
    using Op = ruvia::db_binary_operator;
    ruvia::db_query parent(c.pool());
    parent
        .select(parent.cast(parent.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), ruvia::db_data_type::text))
        .from(service::vpn::entities::VpnPeerEntity::table_name())
        .where(parent.binary(parent.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, parent.cast(parent.value(id), ruvia::db_data_type::uuid)))
        .and_where(parent.binary(parent.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"user_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, parent.cast(parent.value(c.userId), ruvia::db_data_type::uuid)))
        .and_where(parent.binary(parent.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, parent.value("windows")))
        .and_where(parent.binary(parent.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"client_managed">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, parent.value(true)))
        .and_where(parent.binary(parent.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, parent.value("active")))
        .lock({ .mode_ = ruvia::db_row_lock::update });
    const auto rows = co_await tx.query(parent);
    if (rows.empty()) {
        service::common::fail(21004, "Windows VPN 配置不存在或不属于当前用户", 404);
    }
    co_await detail::validateSelectedEdges(tx, detail::rowValue(rows.front(), 0), ids);
    ruvia::db_query removal(c.pool());
    removal
        .delete_from(service::vpn::entities::VpnPeerEdgeSelectionEntity::table_name())
        .where(removal.binary(removal.column(std::tuple_element_t<service::vpn::entities::VpnPeerEdgeSelectionEntity::column_index<"peer_id">(), service::vpn::entities::VpnPeerEdgeSelectionEntity::columns_type>::name.view()), Op::equal, removal.cast(removal.value(id), ruvia::db_data_type::uuid)));
    (void)co_await tx.execute(removal);
    for (const auto& edge : ids) {
        ruvia::db_query selection(c.pool());
        selection
            .insert_into(service::vpn::entities::VpnPeerEdgeSelectionEntity::table_name(), { "peer_id", "edge_node_id" })
            .values({ selection.cast(selection.value(id), ruvia::db_data_type::uuid), selection.cast(selection.value(edge), ruvia::db_data_type::uuid) });
        (void)co_await tx.execute(selection);
    }
    co_await tx.commit();
    co_await audit(c, c.userId, "vpn.desktop.select", "vpn_peer", id, "success", "{}");
    (void)co_await reconcileHub(c);
    co_return co_await desktopPeerConfig(c, id);
}

template <typename Context>
ruvia::task<std::string> VpnService::desktopPeerConfig(Context& c, std::string_view id) {
    using Op = ruvia::db_binary_operator;
    requireUuid(id, "VPN Peer ID 无效");
    ruvia::db_query selectionQuery(c.pool());
    const std::array selectionOrder{
        ruvia::db_order_term{ selectionQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEdgeSelectionEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEdgeSelectionEntity::columns_type>::name.view(), "selection") }
    };
    selectionQuery
        .select(selectionQuery.aggregate("jsonb_agg", { selectionQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEdgeSelectionEntity::column_index<"edge_node_id">(), service::vpn::entities::VpnPeerEdgeSelectionEntity::columns_type>::name.view(), "selection") }, false, selectionOrder))
        .from(service::vpn::entities::VpnPeerEdgeSelectionEntity::table_name(), "selection")
        .where(selectionQuery.binary(selectionQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEdgeSelectionEntity::column_index<"peer_id">(), service::vpn::entities::VpnPeerEdgeSelectionEntity::columns_type>::name.view(), "selection"), Op::equal, selectionQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer")));

    ruvia::db_query routeAccess(c.pool());
    routeAccess
        .select(routeAccess.alias(routeAccess.column(std::tuple_element_t<service::vpn::entities::VpnEffectiveRouteAccessEntity::column_index<"virtual_cidr">(), service::vpn::entities::VpnEffectiveRouteAccessEntity::columns_type>::name.view(), "access"), "route"))
        .from(service::vpn::entities::VpnEffectiveRouteAccessEntity::table_name(), "access")
        .where(routeAccess.binary(routeAccess.column(std::tuple_element_t<service::vpn::entities::VpnEffectiveRouteAccessEntity::column_index<"peer_id">(), service::vpn::entities::VpnEffectiveRouteAccessEntity::columns_type>::name.view(), "access"), Op::equal, routeAccess.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer")));
    ruvia::db_query edgeAccess(c.pool());
    edgeAccess
        .select(edgeAccess.binary(edgeAccess.column(std::tuple_element_t<service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_index<"edge_address">(), service::vpn::entities::VpnEffectiveEdgeAccessEntity::columns_type>::name.view(), "access"), Op::concat, edgeAccess.cast(edgeAccess.value("/32"), ruvia::db_data_type::text)))
        .from(service::vpn::entities::VpnEffectiveEdgeAccessEntity::table_name(), "access")
        .where(edgeAccess.binary(edgeAccess.column(std::tuple_element_t<service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_index<"peer_id">(), service::vpn::entities::VpnEffectiveEdgeAccessEntity::columns_type>::name.view(), "access"), Op::equal, edgeAccess.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer")));
    routeAccess.combine(ruvia::db_set_operation::union_value, edgeAccess);

    ruvia::db_query allowedQuery(c.pool());
    const std::array allowedOrder{
        ruvia::db_order_term{ allowedQuery.column("route", "allowed") }
    };
    allowedQuery
        .select(allowedQuery.aggregate("jsonb_agg", { allowedQuery.column("route", "allowed") }, false, allowedOrder))
        .from(routeAccess, "allowed");

    ruvia::db_query edgeAddressQuery(c.pool());
    const std::array edgeAddressOrder{
        ruvia::db_order_term{ edgeAddressQuery.column(std::tuple_element_t<service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_index<"edge_address">(), service::vpn::entities::VpnEffectiveEdgeAccessEntity::columns_type>::name.view(), "access") }
    };
    edgeAddressQuery
        .select(edgeAddressQuery.aggregate(
            "jsonb_agg",
            { edgeAddressQuery.column(std::tuple_element_t<service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_index<"edge_address">(), service::vpn::entities::VpnEffectiveEdgeAccessEntity::columns_type>::name.view(), "access") },
            false,
            edgeAddressOrder
        ))
        .from(service::vpn::entities::VpnEffectiveEdgeAccessEntity::table_name(), "access")
        .where(edgeAddressQuery.binary(edgeAddressQuery.column(std::tuple_element_t<service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_index<"peer_id">(), service::vpn::entities::VpnEffectiveEdgeAccessEntity::columns_type>::name.view(), "access"), Op::equal, edgeAddressQuery.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer")));

    ruvia::db_query query(c.pool());
    const auto emptyRoutes = query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
    const auto networkEnabled = query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"status">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), Op::equal, query.value("enabled"));
    const auto allowedRoutes = query.case_when(
        { { networkEnabled,
            query.coalesce({ query.subquery(allowedQuery), emptyRoutes }) } },
        emptyRoutes
    );
    const auto edgeAddresses = query.case_when(
        { { networkEnabled,
            query.coalesce({ query.subquery(edgeAddressQuery), emptyRoutes }) } },
        emptyRoutes
    );
    query
        .select({ query.call("jsonb_build_object", { detail::jsonKey(query, "peerId"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), detail::jsonKey(query, "name"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"name">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), detail::jsonKey(query, "publicKey"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"public_key">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), detail::jsonKey(query, "networkEnabled"), networkEnabled, detail::jsonKey(query, "assignedIpv4"), query.call("host", { query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"assigned_ipv4">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer") }), detail::jsonKey(query, "hubPublicKey"), query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"hub_public_key">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), detail::jsonKey(query, "hubEndpoint"), query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"hub_endpoint">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), detail::jsonKey(query, "hubListenPort"), query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"hub_listen_port">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), detail::jsonKey(query, "mtu"), query.cast(query.value(1280), ruvia::db_data_type::integer), detail::jsonKey(query, "persistentKeepalive"), query.cast(query.value(120), ruvia::db_data_type::integer), detail::jsonKey(query, "configRevision"), query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"config_revision">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), detail::jsonKey(query, "edgeNodeIds"), query.coalesce({ query.subquery(selectionQuery), emptyRoutes }), detail::jsonKey(query, "allowedRoutes"), allowedRoutes, detail::jsonKey(query, "edgeAddresses"), edgeAddresses }), query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"hub_public_key">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"hub_endpoint">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network") })
        .from(service::vpn::entities::VpnPeerEntity::table_name(), "peer")
        .join(ruvia::db_join_type::inner, service::vpn::entities::VpnNetworkEntity::table_name(), query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"id">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network"), Op::equal, query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"network_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer")), "network")
        .where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.cast(query.value(id), ruvia::db_data_type::uuid)))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"user_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.cast(query.value(c.userId), ruvia::db_data_type::uuid)))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.value("windows")))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"client_managed">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.value(true)))
        .and_where(query.binary(query.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), "peer"), Op::equal, query.value("active")))
        .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(std::tuple_element_t<service::vpn::entities::VpnNetworkEntity::column_index<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::columns_type>::name.view(), "network")));
    const auto rows = co_await c.db().query(query);
    if (rows.empty()) {
        service::common::fail(21004, "Windows VPN 配置不存在或不属于当前用户", 404);
    }
    if (!detail::validManagedKey(detail::rowValue(rows.front(), 1)) || detail::rowValue(rows.front(), 2).empty()) {
        service::common::fail(21005, "VPN Hub 尚未配置完成", 503);
    }
    co_return detail::rowValue(rows.front(), 0);
}

template <typename Context>
ruvia::task<void> VpnService::desktopDeletePeer(Context& c, std::string_view id) {
    using Op = ruvia::db_binary_operator;
    requireUuid(id, "VPN Peer ID 无效");
    ruvia::db_query update(c.pool());
    update
        .update(service::vpn::entities::VpnPeerEntity::table_name())
        .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"status">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), update.value("revoked"))
        .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"revoked_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), update.call("now"))
        .set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"updated_at">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), update.call("now"))
        .where(update.binary(update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, update.cast(update.value(id), ruvia::db_data_type::uuid)))
        .and_where(update.binary(update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"user_id">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, update.cast(update.value(c.userId), ruvia::db_data_type::uuid)))
        .and_where(update.binary(update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"peer_type">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, update.value("windows")))
        .and_where(update.binary(update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"client_managed">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()), Op::equal, update.value(true)))
        .returning({ update.column(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<"public_key">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view()) });
    const auto rows = co_await c.db().query(update);
    if (rows.empty()) {
        service::common::fail(21004, "Windows VPN 配置不存在或不属于当前用户", 404);
    }
    co_await audit(c, c.userId, "vpn.desktop.revoke", "vpn_peer", id, "success", "{}");
    (void)co_await reconcileHub(c);
}

template <typename Context>
ruvia::task<void> VpnService::queueEdgeConfig(Context& c, std::string_view peerId) {
    (void)co_await control(c, "queue-edge-config", service::message::rpc::encodeLinePair(peerId, c.userId));
}

template <typename Context>
ruvia::task<void> VpnService::audit(Context& c, std::string_view actor, std::string_view action, std::string_view resource, std::string_view resourceId, std::string_view outcome, std::string_view details) {
    const auto auditId = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
    ruvia::db_query insert(c.pool());
    insert
        .insert_into(service::vpn::entities::SecurityAuditLogEntity::table_name(), { "id", "actor_user_id", "action", "resource_type", "resource_id", "outcome", "details" })
        .values({ insert.cast(insert.value(auditId), ruvia::db_data_type::uuid), insert.cast(insert.value(actor), ruvia::db_data_type::uuid), insert.value(action), insert.value(resource), insert.cast(insert.null_if(insert.cast(insert.value(resourceId), ruvia::db_data_type::text), insert.cast(insert.value(""), ruvia::db_data_type::text)), ruvia::db_data_type::uuid), insert.value(outcome), insert.cast(insert.value(details), ruvia::db_data_type::jsonb) });
    (void)co_await c.db().execute(insert);
}

template <typename Context>
ruvia::task<std::string> VpnService::control(Context& c, std::string_view operation, std::string payload) {
    co_return co_await service::rpc::call(c, "vpn", operation, std::move(payload));
}

template <typename Context>
ruvia::task<void> VpnService::reconcileHub(Context& c) {
    (void)co_await control(c, "reconcile", std::string{});
}

inline VpnService& vpnService() {
    return VpnService::instance();
}

} // namespace service::vpn
