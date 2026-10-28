#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <ruvia/web/context.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/model.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/wireguard.h"
#include "service/modules/device/device.entity.h"
#include "service/modules/edge_node/edge_node.entity.h"
#include "service/modules/vpn/vpn.entity.h"
#include "service/modules/vpn/vpn.types.h"
#include "service/utils/json.h"
#include "service/utils/rpc_client.h"
#include "service/utils/uuid.h"

namespace service::vpn {

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

template <typename Db>
ruvia::task<void> validateSelectedEdges(Db& db, std::string_view networkId, const std::vector<std::string>& ids) {
    if (ids.empty()) {
        co_return;
    }
    using Op = ruvia::db_binary_operator;
    ruvia::db_query query;
    std::vector<ruvia::db_expression> selectedIds;
    selectedIds.reserve(ids.size());
    for (const auto& id : ids) {
        selectedIds.push_back(query.cast(query.value(id), ruvia::db_data_type::uuid));
    }
    auto queryBuilder = db.template repository<service::edge_node::entities::EdgeNodeEntity>().query_builder("edge");
    queryBuilder.select({ { "id", query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "edge") } }).distinct().template join<service::vpn::entities::VpnPeerEntity>(ruvia::db_join_type::inner, "peer", query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">(), "peer"), Op::equal, query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "edge"))).template join<service::vpn::entities::VpnNetworkEntity>(ruvia::db_join_type::inner, "network", query.binary(query.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "network"), Op::equal, query.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), "peer"))).where(query.binary(query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "edge"), Op::in, query.list(selectedIds))).and_where(query.binary(query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"enrollment_status">(), "edge"), Op::equal, query.value("approved"))).and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">(), "peer"), Op::equal, query.value("edge"))).and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"status">(), "peer"), Op::equal, query.value("active"))).and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), "peer"), Op::equal, query.cast(query.value(networkId), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::vpn::entities::VpnNetworkEntity::column_name<"status">(), "network"), Op::equal, query.value("enabled"))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">(), "network")));
    const auto rows = co_await queryBuilder.get_raw_many();
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

} // namespace detail

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
    template <typename Context>
    static ruvia::task<void> syncEdgeRoutes(Context&, std::string_view, std::string_view, std::string_view);
    template <typename Context>
    static ruvia::task<void> discardEdgePeer(Context&, std::string_view);
    template <typename Db>
    static ruvia::task<std::optional<std::uint32_t>> allocateAddressFromDb(
        Db&,
        std::string_view,
        const service::utils::network::Ipv4Cidr&
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
        auto countBuilder = c.db().template repository<service::vpn::entities::VpnNetworkEntity>().query_builder("n");
        countBuilder.select({ { "value0", count.aggregate("count", { count.star() }) } }).where(count.binary(count.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "n"), Op::equal, count.cast(count.value(defaultNetworkId), ruvia::db_data_type::uuid))).and_where(count.unary(ruvia::db_unary_operator::is_null, count.column(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">(), "n")));
        if (keyword && !keyword->empty()) {
            const auto pattern = "%" + *keyword + "%";
            countBuilder.and_where(count.binary(count.column(service::vpn::entities::VpnNetworkEntity::column_name<"name">(), "n"), Op::i_like, count.value(pattern)));
        }
        if (status && !status->empty()) {
            countBuilder.and_where(count.binary(count.column(service::vpn::entities::VpnNetworkEntity::column_name<"status">(), "n"), Op::equal, count.value(*status)));
        }
        const auto countRows = co_await countBuilder.get_raw_many();
        const auto total = integer(detail::rowValue(countRows.front(), 0));
        ruvia::db_query peerCount(c.pool());
        peerCount.select(peerCount.aggregate("count", { peerCount.star() }))
            .from(service::vpn::entities::VpnPeerEntity::table_name(), "p")
            .where(peerCount.binary(peerCount.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), "p"), Op::equal, peerCount.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "n")))
            .and_where(peerCount.binary(peerCount.column(service::vpn::entities::VpnPeerEntity::column_name<"status">(), "p"), Op::not_equal, peerCount.value("revoked")));
        ruvia::db_query routeCount(c.pool());
        routeCount.select(routeCount.aggregate("count", { routeCount.star() }))
            .from(service::vpn::entities::VpnRouteEntity::table_name(), "r")
            .where(routeCount.binary(routeCount.column(service::vpn::entities::VpnRouteEntity::column_name<"network_id">(), "r"), Op::equal, routeCount.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "n")))
            .and_where(routeCount.binary(routeCount.column(service::vpn::entities::VpnRouteEntity::column_name<"enabled">(), "r"), Op::equal, routeCount.value(true)));
        ruvia::db_query listQuery(c.pool());
        auto listQueryBuilder = c.db().template repository<service::vpn::entities::VpnNetworkEntity>().query_builder("n");
        listQueryBuilder.select({ { "id", listQuery.call("jsonb_build_object", { detail::jsonKey(listQuery, "id"), listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "n"), detail::jsonKey(listQuery, "name"), listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"name">(), "n"), detail::jsonKey(listQuery, "overlayCidr"), listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"overlay_cidr">(), "n"), detail::jsonKey(listQuery, "hubPublicKey"), listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"hub_public_key">(), "n"), detail::jsonKey(listQuery, "hubEndpoint"), listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"hub_endpoint">(), "n"), detail::jsonKey(listQuery, "hubListenPort"), listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"hub_listen_port">(), "n"), detail::jsonKey(listQuery, "status"), listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"status">(), "n"), detail::jsonKey(listQuery, "peerCount"), listQuery.subquery(peerCount), detail::jsonKey(listQuery, "routeCount"), listQuery.subquery(routeCount), detail::jsonKey(listQuery, "createdAt"), listQuery.call("iot_utc_timestamp", { listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"created_at">(), "n") }), detail::jsonKey(listQuery, "updatedAt"), listQuery.call("iot_utc_timestamp", { listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"updated_at">(), "n") }) }) } }).where(listQuery.binary(listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "n"), Op::equal, listQuery.cast(listQuery.value(defaultNetworkId), ruvia::db_data_type::uuid))).and_where(listQuery.unary(ruvia::db_unary_operator::is_null, listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">(), "n")));
        if (keyword && !keyword->empty()) {
            const auto pattern = "%" + *keyword + "%";
            listQueryBuilder.and_where(listQuery.binary(listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"name">(), "n"), Op::i_like, listQuery.value(pattern)));
        }
        if (status && !status->empty()) {
            listQueryBuilder.and_where(listQuery.binary(listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"status">(), "n"), Op::equal, listQuery.value(*status)));
        }
        listQueryBuilder.order_by(listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"created_at">(), "n"), ruvia::db_order_direction::desc).add_order_by(listQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "n"), ruvia::db_order_direction::desc).limit(static_cast<std::uint64_t>(pageSize)).offset(static_cast<std::uint64_t>((page - 1) * pageSize));
        const auto rows = co_await listQueryBuilder.get_raw_many();
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
        auto queryBuilder = c.db().template repository<service::vpn::entities::VpnRouteEntity>().query_builder("r");
        queryBuilder.select({ { "id", query.call("jsonb_build_object", { detail::jsonKey(query, "id"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"id">(), "r"), detail::jsonKey(query, "networkId"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"network_id">(), "r"), detail::jsonKey(query, "edgePeerId"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"edge_peer_id">(), "r"), detail::jsonKey(query, "edgeNodeId"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">(), "p"), detail::jsonKey(query, "lanInterface"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"lan_interface">(), "r"), detail::jsonKey(query, "targetCidr"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"target_cidr">(), "r"), detail::jsonKey(query, "virtualCidr"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"virtual_cidr">(), "r"), detail::jsonKey(query, "mode"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"mode">(), "r"), detail::jsonKey(query, "natMode"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"nat_mode">(), "r"), detail::jsonKey(query, "status"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"status">(), "r"), detail::jsonKey(query, "enabled"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"enabled">(), "r"), detail::jsonKey(query, "lastError"), query.column(service::vpn::entities::VpnRouteEntity::column_name<"last_error">(), "r"), detail::jsonKey(query, "createdAt"), query.call("iot_utc_timestamp", { query.column(service::vpn::entities::VpnRouteEntity::column_name<"created_at">(), "r") }), detail::jsonKey(query, "updatedAt"), query.call("iot_utc_timestamp", { query.column(service::vpn::entities::VpnRouteEntity::column_name<"updated_at">(), "r") }) }) } }).template join<service::vpn::entities::VpnPeerEntity>(ruvia::db_join_type::inner, "p", query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "p"), Op::equal, query.column(service::vpn::entities::VpnRouteEntity::column_name<"edge_peer_id">(), "r")));
        if (networkId && !networkId->empty()) {
            requireUuid(*networkId, "VPN 网络 ID 无效");
            queryBuilder.where(query.binary(query.column(service::vpn::entities::VpnRouteEntity::column_name<"network_id">(), "r"), Op::equal, query.cast(query.value(*networkId), ruvia::db_data_type::uuid)));
        }
        if (edgeNodeId && !edgeNodeId->empty()) {
            requireUuid(*edgeNodeId, "Edge 节点 ID 无效");
            queryBuilder.and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">(), "p"), Op::equal, query.cast(query.value(*edgeNodeId), ruvia::db_data_type::uuid)));
        }
        queryBuilder.order_by(query.column(service::vpn::entities::VpnRouteEntity::column_name<"created_at">(), "r"), ruvia::db_order_direction::desc).limit(1000);
        const auto rows = co_await queryBuilder.get_raw_many();
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
        auto currentBuilder = c.db().template repository<service::vpn::entities::VpnRouteEntity>().query_builder();
        currentBuilder.select({ { "edge_peer_id", current.column(service::vpn::entities::VpnRouteEntity::column_name<"edge_peer_id">()) }, { "network_id", current.column(service::vpn::entities::VpnRouteEntity::column_name<"network_id">()) }, { "lan_interface", current.column(service::vpn::entities::VpnRouteEntity::column_name<"lan_interface">()) }, { "target_cidr", current.column(service::vpn::entities::VpnRouteEntity::column_name<"target_cidr">()) }, { "virtual_cidr", current.column(service::vpn::entities::VpnRouteEntity::column_name<"virtual_cidr">()) }, { "mode", current.column(service::vpn::entities::VpnRouteEntity::column_name<"mode">()) }, { "enabled", current.column(service::vpn::entities::VpnRouteEntity::column_name<"enabled">()) } }).where(current.binary(current.column(service::vpn::entities::VpnRouteEntity::column_name<"id">()), Op::equal, current.cast(current.value(id), ruvia::db_data_type::uuid)));
        const auto rows = co_await currentBuilder.get_raw_many();
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
        const auto target = service::utils::network::parseCidr(targetText, 1, 30);
        const auto virtualNetwork = service::utils::network::parseCidr(virtualText, 1, 30);
        if (!target || !virtualNetwork || target->prefix != virtualNetwork->prefix ||
            !service::utils::network::isPrivateIpv4(*target) || !service::message::vpn::kVirtualLanPool.contains(virtualNetwork->network) ||
            !service::message::vpn::kVirtualLanPool.contains(virtualNetwork->network + virtualNetwork->size() - 1U) ||
            virtualNetwork->overlaps(*target)) {
            service::common::fail(21001, "VPN 路由 CIDR 无效或不是等长私网映射", 400);
        }
        if (currentMode != "nat" || !currentEnabled) {
            service::common::fail(21003, "VPN 自动映射只能保持启用的 NAT 模式", 409);
        }
        auto transaction = co_await c.db("vpn-address-allocation").begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kVpnAddressAllocation);
        ruvia::db_query conflictsQuery(c.pool());
        auto conflictsQueryBuilder = transaction.template repository<service::vpn::entities::VpnRouteEntity>().query_builder();
        conflictsQueryBuilder.select({ { "target_cidr", conflictsQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"target_cidr">()) }, { "virtual_cidr", conflictsQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"virtual_cidr">()) } }).where(conflictsQuery.binary(conflictsQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"id">()), Op::not_equal, conflictsQuery.cast(conflictsQuery.value(id), ruvia::db_data_type::uuid)));
        const auto conflicts = co_await conflictsQueryBuilder.get_raw_many();
        for (const auto& row : conflicts) {
            const auto otherTarget = service::utils::network::parseCidr(detail::rowValue(row, 0), 1, 30);
            const auto otherVirtual = service::utils::network::parseCidr(detail::rowValue(row, 1), 1, 30);
            if ((otherTarget && virtualNetwork->overlaps(*otherTarget)) ||
                (otherVirtual && virtualNetwork->overlaps(*otherVirtual))) {
                service::common::fail(21002, "虚拟网段与全局已有网段重叠", 409);
            }
        }
        const auto virtualCidr = virtualNetwork->text();
        ruvia::db_query update(c.pool());
        auto updateBuilder = transaction.template repository<service::vpn::entities::VpnRouteEntity>().update_builder();
        updateBuilder.set(service::vpn::entities::VpnRouteEntity::column_name<"virtual_cidr">(), update.value(virtualCidr)).set(service::vpn::entities::VpnRouteEntity::column_name<"last_error">(), update.value("")).set(service::vpn::entities::VpnRouteEntity::column_name<"updated_at">(), update.call("now")).where(update.binary(update.column(service::vpn::entities::VpnRouteEntity::column_name<"id">()), Op::equal, update.cast(update.value(id), ruvia::db_data_type::uuid)));
        (void)co_await updateBuilder.execute();
        co_await transaction.commit();
        co_await audit(c, c.userId, "vpn.route.update", "vpn_route", id, "success", "{}");
        co_await queueEdgeConfig(c, edgePeerId);
        (void)co_await reconcileHub(c);
    }

    template <typename Context>
    ruvia::task<std::string> peers(Context& c, std::optional<std::string> networkId, std::optional<std::string> edgeNodeId = std::nullopt) {
        using Op = ruvia::db_binary_operator;
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::vpn::entities::VpnPeerEntity>().query_builder("p");
        queryBuilder.select({ { "id", query.call("jsonb_build_object", { detail::jsonKey(query, "id"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "p"), detail::jsonKey(query, "networkId"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), "p"), detail::jsonKey(query, "peerType"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">(), "p"), detail::jsonKey(query, "edgeNodeId"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">(), "p"), detail::jsonKey(query, "userId"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"user_id">(), "p"), detail::jsonKey(query, "name"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"name">(), "p"), detail::jsonKey(query, "publicKey"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"public_key">(), "p"), detail::jsonKey(query, "assignedIpv4"), query.call("host", { query.column(service::vpn::entities::VpnPeerEntity::column_name<"assigned_ipv4">(), "p") }), detail::jsonKey(query, "allowedRoutes"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"allowed_routes">(), "p"), detail::jsonKey(query, "status"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"status">(), "p"), detail::jsonKey(query, "configRevision"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"config_revision">(), "p"), detail::jsonKey(query, "lastHandshakeAt"), query.call("iot_utc_timestamp", { query.column(service::vpn::entities::VpnPeerEntity::column_name<"last_handshake_at">(), "p") }), detail::jsonKey(query, "createdAt"), query.call("iot_utc_timestamp", { query.column(service::vpn::entities::VpnPeerEntity::column_name<"created_at">(), "p") }) }) } });
        if (networkId && !networkId->empty()) {
            requireUuid(*networkId, "VPN 网络 ID 无效");
            queryBuilder.where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), "p"), Op::equal, query.cast(query.value(*networkId), ruvia::db_data_type::uuid)));
        }
        if (edgeNodeId && !edgeNodeId->empty()) {
            requireUuid(*edgeNodeId, "Edge 节点 ID 无效");
            queryBuilder.and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">(), "p"), Op::equal, query.cast(query.value(*edgeNodeId), ruvia::db_data_type::uuid)));
        }
        queryBuilder.order_by(query.column(service::vpn::entities::VpnPeerEntity::column_name<"created_at">(), "p"), ruvia::db_order_direction::desc).limit(1000);
        const auto rows = co_await queryBuilder.get_raw_many();
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
        if (!publicKey.empty() && !service::common::wireguard::validManagedKey(publicKey)) {
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
        auto networkQueryBuilder = c.db().template repository<service::vpn::entities::VpnNetworkEntity>().query_builder();
        networkQueryBuilder.select({ { "overlay_cidr", networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"overlay_cidr">()) }, { "status", networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"status">()) } }).where(networkQuery.binary(networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">()), Op::equal, networkQuery.cast(networkQuery.value(networkId), ruvia::db_data_type::uuid))).and_where(networkQuery.unary(ruvia::db_unary_operator::is_null, networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">())));
        const auto networkRows = co_await networkQueryBuilder.get_raw_many();
        if (networkRows.empty()) {
            service::common::fail(21004, "VPN 网络不存在", 404);
        }
        if (detail::rowValue(networkRows.front(), 1) != "enabled") {
            service::common::fail(21003, "VPN 网络已停用", 409);
        }
        if (peerType == "edge") {
            ruvia::db_query edgeQuery(c.pool());
            const auto vpnCapability = edgeQuery.binary(
                edgeQuery.binary(edgeQuery.column(service::edge_node::entities::EdgeNodeEntity::column_name<"capability">()), Op::json_get, edgeQuery.cast(edgeQuery.value("vpn"), ruvia::db_data_type::text)),
                Op::json_get_text,
                edgeQuery.cast(edgeQuery.value("publicKey"), ruvia::db_data_type::text)
            );
            auto edgeQueryBuilder = c.db().template repository<service::edge_node::entities::EdgeNodeEntity>().query_builder();
            edgeQueryBuilder.select({ { "value0", edgeQuery.coalesce({ vpnCapability, edgeQuery.value("") }) } }).where(edgeQuery.binary(edgeQuery.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">()), Op::equal, edgeQuery.cast(edgeQuery.value(edgeNodeId), ruvia::db_data_type::uuid))).and_where(edgeQuery.binary(edgeQuery.column(service::edge_node::entities::EdgeNodeEntity::column_name<"enrollment_status">()), Op::equal, edgeQuery.value("approved")));
            const auto edgeRows = co_await edgeQueryBuilder.get_raw_many();
            if (edgeRows.empty()) {
                service::common::fail(21004, "Edge 节点不存在或尚未批准", 404);
            }
            const auto reportedKey = detail::rowValue(edgeRows.front(), 0);
            publicKey = service::common::wireguard::validManagedKey(reportedKey) ? reportedKey : std::string{};
            ruvia::db_query existingQuery(c.pool());
            const auto revokedFirst = existingQuery.case_when(
                { { existingQuery.binary(existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()), Op::equal, existingQuery.value("revoked")),
                    existingQuery.cast(existingQuery.value(1), ruvia::db_data_type::integer) } },
                existingQuery.cast(existingQuery.value(0), ruvia::db_data_type::integer)
            );
            auto existingQueryBuilder = c.db().template repository<service::vpn::entities::VpnPeerEntity>().query_builder();
            existingQueryBuilder.select({ { "id", existingQuery.cast(existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), ruvia::db_data_type::text) }, { "status", existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()) } }).where(existingQuery.binary(existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">()), Op::equal, existingQuery.cast(existingQuery.value(networkId), ruvia::db_data_type::uuid))).and_where(existingQuery.binary(existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">()), Op::equal, existingQuery.cast(existingQuery.value(edgeNodeId), ruvia::db_data_type::uuid))).order_by(revokedFirst).add_order_by(existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"created_at">()), ruvia::db_order_direction::desc).limit(1);
            const auto existing = co_await existingQueryBuilder.get_raw_many();
            if (!existing.empty()) {
                if (detail::rowValue(existing.front(), 1) != "revoked") {
                    service::common::fail(21002, "该 Edge 节点已经加入 VPN 网络", 409);
                }
                reusableEdgePeerId = detail::rowValue(existing.front(), 0);
            }
            userId.clear();
        }
        const auto overlay = service::utils::network::parseCidr(detail::rowValue(networkRows.front(), 0), 16, 30);
        if (!overlay) {
            service::common::fail(21005, "VPN 网络 Overlay 配置损坏", 500);
        }
        std::vector<std::string> allowedRoutes;
        if (const auto& routes = payload.get<"allowedRoutes">()) {
            for (const auto& route : *routes) {
                allowedRoutes.emplace_back(route.view());
            }
        }
        if (allowedRoutes.size() > 64) {
            service::common::fail(21001, "Peer 最多授权 64 条路由", 400);
        }
        co_await validateAllowedRoutes(c, networkId, allowedRoutes);
        const auto id = reusableEdgePeerId.empty() ? service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>())
                                                   : reusableEdgePeerId;
        const bool edge = peerType == "edge";
        const auto status = publicKey.empty() ? "pending" : "active";
        const auto allowedRoutesJson = detail::jsonArray(allowedRoutes);
        // Address allocation here and route mapping in the VPN feature both take
        // lock::kVpnAddressAllocation. The feature maps routes in its own transaction
        // on this Worker, so no transaction of this request may hold the lock while
        // the sync-edge-routes reply is awaited. An edge peer is mapped while its row
        // is revoked: neither Hub reconciliation nor an edge capability report installs
        // a revoked peer's key, so a failed mapping never exposes a half-created peer.
        // A new edge peer is therefore first committed revoked, reserving its address.
        if (reusableEdgePeerId.empty()) {
            auto transaction = co_await c.db("vpn-address-allocation").begin_transaction();
            co_await transaction.advisory_lock(service::message::lock::kVpnAddressAllocation);
            const auto assigned = co_await allocateAddressFromDb(transaction, networkId, *overlay);
            const auto assignedText = detail::hostText(*assigned);
            ruvia::db_query insert(c.pool());
            const std::array<std::string_view, 11> insertColumns{ service::vpn::entities::VpnPeerEntity::column_name<"id">(), service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), service::vpn::entities::VpnPeerEntity::column_name<"peer_type">(), service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">(), service::vpn::entities::VpnPeerEntity::column_name<"user_id">(), service::vpn::entities::VpnPeerEntity::column_name<"name">(), service::vpn::entities::VpnPeerEntity::column_name<"public_key">(), service::vpn::entities::VpnPeerEntity::column_name<"assigned_ipv4">(), service::vpn::entities::VpnPeerEntity::column_name<"allowed_routes">(), service::vpn::entities::VpnPeerEntity::column_name<"status">(), service::vpn::entities::VpnPeerEntity::column_name<"revoked_at">() };
            auto insertBuilder = transaction.template repository<service::vpn::entities::VpnPeerEntity>().insert_builder();
            ruvia::db_values_builder insertValues(insertColumns);
            insertValues.row({ insert.cast(insert.value(id), ruvia::db_data_type::uuid), insert.cast(insert.value(networkId), ruvia::db_data_type::uuid), insert.value(peerType), insert.cast(insert.null_if(insert.cast(insert.value(edgeNodeId), ruvia::db_data_type::text), insert.cast(insert.value(""), ruvia::db_data_type::text)), ruvia::db_data_type::uuid), insert.cast(insert.null_if(insert.cast(insert.value(userId), ruvia::db_data_type::text), insert.cast(insert.value(""), ruvia::db_data_type::text)), ruvia::db_data_type::uuid), insert.value(name), insert.value(publicKey), insert.cast(insert.value(assignedText), ruvia::db_data_type::inet), insert.cast(insert.value(allowedRoutesJson), ruvia::db_data_type::jsonb), insert.value(edge ? "revoked" : status), edge ? insert.call("now") : insert.null_value() });
            insertBuilder.insert_from(insertColumns, insertValues.query());
            (void)co_await insertBuilder.execute();
            co_await transaction.commit();
        }
        if (edge) {
            std::exception_ptr failure;
            try {
                co_await syncEdgeRoutes(c, id, networkId, edgeNodeId);
            } catch (...) {
                failure = std::current_exception();
            }
            if (failure) {
                // Creation is all-or-nothing for the caller: drop the unmapped new
                // peer. A reused row simply stays revoked.
                if (reusableEdgePeerId.empty()) {
                    co_await discardEdgePeer(c, id);
                }
                std::rethrow_exception(failure);
            }
            auto transaction = co_await c.db("vpn-address-allocation").begin_transaction();
            co_await transaction.advisory_lock(service::message::lock::kVpnAddressAllocation);
            ruvia::db_query reactivation(c.pool());
            auto reactivationBuilder = transaction.template repository<service::vpn::entities::VpnPeerEntity>().update_builder();
            reactivationBuilder.set(service::vpn::entities::VpnPeerEntity::column_name<"name">(), reactivation.value(name)).set(service::vpn::entities::VpnPeerEntity::column_name<"public_key">(), reactivation.value(publicKey)).set(service::vpn::entities::VpnPeerEntity::column_name<"allowed_routes">(), reactivation.cast(reactivation.value(allowedRoutesJson), ruvia::db_data_type::jsonb)).set(service::vpn::entities::VpnPeerEntity::column_name<"status">(), reactivation.value(status)).set(service::vpn::entities::VpnPeerEntity::column_name<"revoked_at">(), reactivation.null_value()).set(service::vpn::entities::VpnPeerEntity::column_name<"updated_at">(), reactivation.call("now")).where(reactivation.binary(reactivation.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), Op::equal, reactivation.cast(reactivation.value(id), ruvia::db_data_type::uuid))).and_where(reactivation.binary(reactivation.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">()), Op::equal, reactivation.value("edge"))).and_where(reactivation.binary(reactivation.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()), Op::equal, reactivation.value("revoked"))).returning({ { "id", reactivation.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()) } });
            const auto reactivated = co_await reactivationBuilder.get_raw_many();
            if (reactivated.empty()) {
                // A concurrent request for the same EdgeNode reactivated this row first.
                service::common::fail(21002, "该 Edge 节点已经加入 VPN 网络", 409);
            }
            co_await transaction.commit();
        }
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
        auto updateBuilder = c.db().template repository<service::vpn::entities::VpnPeerEntity>().update_builder();
        updateBuilder.set(service::vpn::entities::VpnPeerEntity::column_name<"status">(), update.value("revoked")).set(service::vpn::entities::VpnPeerEntity::column_name<"revoked_at">(), update.call("now")).set(service::vpn::entities::VpnPeerEntity::column_name<"updated_at">(), update.call("now")).where(update.binary(update.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), Op::equal, update.cast(update.value(id), ruvia::db_data_type::uuid))).and_where(update.binary(update.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()), Op::not_equal, update.value("revoked"))).returning({ { "public_key", update.column(service::vpn::entities::VpnPeerEntity::column_name<"public_key">()) }, { "edge_node_id", update.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">()) } });
        const auto rows = co_await updateBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(21004, "VPN Peer 不存在或已撤销", 404);
        }
        const auto key = detail::rowValue(rows.front(), 0);
        // Format-only check: any key that may be on the Hub interface must be removable.
        if (service::common::wireguard::validKey(key)) {
            (void)co_await control(c, service::message::vpn::kRemovePeerOperation, key);
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
        auto currentBuilder = c.db().template repository<service::vpn::entities::VpnPeerEntity>().query_builder();
        currentBuilder.select({ { "edge_node_id", current.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">()) }, { "peer_type", current.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">()) }, { "status", current.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()) }, { "network_id", current.cast(current.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">()), ruvia::db_data_type::text) } }).where(current.binary(current.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), Op::equal, current.cast(current.value(id), ruvia::db_data_type::uuid)));
        const auto rows = co_await currentBuilder.get_raw_many();
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
        // The feature maps routes under lock::kVpnAddressAllocation in its own transaction.
        co_await syncEdgeRoutes(c, id, networkId, edgeNodeId);
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

    // Network of a desktop Peer; vpn change ids are network ids.
    template <typename Context>
    ruvia::task<std::string> desktopPeerNetworkId(Context& c, std::string_view id) {
        using Op = ruvia::db_binary_operator;
        requireUuid(id, "VPN Peer ID 无效");
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::vpn::entities::VpnPeerEntity>().query_builder();
        queryBuilder.select({ { "network_id", query.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">()) } }).where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), Op::equal, query.cast(query.value(id), ruvia::db_data_type::uuid)));
        const auto rows = co_await queryBuilder.get_raw_many();
        co_return rows.empty() ? std::string{} : detail::rowValue(rows.front(), 0);
    }

    template <typename Context>
    ruvia::task<void> desktopDeletePeer(Context& c, std::string_view id);

    template <typename Context>
    ruvia::task<std::string> openDesktopSession(Context& c, std::string_view peerId) {
        // Ownership, selections and current authorization must be valid before
        // the connection can create demand. A saved Peer alone creates none.
        (void)co_await desktopPeerConfig(c, peerId);
        const auto connectionId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        co_await renewDesktopSession(c, peerId, connectionId);
        std::exception_ptr failure;
        try {
            co_await reconcileHub(c);
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            co_await closeDesktopSession(c, peerId, connectionId);
            std::rethrow_exception(failure);
        }
        co_return connectionId;
    }

    template <typename Context>
    ruvia::task<void> renewDesktopSession(Context& c, std::string_view peerId, std::string_view connectionId) {
        const auto key = service::message::vpn::desktopSessionKey(peerId);
        const auto duration = std::to_string(service::message::vpn::kDesktopSessionLeaseMs);
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ connectionId, duration };
        const auto reply = co_await c.redis().eval(R"lua(
local time=redis.call('TIME')
local now=time[1]*1000+math.floor(time[2]/1000)
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
redis.call('ZADD',KEYS[1],now+tonumber(ARGV[2]),ARGV[1])
redis.call('PEXPIRE',KEYS[1],ARGV[2])
return 1
)lua",
                                                   keys,
                                                   arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer || reply.integer() != 1) {
            service::utils::redis::throwValue("renew desktop VPN session", reply);
        }
    }

    template <typename Context>
    ruvia::task<void> closeDesktopSession(Context& c, std::string_view peerId, std::string_view connectionId) {
        const auto key = service::message::vpn::desktopSessionKey(peerId);
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ connectionId };
        // A late close removes only its own connection, including after reconnect.
        const auto reply = co_await c.redis().eval("return redis.call('ZREM',KEYS[1],ARGV[1])", keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("close desktop VPN session", reply);
        }
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
    co_await transaction.advisory_lock(service::message::lock::kVpnDefaultNetwork);
    using Op = ruvia::db_binary_operator;
    ruvia::db_query existingQuery(c.pool());
    auto existingQueryBuilder = transaction.template repository<service::vpn::entities::VpnNetworkEntity>().query_builder();
    existingQueryBuilder.select({ { "id", existingQuery.cast(existingQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">()), ruvia::db_data_type::text) } }).where(existingQuery.binary(existingQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"name">()), Op::equal, existingQuery.value(kDefaultNetworkName))).and_where(existingQuery.unary(ruvia::db_unary_operator::is_null, existingQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">()))).limit(1);
    const auto existing = co_await existingQueryBuilder.get_raw_many();
    std::string networkId = std::string(kDefaultNetworkId);
    if (!existing.empty()) {
        networkId = detail::rowValue(existing.front(), 0);
    } else {
        ruvia::db_query insert(c.pool());
        const std::array<std::string_view, 7> insertColumns{ service::vpn::entities::VpnNetworkEntity::column_name<"id">(), service::vpn::entities::VpnNetworkEntity::column_name<"name">(), service::vpn::entities::VpnNetworkEntity::column_name<"overlay_cidr">(), service::vpn::entities::VpnNetworkEntity::column_name<"hub_public_key">(), service::vpn::entities::VpnNetworkEntity::column_name<"hub_endpoint">(), service::vpn::entities::VpnNetworkEntity::column_name<"hub_listen_port">(), service::vpn::entities::VpnNetworkEntity::column_name<"created_by">() };
        auto insertBuilder = transaction.template repository<service::vpn::entities::VpnNetworkEntity>().insert_builder();
        ruvia::db_values_builder insertValues(insertColumns);
        insertValues.row({ insert.cast(insert.value(kDefaultNetworkId), ruvia::db_data_type::uuid), insert.value(kDefaultNetworkName), insert.value(kDefaultOverlayCidr), insert.value(hubPublicKey), insert.value(hubEndpoint), insert.value(static_cast<int>(hubListenPort)), insert.cast(insert.value(c.userId), ruvia::db_data_type::uuid) });

        const std::array<ruvia::borrowed_text, 1> conflictColumns1{ { service::vpn::entities::VpnNetworkEntity::column_name<"id">() } };
        const std::array<ruvia::borrowed_text, 4> updatedColumns2{ { service::vpn::entities::VpnNetworkEntity::column_name<"name">(), service::vpn::entities::VpnNetworkEntity::column_name<"overlay_cidr">(), service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">(), service::vpn::entities::VpnNetworkEntity::column_name<"updated_at">() } };
        const std::array<ruvia::db_assignment, 4> conflictUpdates3{ { { service::vpn::entities::VpnNetworkEntity::column_name<"name">(), insert.excluded(service::vpn::entities::VpnNetworkEntity::column_name<"name">()) }, { service::vpn::entities::VpnNetworkEntity::column_name<"overlay_cidr">(), insert.excluded(service::vpn::entities::VpnNetworkEntity::column_name<"overlay_cidr">()) }, { service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">(), insert.null_value() }, { service::vpn::entities::VpnNetworkEntity::column_name<"updated_at">(), insert.call("now") } } };
        insertBuilder.insert_from(insertColumns, insertValues.query()).on_conflict({ .conflict_paths_ = conflictColumns1, .update_columns_ = updatedColumns2, .update_expressions_ = conflictUpdates3 }).returning({ { "id", insert.cast(insert.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">()), ruvia::db_data_type::text) } });
        const auto inserted = co_await insertBuilder.get_raw_many();
        if (inserted.empty()) {
            service::common::fail(21005, "默认 iot-server VPN 网络创建失败", 500);
        }
        networkId = detail::rowValue(inserted.front(), 0);
    }
    ruvia::db_query update(c.pool());
    auto updateBuilder = transaction.template repository<service::vpn::entities::VpnNetworkEntity>().update_builder();
    updateBuilder.set(service::vpn::entities::VpnNetworkEntity::column_name<"name">(), update.value(kDefaultNetworkName)).set(service::vpn::entities::VpnNetworkEntity::column_name<"overlay_cidr">(), update.value(kDefaultOverlayCidr)).set(service::vpn::entities::VpnNetworkEntity::column_name<"status">(), update.value("enabled")).set(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">(), update.null_value()).set(service::vpn::entities::VpnNetworkEntity::column_name<"updated_at">(), update.call("now")).where(update.binary(update.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">()), Op::equal, update.cast(update.value(networkId), ruvia::db_data_type::uuid)));
    (void)co_await updateBuilder.execute();
    co_await transaction.commit();
    co_return networkId;
}

template <typename Db>
ruvia::task<std::optional<std::uint32_t>> VpnService::allocateAddressFromDb(
    Db& db,
    std::string_view networkId,
    const service::utils::network::Ipv4Cidr& overlay
) {
    using Op = ruvia::db_binary_operator;
    ruvia::db_query query;
    const auto reusable = query.binary(
        query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">()), Op::not_equal, query.value("windows")),
        Op::or_value,
        query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()), Op::not_equal, query.value("revoked"))
    );
    auto queryBuilder = db.template repository<service::vpn::entities::VpnPeerEntity>().query_builder();
    queryBuilder.select({ { "assigned_ipv4", query.call("host", { query.column(service::vpn::entities::VpnPeerEntity::column_name<"assigned_ipv4">()) }) } }).where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">()), Op::equal, query.cast(query.value(networkId), ruvia::db_data_type::uuid))).and_where(reusable);
    const auto rows = co_await queryBuilder.get_raw_many();
    std::unordered_set<std::uint32_t> used;
    for (const auto& row : rows) {
        if (const auto value = service::utils::network::parseIpv4(detail::rowValue(row, 0))) {
            used.emplace(*value);
        }
    }
    for (std::uint32_t offset = 2; offset + 1 < overlay.size(); ++offset) {
        const auto candidate = *service::utils::network::hostAddress(overlay, offset);
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
        if (!service::utils::network::parseCidr(route, 1, 30)) {
            service::common::fail(21001, "Peer 授权路由 CIDR 无效", 400);
        }
    }
    using Op = ruvia::db_binary_operator;
    ruvia::db_query query(c.pool());
    auto queryBuilder = c.db().template repository<service::vpn::entities::VpnRouteEntity>().query_builder();
    queryBuilder.select({ { "virtual_cidr", query.column(service::vpn::entities::VpnRouteEntity::column_name<"virtual_cidr">()) } }).where(query.binary(query.column(service::vpn::entities::VpnRouteEntity::column_name<"network_id">()), Op::equal, query.cast(query.value(networkId), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::vpn::entities::VpnRouteEntity::column_name<"enabled">()), Op::equal, query.value(true)));
    const auto rows = co_await queryBuilder.get_raw_many();
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
        ruvia::db_order_term{ routeQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"virtual_cidr">(), "route") }
    };
    routeQuery
        .select(routeQuery.aggregate("jsonb_agg", { routeQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"virtual_cidr">(), "route") }, false, routeOrder))
        .from(service::vpn::entities::VpnRouteEntity::table_name(), "route")
        .where(routeQuery.binary(routeQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"edge_peer_id">(), "route"), Op::equal, routeQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "peer")))
        .and_where(routeQuery.binary(routeQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"network_id">(), "route"), Op::equal, routeQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "network")))
        .and_where(routeQuery.binary(routeQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"enabled">(), "route"), Op::equal, routeQuery.value(true)))
        .and_where(routeQuery.binary(routeQuery.column(service::vpn::entities::VpnRouteEntity::column_name<"status">(), "route"), Op::equal, routeQuery.value("active")));
    ruvia::db_query query(c.pool());
    const auto emptyRoutes = query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
    auto queryBuilder = c.db().template repository<service::edge_node::entities::EdgeNodeEntity>().query_builder("edge");
    queryBuilder.select({ { "id", query.call("jsonb_build_object", { detail::jsonKey(query, "id"), query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "edge"), detail::jsonKey(query, "name"), query.coalesce({ query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"name">(), "edge"), query.value("") }), detail::jsonKey(query, "imei"), query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"imei">(), "edge"), detail::jsonKey(query, "virtualCidrs"), query.coalesce({ query.subquery(routeQuery), emptyRoutes }), detail::jsonKey(query, "assignedIpv4"), query.call("host", { query.column(service::vpn::entities::VpnPeerEntity::column_name<"assigned_ipv4">(), "peer") }) }) }, { "value1", query.cast(query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "edge"), ruvia::db_data_type::text) } }).template join<service::vpn::entities::VpnPeerEntity>(ruvia::db_join_type::inner, "peer", query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"edge_node_id">(), "peer"), Op::equal, query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "edge"))).template join<service::vpn::entities::VpnNetworkEntity>(ruvia::db_join_type::inner, "network", query.binary(query.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "network"), Op::equal, query.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), "peer"))).where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">(), "peer"), Op::equal, query.value("edge"))).and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"status">(), "peer"), Op::equal, query.value("active"))).and_where(query.binary(query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"enrollment_status">(), "edge"), Op::equal, query.value("approved"))).and_where(query.binary(query.column(service::vpn::entities::VpnNetworkEntity::column_name<"name">(), "network"), Op::equal, query.value(kDefaultNetworkName))).and_where(query.binary(query.column(service::vpn::entities::VpnNetworkEntity::column_name<"status">(), "network"), Op::equal, query.value("enabled"))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">(), "network"))).order_by(query.coalesce({ query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"name">(), "edge"), query.value("") })).add_order_by(query.column(service::edge_node::entities::EdgeNodeEntity::column_name<"id">(), "edge"));
    const auto rows = co_await queryBuilder.get_raw_many();
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
    if (!service::common::wireguard::validManagedKey(publicKey)) {
        service::common::fail(21001, "WireGuard 公钥格式无效", 400);
    }
    const auto ids = normalizeVpnNodeIds(payload.get<"edgeNodeIds">());
    auto tx = co_await c.db("vpn-address-allocation").begin_transaction();
    // Serialize allocation and the public-key retry lookup together. A lost POST
    // response can safely be retried with the locally persisted keypair.
    co_await tx.advisory_lock(service::message::lock::kVpnAddressAllocation);
    using Op = ruvia::db_binary_operator;
    ruvia::db_query existingQuery(c.pool());
    auto existingQueryBuilder = tx.template repository<service::vpn::entities::VpnPeerEntity>().query_builder();
    existingQueryBuilder.select({ { "id", existingQuery.cast(existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), ruvia::db_data_type::text) }, { "user_id", existingQuery.cast(existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"user_id">()), ruvia::db_data_type::text) }, { "status", existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()) }, { "client_managed", existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"client_managed">()) } }).where(existingQuery.binary(existingQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"public_key">()), Op::equal, existingQuery.value(publicKey))).set_lock({ .mode_ = ruvia::db_row_lock::update });
    const auto existing = co_await existingQueryBuilder.get_raw_many();
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
    auto networkQueryBuilder = tx.template repository<service::vpn::entities::VpnNetworkEntity>().query_builder();
    networkQueryBuilder.select({ { "id", networkQuery.cast(networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">()), ruvia::db_data_type::text) }, { "overlay_cidr", networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"overlay_cidr">()) } }).where(networkQuery.binary(networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"name">()), Op::equal, networkQuery.value(kDefaultNetworkName))).and_where(networkQuery.binary(networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"status">()), Op::equal, networkQuery.value("enabled"))).and_where(networkQuery.unary(ruvia::db_unary_operator::is_null, networkQuery.column(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">()))).set_lock({ .mode_ = ruvia::db_row_lock::share });
    const auto network = co_await networkQueryBuilder.get_raw_many();
    if (network.empty()) {
        service::common::fail(21004, "VPN 网络不存在或已停用", 404);
    }
    const auto networkId = detail::rowValue(network.front(), 0);
    const auto overlay = service::utils::network::parseCidr(detail::rowValue(network.front(), 1), 16, 30);
    if (!overlay) {
        service::common::fail(21005, "VPN 网络 Overlay 配置损坏", 500);
    }
    co_await detail::validateSelectedEdges(tx, networkId, ids);
    const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
    const auto assigned = co_await allocateAddressFromDb(tx, networkId, *overlay);
    if (!assigned) {
        service::common::fail(21002, "VPN 网络地址已用尽", 409);
    }
    const auto assignedText = detail::hostText(*assigned);
    ruvia::db_query insert(c.pool());
    const std::array<std::string_view, 10> insertColumns{ service::vpn::entities::VpnPeerEntity::column_name<"id">(), service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), service::vpn::entities::VpnPeerEntity::column_name<"peer_type">(), service::vpn::entities::VpnPeerEntity::column_name<"user_id">(), service::vpn::entities::VpnPeerEntity::column_name<"name">(), service::vpn::entities::VpnPeerEntity::column_name<"public_key">(), service::vpn::entities::VpnPeerEntity::column_name<"assigned_ipv4">(), service::vpn::entities::VpnPeerEntity::column_name<"allowed_routes">(), service::vpn::entities::VpnPeerEntity::column_name<"status">(), service::vpn::entities::VpnPeerEntity::column_name<"client_managed">() };
    auto insertBuilder = tx.template repository<service::vpn::entities::VpnPeerEntity>().insert_builder();
    ruvia::db_values_builder insertValues(insertColumns);
    insertValues.row({ insert.cast(insert.value(id), ruvia::db_data_type::uuid), insert.cast(insert.value(networkId), ruvia::db_data_type::uuid), insert.value("windows"), insert.cast(insert.value(c.userId), ruvia::db_data_type::uuid), insert.value(name), insert.value(publicKey), insert.cast(insert.value(assignedText), ruvia::db_data_type::inet), insert.cast(insert.value("[]"), ruvia::db_data_type::jsonb), insert.value("active"), insert.value(true) });
    insertBuilder.insert_from(insertColumns, insertValues.query());
    (void)co_await insertBuilder.execute();
    for (const auto& edge : ids) {
        ruvia::db_query selection(c.pool());
        const std::array<std::string_view, 2> selectionColumns{ service::vpn::entities::VpnPeerEdgeSelectionEntity::column_name<"peer_id">(), service::vpn::entities::VpnPeerEdgeSelectionEntity::column_name<"edge_node_id">() };
        auto selectionBuilder = tx.template repository<service::vpn::entities::VpnPeerEdgeSelectionEntity>().insert_builder();
        ruvia::db_values_builder selectionValues(selectionColumns);
        selectionValues.row({ selection.cast(selection.value(id), ruvia::db_data_type::uuid), selection.cast(selection.value(edge), ruvia::db_data_type::uuid) });
        selectionBuilder.insert_from(selectionColumns, selectionValues.query());
        (void)co_await selectionBuilder.execute();
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
    auto parentBuilder = tx.template repository<service::vpn::entities::VpnPeerEntity>().query_builder();
    parentBuilder.select({ { "network_id", parent.cast(parent.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">()), ruvia::db_data_type::text) } }).where(parent.binary(parent.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), Op::equal, parent.cast(parent.value(id), ruvia::db_data_type::uuid))).and_where(parent.binary(parent.column(service::vpn::entities::VpnPeerEntity::column_name<"user_id">()), Op::equal, parent.cast(parent.value(c.userId), ruvia::db_data_type::uuid))).and_where(parent.binary(parent.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">()), Op::equal, parent.value("windows"))).and_where(parent.binary(parent.column(service::vpn::entities::VpnPeerEntity::column_name<"client_managed">()), Op::equal, parent.value(true))).and_where(parent.binary(parent.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()), Op::equal, parent.value("active"))).set_lock({ .mode_ = ruvia::db_row_lock::update });
    const auto rows = co_await parentBuilder.get_raw_many();
    if (rows.empty()) {
        service::common::fail(21004, "Windows VPN 配置不存在或不属于当前用户", 404);
    }
    co_await detail::validateSelectedEdges(tx, detail::rowValue(rows.front(), 0), ids);
    ruvia::db_query removal(c.pool());
    auto removalBuilder = tx.template repository<service::vpn::entities::VpnPeerEdgeSelectionEntity>().delete_builder();
    removalBuilder.where(removal.binary(removal.column(service::vpn::entities::VpnPeerEdgeSelectionEntity::column_name<"peer_id">()), Op::equal, removal.cast(removal.value(id), ruvia::db_data_type::uuid)));
    (void)co_await removalBuilder.execute();
    for (const auto& edge : ids) {
        ruvia::db_query selection(c.pool());
        const std::array<std::string_view, 2> selectionColumns{ service::vpn::entities::VpnPeerEdgeSelectionEntity::column_name<"peer_id">(), service::vpn::entities::VpnPeerEdgeSelectionEntity::column_name<"edge_node_id">() };
        auto selectionBuilder = tx.template repository<service::vpn::entities::VpnPeerEdgeSelectionEntity>().insert_builder();
        ruvia::db_values_builder selectionValues(selectionColumns);
        selectionValues.row({ selection.cast(selection.value(id), ruvia::db_data_type::uuid), selection.cast(selection.value(edge), ruvia::db_data_type::uuid) });
        selectionBuilder.insert_from(selectionColumns, selectionValues.query());
        (void)co_await selectionBuilder.execute();
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
        ruvia::db_order_term{ selectionQuery.column(service::vpn::entities::VpnPeerEdgeSelectionEntity::column_name<"edge_node_id">(), "selection") }
    };
    selectionQuery
        .select(selectionQuery.aggregate("jsonb_agg", { selectionQuery.column(service::vpn::entities::VpnPeerEdgeSelectionEntity::column_name<"edge_node_id">(), "selection") }, false, selectionOrder))
        .from(service::vpn::entities::VpnPeerEdgeSelectionEntity::table_name(), "selection")
        .where(selectionQuery.binary(selectionQuery.column(service::vpn::entities::VpnPeerEdgeSelectionEntity::column_name<"peer_id">(), "selection"), Op::equal, selectionQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "peer")));

    constexpr std::string_view kAllowedRouteColumn = "route";
    ruvia::db_query routeAccess(c.pool());
    routeAccess
        .select(routeAccess.alias(routeAccess.column(service::vpn::entities::VpnEffectiveRouteAccessEntity::column_name<"virtual_cidr">(), "access"), kAllowedRouteColumn))
        .from(service::vpn::entities::VpnEffectiveRouteAccessEntity::table_name(), "access")
        .where(routeAccess.binary(routeAccess.column(service::vpn::entities::VpnEffectiveRouteAccessEntity::column_name<"peer_id">(), "access"), Op::equal, routeAccess.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "peer")));
    ruvia::db_query edgeAccess(c.pool());
    edgeAccess
        .select(edgeAccess.binary(edgeAccess.column(service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_name<"edge_address">(), "access"), Op::concat, edgeAccess.cast(edgeAccess.value("/32"), ruvia::db_data_type::text)))
        .from(service::vpn::entities::VpnEffectiveEdgeAccessEntity::table_name(), "access")
        .where(edgeAccess.binary(edgeAccess.column(service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_name<"peer_id">(), "access"), Op::equal, edgeAccess.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "peer")));
    routeAccess.combine(ruvia::db_set_operation::union_value, edgeAccess);

    ruvia::db_query allowedQuery(c.pool());
    const std::array allowedOrder{
        ruvia::db_order_term{ allowedQuery.column(kAllowedRouteColumn, "allowed") }
    };
    allowedQuery
        .select(allowedQuery.aggregate("jsonb_agg", { allowedQuery.column(kAllowedRouteColumn, "allowed") }, false, allowedOrder))
        .from(routeAccess, "allowed");

    ruvia::db_query edgeAddressQuery(c.pool());
    const std::array edgeAddressOrder{
        ruvia::db_order_term{ edgeAddressQuery.column(service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_name<"edge_address">(), "access") }
    };
    edgeAddressQuery
        .select(edgeAddressQuery.aggregate(
            "jsonb_agg",
            { edgeAddressQuery.column(service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_name<"edge_address">(), "access") },
            false,
            edgeAddressOrder
        ))
        .from(service::vpn::entities::VpnEffectiveEdgeAccessEntity::table_name(), "access")
        .where(edgeAddressQuery.binary(edgeAddressQuery.column(service::vpn::entities::VpnEffectiveEdgeAccessEntity::column_name<"peer_id">(), "access"), Op::equal, edgeAddressQuery.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "peer")));

    ruvia::db_query query(c.pool());
    const auto emptyRoutes = query.cast(query.value("[]"), ruvia::db_data_type::jsonb);
    const auto networkEnabled = query.binary(query.column(service::vpn::entities::VpnNetworkEntity::column_name<"status">(), "network"), Op::equal, query.value("enabled"));
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
    auto queryBuilder = c.db().template repository<service::vpn::entities::VpnPeerEntity>().query_builder("peer");
    queryBuilder.select({ { "id", query.call("jsonb_build_object", { detail::jsonKey(query, "peerId"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "peer"), detail::jsonKey(query, "name"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"name">(), "peer"), detail::jsonKey(query, "publicKey"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"public_key">(), "peer"), detail::jsonKey(query, "networkEnabled"), networkEnabled, detail::jsonKey(query, "assignedIpv4"), query.call("host", { query.column(service::vpn::entities::VpnPeerEntity::column_name<"assigned_ipv4">(), "peer") }), detail::jsonKey(query, "hubPublicKey"), query.column(service::vpn::entities::VpnNetworkEntity::column_name<"hub_public_key">(), "network"), detail::jsonKey(query, "hubEndpoint"), query.column(service::vpn::entities::VpnNetworkEntity::column_name<"hub_endpoint">(), "network"), detail::jsonKey(query, "hubListenPort"), query.column(service::vpn::entities::VpnNetworkEntity::column_name<"hub_listen_port">(), "network"), detail::jsonKey(query, "mtu"), query.cast(query.value(1280), ruvia::db_data_type::integer), detail::jsonKey(query, "persistentKeepalive"), query.cast(query.value(120), ruvia::db_data_type::integer), detail::jsonKey(query, "configRevision"), query.column(service::vpn::entities::VpnPeerEntity::column_name<"config_revision">(), "peer"), detail::jsonKey(query, "edgeNodeIds"), query.coalesce({ query.subquery(selectionQuery), emptyRoutes }), detail::jsonKey(query, "allowedRoutes"), allowedRoutes, detail::jsonKey(query, "edgeAddresses"), edgeAddresses }) }, { "hub_public_key", query.column(service::vpn::entities::VpnNetworkEntity::column_name<"hub_public_key">(), "network") }, { "hub_endpoint", query.column(service::vpn::entities::VpnNetworkEntity::column_name<"hub_endpoint">(), "network") } }).template join<service::vpn::entities::VpnNetworkEntity>(ruvia::db_join_type::inner, "network", query.binary(query.column(service::vpn::entities::VpnNetworkEntity::column_name<"id">(), "network"), Op::equal, query.column(service::vpn::entities::VpnPeerEntity::column_name<"network_id">(), "peer"))).where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"id">(), "peer"), Op::equal, query.cast(query.value(id), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"user_id">(), "peer"), Op::equal, query.cast(query.value(c.userId), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">(), "peer"), Op::equal, query.value("windows"))).and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"client_managed">(), "peer"), Op::equal, query.value(true))).and_where(query.binary(query.column(service::vpn::entities::VpnPeerEntity::column_name<"status">(), "peer"), Op::equal, query.value("active"))).and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(service::vpn::entities::VpnNetworkEntity::column_name<"deleted_at">(), "network")));
    const auto rows = co_await queryBuilder.get_raw_many();
    if (rows.empty()) {
        service::common::fail(21004, "Windows VPN 配置不存在或不属于当前用户", 404);
    }
    if (!service::common::wireguard::validManagedKey(detail::rowValue(rows.front(), 1)) || detail::rowValue(rows.front(), 2).empty()) {
        service::common::fail(21005, "VPN Hub 尚未配置完成", 503);
    }
    co_return detail::rowValue(rows.front(), 0);
}

template <typename Context>
ruvia::task<void> VpnService::desktopDeletePeer(Context& c, std::string_view id) {
    using Op = ruvia::db_binary_operator;
    requireUuid(id, "VPN Peer ID 无效");
    ruvia::db_query update(c.pool());
    auto updateBuilder = c.db().template repository<service::vpn::entities::VpnPeerEntity>().update_builder();
    updateBuilder.set(service::vpn::entities::VpnPeerEntity::column_name<"status">(), update.value("revoked")).set(service::vpn::entities::VpnPeerEntity::column_name<"revoked_at">(), update.call("now")).set(service::vpn::entities::VpnPeerEntity::column_name<"updated_at">(), update.call("now")).where(update.binary(update.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), Op::equal, update.cast(update.value(id), ruvia::db_data_type::uuid))).and_where(update.binary(update.column(service::vpn::entities::VpnPeerEntity::column_name<"user_id">()), Op::equal, update.cast(update.value(c.userId), ruvia::db_data_type::uuid))).and_where(update.binary(update.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">()), Op::equal, update.value("windows"))).and_where(update.binary(update.column(service::vpn::entities::VpnPeerEntity::column_name<"client_managed">()), Op::equal, update.value(true))).returning({ { "public_key", update.column(service::vpn::entities::VpnPeerEntity::column_name<"public_key">()) } });
    const auto rows = co_await updateBuilder.get_raw_many();
    if (rows.empty()) {
        service::common::fail(21004, "Windows VPN 配置不存在或不属于当前用户", 404);
    }
    co_await audit(c, c.userId, "vpn.desktop.revoke", "vpn_peer", id, "success", "{}");
    (void)co_await reconcileHub(c);
}

template <typename Context>
ruvia::task<void> VpnService::queueEdgeConfig(Context& c, std::string_view peerId) {
    (void)co_await control(c, service::message::vpn::kQueueEdgeConfigOperation, service::message::rpc::encodeLinePair(peerId, c.userId));
}

template <typename Context>
ruvia::task<void> VpnService::syncEdgeRoutes(Context& c, std::string_view peerId, std::string_view networkId, std::string_view edgeNodeId) {
    service::message::vpn::EdgeRouteSync request(ruvia::model_options{ .resource_ = c.pool() });
    request.set<"peerId">(peerId).set<"networkId">(networkId).set<"edgeNodeId">(edgeNodeId).set<"actorId">(std::string_view(c.userId));
    (void)co_await control(c, service::message::vpn::kSyncEdgeRoutesOperation, service::message::rpc::encode(request, c.pool()));
}

template <typename Context>
ruvia::task<void> VpnService::discardEdgePeer(Context& c, std::string_view id) {
    using Op = ruvia::db_binary_operator;
    auto transaction = co_await c.db("vpn-address-allocation").begin_transaction();
    co_await transaction.advisory_lock(service::message::lock::kVpnAddressAllocation);
    ruvia::db_query removal(c.pool());
    auto removalBuilder = transaction.template repository<service::vpn::entities::VpnPeerEntity>().delete_builder();
    removalBuilder.where(removal.binary(removal.column(service::vpn::entities::VpnPeerEntity::column_name<"id">()), Op::equal, removal.cast(removal.value(id), ruvia::db_data_type::uuid))).and_where(removal.binary(removal.column(service::vpn::entities::VpnPeerEntity::column_name<"peer_type">()), Op::equal, removal.value("edge"))).and_where(removal.binary(removal.column(service::vpn::entities::VpnPeerEntity::column_name<"status">()), Op::equal, removal.value("revoked")));
    (void)co_await removalBuilder.execute();
    co_await transaction.commit();
}

template <typename Context>
ruvia::task<void> VpnService::audit(Context& c, std::string_view actor, std::string_view action, std::string_view resource, std::string_view resourceId, std::string_view outcome, std::string_view details) {
    const auto auditId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
    ruvia::db_query insert(c.pool());
    const std::array<std::string_view, 7> insertColumns{ service::device::entities::SecurityAuditLogEntity::column_name<"id">(), service::device::entities::SecurityAuditLogEntity::column_name<"actor_user_id">(), service::device::entities::SecurityAuditLogEntity::column_name<"action">(), service::device::entities::SecurityAuditLogEntity::column_name<"resource_type">(), service::device::entities::SecurityAuditLogEntity::column_name<"resource_id">(), service::device::entities::SecurityAuditLogEntity::column_name<"outcome">(), service::device::entities::SecurityAuditLogEntity::column_name<"details">() };
    auto insertBuilder = c.db().template repository<service::device::entities::SecurityAuditLogEntity>().insert_builder();
    ruvia::db_values_builder insertValues(insertColumns);
    insertValues.row({ insert.cast(insert.value(auditId), ruvia::db_data_type::uuid), insert.cast(insert.value(actor), ruvia::db_data_type::uuid), insert.value(action), insert.value(resource), insert.cast(insert.null_if(insert.cast(insert.value(resourceId), ruvia::db_data_type::text), insert.cast(insert.value(""), ruvia::db_data_type::text)), ruvia::db_data_type::uuid), insert.value(outcome), insert.cast(insert.value(details), ruvia::db_data_type::jsonb) });
    insertBuilder.insert_from(insertColumns, insertValues.query());
    (void)co_await insertBuilder.execute();
}

template <typename Context>
ruvia::task<std::string> VpnService::control(Context& c, std::string_view operation, std::string payload) {
    co_return co_await service::utils::callRpc(c, service::message::vpn::kRpcComponent, operation, std::move(payload));
}

template <typename Context>
ruvia::task<void> VpnService::reconcileHub(Context& c) {
    (void)co_await control(c, service::message::vpn::kReconcileOperation, std::string{});
}

} // namespace service::vpn
