#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <ruvia/web/validation.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/utils/network.h"

namespace service::vpn {

inline constexpr auto kDefaultPage = RUVIA_DEFAULT(1){};
inline constexpr auto kDefaultPageSize = RUVIA_DEFAULT(20){};

namespace module {
using service::message::vpn::kVirtualLanPool;
using service::message::vpn::mappedVirtualCidr;
using service::utils::network::hostAddress;
using service::utils::network::Ipv4Cidr;
using service::utils::network::isPrivateIpv4;
using service::utils::network::networkCidr;
using service::utils::network::parseCidr;
using service::utils::network::parseIpv4;

inline bool virtualMappingConflicts(const Ipv4Cidr& candidate, const std::optional<Ipv4Cidr>& realTarget, const std::optional<Ipv4Cidr>& existingVirtual) {
    return (realTarget && candidate.overlaps(*realTarget)) || (existingVirtual && candidate.overlaps(*existingVirtual));
}
inline bool realTargetConflictsVirtual(const Ipv4Cidr& candidate, const std::optional<Ipv4Cidr>& existingVirtual) {
    return existingVirtual && candidate.overlaps(*existingVirtual);
}
} // namespace module

inline bool validVpnNodeSelection(const ruvia::array<ruvia::string>& values) {
    std::unordered_set<std::string> unique;
    for (const auto& value : values) {
        if (!service::common::isUuid(value.view())) return false;
        std::string id(value.view());
        std::transform(id.begin(), id.end(), id.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (!unique.insert(std::move(id)).second) return false;
    }
    return true;
}

RUVIA_MODEL(VpnIdParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("VPN ID 必须是 UUID", service::common::isUuidField)));
RUVIA_MODEL(VpnListQuery, RUVIA_OPTIONAL_FIELD(page, ruvia::int64, decltype(kDefaultPage), RUVIA_MIN(1, "page 必须大于 0")), RUVIA_OPTIONAL_FIELD_NAME("pageSize", pageSize, ruvia::int64, decltype(kDefaultPageSize), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD(keyword, ruvia::string), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("VPN 状态无效", "enabled", "disabled")));
RUVIA_MODEL(VpnRoutePatchBody,
    RUVIA_OPTIONAL_FIELD(targetCidr, ruvia::string, RUVIA_MAX(18, "targetCidr 长度超出限制")),
    RUVIA_OPTIONAL_FIELD(lanInterface, ruvia::string, RUVIA_MAX(64, "lanInterface 长度超出限制")),
    RUVIA_OPTIONAL_FIELD(mode, ruvia::string, RUVIA_MAX(32, "mode 长度超出限制")),
    RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(virtualCidr, ruvia::string, RUVIA_MAX(18, "virtualCidr 长度超出限制")));
RUVIA_MODEL(VpnPeerBody,
    RUVIA_OPTIONAL_FIELD(networkId, ruvia::string, RUVIA_CUSTOM("networkId 必须是 UUID", service::common::isUuidField)),
    RUVIA_REQUIRED_FIELD(peerType, ruvia::string, RUVIA_MIN(1, "peerType 不能为空"), RUVIA_MAX(16, "peerType 长度超出限制")),
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "name 不能为空"), RUVIA_MAX(100, "name 长度超出限制")),
    RUVIA_OPTIONAL_FIELD(publicKey, ruvia::string, RUVIA_MAX(64, "publicKey 长度超出限制")),
    RUVIA_OPTIONAL_FIELD(edgeNodeId, ruvia::string, RUVIA_CUSTOM("edgeNodeId 必须是 UUID", service::common::isUuidField)),
    RUVIA_OPTIONAL_FIELD(allowedRoutes, ruvia::array<ruvia::string>, RUVIA_MAX(10000, "allowedRoutes 数量超出限制")));
RUVIA_MODEL(VpnDesktopPeerBody,
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "name 不能为空"), RUVIA_MAX(100, "name 长度超出限制")),
    RUVIA_REQUIRED_FIELD(publicKey, ruvia::string, RUVIA_MIN(1, "publicKey 不能为空"), RUVIA_MAX(64, "publicKey 长度超出限制")),
    RUVIA_REQUIRED_FIELD(edgeNodeIds, ruvia::array<ruvia::string>, RUVIA_MAX(64, "最多选择 64 个节点"), RUVIA_CUSTOM("Edge 节点 ID 必须为不重复的 UUID", validVpnNodeSelection)));
RUVIA_MODEL(VpnDesktopSelectionBody,
    RUVIA_REQUIRED_FIELD(edgeNodeIds, ruvia::array<ruvia::string>, RUVIA_MAX(64, "最多选择 64 个节点"), RUVIA_CUSTOM("Edge 节点 ID 必须为不重复的 UUID", validVpnNodeSelection)));

inline std::optional<std::string> optionalVpnText(const std::optional<ruvia::string>& value) {
    return value ? std::optional<std::string>(value->view()) : std::nullopt;
}
inline std::vector<std::string> normalizeVpnNodeIds(const ruvia::array<ruvia::string>& values) {
    std::vector<std::string> result;
    result.reserve(values.size());
    for (const auto& value : values) {
        auto& id = result.emplace_back(value.view());
        std::transform(id.begin(), id.end(), id.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    }
    std::sort(result.begin(), result.end());
    return result;
}
} // namespace service::vpn
