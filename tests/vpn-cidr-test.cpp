#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>

#ifdef __linux__
#include <net/if.h>
#include <unistd.h>
#endif

#include "service/common/message.h"
#include "service/features/vpn/firewall/firewall.transport.h"
#include "service/features/vpn/vpn.service.h"
#include "service/features/vpn/wireguard/wireguard.transport.h"
#include "service/modules/vpn/vpn.types.h"
#include "service/utils/network.h"

namespace {

void require(bool condition, std::string_view message) {
    if (condition) {
        return;
    }
    std::cerr << "vpn test failed: " << message << '\n';
    std::exit(1);
}

} // namespace

int main() {
    const auto overlay = service::utils::network::parseCidr("100.96.0.0/24", 16, 30);
    const auto virtualLan = service::utils::network::parseCidr("172.31.1.0/24", 1, 30);
    const auto realLan = service::utils::network::parseCidr("192.168.1.0/24", 1, 30);
    require(overlay && overlay->text() == "100.96.0.0/24", "overlay CIDR parsing");
    require(service::message::vpn::kVirtualLanPool.text() == "172.16.0.0/12", "virtual LAN pool size");
    require(virtualLan && service::message::vpn::kVirtualLanPool.contains(virtualLan->network), "virtual LAN pool membership");
    require(realLan && service::utils::network::isPrivateIpv4(*realLan), "private LAN detection");
    const auto mapped = realLan ? service::message::vpn::mappedVirtualCidr(*realLan) : std::nullopt;
    require(mapped && mapped->text() == "172.24.1.0/24", "automatic network mapping");
    require(mapped && service::utils::network::hostAddress(*mapped, 50).value() == 0xac180132U && service::utils::network::hostAddress(*realLan, 50).value() == 0xc0a80132U, "host offset is preserved");
    const auto smallerReal = service::utils::network::parseCidr("192.168.1.128/25", 1, 30);
    const auto smallerMapped = smallerReal ? service::message::vpn::mappedVirtualCidr(*smallerReal)
                                           : std::nullopt;
    require(smallerMapped && smallerMapped->text() == "172.24.1.128/25", "automatic mapping preserves subnet bits");
    const auto expandedReal = service::utils::network::parseCidr("192.168.0.0/16", 1, 30);
    const auto expandedMapped = expandedReal ? service::message::vpn::mappedVirtualCidr(*expandedReal)
                                             : std::nullopt;
    require(expandedMapped && expandedMapped->text() == "172.24.0.0/16", "automatic mapping follows an expanded bridge prefix");
    const auto outsideVirtualPool = service::utils::network::parseCidr("172.168.1.0/24", 1, 30);
    require(outsideVirtualPool && !service::message::vpn::kVirtualLanPool.contains(outsideVirtualPool->network), "public 172/8 space is excluded from the virtual LAN pool");
    require(overlay && service::utils::network::parseCidr("100.96.0.1/24", 16, 30) == std::nullopt, "non-canonical CIDR rejection");
    require(virtualLan && realLan && virtualLan->prefix == realLan->prefix, "equal-prefix route contract");
    require(virtualLan && service::utils::network::hostAddress(*virtualLan, 2).value() == 0xac1f0102U, "host address allocation");
    const auto moduleReal = service::utils::network::parseCidr("192.168.1.0/24", 1, 30);
    const auto moduleVirtual = service::utils::network::parseCidr("172.31.1.0/24", 1, 30);
    const auto otherVirtual = service::utils::network::parseCidr("172.31.2.0/24", 1, 30);
    require(moduleReal && !service::vpn::VpnEdgeService::realTargetConflictsVirtual(*moduleReal, std::nullopt), "duplicate real LANs remain independent across Edge peers");
    require(moduleVirtual && moduleReal && service::vpn::VpnEdgeService::virtualMappingConflicts(*moduleVirtual, moduleReal, moduleVirtual) && !service::vpn::VpnEdgeService::virtualMappingConflicts(*moduleVirtual, moduleReal, otherVirtual), "virtual mapping conflicts are checked independently");

    namespace wg = service::common::wireguard;
    constexpr std::string_view key = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopo=";
    require(wg::validKey(key) && wg::validManagedKey(key), "canonical WireGuard key accepted");
    const auto decoded = wg::decodeKey(key);
    require(decoded && wg::encodeKey(*decoded) == key, "WireGuard key round trip");
    require(!wg::validKey("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq="), "non-zero padding bits rejected");
    require(!wg::validKey("invalid") && !wg::validKey(std::string(44, '=')), "malformed key rejected");
    require(!wg::validKey("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij=lmnopo="), "inner padding rejected");
    const std::string zero = std::string(43, 'A') + "=";
    require(wg::validKey(zero) && !wg::validManagedKey(zero), "all-zero key is format-valid but not managed");
    wg::Key bytes{};
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::uint8_t>(index * 7 + 1);
    }
    require(wg::decodeKey(wg::encodeKey(bytes)) == bytes, "arbitrary key bytes round trip");

    std::string privateKey;
    std::string publicKey;
    require(service::vpn::VpnHubKeyService::generateKeyPair(privateKey, publicKey) && wg::validManagedKey(privateKey), "Hub key pair generation");
    std::string derivedPublicKey;
    require(service::vpn::VpnHubKeyService::derivePublicKey(privateKey, derivedPublicKey) && derivedPublicKey == publicKey, "Hub public key derivation");

#ifdef __linux__
    const service::vpn::wireguard::HubConfig absentHub{
        .interfaceName = "iot-rm-" + std::to_string(::getpid()),
        .privateKey = privateKey,
        .publicKey = publicKey,
    };
    require(::if_nametoindex(absentHub.interfaceName.c_str()) == 0, "VPN regression interface must not exist");
    auto& controller = service::vpn::wireguard::controller();
    require(controller.removePeer(absentHub, publicKey).configured, "removing a peer from an absent interface succeeds");
    require(controller.removePeer(absentHub, publicKey).configured, "repeated absent-interface peer removal is idempotent");
    const auto invalidRemoval = controller.removePeer(absentHub, "invalid");
    require(!invalidRemoval.configured && invalidRemoval.code == "invalid_peer", "absent-interface cleanup retains key validation");
    if (std::system("command -v nft >/dev/null 2>&1") == 0) {
        require(service::vpn::firewall::runNft({ "nft", "--version" }), "VPN firewall cannot launch the installed nft executable");
    }
#endif
    std::string firewallScript;
    const auto firewallResult = service::vpn::firewall::render(
        "wg0",
        { { .assignedIpv4 = "100.96.0.2",
            .sourceRoutes = { "172.24.1.0/24" },
            .allowedRoutes = { "172.24.1.0/24", "172.24.2.0/24" } },
          { .assignedIpv4 = "100.96.0.3",
            .allowedRoutes = { "172.24.1.0/24" },
            .edgeAddresses = { "100.96.0.2" } } },
        firewallScript
    );
    require(firewallResult.configured, "site-to-site firewall rendering");
    require(firewallScript.find("ip saddr { 100.96.0.2/32, 172.24.1.0/24 } ip daddr { "
                                "172.24.1.0/24, 172.24.2.0/24 } accept") != std::string::npos,
            "Edge overlay and virtual LAN sources can reach same-network virtual routes");
    require(firewallScript.find("ip saddr 100.96.0.3/32 ip daddr { 172.24.1.0/24, "
                                "100.96.0.2/32 } accept") != std::string::npos,
            "Windows clients retain virtual route and Edge address access");
    require(firewallScript.find("ip saddr { 100.96.0.0/11, 172.16.0.0/12 } drop") != std::string::npos, "unapproved overlay and virtual LAN forwarding is denied");
    require(firewallScript.find("ct state established,related ip daddr") != std::string::npos, "permitted reverse established traffic is accepted");
    require(firewallScript.find("ct state established,related ip daddr 100.96.0.3/32 ip saddr { 172.24.1.0/24, 100.96.0.2/32 } accept") != std::string::npos, "reverse traffic is narrowed to the selected route");
    require(firewallScript.find("iifname \"wg0\" oifname \"wg0\" ct state established,related accept\n") == std::string::npos, "global established traffic acceptance is absent");
    require(service::vpn::firewall::render("wg0", { service::vpn::firewall::ClientAccess{ .assignedIpv4 = "100.96.0.3" } }, firewallScript).configured, "empty client selection renders");
    require(firewallScript.find(" accept\n") == std::string::npos && firewallScript.find("ct state established,related") == std::string::npos, "clearing a selection removes both forward and established reverse grants");
    return 0;
}
