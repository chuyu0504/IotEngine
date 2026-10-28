#pragma once

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef __linux__
#include <linux/genetlink.h>
#include <linux/if_addr.h>
#include <linux/if_link.h>
#include <linux/netlink.h>
#include <linux/rtnetlink.h>
#if __has_include(<linux/wireguard.h>)
#include <linux/wireguard.h>
#endif
#include <net/if.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "service/common/wireguard.h"
#include "service/features/vpn/wireguard/wireguard.config.h"
#include "service/features/vpn/wireguard/wireguard.types.h"
#include "service/utils/network.h"

namespace service::vpn::wireguard {

using service::utils::network::Ipv4Cidr;
using service::utils::network::parseCidr;
using service::utils::network::parseIpv4;

class IWireGuardController {
  public:
    virtual ~IWireGuardController() = default;
    virtual RuntimeStatus configure(const HubConfig& config) = 0;
    virtual RuntimeStatus upsertPeer(const HubConfig& config, const Peer& peer) = 0;
    virtual RuntimeStatus removePeer(const HubConfig& config, std::string_view publicKey) = 0;
    virtual RuntimeStatus reconcileRoutes(const HubConfig& config, const std::vector<std::string>& expectedRoutes) = 0;
    virtual std::optional<std::vector<std::string>> peerKeys(const HubConfig& config) = 0;
    virtual std::optional<std::unordered_map<std::string, std::uint64_t>> peerHandshakes(
        const HubConfig& config
    ) = 0;
};

class UnsupportedController final : public IWireGuardController {
  public:
    RuntimeStatus configure(const HubConfig&) override { return unsupported(); }

    RuntimeStatus upsertPeer(const HubConfig&, const Peer&) override { return unsupported(); }

    RuntimeStatus removePeer(const HubConfig&, std::string_view) override { return unsupported(); }

    RuntimeStatus reconcileRoutes(const HubConfig&, const std::vector<std::string>&) override {
        return unsupported();
    }

    std::optional<std::vector<std::string>> peerKeys(const HubConfig&) override { return std::nullopt; }

    std::optional<std::unordered_map<std::string, std::uint64_t>>
    peerHandshakes(const HubConfig&) override {
        return std::nullopt;
    }

  private:
    static RuntimeStatus unsupported() {
        return { .supported = false,
                 .configured = false,
                 .code = "unsupported_platform",
                 .message = "WireGuard hub is only supported on Linux",
                 .peerCount = 0 };
    }
};

#ifdef __linux__

namespace linux_detail {

inline constexpr std::string_view kWireGuardFamily = "wireguard";
inline constexpr std::uint8_t kWireGuardVersion = 1;

enum : std::uint8_t {
    kWgCmdGetDevice = 0,
    kWgCmdSetDevice = 1,
};

enum : std::uint16_t {
    kWgDeviceIfname = 2,
    kWgDevicePrivateKey = 3,
    kWgDeviceListenPort = 6,
    kWgDevicePeers = 8,
};

enum : std::uint16_t {
    kWgPeerPublicKey = 1,
    kWgPeerFlags = 3,
    kWgPeerLastHandshake = 6,
    kWgPeerAllowedIps = 9,
};

enum : std::uint16_t {
    kWgAllowedIpFamily = 1,
    kWgAllowedIpAddress = 2,
    kWgAllowedIpPrefix = 3,
};

inline constexpr std::uint32_t kWgPeerRemove = 1U << 0;
inline constexpr std::uint32_t kWgPeerReplaceAllowedIps = 1U << 1;

#if __has_include(<linux/wireguard.h>)
static_assert(static_cast<int>(kWgCmdGetDevice) == static_cast<int>(WG_CMD_GET_DEVICE));
static_assert(static_cast<int>(kWgCmdSetDevice) == static_cast<int>(WG_CMD_SET_DEVICE));
static_assert(static_cast<int>(kWgDeviceIfname) == static_cast<int>(WGDEVICE_A_IFNAME));
static_assert(static_cast<int>(kWgDevicePrivateKey) == static_cast<int>(WGDEVICE_A_PRIVATE_KEY));
static_assert(static_cast<int>(kWgDeviceListenPort) == static_cast<int>(WGDEVICE_A_LISTEN_PORT));
static_assert(static_cast<int>(kWgDevicePeers) == static_cast<int>(WGDEVICE_A_PEERS));
static_assert(static_cast<int>(kWgPeerPublicKey) == static_cast<int>(WGPEER_A_PUBLIC_KEY));
static_assert(static_cast<int>(kWgPeerFlags) == static_cast<int>(WGPEER_A_FLAGS));
static_assert(static_cast<int>(kWgPeerLastHandshake) == static_cast<int>(WGPEER_A_LAST_HANDSHAKE_TIME));
static_assert(static_cast<int>(kWgPeerAllowedIps) == static_cast<int>(WGPEER_A_ALLOWEDIPS));
static_assert(static_cast<int>(kWgAllowedIpFamily) == static_cast<int>(WGALLOWEDIP_A_FAMILY));
static_assert(static_cast<int>(kWgAllowedIpAddress) == static_cast<int>(WGALLOWEDIP_A_IPADDR));
static_assert(static_cast<int>(kWgAllowedIpPrefix) == static_cast<int>(WGALLOWEDIP_A_CIDR_MASK));
static_assert(kWgPeerRemove == WGPEER_F_REMOVE_ME);
static_assert(kWgPeerReplaceAllowedIps == WGPEER_F_REPLACE_ALLOWEDIPS);
#endif

inline constexpr std::uint16_t kNested = NLA_F_NESTED;

inline std::size_t align4(std::size_t value) noexcept {
    return (value + 3U) & ~std::size_t{ 3U };
}

class MessageBuilder final {
  public:
    MessageBuilder(std::uint16_t type, std::uint16_t flags, std::uint32_t sequence) {
        nlmsghdr header{};
        header.nlmsg_len = NLMSG_HDRLEN;
        header.nlmsg_type = type;
        header.nlmsg_flags = flags;
        header.nlmsg_seq = sequence;
        append(header);
    }

    void append(const void* value, std::size_t length) {
        const auto* bytes = static_cast<const std::uint8_t*>(value);
        data_.insert(data_.end(), bytes, bytes + length);
    }

    template <typename T>
    void append(const T& value) {
        append(&value, sizeof(value));
    }

    void attribute(std::uint16_t type, const void* value, std::size_t length) {
        nla::Header header{ static_cast<std::uint16_t>(NLA_HDRLEN + length), type };
        append(header);
        if (length != 0) {
            append(value, length);
        }
        data_.resize(align4(data_.size()), 0);
    }

    template <typename T>
    void attribute(std::uint16_t type, const T& value) {
        attribute(type, &value, sizeof(value));
    }

    void stringAttribute(std::uint16_t type, std::string_view value) {
        nla::Header header{ static_cast<std::uint16_t>(NLA_HDRLEN + value.size() + 1U), type };
        append(header);
        append(value.data(), value.size());
        const char terminator = '\0';
        append(&terminator, sizeof(terminator));
        data_.resize(align4(data_.size()), 0);
    }

    std::size_t beginNested(std::uint16_t type) {
        const auto offset = data_.size();
        nla::Header header{ NLA_HDRLEN, static_cast<std::uint16_t>(type | kNested) };
        append(header);
        return offset;
    }

    void endNested(std::size_t offset) {
        auto* header = reinterpret_cast<nla::Header*>(data_.data() + offset);
        header->length = static_cast<std::uint16_t>(data_.size() - offset);
    }

    [[nodiscard]] std::vector<std::uint8_t> finish() {
        auto* header = reinterpret_cast<nlmsghdr*>(data_.data());
        header->nlmsg_len = static_cast<std::uint32_t>(data_.size());
        return std::move(data_);
    }

  private:
    struct nla {
        struct Header {
            std::uint16_t length;
            std::uint16_t type;
        };
    };

    std::vector<std::uint8_t> data_;
};

struct Reply final {
    bool ok{};
    int errorCode{};
    std::vector<std::vector<std::uint8_t>> messages;
};

class NetlinkClient final {
  public:
    explicit NetlinkClient(int protocol) : fd_(::socket(AF_NETLINK, SOCK_RAW, protocol)) {
        if (fd_ < 0) {
            return;
        }
        sockaddr_nl address{};
        address.nl_family = AF_NETLINK;
        if (::bind(fd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    ~NetlinkClient() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    NetlinkClient(const NetlinkClient&) = delete;
    NetlinkClient& operator=(const NetlinkClient&) = delete;

    [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

    Reply request(const std::vector<std::uint8_t>& message, bool dump = false) const {
        if (!valid()) {
            return { .ok = false, .errorCode = errno != 0 ? errno : ENOTSOCK };
        }

        sockaddr_nl destination{};
        destination.nl_family = AF_NETLINK;
        iovec vector{ const_cast<std::uint8_t*>(message.data()), message.size() };
        msghdr outgoing{};
        outgoing.msg_name = &destination;
        outgoing.msg_namelen = sizeof(destination);
        outgoing.msg_iov = &vector;
        outgoing.msg_iovlen = 1;
        if (::sendmsg(fd_, &outgoing, 0) < 0) {
            return { .ok = false, .errorCode = errno };
        }

        std::array<std::uint8_t, 65536> buffer{};
        Reply reply{ .ok = true };
        for (;;) {
            const auto length = ::recv(fd_, buffer.data(), buffer.size(), 0);
            if (length < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return { .ok = false, .errorCode = errno };
            }
            if (length == 0) {
                return { .ok = false, .errorCode = EIO };
            }

            std::size_t offset = 0;
            bool completed = false;
            while (offset + NLMSG_HDRLEN <= static_cast<std::size_t>(length)) {
                const auto* header = reinterpret_cast<const nlmsghdr*>(buffer.data() + offset);
                if (header->nlmsg_len < NLMSG_HDRLEN ||
                    offset + header->nlmsg_len > static_cast<std::size_t>(length)) {
                    return { .ok = false, .errorCode = EPROTO };
                }
                if (header->nlmsg_seq != reinterpret_cast<const nlmsghdr*>(message.data())->nlmsg_seq) {
                    offset += NLMSG_ALIGN(header->nlmsg_len);
                    continue;
                }
                if (header->nlmsg_type == NLMSG_ERROR) {
                    if (header->nlmsg_len < NLMSG_LENGTH(sizeof(nlmsgerr))) {
                        return { .ok = false, .errorCode = EPROTO };
                    }
                    const auto* error = reinterpret_cast<const nlmsgerr*>(NLMSG_DATA(header));
                    if (error->error != 0) {
                        return { .ok = false, .errorCode = -error->error };
                    }
                    completed = !dump;
                } else if (header->nlmsg_type == NLMSG_DONE) {
                    completed = true;
                } else {
                    reply.messages.emplace_back(buffer.begin() + static_cast<std::ptrdiff_t>(offset), buffer.begin() + static_cast<std::ptrdiff_t>(offset + header->nlmsg_len));
                }
                offset += NLMSG_ALIGN(header->nlmsg_len);
            }
            if (completed) {
                return reply;
            }
        }
    }

    [[nodiscard]] std::optional<std::uint16_t> resolveFamily(std::string_view name) const {
        MessageBuilder request(GENL_ID_CTRL, NLM_F_REQUEST | NLM_F_ACK, 1);
        genlmsghdr generic{ CTRL_CMD_GETFAMILY, 1, 0 };
        request.append(generic);
        request.stringAttribute(CTRL_ATTR_FAMILY_NAME, name);
        const auto reply = requestMessage(request.finish());
        if (!reply.ok) {
            return std::nullopt;
        }
        for (const auto& message : reply.messages) {
            const auto* header = reinterpret_cast<const nlmsghdr*>(message.data());
            const auto* genericHeader = reinterpret_cast<const genlmsghdr*>(NLMSG_DATA(header));
            const auto payload = reinterpret_cast<const std::uint8_t*>(genericHeader) + GENL_HDRLEN;
            const auto payloadLength = header->nlmsg_len - NLMSG_HDRLEN - GENL_HDRLEN;
            std::optional<std::uint16_t> family;
            forEachAttribute(payload, payloadLength, [&](const nlattr& attribute, const std::uint8_t* value, std::size_t length) {
                if ((attribute.nla_type & NLA_TYPE_MASK) == CTRL_ATTR_FAMILY_ID && length >= sizeof(std::uint16_t)) {
                    std::uint16_t identifier = 0;
                    std::memcpy(&identifier, value, sizeof(identifier));
                    family = identifier;
                }
            });
            if (family) {
                return family;
            }
        }
        return std::nullopt;
    }

    Reply requestMessage(std::vector<std::uint8_t> message, bool dump = false) const {
        return request(message, dump);
    }

  public:
    struct nlattr {
        std::uint16_t nla_len;
        std::uint16_t nla_type;
    };

  public:
    template <typename Fn>
    static void forEachAttribute(const std::uint8_t* data, std::size_t length, Fn&& callback) {
        std::size_t offset = 0;
        while (offset + NLA_HDRLEN <= length) {
            const auto* attribute = reinterpret_cast<const nlattr*>(data + offset);
            if (attribute->nla_len < NLA_HDRLEN || offset + attribute->nla_len > length) {
                return;
            }
            callback(*attribute, data + offset + NLA_HDRLEN, attribute->nla_len - NLA_HDRLEN);
            offset += align4(attribute->nla_len);
        }
    }

  private:
    int fd_{ -1 };
};

inline std::string errorText(int errorCode) {
    return std::strerror(errorCode > 0 ? errorCode : EIO);
}

inline bool addWireGuardInterface(std::string_view interfaceName, int& errorCode) {
    NetlinkClient client(NETLINK_ROUTE);
    if (!client.valid()) {
        errorCode = errno;
        return false;
    }
    MessageBuilder request(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL, 1);
    ifinfomsg link{};
    link.ifi_family = AF_UNSPEC;
    request.append(link);
    request.stringAttribute(IFLA_IFNAME, interfaceName);
    const auto linkInfo = request.beginNested(IFLA_LINKINFO);
    request.stringAttribute(IFLA_INFO_KIND, kWireGuardFamily);
    request.endNested(linkInfo);
    const auto reply = client.request(request.finish());
    errorCode = reply.errorCode;
    return reply.ok;
}

inline bool replaceAddress(std::string_view interfaceName, std::string_view address, int& errorCode) {
    const auto cidr = parseCidr(address, 1, 32);
    const auto ifindex = ::if_nametoindex(std::string(interfaceName).c_str());
    if (!cidr || ifindex == 0) {
        errorCode = ifindex == 0 ? (errno != 0 ? errno : ENODEV) : EINVAL;
        return false;
    }
    NetlinkClient client(NETLINK_ROUTE);
    if (!client.valid()) {
        errorCode = errno;
        return false;
    }
    MessageBuilder request(RTM_NEWADDR, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_REPLACE, 1);
    ifaddrmsg addressMessage{};
    addressMessage.ifa_family = AF_INET;
    addressMessage.ifa_prefixlen = cidr->prefix;
    addressMessage.ifa_scope = RT_SCOPE_UNIVERSE;
    addressMessage.ifa_index = ifindex;
    const auto networkAddress = htonl(cidr->network);
    request.append(addressMessage);
    request.attribute(IFA_LOCAL, networkAddress);
    request.attribute(IFA_ADDRESS, networkAddress);
    const auto reply = client.request(request.finish());
    errorCode = reply.errorCode;
    return reply.ok;
}

inline bool setInterfaceUp(std::string_view interfaceName, int& errorCode) {
    const auto ifindex = ::if_nametoindex(std::string(interfaceName).c_str());
    if (ifindex == 0) {
        errorCode = errno != 0 ? errno : ENODEV;
        return false;
    }
    NetlinkClient client(NETLINK_ROUTE);
    if (!client.valid()) {
        errorCode = errno;
        return false;
    }
    MessageBuilder request(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK, 1);
    ifinfomsg link{};
    link.ifi_family = AF_UNSPEC;
    link.ifi_index = static_cast<int>(ifindex);
    link.ifi_flags = IFF_UP;
    link.ifi_change = IFF_UP;
    request.append(link);
    const auto reply = client.request(request.finish());
    errorCode = reply.errorCode;
    return reply.ok;
}

inline bool replaceRoute(std::string_view interfaceName, std::string_view destination, int& errorCode) {
    const auto cidr = parseCidr(destination, 0, 32);
    const auto ifindex = ::if_nametoindex(std::string(interfaceName).c_str());
    if (!cidr || ifindex == 0) {
        errorCode = ifindex == 0 ? (errno != 0 ? errno : ENODEV) : EINVAL;
        return false;
    }
    NetlinkClient client(NETLINK_ROUTE);
    if (!client.valid()) {
        errorCode = errno;
        return false;
    }
    MessageBuilder request(RTM_NEWROUTE, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_REPLACE, 1);
    rtmsg route{};
    route.rtm_family = AF_INET;
    route.rtm_dst_len = cidr->prefix;
    route.rtm_table = RT_TABLE_MAIN;
    route.rtm_protocol = RTPROT_STATIC;
    route.rtm_scope = RT_SCOPE_LINK;
    route.rtm_type = RTN_UNICAST;
    request.append(route);
    if (cidr->prefix != 0) {
        const auto destinationAddress = htonl(cidr->network);
        request.attribute(RTA_DST, destinationAddress);
    }
    request.attribute(RTA_OIF, ifindex);
    const auto reply = client.request(request.finish());
    errorCode = reply.errorCode;
    return reply.ok;
}

inline std::optional<std::vector<Ipv4Cidr>> managedRoutes(
    std::string_view interfaceName,
    int& errorCode
) {
    const auto ifindex = ::if_nametoindex(std::string(interfaceName).c_str());
    if (ifindex == 0) {
        errorCode = errno != 0 ? errno : ENODEV;
        return std::nullopt;
    }
    NetlinkClient client(NETLINK_ROUTE);
    if (!client.valid()) {
        errorCode = errno;
        return std::nullopt;
    }
    MessageBuilder request(RTM_GETROUTE, NLM_F_REQUEST | NLM_F_DUMP, 1);
    rtmsg filter{};
    filter.rtm_family = AF_INET;
    request.append(filter);
    const auto reply = client.request(request.finish(), true);
    errorCode = reply.errorCode;
    if (!reply.ok) {
        return std::nullopt;
    }

    std::vector<Ipv4Cidr> routes;
    for (const auto& message : reply.messages) {
        const auto* header = reinterpret_cast<const nlmsghdr*>(message.data());
        if (header->nlmsg_type != RTM_NEWROUTE ||
            header->nlmsg_len < NLMSG_LENGTH(sizeof(rtmsg))) {
            continue;
        }
        const auto* route = reinterpret_cast<const rtmsg*>(NLMSG_DATA(header));
        if (route->rtm_family != AF_INET || route->rtm_table != RT_TABLE_MAIN ||
            route->rtm_protocol != RTPROT_STATIC || route->rtm_type != RTN_UNICAST ||
            route->rtm_dst_len > 32) {
            continue;
        }
        std::optional<std::uint32_t> outputInterface;
        std::uint32_t destination = 0;
        const auto* payload = reinterpret_cast<const std::uint8_t*>(route) + sizeof(rtmsg);
        const auto payloadLength = header->nlmsg_len - NLMSG_HDRLEN - sizeof(rtmsg);
        NetlinkClient::forEachAttribute(
            payload,
            payloadLength,
            [&](const NetlinkClient::nlattr& attribute, const std::uint8_t* value, std::size_t length) {
                const auto type = attribute.nla_type & NLA_TYPE_MASK;
                if (type == RTA_OIF && length >= sizeof(std::uint32_t)) {
                    std::uint32_t index = 0;
                    std::memcpy(&index, value, sizeof(index));
                    outputInterface = index;
                } else if (type == RTA_DST && length >= sizeof(std::uint32_t)) {
                    std::uint32_t address = 0;
                    std::memcpy(&address, value, sizeof(address));
                    destination = ntohl(address);
                }
            }
        );
        if (outputInterface && *outputInterface == ifindex) {
            routes.push_back(Ipv4Cidr{ destination, route->rtm_dst_len });
        }
    }
    return routes;
}

inline bool deleteRoute(std::string_view interfaceName, const Ipv4Cidr& destination, int& errorCode) {
    const auto ifindex = ::if_nametoindex(std::string(interfaceName).c_str());
    if (ifindex == 0) {
        errorCode = errno != 0 ? errno : ENODEV;
        return false;
    }
    NetlinkClient client(NETLINK_ROUTE);
    if (!client.valid()) {
        errorCode = errno;
        return false;
    }
    MessageBuilder request(RTM_DELROUTE, NLM_F_REQUEST | NLM_F_ACK, 1);
    rtmsg route{};
    route.rtm_family = AF_INET;
    route.rtm_dst_len = destination.prefix;
    route.rtm_table = RT_TABLE_MAIN;
    route.rtm_protocol = RTPROT_STATIC;
    route.rtm_scope = RT_SCOPE_LINK;
    route.rtm_type = RTN_UNICAST;
    request.append(route);
    if (destination.prefix != 0) {
        const auto destinationAddress = htonl(destination.network);
        request.attribute(RTA_DST, destinationAddress);
    }
    request.attribute(RTA_OIF, ifindex);
    const auto reply = client.request(request.finish());
    errorCode = reply.errorCode;
    return reply.ok;
}

struct Device final {
    std::vector<std::string> peerKeys;
    std::unordered_map<std::string, std::uint64_t> handshakes;
};

inline void parsePeer(const std::uint8_t* data, std::size_t length, Device& device);

inline std::optional<Device> getDevice(const NetlinkClient& client, std::uint16_t family, std::string_view interfaceName, int& errorCode) {
    MessageBuilder request(family, NLM_F_REQUEST | NLM_F_ACK | NLM_F_DUMP, 1);
    genlmsghdr generic{ kWgCmdGetDevice, kWireGuardVersion, 0 };
    request.append(generic);
    request.stringAttribute(kWgDeviceIfname, interfaceName);
    const auto reply = client.request(request.finish(), true);
    errorCode = reply.errorCode;
    if (!reply.ok) {
        return std::nullopt;
    }

    Device result;
    for (const auto& message : reply.messages) {
        const auto* header = reinterpret_cast<const nlmsghdr*>(message.data());
        if (header->nlmsg_len < NLMSG_LENGTH(GENL_HDRLEN)) {
            continue;
        }
        const auto* genericHeader = reinterpret_cast<const genlmsghdr*>(NLMSG_DATA(header));
        const auto payload = reinterpret_cast<const std::uint8_t*>(genericHeader) + GENL_HDRLEN;
        const auto payloadLength = header->nlmsg_len - NLMSG_HDRLEN - GENL_HDRLEN;
        NetlinkClient::forEachAttribute(payload, payloadLength, [&](const NetlinkClient::nlattr& attribute, const std::uint8_t* value, std::size_t length) {
            if ((attribute.nla_type & NLA_TYPE_MASK) != kWgDevicePeers) {
                return;
            }
            NetlinkClient::forEachAttribute(value, length, [&](const NetlinkClient::nlattr& peerContainer, const std::uint8_t* peerValue, std::size_t peerLength) {
                if ((peerContainer.nla_type & NLA_TYPE_MASK) == 0) {
                    parsePeer(peerValue, peerLength, result);
                }
            });
        });
    }
    return result;
}

inline void parsePeer(const std::uint8_t* data, std::size_t length, Device& device) {
    std::optional<std::string> publicKey;
    std::uint64_t handshake = 0;
    NetlinkClient::forEachAttribute(data, length, [&](const NetlinkClient::nlattr& attribute, const std::uint8_t* value, std::size_t valueLength) {
        const auto type = attribute.nla_type & NLA_TYPE_MASK;
        if (type == kWgPeerPublicKey && valueLength == service::common::wireguard::kKeySize) {
            service::common::wireguard::Key key{};
            std::memcpy(key.data(), value, key.size());
            publicKey = service::common::wireguard::encodeKey(key);
        } else if (type == kWgPeerLastHandshake && valueLength >= sizeof(std::uint64_t)) {
            std::memcpy(&handshake, value, sizeof(handshake));
        }
    });
    if (publicKey) {
        device.peerKeys.push_back(*publicKey);
        device.handshakes.emplace(std::move(*publicKey), handshake);
    }
}

inline RuntimeStatus failure(std::string code, std::string message) {
    return { .supported = true,
             .configured = false,
             .code = std::move(code),
             .message = std::move(message),
             .peerCount = 0 };
}

inline RuntimeStatus success() {
    return { .supported = true,
             .configured = true,
             .code = "ok",
             .message = "WireGuard hub is configured",
             .peerCount = 0 };
}

class Controller final : public IWireGuardController {
  public:
    RuntimeStatus configure(const HubConfig& config) override {
        if (!validConfig(config)) {
            return failure("invalid_config", "WireGuard hub configuration is invalid");
        }
        NetlinkClient generic(NETLINK_GENERIC);
        if (!generic.valid()) {
            return failure("netlink_unavailable", errorText(errno));
        }
        const auto family = generic.resolveFamily(kWireGuardFamily);
        if (!family) {
            return failure("wireguard_family_unavailable", "WireGuard Generic Netlink family is unavailable");
        }

        if (::if_nametoindex(config.interfaceName.c_str()) == 0) {
            int errorCode = 0;
            if (!addWireGuardInterface(config.interfaceName, errorCode) && errorCode != EEXIST) {
                return failure("interface_create_failed", errorText(errorCode));
            }
        }
        std::string errorMessage;
        if (!setDevice(generic, *family, config, nullptr, errorMessage)) {
            return failure("private_key_configure_failed", std::move(errorMessage));
        }
        int errorCode = 0;
        if (!replaceAddress(config.interfaceName, config.address, errorCode)) {
            return failure("address_configure_failed", errorText(errorCode));
        }
        if (!setInterfaceUp(config.interfaceName, errorCode)) {
            return failure("interface_up_failed", errorText(errorCode));
        }
        return success();
    }

    RuntimeStatus upsertPeer(const HubConfig& config, const Peer& peer) override {
        if (!validConfig(config) || !service::common::wireguard::validKey(peer.publicKey) || peer.allowedIps.empty()) {
            return failure("invalid_peer", "WireGuard peer configuration is invalid");
        }
        NetlinkClient generic(NETLINK_GENERIC);
        if (!generic.valid()) {
            return failure("netlink_unavailable", errorText(errno));
        }
        const auto family = generic.resolveFamily(kWireGuardFamily);
        if (!family) {
            return failure("wireguard_family_unavailable", "WireGuard Generic Netlink family is unavailable");
        }
        std::string errorMessage;
        if (!setDevice(generic, *family, config, &peer, errorMessage)) {
            return failure("peer_configure_failed", std::move(errorMessage));
        }
        for (const auto& route : peer.allowedIps) {
            int errorCode = 0;
            if (!replaceRoute(config.interfaceName, route, errorCode)) {
                return failure("peer_route_failed", errorText(errorCode));
            }
        }
        return success();
    }

    RuntimeStatus removePeer(const HubConfig& config, std::string_view publicKey) override {
        const auto key = service::common::wireguard::decodeKey(publicKey);
        if (!validConfig(config) || !key) {
            return failure("invalid_peer", "WireGuard peer public key is invalid");
        }
        errno = 0;
        if (::if_nametoindex(config.interfaceName.c_str()) == 0) {
            const auto errorCode = errno;
            if (errorCode == ENODEV || errorCode == ENXIO) {
                return success();
            }
            return failure("interface_lookup_failed", errorText(errorCode != 0 ? errorCode : EIO));
        }
        NetlinkClient generic(NETLINK_GENERIC);
        if (!generic.valid()) {
            return failure("netlink_unavailable", errorText(errno));
        }
        const auto family = generic.resolveFamily(kWireGuardFamily);
        if (!family) {
            return failure("wireguard_family_unavailable", "WireGuard Generic Netlink family is unavailable");
        }
        MessageBuilder request(*family, NLM_F_REQUEST | NLM_F_ACK, 1);
        genlmsghdr genericHeader{ kWgCmdSetDevice, kWireGuardVersion, 0 };
        request.append(genericHeader);
        request.stringAttribute(kWgDeviceIfname, config.interfaceName);
        const auto peers = request.beginNested(kWgDevicePeers);
        const auto peer = request.beginNested(kNested);
        request.attribute(kWgPeerPublicKey, key->data(), key->size());
        const std::uint32_t flags = kWgPeerRemove;
        request.attribute(kWgPeerFlags, flags);
        request.endNested(peer);
        request.endNested(peers);
        const auto reply = generic.request(request.finish());
        if (!reply.ok) {
            if (reply.errorCode == ENODEV || reply.errorCode == ENOENT) {
                return success();
            }
            return failure("peer_remove_failed", errorText(reply.errorCode));
        }
        return success();
    }

    RuntimeStatus reconcileRoutes(
        const HubConfig& config,
        const std::vector<std::string>& expectedRoutes
    ) override {
        if (!validConfig(config)) {
            return failure("invalid_config", "WireGuard hub configuration is invalid");
        }
        std::vector<Ipv4Cidr> expected;
        expected.reserve(expectedRoutes.size());
        for (const auto& route : expectedRoutes) {
            const auto parsed = parseCidr(route, 0, 32);
            if (!parsed) {
                return failure("invalid_route", "WireGuard peer route is invalid");
            }
            if (std::find(expected.begin(), expected.end(), *parsed) == expected.end()) {
                expected.push_back(*parsed);
            }
        }
        int errorCode = 0;
        const auto current = managedRoutes(config.interfaceName, errorCode);
        if (!current) {
            return failure("route_list_failed", errorText(errorCode));
        }
        for (const auto& route : *current) {
            if (std::find(expected.begin(), expected.end(), route) != expected.end()) {
                continue;
            }
            if (!deleteRoute(config.interfaceName, route, errorCode) && errorCode != ESRCH) {
                return failure("route_remove_failed", errorText(errorCode));
            }
        }
        return success();
    }

    std::optional<std::vector<std::string>> peerKeys(const HubConfig& config) override {
        const auto device = readDevice(config);
        if (!device) {
            return std::nullopt;
        }
        return device->peerKeys;
    }

    std::optional<std::unordered_map<std::string, std::uint64_t>>
    peerHandshakes(const HubConfig& config) override {
        const auto device = readDevice(config);
        if (!device) {
            return std::nullopt;
        }
        return device->handshakes;
    }

  private:
    bool setDevice(const NetlinkClient& client, std::uint16_t family, const HubConfig& config, const Peer* peer, std::string& errorMessage) {
        MessageBuilder request(family, NLM_F_REQUEST | NLM_F_ACK, 1);
        genlmsghdr genericHeader{ kWgCmdSetDevice, kWireGuardVersion, 0 };
        request.append(genericHeader);
        request.stringAttribute(kWgDeviceIfname, config.interfaceName);
        if (peer == nullptr) {
            const auto privateKey = service::common::wireguard::decodeKey(config.privateKey);
            if (!privateKey) {
                errorMessage = "WireGuard private key is invalid";
                return false;
            }
            request.attribute(kWgDevicePrivateKey, privateKey->data(), privateKey->size());
            request.attribute(kWgDeviceListenPort, config.listenPort);
        } else {
            const auto publicKey = service::common::wireguard::decodeKey(peer->publicKey);
            if (!publicKey) {
                errorMessage = "WireGuard peer public key is invalid";
                return false;
            }
            const auto peers = request.beginNested(kWgDevicePeers);
            const auto peerContainer = request.beginNested(kNested);
            request.attribute(kWgPeerPublicKey, publicKey->data(), publicKey->size());
            const std::uint32_t flags = kWgPeerReplaceAllowedIps;
            request.attribute(kWgPeerFlags, flags);
            const auto allowedIps = request.beginNested(kWgPeerAllowedIps);
            for (const auto& text : peer->allowedIps) {
                const auto cidr = parseCidr(text, 0, 32);
                if (!cidr) {
                    errorMessage = "WireGuard peer route is invalid";
                    return false;
                }
                const auto allowedIp = request.beginNested(kNested);
                const std::uint16_t addressFamily = AF_INET;
                const auto address = htonl(cidr->network);
                request.attribute(kWgAllowedIpFamily, addressFamily);
                request.attribute(kWgAllowedIpAddress, address);
                request.attribute(kWgAllowedIpPrefix, cidr->prefix);
                request.endNested(allowedIp);
            }
            request.endNested(allowedIps);
            request.endNested(peerContainer);
            request.endNested(peers);
        }
        const auto reply = client.request(request.finish());
        if (!reply.ok) {
            errorMessage = errorText(reply.errorCode);
            return false;
        }
        return true;
    }

    std::optional<Device> readDevice(const HubConfig& config) {
        if (!validConfig(config)) {
            return std::nullopt;
        }
        NetlinkClient generic(NETLINK_GENERIC);
        if (!generic.valid()) {
            return std::nullopt;
        }
        const auto family = generic.resolveFamily(kWireGuardFamily);
        if (!family) {
            return std::nullopt;
        }
        int errorCode = 0;
        return getDevice(generic, *family, config.interfaceName, errorCode);
    }

    static bool validConfig(const HubConfig& config) noexcept {
        return !config.interfaceName.empty() && config.interfaceName.size() <= 15 &&
            service::common::wireguard::validKey(config.privateKey) && parseCidr(config.address, 1, 32).has_value() &&
            config.listenPort != 0;
    }
};

} // namespace linux_detail

using LinuxController = linux_detail::Controller;

#endif

// The interface is a per-host kernel resource. Each Worker thread owns its own
// controller (netlink sockets are opened per call); every change is serialized
// across Workers and instances by lock::kVpnReconciliation in the runtime.
inline IWireGuardController& controller() {
#ifdef __linux__
    static thread_local LinuxController value;
#else
    static thread_local UnsupportedController value;
#endif
    return value;
}

} // namespace service::vpn::wireguard
