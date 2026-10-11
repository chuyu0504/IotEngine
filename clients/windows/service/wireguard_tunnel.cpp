#include "wireguard_tunnel.h"
#include "../common/files.h"
#include "../common/text.h"
#include "../common/win32.h"
#include <winsock2.h>
#include <ws2tcpip.h>
#include <wincrypt.h>
#include <iphlpapi.h>
#include "../vendor/wireguard.h"
#include <array>
#include <charconv>
#include <chrono>
#include <regex>
#include <set>
#include <sstream>
#include <thread>

namespace iotvpn {
namespace {
std::string field(const Json& config, const char* name) {
    if (!config.contains(name) || !config[name].is_string()) throw std::runtime_error("VPN 配置字段无效。");
    return config[name].get<std::string>();
}
std::vector<std::string> array(const Json& config, const char* name, std::size_t max) {
    if (!config.contains(name) || !config[name].is_array() || config[name].size() > max) throw std::runtime_error("VPN 配置列表无效。");
    std::vector<std::string> result;
    for (const auto& entry : config[name]) {
        if (!entry.is_string()) throw std::runtime_error("VPN 配置列表包含无效值。");
        result.push_back(entry.get<std::string>());
    }
    return result;
}
int integer(const Json& config, const char* key, int min, int max) {
    if (!config.contains(key) || !config[key].is_number_integer()) throw std::runtime_error("VPN 配置参数无效。");
    auto value = config[key].get<std::int64_t>();
    if (value < min || value > max) throw std::runtime_error("VPN 配置参数超出范围。");
    return static_cast<int>(value);
}
bool keyValid(std::string_view key) {
    if (key.size() != 44 || key.back() != '=' || key.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=") != std::string_view::npos) return false;
    std::array<BYTE, 64> bytes{}; DWORD count = static_cast<DWORD>(bytes.size());
    const bool valid = CryptStringToBinaryA(key.data(), static_cast<DWORD>(key.size()), CRYPT_STRING_BASE64 | CRYPT_STRING_STRICT, bytes.data(), &count, nullptr, nullptr) && count == 32 &&
        std::any_of(bytes.begin(), bytes.begin() + 32, [](BYTE value) { return value != 0; });
    SecureZeroMemory(bytes.data(), bytes.size()); return valid;
}
bool uuid(std::string_view text) {
    if (text.size() != 36) return false;
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) { if (text[i] != '-') return false; }
        else if (!std::isxdigit(static_cast<unsigned char>(text[i]))) return false;
    }
    return true;
}
bool ipv4(std::string_view text, std::uint32_t& value) {
    in_addr address{}; const std::string input(text);
    if (InetPtonA(AF_INET, input.c_str(), &address) != 1) return false;
    char canonical[INET_ADDRSTRLEN]{};
    if (!InetNtopA(AF_INET, &address, canonical, sizeof(canonical)) || text != canonical) return false;
    value = ntohl(address.s_addr); return true;
}
bool overlay(std::uint32_t address) { return (address & 0xffe00000U) == 0x64600000U; }
std::string endpoint(std::string_view text) {
    if (text.empty() || text.size() > 253 || std::any_of(text.begin(), text.end(), [](unsigned char c) { return c <= 32 || c >= 127; }))
        throw std::runtime_error("VPN 服务地址无效。");
    std::string host(text);
    if (host.starts_with('[') && host.ends_with(']')) host = host.substr(1, host.size() - 2);
    in6_addr ipv6{};
    if (InetPtonA(AF_INET6, host.c_str(), &ipv6) == 1) return "[" + host + "]";
    std::uint32_t v4;
    if (ipv4(host, v4)) return host;
    if (text.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.") != std::string_view::npos)
        throw std::runtime_error("VPN 服务域名无效。");
    std::size_t offset = 0;
    while (offset < host.size()) {
        auto end = host.find('.', offset); if (end == std::string::npos) end = host.size();
        const auto label = std::string_view(host).substr(offset, end - offset);
        if (label.empty() || label.size() > 63 || label.front() == '-' || label.back() == '-') throw std::runtime_error("VPN 服务域名无效。");
        offset = end + 1;
    }
    return host;
}
std::string base64(std::span<const BYTE> data) {
    DWORD size = 0;
    if (!CryptBinaryToStringA(data.data(), static_cast<DWORD>(data.size()), CRYPT_STRING_BASE64 | CRYPT_STRING_NOCRLF, nullptr, &size)) win32Error("Cannot encode key");
    std::string result(size, '\0');
    if (!CryptBinaryToStringA(data.data(), static_cast<DWORD>(data.size()), CRYPT_STRING_BASE64 | CRYPT_STRING_NOCRLF, result.data(), &size)) win32Error("Cannot encode key");
    result.resize(size); while (!result.empty() && result.back() == '\0') result.pop_back(); return result;
}
class WindowsWireGuardTunnel final : public WireGuardTunnel {
public:
    std::pair<std::string, std::string> generateKeys() override {
        // A Go c-shared runtime owns background threads and must remain loaded for this process lifetime.
        static HMODULE module = [] {
            const auto path = moduleDirectory() / L"tunnel.dll";
            auto loaded = LoadLibraryExW(path.c_str(), nullptr, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_SYSTEM32);
            if (!loaded) win32Error("Cannot load tunnel key generator");
            return loaded;
        }();
        using Generate = void(__cdecl*)(BYTE*, BYTE*);
        const auto generate = reinterpret_cast<Generate>(GetProcAddress(module, "WireGuardGenerateKeypair"));
        if (!generate) throw std::runtime_error("Native tunnel key API is unavailable");
        std::array<BYTE, 32> pub{}, priv{};
        try {
            generate(pub.data(), priv.data());
            auto result = std::pair(base64(pub), base64(priv)); SecureZeroMemory(priv.data(), priv.size()); return result;
        } catch (...) { SecureZeroMemory(priv.data(), priv.size()); throw; }
    }
    ~WindowsWireGuardTunnel() override { stop(); }
    bool running() override {
        if (!adapter_) return false;
        WIREGUARD_ADAPTER_STATE state{};
        if (!api().getState(adapter_, &state)) win32Error("Cannot query tunnel adapter");
        return state == WIREGUARD_ADAPTER_STATE_UP;
    }
    void stop() override {
        if (adapter_) {
            api().setState(adapter_, WIREGUARD_ADAPTER_STATE_DOWN);
            api().close(adapter_); adapter_ = nullptr;
        }
        activeAddress_.clear(); activePublicKey_.clear(); activeHubKey_.clear(); activeRoutes_.clear();
    }
    void apply(const Json& config, const std::string& privateKey) override {
        validateTunnelConfig(config, privateKey);
        const auto routes = array(config, "allowedRoutes", 8192);
        if (routes.empty()) { stop(); return; }
        const auto remote = resolve(field(config, "hubEndpoint"), config.at("hubListenPort").get<int>());
        const DWORD size = static_cast<DWORD>(sizeof(WIREGUARD_INTERFACE) + sizeof(WIREGUARD_PEER) + routes.size()*sizeof(WIREGUARD_ALLOWED_IP));
        std::vector<BYTE> bytes(size);
        struct Wipe { std::vector<BYTE>& bytes; ~Wipe(){SecureZeroMemory(bytes.data(),bytes.size());} } wipe{bytes};
        auto interface = reinterpret_cast<WIREGUARD_INTERFACE*>(bytes.data());
        const bool reuse=adapter_ && activeAddress_==field(config,"assignedIpv4") && activePublicKey_==field(config,"publicKey");
        interface->Flags = reuse ? static_cast<WIREGUARD_INTERFACE_FLAG>(0) : WIREGUARD_INTERFACE_HAS_PRIVATE_KEY;
        if(!reuse||activeHubKey_!=field(config,"hubPublicKey")) interface->Flags |= WIREGUARD_INTERFACE_REPLACE_PEERS;
        decodeKey(privateKey, interface->PrivateKey); interface->PeersCount = 1;
        auto peer = reinterpret_cast<WIREGUARD_PEER*>(interface + 1);
        peer->Flags = WIREGUARD_PEER_HAS_PUBLIC_KEY | WIREGUARD_PEER_HAS_ENDPOINT | WIREGUARD_PEER_HAS_PERSISTENT_KEEPALIVE | WIREGUARD_PEER_REPLACE_ALLOWED_IPS;
        decodeKey(field(config,"hubPublicKey"),peer->PublicKey);
        peer->Endpoint = remote;
        peer->PersistentKeepalive = config.at("persistentKeepalive").get<WORD>();
        peer->AllowedIPsCount = static_cast<DWORD>(routes.size());
        auto allowed = reinterpret_cast<WIREGUARD_ALLOWED_IP*>(peer + 1);
        for (size_t i=0;i<routes.size();++i) {
            const auto slash=routes[i].find('/');
            allowed[i].AddressFamily=AF_INET;
            InetPtonA(AF_INET,routes[i].substr(0,slash).c_str(),&allowed[i].Address.V4);
            allowed[i].Cidr=static_cast<BYTE>(std::stoi(routes[i].substr(slash+1)));
        }
        if(!reuse) stop();
        try {
            static constexpr GUID id{0x2a79d553,0xf678,0x4ac5,{0x86,0x34,0xd4,0x57,0x63,0xce,0xae,0xa1}};
            if(!adapter_) adapter_=api().create(L"iot-egine",L"iot-egine",&id);
            if (!adapter_) win32Error("Cannot create tunnel adapter");
            if (!api().configure(adapter_,interface,size)) win32Error("Cannot configure tunnel adapter");
            NET_LUID luid{}; api().luid(adapter_,&luid);
            MIB_IPINTERFACE_ROW ip{}; InitializeIpInterfaceEntry(&ip);
            ip.Family=AF_INET; ip.InterfaceLuid=luid;
            check(GetIpInterfaceEntry(&ip),"Cannot read tunnel interface");
            ip.NlMtu=config.at("mtu").get<ULONG>(); ip.UseAutomaticMetric=FALSE; ip.Metric=5;
            ip.DadTransmits=0; ip.RouterDiscoveryBehavior=RouterDiscoveryDisabled;
            check(SetIpInterfaceEntry(&ip),"Cannot configure tunnel interface");
            MIB_UNICASTIPADDRESS_ROW address{}; InitializeUnicastIpAddressEntry(&address);
            address.InterfaceLuid=luid; address.Address.si_family=AF_INET;
            InetPtonA(AF_INET,field(config,"assignedIpv4").c_str(),&address.Address.Ipv4.sin_addr);
            address.OnLinkPrefixLength=32; address.DadState=IpDadStatePreferred;
            if(!reuse) check(CreateUnicastIpAddressEntry(&address),"Cannot assign tunnel address");
            const std::set<std::string> nextRoutes(routes.begin(),routes.end());
            for(const auto& previous:activeRoutes_) if(!nextRoutes.contains(previous)) {
                MIB_IPFORWARD_ROW2 old{}; InitializeIpForwardEntry(&old);
                old.InterfaceLuid=luid; old.DestinationPrefix.Prefix.si_family=AF_INET; old.NextHop.si_family=AF_INET;
                const auto slash=previous.find('/'); InetPtonA(AF_INET,previous.substr(0,slash).c_str(),&old.DestinationPrefix.Prefix.Ipv4.sin_addr);
                old.DestinationPrefix.PrefixLength=static_cast<UINT8>(std::stoi(previous.substr(slash+1)));
                const auto error=DeleteIpForwardEntry2(&old);
                if(error!=ERROR_NOT_FOUND) check(error,"Cannot remove obsolete tunnel route");
            }
            auto installedRoutes=activeRoutes_;
            for(size_t i=0;i<routes.size();++i) {
                if(!installedRoutes.insert(routes[i]).second) continue;
                MIB_IPFORWARD_ROW2 route{}; InitializeIpForwardEntry(&route);
                route.InterfaceLuid=luid; route.DestinationPrefix.Prefix.si_family=AF_INET;
                route.DestinationPrefix.Prefix.Ipv4.sin_addr=allowed[i].Address.V4;
                route.DestinationPrefix.PrefixLength=allowed[i].Cidr;
                route.NextHop.si_family=AF_INET; route.Metric=0; route.Protocol=MIB_IPPROTO_NETMGMT;
                check(CreateIpForwardEntry2(&route),"Cannot add tunnel route");
            }
            if (!api().setState(adapter_,WIREGUARD_ADAPTER_STATE_UP)) win32Error("Cannot start tunnel adapter");
            activeAddress_=field(config,"assignedIpv4"); activePublicKey_=field(config,"publicKey"); activeHubKey_=field(config,"hubPublicKey"); activeRoutes_=nextRoutes;
        } catch (...) { stop(); throw; }
    }
private:
    struct Api {
        HMODULE module{};
        WIREGUARD_CREATE_ADAPTER_FUNC* create{};
        WIREGUARD_CLOSE_ADAPTER_FUNC* close{};
        WIREGUARD_GET_ADAPTER_LUID_FUNC* luid{};
        WIREGUARD_SET_CONFIGURATION_FUNC* configure{};
        WIREGUARD_SET_ADAPTER_STATE_FUNC* setState{};
        WIREGUARD_GET_ADAPTER_STATE_FUNC* getState{};
        template<class T> void load(T& fn,const char* name) {
            fn=reinterpret_cast<T>(GetProcAddress(module,name));
            if(!fn) throw std::runtime_error("WireGuard driver API is unavailable");
        }
        Api() {
            const auto path=moduleDirectory()/L"wireguard.dll";
            module=LoadLibraryExW(path.c_str(),nullptr,LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR|LOAD_LIBRARY_SEARCH_SYSTEM32);
            if(!module) win32Error("Cannot load WireGuard driver");
            load(create,"WireGuardCreateAdapter"); load(close,"WireGuardCloseAdapter");
            load(luid,"WireGuardGetAdapterLUID"); load(configure,"WireGuardSetConfiguration");
            load(setState,"WireGuardSetAdapterState"); load(getState,"WireGuardGetAdapterState");
        }
    };
    static Api& api() { static Api instance; return instance; }
    static void check(DWORD error,const char* message) { if(error!=NO_ERROR) win32Error(message,error); }
    static void decodeKey(std::string_view text,BYTE* output) {
        DWORD size=32;
        if(!CryptStringToBinaryA(text.data(),static_cast<DWORD>(text.size()),CRYPT_STRING_BASE64|CRYPT_STRING_STRICT,output,&size,nullptr,nullptr)||size!=32)
            throw std::runtime_error("Invalid WireGuard key");
    }
    static SOCKADDR_INET resolve(std::string host,int port) {
        struct Winsock { Winsock(){WSADATA data{}; if(WSAStartup(MAKEWORD(2,2),&data)) throw std::runtime_error("Cannot initialize networking");} ~Winsock(){WSACleanup();} };
        static Winsock winsock;
        if(host.starts_with('[')&&host.ends_with(']')) host=host.substr(1,host.size()-2);
        ADDRINFOA hints{}; hints.ai_family=AF_UNSPEC; hints.ai_socktype=SOCK_DGRAM; hints.ai_protocol=IPPROTO_UDP;
        ADDRINFOA* result{};
        const int error=getaddrinfo(host.c_str(),std::to_string(port).c_str(),&hints,&result);
        if(error) win32Error("Cannot resolve tunnel endpoint",error);
        SOCKADDR_INET address{};
        for(auto next=result;next;next=next->ai_next) {
            if(next->ai_family==AF_INET||next->ai_family==AF_INET6) { memcpy(&address,next->ai_addr,next->ai_addrlen); break; }
        }
        freeaddrinfo(result);
        if(!address.si_family) throw std::runtime_error("WireGuard endpoint has no usable address");
        return address;
    }
    WIREGUARD_ADAPTER_HANDLE adapter_{};
    std::string activeAddress_,activePublicKey_,activeHubKey_;
    std::set<std::string> activeRoutes_;
};
}
void validateTunnelConfig(const Json& config, std::string_view privateKey) {
    if (!keyValid(privateKey) || !keyValid(field(config, "publicKey")) || !keyValid(field(config, "hubPublicKey")))
        throw std::runtime_error("WireGuard 密钥格式无效。");
    std::uint32_t assigned = 0;
    if (!ipv4(field(config, "assignedIpv4"), assigned) || !overlay(assigned)) throw std::runtime_error("客户端地址不在 VPN 地址池中。");
    integer(config, "mtu", 576, 9000); integer(config, "persistentKeepalive", 0, 65535); integer(config, "hubListenPort", 1, 65535);
    endpoint(field(config, "hubEndpoint"));
    if (!uuid(field(config, "peerId"))) throw std::runtime_error("VPN 客户端编号无效。");
    const auto edges = array(config, "edgeNodeIds", 64);
    for (const auto& id : edges) if (!uuid(id)) throw std::runtime_error("设备编号无效。");
    const auto routes = array(config, "allowedRoutes", 8192);
    if (edges.empty() && !routes.empty()) throw std::runtime_error("空设备选择不能包含访问路由。");
    std::set<std::string> edgeRoutes;
    for (const auto& address : array(config, "edgeAddresses", 512)) {
        std::uint32_t value;
        if (!ipv4(address, value) || !overlay(value)) throw std::runtime_error("设备虚拟地址无效。");
        edgeRoutes.insert(address + "/32");
    }
    for (const auto& route : routes) {
        const auto slash = route.find('/');
        std::uint32_t address = 0; int prefix = -1;
        if (slash == std::string::npos || !ipv4(std::string_view(route).substr(0, slash), address)) throw std::runtime_error("VPN 路由格式无效。");
        const auto parsed = std::from_chars(route.data() + slash + 1, route.data() + route.size(), prefix);
        if (parsed.ec != std::errc{} || parsed.ptr != route.data() + route.size() || prefix < 0 || prefix > 32) throw std::runtime_error("VPN 路由前缀无效。");
        const std::uint32_t mask = prefix == 0 ? 0 : 0xffffffffU << (32 - prefix);
        if ((address & mask) != address || (overlay(address) ? prefix != 32 || !edgeRoutes.contains(route) : prefix < 12 || (address & 0xfff00000U) != 0xac100000U))
            throw std::runtime_error("VPN 路由超出允许的虚拟网段或设备地址。");
    }
    for (const auto& route : edgeRoutes) if (std::find(routes.begin(), routes.end(), route) == routes.end()) throw std::runtime_error("平台缺少必要设备路由。");
}
std::string renderTunnelConfig(const Json& config, std::string_view privateKey) {
    validateTunnelConfig(config, privateKey);
    const auto routes = array(config, "allowedRoutes", 8192);
    std::ostringstream out;
    out << "[Interface]\nPrivateKey = " << privateKey << "\nAddress = " << field(config, "assignedIpv4") << "/32\nMTU = " << config["mtu"]
        << "\n\n[Peer]\nPublicKey = " << field(config, "hubPublicKey") << "\nEndpoint = " << endpoint(field(config, "hubEndpoint")) << ':' << config["hubListenPort"] << "\nAllowedIPs = ";
    bool first = true;
    for (const auto& route : std::set<std::string>(routes.begin(), routes.end())) { if (!first) out << ", "; out << route; first = false; }
    out << "\nPersistentKeepalive = " << config["persistentKeepalive"] << '\n';
    return out.str();
}
std::unique_ptr<WireGuardTunnel> createWindowsWireGuardTunnel() { return std::make_unique<WindowsWireGuardTunnel>(); }
}
