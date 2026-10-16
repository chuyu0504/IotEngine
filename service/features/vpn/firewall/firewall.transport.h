#pragma once

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#ifdef __linux__
#include <fcntl.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "service/common/message.h"
#include "service/features/vpn/firewall/firewall.types.h"
#include "service/utils/network.h"

#ifdef __linux__
extern char** environ;
#endif

namespace service::vpn::firewall {

using service::message::vpn::kOverlayPool;
using service::message::vpn::kVirtualLanPool;
using service::utils::network::parseCidr;
using service::utils::network::parseIpv4;

inline bool validInterface(std::string_view value) noexcept {
    if (value.empty() || value.size() > 15) {
        return false;
    }
    for (const auto character : value) {
        if (!std::isalnum(static_cast<unsigned char>(character)) && character != '_' &&
            character != '-' && character != '.') {
            return false;
        }
    }
    return true;
}

inline std::string ipv4Text(std::uint32_t address) {
    return std::to_string((address >> 24U) & 0xffU) + "." +
        std::to_string((address >> 16U) & 0xffU) + "." +
        std::to_string((address >> 8U) & 0xffU) + "." +
        std::to_string(address & 0xffU);
}

inline void appendSet(std::string& script, const std::vector<std::string>& values) {
    if (values.size() == 1) {
        script += values.front();
        return;
    }
    script += "{ ";
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (index != 0) {
            script += ", ";
        }
        script += values[index];
    }
    script += " }";
}

inline Result render(std::string_view interfaceName, const std::vector<ClientAccess>& clients, std::string& script) {
    script.clear();
    if (!validInterface(interfaceName)) {
        return { .configured = false, .message = "invalid VPN interface name" };
    }

    script =
        "flush table inet iot_vpn\n"
        "add chain inet iot_vpn forward { type filter hook forward priority 0; policy accept; }\n";
    for (const auto& client : clients) {
        const auto assigned = parseIpv4(client.assignedIpv4);
        if (!assigned || !kOverlayPool.contains(*assigned)) {
            return { .configured = false, .message = "invalid VPN client address" };
        }
        std::vector<std::string> sources{ ipv4Text(*assigned) + "/32" };
        sources.reserve(client.sourceRoutes.size() + 1);
        for (const auto& routeText : client.sourceRoutes) {
            const auto route = parseCidr(routeText, kVirtualLanPool.prefix, 30);
            if (!route || !kVirtualLanPool.contains(route->network) ||
                !kVirtualLanPool.contains(route->network + route->size() - 1U)) {
                return { .configured = false, .message = "invalid VPN client source route" };
            }
            if (std::find(sources.begin(), sources.end(), route->text()) == sources.end()) {
                sources.push_back(route->text());
            }
        }
        std::vector<std::string> routes;
        routes.reserve(client.allowedRoutes.size() + client.edgeAddresses.size());
        for (const auto& routeText : client.allowedRoutes) {
            const auto route = parseCidr(routeText, kVirtualLanPool.prefix, 30);
            if (!route || !kVirtualLanPool.contains(route->network) ||
                !kVirtualLanPool.contains(route->network + route->size() - 1U)) {
                return { .configured = false, .message = "invalid VPN client route" };
            }
            routes.push_back(route->text());
        }
        for (const auto& addressText : client.edgeAddresses) {
            const auto address = parseIpv4(addressText);
            if (!address || !kOverlayPool.contains(*address)) {
                return { .configured = false, .message = "invalid VPN Edge address" };
            }
            const auto route = ipv4Text(*address) + "/32";
            if (std::find(routes.begin(), routes.end(), route) == routes.end()) {
                routes.push_back(route);
            }
        }
        if (routes.empty()) {
            continue;
        }
        script += "add rule inet iot_vpn forward iifname \"" +
            std::string(interfaceName) + "\" oifname \"" +
            std::string(interfaceName) + "\" ip saddr ";
        appendSet(script, sources);
        script += " ip daddr ";
        appendSet(script, routes);
        script += " accept\n";
        // Permit replies only for routes currently selected by this client.  A
        // global established rule would keep deselected Edge connections alive.
        script += "add rule inet iot_vpn forward iifname \"" +
            std::string(interfaceName) + "\" oifname \"" +
            std::string(interfaceName) + "\" ct state established,related ip daddr ";
        appendSet(script, sources);
        script += " ip saddr ";
        appendSet(script, routes);
        script += " accept\n";
    }
    script += "add rule inet iot_vpn forward iifname \"" + std::string(interfaceName) +
        "\" oifname \"" + std::string(interfaceName) +
        "\" ip saddr { " + kOverlayPool.text() + ", " +
        kVirtualLanPool.text() + " } drop\n";
    return { .configured = true, .message = "VPN firewall rules rendered" };
}

#ifdef __linux__

inline bool writeAll(int fd, std::string_view value) {
    std::size_t offset = 0;
    while (offset < value.size()) {
        const auto written = ::write(fd, value.data() + offset, value.size() - offset);
        if (written > 0) {
            offset += static_cast<std::size_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

inline bool runNft(const std::vector<std::string>& arguments) {
    std::vector<char*> argv;
    argv.reserve(arguments.size() + 1);
    for (const auto& argument : arguments) {
        argv.push_back(const_cast<char*>(argument.c_str()));
    }
    argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    if (::posix_spawn_file_actions_init(&actions) != 0) {
        return false;
    }
    if (::posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, "/dev/null", O_WRONLY, 0) != 0 ||
        ::posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, "/dev/null", O_WRONLY, 0) != 0) {
        (void)::posix_spawn_file_actions_destroy(&actions);
        return false;
    }
    pid_t process = -1;
    const auto spawned =
        ::posix_spawnp(&process, "nft", &actions, nullptr, argv.data(), environ);
    (void)::posix_spawn_file_actions_destroy(&actions);
    if (spawned != 0) {
        return false;
    }
    int status = 0;
    while (::waitpid(process, &status, 0) < 0) {
        if (errno == EINTR) {
            continue;
        }
        return false;
    }
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

inline Result apply(std::string_view interfaceName, const std::vector<ClientAccess>& clients) {
    std::string script;
    const auto rendered = render(interfaceName, clients, script);
    if (!rendered.configured) {
        return rendered;
    }

    (void)runNft({ "nft", "add", "table", "inet", "iot_vpn" });
    char path[] = "/tmp/iot-vpn-nft.XXXXXX";
    const int fd = ::mkstemp(path);
    if (fd < 0) {
        return { .configured = false, .message = "cannot create VPN firewall input" };
    }
    (void)::fchmod(fd, S_IRUSR | S_IWUSR);
    const bool written = writeAll(fd, script);
    const bool closed = ::close(fd) == 0;
    const bool applied = written && closed && runNft({ "nft", "-f", path });
    (void)::unlink(path);
    return applied ? Result{ .configured = true, .message = "VPN firewall is configured" }
                   : Result{ .configured = false, .message = "cannot configure VPN firewall" };
}

#else

inline Result apply(std::string_view, const std::vector<ClientAccess>&) {
    return { .configured = false, .message = "VPN firewall is only supported on Linux" };
}

#endif

} // namespace service::vpn::firewall
