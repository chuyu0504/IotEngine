#pragma once

#include <cstdint>
#include <string>

#include <ruvia/web/dotenv.h>

#include "service/features/vpn/wireguard/wireguard.config.h"

namespace service::vpn::config {

struct Config final {
    wireguard::HubConfig hub;
    bool enabled{ true };
};

inline Config fromEnv(const ruvia::env& env) {
    Config config;
    config.hub.interfaceName = std::string(env.get("VPN_HUB_INTERFACE").value_or("wg"));
    config.hub.privateKey = std::string(env.get("VPN_HUB_PRIVATE_KEY").value_or(""));
    config.hub.publicKey = std::string(env.get("VPN_HUB_PUBLIC_KEY").value_or(""));
    config.hub.endpoint = std::string(env.get("VPN_HUB_ENDPOINT").value_or(""));
    config.hub.listenPort = env.get<std::uint16_t>("VPN_HUB_LISTEN_PORT").value_or(51820);
    config.enabled = env.get<bool>("VPN_HUB_ENABLED").value_or(true);
    return config;
}

} // namespace service::vpn::config
