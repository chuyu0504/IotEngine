#pragma once
#include "../common/json.h"
#include <memory>
#include <utility>
namespace iotvpn {
class WireGuardTunnel {
public:
    virtual ~WireGuardTunnel() = default;
    // public key first, private key second
    virtual std::pair<std::string, std::string> generateKeys() = 0;
    virtual bool running() = 0;
    virtual void stop() = 0;
    virtual void apply(const Json& config, const std::string& privateKey) = 0;
};
std::unique_ptr<WireGuardTunnel> createWindowsWireGuardTunnel();
void validateTunnelConfig(const Json& config, std::string_view privateKey);
std::string renderTunnelConfig(const Json& config, std::string_view privateKey);

}
