#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

struct DeviceRouteSnapshot {
    bool online{ false };
    std::string remoteAddress;
    bool hasChannels{ false };
    bool channelExists{ false };
};

enum class DeviceChange {
    Status,
    Catalog,
    DeviceName,
    ChannelName,
};

struct Channel {
    std::string id;
    std::string name;
    std::string customName;
    std::string manufacturer;
    bool online{ false };
    int ptzType{ -1 };

    [[nodiscard]] std::string_view displayName() const noexcept {
        return customName.empty() ? std::string_view{ name }
                                  : std::string_view{ customName };
    }
};

struct Device {
    std::string id;
    std::string name;
    std::string customName;
    std::string manufacturer;
    std::string remoteAddress;
    std::string registrationSource{ "sip" };
    bool online{ false };
    std::chrono::system_clock::time_point lastSeen{};
    std::vector<Channel> channels;
    // A registration generation fences delayed events from a previous SIP
    // session on the same Collector.  It is combined with the process
    // incarnation and Collector index in the Redis owner token.
    std::uint64_t sessionGeneration{ 0 };

    [[nodiscard]] std::string_view displayName() const noexcept {
        return customName.empty() ? std::string_view{ name }
                                  : std::string_view{ customName };
    }
};
