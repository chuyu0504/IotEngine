#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace service::gb28181::device {

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

// The device changes carried by one coalesced projection.  The Service applies
// the union of the fields owned by these kinds from the latest snapshot, which
// equals applying the individual changes of one session in order.
class DeviceChangeSet final {
  public:
    DeviceChangeSet() = default;

    explicit DeviceChangeSet(DeviceChange change) noexcept { add(change); }

    void add(DeviceChange change) noexcept { bits_ |= bit(change); }

    void add(DeviceChangeSet changes) noexcept { bits_ |= changes.bits_; }

    [[nodiscard]] bool contains(DeviceChange change) const noexcept {
        return (bits_ & bit(change)) != 0;
    }

    [[nodiscard]] bool empty() const noexcept { return bits_ == 0; }

    friend bool operator==(DeviceChangeSet, DeviceChangeSet) = default;

  private:
    static constexpr std::uint8_t bit(DeviceChange change) noexcept {
        return static_cast<std::uint8_t>(1U << static_cast<unsigned>(change));
    }

    std::uint8_t bits_{ 0 };
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

} // namespace service::gb28181::device
