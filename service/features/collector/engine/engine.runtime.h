#pragma once

#include "service/features/collector/engine/engine.types.h"

#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "service/features/collector/collector.protocol.h"

namespace service::collector {

// The registry is owned and accessed by one Collector Worker. It deliberately has no mutex and no
// process-wide singleton; every Worker gets isolated protocol factory objects.
class ProtocolSessionFactoryRegistry final {
  public:
    void add(std::unique_ptr<ProtocolSessionFactory> factory) {
        if (!factory)
            throw std::invalid_argument("protocol factory is null");
        const std::string name(factory->definition().name);
        if (name.empty())
            throw std::invalid_argument("protocol factory name is empty");
        if (!factories_.emplace(name, std::move(factory)).second)
            throw std::invalid_argument("duplicate protocol factory: " + name);
    }

    [[nodiscard]] const ProtocolSessionFactory* find(std::string_view protocol) const noexcept {
        const auto current = factories_.find(protocol);
        return current == factories_.end() ? nullptr : current->second.get();
    }

    [[nodiscard]] const ProtocolSessionFactory& require(std::string_view protocol) const {
        const auto* factory = find(protocol);
        if (!factory)
            throw std::runtime_error("protocol factory is not registered: " +
                                     std::string(protocol));
        return *factory;
    }

  private:
    std::map<std::string, std::unique_ptr<ProtocolSessionFactory>, std::less<>> factories_;
};

} // namespace service::collector

#include <algorithm>
#include <set>
#include <utility>
#include <vector>

#include "service/features/collector/collector.types.h"

namespace service::collector {

namespace detail {

inline const LinkDefinition* findLink(
    const std::map<std::string, LinkDefinition, std::less<>>& links,
    std::string_view id) {
    const auto current = links.find(id);
    return current == links.end() ? nullptr : &current->second;
}

inline bool targetAllowsSessionRefresh(const RuntimeSnapshot& snapshot,
                                       const ClientTargetKey& target, const ProtocolSessionFactoryRegistry& registry) {
    return std::none_of(snapshot.devices.begin(), snapshot.devices.end(),
                        [&target, &registry](const auto& device) {
                            return device.linkId == target.first &&
                                   device.targetId == target.second &&
                                   !registry.require(device.protocol).canRefreshTransport(device);
                        });
}

inline void addChangedDeviceTransport(
    RuntimeReconcilePlan& plan,
    const std::map<std::string, LinkDefinition, std::less<>>& links,
    const DeviceDefinition& device, const ProtocolSessionFactoryRegistry& registry) {
    plan.affectedLinks.insert(device.linkId);
    const auto* link = findLink(links, device.linkId);
    if (!link || link->mode != "TCP Client") {
        plan.restartLinks.insert(device.linkId);
        return;
    }
    if (device.targetId.empty()) {
        plan.restartLinks.insert(device.linkId);
        return;
    }
    if (registry.require(device.protocol).canRefreshTransport(device))
        plan.refreshClientSessions.emplace(device.linkId, device.targetId);
    else
        plan.restartClientTargets.emplace(device.linkId, device.targetId);
}

} // namespace detail

// Worker-affine protocol engine. It owns every socket's protocol session and is the only layer
// shared by Modbus, S7 and SL651. Scheduling semantics remain inside each concrete session.
class ProtocolEngine final {
  public:
    explicit ProtocolEngine(ProtocolSessionFactoryRegistry registry) : registry_(std::move(registry)) {}

    [[nodiscard]] std::vector<std::uint8_t> packetForLogging(std::string_view protocol,
        std::span<const std::uint8_t> bytes) const {
        const auto* factory = registry_.find(protocol);
        return factory ? factory->packetForLogging(bytes) : std::vector<std::uint8_t>(bytes.begin(), bytes.end());
    }

    [[nodiscard]] const DeviceDefinition* identifyDebugDevice(
        const RuntimeSnapshot& snapshot, const DebugPacketIdentity& packet) const {
        const auto link = std::find_if(snapshot.links.begin(), snapshot.links.end(),
            [&](const auto& candidate) { return candidate.id == packet.linkId; });
        if (link == snapshot.links.end()) return nullptr;
        const auto& factory = registry_.require(link->protocol);
        const auto attribution = factory.definition().packetAttribution;
        // 主动上报协议的传输片段不能代替已通过完整帧校验的设备身份。
        if (attribution == PacketAttribution::ParsedFrame && packet.direction == "RX" &&
            !packet.deviceOnly && packet.knownDevice.empty()) return nullptr;
        const DeviceDefinition* selected = nullptr;
        for (const auto& device : snapshot.devices) {
            if (device.linkId != packet.linkId || device.protocol != link->protocol) continue;
            if (!packet.knownDevice.empty() && device.id != packet.knownDevice) continue;
            if (!packet.targetId.empty() && device.targetId != packet.targetId) continue;
            if (attribution == PacketAttribution::Connection && packet.knownDevice.empty() &&
                packet.boundDevices && !packet.boundDevices->empty() &&
                !packet.boundDevices->contains(device.id)) continue;
            if (!factory.packetMatchesDevice(device, packet.direction, packet.bytes)) continue;
            if (selected) return nullptr;
            selected = &device;
        }
        return selected;
    }

    [[nodiscard]] bool expectsResponse(std::string_view protocol, std::string_view causationId) const {
        const auto* factory = registry_.find(protocol);
        return factory && (factory->definition().responseTracking == ResponseTracking::EverySend ||
                           !causationId.empty());
    }

    [[nodiscard]] RuntimeReconcilePlan
    planReconcile(const RuntimeSnapshot& previous, const RuntimeSnapshot& next) const {
        std::map<std::string, LinkDefinition, std::less<>> previousLinks;
        std::map<std::string, LinkDefinition, std::less<>> nextLinks;
        std::map<std::string, DeviceDefinition, std::less<>> previousDevices;
        std::map<std::string, DeviceDefinition, std::less<>> nextDevices;

        for (const auto& link : previous.links)
            previousLinks.insert_or_assign(link.id, link);
        for (auto& [id, link] : previousLinks) link.debugEnabled = false;
        for (const auto& link : next.links)
            nextLinks.insert_or_assign(link.id, link);
        for (auto& [id, link] : nextLinks) link.debugEnabled = false;
        for (const auto& device : previous.devices)
            previousDevices.insert_or_assign(device.id, device);
        for (auto& [id, device] : previousDevices) device.debugEnabled = false;
        for (const auto& device : next.devices)
            nextDevices.insert_or_assign(device.id, device);
        for (auto& [id, device] : nextDevices) device.debugEnabled = false;

        RuntimeReconcilePlan plan;
        for (const auto& [id, link] : previousLinks) {
            const auto current = nextLinks.find(id);
            if (current == nextLinks.end() || current->second != link)
                plan.affectedLinks.insert(id);
        }
        for (const auto& [id, link] : nextLinks) {
            const auto old = previousLinks.find(id);
            if (old == previousLinks.end() || old->second != link)
                plan.affectedLinks.insert(id);
        }

        for (const auto& [id, device] : previousDevices) {
            const auto current = nextDevices.find(id);
            if (current == nextDevices.end() || current->second != device) {
                detail::addChangedDeviceTransport(plan, previousLinks, device, registry_);
                if (current != nextDevices.end())
                    detail::addChangedDeviceTransport(plan, nextLinks, current->second, registry_);
            }
        }
        for (const auto& [id, device] : nextDevices) {
            if (!previousDevices.contains(id))
                detail::addChangedDeviceTransport(plan, nextLinks, device, registry_);
        }
        for (auto current = plan.refreshClientSessions.begin();
             current != plan.refreshClientSessions.end();) {
            if (!detail::targetAllowsSessionRefresh(previous, *current, registry_) ||
                !detail::targetAllowsSessionRefresh(next, *current, registry_)) {
                plan.restartClientTargets.insert(*current);
                current = plan.refreshClientSessions.erase(current);
            } else
                ++current;
        }
        return plan;
    }

    void reload(RuntimeSnapshot snapshot) {
        replaceSnapshot(std::move(snapshot));
        // Configuration generations are immutable for an established connection. Close/recreate
        // is handled by the link factory; retaining a session across incompatible config would
        // allow stale device routes and command responses.
        sessions_.clear();
    }

    void reloadRetainingSessions(RuntimeSnapshot snapshot) {
        replaceSnapshot(std::move(snapshot));
        // TcpTransport owns transport reconciliation. Sessions are retained here until either the target
        // emits a real disconnected event or Worker selectively refreshes an eligible Modbus TCP
        // session after rotating its transport epoch.
    }

    [[nodiscard]] std::vector<ProtocolSessionRefresh>
    refreshClientSessions(const std::set<ClientTargetKey>& targets) {
        std::vector<ProtocolSessionRefresh> refreshed;
        for (auto& [connectionId, entry] : sessions_) {
            if (entry.info.targetId.empty() ||
                !targets.contains({entry.info.linkId, entry.info.targetId}))
                continue;

            const auto link = links_.find(entry.info.linkId);
            if (link == links_.end() || link->second->mode != "TCP Client" ||
                link->second->status != "enabled")
                continue;
            const auto target = std::find_if(
                link->second->targets.begin(), link->second->targets.end(),
                [&entry](const auto& candidate) {
                    return candidate.id == entry.info.targetId && candidate.status == "enabled";
                });
            // A removed or disabled target is being closed by TcpTransport. Let its real disconnected
            // event retire the protocol session and connection epoch in the normal order.
            if (target == link->second->targets.end())
                continue;

            const auto& factory = registry_.require(link->second->protocol);
            auto nextSession = factory.createSession(*link->second, connectionId,
                                                     entry.info.targetId, snapshot_);
            nextSession->inheritTransportState(*entry.session);
            auto startedActions = nextSession->connected();
            auto retiredActions = entry.session->disconnected("configuration_reloaded");
            entry.session = std::move(nextSession);
            refreshed.push_back({.connectionId = connectionId,
                                 .retiredActions = std::move(retiredActions),
                                 .startedActions = std::move(startedActions)});
        }
        return refreshed;
    }

    [[nodiscard]] std::vector<ProtocolAction> connected(ProtocolConnectionInfo info) {
        const auto link = links_.find(info.linkId);
        if (link == links_.end())
            return {{.kind = ProtocolActionKind::Close,
                     .connectionId = info.connectionId,
                     .reason = "link_config_not_found"}};
        const auto& factory = registry_.require(link->second->protocol);
        auto session =
            factory.createSession(*link->second, info.connectionId, info.targetId, snapshot_);
        auto actions = session->connected();
        auto connectionId = info.connectionId;
        sessions_.insert_or_assign(std::move(connectionId),
                                   SessionEntry{std::move(info), std::move(session)});
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> consume(const message::IngressPacket& packet) {
        const auto current = sessions_.find(packet.connectionId);
        if (current == sessions_.end())
            return {{.kind = ProtocolActionKind::Close,
                     .connectionId = packet.connectionId,
                     .reason = "protocol_session_not_found"}};
        ProtocolInput input{.messageId = packet.messageId,
                            .linkId = packet.linkId,
                            .connectionId = packet.connectionId,
                            .remoteAddress = packet.remoteAddress,
                            .receivedAtMs = packet.occurredAtMs,
                            .bytes = packet.payload};
        return current->second.session->consume(input);
    }

    [[nodiscard]] std::vector<ProtocolAction> disconnected(std::string_view connectionId,
                                                           std::string_view reason) {
        const auto current = sessions_.find(connectionId);
        if (current == sessions_.end())
            return {};
        auto actions = current->second.session->disconnected(reason);
        sessions_.erase(current);
        return actions;
    }

    [[nodiscard]] std::vector<ProtocolAction> execute(std::string_view connectionId,
                                                      ProtocolCommand command) {
        const auto current = sessions_.find(connectionId);
        if (current == sessions_.end())
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = std::string(connectionId),
                     .commandId = std::move(command.id),
                     .reason = "engine_device_offline"}};
        const auto link = links_.find(current->second.info.linkId);
        if (link == links_.end() || (!command.protocol.empty() &&
            link->second->protocol != command.protocol))
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = std::string(connectionId),
                     .commandId = std::move(command.id),
                     .reason = "protocol_route_mismatch"}};
        const auto& definition = registry_.require(link->second->protocol).definition();
        const auto supported = command.kind == "discovery" ? ProtocolCapability::Discovery
                                                          : ProtocolCapability::Commands;
        if (!definition.capabilities.has(supported))
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = std::string(connectionId),
                     .commandId = std::move(command.id),
                     .reason = "protocol_command_not_supported"}};
        const auto capability =
            dynamic_cast<CommandCapabilitySession*>(current->second.session.get());
        if (!capability)
            return {{.kind = ProtocolActionKind::FailCommand,
                     .connectionId = std::string(connectionId),
                     .commandId = std::move(command.id),
                     .reason = "protocol_command_not_supported"}};
        return capability->execute(std::move(command));
    }

    [[nodiscard]] std::vector<ProtocolAction> deadline(std::string_view connectionId,
                                                       std::uint64_t token) {
        const auto current = sessions_.find(connectionId);
        if (current == sessions_.end())
            return {};
        const auto capability =
            dynamic_cast<DeadlineCapabilitySession*>(current->second.session.get());
        return capability ? capability->deadline(token) : std::vector<ProtocolAction>{};
    }

    [[nodiscard]] std::vector<ProtocolAction> parsedPublished(std::string_view connectionId,
                                                            std::uint64_t publicationToken) {
        const auto current = sessions_.find(connectionId);
        return current == sessions_.end() ? std::vector<ProtocolAction>{}
                                         : current->second.session->parsedPublished(publicationToken);
    }

    [[nodiscard]] bool contains(std::string_view connectionId) const noexcept {
        return sessions_.contains(connectionId);
    }

  private:
    struct SessionEntry {
        ProtocolConnectionInfo info;
        std::unique_ptr<ProtocolSession> session;
    };

    void replaceSnapshot(RuntimeSnapshot snapshot) {
        for (const auto& link : snapshot.links) {
            const auto& factory = registry_.require(link.protocol);
            factory.validateLink(link);
        }
        snapshot_ = std::make_shared<RuntimeSnapshot>(std::move(snapshot));
        links_.clear();
        for (const auto& link : snapshot_->links)
            links_.emplace(link.id, &link);
    }

    ProtocolSessionFactoryRegistry registry_;
    std::shared_ptr<const RuntimeSnapshot> snapshot_ = std::make_shared<RuntimeSnapshot>();
    std::map<std::string, const LinkDefinition*, std::less<>> links_;
    std::map<std::string, SessionEntry, std::less<>> sessions_;
};

} // namespace service::collector
