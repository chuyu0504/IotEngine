#include <array>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "service/modules/device/device.service.h"

namespace {
void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void verifyDeviceCapabilities() {
    using namespace service::device;
    constexpr std::array levels{ DeviceAccessLevel::none, DeviceAccessLevel::view, DeviceAccessLevel::operate, DeviceAccessLevel::owner };
    for (const auto level : levels) {
        for (unsigned permissions = 0; permissions != 16; ++permissions) {
            DeviceActor actor;
            actor.canEdit = (permissions & 1) != 0;
            actor.canDelete = (permissions & 2) != 0;
            actor.canShare = (permissions & 4) != 0;
            actor.canCommand = (permissions & 8) != 0;
            for (const bool remoteControl : { false, true }) {
                const auto result = DeviceAccessService::capabilities(actor, level, remoteControl);
                const bool owner = level == DeviceAccessLevel::owner;
                const bool operatorAccess = owner || level == DeviceAccessLevel::operate;
                require(result.canEdit == (owner && actor.canEdit), "edit requires ownership and permission");
                require(result.canDelete == (owner && actor.canDelete), "delete requires ownership and permission");
                require(result.canShare == (owner && actor.canShare), "share requires ownership and permission");
                require(result.canCommand == (operatorAccess && remoteControl && actor.canCommand), "command requires operate access, remote control and permission");
            }
        }
    }
    require(DeviceAccessService::rank("0") == DeviceAccessLevel::none, "none rank");
    require(DeviceAccessService::rank("1") == DeviceAccessLevel::view, "view rank");
    require(DeviceAccessService::rank("2") == DeviceAccessLevel::operate, "operate rank");
    require(DeviceAccessService::rank("4") == DeviceAccessLevel::owner, "owner rank");
    for (const auto invalid : { "", "-1", "1junk", "nan", "9999999999999999999999999" }) {
        require(DeviceAccessService::rank(invalid) == DeviceAccessLevel::none, "invalid rank must not grant access");
    }
}

bool packetRejected(std::string_view json) {
    try {
        auto packet = ruvia::from_json<service::device::DevicePacketBody>(json);
        service::device::validatePacket(std::optional<service::device::DevicePacketBody>(std::move(packet)));
    } catch (...) {
        return true;
    }
    return false;
}

void verifyPacketValidation() {
    service::device::validatePacket(std::nullopt);
    require(!packetRejected(R"({"mode":"OFF"})"), "OFF packet rejected");
    require(!packetRejected(R"({})"), "default packet mode must be OFF");
    require(!packetRejected(R"({"mode":"ASCII","content":"hello"})"), "ASCII packet rejected");
    require(!packetRejected(R"({"mode":"HEX","content":"0a 1B"})"), "spaced HEX packet rejected");
    require(packetRejected(R"({"mode":"BIN","content":"00"})"), "unknown mode accepted");
    require(packetRejected(R"({"mode":"ASCII"})"), "empty content accepted");
    require(packetRejected(R"({"mode":"HEX","content":"ABC"})"), "odd HEX accepted");
    require(packetRejected(R"({"mode":"HEX","content":"ZZ"})"), "non-hex accepted");
    require(packetRejected(std::string(R"({"mode":"ASCII","content":")") + std::string(257, 'a') + "\"}"), "oversized ASCII accepted");
    require(!packetRejected(std::string(R"({"mode":"HEX","content":")") + std::string(512, 'F') + "\"}"), "256-byte HEX rejected");
    require(packetRejected(std::string(R"({"mode":"HEX","content":")") + std::string(514, 'F') + "\"}"), "oversized HEX accepted");
}

void verifyDebugKeys() {
    using namespace service::device;
    require(entities::debugLeaseKey("d1") == "iot:debug:lease:device:d1", "device debug lease key changed");
    require(entities::debugWatchKey("d1") == "iot:debug:watch:device:d1", "device debug watch key changed");
    const auto keys = DeviceService::debugIdleKeys("d1");
    require(keys.lease == "iot:debug:lease:device:d1" && keys.watch == "iot:debug:watch:device:d1", "device debug idle keys mismatch");
}
} // namespace

int main() {
    try {
        verifyDeviceCapabilities();
        verifyPacketValidation();
        verifyDebugKeys();
        std::cout << "device access behavior tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "device service test failed: " << error.what() << '\n';
        return 1;
    }
}
