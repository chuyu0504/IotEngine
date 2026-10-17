#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/redis/redis_handle.h>
#include <ruvia/web/redis/redis_types.h>

#include "service/common/message.h"
#include "service/common/worker.h"
#include "service/features/collector/collector.protocol.h"
#include "service/features/collector/collector.types.h"
#include "service/features/collector/engine/engine.types.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/packet_log/packet_log.service.h"
#include "service/features/packet_log/packet_log.transport.h"
#include "service/features/telemetry/derived/derived.service.h"
#include "service/utils/log.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"
#include "service/utils/uuid.h"

namespace service::collector::config {

inline constexpr std::int64_t kSnapshotGraceMilliseconds = 120000;

inline std::string prefix(std::string_view version) {
    return std::string(service::message::projection::kRuntimeVersionPrefix) + std::string(version);
}

inline std::string linkKey(std::string_view version, std::string_view linkId) {
    return prefix(version) + ":link:" + std::string(linkId);
}

inline std::string targetKey(std::string_view version, std::string_view linkId, std::string_view targetId) {
    return linkKey(version, linkId) + ":target:" + std::string(targetId);
}

inline std::string deviceKey(std::string_view version, std::string_view deviceId) {
    return prefix(version) + ":device:" + std::string(deviceId);
}

inline std::string metadataKey(std::string_view version) {
    return prefix(version) + ":metadata";
}

// A deterministic content signature makes periodic Service reconciliation cheap and
// idempotent. It is not a security hash; length-prefixed FNV-1a is sufficient to distinguish
// runtime snapshots and remains stable across process restarts.
inline std::string signature(const RuntimeSnapshot& snapshot) {
    std::uint64_t value = 14695981039346656037ULL;
    const auto byte = [&value](std::uint8_t input) {
        value ^= input;
        value *= 1099511628211ULL;
    };
    const auto number = [&byte](std::uint64_t input) {
        for (std::size_t shift = 0; shift < sizeof(input) * 8; shift += 8) {
            byte(static_cast<std::uint8_t>(input >> shift));
        }
    };
    const auto text = [&number, &byte](std::string_view input) {
        number(input.size());
        for (const auto character : input) {
            byte(static_cast<std::uint8_t>(character));
        }
    };
    const auto bytes = [&number, &byte](const std::vector<std::uint8_t>& input) {
        number(input.size());
        for (const auto character : input) {
            byte(character);
        }
    };
    const auto integer = [&number](std::int64_t input) {
        number(static_cast<std::uint64_t>(input));
    };

    number(snapshot.links.size());
    for (const auto& link : snapshot.links) {
        text(link.id);
        text(link.name);
        text(link.mode);
        text(link.protocol);
        text(link.ip);
        number(link.port);
        text(link.status);
        number(link.debugEnabled);
        number(link.targets.size());
        for (const auto& target : link.targets) {
            text(target.id);
            text(target.name);
            text(target.ip);
            number(target.port);
            text(target.status);
            text(target.mqttConfig);
        }
    }
    number(snapshot.devices.size());
    for (const auto& device : snapshot.devices) {
        text(device.id);
        text(device.modelId);
        text(device.code);
        text(device.name);
        text(device.linkId);
        text(device.linkMode);
        text(device.targetId);
        text(device.protocol);
        text(device.timezone);
        text(device.sl651ResponseMode);
        number(device.debugEnabled);
        integer(device.onlineTimeout);
        text(device.heartbeatMode);
        bytes(device.heartbeatBytes);
        text(device.registrationMode);
        bytes(device.registrationBytes);
        text(device.modbusMode);
        number(device.slaveId);
        integer(device.modbusMergeGap);
        integer(device.modbusMaxQuantity);
        text(device.s7ConnectionMode);
        text(device.s7ConnectionType);
        integer(device.s7Rack);
        integer(device.s7Slot);
        text(device.s7LocalTsap);
        text(device.s7RemoteTsap);
        integer(device.s7HandshakeTimeoutMs);
        integer(device.s7DirectProbeTimeoutMs);
        text(device.s7ProbeMode);
        for (const auto& field : ConnectionConfig::fields(device)) {
            text(field.name);
            text(field.value);
        }
        integer(device.readInterval);
        text(device.calculationConfig);
        text(device.parameters);
        text(device.storagePolicy);
        integer(device.commandFastReadDuration);
        integer(device.commandFastReadInterval);
        number(device.elements.size());
        for (const auto& element : device.elements) {
            text(element.configKey);
            text(element.id);
            text(element.name);
            text(element.unit);
            text(element.dataType);
            text(element.byteOrder);
            text(element.registerType);
            text(element.mqttField);
            integer(element.address);
            integer(element.quantity);
            integer(element.decimals);
            number(element.writable ? 1 : 0);
            text(element.area);
            integer(element.dbNumber);
            integer(element.start);
            integer(element.startBit);
            integer(element.size);
            text(element.functionCode);
            text(element.direction);
            text(element.guideHex);
            text(element.positionMode);
            integer(element.byteOffset);
            text(element.encoding);
            integer(element.length);
            integer(element.digits);
            number(element.responseElement ? 1 : 0);
        }
    }
    number(snapshot.realtimeDevices.size());
    for (const auto& device : snapshot.realtimeDevices) {
        text(device.id);
        text(device.code);
        text(device.name);
        number(device.points.size());
        for (const auto& point : device.points) {
            text(point.id);
            text(point.name);
            text(point.unit);
        }
    }
    std::array<char, 16> output{};
    constexpr char digits[] = "0123456789abcdef";
    for (std::size_t index = 0; index < output.size(); ++index) {
        output[index] = digits[(value >> ((output.size() - index - 1) * 4)) & 0x0F];
    }
    return { output.data(), output.size() };
}

inline std::string elementKey(std::string_view version, std::string_view deviceId, std::string_view elementId) {
    return deviceKey(version, deviceId) + ":element:" + std::string(elementId);
}

namespace detail {

inline std::string stringReply(const ruvia::redis_value& value, std::string_view operation) {
    if (value.kind() != ruvia::redis_value::kind_type::string) {
        service::utils::redis::throwValue(operation, value);
    }
    return std::string(value.string());
}

inline std::int64_t integerReply(const ruvia::redis_value& value, std::string_view operation) {
    if (value.kind() != ruvia::redis_value::kind_type::integer) {
        service::utils::redis::throwValue(operation, value);
    }
    return value.integer();
}

inline void requireOk(const ruvia::redis_value& value, std::string_view operation) {
    if (stringReply(value, operation) != "OK") {
        throw std::runtime_error(std::string(operation) + " returned an unexpected status");
    }
}

inline std::vector<std::string> stringArray(const ruvia::redis_value& value, std::string_view operation) {
    if (value.kind() != ruvia::redis_value::kind_type::array) {
        service::utils::redis::throwValue(operation, value);
    }
    std::vector<std::string> result;
    result.reserve(value.array().size());
    for (const auto& entry : value.array()) {
        if (entry.kind() != ruvia::redis_value::kind_type::string) {
            service::utils::redis::throwValue(operation, value);
        }
        result.emplace_back(entry.string());
    }
    return result;
}

inline std::vector<std::string> sortedStrings(std::vector<std::string> values) {
    std::ranges::sort(values);
    return values;
}

inline std::vector<message::StreamField> hashFields(const ruvia::redis_value& value, std::string_view operation) {
    if (value.kind() != ruvia::redis_value::kind_type::array) {
        service::utils::redis::throwValue(operation, value);
    }
    const auto values = value.array();
    if (values.size() % 2 != 0) {
        service::utils::redis::throwValue(operation, value);
    }
    std::vector<message::StreamField> result;
    result.reserve(values.size() / 2);
    for (std::size_t index = 0; index < values.size(); index += 2) {
        if (values[index].kind() != ruvia::redis_value::kind_type::string ||
            values[index + 1].kind() != ruvia::redis_value::kind_type::string) {
            service::utils::redis::throwValue(operation, value);
        }
        result.push_back(
            { std::string(values[index].string()), std::string(values[index + 1].string()) }
        );
    }
    return result;
}

template <typename Redis>
ruvia::task<std::pmr::vector<ruvia::redis_value>>
execute(const Redis& redis, const std::vector<std::vector<std::string>>& commands) {
    if (commands.empty()) {
        co_return std::pmr::vector<ruvia::redis_value>{};
    }
    if constexpr (requires { redis.pipeline(); }) {
        auto pipeline = redis.pipeline();
        for (const auto& command : commands) {
            std::vector<std::string_view> views(command.begin(), command.end());
            pipeline.command(views);
        }
        auto replies = co_await std::move(pipeline).exec();
        for (const auto& reply : replies) {
            if (reply.kind() == ruvia::redis_value::kind_type::error) {
                service::utils::redis::throwValue("runtime pipeline", reply);
            }
        }
        co_return replies;
    } else {
        static constexpr std::string_view script = R"lua(
local replies = {}
for index = 1, #ARGV, 2 do
  replies[#replies + 1] = redis.call(ARGV[index], ARGV[index + 1])
end
return replies
)lua";
        std::vector<std::string> arguments;
        arguments.reserve(commands.size() * 2);
        for (const auto& command : commands) {
            if (command.size() != 2 || (command[0] != "HGETALL" && command[0] != "SMEMBERS")) {
                throw std::runtime_error("unsupported runtime batch command");
            }
            arguments.insert(arguments.end(), command.begin(), command.end());
        }
        std::vector<std::string_view> views(arguments.begin(), arguments.end());
        const auto reply = co_await redis.eval(script, {}, views);
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            service::utils::redis::throwValue("runtime batch", reply);
        }
        std::pmr::vector<ruvia::redis_value> replies;
        replies.reserve(reply.array().size());
        for (const auto& value : reply.array()) {
            replies.push_back(value);
        }
        co_return replies;
    }
}

template <typename Redis>
ruvia::task<void> flush(const Redis& redis, std::vector<std::vector<std::string>>& commands) {
    if (commands.empty()) {
        co_return;
    }
    (void)co_await execute(redis, commands);
    commands.clear();
}

inline void appendRememberedKey(std::vector<std::vector<std::string>>& commands, std::string_view version, const std::string& key) {
    commands.push_back({ "SADD", prefix(version) + ":keys", key });
}

inline void appendHash(std::vector<std::vector<std::string>>& commands, std::string_view version, std::string key, const std::vector<message::StreamField>& fields) {
    std::vector<std::string> command{ "HSET", key };
    command.reserve(2 + fields.size() * 2);
    for (const auto& field : fields) {
        command.push_back(field.name);
        command.push_back(field.value);
    }
    commands.push_back(std::move(command));
    appendRememberedKey(commands, version, key);
}

inline void appendId(std::vector<std::vector<std::string>>& commands, std::string_view version, std::string key, std::string_view id) {
    commands.push_back({ "SADD", key, std::string(id) });
    appendRememberedKey(commands, version, key);
}

inline void appendRealtimePoints(std::vector<std::vector<std::string>>& commands, std::string_view version, std::string key, const std::vector<RealtimePointDefinition>& points) {
    constexpr std::size_t chunkSize = 256;
    for (std::size_t offset = 0; offset < points.size(); offset += chunkSize) {
        const auto end = std::min(points.size(), offset + chunkSize);
        std::vector<std::string> command{ "RPUSH", key };
        command.reserve(2 + end - offset);
        for (std::size_t index = offset; index < end; ++index) {
            command.push_back(service::message::realtime::encodePoint(points[index]));
        }
        commands.push_back(std::move(command));
        appendRememberedKey(commands, version, key);
    }
}

inline std::string_view field(const std::vector<message::StreamField>& fields, std::string_view name) noexcept {
    for (const auto& value : fields) {
        if (value.name == name) {
            return value.value;
        }
    }
    return {};
}

inline std::int64_t integer(const std::vector<message::StreamField>& fields, std::string_view name, std::int64_t fallback = 0) {
    const auto value = field(fields, name);
    if (value.empty()) {
        return fallback;
    }
    std::int64_t result = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size()) {
        throw std::runtime_error("invalid runtime integer: " + std::string(name));
    }
    return result;
}

inline std::int64_t integerInRange(const std::vector<message::StreamField>& fields, std::string_view name, std::int64_t minimum, std::int64_t maximum, std::int64_t fallback = 0) {
    const auto value = integer(fields, name, fallback);
    if (value < minimum || value > maximum) {
        throw std::runtime_error("invalid runtime integer range: " + std::string(name));
    }
    return value;
}

inline bool boolean(const std::vector<message::StreamField>& fields, std::string_view name, bool fallback = false) {
    const auto value = field(fields, name);
    if (value.empty()) {
        return fallback;
    }
    if (value == "0") {
        return false;
    }
    if (value == "1") {
        return true;
    }
    throw std::runtime_error("invalid runtime boolean: " + std::string(name));
}

inline std::vector<std::uint8_t> hexBytes(const std::vector<message::StreamField>& fields, std::string_view name) {
    const auto value = field(fields, name);
    auto result = message::fromHex(value);
    if (result.empty() && !value.empty()) {
        throw std::runtime_error("invalid runtime hex: " + std::string(name));
    }
    return result;
}

inline DeviceDefinition device(const std::vector<message::StreamField>& fields) {
    DeviceDefinition result;
    result.id = field(fields, "id");
    result.modelId = field(fields, "model_id");
    result.code = field(fields, "code");
    result.name = field(fields, "name");
    result.linkId = field(fields, "link_id");
    result.linkMode = field(fields, "link_mode");
    result.targetId = field(fields, "target_id");
    result.protocol = field(fields, "protocol");
    result.timezone = field(fields, "timezone");
    result.debugEnabled = field(fields, "debug_enabled") == "1";
    result.sl651ResponseMode = field(fields, "sl651_response_mode");
    if (result.sl651ResponseMode.empty()) {
        result.sl651ResponseMode = "M1";
    }
    result.onlineTimeout = integer(fields, "online_timeout", 300);
    result.heartbeatMode = field(fields, "heartbeat_mode");
    result.heartbeatBytes = hexBytes(fields, "heartbeat_hex");
    result.registrationMode = field(fields, "registration_mode");
    result.registrationBytes = hexBytes(fields, "registration_hex");
    result.modbusMode = field(fields, "modbus_mode");
    result.slaveId = static_cast<std::uint8_t>(integerInRange(fields, "slave_id", 0, 255, 1));
    result.modbusMergeGap = integer(fields, "modbus_merge_gap", 100);
    result.modbusMaxQuantity = integer(fields, "modbus_max_quantity", 125);
    result.s7ConnectionMode = field(fields, "s7_connection_mode");
    result.s7ConnectionType = field(fields, "s7_connection_type");
    result.s7Rack = integer(fields, "s7_rack");
    result.s7Slot = integer(fields, "s7_slot", 1);
    result.s7LocalTsap = field(fields, "s7_local_tsap");
    result.s7RemoteTsap = field(fields, "s7_remote_tsap");
    result.s7HandshakeTimeoutMs = integer(fields, "s7_handshake_timeout_ms", 5000);
    result.s7DirectProbeTimeoutMs = integer(fields, "s7_direct_probe_timeout_ms", 5000);
    result.s7ProbeMode = field(fields, "s7_probe_mode");
    if (result.s7ProbeMode.empty()) {
        result.s7ProbeMode = "STANDARD";
    }
    ConnectionConfig::apply(result, fields);
    result.readInterval = integer(fields, "read_interval", 300);
    result.calculationConfig = field(fields, "calculation_config");
    result.parameters = field(fields, "parameters");
    if (result.parameters.empty()) {
        result.parameters = "{}";
    }
    result.storagePolicy = field(fields, "storage_policy");
    if (result.storagePolicy != "report" && result.storagePolicy != "change") {
        throw std::runtime_error("invalid runtime storage policy");
    }
    result.commandFastReadDuration = integer(fields, "command_fast_read_duration", 60);
    result.commandFastReadInterval = integer(fields, "command_fast_read_interval", 1);
    return result;
}

inline ElementDefinition element(const std::vector<message::StreamField>& fields) {
    ElementDefinition result;
    result.configKey = field(fields, "config_key");
    result.id = field(fields, "id");
    result.name = field(fields, "name");
    result.unit = field(fields, "unit");
    result.dataType = field(fields, "data_type");
    result.byteOrder = field(fields, "byte_order");
    result.registerType = field(fields, "register_type");
    result.mqttField = field(fields, "mqtt_field");
    result.address = integer(fields, "address");
    result.quantity = integer(fields, "quantity");
    result.decimals = integer(fields, "decimals", -1);
    result.writable = boolean(fields, "writable");
    result.area = field(fields, "area");
    result.dbNumber = integer(fields, "db_number");
    result.start = integer(fields, "start");
    result.startBit = integer(fields, "start_bit");
    result.size = integer(fields, "size");
    result.functionCode = field(fields, "function_code");
    result.direction = field(fields, "direction");
    result.guideHex = field(fields, "guide_hex");
    result.positionMode = field(fields, "position_mode");
    if (result.positionMode.empty()) {
        result.positionMode = "GUIDE";
    }
    result.byteOffset = integer(fields, "byte_offset", 0);
    result.encoding = field(fields, "encoding");
    result.length = integer(fields, "length");
    result.digits = integer(fields, "digits");
    result.responseElement = boolean(fields, "response_element");
    return result;
}

template <typename Redis>
ruvia::task<std::vector<std::string>> members(const Redis& redis, std::string key) {
    const std::vector<std::string> arguments{ "SMEMBERS", std::move(key) };
    co_return sortedStrings(
        stringArray(co_await redis.command(std::vector<std::string_view>(arguments.begin(), arguments.end())), "SMEMBERS runtime")
    );
}

template <typename Redis>
ruvia::task<void> eraseSnapshot(const Redis& redis, std::string_view version) {
    const auto keysSet = prefix(version) + ":keys";
    const std::vector<std::string> keysArguments{ "SMEMBERS", keysSet };
    auto keys = stringArray(co_await redis.command(std::vector<std::string_view>(keysArguments.begin(), keysArguments.end())), "SMEMBERS runtime snapshot keys");
    keys.push_back(keysSet);
    constexpr std::size_t batchSize = 128;
    for (std::size_t offset = 0; offset < keys.size(); offset += batchSize) {
        std::vector<std::string> args{ "UNLINK" };
        const auto end = std::min(keys.size(), offset + batchSize);
        args.insert(args.end(), keys.begin() + static_cast<std::ptrdiff_t>(offset), keys.begin() + static_cast<std::ptrdiff_t>(end));
        (void)integerReply(co_await redis.command(std::vector<std::string_view>(args.begin(), args.end())), "UNLINK runtime snapshot keys");
    }
}

} // namespace detail

template <typename Redis>
ruvia::task<void> eraseExpiredSnapshots(const Redis& redis, std::int64_t nowMilliseconds, std::string_view preserveVersion) {
    const std::vector<std::string> expiredArguments{
        "ZRANGEBYSCORE",
        std::string(service::message::projection::kRuntimeVersionsKey),
        "-inf",
        std::to_string(nowMilliseconds - kSnapshotGraceMilliseconds)
    };
    const auto expired = detail::stringArray(
        co_await redis.command(std::vector<std::string_view>(expiredArguments.begin(), expiredArguments.end())),
        "ZRANGEBYSCORE runtime versions"
    );
    for (const auto& candidate : expired) {
        if (candidate == preserveVersion) {
            continue;
        }
        co_await detail::eraseSnapshot(redis, candidate);
        const std::vector<std::string> removeArguments{ "ZREM", std::string(service::message::projection::kRuntimeVersionsKey), candidate };
        (void)detail::integerReply(
            co_await redis.command(std::vector<std::string_view>(removeArguments.begin(), removeArguments.end())),
            "ZREM runtime version"
        );
    }
}

// Service projection. The version pointer is switched only after every readable
// Hash/Set/List has been written, so readers never observe a partial snapshot.
template <typename Redis>
ruvia::task<std::string> project(const Redis& redis, const RuntimeSnapshot& snapshot, service::common::UuidV7State& uuidState) {
    const auto snapshotSignature = signature(snapshot);
    const std::vector<std::string> activeReplyArguments{ "GET", std::string(service::message::projection::kRuntimeActiveVersionKey) };
    const auto activeReply =
        co_await redis.command(std::vector<std::string_view>(activeReplyArguments.begin(), activeReplyArguments.end()));
    if (!activeReply.null()) {
        const auto active = detail::stringReply(activeReply, "GET active runtime");
        const std::vector<std::string> signatureReplyArguments{ "HGET", metadataKey(active), "signature" };
        const auto signatureReply =
            co_await redis.command(std::vector<std::string_view>(signatureReplyArguments.begin(), signatureReplyArguments.end()));
        if (!signatureReply.null() &&
            detail::stringReply(signatureReply, "HGET runtime signature") ==
                snapshotSignature) {
            co_return active;
        }
    }

    const auto version = service::utils::nextUuid(uuidState);
    const auto createdAt = message::utcNowMilliseconds();
    const std::vector<std::string> versionArguments{ "ZADD", std::string(service::message::projection::kRuntimeVersionsKey), std::to_string(createdAt), version };
    (void)detail::integerReply(
        co_await redis.command(std::vector<std::string_view>(versionArguments.begin(), versionArguments.end())),
        "ZADD runtime version"
    );
    const auto snapshotPrefix = prefix(version);
    const auto linksKey = snapshotPrefix + ":links";
    const auto devicesKey = snapshotPrefix + ":devices";
    std::vector<std::vector<std::string>> commands;
    commands.reserve(1024);
    constexpr std::size_t pipelineSize = 512;

    detail::appendHash(commands, version, metadataKey(version), { { "version", version }, { "signature", snapshotSignature }, { "created_at_ms", std::to_string(createdAt) } });

    for (const auto& link : snapshot.links) {
        detail::appendId(commands, version, linksKey, link.id);
        detail::appendHash(commands, version, linkKey(version, link.id), { { "id", link.id }, { "name", link.name }, { "mode", link.mode }, { "protocol", link.protocol }, { "ip", link.ip }, { "port", std::to_string(link.port) }, { "status", link.status }, { "debug_enabled", link.debugEnabled ? "1" : "0" } });
        const auto targetsKey = linkKey(version, link.id) + ":targets";
        for (const auto& target : link.targets) {
            detail::appendId(commands, version, targetsKey, target.id);
            detail::appendHash(commands, version, targetKey(version, link.id, target.id), { { "id", target.id }, { "name", target.name }, { "ip", target.ip }, { "port", std::to_string(target.port) }, { "status", target.status }, { "mqtt_config", target.mqttConfig } });
        }
        if (commands.size() >= pipelineSize) {
            co_await detail::flush(redis, commands);
        }
    }

    for (const auto& device : snapshot.devices) {
        detail::appendId(commands, version, devicesKey, device.id);
        detail::appendHash(commands, version, deviceKey(version, device.id), ConnectionConfig::fields(device));
        detail::appendHash(
            commands,
            version,
            deviceKey(version, device.id),
            { { "id", device.id },
              { "model_id", device.modelId },
              { "code", device.code },
              { "name", device.name },
              { "link_id", device.linkId },
              { "link_mode", device.linkMode },
              { "target_id", device.targetId },
              { "protocol", device.protocol },
              { "timezone", device.timezone },
              { "online_timeout", std::to_string(device.onlineTimeout) },
              { "heartbeat_mode", device.heartbeatMode },
              { "heartbeat_hex", message::toHex(device.heartbeatBytes) },
              { "registration_mode", device.registrationMode },
              { "registration_hex", message::toHex(device.registrationBytes) },
              { "modbus_mode", device.modbusMode },
              { "sl651_response_mode", device.sl651ResponseMode },
              { "debug_enabled", device.debugEnabled ? "1" : "0" },
              { "slave_id", std::to_string(device.slaveId) },
              { "modbus_merge_gap", std::to_string(device.modbusMergeGap) },
              { "modbus_max_quantity", std::to_string(device.modbusMaxQuantity) },
              { "s7_connection_mode", device.s7ConnectionMode },
              { "s7_connection_type", device.s7ConnectionType },
              { "s7_rack", std::to_string(device.s7Rack) },
              { "s7_slot", std::to_string(device.s7Slot) },
              { "s7_local_tsap", device.s7LocalTsap },
              { "s7_remote_tsap", device.s7RemoteTsap },
              { "s7_handshake_timeout_ms", std::to_string(device.s7HandshakeTimeoutMs) },
              { "s7_direct_probe_timeout_ms", std::to_string(device.s7DirectProbeTimeoutMs) },
              { "s7_probe_mode", device.s7ProbeMode },
              { "read_interval", std::to_string(device.readInterval) },
              { "calculation_config", device.calculationConfig },
              { "parameters", device.parameters },
              { "storage_policy", device.storagePolicy },
              { "command_fast_read_duration", std::to_string(device.commandFastReadDuration) },
              { "command_fast_read_interval", std::to_string(device.commandFastReadInterval) } }
        );
        const auto elementsKey = deviceKey(version, device.id) + ":elements";
        for (const auto& element : device.elements) {
            const auto& configKey = element.configKey.empty() ? element.id : element.configKey;
            detail::appendId(commands, version, elementsKey, configKey);
            detail::appendHash(commands, version, elementKey(version, device.id, configKey), { { "config_key", configKey }, { "id", element.id }, { "name", element.name }, { "unit", element.unit }, { "data_type", element.dataType }, { "byte_order", element.byteOrder }, { "register_type", element.registerType }, { "mqtt_field", element.mqttField }, { "address", std::to_string(element.address) }, { "quantity", std::to_string(element.quantity) }, { "decimals", std::to_string(element.decimals) }, { "writable", element.writable ? "1" : "0" }, { "area", element.area }, { "db_number", std::to_string(element.dbNumber) }, { "start", std::to_string(element.start) }, { "start_bit", std::to_string(element.startBit) }, { "size", std::to_string(element.size) }, { "function_code", element.functionCode }, { "direction", element.direction }, { "guide_hex", element.guideHex }, { "position_mode", element.positionMode }, { "byte_offset", std::to_string(element.byteOffset) }, { "encoding", element.encoding }, { "length", std::to_string(element.length) }, { "digits", std::to_string(element.digits) }, { "response_element", element.responseElement ? "1" : "0" } });
            if (commands.size() >= pipelineSize) {
                co_await detail::flush(redis, commands);
            }
        }
    }

    for (const auto& device : snapshot.realtimeDevices) {
        detail::appendHash(commands, version, service::message::projection::realtimeDeviceKey(version, device.id), { { "id", device.id }, { "code", device.code }, { "name", device.name } });
        detail::appendRealtimePoints(commands, version, service::message::projection::realtimeDevicePointsKey(version, device.id), device.points);
        if (commands.size() >= pipelineSize) {
            co_await detail::flush(redis, commands);
        }
    }
    co_await detail::flush(redis, commands);

    const std::vector<std::string> previousReplyArguments{ "GET", std::string(service::message::projection::kRuntimeActiveVersionKey) };
    const auto previousReply =
        co_await redis.command(std::vector<std::string_view>(previousReplyArguments.begin(), previousReplyArguments.end()));
    if (!previousReply.null()) {
        const auto previous = detail::stringReply(previousReply, "GET active runtime");
        if (!previous.empty() && previous != version) {
            const std::vector<std::string> previousVersionArguments{ "ZADD", std::string(service::message::projection::kRuntimeVersionsKey), std::to_string(createdAt), previous };
            (void)detail::integerReply(
                // The score is the time this version stopped being active, not its creation
                // time. Refresh it on every switch so readers that captured the old pointer
                // receive the complete grace window even after a long-lived generation.
                co_await redis.command(std::vector<std::string_view>(previousVersionArguments.begin(), previousVersionArguments.end())),
                "ZADD previous runtime version"
            );
        }
    }
    const std::vector<std::string> activateArguments{ "SET", std::string(service::message::projection::kRuntimeActiveVersionKey), version };
    detail::requireOk(
        co_await redis.command(std::vector<std::string_view>(activateArguments.begin(), activateArguments.end())),
        "SET active runtime version"
    );

    // A worker may have read the previous pointer just before this switch. Keep completed
    // snapshots for a bounded grace window so its multi-key load cannot be torn down midway.
    co_await eraseExpiredSnapshots(redis, createdAt, version);
    co_return version;
}

template <typename Redis>
ruvia::task<std::string> activeVersion(const Redis& redis) {
    const std::vector<std::string> versionReplyArguments{ "GET", std::string(service::message::projection::kRuntimeActiveVersionKey) };
    const auto versionReply =
        co_await redis.command(std::vector<std::string_view>(versionReplyArguments.begin(), versionReplyArguments.end()));
    if (versionReply.null()) {
        throw std::runtime_error("runtime is not ready");
    }
    co_return detail::stringReply(versionReply, "GET active runtime");
}

template <typename Redis>
ruvia::task<RuntimeSnapshot> load(const Redis& redis, std::string version) {
    RuntimeSnapshot snapshot;

    for (const auto& linkId : co_await detail::members(redis, prefix(version) + ":links")) {
        const auto fields = co_await service::utils::redis::hashEntries(redis, linkKey(version, linkId));
        LinkDefinition link;
        link.id = detail::field(fields, "id");
        link.name = detail::field(fields, "name");
        link.mode = detail::field(fields, "mode");
        link.protocol = detail::field(fields, "protocol");
        link.ip = detail::field(fields, "ip");
        link.port = static_cast<std::uint16_t>(detail::integer(fields, "port"));
        link.status = detail::field(fields, "status");
        link.debugEnabled = detail::field(fields, "debug_enabled") == "1";
        for (const auto& targetId :
             co_await detail::members(redis, linkKey(version, linkId) + ":targets")) {
            const auto targetFields =
                co_await service::utils::redis::hashEntries(redis, targetKey(version, linkId, targetId));
            LinkTargetDefinition target;
            target.id = detail::field(targetFields, "id");
            target.name = detail::field(targetFields, "name");
            target.ip = detail::field(targetFields, "ip");
            target.port = static_cast<std::uint16_t>(detail::integer(targetFields, "port"));
            target.status = detail::field(targetFields, "status");
            const auto mqttConfig = detail::field(targetFields, "mqtt_config");
            target.mqttConfig = mqttConfig.empty() ? "{}" : std::string(mqttConfig);
            link.targets.push_back(std::move(target));
        }
        snapshot.links.push_back(std::move(link));
    }

    const auto deviceIds = co_await detail::members(redis, prefix(version) + ":devices");
    std::vector<std::vector<std::string>> deviceCommands;
    deviceCommands.reserve(deviceIds.size() * 2);
    for (const auto& deviceId : deviceIds) {
        deviceCommands.push_back({ "HGETALL", deviceKey(version, deviceId) });
        deviceCommands.push_back({ "SMEMBERS", deviceKey(version, deviceId) + ":elements" });
    }
    const auto deviceReplies = co_await detail::execute(redis, deviceCommands);

    struct ElementLoad {
        std::size_t deviceIndex;
        std::string key;
    };

    std::vector<ElementLoad> elementLoads;
    snapshot.devices.reserve(deviceIds.size());
    for (std::size_t index = 0; index < deviceIds.size(); ++index) {
        snapshot.devices.push_back(
            detail::device(detail::hashFields(deviceReplies[index * 2], "device config"))
        );
        const auto elements = detail::sortedStrings(
            detail::stringArray(deviceReplies[index * 2 + 1], "device element ids")
        );
        const auto deviceIndex = snapshot.devices.size() - 1;
        for (const auto& elementId : elements) {
            elementLoads.push_back({ deviceIndex, elementKey(version, deviceIds[index], elementId) });
        }
    }

    constexpr std::size_t elementBatchSize = 512;
    for (std::size_t offset = 0; offset < elementLoads.size(); offset += elementBatchSize) {
        const auto end = std::min(elementLoads.size(), offset + elementBatchSize);
        std::vector<std::vector<std::string>> elementCommands;
        elementCommands.reserve(end - offset);
        for (std::size_t index = offset; index < end; ++index) {
            elementCommands.push_back({ "HGETALL", elementLoads[index].key });
        }
        const auto elementReplies = co_await detail::execute(redis, elementCommands);
        for (std::size_t index = offset; index < end; ++index) {
            auto fields = detail::hashFields(elementReplies[index - offset], "element config");
            snapshot.devices[elementLoads[index].deviceIndex].elements.push_back(
                detail::element(fields)
            );
        }
    }
    co_return snapshot;
}

template <typename Redis>
ruvia::task<RuntimeSnapshot> load(const Redis& redis) {
    co_return co_await load(redis, co_await activeVersion(redis));
}

} // namespace service::collector::config

namespace service::collector::ownership {

inline std::string key(std::string_view linkId) {
    return message::collector::linkOwnerKey(linkId);
}

inline std::string targetKey(std::string_view linkId, std::string_view targetId) {
    return "iot:v2:owner:target:" + std::string(linkId) + ":" + std::string(targetId);
}

template <typename Redis>
ruvia::task<bool> isLinkOwner(const Redis& redis, std::string_view linkId, std::string_view expectedOwner) {
    const std::vector<std::string> ownerArguments{ "GET", key(linkId) };
    const auto owner = co_await redis.command(std::vector<std::string_view>(ownerArguments.begin(), ownerArguments.end()));
    co_return owner.kind() == ruvia::redis_value::kind_type::string&& owner.string() == expectedOwner;
}

inline constexpr std::string_view kRetainTargetScript = R"lua(
local current=redis.call('GET',KEYS[1])
if current and current~=ARGV[1] then return 0 end
redis.call('SET',KEYS[1],ARGV[1],'PX',15000)
return 1
)lua";

template <typename Redis>
ruvia::task<bool> retainTarget(const Redis& redis, std::string_view linkId, std::string_view targetId, std::string_view owner) {
    const auto lease = targetKey(linkId, targetId);
    const std::string_view keys[]{ lease };
    const std::string_view args[]{ owner };
    const auto reply = co_await redis.eval(kRetainTargetScript, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        utils::redis::throwValue("claim target ownership", reply);
    }
    co_return reply.integer() == 1;
}

inline constexpr std::string_view kRenewTargetScript = R"lua(
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
return redis.call('PEXPIRE',KEYS[1],15000)
)lua";

template <typename Redis>
ruvia::task<bool> renewTarget(const Redis& redis, std::string_view linkId, std::string_view targetId, std::string_view owner) {
    const auto lease = targetKey(linkId, targetId);
    const std::string_view keys[]{ lease };
    const std::string_view args[]{ owner };
    const auto reply = co_await redis.eval(kRenewTargetScript, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        utils::redis::throwValue("renew target ownership", reply);
    }
    co_return reply.integer() == 1;
}

inline constexpr std::string_view kReleaseTargetScript = R"lua(
if redis.call('GET',KEYS[1])==ARGV[1] then
  redis.call('DEL',KEYS[1])
  return 1
end
return 0
)lua";

template <typename Redis>
ruvia::task<void> releaseTarget(const Redis& redis, std::string_view linkId, std::string_view targetId, std::string_view owner) {
    const auto lease = targetKey(linkId, targetId);
    const std::string_view keys[]{ lease };
    const std::string_view args[]{ owner };
    const auto reply = co_await redis.eval(kReleaseTargetScript, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        utils::redis::throwValue("release target ownership", reply);
    }
}

inline constexpr std::string_view kRetainLinkOwnershipScript = R"lua(
local owner=redis.call('GET',KEYS[1])
if owner and owner~=ARGV[1] then return {0,redis.call('PTTL',KEYS[1])} end
redis.call('SET',KEYS[1],ARGV[1],'PX',15000)
return {1,redis.call('PTTL',KEYS[1])}
)lua";

template <typename Redis>
ruvia::task<OwnedSnapshot> retain(const Redis& redis, RuntimeSnapshot snapshot, std::string_view instanceId, std::map<std::string, LinkLease, std::less<>>& leases) {
    bool ownershipChanged = false;
    std::erase_if(leases, [&snapshot, &ownershipChanged](const auto& entry) {
        const bool removed = std::none_of(snapshot.links.begin(), snapshot.links.end(), [&entry](const auto& link) {
            return link.id == entry.first;
        });
        ownershipChanged = ownershipChanged || (removed && entry.second.owned);
        return removed;
    });
    std::optional<std::chrono::steady_clock::time_point> nextAttempt;
    for (const auto& link : snapshot.links) {
        auto [current, inserted] = leases.try_emplace(link.id);
        auto& state = current->second;
        const auto began = std::chrono::steady_clock::now();
        if (inserted || (state.nextAttempt && *state.nextAttempt <= began)) {
            state.nextAttempt = began;
            const auto lease = key(link.id);
            const std::string_view keys[]{ lease };
            const std::string_view args[]{ instanceId };
            const auto reply = co_await redis.eval(kRetainLinkOwnershipScript, keys, args);
            if (reply.kind() != ruvia::redis_value::kind_type::array ||
                reply.array().size() != 2 ||
                reply.array()[0].kind() != ruvia::redis_value::kind_type::integer ||
                reply.array()[1].kind() != ruvia::redis_value::kind_type::integer) {
                utils::redis::throwValue("claim link ownership", reply);
            }
            const bool owned = reply.array()[0].integer() == 1;
            ownershipChanged = ownershipChanged || state.owned != owned;
            state.owned = owned;
            const auto ttl = reply.array()[1].integer();
            // A foreign permanent lease has no expiration to wake for. A key that
            // vanished between GET and PTTL gets a bounded retry, never a busy loop.
            if (!state.owned && ttl == -1) {
                state.nextAttempt.reset();
            } else {
                const auto delay = ttl < 0 ? std::int64_t{ 250 }
                    : state.owned          ? (std::max)(std::int64_t{ 1 }, ttl / 3)
                                           : (std::max)(std::int64_t{ 1 }, ttl);
                state.nextAttempt = began + std::chrono::milliseconds(delay);
            }
        }
        if (state.nextAttempt && (!nextAttempt || *state.nextAttempt < *nextAttempt)) {
            nextAttempt = state.nextAttempt;
        }
    }
    std::erase_if(snapshot.links, [&leases](const auto& link) {
        return !leases.at(link.id).owned;
    });
    std::erase_if(snapshot.devices, [&leases](const auto& device) {
        const auto lease = leases.find(device.linkId);
        return lease == leases.end() || !lease->second.owned;
    });
    const auto wait = nextAttempt
        ? std::optional{ (std::max)(std::chrono::milliseconds(1),
                                    std::chrono::duration_cast<std::chrono::milliseconds>(*nextAttempt - std::chrono::steady_clock::now())) }
        : std::nullopt;
    co_return OwnedSnapshot{ std::move(snapshot), wait, ownershipChanged };
}

} // namespace service::collector::ownership

namespace service::collector {

inline constexpr std::size_t kDeadLetterCapacity = 1000;

// 死信条目保留原消息字段，并追加来源条目、失败原因与所属 Worker。
[[nodiscard]] inline std::vector<message::StreamField> deadLetterFields(
    std::vector<message::StreamField> fields,
    std::string_view sourceEntryId,
    std::string_view reason,
    std::string_view detail,
    std::size_t workerIndex
) {
    fields.push_back({ "source_entry_id", std::string(sourceEntryId) });
    fields.push_back({ "failure_reason", std::string(reason) });
    fields.push_back({ "failure_detail", std::string(detail) });
    fields.push_back({ "worker_id", std::to_string(workerIndex) });
    fields.push_back({ "failed_at_ms", std::to_string(message::utcNowMilliseconds()) });
    return fields;
}

// 命令生命周期：登记本 Worker 已认领的命令与广播子任务，按失败原因决定重试，
// 终结时发布结果或死信并确认输入条目。实例归所属 Collector Worker，借用其稳定身份（Worker 必须活得更久），不持有连接或协议会话；
// Worker 只负责消费命令、驱动协议引擎并把引擎结果交给本服务。
class CollectorCommandService final {
  public:
    static constexpr std::size_t kCommandStreamCapacity = 10000;
    static constexpr std::size_t kCommandResultCapacity = 10000;

    template <typename Redis>
    static ruvia::task<void> rejectMessage(const Redis& redis, std::size_t workerIndex, std::string_view instanceId, std::string_view consumer, const message::StreamMessage& message, std::string_view reason, std::string_view detail, std::string_view inputStream, std::string_view inputGroup) {
        auto fields = deadLetterFields(message.fields, message.id, reason, detail, workerIndex);
        const auto outputStream = message::deadLetterStream(workerIndex, instanceId);
        (void)co_await service::messaging::stream::publishAndAcknowledge(redis, outputStream, fields, kDeadLetterCapacity, inputStream, inputGroup, consumer, message.id, std::nullopt, instanceId);
    }

    CollectorCommandService(ruvia::redis_handle redis, service::common::UuidV7State& uuidState, const service::common::WorkerIdentity& identity, std::string group, std::string consumer, std::string deadLetterStream)
        : redis_(std::move(redis)), uuidState_(uuidState), identity_(identity),
          group_(std::move(group)), consumer_(std::move(consumer)),
          deadLetterStream_(std::move(deadLetterStream)) {}

    CollectorCommandService(const CollectorCommandService&) = delete;
    CollectorCommandService& operator=(const CollectorCommandService&) = delete;

    template <typename Redis>
    static ruvia::task<TransmissionReservation> reserveTransmission(
        const Redis& redis,
        const message::ProtocolTask& task,
        std::int64_t nowMs
    ) {
        if (nowMs - task.createdAtMs >= 60000) {
            co_return TransmissionReservation::Expired;
        }
        // 发送前的幂等认领锁：保留一天，避免同一命令被重复发送。
        const auto key = "iot:v2:command:sent:" + task.messageId;
        const std::vector<std::string> claimedArguments{ "SET", key, "1", "NX", "EX", "86400" };
        const auto claimed = co_await redis.command(std::vector<std::string_view>(claimedArguments.begin(), claimedArguments.end()));
        if (claimed.kind() == ruvia::redis_value::kind_type::null) {
            co_return TransmissionReservation::Duplicate;
        }
        if (claimed.kind() == ruvia::redis_value::kind_type::error) {
            utils::redis::throwValue("claim command transmission", claimed);
        }
        co_return TransmissionReservation::Reserved;
    }

    // 广播子任务返回其父命令。
    [[nodiscard]] const message::ProtocolTask* taskForCausation(std::string_view causationId) const noexcept {
        auto current = pending_.find(causationId);
        if (current != pending_.end()) {
            return &current->second.task;
        }
        const auto child = broadcastParents_.find(causationId);
        if (child == broadcastParents_.end()) {
            return nullptr;
        }
        current = pending_.find(child->second);
        return current == pending_.end() ? nullptr : &current->second.task;
    }

    void track(std::string_view stream, std::string_view entryId, const message::ProtocolTask& task) {
        pending_.insert_or_assign(task.messageId, Pending{ std::string(stream), std::string(entryId), task });
    }

    void startBroadcast(std::string_view stream, std::string_view entryId, const message::ProtocolTask& task, std::size_t targets) {
        track(stream, entryId, task);
        broadcasts_.insert_or_assign(task.messageId, Broadcast{ .remaining = targets });
    }

    void addBroadcastTarget(std::string childId, std::string_view parentId) {
        broadcastParents_.insert_or_assign(std::move(childId), std::string(parentId));
    }

    [[nodiscard]] std::vector<std::string> pendingForConnection(std::string_view connectionId) const {
        std::vector<std::string> commandIds;
        for (const auto& [commandId, pending] : pending_) {
            if (pending.task.connectionId == connectionId) {
                commandIds.push_back(commandId);
            }
        }
        return commandIds;
    }

    // 广播子任务全部结束后聚合父命令；可重试的失败重新入队，其余发布结果并确认输入条目。
    ruvia::task<void> finish(std::string_view commandId, bool success, std::string_view reason, std::string_view remoteAddress) {
        if (commandId.empty()) {
            co_return;
        }
        const auto child = broadcastParents_.find(commandId);
        if (child != broadcastParents_.end()) {
            const auto parentId = child->second;
            broadcastParents_.erase(child);
            const auto broadcast = broadcasts_.find(parentId);
            if (broadcast == broadcasts_.end()) {
                co_return;
            }
            service::packet_log::Context childContext;
            childContext.workerIndex = identity_.index;
            childContext.operation = "broadcast";
            childContext.messageId = commandId;
            childContext.causationId = parentId;
            service::packet_log::write(
                success ? service::packet_log::Level::Debug : service::packet_log::Level::Warn,
                success ? "BROADCAST_TARGET_SUCCESS" : "BROADCAST_TARGET_FAILED",
                childContext,
                {},
                reason
            );
            broadcast->second.anySuccess = broadcast->second.anySuccess || success;
            if (!reason.empty()) {
                broadcast->second.reason = reason;
            }
            if (--broadcast->second.remaining != 0) {
                co_return;
            }
            const auto aggregateSuccess = broadcast->second.anySuccess;
            const auto aggregateReason = broadcast->second.reason.empty()
                ? std::string("discovery_window_closed")
                : broadcast->second.reason;
            broadcasts_.erase(broadcast);
            service::packet_log::Context aggregateContext;
            aggregateContext.workerIndex = identity_.index;
            aggregateContext.operation = "broadcast";
            aggregateContext.messageId = parentId;
            service::packet_log::write(
                aggregateSuccess ? service::packet_log::Level::Info : service::packet_log::Level::Warn,
                aggregateSuccess ? "BROADCAST_COMPLETED" : "BROADCAST_FAILED",
                aggregateContext,
                {},
                aggregateReason
            );
            co_await finish(parentId, aggregateSuccess, aggregateReason, {});
            co_return;
        }
        const auto current = pending_.find(commandId);
        if (current == pending_.end()) {
            co_return;
        }
        const auto pending = current->second;
        const std::string id(commandId);
        if (!success && co_await retry(pending.stream, pending.entryId, pending.task, reason, remoteAddress)) {
            pending_.erase(id);
            co_return;
        }
        service::packet_log::write(
            success ? service::packet_log::Level::Info : service::packet_log::Level::Warn,
            pending.task.kind == "discovery"
                ? (success ? "DISCOVERY_RESULT" : "DISCOVERY_FAILED")
                : (success ? "COMMAND_RESULT" : "COMMAND_FAILED"),
            logContext(pending.task, remoteAddress),
            {},
            reason
        );
        co_await finalize(pending.stream, pending.entryId, id, pending.task, success, reason, remoteAddress);
        pending_.erase(id);
    }

    // 未能交给协议会话的命令：可重试则重新入队，否则直接终结。
    ruvia::task<void> failUndeliverable(std::string_view stream, std::string_view entryId, const message::ProtocolTask& task, std::string_view reason, std::string_view remoteAddress) {
        if (co_await retry(stream, entryId, task, reason, remoteAddress)) {
            co_return;
        }
        co_await finalize(stream, entryId, task.messageId, task, false, reason, remoteAddress);
    }

  private:
    struct Pending {
        std::string stream;
        std::string entryId;
        message::ProtocolTask task;
    };

    struct Broadcast {
        std::size_t remaining = 0;
        bool anySuccess = false;
        std::string reason;
    };

    [[nodiscard]] service::packet_log::Context logContext(const message::ProtocolTask& task, std::string_view remoteAddress) const noexcept {
        service::packet_log::Context context;
        context.workerIndex = identity_.index;
        context.operation = task.kind;
        context.protocol = task.protocol;
        context.linkId = task.linkId;
        context.deviceId = task.deviceId;
        context.deviceCode = task.deviceCode;
        context.connectionId = task.connectionId;
        context.remoteAddress = remoteAddress;
        context.messageId = task.messageId;
        context.causationId = task.causationId;
        context.sessionEpoch = task.sessionEpoch;
        return context;
    }

    ruvia::task<bool> retry(std::string_view stream, std::string_view entryId, message::ProtocolTask task, std::string_view reason, std::string_view remoteAddress) {
        if (!message::command::retryableFailure(reason) || task.attempt >= task.maxAttempts) {
            co_return false;
        }
        ++task.attempt;
        const auto retryReason = "attempt=" + std::to_string(task.attempt) + " reason=" + std::string(reason);
        service::packet_log::write(
            service::packet_log::Level::Warn,
            task.kind == "discovery" ? "DISCOVERY_RETRY" : "COMMAND_RETRY",
            logContext(task, remoteAddress),
            {},
            retryReason
        );
        (void)co_await service::messaging::stream::publishAndAcknowledge(
            redis_,
            stream,
            message::protocolTaskFields(task),
            kCommandStreamCapacity,
            stream,
            group_,
            consumer_,
            entryId,
            std::nullopt,
            identity_.instanceId
        );
        co_return true;
    }

    [[nodiscard]] std::vector<message::StreamField>
    resultFields(std::string_view commandId, const message::ProtocolTask& task, bool success, std::string_view reason) {
        const auto completedAt = std::to_string(message::utcNowMilliseconds());
        return { { "message_id", service::utils::nextUuid(uuidState_) },
                 { "causation_id", std::string(commandId) },
                 { "command_id", std::string(commandId) },
                 { "device_id", task.deviceId },
                 { "device_code", task.deviceCode },
                 { "protocol", task.protocol },
                 { "attempt", std::to_string(task.attempt) },
                 { "success", success ? "1" : "0" },
                 { "reason", std::string(reason) },
                 { "worker_id", std::to_string(identity_.index) },
                 { "created_at_ms", completedAt },
                 { "completed_at_ms", completedAt } };
    }

    ruvia::task<void> finalize(std::string_view inputStream, std::string_view inputEntryId, std::string_view commandId, const message::ProtocolTask& task, bool success, std::string_view reason, std::string_view remoteAddress) {
        auto results = resultFields(commandId, task, success, reason);
        const auto resultStream = message::commandResultStream();
        if (success) {
            (void)co_await service::messaging::stream::publishAndAcknowledge(
                redis_,
                resultStream,
                results,
                kCommandResultCapacity,
                inputStream,
                group_,
                consumer_,
                inputEntryId,
                service::messaging::stream::StreamWake{ std::nullopt, message::WorkerStreamTask::CommandResult },
                identity_.instanceId
            );
            co_return;
        }

        service::packet_log::write(
            service::packet_log::Level::Error,
            "DEAD_LETTER",
            logContext(task, remoteAddress),
            {},
            reason
        );
        auto deadFields = deadLetterFields(message::protocolTaskFields(task), inputEntryId, reason, {}, identity_.index);
        const std::array publications{
            service::messaging::stream::StreamPublication{ deadLetterStream_, deadFields, kDeadLetterCapacity },
            service::messaging::stream::StreamPublication{
                resultStream,
                results,
                kCommandResultCapacity,
                service::messaging::stream::StreamWake{ std::nullopt, message::WorkerStreamTask::CommandResult } }
        };
        (void)co_await service::messaging::stream::publishAllAndAcknowledge(
            redis_,
            publications,
            inputStream,
            group_,
            consumer_,
            inputEntryId,
            identity_.instanceId
        );
    }

    ruvia::redis_handle redis_;
    service::common::UuidV7State& uuidState_;
    const service::common::WorkerIdentity& identity_;
    std::string group_;
    std::string consumer_;
    std::string deadLetterStream_;
    std::map<std::string, Pending, std::less<>> pending_;
    std::map<std::string, Broadcast, std::less<>> broadcasts_;
    std::map<std::string, std::string, std::less<>> broadcastParents_;
};

// 派生值计算：按设备缓存已解析的派生配置（快照更新时只重建原文变化的设备），
// 对采集样本追加派生值，并在到期时为设备生成派生样本。实例归所属 Collector Worker；
// 到期时刻的调度与取消由 Worker 驱动。
class CollectorDerivedService final {
  public:
    struct Outcome {
        bool published = false;
        std::optional<std::int64_t> nextDeadline;
        // 生成本结果的配置版本；0 表示设备没有生效的派生配置，不再调度到期。
        std::uint64_t revision = 0;
    };

    CollectorDerivedService(ruvia::redis_handle redis, service::common::UuidV7State& uuidState, const common::WorkerIdentity& identity)
        : redis_(std::move(redis)), uuidState_(uuidState), identity_(identity) {}

    CollectorDerivedService(const CollectorDerivedService&) = delete;
    CollectorDerivedService& operator=(const CollectorDerivedService&) = delete;

    void reload(const RuntimeSnapshot& snapshot) {
        std::map<std::string, Entry, std::less<>> next;
        for (const auto& device : snapshot.devices) {
            if (device.calculationConfig.empty()) {
                continue;
            }
            if (const auto previous = calculations_.find(device.id);
                previous != calculations_.end() && previous->second.calculation->configuration == device.calculationConfig) {
                next.insert_or_assign(device.id, previous->second);
                continue;
            }
            try {
                if (auto prepared = telemetry::derived::DerivedService::prepare(device.id, device.calculationConfig)) {
                    next.insert_or_assign(device.id, Entry{ std::make_shared<const telemetry::derived::DeviceCalculation>(std::move(*prepared)), ++revision_ });
                }
            } catch (const std::exception& error) {
                LOG_WARN << "[Collector] derived_config_invalid: device_id=" << device.id << " " << error.what();
            }
        }
        calculations_ = std::move(next);
    }

    // 把派生值合入采集样本并与计算状态一起发布；published 为 false 时调用方自行发布原样本。
    ruvia::task<Outcome> append(message::ParsedDeviceMessage& parsed) {
        const auto current = calculations_.find(parsed.deviceId);
        if (current == calculations_.end()) {
            co_return Outcome{};
        }
        // 持有本次使用的解析结果；等待 Redis 期间快照更新不会使其失效。
        const auto entry = current->second;
        const auto result = co_await telemetry::derived::DerivedService::append(redis_, *entry.calculation, parsed, identity_.instanceId);
        co_return Outcome{ result.published, result.nextDeadline, entry.revision };
    }

    // 输入或窗口到期时为设备生成派生样本；revision 与当前配置不一致（配置已更新）时不再生效。
    ruvia::task<Outcome> expire(const DeviceDefinition& device, std::string_view connectionId, std::optional<std::uint64_t> revision) {
        const auto current = calculations_.find(device.id);
        if (current == calculations_.end() || (revision && current->second.revision != *revision)) {
            co_return Outcome{};
        }
        const auto entry = current->second;
        message::ParsedDeviceMessage parsed;
        parsed.messageId = service::utils::nextUuid(uuidState_);
        parsed.acquisitionId = parsed.messageId;
        parsed.causationId = parsed.messageId;
        parsed.linkId = device.linkId;
        parsed.deviceId = device.id;
        parsed.modelId = device.modelId;
        parsed.deviceCode = device.code;
        parsed.protocol = device.protocol;
        parsed.connectionId = connectionId;
        parsed.observedAtMs = parsed.occurredAtMs = message::utcNowMilliseconds();
        parsed.storagePolicy = device.storagePolicy;
        parsed.onlineWindowMs = std::clamp<std::int64_t>(device.onlineTimeout, 1, 86400) * 1000;
        parsed.source = "derived";
        parsed.valuesJson = "{\"values\":{}}";
        Outcome outcome{ .revision = entry.revision };
        try {
            const auto result = co_await telemetry::derived::DerivedService::append(redis_, *entry.calculation, parsed, identity_.instanceId, true);
            outcome.published = result.published;
            outcome.nextDeadline = result.nextDeadline;
        } catch (const std::exception& error) {
            LOG_WARN << "[Collector] derived_expiry_failed: " << error.what();
            outcome.nextDeadline = message::utcNowMilliseconds() + 1000;
        }
        co_return outcome;
    }

  private:
    struct Entry {
        std::shared_ptr<const telemetry::derived::DeviceCalculation> calculation;
        std::uint64_t revision = 0;
    };

    ruvia::redis_handle redis_;
    service::common::UuidV7State& uuidState_;
    const common::WorkerIdentity& identity_;
    std::map<std::string, Entry, std::less<>> calculations_;
    std::uint64_t revision_ = 0;
};

class CollectorStateService final {
  public:
    template <typename Redis>
    static ruvia::task<DeviceRouteOwner> bindRoute(const Redis& redis, const ProtocolAction& action, std::uint64_t sessionEpoch, std::size_t workerIndex, std::string_view workerInstanceId, std::string linkId, std::string_view instanceId) {
        const auto key = message::collector::connectionKey(action.deviceId);
        const auto runtimeKey = service::message::telemetry::runtimeKey(action.deviceId);
        const auto ownerKey = message::collector::linkOwnerKey(linkId);
        const auto revisionKey = message::live::channel(message::live::topic::kDeviceState);
        const auto worker = std::to_string(workerIndex);
        const auto epoch = std::to_string(sessionEpoch);
        const auto now = std::to_string(message::utcNowMilliseconds());
        const auto ttl = std::to_string(message::collector::kConnectionTtlMilliseconds);
        const std::string_view keys[]{ key, runtimeKey, revisionKey, ownerKey, message::collector::kConnectionDeadlinesKey };
        const std::string_view args[]{
            action.deviceId,
            action.deviceCode,
            worker,
            action.connectionId,
            epoch,
            now,
            instanceId,
            linkId,
            workerInstanceId,
            ttl
        };
        const auto reply = co_await redis.eval(CollectorDeviceRouteRecord::bindScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::array || reply.array().size() != 3) {
            utils::redis::throwValue("bind device route", reply);
        }
        const auto oldWorker = reply.array()[0].kind() == ruvia::redis_value::kind_type::string
            ? std::string(reply.array()[0].string())
            : std::string{};
        const auto oldConnection = reply.array()[1].kind() == ruvia::redis_value::kind_type::string
            ? std::string(reply.array()[1].string())
            : std::string{};
        const auto oldInstance = std::string(reply.array()[2].string());
        co_await service::messaging::stream::wakeWorker(redis, std::nullopt, message::WorkerStreamTask::Freshness, instanceId);
        co_return DeviceRouteOwner{ oldWorker, oldConnection, oldInstance };
    }

    template <typename Redis>
    static ruvia::task<void> markDeviceOffline(const Redis& redis, std::string_view deviceId, std::string_view connectionId, std::uint64_t sessionEpoch, std::size_t workerIndex, std::string_view workerInstanceId, std::string_view reason, std::string_view instanceId) {
        const auto key = message::collector::connectionKey(deviceId);
        const auto runtimeKey = service::message::telemetry::runtimeKey(deviceId);
        const auto revisionKey = message::live::channel(message::live::topic::kDeviceState);
        const auto now = std::to_string(message::utcNowMilliseconds());
        const auto worker = std::to_string(workerIndex);
        const auto epoch = std::to_string(sessionEpoch);
        const std::string_view keys[]{ key, runtimeKey, revisionKey, message::collector::kConnectionDeadlinesKey };
        const std::string_view args[]{ instanceId, worker, workerInstanceId, connectionId, epoch, reason, now, deviceId };
        const auto reply = co_await redis.eval(CollectorDeviceRouteRecord::eraseScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            utils::redis::throwValue("erase device route", reply);
        }
    }

    template <typename Redis>
    static ruvia::task<bool> renewRoute(const Redis& redis, std::string_view deviceId, std::string_view connectionId, std::uint64_t sessionEpoch, std::size_t workerIndex, std::string_view workerInstanceId, std::string_view instanceId) {
        const auto key = message::collector::connectionKey(deviceId);
        const auto worker = std::to_string(workerIndex);
        const auto epoch = std::to_string(sessionEpoch);
        const auto ttl = std::to_string(message::collector::kConnectionTtlMilliseconds);
        const std::string_view keys[]{ key, message::collector::kConnectionDeadlinesKey };
        const std::string_view args[]{ instanceId, worker, workerInstanceId, connectionId, epoch, ttl, message::collector::kLinkOwnerPrefix, deviceId };
        const auto reply = co_await redis.eval(CollectorDeviceRouteRecord::renewScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            utils::redis::throwValue("renew device route", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<void> eraseLegacyRoutes(const Redis& redis, const RuntimeSnapshot& snapshot, std::string_view instanceId) {
        const auto now = std::to_string(message::utcNowMilliseconds());
        const auto changes = message::live::channel(message::live::topic::kDeviceState);
        for (const auto& device : snapshot.devices) {
            const auto key = service::message::telemetry::runtimeKey(device.id);
            const auto owner = message::collector::linkOwnerKey(device.linkId);
            const std::string_view keys[]{ key, owner, changes };
            const std::string_view args[]{ instanceId, device.linkId, now, device.id };
            const auto reply = co_await redis.eval(CollectorDeviceRouteRecord::eraseLegacyScript, keys, args);
            if (reply.kind() != ruvia::redis_value::kind_type::integer) {
                utils::redis::throwValue("erase legacy device route", reply);
            }
        }
    }

    template <typename Redis>
    static ruvia::task<void> eraseLegacyLinkSnapshots(const Redis& redis, const RuntimeSnapshot& snapshot, std::string_view instanceId) {
        for (const auto& link : snapshot.links) {
            const auto prefix = message::collector::linkRuntimePrefix(link.id);
            const auto ownerKey = message::collector::linkOwnerKey(link.id);
            for (const auto& key : co_await utils::redis::keysMatching(redis, prefix + '*')) {
                const auto separator = key.find_last_of(':');
                if (separator == std::string::npos || separator <= prefix.size()) {
                    continue;
                }
                const auto instance = std::string_view(key).substr(prefix.size(), separator - prefix.size());
                if (instance == instanceId) {
                    continue;
                }
                const std::string_view keys[]{ key, ownerKey };
                const std::string_view args[]{ instanceId, instance };
                const auto reply = co_await redis.eval(CollectorLinkRecord::eraseLegacyScript, keys, args);
                if (reply.kind() != ruvia::redis_value::kind_type::integer) {
                    utils::redis::throwValue("erase legacy link snapshot", reply);
                }
            }
        }
    }

    template <typename Redis>
    static ruvia::task<void> storeLinkEvent(const Redis& redis, const message::StreamMessage& event, std::size_t workerIndex, std::string_view instanceId) {
        const auto record = CollectorLinkRecord::fromEvent(event, message::utcNowMilliseconds());
        const auto key =
            CollectorLinkRecord::key(event.get("link_id"), workerIndex, instanceId);
        co_await storeSnapshot(redis, key, record.fields);
    }

    template <typename Redis>
    static ruvia::task<void> publishWorker(const Redis& redis, std::size_t workerIndex, std::string_view workerInstanceId, std::string_view version, std::string_view instanceId) {
        const CollectorWorkerRecord record{
            workerIndex,
            std::string(version),
            "applied",
            message::utcNowMilliseconds(),
            std::string(workerInstanceId)
        };
        co_await storeSnapshot(redis, CollectorWorkerRecord::key(workerIndex, instanceId), record.fields());
    }

    template <typename Redis>
    static ruvia::task<void> renewWorkerSnapshot(const Redis& redis, std::size_t workerIndex, std::string_view workerInstanceId, std::string_view version, std::string_view instanceId) {
        const CollectorWorkerRecord record{ workerIndex, std::string(version), "applied", message::utcNowMilliseconds(), std::string(workerInstanceId) };
        const auto key = CollectorWorkerRecord::key(workerIndex, instanceId);
        co_await retainSnapshot(redis, key, workerInstanceId, record.fields());
    }

    template <typename Redis>
    static ruvia::task<void> retainLinkSnapshot(const Redis& redis, const message::StreamMessage& event, std::size_t workerIndex, std::string_view instanceId) {
        const auto record = CollectorLinkRecord::fromEvent(event, message::utcNowMilliseconds());
        const auto key = CollectorLinkRecord::key(event.get("link_id"), workerIndex, instanceId);
        co_await retainSnapshot(redis, key, event.get("worker_instance_id"), record.fields);
    }

    template <typename Redis>
    static ruvia::task<void> eraseWorker(const Redis& redis, std::size_t index, std::string_view workerInstanceId, std::string_view instanceId) {
        const auto keys = co_await utils::redis::keysMatching(
            redis,
            CollectorLinkRecord::key("*", index, instanceId)
        );
        for (const auto& key : keys) {
            co_await eraseSnapshot(redis, key, workerInstanceId);
        }
        co_await eraseSnapshot(redis, CollectorWorkerRecord::key(index, instanceId), workerInstanceId);
    }

    template <typename Redis>
    static ruvia::task<void> eraseLink(const Redis& redis, std::string_view id, std::size_t index, std::string_view workerInstanceId, std::string_view instanceId) {
        co_await eraseSnapshot(redis, CollectorLinkRecord::key(id, index, instanceId), workerInstanceId);
    }

  private:
    template <typename Redis>
    static ruvia::task<void> retainSnapshot(const Redis& redis, const std::string& key, std::string_view workerInstanceId, const std::vector<message::StreamField>& fields) {
        std::vector<std::string> args{ std::to_string(message::collector::kConnectionTtlMilliseconds),
                                       std::string(workerInstanceId) };
        args.reserve(2 + fields.size() * 2);
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const std::vector<std::string_view> views(args.begin(), args.end());
        const std::string_view keys[]{ key };
        const auto reply = co_await redis.eval(CollectorWorkerRecord::retainScript, keys, views);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            utils::redis::throwValue("retain collector snapshot", reply);
        }
    }

    template <typename Redis>
    static ruvia::task<void> eraseSnapshot(const Redis& redis, const std::string& key, std::string_view workerInstanceId) {
        const std::string_view keys[]{ key };
        const std::string_view args[]{ workerInstanceId };
        const auto reply = co_await redis.eval(CollectorWorkerRecord::eraseScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            utils::redis::throwValue("erase collector snapshot", reply);
        }
    }

    template <typename Redis>
    static ruvia::task<void> storeSnapshot(const Redis& redis, const std::string& key, const std::vector<message::StreamField>& fields) {
        std::vector<std::string> args{ std::to_string(message::collector::kConnectionTtlMilliseconds) };
        args.reserve(1 + fields.size() * 2);
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const std::vector<std::string_view> views(args.begin(), args.end());
        const std::string_view keys[]{ key };
        const auto reply = co_await redis.eval(CollectorWorkerRecord::storeScript, keys, views);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            utils::redis::throwValue("store collector snapshot", reply);
        }
    }
};

// 本 Worker 的报文诊断与调试业务；借用的快照、索引与连接状态始终由所属 Worker 更新。
class CollectorPacketService final {
  public:
    CollectorPacketService(std::size_t workerIndex, const RuntimeSnapshot& snapshot, const std::map<std::string, std::size_t, std::less<>>& linkIndexes, const std::map<std::string, ProtocolConnectionInfo, std::less<>>& connections, const std::map<std::string, std::set<std::string>, std::less<>>& routes)
        : workerIndex_(workerIndex), loadedSnapshot_(snapshot), linkIndexes_(linkIndexes),
          networkConnections_(connections), routes_(routes) {}

    [[nodiscard]] const LinkDefinition* findLink(std::string_view linkId) const noexcept {
        const auto current = linkIndexes_.find(linkId);
        return current == linkIndexes_.end() ? nullptr : &loadedSnapshot_.links[current->second];
    }

    [[nodiscard]] std::string_view protocolForLink(std::string_view linkId) const noexcept {
        const auto* link = findLink(linkId);
        return link ? std::string_view(link->protocol) : std::string_view{};
    }

    [[nodiscard]] std::string deviceCodesForConnection(std::string_view connectionId) const {
        const auto current = routes_.find(connectionId);
        if (current == routes_.end()) {
            return {};
        }
        std::string result;
        for (const auto& deviceCode : current->second) {
            if (!result.empty()) {
                result.push_back(',');
            }
            result += deviceCode;
        }
        return result;
    }

    [[nodiscard]] bool captureBytes() const noexcept {
        return packet_log::enabled(packet_log::Level::Debug);
    }

    void discoveryRequested(const message::ProtocolTask& task) const {
        if (task.kind != "discovery" || !packet_log::enabled(packet_log::Level::Info)) {
            return;
        }
        const auto payload = message::fromHex(task.payload);
        packet_log::write(packet_log::Level::Info, "DISCOVERY_REQUEST", taskLogContext(task), payload);
    }

    void discoveryUnavailable(const message::ProtocolTask& task) const {
        packet_log::write(packet_log::Level::Warn, "DISCOVERY_FAILED", taskLogContext(task), {}, "discovery_no_connections");
    }

    void broadcastStarted(const message::ProtocolTask& task, std::size_t targets) const {
        const auto reason = "targets=" + std::to_string(targets);
        packet_log::write(packet_log::Level::Info, "BROADCAST_START", taskLogContext(task), {}, reason);
    }

    void broadcastTarget(const message::ProtocolTask& task, std::string_view connectionId, std::string_view childId) const {
        auto context = taskLogContext(task, connectionId);
        context.causationId = task.messageId;
        context.messageId = childId;
        packet_log::write(packet_log::Level::Debug, "BROADCAST_TARGET", context);
    }

    void discoveryStarted(const message::ProtocolTask& task) const {
        if (task.kind == "discovery") {
            packet_log::write(packet_log::Level::Info, "DISCOVERY_START", taskLogContext(task, task.connectionId));
        }
    }

    void ingressReceived(const IngressPacket& packet) const {
        if (!captureBytes()) {
            return;
        }
        const auto codes = deviceCodesForConnection(packet.connectionId);
        packet_log::write(packet_log::Level::Debug, "RX_BYTES", ingressLogContext(packet, codes), packet.payload);
    }

    void ingressDropped(const IngressPacket& packet) const {
        if (!packet_log::enabled(packet_log::Level::Error)) {
            return;
        }
        const auto codes = deviceCodesForConnection(packet.connectionId);
        packet_log::write(packet_log::Level::Error, "RX_DROPPED", ingressLogContext(packet, codes), packet.payload, "raw_ingress_backpressure");
    }

    void connectionOpened(const ProtocolConnectionInfo& info) const {
        packet_log::write(packet_log::Level::Info, "CONNECTED", connectionLogContext(info));
    }

    void connectionClosed(const ProtocolConnectionInfo& info, std::string_view reason) const {
        packet_log::write(packet_log::Level::Warn, "DISCONNECTED", connectionLogContext(info), {}, reason);
    }

    void staleConnection(const ProtocolConnectionInfo& info) const {
        packet_log::write(packet_log::Level::Warn, "RX_REJECTED", connectionLogContext(info), {}, "stale_session_epoch");
    }

    void ingressRejected(const IngressPacket& packet) const {
        if (!packet_log::enabled(packet_log::Level::Warn)) {
            return;
        }
        const auto codes = deviceCodesForConnection(packet.connectionId);
        packet_log::write(packet_log::Level::Warn, "RX_REJECTED", ingressLogContext(packet, codes), packet.payload, "stale_session_epoch");
    }

    void ingressFailed(const IngressPacket& packet, std::string_view failure) const {
        if (!packet_log::enabled(packet_log::Level::Error)) {
            return;
        }
        const auto codes = deviceCodesForConnection(packet.connectionId);
        packet_log::write(packet_log::Level::Error, "PARSE_ERROR", ingressLogContext(packet, codes), packet.payload, failure);
    }

    void egressInvalid(const message::StreamMessage& message, std::string_view reason) const {
        packet_log::Context context;
        context.workerIndex = workerIndex_;
        context.direction = "TX";
        context.operation = "decode_egress";
        context.connectionId = message.get("connection_id");
        context.messageId = message.get("message_id");
        context.causationId = message.get("causation_id");
        packet_log::write(packet_log::Level::Error, "TX_REJECTED", context, {}, reason);
    }

    void egressRejected(const message::EgressPacket& packet, std::string_view reason) const {
        packet_log::Context context;
        context.workerIndex = workerIndex_;
        context.direction = "TX";
        context.operation = "transport";
        context.connectionId = packet.connectionId;
        context.messageId = packet.messageId;
        context.causationId = packet.causationId;
        context.sessionEpoch = packet.sessionEpoch;
        packet_log::write(packet_log::Level::Warn, "TX_REJECTED", context, {}, reason);
    }

    void sending(const EgressLogContext& egress) const {
        packet_log::write(packet_log::Level::Debug, "TX_BYTES", egressLogContext(egress), egress.debugPayload);
    }

    void sent(const EgressLogContext& egress, bool success) const {
        packet_log::write(success ? packet_log::Level::Debug : packet_log::Level::Error, success ? "TX_SUCCESS" : "TX_FAILED", egressLogContext(egress), {}, success ? std::string_view{} : std::string_view("socket_write_failed"));
    }

    void protocolClosed(const ProtocolAction& action) const {
        packet_log::write(packet_log::Level::Warn, hasMarker(action.reason, "timeout") ? "TIMEOUT" : "PROTOCOL_CLOSE", actionLogContext(action, "protocol"), {}, action.reason);
    }

    void deviceBound(const ProtocolAction& action) const {
        packet_log::write(packet_log::Level::Info, "DEVICE_BOUND", actionLogContext(action, "bind"));
    }

    void parsed(const message::ParsedDeviceMessage& parsed) const {
        if (captureBytes()) {
            packet_log::write(packet_log::Level::Debug, "PARSE_SUCCESS", parsedLogContext(parsed), {}, parsed.source);
        }
    }

    void commandCompleted(const ProtocolAction& action, const message::ProtocolTask* task, bool success) const {
        auto context = task ? taskLogContext(*task, action.connectionId) : actionLogContext(action, "command");
        context.causationId = action.commandId;
        const auto event = success
            ? (task && task->kind == "discovery" ? "DISCOVERY_COMPLETED" : "COMMAND_COMPLETED")
            : hasMarker(action.reason, "timeout") ? "TIMEOUT"
                                                  : (task && task->kind == "discovery" ? "DISCOVERY_FAILED" : "COMMAND_FAILED");
        packet_log::write(success ? packet_log::Level::Info : packet_log::Level::Warn, event, context, {}, action.reason);
    }

    void deadlineFired(std::string_view connectionId, std::uint64_t token) const {
        ProtocolAction diagnostic;
        diagnostic.connectionId = connectionId;
        const auto reason = std::to_string(token);
        packet_log::write(packet_log::Level::Debug, "DEADLINE_FIRED", actionLogContext(diagnostic, "deadline"), {}, reason);
    }

    void deadlineUnresolved(std::string_view connectionId) const {
        ProtocolAction diagnostic;
        diagnostic.connectionId = connectionId;
        packet_log::write(packet_log::Level::Warn, "TIMEOUT", actionLogContext(diagnostic, "deadline"), {}, "deadline_expired_without_protocol_action");
    }

    template <typename Redis>
    static ruvia::task<void> completeAcquisition(const Redis& redis, const ProtocolAction& action) {
        if (action.kind == ProtocolActionKind::FinishAcquisition) {
            co_await packet_log::DebugPacketService::finishAcquisition(redis, action.acquisitionId, action.reason);
        } else if (!action.commandId.empty()) {
            co_await packet_log::DebugPacketService::finishAcquisition(redis, action.commandId, action.kind == ProtocolActionKind::CompleteCommand ? "success" : "failed");
        }
    }

    template <typename Redis>
    static ruvia::task<void> recordDebugPacket(const Redis& redis, service::common::UuidV7State& uuidState, const LinkDefinition& link, const DeviceDefinition* device, std::string_view direction, std::string_view address, std::span<const std::uint8_t> bytes, std::int64_t timestamp, bool deviceOnly, std::string_view eventId, std::string_view status, std::string_view reason, std::string_view parsedJson, std::string_view replyToPacketId, std::string_view acquisitionId) {
        if (!link.debugEnabled && !(device && device->debugEnabled)) {
            co_return;
        }
        try {
            co_await packet_log::DebugPacketService::recordPacket(redis, uuidState, link.id, device ? std::string_view(device->id) : std::string_view{}, direction, "collector", address, bytes, timestamp, link.debugEnabled && !deviceOnly, device && device->debugEnabled, eventId, status, reason, parsedJson, replyToPacketId, false, 0, acquisitionId);
        } catch (const std::exception& error) {
            LOG_WARN << "[Collector] debug_packet_failed: " << error.what();
        }
    }

  private:
    [[nodiscard]] service::packet_log::Context
    ingressLogContext(const IngressPacket& packet, std::string_view deviceCodes = {}) const noexcept {
        service::packet_log::Context context;
        context.workerIndex = workerIndex_;
        context.direction = "RX";
        context.operation = "transport";
        context.protocol = protocolForLink(packet.linkId);
        context.linkId = packet.linkId;
        context.deviceCode = deviceCodes;
        context.connectionId = packet.connectionId;
        context.remoteAddress = packet.remoteAddress;
        context.messageId = packet.messageId;
        context.sessionEpoch = packet.sessionEpoch;
        return context;
    }

    [[nodiscard]] service::packet_log::Context
    connectionLogContext(const ProtocolConnectionInfo& info) const noexcept {
        service::packet_log::Context context;
        context.workerIndex = workerIndex_;
        context.operation = "connection";
        context.protocol = protocolForLink(info.linkId);
        context.linkId = info.linkId;
        context.connectionId = info.connectionId;
        context.remoteAddress = info.remoteAddress;
        context.sessionEpoch = info.sessionEpoch;
        return context;
    }

    [[nodiscard]] service::packet_log::Context
    taskLogContext(const message::ProtocolTask& task, std::string_view connectionId = {}) const noexcept {
        service::packet_log::Context context;
        context.workerIndex = workerIndex_;
        context.operation = task.kind;
        context.protocol = task.protocol;
        context.linkId = task.linkId;
        context.deviceId = task.deviceId;
        context.deviceCode = task.deviceCode;
        context.connectionId = connectionId.empty() ? task.connectionId : connectionId;
        context.messageId = task.messageId;
        context.causationId = task.causationId;
        context.sessionEpoch = task.sessionEpoch;
        const auto network = networkConnections_.find(context.connectionId);
        if (network != networkConnections_.end()) {
            context.remoteAddress = network->second.remoteAddress;
        }
        return context;
    }

    [[nodiscard]] service::packet_log::Context
    actionLogContext(const ProtocolAction& action, std::string_view operation = {}) const noexcept {
        service::packet_log::Context context;
        context.workerIndex = workerIndex_;
        context.operation = operation;
        context.deviceId = action.deviceId;
        context.deviceCode = action.deviceCode;
        context.connectionId = action.connectionId;
        context.causationId = action.commandId;
        const auto network = networkConnections_.find(action.connectionId);
        if (network != networkConnections_.end()) {
            context.linkId = network->second.linkId;
            context.remoteAddress = network->second.remoteAddress;
            context.sessionEpoch = network->second.sessionEpoch;
            context.protocol = protocolForLink(network->second.linkId);
        }
        return context;
    }

    [[nodiscard]] service::packet_log::Context
    parsedLogContext(const message::ParsedDeviceMessage& parsed) const noexcept {
        service::packet_log::Context context;
        context.workerIndex = workerIndex_;
        context.direction = "RX";
        context.operation = "parse";
        context.protocol = parsed.protocol;
        context.linkId = parsed.linkId;
        context.deviceId = parsed.deviceId;
        context.deviceCode = parsed.deviceCode;
        context.connectionId = parsed.connectionId;
        context.messageId = parsed.messageId;
        context.causationId = parsed.causationId;
        const auto network = networkConnections_.find(parsed.connectionId);
        if (network != networkConnections_.end()) {
            context.remoteAddress = network->second.remoteAddress;
            context.sessionEpoch = network->second.sessionEpoch;
        }
        return context;
    }

    [[nodiscard]] service::packet_log::Context
    egressLogContext(const EgressLogContext& egress) const noexcept {
        service::packet_log::Context context;
        context.workerIndex = workerIndex_;
        context.direction = "TX";
        context.operation = egress.operation;
        context.protocol = egress.protocol;
        context.linkId = egress.linkId;
        context.deviceId = egress.deviceId;
        context.deviceCode = egress.deviceCode;
        context.connectionId = egress.connectionId;
        context.remoteAddress = egress.remoteAddress;
        context.messageId = egress.messageId;
        context.causationId = egress.causationId;
        context.sessionEpoch = egress.sessionEpoch;
        return context;
    }

    [[nodiscard]] static bool hasMarker(std::string_view value, std::string_view marker) noexcept {
        return value.find(marker) != std::string_view::npos;
    }

    std::size_t workerIndex_;
    const RuntimeSnapshot& loadedSnapshot_;
    const std::map<std::string, std::size_t, std::less<>>& linkIndexes_;
    const std::map<std::string, ProtocolConnectionInfo, std::less<>>& networkConnections_;
    const std::map<std::string, std::set<std::string>, std::less<>>& routes_;
};

class CollectorTelemetryService final {
  public:
    template <typename Redis>
    static ruvia::task<void> publishParsed(const Redis& redis, const message::ParsedDeviceMessage& parsed, std::string_view instanceId) {
        (void)co_await service::messaging::stream::publishAndWake(redis, message::parsedStream(), message::parsedFields(parsed), std::nullopt, message::WorkerStreamTask::Telemetry, 10000, instanceId);
    }
};

} // namespace service::collector
