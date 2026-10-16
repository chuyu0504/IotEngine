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
#include "service/common/uuid.h"
#include "service/features/collector/collector.protocol.h"
#include "service/features/collector/collector.types.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/packet_log/packet_log.transport.h"
#include "service/features/telemetry/derived/derived.service.h"
#include "service/middleware/log.h"
#include "service/utils/number.h"

namespace service::collector::config {

namespace redis = service::message::redis;

using service::message::projection::kRuntimeActiveVersionKey;
using service::message::projection::realtimeDeviceKey;
using service::message::projection::realtimeDevicePointsKey;
using service::message::projection::kRuntimeVersionsKey;
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
        for (const auto& field : ConnectionConfig::fields(device)) { text(field.name); text(field.value); }
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
        redis::throwValue(operation, value);
    }
    return std::string(value.string());
}

inline std::int64_t integerReply(const ruvia::redis_value& value, std::string_view operation) {
    if (value.kind() != ruvia::redis_value::kind_type::integer) {
        redis::throwValue(operation, value);
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
        redis::throwValue(operation, value);
    }
    std::vector<std::string> result;
    result.reserve(value.array().size());
    for (const auto& entry : value.array()) {
        if (entry.kind() != ruvia::redis_value::kind_type::string) {
            redis::throwValue(operation, value);
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
        redis::throwValue(operation, value);
    }
    const auto values = value.array();
    if (values.size() % 2 != 0) {
        redis::throwValue(operation, value);
    }
    std::vector<message::StreamField> result;
    result.reserve(values.size() / 2);
    for (std::size_t index = 0; index < values.size(); index += 2) {
        if (values[index].kind() != ruvia::redis_value::kind_type::string ||
            values[index + 1].kind() != ruvia::redis_value::kind_type::string) {
            redis::throwValue(operation, value);
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
                redis::throwValue("runtime pipeline", reply);
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
            redis::throwValue("runtime batch", reply);
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
    if (result.sl651ResponseMode.empty()) result.sl651ResponseMode = "M1";
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
    if (result.parameters.empty()) result.parameters = "{}";
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
    if (result.positionMode.empty()) result.positionMode = "GUIDE";
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
        stringArray(co_await redis::command(redis, arguments), "SMEMBERS runtime")
    );
}

template <typename Redis>
ruvia::task<void> eraseSnapshot(const Redis& redis, std::string_view version) {
    const auto keysSet = prefix(version) + ":keys";
    const std::vector<std::string> keysArguments{ "SMEMBERS", keysSet };
    auto keys = stringArray(co_await redis::command(redis, keysArguments), "SMEMBERS runtime snapshot keys");
    keys.push_back(keysSet);
    constexpr std::size_t batchSize = 128;
    for (std::size_t offset = 0; offset < keys.size(); offset += batchSize) {
        std::vector<std::string> args{ "UNLINK" };
        const auto end = std::min(keys.size(), offset + batchSize);
        args.insert(args.end(), keys.begin() + static_cast<std::ptrdiff_t>(offset), keys.begin() + static_cast<std::ptrdiff_t>(end));
        (void)integerReply(co_await redis::command(redis, args), "UNLINK runtime snapshot keys");
    }
}

} // namespace detail

template <typename Redis>
ruvia::task<void> eraseExpiredSnapshots(const Redis& redis, std::int64_t nowMilliseconds, std::string_view preserveVersion) {
    const std::vector<std::string> expiredArguments{
        "ZRANGEBYSCORE", std::string(kRuntimeVersionsKey), "-inf", std::to_string(nowMilliseconds - kSnapshotGraceMilliseconds)
    };
    const auto expired = detail::stringArray(
        co_await redis::command(redis, expiredArguments),
        "ZRANGEBYSCORE runtime versions"
    );
    for (const auto& candidate : expired) {
        if (candidate == preserveVersion) {
            continue;
        }
        co_await detail::eraseSnapshot(redis, candidate);
        const std::vector<std::string> removeArguments{ "ZREM", std::string(kRuntimeVersionsKey), candidate };
        (void)detail::integerReply(
            co_await redis::command(redis, removeArguments),
            "ZREM runtime version"
        );
    }
}

template <typename Redis>
ruvia::task<void> cleanupExpiredSnapshots(const Redis& redis, std::int64_t nowMilliseconds) {
    const std::vector<std::string> activeReplyArguments{ "GET", std::string(kRuntimeActiveVersionKey) };
    const auto activeReply =
        co_await redis::command(redis, activeReplyArguments);
    if (activeReply.null()) {
        co_return;
    }
    co_await eraseExpiredSnapshots(
        redis,
        nowMilliseconds,
        detail::stringReply(activeReply, "GET active runtime")
    );
}

// Service projection. The version pointer is switched only after every readable
// Hash/Set/List has been written, so readers never observe a partial snapshot.
template <typename Redis>
ruvia::task<std::string> project(const Redis& redis, const RuntimeSnapshot& snapshot, service::common::UuidV7Generator& uuidGenerator) {
    const auto snapshotSignature = signature(snapshot);
    const std::vector<std::string> activeReplyArguments{ "GET", std::string(kRuntimeActiveVersionKey) };
    const auto activeReply =
        co_await redis::command(redis, activeReplyArguments);
    if (!activeReply.null()) {
        const auto active = detail::stringReply(activeReply, "GET active runtime");
        const std::vector<std::string> signatureReplyArguments{ "HGET", metadataKey(active), "signature" };
        const auto signatureReply =
            co_await redis::command(redis, signatureReplyArguments);
        if (!signatureReply.null() &&
            detail::stringReply(signatureReply, "HGET runtime signature") ==
                snapshotSignature) {
            co_return active;
        }
    }

    const auto version = uuidGenerator.next();
    const auto createdAt = message::utcNowMilliseconds();
    const std::vector<std::string> versionArguments{ "ZADD", std::string(kRuntimeVersionsKey), std::to_string(createdAt), version };
    (void)detail::integerReply(
        co_await redis::command(redis, versionArguments),
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
        detail::appendHash(commands, version, realtimeDeviceKey(version, device.id), { { "id", device.id }, { "code", device.code }, { "name", device.name } });
        detail::appendRealtimePoints(commands, version, realtimeDevicePointsKey(version, device.id), device.points);
        if (commands.size() >= pipelineSize) {
            co_await detail::flush(redis, commands);
        }
    }
    co_await detail::flush(redis, commands);

    const std::vector<std::string> previousReplyArguments{ "GET", std::string(kRuntimeActiveVersionKey) };
    const auto previousReply =
        co_await redis::command(redis, previousReplyArguments);
    if (!previousReply.null()) {
        const auto previous = detail::stringReply(previousReply, "GET active runtime");
        if (!previous.empty() && previous != version) {
            const std::vector<std::string> previousVersionArguments{ "ZADD", std::string(kRuntimeVersionsKey), std::to_string(createdAt), previous };
            (void)detail::integerReply(
                // The score is the time this version stopped being active, not its creation
                // time. Refresh it on every switch so readers that captured the old pointer
                // receive the complete grace window even after a long-lived generation.
                co_await redis::command(redis, previousVersionArguments),
                "ZADD previous runtime version"
            );
        }
    }
    const std::vector<std::string> activateArguments{ "SET", std::string(kRuntimeActiveVersionKey), version };
    detail::requireOk(
        co_await redis::command(redis, activateArguments),
        "SET active runtime version"
    );

    // A worker may have read the previous pointer just before this switch. Keep completed
    // snapshots for a bounded grace window so its multi-key load cannot be torn down midway.
    co_await eraseExpiredSnapshots(redis, createdAt, version);
    co_return version;
}

template <typename Redis>
ruvia::task<std::string> activeVersion(const Redis& redis) {
    const std::vector<std::string> versionReplyArguments{ "GET", std::string(kRuntimeActiveVersionKey) };
    const auto versionReply =
        co_await redis::command(redis, versionReplyArguments);
    if (versionReply.null()) {
        throw std::runtime_error("runtime is not ready");
    }
    co_return detail::stringReply(versionReply, "GET active runtime");
}

template <typename Redis>
ruvia::task<RuntimeSnapshot> load(const Redis& redis, std::string version) {
    RuntimeSnapshot snapshot;

    for (const auto& linkId : co_await detail::members(redis, prefix(version) + ":links")) {
        const auto fields = co_await redis::hashEntries(redis, linkKey(version, linkId));
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
                co_await redis::hashEntries(redis, targetKey(version, linkId, targetId));
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
ruvia::task<bool> isLinkOwner(const Redis& redis, std::string_view linkId,
                            std::string_view expectedOwner) {
    const std::vector<std::string> ownerArguments{"GET", key(linkId)};
    const auto owner = co_await message::redis::command(redis, ownerArguments);
    co_return owner.kind() == ruvia::redis_value::kind_type::string &&
        owner.string() == expectedOwner;
}

template <typename Redis>
ruvia::task<bool> retainTarget(const Redis& redis, std::string_view linkId, std::string_view targetId, std::string_view owner) {
    static constexpr std::string_view script = R"lua(
local current=redis.call('GET',KEYS[1])
if current and current~=ARGV[1] then return 0 end
redis.call('SET',KEYS[1],ARGV[1],'PX',15000)
return 1
)lua";
    const auto lease = targetKey(linkId, targetId);
    const std::string_view keys[]{ lease };
    const std::string_view args[]{ owner };
    const auto reply = co_await redis.eval(script, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        message::redis::throwValue("claim target ownership", reply);
    }
    co_return reply.integer() == 1;
}

template <typename Redis>
ruvia::task<bool> renewTarget(const Redis& redis, std::string_view linkId, std::string_view targetId, std::string_view owner) {
    static constexpr std::string_view script = R"lua(
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
return redis.call('PEXPIRE',KEYS[1],15000)
)lua";
    const auto lease = targetKey(linkId, targetId);
    const std::string_view keys[]{ lease };
    const std::string_view args[]{ owner };
    const auto reply = co_await redis.eval(script, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        message::redis::throwValue("renew target ownership", reply);
    }
    co_return reply.integer() == 1;
}

template <typename Redis>
ruvia::task<void> releaseTarget(const Redis& redis, std::string_view linkId, std::string_view targetId, std::string_view owner) {
    static constexpr std::string_view script = R"lua(
if redis.call('GET',KEYS[1])==ARGV[1] then
  redis.call('DEL',KEYS[1])
  return 1
end
return 0
)lua";
    const auto lease = targetKey(linkId, targetId);
    const std::string_view keys[]{ lease };
    const std::string_view args[]{ owner };
    const auto reply = co_await redis.eval(script, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        message::redis::throwValue("release target ownership", reply);
    }
}

template <typename Redis>
ruvia::task<RuntimeSnapshot> retain(const Redis& redis, RuntimeSnapshot snapshot) {
    static constexpr std::string_view script = R"lua(
local owner=redis.call('GET',KEYS[1])
if owner and owner~=ARGV[1] then return 0 end
redis.call('SET',KEYS[1],ARGV[1],'PX',15000)
return 1
)lua";
    std::set<std::string, std::less<>> owned;
    for (const auto& link : snapshot.links) {
        const auto lease = key(link.id);
        const std::string_view keys[]{ lease };
        const std::string_view args[]{ service::runtime::instanceId() };
        const auto reply = co_await redis.eval(script, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            message::redis::throwValue("claim link ownership", reply);
        }
        if (reply.integer() == 1) {
            owned.insert(link.id);
        }
    }
    std::erase_if(snapshot.links, [&](const auto& link) {
        return !owned.contains(link.id);
    });
    std::erase_if(snapshot.devices, [&](const auto& device) {
        return !owned.contains(device.linkId);
    });
    co_return snapshot;
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
// 终结时发布结果或死信并确认输入条目。实例归所属 Collector Worker，不持有连接或协议会话；
// Worker 只负责消费命令、驱动协议引擎并把引擎结果交给本服务。
class CollectorCommandService final {
  public:
    static constexpr std::size_t kCommandStreamCapacity = 10000;
    static constexpr std::size_t kCommandResultCapacity = 10000;

    CollectorCommandService(ruvia::redis_handle redis, service::common::UuidV7Generator& uuidGenerator, std::size_t workerIndex, std::string group, std::string consumer, std::string deadLetterStream)
        : redis_(std::move(redis)), uuidGenerator_(uuidGenerator), workerIndex_(workerIndex),
          group_(std::move(group)), consumer_(std::move(consumer)),
          deadLetterStream_(std::move(deadLetterStream)) {}

    CollectorCommandService(const CollectorCommandService&) = delete;
    CollectorCommandService& operator=(const CollectorCommandService&) = delete;

    template <typename Redis>
    static ruvia::task<TransmissionReservation> reserveTransmission(
        const Redis& redis, const message::ProtocolTask& task, std::int64_t nowMs) {
        if (nowMs - task.createdAtMs >= 60000)
            co_return TransmissionReservation::Expired;
        // 发送前的幂等认领锁：保留一天，避免同一命令被重复发送。
        const auto key = "iot:v2:command:sent:" + task.messageId;
        const std::vector<std::string> claimedArguments{"SET", key, "1", "NX", "EX", "86400"};
        const auto claimed = co_await message::redis::command(
            redis, claimedArguments);
        if (claimed.kind() == ruvia::redis_value::kind_type::null)
            co_return TransmissionReservation::Duplicate;
        if (claimed.kind() == ruvia::redis_value::kind_type::error)
            message::redis::throwValue("claim command transmission", claimed);
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
            childContext.workerIndex = workerIndex_;
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
            aggregateContext.workerIndex = workerIndex_;
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
        context.workerIndex = workerIndex_;
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
        (void)co_await message::redis::publishAndAcknowledge(
            redis_,
            stream,
            message::protocolTaskFields(task),
            kCommandStreamCapacity,
            stream,
            group_,
            consumer_,
            entryId
        );
        co_return true;
    }

    [[nodiscard]] std::vector<message::StreamField>
    resultFields(std::string_view commandId, const message::ProtocolTask& task, bool success, std::string_view reason) {
        const auto completedAt = std::to_string(message::utcNowMilliseconds());
        return { { "message_id", uuidGenerator_.next() },
                 { "causation_id", std::string(commandId) },
                 { "command_id", std::string(commandId) },
                 { "device_id", task.deviceId },
                 { "device_code", task.deviceCode },
                 { "protocol", task.protocol },
                 { "attempt", std::to_string(task.attempt) },
                 { "success", success ? "1" : "0" },
                 { "reason", std::string(reason) },
                 { "worker_id", std::to_string(workerIndex_) },
                 { "created_at_ms", completedAt },
                 { "completed_at_ms", completedAt } };
    }

    ruvia::task<void> finalize(std::string_view inputStream, std::string_view inputEntryId, std::string_view commandId, const message::ProtocolTask& task, bool success, std::string_view reason, std::string_view remoteAddress) {
        auto results = resultFields(commandId, task, success, reason);
        const auto resultStream = message::commandResultStream();
        if (success) {
            (void)co_await message::redis::publishAndAcknowledge(
                redis_,
                resultStream,
                results,
                kCommandResultCapacity,
                inputStream,
                group_,
                consumer_,
                inputEntryId,
                message::redis::StreamWake{ std::nullopt, message::WorkerStreamTask::CommandResult }
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
        auto deadFields = deadLetterFields(message::protocolTaskFields(task), inputEntryId, reason, {}, workerIndex_);
        const std::array publications{
            message::redis::StreamPublication{ deadLetterStream_, deadFields, kDeadLetterCapacity },
            message::redis::StreamPublication{
                resultStream,
                results,
                kCommandResultCapacity,
                message::redis::StreamWake{ std::nullopt, message::WorkerStreamTask::CommandResult } }
        };
        (void)co_await message::redis::publishAllAndAcknowledge(
            redis_,
            publications,
            inputStream,
            group_,
            consumer_,
            inputEntryId
        );
    }

    ruvia::redis_handle redis_;
    service::common::UuidV7Generator& uuidGenerator_;
    std::size_t workerIndex_;
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

    CollectorDerivedService(ruvia::redis_handle redis, service::common::UuidV7Generator& uuidGenerator)
        : redis_(std::move(redis)), uuidGenerator_(uuidGenerator) {}

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
        const auto result = co_await telemetry::derived::DerivedService::append(redis_, *entry.calculation, parsed);
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
        parsed.messageId = uuidGenerator_.next();
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
            const auto result = co_await telemetry::derived::DerivedService::append(redis_, *entry.calculation, parsed, true);
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
    service::common::UuidV7Generator& uuidGenerator_;
    std::map<std::string, Entry, std::less<>> calculations_;
    std::uint64_t revision_ = 0;
};

class CollectorStateService final {
  public:
    template <typename Redis>
    static ruvia::task<DeviceRouteOwner> bindRoute(const Redis& redis,
        const ProtocolAction& action, std::uint64_t sessionEpoch,
        std::size_t workerIndex, std::string_view workerInstanceId, std::string linkId) {
        const auto key = message::collector::connectionKey(action.deviceId);
        const auto runtimeKey = service::message::telemetry::runtimeKey(action.deviceId);
        const auto ownerKey = message::collector::linkOwnerKey(linkId);
        const auto revisionKey = std::string(service::message::live::kChanges);
        const auto worker = std::to_string(workerIndex);
        const auto epoch = std::to_string(sessionEpoch);
        const auto now = std::to_string(message::utcNowMilliseconds());
        const auto ttl = std::to_string(message::collector::kConnectionTtlMilliseconds);
        const std::string_view keys[]{ key, runtimeKey, revisionKey, ownerKey,
            message::collector::kConnectionDeadlinesKey };
        const std::string_view args[]{
            action.deviceId,
            action.deviceCode,
            worker,
            action.connectionId,
            epoch,
            now,
            service::runtime::instanceId(),
            linkId,
            workerInstanceId,
            ttl
        };
        const auto reply = co_await redis.eval(CollectorDeviceRouteRecord::bindScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::array || reply.array().size() != 3) {
            message::redis::throwValue("bind device route", reply);
        }
        const auto oldWorker = reply.array()[0].kind() == ruvia::redis_value::kind_type::string
            ? std::string(reply.array()[0].string())
            : std::string{};
        const auto oldConnection = reply.array()[1].kind() == ruvia::redis_value::kind_type::string
            ? std::string(reply.array()[1].string())
            : std::string{};
        const auto oldInstance = std::string(reply.array()[2].string());
        co_await message::redis::wakeWorker(redis, std::nullopt, message::WorkerStreamTask::Freshness);
        co_return DeviceRouteOwner{oldWorker, oldConnection, oldInstance};
    }

    template <typename Redis>
    static ruvia::task<void> markDeviceOffline(const Redis& redis, std::string_view deviceId,
        std::string_view connectionId, std::uint64_t sessionEpoch, std::size_t workerIndex,
        std::string_view workerInstanceId, std::string_view reason) {
        const auto key = message::collector::connectionKey(deviceId);
        const auto runtimeKey = service::message::telemetry::runtimeKey(deviceId);
        const auto revisionKey = std::string(service::message::live::kChanges);
        const auto now = std::to_string(message::utcNowMilliseconds());
        const auto worker = std::to_string(workerIndex);
        const auto epoch = std::to_string(sessionEpoch);
        const std::string_view keys[]{ key, runtimeKey, revisionKey,
            message::collector::kConnectionDeadlinesKey };
        const std::string_view args[]{service::runtime::instanceId(), worker,
            workerInstanceId, connectionId, epoch, reason, now, deviceId};
        const auto reply = co_await redis.eval(CollectorDeviceRouteRecord::eraseScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer)
            message::redis::throwValue("erase device route", reply);
    }

    template <typename Redis>
    static ruvia::task<bool> renewRoute(const Redis& redis, std::string_view deviceId,
        std::string_view connectionId, std::uint64_t sessionEpoch, std::size_t workerIndex,
        std::string_view workerInstanceId) {
        const auto key = message::collector::connectionKey(deviceId);
        const auto worker = std::to_string(workerIndex);
        const auto epoch = std::to_string(sessionEpoch);
        const auto ttl = std::to_string(message::collector::kConnectionTtlMilliseconds);
        const std::string_view keys[]{key, message::collector::kConnectionDeadlinesKey};
        const std::string_view args[]{service::runtime::instanceId(), worker,
            workerInstanceId, connectionId, epoch, ttl, message::collector::kLinkOwnerPrefix, deviceId};
        const auto reply = co_await redis.eval(CollectorDeviceRouteRecord::renewScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer)
            message::redis::throwValue("renew device route", reply);
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<void> eraseLegacyRoutes(const Redis& redis, const RuntimeSnapshot& snapshot) {
        const auto now = std::to_string(message::utcNowMilliseconds());
        const auto changes = std::string(message::live::kChanges);
        for (const auto& device : snapshot.devices) {
            const auto key = service::message::telemetry::runtimeKey(device.id);
            const auto owner = message::collector::linkOwnerKey(device.linkId);
            const std::string_view keys[]{key, owner, changes};
            const std::string_view args[]{service::runtime::instanceId(), device.linkId, now};
            const auto reply = co_await redis.eval(CollectorDeviceRouteRecord::eraseLegacyScript, keys, args);
            if (reply.kind() != ruvia::redis_value::kind_type::integer)
                message::redis::throwValue("erase legacy device route", reply);
        }
    }

    template <typename Redis>
    static ruvia::task<void> eraseLegacyLinkSnapshots(const Redis& redis, const RuntimeSnapshot& snapshot) {
        for (const auto& link : snapshot.links) {
            const auto prefix = message::collector::linkRuntimePrefix(link.id);
            const auto ownerKey = message::collector::linkOwnerKey(link.id);
            for (const auto& key : co_await message::redis::keysMatching(redis, prefix + '*')) {
                const auto separator = key.find_last_of(':');
                if (separator == std::string::npos || separator <= prefix.size())
                    continue;
                const auto instance = std::string_view(key).substr(prefix.size(), separator - prefix.size());
                if (instance == service::runtime::instanceId())
                    continue;
                const std::string_view keys[]{key, ownerKey};
                const std::string_view args[]{service::runtime::instanceId(), instance};
                const auto reply = co_await redis.eval(CollectorLinkRecord::eraseLegacyScript, keys, args);
                if (reply.kind() != ruvia::redis_value::kind_type::integer)
                    message::redis::throwValue("erase legacy link snapshot", reply);
            }
        }
    }

    template <typename Redis>
    static ruvia::task<void> storeLinkEvent(const Redis& redis,
        const message::StreamMessage& event, std::size_t workerIndex) {
        const auto record = CollectorLinkRecord::fromEvent(event, message::utcNowMilliseconds());
        const auto key =
            CollectorLinkRecord::key(event.get("link_id"), workerIndex, service::runtime::instanceId());
        co_await storeSnapshot(redis, key, record.fields);
    }

    template <typename Redis>
    static ruvia::task<void> publishWorker(const Redis& redis, std::size_t workerIndex,
        std::string_view workerInstanceId, std::string_view version) {
        const CollectorWorkerRecord record{
            workerIndex, std::string(version), "applied", message::utcNowMilliseconds(), std::string(workerInstanceId)};
        co_await storeSnapshot(redis,
            CollectorWorkerRecord::key(workerIndex, service::runtime::instanceId()), record.fields());
    }

    template <typename Redis>
    static ruvia::task<void> renewWorkerSnapshot(const Redis& redis, std::size_t workerIndex,
        std::string_view workerInstanceId, std::string_view version) {
        const CollectorWorkerRecord record{workerIndex, std::string(version), "applied",
            message::utcNowMilliseconds(), std::string(workerInstanceId)};
        const auto key = CollectorWorkerRecord::key(workerIndex, service::runtime::instanceId());
        co_await retainSnapshot(redis, key, workerInstanceId, record.fields());
    }

    template <typename Redis>
    static ruvia::task<void> retainLinkSnapshot(const Redis& redis,
        const message::StreamMessage& event, std::size_t workerIndex) {
        const auto record = CollectorLinkRecord::fromEvent(event, message::utcNowMilliseconds());
        const auto key = CollectorLinkRecord::key(event.get("link_id"), workerIndex, service::runtime::instanceId());
        co_await retainSnapshot(redis, key, event.get("worker_instance_id"), record.fields);
    }

    template <typename Redis>
    static ruvia::task<void> eraseWorker(const Redis& redis, std::size_t index,
        std::string_view workerInstanceId) {
        const auto keys = co_await message::redis::keysMatching(
            redis, CollectorLinkRecord::key("*", index, service::runtime::instanceId()));
        for (const auto& key : keys)
            co_await eraseSnapshot(redis, key, workerInstanceId);
        co_await eraseSnapshot(redis, CollectorWorkerRecord::key(index, service::runtime::instanceId()), workerInstanceId);
    }

    template <typename Redis>
    static ruvia::task<void> eraseLink(const Redis& redis, std::string_view id, std::size_t index,
        std::string_view workerInstanceId) {
        co_await eraseSnapshot(redis, CollectorLinkRecord::key(id, index, service::runtime::instanceId()), workerInstanceId);
    }

  private:
    template <typename Redis>
    static ruvia::task<void> retainSnapshot(const Redis& redis, const std::string& key,
        std::string_view workerInstanceId, const std::vector<message::StreamField>& fields) {
        std::vector<std::string> args{std::to_string(message::collector::kConnectionTtlMilliseconds),
            std::string(workerInstanceId)};
        args.reserve(2 + fields.size() * 2);
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const std::vector<std::string_view> views(args.begin(), args.end());
        const std::string_view keys[]{key};
        const auto reply = co_await redis.eval(CollectorWorkerRecord::retainScript, keys, views);
        if (reply.kind() != ruvia::redis_value::kind_type::integer)
            message::redis::throwValue("retain collector snapshot", reply);
    }

    template <typename Redis>
    static ruvia::task<void> eraseSnapshot(const Redis& redis, const std::string& key,
        std::string_view workerInstanceId) {
        const std::string_view keys[]{key};
        const std::string_view args[]{workerInstanceId};
        const auto reply = co_await redis.eval(CollectorWorkerRecord::eraseScript, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::integer)
            message::redis::throwValue("erase collector snapshot", reply);
    }

    template <typename Redis>
    static ruvia::task<void> storeSnapshot(const Redis& redis, const std::string& key,
        const std::vector<message::StreamField>& fields) {
        std::vector<std::string> args{std::to_string(message::collector::kConnectionTtlMilliseconds)};
        args.reserve(1 + fields.size() * 2);
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const std::vector<std::string_view> views(args.begin(), args.end());
        const std::string_view keys[]{key};
        const auto reply = co_await redis.eval(CollectorWorkerRecord::storeScript, keys, views);
        if (reply.kind() != ruvia::redis_value::kind_type::integer)
            message::redis::throwValue("store collector snapshot", reply);
    }
};

} // namespace service::collector
