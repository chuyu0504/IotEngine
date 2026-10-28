#pragma once

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <locale>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/web/model.h>

#include "service/common/message.h"
#include "service/features/collector/collector.config.h"
#include "service/features/collector/collector.types.h"
#include "service/features/collector/mqtt/mqtt.types.h"
#include "service/utils/crypto.h"
#include "service/utils/json.h"
#include "service/utils/payload.h"

namespace service::collector {

// 连接身份由运行层提供，协议只使用连接内序号确定轮次身份，不生成随机运行身份。
inline std::string acquisitionIdentity(std::string_view connection, std::uint64_t sequence) {
    auto hash = service::utils::sha256(std::string(connection) + ":acquisition:" + std::to_string(sequence));
    hash[12] = '8';
    hash[16] = "89ab"[service::common::hexDigit(hash[16]) & 3];
    return hash.substr(0, 8) + '-' + hash.substr(8, 4) + '-' + hash.substr(12, 4) + '-' +
        hash.substr(16, 4) + '-' + hash.substr(20, 12);
}

// 一轮采集持有自己的响应，队列结束后仅发布一次；失败轮次不得污染下一轮。
class AcquisitionCycle final {
  public:
    [[nodiscard]] const std::string& id(std::string_view connection) {
        if (id_.empty()) {
            id_ = acquisitionIdentity(connection, generation_);
        }
        started_ = true;
        return id_;
    }

    void observe(ProtocolAction response, std::vector<ProtocolAction>& actions) {
        if (response.parsed.rawPacketIds.size() != response.parsed.rawPayloads.size()) {
            throw std::invalid_argument("acquisition response requires packet identities");
        }
        if (id_.empty()) {
            throw std::logic_error("acquisition must begin before its first response");
        }
        response.acquisitionId = id_;
        response.parsed.acquisitionId = id_;
        response.parsed.messageId = id_;
        if (!collected_) {
            collected_ = response;
            collected_->parsed.rawPayloads.clear();
            collected_->parsed.rawPacketIds.clear();
        }
        auto& combined = collected_->parsed;
        combined.rawPayloads.insert(combined.rawPayloads.end(), response.parsed.rawPayloads.begin(), response.parsed.rawPayloads.end());
        combined.rawPacketIds.insert(combined.rawPacketIds.end(), response.parsed.rawPacketIds.begin(), response.parsed.rawPacketIds.end());
        const auto readFields = [](std::string_view json, auto visit) {
            const auto object = ruvia::json_value::parse(json);
            if (!object.for_each_field([&](std::string_view name, const ruvia::json_value& value) {
                    return visit(name, value.view());
                })) {
                throw std::runtime_error("invalid acquisition values");
            }
        };
        readFields(response.parsed.valuesJson, [&](std::string_view name, std::string_view value) {
            if (name == "values") {
                readFields(value, [&](std::string_view id, std::string_view point) {
                    values_.insert_or_assign(std::string(id), std::string(point));
                    return true;
                });
            }
            return true;
        });
        response.kind = ProtocolActionKind::ObserveParsed;
        actions.push_back(std::move(response));
    }

    void fail() noexcept { failed_ = true; }

    void finish(std::vector<ProtocolAction>& actions) {
        if (collected_) {
            auto result = std::move(*collected_);
            result.kind = failed_ ? ProtocolActionKind::DiscardCollection : ProtocolActionKind::PublishParsed;
            result.acquisitionSummary = true;
            result.reason = failed_ ? "acquisition_cycle_incomplete" : "";
            result.parsed.valuesJson = "{\"function_code\":\"POLL\",\"values\":{";
            bool first = true;
            for (const auto& [id, point] : values_) {
                if (!first) {
                    result.parsed.valuesJson += ',';
                }
                first = false;
                result.parsed.valuesJson += service::utils::jsonQuoted(id) + ":" + point;
            }
            result.parsed.valuesJson += "}}";
            actions.push_back(std::move(result));
        }
        if (started_ || collected_) {
            actions.push_back({ .kind = ProtocolActionKind::FinishAcquisition, .reason = failed_ ? (collected_ ? "partial" : "failed") : "success", .acquisitionId = id_ });
        }
        id_.clear();
        collected_.reset();
        values_.clear();
        failed_ = false;
        started_ = false;
        ++generation_;
    }

  private:
    std::string id_;
    std::uint64_t generation_ = 0;
    std::optional<ProtocolAction> collected_;
    std::map<std::string, std::string, std::less<>> values_;
    bool failed_ = false;
    bool started_ = false;
};

class ProtocolSession {
  public:
    virtual ~ProtocolSession() = default;

    virtual void inheritTransportState(const ProtocolSession&) {}

    [[nodiscard]] virtual std::vector<ProtocolAction> connected() { return {}; }

    [[nodiscard]] virtual std::vector<ProtocolAction> consume(const ProtocolInput& input) = 0;

    // 持久化消息接入成功后，协议才能确认接收或结束查询。
    [[nodiscard]] virtual std::vector<ProtocolAction> parsedPublished(std::uint64_t) { return {}; }

    [[nodiscard]] virtual std::vector<ProtocolAction> disconnected(std::string_view reason) = 0;
};

class CommandCapabilitySession {
  public:
    virtual ~CommandCapabilitySession() = default;
    [[nodiscard]] virtual std::vector<ProtocolAction> execute(ProtocolCommand command) = 0;
};

class DeadlineCapabilitySession {
  public:
    virtual ~DeadlineCapabilitySession() = default;
    [[nodiscard]] virtual std::vector<ProtocolAction> deadline(std::uint64_t token) = 0;
};

class ProtocolSessionFactory {
  public:
    virtual ~ProtocolSessionFactory() = default;

    [[nodiscard]] virtual const ProtocolDefinition& definition() const noexcept = 0;

    // 只有能区分新旧应答的协议才允许保留 TCP 连接并重建会话。
    [[nodiscard]] virtual bool canRefreshTransport(const DeviceDefinition&) const noexcept {
        return false;
    }

    // 仅作日志归属筛选，不能据此绑定设备或改变连接归属。
    [[nodiscard]] virtual bool packetMatchesDevice(const DeviceDefinition&, std::string_view, std::span<const std::uint8_t>) const noexcept { return true; }

    [[nodiscard]] virtual std::vector<std::uint8_t> packetForLogging(std::span<const std::uint8_t> bytes) const {
        return { bytes.begin(), bytes.end() };
    }

    void validateLink(const LinkDefinition& link) const {
        const auto& descriptor = definition();
        if (link.protocol != descriptor.name) {
            throw std::invalid_argument("protocol factory does not match link");
        }
        if (link.mode != "TCP Server" && link.mode != "TCP Client") {
            throw std::invalid_argument("unsupported collector link mode");
        }
        const auto capability = link.mode == "TCP Server" ? ProtocolCapability::TcpServer
                                                          : ProtocolCapability::TcpClient;
        if (!descriptor.capabilities.has(capability)) {
            throw std::invalid_argument(std::string(descriptor.name) + " does not support " + link.mode + " links");
        }
    }

    [[nodiscard]] std::unique_ptr<ProtocolSession>
    createSession(const LinkDefinition& link, std::string_view connectionId, std::string_view targetId, const std::shared_ptr<const RuntimeSnapshot>& snapshot) const {
        validateLink(link);
        if (!snapshot) {
            throw std::invalid_argument("protocol snapshot is null");
        }
        if (link.mode == "TCP Server" && !targetId.empty()) {
            throw std::invalid_argument("TCP Server session cannot have a client target");
        }
        std::vector<const DeviceDefinition*> devices;
        for (const auto& device : snapshot->devices) {
            if (device.linkId == link.id && device.protocol == definition().name &&
                (targetId.empty() || device.targetId == targetId)) {
                devices.push_back(&device);
            }
        }
        auto session = createDeviceSession(link, connectionId, targetId, snapshot, std::move(devices));
        if (!session) {
            throw std::runtime_error("protocol factory returned a null session");
        }
        if (definition().capabilities.has(ProtocolCapability::Commands) &&
            !dynamic_cast<CommandCapabilitySession*>(session.get())) {
            throw std::logic_error("protocol declares commands without a command session");
        }
        if (definition().capabilities.has(ProtocolCapability::Polling) &&
            !dynamic_cast<DeadlineCapabilitySession*>(session.get())) {
            throw std::logic_error("protocol declares polling without a deadline session");
        }
        return session;
    }

  protected:
    [[nodiscard]] virtual std::unique_ptr<ProtocolSession>
    createDeviceSession(const LinkDefinition& link, std::string_view connectionId, std::string_view targetId, const std::shared_ptr<const RuntimeSnapshot>& snapshot, std::vector<const DeviceDefinition*> devices) const = 0;
};

} // namespace service::collector

namespace service::collector::command {

// 完整功能或消息模板保持为一个任务；独立点位写入分别跟踪应答和回读。
inline std::vector<std::vector<CommandElementValue>> groupElements(
    const ProtocolDefinition& definition,
    std::vector<CommandElementValue> requested
) {
    std::vector<std::vector<CommandElementValue>> tasks;
    if (requested.empty()) {
        return tasks;
    }
    if (definition.commandLayout != CommandLayout::WritableElements) {
        tasks.push_back(std::move(requested));
    } else {
        tasks.reserve(requested.size());
        for (auto& element : requested) {
            tasks.push_back({ std::move(element) });
        }
    }
    return tasks;
}

inline std::string_view trim(std::string_view value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

template <typename Number>
Number integer(std::string_view value, std::string_view name) {
    Number result{};
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size()) {
        throw std::invalid_argument("command_invalid: " + std::string(name) + " must be an integer");
    }
    return result;
}

inline double decimal(std::string_view value, std::string_view name) {
    double result{};
    std::istringstream input{ std::string(value) };
    input.imbue(std::locale::classic());
    input >> std::noskipws >> result;
    char trailing{};
    if (!input || input >> trailing || !std::isfinite(result)) {
        throw std::invalid_argument("command_invalid: " + std::string(name) + " must be a finite number");
    }
    return result;
}

// Wire-order packed BCD, shared by command preparation and the actual encoder.
// Keep lexical precision/sign rules here; SL651 uses a different BCD format.
inline std::uint64_t encodeDlt645Bcd(std::string_view value, std::size_t width, unsigned decimals, bool signedValue) {
    if (width == 0 || width > 8 || decimals > 8 || value.empty()) {
        throw std::invalid_argument("dlt645_bcd_size_invalid");
    }
    const bool negative = value.front() == '-';
    if (negative) {
        value.remove_prefix(1);
    }
    if (value.empty() || (negative && !signedValue) || value.back() == '.') {
        throw std::invalid_argument("dlt645_bcd_value_invalid");
    }
    std::uint64_t limit = 1;
    for (std::size_t index = 0; index < width * 2; ++index) {
        limit *= 10;
    }
    if (signedValue) {
        limit = limit / 10 * 8;
    }
    std::uint64_t magnitude{};
    bool point = false;
    bool integerDigit = false;
    unsigned fractional{};
    const auto append = [&](unsigned digit) {
        if (magnitude > (limit - 1 - digit) / 10) {
            throw std::invalid_argument("dlt645_bcd_value_out_of_range");
        }
        magnitude = magnitude * 10 + digit;
    };
    for (const auto character : value) {
        if (character == '.' && !point && integerDigit) {
            point = true;
            continue;
        }
        if (character < '0' || character > '9') {
            throw std::invalid_argument("dlt645_bcd_value_invalid");
        }
        if (point && fractional >= decimals) {
            if (character != '0') {
                throw std::invalid_argument("dlt645_bcd_precision_exceeded");
            }
            continue;
        }
        append(static_cast<unsigned>(character - '0'));
        if (point) {
            ++fractional;
        } else {
            integerDigit = true;
        }
    }
    while (fractional++ < decimals) {
        append(0);
    }
    std::uint64_t encoded{};
    for (unsigned shift = 0; magnitude; shift += 4) {
        encoded |= (magnitude % 10) << shift;
        magnitude /= 10;
    }
    if (negative && encoded != 0) {
        encoded |= std::uint64_t{ 1 } << (width * 8 - 1);
    }
    return encoded;
}

inline void validateValue(const ElementDefinition& element, std::string_view value) {
    const auto name = element.name.empty() ? element.id : element.name;
    if (value.empty()) {
        throw std::invalid_argument("command_invalid: " + name + " value is empty");
    }
    const auto type = element.dataType;
    if (type == "BCD" || type == "BCD_SIGNED") {
        (void)encodeDlt645Bcd(value, static_cast<std::size_t>(element.length), static_cast<unsigned>(element.digits), type == "BCD_SIGNED");
        return;
    }
    if (type == "BOOL") {
        if (value != "0" && value != "1") {
            throw std::invalid_argument("command_invalid: " + name + " BOOL must be 0 or 1");
        }
        return;
    }
    if (type == "INT8") {
        const auto parsed = integer<std::int64_t>(value, name);
        if (parsed < -128 || parsed > 127) {
            throw std::invalid_argument("command_invalid: " + name + " INT8 out of range");
        }
        return;
    }
    if (type == "UINT8" || type == "BYTE") {
        const auto parsed = integer<std::uint64_t>(value, name);
        if (parsed > 255) {
            throw std::invalid_argument("command_invalid: " + name + " UINT8 out of range");
        }
        return;
    }
    if (type == "INT16") {
        const auto parsed = integer<std::int64_t>(value, name);
        if (parsed < -32768 || parsed > 32767) {
            throw std::invalid_argument("command_invalid: " + name + " INT16 out of range");
        }
        return;
    }
    if (type == "UINT16" || type == "WORD") {
        const auto parsed = integer<std::uint64_t>(value, name);
        if (parsed > 65535) {
            throw std::invalid_argument("command_invalid: " + name + " UINT16 out of range");
        }
        return;
    }
    if (type == "INT32") {
        const auto parsed = integer<std::int64_t>(value, name);
        if (parsed < std::numeric_limits<std::int32_t>::min() ||
            parsed > std::numeric_limits<std::int32_t>::max()) {
            throw std::invalid_argument("command_invalid: " + name + " INT32 out of range");
        }
        return;
    }
    if (type == "UINT32" || type == "DWORD") {
        const auto parsed = integer<std::uint64_t>(value, name);
        if (parsed > std::numeric_limits<std::uint32_t>::max()) {
            throw std::invalid_argument("command_invalid: " + name + " UINT32 out of range");
        }
        return;
    }
    if (type == "INT64") {
        (void)integer<std::int64_t>(value, name);
        return;
    }
    if (type == "UINT64") {
        (void)integer<std::uint64_t>(value, name);
        return;
    }
    if (type == "FLOAT" || type == "FLOAT32" || type == "REAL") {
        const auto parsed = static_cast<float>(decimal(value, name));
        if (!std::isfinite(parsed)) {
            throw std::invalid_argument("command_invalid: " + name + " FLOAT out of range");
        }
        return;
    }
    if (type == "DOUBLE" || type == "LREAL") {
        (void)decimal(value, name);
        return;
    }
    if (type == "STRING") {
        if (element.size <= 0 || value.size() > static_cast<std::size_t>(element.size)) {
            throw std::invalid_argument("command_invalid: " + name + " STRING is too long");
        }
        return;
    }
    if (element.encoding == "BCD") {
        const auto parsed = decimal(value, name);
        if (element.digits < 0 || element.digits > 7 || element.length < 1 || element.length > 31) {
            throw std::invalid_argument("command_invalid: " + name + " BCD definition is invalid");
        }
        const auto length = element.length - (parsed < 0 ? 1 : 0);
        const auto scaled = std::round(std::abs(parsed) * std::pow(10.0, element.digits));
        if (length == 0 || !std::isfinite(scaled) || scaled >= std::pow(10.0, length * 2)) {
            throw std::invalid_argument("command_invalid: " + name + " BCD is too long");
        }
        return;
    }
    if (!element.encoding.empty()) {
        if (value.size() >
                static_cast<std::size_t>(std::max<std::int64_t>(1, element.length) * 2) ||
            !std::ranges::all_of(
                value,
                [](unsigned char character) {
                    return std::isxdigit(character) != 0;
                }
            )) {
            throw std::invalid_argument("command_invalid: " + name + " HEX is invalid");
        }
        return;
    }
    (void)decimal(value, name);
}

inline ResolvedCommand resolve(const DeviceDefinition& device, std::span<const CommandElementValue> requested, std::string_view mqttMessageId = {}) {
    if (device.protocol != "MQTT" && !mqttMessageId.empty()) {
        throw std::invalid_argument("command_invalid: MQTT message selection requires MQTT");
    }
    const auto& definition = protocolDefinition(device.protocol);
    if (requested.empty() || requested.size() > 256) {
        throw std::invalid_argument("command_invalid: element count must be between 1 and 256");
    }
    ResolvedCommand result;
    // pointId 是遥测身份，可能同时出现在 UP、DOWN 和响应函数中。命令选择
    // 配置顺序中首个完整匹配的 DOWN 函数，而不是让较早的 UP occurrence 遮挡它。
    if (definition.commandLayout == CommandLayout::CompleteFunction) {
        std::set<std::string_view, std::less<>> examined;
        for (const auto& candidate : device.elements) {
            if (candidate.responseElement || candidate.direction != "DOWN" || candidate.encoding == "JPEG" ||
                !examined.insert(candidate.functionCode).second) {
                continue;
            }
            std::size_t required{};
            bool matches = true;
            for (const auto& element : device.elements) {
                if (element.responseElement || element.direction != "DOWN" || element.functionCode != candidate.functionCode) {
                    continue;
                }
                ++required;
                if (element.encoding == "JPEG" || !std::ranges::any_of(requested, [&](const auto& input) {
                        return input.elementId == element.id;
                    })) {
                    matches = false;
                }
            }
            if (matches && required == requested.size()) {
                result.functionCode = candidate.functionCode;
                break;
            }
        }
    }
    const bool selectedFunction = !result.functionCode.empty();
    std::set<std::string, std::less<>> ids;
    for (const auto& input : requested) {
        const auto value = trim(input.value);
        if (input.elementId.empty() || !ids.insert(input.elementId).second) {
            throw std::invalid_argument("command_invalid: element id is empty or duplicated");
        }
        const auto matched =
            std::find_if(device.elements.begin(), device.elements.end(), [&](const auto& element) {
                return element.id == input.elementId && !element.responseElement &&
                    (!selectedFunction || (element.direction == "DOWN" && element.functionCode == result.functionCode));
            });
        if (matched == device.elements.end()) {
            throw std::invalid_argument("command_invalid: element is not configured");
        }
        if (definition.commandLayout != CommandLayout::CompleteFunction && !matched->writable) {
            throw std::invalid_argument("command_invalid: element is not writable");
        }
        if (definition.commandLayout == CommandLayout::CompleteFunction) {
            if (matched->direction != "DOWN" || matched->encoding == "JPEG") {
                throw std::invalid_argument("command_invalid: SL651 element is not writable");
            }
            if (result.functionCode.empty()) {
                result.functionCode = matched->functionCode;
            } else if (result.functionCode != matched->functionCode) {
                throw std::invalid_argument(
                    "command_invalid: SL651 elements must share one function code"
                );
            }
        }
        validateValue(*matched, value);
        result.elements.push_back({ &*matched, std::string(value) });
    }
    if (definition.commandLayout == CommandLayout::CompleteFunction) {
        std::size_t required = 0;
        for (const auto& element : device.elements) {
            if (!element.responseElement && element.direction == "DOWN" &&
                element.functionCode == result.functionCode) {
                ++required;
            }
        }
        if (required == 0 || required != result.elements.size()) {
            throw std::invalid_argument(
                "command_invalid: SL651 command requires every element in the function"
            );
        }
    }
    if (device.protocol == "MQTT") {
        const auto config = ruvia::from_json<mqtt::Config>(device.calculationConfig);

        const auto& commands = config.get<"commands">();
        if (commands) {
            const auto selected = std::find_if(commands->begin(), commands->end(), [&](const auto& message) {
                return message.template get<"id">().view() == mqttMessageId;
            });
            if (mqttMessageId.empty() || selected == commands->end()) {
                throw std::invalid_argument("command_invalid: 请选择有效的 MQTT 控制消息");
            }
            service::utils::JsonValueTemplate::Tokens allowed{ "$deviceCode", "$deviceId", "$commandId" };
            for (const auto& point : config.get<"points">()) {
                allowed.emplace("$point:" + std::string(point.get<"name">().view()));
            }
            const service::utils::JsonValueTemplate message(selected->get<"templateText">().view(), allowed, {}, false, 16384, 32, { "$param:" });
            std::set<std::string, std::less<>> topicNames;
            for (const auto level : service::utils::splitPayload(selected->get<"topic">().view(), "/")) {
                if (level.starts_with("{point:") && level.ends_with('}')) {
                    topicNames.emplace(level.substr(7, level.size() - 8));
                }
            }
            for (const auto& input : requested) {
                const auto point = std::find_if(config.get<"points">().begin(), config.get<"points">().end(), [&](const auto& point) {
                    return point.template get<"id">().view() == input.elementId;
                });
                if (point == config.get<"points">().end() || (!message.references("$point:" + std::string(point->get<"name">().view())) && !topicNames.contains(point->get<"name">().view()))) {
                    throw std::invalid_argument("command_invalid: 控制点不属于所选消息");
                }
            }
            for (const auto& required : selected->get<"requiredPointIds">()) {
                if (!ids.contains(required.view())) {
                    throw std::invalid_argument("command_invalid: 缺少必填控制点: " + std::string(required.view()));
                }
            }
            for (const auto& point : config.get<"points">()) {
                if (topicNames.contains(point.get<"name">().view()) && !ids.contains(point.get<"id">().view())) {
                    throw std::invalid_argument("command_invalid: 缺少 Topic 属性: " + std::string(point.get<"name">().view()));
                }
            }
        } else if (!mqttMessageId.empty()) {
            throw std::invalid_argument("command_invalid: MQTT 控制消息不存在");
        }
    }
    return result;
}

} // namespace service::collector::command
