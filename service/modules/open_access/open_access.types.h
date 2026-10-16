#pragma once
#include "service/utils/text.h"
#include "service/utils/mqtt.h"
#include "service/utils/json.h"
#include "service/common/http.h"
#include "service/common/uuid.h"
#include "service/common/message.h"
#include "service/modules/device/device.types.h"
#include <ruvia/web/validation.h>
#include <ruvia/web/model_object.h>
#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace service::open_access {

inline constexpr auto kDefaultStatus = RUVIA_DEFAULT("enabled"){};
inline constexpr auto kDefaultTimeoutSeconds = RUVIA_DEFAULT(5){};
inline constexpr auto kDefaultSkipTlsVerify = RUVIA_DEFAULT(false){};
inline constexpr auto kDefaultPage = RUVIA_DEFAULT(1){};
inline constexpr auto kDefaultPageSize = RUVIA_DEFAULT(20){};

inline constexpr std::string_view kScopeRealtime = "device:realtime";
inline constexpr std::string_view kScopeHistory = "device:history";
inline constexpr std::string_view kScopeCommand = "device:command";
inline constexpr std::string_view kScopeAlert = "alert:read";
inline bool supportedScope(std::string_view value) {
    return value == kScopeRealtime || value == kScopeHistory || value == kScopeCommand || value == kScopeAlert;
}

// 第三方开放接口下发设备命令的请求体与响应，沿用设备命令契约。
using PublicCommandBody = service::device::DeviceCommandBody;
using PublicCommandResponse = service::device::DeviceCommandCreateResponse;
using PublicCommandResult = service::device::DeviceCommandCreateDto;

struct AccessSession final {
    std::string id;
    std::string name;
    std::set<std::string, std::less<>> scopes;
    std::set<std::string, std::less<>> deviceIds;
    [[nodiscard]] bool allows(std::string_view scope) const { return scopes.contains(scope); }
    [[nodiscard]] bool allowsDevice(std::string_view id) const { return deviceIds.contains(id); }
};

inline bool isAccessName(const ruvia::string& value) {
    const auto text = service::utils::trim(value.view());
    return !text.empty() && text.size() <= 64;
}
inline bool isAccessScopes(const ruvia::array<ruvia::string>& values) {
    return !values.empty() && std::ranges::all_of(values, [](const auto& value) { return supportedScope(value.view()); });
}
inline bool isAccessDevices(const ruvia::array<ruvia::string>& values) {
    return !values.empty() && values.size() <= 10000 && std::ranges::all_of(values, service::common::isUuidField);
}
inline bool isWebhookEvents(const ruvia::array<ruvia::string>& values) {
    return !values.empty() && std::ranges::all_of(values, [](const auto& value) { return service::message::supportedEvent(value.view()); });
}
inline bool isWebhookUrl(const ruvia::string& value) {
    const auto url = service::utils::trim(value.view());
    if (url.empty() || url.size() > 2048 || (!url.starts_with("http://") && !url.starts_with("https://")) || url.find_first_of("\r\n@#") != std::string::npos) return false;
    const auto authority = url.find("://") + 3;
    return authority < url.size() && url[authority] != '/' && url[authority] != '?' && url[authority] != '#';
}
inline bool isWebhookHeaders(const ruvia::json_object& headers) {
    // 静态常量配合无捕获回调，避免 MSVC Release 对局部 constexpr 表的引用捕获误编译。
    static constexpr std::array<std::string_view, 15> reserved{
        "host", "content-length", "connection", "x-iot-event", "x-iot-timestamp", "x-iot-delivery", "x-iot-signature", "content-type", "user-agent", "transfer-encoding", "trailer", "te", "upgrade", "expect", "proxy-connection"
    };
    return headers.for_each_field([](std::string_view key, const ruvia::json_value& fieldValue) {
        const bool token = !key.empty() && std::ranges::all_of(key, [](char ch) {
            return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || std::string_view("!#$%&'*+.^_`|~-").find(ch) != std::string_view::npos;
        });
        if (!token) return false;
        std::string lower(key);
        for (auto& ch : lower) if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch + ('a' - 'A'));
        if (std::ranges::find(reserved, lower) != reserved.end()) return false;
        const auto value = fieldValue.get<ruvia::string>();
        return value && value->view().find_first_of("\r\n") == std::string_view::npos;
    });
}
inline std::optional<std::string> trimAccessText(const std::optional<ruvia::string>& value) {
    if (!value) return std::nullopt;
    auto text = service::utils::trim(value->view());
    return text.empty() ? std::nullopt : std::optional<std::string>(std::move(text));
}
inline std::set<std::string, std::less<>> accessStringSet(const ruvia::array<ruvia::string>& values) {
    std::set<std::string, std::less<>> result;
    for (const auto& value : values) result.emplace(value.view());
    return result;
}
inline std::vector<std::string> accessDeviceIds(const ruvia::array<ruvia::string>& values) {
    const auto unique = accessStringSet(values);
    return {unique.begin(), unique.end()};
}

RUVIA_MODEL(CreateAccessKeyBody,
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_CUSTOM("调用配置名称不能为空或过长", isAccessName)),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, decltype(kDefaultStatus), RUVIA_ONE_OF("status 无效", "enabled", "disabled")),
    RUVIA_REQUIRED_FIELD(scopes, ruvia::array<ruvia::string>, RUVIA_CUSTOM("开放权限无效", isAccessScopes)),
    RUVIA_REQUIRED_FIELD(deviceIds, ruvia::array<ruvia::string>, RUVIA_CUSTOM("请选择有效设备", isAccessDevices)),
    RUVIA_OPTIONAL_FIELD(expiresAt, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(64, "过期时间过长")),
    RUVIA_OPTIONAL_FIELD(remark, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(200, "备注过长")));
RUVIA_MODEL(UpdateAccessKeyBody,
    RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_CUSTOM("调用配置名称不能为空或过长", isAccessName)),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("status 无效", "enabled", "disabled")),
    RUVIA_OPTIONAL_FIELD(scopes, ruvia::array<ruvia::string>, RUVIA_CUSTOM("开放权限无效", isAccessScopes)),
    RUVIA_OPTIONAL_FIELD(deviceIds, ruvia::array<ruvia::string>, RUVIA_CUSTOM("请选择有效设备", isAccessDevices)),
    RUVIA_OPTIONAL_FIELD(expiresAt, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(64, "过期时间过长")),
    RUVIA_OPTIONAL_FIELD(remark, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(200, "备注过长")));
RUVIA_MODEL(CreateWebhookBody,
    RUVIA_REQUIRED_FIELD(accessKeyId, ruvia::string, RUVIA_CUSTOM("请选择调用配置", service::common::isUuidField)),
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_CUSTOM("Webhook 名称无效", isAccessName)),
    RUVIA_REQUIRED_FIELD(url, ruvia::string, RUVIA_CUSTOM("Webhook 地址必须是有效的 HTTP(S) URL", isWebhookUrl)),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, decltype(kDefaultStatus), RUVIA_ONE_OF("status 无效", "enabled", "disabled")),
    RUVIA_OPTIONAL_FIELD(timeoutSeconds, ruvia::int64, decltype(kDefaultTimeoutSeconds), RUVIA_MIN(1, "超时太短"), RUVIA_MAX(30, "超时过长")),
    RUVIA_OPTIONAL_FIELD(skipTlsVerify, ruvia::bool_value, decltype(kDefaultSkipTlsVerify)),
    RUVIA_OPTIONAL_FIELD(headers, ruvia::json_object),
    RUVIA_OPTIONAL_FIELD(eventTypes, ruvia::array<ruvia::string>, RUVIA_CUSTOM("Webhook 事件无效", isWebhookEvents)),
    RUVIA_OPTIONAL_FIELD(secret, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(255, "密钥过长")));
RUVIA_MODEL(UpdateWebhookBody,
    RUVIA_OPTIONAL_FIELD(accessKeyId, ruvia::string, RUVIA_CUSTOM("请选择调用配置", service::common::isUuidField)),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_CUSTOM("Webhook 名称无效", isAccessName)),
    RUVIA_OPTIONAL_FIELD(url, ruvia::string, RUVIA_CUSTOM("Webhook 地址必须是有效的 HTTP(S) URL", isWebhookUrl)),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_ONE_OF("status 无效", "enabled", "disabled")),
    RUVIA_OPTIONAL_FIELD(timeoutSeconds, ruvia::int64, RUVIA_MIN(1, "超时太短"), RUVIA_MAX(30, "超时过长")),
    RUVIA_OPTIONAL_FIELD(skipTlsVerify, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(headers, ruvia::json_object),
    RUVIA_OPTIONAL_FIELD(eventTypes, ruvia::array<ruvia::string>, RUVIA_CUSTOM("Webhook 事件无效", isWebhookEvents)),
    RUVIA_OPTIONAL_FIELD(secret, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(255, "密钥过长")));

inline bool isMqttBroker(const ruvia::string& value) {
    try { (void)service::utils::mqtt::parseBroker(value.view()); return value.view().size() <= 2048; }
    catch (const std::invalid_argument&) { return false; }
}
RUVIA_MODEL(MqttSaveBody,
    RUVIA_REQUIRED_FIELD(accessKeyId, ruvia::string, RUVIA_CUSTOM("请选择调用配置", service::common::isUuidField)),
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_CUSTOM("MQTT 配置名称无效", isAccessName)),
    RUVIA_REQUIRED_FIELD(brokerUrl, ruvia::string, RUVIA_CUSTOM("Broker 地址无效", isMqttBroker)),
    RUVIA_OPTIONAL_FIELD(status, ruvia::string, decltype(kDefaultStatus), RUVIA_ONE_OF("status 无效", "enabled", "disabled")),
    RUVIA_OPTIONAL_FIELD(timeoutSeconds, ruvia::int64, decltype(kDefaultTimeoutSeconds), RUVIA_MIN(1, "超时太短"), RUVIA_MAX(30, "超时过长")),
    RUVIA_REQUIRED_FIELD(eventTypes, ruvia::array<ruvia::string>, RUVIA_CUSTOM("MQTT 事件无效", isWebhookEvents)),
    RUVIA_OPTIONAL_FIELD(username, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(65535, "用户名过长")),
    RUVIA_OPTIONAL_FIELD(password, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(65535, "密码过长")));
RUVIA_MODEL(MqttItem,
    RUVIA_REQUIRED_FIELD(id, ruvia::string),
    RUVIA_REQUIRED_FIELD(accessKeyId, ruvia::string),
    RUVIA_REQUIRED_FIELD(accessKeyName, ruvia::string),
    RUVIA_REQUIRED_FIELD(name, ruvia::string),
    RUVIA_REQUIRED_FIELD(brokerUrl, ruvia::string),
    RUVIA_REQUIRED_FIELD(status, ruvia::string),
    RUVIA_REQUIRED_FIELD(timeoutSeconds, ruvia::int64),
    RUVIA_REQUIRED_FIELD(eventTypes, ruvia::array<ruvia::string>),
    RUVIA_OPTIONAL_FIELD(username, ruvia::string, RUVIA_NULLABLE),
    RUVIA_REQUIRED_FIELD(hasPassword, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(lastTriggeredAt, ruvia::string, RUVIA_NULLABLE),
    RUVIA_OPTIONAL_FIELD(lastSuccessAt, ruvia::string, RUVIA_NULLABLE),
    RUVIA_OPTIONAL_FIELD(lastFailureAt, ruvia::string, RUVIA_NULLABLE),
    RUVIA_OPTIONAL_FIELD(lastError, ruvia::string, RUVIA_NULLABLE),
    RUVIA_REQUIRED_FIELD(createdAt, ruvia::string),
    RUVIA_REQUIRED_FIELD(updatedAt, ruvia::string));
RUVIA_MODEL(MqttListResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64),
    RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, ruvia::boxed_array<MqttItem>));

RUVIA_MODEL(AccessIdParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("ID 必须是 UUID", service::common::isUuidField)));
RUVIA_MODEL(WebhookQueryParams, RUVIA_OPTIONAL_FIELD(accessKeyId, ruvia::string, RUVIA_CUSTOM("accessKeyId 必须是 UUID", service::common::isUuidField)));
RUVIA_MODEL(AccessLogParams, RUVIA_OPTIONAL_FIELD(mqttId, ruvia::string, RUVIA_CUSTOM("mqttId 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(page, ruvia::int64, decltype(kDefaultPage), RUVIA_MIN(1, "page 必须大于 0")), RUVIA_OPTIONAL_FIELD(pageSize, ruvia::int64, decltype(kDefaultPageSize), RUVIA_MIN(1, "pageSize 必须在 1 - 100 之间"), RUVIA_MAX(100, "pageSize 必须在 1 - 100 之间")), RUVIA_OPTIONAL_FIELD(accessKeyId, ruvia::string, RUVIA_CUSTOM("accessKeyId 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(webhookId, ruvia::string, RUVIA_CUSTOM("webhookId 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(deviceId, ruvia::string, RUVIA_CUSTOM("deviceId 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(direction, ruvia::string, RUVIA_MAX(128, "direction 长度超出限制")), RUVIA_OPTIONAL_FIELD(action, ruvia::string, RUVIA_MAX(128, "action 长度超出限制")), RUVIA_OPTIONAL_FIELD(status, ruvia::string, RUVIA_MAX(128, "status 长度超出限制")), RUVIA_OPTIONAL_FIELD(eventType, ruvia::string, RUVIA_MAX(128, "eventType 长度超出限制")));

} // namespace service::open_access
