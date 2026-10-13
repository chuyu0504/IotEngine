#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace service::access {

struct WebhookUrl final {
    bool tls{ false };
    std::string host;
    std::string port;
    std::string target;
};

} // namespace service::access

namespace service::access::delivery {

    struct Delivery final {
        std::string id;
        std::string eventType;
        std::string deviceId;
        std::string deviceCode;
        std::string occurredAt;
        std::string body;
    };

    struct MqttDestination final {
        std::string username;
        std::string password;
    };

    struct Target final {
        std::string id;
        std::string accessKeyId;
        std::string url;
        std::string secret;
        std::string headers;
        std::int64_t timeout{5};
        bool skipTlsVerify{false};
        std::int64_t expiresAtMs{0};
        std::optional<MqttDestination> mqtt;
    };

    struct DeviceCatalog final {
        std::string name;
        std::string code;
        std::map<std::string, std::vector<Target>, std::less<>> targets;
    };

    using Catalog = std::map<std::string, DeviceCatalog, std::less<>>;

} // namespace service::access::delivery

namespace service::access {

struct DeliveryResponse final {
    std::int64_t status{ 0 };
    std::string body;
    std::string error;
};

} // namespace service::access
