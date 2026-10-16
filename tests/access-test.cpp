#include <iostream>
#include <chrono>
#include <stdexcept>
#include <string>

#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/modules/open_access/open_access.service.h"
#include "service/modules/open_access/open_access.types.h"
#include "service/features/access/access.service.h"
#include "service/features/access/access.transport.h"
#include "service/features/access/access.runtime.h"
#include "service/features/messaging/messaging.service.h"
#include "service/utils/crypto.h"
#include "service/utils/json.h"
#include "service/modules/system/auth/auth.service.h"

void require(bool condition, const char* message) {
    if (!condition)
        throw std::runtime_error(message);
}

int main() {
    try {
        using WebhookDelivery = service::access::delivery::DeliveryService;
        require(WebhookDelivery::mergeEventData(R"({"id":"trusted"})",
                    R"({"device":{"id":"spoofed"},"status":"online"})") ==
                    R"({"device":{"id":"trusted"},"status":"online"})",
                "webhook event data must preserve the catalog device identity");
        require(WebhookDelivery::imageEventData(R"({"id":"device-1"})",
                    R"({"values":{"camera":{"type":"JPEG","name":"Camera","value":"data:image/jpeg;base64,AQ=="}}})",
                    "1970-01-01T00:00:00Z") ==
                    R"({"device":{"id":"device-1"},"image":{"id":"camera","name":"Camera","data":"data:image/jpeg;base64,AQ==","time":"1970-01-01T00:00:00Z"}})",
                "webhook image payload contract changed");
        require(WebhookDelivery::imageEventData(R"({"id":"device-1"})", "invalid", "unused") ==
                    R"({"device":{"id":"device-1"}})",
                "malformed image event fallback changed");
        require(service::utils::sha256("abc") ==
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "SHA-256 contract changed");
        require(service::utils::hmacSha256("key", "The quick brown fox jumps over the lazy dog") ==
                    "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
                "HMAC-SHA256 contract changed");
        require(service::utils::jsonQuoted("a\n\"b") == "\"a\\n\\\"b\"", "JSON escaping contract changed");
        require(service::message::webhookEnvelope("device.data.reported", "1970-01-01T00:00:00Z",
                                      "delivery-1",
                                      R"({"device":{"id":"device-1"},"points":[]})") ==
                    R"({"event":"device.data.reported","time":"1970-01-01T00:00:00Z","deliveryId":"delivery-1","data":{"device":{"id":"device-1"},"points":[]}})",
                "Webhook envelope contract changed");
        require(service::common::utcTimestampFromMilliseconds(0) == "1970-01-01T00:00:00Z", "UTC timestamp contract changed");
        require(service::common::utcTimestampFromMilliseconds(999) == "1970-01-01T00:00:00Z",
                "UTC timestamp must use seconds precision");
        require(service::common::utcTimestampFromMilliseconds(-1) == "1969-12-31T23:59:59Z",
                "UTC timestamp must floor negative milliseconds");
        require(service::auth::AuthTokenService::duration("15m", std::chrono::seconds(1)) ==
                    std::chrono::minutes(15),
                "JWT duration suffix parsing changed");
        require(service::auth::AuthTokenService::duration("1x", std::chrono::hours(1)) ==
                    std::chrono::hours(1),
                "JWT duration accepted trailing non-duration bytes");
        const auto now = service::common::utcTimestampNow();
        require(now.size() == 20 && now[4] == '-' && now[7] == '-' && now[10] == 'T' &&
                    now[13] == ':' && now[16] == ':' && now[19] == 'Z',
                "current timestamp must use the RFC 3339 wire format");
        const auto key = service::access::generateAccessKey();
        require(key.starts_with("ak_") && key.size() == 51, "AccessKey format contract changed");
        require(service::access::supportedScope("device:command") &&
                    !service::access::supportedScope("device:admin"),
                "scope allowlist contract changed");
        require(service::message::supportedEvent("device.data.reported") &&
                    !service::message::supportedEvent("device.deleted"),
                "event allowlist contract changed");
        const auto dataPublication = service::message::access::stream::eventPublicationKey(
            "019fd9f6-4be5-7272-a194-9e571bce848d",
            "device.data.reported"
        );
        require(dataPublication ==
                    "iot:open-access:event:published:device.data.reported:"
                    "019fd9f6-4be5-7272-a194-9e571bce848d",
                "open-access publication idempotency key changed");
        require(dataPublication != service::message::access::stream::eventPublicationKey("019fd9f6-4be5-7272-a194-9e571bce848d", "device.command.updated"), "different event types incorrectly share an idempotency key");
        const auto publication = service::message::access::stream::eventPublication(
            "019fd9f6-4be5-7272-a194-9e571bce848d",
            "device.command.updated",
            "019fd9f6-4be5-7272-a194-9e571bce848e",
            "DEV-1",
            1700000000000LL,
            R"({"commandId":"c"})",
            "instance-a"
        );
        require(publication.keys[0] == "iot:channel:open-access:event" && publication.keys[1] == "iot:open-access:event:published:device.command.updated:"
                                                                                                 "019fd9f6-4be5-7272-a194-9e571bce848d" &&
                    publication.keys[2] == service::message::workerWakeStream(std::nullopt, "instance-a"),
                "open-access event publication keys changed");
        require(publication.arguments == std::array<std::string, 16>{ "100000", "604800", "100000", "webhook", "event_id", "019fd9f6-4be5-7272-a194-9e571bce848d", "event_type", "device.command.updated", "device_id", "019fd9f6-4be5-7272-a194-9e571bce848e", "device_code", "DEV-1", "occurred_at_ms", "1700000000000", "data_json", R"({"commandId":"c"})" }, "open-access event Stream fields changed");
        require(service::message::access::stream::event() ==
                        "iot:channel:open-access:event" &&
                    service::message::access::stream::deliveryResult() ==
                        "iot:channel:open-access:delivery-result" &&
                    service::message::access::stream::sessionChanges() ==
                        "iot:channel:open-access:session-change",
                "shared access streams changed");
        require(service::message::access::stream::catalogChanges(0) !=
                    service::message::access::stream::catalogChanges(1),
                "webhook workers share a catalog refresh Stream");
        require(service::message::access::stream::catalogChanges(0) !=
                    service::message::access::stream::sessionChanges(),
                "local catalog refresh and shared session work must use distinct streams");
        require(service::message::configuration::runtimeChangesStream() !=
                    service::message::access::stream::catalogChanges(0),
                "independent config consumers must not XDEL from a shared Stream");
        const auto encodedSession = service::message::access::session::encode(
            "019fd9f6-4be5-7272-a194-9e571bce848d", "production key", "enabled",
            "4102444800000", R"(["device:realtime","device:history"])",
            R"(["019fd9f6-4be5-7272-a194-9e571bce848e"])");
        const auto decodedSession = service::message::access::session::decode(encodedSession);
        require(decodedSession.id == "019fd9f6-4be5-7272-a194-9e571bce848d" &&
                    decodedSession.name == "production key" &&
                    decodedSession.status == "enabled" &&
                    decodedSession.expiresAtMs == 4102444800000LL &&
                    decodedSession.scopes.contains("device:realtime") &&
                    decodedSession.scopes.contains("device:history") &&
                    decodedSession.deviceIds.contains(
                        "019fd9f6-4be5-7272-a194-9e571bce848e"),
                "projected AccessKey session round-trip changed");
        require(!service::message::access::session::expired(decodedSession, 4102444799999LL) &&
                    service::message::access::session::expired(decodedSession, 4102444800000LL),
                "projected AccessKey expiration boundary changed");
        bool invalidSessionExpirationRejected = false;
        try {
            (void)service::message::access::session::decode(service::message::access::session::encode(
                "019fd9f6-4be5-7272-a194-9e571bce848d", "production key", "enabled",
                "4102444800000x", R"(["device:realtime"])", R"([])"));
        } catch (const std::runtime_error&) {
            invalidSessionExpirationRejected = true;
        }
        require(invalidSessionExpirationRejected,
                "projected AccessKey session accepted a non-decimal expiration");
        const auto queryOnlyUrl = service::access::parseWebhookUrl(
            "https://example.test?token=delivery");
        require(queryOnlyUrl.target == "/?token=delivery",
                "query-only webhook URL produced an invalid HTTP request target");
        const auto ipv6Url =
            service::access::parseWebhookUrl("https://[2001:db8::1]:8443/callback");
        const auto ipv6Request =
            service::access::DeliveryClient::request(ipv6Url, "{}", {});
        require(ipv6Request.find("\r\nHost: [2001:db8::1]:8443\r\n") != std::string::npos,
                "IPv6 webhook request produced an invalid Host header");
        for (const auto& headers :
             {std::vector<std::pair<std::string, std::string>>{{"X-Trace", "safe\r\nX-Injected: yes"}},
              std::vector<std::pair<std::string, std::string>>{{"Content-Length", "0"}}}) {
            bool rejected = false;
            try {
                (void)service::access::DeliveryClient::request(ipv6Url, "{}", headers);
            } catch (const std::invalid_argument&) {
                rejected = true;
            }
            require(rejected, "unsafe stored webhook header reached the HTTP wire request");
        }
        require(service::access::managedWebhookHeader("Transfer-Encoding", true) &&
                    service::access::managedWebhookHeader("X-IOT-Signature", true),
                "custom webhook headers could override transport or signature metadata");
        require(service::access::DeliveryClient::tlsVerifyMode(false) ==
                        asio::ssl::verify_peer &&
                    service::access::DeliveryClient::tlsVerifyMode(true) ==
                        asio::ssl::verify_none,
                "webhook TLS verification opt-out policy changed");
        bool unsafeUrlRejected = false;
        try {
            (void)service::access::parseWebhookUrl("https://example.test/callback\r\nX-Injected: yes");
        } catch (const std::invalid_argument&) {
            unsafeUrlRejected = true;
        }
        require(unsafeUrlRejected, "stored webhook URL reached the HTTP request line with CRLF");
        for (const auto response : {"NOT-HTTP 200 OK\r\n\r\n",
                                    "HTTP/1.1 2000 Invalid\r\n\r\n",
                                    "HTTP/1.1 200 OK\r\n"}) {
            bool rejected = false;
            try {
                (void)service::access::DeliveryClient::parseResponse(response);
            } catch (const std::runtime_error&) {
                rejected = true;
            }
            require(rejected, "malformed webhook response was accepted as HTTP success");
        }
        const auto informational = service::access::DeliveryClient::parseResponse(
            "HTTP/1.1 103 Early Hints\r\nLink: </schema>; rel=preload\r\n\r\n"
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        require(informational.status == 200 && informational.body == "ok",
                "webhook client treated an informational response as the final result");
        std::cout << "open access tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "open access test failed: " << error.what() << '\n';
        return 1;
    }
}
