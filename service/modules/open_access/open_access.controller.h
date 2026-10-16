#pragma once

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/live.h"
#include "service/middleware/permission.h"
#include "service/middleware/validation.h"
#include "service/modules/command/command.service.h"
#include "service/modules/device/device.types.h"
#include "service/modules/open_access/open_access.types.h"
#include "service/modules/open_access/open_access.service.h"
#include "service/utils/change_feed.h"
#include "service/utils/json.h"
#include "service/utils/text.h"

namespace service::access {

inline ruvia::http_response jsonData(ruvia::context& c, std::string_view data, std::string_view message = "ok") {
    std::pmr::string body(c.allocator<char>());
    body.append("{\"code\":0,\"message\":");
    body.append(service::utils::jsonQuoted(message));
    body.append(",\"data\":");
    body.append(data);
    body.push_back('}');
    auto response = c.body(std::move(body));
    response.header("Content-Type", "application/json; charset=UTF-8");
    return response;
}

class AccessAdminController final {
  public:
    RUVIA_CONTROLLER_GROUP("/api", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(AccessAdminController)
    RUVIA_GET("/device/options", devices, service::middleware::PermissionMiddleware<"iot:open-access:query">);
    RUVIA_GET("/open-access-key", keys, service::middleware::PermissionMiddleware<"iot:open-access:query">);
    RUVIA_GET("/open-webhook", webhooks, service::middleware::PermissionMiddleware<"iot:open-access:query">, ruvia::query_model<WebhookQueryParams>);
    RUVIA_GET("/open-access-log", logs, service::middleware::PermissionMiddleware<"iot:open-access:query">, ruvia::query_model<AccessLogParams>);
    RUVIA_POST("/open-access-key", createKey, service::middleware::PermissionMiddleware<"iot:open-access:add">, service::middleware::ValidationErrorCodeMiddleware<19002>, ruvia::json_body<CreateAccessKeyBody>);
    RUVIA_PUT("/open-access-key/:id", updateKey, service::middleware::PermissionMiddleware<"iot:open-access:edit">, ruvia::path_model<AccessIdParams>, service::middleware::ValidationErrorCodeMiddleware<19002>, ruvia::json_body<UpdateAccessKeyBody>);
    RUVIA_POST("/open-access-key/:id/rotate", rotateKey, service::middleware::PermissionMiddleware<"iot:open-access:edit">, ruvia::path_model<AccessIdParams>);
    RUVIA_DELETE("/open-access-key/:id", removeKey, service::middleware::PermissionMiddleware<"iot:open-access:delete">, ruvia::path_model<AccessIdParams>);
    RUVIA_POST("/open-webhook", createWebhook, service::middleware::PermissionMiddleware<"iot:open-access:add">, service::middleware::ValidationErrorCodeMiddleware<19002>, ruvia::json_body<CreateWebhookBody>);
    RUVIA_PUT("/open-webhook/:id", updateWebhook, service::middleware::PermissionMiddleware<"iot:open-access:edit">, ruvia::path_model<AccessIdParams>, service::middleware::ValidationErrorCodeMiddleware<19002>, ruvia::json_body<UpdateWebhookBody>);
    RUVIA_DELETE("/open-webhook/:id", removeWebhook, service::middleware::PermissionMiddleware<"iot:open-access:delete">, ruvia::path_model<AccessIdParams>);
    RUVIA_GET("/open-mqtt", mqtt, service::middleware::PermissionMiddleware<"iot:open-access:query">, ruvia::query_model<WebhookQueryParams>);
    RUVIA_POST("/open-mqtt", createMqtt, service::middleware::PermissionMiddleware<"iot:open-access:add">, service::middleware::ValidationErrorCodeMiddleware<19002>, ruvia::json_body<MqttSaveBody>);
    RUVIA_PUT("/open-mqtt/:id", updateMqtt, service::middleware::PermissionMiddleware<"iot:open-access:edit">, ruvia::path_model<AccessIdParams>, service::middleware::ValidationErrorCodeMiddleware<19002>, ruvia::json_body<MqttSaveBody>);
    RUVIA_DELETE("/open-mqtt/:id", removeMqtt, service::middleware::PermissionMiddleware<"iot:open-access:delete">, ruvia::path_model<AccessIdParams>);
    RUVIA_ROUTES_END
  private:
    static std::string id(ruvia::context& c) {
        return std::string(c.req().validated<AccessIdParams>().get<"id">().view());
    }

    ruvia::task<ruvia::http_response> devices(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = co_await devicesSnapshot(c, request);
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<std::string> devicesSnapshot(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::middleware::dataJson(co_await accessService().deviceOptions(request));
    }

    ruvia::task<ruvia::http_response> keys(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = co_await keysSnapshot(c, request);
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<std::string> keysSnapshot(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::middleware::dataJson(co_await accessService().listKeys(request));
    }

    ruvia::task<ruvia::http_response> webhooks(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = co_await webhooksSnapshot(c, request);
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<std::string> webhooksSnapshot(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::middleware::dataJson(co_await accessService().listWebhooks(request, c.req().validated<WebhookQueryParams>()));
    }

    ruvia::task<ruvia::http_response> logs(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = co_await logsSnapshot(c, request);
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<std::string> logsSnapshot(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::middleware::dataJson(co_await accessService().listLogs(request, c.req().validated<AccessLogParams>()));
    }

    ruvia::task<ruvia::http_response> createKey(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<CreateAccessKeyBody>();
        co_return jsonData(c, co_await accessService().createKey(request, body));
    }

    ruvia::task<ruvia::http_response> updateKey(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await accessService().updateKey(request, id(c), c.req().validated<UpdateAccessKeyBody>());
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> rotateKey(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return jsonData(c, co_await accessService().rotateKey(request, id(c)));
    }

    ruvia::task<ruvia::http_response> removeKey(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await accessService().removeKey(request, id(c));
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> createWebhook(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<CreateWebhookBody>();
        co_return jsonData(c, co_await accessService().createWebhook(request, body));
    }

    ruvia::task<ruvia::http_response> updateWebhook(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await accessService().updateWebhook(request, id(c), c.req().validated<UpdateWebhookBody>());
        co_return c.json(service::common::operation(c, "操作成功"));
    }

    ruvia::task<ruvia::http_response> mqtt(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        auto data = co_await accessService().listMqtt(request,c.req().validated<WebhookQueryParams>());
        co_return c.json(service::common::ok<MqttListResponse>(c,std::move(data)));
    }
    ruvia::task<ruvia::http_response> createMqtt(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await accessService().saveMqtt(request,std::nullopt,c.req().validated<MqttSaveBody>());
        co_return c.json(service::common::operation(c,"MQTT 配置已创建"));
    }
    ruvia::task<ruvia::http_response> updateMqtt(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await accessService().saveMqtt(request,id(c),c.req().validated<MqttSaveBody>());
        co_return c.json(service::common::operation(c,"MQTT 配置已更新"));
    }
    ruvia::task<ruvia::http_response> removeMqtt(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await accessService().removeMqtt(request,id(c));
        co_return c.json(service::common::operation(c,"MQTT 配置已删除"));
    }

    ruvia::task<ruvia::http_response> removeWebhook(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await accessService().removeWebhook(request, id(c));
        co_return c.json(service::common::operation(c, "操作成功"));
    }
};

class AccessController final {
  public:
    RUVIA_CONTROLLER_GROUP("/open-api/device")
    RUVIA_ROUTES_BEGIN(AccessController)
    RUVIA_GET_SSE("/list", devices);
    RUVIA_GET_SSE("/realtime", realtime);
    RUVIA_GET_SSE("/history", history);
    RUVIA_POST("/command", command);
    RUVIA_GET_SSE("/alert", alerts);
    RUVIA_ROUTES_END

  private:
    // True when `topic` changed as a whole or for an id accepted by `accepts`.
    template <typename Accepts>
    static bool touches(const service::utils::ChangeSet& changes, std::string_view topic, Accepts accepts) {
        const auto* ids = changes.find(topic);
        if (!ids) {
            return false;
        }
        if (!*ids) {
            return true;
        }
        for (const auto& id : **ids) {
            if (accepts(id)) {
                return true;
            }
        }
        return false;
    }

    // Third-party snapshot stream. The wire (snapshot/id/retry/heartbeat, expiry,
    // JSON mode) is a public contract; `relevant` decides which committed changes
    // of `topics` affect this key's result. Access key/auth changes and feed
    // recovery always re-query because they may change the authorized scope.
    template <typename Query, typename Relevant>
    static ruvia::task<void> servePublicQuery(ruvia::context& context, std::vector<std::string> topics, Query query, Relevant relevant, std::function<ruvia::task<void>()> authorize, std::chrono::milliseconds coalesceDelay = {}) {
        using namespace std::chrono_literals;
        if (context.req().header("Accept").value_or("").find("application/json") != std::string_view::npos) {
            co_await authorize();
            const auto snapshot = co_await query();
            context.header("Content-Type", "application/json");
            context.header("Cache-Control", "no-store");
            context.header("Vary", "Accept");
            context.header("X-Snapshot-Topic", service::message::live::topic::kAccess);
            context.header("X-Snapshot-Coalesce-Ms", std::to_string(coalesceDelay.count()));
            context.respond(context.body(std::string_view(snapshot)));
            co_return;
        }
        if (context.req().header("Accept").value_or("").find("text/event-stream") ==
            std::string_view::npos) {
            service::common::fail(10002, "This query requires text/event-stream", 406);
        }
        topics.emplace_back(service::message::live::topic::kAuth);
        topics.emplace_back(service::message::live::topic::kAccess);
        auto subscription = co_await context.worker_state<service::utils::ChangeFeed>().watch(context, std::move(topics));
        auto snapshot = co_await query();
        context.header("X-Accel-Buffering", "no");
        context.header("Cache-Control", "no-store");
        auto stream = context.stream_sse();
        std::uint64_t revision = 1;
        auto id = std::to_string(revision);
        co_await stream.write({ .data_ = snapshot, .event_ = "snapshot", .id_ = id, .retry_ = 1s });
        snapshot.clear();
        // Bound request-arena retention. Reconnection creates a new authorized
        // snapshot; event IDs are connection-local, never misleading replay cursors.
        const auto expires = std::chrono::steady_clock::now() + (coalesceDelay.count() > 0 ? 1min : 5min);
        auto heartbeat = std::chrono::steady_clock::now() + 15s;
        while (!stream.aborted() && std::chrono::steady_clock::now() < expires) {
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(heartbeat - std::chrono::steady_clock::now());
            const auto woke = co_await subscription->wait(std::max(remaining, std::chrono::milliseconds(0)), context.get_stop_token());
            if (stream.aborted() || context.get_stop_token().stop_requested() || woke == service::utils::ChangeWait::Stopped) {
                co_return;
            }
            const bool changed = woke == service::utils::ChangeWait::Changed;
            if (changed && coalesceDelay.count() > 0) {
                (void)co_await ruvia::sleep_for(context.worker(), coalesceDelay, context.get_stop_token());
                if (stream.aborted() || context.get_stop_token().stop_requested()) {
                    co_return;
                }
            }
            std::string error;
            std::string next;
            try {
                // Every wake rechecks the key; permission changes publish auth or
                // access events, which reexecute the authorized query immediately.
                co_await authorize();
                if (changed) {
                    const auto changes = subscription->take();
                    if (changes.recovered || changes.find(service::message::live::topic::kAuth) ||
                        changes.find(service::message::live::topic::kAccess) || relevant(changes)) {
                        next = co_await query();
                    }
                }
            } catch (const ruvia::http_error& failure) {
                const auto info = failure.info();
                error = service::middleware::modelJson(service::common::error(context, service::common::errorCode(info.code(), info.status().value()), info.message()));
            } catch (const std::exception&) {
                error = "{\"code\":10004,\"message\":\"Subscription interrupted\"}";
            }
            if (!error.empty()) {
                co_await stream.write({ .data_ = error, .event_ = "error" });
                co_return;
            }
            if (!next.empty()) {
                id = std::to_string(++revision);
                co_await stream.write({ .data_ = next, .event_ = "snapshot", .id_ = id });
                heartbeat = std::chrono::steady_clock::now() + 15s;
            } else if (std::chrono::steady_clock::now() >= heartbeat) {
                co_await stream.write({ .data_ = "{}", .event_ = "heartbeat" });
                heartbeat = std::chrono::steady_clock::now() + 15s;
            }
        }
    }

    // Ids of devices whose change can alter a result bound to `session`.
    static auto boundDevice(const std::shared_ptr<AccessSession>& session) {
        return [session](const std::string& id) { return session->allowsDevice(id); };
    }

    static auto requestedDevice(ruvia::context& c) {
        return [id = std::string(service::utils::trim(c.req().query("deviceId").value_or("")))](const std::string& changed) { return changed == id; };
    }

    ruvia::task<void> devices(ruvia::context& c) {
        bool audited = false;
        auto session = std::make_shared<AccessSession>();
        co_await servePublicQuery(c, { std::string(service::message::live::topic::kDevice) },
            [this, &c, &audited, session]() { return devicesSnapshot(c, audited, *session); },
            [session](const service::utils::ChangeSet& changes) {
                return touches(changes, service::message::live::topic::kDevice, boundDevice(session));
            },
            [&c, session]() -> ruvia::task<void> { *session = co_await accessService().authenticate(c, {}); });
    }

    ruvia::task<std::string> devicesSnapshot(ruvia::context& c, bool& audited, AccessSession& session) {
        session = co_await accessService().authenticate(c, {});
        const auto data = co_await accessService().publicDevices(c, session);
        if (!audited) {
            co_await accessService().audit(c, "device-list", session, std::string_view{}, "{}", data);
            audited = true;
        }
        co_return service::middleware::dataJson(data);
    }

    ruvia::task<void> realtime(ruvia::context& c) {
        bool audited = false;
        co_await servePublicQuery(c,
            { std::string(service::message::live::topic::kDeviceRealtime), std::string(service::message::live::topic::kDeviceState), std::string(service::message::live::topic::kDevice) },
            [this, &c, &audited]() { return realtimeSnapshot(c, audited); },
            [device = requestedDevice(c)](const service::utils::ChangeSet& changes) {
                return touches(changes, service::message::live::topic::kDeviceRealtime, device) ||
                    touches(changes, service::message::live::topic::kDeviceState, device) ||
                    touches(changes, service::message::live::topic::kDevice, device);
            },
            [&c]() -> ruvia::task<void> { (void)co_await accessService().authenticate(c, kScopeRealtime); });
    }
    ruvia::task<std::string> realtimeSnapshot(ruvia::context& c, bool& audited) {
        const auto session = co_await accessService().authenticate(c, kScopeRealtime);
        const auto id = service::utils::trim(c.req().query("deviceId").value_or(""));
        const auto data = co_await accessService().publicRealtime(c, session, id);
        if (!audited) {
            co_await accessService().audit(c, "realtime", session, id, "{}", data);
            audited = true;
        }
        co_return service::middleware::dataJson(data);
    }

    ruvia::task<void> history(ruvia::context& c) {
        bool audited = false;
        co_await servePublicQuery(c, { std::string(service::message::live::topic::kDeviceRealtime) },
            [this, &c, &audited]() { return historySnapshot(c, audited); },
            [device = requestedDevice(c)](const service::utils::ChangeSet& changes) {
                return touches(changes, service::message::live::topic::kDeviceRealtime, device);
            },
            [&c]() -> ruvia::task<void> { (void)co_await accessService().authenticate(c, kScopeHistory); });
    }

    ruvia::task<std::string> historySnapshot(ruvia::context& c, bool& audited) {
        const auto session = co_await accessService().authenticate(c, kScopeHistory);
        const auto id = service::utils::trim(c.req().query("deviceId").value_or(""));
        const auto data = co_await accessService().publicHistory(c, session, id);
        if (!audited) {
            co_await accessService().audit(c, "history", session, id, "{}", data);
            audited = true;
        }
        co_return service::middleware::dataJson(data);
    }

    ruvia::task<ruvia::http_response> command(ruvia::context& c) {
        const auto session = co_await accessService().authenticate(c, kScopeCommand);
        const auto parsedBody = co_await c.req().json_if<service::device::DeviceCommandBody>();
        if (!parsedBody) {
            throw ruvia::http_error({ .status_ = ruvia::http_status::unsupported_media_type,
                .code_ = "unsupported_media_type", .message_ = "request body must be application/json" });
        }
        const auto& body = *parsedBody;
        const auto id =
            body.get<"deviceId">() ? std::string(body.get<"deviceId">()->view()) : std::string{};
        service::common::requireUuid(19002, id, "设备 ID 无效");
        if (!session.allowsDevice(id)) {
            service::common::fail(19011, "AccessKey 无权控制该设备", 403);
        }
        auto result = co_await service::command::commandService().createExternal(
            c,
            id,
            body,
            session.id
        );
        co_await accessService().audit(c, "command", session, id);
        co_return c.json(service::common::ok<service::device::DeviceCommandCreateResponse>(c, std::move(result)));
    }

    ruvia::task<void> alerts(ruvia::context& c) {
        bool audited = false;
        auto session = std::make_shared<AccessSession>();
        co_await servePublicQuery(c, { std::string(service::message::live::topic::kAlert), std::string(service::message::live::topic::kDevice) },
            [this, &c, &audited, session]() { return alertsSnapshot(c, audited, *session); },
            [session](const service::utils::ChangeSet& changes) {
                // Alert record ids cannot be mapped to devices without a query.
                return changes.find(service::message::live::topic::kAlert) != nullptr ||
                    touches(changes, service::message::live::topic::kDevice, boundDevice(session));
            },
            [&c, session]() -> ruvia::task<void> { *session = co_await accessService().authenticate(c, kScopeAlert); });
    }

    ruvia::task<std::string> alertsSnapshot(ruvia::context& c, bool& audited, AccessSession& session) {
        session = co_await accessService().authenticate(c, kScopeAlert);
        const auto data = co_await accessService().publicAlerts(c, session);
        if (!audited) {
            co_await accessService().audit(c, "alert", session, std::string_view{}, "{}", data);
            audited = true;
        }
        co_return service::middleware::dataJson(data);
    }
};

} // namespace service::access
