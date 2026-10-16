#pragma once

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <memory_resource>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/web/controller.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/permission.h"
#include "service/modules/device/device.service.h"
#include "service/modules/device/device.types.h"

namespace service::device {

class DeviceController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/device", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(DeviceController)
    // 共享流按频道独立鉴权，不能把设备、分组及用户频道改成整体 AND 权限。
    RUVIA_GET_SSE("/events", deviceEvents, ruvia::query_model<DeviceEventsQuery>);
    RUVIA_PUT("/:id/debug", setDebug, service::middleware::PermissionMiddleware<"iot:device:edit">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<DeviceDebugBody>);
    // 设备
    RUVIA_GET("/options", options, service::middleware::PermissionMiddleware<"iot:device:query">);
    // 设备分组（统一收编到 /v1/device/groups）
    RUVIA_GET("/groups/:id/shares", groupShares, service::middleware::PermissionMiddleware<"iot:device-group:share">, ruvia::path_model<DeviceIdParams>);
    RUVIA_GET("/groups/:id/share-targets", groupShareTargets, service::middleware::PermissionMiddleware<"iot:device-group:share">, ruvia::path_model<DeviceIdParams>);
    RUVIA_PUT("/groups/:id/shares", replaceGroupShares, service::middleware::PermissionMiddleware<"iot:device-group:share">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<ReplaceDeviceSharesBody>);
    RUVIA_POST("/groups", groupCreate, service::middleware::PermissionMiddleware<"iot:device-group:add">, ruvia::json_body<CreateDeviceGroupBody>);
    RUVIA_PUT("/groups/:id", groupUpdate, service::middleware::PermissionMiddleware<"iot:device-group:edit">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<UpdateDeviceGroupBody>);
    RUVIA_DELETE("/groups/:id", groupRemove, service::middleware::PermissionMiddleware<"iot:device-group:delete">, ruvia::path_model<DeviceIdParams>);
    RUVIA_GET("/:id/history", history, service::middleware::PermissionMiddleware<"iot:device:query">, ruvia::path_model<DeviceIdParams>, ruvia::query_model<DeviceHistoryQuery>);
    RUVIA_GET("/:id/shares", shares, service::middleware::PermissionMiddleware<"iot:device:share">, ruvia::path_model<DeviceIdParams>);
    RUVIA_GET("/:id/share-targets", shareTargets, service::middleware::PermissionMiddleware<"iot:device:share">, ruvia::path_model<DeviceIdParams>);
    RUVIA_PUT("/:id/shares", replaceShares, service::middleware::PermissionMiddleware<"iot:device:share">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<ReplaceDeviceSharesBody>);
    // 设备（带参数的通配路由放在静态路由之后）
    RUVIA_GET("/:id", detail, service::middleware::PermissionMiddleware<"iot:device:query">, ruvia::path_model<DeviceIdParams>);
    RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"iot:device:query">);
    RUVIA_POST("/", create, service::middleware::PermissionMiddleware<"iot:device:add">, ruvia::json_body<CreateDeviceBody>);
    RUVIA_PUT("/:id", update, service::middleware::PermissionMiddleware<"iot:device:edit">, ruvia::path_model<DeviceIdParams>, ruvia::json_body<UpdateDeviceBody>);
    RUVIA_DELETE("/:id", remove, service::middleware::PermissionMiddleware<"iot:device:delete">, ruvia::path_model<DeviceIdParams>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<DeviceCommandStatusesResponse> queryCommandStatuses(service::middleware::RequestContext& request, std::string_view ids) {
        co_return service::common::ok<DeviceCommandStatusesResponse>(request, co_await deviceService().commandStatuses(request, ids));
    }

    ruvia::task<DeviceDebugPacketsResponse> queryDebugPackets(service::middleware::RequestContext& request, std::string_view deviceId) {
        co_return service::common::ok<DeviceDebugPacketsResponse>(request, co_await deviceService().debugPackets(request, deviceId));
    }

    ruvia::task<ruvia::http_response> setDebug(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().setDebug(request, id(c), c.req().validated<DeviceDebugBody>().get<"enabled">());
        co_return c.json(service::common::operation(c, "调试设置已保存"));
    }

    static std::string id(ruvia::context& c) {
        return std::string(c.req().validated<DeviceIdParams>().get<"id">().view());
    }

    // ---- 设备 ----
    ruvia::task<ruvia::http_response> list(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(co_await queryList(c, request));
    }

    ruvia::task<void> deviceEvents(ruvia::context& c) {
        namespace topic = service::message::live::topic;
        const auto& events = c.req().validated<DeviceEventsQuery>();
        std::vector<service::middleware::LiveChannel> channels;
        channels.push_back(deviceRows<DevicePageResponse>("devices", { std::string(topic::kDevice), std::string(topic::kDeviceState), std::string(topic::kDeviceGroup), std::string(topic::kLink), std::string(topic::kProtocol), std::string(topic::kEdge) }, [](service::middleware::RequestContext& request, const std::vector<std::string>* ids) {
            return deviceService().list(request, ids);
        }));
        // 分组树的设备计数随设备增删、换组变化；设备主题只来自配置写入，不含遥测。
        channels.push_back({ "groups", { std::string(topic::kDeviceGroup), std::string(topic::kDevice) }, [](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                                co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:device-group:query");
                                co_return service::middleware::modelJson(service::common::ok<DeviceGroupListResponse>(request, co_await deviceService().listGroups(request)));
                            },
                             {} });
        // device-group：分组改挂父级会改变继承的共享范围，受影响设备按子树重新判定可见性。
        channels.push_back(deviceRows<DeviceRealtimeResponse>("realtime", { std::string(topic::kDeviceRealtime), std::string(topic::kDeviceState), std::string(topic::kDevice), std::string(topic::kDeviceGroup), std::string(topic::kLink), std::string(topic::kProtocol), std::string(topic::kEdge) }, [](service::middleware::RequestContext& request, const std::vector<std::string>* ids) {
            return deviceService().realtime(request, ids);
        }));
        std::string debugId;
        if (const auto& debugDeviceId = events.get<"debugDeviceId">()) {
            debugId = std::string(debugDeviceId->view());
            auto snapshot = [this, deviceId = debugId](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:device:edit");
                co_return service::middleware::modelJson(co_await queryDebugPackets(request, deviceId));
            };
            auto update = [snapshot, deviceId = debugId](service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
                if (!touches(changes, topic::kPacketDebug, { deviceId })) {
                    co_return std::nullopt;
                }
                co_return co_await snapshot(request);
            };
            channels.push_back({ "packets", { std::string(topic::kPacketDebug) }, snapshot, update });
        }
        if (const auto& commandIds = events.get<"commandIds">()) {
            std::set<std::string, std::less<>> subscribed;
            for (const auto id : service::message::live::splitIds(commandIds->view())) {
                subscribed.emplace(id);
            }
            auto snapshot = [this, ids = std::string(commandIds->view())](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                co_return service::middleware::modelJson(co_await queryCommandStatuses(request, ids));
            };
            auto update = [snapshot, subscribed = std::move(subscribed)](service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
                if (!touches(changes, topic::kCommand, subscribed)) {
                    co_return std::nullopt;
                }
                co_return co_await snapshot(request);
            };
            channels.push_back({ "commands", { std::string(topic::kCommand) }, snapshot, update });
        }
        co_await service::middleware::serveLive(
            c,
            service::middleware::requireAuth(c).userId,
            std::move(channels),
            [&c] {
                (void)service::middleware::requireAuth(c);
            },
            [&c, debugId]() -> ruvia::task<void> {
                if (!debugId.empty()) {
                    co_await service::debug_idle::touch(c.redis(), "device", debugId);
                }
                co_return;
            }
        );
    }

    // Whether `topic` changed as a whole or for one of `ids`.
    static bool touches(const service::utils::ChangeSet& changes, std::string_view topic, const std::set<std::string, std::less<>>& ids) {
        const auto* changed = changes.find(topic);
        if (changed == nullptr) {
            return false;
        }
        if (!*changed) {
            return true;
        }
        return std::ranges::any_of(**changed, [&ids](const std::string& id) {
            return ids.contains(id);
        });
    }

    // Devices whose rows the changes may alter; nullopt when a topic changed as
    // a whole. Link, protocol, edge and group ids map to the devices showing them.
    static ruvia::task<std::optional<service::utils::ChangeSet::Ids>> changedDevices(service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) {
        namespace topic = service::message::live::topic;
        service::utils::ChangeSet::Ids devices;
        std::vector<std::string> links;
        std::vector<std::string> protocols;
        std::vector<std::string> nodes;
        std::vector<std::string> groups;
        for (const auto& [name, ids] : changes.topics) {
            if (!ids) {
                co_return std::nullopt;
            }
            auto* related = name == topic::kLink ? &links
                : name == topic::kProtocol       ? &protocols
                : name == topic::kEdge           ? &nodes
                : name == topic::kDeviceGroup    ? &groups
                                                 : nullptr;
            if (related != nullptr) {
                related->insert(related->end(), ids->begin(), ids->end());
            } else {
                devices.insert(ids->begin(), ids->end());
            }
        }
        for (auto& id : co_await deviceService().devicesReferencing(request, links, protocols, nodes, groups)) {
            devices.insert(std::move(id));
        }
        co_return devices;
    }

    // Entity collection channel over the scoped device rows (non-paginated):
    // changes re-read only the affected devices and answer with a delta.
    template <typename Response, typename Read>
    static service::middleware::LiveChannel deviceRows(std::string event, std::vector<std::string> topics, Read read) {
        auto visible = std::make_shared<service::middleware::LiveCollection>();
        auto snapshot = [visible, read](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
            co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:device:query");
            auto page = co_await read(request, nullptr);
            visible->assign(rows(page));
            co_return service::middleware::modelJson(service::common::ok<Response>(request, std::move(page)));
        };
        auto update = [visible, read, snapshot](service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
            co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:device:query");
            const auto requested = co_await changedDevices(request, changes);
            if (!requested) {
                co_return co_await snapshot(request);
            }
            const std::vector<std::string> ids(requested->begin(), requested->end());
            const auto page = co_await read(request, &ids);
            auto answer = visible->answer(*requested, rows(page));
            if (answer.outcome == service::middleware::LiveCollection::Outcome::Snapshot) {
                co_return co_await snapshot(request);
            }
            if (answer.outcome == service::middleware::LiveCollection::Outcome::Unchanged) {
                co_return std::nullopt;
            }
            co_return std::move(answer.payload);
        };
        return { std::move(event), std::move(topics), snapshot, update };
    }

    template <typename Page>
    static std::vector<service::middleware::LiveCollection::Row> rows(const Page& page) {
        std::vector<service::middleware::LiveCollection::Row> result;
        if (const auto& list = page.template get<"list">()) {
            for (const auto& item : *list) {
                const auto& id = item.template get<"id">();
                result.push_back({ id ? std::string(id->view()) : std::string(), service::middleware::modelJson(item) });
            }
        }
        return result;
    }

    ruvia::task<DevicePageResponse> queryList(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::common::ok<DevicePageResponse>(request, co_await deviceService().list(request));
    }

    ruvia::task<ruvia::http_response> options(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceOptionsResponse>(request, co_await deviceService().options(request)));
    }

    ruvia::task<ruvia::http_response> detail(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceDetailResponse>(request, co_await deviceService().detail(request, id(c))));
    }

    ruvia::task<ruvia::http_response> history(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto payload = co_await historySnapshot(c, request);
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(payload));
    }

    ruvia::task<std::string> historySnapshot(ruvia::context& c, service::middleware::RequestContext& request) {
        co_return service::middleware::dataJson(co_await deviceService().history(request, id(c), c.req().validated<DeviceHistoryQuery>()));
    }

    ruvia::task<ruvia::http_response> create(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().create(request, c.req().validated<CreateDeviceBody>());
        co_return c.json(service::common::operation(c, "创建成功"));
    }

    ruvia::task<ruvia::http_response> update(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().update(request, id(c), c.req().validated<UpdateDeviceBody>());
        co_return c.json(service::common::operation(c, "更新成功"));
    }

    ruvia::task<ruvia::http_response> remove(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().remove(request, id(c));
        co_return c.json(service::common::operation(c, "删除成功"));
    }

    ruvia::task<ruvia::http_response> shares(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceSharesResponse>(request, co_await deviceShareService().list(request, id(c))));
    }

    ruvia::task<ruvia::http_response> shareTargets(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceShareTargetsResponse>(request, co_await deviceShareService().targets(request, id(c))));
    }

    ruvia::task<ruvia::http_response> replaceShares(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceShareService().replace(request, id(c), c.req().validated<ReplaceDeviceSharesBody>());
        co_return c.json(service::common::operation(c, "设备分享已更新"));
    }

    // ---- 设备分组 ----

    ruvia::task<ruvia::http_response> groupShares(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceSharesResponse>(request, co_await deviceShareService().listGroup(request, id(c))));
    }

    ruvia::task<ruvia::http_response> groupShareTargets(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<DeviceShareTargetsResponse>(request, co_await deviceShareService().groupTargets(request, id(c))));
    }

    ruvia::task<ruvia::http_response> replaceGroupShares(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceShareService().replaceGroup(request, id(c), c.req().validated<ReplaceDeviceSharesBody>());
        co_return c.json(service::common::operation(c, "设备分组分享已更新"));
    }

    ruvia::task<ruvia::http_response> groupCreate(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().createGroup(request, c.req().validated<CreateDeviceGroupBody>());
        co_return c.json(service::common::operation(c, "创建成功"));
    }

    ruvia::task<ruvia::http_response> groupUpdate(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().updateGroup(request, id(c), c.req().validated<UpdateDeviceGroupBody>());
        co_return c.json(service::common::operation(c, "更新成功"));
    }

    ruvia::task<ruvia::http_response> groupRemove(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_await deviceService().removeGroup(request, id(c));
        co_return c.json(service::common::operation(c, "删除成功"));
    }
};

} // namespace service::device
