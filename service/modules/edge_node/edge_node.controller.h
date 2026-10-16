#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/web/controller.h>
#include <ruvia/web/db/db_handle.h>
#include <ruvia/web/redis/redis_handle.h>
#include <ruvia/web/validation.h>

#include "service/common/http.h"
#include "service/middleware/auth.h"
#include "service/middleware/live.h"
#include "service/middleware/log.h"
#include "service/middleware/permission.h"
#include "service/middleware/request_context.h"
#include "service/modules/edge_node/edge_node.service.h"
#include "service/modules/edge_node/edge_node.types.h"

namespace service::edge_node {


class EdgeController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/edge")
    RUVIA_ROUTES_BEGIN(EdgeController)
    const auto options = ruvia::websocket_route_config{ .lifecycle_ = {
                                                            .heartbeat_ = { .ping_interval_ = std::chrono::seconds(30), .pong_timeout_ = std::chrono::seconds(15) },
                                                            .close_handshake_timeout_ = std::chrono::seconds(5) } };
    RUVIA_GET_WS_OPTIONS("/debug", debugConnection, options);
    RUVIA_ROUTES_END

  private:
    using Registry = service::edge_node::debug::EventRegistry<service::edge_node::debug::EventContext>;

    static void registerDebugOperations(Registry& registry) {
        using service::edge_node::debug::invokeEvent;
        auto* handlers = &edgeService();
        registry.add("edge.debug.authenticate", handlers, &invokeEvent<EdgeService, &EdgeService::authenticateDebugConnection, DebugAuthenticationBody>, { .authenticated = false });
        registry.add("edge.serial.open", handlers, &invokeEvent<EdgeService, &EdgeService::openSerial, NodeSerialOpenInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.serial.command", handlers, &invokeEvent<EdgeService, &EdgeService::sendSerialCommand, NodeSerialCommandInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.serial.events.subscribe", handlers, &invokeEvent<EdgeService, &EdgeService::serialEvents, NodeSerialSessionInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.serial.close", handlers, &invokeEvent<EdgeService, &EdgeService::closeSerial, NodeSerialSessionInput>);
        registry.add("edge.terminal.open", handlers, &invokeEvent<EdgeService, &EdgeService::openTerminal, TerminalOpenInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.terminal.events.subscribe", handlers, &invokeEvent<EdgeService, &EdgeService::terminalEvents, TerminalSessionInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.terminal.write", handlers, &invokeEvent<EdgeService, &EdgeService::writeTerminal, TerminalWriteInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.terminal.resize", handlers, &invokeEvent<EdgeService, &EdgeService::resizeTerminal, TerminalResizeInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.terminal.keepalive", handlers, &invokeEvent<EdgeService, &EdgeService::keepTerminalAlive, TerminalSessionInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.terminal.output.ack", handlers, &invokeEvent<EdgeService, &EdgeService::acknowledgeTerminalOutput, TerminalAckInput>, { .permission = "iot:edge:terminal" });
        registry.add("edge.terminal.close", handlers, &invokeEvent<EdgeService, &EdgeService::closeTerminal, TerminalSessionInput>);
    }

    ruvia::task<void> debugConnection(ruvia::context& context) {
        Registry registry;
        registerDebugOperations(registry);
        registry.seal();
        DebugConnection session(context, registry);
        co_await session.run();
    }

};

class EdgeManagementController final {
  public:
    RUVIA_CONTROLLER_GROUP("/v1/edge", service::middleware::AuthMiddleware)
    RUVIA_ROUTES_BEGIN(EdgeManagementController)
    RUVIA_GET_SSE("/events", pageEvents, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::query_model<EdgeEventsQuery>, ruvia::query_model<LogsQuery>);
    RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::query_model<EdgeListQuery>);
    RUVIA_GET("/:id", detail, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<EdgeIdParams>);
    RUVIA_GET("/groups", groups, service::middleware::PermissionMiddleware<"iot:edge:query">);
    RUVIA_POST("/groups", createGroup, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::json_body<EdgeGroupBody>);
    RUVIA_PUT("/groups/:id", updateGroup, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<EdgeGroupBody>);
    RUVIA_DELETE("/groups/:id", removeGroup, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>);
    RUVIA_PUT("/:id/enrollment", enrollment, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<EnrollmentBody>);
    RUVIA_DELETE("/:id", removeEnrollment, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>);
    RUVIA_PUT("/:id/name", renameNode, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<NodeNameBody>);
    RUVIA_PUT("/:id/group", setNodeGroup, service::middleware::PermissionMiddleware<"iot:edge:edit">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<NodeGroupBody>);
    RUVIA_POST("/:id/network", network, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<NetworkBody>);
    RUVIA_GET("/:id/dtu", dtuChannels, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<EdgeIdParams>);
    RUVIA_PUT("/:id/dtu", saveDtuChannel, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<DtuChannelBody>);
    RUVIA_DELETE("/:id/dtu/:channelId", deleteDtuChannel, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<DtuChannelParams>);
    RUVIA_POST("/:id/sync", sync, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<EdgeIdParams>);
    RUVIA_POST_STREAM("/:id/firmware", uploadFirmware, service::middleware::PermissionMiddleware<"iot:edge:firmware">, ruvia::path_model<EdgeIdParams>, ruvia::query_model<FirmwareUploadBody>);
    RUVIA_POST("/:id/firmware/reuse", reuseFirmware, service::middleware::PermissionMiddleware<"iot:edge:firmware">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<FirmwareReuseBody>);
    RUVIA_POST("/:id/logs/capture", captureLogs, service::middleware::PermissionMiddleware<"iot:edge:query">, ruvia::path_model<EdgeIdParams>);
    RUVIA_PUT("/:id/logs/level", logLevel, service::middleware::PermissionMiddleware<"iot:edge:config">, ruvia::path_model<EdgeIdParams>, ruvia::json_body<LogLevelBody>);
    RUVIA_ROUTES_END
  private:
    static std::optional<std::string> text(const std::optional<ruvia::string>& value) {
        return value ? std::optional<std::string>(std::string(value->view())) : std::nullopt;
    }

    // True when `topic` changed as a whole or for entity `id`.
    static bool touches(const service::utils::ChangeSet& changes, std::string_view topic, std::string_view id) {
        const auto* ids = changes.find(topic);
        return ids && (!*ids || (*ids)->contains(id));
    }

    static bool touchesAny(const service::utils::ChangeSet::Ids& changed, const service::utils::ChangeSet::Ids& shown) {
        for (const auto& id : changed) {
            if (shown.contains(id)) {
                return true;
            }
        }
        return false;
    }

    // Inventory rows in snapshot shape; records nodes shown with VPN routes.
    static std::vector<service::middleware::LiveCollection::Row> nodeRows(const ruvia::boxed_array<EdgeNodeDto>& nodes, service::utils::ChangeSet::Ids& vpnNodes) {
        std::vector<service::middleware::LiveCollection::Row> rows;
        for (const auto& node : nodes) {
            std::string id(node.get<"id">()->view());
            const auto& cidrs = node.get<"vpnVirtualCidrs">();
            if (cidrs && cidrs->begin() != cidrs->end()) {
                vpnNodes.insert(id);
            } else {
                vpnNodes.erase(id);
            }
            rows.push_back({ std::move(id), service::middleware::modelJson(node) });
        }
        return rows;
    }

    ruvia::task<void> pageEvents(ruvia::context& c) {
        namespace topic = service::message::live::topic;
        const auto& scope = c.req().validated<EdgeEventsQuery>();
        const auto nodeId = text(scope.get<"nodeId">());
        if ((*scope.get<"logs">() || *scope.get<"vpn">()) && !nodeId) {
            service::common::fail(17003, "实时范围缺少节点 ID", 400);
        }

        // Complete inventory (not paginated): later changes of node, group or
        // VPN route fields become deltas of the affected nodes only.
        struct Inventory final {
            service::middleware::LiveCollection collection;
            service::utils::ChangeSet::Ids vpnNodes;
        };

        const auto inventory = std::make_shared<Inventory>();
        auto nodes = [inventory](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
            co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
            const auto all = co_await edgeService().inventory(request);
            inventory->vpnNodes.clear();
            const auto rows = nodeRows(all, inventory->vpnNodes);
            inventory->collection.assign(rows);
            std::string array{ "[" };
            for (const auto& row : rows) {
                if (array.size() > 1) {
                    array += ',';
                }
                array += row.json;
            }
            co_return service::middleware::dataJson(array + "]");
        };
        std::vector<service::middleware::LiveChannel> channels;
        channels.push_back({ "nodes", { std::string(topic::kEdge), std::string(topic::kEdgeGroup), std::string(topic::kVpn) }, nodes, [nodes, inventory](service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
                                co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                                const auto ids = co_await edgeService().changedNodeIds(request, changes, inventory->vpnNodes);
                                if (!ids) {
                                    co_return co_await nodes(request);
                                }
                                if (ids->empty()) {
                                    co_return std::nullopt;
                                }
                                const auto changed = co_await edgeService().inventory(request, *ids);
                                auto vpnNodes = inventory->vpnNodes;
                                for (const auto& id : *ids) {
                                    vpnNodes.erase(id);
                                }
                                const auto rows = nodeRows(changed, vpnNodes);
                                auto answer = inventory->collection.answer(*ids, rows);
                                switch (answer.outcome) {
                                    case service::middleware::LiveCollection::Outcome::Unchanged:
                                        inventory->vpnNodes = std::move(vpnNodes);
                                        co_return std::nullopt;
                                    case service::middleware::LiveCollection::Outcome::Delta:
                                        inventory->vpnNodes = std::move(vpnNodes);
                                        co_return std::move(answer.payload);
                                    case service::middleware::LiveCollection::Outcome::Snapshot:
                                        break;
                                }
                                co_return co_await nodes(request);
                            } });
        if (nodeId) {
            const auto id = *nodeId;
            auto detail = [id](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                co_return service::middleware::dataJson(service::middleware::modelJson(co_await edgeService().detail(request, id)));
            };
            channels.push_back({ "detail", { std::string(topic::kEdge) }, detail, [detail, id](service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
                                    if (!touches(changes, topic::kEdge, id)) {
                                        co_return std::nullopt;
                                    }
                                    co_return co_await detail(request);
                                } });
            if (*scope.get<"dtu">()) {
                auto dtu = [id](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                    co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                    co_return service::middleware::dataJson(co_await edgeService().dtuChannels(request, id));
                };
                channels.push_back({ "dtu", { std::string(topic::kEdgeDtu) }, dtu, [dtu, id](service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
                                        if (!touches(changes, topic::kEdgeDtu, id)) {
                                            co_return std::nullopt;
                                        }
                                        co_return co_await dtu(request);
                                    } });
            }
            if (*scope.get<"logs">()) {
                auto logs = [id, &c](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                    co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                    co_return service::middleware::dataJson(service::middleware::modelJson(co_await edgeService().logSnapshot(request, id, c.req().validated<LogsQuery>())));
                };
                channels.push_back({ "logs", { std::string(topic::kEdgeLogs) }, logs, [logs, id](service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
                                        if (!touches(changes, topic::kEdgeLogs, id)) {
                                            co_return std::nullopt;
                                        }
                                        co_return co_await logs(request);
                                    } });
            }
            if (*scope.get<"vpn">()) {
                // VPN changes carry network ids: the view changes when the node
                // held or now holds a peer in one of them.
                const auto networks = std::make_shared<service::utils::ChangeSet::Ids>();
                auto vpn = [id, networks](service::middleware::RequestContext& request) -> ruvia::task<std::string> {
                    co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:query");
                    *networks = co_await edgeService().nodeNetworks(request, id);
                    co_return service::middleware::dataJson(co_await edgeService().vpnState(request, id));
                };
                channels.push_back({ "vpn", { std::string(topic::kVpn), std::string(topic::kEdge) }, vpn, [vpn, id, networks](service::middleware::RequestContext& request, const service::utils::ChangeSet& changes) -> ruvia::task<std::optional<std::string>> {
                                        bool changed = touches(changes, topic::kEdge, id);
                                        if (const auto* ids = changes.find(topic::kVpn); !changed && ids) {
                                            changed = !*ids || touchesAny(**ids, *networks) ||
                                                touchesAny(**ids, co_await edgeService().nodeNetworks(request, id));
                                        }
                                        if (!changed) {
                                            co_return std::nullopt;
                                        }
                                        co_return co_await vpn(request);
                                    } });
            }
        }
        co_await service::middleware::serveLive(c, service::middleware::requireAuth(c).userId, std::move(channels), [&c] {
            (void)service::middleware::requireAuth(c);
        });
    }

    ruvia::task<ruvia::http_response> list(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& query = c.req().validated<EdgeListQuery>();
        auto result = co_await edgeService().list(request, *query.get<"page">(), *query.get<"pageSize">(), text(query.get<"keyword">()), text(query.get<"status">()), text(query.get<"groupId">()));
        co_return c.json(service::common::ok<EdgePageResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> detail(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        auto result = co_await edgeService().detail(request, body.get<"id">().view());
        co_return c.json(service::common::ok<EdgeNodeResponse>(request, std::move(result)));
    }

    ruvia::task<ruvia::http_response> groups(ruvia::context& c) {
        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        co_return c.json(service::common::ok<EdgeGroupsResponse>(request, co_await edgeService().groups(request)));
    }

    ruvia::task<ruvia::http_response> createGroup(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeGroupBody>();
        co_await edgeService().createGroup(request, body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> updateGroup(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeGroupBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().updateGroup(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> removeGroup(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        co_await edgeService().removeGroup(request, body.get<"id">().view());
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> enrollment(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EnrollmentBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().setEnrollment(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> removeEnrollment(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        co_await edgeService().removeEnrollment(request, body.get<"id">().view());
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> renameNode(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<NodeNameBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().renameNode(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> setNodeGroup(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<NodeGroupBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().setNodeGroup(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> network(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<NetworkBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().queueNetwork(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> dtuChannels(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto result = service::middleware::dataJson(co_await edgeService().dtuChannels(request, c.req().validated<EdgeIdParams>().get<"id">().view()));
        c.header("Content-Type", "application/json");
        co_return c.body(std::string_view(result));
    }

    ruvia::task<ruvia::http_response> saveDtuChannel(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto body = c.req().validated_json<DtuChannelBody>();
        co_await edgeService().saveDtuChannel(request, c.req().validated<EdgeIdParams>().get<"id">().view(), body.value(), body.raw());
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> deleteDtuChannel(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<DtuChannelParams>();
        co_await edgeService().deleteDtuChannel(request, body.get<"id">().view(), body.get<"channelId">().view());
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> sync(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        (void)co_await EdgeService::queueSnapshot(request, body.get<"id">().view(), request.userId);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> reuseFirmware(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        const auto& body = c.req().validated<FirmwareReuseBody>();
        FirmwareReuseResult result(ruvia::model_options{ .resource_ = c.arena() });
        result.set<"reused">(co_await edgeService().reuseFirmware(request, id, body));
        co_return c.json(result);
    }

    ruvia::task<ruvia::http_response> uploadFirmware(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        if (c.req().header("Content-Type").value_or("") != "application/octet-stream") {
            service::common::fail(17017, "固件上传需要 application/octet-stream", 415);
        }
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        const auto& configuration = c.req().validated<FirmwareUploadBody>();
        try {
            const auto directory = co_await edgeService().prepareFirmwareUpload(request, id);
            service::edge_node::FirmwareUpload file(request.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next(), directory, static_cast<std::uint64_t>(configuration.get<"sizeBytes">().value_));
            auto& reader = c.req().get_body_reader();
            while (auto chunk = co_await reader.read()) {
                auto remaining = *chunk;
                while (!remaining.empty()) {
                    const auto bytes = remaining.first(std::min(remaining.size(), service::edge_node::FirmwareUpload::kChunkBytes));
                    try {
                        file.appendBytes(std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
                    } catch (const std::invalid_argument&) {
                        service::common::fail(17017, "固件内容超出声明大小", 400);
                    }
                    remaining = remaining.subspan(bytes.size());
                }
            }
            std::string hash;
            try {
                hash = file.finish();
            } catch (const std::invalid_argument&) {
                service::common::fail(17017, "固件上传尚未完成", 400);
            }
            (void)service::middleware::requireAuth(c);
            co_await service::auth::AuthService::requirePermission(request, request.userId, "iot:edge:firmware");
            co_await edgeService().finishFirmwareUpload(request, id, configuration, file.path, hash, static_cast<std::int64_t>(file.received));
        } catch (const ruvia::http_error&) {
            throw;
        } catch (const std::exception& error) {
            // 业务错误已映射为 http_error；其余异常以 500 返回，在请求边界记录原因。
            LOG_ERROR << "edge firmware upload failed: node_id=" << id << " error=" << error.what();
            throw;
        }
        co_return c.json(service::common::operation(c, "固件已上传，刷写任务已下发"));
    }

    ruvia::task<ruvia::http_response> captureLogs(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<EdgeIdParams>();
        const LogsQuery query;
        (void)co_await edgeService().logs(request, body.get<"id">().view(), query);
        co_return c.json(service::common::operation(c, "ok"));
    }

    ruvia::task<ruvia::http_response> logLevel(ruvia::context& c) {

        service::middleware::RequestContext request(c, service::middleware::requireAuth(c).userId);
        const auto& body = c.req().validated<LogLevelBody>();
        const auto id = c.req().validated<EdgeIdParams>().get<"id">().view();
        co_await edgeService().setLogLevel(request, std::string(id), body);
        co_return c.json(service::common::operation(c, "ok"));
    }
};

class EdgePublicController final {
  public:
    RUVIA_CONTROLLER_GROUP("/edge/v1/firmware")
    RUVIA_ROUTES_BEGIN(EdgePublicController)
    RUVIA_GET("/:id/download", download, ruvia::path_model<EdgeIdParams>, ruvia::query_model<FirmwareDownloadQuery>);
    RUVIA_ROUTES_END

  private:
    ruvia::task<ruvia::http_response> download(ruvia::context& c) {

        const auto& id = c.req().validated<EdgeIdParams>();
        const auto& query = c.req().validated<FirmwareDownloadQuery>();
        auto [path, fileName] = co_await edgeService().firmwareDownload(
            c,
            id.get<"id">().view(),
            query.get<"token">().view()
        );
        if (!std::filesystem::is_regular_file(path)) {
            service::common::fail(17009, "固件文件不存在", 404);
        }
        c.header("Content-Disposition", "attachment; filename=firmware.bin");
        co_return c.file(ruvia::file_response_options{
            .path_ = path,
            .content_type_ = "application/octet-stream",
        });
    }
};

} // namespace service::edge_node
