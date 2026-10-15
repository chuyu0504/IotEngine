#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "service/features/edge/gateway/gateway.entity.h"
#include "service/features/edge/gateway/gateway.service.h"
#include "service/modules/edge_node/edge_node.service.h"

namespace {

void requireMissing(std::string_view haystack, std::string_view needle, const char* message) {
    if (haystack.find(needle) != std::string_view::npos) {
        throw std::runtime_error(message);
    }
}

void requireContains(std::string_view haystack, std::string_view needle, const char* message) {
    if (haystack.find(needle) == std::string_view::npos) {
        throw std::runtime_error(message);
    }
}

void requireBefore(std::string_view haystack, std::string_view before, std::string_view after, const char* message) {
    const auto beforePosition = haystack.find(before);
    const auto afterPosition = beforePosition == std::string_view::npos
        ? std::string_view::npos
        : haystack.find(after, beforePosition + before.size());
    if (beforePosition == std::string_view::npos || afterPosition == std::string_view::npos) {
        throw std::runtime_error(message);
    }
}

std::string edgeSource(const char* relativePath) {
    auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
        relativePath;
    std::ifstream input(path, std::ios::binary);
    if (!input.good()) {
        throw std::runtime_error("cannot open edge source");
    }
    std::string source{ std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>() };
    std::erase(source, '\r');
    return source;
}

} // namespace

int main() {
    try {
        const auto approved = service::edge::gateway::EnrollmentRecord::decode("node-1|approved");
        if (!approved || approved->nodeId != "node-1" || approved->status != "approved") {
            throw std::runtime_error("gateway enrollment record lost node or approval status");
        }
        if (service::edge::gateway::EnrollmentRecord::decode("node-1").has_value()) {
            throw std::runtime_error("gateway accepted an incomplete enrollment record");
        }
        const auto pending = service::edge::gateway::EnrollmentRecord::decode("node-1|pending");
        if (!pending || pending->status != "pending") {
            throw std::runtime_error("gateway enrollment record changed pending status");
        }
        service::edge::pb::CommandRequest capableCommand;
        capableCommand.set_timeout_ms(5000);
        constexpr std::int64_t persistedDeadlineMs = 1780000000123;
        service::edge::gateway::GatewayService::applyCommandDeadline(
            capableCommand, true, persistedDeadlineMs);
        if (!capableCommand.has_start_before_ms() ||
            capableCommand.start_before_ms() != persistedDeadlineMs ||
            capableCommand.timeout_ms() != 5000) {
            throw std::runtime_error("negotiated command deadline did not preserve the persisted millisecond value");
        }
        service::edge::pb::CommandRequest legacyCommand;
        legacyCommand.set_timeout_ms(5000);
        service::edge::gateway::GatewayService::applyCommandDeadline(
            legacyCommand, false, persistedDeadlineMs);
        if (legacyCommand.has_start_before_ms() || legacyCommand.timeout_ms() != 5000) {
            throw std::runtime_error("legacy command wire behavior changed");
        }
        bool rejectedUnnegotiatedField = false;
        legacyCommand.set_start_before_ms(persistedDeadlineMs);
        try {
            service::edge::gateway::GatewayService::applyCommandDeadline(
                legacyCommand, false, persistedDeadlineMs);
        } catch (const std::invalid_argument&) {
            rejectedUnnegotiatedField = true;
        }
        if (!rejectedUnnegotiatedField) {
            throw std::runtime_error("legacy session accepted an unnegotiated command deadline field");
        }
        bool rejectedMissingDeadline = false;
        try {
            service::edge::pb::CommandRequest invalidCommand;
            service::edge::gateway::GatewayService::applyCommandDeadline(
                invalidCommand, true, std::nullopt);
        } catch (const std::invalid_argument&) {
            rejectedMissingDeadline = true;
        }
        if (!rejectedMissingDeadline) {
            throw std::runtime_error("negotiated command deadline silently proceeded without its database value");
        }
        const auto serviceSource = edgeSource("service/modules/edge_node/edge_node.service.h");
        requireMissing(serviceSource, "R\"sql", "edge service still embeds raw SQL");
        requireMissing(serviceSource, "c.db().query(\"", "edge service still calls raw query overload");
        requireMissing(serviceSource, "c.db().execute(\"", "edge service still calls raw execute overload");
        requireContains(serviceSource, "ruvia::db_query", "edge service does not use db_query");
        requireContains(serviceSource, "EdgeNodeEntity::table_name()", "edge service does not use the edge node entity");
        requireContains(serviceSource, "EdgeNodeGroupEntity::table_name()", "edge service does not use the edge group entity");
        requireContains(serviceSource, "c.pool()", "edge service does not build queries from Context.pool()");
        requireContains(serviceSource, "safeBigInt", "edge service does not guard numeric JSON fields");
        requireContains(serviceSource, "booleanText", "edge service does not normalize JSON booleans");
        requireContains(serviceSource, "jsonb_array_elements_text", "edge service does not preserve bridge port JSON arrays");
        requireContains(serviceSource, "join_function", "edge service does not use public lateral joins");
        requireContains(serviceSource, "with(\"selected_group\"", "edge service does not express recursive group traversal through db_query");
        requireContains(serviceSource, "roots.cast(roots.value(groupId), Type::uuid)", "group cycle validation does not start at the group being moved");
        requireContains(serviceSource, "cycle.cast(cycle.value(parentId), Type::uuid)", "group cycle validation does not reject moving into a descendant");
        requireContains(serviceSource, "nullableUuid", "edge service does not preserve nullable UUID writes");
        requireContains(serviceSource, "query.call(\n            \"jsonb_build_object\"", "edge task request does not use typed JSON construction");
        const auto controllerSource = edgeSource("service/modules/edge_node/edge_node.controller.h");
        const auto gatewaySource = edgeSource("service/features/edge/gateway/gateway.runtime.h");
        const auto dispatchSource = edgeSource("service/features/edge/session/session.service.h");
        requireContains(gatewaySource, ".ping_interval_ = std::chrono::seconds(300)", "node WebSocket heartbeat is not five minutes");
        requireContains(gatewaySource, "if (live->session->sparseHeartbeat) {\n                    continue;\n                }", "node-driven heartbeats still trigger application probes");
        requireContains(gatewaySource, "? std::chrono::seconds(900) : std::chrono::seconds(60)", "application watchdog cannot span three heartbeats");
        requireContains(dispatchSource, "'EX',930", "routing lease expires before the application watchdog");
        requireContains(dispatchSource, "+930000", "session deadline index disagrees with the lease");
        requireContains(gatewaySource, "!session.sparseHeartbeat && freshTelemetry && std::any_of", "node-driven or duplicate telemetry requests redundant status");
        requireContains(gatewaySource, "!record.has_device_status() && !record.derived_update()", "derived updates request redundant device status");
        requireContains(gatewaySource, "input.hello().supports_sparse_heartbeat()", "five-minute heartbeat requires explicit firmware capability");
        requireMissing(gatewaySource, "pendingTelemetryAck", "legacy single-ID nanopb ACK limit must be preserved");
        const auto dispatcherSource = edgeSource("service/features/edge/edge.runtime.h");
        const auto multiplexerSource =
            edgeSource("service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h");
        const auto projectorRuntimeSource =
            edgeSource("service/features/edge/edge.runtime.h");
        const auto projectorServiceSource =
            edgeSource("service/features/edge/edge.service.h");
        const auto projectorStreamSource =
            edgeSource("service/common/message.h");
        const auto metadataSource = edgeSource("service/features/edge/edge.service.h");
        const auto vpnEdgeConfigSource =
            edgeSource("service/features/vpn/vpn.service.h");
        const auto vpnServiceSource =
            edgeSource("service/modules/vpn/vpn.service.h");
        requireContains(serviceSource, "operation != \"upsert\" && operation != \"delete\"", "edge network config accepts unknown operations as upsert");
        requireMissing(controllerSource, "/:id/modem", "edge controller still exposes modem mutation route");
        requireMissing(serviceSource, "queueModem(", "edge service still exposes modem mutation commands");
        requireMissing(serviceSource, "queuePlatform(", "edge service still exposes remote platform configuration");
        requireMissing(serviceSource, "deletePlatform(", "edge service still exposes remote platform deletion");
        requireMissing(controllerSource, "/:id/platforms", "edge controller still exposes platform management routes");
        requireContains(controllerSource, "/:id/download", "legacy firmware download route is missing");
        requireContains(serviceSource, "download_token", "legacy firmware download tokens are missing");
        requireContains(serviceSource, "EDGE_PUBLIC_BASE_URL", "legacy firmware download URL is missing");
        requireContains(serviceSource, "baseUrl.ends_with('/')", "legacy firmware URL does not normalize a trailing slash");
        requireContains(serviceSource, "/download?token=", "legacy firmware download URL does not include its token");
        requireContains(serviceSource, "firmwareStream", "firmware transport does not select by node capability");
        requireContains(gatewaySource, "case pb::Envelope::kFirmwareChunkRequest", "edge gateway does not serve firmware over the node WebSocket");
        requireContains(gatewaySource, "firmware::readChunk(", "edge gateway does not bound firmware WS chunks");
        requireMissing(serviceSource, "std::clamp<std::int64_t>(*query.limit(), 1, 48)", "edge log request clamps invalid limit instead of rejecting it");
        requireContains(serviceSource, "status != \"approved\"", "edge enrollment accepts invalid registration status");
        requireContains(controllerSource, "RUVIA_DELETE(\"/:id\", removeEnrollment, service::middleware::PermissionMiddleware<\"iot:edge:edit\">, ruvia::path_model<EdgeIdParams>)", "edge enrollment HTTP deletion route is missing");
        requireContains(serviceSource, "removal.value(\"pending\")", "edge enrollment deletion is not limited to pending registrations");
        requireContains(serviceSource, "module_wire::authKey(imei)", "edge enrollment deletion leaves stale authorization state");
        requireContains(serviceSource, "\"iot:edge:session:\" + std::string(id)", "edge enrollment deletion does not disconnect the pending session");
        requireContains(serviceSource, "level != \"debug\" && level != \"info\" && level != \"warn\"", "edge log level accepts invalid values");
        requireContains(serviceSource, "sourceValue.size() > 16", "edge log source does not enforce local length limit");
        requireMissing(gatewaySource, "if (input.ping().nonce() != 0)", "edge gateway drops zero-nonce terminal liveness pings");
        requireContains(gatewaySource, "reply.mutable_pong()->set_nonce(input.ping().nonce())", "edge gateway does not echo terminal liveness pings");
        const auto terminalRpcSource = edgeSource("service/features/edge/terminal/terminal.service.h");
        requireContains(terminalRpcSource, "if (state->protocolVersion <= 3)", "terminal RPC does not preserve legacy immediate-ready behavior");
        requireContains(terminalRpcSource, "message->set_ticket(id)", "terminal RPC does not satisfy legacy terminal-open validation");
        requireContains(gatewaySource, "case pb::Envelope::kTerminalOpened", "edge gateway does not consume terminal-open acknowledgement");
        requireContains(terminalRpcSource, "TerminalBrowserRecord::readyKey(id)", "terminal RPC does not track terminal readiness");
        requireContains(serviceSource, "notification->receiver.receive_for(remaining, c.get_stop_token())", "terminal input does not wait for device acknowledgement notifications");
        requireMissing(gatewaySource, "waitTerminalInputAck", "gateway retains the terminal acknowledgement polling loop");
        requireMissing(gatewaySource, "pumpTerminal", "gateway retains the terminal output polling loop");
        requireMissing(gatewaySource, "GatewayTicketValidator", "gateway retains the separate browser terminal connection");
        requireContains(terminalRpcSource, "mutable_terminal_data_ack", "terminal RPC does not acknowledge delivered terminal output");
        requireContains(gatewaySource, "std::deque<std::string> outbound", "edge gateway does not serialize established-session replies");
        requireContains(gatewaySource, "enqueue(session, reply)", "edge gateway still writes node acknowledgements outside the flush path");
        requireMissing(gatewaySource, "co_await drain(c, socket, session)", "edge gateway still sends config frames from the session read loop");
        requireContains(gatewaySource, "co_await c.redis().lpush(key, item)", "edge gateway drops a popped command when its socket send fails");
        requireMissing(gatewaySource, ".blpop(", "edge gateway still allocates one blocking Redis connection per node");
        requireMissing(gatewaySource, "redis_block_wait::indefinitely()", "edge gateway still owns a per-session blocking Redis read");
        requireContains(gatewaySource, "c.worker_state<SessionDispatcher>()", "edge gateway does not resolve its worker-local dispatcher");
        requireContains(gatewaySource, "dispatcher.registerSession(", "edge gateway does not register sessions with its worker dispatcher");
        requireContains(gatewaySource, "requestFlush(live)", "edge gateway does not drain queued work on session establishment");
        requireMissing(dispatcherSource, "readGroupBlocking(", "edge dispatcher still owns a dedicated blocking Redis connection");
        requireContains(multiplexerSource, "readGroupManyBlockingUntil(", "Service Worker wake bus has no blocking Stream consumer");
        requireContains(dispatcherSource, "context.worker_state<SessionDispatcher>().run(", "edge dispatcher does not start the same local state on every worker");
        requireContains(edgeSource("service/common/message.h"), "iot:v2:edge:dispatch:", "edge dispatch notifications do not use worker-isolated Redis keys");
        requireContains(dispatchSource, "session_state::parse(", "edge dispatch notifications are not routed by session ownership");
        requireContains(projectorStreamSource, "iot:v3:edge:projector:", "edge projection does not use worker-isolated Redis keys");
        requireContains(gatewaySource, "publishIngress(c, workerIndex", "edge ingress is not routed by the accepting Worker");
        requireContains(gatewaySource, "projector_stream::publishMetadata(", "edge reconnect does not refresh accepting-Worker metadata");
        requireContains(metadataSource, "session_state::parse(", "edge metadata updates are not routed by session ownership");
        requireContains(projectorRuntimeSource, "projector_stream::stream(index, service::runtime::instanceId())", "edge projector does not preserve the accepting Worker");
        requireContains(projectorServiceSource, "jsonKey(\"firmwareUpdate\"), boolean(hello.supports_firmware_update())", "edge projector does not retain legacy firmware capability");
        requireContains(projectorServiceSource, "jsonKey(\"firmwareStream\"), boolean(hello.supports_firmware_stream())", "edge projector does not record WS firmware capability separately");
        requireBefore(projectorServiceSource, "context.db().execute(derivedCapability)", "configService().requeueIfStale(context, nodeId)", "edge configuration recovery does not follow fresh capability persistence");
        requireContains(projectorServiceSource, "desiredRevision <= activeRevision", "edge reconnect can replay an older configuration over a newer node revision");
        requireContains(projectorServiceSource, "protocol::persistableCommandResult(result.state(), result.write_ack_missing())", "edge command result drops missing-write-ACK outcomes before persistence");
        requireContains(projectorServiceSource, "protocol::commandResultDomainState(", "edge command results ignore write acknowledgement ambiguity");
        requireContains(projectorServiceSource, "result.write_ack_missing()", "edge command projection does not persist the missing-write-ACK result");
        requireContains(projectorServiceSource, "reason = \"write_ack_missing\"", "edge API reason omits the missing-write-ACK context");
        requireContains(projectorServiceSource, "query.null_if(config::detail::jsonText(query, excludedMobile, \"apn\")", "edge hello projection clears the last known mobile APN");
        requireContains(projectorServiceSource, "query.null_if(config::detail::jsonText(query, excludedMobile, \"operator\")", "edge hello projection clears the last known mobile operator");
        requireContains(projectorServiceSource, "query.null_if(text(heartbeat.apn()), query.value(std::string_view{}))", "edge heartbeat projection clears the last known mobile APN");
        requireContains(projectorServiceSource, "query.null_if(text(heartbeat.mobile_operator()), query.value(std::string_view{}))", "edge heartbeat projection clears the last known mobile operator");
        requireContains(vpnEdgeConfigSource, "task.cast(task.value(peerId), ruvia::db_data_type::text)", "VPN task peer id has no explicit PostgreSQL type");
        requireContains(vpnEdgeConfigSource, "task.cast(task.value(request->enabled()), ruvia::db_data_type::boolean)", "VPN task enabled flag has no explicit PostgreSQL type");
        requireContains(gatewaySource, "case pb::Envelope::kVpnConfigResult:", "edge gateway does not project VPN configuration results");
        requireContains(projectorServiceSource, "transitioned, transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<\"request\">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), \"enabled\")", "VPN result transition does not return the requested enabled state");
        requireContains(projectorServiceSource, "\"config_version\")", "VPN result transition does not return the requested config version");
        requireContains(projectorServiceSource, "update.cast(update.column(\"enabled\", \"task\"), ruvia::db_data_type::boolean)", "VPN result projection reads a column outside the transition CTE");
        requireContains(projectorServiceSource, "update.column(\"config_version\", \"task\")", "VPN result projection does not compare the returned config version");
        requireContains(vpnEdgeConfigSource, "superseded.cast(superseded.value(\"superseded\"), ruvia::db_data_type::text)", "new VPN tasks leave older tasks pending forever");
        requireContains(vpnServiceSource, "reusableEdgePeerId.empty() ? c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next()", "revoked Edge VPN peers are not reused when VPN is enabled again");
        requireContains(vpnServiceSource, ".set(std::tuple_element_t<service::vpn::entities::VpnPeerEntity::column_index<\"revoked_at\">(), service::vpn::entities::VpnPeerEntity::columns_type>::name.view(), reactivation.null_value())", "reactivating an Edge VPN peer does not clear its revoked state");
        requireContains(vpnServiceSource, "\"vpn.peer.reactivate\"", "Edge VPN peer reactivation is not audited separately");
        requireMissing(dispatcherSource, "workers_.back().post(", "edge dispatcher still gives one worker a special role");
        requireMissing(dispatcherSource, "target.worker.post(", "edge dispatcher still forwards wakeups between workers");
        requireContains(gatewaySource, ".reason_ = \"edge egress failed\"", "edge gateway leaves a half-open session online after its flush fails");
        const auto terminalSource = edgeSource("service/features/edge/terminal/terminal.service.h");
        requireContains(terminalSource, "session.protocolVersion < 5", "edge gateway does not isolate legacy terminal data handling");
        requireBefore(terminalSource, "redis.call('SET',KEYS[2],ARGV[1],'EX',120)", "co_await enqueueBrowserInput(c, binding, id, open)", "terminal RPC does not register ownership before opening");
        requireContains(edgeSource("service/common/message.h"), "return \"iot:edge:terminal:out:\" + std::string(nodeId)", "edge terminal output keys are not isolated by node");
        requireContains(terminalSource, "if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end", "edge gateway does not atomically verify terminal ownership");
        requireContains(gatewaySource, "co_await terminal_state::TerminalService::saveTerminalData(c, terminalIdentity(session), input.terminal_data())", "edge gateway does not bind terminal output to the authenticated session");
        requireContains(terminalSource, "redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])", "edge gateway does not atomically release terminal state");
        requireMissing(gatewaySource, "\"iot:edge:terminal:out:\" + terminalId", "edge gateway still routes terminal output by unscoped identifier");

        std::cout << "edge service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "edge service test failed: " << error.what() << '\n';
        return 1;
    }
}
