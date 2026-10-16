#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <google/protobuf/util/json_util.h>
#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/model_json.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/wireguard.h"
#include "service/features/edge/acquisition/acquisition.protocol.h"
#include "service/features/edge/edge.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/edge.service.h"
#include "service/features/edge/gateway/gateway.entity.h"
#include "service/features/edge/projection/projection.entity.h"
#include "service/features/edge/projection/projection.types.h"
#include "service/features/telemetry/latest/latest.service.h"
#include "service/middleware/log.h"
#include "service/utils/json.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"

namespace service::edge::projection {

// Persists EdgeNode protocol messages taken from a Worker's projector Stream.
// Stateless; the Worker-local catalog and caches live in ProjectionState.
class ProjectionService final {
  public:
    static ruvia::task<void> hydrateAuth(ruvia::web_worker_context& context) {
        ruvia::db_query query;
        query.select({query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"imei">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                      query.cast(query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_data_type::text),
                      query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"enrollment_status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view())})
            .from(service::edge::persistence::EdgeNodeEntity::table_name());
        const auto rows = co_await context.db().query(query);
        if (rows.empty())
            co_return;
        auto pipeline = context.redis().pipeline();
        for (const auto& row : rows) {
            const auto key = protocol::authKey(row[0].value().value_or(std::string_view{}));
            const auto value = std::string(row[1].value().value_or(std::string_view{})) + "|" + std::string(row[2].value().value_or(std::string_view{}));
            pipeline.set(key, value);
        }
        const auto replies = co_await std::move(pipeline).exec();
        service::message::redis::requirePipelineSuccess("hydrate edge authorization", replies);
    }

    // Projects one ingress entry. Telemetry, completed uploads and raw-acquisition
    // calculations are collected in `batch` and applied once per Stream batch.
    static ruvia::task<void> project(ruvia::web_worker_context& context, std::size_t workerIndex,
                                     ProjectionState& state, std::string_view wire,
                                     std::string_view receivedAtText, ProjectionBatch& batch) {
        auto& catalog = state.catalog;
        pb::Envelope envelope;
        if (!protocol::decode(wire, envelope))
            co_return;
        const auto receivedAt = service::utils::parseInt64(
            receivedAtText.empty() ? std::nullopt
                                   : std::optional<std::string_view>(receivedAtText));
        const auto receivedAtMs = receivedAt.value_or(service::message::utcNowMilliseconds());
        if (envelope.payload_case() == pb::Envelope::kHello) {
            co_await saveHello(context, workerIndex, envelope.hello());
            co_return;
        }
        if (envelope.node_id().size() != 16)
            co_return;
        const auto nodeId = protocol::uuidText(envelope.node_id());
        if (envelope.has_telemetry_batch() || envelope.has_command_result()) {
            // Another Worker may have refreshed this node's Redis snapshot.
            if (auto snapshot = co_await metadata::loadNode(context.redis(), nodeId))
                catalog[nodeId] = std::move(*snapshot);
            else
                catalog.erase(nodeId);
        }
        switch (envelope.payload_case()) {
        case pb::Envelope::kHeartbeat:
            co_await saveHeartbeat(context, nodeId, envelope.heartbeat());
            break;
        case pb::Envelope::kDtuStatus: {
            const auto& status = envelope.dtu_status();
            if (status.channel_id().size() != 16) break;
            std::string json;
            if (!google::protobuf::util::MessageToJsonString(status, &json).ok()) break;
            auto withoutDebug = status;
            withoutDebug.clear_traces();
            withoutDebug.clear_omitted_traces();
            std::string statusOnly;
            if (!google::protobuf::util::MessageToJsonString(withoutDebug, &statusOnly).ok()) break;
            ruvia::db_query update;
            const auto projected = update.case_when({{update.binary(
                config::detail::jsonText(update, update.column(std::tuple_element_t<persistence::EdgeDtuEntity::column_index<"config">(), persistence::EdgeDtuEntity::columns_type>::name.view()), "debugEnabled"),
                ruvia::db_binary_operator::equal, update.value("true")), update.cast(update.value(json), ruvia::db_data_type::jsonb)}},
                update.cast(update.value(statusOnly), ruvia::db_data_type::jsonb));
            update.update(persistence::EdgeDtuEntity::table_name())
                .set(std::tuple_element_t<persistence::EdgeDtuEntity::column_index<"status">(), persistence::EdgeDtuEntity::columns_type>::name.view(), projected)
                .where(update.binary(update.column(std::tuple_element_t<persistence::EdgeDtuEntity::column_index<"node_id">(), persistence::EdgeDtuEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                    update.cast(update.value(nodeId), ruvia::db_data_type::uuid)))
                .and_where(update.binary(update.column(std::tuple_element_t<persistence::EdgeDtuEntity::column_index<"channel_id">(), persistence::EdgeDtuEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                    update.cast(update.value(protocol::uuidText(status.channel_id())), ruvia::db_data_type::uuid)))
                .and_where(update.binary(update.column(std::tuple_element_t<persistence::EdgeDtuEntity::column_index<"status">(), persistence::EdgeDtuEntity::columns_type>::name.view()), ruvia::db_binary_operator::not_equal,
                    projected));
            (void)co_await context.db().execute(update);
            break;
        }
        case pb::Envelope::kCapabilityReport:
            co_await saveCapabilities(context, workerIndex, nodeId, envelope.capability_report());
            break;
        case pb::Envelope::kNetworkConfigResult:
            co_await saveNetworkResult(context, nodeId, envelope.network_config_result());
            break;
        case pb::Envelope::kVpnConfigResult:
            co_await saveVpnResult(context, workerIndex, nodeId, envelope.vpn_config_result());
            break;
        case pb::Envelope::kFirmwareUpdateResult:
            co_await saveFirmwareResult(context, workerIndex, nodeId, envelope.firmware_update_result());
            break;
        case pb::Envelope::kModemControlResult:
            co_await saveModemResult(context, nodeId, envelope.modem_control_result());
            break;
        case pb::Envelope::kPlatformConfigResult:
            co_await savePlatformResult(context, nodeId, envelope.platform_config_result());
            break;
        case pb::Envelope::kConfigApplied:
            co_await saveConfigApplied(context, nodeId, envelope.config_applied());
            break;
        case pb::Envelope::kConfigRejected:
            co_await saveConfigRejected(context, nodeId, envelope.config_rejected());
            break;
        case pb::Envelope::kTelemetryBatch: {
            for (const auto& record : envelope.telemetry_batch().records()) {
                if (record.has_device_status() &&
                    record.device_status().device_id() == record.device_id()) {
                    pb::DeviceStatusReport report;
                    *report.add_devices() = record.device_status();
                    co_await saveDeviceStatus(context, nodeId, report);
                }
            }
            std::vector<std::string> deviceIds;
            for (const auto& record : envelope.telemetry_batch().records())
                if (record.device_id().size() == 16)
                    deviceIds.push_back(protocol::uuidText(record.device_id()));
            co_await ensureMetadata(context, catalog, nodeId, deviceIds);
            pb::TelemetryBatch assembled;
            for (const auto& record : envelope.telemetry_batch().records()) {
                if (record.part_count() == 0 && record.part_index() == 0 && record.report_id().empty()) {
                    *assembled.add_records() = record;
                    continue;
                }
                const auto complete = co_await storeTelemetryPart(context.redis(), nodeId, record);
                if (complete) {
                    *assembled.add_records() = *complete;
                    batch.completedUploads.push_back({nodeId, protocol::uuidText(record.device_id()),
                                                      protocol::uuidText(record.report_id())});
                }
            }
            if (std::any_of(assembled.records().begin(), assembled.records().end(),
                           [](const auto& record) { return record.raw_requests_size() != 0; }) &&
                !state.acquisitionConfigurations.contains(nodeId)) {
                AcquisitionConfiguration configuration;
                configuration.items = co_await ConfigService::loadItems(context, nodeId, &configuration.calculations);
                state.acquisitionConfigurations.emplace(nodeId, std::move(configuration));
            }
            const auto configuration = state.acquisitionConfigurations.find(nodeId);
            const std::vector<pb::ConfigItem> emptyItems;
            collectTelemetry(catalog, nodeId, receivedAtMs, assembled, batch.telemetry,
                configuration == state.acquisitionConfigurations.end() ? emptyItems : configuration->second.items);
            for (const auto& record : assembled.records())
                if (record.raw_requests_size() && configuration != state.acquisitionConfigurations.end()) {
                    const auto calculation = configuration->second.calculations.find(protocol::uuidText(record.device_id()));
                    if (calculation != configuration->second.calculations.end())
                        batch.rawCalculations[protocol::uuidText(record.record_id())] = calculation->second;
                }
            break;
        }
        case pb::Envelope::kCommandResult:
            if (envelope.command_result().device_id().size() == 16) {
                const std::string deviceIds[]{protocol::uuidText(envelope.command_result().device_id())};
                co_await ensureMetadata(context, catalog, nodeId, deviceIds);
            }
            co_await saveCommandResult(context, catalog, nodeId, receivedAtMs,
                                       envelope.command_result());
            break;
        case pb::Envelope::kDeviceStatusReport:
            co_await saveDeviceStatus(context, nodeId, envelope.device_status_report());
            break;
        default:
            break;
        }
    }

    // Applies a metadata change event: the publisher has already stored the
    // node's Redis snapshot, so the database is read only when that hash is absent.
    static ruvia::task<void> refreshMetadata(ruvia::web_worker_context& context, ProjectionState& state,
                                             std::string_view nodeId) {
        state.acquisitionConfigurations.erase(std::string(nodeId));
        auto snapshot = co_await metadata::loadNode(context.redis(), nodeId);
        if (!snapshot) {
            snapshot = co_await metadata::loadNodeFromDatabase(context, nodeId);
            co_await metadata::storeNode(context.redis(), nodeId, *snapshot);
        }
        state.catalog[std::string(nodeId)] = std::move(*snapshot);
    }

  private:
    // Makes `catalog` cover `deviceIds` of the node: in-memory first, then the
    // shared Redis snapshot, then the database (which refreshes Redis).
    static ruvia::task<void> ensureMetadata(ruvia::web_worker_context& context,
                                            metadata::Catalog& catalog, std::string_view nodeId,
                                            std::span<const std::string> deviceIds) {
        const auto containsAll = [deviceIds](const metadata::NodeSnapshot& snapshot) {
            return std::all_of(deviceIds.begin(), deviceIds.end(),
                               [&snapshot](const std::string& id) { return snapshot.contains(id); });
        };
        const auto existingNode = catalog.find(std::string(nodeId));
        if (existingNode != catalog.end() && containsAll(existingNode->second))
            co_return;
        auto snapshot = co_await metadata::loadNode(context.redis(), nodeId);
        if (!snapshot || !containsAll(*snapshot)) {
            snapshot = co_await metadata::loadNodeFromDatabase(context, nodeId);
            co_await metadata::storeNode(context.redis(), nodeId, *snapshot);
        }
        catalog[std::string(nodeId)] = std::move(*snapshot);
    }

    static std::string_view simState(pb::ModemSimState state) {
        switch (state) {
        case pb::MODEM_SIM_READY:
            return "ready";
        case pb::MODEM_SIM_NOT_INSERTED:
            return "not_inserted";
        case pb::MODEM_SIM_PIN_REQUIRED:
            return "pin_required";
        case pb::MODEM_SIM_PUK_REQUIRED:
            return "puk_required";
        case pb::MODEM_SIM_BLOCKED:
            return "blocked";
        default:
            return "unknown";
        }
    }

    static ruvia::task<void> saveHello(ruvia::web_worker_context& context, std::size_t workerIndex,
                                       const pb::Hello& hello) {
        using Node = persistence::EdgeNodeEntity;
        using config::detail::columnName;
        if (!protocol::validImei(hello.imei()))
            co_return;
        const auto candidate = context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        ruvia::db_query query;
        const auto uuid = [&query](std::string_view value) {
            return query.cast(query.value(value), ruvia::db_data_type::uuid);
        };
        const auto text = [&query](const auto& value) {
            return query.cast(query.value(std::string_view(value)),
                              ruvia::db_data_type::text);
        };
        const auto boolean = [&query](bool value) {
            return query.cast(query.value(value), ruvia::db_data_type::boolean);
        };
        const auto integer = [&query](auto value) {
            return query.cast(query.value(static_cast<std::int64_t>(value)),
                              ruvia::db_data_type::big_int);
        };
        const auto jsonKey = [&query](std::string_view value) {
            return config::detail::jsonKey(query, value);
        };
        const auto vpn = query.call(
            "jsonb_build_object",
            {jsonKey("supportsVpn"), boolean(false),
             jsonKey("wireguardVersion"), text(std::string_view{}),
             jsonKey("agentVersion"), text(std::string_view{}),
             jsonKey("publicKey"), text(std::string_view{})});
        const auto capability = query.call(
            "jsonb_build_object",
            {jsonKey("networkConfig"), boolean(hello.supports_network_config()),
             jsonKey("firmwareUpdate"), boolean(hello.supports_firmware_update()),
             jsonKey("firmwareStream"), boolean(hello.supports_firmware_stream()),
             jsonKey("platformConfig"), boolean(hello.supports_platform_config()),
             jsonKey("deviceConfig"), boolean(hello.supports_device_config()),
             jsonKey("networkConfigVersion"), integer(hello.network_config_version()),
             jsonKey("modemControl"), boolean(hello.supports_modem_control()),
             jsonKey("commandStartBefore"), boolean(hello.supports_command_start_before()),
             jsonKey("logs"), boolean(hello.supports_logs()),
             jsonKey("terminal"), boolean(false), jsonKey("serialDebug"), boolean(false),
             jsonKey("vpn"), vpn});
        const auto signal = query.call(
            "jsonb_build_object", {jsonKey("csq"), integer(hello.signal_csq()),
                                    jsonKey("rssiDbm"), integer(hello.signal_rssi_dbm()),
                                    jsonKey("percent"), integer(hello.signal_percent())});
        const auto mobile = query.call(
            "jsonb_build_object",
            {jsonKey("available"), boolean(hello.modem_available()),
             jsonKey("simState"), text(simState(hello.sim_state())),
             jsonKey("iccid"), text(hello.iccid()), jsonKey("signal"), signal,
             jsonKey("registered"), boolean(hello.mobile_registered()),
             jsonKey("registrationStatus"), integer(hello.mobile_registration_status()),
             jsonKey("apn"), text(hello.apn()), jsonKey("operator"), text(hello.mobile_operator()),
             jsonKey("connected"), boolean(hello.mobile_connected()),
             jsonKey("ipv4"), text(hello.mobile_ipv4())});
        const auto config = query.call(
            "jsonb_build_object", {jsonKey("activeVersion"), integer(hello.last_applied_config_version()),
                                    jsonKey("desiredVersion"), integer(0),
                                    jsonKey("state"), text("idle"),
                                    jsonKey("message"), text(std::string_view{})});
        const auto outbox = query.call(
            "jsonb_build_object", {jsonKey("records"), integer(0),
                                    jsonKey("bytes"), integer(0)});
        const auto log = query.call(
            "jsonb_build_object", {jsonKey("level"),
                                    config::detail::textDefault(
                                        query, text(hello.log_level()), "info")});
        const auto status = query.call(
            "jsonb_build_object", {jsonKey("config"), config,
                                    jsonKey("outbox"), outbox, jsonKey("log"), log});
        query.insert_into(Node::table_name(),
                         {columnName<Node, "id">(), columnName<Node, "platform_id">(), columnName<Node, "imei">(),
                          columnName<Node, "model">(), columnName<Node, "software_version">(),
                          columnName<Node, "hostname">(), columnName<Node, "architecture">(),
                          columnName<Node, "openwrt_release">(), columnName<Node, "capability">(),
                          columnName<Node, "mobile">(), columnName<Node, "status">(),
                          columnName<Node, "last_seen_at">(), columnName<Node, "updated_at">()})
            .values({uuid(std::string_view(candidate)), uuid(context.template worker_state<service::edge::config::PlatformIdentity>().id),
                     query.value(std::string_view(hello.imei())),
                     query.value(std::string_view(hello.model())),
                     query.value(std::string_view(hello.software_version())),
                     query.value(std::string_view(hello.hostname())),
                     query.value(std::string_view(hello.architecture())),
                     query.value(std::string_view(hello.openwrt_release())), capability, mobile,
                     status,
                     query.call("now"), query.call("now")});

        const auto existingCapability = query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), "edge_node");
        const auto existingMobile = query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"mobile">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), "edge_node");
        const auto existingStatus = query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), "edge_node");
        const auto excludedMobile = query.excluded(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"mobile">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view());
        const auto apn = query.coalesce({
            query.null_if(config::detail::jsonText(query, excludedMobile, "apn"),
                         query.value(std::string_view{})),
            config::detail::jsonText(query, existingMobile, "apn"),
            query.value(std::string_view{})});
        const auto operatorName = query.coalesce({
            query.null_if(config::detail::jsonText(query, excludedMobile, "operator"),
                         query.value(std::string_view{})),
            config::detail::jsonText(query, existingMobile, "operator"),
            query.value(std::string_view{})});
        const auto mobileWithApn = query.call(
            "jsonb_set", {excludedMobile, config::detail::jsonPath(query, "{apn}"),
                           config::detail::toJsonb(query,
                               query.cast(apn, ruvia::db_data_type::text)),
                           query.cast(query.value(true), ruvia::db_data_type::boolean)});
        const auto mobileUpdate = query.call(
            "jsonb_set", {mobileWithApn, config::detail::jsonPath(query, "{operator}"),
                           config::detail::toJsonb(query,
                               query.cast(operatorName, ruvia::db_data_type::text)),
                           query.cast(query.value(true), ruvia::db_data_type::boolean)});
        const auto reportedCapabilities = query.binary(
            query.excluded(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::json_concat,
            query.call("jsonb_build_object",
                       {jsonKey("terminal"),
                        config::detail::booleanText(
                            query, config::detail::jsonText(query, existingCapability, "terminal")),
                        jsonKey("serialDebug"),
                        config::detail::booleanText(
                            query, config::detail::jsonText(query, existingCapability, "serialDebug")),
                        jsonKey("vpn"),
                        query.coalesce({config::detail::jsonGet(query, existingCapability, "vpn"),
                                        config::detail::jsonGet(query, query.excluded(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                                                                 "vpn")})}));
        const auto capabilityUpdate = query.binary(existingCapability,
            ruvia::db_binary_operator::json_concat, reportedCapabilities);
        const auto statusWithLog = query.call(
            "jsonb_set", {existingStatus, config::detail::jsonPath(query, "{log}"),
                           query.coalesce({config::detail::jsonGet(query, existingStatus, "log"),
                                           query.cast(query.value(std::string_view{"{}"}),
                                                      ruvia::db_data_type::jsonb)}),
                           query.cast(query.value(true), ruvia::db_data_type::boolean)});
        const auto statusWithLevel = query.call(
            "jsonb_set", {statusWithLog, config::detail::jsonPath(query, "{log,level}"),
                           config::detail::toJsonb(
                               query, config::detail::textDefault(
                                         query, text(hello.log_level()), "info")),
                           query.cast(query.value(true), ruvia::db_data_type::boolean)});
        const auto statusUpdate = query.call("jsonb_set", {
            statusWithLevel, config::detail::jsonPath(query, "{config,activeVersion}"),
            config::detail::toJsonb(query, integer(hello.last_applied_config_version())),
            boolean(true)});
        ruvia::db_conflict_options conflict;
        conflict.columns_ = {std::string(columnName<Node, "platform_id">()), std::string(columnName<Node, "imei">())};
        const auto excluded = [&query](std::string_view name) {
            return ruvia::db_assignment{std::string(name), query.excluded(name)};
        };
        conflict.update_ = {
            excluded(columnName<Node, "model">()),
            excluded(columnName<Node, "software_version">()),
            excluded(columnName<Node, "hostname">()),
            excluded(columnName<Node, "architecture">()),
            excluded(columnName<Node, "openwrt_release">()),
            {std::string(columnName<Node, "capability">()), capabilityUpdate},
            {std::string(columnName<Node, "mobile">()), mobileUpdate},
            {std::string(columnName<Node, "status">()), statusUpdate},
            {std::string(columnName<Node, "last_seen_at">()), query.call("now")},
            {std::string(columnName<Node, "updated_at">()), query.call("now")},
        };
        query.on_conflict(conflict)
            .returning({query.cast(query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_data_type::text),
                        query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"enrollment_status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view())});
        const auto rows = co_await context.db().query(query);
        const auto key = protocol::authKey(hello.imei());
        const auto nodeId = std::string(rows.front()[0].value().value_or(std::string_view{}));
        const auto enrollmentStatus = std::string(rows.front()[1].value().value_or(std::string_view{}));
        const auto value = nodeId + "|" + enrollmentStatus;
        co_await context.redis().set(key, value);
        if (enrollmentStatus == "approved") {
            // Configuration restoration follows the capability report so a
            // changed firmware is validated against its current protocol set.
            ruvia::db_query target;
            target
                .select({target.alias(target.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), "task_id"),
                         target.alias(target.column(std::tuple_element_t<service::edge::persistence::EdgeFirmwareEntity::column_index<"id">(), service::edge::persistence::EdgeFirmwareEntity::columns_type>::name.view(), "firmware"), "firmware_id")})
                .from(service::edge::persistence::EdgeTaskEntity::table_name(), "task")
                .join(ruvia::db_join_type::inner, service::edge::persistence::EdgeFirmwareEntity::table_name(),
                      target.binary(
                          target.cast(target.column(std::tuple_element_t<service::edge::persistence::EdgeFirmwareEntity::column_index<"id">(), service::edge::persistence::EdgeFirmwareEntity::columns_type>::name.view(), "firmware"),
                                      ruvia::db_data_type::text),
                          ruvia::db_binary_operator::equal,
                          config::detail::jsonText(
                              target, target.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"request">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), "firmware_id")),
                      "firmware")
                .where(target.binary(
                    target.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"node_id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), ruvia::db_binary_operator::equal,
                    target.cast(target.value(nodeId), ruvia::db_data_type::uuid)))
                .and_where(target.binary(target.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"task_type">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"),
                                        ruvia::db_binary_operator::equal,
                                        target.value(std::string_view{"firmware"})))
                .and_where(target.binary(target.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"),
                                        ruvia::db_binary_operator::equal,
                                        target.value(std::string_view{"running"})))
                .and_where(target.binary(
                    config::detail::jsonText(target, target.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"result">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), "state"),
                    ruvia::db_binary_operator::equal,
                    target.value(std::string_view{"flashing"})))
                .order_by(target.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"created_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), ruvia::db_order_direction::desc)
                .limit(1);

            ruvia::db_query completed;
            const auto rebootedResult = completed.binary(
                completed.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"result">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), ruvia::db_binary_operator::json_concat,
                completed.call(
                    "jsonb_build_object",
                    {config::detail::jsonKey(completed, "state"),
                     completed.cast(completed.value(std::string_view{"rebooted"}),
                                    ruvia::db_data_type::text),
                     config::detail::jsonKey(completed, "message"),
                     completed.cast(completed.value(std::string_view{"firmware reboot confirmed"}),
                                    ruvia::db_data_type::text),
                     config::detail::jsonKey(completed, "softwareVersion"),
                     completed.cast(completed.value(std::string_view(hello.software_version())),
                                    ruvia::db_data_type::text)}));
            completed.update(service::edge::persistence::EdgeTaskEntity::table_name(), "task")
                .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), completed.value(std::string_view{"succeeded"}))
                .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"result">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), rebootedResult)
                .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"updated_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), completed.call("now"))
                .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"completed_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), completed.call("now"))
                .update_from("target")
                .where(completed.binary(completed.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"),
                                        ruvia::db_binary_operator::equal,
                                        completed.column("task_id", "target")))
                .returning({completed.column("firmware_id", "target")});

            ruvia::db_query recovery;
            recovery.with("target", target)
                .with("completed", completed)
                .update(service::edge::persistence::EdgeFirmwareEntity::table_name(), "firmware")
                .set(std::tuple_element_t<service::edge::persistence::EdgeFirmwareEntity::column_index<"version">(), service::edge::persistence::EdgeFirmwareEntity::columns_type>::name.view(), recovery.cast(
                                                recovery.value(std::string_view(
                                                    hello.software_version())),
                                                ruvia::db_data_type::text))
                .update_from("completed")
                .where(recovery.binary(recovery.column(std::tuple_element_t<service::edge::persistence::EdgeFirmwareEntity::column_index<"id">(), service::edge::persistence::EdgeFirmwareEntity::columns_type>::name.view(), "firmware"),
                                       ruvia::db_binary_operator::equal,
                                       recovery.column("firmware_id", "completed")))
                .returning({recovery.cast(recovery.column(std::tuple_element_t<service::edge::persistence::EdgeFirmwareEntity::column_index<"id">(), service::edge::persistence::EdgeFirmwareEntity::columns_type>::name.view(), "firmware"),
                                          ruvia::db_data_type::text)});
            const auto rebooted = co_await context.db().query(recovery);
            if (!rebooted.empty())
                co_await scheduleFirmwareCleanup(context, workerIndex);
        }
    }

    static ruvia::task<void> saveHeartbeat(ruvia::web_worker_context& context,
                                           std::string_view nodeId,
                                           const pb::Heartbeat& heartbeat) {
        ruvia::db_query query;
        const auto text = [&query](std::string_view value) {
            return query.cast(query.value(value), ruvia::db_data_type::text);
        };
        const auto integer = [&query](auto value) {
            return query.cast(query.value(static_cast<std::int64_t>(value)),
                              ruvia::db_data_type::big_int);
        };
        const auto boolean = [&query](bool value) {
            return query.cast(query.value(value), ruvia::db_data_type::boolean);
        };
        const auto status = query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view());
        const auto desiredVersion = config::detail::configVersion(
            query, status, "desiredVersion");
        const auto receivedVersion = integer(heartbeat.active_config_version());
        const auto applied = query.binary(
            query.binary(desiredVersion, ruvia::db_binary_operator::equal,
                         receivedVersion),
            ruvia::db_binary_operator::and_value,
            query.binary(receivedVersion, ruvia::db_binary_operator::greater,
                         integer(0)));
        const auto config = query.call(
            "jsonb_build_object",
            {config::detail::jsonKey(query, "activeVersion"),
             receivedVersion,
             config::detail::jsonKey(query, "desiredVersion"), desiredVersion,
             config::detail::jsonKey(query, "state"),
             query.case_when({{applied, text("applied")}},
                            config::detail::configState(query, status)),
             config::detail::jsonKey(query, "message"),
             query.case_when(
                 {{applied, text(std::string_view{})}},
                 query.coalesce({config::detail::jsonText(
                                     query, config::detail::jsonGet(query, status, "config"),
                                     "message"),
                                 query.value(std::string_view{})}))});
        const auto signal = query.call(
            "jsonb_build_object", {config::detail::jsonKey(query, "csq"),
                                    integer(heartbeat.signal_csq()),
                                    config::detail::jsonKey(query, "rssiDbm"),
                                    integer(heartbeat.signal_rssi_dbm()),
                                    config::detail::jsonKey(query, "percent"),
                                    integer(heartbeat.signal_percent())});
        const auto existingMobile = query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"mobile">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view());
        const auto apn = query.coalesce({
            query.null_if(text(heartbeat.apn()), query.value(std::string_view{})),
            config::detail::jsonText(query, existingMobile, "apn"),
            query.value(std::string_view{})});
        const auto operatorName = query.coalesce({
            query.null_if(text(heartbeat.mobile_operator()), query.value(std::string_view{})),
            config::detail::jsonText(query, existingMobile, "operator"),
            query.value(std::string_view{})});
        const auto mobile = query.call(
            "jsonb_build_object",
            {config::detail::jsonKey(query, "available"), boolean(heartbeat.modem_available()),
             config::detail::jsonKey(query, "simState"), text(simState(heartbeat.sim_state())),
             config::detail::jsonKey(query, "iccid"), text(heartbeat.iccid()),
             config::detail::jsonKey(query, "signal"), signal,
             config::detail::jsonKey(query, "registered"), boolean(heartbeat.mobile_registered()),
             config::detail::jsonKey(query, "registrationStatus"),
             integer(heartbeat.mobile_registration_status()),
             config::detail::jsonKey(query, "apn"), apn,
             config::detail::jsonKey(query, "operator"), operatorName,
             config::detail::jsonKey(query, "connected"), boolean(heartbeat.mobile_connected()),
             config::detail::jsonKey(query, "ipv4"), text(heartbeat.mobile_ipv4())});
        const auto tcpTrafficText = [&query](std::string_view key) {
            return config::detail::jsonText(
                query,
                config::detail::jsonGet(
                    query,
                    query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                    "tcpTraffic"),
                key);
        };
        const auto shanghaiNow = query.call("timezone", {text("Asia/Shanghai"), query.call("now")});
        const auto currentMonth = query.call("to_char", {shanghaiNow, text("YYYY-MM")});
        const auto currentDay = query.call("to_char", {shanghaiNow, text("YYYY-MM-DD")});
        const auto tcpTraffic = heartbeat.has_tcp_traffic()
            ? [&] {
                const auto& sample = heartbeat.tcp_traffic();
                const auto sameSample = query.binary(
                    tcpTrafficText("sampleId"), ruvia::db_binary_operator::equal,
                    text(std::to_string(sample.sample_id())));
                const auto sameMonth = query.binary(
                    tcpTrafficText("month"), ruvia::db_binary_operator::equal, currentMonth);
                const auto monthMissing = query.unary(
                    ruvia::db_unary_operator::is_null,
                    query.null_if(tcpTrafficText("month"), query.value(std::string_view{})));
                const auto continueMonth = query.binary(
                    sameMonth, ruvia::db_binary_operator::or_value, monthMissing);
                const auto sameDay = query.binary(
                    tcpTrafficText("day"), ruvia::db_binary_operator::equal, currentDay);
                const auto dayMissing = query.unary(
                    ruvia::db_unary_operator::is_null,
                    query.null_if(tcpTrafficText("day"), query.value(std::string_view{})));
                const auto continueDay = query.binary(
                    sameDay, ruvia::db_binary_operator::or_value, dayMissing);
                const auto periodTotal = [&](std::string_view key, const auto& continuePeriod, std::uint64_t bytes) {
                    const auto kept = query.coalesce({
                        query.cast(
                            query.null_if(tcpTrafficText(key), query.value(std::string_view{})),
                            ruvia::db_data_type::big_int),
                        integer(0)});
                    const auto added = query.binary(kept, ruvia::db_binary_operator::add, integer(bytes));
                    return query.cast(
                        query.case_when(
                            {{sameSample, kept},
                             {continuePeriod, added}},
                            integer(bytes)),
                        ruvia::db_data_type::text);
                };
                const auto monthValue = query.case_when(
                    {{sameSample, query.coalesce({tcpTrafficText("month"), currentMonth})}},
                    currentMonth);
                const auto dayValue = query.case_when(
                    {{sameSample, query.coalesce({tcpTrafficText("day"), currentDay})}},
                    currentDay);
                return query.call("jsonb_build_object", {
                    config::detail::jsonKey(query, "uploadBytes"), text(std::to_string(sample.upload_bytes())),
                    config::detail::jsonKey(query, "downloadBytes"), text(std::to_string(sample.download_bytes())),
                    config::detail::jsonKey(query, "intervalMs"), text(std::to_string(sample.interval_ms())),
                    config::detail::jsonKey(query, "sampleId"), text(std::to_string(sample.sample_id())),
                    config::detail::jsonKey(query, "month"), monthValue,
                    config::detail::jsonKey(query, "monthlyUploadBytes"), periodTotal("monthlyUploadBytes", continueMonth, sample.upload_bytes()),
                    config::detail::jsonKey(query, "monthlyDownloadBytes"), periodTotal("monthlyDownloadBytes", continueMonth, sample.download_bytes()),
                    config::detail::jsonKey(query, "day"), dayValue,
                    config::detail::jsonKey(query, "dailyUploadBytes"), periodTotal("dailyUploadBytes", continueDay, sample.upload_bytes()),
                    config::detail::jsonKey(query, "dailyDownloadBytes"), periodTotal("dailyDownloadBytes", continueDay, sample.download_bytes())});
            }()
            : config::detail::jsonGet(
                  query,
                  query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                  "tcpTraffic");
        const auto vpnTrafficText = [&query](std::string_view key) {
            return config::detail::jsonText(
                query,
                config::detail::jsonGet(
                    query,
                    query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                    "vpnTraffic"),
                key);
        };
        const auto vpnTraffic = heartbeat.has_vpn_traffic()
            ? [&] {
                const auto& sample = heartbeat.vpn_traffic();
                const auto sameSample = query.binary(
                    vpnTrafficText("sampleId"), ruvia::db_binary_operator::equal,
                    text(std::to_string(sample.sample_id())));
                const auto sameMonth = query.binary(
                    vpnTrafficText("month"), ruvia::db_binary_operator::equal, currentMonth);
                const auto monthMissing = query.unary(
                    ruvia::db_unary_operator::is_null,
                    query.null_if(vpnTrafficText("month"), query.value(std::string_view{})));
                const auto continueMonth = query.binary(
                    sameMonth, ruvia::db_binary_operator::or_value, monthMissing);
                const auto sameDay = query.binary(
                    vpnTrafficText("day"), ruvia::db_binary_operator::equal, currentDay);
                const auto dayMissing = query.unary(
                    ruvia::db_unary_operator::is_null,
                    query.null_if(vpnTrafficText("day"), query.value(std::string_view{})));
                const auto continueDay = query.binary(
                    sameDay, ruvia::db_binary_operator::or_value, dayMissing);
                const auto periodTotal = [&](std::string_view key, const auto& continuePeriod, std::uint64_t bytes) {
                    const auto kept = query.coalesce({
                        query.cast(
                            query.null_if(vpnTrafficText(key), query.value(std::string_view{})),
                            ruvia::db_data_type::big_int),
                        integer(0)});
                    const auto added = query.binary(kept, ruvia::db_binary_operator::add, integer(bytes));
                    return query.cast(
                        query.case_when(
                            {{sameSample, kept},
                             {continuePeriod, added}},
                            integer(bytes)),
                        ruvia::db_data_type::text);
                };
                const auto monthValue = query.case_when(
                    {{sameSample, query.coalesce({vpnTrafficText("month"), currentMonth})}},
                    currentMonth);
                const auto dayValue = query.case_when(
                    {{sameSample, query.coalesce({vpnTrafficText("day"), currentDay})}},
                    currentDay);
                return query.call("jsonb_build_object", {
                    config::detail::jsonKey(query, "uploadBytes"), text(std::to_string(sample.upload_bytes())),
                    config::detail::jsonKey(query, "downloadBytes"), text(std::to_string(sample.download_bytes())),
                    config::detail::jsonKey(query, "intervalMs"), text(std::to_string(sample.interval_ms())),
                    config::detail::jsonKey(query, "sampleId"), text(std::to_string(sample.sample_id())),
                    config::detail::jsonKey(query, "month"), monthValue,
                    config::detail::jsonKey(query, "monthlyUploadBytes"), periodTotal("monthlyUploadBytes", continueMonth, sample.upload_bytes()),
                    config::detail::jsonKey(query, "monthlyDownloadBytes"), periodTotal("monthlyDownloadBytes", continueMonth, sample.download_bytes()),
                    config::detail::jsonKey(query, "day"), dayValue,
                    config::detail::jsonKey(query, "dailyUploadBytes"), periodTotal("dailyUploadBytes", continueDay, sample.upload_bytes()),
                    config::detail::jsonKey(query, "dailyDownloadBytes"), periodTotal("dailyDownloadBytes", continueDay, sample.download_bytes())});
            }()
            : config::detail::jsonGet(
                  query,
                  query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                  "vpnTraffic");
        query.update(service::edge::persistence::EdgeNodeEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), query.call(
                               "jsonb_build_object",
                               {config::detail::jsonKey(query, "config"), config,
                                config::detail::jsonKey(query, "outbox"),
                                query.call("jsonb_build_object",
                                           {config::detail::jsonKey(query, "records"),
                                            integer(heartbeat.outbox_records()),
                                            config::detail::jsonKey(query, "bytes"),
                                            integer(heartbeat.outbox_bytes())}),
                                config::detail::jsonKey(query, "tcpTraffic"), tcpTraffic,
                                config::detail::jsonKey(query, "vpnTraffic"), vpnTraffic,
                                config::detail::jsonKey(query, "log"),
                                query.call("jsonb_build_object",
                                           {config::detail::jsonKey(query, "level"),
                                            config::detail::textDefault(
                                                query, text(heartbeat.log_level()), "info")})}))
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"mobile">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), mobile)
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), query.call(
                                  "jsonb_set",
                                  {query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                                   config::detail::jsonPath(query, "{modemControl}"),
                                   config::detail::toJsonb(
                                       query, boolean(heartbeat.supports_modem_control())),
                                   query.cast(query.value(true), ruvia::db_data_type::boolean)}))
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"last_seen_at">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), query.call("now"))
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"updated_at">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), query.call("now"))
            .where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                query.cast(query.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(query);
        if (heartbeat.active_config_version() != 0) {
            ruvia::db_query revision;
            revision.update(service::edge::persistence::EdgeConfigRevisionEntity::table_name(), "revision")
                .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"status">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.value(std::string_view{"applied"}))
                .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"message">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.value(std::string_view{}))
                .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"completed_at">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.coalesce({
                    revision.column(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"completed_at">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), "revision"), revision.call("now")}))
                .update_from(service::edge::persistence::EdgeNodeEntity::table_name(), "node")
                .where(revision.binary(
                    revision.column(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"node_id">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), "revision"), ruvia::db_binary_operator::equal,
                    revision.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), "node")))
                .and_where(revision.binary(
                    revision.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), "node"), ruvia::db_binary_operator::equal,
                    revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid)))
                .and_where(revision.binary(
                    revision.column(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"revision">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), "revision"),
                    ruvia::db_binary_operator::equal,
                    revision.cast(revision.value(static_cast<std::int64_t>(
                                                  heartbeat.active_config_version())),
                                                ruvia::db_data_type::big_int)))
                .and_where(revision.binary(
                    config::detail::configVersion(
                        revision, revision.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), "node"), "desiredVersion"),
                    ruvia::db_binary_operator::equal,
                    revision.cast(revision.value(static_cast<std::int64_t>(
                                                  heartbeat.active_config_version())),
                                  ruvia::db_data_type::big_int)));
            (void)co_await context.db().execute(revision);
        }
    }

    static std::string mac(const pb::InterfaceCapability& item) {
        if (item.mac().size() != 6)
            return {};
        constexpr char digits[] = "0123456789abcdef";
        std::string output;
        output.reserve(17);
        for (std::size_t index = 0; index < 6; ++index) {
            if (index != 0)
                output.push_back(':');
            const auto byte = static_cast<std::uint8_t>(item.mac()[index]);
            output.push_back(digits[byte >> 4U]);
            output.push_back(digits[byte & 0x0fU]);
        }
        return output;
    }

    template <typename Strings>
    static std::string jsonArray(const Strings& bridgePorts) {
        std::string output{"["};
        bool first = true;
        for (const auto& port : bridgePorts) {
            if (!first)
                output.push_back(',');
            output.push_back('"');
            output += service::utils::jsonEscape(port);
            output.push_back('"');
            first = false;
        }
        output.push_back(']');
        return output;
    }

    static std::string addressMode(pb::NetworkAddressMode mode) {
        switch (mode) {
        case pb::NETWORK_ADDRESS_DHCP:
            return "dhcp";
        case pb::NETWORK_ADDRESS_STATIC:
            return "static";
        default:
            return "none";
        }
    }

    static ruvia::task<void> saveCapabilities(
        ruvia::web_worker_context& context, std::size_t workerIndex, std::string_view nodeId,
        const pb::CapabilityReport& report) {
        using config::detail::columnName;
        using Interface = persistence::EdgeNodeInterfaceEntity;
        using Network = persistence::EdgeNodeNetworkEntity;
        using Serial = persistence::EdgeNodeSerialEntity;
        ruvia::db_query deleteInterfaces;
        deleteInterfaces.delete_from(service::edge::persistence::EdgeNodeInterfaceEntity::table_name())
            .where(deleteInterfaces.binary(
                deleteInterfaces.column(std::tuple_element_t<service::edge::persistence::EdgeNodeInterfaceEntity::column_index<"node_id">(), service::edge::persistence::EdgeNodeInterfaceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                deleteInterfaces.cast(deleteInterfaces.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(deleteInterfaces);
        for (const auto& item : report.interfaces()) {
            const auto macAddress = mac(item);
            const auto ports = jsonArray(item.bridge_ports());
            ruvia::db_query insert;
            insert.insert_into(Interface::table_name(),
                              {columnName<Interface, "node_id">(), columnName<Interface, "name">(),
                               columnName<Interface, "display_name">(), columnName<Interface, "mac">(),
                               columnName<Interface, "is_up">(), columnName<Interface, "is_bridge">(),
                               columnName<Interface, "ipv4">(), columnName<Interface, "prefix_length">(),
                               columnName<Interface, "gateway">(), columnName<Interface, "bridge_ports">()})
                .values({
                    insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                    insert.value(std::string_view(item.name())),
                    insert.value(std::string_view(item.display_name())),
                    insert.null_if(insert.value(std::string_view(macAddress)),
                                  insert.value(std::string_view{})),
                    insert.cast(insert.value(item.up()), ruvia::db_data_type::boolean),
                    insert.cast(insert.value(item.bridge()), ruvia::db_data_type::boolean),
                    insert.null_if(insert.value(std::string_view(item.ipv4())),
                                  insert.value(std::string_view{})),
                    insert.value(item.prefix_length()),
                    insert.null_if(insert.value(std::string_view(item.gateway())),
                                  insert.value(std::string_view{})),
                    insert.cast(insert.value(std::string_view(ports)),
                                 ruvia::db_data_type::jsonb),
                });
            (void)co_await context.db().execute(insert);
        }
        ruvia::db_query deleteNetworks;
        deleteNetworks.delete_from(service::edge::persistence::EdgeNodeNetworkEntity::table_name())
            .where(deleteNetworks.binary(
                deleteNetworks.column(std::tuple_element_t<service::edge::persistence::EdgeNodeNetworkEntity::column_index<"node_id">(), service::edge::persistence::EdgeNodeNetworkEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                deleteNetworks.cast(deleteNetworks.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(deleteNetworks);
        for (const auto& item : report.networks()) {
            const auto ports = jsonArray(item.bridge_ports());
            const auto mode = addressMode(item.mode());
            ruvia::db_query insert;
            insert.insert_into(Network::table_name(),
                              {columnName<Network, "node_id">(), columnName<Network, "name">(),
                               columnName<Network, "address_mode">(), columnName<Network, "device">(),
                               columnName<Network, "is_up">(), columnName<Network, "is_bridge">(),
                               columnName<Network, "ipv4">(), columnName<Network, "prefix_length">(),
                               columnName<Network, "gateway">(), columnName<Network, "bridge_ports">()})
                .values({
                    insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                    insert.value(std::string_view(item.name())),
                    insert.value(std::string_view(mode)),
                    insert.value(std::string_view(item.device())),
                    insert.cast(insert.value(item.up()), ruvia::db_data_type::boolean),
                    insert.cast(insert.value(item.bridge()), ruvia::db_data_type::boolean),
                    insert.null_if(insert.value(std::string_view(item.ipv4())),
                                  insert.value(std::string_view{})),
                    insert.value(item.prefix_length()),
                    insert.null_if(insert.value(std::string_view(item.gateway())),
                                  insert.value(std::string_view{})),
                    insert.cast(insert.value(std::string_view(ports)),
                                 ruvia::db_data_type::jsonb),
                });
            (void)co_await context.db().execute(insert);
        }
        ruvia::db_query deleteSerial;
        deleteSerial.delete_from(service::edge::persistence::EdgeNodeSerialEntity::table_name())
            .where(deleteSerial.binary(
                deleteSerial.column(std::tuple_element_t<service::edge::persistence::EdgeNodeSerialEntity::column_index<"node_id">(), service::edge::persistence::EdgeNodeSerialEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                deleteSerial.cast(deleteSerial.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(deleteSerial);
        for (const auto& item : report.serial_ports()) {
            ruvia::db_query insert;
            insert.insert_into(Serial::table_name(),
                              {columnName<Serial, "node_id">(), columnName<Serial, "path">(),
                               columnName<Serial, "display_name">(), columnName<Serial, "available">(),
                               columnName<Serial, "rs485">()})
                .values({
                    insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                    insert.value(std::string_view(item.path())),
                    insert.value(std::string_view(item.display_name())),
                    insert.cast(insert.value(item.available()), ruvia::db_data_type::boolean),
                    insert.cast(insert.value(item.rs485()), ruvia::db_data_type::boolean),
                });
            (void)co_await context.db().execute(insert);
        }
        std::vector<std::string> supported;
        for (const auto protocol : report.supported_protocols()) {
            switch (protocol) {
            case pb::PROTOCOL_MODBUS: supported.emplace_back("Modbus"); break;
            case pb::PROTOCOL_S7: supported.emplace_back("S7"); break;
            case pb::PROTOCOL_SL651: supported.emplace_back("SL651"); break;
            case pb::PROTOCOL_MC: supported.emplace_back("MC"); break;
            case pb::PROTOCOL_FINS: supported.emplace_back("FINS"); break;
            case pb::PROTOCOL_DLT645: supported.emplace_back("DLT645"); break;
            default: break;
            }
        }
        const auto protocols = jsonArray(supported);
        ruvia::db_query terminal;
        terminal.update(service::edge::persistence::EdgeNodeEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), terminal.call(
                                   "jsonb_set",
                                   {terminal.call("jsonb_set", {
                                        terminal.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                                        config::detail::jsonPath(terminal, "{protocols}"),
                                        terminal.cast(terminal.value(std::string_view(protocols)), ruvia::db_data_type::jsonb),
                                        terminal.value(true)}),
                                    config::detail::jsonPath(terminal, "{terminal}"),
                                    config::detail::toJsonb(
                                        terminal, terminal.cast(terminal.value(
                                                                           report.ttyd_available()),
                                                                       ruvia::db_data_type::boolean)),
                                    terminal.cast(terminal.value(true),
                                                  ruvia::db_data_type::boolean)}))
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"updated_at">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), terminal.call("now"))
            .where(terminal.binary(terminal.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                   terminal.cast(terminal.value(nodeId),
                                                 ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(terminal);
        ruvia::db_query serialCapability;
        serialCapability.update(persistence::EdgeNodeEntity::table_name())
            .set(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"capability">(), persistence::EdgeNodeEntity::columns_type>::name.view(),
                serialCapability.call("jsonb_set", {
                    serialCapability.column(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"capability">(), persistence::EdgeNodeEntity::columns_type>::name.view()),
                    config::detail::jsonPath(serialCapability, "{serialDebug}"),
                    config::detail::toJsonb(serialCapability, serialCapability.cast(
                        serialCapability.value(report.supports_serial_debug()), ruvia::db_data_type::boolean)),
                    serialCapability.value(true)}))
            .where(serialCapability.binary(serialCapability.column(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"id">(), persistence::EdgeNodeEntity::columns_type>::name.view()),
                ruvia::db_binary_operator::equal, serialCapability.cast(serialCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(serialCapability);
        ruvia::db_query dtuCapability;
        dtuCapability.update(persistence::EdgeNodeEntity::table_name())
            .set(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"capability">(), persistence::EdgeNodeEntity::columns_type>::name.view(), dtuCapability.call("jsonb_set", {
                dtuCapability.column(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"capability">(), persistence::EdgeNodeEntity::columns_type>::name.view()),
                config::detail::jsonPath(dtuCapability, "{dtu}"),
                config::detail::toJsonb(dtuCapability, dtuCapability.cast(dtuCapability.value(report.supports_dtu()), ruvia::db_data_type::boolean)), dtuCapability.value(true)}))
            .where(dtuCapability.binary(dtuCapability.column(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"id">(), persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                dtuCapability.cast(dtuCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(dtuCapability);
        ruvia::db_query derivedCapability;
        derivedCapability.update(persistence::EdgeNodeEntity::table_name())
            .set(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"capability">(), persistence::EdgeNodeEntity::columns_type>::name.view(), derivedCapability.call("jsonb_set", {
                derivedCapability.column(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"capability">(), persistence::EdgeNodeEntity::columns_type>::name.view()), config::detail::jsonPath(derivedCapability, "{derivedPoints}"),
                config::detail::toJsonb(derivedCapability, derivedCapability.cast(derivedCapability.value(report.supports_derived_points()), ruvia::db_data_type::boolean)), derivedCapability.value(true)}))
            .where(derivedCapability.binary(derivedCapability.column(std::tuple_element_t<persistence::EdgeNodeEntity::column_index<"id">(), persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, derivedCapability.cast(derivedCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(derivedCapability);
        (void)co_await ConfigService::requeueIfStale(context, nodeId);
        if (report.has_vpn()) {
            const auto vpnPublicKey = service::common::wireguard::validManagedKey(report.vpn().public_key())
                                          ? std::string(report.vpn().public_key())
                                          : std::string{};
            ruvia::db_query vpn;
            vpn.update(service::edge::persistence::EdgeNodeEntity::table_name())
                .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), vpn.call(
                                       "jsonb_set",
                                       {vpn.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"capability">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()),
                                        config::detail::jsonPath(vpn, "{vpn}"),
                                        vpn.call(
                                            "jsonb_build_object",
                                            {config::detail::jsonKey(vpn, "supportsVpn"),
                                             vpn.cast(vpn.value(report.vpn().supports_vpn()),
                                                      ruvia::db_data_type::boolean),
                                             config::detail::jsonKey(vpn, "wireguardVersion"),
                                             vpn.cast(vpn.value(std::string_view(
                                                                     report.vpn().wireguard_version())),
                                                      ruvia::db_data_type::text),
                                             config::detail::jsonKey(vpn, "agentVersion"),
                                             vpn.cast(vpn.value(std::string_view(
                                                                     report.vpn().agent_version())),
                                                      ruvia::db_data_type::text),
                                             config::detail::jsonKey(vpn, "publicKey"),
                                             vpn.cast(vpn.value(std::string_view(vpnPublicKey)),
                                                      ruvia::db_data_type::text)}),
                                        vpn.cast(vpn.value(true), ruvia::db_data_type::boolean)}))
                .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"updated_at">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), vpn.call("now"))
                .where(vpn.binary(vpn.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                  vpn.cast(vpn.value(nodeId), ruvia::db_data_type::uuid)));
            (void)co_await context.db().execute(vpn);
            // vpn_peer activation, bridge route allocation and Hub reconciliation
            // belong to the VPN feature; it receives the reported key as an event.
            if (report.vpn().supports_vpn() && !vpnPublicKey.empty()) {
                service::message::vpn::EdgeCapability capability(ruvia::model_options{.resource_ = context.pool()});
                capability.set<"nodeId">(nodeId).set<"publicKey">(vpnPublicKey);
                co_await notifyFeature(context, workerIndex, service::message::vpn::kRpcComponent,
                                       service::message::vpn::kEdgeCapabilityOperation,
                                       service::message::rpc::encode(capability, context.pool()));
            }
        }
    }

    static ruvia::task<void> saveNetworkResult(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::NetworkConfigResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        const std::string status = result.success() ? "succeeded" : "failed";
        const std::string json = "{\"message\":" + service::utils::jsonQuoted(result.message()) +
                                 ",\"rolled_back\":" +
                                 (result.rolled_back() ? "true" : "false") + "}";
        ruvia::db_query query;
        query.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.value(std::string_view(status)))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"result">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.cast(query.value(std::string_view(json)),
                                       ruvia::db_data_type::jsonb))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"updated_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.call("now"))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"completed_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.call("now"))
            .where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"node_id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                   query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"task_type">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                   query.value(std::string_view{"network"})))
            .and_where(query.binary(
                query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::not_in,
                query.list({query.value(std::string_view{"succeeded"}),
                            query.value(std::string_view{"failed"})})));
        (void)co_await context.db().execute(query);
    }

    // Completes the VPN edge_task; the VPN feature projects the result onto vpn_route.
    static ruvia::task<void> saveVpnResult(
        ruvia::web_worker_context& context, std::size_t workerIndex, std::string_view nodeId,
        const pb::VpnConfigResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        const bool applied = result.applied();
        const std::string status = applied ? "succeeded" : "failed";
        const std::string json = "{\"configVersion\":" +
                                 std::to_string(result.config_version()) +
                                 ",\"errorCode\":" + service::utils::jsonQuoted(result.error_code()) +
                                 ",\"errorMessage\":" + service::utils::jsonQuoted(result.error_message()) + "}";
        ruvia::db_query transitioned;
        transitioned.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), transitioned.value(std::string_view(status)))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"result">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), transitioned.cast(
                               transitioned.value(std::string_view(json)),
                               ruvia::db_data_type::jsonb))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"updated_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), transitioned.call("now"))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"completed_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), transitioned.call("now"))
            .where(transitioned.binary(
                transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(id), ruvia::db_data_type::uuid)))
            .and_where(transitioned.binary(
                transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"node_id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(transitioned.binary(
                transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"task_type">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                transitioned.value(std::string_view{"vpn"})))
            .and_where(transitioned.binary(
                transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::not_in,
                transitioned.list({transitioned.value(std::string_view{"succeeded"}),
                                   transitioned.value(std::string_view{"failed"})})))
            .returning({config::detail::jsonText(
                            transitioned, transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"request">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), "peerId"),
                        config::detail::jsonText(
                            transitioned, transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"request">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), "enabled"),
                        config::detail::jsonText(
                            transitioned, transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"request">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), "configVersion")});
        const auto rows = co_await context.db().query(transitioned);
        for (const auto& row : rows) {
            const auto configVersion = service::utils::parseInt64(row[2].value());
            if (!configVersion)
                continue;
            service::message::vpn::EdgeConfigResult event(ruvia::model_options{.resource_ = context.pool()});
            event.set<"peerId">(row[0].value().value_or(std::string_view{}))
                .set<"configVersion">(*configVersion)
                .set<"enabled">(row[1].value().value_or(std::string_view{"true"}) != "false")
                .set<"applied">(applied)
                .set<"errorMessage">(std::string_view(result.error_message()));
            co_await notifyFeature(context, workerIndex, service::message::vpn::kRpcComponent,
                                   service::message::vpn::kEdgeConfigResultOperation,
                                   service::message::rpc::encode(event, context.pool()));
        }
    }

    static ruvia::task<void> saveFirmwareResult(
        ruvia::web_worker_context& context, std::size_t workerIndex, std::string_view nodeId,
        const pb::FirmwareUpdateResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        std::string status = "running";
        bool completed = false;
        if (result.state() == pb::FIRMWARE_UPDATE_ACCEPTED)
            status = "accepted";
        else if (result.state() == pb::FIRMWARE_UPDATE_FAILED) {
            status = "failed";
            completed = true;
        }
        std::string state = "running";
        if (result.state() == pb::FIRMWARE_UPDATE_ACCEPTED)
            state = "accepted";
        else if (result.state() == pb::FIRMWARE_UPDATE_DOWNLOADING)
            state = "downloading";
        else if (result.state() == pb::FIRMWARE_UPDATE_VERIFYING)
            state = "verifying";
        else if (result.state() == pb::FIRMWARE_UPDATE_FLASHING)
            state = "flashing";
        else if (result.state() == pb::FIRMWARE_UPDATE_FAILED)
            state = "failed";
        const std::string json =
            "{\"state\":\"" + state + "\",\"message\":\"" +
            service::utils::jsonEscape(result.message()) +
            "\",\"progressPercent\":" + std::to_string(result.progress_percent()) +
            ",\"downloadedBytes\":" + std::to_string(result.downloaded_bytes()) +
            ",\"totalBytes\":" + std::to_string(result.total_bytes()) + "}";
        ruvia::db_query query;
        query.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.value(std::string_view(status)))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"result">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.cast(query.value(std::string_view(json)),
                                       ruvia::db_data_type::jsonb))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"updated_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.call("now"))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"completed_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.case_when(
                                     {{query.cast(query.value(completed),
                                                  ruvia::db_data_type::boolean),
                                       query.call("now")} },
                                     query.null_value()))
            .where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"node_id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                   query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"task_type">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                   query.value(std::string_view{"firmware"})))
            .and_where(query.binary(
                query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::not_in,
                query.list({query.value(std::string_view{"succeeded"}),
                            query.value(std::string_view{"failed"})})));
        const auto updated = co_await context.db().execute(query);
        if (completed && updated.affected_rows() != 0)
            co_await scheduleFirmwareCleanup(context, workerIndex);
    }

    static ruvia::task<void> saveModemResult(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::ModemControlResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        std::string status = "running";
        bool completed = false;
        if (result.state() == pb::MODEM_CONTROL_ACCEPTED)
            status = "accepted";
        else if (result.state() == pb::MODEM_CONTROL_SUCCEEDED) {
            status = "succeeded";
            completed = true;
        } else if (result.state() == pb::MODEM_CONTROL_FAILED) {
            status = "failed";
            completed = true;
        }
        const std::string json = "{\"message\":" + service::utils::jsonQuoted(result.message()) +
                                 ",\"apn\":" + service::utils::jsonQuoted(result.apn()) + "}";
        ruvia::db_query query;
        query.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.value(std::string_view(status)))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"result">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.cast(query.value(std::string_view(json)),
                                       ruvia::db_data_type::jsonb))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"updated_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.call("now"))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"completed_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), query.case_when(
                                     {{query.cast(query.value(completed),
                                                  ruvia::db_data_type::boolean),
                                       query.call("now")} },
                                     query.null_value()))
            .where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"node_id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                   query.cast(query.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"task_type">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                                   query.value(std::string_view{"modem"})))
            .and_where(query.binary(
                query.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::not_in,
                query.list({query.value(std::string_view{"succeeded"}),
                            query.value(std::string_view{"failed"})})));
        (void)co_await context.db().execute(query);
    }

    static ruvia::task<void> savePlatformResult(
        ruvia::web_worker_context& context, std::string_view nodeId,
        const pb::PlatformConfigResult& result) {
        if (result.request_id().size() != 16)
            co_return;
        const auto id = protocol::uuidText(result.request_id());
        const std::string status = result.success() ? "succeeded" : "failed";
        const std::string json = "{\"message\":" + service::utils::jsonQuoted(result.message()) + "}";
        ruvia::db_query transitioned;
        transitioned.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), transitioned.value(std::string_view(status)))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"result">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), transitioned.cast(
                               transitioned.value(std::string_view(json)),
                               ruvia::db_data_type::jsonb))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"updated_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), transitioned.call("now"))
            .set(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"completed_at">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view(), transitioned.call("now"))
            .where(transitioned.binary(
                transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(id), ruvia::db_data_type::uuid)))
            .and_where(transitioned.binary(
                transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"node_id">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(transitioned.binary(
                transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"task_type">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::in,
                transitioned.list({transitioned.value(std::string_view{"platform_upsert"}),
                                   transitioned.value(std::string_view{"platform_delete"})})))
            .and_where(transitioned.binary(
                transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"status">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), ruvia::db_binary_operator::not_in,
                transitioned.list({transitioned.value(std::string_view{"succeeded"}),
                                   transitioned.value(std::string_view{"failed"})})))
            .returning({transitioned.alias(
                            transitioned.cast(
                                config::detail::jsonText(
                                    transitioned, transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"request">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view()), "platform_id"),
                                ruvia::db_data_type::uuid),
                            "platform_id"),
                        transitioned.column(std::tuple_element_t<service::edge::persistence::EdgeTaskEntity::column_index<"task_type">(), service::edge::persistence::EdgeTaskEntity::columns_type>::name.view())});

        ruvia::db_query updated;
        const auto success = updated.cast(updated.value(result.success()),
                                          ruvia::db_data_type::boolean);
        const auto deleteTask = updated.binary(
            updated.column("task_type", "task"), ruvia::db_binary_operator::equal,
            updated.value(std::string_view{"platform_delete"}));
        updated.update(service::edge::persistence::EdgeNodePlatformEntity::table_name(), "target")
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodePlatformEntity::column_index<"status">(), service::edge::persistence::EdgeNodePlatformEntity::columns_type>::name.view(), updated.call(
                               "jsonb_build_object",
                               {config::detail::jsonKey(updated, "state"),
                                updated.cast(updated.value(std::string_view(
                                                               result.success() ? "applied"
                                                                                : "failed")),
                                             ruvia::db_data_type::text),
                                config::detail::jsonKey(updated, "message"),
                                updated.cast(updated.value(std::string_view(result.message())),
                                             ruvia::db_data_type::text)}))
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodePlatformEntity::column_index<"updated_at">(), service::edge::persistence::EdgeNodePlatformEntity::columns_type>::name.view(), updated.call("now"))
            .update_from("transitioned", "task")
            .where(updated.binary(
                updated.column(std::tuple_element_t<service::edge::persistence::EdgeNodePlatformEntity::column_index<"node_id">(), service::edge::persistence::EdgeNodePlatformEntity::columns_type>::name.view(), "target"), ruvia::db_binary_operator::equal,
                updated.cast(updated.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(updated.binary(updated.column(std::tuple_element_t<service::edge::persistence::EdgeNodePlatformEntity::column_index<"platform_id">(), service::edge::persistence::EdgeNodePlatformEntity::columns_type>::name.view(), "target"),
                                     ruvia::db_binary_operator::equal,
                                     updated.column("platform_id", "task")))
            .and_where(updated.unary(
                ruvia::db_unary_operator::not_value,
                updated.binary(success, ruvia::db_binary_operator::and_value, deleteTask)))
            .returning({updated.column(std::tuple_element_t<service::edge::persistence::EdgeNodePlatformEntity::column_index<"platform_id">(), service::edge::persistence::EdgeNodePlatformEntity::columns_type>::name.view(), "target")});

        ruvia::db_query cleanup;
        const auto cleanupSuccess = cleanup.import_expression(success);
        const auto cleanupDeleteTask = cleanup.import_expression(deleteTask);
        cleanup.with("transitioned", transitioned)
            .with("updated", updated)
            .delete_from(service::edge::persistence::EdgeNodePlatformEntity::table_name(), "target")
            .delete_using("transitioned", "task")
            .where(cleanup.binary(cleanupSuccess, ruvia::db_binary_operator::and_value,
                                  cleanupDeleteTask))
            .and_where(cleanup.binary(
                cleanup.column(std::tuple_element_t<service::edge::persistence::EdgeNodePlatformEntity::column_index<"node_id">(), service::edge::persistence::EdgeNodePlatformEntity::columns_type>::name.view(), "target"), ruvia::db_binary_operator::equal,
                cleanup.cast(cleanup.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(cleanup.binary(cleanup.column(std::tuple_element_t<service::edge::persistence::EdgeNodePlatformEntity::column_index<"platform_id">(), service::edge::persistence::EdgeNodePlatformEntity::columns_type>::name.view(), "target"),
                                     ruvia::db_binary_operator::equal,
                                     cleanup.column("platform_id", "task")));
        (void)co_await context.db().execute(cleanup);
    }

    static std::string hex(std::string_view value) {
        constexpr char digits[] = "0123456789abcdef";
        std::string output;
        output.reserve(value.size() * 2);
        for (const char item : value) {
            const auto byte = static_cast<std::uint8_t>(item);
            output.push_back(digits[byte >> 4U]);
            output.push_back(digits[byte & 0x0fU]);
        }
        return output;
    }

    static ruvia::task<void> saveConfigApplied(ruvia::web_worker_context& context,
                                                std::string_view nodeId,
                                                const pb::ConfigApplied& result) {
        if (result.revision() == 0 || result.sha256().size() != 32)
            co_return;
        const auto digest = hex(result.sha256());
        ruvia::db_query revision;
        revision.update(service::edge::persistence::EdgeConfigRevisionEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"status">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.value(std::string_view{"applied"}))
            .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"message">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.value(std::string_view{}))
            .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"completed_at">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.call("now"))
            .where(revision.binary(
                revision.column(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"node_id">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(revision.binary(
                revision.column(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"revision">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                revision.cast(revision.value(static_cast<std::int64_t>(result.revision())),
                              ruvia::db_data_type::big_int)))
            .and_where(revision.binary(revision.column(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"sha256">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view()),
                                      ruvia::db_binary_operator::equal,
                                      revision.value(std::string_view(digest))));
        (void)co_await context.db().execute(revision);

        ruvia::db_query node;
        const auto status = node.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view());
        const auto revisionValue = node.cast(
            node.value(static_cast<std::int64_t>(result.revision())),
            ruvia::db_data_type::big_int);
        const auto desired = config::detail::configVersion(node, status, "desiredVersion");
        const auto matches = node.binary(desired, ruvia::db_binary_operator::equal,
                                         revisionValue);
        const auto configWithActive = node.call(
            "jsonb_set",
            {status, config::detail::jsonPath(node, "{config,activeVersion}"),
             config::detail::toJsonb(
                 node, node.greatest({config::detail::configVersion(
                                           node, status, "activeVersion"),
                                      revisionValue})),
             node.cast(node.value(true), ruvia::db_data_type::boolean)});
        const auto configWithState = node.call(
            "jsonb_set",
            {configWithActive, config::detail::jsonPath(node, "{config,state}"),
             config::detail::toJsonb(
                 node, node.case_when(
                           {{matches, node.cast(node.value(std::string_view{"applied"}),
                                                ruvia::db_data_type::text)}},
                           config::detail::configState(node, status))),
             node.cast(node.value(true), ruvia::db_data_type::boolean)});
        const auto configWithMessage = node.call(
            "jsonb_set",
            {configWithState, config::detail::jsonPath(node, "{config,message}"),
             config::detail::toJsonb(
                 node, node.case_when(
                           {{matches, node.cast(node.value(std::string_view{}),
                                                ruvia::db_data_type::text)}},
                           node.coalesce({config::detail::jsonText(
                                              node,
                                              config::detail::jsonGet(node, status, "config"),
                                              "message"),
                                          node.value(std::string_view{})}))),
             node.cast(node.value(true), ruvia::db_data_type::boolean)});
        node.update(service::edge::persistence::EdgeNodeEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), configWithMessage)
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"updated_at">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), node.call("now"))
            .where(node.binary(node.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                               node.cast(node.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(node);
    }

    static ruvia::task<void> saveConfigRejected(ruvia::web_worker_context& context,
                                                 std::string_view nodeId,
                                                 const pb::ConfigRejected& result) {
        if (result.revision() == 0)
            co_return;
        const std::string message = result.code() + ": " + result.message();
        ruvia::db_query revision;
        revision.update(service::edge::persistence::EdgeConfigRevisionEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"status">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.value(std::string_view{"rejected"}))
            .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"message">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.value(std::string_view(message)))
            .set(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"completed_at">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view(), revision.call("now"))
            .where(revision.binary(
                revision.column(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"node_id">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(revision.binary(
                revision.column(std::tuple_element_t<service::edge::persistence::EdgeConfigRevisionEntity::column_index<"revision">(), service::edge::persistence::EdgeConfigRevisionEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                revision.cast(revision.value(static_cast<std::int64_t>(result.revision())),
                              ruvia::db_data_type::big_int)));
        (void)co_await context.db().execute(revision);

        ruvia::db_query node;
        const auto status = node.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view());
        const auto revisionValue = node.cast(
            node.value(static_cast<std::int64_t>(result.revision())),
            ruvia::db_data_type::big_int);
        const auto matches = node.binary(
            config::detail::configVersion(node, status, "desiredVersion"),
            ruvia::db_binary_operator::equal, revisionValue);
        const auto state = node.case_when(
            {{matches, node.cast(node.value(std::string_view{"rejected"}),
                                 ruvia::db_data_type::text)}},
            config::detail::configState(node, status));
        const auto messageValue = node.case_when(
            {{matches, node.cast(node.value(std::string_view(message)),
                                 ruvia::db_data_type::text)}},
            node.coalesce({config::detail::jsonText(
                               node, config::detail::jsonGet(node, status, "config"),
                               "message"),
                           node.value(std::string_view{})}));
        const auto statusWithState = node.call(
            "jsonb_set", {status, config::detail::jsonPath(node, "{config,state}"),
                           config::detail::toJsonb(node, state),
                           node.cast(node.value(true), ruvia::db_data_type::boolean)});
        node.update(service::edge::persistence::EdgeNodeEntity::table_name())
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"status">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), node.call(
                               "jsonb_set",
                               {statusWithState,
                                config::detail::jsonPath(node, "{config,message}"),
                                config::detail::toJsonb(node, messageValue),
                                node.cast(node.value(true),
                                          ruvia::db_data_type::boolean)}))
            .set(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"updated_at">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view(), node.call("now"))
            .where(node.binary(node.column(std::tuple_element_t<service::edge::persistence::EdgeNodeEntity::column_index<"id">(), service::edge::persistence::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                               node.cast(node.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await context.db().execute(node);
    }

  public:
    static pb::TelemetryRecord assembleTelemetryParts(const std::vector<pb::TelemetryRecord>& parts) {
        if (parts.empty() || parts.size() > 256)
            throw std::runtime_error("invalid telemetry upload part count");
        auto result = parts.front();
        result.set_record_id(result.report_id());
        result.clear_values();
        result.clear_raw_payloads();
        result.clear_raw_requests();
        result.clear_raw_packet_ids();
        std::set<std::string> elements;
        std::size_t rawBytes = 0;
        for (std::size_t index = 0; index < parts.size(); ++index) {
            const auto& part = parts[index];
            if (part.part_index() != index || part.part_count() != parts.size() ||
                part.report_id() != result.report_id() || part.device_id() != result.device_id() ||
                part.endpoint_id() != result.endpoint_id() || part.protocol() != result.protocol() ||
                part.observed_at_ms() != result.observed_at_ms() ||
                part.function_code() != result.function_code() || part.function_name() != result.function_name() ||
                part.direction() != result.direction() || part.model_id() != result.model_id())
                throw std::runtime_error("inconsistent telemetry upload parts");
            for (const auto& value : part.values()) {
                if (!elements.insert(value.element_id()).second)
                    throw std::runtime_error("duplicate element in telemetry upload");
                *result.add_values() = value;
            }
            for (const auto& raw : part.raw_payloads()) {
                rawBytes += raw.size();
                if (raw.empty() || raw.size() > 4112 || rawBytes > 147456 ||
                    result.raw_payloads_size() >= 4095)
                    throw std::runtime_error("invalid telemetry original frame array");
                result.add_raw_payloads(raw);
            }
            if ((parts.front().raw_requests_size() != 0) != (part.raw_requests_size() != 0) ||
                (part.raw_requests_size() != 0 &&
                    (part.raw_requests_size() != part.raw_payloads_size() || part.values_size() != 0)))
                throw std::runtime_error("inconsistent telemetry business exchange parts");
            for (const auto& request : part.raw_requests()) {
                rawBytes += request.size();
                if (request.size() > 4096 || rawBytes > 147456)
                    throw std::runtime_error("invalid telemetry business request");
                result.add_raw_requests(request);
            }
            if (part.raw_packet_ids_size() != 0 && part.raw_packet_ids_size() != part.raw_payloads_size())
                throw std::runtime_error("original packet ID count mismatch");
            for (const auto& id : part.raw_packet_ids()) {
                if (id.size() != 16) throw std::runtime_error("invalid original packet ID");
                result.add_raw_packet_ids(id);
            }
        }
        if (result.raw_payloads().empty())
            throw std::runtime_error("telemetry upload has no original frames");
        result.clear_report_id();
        result.clear_part_count();
        result.clear_part_index();
        return result;
    }

    template <typename Redis>
    static ruvia::task<std::optional<pb::TelemetryRecord>> storeTelemetryPart(
        const Redis& redis, std::string_view nodeId, const pb::TelemetryRecord& record) {
        if (record.report_id().size() != 16 || record.record_id().size() != 16 ||
            record.device_id().size() != 16 || record.part_count() == 0 ||
            record.part_count() > 256 || record.part_index() >= record.part_count() ||
            (record.protocol() != pb::PROTOCOL_SL651 && record.protocol() != pb::PROTOCOL_MODBUS &&
             record.protocol() != pb::PROTOCOL_S7 && record.protocol() != pb::PROTOCOL_MC &&
             record.protocol() != pb::PROTOCOL_FINS && record.protocol() != pb::PROTOCOL_DLT645) ||
            record.ByteSizeLong() > 14000)
            throw std::runtime_error("invalid telemetry upload part");
        const persistence::TelemetryUploadRecord stored{std::string(nodeId),
            protocol::uuidText(record.device_id()), protocol::uuidText(record.report_id())};
        auto metadata = record;
        metadata.clear_record_id();
        metadata.clear_values();
        metadata.clear_raw_payloads();
        metadata.clear_raw_requests();
        metadata.clear_raw_packet_ids();
        metadata.clear_part_index();
        const auto signature = metadata.SerializeAsString();
        const auto wire = record.SerializeAsString();
        const auto index = std::to_string(record.part_index());
        const auto count = std::to_string(record.part_count());
        const auto key = stored.key();
        const std::string_view keys[]{key};
        const std::string_view args[]{signature, index, count, wire};
        const auto reply = co_await redis.eval(persistence::kStoreTelemetryPartScript, keys, args);
        if (reply.kind() == ruvia::redis_value::kind_type::integer && reply.integer() == 0)
            co_return std::nullopt;
        if (reply.kind() != ruvia::redis_value::kind_type::array)
            service::message::redis::throwValue("store telemetry upload", reply);
        std::vector<pb::TelemetryRecord> parts;
        for (const auto& item : reply.array()) {
            if (item.kind() != ruvia::redis_value::kind_type::string)
                throw std::runtime_error("invalid stored telemetry part");
            auto& part = parts.emplace_back();
            if (!part.ParseFromString(std::string(item.string())))
                throw std::runtime_error("corrupt stored telemetry part");
        }
        co_return assembleTelemetryParts(parts);
    }

    template <typename Redis>
    static ruvia::task<void> finishTelemetryUploads(
        const Redis& redis, const std::vector<persistence::TelemetryUploadRecord>& uploads) {
        for (const auto& upload : uploads) {
            const auto key = upload.key();
            const std::string_view keys[]{key};
            const auto reply = co_await redis.eval(persistence::kFinishTelemetryUploadScript, keys,
                                                   std::span<const std::string_view>{});
            if (reply.kind() != ruvia::redis_value::kind_type::integer || reply.integer() != 1)
                service::message::redis::throwValue("finish telemetry upload", reply);
        }
    }

    static void collectTelemetry(const metadata::Catalog& catalog,
                          std::string_view nodeId, std::int64_t receivedAtMs,
                          const pb::TelemetryBatch& batch,
                          std::vector<service::message::ParsedDeviceMessage>& messages,
                          const std::vector<pb::ConfigItem>& parsingItems = {}) {
        const auto node = catalog.find(std::string(nodeId));
        if (node == catalog.end())
            return;
        for (const auto& record : batch.records()) {
            if (record.record_id().size() != 16 || record.device_id().size() != 16)
                continue;
            const auto deviceId = protocol::uuidText(record.device_id());
            const auto device = node->second.find(deviceId);
            if (device == node->second.end())
                continue;
            message::ParsedDeviceMessage parsed;
            if (record.model_id().size() == 16) {
                parsed.modelId = protocol::uuidText(record.model_id());
            } else if (!record.model_id().empty()) {
                throw std::runtime_error("invalid edge telemetry model reference");
            }
            parsed.messageId = protocol::uuidText(record.record_id());
            parsed.acquisitionId = parsed.messageId;
            parsed.causationId = parsed.messageId;
            parsed.linkId = device->second.linkId;
            parsed.deviceId = deviceId;
            parsed.deviceCode = device->second.deviceCode;
            parsed.protocol = protocol::TelemetryValues::protocolName(record.protocol());
            if (parsed.protocol.empty())
                parsed.protocol = device->second.protocol;
            parsed.connectionId = std::string(nodeId);
            parsed.occurredAtMs = receivedAtMs;
            parsed.observedAtMs = record.observed_at_ms();
            parsed.storagePolicy = device->second.storagePolicy;
            parsed.onlineWindowMs = device->second.onlineWindowMs;
            parsed.source = record.derived_update() ? "derived" : "edge";
            if (record.raw_requests_size() != 0) {
                const auto configured = acquisition::RawAcquisition::device(parsingItems, record.device_id());
                if (configured.linkId != device->second.linkId ||
                    (record.endpoint_id().size() == 16 && protocol::uuidText(record.endpoint_id()) != configured.linkId))
                    throw std::runtime_error("edge acquisition endpoint mismatch");
                const auto decoded = acquisition::RawAcquisition::decode(record, configured);
                parsed.valuesJson = decoded.valuesJson;
                parsed.observedAtMs = decoded.observedAtMs;
            } else parsed.valuesJson = protocol::TelemetryValues::telemetryJson(record);
            if (record.raw_packet_ids_size() != 0 &&
                record.raw_packet_ids_size() != record.raw_payloads_size())
                throw std::runtime_error("original packet ID count mismatch");
            for (int index = 0; index < record.raw_payloads_size(); ++index) {
                std::string packetId;
                if (record.raw_packet_ids_size()) {
                    const auto& id = record.raw_packet_ids(index);
                    if (id.size() != 16) throw std::runtime_error("invalid original packet ID");
                    packetId = std::string(nodeId) + ':' + protocol::uuidText(id);
                } else packetId = std::string(nodeId) + ":legacy-history:" + parsed.messageId + ':' + std::to_string(index);
                if (record.raw_requests_size() && !record.raw_requests(index).empty()) {
                    const auto& request = record.raw_requests(index);
                    parsed.rawPayloads.emplace_back(request.begin(), request.end());
                    parsed.rawPacketIds.push_back(packetId + ":request");
                }
                const auto& response = record.raw_payloads(index);
                parsed.rawPayloads.emplace_back(response.begin(), response.end());
                parsed.rawPacketIds.push_back(std::move(packetId));
            }
            messages.push_back(std::move(parsed));
        }
    }

  private:
    static ruvia::task<void> saveCommandResult(ruvia::web_worker_context& context,
                                        const metadata::Catalog& catalog,
                                        std::string_view nodeId,
                                        std::int64_t receivedAtMs,
                                        const pb::CommandResult& result) {
        if (result.command_id().size() != 16 || result.device_id().size() != 16)
            co_return;
        if (!protocol::persistableCommandResult(result.state(), result.write_ack_missing()))
            co_return;
        const auto commandId = protocol::uuidText(result.command_id());
        const auto deviceId = protocol::uuidText(result.device_id());
        const auto node = catalog.find(std::string(nodeId));
        if (node == catalog.end())
            co_return;
        const auto device = node->second.find(deviceId);
        if (device == node->second.end())
            co_return;
        const auto domainState = protocol::commandResultDomainState(
            result.state(), result.write_ack_missing(), result.message());
        const bool success = domainState == "SUCCEEDED";
        const std::string state(domainState);
        std::string reason = result.message();
        if (result.write_ack_missing()) {
            reason = "write_ack_missing";
            if (!result.message().empty())
                reason += ": " + result.message();
        }
        const auto completedAtMs = message::effectiveObservedAt(
            result.completed_at_ms(), receivedAtMs);
        std::vector<message::StreamField> fields{
            {"message_id", context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next()},
            {"causation_id", commandId},
            {"command_id", commandId},
            {"device_id", deviceId},
            {"device_code", device->second.deviceCode},
            {"protocol", device->second.protocol},
            {"attempt", "1"},
            {"success", success ? "1" : "0"},
            {"result_state", state},
            {"reason", reason},
            {"worker_id", "0"},
            {"created_at_ms", std::to_string(message::utcNowMilliseconds())},
            {"completed_at_ms", std::to_string(completedAtMs)},
            {"actual_value_count", std::to_string(result.actual_values_size())}};
        for (int index = 0; index < result.actual_values_size(); ++index) {
            const auto& actual = result.actual_values(index);
            const auto prefix = "actual_value_" + std::to_string(index) + "_";
            fields.push_back({prefix + "element_id", actual.element_id()});
            fields.push_back({prefix + "name", actual.name()});
            fields.push_back({prefix + "kind",
                              actual.has_value() ? protocol::TelemetryValues::scalarKind(actual.value()) : "UNSPECIFIED"});
            fields.push_back({prefix + "value",
                              actual.has_value() ? protocol::TelemetryValues::scalarText(actual.value()) : std::string{}});
            fields.push_back({prefix + "unit", actual.unit()});
        }
        (void)co_await message::redis::publishAndWake(
            context.redis(), message::commandResultStream(), fields,
            std::nullopt,
            service::message::WorkerStreamTask::CommandResult, 10000);
    }

    static ruvia::task<void> saveDeviceStatus(ruvia::web_worker_context& context,
                                               std::string_view nodeId,
                                               const pb::DeviceStatusReport& report) {
        bool updated = false;
        for (const auto& status : report.devices()) {
            if (status.device_id().size() != 16)
                continue;
            const auto deviceId = protocol::uuidText(status.device_id());
            std::string clients;
            for (const auto& client : status.clients()) {
                if (!clients.empty())
                    clients.push_back(',');
                clients += client;
            }
            const auto key = service::message::edge::deviceStatusKey(nodeId, deviceId);
            co_await service::message::redis::eraseHash(context.redis(), key);
            const std::vector<message::StreamField> fields{
                 {"node_id", std::string(nodeId)},
                 {"device_id", deviceId},
                 {"state", status.state()},
                 {"reason", status.reason()},
                 {"client_count", std::to_string(status.client_count())},
                 {"clients", clients},
                 {"last_activity_at_ms", std::to_string(status.last_activity_at_ms())},
                 {"updated_at_ms", std::to_string(service::message::utcNowMilliseconds())}
            };
            co_await service::message::redis::setHash(context.redis(), key, fields);
            const std::vector<std::string> pexpireArguments{"PEXPIRE", key, "900000"};
            (void)co_await service::message::redis::command(
                context.redis(), pexpireArguments);
            updated = true;
        }
        if (updated)
            co_await service::telemetry::latest::publishRealtimeChange(context.redis());
    }

    // Appends a request to this Worker's own RPC Stream for another feature on
    // the same Worker. Nobody waits for the reply, so failures are logged here
    // and the projection of the source message still completes.
    static ruvia::task<void> notifyFeature(ruvia::web_worker_context& context, std::size_t workerIndex,
                                           std::string_view component, std::string_view operation,
                                           std::string payload) {
        using Contract = service::message::rpc::Contract;
        try {
            const auto id = context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
            const auto deadline =
                Contract::now() + std::chrono::duration_cast<std::chrono::milliseconds>(Contract::timeout).count();
            const auto arguments = Contract::requestArguments(
                Contract::requests(service::runtime::instanceId(), workerIndex), id, component, operation,
                std::move(payload), deadline);
            const auto reply = co_await service::message::redis::command(context.redis(), arguments);
            if (reply.kind() != ruvia::redis_value::kind_type::string)
                service::message::redis::throwValue("edge feature notification", reply);
        } catch (const std::exception& error) {
            LOG_ERROR << "edge " << component << '.' << operation << " notification failed: " << error.what();
        }
    }

    // A firmware task reached a terminal state: its image may expire in an hour.
    static ruvia::task<void> scheduleFirmwareCleanup(ruvia::web_worker_context& context, std::size_t workerIndex) {
        co_await notifyFeature(context, workerIndex, service::message::edge::firmware::kRpcComponent,
                               service::message::edge::firmware::kScheduleCleanupOperation, std::string{});
    }
};

} // namespace service::edge::projection
