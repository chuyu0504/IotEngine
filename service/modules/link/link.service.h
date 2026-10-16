#pragma once

#include <tuple>
#include <ruvia/web/validation.h>
#include <system_error>
#include <asio/ip/address_v4.hpp>
#include <asio/ip/address.hpp>

#include <memory>
#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/web/http_client_handle.h>
#include <ruvia/web/http_client_response.h>
#include <ruvia/web/model_json.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/modules/edge_node/edge_node.service.h"
#include "service/modules/link/link.entity.h"
#include "service/modules/link/link.types.h"
#include "service/modules/system/outbox/outbox.service.h"
#include "service/modules/system/role/role.entity.h"
#include "service/modules/system/user/user.entity.h"
#include "service/utils/number.h"
#include "service/utils/payload.h"
#include "service/utils/debug_idle.h"
#include "service/utils/redis.h"

namespace service::link {

class LinkService {
  public:
    static LinkService& instance() {
        static thread_local LinkService service;
        return service;
    }

    template <typename Context>
    ruvia::task<LinkPageDataDto> list(Context& c, std::int64_t page, std::int64_t pageSize, std::optional<std::string> keyword, std::optional<std::string> mode, std::optional<std::string> protocol, std::optional<std::string> status) {
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 100);
        ruvia::db_query count(c.pool());
        count.select(count.aggregate("count", { count.star() })).from(LinkEntity::table_name());
        applyFilters(count, keyword, mode, protocol, status);
        const auto countRows = co_await c.db().query(count);
        const ruvia::int64 total = countRows.empty()
            ? ruvia::int64{ 0 }
            : toInt(countRows.front()[0].value().value_or(std::string_view{}));

        auto query = linkSelect(c.pool());
        applyFilters(query, keyword, mode, protocol, status);
        query.order_by((LinkEntity::column<"execution">() == "edge").expression(query), ruvia::db_order_direction::desc)
            .add_order_by(query.column("id"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>((page - 1) * pageSize));
        const auto rows = co_await c.db().query(query);

        ruvia::boxed_array<LinkItemDto> links(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = links.emplace(ruvia::model_options{ .resource_ = c.arena() });
            co_await fill(c, item, row);
        }
        LinkPageDataDto result(ruvia::model_options{ .resource_ = c.arena() });
        result.set<"list">(std::move(links))
            .template set<"total">(total)
            .template set<"page">(page)
            .template set<"pageSize">(pageSize)
            .template set<"totalPages">(total == 0 ? 0 : (total + pageSize - 1) / pageSize);
        co_return result;
    }

    template <typename Context>
    ruvia::task<LinkItemDto> detail(Context& c, std::string_view id) {
        auto query = linkSelect(c.pool());
        query.where((LinkEntity::column<"id">() == id &&
                     LinkEntity::column<"deleted_at">().is_null())
                        .expression(query))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(15001, "链路不存在", 404);
        }
        LinkItemDto item(ruvia::model_options{ .resource_ = c.arena() });
        co_await fill(c, item, rows.front());
        co_return item;
    }

    template <typename Context>
    LinkEnumsDto enums(Context& c) {
        LinkEnumsDto result(ruvia::model_options{ .resource_ = c.arena() });
        const auto modelOptions = ruvia::model_options{ .resource_ = c.arena() };
        ruvia::boxed_array<ruvia::string> modes(modelOptions);
        modes.emplace("TCP Server", modelOptions);
        modes.emplace("TCP Client", modelOptions);
        ruvia::boxed_array<ruvia::string> protocols(modelOptions);
        protocols.emplace("SL651", modelOptions);
        protocols.emplace("Modbus", modelOptions);
        protocols.emplace("S7", modelOptions);
        protocols.emplace("MC", modelOptions);
        protocols.emplace("FINS", modelOptions);
        protocols.emplace("DLT645", modelOptions);
        protocols.emplace("MQTT", modelOptions);
        ruvia::boxed_array<ruvia::string> statuses(modelOptions);
        statuses.emplace("enabled", modelOptions);
        statuses.emplace("disabled", modelOptions);
        result.set<"modes">(std::move(modes))
            .template set<"protocols">(std::move(protocols))
            .template set<"statuses">(std::move(statuses));
        return result;
    }

    // 客户端、请求和缓存均由接入请求的 Service Worker 持有。
    template <typename Context>
    ruvia::task<std::string> publicIp(Context& c) {
        const auto now = std::chrono::steady_clock::now();
        if (!cachedPublicIp_.empty() && now - publicIpCachedAt_ < std::chrono::minutes(5)) {
            co_return cachedPublicIp_;
        }
        try {
            const std::array<ruvia::http_header_view, 1> headers{ { { "Accept", "text/plain" } } };
            auto response = co_await c.get_http_client("link-public-ip").send({ .headers_ = headers });
            if (response.status().value() == 200) {
                const auto body = co_await response.body().read_all(64U * 1024U);
                const auto resolved = parsePublicIp(std::string_view(reinterpret_cast<const char*>(body.bytes().data()), body.size()));
                if (!resolved.empty()) {
                    cachedPublicIp_ = resolved;
                    publicIpCachedAt_ = std::chrono::steady_clock::now();
                }
            }
        } catch (const ruvia::http_client_error&) {
            // 查询失败保留旧值；首次查询失败仍返回空字符串。
        }
        co_return cachedPublicIp_;
    }

    template <typename Context>
    ruvia::task<void> create(Context& c, const SaveLinkBody& body) {
        if (body.get<"execution">() && body.get<"execution">()->view() == "edge") {
            service::common::fail(15002, "边缘连接参数请在设备表单中配置", 400);
        }
        const auto name = std::string(body.get<"name">().view());
        const auto protocol = std::string(body.get<"protocol">().view());
        const auto& endpoint = body.get<"endpoint">();
        const auto mode = required(endpoint.get<"mode">(), "链路模式不能为空");
        const auto ip = endpoint.get<"ip">() ? std::string(endpoint.get<"ip">()->view()) : "";
        const auto port = endpoint.get<"port">() ? static_cast<std::int64_t>(*endpoint.get<"port">()) : 0;
        const auto status = body.get<"status">() ? std::string(body.get<"status">()->view()) : "enabled";
        validateStatus(status);
        const auto& targets = requiredTargets(endpoint);
        validateConfiguration(mode, protocol, ip, port, targets);
        co_await ensureAvailable(c, name, mode, ip, port, std::nullopt);
        const auto endpointJson = serializeEndpoint(mode, ip, port, targets);
        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        auto transaction = co_await c.db().begin_transaction();
        LinkEntity link(c.pool());
        link.set<"id">(id);
        link.set<"name">(name);
        link.set<"protocol">(protocol);
        link.set<"endpoint">(endpointJson);
        link.set<"status">(status);
        link.set<"created_by">(c.userId);
        link.set<"execution">("collector");
        (void)co_await transaction.template get_repository<LinkEntity>().insert(link);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "created", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<LinkDebugAcquisitionDto>> debugPackets(Context& c, std::string_view id) {
        const auto link = co_await detail(c, id);
        if (link.template get<"execution">() && link.template get<"execution">()->view() == "edge")
            co_await requireOwner(c, link.template get<"createdBy">()->view());
        auto leaseTransaction = co_await c.db().begin_transaction();
        ruvia::db_query lease(c.pool());
        lease.select(lease.column(std::tuple_element_t<LinkEntity::column_index<"debug_enabled">(), LinkEntity::columns_type>::name.view()))
            .from(LinkEntity::table_name())
            .where((LinkEntity::column<"id">() == id && LinkEntity::column<"deleted_at">().is_null()).expression(lease))
            .lock(ruvia::db_lock_options{});
        const auto leaseRows = co_await leaseTransaction.query(lease);
        if (!leaseRows.empty() && leaseRows.front()[0].value().value_or("") == "t")
            co_await service::debug_idle::touch(c.redis(), "link", id);
        co_await leaseTransaction.commit();
        // 有界 ZSET 索引与报文 Hash 的一致性读取需要 Lua，ORM 无原子跨键读取接口。
        const auto key = service::message::packet_log::linkIndexKey(id);
        static constexpr std::string_view readPackets = R"lua(
local result={}
local count=0
for _,acquisition in ipairs(redis.call('ZREVRANGE',KEYS[1],0,99)) do
    local round='iot:debug:v4:acquisition:'..acquisition
    local metadata=redis.call('HGETALL',round)
    if #metadata==0 then
        redis.call('ZREM',KEYS[1],acquisition)
    else
        local ids=redis.call('ZRANGE',round..':packets',0,-1)
        if count+#ids>4096 then break end
        local packets={}
        for _,id in ipairs(ids) do
            local fields=redis.call('HGETALL','iot:debug:v4:packet:'..id)
            if #fields>0 then packets[#packets+1]={id,fields} end
        end
        result[#result+1]={acquisition,metadata,packets}
        count=count+#packets
    end
end
return result
)lua";
        const std::vector<std::string_view> keys{ key };
        const std::vector<std::string_view> args;
        const auto reply = co_await c.redis().eval(readPackets, keys, args);
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            service::message::redis::throwValue("read debug packets", reply);
        }
        ruvia::boxed_array<LinkDebugAcquisitionDto> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : reply.array()) {
            if (row.kind() != ruvia::redis_value::kind_type::array || row.array().size() != 3) {
                continue;
            }
            auto& acquisition = result.emplace(ruvia::model_options{ .resource_ = c.arena() });
            acquisition.set<"id">(row.array()[0].string());
            const auto metadata = row.array()[1].array();
            for (std::size_t index = 0; index + 1 < metadata.size(); index += 2) {
                const auto field = metadata[index].string();
                const auto value = metadata[index + 1].string();
                if (field == "started_at_ms") {
                    acquisition.set<"startedAtMs">(value);
                }
                if (field == "finished_at_ms") {
                    acquisition.set<"finishedAtMs">(value);
                }
                if (field == "last_packet_at_ms") {
                    acquisition.set<"lastPacketAtMs">(value);
                }
                if (field == "state") {
                    acquisition.set<"state">(value);
                }
                if (field == "device_id") {
                    acquisition.set<"deviceId">(value);
                }
            }
            ruvia::boxed_array<LinkDebugPacketDto> packets(ruvia::model_options{ .resource_ = c.arena() });
            for (const auto& packetRow : row.array()[2].array()) {
                const auto fields = packetRow.array()[1].array();
                std::string eventId(packetRow.array()[0].string());
                auto& packet = packets.emplace(ruvia::model_options{ .resource_ = c.arena() });
                packet.set<"id">(eventId);
                for (std::size_t index = 0; index + 1 < fields.size(); index += 2) {
                    const auto name = fields[index].string();
                    const auto value = fields[index + 1].string();
                    if (name == "acquisition_id") {
                        packet.set<"acquisitionId">(value);
                    } else if (name == "device_id") {
                        packet.set<"deviceId">(value);
                    } else if (name == "direction") {
                        packet.set<"direction">(value);
                    } else if (name == "source") {
                        packet.set<"source">(value);
                    } else if (name == "address") {
                        packet.set<"address">(value);
                    } else if (name == "edge_node_id") {
                        packet.set<"edgeNodeId">(value);
                    } else if (name == "edge_node_name") {
                        packet.set<"edgeNodeName">(value);
                    } else if (name == "payload_hex") {
                        packet.set<"payloadHex">(value);
                    } else if (name == "time_ms") {
                        packet.set<"timeMs">(value);
                    } else if (name == "transport_status") {
                        packet.set<"transportStatus">(value);
                    } else if (name == "response_status") {
                        packet.set<"responseStatus">(value);
                    } else if (name == "parse_status") {
                        packet.set<"parseStatus">(value);
                    } else if (name == "revision") {
                        packet.set<"revision">(value);
                    } else if (name == "reply_to_packet_id") {
                        packet.set<"replyToPacketId">(value);
                    } else if (name == "reason") {
                        packet.set<"reason">(value);
                    } else if (name == "parsed_json") {
                        packet.set<"parsedJson">(value);
                    }
                }
            }
            acquisition.set<"packets">(std::move(packets));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> setDebug(Context& c, std::string_view id, bool enabled) {
        const auto link = co_await detail(c, id);
        const auto edgeNodeId = link.template get<"edgeNodeId">()
            ? std::string(link.template get<"edgeNodeId">()->view()) : std::string{};
        if (link.template get<"execution">() && link.template get<"execution">()->view() == "edge") {
            co_await requireOwner(c, link.template get<"createdBy">()->view());
            if (enabled)
                co_await requireEdgeDebugNode(c, edgeNodeId);
        }
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query current(c.pool());
        current.select(current.column(std::tuple_element_t<LinkEntity::column_index<"debug_enabled">(), LinkEntity::columns_type>::name.view()))
            .from(LinkEntity::table_name())
            .where((LinkEntity::column<"id">() == id && LinkEntity::column<"deleted_at">().is_null()).expression(current))
            .lock(ruvia::db_lock_options{});
        const auto currentRows = co_await transaction.query(current);
        if (currentRows.empty())
            service::common::fail(15001, "链路不存在", 404);
        const bool changed = (currentRows.front()[0].value().value_or("") == "t") != enabled;
        ruvia::db_query query(c.pool());
        query.update(LinkEntity::table_name())
            .set(std::tuple_element_t<LinkEntity::column_index<"debug_enabled">(), LinkEntity::columns_type>::name.view(), query.value(enabled))
            .set("updated_at", query.call("now"))
            .where((LinkEntity::column<"id">() == id && LinkEntity::column<"deleted_at">().is_null()).expression(query))
            .and_where(query.binary(query.column(std::tuple_element_t<LinkEntity::column_index<"debug_enabled">(), LinkEntity::columns_type>::name.view()), ruvia::db_binary_operator::is_distinct_from, query.value(enabled)))
            .returning({query.column(std::tuple_element_t<LinkEntity::column_index<"id">(), LinkEntity::columns_type>::name.view())});
        if (changed) {
            (void)co_await transaction.query(query);
            co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "updated", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        }
        if (enabled) {
            co_await service::debug_idle::touch(c.redis(), "link", id);
        } else {
            co_await service::debug_idle::clear(c.redis(), "link", id);
        }
        co_await transaction.commit();
        if (enabled) {
            service::debug_idle::watch(
                c.template worker_state<ruvia::web_worker_handle>(),
                "link",
                std::string(id),
                [](ruvia::web_worker_context& ctx, const std::string& linkId) -> ruvia::task<void> {
                    co_await LinkService::instance().disableExpiredDebug(ctx, linkId);
                });
        }
        if (changed && !edgeNodeId.empty() && (enabled || co_await edgeDebugNodeAvailable(c, edgeNodeId))) {
            try { (void)co_await service::edge::EdgeService::queueSnapshot(c, edgeNodeId, c.userId); }
            catch (...) { service::common::fail(15002, "调试设置已保存，下发未成功，请从边缘节点同步配置重试", 502); }
        }
    }

    template <typename Context>
    ruvia::task<void> disableExpiredDebug(Context& c, std::string_view id) {
        if (co_await service::debug_idle::leased(c.redis(), "link", id)) {
            co_return;
        }
        ruvia::db_query lookup(c.pool());
        lookup.select({ lookup.column("debug_enabled"), lookup.column("created_by"),
                        lookup.coalesce({lookup.cast(lookup.column("edge_node_id"), ruvia::db_data_type::text), lookup.value("")}) })
            .from(LinkEntity::table_name())
            .where((LinkEntity::column<"id">() == id && LinkEntity::column<"deleted_at">().is_null()).expression(lookup));
        const auto rows = co_await c.db().query(lookup);
        if (rows.empty() || rows.front()[0].value().value_or("") != "t") {
            co_return;
        }
        const auto actor = std::string(rows.front()[1].value().value_or(""));
        const auto edgeNodeId = std::string(rows.front()[2].value().value_or(""));
        auto transaction = co_await c.db().begin_transaction();
        lookup.lock(ruvia::db_lock_options{});
        const auto current = co_await transaction.query(lookup);
        if (current.empty() || current.front()[0].value().value_or("") != "t") {
            co_await transaction.commit();
            co_return;
        }
        if (co_await service::debug_idle::leased(c.redis(), "link", id)) {
            co_await transaction.commit();
            co_return;
        }
        ruvia::db_query query(c.pool());
        query.update(LinkEntity::table_name())
            .set(std::tuple_element_t<LinkEntity::column_index<"debug_enabled">(), LinkEntity::columns_type>::name.view(), query.value(false))
            .set("updated_at", query.call("now"))
            .where((LinkEntity::column<"id">() == id && LinkEntity::column<"deleted_at">().is_null()).expression(query))
            .and_where(query.binary(query.column(std::tuple_element_t<LinkEntity::column_index<"debug_enabled">(), LinkEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, query.value(true)))
            .returning({query.column(std::tuple_element_t<LinkEntity::column_index<"id">(), LinkEntity::columns_type>::name.view())});
        const auto queryRows = co_await transaction.query(query);
        if (queryRows.empty()) {
            co_await transaction.commit();
            co_return;
        }
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "updated", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
        if (!edgeNodeId.empty() && service::common::isUuid(actor) && co_await edgeDebugNodeAvailable(c, edgeNodeId))
            (void)co_await service::edge::EdgeService::queueSnapshot(c, edgeNodeId, actor);
    }

    template <typename Context>
    ruvia::task<void> update(Context& c, std::string_view id, const SaveLinkBody& body) {
        if (body.get<"execution">() && body.get<"execution">()->view() == "edge") {
            service::common::fail(15002, "边缘连接参数请在设备表单中配置", 400);
        }
        ruvia::db_query lookup(c.pool());
        lookup
            .select({ jsonText(lookup, "endpoint", "mode"), lookup.column("protocol"), lookup.column("created_by") })
            .from(LinkEntity::table_name())
            .where((LinkEntity::column<"id">() == id &&
                    LinkEntity::column<"deleted_at">().is_null() &&
                    LinkEntity::column<"execution">() == "collector")
                       .expression(lookup))
            .limit(1);
        const auto rows = co_await c.db().query(lookup);
        if (rows.empty()) {
            service::common::fail(15001, "链路不存在", 404);
        }
        co_await requireOwner(c, rows.front()[2].value().value_or(std::string_view{}));

        const auto name = std::string(body.get<"name">().view());
        const auto protocol = std::string(body.get<"protocol">().view());
        const auto& endpoint = body.get<"endpoint">();
        const auto mode = required(endpoint.get<"mode">(), "链路模式不能为空");
        if (mode != rows.front()[0].value().value_or(std::string_view{}) || protocol != rows.front()[1].value().value_or(std::string_view{})) {
            service::common::fail(15006, "链路模式和协议创建后不能修改", 400);
        }
        const auto ip = endpoint.get<"ip">() ? std::string(endpoint.get<"ip">()->view()) : "";
        const auto port = endpoint.get<"port">() ? static_cast<std::int64_t>(*endpoint.get<"port">()) : 0;
        const auto status = body.get<"status">() ? std::string(body.get<"status">()->view()) : "enabled";
        validateStatus(status);
        const auto& targets = requiredTargets(endpoint);
        validateConfiguration(mode, protocol, ip, port, targets);
        co_await ensureAvailable(c, name, mode, ip, port, std::string(id));
        const auto endpointJson = serializeEndpoint(mode, ip, port, targets);
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query query(c.pool());
        query.update(LinkEntity::table_name())
            .set("name", query.value(name))
            .set("endpoint", query.cast(query.value(endpointJson), ruvia::db_data_type::jsonb))
            .set("status", query.value(status))
            .set("updated_at", query.call("now"))
            .where((LinkEntity::column<"id">() == id &&
                    LinkEntity::column<"deleted_at">().is_null() &&
                    LinkEntity::column<"execution">() == "collector")
                       .expression(query));
        const auto nameChanged = query.binary(query.column("name"), ruvia::db_binary_operator::is_distinct_from, query.value(name));
        const auto endpointChanged = query.binary(
            query.column("endpoint"),
            ruvia::db_binary_operator::is_distinct_from,
            query.cast(query.value(endpointJson), ruvia::db_data_type::jsonb)
        );
        const auto statusChanged = query.binary(query.column("status"), ruvia::db_binary_operator::is_distinct_from, query.value(status));
        query.and_where(query.binary(query.binary(nameChanged, ruvia::db_binary_operator::or_value, endpointChanged), ruvia::db_binary_operator::or_value, statusChanged));
        const auto updated = co_await transaction.execute(query);
        if (updated.affected_rows() != 0) {
            co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "updated", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        }
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> remove(Context& c, std::string_view id) {
        ruvia::db_find_options options;
        options.where_ = LinkEntity::column<"id">() == id && LinkEntity::column<"deleted_at">().is_null();
        const auto link = co_await c.db().template get_repository<LinkEntity>().find_one(options);
        if (!link) {
            service::common::fail(15001, "链路不存在", 404);
        }
        if (link->template get<"execution">() == "edge") {
            service::common::fail(15002, "边缘连接由设备配置管理，不能单独删除", 400);
        }
        co_await requireOwner(c, link->template get<"created_by">());
        ruvia::db_find_options used;
        used.where_ = entities::DeviceEntity::column<"link_id">() == id &&
            entities::DeviceEntity::column<"deleted_at">().is_null();
        if (co_await c.db().template get_repository<entities::DeviceEntity>().exists(used)) {
            service::common::fail(15008, "链路已被设备使用，请先删除关联设备", 409);
        }
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query expressions(c.pool());
        const ruvia::db_assignment deletionChanges[]{
            { "deleted_at", expressions.call("now") }, { "updated_at", expressions.call("now") }
        };
        (void)co_await transaction.template get_repository<LinkEntity>().update(
            LinkEntity::column<"id">() == id && LinkEntity::column<"deleted_at">().is_null(),
            deletionChanges
        );
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "deleted", id, c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next());
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<std::string> configureDeviceEndpoint(Context& c, ruvia::db_transaction& tx, const SaveLinkBody& body, std::string_view deviceId = {}) {
        const auto name = std::string(body.get<"name">().view());
        const auto protocol = std::string(body.get<"protocol">().view());
        const auto nodeId = required(body.get<"edgeNodeId">(), "请选择边缘节点");
        validateNodeId(nodeId);
        const auto& endpoint = body.get<"endpoint">();
        const auto transport = required(endpoint.get<"transport">(), "请选择传输类型");
        const auto interfaceName = required(endpoint.get<"interfaceName">(), "请选择接口");
        validateEdgeEndpoint(endpoint, protocol, transport, interfaceName);
        ruvia::db_query nodeQuery(c.pool());
        nodeQuery.select(nodeQuery.cast(nodeQuery.value(1), ruvia::db_data_type::integer))
            .from(service::link::entities::EdgeNodeEntity::table_name())
            .where(nodeQuery.binary(nodeQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeEntity::column_index<"id">(), service::link::entities::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, nodeQuery.cast(nodeQuery.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(nodeQuery.binary(nodeQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeEntity::column_index<"enrollment_status">(), service::link::entities::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, nodeQuery.value("approved")))
            .and_where(nodeQuery.binary(jsonText(nodeQuery, "capability", "deviceConfig"), ruvia::db_binary_operator::equal, nodeQuery.value("true")));
        if (protocol == "MC" || protocol == "FINS" || protocol == "DLT645") {
            nodeQuery.and_where(nodeQuery.call("jsonb_exists", { nodeQuery.binary(nodeQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeEntity::column_index<"capability">(), service::link::entities::EdgeNodeEntity::columns_type>::name.view()), ruvia::db_binary_operator::json_get, nodeQuery.value("protocols")), nodeQuery.value(protocol) }));
        }
        const auto node = co_await tx.query(nodeQuery);
        if (node.empty()) {
            service::common::fail(15002, "节点未批准或不支持采集配置", 400);
        }
        if (transport == "serial") {
            ruvia::db_query serialQuery(c.pool());
            serialQuery.select(serialQuery.cast(serialQuery.value(1), ruvia::db_data_type::integer))
                .from(service::link::entities::EdgeNodeSerialEntity::table_name())
                .where(serialQuery.binary(serialQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeSerialEntity::column_index<"node_id">(), service::link::entities::EdgeNodeSerialEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, serialQuery.cast(serialQuery.value(nodeId), ruvia::db_data_type::uuid)))
                .and_where(serialQuery.binary(serialQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeSerialEntity::column_index<"path">(), service::link::entities::EdgeNodeSerialEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, serialQuery.value(interfaceName)))
                .and_where(serialQuery.unary(ruvia::db_unary_operator::is_true, serialQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeSerialEntity::column_index<"available">(), service::link::entities::EdgeNodeSerialEntity::columns_type>::name.view())));
            const auto serial = co_await tx.query(serialQuery);
            if (serial.empty()) {
                service::common::fail(15002, "所选串口不存在或当前不可用", 409);
            }
        } else {
            ruvia::db_query networkQuery(c.pool());
            networkQuery.select(networkQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeInterfaceEntity::column_index<"ipv4">(), service::link::entities::EdgeNodeInterfaceEntity::columns_type>::name.view()))
                .from(service::link::entities::EdgeNodeInterfaceEntity::table_name())
                .where(networkQuery.binary(networkQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeInterfaceEntity::column_index<"node_id">(), service::link::entities::EdgeNodeInterfaceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, networkQuery.cast(networkQuery.value(nodeId), ruvia::db_data_type::uuid)))
                .and_where(networkQuery.binary(networkQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeInterfaceEntity::column_index<"name">(), service::link::entities::EdgeNodeInterfaceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal, networkQuery.value(interfaceName)))
                .and_where(networkQuery.binary(
                    networkQuery.coalesce({ networkQuery.column(std::tuple_element_t<service::link::entities::EdgeNodeInterfaceEntity::column_index<"ipv4">(), service::link::entities::EdgeNodeInterfaceEntity::columns_type>::name.view()), networkQuery.value("") }),
                    ruvia::db_binary_operator::not_equal,
                    networkQuery.value("")
                ))
                .limit(1);
            const auto network = co_await tx.query(networkQuery);
            if (network.empty()) {
                service::common::fail(15002, "所选网口不存在或未上报 IPv4", 409);
            }
            const auto mode = endpoint.get<"mode">()->view();
            const auto ip = endpoint.get<"ip">()->view();
            if ((protocol == "S7" && mode != "TCP Client") || (protocol == "SL651" && mode != "TCP Server")) {
                service::common::fail(15002, "协议不支持所选 TCP 模式", 400);
            }
            if (mode == "TCP Server" && ip != "0.0.0.0" && ip != network.front()[0].value().value_or("")) {
                service::common::fail(15002, "监听地址必须是所选网口地址", 400);
            }
        }
        std::string endpointJson = "{\"transport\":";
        appendJsonString(endpointJson, transport);
        endpointJson += ",\"interface\":";
        appendJsonString(endpointJson, interfaceName);
        if (transport == "serial") {
            endpointJson += ",\"baud_rate\":" + std::to_string(endpoint.get<"baudRate">().value_or(9600));
            endpointJson += ",\"data_bits\":" + std::to_string(endpoint.get<"dataBits">().value_or(8));
            endpointJson += ",\"stop_bits\":" + std::to_string(endpoint.get<"stopBits">().value_or(1));
            endpointJson += ",\"parity\":";
            appendJsonString(endpointJson, endpoint.get<"parity">() ? endpoint.get<"parity">()->view() : std::string_view("none"));
            endpointJson += endpoint.get<"rs485">().value_or(false) ? ",\"rs485\":true" : ",\"rs485\":false";
        } else {
            endpointJson += ",\"mode\":";
            appendJsonString(endpointJson, endpoint.get<"mode">()->view());
            endpointJson += ",\"ip\":";
            appendJsonString(endpointJson, endpoint.get<"ip">()->view());
            endpointJson += ",\"port\":" + std::to_string(*endpoint.get<"port">());
        }
        endpointJson += '}';
        // Serialize endpoint resolution within a node; reuse the same physical endpoint.
        ruvia::db_query nodeLock(c.pool());
        nodeLock.select(nodeLock.column("id")).from(service::link::entities::EdgeNodeEntity::table_name())
            .where(nodeLock.binary(nodeLock.column("id"), ruvia::db_binary_operator::equal, nodeLock.cast(nodeLock.value(nodeId), ruvia::db_data_type::uuid)));
        nodeLock.lock(ruvia::db_lock_options{});
        (void)co_await tx.query(nodeLock);
        ruvia::db_query existing(c.pool());
        auto sameSettings = (LinkEntity::column<"protocol">() == protocol).expression(existing);
        auto compareSetting = [&](std::string_view key, std::string_view value, std::string_view fallback = "") {
            sameSettings = existing.binary(sameSettings, ruvia::db_binary_operator::and_value,
                existing.binary(existing.coalesce({ jsonText(existing, "endpoint", key), existing.value(fallback) }),
                    ruvia::db_binary_operator::equal, existing.value(value)));
        };
        if (transport == "serial") {
            compareSetting("baud_rate", std::to_string(endpoint.get<"baudRate">().value_or(9600)), "9600");
            compareSetting("data_bits", std::to_string(endpoint.get<"dataBits">().value_or(8)), "8");
            compareSetting("stop_bits", std::to_string(endpoint.get<"stopBits">().value_or(1)), "1");
            compareSetting("parity", endpoint.get<"parity">() ? endpoint.get<"parity">()->view() : std::string_view("none"), "none");
            compareSetting("rs485", endpoint.get<"rs485">().value_or(false) ? "true" : "false", "false");
        }
        existing.select({ existing.cast(existing.column("id"), ruvia::db_data_type::text), sameSettings, existing.column("protocol") })
            .from(LinkEntity::table_name())
            .where((LinkEntity::column<"execution">() == "edge" && LinkEntity::column<"edge_node_id">() == nodeId && LinkEntity::column<"deleted_at">().is_null()).expression(existing));
        existing.and_where(existing.binary(jsonText(existing, "endpoint", "transport"), ruvia::db_binary_operator::equal, existing.value(transport)))
            .and_where(existing.binary(jsonText(existing, "endpoint", "interface"), ruvia::db_binary_operator::equal, existing.value(interfaceName)));
        if (transport == "tcp") {
            existing.and_where(existing.binary(jsonText(existing, "endpoint", "mode"), ruvia::db_binary_operator::equal, existing.value(endpoint.get<"mode">()->view())))
                .and_where(existing.binary(jsonText(existing, "endpoint", "ip"), ruvia::db_binary_operator::equal, existing.value(endpoint.get<"ip">()->view())))
                .and_where(existing.binary(jsonText(existing, "endpoint", "port"), ruvia::db_binary_operator::equal, existing.value(std::to_string(*endpoint.get<"port">()))));
        }
        existing.limit(1);
        const auto matches = co_await tx.query(existing);
        if (!matches.empty()) {
            const auto id = std::string(matches.front()[0].value().value_or(""));
            const bool same = matches.front()[1].value().value_or("") == "t";
            if (!same) {
                ruvia::db_query attached(c.pool());
                attached.select(attached.value(1)).from(entities::DeviceEntity::table_name())
                    .where((entities::DeviceEntity::column<"link_id">() == id && entities::DeviceEntity::column<"deleted_at">().is_null()).expression(attached));
                if (!deviceId.empty() && matches.front()[2].value().value_or("") == protocol) attached.and_where((entities::DeviceEntity::column<"id">() != deviceId).expression(attached));
                const auto attachedRows = co_await tx.query(attached);
                if (!attachedRows.empty()) {
                    service::common::fail(15002, "该接口已被设备使用，连接协议及共享串口参数必须一致", 409);
                }
            }
            ruvia::db_query update(c.pool());
            update.update(LinkEntity::table_name()).set("status", update.value("enabled"))
                .where((LinkEntity::column<"id">() == id).expression(update));
            if (!same) {
                update.set("protocol", update.value(protocol))
                    .set("endpoint", update.cast(update.value(endpointJson), ruvia::db_data_type::jsonb))
                    .set("updated_at", update.call("now"));
            } else {
                update.and_where((LinkEntity::column<"status">() != "enabled").expression(update));
            }
            (void)co_await tx.execute(update);
            co_return id;
        }
        const auto id = c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
        auto nameEnd = std::min<std::size_t>(name.size(), 60);
        while (nameEnd < name.size() && nameEnd > 0 && (static_cast<unsigned char>(name[nameEnd]) & 0xc0) == 0x80) --nameEnd;
        const auto endpointName = name.substr(0, nameEnd) + " [" + id + "]";
        ruvia::db_query query(c.pool());
        query.insert_into(LinkEntity::table_name(), { "id", "name", "protocol", "endpoint", "status", "created_by", "execution", "edge_node_id" })
            .values({ query.cast(query.value(id), ruvia::db_data_type::uuid), query.value(endpointName), query.value(protocol), query.cast(query.value(endpointJson), ruvia::db_data_type::jsonb), query.value("enabled"), query.cast(query.value(c.userId), ruvia::db_data_type::uuid), query.value("edge"), query.cast(query.value(nodeId), ruvia::db_data_type::uuid) });
        (void)co_await tx.execute(query);
        co_return id;
    }

    template <typename Context>
    ruvia::task<void> retireUnusedDeviceEndpoint(Context& c, ruvia::db_transaction& tx, std::string_view id) {
        ruvia::db_query attached(c.pool());
        attached.select(attached.value(1)).from(entities::DeviceEntity::table_name())
            .where((entities::DeviceEntity::column<"link_id">() == id && entities::DeviceEntity::column<"deleted_at">().is_null()).expression(attached));
        ruvia::db_query disable(c.pool());
        disable.update(LinkEntity::table_name()).set("status", disable.value("disabled"))
            .where((LinkEntity::column<"id">() == id && LinkEntity::column<"execution">() == "edge" && LinkEntity::column<"status">() != "disabled").expression(disable))
            .and_where(disable.unary(ruvia::db_unary_operator::not_value, disable.exists(attached)));
        (void)co_await tx.execute(disable);
    }

  private:

    static std::string required(const std::optional<ruvia::string>& value, std::string_view message) {
        if (!value || value->view().empty()) {
            service::common::fail(15002, std::string(message), 400);
        }
        return std::string(value->view());
    }



    static const ruvia::array<LinkTargetBody>& requiredTargets(const LinkEndpointBody& endpoint) {
        if (!endpoint.get<"targets">()) {
            service::common::fail(15002, "目标列表不能为空", 400);
        }
        return *endpoint.get<"targets">();
    }

    static void validateStatus(std::string_view status) {
        if (status != "enabled" && status != "disabled") {
            service::common::fail(15002, "状态无效", 400);
        }
    }

    static void validateNodeId(std::string_view nodeId) {
        if (!service::common::isUuid(nodeId)) {
            service::common::fail(15002, "节点 ID 无效", 400);
        }
    }

    static void validateEdgeEndpoint(const LinkEndpointBody& endpoint, std::string_view protocol, std::string_view transport, std::string_view interfaceName) {
        if (interfaceName.size() > 96) {
            service::common::fail(15002, "接口名称过长", 400);
        }
        if (transport == "serial") {
            if (protocol == "S7" || protocol == "MC" || protocol == "FINS") {
                service::common::fail(15002, "所选 PLC 协议仅支持 TCP", 400);
            }
            const auto baud = endpoint.get<"baudRate">().value_or(9600);
            const auto bits = endpoint.get<"dataBits">().value_or(8);
            const auto stops = endpoint.get<"stopBits">().value_or(1);
            const auto parity = endpoint.get<"parity">() ? endpoint.get<"parity">()->view() : std::string_view("none");
            if (baud < 300 || baud > 4000000 || bits < 5 || bits > 8 || stops < 1 || stops > 2 ||
                (parity != "none" && parity != "odd" && parity != "even")) {
                service::common::fail(15002, "串口参数无效", 400);
            }
        } else if (transport == "tcp") {
            const auto mode = required(endpoint.get<"mode">(), "请选择 TCP 模式");
            const auto ip = required(endpoint.get<"ip">(), "请输入 IP 地址");
            std::error_code error;
            (void)asio::ip::make_address_v4(ip, error);
            const auto port = endpoint.get<"port">().value_or(0);
            if (error || port < 1 || port > 65535 || (mode != "TCP Client" && mode != "TCP Server")) {
                service::common::fail(15002, "TCP 参数无效", 400);
            }
        } else {
            service::common::fail(15002, "传输类型无效", 400);
        }
    }

    template <typename Targets>
    static void validateConfiguration(std::string_view mode, std::string_view protocol, std::string_view ip, std::int64_t port, const Targets& targets) {
        if (mode != "TCP Server" && mode != "TCP Client") {
            service::common::fail(15003, "链路模式无效", 400);
        }
        if (protocol != "SL651" && protocol != "Modbus" && protocol != "S7" && protocol != "MC" && protocol != "FINS" && protocol != "DLT645" && protocol != "MQTT") {
            service::common::fail(15003, "协议无效", 400);
        }
        if (protocol == "SL651" && mode != "TCP Server") {
            service::common::fail(15003, "SL651 只支持 TCP Server 模式", 400);
        }
        if (protocol == "MQTT" && mode != "TCP Client")
            service::common::fail(15003, "MQTT Client 只支持 TCP Client 模式", 400);
        if (mode == "TCP Server") {
            if (ip != "0.0.0.0") {
                service::common::fail(15003, "TCP Server 监听 IP 必须是 0.0.0.0", 400);
            }
            if (port < 1 || port > 65535) {
                service::common::fail(15003, "TCP Server 必须配置有效的监听端口", 400);
            }
            if (!targets.empty()) {
                service::common::fail(15003, "TCP Server 不能配置目标地址", 400);
            }
            return;
        }
        if (!ip.empty() || port != 0) {
            service::common::fail(15003, "TCP Client 不能配置监听地址", 400);
        }
        if (targets.empty()) {
            service::common::fail(15003, "TCP Client 至少需要一个目标地址", 400);
        }
        std::set<std::string> ids;
        std::set<std::string> endpoints;
        for (const auto& target : targets) {
            const auto& mqtt = target.template get<"mqtt">();
            if (protocol == "MQTT" && (!mqtt || mqtt->template get<"clientId">().view().empty()))
                service::common::fail(15003, "MQTT 目标必须配置 Client ID", 400);
            if (mqtt && mqtt->template get<"password">() && !mqtt->template get<"password">()->view().empty() &&
                (!mqtt->template get<"username">() || mqtt->template get<"username">()->view().empty()))
                service::common::fail(15003, "MQTT 密码需要同时配置用户名", 400);
            const auto id = std::string(target.template get<"id">().view());
            const auto name = std::string(target.template get<"name">().view());
            const auto targetIp = std::string(target.template get<"ip">().view());
            const auto targetPort = static_cast<std::int64_t>(target.template get<"port">());
            const auto targetStatus = target.template get<"status">()
                ? std::string(target.template get<"status">()->view())
                : "enabled";
            if (targetStatus != "enabled" && targetStatus != "disabled") {
                service::common::fail(15003, "目标状态无效", 400);
            }
            const auto brokerHost = [&] {
                std::error_code error;
                (void)asio::ip::make_address(targetIp, error);
                if (!error) return true;
                std::string_view host = targetIp;
                if (host.ends_with('.')) host.remove_suffix(1);
                if (host.empty() || host.size() > 253) return false;
                for (const auto label : service::utils::splitPayload(host, ".")) {
                    if (label.empty() || label.size() > 63 || label.front() == '-' || label.back() == '-') return false;
                    for (const unsigned char ch : label) if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-')) return false;
                }
                return true;
            };
            if (name.empty() || !(protocol == "MQTT" ? brokerHost() : isIpv4(targetIp)) || targetPort < 1 || targetPort > 65535) {
                service::common::fail(15003, "目标地址配置无效", 400);
            }
            if (!ids.emplace(id).second) {
                service::common::fail(15004, "同一链路内目标 ID 不能重复", 409);
            }
            if (!endpoints.emplace(targetIp + ":" + std::to_string(targetPort)).second) {
                service::common::fail(15004, "同一链路内目标地址不能重复", 409);
            }
        }
    }

    static bool isIpv4(std::string_view value) {
        int parts = 0;
        std::size_t start = 0;
        while (start < value.size()) {
            const auto end = value.find('.', start);
            const auto part = value.substr(
                start,
                end == std::string_view::npos ? value.size() - start : end - start
            );
            if (part.empty() || part.size() > 3) {
                return false;
            }
            int number = 0;
            for (const char ch : part) {
                if (!std::isdigit(static_cast<unsigned char>(ch))) {
                    return false;
                }
                number = number * 10 + (ch - '0');
            }
            if (number > 255) {
                return false;
            }
            ++parts;
            if (end == std::string_view::npos) {
                break;
            }
            start = end + 1;
        }
        return parts == 4;
    }

    static ruvia::db_expression jsonText(ruvia::db_query& query, std::string_view column, std::string_view key, std::string_view table = {}) {
        return query.binary(query.column(column, table), ruvia::db_binary_operator::json_get_text, query.cast(query.value(key), ruvia::db_data_type::text));
    }

    static ruvia::db_expression jsonValue(ruvia::db_query& query, std::string_view column, std::string_view key, std::string_view table = {}) {
        return query.binary(query.column(column, table), ruvia::db_binary_operator::json_get, query.cast(query.value(key), ruvia::db_data_type::text));
    }

    static ruvia::db_expression displayName(ruvia::db_query& query) {
        ruvia::db_query node(query.resource());
        node.select(node.coalesce({
                node.null_if(node.column(std::tuple_element_t<entities::EdgeNodeEntity::column_index<"name">(), entities::EdgeNodeEntity::columns_type>::name.view(), "edge_link_node"), node.value("")),
                node.null_if(node.column(std::tuple_element_t<entities::EdgeNodeEntity::column_index<"hostname">(), entities::EdgeNodeEntity::columns_type>::name.view(), "edge_link_node"), node.value("")),
                node.column(std::tuple_element_t<entities::EdgeNodeEntity::column_index<"imei">(), entities::EdgeNodeEntity::columns_type>::name.view(), "edge_link_node")}))
            .from(entities::EdgeNodeEntity::table_name(), "edge_link_node")
            .where(node.binary(node.column(std::tuple_element_t<entities::EdgeNodeEntity::column_index<"id">(), entities::EdgeNodeEntity::columns_type>::name.view(), "edge_link_node"),
                ruvia::db_binary_operator::equal, node.column(std::tuple_element_t<LinkEntity::column_index<"edge_node_id">(), LinkEntity::columns_type>::name.view(), "link")));
        return query.case_when({{(LinkEntity::column<"execution">() == "edge").expression(query),
            query.call("concat", {query.coalesce({query.subquery(node), query.value("边缘节点")}),
                                  query.cast(query.value(" · "), ruvia::db_data_type::text),
                                  query.column(std::tuple_element_t<LinkEntity::column_index<"protocol">(), LinkEntity::columns_type>::name.view())})}},
            query.column(std::tuple_element_t<LinkEntity::column_index<"name">(), LinkEntity::columns_type>::name.view()));
    }

    template <typename Context>
    static ruvia::task<bool> edgeDebugNodeAvailable(Context& c, std::string_view nodeId) {
        if (!service::common::isUuid(nodeId))
            co_return false;
        ruvia::db_query query(c.pool());
        query.select({query.column(std::tuple_element_t<entities::EdgeNodeEntity::column_index<"enrollment_status">(), entities::EdgeNodeEntity::columns_type>::name.view()),
                      jsonText(query, std::tuple_element_t<entities::EdgeNodeEntity::column_index<"capability">(), entities::EdgeNodeEntity::columns_type>::name.view(), "deviceConfig")})
            .from(entities::EdgeNodeEntity::table_name())
            .where((entities::EdgeNodeEntity::column<"id">() == nodeId).expression(query));
        const auto rows = co_await c.db().query(query);
        co_return !rows.empty() && rows.front()[0].value().value_or("") == "approved" &&
            rows.front()[1].value().value_or("") == "true";
    }

    template <typename Context>
    static ruvia::task<void> requireEdgeDebugNode(Context& c, std::string_view nodeId) {
        if (!co_await edgeDebugNodeAvailable(c, nodeId))
            service::common::fail(15002, "节点未批准或不支持链路调试配置", 409);
    }

    static ruvia::db_query linkSelect(std::pmr::memory_resource* resource) {
        ruvia::db_query query(resource);
        const auto endpointPort = query.coalesce(
            { query.null_if(jsonText(query, "endpoint", "port"), query.value("")),
              query.value("0") }
        );
        query.select({ query.cast(query.column("id"), ruvia::db_data_type::text), displayName(query), query.column("protocol"), jsonText(query, "endpoint", "mode"), query.coalesce({ jsonText(query, "endpoint", "ip"), query.value("") }), endpointPort, query.column("status"), query.cast(query.column("created_by"), ruvia::db_data_type::text), query.call("iot_utc_timestamp", { query.column("created_at") }), query.call("iot_utc_timestamp", { query.column("updated_at") }), query.column("execution"), query.coalesce({ query.cast(query.column("edge_node_id"), ruvia::db_data_type::text), query.value("") }), query.column(std::tuple_element_t<LinkEntity::column_index<"debug_enabled">(), LinkEntity::columns_type>::name.view()) })
            .from(LinkEntity::table_name());
        return query;
    }

    static void applyFilters(ruvia::db_query& query, const std::optional<std::string>& keyword, const std::optional<std::string>& mode, const std::optional<std::string>& protocol, const std::optional<std::string>& status) {
        query.where(LinkEntity::column<"deleted_at">().is_null().expression(query));
        ruvia::db_query devices(query.resource());
        devices.select(devices.value(1)).from(entities::DeviceEntity::table_name(), "linked_device")
            .where(devices.binary(devices.column("link_id", "linked_device"), ruvia::db_binary_operator::equal, devices.column("id", "link")))
            .and_where(devices.unary(ruvia::db_unary_operator::is_null, devices.column("deleted_at", "linked_device")));
        query.and_where(query.binary((LinkEntity::column<"execution">() == "collector").expression(query), ruvia::db_binary_operator::or_value, query.exists(devices)));

        if (keyword && !keyword->empty()) {
            query.and_where(query.binary(displayName(query), ruvia::db_binary_operator::i_like, query.value("%" + *keyword + "%")));
        }
        if (mode && !mode->empty()) {
            query.and_where(query.binary(jsonText(query, "endpoint", "mode"), ruvia::db_binary_operator::equal, query.value(*mode)));
        }
        if (protocol && !protocol->empty()) {
            query.and_where((LinkEntity::column<"protocol">() == *protocol).expression(query));
        }
        if (status && !status->empty()) {
            query.and_where((LinkEntity::column<"status">() == *status).expression(query));
        }
    }

    template <typename Context>
    static ruvia::task<void> fillEndpoint(Context& c, LinkEndpointDto& endpoint, std::string_view id) {
        ruvia::db_query query(c.pool());
        query.select({ jsonText(query, "endpoint", "transport"), jsonText(query, "endpoint", "interface"), jsonText(query, "endpoint", "baud_rate"), jsonText(query, "endpoint", "data_bits"), jsonText(query, "endpoint", "stop_bits"), jsonText(query, "endpoint", "parity"), jsonText(query, "endpoint", "rs485") })
            .from(LinkEntity::table_name())
            .where((LinkEntity::column<"id">() == id &&
                    LinkEntity::column<"execution">() == "edge")
                       .expression(query))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            co_return;
        }
        const auto& row = rows.front();
        endpoint.set<"transport">(row[0].value().value_or(""));
        endpoint.set<"interfaceName">(row[1].value().value_or(""));
        endpoint.set<"baudRate">(toInt(row[2].value().value_or("9600")));
        endpoint.set<"dataBits">(toInt(row[3].value().value_or("8")));
        endpoint.set<"stopBits">(toInt(row[4].value().value_or("1")));
        endpoint.set<"parity">(row[5].value().value_or("none"));
        endpoint.set<"rs485">(row[6].value().value_or("false") == "true");
    }

    static ruvia::int64 toInt(std::string_view value) {
        return static_cast<ruvia::int64>(
            service::utils::parseInt64(std::optional<std::string_view>{ value }).value_or(0)
        );
    }

    static std::string parsePublicIp(std::string_view body) {
        std::string value(body);
        const auto first = value.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) {
            return {};
        }
        const auto last = value.find_last_not_of(" \t\r\n");
        value = value.substr(first, last - first + 1);
        if (value.size() > 64 ||
            !std::all_of(value.begin(), value.end(), [](const unsigned char character) {
                return std::isxdigit(character) || character == '.' || character == ':';
            })) {
            return {};
        }
        return value;
    }

    template <typename Context, typename Row>
    ruvia::task<void> fill(Context& c, LinkItemDto& item, const Row& row) {
        const auto id = std::string(row[0].value().value_or(std::string_view{}));
        const auto runtime = co_await loadRuntimeStatus(c, id);
        ruvia::boxed_array<ruvia::string> clients(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        const auto clientText = runtime.text("clients");
        std::size_t start = 0;
        while (start < clientText.size()) {
            const auto end = clientText.find('\n', start);
            clients.emplace(clientText.substr(start, end == std::string::npos ? std::string::npos : end - start), ruvia::model_options{ .resource_ = c.arena() });
            if (end == std::string::npos) {
                break;
            }
            start = end + 1;
        }
        RuntimeDto runtimeDto(ruvia::model_options{ .resource_ = c.arena() });
        runtimeDto.set<"state">(runtime.text("state", "stopped"));
        runtimeDto.set<"reason">(runtime.text("state_reason"));
        runtimeDto.set<"error">(runtime.text("error"));
        runtimeDto.set<"clientCount">(runtime.integer("connection_count"));
        runtimeDto.set<"clients">(std::move(clients));
        const auto lastActivityAt = runtime.integer("last_activity_at_ms");
        if (lastActivityAt > 0) {
            runtimeDto.set<"lastActivityAt">(
                service::common::utcTimestampFromMilliseconds(lastActivityAt)
            );
        }
        item.set<"id">(id);
        item.set<"name">(row[1].value().value_or(std::string_view{}));
        item.set<"protocol">(row[2].value().value_or(std::string_view{}));
        item.set<"status">(row[6].value().value_or(std::string_view{}));
        item.set<"runtime">(std::move(runtimeDto));
        item.set<"createdBy">(row[7].value().value_or(std::string_view{}));
        item.set<"createdAt">(row[8].value().value_or(std::string_view{}));
        item.set<"updatedAt">(row[9].value().value_or(std::string_view{}));
        item.set<"debugEnabled">(row[12].value().value_or("") == "t");
        item.set<"execution">(row[10].value().value_or("collector"));
        item.set<"edgeNodeId">(row[11].value().value_or(""));
        LinkEndpointDto endpoint(ruvia::model_options{ .resource_ = c.arena() });
        endpoint.set<"mode">(row[3].value().value_or(std::string_view{}));
        endpoint.set<"ip">(row[4].value().value_or(std::string_view{}));
        endpoint.set<"port">(toInt(row[5].value().value_or(std::string_view{})));
        endpoint.set<"targets">(co_await loadTargets(c, id, runtime));
        co_await fillEndpoint(c, endpoint, id);
        item.set<"endpoint">(std::move(endpoint));
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<LinkTargetDto>> loadTargets(Context& c, std::string_view id, const LinkRuntimeRecord& runtime) {
        ruvia::db_query query(c.pool());
        const auto targets = query.coalesce(
            { jsonValue(query, "endpoint", "targets"),
              query.cast(query.value("[]"), ruvia::db_data_type::jsonb) }
        );
        query.select({ jsonText(query, "target", "id", "value"), jsonText(query, "target", "name", "value"), jsonText(query, "target", "ip", "value"), jsonText(query, "target", "port", "value"), jsonText(query, "target", "status", "value"), jsonText(query, "target", "mqtt", "value") })
            .from(LinkEntity::table_name(), "link")
            .join_function(ruvia::db_join_type::cross, query.call("jsonb_array_elements", { targets }), {}, "value", { .lateral_ = true, .with_ordinality_ = true, .columns_ = { { .name_ = "target" }, { .name_ = "position" } } })
            .where(query.binary(query.column("id", "link"), ruvia::db_binary_operator::equal, query.value(id)))
            .order_by(query.column("position", "value"));
        const auto rows = co_await c.db().query(query);
        ruvia::boxed_array<LinkTargetDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& target = result.emplace(ruvia::model_options{ .resource_ = c.arena() });
            const auto targetId = std::string(row[0].value().value_or(std::string_view{}));
            const auto prefix = "target:" + targetId + ':';
            RuntimeDto targetRuntime(ruvia::model_options{ .resource_ = c.arena() });
            targetRuntime.set<"state">(runtime.text(prefix + "state", "stopped"))
                .template set<"reason">(runtime.text(prefix + "reason"))
                .template set<"error">(runtime.text(prefix + "error"));
            const auto lastActivityAt = runtime.integer(prefix + "last_activity_at_ms");
            if (lastActivityAt > 0) {
                targetRuntime.set<"lastActivityAt">(
                    service::common::utcTimestampFromMilliseconds(lastActivityAt)
                );
            }
            if (const auto json = row[5].value(); json && !json->empty()) {
                auto mqtt = ruvia::from_json<MqttConnectionBody>(*json);
                if (mqtt) target.set<"mqtt">(std::move(*mqtt));
            }
            target.set<"id">(targetId)
                .template set<"name">(row[1].value().value_or(std::string_view{}))
                .template set<"ip">(row[2].value().value_or(std::string_view{}))
                .template set<"port">(toInt(row[3].value().value_or(std::string_view{})))
                .template set<"status">(row[4].value().value_or(std::string_view{}))
                .template set<"runtime">(std::move(targetRuntime));
        }
        co_return result;
    }

    static int runtimeStateRank(std::string_view state, std::string_view reason) {
        if (reason == "target_owned") return -1;
        if (state == "connected") return 6;
        if (state == "listening") return 5;
        if (state == "reconnecting") return 4;
        if (state == "connecting") return 3;
        if (state == "error") return 2;
        if (state == "idle") return 1;
        return 0;
    }

    static void mergeRuntimeState(LinkRuntimeRecord& aggregate, const LinkRuntimeRecord& worker,
                                  const std::string& prefix = {}) {
        const auto stateKey = prefix + "state";
        const auto reasonKey = prefix.empty() ? "state_reason" : prefix + "reason";
        const auto updatedKey = prefix + "updated_at_ms";
        const auto rank = runtimeStateRank(worker.text(stateKey), worker.text(reasonKey));
        const auto currentRank = runtimeStateRank(aggregate.text(stateKey), aggregate.text(reasonKey));
        if (aggregate.fields.contains(stateKey) &&
            (rank < currentRank || (rank == currentRank &&
                worker.integer("updated_at_ms") <= aggregate.integer(updatedKey)))) return;

        // 状态、原因、错误和活动时间必须来自同一份 Worker 快照。
        aggregate.fields[stateKey] = worker.text(stateKey, "stopped");
        aggregate.fields[reasonKey] = worker.text(reasonKey);
        aggregate.fields[prefix + "error"] = worker.text(prefix + "error");
        aggregate.fields[prefix + "last_activity_at_ms"] = worker.text(prefix + "last_activity_at_ms", "0");
        aggregate.fields[updatedKey] = worker.text("updated_at_ms", "0");
    }

    static void normalizeStandbyState(LinkRuntimeRecord& status, const std::string& prefix = {}) {
        const auto reasonKey = prefix.empty() ? "state_reason" : prefix + "reason";
        if (status.text(reasonKey) != "target_owned") return;
        // 尚未收到持有者快照时显示连接中，内部认领竞争不代表目标连接失败。
        status.fields[prefix + "state"] = "connecting";
        status.fields[reasonKey].clear();
        status.fields[prefix + "error"].clear();
    }

    template <typename Context>
    static ruvia::task<LinkRuntimeRecord> loadRuntimeStatus(Context& c, std::string_view id) {
        LinkRuntimeRecord status;
        try {
            const auto owner = co_await c.redis().get(service::message::collector::linkOwnerKey(id));
            if (!owner) {
                co_return status;
            }
            const auto pattern = service::message::collector::linkRuntimePrefix(id) + std::string(owner->data(), owner->size()) + ":*";
            std::string cursor = "0";
            std::set<std::string> keys;
            do {
                const std::array<std::string_view, 6> scanArgs{ "SCAN", cursor, "MATCH", pattern, "COUNT", "100" };
                const auto page = co_await c.redis().command(scanArgs);
                if (page.kind() != ruvia::redis_value::kind_type::array || page.array().size() != 2 ||
                    page.array()[0].kind() != ruvia::redis_value::kind_type::string ||
                    page.array()[1].kind() != ruvia::redis_value::kind_type::array) {
                    break;
                }
                cursor.assign(page.array()[0].string());
                for (const auto& value : page.array()[1].array()) {
                    if (value.kind() == ruvia::redis_value::kind_type::string) {
                        keys.emplace(value.string());
                    }
                }
            } while (cursor != "0");

            std::int64_t connectionCount = 0;
            std::int64_t lastActivityAt = 0;
            std::string clients;
            std::set<std::string> targetPrefixes;
            for (const auto& key : keys) {
                const std::array<std::string_view, 2> hashArgs{ "HGETALL", key };
                const auto reply = co_await c.redis().command(hashArgs);
                if (reply.kind() != ruvia::redis_value::kind_type::array) {
                    continue;
                }
                LinkRuntimeRecord worker;
                const auto values = reply.array();
                for (std::size_t index = 0; index + 1 < values.size(); index += 2) {
                    if (values[index].kind() != ruvia::redis_value::kind_type::string ||
                        values[index + 1].kind() != ruvia::redis_value::kind_type::string) {
                        continue;
                    }
                    worker.fields.insert_or_assign(std::string(values[index].string()), std::string(values[index + 1].string()));
                }
                connectionCount += worker.integer("connection_count");
                lastActivityAt = std::max(lastActivityAt, worker.integer("last_activity_at_ms"));
                const auto endpoints = worker.text("remote_endpoints");
                std::size_t start = 0;
                while (start < endpoints.size()) {
                    const auto end = endpoints.find(',', start);
                    if (!clients.empty()) {
                        clients.push_back('\n');
                    }
                    clients.append(endpoints, start, end == std::string::npos ? std::string::npos : end - start);
                    if (end == std::string::npos) {
                        break;
                    }
                    start = end + 1;
                }
                mergeRuntimeState(status, worker);
                for (const auto& [name, value] : worker.fields) {
                    if (!name.starts_with("target:") || !name.ends_with(":state")) continue;
                    const auto prefix = name.substr(0, name.size() - 5);
                    targetPrefixes.insert(prefix);
                    mergeRuntimeState(status, worker, prefix);
                }
            }
            normalizeStandbyState(status);
            for (const auto& prefix : targetPrefixes) normalizeStandbyState(status, prefix);
            status.fields["connection_count"] = std::to_string(connectionCount);
            status.fields["clients"] = std::move(clients);
            status.fields["last_activity_at_ms"] = std::to_string(lastActivityAt);
        } catch (const std::exception&) {
        }
        co_return status;
    }

    static void appendJsonString(std::string& output, std::string_view value) {
        static constexpr char hex[] = "0123456789abcdef";
        output.push_back('"');
        for (const unsigned char ch : value) {
            switch (ch) {
                case '"':
                    output += "\\\"";
                    break;
                case '\\':
                    output += "\\\\";
                    break;
                case '\b':
                    output += "\\b";
                    break;
                case '\f':
                    output += "\\f";
                    break;
                case '\n':
                    output += "\\n";
                    break;
                case '\r':
                    output += "\\r";
                    break;
                case '\t':
                    output += "\\t";
                    break;
                default:
                    if (ch < 0x20) {
                        output += "\\u00";
                        output.push_back(hex[ch >> 4]);
                        output.push_back(hex[ch & 0x0f]);
                    } else {
                        output.push_back(static_cast<char>(ch));
                    }
            }
        }
        output.push_back('"');
    }

    template <typename Targets>
    static std::string serializeTargets(const Targets& targets) {
        std::string result = "[";
        for (const auto& target : targets) {
            if (result.size() > 1) {
                result.push_back(',');
            }
            result += "{\"id\":";
            appendJsonString(result, target.template get<"id">().view());
            result += ",\"name\":";
            appendJsonString(result, target.template get<"name">().view());
            result += ",\"ip\":";
            appendJsonString(result, target.template get<"ip">().view());
            result += ",\"port\":" +
                std::to_string(static_cast<std::int64_t>(target.template get<"port">()));
            result += ",\"status\":";
            const auto& status = target.template get<"status">();
            appendJsonString(result, status ? status->view() : "enabled");
            if (const auto& mqtt = target.template get<"mqtt">()) {
                result += ",\"mqtt\":";
                const auto json = ruvia::to_json(*mqtt);
                result.append(json.data(), json.size());
            }
            result.push_back('}');
        }
        result.push_back(']');
        return result;
    }

    template <typename Targets>
    static std::string serializeEndpoint(std::string_view mode, std::string_view ip, std::int64_t port, const Targets& targets) {
        std::string result = "{\"transport\":\"tcp\",\"mode\":";
        appendJsonString(result, mode);
        result += ",\"ip\":";
        appendJsonString(result, mode == "TCP Server" ? ip : "");
        result += ",\"port\":" + std::to_string(mode == "TCP Server" ? port : 0);
        result += ",\"targets\":";
        result += mode == "TCP Client" ? serializeTargets(targets) : "[]";
        result.push_back('}');
        return result;
    }

    template <typename Context>
    ruvia::task<void> ensureAvailable(Context& c, const std::string& name, const std::string& mode, const std::string& ip, std::int64_t port, std::optional<std::string> excludedId) {
        ruvia::db_query query(c.pool());
        const auto endpointPort = jsonText(query, "endpoint", "port");
        const auto endpointPortText = query.coalesce({ endpointPort, query.value("") });
        const auto endpointPortIsInteger = query.binary(
            endpointPortText,
            ruvia::db_binary_operator::regex,
            query.value("^[0-9]{1,5}$")
        );
        const auto safeEndpointPort = query.case_when(
            { { endpointPortIsInteger, query.cast(endpointPort, ruvia::db_data_type::integer) } },
            query.cast(query.value(std::int64_t{ 0 }), ruvia::db_data_type::integer)
        );
        const auto nameTaken = query.binary(query.column("name"), ruvia::db_binary_operator::equal, query.value(name));
        query.select(query.cast(query.value(1), ruvia::db_data_type::integer))
            .from(LinkEntity::table_name())
            .where(LinkEntity::column<"deleted_at">().is_null().expression(query));
        if (mode == "TCP Server") {
            const auto serverAddress = query.binary(
                query.binary(query.column("execution"), ruvia::db_binary_operator::equal, query.value("collector")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(jsonText(query, "endpoint", "mode"), ruvia::db_binary_operator::equal, query.value(mode)),
                    ruvia::db_binary_operator::and_value,
                    query.binary(
                        query.binary(jsonText(query, "endpoint", "ip"), ruvia::db_binary_operator::equal, query.value(ip)),
                        ruvia::db_binary_operator::and_value,
                        query.binary(safeEndpointPort, ruvia::db_binary_operator::equal, query.value(port))
                    )
                )
            );
            query.and_where(query.binary(nameTaken, ruvia::db_binary_operator::or_value, serverAddress));
        } else {
            query.and_where(nameTaken);
        }
        if (excludedId) {
            query.and_where(query.binary(query.column("id"), ruvia::db_binary_operator::not_equal, query.cast(query.value(*excludedId), ruvia::db_data_type::uuid)));
        }
        query.limit(1);
        const auto rows = co_await c.db().query(query);
        if (!rows.empty()) {
            service::common::fail(15005, "链路名称或监听地址已存在", 409);
        }
    }

    template <typename Context>
    ruvia::task<void> requireOwner(Context& c, std::string_view ownerId) {
        if (c.userId == ownerId) {
            co_return;
        }
        ruvia::db_query query(c.pool());
        query.select(query.cast(query.value(1), ruvia::db_data_type::integer))
            .from(service::user::UserRoleEntity::table_name(), "ur")
            .join(ruvia::db_join_type::inner, service::role::RoleEntity::table_name(), query.binary(query.column("role_id", "ur"), ruvia::db_binary_operator::equal, query.column("id", "r")), "r")
            .where(query.binary(query.column("user_id", "ur"), ruvia::db_binary_operator::equal, query.value(c.userId)))
            .and_where(query.binary(query.column("code", "r"), ruvia::db_binary_operator::equal, query.value("superadmin")))
            .and_where(query.binary(query.column("status", "r"), ruvia::db_binary_operator::equal, query.value("enabled")))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column("deleted_at", "r")))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(15007, "只能修改或删除自己创建的链路", 403);
        }
    }

    std::string cachedPublicIp_;
    std::chrono::steady_clock::time_point publicIpCachedAt_{};
};

inline LinkService& linkService() {
    return LinkService::instance();
}

} // namespace service::link
