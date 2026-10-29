#pragma once

#include <algorithm>
#include <array>
#include <asio/ip/address.hpp>
#include <asio/ip/address_v4.hpp>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <ruvia/core/timer.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/http_client.h>
#include <ruvia/web/model.h>
#include <ruvia/web/redis/redis_types.h>
#include <ruvia/web/validation.h>
#include <ruvia/web/web_worker.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/modules/device/device.entity.h"
#include "service/modules/edge_node/edge_node.entity.h"
#include "service/modules/edge_node/edge_node.service.h"
#include "service/modules/link/link.entity.h"
#include "service/modules/link/link.types.h"
#include "service/modules/system/outbox/outbox.service.h"
#include "service/modules/system/role/role.entity.h"
#include "service/modules/system/user/user.entity.h"
#include "service/utils/json.h"
#include "service/utils/log.h"
#include "service/utils/number.h"
#include "service/utils/payload.h"
#include "service/utils/redis.h"
#include "service/utils/uuid.h"

namespace service::link {

namespace db = service::common::database;

inline constexpr auto kDebugIdle = std::chrono::minutes(5);

struct DebugIdleKeys {
    std::string lease;
    std::string watch;
};

// 调试空闲关闭（链路与设备共用）：调试活动以 lease 键续租；首个认领 watch 键的 Worker
// 按租约截止时间唤醒，租约消失后才关闭调试。关闭失败保留认领并有界退避重试，Worker 停止即退出。
class DebugIdle final {
  public:
    template <typename Redis>
    static ruvia::task<void> touch(const Redis& redis, const DebugIdleKeys& keys) {
        ruvia::redis_set_options options;
        options.expiration_ = ruvia::redis_set_expiration::expires_after(kDebugIdle);
        (void)co_await redis.set(keys.lease, "1", options);
    }

    // 只延长仍存在的租约，已关闭的调试不会因迟到的活动重新建立租约。
    template <typename Redis>
    static ruvia::task<void> extend(const Redis& redis, const DebugIdleKeys& keys) {
        (void)co_await redis.expire(keys.lease, std::chrono::duration_cast<std::chrono::seconds>(kDebugIdle));
    }

    template <typename Redis>
    static ruvia::task<void> clear(const Redis& redis, const DebugIdleKeys& keys) {
        (void)co_await redis.del(keys.lease);
        (void)co_await redis.del(keys.watch);
    }

    template <typename Redis>
    static ruvia::task<bool> leased(const Redis& redis, const DebugIdleKeys& keys) {
        co_return co_await redis.exists(keys.lease);
    }

    // disable: (ruvia::web_worker_context&, std::string_view id) -> ruvia::task<void>
    template <typename Disable>
    static void watch(const ruvia::web_worker_handle& worker, DebugIdleKeys keys, std::string id, Disable disable) {
        (void)worker.post([keys = std::move(keys), id = std::move(id), disable = std::move(disable)](ruvia::web_worker_context& context) mutable -> ruvia::task<void> {
            return run(context, std::move(keys), std::move(id), std::move(disable));
        });
    }

  private:
    static constexpr std::chrono::milliseconds kClaimMargin = std::chrono::seconds(30);
    static constexpr std::chrono::milliseconds kWakeMargin = std::chrono::seconds(1);
    static constexpr std::chrono::milliseconds kRetryInitial = std::chrono::seconds(1);
    static constexpr std::chrono::milliseconds kRetryMax = std::chrono::minutes(1);

    // 租约剩余时长，无租约时为空；无过期时间的租约不是 touch 写入的，恢复为空闲时长后按其截止。
    template <typename Redis>
    static ruvia::task<std::optional<std::chrono::milliseconds>> leaseRemaining(const Redis& redis, const DebugIdleKeys& keys) {
        const auto ttl = co_await redis.pttl(keys.lease);
        if (ttl.state() == ruvia::redis_ttl_state::missing) {
            co_return std::nullopt;
        }
        if (ttl.state() == ruvia::redis_ttl_state::expiring && ttl.remaining()) {
            co_return std::max(*ttl.remaining(), std::chrono::milliseconds::zero());
        }
        (void)co_await redis.expire(keys.lease, std::chrono::duration_cast<std::chrono::seconds>(kDebugIdle));
        co_return std::chrono::milliseconds(kDebugIdle);
    }

    template <typename Redis>
    static ruvia::task<void> holdClaim(const Redis& redis, const DebugIdleKeys& keys, std::chrono::milliseconds wait) {
        ruvia::redis_set_options keep;
        keep.expiration_ = ruvia::redis_set_expiration::expires_after(wait + kClaimMargin);
        (void)co_await redis.set(keys.watch, "1", keep);
    }

    template <typename Disable>
    static ruvia::task<void> run(ruvia::web_worker_context& context, DebugIdleKeys keys, std::string id, Disable disable) {
        ruvia::redis_set_options claim;
        claim.condition_ = ruvia::redis_set_condition::if_absent;
        claim.expiration_ = ruvia::redis_set_expiration::expires_after(std::chrono::milliseconds(kDebugIdle) + kClaimMargin);
        if (!(co_await context.redis().set(keys.watch, "1", claim)).applied()) {
            co_return;
        }
        auto retryDelay = kRetryInitial;
        while (!context.stop_token().stop_requested()) {
            std::chrono::milliseconds wait{};
            std::string failure;
            try {
                if (const auto remaining = co_await leaseRemaining(context.redis(), keys)) {
                    wait = *remaining + kWakeMargin;
                    retryDelay = kRetryInitial;
                } else {
                    co_await disable(context, std::string_view(id));
                    (void)co_await context.redis().del(keys.watch);
                    co_return;
                }
            } catch (const std::exception& error) {
                failure = error.what();
            } catch (...) {
                failure = "未知错误";
            }
            if (!failure.empty()) {
                LOG_ERROR << "调试空闲关闭失败，" << retryDelay.count() << " ms 后重试 key=" << keys.lease << ": " << failure;
                wait = retryDelay;
                retryDelay = std::min(retryDelay * 2, kRetryMax);
            }
            try {
                co_await holdClaim(context.redis(), keys, wait);
            } catch (const std::exception& error) {
                LOG_WARN << "调试空闲看守续期失败 key=" << keys.watch << ": " << error.what();
            }
            (void)co_await ruvia::sleep_for(context.worker(), wait, context.stop_token());
        }
    }
};

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
        auto countBuilder = c.db().template repository<entities::LinkEntity>().query_builder();
        countBuilder.select({ { "value0", count.aggregate("count", { count.star() }) } });
        applyFilters(countBuilder, count, keyword, mode, protocol, status);
        const auto countRows = co_await countBuilder.get_raw_many();
        const ruvia::int64 total = countRows.empty()
            ? ruvia::int64{ 0 }
            : toInt(countRows.front()[0].value().value_or(std::string_view{}));

        ruvia::db_query queryExpressions;
        auto query = linkSelect(c.db(), c.pool());
        applyFilters(query, queryExpressions, keyword, mode, protocol, status);
        query.order_by((entities::LinkEntity::column<"execution">() == "edge").expression(queryExpressions), ruvia::db_order_direction::desc)
            .add_order_by(entities::LinkEntity::column<"id">().expression(queryExpressions), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>((page - 1) * pageSize));
        const auto rows = co_await query.get_raw_many();

        ruvia::boxed_array<LinkItemDto> links(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = links.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
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

    // Links among `ids` that pass the list filters, in the list row shape (live updates).
    template <typename Context>
    ruvia::task<ruvia::boxed_array<LinkItemDto>> listByIds(Context& c, const std::vector<std::string>& ids, std::optional<std::string> keyword, std::optional<std::string> mode, std::optional<std::string> protocol, std::optional<std::string> status) {
        ruvia::boxed_array<LinkItemDto> links(ruvia::model_options{ .resource_ = c.arena() });
        ruvia::db_query queryExpressions;
        auto query = linkSelect(c.db(), c.pool());
        const auto values = uuidList(queryExpressions, ids);
        if (!values) {
            co_return links;
        }
        applyFilters(query, queryExpressions, keyword, mode, protocol, status);
        query.and_where(queryExpressions.binary(queryExpressions.column(entities::LinkEntity::column_name<"id">()), ruvia::db_binary_operator::in, *values));
        const auto rows = co_await query.get_raw_many();
        for (const auto& row : rows) {
            auto& item = links.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
            co_await fill(c, item, row);
        }
        co_return links;
    }

    // Links used by the given devices (deleted ones included): edge links are
    // listed only while a device uses them, so device changes alter their visibility.
    template <typename Context>
    ruvia::task<std::vector<std::string>> linksOfDevices(Context& c, const std::vector<std::string>& deviceIds) {
        std::vector<std::string> result;
        ruvia::db_query query(c.pool());
        const auto values = uuidList(query, deviceIds);
        if (!values) {
            co_return result;
        }
        auto queryBuilder = c.db().template repository<service::device::entities::DeviceEntity>().query_builder();
        queryBuilder.select({ { "link_id", query.cast(query.column(service::device::entities::DeviceEntity::column_name<"link_id">()), ruvia::db_data_type::text) } }).distinct().where(query.binary(query.column(service::device::entities::DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::in, *values));
        const auto rows = co_await queryBuilder.get_raw_many();
        for (const auto& row : rows) {
            if (const auto id = row[0].value()) {
                result.emplace_back(*id);
            }
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<LinkItemDto> detail(Context& c, std::string_view id) {
        ruvia::db_query queryExpressions;
        auto query = linkSelect(c.db(), c.pool());
        query.where((entities::LinkEntity::column<"id">() == id &&
                     entities::LinkEntity::column<"deleted_at">().is_null())
                        .expression(queryExpressions))
            .limit(1);
        const auto rows = co_await query.get_raw_many();
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
        modes.emplace_back("TCP Server", modelOptions);
        modes.emplace_back("TCP Client", modelOptions);
        ruvia::boxed_array<ruvia::string> protocols(modelOptions);
        protocols.emplace_back("SL651", modelOptions);
        protocols.emplace_back("Modbus", modelOptions);
        protocols.emplace_back("S7", modelOptions);
        protocols.emplace_back("MC", modelOptions);
        protocols.emplace_back("FINS", modelOptions);
        protocols.emplace_back("DLT645", modelOptions);
        protocols.emplace_back("MQTT", modelOptions);
        ruvia::boxed_array<ruvia::string> statuses(modelOptions);
        statuses.emplace_back("enabled", modelOptions);
        statuses.emplace_back("disabled", modelOptions);
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
            auto response = co_await c.http_client("link-public-ip").send({ .headers_ = headers });
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
        const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        entities::LinkEntity link(c.pool());
        link.set<"id">(id);
        link.set<"name">(name);
        link.set<"protocol">(protocol);
        link.set<"endpoint">(endpointJson);
        link.set<"status">(status);
        link.set<"created_by">(c.userId);
        link.set<"execution">("collector");
        (void)co_await transaction.template repository<entities::LinkEntity>().insert(link);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "created", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<LinkDebugAcquisitionDto>> debugPackets(Context& c, std::string_view id) {
        const auto link = co_await detail(c, id);
        if (link.template get<"execution">() && link.template get<"execution">()->view() == "edge") {
            co_await requireOwner(c, link.template get<"createdBy">()->view());
        }
        auto leaseTransaction = co_await c.db().begin_transaction();
        ruvia::db_query lease(c.pool());
        auto leaseBuilder = leaseTransaction.template repository<entities::LinkEntity>().query_builder();
        leaseBuilder.select({ { "debug_enabled", entities::LinkEntity::column<"debug_enabled">().expression(lease) } }).where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null()).expression(lease)).set_lock(ruvia::db_lock_options{});
        const auto leaseRows = co_await leaseBuilder.get_raw_many();
        if (!leaseRows.empty() && leaseRows.front()[0].value().value_or("") == "t") {
            co_await DebugIdle::touch(c.redis(), debugKeys(id));
        }
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
            service::utils::redis::throwValue("read debug packets", reply);
        }
        ruvia::boxed_array<LinkDebugAcquisitionDto> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : reply.array()) {
            if (row.kind() != ruvia::redis_value::kind_type::array || row.array().size() != 3) {
                continue;
            }
            auto& acquisition = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
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
                auto& packet = packets.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
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
            ? std::string(link.template get<"edgeNodeId">()->view())
            : std::string{};
        if (link.template get<"execution">() && link.template get<"execution">()->view() == "edge") {
            co_await requireOwner(c, link.template get<"createdBy">()->view());
            if (enabled) {
                co_await requireEdgeDebugNode(c, edgeNodeId);
            }
        }
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        ruvia::db_query current(c.pool());
        auto currentBuilder = transaction.template repository<entities::LinkEntity>().query_builder();
        currentBuilder.select({ { "debug_enabled", entities::LinkEntity::column<"debug_enabled">().expression(current) } }).where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null()).expression(current)).set_lock(ruvia::db_lock_options{});
        const auto currentRows = co_await currentBuilder.get_raw_many();
        if (currentRows.empty()) {
            service::common::fail(15001, "链路不存在", 404);
        }
        const bool changed = (currentRows.front()[0].value().value_or("") == "t") != enabled;
        ruvia::db_query query(c.pool());
        auto queryBuilder = transaction.template repository<entities::LinkEntity>().update_builder();
        queryBuilder.set(entities::LinkEntity::column_name<"debug_enabled">(), query.value(enabled)).set(entities::LinkEntity::column_name<"updated_at">(), query.call("now")).where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null()).expression(query)).and_where(query.binary(entities::LinkEntity::column<"debug_enabled">().expression(query), ruvia::db_binary_operator::is_distinct_from, query.value(enabled))).returning({ { "id", entities::LinkEntity::column<"id">().expression(query) } });
        if (changed) {
            (void)co_await queryBuilder.get_raw_many();
            co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        }
        if (enabled) {
            co_await DebugIdle::touch(c.redis(), debugKeys(id));
        } else {
            co_await DebugIdle::clear(c.redis(), debugKeys(id));
        }
        co_await transaction.commit();
        if (enabled) {
            DebugIdle::watch(
                c.template worker_state<ruvia::web_worker_handle>(),
                debugKeys(id),
                std::string(id),
                [](ruvia::web_worker_context& ctx, std::string_view linkId) -> ruvia::task<void> {
                    co_await LinkService::instance().disableExpiredDebug(ctx, linkId);
                }
            );
        }
        if (changed && !edgeNodeId.empty() && (enabled || co_await edgeDebugNodeAvailable(c, edgeNodeId))) {
            try {
                (void)co_await service::edge_node::EdgeService::queueSnapshot(c, edgeNodeId, c.userId);
            } catch (...) {
                service::common::fail(15002, "调试设置已保存，下发未成功，请从边缘节点同步配置重试", 502);
            }
        }
    }

    template <typename Database>
    static auto expiredDebugQuery(Database&& database, std::pmr::memory_resource* resource, std::string_view id) {
        ruvia::db_query expressions(resource);
        auto query = database.template repository<entities::LinkEntity>().query_builder();
        query.select({
                         { "debug_enabled", entities::LinkEntity::column<"debug_enabled">().expression(expressions) },
                         { "created_by", entities::LinkEntity::column<"created_by">().expression(expressions) },
                         { "edge_node_id", db::emptyText(expressions, entities::LinkEntity::column_name<"edge_node_id">()) },
                     })
            .where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null()).expression(expressions));
        return query;
    }

    // 已授权的调试订阅保活时续租；租约已清除（调试已关闭）时不重新建立。
    template <typename Context>
    ruvia::task<void> extendDebugLease(Context& c, std::string_view id) {
        co_await DebugIdle::extend(c.redis(), debugKeys(id));
    }

    template <typename Context>
    ruvia::task<void> disableExpiredDebug(Context& c, std::string_view id) {
        if (co_await DebugIdle::leased(c.redis(), debugKeys(id))) {
            co_return;
        }
        auto lookup = expiredDebugQuery(c.db(), c.pool(), id);
        const auto rows = co_await lookup.get_raw_many();
        if (rows.empty() || rows.front()[0].value().value_or("") != "t") {
            co_return;
        }
        const auto actor = std::string(rows.front()[1].value().value_or(""));
        const auto edgeNodeId = std::string(rows.front()[2].value().value_or(""));
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        auto lockedLookup = expiredDebugQuery(transaction, c.pool(), id);
        lockedLookup.set_lock(ruvia::db_lock_options{});
        const auto current = co_await lockedLookup.get_raw_many();
        if (current.empty() || current.front()[0].value().value_or("") != "t") {
            co_await transaction.commit();
            co_return;
        }
        if (co_await DebugIdle::leased(c.redis(), debugKeys(id))) {
            co_await transaction.commit();
            co_return;
        }
        ruvia::db_query query(c.pool());
        auto queryBuilder = transaction.template repository<entities::LinkEntity>().update_builder();
        queryBuilder.set(entities::LinkEntity::column_name<"debug_enabled">(), query.value(false)).set(entities::LinkEntity::column_name<"updated_at">(), query.call("now")).where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null()).expression(query)).and_where(query.binary(entities::LinkEntity::column<"debug_enabled">().expression(query), ruvia::db_binary_operator::equal, query.value(true))).returning({ { "id", entities::LinkEntity::column<"id">().expression(query) } });
        const auto queryRows = co_await queryBuilder.get_raw_many();
        if (queryRows.empty()) {
            co_await transaction.commit();
            co_return;
        }
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        if (!edgeNodeId.empty() && service::common::isUuid(actor) && co_await edgeDebugNodeAvailable(c, edgeNodeId)) {
            (void)co_await service::edge_node::EdgeService::queueSnapshot(c, edgeNodeId, actor);
        }
    }

    template <typename Context>
    ruvia::task<void> update(Context& c, std::string_view id, const SaveLinkBody& body) {
        if (body.get<"execution">() && body.get<"execution">()->view() == "edge") {
            service::common::fail(15002, "边缘连接参数请在设备表单中配置", 400);
        }
        ruvia::db_query lookup(c.pool());
        auto lookupBuilder = c.db().template repository<entities::LinkEntity>().query_builder();
        lookupBuilder.select({ { "endpoint", db::jsonText(lookup, entities::LinkEntity::column<"endpoint">().expression(lookup), "mode") }, { "protocol", entities::LinkEntity::column<"protocol">().expression(lookup) }, { "created_by", entities::LinkEntity::column<"created_by">().expression(lookup) } }).where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null() && entities::LinkEntity::column<"execution">() == "collector").expression(lookup)).limit(1);
        const auto rows = co_await lookupBuilder.get_raw_many();
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
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        ruvia::db_query query(c.pool());
        auto queryBuilder = transaction.template repository<entities::LinkEntity>().update_builder();
        queryBuilder.set(entities::LinkEntity::column_name<"name">(), query.value(name)).set(entities::LinkEntity::column_name<"endpoint">(), query.cast(query.value(endpointJson), ruvia::db_data_type::jsonb)).set(entities::LinkEntity::column_name<"status">(), query.value(status)).set(entities::LinkEntity::column_name<"updated_at">(), query.call("now")).where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null() && entities::LinkEntity::column<"execution">() == "collector").expression(query));
        const auto nameChanged = query.binary(entities::LinkEntity::column<"name">().expression(query), ruvia::db_binary_operator::is_distinct_from, query.value(name));
        const auto endpointChanged = query.binary(
            entities::LinkEntity::column<"endpoint">().expression(query),
            ruvia::db_binary_operator::is_distinct_from,
            query.cast(query.value(endpointJson), ruvia::db_data_type::jsonb)
        );
        const auto statusChanged = query.binary(entities::LinkEntity::column<"status">().expression(query), ruvia::db_binary_operator::is_distinct_from, query.value(status));
        queryBuilder.and_where(query.binary(query.binary(nameChanged, ruvia::db_binary_operator::or_value, endpointChanged), ruvia::db_binary_operator::or_value, statusChanged));
        const auto updated = co_await queryBuilder.execute();
        if (updated.affected_rows() != 0) {
            co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        }
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> remove(Context& c, std::string_view id) {
        ruvia::db_find_options options;
        options.where_ = entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null();
        const auto link = co_await c.db().template repository<entities::LinkEntity>().find_one(options);
        if (!link) {
            service::common::fail(15001, "链路不存在", 404);
        }
        if (link->template get<"execution">() == "edge") {
            service::common::fail(15002, "边缘连接由设备配置管理，不能单独删除", 400);
        }
        co_await requireOwner(c, link->template get<"created_by">());
        ruvia::db_find_options used;
        used.where_ = service::device::entities::DeviceEntity::column<"link_id">() == id &&
            service::device::entities::DeviceEntity::column<"deleted_at">().is_null();
        if (co_await c.db().template repository<service::device::entities::DeviceEntity>().exists(used)) {
            service::common::fail(15008, "链路已被设备使用，请先删除关联设备", 409);
        }
        auto transaction = co_await c.db().begin_transaction();
        co_await transaction.advisory_lock(service::message::lock::kAcquisitionConfiguration);
        ruvia::db_query expressions(c.pool());
        const ruvia::db_assignment deletionChanges[]{
            { entities::LinkEntity::column_name<"deleted_at">(), expressions.call("now") },
            { entities::LinkEntity::column_name<"updated_at">(), expressions.call("now") }
        };
        (void)co_await transaction.template repository<entities::LinkEntity>().update(
            entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"deleted_at">().is_null(),
            deletionChanges
        );
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "link", "deleted", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
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
        const auto interfaceName = transport == "io" ? std::string{} : required(endpoint.get<"interfaceName">(), "请选择接口");
        validateEdgeEndpoint(endpoint, protocol, transport, interfaceName);
        ruvia::db_query nodeQuery(c.pool());
        auto nodeQueryBuilder = tx.template repository<service::edge_node::entities::EdgeNodeEntity>().query_builder();
        nodeQueryBuilder.select({ { "value0", db::integer(nodeQuery, 1) } }).where(nodeQuery.binary(service::edge_node::entities::EdgeNodeEntity::column<"id">().expression(nodeQuery), ruvia::db_binary_operator::equal, db::uuid(nodeQuery, nodeId))).and_where(nodeQuery.binary(service::edge_node::entities::EdgeNodeEntity::column<"enrollment_status">().expression(nodeQuery), ruvia::db_binary_operator::equal, nodeQuery.value("approved"))).and_where(nodeQuery.binary(db::jsonText(nodeQuery, service::edge_node::entities::EdgeNodeEntity::column<"capability">().expression(nodeQuery), "deviceConfig"), ruvia::db_binary_operator::equal, nodeQuery.value("true")));
        if (protocol == "MC" || protocol == "FINS" || protocol == "DLT645" || protocol == "ExternalIO") {
            nodeQueryBuilder.and_where(nodeQuery.call("jsonb_exists", { db::jsonValue(nodeQuery, service::edge_node::entities::EdgeNodeEntity::column<"capability">().expression(nodeQuery), "protocols"), nodeQuery.value(protocol) }));
        }
        const auto node = co_await nodeQueryBuilder.get_raw_many();
        if (node.empty()) {
            service::common::fail(15002, "节点未批准或不支持采集配置", 400);
        }
        if (transport == "serial") {
            ruvia::db_query serialQuery(c.pool());
            auto serialQueryBuilder = tx.template repository<service::edge_node::entities::EdgeNodeSerialEntity>().query_builder();
            serialQueryBuilder.select({ { "value0", db::integer(serialQuery, 1) } }).where(serialQuery.binary(service::edge_node::entities::EdgeNodeSerialEntity::column<"node_id">().expression(serialQuery), ruvia::db_binary_operator::equal, db::uuid(serialQuery, nodeId))).and_where(serialQuery.binary(service::edge_node::entities::EdgeNodeSerialEntity::column<"path">().expression(serialQuery), ruvia::db_binary_operator::equal, serialQuery.value(interfaceName))).and_where(serialQuery.unary(ruvia::db_unary_operator::is_true, service::edge_node::entities::EdgeNodeSerialEntity::column<"available">().expression(serialQuery)));
            const auto serial = co_await serialQueryBuilder.get_raw_many();
            if (serial.empty()) {
                service::common::fail(15002, "所选串口不存在或当前不可用", 409);
            }
        } else if (transport == "tcp") {
            ruvia::db_query networkQuery(c.pool());
            auto networkQueryBuilder = tx.template repository<service::edge_node::entities::EdgeNodeInterfaceEntity>().query_builder();
            networkQueryBuilder.select({ { "ipv4", service::edge_node::entities::EdgeNodeInterfaceEntity::column<"ipv4">().expression(networkQuery) } }).where(networkQuery.binary(service::edge_node::entities::EdgeNodeInterfaceEntity::column<"node_id">().expression(networkQuery), ruvia::db_binary_operator::equal, db::uuid(networkQuery, nodeId))).and_where(networkQuery.binary(service::edge_node::entities::EdgeNodeInterfaceEntity::column<"name">().expression(networkQuery), ruvia::db_binary_operator::equal, networkQuery.value(interfaceName))).and_where(networkQuery.binary(networkQuery.coalesce({ service::edge_node::entities::EdgeNodeInterfaceEntity::column<"ipv4">().expression(networkQuery), networkQuery.value("") }), ruvia::db_binary_operator::not_equal, networkQuery.value(""))).limit(1);
            const auto network = co_await networkQueryBuilder.get_raw_many();
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
        std::string endpointJson = "{\"transport\":\"";
        service::utils::appendJsonEscaped(endpointJson, transport);
        endpointJson += '"';
        if (transport != "io") {
            endpointJson += ",\"interface\":\"";
            service::utils::appendJsonEscaped(endpointJson, interfaceName);
            endpointJson += '"';
        }
        if (transport == "serial") {
            endpointJson += ",\"baud_rate\":" + std::to_string(endpoint.get<"baudRate">().value_or(9600));
            endpointJson += ",\"data_bits\":" + std::to_string(endpoint.get<"dataBits">().value_or(8));
            endpointJson += ",\"stop_bits\":" + std::to_string(endpoint.get<"stopBits">().value_or(1));
            endpointJson += ",\"parity\":\"";
            service::utils::appendJsonEscaped(endpointJson, endpoint.get<"parity">() ? endpoint.get<"parity">()->view() : std::string_view("none"));
            endpointJson += endpoint.get<"rs485">().value_or(false) ? "\",\"rs485\":true" : "\",\"rs485\":false";
        } else if (transport == "tcp") {
            endpointJson += ",\"mode\":\"";
            service::utils::appendJsonEscaped(endpointJson, endpoint.get<"mode">()->view());
            endpointJson += "\",\"ip\":\"";
            service::utils::appendJsonEscaped(endpointJson, endpoint.get<"ip">()->view());
            endpointJson += "\",\"port\":" + std::to_string(*endpoint.get<"port">());
        }
        endpointJson += '}';
        // Serialize endpoint resolution within a node. IO devices own logical endpoints.
        ruvia::db_query nodeLock(c.pool());
        auto nodeLockBuilder = tx.template repository<service::edge_node::entities::EdgeNodeEntity>().query_builder();
        nodeLockBuilder.select({ { "id", service::edge_node::entities::EdgeNodeEntity::column<"id">().expression(nodeLock) } }).where(nodeLock.binary(service::edge_node::entities::EdgeNodeEntity::column<"id">().expression(nodeLock), ruvia::db_binary_operator::equal, db::uuid(nodeLock, nodeId)));
        nodeLockBuilder.set_lock(ruvia::db_lock_options{});
        (void)co_await nodeLockBuilder.get_raw_many();
        ruvia::db_query existing(c.pool());
        const auto existingEndpoint = entities::LinkEntity::column<"endpoint">().expression(existing);
        auto sameSettings = (entities::LinkEntity::column<"protocol">() == protocol).expression(existing);
        auto compareSetting = [&](std::string_view key, std::string_view value, std::string_view fallback = "") {
            sameSettings = existing.binary(sameSettings, ruvia::db_binary_operator::and_value, existing.binary(existing.coalesce({ db::jsonText(existing, existingEndpoint, key), existing.value(fallback) }), ruvia::db_binary_operator::equal, existing.value(value)));
        };
        if (transport == "serial") {
            compareSetting("baud_rate", std::to_string(endpoint.get<"baudRate">().value_or(9600)), "9600");
            compareSetting("data_bits", std::to_string(endpoint.get<"dataBits">().value_or(8)), "8");
            compareSetting("stop_bits", std::to_string(endpoint.get<"stopBits">().value_or(1)), "1");
            compareSetting("parity", endpoint.get<"parity">() ? endpoint.get<"parity">()->view() : std::string_view("none"), "none");
            compareSetting("rs485", endpoint.get<"rs485">().value_or(false) ? "true" : "false", "false");
        }
        auto existingBuilder = tx.template repository<entities::LinkEntity>().query_builder();
        existingBuilder.select({ { "id", db::text(existing, entities::LinkEntity::column<"id">().expression(existing)) }, { "value1", sameSettings }, { "protocol", entities::LinkEntity::column<"protocol">().expression(existing) } }).where((entities::LinkEntity::column<"execution">() == "edge" && entities::LinkEntity::column<"edge_node_id">() == nodeId && entities::LinkEntity::column<"deleted_at">().is_null()).expression(existing));
        existingBuilder.and_where(existing.binary(db::jsonText(existing, existingEndpoint, "transport"), ruvia::db_binary_operator::equal, existing.value(transport)));
        if (transport != "io") {
            existingBuilder.and_where(existing.binary(db::jsonText(existing, existingEndpoint, "interface"), ruvia::db_binary_operator::equal, existing.value(interfaceName)));
        }
        if (transport == "tcp") {
            existingBuilder.and_where(existing.binary(db::jsonText(existing, existingEndpoint, "mode"), ruvia::db_binary_operator::equal, existing.value(endpoint.get<"mode">()->view()))).and_where(existing.binary(db::jsonText(existing, existingEndpoint, "ip"), ruvia::db_binary_operator::equal, existing.value(endpoint.get<"ip">()->view()))).and_where(existing.binary(db::jsonText(existing, existingEndpoint, "port"), ruvia::db_binary_operator::equal, existing.value(std::to_string(*endpoint.get<"port">()))));
        } else if (transport == "io") {
            if (deviceId.empty()) {
                existingBuilder.and_where(existing.value(false));
            } else {
                ruvia::db_query device(c.pool());
                device.select(device.value(1)).from(service::device::entities::DeviceEntity::table_name(), "io_device").where(device.binary(service::device::entities::DeviceEntity::column<"id">().expression(device, "io_device"), ruvia::db_binary_operator::equal, db::uuid(device, deviceId))).and_where(device.binary(service::device::entities::DeviceEntity::column<"link_id">().expression(device, "io_device"), ruvia::db_binary_operator::equal, entities::LinkEntity::column<"id">().expression(device, "link")));
                existingBuilder.and_where(existing.exists(device));
            }
        }
        existingBuilder.limit(1);
        const auto matches = co_await existingBuilder.get_raw_many();
        if (!matches.empty()) {
            const auto id = std::string(matches.front()[0].value().value_or(""));
            const bool same = matches.front()[1].value().value_or("") == "t";
            if (!same) {
                ruvia::db_query attached(c.pool());
                auto attachedBuilder = tx.template repository<service::device::entities::DeviceEntity>().query_builder();
                attachedBuilder.select({ { "value0", attached.value(1) } }).where((service::device::entities::DeviceEntity::column<"link_id">() == id && service::device::entities::DeviceEntity::column<"deleted_at">().is_null()).expression(attached));
                if (!deviceId.empty() && matches.front()[2].value().value_or("") == protocol) {
                    attachedBuilder.and_where((service::device::entities::DeviceEntity::column<"id">() != deviceId).expression(attached));
                }
                const auto attachedRows = co_await attachedBuilder.get_raw_many();
                if (!attachedRows.empty()) {
                    service::common::fail(15002, "该接口已被设备使用，连接协议及共享串口参数必须一致", 409);
                }
            }
            ruvia::db_query update(c.pool());
            auto updateBuilder = tx.template repository<entities::LinkEntity>().update_builder();
            updateBuilder.set(entities::LinkEntity::column_name<"status">(), update.value("enabled")).where((entities::LinkEntity::column<"id">() == id).expression(update));
            if (!same) {
                updateBuilder.set(entities::LinkEntity::column_name<"protocol">(), update.value(protocol)).set(entities::LinkEntity::column_name<"endpoint">(), update.cast(update.value(endpointJson), ruvia::db_data_type::jsonb)).set(entities::LinkEntity::column_name<"updated_at">(), update.call("now"));
            } else {
                updateBuilder.and_where((entities::LinkEntity::column<"status">() != "enabled").expression(update));
            }
            (void)co_await updateBuilder.execute();
            co_return id;
        }
        const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        auto nameEnd = std::min<std::size_t>(name.size(), 60);
        while (nameEnd < name.size() && nameEnd > 0 && (static_cast<unsigned char>(name[nameEnd]) & 0xc0) == 0x80) {
            --nameEnd;
        }
        const auto endpointName = name.substr(0, nameEnd) + " [" + id + "]";
        ruvia::db_query query(c.pool());
        const std::array<std::string_view, 8> queryColumns{ entities::LinkEntity::column_name<"id">(), entities::LinkEntity::column_name<"name">(), entities::LinkEntity::column_name<"protocol">(), entities::LinkEntity::column_name<"endpoint">(), entities::LinkEntity::column_name<"status">(), entities::LinkEntity::column_name<"created_by">(), entities::LinkEntity::column_name<"execution">(), entities::LinkEntity::column_name<"edge_node_id">() };
        auto queryBuilder = tx.template repository<entities::LinkEntity>().insert_builder();
        ruvia::db_values_builder queryValues(queryColumns);
        queryValues.row({ db::uuid(query, id), query.value(endpointName), query.value(protocol), query.cast(query.value(endpointJson), ruvia::db_data_type::jsonb), query.value("enabled"), db::uuid(query, c.userId), query.value("edge"), db::uuid(query, nodeId) });
        queryBuilder.insert_from(queryColumns, queryValues.query());
        (void)co_await queryBuilder.execute();
        co_return id;
    }

    template <typename Context>
    ruvia::task<void> retireUnusedDeviceEndpoint(Context& c, ruvia::db_transaction& tx, std::string_view id) {
        ruvia::db_query attached(c.pool());
        attached.select(attached.value(1)).from(service::device::entities::DeviceEntity::table_name()).where((service::device::entities::DeviceEntity::column<"link_id">() == id && service::device::entities::DeviceEntity::column<"deleted_at">().is_null()).expression(attached));
        ruvia::db_query disable(c.pool());
        auto disableBuilder = tx.template repository<entities::LinkEntity>().update_builder();
        disableBuilder.set(entities::LinkEntity::column_name<"status">(), disable.value("disabled")).where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"execution">() == "edge" && entities::LinkEntity::column<"status">() != "disabled").expression(disable)).and_where(disable.unary(ruvia::db_unary_operator::not_value, disable.exists(attached)));
        (void)co_await disableBuilder.execute();
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
        if (protocol == "ExternalIO" || transport == "io") {
            if (protocol != "ExternalIO" || transport != "io" || endpoint.get<"interfaceName">() || endpoint.get<"mode">() || endpoint.get<"ip">() || endpoint.get<"port">() || endpoint.get<"targets">() || endpoint.get<"baudRate">() || endpoint.get<"dataBits">() || endpoint.get<"stopBits">() || endpoint.get<"parity">() || endpoint.get<"rs485">()) {
                service::common::fail(15002, "独立外部 IO 不使用网络或串口参数", 400);
            }
            return;
        }
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
        if (protocol == "MQTT" && mode != "TCP Client") {
            service::common::fail(15003, "MQTT Client 只支持 TCP Client 模式", 400);
        }
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
            if (protocol == "MQTT" && (!mqtt || mqtt->template get<"clientId">().view().empty())) {
                service::common::fail(15003, "MQTT 目标必须配置 Client ID", 400);
            }
            if (mqtt && mqtt->template get<"password">() && !mqtt->template get<"password">()->view().empty() &&
                (!mqtt->template get<"username">() || mqtt->template get<"username">()->view().empty())) {
                service::common::fail(15003, "MQTT 密码需要同时配置用户名", 400);
            }
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
                if (!error) {
                    return true;
                }
                std::string_view host = targetIp;
                if (host.ends_with('.')) {
                    host.remove_suffix(1);
                }
                if (host.empty() || host.size() > 253) {
                    return false;
                }
                for (const auto label : service::utils::splitPayload(host, ".")) {
                    if (label.empty() || label.size() > 63 || label.front() == '-' || label.back() == '-') {
                        return false;
                    }
                    for (const unsigned char ch : label) {
                        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-')) {
                            return false;
                        }
                    }
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

    static DebugIdleKeys debugKeys(std::string_view id) {
        return { entities::debugLeaseKey(id), entities::debugWatchKey(id) };
    }

    static ruvia::db_expression displayName(ruvia::db_query& query) {
        static constexpr std::string_view kNodeAlias = "edge_link_node";
        ruvia::db_query node(query.resource());
        node.select(node.coalesce({ node.null_if(service::edge_node::entities::EdgeNodeEntity::column<"name">().expression(node, kNodeAlias), node.value("")), node.null_if(service::edge_node::entities::EdgeNodeEntity::column<"hostname">().expression(node, kNodeAlias), node.value("")), service::edge_node::entities::EdgeNodeEntity::column<"imei">().expression(node, kNodeAlias) }))
            .from(service::edge_node::entities::EdgeNodeEntity::table_name(), kNodeAlias)
            .where(node.binary(service::edge_node::entities::EdgeNodeEntity::column<"id">().expression(node, kNodeAlias), ruvia::db_binary_operator::equal, entities::LinkEntity::column<"edge_node_id">().expression(node)));
        return query.case_when({ { (entities::LinkEntity::column<"execution">() == "edge").expression(query), query.call("concat", { query.coalesce({ query.subquery(node), query.value("边缘节点") }), db::text(query, " · "), entities::LinkEntity::column<"protocol">().expression(query) }) } }, entities::LinkEntity::column<"name">().expression(query));
    }

    template <typename Context>
    static ruvia::task<bool> edgeDebugNodeAvailable(Context& c, std::string_view nodeId) {
        if (!service::common::isUuid(nodeId)) {
            co_return false;
        }
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::edge_node::entities::EdgeNodeEntity>().query_builder();
        queryBuilder.select({ { "enrollment_status", service::edge_node::entities::EdgeNodeEntity::column<"enrollment_status">().expression(query) }, { "capability", db::jsonText(query, service::edge_node::entities::EdgeNodeEntity::column<"capability">().expression(query), "deviceConfig") } }).where((service::edge_node::entities::EdgeNodeEntity::column<"id">() == nodeId).expression(query));
        const auto rows = co_await queryBuilder.get_raw_many();
        co_return !rows.empty() && rows.front()[0].value().value_or("") == "approved" &&
            rows.front()[1].value().value_or("") == "true";
    }

    template <typename Context>
    static ruvia::task<void> requireEdgeDebugNode(Context& c, std::string_view nodeId) {
        if (!co_await edgeDebugNodeAvailable(c, nodeId)) {
            service::common::fail(15002, "节点未批准或不支持链路调试配置", 409);
        }
    }

    // Change-feed ids come from producers outside this request; only UUIDs reach
    // SQL casts. nullopt when no valid id remains.
    static std::optional<ruvia::db_expression> uuidList(ruvia::db_query& query, const std::vector<std::string>& ids) {
        std::vector<ruvia::db_expression> values;
        for (const auto& id : ids) {
            if (service::common::isUuid(id)) {
                values.push_back(query.cast(query.value(id), ruvia::db_data_type::uuid));
            }
        }
        if (values.empty()) {
            return std::nullopt;
        }
        return query.list(values);
    }

    template <typename Database>
    static auto linkSelect(Database&& database, std::pmr::memory_resource* resource) {
        ruvia::db_query query(resource);
        const auto endpoint = entities::LinkEntity::column<"endpoint">().expression(query);
        const auto endpointPort = query.coalesce(
            { query.null_if(db::jsonText(query, endpoint, "port"), query.value("")),
              query.value("0") }
        );
        auto queryBuilder = database.template repository<entities::LinkEntity>().query_builder();
        queryBuilder.select({ { "id", db::text(query, entities::LinkEntity::column<"id">().expression(query)) }, { "value1", displayName(query) }, { "protocol", entities::LinkEntity::column<"protocol">().expression(query) }, { "value3", db::jsonText(query, endpoint, "mode") }, { "value4", query.coalesce({ db::jsonText(query, endpoint, "ip"), query.value("") }) }, { "value5", endpointPort }, { "status", entities::LinkEntity::column<"status">().expression(query) }, { "created_by", db::text(query, entities::LinkEntity::column<"created_by">().expression(query)) }, { "created_at", query.call("iot_utc_timestamp", { entities::LinkEntity::column<"created_at">().expression(query) }) }, { "updated_at", query.call("iot_utc_timestamp", { entities::LinkEntity::column<"updated_at">().expression(query) }) }, { "execution", entities::LinkEntity::column<"execution">().expression(query) }, { "edge_node_id", db::emptyText(query, entities::LinkEntity::column_name<"edge_node_id">()) }, { "debug_enabled", entities::LinkEntity::column<"debug_enabled">().expression(query) } });
        return queryBuilder;
    }

    static void applyFilters(auto& target, ruvia::db_query& query, const std::optional<std::string>& keyword, const std::optional<std::string>& mode, const std::optional<std::string>& protocol, const std::optional<std::string>& status) {
        target.where(entities::LinkEntity::column<"deleted_at">().is_null().expression(query));
        static constexpr std::string_view kDeviceAlias = "linked_device";
        ruvia::db_query devices(query.resource());
        devices.select(devices.value(1)).from(service::device::entities::DeviceEntity::table_name(), kDeviceAlias).where(devices.binary(service::device::entities::DeviceEntity::column<"link_id">().expression(devices, kDeviceAlias), ruvia::db_binary_operator::equal, entities::LinkEntity::column<"id">().expression(devices))).and_where(devices.unary(ruvia::db_unary_operator::is_null, service::device::entities::DeviceEntity::column<"deleted_at">().expression(devices, kDeviceAlias)));
        target.and_where(query.binary((entities::LinkEntity::column<"execution">() == "collector").expression(query), ruvia::db_binary_operator::or_value, query.exists(devices)));

        if (keyword && !keyword->empty()) {
            target.and_where(query.binary(displayName(query), ruvia::db_binary_operator::i_like, query.value("%" + *keyword + "%")));
        }
        if (mode && !mode->empty()) {
            target.and_where(query.binary(db::jsonText(query, entities::LinkEntity::column<"endpoint">().expression(query), "mode"), ruvia::db_binary_operator::equal, query.value(*mode)));
        }
        if (protocol && !protocol->empty()) {
            target.and_where((entities::LinkEntity::column<"protocol">() == *protocol).expression(query));
        }
        if (status && !status->empty()) {
            target.and_where((entities::LinkEntity::column<"status">() == *status).expression(query));
        }
    }

    template <typename Context>
    static ruvia::task<void> fillEndpoint(Context& c, LinkEndpointDto& endpoint, std::string_view id) {
        ruvia::db_query query(c.pool());
        const auto stored = entities::LinkEntity::column<"endpoint">().expression(query);
        auto queryBuilder = c.db().template repository<entities::LinkEntity>().query_builder();
        queryBuilder.select({ { "value0", db::jsonText(query, stored, "transport") }, { "value1", db::jsonText(query, stored, "interface") }, { "value2", db::jsonText(query, stored, "baud_rate") }, { "value3", db::jsonText(query, stored, "data_bits") }, { "value4", db::jsonText(query, stored, "stop_bits") }, { "value5", db::jsonText(query, stored, "parity") }, { "value6", db::jsonText(query, stored, "rs485") } }).where((entities::LinkEntity::column<"id">() == id && entities::LinkEntity::column<"execution">() == "edge").expression(query)).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
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
            clients.emplace_back(clientText.substr(start, end == std::string::npos ? std::string::npos : end - start), ruvia::model_options{ .resource_ = c.arena() });
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
    ruvia::task<ruvia::boxed_array<LinkTargetDto>> loadTargets(Context& c, std::string_view id, const entities::LinkRuntimeRecord& runtime) {
        // jsonb_array_elements WITH ORDINALITY 输出列（非实体列），按别名 value(target, position) 引用。
        static constexpr std::string_view kElements = "value";
        static constexpr std::string_view kTarget = "target";
        static constexpr std::string_view kPosition = "position";
        ruvia::db_query query(c.pool());
        const auto targets = query.coalesce(
            { db::jsonValue(query, entities::LinkEntity::column<"endpoint">().expression(query), "targets"),
              query.cast(query.value("[]"), ruvia::db_data_type::jsonb) }
        );
        const auto target = query.column(kTarget, kElements);
        auto queryBuilder = c.db().template repository<entities::LinkEntity>().query_builder();

        const std::array<ruvia::db_source_column, 2> sourceColumns1{ { { .name_ = kTarget }, { .name_ = kPosition } } };
        queryBuilder.select({ { "value0", db::jsonText(query, target, "id") }, { "value1", db::jsonText(query, target, "name") }, { "value2", db::jsonText(query, target, "ip") }, { "value3", db::jsonText(query, target, "port") }, { "value4", db::jsonText(query, target, "status") }, { "value5", db::jsonText(query, target, "mqtt") } }).join_function(ruvia::db_join_type::cross, query.call("jsonb_array_elements", { targets }), kElements, {}, { .lateral_ = true, .with_ordinality_ = true, .columns_ = sourceColumns1 }).where(query.binary(entities::LinkEntity::column<"id">().expression(query), ruvia::db_binary_operator::equal, query.value(id))).order_by(query.column(kPosition, kElements));
        const auto rows = co_await queryBuilder.get_raw_many();
        ruvia::boxed_array<LinkTargetDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& target = result.emplace_back(ruvia::model_options{ .resource_ = c.arena() });
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
                {
                    target.set<"mqtt">(std::move(mqtt));
                }
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
        if (reason == "target_owned") {
            return -1;
        }
        if (state == "connected") {
            return 6;
        }
        if (state == "listening") {
            return 5;
        }
        if (state == "reconnecting") {
            return 4;
        }
        if (state == "connecting") {
            return 3;
        }
        if (state == "error") {
            return 2;
        }
        if (state == "idle") {
            return 1;
        }
        return 0;
    }

    static void mergeRuntimeState(entities::LinkRuntimeRecord& aggregate, const entities::LinkRuntimeRecord& worker, const std::string& prefix = {}) {
        const auto stateKey = prefix + "state";
        const auto reasonKey = prefix.empty() ? "state_reason" : prefix + "reason";
        const auto updatedKey = prefix + "updated_at_ms";
        const auto rank = runtimeStateRank(worker.text(stateKey), worker.text(reasonKey));
        const auto currentRank = runtimeStateRank(aggregate.text(stateKey), aggregate.text(reasonKey));
        if (aggregate.fields.contains(stateKey) &&
            (rank < currentRank || (rank == currentRank && worker.integer("updated_at_ms") <= aggregate.integer(updatedKey)))) {
            return;
        }

        // 状态、原因、错误和活动时间必须来自同一份 Worker 快照。
        aggregate.fields[stateKey] = worker.text(stateKey, "stopped");
        aggregate.fields[reasonKey] = worker.text(reasonKey);
        aggregate.fields[prefix + "error"] = worker.text(prefix + "error");
        aggregate.fields[prefix + "last_activity_at_ms"] = worker.text(prefix + "last_activity_at_ms", "0");
        aggregate.fields[updatedKey] = worker.text("updated_at_ms", "0");
    }

    static void normalizeStandbyState(entities::LinkRuntimeRecord& status, const std::string& prefix = {}) {
        const auto reasonKey = prefix.empty() ? "state_reason" : prefix + "reason";
        if (status.text(reasonKey) != "target_owned") {
            return;
        }
        // 尚未收到持有者快照时显示连接中，内部认领竞争不代表目标连接失败。
        status.fields[prefix + "state"] = "connecting";
        status.fields[reasonKey].clear();
        status.fields[prefix + "error"].clear();
    }

    template <typename Context>
    static ruvia::task<entities::LinkRuntimeRecord> loadRuntimeStatus(Context& c, std::string_view id) {
        entities::LinkRuntimeRecord status;
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
                entities::LinkRuntimeRecord worker;
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
                    if (!name.starts_with("target:") || !name.ends_with(":state")) {
                        continue;
                    }
                    const auto prefix = name.substr(0, name.size() - 5);
                    targetPrefixes.insert(prefix);
                    mergeRuntimeState(status, worker, prefix);
                }
            }
            normalizeStandbyState(status);
            for (const auto& prefix : targetPrefixes) {
                normalizeStandbyState(status, prefix);
            }
            status.fields["connection_count"] = std::to_string(connectionCount);
            status.fields["clients"] = std::move(clients);
            status.fields["last_activity_at_ms"] = std::to_string(lastActivityAt);
        } catch (const std::exception&) {
        }
        co_return status;
    }

    template <typename Targets>
    static std::string serializeTargets(const Targets& targets) {
        std::string result = "[";
        for (const auto& target : targets) {
            if (result.size() > 1) {
                result.push_back(',');
            }
            result += "{\"id\":\"";
            service::utils::appendJsonEscaped(result, target.template get<"id">().view());
            result += "\",\"name\":\"";
            service::utils::appendJsonEscaped(result, target.template get<"name">().view());
            result += "\",\"ip\":\"";
            service::utils::appendJsonEscaped(result, target.template get<"ip">().view());
            result += "\",\"port\":" +
                std::to_string(static_cast<std::int64_t>(target.template get<"port">()));
            result += ",\"status\":\"";
            const auto& status = target.template get<"status">();
            service::utils::appendJsonEscaped(result, status ? status->view() : "enabled");
            result.push_back('"');
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
        std::string result = "{\"transport\":\"tcp\",\"mode\":\"";
        service::utils::appendJsonEscaped(result, mode);
        result += "\",\"ip\":\"";
        service::utils::appendJsonEscaped(result, mode == "TCP Server" ? ip : "");
        result += "\",\"port\":" + std::to_string(mode == "TCP Server" ? port : 0);
        result += ",\"targets\":";
        result += mode == "TCP Client" ? serializeTargets(targets) : "[]";
        result.push_back('}');
        return result;
    }

    template <typename Context>
    ruvia::task<void> ensureAvailable(Context& c, const std::string& name, const std::string& mode, const std::string& ip, std::int64_t port, std::optional<std::string> excludedId) {
        ruvia::db_query query(c.pool());
        const auto endpoint = entities::LinkEntity::column<"endpoint">().expression(query);
        const auto endpointPort = db::jsonText(query, endpoint, "port");
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
        const auto nameTaken = query.binary(entities::LinkEntity::column<"name">().expression(query), ruvia::db_binary_operator::equal, query.value(name));
        auto queryBuilder = c.db().template repository<entities::LinkEntity>().query_builder();
        queryBuilder.select({ { "value0", db::integer(query, 1) } }).where(entities::LinkEntity::column<"deleted_at">().is_null().expression(query));
        if (mode == "TCP Server") {
            const auto serverAddress = query.binary(
                query.binary(entities::LinkEntity::column<"execution">().expression(query), ruvia::db_binary_operator::equal, query.value("collector")),
                ruvia::db_binary_operator::and_value,
                query.binary(
                    query.binary(db::jsonText(query, endpoint, "mode"), ruvia::db_binary_operator::equal, query.value(mode)),
                    ruvia::db_binary_operator::and_value,
                    query.binary(
                        query.binary(db::jsonText(query, endpoint, "ip"), ruvia::db_binary_operator::equal, query.value(ip)),
                        ruvia::db_binary_operator::and_value,
                        query.binary(safeEndpointPort, ruvia::db_binary_operator::equal, query.value(port))
                    )
                )
            );
            queryBuilder.and_where(query.binary(nameTaken, ruvia::db_binary_operator::or_value, serverAddress));
        } else {
            queryBuilder.and_where(nameTaken);
        }
        if (excludedId) {
            queryBuilder.and_where(query.binary(entities::LinkEntity::column<"id">().expression(query), ruvia::db_binary_operator::not_equal, db::uuid(query, *excludedId)));
        }
        queryBuilder.limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (!rows.empty()) {
            service::common::fail(15005, "链路名称或监听地址已存在", 409);
        }
    }

    template <typename Context>
    ruvia::task<void> requireOwner(Context& c, std::string_view ownerId) {
        if (c.userId == ownerId) {
            co_return;
        }
        static constexpr std::string_view kUserRoleAlias = "ur";
        static constexpr std::string_view kRoleAlias = "r";
        ruvia::db_query query(c.pool());
        auto queryBuilder = c.db().template repository<service::user::entities::UserRoleEntity>().query_builder(kUserRoleAlias);
        queryBuilder.select({ { "value0", db::integer(query, 1) } }).template join<service::role::entities::RoleEntity>(ruvia::db_join_type::inner, kRoleAlias, query.binary(service::user::entities::UserRoleEntity::column<"role_id">().expression(query, kUserRoleAlias), ruvia::db_binary_operator::equal, service::role::entities::RoleEntity::column<"id">().expression(query, kRoleAlias))).where(query.binary(service::user::entities::UserRoleEntity::column<"user_id">().expression(query, kUserRoleAlias), ruvia::db_binary_operator::equal, query.value(c.userId))).and_where(query.binary(service::role::entities::RoleEntity::column<"code">().expression(query, kRoleAlias), ruvia::db_binary_operator::equal, query.value("superadmin"))).and_where(query.binary(service::role::entities::RoleEntity::column<"status">().expression(query, kRoleAlias), ruvia::db_binary_operator::equal, query.value("enabled"))).and_where(query.unary(ruvia::db_unary_operator::is_null, service::role::entities::RoleEntity::column<"deleted_at">().expression(query, kRoleAlias))).limit(1);
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            service::common::fail(15007, "只能修改或删除自己创建的链路", 403);
        }
    }

    std::string cachedPublicIp_;
    std::chrono::steady_clock::time_point publicIpCachedAt_{};
};

} // namespace service::link
