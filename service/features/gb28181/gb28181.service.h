#pragma once

#include <tuple>

#include "service/common/uuid.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/timer.h>
#include "service/features/gb28181/media/media.transport.h"

#include "service/features/gb28181/gb28181.config.h"
#include "service/features/gb28181/gb28181.protocol.h"

#include "service/features/gb28181/gb28181.entity.h"

#include <atomic>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ruvia/web/web_worker.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/gb28181/gb28181.types.h"

namespace service::gb28181 {

class GbProjectionService {
  public:
    static ruvia::task<bool> projectionOwnerMatches(const ruvia::redis_handle& redis, std::string_view aggregateId, bool online, std::string_view ownerToken) {
        if (ownerToken.empty()) {
            co_return false;
        }
        const auto current = co_await redis.get(
            control_protocol::stream::owner(aggregateId)
        );
        if (online) {
            co_return current&& std::string_view(*current) == ownerToken;
        }
        // A legitimate offline event removes its owner key.  An old offline event
        // must not overwrite a newer online owner.
        co_return !current || std::string_view(*current) == ownerToken;
    }

    template <typename Redis>
    static ruvia::task<void> publishConfig(const Redis& redis, const AppConfig& config) {
        // Configuration is immutable for the process lifetime.  Service Workers read
        // the enabled flag from this projection instead of a process-global runtime.
        const ConfigProjectionRecord record{ .enabled = config.enabled };
        co_await service::message::redis::setHash(redis, control_protocol::stream::kConfigKey, record.encode());
        co_return;
    }

    static ruvia::task<bool> configuredEnabled(const ruvia::redis_handle& redis) {
        const auto value = co_await redis.hget(control_protocol::stream::kConfigKey, "enabled");
        co_return value && ConfigProjectionRecord::enabledValue(std::string_view(*value));
    }

#ifdef IOT_ENGINE_TESTING
    static int integerForTest(std::string_view value, int fallback = 0) {
        return integer(value, fallback);
    }
#endif

    // Northbound queries read the durable Service projection directly.  The
    // Collector never exposes its in-memory registry to HTTP workers.
    static ruvia::task<ProjectionSnapshot> loadSnapshot(ruvia::web_worker_context& context) {
        co_return co_await hydrate(context);
    }

    // Every Service Worker runs the same reconciliation pass.  There is no
    // worker-index partition: the PostgreSQL advisory lock serializes two
    // workers that notice the same expired lease, while the second Redis GET
    // is performed after that lock to avoid clearing a newly acquired owner.
    static ruvia::task<void>
    reconcileExpiredOwners(ruvia::web_worker_context& context) {
        ruvia::db_query deviceQuery(context.pool());
        deviceQuery.select(deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"id">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()))
            .from(service::gb28181::persistence::Gb28181DeviceEntity::table_name())
            .where(deviceQuery.binary(
                deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"online">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                deviceQuery.value(true)))
            .order_by(deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"id">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()));
        const auto devices = co_await context.db().query(deviceQuery);
        for (const auto& row : devices) {
            const auto id = std::string(row[0].value().value_or(std::string_view{}));
            if (id.empty()) {
                continue;
            }
            const auto ownerKey = "iot:gb28181:owner:" + id;
            if (co_await context.redis().get(ownerKey)) {
                continue;
            }

            auto transaction = co_await context.db().begin_transaction();
            (void)co_await transaction.query(advisoryLockQuery(context.pool(), id));
            ruvia::db_query currentQuery(context.pool());
            currentQuery.select(currentQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"online">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()))
                .from(service::gb28181::persistence::Gb28181DeviceEntity::table_name())
                .where(currentQuery.binary(
                    currentQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"id">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                    currentQuery.value(id)))
                .lock({.mode_ = ruvia::db_row_lock::update});
            const auto current = co_await transaction.query(currentQuery);
            if (current.empty() ||
                !boolean(current.front()[0].value().value_or(std::string_view{}))) {
                co_await transaction.commit();
                continue;
            }
            if (co_await context.redis().get(ownerKey)) {
                co_await transaction.commit();
                continue;
            }
            ruvia::db_query updateQuery(context.pool());
            updateQuery.update(service::gb28181::persistence::Gb28181DeviceEntity::table_name())
                .set(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"online">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view(), updateQuery.value(false))
                .set(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"updated_at">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view(), updateQuery.call("now"))
                .where(updateQuery.binary(
                    updateQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"id">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                    updateQuery.value(id)))
                .and_where(updateQuery.binary(
                    updateQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"online">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                    updateQuery.value(true)));
            (void)co_await transaction.execute(updateQuery);
            co_await transaction.commit();
        }

        ruvia::db_query streamQuery(context.pool());
        streamQuery
            .select({streamQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"app">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()), streamQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"stream">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                     streamQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"schema">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view())})
            .from(service::gb28181::persistence::Gb28181StreamEntity::table_name())
            .where(streamQuery.binary(
                streamQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"online">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                streamQuery.value(true)))
            .order_by(streamQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"app">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()))
            .add_order_by(streamQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"stream">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()))
            .add_order_by(streamQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"schema">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()));
        const auto streams = co_await context.db().query(streamQuery);
        for (const auto& row : streams) {
            const auto app = std::string(row[0].value().value_or(std::string_view{}));
            const auto stream =
                std::string(row[1].value().value_or(std::string_view{}));
            const auto schema =
                std::string(row[2].value().value_or(std::string_view{}));
            if (app.empty() || stream.empty() || schema.empty()) {
                continue;
            }
            const auto identity = StreamStatus::identity(app, stream, schema);
            const auto ownerKey = "iot:gb28181:owner:" + identity;
            if (co_await context.redis().get(ownerKey)) {
                continue;
            }

            auto transaction = co_await context.db().begin_transaction();
            (void)co_await transaction.query(
                advisoryLockQuery(context.pool(), identity));
            ruvia::db_query currentQuery(context.pool());
            currentQuery.select(currentQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"online">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()))
                .from(service::gb28181::persistence::Gb28181StreamEntity::table_name())
                .where(currentQuery.binary(
                    currentQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"app">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    currentQuery.value(app)))
                .and_where(currentQuery.binary(
                    currentQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"stream">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    currentQuery.value(stream)))
                .and_where(currentQuery.binary(
                    currentQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"schema">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    currentQuery.value(schema)))
                .lock({.mode_ = ruvia::db_row_lock::update});
            const auto current = co_await transaction.query(currentQuery);
            if (current.empty() ||
                !boolean(current.front()[0].value().value_or(std::string_view{}))) {
                co_await transaction.commit();
                continue;
            }
            if (co_await context.redis().get(ownerKey)) {
                co_await transaction.commit();
                continue;
            }
            ruvia::db_query updateQuery(context.pool());
            updateQuery.update(service::gb28181::persistence::Gb28181StreamEntity::table_name())
                .set(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"online">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view(), updateQuery.value(false))
                .set(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"reader_count">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view(), updateQuery.value(0))
                .set(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"updated_at">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view(), updateQuery.call("now"))
                .where(updateQuery.binary(
                    updateQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"app">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    updateQuery.value(app)))
                .and_where(updateQuery.binary(
                    updateQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"stream">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    updateQuery.value(stream)))
                .and_where(updateQuery.binary(
                    updateQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"schema">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    updateQuery.value(schema)))
                .and_where(updateQuery.binary(
                    updateQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"online">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    updateQuery.value(true)));
            (void)co_await transaction.execute(updateQuery);
            co_await transaction.commit();
        }
        co_return;
    }

  protected:
    static ruvia::db_query advisoryLockQuery(
        std::pmr::memory_resource* resource, std::string_view key) {
        ruvia::db_query query(resource);
        query.select(query.call(
            "pg_advisory_xact_lock",
            {query.call("hashtextextended",
                        {query.value(key), query.value(std::int64_t{28181})})}));
        return query;
    }

    static ruvia::db_query::expr_type projectionCursor(
        ruvia::db_query& query, std::string_view streamMessageId) {
        const auto messageId = query.value(streamMessageId);
        const auto separator = query.value(std::string_view{"-"});
        const auto radix = query.cast(
            query.value(std::string_view{"18446744073709551616"}),
            ruvia::db_data_type::numeric);
        const auto milliseconds = query.cast(
            query.call("split_part",
                       {messageId, separator,
                        query.cast(query.value(std::int64_t{1}),
                                   ruvia::db_data_type::integer)}),
            ruvia::db_data_type::numeric);
        const auto sequence = query.cast(
            query.call("split_part",
                       {messageId, separator,
                        query.cast(query.value(std::int64_t{2}),
                                   ruvia::db_data_type::integer)}),
            ruvia::db_data_type::numeric);
        return query.binary(
            query.binary(milliseconds, ruvia::db_binary_operator::multiply,
                         radix),
            ruvia::db_binary_operator::add, sequence);
    }

    static bool boolean(std::string_view value) {
        return value == "t" || value == "true" || value == "1";
    }

    static int integer(std::string_view value, int fallback = 0) {
        int parsed = 0;
        const auto* begin = value.data();
        const auto* end = begin + value.size();
        const auto result = std::from_chars(begin, end, parsed);
        if (result.ec != std::errc{} || result.ptr != end) {
            return fallback;
        }
        return parsed;
    }

    static ruvia::task<ProjectionSnapshot> hydrate(ruvia::web_worker_context& context) {
        ProjectionSnapshot snapshot;
        std::unordered_map<std::string, std::size_t> deviceIndexes;
        ruvia::db_query deviceQuery(context.pool());
        deviceQuery
            .select({
                deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"id">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()), deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"name">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()),
                deviceQuery.coalesce({deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()),
                                      deviceQuery.value(std::string_view{})}),
                deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"manufacturer">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()),
                deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"remote_address">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()),
                deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"registration_source">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()),
                deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"online">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()),
                deviceQuery.call("iot_utc_timestamp",
                                 {deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"last_seen_at">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())}),
            })
            .from(service::gb28181::persistence::Gb28181DeviceEntity::table_name())
            .order_by(deviceQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"id">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()));
        const auto devices = co_await context.db().query(deviceQuery);
        snapshot.devices.reserve(devices.size());
        for (const auto& row : devices) {
            Device device;
            device.id = std::string(row[0].value().value_or(std::string_view{}));
            device.name = std::string(row[1].value().value_or(std::string_view{}));
            device.customName =
                std::string(row[2].value().value_or(std::string_view{}));
            device.manufacturer =
                std::string(row[3].value().value_or(std::string_view{}));
            device.remoteAddress =
                std::string(row[4].value().value_or(std::string_view{}));
            device.registrationSource =
                std::string(row[5].value().value_or(std::string_view{}));
            device.online = boolean(row[6].value().value_or(std::string_view{}));
            if (const auto parsed = service::common::parseUtcTimestamp(
                    row[7].value().value_or(std::string_view{})
                )) {
                device.lastSeen = *parsed;
            }
            deviceIndexes.emplace(device.id, snapshot.devices.size());
            snapshot.devices.push_back(std::move(device));
        }

        ruvia::db_query channelQuery(context.pool());
        channelQuery
            .select({channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"device_id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                     channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()), channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                     channelQuery.coalesce(
                         {channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                          channelQuery.value(std::string_view{})}),
                     channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"manufacturer">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                     channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"online">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                     channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"ptz_type">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view())})
            .from(service::gb28181::persistence::Gb28181ChannelEntity::table_name())
            .order_by(channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"device_id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()))
            .add_order_by(channelQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()));
        const auto channels = co_await context.db().query(channelQuery);
        for (const auto& row : channels) {
            const auto device = deviceIndexes.find(
                std::string(row[0].value().value_or(std::string_view{}))
            );
            if (device == deviceIndexes.end()) {
                continue;
            }
            snapshot.devices[device->second].channels.push_back(Channel{
                .id = std::string(row[1].value().value_or(std::string_view{})),
                .name = std::string(row[2].value().value_or(std::string_view{})),
                .customName = std::string(row[3].value().value_or(std::string_view{})),
                .manufacturer = std::string(row[4].value().value_or(std::string_view{})),
                .online = boolean(row[5].value().value_or(std::string_view{})),
                .ptzType = integer(row[6].value().value_or(std::string_view{}), -1),
            });
        }

        co_return snapshot;
    }

    template <typename Transaction>
    static ruvia::task<void> syncChannels(Transaction& transaction, const Device& device, bool updateCustomNames = true) {
        if (device.channels.empty()) {
            ruvia::db_query removal;
            removal.delete_from(service::gb28181::persistence::Gb28181ChannelEntity::table_name())
                .where(removal.binary(
                    removal.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"device_id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                    ruvia::db_binary_operator::equal,
                    removal.value(device.id)));
            (void)co_await transaction.execute(removal);
            co_return;
        }

        // CTE 参数使用 TEXT，目标列仍按实体和数据库约束验证长度，避免 VARCHAR(n) 转换静默截断。
        ruvia::db_query rawIncoming;
        std::size_t ordinal = 0;
        for (const auto& channel : device.channels) {
            rawIncoming.values({
                rawIncoming.cast(
                    rawIncoming.value(static_cast<std::int64_t>(ordinal)),
                    ruvia::db_data_type::big_int),
                rawIncoming.cast(rawIncoming.value(channel.id),
                                 ruvia::db_data_type::text),
                rawIncoming.cast(rawIncoming.value(channel.name),
                                 ruvia::db_data_type::text),
                rawIncoming.cast(rawIncoming.value(channel.customName),
                                 ruvia::db_data_type::text),
                rawIncoming.cast(rawIncoming.value(channel.manufacturer),
                                 ruvia::db_data_type::text),
                rawIncoming.cast(rawIncoming.value(channel.online),
                                 ruvia::db_data_type::boolean),
                rawIncoming.cast(rawIncoming.value(channel.ptzType),
                                 ruvia::db_data_type::integer),
            });
            ++ordinal;
        }

        ruvia::db_query incoming;
        incoming
            .select({incoming.column("id"), incoming.column("name"),
                     incoming.column("custom_name"),
                     incoming.column("manufacturer"),
                     incoming.column("online"), incoming.column("ptz_type")})
            .distinct_on({incoming.column("id")})
            .from("raw_incoming")
            .order_by(incoming.column("id"))
            .add_order_by(incoming.column("ordinal"),
                        ruvia::db_order_direction::desc);

        ruvia::db_query insertSource;
        insertSource
            .select({
                insertSource.value(device.id),
                insertSource.column("id", "incoming"),
                insertSource.column("name", "incoming"),
                insertSource.null_if(insertSource.column("custom_name", "incoming"),
                                    insertSource.value(std::string_view{})),
                insertSource.column("manufacturer", "incoming"),
                insertSource.column("online", "incoming"),
                insertSource.column("ptz_type", "incoming"),
                insertSource.call("now"),
            })
            .from("incoming", "incoming");

        ruvia::db_query upsert;
        upsert
            .insert_into(service::gb28181::persistence::Gb28181ChannelEntity::table_name(),
                        {"device_id", "id", "name", "custom_name",
                         "manufacturer", "online", "ptz_type", "updated_at"})
            .insert_from(insertSource);
        ruvia::db_conflict_options conflict;
        conflict.columns_ = {"device_id", "id"};
        conflict.update_ = {
            {"name", upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view())},
            {"custom_name", updateCustomNames
                                 ? upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view())
                                 : upsert.coalesce(
                                       {upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                                        upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(),
                                                      "gb28181_channel")})},
            {"manufacturer", upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"manufacturer">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view())},
            {"online", upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"online">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view())},
            {"ptz_type", upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"ptz_type">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view())},
            {"updated_at", upsert.call("now")},
        };
        conflict.update_where_ = upsert.binary(
            upsert.tuple({
                upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), "gb28181_channel"),
                upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), "gb28181_channel"),
                upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"manufacturer">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), "gb28181_channel"),
                upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"online">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), "gb28181_channel"),
                upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"ptz_type">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), "gb28181_channel"),
            }),
            ruvia::db_binary_operator::is_distinct_from,
            upsert.tuple({upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                          upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                          upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"manufacturer">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                          upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"online">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                          upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"ptz_type">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view())}));
        upsert.on_conflict(conflict)
            .returning({upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view())});

        ruvia::db_query incomingIds;
        incomingIds
            .select(incomingIds.value(1))
            .from("incoming", "incoming")
            .where(incomingIds.binary(
                incomingIds.column("id", "incoming"),
                ruvia::db_binary_operator::equal,
                incomingIds.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), "stored")));

        ruvia::db_query removal;
        removal
            .with("raw_incoming", rawIncoming,
                  {.columns_ = {"ordinal", "id", "name", "custom_name",
                               "manufacturer", "online", "ptz_type"}})
            .with("incoming", incoming)
            .with("upserted", upsert)
            .delete_from(service::gb28181::persistence::Gb28181ChannelEntity::table_name(), "stored")
            .where(removal.binary(
                removal.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"device_id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), "stored"),
                ruvia::db_binary_operator::equal,
                removal.value(device.id)))
            .and_where(removal.unary(
                ruvia::db_unary_operator::not_value,
                removal.exists(incomingIds)));
        (void)co_await transaction.execute(removal);
    }

    // Projection events carry a complete Collector-side snapshot, but a new
    // Collector registry does not hydrate Service metadata before REGISTER.
    // Apply only the fields owned by the event change so an empty in-memory
    // custom name or channel list cannot erase durable state from an earlier
    // session.
    static ruvia::task<bool> applyDeviceProjection(
        ruvia::web_worker_context& context,
        const Device& device,
        DeviceChange change,
        std::string ownerKey,
        std::string ownerToken,
        std::string streamMessageId
    ) {
        auto transaction = co_await context.db().begin_transaction();
        (void)co_await transaction.query(
            advisoryLockQuery(context.pool(), device.id));
        ruvia::db_query cursorQuery(context.pool());
        cursorQuery
            .select(cursorQuery.binary(
                cursorQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"projection_cursor">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()),
                ruvia::db_binary_operator::greater_equal,
                projectionCursor(cursorQuery, streamMessageId)))
            .from(service::gb28181::persistence::Gb28181DeviceEntity::table_name())
            .where(cursorQuery.binary(
                cursorQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"id">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                cursorQuery.value(device.id)))
            .lock({.mode_ = ruvia::db_row_lock::update});
        const auto cursor = co_await transaction.query(cursorQuery);
        if (!cursor.empty() &&
            boolean(cursor.front()[0].value().value_or(std::string_view{}))) {
            co_await transaction.commit();
            co_return true;
        }
        const auto currentOwner = co_await context.redis().get(ownerKey);
        const bool ownerMatches = device.online
            ? currentOwner &&
                std::string_view(*currentOwner) ==
                    ownerToken
            : !currentOwner ||
                std::string_view(*currentOwner) ==
                    ownerToken;
        if (!ownerMatches) {
            co_await transaction.commit();
            co_return false;
        }
        const auto lastSeen = service::common::utcTimestamp(device.lastSeen);

        const auto buildDeviceUpsert = [&]() {
            ruvia::db_query query(context.pool());
            query
                .insert_into(
                    service::gb28181::persistence::Gb28181DeviceEntity::table_name(),
                    {"id", "name", "custom_name", "manufacturer",
                     "remote_address", "registration_source", "online",
                     "last_seen_at", "updated_at"})
                .values({
                    query.value(device.id),
                    query.value(device.name),
                    query.null_if(query.value(device.customName),
                                 query.value(std::string_view{})),
                    query.value(device.manufacturer),
                    query.value(device.remoteAddress),
                    query.value(device.registrationSource),
                    query.value(device.online),
                    query.cast(query.value(lastSeen),
                               ruvia::db_data_type::timestamp_tz),
                    query.call("now"),
                });

            ruvia::db_conflict_options conflict;
            conflict.columns_ = {"id"};
            switch (change) {
                case DeviceChange::Status:
                    conflict.update_ = {
                        {"remote_address", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"remote_address">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"registration_source",
                         query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"registration_source">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"online", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"online">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"last_seen_at", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"last_seen_at">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"updated_at", query.call("now")},
                    };
                    break;
                case DeviceChange::Catalog:
                    conflict.update_ = {
                        {"name", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"name">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"manufacturer", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"manufacturer">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"remote_address", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"remote_address">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"registration_source",
                         query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"registration_source">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"online", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"online">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"last_seen_at", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"last_seen_at">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"updated_at", query.call("now")},
                    };
                    break;
                case DeviceChange::DeviceName:
                    conflict.update_ = {
                        {"custom_name", query.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view())},
                        {"updated_at", query.call("now")},
                    };
                    break;
                case DeviceChange::ChannelName:
                    break;
            }
            query.on_conflict(conflict);
            return query;
        };

        switch (change) {
            case DeviceChange::Status:
                (void)co_await transaction.execute(buildDeviceUpsert());
                break;
            case DeviceChange::Catalog:
                (void)co_await transaction.execute(buildDeviceUpsert());
                co_await syncChannels(transaction, device, false);
                break;
            case DeviceChange::DeviceName:
                (void)co_await transaction.execute(buildDeviceUpsert());
                break;
            case DeviceChange::ChannelName:
                for (const auto& channel : device.channels) {
                    if (channel.customName.empty()) {
                        continue;
                    }
                    ruvia::db_query updateQuery(context.pool());
                    updateQuery.update(service::gb28181::persistence::Gb28181ChannelEntity::table_name())
                        .set(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"custom_name">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), updateQuery.value(channel.customName))
                        .set(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"updated_at">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view(), updateQuery.call("now"))
                        .where(updateQuery.binary(
                            updateQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"device_id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                            ruvia::db_binary_operator::equal,
                            updateQuery.value(device.id)))
                        .and_where(updateQuery.binary(
                            updateQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181ChannelEntity::column_index<"id">(), service::gb28181::persistence::Gb28181ChannelEntity::columns_type>::name.view()),
                            ruvia::db_binary_operator::equal,
                            updateQuery.value(channel.id)));
                    (void)co_await transaction.execute(updateQuery);
                }
                break;
        }
        ruvia::db_query cursorUpdate(context.pool());
        cursorUpdate
            .update(service::gb28181::persistence::Gb28181DeviceEntity::table_name())
            .set(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"projection_cursor">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view(),
                 projectionCursor(cursorUpdate, streamMessageId))
            .where(cursorUpdate.binary(
                cursorUpdate.column(std::tuple_element_t<service::gb28181::persistence::Gb28181DeviceEntity::column_index<"id">(), service::gb28181::persistence::Gb28181DeviceEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                cursorUpdate.value(device.id)));
        (void)co_await transaction.execute(cursorUpdate);
        co_await transaction.commit();
        co_return true;
    }


    static ruvia::task<bool> applyStreamProjection(
        ruvia::web_worker_context& context,
        const StreamStatus& stream,
        std::string ownerKey,
        std::string ownerToken,
        std::string streamMessageId
    ) {
        const auto identity =
            StreamStatus::identity(stream.app, stream.stream, stream.schema);
        auto transaction = co_await context.db().begin_transaction();
        (void)co_await transaction.query(
            advisoryLockQuery(context.pool(), identity));
        ruvia::db_query cursorQuery(context.pool());
        cursorQuery
            .select(cursorQuery.binary(
                cursorQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"projection_cursor">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                ruvia::db_binary_operator::greater_equal,
                projectionCursor(cursorQuery, streamMessageId)))
            .from(service::gb28181::persistence::Gb28181StreamEntity::table_name())
            .where(cursorQuery.binary(
                cursorQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"app">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                cursorQuery.value(stream.app)))
            .and_where(cursorQuery.binary(
                cursorQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"stream">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                cursorQuery.value(stream.stream)))
            .and_where(cursorQuery.binary(
                cursorQuery.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"schema">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                cursorQuery.value(stream.schema)))
            .lock({.mode_ = ruvia::db_row_lock::update});
        const auto cursor = co_await transaction.query(cursorQuery);
        if (!cursor.empty() &&
            boolean(cursor.front()[0].value().value_or(std::string_view{}))) {
            co_await transaction.commit();
            co_return true;
        }
        const auto currentOwner = co_await context.redis().get(ownerKey);
        const bool ownerMatches = stream.online
            ? currentOwner &&
                std::string_view(*currentOwner) ==
                    ownerToken
            : !currentOwner ||
                std::string_view(*currentOwner) ==
                    ownerToken;
        if (!ownerMatches) {
            co_await transaction.commit();
            co_return false;
        }
        ruvia::db_query upsert(context.pool());
        upsert
            .insert_into(service::gb28181::persistence::Gb28181StreamEntity::table_name(),
                        {"app", "stream", "schema", "online", "reader_count",
                         "updated_at"})
            .values({upsert.value(stream.app), upsert.value(stream.stream),
                     upsert.value(stream.schema), upsert.value(stream.online),
                     upsert.value(stream.readerCount), upsert.call("now")});
        ruvia::db_conflict_options conflict;
        conflict.columns_ = {"app", "stream", "schema"};
        conflict.update_ = {
            {"online", upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"online">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view())},
            {"reader_count", upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"reader_count">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view())},
            {"updated_at", upsert.call("now")},
        };
        conflict.update_where_ = upsert.binary(
            upsert.tuple({upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"online">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view(), "gb28181_stream"),
                          upsert.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"reader_count">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view(), "gb28181_stream")}),
            ruvia::db_binary_operator::is_distinct_from,
            upsert.tuple({upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"online">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()),
                          upsert.excluded(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"reader_count">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view())}));
        upsert.on_conflict(conflict);
        (void)co_await transaction.execute(upsert);
        ruvia::db_query cursorUpdate(context.pool());
        cursorUpdate
            .update(service::gb28181::persistence::Gb28181StreamEntity::table_name())
            .set(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"projection_cursor">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view(),
                 projectionCursor(cursorUpdate, streamMessageId))
            .where(cursorUpdate.binary(
                cursorUpdate.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"app">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                cursorUpdate.value(stream.app)))
            .and_where(cursorUpdate.binary(
                cursorUpdate.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"stream">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                cursorUpdate.value(stream.stream)))
            .and_where(cursorUpdate.binary(
                cursorUpdate.column(std::tuple_element_t<service::gb28181::persistence::Gb28181StreamEntity::column_index<"schema">(), service::gb28181::persistence::Gb28181StreamEntity::columns_type>::name.view()), ruvia::db_binary_operator::equal,
                cursorUpdate.value(stream.schema)));
        (void)co_await transaction.execute(cursorUpdate);
        co_await transaction.commit();
        co_return true;
    }
};

} // namespace service::gb28181

namespace service::gb28181 {

class GbControlService final {
  public:
    static void requireEnabled(bool enabled) {
        if (!enabled) {
            service::common::fail(10004, "GB28181 功能未启用", 404);
        }
    }

    static ruvia::task<void> publishControlCommand(
        const ruvia::redis_handle& redis, const std::string& controlKey,
        const std::string& ownerKey, std::string_view ownerToken,
        std::span<const service::message::StreamField> fields) {
        std::vector<std::string_view> args{ ownerToken };
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const std::string_view keys[]{ controlKey, ownerKey };
        static constexpr std::string_view publishScript = R"lua(
if redis.call('GET',KEYS[2])~=ARGV[1] then return 0 end
redis.call('XADD',KEYS[1],'*',unpack(ARGV,2))
redis.call('EXPIRE',KEYS[1],600)
return 1
)lua";
        const auto published = co_await redis.eval(publishScript, keys, args);
        if (published.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("GB28181 command publish", published);
        }
        if (published.integer() != 1) {
            service::common::fail(10003, "GB28181 connection owner expired", 409);
        }
    }

    static ruvia::task<ControlClaimResult> claimControlExecution(
        const ruvia::redis_handle& redis, const std::string& ownerKey,
        const std::string& claimKey, const std::string& cancelKey,
        const std::string& ownerToken, std::string_view deadline) {
        // Claim and owner/cancel/deadline checks are atomic in Redis.  The
        // claim lives longer than the mandatory maximum command lifetime.
        static constexpr std::string_view script = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('EXISTS', KEYS[3]) == 1 then return -2 end
local t = redis.call('TIME')
if tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000) >= tonumber(ARGV[2]) then return -3 end
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'EX', 600) then return 1 end
return 0
)lua";
        const std::string_view keys[]{ ownerKey, claimKey, cancelKey };
        const std::string_view args[]{ ownerToken, deadline };
        const auto claimed = co_await redis.eval(script, keys, args);
        if (claimed.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("GB28181 command claim", claimed);
        }
        co_return static_cast<ControlClaimResult>(claimed.integer());
    }

    static ruvia::task<bool> controlCancelled(
        const ruvia::redis_handle& redis, const std::string& cancelKey) {
        const auto cancelled = co_await redis.get(cancelKey);
        co_return static_cast<bool>(cancelled);
    }

    static ruvia::task<ProjectionPublishResult> publishProjection(
        const ruvia::redis_handle& redis, const std::string& projectionId,
        std::string_view projectionOrder, std::string_view currentEntryId,
        std::span<const service::message::StreamField> fields) {
    // The first stream ID is the actor's ordering cursor.  A later repair may
    // need a new Redis entry after the old one has been acknowledged/deleted,
    // but it must carry this original order so a retry cannot move the DB
    // cursor backwards or reapply an older event over a newer one.
    static constexpr std::string_view script = R"lua(
local function valid_id(id)
  return id ~= nil and string.match(id, '^%d+%-%d+$') ~= nil
end

local marker = redis.call('GET', KEYS[2])
local order = ARGV[1]
local current = ARGV[2]
if order ~= '' and not valid_id(order) then
  return redis.error_reply('invalid local GB28181 projection order')
end
if current ~= '' and not valid_id(current) then
  return redis.error_reply('invalid local GB28181 projection entry')
end

if marker then
  local separator = string.find(marker, '|', 1, true)
  if not separator or string.find(marker, '|', separator + 1, true) then
    return redis.error_reply('invalid GB28181 projection sent marker')
  end
  local marker_order = string.sub(marker, 1, separator - 1)
  local marker_current = string.sub(marker, separator + 1)
  if not valid_id(marker_order) or not valid_id(marker_current) then
    return redis.error_reply('invalid GB28181 projection sent marker IDs')
  end
  if order ~= '' and order ~= marker_order then
    return redis.error_reply('GB28181 projection order changed')
  end
  order = marker_order
  -- If the previous XADD succeeded but its reply was lost, the marker's
  -- current ID is authoritative and avoids a duplicate repair entry.
  current = marker_current
end

if current ~= '' then
  local existing = redis.call('XRANGE', KEYS[1], current, current)
  if #existing > 0 then
    -- A marker may have expired while this coroutine was waiting.  Restore
    -- it once, without refreshing an existing marker on every poll.
    if not marker then
      redis.call('SET', KEYS[2], order .. '|' .. current, 'EX', 86400)
    end
    return { order, current }
  end
end

local values = {}
for index = 3, #ARGV do
  values[#values + 1] = ARGV[index]
end
if order ~= '' then
  values[#values + 1] = 'projection_order'
  values[#values + 1] = order
end
local id = redis.call('XADD', KEYS[1], '*', unpack(values))
if order == '' then
  order = id
end
redis.call('SET', KEYS[2], order .. '|' .. id, 'EX', 86400)
return { order, id }
)lua";
    const auto sentKey = "iot:gb28181:projection:sent:" + projectionId;
    const std::string_view keys[]{ control_protocol::stream::kProjection, sentKey };
        std::vector<std::string_view> args;
        args.reserve(2 + fields.size() * 2);
        args.push_back(projectionOrder);
        args.push_back(currentEntryId);
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const auto published = co_await redis.eval(script, keys, args);
        co_return parseProjectionPublishResult(published);
    }

    static ruvia::task<std::optional<bool>> projectionReceipt(
        const ruvia::redis_handle& redis, std::string_view projectionId) {
        const auto done = co_await redis.get(control_protocol::stream::projectionDone(projectionId));
        if (!done) co_return std::nullopt;
        if (*done != "0" && *done != "1") {
            throw std::runtime_error("invalid GB28181 projection receipt");
        }
        co_return std::string_view(*done) == "1";
    }

    static ProjectionPublishResult parseProjectionPublishResult(
    const ruvia::redis_value& reply
) {
    if (reply.kind() != ruvia::redis_value::kind_type::array ||
        reply.array().size() != 2 ||
        reply.array()[0].kind() != ruvia::redis_value::kind_type::string ||
        reply.array()[1].kind() != ruvia::redis_value::kind_type::string) {
        service::message::redis::throwValue(
            "GB28181 projection publish result",
            reply
        );
    }
    ProjectionPublishResult result{
        std::string(reply.array()[0].string()),
        std::string(reply.array()[1].string())
    };
    if (!control_protocol::validProjectionStreamId(result.order) ||
        !control_protocol::validProjectionStreamId(result.current)) {
        throw std::runtime_error("GB28181 projection publish returned invalid stream ID");
    }
    return result;
}

    static ruvia::task<std::optional<ControlResultRecord>> loadControlResult(
        const ruvia::redis_handle& redis, std::string_view key) {
        const auto value = co_await redis.get(key);
        if (!value) co_return std::nullopt;
        co_return ControlResultRecord::decode(*value);
    }

    static ruvia::task<bool> renewOwnerLease(const ruvia::redis_handle& connection, std::string_view key, std::string_view token) {
        static constexpr std::string_view script = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
)lua";

        const auto redis = connection.with_options({ .timeout_ = std::chrono::seconds(3) });
        const std::string_view leaseTtl = "15000";
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token, leaseTtl };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("renew GB28181 owner lease", reply);
        }
        co_return reply.integer() == 1;
    }

    static ruvia::task<bool> claimOwnerLease(const ruvia::redis_handle& connection, std::string_view key, std::string_view token, std::string_view previousToken) {
        static constexpr std::string_view script = R"lua(
local current=redis.call('GET',KEYS[1])
if current==ARGV[1] then
  redis.call('PEXPIRE',KEYS[1],ARGV[3])
  return 1
end
if ARGV[2]~='' then
  if current~=ARGV[2] then return 0 end
  redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[3])
  return 1
end
if current then return 0 end
local claimed=redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[3],'NX')
return claimed and 1 or 0
)lua";

        const auto redis = connection.with_options({ .timeout_ = std::chrono::seconds(3) });
        const std::string_view leaseTtl = "15000";
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token, previousToken, leaseTtl };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("claim GB28181 owner lease", reply);
        }
        co_return reply.integer() == 1;
    }

    static ruvia::task<void> releaseOwnerLease(const ruvia::redis_handle& connection, std::string_view key, std::string_view token) {
        static constexpr std::string_view script = R"lua(
if redis.call('GET',KEYS[1])==ARGV[1] then
  return redis.call('DEL',KEYS[1])
end
return 0
)lua";

        const auto redis = connection.with_options({ .timeout_ = std::chrono::seconds(3) });
        const std::string_view keys[]{ key };
        const std::string_view arguments[]{ token };
        const auto reply = co_await redis.eval(script, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::message::redis::throwValue("release GB28181 owner lease", reply);
        }
        co_return;
    }

    static ruvia::task<void> publishReplyAndAcknowledge(
    const ruvia::redis_handle& redis,
    std::string_view replyStream,
    std::string_view controlStream,
    std::string_view group,
    std::string_view messageId,
    std::string_view requestId,
    std::string_view operation,
    std::string_view status,
    std::string_view payload,
    std::string_view resultKey,
    std::string_view claimKey,
    std::string_view claimOwner
) {
    // Reply publication and PEL removal are one Redis transaction.  A worker
    // crash between the two cannot acknowledge a command whose response was
    // never made visible to the Service Worker.
    static constexpr std::string_view script = R"lua(
 if ARGV[7] ~= '' and redis.call('EXISTS', KEYS[3]) == 0 then
   redis.call('SET', KEYS[3], ARGV[7], 'EX', ARGV[8])
 end
 local reply = redis.call('XADD', KEYS[1], '*',
   'request_id', ARGV[1], 'operation', ARGV[2], 'status', ARGV[3],
   'payload', ARGV[4])
 redis.call('EXPIRE', KEYS[1], ARGV[9])
 redis.call('EXPIRE', KEYS[2], ARGV[10])
 local acknowledged = redis.call('XACK', KEYS[2], ARGV[5], ARGV[6])
 redis.call('XDEL', KEYS[2], ARGV[6])
 if ARGV[11] ~= '' and redis.call('GET', KEYS[4]) == ARGV[11] then
   redis.call('DEL', KEYS[4])
 end
return acknowledged
)lua";
    const std::string replyKey(replyStream);
    const std::string controlKey(controlStream);
    const std::string resultKeyValue(resultKey);
    const std::string claimKeyValue(claimKey);
    const std::string request(requestId);
    const std::string op(operation);
    const std::string state(status);
    const std::string body(payload);
    const std::string groupValue(group);
    const std::string id(messageId);
    const std::string result = ControlResultRecord{std::string(status), std::string(payload)}.encode();
    const std::string claim(claimOwner);
    const std::string resultTtl{ "600" };
    const std::string replyTtl{ "600" };
    const std::string controlTtl{ "600" };
    const std::string_view keys[]{ replyKey, controlKey, resultKeyValue, claimKeyValue };
    const std::string_view arguments[]{ request, op, state, body, groupValue, id, result, resultTtl, replyTtl, controlTtl, claim };
    const auto reply = co_await redis.eval(script, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        service::message::redis::throwValue("GB28181 reply/ack", reply);
    }
    co_return;
}

    static ruvia::task<void> markControlCancelled(const ruvia::redis_handle& redis, std::string_view cancelKey) {
    ruvia::redis_set_options options;
    options.expiration_ =
        ruvia::redis_set_expiration::expires_after(std::chrono::minutes(10));
    co_await redis.set(std::string(cancelKey), "1", std::move(options));
    co_return;
}

    static ruvia::task<std::optional<std::string>> ownerTokenFor(
    const ruvia::redis_handle& redis,
    std::string_view key
) {
    const auto value = co_await redis.get(key);
    if (!value) {
        co_return std::nullopt;
    }
    co_return std::optional<std::string>(std::string(*value));
}

    static ruvia::task<void> markProjectionDoneAndAcknowledge(
    const ruvia::redis_handle& redis,
    std::string_view stream,
    std::string_view group,
    std::string_view messageId,
    std::string_view projectionId,
    bool applied
) {
    if (projectionId.empty()) {
        throw std::runtime_error("GB28181 projection has no projection_id");
    }
    static constexpr std::string_view script = R"lua(
 redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
 local acknowledged = redis.call('XACK', KEYS[2], ARGV[3], ARGV[4])
 redis.call('XDEL', KEYS[2], ARGV[4])
return acknowledged
)lua";
    const std::string doneKey(
        control_protocol::stream::projectionDone(projectionId)
    );
    const std::string streamKey(stream);
    const std::string appliedValue(applied ? "1" : "0");
    const std::string ttl{ "600" };
    const std::string groupValue(group);
    const std::string idValue(messageId);
    const std::string_view keys[]{ doneKey, streamKey };
    const std::string_view arguments[]{ appliedValue, ttl, groupValue, idValue };
    const auto reply = co_await redis.eval(script, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        service::message::redis::throwValue("GB28181 projection done", reply);
    }
    co_return;
}

static ruvia::task<std::string> executeOperation(
    ruvia::web_worker_context& context,
    std::string_view operation,
    std::string_view payload,
    ruvia::stop_token stop
) {
    if (stop.stop_requested()) {
        service::common::fail(10004, "GB28181 operation cancelled", 503);
    }

    std::pmr::monotonic_buffer_resource resource;
    auto request = control_protocol::parseRequest(
        payload.empty() ? std::string_view{ "{}" } : payload,
        &resource
    );
    if (!request) {
        service::common::fail(10001, "GB28181 RPC 请求体无效", 400);
    }
    const auto& input = *request;
    const auto enabled = co_await GbProjectionService::configuredEnabled(context.redis());

    if (operation == control_protocol::kHealthOperation) {
        co_return control_protocol::successJson(
            healthJson(context, enabled),
            context.pool()
        );
    }

    if (operation == control_protocol::kDevicesOperation) {
        GbControlService::requireEnabled(enabled);
        const auto snapshot = co_await GbProjectionService::loadSnapshot(context);
        ruvia::boxed_array<control_protocol::DeviceJson> items(
            ruvia::model_options{ .resource_ = context.pool() }
        );
        for (const auto& device : snapshot.devices) {
            items.emplace(control_protocol::deviceJson(context.pool(), device));
        }
        control_protocol::DeviceListJson result(ruvia::model_options{ .resource_ = context.pool() });
        result.set<"items">(std::move(items));
        co_return control_protocol::successJson(result, context.pool());
    }

    if (operation == control_protocol::kCatalogOperation ||
        operation == control_protocol::kRenameDeviceOperation ||
        operation == control_protocol::kRenameChannelOperation ||
        operation == control_protocol::kPreviewStartOperation ||
        operation == control_protocol::kPtzOperation ||
        operation == control_protocol::kPtzPositionOperation) {
        GbControlService::requireEnabled(enabled);
        const auto deviceId = control_protocol::requiredText(input.get<"deviceId">(), "设备编号不能为空");
        if (operation == control_protocol::kRenameDeviceOperation) {
            (void)control_protocol::requiredText(input.get<"name">(), "名称不能为空");
        }
        if (operation == control_protocol::kRenameChannelOperation) {
            (void)control_protocol::requiredText(input.get<"channelId">(), "通道编号不能为空");
            (void)control_protocol::requiredText(input.get<"name">(), "名称不能为空");
        }
        if (operation == control_protocol::kPreviewStartOperation ||
            operation == control_protocol::kPtzOperation ||
            operation == control_protocol::kPtzPositionOperation) {
            (void)control_protocol::requiredText(input.get<"channelId">(), "通道编号不能为空");
        }
        if (operation == control_protocol::kPtzOperation) {
            const auto action = control_protocol::requiredText(input.get<"action">(), "云台动作不能为空");
            control_protocol::requireAction(action);
            const auto speedValue = input.get<"speed">();
            const auto speed = speedValue ? control_protocol::requiredInteger(speedValue, "speed 无效")
                                          : 80;
            if (speed < 0 || speed > 255) {
                service::common::fail(10001, "speed 必须是 0 - 255 的整数", 400);
            }
        }
        if (operation == control_protocol::kPtzPositionOperation) {
            (void)control_protocol::requiredFinite(input.get<"pan">(), "pan", 0.0, 360.0);
            (void)control_protocol::requiredFinite(input.get<"tilt">(), "tilt", -30.0, 90.0);
            (void)control_protocol::requiredFinite(input.get<"zoom">(), "zoom", 1.0, 1000.0);
        }
        co_return co_await dispatchByOwnerKey(
            context,
            operation,
            payload,
            control_protocol::stream::owner(deviceId),
            stop
        );
    }

    if (operation == control_protocol::kPreviewStopOperation ||
        operation == control_protocol::kPreviewHeartbeatOperation) {
        GbControlService::requireEnabled(enabled);
        const auto sessionId = control_protocol::requiredText(input.get<"sessionId">(), "会话编号不能为空");
        co_return co_await dispatchByOwnerKey(
            context,
            operation,
            payload,
            control_protocol::stream::sessionOwner(sessionId),
            stop
        );
    }

    if (operation == control_protocol::kRecordingOperation ||
        operation == control_protocol::kRecordingStartOperation ||
        operation == control_protocol::kRecordingStopOperation) {
        GbControlService::requireEnabled(enabled);
        const auto streamId = control_protocol::requiredText(input.get<"streamId">(), "流编号不能为空");
        co_return co_await dispatchByOwnerKey(
            context,
            operation,
            payload,
            control_protocol::stream::owner(streamId),
            stop
        );
    }

    service::common::fail(10002, "不支持的 GB28181 操作", 400);
}

  private:
static control_protocol::HealthJson healthJson(ruvia::web_worker_context& context, bool enabled, std::string_view error = {}) {
    const auto started = sdkSupervisor().started();
    control_protocol::HealthJson result(ruvia::model_options{ .resource_ = context.pool() });
    result.set<"status">(started ? "ok" : (enabled ? "error" : "disabled"))
        .set<"service">("iot-engine-gb28181")
        .set<"enabled">(enabled)
        .set<"started">(started)
        .set<"error">(error)
        .set<"mediaPorts">(control_protocol::mediaPortsJson(context.pool(), sdkSupervisor().ports()))
        .set<"mediaCapabilities">(
            control_protocol::mediaCapabilitiesJson(context.pool(), sdkSupervisor().capabilities())
        );
    return result;
}

static ruvia::task<std::optional<service::message::StreamMessage>> readControlReply(
    const ruvia::worker_handle& worker,
    const ruvia::redis_handle& redis,
    std::string_view stream,
    std::string_view requestId,
    ruvia::stop_token stop,
    std::chrono::system_clock::time_point deadline
) {
    const auto boundedRedis = redis.with_options({ .stop_token_ = stop });
    while (!stop.stop_requested() &&
           std::chrono::system_clock::now() < deadline) {
        const std::vector<std::string> replyArguments{ "XREAD", "COUNT", "1", "STREAMS", std::string(stream), "0-0" };
        const auto reply = co_await service::message::redis::command(
            boundedRedis,
            replyArguments
        );
        if (reply.null()) {
            (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(100), stop);
            continue;
        }
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            service::message::redis::throwValue("XREAD GB28181 reply", reply);
        }
        for (const auto& streamReply : reply.array()) {
            if (streamReply.kind() != ruvia::redis_value::kind_type::array ||
                streamReply.array().size() != 2) {
                continue;
            }
            const auto& entries = streamReply.array()[1];
            if (entries.kind() != ruvia::redis_value::kind_type::array) {
                continue;
            }
            for (const auto& entry : entries.array()) {
                if (entry.kind() != ruvia::redis_value::kind_type::array ||
                    entry.array().size() != 2) {
                    continue;
                }
                const auto& id = entry.array()[0];
                const auto& fields = entry.array()[1];
                if (id.kind() != ruvia::redis_value::kind_type::string ||
                    fields.kind() != ruvia::redis_value::kind_type::array) {
                    continue;
                }
                service::message::StreamMessage message;
                message.id = std::string(id.string());
                for (std::size_t index = 0; index + 1 < fields.array().size();
                     index += 2) {
                    const auto& name = fields.array()[index];
                    const auto& value = fields.array()[index + 1];
                    if (name.kind() != ruvia::redis_value::kind_type::string ||
                        value.kind() != ruvia::redis_value::kind_type::string) {
                        continue;
                    }
                    message.fields.push_back(
                        { std::string(name.string()), std::string(value.string()) }
                    );
                }
                if (message.get("request_id") == requestId) {
                    co_return message;
                }
            }
        }
    }
    if (stop.stop_requested()) {
        throw std::runtime_error("GB28181 operation was cancelled");
    }
    co_return std::nullopt;
}

static ruvia::task<std::string> dispatchToCollector(
    ruvia::web_worker_context& context,
    std::string_view operation,
    std::string_view payload,
    std::string ownerKey,
    std::string_view ownerToken,
    ruvia::stop_token stop
) {
    const auto owner = control_protocol::stream::ownerIndex(ownerToken);
    const auto instance = control_protocol::stream::ownerInstance(ownerToken);
    if (!owner || !instance || !control_protocol::stream::completeOwnerToken(ownerToken)) {
        throw std::runtime_error("GB28181 owner token is invalid");
    }
    if (stop.stop_requested()) {
        throw std::runtime_error("GB28181 operation was cancelled");
    }
    const auto currentOwner = co_await GbControlService::ownerTokenFor(context.redis(), ownerKey);
    if (!currentOwner || std::string_view(*currentOwner) != ownerToken) {
        service::common::fail(10003, "GB28181 连接归属已变更或已过期", 404);
    }

    const auto requestId = context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
    const auto replyStream = control_protocol::stream::reply(requestId);
    const auto resultKey = control_protocol::stream::result(requestId);
    const auto cancelKey = control_protocol::stream::cancel(requestId);
    const auto now = std::chrono::system_clock::now();
    const auto deadline = now + std::chrono::seconds(15);
    const auto deadlineMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                deadline.time_since_epoch()
    )
                                .count();
    std::exception_ptr publishFailure;
    try {
        const auto controlKey = control_protocol::stream::control(*owner, *instance);
        std::vector<service::message::StreamField> fields{
            { "request_id", requestId },
            { "owner_key", std::string(ownerKey) },
            { "owner_token", std::string(ownerToken) },
            { "operation", std::string(operation) },
            { "payload", std::string(payload) },
            { "reply_stream", replyStream },
            { "result_key", resultKey },
            { "cancel_key", cancelKey },
            { "deadline_ms", std::to_string(deadlineMs) }
        };
        co_await GbControlService::publishControlCommand(
            context.redis(), controlKey, ownerKey, ownerToken, fields);
    } catch (...) {
        publishFailure = std::current_exception();
    }
    if (publishFailure) {
        // XADD may have reached Redis before the client observed a transport
        // error.  Mark the request cancelled so an uncertain delivery cannot
        // execute a device side effect after the Service has failed it.
        try {
            co_await GbControlService::markControlCancelled(context.redis(), cancelKey);
        } catch (...) {
        }
        std::rethrow_exception(publishFailure);
    }

    std::optional<service::message::StreamMessage> response;
    std::exception_ptr failure;
    try {
        response = co_await readControlReply(context.worker(), context.redis(), replyStream, requestId, stop, deadline);
    } catch (...) {
        failure = std::current_exception();
    }
    if (failure) {
        // The Collector may have claimed the command before this wait was
        // cancelled.  Leave a short lived fence for it to check before any
        // device side effect.
        try {
            co_await GbControlService::markControlCancelled(context.redis(), cancelKey);
        } catch (...) {
        }
        try {
            (void)co_await context.redis().del(replyStream);
        } catch (...) {
        }
        std::rethrow_exception(failure);
    }
    if (!response) {
        try {
            co_await GbControlService::markControlCancelled(context.redis(), cancelKey);
        } catch (...) {
        }
        try {
            (void)co_await context.redis().del(replyStream);
        } catch (...) {
        }
        throw std::runtime_error(
            "GB28181 Collector did not reply before deadline"
        );
    }
    (void)co_await context.redis().del(replyStream);
    const auto status = response->get("status");
    const auto result = response->get("payload");
    if (status != "ok") {
        throw std::runtime_error(result.empty() ? "GB28181 Collector operation failed" : std::string(result));
    }
    co_return std::string(result);
}

static ruvia::task<std::string> dispatchByOwnerKey(
    ruvia::web_worker_context& context,
    std::string_view operation,
    std::string_view payload,
    std::string ownerKey,
    ruvia::stop_token stop
) {
    const auto token = co_await GbControlService::ownerTokenFor(context.redis(), ownerKey);
    if (!token) {
        service::common::fail(10003, "GB28181 连接归属不存在或已过期", 404);
    }
    try {
        co_return co_await dispatchToCollector(context, operation, payload, ownerKey, *token, stop);
    } catch (const ruvia::http_error&) {
        throw;
    } catch (const std::exception& error) {
        service::common::fail(10004, error.what(), 502);
    }
}

};

} // namespace service::gb28181
