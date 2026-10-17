#pragma once

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/utils/uuid.h"
#include "service/features/collector/collector.config.h"
#include "service/features/collector/collector.protocol.h"
#include "service/features/collector/collector.types.h"
#include "service/features/command/command.entity.h"
#include "service/features/command/command.types.h"
#include "service/features/configuration/configuration.entity.h"
#include "service/features/configuration/configuration.service.h"
#include "service/features/edge/edge.config.h"
#include "service/features/edge/edge.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/session/session.service.h"
#include "service/features/messaging/messaging.entity.h"
#include "service/utils/json.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"

namespace service::command {

template <typename Redis>
ruvia::task<bool> dispatchPendingBatch(const Redis& redis, std::string_view queueKey,
                                       PendingQueueKind kind,
                                       const std::vector<PendingDispatch>& dispatches,
                                       std::size_t maxLength) {
    if (queueKey.empty() || dispatches.empty() || maxLength == 0)
        throw std::invalid_argument("pending command batch is incomplete");
    std::set<std::string, std::less<>> commandIds;
    std::vector<std::string> arguments{
        kind == PendingQueueKind::Stream ? "stream" : "list",
        std::to_string(maxLength), std::to_string(dispatches.size())};
    for (const auto& dispatch : dispatches) {
        if (dispatch.task.messageId.empty() ||
            !commandIds.emplace(dispatch.task.messageId).second)
            throw std::invalid_argument("pending command identity is invalid");
        if (kind == PendingQueueKind::Stream) {
            if (dispatch.streamFields.empty()) throw std::invalid_argument("empty command payload");
            arguments.push_back(std::to_string(dispatch.streamFields.size()*2));
            for (const auto& field : dispatch.streamFields) {
                arguments.push_back(field.name); arguments.push_back(field.value);
            }
        } else {
            if (dispatch.listPayload.empty()) throw std::invalid_argument("empty command payload");
            arguments.emplace_back("1"); arguments.push_back(dispatch.listPayload);
        }
    }
    const std::string_view keyViews[]{queueKey};
    std::vector<std::string_view> argumentViews(arguments.begin(),arguments.end());
    const auto reply = co_await redis.eval(persistence::kPendingBatchDispatchScript, keyViews, argumentViews);
    if (reply.kind() == ruvia::redis_value::kind_type::integer && reply.integer() == 0)
        co_return false;
    if (reply.kind() != ruvia::redis_value::kind_type::integer ||
        reply.integer() != static_cast<std::int64_t>(dispatches.size()))
        utils::redis::throwValue("dispatch pending command batch", reply);
    co_return true;
}

// 读取设备当前 Collector 路由；设备离线、租约失效或路由字段不完整时返回空。
template <typename Redis>
ruvia::task<std::optional<DeviceRoute>> deviceRoute(const Redis& redis, std::string_view deviceId) {
    const auto key = message::collector::connectionKey(deviceId);
    const std::string_view keys[]{key};
    const std::string_view args[]{message::collector::kLinkOwnerPrefix};
    const auto reply = co_await redis.eval(persistence::kDeviceRouteReadScript, keys, args);
    if (reply.kind() != ruvia::redis_value::kind_type::array)
        utils::redis::throwValue("read device collector route", reply);
    if (reply.array().size() != 6)
        co_return std::nullopt;
    for (const auto& value : reply.array())
        if (value.kind() != ruvia::redis_value::kind_type::string || value.string().empty())
            co_return std::nullopt;
    const auto worker = reply.array()[0].string();
    const auto instance = reply.array()[1].string();
    const auto connection = reply.array()[2].string();
    const auto epoch = reply.array()[3].string();

    DeviceRoute route;
    const auto [workerEnd, workerError] =
        std::from_chars(worker.data(), worker.data() + worker.size(), route.workerIndex);
    const auto [epochEnd, epochError] =
        std::from_chars(epoch.data(), epoch.data() + epoch.size(), route.sessionEpoch);
    if (workerError != std::errc{} || workerEnd != worker.data() + worker.size() ||
        epochError != std::errc{} || epochEnd != epoch.data() + epoch.size() ||
        route.sessionEpoch == 0)
        co_return std::nullopt;
    route.connectionId = connection;
    route.instanceId = instance;
    co_return route;
}

// 在调用方事务内为命令状态变化追加 device.command.updated 外发事件（payload 取自 command_operation 当前行）。
template <typename Database>
ruvia::task<void> appendUpdatedEvent(Database& db, std::string_view commandId, std::string_view eventId) {
    using Operation = persistence::CommandOperationEntity;
    using Outbox = service::messaging::persistence::OutboxEventEntity;
    ruvia::db_query operation;
    operation.select({ operation.cast(operation.value(eventId), ruvia::db_data_type::uuid), operation.value("device.command.updated"), operation.value("command"), operation.cast(operation.column(Operation::column<"device_id">().name()), ruvia::db_data_type::text), operation.value("updated"), operation.value(2), operation.call("jsonb_build_object", { operation.cast(operation.value("device_code"), ruvia::db_data_type::text), operation.column(Operation::column<"device_code">().name()), operation.cast(operation.value("data"), ruvia::db_data_type::text), operation.call("jsonb_build_object", { operation.cast(operation.value("commandId"), ruvia::db_data_type::text), operation.cast(operation.column(Operation::column<"id">().name()), ruvia::db_data_type::text), operation.cast(operation.value("status"), ruvia::db_data_type::text), operation.column(Operation::column<"status">().name()), operation.cast(operation.value("reason"), ruvia::db_data_type::text), operation.column(Operation::column<"reason">().name()), operation.cast(operation.value("elements"), ruvia::db_data_type::text), operation.column(Operation::column<"elements">().name()), operation.cast(operation.value("actualValues"), ruvia::db_data_type::text), operation.column(Operation::column<"actual_values">().name()) }) }) })
        .from(Operation::table_name())
        .and_where(operation.binary(operation.column(Operation::column<"id">().name()), ruvia::db_binary_operator::equal, operation.cast(operation.value(commandId), ruvia::db_data_type::uuid)));
    ruvia::db_query outbox;
    outbox.insert_into(Outbox::table_name(), { Outbox::column<"id">().name(), Outbox::column<"event_type">().name(), Outbox::column<"aggregate_type">().name(), Outbox::column<"aggregate_id">().name(), Outbox::column<"action">().name(), Outbox::column<"schema_version">().name(), Outbox::column<"payload">().name() })
        .insert_from(operation);
    (void)co_await db.execute(outbox);
}

// 已接受命令的认领、物理下发与截止过期处理。
class CommandDispatchService final {
  public:
    // Claim is committed BEFORE touching the physical delivery path. A crashed or ambiguous
    // attempt is never replayed automatically: old EdgeNode cannot promise durable deduplication.
    template <typename Context>
    static ruvia::task<void> dispatch(Context& context) {
        auto tx = co_await context.db().begin_transaction();
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        ruvia::db_query pending;
        pending.select({ pending.cast(service::command::persistence::CommandAttemptEntity::column<"operation_id">().expression(pending, "a"), Type::text), service::command::persistence::CommandAttemptEntity::column<"queue_key">().expression(pending, "a"), service::command::persistence::CommandAttemptEntity::column<"queue_kind">().expression(pending, "a"), service::command::persistence::CommandAttemptEntity::column<"node_id">().expression(pending, "a"), pending.cast(service::command::persistence::CommandAttemptEntity::column<"max_length">().expression(pending, "a"), Type::text), pending.cast(service::command::persistence::CommandOperationEntity::column<"device_id">().expression(pending, "o"), Type::text), service::command::persistence::CommandOperationEntity::column<"device_code">().expression(pending, "o"), service::command::persistence::CommandOperationEntity::column<"protocol">().expression(pending, "o"), pending.cast(pending.cast(pending.binary(pending.extract(ruvia::db_date_part::epoch, service::command::persistence::CommandOperationEntity::column<"created_at">().expression(pending, "o")), Op::multiply, pending.value(1000)), Type::big_int), Type::text) })
            .from(service::command::persistence::CommandAttemptEntity::table_name(), "a")
            .join(ruvia::db_join_type::inner, service::command::persistence::CommandOperationEntity::table_name(), pending.binary(service::command::persistence::CommandOperationEntity::column<"id">().expression(pending, "o"), Op::equal, service::command::persistence::CommandAttemptEntity::column<"operation_id">().expression(pending, "a")), "o")
            .and_where(pending.unary(ruvia::db_unary_operator::is_null, service::command::persistence::CommandAttemptEntity::column<"claimed_at">().expression(pending, "a")))
            .and_where(pending.binary(service::command::persistence::CommandAttemptEntity::column<"deadline">().expression(pending, "a"), Op::greater, pending.call("now")))
            .and_where(pending.binary(service::command::persistence::CommandOperationEntity::column<"status">().expression(pending, "o"), Op::equal, pending.value("ACCEPTED")))
            .add_order_by(service::command::persistence::CommandOperationEntity::column<"created_at">().expression(pending, "o"))
            .lock({ .mode_ = ruvia::db_row_lock::update, .skip_locked_ = true, .tables_ = { "a", "o" } })
            .limit(16);
        const auto rows = co_await tx.query(pending);
        for (const auto& row : rows) {
            const auto id = row[0].value().value_or(std::string_view{});
            ruvia::db_query claim;
            claim.update(service::command::persistence::CommandAttemptEntity::table_name()).set(service::command::persistence::CommandAttemptEntity::column<"claimed_at">().name(), claim.call("now")).and_where(claim.binary(claim.column(service::command::persistence::CommandAttemptEntity::column<"operation_id">().name()), Op::equal, claim.cast(claim.value(id), Type::uuid)));
            (void)co_await tx.execute(claim);
            ruvia::db_query operation;
            operation.update(service::command::persistence::CommandOperationEntity::table_name()).set(service::command::persistence::CommandOperationEntity::column<"status">().name(), operation.value("DISPATCHING")).and_where(operation.binary(operation.column(service::command::persistence::CommandOperationEntity::column<"id">().name()), Op::equal, operation.cast(operation.value(id), Type::uuid)));
            (void)co_await tx.execute(operation);
        }
        co_await tx.commit();
        for (const auto& row : rows) {
            const auto cell = [&](std::size_t index) {
                return row[index].value().value_or(std::string_view{});
            };
            std::string failure;
            bool published = false;
            try {
                ruvia::db_query payload;
                payload.select(payload.column("value")).from(service::command::persistence::CommandAttemptEntity::table_name()).join_function(ruvia::db_join_type::cross, payload.call("jsonb_array_elements_text", { payload.column(service::command::persistence::CommandAttemptEntity::column<"payload">().name()) }), {}, "p", { .with_ordinality_ = true, .columns_ = { { .name_ = "value" }, { .name_ = "idx" } } }).and_where(payload.binary(payload.column(service::command::persistence::CommandAttemptEntity::column<"operation_id">().name()), Op::equal, payload.cast(payload.value(cell(0)), Type::uuid))).add_order_by(payload.column("idx"));
                const auto values = co_await context.db().query(payload);
                PendingDispatch item;
                item.task.messageId = cell(0);
                item.task.deviceId = cell(5);
                item.task.deviceCode = cell(6);
                item.task.protocol = cell(7);
                item.task.createdAtMs = utils::parseInt64(cell(8)).value_or(0);
                const bool list = cell(2) == "list";
                if (list) {
                    const auto bytes = message::fromHex(values.front()[0].value().value_or(std::string_view{}));
                    item.listPayload.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                } else {
                    for (std::size_t index = 0; index + 1 < values.size(); index += 2)
                        item.streamFields.push_back({ std::string(values[index][0].value().value_or(std::string_view{})), std::string(values[index + 1][0].value().value_or(std::string_view{})) });
                }
                const std::vector<PendingDispatch> pending{ std::move(item) };
                published = co_await dispatchPendingBatch(context.redis(), cell(1), list ? PendingQueueKind::List : PendingQueueKind::Stream, pending, static_cast<std::size_t>(utils::parseInt64(cell(4)).value_or(1)));
                if (!published)
                    failure = "queue_capacity_exceeded";
            } catch (const std::exception& error) {
                failure = error.what();
            }
            // A known capacity rejection is safe to report as rejected. Other failures can
            // occur after Redis committed the enqueue, so the outcome must stay unknown.
            const std::string_view next = published ? "AWAITING_RESULT" : failure == "queue_capacity_exceeded" ? "REJECTED"
                                                                                                               : "UNKNOWN";
            auto update = co_await context.db().begin_transaction();
            ruvia::db_query outcome;
            outcome.update(service::command::persistence::CommandOperationEntity::table_name()).set(service::command::persistence::CommandOperationEntity::column<"status">().name(), outcome.value(next)).set(service::command::persistence::CommandOperationEntity::column<"reason">().name(), outcome.value(failure)).set(service::command::persistence::CommandOperationEntity::column<"completed_at">().name(), outcome.case_when({ { outcome.binary(outcome.value(next), Op::equal, outcome.value("AWAITING_RESULT")), outcome.null_value() } }, outcome.call("now"))).and_where(outcome.binary(outcome.column(service::command::persistence::CommandOperationEntity::column<"id">().name()), Op::equal, outcome.cast(outcome.value(cell(0)), Type::uuid))).and_where(outcome.binary(outcome.column(service::command::persistence::CommandOperationEntity::column<"status">().name()), Op::equal, outcome.value("DISPATCHING")));
            (void)co_await update.execute(outcome);
            if (published) {
                ruvia::db_query dispatched;
                dispatched.update(service::command::persistence::CommandAttemptEntity::table_name()).set(service::command::persistence::CommandAttemptEntity::column<"dispatched_at">().name(), dispatched.call("now")).and_where(dispatched.binary(dispatched.column(service::command::persistence::CommandAttemptEntity::column<"operation_id">().name()), Op::equal, dispatched.cast(dispatched.value(cell(0)), Type::uuid)));
                (void)co_await update.execute(dispatched);
            } else {
                co_await appendUpdatedEvent(update, cell(0), service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()));
            }
            co_await update.commit();
            if (published && !cell(3).empty())
                co_await edge::dispatch::notifyNode(context.redis(), cell(3));
        }
        auto expiry = co_await context.db().begin_transaction();
        ruvia::db_query overdue;
        const auto unclaimed = overdue.unary(ruvia::db_unary_operator::is_null, service::command::persistence::CommandAttemptEntity::column<"claimed_at">().expression(overdue, "a"));
        overdue.update(service::command::persistence::CommandOperationEntity::table_name(), "o")
            .set(service::command::persistence::CommandOperationEntity::column<"status">().name(), overdue.case_when({ { unclaimed, overdue.value(service::message::command::expiredDispatchState(false)) } }, overdue.value(service::message::command::expiredDispatchState(true))))
            .set(service::command::persistence::CommandOperationEntity::column<"reason">().name(), overdue.case_when({ { unclaimed, overdue.value("dispatch_deadline_expired") } }, overdue.value("result_not_confirmed")))
            .set(service::command::persistence::CommandOperationEntity::column<"completed_at">().name(), overdue.call("now"))
            .update_from(service::command::persistence::CommandAttemptEntity::table_name(), "a")
            .and_where(overdue.binary(service::command::persistence::CommandOperationEntity::column<"id">().expression(overdue, "o"), Op::equal, service::command::persistence::CommandAttemptEntity::column<"operation_id">().expression(overdue, "a")))
            .and_where(overdue.binary(service::command::persistence::CommandAttemptEntity::column<"deadline">().expression(overdue, "a"), Op::less_equal, overdue.call("now")))
            .and_where(overdue.binary(service::command::persistence::CommandOperationEntity::column<"status">().expression(overdue, "o"), Op::in, overdue.list({ overdue.value("ACCEPTED"), overdue.value("DISPATCHING"), overdue.value("AWAITING_RESULT") })))
            .returning({ overdue.cast(service::command::persistence::CommandOperationEntity::column<"id">().expression(overdue, "o"), Type::text) });
        const auto expired = co_await expiry.query(overdue);
        for (const auto& row : expired)
            co_await appendUpdatedEvent(expiry, row[0].value().value_or(std::string_view{}), service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()));
        co_await expiry.commit();
    }

    // 下一次需要认领或过期处理的等待时长；无待处理命令时为空。
    template <typename Context>
    static ruvia::task<std::optional<std::chrono::milliseconds>>
    nextDispatchDelay(Context& context) {
        using Op = ruvia::db_binary_operator;
        ruvia::db_query delayQuery;
        const auto claimable = delayQuery.binary(delayQuery.binary(delayQuery.binary(service::command::persistence::CommandOperationEntity::column<"status">().expression(delayQuery, "o"), Op::equal, delayQuery.value("ACCEPTED")), Op::and_value, delayQuery.unary(ruvia::db_unary_operator::is_null, service::command::persistence::CommandAttemptEntity::column<"claimed_at">().expression(delayQuery, "a"))), Op::and_value, delayQuery.binary(service::command::persistence::CommandAttemptEntity::column<"deadline">().expression(delayQuery, "a"), Op::greater, delayQuery.call("now")));
        const auto deadlineMs = delayQuery.cast(delayQuery.call("ceil", { delayQuery.binary(delayQuery.extract(ruvia::db_date_part::epoch, delayQuery.binary(delayQuery.aggregate("min", { service::command::persistence::CommandAttemptEntity::column<"deadline">().expression(delayQuery, "a") }), Op::subtract, delayQuery.call("now"))), Op::multiply, delayQuery.value(1000)) }), ruvia::db_data_type::big_int);
        delayQuery.select(delayQuery.cast(delayQuery.case_when({ { delayQuery.binary(delayQuery.aggregate("count", { delayQuery.star() }), Op::equal, delayQuery.value(0)), delayQuery.null_value() }, { delayQuery.aggregate("bool_or", { claimable }), delayQuery.value(25) } }, delayQuery.greatest({ delayQuery.value(25), deadlineMs })), ruvia::db_data_type::text))
            .from(service::command::persistence::CommandAttemptEntity::table_name(), "a")
            .join(ruvia::db_join_type::inner, service::command::persistence::CommandOperationEntity::table_name(), delayQuery.binary(service::command::persistence::CommandOperationEntity::column<"id">().expression(delayQuery, "o"), Op::equal, service::command::persistence::CommandAttemptEntity::column<"operation_id">().expression(delayQuery, "a")), "o")
            .and_where(delayQuery.binary(service::command::persistence::CommandOperationEntity::column<"status">().expression(delayQuery, "o"), Op::in, delayQuery.list({ delayQuery.value("ACCEPTED"), delayQuery.value("DISPATCHING"), delayQuery.value("AWAITING_RESULT") })))
            .and_where(delayQuery.unary(ruvia::db_unary_operator::is_not_null, service::command::persistence::CommandAttemptEntity::column<"deadline">().expression(delayQuery, "a")));
        const auto rows = co_await context.db().query(delayQuery);
        if (rows.empty())
            co_return std::nullopt;
        const auto value = rows.front()[0].value();
        if (!value)
            co_return std::nullopt;
        const auto delay = utils::parseInt64(std::optional<std::string_view>{ *value });
        if (!delay)
            throw std::runtime_error("invalid command dispatch deadline delay");
        co_return std::chrono::milliseconds(*delay);
    }
};

class CommandResultService final {
  public:
    template <typename Context>
    static ruvia::task<void> project(
        Context& context, const std::vector<message::StreamMessage>& messages) {
        if (messages.empty())
            co_return;
        auto transaction = co_await context.db().begin_transaction();
        for (const auto& message : messages) {
            const auto id = message.get("command_id");
            const auto deviceId = message.get("device_id");
            if (!common::isUuid(id) || !common::isUuid(deviceId))
                continue;
            const auto explicitState = message.get("result_state");
            const auto state = service::message::command::terminalState(explicitState)
                ? explicitState
                : service::message::command::collectorResultState(message.get("success") == "1", message.get("reason"));
            const auto actual = actualValuesJson(message);
            // A late verified success may resolve UNKNOWN. An ambiguous failure or an
            // unconfirmed rejection must never turn it into a definitive outcome.
            const bool mayResolveUnknown = state == "SUCCEEDED" && message.get("success") == "1";
            using Op = ruvia::db_binary_operator;
            ruvia::db_query outcome;
            outcome.update(service::command::persistence::CommandOperationEntity::table_name()).set(service::command::persistence::CommandOperationEntity::column<"status">().name(), outcome.value(state)).set(service::command::persistence::CommandOperationEntity::column<"reason">().name(), outcome.value(message.get("reason"))).set(service::command::persistence::CommandOperationEntity::column<"actual_values">().name(), outcome.cast(outcome.value(actual), ruvia::db_data_type::jsonb)).set(service::command::persistence::CommandOperationEntity::column<"completed_at">().name(), outcome.call("now")).and_where(outcome.binary(outcome.column(service::command::persistence::CommandOperationEntity::column<"id">().name()), Op::equal, outcome.cast(outcome.value(id), ruvia::db_data_type::uuid))).and_where(outcome.binary(outcome.column(service::command::persistence::CommandOperationEntity::column<"device_id">().name()), Op::equal, outcome.cast(outcome.value(deviceId), ruvia::db_data_type::uuid))).and_where(outcome.binary(outcome.binary(outcome.column(service::command::persistence::CommandOperationEntity::column<"status">().name()), Op::in, outcome.list({ outcome.value(service::message::command::kResultAcceptingStatuses[0]), outcome.value(service::message::command::kResultAcceptingStatuses[1]) })), Op::or_value, outcome.binary(outcome.binary(outcome.column(service::command::persistence::CommandOperationEntity::column<"status">().name()), Op::equal, outcome.value("UNKNOWN")), Op::and_value, outcome.value(mayResolveUnknown)))).returning({ outcome.cast(outcome.column(service::command::persistence::CommandOperationEntity::column<"id">().name()), ruvia::db_data_type::text) });
            const auto updated = co_await transaction.query(outcome);
            if (!updated.empty())
                co_await appendUpdatedEvent(transaction, id, service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()));
        }
        co_await transaction.commit();
    }

  private:
    static std::size_t actualValueCount(const message::StreamMessage& message) {
        const auto value = message.get("actual_value_count");
        std::size_t count{};
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), count);
        if (error != std::errc{} || end != value.data() + value.size())
            return 0;
        return std::min<std::size_t>(count, 8);
    }

    static std::string actualValueField(std::size_t index, std::string_view name) {
        return "actual_value_" + std::to_string(index) + "_" + std::string(name);
    }

    static std::string actualValuesJson(const message::StreamMessage& message) {
        std::string output{"["};
        for (std::size_t index = 0; index < actualValueCount(message); ++index) {
            if (index != 0)
                output.push_back(',');
            output += "{\"elementId\":" + service::utils::jsonQuoted(
                          message.get(actualValueField(index, "element_id"))) +
                      ",\"name\":" + service::utils::jsonQuoted(
                          message.get(actualValueField(index, "name"))) +
                      ",\"kind\":" + service::utils::jsonQuoted(
                          message.get(actualValueField(index, "kind"))) +
                      ",\"value\":" + service::utils::jsonQuoted(
                          message.get(actualValueField(index, "value"))) +
                      ",\"unit\":" + service::utils::jsonQuoted(
                          message.get(actualValueField(index, "unit"))) + "}";
        }
        output.push_back(']');
        return output;
    }
};

class PreparationService final {
    struct Prepared {
        std::string queue;
        PendingQueueKind kind;
        std::vector<PendingDispatch> dispatches;
        std::size_t maximum;
        std::string nodeId;
    };

public:
    static ruvia::task<std::string> executeOperation(ruvia::web_worker_context& context, std::string_view operation, std::string_view payload, ruvia::stop_token stop) {
        if (stop.stop_requested()) {
            common::fail(10004, "Command preparation cancelled", 503);
        }
        if (operation != "prepare") {
            common::fail(18010, "Unknown command operation", 400);
        }
        co_return co_await PreparationService::prepare(context, payload);
    }

    static ruvia::task<std::string> prepare(ruvia::web_worker_context& context,
                                           std::string_view payload) {
        const auto request = ruvia::from_json<service::message::command::PrepareRequest>(payload, {.resource_ = context.pool()});
        if (!request) common::fail(18010, "Invalid command preparation request", 400);
        const auto id = request->get<"deviceId">().view();
        if (!common::isUuid(id)) common::fail(18010, "Invalid command preparation parameters", 400);
        const auto& values = request->get<"elements">();
        if (values.empty() || values.size() > 256)
            common::fail(18010, "Invalid command preparation elements", 400);
        std::vector<service::collector::CommandElementValue> requested;
        for (const auto& pair : values) {
            if (pair.size() != 2)
                common::fail(18010, "Invalid command preparation element", 400);
            requested.push_back({std::string(pair[0].view()), std::string(pair[1].view())});
        }
        auto transaction = co_await context.db("control").begin_transaction();
        const std::string mqttMessageId = request->get<"mqttMessageId">() ? std::string(request->get<"mqttMessageId">()->view()) : "";
        const auto prepared = co_await compileDevice(context, id, std::move(requested), transaction, mqttMessageId);
        co_await transaction.commit();
        co_return encode(prepared, context.pool());
    }

private:
    static std::string encode(const Prepared& prepared, std::pmr::memory_resource* resource) {
        const ruvia::model_options options{ .resource_ = resource };
        ruvia::boxed_array<service::message::command::PreparedCommand> commands(options);
        for (const auto& dispatch : prepared.dispatches) {
            const auto& task = dispatch.task;
            ruvia::boxed_array<service::message::command::PreparedElement> elements(options);
            for (const auto& [id, value] : task.elements) {
                elements.emplace().set<"elementId">(id).set<"value">(value);
            }
            ruvia::array<ruvia::string> payload(options);
            if (prepared.kind == PendingQueueKind::List) {
                const auto* begin = reinterpret_cast<const std::uint8_t*>(dispatch.listPayload.data());
                payload.emplace_back(message::toHex(
                    std::vector<std::uint8_t>(begin, begin + dispatch.listPayload.size())));
            } else {
                for (const auto& field : dispatch.streamFields) {
                    payload.emplace_back(field.name);
                    payload.emplace_back(field.value);
                }
            }
            commands.emplace()
                .set<"id">(task.messageId)
                .set<"deviceId">(task.deviceId)
                .set<"deviceCode">(task.deviceCode)
                .set<"protocol">(task.protocol)
                .set<"elements">(std::move(elements))
                .set<"payload">(std::move(payload));
        }
        service::message::command::PreparedBatch batch(options);
        batch.set<"queue">(prepared.queue)
            .set<"kind">(prepared.kind == PendingQueueKind::List ? "list" : "stream")
            .set<"maximum">(static_cast<std::int64_t>(prepared.maximum))
            .set<"nodeId">(prepared.nodeId)
            .set<"commands">(std::move(commands));
        return service::message::rpc::encode(batch, resource);
    }
    static ruvia::task<Prepared> compileDevice(ruvia::web_worker_context& context,
                  std::string_view deviceId, std::vector<service::collector::CommandElementValue> requested,
                  ruvia::db_transaction& transaction, std::string_view mqttMessageId) {

        // 调用方在命令事务中持有设备共享锁，准备事务只读取，避免跨连接重复加锁。

        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        ruvia::db_query routeQuery;
        const auto parameters = service::configuration::persistence::DeviceEntity::column<"protocol_params">().expression(routeQuery, "d");
        const auto text = [&](ruvia::db_expression object, std::string_view key) {
            return routeQuery.binary(object, Op::json_get_text, routeQuery.value(key));
        };
        const auto boolean = [&](ruvia::db_expression value) {
            return routeQuery.binary(routeQuery.call("lower", { routeQuery.coalesce({ value, routeQuery.value("") }) }), Op::in,
                routeQuery.list({ routeQuery.value("true"), routeQuery.value("t"), routeQuery.value("1"), routeQuery.value("yes"), routeQuery.value("y"), routeQuery.value("on") }));
        };
        const auto nodeConfig = routeQuery.binary(service::edge::persistence::EdgeNodeEntity::column<"status">().expression(routeQuery, "n"), Op::json_get, routeQuery.value("config"));
        const auto version = [&](std::string_view key) {
            const auto value = text(nodeConfig, key);
            return routeQuery.coalesce({ routeQuery.case_when({ { routeQuery.binary(routeQuery.coalesce({ value, routeQuery.value("") }), Op::regex,
                routeQuery.value("^-?[0-9]{1,18}$")), routeQuery.cast(value, Type::big_int) } }), routeQuery.value(0) });
        };
        auto applied = routeQuery.binary(service::configuration::persistence::LinkEntity::column<"status">().expression(routeQuery, "l"), Op::equal, routeQuery.value("enabled"));
        applied = routeQuery.binary(applied, Op::and_value, routeQuery.binary(service::edge::persistence::EdgeNodeEntity::column<"enrollment_status">().expression(routeQuery, "n"), Op::equal, routeQuery.value("approved")));
        applied = routeQuery.binary(applied, Op::and_value, boolean(text(service::edge::persistence::EdgeNodeEntity::column<"capability">().expression(routeQuery, "n"), "deviceConfig")));
        applied = routeQuery.binary(applied, Op::and_value, routeQuery.binary(routeQuery.coalesce({ text(nodeConfig, "state"), routeQuery.value("idle") }), Op::equal, routeQuery.value("applied")));
        applied = routeQuery.binary(applied, Op::and_value, routeQuery.binary(version("activeVersion"), Op::equal, version("desiredVersion")));
        routeQuery.select({ routeQuery.coalesce({ routeQuery.cast(service::configuration::persistence::LinkEntity::column<"edge_node_id">().expression(routeQuery, "l"), Type::text), routeQuery.value("") }), text(parameters, "device_code"), service::configuration::persistence::DeviceModelEntity::column<"protocol">().expression(routeQuery, "p"), routeQuery.case_when({ { routeQuery.binary(parameters, Op::json_has_key, routeQuery.value("remote_control")), boolean(text(parameters, "remote_control")) } }, routeQuery.value(true)), routeQuery.coalesce({ applied, routeQuery.value(false) }), routeQuery.coalesce({ routeQuery.null_if(text(service::configuration::persistence::DeviceModelEntity::column<"config">().expression(routeQuery, "p"), "commandFastReadDuration"), routeQuery.value("")), routeQuery.value("60") }), routeQuery.coalesce({ routeQuery.null_if(text(service::configuration::persistence::DeviceModelEntity::column<"config">().expression(routeQuery, "p"), "commandFastReadInterval"), routeQuery.value("")), routeQuery.value("1") }) })
            .from(service::configuration::persistence::DeviceEntity::table_name(), "d")
            .join(ruvia::db_join_type::inner, service::configuration::persistence::LinkEntity::table_name(), routeQuery.binary(service::configuration::persistence::LinkEntity::column<"id">().expression(routeQuery, "l"), Op::equal, service::configuration::persistence::DeviceEntity::column<"link_id">().expression(routeQuery, "d")), "l")
            .join(ruvia::db_join_type::inner, service::configuration::persistence::DeviceModelEntity::table_name(), routeQuery.binary(service::configuration::persistence::DeviceModelEntity::column<"device_id">().expression(routeQuery, "p"), Op::equal, service::configuration::persistence::DeviceEntity::column<"id">().expression(routeQuery, "d")), "p")
            .join(ruvia::db_join_type::left, service::edge::persistence::EdgeNodeEntity::table_name(), routeQuery.binary(service::edge::persistence::EdgeNodeEntity::column<"id">().expression(routeQuery, "n"), Op::equal, service::configuration::persistence::LinkEntity::column<"edge_node_id">().expression(routeQuery, "l")), "n")
            .and_where(routeQuery.binary(service::configuration::persistence::DeviceEntity::column<"id">().expression(routeQuery, "d"), Op::equal, routeQuery.cast(routeQuery.value(deviceId), Type::uuid)))
            .and_where(routeQuery.unary(ruvia::db_unary_operator::is_null, service::configuration::persistence::DeviceEntity::column<"deleted_at">().expression(routeQuery, "d")))
            .and_where(routeQuery.binary(service::configuration::persistence::DeviceEntity::column<"status">().expression(routeQuery, "d"), Op::equal, routeQuery.value("enabled")))
            .and_where(routeQuery.unary(ruvia::db_unary_operator::is_null, service::configuration::persistence::LinkEntity::column<"deleted_at">().expression(routeQuery, "l")))
            .and_where(routeQuery.unary(ruvia::db_unary_operator::is_null, service::configuration::persistence::DeviceModelEntity::column<"deleted_at">().expression(routeQuery, "p")))
            .and_where(service::configuration::persistence::DeviceModelEntity::column<"enabled">().expression(routeQuery, "p"))
            .limit(1);
        const auto edge = co_await transaction.query(routeQuery);
        if (!edge.empty() && !edge.front()[0].value().value_or(std::string_view{}).empty()) {
            if (!mqttMessageId.empty()) service::common::fail(18010, "MQTT 消息选择不适用于边缘协议", 400);
            if (edge.front()[4].value().value_or(std::string_view{}) != "t")
                service::common::fail(18013, "边缘节点设备配置尚未生效", 409);
            co_return co_await enqueueEdgeDevice(context, deviceId, std::move(requested),
                                                 edge.front()[0].value().value_or(std::string_view{}),
                                                 edge.front()[1].value().value_or(std::string_view{}),
                                                 edge.front()[2].value().value_or(std::string_view{}),
                                                 edge.front()[3].value().value_or(std::string_view{}) == "t",
                                                 boundedUnsigned(
                                                     edge.front()[5].value().value_or(std::string_view{}),
                                                     60, 0),
                                                 boundedUnsigned(
                                                     edge.front()[6].value().value_or(std::string_view{}),
                                                     1, 1), transaction);
        }

        auto devices = co_await service::configuration::loadDeviceDefinitions(transaction, deviceId);
        if (devices.empty()) {
            service::common::fail(18011, "设备、链路或协议配置未启用", 409);
        }
        const auto device = devices.begin();
        try {
            (void)service::collector::command::resolve(*device, requested, mqttMessageId);
        } catch (const std::invalid_argument& error) {
            service::common::fail(18010, error.what(), 400);
        }

        const auto route = co_await deviceRoute(context.redis(), device->id);
        if (!route) {
            service::common::fail(18013, "设备离线或没有可用的南桥连接", 409);
        }
        const auto& definition = service::collector::protocolDefinition(device->protocol);
        auto tasks = service::collector::command::groupElements(definition, std::move(requested));

        std::vector<PendingDispatch> dispatches;
        dispatches.reserve(tasks.size());
        for (const auto& elements : tasks) {
            PendingDispatch dispatch;
            auto& task = dispatch.task;
            task.messageId = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
            task.groupKey = "device:" + device->id;
            task.protocol = device->protocol;
            task.transport = definition.commandTransport == service::collector::CommandTransport::DeviceConfigured
                ? device->modbusMode : "RAW";
            task.kind = "command";
            task.mqttMessageId = mqttMessageId;
            task.linkId = device->linkId;
            task.deviceId = device->id;
            task.deviceCode = device->code;
            task.responseTimeoutMs = 5000;
            task.maxAttempts = 1;
            task.connectionId = route->connectionId;
            task.sessionEpoch = route->sessionEpoch;
            task.createdAtMs = message::utcNowMilliseconds();
            for (const auto& element : elements)
                task.elements.emplace_back(element.elementId, element.value);
            dispatch.streamFields = message::protocolTaskFields(task);
            dispatches.push_back(std::move(dispatch));
        }
        co_return Prepared{
            .queue = message::commandStream(route->workerIndex, true, route->instanceId),
            .kind = PendingQueueKind::Stream,
            .dispatches = std::move(dispatches),
            .maximum = 10000
        };
    }

    static ruvia::task<Prepared>
    enqueueEdgeDevice(ruvia::web_worker_context& context, std::string_view deviceId,
                      std::vector<service::collector::CommandElementValue> requested,
                      std::string_view nodeId, std::string_view deviceCode,
                      std::string_view protocol, bool remoteControl,
                      std::uint32_t fastReadDurationSec,
                      std::uint32_t fastReadIntervalSec,
                      ruvia::db_transaction& transaction) {
        if (!remoteControl)
            service::common::fail(18005, "设备未开启远程控制", 403);
        if (!co_await context.redis().get(service::message::edge::sessionKey(nodeId)))
            service::common::fail(18013, "边缘节点离线，无法下发设备命令", 409);

        service::collector::DeviceDefinition device;
        device.id = std::string(deviceId);
        device.code = std::string(deviceCode);
        device.protocol = std::string(protocol);
        co_await loadEdgeElements(transaction, device);
        service::collector::command::ResolvedCommand resolved;
        try {
            resolved = service::collector::command::resolve(device, requested);
        } catch (const std::invalid_argument& error) {
            service::common::fail(18010, error.what(), 400);
        }
        if (device.protocol == "SL651" && resolved.elements.size() > 8)
            service::common::fail(18010, "SL651 边缘命令最多包含 8 个要素", 400);

        auto tasks = service::collector::command::groupElements(
            service::collector::protocolDefinition(device.protocol), std::move(requested));

        std::vector<PendingDispatch> dispatches;
        dispatches.reserve(tasks.size());
        for (const auto& elements : tasks) {
            PendingDispatch dispatch;
            auto& task = dispatch.task;
            task.messageId = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
            task.groupKey = "device:" + device.id;
            task.protocol = device.protocol;
            task.transport = "EDGE";
            task.kind = "command";
            task.deviceId = device.id;
            task.deviceCode = device.code;
            task.responseTimeoutMs = 5000;
            task.maxAttempts = 1;
            task.createdAtMs = message::utcNowMilliseconds();
            for (const auto& element : elements)
                task.elements.emplace_back(element.elementId, element.value);

            auto envelope = service::edge::protocol::outbound(service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()), service::message::utcNowMilliseconds(), context.template worker_state<service::edge::config::PlatformIdentity>().id, nodeId);
            auto* command = envelope.mutable_command_request();
            if (!setUuid(command->mutable_command_id(), task.messageId) ||
                !setUuid(command->mutable_device_id(), task.deviceId))
                service::common::fail(18010, "边缘命令标识无效", 500);
            command->set_timeout_ms(5000);
            command->set_readback_count(1);
            command->set_fast_read_duration_sec(fastReadDurationSec);
            command->set_fast_read_interval_sec(fastReadIntervalSec);
            for (const auto& element : elements) {
                if (element.elementId.size() > 64 || element.value.size() > 128)
                    service::common::fail(
                        18010, "边缘命令要素 ID 最长 64 字符、值最长 128 字符", 400);
                auto* value = command->add_values();
                value->set_element_id(element.elementId);
                auto* expected = value->mutable_expected();
                expected->set_kind(service::edge::pb::VALUE_STRING);
                expected->set_string_value(element.value);
            }
            dispatch.listPayload = service::edge::protocol::encode(envelope);
            if (dispatch.listPayload.empty())
                service::common::fail(18010, "边缘命令编码失败", 500);
            dispatches.push_back(std::move(dispatch));
        }
        co_return Prepared{
            .queue = service::message::edge::commandQueueKey(nodeId),
            .kind = PendingQueueKind::List, .dispatches = std::move(dispatches),
            .maximum = 1024, .nodeId = std::string(nodeId)};

    }

    static ruvia::task<void> loadEdgeElements(ruvia::db_transaction& transaction,
                                               service::collector::DeviceDefinition& device) {
        using Op = ruvia::db_binary_operator;
        using Type = ruvia::db_data_type;
        if (device.protocol != "Modbus" && device.protocol != "S7" && device.protocol != "SL651" &&
            device.protocol != "MC" && device.protocol != "FINS" && device.protocol != "DLT645")
            service::common::fail(18010, "边缘节点不支持该设备协议", 400);
        ruvia::db_query elements;
        const auto field = [&](std::string_view key) {
            return elements.binary(elements.column("item"), Op::json_get_text, elements.value(key));
        };
        const auto integer = [&](std::string_view key, int fallback) {
            const auto value = field(key);
            return elements.coalesce({ elements.case_when({ { elements.binary(elements.coalesce({ value, elements.value("") }),
                Op::regex, elements.value("^-?[0-9]{1,18}$")), elements.cast(value, Type::integer) } }), elements.value(fallback) });
        };
        const auto functionText = [&](std::string_view key) {
            return elements.binary(elements.column("func"), Op::json_get_text, elements.value(key));
        };
        const auto isIndustrial = device.protocol == "MC" || device.protocol == "FINS" || device.protocol == "DLT645";
        const auto isDlt645 = device.protocol == "DLT645";
        const auto isSl651 = device.protocol == "SL651";
        const auto isS7 = device.protocol == "S7";
        const auto writable = elements.binary(elements.call("lower", { elements.coalesce({ field("writable"), elements.value("") }) }), Op::in,
            elements.list({ elements.value("true"), elements.value("t"), elements.value("1"), elements.value("yes"), elements.value("y"), elements.value("on") }));
        elements.select({ field("id"), field("name"), elements.coalesce({ field("unit"), elements.value("") }), isSl651 ? elements.value("") : isS7 ? elements.coalesce({ field("dataType"), elements.value("BOOL") })
                                                                                                                                                    : field("dataType"),
                          isSl651 ? functionText("dir") : elements.value(""),
                          (isSl651 || isDlt645) ? integer("length", 1) : isS7 ? integer("size", 1)
                                                                              : elements.value(0),
                          (isSl651 || isDlt645) ? integer("digits", 0) : elements.value(0),
                          isSl651 ? elements.binary(functionText("dir"), Op::equal, elements.value("DOWN")) : writable,
                          isSl651 ? elements.column("response_element") : elements.cast(elements.value(false), Type::boolean),
                          isSl651 ? functionText("funcCode") : elements.value(""),
                          isSl651 ? field("encode") : isDlt645 ? field("dataType")
                                                               : elements.value("") })
            .from(service::configuration::persistence::DeviceEntity::table_name(), "d")
            .join(ruvia::db_join_type::inner, service::configuration::persistence::LinkEntity::table_name(), elements.binary(service::configuration::persistence::LinkEntity::column<"id">().expression(elements, "l"), Op::equal, service::configuration::persistence::DeviceEntity::column<"link_id">().expression(elements, "d")), "l")
            .join(ruvia::db_join_type::inner, service::configuration::persistence::DeviceModelEntity::table_name(), elements.binary(service::configuration::persistence::DeviceModelEntity::column<"device_id">().expression(elements, "p"), Op::equal, service::configuration::persistence::DeviceEntity::column<"id">().expression(elements, "d")), "p")
            .join_function(ruvia::db_join_type::cross, elements.call("jsonb_array_elements", { elements.coalesce({ elements.binary(service::configuration::persistence::DeviceModelEntity::column<"config">().expression(elements, "p"), Op::json_get, elements.value(isSl651 ? "funcs" : isS7 ? "areas"
                                                                                                                                                                                                                                                                          : isIndustrial       ? "points"
                                                                                                                                                                                                                                                                                               : "registers")),
                                                                                                                   elements.cast(elements.value("[]"), Type::jsonb) }) }),
                           {},
                           isSl651 ? "func" : "item",
                           { .lateral_ = true })
            .and_where(elements.binary(service::configuration::persistence::DeviceEntity::column<"id">().expression(elements, "d"), Op::equal, elements.cast(elements.value(device.id), Type::uuid)))
            .and_where(elements.binary(service::configuration::persistence::DeviceModelEntity::column<"protocol">().expression(elements, "p"), Op::equal, elements.value(device.protocol)))
            .and_where(elements.binary(service::configuration::persistence::LinkEntity::column<"execution">().expression(elements, "l"), Op::equal, elements.value("edge")))
            .and_where(elements.unary(ruvia::db_unary_operator::is_null, service::configuration::persistence::LinkEntity::column<"deleted_at">().expression(elements, "l")));
        if (isSl651) {
            const auto functionFields = [](std::string_view key, bool response) {
                ruvia::db_query query;
                query.select({ query.alias(query.column("value"), "item"), query.alias(query.cast(query.value(response), Type::boolean), "response_element") })
                    .from_function(query.call("jsonb_array_elements", { query.coalesce({
                        query.binary(query.column("func"), Op::json_get, query.value(key)), query.cast(query.value("[]"), Type::jsonb) }) }), "field");
                return query;
            };
            auto fields = functionFields("elements", false);
            const auto response = functionFields("responseElements", true);
            fields.combine(ruvia::db_set_operation::union_all, response);
            elements.join(ruvia::db_join_type::cross, fields, {}, "configured", { .lateral_ = true });
        }
        const auto rows = co_await transaction.query(elements);
        for (const auto& row : rows) {
            service::collector::ElementDefinition element;
            element.id = std::string(row[0].value().value_or(std::string_view{}));
            element.name = std::string(row[1].value().value_or(std::string_view{}));
            element.unit = std::string(row[2].value().value_or(std::string_view{}));
            element.dataType = std::string(row[3].value().value_or(std::string_view{}));
            element.direction = std::string(row[4].value().value_or(std::string_view{}));
            element.size = parseInteger(row[5].value().value_or(std::string_view{}));
            element.length = parseInteger(row[5].value().value_or(std::string_view{}));
            element.digits = parseInteger(row[6].value().value_or(std::string_view{}));
            element.writable = row[7].value().value_or(std::string_view{}) == "t";
            element.responseElement = row[8].value().value_or(std::string_view{}) == "t";
            element.functionCode = std::string(row[9].value().value_or(std::string_view{}));
            element.encoding = std::string(row[10].value().value_or(std::string_view{}));
            device.elements.push_back(std::move(element));
        }
    }

    static std::int64_t parseInteger(std::string_view value) {
        std::int64_t output{};
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), output);
        return error == std::errc{} && end == value.data() + value.size() ? output : 0;
    }

    static bool setUuid(std::string* output, std::string_view text) {
        std::uint8_t value[16]{};
        if (!service::common::uuidBytes(text, value))
            return false;
        output->assign(service::edge::protocol::bytes(value, sizeof(value)));
        return true;
    }



    static std::uint32_t boundedUnsigned(std::string_view value, std::uint32_t fallback,
                                         std::uint32_t minimum) {
        std::uint32_t result{};
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (error != std::errc{} || end != value.data() + value.size() ||
            result < minimum || result > 3600U)
            return fallback;
        return result;
    }


};

} // namespace service::command
