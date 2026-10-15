#pragma once

#include <tuple>

#include "service/common/uuid.h"

#include <memory>

#include "service/features/edge/gateway/gateway.entity.h"

#include <charconv>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>

#include "service/common/http.h"
#include "service/features/edge/gateway/gateway.types.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/live/live.service.h"

namespace service::edge::gateway {

class GatewayService final {
  public:
    static ruvia::task<EnrollmentRecord> loadEnrollment(ruvia::context& c,
        std::string_view imei, std::string_view pendingNodeId = {}) {
        const auto value = co_await c.redis().get(service::message::edge::authKey(imei));
        if (value) {
            const auto record = EnrollmentRecord::decode(std::string_view(value->data(), value->size()));
            if (record) co_return *record;
        }
        co_return EnrollmentRecord{pendingNodeId.empty() ? c.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next() :
                                   std::string(pendingNodeId), "pending"};
    }

    static ruvia::task<void> saveLogResult(ruvia::context& c, std::string_view nodeId, const pb::LogResult& result) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        LogResultRecord record;
        if (!result.SerializeToString(&record.protobufBytes)) {
            co_return;
        }
        const auto id = protocol::uuidText(result.request_id());
        ruvia::redis_set_options options;
        options.expiration_ =
            ruvia::redis_set_expiration::expires_after(std::chrono::seconds(60));
        const auto key = LogResultRecord::responseKey(id);
        co_await c.redis().set(key, record.protobufBytes, std::move(options));
        co_await service::live::publish(c.redis(), key);
        if (result.success()) {
            const auto snapshotKey = LogResultRecord::snapshotKey(nodeId);
            co_await c.redis().set(snapshotKey, record.protobufBytes);
            co_await service::live::publish(c.redis(), snapshotKey);
        }
    }

    static ruvia::task<void> saveLogLevelResult(ruvia::context& c, const pb::LogLevelResult& result) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        LogResultRecord record;
        if (!result.SerializeToString(&record.protobufBytes)) {
            co_return;
        }
        const auto id = protocol::uuidText(result.request_id());
        ruvia::redis_set_options options;
        options.expiration_ =
            ruvia::redis_set_expiration::expires_after(std::chrono::seconds(60));
        const auto key = LogResultRecord::levelKey(id);
        co_await c.redis().set(key, record.protobufBytes, std::move(options));
        co_await service::live::publish(c.redis(), key);
    }


    template <typename Context>
    static ruvia::task<std::int64_t> databaseTimeMilliseconds(Context& context) {
        ruvia::db_query sample(context.pool());
        const auto epochMilliseconds = sample.cast(
            sample.call("floor", {sample.binary(
                sample.extract(ruvia::db_date_part::epoch, sample.call("clock_timestamp")),
                ruvia::db_binary_operator::multiply, sample.value(std::int64_t{1000}))}),
            ruvia::db_data_type::big_int);
        sample.select(sample.cast(epochMilliseconds, ruvia::db_data_type::text));
        const auto rows = co_await context.db().query(sample);
        if (rows.empty())
            throw std::runtime_error("PostgreSQL clock sample is unavailable");
        const auto sampledValue = rows.front()[0].value();
        if (!sampledValue)
            throw std::runtime_error("PostgreSQL clock sample is unavailable");
        const auto value = *sampledValue;
        std::int64_t milliseconds{};
        const auto parsed = std::from_chars(value.data(), value.data() + value.size(), milliseconds);
        if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size())
            throw std::runtime_error("PostgreSQL clock sample is invalid");
        co_return milliseconds;
    }

    template <typename Context>
    static ruvia::task<std::optional<std::int64_t>> claimCommand(
        Context& context, std::string_view operationId, std::string_view nodeId) {
        ruvia::db_query claim(context.pool());
        using Binary = ruvia::db_binary_operator;
        const auto deadlineMs = claim.cast(
            claim.call("floor", {claim.binary(
                claim.extract(ruvia::db_date_part::epoch,
                    claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandAttemptEntity::column_index<"deadline">(), service::edge::gateway::persistence::CommandAttemptEntity::columns_type>::name.view(), "a")),
                Binary::multiply, claim.value(1000))}),
            ruvia::db_data_type::big_int);
        claim.update(service::edge::gateway::persistence::CommandAttemptEntity::table_name(), "a")
            .set(std::tuple_element_t<service::edge::gateway::persistence::CommandAttemptEntity::column_index<"sent_at">(), service::edge::gateway::persistence::CommandAttemptEntity::columns_type>::name.view(), claim.call("now"))
            .update_from(service::edge::gateway::persistence::CommandOperationEntity::table_name(), "o")
            .and_where(claim.binary(claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandAttemptEntity::column_index<"operation_id">(), service::edge::gateway::persistence::CommandAttemptEntity::columns_type>::name.view(), "a"), Binary::equal,
                claim.cast(claim.value(operationId), ruvia::db_data_type::uuid)))
            .and_where(claim.binary(claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandOperationEntity::column_index<"id">(), service::edge::gateway::persistence::CommandOperationEntity::columns_type>::name.view(), "o"), Binary::equal,
                claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandAttemptEntity::column_index<"operation_id">(), service::edge::gateway::persistence::CommandAttemptEntity::columns_type>::name.view(), "a")))
            .and_where(claim.binary(claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandAttemptEntity::column_index<"node_id">(), service::edge::gateway::persistence::CommandAttemptEntity::columns_type>::name.view(), "a"), Binary::equal,
                claim.value(nodeId)))
            .and_where(claim.unary(ruvia::db_unary_operator::is_null,
                claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandAttemptEntity::column_index<"sent_at">(), service::edge::gateway::persistence::CommandAttemptEntity::columns_type>::name.view(), "a")))
            .and_where(claim.binary(claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandAttemptEntity::column_index<"deadline">(), service::edge::gateway::persistence::CommandAttemptEntity::columns_type>::name.view(), "a"), Binary::greater,
                claim.call("now")))
            .and_where(claim.binary(claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandOperationEntity::column_index<"status">(), service::edge::gateway::persistence::CommandOperationEntity::columns_type>::name.view(), "o"), Binary::in,
                claim.list({claim.value("DISPATCHING"), claim.value("AWAITING_RESULT")})))
            .returning({claim.cast(claim.column(std::tuple_element_t<service::edge::gateway::persistence::CommandAttemptEntity::column_index<"operation_id">(), service::edge::gateway::persistence::CommandAttemptEntity::columns_type>::name.view(), "a"), ruvia::db_data_type::text), deadlineMs});
        const auto claimed = co_await context.db().query(claim);
        if (claimed.empty())
            co_return std::nullopt;
        // sent_at is already committed. A bad returned value must never reopen the attempt;
        // the command deadline sweep will conservatively transition it to UNKNOWN.
        const auto value = claimed.front()[1].value();
        if (!value)
            throw std::runtime_error("claimed command attempt has no persisted deadline");
        std::int64_t milliseconds{};
        const auto parsed = std::from_chars(value->data(), value->data() + value->size(), milliseconds);
        if (parsed.ec != std::errc{} || parsed.ptr != value->data() + value->size())
            throw std::runtime_error("claimed command attempt deadline is invalid");
        co_return milliseconds;
    }

    static void applyCommandDeadline(pb::CommandRequest& request,
                                     bool supportsCommandStartBefore,
                                     std::optional<std::int64_t> deadlineMs) {
        if (!supportsCommandStartBefore) {
            if (request.has_start_before_ms())
                throw std::invalid_argument("command start deadline was not negotiated");
            return;
        }
        if (request.has_start_before_ms())
            throw std::invalid_argument("command start deadline must come from its persisted attempt");
        if (!deadlineMs)
            throw std::invalid_argument("negotiated command start deadline is unavailable");
        request.set_start_before_ms(*deadlineMs);
    }

    template <typename Context>
    static ruvia::task<std::optional<FirmwareSource>> loadFirmwareSource(
        Context& context, std::string_view requestId, std::string_view nodeId) {
        ruvia::db_query firmware(context.pool());
        using Binary = ruvia::db_binary_operator;
        firmware.select({firmware.column(std::tuple_element_t<service::edge::gateway::persistence::EdgeFirmwareEntity::column_index<"storage_path">(), service::edge::gateway::persistence::EdgeFirmwareEntity::columns_type>::name.view(), "firmware"),
                         firmware.column(std::tuple_element_t<service::edge::gateway::persistence::EdgeFirmwareEntity::column_index<"size_bytes">(), service::edge::gateway::persistence::EdgeFirmwareEntity::columns_type>::name.view(), "firmware")})
            .from(service::edge::gateway::persistence::EdgeTaskEntity::table_name(), "task")
            .join(ruvia::db_join_type::inner, service::edge::gateway::persistence::EdgeFirmwareEntity::table_name(),
                firmware.binary(
                    firmware.cast(firmware.column(std::tuple_element_t<service::edge::gateway::persistence::EdgeFirmwareEntity::column_index<"id">(), service::edge::gateway::persistence::EdgeFirmwareEntity::columns_type>::name.view(), "firmware"), ruvia::db_data_type::text),
                    Binary::equal,
                    firmware.binary(firmware.column(std::tuple_element_t<service::edge::gateway::persistence::EdgeTaskEntity::column_index<"request">(), service::edge::gateway::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), Binary::json_get_text,
                        firmware.value("firmware_id"))), "firmware")
            .and_where(firmware.binary(firmware.column(std::tuple_element_t<service::edge::gateway::persistence::EdgeTaskEntity::column_index<"id">(), service::edge::gateway::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), Binary::equal,
                firmware.cast(firmware.value(requestId), ruvia::db_data_type::uuid)))
            .and_where(firmware.binary(firmware.column(std::tuple_element_t<service::edge::gateway::persistence::EdgeTaskEntity::column_index<"node_id">(), service::edge::gateway::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), Binary::equal,
                firmware.cast(firmware.value(nodeId), ruvia::db_data_type::uuid)))
            .and_where(firmware.binary(firmware.column(std::tuple_element_t<service::edge::gateway::persistence::EdgeTaskEntity::column_index<"task_type">(), service::edge::gateway::persistence::EdgeTaskEntity::columns_type>::name.view(), "task"), Binary::equal,
                firmware.value("firmware")))
            .limit(1);
        const auto rows = co_await context.db().query(firmware);
        if (rows.empty())
            co_return std::nullopt;

        FirmwareSource source;
        source.storagePath =
            std::string(rows.front()[0].value().value_or(std::string_view{}));
        const auto sizeText = rows.front()[1].value().value_or(std::string_view{});
        const auto parsed = std::from_chars(sizeText.data(), sizeText.data() + sizeText.size(),
                                            source.sizeBytes);
        if (source.storagePath.empty() || source.sizeBytes == 0 || parsed.ec != std::errc{} ||
            parsed.ptr != sizeText.data() + sizeText.size())
            co_return std::nullopt;
        co_return source;
    }
};

} // namespace service::edge::gateway
