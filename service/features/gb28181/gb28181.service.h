#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <memory_resource>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/model.h>
#include <ruvia/web/redis/redis_handle.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/features/gb28181/gb28181.config.h"
#include "service/features/gb28181/gb28181.entity.h"
#include "service/features/gb28181/gb28181.protocol.h"
#include "service/features/gb28181/gb28181.types.h"
#include "service/utils/log.h"
#include "service/utils/redis.h"
#include "service/utils/uuid.h"

namespace service::gb28181 {

// KEYS[1]=Collector control stream, KEYS[2]=owner key, KEYS[3]=control input channel.
// ARGV[1]=owner token, ARGV[2..]=command fields.
inline constexpr std::string_view kPublishControlCommandScript = R"lua(
if redis.call('GET',KEYS[2])~=ARGV[1] then return 0 end
local id = redis.call('XADD',KEYS[1],'*',unpack(ARGV,2))
redis.call('EXPIRE',KEYS[1],600)
redis.call('PUBLISH', KEYS[3], id)
return 1
)lua";

// Claim and owner/cancel/deadline checks are atomic in Redis.  The claim lives
// longer than the mandatory maximum command lifetime.
// KEYS[1]=owner key, KEYS[2]=claim key, KEYS[3]=cancel key.
// ARGV[1]=owner token, ARGV[2]=deadline in Unix milliseconds.
inline constexpr std::string_view kClaimControlExecutionScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('EXISTS', KEYS[3]) == 1 then return -2 end
local t = redis.call('TIME')
if tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000) >= tonumber(ARGV[2]) then return -3 end
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'EX', 600) then return 1 end
return 0
)lua";

// The first stream ID is the actor's ordering cursor.  A later repair may need
// a new Redis entry after the old one has been acknowledged/deleted, but it
// must carry this original order so a retry cannot move the DB cursor backwards
// or reapply an older event over a newer one.  Every new entry wakes the
// Service Workers of the publishing process.
// KEYS[1]=projection stream, KEYS[2]=sent marker, KEYS[3]=Service wake stream.
// KEYS[4]=projection input channel, KEYS[5]=Service wake input channel.
// ARGV[1]=local order ID, ARGV[2]=local entry ID, ARGV[3]=wake task,
// ARGV[4]=wake stream capacity, ARGV[5..]=projection fields.
inline constexpr std::string_view kPublishProjectionScript = R"lua(
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
    -- it once, without refreshing an existing marker on every attempt.
    if not marker then
      redis.call('SET', KEYS[2], order .. '|' .. current, 'EX', 86400)
    end
    return { order, current }
  end
end

local values = {}
for index = 5, #ARGV do
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
local wake = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[4], '*', 'task', ARGV[3])
redis.call('PUBLISH', KEYS[4], id)
redis.call('PUBLISH', KEYS[5], wake)
return { order, id }
)lua";

// Durable receipt write, PEL removal and the direct Pub/Sub notification to the
// publishing Collector Worker are one Redis transaction. The receipt key remains
// authoritative when the Collector misses the notification.
// KEYS[1]=receipt key, KEYS[2]=projection stream, KEYS[3]=Collector receipt topic,
// KEYS[4]=projection pending deadlines, KEYS[5]=projection deadline change channel.
// ARGV[1]=applied (0/1), ARGV[2]=receipt TTL seconds, ARGV[3]=group,
// ARGV[4]=entry ID, ARGV[5]=projection ID.
inline constexpr std::string_view kMarkProjectionDoneScript = R"lua(
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
local acknowledged = redis.call('XACK', KEYS[2], ARGV[3], ARGV[4])
redis.call('XDEL', KEYS[2], ARGV[4])
if redis.call('ZREM', KEYS[4], ARGV[4]) > 0 then
  redis.call('PUBLISH', KEYS[5], ARGV[4])
end
if KEYS[3] ~= '' then
  redis.call('PUBLISH', KEYS[3], ARGV[5])
end
return acknowledged
)lua";

// Owner leases also maintain the owner deadline set scored by Redis time.  A
// deadline earlier than every tracked deadline wakes the local Service Workers,
// whose expiry wait was computed from the previous earliest deadline.
// KEYS[1]=owner key, KEYS[2]=owner deadline set, KEYS[3]=Service wake stream.
// KEYS[4]=Service wake input channel.
// ARGV[1]=token, ARGV[2]=previous token ('' claims only a free owner),
// ARGV[3]=lease milliseconds, ARGV[4]=deadline member ('' for session owners),
// ARGV[5]=wake task, ARGV[6]=wake stream capacity.
inline constexpr std::string_view kClaimOwnerLeaseScript = R"lua(
local function track()
  if ARGV[4] == '' then return end
  local t = redis.call('TIME')
  local deadline = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000) + tonumber(ARGV[3])
  local first = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
  redis.call('ZADD', KEYS[2], deadline, ARGV[4])
  if #first == 0 or deadline < tonumber(first[2]) then
    local id = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[6], '*', 'task', ARGV[5])
    redis.call('PUBLISH', KEYS[4], id)
  end
end
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  track()
  return 1
end
if ARGV[2] ~= '' then
  if current ~= ARGV[2] then return 0 end
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
  track()
  return 1
end
if current then return 0 end
if redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3], 'NX') then
  track()
  return 1
end
return 0
)lua";

// Renewal only moves a deadline later, so it never wakes a Service Worker.
// KEYS[1]=owner key, KEYS[2]=owner deadline set.
// ARGV[1]=token, ARGV[2]=lease milliseconds, ARGV[3]=deadline member ('' for session owners).
inline constexpr std::string_view kRenewOwnerLeaseScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
if ARGV[3] ~= '' then
  local t = redis.call('TIME')
  redis.call('ZADD', KEYS[2], tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000) + tonumber(ARGV[2]), ARGV[3])
end
return 1
)lua";

// A released owner keeps its deadline unless its offline state is durable, so
// a Service Worker reconciles the aggregate when the deadline is due.
// KEYS[1]=owner key, KEYS[2]=owner deadline set.
// ARGV[1]=token, ARGV[2]=deadline member to remove ('' keeps the deadline).
inline constexpr std::string_view kReleaseOwnerLeaseScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if ARGV[2] ~= '' then
  redis.call('ZREM', KEYS[2], ARGV[2])
end
return redis.call('DEL', KEYS[1])
)lua";

// Claims due owner deadlines for one Service Worker.  A member whose owner key
// still lives is rescheduled to the owner's expiry.  A member without an owner
// is claimed by moving its score to a processing deadline: if the claimer dies
// before completing it, the member becomes due again.
// KEYS[1]=owner deadline set.
// ARGV[1]=owner key prefix, ARGV[2]=claim limit, ARGV[3]=processing milliseconds.
// Returns { now, next deadline or '', member, claim score, ... }.
inline constexpr std::string_view kClaimDueOwnersScript = R"lua(
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
local claimed = {}
for _, member in ipairs(due) do
  local separator = string.find(member, ':', 1, true)
  if not separator then
    redis.call('ZREM', KEYS[1], member)
  else
    local ttl = redis.call('PTTL', ARGV[1] .. string.sub(member, separator + 1))
    if ttl > 0 then
      redis.call('ZADD', KEYS[1], now + ttl, member)
    else
      local score = now + tonumber(ARGV[3])
      redis.call('ZADD', KEYS[1], score, member)
      if ttl == -2 then
        claimed[#claimed + 1] = member
        claimed[#claimed + 1] = tostring(score)
      end
    end
  end
end
local first = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local result = { tostring(now), first[2] or '' }
for index = 1, #claimed do
  result[#result + 1] = claimed[index]
end
return result
)lua";

// KEYS[1]=owner deadline set.  ARGV[1]=member, ARGV[2]=claim score.
inline constexpr std::string_view kCompleteOwnerExpiryScript = R"lua(
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) == tonumber(ARGV[2]) then
  return redis.call('ZREM', KEYS[1], ARGV[1])
end
return 0
)lua";

// Collector streams belong to one process incarnation and expire after it
// stops refreshing them.
// KEYS[1]=stream.  ARGV[1]=group, ARGV[2]=TTL seconds.
inline constexpr std::string_view kCreateCollectorStreamScript = R"lua(
local created = redis.pcall('XGROUP', 'CREATE', KEYS[1], ARGV[1], '0', 'MKSTREAM')
if type(created) == 'table' and created.err and string.sub(created.err, 1, 9) ~= 'BUSYGROUP' then
  return redis.error_reply(created.err)
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1
)lua";

// Persist the result before removing the pending command. Pub/Sub is only a
// wake-up hint: the caller rereads the result after subscribing again.
// KEYS[1]=reply topic, KEYS[2]=control stream, KEYS[3]=result key,
// KEYS[4]=claim key, KEYS[5]=pending deadline key, KEYS[6]=deadline channel.
// ARGV[1]=group, ARGV[2]=entry ID, ARGV[3]=encoded result,
// ARGV[4]=result TTL, ARGV[5]=control TTL, ARGV[6]=claim owner.
inline constexpr std::string_view kPublishReplyAndAcknowledgeScript = R"lua(
if ARGV[3] ~= '' and redis.call('EXISTS', KEYS[3]) == 0 then
  redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[4])
end
redis.call('EXPIRE', KEYS[2], ARGV[5])
local acknowledged = redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[2], ARGV[2])
if redis.call('ZREM', KEYS[5], ARGV[2]) > 0 then
  redis.call('PUBLISH', KEYS[6], '')
end
if ARGV[6] ~= '' and redis.call('GET', KEYS[4]) == ARGV[6] then
  redis.call('DEL', KEYS[4])
end
redis.call('PUBLISH', KEYS[1], '')
return acknowledged
)lua";

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

    // Configuration and media health are immutable for the process lifetime.
    // The Collector holding the SDK handle passes its ports and capabilities;
    // Service Workers and the media proxy read this projection instead of a
    // process-global runtime.
    template <typename Redis>
    static ruvia::task<void> publishConfig(const Redis& redis, const Config& config, const media::MediaStatus& media) {
        ConfigProjectionRecord record{ .enabled = config.enabled, .mediaStarted = media.started };
        record.mediaPorts = ruvia::to_json(control_protocol::mediaPortsJson(nullptr, media.ports));
        record.mediaCapabilities = ruvia::to_json(control_protocol::mediaCapabilitiesJson(nullptr, media.capabilities));
        co_await service::utils::redis::setHash(redis, service::message::gb28181::kStateKey, record.encode());
        co_return;
    }

    static ruvia::task<bool> configuredEnabled(const ruvia::redis_handle& redis) {
        const auto value = co_await redis.hget(service::message::gb28181::kStateKey, service::message::gb28181::kStateEnabledField);
        co_return value&& ConfigProjectionRecord::flag(std::string_view(*value));
    }

    static ruvia::task<ConfigProjectionRecord> loadState(const ruvia::redis_handle& redis) {
        co_return ConfigProjectionRecord::decode(
            co_await service::utils::redis::hashEntries(redis, service::message::gb28181::kStateKey)
        );
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

    // Owner leases are tracked in a deadline set scored by Redis time.  Every
    // Service Worker wakes at the earliest deadline and claims due members
    // atomically; only the claimer reconciles an aggregate whose owner expired.
    // Returns the delay until the next deadline.
    static ruvia::task<std::optional<std::chrono::milliseconds>>
    expireDueOwners(ruvia::web_worker_context& context) {
        const auto claim = co_await claimDueOwners(context.redis());
        for (const auto& owner : claim.owners) {
            const auto member = control_protocol::stream::parseOwnerDeadlineMember(owner.member);
            try {
                if (member && member->kind == control_protocol::stream::kDeviceDeadlineKind) {
                    co_await releaseUnownedDevice(context, std::string(member->aggregateId));
                } else if (member) {
                    if (const auto stream = projection_protocol::streamFromIdentity(member->aggregateId)) {
                        co_await releaseUnownedStream(context, *stream);
                    }
                }
                co_await completeOwnerExpiry(context.redis(), owner);
            } catch (const std::exception& error) {
                // The claim keeps the member until its processing deadline, so
                // the next claimer retries this aggregate.
                LOG_WARN << "[GB28181][GbProjectionService] owner expiry failed, member="
                         << owner.member << ": " << error.what();
            }
        }
        if (!claim.next) {
            co_return std::nullopt;
        }
        co_return std::chrono::milliseconds(std::max<std::int64_t>(0, *claim.next - claim.now));
    }

    // Owner deadlines exist only for aggregates claimed through the deadline
    // set.  Once per process incarnation, one Service Worker enrolls every
    // durable online aggregate as immediately due so state left by a stopped
    // process or a lost Redis dataset is reconciled through the same claim.
    static ruvia::task<void> seedOwnerDeadlines(ruvia::web_worker_context& context) {
        ruvia::redis_set_options options;
        options.condition_ = ruvia::redis_set_condition::if_absent;
        options.expiration_ = ruvia::redis_set_expiration::expires_after(std::chrono::hours(24));
        const auto seed = co_await context.redis().set(
            control_protocol::stream::ownerDeadlineSeed(context.worker_state<std::optional<common::WorkerIdentity>>().value().instanceId),
            "1",
            std::move(options)
        );
        if (!seed.applied()) {
            co_return;
        }
        std::vector<std::string> arguments{ "ZADD", std::string(control_protocol::stream::kOwnerDeadlines), "NX" };
        ruvia::db_query deviceQuery(context.pool());
        auto deviceQueryBuilder = context.db().template repository<service::gb28181::persistence::Gb28181DeviceEntity>().query_builder();
        deviceQueryBuilder.select({ { "id", deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"id">()) } }).where(deviceQuery.binary(deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"online">()), ruvia::db_binary_operator::equal, deviceQuery.value(true)));
        for (const auto& row : co_await deviceQueryBuilder.get_raw_many()) {
            const auto id = row[0].value().value_or(std::string_view{});
            if (!id.empty()) {
                arguments.emplace_back("0");
                arguments.push_back(control_protocol::stream::ownerDeadlineMember(control_protocol::stream::kDeviceDeadlineKind, id));
            }
        }
        ruvia::db_query streamQuery(context.pool());
        auto streamQueryBuilder = context.db().template repository<service::gb28181::persistence::Gb28181StreamEntity>().query_builder();
        streamQueryBuilder.select({ { "app", streamQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"app">()) }, { "stream", streamQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"stream">()) }, { "schema", streamQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"schema">()) } }).where(streamQuery.binary(streamQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"online">()), ruvia::db_binary_operator::equal, streamQuery.value(true)));
        for (const auto& row : co_await streamQueryBuilder.get_raw_many()) {
            const auto app = std::string(row[0].value().value_or(std::string_view{}));
            const auto stream = std::string(row[1].value().value_or(std::string_view{}));
            const auto schema = std::string(row[2].value().value_or(std::string_view{}));
            if (!app.empty() && !stream.empty() && !schema.empty()) {
                arguments.emplace_back("0");
                arguments.push_back(control_protocol::stream::ownerDeadlineMember(control_protocol::stream::kStreamDeadlineKind, media::StreamStatus::identity(app, stream, schema)));
            }
        }
        if (arguments.size() == 3) {
            co_return;
        }
        const auto reply = co_await context.redis().command(std::vector<std::string_view>(arguments.begin(), arguments.end()));
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("seed GB28181 owner deadlines", reply);
        }
    }

  private:
    static constexpr std::size_t kOwnerExpiryBatch = 64;
    static constexpr std::string_view kOwnerExpiryProcessingMs{ "30000" };

    static ruvia::task<DueOwnerClaim> claimDueOwners(const ruvia::redis_handle& redis) {
        const auto limit = std::to_string(kOwnerExpiryBatch);
        const std::string_view keys[]{ control_protocol::stream::kOwnerDeadlines };
        const std::string_view arguments[]{ control_protocol::stream::kOwnerPrefix, limit, kOwnerExpiryProcessingMs };
        const auto reply = co_await redis.eval(kClaimDueOwnersScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::array || reply.array().size() < 2 || reply.array().size() % 2 != 0) {
            service::utils::redis::throwValue("claim GB28181 owner deadlines", reply);
        }
        const auto values = reply.array();
        for (const auto& value : values) {
            if (value.kind() != ruvia::redis_value::kind_type::string) {
                service::utils::redis::throwValue("claim GB28181 owner deadlines", reply);
            }
        }
        const auto milliseconds = [](std::string_view text) -> std::optional<std::int64_t> {
            std::int64_t parsed{};
            const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), parsed);
            if (error != std::errc{} || end != text.data() + text.size()) {
                return std::nullopt;
            }
            return parsed;
        };
        DueOwnerClaim claim;
        const auto now = milliseconds(values[0].string());
        if (!now) {
            throw std::runtime_error("invalid GB28181 owner deadline clock");
        }
        claim.now = *now;
        if (!values[1].string().empty()) {
            claim.next = milliseconds(values[1].string());
            if (!claim.next) {
                throw std::runtime_error("invalid GB28181 owner deadline score");
            }
        }
        for (std::size_t index = 2; index + 1 < values.size(); index += 2) {
            claim.owners.push_back({ std::string(values[index].string()), std::string(values[index + 1].string()) });
        }
        co_return claim;
    }

    static ruvia::task<void> completeOwnerExpiry(const ruvia::redis_handle& redis, const DueOwner& owner) {
        const std::string_view keys[]{ control_protocol::stream::kOwnerDeadlines };
        const std::string_view arguments[]{ owner.member, owner.score };
        const auto reply = co_await redis.eval(kCompleteOwnerExpiryScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("complete GB28181 owner expiry", reply);
        }
    }

    // The advisory lock serializes this check with projections of the same
    // device; the owner key is read again under the lock so a newly acquired
    // owner is never cleared.
    static ruvia::task<void> releaseUnownedDevice(ruvia::web_worker_context& context, std::string id) {
        const auto ownerKey = control_protocol::stream::owner(id);
        {
            auto transaction = co_await context.db().begin_transaction();
            co_await transaction.advisory_lock(id, service::message::lock::kGb28181OwnerSeed);
            ruvia::db_query currentQuery(context.pool());
            auto currentQueryBuilder = transaction.template repository<service::gb28181::persistence::Gb28181DeviceEntity>().query_builder();
            currentQueryBuilder.select({ { "online", currentQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"online">()) } }).where(currentQuery.binary(currentQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, currentQuery.value(id))).set_lock({ .mode_ = ruvia::db_row_lock::update });
            const auto current = co_await currentQueryBuilder.get_raw_many();
            if (current.empty() ||
                !boolean(current.front()[0].value().value_or(std::string_view{}))) {
                co_await transaction.commit();
                co_return;
            }
            if (co_await context.redis().get(ownerKey)) {
                co_await transaction.commit();
                co_return;
            }
            ruvia::db_query updateQuery(context.pool());
            auto updateQueryBuilder = transaction.template repository<service::gb28181::persistence::Gb28181DeviceEntity>().update_builder();
            updateQueryBuilder.set(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"online">(), updateQuery.value(false)).set(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"updated_at">(), updateQuery.call("now")).where(updateQuery.binary(updateQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, updateQuery.value(id))).and_where(updateQuery.binary(updateQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"online">()), ruvia::db_binary_operator::equal, updateQuery.value(true)));
            (void)co_await updateQueryBuilder.execute();
            co_await transaction.commit();
        }
    }

    static ruvia::task<void> releaseUnownedStream(ruvia::web_worker_context& context, const media::StreamStatus& target) {
        const auto& app = target.app;
        const auto& stream = target.stream;
        const auto& schema = target.schema;
        const auto identity = media::StreamStatus::identity(app, stream, schema);
        const auto ownerKey = control_protocol::stream::owner(identity);
        {
            auto transaction = co_await context.db().begin_transaction();
            co_await transaction.advisory_lock(identity, service::message::lock::kGb28181OwnerSeed);
            ruvia::db_query currentQuery(context.pool());
            auto currentQueryBuilder = transaction.template repository<service::gb28181::persistence::Gb28181StreamEntity>().query_builder();
            currentQueryBuilder.select({ { "online", currentQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"online">()) } }).where(currentQuery.binary(currentQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"app">()), ruvia::db_binary_operator::equal, currentQuery.value(app))).and_where(currentQuery.binary(currentQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"stream">()), ruvia::db_binary_operator::equal, currentQuery.value(stream))).and_where(currentQuery.binary(currentQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"schema">()), ruvia::db_binary_operator::equal, currentQuery.value(schema))).set_lock({ .mode_ = ruvia::db_row_lock::update });
            const auto current = co_await currentQueryBuilder.get_raw_many();
            if (current.empty() ||
                !boolean(current.front()[0].value().value_or(std::string_view{}))) {
                co_await transaction.commit();
                co_return;
            }
            if (co_await context.redis().get(ownerKey)) {
                co_await transaction.commit();
                co_return;
            }
            ruvia::db_query updateQuery(context.pool());
            auto updateQueryBuilder = transaction.template repository<service::gb28181::persistence::Gb28181StreamEntity>().update_builder();
            updateQueryBuilder.set(service::gb28181::persistence::Gb28181StreamEntity::column_name<"online">(), updateQuery.value(false)).set(service::gb28181::persistence::Gb28181StreamEntity::column_name<"reader_count">(), updateQuery.value(0)).set(service::gb28181::persistence::Gb28181StreamEntity::column_name<"updated_at">(), updateQuery.call("now")).where(updateQuery.binary(updateQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"app">()), ruvia::db_binary_operator::equal, updateQuery.value(app))).and_where(updateQuery.binary(updateQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"stream">()), ruvia::db_binary_operator::equal, updateQuery.value(stream))).and_where(updateQuery.binary(updateQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"schema">()), ruvia::db_binary_operator::equal, updateQuery.value(schema))).and_where(updateQuery.binary(updateQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"online">()), ruvia::db_binary_operator::equal, updateQuery.value(true)));
            (void)co_await updateQueryBuilder.execute();
            co_await transaction.commit();
        }
    }

  protected:
    static ruvia::db_expression projectionCursor(
        ruvia::db_query& query,
        std::string_view streamMessageId
    ) {
        const auto messageId = query.value(streamMessageId);
        const auto separator = query.value(std::string_view{ "-" });
        const auto radix = query.cast(
            query.value(std::string_view{ "18446744073709551616" }),
            ruvia::db_data_type::numeric
        );
        const auto milliseconds = query.cast(
            query.call("split_part", { messageId, separator, query.cast(query.value(std::int64_t{ 1 }), ruvia::db_data_type::integer) }),
            ruvia::db_data_type::numeric
        );
        const auto sequence = query.cast(
            query.call("split_part", { messageId, separator, query.cast(query.value(std::int64_t{ 2 }), ruvia::db_data_type::integer) }),
            ruvia::db_data_type::numeric
        );
        return query.binary(
            query.binary(milliseconds, ruvia::db_binary_operator::multiply, radix),
            ruvia::db_binary_operator::add,
            sequence
        );
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
        auto deviceQueryBuilder = context.db().template repository<service::gb28181::persistence::Gb28181DeviceEntity>().query_builder();
        deviceQueryBuilder.select({ { "id", deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"id">()) }, { "name", deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"name">()) }, { "custom_name", deviceQuery.coalesce({ deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"custom_name">()), deviceQuery.value(std::string_view{}) }) }, { "manufacturer", deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"manufacturer">()) }, { "remote_address", deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"remote_address">()) }, { "registration_source", deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"registration_source">()) }, { "online", deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"online">()) }, { "last_seen_at", deviceQuery.call("iot_utc_timestamp", { deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"last_seen_at">()) }) } }).order_by(deviceQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"id">()));
        const auto devices = co_await deviceQueryBuilder.get_raw_many();
        snapshot.devices.reserve(devices.size());
        for (const auto& row : devices) {
            device::Device device;
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
        auto channelQueryBuilder = context.db().template repository<service::gb28181::persistence::Gb28181ChannelEntity>().query_builder();
        channelQueryBuilder.select({ { "device_id", channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"device_id">()) }, { "id", channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"id">()) }, { "name", channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"name">()) }, { "custom_name", channelQuery.coalesce({ channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"custom_name">()), channelQuery.value(std::string_view{}) }) }, { "manufacturer", channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"manufacturer">()) }, { "online", channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"online">()) }, { "ptz_type", channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"ptz_type">()) } }).order_by(channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"device_id">())).add_order_by(channelQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"id">()));
        const auto channels = co_await channelQueryBuilder.get_raw_many();
        for (const auto& row : channels) {
            const auto device = deviceIndexes.find(
                std::string(row[0].value().value_or(std::string_view{}))
            );
            if (device == deviceIndexes.end()) {
                continue;
            }
            snapshot.devices[device->second].channels.push_back(device::Channel{
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
    static ruvia::task<void> syncChannels(Transaction& transaction, const device::Device& device, bool updateCustomNames = true) {
        if (device.channels.empty()) {
            ruvia::db_query removal;
            auto removalBuilder = transaction.template repository<service::gb28181::persistence::Gb28181ChannelEntity>().delete_builder();
            removalBuilder.where(removal.binary(
                removal.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"device_id">()),
                ruvia::db_binary_operator::equal,
                removal.value(device.id)
            ));
            (void)co_await removalBuilder.execute();
            co_return;
        }

        // CTE 参数使用 TEXT，目标列仍按实体和数据库约束验证长度，避免 VARCHAR(n) 转换静默截断。
        ruvia::db_query rawIncoming;
        std::size_t ordinal = 0;
        for (const auto& channel : device.channels) {
            rawIncoming.values({
                rawIncoming.cast(rawIncoming.value(static_cast<std::int64_t>(ordinal)), ruvia::db_data_type::big_int),
                rawIncoming.cast(rawIncoming.value(channel.id), ruvia::db_data_type::text),
                rawIncoming.cast(rawIncoming.value(channel.name), ruvia::db_data_type::text),
                rawIncoming.cast(rawIncoming.value(channel.customName), ruvia::db_data_type::text),
                rawIncoming.cast(rawIncoming.value(channel.manufacturer), ruvia::db_data_type::text),
                rawIncoming.cast(rawIncoming.value(channel.online), ruvia::db_data_type::boolean),
                rawIncoming.cast(rawIncoming.value(channel.ptzType), ruvia::db_data_type::integer),
            });
            ++ordinal;
        }

        ruvia::db_query incoming;
        incoming
            .select({ incoming.column("id"), incoming.column("name"), incoming.column("custom_name"), incoming.column("manufacturer"), incoming.column("online"), incoming.column("ptz_type") })
            .distinct_on({ incoming.column("id") })
            .from("raw_incoming")
            .order_by(incoming.column("id"))
            .add_order_by(incoming.column("ordinal"), ruvia::db_order_direction::desc);

        ruvia::db_query insertSource;
        insertSource
            .select({
                insertSource.value(device.id),
                insertSource.column("id", "incoming"),
                insertSource.column("name", "incoming"),
                insertSource.null_if(insertSource.column("custom_name", "incoming"), insertSource.value(std::string_view{})),
                insertSource.column("manufacturer", "incoming"),
                insertSource.column("online", "incoming"),
                insertSource.column("ptz_type", "incoming"),
                insertSource.call("now"),
            })
            .from("incoming", "incoming");

        ruvia::db_query upsert;
        upsert
            .insert_into(service::gb28181::persistence::Gb28181ChannelEntity::table_name(), { "device_id", "id", "name", "custom_name", "manufacturer", "online", "ptz_type", "updated_at" })
            .insert_from(insertSource);
        ruvia::db_conflict_options conflict;

        const std::array<ruvia::borrowed_text, 2> sourceColumns1{ { "device_id", "id" } };
        conflict.columns_ = sourceColumns1;

        const std::array<ruvia::db_assignment, 6> conflictUpdates2{ { { "name", upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"name">()) }, { "custom_name", updateCustomNames ? upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"custom_name">()) : upsert.coalesce({ upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"custom_name">()), upsert.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"custom_name">(), "gb28181_channel") }) }, { "manufacturer", upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"manufacturer">()) }, { "online", upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"online">()) }, { "ptz_type", upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"ptz_type">()) }, { "updated_at", upsert.call("now") } } };
        conflict.update_ = conflictUpdates2;
        conflict.update_where_ = upsert.binary(
            upsert.tuple({
                upsert.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"name">(), "gb28181_channel"),
                upsert.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"custom_name">(), "gb28181_channel"),
                upsert.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"manufacturer">(), "gb28181_channel"),
                upsert.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"online">(), "gb28181_channel"),
                upsert.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"ptz_type">(), "gb28181_channel"),
            }),
            ruvia::db_binary_operator::is_distinct_from,
            upsert.tuple({ upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"name">()), upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"custom_name">()), upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"manufacturer">()), upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"online">()), upsert.excluded(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"ptz_type">()) })
        );
        upsert.on_conflict(conflict)
            .returning({ upsert.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"id">()) });

        ruvia::db_query incomingIds;
        incomingIds
            .select(incomingIds.value(1))
            .from("incoming", "incoming")
            .where(incomingIds.binary(
                incomingIds.column("id", "incoming"),
                ruvia::db_binary_operator::equal,
                incomingIds.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"id">(), "stored")
            ));

        ruvia::db_query removal;
        auto removalBuilder = transaction.template repository<service::gb28181::persistence::Gb28181ChannelEntity>().delete_builder("stored");

        const std::array<ruvia::borrowed_text, 7> sourceColumns3{ { "ordinal", "id", "name", "custom_name", "manufacturer", "online", "ptz_type" } };
        removalBuilder.with("raw_incoming", rawIncoming, { .columns_ = sourceColumns3 }).with("incoming", incoming).with("upserted", upsert).where(removal.binary(removal.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"device_id">(), "stored"), ruvia::db_binary_operator::equal, removal.value(device.id))).and_where(removal.unary(ruvia::db_unary_operator::not_value, removal.exists(incomingIds)));
        (void)co_await removalBuilder.execute();
    }

    // Projection events carry a complete Collector-side snapshot, but a new
    // Collector registry does not hydrate Service metadata before REGISTER.
    // Apply only the fields owned by the event's change kinds so an empty
    // in-memory custom name or channel list cannot erase durable state from an
    // earlier session.  A coalesced event applies the union of its kinds.
    static ruvia::task<bool> applyDeviceProjection(
        ruvia::web_worker_context& context,
        const device::Device& device,
        device::DeviceChangeSet changes,
        std::string ownerKey,
        std::string ownerToken,
        std::string streamMessageId
    ) {
        auto transaction = co_await context.db().begin_transaction();
        co_await transaction.advisory_lock(device.id, service::message::lock::kGb28181OwnerSeed);
        ruvia::db_query cursorQuery(context.pool());
        auto cursorQueryBuilder = transaction.template repository<service::gb28181::persistence::Gb28181DeviceEntity>().query_builder();
        cursorQueryBuilder.select({ { "projection_cursor", cursorQuery.binary(cursorQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"projection_cursor">()), ruvia::db_binary_operator::greater_equal, projectionCursor(cursorQuery, streamMessageId)) } }).where(cursorQuery.binary(cursorQuery.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, cursorQuery.value(device.id))).set_lock({ .mode_ = ruvia::db_row_lock::update });
        const auto cursor = co_await cursorQueryBuilder.get_raw_many();
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

        if (changes.contains(device::DeviceChange::Status) || changes.contains(device::DeviceChange::Catalog) ||
            changes.contains(device::DeviceChange::DeviceName)) {
            ruvia::db_query query(context.pool());
            using Entity = persistence::Gb28181DeviceEntity;
            const std::array<std::string_view, 9> columns{ Entity::column_name<"id">(), Entity::column_name<"name">(), Entity::column_name<"custom_name">(), Entity::column_name<"manufacturer">(), Entity::column_name<"remote_address">(), Entity::column_name<"registration_source">(), Entity::column_name<"online">(), Entity::column_name<"last_seen_at">(), Entity::column_name<"updated_at">() };
            ruvia::db_values_builder values(columns);
            values.row({
                query.value(device.id),
                query.value(device.name),
                query.null_if(query.value(device.customName), query.value(std::string_view{})),
                query.value(device.manufacturer),
                query.value(device.remoteAddress),
                query.value(device.registrationSource),
                query.value(device.online),
                query.cast(query.value(lastSeen), ruvia::db_data_type::timestamp_tz),
                query.call("now"),
            });

            ruvia::db_upsert_options conflict;
            std::vector<ruvia::db_assignment> updates;

            const std::array<ruvia::borrowed_text, 1> sourceColumns4{ Entity::column_name<"id">() };
            conflict.conflict_paths_ = sourceColumns4;
            const auto excluded = [&query](auto column) {
                return ruvia::db_assignment{ column, query.excluded(column) };
            };
            if (changes.contains(device::DeviceChange::Catalog)) {
                updates.push_back(excluded(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"name">()));
                updates.push_back(excluded(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"manufacturer">()));
            }
            if (changes.contains(device::DeviceChange::Status) || changes.contains(device::DeviceChange::Catalog)) {
                updates.push_back(excluded(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"remote_address">()));
                updates.push_back(excluded(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"registration_source">()));
                updates.push_back(excluded(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"online">()));
                updates.push_back(excluded(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"last_seen_at">()));
            }
            if (changes.contains(device::DeviceChange::DeviceName)) {
                updates.push_back(excluded(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"custom_name">()));
            }
            updates.push_back({ "updated_at", query.call("now") });
            std::vector<ruvia::borrowed_text> updateColumns;
            for (const auto& update : updates) {
                updateColumns.emplace_back(update.column_);
            }
            conflict.update_columns_ = updateColumns;
            conflict.update_expressions_ = updates;
            auto builder = transaction.template repository<Entity>().insert_builder();
            builder.insert_from(columns, values.query()).on_conflict(conflict);
            (void)co_await builder.execute();
        }
        if (changes.contains(device::DeviceChange::Catalog)) {
            co_await syncChannels(transaction, device, false);
        }
        if (changes.contains(device::DeviceChange::ChannelName)) {
            for (const auto& channel : device.channels) {
                if (channel.customName.empty()) {
                    continue;
                }
                ruvia::db_query updateQuery(context.pool());
                auto updateQueryBuilder = transaction.template repository<service::gb28181::persistence::Gb28181ChannelEntity>().update_builder();
                updateQueryBuilder.set(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"custom_name">(), updateQuery.value(channel.customName)).set(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"updated_at">(), updateQuery.call("now")).where(updateQuery.binary(updateQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"device_id">()), ruvia::db_binary_operator::equal, updateQuery.value(device.id))).and_where(updateQuery.binary(updateQuery.column(service::gb28181::persistence::Gb28181ChannelEntity::column_name<"id">()), ruvia::db_binary_operator::equal, updateQuery.value(channel.id)));
                (void)co_await updateQueryBuilder.execute();
            }
        }
        ruvia::db_query cursorUpdate(context.pool());
        auto cursorUpdateBuilder = transaction.template repository<service::gb28181::persistence::Gb28181DeviceEntity>().update_builder();
        cursorUpdateBuilder.set(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"projection_cursor">(), projectionCursor(cursorUpdate, streamMessageId)).where(cursorUpdate.binary(cursorUpdate.column(service::gb28181::persistence::Gb28181DeviceEntity::column_name<"id">()), ruvia::db_binary_operator::equal, cursorUpdate.value(device.id)));
        (void)co_await cursorUpdateBuilder.execute();
        co_await transaction.commit();
        co_return true;
    }

    static ruvia::task<bool> applyStreamProjection(
        ruvia::web_worker_context& context,
        const media::StreamStatus& stream,
        std::string ownerKey,
        std::string ownerToken,
        std::string streamMessageId
    ) {
        const auto identity =
            media::StreamStatus::identity(stream.app, stream.stream, stream.schema);
        auto transaction = co_await context.db().begin_transaction();
        co_await transaction.advisory_lock(identity, service::message::lock::kGb28181OwnerSeed);
        ruvia::db_query cursorQuery(context.pool());
        auto cursorQueryBuilder = transaction.template repository<service::gb28181::persistence::Gb28181StreamEntity>().query_builder();
        cursorQueryBuilder.select({ { "projection_cursor", cursorQuery.binary(cursorQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"projection_cursor">()), ruvia::db_binary_operator::greater_equal, projectionCursor(cursorQuery, streamMessageId)) } }).where(cursorQuery.binary(cursorQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"app">()), ruvia::db_binary_operator::equal, cursorQuery.value(stream.app))).and_where(cursorQuery.binary(cursorQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"stream">()), ruvia::db_binary_operator::equal, cursorQuery.value(stream.stream))).and_where(cursorQuery.binary(cursorQuery.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"schema">()), ruvia::db_binary_operator::equal, cursorQuery.value(stream.schema))).set_lock({ .mode_ = ruvia::db_row_lock::update });
        const auto cursor = co_await cursorQueryBuilder.get_raw_many();
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
        const std::array<std::string_view, 6> upsertColumns{ "app", "stream", "schema", "online", "reader_count", "updated_at" };
        auto upsertBuilder = transaction.template repository<service::gb28181::persistence::Gb28181StreamEntity>().insert_builder();
        ruvia::db_values_builder upsertValues(upsertColumns);
        upsertValues.row({ upsert.value(stream.app), upsert.value(stream.stream), upsert.value(stream.schema), upsert.value(stream.online), upsert.value(stream.readerCount), upsert.call("now") });
        ruvia::db_upsert_options conflict;

        const std::array<ruvia::borrowed_text, 3> conflictColumns5{ { "app", "stream", "schema" } };
        conflict.conflict_paths_ = conflictColumns5;

        const std::array<ruvia::db_assignment, 3> conflictUpdates6{ { { "online", upsert.excluded(service::gb28181::persistence::Gb28181StreamEntity::column_name<"online">()) }, { "reader_count", upsert.excluded(service::gb28181::persistence::Gb28181StreamEntity::column_name<"reader_count">()) }, { "updated_at", upsert.call("now") } } };
        conflict.update_expressions_ = conflictUpdates6;

        const std::array<ruvia::borrowed_text, 3> updatedColumns7{ { "online", "reader_count", "updated_at" } };
        conflict.update_columns_ = updatedColumns7;
        conflict.update_where_ = upsert.binary(
            upsert.tuple({ upsert.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"online">(), "gb28181_stream"), upsert.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"reader_count">(), "gb28181_stream") }),
            ruvia::db_binary_operator::is_distinct_from,
            upsert.tuple({ upsert.excluded(service::gb28181::persistence::Gb28181StreamEntity::column_name<"online">()), upsert.excluded(service::gb28181::persistence::Gb28181StreamEntity::column_name<"reader_count">()) })
        );
        upsertBuilder.insert_from(upsertColumns, upsertValues.query()).on_conflict(conflict);
        (void)co_await upsertBuilder.execute();
        ruvia::db_query cursorUpdate(context.pool());
        auto cursorUpdateBuilder = transaction.template repository<service::gb28181::persistence::Gb28181StreamEntity>().update_builder();
        cursorUpdateBuilder.set(service::gb28181::persistence::Gb28181StreamEntity::column_name<"projection_cursor">(), projectionCursor(cursorUpdate, streamMessageId)).where(cursorUpdate.binary(cursorUpdate.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"app">()), ruvia::db_binary_operator::equal, cursorUpdate.value(stream.app))).and_where(cursorUpdate.binary(cursorUpdate.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"stream">()), ruvia::db_binary_operator::equal, cursorUpdate.value(stream.stream))).and_where(cursorUpdate.binary(cursorUpdate.column(service::gb28181::persistence::Gb28181StreamEntity::column_name<"schema">()), ruvia::db_binary_operator::equal, cursorUpdate.value(stream.schema)));
        (void)co_await cursorUpdateBuilder.execute();
        co_await transaction.commit();
        co_return true;
    }
};

class GbControlService final {
  public:
    static void requireEnabled(bool enabled) {
        if (!enabled) {
            service::common::fail(10004, "GB28181 功能未启用", 404);
        }
    }

    static ruvia::task<void> publishControlCommand(
        const ruvia::redis_handle& redis,
        const service::message::RedisChannelScope& scope,
        const std::string& controlKey,
        const std::string& ownerKey,
        std::string_view ownerToken,
        std::span<const service::message::StreamField> fields
    ) {
        std::vector<std::string_view> args{ ownerToken };
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const auto inputChannel = service::message::consumer::inputChannel(scope, controlKey);
        const std::string_view keys[]{ controlKey, ownerKey, inputChannel };
        const auto published = co_await redis.eval(kPublishControlCommandScript, keys, args);
        if (published.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("GB28181 command publish", published);
        }
        if (published.integer() != 1) {
            service::common::fail(10003, "GB28181 connection owner expired", 409);
        }
    }

    static ruvia::task<ControlClaimResult> claimControlExecution(
        const ruvia::redis_handle& redis,
        const std::string& ownerKey,
        const std::string& claimKey,
        const std::string& cancelKey,
        const std::string& ownerToken,
        std::string_view deadline
    ) {
        const std::string_view keys[]{ ownerKey, claimKey, cancelKey };
        const std::string_view args[]{ ownerToken, deadline };
        const auto claimed = co_await redis.eval(kClaimControlExecutionScript, keys, args);
        if (claimed.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("GB28181 command claim", claimed);
        }
        co_return static_cast<ControlClaimResult>(claimed.integer());
    }

    static ruvia::task<bool> controlCancelled(
        const ruvia::redis_handle& redis,
        const std::string& cancelKey
    ) {
        const auto cancelled = co_await redis.get(cancelKey);
        co_return static_cast<bool>(cancelled);
    }

    // Creates the consumer group of a Collector incarnation stream and
    // refreshes its lifetime.
    static ruvia::task<void> ensureCollectorStream(const ruvia::redis_handle& redis, std::string_view stream, std::string_view group) {
        const std::string_view keys[]{ stream };
        const std::string_view args[]{ group, kCollectorStreamTtlSeconds };
        const auto created = co_await redis.eval(kCreateCollectorStreamScript, keys, args);
        if (created.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("GB28181 Collector stream group", created);
        }
    }

    static ruvia::task<void> refreshCollectorStream(const ruvia::redis_handle& redis, std::string_view stream) {
        const std::vector<std::string> arguments{ "EXPIRE", std::string(stream), std::string(kCollectorStreamTtlSeconds) };
        const auto refreshed = co_await redis.command(std::vector<std::string_view>(arguments.begin(), arguments.end()));
        if (refreshed.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("GB28181 Collector stream lifetime", refreshed);
        }
    }

    static ruvia::task<ProjectionPublishResult> publishProjection(
        const ruvia::redis_handle& redis,
        const service::message::RedisChannelScope& scope,
        const std::string& projectionId,
        std::string_view projectionOrder,
        std::string_view currentEntryId,
        std::span<const service::message::StreamField> fields,
        std::string_view instanceId
    ) {
        const auto sentKey = control_protocol::stream::projectionSent(projectionId);
        const auto wakeStream = service::message::sharedWakeStream(instanceId);
        const auto wakeCapacity = std::to_string(service::message::kWorkerWakeCapacity);
        const auto inputChannel = service::message::consumer::inputChannel(scope, control_protocol::stream::kProjection);
        const auto wakeChannel = service::message::consumer::inputChannel(scope, wakeStream);
        const std::string_view keys[]{ control_protocol::stream::kProjection, sentKey, wakeStream, inputChannel, wakeChannel };
        std::vector<std::string_view> args;
        args.reserve(4 + fields.size() * 2);
        args.push_back(projectionOrder);
        args.push_back(currentEntryId);
        args.push_back(service::message::workerStreamTaskName(service::message::WorkerStreamTask::Gb28181Projector));
        args.push_back(wakeCapacity);
        for (const auto& field : fields) {
            args.push_back(field.name);
            args.push_back(field.value);
        }
        const auto published = co_await redis.eval(kPublishProjectionScript, keys, args);
        co_return parseProjectionPublishResult(published);
    }

    static ruvia::task<std::optional<bool>> projectionReceipt(
        const ruvia::redis_handle& redis,
        std::string_view projectionId
    ) {
        const auto done = co_await redis.get(control_protocol::stream::projectionDone(projectionId));
        if (!done) {
            co_return std::nullopt;
        }
        if (*done != "0" && *done != "1") {
            throw std::runtime_error("invalid GB28181 projection receipt");
        }
        co_return std::string_view(*done) == "1";
    }

    static ProjectionPublishResult parseProjectionPublishResult(const ruvia::redis_value& reply) {
        if (reply.kind() != ruvia::redis_value::kind_type::array ||
            reply.array().size() != 2 ||
            reply.array()[0].kind() != ruvia::redis_value::kind_type::string ||
            reply.array()[1].kind() != ruvia::redis_value::kind_type::string) {
            service::utils::redis::throwValue("GB28181 projection publish result", reply);
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
        const ruvia::redis_handle& redis,
        std::string_view key
    ) {
        const auto value = co_await redis.get(key);
        if (!value) {
            co_return std::nullopt;
        }
        co_return ControlResultRecord::decode(*value);
    }

    // `deadlineMember` is empty for preview session owners, which have no
    // durable aggregate to reconcile.
    static ruvia::task<bool> claimOwnerLease(const ruvia::redis_handle& connection, const service::message::RedisChannelScope& scope, std::string_view key, std::string_view token, std::string_view previousToken, std::string_view deadlineMember, std::string_view instanceId) {
        const auto redis = connection.with_options({ .timeout_ = std::chrono::seconds(3) });
        const auto wakeStream = service::message::sharedWakeStream(instanceId);
        const auto wakeCapacity = std::to_string(service::message::kWorkerWakeCapacity);
        const auto wakeChannel = service::message::consumer::inputChannel(scope, wakeStream);
        const std::string_view keys[]{ key, control_protocol::stream::kOwnerDeadlines, wakeStream, wakeChannel };
        const std::string_view arguments[]{
            token,
            previousToken,
            kOwnerLeaseMs,
            deadlineMember,
            service::message::workerStreamTaskName(service::message::WorkerStreamTask::Gb28181Projector),
            wakeCapacity,
        };
        const auto reply = co_await redis.eval(kClaimOwnerLeaseScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("claim GB28181 owner lease", reply);
        }
        co_return reply.integer() == 1;
    }

    static ruvia::task<bool> renewOwnerLease(const ruvia::redis_handle& connection, std::string_view key, std::string_view token, std::string_view deadlineMember) {
        const auto redis = connection.with_options({ .timeout_ = std::chrono::seconds(3) });
        const std::string_view keys[]{ key, control_protocol::stream::kOwnerDeadlines };
        const std::string_view arguments[]{ token, kOwnerLeaseMs, deadlineMember };
        const auto reply = co_await redis.eval(kRenewOwnerLeaseScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("renew GB28181 owner lease", reply);
        }
        co_return reply.integer() == 1;
    }

    // `settledMember` removes the aggregate deadline; pass it only after the
    // owner's offline state is durable.
    static ruvia::task<void> releaseOwnerLease(const ruvia::redis_handle& connection, std::string_view key, std::string_view token, std::string_view settledMember) {
        const auto redis = connection.with_options({ .timeout_ = std::chrono::seconds(3) });
        const std::string_view keys[]{ key, control_protocol::stream::kOwnerDeadlines };
        const std::string_view arguments[]{ token, settledMember };
        const auto reply = co_await redis.eval(kReleaseOwnerLeaseScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("release GB28181 owner lease", reply);
        }
        co_return;
    }

    static ruvia::task<void> publishReplyAndAcknowledge(
        const ruvia::redis_handle& redis,
        const service::message::RedisChannelScope& scope,
        std::string_view replyTopic,
        std::string_view controlStream,
        std::string_view group,
        std::string_view messageId,
        std::string_view status,
        std::string_view payload,
        std::string_view resultKey,
        std::string_view claimKey,
        std::string_view claimOwner
    ) {
        const std::string result = ControlResultRecord{ std::string(status), std::string(payload) }.encode();
        const std::string_view ttl{ "600" };
        const auto deadlineKey = service::message::consumer::deadlineKey(controlStream, group);
        const auto deadlineChannel = service::message::consumer::deadlineChannel(scope, controlStream, group);
        const std::string_view keys[]{ replyTopic, controlStream, resultKey, claimKey, deadlineKey, deadlineChannel };
        const std::string_view arguments[]{ group, messageId, result, ttl, ttl, claimOwner };
        const auto reply = co_await redis.eval(kPublishReplyAndAcknowledgeScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("GB28181 reply/ack", reply);
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

    static ruvia::task<std::optional<std::string>> ownerTokenFor(const ruvia::redis_handle& redis, std::string_view key) {
        const auto value = co_await redis.get(key);
        if (!value) {
            co_return std::nullopt;
        }
        co_return std::optional<std::string>(std::string(*value));
    }

    // `receiptTopic` is the publishing Collector Worker's projection receipt topic.
    static ruvia::task<void> markProjectionDoneAndAcknowledge(
        const ruvia::redis_handle& redis,
        const service::message::RedisChannelScope& scope,
        std::string_view stream,
        std::string_view group,
        std::string_view messageId,
        std::string_view projectionId,
        std::string_view receiptTopic,
        bool applied
    ) {
        if (projectionId.empty()) {
            throw std::runtime_error("GB28181 projection has no projection_id");
        }
        const auto doneKey = control_protocol::stream::projectionDone(projectionId);
        const std::string_view appliedValue{ applied ? "1" : "0" };
        const std::string_view ttl{ "600" };
        const auto deadlines = service::message::consumer::deadlineKey(stream, group);
        const auto deadlineChannel = service::message::consumer::deadlineChannel(scope, stream, group);
        const std::string_view keys[]{ doneKey, stream, receiptTopic, deadlines, deadlineChannel };
        const std::string_view arguments[]{ appliedValue, ttl, group, messageId, projectionId };
        const auto reply = co_await redis.eval(kMarkProjectionDoneScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("GB28181 projection done", reply);
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
        std::optional<service::message::gb28181::ControlRequest> request;
        try {
            request = ruvia::from_json<service::message::gb28181::ControlRequest>(
                payload.empty() ? std::string_view{ "{}" } : payload,
                { .resource_ = &resource }
            );
        } catch (const std::invalid_argument&) {
            service::common::fail(10001, "GB28181 RPC 请求体无效", 400);
        }
        const auto& input = *request;

        if (operation == control_protocol::kHealthOperation) {
            const auto state = co_await GbProjectionService::loadState(context.redis());
            co_return control_protocol::successJson<service::message::gb28181::HealthResponse>(
                healthJson(context, state),
                context.pool()
            );
        }

        const auto enabled = co_await GbProjectionService::configuredEnabled(context.redis());

        if (operation == control_protocol::kDevicesOperation) {
            GbControlService::requireEnabled(enabled);
            const auto snapshot = co_await GbProjectionService::loadSnapshot(context);
            ruvia::boxed_array<service::message::gb28181::Device> items(
                ruvia::model_options{ .resource_ = context.pool() }
            );
            for (const auto& device : snapshot.devices) {
                items.emplace_back(control_protocol::deviceJson(context.pool(), device));
            }
            service::message::gb28181::DeviceList result(ruvia::model_options{ .resource_ = context.pool() });
            result.set<"items">(std::move(items));
            co_return control_protocol::successJson<service::message::gb28181::DeviceListResponse>(std::move(result), context.pool());
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
    static constexpr std::string_view kOwnerLeaseMs{ "15000" };
    static constexpr std::string_view kCollectorStreamTtlSeconds{ "600" };

    static service::message::gb28181::Health healthJson(ruvia::web_worker_context& context, const ConfigProjectionRecord& state) {
        const ruvia::model_options options{ .resource_ = context.pool() };
        auto ports = ruvia::from_json<service::message::gb28181::MediaPorts>(state.mediaPorts, { .resource_ = context.pool() });
        auto capabilities = ruvia::from_json<service::message::gb28181::MediaCapabilities>(state.mediaCapabilities, { .resource_ = context.pool() });
        service::message::gb28181::Health result(options);
        result.set<"status">(state.mediaStarted ? "ok" : (state.enabled ? "error" : "disabled"))
            .set<"service">("iot-engine-gb28181")
            .set<"enabled">(state.enabled)
            .set<"started">(state.mediaStarted)
            .set<"error">(std::string_view{})
            .set<"mediaPorts">(std::move(ports))
            .set<"mediaCapabilities">(
                std::move(capabilities)
            );
        return result;
    }

    // The timer owns the absolute operation deadline, including subscribe,
    // command publication, Redis reads and reconnects.
    static ruvia::task<void> cancelControlAtDeadline(
        const ruvia::worker_handle& worker,
        std::chrono::system_clock::time_point deadline,
        ruvia::stop_source& expired,
        ruvia::stop_token stop
    ) {
        (void)co_await ruvia::sleep_for(worker, deadline - std::chrono::system_clock::now(), stop);
        if (!stop.stop_requested()) {
            expired.request_stop();
        }
    }

    static ruvia::task<std::optional<ControlResultRecord>> readControlReply(
        ruvia::web_worker_context& context,
        std::string_view topic,
        std::string_view resultKey,
        const std::string& controlKey,
        const std::string& ownerKey,
        std::string_view ownerToken,
        std::span<const service::message::StreamField> fields,
        ruvia::stop_token stop
    ) {
        const auto redis = context.redis().with_options({ .stop_token_ = stop });
        const std::string_view channels[]{ topic };
        bool publishAttempted = false;
        bool published = false;
        while (!stop.stop_requested()) {
            try {
                auto notification = redis.subscription();
                co_await notification.subscribe(channels);
                bool confirmed = false;
                while (const auto event = co_await notification.read()) {
                    if (event->kind() == ruvia::redis_pubsub_event_kind::subscribed && event->channel() == topic) {
                        confirmed = true;
                        break;
                    }
                }
                if (!confirmed) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    throw ruvia::redis_error(ruvia::redis_error::code_type::io_error, "GB28181 reply subscription closed before confirmation");
                }
                if (!published) {
                    // An uncertain XADD must never be retried: the caller
                    // records cancellation and leaves execution owner fencing.
                    publishAttempted = true;
                    co_await GbControlService::publishControlCommand(redis, context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, controlKey, ownerKey, ownerToken, fields);
                    published = true;
                }
                while (!stop.stop_requested()) {
                    if (auto result = co_await GbControlService::loadControlResult(redis, resultKey)) {
                        co_return result;
                    }
                    const auto event = co_await notification.read();
                    if (!event || event->kind() == ruvia::redis_pubsub_event_kind::unsubscribed) {
                        break;
                    }
                }
            } catch (const ruvia::redis_error& error) {
                if (stop.stop_requested()) {
                    break;
                }
                if ((publishAttempted && !published) ||
                    (error.code() != ruvia::redis_error::code_type::io_error &&
                     error.code() != ruvia::redis_error::code_type::connect_failed &&
                     error.code() != ruvia::redis_error::code_type::timeout)) {
                    throw;
                }
                LOG_WARN << "GB28181 reply subscription " << topic << " reconnecting: " << error.what();
            }
            // No business polling: retry only a lost subscription, then
            // confirm it before reading the persisted result again.
            (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(50), stop);
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

        const auto requestId = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
        const auto replyTopic = control_protocol::stream::reply(requestId);
        const auto resultKey = control_protocol::stream::result(requestId);
        const auto cancelKey = control_protocol::stream::cancel(requestId);
        const auto now = std::chrono::system_clock::now();
        const auto deadline = now + std::chrono::seconds(15);
        const auto deadlineMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    deadline.time_since_epoch()
        )
                                    .count();
        const auto controlKey = control_protocol::stream::control(*owner, *instance);
        const std::vector<service::message::StreamField> fields{
            { "request_id", requestId },
            { "owner_key", std::string(ownerKey) },
            { "owner_token", std::string(ownerToken) },
            { "operation", std::string(operation) },
            { "payload", std::string(payload) },
            { "reply_stream", replyTopic },
            { "result_key", resultKey },
            { "cancel_key", cancelKey },
            { "deadline_ms", std::to_string(deadlineMs) }
        };
        ruvia::stop_source expired;
        ruvia::task_scope timer(context.worker());
        const auto timerStop = ruvia::combine_stop_tokens(stop, context.stop_token());
        const auto operationStop = ruvia::combine_stop_tokens(timerStop, expired.token());
        timer.spawn(cancelControlAtDeadline(context.worker(), deadline, expired, ruvia::combine_stop_tokens(timerStop, timer.get_stop_token())));
        std::optional<ControlResultRecord> response;
        std::exception_ptr failure;
        try {
            response = co_await readControlReply(context, replyTopic, resultKey, controlKey, ownerKey, ownerToken, fields, operationStop);
        } catch (...) {
            failure = std::current_exception();
        }
        timer.request_stop();
        co_await timer.join();
        if (failure || !response) {
            // Publication may have reached Redis even if its reply was lost.
            // Fence uncertain delivery and already-claimed execution alike.
            try {
                co_await GbControlService::markControlCancelled(context.redis(), cancelKey);
            } catch (const std::exception& error) {
                LOG_WARN << "GB28181 control " << requestId << " cancellation was not recorded: " << error.what();
            }
            if (failure) {
                std::rethrow_exception(failure);
            }
            throw std::runtime_error(timerStop.stop_requested() ? "GB28181 operation was cancelled" : "GB28181 Collector did not reply before deadline");
        }
        const auto& status = response->status;
        const auto& result = response->payload;
        if (status != "ok") {
            throw std::runtime_error(result.empty() ? "GB28181 Collector operation failed" : std::string(result));
        }
        co_return std::move(response->payload);
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
