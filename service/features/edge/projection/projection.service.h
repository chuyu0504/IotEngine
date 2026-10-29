#pragma once
#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <google/protobuf/util/json_util.h>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/task.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/db/db_repository.h>
#include <ruvia/web/model.h>
#include <ruvia/web/web_worker.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/wireguard.h"
#include "service/common/worker.h"
#include "service/features/command/command.entity.h"
#include "service/features/edge/acquisition/acquisition.protocol.h"
#include "service/features/edge/edge.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/edge.service.h"
#include "service/features/edge/gateway/gateway.entity.h"
#include "service/features/edge/gateway/gateway.types.h"
#include "service/features/edge/projection/projection.entity.h"
#include "service/features/edge/projection/projection.types.h"
#include "service/features/observability/observability.service.h"
#include "service/features/telemetry/derived/derived.service.h"
#include "service/features/telemetry/latest/latest.service.h"
#include "service/features/telemetry/telemetry.service.h"
#include "service/utils/crypto.h"
#include "service/utils/json.h"
#include "service/utils/log.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"
#include "service/utils/rpc_client.h"
#include "service/utils/startup.h"
#include "service/utils/uuid.h"

namespace service::edge::projection {
inline constexpr std::string_view kIngressGroup{ "iot-engine:edge-projector" };
inline constexpr auto kLeaseTtl = std::chrono::milliseconds(15000);

// SET NX: establishes a Worker lease or a recovery lease of an expired owner.
inline constexpr std::string_view kClaimLeaseScript = R"lua(
if not redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') then return 0 end
local now = redis.call('TIME')
redis.call('ZADD', KEYS[2], now[1] * 1000 + math.floor(now[2] / 1000) + tonumber(ARGV[2]), ARGV[3])
return 1
)lua";

// Token-checked renewal; never recreates an expired lease.
inline constexpr std::string_view kRenewLeaseScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
local now = redis.call('TIME')
redis.call('ZADD', KEYS[2], now[1] * 1000 + math.floor(now[2] / 1000) + tonumber(ARGV[2]), ARGV[3])
return 1
)lua";

inline constexpr std::string_view kLeaseOwnedScript = R"lua(
if redis.call('GET', KEYS[1]) == ARGV[1] then return 1 end
return 0
)lua";

inline constexpr std::string_view kReleaseLeaseScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
)lua";

// Deletes a recovered Stream only after it is empty and has no pending entries.
inline constexpr std::string_view kEraseDeadStreamScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) ~= 0 then
  if redis.call('XLEN', KEYS[2]) ~= 0 then return 0 end
  local groups = redis.call('XINFO', 'GROUPS', KEYS[2])
  for _, group in ipairs(groups) do
    for index = 1, #group, 2 do
      if group[index] == 'pending' and tonumber(group[index + 1]) > 0 then
        return 0
      end
    end
  end
  redis.call('DEL', KEYS[2])
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[3], KEYS[2])
redis.call('ZREM', KEYS[4], KEYS[2])
return 1
)lua";

// Acknowledges projected entries only while the consuming lease is still owned.
inline constexpr std::string_view kAcknowledgeFencedScript = R"lua(
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local group = ARGV[2]
local removed = 0
for index = 3, #ARGV do
  redis.call('XACK', KEYS[2], group, ARGV[index])
  redis.call('XDEL', KEYS[2], ARGV[index])
  removed = removed + redis.call('ZREM', KEYS[3], ARGV[index])
end
if removed > 0 then redis.call('PUBLISH', KEYS[4], 'changed') end
return 1
)lua";

// Bootstraps existing registered Streams once on startup, preserving live leases.
inline constexpr std::string_view kSeedRecoveryDeadlineScript = R"lua(
if redis.call('EXISTS', KEYS[2]) == 0 then
  redis.call('SREM', KEYS[1], KEYS[2])
  redis.call('ZREM', KEYS[4], KEYS[2])
  return 0
end
local now = redis.call('TIME')
local ttl = math.max(0, redis.call('PTTL', KEYS[3]))
return redis.call('ZADD', KEYS[4], 'NX', now[1] * 1000 + math.floor(now[2] / 1000) + ttl, KEYS[2])
)lua";

inline constexpr std::string_view kDueRecoveryStreamsScript = R"lua(
local now = redis.call('TIME')
return redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now[1] * 1000 + math.floor(now[2] / 1000), 'LIMIT', 0, 128)
)lua";

inline constexpr std::string_view kNextRecoveryDeadlineScript = R"lua(
local first = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #first == 0 then return 0 end
local now = redis.call('TIME')
return math.max(1, tonumber(first[2]) - now[1] * 1000 - math.floor(now[2] / 1000))
)lua";

// Lease, fencing and dead-Stream recovery of the projector Streams in Redis.
class ProjectorStreams final {
  public:
    static ProjectorStreamOwner current(std::size_t workerIndex, std::string_view instance) {
        return { service::message::edge::projector_stream::stream(workerIndex, instance),
                 service::message::edge::projector_stream::leaseKey(workerIndex, instance),
                 service::message::edge::projector_stream::ownerToken(workerIndex, instance) };
    }

    template <typename Redis>
    static ruvia::task<bool> claimLease(const Redis& redis, std::string_view key, std::string_view token) {
        co_return co_await leaseScript(redis, kClaimLeaseScript, key, token, true, "claim edge projector lease");
    }

    template <typename Redis>
    static ruvia::task<bool> renewLease(const Redis& redis, std::string_view key, std::string_view token) {
        co_return co_await leaseScript(redis, kRenewLeaseScript, key, token, true, "renew edge projector lease");
    }

    template <typename Redis>
    static ruvia::task<bool> leaseOwned(const Redis& redis, std::string_view key, std::string_view token) {
        co_return co_await leaseScript(redis, kLeaseOwnedScript, key, token, false, "check edge projector lease");
    }

    template <typename Redis>
    static ruvia::task<bool> releaseLease(const Redis& redis, std::string_view key, std::string_view token) {
        co_return co_await leaseScript(redis, kReleaseLeaseScript, key, token, false, "release edge projector lease");
    }

    template <typename Redis>
    static ruvia::task<void> registerStream(const Redis& redis, std::string_view streamName) {
        const std::vector<std::string> arguments{
            "SADD",
            std::string(service::message::edge::projector_stream::kStreamRegistry),
            std::string(streamName)
        };
        const auto reply = co_await redis.command(std::vector<std::string_view>(arguments.begin(), arguments.end()));
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("register edge projector stream", reply);
        }
    }

    // Drops registry members whose Stream no longer exists.
    template <typename Redis>
    static ruvia::task<void> cleanupRegistry(const Redis& redis) {
        const std::string registry(service::message::edge::projector_stream::kStreamRegistry);
        const std::vector<std::string> arguments{ "SMEMBERS", registry };
        const auto reply = co_await redis.command(std::vector<std::string_view>(arguments.begin(), arguments.end()));
        if (reply.kind() == ruvia::redis_value::kind_type::null) {
            co_return;
        }
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            service::utils::redis::throwValue("list edge projector streams", reply);
        }
        for (const auto& value : reply.array()) {
            if (value.kind() != ruvia::redis_value::kind_type::string) {
                continue;
            }
            const std::string streamName(value.string());
            const auto owner = ownerFromStream(streamName);
            if (!owner) {
                throw std::runtime_error("invalid registered edge projector stream");
            }
            const auto lease = service::message::edge::projector_stream::leaseKey(owner->second, owner->first);
            const std::string_view keys[]{ registry, streamName, lease, service::message::edge::projector_stream::kRecoveryDeadlines };
            const auto removed = co_await redis.eval(kSeedRecoveryDeadlineScript, keys, std::span<const std::string_view>{});
            if (removed.kind() != ruvia::redis_value::kind_type::integer) {
                service::utils::redis::throwValue("clean edge projector registry", removed);
            }
        }
    }

    // Owner (instance, Worker index) encoded in a projector Stream name.
    static std::optional<std::pair<std::string, std::size_t>> ownerFromStream(std::string_view streamName) {
        if (!streamName.starts_with(service::message::edge::projector_stream::kStreamPrefix)) {
            return std::nullopt;
        }
        const auto suffix = streamName.substr(service::message::edge::projector_stream::kStreamPrefix.size());
        const auto separator = suffix.rfind(':');
        if (separator == std::string_view::npos || separator == 0 || separator + 1 >= suffix.size()) {
            return std::nullopt;
        }
        const auto instance = suffix.substr(0, separator);
        if (instance.find(':') != std::string_view::npos) {
            return std::nullopt;
        }
        std::uint64_t parsedIndex = 0;
        const auto indexText = suffix.substr(separator + 1);
        const auto [end, error] = std::from_chars(indexText.data(), indexText.data() + indexText.size(), parsedIndex);
        if (error != std::errc{} || end != indexText.data() + indexText.size() ||
            parsedIndex > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
            return std::nullopt;
        }
        return std::pair{ std::string(instance), static_cast<std::size_t>(parsedIndex) };
    }

    static std::string recoveryToken(std::size_t workerIndex, std::string_view instance, const std::pair<std::string, std::size_t>& owner) {
        return std::string(instance) + ":recovery:" + std::to_string(workerIndex) + ":" +
            owner.first + ":" + std::to_string(owner.second);
    }

    // Streams of expired owners whose recovery lease this Worker now holds.
    template <typename Redis>
    static ruvia::task<std::vector<ProjectorStreamOwner>> discoverDeadStreams(const Redis& redis, std::string_view currentStream, std::size_t workerIndex, std::string_view instance) {
        std::map<std::string, ProjectorStreamOwner, std::less<>> dead;
        const std::string_view keys[]{ service::message::edge::projector_stream::kRecoveryDeadlines };
        const auto due = co_await redis.eval(kDueRecoveryStreamsScript, keys, std::span<const std::string_view>{});
        if (due.kind() != ruvia::redis_value::kind_type::array) {
            service::utils::redis::throwValue("find due edge projector recovery", due);
        }
        for (const auto& entry : due.array()) {
            if (entry.kind() != ruvia::redis_value::kind_type::string) {
                throw std::runtime_error("invalid edge projector recovery deadline");
            }
            const std::string candidate(entry.string());
            if (candidate == currentStream) {
                continue;
            }
            const auto owner = ownerFromStream(candidate);
            if (!owner) {
                continue;
            }
            const auto lease = service::message::edge::projector_stream::leaseKey(owner->second, owner->first);
            const auto token = recoveryToken(workerIndex, instance, *owner);
            const auto current = co_await redis.get(lease);
            if (current) {
                // A non-expired owner or another recovery Worker still owns this
                // Stream; neither case is safe to steal. Seeing our own recovery
                // token is not enough either: the lease may expire before the
                // first read, so it must be renewed (token-checked) first.
                if (std::string_view(current->data(), current->size()) != token ||
                    !co_await renewLease(redis, lease, token)) {
                    continue;
                }
            } else if (!co_await claimLease(redis, lease, token)) {
                continue;
            }
            dead.emplace(candidate, ProjectorStreamOwner{ candidate, lease, token });
        }
        std::vector<ProjectorStreamOwner> result;
        result.reserve(dead.size());
        for (auto& [streamName, owner] : dead) {
            result.push_back(std::move(owner));
        }
        co_return result;
    }

    template <typename Redis>
    static ruvia::task<bool> eraseDeadStream(const Redis& redis, const ProjectorStreamOwner& deadStream) {
        const std::string registry(service::message::edge::projector_stream::kStreamRegistry);
        const std::string_view keys[]{ deadStream.lease, deadStream.stream, registry, service::message::edge::projector_stream::kRecoveryDeadlines };
        const std::string_view arguments[]{ deadStream.token };
        const auto reply = co_await redis.eval(kEraseDeadStreamScript, keys, arguments);
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("erase dead edge projector stream", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<bool> acknowledgeFenced(const Redis& redis, const service::message::RedisChannelScope& scope, const ProjectorStreamOwner& owner, const std::vector<service::message::StreamMessage>& messages) {
        if (messages.empty()) {
            co_return true;
        }
        const auto deadline = service::message::consumer::deadlineKey(owner.stream, kIngressGroup);
        const auto channel = service::message::consumer::deadlineChannel(scope, owner.stream, kIngressGroup);
        const std::string_view keys[]{ owner.lease, owner.stream, deadline, channel };
        std::vector<std::string_view> arguments;
        arguments.reserve(messages.size() + 2);
        arguments.push_back(owner.token);
        arguments.push_back(kIngressGroup);
        for (const auto& message : messages) {
            arguments.push_back(message.id);
        }
        const auto reply = co_await redis.eval(kAcknowledgeFencedScript, keys, std::span<const std::string_view>(arguments));
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("fenced edge projector acknowledgement", reply);
        }
        co_return reply.integer() == 1;
    }

    template <typename Redis>
    static ruvia::task<std::chrono::milliseconds> nextRecoveryDelay(const Redis& redis) {
        const std::string_view keys[]{ service::message::edge::projector_stream::kRecoveryDeadlines };
        const auto reply = co_await redis.eval(kNextRecoveryDeadlineScript, keys, std::span<const std::string_view>{});
        if (reply.kind() != ruvia::redis_value::kind_type::integer || reply.integer() <= 0) {
            throw std::runtime_error("edge projector recovery deadline missing");
        }
        co_return std::chrono::milliseconds(reply.integer());
    }

  private:
    template <typename Redis>
    static ruvia::task<bool> leaseScript(const Redis& redis, std::string_view script, std::string_view key, std::string_view token, bool withTtl, std::string_view operation) {
        const auto ttl = std::to_string(kLeaseTtl.count());
        const auto stream = std::string(service::message::edge::projector_stream::kStreamPrefix) +
            std::string(key.substr(service::message::edge::projector_stream::kLeasePrefix.size()));
        const std::string_view keys[]{ key, service::message::edge::projector_stream::kRecoveryDeadlines };
        const std::string_view arguments[]{ token, ttl, stream };
        const auto reply = co_await redis.eval(script, keys, std::span<const std::string_view>(arguments, withTtl ? 3U : 1U));
        if (reply.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue(operation, reply);
        }
        co_return reply.integer() == 1;
    }
};

// Persists EdgeNode protocol messages taken from a Worker's projector Stream.
// Stateless; the Worker-local catalog and caches live in ProjectionState.
class ProjectionService final {
  public:
    static ruvia::task<void> initialize(ruvia::web_worker_context& context, ProjectionState& state) {
        const auto& identity = context.worker_state<std::optional<service::common::WorkerIdentity>>().value();
        co_await service::utils::initializeOnce(
            context,
            service::message::edge::projector_stream::kStartupLock,
            service::message::edge::projector_stream::startupKey(identity.instanceId),
            [](ruvia::web_worker_context& owner) -> ruvia::task<void> {
                co_await ProjectorStreams::cleanupRegistry(owner.redis());
                co_await hydrateAuth(owner);
                (void)co_await metadata::hydrate(owner);
            }
        );
        // Every Worker starts with an empty local cache and loads only the node
        // named by its first ingress/metadata entry from the completed snapshot.
        state = {};
    }

    static service::message::ParsedDeviceMessage decodeAcquisitionSample(const service::edge::persistence::AcquisitionSampleEntity& row, const service::message::ParsedDeviceMessage& prototype) {
        const auto document = ruvia::from_json<AcquisitionSampleDocument>(row.get<"payload">());
        auto input = prototype;
        input.messageId = input.acquisitionId = row.get<"record_id">();
        input.linkId = document.get<"linkId">().view();
        input.modelId = document.get<"modelId">().view();
        input.protocol = document.get<"protocol">().view();
        input.occurredAtMs = document.get<"occurredAtMs">().value_;
        input.observedAtMs = row.get<"observed_at_ms">();
        input.valuesJson = document.get<"valuesJson">().view();
        input.storagePolicy = document.get<"storagePolicy">().view();
        return input;
    }

    static ruvia::task<std::vector<service::message::ParsedDeviceMessage>> loadHistoricalPage(ruvia::db_transaction& transaction, const service::message::ParsedDeviceMessage& prototype, const std::optional<std::pair<std::int64_t, std::string>>& cursor) {
        using Entity = service::edge::persistence::AcquisitionSampleEntity;
        ruvia::db_find_options options;
        options.where_ = Entity::column<"node_id">() == std::string_view(prototype.connectionId) && Entity::column<"digest">() == std::string_view(prototype.configurationDigest) && Entity::column<"device_id">() == std::string_view(prototype.deviceId);
        if (cursor) {
            options.where_ = options.where_ && ((Entity::column<"observed_at_ms">() > cursor->first) || ((Entity::column<"observed_at_ms">() == cursor->first) && (Entity::column<"record_id">() > std::string_view(cursor->second))));
        }
        const std::array<ruvia::db_find_order, 2> order{ { { Entity::column_name<"observed_at_ms">() }, { Entity::column_name<"record_id">() } } };
        options.order_ = order;
        options.take_ = 256;
        const auto rows = co_await transaction.repository<Entity>().find(options);
        std::vector<service::message::ParsedDeviceMessage> inputs;
        inputs.reserve(rows.size());
        for (const auto& row : rows) {
            inputs.push_back(decodeAcquisitionSample(row, prototype));
        }
        co_return inputs;
    }

    // Journal, calculation checkpoint and derived history commit before ingress ACK.
    // Ordered samples advance a durable checkpoint. Late samples rebuild in bounded
    // pages, independent of arrival order, Redis state and PostgreSQL parameter limits.
    static ruvia::task<void> recomputeHistoricalDerived(ruvia::web_worker_context& context, const ProjectionBatch& batch) {
        using Entity = service::edge::persistence::AcquisitionSampleEntity;
        using Checkpoint = service::edge::persistence::AcquisitionCalculationEntity;
        using Derived = service::telemetry::derived::DerivedService;
        using Key = std::tuple<std::string, std::string, std::string>;
        std::map<Key, std::vector<const service::message::ParsedDeviceMessage*>> groups;
        for (const auto& sample : batch.telemetry) {
            if (!sample.configurationDigest.empty() && batch.rawCalculations.contains(sample.messageId)) {
                groups[{ sample.connectionId, sample.configurationDigest, sample.deviceId }].push_back(&sample);
            }
        }
        for (const auto& [key, samples] : groups) {
            const auto& [nodeId, digest, deviceId] = key;
            const auto& config = batch.rawCalculations.at(samples.front()->messageId);
            const auto sourceViews = config.sourceViews();
            const auto prepared = Derived::prepare(deviceId, config.config, sourceViews, digest);
            if (!prepared) {
                continue;
            }
            auto transaction = co_await context.db("edge-history").begin_transaction();
            co_await transaction.advisory_lock(deviceId, service::message::lock::kTelemetryDeviceSeed);
            std::vector<service::message::ParsedDeviceMessage> insertedInputs;
            for (const auto* sample : samples) {
                AcquisitionSampleDocument document;
                document.set<"linkId">(sample->linkId).set<"modelId">(sample->modelId).set<"protocol">(sample->protocol).set<"occurredAtMs">(sample->occurredAtMs).set<"valuesJson">(sample->valuesJson).set<"storagePolicy">(sample->storagePolicy);
                const auto payload = ruvia::to_json(document);
                Entity row;
                row.set<"node_id">(nodeId).set<"digest">(digest).set<"device_id">(deviceId).set<"record_id">(sample->messageId).set<"observed_at_ms">(sample->observedAtMs).set<"payload">(std::string_view(payload.data(), payload.size()));
                const auto inserted = co_await transaction.repository<Entity>().upsert(row, { .do_nothing_ = true });
                if (inserted.affected_rows() != 0) {
                    insertedInputs.push_back(*sample);
                } else {
                    ruvia::db_find_options existing;
                    existing.where_ = Entity::column<"node_id">() == std::string_view(nodeId) && Entity::column<"digest">() == std::string_view(digest) && Entity::column<"device_id">() == std::string_view(deviceId) && Entity::column<"record_id">() == std::string_view(sample->messageId);
                    const auto previous = co_await transaction.repository<Entity>().find_one(existing);
                    if (!previous) {
                        throw std::runtime_error("acquisition sample was not persisted");
                    }
                    auto previousDocument = ruvia::from_json<AcquisitionSampleDocument>(previous->get<"payload">());
                    previousDocument.set<"occurredAtMs">(sample->occurredAtMs);
                    if (ruvia::to_json(previousDocument) != payload || previous->get<"observed_at_ms">() != sample->observedAtMs) {
                        throw std::runtime_error("replayed acquisition sample changed its protected payload");
                    }
                }
            }
            bool historyChanged = false;
            if (!insertedInputs.empty()) {
                std::ranges::sort(insertedInputs, {}, [](const auto& value) {
                    return std::tie(value.observedAtMs, value.messageId);
                });
                ruvia::db_find_options current;
                current.where_ = Checkpoint::column<"node_id">() == std::string_view(nodeId) && Checkpoint::column<"digest">() == std::string_view(digest) && Checkpoint::column<"device_id">() == std::string_view(deviceId);
                const auto checkpoint = co_await transaction.repository<Checkpoint>().find_one(current);
                const bool append = checkpoint && std::tie(insertedInputs.front().observedAtMs, insertedInputs.front().messageId) > std::tuple{ checkpoint->get<"observed_at_ms">(), std::string(checkpoint->get<"record_id">()) };
                service::telemetry::derived::State state;
                std::optional<service::message::ParsedDeviceMessage> previous;
                if (append) {
                    state = service::telemetry::derived::StateRecord::decode(checkpoint->get<"state">());
                    ruvia::db_find_options last;
                    last.where_ = Entity::column<"node_id">() == std::string_view(nodeId) && Entity::column<"digest">() == std::string_view(digest) && Entity::column<"device_id">() == std::string_view(deviceId) && Entity::column<"record_id">() == std::string_view(checkpoint->get<"record_id">());
                    const auto row = co_await transaction.repository<Entity>().find_one(last);
                    if (!row) {
                        throw std::runtime_error("historical calculation checkpoint lost its input");
                    }
                    previous = decodeAcquisitionSample(*row, *samples.front());
                    const auto output = Derived::replayHistory(*prepared, insertedInputs, &state, &*previous);
                    co_await service::telemetry::TelemetryService::persistSnapshotDerived(context, transaction, nodeId, digest, deviceId, output);
                    historyChanged = !output.empty();
                    previous = insertedInputs.back();
                } else {
                    co_await service::telemetry::TelemetryService::persistSnapshotDerived(context, transaction, nodeId, digest, deviceId, {}, true);
                    std::optional<std::pair<std::int64_t, std::string>> cursor;
                    for (;;) {
                        auto inputs = co_await loadHistoricalPage(transaction, *samples.front(), cursor);
                        if (inputs.empty()) {
                            break;
                        }
                        const auto output = Derived::replayHistory(*prepared, inputs, &state, previous ? &*previous : nullptr);
                        co_await service::telemetry::TelemetryService::persistSnapshotDerived(context, transaction, nodeId, digest, deviceId, output);
                        previous = std::move(inputs.back());
                        cursor.emplace(previous->observedAtMs, previous->messageId);
                    }
                    historyChanged = true;
                }
                Checkpoint next;
                next.set<"node_id">(nodeId).set<"digest">(digest).set<"device_id">(deviceId).set<"record_id">(previous->messageId).set<"observed_at_ms">(previous->observedAtMs).set<"state">(service::telemetry::derived::StateRecord::encode(state));
                const std::array<ruvia::borrowed_text, 3> conflictColumns{ Checkpoint::column_name<"node_id">(), Checkpoint::column_name<"digest">(), Checkpoint::column_name<"device_id">() };
                (void)co_await transaction.repository<Checkpoint>().upsert(next, { .conflict_paths_ = conflictColumns });
            }
            co_await transaction.commit();
            if (historyChanged) {
                co_await service::live::publish(context.redis(), context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, service::message::live::topic::kDeviceHistory, std::vector<std::string>{ deviceId });
            }
        }
    }

    static ruvia::task<std::vector<DerivedDeadline>> publishTelemetry(ruvia::web_worker_context& context, ProjectionBatch& projected) {
        std::vector<DerivedDeadline> deadlines;
        std::vector<service::message::StreamMessage> messages;
        messages.reserve(projected.telemetry.size());
        co_await recomputeHistoricalDerived(context, projected);
        auto fence = co_await service::configuration::fenceAcquisition(context, projected.telemetry);
        for (auto& parsed : projected.telemetry) {
            const auto calculation = projected.rawCalculations.find(parsed.messageId);
            if (!parsed.historyOnly && calculation != projected.rawCalculations.end()) {
                const auto sourceViews = calculation->second.sourceViews();
                const auto prepared = service::telemetry::derived::DerivedService::prepare(parsed.deviceId, calculation->second.config, sourceViews, calculation->second.snapshotDigest);
                std::optional<std::int64_t> next;
                bool published = false;
                if (prepared) {
                    const auto result = co_await service::telemetry::derived::DerivedService::append(
                        context.redis(),
                        context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                        *prepared,
                        parsed,
                        context.worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId,
                        false,
                        true
                    );
                    next = result.nextDeadline;
                    published = result.published;
                }
                deadlines.push_back({ parsed, calculation->second, next });
                if (published) {
                    continue;
                }
            }
            service::message::StreamMessage message;
            message.fields = service::message::parsedFields(parsed);
            messages.push_back(std::move(message));
        }
        co_await service::telemetry::TelemetryService::ingest(context, messages);
        if (fence) {
            co_await fence->commit();
        }
        co_await finishTelemetryUploads(context.redis(), projected.completedUploads);
        co_return deadlines;
    }

    static ruvia::task<std::optional<std::int64_t>> expireDerived(ruvia::web_worker_context& context, service::message::ParsedDeviceMessage& parsed, const service::configuration::DerivedConfiguration& config) {
        std::optional<ruvia::db_transaction> fence;
        if (!config.snapshotDigest.empty()) {
            fence.emplace(co_await service::configuration::lockAcquisition(context));
            if (parsed.configurationEpoch < 0 || parsed.configurationEpoch != co_await service::configuration::acquisitionEpoch(*fence, parsed.connectionId)) {
                co_await fence->commit();
                co_return std::nullopt;
            }
            const auto snapshot = co_await ConfigService::loadSnapshotByDigest(context, parsed.connectionId, config.snapshotDigest);
            if (!snapshot) {
                throw std::runtime_error("edge acquisition snapshot is unavailable");
            }
            std::map<std::string, service::configuration::DerivedConfiguration> calculations;
            const auto items = co_await ConfigService::loadItems(context, parsed.connectionId, &calculations);
            if (!ConfigService::snapshotMatches(*snapshot, items, calculations)) {
                co_return std::nullopt;
            }
        }
        parsed.messageId = service::utils::nextUuid(context.worker_state<service::common::UuidV7State>());
        parsed.acquisitionId = parsed.causationId = parsed.messageId;
        parsed.source = "derived";
        parsed.rawPayloads.clear();
        parsed.rawPacketIds.clear();
        parsed.observedAtMs = parsed.occurredAtMs = service::message::utcNowMilliseconds();
        parsed.valuesJson = "{\"values\":{}}";
        if (!config.snapshotDigest.empty()) {
            ProjectionBatch history;
            history.telemetry.push_back(parsed);
            history.rawCalculations.emplace(parsed.messageId, config);
            co_await recomputeHistoricalDerived(context, history);
        }
        const auto sourceViews = config.sourceViews();
        const auto prepared = service::telemetry::derived::DerivedService::prepare(parsed.deviceId, config.config, sourceViews, config.snapshotDigest);
        if (!prepared) {
            co_return std::nullopt;
        }
        const auto result = (co_await service::telemetry::derived::DerivedService::append(
                                 context.redis(),
                                 context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
                                 *prepared,
                                 parsed,
                                 context.worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId,
                                 true
                             ))
                                .nextDeadline;
        if (fence) {
            co_await fence->commit();
        }
        co_return result;
    }

    static void failWorker(ruvia::web_worker_context& context, observability::RuntimeDiagnostics& diagnostics) {
        try {
            diagnostics.setComponentStatus("edge-projector", observability::ComponentState::Failed, "worker lease lost");
        } catch (const std::exception& error) {
            LOG_ERROR << "edge projector status update failed: " << error.what();
        } catch (...) {
            LOG_ERROR << "edge projector status update failed: unknown error";
        }
        context.worker_state<SessionDispatcher>().failSessions("worker lease lost");
    }

    static ruvia::task<void> hydrateAuth(ruvia::web_worker_context& context) {
        ruvia::db_query query;
        auto queryBuilder = context.db().template repository<service::edge::persistence::EdgeNodeEntity>().query_builder();
        queryBuilder.select({ { "imei", query.column(service::edge::persistence::EdgeNodeEntity::column_name<"imei">()) }, { "id", query.cast(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_data_type::text) }, { "enrollment_status", query.column(service::edge::persistence::EdgeNodeEntity::column_name<"enrollment_status">()) } });
        const auto rows = co_await queryBuilder.get_raw_many();
        if (rows.empty()) {
            co_return;
        }
        auto pipeline = context.redis().pipeline();
        for (const auto& row : rows) {
            const auto key = service::message::edge::authKey(row[0].value().value_or(std::string_view{}));
            const auto value = std::string(row[1].value().value_or(std::string_view{})) + "|" + std::string(row[2].value().value_or(std::string_view{}));
            pipeline.set(key, value);
        }
        const auto replies = co_await std::move(pipeline).exec();
        service::utils::redis::requirePipelineSuccess("hydrate edge authorization", replies);
    }

    // Projects one ingress entry. Telemetry, completed uploads and raw-acquisition
    // calculations are collected in `batch` and applied once per Stream batch.
    static ruvia::task<void> project(ruvia::web_worker_context& context, std::size_t workerIndex, ProjectionState& state, std::string_view wire, std::string_view receivedAtText, ProjectionBatch& batch) {
        auto& catalog = state.catalog;
        pb::Envelope envelope;
        if (!protocol::decode(wire, envelope)) {
            co_return;
        }
        const auto receivedAt = service::utils::parseInt64(
            receivedAtText.empty() ? std::nullopt
                                   : std::optional<std::string_view>(receivedAtText)
        );
        const auto receivedAtMs = receivedAt.value_or(service::message::utcNowMilliseconds());
        if (envelope.payload_case() == pb::Envelope::kHello) {
            co_await saveHello(context, envelope.hello());
            co_return;
        }
        if (envelope.node_id().size() != 16) {
            co_return;
        }
        const auto nodeId = service::common::uuidText(envelope.node_id());
        if (envelope.has_telemetry_batch() || envelope.has_command_result()) {
            // Another Worker may have refreshed this node's Redis snapshot.
            if (auto snapshot = co_await metadata::loadNode(context.redis(), nodeId)) {
                catalog[nodeId] = std::move(*snapshot);
            } else {
                catalog.erase(nodeId);
            }
        }
        switch (envelope.payload_case()) {
            case pb::Envelope::kHeartbeat:
                co_await saveHeartbeat(context, nodeId, envelope.heartbeat());
                break;
            case pb::Envelope::kDtuStatus: {
                const auto& status = envelope.dtu_status();
                if (status.channel_id().size() != 16) {
                    break;
                }
                std::string json;
                if (!google::protobuf::util::MessageToJsonString(status, &json).ok()) {
                    break;
                }
                auto withoutDebug = status;
                withoutDebug.clear_traces();
                withoutDebug.clear_omitted_traces();
                std::string statusOnly;
                if (!google::protobuf::util::MessageToJsonString(withoutDebug, &statusOnly).ok()) {
                    break;
                }
                ruvia::db_query update;
                const auto projected = update.case_when({ { update.binary(config::detail::jsonText(update, update.column(persistence::EdgeDtuEntity::column_name<"config">()), "debugEnabled"), ruvia::db_binary_operator::equal, update.value("true")), update.cast(update.value(json), ruvia::db_data_type::jsonb) } }, update.cast(update.value(statusOnly), ruvia::db_data_type::jsonb));
                auto updateBuilder = context.db().template repository<persistence::EdgeDtuEntity>().update_builder();
                updateBuilder.set(persistence::EdgeDtuEntity::column_name<"status">(), projected).where(update.binary(update.column(persistence::EdgeDtuEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, update.cast(update.value(nodeId), ruvia::db_data_type::uuid))).and_where(update.binary(update.column(persistence::EdgeDtuEntity::column_name<"channel_id">()), ruvia::db_binary_operator::equal, update.cast(update.value(service::common::uuidText(status.channel_id())), ruvia::db_data_type::uuid))).and_where(update.binary(update.column(persistence::EdgeDtuEntity::column_name<"status">()), ruvia::db_binary_operator::not_equal, projected));
                (void)co_await updateBuilder.execute();
                break;
            }
            case pb::Envelope::kCapabilityReport:
                co_await saveCapabilities(context, workerIndex, nodeId, envelope.capability_report());
                break;
            case pb::Envelope::kExternalIoStatus:
                co_await saveIoState(context, nodeId, envelope.session_epoch(), envelope.sequence(), envelope.external_io_status());
                break;
            case pb::Envelope::kNetworkConfigResult:
                co_await saveNetworkResult(context, nodeId, envelope.network_config_result());
                break;
            case pb::Envelope::kVpnConfigResult:
                co_await saveVpnResult(context, workerIndex, nodeId, envelope.vpn_config_result());
                break;
            case pb::Envelope::kFirmwareUpdateResult:
                co_await saveFirmwareResult(context, nodeId, envelope.firmware_update_result());
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
                    if (record.configuration_sha256().empty() && record.has_device_status() &&
                        record.device_status().device_id() == record.device_id()) {
                        pb::DeviceStatusReport report;
                        *report.add_devices() = record.device_status();
                        co_await saveDeviceStatus(context, nodeId, report);
                    }
                }
                std::vector<std::string> deviceIds;
                for (const auto& record : envelope.telemetry_batch().records()) {
                    if (record.device_id().size() == 16) {
                        deviceIds.push_back(service::common::uuidText(record.device_id()));
                    }
                }
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
                        batch.completedUploads.push_back({ nodeId, service::common::uuidText(record.device_id()), service::common::uuidText(record.report_id()) });
                    }
                }
                const bool versioned = std::any_of(assembled.records().begin(), assembled.records().end(), [](const auto& record) {
                    return !record.configuration_sha256().empty();
                });
                std::optional<ruvia::db_transaction> acquisitionFence;
                std::int64_t epoch = -1;
                if (versioned) {
                    acquisitionFence.emplace(co_await service::configuration::lockAcquisition(context));
                    epoch = co_await service::configuration::acquisitionEpoch(*acquisitionFence, nodeId);
                }
                if (versioned) {
                    // The configuration notification and telemetry use independent streams.
                    // Re-read once per incoming batch to account for committed configuration
                    // changes that have not yet reached the notification stream.
                    AcquisitionConfiguration current;
                    current.items = co_await ConfigService::loadItems(context, nodeId, &current.calculations);
                    state.acquisitionConfigurations.insert_or_assign(nodeId, std::move(current));
                } else if (std::any_of(assembled.records().begin(), assembled.records().end(), [](const auto& record) {
                               return record.raw_requests_size() != 0 || record.protocol() == pb::PROTOCOL_EXTERNAL_IO || !record.configuration_sha256().empty();
                           }) &&
                           !state.acquisitionConfigurations.contains(nodeId)) {
                    AcquisitionConfiguration configuration;
                    configuration.items = co_await ConfigService::loadItems(context, nodeId, &configuration.calculations);
                    state.acquisitionConfigurations.emplace(nodeId, std::move(configuration));
                }
                const auto configuration = state.acquisitionConfigurations.find(nodeId);
                for (const auto& record : assembled.records()) {
                    pb::TelemetryBatch one;
                    *one.add_records() = record;
                    const std::vector<pb::ConfigItem> emptyItems;
                    const std::map<std::string, service::configuration::DerivedConfiguration> emptyCalculations;
                    const auto& items = configuration == state.acquisitionConfigurations.end() ? emptyItems : configuration->second.items;
                    const auto& calculations = configuration == state.acquisitionConfigurations.end() ? emptyCalculations : configuration->second.calculations;
                    if (record.configuration_sha256().empty()) {
                        collectTelemetry(catalog, nodeId, receivedAtMs, one, batch.telemetry, items, calculations);
                        if ((record.raw_requests_size() || record.protocol() == pb::PROTOCOL_EXTERNAL_IO) && record.device_id().size() == 16 && record.record_id().size() == 16) {
                            const auto calculation = calculations.find(service::common::uuidText(record.device_id()));
                            if (calculation != calculations.end()) {
                                batch.rawCalculations[service::common::uuidText(record.record_id())] = calculation->second;
                            }
                        }
                        continue;
                    }
                    if (record.configuration_sha256().size() != 32 || record.device_id().size() != 16 || record.record_id().size() != 16) {
                        throw std::runtime_error("invalid edge acquisition snapshot identity");
                    }
                    const auto digest = service::utils::hexEncode(reinterpret_cast<const unsigned char*>(record.configuration_sha256().data()), 32);
                    const auto key = nodeId + ':' + digest;
                    auto snapshot = state.acquisitionSnapshots.find(key);
                    if (snapshot == state.acquisitionSnapshots.end()) {
                        auto loaded = co_await ConfigService::loadSnapshotByDigest(context, nodeId, digest);
                        if (!loaded) {
                            throw std::runtime_error("edge acquisition snapshot is unavailable");
                        }
                        if (state.acquisitionSnapshots.size() >= 16) {
                            state.acquisitionSnapshots.erase(state.acquisitionSnapshots.begin());
                        }
                        snapshot = state.acquisitionSnapshots.emplace(key, std::move(*loaded)).first;
                    }
                    const bool current = snapshot->second.epoch >= 0 && snapshot->second.epoch == epoch && ConfigService::snapshotMatches(snapshot->second, items, calculations);
                    if (current && record.has_device_status() && record.device_status().device_id() == record.device_id()) {
                        pb::DeviceStatusReport report;
                        *report.add_devices() = record.device_status();
                        co_await saveDeviceStatus(context, nodeId, report);
                    }
                    metadata::Catalog recordedCatalog;
                    recordedCatalog.emplace(nodeId, ConfigService::snapshotCatalog(snapshot->second));
                    const auto before = batch.telemetry.size();
                    collectTelemetry(recordedCatalog, nodeId, receivedAtMs, one, batch.telemetry, snapshot->second.items, snapshot->second.calculations);
                    if (batch.telemetry.size() != before + 1) {
                        throw std::runtime_error("snapshot does not own the reported device");
                    }
                    auto& parsed = batch.telemetry.back();
                    parsed.historyOnly = !current;
                    parsed.configurationDigest = digest;
                    parsed.configurationEpoch = snapshot->second.epoch;
                    if (record.raw_requests_size() || record.protocol() == pb::PROTOCOL_EXTERNAL_IO) {
                        const auto calculation = snapshot->second.calculations.find(parsed.deviceId);
                        if (calculation != snapshot->second.calculations.end()) {
                            auto value = calculation->second;
                            value.snapshotDigest = digest;
                            batch.rawCalculations[parsed.messageId] = std::move(value);
                        }
                    }
                }
                if (acquisitionFence) {
                    co_await acquisitionFence->commit();
                }
                break;
            }
            case pb::Envelope::kCommandResult:
                if (!envelope.command_result().node_io_channel().empty()) {
                    co_await saveIoResult(context, nodeId, envelope.command_result());
                    break;
                }
                if (envelope.command_result().device_id().size() == 16) {
                    const std::string deviceIds[]{ service::common::uuidText(envelope.command_result().device_id()) };
                    co_await ensureMetadata(context, catalog, nodeId, deviceIds);
                }
                co_await saveCommandResult(context, catalog, nodeId, receivedAtMs, envelope.command_result());
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
    static ruvia::task<void> refreshMetadata(ruvia::web_worker_context& context, ProjectionState& state, std::string_view nodeId) {
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
    static ruvia::task<void> ensureMetadata(ruvia::web_worker_context& context, metadata::Catalog& catalog, std::string_view nodeId, std::span<const std::string> deviceIds) {
        const auto containsAll = [deviceIds](const metadata::NodeSnapshot& snapshot) {
            return std::all_of(deviceIds.begin(), deviceIds.end(), [&snapshot](const std::string& id) {
                return snapshot.contains(id);
            });
        };
        const auto existingNode = catalog.find(std::string(nodeId));
        if (existingNode != catalog.end() && containsAll(existingNode->second)) {
            co_return;
        }
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

    static ruvia::task<void> saveHello(ruvia::web_worker_context& context, const pb::Hello& hello) {
        using Node = persistence::EdgeNodeEntity;
        if (!protocol::validImei(hello.imei())) {
            co_return;
        }
        const auto candidate = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
        ruvia::db_query query;
        const auto uuid = [&query](std::string_view value) {
            return query.cast(query.value(value), ruvia::db_data_type::uuid);
        };
        const auto text = [&query](const auto& value) {
            return query.cast(query.value(std::string_view(value)), ruvia::db_data_type::text);
        };
        const auto boolean = [&query](bool value) {
            return query.cast(query.value(value), ruvia::db_data_type::boolean);
        };
        const auto integer = [&query](auto value) {
            return query.cast(query.value(static_cast<std::int64_t>(value)), ruvia::db_data_type::big_int);
        };
        const auto jsonKey = [&query](std::string_view value) {
            return config::detail::jsonKey(query, value);
        };
        const auto vpn = query.call(
            "jsonb_build_object",
            { jsonKey("supportsVpn"), boolean(false), jsonKey("wireguardVersion"), text(std::string_view{}), jsonKey("agentVersion"), text(std::string_view{}), jsonKey("publicKey"), text(std::string_view{}) }
        );
        const auto capability = query.call(
            "jsonb_build_object",
            { jsonKey("nodeIoControl"), boolean(hello.supports_node_io() && hello.supports_external_io() && hello.supports_raw_telemetry()), jsonKey("networkConfig"), boolean(hello.supports_network_config()), jsonKey("firmwareUpdate"), boolean(hello.supports_firmware_update()), jsonKey("firmwareStream"), boolean(hello.supports_firmware_stream()), jsonKey("platformConfig"), boolean(hello.supports_platform_config()), jsonKey("deviceConfig"), boolean(hello.supports_device_config()), jsonKey("networkConfigVersion"), integer(hello.network_config_version()), jsonKey("modemControl"), boolean(hello.supports_modem_control()), jsonKey("commandStartBefore"), boolean(hello.supports_command_start_before()), jsonKey("supportsExternalIo"), boolean(hello.supports_external_io() && hello.supports_raw_telemetry()), jsonKey("externalIo"), query.cast(query.value(R"({"supported":false,"channels":[]})"), ruvia::db_data_type::jsonb), jsonKey("logs"), boolean(hello.supports_logs()), jsonKey("terminal"), boolean(false), jsonKey("serialDebug"), boolean(false), jsonKey("vpn"), vpn }
        );
        const auto signal = query.call(
            "jsonb_build_object",
            { jsonKey("csq"), integer(hello.signal_csq()), jsonKey("rssiDbm"), integer(hello.signal_rssi_dbm()), jsonKey("percent"), integer(hello.signal_percent()) }
        );
        const auto mobile = query.call(
            "jsonb_build_object",
            { jsonKey("available"), boolean(hello.modem_available()), jsonKey("simState"), text(simState(hello.sim_state())), jsonKey("iccid"), text(hello.iccid()), jsonKey("signal"), signal, jsonKey("registered"), boolean(hello.mobile_registered()), jsonKey("registrationStatus"), integer(hello.mobile_registration_status()), jsonKey("apn"), text(hello.apn()), jsonKey("operator"), text(hello.mobile_operator()), jsonKey("connected"), boolean(hello.mobile_connected()), jsonKey("ipv4"), text(hello.mobile_ipv4()) }
        );
        const auto config = query.call(
            "jsonb_build_object",
            { jsonKey("activeVersion"), integer(hello.last_applied_config_version()), jsonKey("desiredVersion"), integer(0), jsonKey("state"), text("idle"), jsonKey("message"), text(std::string_view{}) }
        );
        const auto outbox = query.call(
            "jsonb_build_object",
            { jsonKey("records"), integer(0), jsonKey("bytes"), integer(0) }
        );
        const auto log = query.call(
            "jsonb_build_object",
            { jsonKey("level"),
              config::detail::textDefault(
                  query,
                  text(hello.log_level()),
                  "info"
              ) }
        );
        const auto status = query.call(
            "jsonb_build_object",
            { jsonKey("config"), config, jsonKey("outbox"), outbox, jsonKey("log"), log }
        );
        const std::array<std::string_view, 13> queryColumns{ Node::column_name<"id">(), Node::column_name<"platform_id">(), Node::column_name<"imei">(), Node::column_name<"model">(), Node::column_name<"software_version">(), Node::column_name<"hostname">(), Node::column_name<"architecture">(), Node::column_name<"openwrt_release">(), Node::column_name<"capability">(), Node::column_name<"mobile">(), Node::column_name<"status">(), Node::column_name<"last_seen_at">(), Node::column_name<"updated_at">() };
        auto queryBuilder = context.db().template repository<Node>().insert_builder();
        ruvia::db_values_builder queryValues(queryColumns);
        queryValues.row({ uuid(std::string_view(candidate)), uuid(context.template worker_state<service::edge::config::PlatformIdentity>().id), query.value(std::string_view(hello.imei())), query.value(std::string_view(hello.model())), query.value(std::string_view(hello.software_version())), query.value(std::string_view(hello.hostname())), query.value(std::string_view(hello.architecture())), query.value(std::string_view(hello.openwrt_release())), capability, mobile, status, query.call("now"), query.call("now") });

        const auto existingCapability = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"capability">(), "edge_node");
        const auto existingMobile = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"mobile">(), "edge_node");
        const auto existingStatus = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), "edge_node");
        const auto excludedMobile = query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"mobile">());
        const auto apn = query.coalesce({ query.null_if(config::detail::jsonText(query, excludedMobile, "apn"), query.value(std::string_view{})), config::detail::jsonText(query, existingMobile, "apn"), query.value(std::string_view{}) });
        const auto operatorName = query.coalesce({ query.null_if(config::detail::jsonText(query, excludedMobile, "operator"), query.value(std::string_view{})), config::detail::jsonText(query, existingMobile, "operator"), query.value(std::string_view{}) });
        const auto mobileWithApn = query.call(
            "jsonb_set",
            { excludedMobile, config::detail::jsonPath(query, "{apn}"), config::detail::toJsonb(query, query.cast(apn, ruvia::db_data_type::text)), query.cast(query.value(true), ruvia::db_data_type::boolean) }
        );
        const auto mobileUpdate = query.call(
            "jsonb_set",
            { mobileWithApn, config::detail::jsonPath(query, "{operator}"), config::detail::toJsonb(query, query.cast(operatorName, ruvia::db_data_type::text)), query.cast(query.value(true), ruvia::db_data_type::boolean) }
        );
        const auto reportedCapabilities = query.binary(
            query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()),
            ruvia::db_binary_operator::json_concat,
            query.call("jsonb_build_object", { jsonKey("terminal"), config::detail::booleanText(query, config::detail::jsonText(query, existingCapability, "terminal")), jsonKey("serialDebug"), config::detail::booleanText(query, config::detail::jsonText(query, existingCapability, "serialDebug")), jsonKey("vpn"), query.coalesce({ config::detail::jsonGet(query, existingCapability, "vpn"), config::detail::jsonGet(query, query.excluded(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()), "vpn") }) })
        );
        const auto capabilityUpdate = query.binary(existingCapability, ruvia::db_binary_operator::json_concat, reportedCapabilities);
        const auto statusWithLog = query.call(
            "jsonb_set",
            { existingStatus, config::detail::jsonPath(query, "{log}"), query.coalesce({ config::detail::jsonGet(query, existingStatus, "log"), query.cast(query.value(std::string_view{ "{}" }), ruvia::db_data_type::jsonb) }), query.cast(query.value(true), ruvia::db_data_type::boolean) }
        );
        const auto statusWithLevel = query.call(
            "jsonb_set",
            { statusWithLog, config::detail::jsonPath(query, "{log,level}"), config::detail::toJsonb(query, config::detail::textDefault(query, text(hello.log_level()), "info")), query.cast(query.value(true), ruvia::db_data_type::boolean) }
        );
        const auto statusUpdate = query.call("jsonb_set", { statusWithLevel, config::detail::jsonPath(query, "{config,activeVersion}"), config::detail::toJsonb(query, integer(hello.last_applied_config_version())), boolean(true) });
        ruvia::db_upsert_options conflict;

        const std::array<ruvia::borrowed_text, 2> conflictColumns1{ { Node::column_name<"platform_id">(), Node::column_name<"imei">() } };
        conflict.conflict_paths_ = conflictColumns1;
        const auto excluded = [&query](std::string_view name) {
            return ruvia::db_assignment{ name, query.excluded(name) };
        };

        const std::array<ruvia::db_assignment, 10> conflictUpdates2{ { excluded(Node::column_name<"model">()), excluded(Node::column_name<"software_version">()), excluded(Node::column_name<"hostname">()), excluded(Node::column_name<"architecture">()), excluded(Node::column_name<"openwrt_release">()), { Node::column_name<"capability">(), capabilityUpdate }, { Node::column_name<"mobile">(), mobileUpdate }, { Node::column_name<"status">(), statusUpdate }, { Node::column_name<"last_seen_at">(), query.call("now") }, { Node::column_name<"updated_at">(), query.call("now") } } };
        conflict.update_expressions_ = conflictUpdates2;

        const std::array<ruvia::borrowed_text, 5> updatedColumns3{ { Node::column_name<"capability">(), Node::column_name<"mobile">(), Node::column_name<"status">(), Node::column_name<"last_seen_at">(), Node::column_name<"updated_at">() } };
        conflict.update_columns_ = updatedColumns3;
        queryBuilder.insert_from(queryColumns, queryValues.query()).on_conflict(conflict).returning({ { "id", query.cast(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_data_type::text) }, { "enrollment_status", query.column(service::edge::persistence::EdgeNodeEntity::column_name<"enrollment_status">()) } });
        const auto rows = co_await queryBuilder.get_raw_many();
        const auto key = service::message::edge::authKey(hello.imei());
        const auto nodeId = std::string(rows.front()[0].value().value_or(std::string_view{}));
        const auto enrollmentStatus = std::string(rows.front()[1].value().value_or(std::string_view{}));
        const auto value = nodeId + "|" + enrollmentStatus;
        co_await context.redis().set(key, value);
        if (enrollmentStatus == "approved") {
            // Configuration restoration follows the capability report so a
            // changed firmware is validated against its current protocol set.
            ruvia::db_query target;
            target
                .select({ target.alias(target.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">(), "task"), "task_id"), target.alias(target.column(service::edge::persistence::EdgeFirmwareEntity::column_name<"id">(), "firmware"), "firmware_id") })
                .from(service::edge::persistence::EdgeTaskEntity::table_name(), "task")
                .join(ruvia::db_join_type::inner, service::edge::persistence::EdgeFirmwareEntity::table_name(), target.binary(target.cast(target.column(service::edge::persistence::EdgeFirmwareEntity::column_name<"id">(), "firmware"), ruvia::db_data_type::text), ruvia::db_binary_operator::equal, config::detail::jsonText(target, target.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">(), "task"), "firmware_id")), "firmware")
                .where(target.binary(
                    target.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">(), "task"),
                    ruvia::db_binary_operator::equal,
                    target.cast(target.value(nodeId), ruvia::db_data_type::uuid)
                ))
                .and_where(target.binary(target.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">(), "task"), ruvia::db_binary_operator::equal, target.value(std::string_view{ "firmware" })))
                .and_where(target.binary(target.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), "task"), ruvia::db_binary_operator::equal, target.value(std::string_view{ "running" })))
                .and_where(target.binary(
                    config::detail::jsonText(target, target.column(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), "task"), "state"),
                    ruvia::db_binary_operator::equal,
                    target.value(std::string_view{ "flashing" })
                ))
                .order_by(target.column(service::edge::persistence::EdgeTaskEntity::column_name<"created_at">(), "task"), ruvia::db_order_direction::desc)
                .limit(1);

            ruvia::db_query completed;
            const auto rebootedResult = completed.binary(
                completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), "task"),
                ruvia::db_binary_operator::json_concat,
                completed.call(
                    "jsonb_build_object",
                    { config::detail::jsonKey(completed, "state"),
                      completed.cast(completed.value(std::string_view{ "rebooted" }), ruvia::db_data_type::text),
                      config::detail::jsonKey(completed, "message"),
                      completed.cast(completed.value(std::string_view{ "firmware reboot confirmed" }), ruvia::db_data_type::text),
                      config::detail::jsonKey(completed, "softwareVersion"),
                      completed.cast(completed.value(std::string_view(hello.software_version())), ruvia::db_data_type::text) }
                )
            );
            completed.update(service::edge::persistence::EdgeTaskEntity::table_name(), "task")
                .set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), completed.value(std::string_view{ "succeeded" }))
                .set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), rebootedResult)
                .set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), completed.call("now"))
                .set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), completed.call("now"))
                .update_from("target")
                .where(completed.binary(completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">(), "task"), ruvia::db_binary_operator::equal, completed.column("task_id", "target")))
                .returning({ completed.column("firmware_id", "target") });

            ruvia::db_query recovery;
            auto recoveryBuilder = context.db().template repository<service::edge::persistence::EdgeFirmwareEntity>().update_builder("firmware");
            recoveryBuilder.with("target", target).with("completed", completed).set(service::edge::persistence::EdgeFirmwareEntity::column_name<"version">(), recovery.cast(recovery.value(std::string_view(hello.software_version())), ruvia::db_data_type::text)).update_from_cte("completed").where(recovery.binary(recovery.column(service::edge::persistence::EdgeFirmwareEntity::column_name<"id">(), "firmware"), ruvia::db_binary_operator::equal, recovery.column("firmware_id", "completed"))).returning({ { "id", recovery.cast(recovery.column(service::edge::persistence::EdgeFirmwareEntity::column_name<"id">(), "firmware"), ruvia::db_data_type::text) } });
            const auto rebooted = co_await recoveryBuilder.get_raw_many();
            if (!rebooted.empty()) {
                scheduleFirmwareCleanup(context);
            }
        }
    }

    static ruvia::task<void> saveHeartbeat(ruvia::web_worker_context& context, std::string_view nodeId, const pb::Heartbeat& heartbeat) {
        ruvia::db_query query;
        const auto text = [&query](std::string_view value) {
            return query.cast(query.value(value), ruvia::db_data_type::text);
        };
        const auto integer = [&query](auto value) {
            return query.cast(query.value(static_cast<std::int64_t>(value)), ruvia::db_data_type::big_int);
        };
        const auto boolean = [&query](bool value) {
            return query.cast(query.value(value), ruvia::db_data_type::boolean);
        };
        const auto status = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
        const auto desiredVersion = config::detail::configVersion(
            query,
            status,
            "desiredVersion"
        );
        const auto receivedVersion = integer(heartbeat.active_config_version());
        const auto applied = query.binary(
            query.binary(desiredVersion, ruvia::db_binary_operator::equal, receivedVersion),
            ruvia::db_binary_operator::and_value,
            query.binary(receivedVersion, ruvia::db_binary_operator::greater, integer(0))
        );
        const auto config = query.call(
            "jsonb_build_object",
            { config::detail::jsonKey(query, "activeVersion"),
              receivedVersion,
              config::detail::jsonKey(query, "desiredVersion"),
              desiredVersion,
              config::detail::jsonKey(query, "state"),
              query.case_when({ { applied, text("applied") } }, config::detail::configState(query, status)),
              config::detail::jsonKey(query, "message"),
              query.case_when(
                  { { applied, text(std::string_view{}) } },
                  query.coalesce({ config::detail::jsonText(query, config::detail::jsonGet(query, status, "config"), "message"), query.value(std::string_view{}) })
              ) }
        );
        const auto signal = query.call(
            "jsonb_build_object",
            { config::detail::jsonKey(query, "csq"),
              integer(heartbeat.signal_csq()),
              config::detail::jsonKey(query, "rssiDbm"),
              integer(heartbeat.signal_rssi_dbm()),
              config::detail::jsonKey(query, "percent"),
              integer(heartbeat.signal_percent()) }
        );
        const auto existingMobile = query.column(service::edge::persistence::EdgeNodeEntity::column_name<"mobile">());
        const auto apn = query.coalesce({ query.null_if(text(heartbeat.apn()), query.value(std::string_view{})), config::detail::jsonText(query, existingMobile, "apn"), query.value(std::string_view{}) });
        const auto operatorName = query.coalesce({ query.null_if(text(heartbeat.mobile_operator()), query.value(std::string_view{})), config::detail::jsonText(query, existingMobile, "operator"), query.value(std::string_view{}) });
        const auto mobile = query.call(
            "jsonb_build_object",
            { config::detail::jsonKey(query, "available"), boolean(heartbeat.modem_available()), config::detail::jsonKey(query, "simState"), text(simState(heartbeat.sim_state())), config::detail::jsonKey(query, "iccid"), text(heartbeat.iccid()), config::detail::jsonKey(query, "signal"), signal, config::detail::jsonKey(query, "registered"), boolean(heartbeat.mobile_registered()), config::detail::jsonKey(query, "registrationStatus"), integer(heartbeat.mobile_registration_status()), config::detail::jsonKey(query, "apn"), apn, config::detail::jsonKey(query, "operator"), operatorName, config::detail::jsonKey(query, "connected"), boolean(heartbeat.mobile_connected()), config::detail::jsonKey(query, "ipv4"), text(heartbeat.mobile_ipv4()) }
        );
        const auto tcpTrafficText = [&query](std::string_view key) {
            return config::detail::jsonText(
                query,
                config::detail::jsonGet(
                    query,
                    query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                    "tcpTraffic"
                ),
                key
            );
        };
        const auto shanghaiNow = query.call("timezone", { text("Asia/Shanghai"), query.call("now") });
        const auto currentMonth = query.call("to_char", { shanghaiNow, text("YYYY-MM") });
        const auto currentDay = query.call("to_char", { shanghaiNow, text("YYYY-MM-DD") });
        const auto tcpTraffic = heartbeat.has_tcp_traffic()
            ? [&] {
                  const auto& sample = heartbeat.tcp_traffic();
                  const auto sameSample = query.binary(
                      tcpTrafficText("sampleId"),
                      ruvia::db_binary_operator::equal,
                      text(std::to_string(sample.sample_id()))
                  );
                  const auto sameMonth = query.binary(
                      tcpTrafficText("month"),
                      ruvia::db_binary_operator::equal,
                      currentMonth
                  );
                  const auto monthMissing = query.unary(
                      ruvia::db_unary_operator::is_null,
                      query.null_if(tcpTrafficText("month"), query.value(std::string_view{}))
                  );
                  const auto continueMonth = query.binary(
                      sameMonth,
                      ruvia::db_binary_operator::or_value,
                      monthMissing
                  );
                  const auto sameDay = query.binary(
                      tcpTrafficText("day"),
                      ruvia::db_binary_operator::equal,
                      currentDay
                  );
                  const auto dayMissing = query.unary(
                      ruvia::db_unary_operator::is_null,
                      query.null_if(tcpTrafficText("day"), query.value(std::string_view{}))
                  );
                  const auto continueDay = query.binary(
                      sameDay,
                      ruvia::db_binary_operator::or_value,
                      dayMissing
                  );
                  const auto periodTotal = [&](std::string_view key, const auto& continuePeriod, std::uint64_t bytes) {
                      const auto kept = query.coalesce({ query.cast(query.null_if(tcpTrafficText(key), query.value(std::string_view{})), ruvia::db_data_type::big_int), integer(0) });
                      const auto added = query.binary(kept, ruvia::db_binary_operator::add, integer(bytes));
                      return query.cast(
                          query.case_when(
                              { { sameSample, kept },
                                { continuePeriod, added } },
                              integer(bytes)
                          ),
                          ruvia::db_data_type::text
                      );
                  };
                  const auto monthValue = query.case_when(
                      { { sameSample, query.coalesce({ tcpTrafficText("month"), currentMonth }) } },
                      currentMonth
                  );
                  const auto dayValue = query.case_when(
                      { { sameSample, query.coalesce({ tcpTrafficText("day"), currentDay }) } },
                      currentDay
                  );
                  return query.call("jsonb_build_object", { config::detail::jsonKey(query, "uploadBytes"), text(std::to_string(sample.upload_bytes())), config::detail::jsonKey(query, "downloadBytes"), text(std::to_string(sample.download_bytes())), config::detail::jsonKey(query, "intervalMs"), text(std::to_string(sample.interval_ms())), config::detail::jsonKey(query, "sampleId"), text(std::to_string(sample.sample_id())), config::detail::jsonKey(query, "month"), monthValue, config::detail::jsonKey(query, "monthlyUploadBytes"), periodTotal("monthlyUploadBytes", continueMonth, sample.upload_bytes()), config::detail::jsonKey(query, "monthlyDownloadBytes"), periodTotal("monthlyDownloadBytes", continueMonth, sample.download_bytes()), config::detail::jsonKey(query, "day"), dayValue, config::detail::jsonKey(query, "dailyUploadBytes"), periodTotal("dailyUploadBytes", continueDay, sample.upload_bytes()), config::detail::jsonKey(query, "dailyDownloadBytes"), periodTotal("dailyDownloadBytes", continueDay, sample.download_bytes()) });
              }()
            : config::detail::jsonGet(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "tcpTraffic");
        const auto vpnTrafficText = [&query](std::string_view key) {
            return config::detail::jsonText(
                query,
                config::detail::jsonGet(
                    query,
                    query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()),
                    "vpnTraffic"
                ),
                key
            );
        };
        const auto vpnTraffic = heartbeat.has_vpn_traffic()
            ? [&] {
                  const auto& sample = heartbeat.vpn_traffic();
                  const auto sameSample = query.binary(
                      vpnTrafficText("sampleId"),
                      ruvia::db_binary_operator::equal,
                      text(std::to_string(sample.sample_id()))
                  );
                  const auto sameMonth = query.binary(
                      vpnTrafficText("month"),
                      ruvia::db_binary_operator::equal,
                      currentMonth
                  );
                  const auto monthMissing = query.unary(
                      ruvia::db_unary_operator::is_null,
                      query.null_if(vpnTrafficText("month"), query.value(std::string_view{}))
                  );
                  const auto continueMonth = query.binary(
                      sameMonth,
                      ruvia::db_binary_operator::or_value,
                      monthMissing
                  );
                  const auto sameDay = query.binary(
                      vpnTrafficText("day"),
                      ruvia::db_binary_operator::equal,
                      currentDay
                  );
                  const auto dayMissing = query.unary(
                      ruvia::db_unary_operator::is_null,
                      query.null_if(vpnTrafficText("day"), query.value(std::string_view{}))
                  );
                  const auto continueDay = query.binary(
                      sameDay,
                      ruvia::db_binary_operator::or_value,
                      dayMissing
                  );
                  const auto periodTotal = [&](std::string_view key, const auto& continuePeriod, std::uint64_t bytes) {
                      const auto kept = query.coalesce({ query.cast(query.null_if(vpnTrafficText(key), query.value(std::string_view{})), ruvia::db_data_type::big_int), integer(0) });
                      const auto added = query.binary(kept, ruvia::db_binary_operator::add, integer(bytes));
                      return query.cast(
                          query.case_when(
                              { { sameSample, kept },
                                { continuePeriod, added } },
                              integer(bytes)
                          ),
                          ruvia::db_data_type::text
                      );
                  };
                  const auto monthValue = query.case_when(
                      { { sameSample, query.coalesce({ vpnTrafficText("month"), currentMonth }) } },
                      currentMonth
                  );
                  const auto dayValue = query.case_when(
                      { { sameSample, query.coalesce({ vpnTrafficText("day"), currentDay }) } },
                      currentDay
                  );
                  return query.call("jsonb_build_object", { config::detail::jsonKey(query, "uploadBytes"), text(std::to_string(sample.upload_bytes())), config::detail::jsonKey(query, "downloadBytes"), text(std::to_string(sample.download_bytes())), config::detail::jsonKey(query, "intervalMs"), text(std::to_string(sample.interval_ms())), config::detail::jsonKey(query, "sampleId"), text(std::to_string(sample.sample_id())), config::detail::jsonKey(query, "month"), monthValue, config::detail::jsonKey(query, "monthlyUploadBytes"), periodTotal("monthlyUploadBytes", continueMonth, sample.upload_bytes()), config::detail::jsonKey(query, "monthlyDownloadBytes"), periodTotal("monthlyDownloadBytes", continueMonth, sample.download_bytes()), config::detail::jsonKey(query, "day"), dayValue, config::detail::jsonKey(query, "dailyUploadBytes"), periodTotal("dailyUploadBytes", continueDay, sample.upload_bytes()), config::detail::jsonKey(query, "dailyDownloadBytes"), periodTotal("dailyDownloadBytes", continueDay, sample.download_bytes()) });
              }()
            : config::detail::jsonGet(query, query.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">()), "vpnTraffic");
        auto queryBuilder = context.db().template repository<service::edge::persistence::EdgeNodeEntity>().update_builder();
        queryBuilder.set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), query.call("jsonb_build_object", { config::detail::jsonKey(query, "config"), config, config::detail::jsonKey(query, "outbox"), query.call("jsonb_build_object", { config::detail::jsonKey(query, "records"), integer(heartbeat.outbox_records()), config::detail::jsonKey(query, "bytes"), integer(heartbeat.outbox_bytes()) }), config::detail::jsonKey(query, "tcpTraffic"), tcpTraffic, config::detail::jsonKey(query, "vpnTraffic"), vpnTraffic, config::detail::jsonKey(query, "log"), query.call("jsonb_build_object", { config::detail::jsonKey(query, "level"), config::detail::textDefault(query, text(heartbeat.log_level()), "info") }) })).set(service::edge::persistence::EdgeNodeEntity::column_name<"mobile">(), mobile).set(service::edge::persistence::EdgeNodeEntity::column_name<"capability">(), query.call("jsonb_set", { query.column(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()), config::detail::jsonPath(query, "{modemControl}"), config::detail::toJsonb(query, boolean(heartbeat.supports_modem_control())), query.cast(query.value(true), ruvia::db_data_type::boolean) })).set(service::edge::persistence::EdgeNodeEntity::column_name<"last_seen_at">(), query.call("now")).set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), query.call("now")).where(query.binary(query.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, query.cast(query.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await queryBuilder.execute();
        if (heartbeat.active_config_version() != 0) {
            ruvia::db_query revision;
            auto revisionBuilder = context.db().template repository<service::edge::persistence::EdgeConfigRevisionEntity>().update_builder("revision");
            revisionBuilder.set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"status">(), revision.value(std::string_view{ "applied" })).set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"message">(), revision.value(std::string_view{})).set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"completed_at">(), revision.coalesce({ revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"completed_at">(), "revision"), revision.call("now") })).template update_from<service::edge::persistence::EdgeNodeEntity>("node").where(revision.binary(revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"node_id">(), "revision"), ruvia::db_binary_operator::equal, revision.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "node"))).and_where(revision.binary(revision.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">(), "node"), ruvia::db_binary_operator::equal, revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid))).and_where(revision.binary(revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"revision">(), "revision"), ruvia::db_binary_operator::equal, revision.cast(revision.value(static_cast<std::int64_t>(heartbeat.active_config_version())), ruvia::db_data_type::big_int))).and_where(revision.binary(config::detail::configVersion(revision, revision.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), "node"), "desiredVersion"), ruvia::db_binary_operator::equal, revision.cast(revision.value(static_cast<std::int64_t>(heartbeat.active_config_version())), ruvia::db_data_type::big_int)));
            (void)co_await revisionBuilder.execute();
        }
    }

    static std::string mac(const pb::InterfaceCapability& item) {
        if (item.mac().size() != 6) {
            return {};
        }
        constexpr char digits[] = "0123456789abcdef";
        std::string output;
        output.reserve(17);
        for (std::size_t index = 0; index < 6; ++index) {
            if (index != 0) {
                output.push_back(':');
            }
            const auto byte = static_cast<std::uint8_t>(item.mac()[index]);
            output.push_back(digits[byte >> 4U]);
            output.push_back(digits[byte & 0x0fU]);
        }
        return output;
    }

    template <typename Strings>
    static std::string jsonArray(const Strings& bridgePorts) {
        std::string output{ "[" };
        bool first = true;
        for (const auto& port : bridgePorts) {
            if (!first) {
                output.push_back(',');
            }
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
        ruvia::web_worker_context& context,
        std::size_t workerIndex,
        std::string_view nodeId,
        const pb::CapabilityReport& report
    ) {
        using Interface = persistence::EdgeNodeInterfaceEntity;
        using Network = persistence::EdgeNodeNetworkEntity;
        using Serial = persistence::EdgeNodeSerialEntity;
        ruvia::db_query deleteInterfaces;
        auto deleteInterfacesBuilder = context.db().template repository<service::edge::persistence::EdgeNodeInterfaceEntity>().delete_builder();
        deleteInterfacesBuilder.where(deleteInterfaces.binary(
            deleteInterfaces.column(service::edge::persistence::EdgeNodeInterfaceEntity::column_name<"node_id">()),
            ruvia::db_binary_operator::equal,
            deleteInterfaces.cast(deleteInterfaces.value(nodeId), ruvia::db_data_type::uuid)
        ));
        (void)co_await deleteInterfacesBuilder.execute();
        for (const auto& item : report.interfaces()) {
            const auto macAddress = mac(item);
            const auto ports = jsonArray(item.bridge_ports());
            ruvia::db_query insert;
            const std::array<std::string_view, 10> insertColumns{ Interface::column_name<"node_id">(), Interface::column_name<"name">(), Interface::column_name<"display_name">(), Interface::column_name<"mac">(), Interface::column_name<"is_up">(), Interface::column_name<"is_bridge">(), Interface::column_name<"ipv4">(), Interface::column_name<"prefix_length">(), Interface::column_name<"gateway">(), Interface::column_name<"bridge_ports">() };
            auto insertBuilder = context.db().template repository<Interface>().insert_builder();
            ruvia::db_values_builder insertValues(insertColumns);
            insertValues.row({
                insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                insert.value(std::string_view(item.name())),
                insert.value(std::string_view(item.display_name())),
                insert.null_if(insert.value(std::string_view(macAddress)), insert.value(std::string_view{})),
                insert.cast(insert.value(item.up()), ruvia::db_data_type::boolean),
                insert.cast(insert.value(item.bridge()), ruvia::db_data_type::boolean),
                insert.null_if(insert.value(std::string_view(item.ipv4())), insert.value(std::string_view{})),
                insert.value(item.prefix_length()),
                insert.null_if(insert.value(std::string_view(item.gateway())), insert.value(std::string_view{})),
                insert.cast(insert.value(std::string_view(ports)), ruvia::db_data_type::jsonb),
            });
            insertBuilder.insert_from(insertColumns, insertValues.query());
            (void)co_await insertBuilder.execute();
        }
        ruvia::db_query deleteNetworks;
        auto deleteNetworksBuilder = context.db().template repository<service::edge::persistence::EdgeNodeNetworkEntity>().delete_builder();
        deleteNetworksBuilder.where(deleteNetworks.binary(
            deleteNetworks.column(service::edge::persistence::EdgeNodeNetworkEntity::column_name<"node_id">()),
            ruvia::db_binary_operator::equal,
            deleteNetworks.cast(deleteNetworks.value(nodeId), ruvia::db_data_type::uuid)
        ));
        (void)co_await deleteNetworksBuilder.execute();
        for (const auto& item : report.networks()) {
            const auto ports = jsonArray(item.bridge_ports());
            const auto mode = addressMode(item.mode());
            ruvia::db_query insert;
            const std::array<std::string_view, 10> insertColumns{ Network::column_name<"node_id">(), Network::column_name<"name">(), Network::column_name<"address_mode">(), Network::column_name<"device">(), Network::column_name<"is_up">(), Network::column_name<"is_bridge">(), Network::column_name<"ipv4">(), Network::column_name<"prefix_length">(), Network::column_name<"gateway">(), Network::column_name<"bridge_ports">() };
            auto insertBuilder = context.db().template repository<Network>().insert_builder();
            ruvia::db_values_builder insertValues(insertColumns);
            insertValues.row({
                insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                insert.value(std::string_view(item.name())),
                insert.value(std::string_view(mode)),
                insert.value(std::string_view(item.device())),
                insert.cast(insert.value(item.up()), ruvia::db_data_type::boolean),
                insert.cast(insert.value(item.bridge()), ruvia::db_data_type::boolean),
                insert.null_if(insert.value(std::string_view(item.ipv4())), insert.value(std::string_view{})),
                insert.value(item.prefix_length()),
                insert.null_if(insert.value(std::string_view(item.gateway())), insert.value(std::string_view{})),
                insert.cast(insert.value(std::string_view(ports)), ruvia::db_data_type::jsonb),
            });
            insertBuilder.insert_from(insertColumns, insertValues.query());
            (void)co_await insertBuilder.execute();
        }
        ruvia::db_query deleteSerial;
        auto deleteSerialBuilder = context.db().template repository<service::edge::persistence::EdgeNodeSerialEntity>().delete_builder();
        deleteSerialBuilder.where(deleteSerial.binary(
            deleteSerial.column(service::edge::persistence::EdgeNodeSerialEntity::column_name<"node_id">()),
            ruvia::db_binary_operator::equal,
            deleteSerial.cast(deleteSerial.value(nodeId), ruvia::db_data_type::uuid)
        ));
        (void)co_await deleteSerialBuilder.execute();
        for (const auto& item : report.serial_ports()) {
            ruvia::db_query insert;
            const std::array<std::string_view, 5> insertColumns{ Serial::column_name<"node_id">(), Serial::column_name<"path">(), Serial::column_name<"display_name">(), Serial::column_name<"available">(), Serial::column_name<"rs485">() };
            auto insertBuilder = context.db().template repository<Serial>().insert_builder();
            ruvia::db_values_builder insertValues(insertColumns);
            insertValues.row({
                insert.cast(insert.value(nodeId), ruvia::db_data_type::uuid),
                insert.value(std::string_view(item.path())),
                insert.value(std::string_view(item.display_name())),
                insert.cast(insert.value(item.available()), ruvia::db_data_type::boolean),
                insert.cast(insert.value(item.rs485()), ruvia::db_data_type::boolean),
            });
            insertBuilder.insert_from(insertColumns, insertValues.query());
            (void)co_await insertBuilder.execute();
        }
        std::vector<std::string> supported;
        for (const auto protocol : report.supported_protocols()) {
            switch (protocol) {
                case pb::PROTOCOL_MODBUS:
                    supported.emplace_back("Modbus");
                    break;
                case pb::PROTOCOL_S7:
                    supported.emplace_back("S7");
                    break;
                case pb::PROTOCOL_SL651:
                    supported.emplace_back("SL651");
                    break;
                case pb::PROTOCOL_MC:
                    supported.emplace_back("MC");
                    break;
                case pb::PROTOCOL_FINS:
                    supported.emplace_back("FINS");
                    break;
                case pb::PROTOCOL_DLT645:
                    supported.emplace_back("DLT645");
                    break;
                case pb::PROTOCOL_EXTERNAL_IO:
                    supported.emplace_back("ExternalIO");
                    break;
                default:
                    break;
            }
        }
        const auto protocols = jsonArray(supported);
        ruvia::db_query terminal;
        auto terminalBuilder = context.db().template repository<service::edge::persistence::EdgeNodeEntity>().update_builder();
        terminalBuilder.set(service::edge::persistence::EdgeNodeEntity::column_name<"capability">(), terminal.call("jsonb_set", { terminal.call("jsonb_set", { terminal.column(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()), config::detail::jsonPath(terminal, "{protocols}"), terminal.cast(terminal.value(std::string_view(protocols)), ruvia::db_data_type::jsonb), terminal.value(true) }), config::detail::jsonPath(terminal, "{terminal}"), config::detail::toJsonb(terminal, terminal.cast(terminal.value(report.ttyd_available()), ruvia::db_data_type::boolean)), terminal.cast(terminal.value(true), ruvia::db_data_type::boolean) })).set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), terminal.call("now")).where(terminal.binary(terminal.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, terminal.cast(terminal.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await terminalBuilder.execute();
        ruvia::db_query serialCapability;
        auto serialCapabilityBuilder = context.db().template repository<persistence::EdgeNodeEntity>().update_builder();
        serialCapabilityBuilder.set(persistence::EdgeNodeEntity::column_name<"capability">(), serialCapability.call("jsonb_set", { serialCapability.column(persistence::EdgeNodeEntity::column_name<"capability">()), config::detail::jsonPath(serialCapability, "{serialDebug}"), config::detail::toJsonb(serialCapability, serialCapability.cast(serialCapability.value(report.supports_serial_debug()), ruvia::db_data_type::boolean)), serialCapability.value(true) })).where(serialCapability.binary(serialCapability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, serialCapability.cast(serialCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await serialCapabilityBuilder.execute();
        ruvia::db_query dtuCapability;
        auto dtuCapabilityBuilder = context.db().template repository<persistence::EdgeNodeEntity>().update_builder();
        dtuCapabilityBuilder.set(persistence::EdgeNodeEntity::column_name<"capability">(), dtuCapability.call("jsonb_set", { dtuCapability.column(persistence::EdgeNodeEntity::column_name<"capability">()), config::detail::jsonPath(dtuCapability, "{dtu}"), config::detail::toJsonb(dtuCapability, dtuCapability.cast(dtuCapability.value(report.supports_dtu()), ruvia::db_data_type::boolean)), dtuCapability.value(true) })).where(dtuCapability.binary(dtuCapability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, dtuCapability.cast(dtuCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await dtuCapabilityBuilder.execute();
        ruvia::db_query derivedCapability;
        auto derivedCapabilityBuilder = context.db().template repository<persistence::EdgeNodeEntity>().update_builder();
        derivedCapabilityBuilder.set(persistence::EdgeNodeEntity::column_name<"capability">(), derivedCapability.call("jsonb_set", { derivedCapability.column(persistence::EdgeNodeEntity::column_name<"capability">()), config::detail::jsonPath(derivedCapability, "{derivedPoints}"), config::detail::toJsonb(derivedCapability, derivedCapability.cast(derivedCapability.value(report.supports_derived_points()), ruvia::db_data_type::boolean)), derivedCapability.value(true) })).where(derivedCapability.binary(derivedCapability.column(persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, derivedCapability.cast(derivedCapability.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await derivedCapabilityBuilder.execute();
        ExternalIoCapability io(ruvia::model_options{ .resource_ = context.pool() });
        ruvia::array<ExternalIoChannelCapability> channels(ruvia::model_options{ .resource_ = context.pool() });
        std::set<std::string> channelNames;
        if (report.external_io_channels_size() > 15) {
            throw std::runtime_error("too many external IO channels");
        }
        for (const auto& source : report.external_io_channels()) {
            const std::string kind = source.kind() == pb::EXTERNAL_IO_DI ? "DI" : source.kind() == pb::EXTERNAL_IO_DO ? "DO"
                : source.kind() == pb::EXTERNAL_IO_AI                                                                 ? "AI"
                                                                                                                      : "";
            const std::string prefix = kind == "DI" ? "di" : kind == "DO" ? "do"
                                                                          : "AI";
            if (kind.empty() || !source.channel().starts_with(prefix) || source.channel().size() <= prefix.size() || source.channel().size() > 16 ||
                !std::all_of(source.channel().begin() + static_cast<std::ptrdiff_t>(prefix.size()), source.channel().end(), [](char value) {
                    return value >= '0' && value <= '9';
                }) ||
                !channelNames.insert(source.channel()).second || (kind != "DO" && source.controllable())) {
                throw std::runtime_error("invalid external IO capability");
            }
            auto& channel = channels.emplace_back(ruvia::model_options{ .resource_ = context.pool() });
            channel.set<"channel">(source.channel()).set<"kind">(kind).set<"available">(source.available()).set<"controllable">(source.controllable());
        }
        io.set<"supported">(report.supports_external_io()).set<"channels">(std::move(channels));
        const auto ioJson = ruvia::to_json(io);
        ruvia::db_query ioQuery;
        auto ioBuilder = context.db().template repository<persistence::EdgeNodeEntity>().update_builder();
        const auto capabilityColumn = persistence::EdgeNodeEntity::column<"capability">().expression(ioQuery);
        const auto negotiated = ioQuery.binary(config::detail::jsonText(ioQuery, capabilityColumn, "supportsExternalIo"), ruvia::db_binary_operator::equal, ioQuery.value("true"));
        const auto ioValue = ioQuery.case_when({ { negotiated, ioQuery.cast(ioQuery.value(std::string_view(ioJson.data(), ioJson.size())), ruvia::db_data_type::jsonb) } }, ioQuery.cast(ioQuery.value("{\"supported\":false,\"channels\":[]}"), ruvia::db_data_type::jsonb));
        ioBuilder.set(persistence::EdgeNodeEntity::column_name<"capability">(), ioQuery.call("jsonb_set", { capabilityColumn, config::detail::jsonPath(ioQuery, "{externalIo}"), ioValue, ioQuery.value(true) }))
            .where((persistence::EdgeNodeEntity::column<"id">() == nodeId).expression(ioQuery));
        (void)co_await ioBuilder.execute();
        (void)co_await ConfigService::requeueIfStale(context, nodeId);
        if (report.has_vpn()) {
            const auto vpnPublicKey = service::common::wireguard::validManagedKey(report.vpn().public_key())
                ? std::string(report.vpn().public_key())
                : std::string{};
            ruvia::db_query vpn;
            auto vpnBuilder = context.db().template repository<service::edge::persistence::EdgeNodeEntity>().update_builder();
            vpnBuilder.set(service::edge::persistence::EdgeNodeEntity::column_name<"capability">(), vpn.call("jsonb_set", { vpn.column(service::edge::persistence::EdgeNodeEntity::column_name<"capability">()), config::detail::jsonPath(vpn, "{vpn}"), vpn.call("jsonb_build_object", { config::detail::jsonKey(vpn, "supportsVpn"), vpn.cast(vpn.value(report.vpn().supports_vpn()), ruvia::db_data_type::boolean), config::detail::jsonKey(vpn, "wireguardVersion"), vpn.cast(vpn.value(std::string_view(report.vpn().wireguard_version())), ruvia::db_data_type::text), config::detail::jsonKey(vpn, "agentVersion"), vpn.cast(vpn.value(std::string_view(report.vpn().agent_version())), ruvia::db_data_type::text), config::detail::jsonKey(vpn, "publicKey"), vpn.cast(vpn.value(std::string_view(vpnPublicKey)), ruvia::db_data_type::text) }), vpn.cast(vpn.value(true), ruvia::db_data_type::boolean) })).set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), vpn.call("now")).where(vpn.binary(vpn.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, vpn.cast(vpn.value(nodeId), ruvia::db_data_type::uuid)));
            (void)co_await vpnBuilder.execute();
            // vpn_peer activation, bridge route allocation and Hub reconciliation
            // belong to the VPN feature; it receives the reported key as an event.
            if (report.vpn().supports_vpn() && !vpnPublicKey.empty()) {
                service::message::vpn::EdgeCapability capability(ruvia::model_options{ .resource_ = context.pool() });
                capability.set<"nodeId">(nodeId).set<"publicKey">(vpnPublicKey);
                co_await notifyFeature(context, workerIndex, service::message::vpn::kRpcComponent, service::message::vpn::kEdgeCapabilityOperation, service::message::rpc::encode(capability, context.pool()));
            }
        }
    }

    static ruvia::task<void> saveNetworkResult(
        ruvia::web_worker_context& context,
        std::string_view nodeId,
        const pb::NetworkConfigResult& result
    ) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        const auto id = service::common::uuidText(result.request_id());
        const std::string status = result.success() ? "succeeded" : "failed";
        const std::string json = "{\"message\":" + service::utils::jsonQuoted(result.message()) +
            ",\"rolled_back\":" +
            (result.rolled_back() ? "true" : "false") + "}";
        ruvia::db_query query;
        auto queryBuilder = context.db().template repository<service::edge::persistence::EdgeTaskEntity>().update_builder();
        queryBuilder.set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), query.value(std::string_view(status))).set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), query.cast(query.value(std::string_view(json)), ruvia::db_data_type::jsonb)).set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), query.call("now")).set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), query.call("now")).where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal, query.cast(query.value(id), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, query.cast(query.value(nodeId), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal, query.value(std::string_view{ "network" }))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in, query.list({ query.value(std::string_view{ "succeeded" }), query.value(std::string_view{ "failed" }) })));
        (void)co_await queryBuilder.execute();
    }

    // Completes the VPN edge_task; the VPN feature projects the result onto vpn_route.
    static ruvia::task<void> saveVpnResult(
        ruvia::web_worker_context& context,
        std::size_t workerIndex,
        std::string_view nodeId,
        const pb::VpnConfigResult& result
    ) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        const auto id = service::common::uuidText(result.request_id());
        const bool applied = result.applied();
        const std::string status = applied ? "succeeded" : "failed";
        const std::string json = "{\"configVersion\":" +
            std::to_string(result.config_version()) +
            ",\"errorCode\":" + service::utils::jsonQuoted(result.error_code()) +
            ",\"errorMessage\":" + service::utils::jsonQuoted(result.error_message()) + "}";
        ruvia::db_query transitioned;
        auto transitionedBuilder = context.db().template repository<service::edge::persistence::EdgeTaskEntity>().update_builder();
        transitionedBuilder.set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), transitioned.value(std::string_view(status))).set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), transitioned.cast(transitioned.value(std::string_view(json)), ruvia::db_data_type::jsonb)).set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), transitioned.call("now")).set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), transitioned.call("now")).where(transitioned.binary(transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal, transitioned.cast(transitioned.value(id), ruvia::db_data_type::uuid))).and_where(transitioned.binary(transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, transitioned.cast(transitioned.value(nodeId), ruvia::db_data_type::uuid))).and_where(transitioned.binary(transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal, transitioned.value(std::string_view{ "vpn" }))).and_where(transitioned.binary(transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in, transitioned.list({ transitioned.value(std::string_view{ "succeeded" }), transitioned.value(std::string_view{ "failed" }) })));
        (void)co_await transitionedBuilder.execute();
        // The event is derived from the persisted terminal state, not from whether
        // this call moved the task: when sending fails the batch stays unacknowledged
        // and its retry finds the task already moved, yet still delivers the event.
        // The VPN feature applies a configuration result idempotently per revision.
        ruvia::db_query completed;
        auto completedBuilder = context.db().template repository<service::edge::persistence::EdgeTaskEntity>().query_builder();
        completedBuilder.select({ { "request", config::detail::jsonText(completed, completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">()), "peerId") }, { "value1", config::detail::jsonText(completed, completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">()), "enabled") }, { "value2", config::detail::jsonText(completed, completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">()), "configVersion") } }).where(completed.binary(completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal, completed.cast(completed.value(id), ruvia::db_data_type::uuid))).and_where(completed.binary(completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, completed.cast(completed.value(nodeId), ruvia::db_data_type::uuid))).and_where(completed.binary(completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal, completed.value(std::string_view{ "vpn" }))).and_where(completed.binary(completed.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::equal, completed.value(std::string_view(status))));
        const auto rows = co_await completedBuilder.get_raw_many();
        for (const auto& row : rows) {
            const auto configVersion = service::utils::parseInt64(row[2].value());
            if (!configVersion) {
                continue;
            }
            service::message::vpn::EdgeConfigResult event(ruvia::model_options{ .resource_ = context.pool() });
            event.set<"peerId">(row[0].value().value_or(std::string_view{}))
                .set<"configVersion">(*configVersion)
                .set<"enabled">(row[1].value().value_or(std::string_view{ "true" }) != "false")
                .set<"applied">(applied)
                .set<"errorMessage">(std::string_view(result.error_message()));
            co_await notifyFeature(context, workerIndex, service::message::vpn::kRpcComponent, service::message::vpn::kEdgeConfigResultOperation, service::message::rpc::encode(event, context.pool()));
        }
    }

    static ruvia::task<void> saveFirmwareResult(
        ruvia::web_worker_context& context,
        std::string_view nodeId,
        const pb::FirmwareUpdateResult& result
    ) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        const auto id = service::common::uuidText(result.request_id());
        std::string status = "running";
        bool completed = false;
        if (result.state() == pb::FIRMWARE_UPDATE_ACCEPTED) {
            status = "accepted";
        } else if (result.state() == pb::FIRMWARE_UPDATE_FAILED) {
            status = "failed";
            completed = true;
        }
        std::string state = "running";
        if (result.state() == pb::FIRMWARE_UPDATE_ACCEPTED) {
            state = "accepted";
        } else if (result.state() == pb::FIRMWARE_UPDATE_DOWNLOADING) {
            state = "downloading";
        } else if (result.state() == pb::FIRMWARE_UPDATE_VERIFYING) {
            state = "verifying";
        } else if (result.state() == pb::FIRMWARE_UPDATE_FLASHING) {
            state = "flashing";
        } else if (result.state() == pb::FIRMWARE_UPDATE_FAILED) {
            state = "failed";
        }
        const std::string json =
            "{\"state\":\"" + state + "\",\"message\":\"" +
            service::utils::jsonEscape(result.message()) +
            "\",\"progressPercent\":" + std::to_string(result.progress_percent()) +
            ",\"downloadedBytes\":" + std::to_string(result.downloaded_bytes()) +
            ",\"totalBytes\":" + std::to_string(result.total_bytes()) + "}";
        ruvia::db_query query;
        auto queryBuilder = context.db().template repository<service::edge::persistence::EdgeTaskEntity>().update_builder();
        queryBuilder.set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), query.value(std::string_view(status))).set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), query.cast(query.value(std::string_view(json)), ruvia::db_data_type::jsonb)).set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), query.call("now")).set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), query.case_when({ { query.cast(query.value(completed), ruvia::db_data_type::boolean), query.call("now") } }, query.null_value())).where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal, query.cast(query.value(id), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, query.cast(query.value(nodeId), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal, query.value(std::string_view{ "firmware" }))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in, query.list({ query.value(std::string_view{ "succeeded" }), query.value(std::string_view{ "failed" }) })));
        const auto updated = co_await queryBuilder.execute();
        if (completed && updated.affected_rows() != 0) {
            scheduleFirmwareCleanup(context);
        }
    }

    static ruvia::task<void> saveModemResult(
        ruvia::web_worker_context& context,
        std::string_view nodeId,
        const pb::ModemControlResult& result
    ) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        const auto id = service::common::uuidText(result.request_id());
        std::string status = "running";
        bool completed = false;
        if (result.state() == pb::MODEM_CONTROL_ACCEPTED) {
            status = "accepted";
        } else if (result.state() == pb::MODEM_CONTROL_SUCCEEDED) {
            status = "succeeded";
            completed = true;
        } else if (result.state() == pb::MODEM_CONTROL_FAILED) {
            status = "failed";
            completed = true;
        }
        const std::string json = "{\"message\":" + service::utils::jsonQuoted(result.message()) +
            ",\"apn\":" + service::utils::jsonQuoted(result.apn()) + "}";
        ruvia::db_query query;
        auto queryBuilder = context.db().template repository<service::edge::persistence::EdgeTaskEntity>().update_builder();
        queryBuilder.set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), query.value(std::string_view(status))).set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), query.cast(query.value(std::string_view(json)), ruvia::db_data_type::jsonb)).set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), query.call("now")).set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), query.case_when({ { query.cast(query.value(completed), ruvia::db_data_type::boolean), query.call("now") } }, query.null_value())).where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()), ruvia::db_binary_operator::equal, query.cast(query.value(id), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, query.cast(query.value(nodeId), ruvia::db_data_type::uuid))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()), ruvia::db_binary_operator::equal, query.value(std::string_view{ "modem" }))).and_where(query.binary(query.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()), ruvia::db_binary_operator::not_in, query.list({ query.value(std::string_view{ "succeeded" }), query.value(std::string_view{ "failed" }) })));
        (void)co_await queryBuilder.execute();
    }

    static ruvia::task<void> savePlatformResult(
        ruvia::web_worker_context& context,
        std::string_view nodeId,
        const pb::PlatformConfigResult& result
    ) {
        if (result.request_id().size() != 16) {
            co_return;
        }
        const auto id = service::common::uuidText(result.request_id());
        const std::string status = result.success() ? "succeeded" : "failed";
        const std::string json = "{\"message\":" + service::utils::jsonQuoted(result.message()) + "}";
        ruvia::db_query transitioned;
        transitioned.update(service::edge::persistence::EdgeTaskEntity::table_name())
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"status">(), transitioned.value(std::string_view(status)))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"result">(), transitioned.cast(transitioned.value(std::string_view(json)), ruvia::db_data_type::jsonb))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"updated_at">(), transitioned.call("now"))
            .set(service::edge::persistence::EdgeTaskEntity::column_name<"completed_at">(), transitioned.call("now"))
            .where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"id">()),
                ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(id), ruvia::db_data_type::uuid)
            ))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"node_id">()),
                ruvia::db_binary_operator::equal,
                transitioned.cast(transitioned.value(nodeId), ruvia::db_data_type::uuid)
            ))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()),
                ruvia::db_binary_operator::in,
                transitioned.list({ transitioned.value(std::string_view{ "platform_upsert" }), transitioned.value(std::string_view{ "platform_delete" }) })
            ))
            .and_where(transitioned.binary(
                transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"status">()),
                ruvia::db_binary_operator::not_in,
                transitioned.list({ transitioned.value(std::string_view{ "succeeded" }), transitioned.value(std::string_view{ "failed" }) })
            ))
            .returning({ transitioned.alias(transitioned.cast(config::detail::jsonText(transitioned, transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"request">()), "platform_id"), ruvia::db_data_type::uuid), "platform_id"), transitioned.column(service::edge::persistence::EdgeTaskEntity::column_name<"task_type">()) });

        ruvia::db_query updated;
        const auto success = updated.cast(updated.value(result.success()), ruvia::db_data_type::boolean);
        const auto deleteTask = updated.binary(
            updated.column("task_type", "task"),
            ruvia::db_binary_operator::equal,
            updated.value(std::string_view{ "platform_delete" })
        );
        updated.update(service::edge::persistence::EdgeNodePlatformEntity::table_name(), "target")
            .set(service::edge::persistence::EdgeNodePlatformEntity::column_name<"status">(), updated.call("jsonb_build_object", { config::detail::jsonKey(updated, "state"), updated.cast(updated.value(std::string_view(result.success() ? "applied" : "failed")), ruvia::db_data_type::text), config::detail::jsonKey(updated, "message"), updated.cast(updated.value(std::string_view(result.message())), ruvia::db_data_type::text) }))
            .set(service::edge::persistence::EdgeNodePlatformEntity::column_name<"updated_at">(), updated.call("now"))
            .update_from("transitioned", "task")
            .where(updated.binary(
                updated.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"node_id">(), "target"),
                ruvia::db_binary_operator::equal,
                updated.cast(updated.value(nodeId), ruvia::db_data_type::uuid)
            ))
            .and_where(updated.binary(updated.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"platform_id">(), "target"), ruvia::db_binary_operator::equal, updated.column("platform_id", "task")))
            .and_where(updated.unary(
                ruvia::db_unary_operator::not_value,
                updated.binary(success, ruvia::db_binary_operator::and_value, deleteTask)
            ))
            .returning({ updated.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"platform_id">(), "target") });

        ruvia::db_query cleanup;
        const auto cleanupSuccess = cleanup.import_expression(success);
        const auto cleanupDeleteTask = cleanup.import_expression(deleteTask);
        auto cleanupBuilder = context.db().template repository<service::edge::persistence::EdgeNodePlatformEntity>().delete_builder("target");
        cleanupBuilder.with("transitioned", transitioned).with("updated", updated).delete_using_cte("transitioned", "task").where(cleanup.binary(cleanupSuccess, ruvia::db_binary_operator::and_value, cleanupDeleteTask)).and_where(cleanup.binary(cleanup.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"node_id">(), "target"), ruvia::db_binary_operator::equal, cleanup.cast(cleanup.value(nodeId), ruvia::db_data_type::uuid))).and_where(cleanup.binary(cleanup.column(service::edge::persistence::EdgeNodePlatformEntity::column_name<"platform_id">(), "target"), ruvia::db_binary_operator::equal, cleanup.column("platform_id", "task")));
        (void)co_await cleanupBuilder.execute();
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

    static ruvia::task<void> saveConfigApplied(ruvia::web_worker_context& context, std::string_view nodeId, const pb::ConfigApplied& result) {
        if (result.revision() == 0 || result.sha256().size() != 32) {
            co_return;
        }
        const auto digest = hex(result.sha256());
        ruvia::db_query revision;
        auto revisionBuilder = context.db().template repository<service::edge::persistence::EdgeConfigRevisionEntity>().update_builder();
        revisionBuilder.set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"status">(), revision.value(std::string_view{ "applied" })).set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"message">(), revision.value(std::string_view{})).set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"completed_at">(), revision.call("now")).where(revision.binary(revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid))).and_where(revision.binary(revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"revision">()), ruvia::db_binary_operator::equal, revision.cast(revision.value(static_cast<std::int64_t>(result.revision())), ruvia::db_data_type::big_int))).and_where(revision.binary(revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"sha256">()), ruvia::db_binary_operator::equal, revision.value(std::string_view(digest))));
        (void)co_await revisionBuilder.execute();

        ruvia::db_query node;
        const auto status = node.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
        const auto revisionValue = node.cast(
            node.value(static_cast<std::int64_t>(result.revision())),
            ruvia::db_data_type::big_int
        );
        const auto desired = config::detail::configVersion(node, status, "desiredVersion");
        const auto matches = node.binary(desired, ruvia::db_binary_operator::equal, revisionValue);
        const auto configWithActive = node.call(
            "jsonb_set",
            { status, config::detail::jsonPath(node, "{config,activeVersion}"), config::detail::toJsonb(node, node.greatest({ config::detail::configVersion(node, status, "activeVersion"), revisionValue })), node.cast(node.value(true), ruvia::db_data_type::boolean) }
        );
        const auto configWithState = node.call(
            "jsonb_set",
            { configWithActive, config::detail::jsonPath(node, "{config,state}"), config::detail::toJsonb(node, node.case_when({ { matches, node.cast(node.value(std::string_view{ "applied" }), ruvia::db_data_type::text) } }, config::detail::configState(node, status))), node.cast(node.value(true), ruvia::db_data_type::boolean) }
        );
        const auto configWithMessage = node.call(
            "jsonb_set",
            { configWithState, config::detail::jsonPath(node, "{config,message}"), config::detail::toJsonb(node, node.case_when({ { matches, node.cast(node.value(std::string_view{}), ruvia::db_data_type::text) } }, node.coalesce({ config::detail::jsonText(node, config::detail::jsonGet(node, status, "config"), "message"), node.value(std::string_view{}) }))), node.cast(node.value(true), ruvia::db_data_type::boolean) }
        );
        auto nodeBuilder = context.db().template repository<service::edge::persistence::EdgeNodeEntity>().update_builder();
        nodeBuilder.set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), configWithMessage).set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), node.call("now")).where(node.binary(node.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, node.cast(node.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await nodeBuilder.execute();
    }

    static ruvia::task<void> saveConfigRejected(ruvia::web_worker_context& context, std::string_view nodeId, const pb::ConfigRejected& result) {
        if (result.revision() == 0) {
            co_return;
        }
        const std::string message = result.code() + ": " + result.message();
        ruvia::db_query revision;
        auto revisionBuilder = context.db().template repository<service::edge::persistence::EdgeConfigRevisionEntity>().update_builder();
        revisionBuilder.set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"status">(), revision.value(std::string_view{ "rejected" })).set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"message">(), revision.value(std::string_view(message))).set(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"completed_at">(), revision.call("now")).where(revision.binary(revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"node_id">()), ruvia::db_binary_operator::equal, revision.cast(revision.value(nodeId), ruvia::db_data_type::uuid))).and_where(revision.binary(revision.column(service::edge::persistence::EdgeConfigRevisionEntity::column_name<"revision">()), ruvia::db_binary_operator::equal, revision.cast(revision.value(static_cast<std::int64_t>(result.revision())), ruvia::db_data_type::big_int)));
        (void)co_await revisionBuilder.execute();

        ruvia::db_query node;
        const auto status = node.column(service::edge::persistence::EdgeNodeEntity::column_name<"status">());
        const auto revisionValue = node.cast(
            node.value(static_cast<std::int64_t>(result.revision())),
            ruvia::db_data_type::big_int
        );
        const auto matches = node.binary(
            config::detail::configVersion(node, status, "desiredVersion"),
            ruvia::db_binary_operator::equal,
            revisionValue
        );
        const auto state = node.case_when(
            { { matches, node.cast(node.value(std::string_view{ "rejected" }), ruvia::db_data_type::text) } },
            config::detail::configState(node, status)
        );
        const auto messageValue = node.case_when(
            { { matches, node.cast(node.value(std::string_view(message)), ruvia::db_data_type::text) } },
            node.coalesce({ config::detail::jsonText(node, config::detail::jsonGet(node, status, "config"), "message"), node.value(std::string_view{}) })
        );
        const auto statusWithState = node.call(
            "jsonb_set",
            { status, config::detail::jsonPath(node, "{config,state}"), config::detail::toJsonb(node, state), node.cast(node.value(true), ruvia::db_data_type::boolean) }
        );
        auto nodeBuilder = context.db().template repository<service::edge::persistence::EdgeNodeEntity>().update_builder();
        nodeBuilder.set(service::edge::persistence::EdgeNodeEntity::column_name<"status">(), node.call("jsonb_set", { statusWithState, config::detail::jsonPath(node, "{config,message}"), config::detail::toJsonb(node, messageValue), node.cast(node.value(true), ruvia::db_data_type::boolean) })).set(service::edge::persistence::EdgeNodeEntity::column_name<"updated_at">(), node.call("now")).where(node.binary(node.column(service::edge::persistence::EdgeNodeEntity::column_name<"id">()), ruvia::db_binary_operator::equal, node.cast(node.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await nodeBuilder.execute();
    }

  public:
    static pb::TelemetryRecord assembleTelemetryParts(const std::vector<pb::TelemetryRecord>& parts) {
        if (parts.empty() || parts.size() > 256) {
            throw std::runtime_error("invalid telemetry upload part count");
        }
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
                part.direction() != result.direction() || part.model_id() != result.model_id() ||
                part.configuration_sha256() != result.configuration_sha256()) {
                throw std::runtime_error("inconsistent telemetry upload parts");
            }
            for (const auto& value : part.values()) {
                if (!elements.insert(value.element_id()).second) {
                    throw std::runtime_error("duplicate element in telemetry upload");
                }
                *result.add_values() = value;
            }
            for (const auto& raw : part.raw_payloads()) {
                rawBytes += raw.size();
                if (raw.empty() || raw.size() > 4112 || rawBytes > 147456 ||
                    result.raw_payloads_size() >= 4095) {
                    throw std::runtime_error("invalid telemetry original frame array");
                }
                result.add_raw_payloads(raw);
            }
            if ((parts.front().raw_requests_size() != 0) != (part.raw_requests_size() != 0) ||
                (part.raw_requests_size() != 0 &&
                 (part.raw_requests_size() != part.raw_payloads_size() || part.values_size() != 0))) {
                throw std::runtime_error("inconsistent telemetry business exchange parts");
            }
            for (const auto& request : part.raw_requests()) {
                rawBytes += request.size();
                if (request.size() > 4096 || rawBytes > 147456) {
                    throw std::runtime_error("invalid telemetry business request");
                }
                result.add_raw_requests(request);
            }
            if (part.raw_packet_ids_size() != 0 && part.raw_packet_ids_size() != part.raw_payloads_size()) {
                throw std::runtime_error("original packet ID count mismatch");
            }
            for (const auto& id : part.raw_packet_ids()) {
                if (id.size() != 16) {
                    throw std::runtime_error("invalid original packet ID");
                }
                result.add_raw_packet_ids(id);
            }
        }
        if (result.raw_payloads().empty()) {
            throw std::runtime_error("telemetry upload has no original frames");
        }
        result.clear_report_id();
        result.clear_part_count();
        result.clear_part_index();
        return result;
    }

    template <typename Redis>
    static ruvia::task<std::optional<pb::TelemetryRecord>> storeTelemetryPart(
        const Redis& redis,
        std::string_view nodeId,
        const pb::TelemetryRecord& record
    ) {
        if (record.report_id().size() != 16 || record.record_id().size() != 16 ||
            record.device_id().size() != 16 || record.part_count() == 0 ||
            record.part_count() > 256 || record.part_index() >= record.part_count() ||
            (record.protocol() != pb::PROTOCOL_SL651 && record.protocol() != pb::PROTOCOL_MODBUS &&
             record.protocol() != pb::PROTOCOL_S7 && record.protocol() != pb::PROTOCOL_MC &&
             record.protocol() != pb::PROTOCOL_FINS && record.protocol() != pb::PROTOCOL_DLT645 && record.protocol() != pb::PROTOCOL_EXTERNAL_IO) ||
            record.ByteSizeLong() > 14000) {
            throw std::runtime_error("invalid telemetry upload part");
        }
        const persistence::TelemetryUploadRecord stored{ std::string(nodeId),
                                                         service::common::uuidText(record.device_id()),
                                                         service::common::uuidText(record.report_id()) };
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
        const std::string_view keys[]{ key };
        const std::string_view args[]{ signature, index, count, wire };
        const auto reply = co_await redis.eval(persistence::kStoreTelemetryPartScript, keys, args);
        if (reply.kind() == ruvia::redis_value::kind_type::integer && reply.integer() == 0) {
            co_return std::nullopt;
        }
        if (reply.kind() != ruvia::redis_value::kind_type::array) {
            service::utils::redis::throwValue("store telemetry upload", reply);
        }
        std::vector<pb::TelemetryRecord> parts;
        for (const auto& item : reply.array()) {
            if (item.kind() != ruvia::redis_value::kind_type::string) {
                throw std::runtime_error("invalid stored telemetry part");
            }
            auto& part = parts.emplace_back();
            if (!part.ParseFromString(std::string(item.string()))) {
                throw std::runtime_error("corrupt stored telemetry part");
            }
        }
        co_return assembleTelemetryParts(parts);
    }

    template <typename Redis>
    static ruvia::task<void> finishTelemetryUploads(
        const Redis& redis,
        const std::vector<persistence::TelemetryUploadRecord>& uploads
    ) {
        for (const auto& upload : uploads) {
            const auto key = upload.key();
            const std::string_view keys[]{ key };
            const auto reply = co_await redis.eval(persistence::kFinishTelemetryUploadScript, keys, std::span<const std::string_view>{});
            if (reply.kind() != ruvia::redis_value::kind_type::integer || reply.integer() != 1) {
                service::utils::redis::throwValue("finish telemetry upload", reply);
            }
        }
    }

    static void collectTelemetry(const metadata::Catalog& catalog, std::string_view nodeId, std::int64_t receivedAtMs, const pb::TelemetryBatch& batch, std::vector<service::message::ParsedDeviceMessage>& messages, const std::vector<pb::ConfigItem>& parsingItems = {}, const std::map<std::string, service::configuration::DerivedConfiguration>& calculations = {}) {
        const auto node = catalog.find(std::string(nodeId));
        if (node == catalog.end()) {
            return;
        }
        for (const auto& record : batch.records()) {
            if (record.record_id().size() != 16 || record.device_id().size() != 16) {
                continue;
            }
            const auto deviceId = service::common::uuidText(record.device_id());
            const auto device = node->second.find(deviceId);
            if (device == node->second.end()) {
                continue;
            }
            message::ParsedDeviceMessage parsed;
            if (record.model_id().size() == 16) {
                parsed.modelId = service::common::uuidText(record.model_id());
            } else if (!record.model_id().empty()) {
                throw std::runtime_error("invalid edge telemetry model reference");
            }
            parsed.messageId = service::common::uuidText(record.record_id());
            parsed.acquisitionId = parsed.messageId;
            parsed.causationId = parsed.messageId;
            parsed.linkId = device->second.linkId;
            parsed.deviceId = deviceId;
            parsed.deviceCode = device->second.deviceCode;
            parsed.protocol = protocol::TelemetryValues::protocolName(record.protocol());
            if (parsed.protocol.empty()) {
                parsed.protocol = device->second.protocol;
            }
            parsed.connectionId = std::string(nodeId);
            parsed.occurredAtMs = receivedAtMs;
            parsed.observedAtMs = record.observed_at_ms();
            parsed.storagePolicy = device->second.storagePolicy;
            parsed.onlineWindowMs = device->second.onlineWindowMs;
            parsed.source = record.derived_update() ? "derived" : "edge";
            if (record.protocol() == pb::PROTOCOL_EXTERNAL_IO) {
                if (record.values_size() > 15 || record.raw_requests_size() || record.raw_payloads_size() || record.derived_update() || record.model_id().size() != 16 || record.observed_at_ms() <= 0) {
                    throw std::runtime_error("invalid external IO telemetry");
                }
                const auto calculation = calculations.find(deviceId);
                const std::size_t expectedSources = device->second.protocol == "ExternalIO" ? 1 : 2;
                if (calculation == calculations.end() || calculation->second.sources.size() != expectedSources) {
                    throw std::runtime_error("external IO configuration is unavailable");
                }
                const auto ioConfig = ruvia::json_value::parse(calculation->second.sources.back());
                const auto policy = ioConfig.get<ruvia::string>("storagePolicy");
                if (!policy || (policy->view() != "report" && policy->view() != "change")) {
                    throw std::runtime_error("invalid external IO storage policy");
                }
                parsed.storagePolicy = policy->view();
                auto verified = record;
                std::set<std::string> seen;
                for (auto& point : *verified.mutable_values()) {
                    const auto binding = std::find_if(parsingItems.begin(), parsingItems.end(), [&](const pb::ConfigItem& item) {
                        return item.has_external_io() && item.external_io().device_id() == record.device_id() && item.external_io().model_id() == record.model_id() && item.external_io().point_id() == point.element_id();
                    });
                    if (binding == parsingItems.end() || !seen.insert(point.element_id()).second || point.derived()) {
                        throw std::runtime_error("external IO point is not bound to this device and node");
                    }
                    const auto& config = binding->external_io();
                    if (point.has_value() && ((config.kind() == pb::EXTERNAL_IO_AI && point.value().value_case() != pb::ScalarValue::kSignedValue) || (config.kind() != pb::EXTERNAL_IO_AI && point.value().value_case() != pb::ScalarValue::kBoolValue))) {
                        throw std::runtime_error("external IO scalar type mismatch");
                    }
                    point.set_name(config.name());
                    point.set_unit(config.unit());
                    point.set_hidden(config.hidden());
                }
                parsed.valuesJson = protocol::TelemetryValues::telemetryJson(verified);
            } else if (record.raw_requests_size() != 0) {
                const auto configured = acquisition::RawAcquisition::device(parsingItems, record.device_id());
                if (configured.linkId != device->second.linkId ||
                    (record.endpoint_id().size() == 16 && service::common::uuidText(record.endpoint_id()) != configured.linkId)) {
                    throw std::runtime_error("edge acquisition endpoint mismatch");
                }
                const auto decoded = acquisition::RawAcquisition::decode(record, configured);
                parsed.valuesJson = decoded.valuesJson;
                parsed.observedAtMs = decoded.observedAtMs;
            } else {
                parsed.valuesJson = protocol::TelemetryValues::telemetryJson(record);
            }
            if (record.raw_packet_ids_size() != 0 &&
                record.raw_packet_ids_size() != record.raw_payloads_size()) {
                throw std::runtime_error("original packet ID count mismatch");
            }
            for (int index = 0; index < record.raw_payloads_size(); ++index) {
                std::string packetId;
                if (record.raw_packet_ids_size()) {
                    const auto& id = record.raw_packet_ids(index);
                    if (id.size() != 16) {
                        throw std::runtime_error("invalid original packet ID");
                    }
                    packetId = std::string(nodeId) + ':' + service::common::uuidText(id);
                } else {
                    packetId = std::string(nodeId) + ":legacy-history:" + parsed.messageId + ':' + std::to_string(index);
                }
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
    static ruvia::task<void> saveIoState(ruvia::web_worker_context& context, std::string_view nodeId, std::uint64_t epoch, std::uint64_t sequence, const pb::ExternalIoStatus& status) {
        if (status.channels_size() > 15 || status.observed_at_ms() <= 0) {
            throw std::runtime_error("invalid IO status");
        }
        const auto lease = co_await context.redis().get(service::message::edge::sessionKey(nodeId));
        const auto session = lease ? session_state::parse(*lease) : std::nullopt;
        if (!session || session->epoch != epoch) {
            co_return;
        }
        service::message::edge::IoSnapshot snapshot(ruvia::model_options{ .resource_ = context.pool() });
        ruvia::array<service::message::edge::IoChannelState> channels(ruvia::model_options{ .resource_ = context.pool() });
        std::set<std::string> names;
        for (const auto& channel : status.channels()) {
            const auto kind = channel.kind() == pb::EXTERNAL_IO_DI ? "DI" : channel.kind() == pb::EXTERNAL_IO_DO ? "DO"
                : channel.kind() == pb::EXTERNAL_IO_AI                                                           ? "AI"
                                                                                                                 : "";
            if (!*kind || channel.channel().empty() || channel.channel().size() > 16 || !names.insert(channel.channel()).second) {
                throw std::runtime_error("invalid IO channel");
            }
            auto& row = channels.emplace_back(ruvia::model_options{ .resource_ = context.pool() });
            row.set<"channel">(channel.channel()).set<"kind">(kind).set<"available">(channel.available()).set<"controllable">(channel.controllable() && channel.kind() == pb::EXTERNAL_IO_DO);
            if (channel.available() && channel.has_value()) {
                row.set<"value">(std::to_string(channel.value()));
            }
        }
        snapshot.set<"channels">(std::move(channels));
        const auto json = ruvia::to_json(snapshot);
        const auto key = service::message::edge::ioStateKey(nodeId);
        const auto leaseKey = service::message::edge::sessionKey(nodeId);
        const auto topic = service::message::live::channel(context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, service::message::live::topic::kEdgeIo);
        const auto timestamp = std::to_string(status.observed_at_ms());
        persistence::IoStateEntity stored(ruvia::model_options{ .resource_ = context.pool() });
        stored.set<"sequence">(std::to_string(sequence)).set<"node_id">(nodeId).set<"session">(*lease).set<"observed_at_ms">(status.observed_at_ms()).set<"snapshot">(std::string_view(json.data(), json.size()));
        const auto document = ruvia::to_json(stored);
        const std::string_view keys[]{ key, leaseKey, topic };
        const auto order = std::to_string(sequence);
        const std::string_view args[]{ nodeId, *lease, timestamp, std::string_view(json.data(), json.size()), std::string_view(document.data(), document.size()), order };
        (void)co_await context.redis().eval(persistence::kSaveIoStateScript, keys, args);
    }

    static ruvia::task<void> saveIoResult(ruvia::web_worker_context& context, std::string_view nodeId, const pb::CommandResult& result) {
        if (result.command_id().size() != 16 || !result.device_id().empty() || !protocol::persistableCommandResult(result.state(), result.write_ack_missing())) {
            throw std::runtime_error("invalid node IO result");
        }
        using Task = persistence::EdgeTaskEntity;
        const auto id = service::common::uuidText(result.command_id());
        std::string json;
        if (!google::protobuf::util::MessageToJsonString(result, &json).ok()) {
            throw std::runtime_error("cannot encode IO result");
        }
        ruvia::db_query query(context.pool());
        auto update = context.db().repository<Task>().update_builder();
        const auto state = protocol::commandResultDomainState(result.state(), result.write_ack_missing(), result.message());
        update.set(Task::column_name<"status">(), query.value(result.write_ack_missing() ? "running" : state == "SUCCEEDED" ? "succeeded"
                                                                                                                            : "failed"))
            .set(Task::column_name<"result">(), query.cast(query.value(json), ruvia::db_data_type::jsonb))
            .set(Task::column_name<"updated_at">(), query.call("now"))
            .set(Task::column_name<"completed_at">(), result.write_ack_missing() ? query.null_value() : query.call("now"))
            .where((Task::column<"id">() == id && Task::column<"node_id">() == nodeId && Task::column<"task_type">() == service::message::edge::kIoTaskType && Task::column<"status">() != std::string_view("succeeded") && Task::column<"status">() != std::string_view("failed")).expression(query))
            .and_where(query.binary(config::detail::jsonText(query, Task::column<"request">().expression(query), "channel"), ruvia::db_binary_operator::equal, query.value(result.node_io_channel())))
            .and_where(query.binary(Task::column<"result">().expression(query), ruvia::db_binary_operator::not_equal, query.cast(query.value(json), ruvia::db_data_type::jsonb)));
        const auto changed = co_await update.execute();
        (void)changed;
    }

    static ruvia::task<void> saveCommandResult(ruvia::web_worker_context& context, const metadata::Catalog& catalog, std::string_view nodeId, std::int64_t receivedAtMs, const pb::CommandResult& result) {
        if (result.command_id().size() != 16 || result.device_id().size() != 16) {
            co_return;
        }
        if (!protocol::persistableCommandResult(result.state(), result.write_ack_missing())) {
            co_return;
        }
        const auto commandId = service::common::uuidText(result.command_id());
        const auto deviceId = service::common::uuidText(result.device_id());
        const auto node = catalog.find(std::string(nodeId));
        if (node == catalog.end()) {
            co_return;
        }
        const auto device = node->second.find(deviceId);
        if (device == node->second.end()) {
            co_return;
        }
        using Operation = service::command::persistence::CommandOperationEntity;
        using Attempt = service::command::persistence::CommandAttemptEntity;
        ruvia::db_query operationQuery(context.pool());
        auto operationBuilder = context.db().template repository<Operation>().query_builder("o");
        operationBuilder.select({ { "protocol", Operation::column<"protocol">().expression(operationQuery, "o") } })
            .template join<Attempt>(ruvia::db_join_type::inner, "a", operationQuery.binary(Attempt::column<"operation_id">().expression(operationQuery, "a"), ruvia::db_binary_operator::equal, Operation::column<"id">().expression(operationQuery, "o")))
            .where(operationQuery.binary(operationQuery.binary(operationQuery.binary(Operation::column<"id">().expression(operationQuery, "o"), ruvia::db_binary_operator::equal, operationQuery.cast(operationQuery.value(commandId), ruvia::db_data_type::uuid)), ruvia::db_binary_operator::and_value, operationQuery.binary(Operation::column<"device_id">().expression(operationQuery, "o"), ruvia::db_binary_operator::equal, operationQuery.cast(operationQuery.value(deviceId), ruvia::db_data_type::uuid))), ruvia::db_binary_operator::and_value, operationQuery.binary(Attempt::column<"node_id">().expression(operationQuery, "a"), ruvia::db_binary_operator::equal, operationQuery.value(nodeId))));
        const auto operations = co_await operationBuilder.get_raw_many();
        if (operations.empty()) {
            co_return;
        }
        const std::string commandProtocol(operations.front()[0].value().value_or(""));
        const auto domainState = protocol::commandResultDomainState(
            result.state(),
            result.write_ack_missing(),
            result.message()
        );
        const bool success = domainState == "SUCCEEDED";
        const std::string state(domainState);
        std::string reason = result.message();
        if (result.write_ack_missing()) {
            reason = "write_ack_missing";
            if (!result.message().empty()) {
                reason += ": " + result.message();
            }
        }
        const auto completedAtMs = message::effectiveObservedAt(
            result.completed_at_ms(),
            receivedAtMs
        );
        std::vector<message::StreamField> fields{
            { "message_id", service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>()) },
            { "causation_id", commandId },
            { "command_id", commandId },
            { "device_id", deviceId },
            { "device_code", device->second.deviceCode },
            { "protocol", commandProtocol },
            { "attempt", "1" },
            { "success", success ? "1" : "0" },
            { "result_state", state },
            { "reason", reason },
            { "worker_id", "0" },
            { "created_at_ms", std::to_string(message::utcNowMilliseconds()) },
            { "completed_at_ms", std::to_string(completedAtMs) },
            { "actual_value_count", std::to_string(result.actual_values_size()) }
        };
        for (int index = 0; index < result.actual_values_size(); ++index) {
            const auto& actual = result.actual_values(index);
            const auto prefix = "actual_value_" + std::to_string(index) + "_";
            fields.push_back({ prefix + "element_id", actual.element_id() });
            fields.push_back({ prefix + "name", actual.name() });
            fields.push_back({ prefix + "kind", actual.has_value() ? protocol::TelemetryValues::scalarKind(actual.value()) : "UNSPECIFIED" });
            fields.push_back({ prefix + "value", actual.has_value() ? protocol::TelemetryValues::scalarText(actual.value()) : std::string{} });
            fields.push_back({ prefix + "unit", actual.unit() });
        }
        (void)co_await service::messaging::stream::publishAndWake(
            context.redis(),
            context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels,
            message::commandResultStream(),
            fields,
            std::nullopt,
            service::message::WorkerStreamTask::CommandResult,
            10000,
            context.worker_state<std::optional<service::common::WorkerIdentity>>().value().instanceId
        );
    }

    static ruvia::task<void> saveDeviceStatus(ruvia::web_worker_context& context, std::string_view nodeId, const pb::DeviceStatusReport& report) {
        std::vector<std::string> updated;
        for (const auto& status : report.devices()) {
            if (status.device_id().size() != 16) {
                continue;
            }
            const auto deviceId = service::common::uuidText(status.device_id());
            std::string clients;
            for (const auto& client : status.clients()) {
                if (!clients.empty()) {
                    clients.push_back(',');
                }
                clients += client;
            }
            const auto key = service::message::edge::deviceStatusKey(nodeId, deviceId);
            (void)co_await context.redis().del(key);
            const std::vector<message::StreamField> fields{
                { "node_id", std::string(nodeId) },
                { "device_id", deviceId },
                { "state", status.state() },
                { "reason", status.reason() },
                { "client_count", std::to_string(status.client_count()) },
                { "clients", clients },
                { "last_activity_at_ms", std::to_string(status.last_activity_at_ms()) },
                { "updated_at_ms", std::to_string(service::message::utcNowMilliseconds()) }
            };
            co_await service::utils::redis::setHash(context.redis(), key, fields);
            const std::vector<std::string> pexpireArguments{ "PEXPIRE", key, "900000" };
            (void)co_await context.redis().command(std::vector<std::string_view>(pexpireArguments.begin(), pexpireArguments.end()));
            updated.push_back(deviceId);
        }
        if (!updated.empty()) {
            co_await service::telemetry::latest::publishRealtimeChange(context.redis(), context.worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, updated);
        }
    }

    // Appends a request to this Worker's own RPC Stream for another feature on
    // the same Worker; nobody waits for the reply. A failed append propagates, so
    // the source entry stays unacknowledged and its retry sends the event again.
    static ruvia::task<void> notifyFeature(ruvia::web_worker_context& context, std::size_t workerIndex, std::string_view component, std::string_view operation, std::string payload) {
        using Contract = service::message::rpc::Contract;
        const auto id = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
        const auto deadline =
            Contract::now() + std::chrono::duration_cast<std::chrono::milliseconds>(Contract::timeout).count();
        const auto& identity = context.template worker_state<std::optional<service::common::WorkerIdentity>>().value();
        const auto requestStream = Contract::requests(identity.instanceId, workerIndex);
        const auto arguments = Contract::requestArguments(
            requestStream,
            id,
            component,
            operation,
            std::move(payload),
            deadline
        );
        const auto inputChannel = service::message::consumer::inputChannel(identity.channels, requestStream);
        const std::string_view keys[]{ requestStream, inputChannel };
        const std::vector<std::string_view> argViews(arguments.begin() + 3, arguments.end());
        const auto reply = co_await context.redis().eval(service::utils::kEnqueueRpcScript, keys, argViews);
        if (reply.kind() != ruvia::redis_value::kind_type::string) {
            service::utils::redis::throwValue("edge feature notification", reply);
        }
    }

    // A firmware task reached a terminal state: its image may expire in an hour.
    // The wakeup is recorded in this Worker's memory, so it cannot fail after the
    // commit; a restart recomputes every deadline anyway.
    static void scheduleFirmwareCleanup(ruvia::web_worker_context& context) {
        context.worker_state<gateway::FirmwareCleanupSignal>().request();
    }
};

} // namespace service::edge::projection
