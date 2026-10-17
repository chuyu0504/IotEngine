#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/channel.h>
#include <ruvia/core/one_shot.h>
#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/redis/redis.h>
#include <ruvia/web/redis/redis_types.h>

#include "service/common/message.h"

namespace service::utils::redis {

[[noreturn]] inline void throwValue(std::string_view operation, const ruvia::redis_value& value) {
    std::string message(operation);
    message += " failed";
    if (value.kind() == ruvia::redis_value::kind_type::error) {
        message += ": ";
        message.append(value.error());
        throw ruvia::redis_error(ruvia::redis_error::code_type::command_error, message);
    }
    throw std::runtime_error(message);
}

// ---- 消费组 / 读取 ----

inline constexpr std::string_view kEnsureGroupScript = R"lua(
local result = redis.pcall('XGROUP', 'CREATE', KEYS[1], ARGV[1], '0', 'MKSTREAM')
local created = 1
if type(result) == 'table' and result.err then
  if string.sub(result.err, 1, 9) ~= 'BUSYGROUP' then return redis.error_reply(result.err) end
  created = 0
end
if ARGV[2] ~= '' and redis.call('EXPIRE', KEYS[1], ARGV[2]) ~= 1 then
  return redis.error_reply('consumer stream expiry was not applied')
end
return created
)lua";

inline constexpr std::string_view kEnsurePresentGroupScript = R"lua(
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local result = redis.pcall('XGROUP', 'CREATE', KEYS[1], ARGV[1], '0')
if type(result) == 'table' and result.err then
  if string.sub(result.err, 1, 9) == 'BUSYGROUP' then return 1 end
  return redis.error_reply(result.err)
end
return 1
)lua";

template <typename Redis>
ruvia::task<void> ensureGroup(const Redis& redis, std::string_view stream, std::string_view group, std::optional<std::chrono::seconds> streamTtl = std::nullopt) {
    if (streamTtl && streamTtl->count() <= 0) {
        throw std::invalid_argument("consumer stream expiry must be positive");
    }
    const auto ttl = streamTtl ? std::to_string(streamTtl->count()) : std::string{};
    const std::string_view keys[]{ stream };
    const std::string_view arguments[]{ group, ttl };
    const auto reply = co_await redis.eval(kEnsureGroupScript, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        throwValue("XGROUP CREATE", reply);
    }
}

// A discovered stream may disappear while recovery is being prepared.  Create
// its consumer group only when the stream still exists; this keeps recovery
// from resurrecting a stream that was already drained and removed.
template <typename Redis>
ruvia::task<bool> ensureGroupIfPresent(const Redis& redis, std::string_view stream, std::string_view group) {
    const std::string_view keys[]{ stream };
    const std::string_view arguments[]{ group };
    const auto reply = co_await redis.eval(kEnsurePresentGroupScript, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        throwValue("XGROUP CREATE discovered stream", reply);
    }
    co_return reply.integer() != 0;
}

struct StreamBatch final {
    std::string stream;
    std::vector<service::message::StreamMessage> messages;
};

// Parses an XREAD/XREADGROUP array reply; a null reply is an empty batch list.
inline std::vector<StreamBatch> parseStreamBatches(std::string_view operation, const ruvia::redis_value& reply) {
    std::vector<StreamBatch> batches;
    if (reply.null()) {
        return batches;
    }
    if (reply.kind() != ruvia::redis_value::kind_type::array) {
        throwValue(operation, reply);
    }
    for (const auto& streamReply : reply.array()) {
        if (streamReply.kind() != ruvia::redis_value::kind_type::array ||
            streamReply.array().size() != 2) {
            continue;
        }
        const auto& streamName = streamReply.array()[0];
        const auto& entries = streamReply.array()[1];
        if (streamName.kind() != ruvia::redis_value::kind_type::string ||
            entries.kind() != ruvia::redis_value::kind_type::array) {
            continue;
        }
        StreamBatch batch;
        batch.stream.assign(streamName.string());
        batch.messages.reserve(entries.array().size());
        for (const auto& entry : entries.array()) {
            if (entry.kind() != ruvia::redis_value::kind_type::array || entry.array().size() != 2) {
                continue;
            }
            const auto& entryId = entry.array()[0];
            const auto& fieldArray = entry.array()[1];
            if (entryId.kind() != ruvia::redis_value::kind_type::string ||
                fieldArray.kind() != ruvia::redis_value::kind_type::array) {
                continue;
            }
            service::message::StreamMessage message;
            message.id.assign(entryId.string());
            const auto fieldValues = fieldArray.array();
            for (std::size_t index = 0; index + 1 < fieldValues.size(); index += 2) {
                const auto& name = fieldValues[index];
                const auto& value = fieldValues[index + 1];
                if (name.kind() != ruvia::redis_value::kind_type::string ||
                    value.kind() != ruvia::redis_value::kind_type::string) {
                    continue;
                }
                message.fields.push_back(
                    { std::string(name.string()), std::string(value.string()) }
                );
            }
            batch.messages.push_back(std::move(message));
        }
        if (!batch.messages.empty()) {
            batches.push_back(std::move(batch));
        }
    }
    return batches;
}

// Plain XREAD without a consumer group: the caller owns one cursor per stream.
// The blocking wait ends on new entries, the timeout, or the stop token.
template <typename Redis>
ruvia::task<std::vector<StreamBatch>>
readStreamsBlocking(const Redis& redis, std::span<const std::string> streams, std::span<const std::string> cursors, ruvia::stop_token stopToken, std::chrono::milliseconds timeout, std::size_t count) {
    if (streams.empty() || streams.size() != cursors.size()) {
        throw std::invalid_argument("XREAD requires one cursor per stream");
    }
    std::vector<std::string_view> args{ "XREAD", "COUNT" };
    const auto countText = std::to_string(count);
    const auto blockText = std::to_string(std::max<std::int64_t>(1, timeout.count()));
    args.push_back(countText);
    args.push_back("BLOCK");
    args.push_back(blockText);
    args.push_back("STREAMS");
    args.insert(args.end(), streams.begin(), streams.end());
    args.insert(args.end(), cursors.begin(), cursors.end());
    const auto reply = co_await redis.with_options({ .stop_token_ = std::move(stopToken) })
                           .command(std::span<const std::string_view>(args));
    co_return parseStreamBatches("XREAD", reply);
}

// Id of the newest entry of a stream, or "0-0" when it is empty or absent.
template <typename Redis>
ruvia::task<std::string> lastEntryId(const Redis& redis, std::string_view stream) {
    const std::vector<std::string> args{ "XREVRANGE", std::string(stream), "+", "-", "COUNT", "1" };
    const auto reply = co_await redis.command(std::vector<std::string_view>(args.begin(), args.end()));
    if (reply.kind() != ruvia::redis_value::kind_type::array) {
        throwValue("XREVRANGE", reply);
    }
    if (reply.array().empty()) {
        co_return "0-0";
    }
    const auto& entry = reply.array().front();
    if (entry.kind() != ruvia::redis_value::kind_type::array || entry.array().empty() ||
        entry.array().front().kind() != ruvia::redis_value::kind_type::string) {
        throwValue("XREVRANGE", reply);
    }
    co_return std::string(entry.array().front().string());
}

inline std::vector<StreamBatch> streamBatches(const std::optional<ruvia::redis_x_read_group_result>& result) {
    std::vector<StreamBatch> batches;
    if (!result) {
        return batches;
    }
    batches.reserve(result->streams().size());
    for (const auto& stream : result->streams()) {
        StreamBatch batch;
        batch.stream.assign(stream.stream());
        batch.messages.reserve(stream.entries().size());
        for (const auto& entry : stream.entries()) {
            service::message::StreamMessage message;
            message.id.assign(entry.id());
            message.fields.reserve(entry.fields().size());
            for (const auto& field : entry.fields()) {
                message.fields.push_back({ std::string(field.key()), std::string(field.value()) });
            }
            batch.messages.push_back(std::move(message));
        }
        if (!batch.messages.empty()) {
            batches.push_back(std::move(batch));
        }
    }
    return batches;
}

template <typename Redis>
ruvia::task<std::vector<StreamBatch>>
readGroupManyImpl(const Redis& redis, std::span<const std::string> streams, std::string_view group, std::string_view consumer, std::string_view id, std::optional<std::chrono::milliseconds> block, std::size_t count) {
    if (streams.empty()) {
        co_return std::vector<StreamBatch>{};
    }
    std::vector<ruvia::redis_stream_read_view> reads;
    reads.reserve(streams.size());
    for (const auto& stream : streams) {
        reads.push_back({ .stream_ = stream, .id_ = id });
    }
    ruvia::redis_x_read_group_options options;
    options.count_ = count;
    if (block) {
        options.block_ = ruvia::redis_block_wait::for_duration(*block);
    }
    co_return streamBatches(co_await redis.xread_group(group, consumer, reads, options));
}

template <typename Redis>
ruvia::task<std::vector<StreamBatch>>
readGroupMany(const Redis& redis, std::span<const std::string> streams, std::string_view group, std::string_view consumer, std::string_view id, std::size_t count = 100) {
    co_return co_await readGroupManyImpl(redis, streams, group, consumer, id, std::nullopt, count);
}

template <typename Redis>
ruvia::task<std::vector<StreamBatch>>
readGroupManyBlockingUntil(const Redis& redis, std::span<const std::string> streams, std::string_view group, std::string_view consumer, ruvia::stop_token stopToken, std::optional<std::chrono::milliseconds> timeout, std::size_t count = 100) {
    if (streams.empty()) {
        co_return std::vector<StreamBatch>{};
    }

    std::vector<ruvia::redis_stream_read_view> reads;
    reads.reserve(streams.size());
    for (const auto& stream : streams) {
        reads.push_back({ .stream_ = stream, .id_ = ">" });
    }

    ruvia::redis_x_read_group_options options;
    options.count_ = count;
    options.block_ = timeout.has_value()
        ? ruvia::redis_block_wait::for_duration(*timeout)
        : ruvia::redis_block_wait::indefinitely();
    auto result = co_await redis.with_options({ .stop_token_ = std::move(stopToken) })
                      .xread_group(group, consumer, reads, std::move(options));

    co_return streamBatches(result);
}

inline constexpr std::string_view kConsumeGroupScript = R"lua(
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local idle = tonumber(ARGV[4])
local count = tonumber(ARGV[3])
local batches = {}
local function record(offset, entries)
  local kept = {}
  for _, entry in ipairs(entries) do
    if type(entry[2]) == 'table' then
      redis.call('ZADD', KEYS[offset + 1], now + idle, entry[1])
      kept[#kept + 1] = entry
    else
      redis.call('XACK', KEYS[offset], ARGV[1], entry[1])
      redis.call('ZREM', KEYS[offset + 1], entry[1])
    end
  end
  if #kept > 0 then
    redis.call('PUBLISH', KEYS[offset + 2], tostring(now + idle))
    batches[#batches + 1] = {KEYS[offset], kept}
  end
end
if ARGV[6] ~= '1' then
for offset = 1, #KEYS, 3 do
  if ARGV[5] == '1' then
    local cursor = '-'
    repeat
      local pending = redis.call('XPENDING', KEYS[offset], ARGV[1], cursor, '+', 128)
      for _, entry in ipairs(pending) do
        redis.call('ZADD', KEYS[offset + 1], now + math.max(0, idle - tonumber(entry[3])), entry[1])
        cursor = '(' .. entry[1]
      end
    until #pending < 128
  end
  local owned = redis.call('XREADGROUP', 'GROUP', ARGV[1], ARGV[2], 'COUNT', count, 'STREAMS', KEYS[offset], '0')
  if owned then record(offset, owned[1][2]) end
end
if #batches == 0 then
  for offset = 1, #KEYS, 3 do
    local due = redis.call('ZRANGEBYSCORE', KEYS[offset + 1], '-inf', now, 'LIMIT', 0, count)
    local recovered = {}
    for _, id in ipairs(due) do
      local claimed = redis.call('XCLAIM', KEYS[offset], ARGV[1], ARGV[2], idle, id)
      if #claimed > 0 then
        for _, entry in ipairs(claimed) do recovered[#recovered + 1] = entry end
      else
        local pending = redis.call('XPENDING', KEYS[offset], ARGV[1], id, id, 1)
        if #pending == 0 then
          redis.call('ZREM', KEYS[offset + 1], id)
        else
          redis.call('ZADD', KEYS[offset + 1], now + math.max(1, idle - tonumber(pending[1][3])), id)
        end
      end
    end
    record(offset, recovered)
  end
end
end
if #batches == 0 then
  for offset = 1, #KEYS, 3 do
    local incoming = redis.call('XREADGROUP', 'GROUP', ARGV[1], ARGV[2], 'COUNT', count, 'STREAMS', KEYS[offset], '>')
    if incoming then record(offset, incoming[1][2]) end
  end
end
local nearest = false
for offset = 1, #KEYS, 3 do
  local ttl = redis.call('PTTL', KEYS[offset])
  if ttl > 0 then
    redis.call('PEXPIRE', KEYS[offset + 1], ttl)
  elseif ttl == -1 then
    redis.call('PERSIST', KEYS[offset + 1])
  end
  local first = redis.call('ZRANGE', KEYS[offset + 1], 0, 0, 'WITHSCORES')
  if #first == 2 and (not nearest or tonumber(first[2]) < nearest) then nearest = tonumber(first[2]) end
end
return {batches, nearest, now}
)lua";

template <typename Redis>
ruvia::task<void> forwardConsumerNotifications(
    Redis redis,
    const ruvia::worker_handle& worker,
    std::vector<std::string> channels,
    ruvia::channel_sender<std::uint8_t> sender,
    ruvia::one_shot_completion<std::exception_ptr> ready,
    ruvia::stop_token stop
) {
    bool initialized = false;
    auto backoff = std::chrono::milliseconds(250);
    while (!stop.stop_requested()) {
        bool failed = false;
        try {
            auto subscription = redis.with_options({ .stop_token_ = stop }).subscription();
            std::vector<std::string_view> views(channels.begin(), channels.end());
            co_await subscription.subscribe(views);
            std::size_t confirmed = 0;
            while (confirmed < channels.size()) {
                const auto event = co_await subscription.read();
                if (!event) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    throw std::runtime_error("consumer subscription closed before confirmation");
                }
                if (event->kind() == ruvia::redis_pubsub_event_kind::subscribed) {
                    ++confirmed;
                }
            }
            if (stop.stop_requested()) {
                break;
            }
            if (!initialized) {
                initialized = true;
                (void)ready.complete(nullptr);
            }
            (void)sender.send(1); // Reconnect always drains durable state once.
            backoff = std::chrono::milliseconds(250);
            while (!stop.stop_requested()) {
                const auto event = co_await subscription.read();
                if (!event) {
                    if (stop.stop_requested()) {
                        break;
                    }
                    throw std::runtime_error("consumer subscription disconnected");
                }
                if (event->kind() == ruvia::redis_pubsub_event_kind::message) {
                    (void)sender.send(1); // Capacity one coalesces notifications, not data.
                }
            }
        } catch (...) {
            if (!initialized) {
                (void)ready.complete(std::current_exception());
                sender.close();
                co_return;
            }
            if (!stop.stop_requested()) {
                try {
                    throw;
                } catch (const std::exception& error) {
                    std::cerr << "consumer subscription failed: " << error.what() << '\n';
                } catch (...) {
                    std::cerr << "consumer subscription failed: unknown exception\n";
                }
                failed = true;
            }
        }
        if (failed) {
            (void)co_await ruvia::sleep_for(worker, backoff, stop);
            backoff = std::min(backoff * 2, std::chrono::milliseconds(5000));
        }
    }
    if (!initialized) {
        (void)ready.complete(std::make_exception_ptr(std::runtime_error("consumer subscription startup cancelled")));
    }
    sender.close();
}

// One runtime invocation owns this structured notification scope. Healthy
// subscription reads never time out; only the local channel waits for deadlines.
template <typename Redis, typename Operation>
ruvia::task<void> withConsumerNotifications(
    const Redis& redis,
    const ruvia::worker_handle& worker,
    std::span<const std::string> streams,
    std::string_view group,
    ruvia::stop_token stop,
    Operation operation
) {
    std::vector<std::string> channels;
    channels.reserve(streams.size() * 2);
    for (const auto& stream : streams) {
        channels.push_back(service::message::consumer::inputChannel(stream));
        channels.push_back(service::message::consumer::deadlineChannel(stream, group));
    }
    auto [sender, receiver] = ruvia::make_channel<std::uint8_t>(worker, { .capacity_ = 1 });
    auto [ready, initialized] = ruvia::make_one_shot<std::exception_ptr>(worker);
    ruvia::task_scope scope(worker);
    const auto combined = ruvia::combine_stop_tokens(stop, scope.get_stop_token());
    scope.spawn(forwardConsumerNotifications(redis, worker, std::move(channels), std::move(sender), std::move(ready), combined));
    std::exception_ptr failure;
    try {
        auto result = co_await initialized.wait(combined);
        if (!result.has_value()) {
            throw std::runtime_error("consumer subscription startup interrupted");
        }
        if (auto error = std::move(result).take_value()) {
            std::rethrow_exception(error);
        }
        co_await operation(receiver);
    } catch (...) {
        failure = std::current_exception();
    }
    scope.request_stop();
    co_await scope.join();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

template <typename Redis>
ruvia::task<std::vector<StreamBatch>> consumeGroup(
    const Redis& redis,
    std::span<const std::string> streams,
    std::string_view group,
    std::string_view consumer,
    bool& recovering,
    ruvia::channel_receiver<std::uint8_t>& notifications,
    ruvia::stop_token stop,
    std::optional<std::chrono::milliseconds> timeout = std::nullopt,
    std::size_t count = 256,
    std::chrono::milliseconds minIdle = std::chrono::minutes(1),
    bool newOnly = false,
    std::optional<std::chrono::seconds> streamTtl = std::nullopt
) {
    const auto businessDeadline = timeout
        ? std::optional(std::chrono::steady_clock::now() + *timeout)
        : std::nullopt;
    std::vector<std::string> storage;
    storage.reserve(streams.size() * 2);
    for (const auto& stream : streams) {
        storage.push_back(service::message::consumer::deadlineKey(stream, group));
        storage.push_back(service::message::consumer::deadlineChannel(stream, group));
    }
    std::vector<std::string_view> keys;
    keys.reserve(streams.size() * 3);
    for (std::size_t index = 0; index < streams.size(); ++index) {
        keys.push_back(streams[index]);
        keys.push_back(storage[index * 2]);
        keys.push_back(storage[index * 2 + 1]);
    }
    const auto countText = std::to_string(count);
    const auto idleText = std::to_string(std::max<std::int64_t>(0, minIdle.count()));
    std::string firstFailure;
    bool retried = false;
    while (!stop.stop_requested()) {
        bool restoreGroups = false;
        try {
            const std::string_view arguments[]{ group, consumer, countText, idleText, recovering ? "1" : "0", newOnly ? "1" : "0" };
            const auto reply = co_await redis.eval(kConsumeGroupScript, keys, arguments);
            if (reply.kind() != ruvia::redis_value::kind_type::array || reply.array().size() != 3) {
                throwValue("consumer claim/read", reply);
            }
            auto batches = parseStreamBatches("consumer claim/read entries", reply.array()[0]);
            recovering = false;
            if (!batches.empty()) {
                co_return batches;
            }
            std::optional<std::chrono::milliseconds> wait;
            if (!reply.array()[1].null()) {
                if (reply.array()[1].kind() != ruvia::redis_value::kind_type::integer ||
                    reply.array()[2].kind() != ruvia::redis_value::kind_type::integer) {
                    throwValue("consumer pending deadline", reply);
                }
                wait = std::chrono::milliseconds(std::max<std::int64_t>(0, reply.array()[1].integer() - reply.array()[2].integer()));
            }
            if (businessDeadline) {
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(*businessDeadline - std::chrono::steady_clock::now());
                if (remaining <= std::chrono::milliseconds(0)) {
                    co_return batches;
                }
                wait = wait ? std::min(*wait, remaining) : remaining;
            }
            if (wait && *wait <= std::chrono::milliseconds(0)) {
                continue;
            }
            const auto wake = wait
                ? co_await notifications.receive_for(*wait, stop)
                : co_await notifications.receive(stop);
            if (!wake.has_value() && !stop.stop_requested() &&
                wake.status() != ruvia::worker_wait_status::timed_out) {
                throw std::runtime_error("consumer notification reader closed");
            }
        } catch (const ruvia::redis_error& error) {
            recovering = true;
            if (retried || error.code() != ruvia::redis_error::code_type::command_error || stop.stop_requested()) {
                if (!firstFailure.empty()) {
                    std::cerr << "Redis consumer retry failed after: " << firstFailure << "; retry: " << error.what() << '\n';
                }
                throw;
            }
            firstFailure = error.what();
            std::cerr << "Redis consumer command failed before group recovery: " << firstFailure << '\n';
            retried = true;
            restoreGroups = true;
        }
        if (restoreGroups) {
            try {
                for (const auto& stream : streams) {
                    co_await ensureGroup(redis, stream, group, streamTtl);
                }
            } catch (const std::exception& error) {
                std::cerr << "Redis consumer group recovery failed after: " << firstFailure << "; recovery: " << error.what() << '\n';
                throw;
            }
        }
    }
    co_return std::vector<StreamBatch>{};
}

template <typename Redis>
ruvia::task<std::vector<service::message::StreamMessage>>
readGroup(const Redis& redis, std::string_view stream, std::string_view group, std::string_view consumer, std::string_view id, std::chrono::milliseconds block, std::size_t count = 100) {
    const std::vector<std::string> streams{ std::string(stream) };
    auto batches = co_await readGroupManyImpl(
        redis,
        streams,
        group,
        consumer,
        id,
        block.count() > 0 ? std::optional<std::chrono::milliseconds>(block) : std::nullopt,
        count
    );
    if (batches.empty()) {
        co_return std::vector<service::message::StreamMessage>{};
    }
    co_return std::move(batches.front().messages);
}

template <typename Redis>
ruvia::task<std::vector<service::message::StreamMessage>>
readGroupBlockingUntil(const Redis& redis, std::string_view stream, std::string_view group, std::string_view consumer, ruvia::stop_token stopToken, std::optional<std::chrono::milliseconds> timeout, std::size_t count = 100) {
    const std::vector<std::string> streams{ std::string(stream) };
    auto batches = co_await readGroupManyBlockingUntil(
        redis,
        streams,
        group,
        consumer,
        std::move(stopToken),
        timeout,
        count
    );
    if (batches.empty()) {
        co_return std::vector<service::message::StreamMessage>{};
    }
    co_return std::move(batches.front().messages);
}

template <typename Redis>
ruvia::task<std::vector<service::message::StreamMessage>>
readGroupBlocking(const Redis& redis, std::string_view stream, std::string_view group, std::string_view consumer, ruvia::stop_token stopToken, std::size_t count = 100) {
    co_return co_await readGroupBlockingUntil(redis, stream, group, consumer, std::move(stopToken), std::nullopt, count);
}

inline constexpr std::string_view kAcknowledgeScript = R"lua(
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('PUBLISH', KEYS[3], '')
return acknowledged
)lua";

template <typename Redis>
ruvia::task<void> acknowledge(const Redis& redis, std::string_view stream, std::string_view group, std::string_view id) {
    const auto pending = service::message::consumer::deadlineKey(stream, group);
    const auto channel = service::message::consumer::deadlineChannel(stream, group);
    const std::string_view keys[]{ stream, pending, channel };
    const std::string_view arguments[]{ group, id };
    const auto reply = co_await redis.eval(kAcknowledgeScript, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        throwValue("XACK", reply);
    }
}

template <typename Replies>
void requirePipelineSuccess(std::string_view operation, const Replies& replies) {
    for (const auto& reply : replies) {
        if (reply.kind() == ruvia::redis_value::kind_type::error) {
            throwValue(operation, reply);
        }
    }
}

} // namespace service::utils::redis

namespace service::utils::redis {
using ruvia::redis_value;

inline constexpr std::string_view kAddStreamScript = R"lua(
local fields = {'MAXLEN', '~', ARGV[1], '*'}
for index = 2, #ARGV do fields[#fields + 1] = ARGV[index] end
local id = redis.call('XADD', KEYS[1], unpack(fields))
redis.call('PUBLISH', KEYS[2], id)
return id
)lua";

template <typename Redis>
ruvia::task<std::string> add(const Redis& redis, std::string_view stream, const std::vector<service::message::StreamField>& fields, std::size_t maxLength = 100000) {
    if (fields.empty()) {
        throw std::invalid_argument("Redis Stream message must contain fields");
    }
    const auto length = std::to_string(maxLength);
    const auto channel = service::message::consumer::inputChannel(stream);
    const std::string_view keys[]{ stream, channel };
    std::vector<std::string_view> arguments{ length };
    arguments.reserve(1 + fields.size() * 2);
    for (const auto& field : fields) {
        arguments.push_back(field.name);
        arguments.push_back(field.value);
    }
    const auto reply = co_await redis.eval(kAddStreamScript, keys, arguments);
    if (reply.kind() != redis_value::kind_type::string) {
        throwValue("XADD", reply);
    }
    co_return std::string(reply.string());
}

inline constexpr std::string_view kAcknowledgeDeleteScript = R"lua(
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('PUBLISH', KEYS[3], '')
return acknowledged
)lua";

template <typename Redis>
ruvia::task<void> acknowledgeAndDelete(const Redis& redis, std::string_view stream, std::string_view group, std::string_view id) {
    const auto pending = service::message::consumer::deadlineKey(stream, group);
    const auto channel = service::message::consumer::deadlineChannel(stream, group);
    const std::string_view keys[]{ stream, pending, channel };
    const std::string_view arguments[]{ group, id };
    const auto reply = co_await redis.eval(kAcknowledgeDeleteScript, keys, arguments);
    if (reply.kind() != redis_value::kind_type::integer) {
        throwValue("XACK/XDEL", reply);
    }
}

inline constexpr std::string_view kAcknowledgeDeleteManyScript = R"lua(
local ids = {}
for index = 2, #ARGV do ids[#ids + 1] = ARGV[index] end
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], unpack(ids))
redis.call('XDEL', KEYS[1], unpack(ids))
redis.call('ZREM', KEYS[2], unpack(ids))
redis.call('PUBLISH', KEYS[3], '')
return acknowledged
)lua";

template <typename Redis>
ruvia::task<void> acknowledgeAndDeleteMany(
    const Redis& redis,
    std::string_view stream,
    std::string_view group,
    const std::vector<service::message::StreamMessage>& messages
) {
    if (messages.empty()) {
        co_return;
    }
    const auto pending = service::message::consumer::deadlineKey(stream, group);
    const auto channel = service::message::consumer::deadlineChannel(stream, group);
    const std::string_view keys[]{ stream, pending, channel };
    std::vector<std::string_view> arguments;
    arguments.reserve(messages.size() + 1);
    arguments.push_back(group);
    for (const auto& message : messages) {
        arguments.push_back(message.id);
    }
    const auto reply = co_await redis.eval(kAcknowledgeDeleteManyScript, keys, arguments);
    if (reply.kind() != redis_value::kind_type::integer) {
        throwValue("batch XACK/XDEL", reply);
    }
}

// ---- Hash 状态 ----

template <typename Redis>
ruvia::task<void> setHash(const Redis& redis, std::string_view key, const std::vector<service::message::StreamField>& fields) {
    if (fields.empty()) {
        co_return;
    }
    std::vector<std::string_view> args{ "HSET", key };
    args.reserve(2 + fields.size() * 2);
    for (const auto& field : fields) {
        args.push_back(field.name);
        args.push_back(field.value);
    }
    const auto reply = co_await redis.command(args);
    if (reply.kind() != redis_value::kind_type::integer) {
        throwValue("HSET", reply);
    }
}

template <typename Redis>
ruvia::task<std::vector<service::message::StreamField>> hashEntries(const Redis& redis, std::string_view key) {
    const auto entries = co_await redis.command("HGETALL", key);
    if (entries.kind() != redis_value::kind_type::array) {
        throwValue("HGETALL", entries);
    }
    std::vector<service::message::StreamField> result;
    const auto values = entries.array();
    result.reserve(values.size() / 2);
    for (std::size_t index = 0; index + 1 < values.size(); index += 2) {
        if (values[index].kind() != redis_value::kind_type::string ||
            values[index + 1].kind() != redis_value::kind_type::string) {
            throwValue("HGETALL fields", entries);
        }
        result.push_back(
            { std::string(values[index].string()), std::string(values[index + 1].string()) }
        );
    }
    co_return result;
}

// ---- SCAN ----

template <typename Redis>
ruvia::task<std::vector<std::string>> keysMatching(const Redis& redis, std::string_view pattern) {
    std::vector<std::string> result;
    std::string cursor = "0";
    do {
        const std::vector<std::string> pageArguments{ "SCAN", cursor, "MATCH", std::string(pattern), "COUNT", "100" };
        const auto page = co_await redis.command(std::vector<std::string_view>(pageArguments.begin(), pageArguments.end()));
        if (page.kind() != redis_value::kind_type::array || page.array().size() != 2 ||
            page.array()[0].kind() != redis_value::kind_type::string ||
            page.array()[1].kind() != redis_value::kind_type::array) {
            throwValue("SCAN", page);
        }
        cursor.assign(page.array()[0].string());
        for (const auto& value : page.array()[1].array()) {
            if (value.kind() != redis_value::kind_type::string) {
                throwValue("SCAN key", page);
            }
            result.emplace_back(value.string());
        }
    } while (cursor != "0");
    co_return result;
}

} // namespace service::utils::redis
