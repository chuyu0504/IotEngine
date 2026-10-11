#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/stop_token.h>
#include <ruvia/core/task.h>
#include <ruvia/web/redis/redis.h>
#include <ruvia/web/redis/redis_types.h>

#include "service/common/message.h"

namespace service::message::redis {

template <typename Redis>
ruvia::task<ruvia::redis_value> command(const Redis& redis, const std::vector<std::string>& args) {
    std::vector<std::string_view> views;
    views.reserve(args.size());
    for (const auto& arg : args) {
        views.emplace_back(arg);
    }
    co_return co_await redis.command(std::span<const std::string_view>(views));
}

[[noreturn]] inline void throwValue(std::string_view operation, const ruvia::redis_value& value) {
    std::string message(operation);
    message += " failed";
    if (value.kind() == ruvia::redis_value::kind_type::error) {
        message += ": ";
        message.append(value.error());
    }
    throw std::runtime_error(message);
}

// ---- 消费组 / 读取 ----

template <typename Redis>
ruvia::task<void> ensureGroup(const Redis& redis, std::string_view stream, std::string_view group) {
    const auto reply = co_await command(
        redis,
        { "XGROUP", "CREATE", std::string(stream), std::string(group), "0", "MKSTREAM" }
    );
    if (reply.kind() == ruvia::redis_value::kind_type::error &&
        !reply.error().starts_with("BUSYGROUP")) {
        throwValue("XGROUP CREATE", reply);
    }
}

// A discovered stream may disappear while recovery is being prepared.  Create
// its consumer group only when the stream still exists; this keeps recovery
// from resurrecting a stream that was already drained and removed.
template <typename Redis>
ruvia::task<bool> ensureGroupIfPresent(const Redis& redis, std::string_view stream, std::string_view group) {
    const auto reply = co_await command(
        redis,
        { "XGROUP", "CREATE", std::string(stream), std::string(group), "0" }
    );
    if (reply.kind() == ruvia::redis_value::kind_type::error) {
        const auto error = reply.error();
        if (error.starts_with("BUSYGROUP") ||
            error.find("requires the key to exist") != std::string_view::npos ||
            error.find("no such key") != std::string_view::npos) {
            co_return error.starts_with("BUSYGROUP");
        }
        throwValue("XGROUP CREATE discovered stream", reply);
    }
    if (reply.kind() != ruvia::redis_value::kind_type::string) {
        throwValue("XGROUP CREATE discovered stream", reply);
    }
    co_return true;
}

struct StreamBatch final {
    std::string stream;
    std::vector<service::message::StreamMessage> messages;
};

template <typename Redis>
ruvia::task<std::vector<StreamBatch>>
readGroupManyImpl(const Redis& redis, std::span<const std::string> streams, std::string_view group, std::string_view consumer, std::string_view id, std::optional<std::chrono::milliseconds> block, std::size_t count);

template <typename Redis>
ruvia::task<std::vector<StreamBatch>>
claimGroupMany(
    const Redis& redis,
    std::span<const std::string> streams,
    std::string_view group,
    std::string_view consumer,
    std::size_t count = 100,
    std::chrono::milliseconds minIdle = std::chrono::minutes(1)
) {
    auto own = co_await readGroupManyImpl(redis, streams, group, consumer, "0", std::nullopt, count);
    if (!own.empty()) {
        co_return own;
    }
    std::vector<StreamBatch> batches;
    for (const auto& stream : streams) {
        const auto reply = co_await command(
            redis,
            { "XAUTOCLAIM",
              stream,
              std::string(group),
              std::string(consumer),
              std::to_string(std::max<std::int64_t>(0, minIdle.count())),
              "0-0",
              "COUNT",
              std::to_string(count) }
        );
        if (reply.kind() != ruvia::redis_value::kind_type::array || reply.array().size() < 2) {
            throwValue("XAUTOCLAIM", reply);
        }
        const auto& entries = reply.array()[1];
        if (entries.kind() != ruvia::redis_value::kind_type::array) {
            throwValue("XAUTOCLAIM entries", reply);
        }
        StreamBatch batch;
        batch.stream = stream;
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
    co_return batches;
}

template <typename Redis>
ruvia::task<std::vector<StreamBatch>>
readGroupManyImpl(const Redis& redis, std::span<const std::string> streams, std::string_view group, std::string_view consumer, std::string_view id, std::optional<std::chrono::milliseconds> block, std::size_t count) {
    if (streams.empty()) {
        co_return std::vector<StreamBatch>{};
    }
    std::vector<std::string> args{ "XREADGROUP", "GROUP", std::string(group), std::string(consumer), "COUNT", std::to_string(count) };
    if (block.has_value()) {
        args.emplace_back("BLOCK");
        args.push_back(std::to_string(block->count()));
    }
    args.emplace_back("STREAMS");
    args.insert(args.end(), streams.begin(), streams.end());
    for (std::size_t index = 0; index < streams.size(); ++index) {
        args.emplace_back(id);
    }
    const auto reply = co_await command(redis, args);
    std::vector<StreamBatch> batches;
    if (reply.null()) {
        co_return batches;
    }
    if (reply.kind() != ruvia::redis_value::kind_type::array) {
        throwValue("XREADGROUP", reply);
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
    co_return batches;
}

template <typename Redis>
ruvia::task<std::vector<StreamBatch>>
readGroupMany(const Redis& redis, std::span<const std::string> streams, std::string_view group, std::string_view consumer, std::string_view id, std::size_t count = 100) {
    auto batches = co_await readGroupManyImpl(redis, streams, group, consumer, id, std::nullopt, count);
    if (batches.empty() && id == ">") {
        co_return co_await claimGroupMany(redis, streams, group, consumer, count);
    }
    co_return batches;
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

    std::vector<StreamBatch> batches;
    if (!result.has_value()) {
        co_return batches;
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
                message.fields.push_back(
                    { std::string(field.key()), std::string(field.value()) }
                );
            }
            batch.messages.push_back(std::move(message));
        }
        if (!batch.messages.empty()) {
            batches.push_back(std::move(batch));
        }
    }
    co_return batches;
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

template <typename Redis>
ruvia::task<void> acknowledge(const Redis& redis, std::string_view stream, std::string_view group, std::string_view id) {
    const auto reply =
        co_await command(redis, { "XACK", std::string(stream), std::string(group), std::string(id) });
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

} // namespace service::message::redis

namespace service::message::redis {
using ruvia::redis_value;

template <typename Pipeline>
void queueEval(Pipeline& pipeline, std::string_view script, std::span<const std::string_view> keys, std::span<const std::string_view> arguments) {
    const auto keyCount = std::to_string(keys.size());
    std::vector<std::string_view> command{ "EVAL", script, keyCount };
    command.reserve(command.size() + keys.size() + arguments.size());
    command.insert(command.end(), keys.begin(), keys.end());
    command.insert(command.end(), arguments.begin(), arguments.end());
    // redis_pipeline copies every argument synchronously.
    pipeline.command(command);
}

template <typename Pipeline>
void queueEvalSha(Pipeline& pipeline, std::string_view sha, std::span<const std::string_view> keys, std::span<const std::string_view> arguments) {
    const auto keyCount = std::to_string(keys.size());
    std::vector<std::string_view> command{ "EVALSHA", sha, keyCount };
    command.reserve(command.size() + keys.size() + arguments.size());
    command.insert(command.end(), keys.begin(), keys.end());
    command.insert(command.end(), arguments.begin(), arguments.end());
    // redis_pipeline copies every argument synchronously.
    pipeline.command(command);
}

template <typename Redis>
ruvia::task<std::string> add(const Redis& redis, std::string_view stream, const std::vector<StreamField>& fields, std::size_t maxLength = 100000) {
    if (fields.empty()) {
        throw std::invalid_argument("Redis Stream message must contain fields");
    }
    std::vector<std::string> args{ "XADD", std::string(stream), "MAXLEN", "~", std::to_string(maxLength), "*" };
    args.reserve(args.size() + fields.size() * 2);
    for (const auto& field : fields) {
        args.push_back(field.name);
        args.push_back(field.value);
    }
    const auto reply = co_await command(redis, args);
    if (reply.kind() != redis_value::kind_type::string) {
        throwValue("XADD", reply);
    }
    co_return std::string(reply.string());
}

template <typename Redis>
ruvia::task<void> acknowledgeAndDelete(const Redis& redis, std::string_view stream, std::string_view group, std::string_view id) {
    static constexpr std::string_view script = R"lua(
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return acknowledged
)lua";
    const std::string streamKey(stream);
    const std::string groupValue(group);
    const std::string idValue(id);
    const std::string_view keys[]{ streamKey };
    const std::string_view arguments[]{ groupValue, idValue };
    const auto reply = co_await redis.eval(script, keys, arguments);
    if (reply.kind() != redis_value::kind_type::integer) {
        throwValue("XACK/XDEL", reply);
    }
}

template <typename Redis>
ruvia::task<void> acknowledgeAndDeleteMany(
    const Redis& redis,
    std::string_view stream,
    std::string_view group,
    const std::vector<StreamMessage>& messages
) {
    if (messages.empty()) {
        co_return;
    }
    static constexpr std::string_view script = R"lua(
local ids = {}
for index = 2, #ARGV do ids[#ids + 1] = ARGV[index] end
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], unpack(ids))
redis.call('XDEL', KEYS[1], unpack(ids))
return acknowledged
)lua";
    const std::string streamKey(stream);
    const std::string groupValue(group);
    const std::string_view keys[]{ streamKey };
    std::vector<std::string_view> arguments;
    arguments.reserve(messages.size() + 1);
    arguments.push_back(groupValue);
    for (const auto& message : messages) {
        arguments.push_back(message.id);
    }
    const auto reply = co_await redis.eval(script, keys, arguments);
    if (reply.kind() != redis_value::kind_type::integer) {
        throwValue("batch XACK/XDEL", reply);
    }
}

// ---- Hash 状态 ----

template <typename Redis>
ruvia::task<void> setHash(const Redis& redis, std::string_view key, const std::vector<StreamField>& fields) {
    if (fields.empty()) {
        co_return;
    }
    std::vector<std::string> args{ "HSET", std::string(key) };
    for (const auto& field : fields) {
        args.push_back(field.name);
        args.push_back(field.value);
    }
    const auto reply = co_await command(redis, args);
    if (reply.kind() != redis_value::kind_type::integer) {
        throwValue("HSET", reply);
    }
}

template <typename Redis>
ruvia::task<void> eraseHash(const Redis& redis, std::string_view key) {
    const auto reply = co_await command(redis, { "DEL", std::string(key) });
    if (reply.kind() != redis_value::kind_type::integer) {
        throwValue("DEL Hash", reply);
    }
}

template <typename Redis>
ruvia::task<std::vector<StreamField>> hashEntries(const Redis& redis, std::string_view key) {
    const auto entries = co_await command(redis, { "HGETALL", std::string(key) });
    if (entries.kind() != redis_value::kind_type::array) {
        throwValue("HGETALL", entries);
    }
    std::vector<StreamField> result;
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
        const auto page = co_await command(
            redis,
            { "SCAN", cursor, "MATCH", std::string(pattern), "COUNT", "100" }
        );
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

template <typename Redis>
ruvia::task<void> eraseMatching(const Redis& redis, std::string_view pattern) {
    for (const auto& key : co_await keysMatching(redis, pattern)) {
        (void)co_await command(redis, { "DEL", key });
    }
}

// ---- 删除 ----

template <typename Redis>
ruvia::task<void> erase(const Redis& redis, std::string_view key) {
    const auto reply = co_await command(redis, { "DEL", std::string(key) });
    if (reply.kind() != redis_value::kind_type::integer) {
        throwValue("DEL", reply);
    }
}

// ---- 生产者语义 ----

template <typename Redis>
ruvia::task<std::string> publish(const Redis& redis, std::string_view stream, const std::vector<StreamField>& fields, std::size_t maxLength = 0) {
    if (maxLength == 0) {
        maxLength = 10000;
    }
    co_return co_await add(redis, stream, fields, maxLength);
}

} // namespace service::message::redis
