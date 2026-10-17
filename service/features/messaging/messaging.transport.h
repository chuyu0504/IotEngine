#pragma once

#include <vector>

#include "service/common/message.h"
#include "service/features/messaging/messaging.types.h"
#include "service/utils/redis.h"

namespace service::messaging::stream {

inline constexpr std::string_view kAddAndWakeScript = R"lua(
local arguments = {'MAXLEN', '~', ARGV[1], '*'}
for index = 4, #ARGV do arguments[#arguments + 1] = ARGV[index] end
local id = redis.call('XADD', KEYS[1], unpack(arguments))
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', 'task', ARGV[3])
return id
)lua";

template <typename Redis>
ruvia::task<std::string> addAndWake(
    const Redis& redis,
    std::string_view stream,
    const std::vector<service::message::StreamField>& fields,
    std::optional<std::size_t> workerIndex,
    service::message::WorkerStreamTask task,
    std::size_t maxLength = 100000,
    std::string_view instance = service::runtime::instanceId()
) {
    if (fields.empty()) {
        throw std::invalid_argument("Redis Stream message must contain fields");
    }
    const std::vector<std::string> keyStore{
        std::string(stream),
        service::message::workerWakeStream(workerIndex, instance)
    };
    std::vector<std::string> argumentStore{
        std::to_string(maxLength),
        std::to_string(service::message::kWorkerWakeCapacity),
        std::string(service::message::workerStreamTaskName(task))
    };
    argumentStore.reserve(argumentStore.size() + fields.size() * 2);
    for (const auto& field : fields) {
        argumentStore.push_back(field.name);
        argumentStore.push_back(field.value);
    }
    const std::vector<std::string_view> keys(keyStore.begin(), keyStore.end());
    const std::vector<std::string_view> arguments(argumentStore.begin(), argumentStore.end());
    const auto reply = co_await redis.eval(kAddAndWakeScript, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::string) {
        service::message::redis::throwValue("atomic XADD/wake", reply);
    }
    co_return std::string(reply.string());
}

template <typename Pipeline>
void queueAddAndWake(Pipeline& pipeline, std::string_view stream, const std::vector<service::message::StreamField>& fields, std::optional<std::size_t> workerIndex, service::message::WorkerStreamTask task, std::size_t maxLength = 100000) {
    if (fields.empty()) {
        throw std::invalid_argument("Redis Stream message must contain fields");
    }
    const std::vector<std::string> keyStore{
        std::string(stream),
        service::message::workerWakeStream(workerIndex, service::runtime::instanceId())
    };
    std::vector<std::string> argumentStore{
        std::to_string(maxLength),
        std::to_string(service::message::kWorkerWakeCapacity),
        std::string(service::message::workerStreamTaskName(task))
    };
    argumentStore.reserve(argumentStore.size() + fields.size() * 2);
    for (const auto& field : fields) {
        argumentStore.push_back(field.name);
        argumentStore.push_back(field.value);
    }
    const std::vector<std::string_view> keys(keyStore.begin(), keyStore.end());
    const std::vector<std::string_view> arguments(argumentStore.begin(), argumentStore.end());
    pipeline.eval(kAddAndWakeScript, keys, arguments);
}

template <typename Redis>
ruvia::task<void> wakeWorker(const Redis& redis, std::optional<std::size_t> workerIndex, service::message::WorkerStreamTask task) {
    const std::vector<service::message::StreamField> fields{{ "task", std::string(service::message::workerStreamTaskName(task)) }};
    (void)co_await service::message::redis::add(redis, service::message::workerWakeStream(workerIndex, service::runtime::instanceId()), fields, service::message::kWorkerWakeCapacity);
}

template <typename Redis>
ruvia::task<std::string> publishAndWake(
    const Redis& redis,
    std::string_view stream,
    const std::vector<service::message::StreamField>& fields,
    std::optional<std::size_t> workerIndex,
    service::message::WorkerStreamTask task,
    std::size_t maxLength = 0,
    std::string_view instance = service::runtime::instanceId()
) {
    if (maxLength == 0) {
        maxLength = 10000;
    }
    co_return co_await addAndWake(redis, stream, fields, workerIndex, task, maxLength, instance);
}

template <typename Redis>
ruvia::task<bool> publishAllAndAcknowledge(
    const Redis& redis,
    std::span<const StreamPublication> publications,
    std::string_view inputStream,
    std::string_view inputGroup,
    std::string_view inputConsumer,
    std::string_view inputId
) {
    if (publications.empty()) {
        throw std::invalid_argument("Redis Stream transaction must contain publications");
    }
    for (const auto& publication : publications) {
        if (publication.fields.empty()) {
            throw std::invalid_argument("Redis Stream message must contain fields");
        }
    }
    static constexpr std::string_view script = R"lua(
local input_key = KEYS[#KEYS]
local pending = redis.call('XPENDING', input_key, ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then return 0 end
local output_count = (#KEYS - 1) / 2
local argument = 4
for output = 1, output_count do
  local output_key = KEYS[(output - 1) * 2 + 1]
  local wake_key = KEYS[(output - 1) * 2 + 2]
  local output_type = redis.call('TYPE', output_key).ok
  if output_type ~= 'none' and output_type ~= 'stream' then
    return redis.error_reply('output key is not a stream')
  end
  local field_count = tonumber(ARGV[argument + 1])
  local wake_task = ARGV[argument + 2]
  if wake_task ~= '' then
    local wake_type = redis.call('TYPE', wake_key).ok
    if wake_type ~= 'none' and wake_type ~= 'stream' then
      return redis.error_reply('wake key is not a stream')
    end
  end
  argument = argument + 3 + field_count * 2
end

argument = 4
for output = 1, output_count do
  local maximum = ARGV[argument]
  local field_count = tonumber(ARGV[argument + 1])
  local wake_task = ARGV[argument + 2]
  argument = argument + 3
  local fields = {}
  for field = 1, field_count do
    fields[#fields + 1] = ARGV[argument]
    fields[#fields + 1] = ARGV[argument + 1]
    argument = argument + 2
  end
  redis.call('XADD', KEYS[(output - 1) * 2 + 1],
             'MAXLEN', '~', maximum, '*', unpack(fields))
  if wake_task ~= '' then
    redis.call('XADD', KEYS[(output - 1) * 2 + 2],
               'MAXLEN', '~', '100000', '*', 'task', wake_task)
  end
end

local acknowledged = redis.call('XACK', input_key, ARGV[1], ARGV[2])
if acknowledged ~= 1 then
  return redis.error_reply('input stream entry was not pending')
end
redis.call('XDEL', input_key, ARGV[2])
return 1
)lua";

    std::vector<std::string> ownedKeys;
    ownedKeys.reserve(publications.size() * 2 + 1);
    for (const auto& publication : publications) {
        ownedKeys.emplace_back(publication.stream);
        ownedKeys.push_back(publication.wake ? service::message::workerWakeStream(publication.wake->workerIndex, service::runtime::instanceId()) : std::string(publication.stream));
    }
    ownedKeys.emplace_back(inputStream);
    const std::vector<std::string_view> keys(ownedKeys.begin(), ownedKeys.end());

    std::size_t argumentCount = 3;
    for (const auto& publication : publications) {
        argumentCount += 3 + publication.fields.size() * 2;
    }
    std::vector<std::string> ownedArguments;
    ownedArguments.reserve(argumentCount);
    ownedArguments.emplace_back(inputGroup);
    ownedArguments.emplace_back(inputId);
    ownedArguments.emplace_back(inputConsumer);
    for (const auto& publication : publications) {
        ownedArguments.push_back(
            std::to_string(publication.maxLength == 0 ? 10000 : publication.maxLength)
        );
        ownedArguments.push_back(std::to_string(publication.fields.size()));
        ownedArguments.push_back(
            publication.wake
                ? std::string(service::message::workerStreamTaskName(publication.wake->task))
                : std::string{}
        );
        for (const auto& field : publication.fields) {
            ownedArguments.push_back(field.name);
            ownedArguments.push_back(field.value);
        }
    }
    const std::vector<std::string_view> arguments(ownedArguments.begin(), ownedArguments.end());
    const auto reply = co_await redis.eval(script, keys, arguments);
    if (reply.kind() != ruvia::redis_value::kind_type::integer) {
        service::message::redis::throwValue("atomic XPENDING/XADD/XACK/XDEL", reply);
    }
    co_return reply.integer() == 1;
}

template <typename Redis>
ruvia::task<bool> publishAndAcknowledge(
    const Redis& redis,
    std::string_view outputStream,
    const std::vector<service::message::StreamField>& outputFields,
    std::size_t outputMaxLength,
    std::string_view inputStream,
    std::string_view inputGroup,
    std::string_view inputConsumer,
    std::string_view inputId,
    std::optional<StreamWake> wake = std::nullopt
) {
    const StreamPublication publication{ outputStream, outputFields, outputMaxLength, wake };
    co_return co_await publishAllAndAcknowledge(
        redis,
        std::span<const StreamPublication>(&publication, 1),
        inputStream,
        inputGroup,
        inputConsumer,
        inputId
    );
}

} // namespace service::messaging::stream
