#include <iostream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include <ruvia/core/event_loop_pool.h>

#include "service/features/edge/edge.service.h"
#include "service/features/edge/projection/projection.runtime.h"
#include "service/features/observability/observability.service.h"

#include "tests/redis_reply.fixture.h"

namespace {
static_assert(!std::is_default_constructible_v<service::edge::projection::ProjectionRuntime>);
static_assert(std::is_constructible_v<service::edge::projection::ProjectionRuntime, service::observability::RuntimeDiagnostics&>);

struct RecordingRedis {
    mutable std::vector<std::string> keys;
    mutable std::vector<std::string> arguments;

    ruvia::task<ruvia::redis_value> eval(std::string_view, std::span<const std::string_view> inputKeys, std::span<const std::string_view> inputArguments) const {
        keys.assign(inputKeys.begin(), inputKeys.end());
        arguments.assign(inputArguments.begin(), inputArguments.end());
        co_return test::redisString("1-0", std::pmr::get_default_resource());
    }
};

ruvia::task<void> verifyMetadataOwner() {
    RecordingRedis redis;
    const auto published = co_await service::edge::projector_stream::publishMetadata(redis, 7, "node", "remote-instance");
    if (!published || redis.keys != std::vector<std::string>{ "iot:v3:edge:projector:lease:remote-instance:7", "iot:v3:edge:projector:remote-instance:7", "iot:v3:edge:projector:streams", "iot:service:worker:remote-instance:7:wake" } ||
        redis.arguments.empty() || redis.arguments[0] != "remote-instance:7") {
        throw std::runtime_error("metadata payload and wake did not retain the connection owner");
    }
}

// Requests stay on the accepting worker. Replies/frames have independent
// operation/session channels rather than a worker-wide notification stream.
void verifyDirectedAddresses() {
    using service::message::rpc::Contract;
    if (Contract::requests("instance", 0) == Contract::requests("instance", 1) ||
        Contract::requests("instance", 0) == Contract::requests("other", 0) ||
        Contract::reply("request-a") == Contract::reply("request-b") ||
        service::message::terminal::terminalEventTopic("node", "session-a") ==
            service::message::terminal::terminalEventTopic("node", "session-b") ||
        service::message::terminal::terminalEventTopic("node", "session-a") ==
            service::message::terminal::terminalAckTopic("node", "session-a")) {
        throw std::runtime_error("directed requests or operation channels overlap");
    }
}
} // namespace

int main() {
    try {
        verifyDirectedAddresses();
        ruvia::event_loop_pool pool({ .loop_count_ = 1, .queue_capacity_ = 64 });
        pool.start();
        pool.loop(0).start(verifyMetadataOwner()).get();
        pool.stop();
        pool.join();
        std::cout << "worker isolation tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "worker isolation test failed: " << error.what() << '\n';
        return 1;
    }
}
