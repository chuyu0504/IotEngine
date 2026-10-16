#include <iostream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include <ruvia/core/event_loop_pool.h>

#include "service/features/edge/edge.runtime.h"
#include "service/features/observability/observability.service.h"

#include "tests/redis_reply.fixture.h"

namespace {
static_assert(!std::is_default_constructible_v<service::edge::EdgeProjectionRuntime>);
static_assert(std::is_constructible_v<service::edge::EdgeProjectionRuntime, service::observability::RuntimeDiagnostics&>);

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

// Directed messages (RPC requests, RPC replies, terminal frames) are addressed
// to one worker of one process incarnation; two workers never share a stream.
void verifyDirectedStreams() {
    using service::message::live::workerStream;
    using service::message::rpc::Contract;
    if (workerStream("instance", 0) == workerStream("instance", 1) ||
        workerStream("instance", 0) == workerStream("other", 0) ||
        Contract::requests("instance", 0) == Contract::requests("instance", 1) ||
        Contract::requests("instance", 0) == Contract::requests("other", 0)) {
        throw std::runtime_error("directed streams are shared across workers or instances");
    }
}
} // namespace

int main() {
    try {
        verifyDirectedStreams();
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
