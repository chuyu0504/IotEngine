#include <iostream>
#include <map>
#include <stdexcept>
#include <string>

#include <ruvia/core/event_loop_pool.h>
#include "tests/redis_reply.fixture.h"
#include "service/features/collector/collector.service.h"

namespace {
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

struct TransmissionRedis {
    mutable std::vector<std::string> arguments;
    enum class Reply { Success, Duplicate, Error } reply = Reply::Success;
    ruvia::task<ruvia::redis_value> command(std::span<const std::string_view> values) const {
        arguments.assign(values.begin(), values.end());
        const auto resource = std::pmr::get_default_resource();
        if (reply == Reply::Duplicate) co_return test::redisNull(resource);
        if (reply == Reply::Error) co_return test::redisError("injected transmission failure", resource);
        co_return test::redisString("OK", resource);
    }
};

ruvia::task<void> verifyTransmissionReservation() {
    using service::collector::CollectorCommandService;
    using service::collector::TransmissionReservation;
    TransmissionRedis redis;
    service::message::ProtocolTask task;
    task.messageId = "command-a";
    task.createdAtMs = 100000;
    require(co_await CollectorCommandService::reserveTransmission(redis, task, 160000) ==
                TransmissionReservation::Expired && redis.arguments.empty(),
            "expired command reserved transmission");
    require(co_await CollectorCommandService::reserveTransmission(redis, task, 159999) ==
                TransmissionReservation::Reserved,
            "command just before its deadline was rejected");
    require(redis.arguments == std::vector<std::string>{"SET", "iot:v2:command:sent:command-a", "1", "NX", "EX", "86400"},
            "transmission reservation changed atomicity, key or retention");
    redis.reply = TransmissionRedis::Reply::Duplicate;
    require(co_await CollectorCommandService::reserveTransmission(redis, task, 159999) ==
                TransmissionReservation::Duplicate,
            "duplicate transmission was accepted");
    redis.reply = TransmissionRedis::Reply::Error;
    bool rejected = false;
    try { (void)co_await CollectorCommandService::reserveTransmission(redis, task, 159999); }
    catch (const std::runtime_error&) { rejected = true; }
    require(rejected, "Redis failure allowed transmission");
}

// Redis HSET applies repeated field names in argument order.
std::map<std::string, std::string> hash(const std::vector<service::message::StreamField>& fields) {
    std::map<std::string, std::string> result;
    for (const auto& field : fields) result.insert_or_assign(field.name, field.value);
    return result;
}
}

int main() {
    try {
        ruvia::event_loop_pool pool({.loop_count_ = 1});
        pool.start();
        pool.loop(0).start(verifyTransmissionReservation()).get();
        pool.stop();
        pool.join();
        const service::message::StreamMessage event{
            "100-0", {{"message_id", "source-id"}, {"created_at_ms", "10"},
                      {"link_id", "link-a"}, {"state", "ready"},
                      {"remote_address", "[::1]:5000"}, {"updated_at_ms", "11"},
                      {"vendor_field", std::string("a\0b", 3)}}};
        const auto snapshot = service::collector::CollectorLinkRecord::fromEvent(event, 1700000000000);
        const auto stored = hash(snapshot.fields);
        require(!stored.contains("message_id") && !stored.contains("created_at_ms"),
                "link snapshot leaked message metadata");
        require(stored.at("updated_at_ms") == "1700000000000",
                "source event timestamp overrode snapshot time");
        require(stored.at("remote_address") == "[::1]:5000" &&
                    stored.at("vendor_field") == std::string("a\0b", 3),
                "link snapshot lost dynamic or binary field content");
        require(event.get("updated_at_ms") == "11", "mapping modified the source event");
        const service::collector::CollectorWorkerRecord worker{3, "version-a", "applied", 1700000000000};
        const auto workerHash = hash(worker.fields());
        require(workerHash.size() == 4 && workerHash.at("worker_id") == "3" &&
                    workerHash.at("version") == "version-a" && workerHash.at("state") == "applied" &&
                    workerHash.at("applied_at_ms") == "1700000000000",
                "worker snapshot changed the stored field contract");
        constexpr std::string_view instance = "01970000-1234-7000-8000-000000000123";
        constexpr std::string_view restarted = "01970000-1234-7000-8000-000000000124";
        require(service::collector::CollectorLinkRecord::key("link-a", 2, instance) !=
                    service::collector::CollectorLinkRecord::key("link-a", 3, instance),
                "link snapshots collide between workers");
        require(service::collector::CollectorLinkRecord::key("link-a", 2, instance) ==
                    "iot:runtime:link:link-a:worker:01970000-1234-7000-8000-000000000123:2",
                "link snapshot persisted key changed");
        require(service::collector::CollectorWorkerRecord::key(2, instance) ==
                    "iot:runtime:collector:01970000-1234-7000-8000-000000000123:2",
                "worker snapshot persisted key changed");
        require(service::collector::CollectorLinkRecord::key("link-a", 2, instance) !=
                    service::collector::CollectorLinkRecord::key("link-a", 2, restarted) &&
                    service::collector::CollectorWorkerRecord::key(2, instance) !=
                    service::collector::CollectorWorkerRecord::key(2, restarted),
                "collector snapshots collide between process incarnations");
        std::cout << "collector state mapping tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
