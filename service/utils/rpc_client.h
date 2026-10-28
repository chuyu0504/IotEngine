#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/redis/redis.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"
#include "service/utils/uuid.h"

namespace service::utils {

inline constexpr std::string_view kCancelRpcScript = R"lua(
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
return redis.call('PUBLISH', KEYS[2], ARGV[2])
)lua";

inline constexpr std::string_view kEnqueueRpcScript = R"lua(
local id = redis.call('XADD', KEYS[1], '*', unpack(ARGV))
redis.call('PUBLISH', KEYS[2], id)
return id
)lua";

namespace rpc_detail {

inline std::string decodeReply(std::string_view reply) {
    if (reply.starts_with("OK\n")) {
        return std::string(reply.substr(3));
    }
    if (reply.starts_with("ERR\n")) {
        reply.remove_prefix(4);
        const auto statusEnd = reply.find('\n');
        if (statusEnd != std::string_view::npos) {
            const auto status = service::utils::parseInt64(reply.substr(0, statusEnd));
            reply.remove_prefix(statusEnd + 1);
            const auto codeEnd = reply.find('\n');
            if (status && *status >= 400 && *status <= 599 && codeEnd != std::string_view::npos) {
                throw ruvia::http_error(ruvia::http_error_info_options{ .status_ = ruvia::http_status_code::from_value(static_cast<std::uint16_t>(*status)), .code_ = reply.substr(0, codeEnd), .message_ = reply.substr(codeEnd + 1) });
            }
        }
    }
    service::common::fail(10004, "Invalid background operation response", 502);
}

inline ruvia::task<void> expireAtDeadline(const ruvia::worker_handle& worker, std::int64_t deadline, ruvia::stop_source& expired, ruvia::stop_token stop) {
    const auto delay = std::chrono::milliseconds(std::max<std::int64_t>(0, deadline - service::message::rpc::Contract::now()));
    (void)co_await ruvia::sleep_for(worker, delay, stop);
    if (!stop.stop_requested()) {
        expired.request_stop();
    }
}

template <typename Context>
ruvia::task<std::optional<std::string>> waitForReply(Context& context, const ruvia::worker_handle& worker, const std::string& requestStream, const std::string& id, const std::string& replyKey, std::string_view component, std::string_view operation, std::string payload, std::int64_t deadline, ruvia::stop_token stop) {
    const auto redis = context.redis().with_options({ .stop_token_ = stop });
    bool enqueued = false;
    while (!stop.stop_requested()) {
        bool sendingRequest = false;
        try {
            auto notifications = redis.subscription();
            const std::string_view channels[]{ replyKey };
            co_await notifications.subscribe(channels);
            bool confirmed = false;
            while (const auto event = co_await notifications.read()) {
                if (event->kind() == ruvia::redis_pubsub_event_kind::subscribed && event->channel() == replyKey) {
                    confirmed = true;
                    break;
                }
            }
            if (!confirmed) {
                if (stop.stop_requested()) {
                    break;
                }
                throw ruvia::redis_error(ruvia::redis_error::code_type::io_error, "RPC reply subscription closed before confirmation");
            }
            if (!enqueued) {
                const auto requestArguments = service::message::rpc::Contract::requestArguments(requestStream, id, component, operation, std::move(payload), deadline);
                const auto inputChannel = service::message::consumer::inputChannel(context.template worker_state<std::optional<service::common::WorkerIdentity>>().value().channels, requestStream);
                const std::string_view keys[]{ requestStream, inputChannel };
                const std::vector<std::string_view> arguments(requestArguments.begin() + 3, requestArguments.end());
                // An uncertain append is never repeated. Its persisted deadline
                // and cancellation fence bound execution even if its reply is lost.
                sendingRequest = true;
                const auto result = co_await redis.eval(kEnqueueRpcScript, keys, arguments);
                if (result.kind() != ruvia::redis_value::kind_type::string) {
                    service::utils::redis::throwValue("RPC enqueue", result);
                }
                enqueued = true;
                sendingRequest = false;
            }
            while (!stop.stop_requested()) {
                // Confirm before this read: it closes both the initial request
                // race and the lost-notification window on subscription recovery.
                if (const auto result = co_await redis.get(replyKey)) {
                    co_return std::string(*result);
                }
                const auto event = co_await notifications.read();
                if (!event || event->kind() == ruvia::redis_pubsub_event_kind::unsubscribed) {
                    break;
                }
            }
        } catch (const ruvia::redis_error& error) {
            if (stop.stop_requested()) {
                break;
            }
            if (sendingRequest ||
                (error.code() != ruvia::redis_error::code_type::io_error &&
                 error.code() != ruvia::redis_error::code_type::connect_failed &&
                 error.code() != ruvia::redis_error::code_type::timeout)) {
                throw;
            }
            spdlog::warn("RPC {}.{} subscription reconnecting: {}", component, operation, error.what());
        }
        if (!stop.stop_requested()) {
            (void)co_await ruvia::sleep_for(worker, std::chrono::milliseconds(100), stop);
        }
    }
    co_return std::nullopt;
}

} // namespace rpc_detail

// A stateless call: all subscription, deadline and response resources belong to
// the accepting request. Durable replies remain authoritative, Pub/Sub is wakeup only.
template <typename Context>
ruvia::task<std::string> callRpc(Context& context, std::string_view component, std::string_view operation, std::string payload) {
    using Contract = service::message::rpc::Contract;
    if (payload.size() > Contract::maximumPayload) {
        service::common::fail(10002, "RPC payload exceeds limit", 413);
    }
    const auto& identity = context.template worker_state<std::optional<service::common::WorkerIdentity>>().value();
    const auto id = service::utils::nextUuid(context.template worker_state<service::common::UuidV7State>());
    const auto replyKey = Contract::reply(id);
    const auto requestStream = Contract::requests(identity.instanceId, identity.index);
    const auto deadline = Contract::now() + std::chrono::duration_cast<std::chrono::milliseconds>(Contract::timeout).count();
    // Keep the public capability address-stable until the deadline task is joined.
    const auto& worker = context.worker();
    ruvia::stop_source expired;
    ruvia::task_scope timer(worker);
    const auto stop = ruvia::combine_stop_tokens(context.stop_token(), expired.token());
    timer.spawn(rpc_detail::expireAtDeadline(worker, deadline, expired, ruvia::combine_stop_tokens(context.stop_token(), timer.get_stop_token())));
    std::optional<std::string> response;
    std::exception_ptr failure;
    try {
        response = co_await rpc_detail::waitForReply(context, worker, requestStream, id, replyKey, component, operation, std::move(payload), deadline, stop);
    } catch (...) {
        failure = std::current_exception();
    }
    timer.request_stop();
    co_await timer.join();
    if (!response && !context.stop_token().stop_requested() && expired.token().stop_requested()) {
        // A result may have committed at the deadline while read() was stopped.
        try {
            if (const auto result = co_await context.redis().get(replyKey)) {
                response = std::string(*result);
            }
        } catch (const std::exception& error) {
            spdlog::warn("RPC {}.{} final persisted reply read failed: {}", component, operation, error.what());
        }
    }
    if (response) {
        co_return rpc_detail::decodeReply(*response);
    }
    try {
        const auto cancelledKey = Contract::cancelled(id);
        const std::string_view keys[]{ cancelledKey, requestStream };
        const std::string_view args[]{ Contract::replyLifetime, id };
        const auto cancelled = co_await context.redis().eval(kCancelRpcScript, keys, args);
        if (cancelled.kind() != ruvia::redis_value::kind_type::integer) {
            service::utils::redis::throwValue("RPC cancellation", cancelled);
        }
    } catch (const std::exception& error) {
        spdlog::warn("RPC {}.{} cancellation was not recorded: {}", component, operation, error.what());
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
    service::common::fail(10004, context.stop_token().stop_requested() ? "Background operation cancelled" : "Background operation timed out", context.stop_token().stop_requested() ? 503 : 504);
}

} // namespace service::utils
