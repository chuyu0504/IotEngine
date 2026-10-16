#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <vector>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/common/worker.h"
#include "service/utils/change_feed.h"
#include "service/utils/number.h"
#include "service/utils/redis.h"

namespace service::utils {

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

} // namespace rpc_detail

// Module → feature request over Redis. The request goes to this Service Worker's
// own request stream; the consumer on the same worker sets the reply key and
// notifies this worker's directed stream, which the worker change feed delivers
// to this waiter only.
template <typename Context>
ruvia::task<std::string> callRpc(Context& context, std::string_view component, std::string_view operation, std::string payload) {
    using Contract = service::message::rpc::Contract;
    if (payload.size() > Contract::maximumPayload) {
        service::common::fail(10002, "RPC payload exceeds limit", 413);
    }
    const auto& topology = context.template worker_state<service::ServiceWorkerTopology>();
    if (!topology.index) {
        throw std::logic_error("service worker index is not initialized");
    }
    const auto id = context.template worker_state<std::unique_ptr<service::common::UuidV7Generator>>()->next();
    const auto replyKey = Contract::reply(id);
    const auto deadline = Contract::now() +
        std::chrono::duration_cast<std::chrono::milliseconds>(Contract::timeout).count();
    auto reply = co_await context.template worker_state<ChangeFeed>().await(context, replyKey);
    const auto requestStream = Contract::requests(service::runtime::instanceId(), *topology.index);
    std::exception_ptr failure;
    try {
        const auto requestArguments = Contract::requestArguments(requestStream, id, component, operation, std::move(payload), deadline);
        const auto enqueued = co_await service::message::redis::command(context.redis(), requestArguments);
        if (enqueued.kind() != ruvia::redis_value::kind_type::string) {
            service::message::redis::throwValue("RPC enqueue", enqueued);
        }
        while (!context.get_stop_token().stop_requested()) {
            const std::vector<std::string> replyArguments{ "GET", replyKey };
            const auto result = co_await service::message::redis::command(context.redis(), replyArguments);
            if (result.kind() == ruvia::redis_value::kind_type::string) {
                co_return rpc_detail::decodeReply(result.string());
            }
            if (result.kind() == ruvia::redis_value::kind_type::error) {
                service::message::redis::throwValue("RPC reply", result);
            }
            const auto remaining = deadline - Contract::now();
            if (remaining <= 0) {
                service::common::fail(10004, "Background operation timed out", 504);
            }
            (void)reply->take();
            (void)co_await reply->wait(std::chrono::milliseconds(remaining), context.get_stop_token());
        }
        service::common::fail(10004, "Background operation cancelled", 503);
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        const std::vector<std::string> cancelArguments{ "SET", Contract::cancelled(id), "1", "EX", std::string(Contract::replyLifetime) };
        const auto cancelled = co_await service::message::redis::command(context.redis(), cancelArguments);
        if (cancelled.kind() == ruvia::redis_value::kind_type::error) {
            service::message::redis::throwValue("RPC cancel", cancelled);
        }
    } catch (const std::exception& error) {
        // The persisted request deadline still cancels the operation; report
        // the lost cancellation instead of hiding it, then keep the original error.
        spdlog::warn("RPC {}.{} cancellation was not recorded: {}", component, operation, error.what());
    }
    std::rethrow_exception(failure);
}

} // namespace service::utils
