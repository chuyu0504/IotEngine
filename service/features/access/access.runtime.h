#pragma once

#include <algorithm>
#include <array>
#include <asio.hpp>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/one_shot.h>
#include <ruvia/core/task.h>
#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/db/db.h>
#include <ruvia/web/web_worker.h>

#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/features/access/access.service.h"
#include "service/features/access/access.transport.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/messaging/stream_multiplexer/stream_multiplexer.runtime.h"
#include "service/utils/crypto.h"
#include "service/utils/json.h"
#include "service/utils/text.h"

namespace service::access {

class DeliveryRuntime final {
  public:
    DeliveryRuntime() = default;
    DeliveryRuntime(const DeliveryRuntime&) = delete;
    DeliveryRuntime& operator=(const DeliveryRuntime&) = delete;

    ~DeliveryRuntime() { stop(); }

    void start(ruvia::web_worker_handle worker, std::size_t workerIndex, std::size_t serviceWorkerCount) {
        if (running_.exchange(true)) {
            return;
        }
        worker_ = std::move(worker);
        workerIndex_ = workerIndex;
        serviceWorkerCount_ = serviceWorkerCount;
        if (!worker_.valid() || serviceWorkerCount_ == 0 ||
            workerIndex_ >= serviceWorkerCount_) {
            running_.store(false);
            worker_ = {};
            throw std::runtime_error("access webhook runtime requires valid Service Workers");
        }
        auto ready = std::make_shared<std::promise<void>>();
        auto stopped = std::make_shared<std::promise<void>>();
        auto readiness = ready->get_future();
        stopped_ = stopped->get_future().share();
        const auto posted = worker_.post(
            [this, ready, stopped](ruvia::web_worker_context& context) {
                return run(context, workerIndex_, ready, stopped);
            }
        );
        if (!posted.accepted()) {
            stopped->set_value();
            running_.store(false);
            worker_ = {};
            stopped_ = {};
            throw std::runtime_error("service worker rejected access webhook runtime");
        }
        try {
            readiness.get();
        } catch (...) {
            stop();
            throw;
        }
    }

    void stop() noexcept {
        if (!running_.exchange(false)) {
            return;
        }
        if (worker_.valid()) {
            const auto worker = worker_;
            const auto wakeAccepted = worker.post([](ruvia::web_worker_context&) -> ruvia::task<void> {
                                                service::messaging::workerStreamMultiplexer().signal(
                                                    service::message::WorkerStreamTask::Webhook
                                                );
                                                co_return;
                                            })
                                          .accepted();
            if (!wakeAccepted) {
                // A closed worker already propagates its stop token to the task.
            }
        }
        if (stopped_.valid()) {
            stopped_.wait();
        }
        stopped_ = {};
        worker_ = {};
    }

  private:
    static constexpr std::string_view kGroup = "iot-engine:open-webhook";
    static constexpr std::size_t kBatchSize = 100;
    static constexpr std::size_t kEventConcurrency = 4;
    static constexpr std::size_t kTargetConcurrency = 4;

    using Delivery = delivery::Delivery;
    using Target = delivery::Target;
    using Catalog = delivery::Catalog;
    using DeviceCatalog = delivery::DeviceCatalog;

    enum class StreamKind {
        Catalog,
        Session,
        DeliveryResult,
        Audit,
        Event
    };

    ruvia::task<void> run(ruvia::web_worker_context& context, std::size_t index, std::shared_ptr<std::promise<void>> ready, std::shared_ptr<std::promise<void>> stopped) {
        try {
            const auto redis = context.redis();
            std::map<std::string, StreamKind, std::less<>> streamKinds;
            streamKinds.emplace(
                service::message::access::stream::catalogChanges(index),
                StreamKind::Catalog
            );
            streamKinds.emplace(service::message::access::stream::sessionChanges(), StreamKind::Session);
            streamKinds.emplace(service::message::access::stream::deliveryResult(), StreamKind::DeliveryResult);
            streamKinds.emplace(service::message::access::stream::audit(), StreamKind::Audit);
            streamKinds.emplace(service::message::access::stream::event(), StreamKind::Event);
            std::vector<std::string> streams;
            streams.reserve(streamKinds.size());
            for (const auto& [streamName, kind] : streamKinds) {
                (void)kind;
                co_await message::redis::ensureGroup(redis, streamName, kGroup);
                streams.push_back(streamName);
            }
            DeliveryClient http;
            co_await session::ensure(context);
            auto catalog = co_await delivery::loadCatalog(context);
            ready->set_value();
            bool recovering = true;
            const auto consumer = service::runtime::instanceId() + ":service-" + std::to_string(index);
            const auto catalogReceiptConsumer =
                std::string(kGroup) + ":catalog:" + std::to_string(index);
            const auto sessionReceiptConsumer = std::string(kGroup) + ":session";
            static constexpr auto kPendingRecoveryInterval =
                std::chrono::milliseconds(5000);
            auto nextRecoverySweep = std::chrono::steady_clock::now();
            while (running_.load() && !context.get_stop_token().stop_requested()) {
                std::vector<message::redis::StreamBatch> batches;
                bool readFailed = false;
                bool groupMissing = false;
                try {
                    if (recovering ||
                        std::chrono::steady_clock::now() >= nextRecoverySweep) {
                        batches = co_await message::redis::claimGroupMany(
                            redis,
                            streams,
                            kGroup,
                            consumer,
                            kBatchSize
                        );
                        nextRecoverySweep = std::chrono::steady_clock::now() +
                            kPendingRecoveryInterval;
                        if (batches.empty()) {
                            // A wake may coincide with pending recovery. An empty
                            // pending set must still drain newly published work.
                            recovering = false;
                            batches = co_await message::redis::readGroupMany(
                                redis,
                                streams,
                                kGroup,
                                consumer,
                                ">",
                                kBatchSize
                            );
                        }
                    } else {
                        batches = co_await message::redis::readGroupMany(
                            redis,
                            streams,
                            kGroup,
                            consumer,
                            ">",
                            kBatchSize
                        );
                    }
                } catch (const std::exception& error) {
                    if (!running_.load() || context.get_stop_token().stop_requested()) {
                        break;
                    }
                    std::cerr << "open webhook stream read failed: " << error.what() << '\n';
                    recovering = true;
                    readFailed = true;
                    groupMissing = std::string_view(error.what()).find("NOGROUP") !=
                        std::string_view::npos;
                }
                if (readFailed && running_.load() &&
                    !context.get_stop_token().stop_requested()) {
                    try {
                        if (groupMissing) {
                            for (const auto& streamName : streams) {
                                co_await message::redis::ensureGroup(
                                    redis,
                                    streamName,
                                    kGroup
                                );
                            }
                            readFailed = false;
                        }
                    } catch (const std::exception& ensureError) {
                        std::cerr << "open webhook stream group recovery failed: "
                                  << ensureError.what() << '\n';
                    }
                }
                if (!running_.load() || context.get_stop_token().stop_requested()) {
                    break;
                }
                if (readFailed) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::milliseconds(250));
                    continue;
                }
                if (batches.empty()) {
                    co_await service::messaging::workerStreamMultiplexer().wait(
                        index,
                        service::message::WorkerStreamTask::Webhook,
                        context.get_stop_token(),
                        kPendingRecoveryInterval
                    );
                    continue;
                }
                bool failed = false;
                try {
                    std::vector<message::StreamMessage> sessionMessages;
                    for (const auto& batch : batches) {
                        const auto kind = streamKinds.at(batch.stream);
                        if (kind != StreamKind::Session) {
                            continue;
                        }
                        sessionMessages.insert(sessionMessages.end(), batch.messages.begin(), batch.messages.end());
                    }
                    co_await session::applyChanges(context, sessionReceiptConsumer, sessionMessages);

                    std::vector<message::StreamMessage> catalogMessages;
                    for (const auto& batch : batches) {
                        const auto kind = streamKinds.at(batch.stream);
                        if (kind != StreamKind::Catalog) {
                            continue;
                        }
                        catalogMessages.insert(catalogMessages.end(), batch.messages.begin(), batch.messages.end());
                    }
                    catalog = co_await delivery::reloadCatalog(context, catalogReceiptConsumer, catalogMessages);
                    for (const auto& batch : batches) {
                        const auto kind = streamKinds.at(batch.stream);
                        if (kind != StreamKind::Catalog &&
                            kind != StreamKind::Session) {
                            continue;
                        }
                        co_await message::redis::acknowledgeAndDeleteMany(
                            redis,
                            batch.stream,
                            kGroup,
                            batch.messages
                        );
                    }
                    for (const auto& batch : batches) {
                        if (streamKinds.at(batch.stream) != StreamKind::DeliveryResult) {
                            continue;
                        }
                        co_await delivery::persistResults(context, batch.messages);
                        co_await message::redis::acknowledgeAndDeleteMany(
                            redis,
                            batch.stream,
                            kGroup,
                            batch.messages
                        );
                    }
                    for (const auto& batch : batches) {
                        if (streamKinds.at(batch.stream) != StreamKind::Audit) {
                            continue;
                        }
                        co_await delivery::persistAudits(context, batch.messages);
                        co_await message::redis::acknowledgeAndDeleteMany(
                            redis,
                            batch.stream,
                            kGroup,
                            batch.messages
                        );
                    }
                    for (const auto& batch : batches) {
                        if (streamKinds.at(batch.stream) != StreamKind::Event) {
                            continue;
                        }
                        co_await deliverEvents(context, batch.stream, batch.messages, catalog, http);
                    }
                } catch (const std::exception& error) {
                    std::cerr << "open webhook dispatch failed: " << error.what() << '\n';
                    recovering = true;
                    failed = true;
                }
                if (failed) {
                    (void)co_await ruvia::sleep_for(context.worker(), std::chrono::seconds(5));
                }
            }
        } catch (...) {
            try {
                ready->set_exception(std::current_exception());
            } catch (...) {
            }
        }
        try {
            stopped->set_value();
        } catch (...) {
        }
    }

    struct DeliveryAttempt final {
        bool succeeded{ false };
        std::string error;
    };

    struct DeviceEventQueue final {
        std::vector<std::size_t> indexes;
        std::size_t next{ 0 };
        bool blocked{ false };
    };

    ruvia::task<void> deliverCaptured(ruvia::web_worker_context& context, const message::StreamMessage& message, const Catalog& catalog, DeliveryClient& http, DeliveryAttempt& attempt) {
        try {
            co_await deliver(context, message, catalog, http);
            attempt.succeeded = true;
        } catch (const std::exception& error) {
            attempt.error = error.what();
        } catch (...) {
            attempt.error = "unknown webhook delivery failure";
        }
    }

    ruvia::task<void> deliverEvents(
        ruvia::web_worker_context& context,
        std::string_view sourceStream,
        const std::vector<message::StreamMessage>& messages,
        const Catalog& catalog,
        DeliveryClient& http
    ) {
        if (messages.empty()) {
            co_return;
        }
        std::map<std::string, DeviceEventQueue, std::less<>> queues;
        for (std::size_t index = 0; index < messages.size(); ++index) {
            auto key = std::string(messages[index].get("device_id"));
            if (key.empty()) {
                key = "invalid:" + std::to_string(index);
            }
            queues[key].indexes.push_back(index);
        }

        std::vector<message::StreamMessage> completed;
        completed.reserve(messages.size());
        std::string firstError;
        while (true) {
            std::vector<DeviceEventQueue*> selected;
            selected.reserve(kEventConcurrency);
            for (auto& [deviceId, queue] : queues) {
                (void)deviceId;
                if (!queue.blocked && queue.next < queue.indexes.size()) {
                    selected.push_back(&queue);
                }
                if (selected.size() == kEventConcurrency) {
                    break;
                }
            }
            if (selected.empty()) {
                break;
            }

            std::vector<DeliveryAttempt> attempts(selected.size());
            ruvia::task_scope scope(
                context.worker(),
                ruvia::task_scope_options{ .resource_ = context.pool() }
            );
            for (std::size_t index = 0; index < selected.size(); ++index) {
                const auto messageIndex = selected[index]->indexes[selected[index]->next];
                scope.spawn(deliverCaptured(context, messages[messageIndex], catalog, http, attempts[index]));
            }
            co_await scope.join();
            for (std::size_t index = 0; index < selected.size(); ++index) {
                auto& queue = *selected[index];
                const auto messageIndex = queue.indexes[queue.next];
                if (attempts[index].succeeded) {
                    completed.push_back(messages[messageIndex]);
                    ++queue.next;
                } else {
                    queue.blocked = true;
                    if (firstError.empty()) {
                        firstError = std::move(attempts[index].error);
                    }
                }
            }
        }
        if (!completed.empty()) {
            co_await message::redis::acknowledgeAndDeleteMany(
                context.redis(),
                sourceStream,
                kGroup,
                completed
            );
        }
        if (!firstError.empty()) {
            throw std::runtime_error(firstError);
        }
    }

    ruvia::task<void> deliver(ruvia::web_worker_context& context, const message::StreamMessage& message, const Catalog& catalog, DeliveryClient& http) {
        const auto eventType = message.get("event_type");
        const auto deviceId = message.get("device_id");
        if (!service::message::supportedEvent(eventType) || !service::common::isUuid(deviceId)) {
            co_return;
        }
        const auto device = catalog.find(deviceId);
        if (device == catalog.end()) {
            co_return;
        }
        const auto targetEntry = device->second.targets.find(eventType);
        if (targetEntry == device->second.targets.end()) {
            co_return;
        }
        const auto now = service::message::utcNowMilliseconds();
        std::vector<const Target*> targets;
        targets.reserve(targetEntry->second.size());
        for (const auto& target : targetEntry->second) {
            if (target.expiresAtMs == 0 || target.expiresAtMs > now) {
                targets.push_back(&target);
            }
        }
        if (targets.empty()) {
            co_return;
        }
        const auto delivery = co_await delivery::DeliveryService::buildDelivery(context, message, device->second);
        const auto completed = co_await delivery::DeliveryService::completedTargets(context.redis(), delivery);
        std::erase_if(targets, [&completed](const Target* target) {
            return completed.contains(target->id);
        });
        if (targets.empty()) {
            co_return;
        }
        for (std::size_t offset = 0; offset < targets.size(); offset += kTargetConcurrency) {
            const auto end = std::min(targets.size(), offset + kTargetConcurrency);
            ruvia::task_scope scope(
                context.worker(),
                ruvia::task_scope_options{ .resource_ = context.pool() }
            );
            for (auto index = offset; index < end; ++index) {
                scope.spawn(deliverTarget(context, http, *targets[index], delivery));
            }
            co_await scope.join();
        }
    }

    ruvia::task<void> deliverTarget(ruvia::web_worker_context& context, DeliveryClient& http, const Target& target, const Delivery& delivery) {
        if (target.mqtt) {
            DeliveryResponse response;
            try {
                auto [completion, receiver] = ruvia::make_one_shot<DeliveryResponse>(context.worker());
                auto shared = std::make_shared<ruvia::one_shot_completion<DeliveryResponse>>(std::move(completion));
                http.publishMqtt(service::utils::mqtt::parseBroker(target.url),
                    "iot-" + service::utils::sha256(target.id + delivery.id).substr(0, 19),
                    target.mqtt->username, target.mqtt->password,
                    service::message::openMqttTopic(delivery.deviceId),
                    delivery.body, std::chrono::seconds(target.timeout),
                    [shared](DeliveryResponse result) mutable { (void)shared->complete(std::move(result)); });
                auto outcome = co_await receiver.wait();
                if (outcome.has_value()) response = std::move(outcome).take_value();
                else response.error = "MQTT publish was cancelled";
            } catch (const std::exception& error) {
                response.error = error.what();
            }
            const bool success = response.error.empty();
            co_await delivery::DeliveryService::enqueueResult(context, target, delivery, response, success);
            if (!success) throw std::runtime_error(response.error);
            co_return;
        }
        const auto requestTimestamp = service::common::utcTimestampNow();
        auto headers = parseHeaders(target.headers);
        headers.emplace_back("X-IOT-Event", delivery.eventType);
        headers.emplace_back("X-IOT-Timestamp", requestTimestamp);
        headers.emplace_back("X-IOT-Delivery", delivery.id);
        if (!target.secret.empty()) {
            headers.emplace_back("X-IOT-Signature", "sha256=" + service::utils::hmacSha256(target.secret, delivery.body));
        }

        DeliveryResponse response;
        try {
            const auto url = parseWebhookUrl(target.url);
            auto [completion, receiver] = ruvia::make_one_shot<DeliveryResponse>(context.worker());
            auto shared = std::make_shared<ruvia::one_shot_completion<DeliveryResponse>>(
                std::move(completion)
            );
            http.post(url, DeliveryClient::request(url, delivery.body, headers), std::chrono::seconds(target.timeout), target.skipTlsVerify, [shared](DeliveryResponse result) mutable {
                (void)shared->complete(std::move(result));
            });
            auto outcome = co_await receiver.wait();
            if (outcome.has_value()) {
                response = std::move(outcome).take_value();
            } else {
                response.error = "Webhook request was cancelled";
            }
        } catch (const std::exception& error) {
            response.error = error.what();
        }
        const bool success =
            response.error.empty() && response.status >= 200 && response.status < 300;
        if (!success && response.error.empty()) {
            response.error =
                "HTTP " + std::to_string(response.status) + " " + service::utils::sanitize(response.body, 500);
        }
        co_await delivery::DeliveryService::enqueueResult(context, target, delivery, response, success);
    }

    static std::vector<std::pair<std::string, std::string>> parseHeaders(std::string_view json) {
        std::vector<std::pair<std::string, std::string>> result;
        const auto object = ruvia::json_value::parse(json);
        if (!object || !object->is_object()) return result;
        (void)object->for_each_field(
            [&](std::string_view name, const ruvia::json_value&) {
                const auto value = object->get<ruvia::string>(name);
                if (value) {
                    validateWebhookHeader(name, value->view(), true);
                    result.emplace_back(name, value->view());
                }
                return true;
            }
        );
        return result;
    }

    ruvia::web_worker_handle worker_;
    std::shared_future<void> stopped_;
    std::size_t workerIndex_ = 0;
    std::size_t serviceWorkerCount_ = 0;
    std::atomic_bool running_{ false };
};

} // namespace service::access
