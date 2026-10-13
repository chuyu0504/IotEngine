#include <algorithm>
#include <array>
#include <asio.hpp>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ruvia/core/event_loop_pool.h>
#include <type_traits>
#include "tests/redis_reply.fixture.h"

#include "service/middleware/log.h"
#include "service/features/packet_log/packet_log.transport.h"
#include "service/features/alert/alert.service.h"
#include "service/features/collector/collector.protocol.h"
#include "service/features/edge/edge.service.h"
#include "service/features/collector/collector.service.h"
#include "service/features/collector/engine/engine.runtime.h"
#include "service/features/collector/modbus/modbus.protocol.h"
#include "service/features/collector/scheduling/scheduling.runtime.h"
#include "service/features/collector/scheduling/scheduling.protocol.h"
#include "service/features/collector/s7/s7.protocol.h"
#include "service/features/collector/sl651/sl651.protocol.h"
#include "service/features/collector/tcp/tcp.transport.h"
#include "service/features/command/command.service.h"
#include "service/features/command/command.types.h"
#include "service/features/configuration/configuration.service.h"
#include "service/features/edge/session/session.service.h"
#include "service/features/messaging/messaging.transport.h"
#include "service/features/telemetry/latest/latest.service.h"

namespace collector = service::collector;

namespace {

template <typename Result>
Result runTask(ruvia::task<Result> task) {
    ruvia::event_loop_pool pool({.loop_count_ = 1});
    pool.start();
    auto completion = pool.loop(0).start(std::move(task));
    if constexpr (std::is_void_v<Result>) {
        completion.get();
        pool.stop();
        pool.join();
    } else {
        auto result = completion.get();
        pool.stop();
        pool.join();
        return result;
    }
}

struct FakeDbCell {
    std::string content;
    bool null = false;

    [[nodiscard]] std::optional<std::string_view> value() const noexcept {
        if (null) {
            return std::nullopt;
        }
        return content;
    }
};

struct FakeDbRow {
    std::vector<FakeDbCell> cells;

    explicit FakeDbRow(std::initializer_list<std::string_view> values) {
        cells.reserve(values.size());
        for (const auto value : values) {
            cells.push_back({ std::string(value), false });
        }
    }

    [[nodiscard]] const FakeDbCell& operator[](std::size_t index) const {
        return cells.at(index);
    }
};

struct FakeDbResult {
    std::vector<FakeDbRow> values;

    [[nodiscard]] auto begin() const noexcept { return values.begin(); }

    [[nodiscard]] auto end() const noexcept { return values.end(); }

    [[nodiscard]] bool empty() const noexcept { return values.empty(); }

    [[nodiscard]] std::size_t size() const noexcept { return values.size(); }

    [[nodiscard]] const FakeDbRow& operator[](std::size_t index) const { return values.at(index); }
};

struct RuntimeRepositoryScaleDb {
    ruvia::task<FakeDbResult> query(const ruvia::db_query& query) {
        const auto statement = query.compile(ruvia::db_driver::postgresql, std::pmr::get_default_resource(), ruvia::db_parameter_mode::literal);
        const auto sql = statement.sql();
        FakeDbResult result;
        if (sql.find("FROM \"link\" WHERE") !=
            std::string_view::npos) {
            result.values.emplace_back(FakeDbRow{ "link-1", "collector link", "TCP Client", "Modbus", "127.0.0.1", "1502", "enabled", "f" });
        } else if (sql.find("ORDER BY \"d\".\"link_id\"") != std::string_view::npos) {
            result.values.emplace_back(FakeDbRow{ "device-1", "MODBUS001", "modbus device", "link-1", "TCP Client", "", "Modbus", "+08:00", "300", "OFF", "", "OFF", "", "TCP", "1", "RACK_SLOT", "PG", "0", "1", "0100", "0101", "5000", "5000", "STANDARD", "5", "1", "60", "1", "100", "125", "model-1", "1", "f", "3E", "2007", "0", "255", "1023", "0", "16", "0", "0", "0", "0", "0", "0", "4", "", "", "{}" });
        } else if (sql.find("\"p\".\"protocol\" = E'Modbus'") != std::string_view::npos &&
                   sql.find("'registerType'") != std::string_view::npos) {
            result.values.emplace_back(FakeDbRow{ "device-1", "temperature", "Temperature", "℃", "UINT16", "BIG_ENDIAN", "HOLDING_REGISTER", "0", "1", "1x", "-1", "f" });
        }
        co_return result;
    }
};

struct RecordingRedis {
    explicit RecordingRedis(std::int64_t result)
        : reply(test::redisInteger(result, std::pmr::get_default_resource())) {}

    ruvia::task<ruvia::redis_value>
    eval(std::string_view value, std::span<const std::string_view> inputKeys, std::span<const std::string_view> inputArguments) const {
        script.assign(value);
        keys.assign(inputKeys.begin(), inputKeys.end());
        arguments.assign(inputArguments.begin(), inputArguments.end());
        co_return reply;
    }

    mutable std::string script;
    mutable std::vector<std::string> keys;
    mutable std::vector<std::string> arguments;
    ruvia::redis_value reply;
};

struct AlertScheduleRedisState {
    std::string script;
    std::vector<std::vector<std::string>> pipelineCommands;
    bool invalidStoredDuration = true;
};

struct AlertScheduleRedis {
    std::shared_ptr<AlertScheduleRedisState> state =
        std::make_shared<AlertScheduleRedisState>();

    struct Pipeline {
        std::shared_ptr<AlertScheduleRedisState> state;
        std::vector<std::vector<std::string>> commands;

        void command(std::span<const std::string_view> arguments) {
            commands.emplace_back(arguments.begin(), arguments.end());
        }

        ruvia::task<std::pmr::vector<ruvia::redis_value>> exec() && {
            state->pipelineCommands.insert(state->pipelineCommands.end(), commands.begin(), commands.end());
            std::pmr::vector<ruvia::redis_value> replies;
            replies.reserve(commands.size());
            for (const auto& command : commands) {
                if (!command.empty() && command.front() == "EVALSHA" &&
                    state->invalidStoredDuration &&
                    state->script.find("if duration then") == std::string::npos) {
                    replies.push_back(test::redisError("attempt to perform arithmetic on a nil value", std::pmr::get_default_resource()));
                    continue;
                }
                replies.push_back(test::redisInteger(1, std::pmr::get_default_resource()));
            }
            co_return replies;
        }
    };

    ruvia::task<std::string> script_load(std::string_view script) const {
        state->script = std::string(script);
        co_return "alert-schedule-sha";
    }

    [[nodiscard]] Pipeline pipeline() const { return Pipeline{ state }; }
};

struct FailingLatestPipeline {
    ruvia::task<std::pmr::vector<ruvia::redis_value>> exec() && {
        std::pmr::vector<ruvia::redis_value> replies;
        replies.push_back(test::redisError("injected latest projection failure", std::pmr::get_default_resource()));
        co_return replies;
    }
};

struct FailingConfigRedis {
    explicit FailingConfigRedis(bool failSet = true, bool activeVersion = false)
        : failActiveSet(failSet), hasActiveVersion(activeVersion) {}

    struct Pipeline {
        const FailingConfigRedis* owner;
        std::vector<std::vector<std::string>> commands;

        void command(std::span<const std::string_view> arguments) {
            commands.emplace_back(arguments.begin(), arguments.end());
        }

        ruvia::task<std::pmr::vector<ruvia::redis_value>> exec() && {
            owner->pipelineCommands.insert(owner->pipelineCommands.end(), commands.begin(), commands.end());
            std::pmr::vector<ruvia::redis_value> replies;
            replies.reserve(commands.size());
            for (std::size_t index = 0; index < commands.size(); ++index) {
                replies.push_back(test::redisInteger(1, std::pmr::get_default_resource()));
            }
            co_return replies;
        }
    };

    [[nodiscard]] Pipeline pipeline() const { return Pipeline{ this }; }

    ruvia::task<ruvia::redis_value>
    command(std::span<const std::string_view> arguments) const {
        if (arguments.empty()) {
            co_return test::redisError(
                "empty command",
                std::pmr::get_default_resource()
            );
        }
        commands.emplace_back(arguments.begin(), arguments.end());
        if (arguments.front() == "GET" && !hasActiveVersion) {
            co_return test::redisNull(
                std::pmr::get_default_resource()
            );
        }
        if (arguments.front() == "GET") {
            co_return test::redisString(
                "previous-version",
                std::pmr::get_default_resource()
            );
        }
        if (arguments.front() == "HGET") {
            co_return test::redisString(
                "different-signature",
                std::pmr::get_default_resource()
            );
        }
        if (arguments.front() == "SET" && failActiveSet) {
            co_return test::redisError(
                "injected active pointer failure",
                std::pmr::get_default_resource()
            );
        }
        if (arguments.front() == "SET") {
            co_return test::redisString(
                "OK",
                std::pmr::get_default_resource()
            );
        }
        if (arguments.front() == "ZRANGEBYSCORE") {
            std::pmr::vector<ruvia::redis_value> values;
            co_return test::redisArray(
                std::move(values),
                std::pmr::get_default_resource()
            );
        }
        co_return test::redisInteger(
            1,
            std::pmr::get_default_resource()
        );
    }

    mutable std::vector<std::vector<std::string>> commands;
    mutable std::vector<std::vector<std::string>> pipelineCommands;
    bool failActiveSet;
    bool hasActiveVersion;
};

struct RecordingLatestRedisState {
    std::vector<std::vector<std::string>> pipelineCommands;
    std::vector<std::vector<std::string>> directCommands;
    std::string preservedOnlineUntilMs{ "2000000000000" };
};

struct RecordingLatestRedis {
    std::shared_ptr<RecordingLatestRedisState> state =
        std::make_shared<RecordingLatestRedisState>();

    struct Pipeline {
        std::shared_ptr<RecordingLatestRedisState> state;
        std::vector<std::vector<std::string>> commands;

        void command(std::span<const std::string_view> arguments) {
            commands.emplace_back(arguments.begin(), arguments.end());
        }

        static ruvia::redis_value string(std::string_view value) {
            return test::redisString(
                value,
                std::pmr::get_default_resource()
            );
        }

        static ruvia::redis_value existingLatestReply() {
            std::pmr::vector<ruvia::redis_value> values;
            values.push_back(string("device-1"));
            values.push_back(string("{\"temperature\":true}"));
            return test::redisArray(
                std::move(values),
                std::pmr::get_default_resource()
            );
        }

        ruvia::redis_value existingRuntimeReply() const {
            std::pmr::vector<ruvia::redis_value> values;
            values.push_back(string("device-1"));
            values.push_back(string(state->preservedOnlineUntilMs));
            return test::redisArray(
                std::move(values),
                std::pmr::get_default_resource()
            );
        }

        ruvia::task<std::pmr::vector<ruvia::redis_value>> exec() && {
            state->pipelineCommands.insert(state->pipelineCommands.end(), commands.begin(), commands.end());
            std::pmr::vector<ruvia::redis_value> replies;
            replies.reserve(commands.size());
            for (const auto& command : commands) {
                if (command.size() >= 3 && command[0] == "HMGET" &&
                    command[1] == "iot:v2:device:device-1:latest") {
                    replies.push_back(existingLatestReply());
                } else if (command.size() >= 3 && command[0] == "HMGET" &&
                           command[1] == "iot:v2:runtime:device:device-1") {
                    replies.push_back(existingRuntimeReply());
                } else {
                    replies.push_back(test::redisInteger(1, std::pmr::get_default_resource()));
                }
            }
            co_return replies;
        }
    };

    [[nodiscard]] Pipeline pipeline() const { return Pipeline{ state }; }

    ruvia::task<ruvia::redis_value>
    command(std::span<const std::string_view> arguments) const {
        state->directCommands.emplace_back(arguments.begin(), arguments.end());
        if (!arguments.empty() && arguments.front() == "XADD") {
            co_return test::redisString(
                "1-0",
                std::pmr::get_default_resource()
            );
        }
        co_return test::redisInteger(
            1,
            std::pmr::get_default_resource()
        );
    }
};

struct LatestProjectionDb {
    ruvia::task<FakeDbResult> query(const ruvia::db_query& query) {
        const auto statement = query.compile(ruvia::db_driver::postgresql, std::pmr::get_default_resource(), ruvia::db_parameter_mode::literal);
        const auto text = statement.sql();
        FakeDbResult result;
        if (text.find("'online_timeout'") != std::string_view::npos) {
            result.values.emplace_back(FakeDbRow{ "device-1", "D1", "300000" });
        } else if (text.find("WITH \"configured\"") != std::string_view::npos) {
            result.values.emplace_back(FakeDbRow{ "device-1", "D1", "Modbus", "temperature", "New Name", "℃", "12.5", "1700000000000", "2", "1", "environment", "", "0", "{\"id\":\"temperature\",\"name\":\"New Name\",\"value\":\"12.5\",\"unit\":\"℃\","
                                                                                                                                                                         "\"scale\":2,\"decimals\":1,\"group\":\"environment\",\"encode\":\"\","
                                                                                                                                                                         "\"sort\":0,\"protocol\":\"Modbus\",\"observedAt\":1700000000000,"
                                                                                                                                                                         "\"updatedAt\":1700000001000,\"source\":\"database\"}" });
        }
        co_return result;
    }
};

struct LatestProjectionContext {
    LatestProjectionDb database;
    RecordingLatestRedis redisClient;

    LatestProjectionDb& db() noexcept { return database; }

    RecordingLatestRedis redis() const noexcept { return redisClient; }
};

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

bool has(const std::vector<collector::ProtocolAction>& actions, collector::ProtocolActionKind kind) {
    return std::any_of(actions.begin(), actions.end(), [kind](const auto& action) {
        return action.kind == kind;
    });
}

const collector::ProtocolAction& first(const std::vector<collector::ProtocolAction>& actions, collector::ProtocolActionKind kind) {
    const auto current = std::find_if(actions.begin(), actions.end(), [kind](const auto& action) {
        return action.kind == kind;
    });
    if (current == actions.end()) {
        throw std::runtime_error("expected protocol action was not emitted");
    }
    return *current;
}

std::uint16_t crc16(std::span<const std::uint8_t> bytes) {
    std::uint16_t crc = 0xFFFFU;
    for (const auto byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1U) != 0 ? static_cast<std::uint16_t>((crc >> 1U) ^ 0xA001U)
                                  : static_cast<std::uint16_t>(crc >> 1U);
        }
    }
    return crc;
}

std::vector<std::uint8_t> slFrame(std::uint8_t functionCode, std::vector<std::uint8_t> body = { 0x00, 0x01, 0x24, 0x01, 0x02, 0x03, 0x04, 0x05, 0x39, 0x12, 0x12, 0x34 }) {
    std::vector<std::uint8_t> frame{ 0x7E,
                                     0x7E,
                                     0x01,
                                     0x00,
                                     0x00,
                                     0x00,
                                     0x00,
                                     0x01,
                                     0x00,
                                     0x00,
                                     functionCode,
                                     static_cast<std::uint8_t>(body.size() >> 8U),
                                     static_cast<std::uint8_t>(body.size()),
                                     0x02 };
    frame.insert(frame.end(), body.begin(), body.end());
    frame.push_back(0x03);
    const auto crc = crc16(frame);
    frame.push_back(static_cast<std::uint8_t>(crc >> 8U));
    frame.push_back(static_cast<std::uint8_t>(crc));
    return frame;
}

std::vector<std::uint8_t> slMultiFrame(std::uint8_t functionCode, std::uint16_t total, std::uint16_t sequence, std::vector<std::uint8_t> body) {
    const auto packed = (static_cast<std::uint32_t>(total) << 12U) | sequence;
    body.insert(body.begin(), { static_cast<std::uint8_t>(packed >> 16U), static_cast<std::uint8_t>(packed >> 8U), static_cast<std::uint8_t>(packed) });
    auto frame = slFrame(functionCode, std::move(body));
    frame[13] = 0x16;
    frame[frame.size() - 3] = sequence == total ? 0x03 : 0x17;
    const auto crc = crc16(std::span<const std::uint8_t>(frame).first(frame.size() - 2));
    frame[frame.size() - 2] = static_cast<std::uint8_t>(crc >> 8U);
    frame.back() = static_cast<std::uint8_t>(crc);
    return frame;
}

std::vector<std::uint8_t> modbusRead(std::uint16_t transaction, std::uint8_t function = 3) {
    return { static_cast<std::uint8_t>(transaction >> 8U),
             static_cast<std::uint8_t>(transaction),
             0,
             0,
             0,
             6,
             1,
             function,
             0,
             0,
             0,
             1 };
}

std::vector<std::uint8_t> modbusReadResponse(std::uint16_t transaction, std::uint8_t function = 3, std::vector<std::uint8_t> data = { 0x12, 0x34 }) {
    const auto length = static_cast<std::uint16_t>(3 + data.size());
    std::vector<std::uint8_t> response{ static_cast<std::uint8_t>(transaction >> 8U),
                                        static_cast<std::uint8_t>(transaction),
                                        0,
                                        0,
                                        static_cast<std::uint8_t>(length >> 8U),
                                        static_cast<std::uint8_t>(length),
                                        1,
                                        function,
                                        static_cast<std::uint8_t>(data.size()) };
    response.insert(response.end(), data.begin(), data.end());
    return response;
}

std::vector<std::uint8_t> modbusWrite(std::uint16_t transaction) {
    return { static_cast<std::uint8_t>(transaction >> 8U),
             static_cast<std::uint8_t>(transaction),
             0,
             0,
             0,
             6,
             1,
             6,
             0,
             0,
             0x12,
             0x34 };
}

std::vector<std::uint8_t> withModbusCrc(std::vector<std::uint8_t> frame) {
    const auto crc = crc16(frame);
    frame.push_back(static_cast<std::uint8_t>(crc));
    frame.push_back(static_cast<std::uint8_t>(crc >> 8U));
    return frame;
}

std::vector<std::uint8_t> modbusRequest(bool tcp, std::uint16_t transaction, std::uint8_t function) {
    std::vector<std::uint8_t> pdu{ 1, function, 0, 0 };
    if (function >= 1 && function <= 4) {
        pdu.insert(pdu.end(), { 0, 1 });
    } else if (function == 5) {
        pdu.insert(pdu.end(), { 0xFF, 0x00 });
    } else if (function == 6) {
        pdu.insert(pdu.end(), { 0x12, 0x34 });
    } else if (function == 15) {
        pdu.insert(pdu.end(), { 0, 8, 1, 0x01 });
    } else if (function == 16) {
        pdu.insert(pdu.end(), { 0, 2, 4, 0x12, 0x34, 0x12, 0x34 });
    }
    if (!tcp) {
        return withModbusCrc(std::move(pdu));
    }
    const auto length = static_cast<std::uint16_t>(pdu.size());
    std::vector<std::uint8_t> frame{
        static_cast<std::uint8_t>(transaction >> 8U),
        static_cast<std::uint8_t>(transaction),
        0,
        0,
        static_cast<std::uint8_t>(length >> 8U),
        static_cast<std::uint8_t>(length)
    };
    frame.insert(frame.end(), pdu.begin(), pdu.end());
    return frame;
}

std::vector<std::uint8_t> modbusResponse(bool tcp, std::uint16_t transaction, std::uint8_t function) {
    std::vector<std::uint8_t> pdu{ 1, function };
    if (function == 1 || function == 2) {
        pdu.insert(pdu.end(), { 1, 0x01 });
    } else if (function == 3 || function == 4) {
        pdu.insert(pdu.end(), { 2, 0x12, 0x34 });
    } else if (function == 5) {
        pdu.insert(pdu.end(), { 0, 0, 0xFF, 0x00 });
    } else if (function == 6) {
        pdu.insert(pdu.end(), { 0, 0, 0x12, 0x34 });
    } else if (function == 15) {
        pdu.insert(pdu.end(), { 0, 0, 0, 8 });
    } else if (function == 16) {
        pdu.insert(pdu.end(), { 0, 0, 0, 2 });
    }
    if (!tcp) {
        return withModbusCrc(std::move(pdu));
    }
    const auto length = static_cast<std::uint16_t>(pdu.size());
    std::vector<std::uint8_t> frame{
        static_cast<std::uint8_t>(transaction >> 8U),
        static_cast<std::uint8_t>(transaction),
        0,
        0,
        static_cast<std::uint8_t>(length >> 8U),
        static_cast<std::uint8_t>(length)
    };
    frame.insert(frame.end(), pdu.begin(), pdu.end());
    return frame;
}

std::vector<std::uint8_t> modbusRtuRead() {
    return withModbusCrc({ 1, 3, 0, 0, 0, 1 });
}

std::vector<std::uint8_t> modbusRtuReadResponse() {
    return withModbusCrc({ 1, 3, 2, 0x12, 0x34 });
}

std::vector<std::uint8_t> modbusRtuWrite() {
    return withModbusCrc({ 1, 6, 0, 0, 0x12, 0x34 });
}

std::vector<std::uint8_t> modbusReadResponseForRequest(std::span<const std::uint8_t> request, bool tcp) {
    const auto functionOffset = tcp ? 7U : 1U;
    const auto quantityOffset = tcp ? 10U : 4U;
    require(request.size() >= quantityOffset + 2 && request[functionOffset] >= 1 && request[functionOffset] <= 4, "Modbus poll response helper received an invalid read request");
    const auto function = request[functionOffset];
    const auto quantity = static_cast<std::size_t>(request[quantityOffset] << 8U) |
        request[quantityOffset + 1];
    const auto byteCount = function <= 2 ? (quantity + 7U) / 8U : quantity * 2U;
    if (!tcp) {
        std::vector<std::uint8_t> response{ request[0], function, static_cast<std::uint8_t>(byteCount) };
        response.resize(response.size() + byteCount, 0);
        return withModbusCrc(std::move(response));
    }
    const auto length = static_cast<std::uint16_t>(3 + byteCount);
    std::vector<std::uint8_t> response{ request[0],
                                        request[1],
                                        0,
                                        0,
                                        static_cast<std::uint8_t>(length >> 8U),
                                        static_cast<std::uint8_t>(length),
                                        request[6],
                                        function,
                                        static_cast<std::uint8_t>(byteCount) };
    response.resize(response.size() + byteCount, 0);
    return response;
}

std::vector<collector::ProtocolAction>
drainInitialModbusPoll(collector::ProtocolEngine& engine, std::vector<collector::ProtocolAction> actions, service::message::IngressPacket& packet, bool tcp) {
    while (true) {
        const auto outbound = std::find_if(actions.begin(), actions.end(), [](const auto& action) {
            return action.kind == collector::ProtocolActionKind::Send;
        });
        if (outbound == actions.end()) {
            return actions;
        }
        packet.payload = modbusReadResponseForRequest(outbound->bytes, tcp);
        actions = engine.consume(packet);
    }
}

std::vector<std::uint8_t> s7ReadRequest(std::uint16_t reference) {
    return { 0x03,
             0x00,
             0x00,
             0x1F,
             0x02,
             0xF0,
             0x80,
             0x32,
             0x01,
             0x00,
             0x00,
             static_cast<std::uint8_t>(reference >> 8U),
             static_cast<std::uint8_t>(reference),
             0x00,
             0x0E,
             0x00,
             0x00,
             0x04,
             0x01,
             0x12,
             0x0A,
             0x10,
             0x02,
             0x00,
             0x01,
             0x00,
             0x01,
             0x84,
             0x00,
             0x00,
             0x00 };
}

std::vector<std::uint8_t> s7ReadResponse(std::uint16_t reference) {
    return { 0x03,
             0x00,
             0x00,
             0x1B,
             0x02,
             0xF0,
             0x80,
             0x32,
             0x03,
             0x00,
             0x00,
             static_cast<std::uint8_t>(reference >> 8U),
             static_cast<std::uint8_t>(reference),
             0x00,
             0x02,
             0x00,
             0x06,
             0x00,
             0x00,
             0x04,
             0x01,
             0xFF,
             0x04,
             0x00,
             0x10,
             0x12,
             0x34 };
}

std::vector<std::uint8_t> s7WriteRequest(std::uint16_t reference) {
    return { 0x03,
             0x00,
             0x00,
             0x13,
             0x02,
             0xF0,
             0x80,
             0x32,
             0x01,
             0x00,
             0x00,
             static_cast<std::uint8_t>(reference >> 8U),
             static_cast<std::uint8_t>(reference),
             0x00,
             0x02,
             0x00,
             0x00,
             0x05,
             0x01 };
}

std::vector<std::uint8_t> s7WriteResponse(std::uint16_t reference) {
    return { 0x03,
             0x00,
             0x00,
             0x15,
             0x02,
             0xF0,
             0x80,
             0x32,
             0x03,
             0x00,
             0x00,
             static_cast<std::uint8_t>(reference >> 8U),
             static_cast<std::uint8_t>(reference),
             0x00,
             0x02,
             0x00,
             0x00,
             0x00,
             0x00,
             0x05,
             0x01 };
}

std::uint16_t s7Reference(std::span<const std::uint8_t> frame) {
    require(frame.size() >= 13, "S7 frame does not contain a PDU reference");
    return static_cast<std::uint16_t>(static_cast<std::uint16_t>(frame[11]) << 8U) | frame[12];
}

std::vector<std::uint8_t> s7CotpConfirm() {
    return { 0x03, 0x00, 0x00, 0x0B, 0x06, 0xD0, 0x00, 0x01, 0x00, 0x06, 0x00 };
}

std::vector<std::uint8_t> s7SetupResponse(std::uint16_t reference = 0) {
    return { 0x03,
             0x00,
             0x00,
             0x1B,
             0x02,
             0xF0,
             0x80,
             0x32,
             0x03,
             0x00,
             0x00,
             static_cast<std::uint8_t>(reference >> 8U),
             static_cast<std::uint8_t>(reference),
             0x00,
             0x08,
             0x00,
             0x00,
             0x00,
             0x00,
             0xF0,
             0x00,
             0x00,
             0x01,
             0x00,
             0x01,
             0x01,
             0xE0 };
}

std::vector<std::uint8_t> s7ReadResponseForRequest(std::span<const std::uint8_t> request) {
    require(request.size() >= 31 && request[17] == 0x04, "S7 read response helper received an invalid request");
    const auto count = request[18];
    std::vector<std::uint8_t> data;
    for (std::size_t index = 0; index < count; ++index) {
        const auto offset = 19 + index * 12;
        require(offset + 12 <= request.size(), "S7 read request item is truncated");
        const auto amount = static_cast<std::size_t>(request[offset + 4] << 8U) |
            request[offset + 5];
        const auto wordLength = request[offset + 3];
        const auto byteLength =
            amount * (wordLength == 0x1C || wordLength == 0x1D ? 2U : 1U);
        data.insert(data.end(), { 0xFF, 0x04, static_cast<std::uint8_t>((byteLength * 8U) >> 8U), static_cast<std::uint8_t>(byteLength * 8U) });
        for (std::size_t byte = 0; byte < byteLength; ++byte) {
            data.push_back(index == 0 && byte == 0 ? 0x12 : index == 0 && byte == 1 ? 0x34
                                                                                    : 0);
        }
        if ((byteLength & 1U) != 0 && index + 1 < count) {
            data.push_back(0);
        }
    }
    const auto totalLength = static_cast<std::uint16_t>(21 + data.size());
    const auto dataLength = static_cast<std::uint16_t>(data.size());
    std::vector<std::uint8_t> response{ 0x03,
                                        0x00,
                                        static_cast<std::uint8_t>(totalLength >> 8U),
                                        static_cast<std::uint8_t>(totalLength),
                                        0x02,
                                        0xF0,
                                        0x80,
                                        0x32,
                                        0x03,
                                        0x00,
                                        0x00,
                                        request[11],
                                        request[12],
                                        0x00,
                                        0x02,
                                        static_cast<std::uint8_t>(dataLength >> 8U),
                                        static_cast<std::uint8_t>(dataLength),
                                        0x00,
                                        0x00,
                                        0x04,
                                        count };
    response.insert(response.end(), data.begin(), data.end());
    return response;
}

collector::ProtocolSessionFactoryRegistry sessionFactories() {
    collector::ProtocolSessionFactoryRegistry result;
    result.add(std::make_unique<collector::modbus::SessionFactory>());
    result.add(std::make_unique<collector::s7::SessionFactory>());
    result.add(std::make_unique<collector::sl651::SessionFactory>());
    return result;
}

void testCommandOutcomeDomain() {
    struct ProtocolFailure {
        std::string_view protocol;
        std::string_view lostAck;
        std::string_view readbackFailure;
    };
    require(service::command::collectorResultState(true, "write_ack_missing") == "UNKNOWN",
            "a missing write ACK was overridden by a success flag");
    constexpr ProtocolFailure failures[]{
        {"FINS", "polling_write_result_unknown", "polling_readback_timeout"},
        {"S7", "write_ack_missing:s7_readback_timeout", "s7_readback_timeout"},
        {"Modbus", "write_ack_missing:modbus_readback_timeout", "modbus_readback_timeout"},
    };
    for (const auto& failure : failures) {
        require(service::command::collectorResultState(false, failure.lostAck) == "UNKNOWN" &&
                    !service::command::retryableFailure(failure.lostAck),
                failure.protocol.data());
        require(service::command::collectorResultState(false, failure.readbackFailure) == "UNKNOWN" &&
                    !service::command::retryableFailure(failure.readbackFailure),
                failure.protocol.data());
    }
    require(service::command::collectorResultState(false, "s7_readback_mismatch") ==
                "READBACK_MISMATCH",
            "a confirmed readback mismatch lost its distinct result state");
    require(service::command::collectorResultState(false, "engine_device_offline") == "REJECTED" &&
                service::command::collectorResultState(false, "dispatch_deadline_expired") ==
                    "REJECTED" && !service::command::retryableFailure("dispatch_deadline_expired") &&
                service::command::expiredDispatchState(false) == "REJECTED" &&
                service::command::expiredDispatchState(true) == "UNKNOWN",
            "definitely unsent and ambiguous command outcomes were conflated");
    require(service::command::kCommandResultAcceptingStatuses ==
                std::array<std::string_view, 2>{"DISPATCHING", "AWAITING_RESULT"},
            "late command results may overwrite a terminal status");
}

void testCapabilities() {
    require(collector::DeviceDefinition{}.timezone == "+08:00", "device timezone must default to UTC+8");
    auto registry = sessionFactories();
    require(registry.require("SL651").definition().capabilities.has(collector::ProtocolCapability::TcpServer), "SL651 must support TCP Server");
    require(!registry.require("SL651").definition().capabilities.has(collector::ProtocolCapability::TcpClient), "SL651 must reject TCP Client");
    require(!registry.require("SL651").definition().capabilities.has(collector::ProtocolCapability::Polling), "SL651 must not expose polling");
    require(!registry.require("SL651").definition().capabilities.has(collector::ProtocolCapability::Registration) && !registry.require("SL651").definition().capabilities.has(collector::ProtocolCapability::Heartbeat), "SL651 must derive identity from protocol frames without registration or heartbeat");
    require(registry.require("Modbus").definition().capabilities.has(collector::ProtocolCapability::Discovery), "Modbus discovery capability missing");
    require(registry.require("S7").definition().capabilities.has(collector::ProtocolCapability::Polling), "S7 polling capability missing");
}

void testProtocolFactoryBoundary() {
    auto registry = sessionFactories();
    auto snapshot = std::make_shared<collector::RuntimeSnapshot>();
    snapshot->links.push_back({ .id = "link", .mode = "TCP Client", .protocol = "Modbus", .status = "enabled" });
    for (const auto& [id, link, protocol, target] : std::array{
        std::array{"selected", "link", "Modbus", "target"},
        std::array{"other-target", "link", "Modbus", "other"},
        std::array{"other-link", "other", "Modbus", "target"},
        std::array{"other-protocol", "link", "S7", "target"}}) {
        collector::DeviceDefinition device;
        device.id = id; device.code = id; device.linkId = link;
        device.protocol = protocol; device.targetId = target;
        device.modbusMode = "TCP";
        snapshot->devices.push_back(device);
    }
    auto session = registry.require("Modbus").createSession(snapshot->links.front(), "connection", "target", snapshot);
    const auto started = session->connected();
    const auto count = std::count_if(started.begin(), started.end(), [](const auto& action) {
        return action.kind == collector::ProtocolActionKind::BindDevice;
    });
    require(count == 1 && first(started, collector::ProtocolActionKind::BindDevice).deviceId == "selected",
        "factory bound devices from another target, link or protocol");
    const auto rejects = [](auto&& operation) {
        try { operation(); } catch (const std::invalid_argument&) { return true; }
        return false;
    };
    require(rejects([&] { (void)registry.require("S7").createSession(snapshot->links.front(), "c", "target", snapshot); }),
        "factory accepted another protocol's link");
    auto invalid = snapshot->links.front();
    invalid.protocol = "SL651";
    require(rejects([&] { (void)registry.require("SL651").createSession(invalid, "c", "target", snapshot); }),
        "reporting protocol accepted an unsupported client transport");
    invalid = snapshot->links.front();
    invalid.mode = "TCP Server";
    require(rejects([&] { (void)registry.require("Modbus").createSession(invalid, "c", "target", snapshot); }),
        "server session accepted a client target");
    require(rejects([&] { (void)registry.require("Modbus").createSession(snapshot->links.front(), "c", "target", {}); }),
        "factory accepted a missing immutable snapshot");
}

void testProtocolDebugAttribution() {
    collector::ProtocolEngine engine(sessionFactories());
    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({ .id = "link", .mode = "TCP Client", .protocol = "Modbus" });
    for (const auto& id : {"first", "second"}) {
        collector::DeviceDefinition device;
        device.id = id; device.code = id; device.linkId = "link";
        device.targetId = "target"; device.protocol = "Modbus"; device.modbusMode = "TCP";
        device.slaveId = static_cast<std::uint8_t>(snapshot.devices.size() + 1);
        snapshot.devices.push_back(device);
    }
    const std::array<std::uint8_t, 9> response{0, 1, 0, 0, 0, 3, 2, 3, 0};
    collector::DebugPacketIdentity packet{.linkId = "link", .targetId = "target", .direction = "RX", .bytes = response};
    require(engine.identifyDebugDevice(snapshot, packet) == &snapshot.devices[1],
        "TCP debug packet was attributed to the wrong unit");
    packet.bytes = std::span(response).first(3);
    require(!engine.identifyDebugDevice(snapshot, packet), "ambiguous TCP fragment was assigned a device");
    std::set<std::string> bound{"first"};
    packet.boundDevices = &bound;
    require(engine.identifyDebugDevice(snapshot, packet) == &snapshot.devices[0],
        "debug fragment ignored the connection's bound device");
    packet.bytes = response;
    require(!engine.identifyDebugDevice(snapshot, packet), "debug packet escaped connection ownership");
    packet.knownDevice = "second";
    require(engine.identifyDebugDevice(snapshot, packet) == &snapshot.devices[1],
        "validated device identity was overridden by stale route hints");
    packet.knownDevice = {}; packet.boundDevices = nullptr;
    packet.targetId = "another-target";
    require(!engine.identifyDebugDevice(snapshot, packet), "debug attribution escaped the client target");
    packet.targetId = "target";
    for (auto& device : snapshot.devices) device.modbusMode = "RTU";
    const std::array<std::uint8_t, 3> rtu{2, 3, 0};
    packet.bytes = rtu;
    require(engine.identifyDebugDevice(snapshot, packet) == &snapshot.devices[1], "RTU unit attribution changed");

    snapshot.links.front().protocol = "SL651";
    snapshot.links.front().mode = "TCP Server";
    for (auto& device : snapshot.devices) { device.protocol = "SL651"; device.code = device.slaveId == 1 ? "0000000001" : "0000000002"; }
    const std::array<std::uint8_t, 8> station{0x7e, 0x7e, 1, 0, 0, 0, 0, 2};
    packet.bytes = station;
    require(!engine.identifyDebugDevice(snapshot, packet), "unvalidated reporting frame was attributed to a station");
    packet.deviceOnly = true;
    require(engine.identifyDebugDevice(snapshot, packet) == &snapshot.devices[1], "reporting station address did not match");
    const std::array<std::uint8_t, 8> outbound{0x7e, 0x7e, 0, 0, 0, 0, 2, 1};
    packet.direction = "TX"; packet.deviceOnly = false; packet.bytes = outbound;
    require(engine.identifyDebugDevice(snapshot, packet) == &snapshot.devices[1], "downlink station address offset changed");
    require(engine.expectsResponse("Modbus", {}) && engine.expectsResponse("S7", {}),
        "request protocols lost response tracking");
    require(!engine.expectsResponse("SL651", {}) && engine.expectsResponse("SL651", "command"),
        "report acknowledgements were treated as requests");
    require(!engine.expectsResponse("unregistered", "command"), "unknown protocol exposed response tracking");
}

void testProtocolCommandGrouping() {
    const std::vector<collector::CommandElementValue> values{{"first", "1"}, {"second", "2"}};
    for (const auto* protocol : {"Modbus", "S7"}) {
        const auto tasks = collector::command::groupElements(collector::protocolDefinition(protocol), values);
        require(tasks.size() == 2 && tasks[0].size() == 1 && tasks[1].size() == 1 &&
            tasks[0][0].elementId == "first" && tasks[1][0].value == "2",
            "independent writes no longer have separate receipts");
    }
    const auto tasks = collector::command::groupElements(collector::protocolDefinition("SL651"), values);
    require(tasks.size() == 1 && tasks[0].size() == 2 && tasks[0][1].elementId == "second",
        "complete function command was split into incomplete requests");
    const auto mqttTasks = collector::command::groupElements(collector::kMqttProtocol, values);
    require(mqttTasks.size() == 1 && mqttTasks.front().size() == 2 &&
        mqttTasks.front()[0].elementId == "first" && mqttTasks.front()[1].value == "2",
        "MQTT message template was split into partial payloads");
    require(collector::command::groupElements(collector::kSl651Protocol, {}).empty(),
        "empty command created an empty task");
}

void testStationScopeAndInstanceIdentity() {
    collector::RuntimeSnapshot snapshot;
    for (const auto* link : { "station-link-a", "station-link-b" }) {
        snapshot.links.push_back({ .id = link, .mode = "TCP Server", .protocol = "SL651", .status = "enabled" });
        collector::DeviceDefinition device;
        device.id = std::string(link) + "-device";
        device.linkId = link;
        device.code = "0000000001";
        device.protocol = "SL651";
        device.elements.push_back({ .id = "water", .name = "Water", .functionCode = "32", .guideHex = "3912", .encoding = "BCD", .length = 2, .digits = 2 });
        snapshot.devices.push_back(device);
    }
    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    for (const auto* link : { "station-link-a", "station-link-b" }) {
        const auto connection = std::string(link) + "-connection";
        (void)engine.connected({ .connectionId = connection, .linkId = link, .sessionEpoch = 1 });
        service::message::IngressPacket packet{ .messageId = "frame", .linkId = link, .connectionId = connection, .occurredAtMs = 1000 };
        packet.payload = slFrame(0x32, { 0x00, 0x01, 0x24, 0x01, 0x02, 0x03, 0x04, 0x05, 0x39, 0x12, 0x12, 0x34 });
        const auto actions = engine.consume(packet);
        const auto& parsed = first(actions, collector::ProtocolActionKind::PublishParsed).parsed;
        require(parsed.deviceId == std::string(link) + "-device", "same SL651 station code crossed link identity");
        require(parsed.deviceCode == "0000000001", "SL651 wire address was replaced by UUID");
    }
    auto alias = snapshot.devices.front();
    alias.id = "alias";
    alias.code = "1";
    snapshot.devices.push_back(alias);
    engine.reload(snapshot);
    bool rejected = false;
    try {
        (void)engine.connected({ .connectionId = "duplicate", .linkId = "station-link-a", .sessionEpoch = 1 });
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    require(rejected, "normalized station address alias was silently accepted");
    require(service::message::commandStream(0, true, "instance-a") != service::message::commandStream(0, true, "instance-b"), "worker 0 aliases across instances");
    require(service::command::collectorResultState(false, "response_timeout") == "UNKNOWN", "transport timeout was misrepresented as confirmed failure");
    require(service::command::collectorResultState(false, "device_negative_ack: {\"code\":1}") == "FAILED", "explicit device rejection was not recorded as failure");
    require(service::command::terminalState("UNKNOWN") && !service::command::terminalState("AWAITING_RESULT"), "command wait completion semantics are incorrect");
}

void testSl651ObservationReplay() {
    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({.id = "replay-link", .mode = "TCP Server", .protocol = "SL651", .status = "enabled"});
    collector::DeviceDefinition device;
    device.id = "replay-device"; device.code = "0001000102";
    device.linkId = "replay-link"; device.protocol = "SL651"; device.sl651ResponseMode = "M2";
    device.elements.push_back({.id = "water", .name = "Water", .functionCode = "32", .guideHex = "3923", .encoding = "BCD", .length = 4, .digits = 3});
    snapshot.devices.push_back(device);
    const std::array<std::string_view, 3> frames{
        "7E7E010001000102FFFA320031020076260917021000F1F1000100010249F0F02609170210392300001521371B000654272B0000001458FFB02800000000190330CE",
        "7E7E010001000102FFFA320031020076260917021005F1F1000100010249F0F02609170210392300001521371B000654272B0000001458FFB0280000000019033607",
        "7E7E010001000102FFFA320031020076260917021501F1F1000100010249F0F02609170210392300001521371B000654272B0000001458FFB02800000000190327CA"};
    std::string identity;
    for (std::size_t index = 0; index < frames.size(); ++index) {
        // 新建引擎模拟重连、Worker 变化和进程重启，不能依赖会话缓存去重。
        collector::ProtocolEngine engine(sessionFactories());
        engine.reload(snapshot);
        const auto connection = "replay-" + std::to_string(index);
        (void)engine.connected({.connectionId = connection, .linkId = "replay-link", .sessionEpoch = 1});
        service::message::IngressPacket packet{.messageId = connection, .linkId = "replay-link", .connectionId = connection, .occurredAtMs = 1789582200000 + 19000};
        packet.payload = collector::sl651::detail::hexBytes(frames[index]);
        const auto actions = engine.consume(packet);
        const auto& publication = first(actions, collector::ProtocolActionKind::PublishParsed);
        require(publication.parsed.observedAtMs == 1789582200000, "SL651 replay used sending time instead of observation time");
        if (identity.empty()) identity = publication.parsed.acquisitionId;
        require(identity == publication.parsed.acquisitionId, "SL651 replay changed acquisition across sessions");
        require(!has(actions, collector::ProtocolActionKind::Send), "SL651 replay ACK preceded durable publication");
        const auto acknowledged = engine.parsedPublished(connection, publication.publicationToken);
        const auto& ack = first(acknowledged, collector::ProtocolActionKind::Send).bytes;
        require(ack[14] == 0 && ack[15] == 0x76 && ack[ack.size() - 3] == 0x04,
                "SL651 replay ACK lost original serial or EOT");
        auto next = packet.payload;
        next[14 + 22] = 0x15; // 相同数值、流水号的下一观测时刻仍是不同采样。
        const auto crc = crc16(std::span<const std::uint8_t>(next).first(next.size() - 2));
        next[next.size() - 2] = static_cast<std::uint8_t>(crc >> 8U); next.back() = static_cast<std::uint8_t>(crc);
        packet.payload = next;
        require(first(engine.consume(packet), collector::ProtocolActionKind::PublishParsed).parsed.acquisitionId != identity,
                "SL651 distinct observation was deduplicated");
    }
    auto invalid = collector::sl651::detail::hexBytes(frames[0]);
    auto body = std::vector<std::uint8_t>(invalid.begin() + 14, invalid.end() - 3);
    body[19] = 0x13;
    require(!collector::sl651::detail::observationTimeMilliseconds(body, 0x32, "+08:00"), "invalid SL651 observation month accepted");
    require(!collector::sl651::detail::observationTimeMilliseconds(body, 0x4C, "+08:00"), "command response treated as unsolicited observation");
}

void testSl651() {
    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({ .id = "sl-link", .name = "SL", .mode = "TCP Server", .protocol = "SL651", .ip = "127.0.0.1", .port = 15001, .status = "enabled" });
    collector::DeviceDefinition device;
    device.id = "sl-device";
    device.code = "0000000001";
    device.linkId = "sl-link";
    device.protocol = "SL651";
    device.sl651ResponseMode = "M2";
    device.elements.push_back({ .id = "water", .name = "Water", .unit = "m", .functionCode = "32", .guideHex = "3912", .encoding = "BCD", .length = 2, .digits = 2 });
    device.elements.push_back({ .id = "request-value", .name = "Request value", .functionCode = "4C", .direction = "DOWN", .guideHex = "3912", .encoding = "BCD", .length = 2, .digits = 2 });
    device.elements.push_back({ .id = "response-value", .name = "Response value", .functionCode = "4C", .direction = "DOWN", .guideHex = "3912", .encoding = "BCD", .length = 2, .digits = 2, .responseElement = true });
    snapshot.devices.push_back(device);

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    (void)engine.connected({ .connectionId = "sl-connection", .linkId = "sl-link", .remoteAddress = "127.0.0.1:10001", .sessionEpoch = 1 });
    const auto frame =
        slFrame(0x32, { 0x00, 0x01, 0x24, 0x01, 0x02, 0x03, 0x04, 0x05, 0x39, 0x12, 0x12, 0x34 });
    service::message::IngressPacket packet{ .messageId = "sl-ingress",
                                            .linkId = "sl-link",
                                            .connectionId = "sl-connection",
                                            .remoteAddress = "127.0.0.1:10001",
                                            .occurredAtMs = 1000 };
    packet.payload.assign(frame.begin(), frame.begin() + 5);
    require(engine.consume(packet).empty(), "partial SL651 header must not emit actions");
    packet.payload.assign(frame.begin() + 5, frame.end());
    const auto actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::BindDevice), "SL651 did not bind device code");
    const auto& parsed = first(actions, collector::ProtocolActionKind::PublishParsed).parsed;
    require(parsed.rawPayloads == std::vector<std::vector<std::uint8_t>>{ frame }, "SL651 raw frame was not preserved as a one-item array");
    require(parsed.valuesJson.find("12.34") != std::string::npos, "SL651 BCD element was not parsed");
    require(parsed.observedAtMs == 1704135845000, "SL651 device-local report time was not converted from UTC+8 to UTC");

    packet.payload = slFrame(0x4C);
    const auto responseActions = engine.consume(packet);
    const auto& responseParsed =
        first(responseActions, collector::ProtocolActionKind::PublishParsed).parsed;
    require(responseParsed.valuesJson.find("response-value") != std::string::npos && responseParsed.valuesJson.find("request-value") == std::string::npos, "SL651 upstream response did not use responseElements");

    auto commandFrame = slFrame(0x4C, {0, 0, 0x24, 1, 2, 3, 4, 5});
    // Downlink direction occupies the high nibble of the length field; refresh the CRC.
    commandFrame[2] = 0x00;
    commandFrame[6] = 0x01;
    commandFrame[11] = 0x80;
    commandFrame[commandFrame.size() - 3] = 0x05;
    const auto commandCrc =
        crc16(std::span<const std::uint8_t>(commandFrame).first(commandFrame.size() - 2));
    commandFrame[commandFrame.size() - 2] = static_cast<std::uint8_t>(commandCrc >> 8U);
    commandFrame.back() = static_cast<std::uint8_t>(commandCrc);
    auto commandActions = engine.execute("sl-connection", { .id = "sl-command", .deviceId = "sl-device", .deviceCode = "0000000001", .kind = "control", .payload = commandFrame });
    require(has(commandActions, collector::ProtocolActionKind::Send), "SL651 command was not sent");
    packet.payload = slFrame(0x32);
    commandActions = engine.consume(packet);
    require(!has(commandActions, collector::ProtocolActionKind::CompleteCommand), "SL651 unsolicited report completed a command");
    packet.payload = slFrame(0x4C, {0, 2, 0x24, 1, 2, 3, 4, 6});
    commandActions = engine.consume(packet);
    commandActions = engine.parsedPublished("sl-connection", first(commandActions, collector::ProtocolActionKind::PublishParsed).publicationToken);
    require(has(commandActions, collector::ProtocolActionKind::CompleteCommand), "SL651 ACK did not complete command");

    commandActions = engine.execute(
        "sl-connection",
        { .id = "sl-element-command",
          .deviceId = "sl-device",
          .deviceCode = "0000000001",
          .kind = "command",
          .elements = { { .elementId = "request-value", .value = "12.34" } } }
    );
    const auto& generatedSl651 = first(commandActions, collector::ProtocolActionKind::Send).bytes;
    require(generatedSl651[2] == 0x00 && generatedSl651[6] == 0x01 && generatedSl651[7] == 0x01 && generatedSl651[10] == 0x4C, "SL651 element command did not use the iot-manager default address header");
    const std::array<std::uint8_t, 4> encodedSl651Value{ 0x39, 0x12, 0x12, 0x34 };
    require(std::search(generatedSl651.begin(), generatedSl651.end(), encodedSl651Value.begin(), encodedSl651Value.end()) != generatedSl651.end(), "SL651 element command did not encode its guide and BCD value");
    packet.payload = slFrame(0x4C, {0, 3, 0x24, 1, 2, 3, 4, 7});
    commandActions = engine.consume(packet);
    commandActions = engine.parsedPublished("sl-connection", first(commandActions, collector::ProtocolActionKind::PublishParsed).publicationToken);
    require(has(commandActions, collector::ProtocolActionKind::CompleteCommand), "generated SL651 command did not complete on ACK");

    commandActions = engine.execute("sl-connection", { .id = "sl-negative-command", .deviceId = "sl-device", .deviceCode = "0000000001", .kind = "control", .payload = commandFrame });
    require(has(commandActions, collector::ProtocolActionKind::Send), "SL651 negative-ack test command was not sent");
    packet.payload = slFrame(0xE2);
    commandActions = engine.consume(packet);
    commandActions = engine.parsedPublished("sl-connection", first(commandActions, collector::ProtocolActionKind::PublishParsed).publicationToken);
    require(!has(commandActions, collector::ProtocolActionKind::FailCommand) &&
            !has(commandActions, collector::ProtocolActionKind::CompleteCommand),
            "SL651 user extension E2 was treated as a universal negative acknowledgement");
}

void testSl651ElementOrder() {
    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({ .id = "sl-link", .name = "SL", .mode = "TCP Server", .protocol = "SL651", .status = "enabled" });
    collector::DeviceDefinition device;
    device.id = "sl-device";
    device.code = "0001000102";
    device.linkId = "sl-link";
    device.protocol = "SL651";
    device.elements = {
        { .id = "total", .name = "Total", .functionCode = "32", .guideHex = "FFB028", .encoding = "BCD", .length = 5 },
        { .id = "velocity", .name = "Velocity", .functionCode = "32", .guideHex = "371B", .encoding = "BCD", .length = 3, .digits = 3 },
        { .id = "water", .name = "Water", .functionCode = "32", .guideHex = "3923", .encoding = "BCD", .length = 4, .digits = 3 },
        { .id = "flow", .name = "Flow", .functionCode = "32", .guideHex = "272B", .encoding = "BCD", .length = 5, .digits = 3 }
    };
    const auto frame = service::message::fromHex(
        "7E7E010001000102FFFA32003102001D260914134500F1F1000100010249F0F02609141345392300000587371B000125272B0000000000FFB0280000000003036A65");
    // 同一真实报文在所有配置顺序下都必须保留全部指标。
    std::sort(device.elements.begin(), device.elements.end(), [](const auto& left, const auto& right) { return left.id < right.id; });
    do {
        snapshot.devices = { device };
        collector::ProtocolEngine engine(sessionFactories());
        engine.reload(snapshot);
        (void)engine.connected({ .connectionId = "sl-connection", .linkId = "sl-link", .sessionEpoch = 1 });
        const auto actions = engine.consume({ .messageId = "sl-ingress", .linkId = "sl-link", .connectionId = "sl-connection", .occurredAtMs = 1000, .payload = frame });
        const auto& parsed = first(actions, collector::ProtocolActionKind::PublishParsed).parsed;
        for (const auto expected : {
                 "\"total\":{\"name\":\"Total\",\"value\":\"0000000003\"",
                 "\"velocity\":{\"name\":\"Velocity\",\"value\":\"000.125\"",
                 "\"water\":{\"name\":\"Water\",\"value\":\"00000.587\"",
                 "\"flow\":{\"name\":\"Flow\",\"value\":\"0000000.000\"" })
            require(parsed.valuesJson.find(expected) != std::string::npos, "SL651 real report lost an element with reordered configuration");
        require(parsed.rawPayloads == std::vector<std::vector<std::uint8_t>>{ frame }, "SL651 real report raw payload changed");
    } while (std::next_permutation(device.elements.begin(), device.elements.end(), [](const auto& left, const auto& right) { return left.id < right.id; }));
    require(collector::sl651::detail::hexBytes("39 23").empty(), "SL651 noncanonical HEX guide was accepted");
    require(collector::sl651::detail::hexBytes("39 X3").empty(), "SL651 invalid HEX guide was accepted");
    require(collector::sl651::detail::hexBytes("39 2").empty(), "SL651 odd-length HEX guide was accepted");
    collector::ProtocolEngine heartbeatEngine(sessionFactories());
    heartbeatEngine.reload(snapshot);
    (void)heartbeatEngine.connected({ .connectionId = "heartbeat-connection", .linkId = "sl-link", .sessionEpoch = 1 });
    const auto heartbeat = service::message::fromHex("7E7E010001000102FFFA2F000802001D26091413461003A0BA");
    const auto heartbeatActions = heartbeatEngine.consume({ .messageId = "heartbeat-ingress", .linkId = "sl-link", .connectionId = "heartbeat-connection", .occurredAtMs = 1789364770343, .payload = heartbeat });
    require(has(heartbeatActions, collector::ProtocolActionKind::BindDevice), "SL651 heartbeat did not bind the device");
    const auto& activity = first(heartbeatActions, collector::ProtocolActionKind::PublishParsed).parsed;
    require(activity.valuesJson.find("\"values\":{}") != std::string::npos, "SL651 heartbeat fabricated element values");
    require(activity.observedAtMs == 1789364770000 && activity.onlineWindowMs > 0, "SL651 heartbeat lost its online activity timing");
}

void testSl651AllEncodingsAndFunctionCodes() {
    collector::ElementDefinition element;
    element.encoding = "BCD";
    element.digits = 2;
    const std::array<std::uint8_t, 2> bcd{ 0x12, 0x34 };
    require(collector::sl651::detail::elementValue(bcd, element) == "12.34", "SL651 BCD encoding failed");
    element.encoding = "TIME_YYMMDDHHMMSS";
    const std::array<std::uint8_t, 6> time{ 0x24, 0x01, 0x02, 0x03, 0x04, 0x05 };
    require(collector::sl651::detail::elementValue(time, element) == "2024-01-02T03:04:05", "SL651 time encoding failed");
    const std::array<std::uint8_t, 3> binary{ 0x00, 0xAB, 0xFF };
    for (const auto encoding : { "HEX", "DICT" }) {
        element.encoding = encoding;
        require(collector::sl651::detail::elementValue(binary, element) == "00ABFF", "SL651 binary encoding failed");
    }
    element.encoding = "JPEG";
    require(collector::sl651::detail::elementValue(binary, element) == "INVALID_JPEG", "SL651 accepted an invalid JPEG");
    const std::array<std::uint8_t, 4> jpeg{ 0xFF, 0xD8, 0xFF, 0xD9 };
    require(collector::sl651::detail::elementValue(jpeg, element) == "data:image/jpeg;base64,/9j/2Q==", "SL651 JPEG was not stored as a Base64 data URL");

    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({ .id = "sl-function-link", .name = "SL function matrix", .mode = "TCP Server", .protocol = "SL651", .status = "enabled" });
    collector::DeviceDefinition device;
    device.id = "sl-function-device";
    device.code = "0000000001";
    device.linkId = "sl-function-link";
    device.protocol = "SL651";
    for (std::uint16_t function = 0; function <= 0xFF; ++function) {
        const auto code = collector::sl651::detail::hexByte(static_cast<std::uint8_t>(function));
        device.elements.push_back({ .id = "fc-" + code, .name = "Function " + code, .functionCode = code, .guideHex = "3908", .encoding = "HEX", .length = 1 });
    }
    snapshot.devices.push_back(std::move(device));

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    (void)engine.connected({ .connectionId = "sl-function-connection", .linkId = "sl-function-link", .sessionEpoch = 1 });
    service::message::IngressPacket packet{ .messageId = "sl-function-frame",
                                            .linkId = "sl-function-link",
                                            .connectionId = "sl-function-connection",
                                            .occurredAtMs = 7000 };
    for (std::uint16_t function = 0; function <= 0xFF; ++function) {
        const auto code = collector::sl651::detail::hexByte(static_cast<std::uint8_t>(function));
        packet.payload = slFrame(static_cast<std::uint8_t>(function), { 0, 1, 0x24, 1, 2, 3, 4, 5, 0x39, 0x08, static_cast<std::uint8_t>(function) });
        const auto actions = engine.consume(packet);
        const auto& parsed = first(actions, collector::ProtocolActionKind::PublishParsed).parsed;
        require(parsed.valuesJson.find("fc-" + code) != std::string::npos, "SL651 configured function code was not routed");
    }
}

void testSl651MultiPacketImages() {
    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({ .id = "sl-image-link", .name = "SL image", .mode = "TCP Server", .protocol = "SL651", .status = "enabled" });
    collector::DeviceDefinition device;
    device.id = "sl-image-device";
    device.code = "0000000001";
    device.linkId = "sl-image-link";
    device.protocol = "SL651";
    device.sl651ResponseMode = "M3";
    device.elements.push_back({ .id = "image", .name = "Image", .functionCode = "36", .guideHex = "F3F3", .encoding = "JPEG", .length = 0 });
    snapshot.devices.push_back(std::move(device));

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    (void)engine.connected(
        { .connectionId = "sl-image-connection", .linkId = "sl-image-link", .sessionEpoch = 1 }
    );
    service::message::IngressPacket packet{ .messageId = "sl-image-frame",
                                            .linkId = "sl-image-link",
                                            .connectionId = "sl-image-connection",
                                            .occurredAtMs = 8000 };

    const auto firstFrame = slMultiFrame(0x36, 3, 1, { 0x00, 0x01, 0x24, 0x01, 0x02 });
    const auto secondFrame = slMultiFrame(0x36, 3, 2, { 0x03, 0x04, 0x05, 0xF3, 0xF3, 0xFF });
    packet.payload = secondFrame;
    auto actions = engine.consume(packet);
    require(!has(actions, collector::ProtocolActionKind::PublishParsed), "SL651 published an incomplete image");
    const auto firstToken = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;

    packet.payload = firstFrame;
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::CancelDeadline), "SL651 did not cancel the previous image idle deadline");
    const auto secondToken = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
    require(secondToken != firstToken, "SL651 did not refresh the image idle deadline");
    require(engine.deadline("sl-image-connection", firstToken).empty(), "a stale SL651 image deadline affected the refreshed assembly");

    const auto thirdFrame = slMultiFrame(0x36, 3, 3, { 0xD8, 0xFF, 0xD9 });
    packet.payload = thirdFrame;
    actions = engine.consume(packet);
    const auto& parsed = first(actions, collector::ProtocolActionKind::PublishParsed).parsed;
    require(parsed.valuesJson.find("data:image/jpeg;base64,/9j/2Q==") != std::string::npos, "SL651 multi-packet image was not assembled and encoded");
    require(parsed.valuesJson.find("\"type\":\"JPEG\"") != std::string::npos, "SL651 image storage omitted the JPEG type");
    require(parsed.valuesJson.find("\"is_multi_packet\":true") != std::string::npos && parsed.valuesJson.find("\"total_packets\":3") != std::string::npos, "SL651 image storage omitted multi-packet metadata");
    const std::vector<std::vector<std::uint8_t>> expectedRaw{ firstFrame, secondFrame, thirdFrame };
    require(parsed.rawPayloads == expectedRaw, "SL651 multi-packet raw frames were not stored in sequence order");
    auto published = parsed;
    published.messageId = "019f91c9-4087-7e6c-88c0-c431b0dc15d8";
    const auto streamFields = service::message::parsedFields(published);
    service::message::StreamMessage streamMessage{ .id = "1-0", .fields = streamFields };
    const auto roundTrip = service::message::parsedFrom(streamMessage);
    require(roundTrip.rawPayloads == expectedRaw && streamMessage.get("raw_payload_hex") == service::message::rawPayloadsJson(expectedRaw), "parsed Redis message did not preserve the ordered HEX payload array");

    const auto oldFirst = slMultiFrame(0x36, 2, 1, { 0x00, 0x01, 0x24, 0x01 });
    packet.payload = oldFirst;
    actions = engine.consume(packet);
    const auto oldToken = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
    const auto newFirst = slMultiFrame(
        0x36,
        2,
        1,
        { 0x00, 0x01, 0x24, 0x01, 0x02, 0x03, 0x04, 0x05, 0xF3, 0xF3, 0xFF, 0xD8 }
    );
    packet.payload = newFirst;
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::CancelDeadline).deadlineToken == oldToken, "a new SL651 image did not replace the previous partial image");
    require(engine.deadline("sl-image-connection", oldToken).empty(), "the replaced SL651 image deadline affected the new image");
    const auto newLast = slMultiFrame(0x36, 2, 2, { 0xFF, 0xD9 });
    packet.payload = newLast;
    actions = engine.consume(packet);
    const auto& replacement = first(actions, collector::ProtocolActionKind::PublishParsed).parsed;
    const std::vector<std::vector<std::uint8_t>> replacementRaw{ newFirst, newLast };
    require(replacement.rawPayloads == replacementRaw, "the replaced SL651 image leaked old raw packets into storage");
}

void testSl651CommunicationModes() {
    collector::RuntimeSnapshot previous;
    previous.devices.push_back({.id = "mode-version", .protocol = "SL651"});
    auto next = previous;
    next.devices.front().sl651ResponseMode = "M4";
    require(collector::config::signature(previous) != collector::config::signature(next),
        "SL651 mode change did not invalidate runtime snapshot");
    const auto restored = collector::config::detail::device({{"protocol", "SL651"}, {"storage_policy", "report"}, {"sl651_response_mode", "M3"}});
    require(restored.sl651ResponseMode == "M3", "SL651 mode was lost when loading Redis snapshot");
    const auto refreshCrc = [](std::vector<std::uint8_t> frame, std::uint8_t ending) {
        frame[frame.size() - 3] = ending;
        const auto checksum = crc16(std::span<const std::uint8_t>(frame).first(frame.size() - 2));
        frame[frame.size() - 2] = static_cast<std::uint8_t>(checksum >> 8U);
        frame.back() = static_cast<std::uint8_t>(checksum);
        return frame;
    };
    for (const auto mode : {"M1", "M2", "M3", "M4"}) {
        collector::RuntimeSnapshot snapshot;
        snapshot.links.push_back({.id = "modes", .mode = "TCP Server", .protocol = "SL651", .status = "enabled"});
        collector::DeviceDefinition device;
        device.id = "station"; device.code = "0000000001"; device.linkId = "modes";
        device.protocol = "SL651"; device.sl651ResponseMode = mode;
        device.elements.push_back({.id = "custom", .name = "Custom", .functionCode = "E0", .guideHex = "AB12", .encoding = "BCD", .length = 2, .digits = 2});
        snapshot.devices.push_back(device);
        collector::ProtocolEngine engine(sessionFactories());
        engine.reload(snapshot);
        (void)engine.connected({.connectionId = "modes", .linkId = "modes", .sessionEpoch = 1});
        service::message::IngressPacket packet{.messageId = "mode-report", .linkId = "modes", .connectionId = "modes", .occurredAtMs = 1000};
        packet.payload = slFrame(0x32);
        auto actions = engine.consume(packet);
        require(!has(actions, collector::ProtocolActionKind::Send), "SL651 acknowledged before durable publication");
        const auto publicationToken = first(actions, collector::ProtocolActionKind::PublishParsed).publicationToken;
        require(publicationToken != 0, "SL651 report has no publication token");
        require(first(actions, collector::ProtocolActionKind::PublishParsed).parsed.messageId.empty(),
                "protocol parsing still allocates a runtime message UUID");
        require(engine.parsedPublished("modes", 0).empty(), "unknown publication token acknowledged a report");
        actions = engine.parsedPublished("modes", publicationToken);
        require(engine.parsedPublished("modes", publicationToken).empty(),
                "duplicate publication completion acknowledged a report twice");
        require(has(actions, collector::ProtocolActionKind::Send) == (std::string_view(mode) == "M2"), "SL651 mode did not control unsolicited confirmation");
        if (!actions.empty()) {
            const auto& ack = first(actions, collector::ProtocolActionKind::Send).bytes;
            require(ack[11] == 0x80 && ack[14] == 0 && ack[15] == 1 && ack[ack.size()-3] == 4, "M2 final confirmation header or serial is wrong");
        }
        packet.payload = slFrame(0xE0, {0, 1, 0x24, 1, 2, 3, 4, 5, 0x50, 0x18, 0xAB, 0x12, 0xFF, 0xAB, 0x12, 0x12, 0x34});
        auto nextReport = slFrame(0xE0, {0, 2, 0x24, 1, 2, 3, 4, 5, 0xAB, 0x12, 0x56, 0x78});
        packet.payload.insert(packet.payload.end(), nextReport.begin(), nextReport.end());
        actions = engine.consume(packet);
        std::size_t published = 0;
        for (const auto& action : actions) {
            if (action.kind != collector::ProtocolActionKind::PublishParsed) continue;
            require(action.parsed.valuesJson.find(published == 0 ? "12.34" : "56.78") != std::string::npos,
                "custom guide matched bytes inside another element");
            const auto confirmations = engine.parsedPublished("modes", action.publicationToken);
            require(has(confirmations, collector::ProtocolActionKind::Send) == (std::string_view(mode) == "M2"),
                "concatenated reports lost an independent publication confirmation");
            ++published;
        }
        require(published == 2, "concatenated SL651 reports were lost");
        packet.payload = slFrame(0x32);
        packet.payload.back() ^= 1;
        require(engine.consume(packet).empty(), "bad SL651 CRC emitted protocol actions");
        packet.payload = refreshCrc(slFrame(0x32), 5);
        require(engine.consume(packet).empty(), "upstream ENQ was accepted");
        auto request = slFrame(0x37, {0, 0, 0x24, 1, 2, 3, 4, 5});
        request[2] = 0; request[6] = 1; request[11] = 0x80;
        if (std::string_view(mode) == "M3") {
            request[13] = 0x16; request[12] += 3;
            request.insert(request.begin() + 14, {0x00, 0x10, 0x01});
        }
        request = refreshCrc(request, 5);
        const auto execute = [&](std::string id) {
            return engine.execute("modes", {.id = std::move(id), .deviceId = "station", .deviceCode = "0000000001", .kind = "control", .payload = request});
        };
        actions = execute("query");
        if (std::string_view(mode) == "M1") {
            require(has(actions, collector::ProtocolActionKind::FailCommand) && !has(actions, collector::ProtocolActionKind::Send), "M1 sent a downlink");
            packet.payload = slMultiFrame(0x36, 2, 1, {0, 2, 0x24, 1, 2, 3, 4, 5});
            require(engine.consume(packet).empty(), "M1 accepted SYN packets");
            continue;
        }
        const auto queryToken = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
        packet.payload = refreshCrc(slFrame(0x37, {0, 2, 0x24, 1, 2, 3, 4, 6}), 0x17);
        actions = engine.consume(packet);
        actions = engine.parsedPublished("modes", first(actions, collector::ProtocolActionKind::PublishParsed).publicationToken);
        require(!has(actions, collector::ProtocolActionKind::CompleteCommand), "intermediate ETB completed query");
        const auto& ack = first(actions, collector::ProtocolActionKind::Send).bytes;
        require(ack[ack.size()-3] == 6 && ack[15] == 2, "intermediate query response did not receive ACK");
        require(engine.deadline("modes", queryToken).empty(), "stale query timer retransmitted");
        packet.payload = slFrame(0x37, {0, 3, 0x24, 1, 2, 3, 4, 7});
        actions = engine.consume(packet);
        actions = engine.parsedPublished("modes", first(actions, collector::ProtocolActionKind::PublishParsed).publicationToken);
        require(first(actions, collector::ProtocolActionKind::CompleteCommand).commandId == "query", "final query response did not complete original command");
        actions = execute("next-query");
        const auto initialToken = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
        actions = engine.consume(packet);
        actions = engine.parsedPublished("modes", first(actions, collector::ProtocolActionKind::PublishParsed).publicationToken);
        require(first(actions, collector::ProtocolActionKind::CompleteCommand).commandId == "query", "duplicate final response completed a newer query");
        auto token = initialToken;
        for (int retry = 0; retry < 2; ++retry) {
            actions = engine.deadline("modes", token);
            require(first(actions, collector::ProtocolActionKind::Send).bytes == request, "query retry changed original bytes");
            token = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
        }
        actions = engine.deadline("modes", token);
        require(first(actions, collector::ProtocolActionKind::FailCommand).commandId == "next-query", "query did not fail after two retries");
        require(has(execute("ambiguous-query"), collector::ProtocolActionKind::FailCommand), "timed-out station accepted an ambiguous query");

        packet.payload = slMultiFrame(0x36, 3, 1, {0, 4, 0x24, 1, 2, 3, 4, 8, 0xF3, 0xF3, 0xFF});
        (void)engine.consume(packet);
        (void)engine.consume(packet); // 相同首包不得清除后续包。
        packet.payload = slMultiFrame(0x36, 3, 3, {0xD9});
        actions = engine.consume(packet);
        const auto& nak = first(actions, collector::ProtocolActionKind::Send).bytes;
        require(nak[13] == 0x16 && nak[nak.size()-3] == 0x15 && nak[16] == 2, "M3 did not request the missing sequence");
        require(!has(actions, collector::ProtocolActionKind::PublishParsed), "M3 published before all packets arrived");
        packet.payload = refreshCrc(slMultiFrame(0x36, 3, 2, {0xD8, 0xFF}), 3);
        actions = engine.consume(packet);
        require(!has(actions, collector::ProtocolActionKind::Send), "M3 confirmed before durable publication");
        actions = engine.parsedPublished("modes", first(actions, collector::ProtocolActionKind::PublishParsed).publicationToken);
        const auto& finalAck = first(actions, collector::ProtocolActionKind::Send).bytes;
        require(finalAck[13] == 0x16 && finalAck[16] == 3 && finalAck[finalAck.size()-3] == 4, "M3 final acknowledgement has wrong sequence or ending");
        packet.payload = slMultiFrame(0x36, 2, 1, {0, 5, 0x24, 1, 2, 3, 4, 9});
        actions = engine.consume(packet);
        auto missingToken = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
        for (int retry = 0; retry < 2; ++retry) {
            actions = engine.deadline("modes", missingToken);
            const auto& missingAck = first(actions, collector::ProtocolActionKind::Send).bytes;
            require(missingAck[16] == 2 && missingAck[missingAck.size()-3] == 0x15,
                "M3 timeout did not request its missing packet");
            missingToken = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
        }
        require(!has(engine.deadline("modes", missingToken), collector::ProtocolActionKind::Send), "M3 exceeded two missing-packet retries");
    }
    collector::ElementDefinition numeric{.name = "signed", .encoding = "BCD", .length = 3, .digits = 2};
    collector::command::validateValue(numeric, "-12.34");
    const std::vector<std::uint8_t> signedBcd{0xFF, 0x12, 0x34};
    require(collector::sl651::detail::encodeValue(numeric, "-12.34") == signedBcd &&
        collector::sl651::detail::elementValue(signedBcd, numeric) == "-12.34", "SL651 signed BCD did not round trip");
}

void testSl651FixedPosition() {
    using EdgeTelemetryProjection = service::edge::protocol::TelemetryValues;
    service::edge::pb::TelemetryRecord binaryRecord;
    binaryRecord.set_protocol(service::edge::pb::PROTOCOL_SL651);
    auto* binaryValue = binaryRecord.add_values();
    binaryValue->set_element_id("image");
    binaryValue->set_encoding("JPEG");
    binaryValue->set_encoded_value(std::string("\xFF\xD8\xFF\xD9", 4));
    require(EdgeTelemetryProjection::telemetryJson(binaryRecord).find("data:image/jpeg;base64,/9j/2Q==") != std::string::npos,
            "edge SL651 binary image was not restored");
    binaryValue->set_encoding("HEX");
    require(EdgeTelemetryProjection::telemetryJson(binaryRecord).find("FFD8FFD9") != std::string::npos,
            "edge SL651 binary hex was not restored");
    binaryValue->set_encoded_value(std::string(8193, 'x'));
    bool oversizedRejected = false;
    try { (void)EdgeTelemetryProjection::telemetryJson(binaryRecord); }
    catch (const std::runtime_error&) { oversizedRejected = true; }
    require(oversizedRejected, "edge SL651 binary limit was not enforced");
    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({.id = "fixed", .mode = "TCP Server", .protocol = "SL651", .status = "enabled"});
    collector::DeviceDefinition device;
    device.id = "fixed"; device.code = "0000000001"; device.linkId = "fixed"; device.protocol = "SL651"; device.sl651ResponseMode = "M2";
    device.elements = {
        {.id = "water", .name = "Fixed water", .functionCode = "32", .encoding = "BCD", .length = 2, .digits = 2, .positionMode = "OFFSET", .byteOffset = 8},
        {.id = "serial", .name = "Serial", .functionCode = "32", .encoding = "HEX", .length = 2, .positionMode = "OFFSET", .byteOffset = 0},
        {.id = "outside", .name = "Outside", .functionCode = "32", .encoding = "HEX", .length = 2, .positionMode = "OFFSET", .byteOffset = 10},
        {.id = "prefix", .name = "Prefix", .functionCode = "33", .encoding = "BCD", .length = 2, .digits = 2, .positionMode = "OFFSET", .byteOffset = 8},
        {.id = "guided", .name = "Guided", .functionCode = "33", .guideHex = "AB12", .encoding = "BCD", .length = 2, .digits = 2},
        {.id = "write", .name = "Fixed write", .functionCode = "4C", .direction = "DOWN", .encoding = "BCD", .length = 2, .digits = 2, .positionMode = "OFFSET", .byteOffset = 10}
    };
    snapshot.devices.push_back(device);
    collector::ProtocolEngine engine(sessionFactories()); engine.reload(snapshot);
    (void)engine.connected({.connectionId = "fixed", .linkId = "fixed", .sessionEpoch = 1});
    service::message::IngressPacket packet{.messageId = "fixed-report", .linkId = "fixed", .connectionId = "fixed", .occurredAtMs = 1000};
    packet.payload = slFrame(0x32, {0, 1, 0x24, 1, 2, 3, 4, 5, 0x12, 0x34});
    auto actions = engine.consume(packet);
    auto values = first(actions, collector::ProtocolActionKind::PublishParsed).parsed.valuesJson;
    require(values.find("12.34") != std::string::npos && values.find("0001") != std::string::npos && values.find("Outside") == std::string::npos, "SL651 fixed positions or bounds were not respected");
    packet.payload = slMultiFrame(0x32, 2, 1, {0, 2, 0x24, 1, 2, 3, 4, 5, 0x56});
    (void)engine.consume(packet); packet.payload = slMultiFrame(0x32, 2, 2, {0x78});
    actions = engine.consume(packet); values = first(actions, collector::ProtocolActionKind::PublishParsed).parsed.valuesJson;
    require(values.find("56.78") != std::string::npos, "SL651 fixed offset was applied before reassembly");
    packet.payload = slFrame(0x33, {0, 3, 0x24, 1, 2, 3, 4, 5, 0x12, 0x34, 0xAB, 0x12, 0x56, 0x78});
    actions = engine.consume(packet);
    values = first(actions, collector::ProtocolActionKind::PublishParsed).parsed.valuesJson;
    require(values.find("12.34") != std::string::npos && values.find("56.78") != std::string::npos,
            "SL651 mixed fixed prefix and guided elements were not decoded");
    actions = engine.execute("fixed", {.id = "fixed-command", .deviceId = "fixed", .deviceCode = "0000000001", .kind = "command", .elements = {{.elementId = "write", .value = "12.34"}}});
    const auto& bytes = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(bytes[22] == 0 && bytes[23] == 0 && bytes[24] == 0x12 && bytes[25] == 0x34, "SL651 fixed command did not use body-relative byte offsets");
}

void testModbus() {
    collector::RuntimeSnapshot snapshot;
    collector::LinkDefinition link{ .id = "modbus-link",
                                    .name = "Modbus",
                                    .mode = "TCP Client",
                                    .protocol = "Modbus",
                                    .status = "enabled" };
    link.targets.push_back({ .id = "target-1", .name = "Target", .ip = "127.0.0.1", .port = 15002, .status = "enabled" });
    snapshot.links.push_back(link);
    collector::DeviceDefinition device;
    device.id = "modbus-device";
    device.code = "MODBUS-1";
    device.linkId = "modbus-link";
    device.linkMode = "TCP Client";
    device.targetId = "target-1";
    device.protocol = "Modbus";
    device.modbusMode = "TCP";
    device.slaveId = 1;
    device.elements.push_back({ .id = "holding-0", .name = "Holding", .unit = "V", .dataType = "UINT16", .byteOrder = "BIG_ENDIAN", .registerType = "HOLDING_REGISTER", .address = 0, .quantity = 1, .writable = true });
    device.elements.push_back({ .id = "holding-2", .name = "Holding 2", .dataType = "UINT16", .byteOrder = "BIG_ENDIAN", .registerType = "HOLDING_REGISTER", .address = 2, .quantity = 1 });
    snapshot.devices.push_back(device);

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    auto connected = engine.connected({ .connectionId = "modbus-connection", .linkId = "modbus-link", .remoteAddress = "127.0.0.1:15002", .targetId = "target-1", .sessionEpoch = 1 });
    require(has(connected, collector::ProtocolActionKind::BindDevice), "Modbus client target was not bound");
    require(has(connected, collector::ProtocolActionKind::Send), "Modbus did not trigger the first poll immediately after binding");
    service::message::IngressPacket packet{ .messageId = "modbus-ingress",
                                            .linkId = "modbus-link",
                                            .connectionId = "modbus-connection",
                                            .remoteAddress = "127.0.0.1:15002",
                                            .occurredAtMs = 2000 };
    connected = drainInitialModbusPoll(engine, std::move(connected), packet, true);
    const auto pollToken = first(connected, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;

    auto actions = engine.execute("modbus-connection", { .id = "modbus-read", .deviceId = "modbus-device", .deviceCode = "MODBUS-1", .kind = "read", .payload = modbusRead(1) });
    require(has(actions, collector::ProtocolActionKind::Send), "Modbus read was not dispatched");
    packet.payload = modbusReadResponse(1);
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::PublishParsed), "Modbus response was not parsed");
    require(first(actions, collector::ProtocolActionKind::PublishParsed).parsed.rawPayloads ==
                std::vector<std::vector<std::uint8_t>>{packet.payload},
            "Modbus history did not retain its exact response as an array");
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "Modbus read did not complete");

    actions = engine.execute("modbus-connection", { .id = "invalid-write", .deviceId = "modbus-device", .deviceCode = "MODBUS-1", .kind = "write", .payload = modbusWrite(2) });
    require(first(actions, collector::ProtocolActionKind::FailCommand).reason == "modbus_write_readback_required", "Modbus accepted a write without readback");

    actions = engine.execute("modbus-connection", { .id = "verified-write", .deviceId = "modbus-device", .deviceCode = "MODBUS-1", .kind = "write", .payload = modbusWrite(3), .readbackPayload = modbusRead(4), .expectedReadbackData = { 0x12, 0x34 } });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusWrite(3), "Modbus write was not sent first");
    packet.payload = modbusWrite(3);
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRead(4), "Modbus write response did not trigger readback");
    packet.payload = modbusReadResponse(4);
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "Modbus verified write did not complete");

    actions = engine.execute("modbus-connection", { .id = "element-write", .deviceId = "modbus-device", .deviceCode = "MODBUS-1", .kind = "command", .elements = { { .elementId = "holding-0", .value = "4660" } } });
    const auto generatedWrite = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(generatedWrite.size() == 12 && generatedWrite[7] == 6 && generatedWrite[10] == 0x12 && generatedWrite[11] == 0x34, "Modbus element command did not compile FC06");
    packet.payload = generatedWrite;
    actions = engine.consume(packet);
    const auto generatedReadback = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(generatedReadback.size() == 12 && generatedReadback[7] == 3, "Modbus element command did not compile FC03 readback");
    const auto readbackTransaction = static_cast<std::uint16_t>(generatedReadback[0] << 8U) |
        generatedReadback[1];
    packet.payload = modbusReadResponse(readbackTransaction);
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "Modbus element command did not complete after readback");

    actions = engine.execute("modbus-connection", { .id = "modbus-write-lost-ack", .deviceId = "modbus-device", .deviceCode = "MODBUS-1", .kind = "write", .payload = modbusWrite(5), .readbackPayload = modbusRead(6), .expectedReadbackData = { 0x12, 0x34 } });
    const auto lostAckWriteDeadline = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
    actions = engine.deadline("modbus-connection", lostAckWriteDeadline);
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRead(6), "Modbus missing write ACK did not proceed to readback");
    packet.payload = modbusReadResponse(6);
    actions = engine.consume(packet);
    const auto& lostAckResult = first(actions, collector::ProtocolActionKind::FailCommand);
    require(lostAckResult.reason == "write_ack_missing" &&
                service::command::collectorResultState(false, lostAckResult.reason) == "UNKNOWN" &&
                !service::command::retryableFailure(lostAckResult.reason),
            "Modbus write with a missing ACK was marked successful or retried despite matching readback");

    actions = engine.execute("modbus-connection", { .id = "modbus-readback-timeout", .deviceId = "modbus-device", .deviceCode = "MODBUS-1", .kind = "write", .payload = modbusWrite(7), .readbackPayload = modbusRead(8), .expectedReadbackData = { 0x12, 0x34 } });
    packet.payload = modbusWrite(7);
    actions = engine.consume(packet);
    const auto readbackDeadline = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
    actions = engine.deadline("modbus-connection", readbackDeadline);
    const auto& readbackFailure = first(actions, collector::ProtocolActionKind::FailCommand);
    require(readbackFailure.reason == "modbus_readback_timeout" &&
                service::command::collectorResultState(false, readbackFailure.reason) == "UNKNOWN" &&
                !service::command::retryableFailure(readbackFailure.reason),
            "Modbus readback timeout after a confirmed write ACK was rejected or retried");

    actions = engine.deadline("modbus-connection", pollToken);
    require(has(actions, collector::ProtocolActionKind::Send), "Modbus periodic poll was not generated by its session");
    const auto& poll = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(poll.size() == 12 && poll[7] == 3 && poll[10] == 0 && poll[11] == 3, "Modbus register mergeGap did not combine one read range");
}

void testAcquisitionCycle() {
    collector::AcquisitionCycle cycle;
    std::vector<collector::ProtocolAction> actions;
    collector::ProtocolAction response;
    response.parsed.rawPayloads = {{1, 2}};
    response.parsed.rawPacketIds = {"ingress-1:frame:0"};
    response.parsed.valuesJson = R"({"values":{"a":{"value":1}}})";
    (void)cycle.id("test-connection");
    cycle.observe(response, actions);
    const auto firstId = actions.back().parsed.rawPacketIds.front();
    require(actions.back().kind == collector::ProtocolActionKind::ObserveParsed,
            "partial cycle published a history record");
    response.parsed.rawPayloads = {{3, 4}};
    response.parsed.rawPacketIds = {"ingress-2:frame:1"};
    response.parsed.valuesJson = R"({"values":{"b":{"value":2}}})";
    (void)cycle.id("test-connection");
    cycle.observe(response, actions);
    const auto secondId = actions.back().parsed.rawPacketIds.front();
    cycle.finish(actions);
    const auto& record = first(actions, collector::ProtocolActionKind::PublishParsed);
    require(record.kind == collector::ProtocolActionKind::PublishParsed &&
            record.parsed.rawPayloads == std::vector<std::vector<std::uint8_t>>{{1, 2}, {3, 4}} &&
            record.parsed.rawPacketIds == std::vector<std::string>{firstId, secondId} && firstId != secondId,
            "cycle lost response ordering or physical packet identity");
    require(record.parsed.valuesJson.find("\"a\"") != std::string::npos &&
            record.parsed.valuesJson.find("\"b\"") != std::string::npos,
            "cycle did not merge all parsed points");
    actions.clear();
    (void)cycle.id("test-connection");
    cycle.observe(response, actions);
    cycle.fail();
    cycle.finish(actions);
    require(has(actions, collector::ProtocolActionKind::DiscardCollection),
            "failed cycle was persisted as complete history");
    actions.clear();
    (void)cycle.id("test-connection");
    cycle.observe(response, actions);
    cycle.finish(actions);
    require(has(actions, collector::ProtocolActionKind::PublishParsed) &&
            first(actions, collector::ProtocolActionKind::PublishParsed).parsed.rawPayloads.size() == 1,
            "failed cycle contaminated the next collection");
    actions.clear();
    const auto failedId = cycle.id("test-connection");
    cycle.fail();
    cycle.finish(actions);
    require(actions.size() == 1 && actions[0].kind == collector::ProtocolActionKind::FinishAcquisition &&
            actions[0].acquisitionId == failedId && actions[0].reason == "failed",
            "zero-response timeout disappeared instead of completing its round");
    require(cycle.id("test-connection") != failedId, "new scan reused a completed acquisition identity");
}

void testModbusTypesAndPriority() {
    collector::ElementDefinition element;
    element.dataType = "UINT64";
    element.byteOrder = "BIG_ENDIAN";
    const std::array<std::uint8_t, 8> maximum{ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
    require(collector::register_value::numericJson(maximum, element) == "18446744073709551615", "Modbus UINT64 lost integer precision");
    element.dataType = "UINT32";
    element.byteOrder = "BIG_ENDIAN_BYTE_SWAP";
    const std::array<std::uint8_t, 4> bigSwap{ 0x34, 0x12, 0x78, 0x56 };
    require(collector::register_value::numericJson(bigSwap, element) == "305419896", "Modbus BIG_ENDIAN_BYTE_SWAP was decoded incorrectly");
    element.byteOrder = "LITTLE_ENDIAN_BYTE_SWAP";
    const std::array<std::uint8_t, 4> littleSwap{ 0x56, 0x78, 0x12, 0x34 };
    require(collector::register_value::numericJson(littleSwap, element) == "305419896", "Modbus LITTLE_ENDIAN_BYTE_SWAP was decoded incorrectly");

    collector::RuntimeSnapshot snapshot;
    collector::LinkDefinition link{ .id = "priority-link",
                                    .name = "Priority",
                                    .mode = "TCP Client",
                                    .protocol = "Modbus",
                                    .status = "enabled" };
    link.targets.push_back({ .id = "priority-target", .name = "Target", .ip = "127.0.0.1", .port = 15004, .status = "enabled" });
    snapshot.links.push_back(link);
    collector::DeviceDefinition device;
    device.id = "priority-device";
    device.code = "PRIORITY-1";
    device.linkId = link.id;
    device.linkMode = link.mode;
    device.targetId = "priority-target";
    device.protocol = "Modbus";
    device.modbusMode = "TCP";
    device.elements.push_back({ .id = "coil", .name = "Coil", .dataType = "BOOL", .registerType = "COIL", .address = 0, .quantity = 1 });
    snapshot.devices.push_back(device);

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    auto connected = engine.connected({ .connectionId = "priority-connection", .linkId = link.id, .targetId = "priority-target", .sessionEpoch = 1 });
    service::message::IngressPacket packet{ .messageId = "initial-poll-response",
                                            .linkId = link.id,
                                            .connectionId = "priority-connection",
                                            .occurredAtMs = 4999 };
    (void)drainInitialModbusPoll(engine, std::move(connected), packet, true);
    auto actions = engine.execute("priority-connection", { .id = "active-read", .deviceId = device.id, .deviceCode = device.code, .kind = "read", .payload = modbusRead(10, 1) });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRead(10, 1), "Modbus active read was not dispatched");
    actions = engine.execute("priority-connection", { .id = "normal-read", .deviceId = device.id, .deviceCode = device.code, .kind = "read", .payload = modbusRead(11, 1), .highPriority = false });
    require(actions.empty(), "Modbus normal read bypassed the in-flight request");
    actions = engine.execute("priority-connection", { .id = "high-write", .deviceId = device.id, .deviceCode = device.code, .kind = "write", .payload = modbusWrite(12), .readbackPayload = modbusRead(13), .expectedReadbackData = { 0x12, 0x34 }, .highPriority = true });
    require(actions.empty(), "Modbus high write bypassed the in-flight request");
    packet.messageId = "active-response";
    packet.occurredAtMs = 5000;
    packet.payload = modbusReadResponse(10, 1, { 0x01 });
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::PublishParsed).parsed.valuesJson.find("\"value\":1") != std::string::npos, "Modbus coil response was not parsed bitwise");
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusWrite(12), "Modbus high-priority write did not jump ahead of a normal read");
    packet.payload = modbusWrite(12);
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRead(13), "Modbus high-priority write did not enter atomic readback");
    packet.payload = modbusReadResponse(13);
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRead(11, 1), "Modbus normal read was not resumed after write readback");
}

void testModbusAllDataTypesAndByteOrders() {
    struct TypeCase {
        std::string_view type;
        std::vector<std::uint8_t> canonical;
        std::string_view expected;
    };

    const std::array cases{
        TypeCase{ "BOOL", { 0x01 }, "1" },
        TypeCase{ "INT16", { 0xFF, 0xFE }, "-2" },
        TypeCase{ "UINT16", { 0x12, 0x34 }, "4660" },
        TypeCase{ "INT32", { 0xFF, 0xFF, 0xFF, 0xFE }, "-2" },
        TypeCase{ "UINT32", { 0x12, 0x34, 0x56, 0x78 }, "305419896" },
        TypeCase{ "FLOAT32", { 0x3F, 0xC0, 0x00, 0x00 }, "1.5" },
        TypeCase{ "INT64", { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE }, "-2" },
        TypeCase{ "UINT64", { 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF }, "81985529216486895" },
        TypeCase{ "DOUBLE", { 0x3F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, "1.5" },
    };
    const std::array<std::string_view, 4> orders{ "BIG_ENDIAN", "LITTLE_ENDIAN", "BIG_ENDIAN_BYTE_SWAP", "LITTLE_ENDIAN_BYTE_SWAP" };
    for (const auto& current : cases) {
        collector::ElementDefinition element;
        element.dataType = current.type;
        for (const auto order : orders) {
            element.byteOrder = order;
            const auto wire = collector::register_value::orderedBytes(current.canonical, order);
            const auto decoded = collector::register_value::numericJson(wire, element);
            require(decoded && *decoded == current.expected, "Modbus data type or byte order matrix failed");
        }
    }

    collector::ElementDefinition scaled;
    scaled.dataType = "UINT16";
    scaled.byteOrder = "BIG_ENDIAN";
    scaled.scale = 0.1;
    scaled.decimals = 1;
    const std::array<std::uint8_t, 2> raw{ 0x04, 0xD2 };
    require(collector::register_value::numericJson(raw, scaled) == "123.4", "Modbus scale and decimals were not applied");
}

void testModbusAllFunctionCodes(bool tcp) {
    collector::RuntimeSnapshot snapshot;
    collector::LinkDefinition link{ .id = tcp ? "modbus-tcp-matrix" : "modbus-rtu-matrix",
                                    .name = "Modbus function matrix",
                                    .mode = "TCP Client",
                                    .protocol = "Modbus",
                                    .status = "enabled" };
    link.targets.push_back({ .id = "matrix-target", .name = "Matrix target", .ip = "127.0.0.1", .port = 15005, .status = "enabled" });
    snapshot.links.push_back(link);
    collector::DeviceDefinition device;
    device.id = tcp ? "modbus-tcp-device" : "modbus-rtu-device-matrix";
    device.code = tcp ? "MODBUS-TCP-MATRIX" : "MODBUS-RTU-MATRIX";
    device.linkId = link.id;
    device.linkMode = link.mode;
    device.targetId = "matrix-target";
    device.protocol = "Modbus";
    device.modbusMode = tcp ? "TCP" : "RTU";
    device.slaveId = 1;
    device.elements = {
        { .id = "coil", .name = "Coil", .dataType = "BOOL", .registerType = "COIL", .quantity = 1 },
        { .id = "discrete",
          .name = "Discrete",
          .dataType = "BOOL",
          .registerType = "DISCRETE_INPUT",
          .quantity = 1 },
        { .id = "holding",
          .name = "Holding",
          .dataType = "UINT16",
          .registerType = "HOLDING_REGISTER",
          .quantity = 1 },
        { .id = "input",
          .name = "Input",
          .dataType = "UINT16",
          .registerType = "INPUT_REGISTER",
          .quantity = 1 },
    };
    snapshot.devices.push_back(device);

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    service::message::IngressPacket packet{ .messageId = "modbus-matrix-response",
                                            .linkId = link.id,
                                            .connectionId = "modbus-matrix-connection",
                                            .occurredAtMs = 6000 };
    auto connected = engine.connected({ .connectionId = "modbus-matrix-connection", .linkId = link.id, .targetId = "matrix-target", .sessionEpoch = 1 });
    (void)drainInitialModbusPoll(engine, std::move(connected), packet, tcp);
    std::uint16_t transaction = 100;
    for (const auto function : std::array<std::uint8_t, 8>{ 1, 2, 3, 4, 5, 6, 15, 16 }) {
        const auto request = modbusRequest(tcp, transaction, function);
        collector::ProtocolCommand command{
            .id = "fc-" + std::to_string(function),
            .deviceId = device.id,
            .deviceCode = device.code,
            .kind = function == 5 || function == 6 || function == 15 || function == 16 ? "write"
                                                                                       : "read",
            .payload = request
        };
        const auto write = command.kind == "write";
        std::uint8_t readFunction = 0;
        std::uint16_t readTransaction = 0;
        if (write) {
            readFunction = function == 5 || function == 15 ? 1 : 3;
            readTransaction = ++transaction;
            command.readbackPayload = modbusRequest(tcp, readTransaction, readFunction);
            command.expectedReadbackData = readFunction == 1
                ? std::vector<std::uint8_t>{ 0x01 }
                : std::vector<std::uint8_t>{ 0x12, 0x34 };
        }
        auto actions = engine.execute("modbus-matrix-connection", std::move(command));
        require(first(actions, collector::ProtocolActionKind::Send).bytes == request, "Modbus function code request was not dispatched");
        packet.payload = modbusResponse(tcp, transaction - (write ? 1 : 0), function);
        actions = engine.consume(packet);
        if (write) {
            require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRequest(tcp, readTransaction, readFunction), "Modbus write function did not trigger readback");
            packet.payload = modbusResponse(tcp, readTransaction, readFunction);
            actions = engine.consume(packet);
        }
        require(has(actions, collector::ProtocolActionKind::CompleteCommand), "Modbus function code did not complete");
        ++transaction;
    }
}

void testModbusRtuZeroAddress() {
    collector::RuntimeSnapshot snapshot;
    collector::LinkDefinition link{ .id = "modbus-rtu-link",
                                    .name = "Modbus RTU",
                                    .mode = "TCP Client",
                                    .protocol = "Modbus",
                                    .status = "enabled" };
    link.targets.push_back({ .id = "rtu-target", .name = "RTU Target", .ip = "127.0.0.1", .port = 15003, .status = "enabled" });
    snapshot.links.push_back(link);
    collector::DeviceDefinition device;
    device.id = "modbus-rtu-device";
    device.code = "MODBUS-RTU-1";
    device.linkId = link.id;
    device.linkMode = link.mode;
    device.targetId = "rtu-target";
    device.protocol = "Modbus";
    device.modbusMode = "RTU";
    device.slaveId = 1;
    device.elements.push_back({ .id = "rtu-holding-0", .name = "Holding", .dataType = "UINT16", .registerType = "HOLDING_REGISTER", .address = 0, .quantity = 1 });
    snapshot.devices.push_back(device);

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    auto connected = engine.connected({ .connectionId = "modbus-rtu-connection", .linkId = link.id, .targetId = "rtu-target", .sessionEpoch = 1 });
    service::message::IngressPacket packet{ .messageId = "rtu-initial-poll-response",
                                            .linkId = link.id,
                                            .connectionId = "modbus-rtu-connection",
                                            .occurredAtMs = 2999 };
    (void)drainInitialModbusPoll(engine, std::move(connected), packet, false);
    auto actions = engine.execute("modbus-rtu-connection", { .id = "rtu-read", .deviceId = device.id, .deviceCode = device.code, .kind = "read", .payload = modbusRtuRead() });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRtuRead(), "zero-address Modbus RTU read was mistaken for TCP");
    packet.messageId = "rtu-read-response";
    packet.occurredAtMs = 3000;
    packet.payload = modbusRtuReadResponse();
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "zero-address Modbus RTU response did not complete");

    actions = engine.execute("modbus-rtu-connection", { .id = "rtu-write", .deviceId = device.id, .deviceCode = device.code, .kind = "write", .payload = modbusRtuWrite(), .readbackPayload = modbusRtuRead(), .expectedReadbackData = { 0x12, 0x34 } });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRtuWrite(), "zero-address Modbus RTU write was mistaken for TCP");
    packet.messageId = "rtu-write-response";
    packet.payload = modbusRtuWrite();
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::Send).bytes == modbusRtuRead(), "Modbus RTU write did not trigger readback");
    packet.messageId = "rtu-readback-response";
    packet.payload = modbusRtuReadResponse();
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "Modbus RTU write readback did not complete");
}

void testModbusDiscoveryAndOffline() {
    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({ .id = "modbus-server", .name = "Modbus Server", .mode = "TCP Server", .protocol = "Modbus", .status = "enabled" });
    for (std::uint8_t unit = 1; unit <= 2; ++unit) {
        collector::DeviceDefinition device;
        device.id = "discovery-device-" + std::to_string(unit);
        device.code = "DISCOVERY" + std::to_string(unit);
        device.linkId = "modbus-server";
        device.linkMode = "TCP Server";
        device.protocol = "Modbus";
        device.modbusMode = "TCP";
        device.slaveId = unit;
        device.onlineTimeout = 1;
        snapshot.devices.push_back(std::move(device));
    }
    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    (void)engine.connected(
        { .connectionId = "discovery-connection", .linkId = "modbus-server", .sessionEpoch = 1 }
    );
    auto actions =
        engine.execute("discovery-connection", { .id = "discovery-command", .transport = "TCP", .kind = "discovery", .payload = modbusRead(11), .timeout = std::chrono::milliseconds(500) });
    const auto discoveryToken =
        first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
    service::message::IngressPacket packet{ .messageId = "discovery-response-1",
                                            .linkId = "modbus-server",
                                            .connectionId = "discovery-connection",
                                            .occurredAtMs = 4000,
                                            .payload = modbusReadResponse(11) };
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::BindDevice).deviceCode == "DISCOVERY1", "Modbus discovery did not register the first response");
    packet.messageId = "discovery-response-2";
    packet.payload[6] = 2;
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::BindDevice).deviceCode == "DISCOVERY2", "Modbus discovery did not register the second response");
    actions = engine.deadline("discovery-connection", discoveryToken);
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "Modbus discovery window did not complete");

    collector::RuntimeSnapshot registrationSnapshot;
    registrationSnapshot.links = snapshot.links;
    auto registeredDevice = snapshot.devices.front();
    registeredDevice.registrationMode = "ASCII";
    registeredDevice.registrationBytes = { 'R', 'E', 'G' };
    registrationSnapshot.devices.push_back(registeredDevice);
    auto conflictingDevice = registeredDevice;
    conflictingDevice.id = "registration-conflict";
    conflictingDevice.code = "REGISTRATION-CONFLICT";
    conflictingDevice.slaveId = 2;
    conflictingDevice.registrationBytes = { 'B', 'A', 'D' };
    registrationSnapshot.devices.push_back(conflictingDevice);
    engine.reload(registrationSnapshot);
    (void)engine.connected(
        { .connectionId = "registration-connection", .linkId = "modbus-server", .sessionEpoch = 2 }
    );
    packet.connectionId = "registration-connection";
    packet.payload = { 'R', 'E', 'G' };
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::BindDevice), "Modbus standalone registration packet did not bind the device");
    require(!has(actions, collector::ProtocolActionKind::ScheduleDeadline), "Modbus incorrectly applied a device-online timeout to the persistent DTU socket");
    actions = engine.consume(packet);
    require(!has(actions, collector::ProtocolActionKind::Close), "Modbus repeated registration packet closed its own DTU connection");
    packet.payload = { 'B', 'A', 'D' };
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::Close).reason == "modbus_registration_conflict", "Modbus conflicting registration was not rejected");

    (void)engine.connected(
        { .connectionId = "prefixed-registration", .linkId = "modbus-server", .sessionEpoch = 3 }
    );
    packet.connectionId = "prefixed-registration";
    packet.payload = { 'R', 'E', 'G' };
    const auto prefixedResponse = modbusReadResponse(11);
    packet.payload.insert(packet.payload.end(), prefixedResponse.begin(), prefixedResponse.end());
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::BindDevice) && !has(actions, collector::ProtocolActionKind::Close), "Modbus registration prefix did not preserve the payload after binding");
}

void testS7() {
    collector::RuntimeSnapshot snapshot;
    collector::LinkDefinition link{
        .id = "s7-link",
        .name = "S7",
        .mode = "TCP Client",
        .protocol = "S7",
        .status = "enabled"
    };
    link.targets.push_back(
        { .id = "target-1", .name = "PLC", .ip = "127.0.0.1", .port = 102, .status = "enabled" }
    );
    snapshot.links.push_back(link);
    collector::DeviceDefinition device;
    device.id = "s7-device";
    device.code = "S7-1";
    device.linkId = "s7-link";
    device.linkMode = "TCP Client";
    device.targetId = "target-1";
    device.protocol = "S7";
    device.elements.push_back({ .id = "db1-word", .name = "DB1 Word", .unit = "", .dataType = "UINT16", .area = "DB", .dbNumber = 1, .start = 0, .size = 2, .writable = true });
    device.elements.push_back({ .id = "v-lreal", .name = "V LREAL", .dataType = "LREAL", .area = "V", .start = 4, .size = 8 });
    device.elements.push_back({ .id = "marker-float", .name = "Marker float", .dataType = "FLOAT", .area = "MK", .start = 0, .size = 4 });
    device.elements.push_back({ .id = "input-bit", .name = "Input bit", .dataType = "BOOL", .area = "PE", .start = 2, .startBit = 3, .size = 1 });
    device.elements.push_back({ .id = "output-bit", .name = "Output bit", .dataType = "BOOL", .area = "PA", .start = 3, .startBit = 2, .size = 1, .writable = true });
    device.elements.push_back({ .id = "counter", .name = "Counter", .dataType = "UINT16", .area = "CT", .start = 1, .size = 2 });
    device.elements.push_back({ .id = "timer", .name = "Timer", .dataType = "UINT16", .area = "TM", .start = 1, .size = 2 });
    snapshot.devices.push_back(device);

    const std::vector<std::uint8_t> expectedCotp{ 0x03, 0x00, 0x00, 0x16, 0x11, 0xE0, 0x00, 0x00, 0x00, 0x01, 0x00, 0xC0, 0x01, 0x0A, 0xC1, 0x02, 0x01, 0x00, 0xC2, 0x02, 0x01, 0x01 };
    const std::vector<std::uint8_t> expectedSetup{ 0x03, 0x00, 0x00, 0x19, 0x02, 0xF0, 0x80, 0x32, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0xF0, 0x00, 0x00, 0x01, 0x00, 0x01, 0x01, 0xE0 };
    const std::vector<std::uint8_t> expectedDisconnect{ 0x03, 0x00, 0x00, 0x0B, 0x06, 0x80, 0x00, 0x06, 0x00, 0x01, 0x00 };

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(snapshot);
    service::message::IngressPacket packet{ .messageId = "s7-ingress",
                                            .linkId = "s7-link",
                                            .connectionId = "s7-connection",
                                            .remoteAddress = "127.0.0.1:102",
                                            .occurredAtMs = 3000 };
    const auto finishHandshake = [&]() {
        packet.payload = s7CotpConfirm();
        auto handshake = engine.consume(packet);
        require(first(handshake, collector::ProtocolActionKind::Send).bytes == expectedSetup, "S7 Setup Communication packet does not match iot-manager");
        require(!has(handshake, collector::ProtocolActionKind::ScheduleDeadline), "S7 handshake timeout was incorrectly restarted after ISO-CC");
        packet.payload = s7SetupResponse();
        return engine.consume(packet);
    };

    auto actions = engine.connected({ .connectionId = "s7-connection", .linkId = "s7-link", .remoteAddress = "127.0.0.1:102", .targetId = "target-1", .sessionEpoch = 1 });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 ISO-CR packet does not match iot-manager");
    require(first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineAfter == std::chrono::seconds(5), "S7 handshake timeout does not match iot-manager");
    require(!has(actions, collector::ProtocolActionKind::BindDevice), "S7 device became online before protocol negotiation");

    actions = finishHandshake();
    require(has(actions, collector::ProtocolActionKind::BindDevice), "S7 device was not bound after protocol negotiation");
    const auto immediatePoll = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(immediatePoll[17] == 0x04, "S7 did not trigger the first poll immediately after registration");
    require(immediatePoll.size() == 19 + 7 * 12 && immediatePoll[18] == 7, "S7 initial poll did not batch elements within the negotiated PDU");
    packet.payload = s7ReadResponseForRequest(immediatePoll);
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::PublishParsed), "S7 immediate poll response was not parsed");
    require(first(actions, collector::ProtocolActionKind::PublishParsed).parsed.rawPayloads ==
                std::vector<std::vector<std::uint8_t>>{packet.payload},
            "S7 multi-element history did not retain its exact response as an array");
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedDisconnect, "S7 did not send the iot-manager ISO-DR packet after polling");
    const auto pollToken = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;

    actions = engine.execute("s7-connection", { .id = "s7-read", .deviceId = "s7-device", .deviceCode = "S7-1", .kind = "read", .payload = s7ReadRequest(99) });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 active read did not open a fresh PLC session");
    actions = finishHandshake();
    require(first(actions, collector::ProtocolActionKind::Send).bytes == s7ReadRequest(1), "S7 active read PDU sequence does not match iot-manager");
    packet.payload = s7ReadResponse(1);
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::PublishParsed), "S7 response was not parsed");
    require(first(actions, collector::ProtocolActionKind::PublishParsed).parsed.rawPayloads ==
                std::vector<std::vector<std::uint8_t>>{packet.payload},
            "S7 history retained a response from another acquisition");
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "S7 read did not complete");
    require(first(actions, collector::ProtocolActionKind::PublishParsed).parsed.valuesJson.find("4660") != std::string::npos, "S7 DB value was not decoded");
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedDisconnect, "S7 read did not close only the PLC session");

    actions = engine.execute("s7-connection", { .id = "s7-write", .deviceId = "s7-device", .deviceCode = "S7-1", .kind = "write", .payload = s7WriteRequest(77), .readbackPayload = s7ReadRequest(78), .expectedReadbackData = { 0x12, 0x34 } });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 write did not open a fresh PLC session");
    actions = finishHandshake();
    require(first(actions, collector::ProtocolActionKind::Send).bytes == s7WriteRequest(1), "S7 Write Var PDU sequence does not match iot-manager");
    packet.payload = s7WriteResponse(1);
    actions = engine.consume(packet);
    require(first(actions, collector::ProtocolActionKind::Send).bytes == s7ReadRequest(2), "S7 Write Var did not trigger same-session Read Var verification");
    packet.payload = s7ReadResponse(2);
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "S7 Write Var readback did not complete");
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedDisconnect, "S7 write verification did not finish with ISO-DR");

    actions =
        engine.execute("s7-connection", { .id = "s7-element-write", .deviceId = "s7-device", .deviceCode = "S7-1", .kind = "command", .elements = { { .elementId = "db1-word", .value = "4660" } } });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 element write did not open a fresh PLC session");
    actions = finishHandshake();
    const auto generatedWrite = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(generatedWrite.size() == 37 && generatedWrite[17] == 0x05 && generatedWrite[35] == 0x12 && generatedWrite[36] == 0x34 && s7Reference(generatedWrite) == 1, "S7 element command did not compile Write Var");
    packet.payload = s7WriteResponse(1);
    actions = engine.consume(packet);
    const auto generatedReadback = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(generatedReadback[17] == 0x04 && s7Reference(generatedReadback) == 2, "S7 element command did not compile Read Var verification");
    packet.payload = s7ReadResponse(2);
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "S7 element command did not complete after readback");

    actions = engine.execute(
        "s7-connection",
        { .id = "s7-bool-write",
          .deviceId = "s7-device",
          .deviceCode = "S7-1",
          .kind = "command",
          .elements = { { .elementId = "output-bit", .value = "1" } } }
    );
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 BOOL write did not open a fresh PLC session");
    actions = finishHandshake();
    const auto prepareRead = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(prepareRead[17] == 0x04 && prepareRead[22] == 0x02 && s7Reference(prepareRead) == 1, "S7 BOOL write did not read the containing byte first");
    packet.payload = s7ReadResponseForRequest(prepareRead);
    actions = engine.consume(packet);
    const auto boolWrite = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(boolWrite[17] == 0x05 && boolWrite[22] == 0x02 && boolWrite.back() == 0x16 && s7Reference(boolWrite) == 2, "S7 BOOL write did not preserve adjacent bits during read-modify-write");
    packet.payload = s7WriteResponse(2);
    actions = engine.consume(packet);
    const auto boolReadback = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(boolReadback[17] == 0x04 && boolReadback[22] == 0x02 && s7Reference(boolReadback) == 3, "S7 BOOL write did not start same-session byte readback");
    packet.payload = s7ReadResponseForRequest(boolReadback);
    packet.payload.back() = 0x16;
    actions = engine.consume(packet);
    require(has(actions, collector::ProtocolActionKind::CompleteCommand), "S7 BOOL write did not complete after bit-level readback verification");

    actions = engine.execute("s7-connection", { .id = "s7-write-lost-ack", .deviceId = "s7-device", .deviceCode = "S7-1", .kind = "write", .payload = s7WriteRequest(77), .readbackPayload = s7ReadRequest(78), .expectedReadbackData = { 0x12, 0x34 } });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 lost-ACK write did not open a fresh PLC session");
    actions = finishHandshake();
    const auto lostAckWriteDeadline = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
    actions = engine.deadline("s7-connection", lostAckWriteDeadline);
    require(first(actions, collector::ProtocolActionKind::Send).bytes == s7ReadRequest(2), "S7 missing write ACK did not proceed to readback");
    packet.payload = s7ReadResponse(2);
    actions = engine.consume(packet);
    const auto& lostAckResult = first(actions, collector::ProtocolActionKind::FailCommand);
    require(lostAckResult.reason == "write_ack_missing" &&
                service::command::collectorResultState(false, lostAckResult.reason) == "UNKNOWN" &&
                !service::command::retryableFailure(lostAckResult.reason),
            "S7 write with a missing ACK was marked successful or retried despite matching readback");

    actions = engine.execute("s7-connection", { .id = "s7-readback-timeout", .deviceId = "s7-device", .deviceCode = "S7-1", .kind = "write", .payload = s7WriteRequest(77), .readbackPayload = s7ReadRequest(78), .expectedReadbackData = { 0x12, 0x34 } });
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 readback-timeout write did not open a fresh PLC session");
    actions = finishHandshake();
    packet.payload = s7WriteResponse(1);
    actions = engine.consume(packet);
    const auto readbackDeadline = first(actions, collector::ProtocolActionKind::ScheduleDeadline).deadlineToken;
    actions = engine.deadline("s7-connection", readbackDeadline);
    const auto& readbackFailure = first(actions, collector::ProtocolActionKind::FailCommand);
    require(readbackFailure.reason == "s7_readback_timeout" &&
                service::command::collectorResultState(false, readbackFailure.reason) == "UNKNOWN" &&
                !service::command::retryableFailure(readbackFailure.reason),
            "S7 readback timeout after a confirmed write ACK was rejected or retried");

    actions = engine.deadline("s7-connection", pollToken);
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 periodic poll did not open a fresh PLC session");
    actions = finishHandshake();
    const auto& batched = first(actions, collector::ProtocolActionKind::Send).bytes;
    require(batched.size() == 19 + 7 * 12 && batched[18] == 7, "S7 periodic poll did not batch elements within the negotiated PDU");
    std::map<std::uint8_t, std::size_t> areaCounts;
    std::map<std::uint8_t, std::uint8_t> wordLengths;
    for (std::size_t index = 0; index < batched[18]; ++index) {
        const auto offset = 19 + index * 12;
        ++areaCounts[batched[offset + 8]];
        wordLengths[batched[offset + 8]] = batched[offset + 3];
    }
    require(areaCounts[0x84] == 2 && areaCounts[0x83] == 1 && areaCounts[0x81] == 1 && areaCounts[0x82] == 1 && areaCounts[0x1C] == 1 && areaCounts[0x1D] == 1, "S7 area mapping matrix is incomplete");
    require(wordLengths[0x1C] == 0x1C && wordLengths[0x1D] == 0x1D, "S7 timer/counter word length is incorrect");

    collector::ElementDefinition decoded;
    decoded.dataType = "LREAL";
    const std::array<std::uint8_t, 8> one{ 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    require(collector::s7::detail::decodeJson(one, decoded) == "1", "S7 LREAL was not decoded");
    decoded.dataType = "STRING";
    const std::array<std::uint8_t, 4> text{ 'A', 'B', 'C', 0x00 };
    require(collector::s7::detail::decodeJson(text, decoded) == "\"ABC\"", "S7 STRING was not decoded");

    collector::RuntimeSnapshot serverSnapshot;
    serverSnapshot.links.push_back({ .id = "s7-server", .name = "S7 Server", .mode = "TCP Server", .protocol = "S7", .status = "enabled" });
    auto serverDevice = device;
    serverDevice.id = "s7-server-device";
    serverDevice.code = "860406088591522";
    serverDevice.linkId = "s7-server";
    serverDevice.linkMode = "TCP Server";
    serverDevice.targetId.clear();
    serverDevice.registrationMode = "HEX";
    serverDevice.registrationBytes = { 0x08, 0x60, 0x40, 0x60, 0x88, 0x59, 0x15, 0x22 };
    serverSnapshot.devices.push_back(serverDevice);
    auto conflictingDevice = serverDevice;
    conflictingDevice.id = "s7-server-device-2";
    conflictingDevice.code = "860406088591523";
    conflictingDevice.registrationBytes.back() = 0x23;
    serverSnapshot.devices.push_back(conflictingDevice);

    collector::ProtocolEngine serverEngine(sessionFactories());
    serverEngine.reload(serverSnapshot);
    (void)serverEngine.connected({ .connectionId = "s7-server-connection", .linkId = "s7-server", .sessionEpoch = 1 });
    service::message::IngressPacket serverPacket{ .messageId = "s7-registration",
                                                  .linkId = "s7-server",
                                                  .connectionId = "s7-server-connection",
                                                  .occurredAtMs = 4000,
                                                  .payload = serverDevice.registrationBytes };
    actions = serverEngine.consume(serverPacket);
    require(has(actions, collector::ProtocolActionKind::BindDevice), "S7 standalone registration packet did not bind the device");
    require(first(actions, collector::ProtocolActionKind::Send).bytes == expectedCotp, "S7 registration did not start the PLC session");
    const auto handshakeDeadline = std::find_if(
        actions.begin(),
        actions.end(),
        [](const auto& action) {
            return action.kind == collector::ProtocolActionKind::ScheduleDeadline &&
                action.deadlineAfter == std::chrono::seconds(5);
        }
    );
    require(handshakeDeadline != actions.end(), "S7 registration omitted handshake timeout");
    const auto handshakeDeadlineToken = handshakeDeadline->deadlineToken;
    actions = serverEngine.consume(serverPacket);
    require(!has(actions, collector::ProtocolActionKind::Close), "S7 repeated registration packet closed its own DTU connection");
    serverPacket.payload = conflictingDevice.registrationBytes;
    actions = serverEngine.consume(serverPacket);
    require(first(actions, collector::ProtocolActionKind::Close).reason == "s7_registration_conflict", "S7 conflicting registration was not rejected");
    actions = serverEngine.deadline("s7-server-connection", handshakeDeadlineToken);
    require(!has(actions, collector::ProtocolActionKind::Close), "S7 handshake failure incorrectly closed the persistent DTU socket");

    (void)serverEngine.connected({ .connectionId = "s7-prefixed-registration", .linkId = "s7-server", .sessionEpoch = 2 });
    serverPacket.connectionId = "s7-prefixed-registration";
    serverPacket.payload = serverDevice.registrationBytes;
    const auto cotpConfirm = s7CotpConfirm();
    serverPacket.payload.insert(serverPacket.payload.end(), cotpConfirm.begin(), cotpConfirm.end());
    actions = serverEngine.consume(serverPacket);
    require(has(actions, collector::ProtocolActionKind::BindDevice), "S7 registration prefix did not bind the device");
    require(std::count_if(actions.begin(), actions.end(), [](const auto& action) {
                return action.kind == collector::ProtocolActionKind::Send;
            }) == 2,
            "S7 registration prefix payload was not preserved for protocol parsing");
}

void testS7AllDataTypes() {
    struct TypeCase {
        std::string_view type;
        std::vector<std::uint8_t> bytes;
        std::string_view expected;
        std::int64_t startBit = 0;
    };

    const std::array cases{
        TypeCase{ "BOOL", { 0x08 }, "1", 3 },
        TypeCase{ "INT8", { 0xFE }, "-2" },
        TypeCase{ "UINT8", { 0xFE }, "254" },
        TypeCase{ "INT16", { 0xFF, 0xFE }, "-2" },
        TypeCase{ "UINT16", { 0x12, 0x34 }, "4660" },
        TypeCase{ "INT32", { 0xFF, 0xFF, 0xFF, 0xFE }, "-2" },
        TypeCase{ "UINT32", { 0x12, 0x34, 0x56, 0x78 }, "305419896" },
        TypeCase{ "FLOAT", { 0x3F, 0xC0, 0x00, 0x00 }, "1.5" },
        TypeCase{ "LREAL", { 0x3F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, "1.5" },
        TypeCase{ "STRING", { 'A', 'B', 'C', 0x00 }, "\"ABC\"" },
    };
    for (const auto& current : cases) {
        collector::ElementDefinition element;
        element.dataType = current.type;
        element.startBit = current.startBit;
        const auto decoded = collector::s7::detail::decodeJson(current.bytes, element);
        require(decoded && *decoded == current.expected, "S7 data type matrix failed");
    }
    const std::string invalidUtf8{ static_cast<char>(0xC0), '"' };
    require(collector::s7::detail::jsonEscape(invalidUtf8) == "\\u00C0\\\"", "S7 invalid string bytes did not produce valid JSON escapes");
    require(collector::s7::detail::jsonEscape("中文") == "中文", "S7 valid UTF-8 text was not preserved");
}

void testDeadlineScheduler() {
    asio::io_context io;
    collector::DeadlineScheduler scheduler(io);
    int completed = 0;
    const auto cancelled =
        scheduler.scheduleAfter(std::chrono::milliseconds(1), [&] {
            completed += 100;
        });
    scheduler.cancel(cancelled);
    (void)scheduler.scheduleAfter(std::chrono::milliseconds(1), [&] {
        ++completed;
    });
    (void)scheduler.scheduleAfter(std::chrono::milliseconds(2), [&] {
        ++completed;
        scheduler.stop();
    });
    io.run();
    require(completed == 2, "deadline scheduler cancellation or execution failed");
}

void testRuntimeWritableContract() {
    collector::RuntimeSnapshot readOnly;
    collector::DeviceDefinition device;
    device.id = "runtime-config-device";
    device.elements.push_back({ .id = "runtime-config-element", .writable = false });
    readOnly.devices.push_back(device);

    auto writable = readOnly;
    writable.devices.front().elements.front().writable = true;
    require(service::collector::config::signature(readOnly) != service::collector::config::signature(writable), "runtime signature ignored writable changes");

    const auto expectSignatureChange = [&readOnly](std::string_view field, auto mutate) {
        auto changed = readOnly;
        mutate(changed.devices.front(), changed.devices.front().elements.front());
        require(service::collector::config::signature(readOnly) != service::collector::config::signature(changed), std::string("runtime signature ignored ") + std::string(field));
    };
    expectSignatureChange("Modbus merge gap", [](auto& changed, auto&) {
        changed.modbusMergeGap += 1;
    });
    expectSignatureChange("Modbus maximum quantity", [](auto& changed, auto&) {
        changed.modbusMaxQuantity -= 1;
    });
    expectSignatureChange("storage policy", [](auto& changed, auto&) {
        changed.storagePolicy = "change";
    });
    expectSignatureChange("command fast-read duration", [](auto& changed, auto&) {
        changed.commandFastReadDuration += 1;
    });
    expectSignatureChange("command fast-read interval", [](auto& changed, auto&) {
        changed.commandFastReadInterval += 1;
    });
    expectSignatureChange("element config key", [](auto&, auto& changed) {
        changed.configKey = "element:replacement";
    });

    const auto element = service::collector::config::detail::element(
        { { "id", "runtime-config-element" }, { "writable", "1" } }
    );
    require(element.writable, "runtime did not deserialize writable state");
    bool rejectedInvalidScale = false;
    try {
        (void)service::collector::config::detail::element(
            { { "id", "runtime-config-element" }, { "scale", "1x" } }
        );
    } catch (const std::runtime_error& error) {
        rejectedInvalidScale =
            std::string_view(error.what()).find("invalid runtime decimal: scale") !=
            std::string_view::npos;
    }
    require(rejectedInvalidScale, "runtime accepted an element scale with trailing non-decimal bytes");
    bool rejectedOutOfRangeSlave = false;
    try {
        (void)service::collector::config::detail::device(
            { { "id", "runtime-config-device" }, { "slave_id", "300" } }
        );
    } catch (const std::runtime_error& error) {
        rejectedOutOfRangeSlave =
            std::string_view(error.what()).find("invalid runtime integer range: slave_id") !=
            std::string_view::npos;
    }
    require(rejectedOutOfRangeSlave, "runtime wrapped an out-of-range Modbus slave id");
    bool rejectedInvalidWritable = false;
    try {
        (void)service::collector::config::detail::element(
            { { "id", "runtime-config-element" }, { "writable", "2" } }
        );
    } catch (const std::runtime_error& error) {
        rejectedInvalidWritable =
            std::string_view(error.what()).find("invalid runtime boolean: writable") !=
            std::string_view::npos;
    }
    require(rejectedInvalidWritable, "runtime accepted a non-boolean writable flag");
    bool rejectedInvalidHeartbeat = false;
    try {
        (void)service::collector::config::detail::device(
            { { "id", "runtime-config-device" }, { "heartbeat_hex", "0G" } }
        );
    } catch (const std::runtime_error& error) {
        rejectedInvalidHeartbeat =
            std::string_view(error.what()).find("invalid runtime hex: heartbeat_hex") !=
            std::string_view::npos;
    }
    require(rejectedInvalidHeartbeat, "runtime silently dropped an invalid heartbeat HEX payload");
}

void testRuntimeRepositoryRejectsInvalidScale() {
    RuntimeRepositoryScaleDb db;
    bool rejected = false;
    try {
        (void)runTask(service::configuration::loadRuntimeSnapshot(db));
    } catch (const std::exception& error) {
        rejected = std::string_view(error.what()).find("invalid runtime repository decimal: scale") != std::string_view::npos;
        if (!rejected) throw std::runtime_error(std::string("unexpected runtime snapshot failure: ") + error.what());
    }
    require(rejected, "runtime repository accepted a Modbus scale with trailing bytes");
}

void testAtomicStreamFinalizationContract() {
    const std::vector<service::message::StreamField> deadLetterFields{
        { "source_entry_id", "10-0" }
    };
    const std::vector<service::message::StreamField> resultFields{
        { "command_id", "command-1" }
    };
    const std::array publications{
        service::message::redis::StreamPublication{ "dead-letter", deadLetterFields, 100 },
        service::message::redis::StreamPublication{
            "command-result",
            resultFields,
            200,
            service::message::redis::StreamWake{
                0,
                service::message::WorkerStreamTask::CommandResult } }
    };

    RecordingRedis committed(1);
    require(runTask(service::message::redis::publishAllAndAcknowledge(committed, publications, "command-input", "collector-group", "collector-0", "10-0")), "pending stream finalization did not report a commit");
    require(committed.keys == std::vector<std::string>{ "dead-letter", "dead-letter", "command-result", service::message::workerWakeStream(0, service::runtime::instanceId()), "command-input" }, "atomic stream finalization changed Redis key order");
    require(committed.arguments == std::vector<std::string>{ "collector-group", "10-0", "collector-0", "100", "1", "", "source_entry_id", "10-0", "200", "1", "command-result", "command_id", "command-1" }, "atomic stream finalization encoded invalid arguments");
    const auto pendingCheck = committed.script.find("XPENDING");
    const auto typeCheck = committed.script.find("TYPE");
    const auto firstWrite = committed.script.find("XADD");
    require(pendingCheck != std::string::npos && typeCheck != std::string::npos && firstWrite != std::string::npos && pendingCheck < firstWrite && typeCheck < firstWrite, "atomic stream finalization writes before validating ownership and key types");
    require(committed.script.find("pending[1][2] ~= ARGV[3]") != std::string::npos, "atomic stream finalization did not enforce consumer ownership");

    RecordingRedis missingPending(0);
    require(!runTask(service::message::redis::publishAllAndAcknowledge(missingPending, publications, "command-input", "collector-group", "collector-0", "10-0")), "missing PEL entry was reported as atomically finalized");
}

void testRuntimeProjectionRejectsRedisErrors() {
    service::common::UuidV7Generator uuidGenerator;
    FailingConfigRedis redis;
    bool rejected = false;
    try {
        (void)runTask(service::collector::config::project(redis, service::collector::RuntimeSnapshot{}, uuidGenerator));
    } catch (const std::runtime_error& error) {
        rejected = std::string_view(error.what()).find("active runtime") !=
            std::string_view::npos;
    }
    require(rejected, "runtime projection reported success after the active-version SET failed");
}

void testRuntimeProjectionRefreshesPreviousGrace() {
    service::common::UuidV7Generator uuidGenerator;
    FailingConfigRedis redis(false, true);
    (void)runTask(service::collector::config::project(redis, service::collector::RuntimeSnapshot{}, uuidGenerator));
    const auto refresh = std::ranges::find_if(redis.commands, [](const auto& command) {
        return command.size() >= 4 && command.front() == "ZADD" &&
            command.back() == "previous-version";
    });
    require(refresh != redis.commands.end(), "runtime projection did not retain the previous snapshot");
    require(refresh->size() == 4 && (*refresh)[2] != "NX", "runtime projection did not restart the previous snapshot grace window");
}

void testRuntimeSetOrderingContract() {
    const auto first = service::collector::config::detail::sortedStrings(
        { "element:c", "element:a", "element:b" }
    );
    const auto second = service::collector::config::detail::sortedStrings(
        { "element:b", "element:c", "element:a" }
    );
    require(first == second && first == std::vector<std::string>{ "element:a", "element:b", "element:c" }, "runtime Redis Set members were not canonicalized");

    collector::RuntimeSnapshot previous;
    collector::LinkDefinition link;
    link.id = "s7-server";
    link.mode = "TCP Server";
    link.protocol = "S7";
    previous.links.push_back(link);

    collector::DeviceDefinition device;
    device.id = "s7-device";
    device.linkId = link.id;
    device.linkMode = link.mode;
    device.protocol = link.protocol;
    for (const auto& id : first) {
        device.elements.push_back({ .configKey = id, .id = id });
    }
    previous.devices.push_back(device);

    auto next = previous;
    next.devices.front().elements.clear();
    for (const auto& id : second) {
        next.devices.front().elements.push_back({ .configKey = id, .id = id });
    }
    const auto plan = collector::ProtocolEngine(sessionFactories()).planReconcile(previous, next);
    require(plan.affectedLinks.empty() && plan.restartLinks.empty(), "equivalent Redis Set order restarted an unrelated TCP Server link");
}

void testRealtimeProjectionContract() {
    const auto scopedKeys = [](std::size_t worker, std::string_view instance) {
        return std::vector<std::string>{
            service::message::configStream(worker, instance),
            service::message::ingressStream(worker, instance),
            service::message::egressStream(worker, instance),
            service::message::commandStream(worker, true, instance),
            service::message::commandStream(worker, false, instance),
            service::message::linkEventStream(worker, instance),
            service::message::controlStream(worker, instance),
            service::message::deadLetterStream(worker, instance),
            service::message::workerWakeStream(worker, instance),
            service::message::worker_metrics::snapshotId(worker, instance),
            service::edge::dispatch::stream(worker, instance),
            service::edge::projector_stream::stream(worker, instance),
            service::edge::projector_stream::leaseKey(worker, instance),
            service::edge::projector_stream::ownerToken(worker, instance)};
    };
    const auto keys = scopedKeys(2, "process-a");
    const std::vector<std::string> expected{
        "iot:channel:config:worker:process-a:2",
        "iot:channel:packet:raw:worker:process-a:2",
        "iot:channel:socket:egress:worker:process-a:2",
        "iot:channel:command:worker:process-a:2:high",
        "iot:channel:command:worker:process-a:2:normal",
        "iot:channel:link:event:worker:process-a:2",
        "iot:channel:control:worker:process-a:2",
        "iot:channel:dead-letter:worker:process-a:2",
        "iot:service:worker:process-a:2:wake",
        "process-a:worker:2", "iot:v2:edge:dispatch:process-a:2",
        "iot:v3:edge:projector:process-a:2",
        "iot:v3:edge:projector:lease:process-a:2", "process-a:2"};
    require(keys == expected, "explicit process identity changed persisted message keys");
    const auto otherWorker = scopedKeys(3, "process-a");
    const auto otherProcess = scopedKeys(2, "process-b");
    for (std::size_t index = 0; index < keys.size(); ++index)
        require(keys[index] != otherWorker[index] && keys[index] != otherProcess[index],
                "message key collided across Worker or process identity");
    require(service::message::workerWakeStream(std::nullopt, "process-a") ==
                "iot:service:process-a:work-available" &&
                service::message::sharedWakeStream("process-b") !=
                    service::message::sharedWakeStream("process-a"),
            "shared wake stream lost process isolation");
    collector::RuntimeSnapshot original;
    collector::RealtimeDeviceDefinition device;
    device.id = "019fd9f6-4bd5-7ec3-80ff-0ba381987024";
    device.code = "REMOTEIO01";
    device.name = "远程IO";
    device.points.push_back({ "AI1", "模拟量输入1", "mA" });
    original.realtimeDevices.push_back(device);

    const auto packed = service::message::realtime::encodePoint(
        original.realtimeDevices.front().points.front()
    );
    const auto decoded = service::message::realtime::decodePoint(packed);
    require(decoded.has_value() && decoded->id == "AI1" && decoded->name == "模拟量输入1" && decoded->unit == "mA", "realtime point projection encoding changed");
    require(!service::message::realtime::decodePoint("broken").has_value(), "malformed realtime point projection was accepted");
    require(service::collector::config::realtimeDeviceKey("version-1", device.id) == "iot:config:runtime:version-1:realtime-device:" + device.id, "realtime device projection key changed");

    auto renamed = original;
    renamed.realtimeDevices.front().name = "远程IO-更新";
    require(service::collector::config::signature(original) != service::collector::config::signature(renamed), "runtime signature ignored realtime metadata changes");
}

void testBooleanPointValueContract() {
    namespace latest = service::telemetry::latest;
    require(latest::canonicalPointText("true", "BOOL") == "1" && latest::canonicalPointText("false", "BOOL") == "0" && latest::canonicalPointText("1", "BOOL") == "1" && latest::canonicalPointText("0", "BOOL") == "0", "BOOL point text is not canonicalized to 0/1");
    require(latest::canonicalPointJson("true", "BOOL") == "1" && latest::canonicalPointJson("false", "BOOL") == "0" && latest::canonicalPointJson("\"true\"", "BOOL") == "\"1\"" && latest::canonicalPointJson("\"false\"", "BOOL") == "\"0\"", "BOOL point JSON is not canonicalized to 0/1");
    require(latest::canonicalPointText("true", "STRING") == "true" && latest::canonicalPointJson("true", "STRING") == "true", "non-BOOL point values were rewritten");
}

void testFreshnessDeadlineWait() {
    namespace latest = service::telemetry::latest;
    require(!latest::deadlineWait(1000, std::nullopt).has_value(), "missing deadline must block until a wake event");
    require(latest::deadlineWait(1000, 999) == std::chrono::milliseconds::zero(), "past deadline must run immediately");
    require(latest::deadlineWait(1000, 1250) == std::chrono::milliseconds(250), "future deadline wait changed");
}

void testEdgeSessionOwnership() {
    constexpr std::string_view instance = "01970000-1234-7000-8000-000000000123";
    const auto encoded = service::edge::session_state::value(22, 5, 1, instance);
    const auto parsed = service::edge::session_state::parse(encoded);
    require(parsed && parsed->epoch == 22 && parsed->protocolVersion == 5 && parsed->workerIndex == 1, "edge routing identity did not round trip");
    require(parsed->instanceId == instance, "edge owner instance was lost");
    require(encoded == "22|5|1|01970000-1234-7000-8000-000000000123",
            "edge session persisted format changed");
    require(service::edge::session_state::value(22, 5, 1,
                "01970000-1234-7000-8000-000000000124") != encoded,
            "edge sessions collide between process incarnations");
    require(!service::edge::session_state::parse("22|5").has_value(), "incomplete routing identity was accepted");
    require(!service::edge::session_state::parse("22|5|1|bad-instance").has_value(), "invalid owner instance was accepted");
}

void testLatestProjectionRejectsRedisErrors() {
    bool rejected = false;
    try {
        runTask(service::telemetry::latest::executeProjectionPipeline(FailingLatestPipeline{}, "test latest projection"));
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    require(rejected, "latest-value projection ignored a Redis pipeline error");
}

void testLatestProjectionRefreshesPreservedElementMetadata() {
    LatestProjectionContext context;
    runTask(service::telemetry::latest::projectDevice(context, "device-1"));

    bool skippedExistingElement = false;
    bool refreshesElementMetadata = false;
    for (const auto& command : context.redisClient.state->pipelineCommands) {
        if (command.size() >= 4 && command[0] == "HSETNX" &&
            command[1] == "iot:v2:device:device-1:latest" && command[2] == "temperature") {
            skippedExistingElement = true;
        }
        if (!command.empty() && command[0] != "HSETNX") {
            const auto hasElement = std::ranges::find(command, "temperature") != command.end();
            const auto hasUpdatedName = std::ranges::any_of(command, [](const auto& argument) {
                return argument.find("\"name\":\"New Name\"") != std::string::npos;
            });
            refreshesElementMetadata = refreshesElementMetadata || (hasElement && hasUpdatedName);
        }
    }
    require(!skippedExistingElement, "latest projection skipped existing element metadata refresh with HSETNX");
    require(refreshesElementMetadata, "latest projection did not write refreshed metadata for an existing element");
    const auto publishedChange = std::ranges::any_of(
        context.redisClient.state->directCommands,
        [](const auto& command) {
            return command.size() == 8 && command[0] == "XADD" &&
                command[1] == service::telemetry::latest::kRealtimeChangesStream &&
                command[6] == "topic" && command[7] == "device.realtime";
        }
    );
    require(publishedChange, "latest projection did not notify device realtime subscribers");
}

void testLatestProjectionRejectsInvalidPreservedDeadline() {
    LatestProjectionContext context;
    context.redisClient.state->preservedOnlineUntilMs = "not-a-number";
    runTask(service::telemetry::latest::projectDevice(context, "device-1"));

    bool emittedInvalidDeadline = false;
    bool clearedDeadline = false;
    const auto deadlineKey = service::telemetry::latest::onlineDeadlinesKey();
    for (const auto& command : context.redisClient.state->pipelineCommands) {
        if (command.size() >= 4 && command[0] == "ZADD" &&
            command[1] == deadlineKey &&
            command[2] == "not-a-number") {
            emittedInvalidDeadline = true;
        }
        if (command.size() >= 3 && command[0] == "ZREM" &&
            command[1] == deadlineKey && command[2] == "device-1") {
            clearedDeadline = true;
        }
    }
    require(!emittedInvalidDeadline, "latest projection reused an invalid preserved online deadline");
    require(clearedDeadline, "latest projection did not clear an invalid preserved deadline");
}

void testAlertScheduleSkipsInvalidStoredDuration() {
    AlertScheduleRedis redis;
    service::message::ParsedDeviceMessage message;
    message.deviceId = "device-1";
    message.deviceCode = "D1";
    message.observedAtMs = 1700000000000;

    runTask(service::alert::metadata::schedule(redis, std::vector{ message }));
    require(!redis.state->pipelineCommands.empty(), "alert schedule did not issue a Redis pipeline command");
}

void testCommandValueDecimalParsing() {
    namespace command = service::collector::command;
    require(command::decimal("1.5", "value") == 1.5, "command decimal parser rejected a finite value");
    require(command::decimal("-1.25e2", "value") == -125.0, "command decimal parser rejected scientific notation");

    for (const auto value : { "1.5x", "nan", "inf" }) {
        bool rejected = false;
        try {
            (void)command::decimal(value, "value");
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        require(rejected, "command decimal parser accepted an invalid value");
    }

    collector::ElementDefinition bcd;
    bcd.id = "bcd-value";
    bcd.name = "BCD value";
    bcd.encoding = "BCD";
    bcd.length = 2;
    bcd.digits = 2;
    bool rejectedNegativeBcd = false;
    try {
        command::validateValue(bcd, "-12.34");
    } catch (const std::invalid_argument&) {
        rejectedNegativeBcd = true;
    }
    require(rejectedNegativeBcd, "command validation accepted negative BCD without room for the sign byte");

    bool rejectedNegativeBcdEncoding = false;
    try {
        (void)collector::sl651::detail::encodeValue(bcd, "-12.34");
    } catch (const std::invalid_argument&) {
        rejectedNegativeBcdEncoding = true;
    }
    require(rejectedNegativeBcdEncoding, "SL651 encoded a negative BCD value as a positive wire value");
}

void testPacketLog() {
    namespace packetLog = service::packet_log;
    const auto directory = std::filesystem::temp_directory_path() / "iot-engine-packet-log-test";
    std::error_code ignored;
    std::filesystem::remove_all(directory, ignored);
    std::filesystem::create_directories(directory);
    const auto staleWorkerLog = directory / "packet-old-worker.log";
    {
        std::ofstream stale(staleWorkerLog);
        stale << "event=STALE\n";
    }
    std::filesystem::last_write_time(
        staleWorkerLog,
        std::filesystem::file_time_type::clock::now() - std::chrono::hours(24 * 15),
        ignored);

    packetLog::Config config;
    config.directory = directory;
    config.level = packetLog::Level::Debug;
    packetLog::initialize(std::move(config));
    require(!std::filesystem::exists(staleWorkerLog),
            "packet log retention did not remove an old worker basename");

    packetLog::Context context;
    context.workerIndex = 2;
    context.direction = "RX";
    context.operation = "transport";
    context.protocol = "Modbus";
    context.linkId = "link-test";
    context.connectionId = "connection-test";
    context.messageId = "message-test";
    context.sessionEpoch = 7;
    const std::array<std::uint8_t, 4> invalidBytes{ 0x00, 0xFF, 0x7E, 0x01 };
    packetLog::write(packetLog::Level::Debug, "RX_BYTES", context, invalidBytes);
    bool flushed = false;
    const auto flushDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!flushed && std::chrono::steady_clock::now() < flushDeadline) {
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            if (!entry.is_regular_file()) continue;
            std::ifstream input(entry.path());
            const std::string current{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
            flushed = flushed || current.find("event=\"RX_BYTES\"") != std::string::npos;
        }
        if (!flushed) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    require(flushed, "low-volume debug log was not flushed before shutdown");
    packetLog::write(packetLog::Level::Warn, "TIMEOUT", context, {}, "modbus_response_timeout");
    packetLog::write(packetLog::Level::Info, "BROADCAST_START", context, {}, "targets=2");
    packetLog::shutdown();

    std::string content;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::ifstream input(entry.path());
        content.append(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    require(content.find("event=\"RX_BYTES\"") != std::string::npos && content.find("hex=\"00 FF 7E 01\"") != std::string::npos, "packet log did not preserve unparsed binary bytes");
    require(content.find("event=\"TIMEOUT\"") != std::string::npos && content.find("event=\"BROADCAST_START\"") != std::string::npos, "packet log did not persist timeout and broadcast events");
    std::filesystem::remove_all(directory, ignored);
}

collector::RuntimeSnapshot clientReconcileSnapshot() {
    collector::RuntimeSnapshot snapshot;
    collector::LinkDefinition link{ .id = "client-link",
                                    .name = "Client",
                                    .mode = "TCP Client",
                                    .protocol = "Modbus",
                                    .status = "enabled" };
    link.targets.push_back(
        { .id = "target-a", .name = "A", .ip = "127.0.0.1", .port = 15001, .status = "enabled" }
    );
    link.targets.push_back(
        { .id = "target-b", .name = "B", .ip = "127.0.0.1", .port = 15002, .status = "enabled" }
    );
    snapshot.links.push_back(link);

    collector::DeviceDefinition first;
    first.id = "device-a";
    first.code = "A";
    first.linkId = link.id;
    first.linkMode = link.mode;
    first.targetId = "target-a";
    first.protocol = link.protocol;
    first.modbusMode = "TCP";
    first.slaveId = 1;
    snapshot.devices.push_back(first);

    auto second = first;
    second.id = "device-b";
    second.code = "B";
    second.targetId = "target-b";
    snapshot.devices.push_back(second);
    return snapshot;
}

void testRuntimeReconcile() {
    const auto previous = clientReconcileSnapshot();
    auto debugUpdate = previous;
    debugUpdate.links.front().debugEnabled = true;
    debugUpdate.devices.front().debugEnabled = true;
    const auto debugPlan = collector::ProtocolEngine(sessionFactories()).planReconcile(previous, debugUpdate);
    require(debugPlan.affectedLinks.empty() && debugPlan.refreshClientSessions.empty() &&
        debugPlan.restartLinks.empty() && debugPlan.restartClientTargets.empty(), "debug switches restarted live protocol sessions");
    require(collector::config::signature(previous) != collector::config::signature(debugUpdate),
        "debug switches were omitted from configuration publication");

    auto deviceUpdate = previous;
    deviceUpdate.devices.front().readInterval = 10;
    const auto devicePlan = collector::ProtocolEngine(sessionFactories()).planReconcile(previous, deviceUpdate);
    require(devicePlan.affectedLinks.contains("client-link"), "device update did not affect its link");
    require(devicePlan.refreshClientSessions.contains({ "client-link", "target-a" }), "changed Modbus device target was not scheduled for a session refresh");
    require(!devicePlan.refreshClientSessions.contains({ "client-link", "target-b" }), "unchanged sibling target was scheduled for a session refresh");
    require(devicePlan.restartClientTargets.empty(), "Modbus device update incorrectly restarted its TCP Client transport");
    require(devicePlan.restartLinks.empty(), "TCP Client device update incorrectly restarted the whole link");

    auto metadataUpdate = previous;
    metadataUpdate.links.front().name = "Renamed client";
    metadataUpdate.links.front().targets.back().name = "Renamed B";
    const auto metadataPlan = collector::ProtocolEngine(sessionFactories()).planReconcile(previous, metadataUpdate);
    require(metadataPlan.affectedLinks.contains("client-link"), "link metadata update was not observed");
    require(metadataPlan.refreshClientSessions.empty() && metadataPlan.restartClientTargets.empty() && metadataPlan.restartLinks.empty(), "metadata-only update restarted a TCP Client transport");

    auto s7Previous = previous;
    s7Previous.links.front().protocol = "S7";
    for (auto& device : s7Previous.devices) {
        device.protocol = "S7";
    }
    auto s7Next = s7Previous;
    s7Next.devices.front().readInterval = 10;
    const auto s7Plan = collector::ProtocolEngine(sessionFactories()).planReconcile(s7Previous, s7Next);
    require(s7Plan.restartClientTargets.contains({ "client-link", "target-a" }) && s7Plan.refreshClientSessions.empty(), "stateful S7 session update did not retain transport restart semantics");

    auto rtuPrevious = previous;
    rtuPrevious.devices.front().modbusMode = "RTU";
    auto rtuNext = rtuPrevious;
    rtuNext.devices.front().readInterval = 10;
    const auto rtuPlan = collector::ProtocolEngine(sessionFactories()).planReconcile(rtuPrevious, rtuNext);
    require(rtuPlan.restartClientTargets.contains({ "client-link", "target-a" }) && rtuPlan.refreshClientSessions.empty(), "Modbus RTU-over-TCP update did not retain transport restart semantics");

    auto serverPrevious = previous;
    serverPrevious.links.front().mode = "TCP Server";
    serverPrevious.links.front().targets.clear();
    serverPrevious.devices.front().linkMode = "TCP Server";
    serverPrevious.devices.back().linkMode = "TCP Server";
    auto serverNext = serverPrevious;
    serverNext.devices.front().readInterval = 10;
    const auto serverPlan = collector::ProtocolEngine(sessionFactories()).planReconcile(serverPrevious, serverNext);
    require(serverPlan.restartLinks.contains("client-link"), "TCP Server device update did not restart the server link");

    collector::ProtocolEngine engine(sessionFactories());
    engine.reload(previous);
    (void)engine.connected({ .connectionId = "preserved-connection", .linkId = "client-link", .remoteAddress = "127.0.0.1:15002", .targetId = "target-b", .sessionEpoch = 1 });
    engine.reloadRetainingSessions(metadataUpdate);
    require(engine.contains("preserved-connection"), "metadata reload discarded a preserved TCP Client session");

    auto pollingPrevious = previous;
    pollingPrevious.devices.front().elements.push_back(
        { .id = "holding-0",
          .name = "Holding 0",
          .dataType = "UINT16",
          .registerType = "HOLDING_REGISTER",
          .quantity = 1 }
    );
    auto pollingUpdate = pollingPrevious;
    pollingUpdate.devices.front().readInterval = 10;
    const auto pollingPlan =
        collector::ProtocolEngine(sessionFactories()).planReconcile(pollingPrevious, pollingUpdate);

    collector::ProtocolEngine refreshEngine(sessionFactories());
    refreshEngine.reload(pollingPrevious);
    const auto initialActions =
        refreshEngine.connected({ .connectionId = "refreshed-connection", .linkId = "client-link", .remoteAddress = "127.0.0.1:15001", .targetId = "target-a", .sessionEpoch = 2 });
    require(has(initialActions, collector::ProtocolActionKind::BindDevice) && has(initialActions, collector::ProtocolActionKind::Send), "initial Modbus client session was not active");
    refreshEngine.reloadRetainingSessions(pollingUpdate);
    const auto refreshes =
        refreshEngine.refreshClientSessions(pollingPlan.refreshClientSessions);
    require(refreshes.size() == 1 && refreshes.front().connectionId == "refreshed-connection", "changed Modbus target did not rebuild its protocol session");
    require(has(refreshes.front().retiredActions, collector::ProtocolActionKind::CancelDeadline) && has(refreshes.front().startedActions, collector::ProtocolActionKind::BindDevice) && has(refreshes.front().startedActions, collector::ProtocolActionKind::Send), "protocol session refresh did not retire old work and start the new definition");
    const auto& initialSend = first(initialActions, collector::ProtocolActionKind::Send);
    const auto& refreshedSend =
        first(refreshes.front().startedActions, collector::ProtocolActionKind::Send);
    require(initialSend.bytes.size() >= 2 && refreshedSend.bytes.size() >= 2 && (initialSend.bytes[0] != refreshedSend.bytes[0] || initialSend.bytes[1] != refreshedSend.bytes[1]), "refreshed Modbus TCP session reused an in-flight transaction id");
    require(refreshEngine.contains("refreshed-connection"), "protocol session refresh discarded the live TCP connection identity");
}

void testPollStagger() {
    const auto now = std::chrono::steady_clock::time_point(std::chrono::seconds(12345));
    const auto interval = std::chrono::seconds(997);
    const auto delay = collector::staggeredPollDelay("device-a", interval, now);
    require(delay > std::chrono::milliseconds::zero() && delay <= interval, "staggered poll delay escaped its configured interval");
    require(delay == collector::staggeredPollDelay("device-a", interval, now), "poll phase was not stable for the same device");
    const auto nextEpoch =
        std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch() + delay).count();
    require(nextEpoch % interval.count() == collector::stablePollHash("device-a") % static_cast<std::uint64_t>(interval.count()), "poll deadline did not land on the device's stable phase");

    std::set<std::chrono::milliseconds> phases;
    for (int index = 0; index < 32; ++index) {
        phases.insert(collector::staggeredPollDelay("device-" + std::to_string(index), interval, now));
    }
    require(phases.size() > 1, "different devices collapsed onto one polling phase");
    require(collector::staggeredPollDelay("device-a", std::chrono::seconds(1), now) == std::chrono::seconds(1), "one-second polling interval was changed by staggering");
}

void testTcpServerListenersAreWorkerLocal() {
    asio::io_context io;
    collector::DeadlineScheduler scheduler0(io);
    collector::DeadlineScheduler scheduler1(io);
    asio::ip::tcp::acceptor probe(
        io,
        { asio::ip::make_address("127.0.0.1"), 0 }
    );
    const auto port = probe.local_endpoint().port();
    probe.close();

    collector::RuntimeSnapshot snapshot;
    snapshot.links.push_back({ .id = "server-link", .name = "server link", .mode = "TCP Server", .protocol = "Modbus", .ip = "0.0.0.0", .port = port, .status = "enabled" });

    std::map<std::size_t, std::vector<collector::LinkState>> states;
    const auto onState = [&states](collector::LinkState state) {
        states[state.workerIndex].push_back(std::move(state));
    };
    std::vector<std::size_t> acceptedWorkers;
    const auto onConnection0 = [&acceptedWorkers](collector::ProtocolConnectionInfo) {
        acceptedWorkers.push_back(0);
    };
    const auto onConnection1 = [&acceptedWorkers](collector::ProtocolConnectionInfo) {
        acceptedWorkers.push_back(1);
    };
    const auto noPacket = [](service::message::IngressPacket) {
    };
    const auto noDisconnect = [](std::string, std::string) {
    };

    service::common::UuidV7Generator firstWorkerUuids;
    collector::TcpTransport firstWorker(firstWorkerUuids, io, scheduler0, 0, 2, onConnection0, noPacket, noDisconnect, onState);
    service::common::UuidV7Generator secondWorkerUuids;
    collector::TcpTransport secondWorker(secondWorkerUuids, io, scheduler1, 1, 2, onConnection1, noPacket, noDisconnect, onState);
    firstWorker.reload(snapshot);
    secondWorker.reload(snapshot);

    const auto hasListening = [&states](std::size_t worker) {
        const auto current = states.find(worker);
        return current != states.end() &&
            std::ranges::any_of(current->second, [](const auto& state) {
                   return state.state == "listening";
               });
    };
    require(hasListening(0) && hasListening(1),
            "both Collector Workers must establish local TCP listeners");
    require(!std::ranges::any_of(states[0], [](const auto& state) {
        return state.state == "idle";
    }) && !std::ranges::any_of(states[1], [](const auto& state) {
        return state.state == "idle";
    }),
            "a Collector Worker silently disabled its TCP Server listener");

    std::vector<std::shared_ptr<asio::ip::tcp::socket>> clients;
    for (std::size_t index = 0; index < 8; ++index) {
        auto client = std::make_shared<asio::ip::tcp::socket>(io);
        std::error_code error;
        client->connect({ asio::ip::make_address("127.0.0.1"), port }, error);
        require(!error, "a TCP Server listener rejected a local connection");
        clients.push_back(std::move(client));
    }
    io.restart();
    io.run_for(std::chrono::milliseconds(250));
    require(!acceptedWorkers.empty(),
            "no Collector Worker accepted a connection on its local listener");
    require(std::ranges::all_of(acceptedWorkers, [](std::size_t worker) {
        return worker == 0 || worker == 1;
    }),
            "a TCP connection was handled by a foreign Collector Worker");
    for (const auto& client : clients) {
        std::error_code error;
        client->close(error);
    }

    firstWorker.stop();
    secondWorker.stop();
    scheduler0.stop();
    scheduler1.stop();
    io.stop();
}

void testTcpClientTargetReconcile() {
    asio::io_context io;
    collector::DeadlineScheduler scheduler(io);
    asio::ip::tcp::acceptor firstServer(
        io,
        { asio::ip::make_address("127.0.0.1"), 0 }
    );
    asio::ip::tcp::acceptor secondServer(
        io,
        { asio::ip::make_address("127.0.0.1"), 0 }
    );
    std::vector<std::shared_ptr<asio::ip::tcp::socket>> firstAccepted;
    std::vector<std::shared_ptr<asio::ip::tcp::socket>> secondAccepted;
    const auto acceptOne = [&io](
                               asio::ip::tcp::acceptor& server,
                               std::vector<std::shared_ptr<asio::ip::tcp::socket>>& accepted
                           ) {
        auto socket = std::make_shared<asio::ip::tcp::socket>(io);
        server.async_accept(*socket, [socket, &accepted](const std::error_code& error) {
            if (!error) {
                accepted.push_back(socket);
            }
        });
    };
    acceptOne(firstServer, firstAccepted);
    acceptOne(secondServer, secondAccepted);

    std::map<std::string, std::vector<std::string>, std::less<>> connectedByTarget;
    std::vector<std::string> disconnected;
    service::common::UuidV7Generator tcpUuids;
    collector::TcpTransport tcp(tcpUuids,
        io,
        scheduler,
        0,
        1,
        [&connectedByTarget](collector::ProtocolConnectionInfo info) {
            connectedByTarget[info.targetId].push_back(std::move(info.connectionId));
        },
        [](service::message::IngressPacket) {
        },
        [&disconnected](std::string connectionId, std::string) {
            disconnected.push_back(std::move(connectionId));
        },
        [](collector::LinkState) {
        }
    );

    auto previous = clientReconcileSnapshot();
    previous.links.front().targets.front().port = firstServer.local_endpoint().port();
    previous.links.front().targets.back().port = secondServer.local_endpoint().port();
    tcp.reload(previous);
    io.run_for(std::chrono::milliseconds(250));
    require(connectedByTarget["target-a"].size() == 1 && connectedByTarget["target-b"].size() == 1, "initial TCP Client targets did not both connect");
    const auto originalFirst = connectedByTarget["target-a"].front();
    const auto originalSecond = connectedByTarget["target-b"].front();

    auto next = previous;
    next.devices.front().readInterval = 10;
    const auto plan = collector::ProtocolEngine(sessionFactories()).planReconcile(previous, next);
    tcp.reconcile(next, plan);
    io.restart();
    io.run_for(std::chrono::milliseconds(250));

    require(connectedByTarget["target-a"].size() == 1 && connectedByTarget["target-a"].front() == originalFirst, "Modbus device update reconnected its unchanged TCP Client target");
    require(connectedByTarget["target-b"].size() == 1 && connectedByTarget["target-b"].front() == originalSecond, "unchanged sibling target was disconnected");
    require(std::ranges::find(disconnected, originalFirst) == disconnected.end() && std::ranges::find(disconnected, originalSecond) == disconnected.end(), "device-only update emitted a TCP disconnect notification");
    require(tcp.advanceSessionEpoch(originalFirst) != 0, "live TCP connection could not rotate its protocol session epoch");

    tcp.stop();
    firstServer.close();
    secondServer.close();
    scheduler.stop();
    io.stop();
}

void testTcpClientTargetsUseConnectionClaim() {
    asio::io_context io;
    collector::DeadlineScheduler scheduler0(io);
    collector::DeadlineScheduler scheduler1(io);
    asio::ip::tcp::acceptor server(
        io,
        { asio::ip::make_address("127.0.0.1"), 0 }
    );
    std::vector<std::shared_ptr<asio::ip::tcp::socket>> accepted;
    const auto acceptOne = [&io, &server, &accepted] {
        auto socket = std::make_shared<asio::ip::tcp::socket>(io);
        server.async_accept(*socket, [socket, &accepted](const std::error_code& error) {
            if (!error) {
                accepted.push_back(socket);
            }
        });
    };
    acceptOne();
    acceptOne();

    auto snapshot = clientReconcileSnapshot();
    snapshot.links.front().targets.resize(1);
    snapshot.links.front().targets.front().port = server.local_endpoint().port();
    snapshot.devices.resize(1);

    std::size_t claimAttempts = 0;
    bool targetClaimed = false;
    std::vector<std::size_t> connectedWorkers;
    std::set<std::string> claimTokens;
    const auto claim = [&claimAttempts, &targetClaimed, &claimTokens](
                           std::string,
                           std::string,
                           std::string connectionToken,
                           std::function<void(bool)> completed
                       ) {
        ++claimAttempts;
        require(!connectionToken.empty() && claimTokens.insert(connectionToken).second, "TCP connection attempts reused a lease generation");
        auto complete = [&targetClaimed, completed = std::move(completed)](bool claimed) mutable {
            if (claimed) {
                targetClaimed = true;
            }
            completed(claimed);
        };
        if (targetClaimed) {
            complete(false);
            return;
        }
        complete(true);
    };
    const auto release = [&targetClaimed](std::string, std::string, std::string) {
        targetClaimed = false;
    };
    const auto noPacket = [](service::message::IngressPacket) {
    };
    const auto noDisconnect = [](std::string, std::string) {
    };
    const auto noState = [](collector::LinkState) {
    };
    service::common::UuidV7Generator firstWorkerUuids;
    collector::TcpTransport firstWorker(firstWorkerUuids,
        io,
        scheduler0,
        0,
        2,
        [&connectedWorkers, &claimTokens](collector::ProtocolConnectionInfo info) {
            require(claimTokens.contains(info.connectionId), "TCP connection lost its claimed generation");
            connectedWorkers.push_back(0);
            (void)info;
        },
        noPacket,
        noDisconnect,
        noState,
        claim,
        release
    );
    service::common::UuidV7Generator secondWorkerUuids;
    collector::TcpTransport secondWorker(secondWorkerUuids,
        io,
        scheduler1,
        1,
        2,
        [&connectedWorkers, &claimTokens](collector::ProtocolConnectionInfo info) {
            require(claimTokens.contains(info.connectionId), "TCP connection lost its claimed generation");
            connectedWorkers.push_back(1);
            (void)info;
        },
        noPacket,
        noDisconnect,
        noState,
        claim,
        release
    );

    firstWorker.reload(snapshot);
    secondWorker.reload(snapshot);
    io.run_for(std::chrono::milliseconds(250));
    require(claimAttempts == 2, "every Collector Worker did not attempt the TCP Client target");
    require(connectedWorkers.size() == 1, "connection claim did not leave exactly one owning Worker");
    require(accepted.size() == 1, "a losing Worker opened a physical TCP connection before claiming the target");

    firstWorker.stop();
    secondWorker.stop();
    server.close();
    scheduler0.stop();
    scheduler1.stop();
    io.stop();
}

void testTcpClientLateClaimCompletionAfterStop() {
    asio::io_context io;
    collector::DeadlineScheduler scheduler(io);
    asio::ip::tcp::acceptor server(
        io,
        { asio::ip::make_address("127.0.0.1"), 0 }
    );
    std::vector<std::shared_ptr<asio::ip::tcp::socket>> accepted;
    auto acceptedSocket = std::make_shared<asio::ip::tcp::socket>(io);
    server.async_accept(*acceptedSocket, [&accepted, acceptedSocket](const std::error_code& error) {
        if (!error) {
            accepted.push_back(acceptedSocket);
        }
    });

    std::set<std::string> leaseOwners;
    std::vector<std::pair<std::string, std::function<void(bool)>>> claims;
    std::vector<std::string> released;
    auto snapshot = clientReconcileSnapshot();
    snapshot.links.front().targets.resize(1);
    snapshot.links.front().targets.front().port = server.local_endpoint().port();
    snapshot.devices.resize(1);
    service::common::UuidV7Generator tcpUuids;
    collector::TcpTransport tcp(tcpUuids,
        io,
        scheduler,
        0,
        1,
        [](collector::ProtocolConnectionInfo) {
        },
        [](service::message::IngressPacket) {
        },
        [](std::string, std::string) {
        },
        [](collector::LinkState) {
        },
        [&claims, &leaseOwners](std::string, std::string, std::string token, std::function<void(bool)> complete) {
            const auto generation = token;
            claims.emplace_back(
                std::move(token),
                [&leaseOwners, generation, complete = std::move(complete)](bool claimed) mutable {
                    if (claimed) {
                        leaseOwners.insert(generation);
                    }
                    complete(claimed);
                }
            );
        },
        [&released, &leaseOwners](std::string, std::string, std::string token) {
            released.push_back(std::move(token));
            leaseOwners.erase(released.back());
        }
    );

    tcp.reload(snapshot);
    require(claims.size() == 1 && !claims.front().first.empty(), "TCP target claim was not pending before the physical connection");
    const auto firstToken = claims.front().first;
    require(tcp.connectionId(snapshot.links.front().id, snapshot.links.front().targets.front().id) == firstToken, "pending target claim did not expose its connection generation");
    io.run_for(std::chrono::milliseconds(100));
    require(accepted.empty(), "a pending target claim opened a physical connection");

    tcp.stop();
    require(released.size() == 1 && released.front() == firstToken, "stopping a pending target claim did not release its exact generation");
    tcp.reload(snapshot);
    require(claims.size() == 2 && claims.back().first != firstToken, "reloading a stopped target did not create a new claim generation");
    const auto secondToken = claims.back().first;
    claims.front().second(true);
    require(tcp.connectionId(snapshot.links.front().id, snapshot.links.front().targets.front().id) == secondToken, "a late claim callback changed the replacement target generation");
    require(leaseOwners.empty(), "a late successful claim was not CAS-released after the original target stopped");
    claims.back().second(true);
    io.restart();
    io.run_for(std::chrono::milliseconds(250));
    require(accepted.size() == 1, "the replacement target did not establish its claimed physical connection");
    require(released.size() == 2 && released.back() == firstToken, "a late claim callback did not CAS-release the stopped generation");
    require(!leaseOwners.contains(firstToken) && leaseOwners.contains(secondToken), "the late claim callback left the stopped generation owned or lost the replacement lease");

    tcp.stop();
    require(released.size() == 3 && released.back() == secondToken, "stopping the replacement target did not release only its own generation");
    require(leaseOwners.empty(), "stopping the replacement target left its lease owned");
    server.close();
    scheduler.stop();
    io.stop();
}

void testTcpClientRevokePendingConnect() {
    asio::io_context io;
    collector::DeadlineScheduler scheduler(io);
    asio::ip::tcp::acceptor server(
        io,
        { asio::ip::make_address("127.0.0.1"), 0 }
    );
    std::vector<std::shared_ptr<asio::ip::tcp::socket>> accepted;
    auto acceptedSocket = std::make_shared<asio::ip::tcp::socket>(io);
    server.async_accept(*acceptedSocket, [&accepted, acceptedSocket](const std::error_code& error) {
        if (!error) {
            accepted.push_back(acceptedSocket);
        }
    });

    std::set<std::string> leaseOwners;
    std::vector<std::string> released;
    std::string claimedToken;
    std::function<void(bool)> completeClaim;
    auto snapshot = clientReconcileSnapshot();
    snapshot.links.front().targets.resize(1);
    snapshot.links.front().targets.front().port = server.local_endpoint().port();
    snapshot.devices.resize(1);
    service::common::UuidV7Generator tcpUuids;
    collector::TcpTransport tcp(tcpUuids,
        io,
        scheduler,
        0,
        1,
        [](collector::ProtocolConnectionInfo) {
        },
        [](service::message::IngressPacket) {
        },
        [](std::string, std::string) {
        },
        [](collector::LinkState) {
        },
        [&claimedToken, &completeClaim, &leaseOwners](std::string, std::string, std::string token, std::function<void(bool)> complete) {
            claimedToken = std::move(token);
            const auto generation = claimedToken;
            completeClaim = [complete = std::move(complete), &leaseOwners, generation](bool claimed) mutable {
                if (claimed) {
                    leaseOwners.insert(generation);
                }
                complete(claimed);
            };
        },
        [&released, &leaseOwners](std::string, std::string, std::string token) {
            released.push_back(std::move(token));
            leaseOwners.erase(released.back());
        }
    );

    tcp.reload(snapshot);
    require(!claimedToken.empty() && completeClaim, "TCP target claim did not produce a generation before connect");
    require(tcp.revokeTarget(claimedToken, "test_revoke"), "pending target connect generation was not revocable");
    completeClaim(true);
    io.run_for(std::chrono::milliseconds(250));
    require(accepted.empty(), "revoking a pending target connect still reached the device");
    require(released.size() == 2 && released.front() == claimedToken && released.back() == claimedToken, "revoking a pending target connect did not CAS-release its late claimed generation");
    require(leaseOwners.empty(), "revoking a pending target connect left its late claim owned");

    tcp.stop();
    server.close();
    scheduler.stop();
    io.stop();
}

void testTcpCloseDuringPendingWrite() {
    asio::io_context io;
    collector::DeadlineScheduler scheduler(io);
    asio::ip::tcp::acceptor server(io, { asio::ip::make_address("127.0.0.1"), 0 });
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    server.async_accept(*accepted, [](const std::error_code&) {
    });

    std::string connectionId;
    int writeCompletions = 0;
    service::common::UuidV7Generator tcpUuids;
    collector::TcpTransport tcp(tcpUuids,
        io,
        scheduler,
        0,
        1,
        [&connectionId](collector::ProtocolConnectionInfo info) {
            connectionId = std::move(info.connectionId);
        },
        [](service::message::IngressPacket) {
        },
        [](std::string, std::string) {
        },
        [](collector::LinkState) {
        }
    );

    auto snapshot = clientReconcileSnapshot();
    snapshot.links.front().targets.resize(1);
    snapshot.links.front().targets.front().port = server.local_endpoint().port();
    snapshot.devices.resize(1);
    snapshot.devices.front().targetId = snapshot.links.front().targets.front().id;
    tcp.reload(snapshot);
    io.run_for(std::chrono::milliseconds(250));
    require(!connectionId.empty(), "pending-write fixture did not connect");

    io.restart();
    tcp.send(connectionId, std::vector<std::uint8_t>(8 * 1024 * 1024, 0x5A), [&writeCompletions](bool) {
        ++writeCompletions;
    });
    tcp.close(connectionId, "test_close_during_write");
    io.run_for(std::chrono::milliseconds(250));
    require(writeCompletions == 1, "closing a pending TCP write completed its callback more than once");

    tcp.stop();
    server.close();
    accepted->close();
    scheduler.stop();
    io.stop();
}

void testEdgeRawAcquisitionContract() {
    using Raw = service::edge::acquisition::RawAcquisition;
    namespace pb = service::edge::pb;
    collector::DeviceDefinition device;
    device.id = "device"; device.code = "000000000001"; device.linkId = "link";
    device.protocol = "Modbus"; device.modbusMode = "TCP"; device.slaveId = 1;
    device.elements.push_back({.id="point", .name="Value", .dataType="UINT16",
        .byteOrder="BIG_ENDIAN", .registerType="HOLDING_REGISTER", .address=100, .quantity=1,
        .scale=1, .decimals=-1, .area="D", .size=2});
    pb::TelemetryRecord record;
    record.set_record_id(std::string(16, 1)); record.set_device_id(std::string(16, 2));
    record.set_observed_at_ms(1789582200000); record.set_protocol(pb::PROTOCOL_MODBUS);
    const auto append = [&](std::span<const std::uint8_t> request, std::span<const std::uint8_t> response) {
        record.add_raw_requests(request.data(), request.size());
        record.add_raw_payloads(response.data(), response.size());
    };
    const auto check = [&](std::string_view value) {
        const auto decoded = Raw::decode(record, device);
        require(decoded.valuesJson.find("\"value\":" + std::string(value)) != std::string::npos,
                "edge request address/config did not decode the business response");
    };
    const auto reject = [&] {
        bool rejected = false;
        try { (void)Raw::decode(record, device); } catch (const std::exception&) { rejected = true; }
        require(rejected, "edge accepted an abnormal or mismatched business exchange");
    };
    append(std::array<std::uint8_t,12>{0,7,0,0,0,6,1,3,0,100,0,1},
           std::array<std::uint8_t,11>{0,7,0,0,0,5,1,3,2,0x12,0x34});
    check("4660"); (*record.mutable_raw_payloads(0))[1] = 8; reject();
    record.clear_raw_requests(); record.clear_raw_payloads(); device.modbusMode = "RTU";
    auto request = withModbusCrc({1,3,0,100,0,1}); auto response = withModbusCrc({1,3,2,0x12,0x34});
    append(request, response); check("4660"); response.back() ^= 1;
    record.set_raw_payloads(0, response.data(), response.size()); reject();
    record.clear_raw_requests(); record.clear_raw_payloads();
    device.protocol = "S7"; record.set_protocol(pb::PROTOCOL_S7);
    device.elements[0].area="DB"; device.elements[0].dbNumber=1; device.elements[0].start=0;
    request=s7ReadRequest(12); request[24]=2; response=s7ReadResponse(12);
    append(request,response); check("4660");
    device.elements[0].scale=0.1; device.elements[0].decimals=1; check("466.0");
    device.elements[0].scale=1.0; device.elements[0].decimals=-1;
    (*record.mutable_raw_payloads(0))[12]=13; reject();
    const auto checkS7Number = [&](std::string_view type, std::vector<std::uint8_t> bytes,
                                   double scale, std::int64_t decimals, std::string_view expected) {
        record.clear_raw_requests(); record.clear_raw_payloads();
        device.elements[0].dataType=type; device.elements[0].size=bytes.size();
        device.elements[0].scale=scale; device.elements[0].decimals=decimals;
        request=s7ReadRequest(12); request[24]=static_cast<std::uint8_t>(bytes.size());
        response=s7ReadResponse(12); response.resize(25); response.insert(response.end(),bytes.begin(),bytes.end());
        response[3]=static_cast<std::uint8_t>(response.size());
        response[16]=static_cast<std::uint8_t>(4+bytes.size()); response[24]=static_cast<std::uint8_t>(bytes.size()*8);
        append(request,response); check(expected);
    };
    checkS7Number("REAL",{0x40,0x20,0,0},2.0,1,"5.0");
    checkS7Number("LREAL",{0x40,4,0,0,0,0,0,0},2.0,1,"5.0");
    checkS7Number("UINT64",{0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff},0.1,1,"18446744073709551615");
    device.elements[0].scale=1.0; device.elements[0].decimals=-1;
    record.clear_raw_requests(); record.clear_raw_payloads();
    device.elements[0].dataType="BOOL"; device.elements[0].startBit=5; device.elements[0].size=1;
    request=s7ReadRequest(14); request[22]=1; request[30]=5;
    response=s7ReadResponse(14); response.pop_back(); response[3]=26; response[16]=5;
    response[22]=3; response[24]=1; response[25]=1;
    append(request,response); check("1");
    record.clear_raw_requests(); record.clear_raw_payloads();
    device.elements[0].dataType="UINT16"; device.elements[0].size=2; device.elements[0].startBit=0;
    device.protocol="MC"; device.elements[0].area="D"; device.elements[0].byteOrder="LITTLE_ENDIAN";
    record.set_protocol(pb::PROTOCOL_MC);
    for (bool four : {false, true}) {
        collector::mc::Connection connection;
        connection.frame = four ? collector::mc::FrameFormat::Binary4E : collector::mc::FrameFormat::Binary3E;
        request=collector::mc::FrameCodec::read(connection, {.deviceCode=0xa8,.number=100,.count=1}, 7);
        response={0xd0,0,0,0xff,0xff,3,0,4,0,0,0,0x34,0x12};
        if (four) { response[0]=0xd4; response.insert(response.begin()+2,{7,0,0,0}); }
        append(request,response); check("4660"); (*record.mutable_raw_payloads(0))[four ? 13 : 9]=1; reject();
        record.clear_raw_requests(); record.clear_raw_payloads();
    }
    device.protocol="FINS"; device.elements[0].byteOrder="BIG_ENDIAN"; record.set_protocol(pb::PROTOCOL_FINS);
    collector::fins::Connection fins{.destinationNode=20,.sourceNode=10};
    request=collector::fins::FrameCodec::read(fins,{.memoryArea=0x82,.word=100,.count=1},7);
    response={'F','I','N','S',0,0,0,24,0,0,0,2,0,0,0,0,0xc0,0,2,0,10,0,0,20,0,7,1,1,0,0,0x12,0x34};
    append(request,response); check("4660"); (*record.mutable_raw_payloads(0))[25]=8; reject();
    record.clear_raw_requests(); record.clear_raw_payloads();
    device.protocol="DLT645"; record.set_protocol(pb::PROTOCOL_DLT645);
    device.elements[0].dataType="BCD"; device.elements[0].length=4; device.elements[0].digits=2;
    for (bool legacy : {false,true}) {
        device.dlt645Connection.version = legacy ? collector::dlt645::Version::V1997 : collector::dlt645::Version::V2007;
        device.elements[0].guideHex=legacy?"9010":"00000000";
        const auto di=collector::dlt645::FrameCodec::identifier(device.elements[0].guideHex,device.dlt645Connection.version);
        auto data=di; data.insert(data.end(),{0x78,0x56});
        request=collector::dlt645::FrameCodec::read(device.dlt645Connection,device.code,device.elements[0].guideHex);
        response=collector::dlt645::FrameCodec::frame(device.code,legacy?0xa1:0xb1,data); append(request,response);
        data=di; data.insert(data.end(),{0x34,0x12,1});
        request=collector::dlt645::FrameCodec::read(device.dlt645Connection,device.code,device.elements[0].guideHex,1);
        response=collector::dlt645::FrameCodec::frame(device.code,legacy?0x82:0x92,data); append(request,response);
        check("123456.78"); record.mutable_raw_requests()->RemoveLast(); reject();
        record.clear_raw_requests(); record.clear_raw_payloads();
    }
    device.protocol="SL651"; device.code="0001000102"; device.sl651ResponseMode="M2";
    device.elements.clear();
    device.elements.push_back({.id="point",.name="Water",.functionCode="32",.guideHex="3923",.encoding="BCD",.length=4,.digits=3});
    record.set_protocol(pb::PROTOCOL_SL651); record.set_function_code("32");
    response=collector::sl651::detail::hexBytes("7E7E010001000102FFFA320031020076260917021000F1F1000100010249F0F02609170210392300001521371B000654272B0000001458FFB02800000000190330CE");
    append({},response); check("\"00001.521\""); (*record.mutable_raw_payloads(0)).back() ^= 1; reject();
    record.clear_raw_requests(); record.clear_raw_payloads();
    request={0x7e,0x7e,0,1,0,1,2,1,0xff,0xfa,0x32,0x80,8,2,0,0,0x26,9,0x17,2,0x10,0,5};
    const auto queryCrc=crc16(request);
    request.push_back(static_cast<std::uint8_t>(queryCrc>>8)); request.push_back(static_cast<std::uint8_t>(queryCrc));
    append(request,response); check("\"00001.521\"");
}

void testEdgeParsedMessageContract() {
    namespace pb = service::edge::pb;
    service::message::ParsedDeviceMessage parsed;
    parsed.messageId = "019f91c9-4087-7e6c-88c0-c431b0dc15d8";
    parsed.acquisitionId = parsed.messageId;
    parsed.causationId = parsed.messageId;
    parsed.linkId = "019f91bf-6f83-7491-8a53-cd4fde034b73";
    parsed.deviceId = "019f91bf-6f83-7491-8a53-cd4fde034b72";
    parsed.deviceCode = "PCS7";
    parsed.protocol = "S7";
    parsed.connectionId = "019f8c99-913c-7a0c-b6ad-c43bb9b12764";
    parsed.occurredAtMs = 1784856700000;
    parsed.observedAtMs = 1784856700000;
    parsed.storagePolicy = "change";
    parsed.source = "edge";
    parsed.valuesJson = R"json({"values":{"VW0":{"name":"VW0","value":1,"unit":""}}})json";

    service::message::StreamMessage streamMessage{ .id = "1-0",
                                                   .fields = service::message::parsedFields(parsed) };
    const auto roundTrip = service::message::parsedFrom(streamMessage);
    require(roundTrip.linkId == parsed.linkId, "edge parsed message lost its link identity");
    require(roundTrip.rawPayloads.empty(), "edge parsed message changed empty raw payloads");
    parsed.rawPayloads = {{0x00, 0x01, 0xFF}, {0x03, 0x00, 0x04, 0x00}};
    service::message::StreamMessage rawStream{
        .id = "2-0", .fields = service::message::parsedFields(parsed)};
    require(service::message::parsedFrom(rawStream).rawPayloads == parsed.rawPayloads,
            "edge history raw response array lost bytes or ordering in Redis serialization");
    struct EdgeResponseProjection : service::edge::EdgeProjectionService {
        using EdgeProjectionService::collectTelemetry;
        using EdgeProjectionService::assembleTelemetryParts;
        using EdgeProjectionService::storeTelemetryPart;
    };
    service::edge::metadata::Catalog catalog;
    catalog[parsed.connectionId][parsed.deviceId] = {
        .linkId = parsed.linkId, .deviceCode = parsed.deviceCode,
        .protocol = "S7", .storagePolicy = "report", .onlineWindowMs = 60000};
    service::edge::pb::TelemetryBatch batch;
    auto* record = batch.add_records();
    record->set_record_id(std::string(16, '\x01'));
    std::uint8_t deviceBytes[16]{};
    require(service::edge::protocol::uuidBytes(parsed.deviceId, deviceBytes), "invalid edge fixture device id");
    record->set_device_id(service::edge::protocol::bytes(deviceBytes, sizeof(deviceBytes)));
    record->set_protocol(service::edge::pb::PROTOCOL_S7);
    record->set_observed_at_ms(parsed.observedAtMs);
    record->add_raw_payloads(std::string("\x03\x00\x00\x04", 4));
    record->add_raw_packet_ids(std::string(16, '\x05'));
    std::vector<service::message::StreamMessage> projected;
    EdgeResponseProjection::collectTelemetry(catalog, parsed.connectionId,
                                            parsed.occurredAtMs, batch, projected);
    require(projected.size() == 1 && service::message::parsedFrom(projected.front()).rawPayloads ==
                std::vector<std::vector<std::uint8_t>>{{0x03, 0x00, 0x00, 0x04}},
            "edge wire response did not become a single-item history raw payload array");
    record->set_raw_payloads(0, std::string("\x7e\x7e\x00\xff", 4));
    projected.clear();
    EdgeResponseProjection::collectTelemetry(catalog, parsed.connectionId,
                                            parsed.occurredAtMs, batch, projected);
    require(service::message::parsedFrom(projected.front()).rawPayloads ==
                std::vector<std::vector<std::uint8_t>>{{0x7e, 0x7e, 0x00, 0xff}},
            "edge raw array did not preserve the exact response");
    record->clear_raw_packet_ids();
    record->add_raw_payloads(std::string("\x01\x02", 2));
    projected.clear();
    EdgeResponseProjection::collectTelemetry(catalog, parsed.connectionId,
                                            parsed.occurredAtMs, batch, projected);
    const auto legacyRecord = service::message::parsedFrom(projected.front());
    require(legacyRecord.rawPacketIds.size() == 2 &&
                legacyRecord.rawPacketIds[0] != legacyRecord.rawPacketIds[1],
            "legacy telemetry did not retain distinct original frame identities");
    projected.clear();
    EdgeResponseProjection::collectTelemetry(catalog, parsed.connectionId,
                                            parsed.occurredAtMs, batch, projected);
    require(service::message::parsedFrom(projected.front()).rawPacketIds == legacyRecord.rawPacketIds,
            "legacy telemetry replay changed original frame identities");
    pb::TelemetryBatch rawBatch;
    auto& rawRecord = *rawBatch.add_records();
    rawRecord = *record;
    rawRecord.clear_raw_payloads(); rawRecord.clear_raw_packet_ids();
    auto read = s7ReadRequest(22); read[24] = 2;
    const auto reply = s7ReadResponse(22);
    rawRecord.add_raw_requests(read.data(), read.size());
    rawRecord.add_raw_payloads(reply.data(), reply.size());
    rawRecord.add_raw_packet_ids(std::string(16, 9));
    std::vector<pb::ConfigItem> configuration(2);
    auto* configured = configuration[0].mutable_device();
    configured->set_device_id(record->device_id()); configured->set_protocol(pb::PROTOCOL_S7);
    std::uint8_t linkBytes[16]{};
    require(service::edge::protocol::uuidBytes(parsed.linkId, linkBytes), "invalid raw link fixture");
    configured->set_endpoint_id(linkBytes, 16);
    auto* area = configuration[1].mutable_s7_area();
    area->set_device_id(record->device_id()); area->set_element_id("word");
    area->set_area("DB"); area->set_db_number(1); area->set_size(2); area->set_data_type("UINT16");
    area->set_scale(1.0); area->set_decimals(-1);
    projected.clear();
    EdgeResponseProjection::collectTelemetry(catalog, parsed.connectionId, parsed.occurredAtMs, rawBatch, projected, configuration);
    const auto rawProjected = service::message::parsedFrom(projected.front());
    require(rawProjected.rawPayloads == std::vector<std::vector<std::uint8_t>>{read, reply} &&
        rawProjected.rawPacketIds.size() == 2 && rawProjected.rawPacketIds[0] == rawProjected.rawPacketIds[1] + ":request" &&
        rawProjected.valuesJson.find("4660") != std::string::npos,
        "platform projection did not retain paired original requests and responses");
    record->mutable_raw_payloads()->RemoveLast();
    record->add_raw_packet_ids(std::string(16, '\x05'));
    std::vector<service::edge::pb::TelemetryRecord> parts(3, *record);
    for (std::size_t index = 0; index < parts.size(); ++index) {
        parts[index].set_protocol(service::edge::pb::PROTOCOL_SL651);
        parts[index].set_record_id(std::string(16, static_cast<char>(index + 1)));
        parts[index].set_report_id(std::string(16, '\x04'));
        parts[index].set_part_count(3);
        parts[index].set_part_index(static_cast<std::uint32_t>(index));
        parts[index].clear_raw_packet_ids();
        parts[index].clear_raw_payloads();
    }
    auto* value = parts[0].add_values();
    value->set_element_id("temperature");
    value->mutable_value()->set_double_value(12.5);
    parts[1].add_raw_payloads(std::string("\x7e\x7e\x01\x00", 4));
    parts[2].add_raw_payloads(std::string("\x7e\x7e\x02\xff", 4));
    parts[1].add_raw_packet_ids(std::string(16, '\x06'));
    parts[2].add_raw_packet_ids(std::string(16, '\x07'));
    const auto assembled = EdgeResponseProjection::assembleTelemetryParts(parts);
    require(assembled.values_size() == 1 && assembled.values(0).element_id() == "temperature" &&
                assembled.raw_payloads_size() == 2 && assembled.raw_payloads(0) == parts[1].raw_payloads(0) &&
                assembled.raw_payloads(1) == parts[2].raw_payloads(0) &&
                assembled.record_id() == parts[0].report_id() && assembled.report_id().empty() &&
                assembled.part_count() == 0,
            "SL651 upload did not reconstruct one complete history record with ordered raw frames");
    auto incomplete = parts;
    incomplete.pop_back();
    bool rejectedIncomplete = false;
    try { (void)EdgeResponseProjection::assembleTelemetryParts(incomplete); }
    catch (const std::runtime_error&) { rejectedIncomplete = true; }
    require(rejectedIncomplete, "SL651 incomplete upload was published as a historical record");
    auto mixed = parts;
    mixed[2].set_device_id(std::string(16, '\x09'));
    bool rejectedMixed = false;
    try { (void)EdgeResponseProjection::assembleTelemetryParts(mixed); }
    catch (const std::runtime_error&) { rejectedMixed = true; }
    require(rejectedMixed, "SL651 upload mixed raw frames from different devices");
    RecordingRedis uploadRedis(0);
    require(!runTask(EdgeResponseProjection::storeTelemetryPart(uploadRedis, parsed.connectionId, parts[0])),
            "SL651 incomplete stored upload emitted telemetry");
    require(uploadRedis.keys.size() == 1 && uploadRedis.keys[0].find(parsed.deviceId) != std::string::npos &&
                uploadRedis.arguments.size() == 4 && uploadRedis.arguments[1] == "0" && uploadRedis.arguments[2] == "3",
            "SL651 stored upload lost device isolation or part addressing");
    require(roundTrip.deviceCode == "PCS7" && roundTrip.source == "edge" && roundTrip.storagePolicy == "change", "edge parsed message did not round-trip required fields");

    auto invalidPolicy = streamMessage;
    for (auto& field : invalidPolicy.fields) {
        if (field.name == "storage_policy") {
            field.value = "interval";
        }
    }
    bool rejectedInvalidPolicy = false;
    try {
        (void)service::message::parsedFrom(invalidPolicy);
    } catch (const std::runtime_error&) {
        rejectedInvalidPolicy = true;
    }
    require(rejectedInvalidPolicy, "parsed message accepted an invalid storage policy");

    parsed.occurredAtMs = 1784856700000;
    parsed.observedAtMs = 4102444800000;
    streamMessage.fields = service::message::parsedFields(parsed);
    const auto future = service::message::parsedFrom(streamMessage);
    require(future.observedAtMs == parsed.occurredAtMs, "future device timestamp poisoned telemetry ordering");

    parsed.observedAtMs = parsed.occurredAtMs - 60'000;
    streamMessage.fields = service::message::parsedFields(parsed);
    const auto historical = service::message::parsedFrom(streamMessage);
    require(historical.observedAtMs == parsed.observedAtMs, "valid historical device timestamp was replaced by receive time");
}

void testAtomicPendingCommandDispatch() {
    service::message::ProtocolTask first;
    first.messageId = "019f91c9-4087-7e6c-88c0-c431b0dc15d8";
    first.deviceId = "019f91bf-6f83-7491-8a53-cd4fde034b72";
    first.deviceCode = "D1";
    first.protocol = "Modbus";
    first.createdAtMs = 1000;
    auto second = first;
    second.messageId = "019f91c9-4087-7e6c-88c0-c431b0dc15d9";

    std::vector<service::command::PendingDispatch> dispatches;
    dispatches.push_back({ .task = first, .streamFields = { { "message_id", first.messageId }, { "device_id", first.deviceId } } });
    dispatches.push_back({ .task = second, .streamFields = { { "message_id", second.messageId }, { "device_id", second.deviceId } } });

    RecordingRedis redis(2);
    runTask(service::command::dispatchPendingBatch(redis, "iot:channel:command:worker:0:high", service::command::PendingQueueKind::Stream, dispatches, 10'000));
    require(redis.keys.size() == 1, "queue must not own a second command status model");
    require(redis.script.find("depth+count>tonumber(ARGV[2])") != std::string::npos, "queue must reject capacity overflow before writing");
    require(redis.script.find("LTRIM") == std::string::npos && redis.script.find("MAXLEN") == std::string::npos, "accepted commands must never be trimmed");
    RecordingRedis full(0);
    require(!runTask(service::command::dispatchPendingBatch(full, "queue", service::command::PendingQueueKind::Stream, dispatches, 1)), "capacity rejection must be observable");
}

void testCollectorUuidOwnership() {
    service::common::UuidV7Generator firstWorker;
    service::common::UuidV7Generator secondWorker;
    std::set<std::string> identities;
    std::string previousFirst;
    std::string previousSecond;
    for (std::size_t index = 0; index < 2048; ++index) {
        const auto first = firstWorker.next();
        const auto second = secondWorker.next();
        require(service::common::isUuid(first) && first[14] == '7', "Collector identity must remain UUIDv7");
        require(service::common::isUuid(second) && second[14] == '7', "second Collector identity must remain UUIDv7");
        require(previousFirst < first && previousSecond < second, "each Collector UUID sequence must be monotonic");
        require(identities.insert(first).second && identities.insert(second).second, "independent Collector UUID sequences must not collide");
        previousFirst = first;
        previousSecond = second;
    }
    service::common::UuidV7Generator restartedWorker;
    require(identities.insert(restartedWorker.next()).second, "recreated Collector must not reuse an identity");
}

} // namespace

int main() {
    try {
        const auto run = [](std::string_view name, auto&& test) {
            std::cerr << "[ RUN      ] " << name << '\n';
            test();
        };
        run("Collector UUID ownership", testCollectorUuidOwnership);
        run("command outcome domain", testCommandOutcomeDomain);
        run("capabilities", testCapabilities);
        run("protocol factory boundary", testProtocolFactoryBoundary);
        run("protocol debug attribution", testProtocolDebugAttribution);
        run("protocol command grouping", testProtocolCommandGrouping);
        run("runtime reconcile", testRuntimeReconcile);
        run("acquisition cycle", testAcquisitionCycle);
        run("poll stagger", testPollStagger);
        run("TCP Server worker-local listeners", testTcpServerListenersAreWorkerLocal);
        run("TCP Client target reconcile", testTcpClientTargetReconcile);
        run("TCP Client connection claim", testTcpClientTargetsUseConnectionClaim);
        run("TCP Client late claim completion after stop", testTcpClientLateClaimCompletionAfterStop);
        run("TCP Client revoke pending connect", testTcpClientRevokePendingConnect);
        run("TCP close during pending write", testTcpCloseDuringPendingWrite);
        run("station scope and instance identity", testStationScopeAndInstanceIdentity);
        run("sl651 observation replay", testSl651ObservationReplay);
        run("sl651", testSl651);
        run("sl651 element order", testSl651ElementOrder);
        run("sl651 encodings", testSl651AllEncodingsAndFunctionCodes);
        run("sl651 multi-packet images", testSl651MultiPacketImages);
        run("sl651 communication modes", testSl651CommunicationModes);
        run("sl651 fixed positions", testSl651FixedPosition);
        run("modbus", testModbus);
        run("modbus types and priority", testModbusTypesAndPriority);
        run("modbus data types", testModbusAllDataTypesAndByteOrders);
        run("modbus TCP functions", [] {
            testModbusAllFunctionCodes(true);
        });
        run("modbus RTU functions", [] {
            testModbusAllFunctionCodes(false);
        });
        run("modbus RTU zero address", testModbusRtuZeroAddress);
        run("modbus discovery and registration", testModbusDiscoveryAndOffline);
        run("s7", testS7);
        run("s7 data types", testS7AllDataTypes);
        run("deadline scheduler", testDeadlineScheduler);
        run("runtime writable contract", testRuntimeWritableContract);
        run("runtime repository invalid scale", testRuntimeRepositoryRejectsInvalidScale);
        run("atomic stream finalization contract", testAtomicStreamFinalizationContract);
        run("runtime projection Redis errors", testRuntimeProjectionRejectsRedisErrors);
        run("runtime projection previous grace", testRuntimeProjectionRefreshesPreviousGrace);
        run("runtime set ordering contract", testRuntimeSetOrderingContract);
        run("realtime projection contract", testRealtimeProjectionContract);
        run("BOOL point value contract", testBooleanPointValueContract);
        run("freshness deadline wait", testFreshnessDeadlineWait);
        run("edge session ownership", testEdgeSessionOwnership);
        run("latest projection Redis errors", testLatestProjectionRejectsRedisErrors);
        run("latest projection metadata refresh",
            testLatestProjectionRefreshesPreservedElementMetadata);
        run("latest projection invalid preserved deadline",
            testLatestProjectionRejectsInvalidPreservedDeadline);
        run("alert schedule invalid stored duration",
            testAlertScheduleSkipsInvalidStoredDuration);
        run("command value decimal parsing", testCommandValueDecimalParsing);
        run("edge parsed message contract", testEdgeParsedMessageContract);
        run("edge raw acquisition six protocols", testEdgeRawAcquisitionContract);
        run("atomic pending command dispatch", testAtomicPendingCommandDispatch);
        run("packet log", testPacketLog);
        std::cout << "collector protocol tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "collector protocol test failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
