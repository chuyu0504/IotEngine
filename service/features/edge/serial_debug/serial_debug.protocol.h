#pragma once
#include <algorithm>
#include <array>
#include <optional>
#include <string>
#include <edge.pb.h>
#include <ruvia/web/model_json.h>
#include "service/common/message.h"

namespace service::edge::serial_debug {
namespace wire = ::iot::edge::v1;

inline bool validSettings(const wire::SerialSettings& settings) {
    constexpr std::array<unsigned, 12> rates{300,600,1200,2400,4800,9600,19200,38400,57600,115200,230400,460800};
    return std::ranges::find(rates, settings.baud_rate()) != rates.end() &&
        settings.data_bits() >= 5 && settings.data_bits() <= 8 &&
        (settings.stop_bits() == 1 || settings.stop_bits() == 2) &&
        (settings.parity() == "none" || settings.parity() == "even" || settings.parity() == "odd");
}
inline std::string hexBytes(std::string_view bytes) {
    constexpr char digits[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (const unsigned char byte : bytes) {
        result.push_back(digits[byte >> 4]); result.push_back(digits[byte & 15]);
    }
    return result;
}
inline std::optional<std::string> decodeHex(std::string_view hex) {
    if (hex.empty() || hex.size() > 2048 || hex.size() % 2) return std::nullopt;
    const auto digit = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::string result;
    for (std::size_t i = 0; i < hex.size(); i += 2) {
        const int high = digit(hex[i]), low = digit(hex[i + 1]);
        if (high < 0 || low < 0) return std::nullopt;
        result.push_back(static_cast<char>((high << 4) | low));
    }
    return result;
}
inline std::optional<wire::SerialDebugRequest> decodeRequest(
    std::string_view json, std::string_view path, std::uint64_t previous) {
    if (json.size() > 4096) return std::nullopt;
    const auto input = ruvia::from_json<service::message::serial_debug::Command>(json);
    if (!input || !input->get<"action">() || !input->get<"requestId">()) return std::nullopt;
    const auto sequence = *input->get<"requestId">();
    if (sequence <= 0 || sequence > 9007199254740991LL ||
        static_cast<std::uint64_t>(sequence) <= previous) return std::nullopt;
    wire::SerialDebugRequest request;
    request.set_action(input->get<"action">()->view());
    request.set_request_sequence(static_cast<std::uint64_t>(sequence));
    request.mutable_settings()->set_channel(path);
    if (request.action() == "manual") {
        const auto baud = input->get<"baudRate">().value_or(0);
        const auto bits = input->get<"dataBits">().value_or(0);
        const auto stops = input->get<"stopBits">().value_or(0);
        if (baud < 300 || baud > 460800 || bits < 5 || bits > 8 || stops < 1 || stops > 2)
            return std::nullopt;
        auto* settings = request.mutable_settings();
        settings->set_baud_rate(static_cast<unsigned>(input->get<"baudRate">().value_or(0)));
        settings->set_data_bits(static_cast<unsigned>(input->get<"dataBits">().value_or(0)));
        settings->set_stop_bits(static_cast<unsigned>(input->get<"stopBits">().value_or(0)));
        settings->set_parity(input->get<"parity">() ? input->get<"parity">()->view() : "");
        settings->set_rs485(input->get<"rs485">().value_or(false));
        if (!validSettings(*settings)) return std::nullopt;
    } else if (request.action() == "write") {
        if (!input->get<"hex">()) return std::nullopt;
        auto bytes = decodeHex(input->get<"hex">()->view());
        if (!bytes) return std::nullopt;
        request.set_data(std::move(*bytes));
    } else if (request.action() != "monitor" && request.action() != "keepalive") {
        return std::nullopt;
    }
    return request;
}
inline std::string eventJson(const wire::SerialDebugEvent& event) {
    const auto& source = event.settings();
    service::message::serial_debug::Settings settings;
    settings.set<"path">(source.channel())
        .set<"baudRate">(static_cast<std::int64_t>(source.baud_rate()))
        .set<"dataBits">(static_cast<std::int64_t>(source.data_bits()))
        .set<"stopBits">(static_cast<std::int64_t>(source.stop_bits()))
        .set<"parity">(source.parity())
        .set<"rs485">(source.rs485());
    service::message::serial_debug::Event result;
    result.set<"kind">(event.kind())
        .set<"requestId">(static_cast<std::int64_t>(event.request_sequence()))
        .set<"manual">(event.manual())
        .set<"direction">(event.direction())
        .set<"hex">(hexBytes(event.data()))
        .set<"timestamp">(static_cast<std::int64_t>(event.occurred_at_ms()))
        .set<"sequence">(static_cast<std::int64_t>(event.sequence()))
        .set<"droppedBytes">(static_cast<std::int64_t>(event.dropped_bytes()))
        .set<"message">(event.message())
        .set<"settings">(std::move(settings));
    return service::message::rpc::encode(result, nullptr);
}
} // namespace service::edge::serial_debug
