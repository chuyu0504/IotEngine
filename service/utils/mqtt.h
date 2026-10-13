#pragma once

#include <charconv>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace service::utils::mqtt {

struct BrokerAddress final {
    bool tls{false};
    std::string host;
    std::string port;
};

inline bool validText(std::string_view text) {
    if (text.size() > 65535) return false;
    for (std::size_t i = 0; i < text.size();) {
        const auto first = static_cast<unsigned char>(text[i++]);
        std::uint32_t value = first;
        unsigned count = 0;
        if (first < 0x80) { if (!first) return false; continue; }
        if (first >= 0xc2 && first <= 0xdf) { value &= 0x1f; count = 1; }
        else if (first >= 0xe0 && first <= 0xef) { value &= 0x0f; count = 2; }
        else if (first >= 0xf0 && first <= 0xf4) { value &= 7; count = 3; }
        else return false;
        for (unsigned remaining = count; remaining; --remaining) {
            if (i == text.size()) return false;
            const auto next = static_cast<unsigned char>(text[i++]);
            if ((next & 0xc0) != 0x80) return false;
            value = (value << 6) | (next & 0x3f);
        }
        if ((count == 1 && value < 0x80) || (count == 2 && value < 0x800) ||
            (count == 3 && value < 0x10000) || value > 0x10ffff ||
            (value >= 0xd800 && value <= 0xdfff)) return false;
    }
    return true;
}

inline BrokerAddress parseBroker(std::string_view url) {
    BrokerAddress result;
    if (url.starts_with("mqtts://")) { result.tls = true; result.port = "8883"; url.remove_prefix(8); }
    else if (url.starts_with("mqtt://")) { result.port = "1883"; url.remove_prefix(7); }
    else throw std::invalid_argument("Broker 地址须使用 mqtt:// 或 mqtts://");
    if (url.ends_with('/')) url.remove_suffix(1);
    if (url.empty() || url.find_first_of("/@?#\\ \t\r\n") != std::string_view::npos)
        throw std::invalid_argument("Broker 地址不能包含凭据、路径或查询参数");
    std::string_view port;
    if (url.front() == '[') {
        const auto end = url.find(']');
        if (end == std::string_view::npos) throw std::invalid_argument("Broker IPv6 地址无效");
        result.host = url.substr(1, end - 1);
        if (end + 1 < url.size()) {
            if (url[end + 1] != ':') throw std::invalid_argument("Broker 端口无效");
            port = url.substr(end + 2);
            if (port.empty()) throw std::invalid_argument("Broker 端口无效");
        }
    } else {
        const auto colon = url.find(':');
        result.host = url.substr(0, colon);
        if (colon != std::string_view::npos) {
            port = url.substr(colon + 1);
            if (port.empty()) throw std::invalid_argument("Broker 端口无效");
        }
    }
    if (result.host.empty() || !validText(result.host)) throw std::invalid_argument("Broker 主机无效");
    if (!port.empty()) {
        unsigned value = 0;
        const auto parsed = std::from_chars(port.data(), port.data() + port.size(), value);
        if (parsed.ec != std::errc{} || parsed.ptr != port.data() + port.size() || !value || value > 65535)
            throw std::invalid_argument("Broker 端口须在 1 - 65535 之间");
        result.port = std::to_string(value);
    }
    return result;
}

inline void word(std::string& bytes, std::uint16_t value) {
    bytes.push_back(static_cast<char>(value >> 8));
    bytes.push_back(static_cast<char>(value));
}

inline void text(std::string& bytes, std::string_view value) {
    if (!validText(value)) throw std::invalid_argument("MQTT UTF-8 字段无效");
    word(bytes, static_cast<std::uint16_t>(value.size()));
    bytes.append(value);
}

inline std::string packet(unsigned char header, std::string_view body) {
    if (body.size() > 1024 * 1024) throw std::invalid_argument("MQTT 消息超过 1 MiB");
    std::string result(1, static_cast<char>(header));
    auto remaining = body.size();
    do {
        auto digit = static_cast<unsigned char>(remaining % 128);
        remaining /= 128;
        result.push_back(static_cast<char>(digit | (remaining ? 0x80 : 0)));
    } while (remaining);
    result.append(body);
    return result;
}

inline std::string connect(std::string_view clientId, std::string_view username, std::string_view password) {
    if (clientId.empty() || (!password.empty() && username.empty()) || password.size() > 65535)
        throw std::invalid_argument("MQTT 连接凭据无效");
    std::string body;
    text(body, "MQTT");
    body.push_back(4);
    body.push_back(static_cast<char>(2 | (username.empty() ? 0 : 0x80) | (password.empty() ? 0 : 0x40)));
    word(body, 0); // 每次投递是有超时边界的独立会话，无需 Keep Alive。
    text(body, clientId);
    if (!username.empty()) text(body, username);
    if (!password.empty()) { word(body, static_cast<std::uint16_t>(password.size())); body.append(password); }
    return packet(0x10, body);
}

inline std::string publish(std::string_view topicName, std::string_view payload) {
    if (topicName.empty() || topicName.size() > 1024 || !validText(topicName) || topicName.find_first_of("+#{}") != std::string_view::npos)
        throw std::invalid_argument("MQTT 发布 Topic 无效");
    std::string body;
    text(body, topicName);
    word(body, 1); // 独立会话仅有一个在途 QoS 1 消息。
    body.append(payload);
    return packet(0x32, body);
}

inline void checkConnack(std::string_view bytes) {
    if (bytes.size() != 4 || static_cast<unsigned char>(bytes[0]) != 0x20 || bytes[1] != 2 || bytes[2] != 0)
        throw std::runtime_error("MQTT CONNACK 格式无效");
    if (bytes[3] != 0) throw std::runtime_error("Broker 拒绝连接，CONNACK=" + std::to_string(static_cast<unsigned char>(bytes[3])));
}

inline void checkPuback(std::string_view bytes) {
    if (bytes != std::string_view("\x40\x02\x00\x01", 4))
        throw std::runtime_error("MQTT PUBACK 格式或消息编号无效");
}

} // namespace service::utils::mqtt
