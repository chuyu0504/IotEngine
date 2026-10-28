#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/web/model.h>

#include "service/common/message.h"
#include "service/utils/json.h"

namespace service::telemetry::contract {

// 只在 valuesJson 含非法 UTF-8 字节时复制修复（sanitizeJsonUtf8）；合法文本只扫描、不复制。
// Dispatch 复制前调用；消费者对升级前写入、未经修复的副本再调用一次，避免毒消息无限重试。
inline void repairInvalidUtf8(message::ParsedDeviceMessage& input) {
    const std::string_view json = input.valuesJson;
    for (std::size_t index = 0; index < json.size();) {
        const auto length = service::utils::utf8SequenceLength(json, index);
        if (length == 0) {
            input.valuesJson = service::utils::sanitizeJsonUtf8(json);
            return;
        }
        index += length;
    }
}

// 无要素的 SL651 报文只表示设备活动，不能作为指标样本。
inline bool isSl651EmptyReport(const message::ParsedDeviceMessage& input) {
    if (input.protocol != "SL651") {
        return false;
    }
    const auto root = ruvia::json_value::parse(input.valuesJson);
    std::optional<bool> empty;
    if (!root.for_each_field([&](std::string_view name, const ruvia::json_value& value) {
            if (name != "values") {
                return true;
            }
            if (!value.is_object()) {
                empty.reset();
                return true;
            }
            bool found = false;
            (void)value.for_each_field([&](std::string_view, const ruvia::json_value&) {
                found = true;
                return false;
            });
            empty = !found;
            return true;
        }) ||
        !empty) {
        throw std::runtime_error("Invalid telemetry object");
    }
    return *empty;
}

// 将采集结果规范化为统一的历史值结构，保留采集端创建的轮次身份。
// 整个文档只解析一次：根、values 与每个点都在同一次遍历中借用根文档的切片。
inline void normalize(message::ParsedDeviceMessage& input) {
    const auto root = ruvia::json_value::parse(input.valuesJson);
    if (!root.is_object()) {
        throw std::runtime_error("Telemetry must be a JSON object");
    }
    std::string out = "{";
    std::string points;
    bool valuesFound = false;
    bool media = false;
    const auto appendPoint = [&](std::string_view id, const ruvia::json_value& point) {
        if (!point.is_object()) {
            throw std::runtime_error("Telemetry point must be an object");
        }
        std::optional<ruvia::json_value::kind_type> valueKind;
        bool invalidText = false;
        bool image = false;
        std::optional<bool> derived;
        std::optional<std::string> reportedQuality;
        std::optional<std::int64_t> reportedSampleTime;
        std::string body = service::utils::jsonQuoted(id) + ":{";
        (void)point.for_each_field([&](std::string_view key, const ruvia::json_value& field) {
            if (key == "value") {
                valueKind = field.kind();
                const auto text = field.get<ruvia::string>();
                invalidText = text && text->view() == "INVALID_JPEG";
            } else if (key == "type") {
                const auto text = field.get<ruvia::string>();
                image = text && text->view() == "JPEG";
            } else if (key == "derived") {
                const auto flag = field.get<ruvia::bool_value>();
                derived = flag ? std::optional<bool>(static_cast<bool>(*flag)) : std::nullopt;
            } else if (key == "quality") {
                const auto text = field.get<ruvia::string>();
                reportedQuality = text ? std::optional<std::string>(text->view()) : std::nullopt;
            } else if (key == "sample_time_ms") {
                const auto time = field.get<ruvia::int64>();
                reportedSampleTime = time ? std::optional<std::int64_t>(time->value_) : std::nullopt;
            }
            if (key != "value_type" && key != "quality" && key != "sample_time_ms" && key != "received_time_ms") {
                body += service::utils::jsonQuoted(key);
                body += ':';
                body += field.view();
                body += ',';
            }
            return true;
        });
        if (!valueKind) {
            throw std::runtime_error("Telemetry point has no value");
        }
        using Kind = ruvia::json_value::kind_type;
        const bool isNull = *valueKind == Kind::null;
        const bool derivedPoint = derived && *derived;
        media = media || image;
        const std::string_view kind = image ? "media"
            : *valueKind == Kind::number    ? "number"
            : *valueKind == Kind::boolean   ? "boolean"
            : *valueKind == Kind::string    ? "string"
            : isNull                        ? "null"
                                            : "json";
        const bool invalid = image && invalidText;
        const auto quality = derivedPoint && reportedQuality ? *reportedQuality
                                                             : std::string(invalid ? "invalid" : isNull ? "missing"
                                                                                                        : "good");
        const auto sampleTime = derivedPoint && reportedSampleTime ? *reportedSampleTime : input.observedAtMs;
        body += "\"value_type\":" + service::utils::jsonQuoted(kind) + ",\"quality\":" + service::utils::jsonQuoted(quality) +
            ",\"sample_time_ms\":" + std::to_string(sampleTime) + ",\"received_time_ms\":" + std::to_string(input.occurredAtMs) + "}";
        if (!points.empty()) {
            points += ',';
        }
        points += body;
        return true;
    };
    (void)root.for_each_field([&](std::string_view key, const ruvia::json_value& value) {
        if (key == "values") {
            // 重复键以最后一次出现为准，与按键取值的语义一致。
            if (!value.is_object()) {
                valuesFound = false;
                return true;
            }
            valuesFound = true;
            points.clear();
            media = false;
            (void)value.for_each_field(appendPoint);
        } else if (key != "model" && key != "event_kind" && key != "schema_version" && key != "raw_packet_ids") {
            out += service::utils::jsonQuoted(key);
            out += ':';
            out += value.view();
            out += ',';
        }
        return true;
    });
    if (!valuesFound) {
        throw std::runtime_error("Telemetry values must be an object");
    }
    input.eventKind = media ? "image" : "sample";
    out += "\"raw_packet_ids\":[";
    for (std::size_t index = 0; index < input.rawPacketIds.size(); ++index) {
        if (index) {
            out += ',';
        }
        out += service::utils::jsonQuoted(input.rawPacketIds[index]);
    }
    out += "],\"schema_version\":1,\"event_kind\":" + service::utils::jsonQuoted(input.eventKind) + ",\"values\":{" + points + "},\"model\":";
    out += input.modelId.empty() ? "null" : "{\"id\":" + service::utils::jsonQuoted(input.modelId) + "}";
    out += '}';
    // 历史身份由采集端创建；接收时间和内容不能重写轮次身份。
    input.valuesJson = std::move(out);
    if (input.acquisitionId.empty()) {
        throw std::invalid_argument("telemetry acquisition ID is required");
    }
    input.messageId = input.acquisitionId;
}

} // namespace service::telemetry::contract
