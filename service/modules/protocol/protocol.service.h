#pragma once

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/model_object.h>
#include <ruvia/web/request_validation.h>
#include <ruvia/web/validation.h>

#include "service/common/database.h"
#include "service/common/derived_point.h"
#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/mqtt_report.h"
#include "service/common/uuid.h"
#include "service/modules/device/device.entity.h"
#include "service/modules/edge_node/edge_node.entity.h"
#include "service/modules/edge_node/edge_node.service.h"
#include "service/modules/link/link.entity.h"
#include "service/modules/protocol/protocol.entity.h"
#include "service/modules/protocol/protocol.types.h"
#include "service/modules/system/outbox/outbox.service.h"
#include "service/modules/system/role/role.entity.h"
#include "service/modules/system/user/user.entity.h"
#include "service/utils/expression.h"
#include "service/utils/json.h"
#include "service/utils/payload.h"
#include "service/utils/rpc_client.h"
#include "service/utils/uuid.h"

namespace service::protocol {

class ProtocolService {
  public:
    // 校验协议配置。complete 为 false 时是 PATCH 片段：只校验出现的字段，MQTT 在合并后整体校验。
    // 字段约束在 protocol.types.h 的各协议 Model；这里只保留 Model 无法声明的规则：需原文案的必填、
    // 已退役字段、json_value 数字字段（保留指数与字符串写法）以及跨字段规则。
    static void validateConfig(std::string_view protocol, const ruvia::json_value& config, bool complete) {
        if (!config.is_object()) {
            service::common::fail(16004, "config 必须是对象", 400);
        }
        for (const auto field : { "registers", "areas", "points" }) {
            if (const auto points = config.get<ruvia::json_value>(field); points && !nativePointsWithoutConversion(*points)) {
                service::common::fail(16004, "原生点位不支持倍率和偏移，请使用派生值表达式", 400);
            }
        }
        const auto common = parseConfig<ProtocolConfigCommon>(config, { { "storageInterval", "配置不允许 storageInterval，请使用 storagePolicy" }, { "storagePolicy", kStoragePolicyInvalid.view() }, { "deviceParameters", "设备参数请在设备管理中配置" } }, "config 必须是对象");
        if (common.is_present<"storageInterval">()) {
            service::common::fail(16004, "配置不允许 storageInterval，请使用 storagePolicy", 400);
        }
        if (complete && !common.get<"storagePolicy">()) {
            service::common::fail(16004, "配置的 storagePolicy 不能为空", 400);
        }
        if (common.is_present<"deviceParameters">()) {
            service::common::fail(16004, "设备参数请在设备管理中配置", 400);
        }
        const auto parameters = ruvia::from_json<service::common::DeviceParameterConfiguration>(config.view());
        if (!parameters) {
            service::common::fail(16004, "设备参数定义须为数组", 400);
        }
        if (const auto& definitions = parameters->get<"parameters">()) {
            try {
                service::common::validateDeviceParameterDefinitions(*definitions);
            } catch (const std::invalid_argument& error) {
                service::common::fail(16004, error.what(), 400);
            }
        }
        if (protocol == "MQTT") {
            if (complete) {
                validateMqtt(config);
            }
        } else if (protocol == "SL651") {
            validateSl651(config, complete);
        } else if (protocol == "MC") {
            validateMc(config);
        } else if (protocol == "FINS") {
            validateFins(config);
        } else if (protocol == "DLT645") {
            validateDlt645(config);
        } else if (protocol == "S7") {
            validateS7(config, complete);
        } else if (protocol == "Modbus") {
            validateModbus(config, complete);
        }
    }

    // 校验完整配置中的派生点与点位显示设置，返回是否配置了派生点。
    static bool validateDerivedPoints(const ruvia::json_value& config) {
        const auto parsed = parseLastValue<DerivedPointsConfig>(config);
        if (!parsed) {
            const auto points = config.get<ruvia::json_value>("derivedPoints");
            if (points && !points->is_array()) {
                service::common::fail(16004, "derivedPoints 必须是数组", 400);
            }
            bool objects = true;
            if (points) {
                (void)points->for_each_element([&](const ruvia::json_value& point) {
                    objects = point.is_object();
                    return objects;
                });
            }
            if (!objects) {
                fail(kDerivedPointsLimit);
            }
            service::common::fail(16004, std::string(structureMessage<DerivedPointsConfig>(config, { { "pointVisibility", "点位显示设置必须是标识到布尔值的映射" } }, "派生点字段无效")), 400);
        }
        validateRules(*parsed);
        const auto& points = parsed->get<"derivedPoints">();
        if (points) {
            std::set<std::string, std::less<>> ids;
            for (const auto& point : *points) {
                validateDerivedPoint(point, ids);
            }
        }
        try {
            (void)service::common::orderDerivedPoints(config);
        } catch (const service::common::DerivedPointError& error) {
            switch (error.code()) {
                case service::common::DerivedPointErrorCode::malformed:
                    service::common::fail(16004, "派生点字段无效", 400);
                case service::common::DerivedPointErrorCode::duplicateId:
                    service::common::fail(16004, "派生点标识无效或重复", 400);
                case service::common::DerivedPointErrorCode::physicalIdConflict:
                    service::common::fail(16004, "派生点标识不能与采集点重复", 400);
                case service::common::DerivedPointErrorCode::unknownInput:
                    service::common::fail(16004, "派生点引用的采集点不存在或不是上行点", 400);
                case service::common::DerivedPointErrorCode::cycle:
                    service::common::fail(16004, "派生点存在循环依赖", 400);
            }
            throw;
        }
        if (const auto& visibility = parsed->get<"pointVisibility">(); visibility && !pointVisibilityValid(*visibility)) {
            service::common::fail(16004, "点位显示设置必须是标识到布尔值的映射", 400);
        }
        return points && !points->empty();
    }

  private:
    struct ConfigField {
        std::string_view name;
        std::string_view message;
    };

    // 顶层配置按字段读取，重复键以最后一次出现为准（与 PostgreSQL jsonb 一致）；Ruvia Model 把重复键
    // 视为结构错误，故解析失败且存在重复键时按最后值去重后再解析一次。嵌套对象的重复键仍是结构错误。
    template <typename Model>
    static std::optional<Model> parseLastValue(const ruvia::json_value& config) {
        auto parsed = ruvia::from_json<Model>(config.view());
        if (parsed) {
            return parsed;
        }
        std::map<std::string, std::string, std::less<>> fields;
        bool duplicate = false;
        (void)config.for_each_field([&](std::string_view key, const ruvia::json_value& value) {
            const auto [field, inserted] = fields.try_emplace(std::string(key), value.view());
            if (!inserted) {
                duplicate = true;
                field->second = value.view();
            }
            return true;
        });
        if (!duplicate) {
            return parsed;
        }
        std::string unique = "{";
        for (const auto& [key, value] : fields) {
            if (unique.size() > 1) {
                unique += ',';
            }
            unique += service::utils::jsonQuoted(key) + ':' + value;
        }
        unique += '}';
        return ruvia::from_json<Model>(unique);
    }

    // Ruvia 对结构错误（类型不符、缺少必填）只报告通用信息；按 fields 顺序单独解析各字段，
    // 找到首个结构出错的字段并返回其原文案，均未定位时返回 fallback。
    template <typename Model>
    static std::string_view structureMessage(const ruvia::json_value& config, std::initializer_list<ConfigField> fields, std::string_view fallback) {
        for (const auto& [name, message] : fields) {
            const auto value = config.get<ruvia::json_value>(name);
            if (value && !ruvia::from_json<Model>("{" + service::utils::jsonQuoted(name) + ":" + std::string(value->view()) + "}")) {
                return message;
            }
        }
        return fallback;
    }

    // 执行 Model 声明的字段约束，首个问题的文案即业务错误文案。
    template <typename Model>
    static void validateRules(const Model& model) {
        ruvia::validator validation;
        ruvia::json_body<Model>{}.validate(model, validation);
        if (!validation.ok()) {
            service::common::fail(16004, std::string(validation.issues().front().message()), 400);
        }
    }

    // lastValue 为 false 时保持 Ruvia 的整体解析语义（MQTT 配置历史上即按整体 Model 解析）。
    template <typename Model>
    static Model parseConfig(const ruvia::json_value& config, std::initializer_list<ConfigField> fields, std::string_view fallback, bool lastValue = true) {
        auto parsed = lastValue ? parseLastValue<Model>(config) : ruvia::from_json<Model>(config.view());
        if (!parsed) {
            service::common::fail(16004, std::string(structureMessage<Model>(config, fields, fallback)), 400);
        }
        validateRules(*parsed);
        return std::move(*parsed);
    }

    [[noreturn]] static void fail(std::string_view message) {
        service::common::fail(16004, std::string(message), 400);
    }

    template <typename Message>
        requires requires(const Message& message) { message.view(); }
    [[noreturn]] static void fail(const Message& message) {
        fail(message.view());
    }

    // json_value 数字字段：缺失时通过，出现时须满足 rule。
    static bool numberValid(const std::optional<ruvia::json_value>& value, const auto& rule) {
        return !value || rule(*value);
    }

    static void validateProtocol(std::string_view protocol) {
        if (protocol != "SL651" && protocol != "Modbus" && protocol != "S7" &&
            protocol != "MC" && protocol != "FINS" && protocol != "DLT645" && protocol != "MQTT") {
            service::common::fail(16003, "不支持的协议类型", 400);
        }
    }

    static void validateName(std::string_view name) {
        if (name.empty() || name.size() > 64) {
            service::common::fail(16002, "配置名称长度必须在 1 - 64 之间", 400);
        }
    }

    static void validateMqtt(const ruvia::json_value& config) {
        if (config.get<ruvia::json_value>("reports")) {
            std::vector<std::string> reports;
            try {
                reports = service::common::mqttReportConfigurations(config);
            } catch (const std::invalid_argument& error) {
                service::common::fail(16004, error.what(), 400);
            }
            for (const auto& wire : reports) {
                const auto single = ruvia::json_value::parse(std::string_view(wire));
                if (!single) {
                    service::common::fail(16004, "config 不能为空", 400);
                }
                validateConfig("MQTT", *single, true);
            }
            return;
        }
        const auto typed = parseConfig<MqttConfig>(config, {}, "MQTT 配置字段类型无效", false);
        const auto& topic = typed.get<"topic">();
        if (!topic) {
            service::common::fail(16004, "MQTT Topic 过滤器无效", 400);
        }
        const auto& commandTopic = typed.get<"commandTopic">();
        const auto& commands = typed.get<"commands">();
        if (commands && ((commandTopic && !commandTopic->view().empty()) || (typed.get<"commandTemplate">() && !typed.get<"commandTemplate">()->view().empty()))) {
            service::common::fail(16004, "控制消息最多 32 条，且不能与旧版单条控制配置同时使用", 400);
        }
        const auto& codeField = typed.get<"deviceCodeField">();
        const auto& recordsPath = typed.get<"recordsPath">();
        const auto format = typed.get<"payloadFormat">() ? typed.get<"payloadFormat">()->view() : std::string_view("json");
        const auto identity = typed.get<"identitySource">() ? typed.get<"identitySource">()->view() : (codeField && !codeField->view().empty() ? std::string_view("payload") : std::string_view("bound"));
        if (identity == "payload" && (!codeField || codeField->view().empty())) {
            service::common::fail(16004, "负载识别需要设备标识字段", 400);
        }
        if (identity == "bound" && topic->view().find_first_of("+#") != std::string_view::npos) {
            service::common::fail(16004, "通配 Topic 需要从 Topic 或负载识别设备", 400);
        }
        const auto recordLength = typed.get<"recordLength">().value_or(ruvia::int64{ 0 }).value_;
        const auto validateSelector = [&](std::string_view selector) {
            if (selector.size() > 256) {
                throw std::invalid_argument("字段路径过长");
            }
            if (format == "json") {
                (void)service::utils::jsonFieldSegments(selector);
            } else if (format == "text") {
                if (service::utils::payloadIndex(selector) > 4096) {
                    throw std::invalid_argument("文本列号超出范围");
                }
            } else {
                const service::utils::BinaryPayloadField field(selector);
                if (recordLength && field.offset + field.length > static_cast<std::size_t>(recordLength)) {
                    throw std::invalid_argument("二进制字段超出记录长度");
                }
            }
        };
        try {
            if (identity == "payload") {
                validateSelector(codeField->view());
            }
            if (recordsPath && !recordsPath->view().empty()) {
                if (format != "json") {
                    throw std::invalid_argument("记录路径仅用于 JSON");
                }
                (void)service::utils::jsonFieldSegments(recordsPath->view());
            }
            if (const auto& timeField = typed.get<"timeField">(); timeField && !timeField->view().empty()) {
                validateSelector(timeField->view());
            }
        } catch (const std::invalid_argument& error) {
            service::common::fail(16004, error.what(), 400);
        }
        const auto& points = typed.get<"points">();
        if (!points) {
            service::common::fail(16004, "MQTT 点位配置无效", 400);
        }
        std::set<std::string> ids, fields;
        std::map<std::string, bool, std::less<>> templatePoints;
        for (const auto& point : *points) {
            const auto name = point.get<"name">().view(), field = point.get<"field">().view();
            if (!ids.emplace(point.get<"id">().view()).second ||
                (field.empty() && (format != "json" || !typed.get<"reportTemplate">())) ||
                (!field.empty() && (!fields.emplace(field).second || (codeField && codeField->view() == field)))) {
                fail(kMqttPointInvalid);
            }
            if (!templatePoints.emplace(std::string(name), point.get<"writable">().value_or(ruvia::bool_value{ false }).value_).second) {
                service::common::fail(16004, "MQTT 点位名称重复", 400);
            }
            try {
                if (!field.empty()) {
                    validateSelector(field);
                }
            } catch (const std::invalid_argument& error) {
                service::common::fail(16004, error.what(), 400);
            }
            if (const auto& enums = point.get<"enumValues">(); enums) {
                std::set<std::string> inputs, outputs;
                for (const auto& value : *enums) {
                    const auto input = value.get<"input">().view(), output = value.get<"output">().view();
                    const auto converted = service::utils::convertPayloadValue(service::utils::PayloadScalar{ std::string(output) }, point.get<"dataType">().view());
                    if (!inputs.emplace(input).second || !converted || !outputs.emplace(service::utils::payloadScalarJson(*converted)).second) {
                        fail(kMqttEnumInvalid);
                    }
                }
            }
            if (point.get<"writable">().value_or(ruvia::bool_value{ false }).value_ &&
                !commands && (!commandTopic || commandTopic->view().empty())) {
                service::common::fail(16004, "可写 MQTT 点位需要指令 Topic", 400);
            }
        }
        try {
            const auto validateTemplate = [&](std::string_view input, bool report = false, bool legacyValues = true) {
                service::utils::JsonValueTemplate::Tokens allowed{ "$deviceCode", report ? "$time" : "$values" }, sparse;
                if (!report) {
                    allowed.insert("$deviceId");
                    allowed.insert("$commandId");
                }
                if (!report && !legacyValues) {
                    allowed.erase("$values");
                }
                for (const auto& [name, writable] : templatePoints) {
                    const auto token = "$point:" + name;
                    allowed.insert(token);
                    sparse.insert(token);
                }
                const service::utils::JsonValueTemplate parsed(input, allowed, sparse, report, 16384, 32, report ? utils::JsonValueTemplate::Tokens{} : utils::JsonValueTemplate::Tokens{ "$param:" });
                for (const auto& [name, writable] : templatePoints) {
                    if (!parsed.references("$point:" + name)) {
                        continue;
                    }
                    if (!report && !writable) {
                        throw std::invalid_argument("指令模板引用不可写点位: " + name);
                    }
                }
                return parsed;
            };
            if (commands) {
                if (format != "json") {
                    throw std::invalid_argument("多条控制消息仅用于 JSON");
                }
                std::set<std::string, std::less<>> messageIds, messageNames, writableIds;
                for (const auto& message : *commands) {
                    if (!messageIds.emplace(message.get<"id">().view()).second || !messageNames.emplace(message.get<"name">().view()).second) {
                        fail(kMqttCommandInvalid);
                    }
                    const auto parsed = validateTemplate(message.get<"templateText">().view(), false, false);
                    std::set<std::string, std::less<>> referenced, topicNames, requiredIds;
                    for (const auto level : utils::splitPayload(message.get<"topic">().view(), "/")) {
                        if (level == "{deviceCode}") {
                            continue;
                        }
                        if (level.starts_with("{point:") && level.ends_with('}')) {
                            const auto pointName = level.substr(7, level.size() - 8);
                            const auto point = templatePoints.find(pointName);
                            if (point == templatePoints.end() || !point->second) {
                                throw std::invalid_argument("控制 Topic 引用了未知或不可写属性");
                            }
                            topicNames.emplace(pointName);
                        } else if (level.find_first_of("{}") != std::string_view::npos) {
                            throw std::invalid_argument("控制 Topic 占位符须独占一层");
                        }
                    }
                    for (const auto& point : *points) {
                        if (parsed.references("$point:" + std::string(point.get<"name">().view())) || topicNames.contains(point.get<"name">().view())) {
                            referenced.emplace(point.get<"id">().view());
                        }
                    }
                    if (referenced.empty()) {
                        throw std::invalid_argument("控制消息至少需要引用一个属性");
                    }
                    if (const auto& response = message.get<"response">(); response) {
                        if (!parsed.references("$commandId")) {
                            throw std::invalid_argument("设备回执需要固定设备 Topic 及 $commandId");
                        }
                        std::set<std::string> selectors;
                        bool matchesId = false;
                        const utils::PayloadRecord source(message.get<"templateText">().view(), "json", ",");
                        for (const auto& field : response->get<"matchFields">()) {
                            (void)utils::jsonFieldSegments(field.view());
                            if (field.view().empty() || field.view().size() > 256 || !selectors.emplace(field.view()).second) {
                                throw std::invalid_argument("回执关联字段无效或重复");
                            }
                            const auto value = source.field(field.view());
                            if (!value) {
                                throw std::invalid_argument("回执关联字段须出现在指令模板中");
                            }
                            if (const auto* text = std::get_if<std::string>(&*value); text && *text == "$commandId") {
                                matchesId = true;
                            }
                        }
                        if (!matchesId) {
                            throw std::invalid_argument("回执关联字段须包含 $commandId 所在路径");
                        }
                        (void)utils::jsonFieldSegments(response->get<"codeField">().view());
                    }
                    utils::JsonValueTemplate::Values requiredValues{ { "$deviceCode", "\"device\"" }, { "$deviceId", "\"device-id\"" }, { "$commandId", "1" } };
                    for (const auto& token : parsed.references()) {
                        if (token.starts_with("$param:")) {
                            requiredValues.emplace(token, "null");
                        }
                    }
                    for (const auto& id : message.get<"requiredPointIds">()) {
                        if (!referenced.contains(id.view()) || !requiredIds.emplace(id.view()).second) {
                            throw std::invalid_argument("必填控制点必须属于当前消息且不能重复");
                        }
                        for (const auto& point : *points) {
                            if (point.get<"id">().view() == id.view()) {
                                requiredValues.emplace("$point:" + std::string(point.get<"name">().view()), "null");
                            }
                        }
                    }
                    for (const auto& point : *points) {
                        if (topicNames.contains(point.get<"name">().view()) && !requiredIds.contains(point.get<"id">().view())) {
                            throw std::invalid_argument("控制 Topic 中的属性必须设为必填");
                        }
                    }
                    // 数组不能省略下标；对象字段可以省略，且不补造值。
                    (void)parsed.render(requiredValues);
                    writableIds.insert(referenced.begin(), referenced.end());
                }
                for (const auto& point : *points) {
                    if (point.get<"writable">().value_or(ruvia::bool_value{ false }).value_ != writableIds.contains(point.get<"id">().view())) {
                        throw std::invalid_argument("属性可写状态与控制消息引用不一致");
                    }
                }
            }
            if (const auto& value = typed.get<"commandTemplate">(); value && !value->view().empty()) {
                if (format != "json") {
                    throw std::invalid_argument("指令模板仅用于 JSON");
                }
                validateTemplate(value->view());
            }
            if (const auto& value = typed.get<"reportTemplate">(); value) {
                if (format != "json") {
                    throw std::invalid_argument("上报模板仅用于 JSON");
                }
                validateTemplate(value->view(), true);
            }
        } catch (const std::invalid_argument& error) {
            service::common::fail(16004, error.what(), 400);
        }
    }

    static void validateSl651(const ruvia::json_value& config, bool complete) {
        const auto typed = parseConfig<Sl651Config>(config, {}, kSl651Invalid.view());
        const auto& funcs = typed.get<"funcs">();
        if (complete && (!typed.get<"responseMode">() || !funcs)) {
            fail(kSl651Invalid);
        }
        if (!funcs) {
            return;
        }
        for (const auto& function : *funcs) {
            if (!function.get<"funcCode">()) {
                fail(kSl651DefinitionInvalid);
            }
            for (const auto* elements : { &function.get<"elements">(), &function.get<"responseElements">() }) {
                if (!*elements) {
                    continue;
                }
                for (const auto& element : **elements) {
                    if (!sl651ElementValid(element)) {
                        fail(kSl651DefinitionInvalid);
                    }
                }
            }
        }
    }

    // 要素的跨字段规则：OFFSET 按偏移与长度定位；GUIDE 的引导符须与长度、小数位定义一致。
    static bool sl651ElementValid(const Sl651Element& element) {
        const auto& lengthValue = element.get<"length">();
        const auto& digitsValue = element.get<"digits">();
        const auto& encoding = element.get<"encode">();
        const auto length = lengthValue ? kSl651Length.value(*lengthValue) : std::nullopt;
        const auto digits = digitsValue ? kSl651Digits.value(*digitsValue) : std::nullopt;
        if (!encoding || !length || !digits) {
            return false;
        }
        if (element.get<"positionMode">() && element.get<"positionMode">()->view() == "OFFSET") {
            const auto offset = element.get<"byteOffset">() ? kSl651ByteOffset.value(*element.get<"byteOffset">()) : std::optional<std::int64_t>{ 0 };
            return offset && *length >= 1 && *offset + *length <= 8388608;
        }
        const auto& guide = element.get<"guideHex">();
        if (!guide || !(HexText{ .minimum = 4, .maximum = 4 }(*guide) || HexText{ .minimum = 6, .maximum = 6 }(*guide))) {
            return false;
        }
        const auto hexByte = [](std::string_view text) {
            unsigned value = 0;
            (void)std::from_chars(text.data(), text.data() + text.size(), value, 16);
            return value;
        };
        const auto guideText = guide->view();
        const auto lead = hexByte(guideText.substr(0, 2));
        if ((lead == 0xFF) != (guideText.size() == 6)) {
            return false;
        }
        const auto definition = hexByte(guideText.substr(guideText.size() - 2));
        if (lead < 0xF0 || lead == 0xFF) {
            return *length > 0 && *length == (definition >> 3) && (encoding->view() != "BCD" || *digits == (definition & 7));
        }
        if (definition != lead) {
            return false;
        }
        if (lead == 0xF0) {
            return *length == 5;
        }
        if (lead == 0xF1) {
            return *length == 6;
        }
        return lead == 0xF2 || lead == 0xF3 || *length > 0;
    }

    // MC/FINS/DLT645 的连接、点位与采集周期在创建和 PATCH 中都必须完整且有效。
    template <typename Config>
    static Config parseIndustrial(const ruvia::json_value& config) {
        constexpr std::string_view invalid = "协议连接、点位或采集周期配置无效";
        auto typed = parseConfig<Config>(config, {}, invalid);
        if (!typed.template get<"connection">() || !typed.template get<"points">() ||
            !numberValid(typed.template get<"readInterval">(), kReadInterval) ||
            !numberValid(typed.template get<"commandFastReadDuration">(), kCommandFastReadDuration) ||
            !numberValid(typed.template get<"commandFastReadInterval">(), kCommandFastReadInterval)) {
            fail(invalid);
        }
        return typed;
    }

    template <typename Point>
    static void requireIndustrialPoint(const Point& point, std::set<std::string, std::less<>>& ids) {
        const auto& id = point.template get<"id">();
        if (!id || !ids.emplace(id->view()).second || !point.template get<"name">() || !point.template get<"dataType">()) {
            fail(kIndustrialPointInvalid);
        }
    }

    static std::int64_t industrialWidth(std::string_view type) {
        if (type == "BOOL" || type == "INT16" || type == "UINT16") {
            return 1;
        }
        return type == "INT64" || type == "UINT64" || type == "DOUBLE" ? 4 : 2;
    }

    static void validateMc(const ruvia::json_value& config) {
        const auto typed = parseIndustrial<McConfig>(config);
        const auto& connection = *typed.get<"connection">();
        if (!numberValid(connection.get<"network">(), kMcByte) || !numberValid(connection.get<"station">(), kMcByte) ||
            !numberValid(connection.get<"moduleIo">(), kMcModuleIo) || !numberValid(connection.get<"multidrop">(), kMcByte) ||
            !numberValid(connection.get<"monitoringTimer">(), kMcMonitoringTimer)) {
            fail(kMcConnectionInvalid);
        }
        std::set<std::string, std::less<>> ids;
        for (const auto& point : *typed.get<"points">()) {
            requireIndustrialPoint(point, ids);
            const auto& area = point.get<"area">();
            const auto address = point.get<"address">() ? kMcAddress.value(*point.get<"address">()) : std::nullopt;
            if (!area || !numberValid(point.get<"decimals">(), kPointDecimals) || !address) {
                fail(kIndustrialPointInvalid);
            }
            const auto type = point.get<"dataType">()->view();
            const bool bits = type == "BOOL";
            const auto areaName = area->view();
            const bool bitArea = areaName == "M" || areaName == "X" || areaName == "Y" || areaName == "B" || areaName == "L" ||
                areaName == "F" || areaName == "V" || areaName == "S" || areaName == "TS" || areaName == "CS";
            if ((bits && !bitArea) || *address + industrialWidth(type) * (bitArea && !bits ? 16 : 1) - 1 > kMcAddress.maximum) {
                fail(kIndustrialPointInvalid);
            }
        }
    }

    static void validateFins(const ruvia::json_value& config) {
        const auto typed = parseIndustrial<FinsConfig>(config);
        const auto& connection = *typed.get<"connection">();
        if (!numberValid(connection.get<"destinationNetwork">(), kFinsNetwork) || !numberValid(connection.get<"sourceNetwork">(), kFinsNetwork) ||
            !numberValid(connection.get<"destinationNode">(), kFinsNode) || !numberValid(connection.get<"sourceNode">(), kFinsNode) ||
            !numberValid(connection.get<"destinationUnit">(), kFinsUnit) || !numberValid(connection.get<"sourceUnit">(), kFinsUnit)) {
            fail("FINS 路由参数无效");
        }
        std::set<std::string, std::less<>> ids;
        for (const auto& point : *typed.get<"points">()) {
            requireIndustrialPoint(point, ids);
            const auto address = point.get<"address">() ? kFinsAddress.value(*point.get<"address">()) : std::nullopt;
            const auto bit = point.get<"bit">() ? kFinsBit.value(*point.get<"bit">()) : std::nullopt;
            if (!point.get<"area">() || !numberValid(point.get<"decimals">(), kPointDecimals) || !address || (point.get<"bit">() && !bit)) {
                fail(kIndustrialPointInvalid);
            }
            const auto type = point.get<"dataType">()->view();
            const bool bits = type == "BOOL";
            if ((!bits && bit && *bit != 0) || *address + (bits ? 0 : industrialWidth(type) - 1) > kFinsAddress.maximum) {
                fail(kIndustrialPointInvalid);
            }
        }
    }

    static void validateDlt645(const ruvia::json_value& config) {
        const auto typed = parseIndustrial<Dlt645Config>(config);
        const auto& connection = *typed.get<"connection">();
        if (!numberValid(connection.get<"wakeupBytes">(), kDlt645WakeupBytes)) {
            fail(kDlt645ConnectionInvalid);
        }
        const auto text = [](const std::optional<ruvia::string>& value, std::string_view fallback = {}) {
            return value ? value->view() : fallback;
        };
        const auto version = text(connection.get<"version">(), "2007");
        std::set<std::string, std::less<>> ids;
        for (const auto& point : *typed.get<"points">()) {
            requireIndustrialPoint(point, ids);
            const auto& identifier = point.get<"identifier">();
            const auto length = point.get<"length">() ? kDlt645Length.value(*point.get<"length">()) : std::nullopt;
            if (!identifier || identifier->view().size() != (version == "1997" ? 4u : 8u) || !length ||
                (point.get<"dataType">()->view() != "HEX" && *length > 8) || !numberValid(point.get<"digits">(), kDlt645Digits)) {
                fail(kIndustrialPointInvalid);
            }
            if (point.get<"writable">() && static_cast<bool>(*point.get<"writable">()) &&
                (text(connection.get<"writePassword">()).size() != 8 ||
                 (version == "2007" && text(connection.get<"operatorCode">()).size() != 8) ||
                 *length > (version == "2007" ? 38 : 44))) {
                fail(kIndustrialPointInvalid);
            }
        }
    }

    static void validateS7(const ruvia::json_value& config, bool complete) {
        constexpr std::string_view pollInterval = "S7 配置不允许 pollInterval，请使用 readInterval";
        constexpr std::string_view readInterval = "S7 配置的 readInterval 无效";
        const auto typed = parseConfig<S7Config>(config, { { "pollInterval", pollInterval }, { "readInterval", readInterval } }, kS7Invalid.view());
        const auto& connection = typed.get<"connection">();
        const auto& areas = typed.get<"areas">();
        if ((complete && (!typed.get<"plcModel">() || !connection || !areas)) ||
            (connection && (!numberValid(connection->get<"handshakeTimeout">(), kS7ProbeTimeout) || !numberValid(connection->get<"directProbeTimeout">(), kS7ProbeTimeout)))) {
            fail(kS7Invalid);
        }
        if (typed.is_present<"pollInterval">()) {
            fail(pollInterval);
        }
        if (!numberValid(typed.get<"readInterval">(), kReadInterval)) {
            fail(readInterval);
        }
        if (!areas) {
            return;
        }
        for (const auto& area : *areas) {
            const auto& start = area.get<"start">();
            const auto& size = area.get<"size">();
            if (!area.get<"id">() || !area.get<"name">() || !area.get<"area">() || !start || !kS7Start(*start) || !size || !kS7Size(*size) ||
                !numberValid(area.get<"startBit">(), kS7StartBit) || !numberValid(area.get<"decimals">(), kPointDecimals)) {
                fail(kS7AreaInvalid);
            }
            if (area.get<"area">()->view() == "DB" && (!area.get<"dbNumber">() || !kS7DbNumber(*area.get<"dbNumber">()))) {
                fail(kS7AreaInvalid);
            }
        }
    }

    static void validateModbus(const ruvia::json_value& config, bool complete) {
        constexpr std::string_view registersInvalid = "Modbus 配置的 registers 必须是数组";
        constexpr std::string_view readInterval = "Modbus 配置的 readInterval 无效";
        constexpr std::string_view pollInterval = "Modbus 配置不允许 pollInterval，请使用 readInterval";
        const auto typed = parseConfig<ModbusConfig>(config, { { "byteOrder", kModbusByteOrderInvalid.view() }, { "registers", registersInvalid }, { "packet", "Modbus 配置的 packet 必须是对象" }, { "readInterval", readInterval }, { "pollInterval", pollInterval } }, registersInvalid);
        if (complete && !typed.get<"byteOrder">()) {
            fail(kModbusByteOrderInvalid);
        }
        const auto& registers = typed.get<"registers">();
        if (complete && !registers) {
            fail(registersInvalid);
        }
        if (!numberValid(typed.get<"readInterval">(), kReadInterval)) {
            fail(readInterval);
        }
        if (const auto& packet = typed.get<"packet">()) {
            if (!numberValid(packet->get<"mergeGap">(), kModbusMergeGap)) {
                fail("Modbus 配置的 packet.mergeGap 无效");
            }
            if (!numberValid(packet->get<"maxQuantity">(), kModbusMaxQuantity)) {
                fail("Modbus 配置的 packet.maxQuantity 无效");
            }
        }
        if (typed.is_present<"pollInterval">()) {
            fail(pollInterval);
        }
        if (!registers) {
            return;
        }
        for (const auto& entry : *registers) {
            const auto& address = entry.get<"address">();
            const auto& quantity = entry.get<"quantity">();
            if (!entry.get<"id">() || !entry.get<"name">() || !entry.get<"registerType">() || !entry.get<"dataType">() ||
                !address || !kModbusAddress(*address) || !quantity || !kModbusQuantity(*quantity)) {
                fail(kModbusRegisterInvalid);
            }
            if (!numberValid(entry.get<"decimals">(), kModbusDecimals)) {
                fail("Modbus 寄存器 decimals 无效");
            }
        }
    }

    // 派生点的必填（需原文案）、按计算类型的条件字段、变量绑定与公式编译。
    static void validateDerivedPoint(const DerivedPointDefinition& point, std::set<std::string, std::less<>>& ids) {
        const auto& id = point.get<"id">();
        if (!id) {
            fail(kDerivedIdInvalid);
        }
        if (!ids.emplace(id->view()).second) {
            service::common::fail(16004, "派生点标识无效或重复", 400);
        }
        if (!point.get<"name">()) {
            service::common::fail(16004, "派生点字段无效: name", 400);
        }
        const auto& kind = point.get<"kind">();
        if (!kind) {
            service::common::fail(16004, "派生点字段无效: kind", 400);
        }
        const auto& valueType = point.get<"valueType">();
        if (!valueType) {
            service::common::fail(16004, "派生点字段无效: valueType", 400);
        }
        const bool formula = kind->view() == "expression";
        const auto& expression = point.get<"expression">();
        if (formula && (!expression || expression->view().empty())) {
            service::common::fail(16004, "派生点字段无效: expression", 400);
        }
        if (!formula && (point.get<"windowSeconds">().value_or(ruvia::int64{ 0 }).value_ == 0 || valueType->view() != "number")) {
            service::common::fail(16004, "窗口派生点需要时间窗口和数值结果类型", 400);
        }
        const auto& inputs = point.get<"inputs">();
        if (!inputs) {
            service::common::fail(16004, "派生点必须绑定输入点", 400);
        }
        std::set<std::string, std::less<>> names;
        try {
            for (const auto& input : *inputs) {
                const auto& alias = input.get<"alias">();
                if (!alias) {
                    service::common::fail(16004, "派生点字段无效: alias", 400);
                }
                if (!input.get<"pointId">()) {
                    service::common::fail(16004, "派生点字段无效: pointId", 400);
                }
                const std::string name(alias->view());
                const utils::Expression variable(name);
                if (variable.variables() != std::vector<std::string>{ name } || !names.insert(name).second) {
                    service::common::fail(16004, "派生点输入绑定无效", 400);
                }
            }
            if (formula) {
                const utils::Expression compiled(expression->view());
                for (const auto& name : compiled.variables()) {
                    if (!names.contains(name)) {
                        service::common::fail(16004, "公式包含未绑定变量: " + name, 400);
                    }
                }
            } else {
                const auto& source = point.get<"sourceAlias">();
                if (!source || source->view().empty() || source->view().size() > 32) {
                    service::common::fail(16004, "派生点字段无效: sourceAlias", 400);
                }
                if (!names.contains(source->view())) {
                    service::common::fail(16004, "窗口计算的输入点未绑定", 400);
                }
            }
            if (const auto& rules = point.get<"unitRules">()) {
                for (const auto& rule : *rules) {
                    const auto& condition = rule.get<"condition">();
                    if (!condition) {
                        service::common::fail(16004, "派生点字段无效: condition", 400);
                    }
                    const utils::Expression compiled(condition->view());
                    for (const auto& name : compiled.variables()) {
                        if (!names.contains(name)) {
                            service::common::fail(16004, "单位条件包含未绑定变量: " + name, 400);
                        }
                    }
                }
            }
        } catch (const std::invalid_argument& error) {
            service::common::fail(16004, error.what(), 400);
        }
        const bool conditional = point.get<"unitMode">() && point.get<"unitMode">()->view() == "conditional";
        const bool ruled = point.get<"unitRules">() && !point.get<"unitRules">()->empty();
        if (conditional != ruled) {
            service::common::fail(16004, "条件单位必须设置规则，固定单位不得保留条件", 400);
        }
    }

  public:
    static ExpressionTestResult testExpression(const ExpressionTestBody& body) {
        try {
            std::map<std::string, double> inputs;
            for (const auto& input : body.get<"inputs">()) {
                const std::string alias(input.get<"alias">().view());
                const double value = input.get<"value">().value_;
                const utils::Expression variable(alias);
                if (variable.variables().size() != 1 || variable.variables().front() != alias ||
                    !std::isfinite(value) || !inputs.emplace(alias, value).second) {
                    throw std::invalid_argument("变量名无效、重复或测试值不是有限数值");
                }
            }
            const auto compile = [&](std::string_view text) {
                utils::Expression expression(text);
                for (const auto& alias : expression.variables()) {
                    if (!inputs.contains(alias)) {
                        throw std::invalid_argument("未提供变量测试值：" + alias);
                    }
                }
                return expression;
            };
            const auto expression = compile(body.get<"expression">().view());
            std::vector<utils::Expression> conditions;
            for (const auto& rule : body.get<"unitRules">()) {
                conditions.push_back(compile(rule.get<"condition">().view()));
            }
            const auto resolve = [&](std::string_view alias) {
                return inputs.at(std::string(alias));
            };
            const auto value = expression.evaluate(resolve);
            std::string unit(body.get<"unit">().view());
            std::int64_t matched{};
            for (std::size_t i = 0; i < conditions.size(); ++i) {
                if (conditions[i].evaluate(resolve) != 0) {
                    unit = body.get<"unitRules">()[i].get<"unit">().view();
                    matched = static_cast<std::int64_t>(i + 1);
                    break;
                }
            }
            ExpressionTestResult result;
            result.set<"value">(value).set<"unit">(unit).set<"matchedRule">(matched);
            return result;
        } catch (const std::invalid_argument& error) {
            common::fail(16004, error.what(), 400);
        } catch (const std::domain_error& error) {
            common::fail(16004, error.what(), 400);
        }
    }

    static void validateExpression(const ExpressionValidateBody& body) {
        try {
            const utils::Expression expression(body.get<"expression">().view());
            const auto& variables = body.get<"variables">();
            for (const auto& alias : expression.variables()) {
                if (std::none_of(variables.begin(), variables.end(), [&](const auto& name) {
                        return name.view() == alias;
                    })) {
                    throw std::invalid_argument("未提供变量测试值：" + alias);
                }
            }
        } catch (const std::invalid_argument& error) {
            common::fail(16004, error.what(), 400);
        } catch (const std::domain_error& error) {
            common::fail(16004, error.what(), 400);
        }
    }

    static MqttPreviewResult previewMqtt(const MqttPreviewBody& body) {
        try {
            const auto& rawConfig = body.get<"config">();
            validateConfig("MQTT", rawConfig, true);
            (void)validateDerivedPoints(rawConfig);
            const auto reports = service::common::mqttReportConfigurations(rawConfig);
            const auto requestedTopic = body.get<"topic">().view();
            const auto& requestedCode = body.get<"deviceCode">();
            const auto selected = std::find_if(reports.begin(), reports.end(), [&](const std::string& wire) {
                const auto report = ruvia::json_value::parse(std::string_view(wire));
                const auto topic = report ? report->get<ruvia::string>("topic") : std::optional<ruvia::string>{};
                return topic && service::common::mqttReportTopicMatches(std::string(topic->view()), requestedTopic, requestedCode ? requestedCode->view() : std::string_view{});
            });
            if (selected == reports.end()) {
                throw std::invalid_argument("示例 Topic 未匹配上报消息");
            }
            const auto decodedConfig = ruvia::from_json<MqttConfig>(*selected);
            if (!decodedConfig) {
                throw std::invalid_argument("MQTT 配置字段类型无效");
            }
            const auto& config = *decodedConfig;
            const auto topic = body.get<"topic">().view();
            if (topic.empty() || topic.find_first_of("+#") != std::string_view::npos || topic.find('\0') != std::string_view::npos) {
                throw std::invalid_argument("示例 Topic 须为不含通配符的具体 Topic");
            }
            const auto option = [](const auto& value, std::string_view fallback) {
                return value ? std::string(value->view()) : std::string(fallback);
            };
            const auto layout = service::common::mqttReportLayout(config);
            const auto deviceCode = option(body.get<"deviceCode">(), "");
            auto payload = std::string(body.get<"payload">().view());
            if (layout.format == "binary") {
                std::string hex;
                for (const unsigned char ch : payload) {
                    if (!std::isspace(ch)) {
                        hex += static_cast<char>(ch);
                    }
                }
                if (hex.size() % 2) {
                    throw std::invalid_argument("二进制示例须为完整的 HEX 字节");
                }
                payload.clear();
                for (std::size_t i = 0; i < hex.size(); i += 2) {
                    unsigned value = 0;
                    const auto parsed = std::from_chars(hex.data() + i, hex.data() + i + 2, value, 16);
                    if (parsed.ec != std::errc{} || parsed.ptr != hex.data() + i + 2) {
                        throw std::invalid_argument("二进制示例 HEX 无效");
                    }
                    payload += static_cast<char>(value);
                }
            }
            if (payload.size() > 1048576) {
                throw std::invalid_argument("示例负载超过 1 MiB");
            }
            const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            const auto records = service::common::decodeMqttReport(layout, topic, payload, deviceCode, option(body.get<"timezone">(), service::message::device::kDefaultTimezone), now);
            MqttPreviewResult result;
            ruvia::array<MqttPreviewRecord> output;
            for (const auto& record : records) {
                MqttPreviewRecord row;
                ruvia::array<ruvia::string> errors;
                ruvia::array<MqttPreviewPoint> points;
                if (record.deviceCode.empty()) {
                    errors.push_back(ruvia::string("未提取到设备编码"));
                } else if (!deviceCode.empty() && record.deviceCode != deviceCode) {
                    errors.push_back(ruvia::string("设备编码与测试设备不匹配，实际接入将跳过此记录"));
                }
                if (!record.timeValid) {
                    errors.push_back(ruvia::string("设备时间无效，实际接入将跳过此记录"));
                }
                for (std::size_t index = 0; index < layout.points.size(); ++index) {
                    const auto& point = layout.points[index];
                    const auto& value = record.values[index];
                    if (!value) {
                        errors.push_back(ruvia::string(point.name + "：字段缺失或类型转换失败"));
                        continue;
                    }
                    MqttPreviewPoint preview;
                    preview.set<"id">(point.id).set<"name">(point.name).set<"value">(utils::payloadScalarText(*value)).set<"unit">(point.unit);
                    points.push_back(std::move(preview));
                }
                row.set<"deviceCode">(record.deviceCode).set<"time">(common::utcTimestampFromMilliseconds(record.occurredAtMs)).set<"points">(std::move(points)).set<"errors">(std::move(errors));
                output.push_back(std::move(row));
            }
            result.set<"records">(std::move(output));
            return result;
        } catch (const std::invalid_argument& error) {
            common::fail(16004, error.what(), 400);
        }
    }

    static ProtocolService& instance() {
        static thread_local ProtocolService service;
        return service;
    }

    template <typename Context>
    ruvia::task<std::string> list(Context& c, std::int64_t page, std::int64_t pageSize, std::optional<std::string> protocol) {
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 1000);
        ruvia::db_find_options countOptions;
        countOptions.where_ = entities::ProtocolConfigEntity::column<"deleted_at">().is_null();
        if (protocol && !protocol->empty()) {
            countOptions.where_ = std::move(countOptions.where_) &&
                entities::ProtocolConfigEntity::column<"protocol">() == *protocol;
        }
        const auto total = co_await c.db().template get_repository<entities::ProtocolConfigEntity>().count(countOptions);

        auto query = protocolSelect(c.pool());
        if (protocol && !protocol->empty()) {
            query.and_where(query.binary(query.column(entities::ProtocolConfigEntity::column<"protocol">().name()), ruvia::db_binary_operator::equal, query.value(*protocol)));
        }
        query.order_by(query.column(entities::ProtocolConfigEntity::column<"id">().name()), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>((page - 1) * pageSize));
        const auto rows = co_await c.db().query(query);

        std::string result = "{\"list\":[";
        bool first = true;
        for (const auto& row : rows) {
            if (!first) {
                result.push_back(',');
            }
            first = false;
            result += itemJson(row);
        }
        result += "],\"total\":" + std::to_string(total) + ",\"page\":" + std::to_string(page) +
            ",\"pageSize\":" + std::to_string(pageSize) + ",\"totalPages\":" +
            std::to_string(total == 0 ? 0 : (total + pageSize - 1) / pageSize) + "}";
        co_return result;
    }

    template <typename Context>
    ruvia::task<std::string> detail(Context& c, std::string_view id) {
        co_return co_await detailData(c, id);
    }

    template <typename Context>
    ruvia::task<std::string> options(Context& c, const std::string& protocol, std::int64_t page, std::int64_t pageSize) {
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 1000);
        ruvia::db_find_options options;
        options.where_ = entities::ProtocolConfigEntity::column<"deleted_at">().is_null() &&
            entities::ProtocolConfigEntity::column<"protocol">() == protocol;
        const auto total = co_await c.db().template get_repository<entities::ProtocolConfigEntity>().count(options);
        options.where_ = std::move(options.where_) && entities::ProtocolConfigEntity::column<"enabled">() == true;
        options.order_ = { { std::string(entities::ProtocolConfigEntity::column<"name">().name()) } };
        options.take_ = static_cast<std::uint64_t>(pageSize);
        options.skip_ = static_cast<std::uint64_t>((page - 1) * pageSize);
        const auto rows = co_await c.db().template get_repository<entities::ProtocolConfigEntity>().find(options);
        std::string result = "{\"list\":[";
        bool first = true;
        for (const auto& row : rows) {
            if (!first) {
                result.push_back(',');
            }
            first = false;
            result += "{\"id\":" + service::utils::jsonQuoted(row.template get<"id">()) +
                ",\"name\":" + service::utils::jsonQuoted(row.template get<"name">());
            const auto config = ruvia::from_json<service::common::DeviceParameterConfiguration>(row.template get<"config">());
            result += ",\"parameters\":";
            if (config && config->template get<"parameters">()) {
                const auto json = ruvia::to_json(*config->template get<"parameters">());
                result.append(json.data(), json.size());
            } else {
                result += "[]";
            }
            result += "}";
        }
        result += "],\"total\":" + std::to_string(total) + ",\"page\":" + std::to_string(page) +
            ",\"pageSize\":" + std::to_string(pageSize) + "}";
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> create(Context& c, const CreateProtocolBody& body) {
        const std::string protocol(body.get<"protocol">().view());
        const std::string name(body.get<"name">().view());
        validateProtocol(protocol);
        validateName(name);
        if (body.is_null<"config">()) {
            service::common::fail(16004, "config 必须是对象", 400);
        }
        if (!body.get<"config">()) {
            service::common::fail(16004, "config 不能为空", 400);
        }
        validateConfig(protocol, *body.get<"config">(), true);
        (void)validateDerivedPoints(*body.get<"config">());
        co_await ensureNameAvailable(c, name, std::nullopt);
        const auto id = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        const auto& config = body.get<"config">();
        const auto& remark = body.get<"remark">();
        const bool enabled = body.get<"enabled">().value_or(ruvia::bool_value{ true }).value_;
        auto transaction = co_await c.db().begin_transaction();
        entities::ProtocolConfigEntity configuration(c.pool());
        configuration.set<"id">(id);
        configuration.set<"protocol">(protocol);
        configuration.set<"name">(name);
        configuration.set<"enabled">(enabled);
        configuration.set<"config">(config->view());
        configuration.set<"created_by">(c.userId);
        if (remark && !remark->view().empty()) {
            configuration.set<"remark">(remark->view());
        } else {
            configuration.set_null<"remark">();
        }
        (void)co_await transaction.template get_repository<entities::ProtocolConfigEntity>().insert(configuration);
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "protocol", "created", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> update(Context& c, std::string_view id, const UpdateProtocolBody& body) {
        ruvia::db_find_options options;
        options.where_ = service::common::database::activeId<entities::ProtocolConfigEntity>(id);
        const auto existing = co_await c.db().template get_repository<entities::ProtocolConfigEntity>().find_one(options);
        if (!existing) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        co_await requireOwner(c, existing->template get<"created_by">());
        const std::string protocol(existing->template get<"protocol">());
        if (const auto& requested = body.get<"protocol">()) {
            if (requested->view().empty()) {
                service::common::fail(16003, "protocol 不能为空", 400);
            }
            if (requested->view() != protocol) {
                service::common::fail(16006, "协议类型不可修改", 409);
            }
        }
        if (const auto& name = body.get<"name">()) {
            validateName(name->view());
            co_await ensureNameAvailable(c, std::string(name->view()), std::string(id));
        }
        auto transaction = co_await c.db().begin_transaction();
        options.lock_ = ruvia::db_lock_options{};
        const auto locked = co_await transaction.template get_repository<entities::ProtocolConfigEntity>().find_one(options);
        if (!locked) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        if (body.is_null<"config">()) {
            service::common::fail(16004, "config 必须是对象", 400);
        }
        if (body.get<"config">()) {
            validateConfig(protocol, *body.get<"config">(), false);
            const auto previous = ruvia::json_value::parse(locked->template get<"config">());
            std::map<std::string, std::string> fields;
            if (!previous) {
                throw std::runtime_error("invalid stored protocol configuration");
            }
            (void)previous->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
                fields[std::string(key)] = std::string(value.view());
                return true;
            });
            (void)body.get<"config">()->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
                fields[std::string(key)] = std::string(value.view());
                return true;
            });
            std::string merged = "{";
            for (const auto& [key, value] : fields) {
                if (merged.size() > 1) {
                    merged += ',';
                }
                merged += service::utils::jsonQuoted(key) + ':' + value;
            }
            merged += '}';
            const auto mergedConfig = ruvia::json_value::parse(merged);
            if (!mergedConfig) {
                throw std::runtime_error("invalid merged protocol configuration");
            }
            if (protocol == "MQTT") {
                validateConfig(protocol, *mergedConfig, true);
            }
            if (validateDerivedPoints(*mergedConfig)) {
                co_await requireDerivedSupport(transaction, c.pool(), id);
            }
        }
        const auto& name = body.get<"name">();
        const auto& remark = body.get<"remark">();
        const auto& config = body.get<"config">();
        const auto& enabled = body.get<"enabled">();
        ruvia::db_query expressions(c.pool());
        std::vector<ruvia::db_assignment> changes{ { std::string(entities::ProtocolConfigEntity::column<"updated_at">().name()), expressions.call("now") } };
        if (name) {
            changes.push_back({ std::string(entities::ProtocolConfigEntity::column<"name">().name()), expressions.value(name->view()) });
        }
        if (enabled) {
            changes.push_back({ std::string(entities::ProtocolConfigEntity::column<"enabled">().name()), expressions.value(enabled->value_) });
        }
        if (config) {
            const auto column = entities::ProtocolConfigEntity::column<"config">().name();
            changes.push_back({ std::string(column), expressions.binary(expressions.column(column), ruvia::db_binary_operator::json_concat, expressions.cast(expressions.value(config->view()), ruvia::db_data_type::jsonb)) });
        }
        if (body.is_present<"remark">()) {
            changes.push_back({ std::string(entities::ProtocolConfigEntity::column<"remark">().name()), remark && !remark->view().empty() ? expressions.value(remark->view()) : expressions.null_value() });
        }
        (void)co_await transaction.template get_repository<entities::ProtocolConfigEntity>().update(
            service::common::database::activeId<entities::ProtocolConfigEntity>(id),
            changes
        );
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "protocol", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        try {
            (void)co_await service::utils::callRpc(c, "telemetry", "project-protocol", std::string(id));
        } catch (...) {
            // Startup hydration repairs Redis read models if Redis is temporarily unavailable.
        }
        try {
            co_await syncEdgeNodes(c, id);
        } catch (const std::exception& error) {
            logPostUpdateFailure("edge-sync", id, error.what());
        } catch (...) {
            logPostUpdateFailure("edge-sync", id, "unknown exception");
        }
    }

    template <typename Context>
    ruvia::task<void> createDlt645Point(Context& c, std::string_view id, const CreateDlt645PointBody& body) {
        ruvia::db_find_options options;
        options.where_ = service::common::database::activeId<entities::ProtocolConfigEntity>(id);
        const auto existing = co_await c.db().template get_repository<entities::ProtocolConfigEntity>().find_one(options);
        if (!existing) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        co_await requireOwner(c, existing->template get<"created_by">());
        auto transaction = co_await c.db().begin_transaction();
        options.lock_ = ruvia::db_lock_options{};
        const auto configuration = co_await transaction.template get_repository<entities::ProtocolConfigEntity>().find_one(options);
        if (!configuration) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        if (configuration->template get<"protocol">() != "DLT645") {
            service::common::fail(16004, "此点位接口仅支持 DL/T645", 400);
        }
        const auto previous = ruvia::json_value::parse(configuration->template get<"config">());
        if (!previous) {
            throw std::runtime_error("invalid stored protocol configuration");
        }
        const auto points = previous->template get<ruvia::json_value>("points");
        if (!points || !points->is_array()) {
            throw std::runtime_error("invalid stored protocol points");
        }
        const auto pointId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        const auto requestJson = ruvia::to_json(body);
        const auto request = ruvia::json_value::parse(requestJson);
        if (!request) {
            throw std::runtime_error("invalid serialized point request");
        }
        std::string pointJson = "{\"id\":" + service::utils::jsonQuoted(pointId);
        (void)request->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
            if (key != "visible") {
                pointJson += ',' + service::utils::jsonQuoted(key) + ':' + std::string(value.view());
            }
            return true;
        });
        pointJson += '}';
        std::string pointArray = "[";
        (void)points->for_each_element([&](const ruvia::json_value& point) {
            if (pointArray.size() > 1) {
                pointArray += ',';
            }
            pointArray += point.view();
            return true;
        });
        if (pointArray.size() > 1) {
            pointArray += ',';
        }
        pointArray += pointJson + ']';
        std::map<std::string, std::string> fields;
        (void)previous->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
            fields.emplace(key, value.view());
            return true;
        });
        fields["points"] = std::move(pointArray);
        if (!static_cast<bool>(body.get<"visible">().value_or(ruvia::bool_value{ true }))) {
            std::string visibility = "{";
            if (const auto existingVisibility = previous->template get<ruvia::json_value>("pointVisibility")) {
                if (!existingVisibility->is_object()) {
                    throw std::runtime_error("invalid stored point visibility");
                }
                (void)existingVisibility->for_each_field([&](std::string_view key, const ruvia::json_value& value) {
                    if (visibility.size() > 1) {
                        visibility += ',';
                    }
                    visibility += service::utils::jsonQuoted(key) + ':' + std::string(value.view());
                    return true;
                });
            }
            if (visibility.size() > 1) {
                visibility += ',';
            }
            fields["pointVisibility"] = visibility + service::utils::jsonQuoted(pointId) + ":false}";
        }
        std::string merged = "{";
        for (const auto& [key, value] : fields) {
            if (merged.size() > 1) {
                merged += ',';
            }
            merged += service::utils::jsonQuoted(key) + ':' + value;
        }
        merged += '}';
        const auto parsed = ruvia::json_value::parse(merged);
        if (!parsed) {
            throw std::runtime_error("invalid merged protocol configuration");
        }
        validateConfig("DLT645", *parsed, true);
        (void)validateDerivedPoints(*parsed);
        ruvia::db_query expressions(c.pool());
        const ruvia::db_assignment configurationChanges[]{
            { std::string(entities::ProtocolConfigEntity::column<"config">().name()), expressions.cast(expressions.value(merged), ruvia::db_data_type::jsonb) },
            { std::string(entities::ProtocolConfigEntity::column<"updated_at">().name()), expressions.call("now") }
        };
        (void)co_await transaction.template get_repository<entities::ProtocolConfigEntity>().update(
            service::common::database::activeId<entities::ProtocolConfigEntity>(id),
            configurationChanges
        );
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "protocol", "updated", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
        try {
            (void)co_await service::utils::callRpc(c, "telemetry", "project-protocol", std::string(id));
        } catch (...) {
            // Startup hydration repairs Redis read models if Redis is temporarily unavailable.
        }
        try {
            co_await syncEdgeNodes(c, id);
        } catch (const std::exception& error) {
            logPostUpdateFailure("edge-sync", id, error.what());
        } catch (...) {
            logPostUpdateFailure("edge-sync", id, "unknown exception");
        }
    }

    template <typename Context>
    ruvia::task<void> remove(Context& c, std::string_view id) {
        ruvia::db_find_options options;
        options.where_ = service::common::database::activeId<entities::ProtocolConfigEntity>(id);
        const auto existing = co_await c.db().template get_repository<entities::ProtocolConfigEntity>().find_one(options);
        if (!existing) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        co_await requireOwner(c, existing->template get<"created_by">());
        ruvia::db_find_options used;
        used.where_ = service::device::entities::DeviceEntity::column<"protocol_config_id">() == id &&
            service::device::entities::DeviceEntity::column<"deleted_at">().is_null();
        if (co_await c.db().template get_repository<service::device::entities::DeviceEntity>().exists(used)) {
            service::common::fail(16008, "协议配置已被设备使用，请先删除关联设备", 409);
        }
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query expressions(c.pool());
        const ruvia::db_assignment deletionChanges[]{
            { std::string(entities::ProtocolConfigEntity::column<"deleted_at">().name()), expressions.call("now") },
            { std::string(entities::ProtocolConfigEntity::column<"updated_at">().name()), expressions.call("now") }
        };
        (void)co_await transaction.template get_repository<entities::ProtocolConfigEntity>().update(
            service::common::database::activeId<entities::ProtocolConfigEntity>(id),
            deletionChanges
        );
        co_await service::system::OutboxService::enqueueConfigEvent(transaction, "protocol", "deleted", id, service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
    }

  private:
    template <typename Context>
    static ruvia::task<std::string> detailData(Context& c, std::string_view id) {
        auto query = protocolSelect(c.pool());
        query.where(entities::ProtocolConfigEntity::column<"deleted_at">().is_null().expression(query))
            .and_where(query.binary(query.column(entities::ProtocolConfigEntity::column<"id">().name()), ruvia::db_binary_operator::equal, query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(16001, "协议配置不存在", 404);
        }
        co_return itemJson(rows.front());
    }

    static ruvia::db_query edgeNodeReferences(std::pmr::memory_resource* resource, std::string_view configId) {
        using Device = service::device::entities::DeviceEntity;
        using Link = service::link::entities::LinkEntity;
        ruvia::db_query query(resource);
        const auto edgeNodeId =
            query.cast(query.column(Link::column<"edge_node_id">().name(), "l"), ruvia::db_data_type::text);
        query.select(edgeNodeId)
            .from(Device::table_name(), "d")
            .join(ruvia::db_join_type::inner, Link::table_name(), query.binary(query.column(Link::column<"id">().name(), "l"), ruvia::db_binary_operator::equal, query.column(Device::column<"link_id">().name(), "d")), "l")
            .where(query.binary(query.column(Device::column<"protocol_config_id">().name(), "d"), ruvia::db_binary_operator::equal, query.cast(query.value(configId), ruvia::db_data_type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(Device::column<"deleted_at">().name(), "d")))
            .and_where(query.binary(query.column(Link::column<"execution">().name(), "l"), ruvia::db_binary_operator::equal, query.value("edge")))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(Link::column<"deleted_at">().name(), "l")))
            .and_where(query.unary(ruvia::db_unary_operator::is_not_null, query.column(Link::column<"edge_node_id">().name(), "l")))
            .distinct()
            .order_by(edgeNodeId);
        return query;
    }

    static ruvia::task<void> requireDerivedSupport(ruvia::db_transaction& transaction, std::pmr::memory_resource* resource, std::string_view configId) {
        const auto rows = co_await transaction.query(edgeNodeReferences(resource, configId));
        for (const auto& row : rows) {
            const auto nodeId = row[0].value().value_or(std::string_view{});
            ruvia::db_find_options nodeOptions;
            nodeOptions.where_ = service::edge_node::entities::EdgeNodeEntity::column<"id">() == nodeId;
            const auto node = co_await transaction.template get_repository<service::edge_node::entities::EdgeNodeEntity>().find_one(nodeOptions);
            const auto capability = node ? ruvia::json_value::parse(node->template get<"capability">()) : std::optional<ruvia::json_value>{};
            const auto supported = capability ? capability->template get<ruvia::bool_value>("derivedPoints") : std::optional<ruvia::bool_value>{};
            if (!supported || !static_cast<bool>(*supported)) {
                service::common::fail(16009, "关联边缘节点尚不支持派生点，请先升级固件", 409);
            }
        }
    }

    template <typename Context>
    static ruvia::task<void> syncEdgeNodes(Context& c, std::string_view configId) {
        const auto rows = co_await c.db().query(edgeNodeReferences(c.pool(), configId));
        for (const auto& row : rows) {
            const auto nodeId = row[0].value().value_or(std::string_view{});
            if (nodeId.empty()) {
                continue;
            }
            try {
                (void)co_await service::edge_node::EdgeService::queueSnapshot(c, nodeId, c.userId);
            } catch (const std::exception& error) {
                std::cerr << "protocol edge config sync failed: node=" << nodeId
                          << " config=" << configId << " error=" << error.what() << '\n';
            } catch (...) {
                std::cerr << "protocol edge config sync failed: node=" << nodeId
                          << " config=" << configId << " error=unknown exception\n";
            }
        }
    }

    static void logPostUpdateFailure(std::string_view stage, std::string_view configId, std::string_view message) {
        std::cerr << "protocol post-update " << stage << " failed: config=" << configId
                  << " error=" << message << '\n';
    }

    static ruvia::db_query protocolSelect(std::pmr::memory_resource* resource) {
        ruvia::db_query query(resource);
        using Config = entities::ProtocolConfigEntity;
        query.select({ query.cast(query.column(Config::column<"id">().name()), ruvia::db_data_type::text), query.column(Config::column<"protocol">().name()), query.column(Config::column<"name">().name()), query.column(Config::column<"enabled">().name()), query.column(Config::column<"config">().name()), service::common::database::emptyText(query, Config::column<"remark">().name()), query.cast(query.call("iot_utc_timestamp", { query.column(Config::column<"created_at">().name()) }), ruvia::db_data_type::text), query.cast(query.call("iot_utc_timestamp", { query.column(Config::column<"updated_at">().name()) }), ruvia::db_data_type::text) })
            .from(entities::ProtocolConfigEntity::table_name())
            .where(entities::ProtocolConfigEntity::column<"deleted_at">().is_null().expression(query));
        return query;
    }

    static std::string itemJson(const ruvia::db_row& row) {
        const auto enabled = row[3].as<bool>().value_or(false);
        std::string result = "{\"id\":" + service::utils::jsonQuoted(row[0].value().value_or("")) +
            ",\"protocol\":" +
            service::utils::jsonQuoted(row[1].value().value_or("")) +
            ",\"name\":" + service::utils::jsonQuoted(row[2].value().value_or("")) +
            ",\"enabled\":" + (enabled ? "true" : "false") +
            ",\"config\":" + std::string(row[4].value().value_or("{}")) +
            ",\"remark\":" + service::utils::jsonQuoted(row[5].value().value_or("")) +
            ",\"created_at\":" +
            service::utils::jsonQuoted(row[6].value().value_or("")) +
            ",\"updated_at\":" +
            service::utils::jsonQuoted(row[7].value().value_or("")) + "}";
        return result;
    }

    template <typename Context>
    ruvia::task<void> ensureNameAvailable(Context& c, const std::string& name, std::optional<std::string> excludedId) {
        ruvia::db_find_options options;
        options.where_ = entities::ProtocolConfigEntity::column<"deleted_at">().is_null() &&
            entities::ProtocolConfigEntity::column<"name">() == name;
        if (excludedId) {
            options.where_ = std::move(options.where_) && entities::ProtocolConfigEntity::column<"id">() != *excludedId;
        }
        if (co_await c.db().template get_repository<entities::ProtocolConfigEntity>().exists(options)) {
            service::common::fail(16005, "配置名称已存在", 409);
        }
    }

    template <typename Context>
    ruvia::task<void> requireOwner(Context& c, std::string_view ownerId) {
        if (c.userId == ownerId) {
            co_return;
        }
        using UserRole = service::user::entities::UserRoleEntity;
        using Role = service::role::entities::RoleEntity;
        ruvia::db_query query(c.pool());
        query.select(query.cast(query.value(1), ruvia::db_data_type::integer))
            .from(UserRole::table_name(), "ur")
            .join(ruvia::db_join_type::inner, Role::table_name(), query.binary(query.column(UserRole::column<"role_id">().name(), "ur"), ruvia::db_binary_operator::equal, query.column(Role::column<"id">().name(), "r")), "r")
            .where(query.binary(query.column(UserRole::column<"user_id">().name(), "ur"), ruvia::db_binary_operator::equal, query.value(c.userId)))
            .and_where(query.binary(query.column(Role::column<"code">().name(), "r"), ruvia::db_binary_operator::equal, query.value("superadmin")))
            .and_where(query.binary(query.column(Role::column<"status">().name(), "r"), ruvia::db_binary_operator::equal, query.value("enabled")))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(Role::column<"deleted_at">().name(), "r")))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(16007, "只能修改或删除自己创建的协议配置", 403);
        }
    }
};

inline ProtocolService& protocolService() {
    return ProtocolService::instance();
}

} // namespace service::protocol
