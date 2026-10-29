#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include <ruvia/web/model.h>
#include <ruvia/web/validation.h>

#include "service/common/device_parameter.h"
#include "service/common/uuid.h"
#include "service/utils/payload.h"

namespace service::protocol {

inline constexpr auto kDefaultDigits = RUVIA_DEFAULT(0){};
inline constexpr auto kDefaultWritable = RUVIA_DEFAULT(false){};
inline constexpr auto kDefaultVisible = RUVIA_DEFAULT(true){};
inline constexpr auto kDefaultEnabled = RUVIA_DEFAULT(true){};
inline constexpr auto kDefaultPage = RUVIA_DEFAULT(1){};
inline constexpr auto kDefaultPageSize = RUVIA_DEFAULT(10){};

RUVIA_MODEL(ExpressionTestInput, RUVIA_REQUIRED_FIELD(alias, ruvia::string, RUVIA_MIN(1, "变量名不能为空"), RUVIA_MAX(32, "变量名过长")), RUVIA_REQUIRED_FIELD(value, ruvia::double_value));
RUVIA_MODEL(ExpressionTestUnitRule, RUVIA_REQUIRED_FIELD(condition, ruvia::string, RUVIA_MIN(1, "单位条件不能为空"), RUVIA_MAX(512, "单位条件过长")), RUVIA_REQUIRED_FIELD(unit, ruvia::string, RUVIA_MAX(32, "单位过长")));
RUVIA_MODEL(ExpressionTestBody, RUVIA_REQUIRED_FIELD(expression, ruvia::string, RUVIA_MIN(1, "公式不能为空"), RUVIA_MAX(512, "公式过长")), RUVIA_REQUIRED_FIELD(inputs, ruvia::array<ExpressionTestInput>, RUVIA_MAX(16, "最多 16 个变量")), RUVIA_REQUIRED_FIELD(unit, ruvia::string, RUVIA_MAX(32, "单位过长")), RUVIA_REQUIRED_FIELD(unitRules, ruvia::array<ExpressionTestUnitRule>, RUVIA_MAX(8, "最多 8 条单位条件")));
RUVIA_MODEL(ExpressionTestResult, RUVIA_REQUIRED_FIELD(value, ruvia::double_value), RUVIA_REQUIRED_FIELD(unit, ruvia::string), RUVIA_REQUIRED_FIELD(matchedRule, ruvia::int64));
RUVIA_MODEL(ExpressionTestResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, ExpressionTestResult));
RUVIA_MODEL(ExpressionValidateBody, RUVIA_REQUIRED_FIELD(expression, ruvia::string, RUVIA_MIN(1, "公式不能为空"), RUVIA_MAX(512, "公式过长")), RUVIA_REQUIRED_FIELD(variables, ruvia::array<ruvia::string>, RUVIA_MAX(16, "最多 16 个变量")));

// 协议配置数字 token 按 PostgreSQL numeric 语义精确判断：指数形式先规范化（1e3 即 1000），
// 再按十进制比较，不经 double 舍入。Ruvia 整数/浮点字段无法表达此语义，故这些字段保留 JSON token。
class JsonNumericText final {
  public:
    static std::optional<std::string> canonical(std::string_view value, std::size_t maximum = 4096) {
        const auto first = value.find_first_not_of(" \t\r\n");
        const auto last = value.find_last_not_of(" \t\r\n");
        std::string_view token = first == std::string_view::npos ? std::string_view{} : value.substr(first, last - first + 1);
        if (token.empty()) {
            return std::nullopt;
        }
        bool negative = false;
        if (token.front() == '-' || token.front() == '+') {
            negative = token.front() == '-';
            token.remove_prefix(1);
        }
        const auto isDigit = [](char ch) {
            return ch >= '0' && ch <= '9';
        };
        std::size_t position = 0;
        while (position < token.size() && isDigit(token[position])) {
            ++position;
        }
        const auto integerDigits = position;
        std::size_t fractionDigits = 0;
        if (position < token.size() && token[position] == '.') {
            ++position;
            const auto fractionStart = position;
            while (position < token.size() && isDigit(token[position])) {
                ++position;
            }
            fractionDigits = position - fractionStart;
        }
        if (integerDigits == 0 && fractionDigits == 0) {
            return std::nullopt;
        }
        std::int64_t exponent = 0;
        if (position < token.size() && (token[position] == 'e' || token[position] == 'E')) {
            ++position;
            bool exponentNegative = false;
            if (position < token.size() && (token[position] == '-' || token[position] == '+')) {
                exponentNegative = token[position] == '-';
                ++position;
            }
            const auto exponentStart = position;
            while (position < token.size() && isDigit(token[position])) {
                ++position;
            }
            if (position == exponentStart) {
                return std::nullopt;
            }
            std::int64_t parsed = 0;
            const auto [end, error] = std::from_chars(token.data() + exponentStart, token.data() + position, parsed);
            if (error != std::errc{} || end != token.data() + position ||
                (exponentNegative && parsed == (std::numeric_limits<std::int64_t>::max)())) {
                return std::nullopt;
            }
            exponent = exponentNegative ? -parsed : parsed;
        }
        if (position != token.size() || integerDigits > maximum || fractionDigits > maximum ||
            integerDigits + fractionDigits > maximum) {
            return std::nullopt;
        }
        std::string digits;
        digits.reserve(integerDigits + fractionDigits);
        digits.append(token, 0, integerDigits);
        if (fractionDigits != 0) {
            digits.append(token, integerDigits + 1, fractionDigits);
        }
        const auto expansionLimit = static_cast<std::int64_t>(maximum - digits.size());
        if (exponent > expansionLimit || exponent < -expansionLimit) {
            return std::nullopt;
        }
        const auto decimalPosition = static_cast<std::int64_t>(integerDigits) + exponent;
        std::size_t fractionLength = 0;
        if (decimalPosition <= 0) {
            const auto leadingZeros = static_cast<std::size_t>(-decimalPosition);
            if (leadingZeros > maximum - digits.size()) {
                return std::nullopt;
            }
            fractionLength = leadingZeros + digits.size();
        } else if (decimalPosition < static_cast<std::int64_t>(digits.size())) {
            fractionLength = digits.size() - static_cast<std::size_t>(decimalPosition);
        }
        const auto integerLength = decimalPosition <= 0 ? std::size_t{ 1 } : std::max(digits.size(), static_cast<std::size_t>(decimalPosition));
        if (integerLength + (fractionLength == 0 ? 0 : 1 + fractionLength) > maximum) {
            return std::nullopt;
        }
        std::string output;
        if (decimalPosition <= 0) {
            output = "0.";
            output.append(static_cast<std::size_t>(-decimalPosition), '0');
            output += digits;
        } else if (decimalPosition >= static_cast<std::int64_t>(digits.size())) {
            output = digits;
            output.append(static_cast<std::size_t>(decimalPosition) - digits.size(), '0');
        } else {
            const auto split = static_cast<std::size_t>(decimalPosition);
            output.assign(digits.data(), split);
            output.push_back('.');
            output.append(digits.data() + split, digits.size() - split);
        }
        const auto dot = output.find('.');
        const auto integerEnd = dot == std::string::npos ? output.size() : dot;
        const auto firstNonZero = output.find_first_not_of('0');
        if (firstNonZero == std::string::npos || firstNonZero >= integerEnd) {
            output.erase(0, integerEnd > 0 ? integerEnd - 1 : 0);
        } else if (firstNonZero > 0) {
            output.erase(0, firstNonZero);
        }
        if (negative && digits.find_first_not_of('0') != std::string::npos) {
            output.insert(output.begin(), '-');
        }
        return output;
    }

    // 数字 token 规范化为十进制文本；strings 为 true 时字符串按解码后的原文参与整数判断。
    static std::optional<std::string> scalar(const ruvia::json_value& value, bool strings) {
        if (value.is_number()) {
            return canonical(value.view());
        }
        if (strings && value.is_string()) {
            if (const auto decoded = value.get<ruvia::string>()) {
                return std::string(decoded->view());
            }
        }
        return std::nullopt;
    }

    // 两个规范十进制文本的精确比较。
    static int compare(std::string_view lhs, std::string_view rhs) {
        const bool leftNegative = !lhs.empty() && lhs.front() == '-';
        const bool rightNegative = !rhs.empty() && rhs.front() == '-';
        if (leftNegative != rightNegative) {
            return leftNegative ? -1 : 1;
        }
        const auto result = compareAbsolute(leftNegative ? lhs.substr(1) : lhs, rightNegative ? rhs.substr(1) : rhs);
        return leftNegative ? -result : result;
    }

  private:
    static int compareAbsolute(std::string_view lhs, std::string_view rhs) {
        const auto integerPart = [](std::string_view value) {
            value = value.substr(0, std::min(value.find('.'), value.size()));
            const auto first = value.find_first_not_of('0');
            return first == std::string_view::npos ? std::string_view{ "0" } : value.substr(first);
        };
        const auto fractionPart = [](std::string_view value) {
            const auto dot = value.find('.');
            return dot == std::string_view::npos ? std::string_view{} : value.substr(dot + 1);
        };
        const auto lhsInteger = integerPart(lhs);
        const auto rhsInteger = integerPart(rhs);
        if (lhsInteger.size() != rhsInteger.size()) {
            return lhsInteger.size() < rhsInteger.size() ? -1 : 1;
        }
        if (lhsInteger != rhsInteger) {
            return lhsInteger < rhsInteger ? -1 : 1;
        }
        const auto lhsFraction = fractionPart(lhs);
        const auto rhsFraction = fractionPart(rhs);
        for (std::size_t index = 0; index < std::max(lhsFraction.size(), rhsFraction.size()); ++index) {
            const auto left = index < lhsFraction.size() ? lhsFraction[index] : '0';
            const auto right = index < rhsFraction.size() ? rhsFraction[index] : '0';
            if (left != right) {
                return left < right ? -1 : 1;
            }
        }
        return 0;
    }
};

// 整数字段：数字 token（strings 时也接受十进制字符串），十进制位数不超过 digits，值在 [minimum, maximum]。
struct JsonInteger {
    bool strings{};
    bool negative{};
    std::size_t digits{};
    std::int64_t minimum{};
    std::int64_t maximum{};

    std::optional<std::int64_t> value(const ruvia::json_value& raw) const {
        const auto text = JsonNumericText::scalar(raw, strings);
        if (!text) {
            return std::nullopt;
        }
        auto unsignedText = std::string_view(*text);
        if (negative && !unsignedText.empty() && unsignedText.front() == '-') {
            unsignedText.remove_prefix(1);
        }
        if (unsignedText.empty() || unsignedText.size() > digits ||
            !std::all_of(unsignedText.begin(), unsignedText.end(), [](char ch) {
                return ch >= '0' && ch <= '9';
            })) {
            return std::nullopt;
        }
        std::int64_t result{};
        const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), result);
        if (error != std::errc{} || end != text->data() + text->size() || result < minimum || result > maximum) {
            return std::nullopt;
        }
        return result;
    }

    bool operator()(const ruvia::json_value& raw) const {
        return value(raw).has_value();
    }
};

// 十进制数字段（可含小数），值在 [minimum, maximum]；不接受字符串。
struct JsonDecimal {
    std::int64_t minimum{};
    std::int64_t maximum{};

    bool operator()(const ruvia::json_value& raw) const {
        const auto text = JsonNumericText::scalar(raw, false);
        return text && !text->empty() && JsonNumericText::compare(*text, std::to_string(minimum)) >= 0 &&
            JsonNumericText::compare(*text, std::to_string(maximum)) <= 0;
    }
};

// 十六进制文本，长度在 [minimum, maximum]；empty 时也接受空串。
struct HexText {
    std::size_t minimum{};
    std::size_t maximum{};
    bool empty{};

    bool operator()(const ruvia::string& value) const {
        const auto text = value.view();
        return (empty && text.empty()) ||
            (text.size() >= minimum && text.size() <= maximum &&
             std::all_of(text.begin(), text.end(), [](unsigned char ch) {
                 return std::isxdigit(ch) != 0;
             }));
    }
};

// registers/areas/points 中的原生点位不再支持 scale/offset；非数组或非对象元素交给各协议模型判断。
inline bool nativePointsWithoutConversion(const ruvia::json_value& points) {
    bool valid = true;
    (void)points.for_each_element([&](const ruvia::json_value& point) {
        valid = !point.get<ruvia::json_value>("scale") && !point.get<ruvia::json_value>("offset");
        return valid;
    });
    return valid;
}

// pointVisibility：点位 UUID 到布尔值的映射。
inline bool pointVisibilityValid(const ruvia::json_value& visibility) {
    return visibility.is_object() && visibility.for_each_field([](std::string_view id, const ruvia::json_value& visible) {
        return service::common::isUuid(id) && (visible.view() == "true" || visible.view() == "false");
    });
}

// MQTT Topic：filter 允许 + 与末层 #；empty 时空串表示未配置；fixed 时不得含 {point:} 占位。
struct MqttTopic {
    bool filter{};
    bool empty{};
    bool fixed{};

    static bool valid(std::string_view topic, bool filter) {
        if (topic.empty() || topic.size() > 1024 || topic.find('\0') != std::string_view::npos) {
            return false;
        }
        const auto levels = service::utils::splitPayload(topic, "/");
        for (std::size_t i = 0; i < levels.size(); ++i) {
            if (levels[i].find_first_of("+#") == std::string_view::npos) {
                continue;
            }
            if (!filter || (levels[i] != "+" && (levels[i] != "#" || i + 1 != levels.size()))) {
                return false;
            }
        }
        return true;
    }

    bool operator()(const ruvia::string& value) const {
        const auto topic = value.view();
        return (empty && topic.empty()) || (valid(topic, filter) && (!fixed || topic.find("{point:") == std::string_view::npos));
    }
};

// 数字字段保留 json_value：Ruvia 整数字段拒绝指数写法与数字字符串（两者历史上被接受且有集成测试覆盖），
// Ruvia 也不允许 json_value 声明字段约束，因此这些字段的范围由 Service 按下列规则校验。
inline constexpr JsonInteger kReadInterval{ .strings = true, .digits = 5, .minimum = 1, .maximum = 3600 };
inline constexpr JsonInteger kCommandFastReadDuration{ .strings = true, .digits = 5, .minimum = 0, .maximum = 3600 };
inline constexpr JsonInteger kCommandFastReadInterval{ .strings = true, .digits = 5, .minimum = 1, .maximum = 3600 };
inline constexpr JsonInteger kPointDecimals{ .negative = true, .digits = 2, .minimum = -1, .maximum = 8 };
inline constexpr JsonInteger kSl651ByteOffset{ .digits = 7, .minimum = 0, .maximum = 8388607 };
inline constexpr JsonInteger kSl651Length{ .digits = 7, .minimum = 0, .maximum = 8388608 };
inline constexpr JsonInteger kSl651Digits{ .digits = 1, .minimum = 0, .maximum = 7 };
inline constexpr JsonInteger kModbusMergeGap{ .strings = true, .digits = 5, .minimum = 0, .maximum = 2000 };
inline constexpr JsonInteger kModbusMaxQuantity{ .strings = true, .digits = 5, .minimum = 1, .maximum = 125 };
inline constexpr JsonInteger kModbusAddress{ .digits = 5, .minimum = 0, .maximum = 65535 };
inline constexpr JsonInteger kModbusQuantity{ .digits = 1, .minimum = 1, .maximum = 4 };
inline constexpr JsonInteger kModbusDecimals{ .strings = true, .negative = true, .digits = 2, .minimum = -1, .maximum = 8 };
inline constexpr JsonDecimal kS7ProbeTimeout{ .minimum = 1000, .maximum = 30000 };
inline constexpr JsonInteger kS7Start{ .digits = 10, .minimum = 0, .maximum = 2147483647 };
inline constexpr JsonInteger kS7Size{ .digits = 5, .minimum = 1, .maximum = 65535 };
inline constexpr JsonInteger kS7StartBit{ .digits = 1, .minimum = 0, .maximum = 7 };
inline constexpr JsonInteger kS7DbNumber{ .digits = 5, .minimum = 1, .maximum = 65535 };
inline constexpr JsonInteger kMcByte{ .digits = 3, .minimum = 0, .maximum = 255 };
inline constexpr JsonInteger kMcModuleIo{ .digits = 5, .minimum = 0, .maximum = 65535 };
inline constexpr JsonInteger kMcMonitoringTimer{ .digits = 5, .minimum = 1, .maximum = 65535 };
inline constexpr JsonInteger kMcAddress{ .digits = 8, .minimum = 0, .maximum = 16777215 };
inline constexpr JsonInteger kFinsNetwork{ .digits = 3, .minimum = 0, .maximum = 127 };
inline constexpr JsonInteger kFinsNode{ .digits = 3, .minimum = 0, .maximum = 254 };
inline constexpr JsonInteger kFinsUnit{ .digits = 3, .minimum = 0, .maximum = 255 };
inline constexpr JsonInteger kFinsAddress{ .digits = 8, .minimum = 0, .maximum = 65535 };
inline constexpr JsonInteger kFinsBit{ .digits = 2, .minimum = 0, .maximum = 15 };
inline constexpr JsonInteger kDlt645WakeupBytes{ .digits = 1, .minimum = 0, .maximum = 4 };
inline constexpr JsonInteger kDlt645Length{ .digits = 3, .minimum = 1, .maximum = 200 };
inline constexpr JsonInteger kDlt645Digits{ .digits = 1, .minimum = 0, .maximum = 8 };

// 字段约束与 Service 共用的原文案（Service 用于结构错误定位与 json_value 字段校验）。
inline constexpr ruvia::fixed_string kStoragePolicyInvalid{ "配置的 storagePolicy 无效" };
inline constexpr ruvia::fixed_string kSl651Invalid{ "SL651 配置无效" };
inline constexpr ruvia::fixed_string kSl651DefinitionInvalid{ "SL651 引导符、长度或小数位不符合数据定义" };
inline constexpr ruvia::fixed_string kIndustrialPointInvalid{ "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证" };
inline constexpr ruvia::fixed_string kMcConnectionInvalid{ "MC/SLMP 连接参数无效" };
inline constexpr ruvia::fixed_string kDlt645ConnectionInvalid{ "DL/T645 版本或唤醒字节数无效" };
inline constexpr ruvia::fixed_string kS7Invalid{ "S7 配置无效" };
inline constexpr ruvia::fixed_string kS7AreaInvalid{ "S7 寄存器配置无效" };
inline constexpr ruvia::fixed_string kModbusByteOrderInvalid{ "Modbus 配置的 byteOrder 无效" };
inline constexpr ruvia::fixed_string kModbusRegisterInvalid{ "Modbus 寄存器配置无效" };
inline constexpr ruvia::fixed_string kMqttPointInvalid{ "MQTT 点位名称、标识、JSON 字段或类型无效" };
inline constexpr ruvia::fixed_string kMqttCommandInvalid{ "控制消息名称、标识或 Topic 无效或重复" };
inline constexpr ruvia::fixed_string kMqttFormatInvalid{ "MQTT 负载格式或设备识别方式无效" };
inline constexpr ruvia::fixed_string kMqttEnumInvalid{ "枚举原值、转换值须唯一且符合点位类型" };
inline constexpr ruvia::fixed_string kDerivedIdInvalid{ "派生点字段无效: id" };

// 所有协议共用的顶层配置字段；storageInterval、deviceParameters 为已退役字段，出现即拒绝。
// parameters 使用公共契约 service::common::DeviceParameterConfiguration 单独解析。
RUVIA_MODEL(ProtocolConfigCommon, RUVIA_OPTIONAL_FIELD(storageInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(storagePolicy, ruvia::string, RUVIA_ONE_OF(kStoragePolicyInvalid, "report", "change")), RUVIA_OPTIONAL_FIELD(deviceParameters, ruvia::json_value));

RUVIA_MODEL(Sl651Element, RUVIA_OPTIONAL_FIELD(positionMode, ruvia::string, RUVIA_ONE_OF(kSl651DefinitionInvalid, "GUIDE", "OFFSET")), RUVIA_OPTIONAL_FIELD(byteOffset, ruvia::json_value), RUVIA_OPTIONAL_FIELD(length, ruvia::json_value), RUVIA_OPTIONAL_FIELD(digits, ruvia::json_value), RUVIA_OPTIONAL_FIELD(encode, ruvia::string, RUVIA_ONE_OF(kSl651DefinitionInvalid, "BCD", "HEX", "DICT", "JPEG", "TIME_YYMMDDHHMMSS")), RUVIA_OPTIONAL_FIELD(guideHex, ruvia::string));
RUVIA_MODEL(Sl651Func, RUVIA_OPTIONAL_FIELD(funcCode, ruvia::string, RUVIA_CUSTOM(kSl651DefinitionInvalid, (HexText{ .minimum = 2, .maximum = 2 }))), RUVIA_OPTIONAL_FIELD(dir, ruvia::string, RUVIA_ONE_OF(kSl651DefinitionInvalid, "UP", "DOWN")), RUVIA_OPTIONAL_FIELD(elements, ruvia::array<Sl651Element>), RUVIA_OPTIONAL_FIELD(responseElements, ruvia::array<Sl651Element>));
RUVIA_MODEL(Sl651Config, RUVIA_OPTIONAL_FIELD(responseMode, ruvia::string, RUVIA_ONE_OF(kSl651Invalid, "M1", "M2", "M3", "M4")), RUVIA_OPTIONAL_FIELD(funcs, ruvia::array<Sl651Func>));

RUVIA_MODEL(ModbusPacket, RUVIA_OPTIONAL_FIELD(mergeGap, ruvia::json_value), RUVIA_OPTIONAL_FIELD(maxQuantity, ruvia::json_value));
RUVIA_MODEL(ModbusRegister, RUVIA_OPTIONAL_FIELD(id, ruvia::string, RUVIA_MIN(1, kModbusRegisterInvalid)), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(1, kModbusRegisterInvalid)), RUVIA_OPTIONAL_FIELD(registerType, ruvia::string, RUVIA_ONE_OF(kModbusRegisterInvalid, "COIL", "DISCRETE_INPUT", "HOLDING_REGISTER", "INPUT_REGISTER")), RUVIA_OPTIONAL_FIELD(dataType, ruvia::string, RUVIA_ONE_OF(kModbusRegisterInvalid, "BOOL", "INT16", "UINT16", "INT32", "UINT32", "FLOAT32", "INT64", "UINT64", "DOUBLE")), RUVIA_OPTIONAL_FIELD(address, ruvia::json_value), RUVIA_OPTIONAL_FIELD(quantity, ruvia::json_value), RUVIA_OPTIONAL_FIELD(byteOrder, ruvia::string, RUVIA_ONE_OF("Modbus 寄存器 byteOrder 无效", "BIG_ENDIAN", "LITTLE_ENDIAN", "BIG_ENDIAN_BYTE_SWAP", "LITTLE_ENDIAN_BYTE_SWAP")), RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value), RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value));
// pollInterval 为已退役字段，出现即拒绝。
RUVIA_MODEL(ModbusConfig, RUVIA_OPTIONAL_FIELD(byteOrder, ruvia::string, RUVIA_ONE_OF(kModbusByteOrderInvalid, "BIG_ENDIAN", "LITTLE_ENDIAN", "BIG_ENDIAN_BYTE_SWAP", "LITTLE_ENDIAN_BYTE_SWAP")), RUVIA_OPTIONAL_FIELD(registers, ruvia::array<ModbusRegister>), RUVIA_OPTIONAL_FIELD(packet, ModbusPacket), RUVIA_OPTIONAL_FIELD(readInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(pollInterval, ruvia::json_value));

RUVIA_MODEL(ExternalIoPoint, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("外部 IO 点位标识无效", service::common::isUuidField)), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "外部 IO 点位名称不能为空"), RUVIA_MAX(100, "外部 IO 点位名称过长")), RUVIA_REQUIRED_FIELD(kind, ruvia::string, RUVIA_ONE_OF("外部 IO 点位类型无效", "DI", "DO", "AI")), RUVIA_REQUIRED_FIELD(channel, ruvia::string, RUVIA_MIN(1, "请选择 IO 通道"), RUVIA_MAX(16, "IO 通道无效")), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, "外部 IO 点位单位过长")), RUVIA_OPTIONAL_FIELD(visible, ruvia::bool_value));
RUVIA_MODEL(ExternalIoConfig, RUVIA_OPTIONAL_FIELD(edgeNodeModel, ruvia::string, RUVIA_MIN(1, "请选择边缘节点型号"), RUVIA_MAX(128, "边缘节点型号过长")), RUVIA_OPTIONAL_FIELD(points, ruvia::array<ExternalIoPoint>, RUVIA_MAX(15, "外部 IO 最多配置 15 个点位")));

struct ExternalIoChannelRange {
    std::string_view kind;
    std::string_view prefix;
    int count;
};

inline constexpr std::string_view kLegacyExternalIoModel = "Ebyte T113-I ECM30";
inline constexpr std::array kEcm30IoChannels{
    ExternalIoChannelRange{ "DI", "di", 4 },
    ExternalIoChannelRange{ "DO", "do", 7 },
    ExternalIoChannelRange{ "AI", "AI", 4 }
};
RUVIA_MODEL(ExternalIoHardwareChannel, RUVIA_REQUIRED_FIELD(channel, ruvia::string), RUVIA_REQUIRED_FIELD(kind, ruvia::string), RUVIA_REQUIRED_FIELD(controllable, ruvia::bool_value));
RUVIA_MODEL(ExternalIoHardwareModel, RUVIA_REQUIRED_FIELD(model, ruvia::string), RUVIA_REQUIRED_FIELD(channels, ruvia::array<ExternalIoHardwareChannel>));

RUVIA_MODEL(S7Connection, RUVIA_OPTIONAL_FIELD(probeMode, ruvia::string, RUVIA_ONE_OF(kS7Invalid, "STANDARD", "COMPATIBLE", "AUTO")), RUVIA_OPTIONAL_FIELD(handshakeTimeout, ruvia::json_value), RUVIA_OPTIONAL_FIELD(directProbeTimeout, ruvia::json_value));
RUVIA_MODEL(S7Area, RUVIA_OPTIONAL_FIELD(id, ruvia::string, RUVIA_MIN(1, kS7AreaInvalid)), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(1, kS7AreaInvalid)), RUVIA_OPTIONAL_FIELD(area, ruvia::string, RUVIA_ONE_OF(kS7AreaInvalid, "DB", "V", "MK", "PE", "PA", "CT", "TM")), RUVIA_OPTIONAL_FIELD(dataType, ruvia::string, RUVIA_ONE_OF(kS7AreaInvalid, "BOOL", "INT8", "UINT8", "INT16", "UINT16", "INT32", "UINT32", "FLOAT", "LREAL", "STRING")), RUVIA_OPTIONAL_FIELD(start, ruvia::json_value), RUVIA_OPTIONAL_FIELD(size, ruvia::json_value), RUVIA_OPTIONAL_FIELD(startBit, ruvia::json_value), RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value), RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(dbNumber, ruvia::json_value));
// pollInterval 为已退役字段，出现即拒绝。
RUVIA_MODEL(S7Config, RUVIA_OPTIONAL_FIELD(plcModel, ruvia::string, RUVIA_ONE_OF(kS7Invalid, "S7-200", "S7-300", "S7-400", "S7-1200", "S7-1500")), RUVIA_OPTIONAL_FIELD(connection, S7Connection), RUVIA_OPTIONAL_FIELD(areas, ruvia::array<S7Area>), RUVIA_OPTIONAL_FIELD(pollInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(readInterval, ruvia::json_value));

// MC、FINS、DL/T645 的连接与点位历史上共用一个结构：各协议模型保留其他工业协议字段的类型约束，
// 只对本协议字段声明取值规则。
RUVIA_MODEL(McConnection, RUVIA_OPTIONAL_FIELD(frame, ruvia::string, RUVIA_ONE_OF(kMcConnectionInvalid, "3E", "4E")), RUVIA_OPTIONAL_FIELD(network, ruvia::json_value), RUVIA_OPTIONAL_FIELD(station, ruvia::json_value), RUVIA_OPTIONAL_FIELD(moduleIo, ruvia::json_value), RUVIA_OPTIONAL_FIELD(multidrop, ruvia::json_value), RUVIA_OPTIONAL_FIELD(monitoringTimer, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationNetwork, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceNetwork, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationNode, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceNode, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationUnit, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceUnit, ruvia::json_value), RUVIA_OPTIONAL_FIELD(version, ruvia::string), RUVIA_OPTIONAL_FIELD(wakeupBytes, ruvia::json_value), RUVIA_OPTIONAL_FIELD(writePassword, ruvia::string), RUVIA_OPTIONAL_FIELD(operatorCode, ruvia::string));
RUVIA_MODEL(McPoint, RUVIA_OPTIONAL_FIELD(id, ruvia::string, RUVIA_CUSTOM(kIndustrialPointInvalid, service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(1, kIndustrialPointInvalid), RUVIA_MAX(100, kIndustrialPointInvalid)), RUVIA_OPTIONAL_FIELD(dataType, ruvia::string, RUVIA_ONE_OF(kIndustrialPointInvalid, "BOOL", "INT16", "UINT16", "INT32", "UINT32", "FLOAT32", "INT64", "UINT64", "DOUBLE")), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, kIndustrialPointInvalid)), RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(area, ruvia::string, RUVIA_ONE_OF(kIndustrialPointInvalid, "M", "X", "Y", "B", "L", "F", "V", "S", "TS", "CS", "D", "W", "R", "ZR", "TN", "CN")), RUVIA_OPTIONAL_FIELD(byteOrder, ruvia::string, RUVIA_ONE_OF(kIndustrialPointInvalid, "BIG_ENDIAN", "LITTLE_ENDIAN", "BIG_ENDIAN_BYTE_SWAP", "LITTLE_ENDIAN_BYTE_SWAP")), RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value), RUVIA_OPTIONAL_FIELD(address, ruvia::json_value), RUVIA_OPTIONAL_FIELD(identifier, ruvia::string), RUVIA_OPTIONAL_FIELD(length, ruvia::json_value), RUVIA_OPTIONAL_FIELD(digits, ruvia::json_value), RUVIA_OPTIONAL_FIELD(bit, ruvia::json_value));
RUVIA_MODEL(McConfig, RUVIA_OPTIONAL_FIELD(readInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(commandFastReadDuration, ruvia::json_value), RUVIA_OPTIONAL_FIELD(commandFastReadInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(connection, McConnection), RUVIA_OPTIONAL_FIELD(points, ruvia::array<McPoint>, RUVIA_MAX(256, kIndustrialPointInvalid)));

RUVIA_MODEL(FinsConnection, RUVIA_OPTIONAL_FIELD(frame, ruvia::string), RUVIA_OPTIONAL_FIELD(network, ruvia::json_value), RUVIA_OPTIONAL_FIELD(station, ruvia::json_value), RUVIA_OPTIONAL_FIELD(moduleIo, ruvia::json_value), RUVIA_OPTIONAL_FIELD(multidrop, ruvia::json_value), RUVIA_OPTIONAL_FIELD(monitoringTimer, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationNetwork, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceNetwork, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationNode, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceNode, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationUnit, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceUnit, ruvia::json_value), RUVIA_OPTIONAL_FIELD(version, ruvia::string), RUVIA_OPTIONAL_FIELD(wakeupBytes, ruvia::json_value), RUVIA_OPTIONAL_FIELD(writePassword, ruvia::string), RUVIA_OPTIONAL_FIELD(operatorCode, ruvia::string));
RUVIA_MODEL(FinsPoint, RUVIA_OPTIONAL_FIELD(id, ruvia::string, RUVIA_CUSTOM(kIndustrialPointInvalid, service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(1, kIndustrialPointInvalid), RUVIA_MAX(100, kIndustrialPointInvalid)), RUVIA_OPTIONAL_FIELD(dataType, ruvia::string, RUVIA_ONE_OF(kIndustrialPointInvalid, "BOOL", "INT16", "UINT16", "INT32", "UINT32", "FLOAT32", "INT64", "UINT64", "DOUBLE")), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, kIndustrialPointInvalid)), RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(area, ruvia::string, RUVIA_ONE_OF(kIndustrialPointInvalid, "D", "CIO", "W", "H", "A")), RUVIA_OPTIONAL_FIELD(byteOrder, ruvia::string, RUVIA_ONE_OF(kIndustrialPointInvalid, "BIG_ENDIAN", "LITTLE_ENDIAN", "BIG_ENDIAN_BYTE_SWAP", "LITTLE_ENDIAN_BYTE_SWAP")), RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value), RUVIA_OPTIONAL_FIELD(address, ruvia::json_value), RUVIA_OPTIONAL_FIELD(bit, ruvia::json_value), RUVIA_OPTIONAL_FIELD(identifier, ruvia::string), RUVIA_OPTIONAL_FIELD(length, ruvia::json_value), RUVIA_OPTIONAL_FIELD(digits, ruvia::json_value));
RUVIA_MODEL(FinsConfig, RUVIA_OPTIONAL_FIELD(readInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(commandFastReadDuration, ruvia::json_value), RUVIA_OPTIONAL_FIELD(commandFastReadInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(connection, FinsConnection), RUVIA_OPTIONAL_FIELD(points, ruvia::array<FinsPoint>, RUVIA_MAX(256, kIndustrialPointInvalid)));

RUVIA_MODEL(Dlt645Connection, RUVIA_OPTIONAL_FIELD(frame, ruvia::string), RUVIA_OPTIONAL_FIELD(network, ruvia::json_value), RUVIA_OPTIONAL_FIELD(station, ruvia::json_value), RUVIA_OPTIONAL_FIELD(moduleIo, ruvia::json_value), RUVIA_OPTIONAL_FIELD(multidrop, ruvia::json_value), RUVIA_OPTIONAL_FIELD(monitoringTimer, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationNetwork, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceNetwork, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationNode, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceNode, ruvia::json_value), RUVIA_OPTIONAL_FIELD(destinationUnit, ruvia::json_value), RUVIA_OPTIONAL_FIELD(sourceUnit, ruvia::json_value), RUVIA_OPTIONAL_FIELD(version, ruvia::string, RUVIA_ONE_OF(kDlt645ConnectionInvalid, "1997", "2007")), RUVIA_OPTIONAL_FIELD(wakeupBytes, ruvia::json_value), RUVIA_OPTIONAL_FIELD(writePassword, ruvia::string, RUVIA_CUSTOM("DL/T645 写入认证字段必须为空或 8 位十六进制", (HexText{ .minimum = 8, .maximum = 8, .empty = true }))), RUVIA_OPTIONAL_FIELD(operatorCode, ruvia::string, RUVIA_CUSTOM("DL/T645 写入认证字段必须为空或 8 位十六进制", (HexText{ .minimum = 8, .maximum = 8, .empty = true }))));
RUVIA_MODEL(Dlt645Point, RUVIA_OPTIONAL_FIELD(id, ruvia::string, RUVIA_CUSTOM(kIndustrialPointInvalid, service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(1, kIndustrialPointInvalid), RUVIA_MAX(100, kIndustrialPointInvalid)), RUVIA_OPTIONAL_FIELD(dataType, ruvia::string, RUVIA_ONE_OF(kIndustrialPointInvalid, "BCD", "BCD_SIGNED", "HEX")), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, kIndustrialPointInvalid)), RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(identifier, ruvia::string, RUVIA_CUSTOM(kIndustrialPointInvalid, (HexText{ .minimum = 4, .maximum = 8 }))), RUVIA_OPTIONAL_FIELD(length, ruvia::json_value), RUVIA_OPTIONAL_FIELD(digits, ruvia::json_value), RUVIA_OPTIONAL_FIELD(area, ruvia::string), RUVIA_OPTIONAL_FIELD(byteOrder, ruvia::string), RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value), RUVIA_OPTIONAL_FIELD(address, ruvia::json_value), RUVIA_OPTIONAL_FIELD(bit, ruvia::json_value));
RUVIA_MODEL(Dlt645Config, RUVIA_OPTIONAL_FIELD(readInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(commandFastReadDuration, ruvia::json_value), RUVIA_OPTIONAL_FIELD(commandFastReadInterval, ruvia::json_value), RUVIA_OPTIONAL_FIELD(connection, Dlt645Connection), RUVIA_OPTIONAL_FIELD(points, ruvia::array<Dlt645Point>, RUVIA_MAX(256, kIndustrialPointInvalid)));

RUVIA_MODEL(MqttValueMapping, RUVIA_REQUIRED_FIELD(input, ruvia::string, RUVIA_MAX(256, kMqttEnumInvalid)), RUVIA_REQUIRED_FIELD(output, ruvia::string, RUVIA_MAX(256, kMqttEnumInvalid)));
RUVIA_MODEL(MqttPoint, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM(kMqttPointInvalid, service::common::isUuidField)), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, kMqttPointInvalid), RUVIA_MAX(100, kMqttPointInvalid)), RUVIA_REQUIRED_FIELD(field, ruvia::string, RUVIA_MAX(256, kMqttPointInvalid)), RUVIA_REQUIRED_FIELD(dataType, ruvia::string, RUVIA_ONE_OF(kMqttPointInvalid, "BOOL", "STRING", "DOUBLE")), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, "MQTT 点位单位过长")), RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(enumValues, ruvia::array<MqttValueMapping>, RUVIA_MAX(64, "最多 64 条枚举映射")));
RUVIA_MODEL(MqttCommandResponse, RUVIA_REQUIRED_FIELD(topic, ruvia::string, RUVIA_CUSTOM("设备回执需要固定设备 Topic 及用户定义的关联字段", (MqttTopic{ .fixed = true }))), RUVIA_REQUIRED_FIELD(matchFields, ruvia::array<ruvia::string>, RUVIA_MIN(1, "回执须有 1–16 个关联字段"), RUVIA_MAX(16, "回执须有 1–16 个关联字段")), RUVIA_REQUIRED_FIELD(codeField, ruvia::string, RUVIA_MIN(1, "回执状态路径无效"), RUVIA_MAX(256, "回执状态路径无效")), RUVIA_REQUIRED_FIELD(successCode, ruvia::int64, RUVIA_MIN(-9007199254740991LL, "回执成功码须为 JSON 安全整数"), RUVIA_MAX(9007199254740991LL, "回执成功码须为 JSON 安全整数")), RUVIA_OPTIONAL_FIELD(timeoutMs, ruvia::int64, RUVIA_MIN(1000, "回执超时须为 1000–120000 毫秒"), RUVIA_MAX(120000, "回执超时须为 1000–120000 毫秒")));
RUVIA_MODEL(MqttCommandMessage, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM(kMqttCommandInvalid, service::common::isUuidField)), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, kMqttCommandInvalid), RUVIA_MAX(100, kMqttCommandInvalid)), RUVIA_REQUIRED_FIELD(topic, ruvia::string, RUVIA_CUSTOM(kMqttCommandInvalid, MqttTopic{})), RUVIA_REQUIRED_FIELD_NAME("template", templateText, ruvia::string), RUVIA_REQUIRED_FIELD(requiredPointIds, ruvia::array<ruvia::string>), RUVIA_OPTIONAL_FIELD(response, MqttCommandResponse));
RUVIA_MODEL(MqttConfig, RUVIA_REQUIRED_FIELD(identityProperty, ruvia::string), RUVIA_OPTIONAL_FIELD(parameters, ruvia::array<service::common::DeviceParameterDefinition>), RUVIA_OPTIONAL_FIELD(storagePolicy, ruvia::string), RUVIA_OPTIONAL_FIELD(topic, ruvia::string, RUVIA_CUSTOM("MQTT Topic 过滤器无效", (MqttTopic{ .filter = true }))), RUVIA_OPTIONAL_FIELD(recordsPath, ruvia::string, RUVIA_MAX(256, "记录路径仅用于 JSON")), RUVIA_OPTIONAL_FIELD(deviceCodeField, ruvia::string), RUVIA_OPTIONAL_FIELD(identitySource, ruvia::string, RUVIA_ONE_OF(kMqttFormatInvalid, "bound", "payload", "topic")), RUVIA_OPTIONAL_FIELD(topicDeviceSegment, ruvia::int64, RUVIA_MIN(0, "Topic 设备标识段号无效"), RUVIA_MAX(1024, "Topic 设备标识段号无效")), RUVIA_OPTIONAL_FIELD(payloadFormat, ruvia::string, RUVIA_ONE_OF(kMqttFormatInvalid, "json", "text", "binary")), RUVIA_OPTIONAL_FIELD(delimiter, ruvia::string, RUVIA_MIN(1, "分隔符长度须为 1 到 8 字节"), RUVIA_MAX(8, "分隔符长度须为 1 到 8 字节")), RUVIA_OPTIONAL_FIELD(recordDelimiter, ruvia::string, RUVIA_MIN(1, "分隔符长度须为 1 到 8 字节"), RUVIA_MAX(8, "分隔符长度须为 1 到 8 字节")), RUVIA_OPTIONAL_FIELD(recordLength, ruvia::int64, RUVIA_MIN(0, "二进制记录长度无效"), RUVIA_MAX(1048576, "二进制记录长度无效")), RUVIA_OPTIONAL_FIELD(timeField, ruvia::string), RUVIA_OPTIONAL_FIELD(timeSource, ruvia::string, RUVIA_ONE_OF("数据时间无效", "payload", "received")), RUVIA_OPTIONAL_FIELD(timeFormat, ruvia::string, RUVIA_ONE_OF("时间格式无效", "unix_ms", "unix_s", "iso8601")), RUVIA_OPTIONAL_FIELD(reportTemplate, ruvia::string), RUVIA_OPTIONAL_FIELD(commandTopic, ruvia::string, RUVIA_CUSTOM("MQTT 指令 Topic 无效", (MqttTopic{ .empty = true }))), RUVIA_OPTIONAL_FIELD(commandTemplate, ruvia::string), RUVIA_OPTIONAL_FIELD(commands, ruvia::array<MqttCommandMessage>, RUVIA_MAX(32, "控制消息最多 32 条，且不能与旧版单条控制配置同时使用")), RUVIA_OPTIONAL_FIELD(qos, ruvia::int64, RUVIA_MIN(0, "MQTT QoS 必须是 0、1 或 2"), RUVIA_MAX(2, "MQTT QoS 必须是 0、1 或 2")), RUVIA_OPTIONAL_FIELD(points, ruvia::array<MqttPoint>, RUVIA_MAX(256, "MQTT 点位配置无效")));

// 派生点（protocol 写入时的字段约束；结构解码与依赖排序在 service/common/derived_point.h）。
RUVIA_MODEL(DerivedPointInput, RUVIA_OPTIONAL_FIELD(alias, ruvia::string, RUVIA_MIN(1, "派生点字段无效: alias"), RUVIA_MAX(32, "派生点字段无效: alias")), RUVIA_OPTIONAL_FIELD(pointId, ruvia::string, RUVIA_MIN(1, "派生点字段无效: pointId"), RUVIA_MAX(36, "派生点字段无效: pointId"), RUVIA_CUSTOM("派生点输入绑定无效", service::common::isUuidField)));
RUVIA_MODEL(DerivedPointUnitRule, RUVIA_OPTIONAL_FIELD(condition, ruvia::string, RUVIA_MIN(1, "派生点字段无效: condition"), RUVIA_MAX(512, "派生点字段无效: condition")), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, "派生点字段无效: unit")));
RUVIA_MODEL(DerivedPointDefinition, RUVIA_OPTIONAL_FIELD(id, ruvia::string, RUVIA_MIN(1, kDerivedIdInvalid), RUVIA_MAX(36, kDerivedIdInvalid), RUVIA_CUSTOM("派生点标识无效或重复", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_MIN(1, "派生点字段无效: name"), RUVIA_MAX(100, "派生点字段无效: name")), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, "派生点字段无效: unit")), RUVIA_OPTIONAL_FIELD(unitMode, ruvia::string, RUVIA_MIN(1, "派生点字段无效: unitMode"), RUVIA_MAX(16, "派生点字段无效: unitMode"), RUVIA_ONE_OF("单位模式无效", "fixed", "conditional")), RUVIA_OPTIONAL_FIELD(kind, ruvia::string, RUVIA_MIN(1, "派生点字段无效: kind"), RUVIA_MAX(16, "派生点字段无效: kind"), RUVIA_ONE_OF("派生点计算类型无效", "expression", "average", "minimum", "maximum")), RUVIA_OPTIONAL_FIELD(valueType, ruvia::string, RUVIA_MIN(1, "派生点字段无效: valueType"), RUVIA_MAX(8, "派生点字段无效: valueType"), RUVIA_ONE_OF("派生点结果类型无效", "number", "boolean")), RUVIA_OPTIONAL_FIELD(expression, ruvia::string, RUVIA_MAX(512, "派生点字段无效: expression")), RUVIA_OPTIONAL_FIELD(windowSeconds, ruvia::int64, RUVIA_MIN(1, "派生点时间参数无效: windowSeconds"), RUVIA_MAX(86400, "派生点时间参数无效: windowSeconds")), RUVIA_OPTIONAL_FIELD(maxAgeSeconds, ruvia::int64, RUVIA_MIN(1, "派生点时间参数无效: maxAgeSeconds"), RUVIA_MAX(86400, "派生点时间参数无效: maxAgeSeconds")), RUVIA_OPTIONAL_FIELD(visible, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(inputs, ruvia::array<DerivedPointInput>, RUVIA_MIN(1, "派生点必须绑定输入点"), RUVIA_MAX(16, "派生点输入绑定无效")), RUVIA_OPTIONAL_FIELD(sourceAlias, ruvia::string), RUVIA_OPTIONAL_FIELD(unitRules, ruvia::array<DerivedPointUnitRule>, RUVIA_MAX(8, "单位条件最多 8 条")));
inline constexpr ruvia::fixed_string kDerivedPointsLimit{ "派生点最多 32 个" };
RUVIA_MODEL(DerivedPointsConfig, RUVIA_OPTIONAL_FIELD(derivedPoints, ruvia::array<DerivedPointDefinition>, RUVIA_MAX(32, kDerivedPointsLimit)), RUVIA_OPTIONAL_FIELD(pointVisibility, ruvia::json_value));

RUVIA_MODEL(MqttPreviewBody, RUVIA_REQUIRED_FIELD(config, ruvia::json_value), RUVIA_REQUIRED_FIELD(topic, ruvia::string, RUVIA_MIN(1, "Topic 不能为空"), RUVIA_MAX(1024, "Topic 过长")), RUVIA_REQUIRED_FIELD(payload, ruvia::string, RUVIA_MAX(2097152, "示例负载过长")), RUVIA_OPTIONAL_FIELD(deviceCode, ruvia::string, RUVIA_MAX(128, "设备编码过长")), RUVIA_OPTIONAL_FIELD(timezone, ruvia::string, RUVIA_MAX(6, "时区过长")));
RUVIA_MODEL(MqttPreviewPoint, RUVIA_REQUIRED_FIELD(id, ruvia::string), RUVIA_REQUIRED_FIELD(name, ruvia::string), RUVIA_REQUIRED_FIELD(value, ruvia::string), RUVIA_REQUIRED_FIELD(unit, ruvia::string));
RUVIA_MODEL(MqttPreviewRecord, RUVIA_REQUIRED_FIELD(deviceCode, ruvia::string), RUVIA_REQUIRED_FIELD(time, ruvia::string), RUVIA_REQUIRED_FIELD(points, ruvia::array<MqttPreviewPoint>), RUVIA_REQUIRED_FIELD(errors, ruvia::array<ruvia::string>));
RUVIA_MODEL(MqttPreviewResult, RUVIA_REQUIRED_FIELD(records, ruvia::array<MqttPreviewRecord>));
RUVIA_MODEL(MqttPreviewResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, MqttPreviewResult));

RUVIA_MODEL(CreateDlt645PointBody, RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "点位名称不能为空"), RUVIA_MAX(100, "点位名称过长")), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, "单位过长")), RUVIA_REQUIRED_FIELD(dataType, ruvia::string, RUVIA_ONE_OF("DL/T645 数据类型无效", "BCD", "BCD_SIGNED", "HEX")), RUVIA_REQUIRED_FIELD(identifier, ruvia::string, RUVIA_MIN(4, "数据标识 DI 无效"), RUVIA_MAX(8, "数据标识 DI 无效")), RUVIA_REQUIRED_FIELD(length, ruvia::int64, RUVIA_MIN(1, "数据长度无效"), RUVIA_MAX(200, "数据长度无效")), RUVIA_OPTIONAL_FIELD(digits, ruvia::int64, decltype(kDefaultDigits), RUVIA_MIN(0, "小数位数无效"), RUVIA_MAX(8, "小数位数无效")), RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value, decltype(kDefaultWritable)), RUVIA_OPTIONAL_FIELD(visible, ruvia::bool_value, decltype(kDefaultVisible)));

RUVIA_MODEL(CreateProtocolBody, RUVIA_REQUIRED_FIELD(protocol, ruvia::string, RUVIA_MIN(1, "协议不能为空"), RUVIA_MAX(16, "协议过长")), RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "配置名称不能为空"), RUVIA_MAX(64, "配置名称过长")), RUVIA_OPTIONAL_FIELD(remark, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(500, "remark 长度超出限制")), RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value, RUVIA_NULLABLE, decltype(kDefaultEnabled)), RUVIA_OPTIONAL_FIELD(config, ruvia::json_value, RUVIA_NULLABLE));
RUVIA_MODEL(UpdateProtocolBody, RUVIA_OPTIONAL_FIELD(protocol, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(16, "协议过长")), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(64, "配置名称过长")), RUVIA_OPTIONAL_FIELD(remark, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(500, "remark 长度超出限制")), RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(config, ruvia::json_value, RUVIA_NULLABLE));

RUVIA_MODEL(ProtocolListQuery, RUVIA_OPTIONAL_FIELD(page, ruvia::int64, decltype(kDefaultPage), RUVIA_MIN(1, "page 必须大于 0")), RUVIA_OPTIONAL_FIELD(pageSize, ruvia::int64, decltype(kDefaultPageSize), RUVIA_MIN(1, "pageSize 必须在 1 - 1000 之间"), RUVIA_MAX(1000, "pageSize 必须在 1 - 1000 之间")), RUVIA_OPTIONAL_FIELD(protocol, ruvia::string, RUVIA_ONE_OF("协议无效", "SL651", "Modbus", "S7", "MC", "FINS", "DLT645", "MQTT", "ExternalIO")));
RUVIA_MODEL(ProtocolIdParams, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(ExternalIoModelsResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, ruvia::array<ExternalIoHardwareModel>));

} // namespace service::protocol
