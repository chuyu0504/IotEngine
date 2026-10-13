#pragma once

#include "service/common/uuid.h"
#include <ruvia/web/validation.h>
#include <ruvia/web/model_object.h>

namespace service::protocol {

inline constexpr auto kDefaultDigits = RUVIA_DEFAULT(0){};
inline constexpr auto kDefaultWritable = RUVIA_DEFAULT(false){};
inline constexpr auto kDefaultVisible = RUVIA_DEFAULT(true){};
inline constexpr auto kDefaultEnabled = RUVIA_DEFAULT(true){};
inline constexpr auto kDefaultPage = RUVIA_DEFAULT(1){};
inline constexpr auto kDefaultPageSize = RUVIA_DEFAULT(10){};

RUVIA_MODEL(ExpressionTestInput,
    RUVIA_REQUIRED_FIELD(alias, ruvia::string, RUVIA_MIN(1, "变量名不能为空"), RUVIA_MAX(32, "变量名过长")),
    RUVIA_REQUIRED_FIELD(value, ruvia::double_value));
RUVIA_MODEL(ExpressionTestUnitRule,
    RUVIA_REQUIRED_FIELD(condition, ruvia::string, RUVIA_MIN(1, "单位条件不能为空"), RUVIA_MAX(512, "单位条件过长")),
    RUVIA_REQUIRED_FIELD(unit, ruvia::string, RUVIA_MAX(32, "单位过长")));
RUVIA_MODEL(ExpressionTestBody,
    RUVIA_REQUIRED_FIELD(expression, ruvia::string, RUVIA_MIN(1, "公式不能为空"), RUVIA_MAX(512, "公式过长")),
    RUVIA_REQUIRED_FIELD(inputs, ruvia::array<ExpressionTestInput>, RUVIA_MAX(16, "最多 16 个变量")),
    RUVIA_REQUIRED_FIELD(unit, ruvia::string, RUVIA_MAX(32, "单位过长")),
    RUVIA_REQUIRED_FIELD(unitRules, ruvia::array<ExpressionTestUnitRule>, RUVIA_MAX(8, "最多 8 条单位条件")));
RUVIA_MODEL(ExpressionTestResult,
    RUVIA_REQUIRED_FIELD(value, ruvia::double_value),
    RUVIA_REQUIRED_FIELD(unit, ruvia::string),
    RUVIA_REQUIRED_FIELD(matchedRule, ruvia::int64));
RUVIA_MODEL(ExpressionTestResponse,
    RUVIA_REQUIRED_FIELD(code, ruvia::int64),
    RUVIA_REQUIRED_FIELD(message, ruvia::string),
    RUVIA_REQUIRED_FIELD(data, ExpressionTestResult));

RUVIA_MODEL(Sl651Element,
    RUVIA_OPTIONAL_FIELD(positionMode, ruvia::string),
    RUVIA_OPTIONAL_FIELD(byteOffset, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(length, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(digits, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(encode, ruvia::string),
    RUVIA_OPTIONAL_FIELD(guideHex, ruvia::string));
RUVIA_MODEL(Sl651Func,
    RUVIA_OPTIONAL_FIELD(funcCode, ruvia::string),
    RUVIA_OPTIONAL_FIELD(dir, ruvia::string),
    RUVIA_OPTIONAL_FIELD(elements, ruvia::array<Sl651Element>),
    RUVIA_OPTIONAL_FIELD(responseElements, ruvia::array<Sl651Element>));
RUVIA_MODEL(ModbusPacket,
    RUVIA_OPTIONAL_FIELD(mergeGap, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(maxQuantity, ruvia::json_value));
RUVIA_MODEL(ModbusRegister,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(registerType, ruvia::string),
    RUVIA_OPTIONAL_FIELD(dataType, ruvia::string),
    RUVIA_OPTIONAL_FIELD(address, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(quantity, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(byteOrder, ruvia::string),
    RUVIA_OPTIONAL_FIELD(scale, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value));
RUVIA_MODEL(S7Connection,
    RUVIA_OPTIONAL_FIELD(probeMode, ruvia::string),
    RUVIA_OPTIONAL_FIELD(handshakeTimeout, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(directProbeTimeout, ruvia::json_value));
RUVIA_MODEL(S7Area,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(area, ruvia::string),
    RUVIA_OPTIONAL_FIELD(dataType, ruvia::string),
    RUVIA_OPTIONAL_FIELD(start, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(size, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(startBit, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(dbNumber, ruvia::json_value));
RUVIA_MODEL(IndustrialConnection,
    RUVIA_OPTIONAL_FIELD(frame, ruvia::string),
    RUVIA_OPTIONAL_FIELD(network, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(station, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(moduleIo, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(multidrop, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(monitoringTimer, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(destinationNetwork, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(sourceNetwork, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(destinationNode, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(sourceNode, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(destinationUnit, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(sourceUnit, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(version, ruvia::string),
    RUVIA_OPTIONAL_FIELD(wakeupBytes, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(writePassword, ruvia::string),
    RUVIA_OPTIONAL_FIELD(operatorCode, ruvia::string));
RUVIA_MODEL(IndustrialPoint,
    RUVIA_OPTIONAL_FIELD(id, ruvia::string),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string),
    RUVIA_OPTIONAL_FIELD(dataType, ruvia::string),
    RUVIA_OPTIONAL_FIELD(unit, ruvia::string),
    RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(identifier, ruvia::string),
    RUVIA_OPTIONAL_FIELD(length, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(digits, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(area, ruvia::string),
    RUVIA_OPTIONAL_FIELD(byteOrder, ruvia::string),
    RUVIA_OPTIONAL_FIELD(scale, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(address, ruvia::json_value),
    RUVIA_OPTIONAL_FIELD(bit, ruvia::json_value));

// 顶层 config 保留未知键、精确数字 token 和 PATCH 合并；各协议固定嵌套结构用 Model。
RUVIA_MODEL(MqttValueMapping, RUVIA_REQUIRED_FIELD(input, ruvia::string), RUVIA_REQUIRED_FIELD(output, ruvia::string));
RUVIA_MODEL(MqttPoint,
    RUVIA_REQUIRED_FIELD(id, ruvia::string),
    RUVIA_REQUIRED_FIELD(name, ruvia::string),
    RUVIA_REQUIRED_FIELD(field, ruvia::string),
    RUVIA_REQUIRED_FIELD(dataType, ruvia::string),
    RUVIA_OPTIONAL_FIELD(unit, ruvia::string),
    RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(scale, ruvia::double_value),
    RUVIA_OPTIONAL_FIELD(offset, ruvia::double_value),
    RUVIA_OPTIONAL_FIELD(enumValues, ruvia::array<MqttValueMapping>));
RUVIA_MODEL(MqttDeviceParameter, RUVIA_REQUIRED_FIELD(deviceCode, ruvia::string), RUVIA_REQUIRED_FIELD(name, ruvia::string), RUVIA_REQUIRED_FIELD(value, ruvia::json_value));
RUVIA_MODEL(MqttCommandResponse,
    RUVIA_REQUIRED_FIELD(topic, ruvia::string), RUVIA_REQUIRED_FIELD(matchFields, ruvia::array<ruvia::string>),
    RUVIA_REQUIRED_FIELD(codeField, ruvia::string), RUVIA_REQUIRED_FIELD(successCode, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(timeoutMs, ruvia::int64));
RUVIA_MODEL(MqttCommandMessage,
    RUVIA_REQUIRED_FIELD(id, ruvia::string), RUVIA_REQUIRED_FIELD(name, ruvia::string),
    RUVIA_REQUIRED_FIELD(topic, ruvia::string), RUVIA_REQUIRED_FIELD_NAME("template", templateText, ruvia::string),
    RUVIA_REQUIRED_FIELD(requiredPointIds, ruvia::array<ruvia::string>), RUVIA_OPTIONAL_FIELD(response, MqttCommandResponse));
RUVIA_MODEL(MqttConfig,
    RUVIA_OPTIONAL_FIELD(storagePolicy, ruvia::string),
    RUVIA_OPTIONAL_FIELD(topic, ruvia::string),
    RUVIA_OPTIONAL_FIELD(recordsPath, ruvia::string),
    RUVIA_OPTIONAL_FIELD(deviceCodeField, ruvia::string),
    RUVIA_OPTIONAL_FIELD(identitySource, ruvia::string),
    RUVIA_OPTIONAL_FIELD(topicDeviceSegment, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(payloadFormat, ruvia::string),
    RUVIA_OPTIONAL_FIELD(delimiter, ruvia::string),
    RUVIA_OPTIONAL_FIELD(recordDelimiter, ruvia::string),
    RUVIA_OPTIONAL_FIELD(recordLength, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(timeField, ruvia::string),
    RUVIA_OPTIONAL_FIELD(timeFormat, ruvia::string),
    RUVIA_OPTIONAL_FIELD(reportTemplate, ruvia::string),
    RUVIA_OPTIONAL_FIELD(commandTopic, ruvia::string), RUVIA_OPTIONAL_FIELD(commandTemplate, ruvia::string),
    RUVIA_OPTIONAL_FIELD(commands, ruvia::array<MqttCommandMessage>),
    RUVIA_OPTIONAL_FIELD(deviceParameters, ruvia::array<MqttDeviceParameter>),
    RUVIA_OPTIONAL_FIELD(qos, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(points, ruvia::array<MqttPoint>));

RUVIA_MODEL(MqttPreviewBody,
    RUVIA_REQUIRED_FIELD(config, MqttConfig),
    RUVIA_REQUIRED_FIELD(topic, ruvia::string, RUVIA_MIN(1, "Topic 不能为空"), RUVIA_MAX(1024, "Topic 过长")),
    RUVIA_REQUIRED_FIELD(payload, ruvia::string, RUVIA_MAX(2097152, "示例负载过长")),
    RUVIA_OPTIONAL_FIELD(deviceCode, ruvia::string, RUVIA_MAX(128, "设备编码过长")),
    RUVIA_OPTIONAL_FIELD(timezone, ruvia::string, RUVIA_MAX(6, "时区过长")));
RUVIA_MODEL(MqttPreviewPoint,
    RUVIA_REQUIRED_FIELD(id, ruvia::string), RUVIA_REQUIRED_FIELD(name, ruvia::string),
    RUVIA_REQUIRED_FIELD(value, ruvia::string), RUVIA_REQUIRED_FIELD(unit, ruvia::string));
RUVIA_MODEL(MqttPreviewRecord,
    RUVIA_REQUIRED_FIELD(deviceCode, ruvia::string), RUVIA_REQUIRED_FIELD(time, ruvia::string),
    RUVIA_REQUIRED_FIELD(points, ruvia::array<MqttPreviewPoint>), RUVIA_REQUIRED_FIELD(errors, ruvia::array<ruvia::string>));
RUVIA_MODEL(MqttPreviewResult, RUVIA_REQUIRED_FIELD(records, ruvia::array<MqttPreviewRecord>));
RUVIA_MODEL(MqttPreviewResponse, RUVIA_REQUIRED_FIELD(code, ruvia::int64), RUVIA_REQUIRED_FIELD(message, ruvia::string), RUVIA_REQUIRED_FIELD(data, MqttPreviewResult));

RUVIA_MODEL(CreateDlt645PointBody,
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "点位名称不能为空"), RUVIA_MAX(100, "点位名称过长")),
    RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_MAX(32, "单位过长")),
    RUVIA_REQUIRED_FIELD(dataType, ruvia::string, RUVIA_ONE_OF("DL/T645 数据类型无效", "BCD", "BCD_SIGNED", "HEX")),
    RUVIA_REQUIRED_FIELD(identifier, ruvia::string, RUVIA_MIN(4, "数据标识 DI 无效"), RUVIA_MAX(8, "数据标识 DI 无效")),
    RUVIA_REQUIRED_FIELD(length, ruvia::int64, RUVIA_MIN(1, "数据长度无效"), RUVIA_MAX(200, "数据长度无效")),
    RUVIA_OPTIONAL_FIELD(digits, ruvia::int64, decltype(kDefaultDigits), RUVIA_MIN(0, "小数位数无效"), RUVIA_MAX(8, "小数位数无效")),
    RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value, decltype(kDefaultWritable)),
    RUVIA_OPTIONAL_FIELD(visible, ruvia::bool_value, decltype(kDefaultVisible)));

RUVIA_MODEL(CreateProtocolBody,
    RUVIA_REQUIRED_FIELD(protocol, ruvia::string, RUVIA_MIN(1, "协议不能为空"), RUVIA_MAX(16, "协议过长")),
    RUVIA_REQUIRED_FIELD(name, ruvia::string, RUVIA_MIN(1, "配置名称不能为空"), RUVIA_MAX(64, "配置名称过长")),
    RUVIA_OPTIONAL_FIELD(remark, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(500, "remark 长度超出限制")),
    RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value, RUVIA_NULLABLE, decltype(kDefaultEnabled)),
    RUVIA_OPTIONAL_FIELD(config, ruvia::json_value, RUVIA_NULLABLE));
RUVIA_MODEL(UpdateProtocolBody,
    RUVIA_OPTIONAL_FIELD(protocol, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(16, "协议过长")),
    RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(64, "配置名称过长")),
    RUVIA_OPTIONAL_FIELD(remark, ruvia::string, RUVIA_NULLABLE, RUVIA_MAX(500, "remark 长度超出限制")),
    RUVIA_OPTIONAL_FIELD(enabled, ruvia::bool_value, RUVIA_NULLABLE),
    RUVIA_OPTIONAL_FIELD(config, ruvia::json_value, RUVIA_NULLABLE));

RUVIA_MODEL(ProtocolListQuery,
    RUVIA_OPTIONAL_FIELD(page, ruvia::int64, decltype(kDefaultPage), RUVIA_MIN(1, "page 必须大于 0")),
    RUVIA_OPTIONAL_FIELD(pageSize, ruvia::int64, decltype(kDefaultPageSize), RUVIA_MIN(1, "pageSize 必须在 1 - 1000 之间"), RUVIA_MAX(1000, "pageSize 必须在 1 - 1000 之间")),
    RUVIA_OPTIONAL_FIELD(protocol, ruvia::string, RUVIA_ONE_OF("协议无效", "SL651", "Modbus", "S7", "MC", "FINS", "DLT645", "MQTT")));
RUVIA_MODEL(ProtocolIdParams,
    RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

} // namespace service::protocol
