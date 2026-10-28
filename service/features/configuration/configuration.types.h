#pragma once

#include <string>
#include <vector>

#include <ruvia/web/model.h>

#include "service/common/derived_point.h"
#include "service/features/collector/collector.types.h"

namespace service::configuration {

// 数据库中的协议配置保留未知字段；这里仅解码两种执行目标实际消费的字段。
RUVIA_MODEL(ElementConfig, RUVIA_OPTIONAL_FIELD(id, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(unit, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(dataType, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(byteOrder, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(registerType, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(address, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(quantity, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(decimals, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(writable, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(area, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(dbNumber, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(start, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(startBit, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(size, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(bit, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(identifier, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(encode, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(guideHex, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(length, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(digits, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(positionMode, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(byteOffset, ruvia::json_value, RUVIA_NULLABLE));

RUVIA_MODEL(FunctionConfig, RUVIA_OPTIONAL_FIELD(funcCode, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(name, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(dir, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(elements, ruvia::array<ElementConfig>, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(responseElements, ruvia::array<ElementConfig>, RUVIA_NULLABLE));

RUVIA_MODEL(EndpointConfig, RUVIA_OPTIONAL_FIELD(transport, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD_NAME("interface", interfaceName, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(mode, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(ip, ruvia::string, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(port, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(baud_rate, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(data_bits, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(stop_bits, ruvia::json_value, RUVIA_NULLABLE), RUVIA_OPTIONAL_FIELD(parity, ruvia::string, RUVIA_NULLABLE));

struct EndpointDefinition {
    std::string name;
    std::string transport;
    std::string interfaceName;
    std::string mode;
    std::string ip;
    std::int64_t port = 0;
    std::int64_t baudRate = 9600;
    std::int64_t dataBits = 8;
    std::int64_t stopBits = 1;
    std::string parity = "none";
    bool enabled = false;
    bool debugEnabled = false;
};

struct FunctionDefinition {
    std::string code;
    std::string name;
    std::string direction;
};

struct ExternalIoPoint {
    std::string id;
    std::string channel;
    std::string kind;
    std::string name;
    std::string unit;
    bool visible = true;
};

struct DerivedConfiguration {
    std::string config;
    std::vector<std::string> sources;
    std::string snapshotDigest;

    std::vector<std::string_view> sourceViews() const {
        return { sources.begin(), sources.end() };
    }
};

struct DeviceModel {
    service::collector::DeviceDefinition device;
    EndpointDefinition endpoint;
    bool enabled = false;
    std::string execution;
    bool available = true;
    std::vector<service::message::realtime::Point> points;
    std::vector<service::common::DerivedPoint> derivedPoints;
    std::vector<FunctionDefinition> functions;
    std::string externalIoModelId;
    std::string externalIoConfig;
    std::string deviceDerivedConfig;
    DerivedConfiguration calculation;
    std::vector<ExternalIoPoint> externalIo;
};

} // namespace service::configuration
