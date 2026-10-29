#pragma once

#include <set>
#include <stdexcept>
#include <string>

#include <ruvia/web/model.h>

namespace service::common {

RUVIA_MODEL(DeviceParameterDefinition, RUVIA_REQUIRED_FIELD(name, ruvia::string), RUVIA_REQUIRED_FIELD(dataType, ruvia::string));
RUVIA_MODEL(DeviceParameterConfiguration, RUVIA_OPTIONAL_FIELD(identityProperty, ruvia::string), RUVIA_OPTIONAL_FIELD(parameters, ruvia::array<DeviceParameterDefinition>));

inline void validateDeviceParameterDefinitions(const ruvia::array<DeviceParameterDefinition>& definitions) {
    if (definitions.size() > 256) {
        throw std::invalid_argument("最多定义 256 个固有属性");
    }
    std::set<std::string> names;
    for (const auto& definition : definitions) {
        const auto name = definition.get<"name">().view(), type = definition.get<"dataType">().view();
        if (name.empty() || name.size() > 100 || name.front() == ' ' || name.back() == ' ' ||
            name.find_first_of("\r\n\t") != std::string_view::npos || !names.emplace(name).second) {
            throw std::invalid_argument("固有属性名须唯一且为 1–100 字节，不能有首尾空格");
        }
        if (type != "STRING" && type != "DOUBLE" && type != "BOOL" && type != "JSON") {
            throw std::invalid_argument("固有属性类型无效");
        }
    }
}

inline bool deviceParameterValueMatches(std::string_view type, const ruvia::json_value& value) {
    return value.is_null() || type == "JSON" ||
        (type == "STRING" && value.get<ruvia::string>().has_value()) ||
        (type == "DOUBLE" && value.get<ruvia::double_value>().has_value()) ||
        (type == "BOOL" && value.get<ruvia::bool_value>().has_value());
}

} // namespace service::common
