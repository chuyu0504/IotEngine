#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ruvia/web/model.h>

namespace service::collector::mqtt {

RUVIA_MODEL(Connection, RUVIA_REQUIRED_FIELD(clientId, ruvia::string), RUVIA_OPTIONAL_FIELD(username, ruvia::string), RUVIA_OPTIONAL_FIELD(password, ruvia::string), RUVIA_OPTIONAL_FIELD(keepAliveSeconds, ruvia::int64));
RUVIA_MODEL(ValueMapping, RUVIA_REQUIRED_FIELD(input, ruvia::string), RUVIA_REQUIRED_FIELD(output, ruvia::string));
RUVIA_MODEL(Point, RUVIA_REQUIRED_FIELD(id, ruvia::string), RUVIA_REQUIRED_FIELD(name, ruvia::string), RUVIA_REQUIRED_FIELD(field, ruvia::string), RUVIA_REQUIRED_FIELD(dataType, ruvia::string), RUVIA_OPTIONAL_FIELD(unit, ruvia::string), RUVIA_OPTIONAL_FIELD(writable, ruvia::bool_value), RUVIA_OPTIONAL_FIELD(enumValues, ruvia::array<ValueMapping>));
RUVIA_MODEL(CommandResponse, RUVIA_REQUIRED_FIELD(topic, ruvia::string), RUVIA_REQUIRED_FIELD(matchFields, ruvia::array<ruvia::string>), RUVIA_REQUIRED_FIELD(codeField, ruvia::string), RUVIA_REQUIRED_FIELD(successCode, ruvia::int64), RUVIA_OPTIONAL_FIELD(timeoutMs, ruvia::int64));
RUVIA_MODEL(CommandMessage, RUVIA_REQUIRED_FIELD(id, ruvia::string), RUVIA_REQUIRED_FIELD(name, ruvia::string), RUVIA_REQUIRED_FIELD(topic, ruvia::string), RUVIA_REQUIRED_FIELD_NAME("template", templateText, ruvia::string), RUVIA_REQUIRED_FIELD(requiredPointIds, ruvia::array<ruvia::string>), RUVIA_OPTIONAL_FIELD(response, CommandResponse));
RUVIA_MODEL(Config, RUVIA_OPTIONAL_FIELD(topic, ruvia::string), RUVIA_OPTIONAL_FIELD(recordsPath, ruvia::string), RUVIA_OPTIONAL_FIELD(deviceCodeField, ruvia::string), RUVIA_OPTIONAL_FIELD(identitySource, ruvia::string), RUVIA_OPTIONAL_FIELD(topicDeviceSegment, ruvia::int64), RUVIA_OPTIONAL_FIELD(payloadFormat, ruvia::string), RUVIA_OPTIONAL_FIELD(delimiter, ruvia::string), RUVIA_OPTIONAL_FIELD(recordDelimiter, ruvia::string), RUVIA_OPTIONAL_FIELD(recordLength, ruvia::int64), RUVIA_OPTIONAL_FIELD(timeField, ruvia::string), RUVIA_OPTIONAL_FIELD(timeFormat, ruvia::string), RUVIA_OPTIONAL_FIELD(reportTemplate, ruvia::string), RUVIA_OPTIONAL_FIELD(commandTopic, ruvia::string), RUVIA_OPTIONAL_FIELD(commandTemplate, ruvia::string), RUVIA_OPTIONAL_FIELD(commands, ruvia::array<CommandMessage>), RUVIA_OPTIONAL_FIELD(qos, ruvia::int64), RUVIA_REQUIRED_FIELD(points, ruvia::array<Point>));

} // namespace service::collector::mqtt
