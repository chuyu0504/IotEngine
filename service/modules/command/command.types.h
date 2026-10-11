#pragma once

#include <ruvia/web/model.h>

#include "service/modules/device/device.types.h"

namespace service::command {

RUVIA_MODEL(SubmitCommandBody, RUVIA_OPTIONAL_FIELD(mqttMessageId, ruvia::string, RUVIA_CUSTOM("控制消息 ID 必须是 UUID", service::common::isUuidField)), RUVIA_OPTIONAL_FIELD_NAME("idempotency_key", idempotencyKey, ruvia::string), RUVIA_OPTIONAL_FIELD_NAME("deviceId", deviceId, ruvia::string), RUVIA_REQUIRED_FIELD(elements, ruvia::array<service::device::DeviceCommandElementBody>, RUVIA_MIN(1, "请至少选择一个下发要素"), RUVIA_MAX(256, "单次最多下发 256 个要素")));

RUVIA_MODEL(CommandStatusQuery, RUVIA_REQUIRED_FIELD(id, ruvia::string, RUVIA_CUSTOM("id 必须是 UUID", service::common::isUuidField)));

RUVIA_MODEL(PreparedElement, RUVIA_OPTIONAL_FIELD(elementId, ruvia::string), RUVIA_OPTIONAL_FIELD(value, ruvia::string));

RUVIA_MODEL(PreparedOperation, RUVIA_OPTIONAL_FIELD(id, ruvia::string), RUVIA_OPTIONAL_FIELD(deviceId, ruvia::string), RUVIA_OPTIONAL_FIELD(deviceCode, ruvia::string), RUVIA_OPTIONAL_FIELD(protocol, ruvia::string), RUVIA_OPTIONAL_FIELD(elements, ruvia::boxed_array<PreparedElement>), RUVIA_OPTIONAL_FIELD(payload, ruvia::array<ruvia::string>));

RUVIA_MODEL(PreparedBatch, RUVIA_OPTIONAL_FIELD(queue, ruvia::string), RUVIA_OPTIONAL_FIELD(kind, ruvia::string), RUVIA_OPTIONAL_FIELD(maximum, ruvia::int64), RUVIA_OPTIONAL_FIELD(nodeId, ruvia::string), RUVIA_OPTIONAL_FIELD(commands, ruvia::boxed_array<PreparedOperation>));

} // namespace service::command
