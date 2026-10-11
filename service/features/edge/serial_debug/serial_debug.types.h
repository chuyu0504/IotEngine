#pragma once
#include <ruvia/web/model.h>

namespace service::edge::serial_debug {
RUVIA_MODEL(BrowserRequest,
    RUVIA_OPTIONAL_FIELD(action, ruvia::string),
    RUVIA_OPTIONAL_FIELD(requestId, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(baudRate, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(dataBits, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(stopBits, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(parity, ruvia::string),
    RUVIA_OPTIONAL_FIELD(rs485, ruvia::bool_value),
    RUVIA_OPTIONAL_FIELD(hex, ruvia::string));

} // namespace service::edge::serial_debug
