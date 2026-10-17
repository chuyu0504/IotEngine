#pragma once

#include <cctype>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <ruvia/web/dotenv.h>

#include "service/common/message.h"
#include "service/common/uuid.h"

namespace service::edge::config {

inline bool validPlatformId(std::string_view platformId) {
    std::uint8_t value[16]{};
    if (!service::common::uuidBytes(platformId, value)) {
        return false;
    }
    bool nonzero = false;
    for (const auto byte : value) {
        nonzero = nonzero || byte != 0;
    }
    if (!nonzero) {
        return false;
    }
    return true;
}

struct PlatformIdentity final {
    const std::string id;

    explicit PlatformIdentity(std::string value) : id(std::move(value)) {
        if (!validPlatformId(id)) {
            throw std::invalid_argument("EDGE_PLATFORM_ID is invalid");
        }
    }
};

inline bool validPublicBaseUrl(std::string_view publicBaseUrl) {
    const std::size_t schemeSize = publicBaseUrl.starts_with("https://")
        ? 8
        : publicBaseUrl.starts_with("http://") ? 7
                                               : 0;
    if (publicBaseUrl.size() > 255 || schemeSize == 0 ||
        publicBaseUrl.size() <= schemeSize || publicBaseUrl[schemeSize] == '/') {
        return false;
    }
    for (const unsigned char character : publicBaseUrl) {
        if (std::iscntrl(character) || std::isspace(character)) {
            return false;
        }
    }
    return true;
}

struct Config final {
    std::string platformId;
    std::string publicBaseUrl;
    std::filesystem::path firmwareDirectory;

    static Config fromEnv(const ruvia::env& env) {
        Config config{
            std::string(env.get("EDGE_PLATFORM_ID").value_or(service::message::edge::kDefaultPlatformId)),
            std::string(env.get("EDGE_PUBLIC_BASE_URL").value_or(service::message::edge::kDefaultPublicBaseUrl)),
            std::filesystem::path(env.get("EDGE_FIRMWARE_DIR").value_or("firmware"))
        };
        if (!validPlatformId(config.platformId)) {
            throw std::invalid_argument("EDGE_PLATFORM_ID is invalid");
        }
        if (!validPublicBaseUrl(config.publicBaseUrl)) {
            throw std::invalid_argument("EDGE_PUBLIC_BASE_URL is invalid");
        }
        return config;
    }
};

} // namespace service::edge::config
