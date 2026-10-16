#pragma once

#include <cstdint>
#include <string>

#include "service/features/gb28181/media/media.types.h"

namespace service::gb28181::sip {

struct SipPreviewStartResult {
    std::string sessionId;
    std::string deviceId;
    std::string channelId;
    std::string streamId;
    std::string ssrc;
    uint16_t rtpPort{ 0 };
    media::PlayUrls playUrls;
    unsigned int leaseTimeoutSeconds{ 0 };
};

struct SipPreviewStopResult {
    std::string sessionId;
    std::string streamId;
    bool byeSent{ false };
    bool rtpServerClosed{ false };
};

} // namespace service::gb28181::sip
