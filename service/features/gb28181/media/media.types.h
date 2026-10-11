#pragma once

#include <cstdint>
#include <string>

#include <ruvia/web/model.h>

struct StreamStatus {
    std::string app;
    std::string stream;
    std::string schema;
    bool online{false};
    int readerCount{0};

    static std::string identity(const std::string& app, const std::string& stream,
                                const std::string& schema) {
        return app + ":" + schema + ":" + stream;
    }
};

struct PlayUrls {
    std::string httpFlv;
    std::string wsFlv;
    std::string httpTs;
    std::string hls;
    std::string webRtc;
    std::string rtsp;
    std::string rtmp;
};

struct OpenRtpServerResult {
    std::string streamId;
    uint16_t port{0};
    PlayUrls playUrls;
};

RUVIA_MODEL(WebRtcResponseModel,
    RUVIA_OPTIONAL_FIELD(code, ruvia::int64),
    RUVIA_OPTIONAL_FIELD(type, ruvia::string),
    RUVIA_OPTIONAL_FIELD(sdp, ruvia::string),
    RUVIA_OPTIONAL_FIELD(msg, ruvia::string));

struct MediaServerPorts {
    std::uint16_t http{ 0 };
    std::uint16_t https{ 0 };
    std::uint16_t rtsp{ 0 };
    std::uint16_t rtsps{ 0 };
    std::uint16_t rtmp{ 0 };
    std::uint16_t rtmps{ 0 };
    std::uint16_t rtc{ 0 };
    std::uint16_t srt{ 0 };
};

struct MediaCapabilities {
    bool faac{ true };
    bool ffmpeg{ true };
    bool hls{ true };
    bool mp4{ true };
    bool rtpProxy{ true };
    bool srt{ true };
    bool sctp{ true };
    bool webRtc{ true };
    bool x264{ true };
    bool videoStack{ true };
    bool tls{ false };
    bool recording{ false };
};
