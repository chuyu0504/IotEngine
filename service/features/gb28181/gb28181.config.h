#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <ruvia/web/dotenv.h>

namespace service::gb28181 {

struct SipConfig {
    std::string domain;
    std::string id;
    std::string host{ "0.0.0.0" };
    std::string publicIp;
    std::uint16_t port{ 5060 };
    std::string password;
    std::string transport{ "udp" };
    int registrationTimeoutSeconds{ 180 };
    int inviteTimeoutSeconds{ 15 };
    int viewerLeaseTimeoutSeconds{ 90 };
    int nonceTtlSeconds{ 300 };
    bool logging{ true };
};

struct MediaConfig {
    std::string zlmPublicBaseUrl;
    std::string rtpPublicIp;
    std::string playTokenSecret;
    int playTokenTtlSeconds{ 300 };
    std::string corsOrigin;
    int workerThreads{ 1 };
    int logLevel{ 2 };
    std::uint16_t httpPort{ 8080 };
    std::uint16_t httpsPort{ 8443 };
    std::uint16_t rtspPort{ 8554 };
    std::uint16_t rtspsPort{ 8322 };
    std::uint16_t rtmpPort{ 1935 };
    std::uint16_t rtmpsPort{ 1936 };
    std::uint16_t rtcPort{ 8000 };
    std::uint16_t srtPort{ 9000 };
    std::uint16_t rtpPortRangeStart{ 30000 };
    std::uint16_t rtpPortRangeEnd{ 30500 };
    bool tlsEnabled{ false };
    std::string tlsPemPath;
    std::string tlsPassword;
    bool recordingEnabled{ false };
    std::string recordRoot;
    std::uint32_t recordMaxSegmentSeconds{ 3600 };
};

struct Config {
    bool enabled{ false };
    SipConfig sip;
    MediaConfig media;

    // Every GB28181 setting is read here; an absent variable keeps the
    // member default above.
    static Config fromEnv(const ruvia::env& env) {
        Config config;
        assign(config.enabled, flag(env, "GB28181_ENABLED"));
        assign(config.sip.domain, env.get("GB28181_SIP_DOMAIN"));
        assign(config.sip.id, env.get("GB28181_SIP_ID"));
        assign(config.sip.host, env.get("GB28181_SIP_HOST"));
        assign(config.sip.publicIp, env.get("GB28181_SIP_PUBLIC_IP"));
        assign(config.sip.port, env.get<std::uint16_t>("GB28181_SIP_PORT"));
        assign(config.sip.password, env.get("GB28181_SIP_PASSWORD"));
        assign(config.sip.transport, env.get("GB28181_SIP_TRANSPORT"));
        assign(config.sip.registrationTimeoutSeconds, env.get<int>("GB28181_REGISTRATION_TIMEOUT_SECONDS"));
        assign(config.sip.inviteTimeoutSeconds, env.get<int>("GB28181_INVITE_TIMEOUT_SECONDS"));
        assign(config.sip.viewerLeaseTimeoutSeconds, env.get<int>("GB28181_VIEWER_LEASE_TIMEOUT_SECONDS"));
        assign(config.sip.nonceTtlSeconds, env.get<int>("GB28181_NONCE_TTL_SECONDS"));
        assign(config.sip.logging, flag(env, "GB28181_SIP_LOGGING"));
        assign(config.media.zlmPublicBaseUrl, env.get("ZLM_PUBLIC_BASE_URL"));
        assign(config.media.rtpPublicIp, env.get("GB28181_RTP_PUBLIC_IP"));
        assign(config.media.playTokenSecret, env.get("GB28181_MEDIA_TOKEN_SECRET"));
        assign(config.media.playTokenTtlSeconds, env.get<int>("GB28181_MEDIA_TOKEN_TTL_SECONDS"));
        assign(config.media.corsOrigin, env.get("GB28181_MEDIA_CORS_ORIGIN"));
        assign(config.media.workerThreads, env.get<int>("ZLM_WORKER_THREADS"));
        assign(config.media.logLevel, env.get<int>("ZLM_LOG_LEVEL"));
        assign(config.media.httpPort, env.get<std::uint16_t>("ZLM_HTTP_PORT"));
        assign(config.media.httpsPort, env.get<std::uint16_t>("ZLM_HTTPS_PORT"));
        assign(config.media.rtspPort, env.get<std::uint16_t>("ZLM_RTSP_PORT"));
        assign(config.media.rtspsPort, env.get<std::uint16_t>("ZLM_RTSPS_PORT"));
        assign(config.media.rtmpPort, env.get<std::uint16_t>("ZLM_RTMP_PORT"));
        assign(config.media.rtmpsPort, env.get<std::uint16_t>("ZLM_RTMPS_PORT"));
        assign(config.media.rtcPort, env.get<std::uint16_t>("ZLM_RTC_PORT"));
        assign(config.media.srtPort, env.get<std::uint16_t>("ZLM_SRT_PORT"));
        assign(config.media.rtpPortRangeStart, env.get<std::uint16_t>("GB28181_RTP_PORT_START"));
        assign(config.media.rtpPortRangeEnd, env.get<std::uint16_t>("GB28181_RTP_PORT_END"));
        assign(config.media.tlsEnabled, flag(env, "ZLM_TLS_ENABLED"));
        assign(config.media.tlsPemPath, env.get("ZLM_TLS_PEM_PATH"));
        assign(config.media.tlsPassword, env.get("ZLM_TLS_PASSWORD"));
        assign(config.media.recordingEnabled, flag(env, "GB28181_RECORDING_ENABLED"));
        assign(config.media.recordRoot, env.get("GB28181_RECORD_ROOT"));
        assign(config.media.recordMaxSegmentSeconds, env.get<std::uint32_t>("GB28181_RECORD_MAX_SEGMENT_SECONDS"));
        return config;
    }

  private:
    template <typename Value, typename Source>
    static void assign(Value& target, const std::optional<Source>& value) {
        if (value) {
            target = Value(*value);
        }
    }

    // 1/true/yes/on enable, any other present value disables.
    static std::optional<bool> flag(const ruvia::env& env, std::string_view name) {
        const auto value = env.get(name);
        if (!value) {
            return std::nullopt;
        }
        std::string normalized(*value);
        std::ranges::transform(normalized, normalized.begin(), [](unsigned char character) {
            return static_cast<char>(std::tolower(character));
        });
        return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on";
    }
};

} // namespace service::gb28181
