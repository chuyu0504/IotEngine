#pragma once

#include <cstdint>
#include <string>

struct SipConfig {
    std::string domain;
    std::string id;
    std::string host{"0.0.0.0"};
    std::string publicIp;
    std::uint16_t port{5060};
    std::string password;
    std::string transport{"udp"};
    int registrationTimeoutSeconds{180};
    int inviteTimeoutSeconds{15};
    int viewerLeaseTimeoutSeconds{90};
    int nonceTtlSeconds{300};
    bool logging{true};
};

struct MediaConfig {
    std::string zlmPublicBaseUrl;
    std::string rtpPublicIp;
    std::string playTokenSecret;
    int playTokenTtlSeconds{300};
    std::string corsOrigin;
    int workerThreads{1};
    int logLevel{2};
    std::uint16_t httpPort{8080};
    std::uint16_t httpsPort{8443};
    std::uint16_t rtspPort{8554};
    std::uint16_t rtspsPort{8322};
    std::uint16_t rtmpPort{1935};
    std::uint16_t rtmpsPort{1936};
    std::uint16_t rtcPort{8000};
    std::uint16_t srtPort{9000};
    std::uint16_t rtpPortRangeStart{30000};
    std::uint16_t rtpPortRangeEnd{30500};
    bool tlsEnabled{false};
    std::string tlsPemPath;
    std::string tlsPassword;
    bool recordingEnabled{false};
    std::string recordRoot;
    std::uint32_t recordMaxSegmentSeconds{3600};
};

struct AppConfig {
    bool enabled{false};
    SipConfig sip;
    MediaConfig media;
};
