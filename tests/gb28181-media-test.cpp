#include <array>
#include <asio/ip/tcp.hpp>
#include <asio/ip/udp.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include <ruvia/core/event_loop_pool.h>

#include "service/common/uuid.h"
#include "service/features/gb28181/media/media.transport.h"
#include "service/features/packet_log/packet_log.transport.h"
#include "service/middleware/log.h"

#include "tests/port_reservation.fixture.h"

namespace {

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

template <typename Predicate>
bool waitUntil(Predicate predicate, std::chrono::milliseconds timeout = std::chrono::seconds(3)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return predicate();
}

// ZLMediaKit binds RTC UDP and then TCP on the same number; give it a port free
// for both protocols outside the ephemeral range.
std::uint16_t reserveRtcPort() {
    return test::reservePort(asio::ip::address_v4::any(), true);
}

using service::gb28181::MediaConfig;
using service::gb28181::media::OpenRtpServerResult;
using service::gb28181::media::SdkSupervisor;
using service::gb28181::media::sdkSupervisor;
using service::gb28181::media::ZlmSdk;

using MediaHandle = std::unique_ptr<std::remove_pointer_t<mk_media>, decltype(&mk_media_release)>;

MediaHandle publishPcm(const char* app, const std::string& stream) {
    MediaHandle media(mk_media_create("__defaultVhost__", app, stream.c_str(), 0, 0, 0), &mk_media_release);
    require(media != nullptr, "callback media source was not created");
    require(mk_media_init_audio(media.get(), MKCodecAAC, 8000, 1, 16) == 1, "callback media audio initialization failed");
    mk_media_init_complete(media.get());
    std::array<std::int16_t, 1024> pcm{};
    for (std::uint64_t frame = 0; frame < 8; ++frame) {
        (void)mk_media_input_pcm(media.get(), pcm.data(), static_cast<int>(pcm.size() * sizeof(pcm[0])), frame * 128 + 1);
    }
    return media;
}

ruvia::task<OpenRtpServerResult> openOwnedRtp(std::shared_ptr<ZlmSdk> sdk, std::size_t owner) {
    service::common::UuidV7Generator uuidGenerator;
    ZlmSdk::OwnerScope scope(owner);
    auto result = sdk->openRtpServer("callback-device", "channel", "2000000001", uuidGenerator.next());
    require(result.has_value(), "owned callback test RTP server did not open");
    co_return std::move(*result);
}

ruvia::task<void> closeOwnedRtp(std::shared_ptr<ZlmSdk> sdk, std::size_t owner, std::string stream) {
    ZlmSdk::OwnerScope scope(owner);
    (void)sdk->closeRtpServer(stream);
    sdkSupervisor().unregisterCollector(owner);
    co_return;
}

// Every SDK callback carries the owner recorded when its stream was opened and
// is delivered only on that Collector's loop; unowned streams reach no Collector.
void verifySdkCallbackOwners(const MediaConfig& config) {
    std::array<std::atomic_int, 2> received{};
    std::atomic_int crossed{ 0 };
    std::array<std::string, 2> expected;
    ruvia::event_loop_pool pool({ .loop_count_ = 2, .queue_capacity_ = 64 });
    sdkSupervisor().configure(config);
    sdkSupervisor().start();
    pool.start();
    try {
        std::array<std::shared_ptr<ZlmSdk>, 2> handles;
        for (std::size_t index = 0; index < 2; ++index) {
            const auto loop = pool.loop(index);
            SdkSupervisor::CollectorCallbacks callbacks;
            callbacks.onStreamChanged = [&, index, loop](std::string, std::string stream, std::string, bool online, int) {
                if (!loop.is_current() || stream != expected[index]) {
                    ++crossed;
                }
                if (online) {
                    ++received[index];
                }
            };
            handles[index] = sdkSupervisor().registerCollector(index, loop, std::move(callbacks));
            require(handles[index] && handles[index]->started(), "registered Collector did not receive a started SDK handle");
        }
        std::array<OpenRtpServerResult, 2> opened;
        for (std::size_t index = 0; index < 2; ++index) {
            opened[index] = pool.loop(index).start(openOwnedRtp(handles[index], index)).get();
            expected[index] = opened[index].streamId;
        }
        std::vector<MediaHandle> sources;
        sources.push_back(publishPcm("rtp", "unowned-callback-stream"));
        for (std::size_t index = 0; index < 2; ++index) {
            sources.push_back(publishPcm("rtp", opened[index].streamId));
        }
        require(waitUntil([&] {
                    return received[0].load() > 0 && received[1].load() > 0;
                }),
                "SDK callbacks did not return to both originating Collectors");
        require(crossed.load() == 0, "SDK callback crossed Collector ownership or delivered an unowned stream");
        sources.clear();
        for (std::size_t index = 0; index < 2; ++index) {
            pool.loop(index).start(closeOwnedRtp(handles[index], index, opened[index].streamId)).get();
        }
        sdkSupervisor().stop();
        require(!handles[0]->started(), "SDK handle still reports started after supervisor stop");
        require(!handles[0]->openRtpServer("after-stop", "channel", "2000000001", "session").has_value(), "stopped SDK handle still opened an RTP server");
        pool.stop();
        pool.join();
    } catch (...) {
        for (std::size_t index = 0; index < 2; ++index) {
            sdkSupervisor().unregisterCollector(index);
        }
        sdkSupervisor().stop();
        pool.stop();
        pool.join();
        throw;
    }
}

} // namespace

int main() {
    service::common::UuidV7Generator uuidGenerator;
    try {
        namespace packetLog = service::packet_log;
        packetLog::Config packetConfig;
        packetConfig.directory =
            std::filesystem::temp_directory_path() / "iot-engine-gb-packet-log";
        packetConfig.level = packetLog::Level::Info;
        packetLog::initialize(std::move(packetConfig));
        packetLog::shutdown();
        // Packet-log shutdown must not destroy spdlog's process-wide default
        // logger; GB startup logs immediately after this lifecycle transition.
        LOG_INFO << "packet logger lifecycle retained the application logger";

        MediaConfig config;
        config.rtpPublicIp = "127.0.0.1";
        config.playTokenSecret = "gb28181-media-test-secret";
        config.workerThreads = 1;
        config.logLevel = 4;
        config.httpPort = 0;
        config.rtspPort = 0;
        config.rtmpPort = 0;
        config.rtcPort = reserveRtcPort();
        config.srtPort = 0;
        config.rtpPortRangeStart = 0;
        config.rtpPortRangeEnd = 0;

        std::atomic_int pcmRegistered{ 0 };
        std::atomic_int pcmDeregistered{ 0 };
        std::atomic_int invalidDeregisteredReaderCount{ 0 };
        std::atomic_int unexpectedOwner{ 0 };
        ZlmSdk::Callbacks callbacks;
        callbacks.onStreamChanged =
            [&pcmRegistered, &pcmDeregistered, &invalidDeregisteredReaderCount, &unexpectedOwner](std::string, std::string stream, std::string, bool online, int readerCount, ZlmSdk::OwnerIndex owner) {
                if (stream != "pcm-aac") {
                    return;
                }
                if (owner != ZlmSdk::kUnassignedOwner) {
                    ++unexpectedOwner;
                }
                if (online) {
                    ++pcmRegistered;
                } else {
                    ++pcmDeregistered;
                    if (readerCount != 0) {
                        ++invalidDeregisteredReaderCount;
                    }
                }
            };
        ZlmSdk sdk(config, std::move(callbacks));
        sdk.start();
        const auto ports = sdk.ports();
        require(ports.http != 0, "embedded ZLM HTTP server did not start");
        require(ports.rtsp != 0, "embedded ZLM RTSP server did not start");
        require(ports.rtmp != 0, "embedded ZLM RTMP server did not start");
        require(ports.rtc != 0, "embedded ZLM RTC server did not start");
        require(ports.srt != 0, "embedded ZLM SRT server did not start");

        using MediaHandle = std::unique_ptr<std::remove_pointer_t<mk_media>, decltype(&mk_media_release)>;
        MediaHandle pcmMedia(
            mk_media_create("__defaultVhost__", "test", "pcm-aac", 0, 0, 0),
            &mk_media_release
        );
        require(pcmMedia != nullptr, "embedded ZLM PCM media source was not created");
        require(mk_media_init_audio(pcmMedia.get(), MKCodecAAC, 8000, 1, 16) == 1, "embedded ZLM AAC track was not initialized");
        mk_media_init_complete(pcmMedia.get());
        std::array<std::int16_t, 1024> pcm{};
        bool aacProduced = false;
        for (std::uint64_t frame = 0; frame < 8 && !aacProduced; ++frame) {
            aacProduced =
                mk_media_input_pcm(pcmMedia.get(), pcm.data(), static_cast<int>(pcm.size() * sizeof(pcm[0])), frame * 128 + 1) == 1;
        }
        require(aacProduced, "embedded ZLM FAAC PCM input did not produce AAC");
        require(waitUntil([&pcmRegistered] {
                    return pcmRegistered.load() > 0;
                }),
                "embedded ZLM media registration callback was not observed");
        pcmMedia.reset();
        require(waitUntil([&pcmDeregistered] {
                    return pcmDeregistered.load() > 0;
                }) &&
                    invalidDeregisteredReaderCount.load() == 0,
                "embedded ZLM media deregistration queried an unsafe reader count");
        require(unexpectedOwner.load() == 0, "a stream no Collector opened reported an owner");

        const auto sessionId = uuidGenerator.next();
        const auto opened = sdk.openRtpServer("34020000002000000001", "34020000001320000001", "2000000001", sessionId);
        require(opened.has_value(), "embedded ZLM RTP server did not open");
        require(opened->streamId == "gb_34020000002000000001_34020000001320000001_2000000001_" + sessionId, "RTP stream did not preserve the caller session identity");
        require(opened->port != 0, "embedded ZLM RTP server returned port zero");
        require(opened->port >= 30000 && opened->port <= 35000 && opened->port % 2 == 0, "automatic RTP allocation did not return a managed even port");
        require(opened->playUrls.httpFlv.find(std::to_string(ports.http)) != std::string::npos, "HTTP-FLV URL does not use the embedded HTTP server");
        require(opened->playUrls.rtsp.find(std::to_string(ports.rtsp)) != std::string::npos, "RTSP URL does not use the embedded RTSP server");
        require(opened->playUrls.rtmp.find(std::to_string(ports.rtmp)) != std::string::npos, "RTMP URL does not use the embedded RTMP server");
        require(opened->playUrls.webRtc.find("/index/api/webrtc") != std::string::npos, "WebRTC URL does not use the C SDK signaling callback");
        require(opened->playUrls.wsFlv.empty(), "media proxy advertised an unsupported WebSocket playback URL");
        const auto tokenAt = opened->playUrls.httpFlv.find("token=");
        const auto expiresAt = opened->playUrls.httpFlv.find("&expires=");
        require(tokenAt != std::string::npos && expiresAt != std::string::npos, "play URL is not signed");
        const auto token =
            opened->playUrls.httpFlv.substr(tokenAt + 6, expiresAt - (tokenAt + 6));
        const auto expires = std::stoll(opened->playUrls.httpFlv.substr(expiresAt + 9));
        require(sdk.validatePlayToken(opened->streamId, token, expires), "generated play token is not valid");
        require(!sdk.validatePlayToken(opened->streamId, token + "0", expires), "tampered play token was accepted");
        require(!sdk.validatePlayToken(opened->streamId, token, 1), "expired play token was accepted");
        require(sdk.closeRtpServer(opened->streamId), "embedded ZLM RTP server did not close");

        std::optional<OpenRtpServerResult> firstOwned, secondOwned;
        {
            ZlmSdk::OwnerScope owner(0);
            firstOwned = sdk.openRtpServer("same-device", "same-channel", "2000000001", uuidGenerator.next());
        }
        {
            ZlmSdk::OwnerScope owner(1);
            secondOwned = sdk.openRtpServer("same-device", "same-channel", "2000000001", uuidGenerator.next());
            require(firstOwned && secondOwned && firstOwned->streamId != secondOwned->streamId, "different Collector sessions collided in the SDK");
            require(!sdk.closeRtpServer(firstOwned->streamId), "a Collector closed another Collector's RTP server");
            require(sdk.closeRtpServer(secondOwned->streamId), "the calling Collector could not close its RTP server");
        }
        {
            ZlmSdk::OwnerScope owner(0);
            require(sdk.closeRtpServer(firstOwned->streamId), "cross-owner close damaged the original RTP server");
        }

        sdk.stop();
        require(!sdk.started(), "embedded ZLM runtime did not stop");
        config.rtcPort = reserveRtcPort();
        verifySdkCallbackOwners(config);
        std::cout << "gb28181 media sdk tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
