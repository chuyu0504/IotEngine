#include "service/features/gb28181/media/media.transport.h"

#include <algorithm>
#include <asio/post.hpp>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <openssl/crypto.h>
#include <openssl/hmac.h>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <ruvia/web/http_client_handle.h>
#include <ruvia/web/streaming.h>
#include <ruvia/web/model_json.h>

#include "service/common/http.h"
#include "service/middleware/log.h"

namespace {

constexpr std::string_view kDefaultVhost{ "__defaultVhost__" };
constexpr std::string_view kRtpApp{ "rtp" };
constexpr std::string_view kWebRtcPath{ "/index/api/webrtc" };
constexpr unsigned int kAutomaticRtpPortStart{ 30000 };
constexpr unsigned int kAutomaticRtpPortEnd{ 35000 };

std::pair<unsigned int, unsigned int> rtpPortRange(const MediaConfig& config) {
    const auto automatic = config.rtpPortRangeStart == 0 && config.rtpPortRangeEnd == 0;
    const auto configuredStart =
        automatic ? kAutomaticRtpPortStart : config.rtpPortRangeStart;
    const auto configuredEnd = automatic ? kAutomaticRtpPortEnd : config.rtpPortRangeEnd;
    const auto firstEven = configuredStart + configuredStart % 2U;
    const auto lastEven = configuredEnd - configuredEnd % 2U;
    return { firstEven, lastEven };
}

std::string trimTrailingSlash(std::string value) {
    while (!value.empty() && value.back() == '/') {
        value.pop_back();
    }
    return value;
}

std::string authorityHost(std::string_view url) {
    const auto scheme = url.find("://");
    const auto begin = scheme == std::string_view::npos ? 0 : scheme + 3;
    const auto end = url.find('/', begin);
    auto authority = url.substr(begin, end == std::string_view::npos ? url.size() - begin : end - begin);
    if (authority.starts_with('[')) {
        const auto close = authority.find(']');
        return close == std::string_view::npos ? std::string(authority)
                                               : std::string(authority.substr(0, close + 1));
    }
    const auto colon = authority.rfind(':');
    return std::string(colon == std::string_view::npos ? authority : authority.substr(0, colon));
}

std::string publicHttpBase(const MediaConfig& config, std::uint16_t actualHttpPort, std::uint16_t actualHttpsPort) {
    if (!config.zlmPublicBaseUrl.empty()) {
        return trimTrailingSlash(config.zlmPublicBaseUrl);
    }
    std::ostringstream value;
    const auto secure = config.tlsEnabled;
    const auto port = secure ? actualHttpsPort : actualHttpPort;
    value << (secure ? "https://" : "http://") << config.rtpPublicIp;
    if ((secure && port != 443) || (!secure && port != 80)) {
        value << ':' << port;
    }
    return value.str();
}

std::string rtspBase(const MediaConfig& config, std::uint16_t httpPort, std::uint16_t httpsPort, std::uint16_t rtspPort, std::uint16_t rtspsPort) {
    const auto host = authorityHost(publicHttpBase(config, httpPort, httpsPort));
    const auto secure = config.tlsEnabled;
    const auto port = secure ? rtspsPort : rtspPort;
    std::ostringstream value;
    value << (secure ? "rtsps://" : "rtsp://") << host;
    if ((secure && port != 322) || (!secure && port != 554)) {
        value << ':' << port;
    }
    return value.str();
}

std::string rtmpBase(const MediaConfig& config, std::uint16_t httpPort, std::uint16_t httpsPort, std::uint16_t rtmpPort, std::uint16_t rtmpsPort) {
    const auto host = authorityHost(publicHttpBase(config, httpPort, httpsPort));
    const auto secure = config.tlsEnabled;
    const auto port = secure ? rtmpsPort : rtmpPort;
    std::ostringstream value;
    value << (secure ? "rtmps://" : "rtmp://") << host;
    if ((secure && port != 443) || (!secure && port != 1935)) {
        value << ':' << port;
    }
    return value.str();
}

bool managedStream(std::string_view stream) {
    return stream.starts_with("gb_");
}

const char* safe(const char* value) {
    return value == nullptr ? "" : value;
}

std::uint32_t parseSsrc(std::string_view value) {
    std::uint32_t result = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
    return parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size() ? result : 0;
}

std::string queryParameter(std::string_view query, std::string_view name) {
    if (query.starts_with('?')) {
        query.remove_prefix(1);
    }
    while (!query.empty()) {
        const auto separator = query.find('&');
        const auto item = query.substr(0, separator);
        const auto equals = item.find('=');
        if (item.substr(0, equals) == name) {
            return equals == std::string_view::npos
                ? std::string{}
                : std::string(item.substr(equals + 1));
        }
        if (separator == std::string_view::npos) {
            break;
        }
        query.remove_prefix(separator + 1);
    }
    return {};
}

std::optional<std::int64_t> parseExpiry(std::string_view value) {
    std::int64_t result{};
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
        return std::nullopt;
    }
    return result;
}

std::int64_t unixSecondsNow() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()
    )
        .count();
}

std::string hmacSha256Hex(std::string_view secret, std::string_view payload) {
    unsigned int length{};
    unsigned char digest[EVP_MAX_MD_SIZE]{};
    if (HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()), reinterpret_cast<const unsigned char*>(payload.data()), payload.size(), digest, &length) == nullptr) {
        throw std::runtime_error("Unable to sign ZLM playback URL");
    }
    std::ostringstream value;
    value << std::hex << std::setfill('0');
    for (unsigned int index = 0; index < length; ++index) {
        value << std::setw(2) << static_cast<unsigned int>(digest[index]);
    }
    return value.str();
}

} // namespace

std::mutex ZlmSdk::callbackMutex_;
std::weak_ptr<ZlmSdk::CallbackState> ZlmSdk::activeCallbacks_;
thread_local ZlmSdk::OwnerIndex ZlmSdk::actorOwner_ = ZlmSdk::kUnassignedOwner;

struct SdkSupervisor::Route final
    : std::enable_shared_from_this<SdkSupervisor::Route> {
    Route(ruvia::event_loop value, CollectorCallbacks valueCallbacks)
        : loop(std::move(value)), callbacks(std::move(valueCallbacks)) {}

    void streamChanged(std::string app, std::string stream, std::string schema, bool online, int readerCount) {
        auto self = shared_from_this();
        {
            std::lock_guard lock(mutex);
            if (!active.load()) {
                return;
            }
            ++pending;
        }
        auto dispatch =
            [self, app = std::move(app), stream = std::move(stream), schema = std::move(schema), online, readerCount]() mutable {
                try {
                    if (self->active.load() && self->callbacks.onStreamChanged) {
                        self->callbacks.onStreamChanged(
                            std::move(app),
                            std::move(stream),
                            std::move(schema),
                            online,
                            readerCount
                        );
                    }
                } catch (const std::exception& error) {
                    LOG_ERROR << "[GB28181][ZLM SDK] Collector stream callback "
                              << "failed: " << error.what();
                } catch (...) {
                    LOG_ERROR << "[GB28181][ZLM SDK] Collector stream callback "
                              << "failed";
                }
                self->finish();
            };
        const auto posted = loop.post(dispatch);
        if (!posted.accepted()) {
            // A full worker mailbox must not silently discard an SDK event.
            // The owning event_loop executor has its own queue and is still
            // the same worker, so use it as a lossless fallback while the
            // worker is accepting work.  During teardown, deactivate() owns
            // the dropped event and wakes waitForPending().
            if (loop.accepting()) {
                try {
                    asio::post(loop.executor(), std::move(dispatch));
                    return;
                } catch (...) {
                    LOG_ERROR << "[GB28181][ZLM SDK] Collector stream callback "
                              << "could not be queued on its owner loop";
                }
            } else {
                LOG_WARN << "[GB28181][ZLM SDK] Dropping stream callback after "
                         << "owner loop stopped";
            }
            self->finish();
        }
    }

    void rtpDetached(std::string stream) {
        auto self = shared_from_this();
        {
            std::lock_guard lock(mutex);
            if (!active.load()) {
                return;
            }
            ++pending;
        }
        auto dispatch = [self, stream = std::move(stream)]() mutable {
            try {
                if (self->active.load() && self->callbacks.onRtpDetached) {
                    self->callbacks.onRtpDetached(std::move(stream));
                }
            } catch (const std::exception& error) {
                LOG_ERROR << "[GB28181][ZLM SDK] Collector RTP detach callback "
                          << "failed: " << error.what();
            } catch (...) {
                LOG_ERROR << "[GB28181][ZLM SDK] Collector RTP detach callback "
                          << "failed";
            }
            self->finish();
        };
        const auto posted = loop.post(dispatch);
        if (!posted.accepted()) {
            if (loop.accepting()) {
                try {
                    asio::post(loop.executor(), std::move(dispatch));
                    return;
                } catch (...) {
                    LOG_ERROR << "[GB28181][ZLM SDK] Collector RTP detach "
                              << "could not be queued on its owner loop";
                }
            } else {
                LOG_WARN << "[GB28181][ZLM SDK] Dropping RTP detach callback "
                         << "after owner loop stopped";
            }
            self->finish();
        }
    }

    void deactivate() noexcept {
        std::lock_guard lock(mutex);
        active.store(false);
        if (!loop.accepting()) {
            pending = 0;
        }
        if (pending == 0) {
            completed.notify_all();
        }
    }

    void waitForPending() noexcept {
        if (loop.is_current() || !loop.accepting()) {
            std::lock_guard lock(mutex);
            pending = 0;
            return;
        }
        std::unique_lock lock(mutex);
        completed.wait(lock, [this] {
            return pending == 0;
        });
    }

    ruvia::event_loop loop;
    CollectorCallbacks callbacks;
    std::mutex mutex;
    std::condition_variable completed;
    std::size_t pending{ 0 };
    std::atomic_bool active{ true };

  private:
    void finish() noexcept {
        std::lock_guard lock(mutex);
        if (pending != 0) {
            --pending;
        }
        if (pending == 0) {
            completed.notify_all();
        }
    }
};

SdkSupervisor& SdkSupervisor::instance() {
    static SdkSupervisor value;
    return value;
}

SdkSupervisor& sdkSupervisor() {
    return SdkSupervisor::instance();
}

bool SdkSupervisor::enabled() const noexcept {
    std::lock_guard lock(mutex_);
    return configured_;
}

std::unique_ptr<ZlmSdk> SdkSupervisor::makeSdk() const {
    ZlmSdk::Callbacks callbacks;
    callbacks.onStreamChangedOwned =
        [this](std::string app, std::string stream, std::string schema, bool online, int readerCount, OwnerIndex owner) {
            const auto route = routeFor(owner);
            if (route) {
                route->streamChanged(std::move(app), std::move(stream), std::move(schema), online, readerCount);
            }
        };
    callbacks.onRtpDetachedOwned =
        [this](std::string stream, OwnerIndex owner) {
            const auto route = routeFor(owner);
            if (route) {
                route->rtpDetached(std::move(stream));
            }
        };
    return std::make_unique<ZlmSdk>(config_, std::move(callbacks));
}

void SdkSupervisor::configure(MediaConfig config) {
    std::lock_guard lifecycleLock(lifecycleMutex_);
    std::lock_guard lock(mutex_);
    if (sdk_ && sdk_->started()) {
        throw std::runtime_error("ZLMediaKit SDK supervisor is already started");
    }
    if (configured_) {
        throw std::runtime_error("ZLMediaKit SDK supervisor is already configured");
    }
    config_ = std::move(config);
    try {
        sdk_ = makeSdk();
        configured_ = true;
    } catch (...) {
        sdk_.reset();
        config_ = {};
        throw;
    }
}

void SdkSupervisor::start() {
    std::lock_guard lifecycleLock(lifecycleMutex_);
    ZlmSdk* sdk = nullptr;
    {
        std::lock_guard lock(mutex_);
        if (!configured_) {
            return;
        }
        if (!sdk_) {
            sdk_ = makeSdk();
        }
        sdk = sdk_.get();
    }
    sdk->start();
}

void SdkSupervisor::stop() noexcept {
    std::lock_guard lifecycleLock(lifecycleMutex_);
    std::unique_ptr<ZlmSdk> sdk;
    std::vector<std::shared_ptr<Route>> routes;
    {
        std::lock_guard lock(mutex_);
        routes.reserve(routes_.size());
        for (auto& [_, route] : routes_) {
            route->deactivate();
            routes.push_back(std::move(route));
        }
        routes_.clear();
        sdk = std::move(sdk_);
    }
    if (sdk) {
        sdk->stop();
    }
    for (const auto& route : routes) {
        route->waitForPending();
    }
}

bool SdkSupervisor::started() const noexcept {
    std::lock_guard lock(mutex_);
    return sdk_ && sdk_->started();
}

ZlmSdk& SdkSupervisor::sdk() {
    std::lock_guard lock(mutex_);
    if (!sdk_ || !sdk_->started()) {
        throw std::runtime_error("ZLMediaKit SDK supervisor is not started");
    }
    return *sdk_;
}

void SdkSupervisor::registerCollector(OwnerIndex owner, ruvia::event_loop loop, CollectorCallbacks callbacks) {
    if (!loop.valid() || !loop.handle().valid()) {
        throw std::invalid_argument(
            "ZLMediaKit SDK collector route requires a valid EventLoop"
        );
    }
    if (!callbacks.onStreamChanged && !callbacks.onRtpDetached) {
        throw std::invalid_argument(
            "ZLMediaKit SDK collector route requires a callback"
        );
    }
    auto route = std::make_shared<Route>(std::move(loop), std::move(callbacks));
    std::lock_guard lifecycleLock(lifecycleMutex_);
    std::lock_guard lock(mutex_);
    if (!configured_ || !sdk_ || !sdk_->started()) {
        throw std::runtime_error(
            "ZLMediaKit SDK must start before Collector registration"
        );
    }
    if (!routes_.emplace(owner, std::move(route)).second) {
        throw std::logic_error("ZLMediaKit SDK collector owner is already registered");
    }
}

void SdkSupervisor::unregisterCollector(OwnerIndex owner) noexcept {
    std::lock_guard lifecycleLock(lifecycleMutex_);
    std::shared_ptr<Route> route;
    {
        std::lock_guard lock(mutex_);
        const auto found = routes_.find(owner);
        if (found == routes_.end()) {
            return;
        }
        route = std::move(found->second);
        routes_.erase(found);
    }
    route->deactivate();
    route->waitForPending();
}

std::shared_ptr<SdkSupervisor::Route>
SdkSupervisor::routeFor(OwnerIndex owner) const {
    if (owner == ZlmSdk::kUnassignedOwner) {
        return {};
    }
    std::lock_guard lock(mutex_);
    const auto found = routes_.find(owner);
    return found == routes_.end() ? std::shared_ptr<Route>{} : found->second;
}

MediaServerPorts SdkSupervisor::ports() const noexcept {
    std::lock_guard lock(mutex_);
    return sdk_ ? sdk_->ports() : MediaServerPorts{};
}

MediaCapabilities SdkSupervisor::capabilities() const noexcept {
    std::lock_guard lock(mutex_);
    if (sdk_) {
        return sdk_->capabilities();
    }
    MediaCapabilities result;
    result.tls = config_.tlsEnabled;
    result.recording = config_.recordingEnabled;
    return result;
}

ZlmSdk::OwnerScope::OwnerScope(std::optional<OwnerIndex> owner) noexcept
    : previous_(actorOwner_) {
    actorOwner_ = owner.value_or(kUnassignedOwner);
}

ZlmSdk::OwnerScope::~OwnerScope() {
    actorOwner_ = previous_;
}

ZlmSdk::ZlmSdk(MediaConfig config, Callbacks callbacks)
    : config_(std::move(config)),
      callbacks_(std::make_shared<CallbackState>()) {
    callbacks_->callbacks = std::move(callbacks);
    callbacks_->playTokenSecret = config_.playTokenSecret;
    callbacks_->corsOrigin = config_.corsOrigin;
    nextRtpPort_.store(rtpPortRange(config_).first);
}

ZlmSdk::~ZlmSdk() {
    stop();
}

void ZlmSdk::RtpServerDeleter::operator()(
    std::remove_pointer_t<mk_rtp_server>* server
) const noexcept {
    if (server != nullptr) {
        mk_rtp_server_release(server);
    }
}

void ZlmSdk::start() {
    if (started_.exchange(true)) {
        return;
    }

    try {
        if (config_.workerThreads <= 0) {
            throw std::runtime_error("ZLM worker thread count must be positive");
        }
        if (config_.playTokenSecret.size() < 16) {
            throw std::runtime_error(
                "GB28181 media token secret must contain at least 16 characters"
            );
        }
        if (config_.playTokenTtlSeconds <= 0) {
            throw std::runtime_error("GB28181 media token TTL must be positive");
        }
        const auto automaticRtpPorts =
            config_.rtpPortRangeStart == 0 && config_.rtpPortRangeEnd == 0;
        if (!automaticRtpPorts &&
            (config_.rtpPortRangeStart == 0 || config_.rtpPortRangeEnd == 0)) {
            throw std::runtime_error(
                "GB28181 RTP port range must set both bounds or leave both zero"
            );
        }
        const auto [rtpPortStart, rtpPortEnd] = rtpPortRange(config_);
        if (rtpPortStart > rtpPortEnd || rtpPortEnd >= 65535U) {
            throw std::runtime_error(
                "GB28181 RTP port range must contain an even RTP/RTCP port pair"
            );
        }
        if (config_.tlsEnabled &&
            (config_.tlsPemPath.empty() ||
             !std::filesystem::is_regular_file(config_.tlsPemPath))) {
            throw std::runtime_error(
                "ZLM TLS is enabled but the PEM certificate path is invalid"
            );
        }

        {
            std::lock_guard lock(callbackMutex_);
            if (!activeCallbacks_.expired()) {
                throw std::runtime_error("Only one embedded ZLMediaKit runtime is supported");
            }
            callbacks_->active.store(true);
            activeCallbacks_ = callbacks_;
        }

        const auto ini = sdkIni();
        mk_config sdkConfig{};
        sdkConfig.thread_num = config_.workerThreads;
        sdkConfig.log_level = config_.logLevel;
        sdkConfig.log_mask = LOG_CONSOLE;
        sdkConfig.log_file_path = nullptr;
        sdkConfig.log_file_days = 0;
        sdkConfig.ini_is_path = 0;
        sdkConfig.ini = ini.c_str();
        sdkConfig.ssl_is_path = config_.tlsEnabled ? 1 : 0;
        sdkConfig.ssl = config_.tlsEnabled ? config_.tlsPemPath.c_str() : nullptr;
        sdkConfig.ssl_pwd =
            config_.tlsEnabled && !config_.tlsPassword.empty()
            ? config_.tlsPassword.c_str()
            : nullptr;
        mk_env_init(&sdkConfig);

        mk_events events{};
        events.on_mk_media_changed = &ZlmSdk::handleMediaChanged;
        events.on_mk_media_play = &ZlmSdk::handleMediaPlay;
        events.on_mk_http_request = &ZlmSdk::handleHttpRequest;
        mk_events_listen(&events);

        ports_.http = mk_http_server_start(config_.httpPort, 0);
        ports_.rtsp = mk_rtsp_server_start(config_.rtspPort, 0);
        ports_.rtmp = mk_rtmp_server_start(config_.rtmpPort, 0);
        if (config_.tlsEnabled) {
            ports_.https = mk_http_server_start(config_.httpsPort, 1);
            ports_.rtsps = mk_rtsp_server_start(config_.rtspsPort, 1);
            ports_.rtmps = mk_rtmp_server_start(config_.rtmpsPort, 1);
        }
        ports_.rtc = mk_rtc_server_start(config_.rtcPort);
        ports_.srt = mk_srt_server_start(config_.srtPort);
        if (ports_.http == 0 || ports_.rtsp == 0 || ports_.rtmp == 0 ||
            ports_.rtc == 0 || ports_.srt == 0 ||
            (config_.tlsEnabled &&
             (ports_.https == 0 || ports_.rtsps == 0 || ports_.rtmps == 0))) {
            throw std::runtime_error("Embedded ZLMediaKit failed to bind one or more servers");
        }

        LOG_INFO << "[GB28181][ZLM SDK] Started with isolated media workers, workers="
                 << config_.workerThreads << ", http=" << ports_.http
                 << ", rtsp=" << ports_.rtsp << ", rtmp=" << ports_.rtmp
                 << ", rtc=" << ports_.rtc << ", srt=" << ports_.srt;
        if (config_.tlsEnabled) {
            LOG_INFO << "[GB28181][ZLM SDK] TLS listeners, https=" << ports_.https
                     << ", rtsps=" << ports_.rtsps << ", rtmps=" << ports_.rtmps;
        }
    } catch (...) {
        stop();
        throw;
    }
}

void ZlmSdk::stop() noexcept {
    if (!started_.exchange(false)) {
        return;
    }

    {
        std::lock_guard invocationLock(callbacks_->invocationMutex);
        callbacks_->active.store(false);
        callbacks_->streamOwners.clear();
    }
    {
        std::lock_guard lock(callbackMutex_);
        const auto active = activeCallbacks_.lock();
        if (active == callbacks_) {
            activeCallbacks_.reset();
        }
    }
    // ZLMediaKit stores the event table in process-global state without
    // synchronizing readers. Keep our process-lifetime handlers installed so
    // shutdown cannot race worker threads reading a partially cleared table.
    // activeCallbacks_ already makes every handler a no-op after this point.
    {
        std::lock_guard lock(rtpMutex_);
        rtpServers_.clear();
    }
    mk_stop_all_server();
    ports_ = {};
    LOG_INFO << "[GB28181][ZLM SDK] Stopped";
}

std::optional<OpenRtpServerResult>
ZlmSdk::openRtpServer(const std::string& deviceId, const std::string& channelId, const std::string& ssrc, const std::string& sessionId) {
    if (!started_.load()) {
        return std::nullopt;
    }

    const auto streamId = makeStreamId(deviceId, channelId, ssrc, sessionId);
    const auto [rtpPortStart, rtpPortEnd] = rtpPortRange(config_);
    const auto attempts = (rtpPortEnd - rtpPortStart) / 2U + 1U;
    // A stream gets an owner only while it is created by the SIP actor on
    // the originating Collector Worker. An SDK call without that
    // scope remains unowned; it is never assigned from a device or stream ID.
    const auto owner = actorOwner_;

    for (unsigned int attempt = 0; attempt < attempts; ++attempt) {
        const auto requestedPort = allocateRtpPort();
        try {
            // Never delegate port-zero allocation to ZLMediaKit. Its allocator
            // temporarily opens TCP probes before the passive RTP listener;
            // an immediate rebind can therefore collide with those probes on
            // macOS. Explicit ports take the direct UDP/TCP bind path instead.
            auto* raw = mk_rtp_server_create2(requestedPort, 1, kDefaultVhost.data(), kRtpApp.data(), streamId.c_str());
            if (raw == nullptr) {
                continue;
            }

            RtpServerPtr server(raw);
            const auto parsedSsrc = parseSsrc(ssrc);
            if (parsedSsrc != 0) {
                mk_rtp_server_update_ssrc(server.get(), parsedSsrc);
            }

            auto* detachState = new RtpCallbackState{
                .callbacks = callbacks_,
                .stream = streamId,
                .owner = owner
            };
            mk_rtp_server_set_on_detach2(server.get(), &ZlmSdk::handleRtpDetached, detachState, &ZlmSdk::releaseRtpCallback);

            const auto actualPort = mk_rtp_server_port(server.get());
            {
                std::lock_guard lock(rtpMutex_);
                if (!rtpServers_.emplace(streamId, std::move(server)).second) {
                    LOG_WARN << "[GB28181][ZLM SDK] Refusing duplicate RTP "
                             << "stream id, stream=" << streamId;
                    return std::nullopt;
                }
            }

            {
                std::lock_guard lock(callbacks_->invocationMutex);
                if (!callbacks_->streamOwners.emplace(streamId, owner).second) {
                    std::lock_guard rtpLock(rtpMutex_);
                    rtpServers_.erase(streamId);
                    LOG_WARN << "[GB28181][ZLM SDK] Refusing duplicate stream "
                             << "owner record, stream=" << streamId;
                    return std::nullopt;
                }
                if (callbacks_->callbacks.onStreamOwner &&
                    owner != kUnassignedOwner) {
                    callbacks_->callbacks.onStreamOwner(streamId, owner);
                }
            }

            LOG_INFO << "[GB28181][ZLM SDK] RTP server opened, stream=" << streamId
                     << ", port=" << actualPort << ", ssrc=" << ssrc;
            return OpenRtpServerResult{
                .streamId = streamId,
                .port = actualPort,
                .playUrls = buildPlayUrls(streamId),
            };
        } catch (const std::exception& error) {
            LOG_WARN << "[GB28181][ZLM SDK] RTP port bind failed, stream=" << streamId
                     << ", port=" << requestedPort << ", error=" << error.what();
        }
    }

    LOG_ERROR << "[GB28181][ZLM SDK] RTP port range exhausted, stream=" << streamId;
    return std::nullopt;
}

bool ZlmSdk::closeRtpServer(const std::string& streamId) {
    const auto caller = actorOwner_;
    {
        std::lock_guard lock(callbacks_->invocationMutex);
        const auto found = callbacks_->streamOwners.find(streamId);
        if (found == callbacks_->streamOwners.end()) {
            return false;
        }
        if (found->second != kUnassignedOwner && found->second != caller) {
            LOG_WARN << "[GB28181][ZLM SDK] Refusing cross-owner RTP close, "
                     << "stream=" << streamId;
            return false;
        }
    }
    RtpServerPtr server;
    {
        std::lock_guard lock(rtpMutex_);
        const auto found = rtpServers_.find(streamId);
        if (found == rtpServers_.end()) {
            return false;
        }
        server = std::move(found->second);
        rtpServers_.erase(found);
    }
    server.reset();
    {
        std::lock_guard lock(callbacks_->invocationMutex);
        callbacks_->streamOwners.erase(streamId);
    }
    LOG_INFO << "[GB28181][ZLM SDK] RTP server closed, stream=" << streamId;
    return true;
}

PlayUrls ZlmSdk::buildPlayUrls(const std::string& streamId) const {
    const auto httpPort = ports_.http == 0 ? config_.httpPort : ports_.http;
    const auto httpsPort = ports_.https == 0 ? config_.httpsPort : ports_.https;
    const auto httpBase = publicHttpBase(config_, httpPort, httpsPort);
    const auto rtsp = rtspBase(
        config_,
        httpPort,
        httpsPort,
        ports_.rtsp == 0 ? config_.rtspPort : ports_.rtsp,
        ports_.rtsps == 0 ? config_.rtspsPort : ports_.rtsps
    );
    const auto rtmp = rtmpBase(
        config_,
        httpPort,
        httpsPort,
        ports_.rtmp == 0 ? config_.rtmpPort : ports_.rtmp,
        ports_.rtmps == 0 ? config_.rtmpsPort : ports_.rtmps
    );
    const auto path = std::string("/") + std::string(kRtpApp) + "/" + streamId;
    const auto expires = unixSecondsNow() + config_.playTokenTtlSeconds;
    const auto auth = "token=" + makePlayToken(streamId, expires) +
        "&expires=" + std::to_string(expires);
    return PlayUrls{
        .httpFlv = httpBase + path + ".live.flv?" + auth,
        // The application proxy intentionally exposes HTTP streaming and WebRTC
        // signaling only. Do not advertise a WebSocket URL that the proxy does
        // not implement; the player falls back to HTTP-FLV/HLS without ambiguity.
        .wsFlv = {},
        .httpTs = httpBase + path + ".live.ts?" + auth,
        .hls = httpBase + path + "/hls.m3u8?" + auth,
        .webRtc = httpBase + std::string(kWebRtcPath) + "?app=" +
            std::string(kRtpApp) + "&stream=" + streamId + "&type=play&" + auth,
        .rtsp = rtsp + path + "?" + auth,
        .rtmp = rtmp + path + "?" + auth,
    };
}

MediaCapabilities ZlmSdk::capabilities() const noexcept {
    auto result = MediaCapabilities{};
    result.tls = config_.tlsEnabled;
    result.recording = config_.recordingEnabled;
    return result;
}

std::string ZlmSdk::makePlayToken(std::string_view stream, std::int64_t expires) const {
    return hmacSha256Hex(config_.playTokenSecret, std::string(stream) + "\n" + std::to_string(expires));
}

bool ZlmSdk::authorizePlay(const CallbackState& state, std::string_view stream, std::string_view token, std::int64_t expires) {
    if (!managedStream(stream) || token.empty() || expires < unixSecondsNow()) {
        return false;
    }
    const auto expected = hmacSha256Hex(
        state.playTokenSecret,
        std::string(stream) + "\n" + std::to_string(expires)
    );
    return token.size() == expected.size() &&
        CRYPTO_memcmp(token.data(), expected.data(), expected.size()) == 0;
}

bool ZlmSdk::validatePlayToken(std::string_view stream, std::string_view token, std::int64_t expires) const {
    return authorizePlay(*callbacks_, stream, token, expires);
}

bool ZlmSdk::startMp4Recording(const std::string& streamId) {
    if (!started_.load() || !config_.recordingEnabled || !managedStream(streamId)) {
        return false;
    }
    const auto owner = streamOwner(streamId);
    if (owner != kUnassignedOwner && owner != actorOwner_) {
        return false;
    }
    const char* path = config_.recordRoot.empty() ? nullptr : config_.recordRoot.c_str();
    return mk_recorder_start(1, kDefaultVhost.data(), kRtpApp.data(), streamId.c_str(), path, config_.recordMaxSegmentSeconds) == 1;
}

bool ZlmSdk::stopMp4Recording(const std::string& streamId) {
    const auto owner = streamOwner(streamId);
    return started_.load() && managedStream(streamId) &&
        (owner == kUnassignedOwner || owner == actorOwner_) &&
        mk_recorder_stop(1, kDefaultVhost.data(), kRtpApp.data(), streamId.c_str()) == 1;
}

bool ZlmSdk::isMp4Recording(const std::string& streamId) const {
    const auto owner = streamOwner(streamId);
    return started_.load() && managedStream(streamId) &&
        (owner == kUnassignedOwner || owner == actorOwner_) &&
        mk_recorder_is_recording(1, kDefaultVhost.data(), kRtpApp.data(), streamId.c_str()) == 1;
}

ZlmSdk::OwnerIndex ZlmSdk::streamOwner(std::string_view stream) const {
    std::lock_guard lock(callbacks_->invocationMutex);
    const auto found = callbacks_->streamOwners.find(std::string(stream));
    return found == callbacks_->streamOwners.end() ? kUnassignedOwner
                                                   : found->second;
}

std::string ZlmSdk::makeStreamId(const std::string& deviceId, const std::string& channelId, const std::string& ssrc, const std::string& sessionId) const {
    // SSRC values may repeat across Collectors and restarts. A new session
    // identity prevents delayed SDK events from being attributed to a replacement.
    return "gb_" + deviceId + "_" + channelId + "_" + ssrc + "_" +
        sessionId;
}

std::uint16_t ZlmSdk::allocateRtpPort() {
    const auto [rangeStart, rangeEnd] = rtpPortRange(config_);
    auto current = nextRtpPort_.load();
    while (true) {
        const auto port =
            current < rangeStart || current > rangeEnd ? rangeStart : current;
        const auto next = port + 2U > rangeEnd ? rangeStart : port + 2U;
        if (nextRtpPort_.compare_exchange_weak(current, next)) {
            return static_cast<std::uint16_t>(port);
        }
    }
}

std::string ZlmSdk::sdkIni() const {
    std::ostringstream ini;
    ini << "[general]\n"
        << "streamNoneReaderDelayMS=20000\n"
        << "[protocol]\n"
        << "enable_hls=1\n"
        << "enable_hls_fmp4=1\n"
        << "enable_rtsp=1\n"
        << "enable_rtmp=1\n"
        << "enable_ts=1\n"
        << "enable_fmp4=1\n"
        << "enable_mp4=0\n"
        << "mp4_save_path=" << config_.recordRoot << "\n"
        << "[rtc]\n"
        << "externIP=" << config_.rtpPublicIp << "\n"
        << "port=" << config_.rtcPort << "\n"
        << "tcpPort=" << config_.rtcPort << "\n";
    return ini.str();
}

void API_CALL ZlmSdk::handleMediaPlay(const mk_media_info media, const mk_auth_invoker invoker, const mk_sock_info) {
    const auto state = activeCallbacks();
    if (!state) {
        mk_auth_invoker_do(invoker, "media service unavailable");
        return;
    }
    const auto app = std::string_view(safe(mk_media_info_get_app(media)));
    const auto stream = std::string_view(safe(mk_media_info_get_stream(media)));
    const auto query = std::string_view(safe(mk_media_info_get_params(media)));
    const auto expires = parseExpiry(queryParameter(query, "expires"));
    const auto token = queryParameter(query, "token");
    const auto allowed =
        app == kRtpApp && expires &&
        authorizePlay(*state, stream, token, *expires);
    mk_auth_invoker_do(invoker, allowed ? nullptr : "invalid or expired play token");
}

std::shared_ptr<ZlmSdk::CallbackState> ZlmSdk::activeCallbacks() {
    std::lock_guard lock(callbackMutex_);
    const auto state = activeCallbacks_.lock();
    return state && state->active.load() ? state : nullptr;
}

void API_CALL ZlmSdk::handleMediaChanged(int registered, const mk_media_source source) {
    const auto state = activeCallbacks();
    if (!state) {
        return;
    }
    std::lock_guard invocationLock(state->invocationMutex);
    if (!state->active.load() ||
        (!state->callbacks.onStreamChanged &&
         !state->callbacks.onStreamChangedOwned)) {
        return;
    }
    const auto online = registered != 0;
    const auto app = safe(mk_media_source_get_app(source));
    const auto stream = safe(mk_media_source_get_stream(source));
    const auto schema = safe(mk_media_source_get_schema(source));
    const auto owner = [&] {
        const auto found = state->streamOwners.find(stream);
        return found == state->streamOwners.end() ? kUnassignedOwner
                                                  : found->second;
    }();
    if (state->callbacks.onStreamChangedOwned) {
        state->callbacks.onStreamChangedOwned(
            app,
            stream,
            schema,
            online,
            online ? mk_media_source_get_total_reader_count(source) : 0,
            owner
        );
    } else {
        state->callbacks.onStreamChanged(app, stream, schema, online, online ? mk_media_source_get_total_reader_count(source) : 0);
    }
}

void API_CALL ZlmSdk::handleHttpRequest(const mk_parser parser, const mk_http_response_invoker invoker, int* consumed, const mk_sock_info) {
    if (consumed == nullptr) {
        return;
    }
    *consumed = 0;
    if (!activeCallbacks() ||
        std::string_view(safe(mk_parser_get_url(parser))) != kWebRtcPath) {
        return;
    }

    *consumed = 1;
    const auto state = activeCallbacks();
    const auto app = std::string(safe(mk_parser_get_url_param(parser, "app")));
    const auto stream = std::string(safe(mk_parser_get_url_param(parser, "stream")));
    const auto type = std::string(safe(mk_parser_get_url_param(parser, "type")));
    const auto token = std::string(safe(mk_parser_get_url_param(parser, "token")));
    const auto expires = parseExpiry(safe(mk_parser_get_url_param(parser, "expires")));
    std::size_t contentLength = 0;
    const auto* offer = mk_parser_get_content(parser, &contentLength);
    if (!state || app != kRtpApp || type != "play" || !expires ||
        !authorizePlay(*state, stream, token, *expires) || offer == nullptr ||
        contentLength == 0) {
        const char* headers[] = { "Content-Type", "application/json", nullptr };
        WebRtcResponseModel body;
        body.set<"code">(-1).set<"msg">("invalid WebRTC play request");
        const auto serialized = ruvia::to_json(body);
        mk_http_response_invoker_do_string(invoker, 400, headers, serialized.c_str());
        return;
    }

    const auto rtcUrl =
        "rtc://" + std::string(kDefaultVhost) + "/" + app + "/" + stream;
    auto* cloned = mk_http_response_invoker_clone(invoker);
    mk_webrtc_get_answer_sdp2(cloned, &ZlmSdk::releaseHttpInvoker, &ZlmSdk::handleWebRtcAnswer, type.c_str(), offer, rtcUrl.c_str());
}

void API_CALL ZlmSdk::handleWebRtcAnswer(void* userData, const char* answer, const char* error) {
    const auto invoker = static_cast<mk_http_response_invoker>(userData);
    const auto state = activeCallbacks();
    const char* headers[5] = { "Content-Type", "application/json", nullptr, nullptr, nullptr };
    if (state && !state->corsOrigin.empty()) {
        headers[2] = "Access-Control-Allow-Origin";
        headers[3] = state->corsOrigin.c_str();
    }
    WebRtcResponseModel body;
    if (answer != nullptr) {
        body.set<"code">(0).set<"type">("answer").set<"sdp">(answer);
    } else {
        body.set<"code">(-1).set<"msg">(safe(error));
    }
    const auto serialized = ruvia::to_json(body);
    mk_http_response_invoker_do_string(invoker, 200, headers, serialized.c_str());
}

void API_CALL ZlmSdk::releaseHttpInvoker(void* userData) {
    if (userData != nullptr) {
        mk_http_response_invoker_clone_release(
            static_cast<mk_http_response_invoker>(userData)
        );
    }
}

void API_CALL ZlmSdk::handleRtpDetached(void* userData) {
    const auto* detached = static_cast<RtpCallbackState*>(userData);
    if (detached == nullptr) {
        return;
    }
    const auto state = detached->callbacks.lock();
    if (!state) {
        return;
    }
    std::lock_guard invocationLock(state->invocationMutex);
    if (!state->active.load()) {
        return;
    }
    if (state->callbacks.onRtpDetachedOwned) {
        state->callbacks.onRtpDetachedOwned(detached->stream, detached->owner);
    } else if (state->callbacks.onRtpDetached) {
        state->callbacks.onRtpDetached(detached->stream);
    }
}

void API_CALL ZlmSdk::releaseRtpCallback(void* userData) {
    delete static_cast<RtpCallbackState*>(userData);
}

#include <cctype>

namespace service::gb28181 {
namespace {
std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

bool forwardedResponseHeader(std::string_view name) noexcept {
    return name == "content-type" || name == "cache-control" ||
        name == "content-range" || name == "accept-ranges" || name == "etag" ||
        name == "last-modified" || name == "access-control-allow-origin" ||
        name == "access-control-expose-headers";
}
} // namespace

namespace media_proxy_detail {

bool allowedTarget(std::string_view method, std::string_view path) noexcept {
    if (method == "POST") {
        return path == "/index/api/webrtc";
    }
    if (method != "GET" || !path.starts_with("/rtp/") ||
        path.find("..") != std::string_view::npos) {
        return false;
    }
    return std::ranges::none_of(path, [](unsigned char character) {
        return character <= 0x20U || character == 0x7FU || character == '\\' ||
            character == '%' || character == '?' || character == '#';
    });
}

std::string rewriteHlsPlaylist(std::string_view playlist, std::string_view query) {
    if (query.empty()) {
        return std::string(playlist);
    }
    std::string output;
    output.reserve(playlist.size() + query.size() * 8U);
    while (!playlist.empty()) {
        const auto end = playlist.find('\n');
        auto line =
            end == std::string_view::npos ? playlist : playlist.substr(0, end);
        const auto carriage = line.ends_with('\r');
        if (carriage) {
            line.remove_suffix(1);
        }
        if (!line.empty() && !line.starts_with('#')) {
            output.append(line);
            output.push_back(line.find('?') == std::string_view::npos ? '?' : '&');
            output.append(query);
        } else {
            output.append(line);
        }
        if (carriage) {
            output.push_back('\r');
        }
        if (end == std::string_view::npos) {
            break;
        }
        output.push_back('\n');
        playlist.remove_prefix(end + 1);
    }
    return output;
}

} // namespace media_proxy_detail

std::string MediaProxyController::percentEncode(std::string_view value) {
    static constexpr char digits[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(value.size());
    for (const auto ch : value) {
        const auto byte = static_cast<unsigned char>(ch);
        if (std::isalnum(byte) != 0 || ch == '-' || ch == '_' || ch == '.' ||
            ch == '~') {
            result.push_back(ch);
        } else {
            result.push_back('%');
            result.push_back(digits[byte >> 4U]);
            result.push_back(digits[byte & 0x0FU]);
        }
    }
    return result;
}

std::string
MediaProxyController::queryString(const ruvia::context_request& request) {
    std::string result;
    for (const auto& field : request.query_fields()) {
        if (!result.empty()) {
            result.push_back('&');
        }
        result.append(percentEncode(field.name()));
        result.push_back('=');
        result.append(percentEncode(field.value()));
    }
    return result;
}

ruvia::task<void> MediaProxyController::proxy(ruvia::context& context) {
    if (!sdkSupervisor().started()) {
        service::common::fail(10002, "GB28181 媒体服务未启用", 503);
    }

    const auto request = context.req();
    const auto path = request.path();
    if (!path.starts_with("/media")) {
        service::common::fail(10001, "媒体代理路径无效", 400);
    }
    const auto upstreamPath = path.substr(6);
    if (!media_proxy_detail::allowedTarget(request.method(), upstreamPath)) {
        service::common::fail(10003, "媒体资源不存在", 404);
    }

    const auto query = queryString(request);
    std::string target(upstreamPath);
    if (!query.empty()) {
        target += '?' + query;
    }
    std::vector<ruvia::http_header_view> headers;
    for (const auto name : { "Content-Type", "Accept", "Range" }) {
        if (const auto value = request.header(name)) {
            headers.emplace_back(name, *value);
        }
    }
    std::string body;
    if (request.method() == "POST") {
        body.assign(co_await request.text());
    }
    const auto content = request.method() == "POST"
        ? ruvia::http_client_request_content_view::bytes(body)
        : ruvia::http_client_request_content_view::none();
    // The registered client, upstream socket, and response body all belong to
    // the accepting Service Worker. Ruvia supplies cancellation and backpressure.
    auto upstream = co_await context.get_http_client("gb-media").send({ .method_ = request.method(), .target_ = target, .headers_ = headers, .content_ = content });
    if (upstream.status().value() < 200 || upstream.status().value() > 599) {
        service::common::fail(10002, "媒体服务返回了无效状态", 502);
    }
    context.status(upstream.status());
    for (const auto& header : upstream.headers()) {
        if (forwardedResponseHeader(lower(std::string(header.name())))) {
            context.header(header.name(), header.value());
        }
    }

    auto& writer = context.stream();
    if (upstreamPath.ends_with(".m3u8")) {
        const auto playlist = co_await upstream.body().read_all(2U * 1024U * 1024U);
        const std::string_view playlistText(reinterpret_cast<const char*>(playlist.bytes().data()), playlist.size());
        const auto rewritten = media_proxy_detail::rewriteHlsPlaylist(playlistText, query);
        co_await writer.write(rewritten);
    } else {
        co_await upstream.body().pipe_to(writer);
    }
    if (!writer.aborted()) {
        co_await writer.end();
    }
}

} // namespace service::gb28181
