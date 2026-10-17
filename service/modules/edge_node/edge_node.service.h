#pragma once

#include <algorithm>
#include <any>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <edge.pb.h>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <ruvia/core/task_scope.h>
#include <ruvia/core/timer.h>
#include <ruvia/web/context.h>
#include <ruvia/web/db/db_query.h>
#include <ruvia/web/redis/redis_types.h>
#include <ruvia/web/redis/redis_handle.h>
#include <ruvia/web/request_validation.h>
#include <ruvia/web/websocket.h>

#include "service/common/database.h"
#include "service/common/http.h"
#include "service/common/message.h"
#include "service/common/timestamp.h"
#include "service/common/uuid.h"
#include "service/utils/uuid.h"
#include "service/common/worker.h"
#include "service/middleware/log.h"
#include "service/middleware/request_context.h"
#include "service/modules/edge_node/edge_node.entity.h"
#include "service/modules/edge_node/edge_node.types.h"
#include "service/modules/system/auth/auth.service.h"
#include "service/modules/system/outbox/outbox.service.h"
#include "service/modules/vpn/vpn.entity.h"
#include "service/modules/vpn/vpn.service.h"
#include "service/utils/base64.h"
#include "service/utils/change_feed.h"
#include "service/utils/json.h"
#include "service/utils/number.h"
#include "service/utils/rpc_client.h"

namespace service::edge_node::debug {

// Each invocation owns its allocation arena. Connection resources are borrowed
// only on the accepting Worker; event arguments never mutate its HTTP request.
class EventContext final : public service::middleware::RequestContext {
  public:
    EventContext(ruvia::context& connection, SessionIdentity& identity, const Request& request, ruvia::stop_token cancellation, ConnectionResources& resources, std::any& state)
        : RequestContext(connection, {}, std::move(cancellation)), connection(connection), identity(identity), request(request), resources(resources), state(state) {}

    ruvia::context& connection;
    SessionIdentity& identity;
    const Request& request;
    ConnectionResources& resources;
    std::any& state;
};

// Registry invoker of one typed event: decodes and validates the body in the
// invocation arena, then runs the handler on the accepting worker.
template <typename Handlers, auto Handler, typename Body>
ruvia::task<EventResult> invokeEvent(void* handlers, EventContext& context) {
    if (!context.connection.worker().is_current()) {
        throw std::logic_error("event invoked outside its accepting worker");
    }
    if (context.stop.stop_requested()) {
        throw EventError(10002, "操作已取消");
    }
    auto body = ruvia::from_json<Body>(context.request.data, { .resource_ = context.arena() });
    if (!body) {
        throw EventError(10001, "事件参数格式无效");
    }
    ruvia::validator validation({ .resource_ = context.arena() });
    ruvia::json_body<Body> validator;
    validator.validate(*body, validation);
    validation.throw_if_invalid();
    co_return co_await (static_cast<Handlers*>(handlers)->*Handler)(context, *body);
}

class TerminalSession final : public ConnectionResource {
  public:
    TerminalSession(std::string id, std::string owner, std::string node, std::string connection, unsigned columns, unsigned rows)
        : ConnectionResource(std::move(owner), "iot:edge:terminal"), id(std::move(id)), node(std::move(node)), connection(std::move(connection)), columns(columns), rows(rows) {}

    ruvia::task<void> close(EventContext& c) override;

    const std::string id;
    const std::string node;
    const std::string connection;
    unsigned columns;
    unsigned rows;
    unsigned protocolVersion{};
    std::uint64_t inputSequence{};
    bool writing{};
    std::weak_ptr<std::string> outputSubscription;
};

class SerialSession final : public ConnectionResource {
  public:
    SerialSession(std::string id, std::string owner, std::string node, std::string connection)
        : ConnectionResource(std::move(owner), "iot:edge:terminal"), id(std::move(id)), node(std::move(node)), connection(std::move(connection)) {}

    ruvia::task<void> close(EventContext& c) override;

    const std::string id;
    const std::string node;
    const std::string connection;
};

} // namespace service::edge_node::debug

namespace service::edge_node {

namespace module_pb = ::iot::edge::v1;
namespace db = service::common::database;

// 请求独占的固件暂存文件。业务登记把完成的文件改名为最终文件；其余退出路径删除暂存字节。
class FirmwareUpload final {
    class StagedFileLock final {
      public:
        StagedFileLock(const std::filesystem::path& path, bool create) : path_(path), created_(create) {
#ifdef _WIN32
            handle_ = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, create ? CREATE_NEW : OPEN_EXISTING,
                                  FILE_FLAG_OPEN_REPARSE_POINT,
                                  nullptr);
            if (handle_ == INVALID_HANDLE_VALUE) {
                const auto error = GetLastError();
                if (!create && (error == ERROR_FILE_NOT_FOUND || error == ERROR_SHARING_VIOLATION || error == ERROR_ACCESS_DENIED)) {
                    return;
                }
                throw std::system_error(error, std::system_category(), "open staged upload");
            }
            BY_HANDLE_FILE_INFORMATION info{};
            if (!GetFileInformationByHandle(handle_, &info) || (info.dwFileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY))) {
                CloseHandle(handle_);
                handle_ = INVALID_HANDLE_VALUE;
                return;
            }
            // Reserve a byte beyond all supported upload sizes; regular writes use another handle.
            overlap_.Offset = 0xfffffffe;
            overlap_.OffsetHigh = 0xffffffff;
            locked_ = LockFileEx(handle_, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlap_) != 0;
#else
            descriptor_ = ::open(path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW | (create ? O_CREAT | O_EXCL : 0), 0600);
            if (descriptor_ < 0) {
                if (!create && (errno == ENOENT || errno == ELOOP)) {
                    return;
                }
                throw std::system_error(errno, std::generic_category(), "open staged upload");
            }
            struct stat status{};
            locked_ = ::fstat(descriptor_, &status) == 0 && S_ISREG(status.st_mode) && ::flock(descriptor_, LOCK_EX | LOCK_NB) == 0;
#endif
            if (create && !locked_) {
                release();
                std::error_code ignored;
                std::filesystem::remove(path_, ignored);
                throw std::runtime_error("Cannot lock staged upload");
            }
        }

        ~StagedFileLock() {
            if (created_ && locked_) {
                std::error_code ignored;
                std::filesystem::remove(path_, ignored);
            }
            release();
        }

        bool locked() const { return locked_; }

        StagedFileLock(const StagedFileLock&) = delete;
        StagedFileLock& operator=(const StagedFileLock&) = delete;

      private:
        void release() noexcept {
#ifdef _WIN32
            if (handle_ != INVALID_HANDLE_VALUE) {
                CloseHandle(handle_);
                handle_ = INVALID_HANDLE_VALUE;
            }
#else
            if (descriptor_ >= 0) {
                ::close(descriptor_);
                descriptor_ = -1;
            }
#endif
            locked_ = false;
        }

        std::filesystem::path path_;
        bool created_{}, locked_{};
#ifdef _WIN32
        HANDLE handle_{ INVALID_HANDLE_VALUE };
        OVERLAPPED overlap_{};
#else
        int descriptor_{ -1 };
#endif
    };

  public:
    static constexpr std::size_t kChunkBytes = 16U * 1024U;
    static constexpr std::uint64_t kMaxBytes = 128ULL * 1024ULL * 1024ULL;

    // 上传目录锁（跨线程、跨进程的内核文件锁）：上传在创建暂存文件并加文件锁期间持共享锁（阻塞等待），
    // 恢复扫描持排他锁（不等待，取不到即本轮未执行），避免扫描删除尚未加锁的新暂存文件。
    class DirectoryLock final {
      public:
        enum class Mode { SharedWait, ExclusiveTry };

        DirectoryLock(const std::filesystem::path& directory, Mode mode) {
            const auto path = directory / ".upload-recovery.lock";
#ifdef _WIN32
            handle_ = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_ALWAYS, FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
            if (handle_ == INVALID_HANDLE_VALUE) {
                throw std::system_error(GetLastError(), std::system_category(), "open upload directory lock");
            }
            BY_HANDLE_FILE_INFORMATION info{};
            if (!GetFileInformationByHandle(handle_, &info) || (info.dwFileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY))) {
                release();
                throw std::runtime_error("Upload directory lock is not a regular file");
            }
            OVERLAPPED overlap{};
            const DWORD flags = mode == Mode::ExclusiveTry ? LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY : 0;
            if (LockFileEx(handle_, flags, 0, 1, 0, &overlap)) {
                locked_ = true;
                return;
            }
            const auto error = GetLastError();
            release();
            if (mode == Mode::ExclusiveTry && error == ERROR_LOCK_VIOLATION) {
                return;
            }
            throw std::system_error(error, std::system_category(), "lock upload directory");
#else
            descriptor_ = ::open(path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_CREAT, 0600);
            if (descriptor_ < 0) {
                throw std::system_error(errno, std::generic_category(), "open upload directory lock");
            }
            struct stat status{};
            if (::fstat(descriptor_, &status) != 0 || !S_ISREG(status.st_mode)) {
                release();
                throw std::runtime_error("Upload directory lock is not a regular file");
            }
            const int operation = mode == Mode::ExclusiveTry ? LOCK_EX | LOCK_NB : LOCK_SH;
            int result{};
            do {
                result = ::flock(descriptor_, operation);
            } while (result != 0 && errno == EINTR);
            if (result == 0) {
                locked_ = true;
                return;
            }
            const auto error = errno;
            release();
            if (mode == Mode::ExclusiveTry && error == EWOULDBLOCK) {
                return;
            }
            throw std::system_error(error, std::generic_category(), "lock upload directory");
#endif
        }

        ~DirectoryLock() { release(); }

        DirectoryLock(const DirectoryLock&) = delete;
        DirectoryLock& operator=(const DirectoryLock&) = delete;

        bool locked() const { return locked_; }

      private:
        void release() noexcept {
#ifdef _WIN32
            if (handle_ != INVALID_HANDLE_VALUE) {
                CloseHandle(handle_);
                handle_ = INVALID_HANDLE_VALUE;
            }
#else
            if (descriptor_ >= 0) {
                ::close(descriptor_);
                descriptor_ = -1;
            }
#endif
        }

        bool locked_{};
#ifdef _WIN32
        HANDLE handle_{ INVALID_HANDLE_VALUE };
#else
        int descriptor_{ -1 };
#endif
    };

    // 删除中断上传遗留的暂存文件；内核文件锁排除仍在写入的上传（含其他进程），异常退出后锁自动释放。
    // 目录排他锁被占用（并发上传正在创建或另一恢复进行中）时返回 nullopt，表示本轮未执行；重复执行幂等。
    static std::optional<std::size_t> recoverAbandoned(const std::filesystem::path& directory) {
        std::error_code error;
        if (!std::filesystem::exists(directory, error)) {
            if (error) {
                throw std::filesystem::filesystem_error("inspect upload directory", directory, error);
            }
            return std::size_t{ 0 };
        }
        DirectoryLock directoryClaim(directory, DirectoryLock::Mode::ExclusiveTry);
        if (!directoryClaim.locked()) {
            return std::nullopt;
        }
        std::size_t removed{};
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            const auto path = entry.path();
            if (path.extension() != ".upload" || !service::common::isUuid(path.stem().string())) {
                continue;
            }
            const auto status = entry.symlink_status(error);
            if (error == std::errc::no_such_file_or_directory) {
                error.clear();
                continue;
            }
            if (error) {
                throw std::filesystem::filesystem_error("inspect staged upload", path, error);
            }
            if (!std::filesystem::is_regular_file(status)) {
                continue;
            }
            StagedFileLock claim(path, false);
            if (!claim.locked()) {
                continue;
            }
            if (std::filesystem::remove(path, error)) {
                ++removed;
            } else if (error && error != std::errc::no_such_file_or_directory) {
                throw std::filesystem::filesystem_error("remove abandoned upload", path, error);
            }
        }
        return removed;
    }

    FirmwareUpload(std::string id, std::filesystem::path directory, std::uint64_t size)
        : id(std::move(id)), path(std::move(directory) / (this->id + ".upload")), size(size), digest_(EVP_MD_CTX_new(), &EVP_MD_CTX_free) {
        if (size == 0 || size > kMaxBytes) {
            throw std::invalid_argument("Invalid upload size");
        }
        if (!digest_ || EVP_DigestInit_ex(digest_.get(), EVP_sha256(), nullptr) != 1) {
            throw std::runtime_error("Cannot initialize upload digest");
        }
        // 并发上传共享目录锁互不阻塞；恢复扫描持排他锁时等待其结束，而不是失败。
        DirectoryLock directoryClaim(path.parent_path(), DirectoryLock::Mode::SharedWait);
        fileLock_ = std::make_unique<StagedFileLock>(path, true);
        output_.open(path, std::ios::binary | std::ios::trunc);
        if (!output_) {
            throw std::runtime_error("Cannot create upload file");
        }
    }

    ~FirmwareUpload() {
        output_.close();
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }

    FirmwareUpload(const FirmwareUpload&) = delete;
    FirmwareUpload& operator=(const FirmwareUpload&) = delete;

    void appendBytes(std::string_view bytes) {
        if (finished_ || bytes.empty() || bytes.size() > kChunkBytes || bytes.size() > size - received) {
            throw std::invalid_argument("Upload exceeds declared size");
        }
        output_.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        if (!output_ || EVP_DigestUpdate(digest_.get(), bytes.data(), bytes.size()) != 1) {
            throw std::runtime_error("Cannot persist upload chunk");
        }
        received += bytes.size();
    }

    std::string finish() {
        if (received != size) {
            throw std::invalid_argument("Upload is incomplete");
        }
        if (finished_) {
            return hash_;
        }
        output_.flush();
        if (!output_) {
            throw std::runtime_error("Cannot flush upload file");
        }
        output_.close();
        std::array<unsigned char, 32> hash{};
        unsigned length{};
        if (EVP_DigestFinal_ex(digest_.get(), hash.data(), &length) != 1 || length != hash.size()) {
            throw std::runtime_error("Cannot finish upload digest");
        }
        finished_ = true;
        constexpr char digits[] = "0123456789abcdef";
        std::string result;
        result.reserve(64);
        for (auto value : hash) {
            result += digits[value >> 4];
            result += digits[value & 15];
        }
        hash_ = result;
        return hash_;
    }

    const std::string id;
    const std::filesystem::path path;
    const std::uint64_t size;
    std::uint64_t received{};

  private:
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest_;
    std::unique_ptr<StagedFileLock> fileLock_;
    std::ofstream output_;
    bool finished_{};
    std::string hash_;
};

class EdgeService {
  public:
    template <typename Context>
    ruvia::task<std::string> dtuChannels(Context& c, std::string_view nodeId) {
        ruvia::db_query node(c.pool());
        node.select(booleanText(node, db::jsonText(node, node.column(entities::EdgeNodeEntity::column<"capability">().name()), "dtu")))
            .from(entities::EdgeNodeEntity::table_name()).where(node.binary(node.column(entities::EdgeNodeEntity::column<"id">().name()), Op::equal, node.cast(node.value(nodeId), Type::uuid)));
        const auto nodes = co_await c.db().query(node);
        if (nodes.empty()) service::common::fail(17001, "边缘节点不存在", 404);
        std::string result = nodes.front()[0].template as<bool>().value_or(false) ? "{\"supported\":true,\"channels\":[" : "{\"supported\":false,\"channels\":[";
        ruvia::db_query query(c.pool());
        query.select({query.column(entities::EdgeDtuEntity::column<"config">().name()), query.column(entities::EdgeDtuEntity::column<"status">().name())})
            .from(entities::EdgeDtuEntity::table_name()).where(query.binary(query.column(entities::EdgeDtuEntity::column<"node_id">().name()), Op::equal, query.cast(query.value(nodeId), Type::uuid)))
            .order_by(query.column(entities::EdgeDtuEntity::column<"channel_id">().name()));
        const auto rows = co_await c.db().query(query);
        bool first = true;
        for (const auto& row : rows) {
            if (!first) result += ',';
            first = false;
            std::string config(row[0].value().value_or("{}"));
            config.pop_back();
            result += config + ",\"status\":" + std::string(row[1].value().value_or("{}")) + "}";
        }
        co_return result + "]}";
    }

    template <typename Context>
    ruvia::task<void> saveDtuChannel(Context& c, std::string_view nodeId, const DtuChannelBody& body, std::string_view json) {
        co_await requireNodeCapability(c, nodeId, "dtu", "DTU 透传");
        const auto stringValue = [](const auto& value, std::string_view fallback) {
            return value ? std::string(value->view()) : std::string(fallback);
        };
        module_pb::DtuConfig config;
        std::array<std::uint8_t,16> id{};
        const auto channelId = body.get<"channelId">().view();
        if (!service::common::uuidBytes(channelId, id.data())) service::common::fail(17003, "通道 ID 无效", 400);
        config.set_channel_id(binary(id.data(), id.size()));
        config.set_name(body.get<"name">().view());
        config.set_enabled(body.get<"enabled">().value_);
        config.set_debug_enabled(body.get<"debugEnabled">() && body.get<"debugEnabled">()->value_);
        config.set_north_host(body.get<"northHost">().view());
        config.set_north_port(static_cast<std::uint32_t>(body.get<"northPort">().value_));
        config.set_max_clients(static_cast<std::uint32_t>(body.get<"maxClients">().value_));
        config.set_queue_bytes(static_cast<std::uint32_t>(body.get<"queueBytes">().value_));
        config.set_serial_frame_ms(static_cast<std::uint32_t>(body.get<"serialFrameMs">().value_));
        config.set_uplink_only(body.get<"uplinkOnly">() && body.get<"uplinkOnly">()->value_);
        const auto mode = body.get<"southMode">().view();
        if (mode == "serial") {
            const auto path = stringValue(body.get<"serialPath">(), "");
            const auto baud = body.get<"baudRate">() ? body.get<"baudRate">()->value_ : 9600;
            const auto bits = body.get<"dataBits">() ? body.get<"dataBits">()->value_ : 8;
            const auto stops = body.get<"stopBits">() ? body.get<"stopBits">()->value_ : 1;
            const auto parity = stringValue(body.get<"parity">(), "none");
            constexpr std::array<std::int64_t,11> rates{300,600,1200,2400,4800,9600,19200,38400,57600,115200,230400};
            if (!path.starts_with("/dev/") || std::ranges::find(rates, baud) == rates.end() || bits < 5 || bits > 8 || stops < 1 || stops > 2 ||
                (parity != "none" && parity != "even" && parity != "odd")) service::common::fail(17003, "串口参数无效", 400);
            config.set_south_mode(module_pb::LINK_MODE_SERIAL);
            auto* serial = config.mutable_serial();
            serial->set_channel(path); serial->set_baud_rate(static_cast<std::uint32_t>(baud));
            serial->set_data_bits(static_cast<std::uint32_t>(bits)); serial->set_stop_bits(static_cast<std::uint32_t>(stops));
            serial->set_parity(parity); serial->set_rs485(body.get<"rs485">() && body.get<"rs485">()->value_);
        } else {
            const auto host = stringValue(body.get<"southHost">(), "");
            if (host.empty() || !body.get<"southPort">()) service::common::fail(17003, "请输入南向地址和端口", 400);
            config.set_south_mode(mode == "tcp_server" ? module_pb::LINK_MODE_TCP_SERVER : module_pb::LINK_MODE_TCP_CLIENT);
            config.set_south_host(host); config.set_south_port(static_cast<std::uint32_t>(body.get<"southPort">()->value_));
        }
        const auto decodePacket = [](std::string_view hex) {
            std::string packet;
            if (hex.size() > 512 || hex.size() % 2) service::common::fail(17003, "报文 HEX 必须为完整字节且不超过 256 字节", 400);
            for (std::size_t i = 0; i < hex.size(); i += 2) {
                const int high = service::common::hexDigit(hex[i]), low = service::common::hexDigit(hex[i+1]);
                if (high < 0 || low < 0) service::common::fail(17003, "报文 HEX 无效", 400);
                packet.push_back(static_cast<char>((high << 4) | low));
            }
            return packet;
        };
        config.set_registration(decodePacket(stringValue(body.get<"registrationHex">(), "")));
        config.set_heartbeat(decodePacket(stringValue(body.get<"heartbeatHex">(), "")));
        const auto heartbeatInterval = body.get<"heartbeatIntervalSec">() ? body.get<"heartbeatIntervalSec">()->value_ : 0;
        if (heartbeatInterval < 0 || heartbeatInterval > 86400 || (heartbeatInterval && config.heartbeat().empty()))
            service::common::fail(17003, "心跳间隔应为 0～86400 秒，启用时须填写心跳包", 400);
        config.set_heartbeat_interval_sec(static_cast<std::uint32_t>(heartbeatInterval));
        const auto validHost = [](std::string_view host) {
            return !host.empty() && std::ranges::all_of(host, [](unsigned char value) {
                return std::isalnum(value) || value == '.' || value == '-' || value == '_' || value == ':' || value == '%';
            });
        };
        if (!validHost(config.north_host()) || (mode != "serial" && !validHost(config.south_host())))
            service::common::fail(17003, "地址只能填写域名或 IP，不能包含空白、URL 或控制字符", 400);
        std::string wire; config.SerializeToString(&wire);
        std::string encoded;
        constexpr char digits[] = "0123456789abcdef";
        for (unsigned char byte : wire) { encoded += digits[byte >> 4]; encoded += digits[byte & 15]; }
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query lock(c.pool());
        lock.select(lock.column(entities::EdgeNodeEntity::column<"id">().name())).from(entities::EdgeNodeEntity::table_name())
            .where(lock.binary(lock.column(entities::EdgeNodeEntity::column<"id">().name()), Op::equal, lock.cast(lock.value(nodeId), Type::uuid))).lock({.mode_=ruvia::db_row_lock::update});
        (void)co_await transaction.query(lock);
        ruvia::db_query existing(c.pool());
        existing.select({existing.column(entities::EdgeDtuEntity::column<"channel_id">().name()), existing.column(entities::EdgeDtuEntity::column<"wire_hex">().name())}).from(entities::EdgeDtuEntity::table_name())
            .where(existing.binary(existing.column(entities::EdgeDtuEntity::column<"node_id">().name()), Op::equal, existing.cast(existing.value(nodeId), Type::uuid)));
        const auto rows = co_await transaction.query(existing);
        bool found = false;
        for (const auto& row : rows) {
            if (row[0].value().value_or("") == channelId) { found = true; continue; }
            if (!config.enabled()) continue;
            const auto encodedOther = row[1].value().value_or("");
            std::string wireOther;
            for (std::size_t i = 0; i + 1 < encodedOther.size(); i += 2)
                wireOther.push_back(static_cast<char>((service::common::hexDigit(encodedOther[i]) << 4) | service::common::hexDigit(encodedOther[i+1])));
            module_pb::DtuConfig other;
            if (!other.ParseFromString(wireOther)) service::common::fail(17005, "已存透传配置无法解析", 500);
            if (!other.enabled() || other.south_mode() != config.south_mode()) continue;
            if ((mode == "serial" && other.serial().channel() == config.serial().channel()) ||
                (mode == "tcp_server" && other.south_port() == config.south_port()))
                service::common::fail(17003, "该南向串口或监听端口已被另一个透传通道占用", 409);
        }
        if (!found && rows.size() >= 8) service::common::fail(17003, "每个节点最多 8 个透传通道", 409);
        ruvia::db_query save(c.pool());
        save.insert_into(entities::EdgeDtuEntity::table_name(), {entities::EdgeDtuEntity::column<"node_id">().name(), entities::EdgeDtuEntity::column<"channel_id">().name(), entities::EdgeDtuEntity::column<"config">().name(), entities::EdgeDtuEntity::column<"wire_hex">().name()})
            .values({save.cast(save.value(nodeId), Type::uuid), save.cast(save.value(channelId), Type::uuid), save.cast(save.value(json), Type::jsonb), save.value(encoded)});
        ruvia::db_conflict_options conflict;
        conflict.columns_ = {std::string(entities::EdgeDtuEntity::column<"node_id">().name()), std::string(entities::EdgeDtuEntity::column<"channel_id">().name())};
        conflict.update_ = {{std::string(entities::EdgeDtuEntity::column<"config">().name()), save.excluded(entities::EdgeDtuEntity::column<"config">().name())},
                           {std::string(entities::EdgeDtuEntity::column<"wire_hex">().name()), save.excluded(entities::EdgeDtuEntity::column<"wire_hex">().name())},
                           {std::string(entities::EdgeDtuEntity::column<"status">().name()), save.cast(save.value("{}"), Type::jsonb)}};
        save.on_conflict(conflict);
        (void)co_await transaction.execute(save);
        co_await transaction.commit();
        try { (void)co_await queueSnapshot(c, nodeId, c.userId); }
        catch (...) { service::common::fail(17005, "配置已保存，下发未成功，请点击同步配置重试", 502); }
    }

    template <typename Context>
    ruvia::task<void> deleteDtuChannel(Context& c, std::string_view nodeId, std::string_view channelId) {
        ruvia::db_query query(c.pool());
        query.delete_from(entities::EdgeDtuEntity::table_name()).where(query.binary(query.column(entities::EdgeDtuEntity::column<"node_id">().name()), Op::equal, query.cast(query.value(nodeId), Type::uuid)))
            .and_where(query.binary(query.column(entities::EdgeDtuEntity::column<"channel_id">().name()), Op::equal, query.cast(query.value(channelId), Type::uuid)));
        (void)co_await c.db().execute(query);
        try { (void)co_await queueSnapshot(c, nodeId, c.userId); }
        catch (...) { service::common::fail(17005, "配置已删除，下发未成功，请点击同步配置重试", 502); }
    }

    template <typename Context>
    static ruvia::task<std::uint64_t> queueSnapshot(Context& context, std::string_view nodeId, std::string_view userId) {
        if (!service::common::isUuid(nodeId)) {
            service::common::fail(17001, "边缘节点不存在", 404);
        }
        const auto revision = co_await service::utils::callRpc(
            context,
            "edge",
            "queue-snapshot",
            service::message::rpc::encodeLinePair(nodeId, userId)
        );
        const auto parsed = service::utils::parseInt64(
            std::optional<std::string_view>(revision)
        );
        if (!parsed || *parsed < 0) {
            service::common::fail(17005, "边缘配置生成失败", 502);
        }
        co_return static_cast<std::uint64_t>(*parsed);
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<EdgeNodeDto>> inventory(Context& c) {
        auto query = nodeSelect(c.pool());
        query.order_by(query.column(entities::EdgeNodeEntity::column<"created_at">().name(), "node"), ruvia::db_order_direction::desc)
            .add_order_by(query.column(entities::EdgeNodeEntity::column<"id">().name(), "node"));
        co_return co_await nodeRows(c, query);
    }

    // Inventory rows of `ids` that still exist (same row shape as inventory()).
    template <typename Context>
    ruvia::task<ruvia::boxed_array<EdgeNodeDto>> inventory(Context& c, const service::utils::ChangeSet::Ids& ids) {
        auto query = nodeSelect(c.pool());
        const auto values = uuidList(query, ids);
        if (values.empty()) {
            co_return ruvia::boxed_array<EdgeNodeDto>(ruvia::model_options{ .resource_ = c.arena() });
        }
        query.where(query.binary(query.column(entities::EdgeNodeEntity::column<"id">().name(), "node"), Op::in, query.list(values)));
        co_return co_await nodeRows(c, query);
    }

    // Node ids whose inventory row may differ after `changes`. nullopt: a change
    // is not entity-scoped and the whole inventory must be resent. `vpnNodes`
    // are nodes last shown with VPN routes; a removed peer is no longer found
    // through its network, so they are re-read on every VPN change.
    template <typename Context>
    ruvia::task<std::optional<service::utils::ChangeSet::Ids>> changedNodeIds(Context& c, const service::utils::ChangeSet& changes, const service::utils::ChangeSet::Ids& vpnNodes) {
        namespace topic = service::message::live::topic;
        service::utils::ChangeSet::Ids result;
        if (const auto* ids = changes.find(topic::kEdge)) {
            if (!*ids) {
                co_return std::nullopt;
            }
            result.insert((*ids)->begin(), (*ids)->end());
        }
        if (const auto* ids = changes.find(topic::kEdgeGroup)) {
            if (!*ids) {
                co_return std::nullopt;
            }
            ruvia::db_query query(c.pool());
            const auto groups = uuidList(query, **ids);
            if (!groups.empty()) {
                query.select(query.cast(query.column(entities::EdgeNodeEntity::column<"id">().name()), Type::text))
                    .from(entities::EdgeNodeEntity::table_name())
                    .where(query.binary(query.column(entities::EdgeNodeEntity::column<"group_id">().name()), Op::in, query.list(groups)));
                for (const auto& row : co_await c.db().query(query)) {
                    result.emplace(row[0].value().value_or(std::string_view{}));
                }
            }
        }
        if (const auto* ids = changes.find(topic::kVpn)) {
            if (!*ids) {
                co_return std::nullopt;
            }
            result.insert(vpnNodes.begin(), vpnNodes.end());
            for (const auto& node : co_await networkNodes(c, **ids)) {
                result.insert(node);
            }
        }
        co_return result;
    }

    // Edge nodes holding a VPN peer in one of `networks`.
    template <typename Context>
    ruvia::task<service::utils::ChangeSet::Ids> networkNodes(Context& c, const service::utils::ChangeSet::Ids& networks) {
        using Peer = service::vpn::entities::VpnPeerEntity;
        service::utils::ChangeSet::Ids result;
        ruvia::db_query query(c.pool());
        const auto values = uuidList(query, networks);
        if (values.empty()) {
            co_return result;
        }
        const auto node = query.column(Peer::column<"edge_node_id">().name());
        query.select(query.cast(node, Type::text))
            .from(Peer::table_name())
            .where(query.binary(query.column(Peer::column<"network_id">().name()), Op::in, query.list(values)))
            .and_where(query.unary(Unary::is_not_null, node));
        for (const auto& row : co_await c.db().query(query)) {
            result.emplace(row[0].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    // VPN networks in which `nodeId` holds a peer.
    template <typename Context>
    ruvia::task<service::utils::ChangeSet::Ids> nodeNetworks(Context& c, std::string_view nodeId) {
        using Peer = service::vpn::entities::VpnPeerEntity;
        service::utils::ChangeSet::Ids result;
        ruvia::db_query query(c.pool());
        query.select(query.cast(query.column(Peer::column<"network_id">().name()), Type::text))
            .from(Peer::table_name())
            .where(query.binary(query.column(Peer::column<"edge_node_id">().name()), Op::equal, query.cast(query.value(nodeId), Type::uuid)));
        for (const auto& row : co_await c.db().query(query)) {
            result.emplace(row[0].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<std::string> vpnState(Context& c, std::string_view nodeId) {
        co_await service::auth::AuthService::requirePermission(c, c.userId, "iot:vpn:query");
        const auto peers = co_await service::vpn::vpnService().peers(c, std::nullopt, std::string(nodeId));
        const auto routes = co_await service::vpn::vpnService().routes(c, std::nullopt, std::string(nodeId));
        co_return "{\"peers\":" + peers + ",\"routes\":" + routes + "}";
    }

    template <typename Context>
    ruvia::task<EdgePageDto> list(Context& c, std::int64_t page, std::int64_t pageSize, std::optional<std::string> keyword, std::optional<std::string> status, std::optional<std::string> groupId) {
        page = std::max<std::int64_t>(1, page);
        pageSize = std::clamp<std::int64_t>(pageSize, 1, 100);
        if (page - 1 > std::numeric_limits<std::int64_t>::max() / pageSize) {
            service::common::fail(17003, "分页范围无效", 400);
        }
        ruvia::db_query count(c.pool());
        count.select(count.aggregate("count", { count.star() }))
            .from(entities::EdgeNodeEntity::table_name(), "node")
            .join(ruvia::db_join_type::left, entities::EdgeNodeGroupEntity::table_name(), count.binary(count.column(entities::EdgeNodeEntity::column<"group_id">().name(), "node"), ruvia::db_binary_operator::equal, count.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "group_item")), "group_item");
        applyNodeFilters(count, keyword, status, groupId);
        const auto countRows = co_await c.db().query(count);
        const auto total = countRows.empty()
            ? std::int64_t{ 0 }
            : countRows.front()[0].template as<std::int64_t>().value_or(0);

        ruvia::db_query query = nodeSelect(c.pool());
        applyNodeFilters(query, keyword, status, groupId);
        query.order_by(query.column(entities::EdgeNodeEntity::column<"created_at">().name(), "node"), ruvia::db_order_direction::desc)
            .limit(static_cast<std::uint64_t>(pageSize))
            .offset(static_cast<std::uint64_t>((page - 1) * pageSize));
        const auto rows = co_await c.db().query(query);
        ruvia::boxed_array<EdgeNodeDto> nodes(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& node = nodes.emplace(ruvia::model_options{ .resource_ = c.arena() });
            co_await fillNode(c, node, row);
        }
        EdgePageDto result(ruvia::model_options{ .resource_ = c.arena() });
        result.template set<"list">(std::move(nodes))
            .template set<"total">(total)
            .template set<"page">(page)
            .template set<"pageSize">(pageSize)
            .template set<"totalPages">(total == 0 ? 0 : (total + pageSize - 1) / pageSize);
        co_return result;
    }

    template <typename Context>
    ruvia::task<ruvia::boxed_array<EdgeGroupDto>> groups(Context& c) {
        ruvia::db_query query(c.pool());
        const auto groupId = query.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "group_item");
        const auto parentId = query.column(entities::EdgeNodeGroupEntity::column<"parent_id">().name(), "group_item");
        const auto name = query.column(entities::EdgeNodeGroupEntity::column<"name">().name(), "group_item");
        const auto status = query.column(entities::EdgeNodeGroupEntity::column<"status">().name(), "group_item");
        const auto sortOrder = query.column(entities::EdgeNodeGroupEntity::column<"sort_order">().name(), "group_item");
        const auto remark = query.column(entities::EdgeNodeGroupEntity::column<"remark">().name(), "group_item");
        query.select({ query.cast(groupId, ruvia::db_data_type::text), name, query.coalesce({ query.cast(parentId, ruvia::db_data_type::text), query.cast(query.value(""), ruvia::db_data_type::text) }), status, sortOrder, query.coalesce({ remark, query.cast(query.value(""), ruvia::db_data_type::text) }), query.aggregate("count", { query.column(entities::EdgeNodeEntity::column<"id">().name(), "node") }) })
            .from(entities::EdgeNodeGroupEntity::table_name(), "group_item")
            .join(ruvia::db_join_type::left, entities::EdgeNodeEntity::table_name(), query.binary(query.column(entities::EdgeNodeEntity::column<"group_id">().name(), "node"), ruvia::db_binary_operator::equal, groupId), "node")
            .where(query.unary(ruvia::db_unary_operator::is_null, query.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name(), "group_item")))
            .group_by({ groupId, name, parentId, status, sortOrder, remark })
            .order_by(sortOrder)
            .add_order_by(groupId);
        const auto rows = co_await c.db().query(query);
        ruvia::boxed_array<EdgeGroupDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& item = result.emplace(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"id">(row[0].value().value_or(std::string_view{}));
            item.template set<"name">(row[1].value().value_or(std::string_view{}));
            item.template set<"parentId">(row[2].value().value_or(std::string_view{}));
            item.template set<"status">(row[3].value().value_or(std::string_view{}));
            item.template set<"sortOrder">(integer(row[4].value().value_or(std::string_view{})));
            item.template set<"remark">(row[5].value().value_or(std::string_view{}));
            item.template set<"nodeCount">(integer(row[6].value().value_or(std::string_view{})));
        }
        co_return result;
    }

    template <typename Context>
    ruvia::task<void> createGroup(Context& c, const EdgeGroupBody& body) {
        const auto name = std::string(body.get<"name">().view());
        const auto parentId = body.get<"parentId">()
            ? std::string(body.get<"parentId">()->view())
            : std::string{};
        co_await validateGroupParent(c, parentId, {});
        ruvia::db_query duplicate(c.pool());
        duplicate.select(duplicate.cast(duplicate.value(1), ruvia::db_data_type::integer))
            .from(entities::EdgeNodeGroupEntity::table_name())
            .where(duplicate.binary(duplicate.column(entities::EdgeNodeGroupEntity::column<"name">().name()), ruvia::db_binary_operator::equal, duplicate.value(name)))
            .and_where(duplicate.unary(ruvia::db_unary_operator::is_null, duplicate.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name())))
            .limit(1);
        const auto duplicateRows = co_await c.db().query(duplicate);
        if (!duplicateRows.empty()) {
            service::common::fail(17002, "边缘节点分组名称已存在", 409);
        }
        const auto status = body.get<"status">()
            ? std::string(body.get<"status">()->view())
            : std::string{ "enabled" };
        const auto sortOrder = body.get<"sortOrder">()
            ? static_cast<std::int64_t>(*body.get<"sortOrder">())
            : 0;
        const auto remark = body.get<"remark">()
            ? std::string(body.get<"remark">()->view())
            : std::string{};
        const auto groupId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        ruvia::db_query query(c.pool());
        query.insert_into(entities::EdgeNodeGroupEntity::table_name(), { entities::EdgeNodeGroupEntity::column<"id">().name(), entities::EdgeNodeGroupEntity::column<"name">().name(), entities::EdgeNodeGroupEntity::column<"parent_id">().name(), entities::EdgeNodeGroupEntity::column<"status">().name(), entities::EdgeNodeGroupEntity::column<"sort_order">().name(), entities::EdgeNodeGroupEntity::column<"remark">().name(), entities::EdgeNodeGroupEntity::column<"created_by">().name() })
            .values({ query.cast(query.value(groupId), ruvia::db_data_type::uuid), query.value(name), db::nullableUuid(query, parentId), query.cast(query.value(status), { .custom_name_ = "status_enum" }), query.cast(query.value(sortOrder), ruvia::db_data_type::integer), nullableText(query, remark), query.cast(query.value(c.userId), ruvia::db_data_type::uuid) });
        (void)co_await c.db().execute(query);
    }

    template <typename Context>
    ruvia::task<void> updateGroup(Context& c, std::string_view id, const EdgeGroupBody& body) {
        ruvia::db_query current(c.pool());
        current.select(current.cast(current.value(1), ruvia::db_data_type::integer))
            .from(entities::EdgeNodeGroupEntity::table_name())
            .where(current.binary(current.column(entities::EdgeNodeGroupEntity::column<"id">().name()), ruvia::db_binary_operator::equal, current.cast(current.value(id), ruvia::db_data_type::uuid)))
            .and_where(current.unary(ruvia::db_unary_operator::is_null, current.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name())))
            .limit(1);
        const auto currentRows = co_await c.db().query(current);
        if (currentRows.empty()) {
            service::common::fail(17001, "边缘节点分组不存在", 404);
        }
        const auto name = std::string(body.get<"name">().view());
        const auto parentId = body.get<"parentId">()
            ? std::string(body.get<"parentId">()->view())
            : std::string{};
        co_await validateGroupParent(c, parentId, id);
        ruvia::db_query duplicate(c.pool());
        duplicate.select(duplicate.cast(duplicate.value(1), ruvia::db_data_type::integer))
            .from(entities::EdgeNodeGroupEntity::table_name())
            .where(duplicate.binary(duplicate.column(entities::EdgeNodeGroupEntity::column<"name">().name()), ruvia::db_binary_operator::equal, duplicate.value(name)))
            .and_where(duplicate.binary(
                duplicate.column(entities::EdgeNodeGroupEntity::column<"id">().name()),
                ruvia::db_binary_operator::not_equal,
                duplicate.cast(duplicate.value(id), ruvia::db_data_type::uuid)
            ))
            .and_where(duplicate.unary(ruvia::db_unary_operator::is_null, duplicate.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name())))
            .limit(1);
        const auto duplicateRows = co_await c.db().query(duplicate);
        if (!duplicateRows.empty()) {
            service::common::fail(17002, "边缘节点分组名称已存在", 409);
        }
        const auto status = body.get<"status">()
            ? std::string(body.get<"status">()->view())
            : std::string{ "enabled" };
        const auto sortOrder = body.get<"sortOrder">()
            ? static_cast<std::int64_t>(*body.get<"sortOrder">())
            : 0;
        const auto remark = body.get<"remark">()
            ? std::string(body.get<"remark">()->view())
            : std::string{};
        ruvia::db_query query(c.pool());
        query.update(entities::EdgeNodeGroupEntity::table_name())
            .set(entities::EdgeNodeGroupEntity::column<"name">().name(), query.value(name))
            .set(entities::EdgeNodeGroupEntity::column<"parent_id">().name(), db::nullableUuid(query, parentId))
            .set(entities::EdgeNodeGroupEntity::column<"status">().name(), query.cast(query.value(status), { .custom_name_ = "status_enum" }))
            .set(entities::EdgeNodeGroupEntity::column<"sort_order">().name(), query.cast(query.value(sortOrder), ruvia::db_data_type::integer))
            .set(entities::EdgeNodeGroupEntity::column<"remark">().name(), nullableText(query, remark))
            .set(entities::EdgeNodeGroupEntity::column<"updated_at">().name(), query.call("now"))
            .where(query.binary(query.column(entities::EdgeNodeGroupEntity::column<"id">().name()), ruvia::db_binary_operator::equal, query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .and_where(query.unary(ruvia::db_unary_operator::is_null, query.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name())));
        (void)co_await c.db().execute(query);
    }

    template <typename Context>
    ruvia::task<void> removeGroup(Context& c, std::string_view id) {
        ruvia::db_query current(c.pool());
        current.select(current.cast(current.value(1), ruvia::db_data_type::integer))
            .from(entities::EdgeNodeGroupEntity::table_name())
            .where(current.binary(current.column(entities::EdgeNodeGroupEntity::column<"id">().name()), ruvia::db_binary_operator::equal, current.cast(current.value(id), ruvia::db_data_type::uuid)))
            .and_where(current.unary(ruvia::db_unary_operator::is_null, current.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name())))
            .limit(1);
        const auto currentRows = co_await c.db().query(current);
        if (currentRows.empty()) {
            service::common::fail(17001, "边缘节点分组不存在", 404);
        }

        ruvia::db_query removal(c.pool());
        const auto targetId = removal.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "target");
        ruvia::db_query child(c.pool());
        child.select(child.cast(child.value(1), ruvia::db_data_type::integer))
            .from(entities::EdgeNodeGroupEntity::table_name(), "child")
            .where(child.binary(
                child.column(entities::EdgeNodeGroupEntity::column<"parent_id">().name(), "child"),
                ruvia::db_binary_operator::equal,
                child.import_expression(targetId, "target", "target")
            ))
            .and_where(child.unary(ruvia::db_unary_operator::is_null, child.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name(), "child")))
            .limit(1);
        ruvia::db_query node(c.pool());
        node.select(node.cast(node.value(1), ruvia::db_data_type::integer))
            .from(entities::EdgeNodeEntity::table_name(), "node")
            .where(node.binary(
                node.column(entities::EdgeNodeEntity::column<"group_id">().name(), "node"),
                ruvia::db_binary_operator::equal,
                node.import_expression(targetId, "target", "target")
            ))
            .limit(1);
        removal.update(entities::EdgeNodeGroupEntity::table_name(), "target")
            .set(entities::EdgeNodeGroupEntity::column<"deleted_at">().name(), removal.call("now"))
            .set(entities::EdgeNodeGroupEntity::column<"updated_at">().name(), removal.call("now"))
            .where(removal.binary(targetId, ruvia::db_binary_operator::equal, removal.cast(removal.value(id), ruvia::db_data_type::uuid)))
            .and_where(removal.unary(ruvia::db_unary_operator::is_null, removal.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name(), "target")))
            .and_where(removal.unary(ruvia::db_unary_operator::not_value, removal.exists(child)))
            .and_where(removal.unary(ruvia::db_unary_operator::not_value, removal.exists(node)));
        const auto removed = co_await c.db().execute(removal);
        if (removed.affected_rows() == 0) {
            service::common::fail(17004, "请先移除子分组和边缘节点", 409);
        }
    }

    template <typename Context>
    ruvia::task<EdgeNodeDto> detail(Context& c, std::string_view id) {
        ruvia::db_query query = nodeSelect(c.pool());
        query.where(query.binary(query.column(entities::EdgeNodeEntity::column<"id">().name(), "node"), ruvia::db_binary_operator::equal, query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17001, "边缘节点不存在", 404);
        }
        EdgeNodeDto node(ruvia::model_options{ .resource_ = c.arena() });
        co_await fillNode(c, node, rows.front());
        node.template set<"interfaces">(co_await interfaces(c, id));
        node.template set<"networks">(co_await networks(c, id));
        node.template set<"serialPorts">(co_await serialPorts(c, id));
        node.template set<"tasks">(co_await tasks(c, id));
        co_return node;
    }

    template <typename Context>
    ruvia::task<void> setEnrollment(Context& c, std::string_view id, const EnrollmentBody& body) {
        const auto& statusValue = body.get<"status">();
        if (statusValue.view().empty()) {
            service::common::fail(17003, "注册状态不能为空", 400);
        }
        const std::string status(statusValue.view());
        if (status != "approved") {
            service::common::fail(17003, "注册状态无效", 400);
        }
        const std::string name = body.get<"name">() ? std::string(body.get<"name">()->view()) : std::string{};
        std::string_view stage = "database";
        try {
            ruvia::db_query update(c.pool());
            update.update(entities::EdgeNodeEntity::table_name())
                .set(entities::EdgeNodeEntity::column<"enrollment_status">().name(), update.value(status))
                .set(entities::EdgeNodeEntity::column<"name">().name(), nullableText(update, name))
                .set(entities::EdgeNodeEntity::column<"approved_by">().name(), update.cast(update.value(c.userId), ruvia::db_data_type::uuid))
                .set(entities::EdgeNodeEntity::column<"approved_at">().name(), update.call("now"))
                .set(entities::EdgeNodeEntity::column<"updated_at">().name(), update.call("now"))
                .where(update.binary(update.column(entities::EdgeNodeEntity::column<"id">().name()), ruvia::db_binary_operator::equal, update.cast(update.value(id), ruvia::db_data_type::uuid)))
                .returning({ update.column(entities::EdgeNodeEntity::column<"imei">().name()) });
            const auto updated = co_await c.db().query(update);
            if (updated.empty()) {
                service::common::fail(17001, "边缘节点不存在", 404);
            }
            const auto key = service::message::edge::authKey(
                updated.front()[0].value().value_or(std::string_view{})
            );
            const auto value = std::string(id) + "|" + status;
            stage = "redis";
            co_await c.redis().set(key, value);
            if (status == "approved") {
                try {
                    // Approval must be useful without a second manual click. Queue the
                    // current device snapshot immediately; the gateway will deliver it
                    // as soon as the pending WebSocket becomes an enrolled session.
                    (void)co_await queueSnapshot(c, id, c.userId);
                } catch (const std::exception& error) {
                    // A node may be approved before its first capability projection or
                    // before it has any configurable devices. Approval itself must not
                    // fail in those compatible/empty cases; the existing heartbeat retry
                    // and manual sync endpoint remain available.
                    std::cerr << "edge enrollment initial config sync skipped: node_id=" << id
                              << " error=" << error.what() << '\n';
                } catch (...) {
                    std::cerr << "edge enrollment initial config sync skipped: node_id=" << id
                              << " error=unknown exception\n";
                }
            }
        } catch (const std::exception& error) {
            std::cerr << "edge enrollment update failed: stage=" << stage << " node_id=" << id
                      << " status=" << status << " error=" << error.what() << '\n';
            throw;
        }
    }

    // 只删除仍处于待处理状态的注册申请；状态谓词与删除在同一语句内，防止并发批准后误删。
    static ruvia::db_query pendingEnrollmentRemoval(std::pmr::memory_resource* resource, std::string_view id) {
        ruvia::db_query removal(resource);
        removal.delete_from(entities::EdgeNodeEntity::table_name())
            .where((entities::EdgeNodeEntity::column<"id">() == id && entities::EdgeNodeEntity::column<"enrollment_status">() == "pending").expression(removal))
            .returning({ removal.column(entities::EdgeNodeEntity::column<"id">().name()) });
        return removal;
    }

    template <typename Context>
    ruvia::task<void> removeEnrollment(Context& c, std::string_view id) {
        ruvia::db_query query(c.pool());
        query.select({ query.column(entities::EdgeNodeEntity::column<"imei">().name()), query.column(entities::EdgeNodeEntity::column<"enrollment_status">().name()) })
            .from(entities::EdgeNodeEntity::table_name())
            .where((entities::EdgeNodeEntity::column<"id">() == id).expression(query))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17001, "边缘节点不存在", 404);
        }
        const auto imei = std::string(rows.front()[0].value().value_or(std::string_view{}));
        if (rows.front()[1].value().value_or(std::string_view{}) != "pending") {
            service::common::fail(17021, "只能删除待处理的注册申请", 409);
        }

        auto transaction = co_await c.db().begin_transaction();
        const auto removal = pendingEnrollmentRemoval(c.pool(), id);
        std::optional<ruvia::db_rows> removed;
        try {
            removed.emplace(co_await transaction.query(removal));
        } catch (const ruvia::db_error& error) {
            // 采集链路以 ON DELETE RESTRICT 外键引用节点（fk_link_edge_node）；节点不查询链路表，
            // 由数据库约束拒绝删除仍被引用的注册申请。
            if (error.sql_state() != std::optional<std::string_view>{ kForeignKeyViolation }) {
                throw;
            }
        }
        if (!removed) {
            service::common::fail(17021, "注册申请仍被采集链路引用，无法删除", 409);
        }
        if (removed->empty()) {
            service::common::fail(17021, "注册状态已变化，请刷新后重试", 409);
        }
        const std::array<std::string, 5> keys{
            service::message::edge::sessionKey(id),
            service::message::edge::metadataKey(id),
            service::message::edge::egressKey(id),
            service::message::edge::configKey(id),
            service::message::edge::configRevisionKey(id),
        };
        co_await c.redis().set(service::message::edge::authKey(imei), std::string(id) + "|pending");
        for (const auto& key : keys) {
            (void)co_await c.redis().del(key);
        }
        co_await transaction.commit();
        (void)co_await c.redis().del(service::message::edge::authKey(imei));
        // 删除节点会级联删除 edge_task，固件可能失去引用；通知固件回收重算最近截止时间。
        co_await scheduleFirmwareCleanup(c, "remove-enrollment");
    }

    template <typename Context>
    ruvia::task<void> renameNode(Context& c, std::string_view id, const NodeNameBody& body) {
        const auto& nameValue = body.get<"name">();
        if (nameValue.view().empty()) {
            service::common::fail(17003, "节点名称不能为空", 400);
        }
        const std::string name(nameValue.view());
        auto transaction = co_await c.db().begin_transaction();
        ruvia::db_query current(c.pool());
        current.select(current.column(entities::EdgeNodeEntity::column<"name">().name()))
            .from(entities::EdgeNodeEntity::table_name())
            .where((entities::EdgeNodeEntity::column<"id">() == id).expression(current))
            .lock(ruvia::db_lock_options{});
        const auto nodes = co_await transaction.query(current);
        if (nodes.empty())
            service::common::fail(17001, "边缘节点不存在", 404);
        if (nodes.front()[0].value().value_or("") == name) {
            co_await transaction.commit();
            co_return;
        }
        ruvia::db_query update(c.pool());
        update.update(entities::EdgeNodeEntity::table_name())
            .set(entities::EdgeNodeEntity::column<"name">().name(), update.value(name))
            .set(entities::EdgeNodeEntity::column<"updated_at">().name(), update.call("now"))
            .where((entities::EdgeNodeEntity::column<"id">() == id).expression(update))
            .returning({ update.column(entities::EdgeNodeEntity::column<"id">().name()) });
        const auto updated = co_await transaction.query(update);
        if (updated.empty()) {
            service::common::fail(17001, "边缘节点不存在", 404);
        }
        // 链路列表展示边缘连接所属节点名称；节点模块不读取链路表，名称实际变化时通知链路查询。
        co_await service::system::OutboxService::enqueueQueryChange(transaction, "link", "link",
            service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>()));
        co_await transaction.commit();
    }

    template <typename Context>
    ruvia::task<void> setNodeGroup(Context& c, std::string_view id, const NodeGroupBody& body) {
        const auto& maybeGroup = body.get<"groupId">();
        if (!maybeGroup) {
            service::common::fail(17003, "节点分组参数不能为空", 400);
        }
        const std::string groupId(maybeGroup->view());
        if (!groupId.empty()) {
            ruvia::db_query group(c.pool());
            group.select(group.cast(group.value(1), ruvia::db_data_type::integer))
                .from(entities::EdgeNodeGroupEntity::table_name())
                .where(group.binary(group.column(entities::EdgeNodeGroupEntity::column<"id">().name()), ruvia::db_binary_operator::equal, group.cast(group.value(groupId), ruvia::db_data_type::uuid)))
                .and_where(group.binary(group.column(entities::EdgeNodeGroupEntity::column<"status">().name()), ruvia::db_binary_operator::equal, group.cast(group.value("enabled"), { .custom_name_ = "status_enum" })))
                .and_where(group.unary(ruvia::db_unary_operator::is_null, group.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name())))
                .limit(1);
            const auto groupRows = co_await c.db().query(group);
            if (groupRows.empty()) {
                service::common::fail(17001, "边缘节点分组不存在或已停用", 404);
            }
        }
        ruvia::db_query update(c.pool());
        update.update(entities::EdgeNodeEntity::table_name())
            .set(entities::EdgeNodeEntity::column<"group_id">().name(), db::nullableUuid(update, groupId))
            .set(entities::EdgeNodeEntity::column<"updated_at">().name(), update.call("now"))
            .where(update.binary(update.column(entities::EdgeNodeEntity::column<"id">().name()), ruvia::db_binary_operator::equal, update.cast(update.value(id), ruvia::db_data_type::uuid)))
            .returning({ update.column(entities::EdgeNodeEntity::column<"id">().name()) });
        const auto updated = co_await c.db().query(update);
        if (updated.empty()) {
            service::common::fail(17001, "边缘节点不存在", 404);
        }
    }

    template <typename Context>
    ruvia::task<void> queueNetwork(Context& c, std::string_view nodeId, const NetworkBody& body) {
        const auto networkConfigVersion = co_await requireNetworkManagement(c, nodeId);
        const auto& configs = body.get<"interfaces">();
        if (configs.empty()) {
            service::common::fail(17003, "至少配置一个网络接口", 400);
        }
        const auto available = co_await manageableInterfaces(c, nodeId);
        std::unordered_set<std::string> names;
        std::unordered_set<std::string> previousNames;
        std::unordered_set<std::string> devices;
        const auto taskId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        module_pb::NetworkConfigRequest request;
        std::uint8_t requestId[16]{};
        if (!service::common::uuidBytes(taskId, requestId)) {
            service::common::fail(17005, "边缘命令编码失败", 500);
        }
        request.set_request_id(binary(requestId, 16));
        for (const auto& config : configs) {
            if (config.get<"operation">().view().empty()) {
                service::common::fail(17003, "网络接口操作不能为空", 400);
            }
            if (config.get<"name">().view().empty()) {
                service::common::fail(17003, "逻辑接口名称不能为空", 400);
            }
            const std::string operation(config.get<"operation">().view());
            const std::string name(config.get<"name">().view());
            if (operation != "upsert" && operation != "delete") {
                service::common::fail(17003, "网络接口操作只支持 upsert 或 delete", 400);
            }
            const std::string previousName =
                config.get<"previousName">() ? std::string(config.get<"previousName">()->view())
                                             : std::string{};
            if (!names.emplace(name).second) {
                service::common::fail(17003, "同一请求不能重复配置逻辑接口 " + name, 400);
            }
            if (name == "loopback") {
                service::common::fail(17003, "loopback 接口不允许远程修改", 400);
            }
            if (!previousName.empty()) {
                if (operation != "upsert" || previousName == name ||
                    previousName == "loopback") {
                    service::common::fail(17003, "原逻辑接口名称无效", 400);
                }
                if (networkConfigVersion < 3) {
                    service::common::fail(
                        17004,
                        "节点代理版本过旧，请先升级后再修改逻辑接口名称",
                        409
                    );
                }
                if (!previousNames.emplace(previousName).second) {
                    service::common::fail(
                        17003,
                        "同一请求不能重复修改原逻辑接口 " + previousName,
                        400
                    );
                }
            }

            auto* item = request.add_interfaces();
            item->set_logical_name(name);
            if (operation == "delete") {
                item->set_operation(module_pb::NETWORK_CONFIG_DELETE);
            } else {
                item->set_operation(module_pb::NETWORK_CONFIG_UPSERT);
                item->set_previous_logical_name(previousName);
                const std::string mode =
                    config.get<"mode">() ? std::string(config.get<"mode">()->view()) : std::string{};
                const std::string device = config.get<"device">()
                    ? std::string(config.get<"device">()->view())
                    : std::string{};
                const std::string ip =
                    config.get<"ip">() ? std::string(config.get<"ip">()->view()) : std::string{};
                const std::string gateway =
                    config.get<"gateway">() ? std::string(config.get<"gateway">()->view()) : std::string{};
                const bool bridge = config.get<"bridge">() && *config.get<"bridge">();
                const auto prefix =
                    config.get<"prefixLength">() ? static_cast<std::uint32_t>(*config.get<"prefixLength">()) : 0U;
                const auto& ports = config.get<"bridgePorts">();
                validateNetworkConfig(name, mode, device, bridge, ports, ip, prefix, gateway, available, devices);
                item->set_mode(mode == "static" ? module_pb::NETWORK_ADDRESS_STATIC : module_pb::NETWORK_ADDRESS_DHCP);
                item->set_bridge(bridge);
                item->set_device(device);
                item->set_name(bridge ? "br-" + name : device);
                if (ports) {
                    for (const auto& port : *ports) {
                        item->add_bridge_ports(port.view());
                    }
                }
                item->set_ip(ip);
                item->set_prefix_length(prefix);
                item->set_gateway(gateway);
            }
        }
        const auto rollbackTimeoutSec = body.get<"rollbackTimeoutSec">().value_or(60);
        if (rollbackTimeoutSec < 30 || rollbackTimeoutSec > 300) {
            service::common::fail(17003, "回滚等待时间必须在 30 - 300 秒之间", 400);
        }
        request.set_rollback_timeout_sec(static_cast<std::uint32_t>(rollbackTimeoutSec));
        co_await createNetworkTaskAndQueue(c, nodeId, taskId, configs.size(), request);
    }

    template <typename Context>
    ruvia::task<void> validateFirmwareTarget(Context& c, std::string_view nodeId) {
        co_await requireNodeCapability(c, nodeId, "firmwareUpdate", "远程刷写");
    }

    template <typename Context, typename Transaction>
    ruvia::task<module_pb::FirmwareUpdateRequest> createFirmwareTask(Context& c, Transaction& transaction, std::string_view nodeId, std::string_view firmwareId, bool keepSettings) {
        const std::string firmwareIdText(firmwareId);
        ruvia::db_query query(c.pool());
        query.select({ query.column(entities::EdgeFirmwareEntity::column<"sha256">().name(), "firmware"), query.column(entities::EdgeFirmwareEntity::column<"size_bytes">().name(), "firmware"), query.column(entities::EdgeFirmwareEntity::column<"download_token">().name(), "firmware"), booleanText(query, db::jsonText(query, query.column(entities::EdgeNodeEntity::column<"capability">().name(), "node"), "firmwareStream")) })
            .from(entities::EdgeFirmwareEntity::table_name(), "firmware")
            .join(ruvia::db_join_type::inner, entities::EdgeNodeEntity::table_name(), query.binary(query.column(entities::EdgeNodeEntity::column<"id">().name(), "node"), ruvia::db_binary_operator::equal, query.cast(query.value(nodeId), ruvia::db_data_type::uuid)), "node")
            .where(query.binary(query.column(entities::EdgeFirmwareEntity::column<"id">().name(), "firmware"), ruvia::db_binary_operator::equal, query.cast(query.value(firmwareIdText), ruvia::db_data_type::uuid)))
            .limit(1);
        const auto rows = co_await transaction.query(query);
        if (rows.empty()) {
            service::common::fail(17009, "固件不存在", 404);
        }
        const auto& row = rows.front();
        const auto taskId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        module_pb::FirmwareUpdateRequest request;
        std::uint8_t bytes[32]{};
        if (!service::common::uuidBytes(taskId, bytes)) {
            service::common::fail(17010, "固件请求 ID 无效", 500);
        }
        const auto requestId = binary(bytes, 16);
        std::string download;
        if (row[3].value().value_or(std::string_view{}) != "t") {
            auto baseUrl = c.template worker_state<FirmwareStorageConfig>().publicBaseUrl;
            while (baseUrl.ends_with('/')) {
                baseUrl.pop_back();
            }
            download = baseUrl +
                "/edge/v1/firmware/" + firmwareIdText + "/download?token=" +
                std::string(row[2].value().value_or(std::string_view{}));
        }
        if (!hex(row[0].value().value_or(std::string_view{}), bytes, 32)) {
            service::common::fail(17010, "固件摘要无效", 500);
        }
        const auto size = static_cast<std::uint64_t>(
            integer(row[1].value().value_or(std::string_view{}))
        );
        if (download.size() > 512 || size == 0) {
            service::common::fail(17010, "固件请求元数据无效", 500);
        }
        request.set_request_id(requestId);
        request.set_download_url(download);
        request.set_sha256(binary(bytes, 32));
        request.set_size_bytes(size);
        request.set_keep_settings(keepSettings);
        const std::string json = "{\"firmware_id\":\"" + firmwareIdText + "\"}";
        ruvia::db_query task(c.pool());
        task.insert_into(entities::EdgeTaskEntity::table_name(), { entities::EdgeTaskEntity::column<"id">().name(), entities::EdgeTaskEntity::column<"node_id">().name(), entities::EdgeTaskEntity::column<"task_type">().name(), entities::EdgeTaskEntity::column<"request">().name(), entities::EdgeTaskEntity::column<"created_by">().name() })
            .values({ task.cast(task.value(taskId), Type::uuid), task.cast(task.value(nodeId), Type::uuid), task.value("firmware"), task.cast(task.value(json), Type::jsonb), task.cast(task.value(c.userId), Type::uuid) });
        (void)co_await transaction.execute(task);
        co_return request;
    }

    template <typename Transaction>
    static ruvia::task<void> lockFirmwareStorage(Transaction& transaction) {
        // 与后台回收使用同一事务锁；同一 Worker 只使用此事务的连接。
        ruvia::db_query lock;
        lock.select(lock.call("pg_advisory_xact_lock", {
            lock.cast(lock.value(service::message::lock::kFirmwareStorageClass), Type::integer), lock.cast(lock.value(service::message::lock::kFirmwareStorageObject), Type::integer)}));
        (void)co_await transaction.query(lock);
    }

    template <typename Transaction>
    static ruvia::task<std::string> findReusableFirmware(Transaction& transaction, std::string_view hash, std::int64_t bytes) {
        ruvia::db_query query;
        query.select({query.cast(query.column(entities::EdgeFirmwareEntity::column<"id">().name()), Type::text), query.column(entities::EdgeFirmwareEntity::column<"storage_path">().name())})
            .from(entities::EdgeFirmwareEntity::table_name())
            .where(query.binary(query.column(entities::EdgeFirmwareEntity::column<"sha256">().name()), Op::equal, query.value(hash)))
            .and_where(query.binary(query.column(entities::EdgeFirmwareEntity::column<"size_bytes">().name()), Op::equal, query.value(bytes)))
            .order_by(query.column(entities::EdgeFirmwareEntity::column<"created_at">().name()), ruvia::db_order_direction::desc);
        const auto rows = co_await transaction.query(query);
        for (const auto& row : rows) {
            const std::filesystem::path path(std::string(row[1].value().value_or(std::string_view{})));
            std::error_code error;
            if (std::filesystem::is_regular_file(std::filesystem::symlink_status(path, error)) && !error &&
                std::filesystem::file_size(path, error) == static_cast<std::uint64_t>(bytes) && !error)
                co_return std::string(row[0].value().value_or(std::string_view{}));
        }
        co_return std::string{};
    }

    template <typename Context>
    ruvia::task<bool> reuseFirmware(Context& c, std::string_view nodeId, const FirmwareReuseBody& body) {
        co_await validateFirmwareTarget(c, nodeId);
        const auto hash = body.get<"sha256">().view();
        std::uint8_t digest[32]{};
        if (hash.size() != 64 || !hex(hash, digest, 32) ||
            hash.find_first_not_of("0123456789abcdef") != std::string_view::npos)
            service::common::fail(17017, "固件摘要无效", 400);
        auto transaction = co_await c.db().begin_transaction();
        co_await lockFirmwareStorage(transaction);
        const auto id = co_await findReusableFirmware(transaction, hash, body.get<"sizeBytes">().value_);
        if (id.empty())
            co_return false;
        auto request = co_await createFirmwareTask(c, transaction, nodeId, id, body.get<"keepSettings">()->value_);
        co_await transaction.commit();
        co_await queueControl(c, nodeId, "queue-firmware", request);
        co_return true;
    }

    template <typename Context>
    ruvia::task<std::filesystem::path> prepareFirmwareUpload(Context& c, std::string_view nodeId) {
        co_await validateFirmwareTarget(c, nodeId);
        std::error_code error;
        auto directory = std::filesystem::absolute(c.template worker_state<FirmwareStorageConfig>().directory, error);
        if (error || (!std::filesystem::create_directories(directory, error) && error)) {
            service::common::fail(17011, "无法创建固件目录", 500);
        }
        co_return directory.lexically_normal();
    }

    // 中断上传恢复：进程启动后由认领本进程实例恢复键（Redis SET NX）的 Service Worker 执行一次，
    // 其他 Worker 跳过。目录被并发上传或其他进程的恢复占用时本轮未执行：释放认领并稍后重新认领，
    // 保证恢复确实完成；恢复幂等，失败时释放认领并上报异常。
    template <typename Context>
    static ruvia::task<std::size_t> recoverFirmwareUploads(Context& context, const std::filesystem::path& directory) {
        const auto key = entities::firmwareUploadRecoveryKey(context.template worker_state<std::optional<common::WorkerIdentity>>().value().instanceId);
        ruvia::redis_set_options claim;
        claim.condition_ = ruvia::redis_set_condition::if_absent;
        claim.expiration_ = ruvia::redis_set_expiration::expires_after(kUploadRecoveryClaimTtl);
        while (true) {
            const auto claimed = co_await context.redis().set(key, "1", claim);
            if (!claimed.applied()) {
                co_return 0;
            }
            std::optional<std::size_t> removed;
            std::exception_ptr failure;
            try {
                removed = FirmwareUpload::recoverAbandoned(directory);
            } catch (...) {
                failure = std::current_exception();
            }
            if (removed) {
                co_return *removed;
            }
            (void)co_await context.redis().del(key);
            if (failure) {
                std::rethrow_exception(failure);
            }
            LOG_INFO << "edge firmware upload directory busy, recovery retry scheduled: directory=" << directory.string();
            if (co_await ruvia::sleep_for(context.worker(), kUploadRecoveryRetry, context.get_stop_token()) != ruvia::timer_sleep_result::elapsed) {
                co_return 0;
            }
        }
    }

    template <typename Context>
    ruvia::task<void> finishFirmwareUpload(Context& c, std::string_view nodeId, const FirmwareUploadBody& body, const std::filesystem::path& uploadedPath, std::string_view hash, std::int64_t bytes) {
        const auto directory = co_await prepareFirmwareUpload(c, nodeId);
        const auto source = uploadedPath.lexically_normal();
        if (source.empty() || source.parent_path() != directory ||
            source.extension() != ".upload" || !service::common::isUuid(source.stem().string()) ||
            hash.size() != 64 || bytes != body.get<"sizeBytes">().value_) {
            service::common::fail(17017, "固件上传尚未完成", 400);
        }
        std::error_code error;
        if (std::filesystem::file_size(source, error) != static_cast<std::uint64_t>(bytes) || error) {
            service::common::fail(17017, "固件大小校验失败", 400);
        }
        bool stored = false;
        std::exception_ptr failure;
        try {
            auto transaction = co_await c.db().begin_transaction();
            co_await lockFirmwareStorage(transaction);
            auto storageId = co_await findReusableFirmware(transaction, hash, bytes);
            if (storageId.empty()) {
                storageId = source.stem().string();
                const auto destination = directory / (storageId + ".bin");
                std::filesystem::rename(source, destination, error);
                if (error)
                    service::common::fail(17014, "无法保存固件文件", 500);
                stored = true;
                // 提交结果不确定时保留文件，由后台在确认无引用且超过保留期后回收。
                const auto fileName = std::filesystem::path(std::string(body.get<"fileName">().view())).filename().string();
                const auto token = randomToken();
                const auto storagePath = destination.string();
                ruvia::db_query query(c.pool());
                query.insert_into(entities::EdgeFirmwareEntity::table_name(), { entities::EdgeFirmwareEntity::column<"id">().name(), entities::EdgeFirmwareEntity::column<"version">().name(), entities::EdgeFirmwareEntity::column<"file_name">().name(), entities::EdgeFirmwareEntity::column<"storage_path">().name(), entities::EdgeFirmwareEntity::column<"sha256">().name(), entities::EdgeFirmwareEntity::column<"size_bytes">().name(), entities::EdgeFirmwareEntity::column<"download_token">().name(), entities::EdgeFirmwareEntity::column<"created_by">().name() })
                    .values({ query.cast(query.value(storageId), Type::uuid), query.value(""), query.value(fileName), query.value(storagePath), query.value(hash), query.cast(query.value(bytes), Type::big_int), query.value(token), query.cast(query.value(c.userId), Type::uuid) });
                (void)co_await transaction.execute(query);
            }
            auto request = co_await createFirmwareTask(c, transaction, nodeId, storageId, body.get<"keepSettings">()->value_);
            co_await transaction.commit();
            stored = true;
            co_await queueControl(c, nodeId, "queue-firmware", request);
        } catch (...) {
            failure = std::current_exception();
        }
        // 新文件落盘或固件任务提交后，回收截止时间可能变化；无论本次上传成败都通知重算。
        if (stored) {
            co_await scheduleFirmwareCleanup(c, "firmware-upload");
        }
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    template <typename Context>
    ruvia::task<std::pair<std::filesystem::path, std::string>> firmwareDownload(
        Context& c,
        std::string_view id,
        std::string_view token
    ) {
        ruvia::db_query query(c.pool());
        query.select({ query.column(entities::EdgeFirmwareEntity::column<"storage_path">().name()), query.column(entities::EdgeFirmwareEntity::column<"file_name">().name()) })
            .from(entities::EdgeFirmwareEntity::table_name())
            .where(query.binary(query.column(entities::EdgeFirmwareEntity::column<"id">().name()), ruvia::db_binary_operator::equal, query.cast(query.value(id), ruvia::db_data_type::uuid)))
            .and_where(query.binary(query.column(entities::EdgeFirmwareEntity::column<"download_token">().name()), ruvia::db_binary_operator::equal, query.value(token)))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17009, "固件不存在或下载凭据无效", 404);
        }
        co_return std::pair<std::filesystem::path, std::string>{
            std::filesystem::path(std::string(rows.front()[0].value().value_or(std::string_view{}))),
            std::string(rows.front()[1].value().value_or(std::string_view{}))
        };
    }

    // Edge debug WebSocket events. The controller registers these handlers;
    // each runs on the Service Worker that accepted the connection.
    ruvia::task<debug::EventResult> authenticateDebugConnection(debug::EventContext& c, const DebugAuthenticationBody& body) {
        const auto revision = c.identity.revision;
        const auto token = body.get<"token">().view();
        const auto principal = service::auth::AuthTokenService::verifyAccessToken(c, token);
        (void)co_await service::auth::authService().current(c, principal.userId);
        if (c.stop.stop_requested() || c.identity.revision != revision) {
            throw debug::EventError(10002, "认证操作已取消或会话已变更");
        }
        c.identity.accessToken.assign(token);
        ++c.identity.revision;
        co_return debug::EventResult{ "null", {}, true };
    }

    ruvia::task<debug::EventResult> openSerial(debug::EventContext& c, const NodeSerialOpenInput& body) {
        if (c.resources.size() >= 8) {
            throw debug::EventError(17021, "当前连接的调试会话过多");
        }
        const auto session = std::make_shared<debug::SerialSession>(service::utils::nextUuid(c.worker_state<service::common::UuidV7State>()), c.userId, std::string(body.get<"id">().view()), c.identity.connectionId);
        const auto key = "serial:" + session->id;
        c.resources.emplace(key, session);
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        std::exception_ptr failure;
        try {
            co_await requireNodeCapability(c, session->node, "serialDebug", "串口调试");
            const std::string path(body.get<"path">().view());
            if (!path.starts_with("/dev/") || path.find_first_of("\r\n") != std::string::npos) {
                service::common::fail(17021, "串口路径无效", 400);
            }
            ruvia::db_query port(c.pool());
            port.select(port.column(entities::EdgeNodeSerialEntity::column<"available">().name()))
                .from(entities::EdgeNodeSerialEntity::table_name())
                .where(port.binary(port.column(entities::EdgeNodeSerialEntity::column<"node_id">().name()), Op::equal, port.cast(port.value(session->node), Type::uuid)))
                .and_where(port.binary(port.column(entities::EdgeNodeSerialEntity::column<"path">().name()), Op::equal, port.value(path)));
            const auto rows = co_await c.db().query(port);
            if (rows.empty() || !rows.front()[0].template as<bool>().value_or(false)) {
                service::common::fail(17021, "节点串口不存在或不可用", 409);
            }
            const auto result = co_await serialOperation(c, session->node, session->id, session->connection, "serial-open", [&path](service::message::serial_debug::Request& request) {
                request.set<"path">(path);
            });
            const auto parsed = ruvia::from_json<service::message::serial_debug::OpenReply>(result, { .resource_ = c.arena() });
            if (!parsed || parsed->get<"id">().view() != session->id) {
                service::common::fail(17021, "串口会话响应无效", 502);
            }
            if (c.stop.stop_requested()) {
                throw debug::EventError(17021, "串口打开已取消");
            }
            co_return debug::EventResult{ result };
        } catch (...) {
            failure = std::current_exception();
        }
        session->lifetime.request_stop();
        c.stop = {};
        co_await releaseResource(c, key, *session);
        std::rethrow_exception(failure);
    }

    ruvia::task<debug::EventResult> sendSerialCommand(debug::EventContext& c, const NodeSerialCommandInput& body) {
        const auto payload = ruvia::json_value::parse(c.request.data);
        const auto command = payload->get<ruvia::json_value>("command");
        if (!command || command->view().size() > 4096) {
            throw debug::EventError(10001, "串口指令过大");
        }
        const auto session = serialSession(c, body.get<"id">().view(), body.get<"sessionId">().view());
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        (void)co_await serialOperation(c, session->node, session->id, session->connection, "serial-command", [&command](service::message::serial_debug::Request& request) {
            request.set<"command">(*command);
        });
        co_return debug::EventResult{};
    }

    ruvia::task<debug::EventResult> serialEvents(debug::EventContext& c, const NodeSerialSessionInput& body) {
        const auto session = serialSession(c, body.get<"id">().view(), body.get<"sessionId">().view());
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        if (!c.state.has_value()) {
            c.state.emplace<std::int64_t>(0);
        }
        auto& cursor = std::any_cast<std::int64_t&>(c.state);
        const auto after = cursor;
        const auto result = co_await serialOperation(c, session->node, session->id, session->connection, "serial-events", [after](service::message::serial_debug::Request& request) {
            request.set<"after">(after);
        });
        const auto parsed = ruvia::from_json<service::message::serial_debug::EventsReply>(result, { .resource_ = c.arena() });
        if (!parsed || parsed->get<"cursor">().value_ < cursor) {
            service::common::fail(17021, "串口事件响应无效", 502);
        }
        cursor = parsed->get<"cursor">().value_;
        co_return debug::EventResult{ result, service::message::serial_debug::eventTopic(session->node, session->id), false, session->lifetime.token() };
    }

    ruvia::task<debug::EventResult> closeSerial(debug::EventContext& c, const NodeSerialSessionInput& body) {
        const auto key = "serial:" + std::string(body.get<"sessionId">().view());
        if (!c.resources.contains(key)) {
            co_return debug::EventResult{};
        }
        const auto session = serialSession(c, body.get<"id">().view(), body.get<"sessionId">().view());
        session->lifetime.request_stop();
        co_await session->close(c);
        c.resources.erase(key);
        co_return debug::EventResult{};
    }

    ruvia::task<debug::EventResult> openTerminal(debug::EventContext& c, const TerminalOpenInput& body) {
        if (c.resources.size() >= 8) {
            throw debug::EventError(17018, "当前连接的调试会话过多");
        }
        const auto session = std::make_shared<debug::TerminalSession>(service::utils::nextUuid(c.worker_state<service::common::UuidV7State>()), c.userId, std::string(body.get<"id">().view()), c.identity.connectionId, static_cast<unsigned>(body.get<"columns">().value_), static_cast<unsigned>(body.get<"rows">().value_));
        const auto key = "terminal:" + session->id;
        c.resources.emplace(key, session);
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        std::exception_ptr failure;
        try {
            ruvia::db_query query(c.pool());
            query.select({ query.column(entities::EdgeNodeEntity::column<"enrollment_status">().name()), booleanText(query, db::jsonText(query, query.column(entities::EdgeNodeEntity::column<"capability">().name()), "terminal")) })
                .from(entities::EdgeNodeEntity::table_name())
                .where(query.binary(query.column(entities::EdgeNodeEntity::column<"id">().name()), Op::equal, query.cast(query.value(session->node), Type::uuid)))
                .limit(1);
            const auto nodes = co_await c.db().query(query);
            if (nodes.empty()) {
                service::common::fail(17001, "边缘节点不存在", 404);
            }
            if (nodes.front()[0].value().value_or(std::string_view{}) != "approved" ||
                !nodes.front()[1].template as<bool>().value_or(false)) {
                service::common::fail(17018, "节点未检测到 ttyd", 409);
            }
            if (!co_await c.redis().get(service::message::edge::sessionKey(session->node))) {
                service::common::fail(17019, "节点当前离线", 409);
            }
            const auto reply = co_await terminalOperation(c, session->node, session->id, session->connection, "terminal-open", [&session](service::message::terminal::Request& request) {
                request.set<"columns">(session->columns).set<"rows">(session->rows);
            });
            const auto parsed = ruvia::from_json<service::message::terminal::OpenReply>(reply, { .resource_ = c.arena() });
            const auto version = parsed ? parsed->get<"protocolVersion">().value_ : 0;
            if (c.stop.stop_requested() || !parsed || parsed->get<"id">().view() != session->id || version < 1 || version > 100) {
                throw debug::EventError(17018, "终端打开已取消或响应无效");
            }
            session->protocolVersion = static_cast<unsigned>(version);
            co_return debug::EventResult{ "{\"id\":" + service::utils::jsonQuoted(session->id) + "}" };
        } catch (...) {
            failure = std::current_exception();
        }
        session->lifetime.request_stop();
        c.stop = {};
        co_await releaseResource(c, key, *session);
        std::rethrow_exception(failure);
    }

    ruvia::task<debug::EventResult> terminalEvents(debug::EventContext& c, const TerminalSessionInput& body) {
        const auto session = terminalSession(c, body);
        if (!c.state.has_value()) {
            if (!session->outputSubscription.expired()) {
                throw debug::EventError(17018, "终端已有输出订阅");
            }
            const auto subscription = std::make_shared<std::string>(c.request.id);
            session->outputSubscription = subscription;
            c.state = subscription;
        }
        const auto reply = co_await terminalOperation(c, session->node, session->id, session->connection, "terminal-events");
        const auto parsed = ruvia::from_json<service::message::terminal::EventsReply>(reply, { .resource_ = c.arena() });
        if (!parsed) {
            throw debug::EventError(17018, "终端输出响应无效");
        }
        co_return debug::EventResult{ reply, service::message::terminal::terminalEventTopic(session->node, session->id), false, session->lifetime.token(), parsed->get<"more">().value_, true };
    }

    ruvia::task<debug::EventResult> writeTerminal(debug::EventContext& c, const TerminalWriteInput& body) {
        const auto session = terminalSession(c, body);
        if (session->writing) {
            throw debug::EventError(17018, "上一笔终端输入尚未确认");
        }
        session->writing = true;
        std::exception_ptr failure;
        try {
            co_await writeTerminalInput(c, *session, body.get<"content">().view());
        } catch (...) {
            failure = std::current_exception();
        }
        session->writing = false;
        if (failure) {
            session->lifetime.request_stop();
            c.stop = {};
            co_await releaseResource(c, "terminal:" + session->id, *session);
            std::rethrow_exception(failure);
        }
        co_return debug::EventResult{};
    }

    ruvia::task<debug::EventResult> resizeTerminal(debug::EventContext& c, const TerminalResizeInput& body) {
        const auto session = terminalSession(c, body);
        session->columns = static_cast<unsigned>(body.get<"columns">().value_);
        session->rows = static_cast<unsigned>(body.get<"rows">().value_);
        (void)co_await terminalOperation(c, session->node, session->id, session->connection, "terminal-resize", [&session](service::message::terminal::Request& request) {
            request.set<"columns">(session->columns).set<"rows">(session->rows);
        });
        co_return debug::EventResult{};
    }

    ruvia::task<debug::EventResult> keepTerminalAlive(debug::EventContext& c, const TerminalSessionInput& body) {
        const auto session = terminalSession(c, body);
        (void)co_await terminalOperation(c, session->node, session->id, session->connection, "terminal-keepalive", [&session](service::message::terminal::Request& request) {
            request.set<"columns">(session->columns).set<"rows">(session->rows);
        });
        co_return debug::EventResult{};
    }

    ruvia::task<debug::EventResult> acknowledgeTerminalOutput(debug::EventContext& c, const TerminalAckInput& body) {
        const auto session = terminalSession(c, body);
        (void)co_await terminalOperation(c, session->node, session->id, session->connection, "terminal-output-ack", [&body](service::message::terminal::Request& request) {
            request.set<"sequence">(body.get<"sequence">().value_);
        });
        co_return debug::EventResult{};
    }

    ruvia::task<debug::EventResult> closeTerminal(debug::EventContext& c, const TerminalSessionInput& body) {
        const auto key = "terminal:" + std::string(body.get<"sessionId">().view());
        if (!c.resources.contains(key)) {
            co_return debug::EventResult{};
        }
        const auto session = terminalSession(c, body);
        session->lifetime.request_stop();
        c.stop = {};
        co_await session->close(c);
        c.resources.erase(key);
        co_return debug::EventResult{};
    }

    template <typename Context, typename Fill = void (*)(service::message::serial_debug::Request&)>
    ruvia::task<std::string> serialOperation(Context& c, std::string_view nodeId, std::string_view sessionId, std::string_view connectionId, std::string_view operation, Fill fill = [](service::message::serial_debug::Request&) {}) {
        if (!service::common::isUuid(connectionId) ||
            (!sessionId.empty() && !service::common::isUuid(sessionId))) {
            service::common::fail(17021, "串口会话身份无效", 400);
        }
        service::message::serial_debug::Request request(ruvia::model_options{ .resource_ = c.arena() });
        request.template set<"nodeId">(nodeId).template set<"userId">(c.userId).template set<"connectionId">(connectionId).template set<"sessionId">(sessionId);
        fill(request);
        co_return co_await service::utils::callRpc(c, "edge-serial", operation, service::message::rpc::encode(request, c.arena()));
    }

    template <typename Context, typename Fill = void (*)(service::message::terminal::Request&)>
    ruvia::task<std::string> terminalOperation(Context& c, std::string_view nodeId, std::string_view sessionId, std::string_view connectionId, std::string_view operation, Fill fill = [](service::message::terminal::Request&) {}) {
        service::message::terminal::Request request(ruvia::model_options{ .resource_ = c.arena() });
        request.template set<"nodeId">(nodeId).template set<"sessionId">(sessionId).template set<"connectionId">(connectionId).template set<"userId">(c.userId);
        fill(request);
        co_return co_await service::utils::callRpc(c, "edge-terminal", operation, service::message::rpc::encode(request, c.arena()));
    }

    template <typename Context>
    ruvia::task<LogsDto> logSnapshot(Context& c, std::string_view nodeId, const LogsQuery& query) {
        co_await requireNodeCapability(c, nodeId, "logs", "节点日志");
        LogsDto output(ruvia::model_options{ .resource_ = c.arena() });
        ruvia::boxed_array<LogLineDto> lines(ruvia::model_options{ .resource_ = c.arena() });
        const auto payload = co_await c.redis().get(service::message::edge::logSnapshotKey(nodeId));
        if (payload) {
            module_pb::LogResult result;
            if (!result.ParseFromArray(payload->data(), static_cast<int>(payload->size()))) {
                service::common::fail(17020, "节点日志快照解析失败", 502);
            }
            const auto limit = std::clamp<std::int64_t>(query.get<"limit">().value_or(48), 1, 48);
            std::int64_t count = 0;
            for (const auto& line : result.lines()) {
                if (query.get<"level">() && query.get<"level">()->view() != line.level()) {
                    continue;
                }
                if (query.get<"source">() && query.get<"source">()->view() != line.source()) {
                    continue;
                }
                if (count++ >= limit) {
                    break;
                }
                lines.emplace(ruvia::model_options{ .resource_ = c.arena() }).template set<"time">(service::common::utcTimestampFromMilliseconds(line.time_ms())).template set<"level">(line.level()).template set<"source">(line.source()).template set<"message">(line.message()).template set<"detail">(line.detail());
            }
        }
        output.template set<"lines">(std::move(lines));
        co_return output;
    }

    template <typename Context>
    ruvia::task<LogsDto> logs(Context& c, std::string_view nodeId, const LogsQuery& query) {
        co_await requireNodeCapability(c, nodeId, "logs", "节点日志");
        const auto session = co_await c.redis().get(service::message::edge::sessionKey(nodeId));
        if (!session) {
            service::common::fail(17019, "节点当前离线", 409);
        }

        const auto requestId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        module_pb::LogRequest request;
        std::uint8_t bytes[16]{};
        if (!service::common::uuidBytes(requestId, bytes)) {
            service::common::fail(17020, "日志请求 ID 无效", 500);
        }
        request.set_request_id(binary(bytes, sizeof(bytes)));
        const auto limit = query.get<"limit">().value_or(48);
        if (limit < 1 || limit > 48) {
            service::common::fail(17020, "日志条数必须在 1 - 48 之间", 400);
        }
        request.set_limit(static_cast<std::uint32_t>(limit));
        if (query.get<"level">()) {
            const auto level = std::string(query.get<"level">()->view());
            if (level != "debug" && level != "info" && level != "warn" && level != "error") {
                service::common::fail(17020, "日志级别无效", 400);
            }
            request.set_level(level);
        }
        if (query.get<"source">()) {
            const auto sourceValue = std::string(query.get<"source">()->view());
            if (sourceValue.size() > 16) {
                service::common::fail(17020, "日志来源不能超过 16 个字符", 400);
            }
            request.set_source(sourceValue);
        }
        const auto payload = co_await requestNodeResult(c, nodeId, "request-logs", request, service::message::edge::logResultKey(requestId));
        if (!payload) {
            service::common::fail(17020, "节点日志请求超时", 504);
        }
        module_pb::LogResult result;
        if (!result.ParseFromArray(payload->data(), static_cast<int>(payload->size()))) {
            service::common::fail(17020, "节点日志回包解析失败", 502);
        }
        if (!result.success()) {
            service::common::fail(17020, result.message(), 502);
        }
        LogsDto output(ruvia::model_options{ .resource_ = c.arena() });
        ruvia::boxed_array<LogLineDto> lines(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& line : result.lines()) {
            auto& item = lines.emplace(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"time">(service::common::utcTimestampFromMilliseconds(line.time_ms()))
                .template set<"level">(line.level())
                .template set<"source">(line.source())
                .template set<"message">(line.message())
                .template set<"detail">(line.detail());
        }
        output.template set<"lines">(std::move(lines));
        co_return output;
    }

    template <typename Context>
    ruvia::task<void> setLogLevel(Context& c, std::string_view nodeId, const LogLevelBody& body) {
        co_await requireNodeCapability(c, nodeId, "logs", "节点日志");
        const auto session = co_await c.redis().get(service::message::edge::sessionKey(nodeId));
        if (!session) {
            service::common::fail(17019, "节点当前离线", 409);
        }

        const auto& levelValue = body.get<"level">();
        if (levelValue.view().empty()) {
            service::common::fail(17020, "日志级别不能为空", 400);
        }
        const auto level = std::string(levelValue.view());
        if (level != "silent" && level != "debug" && level != "info" && level != "warn" && level != "error") {
            service::common::fail(17020, "日志级别无效", 400);
        }
        const auto requestId = service::utils::nextUuid(c.template worker_state<service::common::UuidV7State>());
        module_pb::LogLevelRequest request;
        std::uint8_t bytes[16]{};
        if (!service::common::uuidBytes(requestId, bytes)) {
            service::common::fail(17020, "日志请求 ID 无效", 500);
        }
        request.set_request_id(binary(bytes, sizeof(bytes)));
        request.set_level(level);
        const auto payload = co_await requestNodeResult(c, nodeId, "set-log-level", request, service::message::edge::logLevelResultKey(requestId));
        if (!payload) {
            service::common::fail(17020, "节点日志等级请求超时", 504);
        }
        module_pb::LogLevelResult result;
        if (!result.ParseFromArray(payload->data(), static_cast<int>(payload->size()))) {
            service::common::fail(17020, "节点日志等级回包解析失败", 502);
        }
        if (!result.success()) {
            service::common::fail(17020, result.message(), 502);
        }
        const auto currentLevel = result.level().empty() ? level : result.level();
        ruvia::db_query update(c.pool());
        const auto logPath = update.array(
            { update.cast(update.value("log"), ruvia::db_data_type::text) }
        );
        const auto levelPath = update.array(
            { update.cast(update.value("log"), ruvia::db_data_type::text),
              update.cast(update.value("level"), ruvia::db_data_type::text) }
        );
        const auto withLog = update.call(
            "jsonb_set",
            { update.column(entities::EdgeNodeEntity::column<"status">().name()), logPath, update.coalesce({ db::jsonValue(update, update.column(entities::EdgeNodeEntity::column<"status">().name()), "log"), update.cast(update.value("{}"), ruvia::db_data_type::jsonb) }), update.cast(update.value(true), ruvia::db_data_type::boolean) }
        );
        const auto withLevel = update.call(
            "jsonb_set",
            { withLog, levelPath, update.call("to_jsonb", { update.cast(update.value(currentLevel), ruvia::db_data_type::text) }), update.cast(update.value(true), ruvia::db_data_type::boolean) }
        );
        update.update(entities::EdgeNodeEntity::table_name())
            .set(entities::EdgeNodeEntity::column<"status">().name(), withLevel)
            .set(entities::EdgeNodeEntity::column<"updated_at">().name(), update.call("now"))
            .where(update.binary(update.column(entities::EdgeNodeEntity::column<"id">().name()), ruvia::db_binary_operator::equal, update.cast(update.value(nodeId), ruvia::db_data_type::uuid)));
        (void)co_await c.db().execute(update);
    }

  private:
    using Op = ruvia::db_binary_operator;
    using Unary = ruvia::db_unary_operator;
    using Ids = service::utils::ChangeSet::Ids;

    // Sends a node control request and waits for the reply the gateway stores
    // at `key`. The gateway notifies only the worker routed for `key`, so the
    // route and the directed subscription exist before the request leaves.
    template <typename Context, typename Request>
    static ruvia::task<std::optional<std::string>> requestNodeResult(Context& c, std::string_view nodeId, std::string_view operation, const Request& request, const std::string& key) {
        auto changed = co_await c.template worker_state<service::utils::ChangeFeed>().await(c, key);
        co_await service::utils::ChangeFeed::route(c, key, std::chrono::seconds(15));
        co_await queueControl(c, nodeId, operation, request);
        const std::vector<std::string> cleanup{ "DEL", key, service::message::live::routeKey(key) };
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        std::optional<std::string> result;
        for (;;) {
            if (const auto payload = co_await c.redis().get(key)) {
                result.emplace(payload->data(), payload->size());
                break;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
            if (remaining.count() <= 0 || c.get_stop_token().stop_requested()) {
                break;
            }
            const auto woke = co_await changed->wait(remaining, c.get_stop_token());
            (void)changed->take();
            if (woke == service::utils::ChangeWait::Stopped) {
                break;
            }
        }
        const auto deleted = co_await c.redis().command(std::vector<std::string_view>(cleanup.begin(), cleanup.end()));
        if (deleted.kind() == ruvia::redis_value::kind_type::error) {
            service::utils::redis::throwValue("DEL edge node result", deleted);
        }
        co_return result;
    }

    // Terminal input in 4 KiB chunks. Protocol 5+ waits for the node's input
    // acknowledgement of each chunk before sending the next one.
    ruvia::task<void> writeTerminalInput(debug::EventContext& c, debug::TerminalSession& session, std::string_view encoded) {
        std::string bytes;
        try {
            bytes = service::utils::decode_base64(encoded, 16384);
        } catch (const service::utils::base64_decode_error&) {
            service::common::fail(17018, "终端输入编码无效", 400);
        }
        const auto acknowledged = co_await c.worker_state<service::utils::ChangeFeed>().await(c, service::message::terminal::terminalAckTopic(session.node, session.id));
        for (std::size_t offset = 0; offset < bytes.size(); offset += 4096) {
            if (c.get_stop_token().stop_requested()) {
                service::common::fail(17018, "终端输入已取消", 409);
            }
            const auto part = std::string_view(bytes).substr(offset, std::min<std::size_t>(4096, bytes.size() - offset));
            const auto next = session.protocolVersion >= 5 ? ++session.inputSequence : 0;
            if (next > 9007199254740991ULL) {
                service::common::fail(17018, "终端输入序号已耗尽", 409);
            }
            const auto content = service::utils::encode_base64(part);
            (void)co_await terminalOperation(c, session.node, session.id, session.connection, "terminal-write", [&content, next](service::message::terminal::Request& request) {
                request.set<"content">(content).set<"sequence">(static_cast<std::int64_t>(next));
            });
            if (session.protocolVersion < 5) {
                continue;
            }
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
            for (;;) {
                const auto reply = co_await terminalOperation(c, session.node, session.id, session.connection, "terminal-input-status", [next](service::message::terminal::Request& request) {
                    request.set<"sequence">(static_cast<std::int64_t>(next));
                });
                const auto status = ruvia::from_json<service::message::terminal::InputStatusReply>(reply, { .resource_ = c.arena() });
                if (!status) {
                    service::common::fail(17018, "终端确认格式无效", 502);
                }
                if (status->get<"acknowledged">().value_) {
                    break;
                }
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
                if (remaining.count() <= 0) {
                    service::common::fail(17018, "终端输入确认超时", 504);
                }
                const auto woke = co_await acknowledged->wait(remaining, c.get_stop_token());
                (void)acknowledged->take();
                if (c.get_stop_token().stop_requested() || woke == service::utils::ChangeWait::Stopped) {
                    service::common::fail(17018, "终端输入已取消", 409);
                }
                if (woke == service::utils::ChangeWait::TimedOut) {
                    service::common::fail(17018, "终端输入确认超时", 504);
                }
            }
        }
    }

    template <typename Body>
    static std::shared_ptr<debug::TerminalSession> terminalSession(debug::EventContext& c, const Body& body) {
        const auto found = c.resources.find("terminal:" + std::string(body.template get<"sessionId">().view()));
        const auto session = found == c.resources.end() ? nullptr : std::dynamic_pointer_cast<debug::TerminalSession>(found->second);
        if (!session || session->owner != c.userId || session->node != body.template get<"id">().view() || session->lifetime.stop_requested()) {
            throw debug::EventError(17018, "终端已结束或不属于当前连接");
        }
        c.stop = ruvia::combine_stop_tokens(c.stop, session->lifetime.token());
        return session;
    }

    static std::shared_ptr<debug::SerialSession> serialSession(debug::EventContext& c, std::string_view node, std::string_view id) {
        const auto found = c.resources.find("serial:" + std::string(id));
        const auto session = found == c.resources.end() ? nullptr : std::dynamic_pointer_cast<debug::SerialSession>(found->second);
        if (!session || session->owner != c.userId || session->node != node || session->lifetime.stop_requested()) {
            throw debug::EventError(17021, "串口会话已结束或不属于当前连接");
        }
        return session;
    }

    // Closes a resource after its operation failed. A failed close keeps the
    // stopped resource registered so connection cleanup retries it; collector
    // leases bound the remote state after process failure.
    static ruvia::task<void> releaseResource(debug::EventContext& c, const std::string& key, debug::ConnectionResource& resource) {
        try {
            co_await resource.close(c);
            c.resources.erase(key);
        } catch (const std::exception& error) {
            LOG_WARN << "edge debug resource " << key << " close failed: " << error.what();
        }
    }

    static std::vector<ruvia::db_expression> uuidList(ruvia::db_query& query, const Ids& ids) {
        std::vector<ruvia::db_expression> values;
        for (const auto& id : ids) {
            if (service::common::isUuid(id)) {
                values.push_back(query.cast(query.value(id), Type::uuid));
            }
        }
        return values;
    }

    template <typename Context>
    static ruvia::task<ruvia::boxed_array<EdgeNodeDto>> nodeRows(Context& c, const ruvia::db_query& query) {
        const auto rows = co_await c.db().query(query);
        ruvia::boxed_array<EdgeNodeDto> nodes(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& node = nodes.emplace(ruvia::model_options{ .resource_ = c.arena() });
            co_await fillNode(c, node, row);
        }
        co_return nodes;
    }

    using Type = ruvia::db_data_type;

    // PostgreSQL foreign_key_violation。
    static constexpr std::string_view kForeignKeyViolation = "23503";
    // 恢复认领只需覆盖同一进程实例内各 Worker 的启动窗口；过期仅用于清理键。
    static constexpr std::chrono::milliseconds kUploadRecoveryClaimTtl = std::chrono::hours(24);
    // 上传目录锁被占用时重新认领恢复的间隔；共享锁只覆盖创建暂存文件的瞬间。
    static constexpr std::chrono::milliseconds kUploadRecoveryRetry{ 200 };

    template <typename Context>
    static ruvia::task<void> scheduleFirmwareCleanup(Context& c, std::string_view stage) {
        try {
            (void)co_await service::utils::callRpc(c, service::message::edge::firmware::kRpcComponent,
                service::message::edge::firmware::kScheduleCleanupOperation, std::string{});
        } catch (const std::exception& error) {
            // 回收由下一次固件事件或进程重启时重算；调度失败不改变已完成的业务结果。
            LOG_WARN << "edge firmware cleanup schedule failed: stage=" << stage << " error=" << error.what();
        }
    }

    template <typename Context, typename Request>
    static ruvia::task<void> queueControl(Context& context, std::string_view nodeId, std::string_view operation, const Request& request) {
        std::string requestPayload;
        if (!request.SerializeToString(&requestPayload)) {
            service::common::fail(17005, "边缘命令编码失败", 500);
        }
        (void)co_await service::utils::callRpc(context, "edge", operation, service::message::rpc::encodeLinePair(nodeId, requestPayload));
    }

    static std::string binary(const std::uint8_t* data, std::size_t size) {
        return { reinterpret_cast<const char*>(data), size };
    }

    static ruvia::db_expression textDefault(ruvia::db_query& query, ruvia::db_expression value, std::string_view fallback = {}) {
        return query.coalesce({ value, query.cast(query.value(fallback), Type::text) });
    }

    static ruvia::db_expression nullableText(ruvia::db_query& query, std::string_view value) {
        return query.null_if(query.cast(query.value(value), Type::text), query.cast(query.value(""), Type::text));
    }

    static ruvia::db_expression booleanText(ruvia::db_query& query, ruvia::db_expression value) {
        const auto normalized = query.call(
            "lower",
            { textDefault(query, value) }
        );
        const auto trueValue = query.cast(query.value(true), Type::boolean);
        const auto falseValue = query.cast(query.value(false), Type::boolean);
        return query.case_when(
            { { query.binary(normalized, Op::equal, query.cast(query.value("true"), Type::text)), trueValue },
              { query.binary(normalized, Op::equal, query.cast(query.value("t"), Type::text)), trueValue },
              { query.binary(normalized, Op::equal, query.cast(query.value("1"), Type::text)), trueValue } },
            falseValue
        );
    }

    static ruvia::db_expression safeBigInt(ruvia::db_query& query, ruvia::db_expression value, std::int64_t fallback) {
        const auto text = textDefault(query, value);
        const auto valid = query.binary(
            text,
            Op::regex,
            query.cast(query.value("^-?[0-9]{1,18}$"), Type::text)
        );
        return query.coalesce(
            { query.case_when({ { valid, query.cast(text, Type::big_int) } }),
              query.cast(query.value(fallback), Type::big_int) }
        );
    }

    static ruvia::db_query selectedGroupIds(const ruvia::db_query& source, std::string_view rootId) {
        ruvia::db_query roots(source.resource());
        roots.select(roots.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "root"))
            .from(entities::EdgeNodeGroupEntity::table_name(), "root")
            .where(roots.binary(roots.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "root"), Op::equal, roots.cast(roots.value(rootId), Type::uuid)))
            .and_where(roots.unary(Unary::is_null, roots.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name(), "root")));

        ruvia::db_query descendants(source.resource());
        descendants.select(descendants.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "child"))
            .from(entities::EdgeNodeGroupEntity::table_name(), "child")
            .join(ruvia::db_join_type::inner, "selected_group", descendants.binary(descendants.column(entities::EdgeNodeGroupEntity::column<"parent_id">().name(), "child"), Op::equal, descendants.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "parent")), "parent")
            .and_where(descendants.unary(Unary::is_null, descendants.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name(), "child")));
        roots.combine(ruvia::db_set_operation::union_all, descendants);

        ruvia::db_query result(source.resource());
        result.with("selected_group", roots, { .recursive_ = true })
            .select(result.column(entities::EdgeNodeGroupEntity::column<"id">().name()))
            .from("selected_group");
        return result;
    }

    static void applyNodeFilters(ruvia::db_query& query, const std::optional<std::string>& keyword, const std::optional<std::string>& status, const std::optional<std::string>& groupId) {
        std::vector<ruvia::db_expression> predicates;
        if (keyword && !keyword->empty()) {
            const auto pattern = "%" + *keyword + "%";
            const auto nodeImei = query.binary(query.column(entities::EdgeNodeEntity::column<"imei">().name(), "node"), Op::i_like, query.value(pattern));
            const auto nodeName = query.binary(
                textDefault(query, query.column(entities::EdgeNodeEntity::column<"name">().name(), "node")),
                Op::i_like,
                query.value(pattern)
            );
            const auto groupName = query.binary(
                textDefault(query, query.column(entities::EdgeNodeGroupEntity::column<"name">().name(), "group_item")),
                Op::i_like,
                query.value(pattern)
            );
            const auto model = query.binary(query.column(entities::EdgeNodeEntity::column<"model">().name(), "node"), Op::i_like, query.value(pattern));
            predicates.emplace_back(query.binary(
                query.binary(query.binary(nodeImei, Op::or_value, nodeName), Op::or_value, groupName),
                Op::or_value,
                model
            ));
        }
        if (status && !status->empty()) {
            predicates.emplace_back(query.binary(query.column(entities::EdgeNodeEntity::column<"enrollment_status">().name(), "node"), Op::equal, query.value(*status)));
        }
        if (groupId && !groupId->empty()) {
            if (*groupId == "ungrouped") {
                predicates.emplace_back(
                    query.unary(Unary::is_null, query.column(entities::EdgeNodeEntity::column<"group_id">().name(), "node"))
                );
            } else {
                const auto groups = selectedGroupIds(query, *groupId);
                predicates.emplace_back(query.binary(
                    query.column(entities::EdgeNodeEntity::column<"group_id">().name(), "node"),
                    Op::in,
                    query.subquery(groups)
                ));
            }
        }
        if (predicates.empty()) {
            return;
        }
        auto combined = predicates.front();
        for (std::size_t index = 1; index < predicates.size(); ++index) {
            combined = query.binary(combined, Op::and_value, predicates[index]);
        }
        query.where(combined);
    }

    template <typename Context>
    static ruvia::task<void> validateGroupParent(Context& c, std::string_view parentId, std::string_view groupId) {
        if (parentId.empty()) {
            co_return;
        }
        if (parentId == groupId) {
            service::common::fail(17003, "上级分组不能是自身", 409);
        }
        ruvia::db_query parent(c.pool());
        parent.select(parent.cast(parent.value(1), Type::integer))
            .from(entities::EdgeNodeGroupEntity::table_name())
            .where(parent.binary(parent.column(entities::EdgeNodeGroupEntity::column<"id">().name()), Op::equal, parent.cast(parent.value(parentId), Type::uuid)))
            .and_where(parent.unary(Unary::is_null, parent.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name())))
            .limit(1);
        const auto parentRows = co_await c.db().query(parent);
        if (parentRows.empty()) {
            service::common::fail(17001, "上级分组不存在", 404);
        }
        if (!groupId.empty()) {
            ruvia::db_query roots(c.pool());
            roots.select(roots.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "root"))
                .from(entities::EdgeNodeGroupEntity::table_name(), "root")
                .where(roots.binary(roots.column(entities::EdgeNodeGroupEntity::column<"parent_id">().name(), "root"), Op::equal, roots.cast(roots.value(groupId), Type::uuid)))
                .and_where(roots.unary(Unary::is_null, roots.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name(), "root")));
            ruvia::db_query descendants(c.pool());
            descendants.select(descendants.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "child"))
                .from(entities::EdgeNodeGroupEntity::table_name(), "child")
                .join(ruvia::db_join_type::inner, "descendants", descendants.binary(descendants.column(entities::EdgeNodeGroupEntity::column<"parent_id">().name(), "child"), Op::equal, descendants.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "parent")), "parent")
                .and_where(descendants.unary(Unary::is_null, descendants.column(entities::EdgeNodeGroupEntity::column<"deleted_at">().name(), "child")));
            roots.combine(ruvia::db_set_operation::union_all, descendants);
            ruvia::db_query cycle(c.pool());
            cycle.with("descendants", roots, { .recursive_ = true })
                .select(cycle.cast(cycle.value(1), Type::integer))
                .from("descendants")
                .where(cycle.binary(cycle.column(entities::EdgeNodeGroupEntity::column<"id">().name()), Op::equal, cycle.cast(cycle.value(parentId), Type::uuid)))
                .limit(1);
            const auto cycleRows = co_await c.db().query(cycle);
            if (!cycleRows.empty()) {
                service::common::fail(17003, "不能把分组移动到自己的子分组", 409);
            }
        }
    }

    // 最新固件任务横向子查询的派生列名（非实体列）。
    static constexpr std::string_view kProgressPercentColumn = "progress_percent";
    static constexpr std::string_view kDownloadedBytesColumn = "downloaded_bytes";
    static constexpr std::string_view kTotalBytesColumn = "total_bytes";
    static constexpr std::string_view kMessageColumn = "message";
    // jsonb_array_elements_text 返回的派生列名（非实体列）。
    static constexpr std::string_view kBridgePortColumn = "value";

    static ruvia::db_query nodeSelect(std::pmr::memory_resource* resource) {
        using Route = service::vpn::entities::VpnRouteEntity;
        using Peer = service::vpn::entities::VpnPeerEntity;
        ruvia::db_query query(resource);
        const auto nodeId = query.column(entities::EdgeNodeEntity::column<"id">().name(), "node");
        const auto status = query.column(entities::EdgeNodeEntity::column<"status">().name(), "node");
        const auto capability = query.column(entities::EdgeNodeEntity::column<"capability">().name(), "node");
        const auto mobile = query.column(entities::EdgeNodeEntity::column<"mobile">().name(), "node");
        const auto config = db::jsonValue(query, status, "config");
        const auto outbox = db::jsonValue(query, status, "outbox");
        const auto signal = db::jsonValue(query, mobile, "signal");
        const auto vpnCapability = db::jsonValue(query, capability, "vpn");
        const auto tcpTraffic = db::jsonValue(query, status, "tcpTraffic");
        const auto vpnTraffic = db::jsonValue(query, status, "vpnTraffic");

        ruvia::db_query latest(resource);
        const auto taskResult = latest.column(entities::EdgeTaskEntity::column<"result">().name(), "task");
        latest.select({ latest.alias(latest.column(entities::EdgeTaskEntity::column<"status">().name(), "task"), entities::EdgeTaskEntity::column<"status">().name()), latest.alias(db::jsonText(latest, taskResult, "progressPercent"), kProgressPercentColumn), latest.alias(db::jsonText(latest, taskResult, "downloadedBytes"), kDownloadedBytesColumn), latest.alias(db::jsonText(latest, taskResult, "totalBytes"), kTotalBytesColumn), latest.alias(db::jsonText(latest, taskResult, "message"), kMessageColumn) })
            .from(entities::EdgeTaskEntity::table_name(), "task")
            .where(latest.binary(latest.column(entities::EdgeTaskEntity::column<"node_id">().name(), "task"), Op::equal, latest.import_expression(nodeId, "node", "node")))
            .and_where(latest.binary(latest.column(entities::EdgeTaskEntity::column<"task_type">().name(), "task"), Op::equal, latest.cast(latest.value("firmware"), Type::text)))
            .order_by(latest.column(entities::EdgeTaskEntity::column<"created_at">().name(), "task"), ruvia::db_order_direction::desc)
            .limit(1);

        ruvia::db_query vpn(resource);
        const auto vpnNodeId = vpn.import_expression(nodeId, "node", "node");
        const auto virtualCidr = vpn.column(Route::column<"virtual_cidr">().name(), "route");
        const std::array<ruvia::db_order_term, 1> vpnOrder{ { { virtualCidr, ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value } } };
        vpn.select(vpn.aggregate("string_agg", { virtualCidr, vpn.cast(vpn.value(","), Type::text) }, false, vpnOrder))
            .from(Route::table_name(), "route")
            .join(ruvia::db_join_type::inner, Peer::table_name(), vpn.binary(vpn.column(Peer::column<"id">().name(), "peer"), Op::equal, vpn.column(Route::column<"edge_peer_id">().name(), "route")), "peer")
            .where(vpn.binary(vpn.column(Peer::column<"edge_node_id">().name(), "peer"), Op::equal, vpnNodeId))
            .and_where(vpn.binary(vpn.column(Peer::column<"status">().name(), "peer"), Op::equal, vpn.value("active")))
            .and_where(vpn.unary(Unary::is_true, vpn.column(Route::column<"enabled">().name(), "route")))
            .and_where(vpn.binary(vpn.column(Route::column<"status">().name(), "route"), Op::equal, vpn.value("active")));

        const auto nodeColumn = [&query](std::string_view name) { return query.column(name, "node"); };
        const auto firmwareColumn = [&query](std::string_view name) { return query.column(name, "firmware"); };
        query.select({ query.cast(nodeId, Type::text), nodeColumn(entities::EdgeNodeEntity::column<"imei">().name()), textDefault(query, nodeColumn(entities::EdgeNodeEntity::column<"name">().name())), nodeColumn(entities::EdgeNodeEntity::column<"model">().name()), nodeColumn(entities::EdgeNodeEntity::column<"software_version">().name()), nodeColumn(entities::EdgeNodeEntity::column<"hostname">().name()), nodeColumn(entities::EdgeNodeEntity::column<"architecture">().name()), nodeColumn(entities::EdgeNodeEntity::column<"openwrt_release">().name()), nodeColumn(entities::EdgeNodeEntity::column<"enrollment_status">().name()), query.cast(query.value(false), Type::boolean), textDefault(query, query.call("iot_utc_timestamp", { nodeColumn(entities::EdgeNodeEntity::column<"last_seen_at">().name()) })), query.call("iot_utc_timestamp", { nodeColumn(entities::EdgeNodeEntity::column<"created_at">().name()) }),
                       safeBigInt(query, db::jsonText(query, config, "activeVersion"), 0), safeBigInt(query, db::jsonText(query, config, "desiredVersion"), 0), textDefault(query, db::jsonText(query, config, "state"), "idle"), textDefault(query, db::jsonText(query, config, "message")), safeBigInt(query, db::jsonText(query, outbox, "records"), 0), safeBigInt(query, db::jsonText(query, outbox, "bytes"), 0), textDefault(query, db::jsonText(query, db::jsonValue(query, status, "log"), "level"), "info"),
                       booleanText(query, db::jsonText(query, capability, "networkConfig")), safeBigInt(query, db::jsonText(query, capability, "networkConfigVersion"), 0), booleanText(query, db::jsonText(query, capability, "firmwareUpdate")), booleanText(query, db::jsonText(query, capability, "deviceConfig")), booleanText(query, db::jsonText(query, capability, "modemControl")), booleanText(query, db::jsonText(query, capability, "terminal")), booleanText(query, db::jsonText(query, capability, "logs")),
                       booleanText(query, db::jsonText(query, mobile, "available")), textDefault(query, db::jsonText(query, mobile, "simState"), "unknown"), textDefault(query, db::jsonText(query, mobile, "iccid")), safeBigInt(query, db::jsonText(query, signal, "csq"), 99), safeBigInt(query, db::jsonText(query, signal, "rssiDbm"), -1), safeBigInt(query, db::jsonText(query, signal, "percent"), 0), booleanText(query, db::jsonText(query, mobile, "registered")), safeBigInt(query, db::jsonText(query, mobile, "registrationStatus"), -1), textDefault(query, db::jsonText(query, mobile, "apn")), textDefault(query, db::jsonText(query, mobile, "operator")), booleanText(query, db::jsonText(query, mobile, "connected")), textDefault(query, db::jsonText(query, mobile, "ipv4")),
                       textDefault(query, firmwareColumn(entities::EdgeTaskEntity::column<"status">().name())), safeBigInt(query, firmwareColumn(kProgressPercentColumn), 0), safeBigInt(query, firmwareColumn(kDownloadedBytesColumn), 0), safeBigInt(query, firmwareColumn(kTotalBytesColumn), 0), textDefault(query, firmwareColumn(kMessageColumn)),
                       booleanText(query, db::jsonText(query, vpnCapability, "supportsVpn")), textDefault(query, db::jsonText(query, vpnCapability, "wireguardVersion")), textDefault(query, db::jsonText(query, vpnCapability, "agentVersion")), textDefault(query, db::jsonText(query, vpnCapability, "publicKey")),
                       textDefault(query, query.cast(nodeColumn(entities::EdgeNodeEntity::column<"group_id">().name()), Type::text)), textDefault(query, query.column(entities::EdgeNodeGroupEntity::column<"name">().name(), "group_item")), textDefault(query, query.subquery(vpn)), booleanText(query, db::jsonText(query, capability, "serialDebug")),
                       db::jsonText(query, tcpTraffic, "uploadBytes"), db::jsonText(query, tcpTraffic, "downloadBytes"), db::jsonText(query, tcpTraffic, "intervalMs"), db::jsonText(query, tcpTraffic, "sampleId"), db::jsonText(query, tcpTraffic, "month"), db::jsonText(query, tcpTraffic, "monthlyUploadBytes"), db::jsonText(query, tcpTraffic, "monthlyDownloadBytes"), db::jsonText(query, tcpTraffic, "day"), db::jsonText(query, tcpTraffic, "dailyUploadBytes"), db::jsonText(query, tcpTraffic, "dailyDownloadBytes"),
                       db::jsonText(query, vpnTraffic, "uploadBytes"), db::jsonText(query, vpnTraffic, "downloadBytes"), db::jsonText(query, vpnTraffic, "intervalMs"), db::jsonText(query, vpnTraffic, "sampleId"), db::jsonText(query, vpnTraffic, "month"), db::jsonText(query, vpnTraffic, "monthlyUploadBytes"), db::jsonText(query, vpnTraffic, "monthlyDownloadBytes"), db::jsonText(query, vpnTraffic, "day"), db::jsonText(query, vpnTraffic, "dailyUploadBytes"), db::jsonText(query, vpnTraffic, "dailyDownloadBytes") })
            .from(entities::EdgeNodeEntity::table_name(), "node")
            .join(ruvia::db_join_type::left, entities::EdgeNodeGroupEntity::table_name(), query.binary(nodeColumn(entities::EdgeNodeEntity::column<"group_id">().name()), Op::equal, query.column(entities::EdgeNodeGroupEntity::column<"id">().name(), "group_item")), "group_item")
            .join(ruvia::db_join_type::left, latest, query.cast(query.value(true), Type::boolean), "firmware", { .lateral_ = true });
        return query;
    }

    static std::int64_t integer(std::string_view value) {
        std::int64_t result{};
        const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
        return error == std::errc{} && end == value.data() + value.size() ? result : 0;
    }

    template <typename Context, typename Row>
    static ruvia::task<void> fillNode(Context& c, EdgeNodeDto& node, const Row& row) {
        NodeStatusDto status(ruvia::model_options{ .resource_ = c.arena() });
        ConfigStatusDto config(ruvia::model_options{ .resource_ = c.arena() });
        config.template set<"activeVersion">(integer(row[12].value().value_or(std::string_view{})));
        config.template set<"desiredVersion">(integer(row[13].value().value_or(std::string_view{})));
        config.template set<"state">(row[14].value().value_or(std::string_view{}));
        config.template set<"message">(row[15].value().value_or(std::string_view{}));
        OutboxStatusDto outbox(ruvia::model_options{ .resource_ = c.arena() });
        outbox.template set<"records">(integer(row[16].value().value_or(std::string_view{})));
        outbox.template set<"bytes">(integer(row[17].value().value_or(std::string_view{})));
        LogStatusDto log(ruvia::model_options{ .resource_ = c.arena() });
        log.template set<"level">(row[18].value().value_or(std::string_view{}));
        const auto session = co_await c.redis().get(
            service::message::edge::sessionKey(row[0].value().value_or(std::string_view{}))
        );
        status.template set<"online">(row[8].value().value_or(std::string_view{}) == "approved" && session.has_value());
        status.template set<"lastSeenAt">(row[10].value().value_or(std::string_view{}));
        status.template set<"config">(std::move(config));
        status.template set<"outbox">(std::move(outbox));
        status.template set<"log">(std::move(log));
        if (row[51].value() || row[56].value() || row[57].value() || row[59].value() || row[60].value()) {
            TcpTrafficDto traffic(ruvia::model_options{ .resource_ = c.arena() });
            if (row[51].value()) traffic.template set<"uploadBytes">(*row[51].value());
            if (row[52].value()) traffic.template set<"downloadBytes">(*row[52].value());
            if (row[53].value()) traffic.template set<"intervalMs">(*row[53].value());
            if (row[54].value()) traffic.template set<"sampleId">(*row[54].value());
            if (row[55].value()) traffic.template set<"month">(*row[55].value());
            if (row[56].value()) traffic.template set<"monthlyUploadBytes">(*row[56].value());
            if (row[57].value()) traffic.template set<"monthlyDownloadBytes">(*row[57].value());
            if (row[58].value()) traffic.template set<"day">(*row[58].value());
            if (row[59].value()) traffic.template set<"dailyUploadBytes">(*row[59].value());
            if (row[60].value()) traffic.template set<"dailyDownloadBytes">(*row[60].value());
            status.template set<"tcpTraffic">(std::move(traffic));
        }
        if (row[61].value() || row[66].value() || row[67].value() || row[69].value() || row[70].value()) {
            TcpTrafficDto vpn(ruvia::model_options{ .resource_ = c.arena() });
            if (row[61].value()) vpn.template set<"uploadBytes">(*row[61].value());
            if (row[62].value()) vpn.template set<"downloadBytes">(*row[62].value());
            if (row[63].value()) vpn.template set<"intervalMs">(*row[63].value());
            if (row[64].value()) vpn.template set<"sampleId">(*row[64].value());
            if (row[65].value()) vpn.template set<"month">(*row[65].value());
            if (row[66].value()) vpn.template set<"monthlyUploadBytes">(*row[66].value());
            if (row[67].value()) vpn.template set<"monthlyDownloadBytes">(*row[67].value());
            if (row[68].value()) vpn.template set<"day">(*row[68].value());
            if (row[69].value()) vpn.template set<"dailyUploadBytes">(*row[69].value());
            if (row[70].value()) vpn.template set<"dailyDownloadBytes">(*row[70].value());
            status.template set<"vpnTraffic">(std::move(vpn));
        }

        CapabilityDto capability(ruvia::model_options{ .resource_ = c.arena() });
        capability.template set<"networkConfig">(row[19].value().value_or(std::string_view{}) == "t");
        capability.template set<"networkConfigVersion">(
            integer(row[20].value().value_or(std::string_view{}))
        );
        capability.template set<"firmwareUpdate">(row[21].value().value_or(std::string_view{}) == "t");
        capability.template set<"deviceConfig">(row[22].value().value_or(std::string_view{}) == "t");
        capability.template set<"modemControl">(row[23].value().value_or(std::string_view{}) == "t");
        capability.template set<"terminal">(row[24].value().value_or(std::string_view{}) == "t");
        capability.template set<"logs">(row[25].value().value_or(std::string_view{}) == "t");
        capability.template set<"serialDebug">(row[50].value().value_or(std::string_view{}) == "t");
        VpnCapabilityDto vpn(ruvia::model_options{ .resource_ = c.arena() });
        vpn.template set<"supportsVpn">(row[43].value().value_or(std::string_view{}) == "t");
        vpn.template set<"wireguardVersion">(row[44].value().value_or(std::string_view{}));
        vpn.template set<"agentVersion">(row[45].value().value_or(std::string_view{}));
        vpn.template set<"publicKey">(row[46].value().value_or(std::string_view{}));
        capability.template set<"vpn">(std::move(vpn));

        SignalDto signal(ruvia::model_options{ .resource_ = c.arena() });
        signal.template set<"csq">(integer(row[29].value().value_or(std::string_view{})));
        signal.template set<"rssiDbm">(integer(row[30].value().value_or(std::string_view{})));
        signal.template set<"percent">(integer(row[31].value().value_or(std::string_view{})));
        MobileDto mobile(ruvia::model_options{ .resource_ = c.arena() });
        mobile.template set<"available">(row[26].value().value_or(std::string_view{}) == "t");
        mobile.template set<"simState">(row[27].value().value_or(std::string_view{}));
        mobile.template set<"iccid">(row[28].value().value_or(std::string_view{}));
        mobile.template set<"signal">(std::move(signal));
        mobile.template set<"registered">(row[32].value().value_or(std::string_view{}) == "t");
        mobile.template set<"registrationStatus">(
            integer(row[33].value().value_or(std::string_view{}))
        );
        mobile.template set<"apn">(row[34].value().value_or(std::string_view{}));
        mobile.template set<"operatorName">(row[35].value().value_or(std::string_view{}));
        mobile.template set<"connected">(row[36].value().value_or(std::string_view{}) == "t");
        mobile.template set<"ipv4">(row[37].value().value_or(std::string_view{}));

        FirmwareStatusDto firmware(ruvia::model_options{ .resource_ = c.arena() });
        firmware.template set<"state">(row[38].value().value_or(std::string_view{}));
        firmware.template set<"progressPercent">(
            integer(row[39].value().value_or(std::string_view{}))
        );
        firmware.template set<"downloadedBytes">(
            integer(row[40].value().value_or(std::string_view{}))
        );
        firmware.template set<"totalBytes">(integer(row[41].value().value_or(std::string_view{})));
        firmware.template set<"message">(row[42].value().value_or(std::string_view{}));

        node.template set<"id">(row[0].value().value_or(std::string_view{}));
        node.template set<"imei">(row[1].value().value_or(std::string_view{}));
        node.template set<"name">(row[2].value().value_or(std::string_view{}));
        node.template set<"groupId">(row[47].value().value_or(std::string_view{}));
        node.template set<"groupName">(row[48].value().value_or(std::string_view{}));
        node.template set<"model">(row[3].value().value_or(std::string_view{}));
        node.template set<"softwareVersion">(row[4].value().value_or(std::string_view{}));
        node.template set<"hostname">(row[5].value().value_or(std::string_view{}));
        node.template set<"architecture">(row[6].value().value_or(std::string_view{}));
        node.template set<"openwrtRelease">(row[7].value().value_or(std::string_view{}));
        node.template set<"enrollmentStatus">(row[8].value().value_or(std::string_view{}));
        node.template set<"status">(std::move(status));
        node.template set<"capability">(std::move(capability));
        node.template set<"mobile">(std::move(mobile));
        node.template set<"firmware">(std::move(firmware));
        ruvia::boxed_array<ruvia::string> virtualCidrs(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& cidr : split(row[49].value().value_or(std::string_view{}))) {
            if (!cidr.empty()) {
                virtualCidrs.emplace(cidr, ruvia::model_options{ .resource_ = c.arena() });
            }
        }
        node.template set<"vpnVirtualCidrs">(std::move(virtualCidrs));
        node.template set<"createdAt">(row[11].value().value_or(std::string_view{}));
    }

    static std::vector<std::string> split(std::string_view value) {
        std::vector<std::string> result;
        while (!value.empty()) {
            const auto comma = value.find(',');
            result.emplace_back(value.substr(0, comma));
            if (comma == std::string_view::npos) {
                break;
            }
            value.remove_prefix(comma + 1);
        }
        return result;
    }

    static ruvia::db_query taskSelect(std::pmr::memory_resource* resource, std::string_view nodeId) {
        ruvia::db_query query(resource);
        const auto result = query.column(entities::EdgeTaskEntity::column<"result">().name(), "task");
        query.select({ query.cast(query.column(entities::EdgeTaskEntity::column<"id">().name(), "task"), Type::text), query.column(entities::EdgeTaskEntity::column<"task_type">().name(), "task"), query.column(entities::EdgeTaskEntity::column<"status">().name(), "task"), textDefault(query, db::jsonText(query, result, "message")), safeBigInt(query, db::jsonText(query, result, "progressPercent"), 0), safeBigInt(query, db::jsonText(query, result, "downloadedBytes"), 0), safeBigInt(query, db::jsonText(query, result, "totalBytes"), 0), query.call("iot_utc_timestamp", { query.column(entities::EdgeTaskEntity::column<"created_at">().name(), "task") }), query.call("iot_utc_timestamp", { query.column(entities::EdgeTaskEntity::column<"updated_at">().name(), "task") }) })
            .from(entities::EdgeTaskEntity::table_name(), "task")
            .where(query.binary(query.column(entities::EdgeTaskEntity::column<"node_id">().name(), "task"), Op::equal, query.cast(query.value(nodeId), Type::uuid)))
            .order_by(query.column(entities::EdgeTaskEntity::column<"created_at">().name(), "task"), ruvia::db_order_direction::desc)
            .limit(50);
        return query;
    }

    template <typename Context>
    static ruvia::task<ruvia::boxed_array<InterfaceDto>> interfaces(Context& c, std::string_view id) {
        ruvia::db_query query(c.pool());
        const auto name = query.column(entities::EdgeNodeInterfaceEntity::column<"name">().name(), "iface");
        const auto displayName = query.column(entities::EdgeNodeInterfaceEntity::column<"display_name">().name(), "iface");
        const auto mac = textDefault(query, query.column(entities::EdgeNodeInterfaceEntity::column<"mac">().name(), "iface"));
        const auto up = query.column(entities::EdgeNodeInterfaceEntity::column<"is_up">().name(), "iface");
        const auto bridge = query.column(entities::EdgeNodeInterfaceEntity::column<"is_bridge">().name(), "iface");
        const auto ipv4 = textDefault(query, query.column(entities::EdgeNodeInterfaceEntity::column<"ipv4">().name(), "iface"));
        const auto prefixLength = query.coalesce(
            { query.column(entities::EdgeNodeInterfaceEntity::column<"prefix_length">().name(), "iface"), query.cast(query.value(0), Type::integer) }
        );
        const auto gateway = textDefault(query, query.column(entities::EdgeNodeInterfaceEntity::column<"gateway">().name(), "iface"));
        const auto bridgePortsJson = query.coalesce(
            { query.column(entities::EdgeNodeInterfaceEntity::column<"bridge_ports">().name(), "iface"),
              query.cast(query.value("[]"), Type::jsonb) }
        );
        const auto bridgeValue = query.column(kBridgePortColumn, "bridge_value");
        const std::array<ruvia::db_order_term, 1> bridgeOrder{ { { bridgeValue, ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value } } };
        const auto bridgePorts = query.coalesce(
            { query.aggregate("string_agg", { bridgeValue, query.cast(query.value(","), Type::text) }, false, bridgeOrder),
              query.cast(query.value(""), Type::text) }
        );
        query.select({ name, displayName, mac, up, bridge, ipv4, prefixLength, gateway, bridgePorts })
            .from(entities::EdgeNodeInterfaceEntity::table_name(), "iface")
            .join_function(ruvia::db_join_type::left, query.call("jsonb_array_elements_text", { bridgePortsJson }), query.cast(query.value(true), Type::boolean), "bridge_value", { .lateral_ = true, .columns_ = { { .name_ = std::string(kBridgePortColumn) } } })
            .where(query.binary(query.column(entities::EdgeNodeInterfaceEntity::column<"node_id">().name(), "iface"), Op::equal, query.cast(query.value(id), Type::uuid)))
            .group_by({ name, displayName, mac, up, bridge, ipv4, prefixLength, gateway })
            .order_by(name);
        const auto rows = co_await c.db().query(query);
        ruvia::boxed_array<InterfaceDto> result(
            ruvia::model_options{ .resource_ = c.arena() }
        );
        for (const auto& row : rows) {
            auto& item = result.emplace(ruvia::model_options{ .resource_ = c.arena() });
            ruvia::boxed_array<ruvia::string> ports(
                ruvia::model_options{ .resource_ = c.arena() }
            );
            for (const auto& port : split(row[8].value().value_or(std::string_view{}))) {
                if (!port.empty()) {
                    ports.emplace(port, ruvia::model_options{ .resource_ = c.arena() });
                }
            }
            item.template set<"name">(row[0].value().value_or(std::string_view{}))
                .template set<"displayName">(row[1].value().value_or(std::string_view{}))
                .template set<"mac">(row[2].value().value_or(std::string_view{}))
                .template set<"up">(row[3].value().value_or(std::string_view{}) == "t")
                .template set<"bridge">(row[4].value().value_or(std::string_view{}) == "t")
                .template set<"ipv4">(row[5].value().value_or(std::string_view{}))
                .template set<"prefixLength">(integer(row[6].value().value_or(std::string_view{})))
                .template set<"gateway">(row[7].value().value_or(std::string_view{}))
                .template set<"bridgePorts">(std::move(ports));
        }
        co_return result;
    }

    template <typename Context>
    static ruvia::task<ruvia::boxed_array<NetworkDto>> networks(Context& c, std::string_view id) {
        ruvia::db_query query(c.pool());
        const auto name = query.column(entities::EdgeNodeNetworkEntity::column<"name">().name(), "network");
        const auto mode = query.column(entities::EdgeNodeNetworkEntity::column<"address_mode">().name(), "network");
        const auto device = query.column(entities::EdgeNodeNetworkEntity::column<"device">().name(), "network");
        const auto up = query.column(entities::EdgeNodeNetworkEntity::column<"is_up">().name(), "network");
        const auto bridge = query.column(entities::EdgeNodeNetworkEntity::column<"is_bridge">().name(), "network");
        const auto ipv4 = textDefault(query, query.column(entities::EdgeNodeNetworkEntity::column<"ipv4">().name(), "network"));
        const auto prefixLength = query.coalesce(
            { query.column(entities::EdgeNodeNetworkEntity::column<"prefix_length">().name(), "network"), query.cast(query.value(0), Type::integer) }
        );
        const auto gateway = textDefault(query, query.column(entities::EdgeNodeNetworkEntity::column<"gateway">().name(), "network"));
        const auto bridgePortsJson = query.coalesce(
            { query.column(entities::EdgeNodeNetworkEntity::column<"bridge_ports">().name(), "network"),
              query.cast(query.value("[]"), Type::jsonb) }
        );
        const auto bridgeValue = query.column(kBridgePortColumn, "bridge_value");
        const std::array<ruvia::db_order_term, 1> bridgeOrder{ { { bridgeValue, ruvia::db_order_direction::asc, ruvia::db_nulls_order::default_value } } };
        const auto bridgePorts = query.coalesce(
            { query.aggregate("string_agg", { bridgeValue, query.cast(query.value(","), Type::text) }, false, bridgeOrder),
              query.cast(query.value(""), Type::text) }
        );
        query.select({ name, mode, device, up, bridge, ipv4, prefixLength, gateway, bridgePorts })
            .from(entities::EdgeNodeNetworkEntity::table_name(), "network")
            .join_function(ruvia::db_join_type::left, query.call("jsonb_array_elements_text", { bridgePortsJson }), query.cast(query.value(true), Type::boolean), "bridge_value", { .lateral_ = true, .columns_ = { { .name_ = std::string(kBridgePortColumn) } } })
            .where(query.binary(query.column(entities::EdgeNodeNetworkEntity::column<"node_id">().name(), "network"), Op::equal, query.cast(query.value(id), Type::uuid)))
            .group_by({ name, mode, device, up, bridge, ipv4, prefixLength, gateway })
            .order_by(name);
        const auto rows = co_await c.db().query(query);
        ruvia::boxed_array<NetworkDto> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = result.emplace(ruvia::model_options{ .resource_ = c.arena() });
            ruvia::boxed_array<ruvia::string> ports(
                ruvia::model_options{ .resource_ = c.arena() }
            );
            for (const auto& port : split(row[8].value().value_or(std::string_view{}))) {
                if (!port.empty()) {
                    ports.emplace(port, ruvia::model_options{ .resource_ = c.arena() });
                }
            }
            item.template set<"name">(row[0].value().value_or(std::string_view{}))
                .template set<"mode">(row[1].value().value_or(std::string_view{}))
                .template set<"device">(row[2].value().value_or(std::string_view{}))
                .template set<"up">(row[3].value().value_or(std::string_view{}) == "t")
                .template set<"bridge">(row[4].value().value_or(std::string_view{}) == "t")
                .template set<"ipv4">(row[5].value().value_or(std::string_view{}))
                .template set<"prefixLength">(integer(row[6].value().value_or(std::string_view{})))
                .template set<"gateway">(row[7].value().value_or(std::string_view{}))
                .template set<"bridgePorts">(std::move(ports));
        }
        co_return result;
    }

    template <typename Context>
    static ruvia::task<ruvia::boxed_array<SerialDto>> serialPorts(Context& c, std::string_view id) {
        ruvia::db_query query(c.pool());
        query.select({ query.column(entities::EdgeNodeSerialEntity::column<"path">().name()), query.column(entities::EdgeNodeSerialEntity::column<"display_name">().name()), query.column(entities::EdgeNodeSerialEntity::column<"available">().name()), query.column(entities::EdgeNodeSerialEntity::column<"rs485">().name()) })
            .from(entities::EdgeNodeSerialEntity::table_name())
            .where(query.binary(query.column(entities::EdgeNodeSerialEntity::column<"node_id">().name()), Op::equal, query.cast(query.value(id), Type::uuid)))
            .order_by(query.column(entities::EdgeNodeSerialEntity::column<"path">().name()));
        const auto rows = co_await c.db().query(query);
        ruvia::boxed_array<SerialDto> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = result.emplace(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"path">(row[0].value().value_or(std::string_view{}))
                .template set<"displayName">(row[1].value().value_or(std::string_view{}))
                .template set<"available">(row[2].value().value_or(std::string_view{}) == "t")
                .template set<"rs485">(row[3].value().value_or(std::string_view{}) == "t");
        }
        co_return result;
    }

    template <typename Context>
    static ruvia::task<ruvia::boxed_array<TaskDto>> tasks(Context& c, std::string_view id) {
        const auto rows = co_await c.db().query(taskSelect(c.pool(), id));
        ruvia::boxed_array<TaskDto> result(ruvia::model_options{ .resource_ = c.arena() });
        for (const auto& row : rows) {
            auto& item = result.emplace(ruvia::model_options{ .resource_ = c.arena() });
            item.template set<"id">(row[0].value().value_or(std::string_view{}))
                .template set<"taskType">(row[1].value().value_or(std::string_view{}))
                .template set<"status">(row[2].value().value_or(std::string_view{}))
                .template set<"message">(row[3].value().value_or(std::string_view{}))
                .template set<"progressPercent">(integer(row[4].value().value_or(std::string_view{})))
                .template set<"downloadedBytes">(integer(row[5].value().value_or(std::string_view{})))
                .template set<"totalBytes">(integer(row[6].value().value_or(std::string_view{})))
                .template set<"createdAt">(row[7].value().value_or(std::string_view{}))
                .template set<"updatedAt">(row[8].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    static bool ipv4(std::string_view input, std::uint32_t& output) {
        output = 0;
        for (int part = 0; part < 4; ++part) {
            const auto dot = input.find('.');
            const auto token = input.substr(0, dot);
            unsigned value{};
            const auto [end, error] =
                std::from_chars(token.data(), token.data() + token.size(), value);
            if (token.empty() || error != std::errc{} || end != token.data() + token.size() ||
                value > 255 || (token.size() > 1 && token.front() == '0')) {
                return false;
            }
            output = (output << 8U) | value;
            if (part == 3) {
                return dot == std::string_view::npos;
            }
            if (dot == std::string_view::npos) {
                return false;
            }
            input.remove_prefix(dot + 1);
        }
        return false;
    }

    static void validateStaticNetwork(std::string_view ipText, std::uint32_t prefix, std::string_view gatewayText) {
        std::uint32_t address{};
        if (!ipv4(ipText, address) || prefix == 0 || prefix > 30) {
            service::common::fail(17003, "静态 IPv4 地址或前缀长度无效", 400);
        }
        const auto mask = 0xffffffffU << (32U - prefix);
        const auto host = address & ~mask;
        if (host == 0 || host == ~mask) {
            service::common::fail(17003, "静态 IPv4 不能是网络地址或广播地址", 400);
        }
        if (!gatewayText.empty()) {
            std::uint32_t gateway{};
            if (!ipv4(gatewayText, gateway) || (gateway & mask) != (address & mask) ||
                gateway == address || (gateway & ~mask) == 0 || (gateway & ~mask) == ~mask) {
                service::common::fail(17003, "网关必须是同网段内不同的合法主机地址", 400);
            }
        }
    }

    template <typename Context>
    static ruvia::task<std::unordered_set<std::string>>
    manageableInterfaces(Context& c, std::string_view nodeId) {
        ruvia::db_query query(c.pool());
        query.select(query.column(entities::EdgeNodeInterfaceEntity::column<"name">().name()))
            .from(entities::EdgeNodeInterfaceEntity::table_name())
            .where(query.binary(query.column(entities::EdgeNodeInterfaceEntity::column<"node_id">().name()), Op::equal, query.cast(query.value(nodeId), Type::uuid)))
            .and_where(query.binary(query.column(entities::EdgeNodeInterfaceEntity::column<"name">().name()), Op::not_equal, query.cast(query.value("lo"), Type::text)))
            .and_where(query.unary(Unary::is_false, query.column(entities::EdgeNodeInterfaceEntity::column<"is_bridge">().name())));
        const auto rows = co_await c.db().query(query);
        std::unordered_set<std::string> result;
        for (const auto& row : rows) {
            result.emplace(row[0].value().value_or(std::string_view{}));
        }
        co_return result;
    }

    template <typename Ports>
    static void validateNetworkConfig(std::string_view name, std::string_view mode, std::string_view device, bool bridge, const Ports& ports, std::string_view ip, std::uint32_t prefix, std::string_view gateway, const std::unordered_set<std::string>& available, std::unordered_set<std::string>& selected) {
        if (name.size() > 15 || (bridge && name.size() > 12)) {
            service::common::fail(
                17003,
                bridge ? "网桥逻辑名称不能超过 12 个字符"
                       : "逻辑接口名称不能超过 15 个字符",
                400
            );
        }
        if (mode != "dhcp" && mode != "static") {
            service::common::fail(17003, "地址模式只支持 DHCP 或静态 IPv4", 400);
        }

        const auto useDevice = [&](std::string_view value) {
            const std::string text(value);
            if (!available.contains(text)) {
                service::common::fail(
                    17003,
                    "网卡 " + text + " 不可管理、未上报或属于受保护的 4G 上联",
                    400
                );
            }
            if (!selected.emplace(text).second) {
                service::common::fail(17003, "网卡 " + text + " 在同一请求中被重复占用", 400);
            }
        };

        if (bridge) {
            if (!device.empty()) {
                service::common::fail(17003, "网桥不能同时指定单一设备", 400);
            }
            if (!ports || ports->empty()) {
                service::common::fail(17003, "网桥至少需要一个成员网卡", 400);
            }
            for (const auto& port : *ports) {
                useDevice(port.view());
            }
        } else {
            if (device.empty()) {
                service::common::fail(17003, "非网桥接口必须选择一个网卡", 400);
            }
            if (ports && !ports->empty()) {
                service::common::fail(17003, "非网桥接口不能配置网桥成员", 400);
            }
            useDevice(device);
        }

        if (mode == "static") {
            validateStaticNetwork(ip, prefix, gateway);
        } else if (!ip.empty() || prefix != 0 || !gateway.empty()) {
            service::common::fail(17003, "DHCP 接口不能携带静态 IPv4 配置", 400);
        }
    }

    template <typename Context>
    static ruvia::task<std::int64_t> requireNetworkManagement(
        Context& c,
        std::string_view nodeId
    ) {
        ruvia::db_query query(c.pool());
        const auto capability = query.column(entities::EdgeNodeEntity::column<"capability">().name());
        query.select({ query.column(entities::EdgeNodeEntity::column<"enrollment_status">().name()), booleanText(query, db::jsonText(query, capability, "networkConfig")), safeBigInt(query, db::jsonText(query, capability, "networkConfigVersion"), 0) })
            .from(entities::EdgeNodeEntity::table_name())
            .where(query.binary(query.column(entities::EdgeNodeEntity::column<"id">().name()), Op::equal, query.cast(query.value(nodeId), Type::uuid)))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17001, "边缘节点不存在", 404);
        }
        if (rows.front()[0].value().value_or(std::string_view{}) != "approved") {
            service::common::fail(17002, "边缘节点尚未批准注册", 409);
        }
        if (rows.front()[1].value().value_or(std::string_view{}) != "t") {
            service::common::fail(17004, "网络配置不可用", 409);
        }
        const auto version = rows.front()[2].template as<std::int64_t>().value_or(0);
        if (version < 2) {
            service::common::fail(17004, "节点代理版本过旧，请先升级后再管理网络", 409);
        }
        co_return version;
    }

    static bool hex(std::string_view value, std::uint8_t* output, std::size_t size) {
        if (value.size() != size * 2) {
            return false;
        }
        for (std::size_t index = 0; index < size; ++index) {
            const int high = service::common::hexDigit(value[index * 2]);
            const int low = service::common::hexDigit(value[index * 2 + 1]);
            if (high < 0 || low < 0) {
                return false;
            }
            output[index] = static_cast<std::uint8_t>((high << 4U) | low);
        }
        return true;
    }

    static std::string randomToken() {
        std::array<unsigned char, 32> bytes{};
        if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) {
            throw std::runtime_error("cannot generate firmware token");
        }
        constexpr char digits[] = "0123456789abcdef";
        std::string output;
        output.reserve(64);
        for (const auto byte : bytes) {
            output.push_back(digits[byte >> 4U]);
            output.push_back(digits[byte & 0x0fU]);
        }
        return output;
    }

    template <typename Context>
    static ruvia::task<void> requireNodeCapability(Context& c, std::string_view nodeId, std::string_view key, std::string_view feature) {
        ruvia::db_query query(c.pool());
        query.select({ query.column(entities::EdgeNodeEntity::column<"enrollment_status">().name()), booleanText(query, db::jsonText(query, query.column(entities::EdgeNodeEntity::column<"capability">().name()), key)) })
            .from(entities::EdgeNodeEntity::table_name())
            .where(query.binary(query.column(entities::EdgeNodeEntity::column<"id">().name()), Op::equal, query.cast(query.value(nodeId), Type::uuid)))
            .limit(1);
        const auto rows = co_await c.db().query(query);
        if (rows.empty()) {
            service::common::fail(17001, "边缘节点不存在", 404);
        }
        if (rows.front()[0].value().value_or(std::string_view{}) != "approved") {
            service::common::fail(17002, "边缘节点尚未批准注册", 409);
        }
        if (!rows.front()[1].template as<bool>().value_or(false)) {
            service::common::fail(17004, std::string(feature) + "不可用", 409);
        }
    }

    template <typename Context, typename Request>
    static ruvia::task<void>
    createNetworkTaskAndQueue(Context& c, std::string_view nodeId, std::string_view taskId, std::size_t interfaceCount, const Request& request) {
        ruvia::db_query query(c.pool());
        const auto requestJson = query.call(
            "jsonb_build_object",
            { query.cast(query.value("interfaceCount"), Type::text),
              query.cast(query.value(static_cast<std::int64_t>(interfaceCount)), Type::big_int) }
        );
        query.insert_into(entities::EdgeTaskEntity::table_name(), { entities::EdgeTaskEntity::column<"id">().name(), entities::EdgeTaskEntity::column<"node_id">().name(), entities::EdgeTaskEntity::column<"task_type">().name(), entities::EdgeTaskEntity::column<"request">().name(), entities::EdgeTaskEntity::column<"created_by">().name() })
            .values({ query.cast(query.value(taskId), Type::uuid), query.cast(query.value(nodeId), Type::uuid), query.value("network"), requestJson, query.cast(query.value(c.userId), Type::uuid) });
        (void)co_await c.db().execute(query);
        co_await queueControl(c, nodeId, "queue-network", request);
    }
};

inline EdgeService& edgeService() {
    thread_local EdgeService instance;
    return instance;
}

namespace debug {

inline ruvia::task<void> TerminalSession::close(EventContext& c) {
    (void)co_await edgeService().terminalOperation(c, node, id, connection, "terminal-close");
}

inline ruvia::task<void> SerialSession::close(EventContext& c) {
    (void)co_await edgeService().serialOperation(c, node, id, connection, "serial-close");
}

} // namespace debug

// One edge debug WebSocket on its accepting Service Worker: request
// multiplexing, authorization re-checks, resource cleanup and directed waits
// for the subscription topics its handlers return.
class DebugConnection final {
  public:
    DebugConnection(ruvia::context& context, const debug::EventRegistry<debug::EventContext>& registry)
        : context_(context), socket_(context.get_websocket()), tasks_(context.worker(), { .resource_ = context.pool() }),
          registry_(registry),
          identity_{ service::utils::nextUuid(context.worker_state<service::common::UuidV7State>()) } {}

    ruvia::task<void> run() {
        // Without authorization changes a revoked user would keep its
        // resources, so the connection fails if the feed cannot start.
        auto authorization = co_await context_.worker_state<service::utils::ChangeFeed>().watch(context_, { std::string(service::message::live::topic::kAuth) });
        tasks_.spawn(watchResourceAuthorization(std::move(authorization)));
        bool malformed = false;
        try {
            while (auto frame = co_await socket_.read()) {
                if (!frame->text()) {
                    malformed = true;
                    break;
                }
                co_await accept(debug::Request::parse(frame->payload()));
            }
        } catch (const std::exception& error) {
            LOG_DEBUG << "edge debug connection closed after invalid frame: " << error.what();
            malformed = true;
        }
        closing_ = true;
        for (auto& [id, operation] : operations_) {
            operation->stop.request_stop();
        }
        tasks_.request_stop();
        try {
            co_await socket_.close({ .code_ = static_cast<std::uint16_t>(malformed ? 1002 : 1000), .reason_ = "debug session ended" });
        } catch (const std::exception& error) {
            LOG_DEBUG << "edge debug connection close handshake failed: " << error.what();
        }
        try {
            co_await tasks_.join();
        } catch (const std::exception& error) {
            LOG_WARN << "edge debug connection task failed: " << error.what();
        }
        operations_.clear();
        co_await closeResources({});
        identity_.accessToken.clear();
    }

  private:
    ruvia::task<void> reply(std::string_view id, std::string_view data, bool error = false) {
        if (closing_) {
            co_return;
        }
        const auto wire = "{\"id\":" + service::utils::jsonQuoted(id) +
            (error ? ",\"error\":" : ",\"data\":") + std::string(data) + "}";
        co_await socket_.text(wire);
    }

    ruvia::task<void> reject(std::string_view id, std::int64_t code, std::string_view message) {
        const auto data = "{\"code\":" + std::to_string(code) + ",\"message\":" + service::utils::jsonQuoted(message) + "}";
        co_await reply(id, data, true);
    }

    ruvia::task<void> accept(debug::Request request) {
        if (seen_.size() >= 100000) {
            co_await reject(request.id, 10002, "当前会话请求数量已达上限，请重新连接");
            co_return;
        }
        if (!seen_.insert(request.id).second) {
            co_await reject(request.id, 10002, "每次请求必须使用新的 UUID");
            co_return;
        }
        if (request.event == "subscription.cancel") {
            const auto data = ruvia::json_value::parse(request.data);
            const auto target = data ? data->get<ruvia::string>("subscriptionId") : std::nullopt;
            if (!target || !service::common::isUuid(target->view())) {
                co_await reject(request.id, 10001, "订阅编号无效");
                co_return;
            }
            const auto found = operations_.find(std::string(target->view()));
            if (found != operations_.end()) {
                const auto operation = found->second;
                if (!operation->request.event.ends_with(".subscribe")) {
                    co_await reject(request.id, 10001, "只能取消订阅");
                    co_return;
                }
                operation->stop.request_stop();
                // Acknowledge only after the subscription has released its
                // notification receiver and finished any in-flight response.
                (void)co_await operation->completion.second.receive(tasks_.get_stop_token());
            }
            co_await reply(request.id, "null");
            co_return;
        }
        if (operations_.size() >= 128) {
            co_await reject(request.id, 10002, "并发请求过多");
            co_return;
        }
        auto operation = std::make_shared<debug::Operation>(context_.worker());
        operation->request = std::move(request);
        operations_.emplace(operation->request.id, operation);
        tasks_.spawn(execute(std::move(operation)));
    }

    ruvia::task<void> execute(std::shared_ptr<debug::Operation> operation) {
        const auto operationStop = ruvia::combine_stop_tokens(tasks_.get_stop_token(), operation->stop.token());
        auto stop = operationStop;
        const auto& request = operation->request;
        const bool subscription = request.event.ends_with(".subscribe");
        std::shared_ptr<service::utils::ChangeFeed::Subscription> notification;
        std::int64_t code = 0;
        std::string message;
        std::string previous;
        bool first = true;
        try {
            if (subscription) {
                // Exists before the first invocation so that no directed frame
                // is lost; narrowed to the handler's topic once it is known.
                notification = co_await context_.worker_state<service::utils::ChangeFeed>().await(context_, {});
            }
            do {
                const auto revision = identity_.revision;
                debug::EventContext event(context_, identity_, request, stop, resources_, operation->state);
                const auto policy = registry_.policy(request.event);
                if (policy.authenticated) {
                    if (identity_.accessToken.empty()) {
                        throw debug::EventError(11004, "未登录");
                    }
                    const auto principal = service::auth::AuthTokenService::verifyAccessToken(event, identity_.accessToken);
                    event.userId = principal.userId;
                    if (!policy.permission.empty()) {
                        co_await service::auth::AuthService::requirePermission(event, event.userId, policy.permission);
                    }
                    if (stop.stop_requested() || identity_.revision != revision) {
                        break;
                    }
                }
                const auto result = co_await registry_.invoke(event);
                stop = ruvia::combine_stop_tokens(operationStop, result.lifetime);
                if (stop.stop_requested()) {
                    break;
                }
                if (identity_.revision != revision) {
                    if (!result.identityChanged) {
                        break;
                    }
                    // A successful identity change invalidates all earlier work.
                    for (auto& [id, pending] : operations_) {
                        if (id != request.id) {
                            pending->stop.request_stop();
                        }
                    }
                    const auto owner = identity_.accessToken.empty() ? std::string{} : service::auth::AuthTokenService::verifyAccessToken(event, identity_.accessToken).userId;
                    co_await closeResources(owner);
                }
                if (first || result.incremental || previous != result.data) {
                    co_await reply(request.id, result.data);
                }
                first = false;
                previous = result.data;
                if (!subscription || result.topic.empty()) {
                    break;
                }
                notification->retarget(result.topic);
                if (result.more) {
                    continue;
                }
                co_await awaitTopic(*notification, result.topic, event, stop);
            } while (!stop.stop_requested());
        } catch (const debug::EventError& error) {
            code = error.code;
            message = error.what();
        } catch (const ruvia::validation_error&) {
            code = 10001;
            message = "事件参数校验失败";
        } catch (const ruvia::http_error& error) {
            const auto info = error.info();
            code = service::common::errorCode(info.code(), info.status().value());
            message = info.message();
        } catch (const service::auth::JwtExpiredError&) {
            code = 11005;
            message = "登录已过期";
        } catch (const service::auth::JwtInvalidError&) {
            code = 11006;
            message = "登录令牌无效";
        } catch (const std::exception& error) {
            LOG_WARN << "edge debug event " << request.event << " failed: " << error.what();
            code = 10004;
            message = "操作失败";
        }
        if (code && !stop.stop_requested() && !closing_) {
            try {
                co_await reject(request.id, code, message);
            } catch (const std::exception& error) {
                LOG_DEBUG << "edge debug rejection was not delivered: " << error.what();
            }
        }
        notification.reset();
        operations_.erase(request.id);
        operation->completion.first.close();
    }

    // Returns when a directed message for `topic` arrived (or the feed lost
    // its position) or the operation stopped. Messages delivered before the
    // subscription narrowed to `topic` are discarded unless they match it.
    ruvia::task<void> awaitTopic(service::utils::ChangeFeed::Subscription& notification, std::string_view topic, debug::EventContext& event, ruvia::stop_token stop) {
        using namespace std::chrono_literals;
        if (const auto pending = notification.take(); pending.recovered || pending.find(topic)) {
            co_return;
        }
        while (!stop.stop_requested()) {
            const auto woke = co_await notification.wait(15s, stop);
            if (stop.stop_requested() || woke == service::utils::ChangeWait::Stopped) {
                co_return;
            }
            if (identity_.accessToken.empty()) {
                throw debug::EventError(11004, "未登录");
            }
            (void)service::auth::AuthTokenService::verifyAccessToken(event, identity_.accessToken);
            if (woke == service::utils::ChangeWait::Changed) {
                if (const auto changes = notification.take(); changes.recovered || changes.find(topic)) {
                    co_return;
                }
            }
        }
    }

    ruvia::task<void> closeResources(std::string_view retainedOwner) {
        std::vector<std::pair<std::string, std::shared_ptr<debug::ConnectionResource>>> closing;
        for (const auto& [id, resource] : resources_) {
            if (resource->owner != retainedOwner || resource->lifetime.stop_requested()) {
                resource->lifetime.request_stop();
                closing.emplace_back(id, resource);
            }
        }
        for (const auto& [id, resource] : closing) {
            const debug::Request request;
            std::any state;
            debug::EventContext event(context_, identity_, request, {}, resources_, state);
            event.userId = resource->owner;
            try {
                co_await resource->close(event);
                resources_.erase(id);
            } catch (const std::exception& error) {
                // Retain the stopped identity for a later cleanup attempt.
                // Collector leases also bound resources after process failure.
                LOG_WARN << "edge debug resource " << id << " close failed: " << error.what();
            }
        }
    }

    ruvia::task<void> watchResourceAuthorization(std::shared_ptr<service::utils::ChangeFeed::Subscription> notification) {
        using namespace std::chrono_literals;
        const auto stop = tasks_.get_stop_token();
        while (!stop.stop_requested()) {
            const auto woke = co_await notification->wait(15s, stop);
            if (stop.stop_requested() || woke == service::utils::ChangeWait::Stopped) {
                break;
            }
            const bool changed = woke == service::utils::ChangeWait::Changed;
            if (changed) {
                (void)notification->take();
            }
            if (resources_.empty()) {
                continue;
            }
            const auto revision = identity_.revision;
            const debug::Request request;
            std::any state;
            debug::EventContext event(context_, identity_, request, stop, resources_, state);
            bool denied = false;
            try {
                event.userId = service::auth::AuthTokenService::verifyAccessToken(event, identity_.accessToken).userId;
                // Timeouts only verify token expiry. Permission data is queried
                // after an authorization-change notification, never on a timer.
                if (changed) {
                    std::unordered_set<std::string> permissions;
                    for (const auto& [id, resource] : resources_) {
                        permissions.insert(resource->permission);
                    }
                    for (const auto& permission : permissions) {
                        co_await service::auth::AuthService::requirePermission(event, event.userId, permission);
                        if (identity_.revision != revision || stop.stop_requested()) {
                            break;
                        }
                    }
                }
            } catch (...) {
                // Fail closed: an expired token, revoked permission or failed
                // check releases every resource of the connection.
                denied = true;
            }
            if (denied && identity_.revision == revision) {
                co_await closeResources({});
            } else if (identity_.revision == revision) {
                co_await closeResources(event.userId);
            }
        }
    }

    ruvia::context& context_;
    ruvia::websocket& socket_;
    ruvia::task_scope tasks_;
    const debug::EventRegistry<debug::EventContext>& registry_;
    debug::ConnectionResources resources_;
    debug::SessionIdentity identity_;
    std::unordered_map<std::string, std::shared_ptr<debug::Operation>> operations_;
    std::unordered_set<std::string> seen_;
    bool closing_{};
};

} // namespace service::edge_node
