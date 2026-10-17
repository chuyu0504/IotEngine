#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <ruvia/core/event_loop_pool.h>

#include "service/common/message.h"
#include "service/common/uuid.h"
#include "service/features/edge/gateway/gateway.service.h"
#include "service/utils/uuid.h"

namespace {

using service::edge::gateway::GatewayService;

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

template <typename Result>
Result runTask(ruvia::task<Result> task) {
    ruvia::event_loop_pool pool({ .loop_count_ = 1 });
    pool.start();
    auto completion = pool.loop(0).start(std::move(task));
    if constexpr (std::is_void_v<Result>) {
        completion.get();
        pool.stop();
        pool.join();
    } else {
        auto result = completion.get();
        pool.stop();
        pool.join();
        return result;
    }
}

std::string compileSql(const ruvia::db_query& query) {
    const auto statement = query.compile(ruvia::db_driver::postgresql, std::pmr::new_delete_resource(), ruvia::db_parameter_mode::literal);
    return std::string(statement.sql());
}

std::string lowered(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char character) {
        return static_cast<char>(std::tolower(character));
    });
    return text;
}

struct FakeCell {
    std::string content;

    [[nodiscard]] std::optional<std::string_view> value() const noexcept { return content; }
};

struct FakeRow {
    std::vector<FakeCell> cells;

    [[nodiscard]] const FakeCell& operator[](std::size_t index) const { return cells.at(index); }
};

struct FakeResult {
    std::vector<FakeRow> rows;

    [[nodiscard]] auto begin() const noexcept { return rows.begin(); }

    [[nodiscard]] auto end() const noexcept { return rows.end(); }

    [[nodiscard]] bool empty() const noexcept { return rows.empty(); }

    [[nodiscard]] const FakeRow& front() const { return rows.at(0); }
};

FakeRow row(std::initializer_list<std::string> values) {
    FakeRow result;
    for (const auto& value : values) {
        result.cells.push_back({ value });
    }
    return result;
}

struct FakeState {
    std::string claimSql = compileSql(GatewayService::firmwareCleanupClaimQuery());
    std::string expiredSql = compileSql(GatewayService::expiredFirmwareQuery());
    std::string referencesSql = compileSql(GatewayService::firmwareReferencesQuery());
    std::string scheduleSql = compileSql(GatewayService::firmwareCleanupScheduleQuery());
    bool claimGranted = true;
    FakeResult expired;
    FakeResult references;
    FakeResult schedule;
    std::vector<std::string> removals;
    std::size_t transactions{};
    std::size_t commits{};
    std::vector<std::filesystem::path> watchedAtCommit;
    std::vector<bool> watchedExistedAtCommit;

    FakeResult answer(const ruvia::db_query& query) const {
        const auto sql = compileSql(query);
        if (sql == claimSql) {
            return FakeResult{ { row({ claimGranted ? "t" : "f" }) } };
        }
        if (sql == expiredSql) {
            return expired;
        }
        if (sql == referencesSql) {
            return references;
        }
        if (sql == scheduleSql) {
            return schedule;
        }
        throw std::runtime_error("unexpected firmware query: " + sql);
    }
};

struct FakeTransaction {
    std::shared_ptr<FakeState> state;

    ruvia::task<FakeResult> query(const ruvia::db_query& query) { co_return state->answer(query); }

    ruvia::task<std::uint64_t> execute(const ruvia::db_query& query) {
        const auto sql = compileSql(query);
        require(lowered(sql).starts_with("delete from"), "cleanup executed a statement other than a record removal");
        state->removals.push_back(sql);
        co_return 1;
    }

    ruvia::task<void> commit() {
        ++state->commits;
        for (const auto& path : state->watchedAtCommit) {
            state->watchedExistedAtCommit.push_back(std::filesystem::exists(path));
        }
        co_return;
    }
};

struct FakeDb {
    std::shared_ptr<FakeState> state;

    ruvia::task<FakeTransaction> begin_transaction() {
        ++state->transactions;
        co_return FakeTransaction{ state };
    }

    ruvia::task<FakeResult> query(const ruvia::db_query& query) { co_return state->answer(query); }
};

struct FakeContext {
    std::shared_ptr<FakeState> state = std::make_shared<FakeState>();

    [[nodiscard]] FakeDb db() const { return FakeDb{ state }; }
};

class FirmwareDirectory final {
  public:
    FirmwareDirectory()
        : path_(std::filesystem::absolute(std::filesystem::temp_directory_path() / ("iot-engine-firmware-storage-" + service::utils::nextUuid(uuidState_))).lexically_normal()) {
        std::filesystem::create_directories(path_);
    }

    ~FirmwareDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    FirmwareDirectory(const FirmwareDirectory&) = delete;
    FirmwareDirectory& operator=(const FirmwareDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

    std::filesystem::path managed(std::string& id) {
        id = service::utils::nextUuid(uuidState_);
        return path_ / (id + ".bin");
    }

    static void write(const std::filesystem::path& file, std::chrono::seconds age) {
        {
            std::ofstream output(file, std::ios::binary);
            output << "fixture";
        }
        std::filesystem::last_write_time(file, std::filesystem::file_time_type::clock::now() - age);
    }

  private:
    service::common::UuidV7State uuidState_;
    std::filesystem::path path_;
};

constexpr auto kExpired = std::chrono::seconds(2 * 3600);
constexpr auto kRecent = std::chrono::seconds(30 * 60);

void testFirmwareChunks() {
    FirmwareDirectory directory;
    const auto path = directory.path() / "image.bin";
    const std::string image(service::edge::gateway::kFirmwareChunkSize + 17, 'x');
    {
        std::ofstream output(path, std::ios::binary);
        output.write(image.data(), static_cast<std::streamsize>(image.size()));
    }
    const auto first = GatewayService::readFirmwareChunk(path, image.size(), 0);
    require(first.error.empty() && !first.eof && first.data.size() == service::edge::gateway::kFirmwareChunkSize, "first firmware WS chunk is invalid");
    const auto last = GatewayService::readFirmwareChunk(path, image.size(), service::edge::gateway::kFirmwareChunkSize);
    require(last.error.empty() && last.eof && last.data == std::string(17, 'x'), "last firmware WS chunk is invalid");
    require(!GatewayService::readFirmwareChunk(path, image.size(), image.size()).error.empty(), "out-of-range firmware WS chunk was accepted");
    require(!GatewayService::readFirmwareChunk(path, image.size() + 1, 0).error.empty(), "firmware storage size drift was accepted");
}

void testOwnedFileNames() {
    FirmwareDirectory directory;
    std::string id;
    const auto owned = directory.managed(id);
    require(GatewayService::managedFirmwareFile(owned, directory.path()), "owned firmware file was not recognized");
    require(!GatewayService::managedFirmwareFile(directory.path() / (id + ".upload"), directory.path()), "in-progress upload is treated as owned firmware");
    require(!GatewayService::managedFirmwareFile(directory.path() / "retain.bin", directory.path()), "non-UUID file is treated as owned firmware");
    require(!GatewayService::managedFirmwareFile(directory.path() / "nested" / (id + ".bin"), directory.path()), "nested file is treated as owned firmware");
}

void testCleanupQueries() {
    const auto claim = lowered(compileSql(GatewayService::firmwareCleanupClaimQuery()));
    require(claim.find("pg_try_advisory_xact_lock") != std::string::npos, "cleanup claim must not block on the storage lock");
    require(claim.find(std::to_string(service::message::lock::kFirmwareStorageClass)) != std::string::npos && claim.find(std::to_string(service::message::lock::kFirmwareStorageObject)) != std::string::npos, "cleanup claim does not use the firmware storage lock identity");
    const auto expired = lowered(compileSql(GatewayService::expiredFirmwareQuery()));
    require(expired.find("not") != std::string::npos && expired.find("exists") != std::string::npos, "expired firmware ignores referencing tasks");
    require(expired.find("1 hour") != std::string::npos && expired.find("'succeeded'") != std::string::npos && expired.find("'failed'") != std::string::npos, "expired firmware retention or terminal task states changed");
    require(expired.find("limit") == std::string::npos, "unowned records must not starve the per-run removal batch");
    const auto schedule = lowered(compileSql(GatewayService::firmwareCleanupScheduleQuery()));
    require(schedule.find("left join") != std::string::npos, "records without firmware tasks are not scheduled");
    require(schedule.find("filter") != std::string::npos && schedule.find("'succeeded'") != std::string::npos, "unfinished firmware tasks do not suppress the record deadline");
    // Ruvia quotes function names: "max"(...), "now"().
    require(schedule.find("greatest(") != std::string::npos && schedule.find("\"max\"(") != std::string::npos && schedule.find("1 hour") != std::string::npos, "record deadline is not the later of upload and last task use plus retention");
    require(schedule.find("\"ceil\"(") != std::string::npos && schedule.find("\"now\"()") != std::string::npos, "record deadline is not relative to the database clock");
}

void testLostClaimDoesNothing() {
    FirmwareDirectory directory;
    FakeContext context;
    context.state->claimGranted = false;
    std::string id;
    const auto path = directory.managed(id);
    FirmwareDirectory::write(path, kExpired);
    context.state->expired.rows.push_back(row({ id, path.string() }));
    const auto result = runTask(GatewayService::cleanupFirmwares(context, directory.path()));
    require(!result.claimed && result.removedRecords == 0 && result.removedFiles == 0, "cleanup ran without the claim");
    require(context.state->removals.empty() && context.state->commits == 0 && context.state->transactions == 1, "lost claim still touched the database");
    require(std::filesystem::exists(path), "lost claim removed a firmware file");
}

void testClaimedCleanup() {
    FirmwareDirectory directory;
    FakeContext context;
    auto& state = *context.state;
    std::string expiredId;
    const auto expiredPath = directory.managed(expiredId);
    FirmwareDirectory::write(expiredPath, kExpired);
    service::common::UuidV7State outsideUuidState;
    state.expired.rows.push_back(row({ service::utils::nextUuid(outsideUuidState), "/outside/firmware/directory.bin" }));
    state.expired.rows.push_back(row({ expiredId, expiredPath.string() }));
    std::string ignored;
    const auto orphan = directory.managed(ignored);
    const auto freshOrphan = directory.managed(ignored);
    const auto referenced = directory.managed(ignored);
    const auto unrelated = directory.path() / "retain.bin";
    const auto uploading = directory.path() / (ignored + ".upload");
    FirmwareDirectory::write(orphan, kExpired);
    FirmwareDirectory::write(freshOrphan, kRecent);
    FirmwareDirectory::write(referenced, kExpired);
    FirmwareDirectory::write(unrelated, kExpired);
    FirmwareDirectory::write(uploading, kExpired);
    state.references.rows.push_back(row({ referenced.string() }));
    state.watchedAtCommit.push_back(expiredPath);

    const auto result = runTask(GatewayService::cleanupFirmwares(context, directory.path()));
    require(result.claimed && result.removedRecords == 1 && result.removedFiles == 1, "claimed cleanup counts changed");
    require(state.removals.size() == 1 && state.removals.front().find(expiredId) != std::string::npos, "only the owned expired record may be deleted");
    require(state.commits == 2 && state.transactions == 2, "record and orphan phases must each commit their own claim");
    require(!state.watchedExistedAtCommit.empty() && state.watchedExistedAtCommit.front(), "firmware file was removed before its record deletion committed");
    require(!std::filesystem::exists(expiredPath) && !std::filesystem::exists(orphan), "expired record file or old orphan was kept");
    for (const auto& kept : { freshOrphan, referenced, unrelated, uploading }) {
        require(std::filesystem::exists(kept), "protected firmware file was removed: " + kept.string());
    }
}

void testRemovalBatch() {
    FirmwareDirectory directory;
    FakeContext context;
    for (std::size_t index = 0; index <= service::edge::gateway::kFirmwareCleanupBatch; ++index) {
        std::string id;
        const auto path = directory.managed(id);
        context.state->expired.rows.push_back(row({ id, path.string() }));
    }
    const auto result = runTask(GatewayService::cleanupFirmwares(context, directory.path()));
    require(result.removedRecords == service::edge::gateway::kFirmwareCleanupBatch && context.state->removals.size() == service::edge::gateway::kFirmwareCleanupBatch, "one cleanup run exceeded its record batch");
}

void testNextDeadline() {
    FirmwareDirectory directory;
    FakeContext context;
    auto& state = *context.state;
    require(!runTask(GatewayService::nextFirmwareCleanupDelay(context, directory.path())).has_value(), "idle storage produced a cleanup deadline");

    std::string id;
    const auto collectable = directory.managed(id);
    const auto active = directory.managed(id);
    FirmwareDirectory::write(collectable, kExpired);
    FirmwareDirectory::write(active, kExpired);
    state.schedule.rows.push_back(row({ collectable.string(), "0", "5000" }));
    state.schedule.rows.push_back(row({ active.string(), "1", "-100" }));
    state.schedule.rows.push_back(row({ "/outside/firmware/directory.bin", "0", "-10" }));
    auto delay = runTask(GatewayService::nextFirmwareCleanupDelay(context, directory.path()));
    require(delay && *delay == std::chrono::milliseconds(5000), "record deadline ignored task activity, ownership or ordering");

    const auto recentOrphan = directory.managed(id);
    FirmwareDirectory::write(recentOrphan, kRecent);
    FirmwareDirectory::write(directory.path() / "retain.bin", kExpired);
    delay = runTask(GatewayService::nextFirmwareCleanupDelay(context, directory.path()));
    require(delay && *delay == std::chrono::milliseconds(5000), "recent orphan or unowned file advanced the deadline");

    state.schedule.rows.front().cells[2].content = "7200000";
    delay = runTask(GatewayService::nextFirmwareCleanupDelay(context, directory.path()));
    require(delay && *delay > std::chrono::minutes(29) && *delay <= std::chrono::minutes(30), "orphan deadline is not its modification time plus retention");

    const auto expiredOrphan = directory.managed(id);
    FirmwareDirectory::write(expiredOrphan, kExpired);
    delay = runTask(GatewayService::nextFirmwareCleanupDelay(context, directory.path()));
    require(delay && *delay == std::chrono::milliseconds::zero(), "overdue orphan is not due immediately");
}

// The projector records a terminal firmware task on its Worker's signal; the
// request must survive until the driver consumes it, whether or not the driver
// was bound (sleeping) when it arrived.
void testCleanupSignal() {
    service::edge::gateway::FirmwareCleanupSignal signal;
    signal.request();
    require(signal.pending(), "request before the driver binds was dropped");
    int wakes = 0;
    signal.bind([&wakes] {
        ++wakes;
    });
    require(signal.consume() && !signal.pending() && !signal.consume(), "request was not consumed exactly once");
    signal.request();
    signal.request();
    require(wakes == 2 && signal.consume() && !signal.consume(), "bound driver was not woken or requests did not coalesce");
    signal.bind({});
    signal.request();
    require(wakes == 2 && signal.pending(), "unbound driver was woken or the request was lost");
}

} // namespace

int main() {
    try {
        testFirmwareChunks();
        testOwnedFileNames();
        testCleanupQueries();
        testLostClaimDoesNothing();
        testClaimedCleanup();
        testRemovalBatch();
        testNextDeadline();
        testCleanupSignal();
        std::cout << "Firmware chunks, cleanup claim, deletion order, batch, deadline and wakeup passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
