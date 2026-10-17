#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <latch>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <ruvia/core/event_loop_pool.h>
#include <ruvia/core/stop_token.h>

#include "service/modules/edge_node/edge_node.service.h"

namespace {

using service::edge_node::FirmwareUpload;
using DirectoryLock = FirmwareUpload::DirectoryLock;

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <typename Action>
void rejects(Action action) {
    bool rejected = false;
    try {
        action();
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    require(rejected, "invalid upload operation was accepted");
}

class Loop final {
  public:
    Loop() : pool_({ .loop_count_ = 1 }) {
        pool_.start();
        worker_ = pool_.loop(0).handle();
    }

    ~Loop() {
        pool_.stop();
        pool_.join();
    }

    template <typename Value>
    Value run(ruvia::task<Value> task) {
        return pool_.loop(0).start(std::move(task)).get();
    }

    const ruvia::worker_handle& worker() const { return worker_; }

  private:
    ruvia::event_loop_pool pool_;
    ruvia::worker_handle worker_;
};

// Redis claim double: records SET NX options and DEL calls of the recovery claim.
struct ClaimState {
    std::set<std::string> keys;
    std::vector<std::string> deleted;
    std::optional<ruvia::redis_set_options> options;
    std::size_t claims{};
};

struct ClaimResult {
    bool applied_{};

    bool applied() const { return applied_; }
};

struct ClaimRedis {
    ClaimState* state;

    ruvia::task<ClaimResult> set(std::string key, std::string, ruvia::redis_set_options options) const {
        state->options = options;
        const bool absent = !state->keys.contains(key);
        if (options.condition_ != ruvia::redis_set_condition::if_absent || absent) {
            state->keys.insert(key);
            ++state->claims;
        }
        co_return ClaimResult{ absent || options.condition_ != ruvia::redis_set_condition::if_absent };
    }

    ruvia::task<std::int64_t> del(std::string key) const {
        state->deleted.push_back(key);
        co_return static_cast<std::int64_t>(state->keys.erase(key));
    }
};

struct ClaimContext {
    explicit ClaimContext(const Loop& loop) : loop_(loop) {}

    ClaimRedis redis() { return ClaimRedis{ &state }; }

    const ruvia::worker_handle& worker() const { return loop_.worker(); }

    ruvia::stop_token get_stop_token() const { return {}; }

    template <typename State>
    State& worker_state() { return identity; }

    ClaimState state;
    std::optional<service::common::WorkerIdentity> identity{
        std::in_place,
        "upload-test-instance",
        0,
        1,
        service::message::RedisChannelScope{ 0 }
    };

  private:
    const Loop& loop_;
};

std::string claimKey() {
    return service::edge_node::entities::firmwareUploadRecoveryKey("upload-test-instance");
}

void writeOrphan(const std::filesystem::path& path) {
    std::ofstream output(path);
    output << "partial bytes";
}

void testRecoveryRunsOnceAfterClaim(const std::filesystem::path& directory) {
    const auto first = directory / "00000000-0000-4000-8000-000000000011.upload";
    const auto second = directory / "00000000-0000-4000-8000-000000000012.upload";
    writeOrphan(first);
    {
        // A new upload no longer recovers the directory on every request.
        FirmwareUpload upload("00000000-0000-4000-8000-000000000013", directory, 1);
        require(std::filesystem::exists(first), "upload creation recovered abandoned files");
    }
    Loop loop;
    ClaimContext context(loop);
    require(loop.run(service::edge_node::EdgeService::recoverFirmwareUploads(context, directory)) == 1, "claimed recovery did not remove the abandoned upload");
    require(!std::filesystem::exists(first), "claimed recovery left the abandoned upload");
    require(context.state.keys.contains(claimKey()), "recovery did not claim the process instance key");
    require(context.state.options && context.state.options->condition_ == ruvia::redis_set_condition::if_absent, "recovery claim is not SET NX");
    require(context.state.options->expiration_ && context.state.options->expiration_->duration() != nullptr, "recovery claim has no expiry");

    writeOrphan(second);
    require(loop.run(service::edge_node::EdgeService::recoverFirmwareUploads(context, directory)) == 0, "a second worker repeated the claimed recovery");
    require(std::filesystem::exists(second), "an unclaimed worker scanned the upload directory");
    require(context.state.deleted.empty(), "successful recovery released its claim");
    require(FirmwareUpload::recoverAbandoned(directory) == std::optional<std::size_t>{ 1 }, "direct recovery did not remove the remaining upload");
    require(FirmwareUpload::recoverAbandoned(directory) == std::optional<std::size_t>{ 0 }, "recovery is not idempotent");
}

void testFailedRecoveryReleasesClaim(const std::filesystem::path& directory) {
    const auto notDirectory = directory / "not-a-directory";
    writeOrphan(notDirectory);
    Loop loop;
    ClaimContext context(loop);
    bool failed = false;
    try {
        (void)loop.run(service::edge_node::EdgeService::recoverFirmwareUploads(context, notDirectory));
    } catch (const std::exception&) {
        failed = true;
    }
    std::filesystem::remove(notDirectory);
    require(failed, "recovery failure was swallowed");
    require(context.state.deleted == std::vector<std::string>{ claimKey() }, "failed recovery kept its claim");
    require(!context.state.keys.contains(claimKey()), "failed recovery claim remains");
}

// Concurrent uploads share the directory lock: every simultaneous construction succeeds.
void testConcurrentUploadsShareDirectoryLock(const std::filesystem::path& directory) {
    constexpr std::size_t kUploads = 16;
    std::latch start(kUploads);
    std::atomic<std::size_t> succeeded{};
    std::vector<std::string> failures(kUploads);
    std::vector<std::thread> threads;
    for (std::size_t index = 0; index < kUploads; ++index) {
        threads.emplace_back([&, index] {
            try {
                char id[37]{};
                std::snprintf(id, sizeof(id), "00000000-0000-4000-8000-%012zu", 100 + index);
                start.arrive_and_wait();
                FirmwareUpload upload(id, directory, 1);
                upload.appendBytes("a");
                (void)upload.finish();
                ++succeeded;
            } catch (const std::exception& error) {
                failures[index] = error.what();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (const auto& failure : failures) {
        if (!failure.empty()) {
            throw std::runtime_error("concurrent upload construction failed: " + failure);
        }
    }
    require(succeeded == kUploads, "concurrent uploads did not all succeed");
}

// Recovery holding the exclusive directory lock makes a new upload wait, not fail.
void testUploadWaitsForRecovery(const std::filesystem::path& directory) {
    std::atomic<bool> created{};
    std::string failure;
    std::thread uploader;
    {
        DirectoryLock recovery(directory, DirectoryLock::Mode::ExclusiveTry);
        require(recovery.locked(), "test could not take the recovery lock");
        uploader = std::thread([&] {
            try {
                FirmwareUpload upload("00000000-0000-4000-8000-000000000200", directory, 1);
                created = true;
            } catch (const std::exception& error) {
                failure = error.what();
                created = true;
            }
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        require(!created, "upload did not wait for the recovery lock");
    }
    uploader.join();
    require(failure.empty(), "upload failed instead of waiting for recovery");
    require(created, "upload did not resume after recovery released its lock");
}

// An upload in its creation window holds the shared lock: recovery skips the round,
// releases its claim and claims again until the recovery is actually performed.
void testBusyRecoveryRetriesUntilPerformed(const std::filesystem::path& directory) {
    const auto orphan = directory / "00000000-0000-4000-8000-000000000300.upload";
    writeOrphan(orphan);
    auto upload = std::make_unique<DirectoryLock>(directory, DirectoryLock::Mode::SharedWait);
    require(upload->locked(), "test could not take the upload lock");
    require(!FirmwareUpload::recoverAbandoned(directory).has_value(), "recovery ran while an upload held the directory");
    require(std::filesystem::exists(orphan), "skipped recovery removed files");

    Loop loop;
    ClaimContext context(loop);
    std::thread releaser([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        upload.reset();
    });
    std::size_t removed{};
    try {
        removed = loop.run(service::edge_node::EdgeService::recoverFirmwareUploads(context, directory));
    } catch (...) {
        releaser.join();
        throw;
    }
    releaser.join();
    require(removed == 1, "busy recovery was not retried to completion");
    require(!std::filesystem::exists(orphan), "retried recovery left the abandoned upload");
    require(!context.state.deleted.empty() && context.state.deleted.front() == claimKey(), "skipped recovery kept its claim");
    require(context.state.claims >= 2, "skipped recovery was not claimed again");
    require(context.state.keys.contains(claimKey()), "completed recovery does not hold its claim");
}

} // namespace

int main(int argc, char** argv) {
    if (argc == 3) {
        const std::filesystem::path directory(argv[2]);
        if (std::string_view(argv[1]) == "--recover") {
            const auto removed = FirmwareUpload::recoverAbandoned(directory);
            if (!removed) {
                std::cerr << "upload directory is busy\n";
                return 3;
            }
            std::cout << *removed << '\n';
            return 0;
        }
        if (std::string_view(argv[1]) == "--hold") {
            FirmwareUpload file("00000000-0000-4000-8000-000000000003", directory, 3);
            file.appendBytes("a");
            std::cout << "READY" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(60));
            return 0;
        }
        return 2;
    }
    const auto directory = std::filesystem::current_path() / ("api-upload-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directory(directory);
    try {
        const auto orphan = directory / "00000000-0000-4000-8000-000000000001.upload";
        const auto activeId = "00000000-0000-4000-8000-000000000002";
        {
            FirmwareUpload active(activeId, directory, 3);
            active.appendBytes("a");
            writeOrphan(orphan);
            require(FirmwareUpload::recoverAbandoned(directory) == std::optional<std::size_t>{ 1 }, "recovery did not remove only the abandoned upload");
            require(std::filesystem::exists(active.path), "recovery removed a live worker upload");
            active.appendBytes("bc");
            active.finish();
            require(FirmwareUpload::recoverAbandoned(directory) == std::optional<std::size_t>{ 0 }, "recovery removed a finalizing upload");
        }
        require(!std::filesystem::exists(orphan), "orphan remained after recovery");
        {
            FirmwareUpload file("partial", directory, 3);
            rejects([&] {
                file.appendBytes("");
            });
            rejects([&] {
                file.appendBytes(std::string(16385, 'x'));
            });
            rejects([&] {
                file.finish();
            });
            file.appendBytes("a");
            require(file.received == 1, "persisted byte count differs");
            rejects([&] {
                file.appendBytes("abc");
            });
            file.appendBytes("bc");
            const auto digest = file.finish();
            require(digest == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", "incremental digest differs");
            require(file.finish() == digest, "completed digest changed");
            rejects([&] {
                file.appendBytes("a");
            });
            require(std::filesystem::file_size(file.path) == 3, "completed file size differs");
        }
        require(!std::filesystem::exists(directory / "partial.upload"), "abandoned staged file remains");
        {
            FirmwareUpload file("registered", directory, 1);
            file.appendBytes("a");
            file.finish();
            std::filesystem::rename(file.path, directory / "registered.bin");
        }
        require(std::filesystem::exists(directory / "registered.bin"), "registered firmware was deleted");
        testRecoveryRunsOnceAfterClaim(directory);
        testFailedRecoveryReleasesClaim(directory);
        testConcurrentUploadsShareDirectoryLock(directory);
        testUploadWaitsForRecovery(directory);
        testBusyRecoveryRetriesUntilPerformed(directory);
        std::filesystem::remove_all(directory);
        std::cout << "HTTP upload bounds, digest, concurrent locking, claimed recovery and cleanup passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
        return 1;
    }
}
