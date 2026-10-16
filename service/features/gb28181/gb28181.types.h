#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iterator>
#include <list>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "service/features/gb28181/device/device.types.h"
#include "service/features/gb28181/media/media.types.h"

namespace service::gb28181 {

enum class ControlClaimResult : std::int64_t {
    kExpired = -3,
    kCancelled = -2,
    kOwnerLost = -1,
    kAlreadyClaimed = 0,
    kClaimed = 1,
};

struct ProjectionPublishResult final {
    std::string order;
    std::string current;
};

struct ProjectionSnapshot final {
    std::vector<device::Device> devices;
};

// An owner deadline claimed by one Service Worker.  The score is the claim's
// processing deadline; completion removes the member only while it is unchanged.
struct DueOwner final {
    std::string member;
    std::string score;
};

struct DueOwnerClaim final {
    std::int64_t now{ 0 };
    std::optional<std::int64_t> next;
    std::vector<DueOwner> owners;
};

// Inclusive range of local projection sequences carried by one queued event.
struct ProjectionSequenceRange final {
    std::uint64_t first{ 0 };
    std::uint64_t last{ 0 };
};

struct DeviceProjection final {
    device::Device device;
    device::DeviceChangeSet changes;
    std::string ownerToken;
    std::string projectionId;
    ProjectionSequenceRange sequences;
};

struct StreamProjection final {
    media::StreamStatus stream;
    std::string ownerToken;
    std::string projectionId;
    ProjectionSequenceRange sequences;
};

using ProjectionCompletion = std::function<void(bool)>;

// A barrier completes with true only when every projection sequence in
// (after, through] has been durably confirmed.
struct ProjectionBarrier final {
    ProjectionCompletion complete;
    std::uint64_t after{ 0 };
    std::uint64_t through{ 0 };
};

using ProjectionEvent = std::variant<DeviceProjection, StreamProjection, ProjectionBarrier>;

// Ordered projection queue of one Collector actor.
//
// State projections are coalesced per aggregate and owner token: a later
// snapshot replaces the queued one in place, keeps its projection id and
// widens its sequence range.  The number of queued state projections is
// therefore bounded by the aggregates this actor owns.  Barriers are bounded by
// `barrierCapacity`; a rejected barrier is reported to the caller, which fails
// the waiting SIP transaction or control command instead of queueing it.
// A failed coalesced projection fails every barrier that overlaps its range.
class ProjectionQueue final {
  public:
    explicit ProjectionQueue(std::size_t barrierCapacity) : barrierCapacity_(barrierCapacity) {}

    [[nodiscard]] bool empty() const noexcept { return events_.empty(); }

    [[nodiscard]] std::size_t size() const noexcept { return events_.size(); }

    [[nodiscard]] std::uint64_t sequence() const noexcept { return sequence_; }

    void pushDevice(const device::Device& device, device::DeviceChange change, std::string ownerToken, std::string projectionId) {
        const auto sequence = ++sequence_;
        auto key = aggregateKey("device", device.id, ownerToken);
        if (const auto found = pending_.find(key); found != pending_.end()) {
            auto& queued = std::get<DeviceProjection>(*found->second);
            queued.device = device;
            queued.changes.add(change);
            queued.sequences.last = sequence;
            return;
        }
        events_.push_back(DeviceProjection{
            .device = device,
            .changes = device::DeviceChangeSet(change),
            .ownerToken = std::move(ownerToken),
            .projectionId = std::move(projectionId),
            .sequences = { sequence, sequence },
        });
        pending_.emplace(std::move(key), std::prev(events_.end()));
    }

    void pushStream(const media::StreamStatus& stream, std::string ownerToken, std::string projectionId) {
        const auto sequence = ++sequence_;
        auto key = aggregateKey("stream", media::StreamStatus::identity(stream.app, stream.stream, stream.schema), ownerToken);
        if (const auto found = pending_.find(key); found != pending_.end()) {
            auto& queued = std::get<StreamProjection>(*found->second);
            queued.stream = stream;
            queued.sequences.last = sequence;
            return;
        }
        events_.push_back(StreamProjection{
            .stream = stream,
            .ownerToken = std::move(ownerToken),
            .projectionId = std::move(projectionId),
            .sequences = { sequence, sequence },
        });
        pending_.emplace(std::move(key), std::prev(events_.end()));
    }

    [[nodiscard]] bool barriersFull() const noexcept { return barrierAfters_.size() >= barrierCapacity_; }

    // The caller checks barriersFull() first and fails a rejected barrier.
    void pushBarrier(ProjectionCompletion completion, std::uint64_t after) {
        events_.push_back(ProjectionBarrier{ std::move(completion), after, sequence_ });
        barrierAfters_.insert(after);
    }

    [[nodiscard]] ProjectionEvent pop() {
        auto event = std::move(events_.front());
        unindex(event, events_.begin());
        events_.pop_front();
        return event;
    }

    // Puts an event that could not be processed back at the front.  A newer
    // queued snapshot of the same aggregate keeps receiving later changes.
    void retry(ProjectionEvent event) {
        events_.push_front(std::move(event));
        const auto front = events_.begin();
        if (const auto* barrier = std::get_if<ProjectionBarrier>(&*front)) {
            barrierAfters_.insert(barrier->after);
        } else if (auto key = stateKey(*front)) {
            pending_.try_emplace(std::move(*key), front);
        }
    }

    void recordFailure(ProjectionSequenceRange range) { failures_.push_back(range); }

    [[nodiscard]] bool failedBetween(std::uint64_t after, std::uint64_t through) const {
        for (const auto& failure : failures_) {
            if (failure.last > after && failure.first <= through) {
                return true;
            }
        }
        return false;
    }

    // Drops failures that no queued or future barrier can observe.
    void pruneFailures(std::uint64_t oldestNeeded) {
        if (!barrierAfters_.empty()) {
            oldestNeeded = std::min(oldestNeeded, *barrierAfters_.begin());
        }
        std::erase_if(failures_, [oldestNeeded](const ProjectionSequenceRange& failure) {
            return failure.last <= oldestNeeded;
        });
    }

    // Removes every queued event; the caller completes the returned barriers.
    [[nodiscard]] std::vector<ProjectionBarrier> clear() {
        std::vector<ProjectionBarrier> barriers;
        for (auto& event : events_) {
            if (auto* barrier = std::get_if<ProjectionBarrier>(&event)) {
                barriers.push_back(std::move(*barrier));
            }
        }
        events_.clear();
        pending_.clear();
        barrierAfters_.clear();
        failures_.clear();
        return barriers;
    }

  private:
    using EventList = std::list<ProjectionEvent>;

    static std::string aggregateKey(std::string_view kind, std::string_view aggregateId, std::string_view ownerToken) {
        std::string key;
        key.reserve(kind.size() + aggregateId.size() + ownerToken.size() + 2);
        key.append(kind).push_back('\n');
        key.append(aggregateId).push_back('\n');
        key.append(ownerToken);
        return key;
    }

    static std::optional<std::string> stateKey(const ProjectionEvent& event) {
        if (const auto* device = std::get_if<DeviceProjection>(&event)) {
            return aggregateKey("device", device->device.id, device->ownerToken);
        }
        if (const auto* stream = std::get_if<StreamProjection>(&event)) {
            return aggregateKey("stream", media::StreamStatus::identity(stream->stream.app, stream->stream.stream, stream->stream.schema), stream->ownerToken);
        }
        return std::nullopt;
    }

    void unindex(const ProjectionEvent& event, EventList::iterator position) {
        if (const auto* barrier = std::get_if<ProjectionBarrier>(&event)) {
            if (const auto found = barrierAfters_.find(barrier->after); found != barrierAfters_.end()) {
                barrierAfters_.erase(found);
            }
            return;
        }
        if (const auto key = stateKey(event)) {
            if (const auto found = pending_.find(*key); found != pending_.end() && found->second == position) {
                pending_.erase(found);
            }
        }
    }

    std::size_t barrierCapacity_;
    std::uint64_t sequence_{ 0 };
    EventList events_;
    std::unordered_map<std::string, EventList::iterator> pending_;
    std::multiset<std::uint64_t> barrierAfters_;
    std::deque<ProjectionSequenceRange> failures_;
};

} // namespace service::gb28181
