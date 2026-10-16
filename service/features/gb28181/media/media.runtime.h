#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "service/features/gb28181/media/media.types.h"

namespace service::gb28181::media {

class StreamRegistry {
  public:
    using Observer = std::function<void(const StreamStatus&)>;

    explicit StreamRegistry(Observer observer = {})
        : observer_(std::move(observer)) {}

    void updateStreamChanged(const std::string& app, const std::string& stream, const std::string& schema, bool online, int readerCount);
    void updateViewerCount(const std::string& stream, int viewerCount);
    std::vector<StreamStatus> listStreams() const;
    void replace(std::vector<StreamStatus> streams);

  private:
    static std::string keyFor(const std::string& app, const std::string& stream, const std::string& schema);

    std::unordered_map<std::string, StreamStatus> streams_;
    std::unordered_map<std::string, int> viewerCounts_;
    Observer observer_;
};

} // namespace service::gb28181::media
