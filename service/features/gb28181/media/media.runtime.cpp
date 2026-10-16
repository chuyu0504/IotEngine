#include "service/features/gb28181/media/media.runtime.h"

#include <algorithm>

namespace service::gb28181::media {

void StreamRegistry::updateStreamChanged(const std::string& app, const std::string& stream, const std::string& schema, bool online, int readerCount) {
    if (!online) {
        readerCount = 0;
        viewerCounts_.erase(stream);
    } else if (const auto viewer = viewerCounts_.find(stream);
               viewer != viewerCounts_.end()) {
        readerCount = viewer->second;
    }

    auto& status = streams_[keyFor(app, stream, schema)];
    status.app = app;
    status.stream = stream;
    status.schema = schema;
    status.online = online;
    status.readerCount = readerCount;
    if (observer_) {
        observer_(status);
    }
}

void StreamRegistry::updateViewerCount(const std::string& stream, int viewerCount) {
    viewerCount = std::max(0, viewerCount);
    if (viewerCount > 0) {
        viewerCounts_.insert_or_assign(stream, viewerCount);
    } else {
        viewerCounts_.erase(stream);
    }

    for (auto& [_, status] : streams_) {
        if (!status.online || status.stream != stream ||
            status.readerCount == viewerCount) {
            continue;
        }
        status.readerCount = viewerCount;
        if (observer_) {
            observer_(status);
        }
    }
}

std::vector<StreamStatus> StreamRegistry::listStreams() const {
    std::vector<StreamStatus> result;
    result.reserve(streams_.size());
    for (const auto& [_, status] : streams_) {
        result.push_back(status);
    }
    return result;
}

std::string StreamRegistry::keyFor(const std::string& app, const std::string& stream, const std::string& schema) {
    return app + "\n" + stream + "\n" + schema;
}

void StreamRegistry::replace(std::vector<StreamStatus> streams) {
    streams_.clear();
    viewerCounts_.clear();
    for (auto& stream : streams) {
        streams_.emplace(keyFor(stream.app, stream.stream, stream.schema), std::move(stream));
    }
}

} // namespace service::gb28181::media
