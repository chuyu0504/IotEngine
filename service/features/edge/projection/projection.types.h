#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "service/common/message.h"
#include "service/features/edge/edge.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/projection/projection.entity.h"

namespace service::edge::projection {

// Raw-acquisition parsing items of one node, loaded on first use and dropped
// when the node's metadata changes.
struct AcquisitionConfiguration final {
    std::vector<pb::ConfigItem> items;
    std::map<std::string, std::string> calculations;
};

// Worker-local projection state; owned by ProjectionRuntime and never shared.
struct ProjectionState final {
    metadata::Catalog catalog;
    std::map<std::string, AcquisitionConfiguration, std::less<>> acquisitionConfigurations;
};

// Effects of one projected Stream batch, applied after every entry is projected.
struct ProjectionBatch final {
    std::vector<service::message::ParsedDeviceMessage> telemetry;
    std::vector<persistence::TelemetryUploadRecord> completedUploads;
    // Telemetry message ID -> derived-point configuration of a raw acquisition.
    std::map<std::string, std::string, std::less<>> rawCalculations;
};

struct DerivedDeadline final {
    service::message::ParsedDeviceMessage parsed;
    std::string config;
    std::optional<std::int64_t> deadline;
};

// Lease, Stream and token of one projector Stream this Worker consumes.
struct ProjectorStreamOwner final {
    std::string stream;
    std::string lease;
    std::string token;
};

} // namespace service::edge::projection
