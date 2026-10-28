#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "service/common/message.h"
#include "service/features/configuration/configuration.types.h"
#include "service/features/edge/edge.entity.h"
#include "service/features/edge/edge.protocol.h"
#include "service/features/edge/edge.types.h"
#include "service/features/edge/projection/projection.entity.h"

namespace service::edge::projection {

RUVIA_MODEL(ExternalIoChannelCapability, RUVIA_REQUIRED_FIELD(channel, ruvia::string), RUVIA_REQUIRED_FIELD(kind, ruvia::string), RUVIA_REQUIRED_FIELD(available, ruvia::bool_value), RUVIA_REQUIRED_FIELD(controllable, ruvia::bool_value));
RUVIA_MODEL(ExternalIoCapability, RUVIA_REQUIRED_FIELD(supported, ruvia::bool_value), RUVIA_REQUIRED_FIELD(channels, ruvia::array<ExternalIoChannelCapability>));

// Raw-acquisition parsing items of one node, loaded on first use and dropped
// when the node's metadata changes.
struct AcquisitionConfiguration final {
    std::vector<pb::ConfigItem> items;
    std::map<std::string, service::configuration::DerivedConfiguration> calculations;
};

// Worker-local projection state; owned by ProjectionRuntime and never shared.
struct ProjectionState final {
    metadata::Catalog catalog;
    std::map<std::string, AcquisitionConfiguration, std::less<>> acquisitionConfigurations;
    // Snapshots are immutable. Bound the per-worker cache without changing replay semantics.
    std::map<std::string, AcquisitionSnapshot, std::less<>> acquisitionSnapshots;
};

// Effects of one projected Stream batch, applied after every entry is projected.
struct ProjectionBatch final {
    std::vector<service::message::ParsedDeviceMessage> telemetry;
    std::vector<persistence::TelemetryUploadRecord> completedUploads;
    // Telemetry message ID -> derived-point configuration of a raw acquisition.
    std::map<std::string, service::configuration::DerivedConfiguration, std::less<>> rawCalculations;
};

struct DerivedDeadline final {
    service::message::ParsedDeviceMessage parsed;
    service::configuration::DerivedConfiguration config;
    std::optional<std::int64_t> deadline;
};

// Lease, Stream and token of one projector Stream this Worker consumes.
struct ProjectorStreamOwner final {
    std::string stream;
    std::string lease;
    std::string token;
};

} // namespace service::edge::projection
