#pragma once

#include <cstddef>
#include <cstdint>
#include <edge.pb.h>
#include <map>
#include <string>
#include <vector>

#include <ruvia/web/model.h>

#include "service/features/configuration/configuration.types.h"

namespace service::edge {

RUVIA_MODEL(SnapshotCalculation, RUVIA_REQUIRED_FIELD(deviceId, ruvia::string), RUVIA_REQUIRED_FIELD(config, ruvia::string), RUVIA_REQUIRED_FIELD(sources, ruvia::array<ruvia::string>));
RUVIA_MODEL(SnapshotDocument, RUVIA_OPTIONAL_FIELD(epoch, ruvia::int64), RUVIA_REQUIRED_FIELD(wires, ruvia::array<ruvia::string>), RUVIA_REQUIRED_FIELD(calculations, ruvia::array<SnapshotCalculation>));

RUVIA_MODEL(AcquisitionSampleDocument, RUVIA_REQUIRED_FIELD(linkId, ruvia::string), RUVIA_REQUIRED_FIELD(modelId, ruvia::string), RUVIA_REQUIRED_FIELD(protocol, ruvia::string), RUVIA_REQUIRED_FIELD(occurredAtMs, ruvia::int64), RUVIA_REQUIRED_FIELD(valuesJson, ruvia::string), RUVIA_REQUIRED_FIELD(storagePolicy, ruvia::string));

struct AcquisitionSnapshot final {
    std::uint64_t revision{};
    std::int64_t epoch = -1;
    std::string digest;
    std::size_t itemCount{};
    std::vector<std::string> wires;
    std::vector<::iot::edge::v1::ConfigItem> items;
    std::map<std::string, service::configuration::DerivedConfiguration> calculations;
};

} // namespace service::edge
