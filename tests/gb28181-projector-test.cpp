#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <string_view>
#include <asio.hpp>
#include <ruvia/core/event_loop_pool.h>

#include "service/features/gb28181/gb28181.service.h"
#include "service/features/gb28181/gb28181.protocol.h"
#include "service/features/gb28181/device/device.runtime.h"
#include "service/features/gb28181/media/media.runtime.h"

namespace {

void require(bool condition, std::string_view message) {
  if (!condition)
    throw std::runtime_error(std::string(message));
}

struct ProjectionQueryCompiler {
  std::vector<std::string> statements;
  ruvia::task<void> execute(const ruvia::db_query& query) {
    auto statement = query.compile(ruvia::db_driver::postgresql,
        std::pmr::get_default_resource(), ruvia::db_parameter_mode::literal);
    statements.emplace_back(statement.sql());
    co_return;
  }
};

struct ProjectionQueries : service::gb28181::GbProjectionService {
  using GbProjectionService::syncChannels;
};

void completeProjection(ruvia::task<void> task) {
  ruvia::event_loop_pool pool({.loop_count_ = 1});
  pool.start();
  pool.loop(0).start(std::move(task)).get();
  pool.stop();
  pool.join();
}

void testNonemptyProjectionQueries() {
  Device device;
  device.id = "camera-query-test";
  device.channels.push_back(Channel{.id="channel-1", .name="Camera's channel"});
  ProjectionQueryCompiler transaction;
  completeProjection(ProjectionQueries::syncChannels(transaction, device));
  require(transaction.statements.size() == 1, "nonempty projection did not compile the channel write");
  require(transaction.statements[0].find("gb28181_channel") != std::string::npos,
          "projection compiled a different storage target");
}

} // namespace

int main() {
  try {
    constexpr std::string_view instance = "01970000-1234-7000-8000-000000000123";
    const auto controlStream = service::gb28181::control_protocol::stream::control(2, instance);
    require(controlStream == "iot:gb28181:control:worker:01970000-1234-7000-8000-000000000123:2",
            "GB28181 control stream persisted key changed");
    require(controlStream != service::gb28181::control_protocol::stream::control(3, instance) &&
            controlStream != service::gb28181::control_protocol::stream::control(
                2, "01970000-1234-7000-8000-000000000124"),
            "GB28181 control streams collide between workers or process incarnations");
    testNonemptyProjectionQueries();
    require(service::gb28181::GbProjectionService::integerForTest("12", -1) == 12,
            "GB28181 projector integer parser changed valid integer");
    require(service::gb28181::GbProjectionService::integerForTest("12x", -1) == -1,
            "GB28181 projector integer parser accepted trailing garbage");

    Device device;
    device.id = "camera-1";
    device.name = "Reported camera";
    device.customName = "Production camera";
    device.channels.push_back(Channel{
        .id = "channel-1",
        .name = "Reported channel",
        .customName = "Gate camera",
    });
    namespace projection = service::gb28181::projection_protocol;
    service::message::StreamMessage encodedDevice;
    encodedDevice.fields = projection::deviceProjectionFields(
        device, DeviceChange::Catalog, 2, "owner-token");
    require(encodedDevice.get("schema_version") == "1" &&
                encodedDevice.get("change") == "catalog" &&
                encodedDevice.get("owner_token") == "owner-token",
            "device projection envelope changed");
    const auto decodedDevice = projection::deviceFromProjection(encodedDevice);
    require(decodedDevice.id == device.id && decodedDevice.customName == device.customName &&
                decodedDevice.channels.size() == 1 &&
                decodedDevice.channels[0].customName == "Gate camera",
            "device projection lost identity or channel custom name");
    require(!projection::projectionDeviceChange("records") &&
                !projection::projectionDeviceChange("mapping"),
            "retired device projection changes must use the malformed-projection path");
    auto invalidDevice = encodedDevice;
    for (auto& field : invalidDevice.fields) {
      if (field.name == "channel_count") field.value = "100001";
    }
    bool rejectedCount = false;
    try { (void)projection::deviceFromProjection(invalidDevice); }
    catch (const std::runtime_error&) { rejectedCount = true; }
    require(rejectedCount, "projection accepted an excessive channel count");
    StreamStatus streamProjection;
    streamProjection.app = "rtp";
    streamProjection.stream = "preview-stream";
    streamProjection.schema = "rtsp";
    streamProjection.online = true;
    streamProjection.readerCount = 3;
    service::message::StreamMessage encodedStream;
    encodedStream.fields = projection::streamProjectionFields(streamProjection, 2, "owner-token");
    const auto decodedStream = projection::streamFromProjection(encodedStream);
    require(decodedStream.online && decodedStream.readerCount == 3 &&
                decodedStream.stream == "preview-stream",
            "stream projection lost viewer or identity fields");
    DeviceRegistry registry;
    registry.replace({device});
    registry.updateCatalog(
        device.id,
        {Channel{.id = "channel-1", .name = "Changed reported channel"}});
    const auto refreshed = registry.findDevice(device.id);
    require(refreshed.has_value(), "renamed GB28181 device disappeared");
    require(refreshed->displayName() == "Production camera",
            "device custom name was not preserved");
    require(refreshed->channels.size() == 1 &&
                refreshed->channels[0].displayName() == "Gate camera",
            "catalog refresh replaced the channel custom name");
    require(registry.updateDeviceName(device.id, "Renamed camera"),
            "device rename failed");
    require(
        registry.updateChannelName(device.id, "channel-1", "Renamed channel"),
        "channel rename failed");
    const auto renamed = registry.findDevice(device.id);
    require(renamed && renamed->displayName() == "Renamed camera" &&
                renamed->channels[0].displayName() == "Renamed channel",
            "custom names were not updated in the runtime registry");

    std::vector<StreamStatus> projectedStreams;
    StreamRegistry streams([&projectedStreams](const StreamStatus &stream) {
      projectedStreams.push_back(stream);
    });
    streams.updateViewerCount("preview-stream", 1);
    streams.updateStreamChanged("rtp", "preview-stream", "rtsp", true, 0);
    require(projectedStreams.back().readerCount == 1,
            "pending preview viewer count was not applied on registration");
    streams.updateViewerCount("preview-stream", 2);
    streams.updateViewerCount("preview-stream", 1);
    require(projectedStreams[projectedStreams.size() - 2].readerCount == 2 &&
                projectedStreams.back().readerCount == 1,
            "preview viewer count did not track concurrent viewers");
    streams.updateStreamChanged("rtp", "preview-stream", "rtsp", false, 99);
    require(!projectedStreams.back().online &&
                projectedStreams.back().readerCount == 0,
            "stream deregistration retained an unsafe reader count");
    std::cout << "GB28181 projector tests passed\n";
    return EXIT_SUCCESS;
  } catch (const std::exception &error) {
    std::cerr << "GB28181 projector test failed: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
