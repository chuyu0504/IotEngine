#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "service/features/gb28181/device/device.protocol.h"
#include "service/features/gb28181/gb28181.protocol.h"
#include "service/features/gb28181/gb28181.service.h"
#include "service/features/gb28181/gb28181.types.h"
#include "service/features/gb28181/media/media.runtime.h"

namespace {

using service::gb28181::DeviceProjection;
using service::gb28181::ProjectionBarrier;
using service::gb28181::ProjectionQueue;
using service::gb28181::StreamProjection;
using service::gb28181::device::Channel;
using service::gb28181::device::Device;
using service::gb28181::device::DeviceChange;
using service::gb28181::device::DeviceChangeSet;
using service::gb28181::device::DeviceRegistry;
using service::gb28181::media::StreamRegistry;
using service::gb28181::media::StreamStatus;

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

Device queuedDevice(std::string id, std::string name) {
    Device device;
    device.id = std::move(id);
    device.name = std::move(name);
    return device;
}

StreamStatus queuedStream(std::string stream, bool online, int readers) {
    StreamStatus status;
    status.app = "rtp";
    status.stream = std::move(stream);
    status.schema = "rtsp";
    status.online = online;
    status.readerCount = readers;
    return status;
}

void testProjectionQueueCoalescing() {
    ProjectionQueue queue(8);
    queue.pushDevice(queuedDevice("camera-1", "first"), DeviceChange::Status, "token-a", "projection-1");
    queue.pushDevice(queuedDevice("camera-1", "second"), DeviceChange::Catalog, "token-a", "projection-2");
    queue.pushDevice(queuedDevice("camera-1", "other owner"), DeviceChange::Status, "token-b", "projection-3");
    queue.pushStream(queuedStream("stream-1", true, 1), "token-a", "projection-4");
    queue.pushStream(queuedStream("stream-1", false, 0), "token-a", "projection-5");
    require(queue.size() == 3 && queue.sequence() == 5, "projection queue did not coalesce per aggregate and owner token");

    auto first = queue.pop();
    const auto* device = std::get_if<DeviceProjection>(&first);
    require(device != nullptr && device->device.name == "second" && device->projectionId == "projection-1" && device->changes.contains(DeviceChange::Status) && device->changes.contains(DeviceChange::Catalog) && !device->changes.contains(DeviceChange::DeviceName) && device->sequences.first == 1 && device->sequences.last == 2, "coalesced device projection lost its snapshot, id, changes or range");
    auto second = queue.pop();
    const auto* otherOwner = std::get_if<DeviceProjection>(&second);
    require(otherOwner != nullptr && otherOwner->ownerToken == "token-b" && otherOwner->sequences.first == 3 && otherOwner->sequences.last == 3, "projection from another owner token was merged");
    auto third = queue.pop();
    const auto* stream = std::get_if<StreamProjection>(&third);
    require(stream != nullptr && !stream->stream.online && stream->stream.readerCount == 0 && stream->projectionId == "projection-4" && stream->sequences.first == 4 && stream->sequences.last == 5, "coalesced stream projection did not keep the latest state");
    require(queue.empty(), "projection queue retained popped events");

    // A popped event is in flight; a later change must queue a new projection.
    queue.pushDevice(queuedDevice("camera-1", "third"), DeviceChange::DeviceName, "token-a", "projection-6");
    require(queue.size() == 1, "change after pop was merged into an in-flight projection");
    queue.retry(std::move(first));
    queue.pushDevice(queuedDevice("camera-1", "fourth"), DeviceChange::ChannelName, "token-a", "projection-7");
    require(queue.size() == 2, "retried projection broke aggregate indexing");
    auto retried = queue.pop();
    require(std::get<DeviceProjection>(retried).projectionId == "projection-1", "retried projection was not put back at the front");
    auto newer = queue.pop();
    const auto& newerDevice = std::get<DeviceProjection>(newer);
    require(newerDevice.projectionId == "projection-6" && newerDevice.device.name == "fourth" && newerDevice.changes.contains(DeviceChange::DeviceName) && newerDevice.changes.contains(DeviceChange::ChannelName) && newerDevice.sequences.first == 6 && newerDevice.sequences.last == 7, "later changes did not merge into the newest queued snapshot");
}

void testProjectionQueueBarriers() {
    ProjectionQueue queue(2);
    queue.pushDevice(queuedDevice("camera-1", "first"), DeviceChange::Status, "token-a", "projection-1");
    bool completed = false;
    queue.pushBarrier([&completed](bool ok) {
        completed = ok;
    },
                      0);
    queue.pushDevice(queuedDevice("camera-1", "second"), DeviceChange::Catalog, "token-a", "projection-2");
    require(queue.size() == 2, "state change after a barrier was not coalesced");
    auto state = queue.pop();
    require(std::get<DeviceProjection>(state).sequences.last == 2, "coalesced projection ahead of the barrier lost its newer sequence");
    auto barrierEvent = queue.pop();
    auto* barrier = std::get_if<ProjectionBarrier>(&barrierEvent);
    require(barrier != nullptr && barrier->after == 0 && barrier->through == 1, "barrier did not capture the sequences queued before it");
    barrier->complete(true);
    require(completed, "barrier completion was not retained");

    require(!queue.barriersFull(), "empty barrier set reported full");
    queue.pushBarrier([](bool) {
    },
                      2);
    queue.pushBarrier([](bool) {
    },
                      2);
    require(queue.barriersFull(), "barrier capacity was not enforced");
    (void)queue.pop();
    require(!queue.barriersFull(), "popped barrier still counted against capacity");
    queue.pushBarrier([](bool) {
    },
                      2);
    require(queue.barriersFull(), "barrier capacity was not restored");

    queue.recordFailure({ 3, 5 });
    require(queue.failedBetween(2, 3) && queue.failedBetween(4, 9) && !queue.failedBetween(5, 9) && !queue.failedBetween(0, 2), "projection failures did not fail exactly the overlapping barrier windows");
    queue.pruneFailures(9);
    require(queue.failedBetween(4, 9), "failure needed by a queued barrier was pruned");
    auto cleared = queue.clear();
    require(cleared.size() == 2 && queue.empty() && !queue.barriersFull(), "clear did not return every queued barrier");
    queue.recordFailure({ 3, 5 });
    queue.pruneFailures(5);
    require(!queue.failedBetween(0, 9), "failure no barrier can observe was retained");
}

void testProjectionChangeWire() {
    namespace projection = service::gb28181::projection_protocol;
    DeviceChangeSet changes(DeviceChange::Status);
    changes.add(DeviceChange::ChannelName);
    require(projection::projectionChangeNames(changes) == "status,channel_name", "coalesced change list wire format changed");
    require(projection::projectionDeviceChanges("status,channel_name") == changes, "coalesced change list was not parsed");
    require(projection::projectionDeviceChanges("catalog") == DeviceChangeSet(DeviceChange::Catalog), "single change was not parsed");
    require(!projection::projectionDeviceChanges("") && !projection::projectionDeviceChanges("records") && !projection::projectionDeviceChanges("mapping") && !projection::projectionDeviceChanges("status,records") && !projection::projectionDeviceChanges("status,"), "malformed change list was accepted");
}

} // namespace

int main() {
    try {
        constexpr std::string_view instance = "01970000-1234-7000-8000-000000000123";
        const auto controlStream = service::gb28181::control_protocol::stream::control(2, instance);
        require(controlStream == "iot:gb28181:control:worker:01970000-1234-7000-8000-000000000123:2", "GB28181 control stream persisted key changed");
        require(controlStream != service::gb28181::control_protocol::stream::control(3, instance) && controlStream != service::gb28181::control_protocol::stream::control(2, "01970000-1234-7000-8000-000000000124"), "GB28181 control streams collide between workers or process incarnations");
        namespace control = service::gb28181::control_protocol;
        require(control::jsonAction(true, "camera-1", "channel-1") == R"({"code":0,"message":"ok","data":{"sent":true,"device_id":"camera-1","channel_id":"channel-1"}})", "GB28181 action reply wire shape changed");
        require(control::jsonRecording(false) == R"({"code":0,"message":"ok","data":{"recording":false}})", "GB28181 recording reply wire shape changed");
        require(control::operationJson(400, "invalid command deadline") == R"({"code":400,"message":"invalid command deadline"})", "GB28181 operation reply wire shape changed");
        const service::gb28181::sip::SipPreviewStartResult started{
            .sessionId = "session-1",
            .deviceId = "camera-1",
            .channelId = "channel-1",
            .streamId = "stream-1",
            .ssrc = "0100000001",
            .rtpPort = 30000,
            .playUrls = { .httpFlv = "f", .wsFlv = "w", .httpTs = "t", .hls = "h", .webRtc = "r", .rtsp = "s", .rtmp = "m" },
            .leaseTimeoutSeconds = 90
        };
        const auto startReply = control::jsonPreviewStart(started);
        require(startReply == R"({"code":0,"message":"ok","data":{"sent":true,"session_id":"session-1","device_id":"camera-1","channel_id":"channel-1","stream_id":"stream-1","ssrc":"0100000001","rtp_port":30000,"lease_timeout_seconds":90,"play_urls":{"http_flv":"f","ws_flv":"w","http_ts":"t","hls":"h","webrtc":"r","rtsp":"s","rtmp":"m"}}})", "GB28181 preview start reply wire shape changed");
        const auto parsedStart = ruvia::from_json<service::message::gb28181::PreviewStartResponse>(startReply);
        require(parsedStart.get<"code">()->value_ == 0, "GB28181 preview start reply is not a valid model");
        const auto& startData = parsedStart.get<"data">();
        require(startData && startData->get<"playUrls">() && startData->get<"playUrls">()->get<"webrtc">()->view() == "r", "GB28181 preview start reply is not readable by the module");
        const auto stopReply = control::jsonPreviewStop({ .sessionId = "session-1", .streamId = "stream-1", .byeSent = true });
        require(stopReply == R"({"code":0,"message":"ok","data":{"stopped":true,"session_id":"session-1","stream_id":"stream-1","bye_sent":true,"rtp_server_closed":false}})", "GB28181 preview stop reply wire shape changed");
        testProjectionQueueCoalescing();
        testProjectionQueueBarriers();
        testProjectionChangeWire();
        require(service::gb28181::GbProjectionService::integerForTest("12", -1) == 12, "GB28181 projector integer parser changed valid integer");
        require(service::gb28181::GbProjectionService::integerForTest("12x", -1) == -1, "GB28181 projector integer parser accepted trailing garbage");

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
            device,
            DeviceChangeSet(DeviceChange::Catalog),
            2,
            "owner-token"
        );
        require(encodedDevice.get("schema_version") == "1" && encodedDevice.get("change") == "catalog" && encodedDevice.get("owner_token") == "owner-token", "device projection envelope changed");
        const auto decodedDevice = projection::deviceFromProjection(encodedDevice);
        require(decodedDevice.id == device.id && decodedDevice.customName == device.customName && decodedDevice.channels.size() == 1 && decodedDevice.channels[0].customName == "Gate camera", "device projection lost identity or channel custom name");
        auto invalidDevice = encodedDevice;
        for (auto& field : invalidDevice.fields) {
            if (field.name == "channel_count") {
                field.value = "100001";
            }
        }
        bool rejectedCount = false;
        try {
            (void)projection::deviceFromProjection(invalidDevice);
        } catch (const std::runtime_error&) {
            rejectedCount = true;
        }
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
        require(decodedStream.online && decodedStream.readerCount == 3 && decodedStream.stream == "preview-stream", "stream projection lost viewer or identity fields");
        DeviceRegistry registry;
        registry.replace({ device });
        registry.updateCatalog(
            device.id,
            { Channel{ .id = "channel-1", .name = "Changed reported channel" } }
        );
        const auto refreshed = registry.findDevice(device.id);
        require(refreshed.has_value(), "renamed GB28181 device disappeared");
        require(refreshed->displayName() == "Production camera", "device custom name was not preserved");
        require(refreshed->channels.size() == 1 && refreshed->channels[0].displayName() == "Gate camera", "catalog refresh replaced the channel custom name");
        require(registry.updateDeviceName(device.id, "Renamed camera"), "device rename failed");
        require(
            registry.updateChannelName(device.id, "channel-1", "Renamed channel"),
            "channel rename failed"
        );
        const auto renamed = registry.findDevice(device.id);
        require(renamed && renamed->displayName() == "Renamed camera" && renamed->channels[0].displayName() == "Renamed channel", "custom names were not updated in the runtime registry");

        std::vector<StreamStatus> projectedStreams;
        StreamRegistry streams([&projectedStreams](const StreamStatus& stream) {
            projectedStreams.push_back(stream);
        });
        streams.updateViewerCount("preview-stream", 1);
        streams.updateStreamChanged("rtp", "preview-stream", "rtsp", true, 0);
        require(projectedStreams.back().readerCount == 1, "pending preview viewer count was not applied on registration");
        streams.updateViewerCount("preview-stream", 2);
        streams.updateViewerCount("preview-stream", 1);
        require(projectedStreams[projectedStreams.size() - 2].readerCount == 2 && projectedStreams.back().readerCount == 1, "preview viewer count did not track concurrent viewers");
        streams.updateStreamChanged("rtp", "preview-stream", "rtsp", false, 99);
        require(!projectedStreams.back().online && projectedStreams.back().readerCount == 0, "stream deregistration retained an unsafe reader count");
        std::cout << "GB28181 projector tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "GB28181 projector test failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
