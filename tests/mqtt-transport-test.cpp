#include <iostream>

#include "service/features/collector/engine/engine.runtime.h"
#include "service/features/collector/mqtt/mqtt.protocol.h"
#include "service/features/collector/scheduling/scheduling.runtime.h"
#include "service/features/collector/tcp/tcp.transport.h"
#include "service/utils/uuid.h"

namespace {
using namespace service::collector;

void require(bool value, const char* message) {
    if (!value) {
        throw std::runtime_error(message);
    }
}

void testBrokerReconnect() {
    asio::io_context io;
    asio::ip::tcp::acceptor broker(io, { asio::ip::address_v4::loopback(), 0 });
    asio::ip::tcp::socket peer(io);
    asio::steady_timer timeout(io, std::chrono::seconds(8));
    DeadlineScheduler scheduler(io);
    service::common::UuidV7State uuid;
    RuntimeSnapshot snapshot;
    LinkDefinition link{ .id = "wire-link", .mode = "TCP Client", .protocol = "MQTT", .status = "enabled" };
    link.targets.push_back({ .id = "broker", .ip = "localhost", .port = broker.local_endpoint().port(), .status = "enabled", .mqttConfig = R"({"clientId":"platform-wire-test","keepAliveSeconds":60})" });
    snapshot.links.push_back(link);
    snapshot.devices.push_back({ .calculationConfig = R"({"identityProperty":"serial","topic":"devices/0","qos":1,"points":[{"id":"temperature","name":"温度","field":"temperature","dataType":"DOUBLE"}]})", .id = "wire-device", .code = "0", .linkId = "wire-link", .targetId = "broker", .protocol = "MQTT" });
    snapshot.devices.front().parameters = R"({"serial":"0"})";
    ProtocolSessionFactoryRegistry registry;
    registry.add(std::make_unique<mqtt::Factory>());
    ProtocolEngine engine(std::move(registry));
    engine.reload(snapshot);
    TcpTransport* transport = nullptr;
    std::map<std::pair<std::string, std::uint64_t>, DeadlineScheduler::Token> deadlines;
    std::vector<service::message::ParsedDeviceMessage> telemetry;
    std::set<std::string> connections;
    unsigned subscriptions = 0, acknowledgements = 0, claims = 0, releases = 0;
    bool timedOut = false;
    std::function<void(std::vector<ProtocolAction>)> apply;
    apply = [&](std::vector<ProtocolAction> actions) {
        for (auto& action : actions) {
            const auto key = std::make_pair(action.connectionId, action.deadlineToken);
            switch (action.kind) {
                case ProtocolActionKind::Send:
                    transport->send(action.connectionId, std::move(action.bytes), {});
                    break;
                case ProtocolActionKind::PublishParsed:
                    telemetry.push_back(action.parsed);
                    apply(engine.parsedPublished(action.connectionId, action.publicationToken));
                    break;
                case ProtocolActionKind::ScheduleDeadline:
                    if (deadlines.contains(key)) {
                        scheduler.cancel(deadlines[key]);
                    }
                    deadlines[key] = scheduler.scheduleAfter(action.deadlineAfter, [&, key] {
                        apply(engine.deadline(key.first, key.second));
                    });
                    break;
                case ProtocolActionKind::CancelDeadline:
                    if (deadlines.contains(key)) {
                        scheduler.cancel(deadlines[key]);
                        deadlines.erase(key);
                    }
                    break;
                case ProtocolActionKind::Close:
                    transport->close(action.connectionId, action.reason);
                    break;
                default:
                    break;
            }
        }
    };
    TcpTransport tcp(uuid, io, scheduler.borrowTcpDeadlines(), 1, 3, [&](ProtocolConnectionInfo info) {
        connections.insert(info.connectionId);
        apply(engine.connected(std::move(info)));
    },
                     [&](IngressPacket input) {
                         input.messageId = service::utils::nextUuid(uuid);
                         apply(engine.consume(input));
                     },
                     [&](std::string id, std::string reason) {
                         apply(engine.disconnected(id, reason));
                     },
                     [](LinkState) {
                     },
                     [&](std::string, std::string, std::string, std::function<void(bool)> result) {
                         ++claims;
                         result(true);
                     },
                     [&](std::string, std::string, std::string) {
                         ++releases;
                     });
    transport = &tcp;
    std::array<std::uint8_t, 4096> incoming{};
    mqtt::Bytes buffer;
    std::function<void()> read, accept;
    unsigned phase = 0;
    read = [&] {
        peer.async_read_some(asio::buffer(incoming), [&](std::error_code error, std::size_t size) {
            if (error) {
                return;
            }
            buffer.insert(buffer.end(), incoming.begin(), incoming.begin() + size);
            while (buffer.size() >= 2) {
                std::size_t position = 1, length = 0, multiplier = 1;
                while (position < buffer.size()) {
                    const auto digit = buffer[position++];
                    length += (digit & 127) * multiplier;
                    multiplier *= 128;
                    if (!(digit & 128)) {
                        break;
                    }
                }
                if (position > buffer.size() || (buffer[position - 1] & 128) || buffer.size() < position + length) {
                    break;
                }
                const auto header = buffer.front();
                const mqtt::Bytes body(buffer.begin() + position, buffer.begin() + position + length);
                buffer.erase(buffer.begin(), buffer.begin() + position + length);
                if (phase == 0) {
                    require(header == 0x10 && body.size() >= 10 && body[6] == 4 && body[7] == 2, "wire CONNECT version or clean session invalid");
                    const std::array<std::uint8_t, 4> connack{ 0x20, 2, 0, 0 };
                    asio::write(peer, asio::buffer(connack));
                    ++phase;
                } else if (phase == 1) {
                    require(header == 0x82 && body.size() == 14 && body[2] == 0 && body[3] == 9 && body.back() == 1, "wire SUBSCRIBE invalid");
                    ++subscriptions;
                    ++phase;
                    // 独立 Broker 固定报文；SUBACK 和 PUBLISH 在同一次写入中发送。
                    const mqtt::Bytes response{ 0x90, 3, body[0], body[1], 1, 0x32, 30, 0, 9, 'd', 'e', 'v', 'i', 'c', 'e', 's', '/', '0', 0, 7, '{', '"', 't', 'e', 'm', 'p', 'e', 'r', 'a', 't', 'u', 'r', 'e', '"', ':', '8', '}' };
                    asio::write(peer, asio::buffer(response));
                } else {
                    require(header == 0x40 && body == mqtt::Bytes({ 0, 7 }) && telemetry.size() == subscriptions, "wire PUBACK did not follow telemetry publication");
                    ++acknowledgements;
                    std::error_code ignored;
                    peer.close(ignored);
                    if (acknowledgements == 2) {
                        tcp.stop();
                        scheduler.stop();
                        io.stop();
                    } else {
                        accept();
                    }
                    return;
                }
            }
            read();
        });
    };
    accept = [&] {
        phase = 0;
        buffer.clear();
        peer = asio::ip::tcp::socket(io);
        broker.async_accept(peer, [&](std::error_code error) {
            if (!error) {
                read();
            }
        });
    };
    accept();
    tcp.reload(snapshot);
    timeout.async_wait([&](std::error_code error) {
        if (!error) {
            timedOut = true;
            tcp.stop();
            scheduler.stop();
            io.stop();
        }
    });
    io.run();
    require(!timedOut && acknowledgements == 2 && subscriptions == 2 && connections.size() == 2, "MQTT did not reconnect and resubscribe");
    require(claims == 2 && releases == 2, "MQTT target ownership was not released");
    require(telemetry[0].acquisitionId != telemetry[1].acquisitionId, "reconnect reused telemetry identity");
}
} // namespace

int main() {
    try {
        testBrokerReconnect();
        std::cout << "MQTT transport tests passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
