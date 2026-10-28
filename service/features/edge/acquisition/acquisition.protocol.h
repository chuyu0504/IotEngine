#pragma once

#include "service/common/uuid.h"
#include "service/features/collector/dlt645/dlt645.protocol.h"
#include "service/features/collector/fins/fins.protocol.h"
#include "service/features/collector/mc/mc.protocol.h"
#include "service/features/collector/modbus/modbus.protocol.h"
#include "service/features/collector/s7/s7.protocol.h"
#include "service/features/collector/sl651/sl651.protocol.h"
#include "service/features/edge/edge.protocol.h"

namespace service::edge::acquisition {

// 仅解析完整业务交换；不持有连接，不执行握手，不访问数据库。
class RawAcquisition final {
  public:
    static collector::DeviceDefinition device(const std::vector<pb::ConfigItem>& items, std::string_view id) {
        collector::DeviceDefinition result;
        const pb::DeviceConfig* configured = nullptr;
        for (const auto& item : items) {
            if (item.has_device() && item.device().device_id() == id) {
                configured = &item.device();
            }
        }
        if (!configured) {
            throw std::invalid_argument("edge acquisition device is not configured");
        }
        result.id = service::common::uuidText(id);
        result.code = configured->device_code();
        result.linkId = service::common::uuidText(configured->endpoint_id());
        result.protocol = protocol::TelemetryValues::protocolName(configured->protocol());
        result.timezone = configured->timezone();
        result.sl651ResponseMode = "M" + std::to_string(std::max(1U, configured->sl651_response_mode()));
        result.modbusMode = configured->modbus_mode();
        result.slaveId = static_cast<std::uint8_t>(configured->modbus_slave_id());
        result.dlt645Connection.version = configured->industrial().dlt645_version() == 1997
            ? collector::dlt645::Version::V1997
            : collector::dlt645::Version::V2007;
        result.dlt645Connection.wakeupBytes = static_cast<std::uint8_t>(configured->industrial().dlt645_wakeup_bytes());
        for (const auto& item : items) {
            collector::ElementDefinition element;
            const auto common = [&]<class Config>(const Config& config) {
                if (config.device_id() != id) {
                    return false;
                }
                element.id = config.element_id();
                element.name = config.name();
                element.unit = config.unit();
                return true;
            };
            if (item.has_modbus_register()) {
                const auto& config = item.modbus_register();
                if (!common(config)) {
                    continue;
                }
                element.dataType = config.data_type();
                element.byteOrder = config.byte_order();
                element.registerType = config.register_type();
                element.address = config.address();
                element.quantity = config.quantity();
                element.decimals = config.decimals();
            } else if (item.has_s7_area()) {
                const auto& config = item.s7_area();
                if (!common(config)) {
                    continue;
                }
                element.dataType = config.data_type();
                element.area = config.area();
                element.dbNumber = config.db_number();
                element.start = config.start();
                element.startBit = config.start_bit();
                element.size = config.size();
                element.decimals = config.decimals();
            } else if (item.has_industrial_point()) {
                const auto& config = item.industrial_point();
                if (!common(config)) {
                    continue;
                }
                element.dataType = config.data_type();
                element.byteOrder = config.byte_order();
                element.area = config.area();
                element.address = config.address();
                element.startBit = config.bit();
                element.decimals = config.decimals();
                element.guideHex = config.identifier();
                element.length = config.length();
                element.digits = config.digits();
            } else if (item.has_sl651_element()) {
                const auto& config = item.sl651_element();
                if (!common(config)) {
                    continue;
                }
                element.functionCode = config.function_code();
                element.encoding = config.encoding();
                element.length = config.length();
                element.digits = config.digits();
                element.guideHex = utils::hexEncode(reinterpret_cast<const unsigned char*>(config.guide().data()), config.guide().size());
                element.responseElement = config.response_element();
                element.positionMode = config.fixed_position() ? "OFFSET" : "GUIDE";
                element.byteOffset = config.byte_offset();
                for (const auto& function : items) {
                    if (function.has_sl651_function() && function.sl651_function().device_id() == id &&
                        function.sl651_function().function_code() == element.functionCode) {
                        element.direction = function.sl651_function().direction();
                    }
                }
            } else {
                continue;
            }
            result.elements.push_back(std::move(element));
        }
        return result;
    }

    static message::ParsedDeviceMessage decode(const pb::TelemetryRecord& record, const collector::DeviceDefinition& device) {
        if (record.raw_requests_size() == 0 || record.raw_requests_size() != record.raw_payloads_size() ||
            record.values_size() != 0 || protocol::TelemetryValues::protocolName(record.protocol()) != device.protocol) {
            throw std::invalid_argument("invalid edge acquisition exchange array");
        }
        for (int index = 0; index < record.raw_requests_size(); ++index) {
            if (record.raw_requests(index).size() > 4096 || record.raw_payloads(index).empty() ||
                record.raw_payloads(index).size() > 4112) {
                throw std::invalid_argument("invalid edge acquisition frame size");
            }
        }
        message::ParsedDeviceMessage result;
        result.observedAtMs = record.observed_at_ms();
        std::map<std::string, std::string> values;
        const auto merge = [&](std::string_view json) {
            const auto root = ruvia::json_value::parse(json);
            const auto points = root.get<ruvia::json_value>("values");
            if (!points || !points->is_object()) {
                throw std::invalid_argument("invalid decoded acquisition values");
            }
            (void)points->for_each_field([&](std::string_view id, const ruvia::json_value& value) {
                values[std::string(id)] = std::string(value.view());
                return true;
            });
        };
        if (device.protocol == "SL651") {
            collector::LinkDefinition link;
            link.id = device.linkId;
            collector::sl651::Session session(link, result.connectionId, std::make_shared<collector::RuntimeSnapshot>(), { &device });
            bool complete = false;
            for (int index = 0; index < record.raw_payloads_size(); ++index) {
                const auto response = bytes(record.raw_payloads(index));
                const auto request = bytes(record.raw_requests(index));
                if (response.size() < 17 || response[0] != 0x7e || response[1] != 0x7e ||
                    (response[11] & 0x80U) || response.size() != 17U + (be16(response, 11) & 0x0fffU) ||
                    collector::sl651::detail::crc16Modbus(response.first(response.size() - 2)) != be16(response, response.size() - 2)) {
                    throw std::invalid_argument("invalid SL651 business response");
                }
                if (!request.empty() && (request.size() < 17 || response.size() < 17 || request[10] != response[10] || (request[11] & 0x80U) == 0 || collector::sl651::detail::crc16Modbus(request.first(request.size() - 2)) != be16(request, request.size() - 2))) {
                    throw std::invalid_argument("invalid SL651 business request");
                }
                collector::ProtocolInput input;
                const auto identity = service::common::uuidText(record.record_id()) + ':' + std::to_string(index);
                if (!request.empty()) {
                    session.restoreAcquisitionRequest(request, identity);
                }
                input.messageId = identity;
                input.receivedAtMs = record.observed_at_ms();
                input.bytes = response;
                for (const auto& action : session.consume(input)) {
                    if (action.kind == collector::ProtocolActionKind::PublishParsed) {
                        merge(action.parsed.valuesJson);
                        result.observedAtMs = action.parsed.observedAtMs;
                        complete = true;
                    }
                }
            }
            if (!complete) {
                throw std::invalid_argument("incomplete SL651 acquisition");
            }
        } else if (device.protocol == "DLT645") {
            for (int index = 0; index < record.raw_requests_size();) {
                const auto request = collector::dlt645::FrameCodec::parse(meterFrame(bytes(record.raw_requests(index))));
                const collector::ElementDefinition* point = nullptr;
                for (const auto& element : device.elements) {
                    const auto identifier = collector::dlt645::FrameCodec::identifier(element.guideHex, device.dlt645Connection.version);
                    if (request.data == identifier) {
                        point = &element;
                        break;
                    }
                }
                if (!point) {
                    throw std::invalid_argument("unconfigured meter acquisition identifier");
                }
                collector::dlt645::Exchange exchange(device);
                auto expected = exchange.read(*point);
                for (;;) {
                    if (index == record.raw_requests_size() || !sameMeterRequest(expected, bytes(record.raw_requests(index)))) {
                        throw std::invalid_argument("meter acquisition continuation mismatch");
                    }
                    auto reply = exchange.response(*point, false, meterFrame(bytes(record.raw_payloads(index++))));
                    if (!reply.error.empty()) {
                        throw std::invalid_argument("abnormal meter acquisition");
                    }
                    if (reply.nextRequest.empty()) {
                        for (const auto& alias : device.elements) {
                            if (alias.guideHex == point->guideHex) {
                                values[alias.id] = pointJson(alias, exchange.decode(alias, reply.data));
                            }
                        }
                        break;
                    }
                    expected = std::move(reply.nextRequest);
                }
            }
        } else {
            for (int index = 0; index < record.raw_requests_size(); ++index) {
                const auto request = bytes(record.raw_requests(index)), response = bytes(record.raw_payloads(index));
                if (device.protocol == "Modbus") {
                    merge(collector::modbus::Session::decodeRead(device, request, response));
                } else if (device.protocol == "S7") {
                    merge(collector::s7::Session::decodeRead(device, request, response));
                } else if (device.protocol == "MC") {
                    decodeMc(device, request, response, values);
                } else if (device.protocol == "FINS") {
                    decodeFins(device, request, response, values);
                } else {
                    throw std::invalid_argument("unsupported edge acquisition protocol");
                }
            }
        }
        result.valuesJson = "{\"function_code\":" + utils::jsonQuoted(record.function_code()) +
            ",\"direction\":\"UP\",\"values\":{";
        for (const auto& [id, value] : values) {
            if (result.valuesJson.back() != '{') {
                result.valuesJson += ',';
            }
            result.valuesJson += utils::jsonQuoted(id) + ':' + value;
        }
        result.valuesJson += "}}";
        return result;
    }

  private:
    static std::span<const std::uint8_t> bytes(std::string_view wire) {
        return { reinterpret_cast<const std::uint8_t*>(wire.data()), wire.size() };
    }

    static std::uint16_t be16(std::span<const std::uint8_t> wire, std::size_t offset) {
        return static_cast<std::uint16_t>((wire[offset] << 8U) | wire[offset + 1]);
    }

    static std::uint16_t le16(std::span<const std::uint8_t> wire, std::size_t offset) {
        return static_cast<std::uint16_t>((wire[offset + 1] << 8U) | wire[offset]);
    }

    static std::string pointJson(const collector::ElementDefinition& point, std::string_view value) {
        return "{\"name\":" + utils::jsonQuoted(point.name) + ",\"unit\":" + utils::jsonQuoted(point.unit) +
            ",\"value\":" + std::string(value) + '}';
    }

    static bool sameMeterRequest(std::span<const std::uint8_t> left, std::span<const std::uint8_t> right) {
        left = meterFrame(left);
        right = meterFrame(right);
        return std::equal(left.begin(), left.end(), right.begin(), right.end());
    }

    static std::span<const std::uint8_t> meterFrame(std::span<const std::uint8_t> frame) {
        while (!frame.empty() && frame[0] == 0xfe) {
            frame = frame.subspan(1);
        }
        return frame;
    }

    static void decodeMc(const collector::DeviceDefinition& device, std::span<const std::uint8_t> request, std::span<const std::uint8_t> response, std::map<std::string, std::string>& values) {
        if (request.size() != 21 && request.size() != 25) {
            throw std::invalid_argument("invalid MC read request");
        }
        const bool four = request[0] == 0x54;
        const auto offset = four ? 6U : 2U;
        collector::mc::Connection connection;
        connection.frame = four ? collector::mc::FrameFormat::Binary4E : collector::mc::FrameFormat::Binary3E;
        connection.network = request[offset];
        connection.station = request[offset + 1];
        connection.moduleIo = le16(request, offset + 2);
        connection.multidrop = request[offset + 4];
        connection.monitoringTimer = le16(request, offset + 7);
        collector::mc::Address address{ request[offset + 16],
                                        static_cast<std::uint32_t>(request[offset + 13] | request[offset + 14] << 8 | request[offset + 15] << 16),
                                        le16(request, offset + 17),
                                        le16(request, offset + 11) == 1 };
        const auto serial = four ? le16(request, 2) : 0;
        if (collector::mc::FrameCodec::read(connection, address, serial) != std::vector<std::uint8_t>(request.begin(), request.end())) {
            throw std::invalid_argument("invalid MC business request");
        }
        const auto decoded = collector::mc::FrameCodec::response(connection, address, serial, false, response);
        if (decoded.endCode) {
            throw std::invalid_argument("abnormal MC acquisition");
        }
        collector::mc::Exchange exchange(device);
        for (const auto& point : device.elements) {
            const auto code = collector::mc::FrameCodec::deviceCode(point.area);
            const bool bits = point.dataType == "BOOL";
            const auto stride = collector::mc::FrameCodec::bitDevice(address.deviceCode) && !bits ? 16U : 1U;
            if (!code || *code != address.deviceCode || bits != address.bitAccess || point.address < address.number ||
                (point.address - address.number) % stride != 0) {
                continue;
            }
            const auto offsetBytes = static_cast<std::size_t>((point.address - address.number) / stride) * (bits ? 1 : 2);
            const auto width = collector::register_value::width(point.dataType);
            if (offsetBytes + width <= decoded.data.size()) {
                values[point.id] = pointJson(point, exchange.decode(point, std::span(decoded.data).subspan(offsetBytes, width)));
            }
        }
    }

    static void decodeFins(const collector::DeviceDefinition& device, std::span<const std::uint8_t> request, std::span<const std::uint8_t> response, std::map<std::string, std::string>& values) {
        if (request.size() != 34) {
            throw std::invalid_argument("invalid FINS read request");
        }
        collector::fins::Connection connection{ request[19], request[20], request[21], request[22], request[23], request[24] };
        const auto area = request[28];
        const bool bits = area == 0x02 || (area >= 0x30 && area <= 0x33);
        collector::fins::Address address{ area, be16(request, 29), request[31], be16(request, 32), bits };
        if (collector::fins::FrameCodec::read(connection, address, request[25]) != std::vector<std::uint8_t>(request.begin(), request.end())) {
            throw std::invalid_argument("invalid FINS business request");
        }
        const auto decoded = collector::fins::FrameCodec::response(connection, address, request[25], false, response);
        if (decoded.endCode) {
            throw std::invalid_argument("abnormal FINS acquisition");
        }
        collector::fins::Exchange exchange(device);
        for (const auto& point : device.elements) {
            const auto code = collector::fins::FrameCodec::memoryArea(point.area, bits);
            if (!code || *code != area || (point.dataType == "BOOL") != bits) {
                continue;
            }
            const auto relative = bits ? point.address * 16 + point.startBit - address.word * 16 - address.bit : point.address - address.word;
            if (relative < 0) {
                continue;
            }
            const auto offset = static_cast<std::size_t>(relative) * (bits ? 1 : 2);
            const auto width = collector::register_value::width(point.dataType);
            if (offset + width <= decoded.data.size()) {
                values[point.id] = pointJson(point, exchange.decode(point, std::span(decoded.data).subspan(offset, width)));
            }
        }
    }
};
} // namespace service::edge::acquisition
