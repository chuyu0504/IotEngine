#include <array>
#include <cstdio>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "service/modules/protocol/protocol.service.h"

namespace {

using service::protocol::ProtocolService;

constexpr std::string_view kPhysical = "00000000-0000-4000-8000-000000000001";
constexpr std::string_view kDerived = "00000000-0000-4000-8000-000000000002";
constexpr std::string_view kPoint = "00000000-0000-7000-8000-000000000003";

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

// 返回 16004/400 拒绝文案；接受时返回 std::nullopt。
std::optional<std::string> outcome(const std::function<void()>& action) {
    try {
        action();
    } catch (const ruvia::http_error& error) {
        const auto info = error.info();
        require(info.code() == "16004" && info.status().value() == 400, "rejection used the wrong error code: " + std::string(info.code()) + " " + std::string(info.message()));
        return std::string(info.message());
    }
    return std::nullopt;
}

// 返回的 json_value 借用 text，调用方须保证原文存活；临时字符串直接拒绝。
ruvia::json_value json(std::string&&) = delete;

ruvia::json_value json(const std::string& text) {
    auto value = ruvia::json_value::parse(std::string_view(text));
    return value;
}

struct ConfigCase {
    std::string protocol;
    std::string config;
    bool complete;
    std::optional<std::string> expected;
};

void check(const ConfigCase& item) {
    const auto text = std::string(item.config);
    const auto config = json(text);
    const auto actual = outcome([&] {
        ProtocolService::validateConfig(item.protocol, config, item.complete);
    });
    require(actual == item.expected, item.protocol + (item.complete ? " create " : " patch ") + text + " => " + actual.value_or("accepted") + ", expected " + item.expected.value_or("accepted"));
}

std::string replaced(std::string text, std::string_view from, std::string_view to) {
    const auto position = text.find(from);
    require(position != std::string::npos, "fixture token missing: " + std::string(from));
    return text.replace(position, from.size(), to);
}

const std::optional<std::string> kAccepted;

// ---- 公共字段 ----
void testCommonFields() {
    const std::string base = R"({"storagePolicy":"report","responseMode":"M3","funcs":[]})";
    const std::string conversion = "原生点位不支持倍率和偏移，请使用派生值表达式";
    for (const auto& item : std::vector<ConfigCase>{
             { "SL651", base, true, kAccepted },
             { "SL651", "[]", true, "config 必须是对象" },
             { "SL651", R"({"registers":[{"scale":2}]})", false, conversion },
             { "SL651", R"({"areas":[{"offset":null}]})", false, conversion },
             { "SL651", R"({"points":[1,{"offset":1}]})", false, conversion },
             { "SL651", R"({"registers":5,"points":[1]})", false, kAccepted },
             { "SL651", R"({"storageInterval":5})", false, "配置不允许 storageInterval，请使用 storagePolicy" },
             { "SL651", R"({"storageInterval":null})", false, "配置不允许 storageInterval，请使用 storagePolicy" },
             { "SL651", R"({"storagePolicy":"daily"})", false, "配置的 storagePolicy 无效" },
             { "SL651", R"({"storagePolicy":5})", false, "配置的 storagePolicy 无效" },
             { "SL651", R"({"storagePolicy":null})", false, "配置的 storagePolicy 无效" },
             { "SL651", R"({"responseMode":"M3","funcs":[]})", true, "配置的 storagePolicy 不能为空" },
             { "SL651", R"({"responseMode":"M3"})", false, kAccepted },
             { "SL651", R"({"deviceParameters":[]})", false, "设备参数请在设备管理中配置" },
             { "SL651", R"({"deviceParameters":null})", false, "设备参数请在设备管理中配置" },
             { "SL651", R"({"parameters":"x"})", false, "设备参数定义须为数组" },
             { "SL651", R"({"parameters":[{"name":"a"}]})", false, "设备参数定义须为数组" },
             { "SL651", R"({"parameters":[{"name":"a","dataType":"INT"}]})", false, "设备参数类型无效" },
             { "SL651", R"({"parameters":[{"name":" a","dataType":"STRING"}]})", false, "设备参数名须唯一且为 1–100 字节，不能有首尾空格" },
             { "SL651", R"({"parameters":[{"name":"a","dataType":"STRING"}],"unknownField":{"kept":true}})", false, kAccepted },
             // 顶层重复键以最后一次出现为准；parameters 仍按整体公共契约解析，重复即无效。
             { "SL651", R"({"storagePolicy":"daily","storagePolicy":"report"})", false, kAccepted },
             { "SL651", R"({"storagePolicy":"report","storagePolicy":"daily"})", false, "配置的 storagePolicy 无效" },
             { "SL651", R"({"parameters":[],"parameters":[]})", false, "设备参数定义须为数组" },
         }) {
        check(item);
    }
}

// ---- SL651 ----
void testSl651() {
    const std::string guide = R"({"guideHex":"2019","encode":"BCD","length":3,"digits":1})";
    const std::string offset = R"({"positionMode":"OFFSET","encode":"BCD","digits":0,"length":1,"byteOffset":0})";
    const auto config = [](std::string_view function) {
        return R"({"storagePolicy":"report","responseMode":"M3","funcs":[)" + std::string(function) + "]}";
    };
    const auto withElement = [&](std::string_view element) {
        return config(R"({"funcCode":"E1","dir":"UP","elements":[)" + std::string(element) + "]}");
    };
    const std::string invalid = "SL651 配置无效";
    const std::string definition = "SL651 引导符、长度或小数位不符合数据定义";
    for (const auto& item : std::vector<ConfigCase>{
             { "SL651", withElement(guide), true, kAccepted },
             { "SL651", withElement(offset), true, kAccepted },
             { "SL651", R"({"storagePolicy":"report","funcs":[]})", true, invalid },
             { "SL651", R"({"storagePolicy":"report","responseMode":"M1"})", true, invalid },
             { "SL651", R"({"responseMode":"M9"})", false, invalid },
             { "SL651", R"({"responseMode":3})", false, invalid },
             { "SL651", R"({"funcs":{}})", false, invalid },
             { "SL651", R"({"funcs":[{"funcCode":5}]})", false, invalid },
             { "SL651", config(R"({"dir":"UP"})"), true, definition },
             { "SL651", config(R"({"funcCode":"E"})"), true, definition },
             { "SL651", config(R"({"funcCode":"G1"})"), true, definition },
             { "SL651", config(R"({"funcCode":"E1","dir":"LEFT"})"), true, definition },
             { "SL651", withElement(replaced(guide, R"("encode":"BCD")", R"("encode":"BIN")")), true, definition },
             { "SL651", withElement(replaced(guide, R"("encode":"BCD",)", "")), true, definition },
             { "SL651", withElement(replaced(guide, R"(,"digits":1)", "")), true, definition },
             { "SL651", withElement(replaced(guide, R"("digits":1)", R"("digits":8)")), true, definition },
             { "SL651", withElement(replaced(guide, R"("length":3)", R"("length":"3")")), true, definition },
             { "SL651", withElement(replaced(guide, R"("length":3)", R"("length":4)")), true, definition },
             { "SL651", withElement(replaced(guide, "2019", "ZZ19")), true, definition },
             { "SL651", withElement(replaced(guide, "2019", "FF19")), true, definition },
             { "SL651", withElement(R"({"guideHex":"F0F0","encode":"HEX","length":5,"digits":0})"), true, kAccepted },
             { "SL651", withElement(R"({"guideHex":"F0F0","encode":"HEX","length":6,"digits":0})"), true, definition },
             { "SL651", withElement(R"({"positionMode":"RELATIVE","encode":"BCD","digits":0,"length":1})"), true, definition },
             { "SL651", withElement(replaced(offset, R"("byteOffset":0)", R"("byteOffset":1e3)")), true, kAccepted },
             { "SL651", withElement(replaced(offset, R"("byteOffset":0)", R"("byteOffset":8388607)")), true, kAccepted },
             { "SL651", withElement(replaced(replaced(offset, R"("byteOffset":0)", R"("byteOffset":8388607)"), R"("length":1)", R"("length":2)")), true, definition },
             { "SL651", withElement(replaced(offset, R"("byteOffset":0)", R"("byteOffset":-1)")), true, definition },
             { "SL651", withElement(replaced(offset, R"("byteOffset":0)", R"("byteOffset":0.5)")), true, definition },
             { "SL651", withElement(replaced(offset, R"("byteOffset":0)", R"("byteOffset":"1")")), true, definition },
             { "SL651", withElement(replaced(offset, R"("byteOffset":0)", R"("byteOffset":1e100)")), true, definition },
             { "SL651", withElement(replaced(offset, R"("byteOffset":0)", R"("byteOffset":null)")), true, invalid },
             { "SL651", withElement(replaced(offset, R"("length":1)", R"("length":0)")), true, definition },
             { "SL651", withElement(replaced(offset, R"("length":1)", R"("length":1.5)")), true, definition },
             { "SL651", withElement(replaced(offset, R"("encode":"BCD",)", "")), true, definition },
         }) {
        check(item);
    }
}

// ---- Modbus ----
void testModbus() {
    const std::string registerJson = R"({"id":"r1","name":"t","registerType":"HOLDING_REGISTER","dataType":"UINT16","address":0,"quantity":1})";
    const auto config = [](std::string_view extra, std::string_view entry) {
        return R"({"storagePolicy":"report","byteOrder":"BIG_ENDIAN",)" + std::string(extra) + R"("registers":[)" + std::string(entry) + "]}";
    };
    const auto withRegister = [&](std::string_view from, std::string_view to) {
        return config("", replaced(registerJson, from, to));
    };
    const std::string registerInvalid = "Modbus 寄存器配置无效";
    for (const auto& item : std::vector<ConfigCase>{
             { "Modbus", config("", registerJson), true, kAccepted },
             { "Modbus", R"({"readInterval":5})", false, kAccepted },
             { "Modbus", R"({"storagePolicy":"report","registers":[]})", true, "Modbus 配置的 byteOrder 无效" },
             { "Modbus", R"({"byteOrder":"MIDDLE"})", false, "Modbus 配置的 byteOrder 无效" },
             { "Modbus", R"({"byteOrder":1})", false, "Modbus 配置的 byteOrder 无效" },
             { "Modbus", R"({"storagePolicy":"report","byteOrder":"BIG_ENDIAN"})", true, "Modbus 配置的 registers 必须是数组" },
             { "Modbus", R"({"registers":{}})", false, "Modbus 配置的 registers 必须是数组" },
             { "Modbus", R"({"registers":null})", false, "Modbus 配置的 registers 必须是数组" },
             { "Modbus", R"({"registers":[{"name":5}]})", false, "Modbus 配置的 registers 必须是数组" },
             { "Modbus", R"({"registers":[{"writable":"yes"}]})", false, "Modbus 配置的 registers 必须是数组" },
             { "Modbus", R"({"packet":[]})", false, "Modbus 配置的 packet 必须是对象" },
             { "Modbus", R"({"packet":{"mergeGap":null}})", false, "Modbus 配置的 packet 必须是对象" },
             { "Modbus", R"({"readInterval":0})", false, "Modbus 配置的 readInterval 无效" },
             { "Modbus", R"({"readInterval":3601})", false, "Modbus 配置的 readInterval 无效" },
             { "Modbus", R"({"readInterval":1.5})", false, "Modbus 配置的 readInterval 无效" },
             { "Modbus", R"({"readInterval":null})", false, "Modbus 配置的 readInterval 无效" },
             { "Modbus", R"({"readInterval":"1e1"})", false, "Modbus 配置的 readInterval 无效" },
             { "Modbus", R"({"readInterval":"10"})", false, kAccepted },
             { "Modbus", R"({"readInterval":1e1})", false, kAccepted },
             { "Modbus", R"({"readInterval":36e2})", false, kAccepted },
             { "Modbus", R"({"readInterval":0,"readInterval":5})", false, kAccepted },
             { "Modbus", R"({"readInterval":5,"readInterval":0})", false, "Modbus 配置的 readInterval 无效" },
             { "Modbus", R"({"packet":{"mergeGap":2001}})", false, "Modbus 配置的 packet.mergeGap 无效" },
             { "Modbus", R"({"packet":{"mergeGap":"5","maxQuantity":125}})", false, kAccepted },
             { "Modbus", R"({"packet":{"maxQuantity":126}})", false, "Modbus 配置的 packet.maxQuantity 无效" },
             { "Modbus", R"({"packet":{"maxQuantity":0}})", false, "Modbus 配置的 packet.maxQuantity 无效" },
             { "Modbus", R"({"pollInterval":1})", false, "Modbus 配置不允许 pollInterval，请使用 readInterval" },
             { "Modbus", R"({"pollInterval":null})", false, "Modbus 配置不允许 pollInterval，请使用 readInterval" },
             { "Modbus", withRegister(R"("id":"r1")", R"("id":"")"), true, registerInvalid },
             { "Modbus", withRegister(R"("id":"r1",)", ""), true, registerInvalid },
             { "Modbus", withRegister(R"("name":"t",)", ""), true, registerInvalid },
             { "Modbus", withRegister("HOLDING_REGISTER", "MEMORY"), true, registerInvalid },
             { "Modbus", withRegister("UINT16", "STRING"), true, registerInvalid },
             { "Modbus", withRegister(R"("address":0)", R"("address":65536)"), true, registerInvalid },
             { "Modbus", withRegister(R"("address":0)", R"("address":"0")"), true, registerInvalid },
             { "Modbus", withRegister(R"(,"quantity":1)", ""), true, registerInvalid },
             { "Modbus", withRegister(R"("quantity":1)", R"("quantity":5)"), true, registerInvalid },
             { "Modbus", withRegister(R"("quantity":1)", R"("quantity":1,"byteOrder":"MIDDLE")"), true, "Modbus 寄存器 byteOrder 无效" },
             { "Modbus", withRegister(R"("quantity":1)", R"("quantity":1,"decimals":9)"), true, "Modbus 寄存器 decimals 无效" },
             { "Modbus", withRegister(R"("quantity":1)", R"("quantity":1,"decimals":"-1")"), true, kAccepted },
             { "Modbus", withRegister(R"("quantity":1)", R"("quantity":1,"decimals":-2)"), true, "Modbus 寄存器 decimals 无效" },
         }) {
        check(item);
    }
}

// ---- S7 ----
void testS7() {
    const std::string area = R"({"id":"a1","name":"t","area":"DB","dbNumber":1,"start":0,"size":1})";
    const auto config = [](std::string_view extra, std::string_view entry) {
        return R"({"storagePolicy":"report","plcModel":"S7-1200",)" + std::string(extra) + R"("connection":{},"areas":[)" + std::string(entry) + "]}";
    };
    const auto withArea = [&](std::string_view from, std::string_view to) {
        return config("", replaced(area, from, to));
    };
    const std::string invalid = "S7 配置无效";
    const std::string areaInvalid = "S7 寄存器配置无效";
    for (const auto& item : std::vector<ConfigCase>{
             { "S7", config("", area), true, kAccepted },
             { "S7", R"({"readInterval":"5"})", false, kAccepted },
             { "S7", R"({"storagePolicy":"report","connection":{},"areas":[]})", true, invalid },
             { "S7", R"({"storagePolicy":"report","plcModel":"S7-300","areas":[]})", true, invalid },
             { "S7", R"({"storagePolicy":"report","plcModel":"S7-300","connection":{}})", true, invalid },
             { "S7", R"({"plcModel":"S7-999"})", false, invalid },
             { "S7", R"({"connection":"tcp"})", false, invalid },
             { "S7", R"({"areas":{}})", false, invalid },
             { "S7", R"({"connection":{"probeMode":"FAST"}})", false, invalid },
             { "S7", R"({"connection":{"handshakeTimeout":999}})", false, invalid },
             { "S7", R"({"connection":{"handshakeTimeout":30000.0000000000000001}})", false, invalid },
             { "S7", R"({"connection":{"handshakeTimeout":1000.5,"directProbeTimeout":3e4}})", false, kAccepted },
             { "S7", R"({"connection":{"directProbeTimeout":"2000"}})", false, invalid },
             { "S7", R"({"pollInterval":5})", false, "S7 配置不允许 pollInterval，请使用 readInterval" },
             { "S7", R"({"pollInterval":null})", false, "S7 配置不允许 pollInterval，请使用 readInterval" },
             { "S7", R"({"readInterval":0})", false, "S7 配置的 readInterval 无效" },
             { "S7", R"({"readInterval":null})", false, "S7 配置的 readInterval 无效" },
             { "S7", withArea(R"("id":"a1")", R"("id":"")"), true, areaInvalid },
             { "S7", withArea(R"("name":"t",)", ""), true, areaInvalid },
             { "S7", withArea(R"("area":"DB")", R"("area":"XX")"), true, areaInvalid },
             { "S7", withArea(R"("size":1)", R"("size":1,"dataType":"CHAR")"), true, areaInvalid },
             { "S7", withArea(R"("start":0,)", ""), true, areaInvalid },
             { "S7", withArea(R"("start":0)", R"("start":-1)"), true, areaInvalid },
             { "S7", withArea(R"("start":0)", R"("start":2147483648)"), true, areaInvalid },
             { "S7", withArea(R"("size":1)", R"("size":0)"), true, areaInvalid },
             { "S7", withArea(R"("size":1)", R"("size":1,"startBit":8)"), true, areaInvalid },
             { "S7", withArea(R"("size":1)", R"("size":1,"decimals":9)"), true, areaInvalid },
             { "S7", withArea(R"("dbNumber":1,)", ""), true, areaInvalid },
             { "S7", withArea(R"("dbNumber":1)", R"("dbNumber":0)"), true, areaInvalid },
             { "S7", config("", R"({"id":"a1","name":"t","area":"V","start":0,"size":1})"), true, kAccepted },
             { "S7", R"({"areas":[{"id":5}]})", false, invalid },
         }) {
        check(item);
    }
}

// ---- MC / FINS / DLT645 ----
std::string industrial(std::string_view connection, std::string_view point, std::string_view extra = {}) {
    return R"({"storagePolicy":"report",)" + std::string(extra) + R"("connection":)" + std::string(connection) + R"(,"points":[)" + std::string(point) + "]}";
}

std::string uuidPoint(std::string_view fields) {
    return R"({"id":")" + std::string(kPoint) + R"(","name":"p",)" + std::string(fields) + "}";
}

void testMc() {
    const std::string point = uuidPoint(R"("dataType":"INT16","area":"D","address":0)");
    const auto withPoint = [&](std::string_view from, std::string_view to) {
        return industrial("{}", replaced(point, from, to));
    };
    const std::string invalid = "协议连接、点位或采集周期配置无效";
    const std::string connection = "MC/SLMP 连接参数无效";
    const std::string pointInvalid = "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证";
    std::string many;
    for (int index = 0; index < 257; ++index) {
        char id[37];
        std::snprintf(id, sizeof(id), "00000000-0000-7000-8000-%012d", index);
        many += std::string(index ? "," : "") + R"({"id":")" + id + R"(","name":"p","dataType":"INT16","area":"D","address":0})";
    }
    for (const auto& item : std::vector<ConfigCase>{
             { "MC", industrial(R"({"frame":"3E"})", point, R"("readInterval":10,)"), true, kAccepted },
             { "MC", R"({"storagePolicy":"report","points":[]})", true, invalid },
             { "MC", R"({"readInterval":5})", false, invalid },
             { "MC", R"({"storagePolicy":"report","connection":{}})", true, invalid },
             { "MC", R"({"connection":"tcp","points":[]})", false, invalid },
             { "MC", R"({"connection":{},"points":{}})", false, invalid },
             { "MC", industrial("{}", point, R"("readInterval":0,)"), true, invalid },
             { "MC", industrial("{}", point, R"("readInterval":"10",)"), true, kAccepted },
             { "MC", industrial("{}", point, R"("readInterval":1e1,)"), true, kAccepted },
             { "MC", industrial("{}", point, R"("readInterval":"1e1",)"), true, invalid },
             { "MC", industrial("{}", point, R"("commandFastReadDuration":3601,)"), true, invalid },
             { "MC", industrial("{}", point, R"("commandFastReadDuration":0,)"), true, kAccepted },
             { "MC", industrial("{}", point, R"("commandFastReadInterval":0,)"), true, invalid },
             { "MC", industrial(R"({"frame":"5E"})", point), true, connection },
             { "MC", industrial(R"({"network":256})", point), true, connection },
             { "MC", industrial(R"({"network":"1"})", point), true, connection },
             { "MC", industrial(R"({"moduleIo":65536})", point), true, connection },
             { "MC", industrial(R"({"monitoringTimer":0})", point), true, connection },
             { "MC", industrial(R"({"network":null})", point), true, invalid },
             { "MC", withPoint(kPoint, "not-a-uuid"), true, pointInvalid },
             { "MC", industrial("{}", point + "," + point), true, pointInvalid },
             { "MC", withPoint(R"("name":"p")", R"("name":"")"), true, pointInvalid },
             { "MC", withPoint(R"("dataType":"INT16",)", ""), true, pointInvalid },
             { "MC", withPoint("INT16", "STRING"), true, pointInvalid },
             { "MC", withPoint(R"("area":"D")", R"("area":"D","unit":"123456789012345678901234567890123")"), true, pointInvalid },
             { "MC", withPoint(R"("area":"D",)", ""), true, pointInvalid },
             { "MC", withPoint(R"("area":"D")", R"("area":"CIO")"), true, pointInvalid },
             { "MC", withPoint(R"("area":"D")", R"("area":"D","byteOrder":"MIDDLE")"), true, pointInvalid },
             { "MC", withPoint(R"("area":"D")", R"("area":"D","decimals":9)"), true, pointInvalid },
             { "MC", withPoint(R"(,"address":0)", ""), true, pointInvalid },
             { "MC", withPoint(R"("address":0)", R"("address":16777215)"), true, kAccepted },
             { "MC", industrial("{}", uuidPoint(R"("dataType":"INT32","area":"D","address":16777215)")), true, pointInvalid },
             { "MC", industrial("{}", uuidPoint(R"("dataType":"BOOL","area":"D","address":0)")), true, pointInvalid },
             { "MC", industrial("{}", uuidPoint(R"("dataType":"INT16","area":"M","address":16777200)")), true, kAccepted },
             { "MC", industrial("{}", uuidPoint(R"("dataType":"INT16","area":"M","address":16777201)")), true, pointInvalid },
             { "MC", withPoint(R"("address":0)", R"("address":0,"writable":"yes")"), true, invalid },
             { "MC", industrial("{}", many), true, pointInvalid },
         }) {
        check(item);
    }
}

void testFins() {
    const std::string point = uuidPoint(R"("dataType":"INT16","area":"D","address":0)");
    const std::string route = "FINS 路由参数无效";
    const std::string pointInvalid = "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证";
    for (const auto& item : std::vector<ConfigCase>{
             { "FINS", industrial(R"({"destinationNetwork":127,"sourceNode":254,"sourceUnit":255})", point), true, kAccepted },
             { "FINS", industrial(R"({"destinationNetwork":128})", point), true, route },
             { "FINS", industrial(R"({"sourceNode":255})", point), true, route },
             { "FINS", industrial(R"({"destinationUnit":256})", point), true, route },
             { "FINS", industrial("{}", uuidPoint(R"("dataType":"INT16","area":"M","address":0)")), true, pointInvalid },
             { "FINS", industrial("{}", uuidPoint(R"("dataType":"INT16","area":"D","address":65536)")), true, pointInvalid },
             { "FINS", industrial("{}", uuidPoint(R"("dataType":"INT32","area":"D","address":65535)")), true, pointInvalid },
             { "FINS", industrial("{}", uuidPoint(R"("dataType":"INT16","area":"D","address":0,"bit":1)")), true, pointInvalid },
             { "FINS", industrial("{}", uuidPoint(R"("dataType":"BOOL","area":"CIO","address":65535,"bit":15)")), true, kAccepted },
             { "FINS", industrial("{}", uuidPoint(R"("dataType":"BOOL","area":"CIO","address":0,"bit":16)")), true, pointInvalid },
             { "FINS", industrial("{}", uuidPoint(R"("dataType":"INT16","area":"D","address":0,"decimals":-2)")), true, pointInvalid },
             // 其他工业协议字段保持类型约束，但不按本协议取值规则校验。
             { "FINS", industrial(R"({"frame":5})", point), true, "协议连接、点位或采集周期配置无效" },
             { "FINS", industrial(R"({"frame":"5E","version":"2010"})", point), true, kAccepted },
             { "FINS", industrial("{}", uuidPoint(R"("dataType":"INT16","area":"D","address":0,"identifier":5)")), true, "协议连接、点位或采集周期配置无效" },
             { "MC", industrial(R"({"version":5})", uuidPoint(R"("dataType":"INT16","area":"D","address":0)")), true, "协议连接、点位或采集周期配置无效" },
             { "MC", industrial(R"({"destinationNetwork":null})", uuidPoint(R"("dataType":"INT16","area":"D","address":0)")), true, "协议连接、点位或采集周期配置无效" },
             { "MC", industrial("{}", uuidPoint(R"("dataType":"INT16","area":"D","address":0,"length":null)")), true, "协议连接、点位或采集周期配置无效" },
         }) {
        check(item);
    }
}

void testDlt645() {
    const std::string point = uuidPoint(R"("dataType":"BCD","identifier":"00000000","length":4,"digits":2)");
    const auto withPoint = [&](std::string_view connection, std::string_view from, std::string_view to) {
        return industrial(connection, replaced(point, from, to));
    };
    const std::string connection = "DL/T645 版本或唤醒字节数无效";
    const std::string authentication = "DL/T645 写入认证字段必须为空或 8 位十六进制";
    const std::string pointInvalid = "协议点位配置无效，请检查地址、类型、长度、唯一标识及写入认证";
    const std::string writable = R"({"writePassword":"00000000","operatorCode":"00000000"})";
    for (const auto& item : std::vector<ConfigCase>{
             { "DLT645", industrial(R"({"version":"2007","wakeupBytes":4})", point, R"("readInterval":30,)"), true, kAccepted },
             { "DLT645", industrial(R"({"version":"2010"})", point), true, connection },
             { "DLT645", industrial(R"({"wakeupBytes":5})", point), true, connection },
             { "DLT645", industrial(R"({"writePassword":"123"})", point), true, authentication },
             { "DLT645", industrial(R"({"operatorCode":"ZZZZZZZZ"})", point), true, authentication },
             { "DLT645", industrial(R"({"writePassword":"","operatorCode":""})", point), true, kAccepted },
             { "DLT645", industrial(R"({"frame":"5E","destinationNetwork":999})", point), true, kAccepted },
             { "DLT645", industrial(R"({"network":null})", point), true, "协议连接、点位或采集周期配置无效" },
             { "DLT645", withPoint("{}", R"("digits":2)", R"("digits":2,"bit":null)"), true, "协议连接、点位或采集周期配置无效" },
             { "DLT645", withPoint("{}", R"("digits":2)", R"("digits":2,"area":"XX","address":"x")"), true, kAccepted },
             { "DLT645", withPoint("{}", R"("identifier":"00000000")", R"("identifier":"0000")"), true, pointInvalid },
             { "DLT645", withPoint(R"({"version":"1997"})", R"("identifier":"00000000")", R"("identifier":"0000")"), true, kAccepted },
             { "DLT645", withPoint("{}", R"("identifier":"00000000")", R"("identifier":"ZZZZZZZZ")"), true, pointInvalid },
             { "DLT645", withPoint("{}", R"("identifier":"00000000",)", ""), true, pointInvalid },
             { "DLT645", withPoint("{}", "BCD", "INT16"), true, pointInvalid },
             { "DLT645", withPoint("{}", R"("length":4)", R"("length":9)"), true, pointInvalid },
             { "DLT645", industrial("{}", replaced(replaced(point, "BCD", "HEX"), R"("length":4)", R"("length":200)")), true, kAccepted },
             { "DLT645", industrial("{}", replaced(replaced(point, "BCD", "HEX"), R"("length":4)", R"("length":201)")), true, pointInvalid },
             { "DLT645", withPoint("{}", R"(,"length":4)", ""), true, pointInvalid },
             { "DLT645", withPoint("{}", R"("digits":2)", R"("digits":9)"), true, pointInvalid },
             { "DLT645", withPoint("{}", R"("digits":2)", R"("digits":2,"writable":true)"), true, pointInvalid },
             { "DLT645", industrial(writable, replaced(replaced(point, "BCD", "HEX"), R"("length":4)", R"("length":38,"writable":true)")), true, kAccepted },
             { "DLT645", industrial(writable, replaced(replaced(point, "BCD", "HEX"), R"("length":4)", R"("length":39,"writable":true)")), true, pointInvalid },
             { "DLT645", industrial(R"({"version":"1997","writePassword":"00000000"})", replaced(replaced(replaced(point, "BCD", "HEX"), R"("identifier":"00000000")", R"("identifier":"0000")"), R"("length":4)", R"("length":44,"writable":true)")), true, kAccepted },
         }) {
        check(item);
    }
}

// ---- MQTT ----
void testMqtt() {
    const std::string point = R"({"id":")" + std::string(kPoint) + R"(","name":"温度","field":"temperature","dataType":"DOUBLE"})";
    const auto config = [&](std::string_view extra, std::string_view entry) {
        return R"({"storagePolicy":"report","topic":"devices",)" + std::string(extra) + R"("points":[)" + std::string(entry) + "]}";
    };
    const auto withPoint = [&](std::string_view from, std::string_view to) {
        return config("", replaced(point, from, to));
    };
    const auto withTopic = [&](std::string_view topic) {
        return R"({"storagePolicy":"report","topic":")" + std::string(topic) + R"(","points":[)" + point + "]}";
    };
    const std::string pointInvalid = "MQTT 点位名称、标识、JSON 字段或类型无效";
    for (const auto& item : std::vector<ConfigCase>{
             { "MQTT", config("", point), true, kAccepted },
             { "MQTT", R"({"qos":7,"topic":"a/#/b"})", false, kAccepted },
             { "MQTT", R"({"storagePolicy":"report","points":[]})", true, "MQTT Topic 过滤器无效" },
             { "MQTT", withTopic("a/#/b"), true, "MQTT Topic 过滤器无效" },
             { "MQTT", config(R"("qos":3,)", point), true, "MQTT QoS 必须是 0、1 或 2" },
             { "MQTT", config(R"("qos":"1",)", point), true, "MQTT 配置字段类型无效" },
             { "MQTT", config(R"("payloadFormat":"xml",)", point), true, "MQTT 负载格式或设备识别方式无效" },
             { "MQTT", config(R"("identitySource":"header",)", point), true, "MQTT 负载格式或设备识别方式无效" },
             { "MQTT", config(R"("identitySource":"payload",)", point), true, "负载识别需要设备标识字段" },
             { "MQTT", config(R"("topicDeviceSegment":1025,)", point), true, "Topic 设备标识段号无效" },
             { "MQTT", config(R"("recordLength":-1,)", point), true, "二进制记录长度无效" },
             { "MQTT", config(R"("delimiter":"",)", point), true, "分隔符长度须为 1 到 8 字节" },
             { "MQTT", config(R"("timeFormat":"rfc",)", point), true, "时间格式无效" },
             { "MQTT", config(R"("payloadFormat":"text","recordsPath":"/a",)", replaced(point, "temperature", "1")), true, "记录路径仅用于 JSON" },
             { "MQTT", config(R"("commandTopic":"cmd/+",)", point), true, "MQTT 指令 Topic 无效" },
             { "MQTT", R"({"storagePolicy":"report","topic":"devices"})", true, "MQTT 点位配置无效" },
             { "MQTT", withPoint(kPoint, "not-a-uuid"), true, pointInvalid },
             { "MQTT", withPoint("DOUBLE", "INT"), true, pointInvalid },
             { "MQTT", withPoint(R"("name":"温度")", R"("name":"")"), true, pointInvalid },
             { "MQTT", config("", point + "," + replaced(point, kPoint, "00000000-0000-7000-8000-000000000004")), true, pointInvalid },
             { "MQTT", config("", point + "," + replaced(replaced(point, kPoint, "00000000-0000-7000-8000-000000000004"), "temperature", "humidity")), true, "MQTT 点位名称重复" },
             { "MQTT", withPoint(R"("dataType":"DOUBLE")", R"("dataType":"DOUBLE","unit":"123456789012345678901234567890123")"), true, "MQTT 点位单位过长" },
             { "MQTT", withPoint(R"("dataType":"DOUBLE")", R"("dataType":"DOUBLE","writable":true)"), true, "可写 MQTT 点位需要指令 Topic" },
             { "MQTT", withPoint(R"("dataType":"DOUBLE")", R"("dataType":"DOUBLE","enumValues":[{"input":"a","output":"1"},{"input":"a","output":"2"}])"), true, "枚举原值、转换值须唯一且符合点位类型" },
             { "MQTT", config(R"("commandTopic":"cmd","commands":[],)", point), true, "控制消息最多 32 条，且不能与旧版单条控制配置同时使用" },
             { "MQTT", withTopic("devices/+"), true, "通配 Topic 需要从 Topic 或负载识别设备" },
             { "MQTT", replaced(withTopic("devices/+"), R"("topic":)", R"("identitySource":"topic","topic":)"), true, kAccepted },
             { "MQTT", config(R"("registers":[{"scale":1}],)", point), true, "原生点位不支持倍率和偏移，请使用派生值表达式" },
             { "MQTT", replaced(config("", point), R"("topic":"devices",)", R"("topic":"devices","topic":"devices",)"), true, "MQTT 配置字段类型无效" },
         }) {
        check(item);
    }
}

// ---- 派生点 ----
std::string derivedConfig(std::string_view points, std::string_view extra = {}) {
    return R"({"registers":[{"id":")" + std::string(kPhysical) + R"("}],)" + std::string(extra) + R"("derivedPoints":[)" + std::string(points) + "]}";
}

std::string derivedPoint(std::string_view id = kDerived, std::string_view inputId = kPhysical) {
    return R"({"id":")" + std::string(id) + R"(","name":"d","kind":"expression","valueType":"number","expression":"p * 2","inputs":[{"alias":"p","pointId":")" + std::string(inputId) + R"("}]})";
}

void checkDerived(const std::string& config, const std::optional<std::string>& expected) {
    const auto value = json(config);
    const auto actual = outcome([&] {
        (void)ProtocolService::validateDerivedPoints(value);
    });
    require(actual == expected, "derived " + config + " => " + actual.value_or("accepted") + ", expected " + expected.value_or("accepted"));
}

void testDerivedPoints() {
    const auto point = derivedPoint();
    const auto with = [&](std::string_view from, std::string_view to) {
        return derivedConfig(replaced(point, from, to));
    };
    const std::string window = R"({"id":")" + std::string(kDerived) + R"(","name":"avg","kind":"average","valueType":"number","windowSeconds":60,"sourceAlias":"p","inputs":[{"alias":"p","pointId":")" + std::string(kPhysical) + R"("}]})";
    std::string tooMany, inputs;
    for (int index = 0; index < 33; ++index) {
        char id[37];
        std::snprintf(id, sizeof(id), "00000000-0000-4000-8000-%012d", index + 100);
        tooMany += (index ? "," : "") + derivedPoint(id);
    }
    for (int index = 0; index < 17; ++index) {
        inputs += std::string(index ? "," : "") + R"({"alias":"v)" + std::to_string(index) + R"(","pointId":")" + std::string(kPhysical) + R"("})";
    }
    {
        // json_value::parse 借用输入，原文须覆盖整个使用期。
        const auto acceptedText = derivedConfig(point);
        const auto accepted = json(acceptedText);
        require(ProtocolService::validateDerivedPoints(accepted), "configured derived points must be reported");
        const std::string emptyText = R"({"registers":[]})";
        const auto empty = json(emptyText);
        require(!ProtocolService::validateDerivedPoints(empty), "configuration without derived points must not require edge support");
    }
    {
        const std::string deviceText = R"({"derivedPoints":[)" + derivedPoint(kDerived, kPoint) + "]}";
        const std::string protocolText = R"({"registers":[{"id":")" + std::string(kPhysical) + R"("}]})";
        const std::string ioText = R"({"points":[{"id":")" + std::string(kPoint) + R"("}]})";
        const auto device = json(deviceText);
        const std::array sources{ json(protocolText), json(ioText) };
        require(ProtocolService::validateDerivedPoints(device, sources), "device-level derived point must resolve an IO point from another source");
        const std::string conflictingText = R"({"points":[{"id":")" + std::string(kPhysical) + R"("}]})";
        const std::array conflictingSources{ json(protocolText), json(conflictingText) };
        const auto conflict = outcome([&] {
            (void)ProtocolService::validateDerivedPoints(device, conflictingSources);
        });
        require(conflict == "不同采集来源的点位标识不能重复", "duplicate source ids must be rejected by protocol validation");
    }
    const std::vector<std::pair<std::string, std::optional<std::string>>> cases{
        { derivedConfig(window), kAccepted },
        { derivedConfig(replaced(point, R"("expression":"p * 2")", R"("expression":"p * 2","sourceAlias":"this alias is longer than thirty two bytes")")), kAccepted },
        { R"({"funcs":[{"dir":"UP","elements":[{"id":")" + std::string(kPhysical) + R"("}]}],"derivedPoints":[)" + derivedPoint() + "]}", kAccepted },
        { R"({"derivedPoints":{}})", "derivedPoints 必须是数组" },
        { R"({"derivedPoints":null})", "derivedPoints 必须是数组" },
        { R"({"derivedPoints":[1]})", "派生点最多 32 个" },
        { derivedConfig(tooMany), "派生点最多 32 个" },
        { with(R"("name":"d")", R"("name":5)"), "派生点字段无效" },
        { with(R"("name":"d")", R"("name":"d","visible":null)"), "派生点字段无效" },
        { with(R"("id":")" + std::string(kDerived) + R"(",)", ""), "派生点字段无效: id" },
        { with(kDerived, ""), "派生点字段无效: id" },
        { with(kDerived, "derived"), "派生点标识无效或重复" },
        { derivedConfig(point + "," + replaced(point, "p * 2", "p * 3")), "派生点标识无效或重复" },
        { with(R"("name":"d",)", ""), "派生点字段无效: name" },
        { with(R"("name":"d")", R"("name":"")"), "派生点字段无效: name" },
        { with(R"("name":"d")", R"("name":"d","unit":"123456789012345678901234567890123")"), "派生点字段无效: unit" },
        { with(R"("name":"d")", R"("name":"d","unitMode":"")"), "派生点字段无效: unitMode" },
        { with(R"("name":"d")", R"("name":"d","unitMode":"auto")"), "单位模式无效" },
        { with(R"("kind":"expression",)", ""), "派生点字段无效: kind" },
        { with(R"("kind":"expression")", R"("kind":"sum")"), "派生点计算类型无效" },
        { with(R"("valueType":"number",)", ""), "派生点字段无效: valueType" },
        { with(R"("valueType":"number")", R"("valueType":"text")"), "派生点结果类型无效" },
        { with(R"(,"expression":"p * 2")", ""), "派生点字段无效: expression" },
        { with(R"("name":"d")", R"("name":"d","windowSeconds":0)"), "派生点时间参数无效: windowSeconds" },
        { with(R"("name":"d")", R"("name":"d","maxAgeSeconds":86401)"), "派生点时间参数无效: maxAgeSeconds" },
        { derivedConfig(replaced(window, R"("windowSeconds":60,)", "")), "窗口派生点需要时间窗口和数值结果类型" },
        { derivedConfig(replaced(window, R"("valueType":"number")", R"("valueType":"boolean")")), "窗口派生点需要时间窗口和数值结果类型" },
        { derivedConfig(replaced(window, R"("sourceAlias":"p",)", "")), "派生点字段无效: sourceAlias" },
        { derivedConfig(replaced(window, R"("sourceAlias":"p")", R"("sourceAlias":"z")")), "窗口计算的输入点未绑定" },
        { with(R"(,"inputs":[{"alias":"p","pointId":")" + std::string(kPhysical) + R"("}])", ""), "派生点必须绑定输入点" },
        { with(R"([{"alias":"p","pointId":")" + std::string(kPhysical) + R"("}])", "[]"), "派生点必须绑定输入点" },
        { with(R"([{"alias":"p","pointId":")" + std::string(kPhysical) + R"("}])", "[" + inputs + "]"), "派生点输入绑定无效" },
        { with(R"("alias":"p",)", ""), "派生点字段无效: alias" },
        { with(R"("pointId":")" + std::string(kPhysical) + R"(")", R"("pointId":"x")"), "派生点输入绑定无效" },
        { with(R"("alias":"p")", R"("alias":"p + q")"), "派生点输入绑定无效" },
        { with(R"({"alias":"p","pointId":")" + std::string(kPhysical) + R"("})", R"({"alias":"p","pointId":")" + std::string(kPhysical) + R"("},{"alias":"p","pointId":")" + std::string(kPhysical) + R"("})"), "派生点输入绑定无效" },
        { with("p * 2", "p * q"), "公式包含未绑定变量: q" },
        { with(R"("name":"d")", R"("name":"d","unitMode":"conditional")"), "条件单位必须设置规则，固定单位不得保留条件" },
        { with(R"("name":"d")", R"("name":"d","unitRules":[{"condition":"p > 1","unit":"k"}])"), "条件单位必须设置规则，固定单位不得保留条件" },
        { with(R"("name":"d")", R"("name":"d","unitMode":"conditional","unitRules":[{"unit":"k"}])"), "派生点字段无效: condition" },
        { with(R"("name":"d")", R"("name":"d","unitMode":"conditional","unitRules":[{"condition":"q > 1"}])"), "单位条件包含未绑定变量: q" },
        { with(R"("name":"d")", R"("name":"d","unitMode":"conditional","unitRules":[{"condition":"p > 1","unit":"123456789012345678901234567890123"}])"), "派生点字段无效: unit" },
        { with(R"("name":"d")", R"("name":"d","unitMode":"conditional","unitRules":[{"condition":"p>1"},{"condition":"p>1"},{"condition":"p>1"},{"condition":"p>1"},{"condition":"p>1"},{"condition":"p>1"},{"condition":"p>1"},{"condition":"p>1"},{"condition":"p>1"}])"), "单位条件最多 8 条" },
        { derivedConfig(derivedPoint(kPhysical)), "派生点标识不能与采集点重复" },
        { derivedConfig(derivedPoint(kDerived, "00000000-0000-4000-8000-000000000099")), "派生点引用的采集点不存在或不是上行点" },
        { R"({"funcs":[{"dir":"DOWN","elements":[{"id":")" + std::string(kPhysical) + R"("}]}],"derivedPoints":[)" + derivedPoint() + "]}", "派生点引用的采集点不存在或不是上行点" },
        { derivedConfig(derivedPoint(kDerived, kPoint) + "," + derivedPoint(kPoint, kDerived)), "派生点存在循环依赖" },
        { derivedConfig(point, R"("pointVisibility":"hidden",)"), "点位显示设置必须是标识到布尔值的映射" },
        { derivedConfig(point, R"("pointVisibility":null,)"), "点位显示设置必须是标识到布尔值的映射" },
        { derivedConfig(point, R"("pointVisibility":{"x":true},)"), "点位显示设置必须是标识到布尔值的映射" },
        { derivedConfig(point, R"("pointVisibility":{")" + std::string(kPhysical) + R"(":1},)"), "点位显示设置必须是标识到布尔值的映射" },
        { derivedConfig(point, R"("pointVisibility":{")" + std::string(kPhysical) + R"(":false},)"), kAccepted },
        { derivedConfig(point, R"("pointVisibility":"hidden","pointVisibility":{},)"), kAccepted },
    };
    for (const auto& [config, expected] : cases) {
        checkDerived(config, expected);
    }
}

void testExternalIo() {
    const std::string point = R"({"id":"00000000-0000-4000-8000-000000000001","name":"输入","kind":"DI","channel":"di0"})";
    check({ "ExternalIO", R"({"storagePolicy":"report","points":[]})", true, kAccepted });
    check({ "ExternalIO", R"({"storagePolicy":"report","points":[)" + point + "]}", true, kAccepted });
    check({ "ExternalIO", R"({"storagePolicy":"report"})", true, "外部 IO 点位配置无效" });
    check({ "ExternalIO", R"({"storagePolicy":"report","points":[)" + point + "," + point + "]}", true, "外部 IO 点位标识重复" });
    check({ "ExternalIO", R"({"storagePolicy":"report","points":[{"id":"not-a-uuid","name":"输入","kind":"DI","channel":"di0"}]})", true, "外部 IO 点位标识无效" });
    check({ "ExternalIO", R"({"storagePolicy":"report","points":[{"id":"00000000-0000-4000-8000-000000000001","name":"输入","kind":"AO","channel":"di0"}]})", true, "外部 IO 点位类型无效" });
    check({ "ExternalIO", R"({"storagePolicy":"report","points":[{"id":"00000000-0000-4000-8000-000000000001","name":"电流","kind":"AI","channel":"AI0","unit":"uA"}]})", true, kAccepted });
    check({ "ExternalIO", R"({"storagePolicy":"report","points":[{"id":"00000000-0000-4000-8000-000000000001","name":"电流","kind":"AI","channel":"AI0","unit":"mA"}]})", true, "外部 IO 原始单位无效，请使用派生值换算" });
}

// ---- 请求模型 ----
void testUpdateBodyTypes() {
    for (const auto text : { R"({"name":5})", R"({"protocol":1})" }) {
        bool rejected = false;
        try {
            (void)ruvia::from_json<service::protocol::UpdateProtocolBody>(text);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        require(rejected, "non-string update fields must not parse");
    }
    const auto cleared = ruvia::from_json<service::protocol::UpdateProtocolBody>(R"({"remark":null,"config":{"readInterval":5}})");
    require(cleared.is_null<"remark">() && cleared.get<"config">(), "update remark must be nullable and config kept as raw JSON");
}

} // namespace

int main() {
    try {
        testCommonFields();
        testSl651();
        testModbus();
        testS7();
        testMc();
        testFins();
        testDlt645();
        testMqtt();
        testDerivedPoints();
        testExternalIo();
        testUpdateBodyTypes();
        std::cout << "protocol service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "protocol service test failed: " << error.what() << '\n';
        return 1;
    }
}
