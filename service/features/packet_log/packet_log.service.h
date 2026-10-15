#pragma once

#include <tuple>

#include "service/common/uuid.h"
#include "service/common/message.h"
#include "service/features/packet_log/packet_log.entity.h"
#include "service/utils/redis.h"
#include "service/utils/json.h"
#include "service/features/live/live.service.h"

namespace service::packet_log {
class DebugPacketService {
  public:
    template <typename Redis>
    static ruvia::task<void> finishAcquisition(const Redis& redis, std::string_view acquisitionId,
        std::string_view state) {
        if (acquisitionId.empty() || (state != "success" && state != "partial" && state != "failed"))
            throw std::invalid_argument("invalid acquisition completion");
        static constexpr std::string_view script = R"lua(
if redis.call('EXISTS',KEYS[1])==0 then return 0 end
local previous=redis.call('HGET',KEYS[1],'state')
if previous and previous~='running' then return 0 end
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
redis.call('HSET',KEYS[1],'state',ARGV[1],'finished_at_ms',now)
return 1
)lua";
        const std::string key = std::string(DebugAcquisitionStorage::prefix) + std::string(acquisitionId);
        const std::vector<std::string_view> keys{key}, args{state};
        const auto result = co_await redis.eval(script, keys, args);
        if (result.kind() == ruvia::redis_value::kind_type::error)
            message::redis::throwValue("finish debug acquisition", result);
        co_await service::live::publish(redis, "packet-debug");
    }
    template <typename Redis>
    static ruvia::task<void> recordPacket(const Redis& redis, service::common::UuidV7Generator& uuidGenerator, std::string_view linkId,
        std::string_view deviceId, std::string_view direction, std::string_view source,
        std::string_view address, std::span<const std::uint8_t> payload, std::int64_t time,
        bool captureLink, bool captureDevice, std::string_view eventId = {},
        std::string_view status = {}, std::string_view reason = {},
        std::string_view parsedJson = {},
        std::string_view replyToPacketId = {}, bool updateOnly = false, std::size_t baseOffset = 0,
        std::string_view acquisitionId = {}, std::string_view edgeNodeId = {},
        std::string_view edgeNodeName = {}) {
        if (acquisitionId.empty()) throw std::invalid_argument("debug packet requires acquisition ID");
        if (linkId.empty()) co_return;
        // 索引保存轮次 ID；每轮持有独立报文集合，状态更新只修改原记录。
        static constexpr std::string_view script = R"lua(
local fields={}
for i=1,#ARGV,2 do fields[ARGV[i]]=ARGV[i+1] end
local prefix='iot:debug:v4:'
local acquisition=fields.acquisition_id
if not acquisition or acquisition=='' then return redis.error_reply('missing acquisition ID') end
local packet=prefix..'packet:'..fields.event_id
local round=fields.acquisition_prefix..acquisition
fields.acquisition_prefix=nil
local members=round..':packets'
local hasPayload=fields.payload_hex and fields.payload_hex~=''
local hasPacket=hasPayload or (fields.parsed_json and fields.parsed_json~='')
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if redis.call('EXISTS',round..':retired')==1 then return 0 end
local function validate(key,expected)
    local kind=redis.call('TYPE',key).ok
    return kind=='none' or kind==expected
end
if not validate(round,'hash') or not validate(packet,'hash') or not validate(members,'zset') then
    return redis.error_reply('invalid acquisition storage type')
end
for _,key in ipairs(KEYS) do
    if not validate(key,'zset') then return redis.error_reply('invalid acquisition index type') end
end
local exists=redis.call('EXISTS',packet)==1
if fields.update_only=='1' and ((hasPacket and not exists) or redis.call('EXISTS',round)==0) then return 0 end
fields.update_only=nil
for _,name in ipairs({'acquisition_id','link_id','device_id','direction'}) do
    local previous=redis.call('HGET',packet,name)
    if previous and previous~='' and fields[name] and fields[name]~='' and previous~=fields[name] then
        return redis.error_reply('packet identity conflict')
    end
end
for _,name in ipairs({'link_id','device_id'}) do
    local previous=redis.call('HGET',round,name)
    if previous and previous~='' and fields[name] and fields[name]~='' and previous~=fields[name] then
        return redis.error_reply('acquisition scope conflict')
    end
end
if hasPacket and not exists and redis.call('ZCARD',members)>=4096 then return redis.error_reply('acquisition packet limit exceeded') end
local created=tonumber(redis.call('HGET',round,'created_ms')) or now
local expires=created+86400000
local ranks={
    transport_status={sending=1,sent=2,received=2,failed=3},
    response_status={waiting=1,success=2,failed=2,not_applicable=2},
    parse_status={pending=1,success=2,failed=2,not_applicable=2}
}
local changed=false
if fields.parsed_json and fields.parsed_json~='' then
    local valid,incoming=pcall(cjson.decode,fields.parsed_json)
    if not valid or type(incoming)~='table' or type(incoming.values)~='table' then
        return redis.error_reply('invalid debug parsed values')
    end
    local parsed={}
    local stored=redis.call('HGETALL',packet)
    for i=1,#stored,2 do
        if string.sub(stored[i],1,13)=='parsed_value:' then parsed[string.sub(stored[i],14)]=stored[i+1] end
    end
    for name,value in pairs(fields) do
        if string.sub(name,1,13)=='parsed_value:' then parsed[string.sub(name,14)]=value end
    end
    local names={}
    for name,_ in pairs(parsed) do names[#names+1]=name end
    table.sort(names)
    local values={}
    for _,name in ipairs(names) do values[#values+1]=cjson.encode(name)..':'..parsed[name] end
    incoming.values=nil
    local metadata=cjson.encode(incoming)
    fields.parsed_json=string.sub(metadata,1,-2)..(metadata=='{}' and '' or ',')..'"values":{'..table.concat(values,',')..'}}'

end
local fragment='fragment:'..(fields.offset or '0')
if hasPayload then
    local offset=tonumber(fields.offset)
    if not offset or offset<0 or offset%4096~=0 or offset>1048576 or #fields.payload_hex>8192 then
        return redis.error_reply('invalid packet fragment')
    end
    local previous=redis.call('HGET',packet,fragment)
    if previous and previous~=fields.payload_hex then return redis.error_reply('packet fragment conflict') end
    redis.call('HSET',packet,fragment,fields.payload_hex)
    local offsets={}
    local fragments={}
    local values=redis.call('HGETALL',packet)
    for i=1,#values,2 do
        local position=string.match(values[i],'^fragment:(%d+)$')
        if position then offsets[#offsets+1]=tonumber(position); fragments[tonumber(position)]=values[i+1] end
    end
    table.sort(offsets)
    local payload={}
    for _,position in ipairs(offsets) do payload[#payload+1]=fragments[position] end
    fields.payload_hex=table.concat(payload)
end
if hasPacket then
for name,value in pairs(fields) do
    if value~='' then
        local previous=redis.call('HGET',packet,name)
        local allowed=true
        if ranks[name] and previous and previous~=value then
            allowed=(ranks[name][value] or 0)>(ranks[name][previous] or 0)
        elseif (name=='time_ms' or name=='direction') and previous then
            allowed=previous==value
        end
        if allowed and previous~=value then redis.call('HSET',packet,name,value); changed=true end
    end
end
redis.call('HSET',packet,'created_ms',created)
if changed then redis.call('HINCRBY',packet,'revision',1) end
redis.call('PEXPIREAT',packet,expires)
end
redis.call('HSET',round,'acquisition_id',acquisition,'created_ms',created)
local state=string.sub(acquisition,1,7)=='legacy:' and 'unreported' or fields.acquisition_state
local previousState=redis.call('HGET',round,'state')
if state and state~='' and (not previousState or previousState=='running') then
    redis.call('HSET',round,'state',state)
    if state~='running' then redis.call('HSET',round,'finished_at_ms',now) end
end
for _,name in ipairs({'link_id','device_id','source'}) do
    local value=fields[name]
    if value and value~='' then redis.call('HSET',round,name,value) end
end
local timestamp=tonumber(fields.time_ms) or now
local started=tonumber(redis.call('HGET',round,'started_at_ms')) or timestamp
local ended=tonumber(redis.call('HGET',round,'last_packet_at_ms')) or timestamp
redis.call('HSET',round,'started_at_ms',math.min(started,timestamp),'last_packet_at_ms',math.max(ended,timestamp))
if hasPacket then
    redis.call('ZADD',members,'NX',timestamp,fields.event_id)
    redis.call('PEXPIREAT',members,expires)
end
redis.call('PEXPIREAT',round,expires)
local function retire(id)
    local value=prefix..'acquisition:'..id
    for _,name in ipairs({'link_index','device_index'}) do
        local index=redis.call('HGET',value,name)
        if index and redis.call('ZSCORE',index,id) then return end
    end
    for _,packetId in ipairs(redis.call('ZRANGE',value..':packets',0,-1)) do
        redis.call('DEL',prefix..'packet:'..packetId)
    end
    redis.call('SET',value..':retired','1','PX',86400000)
    redis.call('DEL',value..':packets',value)
end
for _,key in ipairs(KEYS) do
    redis.call('HSET',round,string.find(key,':device:',1,true) and 'device_index' or 'link_index',key)
    redis.call('ZADD',key,'NX',created,acquisition)
    local expired=redis.call('ZRANGEBYSCORE',key,'-inf',now-86400000)
    redis.call('ZREMRANGEBYSCORE',key,'-inf',now-86400000)
    for _,id in ipairs(expired) do retire(id) end
    local excess=redis.call('ZCARD',key)-100
    if excess>0 then
        local removed=redis.call('ZRANGE',key,0,excess-1)
        redis.call('ZREMRANGEBYRANK',key,0,excess-1)
        for _,id in ipairs(removed) do retire(id) end
    end
    redis.call('EXPIRE',key,86400)
end
return changed and 1 or 0

)lua";
        std::vector<std::string> storage;
        if (captureLink) storage.push_back(DebugPacketStorage::key("link", linkId));
        if (captureDevice && !deviceId.empty()) storage.push_back(DebugPacketStorage::key("device", deviceId));
        if (storage.empty()) co_return;
        const std::vector<std::string_view> keys(storage.begin(), storage.end());
        // Preserve decimal and 64-bit numeric text while merging per-response values in Redis.
        std::vector<std::string> parsedFields;
        if (!parsedJson.empty()) {
            const auto decoded = ruvia::json_value::parse(parsedJson);
            if (!decoded) throw std::invalid_argument("invalid debug parsed JSON");
            const auto values = decoded->get<ruvia::json_value>("values");
            if (!values || !values->is_object()) throw std::invalid_argument("invalid debug parsed values");
            const bool valid = values->for_each_field(
                [&](std::string_view name, const ruvia::json_value& value) {
                    parsedFields.push_back("parsed_value:" + std::string(name));
                    parsedFields.emplace_back(value.view());
                    return true;
                });
            if (!valid) throw std::invalid_argument("invalid debug parsed fields");
        }
        const auto identity = eventId.empty() ? uuidGenerator.next() : std::string(eventId);
        for (std::size_t offset = 0; offset < std::max<std::size_t>(1, payload.size()); offset += 4096) {
            const auto chunk = payload.subspan(offset, std::min<std::size_t>(4096, payload.size() - offset));
            const auto hex = message::toHex(std::vector<std::uint8_t>(chunk.begin(), chunk.end()));
            const auto timestamp = std::to_string(time);
            const auto offsetText = std::to_string(baseOffset + offset);
            const auto& stableId = identity;
            const auto transportStatus = direction == "RX" ? "received" :
                status == "failed" && reason == "socket_write_failed" ? "failed" :
                status == "sending" ? "sending" : "sent";
            const auto responseStatus = status == "waiting" || status == "success" ? status :
                status == "failed" && reason != "socket_write_failed" ? std::string_view("failed") : std::string_view{};
            const auto parseStatus = !parsedJson.empty() ? "success" : status == "parse_failed" ? "failed" :
                status == "transport_only" ? "not_applicable" : direction == "RX" ? "pending" : "not_applicable";
            const auto acquisitionState = status == "acquisition_success" ? "success" :
                status == "acquisition_partial" ? "partial" : status == "acquisition_failed" ? "failed" : "running";
            std::vector<std::string_view> args{"acquisition_prefix", DebugAcquisitionStorage::prefix, std::tuple_element_t<DebugAcquisitionHash::column_index<"acquisition_id">(), DebugAcquisitionHash::columns_type>::name.view(), acquisitionId, std::tuple_element_t<DebugPacketHash::column_index<"link_id">(), DebugPacketHash::columns_type>::name.view(), linkId, std::tuple_element_t<DebugPacketHash::column_index<"device_id">(), DebugPacketHash::columns_type>::name.view(), deviceId,
                std::tuple_element_t<DebugPacketHash::column_index<"direction">(), DebugPacketHash::columns_type>::name.view(), direction, std::tuple_element_t<DebugPacketHash::column_index<"source">(), DebugPacketHash::columns_type>::name.view(), source, std::tuple_element_t<DebugPacketHash::column_index<"address">(), DebugPacketHash::columns_type>::name.view(), address,
                std::tuple_element_t<DebugPacketHash::column_index<"edge_node_id">(), DebugPacketHash::columns_type>::name.view(), edgeNodeId, std::tuple_element_t<DebugPacketHash::column_index<"edge_node_name">(), DebugPacketHash::columns_type>::name.view(), edgeNodeName,
                std::tuple_element_t<DebugPacketHash::column_index<"payload_hex">(), DebugPacketHash::columns_type>::name.view(), hex, std::tuple_element_t<DebugPacketHash::column_index<"time_ms">(), DebugPacketHash::columns_type>::name.view(), timestamp, std::tuple_element_t<DebugPacketHash::column_index<"offset">(), DebugPacketHash::columns_type>::name.view(), offsetText,
                std::tuple_element_t<DebugPacketHash::column_index<"event_id">(), DebugPacketHash::columns_type>::name.view(), stableId, std::tuple_element_t<DebugPacketHash::column_index<"transport_status">(), DebugPacketHash::columns_type>::name.view(), transportStatus, std::tuple_element_t<DebugPacketHash::column_index<"response_status">(), DebugPacketHash::columns_type>::name.view(), responseStatus,
                std::tuple_element_t<DebugPacketHash::column_index<"parse_status">(), DebugPacketHash::columns_type>::name.view(), parseStatus, std::tuple_element_t<DebugPacketHash::column_index<"reason">(), DebugPacketHash::columns_type>::name.view(), reason, std::tuple_element_t<DebugPacketHash::column_index<"reply_to_packet_id">(), DebugPacketHash::columns_type>::name.view(), replyToPacketId,
                std::tuple_element_t<DebugPacketHash::column_index<"parsed_json">(), DebugPacketHash::columns_type>::name.view(), parsedJson,
                "acquisition_state", acquisitionState, "update_only", updateOnly ? "1" : "0"};
            for (const auto& field : parsedFields) args.push_back(field);
            const auto result = co_await redis.eval(script, keys, args);
            if (result.kind() == ruvia::redis_value::kind_type::error)
                message::redis::throwValue("append debug packet", result);
        }
        co_await service::live::publish(redis, "packet-debug");
    }
};
}
