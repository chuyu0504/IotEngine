import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { databaseUrl, redisUrl, apiBase } from './architecture-fixture';
import { openSnapshotSubscription } from './sse-fixture';

// 只使用隔离数据库、Redis 和 API；覆盖真实 WebSocket、投影、历史写入。
const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const uuid = () => crypto.randomUUID();
const bytes = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');
function integer(input: number | bigint) {
    let value = BigInt(input);
    const result: number[] = [];
    do {
        let byte = Number(value & 127n);
        value >>= 7n;
        if (value) byte |= 128;
        result.push(byte);
    } while (value);
    return Buffer.from(result);
}
function field(tag: number, value: number | bigint | string | Buffer) {
    if (typeof value === 'number' || typeof value === 'bigint')
        return Buffer.concat([integer(tag * 8), integer(value)]);
    const data = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag * 8 + 2), integer(data.length), data]);
}
function decode(data: Buffer) {
    let position = 0;
    const read = () => {
        let value = 0n, shift = 0n;
        for (;;) {
            const byte = data[position++];
            assert(byte !== undefined);
            value |= BigInt(byte & 127) << shift;
            if (!(byte & 128)) return value;
            shift += 7n;
        }
    };
    const fields = new Map<number, bigint | Buffer>();
    while (position < data.length) {
        const tag = Number(read());
        if ((tag & 7) === 0) fields.set(tag >> 3, read());
        else {
            assert.equal(tag & 7, 2);
            const size = Number(read());
            fields.set(tag >> 3, data.subarray(position, position + size));
            position += size;
        }
    }
    return fields;
}
async function until(test: () => Promise<boolean>, label: string) {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
        if (await test()) return;
        await Bun.sleep(50);
    }
    throw Error(label);
}
const node = uuid(), platform = '00000000-0000-7000-8000-000000000001';
const admin = '00000000-0000-7000-8000-000000000002';
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now()/1000);
const unsigned = `${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:admin,user_id:admin,username:'debug-test',token_type:'access',iat:now,exp:now+3600})}`;
const token = `${unsigned}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
async function debugSwitch(scope: string, id: string, enabled: boolean) {
    const [before] = await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`;
    const response = await fetch(`${apiBase}/v1/${scope}/${id}/debug`, { method:'PUT', headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'}, body:JSON.stringify({enabled}) });
    const body = await response.text();
    assert.equal(response.status,200,body);
    assert.equal(JSON.parse(body).code,0,body);
    const [after] = await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`;
    assert(BigInt(after.revision ?? 0) > BigInt(before.revision ?? 0),
        `${scope} debug switch must queue a new edge configuration, including when disabled`);
}
async function queryDetails(path: string) {
    const response = await fetch(apiBase+path, {
        headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(15000),
    });
    assert.equal(response.status,200);
    const result = await response.json();
    assert.equal(result.code,0);
    return result.data;
}
type DebugRound = {
    id: string;
    history_id?: unknown;
    storage_status?: unknown;
    parsed_json?: unknown;
    packets: { payload_hex: string; transport_status: string; edge_node_id: string; edge_node_name: string }[];
};
async function liveSnapshot<T>(path: string, eventName: string): Promise<T> {
    const stream = await openSnapshotSubscription(path, token);
    try {
        for (;;) {
            const event = await stream.next();
            assert.notEqual(event.event, 'error', event.data);
            if (event.event !== eventName) continue;
            const reply = JSON.parse(event.data) as { code: number; data: T };
            assert.equal(reply.code, 0, event.data);
            return reply.data;
        }
    } finally {
        await stream.close();
    }
}
const baseImei = `99${String(Math.floor(Math.random() * 1e12)).padStart(12, '0')}`;
let checksum = 0;
for (let index = 0; index < 14; ++index) {
    let digit = Number(baseImei[index]) * (index % 2 ? 2 : 1);
    if (digit > 9) digit -= 9;
    checksum += digit;
}
const imei = baseImei + String((10 - checksum % 10) % 10);
let socket: WebSocket | undefined;
const devices: string[] = [];
const uploadKey = `test:telemetry-upload:${uuid()}`;
try {
    const source = await Bun.file('service/features/edge/edge.service.h').text();
    const script = source.slice(source.indexOf('kStoreTelemetryPart =')).match(/R"lua\(([\s\S]*?)\)lua"/)![1];
    const store = (index: number, content: string, signature = 'metadata') =>
        redis.send('EVAL', [script, '1', uploadKey, signature, String(index), '2', content]);
    assert.equal(await store(1, 'second'), 0);
    assert.equal(await store(1, 'second'), 0);
    assert.equal(await redis.send('TTL', [uploadKey]), -1, 'unfinished acknowledged parts must survive long outages');
    await assert.rejects(store(1, 'conflicting'));
    await assert.rejects(store(0, 'first', 'other-device'));
    assert.deepEqual(await store(0, 'first'), ['first', 'second']);
    console.log('PASS Redis out-of-order parts, duplicate retry, conflict rejection and durable partial state');

    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},${imei},'approved')`;
    await db`UPDATE edge_node SET name=${'调试边缘节点'} WHERE id=${node}`;
    await redis.send('SET', [`iot:edge:auth:${imei}`, `${node}|approved`]);
    let epoch = 0n, sequence = 1n;
    const envelope = (tag: number, payload: Buffer) => Buffer.concat([
        field(1, 6), field(2, bytes(uuid())), field(3, bytes(node)), field(4, bytes(platform)),
        field(5, epoch), field(6, Date.now()), field(8, sequence++), field(tag, payload),
    ]);
    const acknowledgements = new Set<string>();
    const connectOnce = async () => {
        socket?.close();
        epoch = 0n;
        sequence = 1n;
        socket = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect');
        socket.binaryType = 'arraybuffer';
        await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(Error('Hello timeout')), 10000);
        socket!.onopen = () => socket!.send(envelope(20, Buffer.concat([field(1, imei), field(2, 'fixture'), field(3, '0.3.46'), field(23, 1)])));
        socket!.onerror = () => { clearTimeout(timeout); reject(Error('WebSocket failed')); };
        socket!.onclose = () => { clearTimeout(timeout); reject(Error('WebSocket closed before Hello')); };
        socket!.onmessage = event => {
            const message = decode(Buffer.from(event.data as ArrayBuffer));
            if (message.has(21)) { epoch = message.get(5) as bigint; clearTimeout(timeout); resolve(); }
            if (message.has(80)) socket!.send(envelope(81, message.get(80) as Buffer));
            if (message.has(41)) {
                const ack = decode(message.get(41) as Buffer).get(1) as Buffer;
                acknowledgements.add(ack.toString('hex'));
            }
        };
        });
    };
    const connect = async () => {
        const deadline = Date.now() + 20000;
        for (;;) {
            try { await connectOnce(); socket!.send(envelope(26,field(7,Buffer.from([1,2,3,4,5,6])))); return; }
            catch (error) {
                if (Date.now() >= deadline) throw error;
                await Bun.sleep(200);
            }
        }
    };
    await connect();
    await until(async () => {
        const [row] = await db`SELECT capability->>'deviceConfig' AS supported FROM edge_node WHERE id=${node}`;
        const [capability] = await db`SELECT jsonb_array_length(capability->'protocols') AS count FROM edge_node WHERE id=${node}`;
        return row.supported === 'true' && Number(capability.count) === 6;
    }, 'edge device configuration capability was not projected');
    for (const [protocol, protocolNumber] of [['SL651', 1], ['Modbus', 2], ['S7', 3], ['MC', 4], ['FINS', 5], ['DLT645', 6]] as const) {
        const device = uuid(), link = uuid(), model = uuid(), report = uuid(), point = uuid();
        devices.push(device);
        const endpoint = { transport: 'tcp', mode: 'TCP Client', interface: 'eth0',
            ip: '127.0.0.1', port: 55190 + protocolNumber };
        await db`INSERT INTO protocol_config(id,name,protocol,config,created_by)
            VALUES(${model},${model},${protocol},'{"storagePolicy":"report"}'::jsonb,${admin})`;
        await db`INSERT INTO link(id,name,protocol,endpoint,created_by,execution,edge_node_id,status)
            VALUES(${link},${link},${protocol},${endpoint}::jsonb,${admin},'edge',${node},'disabled')`;
        await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
            VALUES(${device},${device},${link},${model},'{"device_code":"1"}'::jsonb,${admin})`;
        for (const [linkOn, deviceOn] of [[false,false],[true,false],[true,true],[false,true],[false,false]]) {
            await debugSwitch('link',link,linkOn);
            await debugSwitch('device',device,deviceOn);
            const view = await queryDetails(`/v1/device/${device}`);
            assert.equal(view.debug_enabled,deviceOn);
            assert.equal(view.link_debug_enabled,linkOn);
            const links = await liveSnapshot<{ list: { id: string; debug_enabled: boolean }[] }>(`/v1/link/events?keyword=${link}`, 'links');
            const linkView = links.list.find(item => item.id === link);
            assert(linkView);
            assert.equal(linkView.debug_enabled,linkOn);
            const packetId = uuid();
            const trace = Buffer.concat([field(1,bytes(packetId)),field(2,bytes(link)),field(3,bytes(device)),field(5,Date.now()),field(6,Buffer.from('AABBCC','hex')),field(7,1),field(8,'RX'),field(10,'received'),field(14,bytes(packetId))]);
            const key = `iot:debug:v4:device:${device}`;
            const linkKey = `iot:debug:v4:link:${link}`;
            const before = Number(await redis.send('ZCARD',[key]));
            const linkBefore = Number(await redis.send('ZCARD',[linkKey]));
            socket!.send(envelope(42,trace));
            if(deviceOn) await until(async()=>Number(await redis.send('ZCARD',[key]))===before+1,'device debug packet was not captured');
            if(linkOn) await until(async()=>Number(await redis.send('ZCARD',[linkKey]))===linkBefore+1,'link debug packet was not captured');
            await Bun.sleep(150);
            assert.equal(Number(await redis.send('ZCARD',[key])),before+Number(deviceOn),'link switch must not enable device capture');
            assert.equal(Number(await redis.send('ZCARD',[linkKey])),linkBefore+Number(linkOn),'device switch must not enable link capture');
            const packets = await liveSnapshot<DebugRound[]>(`/v1/device/events?debugDeviceId=${device}`, 'packets');
            assert.equal(packets.length, deviceOn ? before+1 : before);
            if(packets.length) { assert.equal(packets[0].packets[0].payload_hex,'AABBCC'); assert.equal(packets[0].packets[0].transport_status,'received'); }
        }
        assert.equal((await db`SELECT debug_enabled FROM device WHERE id=${device}`)[0].debug_enabled,false);
        console.log(`PASS ${protocol}: independent link/device debug switches, capture gating and separate authorized views`);
        const observed = Date.now();
        const values = field(9, Buffer.concat([field(1, point), field(2, 'temperature'), field(3, 'C'),
            field(4, Buffer.concat([field(1, 3), field(4, 42)]))]));
        const raw = [Buffer.from('7E7E0100FF', 'hex'), Buffer.from('7E7E020000', 'hex')];
        const packetIds = raw.map(() => uuid());
        await debugSwitch('device',device,true);
        for (let index=0;index<raw.length;index++) {
            socket!.send(envelope(42,Buffer.concat([field(1,bytes(packetIds[index])),field(2,bytes(link)),
                field(3,bytes(device)),field(5,observed),field(6,raw[index]),field(7,1),field(8,'RX'),field(10,'received'),field(14,bytes(report))])));
            await until(async()=>Number(await redis.send('EXISTS',[`iot:debug:v4:packet:${node}:${packetIds[index]}`]))===1,'raw packet missing');
        }
        // Parse result arrives before any historical upload and carries only this response's value.
        for(let index=0;index<packetIds.length;index++) {
            const decoded=Buffer.concat([field(1,point),field(2,'temperature'),field(3,'C'),field(4,Buffer.concat([field(1,3),field(4,42+index)]))]);
            socket!.send(envelope(42,Buffer.concat([field(1,bytes(packetIds[index])),field(2,bytes(link)),field(3,bytes(device)),field(5,observed),field(7,1),field(8,'RX'),field(14,bytes(report)),field(16,decoded)])));
            await until(async()=>!!await redis.send('HGET',[`iot:debug:v4:packet:${node}:${packetIds[index]}`,'parsed_json']),'standalone debug parsing missing');
        }
        assert.equal(Number((await db`SELECT count(*)::int AS n FROM device_data WHERE device_id=${device}`)[0].n),0,'debug event wrote history');
        const record = (id: string, additions: Buffer[], sampledAt = observed) => Buffer.concat([
            field(1, bytes(id)), field(2, bytes(device)), field(3, bytes(link)), field(4, protocolNumber),
            field(5, '32'), field(7, 'UP'), field(8, sampledAt), field(12, bytes(model)), ...additions,
        ]);
        const send = (data: Buffer) => socket!.send(envelope(40, field(1, data)));
        {
            const ids = [uuid(), uuid(), uuid()];
            const parts = ids.map((id, index) => record(id, [field(15, bytes(report)), field(16, index), field(17, 3),
                ...(index === 0 ? [values] : [field(14, raw[index - 1]),field(18,bytes(packetIds[index-1]))])]));
            send(parts[2]); send(parts[0]); send(parts[2]);
            const key = `iot:edge:telemetry-upload:${node}:${device}:${report}`;
            await until(async () => Number(await redis.send('HLEN', [key])) >= 4, 'parts were not staged');
            assert.equal((await db`SELECT 1 FROM device_data WHERE device_id=${device}`).length, 0, 'partial upload became history');
            await connect();
            send(parts[1]);
            await until(async () => (await db`SELECT 1 FROM device_data WHERE device_id=${device}`).length === 1, 'assembled history missing');
            for (const part of parts) send(part);
            await until(async () => (await redis.send('HGET', [key, 'done'])) === '1', 'completed upload not reclaimed');
            assert(Number(await redis.send('TTL', [key])) > 0);
            assert.equal(await redis.send('HLEN', [key]), 1);
        }
        // 后续采样作为处理屏障，确保前面的重复上传已经经过历史消费。
        send(record(uuid(), [values, ...raw.map(value => field(14, value)), ...raw.map(() => field(18,bytes(uuid())))], observed + 1));
        await until(async () => (await db`SELECT 1 FROM device_data WHERE device_id=${device}
            AND report_time=to_timestamp(${observed + 1}/1000.0)`).length === 1, 'history replay barrier missing');
        const rows = await db`SELECT raw_payload_hex,data,protocol,source,model_id FROM device_data
            WHERE device_id=${device} AND report_time=to_timestamp(${observed}/1000.0)`;
        assert.equal(rows.length, 1);
        assert.deepEqual(rows[0].raw_payload_hex, raw.map(value => value.toString('hex').toUpperCase()));
        assert.equal(rows[0].data.values[point].value, 42);
        assert.equal(rows[0].data.values[point].value_type, 'number');
        assert.equal(rows[0].model_id, model);
        assert.equal(rows[0].protocol, protocol);
        assert.equal(rows[0].source, 'edge');
        for (const packetId of packetIds) {
            const key=`iot:debug:v4:packet:${node}:${packetId}`;
            assert.equal(await redis.send('HGET',[key,'storage_status']),null);
            assert.equal(JSON.parse(await redis.send('HGET',[key,'parsed_json']) as string).values[point].value,42+packetIds.indexOf(packetId),'history overwrote packet result');
            assert.equal(await redis.send('HGET',[key,'payload_hex']),raw[packetIds.indexOf(packetId)].toString('hex').toUpperCase());
            assert.equal(await redis.send('HGET',[key,'history_id']),null);
        }
        const rounds = await liveSnapshot<DebugRound[]>(`/v1/device/events?debugDeviceId=${device}`, 'packets');
        const round = rounds.find(entry => entry.id === report);
        assert(round);
        assert.equal(round.packets.length, 2, 'history updates must not duplicate packet rows');
        for (const packet of round.packets) {
            assert.equal(packet.edge_node_id,node);
            assert.equal(packet.edge_node_name,'调试边缘节点');
        }
        const linkRounds = await liveSnapshot<DebugRound[]>(`/v1/link/events?keyword=${link}&debugLinkId=${link}`, 'packets');
        const linkRound = linkRounds.find(entry => entry.id === report);
        assert.equal(linkRound,undefined,'device-only debugging must not add a link acquisition');
        assert.equal(round.history_id,undefined);
        assert.equal(round.storage_status,undefined);
        assert.equal(round.parsed_json,undefined);
        socket!.send(envelope(42,Buffer.concat([field(1,bytes(uuid())),field(2,bytes(link)),field(3,bytes(device)),
            field(5,Date.now()),field(7,1),field(8,'RX'),field(14,bytes(report)),field(15,'success')])));
        await until(async()=>await redis.send('HGET',[`iot:debug:v4:acquisition:${report}`,'state'])==='success','round completion missing');
        socket!.send(envelope(42,Buffer.concat([field(1,bytes(uuid())),field(2,bytes(link)),field(3,bytes(device)),
            field(5,Date.now()),field(7,1),field(8,'RX'),field(14,bytes(report)),field(15,'failed')])));
        await Bun.sleep(100);
        assert.equal(await redis.send('HGET',[`iot:debug:v4:acquisition:${report}`,'state']),'success','late completion regressed round state');
        console.log(`PASS ${protocol}: WebSocket → projection → one historical record, raw array and normalized values; round state and packet identities`);
    }
} finally {
    socket?.close();
    await redis.send('DEL', [uploadKey, `iot:edge:auth:${imei}`, `iot:edge:metadata:${node}`]);
    for (const device of devices) await db`UPDATE device SET deleted_at=NOW() WHERE id=${device}`;
    await db.close();
    redis.close();
}
