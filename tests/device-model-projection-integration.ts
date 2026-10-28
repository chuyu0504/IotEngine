import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import net from 'node:net';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription, type SnapshotSubscription } from './sse-fixture';

// Run through the disposable CTest fixture, just like industrial-edge-integration.
// Local TCP peers and a captured edge WebSocket exercise the real HTTP model
// service, SQL loader, Redis publication and Collector without physical firmware.
const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const platform = '00000000-0000-7000-8000-000000000001';
const tag = `dm-${crypto.randomUUID()}`;
const node = crypto.randomUUID();
const protocols = ['SL651', 'Modbus', 'S7', 'MC', 'FINS', 'DLT645', 'MQTT'] as const;
type Protocol = typeof protocols[number];
type Json = Record<string, any>;
type Hash = Record<string, string>;
type Wire = Map<number, bigint | Buffer>;
const protocolNumber: Partial<Record<Protocol, number>> = { SL651: 1, Modbus: 2, S7: 3, MC: 4, FINS: 5, DLT645: 6 };
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ iss: 'iot-engine', aud: 'iot-engine-web', sub: admin, user_id: admin, username: 'admin', token_type: 'access', iat: now, exp: now + 3600 })}`;
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
const bytes = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');
const sha = (value: Buffer) => createHash('sha256').update(value).digest();
const text = (value: Wire, key: number) => (value.get(key) as Buffer | undefined)?.toString() ?? '';
const number = (value: Wire, key: number) => Number(value.get(key) ?? 0n);
const signed = (value: Wire, key: number) => Number(BigInt.asIntN(32, (value.get(key) as bigint | undefined) ?? 0n));

async function http(method: string, path: string, body?: unknown) {
    const response = await fetch(apiBase + path, {
        method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(15000),
    });
    const reply = await response.text();
    assert.equal(response.status, 200, `${method} ${path}: ${reply}`);
    const result = JSON.parse(reply);
    assert.equal(result.code, 0, reply);
    return result.data;
}
async function until(check: () => Promise<boolean>, reason: string) {
    const deadline = Date.now() + 25000;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(50);
    }
    throw Error(reason);
}
function integer(input: number | bigint) {
    let value = BigInt(input);
    const result: number[] = [];
    do { let byte = Number(value & 127n); value >>= 7n; if (value) byte |= 128; result.push(byte); } while (value);
    return Buffer.from(result);
}
function field(tag: number, value: number | bigint | string | Buffer): Buffer {
    if (typeof value === 'number' || typeof value === 'bigint') return Buffer.concat([integer(tag * 8), integer(value)]);
    const buffer = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag * 8 + 2), integer(buffer.length), buffer]);
}
// Keep exact encoded field spans as well as decoded values, so hash verification
// does not rely on a second protobuf implementation or JSON reserialization.
function fields(buffer: Buffer) {
    let offset = 0;
    const result: { tag: number; value: bigint | Buffer; raw: Buffer }[] = [];
    const read = () => {
        let value = 0n;
        for (let shift = 0n; shift < 70n; shift += 7n) {
            const byte = buffer[offset++];
            assert(byte !== undefined, 'truncated protobuf varint');
            value |= BigInt(byte & 127) << shift;
            if (!(byte & 128)) return value;
        }
        throw Error('oversized protobuf varint');
    };
    while (offset < buffer.length) {
        const start = offset, key = Number(read()), tag = key >> 3, wire = key & 7;
        let value: bigint | Buffer;
        if (wire === 0) value = read();
        else {
            assert([1, 2, 5].includes(wire), `unsupported protobuf wire ${wire}`);
            const length = wire === 2 ? Number(read()) : wire === 1 ? 8 : 4;
            assert(offset + length <= buffer.length, 'truncated protobuf field');
            value = buffer.subarray(offset, offset + length); offset += length;
        }
        result.push({ tag, value, raw: buffer.subarray(start, offset) });
    }
    return result;
}
const decode = (buffer: Buffer): Wire => new Map(fields(buffer).map(({ tag, value }) => [tag, value]));
const without = (buffer: Buffer, tags: number[]) => Buffer.concat(fields(buffer).filter(value => !tags.includes(value.tag)).map(value => value.raw));
function imei() {
    const prefix = `99${String(Math.floor(Math.random() * 1e12)).padStart(12, '0')}`;
    let sum = 0;
    for (let i = 0; i < 14; ++i) { let digit = Number(prefix[i]) * (i % 2 ? 2 : 1); if (digit > 9) digit -= 9; sum += digit; }
    return prefix + String((10 - sum % 10) % 10);
}
const identity = imei();
interface Fixture {
    protocol: Protocol;
    explicit: boolean;
    model: string;
    config: Json;
    validConfig: Json;
    collector: string;
    collectorLink: string;
    edge?: string;
    edgeLink?: string;
    target: string;
}
const fixtures: Fixture[] = [];
const modelIds: string[] = [];
const linkIds: string[] = [];
const deviceIds: string[] = [];
// The Collector may attempt connections, but a local rejecting endpoint keeps
// this projection-only test independent of installed hardware and fixed ports.
const endpoint = net.createServer(socket => socket.destroy());
let endpointListening = false;
let socket: WebSocket | undefined;
let stream: SnapshotSubscription | undefined;
let socketFailure: Error | undefined;
let epoch = 0n, sequence = 1n;
const envelope = (tag: number, payload: Buffer) => Buffer.concat([
    field(1, 6), field(2, bytes(crypto.randomUUID())), field(3, bytes(node)), field(4, bytes(platform)),
    field(5, epoch), field(6, Date.now()), field(8, sequence++), field(tag, payload),
]);
interface Snapshot { revision: bigint; begin?: Buffer; items: Buffer[]; commit?: Buffer }
let snapshots = new Map<string, Snapshot>();
const edgeCommands = new Map<string, Buffer>();

function configuration(protocol: Protocol, explicit: boolean) {
    // IDs and addresses both descend while source array order remains intact.
    const ids = Array.from({ length: 3 }, () => crypto.randomUUID()).sort().reverse();
    const points = ids.slice(0, 2).map((id, index) => ({ id, name: `${protocol} ${index} \"quote\", ] \\ slash`, unit: 'V' }));
    const common = explicit ? { readInterval: 17, commandFastReadDuration: 23, commandFastReadInterval: 2, storagePolicy: 'report' } : { storagePolicy: 'report' };
    let config: Json;
    switch (protocol) {
        case 'Modbus':
            config = { ...common, ...(explicit ? { byteOrder: 'LITTLE_ENDIAN', packet: { mergeGap: '7', maxQuantity: '99' } } : {}),
                registers: points.map((point, index) => ({ ...point, registerType: 'HOLDING_REGISTER', dataType: 'UINT16', address: index ? '3' : '100',
                    ...(explicit ? { dataType: 'UINT32', quantity: '2', decimals: 3, writable: true, ...(index ? {} : { byteOrder: 'BIG_ENDIAN' }) } : {}) })) };
            break;
        case 'S7':
            config = { ...common, plcModel: 'S7-1200', connection: explicit ? { mode: 'TSAP', connectionType: 'OP', rack: '2', slot: '3', localTSAP: '0200', remoteTSAP: '0203' } : {},
                areas: points.map((point, index) => ({ ...point, area: 'DB', dbNumber: '1', start: index ? '2' : '100',
                    ...(explicit ? { size: '2', dataType: 'UINT16', decimals: 3, writable: true } : {}) })) };
            break;
        case 'MC':
        case 'FINS':
            config = { ...common, connection: explicit ? protocol === 'MC'
                ? { frame: '4E', network: '2', station: '3', moduleIo: '1000', multidrop: '4', monitoringTimer: '32' }
                : { sourceNetwork: '1', sourceNode: '10', sourceUnit: '2', destinationNetwork: '3', destinationNode: '20', destinationUnit: '4' } : {},
                points: points.map((point, index) => ({ ...point, area: 'D', address: index ? '3' : '100',
                    ...(explicit ? { dataType: 'UINT32', bit: 0, byteOrder: protocol === 'MC' ? 'BIG_ENDIAN' : 'LITTLE_ENDIAN', decimals: 3, writable: true } : {}) })) };
            break;
        case 'DLT645':
            config = { ...common, connection: explicit ? { version: '1997', wakeupBytes: '2', writePassword: '00123456', operatorCode: '00000001' } : {},
                points: points.map((point, index) => ({ ...point, identifier: explicit ? index ? '9011' : '9010' : index ? '00010000' : '00020000',
                    ...(explicit ? { dataType: 'BCD', length: index ? '6' : '1', digits: index ? '3' : '0', decimals: 3, writable: true } : {}) })) };
            break;
        case 'SL651': {
            const elements = points.map((point, index) => ({ ...point, ...(index ? { id: ids[2] } : {}), guideHex: index ? '3912' : '3812', encode: 'BCD', length: 2, digits: 2,
                ...(explicit ? { positionMode: 'OFFSET', byteOffset: index ? 2 : 12 } : {}) }));
            config = { ...common, ...(explicit ? { responseMode: 'M3' } : {}), funcs: [
                { funcCode: '38', name: 'control', dir: 'DOWN', elements,
                    responseElements: [{ ...elements[0], name: 'response to control' }] },
                { funcCode: '32', name: 'report', dir: 'UP', elements: [{ ...elements[1], id: ids[2], name: 'report only' }] },
                { funcCode: '35', name: 'additional report', dir: 'UP', elements: [{ ...elements[1], name: 'report only' }] },
                { funcCode: '39', name: 'later control with identical IDs', dir: 'DOWN', elements: structuredClone(elements) },
            ] };
            break;
        }
        case 'MQTT':
            config = { ...common, topic: `${tag}/telemetry`, payloadFormat: 'json', identitySource: 'payload', deviceCodeField: '/code',
                ...(explicit ? { qos: 1, recordsPath: '/records', timeField: '/time', timeFormat: 'unix_ms' } : {}),
                points: points.map((point, index) => ({ ...point, field: `/values/${index ? 'first' : 'last'}`, dataType: 'DOUBLE', ...(explicit ? { writable: false } : {}) })) };
            break;
    }
    // HTTP deliberately requires some fields which historical SQL configurations
    // may omit. Create through HTTP with valid input, then seed that legacy shape
    // through SQL. GET must retain it, rather than silently materialize defaults.
    const valid = structuredClone(config);
    // The HTTP contract accepts numeric strings only for its documented subset;
    // legacy SQL shapes below deliberately retain strings for loader regression.
    const numericKeys: Record<string, true> = { address: true, quantity: true, dbNumber: true, start: true,
        startBit: true, size: true, rack: true, slot: true, network: true, station: true, moduleIo: true,
        multidrop: true, monitoringTimer: true, sourceNetwork: true, sourceNode: true, sourceUnit: true,
        destinationNetwork: true, destinationNode: true, destinationUnit: true, wakeupBytes: true, length: true, digits: true };
    const numericInput = (value: Json) => {
        for (const [key, item] of Object.entries(value)) {
            if (numericKeys[key] && typeof item === 'string') value[key] = Number(item);
            else if (Array.isArray(item)) for (const child of item) numericInput(child);
            else if (item && typeof item === 'object') numericInput(item);
        }
    };
    numericInput(valid);
    if (protocol === 'Modbus') {
        valid.byteOrder ??= 'BIG_ENDIAN';
        for (const point of valid.registers) { point.dataType ??= 'UINT16'; point.quantity ??= 1; }
    } else if (protocol === 'S7') {
        for (const point of valid.areas) point.size ??= 1;
    } else if (['MC', 'FINS', 'DLT645'].includes(protocol)) {
        for (const point of valid.points) { point.dataType ??= protocol === 'DLT645' ? 'BCD' : 'UINT16'; if (protocol === 'DLT645') point.length ??= 4; }
    } else if (protocol === 'SL651') valid.responseMode ??= 'M1';
    return { config, valid };
}
function elements(fixture: Fixture): { point: Json; key: string; functionCode?: string; direction?: string; response: boolean }[] {
    const { protocol, config } = fixture;
    if (protocol === 'SL651') return config.funcs.flatMap((fn: Json) => [
        ...fn.elements.map((point: Json) => ({ point, key: `element:${point.id}`, functionCode: fn.funcCode, direction: fn.dir, response: false })),
        ...(fn.responseElements ?? []).map((point: Json) => ({ point, key: `response:${point.id}`, functionCode: fn.funcCode, direction: fn.dir, response: true })),
    ]);
    return config[protocol === 'Modbus' ? 'registers' : protocol === 'S7' ? 'areas' : 'points'].map((point: Json) => ({ point, key: `element:${point.id}`, response: false }));
}
async function hash(key: string): Promise<Hash> { return await redis.send('HGETALL', [key]) as Hash; }
async function list(key: string): Promise<string[]> { return await redis.send('LRANGE', [key, '0', '-1']) as string[]; }
const elementFields = ['config_key', 'id', 'name', 'unit', 'data_type', 'byte_order', 'register_type', 'mqtt_field',
    'address', 'quantity', 'decimals', 'writable', 'area', 'db_number', 'start', 'start_bit', 'size',
    'function_code', 'direction', 'guide_hex', 'position_mode', 'byte_offset', 'encoding', 'length', 'digits', 'response_element'];
async function elementHash(key: string, ordinal: number): Promise<Hash> {
    const values = await redis.send('HMGET', [key, ...elementFields.map(name => `${ordinal}:${name}`)]) as (string | null)[];
    assert(values.every(value => value !== null), `${key}: missing source occurrence ${ordinal}`);
    return Object.fromEntries(elementFields.map((name, index) => [name, values[index]!]));
}
const prefix = (version: string) => `iot:config:runtime:${version}`;
const deviceKey = (version: string, id: string) => `${prefix(version)}:device:${id}`;

async function waitForCollector(present: (fixture: Fixture) => boolean) {
    let version = '';
    await until(async () => {
        version = await redis.get('iot:config:runtime:active-version') ?? '';
        if (!version) return false;
        for (const fixture of fixtures) {
            const row = await hash(deviceKey(version, fixture.collector));
            if (present(fixture)) {
                if (row.model_id !== fixture.model || row.read_interval !== String(fixture.config.readInterval ?? 300)) return false;
                if (!row.calculation_config || JSON.stringify(JSON.parse(row.calculation_config)) !== JSON.stringify((await db`SELECT config FROM protocol_config WHERE id=${fixture.model}`)[0].config)) return false;
            } else if (Object.keys(row).length) return false;
        }
        const workers = await redis.send('KEYS', ['iot:runtime:collector:*']) as string[];
        return workers.length > 0 && (await Promise.all(workers.map(key => redis.send('HGET', [key, 'version'])))).every(value => value === version);
    }, 'Collector workers did not apply the complete expected device model snapshot');
    return version;
}
async function assertCollector(version: string, present: (fixture: Fixture) => boolean) {
    const expectedDevices = fixtures.filter(present).sort((a, b) => a.collectorLink.localeCompare(b.collectorLink) || a.collector.localeCompare(b.collector)).map(fixture => fixture.collector);
    const actualDevices = await list(`${prefix(version)}:devices`);
    assert.deepEqual(actualDevices.filter(id => deviceIds.includes(id)), expectedDevices, 'Collector device order must follow link_id,id, not fixture insertion order');
    for (const fixture of fixtures) {
        const key = deviceKey(version, fixture.collector);
        if (!present(fixture)) {
            assert.equal(Number(await redis.send('EXISTS', [key, `${key}:elements`])), 0, `${fixture.protocol}: disabled Collector device survived`);
            continue;
        }
        const row = await hash(key);
        assert.equal(row.protocol, fixture.protocol);
        assert.equal(row.timezone, fixture.explicit ? '+00:00' : '+08:00');
        assert.equal(row.online_timeout, fixture.explicit ? '123' : '300');
        assert.equal(row.heartbeat_mode, 'OFF'); assert.equal(row.heartbeat_hex, '');
        assert.equal(row.registration_mode, 'OFF'); assert.equal(row.registration_hex, '');
        assert.equal(row.link_id, fixture.collectorLink);
        assert.equal(row.model_id, fixture.model);
        assert.equal(row.command_fast_read_duration, String(fixture.config.commandFastReadDuration ?? 60));
        assert.equal(row.command_fast_read_interval, String(fixture.config.commandFastReadInterval ?? 1));
        assert.deepEqual(JSON.parse(row.calculation_config), fixture.config, 'runtime config text must remain original JSON');
        const expected = elements(fixture);
        assert.deepEqual(await list(`${key}:elements`), expected.map(value => value.key), `${fixture.protocol}: Collector reordered the source array or collapsed a response ID`);
        for (const [ordinal, element] of expected.entries()) {
            const value = await elementHash(`${key}:element:${element.key}`, ordinal), point = element.point;
            assert.equal(value.id, point.id); assert.equal(value.config_key, element.key); assert.equal(value.name, point.name);
            assert.equal(value.unit, point.unit);
            if (fixture.protocol === 'MQTT') {
                assert.equal(value.mqtt_field, point.field); assert.equal(value.data_type, point.dataType); assert.equal(value.size, '65535');
            } else if (fixture.protocol === 'SL651') {
                assert.equal(value.function_code, element.functionCode); assert.equal(value.direction, element.direction);
                assert.equal(value.response_element, element.response ? '1' : '0');
                assert.equal(value.writable, element.direction === 'DOWN' && !element.response ? '1' : '0');
                assert.equal(value.encoding, point.encode); assert.equal(value.guide_hex, point.guideHex);
                assert.equal(value.position_mode, point.positionMode ?? 'GUIDE'); assert.equal(value.byte_offset, String(point.byteOffset ?? 0));
            } else {
                assert.equal(value.data_type, point.dataType ?? (fixture.protocol === 'S7' ? 'UINT8' : fixture.protocol === 'DLT645' ? 'BCD' : 'UINT16'));
                assert.equal(value.byte_order, fixture.protocol === 'S7' ? '' : point.byteOrder ?? (fixture.protocol === 'MC' ? 'LITTLE_ENDIAN' : fixture.config.byteOrder ?? 'BIG_ENDIAN'));
                assert.equal(value.decimals, String(point.decimals ?? -1));
                assert.equal(value.writable, point.writable ? '1' : '0');
                if (fixture.protocol === 'Modbus') { assert.equal(value.quantity, String(point.quantity ?? 1)); assert.equal(value.address, String(point.address)); }
                if (fixture.protocol === 'S7') { assert.equal(value.size, String(point.size ?? 1)); assert.equal(value.start, String(point.start)); }
                if (fixture.protocol === 'DLT645') { assert.equal(value.encoding, point.dataType ?? 'BCD'); assert.equal(value.length, String(point.length ?? 4)); assert.equal(value.digits, String(point.digits ?? 2)); assert.equal(value.guide_hex, point.identifier); }
            }
        }
        const expectedPoints = new Map<string, Json>();
        for (const element of expected) {
            if ((fixture.protocol === 'SL651' && (element.direction !== 'UP' || element.response)) || element.point.encode === 'JPEG') continue;
            expectedPoints.set(element.point.id, element.point);
        }
        const configuredPoints = (await list(`${prefix(version)}:realtime-device:${fixture.collector}:points`)).map(value => value.split('\0'));
        assert.deepEqual(configuredPoints, [...expectedPoints.values()].map(point => [point.id, point.name, point.unit]),
            `${fixture.protocol}: realtime points must keep first ID position, last metadata and original external IDs`);
        assert.equal(new Set(configuredPoints.map(point => point[0])).size, configuredPoints.length, 'configured realtime point IDs are not unique');
        if (fixture.protocol === 'Modbus') { assert.equal(row.modbus_merge_gap, String(fixture.config.packet?.mergeGap ?? 100)); assert.equal(row.modbus_max_quantity, String(fixture.config.packet?.maxQuantity ?? 125)); }
        if (fixture.protocol === 'S7') { assert.equal(row.s7_local_tsap, fixture.config.connection.localTSAP ?? '0100'); assert.equal(row.s7_remote_tsap, fixture.config.connection.remoteTSAP ?? '0101'); }
        if (fixture.protocol === 'MC') { assert.equal(row.mc_frame, fixture.config.connection.frame ?? '3E'); assert.equal(row.mc_station, String(fixture.config.connection.station ?? 255)); }
        if (fixture.protocol === 'DLT645') { assert.equal(row.dlt645_version, fixture.config.connection.version ?? '2007'); assert.equal(row.dlt645_wakeup_bytes, String(fixture.config.connection.wakeupBytes ?? 4)); }
    }
}

async function connect() {
    epoch = 0n; sequence = 1n; socketFailure = undefined;
    socket = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect'); socket.binaryType = 'arraybuffer';
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(Error('edge Hello timeout')), 10000);
        socket!.onopen = () => socket!.send(envelope(20, Buffer.concat([field(1, identity), field(2, 'projection-fixture'), field(3, '0.3.65'), field(23, 1), field(37, 1)])));
        socket!.onerror = () => { socketFailure = Error('edge WebSocket error'); clearTimeout(timer); reject(socketFailure); };
        socket!.onclose = () => { socketFailure = Error('edge WebSocket closed unexpectedly'); clearTimeout(timer); reject(socketFailure); };
        socket!.onmessage = event => {
            try {
                const message = decode(Buffer.from(event.data as ArrayBuffer));
                if (message.has(21)) { epoch = message.get(5) as bigint; clearTimeout(timer); socket!.send(envelope(26, field(7, Buffer.from([1, 2, 3, 4, 5, 6])))); resolve(); }
                if (message.has(80)) socket!.send(envelope(81, message.get(80) as Buffer));
                if (message.has(50)) {
                    const command = message.get(50) as Buffer;
                    edgeCommands.set((decode(command).get(1) as Buffer).toString('hex'), command);
                }
                for (const tag of [30, 31, 32]) {
                    if (!message.has(tag)) continue;
                    const payload = message.get(tag) as Buffer, revision = decode(payload).get(1) as bigint;
                    const snapshot = snapshots.get(String(revision)) ?? { revision, items: [] };
                    if (tag === 30) { snapshot.begin = payload; snapshot.items = []; snapshot.commit = undefined; }
                    if (tag === 31) snapshot.items.push(payload);
                    if (tag === 32) snapshot.commit = payload;
                    snapshots.set(String(revision), snapshot);
                }
            } catch (error) { socketFailure = error as Error; clearTimeout(timer); reject(error); }
        };
    });
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'protocols' ? 'DLT645'`).length > 0, 'edge capabilities were not persisted');
}
async function disconnect() {
    const current = socket; socket = undefined;
    if (!current || current.readyState === WebSocket.CLOSED) return;
    current.onclose = null;
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(Error('edge close timeout')), 5000);
        current.addEventListener('close', () => { clearTimeout(timer); resolve(); }, { once: true }); current.close();
    });
    await until(async () => Number(await redis.send('EXISTS', [`iot:edge:session:${node}`])) === 0, 'edge session lease survived disconnect');
}
async function captured(revision: bigint) {
    await until(async () => {
        if (socketFailure) throw socketFailure;
        const snapshot = snapshots.get(String(revision));
        return !!snapshot?.begin && !!snapshot.commit && snapshot.items.length === number(decode(snapshot.begin), 2);
    }, `edge snapshot ${revision} was not delivered in full`);
    const snapshot = snapshots.get(String(revision))!;
    const begin = decode(snapshot.begin!), commit = decode(snapshot.commit!);
    const digests = snapshot.items.map((wire, index) => {
        const item = decode(wire);
        assert.equal(item.get(1), revision); assert.equal(number(item, 2), index);
        assert.deepEqual(item.get(4), sha(without(wire, [4])), `item ${index} SHA must cover exact protobuf bytes including revision`);
        assert(fields(wire).every(value => [1, 2, 3, 4, 10, 11, 12, 13, 14, 15, 16, 17].includes(value.tag)), 'new ConfigItem wire tag appeared');
        return item.get(4) as Buffer;
    });
    assert.deepEqual(begin.get(3), sha(Buffer.concat(digests)));
    assert.deepEqual(commit.get(2), begin.get(3)); assert.equal(commit.get(1), revision);
    const [saved] = await db`SELECT sha256,item_count FROM edge_config_revision WHERE node_id=${node} AND revision=${String(revision)}::bigint`;
    assert(saved); assert.equal(saved.sha256, (begin.get(3) as Buffer).toString('hex')); assert.equal(Number(saved.item_count), snapshot.items.length);
    return snapshot;
}
async function sync() {
    await http('POST', `/v1/edge/${node}/sync`);
    const [row] = await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`;
    return captured(BigInt(row.revision));
}
function payloads(snapshot: Snapshot, tag: number, device?: string) {
    return snapshot.items.flatMap(wire => {
        const item = decode(wire);
        if (!item.has(tag)) return [];
        const value = decode(item.get(tag) as Buffer);
        return !device || (value.get(1) as Buffer)?.equals(bytes(device)) ? [value] : [];
    });
}
function assertEdge(snapshot: Snapshot, enabled: (fixture: Fixture) => boolean, linkEnabled = true) {
    const edgeFixtures = fixtures.filter(fixture => fixture.edge).sort((a, b) => a.edgeLink!.localeCompare(b.edgeLink!) || a.edge!.localeCompare(b.edge!));
    assert.deepEqual(payloads(snapshot, 11).map(value => (value.get(1) as Buffer).toString('hex')), edgeFixtures.map(fixture => bytes(fixture.edge!).toString('hex')), 'edge device order must follow shared SQL ordering');
    assert.equal(payloads(snapshot, 11).length, 12, 'MQTT must remain Collector-only');
    for (const fixture of edgeFixtures) {
        const [device] = payloads(snapshot, 11, fixture.edge), [link] = payloads(snapshot, 10, fixture.edgeLink);
        assert(device); assert(link);
        assert.equal(number(device, 5), protocolNumber[fixture.protocol]);
        assert.deepEqual(device.get(2), bytes(fixture.edgeLink!));
        assert.equal(number(device, 24), Number(enabled(fixture)), `${fixture.protocol}: disabled device was omitted or left enabled`);
        assert.equal(number(link, 9), Number(linkEnabled || fixture.explicit));
        assert.equal(text(link, 10), 'lo'); assert.equal(number(link, 5), protocolNumber[fixture.protocol]);
        assert.equal(number(device, 7), 0, 'legacy io_interval_ms must stay zero');
        assert.equal(text(device, 6), fixture.explicit ? '+00:00' : '+08:00');
        assert.equal(number(device, 9), fixture.explicit ? 123 : 300);
        assert.equal((device.get(22) as Buffer | undefined)?.length ?? 0, 0, 'ineligible TCP Client/SL651 links must not export server-only heartbeat packets');
        assert.equal(number(device, 8), fixture.config.readInterval ?? 300);
        assert.equal(number(device, 20), fixture.config.commandFastReadDuration ?? 60);
        assert.equal(number(device, 21), fixture.config.commandFastReadInterval ?? 1);
        assert.equal(device.has(23), false, 'reserved registration_payload field was resurrected');
        const elementTag = fixture.protocol === 'SL651' ? 15 : fixture.protocol === 'Modbus' ? 12 : fixture.protocol === 'S7' ? 13 : 17;
        const values = payloads(snapshot, elementTag, fixture.edge), expected = elements(fixture);
        assert.deepEqual(values.map(value => text(value, fixture.protocol === 'SL651' ? 3 : 2)), expected.map(value => value.point.id), `${fixture.protocol}: wire reordered elements`);
        values.forEach((value, index) => {
            const element = expected[index], point = element.point;
            assert.equal(text(value, fixture.protocol === 'SL651' ? 4 : 3), point.name);
            if (fixture.protocol === 'SL651') {
                assert.equal(text(value, 2), element.functionCode); assert.equal(number(value, 10), Number(element.response));
                assert.equal(number(value, 11), Number(element.direction === 'DOWN' && !element.response));
                assert.equal(text(value, 6), point.encode); assert.equal(number(value, 7), point.length); assert.equal(number(value, 8), point.digits);
                assert.deepEqual(value.get(9), Buffer.from(point.guideHex, 'hex'));
                assert.equal(number(value, 12), Number(point.positionMode === 'OFFSET')); assert.equal(number(value, 13), point.byteOffset ?? 0);
            } else {
                const modbus = fixture.protocol === 'Modbus', s7 = fixture.protocol === 'S7';
                assert.equal(text(value, s7 ? 10 : modbus ? 6 : 5), point.dataType ?? (s7 ? 'UINT8' : fixture.protocol === 'DLT645' ? 'BCD' : 'UINT16'));
                assert.equal(signed(value, s7 ? 12 : 11), point.decimals ?? -1);
                assert.equal(number(value, s7 ? 13 : modbus ? 12 : 15), Number(!!point.writable));
                assert.equal((value.get(s7 ? 11 : 10) as Buffer).readDoubleLE(), 1, 'wire scale compatibility must remain 1');
                if (!s7) assert.equal(text(value, modbus ? 7 : 9), point.byteOrder ?? (fixture.protocol === 'MC' ? 'LITTLE_ENDIAN' : fixture.config.byteOrder ?? 'BIG_ENDIAN'));
                if (modbus) { assert.equal(number(value, 8), Number(point.address)); assert.equal(number(value, 9), Number(point.quantity ?? 1)); }
                else if (s7) { assert.equal(number(value, 7), Number(point.start)); assert.equal(number(value, 9), Number(point.size ?? 1)); }
                else if (fixture.protocol === 'DLT645') { assert.equal(text(value, 12), point.identifier); assert.equal(number(value, 13), Number(point.length ?? 4)); assert.equal(number(value, 14), Number(point.digits ?? 2)); }
                else assert.equal(number(value, 7), Number(point.address));
            }
        });
        if (fixture.protocol === 'Modbus') { assert.equal(number(device, 12), Number(fixture.config.packet?.mergeGap ?? 100)); assert.equal(number(device, 13), Number(fixture.config.packet?.maxQuantity ?? 125)); }
        if (fixture.protocol === 'S7') { assert.equal(text(device, 18), fixture.config.connection.localTSAP ?? '0100'); assert.equal(text(device, 19), fixture.config.connection.remoteTSAP ?? '0101'); }
        if (fixture.protocol === 'SL651') {
            assert.equal(number(device, 25), Number((fixture.config.responseMode ?? 'M1').slice(1)));
            assert.deepEqual(payloads(snapshot, 14, fixture.edge).map(value => [text(value, 2), text(value, 4)]), fixture.config.funcs.map((fn: Json) => [fn.funcCode, fn.dir]));
        }
        if (['MC', 'FINS', 'DLT645'].includes(fixture.protocol)) {
            const connection = decode(device.get(27) as Buffer), source = fixture.config.connection;
            if (fixture.protocol === 'MC') { assert.equal(number(connection, 1), Number(source.frame === '4E')); assert.equal(number(connection, 2), Number(source.network ?? 0)); assert.equal(number(connection, 3), Number(source.station ?? 255)); assert.equal(number(connection, 4), Number(source.moduleIo ?? 1023)); }
            if (fixture.protocol === 'FINS') { assert.equal(number(connection, 8), Number(source.sourceNode ?? 0)); assert.equal(number(connection, 11), Number(source.destinationNode ?? 0)); }
            if (fixture.protocol === 'DLT645') { assert.equal(number(connection, 13), Number(source.version ?? 2007)); assert.equal(number(connection, 14), Number(source.wakeupBytes ?? 4)); if (source.writePassword) assert.deepEqual(connection.get(15), Buffer.from(source.writePassword, 'hex')); }
        }
    }
}
async function assertHttp() {
    for (const fixture of fixtures) {
        const dto = await http('GET', `/v1/protocol/configs/${fixture.model}`);
        assert.equal(dto.protocol, fixture.protocol); assert.deepEqual(dto.config, fixture.config, 'HTTP injected runtime defaults into the saved DTO');
        assert.deepEqual((await db`SELECT config FROM protocol_config WHERE id=${fixture.model}`)[0].config, fixture.config);
    }
}
// Apply only actual SSE payloads, never fetch business state periodically.
const realtime = new Map<string, Json>();
async function receiveRealtime(reached: () => boolean) {
    const deadline = Date.now() + 25000;
    while (!reached()) {
        assert(Date.now() < deadline, 'SSE did not expose affected model configuration');
        const event = await stream!.next(Math.max(1, deadline - Date.now()));
        assert.notEqual(event.event, 'error', event.data);
        if (event.event !== 'realtime') continue;
        const payload = JSON.parse(event.data); assert.equal(payload.code, 0, event.data);
        if (payload.delta) { for (const id of payload.delta.remove) realtime.delete(id); for (const row of payload.delta.upsert) realtime.set(row.id, row); }
        else { realtime.clear(); for (const row of payload.data.list) realtime.set(row.id, row); }
    }
}
function slCrc(frame: Buffer) {
    let crc = 0xffff;
    for (const byte of frame) {
        crc ^= byte;
        for (let bit = 0; bit < 8; ++bit) crc = crc & 1 ? (crc >> 1) ^ 0xa001 : crc >> 1;
    }
    return crc;
}
function slReport(functionCode: number, serial: number, timestamp: number, value: string) {
    const local = new Date(timestamp + 8 * 3600000);
    const bcd = (value: number) => Number.parseInt(String(value).padStart(2, '0'), 16);
    const body = Buffer.from([serial >> 8, serial & 255, bcd(local.getUTCFullYear() % 100), bcd(local.getUTCMonth() + 1),
        bcd(local.getUTCDate()), bcd(local.getUTCHours()), bcd(local.getUTCMinutes()), bcd(local.getUTCSeconds()),
        0x39, 0x12, ...Buffer.from(value.replace('.', ''), 'hex')]);
    const frame = Buffer.from([0x7e, 0x7e, 1, 0, 0, 0, 0, 1, 0, 0, functionCode, body.length >> 8, body.length & 255, 2, ...body, 3]);
    const crc = slCrc(frame);
    return Buffer.concat([frame, Buffer.from([crc >> 8, crc & 255])]);
}
async function assertRepeatedSl651(fixture: Fixture, snapshot: Snapshot) {
    const [link] = await db`SELECT endpoint FROM link WHERE id=${fixture.collectorLink}`;
    const shared = fixture.config.funcs.find((fn: Json) => fn.funcCode === '32').elements[0];
    assert.equal(fixture.config.funcs.find((fn: Json) => fn.funcCode === '35').elements[0].id, shared.id);
    const latestKey = `iot:v2:device:${fixture.collector}:latest`;
    await until(async () => {
        const raw = await redis.send('HGET', [latestKey, shared.id]);
        return !!raw && JSON.parse(String(raw)).id === shared.id;
    }, 'shared point metadata was not projected under its original ID');
    let peer: net.Socket | undefined;
    let received = Buffer.alloc(0);
    let failure: Error | undefined;
    try {
        await until(async () => {
            const candidate = net.createConnection({ host: '127.0.0.1', port: Number(link.endpoint.port) });
            const connected = await new Promise<boolean>(resolve => {
                candidate.once('connect', () => resolve(true)); candidate.once('error', () => resolve(false));
            });
            if (!connected) { candidate.destroy(); return false; }
            peer = candidate;
            peer.on('data', data => { received = Buffer.concat([received, data]); });
            peer.on('error', error => { failure = error; });
            return true;
        }, 'default SL651 Collector TCP listener did not start');
        const baseTime = Math.floor((Date.now() - 60000) / 1000) * 1000;
        const samples = [
            { code: 0x32, time: baseTime, value: '12.34', latest: '12.34', latestTime: baseTime },
            { code: 0x35, time: baseTime, value: '56.78', latest: '56.78', latestTime: baseTime },
            { code: 0x32, time: baseTime + 1000, value: '12.34', latest: '12.34', latestTime: baseTime + 1000 },
            { code: 0x35, time: baseTime, value: '99.99', latest: '12.34', latestTime: baseTime + 1000 },
        ];
        for (const [index, sample] of samples.entries()) {
            if (failure) throw failure;
            peer!.write(slReport(sample.code, index + 1, sample.time, sample.value));
            let acquisition = '';
            await until(async () => {
                const rows = await db`SELECT id,data,extract(epoch FROM report_time)*1000 AS sample_time
                    FROM device_data WHERE device_id=${fixture.collector}`;
                const row = rows.find((row: Json) => row.data.function_code === sample.code.toString(16).toUpperCase()
                    && Number(row.sample_time) === sample.time && Number(row.data.values?.[shared.id]?.value) === Number(sample.value));
                acquisition = row?.id ?? '';
                return !!acquisition;
            }, `SL651 ${sample.code.toString(16)} ${sample.value} was not ingested under the shared external point ID`);
            // History and latest have independent consumers. Require fanout and
            // durable removal of THIS acquisition from latest's stream before
            // asserting that an older report did not change the existing value.
            await until(async () => Number(await redis.send('EVAL', [
                `if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
                 for _,entry in ipairs(redis.call('XRANGE', KEYS[2], '-', '+')) do
                   local fields = entry[2]
                   for i=1,#fields,2 do
                     if (fields[i] == 'acquisition_id' or fields[i] == 'message_id') and fields[i+1] == ARGV[1] then return 0 end
                   end
                 end
                 return 1`,
                '2', `iot:telemetry:fanout:${acquisition}`, 'iot:v3:telemetry:latest', acquisition,
            ])) === 1, `latest consumer did not finish SL651 acquisition ${acquisition}`);
            const latest = JSON.parse(String(await redis.send('HGET', [latestKey, shared.id])));
            assert.equal(latest.id, shared.id, 'latest projection rewrote the external point ID');
            assert.equal(Number(latest.value), Number(sample.latest), 'newer timestamp wins; equal timestamp uses later arrival; older report cannot overwrite');
            assert.equal(latest.observedAt, sample.latestTime);
            assert.equal(latest.name, shared.name);
        }
        const latest = await hash(latestKey);
        assert.deepEqual(new Set(Object.keys(latest).filter(key => !key.startsWith('_'))),
            new Set(elements(fixture).map(element => element.point.id)), 'latest Hash introduced synthetic function-qualified external IDs');
        const down = fixture.config.funcs.find((fn: Json) => fn.dir === 'DOWN');
        const requested = [...down.elements].reverse().map((point: Json, index: number) => ({ elementId: point.id, value: index ? '56.78' : '12.34' }));
        const rejected = await fetch(`${apiBase}/v1/device/${fixture.collector}/commands`, {
            method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ idempotency_key: crypto.randomUUID(), elements: requested.slice(0, 1) }), signal: AbortSignal.timeout(15000),
        });
        assert.equal(rejected.status, 400, await rejected.text());
        received = Buffer.alloc(0);
        const prepared = await http('POST', `/v1/device/${fixture.collector}/commands`, { idempotency_key: crypto.randomUUID(), elements: requested });
        assert.equal(prepared.command_ids.length, 1, 'a complete DOWN function must prepare one command');
        const [attempt] = await db`SELECT queue_kind,payload FROM command_attempt WHERE operation_id=${prepared.command_ids[0]}`;
        assert.equal(attempt.queue_kind, 'stream');
        const queued: [string, string][] = [];
        for (let index = 0; index < attempt.payload.length; index += 2) queued.push([attempt.payload[index], attempt.payload[index + 1]]);
        assert.deepEqual(queued.filter(([name]) => name.startsWith('element:')), requested.map((point: Json) => [`element:${point.elementId}`, point.value]),
            'queued Collector command must retain request order and original IDs');
        let egress: Buffer | undefined;
        await until(async () => {
            if (failure) throw failure;
            let offset = 0;
            while (offset + 17 <= received.length) {
                assert.equal(received.readUInt16BE(offset), 0x7e7e, 'invalid Collector SL651 egress header');
                const length = (received.readUInt16BE(offset + 11) & 0x7fff) + 17;
                if (offset + length > received.length) return false;
                const frame = received.subarray(offset, offset + length);
                if (frame[frame.length - 3] === 0x05) { egress = frame; return true; }
                offset += length; // M2 report confirmations can precede the command.
            }
            return false;
        }, 'prepared Collector command did not reach the live TCP peer');
        assert.equal(egress![10], Number.parseInt(down.funcCode, 16), 'first configured complete DOWN function must win over the later identical ID set');
        assert.equal(egress!.readUInt16BE(11) & 0x8000, 0x8000, 'command must be downstream');
        assert.equal(egress!.readUInt16BE(egress!.length - 2), slCrc(egress!.subarray(0, -2)));
        const expectedBody = Buffer.concat(requested.map((input: Json) => {
            const point = down.elements.find((point: Json) => point.id === input.elementId);
            return Buffer.from(point.guideHex + input.value.replace('.', ''), 'hex');
        }));
        assert.deepEqual(egress!.subarray(22, -3), expectedBody, 'Collector encoded values from another UP/DOWN/response occurrence or reordered request values');

        // Only acknowledge configuration application on our captured software
        // peer. Edge CommandRequest has IDs+values, not a function selector:
        // observing this wire does not prove physical firmware disambiguation.
        socket!.send(envelope(33, snapshot.commit!));
        await until(async () => {
            const [row] = await db`SELECT status->'config' AS config FROM edge_node WHERE id=${node}`;
            return row.config?.state === 'applied' && String(row.config.activeVersion) === String(snapshot.revision);
        }, 'captured edge peer configuration was not marked applied');
        const meter = fixtures.find(value => value.protocol === 'DLT645' && value.explicit)!;
        const meterPoint = meter.config.points[0];
        for (const device of [meter.collector, meter.edge!]) {
            const overflow = await fetch(`${apiBase}/v1/device/${device}/commands`, {
                method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ idempotency_key: crypto.randomUUID(), elements: [{ elementId: meterPoint.id, value: '100' }] }), signal: AbortSignal.timeout(15000),
            });
            assert.equal(overflow.status, 400, `one-byte BCD overflow must be rejected before Collector/Edge queueing: ${await overflow.text()}`);
            if (device === meter.collector) {
                // This fixture deliberately refuses industrial TCP peers. A
                // representable value must pass preparation and reach routing,
                // not be rejected as invalid or admitted via a fake route.
                const offline = await fetch(`${apiBase}/v1/device/${device}/commands`, {
                    method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
                    body: JSON.stringify({ idempotency_key: crypto.randomUUID(), elements: [{ elementId: meterPoint.id, value: '99' }] }), signal: AbortSignal.timeout(15000),
                });
                assert.equal(offline.status, 409);
                assert.equal((await offline.json()).code, 18013, 'valid BCD must reach the offline-route check after codec validation');
            } else {
                const accepted = await http('POST', `/v1/device/${device}/commands`, {
                    idempotency_key: crypto.randomUUID(), elements: [{ elementId: meterPoint.id, value: '99' }],
                });
                assert.equal(accepted.command_ids.length, 1, 'one-byte BCD boundary must remain accepted by Edge preparation');
            }
        }
        const edgePrepared = await http('POST', `/v1/device/${fixture.edge}/commands`, { idempotency_key: crypto.randomUUID(), elements: requested });
        assert.equal(edgePrepared.command_ids.length, 1);
        const [edgeAttempt] = await db`SELECT queue_kind,payload FROM command_attempt WHERE operation_id=${edgePrepared.command_ids[0]}`;
        assert.equal(edgeAttempt.queue_kind, 'list');
        const queuedEnvelope = decode(Buffer.from(edgeAttempt.payload[0], 'hex'));
        const queuedCommand = queuedEnvelope.get(50) as Buffer;
        assert(queuedCommand, 'edge queue did not contain CommandRequest');
        const command = decode(queuedCommand);
        assert.deepEqual(command.get(2), bytes(fixture.edge!));
        assert.equal(number(command, 6), fixture.config.commandFastReadDuration ?? 60);
        assert.equal(number(command, 7), fixture.config.commandFastReadInterval ?? 1);
        assert(fields(queuedCommand).every(value => value.tag >= 1 && value.tag <= 7), 'edge command introduced an unapproved function selector');
        assert.deepEqual(fields(queuedCommand).filter(value => value.tag === 3).map(value => {
            const input = decode(value.value as Buffer), expected = decode(input.get(2) as Buffer);
            return { elementId: text(input, 1), value: text(expected, 6) };
        }), requested, 'edge command must preserve original IDs and request order');
        await until(async () => edgeCommands.has(bytes(edgePrepared.command_ids[0]).toString('hex')), 'edge command was not delivered to captured WebSocket');
        assert.deepEqual(edgeCommands.get(bytes(edgePrepared.command_ids[0]).toString('hex')), queuedCommand);
    } finally {
        peer?.destroy();
    }
}

try {
    await until(async () => (await fetch(apiBase + '/internal/health/ready', { signal: AbortSignal.timeout(5000) })).status === 200, 'fixture workers are not ready');
    await new Promise<void>((resolve, reject) => { endpoint.once('error', reject); endpoint.listen(0, '127.0.0.1', resolve); });
    endpointListening = true;
    const port = (endpoint.address() as net.AddressInfo).port;
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},${identity},'approved')`;
    await redis.send('SET', [`iot:edge:auth:${identity}`, `${node}|approved`]);
    for (const protocol of protocols) for (const explicit of [false, true]) {
        const { config, valid } = configuration(protocol, explicit), name = `${tag}-${protocol}-${explicit ? 'explicit' : 'default'}`;
        await http('POST', '/v1/protocol/configs', { protocol, name, enabled: true, config: valid });
        const [model] = await db`SELECT id FROM protocol_config WHERE name=${name}`;
        modelIds.push(model.id);
        const fixture: Fixture = { protocol, explicit, model: model.id, config, validConfig: valid,
            collector: crypto.randomUUID(), collectorLink: crypto.randomUUID(), target: crypto.randomUUID(),
            ...(protocol === 'MQTT' ? {} : { edge: crypto.randomUUID(), edgeLink: crypto.randomUUID() }) };
        fixtures.push(fixture);
        await db`UPDATE protocol_config SET config=${config}::jsonb WHERE id=${fixture.model}`;
        for (const execution of ['collector', 'edge'] as const) {
            const link = execution === 'collector' ? fixture.collectorLink : fixture.edgeLink;
            const device = execution === 'collector' ? fixture.collector : fixture.edge;
            if (!link || !device) continue;
            const connection: Json = { transport: 'tcp', interface: 'lo', mode: protocol === 'SL651' ? 'TCP Server' : 'TCP Client',
                ip: '127.0.0.1', port, targets: [] };
            if (execution === 'edge') connection.port = ((port + fixtures.length) % 65535) + 1;
            if (execution === 'collector') {
                if (protocol === 'SL651') {
                    const reservation = net.createServer();
                    await new Promise<void>((resolve, reject) => { reservation.once('error', reject); reservation.listen(0, '127.0.0.1', resolve); });
                    connection.port = (reservation.address() as net.AddressInfo).port;
                    await new Promise<void>((resolve, reject) => reservation.close(error => error ? reject(error) : resolve()));
                    connection.ip = '0.0.0.0';
                } else {
                    connection.ip = ''; connection.port = 0;
                    connection.targets = [{ id: fixture.target, name: 'local rejecting endpoint', ip: '127.0.0.1', port, status: 'enabled',
                        ...(protocol === 'MQTT' ? { mqtt: { clientId: `${tag}-${Number(explicit)}`, keepAliveSeconds: 30 } } : {}) }];
                }
            }
            linkIds.push(link);
            await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,edge_node_id,created_by)
                VALUES(${link},${`${name}-${execution}`},${protocol},${connection}::jsonb,'enabled',${execution},${execution === 'edge' ? node : null}::uuid,${admin})`;
            const parameters = { device_code: protocol === 'DLT645' ? explicit ? '000000000002' : '000000000001'
                : protocol === 'SL651' ? explicit ? '0000000002' : '0000000001' : `${execution}-${protocol}-${Number(explicit)}`,
                ...(execution === 'collector' && protocol !== 'SL651' ? { target_id: fixture.target } : {}),
                ...(explicit ? { online_timeout: '123', timezone: '+00:00', slave_id: '7', modbus_mode: 'RTU', remote_control: true,
                    heartbeat: { mode: 'ASCII', content: 'PING\\r\\n' }, registration: { mode: 'ASCII', content: 'REGISTER' } } : {}) };
            deviceIds.push(device);
            await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,status,created_by)
                VALUES(${device},${`${name}-${execution}`},${link},${fixture.model},${parameters}::jsonb,'enabled',${admin})`;
        }
    }
    await connect();
    // A real model write publishes the SQL-seeded historical shapes through the
    // same outbox/wake and edge sync routes as a user's subsequent edit.
    for (const fixture of fixtures) await http('PUT', `/v1/protocol/configs/${fixture.model}`, { remark: 'projection ready' });
    const initialVersion = await waitForCollector(() => true);
    await assertCollector(initialVersion, () => true); await assertHttp();
    const initial = await sync(); assertEdge(initial, () => true);
    // Reconnect with applied_version=0: replay/rebuild this SAME desired revision.
    // Revision participates in SHA; comparing different revisions would be wrong.
    await disconnect(); snapshots = new Map(); await connect();
    const repeated = await captured(initial.revision);
    assert.deepEqual(repeated.items, initial.items, 'same-revision rebuild was nondeterministic');
    assert.deepEqual(repeated.commit, initial.commit);

    const preservedFixture = fixtures.find(fixture => fixture.protocol === 'Modbus' && !fixture.explicit)!;
    // The rejecting Collector endpoint legitimately reports disconnects during
    // reconfiguration. Use the idle Edge device to isolate metadata preservation
    // from transport state changes while keeping the same projection assertions.
    assert(preservedFixture.edge);
    const preservedDevice = preservedFixture.edge;
    const preservedPoint = preservedFixture.validConfig.registers.at(-1)!;
    const latestKey = `iot:v2:device:${preservedDevice}:latest`;
    const runtimeKey = `iot:v2:runtime:device:${preservedDevice}`;
    const onlineDeadline = String(Date.now() + 600000);
    const observation = Date.now();
    await redis.send('HSET', [latestKey, preservedPoint.id, JSON.stringify({ id: preservedPoint.id,
        name: 'old metadata', value: '27.5', observedAt: observation, source: 'edge' })]);
    await redis.send('HSET', [runtimeKey, 'online_until_ms', onlineDeadline, 'state', 'online']);
    stream = await openSnapshotSubscription('/v1/device/events', token);
    await receiveRealtime(() => fixtures.every(fixture => realtime.has(fixture.collector)));
    for (const fixture of fixtures.filter(fixture => !fixture.explicit)) {
        const changed = structuredClone(fixture.validConfig);
        changed.readInterval = 19;
        if (fixture.protocol === 'SL651') {
            changed.funcs[0].elements.reverse();
            changed.responseMode = 'M2';
            // Latest's control metadata also includes DOWN occurrences, so edit
            // every normal definition of the shared ID rather than only UP.
            for (const fn of changed.funcs)
                for (const point of fn.elements) point.name = `${fixture.protocol} changed first`;
        } else {
            const key = fixture.protocol === 'Modbus' ? 'registers' : fixture.protocol === 'S7' ? 'areas' : 'points';
            changed[key].reverse(); changed[key][0].name = `${fixture.protocol} changed first`;
        }
        await http('PUT', `/v1/protocol/configs/${fixture.model}`, { config: changed });
        fixture.config = changed;
    }
    await receiveRealtime(() => fixtures.filter(fixture => !fixture.explicit).every(fixture =>
        realtime.get(fixture.collector)?.elements?.some((point: Json) => point.name === `${fixture.protocol} changed first`)));
    await until(async () => {
        const value = await redis.send('HGET', [latestKey, preservedPoint.id]);
        return value !== null && JSON.parse(String(value)).name === 'Modbus changed first';
    }, 'model edit did not refresh preserved live point metadata');
    const preserved = JSON.parse(String(await redis.send('HGET', [latestKey, preservedPoint.id])));
    assert.equal(preserved.value, '27.5');
    assert.equal(preserved.observedAt, observation);
    assert.equal(preserved.source, 'edge');
    assert.equal((await hash(runtimeKey)).state, 'online');
    assert.equal((await hash(runtimeKey)).online_until_ms, onlineDeadline);
    assert.equal(Number(await redis.send('ZSCORE', ['iot:v2:schedule:device:online-deadlines', preservedDevice])), Number(onlineDeadline));
    await stream.close(); stream = undefined;
    await redis.send('HSET', [runtimeKey, 'online_until_ms', 'not-a-number']);
    await http('PUT', `/v1/protocol/configs/${preservedFixture.model}`, { remark: 'repair corrupted online deadline' });
    await until(async () => {
        const runtime = await hash(runtimeKey);
        return !runtime.online_until_ms && runtime.state === 'offline';
    }, 'model projection retained an invalid online deadline');
    assert.equal(await redis.send('ZSCORE', ['iot:v2:schedule:device:online-deadlines', preservedDevice]), null);
    const changedVersion = await waitForCollector(() => true);
    assert.notEqual(changedVersion, initialVersion);
    await assertCollector(changedVersion, () => true); await assertHttp();
    const changed = await sync(); assertEdge(changed, () => true);
    await assertRepeatedSl651(fixtures.find(fixture => fixture.protocol === 'SL651' && !fixture.explicit)!, changed);
    assert.notEqual(changed.revision, initial.revision);
    assert.notDeepEqual(decode(changed.commit!).get(2), decode(initial.commit!).get(2), 'model edit did not change snapshot SHA');
    assert.notDeepEqual(changed.items.map(wire => without(wire, [1, 4])), initial.items.map(wire => without(wire, [1, 4])), 'hash changed only because revision changed, not because projection changed');
    await disconnect(); snapshots = new Map(); await connect();
    const changedRepeat = await captured(changed.revision);
    assert.deepEqual(changedRepeat.items, changed.items, 'updated same-revision rebuild was nondeterministic');
    assert.deepEqual(changedRepeat.commit, changed.commit);

    for (const scope of ['device', 'model', 'link'] as const) {
        for (const fixture of fixtures.filter(fixture => !fixture.explicit)) {
            if (scope === 'device') {
                await db`UPDATE device SET status='disabled',updated_at=NOW() WHERE id=${fixture.collector} OR id=${fixture.edge ?? fixture.collector}`;
            } else if (scope === 'link') {
                await db`UPDATE link SET status='disabled',updated_at=NOW() WHERE id=${fixture.collectorLink} OR id=${fixture.edgeLink ?? fixture.collectorLink}`;
            }
            await http('PUT', `/v1/protocol/configs/${fixture.model}`, scope === 'model' ? { enabled: false } : { remark: `${scope} disabled` });
        }
        const version = await waitForCollector(fixture => fixture.explicit);
        await assertCollector(version, fixture => fixture.explicit);
        assertEdge(await sync(), fixture => fixture.explicit, scope !== 'link');
        for (const fixture of fixtures.filter(fixture => !fixture.explicit)) {
            if (scope === 'device') await db`UPDATE device SET status='enabled',updated_at=NOW() WHERE id=${fixture.collector} OR id=${fixture.edge ?? fixture.collector}`;
            if (scope === 'link') await db`UPDATE link SET status='enabled',updated_at=NOW() WHERE id=${fixture.collectorLink} OR id=${fixture.edgeLink ?? fixture.collectorLink}`;
            await http('PUT', `/v1/protocol/configs/${fixture.model}`, scope === 'model' ? { enabled: true } : { remark: `${scope} restored` });
        }
        await assertCollector(await waitForCollector(() => true), () => true);
        assertEdge(await sync(), () => true);
    }
    console.log('PASS seven-protocol HTTP/SQL/Redis projections; repeated SL651 IDs across functions, live TCP latest ordering and command egress; six-protocol edge protobuf defaults/overrides, source order, disabled device/model/link, SSE model edits and revision-correct deterministic SHA');
} finally {
    try { await stream?.close(); } finally {
        try { await disconnect(); } finally {
            try {
                // Keep asynchronous audit/outbox references valid, then publish
                // removal so no Collector listener/session leaks into sibling tests.
                for (const id of deviceIds) await db`UPDATE device SET deleted_at=NOW(),updated_at=NOW() WHERE id=${id}`;
                for (const id of linkIds) await db`UPDATE link SET deleted_at=NOW(),status='disabled',updated_at=NOW() WHERE id=${id}`;
                if (modelIds.length) {
                    await http('PUT', `/v1/protocol/configs/${modelIds[0]}`, { remark: 'projection fixture cleanup' });
                    await waitForCollector(() => false);
                }
                for (const id of modelIds) await db`UPDATE protocol_config SET deleted_at=NOW(),updated_at=NOW() WHERE id=${id}`;
                await db`UPDATE edge_node SET enrollment_status='pending',updated_at=NOW() WHERE id=${node}`;
                await redis.send('DEL', [`iot:edge:auth:${identity}`, `iot:edge:config:${node}`, `iot:edge:config-revision:${node}`, `iot:edge:metadata:${node}`]);
            } finally {
                socket?.close(); redis.close(); await db.close();
                if (endpointListening) await new Promise<void>((resolve, reject) => endpoint.close(error => error ? reject(error) : resolve()));
            }
        }
    }
}
