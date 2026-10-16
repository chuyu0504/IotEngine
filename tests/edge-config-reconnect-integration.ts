import assert from 'node:assert/strict';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';

// 只连接本地隔离服务。模拟节点 Hello 与能力上报，不发送心跳、采集数据或写入指令。
const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const platform = '00000000-0000-7000-8000-000000000001';
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
function field(tag: number, value: number | bigint | string | Buffer): Buffer {
    if (typeof value === 'number' || typeof value === 'bigint')
        return Buffer.concat([integer(tag * 8), integer(value)]);
    const buffer = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag * 8 + 2), integer(buffer.length), buffer]);
}
function decode(buffer: Buffer) {
    const fields = new Map<number, bigint | Buffer>();
    let offset = 0;
    function read() {
        let value = 0n, shift = 0n;
        for (;;) {
            const byte = buffer[offset++];
            assert(byte !== undefined);
            value |= BigInt(byte & 127) << shift;
            if (!(byte & 128)) return value;
            shift += 7n;
        }
    }
    while (offset < buffer.length) {
        const tag = Number(read());
        if ((tag & 7) === 0) fields.set(tag >> 3, read());
        else {
            assert.equal(tag & 7, 2);
            const length = Number(read());
            fields.set(tag >> 3, buffer.subarray(offset, offset + length));
            offset += length;
        }
    }
    return fields;
}
function imei() {
    const prefix = `99${String(Math.floor(Math.random() * 1e12)).padStart(12, '0')}`;
    let sum = 0;
    for (let i = 0; i < 14; ++i) {
        let digit = Number(prefix[i]) * (i % 2 ? 2 : 1);
        if (digit > 9) digit -= 9;
        sum += digit;
    }
    return prefix + String((10 - sum % 10) % 10);
}

async function reconnect(applied: bigint, desired: bigint, expectConfig: boolean, supportsConfig = true) {
    const node = uuid(), serial = imei();
    const revision = desired.toString();
    const status = { config: { desiredVersion: Number(desired), activeVersion: Number(desired), state: 'applied' } };
    let socket: WebSocket | undefined;
    try {
        await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status,capability,status)
            VALUES(${node},${platform},${serial},'approved','{"deviceConfig":true}'::jsonb,${status}::jsonb)`;
        await redis.send('SET', [`iot:edge:auth:${serial}`, `${node}|approved`]);
        // Empty queue is essential: the old implementation waited for a heartbeat to recreate it.
        assert.equal(Number(await redis.send('LLEN', [`iot:edge:config:${node}`])), 0);
        socket = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect');
        socket.binaryType = 'arraybuffer';
        const seen: number[] = [];
        const result = new Promise<void>((resolve, reject) => {
            let timeout: Timer | undefined;
            socket!.onerror = () => { clearTimeout(timeout); reject(new Error('edge WebSocket failed')); };
            socket!.onclose = () => { clearTimeout(timeout); reject(new Error('edge WebSocket closed')); };
            socket!.onopen = () => {
                const hello = Buffer.concat([field(1, serial), field(2, 'fixture'), field(3, '0.3.65'),
                    field(8, applied), field(23, Number(supportsConfig)), field(37, 1)]);
                socket!.send(Buffer.concat([field(1, 6), field(2, bytes(uuid())), field(3, bytes(node)),
                    field(4, bytes(platform)), field(5, 0), field(6, Date.now()), field(8, 1), field(20, hello)]));
            };
            socket!.onmessage = event => {
                const message = decode(Buffer.from(event.data as ArrayBuffer));
                if (message.has(21)) {
                    // Restoration follows the capability report (80a3426) so the
                    // configuration is validated against the reported protocol set.
                    socket!.send(Buffer.concat([field(1, 6), field(2, bytes(uuid())), field(3, bytes(node)),
                        field(4, bytes(platform)), field(5, message.get(5) as bigint), field(6, Date.now()),
                        field(8, 2), field(26, Buffer.alloc(0))]));
                    timeout = setTimeout(() => expectConfig
                        ? reject(new Error('stale configuration waited for heartbeat'))
                        : resolve(), expectConfig ? 5000 : 750);
                }
                if (message.has(30) || message.has(32)) seen.push(message.has(30) ? 30 : 32);
                if (expectConfig && seen.includes(30) && seen.includes(32)) {
                    clearTimeout(timeout);
                    resolve();
                }
                if (!expectConfig && seen.length) {
                    clearTimeout(timeout);
                    reject(new Error('up-to-date node received a configuration replay'));
                }
            };
        });
        await result;
        const rows = await db`SELECT status,created_by FROM edge_config_revision
            WHERE node_id=${node} AND revision=${revision}::bigint`;
        assert.equal(rows.length, Number(expectConfig));
        if (expectConfig) {
            assert.equal(rows[0].status, 'pending');
            assert.equal(rows[0].created_by, null);
        }
    } finally {
        socket?.close();
        await redis.send('DEL', [`iot:edge:auth:${serial}`, `iot:edge:config:${node}`,
            `iot:edge:config-revision:${node}`, `iot:edge:metadata:${node}`]);
        await db`DELETE FROM edge_config_revision WHERE node_id=${node}`;
        await db`DELETE FROM edge_node WHERE id=${node}`;
    }
}
try {
    const revision = BigInt(Date.now());
    await reconnect(0n, revision, true);
    await reconnect(revision, revision, false);
    await reconnect(revision + 1n, revision, false);
    await reconnect(0n, revision, false, false);
    console.log('PASS capability report after Hello reconciles stale config without heartbeat; current/newer and unsupported nodes do not replay');
} finally {
    await db.close();
    redis.close();
}
