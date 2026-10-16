import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import { access, readdir, unlink, utimes, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { apiBase, databaseUrl, fixtureDirectory, redisUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl), redis = new Bun.RedisClient(redisUrl);
const platform = '00000000-0000-7000-8000-000000000001';
const admin = '00000000-0000-7000-8000-000000000002';
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ iss: 'iot-engine', aud: 'iot-engine-web', sub: admin, user_id: admin, username: 'admin', token_type: 'access', iat: now, exp: now + 3600 })}`;
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
const authorization = { Authorization: `Bearer ${token}` };
const node = crypto.randomUUID();
const bytes = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');
function integer(input: bigint | number) {
    let value = BigInt(input); const output: number[] = [];
    do { let byte = Number(value & 127n); value >>= 7n; if (value) byte |= 128; output.push(byte); } while (value);
    return Buffer.from(output);
}
function field(tag: number, value: bigint | number | string | Buffer) {
    if (typeof value === 'bigint' || typeof value === 'number') return Buffer.concat([integer(tag * 8), integer(value)]);
    const buffer = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag * 8 + 2), integer(buffer.length), buffer]);
}
function decode(data: Buffer) {
    const result = new Map<number, bigint | Buffer>(); let position = 0;
    const read = () => { let value = 0n, shift = 0n; for (;;) { const byte = data[position++]; assert(byte !== undefined); value |= BigInt(byte & 127) << shift; if (!(byte & 128)) return value; shift += 7n; } };
    while (position < data.length) {
        const key = Number(read()), wire = key & 7;
        if (!wire) result.set(key >> 3, read());
        else if (wire === 2) { const size = Number(read()); result.set(key >> 3, data.subarray(position, position + size)); position += size; }
        else if (wire === 1 || wire === 5) { const size = wire === 1 ? 8 : 4; result.set(key >> 3, data.subarray(position, position + size)); position += size; }
        else throw Error(`Invalid wire ${wire}`);
    }
    return result;
}
async function until(check: () => Promise<boolean>, reason: string, timeout = 25000) {
    const end = Date.now() + timeout;
    while (Date.now() < end) { if (await check()) return; await Bun.sleep(100); }
    throw Error(reason);
}
async function exists(file: string) { try { await access(file); return true; } catch { return false; } }
function imei() {
    const base = '99' + String(Math.floor(Math.random() * 1e12)).padStart(12, '0'); let sum = 0;
    for (let index = 0; index < 14; index++) { let digit = Number(base[index]) * (index % 2 ? 2 : 1); if (digit > 9) digit -= 9; sum += digit; }
    return base + String((10 - sum % 10) % 10);
}
const identity = imei(), hash = (buffer: Buffer) => createHash('sha256').update(buffer).digest('hex');
const content = Buffer.concat([Buffer.from('firmware integration fixture only\n'), crypto.getRandomValues(new Uint8Array(17000))]);
const commands: Map<number, bigint | Buffer>[] = [], chunks: Map<number, bigint | Buffer>[] = [];
let socket: WebSocket | undefined, sequence = 1n, epoch = 0n;
const envelope = (tag: number, payload: Buffer) => Buffer.concat([
    field(1, 6), field(2, bytes(crypto.randomUUID())), field(3, bytes(node)), field(4, bytes(platform)),
    field(5, epoch), field(6, Date.now()), field(8, sequence++), field(tag, payload),
]);
async function reuse(buffer: Buffer, keepSettings = true) {
    return fetch(`${apiBase}/v1/edge/${node}/firmware/reuse`, {
        method: 'POST', headers: { ...authorization, 'Content-Type': 'application/json' },
        body: JSON.stringify({ sha256: hash(buffer), sizeBytes: buffer.length, keepSettings }),
    });
}
async function upload(buffer: Buffer, name: string) {
    const query = new URLSearchParams({ fileName: name, sizeBytes: String(buffer.length), keepSettings: 'true' });
    const response = await fetch(`${apiBase}/v1/edge/${node}/firmware?${query}`, {
        method: 'POST', headers: { ...authorization, 'Content-Type': 'application/octet-stream' }, body: buffer,
    });
    assert.equal(response.status, 200, await response.text());
}
const planted: string[] = [];
try {
    await until(async () => (await fetch(apiBase + '/internal/health/ready')).status === 200, 'Workers not ready');
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},${identity},'approved')`;
    await redis.send('SET', [`iot:edge:auth:${identity}`, `${node}|approved`]);
    socket = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect'); socket.binaryType = 'arraybuffer';
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(Error('Hello timeout')), 10000);
        socket!.onopen = () => socket!.send(envelope(20, Buffer.concat([field(1, identity), field(2, 'fixture'), field(3, '0.3.44'), field(15, 1)])));
        socket!.onerror = () => reject(Error('Node WebSocket error'));
        socket!.onmessage = event => {
            const message = decode(Buffer.from(event.data as ArrayBuffer));
            if (message.has(21)) { epoch = message.get(5) as bigint; clearTimeout(timer); resolve(); }
            if (message.has(80)) socket!.send(envelope(81, message.get(80) as Buffer));
            if (message.has(62)) commands.push(decode(message.get(62) as Buffer));
            if (message.has(79)) chunks.push(decode(message.get(79) as Buffer));
        };
    });
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->>'firmwareUpdate'='true'`).length > 0, 'Firmware capability missing');
    const miss = await reuse(content); assert.equal(miss.status, 200); assert.equal((await miss.json()).reused, false);
    assert.equal((await fetch(`${apiBase}/v1/edge/${node}/firmware/reuse`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sha256: hash(content), sizeBytes: content.length }) })).status, 401);
    await upload(content, 'first.bin');
    await until(async () => commands.length === 1, 'Legacy upgrade not delivered');
    const downloadUrl = (commands[0].get(2) as Buffer).toString();
    assert(downloadUrl.includes('/edge/v1/firmware/') && downloadUrl.includes('?token='));
    const download = await fetch(downloadUrl); assert.equal(download.status, 200);
    assert.deepEqual(Buffer.from(await download.arrayBuffer()), content);
    const badToken = new URL(downloadUrl); badToken.searchParams.set('token', 'invalid');
    assert.equal((await fetch(badToken)).status, 404);
    const hit = await reuse(content, false); assert.equal(hit.status, 200); assert.equal((await hit.json()).reused, true);
    await until(async () => commands.length === 2, 'Reused upgrade not delivered');
    assert.equal(commands[1].get(6) ?? 0n, 0n, 'keepSettings was not retained');
    await Promise.all([upload(content, 'second-name.bin'), upload(content, 'third-name.bin')]);
    const stored = await db`SELECT id,storage_path FROM edge_firmware WHERE sha256=${hash(content)}`;
    assert.equal(stored.length, 1, 'Concurrent upload created duplicate records');
    assert.equal((await db`SELECT 1 FROM edge_task WHERE node_id=${node} AND request->>'firmware_id'=${stored[0].id}`).length, 4);
    const directory = path.dirname(stored[0].storage_path);
    assert(directory.startsWith(path.resolve(fixtureDirectory) + path.sep), 'File test must stay in disposable fixture');
    assert.equal((await readdir(directory)).filter(file => file.endsWith('.bin')).length, 1, 'Duplicate file remained');

    // 同一固件的 WS 分块读取和偏移续传，使用模拟节点，不刷写真实设备。
    await db`UPDATE edge_node SET capability=jsonb_set(capability,'{firmwareStream}','true') WHERE id=${node}`;
    const before = commands.length;
    assert.equal((await reuse(content)).status, 200);
    await until(async () => commands.length > before, 'Stream upgrade not delivered');
    const command = commands.at(-1)!;
    assert.equal((command.get(2) as Buffer | undefined)?.length ?? 0, 0);
    for (const offset of [0, 8192, 16384, 8192]) {
        const count = chunks.length;
        socket.send(envelope(78, Buffer.concat([field(1, command.get(1) as Buffer), field(2, offset)])));
        await until(async () => chunks.length > count, 'Firmware chunk not delivered');
        const chunk = chunks.at(-1)!;
        assert.equal(chunk.get(2) ?? 0n, BigInt(offset));
        assert.deepEqual(chunk.get(3), content.subarray(offset, Math.min(offset + 8192, content.length)));
        assert.equal((chunk.get(5) as Buffer | undefined)?.length ?? 0, 0);
    }

    // 缺失文件不能命中去重；重新上传后创建可用记录。
    const missing = Buffer.from(`missing-${crypto.randomUUID()}`);
    await upload(missing, 'missing.bin');
    const [missingRecord] = await db`SELECT id,storage_path FROM edge_firmware WHERE sha256=${hash(missing)}`;
    await unlink(missingRecord.storage_path);
    assert.equal((await (await reuse(missing)).json()).reused, false);
    await upload(missing, 'restored.bin');
    assert.equal((await (await reuse(missing)).json()).reused, true);

    const recent = Buffer.from(`recent-${crypto.randomUUID()}`), active = Buffer.from(`active-${crypto.randomUUID()}`);
    await upload(recent, 'recent.bin'); await upload(active, 'active.bin');
    const [recentRecord] = await db`SELECT id,storage_path FROM edge_firmware WHERE sha256=${hash(recent)}`;
    const [activeRecord] = await db`SELECT id,storage_path FROM edge_firmware WHERE sha256=${hash(active)}`;
    await until(async () => commands.length === 10, 'Firmware commands not delivered');
    socket.close(); socket = undefined;
    await db`UPDATE edge_firmware SET created_at=now()-interval '2 hours' WHERE created_by=${admin}`;
    await db`UPDATE edge_task SET status='succeeded',completed_at=now()-interval '2 hours',updated_at=now()-interval '2 hours' WHERE node_id=${node}`;
    await db`UPDATE edge_task SET completed_at=now() WHERE node_id=${node} AND request->>'firmware_id'=${recentRecord.id}`;
    await db`UPDATE edge_task SET status='running',completed_at=NULL WHERE node_id=${node} AND request->>'firmware_id'=${activeRecord.id}`;
    await db`UPDATE edge_firmware SET storage_path=${path.relative(path.dirname(directory), activeRecord.storage_path)} WHERE id=${activeRecord.id}`;
    const old = new Date(Date.now() - 7200000);
    const orphan = path.join(directory, `${crypto.randomUUID()}.bin`), fresh = path.join(directory, `${crypto.randomUUID()}.bin`);
    const unrelated = path.join(directory, 'retain.bin'), uploading = path.join(directory, `${crypto.randomUUID()}.upload`);
    planted.push(orphan, fresh, unrelated, uploading);
    for (const file of planted) await writeFile(file, 'fixture');
    for (const file of [orphan, unrelated, uploading, recentRecord.storage_path, activeRecord.storage_path]) await utimes(file, old, old);
    await until(async () => !(await exists(stored[0].storage_path)) && !(await exists(orphan)), 'Expired firmware cleanup did not run', 85000);
    assert.equal((await db`SELECT 1 FROM edge_firmware WHERE id=${stored[0].id}`).length, 0);
    assert((await db`SELECT 1 FROM edge_task WHERE node_id=${node}`).length > 0, 'Task history was deleted');
    for (const file of [recentRecord.storage_path, activeRecord.storage_path, fresh, unrelated, uploading]) assert(await exists(file), `Protected file removed: ${file}`);
    assert.equal((await fetch(downloadUrl)).status, 404, 'Expired download token still resolves');
    console.log('PASS firmware: hash reuse, concurrent dedup, legacy token download, WS resume, missing-file recovery, one-hour retention, active/recent task and upload protection');
} finally {
    socket?.close();
    // 夹具目录与其他固件测试共享；上传恢复只在启动认领后执行一次，不会再替本测试清理遗留文件。
    for (const file of planted) await unlink(file).catch((error) => { if (error.code !== 'ENOENT') throw error; });
    await redis.send('DEL', [`iot:edge:auth:${identity}`]);
    redis.close(); await db.close();
}
