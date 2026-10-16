import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';

// A VPN config result is projected by the Edge projector (edge_task) and then
// delivered to the VPN feature (vpn_route) as an RPC event. When delivering the
// event fails, the projector entry stays unacknowledged and is projected again;
// by then edge_task has already moved. Re-projecting the same result must still
// deliver the event, otherwise the route state is lost.
// Creating an edge peer maps its routes while the row is still revoked, then
// reactivates it; the API result must be the same as a direct activation.
const db = new Bun.SQL(databaseUrl), redis = new Bun.RedisClient(redisUrl);
const platform = '00000000-0000-7000-8000-000000000001';
const admin = '00000000-0000-7000-8000-000000000002';
const network = '00000000-0000-7000-8000-000000000004';
const node = crypto.randomUUID(), route = crypto.randomUUID(), task = crypto.randomUUID(), peerNode = crypto.randomUUID();
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const issuedAt = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ iss: 'iot-engine', aud: 'iot-engine-web', sub: admin, user_id: admin, username: 'admin', token_type: 'access', iat: issuedAt, exp: issuedAt + 3600 })}`;
const authorization = `Bearer ${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
// Returns the code of an API reply and the id in its data, if any.
async function apiCall(method: string, path: string, body?: unknown): Promise<{ code: unknown; id: string }> {
    const response = await fetch(`${apiBase}/v1${path}`, {
        method, headers: { Authorization: authorization, 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(15000),
    });
    const result: unknown = await response.json();
    assert(result && typeof result === 'object' && 'code' in result, `${method} ${path}: ${JSON.stringify(result)}`);
    const data = 'data' in result ? result.data : undefined;
    const id = data && typeof data === 'object' && 'id' in data && typeof data.id === 'string' ? data.id : '';
    return { code: result.code, id };
}
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
function hasHelloAck(data: Buffer) {
    let position = 0;
    const read = () => { let value = 0n, shift = 0n; for (;;) { const byte = data[position++]; assert(byte !== undefined); value |= BigInt(byte & 127) << shift; if (!(byte & 128)) return value; shift += 7n; } };
    let epoch: bigint | undefined, ack = false;
    while (position < data.length) {
        const key = Number(read()), wire = key & 7;
        if (!wire) { const value = read(); if (key >> 3 === 5) epoch = value; }
        else if (wire === 2) { const size = Number(read()); if (key >> 3 === 21) ack = true; position += size; }
        else if (wire === 1 || wire === 5) position += wire === 1 ? 8 : 4;
        else throw Error(`Invalid wire ${wire}`);
    }
    return ack ? epoch ?? 0n : undefined;
}
async function until(check: () => Promise<boolean>, reason: string, timeout = 20000) {
    const end = Date.now() + timeout;
    while (Date.now() < end) { if (await check()) return; await Bun.sleep(100); }
    throw Error(reason);
}
function imei() {
    const base = '98' + String(Math.floor(Math.random() * 1e12)).padStart(12, '0'); let sum = 0;
    for (let index = 0; index < 14; index++) { let digit = Number(base[index]) * (index % 2 ? 2 : 1); if (digit > 9) digit -= 9; sum += digit; }
    return base + String((10 - sum % 10) % 10);
}
const identity = imei();
const address = `100.96.${100 + Math.floor(Math.random() * 100)}.${2 + Math.floor(Math.random() * 250)}`;
const virtualCidr = `172.29.${2 + Math.floor(Math.random() * 250)}.0/24`;
let socket: WebSocket | undefined, sequence = 1n, epoch = 0n;
const envelope = (tag: number, payload: Buffer) => Buffer.concat([
    field(1, 6), field(2, bytes(crypto.randomUUID())), field(3, bytes(node)), field(4, bytes(platform)),
    field(5, epoch), field(6, Date.now()), field(8, sequence++), field(tag, payload),
]);
const routeStatus = async () => (await db`SELECT status FROM vpn_route WHERE id=${route}`)[0].status as string;
// VpnConfigResult{request_id, config_version: 1, applied: true}, sent twice unchanged.
const vpnResult = Buffer.concat([field(1, bytes(task)), field(2, 1), field(3, 1)]);
try {
    await until(async () => (await fetch(apiBase + '/internal/health/ready')).status === 200, 'Workers not ready');
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},${identity},'approved')`;
    await redis.send('SET', [`iot:edge:auth:${identity}`, `${node}|approved`]);
    await db`INSERT INTO vpn_peer(id,network_id,peer_type,edge_node_id,name,public_key,assigned_ipv4,status) VALUES
        (${node},${network},'edge',${node},'Result retry',${crypto.getRandomValues(Buffer.alloc(32)).toString('base64')},${address}::inet,'active')`;
    await db`INSERT INTO vpn_route(id,network_id,edge_peer_id,lan_interface,target_cidr,virtual_cidr,created_by,status) VALUES
        (${route},${network},${node},'lan','192.168.77.0/24',${virtualCidr},${admin},'error')`;
    await db`INSERT INTO edge_task(id,node_id,task_type,status,request,created_by) VALUES
        (${task},${node},'vpn','running',jsonb_build_object('peerId',${node}::text,'enabled',true,'configVersion',1),${admin})`;

    socket = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect'); socket.binaryType = 'arraybuffer';
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(Error('Hello timeout')), 10000);
        socket!.onopen = () => socket!.send(envelope(20, Buffer.concat([field(1, identity), field(2, 'fixture'), field(3, '0.3.44')])));
        socket!.onerror = () => reject(Error('Node WebSocket error'));
        socket!.onmessage = event => {
            const acknowledged = hasHelloAck(Buffer.from(event.data as ArrayBuffer));
            if (acknowledged !== undefined) { epoch = acknowledged; clearTimeout(timer); resolve(); }
        };
    });

    socket.send(envelope(85, vpnResult));
    await until(async () => (await db`SELECT 1 FROM edge_task WHERE id=${task} AND status='succeeded'`).length > 0, 'VPN task not completed');
    await until(async () => await routeStatus() === 'active', 'VPN result not applied to the route');
    const [completed] = await db`SELECT completed_at FROM edge_task WHERE id=${task}`;
    console.log('PASS VPN config result moves edge_task and reaches the VPN feature');

    // Simulates the event lost after edge_task moved, then the same entry projected again.
    await db`UPDATE vpn_route SET status='error' WHERE id=${route}`;
    socket.send(envelope(85, vpnResult));
    await until(async () => await routeStatus() === 'active', 'Re-projected VPN result was not delivered again');
    const [after] = await db`SELECT status,completed_at FROM edge_task WHERE id=${task}`;
    assert.equal(after.status, 'succeeded');
    assert.equal(after.completed_at.getTime(), completed.completed_at.getTime(), 'a re-projected result must not move the task again');
    console.log('PASS re-projected VPN result is delivered again from the persisted terminal task');

    const reportedKey = crypto.getRandomValues(Buffer.alloc(32)).toString('base64');
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status,capability) VALUES
        (${peerNode},${platform},${imei()},'approved',jsonb_build_object('vpn',jsonb_build_object('supportsVpn',true,'publicKey',${reportedKey}::text)))`;
    const createBody = { name: 'Created edge', peerType: 'edge', edgeNodeId: peerNode };
    const unmapped = await apiCall('POST', '/vpn/peers', createBody);
    assert.equal(unmapped.code, 21008, 'an EdgeNode without a bridge LAN cannot be mapped');
    assert.equal((await db`SELECT count(*)::int AS count FROM vpn_peer WHERE edge_node_id=${peerNode}`)[0].count, 0, 'a failed mapping must discard the new peer');
    await db`INSERT INTO edge_node_network(node_id,name,address_mode,device,is_up,is_bridge,ipv4,prefix_length) VALUES
        (${peerNode},'lan','static','br-lan',true,true,'192.168.78.1',24)`;
    const created = await apiCall('POST', '/vpn/peers', createBody);
    assert.equal(created.code, 0);
    const peerRow = async () => (await db`SELECT status,public_key,revoked_at FROM vpn_peer WHERE id=${created.id}`)[0];
    let peer = await peerRow();
    assert.equal(peer.status, 'active'); assert.equal(peer.public_key, reportedKey); assert.equal(peer.revoked_at, null);
    assert.equal((await db`SELECT count(*)::int AS count FROM vpn_peer WHERE edge_node_id=${peerNode}`)[0].count, 1);
    assert.equal((await db`SELECT count(*)::int AS count FROM vpn_route WHERE edge_peer_id=${created.id}`)[0].count, 1);
    assert.equal((await apiCall('POST', `/vpn/peers/${created.id}/revoke`)).code, 0);
    assert.equal((await peerRow()).status, 'revoked');
    const reactivated = await apiCall('POST', '/vpn/peers', { ...createBody, name: 'Reactivated edge' });
    assert.equal(reactivated.code, 0);
    assert.equal(reactivated.id, created.id, 'a revoked edge peer is reused');
    peer = await peerRow();
    assert.equal(peer.status, 'active'); assert.equal(peer.revoked_at, null);
    console.log('PASS new and reused edge peers are mapped while revoked, then activated');
} finally {
    socket?.close();
    await db`DELETE FROM vpn_peer WHERE edge_node_id IN (${node},${peerNode})`.catch(() => undefined);
    await db`DELETE FROM edge_node WHERE id IN (${node},${peerNode})`.catch(() => undefined);
    await redis.send('DEL', [`iot:edge:auth:${identity}`]).catch(() => undefined);
    redis.close();
    await db.close();
}
