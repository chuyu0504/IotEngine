import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import { dirname, join, resolve, sep } from 'node:path';
import { unlink, readdir } from 'node:fs/promises';
import { createConnection } from 'node:net';
import { apiBase, databaseUrl, fixtureDirectory, redisUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl), redis = new Bun.RedisClient(redisUrl);
const platform = '00000000-0000-7000-8000-000000000001', admin = '00000000-0000-7000-8000-000000000002';
const node = crypto.randomUUID();
const user = crypto.randomUUID(), role = crypto.randomUUID();
const bytes = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ iss: 'iot-engine', aud: 'iot-engine-web', sub: admin, user_id: admin, username: 'admin', token_type: 'access', iat: now, exp: now + 3600 })}`;
const authorization = `Bearer ${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
function userToken(userId: string) {
    const wire = `${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:userId,user_id:userId,username:userId,token_type:'access',iat:now,exp:now+3600})}`;
    return `${wire}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(wire).digest('base64url')}`;
}
function integer(input: bigint | number) {
    let n = BigInt(input); const output: number[] = [];
    do { let byte = Number(n & 127n); n >>= 7n; if (n) byte |= 128; output.push(byte); } while (n);
    return Buffer.from(output);
}
function field(tag: number, value: bigint | number | string | Buffer) {
    if (typeof value === 'number' || typeof value === 'bigint') return Buffer.concat([integer(tag * 8), integer(value)]);
    const data = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag * 8 + 2), integer(data.length), data]);
}
function decode(data: Buffer) {
    const result = new Map<number, bigint | Buffer>(); let position = 0;
    const read = () => { let n = 0n, shift = 0n; for (;;) { const b = data[position++]; assert(b !== undefined); n |= BigInt(b & 127) << shift; if (!(b & 128)) return n; shift += 7n; } };
    while (position < data.length) {
        const key = Number(read()), wire = key & 7;
        if (!wire) result.set(key >> 3, read());
        else if (wire === 2) { const n = Number(read()); result.set(key >> 3, data.subarray(position, position + n)); position += n; }
        else if (wire === 1 || wire === 5) { const n = wire === 1 ? 8 : 4; result.set(key >> 3, data.subarray(position, position + n)); position += n; }
        else throw Error('Invalid device frame');
    }
    return result;
}
async function until(check: () => Promise<boolean>, message: string) {
    const end = Date.now() + 10000;
    while (Date.now() < end) { if (await check()) return; await Bun.sleep(25); }
    throw Error(message);
}
const base = '99' + String(Math.floor(Math.random() * 1e12)).padStart(12, '0');
let sum = 0;
for (let i = 0; i < 14; i++) { let n = Number(base[i]) * (i % 2 ? 2 : 1); if (n > 9) n -= 9; sum += n; }
const identity = base + String((10 - sum % 10) % 10);
const pathFor=(name:string,size:number,keep=true)=>`/v1/edge/${node}/firmware?${new URLSearchParams({fileName:name,sizeBytes:String(size),keepSettings:String(keep)})}`;
async function upload(name:string,body:Uint8Array,declared=body.length,code=0,keep=true,bearer=authorization) {
 const response=await fetch(apiBase+pathFor(name,declared,keep),{method:'POST',headers:{Authorization:bearer,'Content-Type':'application/octet-stream'},body,signal:AbortSignal.timeout(15000)});
 const result=await response.json() as {code:number};assert.equal(result.code,code,JSON.stringify(result));assert.equal(response.ok,code===0);
}
async function partial(name:string,bearer=authorization) {
 const url=new URL(apiBase);const connection=createConnection({host:url.hostname,port:Number(url.port)});
 const chunks:Buffer[]=[];connection.on('data',data=>chunks.push(data));connection.on('error',()=>{});
 const closed=new Promise<void>(resolve=>connection.on('close',()=>resolve()));
 await new Promise<void>((resolve,reject)=>{connection.once('connect',resolve);connection.once('error',reject);});
 connection.write(`POST ${pathFor(name,3)} HTTP/1.1\r\nHost: ${url.host}\r\nAuthorization: ${bearer}\r\nContent-Type: application/octet-stream\r\nContent-Length: 3\r\nConnection: close\r\n\r\na`);
 return {connection,closed,response:()=>Buffer.concat(chunks).toString()};
}
let socket: WebSocket | undefined;
const firmware: { id: string; storage_path: string }[] = [];
try {
    await until(async () => (await fetch(apiBase + '/internal/health/ready')).status === 200, 'Workers not ready');
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},${identity},'approved')`;
    await redis.send('SET', [`iot:edge:auth:${identity}`, `${node}|approved`]);
    let sequence = 1n, epoch = 0n;
    const commands: Map<number, bigint | Buffer>[] = [];
    const envelope = (tag: number, payload: Buffer) => Buffer.concat([field(1, 6), field(2, bytes(crypto.randomUUID())), field(3, bytes(node)), field(4, bytes(platform)), field(5, epoch), field(6, Date.now()), field(8, sequence++), field(tag, payload)]);
    socket = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect');
    socket.binaryType = 'arraybuffer';
    await new Promise<void>((accept, reject) => {
        const timer = setTimeout(() => reject(Error('Device hello timed out')), 10000);
        socket!.onopen = () => socket!.send(envelope(20, Buffer.concat([field(1, identity), field(2, 'fixture'), field(3, '0.3.49'), field(15, 1), field(34, 1)])));
        socket!.onerror = () => { clearTimeout(timer); reject(Error('Device connection failed')); };
        socket!.onmessage = event => {
            const frame = decode(Buffer.from(event.data));
            if (frame.has(21)) { epoch = frame.get(5) as bigint; clearTimeout(timer); accept(); }
            if (frame.has(80)) socket!.send(envelope(81, frame.get(80) as Buffer));
            if (frame.has(62)) commands.push(decode(frame.get(62) as Buffer));
        };
    });
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->>'firmwareUpdate'='true'`).length > 0, 'Firmware capability missing');
    const content = Uint8Array.from({ length: 2097169 }, (_, i) => i % 256);
    await upload('fixture-firmware.bin',content);
    const [row] = await db`SELECT id,storage_path,sha256,size_bytes FROM edge_firmware WHERE file_name='fixture-firmware.bin'`;
    firmware.push(row);
    assert.equal(row.sha256, createHash('sha256').update(content).digest('hex'));
    assert.equal(Number(row.size_bytes), content.length);
    assert.deepEqual(new Uint8Array(await Bun.file(row.storage_path).arrayBuffer()), content);
    await until(async () => commands.length > 0, 'Firmware command not delivered');
    assert.equal(Number(commands[0].get(4)), content.length);
    assert.equal((commands[0].get(3) as Buffer).toString('hex'), row.sha256);
    assert.equal(commands[0].get(6), 1n);
    assert.equal((await db`SELECT count(*)::int AS count FROM edge_task WHERE node_id=${node} AND task_type='firmware'`)[0].count, 1);

    const directory=dirname(row.storage_path);
    const staged=async()=> (await readdir(directory)).filter(name=>/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.upload$/i.test(name));
    await upload('too-short.bin',new Uint8Array([1]),3,17017);
    await upload('too-long.bin',new Uint8Array([1,2,3,4]),3,17017);
    await upload('invalid.bin',new Uint8Array(),0,10001);
    await upload('invalid.bin',new Uint8Array([1]),134217729,10001);
    await upload('',new Uint8Array([1]),1,10001);
    await upload('anonymous.bin',new Uint8Array([1]),1,11004,true,'');
    assert.equal((await staged()).length,0);
    const abandoned=await partial('abandoned.bin');
    await until(async()=> (await staged()).length===1,'streaming upload not staged');
    abandoned.connection.destroy(); await abandoned.closed;
    await until(async()=> (await staged()).length===0,'disconnected upload not cleaned');
    console.log('PASS HTTP size bounds, authentication, incomplete/oversize payloads and disconnect cleanup');
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${user},${user},'unused-test-password')`;
    await db`INSERT INTO sys_role(id,name,code,permissions) VALUES(${role},${role},${role},'["iot:edge:firmware"]'::jsonb)`;
    await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${crypto.randomUUID()},${user},${role})`;
    const revoked=await partial('revoked.bin',`Bearer ${userToken(user)}`);
    await until(async()=> (await staged()).length===1,'revoked upload not staged');
    await db`UPDATE sys_role SET permissions='[]'::jsonb WHERE id=${role}`;
    revoked.connection.write('bc');
    await Promise.race([revoked.closed,Bun.sleep(15000).then(()=>{revoked.connection.destroy();throw Error('revoked upload timeout');})]);
    assert.match(revoked.response(),/"code":11007/);
    assert.equal((await staged()).length,0);
    assert.equal((await db`SELECT count(*)::int AS count FROM edge_firmware WHERE created_by=${user}`)[0].count,0);
    console.log('PASS permission revoked during upload prevents registration and removes staged file');

    await db`UPDATE edge_node SET software_version='0.3.44',capability=jsonb_set(capability,'{firmwareStream}','false'::jsonb) WHERE id=${node}`;
    const legacyContent = Uint8Array.from([0, 255, 13, 10, 128]);
    await upload('legacy-firmware.bin',legacyContent,legacyContent.length,0,false);
    const [legacy] = await db`SELECT id,storage_path,download_token FROM edge_firmware WHERE file_name='legacy-firmware.bin'`;
    firmware.push(legacy);
    const download = `${apiBase}/edge/v1/firmware/${legacy.id}/download?token=${legacy.download_token}`;
    const legacyResponse = await fetch(download);
    assert.equal(legacyResponse.status,200);
    assert.deepEqual(new Uint8Array(await legacyResponse.arrayBuffer()),legacyContent);
    assert.equal((await fetch(`${apiBase}/edge/v1/firmware/${legacy.id}/download?token=invalid`)).status,404);
    await until(async () => commands.length === 2,'Legacy firmware command missing');
    assert.equal((commands[1].get(2) as Buffer).toString(),download);
    console.log('PASS legacy firmware capability retains tokenized download, rejects invalid token and queues one task');

} finally {
    socket?.close();
    await db`DELETE FROM edge_task WHERE node_id=${node}`;
    for (const file of firmware) {
        assert(resolve(file.storage_path).startsWith(resolve(fixtureDirectory) + sep), 'Refusing to delete file outside disposable build fixture');
        await db`DELETE FROM edge_firmware WHERE id=${file.id}`;
        await unlink(file.storage_path);
    }
    await db`DELETE FROM edge_node WHERE id=${node}`;
    await db`DELETE FROM sys_user_role WHERE user_id=${user}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await db`DELETE FROM sys_user WHERE id=${user}`;
    await redis.close(); await db.close();
}
