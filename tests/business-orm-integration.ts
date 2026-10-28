import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';

import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';

// This test intentionally uses only the disposable architecture fixture.  The
// fixture module rejects non-local database, Redis, and API endpoints.
const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const tag = `orm-business-${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
const protocolName = `${tag}-protocol`;
const linkName = `${tag}-link`;
const updatedLinkName = `${linkName}-updated`;
const deviceOneName = `${tag}-device-a`;
const deviceTwoName = `${tag}-device-b`;
const updatedDeviceOneName = `${deviceOneName}-updated`;
const keyOneName = `${tag}-key-a`;
const keyTwoName = `${tag}-key-b`;
const webhookOneName = `${tag}-webhook-a`;
const webhookTwoName = `${tag}-webhook-b`;
const pointId = crypto.randomUUID();
const deviceOneCode = `D${crypto.randomUUID().replaceAll('-', '').slice(0, 10)}`;
const deviceTwoCode = `D${crypto.randomUUID().replaceAll('-', '').slice(0, 10)}`;
const linkPort = 59000 + Math.floor(Math.random() * 500);

const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
    iss: 'iot-engine',
    aud: 'iot-engine-web',
    sub: admin,
    user_id: admin,
    username: 'business-orm-test',
    token_type: 'access',
    iat: now,
    exp: now + 3600,
})}`;
const token = `${unsigned}.${createHmac(
    'sha256',
    'architecture-test-only-access-secret-000000000',
).update(unsigned).digest('base64url')}`;
const adminHeaders = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
};

type RequestHeaders = Record<string, string>;

async function jsonRequest(
    method: string,
    path: string,
    body?: unknown,
    expectedStatus = 200,
    headers: RequestHeaders = adminHeaders,
) {
    const response = await fetch(apiBase + path, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(15000),
    });
    const text = await response.text();
    assert.equal(response.status, expectedStatus, `${method} ${path}: ${text}`);
    const result = text.length === 0 ? undefined : JSON.parse(text);
    if (expectedStatus >= 200 && expectedStatus < 300)
        assert.equal(result?.code, 0, `${method} ${path}: ${text}`);
    return result;
}

async function query(path: string) {
    return (await jsonRequest('GET', path)).data;
}

async function publicSnapshot(path: string, headers: RequestHeaders = adminHeaders) {
    const controller = new AbortController();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
        const response = await fetch(apiBase + path, {
            headers: { ...headers, Accept: 'text/event-stream' },
            signal: controller.signal,
        });
        if (response.status !== 200) {
            const text = await response.text();
            assert.equal(response.status, 200, `${path}: ${text}`);
        }
        assert(response.body, `${path}: missing SSE body`);
        reader = response.body.getReader();
        const decoder = new TextDecoder();
        let pending = '';
        for (;;) {
            const part = await reader.read();
            assert(!part.done, `${path}: SSE ended before its initial snapshot`);
            pending += decoder.decode(part.value, { stream: true });
            const normalized = pending.replaceAll('\r\n', '\n');
            const separator = normalized.indexOf('\n\n');
            if (separator < 0)
                continue;
            const frame = normalized.slice(0, separator);
            pending = normalized.slice(separator + 2);
            if (!frame.split('\n').some((line) => line === 'event: snapshot'))
                continue;
            const data = frame
                .split('\n')
                .filter((line) => line.startsWith('data:'))
                .map((line) => line.slice(5).trimStart())
                .join('\n');
            const result = JSON.parse(data);
            assert.equal(result.code, 0, `${path}: ${data}`);
            return result.data;
        }
    } finally {
        controller.abort();
        await reader?.cancel().catch(() => {});
    }
}

async function statusRequest(
    method: string,
    path: string,
    expectedStatus: number,
    headers: RequestHeaders,
) {
    const response = await fetch(apiBase + path, {
        method,
        headers: { ...headers, Accept: 'text/event-stream' },
        signal: AbortSignal.timeout(10000),
    });
    const text = await response.text();
    assert.equal(response.status, expectedStatus, `${method} ${path}: ${text}`);
}

async function until(check: () => Promise<boolean>, message: string, timeout = 15000) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        if (await check())
            return;
        await Bun.sleep(50);
    }
    throw new Error(message);
}

async function protocolId() {
    const rows = await db`SELECT id FROM protocol_config
        WHERE name=${protocolName} AND deleted_at IS NULL`;
    return rows[0]?.id as string | undefined;
}

async function linkId() {
    const rows = await db`SELECT id FROM link
        WHERE name=${updatedLinkName} AND deleted_at IS NULL`;
    return (rows[0]?.id ?? (await db`SELECT id FROM link
        WHERE name=${linkName} AND deleted_at IS NULL`)[0]?.id) as string | undefined;
}

async function deviceId(name: string) {
    const rows = await db`SELECT id FROM device
        WHERE name=${name} AND deleted_at IS NULL`;
    return rows[0]?.id as string | undefined;
}

async function closeSseDeletes(path: string, ids: string[]) {
    for (const id of [...ids].reverse()) {
        try {
            await jsonRequest('DELETE', `${path}/${id}`);
        } catch {
            // Database cleanup below is the final isolation boundary if an API
            // delete cannot finish after the test's first failure.
        }
    }
}

let protocol: string | undefined;
let link: string | undefined;
let alternateLink: string | undefined;
const devices: string[] = [];
const keys: string[] = [];
const webhooks: string[] = [];

try {
    assert.equal(await redis.send('PING', []), 'PONG');

    const protocolConfig = {
        storagePolicy: 'report',
        readInterval: 10,
        byteOrder: 'BIG_ENDIAN',
        registers: [{
            id: pointId,
            name: 'temperature',
            registerType: 'HOLDING_REGISTER',
            dataType: 'UINT16',
            address: 0,
            quantity: 1,
            writable: true,
        }],
    };
    await jsonRequest('POST', '/v1/protocol/configs', {
        protocol: 'Modbus',
        name: protocolName,
        enabled: true,
        config: protocolConfig,
        remark: `${tag}-remark`,
    });
    await until(async () => {
        protocol = await protocolId();
        return protocol !== undefined;
    }, 'protocol create did not become visible');
    const protocolList = await query('/v1/protocol/configs?page=1&pageSize=20&protocol=Modbus');
    assert(protocolList.list.some((item: { id: string }) => item.id === protocol));
    const protocolDetail = await query(`/v1/protocol/configs/${protocol}`);
    assert.equal(protocolDetail.id, protocol);
    assert.equal(protocolDetail.name, protocolName);
    assert.equal(protocolDetail.enabled, true);
    assert.equal(typeof protocolDetail.config.readInterval, 'number');
    assert.equal(protocolDetail.config.registers[0].writable, true);

    const scientific = await fetch(`${apiBase}/v1/protocol/configs/${protocol}`, {
        method: 'PUT', headers: adminHeaders, body: '{"config":{"readInterval":1e3}}',
        signal: AbortSignal.timeout(15000),
    });
    assert.equal(scientific.status, 200, await scientific.text());
    assert.equal((await query(`/v1/protocol/configs/${protocol}`)).config.readInterval, 1000);
    await jsonRequest('PUT', `/v1/protocol/configs/${protocol}`, {
        config: { registers: [{ ...protocolConfig.registers[0], scale: '1000000000.000000000001' }] },
    }, 400);

    await jsonRequest('PUT', `/v1/protocol/configs/${protocol}`, {
        enabled: false,
        config: { readInterval: 20, ormBooleanProbe: true },
        remark: `${tag}-updated-remark`,
    });
    const updatedProtocol = await query(`/v1/protocol/configs/${protocol}`);
    assert.equal(updatedProtocol.enabled, false);
    assert.equal(updatedProtocol.config.readInterval, 20);
    assert.equal(updatedProtocol.config.ormBooleanProbe, true);
    assert.equal(updatedProtocol.config.registers[0].quantity, 1);
    console.log('PASS protocol ORM create, filtered list, JSON merge update, detail and typed config');

    const linkEndpoint = {
        transport: 'tcp',
        mode: 'TCP Server',
        ip: '0.0.0.0',
        port: linkPort,
        targets: [],
    };
    await jsonRequest('POST', '/v1/link', {
        execution: 'collector',
        name: linkName,
        protocol: 'Modbus',
        endpoint: linkEndpoint,
        status: 'disabled',
    });
    await until(async () => {
        link = await linkId();
        return link !== undefined;
    }, 'link create did not become visible');
    const linkList = await query(`/v1/link?page=1&pageSize=20&keyword=${encodeURIComponent(tag)}`);
    assert(linkList.list.some((item: { id: string }) => item.id === link));
    const linkDetail = linkList.list.find((item: { id: string }) => item.id === link);
    assert.equal(linkDetail.id, link);
    assert.equal(linkDetail.name, linkName);
    assert.equal(linkDetail.endpoint.mode, 'TCP Server');
    assert.equal(typeof linkDetail.endpoint.port, 'number');
    assert.deepEqual(linkDetail.endpoint.targets, []);

    await jsonRequest('PUT', `/v1/link/${link}`, {
        execution: 'collector',
        name: updatedLinkName,
        protocol: 'Modbus',
        endpoint: linkEndpoint,
        status: 'disabled',
    });
    const updatedLink = (await query(`/v1/link?page=1&pageSize=20&keyword=${encodeURIComponent(tag)}`)).list.find((item: { id: string }) => item.id === link);
    assert.equal(updatedLink.name, updatedLinkName);
    assert.equal(updatedLink.endpoint.port, linkPort);
    console.log('PASS link ORM create, list, update and numeric endpoint JSON');

    // 链路业务约束通过真实 HTTP、ORM 与数据库验证，替代旧 SQL 字符串假对象。
    const invalidLinks = [
        [{"execution": "edge", "name": "n", "protocol": "Modbus", "endpoint": {"mode": "TCP Client", "targets": []}}, 15002, "边缘连接参数请在设备表单中配置"],
        [{"name": "n", "protocol": "Modbus", "endpoint": {"targets": []}}, 15002, "链路模式不能为空"],
        [{"name": "n", "protocol": "Modbus", "endpoint": {"mode": "", "targets": []}}, 15002, "链路模式不能为空"],
        [{"name": "n", "protocol": "Modbus", "status": "paused", "endpoint": {"mode": "TCP Client", "targets": []}}, 10001, "request validation failed"],
        [{"name": "n", "protocol": "Modbus", "endpoint": {"mode": "TCP Client"}}, 15002, "目标列表不能为空"],
        [{"name": "n", "protocol": "Modbus", "endpoint": {"mode": "UDP", "targets": []}}, 15003, "链路模式无效"],
        [{"name": "n", "protocol": "HTTP", "endpoint": {"mode": "TCP Client", "targets": []}}, 10001, "request validation failed"],
        [{"name": "n", "protocol": "SL651", "endpoint": {"mode": "TCP Client", "targets": [{"id": "a", "name": "a", "ip": "10.0.0.1", "port": 1}]}}, 15003, "SL651 只支持 TCP Server 模式"],
        [{"name": "n", "protocol": "Modbus", "endpoint": {"mode": "TCP Server", "ip": "10.0.0.1", "port": 502, "targets": []}}, 15003, "TCP Server 监听 IP 必须是 0.0.0.0"],
        [{"name": "n", "protocol": "Modbus", "endpoint": {"mode": "TCP Client", "targets": []}}, 15003, "TCP Client 至少需要一个目标地址"],
        [{"name": "n", "protocol": "Modbus", "endpoint": {"mode": "TCP Client", "targets": [{"id": "a", "name": "a", "ip": "10.0.0.1", "port": 1, "status": "paused"}]}}, 10001, "request validation failed"],
        [{"name": "n", "protocol": "Modbus", "endpoint": {"mode": "TCP Client", "targets": [{"id": "a", "name": "a", "ip": "10.0.0.1", "port": 1}, {"id": "a", "name": "b", "ip": "10.0.0.2", "port": 1}]}}, 15004, "同一链路内目标 ID 不能重复"],
    ] as const;
    for (const [body, code, message] of invalidLinks) {
        const result = await jsonRequest('POST', '/v1/link', { ...body, name: `${tag}-invalid` }, code === 15004 ? 409 : 400);
        assert.equal(result.code, code, JSON.stringify({ body, result }));
        assert.equal(result.message, message);
    }
    assert.equal(Number((await db`SELECT count(*) AS count FROM link WHERE name=${`${tag}-invalid`}`)[0].count), 0);
    const duplicatePort = await jsonRequest('POST', '/v1/link', {
        name: `${tag}-duplicate`, protocol: 'Modbus', endpoint: linkEndpoint, status: 'disabled',
    }, 409);
    assert.equal(duplicatePort.code, 15005);
    const protocolChange = await jsonRequest('PUT', `/v1/link/${link}`, {
        name: updatedLinkName, protocol: 'S7', endpoint: linkEndpoint, status: 'disabled',
    }, 400);
    assert.equal(protocolChange.code, 15006);
    const escapedName = 'A "q" \\ \n\u0001';
    const escapedLinkName = `${tag}-escaped-client`;
    await jsonRequest('POST', '/v1/link', {
        name: escapedLinkName, protocol: 'Modbus', status: 'disabled',
        endpoint: { mode: 'TCP Client', targets: [{ id: 't1', name: escapedName, ip: '192.0.2.10', port: 502 }] },
    });
    const [escapedLink] = await db`SELECT id,endpoint,execution,created_by FROM link WHERE name=${escapedLinkName}`;
    assert.equal(escapedLink.endpoint.targets[0].name, escapedName);
    assert.equal(escapedLink.endpoint.targets[0].status, 'enabled');
    assert.equal(escapedLink.execution, 'collector');
    assert.equal(escapedLink.created_by, admin);
    await closeSseDeletes('/v1/link', [escapedLink.id]);
    console.log('PASS link validation, immutable protocol, listen address conflict and escaped endpoint persistence');


    const missingNodeId = crypto.randomUUID();
    for (const method of ['POST', 'PUT']) {
        const result = await jsonRequest(method, method === 'POST' ? '/v1/link' : `/v1/link/${link}`, {
            execution: 'edge', name: `${tag}-edge-rejected`,
            protocol: 'Modbus', edge_node_id: missingNodeId,
            endpoint: { transport: 'serial', interface: '/dev/ttyS0' }, status: 'disabled',
        }, 400);
        assert.equal(result.code, 15002);
        assert.equal(result.message, '边缘连接参数请在设备表单中配置');
    }
    const rejectedChannels = await db`SELECT count(*) AS count FROM link WHERE edge_node_id=${missingNodeId}`;
    assert.equal(Number(rejectedChannels[0].count), 0);
    const preservedLink = await db`SELECT execution, name FROM link WHERE id=${link}`;
    assert.equal(preservedLink[0].execution, 'collector');
    assert.equal(preservedLink[0].name, updatedLinkName);
    console.log('PASS direct Edge link creation and update reject without writes');


    const createDevice = async (name: string, code: string, registrationContent: string) => {
        await jsonRequest('POST', '/v1/device', {
            name,
            device_code: code,
            link_id: link,
            protocol_config_id: protocol,
            status: 'disabled',
            online_timeout: 120,
            remote_control: true,
            modbus_mode: 'TCP',
            slave_id: 1,
            timezone: '+08:00',
            heartbeat: { mode: 'OFF' },
            registration: { mode: 'ASCII', content: registrationContent },
            remark: `${tag}-device-remark`,
        });
        const id = await deviceId(name);
        assert(id, `device ${name} did not become visible`);
        devices.push(id);
        return id;
    };
    const deviceOne = await createDevice(deviceOneName, deviceOneCode, 'A');
    const deviceTwo = await createDevice(deviceTwoName, deviceTwoCode, 'B');
    const deviceList = await query(`/v1/device?keyword=${encodeURIComponent(tag)}`);
    assert(deviceList.list.some((item: { id: string }) => item.id === deviceOne));
    assert(deviceList.list.some((item: { id: string }) => item.id === deviceTwo));
    const deviceDetail = await query(`/v1/device/${deviceOne}`);
    assert.equal(deviceDetail.id, deviceOne);
    assert.equal(deviceDetail.device_code, deviceOneCode);
    assert.equal(deviceDetail.protocol_config_id, protocol);
    assert.equal(deviceDetail.online_timeout, 120);
    assert.equal(typeof deviceDetail.remote_control, 'boolean');
    assert.equal(deviceDetail.remote_control, true);
    const deviceHistory = await query(
        `/v1/device/${deviceOne}/history?startTime=2026-01-01T00:00:00Z&endTime=2026-01-02T00:00:00Z&page=1&pageSize=10`,
    );
    assert(Array.isArray(deviceHistory.list));
    assert.equal(typeof deviceHistory.total, 'number');

    await jsonRequest('PUT', `/v1/device/${deviceOne}`, {
        name: updatedDeviceOneName,
        status: 'disabled',
        remote_control: false,
        remark: `${tag}-device-updated`,
    });
    const updatedDevice = await query(`/v1/device/${deviceOne}`);
    assert.equal(updatedDevice.name, updatedDeviceOneName);
    assert.equal(updatedDevice.remote_control, false);
    assert.equal(updatedDevice.status, 'disabled');
    console.log('PASS device ORM create, scoped list, detail, update and boolean JSON');

    const alternateLinkName = `${tag}-alternate-channel`;
    await jsonRequest('POST', '/v1/link', {
        execution: 'collector', name: alternateLinkName, protocol: 'Modbus',
        endpoint: { ...linkEndpoint, port: linkPort + 500 }, status: 'disabled',
    });
    alternateLink = (await db`SELECT id FROM link WHERE name=${alternateLinkName} AND deleted_at IS NULL`)[0].id;
    const channelsBefore = (await db`SELECT count(*) AS count FROM link WHERE deleted_at IS NULL`)[0].count;
    await jsonRequest('PUT', `/v1/device/${deviceOne}`, { link_id: alternateLink });
    assert.equal((await query(`/v1/device/${deviceOne}`)).link_id, alternateLink);
    assert.equal((await query(`/v1/device/${deviceTwo}`)).link_id, link);
    assert.equal((await db`SELECT id FROM link WHERE id=${link} AND deleted_at IS NULL`).length, 1);
    assert.equal((await db`SELECT count(*) AS count FROM link WHERE deleted_at IS NULL`)[0].count, channelsBefore);
    await jsonRequest('PUT', `/v1/device/${deviceOne}`, { link_id: link });
    assert.equal((await query(`/v1/device/${deviceOne}`)).link_id, link);
    assert.equal((await db`SELECT id FROM link WHERE id=${alternateLink} AND deleted_at IS NULL`).length, 1);
    console.log('PASS device connection edits preserve both shared physical channels and other devices');


    const keyOneResult = await jsonRequest('POST', '/api/open-access-key', {
        name: keyOneName,
        status: 'enabled',
        scopes: ['device:realtime'],
        deviceIds: [deviceOne],
        remark: `${tag}-key-remark`,
    });
    const keyTwoResult = await jsonRequest('POST', '/api/open-access-key', {
        name: keyTwoName,
        status: 'enabled',
        scopes: ['device:history'],
        deviceIds: [deviceTwo],
    });
    const keyOne = keyOneResult.data as { id: string; accessKey: string };
    const keyTwo = keyTwoResult.data as { id: string; accessKey: string };
    keys.push(keyOne.id, keyTwo.id);
    assert.match(keyOne.accessKey, /^ak_[0-9a-f]+$/);
    assert.match(keyTwo.accessKey, /^ak_[0-9a-f]+$/);

    const webhookOneResult = await jsonRequest('POST', '/api/open-webhook', {
        accessKeyId: keyOne.id,
        name: webhookOneName,
        url: `https://example.test/${tag}/one`,
        status: 'enabled',
        timeoutSeconds: 9,
        skipTlsVerify: true,
        headers: { 'X-Trace-Id': tag },
        eventTypes: ['device.data.reported', 'device.alert.triggered'],
        secret: `${tag}-secret`,
    });
    const webhookTwoResult = await jsonRequest('POST', '/api/open-webhook', {
        accessKeyId: keyTwo.id,
        name: webhookTwoName,
        url: `https://example.test/${tag}/two`,
        status: 'disabled',
        headers: {},
        eventTypes: ['device.command.updated'],
    });
    const webhookOne = webhookOneResult.data as { id: string };
    const webhookTwo = webhookTwoResult.data as { id: string };
    webhooks.push(webhookOne.id, webhookTwo.id);

    for (const patch of [
        { name: 42 }, { timeoutSeconds: '5' }, { timeoutSeconds: 31 },
        { skipTlsVerify: 'false' }, { eventTypes: [] }, { eventTypes: ['unsupported'] },
        { url: 'ftp://example.test/webhook' }, { headers: [] }, { accessKeyId: 'invalid' },
    ]) {
        const rejected = await jsonRequest('PUT', `/api/open-webhook/${webhookOne.id}`, patch, 400);
        assert.equal(rejected.code, 19002);
    }
    console.log('PASS webhook schema rejects invalid partial updates before persistence');

    const reservedHeaders = ['host', 'content-length', 'connection', 'x-iot-event',
        'x-iot-timestamp', 'x-iot-delivery', 'x-iot-signature', 'content-type', 'user-agent',
        'transfer-encoding', 'trailer', 'te', 'upgrade', 'expect', 'proxy-connection'];
    const headerCases = [
        '{}', '{"X-Test":"ok"}', '{"X-Test":42}', '{"X-Test":null}',
        '{"X-Test":false}', '{"X-Test":[]}', '{"X-Test":{}}',
        '{"":"ok"}', '{"X Test":"ok"}', '{"X:Test":"ok"}',
        '{"X-Test":"ok\\r\\nInjected"}',
        String.raw`{"X-Test":"ok\r\nInjected"}`,
        String.raw`{"X-Test":"ok\u000aInjected"}`,
        String.raw`{"\u0048ost":"example.test"}`,
        String.raw`{"X-Test":42,"X-\u0054est":"ok"}`,
        '{"X-Test":"ok","X-Test":42}', '{"X-Test":42,"X-Test":"ok"}',
        '{"X-Test":"ok","x-test":"also ok"}',
        String.raw`{"X-Test":"中文\tvalue"}`,
        ...reservedHeaders.map((name) => JSON.stringify({ [name.toUpperCase()]: 'blocked' })),
    ];
    // 每次出现的 Header 都须有效，不能用后续同名键覆盖非法值来绕过校验。
    const invalidDuplicateHeaders = new Set([
        String.raw`{"X-Test":42,"X-\u0054est":"ok"}`,
        '{"X-Test":"ok","X-Test":42}', '{"X-Test":42,"X-Test":"ok"}',
    ]);
    for (const headersJson of headerCases) {
        const tokenPattern = "^[!#$%&'*+.^_`|~0-9A-Za-z-]+$";
        const newlinePattern = '[\\r\\n]';
        const oracle = await db`SELECT NOT EXISTS (
            SELECT 1 FROM jsonb_each(${headersJson}::text::jsonb) AS header(key,value)
            WHERE key !~ ${tokenPattern} OR jsonb_typeof(value) <> 'string'
               OR (value #>> '{}') ~ ${newlinePattern}
               OR lower(key) IN (SELECT jsonb_array_elements_text(${JSON.stringify(reservedHeaders)}::text::jsonb))
        ) AS valid`;
        const expectedValid = oracle[0].valid && !invalidDuplicateHeaders.has(headersJson);
        const before = (await db`SELECT headers FROM open_webhook WHERE id=${webhookOne.id}`)[0].headers;
        const response = await fetch(`${apiBase}/api/open-webhook/${webhookOne.id}`, {
            method: 'PUT', headers: adminHeaders, body: `{"headers":${headersJson}}`,
            signal: AbortSignal.timeout(15000),
        });
        const result = await response.json();
        assert.equal(response.status, expectedValid ? 200 : 400, headersJson);
        assert.equal(result.code, expectedValid ? 0 : 19002, headersJson);
        const stored = (await db`SELECT headers FROM open_webhook WHERE id=${webhookOne.id}`)[0].headers;
        assert.deepEqual(stored, expectedValid ? JSON.parse(headersJson) : before, headersJson);
    }
    await jsonRequest('PUT', `/api/open-webhook/${webhookOne.id}`, { headers: { 'X-Trace-Id': tag } });
    console.log('PASS webhook Header validation rejects invalid duplicates and preserves rejected updates');



    await jsonRequest('POST', '/api/open-webhook', {
        accessKeyId: keyOne.id,
        name: `${tag}-webhook-crlf`,
        url: `https://example.test/${tag}/crlf`,
        headers: { 'X-Trace-Id': `ok\r\nX-Injected: yes` },
        eventTypes: ['device.data.reported'],
    }, 400);

    const keyList = await query('/api/open-access-key');
    const listedKeyOne = keyList.find((item: { id: string }) => item.id === keyOne.id);
    const listedKeyTwo = keyList.find((item: { id: string }) => item.id === keyTwo.id);
    assert(listedKeyOne && listedKeyTwo);
    assert.deepEqual(listedKeyOne.deviceIds, [deviceOne]);
    assert.deepEqual(listedKeyTwo.deviceIds, [deviceTwo]);
    assert(Array.isArray(listedKeyOne.scopes));
    assert.equal(typeof listedKeyOne.webhookCount, 'number');

    const webhookList = await query('/api/open-webhook');
    const listedWebhookOne = webhookList.find((item: { id: string }) => item.id === webhookOne.id);
    const listedWebhookTwo = webhookList.find((item: { id: string }) => item.id === webhookTwo.id);
    assert(listedWebhookOne && listedWebhookTwo);
    assert.equal(listedWebhookOne.accessKeyId, keyOne.id);
    assert.deepEqual(listedWebhookOne.headers, { 'X-Trace-Id': tag });
    assert(Array.isArray(listedWebhookOne.eventTypes));
    assert.deepEqual(new Set(listedWebhookOne.eventTypes), new Set([
        'device.data.reported',
        'device.alert.triggered',
    ]));
    assert.equal(typeof listedWebhookOne.timeoutSeconds, 'number');
    assert.equal(typeof listedWebhookOne.skipTlsVerify, 'boolean');
    assert.equal(listedWebhookOne.skipTlsVerify, true);
    assert.equal(listedWebhookOne.hasSecret, true);
    assert.deepEqual(listedWebhookOne.deviceIds, [deviceOne]);
    assert.equal(listedWebhookTwo.accessKeyId, keyTwo.id);
    assert.equal(listedWebhookTwo.hasSecret, false);
    assert.deepEqual(listedWebhookTwo.deviceIds, [deviceTwo]);
    assert.equal(listedKeyOne.webhookCount, 1);
    assert.equal(listedKeyTwo.webhookCount, 1);

    const filteredWebhooks = await query(`/api/open-webhook?accessKeyId=${keyOne.id}`);
    assert.equal(filteredWebhooks.length, 1);
    assert.equal(filteredWebhooks[0].id, webhookOne.id);
    console.log('PASS open-access key/webhook JSON projections, header validation and access-key filter');

    const publicHeaders = { 'X-Access-Key': keyOne.accessKey };
    await until(async () => {
        try {
            const page = await publicSnapshot('/open-api/device/list?page=1&pageSize=100', publicHeaders);
            return page.total === 1 && page.list.length === 1 && page.list[0].id === deviceOne;
        } catch {
            return false;
        }
    }, 'open-access device projection did not become visible');
    const publicDevices = await publicSnapshot('/open-api/device/list?page=1&pageSize=100', publicHeaders);
    assert.equal(publicDevices.total, 1);
    assert.deepEqual(publicDevices.list.map((item: { id: string }) => item.id), [deviceOne]);
    assert.equal(publicDevices.list[0].name, updatedDeviceOneName);
    await statusRequest(
        'GET',
        `/open-api/device/history?deviceId=${deviceOne}&startTime=2026-01-01T00:00:00Z&endTime=2026-01-02T00:00:00Z`,
        403,
        publicHeaders,
    );
    console.log('PASS open-access device scope filtering and denied history scope');
    const sampleTime = new Date();
    const historicalValue = { values: { [pointId]: { name: 'boolean-point', value: true } } };
    await db`INSERT INTO device_data(id,device_id,link_id,protocol,data,report_time,connection_id,source,occurred_at)
        VALUES(${crypto.randomUUID()},${deviceTwo},${link},'Modbus',${historicalValue}::jsonb,${sampleTime},${crypto.randomUUID()},'collector',${sampleTime})`;
    const historyStart = new Date(sampleTime.getTime() - 1000).toISOString();
    const historyEnd = new Date(sampleTime.getTime() + 1000).toISOString();
    const publicHistory = await publicSnapshot(`/open-api/device/history?deviceId=${deviceTwo}&startTime=${historyStart}&endTime=${historyEnd}`,
        { 'X-Access-Key': keyTwo.accessKey });
    assert.equal(publicHistory.total, 1);
    assert.equal(publicHistory.list[0].points[0].id, pointId);
    assert.equal(publicHistory.list[0].points[0].value, 1);
    console.log('PASS public history preserves point IDs and converts BOOL to numeric 0/1');

    const groups: string[] = [];
    for (const suffix of ['a', 'b', 'c']) {
        const name = `${tag}-group-${suffix}`;
        await jsonRequest('POST', '/v1/edge/groups', {
            name, parentId: groups.at(-1), status: 'enabled',
        });
        groups.push((await db`SELECT id FROM edge_node_group WHERE name=${name}`)[0].id);
    }
    await jsonRequest('PUT', `/v1/edge/groups/${groups[0]}`, {
        name: `${tag}-group-a`, parentId: groups[2], status: 'enabled',
    }, 409);
    await jsonRequest('PUT', `/v1/edge/groups/${groups[1]}`, {
        name: `${tag}-group-b`, parentId: groups[0], status: 'enabled',
    });
    assert.equal((await db`SELECT parent_id FROM edge_node_group WHERE id=${groups[1]}`)[0].parent_id, groups[0]);
    for (const group of groups.reverse()) await jsonRequest('DELETE', `/v1/edge/groups/${group}`);
    console.log('PASS Edge group recursion rejects descendants and accepts existing ancestors');

} finally {
    await closeSseDeletes('/api/open-webhook', webhooks);
    await closeSseDeletes('/api/open-access-key', keys);
    await closeSseDeletes('/v1/device', devices);
    if (alternateLink) await closeSseDeletes('/v1/link', [alternateLink]);
    if (link)
        await closeSseDeletes('/v1/link', [link]);
    if (protocol)
        await closeSseDeletes('/v1/protocol/configs', [protocol]);

    // Keep cleanup scoped to this run's random prefix.  Soft deletion mirrors
    // the application lifecycle and leaves no cross-test visible rows if an
    // API delete was interrupted.
    await db`DELETE FROM open_access_key_device
        WHERE access_key_id IN (SELECT id FROM open_access_key WHERE name LIKE ${`${tag}%`})`;
    await db`UPDATE open_webhook SET deleted_at=COALESCE(deleted_at,NOW()), updated_at=NOW()
        WHERE name LIKE ${`${tag}%`}`;
    await db`UPDATE open_access_key SET deleted_at=COALESCE(deleted_at,NOW()), updated_at=NOW()
        WHERE name LIKE ${`${tag}%`}`;
    await db`UPDATE device SET deleted_at=COALESCE(deleted_at,NOW()), updated_at=NOW()
        WHERE name LIKE ${`${tag}%`}`;
    await db`UPDATE link SET deleted_at=COALESCE(deleted_at,NOW()), updated_at=NOW()
        WHERE name LIKE ${`${tag}%`}`;
    await db`UPDATE protocol_config SET deleted_at=COALESCE(deleted_at,NOW()), updated_at=NOW()
        WHERE name LIKE ${`${tag}%`}`;
    await db`UPDATE edge_node_group SET deleted_at=COALESCE(deleted_at,NOW()) WHERE name LIKE ${`${tag}%`}`;
    await db.close();
    redis.close();
}
