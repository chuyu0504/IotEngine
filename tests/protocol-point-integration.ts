import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase } from './architecture-fixture';

const admin = '00000000-0000-7000-8000-000000000002';
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
function token(userId = admin) {
    const now = Math.floor(Date.now() / 1000);
    const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
        iss: 'iot-engine', aud: 'iot-engine-web', sub: userId, user_id: userId,
        username: 'admin', token_type: 'access', iat: now, exp: now + 600,
    })}`;
    return `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}
async function request(path: string, body?: unknown, method = body === undefined ? 'GET' : 'POST', access: string | null = token()) {
    const response = await fetch(apiBase + path, {
        method, headers: { 'Content-Type': 'application/json', ...(access ? { Authorization: `Bearer ${access}` } : {}) },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: response.status, body: await response.json() as any };
}
const run = crypto.randomUUID();
const baselineId = crypto.randomUUID();
const configurations: string[] = [];
const point = { name: '电压', unit: 'V', identifier: '02010100', dataType: 'BCD', length: 2, digits: 1, writable: false };
try {
    const created = await request('/v1/protocol/configs', {
        protocol: 'DLT645', name: `point-qa-${run}`, config: {
            storagePolicy: 'report', readInterval: 30, connection: { version: '2007', wakeupBytes: 4 },
            points: [{ id: baselineId, name: '总电能', unit: 'kWh', identifier: '00000000', dataType: 'BCD', length: 4, digits: 2 }],
        },
    });
    assert.equal(created.status, 200, JSON.stringify(created.body));
    const listed = await request('/v1/protocol/configs?page=1&pageSize=1000');
    const configuration = listed.body.data.list.find((item: any) => item.name === `point-qa-${run}`);
    assert(configuration);
    const id = configuration.id;
    configurations.push(id);
    const path = `/v1/protocol/configs/${id}/points`;
    assert.equal((await request(path, {}, 'POST', null)).status, 401);
    const denied = await request(path, {}, 'POST', token(crypto.randomUUID()));
    assert([401, 403].includes(denied.status), JSON.stringify(denied.body));
    assert.equal((await request(`/v1/protocol/configs/${crypto.randomUUID()}/points`, point)).status, 404);
    const additions = await Promise.all([
        request(path, { ...point, visible: false, id: baselineId }),
        request(path, { ...point, name: '电流', identifier: '02020100', unit: 'A', length: 3, digits: 3 }),
    ]);
    for (const addition of additions) assert.equal(addition.status, 200, JSON.stringify(addition.body));
    let detail = (await request(`/v1/protocol/configs/${id}`)).body.data;
    assert.equal(detail.config.points.length, 3, 'concurrent additions lost a point');
    assert.equal(detail.config.points[0].id, baselineId, 'existing point identity changed');
    const voltage = detail.config.points.find((value: any) => value.name === '电压');
    const current = detail.config.points.find((value: any) => value.name === '电流');
    for (const added of [voltage, current]) {
        assert.match(added.id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
        assert.notEqual(added.id, baselineId, 'client supplied the point identity');
        assert.equal(Object.hasOwn(added, 'visible'), false);
    }
    assert.notEqual(voltage.id, current.id);
    assert.equal(detail.config.pointVisibility[voltage.id], false, 'new generated ID lost hidden visibility');
    const beforeInvalid = JSON.stringify(detail.config);
    for (const invalid of [{ ...point, name: '' }, { ...point, length: 9 }, { ...point, identifier: 'ZZZZZZZZ' }]) {
        const result = await request(path, invalid);
        assert.equal(result.status, 400, JSON.stringify(result.body));
        assert.equal(result.body.code, 16004);
    }
    detail = (await request(`/v1/protocol/configs/${id}`)).body.data;
    assert.equal(JSON.stringify(detail.config), beforeInvalid, 'failed validation changed stored configuration');
    const editedConfig = { ...detail.config, points: detail.config.points.map((value: any) => value.id === voltage.id ? { ...value, name: '电压编辑' } : value) };
    assert.equal((await request(`/v1/protocol/configs/${id}`, { config: editedConfig }, 'PUT')).status, 200);
    const edited = (await request(`/v1/protocol/configs/${id}`)).body.data;
    assert.equal(edited.config.points.find((value: any) => value.name === '电压编辑').id, voltage.id);
    assert.equal(edited.config.pointVisibility[voltage.id], false);
    console.log('PASS DL/T645 point HTTP: server UUIDv7, concurrent append, stable existing IDs, hidden visibility, permission order and validation rollback');
} finally {
    for (const id of configurations) {
        const deleted = await request(`/v1/protocol/configs/${id}`, undefined, 'DELETE');
        assert.equal(deleted.status, 200, JSON.stringify(deleted.body));
    }
}
