import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl } from './architecture-fixture';

// The fixture guard only permits a disposable local database and API.
const sql = new Bun.SQL(databaseUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const tag = `http_${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
function accessToken(id: string) {
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const now = Math.floor(Date.now() / 1000);
    const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
        iss: 'iot-engine', aud: 'iot-engine-web', sub: id, user_id: id,
        username: tag, token_type: 'access', iat: now, exp: now + 3600,
    })}`;
    return `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}
const token = accessToken(admin);
async function request(method: string, path: string, body?: unknown, status = 200, bearer: string | null = token) {
    const headers = new Headers({ Accept: 'application/json' });
    if (bearer) headers.set('Authorization', `Bearer ${bearer}`);
    if (body !== undefined) headers.set('Content-Type', 'application/json');
    const response = await fetch(apiBase + path, {
        method, headers, body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(10000),
    });
    const text = await response.text();
    assert.equal(response.status, status, `${method} ${path}: ${text}`);
    assert.match(response.headers.get('content-type') ?? '', /application\/json/);
    const result = JSON.parse(text);
    if (status === 200) assert.equal(result.code, 0);
    return result;
}

try {
    await request('GET', '/v1/departments', undefined, 401, null);
    await request('GET', '/v1/users', undefined, 401, null);
    await request('POST', '/v1/roles', { name: tag, code: tag, permissions: [] });
    const [role] = await sql`SELECT id FROM sys_role WHERE code=${tag}`;
    assert(role);
    await request('PUT', `/v1/roles/${role.id}`, { description: 'HTTP update', permissions: [] });
    assert.equal((await request('GET', `/v1/roles?keyword=${tag}`)).data.list[0].description, 'HTTP update');
    assert.equal((await request('GET', `/v1/roles?keyword=${tag}&pageSize=1`)).data.total, 1);

    await request('POST', '/v1/departments', { name: tag, code: tag });
    const [root] = await sql`SELECT id FROM sys_department WHERE code=${tag}`;
    await request('POST', '/v1/departments', { name: `${tag}_child`, code: `${tag}_child`, parent_id: root.id });
    const [child] = await sql`SELECT id FROM sys_department WHERE code=${`${tag}_child`}`;
    assert.equal((await request('GET', `/v1/departments?keyword=${tag}`)).data.total, 2);
    const roots = (await request('GET', `/v1/departments?keyword=${tag}&parent_id=`)).data;
    assert.equal(roots.total, 1);
    assert.equal(roots.list[0].id, root.id);
    assert.equal((await request('GET', `/v1/departments?parent_id=${root.id}`)).data.list[0].id, child.id);
    assert.equal((await request('PUT', `/v1/departments/${root.id}`, { parent_id: child.id }, 400)).code, 14003);
    assert.equal((await request('DELETE', `/v1/departments/${root.id}`, undefined, 409)).code, 14005);
    await request('PUT', `/v1/departments/${child.id}`, { parent_id: '', leader_id: '' });
    assert.equal((await request('GET', `/v1/departments?keyword=${tag}_child`)).data.list[0].parent_id, '');

    await request('POST', '/v1/users', {
        username: tag, password: 'disposable-test-password', role_ids: [role.id], department_id: root.id,
    });
    const [user] = await sql`SELECT id FROM sys_user WHERE username=${tag}`;
    assert.equal((await request('GET', `/v1/users?keyword=${tag}`)).data.list[0].roles[0].id, role.id);
    assert.equal((await request('GET', `/v1/users?keyword=${tag.toUpperCase()}`)).data.total, 1);
    assert.equal((await request('GET', '/v1/users', undefined, 403, accessToken(user.id))).code, 11007);
    await request('PUT', `/v1/users/${user.id}`, { nickname: 'HTTP update', department_id: '' });
    const changed = (await request('GET', `/v1/users?keyword=${tag}`)).data.list[0];
    assert.equal(changed.nickname, 'HTTP update');
    assert.equal(changed.department_id, '');
    assert.equal((await request('DELETE', `/v1/users/${admin}`, undefined, 400)).code, 12004);
    await request('DELETE', `/v1/users/${user.id}`);
    await request('DELETE', `/v1/departments/${child.id}`);
    await request('DELETE', `/v1/departments/${root.id}`);
    await request('DELETE', `/v1/roles/${role.id}`);
    assert.equal((await request('GET', `/v1/users?keyword=${tag}`)).data.total, 0);
    assert.equal((await request('GET', `/v1/departments?keyword=${tag}`)).data.total, 0);
    assert.equal((await request('GET', `/v1/roles?keyword=${tag}`)).data.total, 0);
    console.log('PASS HTTP role/department/user CRUD, permissions, nullable fields, root filtering and cycle protection');
} finally {
    await sql`DELETE FROM sys_user_role WHERE user_id IN (SELECT id FROM sys_user WHERE username=${tag})`;
    await sql`DELETE FROM sys_user WHERE username=${tag}`;
    await sql`DELETE FROM sys_department WHERE code=${`${tag}_child`}`;
    await sql`DELETE FROM sys_department WHERE code=${tag}`;
    await sql`DELETE FROM sys_role WHERE code=${tag}`;
    await sql.close();
}
