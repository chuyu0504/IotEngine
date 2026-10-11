import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl } from './architecture-fixture';

// Run only against the local disposable fixture guarded by architecture-fixture.
const sql = new Bun.SQL(databaseUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const tag = `orm_${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
    iss: 'iot-engine', aud: 'iot-engine-web', sub: admin, user_id: admin,
    username: 'admin', token_type: 'access', iat: now, exp: now + 3600,
})}`;
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
let activeToken = token;
async function write(method: string, path: string, body?: unknown, code = 0) {
    const response = await fetch(apiBase + path, {
        method, headers: { Authorization: `Bearer ${activeToken}`, 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(10000),
    });
    const reply = await response.json();
    assert.equal(reply.code, code, `${method} ${path}: ${JSON.stringify(reply)}`);
    assert.equal(response.ok, code === 0);
    return reply.data;
}
async function query(path: string, params: Record<string, string | number> = {}) {
    const search = new URLSearchParams(Object.entries(params).map(([key, value]) => [key, String(value)]));
    return write('GET', path + (search.size ? `?${search}` : ''));
}
async function listed(path: string, id: string) {
    const item = (await query(path, { keyword: tag })).list.find((row: { id: string }) => row.id === id);
    assert(item, `${path} does not list ${id}`);
    return item;
}

const roles: string[] = [], departments: string[] = [], users: string[] = [];
try {
    activeToken = '';
    await write('GET', '/v1/departments', undefined, 11004);
    await write('GET', '/v1/users', undefined, 11004);
    activeToken = token;
    for (const suffix of ['a', 'b']) {
        const code = `${tag}_${suffix}`;
        await write('POST', '/v1/roles', { name: code, code, permissions: ['z:test', 'a:test'] });
        const [row] = await sql`SELECT id, description, permissions FROM sys_role WHERE code=${code}`;
        roles.push(row.id);
        assert.equal(row.description, null);
        assert.deepEqual(row.permissions, ['z:test', 'a:test']);
    }
    assert.deepEqual((await listed('/v1/roles', roles[0])).permissions, ['a:test', 'z:test']);
    const page = await query('/v1/roles', {keyword:tag.toUpperCase(), page:2, pageSize:1});
    assert.equal(page.total, 2); assert.equal(page.list.length, 1); assert.equal(page.totalPages, 2);
    const empty = await query('/v1/roles', {keyword:tag, page:9, pageSize:1});
    assert.equal(empty.total, 2); assert.deepEqual(empty.list, []);
    assert.equal((await query('/v1/roles', {keyword:"' OR TRUE --"})).total, 0);
    await write('PUT', `/v1/roles/${roles[0]}`, { permissions: [], description: '' });
    assert.deepEqual((await listed('/v1/roles', roles[0])).permissions, []);
    const escapedPermissions = ['scope:"\\权限', 'scope:"\\权限'];
    await write('PUT', `/v1/roles/${roles[0]}`, { permissions: escapedPermissions });
    const [escapedRole] = await sql`SELECT permissions FROM sys_role WHERE id=${roles[0]}`;
    assert.deepEqual(escapedRole.permissions, escapedPermissions);
    assert.deepEqual((await listed('/v1/roles', roles[0])).permissions, escapedPermissions);
    await write('PUT', `/v1/roles/${roles[0]}`, { permissions: [] });
    await write('PUT', `/v1/roles/${roles[0]}`, { permissions: ['bad,permission'] }, 13005);
    assert.deepEqual((await listed('/v1/roles', roles[0])).permissions, []);
    assert((await query('/v1/roles/options')).some((row: { id: string }) => row.id === roles[0]));
    console.log('PASS role permissions, parameter binding, options and empty-page count');

    await write('POST', '/v1/departments', { name: `${tag}_root`, code: `${tag}_root` });
    const [root] = await sql`SELECT id, parent_id, leader_id FROM sys_department WHERE code=${`${tag}_root`}`;
    departments.push(root.id);
    assert.equal(root.parent_id, null); assert.equal(root.leader_id, null);
    assert.equal((await query('/v1/departments', { parent_id: root.id })).total, 0);
    await write('POST', '/v1/departments', { name: `${tag}_child`, code: `${tag}_child`, parent_id: root.id, leader_id: admin, sort_order: 3 });
    const [child] = await sql`SELECT id FROM sys_department WHERE code=${`${tag}_child`}`;
    departments.push(child.id);
    assert.equal((await query('/v1/departments', { parent_id: root.id })).list[0].id, child.id);
    const detail = await listed('/v1/departments', child.id);
    assert.equal(detail.parent_name, `${tag}_root`); assert.equal(detail.leader_id, admin);
    assert.match(detail.created_at, /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
    assert.equal((await query('/v1/departments', {parent_id:root.id, keyword:tag.toUpperCase()})).total, 1);
    assert.equal((await query('/v1/departments', {parent_id:'', keyword:tag})).total, 1);
    await write('PUT', `/v1/departments/${root.id}`, { parent_id: child.id }, 14003);
    await write('POST', '/v1/departments', { name: `${tag}_grandchild`, code: `${tag}_grandchild`, parent_id: child.id });
    const [grandchild] = await sql`SELECT id FROM sys_department WHERE code=${`${tag}_grandchild`}`;
    departments.push(grandchild.id);
    await write('PUT', `/v1/departments/${root.id}`, { parent_id: grandchild.id }, 14003);
    await write('PUT', `/v1/departments/${grandchild.id}`, { parent_id: root.id });
    await write('DELETE', `/v1/departments/${grandchild.id}`, undefined);
    await write('DELETE', `/v1/departments/${root.id}`, undefined, 14005);
    await write('PUT', `/v1/departments/${child.id}`, { parent_id: '', leader_id: '', code: '', status: 'disabled' });
    const [cleared] = await sql`SELECT parent_id, leader_id, code FROM sys_department WHERE id=${child.id}`;
    assert.equal(cleared.parent_id, null); assert.equal(cleared.leader_id, null); assert.equal(cleared.code, null);
    assert((await query('/v1/departments/options')).some((row: { id: string }) => row.id === child.id));
    console.log('PASS department joins, UTC timestamps, nullable fields and recursive cycle guard');

    await write('POST', '/v1/users', { username: tag, password: 'orm-test-only-password', department_id: root.id, role_ids: roles });
    const [user] = await sql`SELECT id, nickname, phone, email FROM sys_user WHERE username=${tag}`;
    users.push(user.id);
    assert.equal(user.nickname, null); assert.equal(user.phone, null); assert.equal(user.email, null);
    assert.equal((await listed('/v1/users', user.id)).roles.length, 2);
    assert.equal((await query('/v1/users', {keyword:tag.toUpperCase(), status:'enabled'})).total, 1);
    assert.equal((await query('/v1/users/options', {keyword:tag})).length, 1);
    await write('POST', '/v1/auth/login', { username: tag, password: 'wrong-password' }, 11001);
    const login = await write('POST', '/v1/auth/login', { username: tag, password: 'orm-test-only-password' });
    activeToken = login.token;
    assert.equal(login.user.id, user.id);
    assert.equal(login.user.nickname ?? '', '');
    assert.equal((await query('/v1/auth/me')).id, user.id);
    const refreshed = await write('POST', '/v1/auth/refresh', { refresh_token: login.refresh_token });
    assert.equal(refreshed.user.id, user.id);
    assert.equal(refreshed.user.username, tag);
    assert.equal((await query('/v1/auth/me')).nickname ?? '', '');
    await write('GET', '/v1/departments', undefined, 11007);
    await write('GET', '/v1/users', undefined, 11007);
    activeToken = token;
    await write('DELETE', `/v1/users/${admin}`, undefined, 12004);
    await write('PUT', `/v1/users/${admin}`, { status:'disabled'}, 12003);
    await write('PUT', `/v1/users/${admin}`, { role_ids:[roles[0]]}, 12003);
    await write('DELETE', `/v1/roles/${roles[0]}`, undefined, 13004);
    await write('PUT', `/v1/users/${user.id}`, { role_ids: [roles[0], roles[0]] }, 12005);
    assert.equal((await listed('/v1/users', user.id)).roles.length, 2);
    assert.equal((await listed('/v1/users', user.id)).status, 'enabled');
    await write('PUT', `/v1/users/${user.id}`, { role_ids: [roles[1]], department_id: '', nickname: '', status: 'disabled' });
    const changedUser = await listed('/v1/users', user.id);
    assert.equal(changedUser.status, 'disabled');
    assert.equal(changedUser.roles.length, 1);
    assert.equal(changedUser.department_id, '');
    const [updated] = await sql`SELECT department_id, nickname FROM sys_user WHERE id=${user.id}`;
    assert.equal(updated.department_id, null); assert.equal(updated.nickname, '');
    assert.equal((await listed('/v1/users', user.id)).roles.length, 1);
    assert.equal((await query('/v1/users/options', {keyword:tag})).length, 0);
    await write('POST', '/v1/auth/login', { username: tag, password: 'orm-test-only-password' }, 11002);
    await write('POST', '/v1/auth/refresh', { refresh_token: login.refresh_token }, 11002);

    // Force a database failure after the user update / membership deletion to
    // verify that all three writes share the original transaction.
    await sql.unsafe(`CREATE FUNCTION orm_test_reject_binding() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected binding failure'; END $$`);
    await sql.unsafe('CREATE TRIGGER orm_test_reject_binding BEFORE INSERT ON sys_user_role FOR EACH ROW EXECUTE FUNCTION orm_test_reject_binding()');
    try {
        await write('POST', '/v1/users', { username: `${tag}_rollback`, password: 'orm-test-only-password', role_ids: [roles[0]] }, 10004);
        const [failedCreate] = await sql`SELECT count(*)::int AS count FROM sys_user WHERE username=${`${tag}_rollback`}`;
        assert.equal(failedCreate.count, 0, 'failed membership insert must roll back the ORM user insert');
        await write('PUT', `/v1/users/${user.id}`, { nickname: 'must roll back', role_ids: [roles[0]] }, 10004);
        const [retained] = await sql`SELECT nickname FROM sys_user WHERE id=${user.id}`;
        assert.equal(retained.nickname, '');
        const membership = await sql`SELECT role_id FROM sys_user_role WHERE user_id=${user.id}`;
        assert.deepEqual(membership.map((row: { role_id: string }) => row.role_id), [roles[1]]);
    } finally {
        await sql.unsafe('DROP TRIGGER orm_test_reject_binding ON sys_user_role');
        await sql.unsafe('DROP FUNCTION orm_test_reject_binding()');
    }
    await write('DELETE', `/v1/users/${user.id}`, undefined);
    await write('POST', '/v1/auth/login', { username: tag, password: 'orm-test-only-password' }, 11001);
    await write('POST', '/v1/auth/refresh', { refresh_token: login.refresh_token }, 11008);
    assert.equal((await query('/v1/users', {keyword:tag})).total, 0);
    await write('DELETE', `/v1/roles/${roles[1]}`, undefined);
    await write('POST', '/v1/roles', { name: 'duplicate deleted code', code: `${tag}_b` }, 13002);
    await write('DELETE', `/v1/departments/${root.id}`, undefined);
    await write('POST', '/v1/departments', { name: 'duplicate deleted code', code: `${tag}_root` }, 14002);
    console.log('PASS user role replacement, database-failure rollback, soft deletion and reserved codes');
    for (const path of ['/v1/users', '/v1/departments']) {
        assert.equal((await fetch(apiBase+path)).status, 401);
    }
    console.log('PASS department/user HTTP permissions, explicit reads and unauthenticated rejection');
} finally {
    for (const id of users) { await sql`DELETE FROM sys_user_role WHERE user_id=${id}`; await sql`DELETE FROM sys_user WHERE id=${id}`; }
    for (const id of roles) await sql`DELETE FROM sys_role WHERE id=${id}`;
    for (const id of departments.reverse()) await sql`DELETE FROM sys_department WHERE id=${id}`;
    await sql.close();
}
