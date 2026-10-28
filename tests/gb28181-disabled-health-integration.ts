import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase } from './architecture-fixture';

const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const id = '00000000-0000-7000-8000-000000000002';
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
    iss: 'iot-engine', aud: 'iot-engine-web', sub: id, user_id: id,
    username: 'admin', token_type: 'access', iat: now, exp: now + 3600,
})}`;
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
const response = await fetch(`${apiBase}/v1/gb28181/health`, {
    headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(15000),
});
const body = await response.json();
assert.equal(response.status, 200, JSON.stringify(body));
assert.equal(body.code, 0);
assert.equal(body.data.enabled, false);
assert.equal(body.data.status, 'disabled');
console.log('PASS disabled GB28181 health without a Collector projection');
