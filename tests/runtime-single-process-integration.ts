// One disposable local process must already be running with media and VPN enabled.
import assert from 'node:assert/strict';
import { apiBase as base } from './architecture-fixture';

const health = await fetch(`${base}/internal/health/ready`, { signal: AbortSignal.timeout(5000) });
assert.equal(health.status, 200, 'Single process is not ready');
const state = await health.json();
for (const component of ['collector', 'telemetry', 'edge-projector', 'outbox', 'gb28181-projector', 'vpn']) {
    assert.equal(state.components?.[component]?.state, 'ready', `${component} must run in the same process`);
}
for (const path of ['/v1/device', '/v1/gb28181/health', '/v1/vpn/networks']) {
    const response = await fetch(base + path, { signal: AbortSignal.timeout(5000) });
    assert.equal(response.status, 401, `Expected authenticated route on the shared listener: ${path}`);
    await response.body?.cancel();
}
console.log('PASS single-process readiness and shared API/media/VPN listener');
