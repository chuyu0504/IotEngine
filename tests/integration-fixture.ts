// Database and Redis preparation for ports/cmake/integration-fixture.cmake.
// Usage: bun tests/integration-fixture.ts <reset|drop> <server-url> <database> <redis-url>
// The server URL names any database on the shared test server; the fixture
// database is recreated (reset) or removed (drop) and the Redis database index
// in <redis-url> is flushed.
const [action, serverUrl, database, redisUrl] = Bun.argv.slice(2);
if ((action !== 'reset' && action !== 'drop') || !serverUrl || !redisUrl || !/^iot_architecture_[a-z0-9_]+$/.test(database ?? '')) {
    throw new Error('usage: integration-fixture.ts <reset|drop> <server-url> <iot_architecture_*> <redis-url>');
}
if (!/^\/[0-9]+$/.test(new URL(redisUrl).pathname)) throw new Error('Fixture Redis URL must select a database index');

const server = new Bun.SQL(serverUrl);
try {
    await server.unsafe(`DROP DATABASE IF EXISTS "${database}" WITH (FORCE)`);
    if (action === 'reset') await server.unsafe(`CREATE DATABASE "${database}"`);
} finally {
    await server.close();
}
const redis = new Bun.RedisClient(redisUrl);
try {
    await redis.send('FLUSHDB', []);
} finally {
    redis.close();
}
