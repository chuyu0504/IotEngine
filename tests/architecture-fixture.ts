import { sharedChannel } from './redis_channel.fixture';

// Endpoints of the disposable fixture started by ports/cmake/integration-fixture.cmake
// (`ctest -L integration`). Refuse anything but a local fixture database.
function localUrl(name: string, protocol: string, database?: RegExp) {
    const value = Bun.env[name];
    if (!value) throw new Error(`${name} is not set; run integration tests through CTest`);
    const url = new URL(value);
    if (url.protocol !== protocol || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)
        || (database && !database.test(url.pathname))) {
        throw new Error('Architecture integration tests require a local disposable fixture');
    }
    return value;
}

export const databaseUrl = localUrl('ARCHITECTURE_DATABASE_URL', 'postgres:', /^\/iot_architecture_[a-z0-9_]+$/);
export const redisUrl = localUrl('ARCHITECTURE_REDIS_URL', 'redis:');
export const apiBase = localUrl('TEST_BASE_URL', 'http:');

// Working directory of the first iot-engine instance; its relative paths such
// as firmware/ resolve here.
export const fixtureDirectory = Bun.env.IOT_TEST_FIXTURE_DIRECTORY ?? '';
if (!fixtureDirectory) throw new Error('IOT_TEST_FIXTURE_DIRECTORY is not set; run integration tests through CTest');

// Synthetic integration inputs obey the same durable-entry + wake contract as
// the real producers. Do not rely on the recovery timer to make tests progress.
export async function publishFixtureEvent(redis: Bun.RedisClient, stream: string,
    fields: string[], task: string) {
    const wakes = await redis.send('KEYS', ['iot:service:worker:*:wake']) as string[];
    if (!wakes.length) throw new Error('Start disposable API workers before publishing fixture input');
    return redis.send('EVAL', [
        `local id=redis.call('XADD',KEYS[1],'*',unpack(ARGV,2));
         redis.call('PUBLISH',KEYS[2],id);
         for i=3,#KEYS,2 do
           local wake=redis.call('XADD',KEYS[i],'*','task',ARGV[1]);
           redis.call('PUBLISH',KEYS[i+1],wake);
         end;
         return id`,
        String((wakes.length + 1) * 2),
        ...[stream, ...wakes].flatMap((key) => [key, sharedChannel(redisUrl, `consumer:input:${key}`)]),
        task, ...fields]);
}
