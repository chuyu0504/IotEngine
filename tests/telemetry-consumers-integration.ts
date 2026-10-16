import { databaseUrl, redisUrl, publishFixtureEvent } from './architecture-fixture';
// Fault injection uses only the disposable local PostgreSQL/Redis fixture.
import assert from 'node:assert/strict';
const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const id = () => crypto.randomUUID();
const device = id(),
    link = id(),
    model = id(),
    event = id(),
    point = id();
const admin = '00000000-0000-7000-8000-000000000002';
const slDevice = id(), slLink = id(), slModel = id();
let unlock: (() => void) | undefined;
let blocking: Promise<unknown> | undefined;
async function until(test: () => Promise<boolean>, message: string) {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
        if (await test()) return;
        await Bun.sleep(50);
    }
    throw new Error(message);
}
const stream = 'iot:v3:telemetry';
try {
    const config = {
        registers: [
            {
                id: point,
                name: 'temperature',
                unit: 'C',
                registerType: 'HOLDING_REGISTER',
                dataType: 'UINT16',
                address: 0,
                quantity: 1,
            },
        ],
        storagePolicy: 'report',
    };
    await db`INSERT INTO protocol_config(id,name,protocol,config,created_by) VALUES(${model},${model},'Modbus',${config}::jsonb,${admin})`;
    await db`INSERT INTO link(id,name,protocol,endpoint,created_by,execution,status)
        VALUES(${link},${link},'Modbus','{"transport":"tcp","mode":"TCP Server","ip":"0.0.0.0","port":55199,"targets":[]}'::jsonb,${admin},'collector','disabled')`;
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
        VALUES(${device},${device},${link},${model},'{"device_code":"1","slave_id":1,"modbus_mode":"TCP"}'::jsonb,${admin})`;
    // The API create path initializes these before acknowledging creation.
    await redis.send('HSET', [
        `iot:v2:runtime:device:${device}`,
        'device_id',
        device,
        'device_code',
        '1',
    ]);
    const now = String(Date.now());
    const values = JSON.stringify({
        values: { [point]: { name: 'temperature', unit: 'C', value: 42 } },
    });
    const fields = [
        'acquisition_id',
        event,
        'raw_packet_ids',
        '[]',
        'message_id',
        event,
        'causation_id',
        event,
        'link_id',
        link,
        'device_id',
        device,
        'device_code',
        '1',
        'protocol',
        'Modbus',
        'connection_id',
        id(),
        'occurred_at_ms',
        now,
        'observed_at_ms',
        now,
        'model_id',
        model,
        'storage_policy',
        'report',
        'source',
        'collector',
        'online_window_ms',
        '60000',
        'values_json',
        values,
        'raw_payload_hex',
        '[]',
    ];
    let locked!: () => void;
    const acquired = new Promise<void>((resolve) => {
        locked = resolve;
    });
    const release = new Promise<void>((resolve) => {
        unlock = resolve;
    });
    blocking = db.begin(async (tx) => {
        await tx`LOCK TABLE device_data IN ACCESS EXCLUSIVE MODE`;
        locked();
        await release;
    });
    await acquired;
    await publishFixtureEvent(redis, stream, fields, 'telemetry');
    await until(
        async () => !!(await redis.send('HGET', [`iot:v2:device:${device}:latest`, point])),
        'latest consumer was blocked by history persistence'
    );
    await until(
        async () =>
            (await db`SELECT 1 FROM alert_input_state WHERE device_id=${device}`).length === 1,
        'alert consumer was blocked by history persistence'
    );
    assert(Number(await redis.send('XLEN', [`${stream}:history`])) >= 1);
    console.log('PASS latest and alert consumers progress while history table is locked');
    unlock();
    await blocking;
    blocking = undefined;
    await until(
        async () => (await db`SELECT 1 FROM device_data WHERE device_id=${device}`).length === 1,
        'history consumer did not recover after lock release'
    );
    const rows =
        await db`SELECT model_id,data FROM device_data WHERE device_id=${device}`;
    assert.equal(rows[0].model_id, model);
    assert.equal(rows[0].data.values[point].value_type, 'number');
    assert.equal(rows[0].data.values[point].quality, 'good');
    assert.equal(rows[0].data.values[point].sample_time_ms, Number(now));
    await publishFixtureEvent(redis, stream, fields, 'telemetry');
    await Bun.sleep(500);
    assert.equal(
        (await db`SELECT count(*)::int AS count FROM device_data WHERE device_id=${device}`)[0]
            .count,
        1
    );
    console.log('PASS history recovery, typed provenance and duplicate ingestion');

    await db`INSERT INTO protocol_config(id,name,protocol,config,created_by)
        VALUES(${slModel},${slModel},'SL651','{"storagePolicy":"report","elements":[]}'::jsonb,${admin})`;
    await db`INSERT INTO link(id,name,protocol,endpoint,created_by,execution,status)
        VALUES(${slLink},${slLink},'SL651','{"transport":"tcp","mode":"TCP Server","ip":"0.0.0.0","port":55200,"targets":[]}'::jsonb,${admin},'collector','disabled')`;
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
        VALUES(${slDevice},${slDevice},${slLink},${slModel},'{"device_code":"0001000102"}'::jsonb,${admin})`;
    const raw = '7E7E010001000102FFFA320031020025260914142500F1F1000100010249F0F02609141425392300000587371B000125272B0000000001FFB028000000000303CF39';
    // The collector derives one acquisition ID per SL651 report and reuses it for retransmissions.
    async function publishSlReport(acquisition: string, payload: string, sampleTime: string, receivedTime: string) {
        const report = [...fields];
        const replace = (key: string, value: string) => { report[report.indexOf(key) + 1] = value; };
        replace('acquisition_id', acquisition);
        replace('message_id', id());
        replace('causation_id', id());
        replace('connection_id', id());
        replace('device_id', slDevice);
        replace('link_id', slLink);
        replace('model_id', slModel);
        replace('protocol', 'SL651');
        replace('observed_at_ms', sampleTime);
        replace('occurred_at_ms', receivedTime);
        replace('raw_payload_hex', JSON.stringify([payload]));
        await publishFixtureEvent(redis, stream, report, 'telemetry');
    }
    const slReport = id();
    await publishSlReport(slReport, raw, now, now);
    await until(async () => (await db`SELECT 1 FROM device_data WHERE device_id=${slDevice}`).length === 1,
        'SL651 initial report did not persist');
    const stored = (await db`SELECT id FROM device_data WHERE device_id=${slDevice}`)[0].id;
    assert.equal(stored, slReport, 'history identity must be the collector acquisition ID');
    await Promise.all([5000, 10000].map(offset => publishSlReport(slReport, raw, now, String(Number(now) + offset))));
    // Simulate receipt expiration: durable uniqueness must also survive Redis receipt loss.
    await Bun.sleep(500);
    await redis.send('DEL', [`iot:telemetry:fanout:${stored}`]);
    await publishSlReport(slReport, raw, now, String(Number(now) + 15000));
    // A later sample acts as a processing barrier and must remain a separate record.
    await publishSlReport(id(), raw, String(Number(now) + 60000), String(Number(now) + 60000));
    await until(async () => (await db`SELECT 1 FROM device_data WHERE device_id=${slDevice}`).length >= 2,
        'SL651 next sample was incorrectly deduplicated');
    await Bun.sleep(500);
    assert.equal((await db`SELECT count(*)::int AS count FROM device_data WHERE device_id=${slDevice}`)[0].count, 2);
    console.log('PASS SL651 retransmission, reconnect, receipt loss and distinct sample persistence');

    const source = await Bun.file('service/features/telemetry/telemetry.service.h').text();
    const lua = source.match(/kFanoutScript = R"lua\(([\s\S]*?)\)lua"/)![1];
    // KEYS: history, latest, alerts, delivery, shared wake, receipt; ARGV: wake capacity, wake task, then <count> <fields...> per entry.
    const keys = Array.from({ length: 6 }, (_, i) => `test:fanout:${event}:${i}`);
    const fanout = () => redis.send('EVAL', [lua, '6', ...keys, '100000', 'telemetry-alerts', String(fields.length), ...fields]);
    try {
        await redis.send('SET', [keys[2], 'wrong-type']);
        await assert.rejects(fanout());
        assert.equal(await redis.send('EXISTS', [keys[0], keys[1], keys[3], keys[4], keys[5]]), 0);
        await redis.send('DEL', [keys[2]]);
        assert.equal(await fanout(), 1);
        assert.equal(await fanout(), 0);
        for (const key of keys.slice(0, 4)) assert.equal(await redis.send('XLEN', [key]), 1);
        assert.equal(await redis.send('XLEN', [keys[4]]), 1);
    } finally {
        await redis.send('DEL', keys);
    }
    console.log('PASS fanout validation fails before writes and retry emits one copy per consumer');
} finally {
    unlock?.();
    if (blocking) await blocking;
    await db`UPDATE device SET deleted_at=NOW() WHERE id=${slDevice}`;
    await db`UPDATE link SET deleted_at=NOW() WHERE id=${slLink}`;
    await db`UPDATE protocol_config SET deleted_at=NOW() WHERE id=${slModel}`;
    await db`UPDATE device SET deleted_at=NOW() WHERE id=${device}`;
    await db`UPDATE link SET deleted_at=NOW() WHERE id=${link}`;
    await db`UPDATE protocol_config SET deleted_at=NOW() WHERE id=${model}`;
    await db.close();
    redis.close();
}
