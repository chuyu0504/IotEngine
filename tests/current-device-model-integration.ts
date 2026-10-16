// 仅在一次性测试数据库中执行；独立 schema 和全部数据均回滚。
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const migration = async (path: string) => (await Bun.file(path).text()).split('R"sql(')[1].split(')sql"')[0];
const historical = await migration('service/config/model-revisions.h');
const current = await migration('service/config/current_device_model.h');
const rollback = new Error('fixture rollback');
try {
    await db.begin(async (tx) => {
        const schema = `model_test_${randomUUID().replaceAll('-', '')}`;
        await tx.unsafe(`CREATE SCHEMA ${schema}; SET LOCAL search_path TO ${schema};`);
        await tx.unsafe(`
            CREATE TABLE sys_user(id uuid PRIMARY KEY);
            CREATE TABLE protocol_config(id uuid PRIMARY KEY, protocol varchar(20), name varchar(64),
                config jsonb, remark text, created_by uuid, created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now(), enabled boolean DEFAULT true, deleted_at timestamptz);
            CREATE TABLE link(id uuid PRIMARY KEY, protocol text, execution text, endpoint jsonb, deleted_at timestamptz);
            CREATE TABLE device(id uuid PRIMARY KEY, protocol_config_id uuid REFERENCES protocol_config(id),
                link_id uuid, protocol_params jsonb, protocol_address text, deleted_at timestamptz);
            CREATE TABLE device_data(id uuid PRIMARY KEY);
            CREATE TABLE command_operation(id uuid PRIMARY KEY);
        `);
        const user = randomUUID(), model = randomUUID(), device = randomUUID(), link = randomUUID();
        await tx`INSERT INTO sys_user VALUES(${user})`;
        await tx`INSERT INTO protocol_config(id,protocol,name,config,created_by)
            VALUES(${model},'Modbus','current-model','{"readInterval":1}',${user})`;
        await tx`INSERT INTO link VALUES(${link},'Modbus','collector','{}',null)`;
        await tx`INSERT INTO device(id,protocol_config_id,link_id,protocol_params)
            VALUES(${device},${model},${link},'{}')`;
        await tx.unsafe(historical);
        await tx`UPDATE protocol_config SET config='{"readInterval":2}' WHERE id=${model}`;
        assert.equal((await tx`SELECT config FROM device_model`)[0].config.readInterval, 1);
        assert.equal((await tx`SELECT count(*)::int AS count FROM protocol_revision`)[0].count, 2);
        // 人为制造依赖阻止清理，确认失败不会部分删除旧结构。
        await tx.unsafe('CREATE VIEW blocked_cleanup AS SELECT * FROM protocol_revision; SAVEPOINT cleanup_failure;');
        await assert.rejects(tx.unsafe(current), { errno: '2BP01' });
        await tx.unsafe('ROLLBACK TO SAVEPOINT cleanup_failure; DROP VIEW blocked_cleanup;');
        assert.equal((await tx`SELECT count(*)::int AS count FROM protocol_revision`)[0].count, 2);
        assert.equal((await tx`SELECT config FROM device_model`)[0].config.readInterval, 1);
        await tx.unsafe(current);
        assert.equal((await tx`SELECT config FROM device_model`)[0].config.readInterval, 2);
        await tx`UPDATE protocol_config SET config='{"readInterval":3}' WHERE id=${model}`;
        assert.equal((await tx`SELECT config FROM device_model`)[0].config.readInterval, 3);
        const objects = await tx`SELECT to_regclass('protocol_revision') AS history,
            to_regprocedure('retain_protocol_revision()') AS retain,
            to_regprocedure('version_protocol_config()') AS version,
            to_regprocedure('bind_device_model()') AS binding`;
        assert.deepEqual(Object.values(objects[0]), [null, null, null, null]);
        const columns = await tx`SELECT column_name FROM information_schema.columns
            WHERE table_schema=${schema} AND column_name IN ('revision','protocol_revision','model_revision')`;
        assert.equal(columns.length, 0);
        // 无版本字段的新设备也直接读取当前配置。
        await tx`INSERT INTO device(id,protocol_config_id,link_id,protocol_params)
            VALUES(${randomUUID()},${model},${link},'{}')`;
        assert.equal((await tx`SELECT * FROM device_model`).length, 2);
        throw rollback;
    });
} catch (error) {
    if (error !== rollback) throw error;
    console.log('PASS history removal, upgrade from pinned devices, current configuration, and failure rollback');
} finally {
    await db.close();
}
