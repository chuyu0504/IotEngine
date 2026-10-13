import { describe, expect, test } from 'bun:test';
import { mqttSaveSchema } from '../web/pages/open_access/open_access.schema';

const valid = {
    accessKeyId: '00000000-0000-7000-8000-000000000001',
    name: 'MQTT',
    brokerUrl: 'mqtts://broker.example.com:8883',
    status: 'enabled',
    timeoutSeconds: 5,
    eventTypes: ['device.data.reported'],
};

describe('开放接入 MQTT 配置', () => {
    test('接受 TCP、TLS 和 IPv6 Broker，保留未提交密码', () => {
        for (const brokerUrl of ['mqtt://localhost', 'mqtts://broker.example.com:8883', 'mqtt://[::1]:1883']) {
            expect(mqttSaveSchema.parse({ ...valid, brokerUrl }).password).toBeUndefined();
        }
        expect(mqttSaveSchema.parse({ ...valid, password: null }).password).toBeNull();
    });
    test('拒绝地址中夹带凭据、路径、查询和无效端口', () => {
        for (const brokerUrl of ['http://host', 'mqtt://user:pass@host', 'mqtt://host/path', 'mqtt://host:0', 'mqtt://host:65536', 'mqtt://host?a=1']) {
            expect(mqttSaveSchema.safeParse({ ...valid, brokerUrl }).success).toBe(false);
        }
    });
    test('不保留自定义 Topic 配置', () => {
        expect(mqttSaveSchema.parse({ ...valid, topic: 'custom/topic' })).not.toHaveProperty('topic');
    });
    test('密码需要用户名，事件和超时受约束', () => {
        for (const patch of [{ password: 'secret' }, { eventTypes: [] }, { timeoutSeconds: 0 }, { timeoutSeconds: 31 }]) {
            expect(mqttSaveSchema.safeParse({ ...valid, ...patch }).success).toBe(false);
        }
    });
});
