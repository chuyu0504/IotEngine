import {expect, test} from 'bun:test';
import {formatJson, parseJson} from '../web/utils/json';

test('JSON editor formatting preserves exact numbers, escaped strings and nested arrays', () => {
    const source = '[{"number":9007199254740993,"raw":1e-6,"text":"\\u4e2d"},[]]';
    const formatted = formatJson(source);
    expect(formatted).toContain('9007199254740993');
    expect(formatted).toContain('1e-6');
    expect(formatted).toContain('"\\u4e2d"');
    expect(parseJson(formatted)).toEqual(JSON.parse(source));
});

test('JSON editor reports duplicate keys at their source offset instead of discarding them', () => {
    const source = '[{\n "value": 1,\n "value": 2\n}]';
    try {
        formatJson(source);
        throw Error('duplicate key was accepted');
    } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toContain('不能重复');
        expect((error as {offset: number}).offset).toBe(source.lastIndexOf('"value"'));
    }
    expect(() => formatJson('{"invalid":}')).toThrow();
    expect(() => formatJson('['.repeat(35)+'0'+']'.repeat(35))).toThrow('32 层');
});
