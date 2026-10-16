import { expect, spyOn, test } from 'bun:test';
import { getPlmnNames } from '../web/pages/edge_node/edge_node.api';
import { mobileOperatorName } from '../web/pages/edge_node/edge_node.service';

const plmnNames: Record<string, string> = await Bun.file(
    new URL('../web/public/plmn_names.json', import.meta.url)
).json();
const operator = (value: string) => mobileOperatorName(value, plmnNames);

test('the static PLMN table maps MCC+MNC codes to operator names', () => {
    expect(Object.keys(plmnNames).length).toBeGreaterThan(3000);
    for (const [code, name] of Object.entries(plmnNames)) {
        expect(code).toMatch(/^\d{5,6}$/);
        expect(name.trim()).not.toBe('');
    }
});

test('maps China PLMN codes to short Chinese operator names', () => {
    expect(operator('46011')).toBe('中国电信');
    expect(operator('46000')).toBe('中国移动');
    expect(operator('46001')).toBe('中国联通');
    expect(operator('46015')).toBe('中国广电');
    expect(operator(' 46011 ')).toBe('中国电信');
});

test('maps global PLMN codes to short brand names and falls back from six to five digits', () => {
    expect(operator('310260')).toBe('T-Mobile');
    expect(operator('23415')).toBe('Vodafone UK');
    expect(operator('44010')).toBe('NTT docomo');
    expect(operator('50212')).toBe('Maxis');
    expect(operator('460119')).toBe('中国电信');
});

test('keeps non-PLMN values and unknown codes unchanged, also before the table loads', () => {
    expect(operator('中国电信')).toBe('中国电信');
    expect(operator('CHN-CT')).toBe('CHN-CT');
    expect(operator('')).toBe('');
    expect(operator('99998')).toBe('99998');
    expect(mobileOperatorName('46011', {})).toBe('46011');
});

test('the table is fetched from the static asset path and HTTP failures are reported', async () => {
    const requests: string[] = [];
    const fetch = spyOn(globalThis, 'fetch').mockImplementation((async (url: string | URL | Request) => {
        requests.push(String(url));
        return requests.length === 1
            ? Response.json({ '46011': '中国电信' })
            : new Response('missing', { status: 404 });
    }) as typeof globalThis.fetch);
    try {
        expect(await getPlmnNames()).toEqual({ '46011': '中国电信' });
        await expect(getPlmnNames()).rejects.toThrow('运营商名称表加载失败');
        expect(requests).toEqual(['/plmn_names.json', '/plmn_names.json']);
    } finally { fetch.mockRestore(); }
});
