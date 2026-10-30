import { expect, test } from 'bun:test';
import { renderToStaticMarkup } from 'react-dom/server';
import DeviceCard from '../web/components/device_card';

test('grouped device cards preserve configured element names', () => {
    const markup = renderToStaticMarkup(
        <DeviceCard
            title="测试设备"
            items={[
                {
                    key: 'gate-position',
                    label: '1#闸当前闸位值',
                    children: '0.00 mm',
                    group: '1#状态',
                },
            ]}
        />
    );

    expect(markup).toContain('>1#闸当前闸位值<');
    expect(markup).not.toContain('>当前闸位值<');
    expect(markup).toContain('truncate whitespace-nowrap');
    expect(markup).toContain('title="0.00 mm"');
});

test('clickable device cards expose an accessible detail entry', () => {
    const markup = renderToStaticMarkup(
        <DeviceCard
            title="边缘节点"
            ariaLabel="查看边缘节点 860406088541915"
            onClick={() => undefined}
            items={[{ key: 'state', label: '状态', children: '在线' }]}
            extra={<span>查看详情</span>}
        />
    );

    expect(markup).toContain('<button');
    expect(markup).toContain('type="button"');
    expect(markup).toContain('aria-label="查看边缘节点 860406088541915"');
    expect(markup).toContain('>查看详情<');
    expect(markup).toContain('h-full w-full');
});

test('a derived value can start below an incomplete native point row', () => {
    const markup = renderToStaticMarkup(
        <DeviceCard
            title="外部 IO"
            items={[
                { key: 'raw-a', label: 'DI0', children: 0 },
                { key: 'raw-b', label: 'DO0', children: 0 },
                { key: 'derived', label: '派生值', children: 1, startNewRow: true },
            ]}
        />
    );

    expect(markup).toMatch(/style="grid-column-start:1"[^>]*>.*派生值/);
    expect(markup.indexOf('>DO0<')).toBeLessThan(markup.indexOf('>派生值<'));
});
