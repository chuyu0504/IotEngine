import { expect, test } from 'bun:test';
import {
    DEFAULT_MANAGEMENT_PATH,
    expandedMenuKeys,
    getVisibleNavigationGroups,
    managementPages,
    navigationGroups,
} from '../web/routes/navigation';

// 外部地址和权限码是契约，移动源码目录时不得修改。
const expectedPermissions: Record<string, string> = {
    '/iot/link': 'iot:link:query',
    '/iot/edge': 'iot:edge:query',
    '/iot/sl651': 'iot:protocol:query',
    '/iot/modbus': 'iot:protocol:query',
    '/iot/s7': 'iot:protocol:query',
    '/iot/dlt645': 'iot:protocol:query',
    '/iot/fins': 'iot:protocol:query',
    '/iot/mqtt': 'iot:protocol:query',
    '/iot/mc': 'iot:protocol:query',
    '/iot/external-io': 'iot:protocol:query',
    '/device': 'iot:device:query',
    '/iot/alert': 'iot:alert:query',
    '/iot/gb28181': 'iot:gb28181:query',
    '/iot/open-access': 'iot:open-access:query',
    '/system/role': 'system:role:query',
    '/system/dept': 'system:dept:query',
    '/system/user': 'system:user:query',
};

test('all legacy management URLs and permission codes are preserved exactly once', () => {
    expect(Object.fromEntries(managementPages.map(page => [page.path, page.permission]))).toEqual(expectedPermissions);
    expect(managementPages).toHaveLength(Object.keys(expectedPermissions).length);
    expect(new Set(managementPages.map(page => page.id)).size).toBe(managementPages.length);
    expect(DEFAULT_MANAGEMENT_PATH).toBe('/system/role');
    expect(expandedMenuKeys).toEqual(['/iot/protocol', '/system']);
});

test('navigation filters each permission independently and removes empty groups', () => {
    expect(getVisibleNavigationGroups(() => false, { gb28181: true })).toEqual([]);
    for (const permission of new Set(Object.values(expectedPermissions))) {
        const groups = getVisibleNavigationGroups(value => value === permission, { gb28181: true });
        expect(groups.every(group => group.pages.length > 0)).toBe(true);
        expect(groups.flatMap(group => group.pages.map(page => page.path))).toEqual(
            Object.keys(expectedPermissions).filter(path => expectedPermissions[path] === permission)
        );
    }
});

test('video navigation requires both the feature and permission without mutating route definitions', () => {
    const fullRoutes = JSON.stringify(navigationGroups);
    const disabled = getVisibleNavigationGroups(() => true, { gb28181: false });
    expect(disabled.flatMap(group => group.pages).map(page => page.path)).toEqual(
        Object.keys(expectedPermissions).filter(path => path !== '/iot/gb28181')
    );
    const enabled = getVisibleNavigationGroups(() => true, { gb28181: true });
    expect(enabled.flatMap(group => group.pages).map(page => page.path)).toEqual(Object.keys(expectedPermissions));
    expect(JSON.stringify(navigationGroups)).toBe(fullRoutes);
});
