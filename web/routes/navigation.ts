export type ManagementPageId =
    | 'link'
    | 'edge_node'
    | 'sl651'
    | 'modbus'
    | 's7'
    | 'dlt645'
    | 'fins'
    | 'mqtt'
    | 'mc'
    | 'external_io'
    | 'device'
    | 'alert'
    | 'gb28181'
    | 'open_access'
    | 'role'
    | 'dept'
    | 'user';

export type NavigationIcon =
    | 'link'
    | 'edge_node'
    | 'protocol'
    | 'device'
    | 'alert'
    | 'gb28181'
    | 'open_access'
    | 'system'
    | 'role'
    | 'dept'
    | 'user';

export interface NavigationFeatures {
    gb28181: boolean;
}

export interface ManagementPage {
    id: ManagementPageId;
    path: string;
    title: string;
    permission: string;
    icon?: NavigationIcon;
    feature?: keyof NavigationFeatures;
}

export interface NavigationGroup {
    key: string;
    title: string;
    icon: NavigationIcon;
    menuKey?: string;
    pages: readonly ManagementPage[];
}

// 路由注册、菜单和面包屑共用此定义；目录调整不改变页面地址和权限码。
export const navigationGroups: readonly NavigationGroup[] = [
    {
        key: 'connections',
        title: '设备接入',
        icon: 'link',
        pages: [
            {
                id: 'link',
                path: '/iot/link',
                title: '链路管理',
                permission: 'iot:link:query',
                icon: 'link',
            },
            {
                id: 'edge_node',
                path: '/iot/edge',
                title: '边缘节点',
                permission: 'iot:edge:query',
                icon: 'edge_node',
            },
        ],
    },
    {
        key: 'protocol',
        title: '协议管理',
        icon: 'protocol',
        menuKey: '/iot/protocol',
        pages: [
            {
                id: 'sl651',
                path: '/iot/sl651',
                title: 'SL651 配置',
                permission: 'iot:protocol:query',
            },
            {
                id: 'modbus',
                path: '/iot/modbus',
                title: 'Modbus 配置',
                permission: 'iot:protocol:query',
            },
            { id: 's7', path: '/iot/s7', title: 'S7 配置', permission: 'iot:protocol:query' },
            {
                id: 'dlt645',
                path: '/iot/dlt645',
                title: 'DL/T645 配置',
                permission: 'iot:protocol:query',
            },
            { id: 'fins', path: '/iot/fins', title: 'FINS 配置', permission: 'iot:protocol:query' },
            { id: 'mqtt', path: '/iot/mqtt', title: 'MQTT 配置', permission: 'iot:protocol:query' },
            {
                id: 'mc',
                path: '/iot/mc',
                title: 'MC / SLMP 配置',
                permission: 'iot:protocol:query',
            },
            {
                id: 'external_io',
                path: '/iot/external-io',
                title: '外部 IO 配置',
                permission: 'iot:protocol:query',
            },
        ],
    },
    {
        key: 'operations',
        title: '设备运营',
        icon: 'device',
        pages: [
            {
                id: 'device',
                path: '/device',
                title: '设备管理',
                permission: 'iot:device:query',
                icon: 'device',
            },
            {
                id: 'alert',
                path: '/iot/alert',
                title: '告警中心',
                permission: 'iot:alert:query',
                icon: 'alert',
            },
            {
                id: 'gb28181',
                path: '/iot/gb28181',
                title: '视频监控',
                permission: 'iot:gb28181:query',
                icon: 'gb28181',
                feature: 'gb28181',
            },
            {
                id: 'open_access',
                path: '/iot/open-access',
                title: '开放接入',
                permission: 'iot:open-access:query',
                icon: 'open_access',
            },
        ],
    },
    {
        key: 'system',
        title: '系统管理',
        icon: 'system',
        menuKey: '/system',
        pages: [
            {
                id: 'role',
                path: '/system/role',
                title: '角色管理',
                permission: 'system:role:query',
                icon: 'role',
            },
            {
                id: 'dept',
                path: '/system/dept',
                title: '部门管理',
                permission: 'system:dept:query',
                icon: 'dept',
            },
            {
                id: 'user',
                path: '/system/user',
                title: '用户管理',
                permission: 'system:user:query',
                icon: 'user',
            },
        ],
    },
];

export const managementPages = navigationGroups.flatMap((group) => group.pages);
export const expandedMenuKeys = navigationGroups.flatMap((group) =>
    group.menuKey ? [group.menuKey] : []
);

export function getManagementPagePath(id: ManagementPageId): string {
    const page = managementPages.find((item) => item.id === id);
    if (!page) throw new Error(`管理页面未注册：${id}`);
    return page.path;
}

export const DEFAULT_MANAGEMENT_PATH = getManagementPagePath('role');

export function getVisibleNavigationGroups(
    hasPermission: (permission: string) => boolean,
    features: NavigationFeatures
): NavigationGroup[] {
    return navigationGroups
        .map((group) => ({
            ...group,
            pages: group.pages.filter(
                (page) =>
                    hasPermission(page.permission) && (!page.feature || features[page.feature])
            ),
        }))
        .filter((group) => group.pages.length > 0);
}
