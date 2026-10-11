import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';

const source = readFileSync(
    new URL('../web/pages/edge_node/index.tsx', import.meta.url),
    'utf8'
);
const deviceSource = readFileSync(
    new URL('../web/pages/device/index.tsx', import.meta.url),
    'utf8'
);
const client = readFileSync(
    new URL('../web/pages/edge_node/edge_node.api.ts', import.meta.url),
    'utf8'
);
const service = readFileSync(
    new URL('../web/pages/edge_node/edge_node.service.ts', import.meta.url),
    'utf8'
);
const edgeProjector = readFileSync(
    new URL('../service/features/edge/edge.service.h', import.meta.url),
    'utf8'
);
const vpnBackend = readFileSync(
    new URL('../service/features/vpn/vpn.service.h', import.meta.url),
    'utf8'
);

const vpnFirewall = readFileSync(
    new URL('../service/features/vpn/firewall/firewall.transport.h', import.meta.url),
    'utf8'
);
const cardStart = source.indexOf('{items.map((node) => {');
const drawerStart = source.indexOf('<Drawer', cardStart);
const drawerEnd = source.indexOf('<FormModal', drawerStart);
const card = source.slice(cardStart, drawerStart);
const drawer = source.slice(drawerStart, drawerEnd);
const groupPanelStart = source.indexOf('export function EdgeNodeGroupPanel(');
const groupPanelEnd = source.indexOf('\nconst statusLabels:', groupPanelStart);
const groupPanel = source.slice(groupPanelStart, groupPanelEnd);
const cardItemsStart = source.indexOf('function buildNodeCardItems(');
const cardItemsEnd = source.indexOf('\nexport function SerialDebugModal', cardItemsStart);
const cardItems = source.slice(cardItemsStart, cardItemsEnd);
const vpnPanelStart = source.indexOf('export function EdgeVpnPanel(');
const vpnPanelEnd = source.indexOf('\ntype NetworkDraftItem', vpnPanelStart);
const vpnPanel = source.slice(vpnPanelStart, vpnPanelEnd);

test('edge cards only navigate to the detail drawer', () => {
    expect(cardStart).toBeGreaterThan(-1);
    expect(drawerStart).toBeGreaterThan(cardStart);
    expect(card).toContain('onClick={() => showDetail(node)}');
    expect(card).toContain('<span>查看详情</span>');
    expect(card).not.toContain('showRename(node)');
    expect(card).not.toContain('deviceConfigSync.mutate(node.id)');
    expect(card).not.toContain('showNetworkManager(node)');
    expect(card).not.toContain('showModem(node)');
    expect(card).not.toContain('showFirmware(node)');
    expect(card).not.toContain('approveEnrollment(node)');
    expect(card).not.toContain('deleteEnrollment(node)');
});

test('dense device cards span two tracks while every card stretches to its row height', () => {
    expect(deviceSource).toContain('WIDE_DEVICE_CARD_ITEM_COUNT');
    expect(deviceSource).toContain("wide ? 'lg:col-span-2' : ''");
    expect(deviceSource).toContain('flex h-full min-w-0 flex-col');
});

test('edge management actions live in contextual drawer sections', () => {
    expect(drawerEnd).toBeGreaterThan(drawerStart);
    expect(drawer).toContain('修改名称');
    expect(drawer).toContain('Web 终端');
    expect(drawer).toContain('管理网络接口');
    expect(drawer).toContain("key: 'config'");
    expect(drawer).toContain('同步设备配置');
    expect(drawer).toContain("key: 'mobile'");
    expect(drawer).not.toContain('重新拨号');
    expect(drawer).not.toContain('修改接入设置');
    expect(drawer).toContain("key: 'firmware'");
    expect(drawer).toContain('上传固件并刷写');
    expect(drawer).toContain("item.taskType === 'firmware'");
    expect(drawer).toContain('删除注册申请');
    expect(drawer).toContain('批准注册');
});

test('VPN follows mobile status and firmware history stays in its own tab', () => {
    const mobileTab = drawer.indexOf("key: 'mobile'");
    const vpnTab = drawer.indexOf("key: 'vpn'");
    const tasksTab = drawer.indexOf("key: 'tasks'");
    const firmwareTab = drawer.indexOf("key: 'firmware'");

    expect(mobileTab).toBeGreaterThan(-1);
    expect(vpnTab).toBeGreaterThan(mobileTab);
    expect(tasksTab).toBeGreaterThan(vpnTab);
    expect(firmwareTab).toBeGreaterThan(tasksTab);
    expect(drawer).toContain("(item) => item.taskType !== 'firmware'");
    expect(drawer).toContain("(item) => item.taskType === 'firmware'");
});

test('edge nodes use hierarchical groups without repeating the group inside cards', () => {
    expect(groupPanelStart).toBeGreaterThan(-1);
    expect(groupPanelEnd).toBeGreaterThan(groupPanelStart);
    expect(cardItemsStart).toBeGreaterThan(-1);
    expect(cardItemsEnd).toBeGreaterThan(cardItemsStart);
    expect(source).toContain('<EdgeNodeGroupPanel');
    expect(source).toContain('设置分组');
    expect(groupPanel).toContain('全部节点');
    expect(groupPanel).toContain('未分组');
    expect(groupPanel).toContain(
        'children: item.children?.length ? convert(item.children) : undefined'
    );
    expect(groupPanel).toContain('新增子分组');
    expect(cardItems).toContain("label: 'VPN 网段'");
    expect(cardItems).toContain('node.vpnVirtualCidrs?.length');
    expect(cardItems).not.toContain("{ key: 'group', label: '分组'");
    expect(client).toContain("request.get<Edge.GroupItem[]>('/v1/edge/groups', { signal })");
    expect(client).toContain('request.put<void>(`${edgePath(id)}/group`');
});

test('edge VPN route editing uses the current label and permission gates', () => {
    expect(vpnPanelStart).toBeGreaterThan(-1);
    expect(vpnPanelEnd).toBeGreaterThan(vpnPanelStart);
    expect(vpnPanel).toContain('label="虚拟 LAN 网段"');
    expect(vpnPanel).toContain('name="virtualCidr"');
    expect(vpnPanel).toContain('只允许修改虚拟网段网络号');
    expect(vpnPanel).toContain('真实 LAN、桥接接口、掩码和 NAT 模式由 EdgeNode');
    expect(vpnPanel).toContain('使用相同掩码并保持全局唯一。');
    expect(vpnPanel).toContain("const canQuery = has('iot:vpn:query')");
    expect(vpnPanel).toContain("const canAdd = has('iot:vpn:add')");
    expect(vpnPanel).toContain("const canEdit = has('iot:vpn:edit')");
    expect(vpnPanel).toContain("const canRevoke = has('iot:vpn:revoke')");
    expect(vpnPanel).toContain('if (!canQuery) {');
    expect(vpnPanel).toContain('{!peer && canAdd && (');
    expect(vpnPanel).toContain('{canEdit && (');
    expect(vpnPanel).toContain("{canRevoke && peer.status !== 'revoked' && (");
});

test('edge bridge routes commit before configuration is queued', () => {
    const sync = edgeProjector.indexOf('co_await service::vpn::feature::syncEdgeBridgeRoutes');
    const commit = edgeProjector.indexOf('co_await transaction.commit()', sync);
    const queue = edgeProjector.indexOf('co_await service::vpn::queueEdgeConfig', sync);
    expect(sync).toBeGreaterThanOrEqual(0);
    expect(commit).toBeGreaterThan(sync);
    expect(queue).toBeGreaterThan(commit);
});

test('Hub firewall still isolates Windows clients from unauthorized Edge tunnels', () => {
    expect(vpnBackend).toContain(
        'addresses.aggregate("string_agg", { addresses.column(service::vpn::persistence::VpnEffectiveEdgeAccessEntity::column_name<"edge_address">(), "access")'
    );
    expect(vpnBackend).toContain(
        '.from(service::vpn::persistence::VpnEffectiveEdgeAccessEntity::table_name(), "access")'
    );
    expect(vpnBackend).toContain(
        'addresses.column(service::vpn::persistence::VpnEffectiveEdgeAccessEntity::column_name<"peer_id">(), "access"), Op::equal, addresses.column(service::vpn::persistence::VpnPeerEntity::column_name<"id">(), "p")'
    );
    expect(vpnBackend).toContain('peerRecord.edgeAddresses');
    expect(vpnBackend).toContain('client.edgeAddresses.push_back');
    expect(vpnFirewall).toContain('client.edgeAddresses');
    expect(vpnFirewall).toContain('kOverlayPool.contains(*address)');
});

test('drawer actions render above the detail drawer', () => {
    expect(source).toContain('const EDGE_DETAIL_DRAWER_Z_INDEX = 1000;');
    expect(source).toContain('const EDGE_ACTION_MODAL_Z_INDEX = EDGE_DETAIL_DRAWER_Z_INDEX + 100;');
    expect(drawer).toContain('zIndex={EDGE_DETAIL_DRAWER_Z_INDEX}');
    expect(source.match(/zIndex=\{EDGE_ACTION_MODAL_Z_INDEX\}/g)).toHaveLength(6);
    expect(source.match(/zIndex: EDGE_ACTION_MODAL_Z_INDEX/g)).toHaveLength(2);
});

test('platform exposes mobile status without modem mutation controls', () => {
    expect(drawer).toContain('APN');
    expect(drawer).toContain('运营商');
    expect(drawer).toContain('mobileOperatorName(detail.mobile.operator)');
    expect(source).not.toContain('useModemControlMutation');
    expect(client).not.toContain('/modem');
    expect(service).not.toContain('useModemControlMutation');
});
