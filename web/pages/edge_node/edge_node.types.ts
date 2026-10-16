export namespace Edge {
    export interface SerialSettings {
        baudRate: number;
        dataBits: number;
        stopBits: 1 | 2;
        parity: 'none' | 'even' | 'odd';
        rs485: boolean;
    }
    export interface SerialFrame {
        id: number;
        timestamp: number;
        direction: 'RX' | 'TX';
        hex: string;
        text: string;
    }
    export type EnrollmentStatus = 'pending' | 'approved';
    export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
    export type LogSetting = LogLevel | 'silent';
    export interface Query {
        page?: number;
        pageSize?: number;
        keyword?: string;
        status?: EnrollmentStatus;
        groupId?: string;
    }
    export interface NetworkInterface {
        name: string;
        displayName: string;
        mac: string;
        up: boolean;
        bridge: boolean;
        ipv4: string;
        prefixLength: number;
        gateway: string;
        bridgePorts: string[];
    }
    export interface Network {
        name: string;
        mode: 'dhcp' | 'static' | 'none';
        device: string;
        up: boolean;
        bridge: boolean;
        bridgePorts: string[];
        ipv4: string;
        prefixLength: number;
        gateway: string;
    }
    export interface SerialPort {
        path: string;
        displayName: string;
        available: boolean;
        rs485: boolean;
    }
    export interface Task {
        id: string;
        taskType: 'network' | 'firmware' | 'modem' | 'platform_upsert' | 'platform_delete';
        status: 'pending' | 'accepted' | 'running' | 'succeeded' | 'failed';
        message: string;
        progressPercent: number;
        downloadedBytes: number;
        totalBytes: number;
        createdAt: string;
        updatedAt: string;
    }
    export interface ConfigStatus {
        activeVersion: number;
        desiredVersion: number;
        state: 'idle' | 'pending' | 'applied' | 'rejected';
        message: string;
    }
    export interface OutboxStatus {
        records: number;
        bytes: number;
    }
    export interface LogStatus {
        level: LogSetting;
    }
    export interface TcpTraffic {
        uploadBytes: string;
        downloadBytes: string;
        intervalMs: string;
        sampleId: string;
        month?: string;
        monthlyUploadBytes?: string;
        monthlyDownloadBytes?: string;
        day?: string;
        dailyUploadBytes?: string;
        dailyDownloadBytes?: string;
    }
    export interface NodeStatus {
        tcpTraffic?: TcpTraffic;
        vpnTraffic?: TcpTraffic;
        online: boolean;
        lastSeenAt: string;
        config: ConfigStatus;
        outbox: OutboxStatus;
        log: LogStatus;
    }
    export interface Capability {
        serialDebug?: boolean;
        networkConfig: boolean;
        networkConfigVersion: number;
        firmwareUpdate: boolean;
        deviceConfig: boolean;
        modemControl: boolean;
        terminal: boolean;
        logs: boolean;
        vpn?: {
            supportsVpn: boolean;
            wireguardVersion: string;
            agentVersion: string;
            publicKey: string;
        };
    }
    export interface Signal {
        csq: number;
        rssiDbm: number;
        percent: number;
    }
    export interface Mobile {
        available: boolean;
        simState:
            | 'unknown'
            | 'ready'
            | 'not_inserted'
            | 'pin_required'
            | 'puk_required'
            | 'blocked';
        iccid: string;
        signal: Signal;
        registered: boolean;
        registrationStatus: number;
        apn: string;
        operator: string;
        connected: boolean;
        ipv4: string;
    }
    export interface FirmwareStatus {
        state: Task['status'] | '';
        progressPercent: number;
        downloadedBytes: number;
        totalBytes: number;
        message: string;
    }
    export interface DtuTrace {
        sequence?: string;
        direction?: string;
        clientSlot?: number;
        payload?: string;
        totalBytes?: number;
        monotonicMs?: string;
    }
    export interface DtuChannel {
        channelId: string;
        name: string;
        enabled: boolean;
        southMode: 'serial' | 'tcp_client' | 'tcp_server';
        southHost?: string;
        southPort?: number;
        northHost: string;
        northPort: number;
        serialPath?: string;
        baudRate?: number;
        dataBits?: number;
        stopBits?: number;
        parity?: 'none' | 'even' | 'odd';
        rs485?: boolean;
        maxClients: number;
        queueBytes: number;
        serialFrameMs: number;
        uplinkOnly?: boolean;
        registrationHex?: string;
        heartbeatHex?: string;
        heartbeatIntervalSec?: number;
        debugEnabled?: boolean;
        status?: {
            northState?: string;
            southState?: string;
            clientCount?: number;
            upstreamBytes?: string;
            downstreamBytes?: string;
            queuedBytes?: number;
            error?: string;
            traces?: DtuTrace[];
            omittedTraces?: string;
        };
    }
    export interface DtuChannels {
        supported: boolean;
        channels: DtuChannel[];
    }
    export interface EventScope {
        dtu?: boolean;
        nodeId?: string;
        logs?: LogsQuery;
        vpn?: boolean;
    }
    export interface Node {
        id: string;
        imei: string;
        name: string;
        groupId: string;
        groupName: string;
        model: string;
        softwareVersion: string;
        hostname: string;
        architecture: string;
        openwrtRelease: string;
        enrollmentStatus: EnrollmentStatus;
        status: NodeStatus;
        capability: Capability;
        mobile: Mobile;
        firmware: FirmwareStatus;
        vpnVirtualCidrs: string[];
        createdAt: string;
        interfaces?: NetworkInterface[];
        networks?: Network[];
        serialPorts?: SerialPort[];
        tasks?: Task[];
    }
    export interface NetworkConfig {
        operation: 'upsert' | 'delete';
        name: string;
        previousName?: string;
        mode?: 'dhcp' | 'static';
        device?: string;
        bridge?: boolean;
        bridgePorts?: string[];
        ip?: string;
        prefixLength?: number;
        gateway?: string;
    }
    export interface NetworkDto {
        interfaces: NetworkConfig[];
        rollbackTimeoutSec: number;
    }
    export interface NameDto {
        name: string;
    }
    export interface GroupDto {
        groupId: string;
    }
    export type GroupStatus = 'enabled' | 'disabled';
    export interface GroupItem {
        id: string;
        name: string;
        parentId: string;
        status: GroupStatus;
        sortOrder: number;
        remark: string;
        nodeCount: number;
    }
    export interface GroupTreeItem extends GroupItem {
        children?: GroupTreeItem[];
    }
    export interface GroupSaveDto {
        name: string;
        parentId?: string;
        status: GroupStatus;
        sortOrder: number;
        remark?: string;
    }
    export interface FirmwareUpgradeDto {
        file: File;
        keepSettings: boolean;
    }
    export interface FirmwareReuseResult {
        reused: boolean;
    }
    export interface FirmwareUploadProgress {
        loadedBytes: number;
        totalBytes: number;
        percent: number;
    }
    export interface LogsQuery {
        limit?: number;
        level?: LogLevel;
        source?: string;
    }
    export interface LogLine {
        time: string;
        level: LogLevel;
        source: string;
        message: string;
        detail: string;
    }
    export interface Logs {
        lines: LogLine[];
    }
    export interface LogLevelDto {
        level: LogSetting;
    }
    /** The terminal view `useTerminalSession` drives; the page owns the xterm instance. */
    export interface TerminalScreen {
        size(): { columns: number; rows: number };
        /** A new session starts: clear the screen and accept input again. */
        reset(): void;
        /** Renders output; `written` runs once the bytes are on screen. */
        write(bytes: Uint8Array, written: () => void): void;
        /** The session accepts input. */
        ready(): void;
        /** The session ended; stop accepting input. */
        closed(): void;
    }
}
/** HTTP query caches; write mutations invalidate them on success. */
export const edgeQueryKeys = {
    all: ['edge'] as const,
    selection: () => [...edgeQueryKeys.all, 'selection'] as const,
    configuration: (id?: string) => [...edgeQueryKeys.all, 'configuration', id ?? ''] as const,
    groups: () => [...edgeQueryKeys.all, 'groups'] as const,
    vpnNetworks: () => [...edgeQueryKeys.all, 'vpn-networks'] as const,
};
/** Static PLMN table; outside `edgeQueryKeys.all` so node writes never refetch it. */
export const plmnNamesQueryKey = ['plmn-names'] as const;
/** SSE caches; only pushed events update them, so mutations never invalidate them. */
export const edgeLiveQueryKeys = {
    all: ['edge-live'] as const,
    inventory: () => [...edgeLiveQueryKeys.all, 'inventory'] as const,
    detail: (id?: string) => [...edgeLiveQueryKeys.all, 'detail', id ?? ''] as const,
    logs: (id?: string, query?: Edge.LogsQuery) =>
        [...edgeLiveQueryKeys.all, 'logs', id ?? '', query ?? {}] as const,
    vpn: (id?: string) => [...edgeLiveQueryKeys.all, 'vpn', id ?? ''] as const,
    dtu: (id?: string) => [...edgeLiveQueryKeys.all, 'dtu', id ?? ''] as const,
};

export namespace EdgeVpn {
    export type NetworkStatus = 'enabled' | 'disabled';
    export type PeerStatus = 'pending' | 'active' | 'revoked';
    export type RouteStatus = 'active' | 'error' | 'disabled';
    export type RouteMode = 'nat' | 'routed';
    export interface Network {
        id: string;
        name: string;
        overlayCidr: string;
        hubPublicKey: string;
        hubEndpoint: string;
        hubListenPort: number;
        status: NetworkStatus;
        peerCount: number;
        routeCount: number;
    }
    export interface Peer {
        id: string;
        networkId: string;
        peerType: 'edge' | 'windows';
        edgeNodeId?: string;
        name: string;
        publicKey: string;
        assignedIpv4: string;
        allowedRoutes: string[];
        status: PeerStatus;
        configRevision: number;
        lastHandshakeAt?: string | null;
    }
    export interface Route {
        id: string;
        networkId: string;
        edgePeerId: string;
        edgeNodeId: string;
        lanInterface: string;
        targetCidr: string;
        virtualCidr: string;
        mode: RouteMode;
        natMode: 'masquerade' | 'none';
        status: RouteStatus;
        enabled: boolean;
        lastError: string;
    }
    export interface PeerCreateDto {
        networkId?: string;
        peerType: 'edge';
        edgeNodeId: string;
        name: string;
    }
    export interface RouteDto {
        networkId: string;
        edgePeerId: string;
        lanInterface: string;
        targetCidr: string;
        virtualCidr: string;
        mode: RouteMode;
        enabled: boolean;
    }
    export interface Overview {
        networks: Network[];
        peers: Peer[];
        routes: Route[];
    }
}
