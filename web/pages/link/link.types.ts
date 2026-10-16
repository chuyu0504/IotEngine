import type { PageParams } from '@/types/pagination';
import type { ProtocolType } from '../protocol/protocol.types';

type LinkMode = 'TCP Server' | 'TCP Client';
type LinkStatus = 'enabled' | 'disabled';
type ConnectionStatus =
    | 'stopped'
    | 'listening'
    | 'connected'
    | 'partial'
    | 'connecting'
    | 'reconnecting'
    | 'error';
interface LinkTarget {
    id: string;
    name: string;
    ip: string;
    port: number;
    status: LinkStatus;
    runtime?: RuntimeStatus;
    mqtt?: { clientId: string; username?: string; password?: string; keepAliveSeconds?: number };
}
interface RuntimeStatus {
    state?: ConnectionStatus;
    reason?: string;
    error?: string;
    clientCount?: number;
    clients?: string[];
    lastActivityAt?: string;
}
interface LinkEndpoint {
    transport?: 'serial' | 'tcp';
    interface?: string;
    baud_rate?: number;
    data_bits?: number;
    stop_bits?: number;
    parity?: 'none' | 'even' | 'odd';
    rs485?: boolean;
    mode: LinkMode;
    ip: string;
    port: number;
    targets: LinkTarget[];
}
interface LinkItem {
    debug_enabled?: boolean;
    execution?: 'collector' | 'edge';
    edge_node_id?: string;
    id: string;
    name: string;
    protocol: ProtocolType;
    endpoint: LinkEndpoint;
    status: LinkStatus;
    runtime?: RuntimeStatus;
    created_by: string;
    created_at: string;
    updated_at: string;
}
interface LinkQuery extends PageParams {
    mode?: LinkMode;
    protocol?: ProtocolType;
    status?: LinkStatus;
}
interface SaveLinkDto {
    execution?: 'collector' | 'edge';
    edge_node_id?: string;
    name: string;
    protocol: ProtocolType;
    endpoint: LinkEndpoint;
    status: LinkStatus;
}
interface LinkEnums {
    modes: LinkMode[];
    protocols: ProtocolType[];
    statuses: LinkStatus[];
}
interface LinkOption extends LinkItem {}
export namespace Link {
    export type Mode = LinkMode;
    export type Protocol = ProtocolType;
    export type Status = LinkStatus;
    export type Connection = ConnectionStatus;
    export type Runtime = RuntimeStatus;
    export type Endpoint = LinkEndpoint;
    export type Target = LinkTarget;
    export type Item = LinkItem;
    export type Query = LinkQuery;
    export type SaveDto = SaveLinkDto;
    export type Enums = LinkEnums;
    export type Option = LinkOption;
}
