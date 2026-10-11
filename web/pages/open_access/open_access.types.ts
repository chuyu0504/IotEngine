import type { PageParams } from '@/types/pagination';

export namespace Access {
    export type Status = 'enabled' | 'disabled';
    export type Scope = 'device:realtime' | 'device:history' | 'device:command' | 'alert:read';
    export type EventType =
        | 'device.data.reported'
        | 'device.image.reported'
        | 'device.command.accepted'
        | 'device.command.updated'
        | 'device.alert.triggered'
        | 'device.alert.resolved';
    export interface DeviceOption {
        id: string;
        name: string;
        deviceCode: string;
        canCommand: boolean;
    }
    export interface KeyItem {
        id: string;
        name: string;
        accessKeyPrefix: string;
        status: Status;
        scopes: Scope[];
        deviceIds: string[];
        expiresAt: string | null;
        lastUsedAt: string | null;
        lastUsedIp: string | null;
        remark: string | null;
        webhookCount: number;
        createdAt: string;
        updatedAt: string;
    }
    export interface KeySecret {
        id: string;
        name: string;
        accessKey: string;
        accessKeyPrefix: string;
    }
    export interface KeySaveDto {
        name: string;
        status: Status;
        scopes: Scope[];
        deviceIds: string[];
        expiresAt?: string | null;
        remark?: string | null;
    }
    export interface WebhookItem {
        id: string;
        accessKeyId: string;
        accessKeyName: string;
        name: string;
        url: string;
        status: Status;
        timeoutSeconds: number;
        skipTlsVerify: boolean;
        headers: Record<string, string>;
        eventTypes: EventType[];
        deviceIds: string[];
        hasSecret: boolean;
        lastTriggeredAt: string | null;
        lastSuccessAt: string | null;
        lastFailureAt: string | null;
        lastHttpStatus: number | null;
        lastError: string | null;
        createdAt: string;
        updatedAt: string;
    }
    export interface WebhookSaveDto {
        accessKeyId: string;
        name: string;
        url: string;
        status: Status;
        timeoutSeconds: number;
        skipTlsVerify: boolean;
        headers: Record<string, string>;
        eventTypes: EventType[];
        secret?: string | null;
    }
    export interface LogItem {
        id: string;
        accessKeyId: string | null;
        accessKeyName: string | null;
        webhookId: string | null;
        webhookName: string | null;
        direction: 'pull' | 'push';
        action: string;
        eventType: string | null;
        status: 'success' | 'failed';
        httpMethod: string | null;
        target: string | null;
        requestIp: string | null;
        httpStatus: number | null;
        deviceId: string | null;
        deviceCode: string | null;
        message: string | null;
        requestPayload: Record<string, unknown>;
        responsePayload: Record<string, unknown>;
        createdAt: string;
    }
    export interface LogQuery extends PageParams {
        accessKeyId?: string;
        webhookId?: string;
        direction?: 'pull' | 'push';
        status?: 'success' | 'failed';
        action?: string;
    }
}
