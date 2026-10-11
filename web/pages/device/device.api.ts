import request from '@/lib/http';
import { createSseSnapshotStream } from '@/lib/snapshot_request';
import { SnapshotStream } from '@/lib/snapshot_stream';
import type { DebugAcquisition } from '@/types/packet_debug';
import type { PaginatedResult } from '@/types/pagination';
import { createUuid } from '@/utils/uuid';
import {
    deviceCommandSchema,
    deviceDebugSchema,
    deviceIdSchema,
    replaceDeviceSharesSchema,
    saveDeviceGroupSchema,
    saveDeviceSchema,
} from './device.schema';
import type { Device, DeviceGroup } from './device.types';

export const setDebug = (id: string, enabled: boolean) =>
    request.put<void>(
        `/v1/device/${deviceIdSchema.parse(id)}/debug`,
        deviceDebugSchema.parse({ enabled })
    );
const eventParams = (debugDeviceId?: string, commandIds?: readonly string[]) => {
    const ids = [...new Set((commandIds ?? []).map((id) => deviceIdSchema.parse(id)))].sort();
    if (ids.length > 256) throw new Error('单次最多订阅 256 条指令');
    return {
        debugDeviceId: debugDeviceId ? deviceIdSchema.parse(debugDeviceId) : undefined,
        commandIds: ids.length ? ids.join(',') : undefined,
    };
};
export const getDebugPackets = (id: string, commandIds?: readonly string[]) =>
    createSseSnapshotStream<DebugAcquisition[]>(
        '/v1/device/events',
        eventParams(id, commandIds),
        'packets'
    );
export const getDeviceList = (debugDeviceId?: string, commandIds?: readonly string[]) =>
    createSseSnapshotStream<PaginatedResult<Device.Overview>>(
        '/v1/device/events',
        eventParams(debugDeviceId, commandIds),
        'devices'
    );
export const queryDeviceList = (signal?: AbortSignal) =>
    request.get<PaginatedResult<Device.Overview>>('/v1/device', { signal });
export const getDeviceRealtimeSnapshot = (debugDeviceId?: string, commandIds?: readonly string[]) =>
    createSseSnapshotStream<PaginatedResult<Device.RealtimeSnapshot>>(
        '/v1/device/events',
        eventParams(debugDeviceId, commandIds),
        'realtime'
    );
export const getDeviceOptions = (signal?: AbortSignal) =>
    request.get<Device.Option[]>('/v1/device/options', { signal });
export const getDeviceDetail = (id: string, signal?: AbortSignal) =>
    request.get<Device.Overview>(`/v1/device/${deviceIdSchema.parse(id)}`, { signal });
export const getDeviceHistory = (
    id: string,
    query: Device.HistoryRecordQuery,
    signal?: AbortSignal
) =>
    request.get<PaginatedResult<Device.HistoryRecord>>(
        `/v1/device/${deviceIdSchema.parse(id)}/history`,
        { params: { ...query }, signal }
    );
export const createDevice = (data: Device.CreateDto) =>
    request.post<void>('/v1/device', saveDeviceSchema.parse(data));
export const updateDevice = (id: string, data: Device.UpdateDto) =>
    request.put<void>(`/v1/device/${deviceIdSchema.parse(id)}`, saveDeviceSchema.parse(data));
export const removeDevice = (id: string) =>
    request.delete<void>(`/v1/device/${deviceIdSchema.parse(id)}`);
export const createDeviceCommand = (id: string, data: Device.Command) =>
    request.post<Device.CommandCreateResult>(`/v1/device/${deviceIdSchema.parse(id)}/commands`, {
        ...deviceCommandSchema.parse(data),
        idempotency_key: data.idempotency_key ?? createUuid(),
    });
export const getDeviceCommandStatuses = (commandIds: string[], debugDeviceId?: string) => {
    if (!commandIds.length)
        return SnapshotStream.value<Device.CommandStatusesResult>({ complete: true, statuses: [] });
    return createSseSnapshotStream<Device.CommandStatusesResult>(
        '/v1/device/events',
        eventParams(debugDeviceId, commandIds),
        'commands'
    );
};
export const getDeviceShares = (id: string, signal?: AbortSignal) =>
    request.get<Device.ShareItem[]>(`/v1/device/${deviceIdSchema.parse(id)}/shares`, { signal });
export const getDeviceShareTargets = (id: string, signal?: AbortSignal) =>
    request.get<Device.ShareTarget[]>(`/v1/device/${deviceIdSchema.parse(id)}/share-targets`, {
        signal,
    });
export const replaceDeviceShares = (id: string, data: Device.ReplaceSharesDto) =>
    request.put<void>(
        `/v1/device/${deviceIdSchema.parse(id)}/shares`,
        replaceDeviceSharesSchema.parse(data)
    );
export const getDeviceGroupsWithCount = (debugDeviceId?: string, commandIds?: readonly string[]) =>
    createSseSnapshotStream<DeviceGroup.TreeItem[]>(
        '/v1/device/events',
        eventParams(debugDeviceId, commandIds),
        'groups'
    );
export const createDeviceGroup = (data: DeviceGroup.CreateDto) =>
    request.post<void>('/v1/device/groups', saveDeviceGroupSchema.parse(data));
export const updateDeviceGroup = (id: string, data: DeviceGroup.UpdateDto) =>
    request.put<void>(
        `/v1/device/groups/${deviceIdSchema.parse(id)}`,
        saveDeviceGroupSchema.parse(data)
    );
export const removeDeviceGroup = (id: string) =>
    request.delete<void>(`/v1/device/groups/${deviceIdSchema.parse(id)}`);
export const getDeviceGroupShares = (id: string, signal?: AbortSignal) =>
    request.get<Device.ShareItem[]>(`/v1/device/groups/${deviceIdSchema.parse(id)}/shares`, {
        signal,
    });
export const getDeviceGroupShareTargets = (id: string, signal?: AbortSignal) =>
    request.get<Device.ShareTarget[]>(
        `/v1/device/groups/${deviceIdSchema.parse(id)}/share-targets`,
        { signal }
    );
export const replaceDeviceGroupShares = (id: string, data: Device.ReplaceSharesDto) =>
    request.put<void>(
        `/v1/device/groups/${deviceIdSchema.parse(id)}/shares`,
        replaceDeviceSharesSchema.parse(data)
    );
