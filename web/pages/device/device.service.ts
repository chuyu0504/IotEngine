import { keepPreviousData, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback } from 'react';
import { useMutationWithMessage, useSaveMutation } from '@/hooks/use_mutation';
import { useSnapshotQuery } from '@/hooks/use_snapshot_query';
import { createQueryKeys } from '@/utils/query';
import { retainNewerRecords } from '@/utils/versioned_records';
import * as api from './device.api';
import { getDebugPackets, setDebug as saveDebugSwitch } from './device.api';
import type { Device, DeviceGroup } from './device.types';
export const getDeviceCardElements = (device: Device.Overview) => {
    const ids = device.cardPointIds && new Set(device.cardPointIds);
    return (device.elements ?? []).filter(
        (element) => element.visible !== false && (!ids || (element.id && ids.has(element.id)))
    );
};
/** 在线状态由服务端遥测运行投影计算，上报与到期离线均经实时推送更新。 */
export const isDeviceOnline = (device: Pick<Device.Overview, 'runtimeState'>) =>
    device.runtimeState === 'online';

/** 实时推送维护的快照缓存；写入后由服务端推送更新，不做失效。 */
const deviceKeys = createQueryKeys('devices');
const deviceRealtimeSnapshotKey = [...deviceKeys.all, 'realtime'] as const;
const groupKeys = createQueryKeys('device-groups');
/** HTTP 按需查询缓存；写入成功后失效。 */
const deviceQueryKeys = createQueryKeys('device-queries');
const shareKeys = {
    all: ['device-shares'] as const,
    list: (kind: 'device' | 'group', id: string) => ['device-shares', kind, id, 'list'] as const,
    targets: (kind: 'device' | 'group', id: string) =>
        ['device-shares', kind, id, 'targets'] as const,
};

const buildDeviceGroupTree = (items: DeviceGroup.TreeItem[]) => {
    const map = new Map<string, DeviceGroup.TreeItem>();
    const roots: DeviceGroup.TreeItem[] = [];
    for (const item of items) map.set(item.id, { ...item, children: [] });
    for (const item of map.values()) {
        const parent = item.parent_id ? map.get(item.parent_id) : undefined;
        if (parent) parent.children?.push(item);
        else roots.push(item);
    }
    return roots;
};
export function useDeviceList(options?: {
    enabled?: boolean;
    debugDeviceId?: string;
    commandIds?: string[];
}) {
    return useSnapshotQuery({
        queryKey: [...deviceKeys.lists(), options?.debugDeviceId, options?.commandIds],
        queryFn: () => api.getDeviceList(options?.debugDeviceId, options?.commandIds),
        placeholderData: keepPreviousData,
        enabled: options?.enabled ?? true,
    });
}
export function useDeviceConfigurationList(options?: { enabled?: boolean }) {
    return useQuery({
        queryKey: [...deviceQueryKeys.all, 'configuration-list'],
        queryFn: ({ signal }) => api.queryDeviceList(signal),
        enabled: options?.enabled ?? true,
        staleTime: 0,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
}
export function useDeviceOptions(options?: { enabled?: boolean }) {
    return useQuery({
        queryKey: deviceQueryKeys.options(),
        queryFn: ({ signal }) => api.getDeviceOptions(signal),
        enabled: options?.enabled ?? true,
        refetchOnWindowFocus: false,
    });
}
/** 按需加载设备详情（如下发要素），每次调用都向服务端取最新数据并合并并发请求。 */
export function useDeviceDetailLoader() {
    const client = useQueryClient();
    return useCallback(
        (id: string) =>
            client.fetchQuery({
                queryKey: deviceQueryKeys.detail(id),
                queryFn: ({ signal }) => api.getDeviceDetail(id, signal),
                staleTime: 0,
            }),
        [client]
    );
}
export function useDeviceRealtimeSnapshot(options?: {
    enabled?: boolean;
    debugDeviceId?: string;
    commandIds?: string[];
}) {
    return useSnapshotQuery({
        queryKey: [...deviceRealtimeSnapshotKey, options?.debugDeviceId, options?.commandIds],
        queryFn: () => api.getDeviceRealtimeSnapshot(options?.debugDeviceId, options?.commandIds),
        placeholderData: keepPreviousData,
        enabled: options?.enabled ?? true,
    });
}
export function useDeviceHistory(
    deviceId: string | undefined,
    query: Device.HistoryRecordQuery,
    enabled = true
) {
    return useQuery({
        queryKey: [...deviceQueryKeys.all, 'history', deviceId ?? '', query],
        queryFn: ({ signal }) => api.getDeviceHistory(deviceId as string, query, signal),
        enabled: Boolean(deviceId) && enabled,
        refetchOnWindowFocus: false,
    });
}
export function useDeviceSave() {
    return useSaveMutation<
        Device.CreateDto & {
            id?: string;
        },
        Device.CreateDto,
        Device.UpdateDto
    >({
        createFn: api.createDevice,
        updateFn: api.updateDevice,
        toUpdatePayload: ({ id: _id, ...data }) => data,
        createMessage: '创建成功',
        updateMessage: '更新成功',
        invalidateKeys: [deviceQueryKeys.all],
    });
}
export function useDeviceDelete() {
    return useMutationWithMessage({
        mutationFn: api.removeDevice,
        successMessage: '删除成功',
        invalidateKeys: [deviceQueryKeys.all],
    });
}
export function useDeviceCommand() {
    return useMutationWithMessage({
        mutationFn: ({ deviceId, data }: { deviceId: string; data: Device.Command }) =>
            api.createDeviceCommand(deviceId, data),
        errorMessage: (error) => error.message,
    });
}
export function useDeviceCommandResults(
    commandIds: string[],
    enabled: boolean,
    debugDeviceId?: string
) {
    return useSnapshotQuery({
        queryKey: [...deviceKeys.all, 'command-results', commandIds, debugDeviceId],
        queryFn: () => api.getDeviceCommandStatuses(commandIds, debugDeviceId),
        enabled: enabled && commandIds.length > 0,
    });
}
export function summarizeDeviceCommandResult(result: Device.CommandStatusesResult) {
    const failed = result.statuses.find((state) =>
        ['FAILED', 'REJECTED', 'UNKNOWN', 'READBACK_MISMATCH'].includes(state.status)
    );
    if (failed) {
        const reason = failed.reason?.toLowerCase() ?? '';
        const message =
            failed.status === 'UNKNOWN'
                ? reason.includes('write_ack_missing')
                    ? '未收到写入应答，指令可能已执行；请核对设备状态，勿直接重发'
                    : reason.includes('readback')
                      ? '回读未能确认最终设备状态，请核对设备状态，勿直接重发'
                      : '指令结果未知，请核对设备状态，勿直接重发'
                : failed.status === 'READBACK_MISMATCH'
                  ? '回读值与目标不一致，请核对设备状态'
                  : failed.reason || '设备执行指令失败';
        return { failed: true, message };
    }
    if (
        result.statuses.length > 0 &&
        result.statuses.every((status) => status.reason === 'mqtt_broker_acknowledged')
    ) {
        return {
            failed: false,
            message: '控制消息已送达 Broker；设备执行结果需等待设备回执',
        };
    }
    const actualValues = result.statuses.flatMap((status) => status.actual_values ?? []);
    return {
        failed: false,
        message: actualValues.length
            ? `设备执行成功，回读：${actualValues.map((actual) => `${actual.name || actual.element_id}=${actual.value}${actual.unit || ''}`).join('，')}`
            : '设备执行成功，已收到应答',
    };
}
export function useDeviceShares(
    deviceId?: string,
    options?: {
        enabled?: boolean;
    }
) {
    return useQuery({
        queryKey: shareKeys.list('device', deviceId ?? ''),
        queryFn: ({ signal }) => api.getDeviceShares(deviceId as string, signal),
        enabled: !!deviceId && (options?.enabled ?? true),
        refetchOnWindowFocus: false,
    });
}
export function useDeviceShareTargets(
    deviceId?: string,
    options?: {
        enabled?: boolean;
    }
) {
    return useQuery({
        queryKey: shareKeys.targets('device', deviceId ?? ''),
        queryFn: ({ signal }) => api.getDeviceShareTargets(deviceId as string, signal),
        enabled: !!deviceId && (options?.enabled ?? true),
        refetchOnWindowFocus: false,
    });
}
export function useReplaceDeviceShares() {
    return useMutationWithMessage({
        mutationFn: ({ deviceId, data }: { deviceId: string; data: Device.ReplaceSharesDto }) =>
            api.replaceDeviceShares(deviceId, data),
        successMessage: '设备分享已更新',
        invalidateKeys: [shareKeys.all, deviceQueryKeys.all],
    });
}
export function useDeviceGroupShares(
    groupId?: string,
    options?: {
        enabled?: boolean;
    }
) {
    return useQuery({
        queryKey: shareKeys.list('group', groupId ?? ''),
        queryFn: ({ signal }) => api.getDeviceGroupShares(groupId as string, signal),
        enabled: !!groupId && (options?.enabled ?? true),
        refetchOnWindowFocus: false,
    });
}
export function useDeviceGroupShareTargets(
    groupId?: string,
    options?: {
        enabled?: boolean;
    }
) {
    return useQuery({
        queryKey: shareKeys.targets('group', groupId ?? ''),
        queryFn: ({ signal }) => api.getDeviceGroupShareTargets(groupId as string, signal),
        enabled: !!groupId && (options?.enabled ?? true),
        refetchOnWindowFocus: false,
    });
}
export function useReplaceDeviceGroupShares() {
    return useMutationWithMessage({
        mutationFn: ({ groupId, data }: { groupId: string; data: Device.ReplaceSharesDto }) =>
            api.replaceDeviceGroupShares(groupId, data),
        successMessage: '设备分组分享已更新',
        invalidateKeys: [shareKeys.all, deviceQueryKeys.all],
    });
}
export function useDeviceGroupTreeWithCount(
    options?: { enabled?: boolean },
    debugDeviceId?: string,
    commandIds?: string[]
) {
    return useSnapshotQuery({
        queryKey: [...groupKeys.all, 'tree-count', debugDeviceId, commandIds],
        queryFn: () =>
            api.getDeviceGroupsWithCount(debugDeviceId, commandIds).map(buildDeviceGroupTree),
        placeholderData: keepPreviousData,
        enabled: options?.enabled ?? true,
    });
}
export function useDeviceGroupSave() {
    return useSaveMutation<
        DeviceGroup.CreateDto & {
            id?: string;
        },
        DeviceGroup.CreateDto,
        DeviceGroup.UpdateDto
    >({
        createFn: api.createDeviceGroup,
        updateFn: api.updateDeviceGroup,
        toUpdatePayload: ({ id: _id, ...data }) => data,
        createMessage: '创建成功',
        updateMessage: '更新成功',
        invalidateKeys: [deviceQueryKeys.all],
    });
}
export function useDeviceGroupDelete() {
    return useMutationWithMessage({
        mutationFn: api.removeDeviceGroup,
        successMessage: '删除成功',
        invalidateKeys: [deviceQueryKeys.all],
    });
}

export function useDeviceDebug(id: string, open: boolean, commandIds?: string[]) {
    const packets = useSnapshotQuery({
        queryKey: ['device-debug-packets', id, commandIds],
        queryFn: () => getDebugPackets(id, commandIds),
        structuralSharing: retainNewerRecords,
        enabled: open,
    });
    const toggle = useMutationWithMessage({
        mutationFn: (enabled: boolean) => saveDebugSwitch(id, enabled),
        successMessage: '调试设置已保存',
        invalidateKeys: [deviceQueryKeys.all],
    });
    return { packets, toggle };
}
