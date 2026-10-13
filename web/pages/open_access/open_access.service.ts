import { createQueryKeys } from '@/utils/query';
import { useQuery } from '@tanstack/react-query';
import { useMutationWithMessage } from '@/hooks/use_mutation';
import {
    createKey,
    createMqtt,
    deleteMqtt,
    getMqtt,
    updateMqtt,
    createWebhook,
    deleteKey,
    deleteWebhook,
    getDevices,
    getKeys,
    getLogs,
    getWebhooks,
    rotateKey,
    updateKey,
    updateWebhook,
} from './open_access.api';
import type { Access } from './open_access.types';
const keys = createQueryKeys('open-access');
const accessQueryKeys = {
    ...keys,
    devices: () => [...keys.all, 'devices'] as const,
    keys: () => [...keys.all, 'keys'] as const,
    mqtt: () => [...keys.all, 'mqtt'] as const,
    webhooks: (accessKeyId?: string) => [...keys.all, 'webhooks', accessKeyId] as const,
    logs: (query: Access.LogQuery) => [...keys.all, 'logs', query] as const,
};

export const useOpenDevices = () =>
    useQuery({
        queryKey: accessQueryKeys.devices(),
        queryFn: ({ signal }) => getDevices(signal),
        refetchInterval: false,
    });
export const useOpenKeys = () =>
    useQuery({
        queryKey: accessQueryKeys.keys(),
        queryFn: ({ signal }) => getKeys(signal),
        refetchInterval: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
export const useOpenWebhooks = (accessKeyId?: string) =>
    useQuery({
        queryKey: accessQueryKeys.webhooks(accessKeyId),
        queryFn: ({ signal }) => getWebhooks(accessKeyId, signal),
        refetchInterval: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
export const useOpenLogs = (query: Access.LogQuery) =>
    useQuery({
        queryKey: accessQueryKeys.logs(query),
        queryFn: ({ signal }) => getLogs(query, signal),
        refetchInterval: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
export const useKeyCreate = () =>
    useMutationWithMessage({
        mutationFn: createKey,
        successMessage: '调用配置已创建，请立即保存 AccessKey',
        invalidateKeys: [accessQueryKeys.keys()],
    });
export const useKeyUpdate = () =>
    useMutationWithMessage({
        mutationFn: ({ id, data }: { id: string; data: Access.KeySaveDto }) => updateKey(id, data),
        successMessage: '调用配置已更新',
        invalidateKeys: [
            accessQueryKeys.keys(),
            [...accessQueryKeys.all, 'webhooks'],
            accessQueryKeys.mqtt(),
            [...accessQueryKeys.all, 'logs'],
        ],
    });
export const useKeyRotate = () =>
    useMutationWithMessage({
        mutationFn: rotateKey,
        successMessage: 'AccessKey 已轮换，旧密钥立即失效',
        invalidateKeys: [accessQueryKeys.keys()],
    });
export const useKeyDelete = () =>
    useMutationWithMessage({
        mutationFn: deleteKey,
        successMessage: '调用配置已删除',
        invalidateKeys: [
            accessQueryKeys.keys(),
            [...accessQueryKeys.all, 'webhooks'],
            accessQueryKeys.mqtt(),
            [...accessQueryKeys.all, 'logs'],
        ],
    });
export const useWebhookCreate = () =>
    useMutationWithMessage({
        mutationFn: createWebhook,
        successMessage: 'Webhook 已创建',
        invalidateKeys: [accessQueryKeys.keys(), [...accessQueryKeys.all, 'webhooks']],
    });
export const useWebhookUpdate = () =>
    useMutationWithMessage({
        mutationFn: ({ id, data }: { id: string; data: Access.WebhookSaveDto }) =>
            updateWebhook(id, data),
        successMessage: 'Webhook 已更新',
        invalidateKeys: [
            [...accessQueryKeys.all, 'webhooks'],
            [...accessQueryKeys.all, 'logs'],
        ],
    });
export const useWebhookDelete = () =>
    useMutationWithMessage({
        mutationFn: deleteWebhook,
        successMessage: 'Webhook 已删除',
        invalidateKeys: [
            [...accessQueryKeys.all, 'webhooks'],
            [...accessQueryKeys.all, 'logs'],
        ],
    });

export const useOpenMqtt = () =>
    useQuery({
        queryKey: accessQueryKeys.mqtt(),
        queryFn: ({ signal }) => getMqtt(signal),
        refetchInterval: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
export const useMqttCreate = () =>
    useMutationWithMessage({
        mutationFn: createMqtt,
        successMessage: 'MQTT 配置已创建',
        invalidateKeys: [accessQueryKeys.mqtt()],
    });
export const useMqttUpdate = () =>
    useMutationWithMessage({
        mutationFn: ({ id, data }: { id: string; data: Access.MqttSaveDto }) =>
            updateMqtt(id, data),
        successMessage: 'MQTT 配置已更新',
        invalidateKeys: [accessQueryKeys.mqtt(), [...accessQueryKeys.all, 'logs']],
    });
export const useMqttDelete = () =>
    useMutationWithMessage({
        mutationFn: deleteMqtt,
        successMessage: 'MQTT 配置已删除',
        invalidateKeys: [accessQueryKeys.mqtt(), [...accessQueryKeys.all, 'logs']],
    });
