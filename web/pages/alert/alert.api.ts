import request from '@/lib/http';
import { createSseSnapshotStream } from '@/lib/snapshot_request';
import type { PaginatedResult } from '@/types/pagination';
import type { Alert } from './alert.types';

export const getRules = (params?: Record<string, unknown>, signal?: AbortSignal) =>
    request.get<PaginatedResult<Alert.RuleItem>>('/v1/alert/rules', { params, signal });
export const createRule = (data: Alert.RuleDto) => request.post<void>('/v1/alert/rules', data);
export const updateRule = (id: string, data: Alert.RuleDto) =>
    request.put<void>(`/v1/alert/rules/${id}`, data);
export const deleteRule = (id: string) => request.delete<void>(`/v1/alert/rules/${id}`);
export const batchDeleteRules = (ids: string[]) =>
    request.delete<void>('/v1/alert/rules', { data: { ids } });
export const applyTemplate = (data: Alert.ApplyTemplateRequest) =>
    request.post<Alert.ApplyTemplateResponse>('/v1/alert/rules/apply-template', data);
export const getTemplates = (params?: Record<string, unknown>, signal?: AbortSignal) =>
    request.get<PaginatedResult<Alert.TemplateItem>>('/v1/alert/templates', { params, signal });
export const getTemplateDetail = (id: string, signal?: AbortSignal) =>
    request.get<Alert.TemplateDetail>(`/v1/alert/templates/${id}`, { signal });
export const createTemplate = (data: Alert.TemplateDto) =>
    request.post<void>('/v1/alert/templates', data);
export const updateTemplate = (id: string, data: Alert.TemplateDto) =>
    request.put<void>(`/v1/alert/templates/${id}`, data);
export const deleteTemplate = (id: string) => request.delete<void>(`/v1/alert/templates/${id}`);
export const getRecords = (params?: Record<string, unknown>) =>
    createSseSnapshotStream<PaginatedResult<Alert.RecordItem>>(
        '/v1/alert/events',
        params,
        'records'
    );
export const acknowledgeRecord = (id: string) => request.post<void>(`/v1/alert/records/${id}/ack`);
export const batchAcknowledge = (ids: string[]) =>
    request.post<void>('/v1/alert/records/batch-ack', { ids });
export const getStats = (params?: Record<string, unknown>) =>
    createSseSnapshotStream<Alert.ActiveStats>('/v1/alert/events', params, 'stats');
