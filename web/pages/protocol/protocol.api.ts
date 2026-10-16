import request, { type RequestConfig } from '@/lib/http';
import type { PaginatedResult } from '@/types/pagination';
import {
    dlt645PointCreateSchema,
    protocolCreateSchema,
    protocolIdSchema,
    protocolTypeSchema,
    protocolUpdateSchema,
} from './protocol.schema';
import type {
    Dlt645PointCreateDto,
    ExpressionTestRequest,
    ExpressionTestResult,
    ExpressionValidateRequest,
    MqttPreviewRequest,
    MqttPreviewResult,
    Protocol,
} from './protocol.types';

export const getList = (params?: Protocol.Query, config?: RequestConfig) =>
    request.get<PaginatedResult<Protocol.Item>>('/v1/protocol/configs', {
        ...config,
        params: { ...params },
    });
export const getDetail = (id: string, signal?: AbortSignal) =>
    request.get<Protocol.Item>(`/v1/protocol/configs/${protocolIdSchema.parse(id)}`, { signal });
export const create = (data: Protocol.CreateDto, config?: RequestConfig) =>
    request.post<void>('/v1/protocol/configs', protocolCreateSchema.parse(data), config);
export const update = (id: string, data: Protocol.UpdateDto) =>
    request.put<void>(
        `/v1/protocol/configs/${protocolIdSchema.parse(id)}`,
        protocolUpdateSchema.parse(data)
    );
export const createDlt645Point = (id: string, data: Dlt645PointCreateDto) =>
    request.post<void>(
        `/v1/protocol/configs/${protocolIdSchema.parse(id)}/points`,
        dlt645PointCreateSchema.parse(data)
    );
export const remove = (id: string) =>
    request.delete<void>(`/v1/protocol/configs/${protocolIdSchema.parse(id)}`);
export const getOptions = (protocol: Protocol.Type, signal?: AbortSignal) =>
    request.get<PaginatedResult<Protocol.Option>>('/v1/protocol/configs/options', {
        params: { protocol: protocolTypeSchema.parse(protocol) },
        signal,
    });

export const testExpression = (data: ExpressionTestRequest) =>
    request.post<ExpressionTestResult>('/v1/protocol/configs/test-expression', data, {
        _silent: true,
    });

export const validateExpression = (data: ExpressionValidateRequest) =>
    request.post<void>('/v1/protocol/configs/validate-expression', data, { _silent: true });

export const previewMqtt = (data: MqttPreviewRequest) =>
    request.post<MqttPreviewResult>('/v1/protocol/configs/preview-mqtt', data, { _silent: true });
