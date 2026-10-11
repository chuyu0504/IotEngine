export namespace Alert {
    export type Severity = 'critical' | 'warning' | 'info';
    export type RecordStatus = 'active' | 'acknowledged' | 'resolved';
    export type ConditionType = 'threshold' | 'offline' | 'rate_of_change';
    export type Operator = '>' | '>=' | '<' | '<=' | '==' | '!=';
    export type ChangeDirection = 'rise' | 'fall' | 'any';
    export interface Condition {
        type: ConditionType;
        elementKey?: string;
        operator?: Operator;
        value?: string;
        duration?: number;
        changeRate?: string;
        changeDirection?: ChangeDirection;
        bitIndex?: number;
    }
    export interface RuleItem {
        id: string;
        name: string;
        device_id: string;
        device_name: string;
        severity: Severity;
        conditions: Condition[];
        logic: 'and' | 'or';
        silence_duration: number;
        recovery_condition: string;
        recovery_wait_seconds: number;
        status: 'enabled' | 'disabled';
        remark: string | null;
        created_at: string;
        updated_at: string;
    }
    export interface RuleDto {
        name: string;
        device_id: string;
        severity: Severity;
        conditions: Condition[];
        logic: 'and' | 'or';
        silence_duration: number;
        recovery_condition?: string;
        recovery_wait_seconds?: number;
        status?: 'enabled' | 'disabled';
        remark?: string;
    }
    export interface RecordItem {
        id: string;
        rule_id: string;
        rule_name: string;
        device_id: string;
        device_name: string;
        severity: Severity;
        status: RecordStatus;
        message: string;
        detail: Record<string, unknown>;
        triggered_at: string;
        acknowledged_at?: string;
        acknowledged_by?: string;
        resolved_at?: string;
    }
    export interface ActiveStats {
        total: number;
        critical: number;
        warning: number;
        info: number;
        today_new: number;
        acknowledged: number;
        today_resolved: number;
        affected_devices: number;
    }
    export interface TemplateItem {
        id: string;
        name: string;
        category: string | null;
        description: string | null;
        severity: Severity;
        logic: 'and' | 'or';
        silence_duration: number;
        protocol_config_id?: string;
        config_name?: string;
        protocol_type?: string;
        created_at: string;
    }
    export interface TemplateDetail {
        id: string;
        name: string;
        category: string | null;
        description: string | null;
        severity: Severity;
        conditions: Condition[];
        logic: 'and' | 'or';
        silence_duration: number;
        recovery_condition: string;
        recovery_wait_seconds: number;
        applicable_protocols: string[];
        protocol_config_id?: string;
        created_by: string;
        created_at: string;
    }
    export interface TemplateDto {
        name: string;
        category?: string;
        description?: string;
        severity: Severity;
        conditions: Condition[];
        logic: 'and' | 'or';
        silence_duration: number;
        recovery_condition?: string;
        recovery_wait_seconds?: number;
        applicable_protocols?: string[];
        protocol_config_id?: string;
    }
    export interface ApplyTemplateRequest {
        template_id: string;
        device_ids: string[];
    }
    export interface ApplyTemplateResponse {
        success: number;
        total: number;
        createdIds: string[];
    }
}
