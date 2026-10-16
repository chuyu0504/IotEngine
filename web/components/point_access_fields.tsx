import { Form, Switch } from 'antd';

interface PointAccessFieldsProps {
    showWritable?: boolean;
    visibleName?: string | (string | number)[];
    initialVisible?: boolean;
}

/** 点位表单的“可写”“显示”开关，须放在 antd Form 内。 */
export function PointAccessFields({
    showWritable = false,
    visibleName = 'visible',
    initialVisible,
}: PointAccessFieldsProps) {
    return (
        <div className="grid grid-cols-2 gap-x-4">
            {showWritable && (
                <Form.Item
                    name="writable"
                    label="可写"
                    valuePropName="checked"
                    tooltip="开启后该点位可用于写操作下发。"
                >
                    <Switch />
                </Form.Item>
            )}
            <Form.Item
                name={visibleName}
                label="显示"
                className="col-start-2"
                valuePropName="checked"
                initialValue={initialVisible}
                tooltip="关闭后仅隐藏展示，仍继续采集、计算、存储和告警。"
            >
                <Switch />
            </Form.Item>
        </div>
    );
}
