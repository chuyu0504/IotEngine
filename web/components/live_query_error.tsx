import { Alert, Button } from 'antd';

export function LiveQueryError({
    error,
    retry,
    loading = false,
}: {
    error?: Error | null;
    retry: () => unknown;
    loading?: boolean;
}) {
    if (!error) return null;
    return (
        <Alert
            type="error"
            showIcon
            message="实时连接已断开"
            description={error.message}
            action={
                <Button loading={loading} onClick={() => void retry()}>
                    重新连接
                </Button>
            }
        />
    );
}
