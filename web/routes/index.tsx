import { Button, Result, Spin } from 'antd';
import { lazy, type ReactElement, Suspense } from 'react';
import { createHashRouter, Navigate, Outlet, RouterProvider, useLocation } from 'react-router-dom';
import { usePermission } from '@/hooks/use_permission';
import { useGb28181Health } from '@/pages/gb28181/gb28181.service';
import { useCurrentUser, useLogout } from '@/pages/login/login.service';
import { useAuthStore } from '@/store/auth_store';
import { DEFAULT_MANAGEMENT_PATH, managementPages, type ManagementPageId } from './navigation';

const LoginPage = lazy(() => import('@/pages/login'));
const AdminLayout = lazy(() => import('@/layouts/admin_layout'));
const RolePage = lazy(() => import('@/pages/role'));
const DeptPage = lazy(() => import('@/pages/dept'));
const UserPage = lazy(() => import('@/pages/user'));
const LinkPage = lazy(() => import('@/pages/link'));
const ModbusConfigPage = lazy(() =>
    import('@/pages/protocol').then((module) => ({ default: module.ModbusConfigPage }))
);
const SL651ConfigPage = lazy(() =>
    import('@/pages/protocol').then((module) => ({ default: module.SL651ConfigPage }))
);
const S7ConfigPage = lazy(() =>
    import('@/pages/protocol').then((module) => ({ default: module.S7ConfigPage }))
);
const Dlt645ConfigPage = lazy(() =>
    import('@/pages/protocol').then((module) => ({ default: module.Dlt645ConfigPage }))
);
const MqttConfigPage = lazy(() =>
    import('@/pages/protocol').then((module) => ({ default: module.MqttConfigPage }))
);
const FinsConfigPage = lazy(() =>
    import('@/pages/protocol').then((module) => ({ default: module.FinsConfigPage }))
);
const McConfigPage = lazy(() =>
    import('@/pages/protocol').then((module) => ({ default: module.McConfigPage }))
);
const DevicePage = lazy(() => import('@/pages/device'));
const AccessPage = lazy(() => import('@/pages/open_access'));
const EdgeNodePage = lazy(() => import('@/pages/edge_node'));
const AlertPage = lazy(() => import('@/pages/alert'));
const Gb28181Page = lazy(() => import('@/pages/gb28181'));

const pageElements = {
    link: <LinkPage />,
    edge_node: <EdgeNodePage />,
    sl651: <SL651ConfigPage />,
    modbus: <ModbusConfigPage />,
    s7: <S7ConfigPage />,
    dlt645: <Dlt645ConfigPage />,
    fins: <FinsConfigPage />,
    mqtt: <MqttConfigPage />,
    mc: <McConfigPage />,
    device: <DevicePage />,
    alert: <AlertPage />,
    gb28181: <Gb28181Page />,
    open_access: <AccessPage />,
    role: <RolePage />,
    dept: <DeptPage />,
    user: <UserPage />,
} satisfies Record<ManagementPageId, ReactElement>;

const routeErrorElement = (
    <div className="flex h-screen items-center justify-center p-6">
        <Result
            status="error"
            title="页面加载失败"
            subTitle="页面运行时出现异常，请刷新后重试。"
            extra={
                <Button type="primary" onClick={() => window.location.reload()}>
                    刷新页面
                </Button>
            }
        />
    </div>
);

function AuthGuard() {
    const token = useAuthStore((state) => state.token);
    const location = useLocation();
    if (!token) {
        return <Navigate to="/login" replace state={{ from: { pathname: location.pathname } }} />;
    }
    return <Outlet />;
}

/** 应用入口装配布局所需的会话、退出与功能开关，布局本身不依赖页面 Service。 */
function AdminShell() {
    const { data: user, isLoading } = useCurrentUser();
    const logout = useLogout();
    const canQueryGb28181 = usePermission('iot:gb28181:query');
    const { data: gb28181Health } = useGb28181Health({ enabled: canQueryGb28181 });
    return (
        <AdminLayout
            user={user}
            userLoading={isLoading}
            features={{ gb28181: gb28181Health?.enabled === true }}
            onLogout={() => logout.mutate()}
        />
    );
}

const router = createHashRouter([
    {
        path: '/login',
        element: <LoginPage />,
        errorElement: routeErrorElement,
    },
    {
        element: <AuthGuard />,
        errorElement: routeErrorElement,
        children: [
            {
                path: '/',
                element: <AdminShell />,
                children: [
                    { index: true, element: <Navigate to={DEFAULT_MANAGEMENT_PATH} replace /> },
                    ...managementPages.map((page) => ({
                        path: page.path.slice(1),
                        element: pageElements[page.id],
                    })),
                ],
            },
        ],
    },
    { path: '*', element: <Navigate to="/" replace /> },
]);

export function AppRoutes() {
    return (
        <Suspense
            fallback={
                <div className="flex h-screen items-center justify-center">
                    <Spin size="large" />
                </div>
            }
        >
            <RouterProvider router={router} />
        </Suspense>
    );
}
