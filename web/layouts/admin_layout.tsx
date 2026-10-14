import {
    AlertOutlined,
    ApartmentOutlined,
    ApiOutlined,
    AppstoreOutlined,
    CloudServerOutlined,
    ClusterOutlined,
    DownOutlined,
    HddOutlined,
    LinkOutlined,
    LogoutOutlined,
    MenuFoldOutlined,
    MenuUnfoldOutlined,
    SafetyCertificateOutlined,
    SettingOutlined,
    UserOutlined,
    VideoCameraOutlined,
} from '@ant-design/icons';
import {
    Avatar,
    Breadcrumb,
    Button,
    Dropdown,
    Layout,
    Menu,
    type MenuProps,
    Space,
    Spin,
    theme,
} from 'antd';
import { type ReactNode, useMemo, useState } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { APP_NAME } from '@/config/app';
import { usePermissions } from '@/hooks/use_permission';
import {
    expandedMenuKeys,
    getVisibleNavigationGroups,
    navigationGroups,
    type NavigationIcon,
} from '@/routes/navigation';
import { useGb28181Health } from '../pages/gb28181/gb28181.service';
import { useCurrentUser, useLogout } from '../pages/login/login.service';

const { Header, Sider, Content } = Layout;

const navigationIcons: Record<NavigationIcon, ReactNode> = {
    link: <LinkOutlined />,
    edge_node: <CloudServerOutlined />,
    protocol: <ClusterOutlined />,
    device: <HddOutlined />,
    alert: <AlertOutlined />,
    gb28181: <VideoCameraOutlined />,
    open_access: <ApiOutlined />,
    system: <SettingOutlined />,
    role: <SafetyCertificateOutlined />,
    dept: <ApartmentOutlined />,
    user: <UserOutlined />,
};

export default function AdminLayout() {
    const [collapsed, setCollapsed] = useState(false);
    const navigate = useNavigate();
    const location = useLocation();
    const { data: user, isLoading } = useCurrentUser();
    const logout = useLogout();
    const { has } = usePermissions();
    const canQueryGb28181 = has('iot:gb28181:query');
    const { data: gb28181Health } = useGb28181Health({
        enabled: canQueryGb28181,
    });
    const gb28181Enabled = gb28181Health?.enabled === true;
    const { token } = theme.useToken();
    const visibleGroups = useMemo(
        () => getVisibleNavigationGroups(has, { gb28181: gb28181Enabled }),
        [has, gb28181Enabled]
    );
    const currentBreadcrumbGroup = navigationGroups.find((group) =>
        group.pages.some((route) => route.path === location.pathname)
    );
    const currentBreadcrumbRoute = currentBreadcrumbGroup?.pages.find(
        (route) => route.path === location.pathname
    );
    const siblingRoutes =
        visibleGroups.find((group) => group.key === currentBreadcrumbGroup?.key)?.pages ?? [];

    const breadcrumbItems = currentBreadcrumbGroup
        ? [
              {
                  title:
                      siblingRoutes.length > 1 ? (
                          <Dropdown
                              trigger={['hover']}
                              menu={{
                                  items: siblingRoutes.map((route) => ({
                                      key: route.path,
                                      label: route.title,
                                      disabled: route.path === location.pathname,
                                      onClick: () => navigate(route.path),
                                  })),
                              }}
                          >
                              <span
                                  className="inline-flex cursor-pointer items-center gap-1 hover:opacity-75"
                                  style={{ color: token.colorTextSecondary }}
                              >
                                  {navigationIcons[currentBreadcrumbGroup.icon]}
                                  {currentBreadcrumbGroup.title}
                                  <DownOutlined className="text-[10px]" />
                              </span>
                          </Dropdown>
                      ) : (
                          <span className="inline-flex items-center gap-1">
                              {navigationIcons[currentBreadcrumbGroup.icon]}
                              {currentBreadcrumbGroup.title}
                          </span>
                      ),
              },
              {
                  title: (
                      <span
                          className="inline-flex items-center gap-1 font-medium"
                          style={{ color: token.colorText }}
                      >
                          {currentBreadcrumbRoute?.title}
                      </span>
                  ),
              },
          ]
        : [
              {
                  title: (
                      <span className="inline-flex items-center gap-1">
                          <AppstoreOutlined />
                          运营控制台
                      </span>
                  ),
              },
          ];

    const menuItems = useMemo(() => {
        const items: NonNullable<MenuProps['items']> = [];
        for (const group of visibleGroups) {
            const children = group.pages.map((page) => ({
                key: page.path,
                label: page.title,
                icon: page.icon ? navigationIcons[page.icon] : undefined,
            }));
            if (group.menuKey) {
                items.push({
                    key: group.menuKey,
                    icon: navigationIcons[group.icon],
                    label: group.title,
                    children,
                });
            } else {
                items.push(...children);
            }
        }
        return items;
    }, [visibleGroups]);

    if (isLoading && !user) {
        return (
            <div className="flex h-screen items-center justify-center">
                <Spin size="large" />
            </div>
        );
    }

    return (
        <Layout className="h-screen overflow-hidden" style={{ background: '#f5f5f5' }}>
            <Sider
                theme="dark"
                width={236}
                collapsedWidth={72}
                collapsible
                collapsed={collapsed}
                trigger={null}
                breakpoint="lg"
                onBreakpoint={(broken) => setCollapsed(broken)}
                style={{
                    background: '#001529',
                    borderInlineEnd: '1px solid rgba(255, 255, 255, 0.1)',
                }}
            >
                <div className="flex h-full flex-col overflow-hidden">
                    <div
                        className={`flex h-[60px] shrink-0 items-center ${
                            collapsed ? 'justify-center px-2' : 'px-5'
                        }`}
                        style={{ borderBlockEnd: '1px solid rgba(255, 255, 255, 0.1)' }}
                    >
                        <div
                            className="flex size-9 shrink-0 items-center justify-center rounded-md text-lg"
                            style={{
                                background: 'rgba(255, 255, 255, 0.12)',
                                color: '#fff',
                                boxShadow: '0 4px 12px rgba(0, 0, 0, 0.18)',
                            }}
                        >
                            <AppstoreOutlined />
                        </div>
                        {!collapsed && (
                            <div className="ml-3 min-w-0">
                                <div
                                    className="truncate text-[15px] font-semibold tracking-wide"
                                    style={{ color: '#fff' }}
                                >
                                    {APP_NAME}
                                </div>
                                <div
                                    className="mt-0.5 text-[10px] tracking-[0.16em]"
                                    style={{ color: 'rgba(255, 255, 255, 0.45)' }}
                                >
                                    INDUSTRIAL IOT
                                </div>
                            </div>
                        )}
                    </div>
                    {!collapsed && (
                        <div
                            className="px-5 pb-2 pt-5 text-[11px] font-medium tracking-[0.14em]"
                            style={{ color: 'rgba(255, 255, 255, 0.45)' }}
                        >
                            运营控制台
                        </div>
                    )}
                    <Menu
                        theme="dark"
                        mode="inline"
                        selectedKeys={[location.pathname]}
                        defaultOpenKeys={expandedMenuKeys}
                        items={menuItems}
                        onClick={({ key }) => navigate(key)}
                        className="min-h-0 flex-1 overflow-y-auto border-none py-1 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
                        style={{ background: '#001529' }}
                    />
                </div>
            </Sider>
            <Layout className="min-w-0" style={{ background: '#f5f5f5' }}>
                <Header
                    className="z-10 flex h-[60px] shrink-0 items-center justify-between px-3 sm:px-5"
                    style={{
                        background: '#fff',
                        borderBlockEnd: `1px solid ${token.colorBorderSecondary}`,
                    }}
                >
                    <div className="flex min-w-0 flex-1 items-center gap-2">
                        <Button
                            type="text"
                            aria-label={collapsed ? '展开侧边栏' : '收起侧边栏'}
                            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
                            onClick={() => setCollapsed((value) => !value)}
                        />
                        <div className="hidden min-w-0 sm:block">
                            <Breadcrumb items={breadcrumbItems} />
                        </div>
                    </div>
                    <Dropdown
                        trigger={['click']}
                        menu={{
                            items: [
                                {
                                    key: 'logout',
                                    icon: <LogoutOutlined />,
                                    label: '退出登录',
                                },
                            ],
                            onClick: () => logout.mutate(),
                        }}
                    >
                        <Button type="text" className="h-10 px-2">
                            <Space size={10}>
                                <Avatar
                                    size={30}
                                    icon={<UserOutlined />}
                                    style={{
                                        background: token.colorPrimaryBg,
                                        color: token.colorPrimary,
                                    }}
                                />
                                <span
                                    className="hidden text-sm font-medium sm:inline"
                                    style={{ color: token.colorText }}
                                >
                                    {user?.nickname || user?.username}
                                </span>
                            </Space>
                        </Button>
                    </Dropdown>
                </Header>
                <Content
                    className="m-3 min-h-0 overflow-hidden rounded-lg sm:m-4"
                    style={{
                        background: '#fff',
                        border: `1px solid ${token.colorBorderSecondary}`,
                        boxShadow: token.boxShadowTertiary,
                    }}
                >
                    <Outlet />
                </Content>
            </Layout>
        </Layout>
    );
}
