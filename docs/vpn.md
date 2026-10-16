# VPN Hub 内核接口隔离

WireGuard 接口（`VPN_HUB_INTERFACE`）和 nftables 表 `inet iot_vpn` 都属于主机级全局资源。同一主机上的所有 Service Worker 和所有进程实例共用它们。本文说明谁有权修改它们、如何串行、按什么粒度执行，以及失败后如何自愈。

## 谁有权修改

- 只有 `service/features/vpn/vpn.runtime.h` 中的 `VpnHubRuntime` 会调用 `wireguard::controller()` 和 `firewall::apply()`。`VpnHubRuntime` 每个 Service Worker 一个实例。`vpn.service.h` 只访问数据库和 Redis，不引用 transport。
- `wireguard::controller()` 是 `thread_local` 对象。netlink socket 每次调用时打开，Worker 之间不共享任何内核句柄或可变对象。
- 模块（`modules/vpn`）不直接触碰内核。它通过 RPC 组件 `vpn` 请求修改：`reconcile`、`wireguard-remove-peer`、`sync-edge-routes`、`queue-edge-config`。Edge 投影通过 `edge-capability` 和 `edge-config-result` 发出通知。操作名和载荷定义在 `common/message.h` 的 `service::message::vpn` 段。

## 如何串行

- 每次修改内核状态（完整对账、移除 Peer、桌面会话到期后的需求同步）之前，都要在独立连接 `vpn-coordination` 的事务中取得 `lock::kVpnReconciliation`（`pg_advisory_xact_lock`）。这把锁在所有实例之间全局互斥。
- 完整对账在 `kVpnReconciliation` 之后还会持有 `lock::kVpnAddressAllocation`，直到内核状态写完为止。这样可以保证地址被回收后，旧密钥不会被重新装回接口。
- 锁顺序固定为 `kVpnReconciliation → kVpnAddressAllocation`，二者都在 `vpn-coordination` 连接上获取。每个 Peer 的配置锁（`hashtextextended(peerId, kVpnEdgeConfigSeed)`，在 `vpn-edge-config` 连接上）和 `kVpnDefaultNetwork`（在默认连接上）都不会反向等待前两把锁。
- 模块侧的地址分配和路由映射同样取 `kVpnAddressAllocation`，但必须先提交自己的事务，再等待 `sync-edge-routes` 的 RPC 回执。RPC 在调用方所在的同一个 Worker 上执行，如果持锁等待会自锁。

## 执行粒度与调度

- 事件触发：模块在 Peer、路由、桌面选择或会话发生变化后调用 `reconcile`；`edge-capability` 在激活 Peer 后也会执行一次对账。
- 漂移校正：每个实例每 5 分钟（`kDriftCorrectionPeriod`）最多执行一次。各 Worker 先检查本实例的 `ReconciliationSchedule` 租约（Redis 键前缀 `iot_vpn_reconciliation`，按实例 ID，TTL 为 5 分钟），再用 `pg_try_advisory_xact_lock(kVpnReconciliation)` 认领。只有认领者执行。任意一次成功的对账（包括事件触发的）都会续租，从而把下一次漂移校正推后。启动时立即尝试一次，用于创建接口。
- 桌面会话到期：每次计算需求时都会得到最近的会话截止时间（Redis 时钟）。持有该截止时间的 Worker 在截止时间加 250 ms 后唤醒，并通过 `iot:vpn:desktop:expiry` 的比较写脚本认领。只有截止时间晚于已处理值的那个 Worker 执行 `synchronizeEdgeDemand`，并继续跟踪下一个截止时间；其他 Worker 放弃跟踪。会话到期只改变 Edge 配置的 enabled 状态，Hub 的 Peer 和防火墙规则与会话无关，所以这一步不重做内核对账。
- 所有睡眠都使用 `ruvia::sleep_for(worker, duration, stop)`。停止时立即返回，不会等待固定 tick。
- 空闲时（没有事件、没有活动桌面会话）每个 Worker 每 5 分钟只做一次 Redis 租约检查，不再执行固定 10 秒的数据库或 Redis 操作。
- `VPN_HUB_ENABLED=false` 只关闭上述后台调度。RPC 操作仍然可用。

## Handshake 写库时机

`vpn_peer.last_handshake_at` 在每次完整对账结束时根据内核报告更新，触发点包括事件触发的 `reconcile`、`edge-capability` 和漂移校正。因此空闲时最大滞后约 5 分钟（原先是 10 秒）。

## 失败自愈

- 对账失败（内核、防火墙、Peer 清单读取）时不续租，下一次漂移校正会重试。事件触发的 `reconcile` 以 21005 返回失败给调用方。
- 漂移校正和到期处理的异常通过 `LOG_ERROR` 记录，调度继续。
- 持有到期跟踪的 Worker 或实例退出后，其他实例的漂移认领者会重新计算截止时间并接管，最长延迟 5 分钟。
- 移除 Peer 时只做格式校验（`validKey`），这样接口上任何已存在的密钥都能被移除。授予访问时要求 `validManagedKey`：全零密钥是低阶点，永远无法完成握手；全零私钥是公开已知的，因此 Hub 私钥同样拒绝全零值。
