# VPN Hub 内核接口隔离

WireGuard 接口（`VPN_HUB_INTERFACE`）和 nftables 表 `inet iot_vpn` 都属于主机级全局资源。同一主机上的所有 Service Worker 和所有进程实例共用它们。本文说明谁有权修改它们、如何串行、按什么粒度执行，以及失败后如何自愈。

## 谁有权修改

- `vpn.service.h` 中的 `VpnHubService` 负责对账、内核操作和业务持久化；`VpnHubRuntime` 只负责启动、订阅、最近截止时间和停止。每个 Service Worker 独占自己的 service/runtime，runtime 借用 service，RPC 直接调用该 service，不通过 runtime 转发。
- `wireguard::controller()` 是 `thread_local` 对象。netlink socket 每次调用时打开，Worker 之间不共享任何内核句柄或可变对象。
- 模块（`modules/vpn`）不直接触碰内核。它通过 RPC 组件 `vpn` 请求修改：`reconcile`、`wireguard-remove-peer`、`sync-edge-routes`、`queue-edge-config`。Edge 投影提交自身变更后，通过 `edge-capability` 和 `edge-config-result` 发出通知。追加通知失败时投影条目不确认，重试会按已持久化的状态再次发送（`edge-config-result` 取已终态的 `edge_task`），所以两个处理器都是幂等的。操作名和载荷定义在 `common/message.h` 的 `service::message::vpn` 段。

## 如何串行

- 每次修改内核状态（完整对账、移除 Peer、桌面会话到期后的需求同步）之前，都要在独立连接 `vpn-coordination` 的事务中取得 `lock::kVpnReconciliation`（`pg_advisory_xact_lock`）。这把锁在所有实例之间全局互斥。
- 完整对账在 `kVpnReconciliation` 之后还会持有 `lock::kVpnAddressAllocation`，直到内核状态写完为止。这样可以保证地址被回收后，旧密钥不会被重新装回接口。
- 锁顺序固定为 `kVpnReconciliation → kVpnAddressAllocation`，二者都在 `vpn-coordination` 连接上获取。每个 Peer 的配置锁（`hashtextextended(peerId, kVpnEdgeConfigSeed)`，在 `vpn-edge-config` 连接上）和 `kVpnDefaultNetwork`（在默认连接上）都不会反向等待前两把锁。
- 模块侧的地址分配和路由映射同样取 `kVpnAddressAllocation`，但必须先提交自己的事务，再等待 `sync-edge-routes` 的 RPC 回执。RPC 在调用方所在的同一个 Worker 上执行，如果持锁等待会自锁。
- 创建 Edge Peer 时先以 `revoked` 状态提交新行（占用地址），映射路由后再在新事务中重新激活；复用已撤销的行时同样先映射再激活。对账和 `edge-capability` 都不会装入 `revoked` Peer 的密钥，所以映射失败时 Hub 上不会出现半创建的 Peer；新行随后被删除（仅当仍为 `revoked`）。

## 执行粒度与调度

- 事件触发：模块在 Peer、路由、桌面选择或会话发生变化后调用 `reconcile`；`edge-capability` 在激活 Peer 后也会执行一次对账。
- 启动和真实变更触发对账，不再运行每 5 分钟的漂移扫描。各 Worker 使用相同的事务锁与持久截止记录认领，不按 Worker 编号分配任务。
- 桌面会话到期：需求计算得到最近的真实会话截止时间，写入实例的持久调度记录。只有认领者执行到期后的 `synchronizeEdgeDemand`，再保存下一截止；会话到期只改变 Edge 配置的 enabled 状态，不无条件重做完整内核对账。
- 变更和重试截止先持久化，再发布本实例的调度通知。runtime 先确认独立 Pub/Sub 订阅再读取截止；健康订阅读取与截止等待分离，到期不靠关闭订阅或重连刷新。
- 空闲且没有会话或失败重试截止时，不定时查询数据库或 Redis。停止时取消通知读取和截止等待，等待所属 Worker 的任务结束。
- `VPN_HUB_ENABLED=false` 只关闭上述后台调度。RPC 操作仍然可用。

## Handshake 写库时机

`vpn_peer.last_handshake_at` 在真实完整对账结束时根据内核报告更新，触发点包括显式 `reconcile`、`edge-capability`、启动恢复和失败重试；不再为刷新该字段周期性执行内核及数据库对账。

## 失败自愈

- 对账失败（内核、防火墙、Peer 清单读取）时记录异常并保留持久重试截止；事件触发的 `reconcile` 仍以 21005 返回失败给调用方。成功不另设周期扫描。
- 到期与重试操作使用同一认领机制。失败不丢失重试身份，迟到任务不能清掉更新后的截止记录。
- Worker 退出后，同实例的其他 Worker 仍能读取持久截止并认领；进程重启在初始恢复时重建实际会话与失败重试需求，无固定五分钟接管窗口。
- 移除 Peer 时只做格式校验（`validKey`），这样接口上任何已存在的密钥都能被移除。授予访问时要求 `validManagedKey`：全零密钥是低阶点，永远无法完成握手；全零私钥是公开已知的，因此 Hub 私钥同样拒绝全零值。
