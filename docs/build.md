# 源码构建

第三方依赖由根目录 `vcpkg.json` 声明，配置阶段通过 vcpkg 清单模式安装到构建目录的 `vcpkg_installed/`。
vcpkg 与 Ruvia 一致跟踪最新端口，不固定 baseline；CI 的 `vcpkg_revision` 作业每次只解析一次最新 vcpkg 提交，
Linux 与 Windows 构建都检出该提交，两个部署包都等待两个平台构建测试通过，制品内 `build-info.txt` 记录源码提交与 vcpkg 提交。
Ruvia、ZLMediaKit、FAAC、nanopb 不在 vcpkg 中，仍由 CMake FetchContent 固定获取：
Ruvia、FAAC（`faac-1.31.1`）、nanopb 使用固定提交并带 SHA-256 校验的归档，ZLMediaKit 固定完整提交；Ruvia 的依赖由本项目清单统一提供。
`ports/vcpkg/` 只保留需要调整的 overlay 端口：`usrsctp` 在上游端口基础上将内部 `timingsafe_bcmp` 重命名为 `usrsctp_timingsafe_bcmp`。

## 工具链

- CMake 3.28 或更新版本、Git、Bun 1.4.3，以及已 bootstrap 的 vcpkg，`VCPKG_ROOT` 指向其根目录。
- Linux：Ubuntu 24.04、由 `mise` 管理的 `cross-tools/clang-cross` LLVM musl 工具链（`x86_64-unknown-linux-musl-clang` / `clang++`、`lld`、`x64-linux-musl` triplet）、`ccache`、Ninja、NASM、pkg-config、Bison、Flex、Make、curl、zip、unzip、tar、xz-utils；与 FlexEdge 的 Linux CI 工具链一致。
- Windows：MSVC C++ 工具链和 Windows SDK；vcpkg 自行获取 NASM、MSYS2 等端口构建工具。

具体工具安装步骤与 CI 保持一致，见 `.github/workflows/build.yml`。

GCC 13 在协程的 `co_await` 表达式中处理含 `std::string` 等非平凡元素的临时初始化列表时会触发内部编译错误。
Redis 参数、消息字段及 ORM 更新字段先保存到具名局部容器，再传入异步调用，保证其生命周期覆盖挂起与恢复。
请求模型的默认值规则保存为具名 `inline constexpr` 元数据，通过 `decltype` 供模型使用，避免匿名闭包引发链接属性告警；不修改 Ruvia 源码或降低编译检查。

本机使用 mise 管理工具时，可通过 `mise exec bun@1.4.3 -- <命令>` 为配置、构建和测试临时选择 CI 同版本的 Bun；
其他工具同样按上述工具链准备，不需要修改全局 mise 配置。CMake 首次配置须在该环境中运行，避免缓存其他版本的可执行文件路径。

## 配置、构建与测试

先安装前端依赖：

```text
bun install --frozen-lockfile
```

Linux：

```sh
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_COMPILE_WARNING_AS_ERROR=ON \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE="$PWD/ports/cmake/llvm.cmake" \
  -DVCPKG_TARGET_TRIPLET=x64-linux-musl \
  -DVCPKG_HOST_TRIPLET=x64-linux-musl \
  -DVCPKG_OVERLAY_TRIPLETS="$PWD/ports/cmake/triplets" \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build --config Release --parallel
ctest --test-dir build --build-config Release --output-on-failure
```

`collector-route` 回归测试会启动独立的内存 Redis，使用随机本地端口，验证连接租约到期、续期、重连保护、截止事件与遥测保留。
需将 `redis-server` 加入 `PATH`；测试临时目录与日志位于根 `build/`，不连接生产 Redis。
Linux CI 安装该工具并运行真实 Redis 回归；缺少工具的环境会明确标记该项为跳过，不能据此认定连接租约行为已验证。

### 集成测试

`tests/*-integration.ts` 等 Bun 集成测试注册为带 `integration` 标签的 CTest 用例，由 `ports/cmake/integration-fixture.cmake`
作为 CTest fixture 准备数据库与 Redis、启动并停止 `iot-engine`。数据库与 Redis 由环境变量提供：

|变量|内容|
|---|---|
|`IOT_TEST_POSTGRES_URL`|PostgreSQL/TimescaleDB 服务器，`postgres://user[:password]@host[:port][/database]`，账号须可建库|
|`IOT_TEST_REDIS_URL`|Redis 服务器，`redis://[[user]:password@]host[:port]`，不带库号|
|`IOT_TEST_MQTT_URL`|仅 `open-access-mqtt-integration` 使用的匿名 MQTT Broker，`mqtt://host:port`|

每个 fixture 在该服务器上重建自己的 `iot_architecture_<fixture>` 数据库，独占一个 Redis 库号（1–15）与从
`IOT_ENGINE_INTEGRATION_PORT_BASE`（默认 25100）起的十个本机端口，结束后删除数据库并清空 Redis 库；
端口块须避开本机临时端口范围（Linux 默认 32768–60999），否则出站连接占用端口会使 fixture 启动失败。
进程配置、日志与测试状态位于构建目录的 `integration/<fixture>/`。只允许本机地址，禁止指向生产服务。
缺少变量时用例以返回码 77 明确跳过。并行的 fixture 各自运行带连接池的 `iot-engine`，PostgreSQL 的
`max_connections` 需提高到 500（默认 100 会报 `too many clients`）。本地使用官方镜像：

```sh
docker run -d --name iotci-postgres -p 127.0.0.1:25432:5432 -e POSTGRES_USER=iot_test -e POSTGRES_PASSWORD=iot_test timescale/timescaledb:latest-pg18 -c max_connections=500
docker run -d --name iotci-redis -p 127.0.0.1:26379:6379 redis:latest
docker run -d --name iotci-mqtt -p 127.0.0.1:21883:1883 eclipse-mosquitto:latest mosquitto -c /mosquitto-no-auth.conf
IOT_TEST_POSTGRES_URL=postgres://iot_test:iot_test@127.0.0.1:25432/postgres \
IOT_TEST_REDIS_URL=redis://127.0.0.1:26379 \
IOT_TEST_MQTT_URL=mqtt://127.0.0.1:21883 \
ctest --test-dir build --build-config Release --output-on-failure -L integration -j 4
```

Linux CI 以 GitHub Actions `services:` 提供同样镜像并运行 `ctest -L integration`；Windows 托管 Runner 没有服务容器，
这些用例在 Windows 作业中报告为跳过。`windows-native-backend-integration` 仅在 Windows 注册，
需 `IOT_ENGINE_WINDOWS_CLIENT_PROBE` 指向 Windows 客户端构建的 `vpn_platform_http_probe.exe`；
`windows-http-sse-integration` 由 `clients/windows` 的 `windows-native-tests` 运行。
前端与契约测试在仓库根执行 `bun test ./tests/*-test.ts ./tests/*-test.tsx`，其中 `terminal-state-test.ts` 需要 `PATH` 中的 `redis-server`。

Windows：

```powershell
cmake -S . -B build -DCMAKE_COMPILE_WARNING_AS_ERROR=ON
cmake --build build --config Release --parallel
ctest --test-dir build --build-config Release --output-on-failure
```

根 `CMakeLists.txt` 未显式指定 `CMAKE_TOOLCHAIN_FILE` 时从 `VCPKG_ROOT` 选择 vcpkg 工具链；
Linux 默认使用 `x64-linux-musl` triplet 与 `ports/cmake/llvm.cmake` 链式加载工具链，Windows 默认使用与静态 MSVC 运行库匹配的 `x64-windows-static` triplet。
首次配置会编译全部 vcpkg 端口，耗时较长；设置 `VCPKG_DEFAULT_BINARY_CACHE` 可复用二进制包，启用 `ccache` 可缓存编译单元。

当前固定 Ruvia `79059651e0c264a711a9e789d41c8e12b2e003c0`，直接使用经 SHA-256 校验的上游归档（`f48499115a819d7d8ef9363f8ba867be53d92805abfddf35826e1549fd5d930c`），包含上游 MSVC 协程编译修复，不应用本地 Ruvia 补丁。
Ruvia 使用 Asio 的公开运行时配置与 disposition API；根清单通过 `ports/vcpkg/asio` 固定官方稳定版 `1.38.2` 及归档 SHA-512，Linux 和 Windows 共用。
该版本包含 HTTP/1 消息边界、请求与响应生命周期、取消注册、Redis 重连、SQL 表限定符作用域及公开 Redis Pub/Sub API 的修复和支持。每条实时连接和在途 RPC 独占订阅连接，不占普通命令池；订阅确认后才读取快照或发出请求，Pub/Sub 只通知，可靠结果仍从持久化结果键及序号/ACK 恢复。
业务变更分别发布到 `iot:db:<database>:live:topic:<topic>`；共享频道由实际 `REDIS_DATABASE` 显式派生，DB0 也不使用裸频道。同一数据库的多进程共享通知，不同数据库互不唤醒；Redis `SELECT` 本身不隔离 Pub/Sub。SSE 的权限缓存只属于当前连接，收到 auth 或丢失订阅时失效，恢复后只发送一次完整快照。终端 protocol 5+ 的输出一直保留到浏览器序号 ACK，取消订阅后仍能重放未确认帧；旧固件无序号协议保持原交互方式。开发代理在 SSE 上游 close/error 时结束下游，浏览器仅在真实断流时有界退避重连，不再使用 45 秒空闲 watchdog。
重订阅全部确认后、连接重新就绪前再次清空权限缓存并推进 generation，防止断线期间在途查询重填旧授权。第三方 SSE 在首帧提交前的认证、范围及查询错误保留原 HTTP 状态和 JSON；已建连后的撤权才使用 SSE error 并结束连接。终端刷新令牌不会丢弃待渲染帧：已接收但未渲染的重放不提前 ACK，已渲染帧重放会重发可确认序号，原 ACK 或重放 ACK 首次成功只推进一次渲染队列。
`iot:live:changes` 只保留 runtime-config 的持久中继，不用于页面或定向回执。遥测、命令、配置及设备接入的持久消费流与业务截止时间 wake 流保留；每次实际 `XADD` 在同一 Lua 内发布 `iot:db:<database>:consumer:input:<stream>`，消费前先确认输入/截止时间订阅。PEL 可见性期限写入独立有序集合，ACK 同时移除对应期限并通知 `iot:db:<database>:consumer:deadline:<group>:<stream>`，消费者按最近期限唤醒，不固定周期扫描 PEL。开放接入事件与 Webhook wake 同时持久化、同时通知，幂等重放不重复入队或通知。持久键、Stream、结果与专属 UUID 频道不变。
共享频道命名为一次性切换，不保留裸频道旁路。升级与回滚都须先停止使用同一 Redis 数据库的全部旧版本 IotEngine 进程，再统一启动目标版本，不允许新旧版本滚动混跑；保留持久流，由启动恢复流程处理未确认任务。
RPC 请求流只属于接入请求的 Service Worker。启动时先通过共享 `kEnsureGroupScript` 原子创建消费组并设置 24 小时 TTL，再开放监视任务与消费者就绪；缺组恢复沿同一入口携带 TTL，不留下 `MKSTREAM` 后未设置过期的窗口。每分钟只续基础设施租约，续租失败在一秒后重试，不执行周期性业务查询。`rpc-startup-lease-integration` 使用独立 MANAGED fixture，在首次就绪时立即检查各 Worker 请求流 TTL，验证缺组重建后的真实 RPC 回执，并在首分钟内 `SIGKILL` 后检查残留租约；测试仅缩短已有的有界 TTL 来验证无人清理时的过期，不修改产品租约配置。
连接预算须包含普通池、后台持久消费者，以及每条 SSE/WebSocket 和每个在途 RPC 的独立订阅连接，并用部署实例的 `CONFIG GET maxclients` 核定容量；订阅不占阻塞命令池。升级已有构建目录时先取消旧源码覆盖：`cmake -S . -B build -UFETCHCONTENT_SOURCE_DIR_RUVIA`，依赖必须从上述新 pin 下载。
运维快照仅在实际指标/健康状态变化时写入，空闲期只续健康租约；租约缺失或过期仍判定 Worker 不就绪。Outbox 聚合查询不再每 5 秒执行，最老待处理事件的 age 由读取端按持久时间戳计算，告警阈值按真实截止时间触发。回执保留清理按最早到期时间认领。空闲验收须区分业务查询与基础设施健康续租命令，不能把后者报告为零 Redis 命令。
查询参数按需建立多值索引，PostgreSQL 参数复用已拥有的文本存储，Redis 类型化结果接管返回值存储；这些内部调整无需改动现有业务调用。
上游已移除 `db_expressions`；系统管理、协议与链路模块统一使用公开 `db_query` 持有表达式，
保持参数绑定、事务及表达式存储生命周期，不增加兼容包装。
业务读写使用实体 `repository().query_builder()` 与写 Builder，查询表达式由公开 `db_query` 持有。
`db_find_options`、CTE、窗口与 upsert 的 span 参数使用具名容器，覆盖 Builder 导入前的借用期；
upsert 显式限制冲突更新列，保持原有状态和操作员自定义字段。
读写 Builder 的 `query()` 视图用于组合事务 CTE，调用方先保存具名 Builder；导入时克隆 AST 与参数，覆盖源 Builder 的借用期。
复杂查询使用公开的无实体 LATERAL JOIN 与 `db_selection::star()`，保持关联条件、星号展开和原子提交边界。
Ruvia 的 mimalloc 依赖由根 `vcpkg.json` 统一提供；仅在非 Windows 平台启用 `override`，并由 Ruvia 整体链接静态库以替换进程的 `malloc` / `new`。Windows 保持原有分配器集成。

SQL 实体列名统一通过实体公开的 `consteval column_name<"字段">()` 读取，Redis 实体列名通过 `column_index<...>()`、`columns_type` 和列描述的 `name.view()` 读取；事务级 advisory lock 统一使用公开 `db_transaction::advisory_lock` 与 `try_advisory_lock` API。
直接使用 `column_name<...>()` 可保持实体编译期字段归属校验，同时避免在协程表达式中构造临时字段引用对象触发 MSVC 内部编译错误。
`file_response_options::path_` 借用文件路径，调用 `c.file()` 时须传入仍然存活的具名路径；
`firmware-download` 回归测试覆盖路径生命周期、中文文件名、完整下载、HEAD 和 Range。

管理端、后台媒体代理及测试 Controller 使用无框架基类的 `final` 类，并通过
`RUVIA_ROUTES_BEGIN(Type)` 声明路由；Middleware 直接满足公开概念，不再继承框架类型。
既有路由、权限及字段校验顺序保持一致，入口通过公开 `ruvia::exception_text` 记录 UTF-8 失败原因。

`document_root` 默认驻留不超过 64 KiB 的文件，
总预算为 16 MiB；部署重启会重建静态快照，验收须覆盖入口资源、条件请求和固件 Range 下载。
连接、请求体、HTTP 客户端及 WebSocket 接收使用公开的 `http_io_buffer` 管理准备、提交与消费，
项目不依赖此前的字符串覆盖写入实现；静态根目录绑定已打开的目录句柄，文件响应读取后复核身份。
无法从上下文推断类型的 PostgreSQL 无符号参数使用 `numeric`，保留完整 `uint64_t` 范围；
升级验收包含实际数据库表达式、分页与时间字段读写，以及语句失败后连接的继续使用。

服务通过 HTTP/1.1 提供接口，项目继续通过 Nginx 终止 TLS 并反代后端；不安装已无调用的 `ngtcp2`。
数据库连接池默认每个 Worker、每个数据库别名一条连接，迁移使用独立连接。
独立 HTTP、PostgreSQL、Redis 客户端通过公开 `handle()` 获取并复用操作句柄；句柄及结果在所属客户端释放前销毁，
客户端在所属 `event_loop` 等待 `shutdown()` 后再关闭线程。

### Worker 装配与启动认领

环境变量由进程及各组件的配置入口一次解析，装配阶段复用配置值，不在每个 Worker 重读。
进程身份在 `server.cpp` 生成一次，按 Worker 注入不可变的 `common::WorkerIdentity`；
UUID 的时间戳和 74-bit 序列由每个 Worker 的 `common::UuidV7State` 值持有，
`utils::nextUuid(state)` 显式借用该状态，时间前进或序列溢出时获取随机熵，无全局生成器或锁。

所有 Service Worker 都参与配置投影和 latest 恢复认领，不指定 Worker 0。
`startup` 数据库别名专门持有事务 advisory lock，完成标记按进程 incarnation 写入 Redis，
等待者只在成功完成后进入就绪；失败不写完成标记。配置新版本完整生成、latest 恢复完成后，
才启动 Collector、GB28181 和业务消费任务。latest 分页恢复合并为一次最终实时通知。

`ComponentLifecycle` 归属 observability 的运行监督，只有装配入口引用；
各 runtime 自己持有取消源和完成句柄，复用的启动、停止函数不持有运行状态。
Collector 与 GB28181 在同一 Collector event loop 上分别装配，先停止两者业务，
再等待该 loop 的 Redis `shutdown()`、销毁借用对象，最后停止并 join 线程。
设备协议端点位于 `gateway.transport.h`，保留唯一 Ruvia 路由宏和固件线协议。
transport 只持有本连接的 WebSocket 读写适配，不直接包含或调用 service/runtime；
`server.cpp` 为每个 Worker 注册当前连接的 `ConnectionDriver`，
runtime 在原 Worker 持有连接生命周期并经 protocol 解码、交所属 service 执行业务。
业务持久化和后台清理分别归 service、runtime，不建立跨 Worker 回调或预设插件入口。

持久 Stream 消费将领取和 pending 可见期限登记放在同一 Lua 中，确认时原子清除期限。
消费者自己的独立 Pub/Sub 连接只传递输入与期限变化通知，先确认订阅再读取持久状态，
按最早真实 pending/业务截止唤醒；没有 pending 时不运行固定周期的 PEL 扫描。
通知读取与截止等待分开，健康连接不会因截止到来而重连，也不占用命令池的阻塞连接。
领取者崩溃且没有新输入时，其他消费者仍按已登记期限恢复认领。

固定版本的 `redis_error::code()` 只区分 `command_error`，不精确区分服务端 `NOGROUP`。
共享消费者仅对该类型做一次幂等建组及原操作重试；建组 Lua 按官方 `BUSYGROUP` 前缀处理，
其他错误保留。首次原错误留存，建组或再次操作失败时记录并传播实际错误，
不把 ACL 拒绝或 `WRONGTYPE` 当作成功，也不匹配 C++ 异常的 `what()` 文本。

## 连接状态租约与升级

Collector 的连接身份保存在 `iot:v3:device:connection:<deviceId>`，TTL 为 15 秒。
所属 Worker 每 5 秒只为当前实际存活的连接续期，查询与命令路由读取均不续期。
续期和清理核对实例、Worker incarnation、连接及 session epoch；旧连接的迟到任务不能删除或复活新会话。
连接到期索引使用 Redis 时钟和最早截止调度，实际到期后发布一次状态变化，供现有 SSE 更新页面。

升级时自动回收已认领链路设备的旧 `iot:v2:runtime:device:<deviceId>` 连接身份字段，
最后上报时间、在线超时及最新指标继续保留，历史迁移 SQL 和数据库结构不变。
运行中的全部实例应使用同一版本；旧程序不读取新连接键，回滚后通过真实重连重新建立旧版本路由。
新 Worker、链路诊断快照同样有期限，历史无 TTL 的链路快照仅在确认链路已归属当前实例后回收。

第三方许可证随制品打包：

```text
cmake -DDEPENDENCY_BUILD_DIR=build -DDESTINATION=build/artifact -P ports/cmake/stage-third-party-notices.cmake
```
