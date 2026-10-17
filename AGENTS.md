# AGENTS.md

默认中文回复。文档用中文，路径、命令、标识符保留原文。
本文件只记录规范、边界与关键设计约束，覆盖后端、前端、客户端、测试与交付；用户最新明确要求优先。

## 工程原则

- 修改前检查 `git status`，保留用户及并行任务的改动，不夹带无关修改。
- 正确性优先，其次长期可维护性。围绕调用主链、不变量、失败路径和资源回收完整实现，采用最小、清晰的方案。
- 只为现有调用方和明确需求设计，不预设扩展点、配置项或可插拔接口；两个真实用法共享的逻辑才抽象。
- 同类能力统一入口、主链和生命周期，共同逻辑只有一份权威实现；不以复制、薄包装或转发掩盖重复。
- 重构保持 API、协议、权限码、DTO、表名、历史迁移、产品及安装身份。必须改变外部契约时，单独定义并验证迁移方案。
- 移动、改名、删除时一次迁移全部调用、装配、构建、检查、测试与文档；不留旧实现、别名、转发头、兼容旁路、空目录或占位文件。删除前确认无实际用途。
- 权限、归属或消息边界不明时 fail closed；真实异常必须上报，不静默吞掉。
- 按实际行为验收，不以命名、目录或编译通过代替。

## 命名与风格

- 名称表达对象、职责和动作，不重复上下文；避免含糊缩写及 `handle`、`process`、`doWork` 等泛称，`Manager`、`Helper`、`Processor`、`Data` 不得混合无关职责。行业术语、框架约定及第三方源码保持原样。
- 目录用小写 `snake_case`，文件用职责前缀（`device.service.h`、`device.api.ts`）；禁用 `service.h`、`runtime.h` 等裸名、混合大小写和连字符。
- C++ 以 clang-format 21 与根 `.clang-format` 格式化：类型 `PascalCase`，函数与变量 `camelCase`，常量 `kPascalCase`，数据成员尾下划线，命名空间 `service::<component>`。前端由 Biome 格式化与检查。
- include 使用仓库根相对路径 `service/...`，全部位于文件头。一个文件只承载一个职责单元，不在文件中段追加另一组 include 与实现。
- 优先类内方法，仅保留必要的 `inline`，模板不重复标注。

## 分层与目录

技术栈：后端 C++23 与 Ruvia，前端 React 与 Bun，数据库 PostgreSQL/TimescaleDB，消息 Redis。

```text
service/
├── modules/<module>/        # 管理 API 与业务操作；system/<module>/ 为系统管理
├── features/<feature>/      # 后台任务、设备协议、消息消费
│   └── <component>/         # 独立协议、传输或运行职责的子组件，最多一级
├── common/                  # 公共契约与基础定义，无子目录
├── config/                  # 进程配置与数据库迁移
├── middleware/              # 认证、权限、日志、请求校验
├── utils/                   # 无业务归属的无状态工具
└── server.cpp               # 装配、注册、生命周期
web/
├── pages/<module>/          # 页面业务模块
├── components/ layouts/ routes/ providers/ store/ hooks/
└── config/ types/ lib/ utils/ styles/ public/
clients/<platform>/  ports/  tests/  .github/workflows/
build/                       # 构建、后端生成代码、临时验证产物
```

- 按实际业务归属划分，同一业务的页面和接口归同一模块；无菜单功能归所属业务。不按 URL 分层，不建空的前后端镜像目录或无独立职责的分组。
- 私有实现留在模块，确有跨模块复用再提取。共享目录不收纳页面业务或归属未定的代码，不增加平行业务层。
- `common/` 只含契约、格式、键名与无 I/O 编解码，不含运行对象、业务校验或业务实现。`middleware/` 只做请求接入的横切逻辑，不持有运行时、业务存储或跨层调用。`utils/` 不引用业务代码。
- 后台子组件须有独立协议、传输或运行职责，不为缩短文件拆分或继续嵌套。
- 源码、依赖、生成产物分离；生成文件遵循生成器配置，不手工修改。临时脚本、日志、测试数据不放入业务目录。

### 文件职责

仅创建实际需要的文件，同一职责集中在一个文件；`<name>` 为所属模块或组件名。

|文件|范围|职责|
|---|---|---|
|`<name>.entity.h`|modules、features|实体与存储映射；有数据访问时必需|
|`<name>.service.h`|modules、features|业务规则、业务权限、查询、写入、事务|
|`<name>.types.h`|modules、features|DTO、请求模型及字段校验、领域值、内部状态；无 I/O|
|`<name>.error.h`|modules、features|稳定错误|
|`<name>.controller.h`|modules|管理 API 的路由、参数、中间件与响应|
|`<name>.runtime.h`|features|启停、调度、消费、生命周期|
|`<name>.config.h`|features|配置类型、解析、默认值；无业务数据库访问|
|`<name>.protocol.h`|features|编解码、帧解析、协议状态；无数据库或网络 I/O|
|`<name>.transport.h`|features|网络、设备协议接入端点、SDK 适配；无业务数据库访问|
|`<name>.proto`|features|Protobuf 消息|
|`index.tsx`|web/pages|页面入口与私有 UI|
|`<name>.types.ts` / `.schema.ts`|web/pages|类型与 DTO / 请求与表单校验|
|`<name>.api.ts`|web/pages|HTTP 客户端、请求地址与 SSE 接入|
|`<name>.service.ts`|web/pages|业务操作、查询、服务端缓存|

- 后台 `runtime`、`protocol`、`transport` 可使用同名 `.cpp`，其余用头文件实现。
- 禁止白名单外文件及变体，包括 `repository`、`queries`、`helper`、私有 `common`、多个 `service` 变体；模块及子组件不建私有子目录。

### 依赖边界

|调用方|允许依赖|
|---|---|
|页面|所属 `service`、`types`、`schema`，公共 UI 与状态接口|
|`controller`|所属 `service`、`types`，`middleware`|
|`runtime`|所属 `service`、`protocol`、`transport`、`config`|
|`service`|所属实体与类型、同层公开接口、公共契约与工具|
|`protocol`、`types`、`entity`|无 I/O 类型与工具|
|`transport`|协议类型、网络、SDK|
|`server.cpp`|公开装配入口|

- 依赖无环，不访问其他组件内部状态；公共代码不反向依赖业务或页面。
- Controller 负责管理请求接入，Transport 负责设备协议接入，Runtime 驱动连接与任务，业务与数据库操作归 Service。接入端点使用 Ruvia 唯一路由注册入口，不复制协议判断；Transport 不反向依赖 Service 或 Runtime。Service 不依赖 Controller 或 Runtime，不接管生命周期。
- `modules` 与 `features` 互不引用或调用，只通过 Redis 消息或数据库交互；不借共享层、回调、全局对象或装配入口绕过。
- 双方共享的消息格式、Redis 键名与 Stream、RPC 请求与回执、advisory lock ID 及版本只在 `common/message.h` 定义一次，调用方不重复字面量。
- 跨页面只依赖其他模块公开的 `service`、`types`。会话状态只负责读写，认证刷新归所属 Service，在应用入口注入请求层。

## 数据访问与迁移

实体：

- 访问数据的模块或组件有且仅有一个 `<name>.entity.h`；无数据访问不创建。数据访问包括只读、联表、事务、投影、Redis 业务记录与缓存，原生 SQL 或 Redis 命令同样适用。
- PostgreSQL/TimescaleDB、Redis ORM 及持久化映射集中在所属实体文件，准确覆盖对象、类型、主键、空值、默认值，并实际用于查询、映射或写入；仅用字符串表名、列名的 `db_query` 不算完成实体迁移。
- 同层同一张表由所属组件映射，其他组件复用其公开实体，不复制映射，也不能借此寄放自身映射。`modules` 与 `features` 各自维护映射，不共享 ORM。
- 区分实体、DTO、协议与消息；队列、锁、通知不伪装成表。ORM 无法表达时在实体文件定义实际映射，记录具体缺口、调用及保留原因。

查询与事务：

- 使用固定版本 Ruvia 公开 ORM、Query、Schema API，不加转发包装、兼容接口或未经验证的替代方案。
- SQL 输入参数化，保持事务、锁顺序与幂等语义；Redis 保持原子性、过期、重试与确认语义。必要持久化完成后才确认，缓存不能掩盖数据库失败。

迁移与字段：

- 迁移统一放在 `service/config`。未经验证不改已执行 ID、校验和或历史 SQL；迁移重构须验证新库、升级、重复执行、漂移拒绝与失败回滚。
- 枚举按文本读写，物理类型与历史默认值归迁移；`db_column_options` 的 `enum_name_`、`default_expression_` 准确描述现状，补充元数据不修改历史迁移或实际列类型。
- DTO、实体、校验、数据库约束一致，明确空值、可选、默认与清空语义。
- Ruvia 字段必填与可空独立：`RUVIA_OPTIONAL_FIELD` 仅允许缺失，允许 null 须显式 `RUVIA_NULLABLE`；`RUVIA_DEFAULT` 只作用于缺失的可选字段，显式 null 不应用默认值。PATCH 用 `is_present`、`is_null` 区分不修改与清空，不重新解析原文。此前明确接受 null 按缺失处理的字段须显式标记可空并保留测试。
- `json_value`、`json_object` 不附加字段校验，动态内容的业务约束归 Service；`from_json` 只解析结构，不替代路由校验。
- 请求 Model 能直接供 Service 使用时，不保留字段相同的输入 struct 和中间转换。
- 数据库及连接使用 UTC，时间用 `TIMESTAMPTZ`；设备时间按其 `timezone` 入库，API 输出 UTC。

## HTTP、SSE 与 WebSocket

|场景|方式|
|---|---|
|查询 / 创建与执行 / 更新 / 删除|GET / POST / PUT、PATCH / DELETE|
|登录、认证刷新、上传|各自明确的 HTTP 接口|
|设备、运行状态、告警等实时变化|SSE|
|串口、终端、固件分块的双向流|专用 WebSocket|

接口：

- 不设统一业务 WS，不把 GET 改为 SSE。用户操作直接返回 HTTP 结果，不等待 SSE 回执；配置、表单详情、选项、历史记录及用户资料走 HTTP。
- Controller 使用 Ruvia 公开路由 API，接收类型化参数并调用所属 Service。字段校验直接绑定 `types.h` 中的请求模型，不建模块级 `schema.h` 或独立 Validator；创建与更新约束不同时分别建模，跨字段与数据库规则归 Service。
- 普通 handler 返回 `ruvia::task<ruvia::http_response>`，通过 `c.json(model)` 在模型所属内存作用域内显式序列化；Service 返回 `task<Model>` 或 `task<void>`，不保留默认返回类型别名。响应明确状态码与业务结果。
- 固定结构的请求、配置、消息与状态使用所属 `types.h` 或公共消息契约中的 Model，多协议配置分别建模，原文入库用 `validated_json<T>::raw()`。只有运行时键名、任意 JSON 值或未知字段保留需求才用 `json_value`、`json_object`；动态视图借用的输入须覆盖全部使用期，不传临时字符串、不逃逸。
- 升级 Ruvia 时全项目迁移公开 API；不引用框架 `detail`、不恢复旧 API、不加兼容包装。公开 API 无法表达的需求，先列明契约与能力缺口再定方案。

权限：

- 自有管理 API 在路由声明绑定 `service::middleware::PermissionMiddleware<"权限码">`，顺序为认证、权限、模型校验、业务调用；handler 不调用 `requirePermission`，不保留新旧两套接入。
- 接口权限不替代 Service 的业务权限：资源归属、租户与数据范围、共享授权、状态限制仍由 Service 校验；多权限、条件权限保持原逻辑，不合并成更宽松的单权限。
- SSE、WebSocket 建连时校验权限，连接期间复核事件查询与命令执行权限并处理撤销。第三方接口、固件和专用令牌保持各自授权契约。
- 权限改动逐路由核对权限码、允许与拒绝条件、校验顺序与响应，验证未登录、无权限、合法权限、撤销与数据越权。

实时推送：

- 只由真实业务事件触发：写入提交成功、设备数据或状态变化、权限变化。事件携带受影响实体标识，订阅方只查询、推送受影响数据；无变化不发事件，不重复快照。
- 首次订阅发送一次初始快照；断线恢复补发遗漏事件或发送一次恢复快照。高频事件合并同批变化；连续数据保证顺序与背压，不静默丢失必须交付的数据。
- 新增订阅须明确实时展示需求与事件来源，只订阅当前视图需要的数据；调试、日志、进度视图关闭后取消对应订阅。权限变化可随已有连接推送，服务端始终独立校验。
- 每个页面最多一条 SSE，页面内组件、弹窗、抽屉共享连接，按事件类型分发并只更新对应 TanStack Query 缓存；指标变化只更新指标，配置或分组变化才更新相关列表。无实时需求的页面及公共布局不建 SSE，最后一个实时视图关闭即释放连接。
- 订阅范围随查询条件变化时取消旧订阅、按新条件建立。只在连接真正结束或失败后有界退避重连，正常连接不设空闲重连定时器；保活只发 SSE 注释心跳，不携带业务数据、不触发业务查询或缓存更新。
- SSE 断线时立即显示连接状态并允许手动重试；重连状态不等同业务失败，不丢弃已接受操作的跟踪身份。不降级为轮询；取消订阅、离开页面、退出登录、断线时清理所属 Worker 的资源。

禁止轮询：

- 前端禁止 `refetchInterval`、`setInterval`、递归 `setTimeout`、定时重订阅、定时缓存失效或重挂载等周期性业务查询。页面进入、筛选、分页、手动刷新及写入成功后的按需刷新不属于轮询。
- 服务端不为生成事件定时查询全量或增量数据后比较差异。到期与清理任务按最近截止时间唤醒并认领，不固定周期扫描，也不在每个 Worker 重复执行。

兼容：

- 通信规则仅约束自有管理界面与客户端；第三方接口保持现有方法、地址、响应与订阅协议，已部署 EdgeNode 固件保持协议兼容。

## Worker 与生命周期

- 只有 Service Worker 与 Collector Worker 两类，同类装配、注册、职责、流程一致，仅编号与实际持有的连接不同；禁止 `workers.front()`、固定下标或 Worker 0 特殊职责。
- Service Worker 处理接口与业务；Collector Worker 持有 GB28181 SIP 接入、注册、心跳、会话与控制，两类经 Redis 指令与回执交互。无全局 SIP 运行时，不绕过 Ruvia 公开接口。
- 各 Worker 独占组件、数据库与 Redis 连接、网络连接、缓存、会话、订阅、定时器与任务状态。连接从接入到关闭，解析、执行、SDK 调用及响应均归原 Worker。
- 禁止共享可变对象、跨 Worker 调用、回调、内存转发或连接转交；不按设备 ID、哈希、分片、轮询改派，也不借 Redis 或数据库转交同类 Worker 的工作。
- RPC 回执、终端与串口帧使用所属操作或连接的专属频道，不广播到其他 Worker；业务变化频道只承载实体变更通知。Pub/Sub 先确认订阅再请求或查询快照，通知不替代持久结果、序号与确认，断线后从持久状态恢复。
- 无连接任务用相同机制独立认领，由认领者完成，以事务和幂等避免重复执行，不按编号或哈希分工。恢复与清理按实际归属执行。
- 独立 Collector 用公开 `redis_client(event_loop, redis_config)`，每个 Worker 独占客户端；停止业务任务后在所属 `event_loop` 等待 `shutdown()` 再关闭线程，不用私有 `redis_registry` 或新增 Redis 转发包装。
- 已启动协程取消后必须 await/join；运行状态由所属 Runtime 持有，公共工具只接收显式状态，不持有共享运行对象。进程身份在装配时生成并注入，各 Worker 的 UUID 状态独立持有。
- ZLMediaKit 可有独立运行时与 SDK 线程，但业务状态及异步结果归发起 Worker。其他 SDK 或全局资源的隔离障碍须记录并解决。
- 不保留新旧并行实现或旧路由、分片、兼容队列；保留必要的固件兼容路径。覆盖重启、缩容、异常退出、幂等与过期清理。

## 前端

|用途|工具|
|---|---|
|服务端状态 / 客户端状态 / 校验|TanStack Query / Zustand / Zod|
|虚拟列表 / 排序|`@tanstack/react-virtual` / `@dnd-kit`|

- 锁定 Bun，安装用 `bun install --frozen-lockfile`；升级依赖用最新稳定版，删除前检查源码、构建、运行用途。
- 页面通过 Service 获取数据，请求方法与地址集中在 `<name>.api.ts`；前端模块不增加其他私有文件或子目录。
- selector、Hook 默认值、effect 依赖保持引用稳定；路由提供友好错误兜底。
- 左侧树负责导航或筛选，右侧平面表格；表头、分页固定，表格内部滚动，验证窄屏与横向滚动。
- 表单用 `FormModal`：默认宽 720px、高 `min(720px, 90dvh)`，标题与操作区固定，内容独立滚动，标签纵排。
- 复用组件与工具类；指标统一 CSS Grid；虚拟列表切换数据后重新测量并恢复滚动位置。
- `web/styles/index.css` 只含框架导入、层声明、主题变量；主题用 Ant Design 或 Tailwind 配置，不加组件覆盖样式。

## 客户端与固件

Windows 客户端：

- UI 经控制器与注入接口调用连接能力，只依赖 IPC，不导入服务或 WireGuard 运行对象。连接 Service 管理会话、状态、撤销恢复与重试；状态存储负责持久化与加密；WireGuard 适配负责隧道与密钥；状态文件与隧道生命周期归服务。各组件不访问其他组件内部状态。
- 平台适配使用 HTTP/JSON 与 SSE，禁止业务轮询。共享代码限实际复用的产品与 IPC 契约、文本、文件、加密基础操作。
- 移动实现同步更新原生服务及 WinUI 构建，保持产品、服务、IPC、凭据及安装身份。
- VPN 断开、注销、删除、撤销后回收地址；重连优先复用空闲地址，已撤销 Peer 不占地址池。释放失败保留重试身份，迟到请求不影响新连接，复用前在 Hub 移除旧 Peer。

EdgeNode 固件：构建前遵守固件仓库的 `AGENTS.md`。

- 协议、API、配置、任务与升级兼容已部署固件，保留 `0.3.44` 路径；移除兼容路径须有测试通过的迁移方案、兼容窗口及用户明确批准。
- 支持分块的固件用 WS，限制块大小并支持断点续传；旧固件保留带令牌的直接下载。协议兼容不豁免结构和依赖规则。

## 验证与交付

|改动|必须验证|
|---|---|
|文档|`git diff --check`|
|前端|类型检查、lint、生产构建、修改文件的 Biome 格式检查|
|后端|Release 构建、CTest|
|布局|实际页面、窄屏、滚动|
|实时通信|真实浏览器网络记录及服务端查询与事件证据：活跃 SSE 不超过一条，空闲无业务查询与重复推送，单项变更只更新相关缓存，断线不轮询，恢复后数据完整|
|CI|工作流、缓存、制品路径|

- 本地只用根 `build/`，CI 可用 `build/ci/`。如实说明未覆盖的检查；验收同时检查实体遗漏、无用声明、职责混放、跨层引用、依赖环、生成产物及无用文件。
- 测试验证真实行为与 HTTP/SSE 契约，不以源码文本匹配代替行为测试。
- 工具链以 `.github/workflows/build.yml` 为准，缓存区分平台、架构、编译器与依赖输入。CI 仅支持 Linux、Windows；发布须两平台构建与测试通过、制品同一提交，单平台例外须用户明确接受并记录未覆盖平台。
- Ruvia 固定完整提交 SHA，升级统一迁移公开 API，不加过渡宏、包装接口或兼容头。第三方依赖统一由根 `vcpkg.json` 清单模式获取，跟踪最新 vcpkg 端口，Linux、Windows 与本地共用同一清单，必要调整放在 `ports/vcpkg/` overlay；vcpkg 未收录的 Ruvia、ZLMediaKit 等由 CMake FetchContent 固定完整提交或归档哈希。
- Windows 客户端依赖、构建、测试、安装包统一经 CMake；`clients/` 不留 `.ps1` 构建脚本，保留依赖锁定、校验及打包验证。
- 可执行文件与同级 `web/` 同版本交付，静态资源与固件由后端从 `web/` 托管。凭据不写入提交、制品、日志或文档。
