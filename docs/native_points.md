# 原生点位与派生值

所有协议共用 `derivedPoints`。原生点位负责按协议地址、类型、字节序或 JSON 路径解码；业务倍率、偏移和条件换算使用派生值表达式，例如 `x * 0.1 - 1`。原生点位配置不接受 `scale`、`offset`。协议地址偏移、BCD 小数位和已有数值精度设置保持各自语义。

MQTT 页面与 Modbus、S7、SL651、MC、FINS、DLT645 页面提供同一派生值编辑器。派生值采用独立 UUID，仅支持读取；下发原生点位时使用设备原始值。MQTT 枚举仍支持双向映射，不为派生值自动推导控制公式。

## 设备模型投影与执行边界

`features/configuration` 是设备参数、协议缺省值、有序原生点位、SL651 函数和派生点的唯一投影入口；Collector 运行快照、完整实时点位读模型和 Edge 配置均消费该投影。Edge 只把已有值转换为既有 Protobuf 消息，不再独立展开数据库 JSON。原始配置 JSON 保持原样，查询配置不会写回或补齐默认字段。

以下是原 Collector/Edge 差异的逐项取舍。缺省指字段缺失或 `null`；文本空串不被一概视作缺省。整数空串/无效整数使用对应字段的已有缺省值；合法整数数字 token 的 `1000.0` / `1e3` 也按整数读取，不向上取整。协议管理 API 的必填和范围限制不变，部分缺省只对历史或直接导入数据可达。

|项目|原差异|统一语义与可复核依据|
|---|---|---|
|Modbus `modbus_mode`|Collector 缺省空串，Edge `TCP`|缺失取 `TCP`；`modbus/modbus.protocol.h` 的 TCP/RTU 分支，以及 `config/current_device_model.h` 的边缘链路绑定触发器；串口仍由触发器写入 `RTU`，显式值不覆盖。|
|Modbus `packet.mergeGap`|100 / 0|100；`web/pages/protocol/protocol.service.ts` 的默认包配置和 Collector `DeviceDefinition` 一致。`maxQuantity` 保持 125。|
|Modbus `quantity`|0 / 1|1；`modbus/modbus.protocol.h` 读组和解码原本均将 0 钳制为 1，而控制写入拒绝 0。管理 API 仍要求显式 1–4；显式历史 0 不会被冒充为缺失。|
|Modbus 字节序|两者均有继承|保留点位 `byteOrder` → 模型 `byteOrder` → `BIG_ENDIAN`，不读取已退役倍率。|
|S7 缺失 `dataType`|`UINT8` / `BOOL`|`UINT8`；`s7/s7.protocol.h` 的 BYTE/UINT8 解码读取原始首字节，避免把原始字节降为布尔；API 枚举允许 UINT8，不包含 BYTES。显式 BOOL 不变。|
|S7 缺失 `size`|0 / 1|1；`s7/s7.protocol.h` 的请求范围本就把 size 钳制至少 1；管理 API 对新点位仍要求 size。|
|S7 TSAP|`0100/0101` / 空串|`0100/0101`；`s7/s7.protocol.h` 的 `localTsap` / `remoteTsap` 在 TSAP 解码失败时也使用这两个值。显式机型 TSAP、rack/slot、连接类型不覆盖。|
|S7 握手/探测参数|仅 Collector 有字段|共享投影保留 connection → 顶层 → `5000/5000/STANDARD`；现有 `edge.proto` 无对应字段，不扩展固件协议。|
|MC 缺失字节序|`LITTLE_ENDIAN` / `BIG_ENDIAN`|`LITTLE_ENDIAN`；MC 页面新增点位和 Collector MC 寄存器解码的既有配置一致。FINS 保持 `BIG_ENDIAN`。|
|MC/FINS 缺失 `dataType`|空串 / `UINT16`|`UINT16`；两者原生寄存器解码均支持 16 位字，Edge 既有缺省如此；管理 API 仍要求显式类型。|
|DLT645 缺失类型|空串 / 无关的 `UINT16`|`BCD`；页面创建默认 BCD，`dlt645/dlt645.protocol.h` 原本只有 HEX 单独分支，其余均按 BCD 解码。显式 HEX 与其字节序不变。|
|DLT645 缺失 `length/digits`|`0/0` / `4/2`|`4/2`；页面默认值与 Edge 一致；DLT645 解码拒绝非正 length，并按 digits 插入 BCD 小数点。新配置仍须显式 length。|
|DLT645 命令 BCD 编码约束|旧 Edge SQL 把类型复制到 encoding；Collector 曾延至实际编码才拒绝超容量值|唯一模型显式保留 BCD/BCD_SIGNED encoding，准备和实际 FrameCodec 共同调用无分配的 `collector.protocol.h::command::encodeDlt645Bcd`；1 字节无符号 BCD 接受 99、拒绝 100，保留 digits=8 与 BCD_SIGNED 最高符号位语义，不套用 SL651 的 digits<=7/独占符号字节规则。HTTP 同时验证两目标的 100 拒绝；99 在 Edge 真正入队，在本测试故意离线的 Collector 中通过编码校验后返回既有路由 409，实际编码字节另由 FrameCodec 单测验证。|
|工业连接参数|相同值，解析路径重复|保持 MC `3E/0/255/1023/0/16`、FINS 全零路由、DLT645 `2007/4` 和空密码/操作员码；只由共享投影解码，显式值原样转换。|
|数值精度与可写标志|两条投影重复解析|普通协议 decimals 缺省 `-1`；SL651/DLT645 仍使用协议 `digits`，不改 S7 已有原始值解码差异。旧导入数据的 writable `true/t/1` 文本兼容保留；HTTP 仍只接受布尔。|
|SL651 函数|Collector 仅在要素上保留 code/dir，Edge 单独带 name|共享模型同时保留函数 code/name/dir（包括空函数），并给要素保留所属函数；响应要素使用 `response:` 键，只有 DOWN 普通要素可写。`sl651/sl651.protocol.h` 的响应选择和完整函数控制依赖这些信息。|
|SL651 位置与编码|相同值，排序后分发|保留 `GUIDE/0`、length/digits `0/0`、guide/encoding 和 response 标志；OFFSET 从包含流水号/时间的重组体零基偏移计算，不改 `edge.proto` 位置字段。|
|元素与函数顺序|Collector 地址排序、Edge ID 排序；Redis Set 再按 ID 排序|按配置数组顺序保存；SL651 先按 funcs 顺序，再普通 elements、responseElements 各自顺序。工业轮询和 SL651 解码依次消费向量，完整函数元数据不能丢失；Modbus/S7 自己的采集分组仍按协议地址合并，不把展示顺序当作线协议读取地址。|
|同 pointId 跨 SL651 函数|Edge 保留各函数项，Collector 同名 Hash 覆盖；命令首个 UP 项可能遮挡 DOWN|原生运行定义保留每个 occurrence，现有 element Hash 的字段按全局数组 ordinal 限定；读取只 HMGET 当前 ordinal 的字段，不扫描/复用其他函数。外部 pointId、`element:` / `response:` configKey 和 Redis key 字符串不变。|
|边缘命令准备|另有第三份 JSON 展开：S7 BOOL、SL651 length=1、DLT645 digits=0，可写文本和快读范围另行解释|删除 `command.service.h` 的 `loadEdgeElements`；在既有授权/已应用配置检查之后，复用同一 configuration 投影及快读参数，不让命令验证与实际下发定义分叉。|
|读取/上报与快读|Collector 整数、Edge ceil；快读仅 Edge 钳制|API 本就要求整数读取间隔，缺省 300；共享投影不再 ceil 非法小数。快读统一保留原 Edge duration `[0,3600]`、interval `[1,3600]`，缺省 `60/1`。`io_interval_ms=0` 和已有兼容调度注释不变。|
|在线超时与存储|重复解析；存储仅服务端使用|online_timeout 仍为整数、缺省 300，不做 ceil；storagePolicy 缺省 `report`，保持 `report/change`，不向无该字段的固件消息注入新字段。|
|心跳/注册包|Edge 曾不做 Collector 的模式限制|依据 `modules/device/device.service.h` 校验，仅 TCP Server 且非 SL651 保留包；Edge 注册仍被数据库强制 OFF，Protobuf 已移除的字段 23 继续 reserved，不恢复它。|
|停用/执行目标|Collector 排除停用，Edge 要下发停用状态|这是执行边界而非协议缺省：Collector 只取启用的 collector 设备/链路/模型；Edge 保留本节点停用对象并发送 enabled=false；完整实时点位读模型仍包含所有未删除设备。删除关联对象不再执行。|
|MQTT|无 Edge 对应协议枚举|保持 Collector-only；点位保留 JSON field、unit 空串、writable=false、size=65535 和原数组顺序，不生成 PROTOCOL_UNSPECIFIED 配置。|

### 重复点位的控制与最新值

SL651 采集和响应先按收到帧的 function_code / direction / response 选择 occurrence；同一外部 pointId 可以跨函数出现，不被持久化 Hash 合并。完整函数命令按配置顺序选择首个 **请求 ID 与全部普通 DOWN 要素逐项完整匹配、且不含 JPEG** 的函数，返回值保持请求顺序，UP / response 项不遮挡该函数；缺项、跨不相容函数、重复请求 ID 等仍按既有命令错误处理。不根据值内容猜测另一个函数；同一函数中重复 ID 也不能通过重复请求 ID 绕过原有完整函数检查。

实时点位目录按 pointId 聚合：位置取首次出现，名称/单位按最后配置的可展示 UP 定义覆盖。latest Hash 的既有控制能力视图还包含 DOWN / response，初始化/配置刷新按 funcs 顺序、普通要素后接响应要素顺序逐项刷新，同 ID 最后一个定义的元数据生效；这与仅展示 UP 的点位目录是不同执行边界，不人为混为一份重复显示的点位。原生运行定义和固件配置仍保留全部 occurrence。收到遥测时仍使用既有 latest Lua：较新的 observed_at 覆盖，时间相同时后到帧覆盖，较旧帧不覆盖；被接受帧的值、名称、单位来自该帧选中的函数定义，不新增“按函数”拆分的外部点位。

能力边界：`edge.proto` 的 `CommandRequest` 只有 element_id / expected 值和时限等字段，没有 function_code / 原始命令帧。多个 DOWN 函数具有完全相同 ID 集合时，平台选择配置顺序首个完整函数；Collector 的真实命令编码可验证此规则，但不可达固件对此歧义集合的查找实现 **未验证**。不新增 selector、不拒绝原配置输入，也不宣称已证明该歧义硬件命令端到端选择。


### 配置签名和升级影响

本次对齐允许现有 Edge 配置 SHA 改变并在下次配置下发时重载一次；原因包括默认值统一、设备按 link/id 排列、函数/点位按数组排列，以及元素跟随设备输出。SHA 算法、Protobuf 字段号、确定性序列化和 revision 参与计算的规则不变。相同 revision 和相同投影的重放字节/SHA 必须一致；不同 revision 的 SHA 不能当作纯配置内容哈希比较。

Collector 内部版本化 Redis 快照的 `:devices`、`:elements` 从 Set 改为 List；同一现有 element Hash 的字段增加 occurrence ordinal 前缀（例如 `0:id`、`3:function_code`），Redis key 字符串不变。`ordered-device-model-occurrences-v1` 参与内容签名，确保旧 Set / 无 ordinal Hash 版本绝不被复用；这属于同一次已批准配置重载内的内部格式变更。相同内容后续继续去重。共享 Redis 的所有旧进程停止后统一切换，各新进程必须在启动 Collector 前完成初始化认领与新快照投影；不支持新旧二进制混跑或把 active 指针恢复到旧格式。投影全部写完才切换 active，失败前旧指针保持不动且新 Collector 不启动；已完成的旧版本只按原 grace 窗口清理，不是读取回退来源。旧二进制回退需要恢复同次数据库/Redis 备份或在旧进程接入前重新生成旧格式快照，不能只切换二进制。

证据边界：可复核来源为本仓库协议解码器、Protobuf、接口校验和行为测试；本机 `/root/flexedge/node` 属于网站反代节点而非 IoT 固件，历史 IoT 固件构建主机当前不可达。因此这里不宣称完成了真实固件源码或实机验证；保留现有 `0.3.44` 线协议和兼容路径，不从缺失固件推断新能力。


## 升级迁移

新增迁移 `0054_native_point_calculations`，不修改历史迁移和历史遥测。

- 默认倍率 `1`、偏移 `0` 直接删除，原点位 UUID 不变。
- 实际生效的只读换算转成派生表达式。换算结果沿用原点位 UUID、名称、单位及显示状态，已有告警和派生依赖仍引用该结果。原始采集点分配新 UUID，默认隐藏。原来在倍率之后执行的小数舍入写入派生表达式，原始采集点解除该舍入。
- MQTT 保留原始属性名称，保证上报模板占位符仍匹配；原始属性与派生值分区展示。编辑模板时保留派生定义和显示配置。
- 对可写换算、超过 32 个派生值或关联不支持派生值的边缘固件，迁移报错并回滚。须先将控制协议明确改为原始值、整理派生配置或升级固件，再重试升级，避免静默改变下发或采集语义。
- 数据库约束与管理 API 拒绝重新引入原生点位倍率、偏移。保留 EdgeNode Protobuf 的固定倍率字段，平台下发恒为 `1`，兼容已部署固件。

启动前备份数据库、二进制和静态文件；验证迁移后的原始值、派生值和 UUID 引用。运行迁移由框架记录校验和，重复启动不会再次生成 UUID。迁移 SQL 对已转换配置再次执行也不改变结果；若要回退到升级前的配置，应恢复同次备份数据库，不能只切回旧二进制。

## 验证

`tests/native-points-migration-integration.ts` 在事务临时表中验证 UUID 保留、表达式、小数舍入、MQTT 模板、重复转换、数据库约束及失败保护；`tests/mqtt-integration.ts` 验证原始 MQTT 采集值、派生值入库、原始控制值及设备回执。两者由 `ctest -L integration` 运行（见 `docs/build.md`）。原生解码和协议帧继续由 CTest 覆盖。

`device-model-projection` CTest 验证七协议的缺省/null/显式值、精确整数 token、数组顺序、SL651 空函数与响应、完整实时点位、六协议 Protobuf 字段和同 revision SHA；`device-model-projection-integration` 通过真实 HTTP、PostgreSQL、Redis、SSE 和模拟节点 WebSocket 验证七协议配置投影、停用设备/模型/链路、模型修改和同 revision 断线重放。集成测试会区分接口允许的配置与 SQL 导入的历史缺省形状，不放宽管理 API 校验。运行方式为 `ctest --test-dir build -R device-model-projection --output-on-failure`（需 `docs/build.md` 中的本地测试数据库和 Redis）。
