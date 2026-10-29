# MQTT 接入与用户配置

平台以 MQTT 3.1.1 Client 连接已有 Broker，复用 Collector 的 TCP 传输、连接认领、配置投影和遥测流水线。连接、协议状态和计时器归认领连接的 Collector Worker；协议解析不执行数据库或网络 I/O。

## 配置关系

| 配置位置 | 职责 |
| --- | --- |
| 链路管理 | Broker 地址、端口、Client ID、用户名、密码、保活间隔 |
| MQTT 配置 | 可复用的设备类型：消息属性、固有属性、设备标识符、上报和控制消息 |
| 设备管理 | 绑定链路、Broker 目标和设备类型，填写每台设备的固有属性值 |

链路选择 `MQTT`、`TCP Client`，地址支持 IPv4、IPv6 和域名。每条同时工作的连接须使用 Broker 内唯一的 Client ID，保活默认 60 秒。MQTT 只在平台 Collector 执行，不增加 EdgeNode 固件协议。

## 用户定义属性

新建设备类型不预置属性、设备标识字段、Topic 或模板字段。先按设备实际协议定义内容，再配置引用。

- **消息属性**：上报的动态值或本次下发的控制值，使用稳定 UUID，支持 `DOUBLE`、`BOOL`、`STRING`，可配置单位、枚举和显示状态。
- **固有属性**：每台设备的固定值，例如序列号、地址或通道。名称和类型由用户定义，支持 `STRING`、`DOUBLE`、`BOOL`、`JSON`；具体值在设备管理中填写，定义不保存设备值。
- **设备标识符**：从用户定义的文本固有属性中明确选择一项。每台设备必须填写非空文本值，同一链路、同一 Broker 目标内唯一。平台管理编码和 UUID 不参与 MQTT 报文标识替换。

固有属性继续使用现有 API 字段 `config.parameters` 保存定义，设备的 `parameters` 对象保存值；设备类型的 `identityProperty` 保存所选标识属性名称。例如：

```json
{
  "identityProperty": "hardwareSerial",
  "parameters": [
    {"name": "hardwareSerial", "dataType": "STRING"},
    {"name": "stationAddress", "dataType": "STRING"}
  ]
}
```

设备填写 `{"hardwareSerial":"SN001","stationAddress":"A001"}`。设备表单标明所选设备标识符并要求填写。固有属性不作为测量点展示；修改模板不会自动创建、改名或删除属性。

## 上报消息

每种设备类型配置 1–32 条具名上报消息，每条独立设置 Topic、JSON 模板、QoS、数据时间和时间格式。

模板使用完整 JSON 字符串值引用用户定义的属性：

- `"$point:属性名"` 将该字段映射到消息属性 UUID，保存时编译为 JSON Pointer。
- `"$param:固有属性名"` 引用固有属性。所选设备标识属性标记负载中的设备标识字段；其他固有属性不会被上报覆盖。
- `"$time"` 标记设备数据时间，字段名称由用户定义，保存时自动提取 JSON Pointer。
- Topic 使用 `{param:固有属性名}` 替换当前设备的固定值。占位符须独占一层，替换值须为非空标量且不得包含 `/`、`+`、`#` 或 NUL。

例如 Topic `devices/{param:hardwareSerial}/telemetry`，模板由用户按实际字段命名：

```json
{
  "serial_number": "$param:hardwareSerial",
  "sample_clock": "$time",
  "measurements": {"temperature": "$point:温度"}
}
```

表单的“数据时间”选择 `$time` 时，模板必须且只能包含一个完整的 `"$time"`，时间格式支持 `unix_ms`、`unix_s`、`iso8601`；选择“以上报时间为准”时使用平台接收时间，模板不能包含 `$time`。保存时前后端均校验选择与模板是否一致，无需填写时间字段路径。设备时间字段缺失时使用接收时间，显式提供无效时间则跳过该记录。无时区的 ISO 时间按设备时区解释，存储为 UTC。

共享或通配 Topic 须从负载或 Topic 明确识别设备。未知标识符不自动创建设备，不会回退到平台管理编码。相同设备类型的上报 Topic 禁止重复或交叉匹配，避免重复解析。

各消息的 `pointFields` 只引用属性 UUID，同一属性可在不同消息中映射到不同路径。一次上报只更新该消息绑定的属性，其他属性不补零、不覆盖。设备卡片只展示上报消息引用的原生属性并去重；控制专用属性和固有属性不占位。

JSON 模板支持嵌套对象和固定数组位置，最多 256 个消息属性、32 层、16384 UTF-8 字节。占位符只替换完整字符串值，不替换键名，不支持拼接或表达式。重复键、未知名称及重复上报属性引用会被拒绝。未标记的普通字段不采集。

“上报解析测试”调用服务端同一解码函数，只使用本次输入，不保存、不连接 Broker、不下发消息。修改配置或测试输入会作废旧结果。

## 控制消息与设备回执

每种设备类型最多 32 条具名控制消息，每条配置自己的 Topic、JSON 模板和必填属性。下发时选择一种消息、填写对应属性值，一次生成一条 MQTT 消息和一个命令记录。

例如用户自行定义消息属性“请求编号”和“设定值”，以及固有属性 `hardwareSerial`：

```json
{
  "request_number": "$point:请求编号",
  "serial_number": "$param:hardwareSerial",
  "operation": "set",
  "value": "$point:设定值"
}
```

请求编号由调用者提供。平台不向控制模板注入设备 UUID、设备编码、自动指令编号或时间变量。`$deviceId`、`$deviceCode`、`$commandId`、`$values` 均不是合法模板引用；`$time` 仅用于上报模板。

被控制消息引用的消息属性自动标记可写。控制 Topic 还可使用 `{point:属性名}` 引用本次控制值；Topic 引用和数组直接引用的属性必须设为必填。选填属性留空时省略对应 JSON 字段，`0`、`false` 保留原类型，不补零或 null。枚举下发执行反向映射，数值按原始值发送；倍率、偏移和条件换算使用独立的派生值表达式，派生属性只读。

设备回执是每条控制消息的可选规则，字段名和路径均按设备实际协议填写。例如：

```json
{
  "topic": "devices/{param:hardwareSerial}/result",
  "matchFields": ["/request_number", "/serial_number", "/operation"],
  "codeField": "/result/code",
  "successCode": 0,
  "timeoutMs": 10000
}
```

`matchFields` 与实际发送 JSON 的同路径标量逐项比较，须包含用户定义的必填控制属性路径。调用者应为每次操作提供唯一关联值，避免迟到回执与后续操作混淆；同一设备、相同回执 Topic 上不能区分的在途指令会被拒绝。回执 Topic 不得含通配符或本次控制属性。

配置回执后，Broker 确认只表示投递，结果码等于 `successCode` 才确认设备成功。其他整数码为失败，无关、重复和 retained 回执不完成新指令。超时或断线为结果未确认（`UNKNOWN`），不自动重发。回执超时默认 10000 毫秒，范围 1000–120000；原始回执说明最多保留 4096 字节。未配置回执时保留 Broker 确认语义。

设备仍须允许远控，调用者仍须具有业务权限。使用 `commands` 时，下发请求必须提供所选 `mqttMessageId`；幂等比较包含消息 ID。删除全部控制消息后保存会清空旧模板与旧 Topic。

## 底层负载与运行范围

页面提供 JSON 模板编辑；既有 API 的单条消息仍支持 JSON 批量记录、分隔文本和定长二进制，均使用同一解码入口。无法无损表示的配置会提示不能使用页面编辑，不自动覆盖。

- JSON 的 `recordsPath` 先选择对象或数组，字段相对每条记录；裸字段名表示顶层键，`/` 开头使用 JSON Pointer。
- 文本使用字面值 `delimiter`、`recordDelimiter`，字段填写从 0 开始的列号，不解释 CSV 引号或转义。
- 二进制字段为 `偏移:长度:编码[:字节序]`，支持 `UINT`、`INT`（1/2/4 字节）、`FLOAT`（4/8 字节）、`UTF8`、`HEX`，默认 `BE`，可选 `LE`；正 `recordLength` 按定长切分。
- 普通 TCP、MQTT 3.1.1、Clean Session；当前不提供 TLS、MQTT 5、持久会话或离线订阅恢复。断线使用 TCP Client 的有界退避，重连后重新订阅；配置变更重建连接。
- 上行 QoS 0/1/2，下行 QoS 1/2，控制消息不使用 retain。PUBACK/PUBCOMP 等待有效遥测进入持久 Redis 流水线，后续消费者写数据库；Clean Session 重连不恢复内存 QoS 2 状态。
- 无匹配设备、无有效属性或无效业务负载跳过并完成 MQTT 握手；非法协议帧关闭连接。单帧上限 1 MiB、单消息最多 256 条记录、在途最多 256、待处理接收帧最多 16 MiB。

固有属性最多 256 项、256 KiB，名称为 1–100 UTF-8 字节且无首尾空格。设备 PATCH 省略 `parameters` 保持原值，`{}` 清空；显式 null、数组或不匹配的类型会被拒绝，设备标识符不得清空。其他协议继续复用固有属性的原有字段契约。

## 旧配置迁移与验证

新增迁移 `0064_mqtt_intrinsic_properties`，历史迁移 ID 和 SQL 保持不变。迁移将旧设备编码和实际使用的设备 UUID 转为设备类型内的固有属性及设备值，改写相应模板和 Topic 引用；原时间路径保留，旧时间占位改为普通示例值；`$values` 展开为显式属性映射，数组中的控制属性按模板规则完整提供。新增后超过 256 项属性上限时，迁移失败并回滚，须先调整旧配置再升级。

页面打开未声明 `timeSource` 的旧配置时，根据原时间路径恢复 `$time` 标记并选中设备时间，其他模板原文保持不变；没有时间路径的配置选中“以上报时间为准”。保存后记录所选 `timeSource`，时间路径由模板自动生成。与现有属性映射冲突的旧路径仍会拒绝编辑，避免丢失映射。

使用旧自动指令编号的模板迁移为用户定义的“请求编号”控制属性，并加入对应消息的必填项。**调用方需读取新的操作属性并提供关联值**，原自动编号行为不再保留。属性名称冲突时迁移使用不重复的名称，不覆盖既有值和 UUID。数据库约束按所选固有属性检查非空与同 Broker 标识唯一性，切换标识属性时重新校验已绑定设备。

相关验证包括 `mqtt-protocol`、`mqtt-transport`、`mqtt-configuration`、`tests/mqtt-schema-test.ts`、`tests/mqtt-integration.ts` 和 `tests/mqtt-intrinsic-migration-integration.ts`；构建及 CTest 集成测试运行方式见 [build.md](build.md)。

平台向外部 Broker 推送事件的独立功能见 [open-access-mqtt.md](open-access-mqtt.md)。
