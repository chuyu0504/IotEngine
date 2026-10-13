# 开放接入 MQTT 推送

在「开放接入 → MQTT」新建推送配置，绑定一个调用配置，并填写 Broker、认证和事件类型。设备范围继承 AccessKey；数据、图片需要 `device:realtime`，告警需要 `alert:read`，控制状态需要 `device:command`。禁用、删除、过期或收窄授权后，后续投递按最新权限过滤。

此功能是平台向外部 Broker 发布事件。设备采集侧 MQTT 另见 [mqtt.md](mqtt.md)。控制请求仍使用下述 HTTP 接口，执行状态可经 MQTT 推送。

## 连接与确认

- Broker：`mqtt://host:1883` 或 `mqtts://host:8883`，支持 IPv6。地址不包含用户名、密码、路径、查询参数。
- 用户名和密码单独配置；查询接口不返回密码。编辑时省略 `password` 保留原值，传 `null` 清除。密码不会进入投递日志。
- MQTT 3.1.1，QoS 1，`retain=false`。每次投递建立独立 Clean Session，使用唯一 Client ID；完成后断开。
- 单个 MQTT 报文上限为 1 MiB；超限会记录失败，需缩小上游负载或取消对应事件后恢复队列处理。
- TLS 使用系统信任根并验证主机名。投递超时为 1–30 秒，覆盖解析、连接、TLS 和协议确认。
- 收到对应 `PUBACK` 后完成投递。连接、认证、超时或确认失败记入日志，消息保留在持久队列中重试；停机后由 Worker 恢复认领。队列容量遵循现有开放接入队列的限制。
- QoS 1 允许重复；接收方使用 `event` + `deliveryId` 去重。Broker 确认不代表订阅者的业务处理已经完成。协议语义见 [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)。

## Topic 与统一信封

Topic 固定为 `iot/{deviceId}`，其中 `{deviceId}` 由平台替换为设备 UUID，不支持自定义 Topic。同一设备的数据、告警和控制状态发布到同一 Topic，通过 JSON 的 `event` 字段区分事件类型。管理 API 不提供 Topic 配置字段。订阅方可按自己的 Broker 权限使用 `iot/+` 等过滤器。

所有事件沿用 Webhook 的 JSON 信封；字段名是固定契约，不提供任意模板转换。时间统一为 UTC RFC 3339，设备身份使用平台 UUID。

```json
{
  "event": "device.data.reported",
  "time": "2026-10-13T03:00:00Z",
  "deliveryId": "019f0000-0000-7000-8000-000000000001",
  "data": {
    "device": {
      "id": "019f0000-0000-7000-8000-000000000002",
      "code": "D001",
      "name": "一号设备"
    },
    "points": [
      { "id": "019f0000-0000-7000-8000-000000000003", "name": "温度", "access": "ro", "value": 25.6, "unit": "℃", "time": "2026-10-13T03:00:00Z" },
      { "id": "019f0000-0000-7000-8000-000000000004", "name": "水泵开关", "access": "rw", "value": 0, "unit": "", "time": "2026-10-13T03:00:00Z" }
    ]
  }
}
```

| `event` | `data` 内容 |
| --- | --- |
| `device.data.reported` | `device` 和当前 `points`，点值保持数值、字符串或 `null`；BOOL 沿用平台的 `0/1` 契约 |
| `device.image.reported` | `device` 和图片事件内容，沿用 Webhook 图片表示 |
| `device.alert.triggered` / `device.alert.resolved` | `device` 与告警事件字段，沿用 Webhook 告警结构 |
| `device.command.accepted` / `device.command.updated` | `device` 和 `command`：`id`、`status`、`reason`、`elements`、`actual_values` |

## 控制请求和回执

`POST /open-api/device/command`，请求头 `Content-Type: application/json` 和 `X-Access-Key`。AccessKey 必须具有 `device:command`，且包含目标设备。设备本身也必须允许远程控制。

```json
{
  "deviceId": "019f0000-0000-7000-8000-000000000002",
  "idempotency_key": "019f0000-0000-7000-8000-000000000005",
  "elements": [
    { "elementId": "019f0000-0000-7000-8000-000000000004", "value": "1" }
  ]
}
```

`points[].id` 必须是点位 UUID，`access` 使用 `ro`（只读）或 `rw`（可读写），按模型能力生成；派生点位为 `ro`。`access` 不替代 AccessKey 或设备授权。MQTT 与 Webhook 使用同一个序列化入口，完全共用这些字段。

`idempotency_key` 为本次操作生成的 UUID，重试同一操作时复用。`elementId` 使用设备模型中的可写点位 UUID，`value` 使用字符串形式。响应的 `data.command_ids` 用于关联命令；HTTP 受理成功不等于设备执行成功。`device.command.updated` 保留平台状态原值，如 `SUCCEEDED`、`REJECTED`、`FAILED`、`READBACK_MISMATCH`、`UNKNOWN`。超时可能代表结果未知，不能假定设备没有执行，也不得把 MQTT `PUBACK` 解释为设备控制成功。

## 管理 API

- `GET /api/open-mqtt`：查询；可按 `accessKeyId` 筛选。
- `POST /api/open-mqtt`：创建。
- `PUT /api/open-mqtt/:id`：提交完整配置；省略密码保留原值。
- `DELETE /api/open-mqtt/:id`：删除。
- `GET /api/open-access-log?mqttId=<id>`：查看成功、失败和重试投递记录，`action=mqtt`，`httpMethod=PUBLISH`，`httpStatus=null`。

管理接口沿用 `iot:open-access:query/add/edit/delete`。配置页面使用 HTTP 查询和保存，手动刷新状态，没有业务轮询或额外 SSE。
