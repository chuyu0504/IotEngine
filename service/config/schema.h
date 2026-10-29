#pragma once
#include <algorithm>
#include <array>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include <ruvia/web/db/db_schema.h>

#include "service/common/message.h"
#include "service/config/channels.h"
#include "service/config/current_device_model.h"
#include "service/config/device_parameter_definitions.h"
#include "service/config/device_parameters.h"
#include "service/config/industrial_protocols.h"
#include "service/config/model_revisions.h"
#include "service/config/mqtt.h"
#include "service/config/native_points.h"
#include "service/utils/crypto.h"

namespace service::config {

inline const auto kSchemaMigrations = [] {
    using Type = ruvia::db_data_type;
    std::vector<ruvia::db_migration> migrations{
        ruvia::db_migration({ .id_ = "0000_unified_link_boundary", .sql_ = R"sql(
DO $schema$
BEGIN
IF NOT EXISTS (SELECT 1 FROM sys_schema_migrations) THEN
    RETURN;
END IF;

IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'link'
      AND column_name = 'execution'
) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'device'
      AND column_name = 'edge_node_id'
) THEN
    RETURN;
END IF;

IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'link'
      AND column_name = 'usage'
) OR NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'device'
      AND column_name = 'edge_node_id'
) OR NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'device'
      AND column_name = 'edge_endpoint'
) THEN
    RAISE EXCEPTION
        'this iot-engine schema is neither the supported legacy layout nor the unified layout';
END IF;

ALTER TABLE link
    ADD COLUMN execution VARCHAR(16),
    ADD COLUMN edge_node_id UUID;

ALTER TABLE link DROP CONSTRAINT IF EXISTS ck_link_endpoint;

UPDATE link
SET execution = 'collector',
    endpoint = jsonb_set(endpoint, '{transport}', '"tcp"'::jsonb, true);

CREATE TEMP TABLE iot_edge_link_migration ON COMMIT DROP AS
SELECT id AS device_id, gen_random_uuid() AS link_id
FROM device
WHERE edge_node_id IS NOT NULL AND link_id IS NULL;

INSERT INTO link(
    id, name, protocol, endpoint, execution, edge_node_id, status,
    created_by, created_at, updated_at, deleted_at)
SELECT migration.link_id,
       left(device.name, 70) || ' [edge ' || left(device.id::text, 8) || ']',
       protocol_config.protocol,
       device.edge_endpoint,
       'edge',
       device.edge_node_id,
       device.status,
       device.created_by,
       device.created_at,
       device.updated_at,
       device.deleted_at
FROM iot_edge_link_migration AS migration
JOIN device ON device.id = migration.device_id
JOIN protocol_config ON protocol_config.id = device.protocol_config_id;

ALTER TABLE device DROP CONSTRAINT IF EXISTS ck_device_connection_source;

UPDATE device
SET link_id = migration.link_id
FROM iot_edge_link_migration AS migration
WHERE device.id = migration.device_id;

UPDATE device_data
SET link_id = device.link_id
FROM device
WHERE device.id = device_data.device_id AND device_data.link_id IS NULL;

IF EXISTS (SELECT 1 FROM device WHERE link_id IS NULL) OR
   EXISTS (SELECT 1 FROM device_data WHERE link_id IS NULL) THEN
    RAISE EXCEPTION 'legacy device linkage could not be migrated completely';
END IF;

ALTER TABLE link
    ALTER COLUMN execution SET NOT NULL,
    ADD CONSTRAINT fk_link_edge_node
        FOREIGN KEY (edge_node_id) REFERENCES edge_node(id) ON DELETE RESTRICT;

ALTER TABLE link ADD CONSTRAINT ck_link_endpoint CHECK (
    (
        execution = 'collector'
        AND edge_node_id IS NULL
        AND endpoint->>'transport' = 'tcp'
        AND endpoint->>'mode' IN ('TCP Server', 'TCP Client')
        AND COALESCE(jsonb_typeof(endpoint->'targets'), 'array') = 'array'
        AND (
            (
                endpoint->>'mode' = 'TCP Server'
                AND endpoint->>'ip' = '0.0.0.0'
                AND COALESCE(endpoint->>'port', '') ~ '^[0-9]+$'
                AND (endpoint->>'port')::integer BETWEEN 1 AND 65535
                AND jsonb_array_length(COALESCE(endpoint->'targets', '[]'::jsonb)) = 0
            )
            OR
            (
                endpoint->>'mode' = 'TCP Client'
                AND COALESCE(endpoint->>'ip', '') = ''
                AND COALESCE((endpoint->>'port')::integer, 0) = 0
                AND jsonb_array_length(COALESCE(endpoint->'targets', '[]'::jsonb)) > 0
            )
        )
    )
    OR (
        execution = 'edge'
        AND edge_node_id IS NOT NULL
        AND COALESCE(endpoint->>'interface', '') <> ''
        AND (
            endpoint->>'transport' = 'serial'
            OR (
                endpoint->>'transport' = 'tcp'
                AND endpoint->>'mode' IN ('TCP Server', 'TCP Client')
                AND COALESCE(endpoint->>'ip', '') <> ''
                AND COALESCE(endpoint->>'port', '') ~ '^[0-9]+$'
                AND (endpoint->>'port')::integer BETWEEN 1 AND 65535
            )
        )
    )
);

DROP INDEX IF EXISTS idx_link_usage;
CREATE INDEX idx_link_execution ON link(execution) WHERE deleted_at IS NULL;
CREATE INDEX idx_link_edge_node ON link(edge_node_id)
    WHERE execution = 'edge' AND deleted_at IS NULL;

DROP INDEX IF EXISTS idx_device_edge_node;
ALTER TABLE device
    ALTER COLUMN link_id SET NOT NULL,
    DROP COLUMN edge_node_id,
    DROP COLUMN edge_endpoint;
ALTER TABLE device_data ALTER COLUMN link_id SET NOT NULL;

ALTER TABLE link
    DROP COLUMN usage,
    DROP COLUMN agent_id,
    DROP COLUMN agent_interface,
    DROP COLUMN agent_bind_ip,
    DROP COLUMN agent_prefix_length,
    DROP COLUMN agent_gateway;
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0001_initial_schema", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TYPE status_enum AS ENUM ('enabled', 'disabled');

CREATE TABLE sys_role (
    id          UUID PRIMARY KEY,
    name        VARCHAR(50) NOT NULL,
    code        VARCHAR(50) NOT NULL,
    description VARCHAR(255),
    status      status_enum NOT NULL DEFAULT 'enabled',
    sort_order  INTEGER NOT NULL DEFAULT 0,
    permissions JSONB NOT NULL DEFAULT '[]'::jsonb
                CHECK (jsonb_typeof(permissions) = 'array'),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_sys_role_code_active ON sys_role(code) WHERE deleted_at IS NULL;
CREATE INDEX idx_sys_role_deleted ON sys_role(deleted_at);
CREATE INDEX idx_sys_role_status_deleted ON sys_role(status, deleted_at);

CREATE TABLE sys_department (
    id          UUID PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    code        VARCHAR(50),
    parent_id   UUID REFERENCES sys_department(id) ON DELETE RESTRICT,
    leader_id   UUID,
    phone       VARCHAR(20),
    email       VARCHAR(100),
    status      status_enum NOT NULL DEFAULT 'enabled',
    sort_order  INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMPTZ
);
CREATE INDEX idx_sys_department_parent ON sys_department(parent_id);
CREATE INDEX idx_sys_department_deleted ON sys_department(deleted_at);
CREATE UNIQUE INDEX idx_sys_department_code ON sys_department(code)
    WHERE code IS NOT NULL AND deleted_at IS NULL;

CREATE TABLE sys_user (
    id            UUID PRIMARY KEY,
    username      VARCHAR(50) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    nickname      VARCHAR(100),
    email         VARCHAR(100),
    phone         VARCHAR(20),
    avatar        VARCHAR(255),
    status        status_enum NOT NULL DEFAULT 'enabled',
    department_id UUID REFERENCES sys_department(id) ON DELETE SET NULL,
    profile       JSONB NOT NULL DEFAULT '{}'::jsonb
                  CHECK (jsonb_typeof(profile) = 'object'),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at    TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_sys_user_username_active ON sys_user(username)
    WHERE deleted_at IS NULL;
CREATE INDEX idx_sys_user_deleted ON sys_user(deleted_at);
CREATE INDEX idx_sys_user_department ON sys_user(department_id) WHERE deleted_at IS NULL;
ALTER TABLE sys_department ADD CONSTRAINT fk_sys_department_leader
    FOREIGN KEY (leader_id) REFERENCES sys_user(id) ON DELETE SET NULL;

CREATE TABLE sys_user_role (
    id         UUID PRIMARY KEY,
    user_id    UUID NOT NULL REFERENCES sys_user(id) ON DELETE CASCADE,
    role_id    UUID NOT NULL REFERENCES sys_role(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, role_id)
);
CREATE INDEX idx_sys_user_role_user ON sys_user_role(user_id);
CREATE INDEX idx_sys_user_role_role ON sys_user_role(role_id);

CREATE TABLE link (
    id           UUID PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    protocol     VARCHAR(20) NOT NULL CHECK (protocol IN ('SL651', 'Modbus', 'S7')),
    endpoint     JSONB NOT NULL CHECK (jsonb_typeof(endpoint) = 'object'),
    execution    VARCHAR(16) NOT NULL CHECK (execution IN ('collector', 'edge')),
    edge_node_id UUID,
    status       status_enum NOT NULL DEFAULT 'enabled',
    created_by   UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at   TIMESTAMPTZ,
    CONSTRAINT ck_link_endpoint CHECK (
        (
            execution = 'collector'
            AND edge_node_id IS NULL
            AND endpoint->>'transport' = 'tcp'
            AND endpoint->>'mode' IN ('TCP Server', 'TCP Client')
            AND COALESCE(jsonb_typeof(endpoint->'targets'), 'array') = 'array'
            AND (
                (
                    endpoint->>'mode' = 'TCP Server'
                    AND endpoint->>'ip' = '0.0.0.0'
                    AND COALESCE(endpoint->>'port', '') ~ '^[0-9]+$'
                    AND (endpoint->>'port')::integer BETWEEN 1 AND 65535
                    AND jsonb_array_length(COALESCE(endpoint->'targets', '[]'::jsonb)) = 0
                )
                OR
                (
                    endpoint->>'mode' = 'TCP Client'
                    AND COALESCE(endpoint->>'ip', '') = ''
                    AND COALESCE((endpoint->>'port')::integer, 0) = 0
                    AND jsonb_array_length(COALESCE(endpoint->'targets', '[]'::jsonb)) > 0
                )
            )
        )
        OR (
            execution = 'edge'
            AND edge_node_id IS NOT NULL
            AND COALESCE(endpoint->>'interface', '') <> ''
            AND (
                endpoint->>'transport' = 'serial'
                OR (
                    endpoint->>'transport' = 'tcp'
                    AND endpoint->>'mode' IN ('TCP Server', 'TCP Client')
                    AND COALESCE(endpoint->>'ip', '') <> ''
                    AND COALESCE(endpoint->>'port', '') ~ '^[0-9]+$'
                    AND (endpoint->>'port')::integer BETWEEN 1 AND 65535
                )
            )
        )
    )
);
CREATE INDEX idx_link_deleted ON link(deleted_at);
CREATE UNIQUE INDEX idx_link_name ON link(name) WHERE deleted_at IS NULL;
CREATE INDEX idx_link_status_active ON link(status) WHERE deleted_at IS NULL;
CREATE INDEX idx_link_protocol ON link(protocol);
CREATE INDEX idx_link_execution ON link(execution) WHERE deleted_at IS NULL;
CREATE INDEX idx_link_endpoint_mode ON link((endpoint->>'mode')) WHERE deleted_at IS NULL;
CREATE INDEX idx_link_endpoint_gin ON link USING GIN(endpoint);

CREATE TABLE protocol_config (
    id          UUID PRIMARY KEY,
    protocol    VARCHAR(20) NOT NULL CHECK (protocol IN ('SL651', 'Modbus', 'S7')),
    name        VARCHAR(64) NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    config      JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(config) = 'object'),
    remark      TEXT,
    created_by  UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMPTZ
);
CREATE INDEX idx_protocol_config_protocol ON protocol_config(protocol);
CREATE INDEX idx_protocol_config_deleted ON protocol_config(deleted_at);
CREATE UNIQUE INDEX idx_protocol_config_name ON protocol_config(name) WHERE deleted_at IS NULL;

CREATE TABLE device_group (
    id          UUID PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    parent_id   UUID REFERENCES device_group(id) ON DELETE RESTRICT,
    status      status_enum NOT NULL DEFAULT 'enabled',
    sort_order  INTEGER NOT NULL DEFAULT 0 CHECK (sort_order >= 0),
    remark      TEXT,
    created_by  UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMPTZ
);
CREATE INDEX idx_device_group_parent ON device_group(parent_id);
CREATE INDEX idx_device_group_deleted ON device_group(deleted_at);
CREATE UNIQUE INDEX idx_device_group_name ON device_group(name) WHERE deleted_at IS NULL;

CREATE TABLE device (
    id                  UUID PRIMARY KEY,
    name                VARCHAR(100) NOT NULL,
    link_id             UUID NOT NULL REFERENCES link(id) ON DELETE RESTRICT,
    protocol_config_id  UUID NOT NULL REFERENCES protocol_config(id) ON DELETE RESTRICT,
    group_id            UUID REFERENCES device_group(id) ON DELETE RESTRICT,
    status              status_enum NOT NULL DEFAULT 'enabled',
    protocol_params     JSONB NOT NULL DEFAULT '{}'::jsonb
                        CHECK (jsonb_typeof(protocol_params) = 'object'),
    remark              TEXT,
    created_by          UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at          TIMESTAMPTZ
);
CREATE INDEX idx_device_link ON device(link_id);
CREATE INDEX idx_device_protocol ON device(protocol_config_id);
CREATE INDEX idx_device_group ON device(group_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_device_deleted ON device(deleted_at);
CREATE UNIQUE INDEX idx_device_name ON device(name) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX idx_device_protocol_params_code
    ON device((protocol_params->>'device_code'))
    WHERE deleted_at IS NULL
      AND protocol_params->>'device_code' IS NOT NULL
      AND protocol_params->>'device_code' != '';

CREATE TABLE device_data (
    id             UUID NOT NULL,
    device_id      UUID NOT NULL REFERENCES device(id) ON DELETE RESTRICT,
    link_id        UUID NOT NULL REFERENCES link(id) ON DELETE RESTRICT,
    protocol       TEXT NOT NULL CHECK (protocol IN ('SL651', 'Modbus', 'S7')),
    data           JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(data) = 'object'),
    report_time    TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    connection_id  UUID NOT NULL,
    source         TEXT NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    raw_payload_hex JSONB NOT NULL DEFAULT '[]'::jsonb
                    CHECK (jsonb_typeof(raw_payload_hex) = 'array'),
    PRIMARY KEY (id, report_time)
);

PERFORM create_hypertable('device_data', 'report_time', if_not_exists => TRUE);
CREATE INDEX idx_device_data_device_time
    ON device_data(device_id, report_time DESC);
CREATE INDEX idx_device_data_link_time
    ON device_data(link_id, report_time DESC);
CREATE INDEX idx_device_data_history
    ON device_data(device_id, (data->>'function_code'), report_time DESC);

INSERT INTO sys_role(id, code, name, description, status, permissions)
VALUES ('00000000-0000-7000-8000-000000000001', 'superadmin', '超级管理员',
        '系统内置角色', 'enabled', '["*"]'::jsonb);

INSERT INTO sys_user(id, username, password_hash, nickname, status)
VALUES (
    '00000000-0000-7000-8000-000000000002',
    'admin',
    'pbkdf2_sha256$210000$c92f36ef05a2afc548b868057a91af3a$9aa29710cf82ac0a23c8b0a112e76e2b860ecb303c949b9eada7045762df5974',
    '系统管理员',
    'enabled'
);

INSERT INTO sys_user_role(id, user_id, role_id)
VALUES ('00000000-0000-7000-8000-000000000003',
        '00000000-0000-7000-8000-000000000002',
        '00000000-0000-7000-8000-000000000001');

EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'UTC');
PERFORM set_config('TimeZone', 'UTC', false);
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0002_device_access_control", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE TABLE device_access_grant (
    id            UUID PRIMARY KEY,
    device_id     UUID NOT NULL REFERENCES device(id) ON DELETE CASCADE,
    user_id       UUID REFERENCES sys_user(id) ON DELETE CASCADE,
    department_id UUID REFERENCES sys_department(id) ON DELETE CASCADE,
    access_level  VARCHAR(20) NOT NULL
                  CHECK (access_level IN ('view', 'operate', 'manage')),
    granted_by    UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_device_access_subject CHECK (
        num_nonnulls(user_id, department_id) = 1
    )
);
CREATE UNIQUE INDEX idx_device_access_user_unique
    ON device_access_grant(device_id, user_id) WHERE user_id IS NOT NULL;
CREATE UNIQUE INDEX idx_device_access_department_unique
    ON device_access_grant(device_id, department_id) WHERE department_id IS NOT NULL;
CREATE INDEX idx_device_access_user_scope
    ON device_access_grant(user_id, device_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_device_access_department_scope
    ON device_access_grant(department_id, device_id) WHERE department_id IS NOT NULL;
CREATE INDEX idx_device_created_by_active
    ON device(created_by, id) WHERE deleted_at IS NULL;

CREATE TABLE security_audit_log (
    id            UUID PRIMARY KEY,
    actor_user_id UUID REFERENCES sys_user(id) ON DELETE SET NULL,
    action        VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50) NOT NULL,
    resource_id   UUID,
    outcome       VARCHAR(20) NOT NULL CHECK (outcome IN ('success', 'denied', 'failed')),
    reason        TEXT,
    details       JSONB NOT NULL DEFAULT '{}'::jsonb
                  CHECK (jsonb_typeof(details) = 'object'),
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_security_audit_resource_time
    ON security_audit_log(resource_type, resource_id, occurred_at DESC);
CREATE INDEX idx_security_audit_actor_time
    ON security_audit_log(actor_user_id, occurred_at DESC);
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0003_device_share_without_manage", .sql_ = R"sql(
DO $schema$
BEGIN
UPDATE device_access_grant
SET access_level = 'operate', updated_at = NOW()
WHERE access_level = 'manage';

ALTER TABLE device_access_grant
    DROP CONSTRAINT IF EXISTS device_access_grant_access_level_check;
ALTER TABLE device_access_grant
    DROP CONSTRAINT IF EXISTS ck_device_access_level;
ALTER TABLE device_access_grant
    ADD CONSTRAINT ck_device_access_level
    CHECK (access_level IN ('view', 'operate'));
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0004_device_group_access_control", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE TABLE device_group_access_grant (
    id            UUID PRIMARY KEY,
    group_id      UUID NOT NULL REFERENCES device_group(id) ON DELETE CASCADE,
    user_id       UUID REFERENCES sys_user(id) ON DELETE CASCADE,
    department_id UUID REFERENCES sys_department(id) ON DELETE CASCADE,
    access_level  VARCHAR(20) NOT NULL
                  CONSTRAINT ck_device_group_access_level
                  CHECK (access_level IN ('view', 'operate')),
    granted_by    UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_device_group_access_subject CHECK (
        num_nonnulls(user_id, department_id) = 1
    )
);
CREATE UNIQUE INDEX idx_device_group_access_user_unique
    ON device_group_access_grant(group_id, user_id) WHERE user_id IS NOT NULL;
CREATE UNIQUE INDEX idx_device_group_access_department_unique
    ON device_group_access_grant(group_id, department_id) WHERE department_id IS NOT NULL;
CREATE INDEX idx_device_group_access_user_scope
    ON device_group_access_grant(user_id, group_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_device_group_access_department_scope
    ON device_group_access_grant(department_id, group_id) WHERE department_id IS NOT NULL;
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0005_open_access", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE TABLE open_access_key (
    id                UUID PRIMARY KEY,
    name              VARCHAR(64) NOT NULL,
    access_key_prefix VARCHAR(16) NOT NULL,
    access_key_hash   VARCHAR(64) NOT NULL,
    status            status_enum NOT NULL DEFAULT 'enabled',
    scopes            JSONB NOT NULL DEFAULT '[]'::jsonb
                      CHECK (jsonb_typeof(scopes) = 'array'),
    expires_at        TIMESTAMPTZ,
    last_used_at      TIMESTAMPTZ,
    last_used_ip      VARCHAR(64),
    remark            VARCHAR(200),
    created_by        UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at        TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_open_access_key_hash ON open_access_key(access_key_hash);
CREATE UNIQUE INDEX idx_open_access_key_name_active
    ON open_access_key(name) WHERE deleted_at IS NULL;
CREATE INDEX idx_open_access_key_active
    ON open_access_key(status, expires_at) WHERE deleted_at IS NULL;

CREATE TABLE open_access_key_device (
    access_key_id UUID NOT NULL REFERENCES open_access_key(id) ON DELETE CASCADE,
    device_id     UUID NOT NULL REFERENCES device(id) ON DELETE CASCADE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (access_key_id, device_id)
);
CREATE INDEX idx_open_access_key_device_device ON open_access_key_device(device_id);

CREATE TABLE open_webhook (
    id                UUID PRIMARY KEY,
    access_key_id     UUID NOT NULL REFERENCES open_access_key(id) ON DELETE CASCADE,
    name              VARCHAR(64) NOT NULL,
    url               TEXT NOT NULL,
    status            status_enum NOT NULL DEFAULT 'enabled',
    secret            VARCHAR(255),
    headers           JSONB NOT NULL DEFAULT '{}'::jsonb
                      CHECK (jsonb_typeof(headers) = 'object'),
    event_types       JSONB NOT NULL DEFAULT '["device.data.reported"]'::jsonb
                      CHECK (jsonb_typeof(event_types) = 'array'),
    timeout_seconds   INTEGER NOT NULL DEFAULT 5 CHECK (timeout_seconds BETWEEN 1 AND 30),
    last_triggered_at TIMESTAMPTZ,
    last_success_at   TIMESTAMPTZ,
    last_failure_at   TIMESTAMPTZ,
    last_http_status  INTEGER,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at        TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_open_webhook_name_active
    ON open_webhook(access_key_id, name) WHERE deleted_at IS NULL;
CREATE INDEX idx_open_webhook_active
    ON open_webhook(access_key_id, status) WHERE deleted_at IS NULL;

CREATE TABLE open_access_log (
    id               UUID PRIMARY KEY,
    access_key_id    UUID REFERENCES open_access_key(id) ON DELETE SET NULL,
    webhook_id       UUID REFERENCES open_webhook(id) ON DELETE SET NULL,
    direction        VARCHAR(20) NOT NULL CHECK (direction IN ('pull', 'push')),
    action           VARCHAR(50) NOT NULL,
    event_type       VARCHAR(100),
    status           VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed')),
    http_method      VARCHAR(10),
    target           TEXT,
    request_ip       VARCHAR(64),
    http_status      INTEGER,
    device_id        UUID REFERENCES device(id) ON DELETE SET NULL,
    device_code      VARCHAR(100),
    message          TEXT,
    request_payload  JSONB NOT NULL DEFAULT '{}'::jsonb
                     CHECK (jsonb_typeof(request_payload) = 'object'),
    response_payload JSONB NOT NULL DEFAULT '{}'::jsonb
                     CHECK (jsonb_typeof(response_payload) = 'object'),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_open_access_log_key_time
    ON open_access_log(access_key_id, created_at DESC);
CREATE INDEX idx_open_access_log_webhook_time
    ON open_access_log(webhook_id, created_at DESC);
CREATE INDEX idx_open_access_log_device_time
    ON open_access_log(device_id, created_at DESC);

-- 告警引擎尚未迁入 iot-engine；开放接入拥有独立、稳定的读模型，供后续告警投影写入。
CREATE TABLE open_alert_record (
    id           UUID PRIMARY KEY,
    rule_id      UUID,
    device_id    UUID NOT NULL REFERENCES device(id) ON DELETE CASCADE,
    severity     VARCHAR(20) NOT NULL,
    status       VARCHAR(20) NOT NULL,
    message      TEXT NOT NULL,
    detail       JSONB NOT NULL DEFAULT '{}'::jsonb
                 CHECK (jsonb_typeof(detail) = 'object'),
    triggered_at TIMESTAMPTZ NOT NULL,
    resolved_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_open_alert_device_time
    ON open_alert_record(device_id, triggered_at DESC);
CREATE INDEX idx_open_alert_status_time
    ON open_alert_record(status, triggered_at DESC);
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0006_edge_node_management", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE TABLE edge_node (
    id                         UUID PRIMARY KEY,
    platform_id                UUID NOT NULL,
    imei                       VARCHAR(15) NOT NULL,
    name                       VARCHAR(100),
    model                      VARCHAR(128) NOT NULL DEFAULT '',
    software_version           VARCHAR(32) NOT NULL DEFAULT '',
    hostname                   VARCHAR(64) NOT NULL DEFAULT '',
    architecture               VARCHAR(32) NOT NULL DEFAULT '',
    openwrt_release            VARCHAR(64) NOT NULL DEFAULT '',
    enrollment_status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                               CHECK (enrollment_status IN ('pending', 'approved', 'rejected')),
    supports_network_config    BOOLEAN NOT NULL DEFAULT FALSE,
    supports_firmware_update   BOOLEAN NOT NULL DEFAULT FALSE,
    supports_platform_config   BOOLEAN NOT NULL DEFAULT FALSE,
    ttyd_available             BOOLEAN NOT NULL DEFAULT FALSE,
    active_config_version      BIGINT NOT NULL DEFAULT 0,
    outbox_records             BIGINT NOT NULL DEFAULT 0,
    outbox_bytes               BIGINT NOT NULL DEFAULT 0,
    last_seen_at               TIMESTAMPTZ,
    approved_by                UUID REFERENCES sys_user(id) ON DELETE SET NULL,
    approved_at                TIMESTAMPTZ,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT edge_node_imei_digits CHECK (imei ~ '^[0-9]{15}$')
);
CREATE UNIQUE INDEX idx_edge_node_platform_imei ON edge_node(platform_id, imei);
CREATE INDEX idx_edge_node_status_seen ON edge_node(enrollment_status, last_seen_at DESC);
ALTER TABLE link ADD CONSTRAINT fk_link_edge_node
    FOREIGN KEY (edge_node_id) REFERENCES edge_node(id) ON DELETE RESTRICT;
CREATE INDEX idx_link_edge_node ON link(edge_node_id)
    WHERE execution = 'edge' AND deleted_at IS NULL;

CREATE TABLE edge_node_interface (
    node_id       UUID NOT NULL REFERENCES edge_node(id) ON DELETE CASCADE,
    name          VARCHAR(32) NOT NULL,
    display_name  VARCHAR(64) NOT NULL DEFAULT '',
    mac           VARCHAR(17),
    is_up         BOOLEAN NOT NULL DEFAULT FALSE,
    is_bridge     BOOLEAN NOT NULL DEFAULT FALSE,
    ipv4          VARCHAR(15),
    prefix_length INTEGER CHECK (prefix_length BETWEEN 0 AND 32),
    gateway       VARCHAR(15),
    bridge_ports  JSONB NOT NULL DEFAULT '[]'::jsonb
                  CHECK (jsonb_typeof(bridge_ports) = 'array'),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, name)
);

CREATE TABLE edge_node_serial (
    node_id      UUID NOT NULL REFERENCES edge_node(id) ON DELETE CASCADE,
    path         VARCHAR(96) NOT NULL,
    display_name VARCHAR(64) NOT NULL DEFAULT '',
    available    BOOLEAN NOT NULL DEFAULT FALSE,
    rs485        BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, path)
);

CREATE TABLE edge_firmware (
    id             UUID PRIMARY KEY,
    version        VARCHAR(64) NOT NULL,
    file_name      VARCHAR(255) NOT NULL,
    storage_path   TEXT NOT NULL,
    sha256         VARCHAR(64) NOT NULL,
    size_bytes     BIGINT NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 134217728),
    download_token VARCHAR(64) NOT NULL,
    created_by     UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_edge_firmware_created ON edge_firmware(created_at DESC);

CREATE TABLE edge_node_platform (
    node_id                UUID NOT NULL REFERENCES edge_node(id) ON DELETE CASCADE,
    platform_id            UUID NOT NULL,
    name                   VARCHAR(32) NOT NULL,
    base_url               VARCHAR(255) NOT NULL,
    enabled                BOOLEAN NOT NULL DEFAULT TRUE,
    priority               INTEGER NOT NULL DEFAULT 100 CHECK (priority BETWEEN 0 AND 65535),
    reconnect_interval_sec INTEGER NOT NULL DEFAULT 5 CHECK (reconnect_interval_sec BETWEEN 1 AND 3600),
    outbox_max_bytes       INTEGER NOT NULL DEFAULT 262144
                           CHECK (outbox_max_bytes BETWEEN 16384 AND 8388608),
    apply_status           VARCHAR(20) NOT NULL DEFAULT 'pending'
                           CHECK (apply_status IN ('pending', 'applied', 'failed')),
    last_message           VARCHAR(256),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, platform_id)
);

CREATE TABLE edge_task (
    id           UUID PRIMARY KEY,
    node_id      UUID NOT NULL REFERENCES edge_node(id) ON DELETE CASCADE,
    task_type    VARCHAR(30) NOT NULL
                 CHECK (task_type IN ('network', 'firmware', 'platform_upsert', 'platform_delete')),
    status       VARCHAR(20) NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'accepted', 'running', 'succeeded', 'failed')),
    request      JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(request) = 'object'),
    result       JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(result) = 'object'),
    created_by   UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX idx_edge_task_node_created ON edge_task(node_id, created_at DESC);
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0007_edge_device_assignment", .sql_ = R"sql(
DO $schema$
BEGIN
ALTER TABLE edge_node
    ADD COLUMN desired_config_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN supports_device_config BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN config_status VARCHAR(20) NOT NULL DEFAULT 'idle'
        CHECK (config_status IN ('idle', 'pending', 'applied', 'rejected')),
    ADD COLUMN config_message VARCHAR(256) NOT NULL DEFAULT '';

CREATE TABLE edge_config_revision (
    node_id      UUID NOT NULL REFERENCES edge_node(id) ON DELETE CASCADE,
    revision     BIGINT NOT NULL CHECK (revision > 0),
    sha256       VARCHAR(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    item_count   INTEGER NOT NULL CHECK (item_count BETWEEN 0 AND 512),
    status       VARCHAR(20) NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'applied', 'rejected')),
    message      VARCHAR(256) NOT NULL DEFAULT '',
    created_by   UUID REFERENCES sys_user(id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (node_id, revision)
);
CREATE INDEX idx_edge_config_revision_created
    ON edge_config_revision(node_id, created_at DESC);
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0008_edge_network_management", .sql_ = R"sql(
DO $schema$
BEGIN
ALTER TABLE edge_node
    ADD COLUMN network_config_version INTEGER NOT NULL DEFAULT 0
        CHECK (network_config_version BETWEEN 0 AND 100);

CREATE TABLE edge_node_network (
    node_id       UUID NOT NULL REFERENCES edge_node(id) ON DELETE CASCADE,
    name          VARCHAR(32) NOT NULL,
    address_mode  VARCHAR(12) NOT NULL
                  CHECK (address_mode IN ('dhcp', 'static', 'none')),
    device        VARCHAR(32) NOT NULL DEFAULT '',
    is_up         BOOLEAN NOT NULL DEFAULT FALSE,
    is_bridge     BOOLEAN NOT NULL DEFAULT FALSE,
    ipv4          VARCHAR(15),
    prefix_length INTEGER CHECK (prefix_length BETWEEN 0 AND 32),
    gateway       VARCHAR(15),
    bridge_ports  JSONB NOT NULL DEFAULT '[]'::jsonb
                  CHECK (jsonb_typeof(bridge_ports) = 'array'),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, name)
);
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0009_edge_modem_management", .sql_ = R"sql(
DO $schema$
BEGIN
ALTER TABLE edge_node
    ADD COLUMN modem_available BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN sim_state VARCHAR(20) NOT NULL DEFAULT 'unknown'
        CHECK (sim_state IN ('unknown', 'ready', 'not_inserted', 'pin_required',
                             'puk_required', 'blocked')),
    ADD COLUMN iccid VARCHAR(22) NOT NULL DEFAULT '',
    ADD COLUMN signal_csq INTEGER NOT NULL DEFAULT 99 CHECK (signal_csq BETWEEN 0 AND 99),
    ADD COLUMN signal_rssi_dbm INTEGER NOT NULL DEFAULT -1,
    ADD COLUMN signal_percent INTEGER NOT NULL DEFAULT 0
        CHECK (signal_percent BETWEEN 0 AND 100),
    ADD COLUMN mobile_registered BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN mobile_registration_status INTEGER NOT NULL DEFAULT -1,
    ADD COLUMN apn VARCHAR(63) NOT NULL DEFAULT '',
    ADD COLUMN mobile_operator VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN mobile_connected BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN mobile_ipv4 VARCHAR(15) NOT NULL DEFAULT '',
    ADD COLUMN supports_modem_control BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE edge_task DROP CONSTRAINT edge_task_task_type_check;
ALTER TABLE edge_task ADD CONSTRAINT edge_task_task_type_check
    CHECK (task_type IN ('network', 'firmware', 'modem',
                         'platform_upsert', 'platform_delete'));
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0010_unified_json_runtime_design", .sql_ = R"sql(
DO $schema$
BEGIN
ALTER TABLE edge_node
    ADD COLUMN status JSONB NOT NULL DEFAULT
        '{"config":{"activeVersion":0,"desiredVersion":0,"state":"idle","message":""},"outbox":{"records":0,"bytes":0}}'::jsonb
        CHECK (jsonb_typeof(status) = 'object'),
    ADD COLUMN capability JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(capability) = 'object'),
    ADD COLUMN mobile JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(mobile) = 'object');

UPDATE edge_node
SET status = jsonb_build_object(
        'config', jsonb_build_object(
            'activeVersion', active_config_version,
            'desiredVersion', desired_config_version,
            'state', config_status,
            'message', config_message),
        'outbox', jsonb_build_object(
            'records', outbox_records,
            'bytes', outbox_bytes)),
    capability = jsonb_build_object(
        'networkConfig', supports_network_config,
        'networkConfigVersion', network_config_version,
        'firmwareUpdate', supports_firmware_update,
        'platformConfig', supports_platform_config,
        'deviceConfig', supports_device_config,
        'modemControl', supports_modem_control,
        'terminal', ttyd_available),
    mobile = jsonb_build_object(
        'available', modem_available,
        'simState', sim_state,
        'iccid', iccid,
        'signal', jsonb_build_object(
            'csq', signal_csq,
            'rssiDbm', signal_rssi_dbm,
            'percent', signal_percent),
        'registered', mobile_registered,
        'registrationStatus', mobile_registration_status,
        'apn', apn,
        'operator', mobile_operator,
        'connected', mobile_connected,
        'ipv4', mobile_ipv4);

ALTER TABLE edge_node
    DROP COLUMN supports_network_config,
    DROP COLUMN supports_firmware_update,
    DROP COLUMN supports_platform_config,
    DROP COLUMN ttyd_available,
    DROP COLUMN active_config_version,
    DROP COLUMN outbox_records,
    DROP COLUMN outbox_bytes,
    DROP COLUMN desired_config_version,
    DROP COLUMN supports_device_config,
    DROP COLUMN config_status,
    DROP COLUMN config_message,
    DROP COLUMN modem_available,
    DROP COLUMN sim_state,
    DROP COLUMN iccid,
    DROP COLUMN signal_csq,
    DROP COLUMN signal_rssi_dbm,
    DROP COLUMN signal_percent,
    DROP COLUMN mobile_registered,
    DROP COLUMN mobile_registration_status,
    DROP COLUMN apn,
    DROP COLUMN mobile_operator,
    DROP COLUMN mobile_connected,
    DROP COLUMN mobile_ipv4,
    DROP COLUMN supports_modem_control;

CREATE INDEX idx_edge_node_capability_gin ON edge_node USING GIN(capability);

ALTER TABLE edge_node_platform
    ADD COLUMN status JSONB NOT NULL DEFAULT '{"state":"pending","message":""}'::jsonb
        CHECK (jsonb_typeof(status) = 'object');

UPDATE edge_node_platform
SET status = jsonb_build_object(
    'state', apply_status,
    'message', COALESCE(last_message, ''));

ALTER TABLE edge_node_platform
    DROP COLUMN apply_status,
    DROP COLUMN last_message;
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0011_edge_log_status", .sql_ = R"sql(
DO $schema$
BEGIN
ALTER TABLE edge_node
    ALTER COLUMN status SET DEFAULT
        '{"config":{"activeVersion":0,"desiredVersion":0,"state":"idle","message":""},"outbox":{"records":0,"bytes":0},"log":{"level":"info"}}'::jsonb;

UPDATE edge_node
SET status = jsonb_set(
        jsonb_set(status, '{log}', COALESCE(status->'log', '{}'::jsonb), true),
        '{log,level}', to_jsonb(COALESCE(status->'log'->>'level', 'info')::text), true)
WHERE NOT (status ? 'log') OR status->'log'->>'level' IS NULL;
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0012_alert_center", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE TABLE alert_rule (
    id                    UUID PRIMARY KEY,
    name                  VARCHAR(128) NOT NULL,
    device_id             UUID NOT NULL REFERENCES device(id) ON DELETE CASCADE,
    severity              VARCHAR(20) NOT NULL
                          CHECK (severity IN ('critical', 'warning', 'info')),
    conditions            JSONB NOT NULL CHECK (jsonb_typeof(conditions) = 'array'),
    logic                 VARCHAR(8) NOT NULL DEFAULT 'and'
                          CHECK (logic IN ('and', 'or')),
    silence_duration      INTEGER NOT NULL DEFAULT 300
                          CHECK (silence_duration BETWEEN 0 AND 86400),
    recovery_condition    VARCHAR(32) NOT NULL DEFAULT 'reverse',
    recovery_wait_seconds INTEGER NOT NULL DEFAULT 60
                          CHECK (recovery_wait_seconds BETWEEN 0 AND 86400),
    status                status_enum NOT NULL DEFAULT 'enabled',
    remark                VARCHAR(500),
    created_by            UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at            TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_alert_rule_name_device_active
    ON alert_rule(device_id, name) WHERE deleted_at IS NULL;
CREATE INDEX idx_alert_rule_device
    ON alert_rule(device_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_alert_rule_status
    ON alert_rule(status) WHERE deleted_at IS NULL;

CREATE TABLE alert_rule_template (
    id                    UUID PRIMARY KEY,
    name                  VARCHAR(128) NOT NULL,
    category              VARCHAR(64),
    description           VARCHAR(500),
    severity              VARCHAR(20) NOT NULL
                          CHECK (severity IN ('critical', 'warning', 'info')),
    conditions            JSONB NOT NULL CHECK (jsonb_typeof(conditions) = 'array'),
    logic                 VARCHAR(8) NOT NULL DEFAULT 'and'
                          CHECK (logic IN ('and', 'or')),
    silence_duration      INTEGER NOT NULL DEFAULT 300
                          CHECK (silence_duration BETWEEN 0 AND 86400),
    recovery_condition    VARCHAR(32) NOT NULL DEFAULT 'reverse',
    recovery_wait_seconds INTEGER NOT NULL DEFAULT 60
                          CHECK (recovery_wait_seconds BETWEEN 0 AND 86400),
    applicable_protocols  JSONB NOT NULL DEFAULT '[]'::jsonb
                          CHECK (jsonb_typeof(applicable_protocols) = 'array'),
    protocol_config_id    UUID REFERENCES protocol_config(id) ON DELETE SET NULL,
    created_by            UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at            TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_alert_template_name_active
    ON alert_rule_template(name) WHERE deleted_at IS NULL;
CREATE INDEX idx_alert_template_category
    ON alert_rule_template(category) WHERE deleted_at IS NULL;

ALTER TABLE open_alert_record
    ADD COLUMN acknowledged_at TIMESTAMPTZ,
    ADD COLUMN acknowledged_by UUID REFERENCES sys_user(id) ON DELETE SET NULL,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE open_alert_record
    ADD CONSTRAINT fk_open_alert_record_rule
        FOREIGN KEY (rule_id) REFERENCES alert_rule(id) ON DELETE SET NULL NOT VALID,
    ADD CONSTRAINT ck_open_alert_record_severity
        CHECK (severity IN ('critical', 'warning', 'info')) NOT VALID,
    ADD CONSTRAINT ck_open_alert_record_status
        CHECK (status IN ('active', 'acknowledged', 'resolved')) NOT VALID;

CREATE UNIQUE INDEX idx_open_alert_one_unresolved_rule
    ON open_alert_record(rule_id)
    WHERE rule_id IS NOT NULL AND status IN ('active', 'acknowledged');
CREATE INDEX idx_open_alert_rule_time
    ON open_alert_record(rule_id, triggered_at DESC);

CREATE TABLE alert_rule_state (
    rule_id             UUID PRIMARY KEY REFERENCES alert_rule(id) ON DELETE CASCADE,
    matched             BOOLEAN NOT NULL DEFAULT FALSE,
    recovery_started_at TIMESTAMPTZ,
    last_evaluated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0013_timestamp_contract", .sql_ = R"sql(
DO $schema$
BEGIN
EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'UTC');
EXECUTE format('ALTER DATABASE %I SET datestyle TO %L', current_database(), 'ISO, YMD');
PERFORM set_config('TimeZone', 'UTC', false);
PERFORM set_config('DateStyle', 'ISO, YMD', false);
EXECUTE $definition$
CREATE OR REPLACE FUNCTION iot_utc_timestamp(value TIMESTAMPTZ)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS $function$
SELECT to_char(value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
$function$
$definition$;
EXECUTE $definition$
COMMENT ON FUNCTION iot_utc_timestamp(TIMESTAMPTZ) IS
    'Serializes typed timestamps at the public API boundary as UTC RFC 3339 seconds'
$definition$;
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0014_gb28181_projection", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE TABLE gb28181_device (
    id                  VARCHAR(128) PRIMARY KEY,
    name                VARCHAR(255) NOT NULL DEFAULT '',
    manufacturer        VARCHAR(255) NOT NULL DEFAULT '',
    remote_address      VARCHAR(255) NOT NULL DEFAULT '',
    registration_source VARCHAR(32) NOT NULL DEFAULT 'sip',
    online              BOOLEAN NOT NULL DEFAULT FALSE,
    last_seen_at        TIMESTAMPTZ NOT NULL,
    mapped_device_id    UUID REFERENCES device(id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_gb28181_device_online ON gb28181_device(online, last_seen_at DESC);
CREATE INDEX idx_gb28181_device_mapped ON gb28181_device(mapped_device_id)
    WHERE mapped_device_id IS NOT NULL;

CREATE TABLE gb28181_channel (
    device_id   VARCHAR(128) NOT NULL REFERENCES gb28181_device(id) ON DELETE CASCADE,
    id          VARCHAR(128) NOT NULL,
    name        VARCHAR(255) NOT NULL DEFAULT '',
    manufacturer VARCHAR(255) NOT NULL DEFAULT '',
    online      BOOLEAN NOT NULL DEFAULT FALSE,
    ptz_type    INTEGER NOT NULL DEFAULT -1,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (device_id, id)
);
CREATE INDEX idx_gb28181_channel_online
    ON gb28181_channel(device_id, online);

CREATE TABLE gb28181_record (
    device_id   VARCHAR(128) NOT NULL REFERENCES gb28181_device(id) ON DELETE CASCADE,
    channel_id  VARCHAR(128) NOT NULL,
    name        VARCHAR(255) NOT NULL DEFAULT '',
    file_path   TEXT NOT NULL DEFAULT '',
    address     TEXT NOT NULL DEFAULT '',
    start_time  TIMESTAMPTZ NOT NULL,
    end_time    TIMESTAMPTZ NOT NULL,
    record_type VARCHAR(64) NOT NULL DEFAULT '',
    recorder_id VARCHAR(128) NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (device_id, channel_id, start_time, end_time, file_path)
);
CREATE INDEX idx_gb28181_record_range
    ON gb28181_record(device_id, channel_id, start_time DESC, end_time DESC);

CREATE TABLE gb28181_stream (
    app          VARCHAR(128) NOT NULL,
    stream       VARCHAR(255) NOT NULL,
    schema       VARCHAR(32) NOT NULL,
    online       BOOLEAN NOT NULL DEFAULT FALSE,
    reader_count INTEGER NOT NULL DEFAULT 0 CHECK (reader_count >= 0),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (app, stream, schema)
);
CREATE INDEX idx_gb28181_stream_online
    ON gb28181_stream(online, updated_at DESC);
END
$schema$;
)sql" }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_index({ .name_ = "idx_alert_rule_enabled_device", .table_ = "alert_rule", .keys_ = { { .column_ = "device_id" } }, .where_ = q.binary(q.unary(ruvia::db_unary_operator::is_null, q.column("deleted_at")), ruvia::db_binary_operator::and_value, q.binary(q.column("status"), ruvia::db_binary_operator::equal, q.value("enabled"))) });
            schema.create_index({ .name_ = "idx_gb28181_record_device_time", .table_ = "gb28181_record", .keys_ = { { .column_ = "device_id" }, { .column_ = "start_time", .order_ = ruvia::db_order_direction::desc } } });
            auto result = schema.compile("0015_sql_query_optimization");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_index({ .name_ = "idx_open_access_log_time", .table_ = "open_access_log", .keys_ = { { .column_ = "created_at", .order_ = ruvia::db_order_direction::desc }, { .column_ = "id", .order_ = ruvia::db_order_direction::desc } } });
            schema.create_index({ .name_ = "idx_open_alert_time", .table_ = "open_alert_record", .keys_ = { { .column_ = "triggered_at", .order_ = ruvia::db_order_direction::desc }, { .column_ = "id", .order_ = ruvia::db_order_direction::desc } } });
            schema.create_index({ .name_ = "idx_open_alert_unresolved_summary", .table_ = "open_alert_record", .keys_ = { { .column_ = "severity" }, { .column_ = "device_id" } }, .where_ = q.binary(q.column("status"), ruvia::db_binary_operator::in, q.list({ q.value("active"), q.value("acknowledged") })) });
            schema.create_index({ .name_ = "idx_open_alert_resolved_time", .table_ = "open_alert_record", .keys_ = { { .column_ = "resolved_at", .order_ = ruvia::db_order_direction::desc } }, .where_ = q.binary(q.column("status"), ruvia::db_binary_operator::equal, q.value("resolved")) });
            auto result = schema.compile("0016_mass_data_query_optimization");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            schema.set_compression("device_data", { .enabled_ = true, .segment_by_ = { "device_id" }, .order_by_ = { { .column_ = "report_time", .direction_ = ruvia::db_order_direction::desc }, { .column_ = "id", .direction_ = ruvia::db_order_direction::desc } } });
            auto result = schema.compile("0017_device_data_compression_layout");
            return std::move(result.front());
        }(),
        ruvia::db_migration({ .id_ = "0018_device_data_ingest_state", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE TABLE device_data_ingest_state (
    device_id          UUID PRIMARY KEY REFERENCES device(id) ON DELETE CASCADE,
    last_stored_at     TIMESTAMPTZ,
    last_observed_at   TIMESTAMPTZ,
    last_observed_id   UUID,
    last_data          JSONB,
    previous_data      JSONB,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_device_data_ingest_observed_pair CHECK (
        (last_observed_at IS NULL) = (last_observed_id IS NULL)
    )
);

INSERT INTO device_data_ingest_state(
    device_id, last_stored_at, last_observed_at, last_observed_id,
    last_data, previous_data)
SELECT device.id, recent.last_report_time, recent.last_report_time,
       recent.last_report_id, recent.last_data, recent.previous_data
FROM device
CROSS JOIN LATERAL (
    SELECT
        (array_agg(sample.report_time ORDER BY sample.report_time DESC, sample.id DESC))[1]
            AS last_report_time,
        (array_agg(sample.id ORDER BY sample.report_time DESC, sample.id DESC))[1]
            AS last_report_id,
        (jsonb_agg(sample.data ORDER BY sample.report_time DESC, sample.id DESC))->0
            AS last_data,
        (jsonb_agg(sample.data ORDER BY sample.report_time DESC, sample.id DESC))->1
            AS previous_data
    FROM (
        SELECT history.report_time, history.id, history.data
        FROM device_data history
        WHERE history.device_id = device.id
        ORDER BY history.report_time DESC, history.id DESC
        LIMIT 2
    ) sample
) recent
WHERE recent.last_report_time IS NOT NULL
ON CONFLICT (device_id) DO NOTHING;
END
$schema$;
)sql" }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "alert_event_outbox", .columns_ = { { .name_ = "event_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "event_type", .type_ = { .data_type_ = Type::varchar, .length_ = 64 } }, { .name_ = "rule_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "device_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "device_code", .type_ = { .data_type_ = Type::varchar, .length_ = 255 }, .default_value_ = q.value("") }, { .name_ = "occurred_at_ms", .type_ = { .data_type_ = Type::big_int } }, { .name_ = "data", .type_ = { .data_type_ = Type::jsonb } }, { .name_ = "created_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") } }, .constraints_ = { { .name_ = "alert_event_outbox_pkey", .kind_ = ruvia::db_constraint_kind::primary_key, .columns_ = { "event_id", "event_type" } } } });
            schema.create_index({ .name_ = "idx_alert_event_outbox_created", .table_ = "alert_event_outbox", .keys_ = { { .column_ = "created_at" }, { .column_ = "event_id" } } });
            auto result = schema.compile("0019_alert_event_outbox");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "alert_evaluation_receipt", .columns_ = { { .name_ = "message_id", .type_ = { .data_type_ = Type::uuid }, .primary_key_ = true }, { .name_ = "device_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "created_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") } } });
            schema.create_index({ .name_ = "idx_alert_evaluation_receipt_created", .table_ = "alert_evaluation_receipt", .keys_ = { { .column_ = "created_at" } } });
            auto result = schema.compile("0020_alert_evaluation_receipt");
            return std::move(result.front());
        }(),
        ruvia::db_migration({ .id_ = "0021_device_latest_value", .sql_ = R"sql(
DO $schema$
BEGIN
CREATE TABLE device_latest_value (
    device_id    UUID NOT NULL REFERENCES device(id) ON DELETE CASCADE,
    element_id   TEXT NOT NULL,
    value        JSONB NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    record_id    UUID NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (device_id, element_id)
);

INSERT INTO device_latest_value(
    device_id, element_id, value, observed_at, record_id)
SELECT DISTINCT ON (history.device_id, point.key)
       history.device_id, point.key, point.value, history.report_time, history.id
FROM device_data history
CROSS JOIN LATERAL jsonb_each(
    COALESCE(history.data->'values', '{}'::jsonb)) point
ORDER BY history.device_id, point.key, history.report_time DESC, history.id DESC
ON CONFLICT (device_id, element_id) DO NOTHING;
END
$schema$;
)sql" }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            for (const auto table : { "gb28181_device", "gb28181_channel" }) {
                schema.add_column(table, { .name_ = "custom_name", .type_ = { .data_type_ = Type::varchar, .length_ = 255 }, .nullable_ = true });
            }
            auto result = schema.compile("0022_gb28181_custom_names");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "outbox_event", .columns_ = { { .name_ = "id", .type_ = { .data_type_ = Type::uuid }, .primary_key_ = true }, { .name_ = "event_type", .type_ = { .data_type_ = Type::varchar, .length_ = 100 } }, { .name_ = "aggregate_type", .type_ = { .data_type_ = Type::varchar, .length_ = 100 } }, { .name_ = "aggregate_id", .type_ = { .data_type_ = Type::text } }, { .name_ = "action", .type_ = { .data_type_ = Type::varchar, .length_ = 50 } }, { .name_ = "schema_version", .type_ = { .data_type_ = Type::integer } }, { .name_ = "payload", .type_ = { .data_type_ = Type::jsonb }, .default_value_ = q.cast(q.value("{}"), Type::jsonb) }, { .name_ = "occurred_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") }, { .name_ = "available_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") }, { .name_ = "attempts", .type_ = { .data_type_ = Type::integer }, .default_value_ = q.value(0) }, { .name_ = "last_error", .type_ = { .data_type_ = Type::text }, .nullable_ = true }, { .name_ = "published_at", .type_ = { .data_type_ = Type::timestamp_tz }, .nullable_ = true }, { .name_ = "dead_lettered_at", .type_ = { .data_type_ = Type::timestamp_tz }, .nullable_ = true } } });
            schema.create_index({ .name_ = "idx_outbox_event_pending", .table_ = "outbox_event", .keys_ = { { .column_ = "available_at" }, { .column_ = "occurred_at" }, { .column_ = "id" } }, .where_ = q.binary(q.unary(ruvia::db_unary_operator::is_null, q.column("published_at")), ruvia::db_binary_operator::and_value, q.unary(ruvia::db_unary_operator::is_null, q.column("dead_lettered_at"))) });
            auto result = schema.compile("0023_transactional_outbox");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "outbox_consumer_receipt", .columns_ = { { .name_ = "consumer_name", .type_ = { .data_type_ = Type::varchar, .length_ = 150 } }, { .name_ = "event_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "processed_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") } }, .constraints_ = { { .name_ = "outbox_consumer_receipt_pkey", .kind_ = ruvia::db_constraint_kind::primary_key, .columns_ = { "consumer_name", "event_id" } } } });
            schema.create_index({ .name_ = "idx_outbox_consumer_receipt_processed_at", .table_ = "outbox_consumer_receipt", .keys_ = { { .column_ = "processed_at" } } });
            auto result = schema.compile("0024_outbox_consumer_receipt");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.add_column("open_webhook", { .name_ = "skip_tls_verify", .type_ = { .data_type_ = Type::boolean }, .default_value_ = q.value(false) });
            auto result = schema.compile("0025_webhook_tls_verification");
            return std::move(result.front());
        }(),
        ruvia::db_migration({ .id_ = "0026_unify_protocol_read_interval", .sql_ = R"sql(
DO $schema$
BEGIN
UPDATE protocol_config
SET config = jsonb_set(config - 'pollInterval', '{readInterval}',
                       config->'pollInterval', true),
    updated_at = NOW()
WHERE protocol = 'S7' AND config ? 'pollInterval';

ALTER TABLE protocol_config
    ADD CONSTRAINT ck_protocol_config_no_poll_interval
    CHECK (NOT (config ? 'pollInterval'));
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0027_unify_protocol_storage_policy", .sql_ = R"sql(
DO $schema$
BEGIN
UPDATE protocol_config
SET config = jsonb_set(
        config - 'storageInterval',
        '{storagePolicy}',
        to_jsonb(CASE WHEN config->>'storagePolicy' IN ('report', 'change')
                      THEN config->>'storagePolicy' ELSE 'report' END),
        true),
    updated_at = NOW()
WHERE config ? 'storageInterval'
   OR NOT (config ? 'storagePolicy')
   OR COALESCE(config->>'storagePolicy' NOT IN ('report', 'change'), TRUE);

ALTER TABLE protocol_config
    ADD CONSTRAINT ck_protocol_config_storage_policy
    CHECK (NOT (config ? 'storageInterval')
           AND COALESCE(config->>'storagePolicy' IN ('report', 'change'), FALSE));
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0028_remove_edge_enrollment_rejection", .sql_ = R"sql(
DO $schema$
BEGIN
UPDATE edge_node
SET enrollment_status = 'pending', approved_by = NULL, approved_at = NULL,
    updated_at = NOW()
WHERE enrollment_status = 'rejected';

ALTER TABLE edge_node
    DROP CONSTRAINT IF EXISTS edge_node_enrollment_status_check;
ALTER TABLE edge_node
    ADD CONSTRAINT edge_node_enrollment_status_check
    CHECK (enrollment_status IN ('pending', 'approved'));
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0029_vpn_core", .sql_ = R"sql(
DO $schema$
BEGIN
ALTER TABLE edge_task DROP CONSTRAINT IF EXISTS edge_task_task_type_check;
ALTER TABLE edge_task ADD CONSTRAINT edge_task_task_type_check
    CHECK (task_type IN ('network', 'firmware', 'modem', 'vpn',
                         'platform_upsert', 'platform_delete'));

CREATE TABLE vpn_network (
    id                UUID PRIMARY KEY,
    name              VARCHAR(100) NOT NULL,
    overlay_cidr      VARCHAR(18) NOT NULL,
    hub_public_key    VARCHAR(64) NOT NULL DEFAULT '',
    hub_endpoint      VARCHAR(255) NOT NULL DEFAULT '',
    hub_listen_port   INTEGER NOT NULL DEFAULT 51820 CHECK (hub_listen_port BETWEEN 1 AND 65535),
    status            status_enum NOT NULL DEFAULT 'enabled',
    created_by        UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at        TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_vpn_network_name_active ON vpn_network(name) WHERE deleted_at IS NULL;
CREATE INDEX idx_vpn_network_status_active ON vpn_network(status) WHERE deleted_at IS NULL;

CREATE TABLE vpn_peer (
    id                UUID PRIMARY KEY,
    network_id        UUID NOT NULL REFERENCES vpn_network(id) ON DELETE CASCADE,
    peer_type         VARCHAR(16) NOT NULL CHECK (peer_type IN ('windows', 'edge')),
    edge_node_id      UUID REFERENCES edge_node(id) ON DELETE CASCADE,
    user_id           UUID REFERENCES sys_user(id) ON DELETE CASCADE,
    name              VARCHAR(100) NOT NULL,
    public_key        VARCHAR(64) NOT NULL DEFAULT '',
    assigned_ipv4     INET NOT NULL,
    allowed_routes    JSONB NOT NULL DEFAULT '[]'::jsonb
                      CHECK (jsonb_typeof(allowed_routes) = 'array'),
    status            VARCHAR(16) NOT NULL DEFAULT 'active'
                      CHECK (status IN ('pending', 'active', 'revoked')),
    config_revision   BIGINT NOT NULL DEFAULT 1,
    last_handshake_at TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at        TIMESTAMPTZ,
    CONSTRAINT ck_vpn_peer_subject CHECK (
        (peer_type = 'edge' AND edge_node_id IS NOT NULL AND user_id IS NULL)
        OR (peer_type = 'windows' AND edge_node_id IS NULL AND user_id IS NOT NULL)
    )
);
CREATE UNIQUE INDEX idx_vpn_peer_network_ip ON vpn_peer(network_id, assigned_ipv4);
CREATE UNIQUE INDEX idx_vpn_peer_public_key ON vpn_peer(public_key) WHERE public_key <> '';
CREATE INDEX idx_vpn_peer_network_status ON vpn_peer(network_id, status);
CREATE INDEX idx_vpn_peer_edge ON vpn_peer(edge_node_id) WHERE edge_node_id IS NOT NULL;
CREATE INDEX idx_vpn_peer_user ON vpn_peer(user_id) WHERE user_id IS NOT NULL;

CREATE TABLE vpn_route (
    id                UUID PRIMARY KEY,
    network_id        UUID NOT NULL REFERENCES vpn_network(id) ON DELETE CASCADE,
    edge_peer_id      UUID NOT NULL REFERENCES vpn_peer(id) ON DELETE CASCADE,
    lan_interface     VARCHAR(32) NOT NULL,
    target_cidr       VARCHAR(18) NOT NULL,
    virtual_cidr      VARCHAR(18) NOT NULL,
    mode              VARCHAR(16) NOT NULL DEFAULT 'nat' CHECK (mode IN ('nat', 'routed')),
    nat_mode          VARCHAR(16) NOT NULL DEFAULT 'masquerade'
                      CHECK (nat_mode IN ('masquerade', 'none')),
    status            VARCHAR(16) NOT NULL DEFAULT 'active'
                      CHECK (status IN ('active', 'error', 'disabled')),
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    last_error        TEXT NOT NULL DEFAULT '',
    created_by        UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_vpn_route_mode_nat CHECK (
        (mode = 'nat' AND nat_mode = 'masquerade') OR
        (mode = 'routed' AND nat_mode = 'none')
    ),
    UNIQUE (network_id, virtual_cidr),
    UNIQUE (edge_peer_id, target_cidr)
);
CREATE INDEX idx_vpn_route_network ON vpn_route(network_id, enabled);
CREATE INDEX idx_vpn_route_edge_peer ON vpn_route(edge_peer_id, enabled);

CREATE TABLE vpn_access_rule (
    id                UUID PRIMARY KEY,
    peer_id           UUID REFERENCES vpn_peer(id) ON DELETE CASCADE,
    user_id           UUID REFERENCES sys_user(id) ON DELETE CASCADE,
    role_id           UUID REFERENCES sys_role(id) ON DELETE CASCADE,
    route_id          UUID NOT NULL REFERENCES vpn_route(id) ON DELETE CASCADE,
    protocol          VARCHAR(8) NOT NULL DEFAULT 'any'
                      CHECK (protocol IN ('any', 'tcp', 'udp')),
    port_range        VARCHAR(32) NOT NULL DEFAULT '',
    action            VARCHAR(8) NOT NULL CHECK (action IN ('allow', 'deny')),
    priority          INTEGER NOT NULL DEFAULT 100 CHECK (priority >= 0),
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    created_by        UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_vpn_access_subject CHECK (num_nonnulls(peer_id, user_id, role_id) = 1)
);
CREATE INDEX idx_vpn_access_route ON vpn_access_rule(route_id, enabled, priority);

CREATE TABLE vpn_enrollment (
    id                UUID PRIMARY KEY,
    token_hash        VARCHAR(64) NOT NULL UNIQUE,
    network_id        UUID NOT NULL REFERENCES vpn_network(id) ON DELETE CASCADE,
    allowed_routes    JSONB NOT NULL DEFAULT '[]'::jsonb
                      CHECK (jsonb_typeof(allowed_routes) = 'array'),
    expires_at        TIMESTAMPTZ NOT NULL,
    used_at           TIMESTAMPTZ,
    created_by        UUID NOT NULL REFERENCES sys_user(id) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_vpn_enrollment_active ON vpn_enrollment(network_id, expires_at)
    WHERE used_at IS NULL;
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0030_vpn_iot_server_default", .sql_ = R"sql(
DO $schema$
BEGIN
INSERT INTO vpn_network(id, name, overlay_cidr, hub_public_key, hub_endpoint,
                        hub_listen_port, created_by)
VALUES ('00000000-0000-7000-8000-000000000004', 'iot-server', '100.96.0.0/16',
        '', '', 51820, '00000000-0000-7000-8000-000000000002')
ON CONFLICT (id) DO UPDATE
SET name = 'iot-server', overlay_cidr = '100.96.0.0/16', deleted_at = NULL,
    updated_at = NOW();
END
$schema$;
)sql" }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.add_column("vpn_network", { .name_ = "hub_private_key", .type_ = { .data_type_ = Type::varchar, .length_ = 44 }, .default_value_ = q.value("") }, true);
            auto result = schema.compile("0031_vpn_hub_database_config");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "edge_node_group", .columns_ = { { .name_ = "id", .type_ = { .data_type_ = Type::uuid }, .primary_key_ = true }, { .name_ = "name", .type_ = { .data_type_ = Type::varchar, .length_ = 100 } }, { .name_ = "parent_id", .type_ = { .data_type_ = Type::uuid }, .nullable_ = true }, { .name_ = "status", .type_ = { .custom_name_ = "status_enum" }, .default_value_ = q.value("enabled") }, { .name_ = "sort_order", .type_ = { .data_type_ = Type::integer }, .default_value_ = q.value(0) }, { .name_ = "remark", .type_ = { .data_type_ = Type::varchar, .length_ = 500 }, .nullable_ = true }, { .name_ = "created_by", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "created_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") }, { .name_ = "updated_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") }, { .name_ = "deleted_at", .type_ = { .data_type_ = Type::timestamp_tz }, .nullable_ = true } }, .constraints_ = { { .name_ = "edge_node_group_parent_id_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "parent_id" }, .referenced_table_ = "edge_node_group", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::restrict }, { .name_ = "edge_node_group_created_by_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "created_by" }, .referenced_table_ = "sys_user", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::restrict }, { .name_ = "edge_node_group_sort_order_check", .check_ = q.binary(q.column("sort_order"), ruvia::db_binary_operator::greater_equal, q.value(0)) } } });
            schema.create_index({ .name_ = "idx_edge_node_group_name_active", .table_ = "edge_node_group", .keys_ = { { .column_ = "name" } }, .unique_ = true, .where_ = q.unary(ruvia::db_unary_operator::is_null, q.column("deleted_at")) });
            schema.create_index({ .name_ = "idx_edge_node_group_parent", .table_ = "edge_node_group", .keys_ = { { .column_ = "parent_id" }, { .column_ = "sort_order" } }, .where_ = q.unary(ruvia::db_unary_operator::is_null, q.column("deleted_at")) });
            schema.add_column("edge_node", { .name_ = "group_id", .type_ = { .data_type_ = Type::uuid }, .nullable_ = true });
            schema.add_constraint("edge_node", { .name_ = "edge_node_group_id_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "group_id" }, .referenced_table_ = "edge_node_group", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::restrict });
            schema.create_index({ .name_ = "idx_edge_node_group_id", .table_ = "edge_node", .keys_ = { { .column_ = "group_id" } } });
            auto result = schema.compile("0032_edge_node_group");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.add_column("vpn_peer", { .name_ = "client_private_key", .type_ = { .data_type_ = Type::varchar, .length_ = 44 }, .default_value_ = q.value("") }, true);
            auto result = schema.compile("0033_vpn_client_repeat_download");
            return std::move(result.front());
        }(),
        ruvia::db_migration({ .id_ = "0034_device_address_scope", .sql_ = R"sql(
DO $schema$ BEGIN

DROP INDEX idx_device_protocol_params_code;
UPDATE device d SET protocol_params=jsonb_set(d.protocol_params,'{device_code}',
 to_jsonb(lpad(d.protocol_params->>'device_code',10,'0')))
FROM protocol_config p WHERE p.id=d.protocol_config_id AND p.protocol='SL651'
 AND d.protocol_params->>'device_code' ~ '^[0-9]{1,9}$';
CREATE UNIQUE INDEX idx_device_link_code_active
 ON device(link_id, (protocol_params->>'device_code'))
 WHERE deleted_at IS NULL AND COALESCE(protocol_params->>'device_code', '') <> '';

END $schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0035_durable_commands", .sql_ = R"sql(
DO $schema$ BEGIN

UPDATE open_webhook SET event_types=(
 SELECT jsonb_agg(CASE value
 WHEN 'device.command.dispatched' THEN 'device.command.accepted'
 WHEN 'device.command.responded' THEN 'device.command.updated' ELSE value END)
 FROM jsonb_array_elements_text(event_types))
 WHERE event_types ?| ARRAY['device.command.dispatched','device.command.responded'];
CREATE TABLE command_request (
 id UUID PRIMARY KEY, actor TEXT NOT NULL, idempotency_key UUID NOT NULL,
 device_id UUID NOT NULL REFERENCES device(id), payload JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(actor, idempotency_key)
);
CREATE TABLE command_operation (
 id UUID PRIMARY KEY, request_id UUID NOT NULL REFERENCES command_request(id),
 ordinal INTEGER NOT NULL, device_id UUID NOT NULL REFERENCES device(id),
 device_code TEXT NOT NULL, protocol TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('ACCEPTED','DISPATCHING','AWAITING_RESULT',
 'SUCCEEDED','REJECTED','UNKNOWN','READBACK_MISMATCH','FAILED')),
 reason TEXT NOT NULL DEFAULT '', elements JSONB NOT NULL DEFAULT '[]',
 actual_values JSONB NOT NULL DEFAULT '[]',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), completed_at TIMESTAMPTZ,
 UNIQUE(request_id, ordinal)
);
CREATE TABLE command_attempt (
 operation_id UUID PRIMARY KEY REFERENCES command_operation(id),
 queue_key TEXT NOT NULL, queue_kind TEXT NOT NULL CHECK(queue_kind IN ('stream','list')),
 payload JSONB NOT NULL, submitted_by TEXT NOT NULL, node_id TEXT NOT NULL DEFAULT '',
 max_length INTEGER NOT NULL CHECK(max_length > 0),
 claimed_at TIMESTAMPTZ, dispatched_at TIMESTAMPTZ, sent_at TIMESTAMPTZ,
 deadline TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '60 seconds'
);
CREATE INDEX idx_command_attempt_pending ON command_attempt(deadline) WHERE claimed_at IS NULL;
CREATE INDEX idx_command_operation_device ON command_operation(device_id, created_at DESC);

END $schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0036_live_query_changes", .sql_ = R"sql(
DO $schema$ BEGIN
CREATE FUNCTION publish_query_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
  VALUES(gen_random_uuid(),'query.changed',TG_ARGV[0],TG_TABLE_NAME,TG_OP,1);
  RETURN NULL;
END $fn$;
CREATE TRIGGER live_users AFTER INSERT OR UPDATE OR DELETE ON sys_user
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_roles AFTER INSERT OR UPDATE OR DELETE ON sys_role
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_membership AFTER INSERT OR UPDATE OR DELETE ON sys_user_role
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_departments AFTER INSERT OR UPDATE OR DELETE ON sys_department
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_device AFTER INSERT OR UPDATE OR DELETE ON device
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('device');
CREATE TRIGGER live_device_group AFTER INSERT OR UPDATE OR DELETE ON device_group
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('device');
CREATE TRIGGER live_device_access_grant AFTER INSERT OR UPDATE OR DELETE ON device_access_grant
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_device_group_access_grant AFTER INSERT OR UPDATE OR DELETE ON device_group_access_grant
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_open_access_key AFTER INSERT OR UPDATE OR DELETE ON open_access_key
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_open_access_key_device AFTER INSERT OR UPDATE OR DELETE ON open_access_key_device
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_link AFTER INSERT OR UPDATE OR DELETE ON link
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('link');
CREATE TRIGGER live_protocol_config AFTER INSERT OR UPDATE OR DELETE ON protocol_config
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('protocol');
CREATE TRIGGER live_edge_node AFTER INSERT OR UPDATE OR DELETE ON edge_node
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_edge_node_interface AFTER INSERT OR UPDATE OR DELETE ON edge_node_interface
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_edge_node_serial AFTER INSERT OR UPDATE OR DELETE ON edge_node_serial
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_edge_firmware AFTER INSERT OR UPDATE OR DELETE ON edge_firmware
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_edge_node_platform AFTER INSERT OR UPDATE OR DELETE ON edge_node_platform
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_edge_task AFTER INSERT OR UPDATE OR DELETE ON edge_task
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_edge_config_revision AFTER INSERT OR UPDATE OR DELETE ON edge_config_revision
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_edge_node_network AFTER INSERT OR UPDATE OR DELETE ON edge_node_network
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_edge_node_group AFTER INSERT OR UPDATE OR DELETE ON edge_node_group
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge');
CREATE TRIGGER live_alert_rule AFTER INSERT OR UPDATE OR DELETE ON alert_rule
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('alert');
CREATE TRIGGER live_alert_rule_template AFTER INSERT OR UPDATE OR DELETE ON alert_rule_template
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('alert');
CREATE TRIGGER live_open_alert_record AFTER INSERT OR UPDATE OR DELETE ON open_alert_record
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('alert');
CREATE TRIGGER live_open_webhook AFTER INSERT OR UPDATE OR DELETE ON open_webhook
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('access');
CREATE TRIGGER live_open_access_log AFTER INSERT OR UPDATE OR DELETE ON open_access_log
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('access');
CREATE TRIGGER live_vpn_network AFTER INSERT OR UPDATE OR DELETE ON vpn_network
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('vpn');
CREATE TRIGGER live_vpn_peer AFTER INSERT OR UPDATE OR DELETE ON vpn_peer
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('vpn');
CREATE TRIGGER live_vpn_route AFTER INSERT OR UPDATE OR DELETE ON vpn_route
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('vpn');
CREATE TRIGGER live_vpn_access_rule AFTER INSERT OR UPDATE OR DELETE ON vpn_access_rule
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('vpn');
CREATE TRIGGER live_vpn_enrollment AFTER INSERT OR UPDATE OR DELETE ON vpn_enrollment
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('vpn');
CREATE TRIGGER live_gb28181_device AFTER INSERT OR UPDATE OR DELETE ON gb28181_device
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('gb28181');
CREATE TRIGGER live_gb28181_channel AFTER INSERT OR UPDATE OR DELETE ON gb28181_channel
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('gb28181');
CREATE TRIGGER live_gb28181_record AFTER INSERT OR UPDATE OR DELETE ON gb28181_record
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('gb28181');
CREATE TRIGGER live_gb28181_stream AFTER INSERT OR UPDATE OR DELETE ON gb28181_stream
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('gb28181');
CREATE TRIGGER live_command_operation AFTER INSERT OR UPDATE OR DELETE ON command_operation
  FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('command');
END $schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0037_model_revisions", .sql_ = std::string(kModelRevisionMigration) }),
        ruvia::db_migration({ .id_ = "0038_physical_channels", .sql_ = std::string(kChannelMigration) }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "alert_input_state", .columns_ = { { .name_ = "device_id", .type_ = { .data_type_ = Type::uuid }, .primary_key_ = true }, { .name_ = "observed_at_ms", .type_ = { .data_type_ = Type::big_int } }, { .name_ = "message_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "data", .type_ = { .data_type_ = Type::jsonb } }, { .name_ = "previous_data", .type_ = { .data_type_ = Type::jsonb } } }, .constraints_ = { { .name_ = "alert_input_state_device_id_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "device_id" }, .referenced_table_ = "device", .referenced_columns_ = { "id" } } } });
            q.from("device_data_ingest_state").select({ q.column("device_id"), q.cast(q.binary(q.extract(ruvia::db_date_part::epoch, q.column("last_observed_at")), ruvia::db_binary_operator::multiply, q.value(1000)), Type::big_int), q.column("last_observed_id"), q.coalesce({ q.column("last_data"), q.cast(q.value("{}"), Type::jsonb) }), q.coalesce({ q.column("previous_data"), q.cast(q.value("{}"), Type::jsonb) }) }).where(q.unary(ruvia::db_unary_operator::is_not_null, q.column("last_observed_at"))).and_where(q.unary(ruvia::db_unary_operator::is_not_null, q.column("last_observed_id")));
            ruvia::db_query insert;
            insert.insert_into("alert_input_state", { "device_id", "observed_at_ms", "message_id", "data", "previous_data" }).insert_from(q);
            schema.execute(insert);
            auto result = schema.compile("0039_alert_input_state");
            return std::move(result.front());
        }(),
        ruvia::db_migration({ .id_ = "0040_channel_transport_guard", .sql_ = R"sql(
CREATE OR REPLACE FUNCTION protect_channel_binding() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF (NEW.protocol,NEW.execution,NEW.edge_node_id,NEW.deleted_at,
      NEW.endpoint->>'transport',NEW.endpoint->>'mode') IS DISTINCT FROM
     (OLD.protocol,OLD.execution,OLD.edge_node_id,OLD.deleted_at,
      OLD.endpoint->>'transport',OLD.endpoint->>'mode')
     AND EXISTS(SELECT 1 FROM device WHERE link_id=OLD.id AND deleted_at IS NULL) THEN
    RAISE EXCEPTION 'Move or remove bound devices before changing channel identity';
  END IF;
  RETURN NEW;
END $fn$;)sql" }),
        ruvia::db_migration({ .id_ = "0041_outbox_notifications", .sql_ = R"sql(
DO $schema$ BEGIN
CREATE FUNCTION notify_outbox_pending() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  PERFORM pg_notify('iot_outbox_pending', '');
  RETURN NULL;
END $fn$;
CREATE TRIGGER outbox_pending_notification
AFTER INSERT OR UPDATE OF available_at, published_at, dead_lettered_at ON outbox_event
FOR EACH ROW
WHEN (NEW.published_at IS NULL AND NEW.dead_lettered_at IS NULL)
EXECUTE FUNCTION notify_outbox_pending();
END $schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0042_vpn_desktop_selection", .sql_ = R"sql(
DO $schema$
BEGIN
ALTER TABLE vpn_peer ADD COLUMN IF NOT EXISTS client_managed BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE IF NOT EXISTS vpn_peer_edge_selection (
    peer_id UUID NOT NULL REFERENCES vpn_peer(id) ON DELETE CASCADE,
    edge_node_id UUID NOT NULL REFERENCES edge_node(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (peer_id, edge_node_id)
);
CREATE INDEX IF NOT EXISTS idx_vpn_peer_edge_selection_edge
    ON vpn_peer_edge_selection(edge_node_id);
CREATE OR REPLACE FUNCTION vpn_peer_edge_selection_revision() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  UPDATE vpn_peer SET config_revision = config_revision + 1, updated_at = NOW()
  WHERE id = COALESCE(NEW.peer_id, OLD.peer_id);
  IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $fn$;
DROP TRIGGER IF EXISTS vpn_peer_edge_selection_revision ON vpn_peer_edge_selection;
CREATE TRIGGER vpn_peer_edge_selection_revision
AFTER INSERT OR UPDATE OR DELETE ON vpn_peer_edge_selection
FOR EACH ROW EXECUTE FUNCTION vpn_peer_edge_selection_revision();
CREATE FUNCTION vpn_desktop_user_authorized(subject_id UUID) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $fn$
SELECT NOT EXISTS (
  SELECT 1 FROM (VALUES ('iot:vpn:query'), ('iot:vpn:enroll'), ('iot:edge:query')) required(permission)
  WHERE NOT EXISTS (
    SELECT 1 FROM sys_user_role membership
    JOIN sys_user account ON account.id = membership.user_id
    JOIN sys_role role ON role.id = membership.role_id
    WHERE membership.user_id = subject_id
      AND account.status = 'enabled' AND account.deleted_at IS NULL
      AND role.status = 'enabled' AND role.deleted_at IS NULL
      AND (role.code = 'superadmin' OR role.permissions ? '*' OR role.permissions ? required.permission)
  )
);
$fn$;
-- Shared by config snapshots and both Hub reconcilers. The managed-client
-- restriction is additive; legacy peers retain their existing network scope.
CREATE VIEW vpn_effective_edge_access AS
SELECT source.id AS peer_id, source.network_id, edge_peer.id AS edge_peer_id,
       edge.id AS edge_node_id, host(edge_peer.assigned_ipv4) AS edge_address
FROM vpn_peer source
JOIN vpn_network network ON network.id = source.network_id
JOIN vpn_peer edge_peer ON edge_peer.network_id = source.network_id
  AND edge_peer.peer_type = 'edge' AND edge_peer.status = 'active'
JOIN edge_node edge ON edge.id = edge_peer.edge_node_id AND edge.enrollment_status = 'approved'
WHERE source.status = 'active' AND network.status = 'enabled' AND network.deleted_at IS NULL
  AND (NOT source.client_managed OR (
    vpn_desktop_user_authorized(source.user_id) AND EXISTS (
      SELECT 1 FROM vpn_peer_edge_selection selection
      WHERE selection.peer_id = source.id AND selection.edge_node_id = edge.id
    )
  ));
CREATE VIEW vpn_effective_route_access AS
SELECT access.peer_id, route.virtual_cidr
FROM vpn_effective_edge_access access
JOIN vpn_route route ON route.edge_peer_id = access.edge_peer_id
  AND route.network_id = access.network_id AND route.enabled AND route.status = 'active';
END
$schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0043_nonempty_query_notifications", .sql_ = R"sql(
DO $schema$
DECLARE source RECORD; operation TEXT; transition_kind TEXT;
BEGIN
CREATE OR REPLACE FUNCTION publish_query_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM query_changed_rows) THEN RETURN NULL; END IF;
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
  VALUES(gen_random_uuid(),'query.changed',TG_ARGV[0],TG_TABLE_NAME,TG_OP,1);
  RETURN NULL;
END $fn$;
-- Separate transition-table triggers preserve one hint per statement without
-- reporting zero-row UPDATE/DELETE scans as changes.
FOR source IN
  SELECT t.tgname, c.relname, n.nspname,
    convert_from(substring(t.tgargs FROM 1 FOR
      position(decode('00','hex') IN t.tgargs)-1),'UTF8') AS topic
  FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE t.tgfoid='publish_query_change()'::regprocedure
    AND NOT t.tgisinternal AND (t.tgtype::integer & 28)=28
LOOP
  EXECUTE format('DROP TRIGGER %I ON %I.%I',source.tgname,source.nspname,source.relname);
  FOREACH operation IN ARRAY ARRAY['INSERT','UPDATE','DELETE'] LOOP
    transition_kind := CASE WHEN operation='DELETE' THEN 'OLD' ELSE 'NEW' END;
    EXECUTE format('CREATE TRIGGER %I AFTER %s ON %I.%I REFERENCING %s TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change(%L)',
      source.tgname || '_' || lower(operation),operation,source.nspname,source.relname,transition_kind,source.topic);
  END LOOP;
END LOOP;
CREATE TRIGGER command_pending_notification
AFTER INSERT OR UPDATE OF deadline ON command_attempt
FOR EACH ROW EXECUTE FUNCTION notify_outbox_pending();
END $schema$;
)sql" }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            for (const auto table : { "gb28181_device", "gb28181_stream" }) {
                schema.add_column(table, { .name_ = "projection_cursor", .type_ = { .data_type_ = Type::numeric, .precision_ = 40 }, .default_value_ = q.value(0) });
            }
            auto result = schema.compile("0044_gb28181_projection_cursor");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.drop_index("idx_vpn_peer_network_ip");
            schema.create_index({ .name_ = "idx_vpn_peer_network_ip", .table_ = "vpn_peer", .keys_ = { { .column_ = "network_id" }, { .column_ = "assigned_ipv4" } }, .unique_ = true, .where_ = q.binary(q.binary(q.column("peer_type"), ruvia::db_binary_operator::not_equal, q.value("windows")), ruvia::db_binary_operator::or_value, q.binary(q.column("status"), ruvia::db_binary_operator::not_equal, q.value("revoked"))) });
            auto result = schema.compile("0045_vpn_windows_address_reuse");
            return std::move(result.front());
        }(),
        ruvia::db_migration({ .id_ = "0046_current_device_model", .sql_ = std::string(kCurrentDeviceModelMigration) }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "outbox_replay_counter", .columns_ = { { .name_ = "id", .type_ = { .data_type_ = Type::text } }, { .name_ = "replays", .type_ = { .data_type_ = Type::big_int }, .default_value_ = q.value(0) }, { .name_ = "updated_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") } }, .constraints_ = { { .name_ = "outbox_replay_counter_pkey", .kind_ = ruvia::db_constraint_kind::primary_key, .columns_ = { "id" } } } });
            auto result = schema.compile("0047_outbox_replay_counter");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            for (const auto table : { "device", "link" }) {
                schema.add_column(table, { .name_ = "debug_enabled", .type_ = { .data_type_ = Type::boolean }, .default_value_ = q.value(false) });
            }
            auto result = schema.compile("0048_packet_debug_switches");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            for (const auto table : { "link", "protocol_config", "device_data" }) {
                const auto name = std::string(table) + "_protocol_check";
                schema.drop_constraint(table, name);
                schema.add_constraint(table, { .name_ = name, .kind_ = ruvia::db_constraint_kind::check, .check_ = q.binary(q.column("protocol"), ruvia::db_binary_operator::in, q.list({ q.value("SL651"), q.value("Modbus"), q.value("S7"), q.value("MC"), q.value("FINS"), q.value("DLT645") })) });
            }
            auto statements = schema.compile("0049_industrial_protocols");
            std::string sql = "DO $industrial$ BEGIN\n";
            for (const auto& statement : statements) {
                sql += "EXECUTE $ddl$" + std::string(statement.sql()) + "$ddl$;\n";
            }
            sql += "EXECUTE $ddl$" + std::string(kIndustrialProtocolAddressMigration) + "$ddl$;\nEND $industrial$;";
            return ruvia::db_migration({ .id_ = "0049_industrial_protocols", .sql_ = std::move(sql) });
        }(),
        ruvia::db_migration({ .id_ = "0050_dead_letter_query_changes", .sql_ = R"sql(
DO $schema$ BEGIN
CREATE FUNCTION publish_dead_letter_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.published_at IS NOT NULL OR NEW.dead_lettered_at IS NULL THEN RETURN NULL; END IF;
  ELSIF TG_OP = 'DELETE' THEN
    IF OLD.published_at IS NOT NULL OR OLD.dead_lettered_at IS NULL THEN RETURN NULL; END IF;
  ELSE
    IF NOT ((OLD.published_at IS NULL AND OLD.dead_lettered_at IS NOT NULL)
         OR (NEW.published_at IS NULL AND NEW.dead_lettered_at IS NOT NULL)) THEN RETURN NULL; END IF;
    IF OLD IS NOT DISTINCT FROM NEW THEN RETURN NULL; END IF;
  END IF;
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
  VALUES(gen_random_uuid(),'query.changed','system','outbox_event',TG_OP,1);
  RETURN NULL;
END $fn$;
CREATE TRIGGER live_dead_letters AFTER INSERT OR UPDATE OR DELETE ON outbox_event
FOR EACH ROW EXECUTE FUNCTION publish_dead_letter_change();
END $schema$;
)sql" }),
        ruvia::db_migration({ .id_ = "0051_edge_dtu", .sql_ = R"sql(
DO $schema$ BEGIN
CREATE TABLE edge_dtu (
    node_id UUID NOT NULL REFERENCES edge_node(id) ON DELETE CASCADE,
    channel_id UUID NOT NULL,
    config JSONB NOT NULL CHECK (jsonb_typeof(config) = 'object'),
    wire_hex TEXT NOT NULL CHECK (wire_hex ~ '^[0-9a-f]+$'),
    status JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (node_id, channel_id)
);
CREATE FUNCTION publish_edge_dtu_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF TG_OP = 'UPDATE' AND OLD IS NOT DISTINCT FROM NEW THEN RETURN NULL; END IF;
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
  VALUES(gen_random_uuid(),'query.changed','edge-dtu:' || COALESCE(NEW.node_id,OLD.node_id)::text,
    COALESCE(NEW.channel_id,OLD.channel_id)::text,TG_OP,1);
  RETURN NULL;
END $fn$;
CREATE TRIGGER live_edge_dtu AFTER INSERT OR UPDATE OR DELETE ON edge_dtu
FOR EACH ROW EXECUTE FUNCTION publish_edge_dtu_change();
END $schema$;
)sql" }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            for (const auto table : { "link", "protocol_config", "device_data" }) {
                const auto name = std::string(table) + "_protocol_check";
                schema.drop_constraint(table, name);
                schema.add_constraint(table, { .name_ = name, .kind_ = ruvia::db_constraint_kind::check, .check_ = q.binary(q.column("protocol"), ruvia::db_binary_operator::in, q.list({ q.value("SL651"), q.value("Modbus"), q.value("S7"), q.value("MC"), q.value("FINS"), q.value("DLT645"), q.value("MQTT") })) });
            }
            auto statements = schema.compile("0052_mqtt_client");
            std::string sql = "DO $mqtt$ BEGIN\n";
            for (const auto& statement : statements) {
                sql += "EXECUTE $ddl$" + std::string(statement.sql()) + "$ddl$;\n";
            }
            sql += "EXECUTE $ddl$ALTER TABLE link ADD CONSTRAINT link_mqtt_client_check CHECK (protocol <> 'MQTT' OR (execution = 'collector' AND endpoint->>'mode' = 'TCP Client') IS TRUE)$ddl$;\n";
            sql += "EXECUTE $ddl$" + std::string(kMqttDeviceAddressMigration) + "$ddl$;\nEND $mqtt$;";
            return ruvia::db_migration({ .id_ = "0052_mqtt_client", .sql_ = std::move(sql) });
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "open_mqtt", .columns_ = { { .name_ = "id", .type_ = { .data_type_ = Type::uuid }, .primary_key_ = true }, { .name_ = "access_key_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "name", .type_ = { .data_type_ = Type::varchar, .length_ = 64 } }, { .name_ = "broker_url", .type_ = { .data_type_ = Type::text } }, { .name_ = "username", .type_ = { .data_type_ = Type::text }, .nullable_ = true }, { .name_ = "password", .type_ = { .data_type_ = Type::text }, .nullable_ = true }, { .name_ = "status", .type_ = { .data_type_ = Type::text }, .default_value_ = q.value("enabled") }, { .name_ = "event_types", .type_ = { .data_type_ = Type::jsonb }, .default_value_ = q.cast(q.value("[\"device.data.reported\"]"), Type::jsonb) }, { .name_ = "timeout_seconds", .type_ = { .data_type_ = Type::integer }, .default_value_ = q.value(5) }, { .name_ = "last_triggered_at", .type_ = { .data_type_ = Type::timestamp_tz }, .nullable_ = true }, { .name_ = "last_success_at", .type_ = { .data_type_ = Type::timestamp_tz }, .nullable_ = true }, { .name_ = "last_failure_at", .type_ = { .data_type_ = Type::timestamp_tz }, .nullable_ = true }, { .name_ = "last_error", .type_ = { .data_type_ = Type::text }, .nullable_ = true }, { .name_ = "created_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") }, { .name_ = "updated_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = q.call("now") }, { .name_ = "deleted_at", .type_ = { .data_type_ = Type::timestamp_tz }, .nullable_ = true } }, .constraints_ = { { .name_ = "open_mqtt_access_key_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "access_key_id" }, .referenced_table_ = "open_access_key", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::cascade }, { .name_ = "open_mqtt_status_check", .kind_ = ruvia::db_constraint_kind::check, .check_ = q.binary(q.column("status"), ruvia::db_binary_operator::in, q.list({ q.value("enabled"), q.value("disabled") })) }, { .name_ = "open_mqtt_timeout_check", .kind_ = ruvia::db_constraint_kind::check, .check_ = q.binary(q.binary(q.column("timeout_seconds"), ruvia::db_binary_operator::greater_equal, q.value(1)), ruvia::db_binary_operator::and_value, q.binary(q.column("timeout_seconds"), ruvia::db_binary_operator::less_equal, q.value(30))) } } });
            schema.create_index({ .name_ = "idx_open_mqtt_name_active", .table_ = "open_mqtt", .keys_ = { { .column_ = "access_key_id" }, { .column_ = "name" } }, .unique_ = true, .where_ = q.unary(ruvia::db_unary_operator::is_null, q.column("deleted_at")) });
            schema.add_column("open_access_log", { .name_ = "mqtt_id", .type_ = { .data_type_ = Type::uuid }, .nullable_ = true });
            schema.add_constraint("open_access_log", { .name_ = "open_access_log_mqtt_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "mqtt_id" }, .referenced_table_ = "open_mqtt", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::set_null });
            auto result = schema.compile("0053_open_mqtt");
            return std::move(result.front());
        }(),
        ruvia::db_migration({ .id_ = "0054_native_point_calculations", .sql_ = std::string(kNativePointCalculationsMigration) }),
        ruvia::db_migration({ .id_ = "0055_device_parameters", .sql_ = std::string(kDeviceParametersMigration) }),
        ruvia::db_migration({ .id_ = "0056_device_parameter_definitions", .sql_ = std::string(kDeviceParameterDefinitionsMigration) }),
        ruvia::db_migration({ .id_ = "0057_entity_live_changes", .sql_ = R"sql(
DO $schema$
DECLARE source RECORD; operation TEXT; transition_kind TEXT; target_topic TEXT; id_column TEXT;
BEGIN
-- TG_ARGV[0] = live topic, TG_ARGV[1] = optional id column. aggregate_id carries
-- the distinct changed ids (comma separated, schema 2); more than 1000 ids or
-- no id column means the whole topic changed (empty aggregate_id).
CREATE OR REPLACE FUNCTION publish_query_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE changed BIGINT; identified BIGINT; distinct_ids BIGINT; ids TEXT;
BEGIN
  IF TG_NARGS > 1 THEN
    EXECUTE format('SELECT count(*), count(%I), count(DISTINCT %I), string_agg(DISTINCT %I::text, '','') FROM query_changed_rows',
        TG_ARGV[1], TG_ARGV[1], TG_ARGV[1])
      INTO changed, identified, distinct_ids, ids;
    IF changed = 0 THEN RETURN NULL; END IF;
    -- Rows without an id or too many ids publish the whole topic.
    IF identified < changed OR distinct_ids > 1000 THEN ids := ''; END IF;
  ELSE
    IF NOT EXISTS (SELECT 1 FROM query_changed_rows) THEN RETURN NULL; END IF;
    ids := '';
  END IF;
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
  VALUES(gen_random_uuid(),'query.changed',TG_ARGV[0],COALESCE(ids,''),TG_OP,2);
  RETURN NULL;
END $fn$;
CREATE TEMPORARY TABLE live_topic_mapping(relname TEXT PRIMARY KEY, topic TEXT NOT NULL, id_column TEXT) ON COMMIT DROP;
INSERT INTO live_topic_mapping VALUES
  ('sys_user','auth',NULL),('sys_role','auth',NULL),('sys_user_role','auth',NULL),
  ('sys_department','auth',NULL),('device_access_grant','auth',NULL),
  ('device_group_access_grant','auth',NULL),('open_access_key','auth',NULL),
  ('open_access_key_device','auth',NULL),
  ('device','device','id'),('device_group','device-group','id'),
  ('link','link','id'),('protocol_config','protocol','id'),
  ('edge_node','edge','id'),('edge_node_interface','edge','node_id'),
  ('edge_node_serial','edge','node_id'),('edge_node_platform','edge','node_id'),
  ('edge_task','edge','node_id'),('edge_config_revision','edge','node_id'),
  ('edge_node_network','edge','node_id'),('edge_firmware','edge',NULL),
  ('edge_node_group','edge-group','id'),
  ('alert_rule','alert-rule','id'),('alert_rule_template','alert-rule',NULL),
  ('open_alert_record','alert','id'),
  ('open_webhook','access',NULL),('open_access_log','access',NULL),
  ('vpn_network','vpn','id'),('vpn_peer','vpn','network_id'),
  ('vpn_route','vpn','network_id'),('vpn_enrollment','vpn','network_id'),
  ('vpn_access_rule','vpn',NULL),
  ('gb28181_device','gb28181','id'),('gb28181_channel','gb28181','device_id'),
  ('gb28181_record','gb28181','device_id'),('gb28181_stream','gb28181',NULL),
  ('command_operation','command','id');
FOR source IN
  SELECT DISTINCT c.relname, n.nspname,
    convert_from(substring(t.tgargs FROM 1 FOR
      position(decode('00','hex') IN t.tgargs)-1),'UTF8') AS topic
  FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE t.tgfoid='publish_query_change()'::regprocedure AND NOT t.tgisinternal
LOOP
  SELECT m.topic, m.id_column INTO target_topic, id_column
    FROM live_topic_mapping m WHERE m.relname=source.relname;
  IF NOT FOUND THEN target_topic := source.topic; id_column := NULL; END IF;
  FOR operation IN
    SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
      JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE t.tgfoid='publish_query_change()'::regprocedure AND NOT t.tgisinternal
      AND c.relname=source.relname AND n.nspname=source.nspname
  LOOP
    EXECUTE format('DROP TRIGGER %I ON %I.%I',operation,source.nspname,source.relname);
  END LOOP;
  FOREACH operation IN ARRAY ARRAY['INSERT','UPDATE','DELETE'] LOOP
    transition_kind := CASE WHEN operation='DELETE' THEN 'OLD' ELSE 'NEW' END;
    IF id_column IS NULL THEN
      EXECUTE format('CREATE TRIGGER %I AFTER %s ON %I.%I REFERENCING %s TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change(%L)',
        'live_' || source.relname || '_' || lower(operation),operation,source.nspname,source.relname,transition_kind,target_topic);
    ELSE
      EXECUTE format('CREATE TRIGGER %I AFTER %s ON %I.%I REFERENCING %s TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change(%L,%L)',
        'live_' || source.relname || '_' || lower(operation),operation,source.nspname,source.relname,transition_kind,target_topic,id_column);
    END IF;
  END LOOP;
END LOOP;
CREATE OR REPLACE FUNCTION publish_edge_dtu_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF TG_OP = 'UPDATE' AND OLD IS NOT DISTINCT FROM NEW THEN RETURN NULL; END IF;
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
  VALUES(gen_random_uuid(),'query.changed','edge-dtu',COALESCE(NEW.node_id,OLD.node_id)::text,TG_OP,2);
  RETURN NULL;
END $fn$;
END $schema$;
)sql" }),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            for (const auto table : { "protocol_config", "device_data" }) {
                const auto name = std::string(table) + "_protocol_check";
                schema.drop_constraint(table, name);
                schema.add_constraint(table, { .name_ = name, .kind_ = ruvia::db_constraint_kind::check, .check_ = q.binary(q.column("protocol"), ruvia::db_binary_operator::in, q.list({ q.value("SL651"), q.value("Modbus"), q.value("S7"), q.value("MC"), q.value("FINS"), q.value("DLT645"), q.value("MQTT"), q.value("ExternalIO") })) });
            }
            auto result = schema.compile("0058_external_io_protocol");
            return std::move(result.front());
        }(),
        [] {
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            using Op = ruvia::db_binary_operator;
            schema.add_constraint("device", { .name_ = "device_id_link_key", .kind_ = ruvia::db_constraint_kind::unique, .columns_ = { "id", "link_id" } });
            schema.add_constraint("link", { .name_ = "link_id_node_key", .kind_ = ruvia::db_constraint_kind::unique, .columns_ = { "id", "edge_node_id" } });
            schema.create_table({ .name_ = "device_external_io", .columns_ = {
                                                                     { .name_ = "device_id", .type_ = { .data_type_ = Type::uuid }, .primary_key_ = true },
                                                                     { .name_ = "link_id", .type_ = { .data_type_ = Type::uuid } },
                                                                     { .name_ = "node_id", .type_ = { .data_type_ = Type::uuid } },
                                                                     { .name_ = "protocol_config_id", .type_ = { .data_type_ = Type::uuid } },
                                                                     { .name_ = "derived_config", .type_ = { .data_type_ = Type::jsonb }, .default_value_ = q.cast(q.value("{}"), Type::jsonb) },
                                                                 },
                                  .constraints_ = {
                                      { .name_ = "device_external_io_device_link_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "device_id", "link_id" }, .referenced_table_ = "device", .referenced_columns_ = { "id", "link_id" }, .on_delete_ = ruvia::db_referential_action::cascade, .on_update_ = ruvia::db_referential_action::cascade },
                                      { .name_ = "device_external_io_link_node_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "link_id", "node_id" }, .referenced_table_ = "link", .referenced_columns_ = { "id", "edge_node_id" }, .on_delete_ = ruvia::db_referential_action::restrict },
                                      { .name_ = "device_external_io_device_node_key", .kind_ = ruvia::db_constraint_kind::unique, .columns_ = { "device_id", "node_id" } },
                                      { .name_ = "device_external_io_protocol_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "protocol_config_id" }, .referenced_table_ = "protocol_config", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::restrict },
                                      { .name_ = "device_external_io_derived_config_check", .check_ = q.binary(q.call("jsonb_typeof", { q.column("derived_config") }), Op::equal, q.value("object")) },
                                  } });
            schema.create_table({ .name_ = "device_external_io_binding", .columns_ = {
                                                                             { .name_ = "device_id", .type_ = { .data_type_ = Type::uuid } },
                                                                             { .name_ = "point_id", .type_ = { .data_type_ = Type::uuid } },
                                                                             { .name_ = "node_id", .type_ = { .data_type_ = Type::uuid } },
                                                                             { .name_ = "kind", .type_ = { .data_type_ = Type::varchar, .length_ = 2 } },
                                                                             { .name_ = "channel", .type_ = { .data_type_ = Type::varchar, .length_ = 16 } },
                                                                         },
                                  .constraints_ = {
                                      { .name_ = "device_external_io_binding_pkey", .kind_ = ruvia::db_constraint_kind::primary_key, .columns_ = { "device_id", "point_id" } },
                                      { .name_ = "device_external_io_binding_device_node_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "device_id", "node_id" }, .referenced_table_ = "device_external_io", .referenced_columns_ = { "device_id", "node_id" }, .on_delete_ = ruvia::db_referential_action::cascade },
                                      { .name_ = "device_external_io_binding_node_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "node_id" }, .referenced_table_ = "edge_node", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::restrict },
                                      { .name_ = "device_external_io_binding_kind_check", .check_ = q.binary(q.column("kind"), Op::in, q.list({ q.value("DI"), q.value("DO"), q.value("AI") })) },
                                      { .name_ = "device_external_io_binding_channel_check", .check_ = q.binary(q.binary(q.binary(q.column("kind"), Op::equal, q.value("DI")), Op::and_value, q.binary(q.column("channel"), Op::regex, q.value("^di[0-9]+$"))), Op::or_value, q.binary(q.binary(q.binary(q.column("kind"), Op::equal, q.value("DO")), Op::and_value, q.binary(q.column("channel"), Op::regex, q.value("^do[0-9]+$"))), Op::or_value, q.binary(q.binary(q.column("kind"), Op::equal, q.value("AI")), Op::and_value, q.binary(q.column("channel"), Op::regex, q.value("^AI[0-9]+$"))))) },
                                  } });
            schema.create_index({ .name_ = "device_external_io_output_owner", .table_ = "device_external_io_binding", .keys_ = { { .column_ = "node_id" }, { .column_ = "channel" } }, .unique_ = true, .where_ = q.binary(q.column("kind"), Op::equal, q.value("DO")) });
            {
                ruvia::db_query source;
                source.select(source.value(1)).from("device", "d").join(ruvia::db_join_type::inner, "link", source.binary(source.column("id", "l"), Op::equal, source.column("link_id", "d")), "l").join(ruvia::db_join_type::inner, "protocol_config", source.binary(source.column("id", "p"), Op::equal, source.column("protocol_config_id", "new")), "p").where(source.binary(source.column("id", "d"), Op::equal, source.column("device_id", "new"))).and_where(source.binary(source.column("link_id", "d"), Op::equal, source.column("link_id", "new"))).and_where(source.binary(source.column("edge_node_id", "l"), Op::equal, source.column("node_id", "new"))).and_where(source.unary(ruvia::db_unary_operator::is_null, source.column("deleted_at", "d"))).and_where(source.unary(ruvia::db_unary_operator::is_null, source.column("deleted_at", "l"))).and_where(source.unary(ruvia::db_unary_operator::is_null, source.column("deleted_at", "p"))).and_where(source.binary(source.column("execution", "l"), Op::equal, source.value("edge"))).and_where(source.unary(ruvia::db_unary_operator::is_not_null, source.column("edge_node_id", "l"))).and_where(source.binary(source.column("protocol", "p"), Op::equal, source.value("ExternalIO"))).and_where(source.column("enabled", "p")).lock({ .mode_ = ruvia::db_row_lock::share });
                ruvia::db_query expression;
                ruvia::db_procedure guard;
                guard.begin_if(expression.unary(ruvia::db_unary_operator::not_value, expression.exists(source)));
                guard.raise_exception("external IO requires an active IO model and an edge device", "23514");
                guard.end_if();
                guard.return_value(expression.column("new"));
                schema.create_trigger_function("guard_device_external_io", guard);
                schema.create_trigger({ .name_ = "guard_device_external_io", .table_ = "device_external_io", .function_ = "guard_device_external_io", .events_ = { ruvia::db_trigger_event::insert, ruvia::db_trigger_event::update } });
            }
            {
                ruvia::db_query match;
                const auto entries = match.binary(match.column("config", "p"), Op::json_get, match.value("points"));
                const auto id = match.cast(match.binary(match.column("value", "point"), Op::json_get_text, match.value("id")), Type::uuid);
                const auto kind = match.binary(match.column("value", "point"), Op::json_get_text, match.value("kind"));
                const std::array<ruvia::db_source_column, 1> columns{ { { .name_ = "value" } } };
                const std::array<ruvia::borrowed_text, 4> locked{ "s", "d", "l", "p" };
                match.select(match.value(1)).from("device_external_io", "s").join(ruvia::db_join_type::inner, "device", match.binary(match.column("id", "d"), Op::equal, match.column("device_id", "s")), "d").join(ruvia::db_join_type::inner, "link", match.binary(match.column("id", "l"), Op::equal, match.column("link_id", "d")), "l").join(ruvia::db_join_type::inner, "protocol_config", match.binary(match.column("id", "p"), Op::equal, match.column("protocol_config_id", "s")), "p").join_function(ruvia::db_join_type::cross, match.call("jsonb_array_elements", { entries }), {}, "point", { .lateral_ = true, .columns_ = columns }).where(match.binary(match.column("device_id", "s"), Op::equal, match.column("device_id", "new"))).and_where(match.binary(match.column("edge_node_id", "l"), Op::equal, match.column("node_id", "new"))).and_where(match.binary(id, Op::equal, match.column("point_id", "new"))).and_where(match.binary(kind, Op::equal, match.column("kind", "new"))).lock({ .mode_ = ruvia::db_row_lock::share, .tables_ = locked });
                ruvia::db_query expression;
                ruvia::db_procedure guard;
                guard.begin_if(expression.unary(ruvia::db_unary_operator::not_value, expression.exists(match)));
                guard.raise_exception("external IO point or edge node does not match its device", "23514");
                guard.end_if();
                guard.return_value(expression.column("new"));
                schema.create_trigger_function("guard_device_external_io_binding", guard);
                schema.create_trigger({ .name_ = "guard_device_external_io_binding", .table_ = "device_external_io_binding", .function_ = "guard_device_external_io_binding", .events_ = { ruvia::db_trigger_event::insert, ruvia::db_trigger_event::update } });
            }
            {
                ruvia::db_query expression;
                ruvia::db_query bound;
                bound.select(bound.value(1)).from("device_external_io_binding").where(bound.binary(bound.column("device_id"), Op::equal, bound.column("device_id", "new")));
                ruvia::db_procedure guard;
                guard.begin_if(expression.binary(expression.binary(expression.column("protocol_config_id", "old"), Op::is_distinct_from, expression.column("protocol_config_id", "new")), Op::and_value, expression.exists(bound)));
                guard.raise_exception("remove IO bindings before changing the IO model", "23514");
                guard.end_if();
                guard.return_value(expression.column("new"));
                schema.create_trigger_function("guard_device_external_io_model", guard);
                schema.create_trigger({ .name_ = "guard_device_external_io_model", .table_ = "device_external_io", .function_ = "guard_device_external_io_model", .events_ = { ruvia::db_trigger_event::update }, .update_columns_ = { "protocol_config_id" } });
            }
            {
                ruvia::db_query expression;
                ruvia::db_query source;
                source.select(source.value(1)).from("device_external_io").where(source.binary(source.column("protocol_config_id"), Op::equal, source.column("id", "old")));
                ruvia::db_query modelPoint;
                const auto points = modelPoint.binary(modelPoint.column("config", "new"), Op::json_get, modelPoint.value("points"));
                const std::array<ruvia::db_source_column, 1> columns{ { { .name_ = "value" } } };
                modelPoint.select(modelPoint.value(1)).from_function(modelPoint.call("jsonb_array_elements", { points }), "point", { .columns_ = columns }).where(modelPoint.binary(modelPoint.cast(modelPoint.binary(modelPoint.column("value", "point"), Op::json_get_text, modelPoint.value("id")), Type::uuid), Op::equal, modelPoint.column("point_id", "b"))).and_where(modelPoint.binary(modelPoint.binary(modelPoint.column("value", "point"), Op::json_get_text, modelPoint.value("kind")), Op::equal, modelPoint.column("kind", "b")));
                ruvia::db_query invalidBinding;
                invalidBinding.select(invalidBinding.value(1)).from("device_external_io_binding", "b").join(ruvia::db_join_type::inner, "device_external_io", invalidBinding.binary(invalidBinding.column("device_id", "s"), Op::equal, invalidBinding.column("device_id", "b")), "s").where(invalidBinding.binary(invalidBinding.column("protocol_config_id", "s"), Op::equal, invalidBinding.column("id", "old"))).and_where(invalidBinding.unary(ruvia::db_unary_operator::not_value, invalidBinding.exists(modelPoint)));
                const auto unavailable = expression.binary(expression.binary(expression.column("protocol", "new"), Op::not_equal, expression.value("ExternalIO")), Op::or_value, expression.binary(expression.unary(ruvia::db_unary_operator::not_value, expression.column("enabled", "new")), Op::or_value, expression.unary(ruvia::db_unary_operator::is_not_null, expression.column("deleted_at", "new"))));
                ruvia::db_procedure guard;
                guard.begin_if(expression.binary(unavailable, Op::and_value, expression.exists(source)));
                guard.raise_exception("remove device IO associations before disabling their model", "23514");
                guard.end_if();
                guard.begin_if(expression.exists(invalidBinding));
                guard.raise_exception("remove IO bindings before removing or changing their points", "23514");
                guard.end_if();
                guard.return_value(expression.column("new"));
                schema.create_trigger_function("guard_external_io_protocol", guard);
                schema.create_trigger({ .name_ = "guard_external_io_protocol", .table_ = "protocol_config", .function_ = "guard_external_io_protocol", .events_ = { ruvia::db_trigger_event::update }, .update_columns_ = { "protocol", "config", "enabled", "deleted_at" } });
            }
            {
                ruvia::db_query expression;
                ruvia::db_query associated;
                associated.select(associated.value(1)).from("device_external_io").where(associated.binary(associated.column("device_id"), Op::equal, associated.column("id", "new")));
                ruvia::db_query sameNode;
                sameNode.select(sameNode.value(1)).from("link", "previous").join(ruvia::db_join_type::inner, "link", sameNode.binary(sameNode.column("id", "target"), Op::equal, sameNode.column("link_id", "new")), "target").where(sameNode.binary(sameNode.column("id", "previous"), Op::equal, sameNode.column("link_id", "old"))).and_where(sameNode.binary(sameNode.column("edge_node_id", "previous"), Op::equal, sameNode.column("edge_node_id", "target"))).and_where(sameNode.binary(sameNode.column("execution", "target"), Op::equal, sameNode.value("edge"))).and_where(sameNode.unary(ruvia::db_unary_operator::is_null, sameNode.column("deleted_at", "target"))).lock({ .mode_ = ruvia::db_row_lock::share });
                ruvia::db_query remove;
                remove.delete_from("device_external_io")
                    .where(remove.binary(remove.column("device_id"), Op::equal, remove.column("id", "new")));
                ruvia::db_procedure guard;
                guard.begin_if(expression.unary(ruvia::db_unary_operator::is_not_null, expression.column("deleted_at", "new")));
                guard.execute(remove);
                guard.otherwise();
                guard.begin_if(expression.binary(expression.binary(expression.column("link_id", "old"), Op::is_distinct_from, expression.column("link_id", "new")), Op::and_value, expression.exists(associated)));
                guard.begin_if(expression.unary(ruvia::db_unary_operator::not_value, expression.exists(sameNode)));
                guard.raise_exception("remove external IO before changing the edge node", "23514");
                guard.end_if();
                guard.end_if();
                guard.end_if();
                guard.return_value(expression.column("new"));
                schema.create_trigger_function("guard_device_external_io_node", guard);
                schema.create_trigger({ .name_ = "guard_device_external_io_node", .table_ = "device", .function_ = "guard_device_external_io_node", .events_ = { ruvia::db_trigger_event::update }, .update_columns_ = { "link_id", "deleted_at" } });
            }
            {
                ruvia::db_query expression;
                ruvia::db_query associated;
                associated.select(associated.value(1)).from("device_external_io", "s").join(ruvia::db_join_type::inner, "device", associated.binary(associated.column("id", "d"), Op::equal, associated.column("device_id", "s")), "d").where(associated.binary(associated.column("link_id", "d"), Op::equal, associated.column("id", "old"))).and_where(associated.unary(ruvia::db_unary_operator::is_null, associated.column("deleted_at", "d")));
                const auto changedNode = expression.binary(expression.column("edge_node_id", "old"), Op::is_distinct_from, expression.column("edge_node_id", "new"));
                const auto changedExecution = expression.binary(expression.column("execution", "old"), Op::is_distinct_from, expression.column("execution", "new"));
                const auto deleted = expression.unary(ruvia::db_unary_operator::is_not_null, expression.column("deleted_at", "new"));
                ruvia::db_procedure guard;
                guard.begin_if(expression.binary(expression.binary(expression.binary(changedNode, Op::or_value, changedExecution), Op::or_value, deleted), Op::and_value, expression.exists(associated)));
                guard.raise_exception("remove external IO before changing its edge link", "23514");
                guard.end_if();
                guard.return_value(expression.column("new"));
                schema.create_trigger_function("guard_link_external_io_node", guard);
                schema.create_trigger({ .name_ = "guard_link_external_io_node", .table_ = "link", .function_ = "guard_link_external_io_node", .events_ = { ruvia::db_trigger_event::update }, .update_columns_ = { "edge_node_id", "execution", "deleted_at" } });
            }
            auto result = schema.compile("0059_device_external_io");
            return std::move(result.front());
        }(),
        [] {
            using Type = ruvia::db_data_type;
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query query;
            schema.create_table({ .name_ = "edge_acquisition_snapshot", .columns_ = {
                                                                            { .name_ = "node_id", .type_ = { .data_type_ = Type::uuid } },
                                                                            { .name_ = "digest", .type_ = { .data_type_ = Type::varchar, .length_ = 64 } },
                                                                            { .name_ = "revision", .type_ = { .data_type_ = Type::big_int } },
                                                                            { .name_ = "payload", .type_ = { .data_type_ = Type::jsonb } },
                                                                            { .name_ = "created_at", .type_ = { .data_type_ = Type::timestamp_tz }, .default_value_ = query.call("now") },
                                                                        },
                                  .constraints_ = {
                                      { .name_ = "edge_acquisition_snapshot_pkey", .kind_ = ruvia::db_constraint_kind::primary_key, .columns_ = { "node_id", "digest" } },
                                      { .name_ = "edge_acquisition_snapshot_revision_key", .kind_ = ruvia::db_constraint_kind::unique, .columns_ = { "node_id", "revision" } },
                                      { .name_ = "edge_acquisition_snapshot_node_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "node_id" }, .referenced_table_ = "edge_node", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::cascade },
                                  } });
            auto result = schema.compile("0060_edge_acquisition_snapshot");
            return std::move(result.front());
        }(),
        [] {
            using Op = ruvia::db_binary_operator;
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            const auto text = [](ruvia::db_query& expression, ruvia::db_expression value, std::string_view key) {
                return expression.binary(value, Op::json_get_text, expression.value(key));
            };
            const auto json = [](ruvia::db_query& expression, ruvia::db_expression value, std::string_view key) {
                return expression.binary(value, Op::json_get, expression.value(key));
            };
            const std::array<ruvia::db_source_column, 2> entries{ { { .name_ = "value" }, { .name_ = "ordinal" } } };
            // Existing per-device mappings are grouped by their resulting scheme content.
            // Preserve point IDs and formulas; never overwrite a different device's mapping.
            schema.drop_trigger("device_external_io", "guard_device_external_io_model");
            schema.drop_trigger_function("guard_device_external_io_model");
            ruvia::db_procedure upgrade;
            upgrade.declare_record("source");
            upgrade.declare_record("point");
            upgrade.declare_variable({ .name_ = "mapped", .type_ = { .data_type_ = Type::jsonb } });
            upgrade.declare_variable({ .name_ = "configuration", .type_ = { .data_type_ = Type::jsonb } });
            upgrade.declare_variable({ .name_ = "channel_name", .type_ = { .data_type_ = Type::text } });
            upgrade.declare_variable({ .name_ = "scheme_id", .type_ = { .data_type_ = Type::uuid } });
            for (const auto name : { "di_index", "do_index", "ai_index" }) {
                upgrade.declare_variable({ .name_ = name, .type_ = { .data_type_ = Type::integer } });
            }
            ruvia::db_query sources;
            sources.select({ sources.alias(sources.column("device_id", "s"), "device_id"), sources.star("p") }).from("device_external_io", "s").join(ruvia::db_join_type::inner, "protocol_config", sources.binary(sources.column("id", "p"), Op::equal, sources.column("protocol_config_id", "s")), "p").order_by(sources.column("device_id", "s"));
            upgrade.begin_for_query("source", sources);
            upgrade.assign("mapped", q.cast(q.value("[]"), Type::jsonb));
            ruvia::db_query points;
            points.select(points.column("value")).from_function(points.call("jsonb_array_elements", { json(points, points.column("config", "source"), "points") }), "entry", { .with_ordinality_ = true, .columns_ = entries }).order_by(points.column("ordinal"));
            upgrade.begin_for_query("point", points);
            ruvia::db_query binding;
            binding.select(binding.column("channel")).from("device_external_io_binding").where(binding.binary(binding.column("device_id"), Op::equal, binding.column("device_id", "source"))).and_where(binding.binary(binding.column("point_id"), Op::equal, binding.cast(text(binding, binding.column("value", "point"), "id"), Type::uuid)));
            upgrade.select_into(binding, { std::array<std::string_view, 1>{ "channel_name" } });
            upgrade.begin_if(q.unary(ruvia::db_unary_operator::is_null, q.column("channel_name")));
            upgrade.raise_exception("cannot migrate an incomplete external IO mapping", "23514");
            upgrade.end_if();
            upgrade.assign("mapped", q.binary(q.column("mapped"), Op::json_concat, q.call("jsonb_build_array", { q.binary(q.column("value", "point"), Op::json_concat, q.call("jsonb_build_object", { q.cast(q.value("channel"), Type::text), q.column("channel_name") })) })));
            upgrade.end_loop();
            upgrade.assign("configuration", q.call("jsonb_set", { q.column("config", "source"), q.array({ q.value("points") }), q.column("mapped") }));
            ruvia::db_query matching;
            matching.select(matching.column("id")).from("protocol_config").where(matching.binary(matching.column("protocol"), Op::equal, matching.value("ExternalIO"))).and_where(matching.binary(matching.column("config"), Op::equal, matching.column("configuration"))).and_where(matching.binary(matching.column("created_by"), Op::equal, matching.column("created_by", "source"))).and_where(matching.binary(matching.column("enabled"), Op::equal, matching.column("enabled", "source"))).and_where(matching.unary(ruvia::db_unary_operator::is_null, matching.column("deleted_at"))).limit(1);
            upgrade.select_into(matching, { std::array<std::string_view, 1>{ "scheme_id" } });
            upgrade.begin_if(q.unary(ruvia::db_unary_operator::is_null, q.column("scheme_id")));
            upgrade.assign("scheme_id", q.call("gen_random_uuid"));
            ruvia::db_query clone;
            clone.insert_into("protocol_config", { "id", "protocol", "name", "enabled", "config", "remark", "created_by" }).values({ clone.column("scheme_id"), clone.value("ExternalIO"), clone.call("concat", { clone.call("left", { clone.column("name", "source"), clone.value(24) }), clone.value(" ["), clone.cast(clone.column("scheme_id"), Type::text), clone.value("]") }), clone.column("enabled", "source"), clone.column("configuration"), clone.column("remark", "source"), clone.column("created_by", "source") });
            upgrade.execute(clone);
            upgrade.end_if();
            ruvia::db_query move;
            move.update("device_external_io").set("protocol_config_id", move.column("scheme_id")).where(move.binary(move.column("device_id"), Op::equal, move.column("device_id", "source")));
            upgrade.execute(move);
            upgrade.end_loop();
            // Unused legacy templates had no physical mapping. Give them deterministic
            // channels; every active association above already preserves its exact mapping.
            ruvia::db_query models;
            models.select(models.star()).from("protocol_config").where(models.binary(models.column("protocol"), Op::equal, models.value("ExternalIO")));
            upgrade.begin_for_query("source", models);
            upgrade.assign("mapped", q.cast(q.value("[]"), Type::jsonb));
            for (const auto name : { "di_index", "do_index", "ai_index" }) {
                upgrade.assign(name, q.value(0));
            }
            upgrade.begin_for_query("point", points);
            const auto pointValue = q.column("value", "point");
            for (const auto& [kind, prefix, counter] : { std::tuple{ "DI", "di", "di_index" }, std::tuple{ "DO", "do", "do_index" }, std::tuple{ "AI", "AI", "ai_index" } }) {
                upgrade.begin_if(q.binary(text(q, pointValue, "kind"), Op::equal, q.value(kind)));
                upgrade.assign("channel_name", q.coalesce({ text(q, pointValue, "channel"), q.call("concat", { q.value(prefix), q.column(counter) }) }));
                upgrade.assign(counter, q.binary(q.column(counter), Op::add, q.value(1)));
                upgrade.end_if();
            }
            upgrade.assign("mapped", q.binary(q.column("mapped"), Op::json_concat, q.call("jsonb_build_array", { q.binary(pointValue, Op::json_concat, q.call("jsonb_build_object", { q.cast(q.value("channel"), Type::text), q.column("channel_name") })) })));
            upgrade.end_loop();
            ruvia::db_query update;
            update.update("protocol_config").set("config", update.call("jsonb_set", { update.column("config", "source"), update.array({ update.value("points") }), update.column("mapped") })).where(update.binary(update.column("id"), Op::equal, update.column("id", "source")));
            upgrade.execute(update);
            upgrade.end_loop();
            schema.run(upgrade);
            schema.add_constraint("device_external_io_binding", { .name_ = "device_external_io_channel_key", .kind_ = ruvia::db_constraint_kind::unique, .columns_ = { "device_id", "channel" } });
            // Bindings are a database-maintained reservation/projection of scheme channels.
            // The scheme is the only writable source of the physical mapping.
            {
                ruvia::db_query match;
                const auto point = match.column("value", "point");
                const auto channel = match.column("value", "channel");
                const auto cap = match.binary(match.column("capability", "n"), Op::json_get, match.value("externalIo"));
                match.select(match.value(1)).from("device_external_io", "s").join(ruvia::db_join_type::inner, "protocol_config", match.binary(match.column("id", "p"), Op::equal, match.column("protocol_config_id", "s")), "p").join(ruvia::db_join_type::inner, "edge_node", match.binary(match.column("id", "n"), Op::equal, match.column("node_id", "s")), "n").join_function(ruvia::db_join_type::cross, match.call("jsonb_array_elements", { json(match, match.column("config", "p"), "points") }), {}, "point", { .lateral_ = true, .columns_ = std::span(entries).first(1) }).join_function(ruvia::db_join_type::cross, match.call("jsonb_array_elements", { json(match, cap, "channels") }), {}, "channel", { .lateral_ = true, .columns_ = std::span(entries).first(1) }).where(match.binary(match.column("device_id", "s"), Op::equal, match.column("device_id", "new"))).and_where(match.binary(match.column("node_id", "s"), Op::equal, match.column("node_id", "new"))).and_where(match.cast(json(match, cap, "supported"), Type::boolean)).and_where(match.binary(match.cast(text(match, point, "id"), Type::uuid), Op::equal, match.column("point_id", "new"))).and_where(match.binary(text(match, point, "kind"), Op::equal, match.column("kind", "new"))).and_where(match.binary(text(match, point, "channel"), Op::equal, match.column("channel", "new"))).and_where(match.binary(text(match, channel, "kind"), Op::equal, match.column("kind", "new"))).and_where(match.binary(text(match, channel, "channel"), Op::equal, match.column("channel", "new"))).and_where(match.binary(match.binary(match.column("kind", "new"), Op::not_equal, match.value("DO")), Op::or_value, match.cast(json(match, channel, "controllable"), Type::boolean)));
                ruvia::db_procedure guard;
                guard.begin_if(q.unary(ruvia::db_unary_operator::not_value, q.exists(match)));
                guard.raise_exception("external IO mapping must match its scheme and edge capabilities", "23514");
                guard.end_if();
                guard.return_value(q.column("new"));
                schema.create_trigger_function("guard_device_external_io_binding", guard, true);
            }
            for (const bool protocol : { false, true }) {
                ruvia::db_procedure refresh;
                ruvia::db_query affected;
                affected.select(affected.column("device_id")).from("device_external_io").where(affected.binary(affected.column(protocol ? "protocol_config_id" : "device_id"), Op::equal, affected.column(protocol ? "id" : "device_id", "new")));
                ruvia::db_query remove;
                remove.delete_from("device_external_io_binding").where(remove.binary(remove.column("device_id"), Op::in, remove.subquery(affected)));
                refresh.execute(remove);
                ruvia::db_query rows;
                const auto point = rows.column("value", "point");
                rows.select({ rows.column("device_id", "s"), rows.cast(text(rows, point, "id"), Type::uuid), rows.column("node_id", "s"), text(rows, point, "kind"), text(rows, point, "channel") }).from("device_external_io", "s").join(ruvia::db_join_type::inner, "protocol_config", rows.binary(rows.column("id", "p"), Op::equal, rows.column("protocol_config_id", "s")), "p").join_function(ruvia::db_join_type::cross, rows.call("jsonb_array_elements", { json(rows, rows.column("config", "p"), "points") }), {}, "point", { .lateral_ = true, .columns_ = std::span(entries).first(1) }).where(rows.binary(rows.column(protocol ? "protocol_config_id" : "device_id", "s"), Op::equal, rows.column(protocol ? "id" : "device_id", "new")));
                ruvia::db_query insert;
                insert.insert_into("device_external_io_binding", { "device_id", "point_id", "node_id", "kind", "channel" }).insert_from(rows);
                refresh.execute(insert);
                refresh.return_value(q.column("new"));
                const auto name = protocol ? "refresh_external_io_scheme" : "refresh_device_external_io";
                schema.create_trigger_function(name, refresh);
                schema.create_trigger({ .name_ = name, .table_ = protocol ? "protocol_config" : "device_external_io", .function_ = name, .timing_ = ruvia::db_trigger_timing::after, .events_ = protocol ? std::pmr::vector<ruvia::db_trigger_event>{ ruvia::db_trigger_event::update } : std::pmr::vector<ruvia::db_trigger_event>{ ruvia::db_trigger_event::insert, ruvia::db_trigger_event::update }, .update_columns_ = protocol ? std::pmr::vector<ruvia::borrowed_text>{ "config" } : std::pmr::vector<ruvia::borrowed_text>{ "protocol_config_id", "node_id" } });
            }
            auto result = schema.compile("0061_external_io_scheme_channels");
            return std::move(result.front());
        }(),
        [] {
            using Op = ruvia::db_binary_operator;
            ruvia::db_schema schema({ .driver_ = ruvia::db_driver::postgresql });
            ruvia::db_query q;
            schema.create_table({ .name_ = "edge_acquisition_epoch", .columns_ = { { .name_ = "node_id", .type_ = { .data_type_ = Type::uuid }, .primary_key_ = true }, { .name_ = "epoch", .type_ = { .data_type_ = Type::big_int }, .default_value_ = q.value(0) } }, .constraints_ = { { .name_ = "edge_acquisition_epoch_node_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "node_id" }, .referenced_table_ = "edge_node", .referenced_columns_ = { "id" }, .on_delete_ = ruvia::db_referential_action::cascade } } });
            ruvia::db_query nodes;
            nodes.select(nodes.column("id")).from("edge_node");
            ruvia::db_query seed;
            seed.insert_into("edge_acquisition_epoch", { "node_id" }).insert_from(nodes);
            schema.execute(seed);
            ruvia::db_procedure fence;
            fence.perform(q.call("pg_advisory_xact_lock", { q.cast(q.value(service::message::lock::kAcquisitionConfiguration), Type::big_int) }));
            fence.return_value(q.null_value());
            schema.create_trigger_function("lock_acquisition_configuration", fence);
            for (const auto table : { "device", "link", "protocol_config", "device_external_io", "device_external_io_binding", "edge_dtu" }) {
                std::pmr::vector<ruvia::borrowed_text> fields;
                if (std::string_view(table) == "edge_dtu") {
                    fields = { "node_id", "channel_id", "config", "wire_hex" };
                } else if (std::string_view(table) == "protocol_config") {
                    fields = { "protocol", "name", "config", "enabled", "deleted_at" };
                } else if (std::string_view(table) == "link") {
                    fields = { "name", "protocol", "endpoint", "execution", "edge_node_id", "status", "debug_enabled", "deleted_at" };
                } else if (std::string_view(table) == "device") {
                    fields = { "name", "link_id", "protocol_config_id", "protocol_params", "protocol_address", "status", "deleted_at" };
                } else if (std::string_view(table) == "device_external_io") {
                    fields = { "device_id", "link_id", "node_id", "protocol_config_id", "derived_config" };
                } else {
                    fields = { "device_id", "point_id", "node_id", "kind", "channel" };
                }
                schema.create_trigger({ .name_ = "lock_acquisition_configuration", .table_ = table, .function_ = "lock_acquisition_configuration", .events_ = { ruvia::db_trigger_event::insert, ruvia::db_trigger_event::update, ruvia::db_trigger_event::delete_value }, .for_each_row_ = false, .update_columns_ = fields });
                ruvia::db_procedure changed;
                std::vector<ruvia::db_expression> oldValues, newValues;
                for (const auto field : fields) {
                    oldValues.push_back(q.column(field.view(), "old"));
                    newValues.push_back(q.column(field.view(), "new"));
                }
                changed.begin_if(q.binary(q.tuple(oldValues), Op::is_not_distinct_from, q.tuple(newValues)));
                changed.return_value(q.null_value());
                changed.end_if();
                ruvia::db_query affected;
                if (std::string_view(table) == "device") {
                    affected.select(affected.column("edge_node_id")).from("link").where(affected.binary(affected.column("id"), Op::in, affected.list({ affected.column("link_id", "old"), affected.column("link_id", "new") }))).and_where(affected.unary(ruvia::db_unary_operator::is_not_null, affected.column("edge_node_id")));
                } else if (std::string_view(table) == "protocol_config") {
                    affected.select(affected.column("edge_node_id", "l")).from("link", "l").join(ruvia::db_join_type::inner, "device", affected.binary(affected.column("link_id", "d"), Op::equal, affected.column("id", "l")), "d").join(ruvia::db_join_type::left, "device_external_io", affected.binary(affected.column("device_id", "s"), Op::equal, affected.column("id", "d")), "s").where(affected.binary(affected.binary(affected.column("protocol_config_id", "d"), Op::in, affected.list({ affected.column("id", "old"), affected.column("id", "new") })), Op::or_value, affected.binary(affected.column("protocol_config_id", "s"), Op::in, affected.list({ affected.column("id", "old"), affected.column("id", "new") })))).and_where(affected.unary(ruvia::db_unary_operator::is_not_null, affected.column("edge_node_id", "l"))).distinct();
                } else {
                    const auto column = std::string_view(table) == "link" ? "edge_node_id" : "node_id";
                    affected.values({ affected.column(column, "old") }).values({ affected.column(column, "new") });
                }
                ruvia::db_query valid;
                const std::array<ruvia::db_source_column, 1> columns{ { { .name_ = "node_id" } } };
                valid.select(valid.column("node_id")).from(affected, "affected", { .columns_ = columns }).where(valid.unary(ruvia::db_unary_operator::is_not_null, valid.column("node_id"))).distinct();
                ruvia::db_query epoch;
                epoch.insert_into("edge_acquisition_epoch", { "node_id" }).insert_from(valid).on_conflict({ .columns_ = std::array<ruvia::borrowed_text, 1>{ "node_id" }, .update_ = std::array<ruvia::db_assignment, 1>{ { { "epoch", epoch.binary(epoch.column("epoch", "edge_acquisition_epoch"), Op::add, epoch.value(1)) } } } });
                changed.execute(epoch);
                changed.return_value(q.null_value());
                const auto name = std::string("advance_acquisition_epoch_") + table;
                schema.create_trigger_function(name, changed);
                schema.create_trigger({ .name_ = name, .table_ = table, .function_ = name, .timing_ = ruvia::db_trigger_timing::after, .events_ = { ruvia::db_trigger_event::insert, ruvia::db_trigger_event::update, ruvia::db_trigger_event::delete_value }, .update_columns_ = fields });
            }
            schema.create_table({ .name_ = "edge_acquisition_sample", .columns_ = { { .name_ = "node_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "digest", .type_ = { .data_type_ = Type::varchar, .length_ = 64 } }, { .name_ = "device_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "record_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "observed_at_ms", .type_ = { .data_type_ = Type::big_int } }, { .name_ = "payload", .type_ = { .data_type_ = Type::jsonb } } }, .constraints_ = { { .name_ = "edge_acquisition_sample_pkey", .kind_ = ruvia::db_constraint_kind::primary_key, .columns_ = { "node_id", "digest", "device_id", "record_id" } }, { .name_ = "edge_acquisition_sample_snapshot_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "node_id", "digest" }, .referenced_table_ = "edge_acquisition_snapshot", .referenced_columns_ = { "node_id", "digest" }, .on_delete_ = ruvia::db_referential_action::cascade } } });
            schema.create_index({ .name_ = "edge_acquisition_sample_timeline", .table_ = "edge_acquisition_sample", .keys_ = { { .column_ = "node_id" }, { .column_ = "digest" }, { .column_ = "device_id" }, { .column_ = "observed_at_ms" }, { .column_ = "record_id" } } });
            schema.create_table({ .name_ = "edge_acquisition_calculation", .columns_ = { { .name_ = "node_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "digest", .type_ = { .data_type_ = Type::varchar, .length_ = 64 } }, { .name_ = "device_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "record_id", .type_ = { .data_type_ = Type::uuid } }, { .name_ = "observed_at_ms", .type_ = { .data_type_ = Type::big_int } }, { .name_ = "state", .type_ = { .data_type_ = Type::jsonb } } }, .constraints_ = { { .name_ = "edge_acquisition_calculation_pkey", .kind_ = ruvia::db_constraint_kind::primary_key, .columns_ = { "node_id", "digest", "device_id" } }, { .name_ = "edge_acquisition_calculation_snapshot_fkey", .kind_ = ruvia::db_constraint_kind::foreign_key, .columns_ = { "node_id", "digest" }, .referenced_table_ = "edge_acquisition_snapshot", .referenced_columns_ = { "node_id", "digest" }, .on_delete_ = ruvia::db_referential_action::cascade } } });
            schema.add_column("device_data", { .name_ = "configuration_digest", .type_ = { .data_type_ = Type::varchar, .length_ = 64 }, .nullable_ = true });
            auto result = schema.compile("0062_acquisition_epoch_and_history");
            return std::move(result.front());
        }(),
        ruvia::db_migration({ .id_ = "0063_edge_io_management", .sql_ = std::string(R"sql(
DO $schema$
BEGIN
ALTER TABLE edge_task DROP CONSTRAINT edge_task_task_type_check;
ALTER TABLE edge_task ADD CONSTRAINT edge_task_task_type_check
 CHECK (task_type IN ('network','firmware','modem','vpn','platform_upsert','platform_delete','io'));
CREATE UNIQUE INDEX edge_io_pending_channel ON edge_task(node_id, (request->>'channel'))
 WHERE task_type='io' AND status NOT IN ('succeeded','failed');
CREATE FUNCTION guard_edge_io_control_ownership() RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
 PERFORM pg_advisory_xact_lock()sql") +
                                  std::to_string(service::message::lock::kAcquisitionConfiguration) + R"sql();
 IF TG_TABLE_NAME='edge_task' THEN
  IF NEW.task_type='io' AND EXISTS(SELECT 1 FROM device_external_io_binding
    WHERE node_id=NEW.node_id AND channel=NEW.request->>'channel') THEN
   RAISE EXCEPTION 'IO channel is assigned to a device' USING ERRCODE='23514';
  END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM edge_task WHERE node_id=NEW.node_id AND task_type='io'
    AND request->>'channel'=NEW.channel AND status NOT IN ('succeeded','failed')) THEN
   RAISE EXCEPTION 'IO channel has an unfinished node control' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $guard$;
CREATE TRIGGER guard_node_io_control BEFORE INSERT ON edge_task
 FOR EACH ROW WHEN (NEW.task_type='io') EXECUTE FUNCTION guard_edge_io_control_ownership();
CREATE TRIGGER guard_io_binding_control BEFORE INSERT OR UPDATE ON device_external_io_binding
 FOR EACH ROW EXECUTE FUNCTION guard_edge_io_control_ownership();
-- IO task changes only affect the IO view. Other management tasks retain edge notifications.
DO $triggers$ DECLARE trigger_name TEXT; BEGIN
 FOR trigger_name IN SELECT tgname FROM pg_trigger WHERE tgrelid='edge_task'::regclass AND tgfoid='publish_query_change()'::regprocedure AND NOT tgisinternal LOOP
  EXECUTE format('DROP TRIGGER %I ON edge_task',trigger_name);
 END LOOP;
END $triggers$;
CREATE FUNCTION publish_edge_task_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE changed RECORD;
BEGIN
 FOR changed IN SELECT CASE WHEN task_type='io' THEN TG_ARGV[0] ELSE 'edge' END AS topic, string_agg(DISTINCT node_id::text,',') AS ids FROM query_changed_rows GROUP BY 1 LOOP
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
   VALUES(gen_random_uuid(),'query.changed',changed.topic,changed.ids,TG_OP,2);
 END LOOP;
 RETURN NULL;
END $fn$;
CREATE TRIGGER edge_task_io_insert AFTER INSERT ON edge_task REFERENCING NEW TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_edge_task_change(')sql" +
                                  std::string(service::message::live::topic::kEdgeIo) + R"sql(');
CREATE TRIGGER edge_task_io_update AFTER UPDATE ON edge_task REFERENCING NEW TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_edge_task_change(')sql" +
                                  std::string(service::message::live::topic::kEdgeIo) + R"sql(');
CREATE TRIGGER edge_task_io_delete AFTER DELETE ON edge_task REFERENCING OLD TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_edge_task_change(')sql" +
                                  std::string(service::message::live::topic::kEdgeIo) + R"sql(');
CREATE TRIGGER edge_io_binding_insert AFTER INSERT ON device_external_io_binding REFERENCING NEW TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change(')sql" +
                                  std::string(service::message::live::topic::kEdgeIo) + R"sql(','node_id');
CREATE TRIGGER edge_io_binding_delete AFTER DELETE ON device_external_io_binding REFERENCING OLD TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change(')sql" +
                                  std::string(service::message::live::topic::kEdgeIo) + R"sql(','node_id');
CREATE TRIGGER edge_io_binding_update AFTER UPDATE ON device_external_io_binding REFERENCING NEW TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change(')sql" +
                                  std::string(service::message::live::topic::kEdgeIo) + R"sql(','node_id');

CREATE INDEX edge_io_pending_deadline ON edge_task(((request->>'startBeforeMs')::bigint)) WHERE task_type='io' AND status='pending';

END $schema$;
)sql" }),
    };
    migrations.emplace_back(ruvia::db_migration({ .id_ = "0064_mqtt_intrinsic_properties", .sql_ = std::string(kMqttIntrinsicPropertiesMigration) }));
    // Only audited original digests may transition to their equivalent ORM definitions.
    // This transaction runs before normal checksum validation; all other drift still fails.
    ruvia::db_schema adoption({ .driver_ = ruvia::db_driver::postgresql });
    constexpr std::pair<std::string_view, std::string_view> originalChecksums[] = {
        { "0015_sql_query_optimization", "1c0e9800209a1eb7daef1b8eb517ceed0a15aa42e2de31ba221f2d66f8a91b89" },
        { "0016_mass_data_query_optimization", "696aeef5ad92c0137af08b4b987c13fabecd60c589c8757a3b6cdad9aabbe9af" },
        { "0017_device_data_compression_layout", "f242219083c311c5b1409e0227a8339628d9f5941fef54b6a7e37b286c77a6c1" },
        { "0019_alert_event_outbox", "53b7c5b379b27a9233c86f974df67418ae2b6951d8ebf96124d766795001917b" },
        { "0020_alert_evaluation_receipt", "448b71fbe63a6988c712e30f98bf89f6f377a7ceb811f31f3bcf5e4aa2906cc1" },
        { "0022_gb28181_custom_names", "7d3b1fda358e2ed480df49b5f326ad02e9def00201c56730762e51102e80c849" },
        { "0025_webhook_tls_verification", "96b75f7609ce3e73e11860ac59d3b62b878bebf5d9a289f93c5ee67e3d64e1c0" },
        { "0031_vpn_hub_database_config", "772de366d9e192c1b45866d80d678169526d1b7439990d29c36732cbdd5f9de2" },
        { "0033_vpn_client_repeat_download", "5b30acb4142d60711da6fa069e682ee57c8191c11e92baa24cb9140554dc1bc8" },
        { "0044_gb28181_projection_cursor", "6264c9aefdac64bd392177221b6296e090d73b70c8ab91f7ff746faa1846930a" },
        { "0045_vpn_windows_address_reuse", "15b2fe914fdd329c4239c2d73a44f5c2eb190818065a40271dc4f095a54ef84d" },
        { "0023_transactional_outbox", "1a2097a3cac62a766273ebbd18c65c7af8a4732c944b821b4703b9e4f186f008" },
        { "0024_outbox_consumer_receipt", "88697553cbc721fdc18abe370cf619904b2d05b080e620630782253a243f8a0f" },
        { "0032_edge_node_group", "f333cf9f9954562bd9234507811250c899349ff943cdd222674e846f52341c42" },
        { "0039_alert_input_state", "e0057d351f57397cd756792fbb29f7c23b7df8e2573ba030e59bdfee0f18c576" },
    };
    for (const auto& [id, digest] : originalChecksums) {
        const auto migration = std::find_if(migrations.begin(), migrations.end(), [id](const auto& value) {
            return value.id() == id;
        });
        if (migration == migrations.end()) {
            throw std::logic_error("missing converted schema migration");
        }
        const auto nextChecksum = service::utils::sha256(migration->sql());
        ruvia::db_query invalid;
        invalid.from("sys_schema_migrations").select(invalid.value(1)).where(invalid.binary(invalid.column("migration_id"), ruvia::db_binary_operator::equal, invalid.value(id))).and_where(invalid.binary(invalid.column("checksum"), ruvia::db_binary_operator::not_in, invalid.list({ invalid.value(digest), invalid.value(nextChecksum), invalid.value("") })));
        ruvia::db_query condition;
        ruvia::db_procedure guard;
        guard.begin_if(condition.exists(invalid));
        guard.raise_exception("schema checksum differs from the audited migration: " + std::string(id));
        guard.end_if();
        adoption.run(guard);
        ruvia::db_query update;
        update.update("sys_schema_migrations")
            .set("checksum", update.value(nextChecksum))
            .where(update.binary(update.binary(update.column("migration_id"), ruvia::db_binary_operator::equal, update.value(id)), ruvia::db_binary_operator::and_value, update.binary(update.column("checksum"), ruvia::db_binary_operator::equal, update.value(digest))));
        adoption.execute(update);
    }
    auto transition = adoption.compile("orm_schema_checksum_transition_20260914");
    const auto firstConverted = std::find_if(migrations.begin(), migrations.end(), [](const auto& value) {
        return value.id() == "0015_sql_query_optimization";
    });
    migrations.insert(firstConverted, std::move(transition.front()));
    return migrations;
}();

} // namespace service::config
