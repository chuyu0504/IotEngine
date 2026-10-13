#pragma once

#include <string_view>

namespace service::config {

// 将按设备编码配置的固定参数归还设备，保持模板中的 $param:名称 不变。
inline constexpr std::string_view kDeviceParametersMigration = R"sql(
DO $migration$
DECLARE
  model RECORD;
  parameter JSONB;
  bound RECORD;
  values_by_name JSONB;
BEGIN
  FOR model IN SELECT id, config FROM protocol_config WHERE config ? 'deviceParameters' LOOP
    IF jsonb_typeof(model.config->'deviceParameters') <> 'array' THEN
      RAISE EXCEPTION 'deviceParameters must be an array for model %', model.id;
    END IF;
    IF EXISTS (
      SELECT 1 FROM jsonb_array_elements(model.config->'deviceParameters') p
      GROUP BY p->>'deviceCode', p->>'name' HAVING count(*) > 1
    ) THEN RAISE EXCEPTION 'duplicate device parameter for model %', model.id; END IF;
    FOR parameter IN SELECT value FROM jsonb_array_elements(model.config->'deviceParameters') LOOP
      IF jsonb_typeof(parameter->'deviceCode') IS DISTINCT FROM 'string'
        OR jsonb_typeof(parameter->'name') IS DISTINCT FROM 'string'
        OR length(parameter->>'name') = 0 OR NOT parameter ? 'value'
        OR NOT EXISTS (SELECT 1 FROM device d WHERE d.protocol_config_id = model.id
          AND d.protocol_params->>'device_code' = parameter->>'deviceCode') THEN
        RAISE EXCEPTION 'unbound or invalid device parameter for model %', model.id;
      END IF;
    END LOOP;
    FOR bound IN SELECT id, protocol_params FROM device WHERE protocol_config_id = model.id LOOP
      SELECT jsonb_object_agg(p->>'name', p->'value') INTO values_by_name
        FROM jsonb_array_elements(model.config->'deviceParameters') p
        WHERE p->>'deviceCode' = bound.protocol_params->>'device_code';
      IF values_by_name IS NULL THEN CONTINUE; END IF;
      IF jsonb_typeof(COALESCE(bound.protocol_params->'parameters', '{}'::jsonb)) <> 'object'
        OR EXISTS (SELECT 1 FROM jsonb_each(values_by_name) p
          WHERE COALESCE(bound.protocol_params->'parameters', '{}'::jsonb) ? p.key
            AND bound.protocol_params->'parameters'->p.key IS DISTINCT FROM p.value) THEN
        RAISE EXCEPTION 'conflicting device parameters for device %', bound.id;
      END IF;
      UPDATE device SET protocol_params = jsonb_set(protocol_params, '{parameters}',
        values_by_name || COALESCE(protocol_params->'parameters', '{}'::jsonb)), updated_at = now()
        WHERE id = bound.id;
    END LOOP;
    UPDATE protocol_config SET config = config - 'deviceParameters', updated_at = now() WHERE id = model.id;
  END LOOP;
  ALTER TABLE protocol_config ADD CONSTRAINT ck_protocol_config_device_parameters
    CHECK (NOT config ? 'deviceParameters');
  ALTER TABLE device ADD CONSTRAINT ck_device_parameters
    CHECK (NOT protocol_params ? 'parameters' OR jsonb_typeof(protocol_params->'parameters') = 'object');
END
$migration$;
)sql";

} // namespace service::config
