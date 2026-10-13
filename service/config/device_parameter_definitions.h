#pragma once

#include <string_view>

namespace service::config {

// 根据已有设备值和控制模板补齐参数定义；不改设备参数值或模板引用。
inline constexpr std::string_view kDeviceParameterDefinitionsMigration = R"sql(
DO $migration$
DECLARE
  model RECORD;
  field RECORD;
  definition JSONB;
  definitions JSONB;
BEGIN
  FOR model IN SELECT id, protocol, config FROM protocol_config LOOP
    definitions := COALESCE(model.config->'parameters', '[]'::jsonb);
    IF jsonb_typeof(definitions) <> 'array' THEN
      RAISE EXCEPTION 'device parameter definitions must be an array for model %', model.id;
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(definitions) d GROUP BY d->>'name' HAVING count(*) > 1) THEN
      RAISE EXCEPTION 'duplicate device parameter definition for model %', model.id;
    END IF;
    FOR definition IN SELECT value FROM jsonb_array_elements(definitions) LOOP
      IF jsonb_typeof(definition->'name') IS DISTINCT FROM 'string' OR
         octet_length(definition->>'name') NOT BETWEEN 1 AND 100 OR
         (definition->>'name') <> btrim(definition->>'name') OR
         COALESCE(definition->>'dataType','') NOT IN ('STRING','DOUBLE','BOOL','JSON') THEN
        RAISE EXCEPTION 'invalid device parameter definition for model %', model.id;
      END IF;
    END LOOP;
    FOR field IN
      WITH stored AS (
        SELECT p.key AS name, NULLIF(jsonb_typeof(p.value),'null') AS kind
        FROM device d CROSS JOIN LATERAL jsonb_each(COALESCE(d.protocol_params->'parameters','{}'::jsonb)) p
        WHERE d.protocol_config_id = model.id
      ), templates AS (
        SELECT value->>'template' AS body FROM jsonb_array_elements(COALESCE(model.config->'commands','[]'::jsonb))
        WHERE model.protocol = 'MQTT'
        UNION ALL SELECT model.config->>'commandTemplate'
        WHERE model.protocol = 'MQTT' AND COALESCE(model.config->>'commandTemplate','') <> ''
      ), referenced AS (
        SELECT substr(token #>> '{}',8) AS name, NULL::TEXT AS kind
        FROM templates CROSS JOIN LATERAL jsonb_path_query(body::jsonb, '$.** ? (@.type() == "string")') token
        WHERE left(token #>> '{}',7) = '$param:'
      )
      SELECT name, CASE WHEN count(DISTINCT kind) > 1 THEN 'JSON'
        ELSE CASE max(kind) WHEN 'number' THEN 'DOUBLE' WHEN 'boolean' THEN 'BOOL'
          WHEN 'array' THEN 'JSON' WHEN 'object' THEN 'JSON' ELSE 'STRING' END END AS type
      FROM (SELECT * FROM stored UNION ALL SELECT * FROM referenced) input GROUP BY name ORDER BY name
    LOOP
      IF octet_length(field.name) NOT BETWEEN 1 AND 100 OR field.name <> btrim(field.name) THEN
        RAISE EXCEPTION 'invalid device parameter name for model %', model.id;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM jsonb_array_elements(definitions) d WHERE d->>'name' = field.name) THEN
        definitions := definitions || jsonb_build_array(jsonb_build_object('name',field.name,'dataType',field.type));
      END IF;
    END LOOP;
    IF jsonb_array_length(definitions) > 256 THEN
      RAISE EXCEPTION 'too many device parameter definitions for model %', model.id;
    END IF;
    IF definitions <> '[]'::jsonb AND definitions IS DISTINCT FROM model.config->'parameters' THEN
      UPDATE protocol_config SET config = jsonb_set(config,'{parameters}',definitions), updated_at = now() WHERE id = model.id;
    END IF;
  END LOOP;
  ALTER TABLE protocol_config ADD CONSTRAINT ck_protocol_parameter_definitions
    CHECK (NOT config ? 'parameters' OR jsonb_typeof(config->'parameters') = 'array');
END
$migration$;
)sql";

} // namespace service::config
