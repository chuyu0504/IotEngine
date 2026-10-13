#pragma once

#include <string_view>

namespace service::config {

// 保留旧结果的 UUID 和历史连续性；原始采集值使用新 UUID，默认隐藏。
inline constexpr std::string_view kNativePointCalculationsMigration = R"sql(
DO $migration$
DECLARE
  model RECORD;
  point JSONB;
  updated JSONB;
  points JSONB;
  derived JSONB;
  visibility JSONB;
  field TEXT;
  raw_id TEXT;
  expression TEXT;
  factor DOUBLE PRECISION;
  bias DOUBLE PRECISION;
  decimals INTEGER;
  transforms BOOLEAN;
BEGIN
  LOCK TABLE protocol_config, device, link IN SHARE ROW EXCLUSIVE MODE;
  FOR model IN SELECT id, protocol, config FROM protocol_config FOR UPDATE LOOP
    updated := model.config;
    derived := COALESCE(updated->'derivedPoints', '[]'::jsonb);
    visibility := COALESCE(updated->'pointVisibility', '{}'::jsonb);
    FOREACH field IN ARRAY ARRAY['registers','areas','points'] LOOP
      IF jsonb_typeof(updated->field) IS DISTINCT FROM 'array' THEN CONTINUE; END IF;
      points := '[]'::jsonb;
      FOR point IN SELECT value FROM jsonb_array_elements(updated->field) LOOP
        factor := COALESCE((point->>'scale')::double precision, 1);
        bias := CASE WHEN model.protocol='MQTT' THEN COALESCE((point->>'offset')::double precision,0) ELSE 0 END;
        transforms := (model.protocol='Modbus' AND field='registers' OR
                       model.protocol IN ('MC','FINS','MQTT') AND field='points')
                      AND COALESCE(point->>'dataType','') NOT IN ('BOOL','STRING')
                      AND (factor<>1 OR bias<>0);
        IF transforms THEN
          IF COALESCE((point->>'writable')::boolean,false) THEN
            RAISE EXCEPTION 'Model % point %: convert writable scaling to explicit raw control values before upgrading', model.id, point->>'id';
          END IF;
          IF jsonb_array_length(derived)>=32 THEN
            RAISE EXCEPTION 'Model %: native conversion exceeds the 32 derived point limit', model.id;
          END IF;
          IF EXISTS (SELECT 1 FROM device d JOIN link l ON l.id=d.link_id
                     JOIN edge_node e ON e.id=l.edge_node_id
                     WHERE d.protocol_config_id=model.id AND d.deleted_at IS NULL
                       AND l.deleted_at IS NULL AND l.execution='edge'
                       AND COALESCE(e.capability->>'derivedPoints','false')<>'true') THEN
            RAISE EXCEPTION 'Model %: upgrade the connected edge firmware to support derived points before upgrading', model.id;
          END IF;
          raw_id := gen_random_uuid()::text;
          expression := 'x * (' || factor::text || ') + (' || bias::text || ')';
          decimals := CASE WHEN model.protocol='MQTT' THEN -1 ELSE COALESCE((point->>'decimals')::integer,-1) END;
          IF decimals>=0 THEN
            expression := 'round((' || expression || ') * ' || power(10::numeric,decimals)::bigint::text || ') / ' || power(10::numeric,decimals)::bigint::text;
          END IF;
          derived := derived || jsonb_build_array(jsonb_build_object(
            'id',point->>'id','name',point->>'name','unit',COALESCE(point->>'unit',''),
            'kind','expression','valueType','number','expression',expression,
            'inputs',jsonb_build_array(jsonb_build_object('alias','x','pointId',raw_id)),
            'unitMode','fixed','maxAgeSeconds',LEAST(86400,GREATEST(300,COALESCE((updated->>'readInterval')::numeric,1)*3)),
            'visible',COALESCE((visibility->>(point->>'id'))::boolean,true)));
          visibility := (visibility - (point->>'id')) || jsonb_build_object(raw_id,false);
          point := (point - 'scale' - 'offset' - 'unit') || jsonb_build_object(
            'id',raw_id,'name',CASE WHEN model.protocol='MQTT' THEN point->>'name' ELSE left(point->>'name',12) || ' [raw ' || raw_id || ']' END);
          IF model.protocol<>'MQTT' THEN point := point || '{"decimals":-1}'::jsonb; END IF;
        ELSE
          point := point - 'scale' - 'offset';
        END IF;
        points := points || jsonb_build_array(point);
      END LOOP;
      updated := jsonb_set(updated,ARRAY[field],points);
    END LOOP;
    IF derived IS DISTINCT FROM COALESCE(model.config->'derivedPoints','[]'::jsonb) THEN
      updated := updated || jsonb_build_object('derivedPoints',derived,'pointVisibility',visibility);
    END IF;
    IF updated IS DISTINCT FROM model.config THEN
      UPDATE protocol_config SET config=updated,updated_at=NOW() WHERE id=model.id;
    END IF;
  END LOOP;
ALTER TABLE protocol_config ADD CONSTRAINT ck_protocol_config_native_points
  CHECK (NOT jsonb_path_exists(config, '$.registers[*] ? (exists (@.scale) || exists (@.offset))')
     AND NOT jsonb_path_exists(config, '$.areas[*] ? (exists (@.scale) || exists (@.offset))')
     AND NOT jsonb_path_exists(config, '$.points[*] ? (exists (@.scale) || exists (@.offset))'));
END $migration$;
)sql";

} // namespace service::config
