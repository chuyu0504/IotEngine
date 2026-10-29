#pragma once
#include <string_view>

namespace service::config {
inline constexpr std::string_view kMqttDeviceAddressMigration = R"sql(
CREATE OR REPLACE FUNCTION bind_device_channel() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE channel link%ROWTYPE; model_protocol TEXT; registration TEXT;
BEGIN
  SELECT * INTO STRICT channel FROM link WHERE id=NEW.link_id FOR SHARE;
  IF NEW.deleted_at IS NULL AND channel.deleted_at IS NOT NULL THEN
    RAISE EXCEPTION 'Cannot bind a deleted channel';
  END IF;
  SELECT protocol INTO STRICT model_protocol FROM protocol_config WHERE id=NEW.protocol_config_id;
  IF model_protocol <> channel.protocol THEN RAISE EXCEPTION 'Channel and model protocols differ'; END IF;
  IF channel.execution='edge' THEN
    NEW.protocol_params := jsonb_set(NEW.protocol_params,'{registration}','{"mode":"OFF"}');
    IF channel.protocol='Modbus' THEN
      NEW.protocol_params := jsonb_set(NEW.protocol_params,'{modbus_mode}',
        to_jsonb(CASE WHEN channel.endpoint->>'transport'='serial' THEN 'RTU'::text ELSE 'TCP'::text END));
    END IF;
  END IF;
  registration := CASE upper(COALESCE(NEW.protocol_params#>>'{registration,mode}','OFF'))
    WHEN 'OFF' THEN '' WHEN 'HEX' THEN 'HEX:' || upper(regexp_replace(
      COALESCE(NEW.protocol_params#>>'{registration,content}',''),'\s','','g'))
    ELSE 'ASCII:' || COALESCE(NEW.protocol_params#>>'{registration,content}','') END;
  NEW.protocol_address := CASE channel.protocol
    WHEN 'SL651' THEN NEW.protocol_params->>'device_code'
    WHEN 'MQTT' THEN jsonb_build_array(COALESCE(NEW.protocol_params->>'target_id',''),
      NEW.protocol_params->>'device_code')::text
    WHEN 'DLT645' THEN jsonb_build_array(COALESCE(NEW.protocol_params->>'target_id',''),registration,
      NEW.protocol_params->>'device_code')::text
    WHEN 'Modbus' THEN jsonb_build_array(COALESCE(NEW.protocol_params->>'target_id',''),registration,
      COALESCE((NEW.protocol_params->>'slave_id')::integer,1))::text
    ELSE jsonb_build_array(COALESCE(NEW.protocol_params->>'target_id',''),registration)::text END;
  RETURN NEW;
END $fn$;
)sql";

inline constexpr std::string_view kMqttIntrinsicPropertiesMigration = R"sql(
DO $mqtt_intrinsic$ BEGIN EXECUTE $mqtt_schema$
CREATE OR REPLACE FUNCTION pg_temp.mqtt_has_token(value JSONB, token TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql AS $fn$
DECLARE child JSONB;
BEGIN
  IF jsonb_typeof(value)='string' THEN RETURN value=to_jsonb(token); END IF;
  IF jsonb_typeof(value)='object' THEN
    FOR child IN SELECT v FROM jsonb_each(value) AS e(k,v) LOOP
      IF pg_temp.mqtt_has_token(child,token) THEN RETURN TRUE; END IF;
    END LOOP;
  ELSIF jsonb_typeof(value)='array' THEN
    FOR child IN SELECT jsonb_array_elements(value) LOOP
      IF pg_temp.mqtt_has_token(child,token) THEN RETURN TRUE; END IF;
    END LOOP;
  END IF;
  RETURN FALSE;
END $fn$;
CREATE OR REPLACE FUNCTION pg_temp.mqtt_assign(value JSONB, path TEXT[], replacement JSONB) RETURNS JSONB
LANGUAGE plpgsql AS $fn$
DECLARE head TEXT; child JSONB; position INTEGER;
BEGIN
  IF cardinality(path)=0 THEN RETURN replacement; END IF;
  head := path[1];
  IF value IS NULL THEN value := CASE WHEN head ~ '^(0|[1-9][0-9]*)$' THEN '[]'::jsonb ELSE '{}'::jsonb END; END IF;
  IF jsonb_typeof(value)='array' THEN
    position := head::integer;
    IF position<0 OR position>4096 THEN RAISE EXCEPTION 'MQTT template array index is invalid'; END IF;
    WHILE jsonb_array_length(value)<=position LOOP value := value || 'null'::jsonb; END LOOP;
    child := value->position;
  ELSE child := value->head;
  END IF;
  IF child='null'::jsonb THEN child := NULL; END IF;
  RETURN jsonb_set(value,ARRAY[head],pg_temp.mqtt_assign(child,path[2:],replacement),TRUE);
END $fn$;
CREATE OR REPLACE FUNCTION pg_temp.mqtt_rewrite(value JSONB, identity_name TEXT, uuid_name TEXT,
  request_name TEXT, values_tree JSONB) RETURNS JSONB LANGUAGE plpgsql AS $fn$
DECLARE result JSONB; entry RECORD; child JSONB;
BEGIN
  IF jsonb_typeof(value)='string' THEN
    RETURN CASE value#>>'{}'
      WHEN '$deviceCode' THEN to_jsonb('$param:' || identity_name)
      WHEN '$deviceId' THEN to_jsonb('$param:' || uuid_name)
      WHEN '$commandId' THEN to_jsonb('$point:' || request_name)
      WHEN '$time' THEN '0'::jsonb
      WHEN '$values' THEN values_tree
      ELSE value END;
  ELSIF jsonb_typeof(value)='object' THEN
    result := '{}'::jsonb;
    FOR entry IN SELECT k,v FROM jsonb_each(value) AS e(k,v) LOOP
      result := result || jsonb_build_object(entry.k,pg_temp.mqtt_rewrite(entry.v,identity_name,uuid_name,request_name,values_tree));
    END LOOP;
    RETURN result;
  ELSIF jsonb_typeof(value)='array' THEN
    result := '[]'::jsonb;
    FOR child IN SELECT jsonb_array_elements(value) LOOP
      result := result || jsonb_build_array(pg_temp.mqtt_rewrite(child,identity_name,uuid_name,request_name,values_tree));
    END LOOP;
    RETURN result;
  END IF;
  RETURN value;
END $fn$;
DO $migration$
DECLARE model RECORD; configuration JSONB; definitions JSONB; identity_name TEXT; uuid_name TEXT;
  request_name TEXT; request_id TEXT; uses_uuid BOOLEAN; uses_request BOOLEAN; template TEXT;
  point JSONB; message JSONB; messages JSONB; reports JSONB; values_tree JSONB; path TEXT[]; field TEXT;
BEGIN
  FOR model IN SELECT id,config FROM protocol_config WHERE protocol='MQTT' FOR UPDATE LOOP
    configuration := model.config;
    IF configuration ? 'identityProperty' THEN CONTINUE; END IF;
    definitions := COALESCE(configuration->'parameters','[]'::jsonb);
    identity_name := '设备标识符'; uuid_name := '设备UUID'; request_name := '请求编号';
    WHILE EXISTS(SELECT 1 FROM jsonb_array_elements(definitions) AS p WHERE p->>'name'=identity_name) LOOP identity_name := identity_name || '_'; END LOOP;
    WHILE EXISTS(SELECT 1 FROM jsonb_array_elements(definitions) AS p WHERE p->>'name'=uuid_name) LOOP uuid_name := uuid_name || '_'; END LOOP;
    WHILE EXISTS(SELECT 1 FROM jsonb_array_elements(configuration->'points') AS p WHERE p->>'name'=request_name) LOOP request_name := request_name || '_'; END LOOP;
    uses_uuid := FALSE; uses_request := FALSE;
    FOR template IN SELECT configuration->>'commandTemplate'
      UNION ALL SELECT m->>'template' FROM jsonb_array_elements(COALESCE(configuration->'commands','[]'::jsonb)) AS m
    LOOP
      IF NULLIF(template,'') IS NOT NULL THEN
        uses_uuid := uses_uuid OR pg_temp.mqtt_has_token(template::jsonb,'$deviceId');
        uses_request := uses_request OR pg_temp.mqtt_has_token(template::jsonb,'$commandId');
      END IF;
    END LOOP;
    definitions := definitions || jsonb_build_array(jsonb_build_object('name',identity_name,'dataType','STRING'));
    IF uses_uuid THEN definitions := definitions || jsonb_build_array(jsonb_build_object('name',uuid_name,'dataType','STRING')); END IF;
    configuration := configuration || jsonb_build_object('identityProperty',identity_name,'parameters',definitions);
    IF uses_request THEN
      request_id := gen_random_uuid()::text;
      configuration := jsonb_set(configuration,'{points}',configuration->'points' || jsonb_build_array(
        jsonb_build_object('id',request_id,'name',request_name,'field','','dataType','DOUBLE','writable',TRUE)));
    END IF;
    values_tree := '{}'::jsonb;
    FOR point IN SELECT jsonb_array_elements(configuration->'points') LOOP
      field := point->>'field';
      IF COALESCE((point->>'writable')::boolean,FALSE) AND NULLIF(field,'') IS NOT NULL THEN
        path := CASE WHEN left(field,1)='/' THEN string_to_array(substr(field,2),'/') ELSE ARRAY[field] END;
        SELECT array_agg(replace(replace(part,'~1','/'),'~0','~') ORDER BY n) INTO path FROM unnest(path) WITH ORDINALITY AS p(part,n);
        values_tree := pg_temp.mqtt_assign(values_tree,path,to_jsonb('$point:' || (point->>'name')));
      END IF;
    END LOOP;
    IF NULLIF(configuration->>'deviceCodeField','') IS NOT NULL AND COALESCE(configuration->>'identitySource','payload')='payload' THEN
      field := configuration->>'deviceCodeField';
      path := CASE WHEN left(field,1)='/' THEN string_to_array(substr(field,2),'/') ELSE ARRAY[field] END;
      SELECT array_agg(replace(replace(part,'~1','/'),'~0','~') ORDER BY n) INTO path FROM unnest(path) WITH ORDINALITY AS p(part,n);
      values_tree := pg_temp.mqtt_assign(values_tree,path,to_jsonb('$param:' || identity_name));
    END IF;
    FOREACH field IN ARRAY ARRAY['commandTemplate','reportTemplate'] LOOP
      template := configuration->>field;
      IF NULLIF(template,'') IS NOT NULL THEN
        configuration := jsonb_set(configuration,ARRAY[field],to_jsonb(pg_temp.mqtt_rewrite(template::jsonb,identity_name,uuid_name,request_name,values_tree)::text));
      END IF;
    END LOOP;
    FOREACH field IN ARRAY ARRAY['topic','commandTopic'] LOOP
      IF configuration ? field THEN configuration := jsonb_set(configuration,ARRAY[field],to_jsonb(replace(configuration->>field,'{deviceCode}','{param:' || identity_name || '}'))); END IF;
    END LOOP;
    IF configuration ? 'reports' THEN
      reports := '[]'::jsonb;
      FOR message IN SELECT jsonb_array_elements(configuration->'reports') LOOP
        message := jsonb_set(message,'{topic}',to_jsonb(replace(message->>'topic','{deviceCode}','{param:' || identity_name || '}')));
        message := jsonb_set(message,'{reportTemplate}',to_jsonb(pg_temp.mqtt_rewrite((message->>'reportTemplate')::jsonb,identity_name,uuid_name,request_name,values_tree)::text));
        reports := reports || jsonb_build_array(message);
      END LOOP;
      configuration := jsonb_set(configuration,'{reports}',reports);
    END IF;
    IF configuration ? 'commands' THEN
      messages := '[]'::jsonb;
      FOR message IN SELECT jsonb_array_elements(configuration->'commands') LOOP
        template := message->>'template';
        IF uses_request AND pg_temp.mqtt_has_token(template::jsonb,'$commandId') THEN
          message := jsonb_set(message,'{requiredPointIds}',message->'requiredPointIds' || to_jsonb(request_id));
        END IF;
        message := jsonb_set(message,'{topic}',to_jsonb(replace(message->>'topic','{deviceCode}','{param:' || identity_name || '}')));
        message := jsonb_set(message,'{template}',to_jsonb(pg_temp.mqtt_rewrite(template::jsonb,identity_name,uuid_name,request_name,values_tree)::text));
        IF message ? 'response' THEN message := jsonb_set(message,'{response,topic}',to_jsonb(replace(message#>>'{response,topic}','{deviceCode}','{param:' || identity_name || '}'))); END IF;
        messages := messages || jsonb_build_array(message);
      END LOOP;
      configuration := jsonb_set(configuration,'{commands}',messages);
    END IF;
    IF jsonb_array_length(configuration->'points')>256 OR jsonb_array_length(definitions)>256 THEN
      RAISE EXCEPTION 'MQTT intrinsic property migration exceeds the property limit; reduce existing properties before upgrading';
    END IF;
    UPDATE device SET protocol_params=jsonb_set(protocol_params,'{parameters}',COALESCE(protocol_params->'parameters','{}'::jsonb) ||
      jsonb_build_object(identity_name,protocol_params->>'device_code') || CASE WHEN uses_uuid THEN jsonb_build_object(uuid_name,id::text) ELSE '{}'::jsonb END)
      WHERE protocol_config_id=model.id;
    UPDATE protocol_config SET config=configuration WHERE id=model.id;
  END LOOP;
END $migration$;
CREATE OR REPLACE FUNCTION bind_device_channel() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE channel link%ROWTYPE; model_protocol TEXT; model_config JSONB; registration TEXT; identity_value JSONB;
BEGIN
  SELECT * INTO STRICT channel FROM link WHERE id=NEW.link_id FOR SHARE;
  IF NEW.deleted_at IS NULL AND channel.deleted_at IS NOT NULL THEN RAISE EXCEPTION 'Cannot bind a deleted channel'; END IF;
  SELECT protocol,config INTO STRICT model_protocol,model_config FROM protocol_config WHERE id=NEW.protocol_config_id FOR SHARE;
  IF model_protocol<>channel.protocol THEN RAISE EXCEPTION 'Channel and model protocols differ'; END IF;
  IF channel.execution='edge' THEN
    NEW.protocol_params := jsonb_set(NEW.protocol_params,'{registration}','{"mode":"OFF"}');
    IF channel.protocol='Modbus' THEN NEW.protocol_params := jsonb_set(NEW.protocol_params,'{modbus_mode}',to_jsonb(CASE WHEN channel.endpoint->>'transport'='serial' THEN 'RTU'::text ELSE 'TCP'::text END)); END IF;
  END IF;
  registration := CASE upper(COALESCE(NEW.protocol_params#>>'{registration,mode}','OFF')) WHEN 'OFF' THEN ''
    WHEN 'HEX' THEN 'HEX:' || upper(regexp_replace(COALESCE(NEW.protocol_params#>>'{registration,content}',''),'\s','','g'))
    ELSE 'ASCII:' || COALESCE(NEW.protocol_params#>>'{registration,content}','') END;
  IF channel.protocol='MQTT' THEN
    identity_value := NEW.protocol_params#>ARRAY['parameters',model_config->>'identityProperty'];
    IF NEW.deleted_at IS NULL AND (jsonb_typeof(identity_value) IS DISTINCT FROM 'string' OR identity_value#>>'{}'='') THEN
      RAISE EXCEPTION 'MQTT device identifier property is required' USING ERRCODE='23514';
    END IF;
  END IF;
  NEW.protocol_address := CASE channel.protocol
    WHEN 'SL651' THEN NEW.protocol_params->>'device_code'
    WHEN 'MQTT' THEN jsonb_build_array(COALESCE(NEW.protocol_params->>'target_id',''),identity_value#>>'{}')::text
    WHEN 'DLT645' THEN jsonb_build_array(COALESCE(NEW.protocol_params->>'target_id',''),registration,NEW.protocol_params->>'device_code')::text
    WHEN 'Modbus' THEN jsonb_build_array(COALESCE(NEW.protocol_params->>'target_id',''),registration,COALESCE((NEW.protocol_params->>'slave_id')::integer,1))::text
    ELSE jsonb_build_array(COALESCE(NEW.protocol_params->>'target_id',''),registration)::text END;
  RETURN NEW;
END $fn$;
UPDATE device d SET protocol_params=d.protocol_params FROM protocol_config p WHERE p.id=d.protocol_config_id AND p.protocol='MQTT';
CREATE OR REPLACE FUNCTION refresh_mqtt_device_identity() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF NEW.protocol='MQTT' AND NEW.config->>'identityProperty' IS DISTINCT FROM OLD.config->>'identityProperty' THEN
    UPDATE device SET protocol_params=protocol_params WHERE protocol_config_id=NEW.id;
  END IF;
  RETURN NEW;
END $fn$;
DROP TRIGGER IF EXISTS refresh_mqtt_device_identity ON protocol_config;
CREATE TRIGGER refresh_mqtt_device_identity AFTER UPDATE OF config ON protocol_config
  FOR EACH ROW EXECUTE FUNCTION refresh_mqtt_device_identity();
$mqtt_schema$; END $mqtt_intrinsic$;
)sql";
} // namespace service::config
