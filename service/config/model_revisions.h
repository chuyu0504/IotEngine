#pragma once

#include <string_view>

namespace service::config {

inline constexpr std::string_view kModelRevisionMigration = R"sql(
DO $schema$ BEGIN
ALTER TABLE protocol_config ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK(revision > 0);
CREATE TABLE protocol_revision (
  id UUID NOT NULL REFERENCES protocol_config(id),
  revision BIGINT NOT NULL CHECK(revision > 0),
  protocol VARCHAR(20) NOT NULL,
  name VARCHAR(64) NOT NULL,
  config JSONB NOT NULL CHECK(jsonb_typeof(config) = 'object'),
  remark TEXT,
  created_by UUID NOT NULL REFERENCES sys_user(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  origin TEXT NOT NULL CHECK(origin IN ('migration_baseline','published')),
  PRIMARY KEY(id,revision)
);
INSERT INTO protocol_revision(id,revision,protocol,name,config,remark,created_by,origin)
SELECT id,revision,protocol,name,config,remark,created_by,'migration_baseline' FROM protocol_config;

CREATE FUNCTION reject_model_revision_mutation() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  RAISE EXCEPTION 'Published model revisions are immutable';
END $fn$;
CREATE TRIGGER immutable_model_revision BEFORE UPDATE OR DELETE ON protocol_revision
  FOR EACH ROW EXECUTE FUNCTION reject_model_revision_mutation();

CREATE FUNCTION version_protocol_config() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF TG_OP = 'INSERT' THEN NEW.revision := 1;
  ELSIF (NEW.config,NEW.name,NEW.remark) IS DISTINCT FROM (OLD.config,OLD.name,OLD.remark) THEN
    NEW.revision := OLD.revision + 1;
  ELSE NEW.revision := OLD.revision;
  END IF;
  RETURN NEW;
END $fn$;
CREATE TRIGGER version_protocol BEFORE INSERT OR UPDATE ON protocol_config
  FOR EACH ROW EXECUTE FUNCTION version_protocol_config();

CREATE FUNCTION retain_protocol_revision() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF TG_OP = 'INSERT' OR NEW.revision <> OLD.revision THEN
    INSERT INTO protocol_revision(id,revision,protocol,name,config,remark,created_by,origin)
    VALUES(NEW.id,NEW.revision,NEW.protocol,NEW.name,NEW.config,NEW.remark,NEW.created_by,'published');
  END IF;
  RETURN NULL;
END $fn$;
CREATE TRIGGER retain_protocol AFTER INSERT OR UPDATE ON protocol_config
  FOR EACH ROW EXECUTE FUNCTION retain_protocol_revision();

ALTER TABLE device ADD COLUMN protocol_revision BIGINT;
UPDATE device SET protocol_revision = 1;
ALTER TABLE device ALTER COLUMN protocol_revision SET NOT NULL;
ALTER TABLE device ADD CONSTRAINT device_model_revision_fk
  FOREIGN KEY(protocol_config_id,protocol_revision) REFERENCES protocol_revision(id,revision);

CREATE FUNCTION bind_device_model() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF NEW.protocol_revision IS NULL THEN
    SELECT revision INTO NEW.protocol_revision FROM protocol_config WHERE id=NEW.protocol_config_id;
  END IF;
  RETURN NEW;
END $fn$;
CREATE TRIGGER bind_model BEFORE INSERT ON device
  FOR EACH ROW EXECUTE FUNCTION bind_device_model();

CREATE VIEW device_model AS
SELECT d.id AS device_id, r.id,r.revision,r.protocol,r.name,r.config,r.remark,
       r.created_by,r.created_at,r.created_at AS updated_at,p.enabled,p.deleted_at
FROM device d
JOIN protocol_revision r ON r.id=d.protocol_config_id AND r.revision=d.protocol_revision
JOIN protocol_config p ON p.id=r.id;

ALTER TABLE device_data ADD COLUMN model_id UUID;
ALTER TABLE device_data ADD COLUMN model_revision BIGINT;
ALTER TABLE device_data ADD CONSTRAINT telemetry_model_revision_fk
  FOREIGN KEY(model_id,model_revision) REFERENCES protocol_revision(id,revision);
ALTER TABLE command_operation ADD COLUMN model_id UUID;
ALTER TABLE command_operation ADD COLUMN model_revision BIGINT;
ALTER TABLE command_operation ADD CONSTRAINT command_model_revision_fk
  FOREIGN KEY(model_id,model_revision) REFERENCES protocol_revision(id,revision);
END $schema$;
)sql";

} // namespace service::config
