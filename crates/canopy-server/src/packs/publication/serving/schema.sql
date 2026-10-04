-- Bounded read retention, not a creating namespace or historical object table.
-- Expired readers stop serving but remain GC roots until physical work drains.
CREATE TABLE catalog_serving_pins (
    reader BLOB PRIMARY KEY CHECK(typeof(reader)='blob' AND length(reader)=16 AND reader!=zeroblob(16)),
    incarnation BLOB NOT NULL CHECK(typeof(incarnation)='blob' AND length(incarnation)=16),
    admission_sequence INTEGER NOT NULL CHECK(typeof(admission_sequence)='integer' AND admission_sequence>0),
    owner_epoch BLOB NOT NULL CHECK(typeof(owner_epoch)='blob' AND length(owner_epoch)=8 AND owner_epoch!=zeroblob(8)),
    generation INTEGER NOT NULL REFERENCES catalog_generations(generation) CHECK(typeof(generation)='integer' AND generation>0),
    expires_at_ms INTEGER NOT NULL CHECK(typeof(expires_at_ms)='integer' AND expires_at_ms>=0),
    UNIQUE(incarnation,admission_sequence)
) WITHOUT ROWID;
CREATE INDEX catalog_serving_pins_by_generation ON catalog_serving_pins(generation);
CREATE TRIGGER catalog_serving_pin_not_replaced BEFORE INSERT ON catalog_serving_pins
WHEN EXISTS(SELECT 1 FROM catalog_serving_pins WHERE reader=NEW.reader OR (incarnation=NEW.incarnation AND admission_sequence=NEW.admission_sequence))
BEGIN SELECT RAISE(ABORT,'serving pin cannot be replaced'); END;
CREATE TRIGGER catalog_serving_pin_identity_immutable BEFORE UPDATE ON catalog_serving_pins
WHEN NEW.reader IS NOT OLD.reader OR NEW.incarnation IS NOT OLD.incarnation
  OR NEW.admission_sequence IS NOT OLD.admission_sequence OR NEW.owner_epoch IS NOT OLD.owner_epoch
  OR NEW.generation IS NOT OLD.generation OR NEW.expires_at_ms<OLD.expires_at_ms
BEGIN SELECT RAISE(ABORT,'serving pin identity cannot change'); END;
CREATE TRIGGER catalog_serving_pins_bounded BEFORE INSERT ON catalog_serving_pins
WHEN (SELECT count(*) FROM (SELECT reader FROM catalog_serving_pins LIMIT 4096))>=4096
BEGIN SELECT RAISE(ABORT,'serving pin capacity'); END;
