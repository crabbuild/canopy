-- Ephemeral direct-ref publication predicates. They are independent of the
-- catalog/ref generation, allowing unrelated root advancement during paging.
CREATE TABLE ref_policy_epoch (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    version INTEGER NOT NULL CHECK(typeof(version)='integer' AND version>=0)
) WITHOUT ROWID;
INSERT INTO ref_policy_epoch VALUES(1,0);
CREATE TRIGGER ref_policy_epoch_monotonic BEFORE UPDATE ON ref_policy_epoch
WHEN NEW.singleton!=OLD.singleton OR NEW.version!=OLD.version+1
BEGIN SELECT RAISE(ABORT,'ref policy epoch must advance once'); END;
CREATE TRIGGER ref_policy_epoch_retained BEFORE DELETE ON ref_policy_epoch
BEGIN SELECT RAISE(ABORT,'ref policy epoch must be retained'); END;
CREATE TRIGGER ref_policy_epoch_not_replaced BEFORE INSERT ON ref_policy_epoch
WHEN EXISTS(SELECT 1 FROM ref_policy_epoch WHERE singleton=NEW.singleton)
BEGIN SELECT RAISE(ABORT,'ref policy epoch cannot be replaced'); END;

CREATE TABLE ref_policy_guards (
    id BLOB PRIMARY KEY CHECK(length(id)=16),
    scope BLOB NOT NULL CHECK(length(scope)=32),
    token BLOB NOT NULL CHECK(length(token) BETWEEN 1 AND 256),
    policy_epoch INTEGER NOT NULL CHECK(typeof(policy_epoch)='integer' AND policy_epoch>=0),
    total INTEGER NOT NULL CHECK(typeof(total)='integer' AND total BETWEEN 1 AND 100000),
    next INTEGER NOT NULL CHECK(typeof(next)='integer' AND next BETWEEN 0 AND total),
    valid INTEGER NOT NULL CHECK(valid IN (0,1))
) WITHOUT ROWID;
CREATE TRIGGER ref_policy_guards_immutable BEFORE UPDATE ON ref_policy_guards
WHEN NEW.id!=OLD.id OR NEW.scope!=OLD.scope OR NEW.token!=OLD.token
  OR NEW.policy_epoch!=OLD.policy_epoch OR NEW.total!=OLD.total
  OR NEW.next<OLD.next OR NEW.valid>OLD.valid
BEGIN SELECT RAISE(ABORT,'ref policy guard cannot reset'); END;
CREATE TRIGGER ref_policy_guards_not_replaced BEFORE INSERT ON ref_policy_guards
WHEN EXISTS(SELECT 1 FROM ref_policy_guards WHERE id=NEW.id)
BEGIN SELECT RAISE(ABORT,'ref policy guard cannot be replaced'); END;

CREATE TABLE ref_policy_watches (
    guard BLOB NOT NULL REFERENCES ref_policy_guards(id),
    oid BLOB NOT NULL CHECK(length(oid) IN (20,32)),
    context TEXT NOT NULL,
    context_version INTEGER NOT NULL CHECK(typeof(context_version)='integer' AND context_version>0),
    run_number INTEGER NOT NULL CHECK(typeof(run_number)='integer' AND run_number>0),
    PRIMARY KEY(guard,oid,context,context_version,run_number)
) WITHOUT ROWID;
CREATE INDEX ref_policy_watches_by_check
ON ref_policy_watches(oid,context,context_version,run_number,guard);
CREATE TRIGGER ref_policy_watches_immutable BEFORE UPDATE ON ref_policy_watches
BEGIN SELECT RAISE(ABORT,'ref policy watch is immutable'); END;
CREATE TRIGGER ref_policy_watches_not_replaced BEFORE INSERT ON ref_policy_watches
WHEN EXISTS(SELECT 1 FROM ref_policy_watches WHERE guard=NEW.guard AND oid=NEW.oid
 AND context=NEW.context AND context_version=NEW.context_version AND run_number=NEW.run_number)
BEGIN SELECT RAISE(ABORT,'ref policy watch cannot be replaced'); END;
CREATE TRIGGER ref_policy_watches_live BEFORE DELETE ON ref_policy_watches
WHEN EXISTS(SELECT 1 FROM ref_policy_guards WHERE id=OLD.guard AND valid=1)
BEGIN SELECT RAISE(ABORT,'live ref policy watch must be retained'); END;
CREATE TABLE ref_policy_budget (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    watches INTEGER NOT NULL CHECK(typeof(watches)='integer' AND watches BETWEEN 0 AND 2097152)
) WITHOUT ROWID;
INSERT INTO ref_policy_budget VALUES(1,0);
CREATE TRIGGER ref_policy_budget_retained BEFORE DELETE ON ref_policy_budget
BEGIN SELECT RAISE(ABORT,'ref policy budget must be retained'); END;
CREATE TRIGGER ref_policy_budget_not_replaced BEFORE INSERT ON ref_policy_budget
WHEN EXISTS(SELECT 1 FROM ref_policy_budget WHERE singleton=NEW.singleton)
BEGIN SELECT RAISE(ABORT,'ref policy budget cannot be replaced'); END;
CREATE TRIGGER ref_policy_required_capacity_insert BEFORE INSERT ON branch_required_checks
WHEN NOT EXISTS(SELECT 1 FROM branch_required_checks WHERE reference=NEW.reference AND context=NEW.context)
 AND (SELECT count(*) FROM branch_required_checks WHERE reference=NEW.reference)>=16
BEGIN SELECT RAISE(ABORT,'too many required branch checks'); END;
CREATE TRIGGER ref_policy_required_capacity_update BEFORE UPDATE ON branch_required_checks
WHEN NEW.reference!=OLD.reference
 AND NOT EXISTS(SELECT 1 FROM branch_required_checks WHERE reference=NEW.reference AND context=NEW.context)
 AND (SELECT count(*) FROM branch_required_checks WHERE reference=NEW.reference)>=16
BEGIN SELECT RAISE(ABORT,'too many required branch checks'); END;

-- Rule/context configuration changes are rare. Actual run reports do not
-- advance this epoch; their exact dependencies are invalidated below.
CREATE TRIGGER ref_policy_rule_insert AFTER INSERT ON branch_rules
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;
CREATE TRIGGER ref_policy_rule_update AFTER UPDATE ON branch_rules
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;
CREATE TRIGGER ref_policy_rule_delete AFTER DELETE ON branch_rules
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;
CREATE TRIGGER ref_policy_required_insert AFTER INSERT ON branch_required_checks
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;
CREATE TRIGGER ref_policy_required_update AFTER UPDATE ON branch_required_checks
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;
CREATE TRIGGER ref_policy_required_delete AFTER DELETE ON branch_required_checks
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;
CREATE TRIGGER ref_policy_context_insert AFTER INSERT ON check_contexts
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;
CREATE TRIGGER ref_policy_context_update AFTER UPDATE ON check_contexts
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;
CREATE TRIGGER ref_policy_context_delete AFTER DELETE ON check_contexts
BEGIN UPDATE ref_policy_epoch SET version=version+1 WHERE singleton=1; END;

-- The watched run is exactly the newest attempt of the required version.
-- Newer attempts invalidate even when queued. Older/different-version reports
-- and unrelated commits/contexts leave the guard unchanged.
-- REPLACE may suppress DELETE triggers. Observe the row it would replace by
-- either unique key before insertion, even when the replacement changes pair.
CREATE TRIGGER ref_policy_check_replacement BEFORE INSERT ON check_runs
BEGIN
    UPDATE ref_policy_guards SET valid=0 WHERE valid=1 AND id IN (
        SELECT w.guard FROM check_runs r JOIN ref_policy_watches w
          ON w.oid=r.oid AND w.context=r.context
          AND w.context_version=r.context_version AND w.run_number=r.number
        WHERE r.number=NEW.number OR r.id=NEW.id
    );
END;
CREATE TRIGGER ref_policy_check_insert AFTER INSERT ON check_runs
BEGIN
    UPDATE ref_policy_guards SET valid=0 WHERE valid=1 AND id IN (
        SELECT guard FROM ref_policy_watches
        WHERE oid=NEW.oid AND context=NEW.context
          AND context_version=NEW.context_version AND run_number<=NEW.number
    );
END;
CREATE TRIGGER ref_policy_check_update AFTER UPDATE ON check_runs
BEGIN
    UPDATE ref_policy_guards SET valid=0 WHERE valid=1 AND id IN (
        SELECT guard FROM ref_policy_watches
        WHERE oid=OLD.oid AND context=OLD.context
          AND context_version=OLD.context_version AND run_number=OLD.number
        UNION
        SELECT guard FROM ref_policy_watches
        WHERE oid=NEW.oid AND context=NEW.context
          AND context_version=NEW.context_version AND run_number<=NEW.number
    );
END;
CREATE TRIGGER ref_policy_check_delete AFTER DELETE ON check_runs
BEGIN
    UPDATE ref_policy_guards SET valid=0 WHERE valid=1 AND id IN (
        SELECT guard FROM ref_policy_watches
        WHERE oid=OLD.oid AND context=OLD.context
          AND context_version=OLD.context_version AND run_number=OLD.number
    );
END;
