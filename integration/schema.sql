CREATE TABLE records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    key         TEXT NOT NULL,
    rev         INTEGER NOT NULL,
    written_at  INTEGER NOT NULL,
    removed_at  INTEGER,
    label       TEXT,
    payload     BLOB,
    enabled     INTEGER,
    UNIQUE (key, rev)
);
CREATE INDEX records_key_idx ON records (key);
CREATE INDEX records_rev_idx ON records (rev);
CREATE INDEX records_removed_at_idx ON records (removed_at);

-- Reject a new version older than the current max for its key.
CREATE TRIGGER records_monotonic BEFORE INSERT ON records FOR EACH ROW
BEGIN
    SELECT CASE WHEN NEW.rev < (SELECT MAX(rev) FROM records WHERE key = NEW.key)
        THEN RAISE(ABORT, 'rev must not decrease for key') END;
END;

-- A newer version supersedes (soft-deletes) older versions of the same key.
CREATE TRIGGER records_supersede AFTER INSERT ON records FOR EACH ROW
BEGIN
    UPDATE records SET removed_at = NEW.written_at
    WHERE removed_at IS NULL AND key = NEW.key AND rev < NEW.rev;
END;

CREATE TABLE pending_changes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    record_table TEXT NOT NULL,
    record_id    INTEGER NOT NULL,
    change_kind  TEXT NOT NULL,
    not_before   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX pending_changes_record_idx ON pending_changes (record_table, record_id);

CREATE TRIGGER records_notify_insert AFTER INSERT ON records FOR EACH ROW
BEGIN
    INSERT INTO pending_changes (record_table, record_id, change_kind)
    VALUES ('records', NEW.id, 'INSERT');
END;
CREATE TRIGGER records_notify_insert_removed AFTER INSERT ON records FOR EACH ROW
WHEN NEW.removed_at IS NOT NULL
BEGIN
    INSERT INTO pending_changes (record_table, record_id, change_kind, not_before)
    VALUES ('records', NEW.id, 'REMOVE', NEW.removed_at);
END;
CREATE TRIGGER records_notify_update AFTER UPDATE ON records FOR EACH ROW
WHEN NEW.label IS NOT OLD.label OR NEW.payload IS NOT OLD.payload OR NEW.enabled IS NOT OLD.enabled
BEGIN
    INSERT INTO pending_changes (record_table, record_id, change_kind)
    VALUES ('records', NEW.id, 'UPDATE');
END;
CREATE TRIGGER records_notify_update_removed AFTER UPDATE ON records FOR EACH ROW
WHEN NEW.removed_at IS NOT NULL AND OLD.removed_at IS NULL
BEGIN
    INSERT INTO pending_changes (record_table, record_id, change_kind, not_before)
    VALUES ('records', NEW.id, 'REMOVE', NEW.removed_at);
END;
CREATE TRIGGER records_notify_delete AFTER DELETE ON records FOR EACH ROW
BEGIN
    DELETE FROM pending_changes WHERE record_table = 'records' AND record_id = OLD.id;
END;
