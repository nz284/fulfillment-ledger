ALTER TABLE reconciliation_issue ADD COLUMN search_version bigint NOT NULL DEFAULT 1;
CREATE INDEX reconciliation_review_issue ON reconciliation_review(issue_id,id);

-- Coalesce unpublished changes per issue. A publisher may remove only the version it acknowledged.
CREATE TABLE issue_search_outbox (
    issue_id uuid PRIMARY KEY REFERENCES reconciliation_issue,
    version bigint NOT NULL,
    queued_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX issue_search_outbox_ready ON issue_search_outbox(queued_at,issue_id);

CREATE FUNCTION increment_issue_search_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.search_version := OLD.search_version + 1;
    RETURN NEW;
END $$;
CREATE TRIGGER issue_search_version BEFORE UPDATE ON reconciliation_issue
FOR EACH ROW EXECUTE FUNCTION increment_issue_search_version();

CREATE FUNCTION enqueue_issue_search() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO issue_search_outbox(issue_id,version) VALUES (NEW.id,NEW.search_version)
    ON CONFLICT(issue_id) DO UPDATE SET version=excluded.version;
    RETURN NEW;
END $$;
CREATE TRIGGER issue_search_changed AFTER INSERT OR UPDATE ON reconciliation_issue
FOR EACH ROW EXECUTE FUNCTION enqueue_issue_search();

-- A note is append-only. Bump the issue version in the same transaction, including direct note inserts.
CREATE FUNCTION enqueue_review_search() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE reconciliation_issue SET search_version=search_version+1 WHERE id=NEW.issue_id;
    RETURN NEW;
END $$;
CREATE TRIGGER issue_search_reviewed AFTER INSERT ON reconciliation_review
FOR EACH ROW EXECUTE FUNCTION enqueue_review_search();

CREATE TABLE issue_search_generation (
    singleton boolean PRIMARY KEY CHECK(singleton),
    active_index text, building_index text, cursor_id uuid, retired_index text
);
INSERT INTO issue_search_generation(singleton) VALUES(true);
