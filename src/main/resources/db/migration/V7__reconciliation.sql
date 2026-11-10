-- Existing simulator shipments enter the first manifest window after migration.
ALTER TABLE shipment ADD COLUMN recorded_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX shipment_manifest ON shipment(recorded_at,id);
CREATE INDEX fulfillment_order_manifest ON fulfillment_order(created_at,id);

CREATE TABLE reconciliation_schedule (
    singleton boolean PRIMARY KEY CHECK(singleton),
    next_start timestamptz NOT NULL
);
INSERT INTO reconciliation_schedule VALUES (true,date_trunc('hour',now()));

CREATE TABLE reconciliation_run (
    id uuid PRIMARY KEY, starts_at timestamptz NOT NULL, ends_at timestamptz NOT NULL,
    phase text NOT NULL DEFAULT 'ORDERS', cursor_at timestamptz, cursor_id uuid,
    revision bigint NOT NULL DEFAULT 0, pages bigint NOT NULL DEFAULT 0,
    failures integer NOT NULL DEFAULT 0, last_error text, ready_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(starts_at,ends_at), CHECK(starts_at<ends_at)
);
CREATE INDEX reconciliation_ready ON reconciliation_run(ready_at,id) WHERE phase<>'DONE';

CREATE TABLE reconciliation_issue (
    id uuid PRIMARY KEY, order_id uuid NOT NULL, kind text NOT NULL, status text NOT NULL,
    first_evidence text NOT NULL, latest_evidence text NOT NULL, run_id uuid NOT NULL REFERENCES reconciliation_run,
    first_seen timestamptz NOT NULL DEFAULT now(), last_seen timestamptz NOT NULL DEFAULT now(),
    UNIQUE(order_id,kind)
);
CREATE TABLE reconciliation_review (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issue_id uuid NOT NULL REFERENCES reconciliation_issue, actor text NOT NULL, note text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
