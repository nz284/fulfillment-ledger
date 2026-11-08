CREATE TABLE dead_letter_replay (
    consumer_group text NOT NULL,
    source_topic text NOT NULL,
    source_partition integer NOT NULL,
    source_offset bigint NOT NULL,
    event_id uuid NOT NULL,
    order_id uuid NOT NULL,
    state text NOT NULL,
    replayed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group,source_topic,source_partition,source_offset)
);
