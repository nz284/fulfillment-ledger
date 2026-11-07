CREATE INDEX oldest_open_order ON fulfillment_order(created_at) WHERE state <> 'CONFIRMED';
