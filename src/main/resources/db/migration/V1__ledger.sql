CREATE TABLE fulfillment_order (
 id uuid PRIMARY KEY, tenant text NOT NULL, request_key text NOT NULL, sku text NOT NULL,
 quantity integer NOT NULL CHECK(quantity > 0), warehouse text NOT NULL, state text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant,request_key)
);
CREATE TABLE history (
 id uuid PRIMARY KEY, order_id uuid NOT NULL REFERENCES fulfillment_order, state text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(order_id,state)
);
CREATE TABLE work_item (
 id uuid PRIMARY KEY, order_id uuid NOT NULL REFERENCES fulfillment_order, kind text NOT NULL,
 payload text NOT NULL, available_at timestamptz NOT NULL DEFAULT now(), token bigint NOT NULL DEFAULT 0,
 attempts integer NOT NULL DEFAULT 0, done boolean NOT NULL DEFAULT false, UNIQUE(order_id,kind,payload)
);
CREATE INDEX work_ready ON work_item(kind,available_at) WHERE NOT done;
CREATE TABLE timeline (event_id uuid PRIMARY KEY, order_id uuid NOT NULL, state text NOT NULL, sequence_no int NOT NULL);
CREATE INDEX timeline_order ON timeline(order_id,sequence_no);
CREATE TABLE shipment (id uuid PRIMARY KEY, warehouse text NOT NULL, sku text NOT NULL, quantity integer NOT NULL);
CREATE TABLE warehouse_directory (id text PRIMARY KEY, name text NOT NULL);
INSERT INTO warehouse_directory VALUES ('A','East warehouse'),('B','West warehouse');
