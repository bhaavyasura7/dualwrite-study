-- Schema from SPEC 5.1.

CREATE TABLE orders (
  order_id    UUID PRIMARY KEY,
  status      TEXT          NOT NULL,        -- PLACED | PAID | CANCELLED
  version     INT           NOT NULL,        -- +1 on every change; = event seq
  amount      NUMERIC(12,2) NOT NULL,
  updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Column names follow Debezium Outbox Event Router defaults
CREATE TABLE outbox (
  id             UUID PRIMARY KEY,           -- event_id
  aggregatetype  TEXT        NOT NULL,       -- 'order' (routes to topic)
  aggregateid    TEXT        NOT NULL,       -- order_id (Kafka key)
  type           TEXT        NOT NULL,       -- OrderPlaced | OrderPaid | OrderCancelled
  seq            INT         NOT NULL,       -- = orders.version
  payload        JSONB       NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at        TIMESTAMPTZ                 -- NULL = unsent (P3 relay only)
);
CREATE INDEX outbox_unsent ON outbox (created_at, id) WHERE sent_at IS NULL;  -- matches the relay ORDER BY (SPEC 8)

CREATE PUBLICATION pub_outbox FOR TABLE outbox;
CREATE PUBLICATION pub_orders FOR TABLE orders;

-- Every committed version, for the checker (Section 11.2)
CREATE TABLE orders_history (
  order_id  UUID NOT NULL,
  version   INT  NOT NULL,
  status    TEXT NOT NULL,
  PRIMARY KEY (order_id, version)
);
CREATE FUNCTION record_order_version() RETURNS trigger AS $$
BEGIN
  INSERT INTO orders_history (order_id, version, status)
  VALUES (NEW.order_id, NEW.version, NEW.status);
  RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER orders_history_trg AFTER INSERT OR UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION record_order_version();

-- Consumer side (group G): one schema, one table set PER STRATEGY.
-- All three strategies consume the same stream in parallel and never share tables.
CREATE SCHEMA consumer;
CREATE TABLE consumer.payments_naive (
  payment_id BIGSERIAL PRIMARY KEY,
  order_id UUID NOT NULL, event_key TEXT NOT NULL,
  amount NUMERIC(12,2) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);                                                 -- no unique constraint: duplicates must be visible
CREATE TABLE consumer.payments_dedup (LIKE consumer.payments_naive INCLUDING ALL);
CREATE TABLE consumer.processed_events (event_key TEXT PRIMARY KEY);   -- dedup strategy
CREATE TABLE consumer.payments_upsert (LIKE consumer.payments_naive INCLUDING ALL);
CREATE UNIQUE INDEX payments_upsert_order ON consumer.payments_upsert (order_id);
