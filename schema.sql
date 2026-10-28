CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  order_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  payload TEXT NOT NULL,
  UNIQUE(order_id, version)
);
CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  deleted INTEGER NOT NULL CHECK(deleted IN (0,1)),
  status TEXT,
  total_cents INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  digest TEXT NOT NULL,
  result TEXT NOT NULL
);
