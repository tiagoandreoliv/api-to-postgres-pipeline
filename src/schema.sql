CREATE TABLE IF NOT EXISTS raw_posts (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  title TEXT,
  body TEXT,
  _ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_users (
  user_id INTEGER PRIMARY KEY
);
