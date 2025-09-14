import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection string
pg_url = (
    f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
)
engine = create_engine(pg_url, future=True)


def run():
    # 1) Apply base schema (tables only)
    with engine.begin() as conn:
        conn.execute(text(open("src/schema.sql", "r", encoding="utf-8").read()))

    # 2) Extract from API
    r = requests.get(os.getenv("API_URL"), timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json()).rename(columns={"userId": "user_id"})

    # 3) Load into Postgres (idempotent upsert)
    with engine.begin() as conn:
        tmp = "tmp_raw_posts"
        df.to_sql(tmp, conn, if_exists="replace", index=False)

        conn.execute(text("""
            INSERT INTO raw_posts (id, user_id, title, body)
            SELECT id, user_id, title, body FROM tmp_raw_posts
            ON CONFLICT (id) DO UPDATE
            SET user_id = EXCLUDED.user_id,
                title    = EXCLUDED.title,
                body     = EXCLUDED.body;
            DROP TABLE tmp_raw_posts;
        """))

        # dim table + safe (re)build of the aggregate VIEW
        conn.execute(text("""
            INSERT INTO dim_users(user_id)
            SELECT DISTINCT user_id FROM raw_posts
            ON CONFLICT (user_id) DO NOTHING;

            -- Safely drop fct_posts_per_user whether it is a VIEW or a TABLE
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'fct_posts_per_user') THEN
                    EXECUTE 'DROP VIEW fct_posts_per_user';
                ELSIF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'fct_posts_per_user') THEN
                    EXECUTE 'DROP TABLE fct_posts_per_user';
                END IF;
            END$$;

            CREATE VIEW fct_posts_per_user AS
            SELECT user_id, COUNT(*) AS n_posts
            FROM raw_posts
            GROUP BY user_id;
        """))


if __name__ == "__main__":
    run()
