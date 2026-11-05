-- Database setup for the real-time e-commerce streaming pipeline.
-- Run from a terminal:  psql -U postgres -f postgres_setup.sql
-- The script can be run more than once without errors.

-- 1. Application user (development password, see postgres_connection_details.txt)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'streaming_user') THEN
        CREATE ROLE streaming_user LOGIN PASSWORD 'streaming_pass';
    END IF;
END
$$;

-- 2. Database (CREATE DATABASE has no IF NOT EXISTS, so psql runs the generated statement)
SELECT 'CREATE DATABASE ecommerce_streaming OWNER postgres'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ecommerce_streaming')
\gexec

GRANT CONNECT ON DATABASE ecommerce_streaming TO streaming_user;

-- 3. Switch to the new database
\connect ecommerce_streaming

-- 4. Events table (cleaned events written by the Spark job)
CREATE TABLE IF NOT EXISTS events (
    event_id      VARCHAR(36)    PRIMARY KEY,
    user_id       VARCHAR(10)    NOT NULL,
    event_type    VARCHAR(20)    NOT NULL CHECK (event_type IN ('view', 'purchase')),
    product_id    VARCHAR(10)    NOT NULL,
    product_name  VARCHAR(100)   NOT NULL,
    category      VARCHAR(50)    NOT NULL,
    price         NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    quantity      INTEGER        NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    event_time    TIMESTAMP      NOT NULL,
    ingested_at   TIMESTAMP      NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_event_time ON events (event_time);

-- 5. Permissions for the application user
GRANT SELECT, INSERT ON events TO streaming_user;