#!/usr/bin/env bash
# =============================================================================
# init-db.sh  â€” Schema creation + idempotent seeding of 10 000 000 rows
# =============================================================================
# This script is mounted at /docker-entrypoint-initdb.d/ and runs once when
# the PostgreSQL data directory is first initialised (i.e. on a fresh volume).
# The seeding is also guarded by a row-count check so re-runs are safe.
# =============================================================================
set -euo pipefail

echo ">>> [init-db] Creating schema â€¦"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'

    -- -------------------------------------------------------------------------
    -- Schema
    -- -------------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS public.records (
        id          BIGSERIAL                    PRIMARY KEY,
        created_at  TIMESTAMP WITH TIME ZONE     NOT NULL DEFAULT NOW(),
        name        VARCHAR(255)                 NOT NULL,
        value       DECIMAL(18, 4)               NOT NULL,
        metadata    JSONB                        NOT NULL
    );

    -- -------------------------------------------------------------------------
    -- Idempotent seed: only insert if fewer than 10 000 000 rows exist
    -- -------------------------------------------------------------------------
    DO $$
    DECLARE
        existing_count BIGINT;
        target_count   BIGINT := 10000000;
    BEGIN
        SELECT COUNT(*) INTO existing_count FROM public.records;

        IF existing_count >= target_count THEN
            RAISE NOTICE 'Table already contains % rows â€” skipping seed.', existing_count;
        ELSE
            RAISE NOTICE 'Seeding % rows â€¦', target_count;

            INSERT INTO public.records (created_at, name, value, metadata)
            SELECT
                NOW() - (random() * INTERVAL '365 days'),
                'Record_' || gs::TEXT,
                ROUND((random() * 99999)::NUMERIC, 4),
                jsonb_build_object(
                    'index',    gs,
                    'category', (ARRAY['alpha','beta','gamma','delta','epsilon'])[1 + (gs % 5)],
                    'active',   (gs % 2 = 0),
                    'tags',     jsonb_build_array('tag_' || (gs % 10), 'tag_' || (gs % 7)),
                    'address',  jsonb_build_object(
                                    'city',    'City_' || (gs % 100),
                                    'zip',     LPAD((gs % 99999)::TEXT, 5, '0'),
                                    'country', (ARRAY['US','UK','DE','FR','JP'])[1 + (gs % 5)]
                                )
                )
            FROM generate_series(1, target_count) AS gs;

            RAISE NOTICE 'Seeding complete.';
        END IF;
    END;
    $$;

EOSQL

echo ">>> [init-db] Done."
