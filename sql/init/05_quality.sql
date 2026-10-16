-- Phase 6: data-quality results. Idempotent (make db-migrate).

-- One row per check per run, so the dashboard can show the latest run and the
-- history of any check.
CREATE TABLE IF NOT EXISTS dq_results (
    run_id     TEXT NOT NULL,
    ran_at     TIMESTAMPTZ NOT NULL,
    layer      TEXT NOT NULL,                -- silver | gold | live | stream
    check_name TEXT NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
    observed   TEXT NOT NULL,
    expected   TEXT NOT NULL,
    details    TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, check_name)
);
CREATE INDEX IF NOT EXISTS dq_results_ran_at ON dq_results (ran_at DESC);

GRANT SELECT ON dq_results TO retail_reader;
