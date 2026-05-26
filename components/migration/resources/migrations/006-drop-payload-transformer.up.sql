-- Drop the dead payload_transformer parameter from replay_failed_jobs.
-- The Clojure layer has always passed NULL; removing it closes the footgun
-- of direct DB callers passing arbitrary function names.
CREATE OR REPLACE FUNCTION ${migratus.schema}.replay_failed_jobs(
    from_time              TIMESTAMPTZ,
    to_time                TIMESTAMPTZ,
    task_identifier_filter TEXT        DEFAULT NULL,
    max_attempts_default   INTEGER     DEFAULT 25
) RETURNS SETOF ${migratus.schema}.jobs AS $$
DECLARE
    v_record  RECORD;
    v_job     ${migratus.schema}.jobs;
BEGIN
    FOR v_record IN
        SELECT DISTINCT ON (job_id) *
        FROM ${migratus.schema}.job_history
        WHERE status = 'failed'
          AND started_at BETWEEN from_time AND to_time
          AND (task_identifier_filter IS NULL OR task_identifier = task_identifier_filter)
        ORDER BY job_id, started_at DESC
    LOOP
        SELECT * INTO v_job FROM ${migratus.schema}.add_job(
            v_record.task_identifier,
            v_record.payload,
            NULL,
            now(),
            0,
            max_attempts_default,
            v_record.job_id::text,
            'replace',
            ARRAY['replay']
        );

        RETURN NEXT v_job;
    END LOOP;
END;
$$ LANGUAGE plpgsql
