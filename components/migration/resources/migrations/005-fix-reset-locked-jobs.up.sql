-- Fix reset_locked_jobs: change < to <= so jobs whose lock expires exactly at
-- the check instant are recovered. Matches the spec: locked_at + lock_timeout <= now.
CREATE OR REPLACE FUNCTION ${migratus.schema}.reset_locked_jobs(
    job_expiry INTERVAL DEFAULT '4 hours'
) RETURNS INTEGER AS $$
DECLARE
    v_count       INTEGER;
    v_queue_names TEXT[];
BEGIN
    WITH reset_jobs AS (
        UPDATE ${migratus.schema}.jobs
        SET locked_at = NULL, locked_by = NULL
        WHERE locked_at <= (now() - job_expiry)
        RETURNING queue_name
    )
    SELECT count(*), array_agg(DISTINCT queue_name)
    INTO v_count, v_queue_names
    FROM reset_jobs;

    UPDATE ${migratus.schema}.job_queues
    SET locked_at = NULL, locked_by = NULL
    WHERE queue_name = ANY(v_queue_names);

    RETURN coalesce(v_count, 0);
END;
$$ LANGUAGE plpgsql
