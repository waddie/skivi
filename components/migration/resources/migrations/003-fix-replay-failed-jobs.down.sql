-- Restore original replay_failed_jobs body (pre-003).
-- Keeps 5-parameter signature so callers passing max_attempts_default continue to work.
CREATE OR REPLACE FUNCTION ${migratus.schema}.replay_failed_jobs(
    from_time              TIMESTAMPTZ,
    to_time                TIMESTAMPTZ,
    task_identifier_filter TEXT        DEFAULT NULL,
    payload_transformer    TEXT        DEFAULT NULL,
    max_attempts_default   INTEGER     DEFAULT 25
) RETURNS SETOF ${migratus.schema}.jobs AS $$
DECLARE
    v_record  RECORD;
    v_job     ${migratus.schema}.jobs;
    v_payload JSONB;
BEGIN
    FOR v_record IN
        SELECT DISTINCT ON (job_id) *
        FROM ${migratus.schema}.job_history
        WHERE status = 'failed'
          AND created_at BETWEEN from_time AND to_time
          AND (task_identifier_filter IS NULL OR task_identifier = task_identifier_filter)
        ORDER BY job_id, created_at DESC
    LOOP
        IF payload_transformer IS NOT NULL THEN
            EXECUTE format('SELECT %I($1)', payload_transformer)
            INTO v_payload
            USING v_record.payload;
        ELSE
            v_payload := v_record.payload;
        END IF;

        SELECT * INTO v_job FROM ${migratus.schema}.add_job(
            v_record.task_identifier,
            v_payload,
            NULL,
            now(),
            0,
            25,
            'replay-' || v_record.correlation_id::text,
            'replace',
            ARRAY['replay']
        );

        RETURN NEXT v_job;
    END LOOP;
END;
$$ LANGUAGE plpgsql
