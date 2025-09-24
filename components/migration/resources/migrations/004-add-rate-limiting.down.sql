-- Revert rate limiting support.
-- Restores get_jobs, add_job, replay_failed_jobs to pre-rate-limit state
-- and removes rate_limits table and rate_limit_key column.

-- Restore get_jobs without rate limit awareness.
CREATE OR REPLACE FUNCTION ${migratus.schema}.get_jobs(
    worker_id        TEXT,
    task_identifiers TEXT[]  DEFAULT NULL,
    forbidden_flags  TEXT[]  DEFAULT NULL,
    job_batch_size   INTEGER DEFAULT 1
) RETURNS SETOF ${migratus.schema}.jobs AS $$
DECLARE
    v_job_ids     UUID[];
    v_queue_names TEXT[];
BEGIN
    IF worker_id IS NULL OR length(worker_id) < 10 THEN
        RAISE EXCEPTION 'Invalid worker ID';
    END IF;

    WITH available_jobs AS (
        SELECT j.id, j.queue_name
        FROM ${migratus.schema}.jobs j
        LEFT JOIN ${migratus.schema}.job_queues jq ON j.queue_name = jq.queue_name
        WHERE j.locked_at IS NULL
          AND j.run_at <= now()
          AND j.attempts < j.max_attempts
          AND (task_identifiers IS NULL OR j.task_identifier = ANY(task_identifiers))
          AND (forbidden_flags  IS NULL OR NOT (j.flags && forbidden_flags))
          AND (j.queue_name IS NULL OR jq.locked_at IS NULL)
        ORDER BY j.priority ASC, j.run_at ASC, j.id ASC
        LIMIT job_batch_size
        FOR UPDATE OF j SKIP LOCKED
    )
    SELECT array_agg(id), array_agg(DISTINCT queue_name)
    INTO v_job_ids, v_queue_names
    FROM available_jobs;

    UPDATE ${migratus.schema}.job_queues
    SET locked_by = worker_id, locked_at = now()
    WHERE queue_name = ANY(v_queue_names)
      AND queue_name IS NOT NULL;

    UPDATE ${migratus.schema}.jobs
    SET locked_by = worker_id,
        locked_at = now(),
        attempts  = attempts + 1,
        revision  = revision + 1
    WHERE id = ANY(v_job_ids);

    RETURN QUERY
    SELECT * FROM ${migratus.schema}.jobs
    WHERE id = ANY(v_job_ids)
    ORDER BY priority ASC, run_at ASC, id ASC;
END;
$$ LANGUAGE plpgsql
--;;
DROP FUNCTION IF EXISTS ${migratus.schema}.register_rate_limit(TEXT, INTEGER, INTERVAL)
--;;
DROP FUNCTION IF EXISTS ${migratus.schema}.refill_rate_limits()
--;;
-- Drop the 10-parameter add_job before recreating the 9-parameter version.
DROP FUNCTION IF EXISTS ${migratus.schema}.add_job(TEXT, JSONB, TEXT, TIMESTAMPTZ, INTEGER, INTEGER, TEXT, TEXT, TEXT[], TEXT)
--;;
-- Restore 9-parameter add_job (from migration 002).
CREATE OR REPLACE FUNCTION ${migratus.schema}.add_job(
    task_identifier TEXT,
    payload         JSONB       DEFAULT '{}',
    queue_name      TEXT        DEFAULT NULL,
    run_at          TIMESTAMPTZ DEFAULT now(),
    priority        INTEGER     DEFAULT 0,
    max_attempts    INTEGER     DEFAULT 25,
    job_key         TEXT        DEFAULT NULL,
    job_key_mode    TEXT        DEFAULT 'replace',
    flags           TEXT[]      DEFAULT '{}'
) RETURNS ${migratus.schema}.jobs AS $$
#variable_conflict use_column
DECLARE
    v_job ${migratus.schema}.jobs;
BEGIN
    IF job_key IS NOT NULL THEN
        CASE job_key_mode
            WHEN 'replace' THEN
                DELETE FROM ${migratus.schema}.jobs WHERE job_key = add_job.job_key;

            WHEN 'preserve_run_at' THEN
                UPDATE ${migratus.schema}.jobs
                SET task_identifier = add_job.task_identifier,
                    payload         = add_job.payload,
                    queue_name      = add_job.queue_name,
                    priority        = add_job.priority,
                    max_attempts    = add_job.max_attempts,
                    flags           = add_job.flags,
                    revision        = revision + 1
                WHERE job_key = add_job.job_key
                RETURNING * INTO v_job;

                IF FOUND THEN
                    RETURN v_job;
                END IF;

            WHEN 'unsafe_dedupe' THEN
                SELECT * INTO v_job
                FROM ${migratus.schema}.jobs
                WHERE job_key = add_job.job_key;
                IF FOUND THEN
                    RETURN v_job;
                END IF;

            ELSE
                RAISE EXCEPTION 'Unknown job_key_mode: %', job_key_mode;
        END CASE;
    END IF;

    INSERT INTO ${migratus.schema}.jobs (
        task_identifier, payload, queue_name, run_at, priority,
        max_attempts, job_key, job_key_mode, flags
    ) VALUES (
        add_job.task_identifier, add_job.payload, add_job.queue_name,
        add_job.run_at, add_job.priority, add_job.max_attempts,
        add_job.job_key, add_job.job_key_mode, add_job.flags
    ) RETURNING * INTO v_job;

    INSERT INTO ${migratus.schema}.task_identifiers (identifier, last_used)
    VALUES (add_job.task_identifier, now())
    ON CONFLICT (identifier) DO UPDATE SET last_used = now();

    RETURN v_job;
END;
$$ LANGUAGE plpgsql
--;;
-- Restore replay_failed_jobs to call the 9-parameter add_job.
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
          AND started_at BETWEEN from_time AND to_time
          AND (task_identifier_filter IS NULL
               OR task_identifier = task_identifier_filter)
        ORDER BY job_id, started_at DESC
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
            max_attempts_default,
            v_record.job_id::text,
            'replace',
            ARRAY['replay']
        );

        RETURN NEXT v_job;
    END LOOP;
END;
$$ LANGUAGE plpgsql
--;;
DROP INDEX IF EXISTS ${migratus.schema}.idx_jobs_rate_limit_key
--;;
ALTER TABLE ${migratus.schema}.jobs DROP COLUMN IF EXISTS rate_limit_key
--;;
DROP TABLE IF EXISTS ${migratus.schema}.rate_limits
