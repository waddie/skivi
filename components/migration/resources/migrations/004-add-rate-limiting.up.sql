-- Rate limiting support: rate_limits table, rate_limit_key on jobs,
-- register_rate_limit / refill_rate_limits functions, and updates to
-- get_jobs and add_job to enforce and consume rate limit tokens.
-- Statements separated by --;; for migratus.

CREATE TABLE IF NOT EXISTS ${migratus.schema}.rate_limits (
    key              TEXT        PRIMARY KEY,
    capacity         INTEGER     NOT NULL CHECK (capacity > 0),
    interval         INTERVAL    NOT NULL,
    available_tokens INTEGER     NOT NULL CHECK (available_tokens >= 0),
    last_refill_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT rate_limits_tokens_bound CHECK (available_tokens <= capacity)
)
--;;
ALTER TABLE ${migratus.schema}.rate_limits ENABLE ROW LEVEL SECURITY
--;;
ALTER TABLE ${migratus.schema}.jobs
    ADD COLUMN IF NOT EXISTS rate_limit_key TEXT
        REFERENCES ${migratus.schema}.rate_limits(key) ON DELETE SET NULL
--;;
CREATE INDEX IF NOT EXISTS idx_jobs_rate_limit_key
    ON ${migratus.schema}.jobs (rate_limit_key)
    WHERE rate_limit_key IS NOT NULL
--;;
-- register_rate_limit: upsert a rate limit entry.
-- On conflict, updates capacity and interval only; available_tokens is
-- clamped to the new capacity to satisfy the tokens_bound constraint.
-- last_refill_at is preserved so the current window is not disrupted.
CREATE OR REPLACE FUNCTION ${migratus.schema}.register_rate_limit(
    p_key      TEXT,
    p_capacity INTEGER,
    p_interval INTERVAL
) RETURNS ${migratus.schema}.rate_limits AS $$
DECLARE
    v_rl ${migratus.schema}.rate_limits;
BEGIN
    INSERT INTO ${migratus.schema}.rate_limits
        (key, capacity, interval, available_tokens, last_refill_at)
    VALUES (p_key, p_capacity, p_interval, p_capacity, now())
    ON CONFLICT (key) DO UPDATE
        SET capacity         = EXCLUDED.capacity,
            interval         = EXCLUDED.interval,
            available_tokens = LEAST(
                ${migratus.schema}.rate_limits.available_tokens,
                EXCLUDED.capacity
            )
    RETURNING * INTO v_rl;
    RETURN v_rl;
END;
$$ LANGUAGE plpgsql
--;;
-- refill_rate_limits: reset tokens to capacity for all rate limits
-- whose window has expired (last_refill_at + interval <= now()).
-- Returns count of limits refilled.
CREATE OR REPLACE FUNCTION ${migratus.schema}.refill_rate_limits() RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE ${migratus.schema}.rate_limits
    SET available_tokens = capacity,
        last_refill_at   = now()
    WHERE last_refill_at + interval <= now();
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql
--;;
-- Drop the 9-parameter add_job so the 10-parameter replacement below
-- does not create a second overload alongside it.
DROP FUNCTION IF EXISTS ${migratus.schema}.add_job(TEXT, JSONB, TEXT, TIMESTAMPTZ, INTEGER, INTEGER, TEXT, TEXT, TEXT[])
--;;
-- add_job: adds rate_limit_key as 10th parameter (DEFAULT NULL).
CREATE OR REPLACE FUNCTION ${migratus.schema}.add_job(
    task_identifier TEXT,
    payload         JSONB       DEFAULT '{}',
    queue_name      TEXT        DEFAULT NULL,
    run_at          TIMESTAMPTZ DEFAULT now(),
    priority        INTEGER     DEFAULT 0,
    max_attempts    INTEGER     DEFAULT 25,
    job_key         TEXT        DEFAULT NULL,
    job_key_mode    TEXT        DEFAULT 'replace',
    flags           TEXT[]      DEFAULT '{}',
    rate_limit_key  TEXT        DEFAULT NULL
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
                    rate_limit_key  = add_job.rate_limit_key,
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
        max_attempts, job_key, job_key_mode, flags, rate_limit_key
    ) VALUES (
        add_job.task_identifier, add_job.payload, add_job.queue_name,
        add_job.run_at, add_job.priority, add_job.max_attempts,
        add_job.job_key, add_job.job_key_mode, add_job.flags,
        add_job.rate_limit_key
    ) RETURNING * INTO v_job;

    INSERT INTO ${migratus.schema}.task_identifiers (identifier, last_used)
    VALUES (add_job.task_identifier, now())
    ON CONFLICT (identifier) DO UPDATE SET last_used = now();

    RETURN v_job;
END;
$$ LANGUAGE plpgsql
--;;
-- get_jobs: updated to respect and consume rate limit tokens.
-- Jobs blocked by an exhausted rate limit are skipped in selection.
-- After claiming, available_tokens is decremented once per claimed job.
-- GREATEST(0, ...) prevents underflow; steady-state enforcement relies
-- on the pre-claim filter; race-condition overruns are bounded and
-- corrected at the next refill window.
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
        SELECT j.id, j.queue_name, j.rate_limit_key
        FROM ${migratus.schema}.jobs j
        LEFT JOIN ${migratus.schema}.job_queues jq
               ON j.queue_name = jq.queue_name
        LEFT JOIN ${migratus.schema}.rate_limits rl
               ON j.rate_limit_key = rl.key
        WHERE j.locked_at IS NULL
          AND j.run_at <= now()
          AND j.attempts < j.max_attempts
          AND (task_identifiers IS NULL
               OR j.task_identifier = ANY(task_identifiers))
          AND (forbidden_flags IS NULL
               OR NOT (j.flags && forbidden_flags))
          AND (j.queue_name IS NULL OR jq.locked_at IS NULL)
          AND (j.rate_limit_key IS NULL
               OR rl.key IS NULL
               OR rl.available_tokens > 0)
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

    -- Decrement one token per claimed job, grouped by rate_limit_key.
    UPDATE ${migratus.schema}.rate_limits rl
    SET available_tokens = GREATEST(0, rl.available_tokens - counts.cnt)
    FROM (
        SELECT rate_limit_key, count(*) AS cnt
        FROM ${migratus.schema}.jobs
        WHERE id = ANY(v_job_ids)
          AND rate_limit_key IS NOT NULL
        GROUP BY rate_limit_key
    ) counts
    WHERE rl.key = counts.rate_limit_key;

    RETURN QUERY
    SELECT * FROM ${migratus.schema}.jobs
    WHERE id = ANY(v_job_ids)
    ORDER BY priority ASC, run_at ASC, id ASC;
END;
$$ LANGUAGE plpgsql
--;;
-- replay_failed_jobs: updated to call the 10-parameter add_job.
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
            ARRAY['replay'],
            NULL
        );

        RETURN NEXT v_job;
    END LOOP;
END;
$$ LANGUAGE plpgsql
