-- Initial skivi schema: tables, indexes, functions, triggers.
-- Statements are separated by --;; so migratus executes each one individually.

CREATE SCHEMA IF NOT EXISTS ${migratus.schema}
--;;
CREATE TABLE IF NOT EXISTS ${migratus.schema}.jobs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    task_identifier TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    priority        INTEGER     NOT NULL DEFAULT 0,
    queue_name      TEXT,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INTEGER     NOT NULL DEFAULT 0,
    max_attempts    INTEGER     NOT NULL DEFAULT 25,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    job_key         TEXT,
    job_key_mode    TEXT        DEFAULT 'replace'
                                CHECK (job_key_mode IN ('replace', 'preserve_run_at', 'unsafe_dedupe')),
    flags           TEXT[]      NOT NULL DEFAULT '{}',
    locked_at       TIMESTAMPTZ,
    locked_by       TEXT,
    revision        INTEGER     NOT NULL DEFAULT 0
)
--;;
-- RLS is enabled so that future multi-tenant policies can be added without a schema change.
-- The application user is the table owner and therefore bypasses RLS by default —
-- no rows are filtered until explicit policies are defined.
ALTER TABLE ${migratus.schema}.jobs ENABLE ROW LEVEL SECURITY
--;;
CREATE TABLE IF NOT EXISTS ${migratus.schema}.job_queues (
    queue_name TEXT        PRIMARY KEY,
    job_count  INTEGER     NOT NULL DEFAULT 0,
    locked_at  TIMESTAMPTZ,
    locked_by  TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
--;;
ALTER TABLE ${migratus.schema}.job_queues ENABLE ROW LEVEL SECURITY
--;;
CREATE TABLE IF NOT EXISTS ${migratus.schema}.task_identifiers (
    identifier TEXT        PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used  TIMESTAMPTZ NOT NULL DEFAULT now()
)
--;;
CREATE TABLE IF NOT EXISTS ${migratus.schema}.known_crontabs (
    identifier      TEXT        PRIMARY KEY,
    known_since     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_execution  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
--;;
CREATE TABLE IF NOT EXISTS ${migratus.schema}.job_history (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                UUID        NOT NULL,
    correlation_id        UUID        NOT NULL DEFAULT gen_random_uuid(),
    task_identifier       TEXT        NOT NULL,
    payload               JSONB       NOT NULL,
    worker_id             TEXT,
    status                TEXT        NOT NULL
                                      CHECK (status IN ('started', 'completed', 'failed', 'partial_success')),
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    queue_time_ms         INTEGER,
    execution_time_ms     INTEGER,
    attempt_number        INTEGER     NOT NULL,
    error_message         TEXT,
    error_stack           TEXT,
    partial_results       JSONB,
    completed_steps       TEXT[],
    failed_steps          TEXT[],
    retry_from_step       TEXT,
    execution_environment JSONB,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at            TIMESTAMPTZ DEFAULT (now() + interval '30 days')
)
--;;
ALTER TABLE ${migratus.schema}.job_history ENABLE ROW LEVEL SECURITY
--;;
-- Indexes: CONCURRENTLY omitted - index creation inside a migration
-- transaction forbids it.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_job_key
    ON ${migratus.schema}.jobs (job_key)
    WHERE job_key IS NOT NULL
--;;
CREATE INDEX IF NOT EXISTS idx_jobs_available_priority
    ON ${migratus.schema}.jobs (priority, run_at, id)
    WHERE locked_at IS NULL AND attempts < max_attempts
--;;
CREATE INDEX IF NOT EXISTS idx_jobs_queue_priority
    ON ${migratus.schema}.jobs (queue_name, priority, run_at, id)
    WHERE queue_name IS NOT NULL AND locked_at IS NULL AND attempts < max_attempts
--;;
CREATE INDEX IF NOT EXISTS idx_jobs_locked_cleanup
    ON ${migratus.schema}.jobs (locked_at)
    WHERE locked_at IS NOT NULL
--;;
CREATE INDEX IF NOT EXISTS idx_jobs_failed
    ON ${migratus.schema}.jobs (updated_at)
    WHERE attempts >= max_attempts
--;;
CREATE INDEX IF NOT EXISTS idx_jobs_task_identifier
    ON ${migratus.schema}.jobs (task_identifier)
--;;
CREATE INDEX IF NOT EXISTS idx_jobs_flags
    ON ${migratus.schema}.jobs USING GIN (flags)
--;;
CREATE INDEX IF NOT EXISTS idx_known_crontabs_last_execution
    ON ${migratus.schema}.known_crontabs (last_execution)
--;;
CREATE INDEX IF NOT EXISTS idx_job_history_job_id
    ON ${migratus.schema}.job_history (job_id)
--;;
CREATE INDEX IF NOT EXISTS idx_job_history_correlation_id
    ON ${migratus.schema}.job_history (correlation_id)
--;;
CREATE INDEX IF NOT EXISTS idx_job_history_task_identifier
    ON ${migratus.schema}.job_history (task_identifier)
--;;
CREATE INDEX IF NOT EXISTS idx_job_history_created_at
    ON ${migratus.schema}.job_history (created_at)
--;;
CREATE INDEX IF NOT EXISTS idx_job_history_expires_at
    ON ${migratus.schema}.job_history (expires_at)
    WHERE expires_at IS NOT NULL
--;;
CREATE INDEX IF NOT EXISTS idx_job_history_status_created
    ON ${migratus.schema}.job_history (status, created_at)
--;;
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
CREATE OR REPLACE FUNCTION ${migratus.schema}.complete_jobs(
    worker_id TEXT,
    job_ids   UUID[]
) RETURNS SETOF ${migratus.schema}.jobs AS $$
DECLARE
    v_deleted     ${migratus.schema}.jobs;
    v_queue_names TEXT[] := '{}';
BEGIN
    FOR v_deleted IN
        DELETE FROM ${migratus.schema}.jobs
        WHERE id = ANY(job_ids) AND locked_by = worker_id
        RETURNING *
    LOOP
        IF v_deleted.queue_name IS NOT NULL THEN
            v_queue_names := array_append(v_queue_names, v_deleted.queue_name);
        END IF;
        INSERT INTO ${migratus.schema}.task_identifiers (identifier, last_used)
        VALUES (v_deleted.task_identifier, now())
        ON CONFLICT (identifier) DO UPDATE SET last_used = now();
        RETURN NEXT v_deleted;
    END LOOP;

    UPDATE ${migratus.schema}.job_queues
    SET locked_by = NULL, locked_at = NULL
    WHERE queue_name = ANY(v_queue_names) AND locked_by = worker_id;
END;
$$ LANGUAGE plpgsql
--;;
-- Positional parameter order must match core.clj add_job call:
--   add_job(task_identifier, payload, queue_name, run_at, priority,
--           max_attempts, job_key, job_key_mode, flags)
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
                SELECT * INTO v_job FROM ${migratus.schema}.jobs WHERE job_key = add_job.job_key;
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
        WHERE locked_at < (now() - job_expiry)
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
--;;
CREATE OR REPLACE FUNCTION ${migratus.schema}.gc_task_identifiers(
    keep_since INTERVAL DEFAULT '7 days'
) RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    DELETE FROM ${migratus.schema}.task_identifiers
    WHERE last_used < (now() - keep_since)
      AND identifier NOT IN (SELECT DISTINCT task_identifier FROM ${migratus.schema}.jobs);
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql
--;;
CREATE OR REPLACE FUNCTION ${migratus.schema}.gc_job_queues() RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    DELETE FROM ${migratus.schema}.job_queues
    WHERE job_count <= 0
      AND locked_at IS NULL
      AND queue_name NOT IN (
          SELECT DISTINCT queue_name FROM ${migratus.schema}.jobs WHERE queue_name IS NOT NULL
      );
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql
--;;
CREATE OR REPLACE FUNCTION ${migratus.schema}.gc_job_history() RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    DELETE FROM ${migratus.schema}.job_history
    WHERE expires_at IS NOT NULL AND expires_at < now();
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql
--;;
CREATE OR REPLACE FUNCTION ${migratus.schema}.replay_failed_jobs(
    from_time              TIMESTAMPTZ,
    to_time                TIMESTAMPTZ,
    task_identifier_filter TEXT        DEFAULT NULL,
    payload_transformer    TEXT        DEFAULT NULL
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
--;;
CREATE OR REPLACE FUNCTION ${migratus.schema}.update_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = GREATEST(now(), OLD.updated_at + interval '1 millisecond');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
--;;
CREATE OR REPLACE FUNCTION ${migratus.schema}.maintain_job_queue_count() RETURNS TRIGGER AS $$
DECLARE
    v_delta INTEGER := 0;
    v_queue TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_delta := 1;
        v_queue := NEW.queue_name;
    ELSIF TG_OP = 'DELETE' THEN
        v_delta := -1;
        v_queue := OLD.queue_name;
    ELSIF TG_OP = 'UPDATE' AND (OLD.queue_name IS DISTINCT FROM NEW.queue_name) THEN
        IF OLD.queue_name IS NOT NULL THEN
            INSERT INTO ${migratus.schema}.job_queues (queue_name, job_count)
            VALUES (OLD.queue_name, 0)
            ON CONFLICT (queue_name)
            DO UPDATE SET job_count = GREATEST(0, ${migratus.schema}.job_queues.job_count - 1);
        END IF;
        IF NEW.queue_name IS NOT NULL THEN
            INSERT INTO ${migratus.schema}.job_queues (queue_name, job_count)
            VALUES (NEW.queue_name, 1)
            ON CONFLICT (queue_name)
            DO UPDATE SET job_count = ${migratus.schema}.job_queues.job_count + 1;
        END IF;
        RETURN COALESCE(NEW, OLD);
    END IF;

    IF v_queue IS NOT NULL AND v_delta != 0 THEN
        INSERT INTO ${migratus.schema}.job_queues (queue_name, job_count)
        VALUES (v_queue, GREATEST(0, v_delta))
        ON CONFLICT (queue_name)
        DO UPDATE SET job_count = GREATEST(0, ${migratus.schema}.job_queues.job_count + v_delta);
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql
--;;
CREATE OR REPLACE FUNCTION ${migratus.schema}.notify_new_jobs() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('jobs:insert', '{"count":1}');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql
--;;
CREATE TRIGGER jobs_update_updated_at
    BEFORE UPDATE ON ${migratus.schema}.jobs
    FOR EACH ROW
    EXECUTE FUNCTION ${migratus.schema}.update_updated_at()
--;;
CREATE TRIGGER job_queues_update_updated_at
    BEFORE UPDATE ON ${migratus.schema}.job_queues
    FOR EACH ROW
    EXECUTE FUNCTION ${migratus.schema}.update_updated_at()
--;;
CREATE TRIGGER known_crontabs_update_updated_at
    BEFORE UPDATE ON ${migratus.schema}.known_crontabs
    FOR EACH ROW
    EXECUTE FUNCTION ${migratus.schema}.update_updated_at()
--;;
CREATE TRIGGER jobs_maintain_queue_count
    AFTER INSERT OR UPDATE OR DELETE ON ${migratus.schema}.jobs
    FOR EACH ROW
    EXECUTE FUNCTION ${migratus.schema}.maintain_job_queue_count()
--;;
CREATE TRIGGER jobs_notify_workers
    AFTER INSERT ON ${migratus.schema}.jobs
    FOR EACH STATEMENT
    EXECUTE FUNCTION ${migratus.schema}.notify_new_jobs()
