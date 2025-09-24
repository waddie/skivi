-- Recreate add_job with #variable_conflict use_column to resolve ambiguity
-- between the job_key parameter and the job_key column in ${migratus.schema}.jobs.
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
