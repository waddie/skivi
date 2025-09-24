# Skivi

Skivi is a PostgreSQL-backed job queue library for Clojure, in the spirit of
[Graphile Worker](https://github.com/graphile/worker). It runs inside your
existing database, creating its own schema there rather than requiring a
separate service. This means you can enqueue jobs within the same transaction
that triggers them, with no extra infrastructure to operate.

## Installation

### deps.edn

```clojure
{:deps {dev.skivi/skivi {:mvn/version "RELEASE"}}}
```

## How it works

A system is created from configuration, a task registry, and an optional list of
cron entries, then started:

```clojure
(def system
  (-> (skivi/create-system config tasks)
      (skivi/start!)))

;; Enqueue from anywhere in your application
(skivi/add-job system "send-email" {:to "user@example.com" :subject "Hello"})
```

`start!` runs pending database migrations automatically (this can be disabled
with `{:migrate? false}`), then starts the worker pool and any cron scheduler.
`stop!` drains in-flight jobs gracefully before closing connections.

The worker pool polls the database for available jobs, claiming them with
advisory locks to prevent double-processing. Each job is dispatched to its
registered handler function. A normal return marks the job complete; any
uncaught exception triggers a retry with exponential backoff. After exhausting
its allowed attempts, a job is marked exhausted and recorded in history.

## Task handlers

Task handlers are plain Clojure functions keyed by string identifier. There are
two ways to register them.

**Programmatic registry** – pass a map to `create-system`:

```clojure
(def tasks
  {"send-email"   (fn [{:keys [job]}] (send! (:payload job)))
   "resize-image" (fn [{:keys [job]}] (resize! (:payload job)))})

(skivi/create-system config tasks)
```

**File-based loading** – set `:task-directory` in the worker config and drop
`.clj` files there. Each file must return a registry map as its last expression:

```clojure
;; tasks/email.clj
{"send-email" (fn [{:keys [job]}]
                (send! (:payload job)))}
```

```edn
;; skivi-config.edn
{:worker {:task-directory "tasks"
          :file-extensions [".clj"]   ; optional, defaults to [".clj"]
          ...}}
```

Skivi loads all matching files at startup, sorted alphabetically, and merges
their registries. If you also pass a programmatic registry, it wins on any
identifier that appears in both. A job whose task identifier has no registered
handler is immediately exhausted rather than retried – a missing handler is a
configuration error, not a transient failure.

## Database schema

Skivi creates all its tables, functions, and triggers in a dedicated PostgreSQL
schema within your existing application database. The default schema name is
`skivi`, though it is configurable if you need multiple instances in the same
database or prefer a different name:

```edn
;; skivi-config.edn
{:database {:connection-string "postgresql://localhost:5432/my_app"
            :schema-name "jobs"}}
```

## Scheduling

Cron entries are defined as data and passed to `create-system`:

```clojure
(def crontabs
  [{:identifier "daily-report"
    :schedule   "0 2 * * *"
    :spec       {:queue-name "maintenance" :max-attempts 3}}
   {:identifier "hourly-cleanup"
    :schedule   "0 * * * *"}])

(skivi/create-system config tasks crontabs)
```

Each cron entry uses `unsafe_dedupe` by default: if the previous run’s job is
still queued or in flight, the new enqueue is silently skipped. Last-execution
state is persisted to the database, so the scheduler survives restarts without
double-firing.

## Monitoring

The event emitter lets you attach handlers to specific event types or to all
events:

```clojure
(monitoring/on (:emitter system) :all
  (fn [{:keys [type data]}]
    (log/info "event" (assoc data :event/type type))))

(monitoring/on (:emitter system) :job/exhausted
  (fn [{:keys [data]}]
    (alert! "Job exhausted after all attempts" data)))
```

Standard event types include `:job/completed`, `:job/failed`, `:job/exhausted`,
`:job/partial-success`, `:pool/start`, `:pool/stop`, and `:cron/fired`. The
emitter also maintains an in-memory ring buffer of recent events, useful in
tests via `collecting-emitter`.

## Payload validation

Task payloads are validated before they reach the database. You can declare
`malli` schemas or `clojure.spec` specs per task identifier in configuration,
and validation errors surface at enqueue time rather than during execution.

## License

Skivi is dual-licensed. Open-source use is covered by the [GNU Affero General
Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.en.html) or later.
Commercial licenses are available if the AGPL is not suitable for your use case.
