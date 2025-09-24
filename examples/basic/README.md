# Basic example: a welcome email

Your registration endpoint needs to be fast. Sending an email synchronously in
the request handler is fragile – it blocks the response until the email service
replies, and if the email service is slow or down, registration breaks too.

Skivi lets you hand the work off to a background worker: the job is written to
the database as part of your request, the response returns immediately, and a
worker picks it up moments later.

This example adds skivi to a minimal application. When a user registers, a
`send-welcome-email` job is enqueued. The registration response returns as soon
as the job row is written.

## Prerequisites

- Clojure CLI tools
- PostgreSQL running locally, or Docker to start one

## Step 1: add the dependency

```clojure
;; deps.edn
{:deps  {dev.skivi/skivi {:mvn/version "RELEASE"}}
 :paths ["src" "resources"]}
```

The `resources` path is where skivi looks for `skivi-config.edn`.

## Step 2: configure the database

Create `resources/skivi-config.edn`. Skivi reads this at startup via
[aero](https://github.com/juxt/aero), so you can use aero tags. The `#or` tag
provides a fallback literal when the env var isn’t set, which is convenient for
local development:

```edn
{:database {:connection-string
            #or [#env "DATABASE_URL"
                 "jdbc:postgresql://localhost:5432/my_app?user=postgres"]
            :pool-config {:connection-timeout 30000
                          :idle-timeout       600000
                          :max-lifetime       1800000
                          :maximum-pool-size  10
                          :minimum-idle       2}}
 :queue    {:batch-complete-delay 0
            :batch-fail-delay     0
            :local-queue          {:enabled true
                                   :refetch-threshold 10
                                   :size    50
                                   :ttl     60000}}
 :schema   {:job-schemas {}
            :name        "skivi"
            :validate-payloads false}
 :worker   {:concurrency 5
            :graceful-shutdown-timeout 15000
            :max-job-execution-time 3600000
            :poll-interval 2000}}
```

The schema name is configured under `:schema :name`, not inside `:database`.
Change it if you need multiple skivi instances in the same database or prefer a
different name.

## Step 3: write a task handler

A task handler is a plain function. Skivi calls it with a map containing `:job`;
the job’s `:payload` is whatever you put there at enqueue time.

```clojure
;; src/welcome/tasks.clj
(ns welcome.tasks)

(defn send-welcome-email [{:keys [job]}]
  (let [{:keys [to name]} (:payload job)]
    (email/send! {:to to :subject (str "Welcome, " name "!")})))

(def registry
  {"send-welcome-email" send-welcome-email})
```

Return normally and the job is marked complete. Throw an exception and skivi
schedules a retry after an exponential backoff delay. After 25 attempts
(configurable), the job is marked exhausted.

## Step 4: create and start the system

```clojure
;; src/welcome/core.clj
(ns welcome.core
  (:require [dev.skivi.config.interface :as config]
            [dev.skivi.skivi.core :as skivi]
            [welcome.tasks :as tasks]))

(defonce system (atom nil))

(defn start! []
  (reset! system
    (-> (config/load-config)
        (skivi/create-system tasks/registry)
        skivi/start!)))

(defn stop! []
  (when-let [s @system]
    (skivi/stop! s)
    (reset! system nil)))
```

`config/load-config` reads `skivi-config.edn` from the classpath and validates
it. `start!` then runs any pending database migrations and starts the worker
pool. `stop!` drains in-flight jobs gracefully before closing connections – wire
it to your JVM shutdown hook.

## Step 5: enqueue a job

```clojure
(defn on-user-registered! [{:keys [email name]}]
  ;; Your own database insertion goes here first.
  (skivi/add-job @system "send-welcome-email" {:to email :name name}))
```

`add-job` writes the job to the database and returns. It does not wait for a
worker to execute it. The worker pool is polling in the background and will pick
it up within the next poll interval (2 seconds by default).

If you need the job to be enqueued within the same transaction as the user
insert – so that a failed transaction rolls both back – pass your existing
database connection as an option:

```clojure
(skivi/add-job @system "send-welcome-email" {:to email :name name}
               {:conn db-conn})
```

## Running the example

If you don’t have a local PostgreSQL instance, start one with Docker:

```sh
docker compose up -d
```

This starts a `postgres:17` container with a `my_app` database on
the default port, no password required. The config file falls back to
`jdbc:postgresql://localhost:5432/my_app?user=postgres` when `DATABASE_URL`
isn’t set, which matches the Docker setup. If your database is elsewhere,
set `DATABASE_URL` beforehand.

Start a REPL from this directory:

```sh
clojure -M
```

In the REPL, start the system first, then enqueue a job:

```clojure
(require '[welcome.core :as app])
(app/start!)
;; => starts migrations, then the worker pool

(app/on-user-registered! {:email "alice@example.com" :name "Alice"})
```

`start!` must be called before `on-user-registered!`. The system atom is `nil`
until then, and enqueueing against a nil system throws immediately with a clear
message.

You should see the handler execute shortly after. The `skivi.jobs` table in
PostgreSQL will show the job disappear once it’s complete; `skivi.job_history`
will have a record of it.

## What’s happening

Each worker in the pool polls PostgreSQL for available jobs, claiming them with
advisory locks. The lock prevents two workers from picking up the same job,
even when you’re running multiple instances of your application. When a handler
returns, the job row is deleted and a history record is written. If it throws,
the job is unlocked and rescheduled for retry.

Skivi creates all its tables, functions, and triggers in a dedicated schema
within your existing application database. They won’t interfere with your own
tables.

---

Continue to the [advanced example](../advanced/) to see named queues,
deduplication, rate limiting, cron scheduling, and monitoring.
