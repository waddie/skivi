# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Project Overview

Skivi is a PostgreSQL-backed job queue library for Clojure (inspired by Graphile
Worker). It uses a **Polylith monorepo** structure with 11 components, assembled
into a single library JAR. The library is covered by an allium spec.

## Commands

### Development

Start a REPL with all components on the classpath:

```sh
clojure -M:dev
```

Start a CIDER-compatible nREPL server (from within the REPL):

```clojure
(defonce server (nrepl/start-server cider-nrepl-handler))
```

### Testing

Tests require PostgreSQL running. Start it with:

```
docker compose up -d
```

Run tests affected by changes for a single component (e.g. `database`):

```sh
clojure -M:poly test brick:database
```

Run all tests for a single component:

```sh
clojure -M:poly test brick:database :all
```

Run the full test suite:

```sh
clojure -M:poly test :all
```

The test database URL defaults to
`jdbc:postgresql://localhost:5432/test_db?user=postgres` and can be overridden
with `SKIVI_TEST_DB_URL`.

### Polylith

Check workspace integrity and component boundaries:

```
clojure -M:poly check
```

### Build

Build the library JAR (from `projects/library/`):

```
clojure -T:build jar-all
```

Deploy to Clojars (requires credentials):

```
clojure -T:build deploy
```

Never deploy, prompt user to do it instead.

## Architecture

### Polylith Structure

The codebase follows the [Polylith](https://polylith.gitbook.io/polylith/) architecture:

- **`components/`** — 11 independent components, each with `src/`, `resources/`,
  and `test/` directories
- **`bases/skivi/`** — The integration layer that wires all components into a
  single system map
- **`projects/library/`** — The build project that produces the published JAR
- **`development/`** — REPL-only dev namespace (`user.clj`) with nREPL and malli
  instrumentation

Each component exposes a public API via `interface.clj` and keeps implementation
in `core.clj`. Cross-component calls must go through `interface.clj`.

### Component Responsibilities

| Component     | Role                                                                     |
| ------------- | ------------------------------------------------------------------------ |
| `config`      | Loads and validates config via aero; provides typed sub-config accessors |
| `database`    | HikariCP connection pooling + `next.jdbc` / `honey.sql` query execution  |
| `migration`   | Schema versioning via migratus; runs automatically on `start!`           |
| `job-manager` | Core job CRUD: enqueue, claim, complete, fail, replay, rate-limits       |
| `worker-pool` | Thread pool that polls the local queue and dispatches to task handlers   |
| `queue`       | Local in-memory buffer between the DB poll and worker threads            |
| `scheduler`   | Cron-based job firing using `known_crontabs` table                       |
| `job-history` | In-memory ring buffer + DB query API for execution history               |
| `monitoring`  | Event emitter (broadcast to listeners); stats snapshots                  |
| `validation`  | Malli/spec payload validation; per-task schema registry                  |
| `maintenance` | Background GC and lock recovery (two loops: 60s + nightly cron)          |

### System Lifecycle (`bases/skivi/src/dev/skivi/skivi/core.clj`)

The public API is:

```clojure
(create-system config task-registry crontabs)  ; wires components, no I/O
(start! system)                                 ; runs migrations, starts pools
(stop! system)                                  ; graceful drain + shutdown
(add-job system task-id payload opts)           ; enqueue a single job
```

`create-system` builds a plain Clojure map—no atoms, no component framework.
Each sub-system (`worker-pool`, `scheduler`, `maintenance`) holds its own state
internally.

### Job Execution Flow

1. `worker-pool` polls `job-manager/get-jobs` on a configurable interval → fills
   the local `queue` buffer
2. Worker threads block on `queue/take-job!`; the pool claims jobs atomically
   using a single shared `worker-id`
3. Handler receives `{:job … :job-system … :worker-id …}`; return value →
   `complete`, throw → `fail` with exponential backoff
4. Exhausted jobs (max-attempts reached) are recorded in `job_history` with
   status `:exhausted`

### Key Database Patterns

All objects live in a single configurable PostgreSQL schema (default
`job_system`). Advisory locks on jobs and queues prevent double-processing
across distributed workers. Job deduplication is controlled per-enqueue via
`:job-key-mode` (`:replace`, `:preserve_run_at`, `:unsafe_dedupe`).

### Testing Patterns

Component tests hit a real PostgreSQL database—no mocking. Each component
with DB tests has a `test_helpers.clj` containing a `schema-fixture` that runs
migrations before the suite. Use `use-fixtures :once helpers/schema-fixture` in
test namespaces.
