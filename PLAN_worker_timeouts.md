# Implementation Plan: `:graceful-shutdown-timeout` and `:max-job-execution-time`

## Current State

Both settings were removed from `WorkerConfig` in the code-review cleanup because they
were declared in the schema but never wired through to actual behaviour.

### What already exists

`worker-pool/core.clj` and `worker-pool/schema.clj` already model both concepts
internally:

| Internal pool key                   | Default | Used by                                                |
| ----------------------------------- | ------- | ------------------------------------------------------ |
| `:graceful-shutdown-timeout-ms`     | 30 000  | `stop!` — max wait for in-flight jobs before interrupt |
| _(no `:max-job-execution-time-ms`)_ | —       | not implemented                                        |

### The broken wiring

`skivi/core.clj:create-system` passes `worker-cfg` (= `(:worker config)`) directly
to `worker-pool/create-pool`. Config keys and pool keys do **not** match:

| Config key (`:worker` map)   | Pool key                        | Status                                     |
| ---------------------------- | ------------------------------- | ------------------------------------------ |
| `:concurrency`               | `:concurrency`                  | ✓ aligned                                  |
| `:poll-interval`             | `:poll-interval-ms`             | **silent mismatch** — user's value ignored |
| `:graceful-shutdown-timeout` | `:graceful-shutdown-timeout-ms` | missing from schema after review cleanup   |
| `:max-job-execution-time`    | `:max-job-execution-time-ms`    | not implemented anywhere                   |

`skivi/core.clj:stop!` also hardcodes the worker drain timeout:

```clojure
(worker-pool/stop! (:worker-pool system) (get opts :worker-timeout-ms 15000))
;; ↑ ignores the user's :graceful-shutdown-timeout entirely
```

---

## Changes Required

### Step 1 — Config schema (`components/config/src/dev/skivi/config/schema.clj`)

Re-add both fields to `WorkerConfig`:

```clojure
(def WorkerConfig
  [:map
   [:concurrency pos-int?]
   [:poll-interval pos-int?]
   [:graceful-shutdown-timeout pos-int?]   ; ms; how long stop! waits for in-flight jobs
   [:max-job-execution-time pos-int?]      ; ms; per-job hard timeout (0 = disabled)
   [:task-directory {:optional true} string?]
   [:file-extensions {:optional true} [:vector string?]]])
```

Use `pos-int?` for `:max-job-execution-time`; add a note in the schema doc that 0 is not
a valid value — callers who want no timeout should omit the field or rely on the default.

### Step 2 — Config defaults (`components/config/src/dev/skivi/config/core.clj`)

Add both fields to `default-config`:

```clojure
:worker {:concurrency              10
         :graceful-shutdown-timeout 30000   ; 30 s
         :max-job-execution-time   300000   ; 5 min; generous default, not 0
         :poll-interval            2000}
```

A 5-minute default for `:max-job-execution-time` is intentionally generous — a real
deployment will tune it down. Do not default to 0 (disabled) because that turns an
opt-in safety net into an opt-in footgun.

### Step 3 — Config → pool translation (`bases/skivi/src/dev/skivi/skivi/core.clj`)

Replace the bare `worker-cfg` pass-through in `create-system` with an explicit
key-translation step. This also fixes the pre-existing `:poll-interval` silent-mismatch
bug:

```clojure
;; in create-system, replace:
;;   wp (worker-pool/create-pool job-system merged-registry emitter worker-cfg)
;; with:

pool-cfg {:concurrency                  (:concurrency worker-cfg)
           :poll-interval-ms             (:poll-interval worker-cfg)
           :graceful-shutdown-timeout-ms (:graceful-shutdown-timeout worker-cfg)
           :max-job-execution-time-ms    (:max-job-execution-time worker-cfg)
           ;; local-queue config — currently absent; fix in the same pass:
           :queue-size  (get-in config [:queue :local-queue :size])
           :queue-ttl-ms (get-in config [:queue :local-queue :ttl])}
wp       (worker-pool/create-pool job-system merged-registry emitter pool-cfg)
```

> **Note:** `:task-identifiers` and `:forbidden-flags` are pool-internal and not
> surfaced through the config layer — they remain available via the `create-pool` arity
> for programmatic use.

### Step 4 — `stop!` wiring (`bases/skivi/src/dev/skivi/skivi/core.clj`)

Read the configured timeout rather than hardcoding 15 000:

```clojure
;; replace:
;;   (worker-pool/stop! (:worker-pool system) (get opts :worker-timeout-ms 15000))
;; with:

(let [cfg-timeout (get-in system [:config :worker :graceful-shutdown-timeout] 30000)]
  (worker-pool/stop! (:worker-pool system)
                     (get opts :worker-timeout-ms cfg-timeout)))
```

Callers who pass `:worker-timeout-ms` in opts still override the config value, which is
the right precedence order.

### Step 5 — Pool schema (`components/worker-pool/src/dev/skivi/worker_pool/schema.clj`)

Add `:max-job-execution-time-ms` to `WorkerPoolConfig`:

```clojure
[:max-job-execution-time-ms {:optional true} pos-int?]
```

Also add it to the schema docstring.

### Step 6 — Pool defaults (`components/worker-pool/src/dev/skivi/worker_pool/core.clj`)

Add to `defaults`:

```clojure
(def ^:private defaults
  {:concurrency                  10
   :graceful-shutdown-timeout-ms 30000
   :max-job-execution-time-ms    300000
   :poll-interval-ms             2000
   :queue-size                   50
   :queue-ttl-ms                 60000})
```

### Step 7 — Per-job timeout in `execute-job!` / `process-job!`

Wrap `execute-job!` in a `Future` with a bounded `deref` when a timeout is configured.

**Approach:** `clojure.core/future` submits onto the agent thread pool. After the
deadline, `future-cancel` is called with `may-interrupt-if-running? true`. This sends
`Thread.interrupt()` to the executing thread, which unblocks blocking I/O calls
(`Thread/sleep`, JDBC reads, HTTP etc.) but cannot stop a tight CPU loop. Document this
limitation.

```clojure
(defn- execute-with-timeout!
  "Runs execute-job! in a future. Returns the result or throws on timeout."
  [pool job timeout-ms]
  (let [fut (future (execute-job! pool job))
        result (deref fut timeout-ms ::timeout)]
    (when (= result ::timeout)
      (future-cancel fut)
      (throw (ex-info (str "Job timed out after " timeout-ms "ms")
                      {:type       ::job-timeout
                       :job-id     (:id job)
                       :timeout-ms timeout-ms})))
    result))
```

In `process-job!`, replace the `(execute-job! pool job)` call:

```clojure
(let [timeout-ms (get-in pool [:config :max-job-execution-time-ms])
      result     (if timeout-ms
                   (execute-with-timeout! pool job timeout-ms)
                   (execute-job! pool job))]
  ...)
```

The thrown `ex-info` propagates to the existing `catch Throwable` block in
`process-job!`, so the job is failed and retried (or exhausted) via the normal path.
No special-casing is needed in the error handler.

#### Monitoring event

Emit `:job/timeout` before re-throwing from `execute-with-timeout!`, so consumers can
distinguish timeouts from other failures without parsing the error message:

```clojure
(monitoring/emit! (:emitter pool)
                  :job/timeout
                  {:job-id     (:id job)
                   :timeout-ms timeout-ms
                   :worker-id  (queue/worker-id (:queue pool))})
```

Add `:job/timeout` to the `ns` docstring in `worker-pool/core.clj` and `interface.clj`.

---

## Test Plan

### `config` component

- `validate-config-valid-test` — inline map already has the old removed fields; passes
  because Malli schema is open. No change needed there, but add a dedicated
  `worker-timeout-defaults-test` that calls `(load-config ...)` with a minimal config
  and asserts `(get-in cfg [:worker :graceful-shutdown-timeout])` = 30 000 and
  `(get-in cfg [:worker :max-job-execution-time])` = 300 000.

### `worker-pool` component

Add to `worker-pool/interface-test`:

1. **`graceful-shutdown-timeout-config-test`** — create a pool with
   `:graceful-shutdown-timeout-ms 100`, start it, start a task that sleeps 50 ms, call
   `stop!` with no explicit timeout, assert the pool stops cleanly (not forced).

2. **`max-job-execution-time-timeout-test`** — create a pool with
   `:max-job-execution-time-ms 100`, enqueue a task that sleeps 5 000 ms, assert that
   within ~500 ms the job is marked failed and a `:job/timeout` event is emitted.

3. **`max-job-execution-time-no-timeout-test`** — same pool, fast task; assert job
   completes normally.

4. **`max-job-execution-time-nil-test`** — pool with `:max-job-execution-time-ms nil`
   (or absent from config); long task completes without being killed. Documents the
   "disabled" path.

   > Note: the `defaults` map sets `300000` so nil only arises if a caller explicitly
   > passes `{:max-job-execution-time-ms nil}` or passes a config that omits the key
   > before merging with defaults. The test should confirm the nil path is safe.

### `skivi` base

Add `system-timeout-wiring-test` (no DB required — mock job-system):

- Build a system config with `{:worker {:graceful-shutdown-timeout 100}}`.
- Assert `(get-in system [:worker-pool :config :graceful-shutdown-timeout-ms])` = 100.
- Assert `(get-in system [:worker-pool :config :poll-interval-ms])` = user's
  `:poll-interval` (regression test for the silent-mismatch fix).

---

## Limitations to Document

- **`Thread.interrupt()` cannot stop CPU-bound infinite loops.** If a task handler
  contains `(while true ...)` with no I/O, it will not be interrupted. Workers should
  implement cooperative cancellation or bound their own loops.
- **Timeout fires the normal retry cycle.** A timed-out job is failed and may be
  retried up to `:max-attempts` times. Set `:max-attempts 1` on tasks where retrying a
  partial execution would be harmful.
- **`execute-with-timeout!` uses the Clojure agent thread pool.** This means a large
  number of long-running timed-out tasks could saturate the pool before interrupts take
  effect. Consider a dedicated `ExecutorService` if this becomes a problem.

---

## File Change Summary

| File                                                             | Change                                                                                         |
| ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `components/config/src/dev/skivi/config/schema.clj`              | Re-add `:graceful-shutdown-timeout` and `:max-job-execution-time` to `WorkerConfig`            |
| `components/config/src/dev/skivi/config/core.clj`                | Add both to `default-config[:worker]`                                                          |
| `bases/skivi/src/dev/skivi/skivi/core.clj`                       | Translate config→pool keys; fix `stop!` timeout wiring                                         |
| `components/worker-pool/src/dev/skivi/worker_pool/schema.clj`    | Add `:max-job-execution-time-ms` to `WorkerPoolConfig`                                         |
| `components/worker-pool/src/dev/skivi/worker_pool/core.clj`      | Add `execute-with-timeout!`; call it from `process-job!`; update defaults; emit `:job/timeout` |
| `components/worker-pool/src/dev/skivi/worker_pool/interface.clj` | Update ns docstring with `:job/timeout` event                                                  |
| Test EDN configs (`minimal-config.edn` etc.)                     | Add `:graceful-shutdown-timeout` and `:max-job-execution-time` where missing                   |
| `components/config/test/.../interface_test.clj`                  | Add `worker-timeout-defaults-test`                                                             |
| `components/worker-pool/test/.../interface_test.clj`             | Add four timeout tests (see above)                                                             |
| `bases/skivi/test/.../core_test.clj`                             | Add `system-timeout-wiring-test`                                                               |

---

## Verification

```sh
docker compose up -d
clojure -M:poly test :all
```

Specific assertions:

- Minimal config passes `load-config` without error and contains both defaults.
- `worker-timeout-defaults-test` passes.
- All four worker-pool timeout tests pass.
- `system-timeout-wiring-test` passes.
- Existing 387 test assertions continue to pass.
