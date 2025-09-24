;; Copyright (c) 2025-2026 Tom Waddington
;;
;; This software is dual-licensed:
;;
;; 1. GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later)
;;    This program is free software: you can redistribute it and/or modify
;;    it under the terms of the GNU Affero General Public License as published
;;    by
;;    the Free Software Foundation, either version 3 of the License, or
;;    (at your option) any later version.
;;
;;    This program is distributed in the hope that it will be useful,
;;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;    GNU Affero General Public License for more details.
;;
;;    You should have received a copy of the GNU Affero General Public License
;;    along with this program.  If not, see <https://www.gnu.org/licenses/>.
;;
;; 2. Commercial License
;;    If you have purchased a commercial license, you may use this software
;;    under the terms of that license instead of the AGPL-3.0-or-later.

(ns dev.skivi.worker-pool.interface
  "Concurrent worker pool that claims jobs from the queue and executes task handlers.

  All workers share a single local queue backed by job-manager. The pool uses one
  worker-id for all job claims; see worker-id to retrieve it.

  Task functions are plain Clojure functions keyed by task-identifier string:

    (def tasks
      {\"send-email\"  (fn [{:keys [job]}] (send! (:payload job)))
       \"resize-image\" (fn [{:keys [job job-system worker-id]}] ...)})

  A task fn receives a context map with:
    :job        - the full job map returned by job-manager/get-jobs
    :job-system - the {:pool :validator} system map
    :worker-id  - the pool's worker-id string

  Task outcome is determined by the return value or exception:
    • return any value (including nil) - job completes successfully
    • return (partial-success m)       - job reported as partial success
    • throw any Throwable               - job is failed (retry or exhaust)

  Typical usage:

    (def pool
      (-> (worker-pool/create-pool sys tasks emitter {:concurrency 4})
          worker-pool/start!))

    ;; On application shutdown
    (worker-pool/stop! pool 15000)

  Standard events emitted by the pool (via the supplied emitter):
    :pool/start          - pool started; :concurrency in data
    :pool/stop           - pool stopped; :forced? in data
    :job/completed       - task fn returned normally
    :job/failed          - task fn threw; retry scheduled
    :job/exhausted       - task fn threw on the final allowed attempt
    :job/partial-success - task fn returned a partial-success value
    :worker/error        - infrastructure exception outside task execution"
  (:require [dev.skivi.worker-pool.core :as core]
            [dev.skivi.worker-pool.schema :as schema]))

(defn partial-success
  "Wraps partial-results so the pool treats the task return as a partial success.
  partial-results keys: :completed-steps, :failed-steps, :retry-from-step, :results."
  {:malli/schema [:function [:=> [:cat :map] :map]]}
  [partial-results]
  (core/partial-success partial-results))

(defn partial-success?
  "Returns true if v was produced by partial-success."
  {:malli/schema [:function [:=> [:cat :any] :boolean]]}
  [v]
  (core/partial-success? v))

(defn create-pool
  "Creates a worker pool. Call start! to begin processing.
  job-system is {:pool datasource :validator validator}.
  task-registry is a map of task-identifier -> handler fn.
  emitter is a monitoring/Emitter.
  config keys: :concurrency, :poll-interval-ms, :queue-size, :queue-ttl-ms,
               :graceful-shutdown-timeout-ms, :task-identifiers, :forbidden-flags."
  {:malli/schema [:function
                  [:=> [:cat :any schema/TaskRegistry :any] schema/WorkerPool]
                  [:=>
                   [:cat :any schema/TaskRegistry :any schema/WorkerPoolConfig]
                   schema/WorkerPool]]}
  ([job-system task-registry emitter]
   (core/create-pool job-system task-registry emitter))
  ([job-system task-registry emitter config]
   (core/create-pool job-system task-registry emitter config)))

(defn start!
  "Starts the pool and all worker threads. Throws if already running. Returns pool."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] schema/WorkerPool]]}
  [pool]
  (core/start! pool))

(defn stop!
  "Gracefully stops the pool, waiting up to timeout-ms for in-flight jobs to finish.
  Jobs buffered but not yet dispatched remain locked in the DB; they are recovered
  by ResetOverdueJobs after config.lock_timeout. Returns pool."
  {:malli/schema [:function
                  [:=> [:cat schema/WorkerPool] schema/WorkerPool]
                  [:=> [:cat schema/WorkerPool pos-int?] schema/WorkerPool]]}
  ([pool] (core/stop! pool))
  ([pool timeout-ms] (core/stop! pool timeout-ms)))

(defn force-stop!
  "Immediately interrupts all worker threads and stops the pool. Returns pool."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] schema/WorkerPool]]}
  [pool]
  (core/force-stop! pool))

(defn running?
  "Returns true if the pool has been started and not yet stopped."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] :boolean]]}
  [pool]
  (core/running? pool))

(defn active-workers
  "Returns the number of worker threads currently executing a task."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] nat-int?]]}
  [pool]
  (core/active-workers pool))

(defn stats
  "Returns a snapshot of pool operational metrics.
  Keys: :active, :completed, :failed, :errors."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] schema/PoolStats]]}
  [pool]
  (core/stats pool))

(defn worker-id
  "Returns the worker-id used for all job claims from this pool."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] :string]]}
  [pool]
  (core/worker-id pool))
