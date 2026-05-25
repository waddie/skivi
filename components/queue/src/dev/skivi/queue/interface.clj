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

(ns dev.skivi.queue.interface
  "Local in-process job queue with background polling from job-manager.

  The queue batches job claims from the database into a local buffer and
  distributes them to workers via take-job!. A background daemon thread
  polls job-manager continuously, refetching whenever the buffer depth
  drops below the configured threshold.

  Typical usage (worker-pool calls create-queue once per pool):

    (def q (-> (queue/create-queue sys worker-id {:size 50})
               queue/start!))

    ;; Worker thread loop
    (when-let [job (queue/take-job! q 2000)]
      (try
        (execute job)
        (job-manager/complete-jobs sys (queue/worker-id q) [job] elapsed)
        (catch Exception e
          (job-manager/fail-jobs sys (queue/worker-id q)
                                 [{:job job :error e}] elapsed))))

    ;; On shutdown
    (let [undispatched (queue/stop! q)]
      ;; undispatched jobs remain locked in DB; they will be recovered
      ;; by the ResetOverdueJobs rule after config.lock_timeout."
  (:require [dev.skivi.queue.core :as core]
            [dev.skivi.queue.schema :as schema]))

(defn create-queue
  "Creates a local queue. Call start! to begin polling.
  config keys: :size, :ttl-ms, :refetch-threshold, :poll-interval-ms,
  :refetch-delay-ms, :task-identifiers, :forbidden-flags."
  {:malli/schema
   [:function
    [:=> [:cat schema/JobManagerSystem :string] schema/LocalQueue]
    [:=> [:cat schema/JobManagerSystem :string schema/QueueTuningConfig]
     schema/LocalQueue]]}
  ([job-system worker-id] (core/create-queue job-system worker-id))
  ([job-system worker-id config]
   (core/create-queue job-system worker-id config)))

(defn start!
  "Starts the background polling loop. Throws if already running. Returns queue."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] schema/LocalQueue]]}
  [queue]
  (core/start! queue))

(defn stop!
  "Stops polling, drains the buffer, and returns un-dispatched jobs.
  join-timeout-ms controls how long to wait for the poll thread (default 5000)."
  {:malli/schema [:function
                  [:=> [:cat schema/LocalQueue] [:vector :map]]
                  [:=> [:cat schema/LocalQueue :int] [:vector :map]]]}
  ([queue] (core/stop! queue))
  ([queue join-timeout-ms] (core/stop! queue join-timeout-ms)))

(defn take-job!
  "Blocks until a job is available or timeout-ms elapses.
  Entries older than config :ttl-ms are discarded and not returned.
  Returns the job map or nil on timeout."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue :int] [:maybe :map]]]}
  [queue timeout-ms]
  (core/take-job! queue timeout-ms))

(defn offer-jobs!
  "Places jobs directly into the buffer without DB interaction.
  Jobs inserted this way bypass history tracking; the correlation-id-key
  required by complete-jobs and fail-jobs is not set, so job_history rows
  will not be updated from 'started' status when these jobs complete or fail.
  Use for testing only, or for re-dispatch where history tracking is not required."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue [:vector :map]] :nil]]}
  [queue jobs]
  (core/offer-jobs! queue jobs))

(defn depth
  "Returns the number of jobs currently buffered locally."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] :int]]}
  [queue]
  (core/depth queue))

(defn running?
  "Returns true if the background polling loop is active."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] :boolean]]}
  [queue]
  (core/running? queue))

(defn stats
  "Returns a snapshot of queue operational metrics.
  Keys: :fetched, :dispatched, :stale-dropped, :refetch-count, :errors."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] schema/QueueStats]]}
  [queue]
  (core/stats queue))

(defn worker-id
  "Returns the worker-id used for database job locking.
  Pass this to job-manager/complete-jobs and job-manager/fail-jobs."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] :string]]}
  [queue]
  (core/worker-id queue))
