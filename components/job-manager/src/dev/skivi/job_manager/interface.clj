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

(ns dev.skivi.job-manager.interface
  "Job lifecycle management: the JobEnqueue and WorkerExecution surfaces.

  The system map passed to all functions must contain:
    :pool      - HikariCP DataSource (from the database component)
    :validator - PayloadValidator (from the validation component)

  Typical usage:

    ;; Build the system
    (def sys {:pool pool :validator validator})

    ;; Enqueue
    (add-job sys \"send-email\" {:to \"x@y.com\"} {:queue-name \"email\"})

    ;; Worker loop
    (let [jobs (get-jobs sys worker-id {:batch-size 5})]
      (doseq [job jobs]
        (try
          (run-task! job)
          (complete-jobs sys worker-id [job] elapsed-ms)
          (catch Exception e
            (fail-jobs sys worker-id [{:job job :error e}] elapsed-ms)))))"
  (:require [dev.skivi.job-manager.core :as core]
            [dev.skivi.job-manager.schema :as schema]))

(def correlation-id-key
  "Key embedded in job maps returned by get-jobs. Contains the UUID correlating the
  job execution to its history record. Pass the entire job map to complete-jobs,
  fail-jobs, or report-partial-success - do not inspect this key directly."
  schema/correlation-id-key)

;;; JobEnqueue surface

(defn add-job
  "Validates payload then adds single job to queue. Returns the created job."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem :string :map] schema/Job]
                  [:=> [:cat schema/JobManagerSystem :string :map :map]
                   schema/Job]]}
  ([system task-identifier payload]
   (core/add-job system task-identifier payload))
  ([system task-identifier payload opts]
   (core/add-job system task-identifier payload opts)))

(defn add-jobs
  "Validates all payloads then adds multiple jobs atomically. Returns created jobs."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem [:vector schema/JobSpec]]
                   [:vector schema/Job]]]}
  [system job-specs]
  (core/add-jobs system job-specs))

(defn reschedule-jobs
  "Updates run-at, priority, or max-attempts for the given job-ids."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem [:vector :uuid] :map]
                   [:vector schema/Job]]]}
  [system job-ids opts]
  (core/reschedule-jobs system job-ids opts))

(defn replay-failed-jobs
  "Creates new jobs from failed history records matching criteria map.
  Supports :from, :to (instants), :task-identifier, and :max-attempts keys."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem :map]
                   [:vector schema/Job]]]}
  [system criteria]
  (core/replay-failed-jobs system criteria))

(defn permanently-fail-jobs
  "Sets jobs to exhausted status and records reason as last-error.
  Unlocks any named queues held by the affected jobs."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem [:vector :uuid] :string]
                   [:vector schema/Job]]]}
  [system job-ids reason]
  (core/permanently-fail-jobs system job-ids reason))

(defn force-unlock-jobs
  "Clears locks on locked jobs. With no worker-ids, unlocks all locked jobs.
  With worker-ids, unlocks only jobs held by those workers."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem] [:vector schema/Job]]
                  [:=> [:cat schema/JobManagerSystem [:maybe [:vector :string]]]
                   [:vector schema/Job]]]}
  ([system] (core/force-unlock-jobs system))
  ([system worker-ids] (core/force-unlock-jobs system worker-ids)))

(defn force-unlock-queues
  "Clears locks on job queues. With no queue-names, unlocks all locked queues.
  With queue-names, unlocks only those queues."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem] [:vector :map]]
                  [:=> [:cat schema/JobManagerSystem [:maybe [:vector :string]]]
                   [:vector :map]]]}
  ([system] (core/force-unlock-queues system))
  ([system queue-names] (core/force-unlock-queues system queue-names)))

(defn gc-job-queues
  "Removes empty, unlocked job queues. Returns count of deleted queues."
  {:malli/schema [:function [:=> [:cat schema/JobManagerSystem] :int]]}
  [system]
  (core/gc-job-queues system))

;;; WorkerExecution surface

(defn get-jobs
  "Retrieves and locks available jobs for worker-id. Each returned job map
  contains a ::correlation-id key required by complete-jobs, fail-jobs, and
  report-partial-success to update history records.

  opts keys:
    :task-identifiers  - restrict to these task types
    :forbidden-flags   - skip jobs carrying any of these flags
    :batch-size        - max jobs to claim (default 1)
    :history-retention - SQL interval string for history expiry (default '30 days')"
  {:malli/schema
   [:function
    [:=> [:cat schema/JobManagerSystem :string] [:vector schema/Job]]
    [:=> [:cat schema/JobManagerSystem :string :map] [:vector schema/Job]]]}
  ([system worker-id] (core/get-jobs system worker-id))
  ([system worker-id opts] (core/get-jobs system worker-id opts)))

(defn complete-jobs
  "Marks jobs as completed, removes them from the queue, and updates history.
  jobs must be the enriched maps returned by get-jobs.
  execution-time-ms is the wall-clock duration of job execution in milliseconds."
  {:malli/schema
   [:function
    [:=> [:cat schema/JobManagerSystem :string [:vector schema/Job] :int]
     :nil]]}
  [system worker-id jobs execution-time-ms]
  (core/complete-jobs system worker-id jobs execution-time-ms))

(defn fail-jobs
  "Records job failures, schedules retries with exponential backoff, and updates
  history. job-errors is a sequence of {:job job :error throwable-or-string} maps.
  jobs must be the enriched maps returned by get-jobs."
  {:malli/schema [:function
                  [:=>
                   [:cat schema/JobManagerSystem :string
                    [:vector schema/JobError] :int]
                   :nil]]}
  [system worker-id job-errors execution-time-ms]
  (core/fail-jobs system worker-id job-errors execution-time-ms))

(defn report-partial-success
  "Records partial job success: reschedules the job with exponential backoff and
  records partial_success status in history. job must be the enriched map returned
  by get-jobs."
  {:malli/schema [:function
                  [:=>
                   [:cat schema/JobManagerSystem :string schema/Job
                    schema/PartialResults :int]
                   :nil]]}
  [system worker-id job partial-results execution-time-ms]
  (core/report-partial-success system
                               worker-id
                               job
                               partial-results
                               execution-time-ms))

;;; Maintenance

(defn reset-locked-jobs
  "Resets jobs whose locks have exceeded the configurable timeout.
  Returns count of reset jobs."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem] :int]
                  [:=> [:cat schema/JobManagerSystem :map] :int]]}
  ([system] (core/reset-locked-jobs system))
  ([system opts] (core/reset-locked-jobs system opts)))

(defn gc-task-identifiers
  "Removes task identifiers not referenced by any job and older than the
  retention period. Returns count of deleted rows."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem] :int]
                  [:=> [:cat schema/JobManagerSystem :map] :int]]}
  ([system] (core/gc-task-identifiers system))
  ([system opts] (core/gc-task-identifiers system opts)))

(defn gc-job-history
  "Removes expired job history records. Returns count of deleted rows."
  {:malli/schema [:function [:=> [:cat schema/JobManagerSystem] :int]]}
  [system]
  (core/gc-job-history system))

;;; Rate limits

(defn register-rate-limit
  "Registers or updates a rate limit. capacity is max tokens per interval.
  interval is a SQL interval string (e.g. \"1 minute\")."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem :string pos-int? :string]
                   :any]]}
  [system key capacity interval]
  (core/register-rate-limit system key capacity interval))

(defn refill-rate-limits
  "Refills tokens for all rate limits whose window has expired."
  {:malli/schema [:function [:=> [:cat schema/JobManagerSystem] :any]]}
  [system]
  (core/refill-rate-limits system))
