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

(ns dev.skivi.database.interface
  (:require [dev.skivi.database.schema :as schema]
            [dev.skivi.database.core :as core]))

;;; Connection Management

(defn create-pool
  "Create HikariCP connection pool from config map."
  {:malli/schema [:function [:=> [:cat schema/PoolConfig] schema/Pool]]}
  [config]
  (core/create-pool config))

(defn close-pool
  "Close connection pool and release resources."
  {:malli/schema [:function [:=> [:cat schema/Pool] :nil]]}
  [pool]
  (core/close-pool pool))

;;; Query Execution

(defn execute!
  "Execute SQL query with optional retry logic."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool schema/SqlMap] [:vector :map]]
                  [:=> [:cat schema/Pool schema/SqlMap :map] [:vector :map]]]}
  ([pool sql-map] (execute! pool sql-map {}))
  ([pool sql-map opts] (core/execute! pool sql-map opts)))

(defn execute-one!
  "Execute SQL query returning single result."
  {:malli/schema [:function [:=> [:cat schema/Pool schema/SqlMap] [:maybe :map]]
                  [:=> [:cat schema/Pool schema/SqlMap :map] [:maybe :map]]]}
  ([pool sql-map] (execute-one! pool sql-map {}))
  ([pool sql-map opts] (core/execute-one! pool sql-map opts)))

(defn with-transaction
  "Execute f within transaction, passing connection."
  {:malli/schema [:function [:=> [:cat schema/Pool ifn?] :any]]}
  [pool f]
  (core/with-transaction pool f))

(defn health-check
  "Performs a simple health check by executing a basic query.
   Returns true if the database is accessible, false otherwise."
  {:malli/schema [:function [:=> [:cat schema/Pool] :boolean]]}
  [pool]
  (core/health-check pool))

;;; Job Operations

(defn get-jobs
  "Retrieve and claim available jobs for worker. Records a started history entry
  per job atomically. Returns jobs with :correlation-id embedded."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string] [:vector schema/Job]]
                  [:=> [:cat schema/Pool :string :map] [:vector schema/Job]]]}
  ([pool worker-id] (get-jobs pool worker-id {}))
  ([pool worker-id opts] (core/get-jobs pool worker-id opts)))

(defn complete-jobs
  "Mark jobs as completed and remove from queue."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string [:vector :uuid]]
                   [:vector schema/Job]]]}
  [pool worker-id job-ids]
  (core/complete-jobs pool worker-id job-ids))

(defn fail-jobs
  "Mark jobs as failed with error details."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string [:vector schema/JobFailure]]
                   [:vector schema/Job]]]}
  [pool worker-id job-failures]
  (core/fail-jobs pool worker-id job-failures))

(defn add-job
  "Add single job to queue. Opts may include :rate-limit-key to associate a
  registered rate limit with the job."
  {:malli/schema [:function [:=> [:cat schema/Pool :string :map] schema/Job]
                  [:=> [:cat schema/Pool :string :map :map] schema/Job]]}
  ([pool task-identifier payload] (add-job pool task-identifier payload {}))
  ([pool task-identifier payload opts]
   (core/add-job pool task-identifier payload opts)))

(defn add-jobs
  "Add multiple jobs to queue in transaction."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:vector schema/JobSpec]]
                   [:vector schema/Job]]]}
  [pool job-specs]
  (core/add-jobs pool job-specs))

(defn reschedule-jobs
  "Update runat for jobs."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:vector :uuid] :map]
                   [:vector schema/Job]]]}
  [pool job-ids opts]
  (core/reschedule-jobs pool job-ids opts))

(defn force-unlock-jobs
  "Clear locks on locked jobs. When worker-ids is provided, restricted to those workers."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:maybe [:vector :string]]]
                   [:vector schema/Job]]]}
  [pool worker-ids]
  (core/force-unlock-jobs pool worker-ids))

(defn force-unlock-queues
  "Clear locks on job queues. When queue-names is provided, restricted to those queues."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:maybe [:vector :string]]]
                   [:vector :map]]]}
  [pool queue-names]
  (core/force-unlock-queues pool queue-names))

(defn query-job-history
  "Return job history records matching criteria map."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :map] [:vector schema/HistoryRecord]]]}
  [pool criteria]
  (core/query-job-history pool criteria))

(defn permanently-fail-jobs
  "Set jobs to exhausted status with reason. Unlocks any queues affected."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:vector :uuid] :string]
                   [:vector schema/Job]]]}
  [pool job-ids reason]
  (core/permanently-fail-jobs pool job-ids reason))

;;; Crontab Operations

(defn upsert-crontab!
  "Insert known_crontabs entry if identifier is not already present."
  {:malli/schema [:function [:=> [:cat schema/Pool :string] :any]]}
  [pool identifier]
  (core/upsert-crontab! pool identifier))

(defn load-crontab-state
  "Return last_execution and known_since for identifier, or nil if not found."
  {:malli/schema [:function [:=> [:cat schema/Pool :string] [:maybe :map]]]}
  [pool identifier]
  (core/load-crontab-state pool identifier))

(defn update-last-execution!
  "Set last_execution = now() for identifier in known_crontabs."
  {:malli/schema [:function [:=> [:cat schema/Pool :string] :any]]}
  [pool identifier]
  (core/update-last-execution! pool identifier))

;;; Maintenance Operations

(defn reset-locked-jobs
  "Reset locked jobs exceeding timeout."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]
                  [:=> [:cat schema/Pool :map] :int]]}
  ([pool] (reset-locked-jobs pool {}))
  ([pool opts] (core/reset-locked-jobs pool opts)))

(defn gc-task-identifiers
  "Remove unused task identifiers."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]
                  [:=> [:cat schema/Pool :map] :int]]}
  ([pool] (gc-task-identifiers pool {}))
  ([pool opts] (core/gc-task-identifiers pool opts)))

(defn gc-job-queues
  "Remove empty job queues."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]]}
  [pool]
  (core/gc-job-queues pool))

(defn gc-job-history
  "Remove expired job history records."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]]}
  [pool]
  (core/gc-job-history pool))

;;; Job History

(defn record-job-start
  "Record job execution start. Optional retention is a PostgreSQL interval string
  controlling history expiry (default '30 days')."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool schema/Job :string :uuid]
                   schema/HistoryRecord]
                  [:=> [:cat schema/Pool schema/Job :string :uuid :string]
                   schema/HistoryRecord]]}
  ([pool job worker-id correlation-id]
   (core/record-job-start pool job worker-id correlation-id))
  ([pool job worker-id correlation-id retention]
   (core/record-job-start pool job worker-id correlation-id retention)))

(defn record-job-completion
  "Record successful job completion."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :uuid :string :uuid :int [:maybe :map]]
                   schema/HistoryRecord]]}
  [pool job-id worker-id correlation-id execution-time-ms results]
  (core/record-job-completion pool
                              job-id
                              worker-id
                              correlation-id
                              execution-time-ms
                              results))

(defn record-job-failure
  "Record job failure with error details."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :uuid :string :uuid :int :any]
                   schema/HistoryRecord]]}
  [pool job-id worker-id correlation-id execution-time-ms error]
  (core/record-job-failure pool
                           job-id
                           worker-id
                           correlation-id
                           execution-time-ms
                           error))

(defn record-partial-success
  "Record partial job success with step details."
  {:malli/schema [:function
                  [:=>
                   [:cat schema/Pool :uuid :string :uuid :int
                    schema/PartialResults] schema/HistoryRecord]]}
  [pool job-id worker-id correlation-id execution-time-ms partial-results]
  (core/record-partial-success pool
                               job-id
                               worker-id
                               correlation-id
                               execution-time-ms
                               partial-results))

(defn get-job-history
  "Get history records for job-id."
  {:malli/schema
   [:function [:=> [:cat schema/Pool :uuid] [:vector schema/HistoryRecord]]]}
  [pool job-id]
  (core/get-job-history pool job-id))

(defn get-correlation-history
  "Get history records for correlation-id."
  {:malli/schema
   [:function [:=> [:cat schema/Pool :uuid] [:vector schema/HistoryRecord]]]}
  [pool correlation-id]
  (core/get-correlation-history pool correlation-id))

(defn replay-failed-jobs
  "Replay failed jobs matching criteria."
  {:malli/schema [:function [:=> [:cat schema/Pool :map] [:vector schema/Job]]]}
  [pool criteria]
  (core/replay-failed-jobs pool criteria))

;;; Rate Limit Operations

(defn register-rate-limit
  "Register or update a rate limit. Creates a new limit with full capacity, or
  updates capacity and interval on an existing one (preserving current tokens)."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string :int :string]
                   schema/RateLimit]]}
  [pool key capacity interval]
  (core/register-rate-limit pool key capacity interval))

(defn refill-rate-limits
  "Reset tokens to capacity for all rate limits whose window has expired.
  Returns count of limits refilled."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]]}
  [pool]
  (core/refill-rate-limits pool))

(defn get-rate-limit
  "Retrieve rate limit state by key. Returns nil if not found."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string] [:maybe schema/RateLimit]]]}
  [pool key]
  (core/get-rate-limit pool key))
