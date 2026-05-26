;; Copyright (c) 2025-2026 Tom Waddington
;;
;; This software is dual-licensed:
;;
;; 1. GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later)
;;    This program is free software: you can redistribute it and/or modify
;;    it under the terms of the GNU Affero General Public License as published
;;    by the Free Software Foundation, either version 3 of the License, or
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

(ns dev.skivi.job-history.interface
  "Job execution history: DB persistence and in-memory ring buffer of recent executions.

  A HistoryStore wraps a database pool and an atom-backed ring buffer. Create one
  store per system and share it across components that record or query job history.

  Typical usage:

    ;; Create the store (pool from the database component)
    (def store (job-history/create-store pool {:buffer-size 500}))

    ;; In the worker execution path
    (job-history/record-start! store job worker-id correlation-id)
    ;; ... job runs ...
    (job-history/record-completion! store (:id job) worker-id correlation-id elapsed-ms)

    ;; Query DB history
    (job-history/get-by-job-id store job-id)
    (job-history/query store {:from (java.util.Date.) :status \"failed\" :limit 50})

    ;; Inspect recent executions from the ring buffer (no DB)
    (job-history/recent store 20)

    ;; Wire up to monitoring events so the ring buffer is populated without
    ;; routing writes through this component:
    (monitoring/on emitter :job/completed
                   (fn [{:keys [data]}] (job-history/observe! store data)))

    ;; Scheduled maintenance
    (job-history/expire! store)

  Ring buffer notes:
    record-start! writes to the DB only - started records are not buffered.
    record-completion!, record-failure!, and record-partial-success! write to
    the DB and push the terminal record into the ring buffer.
    observe! adds directly to the buffer without any DB interaction."
  (:require [dev.skivi.job-history.core :as core]
            [dev.skivi.job-history.schema :as schema]))

(defn create-store
  "Creates a history store backed by pool with optional config.
  pool may be nil for pure in-memory (ring buffer) use.
  config keys: :buffer-size (default 1000), :history-retention (default '30 days')."
  {:malli/schema [:function
                  [:=> [:cat :any] schema/HistoryStore]
                  [:=> [:cat :any schema/HistoryStoreConfig]
                   schema/HistoryStore]]}
  ([pool] (core/create-store pool))
  ([pool config] (core/create-store pool config)))

(defn record-start!
  "Writes job execution start to the database. Does not add to ring buffer.
  Returns the created HistoryRecord.
  opts key: :history-retention overrides the store's configured retention."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :map :string :uuid]
                   schema/HistoryRecord]
                  [:=> [:cat schema/HistoryStore :map :string :uuid :map]
                   schema/HistoryRecord]]}
  ([store job worker-id correlation-id]
   (core/record-start! store job worker-id correlation-id))
  ([store job worker-id correlation-id opts]
   (core/record-start! store job worker-id correlation-id opts)))

(defn record-completion!
  "Writes job completion to the database and adds the record to the ring buffer.
  Returns the updated HistoryRecord."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :uuid :string :uuid :int]
                   schema/HistoryRecord]]}
  [store job-id worker-id correlation-id execution-time-ms]
  (core/record-completion! store
                           job-id
                           worker-id
                           correlation-id
                           execution-time-ms))

(defn record-failure!
  "Writes job failure to the database and adds the record to the ring buffer.
  error may be a Throwable or a string. Returns the updated HistoryRecord."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :uuid :string :uuid :int :any]
                   schema/HistoryRecord]]}
  [store job-id worker-id correlation-id execution-time-ms error]
  (core/record-failure! store
                        job-id
                        worker-id
                        correlation-id
                        execution-time-ms
                        error))

(defn record-partial-success!
  "Writes partial success to the database and adds the record to the ring buffer.
  Returns the updated HistoryRecord."
  {:malli/schema [:function
                  [:=>
                   [:cat schema/HistoryStore :uuid :string :uuid :int
                    schema/PartialResults]
                   schema/HistoryRecord]]}
  [store job-id worker-id correlation-id execution-time-ms partial-results]
  (core/record-partial-success! store
                                job-id
                                worker-id
                                correlation-id
                                execution-time-ms
                                partial-results))

(defn observe!
  "Adds history-record directly to the ring buffer. No DB interaction.
  Use for testing, or to populate the buffer from monitoring event subscribers."
  {:malli/schema [:function [:=> [:cat schema/HistoryStore :map] :nil]]}
  [store history-record]
  (core/observe! store history-record))

(defn get-by-job-id
  "Returns all history records for job-id from the database, newest first."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :uuid]
                   [:vector schema/HistoryRecord]]]}
  [store job-id]
  (core/get-by-job-id store job-id))

(defn get-by-correlation-id
  "Returns all history records for correlation-id from the database."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :uuid]
                   [:vector schema/HistoryRecord]]]}
  [store correlation-id]
  (core/get-by-correlation-id store correlation-id))

(defn query
  "Returns history records matching criteria map from the database.
  Criteria keys (all optional): :from, :to (inst?), :task-identifier,
  :status ('started' | 'completed' | 'failed' | 'partial_success'),
  :limit (default 100). Results ordered by started_at descending."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore schema/HistoryQuery]
                   [:vector schema/HistoryRecord]]]}
  [store criteria]
  (core/query store criteria))

(defn recent
  "Returns records from the in-memory ring buffer in insertion order.
  With n, returns the last n records. No database access."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore]
                   [:vector schema/HistoryRecord]]
                  [:=> [:cat schema/HistoryStore :int]
                   [:vector schema/HistoryRecord]]]}
  ([store] (core/recent store))
  ([store n] (core/recent store n)))

(defn expire!
  "Deletes history records whose expires_at has passed. Returns count deleted."
  {:malli/schema [:function [:=> [:cat schema/HistoryStore] :int]]}
  [store]
  (core/expire! store))
