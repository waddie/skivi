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

(ns dev.skivi.job-history.core
  "Job execution history: database persistence and in-memory ring buffer.

  The ring buffer retains the last N terminal records (completed, failed,
  partial_success). record-start! writes to the DB only; the terminal
  record-* functions write to the DB and push to the buffer. observe! adds
  directly to the buffer without any DB interaction - useful for tests and
  for wiring up monitoring-event subscribers.

  Thread safety: the ring buffer state lives in an atom updated with swap!."
  (:require [dev.skivi.database.interface :as db]
            [dev.skivi.job-history.schema :as schema]))

(def ^:private defaults
  {:buffer-size       1000
   :history-retention "30 days"})

(defn- add-to-ring
  "Appends record to buf, dropping the oldest entry when at capacity."
  [buf record capacity]
  (if (>= (count buf) capacity) (conj (subvec buf 1) record) (conj buf record)))

(defn create-store
  "Creates a history store backed by pool with optional config map.
  pool may be nil for pure in-memory use (ring buffer only, no DB operations).
  config keys: :buffer-size (default 1000), :history-retention (default '30 days')."
  {:malli/schema [:function
                  [:=> [:cat :any] schema/HistoryStore]
                  [:=> [:cat :any schema/HistoryStoreConfig]
                   schema/HistoryStore]]}
  ([pool] (create-store pool {}))
  ([pool config]
   {:config (merge defaults config)
    :pool   pool
    :state  (atom {:buffer []})}))

(defn observe!
  "Adds history-record directly to the ring buffer. No DB interaction.
  Use for testing, or to wire up monitoring event subscribers."
  {:malli/schema [:function [:=> [:cat schema/HistoryStore :map] :nil]]}
  [store history-record]
  (let [capacity (get-in store [:config :buffer-size])]
    (swap! (:state store) update :buffer add-to-ring history-record capacity))
  nil)

(defn record-start!
  "Writes job execution start to the database. Does not add to ring buffer.
  Returns the created HistoryRecord. opts key: :history-retention (interval string)."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :map :string :uuid]
                   schema/HistoryRecord]
                  [:=> [:cat schema/HistoryStore :map :string :uuid :map]
                   schema/HistoryRecord]]}
  ([store job worker-id correlation-id]
   (record-start! store job worker-id correlation-id {}))
  ([store job worker-id correlation-id opts]
   (let [retention (or (:history-retention opts)
                       (get-in store [:config :history-retention]))]
     (db/record-job-start (:pool store)
                          job
                          worker-id
                          correlation-id
                          retention))))

(defn record-completion!
  "Writes job completion to the database and adds the record to the ring buffer.
  Returns the updated HistoryRecord."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :uuid :string :uuid :int]
                   schema/HistoryRecord]]}
  [store job-id worker-id correlation-id execution-time-ms]
  (let [record (db/record-job-completion (:pool store)
                                         job-id
                                         worker-id
                                         correlation-id
                                         execution-time-ms
                                         nil)]
    (when record (observe! store record))
    record))

(defn record-failure!
  "Writes job failure to the database and adds the record to the ring buffer.
  error may be a Throwable or a string. Returns the updated HistoryRecord."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :uuid :string :uuid :int :any]
                   schema/HistoryRecord]]}
  [store job-id worker-id correlation-id execution-time-ms error]
  (let [record (db/record-job-failure (:pool store)
                                      job-id
                                      worker-id
                                      correlation-id
                                      execution-time-ms
                                      error)]
    (when record (observe! store record))
    record))

(defn record-partial-success!
  "Writes partial success to the database and adds the record to the ring buffer.
  Returns the updated HistoryRecord."
  {:malli/schema [:function
                  [:=>
                   [:cat schema/HistoryStore :uuid :string :uuid :int
                    schema/PartialResults]
                   schema/HistoryRecord]]}
  [store job-id worker-id correlation-id execution-time-ms partial-results]
  (let [record (db/record-partial-success (:pool store)
                                          job-id
                                          worker-id
                                          correlation-id
                                          execution-time-ms
                                          partial-results)]
    (when record (observe! store record))
    record))

(defn get-by-job-id
  "Returns all history records for job-id, newest first."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :uuid]
                   [:vector schema/HistoryRecord]]]}
  [store job-id]
  (db/get-job-history (:pool store) job-id))

(defn get-by-correlation-id
  "Returns all history records for correlation-id."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore :uuid]
                   [:vector schema/HistoryRecord]]]}
  [store correlation-id]
  (db/get-correlation-history (:pool store) correlation-id))

(defn query
  "Returns history records matching criteria map from the database.
  Criteria keys (all optional): :from, :to (inst?), :task-identifier,
  :status, :limit (default 100)."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore schema/HistoryQuery]
                   [:vector schema/HistoryRecord]]]}
  [store criteria]
  (db/query-job-history (:pool store) criteria))

(defn recent
  "Returns records from the in-memory ring buffer in insertion order.
  With n, returns the last n records. No DB access."
  {:malli/schema [:function
                  [:=> [:cat schema/HistoryStore]
                   [:vector schema/HistoryRecord]]
                  [:=> [:cat schema/HistoryStore :int]
                   [:vector schema/HistoryRecord]]]}
  ([store] (:buffer @(:state store)))
  ([store n]
   (let [buf (:buffer @(:state store))]
     (if (>= n (count buf)) buf (subvec buf (- (count buf) n))))))

(defn expire!
  "Deletes history records whose expires_at has passed. Returns count deleted."
  {:malli/schema [:function [:=> [:cat schema/HistoryStore] :int]]}
  [store]
  (db/gc-job-history (:pool store)))
