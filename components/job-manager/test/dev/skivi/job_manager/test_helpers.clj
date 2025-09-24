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

(ns dev.skivi.job-manager.test-helpers
  (:require [dev.skivi.database.interface :as db]
            [dev.skivi.migration.interface :as migration]
            [dev.skivi.validation.interface :as validation]
            [malli.instrument :as mi]
            [next.jdbc.result-set :as rs]))

(def ^:private test-db-url
  (or (System/getenv "SKIVI_TEST_DB_URL")
      "jdbc:postgresql://localhost:5432/test_db?user=postgres"))

(def ^:private pool-config
  {:connection-timeout 30000
   :idle-timeout       600000
   :max-lifetime       1800000
   :maximum-pool-size  5
   :minimum-idle       0})

(def test-config
  {:connection-string "jdbc:postgresql://localhost:5432/test_db"
   :pool-config       pool-config
   :username          "postgres"})

(def ^:private shared-pool (delay (db/create-pool test-config)))

(defn- migrate-with-retry!
  [max-retries delay-ms]
  (loop [remaining max-retries]
    (let [result
          (try (migration/migrate! {:connection-string test-db-url})
               ::ok
               (catch Exception e (when (zero? (dec remaining)) (throw e)) e))]
      (when (instance? Exception result)
        (Thread/sleep ^long delay-ms)
        (recur (dec remaining))))))

(defn schema-fixture
  "Applies pending migrations before the test suite runs."
  [f]
  (migrate-with-retry! 10 1000)
  (mi/instrument!)
  (f)
  (mi/unstrument!))

(defn noop-system
  "Returns a system map with a noop validator (accepts all payloads)."
  []
  {:pool      @shared-pool
   :validator (validation/noop-validator)})

(defn malli-system
  "Returns a system map with a malli validator using the given schema map."
  [schemas]
  {:pool      @shared-pool
   :validator (validation/malli-validator schemas)})

(def ^:private result-opts {:builder-fn rs/as-unqualified-kebab-maps})

(defn get-job
  "Returns the raw job row from the DB by id, or nil if deleted."
  [sys job-id]
  (first (db/execute! (:pool sys)
                      ["SELECT * FROM jobs WHERE id = ?" job-id]
                      result-opts)))

(defn get-job-queue
  "Returns the job_queues row for queue-name, or nil if not found."
  [sys queue-name]
  (first (db/execute! (:pool sys)
                      ["SELECT * FROM job_queues WHERE queue_name = ?"
                       queue-name]
                      result-opts)))

(defn get-job-history
  "Returns all history records for job-id, newest first."
  [sys job-id]
  (db/get-job-history (:pool sys) job-id))

(defn get-task-identifier
  "Returns the task_identifiers row for identifier, or nil if not found."
  [sys identifier]
  (first
   (db/execute!
    (:pool sys)
    ["SELECT identifier, last_used, created_at
                        FROM task_identifiers
                        WHERE identifier = ?"
     identifier]
    result-opts)))

(defn insert-stale-task-identifier!
  "Inserts a task_identifier with last_used 8 days in the past (beyond the
  7-day default retention). Use to set up GarbageCollectTaskIdentifiers tests."
  [sys identifier]
  (db/execute!
   (:pool sys)
   ["INSERT INTO task_identifiers (identifier, last_used, created_at)
                 VALUES (?, now() - interval '8 days', now() - interval '8 days')
                 ON CONFLICT (identifier) DO UPDATE SET last_used = now() - interval '8 days'"
    identifier]
   result-opts))

(defn count-active-jobs-for-task
  "Returns the count of jobs for task-identifier that are not yet exhausted."
  [sys task-identifier]
  (->
    (db/execute!
     (:pool sys)
     ["SELECT count(*) AS n FROM jobs
                     WHERE task_identifier = ? AND attempts < max_attempts"
      task-identifier]
     result-opts)
    first
    :n
    int))

(defn available-jobs-in-queue
  "Returns jobs in the queue that are available (not locked, not exhausted)."
  [sys queue-name]
  (db/execute!
   (:pool sys)
   ["SELECT * FROM jobs
                 WHERE queue_name = ? AND locked_at IS NULL AND attempts < max_attempts"
    queue-name]
   result-opts))

(defn locked-jobs-in-queue
  "Returns jobs in the queue that are currently locked by a worker."
  [sys queue-name]
  (db/execute!
   (:pool sys)
   ["SELECT * FROM jobs WHERE queue_name = ? AND locked_at IS NOT NULL"
    queue-name]
   result-opts))

(defn get-rate-limit
  "Returns the rate_limits row for key, or nil if not found."
  [sys key]
  (db/get-rate-limit (:pool sys) key))
