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

(ns dev.skivi.job-history.test-helpers
  (:require [dev.skivi.database.interface :as db]
            [dev.skivi.job-history.interface :as job-history]
            [dev.skivi.migration.interface :as migration]
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
  "Runs migrations, retrying up to max-retries times with delay-ms between attempts."
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
  "Runs pending migrations before the test suite."
  [f]
  (migrate-with-retry! 10 1000)
  (mi/instrument!)
  (f)
  (mi/unstrument!))

(defn test-store
  "Returns a HistoryStore backed by the shared test database pool."
  []
  (job-history/create-store @shared-pool))

(defn test-store-with
  "Returns a HistoryStore backed by the test pool with the given config."
  [config]
  (job-history/create-store @shared-pool config))

(defn noop-store
  "Returns a HistoryStore with no pool for pure ring-buffer tests."
  []
  (job-history/create-store nil))

(defn noop-store-with
  "Returns a noop HistoryStore with the given config."
  [config]
  (job-history/create-store nil config))

(defn unique-job-id
  "Returns a fresh UUID for test job isolation."
  []
  (random-uuid))

(defn unique-correlation-id
  "Returns a fresh UUID for test correlation isolation."
  []
  (random-uuid))

(defn unique-worker-id
  "Returns a unique worker identifier for test isolation."
  []
  (str "test-history-" (random-uuid)))

(defn unique-task-id
  "Returns a unique task identifier for test isolation."
  []
  (str "history-test-task-" (random-uuid)))

(def ^:private result-opts {:builder-fn rs/as-unqualified-kebab-maps})

(defn insert-expired-record!
  "Inserts a history record with expires_at in the past for testing expire!."
  [pool job-id correlation-id task-identifier]
  (db/execute!
   pool
   ["INSERT INTO job_history
      (job_id, correlation_id, task_identifier, payload, status,
       started_at, attempt_number, expires_at)
     VALUES (?, ?, ?, '{}'::jsonb, 'completed',
             now() - interval '2 days', 1,
             now() - interval '1 hour')
     RETURNING *"
    job-id correlation-id task-identifier]
   result-opts))

(defn make-job
  "Builds a minimal job map for use with record-start!."
  [task-identifier]
  {:attempts        0
   :id              (random-uuid)
   :payload         {}
   :run-at          (java.time.Instant/now)
   :task-identifier task-identifier})
