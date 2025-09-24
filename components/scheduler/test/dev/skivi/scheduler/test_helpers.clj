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

(ns dev.skivi.scheduler.test-helpers
  (:require [dev.skivi.database.interface :as db]
            [dev.skivi.migration.interface :as migration]
            [dev.skivi.monitoring.interface :as monitoring]
            [dev.skivi.validation.interface :as validation]
            [malli.instrument :as mi]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.time Instant]))

(def ^:private test-db-url
  (or (System/getenv "SKIVI_TEST_DB_URL")
      "jdbc:postgresql://localhost:5432/test_db?user=postgres"))

(def ^:private pool-config
  {:connection-timeout 30000
   :idle-timeout       600000
   :max-lifetime       1800000
   :maximum-pool-size  5
   :minimum-idle       0})

(def ^:private shared-pool
  (delay (db/create-pool {:connection-string
                          "jdbc:postgresql://localhost:5432/test_db"
                          :pool-config pool-config
                          :username "postgres"})))

(def ^:private result-opts {:builder-fn rs/as-unqualified-kebab-maps})

(defn- migrate-with-retry!
  "Runs pending migrations, retrying up to max-retries times with delay-ms between attempts."
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

(defn real-system
  "Returns a system map backed by the shared test database pool."
  []
  {:pool      @shared-pool
   :validator (validation/noop-validator)})

(defn collecting-emitter
  "Returns a collecting emitter for asserting event emissions in tests."
  []
  (monitoring/collecting-emitter))

(defn noop-emitter
  "Returns a noop emitter for tests that do not assert on events."
  []
  (monitoring/noop-emitter))

(defn unique-identifier
  "Returns a unique crontab identifier for test isolation."
  []
  (str "sched-test-" (random-uuid)))

(defn scheduler-config
  "Returns a fast-polling config suitable for integration tests."
  []
  {:poll-interval-ms 100
   :timezone         "UTC"})

(defn upsert-crontab!
  "Inserts the crontab identifier into known_crontabs if not already present.
  Use when you need the DB record to exist without starting the scheduler."
  [sys identifier]
  (jdbc/execute!
   (:pool sys)
   ["INSERT INTO skivi.known_crontabs (identifier, known_since)
     VALUES (?, now())
     ON CONFLICT (identifier) DO NOTHING"
    identifier]))

(defn set-last-execution!
  "Sets last_execution to the given Instant for identifier in known_crontabs.
  Used in tests to simulate a prior firing at an arbitrary time."
  [sys identifier ^Instant last-execution]
  (jdbc/execute!
   (:pool sys)
   ["UPDATE skivi.known_crontabs SET last_execution = ? WHERE identifier = ?"
    last-execution identifier]))

(defn get-crontab-state
  "Returns the known_crontabs row for identifier, or nil."
  [sys identifier]
  (jdbc/execute-one!
   (:pool sys)
   ["SELECT last_execution, known_since FROM skivi.known_crontabs WHERE identifier = ?"
    identifier]
   result-opts))

(defn count-jobs-by-task
  "Returns the number of available jobs with the given task-identifier."
  [sys task-identifier]
  (-> (jdbc/execute-one!
       (:pool sys)
       ["SELECT count(*) AS n FROM skivi.jobs WHERE task_identifier = ?"
        task-identifier]
       result-opts)
      :n
      int))

(defn delete-jobs-by-task!
  "Removes all jobs with the given task-identifier. Use between tests."
  [sys task-identifier]
  (jdbc/execute! (:pool sys)
                 ["DELETE FROM skivi.jobs WHERE task_identifier = ?"
                  task-identifier]))

(defn delete-crontab!
  "Removes the known_crontabs record for identifier. Use between tests."
  [sys identifier]
  (jdbc/execute! (:pool sys)
                 ["DELETE FROM skivi.known_crontabs WHERE identifier = ?"
                  identifier]))

(defn get-job-by-task
  "Returns the first job row (kebab-map) for the given task-identifier, or nil."
  [sys task-identifier]
  (jdbc/execute-one!
   (:pool sys)
   ["SELECT * FROM skivi.jobs WHERE task_identifier = ? LIMIT 1"
    task-identifier]
   result-opts))

(defn minutes-ago
  "Returns an Instant n minutes in the past."
  [n]
  (.minusSeconds (Instant/now) (* n 60)))
