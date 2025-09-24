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

(ns dev.skivi.queue.test-helpers
  (:require [dev.skivi.database.interface :as db]
            [dev.skivi.migration.interface :as migration]
            [dev.skivi.validation.interface :as validation]
            [malli.instrument :as mi]))

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
  "Runs pending migrations before the test suite."
  [f]
  (migrate-with-retry! 10 1000)
  (mi/instrument!)
  (f)
  (mi/unstrument!))

(defn noop-job-system
  "Returns a minimal system map for pure-local tests that never touch the database."
  []
  {:pool      nil
   :validator nil})

(defn real-system
  "Returns a system map backed by the shared test database pool."
  []
  {:pool      @shared-pool
   :validator (validation/noop-validator)})

(defn unique-worker-id
  "Returns a unique worker identifier for test isolation."
  []
  (str "test-queue-" (random-uuid)))

(defn unique-task-id
  "Returns a unique task identifier for test isolation."
  []
  (str "queue-test-task-" (random-uuid)))
