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

(ns dev.skivi.database.test-helpers
  (:require [dev.skivi.migration.interface :as migration]
            [malli.instrument :as mi]))

(def ^:private test-db-config
  {:connection-string
   (or (System/getenv "SKIVI_TEST_DB_URL")
       "jdbc:postgresql://localhost:5432/test_db?user=postgres")})

(defn- migrate-with-retry!
  "Retries migrate! up to max-retries times with delay-ms between attempts.
  Tolerates transient failures while the database container is starting up."
  [config max-retries delay-ms]
  (loop [remaining max-retries]
    (let [result
          (try (migration/migrate! config)
               ::ok
               (catch Exception e (when (zero? (dec remaining)) (throw e)) e))]
      (when (instance? Exception result)
        (Thread/sleep ^long delay-ms)
        (recur (dec remaining))))))

(defn schema-fixture
  "Applies pending migrations before the test suite runs.
  Retries for up to 10 seconds to allow for container startup."
  [f]
  (migrate-with-retry! test-db-config 10 1000)
  (mi/instrument!)
  (f)
  (mi/unstrument!))
