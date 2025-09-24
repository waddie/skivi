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

(ns dev.skivi.worker-pool.interface-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.worker-pool.interface :as worker-pool]
            [malli.instrument :as mi]))

(use-fixtures :once (fn [f] (mi/instrument!) (f) (mi/unstrument!)))

;;;; ── partial-success helpers ───────────────────────────────────────────────

(deftest partial-success-roundtrip-test
  (testing "partial-success wraps a result map and partial-success? detects it"
    (let [result (worker-pool/partial-success {:completed-steps ["a" "b"]
                                               :failed-steps    ["c"]})]
      (is (worker-pool/partial-success? result))
      (is (= ["a" "b"] (:completed-steps result)))
      (is (= ["c"] (:failed-steps result))))))

(deftest partial-success-predicate-on-plain-values-test
  (testing "partial-success? returns false for plain map"
    (is (not (worker-pool/partial-success? {:completed-steps []}))))
  (testing "partial-success? returns false for nil"
    (is (not (worker-pool/partial-success? nil))))
  (testing "partial-success? returns false for string"
    (is (not (worker-pool/partial-success? "ok"))))
  (testing "partial-success? returns false for any non-tagged value"
    (is (not (worker-pool/partial-success? 42)))))

;;;; ── create-pool structure ─────────────────────────────────────────────────

(deftest create-pool-structure-test
  (testing "create-pool returns a map with required keys"
    (let [pool (worker-pool/create-pool nil {} (constantly nil))]
      (is (map? pool))
      (is (contains? pool :config))
      (is (contains? pool :emitter))
      (is (contains? pool :job-system))
      (is (contains? pool :queue))
      (is (contains? pool :state))
      (is (contains? pool :task-registry)))))

(deftest create-pool-not-running-by-default-test
  (testing "pool is not running after create-pool"
    (let [pool (worker-pool/create-pool nil {} (constantly nil))]
      (is (not (worker-pool/running? pool))))))

(deftest create-pool-initial-stats-test
  (testing "stats start at zero"
    (let [pool (worker-pool/create-pool nil {} (constantly nil))
          s    (worker-pool/stats pool)]
      (is (= 0 (:active s)))
      (is (= 0 (:completed s)))
      (is (= 0 (:failed s)))
      (is (= 0 (:errors s))))))

(deftest create-pool-worker-id-is-string-test
  (testing "worker-id returns a non-blank string"
    (let [pool (worker-pool/create-pool nil {} (constantly nil))]
      (is (string? (worker-pool/worker-id pool)))
      (is (pos? (count (worker-pool/worker-id pool)))))))

(deftest create-pool-worker-id-is-stable-test
  (testing "worker-id is constant across calls on the same pool"
    (let [pool (worker-pool/create-pool nil {} (constantly nil))
          id1  (worker-pool/worker-id pool)
          id2  (worker-pool/worker-id pool)]
      (is (= id1 id2)))))

(deftest create-pool-worker-id-unique-across-pools-test
  (testing "two pools produce distinct worker-ids"
    (let [p1 (worker-pool/create-pool nil {} (constantly nil))
          p2 (worker-pool/create-pool nil {} (constantly nil))]
      (is (not= (worker-pool/worker-id p1) (worker-pool/worker-id p2))))))

(deftest create-pool-config-merged-test
  (testing "supplied config values appear in :config"
    (let [pool (worker-pool/create-pool nil
                                        {}
                                        (constantly nil)
                                        {:concurrency 3
                                         :queue-size  20})]
      (is (= 3 (get-in pool [:config :concurrency])))
      (is (= 20 (get-in pool [:config :queue-size]))))))

(deftest create-pool-default-concurrency-test
  (testing "default concurrency is 10"
    (let [pool (worker-pool/create-pool nil {} (constantly nil))]
      (is (= 10 (get-in pool [:config :concurrency]))))))

;;;; ── active-workers ────────────────────────────────────────────────────────

(deftest active-workers-starts-zero-test
  (testing "active-workers is 0 before any work is done"
    (let [pool (worker-pool/create-pool nil {} (constantly nil))]
      (is (= 0 (worker-pool/active-workers pool))))))
