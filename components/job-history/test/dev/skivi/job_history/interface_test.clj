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

(ns dev.skivi.job-history.interface-test
  "Pure ring-buffer mechanics tests - no database required."
  (:require [clojure.test :refer [deftest is testing]]
            [dev.skivi.job-history.interface :as job-history]
            [dev.skivi.job-history.test-helpers :as helpers]))

(defn- make-record
  "Minimal HistoryRecord map for ring-buffer tests."
  [status]
  {:correlation-id  (helpers/unique-correlation-id)
   :job-id          (helpers/unique-job-id)
   :status          status
   :task-identifier "test-task"})

;;;; ── Store creation ───────────────────────────────────────────────────────

(deftest create-store-starts-with-empty-buffer-test
  (testing "ring buffer is empty immediately after create-store"
    (let [store (helpers/noop-store)]
      (is (= [] (job-history/recent store))))))

(deftest create-store-default-config-test
  (testing "default buffer-size is 1000"
    (let [store (helpers/noop-store)]
      (is (= 1000 (get-in store [:config :buffer-size])))))
  (testing "default history-retention is '30 days'"
    (let [store (helpers/noop-store)]
      (is (= "30 days" (get-in store [:config :history-retention]))))))

(deftest create-store-accepts-config-override-test
  (testing "buffer-size config is respected"
    (let [store (helpers/noop-store-with {:buffer-size 5})]
      (is (= 5 (get-in store [:config :buffer-size])))))
  (testing "history-retention config is respected"
    (let [store (helpers/noop-store-with {:history-retention "7 days"})]
      (is (= "7 days" (get-in store [:config :history-retention]))))))

;;;; ── observe! / ring buffer ───────────────────────────────────────────────

(deftest observe-adds-to-buffer-test
  (testing "observe! adds a record to the ring buffer"
    (let [store (helpers/noop-store)
          rec   (make-record "completed")]
      (job-history/observe! store rec)
      (is (= [rec] (job-history/recent store))))))

(deftest observe-returns-nil-test
  (testing "observe! returns nil"
    (let [store (helpers/noop-store)]
      (is (nil? (job-history/observe! store (make-record "failed")))))))

(deftest observe-multiple-records-insertion-order-test
  (testing "recent returns records in insertion order"
    (let [store (helpers/noop-store)
          rec1  (make-record "completed")
          rec2  (make-record "failed")
          rec3  (make-record "partial_success")]
      (job-history/observe! store rec1)
      (job-history/observe! store rec2)
      (job-history/observe! store rec3)
      (is (= [rec1 rec2 rec3] (job-history/recent store))))))

(deftest ring-buffer-capped-at-buffer-size-test
  (testing "ring buffer drops the oldest entry when full"
    (let [store (helpers/noop-store-with {:buffer-size 3})]
      (dotimes [_ 5]
        (job-history/observe! store (make-record "completed")))
      (is (= 3 (count (job-history/recent store))))))
  (testing "oldest entry is the one dropped when buffer is full"
    (let [store (helpers/noop-store-with {:buffer-size 2})
          rec1  (make-record "completed")
          rec2  (make-record "failed")
          rec3  (make-record "completed")]
      (job-history/observe! store rec1)
      (job-history/observe! store rec2)
      (job-history/observe! store rec3)
      (let [buf (job-history/recent store)]
        (is (= 2 (count buf)))
        (is (not (some #{rec1} buf)) "oldest record was dropped")
        (is (= [rec2 rec3] buf))))))

;;;; ── recent with n ────────────────────────────────────────────────────────

(deftest recent-with-n-returns-last-n-records-test
  (testing "recent with n returns the n most recently inserted records"
    (let [store (helpers/noop-store)
          recs  (mapv (fn [_] (make-record "completed")) (range 5))]
      (doseq [r recs]
        (job-history/observe! store r))
      (is (= (vec (take-last 2 recs)) (job-history/recent store 2)))))
  (testing "recent with n >= buffer count returns all records"
    (let [store (helpers/noop-store)
          recs  (mapv (fn [_] (make-record "failed")) (range 3))]
      (doseq [r recs]
        (job-history/observe! store r))
      (is (= recs (job-history/recent store 10)))))
  (testing "recent with n on empty buffer returns empty vector"
    (let [store (helpers/noop-store)]
      (is (= [] (job-history/recent store 5))))))

(deftest recent-no-arg-returns-all-buffered-records-test
  (testing "recent with no n returns all buffered records"
    (let [store (helpers/noop-store)
          recs  (mapv (fn [_] (make-record "completed")) (range 4))]
      (doseq [r recs]
        (job-history/observe! store r))
      (is (= recs (job-history/recent store))))))

;;;; ── buffer-size = 1 edge case ────────────────────────────────────────────

(deftest single-slot-buffer-always-holds-latest-test
  (testing "buffer-size 1 always holds only the most recently observed record"
    (let [store (helpers/noop-store-with {:buffer-size 1})
          rec1  (make-record "completed")
          rec2  (make-record "failed")]
      (job-history/observe! store rec1)
      (job-history/observe! store rec2)
      (is (= [rec2] (job-history/recent store))))))

;;;; ── concurrent observe! ─────────────────────────────────────────────────

(deftest concurrent-observe-is-safe-test
  (testing "concurrent observe! calls do not lose records beyond buffer-size"
    (let [n     200
          store (helpers/noop-store-with {:buffer-size n})
          futs  (mapv (fn [_]
                        (future (job-history/observe! store
                                                      (make-record
                                                       "completed"))))
                      (range n))]
      (doseq [f futs]
        @f)
      (is (= n (count (job-history/recent store)))))))
