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

(ns dev.skivi.queue.interface-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.queue.interface :as queue]
            [dev.skivi.queue.test-helpers :as helpers]))

(use-fixtures :once helpers/schema-fixture)

;;; -------------------------------------------------------
;;; Pure buffer mechanics (no database required)
;;; Tests seed the buffer via offer-jobs! to avoid DB.
;;; -------------------------------------------------------

(deftest create-queue-test
  (testing "queue starts in a non-running state"
    (let [q (queue/create-queue (helpers/noop-job-system) "w1")]
      (is (false? (queue/running? q)))
      (is (= 0 (queue/depth q)))))
  (testing "worker-id is preserved"
    (let [q (queue/create-queue (helpers/noop-job-system) "my-worker")]
      (is (= "my-worker" (queue/worker-id q))))))

(deftest depth-reflects-buffer-test
  (testing "depth increases after offer-jobs!"
    (let [q   (queue/create-queue (helpers/noop-job-system) "w1")
          job {:id      (random-uuid)
               :payload {}
               :task-identifier "t"}]
      (is (= 0 (queue/depth q)))
      (queue/offer-jobs! q [job])
      (is (= 1 (queue/depth q)))))
  (testing "depth decreases after take-job!"
    (let [q   (queue/create-queue (helpers/noop-job-system) "w1")
          job {:id      (random-uuid)
               :payload {}
               :task-identifier "t"}]
      (queue/offer-jobs! q [job job])
      (queue/take-job! q 50)
      (is (= 1 (queue/depth q))))))

(deftest take-job-returns-nil-on-timeout-test
  (testing "take-job! returns nil when buffer is empty"
    (let [q (queue/create-queue (helpers/noop-job-system) "w1")]
      (is (nil? (queue/take-job! q 30))))))

(deftest take-job-returns-offered-job-test
  (testing "take-job! returns a job placed via offer-jobs!"
    (let [q   (queue/create-queue (helpers/noop-job-system) "w1")
          job {:id      (random-uuid)
               :payload {:x 1}
               :task-identifier "t"}]
      (queue/offer-jobs! q [job])
      (is (= job (queue/take-job! q 100))))))

(deftest take-job-fifo-order-test
  (testing "take-job! respects insertion order when offered directly"
    (let [q    (queue/create-queue (helpers/noop-job-system) "w1")
          job1 {:id      (random-uuid)
                :payload {:n 1}
                :task-identifier "t"}
          job2 {:id      (random-uuid)
                :payload {:n 2}
                :task-identifier "t"}]
      (queue/offer-jobs! q [job1 job2])
      (is (= job1 (queue/take-job! q 50)))
      (is (= job2 (queue/take-job! q 50))))))

(deftest offer-jobs-nil-return-test
  (testing "offer-jobs! returns nil"
    (let [q (queue/create-queue (helpers/noop-job-system) "w1")]
      (is (nil? (queue/offer-jobs! q []))))))

(deftest stop-drains-buffered-jobs-test
  (testing "stop! returns all buffered jobs"
    (let [q    (queue/create-queue (helpers/noop-job-system) "w1")
          job1 {:id      (random-uuid)
                :payload {:n 1}
                :task-identifier "t"}
          job2 {:id      (random-uuid)
                :payload {:n 2}
                :task-identifier "t"}]
      (queue/offer-jobs! q [job1 job2])
      (let [drained (queue/stop! q 500)]
        (is (= 2 (count drained)))
        (is (= #{job1 job2} (set drained))))))
  (testing "stop! on empty queue returns empty vector"
    (let [q (queue/create-queue (helpers/noop-job-system) "w1")]
      (is (= [] (queue/stop! q 500))))))

(deftest stale-entry-discarded-test
  (testing "take-job! discards entries older than ttl-ms"
    (let [q   (queue/create-queue (helpers/noop-job-system) "w1" {:ttl-ms 5})
          job {:id      (random-uuid)
               :payload {}
               :task-identifier "t"}]
      (queue/offer-jobs! q [job])
      (Thread/sleep 10)
      (is (nil? (queue/take-job! q 30))))))

(deftest stale-entry-increments-stat-test
  (testing "stale-dropped stat increments for each skipped entry"
    (let [q   (queue/create-queue (helpers/noop-job-system) "w1" {:ttl-ms 5})
          job {:id      (random-uuid)
               :payload {}
               :task-identifier "t"}]
      (queue/offer-jobs! q [job])
      (Thread/sleep 10)
      (queue/take-job! q 30)
      (is (= 1 (:stale-dropped (queue/stats q)))))))

(deftest stats-initialised-to-zero-test
  (testing "all stats start at zero"
    (let [q (queue/create-queue (helpers/noop-job-system) "w1")
          s (queue/stats q)]
      (is (= 0 (:fetched s)))
      (is (= 0 (:dispatched s)))
      (is (= 0 (:stale-dropped s)))
      (is (= 0 (:refetch-count s))))))

(deftest dispatched-stat-increments-test
  (testing "dispatched stat increments on each successful take-job!"
    (let [q (queue/create-queue (helpers/noop-job-system) "w1")]
      (queue/offer-jobs! q
                         [{:id      (random-uuid)
                           :payload {}
                           :task-identifier "t"}])
      (queue/take-job! q 50)
      (is (= 1 (:dispatched (queue/stats q)))))))

(deftest start-throws-when-already-running-test
  (testing "start! throws when queue is already running"
    (let [sys (helpers/real-system)
          wid (helpers/unique-worker-id)
          q   (queue/create-queue sys
                                  wid
                                  {:poll-interval-ms  5000
                                   :refetch-threshold 1})]
      (queue/start! q)
      (try (is (thrown? clojure.lang.ExceptionInfo (queue/start! q)))
           (finally (queue/stop! q 500))))))
