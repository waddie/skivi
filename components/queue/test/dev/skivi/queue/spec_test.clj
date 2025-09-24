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

;; Spec compliance tests generated from docs/spec/skivi.allium.
;; Each test maps to one or more obligation IDs from `allium plan`.
;; Obligation IDs are cited in comments as [obligation-id].
;;
;; These are integration tests requiring a live PostgreSQL instance at
;; localhost:5432/test_db (see docker-compose.yml).

(ns dev.skivi.queue.spec-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.job-manager.interface :as job-manager]
            [dev.skivi.queue.interface :as queue]
            [dev.skivi.queue.test-helpers :as helpers]))

(use-fixtures :once helpers/schema-fixture)

;;;; ── WorkerClaimsJobs ──────────────────────────────────────────────────────
;;
;; Spec rule: WorkerClaimsJobs
;; The queue polling loop implements WorkerClaimsJobs: it claims a batch from
;; job-manager and makes them available via take-job!.

(deftest poll-cycle-dispatches-job-test
  ;; [rule-success.WorkerClaimsJobs] - queue polls job-manager and makes
  ;; enqueued jobs available
  (testing "queue polls and makes an enqueued job available via take-job!"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          wid     (helpers/unique-worker-id)
          _ (job-manager/add-job sys task-id {:v 1})
          q       (-> (queue/create-queue sys
                                          wid
                                          {:poll-interval-ms 100
                                           :refetch-threshold 5
                                           :size 5
                                           :task-identifiers [task-id]})
                      queue/start!)]
      (try (let [job (queue/take-job! q 3000)]
             (is (some? job) "job is available after polling")
             (is (= task-id (:task-identifier job)))
             (when job (job-manager/complete-jobs sys wid [job] 0)))
           (finally (queue/stop! q 500))))))

(deftest queue-respects-size-limit-test
  ;; [rule-success.WorkerClaimsJobs] - config.local_queue_size limits
  ;; claims per poll batch
  (testing "queue buffers at most :size jobs per poll batch"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          wid     (helpers/unique-worker-id)
          _ (doseq [n (range 6)]
              (job-manager/add-job sys task-id {:n n}))
          q       (-> (queue/create-queue sys
                                          wid
                                          {:poll-interval-ms 100
                                           :refetch-threshold 3
                                           :size 3
                                           :task-identifiers [task-id]})
                      queue/start!)]
      (try (Thread/sleep 300)
           (is (<= (queue/depth q) 3) "buffer depth bounded by :size")
           (finally (queue/stop! q 500)
                    (job-manager/force-unlock-jobs sys [wid]))))))

(deftest queue-dispatches-in-priority-order-test
  ;; [rule-success.WorkerClaimsJobs] - jobs selected in priority ASC,
  ;; run_at ASC, id ASC order
  (testing
    "take-job! returns lower-priority-number (higher-priority) jobs first"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          wid     (helpers/unique-worker-id)
          _ (job-manager/add-job sys task-id {:p 5} {:priority 5})
          _ (job-manager/add-job sys task-id {:p 0} {:priority 0})
          _ (job-manager/add-job sys task-id {:p 2} {:priority 2})
          q       (-> (queue/create-queue sys
                                          wid
                                          {:poll-interval-ms 100
                                           :refetch-threshold 3
                                           :size 3
                                           :task-identifiers [task-id]})
                      queue/start!)]
      (try (let [j1 (queue/take-job! q 3000)
                 j2 (queue/take-job! q 3000)
                 j3 (queue/take-job! q 3000)]
             (is (some? j1))
             (is (some? j2))
             (is (some? j3))
             (when (and j1 j2 j3)
               (is (= [0 2 5] (mapv #(get-in % [:payload :p]) [j1 j2 j3]))
                   "dispatched in priority ascending order")
               (job-manager/complete-jobs sys wid [j1 j2 j3] 0)))
           (finally (queue/stop! q 500))))))

(deftest queue-skips-forbidden-flag-jobs-test
  ;; [rule-success.WorkerClaimsJobs] - forbidden-flags prevents claiming
  ;; matching jobs
  (testing "queue does not claim jobs carrying a forbidden flag"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          wid     (helpers/unique-worker-id)
          _ (job-manager/add-job sys
                                 task-id
                                 {:type "flagged"}
                                 {:flags ["skip-me"]})
          _ (job-manager/add-job sys task-id {:type "clean"} {})
          q       (-> (queue/create-queue sys
                                          wid
                                          {:forbidden-flags ["skip-me"]
                                           :poll-interval-ms 100
                                           :refetch-threshold 5
                                           :size 5
                                           :task-identifiers [task-id]})
                      queue/start!)]
      (try (let [job (queue/take-job! q 3000)]
             (is (some? job) "at least one job dispatched")
             (when job
               (is (= "clean" (get-in job [:payload :type]))
                   "flagged job was skipped")
               (job-manager/complete-jobs sys wid [job] 0)))
           (finally (queue/stop! q 500))))))

(deftest queue-filters-by-task-identifier-test
  ;; [rule-success.WorkerClaimsJobs] - task-identifiers restricts claims to
  ;; specific task types
  (testing "queue only claims jobs whose task-identifier is in the allowlist"
    (let [sys    (helpers/real-system)
          task-a (helpers/unique-task-id)
          task-b (helpers/unique-task-id)
          wid    (helpers/unique-worker-id)
          _ (job-manager/add-job sys task-a {:from "a"})
          _ (job-manager/add-job sys task-b {:from "b"})
          q      (-> (queue/create-queue sys
                                         wid
                                         {:poll-interval-ms 100
                                          :refetch-threshold 5
                                          :size 5
                                          :task-identifiers [task-a]})
                     queue/start!)]
      (try (let [job (queue/take-job! q 3000)]
             (is (some? job))
             (when job
               (is (= task-a (:task-identifier job)))
               (job-manager/complete-jobs sys wid [job] 0)))
           (finally (queue/stop! q 500))))))

;;;; ── WorkerClaimsJob
;;;; ────────────────────────────────────────────────────────
;;
;; Spec rule: WorkerClaimsJob
;; Per-job claims: correlation-id generated and attached, job history recorded.

(deftest correlation-id-preserved-through-buffer-test
  ;; [rule-entity-creation.WorkerClaimsJob.1] - correlation_id generated on
  ;; claim and preserved through the queue buffer
  (testing
    "job returned by take-job! retains the correlation-id set by job-manager"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          wid     (helpers/unique-worker-id)
          _ (job-manager/add-job sys task-id {:v 1})
          q       (-> (queue/create-queue sys
                                          wid
                                          {:poll-interval-ms 100
                                           :refetch-threshold 1
                                           :size 1
                                           :task-identifiers [task-id]})
                      queue/start!)]
      (try (let [job (queue/take-job! q 3000)]
             (is (some? job))
             (when job
               (is (uuid? (get job job-manager/correlation-id-key))
                   "correlation-id is present and is a UUID")
               (job-manager/complete-jobs sys wid [job] 0)))
           (finally (queue/stop! q 500))))))

;;;; ── Polling and buffer lifecycle ──────────────────────────────────────────

(deftest fetched-stat-increases-after-poll-test
  ;; [rule-success.WorkerClaimsJobs] - fetched stat tracks how many jobs
  ;; were claimed from the database across poll cycles
  (testing "fetched stat increments when jobs are claimed from DB"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          wid     (helpers/unique-worker-id)
          _ (doseq [n (range 3)]
              (job-manager/add-job sys task-id {:n n}))
          q       (-> (queue/create-queue sys
                                          wid
                                          {:poll-interval-ms 100
                                           :refetch-threshold 3
                                           :size 3
                                           :task-identifiers [task-id]})
                      queue/start!)]
      (try (Thread/sleep 300)
           (is (pos? (:fetched (queue/stats q)))
               "fetched count > 0 after polling")
           (is (pos? (:refetch-count (queue/stats q))) "refetch-count > 0")
           (finally (queue/stop! q 500)
                    (job-manager/force-unlock-jobs sys [wid]))))))

(deftest stop-returns-unclaimed-buffered-jobs-test
  ;; [rule-success.WorkerClaimsJobs] - jobs buffered but not dispatched are
  ;; returned by stop! so callers can decide how to handle them
  (testing "stop! returns jobs buffered but not dispatched to workers"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          wid     (helpers/unique-worker-id)
          _ (job-manager/add-job sys task-id {:v 1})
          q       (-> (queue/create-queue sys
                                          wid
                                          {:poll-interval-ms 100
                                           :refetch-threshold 5
                                           :size 5
                                           :task-identifiers [task-id]})
                      queue/start!)]
      (Thread/sleep 300)
      (let [drained (queue/stop! q 500)]
        (is (vector? drained))
        (when (seq drained) (job-manager/force-unlock-jobs sys [wid]))))))
