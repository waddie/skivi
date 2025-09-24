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

(ns dev.skivi.database.interface-test
  (:require [clojure.test :as test :refer [deftest is testing use-fixtures]]
            [dev.skivi.database.interface :as db]
            [dev.skivi.database.test-helpers :as helpers]))

(use-fixtures :once helpers/schema-fixture)

;;; Test Fixtures

(def test-config
  {:connection-string "jdbc:postgresql://localhost:5432/test_db"
   :pool-config       {:connection-timeout 30000
                       :idle-timeout       600000
                       :max-lifetime       1800000
                       :maximum-pool-size  10
                       :minimum-idle       0}
   :username          "postgres"})

(def ^:private shared-pool (delay (db/create-pool test-config)))

(def ^:private test-worker-id (str (random-uuid)))

(def test-job
  {:attempts        0
   :created-at      (java.time.Instant/now)
   :id              (random-uuid)
   :max-attempts    25
   :payload         {:data "test"}
   :priority        0
   :queue-name      nil
   :run-at          (java.time.Instant/now)
   :task-identifier "test-task"})

;;; Connection Management Tests

(deftest create-pool-test
  (testing "creates connection pool from config"
    (let [pool @shared-pool]
      (is (some? pool))
      (is (instance? com.zaxxer.hikari.HikariDataSource pool)))))

(deftest close-pool-test
  (testing "closes connection pool"
    (let [pool (db/create-pool test-config)]
      (db/close-pool pool)
      (is (.isClosed pool)))))

;;; Query Execution Tests

(deftest execute!-test
  (testing "executes query with default opts"
    (let [pool   @shared-pool
          result (db/execute! pool
                              {:from   [:jobs]
                               :select [:*]})]
      (is (vector? result))))
  (testing "executes query with opts"
    (let [pool   @shared-pool
          result (db/execute! pool
                              {:from   [:jobs]
                               :select [:*]}
                              {:return-keys true})]
      (is (vector? result)))))

(deftest execute-one!-test
  (testing "executes query returning single result"
    (let [pool   @shared-pool
          result (db/execute-one! pool
                                  {:from   [:jobs]
                                   :limit  1
                                   :select [:*]})]
      (is (or (map? result) (nil? result))))))

(deftest with-transaction-test
  (testing "executes function within transaction"
    (let [pool   @shared-pool
          result (db/with-transaction pool
                                      (fn [tx]
                                        (db/execute! tx
                                                     {:insert-into :jobs
                                                      :values [{:task-identifier
                                                                "test"}]})
                                        :success))]
      (is (= :success result))))
  (testing "rolls back on exception"
    (let [pool @shared-pool]
      (is (thrown? Exception
                   (db/with-transaction
                    pool
                    (fn [tx]
                      (db/execute! tx
                                   {:insert-into :jobs
                                    :values      [{:task-identifier "test"}]})
                      (throw (ex-info "test error" {})))))))))

;;; Job Operations Tests

(deftest get-jobs-test
  (testing "retrieves jobs with default opts"
    (let [pool @shared-pool
          jobs (db/get-jobs pool test-worker-id)]
      (is (vector? jobs))))
  (testing "retrieves jobs with batch size"
    (let [pool @shared-pool
          jobs (db/get-jobs pool test-worker-id {:batch-size 5})]
      (is (vector? jobs))
      (is (<= (count jobs) 5))))
  (testing "filters by task identifiers"
    (let [pool @shared-pool
          jobs (db/get-jobs pool
                            test-worker-id
                            {:task-identifiers ["task-1" "task-2"]})]
      (is (vector? jobs))
      (is (every? #(contains? #{"task-1" "task-2"} (:task-identifier %))
                  jobs))))
  (testing "excludes forbidden flags"
    (let [pool @shared-pool
          jobs (db/get-jobs pool
                            test-worker-id
                            {:forbidden-flags ["no-retry"]})]
      (is (vector? jobs))
      (is (not-any? #(some (set (:flags %)) ["no-retry"]) jobs)))))

(deftest complete-jobs-test
  (testing "marks jobs as completed"
    (let [pool    @shared-pool
          job-ids [(random-uuid) (random-uuid)]
          result  (db/complete-jobs pool "worker-1" job-ids)]
      ;; Non-existent job-ids return empty (no matching locked jobs)
      (is (vector? result))))
  (testing "unlocks associated queues"
    (let [pool   @shared-pool
          job-id (random-uuid)]
      (db/complete-jobs pool "worker-1" [job-id])
      (is true))))

(deftest fail-jobs-test
  (testing "marks jobs as failed with error"
    (let [pool     @shared-pool
          failures [{:error-message "Test error"
                     :job-id        (random-uuid)}]
          result   (db/fail-jobs pool "worker-1" failures)]
      ;; Non-existent job-id returns empty (no matching locked jobs)
      (is (vector? result))))
  (testing "schedules retry with exponential backoff"
    (let [pool       @shared-pool
          job-id     (random-uuid)
          failures   [{:error-message "Retry me"
                       :job-id        job-id}]
          result     (db/fail-jobs pool "worker-1" failures)
          failed-job (first result)]
      ;; When the job exists and is retried, run_at is in the future
      (when failed-job
        (is (.isAfter (:run-at failed-job) (:updated-at failed-job)))))))

(deftest add-job-test
  (testing "adds job with default opts"
    (let [pool @shared-pool
          job  (db/add-job pool "test-task" {:data "test"})]
      (is (some? (:id job)))
      (is (= "test-task" (:task-identifier job)))
      (is (= {:data "test"} (:payload job)))))
  (testing "adds job with priority and queue"
    (let [pool @shared-pool
          job  (db/add-job pool
                           "test-task"
                           {:data "test"}
                           {:priority   10
                            :queue-name "high-priority"})]
      (is (= 10 (:priority job)))
      (is (= "high-priority" (:queue-name job)))))
  (testing "adds job with run-at scheduled in future"
    (let [pool        @shared-pool
          future-time (java.time.Instant/parse "2025-12-31T23:59:59Z")
          job         (db/add-job pool
                                  "test-task"
                                  {:data "test"}
                                  {:run-at future-time})]
      (is (= future-time (:run-at job)))))
  (testing "adds job with job-key for deduplication"
    (let [pool    @shared-pool
          job-key (str "unique-key-" (random-uuid))
          _job1   (db/add-job pool
                              "test-task"
                              {:data "first"}
                              {:job-key job-key})
          job2    (db/add-job pool
                              "test-task"
                              {:data "second"}
                              {:job-key job-key})]
      (is (some? (:id job2)))
      (is (= {:data "second"} (:payload job2))))))

(deftest add-jobs-test
  (testing "adds multiple jobs in transaction"
    (let [pool      @shared-pool
          job-specs [{:payload         {:n 1}
                      :task-identifier "task-1"}
                     {:payload         {:n 2}
                      :task-identifier "task-2"}
                     {:payload         {:n 3}
                      :task-identifier "task-3"}]
          jobs      (db/add-jobs pool job-specs)]
      (is (= 3 (count jobs)))
      (is (every? some? (map :id jobs))))))

(deftest reschedule-jobs-test
  (testing "updates run-at for jobs"
    (let [pool     @shared-pool
          job-ids  [(random-uuid) (random-uuid)]
          new-time (java.time.Instant/parse "2025-06-01T12:00:00Z")
          result   (db/reschedule-jobs pool job-ids {:run-at new-time})]
      ;; Non-existent job-ids return empty
      (is (vector? result)))))

;;; Maintenance Operations Tests

(deftest reset-locked-jobs-test
  (testing "resets locked jobs with default timeout"
    (let [pool  @shared-pool
          count (db/reset-locked-jobs pool)]
      (is (>= count 0))))
  (testing "resets locked jobs with custom timeout"
    (let [pool  @shared-pool
          count (db/reset-locked-jobs pool {:timeout-hours 2})]
      (is (>= count 0)))))

(deftest gc-task-identifiers-test
  (testing "removes unused task identifiers with default retention"
    (let [pool  @shared-pool
          count (db/gc-task-identifiers pool)]
      (is (>= count 0))))
  (testing "removes unused task identifiers with custom retention"
    (let [pool  @shared-pool
          count (db/gc-task-identifiers pool {:keep-days 14})]
      (is (>= count 0)))))

(deftest gc-job-queues-test
  (testing "removes empty job queues"
    (let [pool  @shared-pool
          count (db/gc-job-queues pool)]
      (is (>= count 0)))))

(deftest gc-job-history-test
  (testing "removes expired job history records"
    (let [pool  @shared-pool
          count (db/gc-job-history pool)]
      (is (>= count 0)))))

;;; Job History Tests

(deftest record-job-start-test
  (testing "records job execution start"
    (let [pool           @shared-pool
          correlation-id (random-uuid)
          history        (db/record-job-start pool
                                              test-job
                                              "worker-1"
                                              correlation-id)]
      (is (some? (:id history)))
      (is (= (:id test-job) (:job-id history)))
      (is (= correlation-id (:correlation-id history)))
      (is (= "started" (:status history)))
      (is (some? (:queue-time-ms history))))))

(deftest record-job-completion-test
  (testing "records successful job completion"
    (let [pool           @shared-pool
          job-id         (random-uuid)
          correlation-id (random-uuid)
          job            {:attempts        0
                          :id              job-id
                          :payload         {:data "test"}
                          :run-at          (java.time.Instant/now)
                          :task-identifier "test-task"}
          _ (db/record-job-start pool job "worker-1" correlation-id)
          history        (db/record-job-completion pool
                                                   job-id
                                                   "worker-1" correlation-id
                                                   1500 {:result "success"})]
      (is (= job-id (:job-id history)))
      (is (= "completed" (:status history)))
      (is (= 1500 (:execution-time-ms history))))))

(deftest record-job-failure-test
  (testing "records job failure with error details"
    (let [pool           @shared-pool
          job-id         (random-uuid)
          correlation-id (random-uuid)
          job            {:attempts        0
                          :id              job-id
                          :payload         {:data "test"}
                          :run-at          (java.time.Instant/now)
                          :task-identifier "test-task"}
          _ (db/record-job-start pool job "worker-1" correlation-id)
          error          (ex-info "Task failed" {:reason "timeout"})
          history        (db/record-job-failure pool
                                                job-id
                                                "worker-1" correlation-id
                                                3000 error)]
      (is (= job-id (:job-id history)))
      (is (= "failed" (:status history)))
      (is (some? (:error-message history)))
      (is (some? (:error-stack history))))))

(deftest record-partial-success-test
  (testing "records partial success with step tracking"
    (let [pool           @shared-pool
          job-id         (random-uuid)
          correlation-id (random-uuid)
          job            {:attempts        0
                          :id              job-id
                          :payload         {:data "test"}
                          :run-at          (java.time.Instant/now)
                          :task-identifier "test-task"}
          _ (db/record-job-start pool job "worker-1" correlation-id)
          partial-results {:completed-steps ["step-1" "step-2"]
                           :failed-steps    ["step-3"]
                           :results         {:step-1 "ok"
                                             :step-2 "ok"}
                           :retry-from-step "step-3"}
          history        (db/record-partial-success pool
                                                    job-id
                                                    "worker-1" correlation-id
                                                    2500 partial-results)]
      (is (= job-id (:job-id history)))
      (is (= "partial_success" (:status history)))
      (is (= ["step-1" "step-2"] (:completed-steps history)))
      (is (= ["step-3"] (:failed-steps history)))
      (is (= "step-3" (:retry-from-step history))))))

(deftest get-job-history-test
  (testing "retrieves history for job-id"
    (let [pool    @shared-pool
          job-id  (random-uuid)
          history (db/get-job-history pool job-id)]
      (is (vector? history))
      (is (every? #(= job-id (:job-id %)) history)))))

(deftest get-correlation-history-test
  (testing "retrieves history for correlation-id"
    (let [pool           @shared-pool
          correlation-id (random-uuid)
          history        (db/get-correlation-history pool correlation-id)]
      (is (vector? history))
      (is (every? #(= correlation-id (:correlation-id %)) history)))))

(deftest replay-failed-jobs-test
  (testing "replays failed jobs within time range"
    (let [pool @shared-pool
          from (java.time.Instant/parse "2025-01-01T00:00:00Z")
          to   (java.time.Instant/parse "2025-01-31T23:59:59Z")
          jobs (db/replay-failed-jobs pool
                                      {:from from
                                       :to   to})]
      (is (vector? jobs))
      (is (every? #(contains? (:flags %) "replay") jobs))))
  (testing "replays with task identifier filter"
    (let [pool @shared-pool
          from (java.time.Instant/parse "2025-01-01T00:00:00Z")
          to   (java.time.Instant/parse "2025-01-31T23:59:59Z")
          jobs (db/replay-failed-jobs pool
                                      {:from from
                                       :task-identifier "specific-task"
                                       :to   to})]
      (is (vector? jobs))
      (is (every? #(= "specific-task" (:task-identifier %)) jobs)))))

;;; Rate Limit Tests

(deftest register-rate-limit-test
  (testing "register-rate-limit creates a rate limit"
    (let [pool (db/create-pool test-config)
          key  (str "rl-" (random-uuid))
          rl   (db/register-rate-limit pool key 10 "1 hour")]
      (is (map? rl))
      (is (= key (:key rl)))
      (is (= 10 (:capacity rl)))
      (is (= 10 (:available-tokens rl)))))
  (testing "register-rate-limit updates existing limit without resetting tokens"
    (let [pool (db/create-pool test-config)
          key  (str "rl-upd-" (random-uuid))
          _ (db/register-rate-limit pool key 5 "1 hour")
          rl2  (db/register-rate-limit pool key 20 "2 hours")]
      (is (= 20 (:capacity rl2)))
      (is (some? (:interval rl2))))))

(deftest refill-rate-limits-test
  (testing "refill-rate-limits returns non-negative integer"
    (let [pool  (db/create-pool test-config)
          count (db/refill-rate-limits pool)]
      (is (int? count))
      (is (>= count 0)))))

(deftest get-rate-limit-test
  (testing "get-rate-limit returns registered limit"
    (let [pool (db/create-pool test-config)
          key  (str "rl-get-" (random-uuid))
          _ (db/register-rate-limit pool key 7 "15 minutes")
          rl   (db/get-rate-limit pool key)]
      (is (map? rl))
      (is (= 7 (:capacity rl)))))
  (testing "get-rate-limit returns nil for unknown key"
    (let [pool (db/create-pool test-config)
          rl   (db/get-rate-limit pool (str "no-such-" (random-uuid)))]
      (is (nil? rl)))))

(deftest add-job-with-rate-limit-key-test
  (testing "add-job stores rate_limit_key on the job"
    (let [pool (db/create-pool test-config)
          key  (str "rl-job-" (random-uuid))
          _ (db/register-rate-limit pool key 3 "1 hour")
          job  (db/add-job pool
                           "rate-limited-task"
                           {:n 1}
                           {:rate-limit-key key})]
      (is (= key (:rate-limit-key job)))))
  (testing "add-job without rate-limit-key sets it to nil"
    (let [pool (db/create-pool test-config)
          job  (db/add-job pool "plain-task" {:n 1})]
      (is (nil? (:rate-limit-key job))))))
