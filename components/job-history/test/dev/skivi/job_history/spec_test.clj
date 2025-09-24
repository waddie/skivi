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

;; Spec compliance tests derived from docs/spec/skivi.allium.
;;
;; These tests verify that the job-history component correctly implements the
;; history side of the spec rules. Integration tests requiring a live
;; PostgreSQL
;; instance at localhost:5432/test_db (see docker-compose.yml).
;;
;; Rule obligations verified here:
;;   WorkerClaimsJob   - record-start! creates HistoryRecord with
;;   status=started
;;   WorkerCompletesJob - record-completion! transitions status to completed
;;   WorkerFailsJob     - record-failure! transitions status to failed
;;   WorkerReportsPartialSuccess - record-partial-success! transitions to
;;   partial_success
;;   ExpireHistoryRecords - expire! deletes records past expires_at
;;   ReplayFailedJobs   - query filters by status=failed across a time range

(ns dev.skivi.job-history.spec-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.job-history.interface :as job-history]
            [dev.skivi.job-history.test-helpers :as helpers]))

(use-fixtures :once helpers/schema-fixture)

;;;; ── WorkerClaimsJob ───────────────────────────────────────────────────────
;;
;; Spec rule: WorkerClaimsJob
;; ensures: HistoryRecord.created(status: started, ...)

(deftest record-start-creates-started-history-record-test
  ;; [rule-entity-creation.WorkerClaimsJob] - HistoryRecord created at
  ;; claim time
  (testing "record-start! creates a HistoryRecord with status=started"
    (let [store (helpers/test-store)
          job   (helpers/make-job (helpers/unique-task-id))
          cid   (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "worker-1" cid)
          recs  (job-history/get-by-job-id store (:id job))]
      (is (= 1 (count recs)) "exactly one record created")
      (is (= "started" (:status (first recs))))
      (is (= cid (:correlation-id (first recs)))
          "correlation-id is preserved")))
  (testing "record-start! sets worker-id on the history record"
    (let [store (helpers/test-store)
          job   (helpers/make-job (helpers/unique-task-id))
          cid   (helpers/unique-correlation-id)
          wid   (helpers/unique-worker-id)
          _ (job-history/record-start! store job wid cid)
          recs  (job-history/get-by-job-id store (:id job))]
      (is (= wid (:worker-id (first recs))))))
  (testing "record-start! sets attempt-number equal to job attempts"
    (let [store (helpers/test-store)
          job   (assoc (helpers/make-job (helpers/unique-task-id)) :attempts 2)
          cid   (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          recs  (job-history/get-by-job-id store (:id job))]
      (is (= 2 (:attempt-number (first recs)))))))

(deftest record-start-stores-task-identifier-test
  ;; [rule-entity-creation.WorkerClaimsJob] - task_identifier copied from
  ;; job
  (testing "record-start! preserves the job's task-identifier"
    (let [store (helpers/test-store)
          task  (helpers/unique-task-id)
          job   (helpers/make-job task)
          cid   (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          recs  (job-history/get-by-job-id store (:id job))]
      (is (= task (:task-identifier (first recs)))))))

(deftest get-by-correlation-id-returns-started-record-test
  ;; [rule-entity-creation.WorkerClaimsJob] - correlation_id generated and
  ;; stored
  (testing "get-by-correlation-id returns the record created by record-start!"
    (let [store (helpers/test-store)
          job   (helpers/make-job (helpers/unique-task-id))
          cid   (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          recs  (job-history/get-by-correlation-id store cid)]
      (is (= 1 (count recs)))
      (is (= (:id job) (:job-id (first recs)))))))

;;;; ── WorkerCompletesJob ────────────────────────────────────────────────────
;;
;; Spec rule: WorkerCompletesJob
;; ensures: history_record.status = completed

(deftest record-completion-transitions-to-completed-test
  ;; [rule-success.WorkerCompletesJob] - status transitions started ->
  ;; completed
  (testing "record-completion! transitions history record status to completed"
    (let [store  (helpers/test-store)
          job    (helpers/make-job (helpers/unique-task-id))
          cid    (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          result (job-history/record-completion! store (:id job) "w1" cid 500)]
      (is (= "completed" (:status result)))
      (is (= 500 (:execution-time-ms result)))))
  (testing "completed record is added to the ring buffer"
    (let [store (helpers/test-store)
          job   (helpers/make-job (helpers/unique-task-id))
          cid   (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          _ (job-history/record-completion! store (:id job) "w1" cid 100)]
      (is (some #(= cid (:correlation-id %)) (job-history/recent store))
          "completed record appears in ring buffer")))
  (testing "completed-at is set on the record"
    (let [store  (helpers/test-store)
          job    (helpers/make-job (helpers/unique-task-id))
          cid    (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          result (job-history/record-completion! store (:id job) "w1" cid 200)]
      (is (some? (:completed-at result)) "completed-at is set")
      (is (inst? (:completed-at result))))))

;;;; ── WorkerFailsJob ────────────────────────────────────────────────────────
;;
;; Spec rule: WorkerFailsJob / WorkerExhaustsJob
;; ensures: history_record.status = failed, history_record.error = error

(deftest record-failure-transitions-to-failed-test
  ;; [rule-success.WorkerFailsJob] - status transitions started -> failed
  (testing "record-failure! transitions history record status to failed"
    (let [store  (helpers/test-store)
          job    (helpers/make-job (helpers/unique-task-id))
          cid    (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          result (job-history/record-failure! store
                                              (:id job)
                                              "w1" cid
                                              300 "connection refused")]
      (is (= "failed" (:status result)))
      (is (= 300 (:execution-time-ms result)))))
  (testing "error message is stored on failure"
    (let [store  (helpers/test-store)
          job    (helpers/make-job (helpers/unique-task-id))
          cid    (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          result (job-history/record-failure! store
                                              (:id job)
                                              "w1" cid
                                              100 "timeout")]
      (is (= "timeout" (:error-message result)))))
  (testing "failed record is added to the ring buffer"
    (let [store (helpers/test-store)
          job   (helpers/make-job (helpers/unique-task-id))
          cid   (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          _ (job-history/record-failure! store (:id job) "w1" cid 100 "err")]
      (is (some #(= "failed" (:status %)) (job-history/recent store))
          "failed record appears in ring buffer")))
  (testing "record-failure! accepts a Throwable as error"
    (let [store  (helpers/test-store)
          job    (helpers/make-job (helpers/unique-task-id))
          cid    (helpers/unique-correlation-id)
          ex     (ex-info "task error" {:code 42})
          _ (job-history/record-start! store job "w1" cid)
          result (job-history/record-failure! store (:id job) "w1" cid 50 ex)]
      (is (= "task error" (:error-message result))
          "ex-message extracted from Throwable"))))

;;;; ── WorkerReportsPartialSuccess ──────────────────────────────────────────
;;
;; Spec rule: WorkerReportsPartialSuccess
;; ensures: history_record.status = partial_success, history_record.partial =
;; partial_results

(deftest record-partial-success-transitions-status-test
  ;; [rule-success.WorkerReportsPartialSuccess] - status = partial_success
  (testing "record-partial-success! transitions status to partial_success"
    (let [store   (helpers/test-store)
          job     (helpers/make-job (helpers/unique-task-id))
          cid     (helpers/unique-correlation-id)
          partial {:completed-steps ["validate" "process"]
                   :failed-steps    ["notify"]
                   :results         nil
                   :retry-from-step "notify"}
          _ (job-history/record-start! store job "w1" cid)
          result  (job-history/record-partial-success! store
                                                       (:id job)
                                                       "w1" cid
                                                       250 partial)]
      (is (= "partial_success" (:status result)))
      (is (= 250 (:execution-time-ms result)))))
  (testing "partial success step fields are persisted"
    (let [store   (helpers/test-store)
          job     (helpers/make-job (helpers/unique-task-id))
          cid     (helpers/unique-correlation-id)
          partial {:completed-steps ["step-a"]
                   :failed-steps    ["step-b"]
                   :results         nil
                   :retry-from-step "step-b"}
          _ (job-history/record-start! store job "w1" cid)
          result  (job-history/record-partial-success! store
                                                       (:id job)
                                                       "w1" cid
                                                       100 partial)]
      (is (= ["step-a"] (:completed-steps result)))
      (is (= ["step-b"] (:failed-steps result)))
      (is (= "step-b" (:retry-from-step result)))))
  (testing "partial success record is added to the ring buffer"
    (let [store   (helpers/test-store)
          job     (helpers/make-job (helpers/unique-task-id))
          cid     (helpers/unique-correlation-id)
          partial {:completed-steps []
                   :failed-steps    ["all"]
                   :results         nil}
          _ (job-history/record-start! store job "w1" cid)
          _ (job-history/record-partial-success! store
                                                 (:id job)
                                                 "w1" cid
                                                 100 partial)]
      (is (some #(= "partial_success" (:status %)) (job-history/recent store))
          "partial_success record appears in ring buffer"))))

;;;; ── HistoryRecord.transitions: started cannot be re-entered ──────────────

(deftest multiple-attempts-create-separate-records-test
  ;; The spec models each attempt as a separate HistoryRecord (started ->
  ;; terminal). Multiple record-start! calls for the same job-id create
  ;; multiple records.
  (testing "each call to record-start! creates a distinct HistoryRecord"
    (let [store (helpers/test-store)
          job   (helpers/make-job (helpers/unique-task-id))
          cid1  (helpers/unique-correlation-id)
          cid2  (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid1)
          _ (job-history/record-start! store job "w1" cid2)
          recs  (job-history/get-by-job-id store (:id job))]
      (is (= 2 (count recs)) "two distinct history records created")
      (is (= #{cid1 cid2} (set (map :correlation-id recs)))))))

;;;; ── query ─────────────────────────────────────────────────────────────────
;;
;; Spec rule: ReplayFailedJobs reads HistoryRecords filtered by status/time
;; range.
;; The query function is the Clojure-side equivalent of the replay_failed_jobs
;; PostgreSQL function's selection logic.

(deftest query-filters-by-status-test
  ;; [rule-success.ReplayFailedJobs] - history is queryable by status
  (testing "query with :status='failed' returns only failed records"
    (let [store    (helpers/test-store)
          task     (helpers/unique-task-id)
          job-ok   (helpers/make-job task)
          job-fail (helpers/make-job task)
          cid-ok   (helpers/unique-correlation-id)
          cid-fail (helpers/unique-correlation-id)
          _ (job-history/record-start! store job-ok "w1" cid-ok)
          _ (job-history/record-start! store job-fail "w1" cid-fail)
          _ (job-history/record-completion! store (:id job-ok) "w1" cid-ok 100)
          _ (job-history/record-failure! store
                                         (:id job-fail)
                                         "w1" cid-fail
                                         50 "oops")
          results  (job-history/query store
                                      {:status "failed"
                                       :task-identifier task})]
      (is (every? #(= "failed" (:status %)) results)
          "all returned records have failed status")
      (is (some #(= cid-fail (:correlation-id %)) results)
          "the failed record is present"))))

(deftest query-filters-by-task-identifier-test
  (testing "query with :task-identifier returns only records for that task"
    (let [store  (helpers/test-store)
          task-a (helpers/unique-task-id)
          task-b (helpers/unique-task-id)
          job-a  (helpers/make-job task-a)
          job-b  (helpers/make-job task-b)
          cid-a  (helpers/unique-correlation-id)
          cid-b  (helpers/unique-correlation-id)
          _ (job-history/record-start! store job-a "w1" cid-a)
          _ (job-history/record-start! store job-b "w1" cid-b)
          result (job-history/query store {:task-identifier task-a})]
      (is (every? #(= task-a (:task-identifier %)) result)
          "all returned records belong to task-a"))))

(deftest query-respects-limit-test
  (testing "query :limit caps result count"
    (let [store (helpers/test-store)
          task  (helpers/unique-task-id)]
      (dotimes [_ 5]
        (let [job (helpers/make-job task)
              cid (helpers/unique-correlation-id)]
          (job-history/record-start! store job "w1" cid)))
      (let [result (job-history/query store
                                      {:limit 2
                                       :task-identifier task})]
        (is (<= (count result) 2) "result count bounded by :limit")))))

(deftest query-empty-criteria-returns-records-test
  (testing "query with empty criteria map returns results (up to default limit)"
    (let [store  (helpers/test-store)
          job    (helpers/make-job (helpers/unique-task-id))
          cid    (helpers/unique-correlation-id)
          _ (job-history/record-start! store job "w1" cid)
          result (job-history/query store {})]
      (is (vector? result)))))

;;;; ── ExpireHistoryRecords ──────────────────────────────────────────────────
;;
;; Spec rule: ExpireHistoryRecords
;; when: record.expires_at <= now
;; ensures: not exists record

(deftest expire-removes-expired-records-test
  ;; [rule-success.ExpireHistoryRecords] - records past expires_at are
  ;; deleted
  (testing "expire! deletes records whose expires_at has passed"
    (let [store         (helpers/test-store)
          pool          (:pool store)
          job-id        (helpers/unique-job-id)
          cid           (helpers/unique-correlation-id)
          task          (helpers/unique-task-id)
          _ (helpers/insert-expired-record! pool job-id cid task)
          before        (job-history/get-by-job-id store job-id)
          deleted-count (job-history/expire! store)
          after         (job-history/get-by-job-id store job-id)]
      (is (= 1 (count before)) "record exists before expire!")
      (is (pos? deleted-count) "expire! reports at least one deleted record")
      (is (= 0 (count after)) "record is gone after expire!"))))

(deftest expire-does-not-delete-unexpired-records-test
  ;; [rule-invariant.ExpireHistoryRecords] - only expired records are
  ;; deleted
  (testing "expire! does not delete records with future expires_at"
    (let [store (helpers/test-store)
          job   (helpers/make-job (helpers/unique-task-id))
          cid   (helpers/unique-correlation-id)
          _ (job-history/record-start! store
                                       job
                                       "w1"
                                       cid
                                       {:history-retention "90 days"})
          _ (job-history/expire! store)
          recs  (job-history/get-by-job-id store (:id job))]
      (is (= 1 (count recs)) "record with future expiry is not deleted"))))

;;;; ── ring buffer does not contain started records ─────────────────────────

(deftest record-start-does-not-populate-ring-buffer-test
  ;; Started records are not terminal; they are excluded from the ring
  ;; buffer to keep recent reflecting completed executions only.
  (testing "record-start! does not add to the ring buffer"
    (let [store        (helpers/test-store)
          job          (helpers/make-job (helpers/unique-task-id))
          cid          (helpers/unique-correlation-id)
          count-before (count (job-history/recent store))
          _ (job-history/record-start! store job "w1" cid)]
      (is (= count-before (count (job-history/recent store)))
          "ring buffer size unchanged after record-start!"))))
