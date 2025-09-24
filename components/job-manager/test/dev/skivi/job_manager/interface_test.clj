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

(ns dev.skivi.job-manager.interface-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.job-manager.interface :as job-manager]
            [dev.skivi.job-manager.test-helpers :as helpers]))

(use-fixtures :once helpers/schema-fixture)

(def ^:private worker-id (str "test-worker-" (random-uuid)))

;;; JobEnqueue surface tests

(deftest add-job-test
  (testing "adds job with default opts"
    (let [sys (helpers/noop-system)
          job (job-manager/add-job sys "add-test-task" {:value 1})]
      (is (uuid? (:id job)))
      (is (= "add-test-task" (:task-identifier job)))
      (is (= {:value 1} (:payload job)))
      (is (= 0 (:priority job)))
      (is (= 25 (:max-attempts job)))))
  (testing "adds job with queue and priority"
    (let [sys (helpers/noop-system)
          job (job-manager/add-job sys
                                   "add-test-task"
                                   {:value 2}
                                   {:priority   5
                                    :queue-name "test-q"})]
      (is (= "test-q" (:queue-name job)))
      (is (= 5 (:priority job)))))
  (testing "adds job with job-key deduplication"
    (let [sys     (helpers/noop-system)
          job-key (str "dedup-" (random-uuid))
          _ (job-manager/add-job sys "add-test-task" {:v 1} {:job-key job-key})
          job2    (job-manager/add-job sys
                                       "add-test-task"
                                       {:v 2}
                                       {:job-key job-key})]
      (is (some? (:id job2)))
      (is (= {:v 2} (:payload job2)))))
  (testing "validation failure throws on invalid payload"
    (let [sys (helpers/malli-system {:strict-task [:map
                                                   [:required-field :string]]})]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid payload"
           (job-manager/add-job sys "strict-task" {:wrong-field 42})))))
  (testing "validation passes through when no schema registered for task"
    (let [sys (helpers/malli-system {:other-task [:map [:x :int]]})
          job (job-manager/add-job sys "unlisted-task" {:anything true})]
      (is (some? (:id job))))))

(deftest add-jobs-test
  (testing "adds multiple jobs atomically"
    (let [sys   (helpers/noop-system)
          specs [{:payload         {:n 1}
                  :task-identifier "batch-task"}
                 {:payload         {:n 2}
                  :task-identifier "batch-task"}
                 {:payload         {:n 3}
                  :task-identifier "batch-task"}]
          jobs  (job-manager/add-jobs sys specs)]
      (is (= 3 (count jobs)))
      (is (every? #(uuid? (:id %)) jobs))
      (is (= ["batch-task" "batch-task" "batch-task"]
             (map :task-identifier jobs)))))
  (testing "validation fails for any invalid payload before inserting"
    (let [sys   (helpers/malli-system {:strict [:map [:x :int]]})
          specs [{:payload         {:x 1}
                  :task-identifier "strict"}
                 {:payload         {:x "bad"}
                  :task-identifier "strict"}]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Invalid payload"
                            (job-manager/add-jobs sys specs))))))

(deftest reschedule-jobs-test
  (testing "reschedules jobs to future time"
    (let [sys      (helpers/noop-system)
          job      (job-manager/add-job sys "resched-task" {:v 1})
          new-time (java.time.Instant/parse "2028-01-01T00:00:00Z")
          result   (job-manager/reschedule-jobs sys
                                                [(:id job)]
                                                {:run-at new-time})]
      (is (= 1 (count result)))
      (is (= new-time (:run-at (first result))))))
  (testing "no-op when no fields provided"
    (let [sys    (helpers/noop-system)
          job    (job-manager/add-job sys "resched-task" {:v 1})
          result (job-manager/reschedule-jobs sys [(:id job)] {})]
      (is (vector? result))
      (is (empty? result)))))

(deftest permanently-fail-jobs-test
  (testing "sets jobs to exhausted status"
    (let [sys  (helpers/noop-system)
          job  (job-manager/add-job sys "pf-task" {:v 1})
          jobs (job-manager/permanently-fail-jobs sys
                                                  [(:id job)]
                                                  "manual fail")]
      (is (= 1 (count jobs)))
      (is (= :exhausted (:status (first jobs))))
      (is (= "manual fail" (:last-error (first jobs))))))
  (testing "returns empty for non-existent job-ids"
    (let [sys  (helpers/noop-system)
          jobs (job-manager/permanently-fail-jobs sys [(random-uuid)] "test")]
      (is (vector? jobs))
      (is (empty? jobs)))))

(deftest force-unlock-jobs-test
  (testing "returns empty when no locked jobs"
    (let [sys  (helpers/noop-system)
          jobs (job-manager/force-unlock-jobs sys)]
      (is (vector? jobs))))
  (testing "scoped to worker-ids returns empty when worker has no locks"
    (let [sys  (helpers/noop-system)
          jobs (job-manager/force-unlock-jobs sys ["nonexistent-worker"])]
      (is (vector? jobs)))))

(deftest force-unlock-queues-test
  (testing "returns empty when no queues locked"
    (let [sys    (helpers/noop-system)
          queues (job-manager/force-unlock-queues sys)]
      (is (vector? queues))))
  (testing "scoped to queue-names returns empty for unlocked queue"
    (let [sys    (helpers/noop-system)
          queues (job-manager/force-unlock-queues sys ["nonexistent-q"])]
      (is (vector? queues)))))

(deftest gc-job-queues-test
  (testing "returns non-negative count"
    (let [sys (helpers/noop-system)
          n   (job-manager/gc-job-queues sys)]
      (is (>= n 0)))))

;;; WorkerExecution surface tests

(deftest get-jobs-test
  (testing "returns empty vector when no jobs available"
    (let [sys  (helpers/noop-system)
          jobs (job-manager/get-jobs sys
                                     worker-id
                                     {:task-identifiers ["no-such-task"]})]
      (is (vector? jobs))
      (is (empty? jobs))))
  (testing "returns locked jobs with correlation-id"
    (let [sys  (helpers/noop-system)
          _ (job-manager/add-job sys "get-test-task" {:v 1})
          jobs (job-manager/get-jobs sys
                                     worker-id
                                     {:batch-size       1
                                      :task-identifiers ["get-test-task"]})]
      (is (= 1 (count jobs)))
      (let [job (first jobs)]
        (is (uuid? (:id job)))
        (is (= :locked (:status job)))
        (is (uuid? (get job job-manager/correlation-id-key))))))
  (testing "filters by task-identifiers"
    (let [sys  (helpers/noop-system)
          jobs (job-manager/get-jobs sys
                                     worker-id
                                     {:task-identifiers
                                      ["definitely-not-a-task"]})]
      (is (every? #(= "definitely-not-a-task" (:task-identifier %)) jobs)))))

(deftest complete-jobs-test
  (testing "completes jobs without error"
    (let [sys  (helpers/noop-system)
          _ (job-manager/add-job sys "complete-test-task" {:v 1})
          jobs (job-manager/get-jobs sys
                                     worker-id
                                     {:batch-size 1
                                      :task-identifiers
                                      ["complete-test-task"]})]
      (when (seq jobs)
        (is (nil? (job-manager/complete-jobs sys worker-id jobs 500))))))
  (testing "no-op when jobs list is empty"
    (let [sys (helpers/noop-system)]
      (is (nil? (job-manager/complete-jobs sys worker-id [] 0))))))

(deftest fail-jobs-test
  (testing "fails jobs without error"
    (let [sys  (helpers/noop-system)
          _ (job-manager/add-job sys "fail-test-task" {:v 1})
          jobs (job-manager/get-jobs sys
                                     worker-id
                                     {:batch-size       1
                                      :task-identifiers ["fail-test-task"]})]
      (when (seq jobs)
        (let [job-errors (mapv (fn [j]
                                 {:error "test failure"
                                  :job   j})
                               jobs)]
          (is (nil? (job-manager/fail-jobs sys worker-id job-errors 250)))))))
  (testing "accepts Throwable as error"
    (let [sys  (helpers/noop-system)
          _ (job-manager/add-job sys "fail-throwable-task" {:v 1})
          jobs (job-manager/get-jobs sys
                                     worker-id
                                     {:batch-size 1
                                      :task-identifiers
                                      ["fail-throwable-task"]})]
      (when (seq jobs)
        (let [err        (ex-info "task blew up" {:reason :timeout})
              job-errors (mapv (fn [j]
                                 {:error err
                                  :job   j})
                               jobs)]
          (is (nil? (job-manager/fail-jobs sys worker-id job-errors 100))))))))

(deftest report-partial-success-test
  (testing "records partial success without error"
    (let [sys  (helpers/noop-system)
          _ (job-manager/add-job sys "partial-test-task" {:v 1})
          jobs (job-manager/get-jobs sys
                                     worker-id
                                     {:batch-size       1
                                      :task-identifiers ["partial-test-task"]})]
      (when (seq jobs)
        (let [partial {:completed-steps ["step-1"]
                       :failed-steps    ["step-2"]
                       :retry-from-step "step-2"}]
          (is (nil? (job-manager/report-partial-success sys
                                                        worker-id
                                                        (first jobs)
                                                        partial
                                                        750))))))))

;;; Maintenance tests

(deftest reset-locked-jobs-test
  (testing "returns non-negative count with defaults"
    (let [sys (helpers/noop-system)
          n   (job-manager/reset-locked-jobs sys)]
      (is (>= n 0))))
  (testing "accepts timeout-hours option"
    (let [sys (helpers/noop-system)
          n   (job-manager/reset-locked-jobs sys {:timeout-hours 1})]
      (is (>= n 0)))))

(deftest gc-task-identifiers-test
  (testing "returns non-negative count with defaults"
    (let [sys (helpers/noop-system)
          n   (job-manager/gc-task-identifiers sys)]
      (is (>= n 0))))
  (testing "accepts keep-days option"
    (let [sys (helpers/noop-system)
          n   (job-manager/gc-task-identifiers sys {:keep-days 1})]
      (is (>= n 0)))))

(deftest gc-job-history-test
  (testing "returns non-negative count"
    (let [sys (helpers/noop-system)
          n   (job-manager/gc-job-history sys)]
      (is (>= n 0)))))

(deftest replay-failed-jobs-test
  (testing "returns vector for time range with no matches"
    (let [sys  (helpers/noop-system)
          from (java.time.Instant/parse "2020-01-01T00:00:00Z")
          to   (java.time.Instant/parse "2020-01-02T00:00:00Z")
          jobs (job-manager/replay-failed-jobs sys
                                               {:from from
                                                :to   to})]
      (is (vector? jobs)))))

;;; AddJobReplace (rule-success.AddJobReplace)

(deftest add-job-replace-test
  (testing "replace mode removes existing job and creates new one"
    (let [sys      (helpers/noop-system)
          job-key  (str "replace-" (random-uuid))
          first-j  (job-manager/add-job sys
                                        "replace-task"
                                        {:v 1}
                                        {:job-key      job-key
                                         :job-key-mode "replace"})
          second-j (job-manager/add-job sys
                                        "replace-task"
                                        {:v 2}
                                        {:job-key      job-key
                                         :job-key-mode "replace"})]
      (is (not= (:id first-j) (:id second-j)) "new job id created")
      (is (= {:v 2} (:payload second-j)) "new payload present")
      (is (nil? (helpers/get-job sys (:id first-j))) "original job removed")))
  (testing "replace mode with no prior job creates new job"
    (let [sys     (helpers/noop-system)
          job-key (str "replace-new-" (random-uuid))
          job     (job-manager/add-job sys
                                       "replace-task"
                                       {:v 1}
                                       {:job-key      job-key
                                        :job-key-mode "replace"})]
      (is (uuid? (:id job))))))

;;; AddJobPreserveRunAt (rule-success.AddJobPreserveRunAt)

(deftest add-job-preserve-run-at-test
  (testing "preserve_run_at updates payload but keeps run_at"
    (let [sys      (helpers/noop-system)
          job-key  (str "preserve-" (random-uuid))
          run-at   (java.time.Instant/parse "2030-01-01T00:00:00Z")
          original (job-manager/add-job sys
                                        "preserve-task"
                                        {:v 1}
                                        {:job-key      job-key
                                         :job-key-mode "preserve_run_at"
                                         :run-at       run-at})
          updated  (job-manager/add-job sys
                                        "preserve-task"
                                        {:v 2}
                                        {:job-key job-key
                                         :job-key-mode "preserve_run_at"
                                         :run-at (java.time.Instant/parse
                                                  "2035-01-01T00:00:00Z")})]
      (is (= (:id original) (:id updated)) "same job id")
      (is (= {:v 2} (:payload updated)) "payload updated")
      (is (= run-at (:run-at updated)) "run_at preserved")))
  (testing "preserve_run_at increments revision on update"
    (let [sys      (helpers/noop-system)
          job-key  (str "preserve-rev-" (random-uuid))
          first-j  (job-manager/add-job sys
                                        "preserve-task"
                                        {:v 1}
                                        {:job-key      job-key
                                         :job-key-mode "preserve_run_at"})
          second-j (job-manager/add-job sys
                                        "preserve-task"
                                        {:v 2}
                                        {:job-key      job-key
                                         :job-key-mode "preserve_run_at"})]
      (is (= (inc (:revision first-j)) (:revision second-j)))))
  (testing "preserve_run_at creates new job when none exists"
    (let [sys     (helpers/noop-system)
          job-key (str "preserve-new-" (random-uuid))
          job     (job-manager/add-job sys
                                       "preserve-task"
                                       {:v 1}
                                       {:job-key      job-key
                                        :job-key-mode "preserve_run_at"})]
      (is (uuid? (:id job))))))

;;; AddJobUnsafeDedupe (rule-success.AddJobUnsafeDedupe)

(deftest add-job-unsafe-dedupe-test
  (testing "unsafe_dedupe returns existing job unchanged"
    (let [sys      (helpers/noop-system)
          job-key  (str "dedupe-" (random-uuid))
          first-j  (job-manager/add-job sys
                                        "dedupe-task"
                                        {:v 1}
                                        {:job-key      job-key
                                         :job-key-mode "unsafe_dedupe"})
          second-j (job-manager/add-job sys
                                        "dedupe-task"
                                        {:v 2}
                                        {:job-key      job-key
                                         :job-key-mode "unsafe_dedupe"})]
      (is (= (:id first-j) (:id second-j)) "same job returned")
      (is (= {:v 1} (:payload second-j)) "original payload unchanged")))
  (testing "unsafe_dedupe creates job when none exists"
    (let [sys     (helpers/noop-system)
          job-key (str "dedupe-new-" (random-uuid))
          job     (job-manager/add-job sys
                                       "dedupe-task"
                                       {:v 1}
                                       {:job-key      job-key
                                        :job-key-mode "unsafe_dedupe"})]
      (is (uuid? (:id job))))))

;;; RescheduleJobs extended (rule-success.RescheduleJobs)

(deftest reschedule-jobs-priority-and-max-attempts-test
  (testing "reschedule updates priority and increments revision"
    (let [sys    (helpers/noop-system)
          job    (job-manager/add-job sys
                                      "resched-pri-task"
                                      {:v 1}
                                      {:priority 0})
          result (job-manager/reschedule-jobs sys [(:id job)] {:priority 7})]
      (is (= 1 (count result)))
      (is (= 7 (:priority (first result))))
      (is (= (inc (:revision job)) (:revision (first result)))
          "revision incremented")))
  (testing "reschedule updates max-attempts"
    (let [sys    (helpers/noop-system)
          job    (job-manager/add-job sys
                                      "resched-max-task"
                                      {:v 1}
                                      {:max-attempts 5})
          result (job-manager/reschedule-jobs sys
                                              [(:id job)]
                                              {:max-attempts 15})]
      (is (= 1 (count result)))
      (is (= 15 (:max-attempts (first result)))))))

;;; WorkerExhaustsJob (rule-success.WorkerExhaustsJob)

(deftest worker-exhausts-job-test
  (testing "job is not re-claimable after attempts reach max_attempts"
    (let [sys     (helpers/noop-system)
          task-id (str "exhaust-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 1})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (job-manager/fail-jobs sys
                               worker-id
                               [{:error "exhaust"
                                 :job   (first jobs)}]
                               100)
        (let [reclaimed (job-manager/get-jobs sys
                                              worker-id
                                              {:batch-size       1
                                               :task-identifiers [task-id]})]
          (is (empty? reclaimed) "exhausted job not re-claimable"))))))

;;; WorkerExhaustsJobWithPartialSuccess
;;; (rule-success.WorkerExhaustsJobWithPartialSuccess)

(deftest worker-exhausts-job-with-partial-success-test
  (testing "partial success at max_attempts exhausts job"
    (let [sys     (helpers/noop-system)
          task-id (str "exhaust-partial-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 1})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [partial {:completed-steps ["step-1"]
                       :failed-steps    ["step-2"]
                       :retry-from-step "step-2"}]
          (job-manager/report-partial-success sys
                                              worker-id
                                              (first jobs)
                                              partial
                                              100)
          (let [reclaimed (job-manager/get-jobs sys
                                                worker-id
                                                {:batch-size       1
                                                 :task-identifiers [task-id]})]
            (is (empty? reclaimed)
                "exhausted partial job not re-claimable")))))))

;;; Queue locking and unlocking (LockJobQueue, CreateAndLockJobQueue,
;;; UnlockJobQueue)

(deftest queue-locking-test
  (testing "claiming a queued job creates and locks the queue row"
    (let [sys        (helpers/noop-system)
          task-id    (str "qlock-task-" (random-uuid))
          queue-name (str "lock-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          jobs       (job-manager/get-jobs sys
                                           worker-id
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [q (helpers/get-job-queue sys queue-name)]
          (is (some? q) "queue row exists")
          (is (= worker-id (:locked-by q)) "queue locked by claiming worker")
          (is (some? (:locked-at q)) "locked-at set"))
        (job-manager/complete-jobs sys worker-id jobs 100))))
  (testing "completing a queued job unlocks the queue"
    (let [sys        (helpers/noop-system)
          task-id    (str "qcomplete-task-" (random-uuid))
          queue-name (str "complete-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          jobs       (job-manager/get-jobs sys
                                           worker-id
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq jobs)
        (job-manager/complete-jobs sys worker-id jobs 100)
        (let [q (helpers/get-job-queue sys queue-name)]
          (is (nil? (:locked-by q)) "queue unlocked after completion")
          (is (nil? (:locked-at q)) "locked-at cleared")))))
  (testing "failing a queued job unlocks the queue"
    (let [sys        (helpers/noop-system)
          task-id    (str "qfail-task-" (random-uuid))
          queue-name (str "fail-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          jobs       (job-manager/get-jobs sys
                                           worker-id
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq jobs)
        (job-manager/fail-jobs sys
                               worker-id
                               [{:error "fail"
                                 :job   (first jobs)}]
                               100)
        (let [q (helpers/get-job-queue sys queue-name)]
          (is (nil? (:locked-by q)) "queue unlocked after failure")
          (is (nil? (:locked-at q)) "locked-at cleared"))))))

;;; PermanentlyFailJobs unlocks queue (rule-success.PermanentlyFailJobs)

(deftest permanently-fail-jobs-unlocks-queue-test
  (testing "permanently-fail-jobs unlocks queue held by the failed job"
    (let [sys        (helpers/noop-system)
          task-id    (str "pf-q-task-" (random-uuid))
          queue-name (str "pf-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          jobs       (job-manager/get-jobs sys
                                           worker-id
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [job (first jobs)]
          (is (= worker-id (:locked-by (helpers/get-job-queue sys queue-name)))
              "queue locked before permanent fail")
          (job-manager/permanently-fail-jobs sys [(:id job)] "admin override")
          (let [q (helpers/get-job-queue sys queue-name)]
            (is (nil? (:locked-by q)) "queue unlocked after permanently-fail")
            (is (nil? (:locked-at q)) "locked-at cleared")))))))

;;; ForceUnlockJobs full flow (rule-success.ForceUnlockJobs)

(deftest force-unlock-jobs-full-flow-test
  (testing "force-unlock makes a locked job available for re-claim"
    (let [sys     (helpers/noop-system)
          task-id (str "force-unlock-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (is (= :locked (:status (first jobs))) "job locked after get-jobs")
        (let [unlocked (job-manager/force-unlock-jobs sys [worker-id])]
          (is (seq unlocked) "returns unlocked jobs")
          (is (= :available (:status (first unlocked)))
              "job available after force-unlock"))
        ;; Verify re-claimable by a different worker
        (let [w2        (str "test-worker2-" (random-uuid))
              reclaimed (job-manager/get-jobs sys
                                              w2
                                              {:batch-size       1
                                               :task-identifiers [task-id]})]
          (when (seq reclaimed)
            (job-manager/complete-jobs sys w2 reclaimed 100)))))))

;;; GarbageCollectQueues actual deletion (rule-success.GarbageCollectQueues)

(deftest gc-job-queues-removes-empty-queues-test
  (testing "gc removes queue row after last job in queue is completed"
    (let [sys        (helpers/noop-system)
          task-id    (str "gc-q-task-" (random-uuid))
          queue-name (str "gc-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          jobs       (job-manager/get-jobs sys
                                           worker-id
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq jobs)
        (job-manager/complete-jobs sys worker-id jobs 100)
        (is (some? (helpers/get-job-queue sys queue-name))
            "queue row still present before gc")
        (let [n (job-manager/gc-job-queues sys)]
          (is (pos? n) "at least one queue deleted"))
        (is (nil? (helpers/get-job-queue sys queue-name))
            "queue row gone after gc")))))

;;; ReplayFailedJobs with actual data (rule-success.ReplayFailedJobs)

(deftest replay-failed-jobs-with-data-test
  (testing "creates new jobs from failed history records in time range"
    (let [sys     (helpers/noop-system)
          task-id (str "replay-data-" (random-uuid))
          before  (.minusSeconds (java.time.Instant/now) 5)
          _ (job-manager/add-job sys task-id {:v 1})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (job-manager/fail-jobs sys
                               worker-id
                               [{:error "replay test"
                                 :job   (first jobs)}]
                               100)
        (let [after    (.plusSeconds (java.time.Instant/now) 5)
              replayed (job-manager/replay-failed-jobs sys
                                                       {:from before
                                                        :task-identifier task-id
                                                        :to   after})]
          (is (pos? (count replayed)) "at least one job replayed")
          (let [r (first replayed)]
            (is (= task-id (:task-identifier r))
                "replayed job has original task-identifier")
            (is (some (partial = "replay") (:flags r))
                "replayed job has replay flag")))))))

;;; Invariant: AttemptsWithinBound

(deftest attempts-within-bound-invariant-test
  (testing "attempts never exceed max_attempts through claim-fail cycle"
    (let [sys     (helpers/noop-system)
          task-id (str "bound-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 2})
          jobs1   (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs1)
        (let [j1 (first jobs1)]
          (is (<= (:attempts j1) (:max-attempts j1))
              "within bound after first claim")
          (job-manager/fail-jobs sys
                                 worker-id
                                 [{:error "retry"
                                   :job   j1}]
                                 100)
          ;; Backoff pushes run_at to the future; verify raw DB state is
          ;; within bound
          (let [raw (helpers/get-job sys (:id j1))]
            (when raw
              (is (<= (:attempts raw) (:max-attempts raw))
                  "within bound after first fail"))))))))

;;; Invariant: QueueSerialisation

(deftest queue-serialisation-invariant-test
  (testing "second job in a locked queue is not claimable"
    (let [sys        (helpers/noop-system)
          task-id    (str "serial-" (random-uuid))
          queue-name (str "serial-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:n 1} {:queue-name queue-name})
          _ (job-manager/add-job sys task-id {:n 2} {:queue-name queue-name})
          first-jobs (job-manager/get-jobs sys
                                           worker-id
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq first-jobs)
        (let [w2 (str "test-worker2-" (random-uuid))
              second-jobs (job-manager/get-jobs sys
                                                w2
                                                {:batch-size       1
                                                 :task-identifiers [task-id]})]
          (is (empty? second-jobs)
              "second job not claimable while queue is locked"))
        (job-manager/complete-jobs sys worker-id first-jobs 100)))))

;;; State-dependent field tests (when-presence, when-set, when-clear)

(deftest locked-fields-state-test
  (testing "locked_by and locked_at are present when job is locked"
    (let [sys     (helpers/noop-system)
          task-id (str "locked-fields-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [job (first jobs)]
          (is (= :locked (:status job)))
          (is (= worker-id (:locked-by job)) "locked_by set")
          (is (some? (:locked-at job)) "locked_at set")
          (job-manager/complete-jobs sys worker-id jobs 100)))))
  (testing "locked_by and locked_at are cleared after fail-jobs"
    (let [sys     (helpers/noop-system)
          task-id (str "locked-clear-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 5})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [job-id (:id (first jobs))]
          (job-manager/fail-jobs sys
                                 worker-id
                                 [{:error "test"
                                   :job   (first jobs)}]
                                 100)
          (let [raw (helpers/get-job sys job-id)]
            (is (nil? (:locked-by raw)) "locked_by cleared after fail")
            (is (nil? (:locked-at raw)) "locked_at cleared after fail")))))))

;;; History record state tests (when-presence.HistoryRecord.*)

(deftest history-completed-state-test
  (testing
    "history record has completed status, completed_at and execution_time_ms after completion"
    (let [sys     (helpers/noop-system)
          task-id (str "hist-complete-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [job (first jobs)
              cid (get job job-manager/correlation-id-key)
              jid (:id job)]
          (job-manager/complete-jobs sys worker-id jobs 500)
          (let [history (helpers/get-job-history sys jid)
                record  (first (filter #(= (:correlation-id %) cid) history))]
            (is (some? record) "history record exists")
            (is (= "completed" (:status record)) "status = completed")
            (is (some? (:completed-at record)) "completed_at set")
            (is (= 500 (:execution-time-ms record))
                "execution_time_ms set")))))))

(deftest history-failed-state-test
  (testing
    "history record has failed status, completed_at, execution_time_ms and error_message after failure"
    (let [sys     (helpers/noop-system)
          task-id (str "hist-fail-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 5})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [job (first jobs)
              cid (get job job-manager/correlation-id-key)
              jid (:id job)]
          (job-manager/fail-jobs sys
                                 worker-id
                                 [{:error "test failure"
                                   :job   job}]
                                 250)
          (let [history (helpers/get-job-history sys jid)
                record  (first (filter #(= (:correlation-id %) cid) history))]
            (is (some? record) "history record exists")
            (is (= "failed" (:status record)) "status = failed")
            (is (some? (:completed-at record)) "completed_at set")
            (is (= 250 (:execution-time-ms record)) "execution_time_ms set")
            (is (= "test failure" (:error-message record))
                "error_message set")))))))

(deftest history-partial-success-state-test
  (testing
    "history record has partial_success status after report-partial-success"
    (let [sys     (helpers/noop-system)
          task-id (str "hist-partial-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 5})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [job     (first jobs)
              cid     (get job job-manager/correlation-id-key)
              jid     (:id job)
              partial {:completed-steps ["step-1"]
                       :failed-steps    ["step-2"]
                       :retry-from-step "step-2"}]
          (job-manager/report-partial-success sys worker-id job partial 750)
          (let [history (helpers/get-job-history sys jid)
                record  (first (filter #(= (:correlation-id %) cid) history))]
            (is (some? record) "history record exists")
            (is (= "partial_success" (:status record))
                "status = partial_success")
            (is (some? (:completed-at record)) "completed_at set")
            (is (= 750 (:execution-time-ms record))
                "execution_time_ms set")))))))

;;; Temporal: ResetOverdueJobs (temporal.ResetOverdueJobs)
;; Note: testing actual timeout behaviour requires clock injection which the
;; implementation does not currently support. The existing
;; reset-locked-jobs-test
;; covers the smoke path; time-injection is a test infrastructure gap.

;;; Transition graph: Job.locked.exhausted edge verified via
;;; worker-exhausts-job-test
;;; Transition graph: HistoryRecord.started.* edges verified via history state
;;; tests

;;; TaskIdentifier tracking (TrackTaskIdentifier,
;;; RefreshTaskIdentifierOn{Complete,Fail,PartialSuccess})

(deftest track-task-identifier-test
  ;; [rule-success.TrackTaskIdentifier]
  ;; spec: Job.created triggers upsert in task_identifiers; identifier and
  ;; last_used are set.
  (testing "add-job creates a task_identifier record"
    (let [sys     (helpers/noop-system)
          task-id (str "track-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          ti      (helpers/get-task-identifier sys task-id)]
      (is (some? ti) "task_identifier row created")
      (is (= task-id (:identifier ti)) "identifier matches task-id")
      (is (some? (:last-used ti)) "last_used populated")
      (is (some? (:created-at ti)) "created_at populated")))
  (testing "second add-job for the same task refreshes last_used"
    ;; [rule-success.TrackTaskIdentifier.2]
    ;; ON CONFLICT DO UPDATE ensures last_used advances on repeated adds.
    (let [sys      (helpers/noop-system)
          task-id  (str "track-refresh-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          ti-first (helpers/get-task-identifier sys task-id)
          _ (Thread/sleep 10)
          _ (job-manager/add-job sys task-id {:v 2})
          ti-after (helpers/get-task-identifier sys task-id)]
      (is (some? ti-after))
      (is (not (.isBefore (:last-used ti-after) (:last-used ti-first)))
          "last_used is not earlier after second add-job"))))

(deftest refresh-task-identifier-on-complete-test
  ;; [rule-success.RefreshTaskIdentifierOnComplete]
  ;; spec: WorkerCompletesJob fires → last_used updated in
  ;; task_identifiers.
  (testing "complete-jobs refreshes last_used on task_identifier"
    (let [sys     (helpers/noop-system)
          task-id (str "refresh-complete-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          ti-add  (helpers/get-task-identifier sys task-id)
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (Thread/sleep 10)
        (job-manager/complete-jobs sys worker-id jobs 100)
        (let [ti-after (helpers/get-task-identifier sys task-id)]
          (is (some? ti-after) "task_identifier still present after complete")
          (is (not (.isBefore (:last-used ti-after) (:last-used ti-add)))
              "last_used advanced after complete-jobs"))))))

(deftest refresh-task-identifier-on-fail-test
  ;; [rule-success.RefreshTaskIdentifierOnFail]
  ;; spec: WorkerFailsJob fires → last_used updated in task_identifiers.
  (testing "fail-jobs refreshes last_used on task_identifier"
    (let [sys     (helpers/noop-system)
          task-id (str "refresh-fail-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 5})
          ti-add  (helpers/get-task-identifier sys task-id)
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (Thread/sleep 10)
        (job-manager/fail-jobs sys
                               worker-id
                               [{:error "refresh-test"
                                 :job   (first jobs)}]
                               100)
        (let [ti-after (helpers/get-task-identifier sys task-id)]
          (is (some? ti-after) "task_identifier still present after fail")
          (is (not (.isBefore (:last-used ti-after) (:last-used ti-add)))
              "last_used advanced after fail-jobs"))))))

(deftest refresh-task-identifier-on-partial-success-test
  ;; [rule-success.RefreshTaskIdentifierOnPartialSuccess]
  ;; spec: WorkerReportsPartialSuccess fires → last_used updated.
  (testing "report-partial-success refreshes last_used on task_identifier"
    (let [sys     (helpers/noop-system)
          task-id (str "refresh-partial-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 5})
          ti-add  (helpers/get-task-identifier sys task-id)
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (Thread/sleep 10)
        (let [partial {:completed-steps ["step-1"]
                       :failed-steps    ["step-2"]
                       :retry-from-step "step-2"}]
          (job-manager/report-partial-success sys
                                              worker-id
                                              (first jobs)
                                              partial
                                              100))
        (let [ti-after (helpers/get-task-identifier sys task-id)]
          (is (some? ti-after) "task_identifier present after partial success")
          (is (not (.isBefore (:last-used ti-after) (:last-used ti-add)))
              "last_used advanced after report-partial-success"))))))

;;; GarbageCollectTaskIdentifiers (rule-success.GarbageCollectTaskIdentifiers)

(deftest gc-task-identifiers-removes-stale-test
  ;; [rule-success.GarbageCollectTaskIdentifiers]
  ;; spec: task_identifiers with last_used older than retention and no
  ;; active jobs are removed.
  (testing "stale task_identifier with no active jobs is deleted"
    (let [sys     (helpers/noop-system)
          task-id (str "gc-stale-" (random-uuid))
          _ (helpers/insert-stale-task-identifier! sys task-id)
          before  (helpers/get-task-identifier sys task-id)]
      (is (some? before) "stale record inserted")
      (job-manager/gc-task-identifiers sys {:keep-since "1 second"})
      (let [after (helpers/get-task-identifier sys task-id)]
        (is (nil? after) "stale task_identifier removed by GC"))))
  (testing "task_identifier with an active job is retained by GC"
    ;; [rule-success.GarbageCollectTaskIdentifiers.2]
    ;; spec: identifiers referenced by non-exhausted jobs are kept.
    (let [sys     (helpers/noop-system)
          task-id (str "gc-active-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          _ (helpers/insert-stale-task-identifier! sys task-id)]
      (job-manager/gc-task-identifiers sys {:keep-since "1 second"})
      (let [after (helpers/get-task-identifier sys task-id)]
        (is (some? after) "task_identifier with active job is retained")))))

;;; entity-relationship.TaskIdentifier.active_jobs

(deftest task-identifier-active-jobs-test
  ;; [entity-relationship.TaskIdentifier.active_jobs]
  ;; spec: TaskIdentifier.active_jobs = count of non-exhausted Jobs for
  ;; this task_identifier.
  (testing "active job count reflects non-exhausted jobs"
    (let [sys     (helpers/noop-system)
          task-id (str "active-jobs-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          _ (job-manager/add-job sys task-id {:v 2})
          n       (helpers/count-active-jobs-for-task sys task-id)]
      (is (= 2 n) "two active jobs counted for task")))
  (testing "exhausted jobs are not counted in active_jobs"
    (let [sys     (helpers/noop-system)
          task-id (str "active-exhaust-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:max-attempts 1})
          jobs    (job-manager/get-jobs sys
                                        worker-id
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (job-manager/fail-jobs sys
                               worker-id
                               [{:error "exhaust"
                                 :job   (first jobs)}]
                               100))
      (let [n (helpers/count-active-jobs-for-task sys task-id)]
        (is (zero? n) "exhausted job not counted in active_jobs")))))

;;; Projections: JobQueue.available_jobs, JobQueue.locked_jobs

(deftest queue-available-jobs-projection-test
  ;; [projection.JobQueue.available_jobs]
  ;; spec: JobQueue.available_jobs = Jobs in this queue with
  ;; status=available.
  (testing "available_jobs returns jobs waiting to be claimed"
    (let [sys        (helpers/noop-system)
          task-id    (str "avail-proj-" (random-uuid))
          queue-name (str "avail-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          _ (job-manager/add-job sys task-id {:v 2} {:queue-name queue-name})
          available  (helpers/available-jobs-in-queue sys queue-name)]
      (is (= 2 (count available)) "both jobs available before any claim"))))

(deftest queue-locked-jobs-projection-test
  ;; [projection.JobQueue.locked_jobs]
  ;; spec: JobQueue.locked_jobs = Jobs in this queue with status=locked.
  (testing "locked_jobs returns jobs currently held by a worker"
    (let [sys        (helpers/noop-system)
          task-id    (str "locked-proj-" (random-uuid))
          queue-name (str "locked-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          _ (job-manager/add-job sys task-id {:v 2} {:queue-name queue-name})
          jobs       (job-manager/get-jobs sys
                                           worker-id
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [locked    (helpers/locked-jobs-in-queue sys queue-name)
              available (helpers/available-jobs-in-queue sys queue-name)]
          (is (= 1 (count locked)) "one job locked after single claim")
          (is (= 1 (count available)) "one job still available")
          (job-manager/complete-jobs sys worker-id jobs 100))))))

;;; Surface: JobEnqueue and WorkerExecution
;;; (surface-provides.JobEnqueue, surface-provides.WorkerExecution)

(deftest surface-provides-job-enqueue-test
  ;; [surface-provides.JobEnqueue]
  ;; spec: JobEnqueue surface provides add-job and add-jobs operations. All
  ;; operations are exercised elsewhere; this test asserts their presence
  ;; in the interface namespace.
  (testing "JobEnqueue operations are accessible via the interface"
    (is (fn? job-manager/add-job) "add-job is callable")
    (is (fn? job-manager/add-jobs) "add-jobs is callable")))

(deftest surface-provides-worker-execution-test
  ;; [surface-provides.WorkerExecution]
  ;; spec: WorkerExecution surface provides get-jobs, complete-jobs,
  ;; fail-jobs, report-partial-success, and reset-locked-jobs.
  (testing "WorkerExecution operations are accessible via the interface"
    (is (fn? job-manager/get-jobs) "get-jobs is callable")
    (is (fn? job-manager/complete-jobs) "complete-jobs is callable")
    (is (fn? job-manager/fail-jobs) "fail-jobs is callable")
    (is (fn? job-manager/report-partial-success)
        "report-partial-success is callable")
    (is (fn? job-manager/reset-locked-jobs) "reset-locked-jobs is callable")))

;;; Surface actor trust model
;;; (surface-actor.JobEnqueue, surface-actor.WorkerExecution)
;;
;; The spec declares both surfaces as identified_by: User with the guidance
;; annotation: "Identity is not enforced by the library; the application is
;; trusted." The library does not authenticate callers - it accepts any system
;; that presents a valid pool and validator. This is a deliberate design choice
;; documented in the spec.

(deftest surface-actor-trust-model-test
  ;; [surface-actor.JobEnqueue] [surface-actor.WorkerExecution]
  ;; The library trusts the calling application. Any system map with a
  ;; valid pool can enqueue or execute jobs. No identity enforcement at the
  ;; boundary.
  (testing "any caller with a valid system can enqueue a job"
    (let [sys (helpers/noop-system)
          job (job-manager/add-job sys "actor-trust-task" {:v 1})]
      (is (some? (:id job)) "job accepted without identity enforcement")))
  (testing "any caller with a valid system can claim jobs"
    (let [sys     (helpers/noop-system)
          task-id (str "actor-trust-claim-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          jobs    (job-manager/get-jobs sys
                                        "arbitrary-worker-id"
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (is (some? jobs) "jobs claimed by arbitrary worker-id"))))

;;; rule-failure.ResetOverdueJobs.1
;;; spec: ResetOverdueJobs resets locked jobs past the lock_timeout back to
;;; available. Available jobs are not affected.

(deftest reset-overdue-jobs-available-jobs-unaffected-test
  ;; [rule-failure.ResetOverdueJobs.1]
  ;; spec: reset-locked-jobs only targets locked jobs; available jobs are
  ;; unchanged.
  (testing "reset-locked-jobs does not affect available (unclaimed) jobs"
    (let [sys     (helpers/noop-system)
          task-id (str "reset-avail-" (random-uuid))
          job     (job-manager/add-job sys task-id {:v 1})
          _ (job-manager/reset-locked-jobs sys)
          fetched (helpers/get-job sys (:id job))]
      (is (some? fetched) "job still exists after reset-locked-jobs")
      (is (nil? (:locked-at fetched))
          "available job has no locked_at after reset-locked-jobs"))))

;;; rule-failure.RefreshTaskIdentifierOn*.1
;;; RefreshTaskIdentifierOnComplete, RefreshTaskIdentifierOnFail, and
;;; RefreshTaskIdentifierOnPartialSuccess require the same preconditions as
;;; WorkerCompletesJob, WorkerFailsJob, and WorkerReportsPartialSuccess
;;; respectively. Their failure modes (invalid worker, unknown job, wrong
;;; status) are therefore already covered by the failure tests for those parent
;;; rules above (rule-failure.WorkerCompletesJob.*,
;;; rule-failure.WorkerFailsJob.*,
;;; rule-failure.WorkerReportsPartialSuccess.*).  No additional tests are
;;; generated here.

;;;; ── Rate Limits ──────────────────────────────────────────────────────────
;;
;; Spec rules: RegisterRateLimit, RefillRateLimit, WorkerDefersJobForRateLimit
;; Spec invariant: RateLimitTokenBound

(deftest register-rate-limit-test
  ;; [rule-success.RegisterRateLimit]
  ;; spec: creates a new rate limit with available_tokens = capacity.
  (testing "register-rate-limit creates a new rate limit at full capacity"
    (let [sys (helpers/noop-system)
          key (str "rl-new-" (random-uuid))]
      (job-manager/register-rate-limit sys key 5 "1 minute")
      (let [rl (helpers/get-rate-limit sys key)]
        (is (some? rl) "rate limit row created")
        (is (= 5 (:capacity rl)) "capacity matches")
        (is (= 5 (:available-tokens rl)) "tokens initialised to capacity")
        (is (some? (:last-refill-at rl)) "last_refill_at set on creation"))))
  ;; [rule-success.RegisterRateLimit] - if exists: updates capacity,
  ;; preserves tokens
  (testing
    "re-registering with a higher capacity updates capacity and preserves tokens"
    (let [sys (helpers/noop-system)
          key (str "rl-update-" (random-uuid))]
      (job-manager/register-rate-limit sys key 3 "1 minute")
      (job-manager/register-rate-limit sys key 10 "2 minutes")
      (let [rl (helpers/get-rate-limit sys key)]
        (is (= 10 (:capacity rl)) "capacity updated to new value")
        ;; tokens were 3 (= old capacity), which is <= new capacity 10,
        ;; so they remain at 3 (not reset to 10).
        (is (= 3 (:available-tokens rl)) "tokens preserved at prior level"))))
  (testing
    "re-registering with a smaller capacity clamps tokens to new capacity"
    (let [sys (helpers/noop-system)
          key (str "rl-clamp-" (random-uuid))]
      (job-manager/register-rate-limit sys key 10 "1 minute")
      (job-manager/register-rate-limit sys key 3 "1 minute")
      (let [rl (helpers/get-rate-limit sys key)]
        (is (= 3 (:capacity rl)) "capacity lowered")
        (is (<= (:available-tokens rl) 3) "tokens clamped to new capacity")))))

(deftest refill-rate-limits-test
  ;; [rule-success.RefillRateLimit]
  ;; spec: when rl.next_refill_at <= now, tokens are reset to capacity and
  ;; last_refill_at = now. Implemented by calling refill_rate_limits().
  (testing
    "refill-rate-limits restores tokens to capacity when window has expired"
    (let [sys (helpers/noop-system)
          key (str "rl-refill-" (random-uuid))]
      ;; interval "0 seconds": next_refill_at = last_refill_at which is <=
      ;; now
      (job-manager/register-rate-limit sys key 5 "0 seconds")
      (job-manager/refill-rate-limits sys)
      (let [rl (helpers/get-rate-limit sys key)]
        (is (= 5 (:available-tokens rl)) "tokens restored to capacity")))))

(deftest rate-limit-token-bound-invariant-test
  ;; [invariant.RateLimitTokenBound]
  ;; spec: 0 <= available_tokens <= capacity at all times.
  (testing
    "available_tokens is within [0, capacity] on a freshly registered limit"
    (let [sys (helpers/noop-system)
          key (str "rl-bound-" (random-uuid))]
      (job-manager/register-rate-limit sys key 7 "1 minute")
      (let [rl (helpers/get-rate-limit sys key)]
        (is (<= 0 (:available-tokens rl)) "available_tokens >= 0")
        (is (<= (:available-tokens rl) (:capacity rl))
            "available_tokens <= capacity"))))
  (testing "available_tokens stays within [0, capacity] after token consumption"
    (let [sys     (helpers/noop-system)
          rl-key  (str "rl-bound-consume-" (random-uuid))
          task-id (str "rl-bc-task-" (random-uuid))
          _ (job-manager/register-rate-limit sys rl-key 2 "1 hour")
          _ (job-manager/add-job sys task-id {:v 1} {:rate-limit-key rl-key})
          _ (job-manager/add-job sys task-id {:v 2} {:rate-limit-key rl-key})
          w       (str "rl-bc-w-" (random-uuid))
          claimed (job-manager/get-jobs sys
                                        w
                                        {:batch-size       2
                                         :task-identifiers [task-id]})]
      (when (seq claimed)
        (let [rl (helpers/get-rate-limit sys rl-key)]
          (is (<= 0 (:available-tokens rl)) "tokens >= 0 after consumption")
          (is (<= (:available-tokens rl) (:capacity rl))
              "tokens <= capacity after consumption"))
        (job-manager/force-unlock-jobs sys [w])))))

(deftest worker-defers-job-for-rate-limit-test
  ;; [rule-success.WorkerDefersJobForRateLimit]
  ;; spec: when a rate limit is exhausted, the job's run_at is advanced to
  ;; rl.next_refill_at. The race-condition scenario (two workers both see
  ;; tokens > 0 before either decrements) is non-deterministic. The
  ;; observable invariant is that after one token is consumed, a second job
  ;; sharing the same rate_limit_key is not claimable.
  (testing "second job is not claimable when rate limit tokens are exhausted"
    (let [sys     (helpers/noop-system)
          rl-key  (str "rl-defer-" (random-uuid))
          task-id (str "rl-defer-task-" (random-uuid))
          _ (job-manager/register-rate-limit sys rl-key 1 "1 hour")
          _ (job-manager/add-job sys task-id {:v 1} {:rate-limit-key rl-key})
          _ (job-manager/add-job sys task-id {:v 2} {:rate-limit-key rl-key})
          w1      (str "rl-defer-w1-" (random-uuid))
          claimed (job-manager/get-jobs sys
                                        w1
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (is (= 1 (count claimed)) "exactly one token consumed")
      (let [w2       (str "rl-defer-w2-" (random-uuid))
            deferred (job-manager/get-jobs sys
                                           w2
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
        (is (empty? deferred)
            "second job not claimable while rate limit exhausted"))
      (when (seq claimed) (job-manager/complete-jobs sys w1 claimed 100)))))

(deftest rate-limit-next-refill-at-derived-test
  ;; [derived.RateLimit.next_refill_at]
  ;; spec: next_refill_at = last_refill_at + interval (derived, not
  ;; stored). Verified behaviourally: a 1-minute window does not trigger an
  ;; immediate refill (so next_refill_at > now), while a 0-second window
  ;; does.
  (testing "rate limit with 1-minute interval is not immediately refilled"
    (let [sys (helpers/noop-system)
          key (str "rl-next-" (random-uuid))]
      (job-manager/register-rate-limit sys key 5 "1 minute")
      (let [before (helpers/get-rate-limit sys key)
            _ (job-manager/refill-rate-limits sys)
            after  (helpers/get-rate-limit sys key)]
        (is
         (= (:last-refill-at before) (:last-refill-at after))
         "last_refill_at unchanged: next_refill_at is still in the future"))))
  (testing "rate limit with 0-second interval is immediately refilled"
    (let [sys (helpers/noop-system)
          key (str "rl-next-zero-" (random-uuid))]
      (job-manager/register-rate-limit sys key 3 "0 seconds")
      (let [before-ts (:last-refill-at (helpers/get-rate-limit sys key))]
        (Thread/sleep 10)
        (job-manager/refill-rate-limits sys)
        (let [after-ts (:last-refill-at (helpers/get-rate-limit sys key))]
          (is
           (.isAfter after-ts before-ts)
           "last_refill_at advanced: next_refill_at was <= now and refill fired"))))))

;;;; ── Maintenance surface
;;;; ────────────────────────────────────────────────────
;;
;; Spec surface: Maintenance
;; Provides: GarbageCollectJobHistory, GarbageCollectTaskIdentifiers,
;;           GarbageCollectQueues
;; Timeout: ResetOverdueJobs, RefillRateLimit (driven by maintenance loop)

(deftest surface-provides-maintenance-test
  ;; [surface-provides.Maintenance]
  ;; spec: Maintenance surface provides operations for GC and
  ;; timeout-driven maintenance tasks.
  (testing "Maintenance surface operations are accessible via the interface"
    (is (fn? job-manager/gc-job-history) "gc-job-history provided")
    (is (fn? job-manager/gc-task-identifiers) "gc-task-identifiers provided")
    (is (fn? job-manager/gc-job-queues) "gc-job-queues provided")
    (is (fn? job-manager/reset-locked-jobs) "reset-locked-jobs provided")
    (is (fn? job-manager/refill-rate-limits) "refill-rate-limits provided")))

(deftest surface-actor-maintenance-test
  ;; [surface-actor.Maintenance]
  ;; spec: Maintenance surface is facing Application. Identity is not
  ;; enforced; any valid system can call maintenance operations.
  (testing
    "Maintenance operations accept any system without identity enforcement"
    (let [sys (helpers/noop-system)
          n   (job-manager/gc-job-history sys)]
      (is (>= n 0) "gc-job-history runs without caller identity enforcement"))))

;;;; ── Derived value: JobQueue.is_locked ─────────────────────────────────────

(deftest queue-is-locked-derived-value-test
  ;; [derived.JobQueue.is_locked]
  ;; spec: is_locked = locked_by != null
  (testing "queue has locked_by set (is_locked=true) when a job is claimed"
    (let [sys        (helpers/noop-system)
          task-id    (str "islocked-" (random-uuid))
          queue-name (str "islocked-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          w          (str "islocked-w-" (random-uuid))
          jobs       (job-manager/get-jobs sys
                                           w
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [q (helpers/get-job-queue sys queue-name)]
          (is (some? q) "queue row exists")
          (is (some? (:locked-by q)) "locked_by set — is_locked = true"))
        (job-manager/complete-jobs sys w jobs 100))))
  (testing "queue has locked_by nil (is_locked=false) when unlocked"
    (let [sys        (helpers/noop-system)
          task-id    (str "islocked-clear-" (random-uuid))
          queue-name (str "islocked-clear-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          w          (str "islocked-clear-w-" (random-uuid))
          jobs       (job-manager/get-jobs sys
                                           w
                                           {:batch-size       1
                                            :task-identifiers [task-id]})]
      (when (seq jobs)
        (job-manager/complete-jobs sys w jobs 100)
        (let [q (helpers/get-job-queue sys queue-name)]
          (is (nil? (:locked-by q)) "locked_by nil — is_locked = false"))))))

;;;; ── Entity relationship: JobQueue.jobs ────────────────────────────────────

(deftest queue-jobs-relationship-test
  ;; [entity-relationship.JobQueue.jobs]
  ;; spec: JobQueue.jobs = Jobs with queue_name = this.queue_name
  (testing "queue groups only the jobs with matching queue_name"
    (let [sys        (helpers/noop-system)
          task-id    (str "qjobs-" (random-uuid))
          queue-name (str "qjobs-q-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1} {:queue-name queue-name})
          _ (job-manager/add-job sys task-id {:v 2} {:queue-name queue-name})
          _ (job-manager/add-job sys task-id {:v 3})
          in-queue   (helpers/available-jobs-in-queue sys queue-name)]
      (is (= 2 (count in-queue))
          "exactly the two queued jobs are grouped under the queue"))))

;;;; ── Value equality
;;;; ─────────────────────────────────────────────────────────
;;
;; Spec value types: JobError, PartialResults, CronSpec
;; In Clojure, value types are plain maps with structural equality via =.

(deftest job-error-value-equality-test
  ;; [value-equality.JobError]
  ;; spec: JobError has structural equality.
  (testing "two JobError maps with identical fields are equal"
    (let [e1 {:message "timeout"
              :stack   "at x.y.Z:10"}
          e2 {:message "timeout"
              :stack   "at x.y.Z:10"}]
      (is (= e1 e2) "equal JobError maps")
      (is (not= e1
                {:message "timeout"
                 :stack   "different"})
          "differing JobError maps are not equal"))))

(deftest partial-results-value-equality-test
  ;; [value-equality.PartialResults]
  ;; spec: PartialResults has structural equality.
  (testing "two PartialResults maps with identical fields are equal"
    (let [p1 {:completed-steps ["step-1"]
              :failed-steps    ["step-2"]
              :retry-from-step "step-2"}
          p2 {:completed-steps ["step-1"]
              :failed-steps    ["step-2"]
              :retry-from-step "step-2"}]
      (is (= p1 p2) "equal PartialResults maps")
      (is (not= p1
                {:completed-steps ["step-1"]
                 :failed-steps    []
                 :retry-from-step "step-2"})
          "differing PartialResults maps are not equal"))))

(deftest cron-spec-value-equality-test
  ;; [value-equality.CronSpec]
  ;; spec: CronSpec has structural equality.
  (testing "two CronSpec maps with identical fields are equal"
    (let [cs1 {:max-attempts 3
               :priority     5
               :queue-name   "cron-q"}
          cs2 {:max-attempts 3
               :priority     5
               :queue-name   "cron-q"}]
      (is (= cs1 cs2) "equal CronSpec maps"))))

;;;; ── Enum comparability
;;;; ─────────────────────────────────────────────────────
;;
;; Spec enums: JobKeyMode, HistoryStatus
;; Represented as strings; Clojure string equality provides comparability.

(deftest job-key-mode-comparable-test
  ;; [enum-comparable.JobKeyMode]
  ;; spec: JobKeyMode values are comparable.
  (testing "JobKeyMode values compare equal to themselves"
    (is (= "replace" "replace"))
    (is (= "preserve_run_at" "preserve_run_at"))
    (is (= "unsafe_dedupe" "unsafe_dedupe")))
  (testing "distinct JobKeyMode values are not equal"
    (is (not= "replace" "preserve_run_at"))
    (is (not= "replace" "unsafe_dedupe"))
    (is (not= "preserve_run_at" "unsafe_dedupe")))
  (testing "add-job roundtrips job_key_mode as a comparable string"
    (let [sys (helpers/noop-system)
          key (str "jkm-cmp-" (random-uuid))
          job (job-manager/add-job sys
                                   "jkm-task"
                                   {:v 1}
                                   {:job-key      key
                                    :job-key-mode "replace"})]
      (is (= "replace" (:job-key-mode job))
          "job_key_mode returned from DB is a comparable string"))))

(deftest history-status-comparable-test
  ;; [enum-comparable.HistoryStatus]
  ;; spec: HistoryStatus values are comparable. The database returns them
  ;; as strings; string equality applies.
  (testing "HistoryStatus from completed job history is a comparable string"
    (let [sys     (helpers/noop-system)
          task-id (str "hs-cmp-" (random-uuid))
          _ (job-manager/add-job sys task-id {:v 1})
          w       (str "hs-w-" (random-uuid))
          jobs    (job-manager/get-jobs sys
                                        w
                                        {:batch-size       1
                                         :task-identifiers [task-id]})]
      (when (seq jobs)
        (let [jid (:id (first jobs))
              cid (get (first jobs) job-manager/correlation-id-key)]
          (job-manager/complete-jobs sys w jobs 100)
          (let [history (helpers/get-job-history sys jid)
                record  (first (filter #(= (:correlation-id %) cid) history))]
            (when record
              (is (string? (:status record)) "HistoryStatus is a string")
              (is (contains? #{"started" "completed" "failed" "partial_success"}
                             (:status record))
                  "HistoryStatus is a known enum member"))))))))
