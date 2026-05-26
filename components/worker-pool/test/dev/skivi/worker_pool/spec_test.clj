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

;; Spec compliance tests derived from docs/spec/skivi.allium, WorkerExecution
;; surface and the worker lifecycle rules.
;;
;; Each test maps to one or more rule/obligation IDs cited in comments.
;; These are integration tests requiring a live PostgreSQL instance at
;; localhost:5432/test_db (see docker-compose.yml).

(ns dev.skivi.worker-pool.spec-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.job-manager.interface :as job-manager]
            [dev.skivi.monitoring.interface :as monitoring]
            [dev.skivi.worker-pool.interface :as worker-pool]
            [dev.skivi.worker-pool.test-helpers :as helpers]))

(use-fixtures :once helpers/schema-fixture)

;;;; ── WorkerCompletesJob ────────────────────────────────────────────────────
;;
;; Spec rule: WorkerCompletesJob
;; When a task fn returns normally the job must be deleted from the queue and
;; a completed history record must exist.

(deftest pool-executes-and-completes-job-test
  ;; [rule-success.WorkerCompletesJob] - task fn executes; job removed from
  ;; queue
  (testing "pool picks up a job and the task fn is invoked"
    (let [sys      (helpers/real-system)
          task-id  (helpers/unique-task-id)
          executed (promise)
          tasks    {task-id (fn [{:keys [job]}]
                              (deliver executed (:payload job)))}
          em       (helpers/noop-emitter)
          pool     (-> (worker-pool/create-pool sys
                                                tasks
                                                em
                                                (assoc (helpers/pool-config)
                                                       :task-identifiers
                                                       [task-id]))
                       worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:value 42})
           (let [payload (deref executed 5000 ::timeout)]
             (is (not= ::timeout payload) "task fn was invoked within 5 s")
             (is (= {:value 42} payload) "task fn received correct payload"))
           (finally (worker-pool/stop! pool 3000))))))

(deftest pool-completion-increments-stats-test
  ;; [rule-success.WorkerCompletesJob] - completed counter tracks
  ;; successful executions
  (testing "stats :completed increments after successful task execution"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_] (deliver done true))}
          em      (helpers/noop-emitter)
          pool    (-> (worker-pool/create-pool sys
                                               tasks
                                               em
                                               (assoc (helpers/pool-config)
                                                      :task-identifiers
                                                      [task-id]))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1})
           (deref done 5000 nil)
           (Thread/sleep 200)
           (is (pos? (:completed (worker-pool/stats pool)))
               ":completed stat incremented after task returns")
           (finally (worker-pool/stop! pool 3000))))))

(deftest pool-emits-completed-event-test
  ;; [rule-success.WorkerCompletesJob] - :job/completed event emitted
  (testing ":job/completed event is emitted after a task succeeds"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_] (deliver done true))}
          em      (helpers/collecting-emitter)
          pool    (-> (worker-pool/create-pool sys
                                               tasks
                                               em
                                               (assoc (helpers/pool-config)
                                                      :task-identifiers
                                                      [task-id]))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1})
           (deref done 5000 nil)
           (Thread/sleep 200)
           (let [events (monitoring/events em)]
             (is (some #(= :job/completed (:type %)) events)
                 ":job/completed event present in emitter buffer"))
           (finally (worker-pool/stop! pool 3000))))))

;;;; ── WorkerFailsJob
;;;; ─────────────────────────────────────────────────────────
;;
;; Spec rule: WorkerFailsJob
;; When a task fn throws the job must have its attempts incremented and be
;; rescheduled (or exhausted if max_attempts reached).

(deftest pool-fails-job-when-task-throws-test
  ;; [rule-success.WorkerFailsJob] - failed counter increments when task
  ;; throws
  (testing "stats :failed increments after task fn throws"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_]
                             (deliver done true)
                             (throw (ex-info "task error" {})))}
          em      (helpers/noop-emitter)
          pool    (-> (worker-pool/create-pool sys
                                               tasks
                                               em
                                               (assoc (helpers/pool-config)
                                                      :task-identifiers
                                                      [task-id]))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1} {:max-attempts 1})
           (deref done 5000 nil)
           (Thread/sleep 300)
           (is (pos? (:failed (worker-pool/stats pool)))
               ":failed stat incremented after task throws")
           (finally (worker-pool/stop! pool 3000))))))

(deftest pool-emits-failed-event-test
  ;; [rule-success.WorkerFailsJob] - :job/failed event emitted for
  ;; non-terminal failure
  (testing ":job/failed event is emitted when task throws and retry remains"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_]
                             (deliver done true)
                             (throw (ex-info "transient" {})))}
          em      (helpers/collecting-emitter)
          pool    (-> (worker-pool/create-pool sys
                                               tasks
                                               em
                                               (assoc (helpers/pool-config)
                                                      :task-identifiers
                                                      [task-id]))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1} {:max-attempts 3})
           (deref done 5000 nil)
           (Thread/sleep 300)
           (let [events (monitoring/events em)
                 types  (set (map :type events))]
             (is (contains? types :job/failed)
                 ":job/failed event present when max_attempts not reached"))
           (finally (worker-pool/stop! pool 3000))))))

;;;; ── WorkerExhaustsJob
;;;; ──────────────────────────────────────────────────────
;;
;; Spec rule: WorkerExhaustsJob
;; When a task fn throws on the final allowed attempt the job becomes
;; exhausted.

(deftest pool-emits-exhausted-event-on-final-attempt-test
  ;; [rule-success.WorkerExhaustsJob] - :job/exhausted emitted when
  ;; attempts >= max
  (testing
    ":job/exhausted event emitted when task fails its only allowed attempt"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_]
                             (deliver done true)
                             (throw (ex-info "permanent" {})))}
          em      (helpers/collecting-emitter)
          pool    (-> (worker-pool/create-pool sys
                                               tasks
                                               em
                                               (assoc (helpers/pool-config)
                                                      :task-identifiers
                                                      [task-id]))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1} {:max-attempts 1})
           (deref done 5000 nil)
           (Thread/sleep 300)
           (let [events (monitoring/events em)
                 types  (set (map :type events))]
             (is (contains? types :job/exhausted)
                 ":job/exhausted emitted on terminal failure"))
           (finally (worker-pool/stop! pool 3000))))))

;;;; ── WorkerReportsPartialSuccess ───────────────────────────────────────────
;;
;; Spec rule: WorkerReportsPartialSuccess
;; When a task fn returns partial-success the job is rescheduled and history
;; records partial_success status.

(deftest pool-handles-partial-success-test
  ;; [rule-success.WorkerReportsPartialSuccess] - partial-success return
  ;; triggers report-partial-success on job-manager
  (testing
    "task fn returning partial-success increments completed and emits event"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_]
                             (deliver done true)
                             (worker-pool/partial-success
                              {:completed-steps ["step-1"]
                               :failed-steps    ["step-2"]
                               :retry-from-step "step-2"}))}
          em      (helpers/collecting-emitter)
          pool    (-> (worker-pool/create-pool sys
                                               tasks
                                               em
                                               (assoc (helpers/pool-config)
                                                      :task-identifiers
                                                      [task-id]))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1} {:max-attempts 3})
           (deref done 5000 nil)
           (Thread/sleep 300)
           (let [events (monitoring/events em)
                 types  (set (map :type events))]
             (is (contains? types :job/partial-success)
                 ":job/partial-success event emitted"))
           (finally (worker-pool/stop! pool 3000))))))

;;;; ── Pool lifecycle events
;;;; ──────────────────────────────────────────────────

(deftest pool-start-event-test
  ;; Pool emits :pool/start with :concurrency when started
  (testing ":pool/start event emitted on start!"
    (let [sys  (helpers/real-system)
          em   (helpers/collecting-emitter)
          pool (-> (worker-pool/create-pool sys {} em (helpers/pool-config))
                   worker-pool/start!)]
      (try (let [start-evt (->> (monitoring/events em)
                                (filter #(= :pool/start (:type %)))
                                first)]
             (is (some? start-evt) ":pool/start event present")
             (is (= 2 (get-in start-evt [:data :concurrency]))
                 ":concurrency matches config"))
           (finally (worker-pool/stop! pool 3000))))))

(deftest pool-stop-event-test
  ;; Pool emits :pool/stop on graceful stop!
  (testing ":pool/stop event emitted on stop!"
    (let [sys  (helpers/real-system)
          em   (helpers/collecting-emitter)
          pool (-> (worker-pool/create-pool sys {} em (helpers/pool-config))
                   worker-pool/start!)]
      (worker-pool/stop! pool 3000)
      (let [stop-evt (->> (monitoring/events em)
                          (filter #(= :pool/stop (:type %)))
                          first)]
        (is (some? stop-evt) ":pool/stop event present")
        (is (false? (get-in stop-evt [:data :forced?]))
            ":forced? is false for graceful stop")))))

(deftest pool-force-stop-event-test
  ;; Pool emits :pool/stop with :forced? true on force-stop!
  (testing ":pool/stop with :forced? true emitted on force-stop!"
    (let [sys  (helpers/real-system)
          em   (helpers/collecting-emitter)
          pool (-> (worker-pool/create-pool sys {} em (helpers/pool-config))
                   worker-pool/start!)]
      (worker-pool/force-stop! pool)
      (let [stop-evt (->> (monitoring/events em)
                          (filter #(= :pool/stop (:type %)))
                          first)]
        (is (some? stop-evt) ":pool/stop event present")
        (is (true? (get-in stop-evt [:data :forced?]))
            ":forced? is true for force-stop!")))))

;;;; ── Unknown task - job exhausts immediately ───────────────────────────────
;;
;; When a job arrives with a task-identifier not in the registry the worker
;; exhausts it immediately regardless of max-attempts. A missing handler is a
;; configuration error that will not resolve between retries, so the retry
;; cycle is bypassed. The worker thread must not crash. :worker/error is
;; reserved for infrastructure-level exceptions outside task execution.

(deftest pool-exhausts-job-for-unknown-task-test
  ;; [rule-success.JobExhaustedMissingHandler] - unknown task-identifier
  ;; immediately exhausts the job
  (testing
    ":job/exhausted emitted and worker survives when task-id has no handler"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          em      (helpers/collecting-emitter)
          done    (promise)
          _ (monitoring/on em :job/exhausted (fn [_] (deliver done true)))
          pool    (-> (worker-pool/create-pool sys
                                               {}
                                               em
                                               (assoc (helpers/pool-config)
                                                      :task-identifiers
                                                      [task-id]))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1} {:max-attempts 1})
           (let [received (deref done 5000 ::timeout)]
             (is (not= ::timeout received)
                 ":job/exhausted emitted within 5 s for unregistered task")
             (is (worker-pool/running? pool)
                 "worker thread survived the failure"))
           (finally (worker-pool/stop! pool 3000))))))

(deftest pool-exhausts-missing-handler-job-bypassing-retries-test
  ;; [rule-success.JobExhaustedMissingHandler] - bypass retry cycle even
  ;; when max-attempts > 1
  (testing
    ":job/exhausted emitted on first attempt even when max-attempts is high"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          em      (helpers/collecting-emitter)
          done    (promise)
          _ (monitoring/on em :job/exhausted (fn [_] (deliver done true)))
          pool    (-> (worker-pool/create-pool sys
                                               {}
                                               em
                                               (assoc (helpers/pool-config)
                                                      :task-identifiers
                                                      [task-id]))
                      worker-pool/start!)]
      (try
        (job-manager/add-job sys task-id {:x 1} {:max-attempts 10})
        (let [received (deref done 5000 ::timeout)]
          (is (not= ::timeout received)
              ":job/exhausted emitted on first attempt for unregistered task")
          (is (worker-pool/running? pool) "worker thread survived the failure"))
        (finally (worker-pool/stop! pool 3000))))))

;;;; ── running? lifecycle ────────────────────────────────────────────────────

(deftest running-predicate-lifecycle-test
  ;; running? reflects the pool state correctly through start/stop
  (testing "running? is false before start, true after start, false after stop"
    (let [sys  (helpers/real-system)
          pool (worker-pool/create-pool sys
                                        {}
                                        (helpers/noop-emitter)
                                        (helpers/pool-config))]
      (is (not (worker-pool/running? pool)) "not running before start!")
      (worker-pool/start! pool)
      (is (worker-pool/running? pool) "running after start!")
      (worker-pool/stop! pool 3000)
      (is (not (worker-pool/running? pool)) "not running after stop!"))))

(deftest double-start-throws-test
  ;; Starting an already-running pool must throw
  (testing "start! throws if pool is already running"
    (let [sys  (helpers/real-system)
          pool (-> (worker-pool/create-pool sys
                                            {}
                                            (helpers/noop-emitter)
                                            (helpers/pool-config))
                   worker-pool/start!)]
      (try (is (thrown? Exception (worker-pool/start! pool))
               "second start! throws")
           (finally (worker-pool/stop! pool 3000))))))

;;;; ── Job execution timeout ─────────────────────────────────────────────────

(deftest max-job-execution-time-timeout-test
  (testing
    "job exceeding :max-job-execution-time-ms is failed and :job/timeout is emitted"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_]
                             (try (Thread/sleep 5000)
                                  (catch InterruptedException _ nil)))}
          em      (helpers/collecting-emitter)
          _ (monitoring/on em :job/timeout (fn [_] (deliver done true)))
          pool    (-> (worker-pool/create-pool
                       sys
                       tasks
                       em
                       (assoc (helpers/pool-config)
                              :task-identifiers [task-id]
                              :max-job-execution-time-ms 200))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1} {:max-attempts 1})
           (let [received (deref done 3000 ::timeout)]
             (is (not= ::timeout received) ":job/timeout emitted within 3 s")
             (Thread/sleep 300)
             (is (pos? (:failed (worker-pool/stats pool)))
                 "pool :failed counter incremented after timeout"))
           (finally (worker-pool/stop! pool 3000))))))

(deftest max-job-execution-time-no-timeout-test
  (testing "fast task completes normally when :max-job-execution-time-ms is set"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_] (deliver done true))}
          em      (helpers/collecting-emitter)
          pool    (-> (worker-pool/create-pool
                       sys
                       tasks
                       em
                       (assoc (helpers/pool-config)
                              :task-identifiers [task-id]
                              :max-job-execution-time-ms 2000))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1})
           (deref done 5000 nil)
           (Thread/sleep 200)
           (let [events (monitoring/events em)
                 types  (set (map :type events))]
             (is (contains? types :job/completed)
                 ":job/completed emitted for fast task with timeout set")
             (is (not (contains? types :job/timeout))
                 "no :job/timeout event for fast task"))
           (finally (worker-pool/stop! pool 3000))))))

(deftest max-job-execution-time-nil-test
  (testing "task completes normally when :max-job-execution-time-ms is nil"
    (let [sys     (helpers/real-system)
          task-id (helpers/unique-task-id)
          done    (promise)
          tasks   {task-id (fn [_] (Thread/sleep 300) (deliver done true))}
          em      (helpers/collecting-emitter)
          pool    (-> (worker-pool/create-pool
                       sys
                       tasks
                       em
                       (assoc (helpers/pool-config)
                              :task-identifiers [task-id]
                              :max-job-execution-time-ms nil))
                      worker-pool/start!)]
      (try (job-manager/add-job sys task-id {:x 1})
           (let [received (deref done 5000 ::timeout)]
             (is (not= ::timeout received)
                 "task with nil timeout completes without being interrupted")
             (Thread/sleep 200)
             (let [events (monitoring/events em)
                   types  (set (map :type events))]
               (is (contains? types :job/completed) ":job/completed emitted")
               (is (not (contains? types :job/timeout))
                   "no :job/timeout event")))
           (finally (worker-pool/stop! pool 3000))))))

;;;; ── Graceful shutdown waits for in-flight work ────────────────────────────

(deftest graceful-shutdown-timeout-config-test
  (testing "stop! with no args uses :graceful-shutdown-timeout-ms from config"
    (let [sys          (helpers/real-system)
          task-id      (helpers/unique-task-id)
          task-started (promise)
          task-done    (promise)
          tasks        {task-id (fn [_]
                                  (deliver task-started true)
                                  (Thread/sleep 50)
                                  (deliver task-done true))}
          em           (helpers/collecting-emitter)
          pool         (-> (worker-pool/create-pool
                            sys
                            tasks
                            em
                            (assoc (helpers/pool-config)
                                   :task-identifiers [task-id]
                                   :graceful-shutdown-timeout-ms 2000))
                           worker-pool/start!)]
      (job-manager/add-job sys task-id {:x 1})
      (deref task-started 3000 nil)
      (worker-pool/stop! pool)
      (is (realized? task-done) "task completed before stop! returned")
      (let [stop-evt (->> (monitoring/events em)
                          (filter #(= :pool/stop (:type %)))
                          first)]
        (is (false? (get-in stop-evt [:data :forced?]))
            "pool stopped gracefully (not forced)")))))

(deftest graceful-shutdown-waits-for-in-flight-job-test
  ;; [surface.WorkerExecution] - stop! must wait for active workers before
  ;; returning, up to the graceful-shutdown-timeout-ms.
  (testing "stop! waits for an in-flight task to complete before returning"
    (let [sys          (helpers/real-system)
          task-id      (helpers/unique-task-id)
          task-started (promise)
          task-done    (promise)
          tasks        {task-id (fn [_]
                                  (deliver task-started true)
                                  (Thread/sleep 400)
                                  (deliver task-done true))}
          em           (helpers/noop-emitter)
          pool         (-> (worker-pool/create-pool sys
                                                    tasks
                                                    em
                                                    (assoc (helpers/pool-config)
                                                           :task-identifiers
                                                           [task-id]))
                           worker-pool/start!)]
      (job-manager/add-job sys task-id {:x 1})
      (deref task-started 3000 nil)
      (worker-pool/stop! pool 5000)
      (is (realized? task-done) "task completed before stop! returned"))))
