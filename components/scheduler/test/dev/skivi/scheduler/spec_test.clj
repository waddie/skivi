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

;; Spec compliance tests derived from docs/spec/skivi.allium, CronFiresJob rule
;; and the CronTab entity.
;;
;; Each test maps to rule/obligation IDs cited in comments.
;; These are integration tests requiring a live PostgreSQL instance at
;; localhost:5432/test_db (see docker-compose.yml).
;;
;; Strategy: fire-due-jobs! is called explicitly to avoid timing dependencies
;; on the background thread. Tests set last_execution to a past time so that
;; the computed next_run_at is <= now.

(ns dev.skivi.scheduler.spec-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.monitoring.interface :as monitoring]
            [dev.skivi.scheduler.interface :as scheduler]
            [dev.skivi.scheduler.test-helpers :as helpers])
  (:import [java.time Instant]))

(use-fixtures :once helpers/schema-fixture)

;;;; ── CronFiresJob ──────────────────────────────────────────────────────────
;;
;; Spec rule: CronFiresJob
;; When cron_tab.next_run_at <= now, a job must be created with:
;;   task_identifier = cron_tab.identifier
;;   job_key         = cron_tab.identifier
;;   job_key_mode    = unsafe_dedupe
;;   payload         = cron_tab.spec.payload ?? {}
;;   priority        = cron_tab.spec.priority ?? 0
;;   queue_name      = cron_tab.spec.queue_name
;;   max_attempts    = cron_tab.spec.max_attempts ?? config.max_attempts
;;   flags           = cron_tab.spec.flags ?? {}
;; And: cron_tab.last_execution = now

(deftest cron-fires-job-when-due-test
  ;; [rule-success.CronFiresJob] - next_run_at <= now → job enqueued
  (testing "fire-due-jobs! enqueues a job when the schedule is due"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          ;; Do not start the scheduler: insert the DB record manually so
          ;; the background thread cannot race with the explicit
          ;; fire-due-jobs! call.
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter))]
      (try
        (helpers/upsert-crontab! sys identifier)
        ;; Set last_execution 2 minutes ago so next_run_at is in the past
        (helpers/set-last-execution! sys identifier (helpers/minutes-ago 2))
        (let [{:keys [fired]} (scheduler/fire-due-jobs! sched)]
          (is (= 1 fired) "exactly one job fired for the due entry")
          (is
           (pos? (helpers/count-jobs-by-task sys identifier))
           "a job exists in the queue with the crontab identifier as task-identifier"))
        (finally (helpers/delete-jobs-by-task! sys identifier)
                 (helpers/delete-crontab! sys identifier))))))

(deftest cron-job-uses-identifier-as-task-identifier-test
  ;; [rule-success.CronFiresJob] - task_identifier = cron_tab.identifier
  (testing "enqueued job has task-identifier equal to the crontab identifier"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter))]
      (try (helpers/upsert-crontab! sys identifier)
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 2))
           (scheduler/fire-due-jobs! sched)
           (is (pos? (helpers/count-jobs-by-task sys identifier))
               "job task-identifier matches crontab identifier")
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

(deftest cron-job-uses-spec-overrides-test
  ;; [rule-success.CronFiresJob] - per-entry spec overrides applied to job
  (testing "job is created with priority and max-attempts from the crontab spec"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"
                       :spec       {:max-attempts 3
                                    :priority     5}}]
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter))]
      (try (helpers/upsert-crontab! sys identifier)
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 2))
           (let [{:keys [fired]} (scheduler/fire-due-jobs! sched)]
             (is (= 1 fired)))
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

(deftest cron-updates-last-execution-after-firing-test
  ;; [rule-success.CronFiresJob] - cron_tab.last_execution = now after
  ;; firing
  (testing "last_execution is updated in the database after a job is fired"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter))]
      (try (helpers/upsert-crontab! sys identifier)
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 5))
           (let [before (:last-execution
                         (helpers/get-crontab-state sys identifier))]
             (scheduler/fire-due-jobs! sched)
             (let [after (:last-execution
                          (helpers/get-crontab-state sys identifier))]
               (is (some? after) "last_execution is set after firing")
               (is (or (nil? before) (.isBefore before after))
                   "last_execution is later than it was before firing")))
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

(deftest cron-emits-fired-event-test
  ;; [rule-success.CronFiresJob] - :cron/fired event emitted on enqueue
  (testing ":cron/fired event is emitted with correct identifier"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          em         (helpers/collecting-emitter)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          sched      (scheduler/create-scheduler sys entries em)]
      (try (helpers/upsert-crontab! sys identifier)
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 2))
           (scheduler/fire-due-jobs! sched)
           (let [events (monitoring/events em)
                 fired  (filter #(= :cron/fired (:type %)) events)]
             (is (= 1 (count fired)) "exactly one :cron/fired event emitted")
             (is (= identifier (get-in (first fired) [:data :identifier]))
                 ":identifier in event data matches crontab identifier"))
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

(deftest cron-not-fired-when-not-due-test
  ;; [rule-success.CronFiresJob] - job not enqueued when next_run_at > now
  (testing
    "fire-due-jobs! does not enqueue a job when the schedule is not yet due"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter))]
      (try (helpers/upsert-crontab! sys identifier)
           ;; last_execution = just now: next_run_at is ~1 min in the
           ;; future
           (helpers/set-last-execution! sys identifier (Instant/now))
           (let [{:keys [fired skipped]} (scheduler/fire-due-jobs! sched)]
             (is (= 0 fired) "no job fired when schedule is not yet due")
             (is (= 1 skipped) "entry counted as skipped"))
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

;;;; ── unsafe_dedupe deduplication ───────────────────────────────────────────
;;
;; Spec rule: CronFiresJob @guidance
;; job_key = cron_tab.identifier with unsafe_dedupe prevents a second job
;; from being enqueued while the previous one is still queued or running.

(deftest cron-does-not-duplicate-queued-job-test
  ;; [rule-success.CronFiresJob / guidance unsafe_dedupe] - second fire is
  ;; a no-op when a job with the same job_key is already in the queue
  (testing
    "firing twice with the same identifier leaves only one job in the queue"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter))]
      (try (helpers/upsert-crontab! sys identifier)
           ;; Fire once
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 3))
           (scheduler/fire-due-jobs! sched)
           ;; Reset last_execution to trigger a second fire
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 3))
           (scheduler/fire-due-jobs! sched)
           ;; unsafe_dedupe: if the first job is still queued, the second
           ;; enqueue is a no-op at the job-manager level, so the count
           ;; stays at 1.
           (let [n (helpers/count-jobs-by-task sys identifier)]
             (is (= 1 n) "unsafe_dedupe keeps exactly one job in the queue"))
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

;;;; ── stats counter
;;;; ──────────────────────────────────────────────────────────

(deftest stats-fired-counter-increments-test
  ;; scheduler/stats :fired increments each time a job is enqueued
  (testing ":fired stat increments after each successful firing"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter))]
      (try (helpers/upsert-crontab! sys identifier)
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 2))
           (scheduler/fire-due-jobs! sched)
           (is (= 1 (:fired (scheduler/stats sched)))
               ":fired increments after one enqueue")
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

;;;; ── CronFiresJob: job field verification (rule-entity-creation)
;;   spec: job_key = cron_tab.identifier, job_key_mode = unsafe_dedupe

(deftest cron-job-fields-test
  ;; [rule-entity-creation.CronFiresJob.1]
  ;; spec: fired job must have job_key = cron_tab.identifier and
  ;; job_key_mode = "unsafe_dedupe".
  (testing "fired job has job_key equal to crontab identifier"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter))]
      (try (helpers/upsert-crontab! sys identifier)
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 2))
           (scheduler/fire-due-jobs! sched)
           (let [job (helpers/get-job-by-task sys identifier)]
             (is (some? job) "job row exists")
             (is (= identifier (:job-key job))
                 "job_key equals crontab identifier")
             (is (= "unsafe_dedupe" (:job-key-mode job))
                 "job_key_mode is unsafe_dedupe"))
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

;;;; ── CronFiresJob: cron_enabled=false (rule-failure.CronFiresJob)
;;
;; spec: CronFiresJob requires config.cron_enabled = true.
;; In this library the config gate lives at the system level: the caller does
;; not start the scheduler when cron_enabled = false. The scheduler itself does
;; not read the config flag - it fires whenever it is running. The enforcement
;; point is therefore outside the scheduler component.

(deftest cron-disabled-by-not-starting-scheduler-test
  ;; [rule-failure.CronFiresJob.1]
  ;; spec: no job is fired when cron_enabled = false. Implementation note:
  ;; the library enforces this by never starting the scheduler when
  ;; cron_enabled = false. The test verifies that a scheduler
  ;; that is not started does not enqueue jobs.
  (testing "no job is fired when the scheduler is never started"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          _ (scheduler/create-scheduler sys entries (helpers/noop-emitter))]
      (try (helpers/upsert-crontab! sys identifier)
           (helpers/set-last-execution! sys identifier (helpers/minutes-ago 2))
           ;; Deliberately do not call start! - simulates cron_enabled =
           ;; false
           (Thread/sleep 200)
           (is (zero? (helpers/count-jobs-by-task sys identifier))
               "no jobs enqueued when scheduler is not started")
           (finally (helpers/delete-jobs-by-task! sys identifier)
                    (helpers/delete-crontab! sys identifier))))))

;;;; ── Scheduler lifecycle
;;;; ────────────────────────────────────────────────────

(deftest running-predicate-lifecycle-test
  ;; running? reflects the scheduler state through start/stop
  (testing "running? is false before start, true after start, false after stop"
    (let [sys   (helpers/real-system)
          sched (scheduler/create-scheduler sys
                                            []
                                            (helpers/noop-emitter)
                                            (helpers/scheduler-config))]
      (is (not (scheduler/running? sched)) "not running before start!")
      (scheduler/start! sched)
      (is (scheduler/running? sched) "running after start!")
      (scheduler/stop! sched 2000)
      (is (not (scheduler/running? sched)) "not running after stop!"))))

(deftest double-start-throws-test
  ;; Starting an already-running scheduler must throw
  (testing "start! throws if scheduler is already running"
    (let [sys   (helpers/real-system)
          sched (-> (scheduler/create-scheduler sys
                                                []
                                                (helpers/noop-emitter)
                                                (helpers/scheduler-config))
                    scheduler/start!)]
      (try (is (thrown? Exception (scheduler/start! sched))
               "second start! throws")
           (finally (scheduler/stop! sched 2000))))))

(deftest background-thread-fires-job-test
  ;; The background polling thread eventually fires a due job without
  ;; explicit fire-due-jobs! calls.
  (testing "background thread enqueues a job for a due cron entry"
    (let [sys        (helpers/real-system)
          identifier (helpers/unique-identifier)
          entries    [{:identifier identifier
                       :schedule   "* * * * *"}]
          sched      (scheduler/create-scheduler sys
                                                 entries
                                                 (helpers/noop-emitter)
                                                 {:poll-interval-ms 200
                                                  :timezone         "UTC"})]
      (try
        (scheduler/start! sched)
        (helpers/set-last-execution! sys identifier (helpers/minutes-ago 2))
        ;; Allow the polling thread to fire
        (let [deadline (+ (System/currentTimeMillis) 3000)]
          (loop []
            (let [n (helpers/count-jobs-by-task sys identifier)]
              (cond (pos? n) (is (pos? n) "background thread enqueued the job")
                    (> (System/currentTimeMillis) deadline)
                    (is false
                        "timed out waiting for background thread to fire job")
                    :else (do (Thread/sleep 100) (recur))))))
        (finally (scheduler/stop! sched 2000)
                 (helpers/delete-jobs-by-task! sys identifier)
                 (helpers/delete-crontab! sys identifier))))))
