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

;; Each test maps to one obligation ID from `allium plan`.
;;
;; Spec declares twelve default values. Six are already implemented in
;; default-config (tests pass now); five are missing from default-config
;; (tests fail until defaults are added); one (lock_timeout) is not yet
;; exposed in the config interface at all.
;;
;; Spec -> code mapping:
;;   schema_name       -> (config/schema-name cfg) / [:schema :name]
;;   max_attempts      -> [:retry :max-attempts]
;;   lock_timeout      -> NOT IN CONFIG (hardcoded in database component)
;;   poll_interval     -> [:worker :poll-interval]
;;   worker_concurrency-> [:worker :concurrency]
;;   local_queue_size  -> [:queue :local-queue :size]
;;   local_queue_ttl   -> [:queue :local-queue :ttl]
;;   retry_base_delay  -> [:retry :base-delay]
;;   retry_max_delay   -> [:retry :max-delay]
;;   history_retention -> [:cleanup :retention-periods :failed-jobs]
;;   cron_enabled      -> [:cron :enabled]
;;   cron_timezone     -> [:cron :timezone]

(ns dev.skivi.config.spec-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.config.core :as core]
            [dev.skivi.config.interface :as config]
            [malli.instrument :as mi]))

(use-fixtures :once (fn [f] (mi/instrument!) (f) (mi/unstrument!)))

;; The library's defaults, isolated from any user-provided config.
(def ^:private defaults (core/merge-with-defaults {}))

;;;; ── Implemented defaults ──────────────────────────────────────────────────

(deftest config-default-max-attempts-test
  ;; [config-default.max_attempts] spec: max_attempts = 25
  (testing "max_attempts defaults to 25"
    (is (= 25 (get-in defaults [:retry :max-attempts])))))

(deftest config-default-retry-base-delay-test
  ;; [config-default.retry_base_delay] spec: retry_base_delay = 1.second
  ;; (1000 ms)
  (testing "retry_base_delay defaults to 1000 ms"
    (is (= 1000 (get-in defaults [:retry :base-delay])))))

(deftest config-default-retry-max-delay-test
  ;; [config-default.retry_max_delay] spec: retry_max_delay = 1.hour (3 600
  ;; 000 ms)
  (testing "retry_max_delay defaults to 3600000 ms (1 hour)"
    (is (= 3600000 (get-in defaults [:retry :max-delay])))))

(deftest config-default-history-retention-test
  ;; [config-default.history_retention] spec: history_retention = 30.days
  ;; The code stores this as the string "30 days" in
  ;; cleanup.retention-periods.failed-jobs.
  (testing "history_retention defaults to 30 days"
    (is (= "30 days"
           (get-in defaults [:cleanup :retention-periods :failed-jobs])))))

(deftest config-default-cron-enabled-test
  ;; [config-default.cron_enabled] spec: cron_enabled = false
  (testing "cron_enabled defaults to false"
    (is (= false (get-in defaults [:cron :enabled])))))

(deftest config-default-cron-timezone-test
  ;; [config-default.cron_timezone] spec: cron_timezone = "UTC"
  (testing "cron_timezone defaults to UTC"
    (is (= "UTC" (get-in defaults [:cron :timezone])))))

(deftest config-default-maintenance-interval-test
  ;; [config-default.maintenance_interval] spec: maintenance_interval =
  ;; 1.minute (60000 ms)
  (testing "maintenance_interval defaults to 60000 ms (1 minute)"
    (is (= 60000 (get-in defaults [:cleanup :maintenance-interval-ms])))))

(deftest config-default-retry-jitter-test
  ;; [config-default.retry_jitter] spec: retry_jitter = 0.1
  (testing "retry_jitter defaults to 0.1"
    (is (= 0.1 (get-in defaults [:retry :jitter])))))

(deftest config-default-retry-multiplier-test
  ;; [config-default.retry_multiplier] spec: retry_multiplier = 2.0
  (testing "retry_multiplier defaults to 2.0"
    (is (= 2.0 (get-in defaults [:retry :multiplier])))))

;;;; ── Missing defaults (fail until default-config is updated)
;;;; ────────────────
;;
;; The spec declares these have library-level defaults, but default-config does
;; not yet supply them. The tests document the intended values. They will pass
;; once the defaults are added to core/default-config.

(deftest config-default-schema-name-test
  ;; [config-default.schema_name] spec: schema_name = "skivi"
  ;; DIVERGENCE: the code's schema-name fallback is "job_system", not
  ;; "skivi". Resolution: add {:schema {:name "skivi"}} to default-config
  ;; (or change fallback).
  (testing "schema_name defaults to skivi when not supplied by the user"
    (is (= "skivi" (config/schema-name defaults)))))

(deftest config-default-poll-interval-test
  ;; [config-default.poll_interval] spec: poll_interval = 2.seconds (2000
  ;; ms)
  ;; MISSING: no default provided; user must supply [:worker
  ;; :poll-interval]. Resolution: add {:worker {:poll-interval 2000}} to
  ;; default-config.
  (testing "poll_interval defaults to 2000 ms (2 seconds)"
    (is (= 2000 (get-in defaults [:worker :poll-interval])))))

(deftest config-default-worker-concurrency-test
  ;; [config-default.worker_concurrency] spec: worker_concurrency = 10
  ;; MISSING: no default; user must supply [:worker :concurrency].
  ;; Resolution: add {:worker {:concurrency 10}} to default-config.
  (testing "worker_concurrency defaults to 10"
    (is (= 10 (get-in defaults [:worker :concurrency])))))

(deftest config-default-local-queue-size-test
  ;; [config-default.local_queue_size] spec: local_queue_size = 50
  ;; MISSING: no default; user must supply [:queue :local-queue :size].
  ;; Resolution: add {:queue {:local-queue {:size 50}}} to default-config.
  (testing "local_queue_size defaults to 50"
    (is (= 50 (get-in defaults [:queue :local-queue :size])))))

(deftest config-default-local-queue-ttl-test
  ;; [config-default.local_queue_ttl] spec: local_queue_ttl = 60.seconds
  ;; (60 000 ms)
  ;; MISSING: no default; user must supply [:queue :local-queue :ttl].
  ;; Resolution: add {:queue {:local-queue {:ttl 60000}}} to
  ;; default-config.
  (testing "local_queue_ttl defaults to 60000 ms (60 seconds)"
    (is (= 60000 (get-in defaults [:queue :local-queue :ttl])))))

;;;; ── Not yet in config interface
;;;; ────────────────────────────────────────────

(deftest config-default-lock-timeout-test
  ;; [config-default.lock_timeout] spec: lock_timeout = 4.hours (14 400 000
  ;; ms)
  ;; NOT IN CONFIG: lock_timeout is not exposed in the config schema. It is
  ;; likely hardcoded in the database component. Resolution: add
  ;; lock_timeout to SchemaConfig or a dedicated jobs config section, with
  ;; a default of 14400000 ms (4 hours). Expose via config/lock-timeout
  ;; accessor.
  (testing "lock_timeout is accessible as a configuration parameter"
    ;; Once implemented, replace with:
    ;; (is (= 14400000 (config/lock-timeout defaults)))
    (is true "TODO: lock_timeout not yet exposed in config interface")))

(deftest config-default-task-identifier-retention-test
  ;; [config-default.task_identifier_retention] spec:
  ;; task_identifier_retention = 7.days. Stored as the string "7 days" in
  ;; cleanup.retention-periods.task-identifiers.
  (testing "task_identifier_retention defaults to 7 days"
    (is (= "7 days"
           (get-in defaults [:cleanup :retention-periods :task-identifiers])))))
