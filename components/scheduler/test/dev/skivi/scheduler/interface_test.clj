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

(ns dev.skivi.scheduler.interface-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.database.interface]
            [dev.skivi.scheduler.interface :as scheduler]
            [malli.instrument :as mi])
  (:import [java.time Instant]))

(use-fixtures :once (fn [f] (mi/instrument!) (f) (mi/unstrument!)))

;;;; ── cron-next ─────────────────────────────────────────────────────────────

(deftest cron-next-top-of-hour-test
  (testing "returns top of the next hour for an hourly schedule"
    (let [base (Instant/parse "2026-01-01T12:30:00Z")
          next (scheduler/cron-next "0 * * * *" base "UTC")]
      (is (= (Instant/parse "2026-01-01T13:00:00Z") next)))))

(deftest cron-next-every-minute-test
  (testing "returns one minute later for a * * * * * schedule"
    (let [base (Instant/parse "2026-06-15T08:04:10Z")
          next (scheduler/cron-next "* * * * *" base "UTC")]
      (is (= (Instant/parse "2026-06-15T08:05:00Z") next)))))

(deftest cron-next-daily-test
  (testing "returns next occurrence of a daily schedule"
    (let [base (Instant/parse "2026-03-10T03:00:00Z")
          next (scheduler/cron-next "0 2 * * *" base "UTC")]
      (is (= (Instant/parse "2026-03-11T02:00:00Z") next)))))

(deftest cron-next-returns-instant-test
  (testing "result is a java.time.Instant"
    (let [base (Instant/parse "2026-01-01T00:00:00Z")
          next (scheduler/cron-next "* * * * *" base "UTC")]
      (is (instance? Instant next)))))

(deftest cron-next-timezone-shifts-result-test
  (testing "timezone shifts the computed occurrence"
    (let [base     (Instant/parse "2026-06-01T05:00:00Z")
          ;; 0 6 * * * fires at 06:00 America/New_York = 10:00 UTC in
          ;; summer
          next-utc (scheduler/cron-next "0 6 * * *" base "America/New_York")]
      ;; base is 01:00 NYT; next occurrence is 06:00 NYT = 10:00 UTC
      (is (= (Instant/parse "2026-06-01T10:00:00Z") next-utc)))))

(deftest cron-next-future-from-past-base-test
  (testing "returns a future instant even when base is far in the past"
    (let [base (Instant/parse "2020-01-01T00:00:00Z")
          next (scheduler/cron-next "0 0 1 1 *" base "UTC")]
      ;; 0 0 1 1 * fires at midnight on 1 January each year
      (is (= (Instant/parse "2021-01-01T00:00:00Z") next)))))

;;;; ── create-scheduler structure ────────────────────────────────────────────

(deftest create-scheduler-returns-map-test
  (testing "create-scheduler returns a map with required keys"
    (let [sched (scheduler/create-scheduler nil [] (constantly nil))]
      (is (map? sched))
      (is (contains? sched :config))
      (is (contains? sched :crontabs))
      (is (contains? sched :emitter))
      (is (contains? sched :job-system))
      (is (contains? sched :state)))))

(deftest create-scheduler-not-running-by-default-test
  (testing "scheduler is not running after creation"
    (let [sched (scheduler/create-scheduler nil [] (constantly nil))]
      (is (not (scheduler/running? sched))))))

(deftest create-scheduler-initial-stats-test
  (testing "stats start at zero"
    (let [sched (scheduler/create-scheduler nil [] (constantly nil))
          s     (scheduler/stats sched)]
      (is (= 0 (:fired s)))
      (is (= 0 (:errors s))))))

(deftest create-scheduler-default-config-test
  (testing "default poll-interval-ms is 10000"
    (let [sched (scheduler/create-scheduler nil [] (constantly nil))]
      (is (= 10000 (get-in sched [:config :poll-interval-ms])))))
  (testing "default timezone is UTC"
    (let [sched (scheduler/create-scheduler nil [] (constantly nil))]
      (is (= "UTC" (get-in sched [:config :timezone]))))))

(deftest create-scheduler-config-overrides-test
  (testing "supplied config values are merged with defaults"
    (let [sched (scheduler/create-scheduler nil
                                            []
                                            (constantly nil)
                                            {:poll-interval-ms 500
                                             :timezone "Europe/London"})]
      (is (= 500 (get-in sched [:config :poll-interval-ms])))
      (is (= "Europe/London" (get-in sched [:config :timezone]))))))

(deftest create-scheduler-stores-crontabs-test
  (testing "crontabs are stored verbatim on the scheduler"
    (let [entries [{:identifier "a"
                    :schedule   "* * * * *"}
                   {:identifier "b"
                    :schedule   "0 0 * * *"}]
          sched   (scheduler/create-scheduler nil entries (constantly nil))]
      (is (= entries (:crontabs sched))))))

(deftest create-scheduler-empty-crontabs-test
  (testing "empty crontab list is accepted"
    (let [sched (scheduler/create-scheduler nil [] (constantly nil))]
      (is (= [] (:crontabs sched))))))

;;;; ── fire-due-jobs! on empty scheduler (no DB) ────────────────────────────

(deftest fire-due-jobs-empty-crontabs-test
  (testing "fire-due-jobs! on empty crontab list returns zero counts"
    (let [sched  (scheduler/create-scheduler nil [] (constantly nil))
          result (scheduler/fire-due-jobs! sched)]
      (is (= 0 (:fired result)))
      (is (= 0 (:skipped result))))))

;;;; ── run-loop! per-entry exception isolation
;;;; ────────────────────────────────

(deftest fire-due-jobs-per-entry-isolation-test
  (testing
    "exception in one entry does not prevent subsequent entries from being evaluated"
    (let [attempted  (atom [])
          far-future (Instant/parse "2099-12-31T23:59:00Z")
          sched      (scheduler/create-scheduler nil
                                                 [{:identifier "entry-a"
                                                   :schedule   "* * * * *"
                                                   :spec       {}}
                                                  {:identifier "entry-b"
                                                   :schedule   "* * * * *"
                                                   :spec       {}}
                                                  {:identifier "entry-c"
                                                   :schedule   "* * * * *"
                                                   :spec       {}}]
                                                 (constantly nil))]
      (with-redefs [dev.skivi.database.interface/load-crontab-state
                    (fn [_ id]
                      (swap! attempted conj id)
                      (if (= id "entry-a")
                        (throw (ex-info "simulated db error" {}))
                        {:known-since    far-future
                         :last-execution nil}))]
        (scheduler/fire-due-jobs! sched))
      (is (= ["entry-a" "entry-b" "entry-c"] @attempted)
          "all entries are attempted even when entry-a throws")
      (is (= 1 (:errors (scheduler/stats sched)))
          "only entry-a's error is counted"))))
