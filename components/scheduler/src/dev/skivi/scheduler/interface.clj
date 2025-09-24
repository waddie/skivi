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

(ns dev.skivi.scheduler.interface
  "Cron scheduler that enqueues jobs via job-manager when schedules are due.

  Each cron entry has an identifier (used as both task-identifier and job-key),
  a standard UNIX 5-field cron expression, and optional per-entry job creation
  overrides. Cron state (last_execution) is persisted to skivi.known_crontabs
  so the scheduler survives restarts without double-firing.

  Jobs are enqueued with job_key_mode = unsafe_dedupe: if the previous run's
  job is still queued or in flight, the new enqueue is a no-op.

  Typical usage:

    (def crontabs
      [{:identifier \"daily-report\"
        :schedule   \"0 2 * * *\"
        :spec       {:queue-name \"maintenance\" :max-attempts 3}}
       {:identifier \"hourly-cleanup\"
        :schedule   \"0 * * * *\"}])

    (def scheduler
      (-> (scheduler/create-scheduler sys crontabs emitter {:timezone \"Europe/London\"})
          scheduler/start!))

    ;; On application shutdown
    (scheduler/stop! scheduler 5000)

  Standard events emitted:
    :cron/fired - a cron entry fired and a job was enqueued; :identifier in data"
  (:require [dev.skivi.scheduler.core :as core]
            [dev.skivi.scheduler.schema :as schema]))

(defn cron-next
  "Returns the next java.time.Instant a cron schedule fires after base-time in timezone.
  schedule is a standard UNIX 5-field cron expression.
  base-time is a java.time.Instant from which to compute the next occurrence.
  timezone is an IANA timezone string (e.g. \"UTC\", \"America/New_York\").
  Returns nil if the expression has no future occurrence."
  {:malli/schema [:function [:=> [:cat :string inst? :string] [:maybe inst?]]]}
  [schedule base-time timezone]
  (core/cron-next schedule base-time timezone))

(defn create-scheduler
  "Creates a cron scheduler. Call start! to begin processing.
  job-system is {:pool datasource :validator validator}.
  crontabs is a vector of CrontabEntry maps (see schema/CrontabEntry).
  emitter is a monitoring/Emitter.
  config keys: :poll-interval-ms (default 10000), :timezone (default \"UTC\")."
  {:malli/schema [:function
                  [:=> [:cat :any [:vector schema/CrontabEntry] :any]
                   schema/Scheduler]
                  [:=>
                   [:cat :any [:vector schema/CrontabEntry] :any
                    schema/SchedulerConfig]
                   schema/Scheduler]]}
  ([job-system crontabs emitter]
   (core/create-scheduler job-system crontabs emitter))
  ([job-system crontabs emitter config]
   (core/create-scheduler job-system crontabs emitter config)))

(defn start!
  "Upserts crontab records in the database then starts the background polling loop.
  Throws if already running. Returns scheduler."
  {:malli/schema [:function [:=> [:cat schema/Scheduler] schema/Scheduler]]}
  [scheduler]
  (core/start! scheduler))

(defn stop!
  "Signals the polling loop to stop, interrupts the thread, and joins it up to
  timeout-ms milliseconds. Returns scheduler."
  {:malli/schema [:function
                  [:=> [:cat schema/Scheduler] schema/Scheduler]
                  [:=> [:cat schema/Scheduler pos-int?] schema/Scheduler]]}
  ([scheduler] (core/stop! scheduler))
  ([scheduler timeout-ms] (core/stop! scheduler timeout-ms)))

(defn running?
  "Returns true if the polling loop is active."
  {:malli/schema [:function [:=> [:cat schema/Scheduler] :boolean]]}
  [scheduler]
  (core/running? scheduler))

(defn stats
  "Returns a snapshot of scheduler operational metrics.
  Keys: :fired (total jobs enqueued), :errors (evaluation errors caught)."
  {:malli/schema [:function
                  [:=> [:cat schema/Scheduler] schema/SchedulerStats]]}
  [scheduler]
  (core/stats scheduler))

(defn fire-due-jobs!
  "Evaluates all crontab entries and enqueues jobs for any whose schedule is due.
  Returns a map with :fired (count enqueued) and :skipped (count not yet due).
  Works on both started and stopped schedulers; does not require the background
  thread to be running. Useful for testing and for explicit one-shot evaluation."
  {:malli/schema [:function [:=> [:cat schema/Scheduler] schema/FiredSummary]]}
  [scheduler]
  (core/fire-due-jobs! scheduler))
