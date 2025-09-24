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

(ns dev.skivi.scheduler.core
  "Cron scheduler: evaluates crontab entries and enqueues jobs when due.

  Cron state (last_execution) is persisted to known_crontabs so that
  the scheduler survives process restarts without double-firing. Jobs are
  enqueued with job_key_mode = unsafe_dedupe, so multiple scheduler instances
  running in parallel will not produce duplicate queue entries.

  The background polling thread evaluates all registered entries on each tick
  and fires any whose next_run_at <= now. The poll interval is coarse
  (default 10 s) because cron granularity is one minute.

  Relies on the ReadableColumn protocol extensions defined in database.core
  to return java.time.Instant values for TIMESTAMPTZ columns."
  (:require [dev.skivi.database.interface :as db]
            [dev.skivi.job-manager.interface :as job-manager]
            [dev.skivi.monitoring.interface :as monitoring]
            [dev.skivi.scheduler.schema :as schema])
  (:import [com.cronutils.model CronType]
           [com.cronutils.model.definition CronDefinitionBuilder]
           [com.cronutils.model.time ExecutionTime]
           [com.cronutils.parser CronParser]
           [java.time Instant ZoneId]))

;;; Cron parser (UNIX 5-field format: minute hour day month weekday)

(def ^:private unix-cron-def
  (CronDefinitionBuilder/instanceDefinitionFor CronType/UNIX))

(def ^:private cron-parser (CronParser. unix-cron-def))

;;; Defaults

(def ^:private defaults
  {:poll-interval-ms 10000
   :timezone         "UTC"})

;;; Cron computation

(defn cron-next
  "Returns the next Instant a cron schedule fires after base-time in timezone.
  Returns nil if the expression has no future occurrence."
  {:malli/schema [:function [:=> [:cat :string inst? :string] [:maybe inst?]]]}
  [schedule ^Instant base-time timezone]
  (let [cron      (.parse cron-parser schedule)
        zone      (ZoneId/of timezone)
        base-zdt  (.atZone base-time zone)
        exec-time (ExecutionTime/forCron cron)
        next-opt  (.nextExecution exec-time base-zdt)]
    (when (.isPresent next-opt) (.toInstant (.get next-opt)))))

;;; Database operations on skivi.known_crontabs

(defn- upsert-crontab! [pool identifier] (db/upsert-crontab! pool identifier))

(defn- load-crontab-state
  [pool identifier]
  (db/load-crontab-state pool identifier))

(defn- update-last-execution!
  [pool identifier]
  (db/update-last-execution! pool identifier))

;;; Job enqueuing helpers

(defn- spec->job-opts
  "Builds the add-job opts map from a crontab entry's spec and identifier."
  [identifier spec]
  (cond-> {:job-key      identifier
           :job-key-mode "unsafe_dedupe"}
    (some? (:priority spec)) (assoc :priority (:priority spec))
    (some? (:queue-name spec)) (assoc :queue-name (:queue-name spec))
    (some? (:max-attempts spec)) (assoc :max-attempts (:max-attempts spec))
    (some? (:flags spec)) (assoc :flags (:flags spec))
    (some? (:rate-limit-key spec)) (assoc :rate-limit-key
                                          (:rate-limit-key spec))))

;;; Firing logic

(defn- entry-timezone
  "Returns the effective timezone for a crontab entry."
  [entry scheduler-timezone]
  (get entry :timezone scheduler-timezone))

(defn- fire-crontab!
  "Checks whether entry is due and enqueues a job if so.
  Returns true when a job was enqueued, false when skipped."
  [scheduler entry]
  (let [{:keys [job-system emitter config]} scheduler
        {:keys [identifier schedule spec]} entry
        pool     (:pool job-system)
        timezone (entry-timezone entry (get config :timezone "UTC"))
        db-state (load-crontab-state pool identifier)
        base     (or (:last-execution db-state)
                     (:known-since db-state)
                     (Instant/now))
        next-run (cron-next schedule base timezone)]
    (if (and next-run (<= (.compareTo ^Instant next-run (Instant/now)) 0))
      (do (job-manager/add-job job-system
                               identifier
                               (get spec :payload {})
                               (spec->job-opts identifier spec))
          (update-last-execution! pool identifier)
          (monitoring/emit! emitter :cron/fired {:identifier identifier})
          (swap! (:state scheduler) update-in [:stats :fired] inc)
          true)
      false)))

;;; Public API

(defn cron-next-public
  "Returns the next Instant a cron schedule fires after base-time in timezone.
  Returns nil if the expression has no future occurrence."
  {:malli/schema [:function [:=> [:cat :string inst? :string] [:maybe inst?]]]}
  [schedule base-time timezone]
  (cron-next schedule base-time timezone))

(defn fire-due-jobs!
  "Evaluates all crontab entries and fires any whose next_run_at <= now.
  Returns a map with :fired (enqueued count) and :skipped (not-due count)."
  {:malli/schema [:function [:=> [:cat schema/Scheduler] schema/FiredSummary]]}
  [scheduler]
  (reduce (fn [acc entry]
            (if (try (fire-crontab! scheduler entry)
                     (catch Exception _
                       (swap! (:state scheduler) update-in [:stats :errors] inc)
                       false))
              (update acc :fired inc)
              (update acc :skipped inc)))
          {:fired   0
           :skipped 0}
          (:crontabs scheduler)))

(defn- run-loop!
  "Worker loop: evaluates all entries on each tick until the scheduler stops."
  [scheduler]
  (try (loop []
         (when (:running? @(:state scheduler))
           (try (fire-due-jobs! scheduler)
                (catch InterruptedException ex (throw ex))
                (catch Exception _
                  (swap! (:state scheduler) update-in [:stats :errors] inc)))
           (Thread/sleep (long (get-in scheduler [:config :poll-interval-ms])))
           (recur)))
       (catch InterruptedException _ (.interrupt (Thread/currentThread)))))

(defn create-scheduler
  "Creates a cron scheduler. Call start! to begin processing.
  job-system is {:pool datasource :validator validator}.
  crontabs is a vector of CrontabEntry maps.
  emitter is a monitoring/Emitter.
  config keys: :poll-interval-ms, :timezone."
  {:malli/schema [:function
                  [:=> [:cat :any [:vector schema/CrontabEntry] :any]
                   schema/Scheduler]
                  [:=>
                   [:cat :any [:vector schema/CrontabEntry] :any
                    schema/SchedulerConfig]
                   schema/Scheduler]]}
  ([job-system crontabs emitter]
   (create-scheduler job-system crontabs emitter {}))
  ([job-system crontabs emitter config]
   {:config     (merge defaults config)
    :crontabs   (vec crontabs)
    :emitter    emitter
    :job-system job-system
    :state      (atom {:running? false
                       :stats    {:errors 0
                                  :fired  0}
                       :thread   nil})}))

(defn start!
  "Upserts crontab records then starts the polling loop. Throws if already running.
  Returns scheduler."
  {:malli/schema [:function [:=> [:cat schema/Scheduler] schema/Scheduler]]}
  [scheduler]
  (let [{:keys [crontabs job-system state]} scheduler
        pool (:pool job-system)]
    (when (:running? @state) (throw (ex-info "Scheduler already started" {})))
    (doseq [{:keys [identifier]} crontabs]
      (upsert-crontab! pool identifier))
    (swap! state assoc :running? true)
    (let [thread (doto (Thread. ^Runnable #(run-loop! scheduler)
                                "skivi-scheduler")
                   (.setDaemon true)
                   (.start))]
      (swap! state assoc :thread thread)
      scheduler)))

(defn stop!
  "Signals the polling loop to stop, interrupts the thread, and joins it.
  Returns scheduler."
  {:malli/schema [:function
                  [:=> [:cat schema/Scheduler] schema/Scheduler]
                  [:=> [:cat schema/Scheduler pos-int?] schema/Scheduler]]}
  ([scheduler] (stop! scheduler 5000))
  ([scheduler timeout-ms]
   (let [{:keys [state]} scheduler]
     (swap! state assoc :running? false)
     (when-let [^Thread t (:thread @state)]
       (.interrupt t)
       (.join t (long timeout-ms)))
     scheduler)))

(defn running?
  "Returns true if the polling loop is active."
  {:malli/schema [:function [:=> [:cat schema/Scheduler] :boolean]]}
  [scheduler]
  (boolean (:running? @(:state scheduler))))

(defn stats
  "Returns a snapshot of scheduler operational metrics.
  Keys: :fired, :errors."
  {:malli/schema [:function
                  [:=> [:cat schema/Scheduler] schema/SchedulerStats]]}
  [scheduler]
  (:stats @(:state scheduler)))
