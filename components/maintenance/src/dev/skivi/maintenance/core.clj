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

(ns dev.skivi.maintenance.core
  "Background maintenance scheduler.

  Runs two classes of work on separate cadences:
    Short interval (maintenance-interval-ms, default 60 s):
      reset-locked-jobs — frees jobs orphaned by crashed workers
      refill-rate-limits — refills token buckets whose window has expired

    Cron schedule (schedule, default '0 3 * * *'):
      GC tasks listed in :tasks config (gc-task-identifiers, gc-job-queues,
      gc-job-history).

  The GC schedule is evaluated on each maintenance tick; the first tick
  after a scheduled time triggers the run. Last-run time is tracked in memory
  and resets to nil on restart (causing GC to wait for the next scheduled slot)."
  (:require [dev.skivi.job-manager.interface :as job-manager]
            [dev.skivi.maintenance.schema :as schema]
            [dev.skivi.scheduler.interface :as scheduler])
  (:import [java.time Instant]))

(def ^:private defaults
  {:maintenance-interval-ms 60000
   :schedule "0 3 * * *"
   :tasks    [:gc-task-identifiers :gc-job-queues :gc-job-history]
   :timezone "UTC"})

(defn- run-gc-task!
  [job-system task retention-periods]
  (case task
    :gc-task-identifiers (job-manager/gc-task-identifiers
                          job-system
                          {:keep-since
                           (get retention-periods :task-identifiers "7 days")})
    :gc-job-queues (job-manager/gc-job-queues job-system)
    :gc-job-history (job-manager/gc-job-history job-system)
    nil))

(defn- run-maintenance-tick!
  "Runs reset and refill; checks GC schedule. Returns updated last-gc-time."
  [sched last-gc-time]
  (let [{:keys [job-system config state]} sched
        {:keys [schedule tasks timezone retention-periods]} config]
    (try (job-manager/reset-locked-jobs job-system)
         (catch Exception _ (swap! state update-in [:stats :errors] inc)))
    (try (job-manager/refill-rate-limits job-system)
         (catch Exception _ (swap! state update-in [:stats :errors] inc)))
    (swap! state update-in [:stats :maintenance-runs] inc)
    (let [now     (Instant/now)
          base    (or last-gc-time now)
          next-gc (scheduler/cron-next schedule base timezone)]
      (if (and next-gc (<= (.compareTo ^Instant next-gc now) 0))
        (do (try (doseq [task tasks]
                   (run-gc-task! job-system task (or retention-periods {})))
                 (swap! state update-in [:stats :gc-runs] inc)
                 (catch Exception _
                   (swap! state update-in [:stats :errors] inc)))
            now)
        last-gc-time))))

(defn- run-loop!
  [sched]
  (try (loop [last-gc-time nil]
         (when (:running? @(:state sched))
           (let [new-last-gc (run-maintenance-tick! sched last-gc-time)]
             (Thread/sleep (long (get-in sched
                                         [:config :maintenance-interval-ms])))
             (recur new-last-gc))))
       (catch InterruptedException _ (.interrupt (Thread/currentThread)))))

(defn create-scheduler
  "Creates a maintenance scheduler. Call start! to begin processing.
  job-system is {:pool datasource :validator validator}.
  config keys: :maintenance-interval-ms, :schedule, :tasks, :timezone, :retention-periods."
  {:malli/schema [:function
                  [:=> [:cat :any] schema/MaintenanceScheduler]
                  [:=> [:cat :any schema/MaintenanceConfig]
                   schema/MaintenanceScheduler]]}
  ([job-system] (create-scheduler job-system {}))
  ([job-system config]
   {:config     (merge defaults config)
    :job-system job-system
    :state      (atom {:running? false
                       :stats    {:errors  0
                                  :gc-runs 0
                                  :maintenance-runs 0}
                       :thread   nil})}))

(defn start!
  "Starts the maintenance loop. Throws if already running. Returns scheduler."
  {:malli/schema [:function
                  [:=> [:cat schema/MaintenanceScheduler]
                   schema/MaintenanceScheduler]]}
  [sched]
  (when (:running? @(:state sched))
    (throw (ex-info "Maintenance scheduler already started" {})))
  (swap! (:state sched) assoc :running? true)
  (let [thread (doto (Thread. ^Runnable #(run-loop! sched) "skivi-maintenance")
                 (.setDaemon true)
                 (.start))]
    (swap! (:state sched) assoc :thread thread)
    sched))

(defn stop!
  "Signals the loop to stop, interrupts the thread, and joins it. Returns scheduler."
  {:malli/schema [:function
                  [:=> [:cat schema/MaintenanceScheduler]
                   schema/MaintenanceScheduler]
                  [:=> [:cat schema/MaintenanceScheduler pos-int?]
                   schema/MaintenanceScheduler]]}
  ([sched] (stop! sched 5000))
  ([sched timeout-ms]
   (swap! (:state sched) assoc :running? false)
   (when-let [^Thread t (:thread @(:state sched))]
     (.interrupt t)
     (.join t (long timeout-ms)))
   sched))

(defn running?
  "Returns true if the maintenance loop is active."
  {:malli/schema [:function [:=> [:cat schema/MaintenanceScheduler] :boolean]]}
  [sched]
  (boolean (:running? @(:state sched))))

(defn stats
  "Returns a snapshot of maintenance scheduler operational metrics.
  Keys: :maintenance-runs, :gc-runs, :errors."
  {:malli/schema
   [:function [:=> [:cat schema/MaintenanceScheduler] schema/MaintenanceStats]]}
  [sched]
  (:stats @(:state sched)))
