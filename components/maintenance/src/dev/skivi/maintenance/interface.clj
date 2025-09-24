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

(ns dev.skivi.maintenance.interface
  "Maintenance scheduler: reset-locked-jobs and refill-rate-limits run on a short
  interval; GC tasks run on a configurable cron schedule."
  (:require [dev.skivi.maintenance.core :as core]
            [dev.skivi.maintenance.schema :as schema]))

(defn create-scheduler
  "Creates a maintenance scheduler. Call start! to begin processing.
  job-system is {:pool datasource :validator validator}.
  config keys: :maintenance-interval-ms, :schedule, :tasks, :timezone, :retention-periods."
  {:malli/schema [:function
                  [:=> [:cat :any] schema/MaintenanceScheduler]
                  [:=> [:cat :any schema/MaintenanceConfig]
                   schema/MaintenanceScheduler]]}
  ([job-system] (core/create-scheduler job-system))
  ([job-system config] (core/create-scheduler job-system config)))

(defn start!
  "Starts the maintenance loop. Throws if already running. Returns scheduler."
  {:malli/schema [:function
                  [:=> [:cat schema/MaintenanceScheduler]
                   schema/MaintenanceScheduler]]}
  [sched]
  (core/start! sched))

(defn stop!
  "Signals the loop to stop and waits up to timeout-ms. Returns scheduler."
  {:malli/schema [:function
                  [:=> [:cat schema/MaintenanceScheduler]
                   schema/MaintenanceScheduler]
                  [:=> [:cat schema/MaintenanceScheduler pos-int?]
                   schema/MaintenanceScheduler]]}
  ([sched] (core/stop! sched))
  ([sched timeout-ms] (core/stop! sched timeout-ms)))

(defn running?
  "Returns true if the maintenance loop is active."
  {:malli/schema [:function [:=> [:cat schema/MaintenanceScheduler] :boolean]]}
  [sched]
  (core/running? sched))

(defn stats
  "Returns a snapshot of maintenance scheduler operational metrics.
  Keys: :maintenance-runs, :gc-runs, :errors."
  {:malli/schema
   [:function [:=> [:cat schema/MaintenanceScheduler] schema/MaintenanceStats]]}
  [sched]
  (core/stats sched))
