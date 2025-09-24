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

(ns dev.skivi.scheduler.schema "Malli schemas for the scheduler component.")

(def CrontabSpec
  "Per-entry job creation overrides applied when a cron entry fires.
  All keys are optional; defaults follow the spec config block."
  [:map
   [:payload {:optional true} :map]
   [:priority {:optional true} :int]
   [:queue-name {:optional true} [:maybe :string]]
   [:max-attempts {:optional true} pos-int?]
   [:flags {:optional true} [:vector :string]]
   [:rate-limit-key {:optional true} :string]])

(def CrontabEntry
  "A single cron schedule entry supplied to create-scheduler.

  :identifier - unique name; also used as the task-identifier and job-key.
  :schedule   - standard 5-field UNIX cron expression (minute hour day month weekday).
  :spec       - optional job creation overrides (payload, priority, etc.).
  :timezone   - IANA timezone string; overrides the scheduler-level default."
  [:map
   [:identifier :string]
   [:schedule :string]
   [:spec {:optional true} CrontabSpec]
   [:timezone {:optional true} :string]])

(def SchedulerConfig
  "Tuning parameters for create-scheduler.

  :poll-interval-ms - milliseconds between cron evaluation passes (default 10000).
  :timezone         - default IANA timezone for all entries (default \"UTC\")."
  [:map
   [:poll-interval-ms {:optional true} pos-int?]
   [:timezone {:optional true} :string]])

(def SchedulerStats
  "Operational metrics snapshot returned by stats."
  [:map
   [:fired nat-int?]
   [:errors nat-int?]])

(def Scheduler
  "A cron scheduler handle returned by create-scheduler."
  [:map
   [:config SchedulerConfig]
   [:crontabs [:vector CrontabEntry]]
   [:emitter :any]
   [:job-system :any]
   [:state :any]])

(def FiredSummary
  "Result map returned by fire-due-jobs!."
  [:map
   [:fired nat-int?]
   [:skipped nat-int?]])
