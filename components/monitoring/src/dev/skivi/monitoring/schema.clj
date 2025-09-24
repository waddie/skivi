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

(ns dev.skivi.monitoring.schema "Malli schemas for the monitoring component.")

(def EmitterConfig
  "Configuration for the event emitter subsystem.
  Mirrors the :events key in the full MonitoringConfig."
  [:map
   [:enabled boolean?]
   [:buffer-size pos-int?]])

(def Event
  "A single emitted event. :type is the event keyword, :data is the payload map,
  :emitted-at is a java.time.Instant."
  [:map
   [:type :keyword]
   [:data :map]
   [:emitted-at inst?]])

(def EmitterStats
  "Operational metrics snapshot for an event emitter."
  [:map
   [:emitted :int]
   [:dropped :int]])

(def Emitter
  "An event emitter handle returned by create-emitter or noop-emitter.
  :enabled? controls whether emit! routes events; false makes all operations no-ops.
  :buffer, :handlers, and :stats are atoms."
  [:map
   [:enabled? boolean?]
   [:config [:maybe EmitterConfig]]
   [:buffer :any]
   [:handlers :any]
   [:stats :any]])
