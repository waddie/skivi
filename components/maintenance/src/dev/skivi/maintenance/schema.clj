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

(ns dev.skivi.maintenance.schema "Malli schemas for the maintenance component.")

(def MaintenanceConfig
  "Tuning parameters for create-scheduler."
  [:map
   [:maintenance-interval-ms {:optional true} pos-int?]
   [:schedule {:optional true} :string]
   [:tasks {:optional true} [:vector :keyword]]
   [:retention-periods {:optional true} [:map-of :keyword :string]]
   [:timezone {:optional true} :string]])

(def MaintenanceStats
  "Operational metrics snapshot returned by stats."
  [:map
   [:maintenance-runs nat-int?]
   [:gc-runs nat-int?]
   [:errors nat-int?]])

(def MaintenanceScheduler
  "A maintenance scheduler handle returned by create-scheduler."
  [:map
   [:config MaintenanceConfig]
   [:job-system :any]
   [:state :any]])
