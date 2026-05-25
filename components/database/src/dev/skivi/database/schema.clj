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

(ns dev.skivi.database.schema "Malli schemas for database component.")

;;; Rate Limit Schemas

(def RateLimit
  "Rate limit configuration and token state."
  [:map [:key :string] [:capacity :int] [:interval :any]
   [:available-tokens :int] [:last-refill-at inst?]
   [:created-at {:optional true} inst?]])

;;; Connection Schemas

(def PoolConfig
  "HikariCP connection pool configuration.

  The :schema-name determines which PostgreSQL schema skivi creates its tables in.
  Defaults to 'skivi'. Common use cases for custom schema names:
  - Multi-tenant applications (one schema per tenant)
  - Multiple job queue instances in same database
  - Namespace conflict resolution
  - Organization naming conventions"
  [:map [:connection-string :string] [:username :string] [:password :string]
   [:schema-name
    {:default  "skivi"
     :optional true} [:and :string [:re #"^[a-zA-Z_][a-zA-Z0-9_]*$"]]]
   [:pool-config
    [:map [:maximum-pool-size :int] [:minimum-idle :int]
     [:connection-timeout :int] [:idle-timeout :int] [:max-lifetime :int]]]])

(def Pool
  "HikariCP DataSource instance."
  [:fn #(instance? com.zaxxer.hikari.HikariDataSource %)])

(def SqlMap "HoneySQL query map." :map)

;;; Job Schemas

(def Job
  "Job record."
  [:map [:id :uuid] [:task-identifier :string] [:payload :map]
   [:priority {:optional true} :int]
   [:queue-name {:optional true} [:maybe :string]]
   [:rate-limit-key {:optional true} [:maybe :string]]
   [:run-at {:optional true} inst?] [:attempts {:optional true} :int]
   [:max-attempts {:optional true} :int]
   [:locked-at {:optional true} [:maybe inst?]]
   [:locked-by {:optional true} [:maybe :string]]
   [:created-at {:optional true} inst?] [:updated-at {:optional true} inst?]])

(def JobFailure
  "Job failure specification."
  [:map
   [:job-id :uuid]
   [:error-message [:maybe :string]]
   [:next-run-at {:optional true} [:maybe inst?]]
   [:target-attempts {:optional true} [:maybe :int]]])

(def JobSpec
  "Job specification for bulk insert."
  [:map [:task-identifier :string] [:payload :map]
   [:priority {:optional true} :int]
   [:queue-name {:optional true} [:maybe :string]]
   [:rate-limit-key {:optional true} [:maybe :string]]
   [:run-at {:optional true} inst?] [:max-attempts {:optional true} :int]
   [:job-key {:optional true} [:maybe :string]]
   [:job-key-mode {:optional true}
    [:enum "replace" "preserve_run_at" "unsafe_dedupe"]]
   [:flags {:optional true} [:vector :string]]])

(def PartialResults
  "Partial success results."
  [:map [:completed-steps [:vector :string]] [:failed-steps [:vector :string]]
   [:retry-from-step {:optional true} [:maybe :string]]
   [:results {:optional true} :any]])

(def HistoryRecord
  "Job history record."
  [:map [:id {:optional true} :uuid] [:job-id :uuid] [:correlation-id :uuid]
   [:task-identifier :string]
   [:status [:enum "started" "completed" "failed" "partial_success"]]
   [:worker-id {:optional true} [:maybe :string]]
   [:started-at {:optional true} [:maybe inst?]]
   [:completed-at {:optional true} [:maybe inst?]]
   [:queue-time-ms {:optional true} [:maybe :int]]
   [:execution-time-ms {:optional true} [:maybe :int]]
   [:attempt-number {:optional true} :int]
   [:error-message {:optional true} [:maybe :string]]
   [:error-stack {:optional true} [:maybe :string]]
   [:completed-steps {:optional true} [:maybe [:vector :string]]]
   [:failed-steps {:optional true} [:maybe [:vector :string]]]
   [:retry-from-step {:optional true} [:maybe :string]]])
