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

(ns dev.skivi.job-history.schema "Malli schemas for the job-history component.")

(def HistoryStoreConfig
  "Configuration for a history store.
  :buffer-size controls how many terminal history records the in-memory ring
  buffer retains (default 1000). :history-retention is a PostgreSQL interval
  string controlling when DB records expire (default '30 days')."
  [:map
   [:buffer-size {:optional true} pos-int?]
   [:history-retention {:optional true} :string]])

(def PartialResults
  "Outcome of a partially successful job execution."
  [:map
   [:completed-steps [:vector :string]]
   [:failed-steps [:vector :string]]
   [:retry-from-step {:optional true} [:maybe :string]]
   [:results {:optional true} :any]])

(def HistoryRecord
  "A job execution history record."
  [:map
   [:job-id :uuid]
   [:correlation-id :uuid]
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

(def HistoryQuery
  "Criteria map for the query function.
  All keys are optional; absence means no restriction on that dimension.
  :from / :to filter on started_at. :limit caps result count (default 100)."
  [:map
   [:from {:optional true} inst?]
   [:to {:optional true} inst?]
   [:task-identifier {:optional true} :string]
   [:status {:optional true}
    [:enum "started" "completed" "failed" "partial_success"]]
   [:limit {:optional true} pos-int?]])

(def HistoryStore
  "A history store handle returned by create-store.
  :pool is the HikariCP DataSource (may be nil for in-memory-only use).
  :config holds merged configuration. :state is an atom with the ring buffer."
  [:map
   [:pool :any]
   [:config HistoryStoreConfig]
   [:state :any]])
