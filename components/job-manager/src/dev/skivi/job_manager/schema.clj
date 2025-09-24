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

(ns dev.skivi.job-manager.schema "Malli schemas for job-manager component.")

(def correlation-id-key
  "Key embedded in job maps returned by get-jobs for history record correlation."
  :dev.skivi.job-manager/correlation-id)

;;; System

(def JobManagerSystem
  "Runtime system map providing a database pool and payload validator."
  [:map [:pool :any] [:validator :any]])

;;; Jobs

(def Job
  "Job record. When returned from get-jobs, contains ::correlation-id for history tracking."
  [:map
   [:id :uuid]
   [:task-identifier :string]
   [:payload :map]
   [:status {:optional true} [:enum :available :locked :exhausted]]
   [:priority {:optional true} :int]
   [:queue-name {:optional true} [:maybe :string]]
   [:run-at {:optional true} inst?]
   [:attempts {:optional true} :int]
   [:max-attempts {:optional true} :int]
   [:locked-at {:optional true} [:maybe inst?]]
   [:locked-by {:optional true} [:maybe :string]]
   [:revision {:optional true} :int]])

(def JobSpec
  "Specification for creating a job."
  [:map
   [:task-identifier :string]
   [:payload :map]
   [:priority {:optional true} :int]
   [:queue-name {:optional true} [:maybe :string]]
   [:run-at {:optional true} inst?]
   [:max-attempts {:optional true} :int]
   [:job-key {:optional true} [:maybe :string]]
   [:job-key-mode {:optional true}
    [:enum "replace" "preserve_run_at" "unsafe_dedupe"]]
   [:flags {:optional true} [:vector :string]]])

(def JobError
  "Failure pairing: the job that failed and the error cause."
  [:map [:job Job] [:error :any]])

(def PartialResults
  "Outcome of a partially successful job execution."
  [:map
   [:completed-steps [:vector :string]]
   [:failed-steps [:vector :string]]
   [:retry-from-step {:optional true} [:maybe :string]]
   [:results {:optional true} :any]])
