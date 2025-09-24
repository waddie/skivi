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

(ns dev.skivi.queue.schema "Malli schemas for queue component.")

(def QueueConfig
  "Local queue tuning parameters.

  :size               - max jobs held in the buffer at one time (default 50).
  :ttl-ms             - entries older than this are discarded on take (default 60000).
  :refetch-threshold  - fetch more jobs when depth drops below this (default 10).
  :poll-interval-ms   - sleep between polls when no jobs are available (default 2000).
  :refetch-delay-ms   - sleep between polls when jobs were found (default 1000).
  :task-identifiers   - restrict claims to these task types (nil = all).
  :forbidden-flags    - skip jobs carrying any of these flags (nil = none)."
  [:map
   [:size {:optional true} pos-int?]
   [:ttl-ms {:optional true} pos-int?]
   [:refetch-threshold {:optional true} nat-int?]
   [:poll-interval-ms {:optional true} pos-int?]
   [:refetch-delay-ms {:optional true} pos-int?]
   [:task-identifiers {:optional true} [:maybe [:vector :string]]]
   [:forbidden-flags {:optional true} [:maybe [:vector :string]]]])

(def QueueStats
  "Snapshot of queue operational metrics."
  [:map
   [:fetched :int]
   [:dispatched :int]
   [:stale-dropped :int]
   [:refetch-count :int]])

(def JobManagerSystem
  "Upstream system map consumed by the job-manager interface."
  [:map [:pool :any] [:validator :any]])

(def LocalQueue
  "A local queue handle returned by create-queue."
  [:map
   [:job-system JobManagerSystem]
   [:worker-id :string]
   [:config QueueConfig]
   [:state :any]])
