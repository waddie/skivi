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

(ns dev.skivi.worker-pool.schema "Malli schemas for the worker-pool component.")

(def WorkerPoolConfig
  "Tuning parameters for a worker pool.

  :concurrency                  - number of worker threads (default 10).
  :poll-interval-ms             - sleep between polls when no jobs found (default 2000).
  :queue-size                   - max jobs held in the local buffer (default 50).
  :queue-ttl-ms                 - entries older than this are discarded on take (default 60000).
  :graceful-shutdown-timeout-ms - max time to wait for in-flight jobs on stop! (default 30000).
  :max-job-execution-time-ms    - per-job hard timeout; Thread.interrupt() sent on breach (default 300000).
  :task-identifiers             - restrict claims to these task types (nil = all).
  :forbidden-flags              - skip jobs carrying any of these flags (nil = none)."
  [:map
   [:concurrency {:optional true} pos-int?]
   [:poll-interval-ms {:optional true} pos-int?]
   [:queue-size {:optional true} pos-int?]
   [:queue-ttl-ms {:optional true} pos-int?]
   [:graceful-shutdown-timeout-ms {:optional true} pos-int?]
   [:max-job-execution-time-ms {:optional true} [:maybe pos-int?]]
   [:task-identifiers {:optional true} [:maybe [:vector :string]]]
   [:forbidden-flags {:optional true} [:maybe [:vector :string]]]])

(def PoolStats
  "Operational metrics snapshot for a worker pool."
  [:map
   [:active nat-int?]
   [:completed nat-int?]
   [:failed nat-int?]
   [:errors nat-int?]])

(def TaskRegistry
  "Map of task-identifier strings to handler functions.
  Each fn receives a context map {:job job :job-system sys :worker-id wid}."
  [:map-of :string ifn?])

(def WorkerPool
  "A worker pool handle returned by create-pool."
  [:map
   [:config WorkerPoolConfig]
   [:emitter :any]
   [:job-system :any]
   [:queue :any]
   [:state :any]
   [:task-registry TaskRegistry]])
