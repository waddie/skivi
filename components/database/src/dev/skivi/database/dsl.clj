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

(ns dev.skivi.database.dsl
  (:require [honey.sql :as sql]
            [honey.sql.helpers :as h]))

(defn sqlvec
  "Format a HoneySQL query map to a JDBC-ready SQL vector."
  {:malli/schema [:function [:=> [:cat :map] [:vector :any]]]}
  [q]
  (sql/format q))

;; -- primitives

(defn now
  "Return a HoneySQL raw SQL expression for the current timestamp."
  {:malli/schema [:function [:=> [:cat] :any]]}
  []
  [:raw "now()"])

(defn any-ids
  "Return a HoneySQL :in clause matching id against ids."
  {:malli/schema [:function [:=> [:cat [:sequential :any]] :any]]}
  [ids]
  [:in :id (vec ids)])

(defn returning-all
  "Append RETURNING * to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map] :map]]}
  [q]
  (h/returning q :*))

;; -- table anchors

(defn job-history
  "Append FROM job_history to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map] :map]]}
  [q]
  (h/from q :job_history))

(defn jobs
  "Append FROM jobs to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map] :map]]}
  [q]
  (h/from q :jobs))

;; -- predicates

(defn by-job-id
  "Append WHERE job_id = job-id to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map :uuid] :map]]}
  [q job-id]
  (h/where q [:= :job_id job-id]))

(defn by-id
  "Append WHERE id = id to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map :uuid] :map]]}
  [q id]
  (h/where q [:= :id id]))

(defn by-worker
  "Append WHERE worker_id = worker-id to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map :string] :map]]}
  [q worker-id]
  (h/where q [:= :worker_id worker-id]))

(defn by-correlation
  "Append WHERE correlation_id = cid to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map :uuid] :map]]}
  [q cid]
  (h/where q [:= :correlation_id cid]))

(defn status-is
  "Append WHERE status = status to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map :string] :map]]}
  [q status]
  (h/where q [:= :status status]))

(defn started-only
  "Append WHERE status = 'started' to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map] :map]]}
  [q]
  (status-is q "started"))

(defn newest-first
  "Append ORDER BY created_at DESC to a HoneySQL query."
  {:malli/schema [:function [:=> [:cat :map] :map]]}
  [q]
  (h/order-by q [:created_at :desc]))

;; -- queries

(defn job-history-by-job-id
  "Build a SELECT * FROM job_history query for job-id, newest first."
  {:malli/schema [:function [:=> [:cat :uuid] :map]]}
  [job-id]
  (-> (h/select :*)
      job-history
      (by-job-id job-id)
      newest-first))

(defn job-history-by-correlation
  "Build a SELECT * FROM job_history query for correlation cid, newest first."
  {:malli/schema [:function [:=> [:cat :uuid] :map]]}
  [cid]
  (-> (h/select :*)
      job-history
      (by-correlation cid)
      newest-first))

(defn query-history
  "Build a SELECT * FROM job_history query filtered by criteria map.
  Criteria keys (all optional): :from, :to (inst?), :task-identifier, :status, :limit."
  {:malli/schema [:function [:=> [:cat :map] :map]]}
  [{:keys [from to task-identifier status limit]}]
  (-> (h/select :*)
      (h/from :job_history)
      (cond-> from (h/where [:>= :started_at [:cast from :timestamptz]]))
      (cond-> to (h/where [:<= :started_at [:cast to :timestamptz]]))
      (cond-> task-identifier (h/where [:= :task_identifier task-identifier]))
      (cond-> status (h/where [:= :status status]))
      (h/order-by [:started_at :desc])
      (h/limit (or limit 100))))

(defn locked-job-queue-names
  "Build a SELECT query for queue_name of locked jobs.
  Restricted to worker-ids when provided."
  {:malli/schema [:function [:=> [:cat [:maybe [:vector :string]]] :map]]}
  [worker-ids]
  (-> (h/select :queue_name)
      (h/from :jobs)
      (h/where [:is-not :locked_at nil] [:is-not :queue_name nil])
      (cond-> (seq worker-ids) (h/where [:in :locked_by (vec worker-ids)]))))

;; -- commands

(defn mark-job-completed
  "Build an UPDATE job_history query to mark a started record as completed."
  {:malli/schema [:function [:=> [:cat :uuid :string :uuid :int] :map]]}
  [job-id worker-id cid execution-time-ms]
  (-> (h/update :job_history)
      (h/set {:completed_at (now)
              :execution_time_ms execution-time-ms
              :status       "completed"})
      (by-job-id job-id)
      (by-worker worker-id)
      (by-correlation cid)
      started-only
      returning-all))

(defn mark-job-failed
  "Build an UPDATE job_history query to mark a started record as failed."
  {:malli/schema [:function
                  [:=>
                   [:cat :uuid :string :uuid :int [:maybe :string]
                    [:maybe :string]] :map]]}
  [job-id worker-id cid execution-time-ms err-msg err-stack]
  (-> (h/update :job_history)
      (h/set {:completed_at      (now)
              :error_message     err-msg
              :error_stack       err-stack
              :execution_time_ms execution-time-ms
              :status            "failed"})
      (by-job-id job-id)
      (by-worker worker-id)
      (by-correlation cid)
      started-only
      returning-all))

(defn mark-job-partial
  "Build an UPDATE job_history query to mark a started record as partial_success."
  {:malli/schema [:function [:=> [:cat :uuid :string :uuid :int :map] :map]]}
  [job-id worker-id cid execution-time-ms partial]
  (-> (h/update :job_history)
      (h/set {:completed_at      (now)
              :completed_steps   (into-array String (:completed-steps partial))
              :execution_time_ms execution-time-ms
              :failed_steps      (into-array String (:failed-steps partial))
              :retry_from_step   (:retry-from-step partial)
              :status            "partial_success"})
      (by-job-id job-id)
      (by-worker worker-id)
      (by-correlation cid)
      started-only
      returning-all))

(defn reschedule
  "Build an UPDATE jobs query to reschedule job-ids. Returns nil if no fields to update."
  {:malli/schema [:function [:=> [:cat [:vector :uuid] :map] [:maybe :map]]]}
  [job-ids {:keys [run-at priority max-attempts]}]
  (let [set-map (cond-> {}
                  run-at (assoc :run_at run-at)
                  priority (assoc :priority (int priority))
                  max-attempts (assoc :max_attempts (int max-attempts)))]
    (when (seq set-map)
      (-> (h/update :jobs)
          (h/set (assoc set-map :revision [:+ :revision 1]))
          (h/where (any-ids (into-array java.util.UUID job-ids)))
          returning-all))))

(defn upsert-task-identifier
  "Build an INSERT ... ON CONFLICT DO UPDATE query for task_identifiers."
  {:malli/schema [:function [:=> [:cat :string] :map]]}
  [identifier]
  (-> (h/insert-into :task_identifiers)
      (h/columns :identifier :last_used)
      (h/values [[identifier (now)]])
      (h/on-conflict :identifier)
      (h/do-update-set {:last_used (now)})))

(defn upsert-crontab
  "Build an INSERT ... ON CONFLICT DO NOTHING query for known_crontabs."
  {:malli/schema [:function [:=> [:cat :string] :map]]}
  [identifier]
  (-> (h/insert-into :known_crontabs)
      (h/columns :identifier :known_since)
      (h/values [[identifier (now)]])
      (h/on-conflict :identifier)
      (h/do-nothing)))

(defn crontab-state
  "Build a SELECT query for last_execution and known_since by identifier."
  {:malli/schema [:function [:=> [:cat :string] :map]]}
  [identifier]
  (-> (h/select :last_execution :known_since)
      (h/from :known_crontabs)
      (h/where [:= :identifier identifier])))

(defn touch-last-execution
  "Build an UPDATE known_crontabs query to set last_execution = now() by identifier."
  {:malli/schema [:function [:=> [:cat :string] :map]]}
  [identifier]
  (-> (h/update :known_crontabs)
      (h/set {:last_execution (now)})
      (h/where [:= :identifier identifier])))

(defn unlock-jobs
  "Build an UPDATE jobs query to clear locks. Restricted to worker-ids when provided."
  {:malli/schema [:function [:=> [:cat [:maybe [:vector :string]]] :map]]}
  [worker-ids]
  (-> (h/update :jobs)
      (h/set {:locked_at nil
              :locked_by nil
              :revision  [:+ :revision 1]})
      (h/where [:is-not :locked_at nil])
      (cond-> (seq worker-ids) (h/where [:in :locked_by (vec worker-ids)]))
      returning-all))

(defn unlock-queues
  "Build an UPDATE job_queues query to clear locks. Restricted to queue-names when provided."
  {:malli/schema [:function [:=> [:cat [:maybe [:vector :string]]] :map]]}
  [queue-names]
  (-> (h/update :job_queues)
      (h/set {:locked_at nil
              :locked_by nil})
      (h/where [:is-not :locked_by nil])
      (cond-> (seq queue-names) (h/where [:in :queue_name (vec queue-names)]))
      returning-all))
