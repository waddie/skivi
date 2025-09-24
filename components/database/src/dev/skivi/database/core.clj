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

(ns dev.skivi.database.core
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [dev.skivi.database.dsl :as dsl]
            [dev.skivi.database.schema :as schema]
            [honey.sql.helpers :as h]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as prepare]
            [next.jdbc.result-set :as rs])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           [java.io PrintWriter StringWriter]
           [java.sql PreparedStatement]))

;;; Type coercions - applied globally to all result sets in this component

(extend-protocol rs/ReadableColumn
 org.postgresql.util.PGobject
   (read-column-by-label [^org.postgresql.util.PGobject v _]
     (when-let [val (some-> v
                            .getValue)]
       (if (#{"jsonb" "json"} (.getType v)) (json/parse-string val true) val)))
   (read-column-by-index [^org.postgresql.util.PGobject v _ _]
     (when-let [val (some-> v
                            .getValue)]
       (if (#{"jsonb" "json"} (.getType v)) (json/parse-string val true) val)))
 java.sql.Array
   (read-column-by-label [^java.sql.Array v _]
     (some-> v
             .getArray
             vec))
   (read-column-by-index [^java.sql.Array v _ _]
     (some-> v
             .getArray
             vec))
 java.sql.Timestamp
   (read-column-by-label [^java.sql.Timestamp v _] (.toInstant v))
   (read-column-by-index [^java.sql.Timestamp v _ _] (.toInstant v)))

(extend-protocol prepare/SettableParameter
 java.time.Instant
   (set-parameter [^java.time.Instant v ^PreparedStatement stmt idx]
     (.setObject
      stmt
      idx
      (java.time.OffsetDateTime/ofInstant v java.time.ZoneOffset/UTC))))

(def ^:private result-opts {:builder-fn rs/as-unqualified-kebab-maps})

(defn- ->sql [q] (if (map? q) (dsl/sqlvec q) q))

(defn- with-status
  [job]
  (when job
    (assoc job
           :status
           (cond (some? (:locked-at job)) :locked
                 (>= (:attempts job) (:max-attempts job)) :exhausted
                 :else :available))))

;;; Connection Management

(defn create-pool
  "Create HikariCP connection pool from config map."
  {:malli/schema [:function [:=> [:cat schema/PoolConfig] schema/Pool]]}
  [config]
  (let [pool-cfg      (:pool-config config)
        hikari-config (doto (HikariConfig.)
                        (.setDriverClassName "org.postgresql.Driver")
                        (.setJdbcUrl (:connection-string config))
                        (.setUsername (:username config))
                        (.setPassword (:password config))
                        (.setMaximumPoolSize (:maximum-pool-size pool-cfg))
                        (.setMinimumIdle (:minimum-idle pool-cfg))
                        (.setConnectionTimeout (:connection-timeout pool-cfg))
                        (.setIdleTimeout (:idle-timeout pool-cfg))
                        (.setMaxLifetime (:max-lifetime pool-cfg))
                        (.setSchema (get config :schema-name "skivi"))
                        (.setInitializationFailTimeout -1))]
    (HikariDataSource. hikari-config)))

(defn close-pool
  "Close connection pool and release resources."
  {:malli/schema [:function [:=> [:cat schema/Pool] :nil]]}
  [pool]
  (when pool (.close pool)))

;;; Query Execution

(defn execute!
  "Execute SQL query with optional retry logic."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool schema/SqlMap] [:vector :map]]
                  [:=> [:cat schema/Pool schema/SqlMap :map] [:vector :map]]]}
  ([pool sql-map] (execute! pool sql-map {}))
  ([pool sql-map opts] (jdbc/execute! pool (->sql sql-map) opts)))

(defn execute-one!
  "Execute SQL query returning single result."
  {:malli/schema [:function [:=> [:cat schema/Pool schema/SqlMap] [:maybe :map]]
                  [:=> [:cat schema/Pool schema/SqlMap :map] [:maybe :map]]]}
  ([pool sql-map] (execute-one! pool sql-map {}))
  ([pool sql-map opts] (jdbc/execute-one! pool (->sql sql-map) opts)))

(defn with-transaction
  "Execute f within transaction, passing connection."
  {:malli/schema [:function [:=> [:cat schema/Pool ifn?] :any]]}
  [pool f]
  (jdbc/with-transaction [tx pool] (f tx)))

(defn health-check
  "Performs a simple health check by executing a basic query.
   Returns true if the database is accessible, false otherwise."
  {:malli/schema [:function [:=> [:cat schema/Pool] :boolean]]}
  [pool]
  (try (jdbc/execute-one! pool (dsl/sqlvec (h/select [1 :health])))
       true
       (catch Exception _ false)))

;;; Job Operations

(declare record-job-start)

(defn get-jobs
  "Retrieve and claim available jobs for worker. Records a started history entry
  per job atomically. Returns jobs with :correlation-id embedded."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string] [:vector schema/Job]]
                  [:=> [:cat schema/Pool :string :map] [:vector schema/Job]]]}
  ([pool worker-id] (get-jobs pool worker-id {}))
  ([pool worker-id opts]
   (let [{:keys [task-identifiers forbidden-flags batch-size history-retention]
          :or   {batch-size 1}}
         opts]
     (jdbc/with-transaction
      [tx pool]
      (let [jobs (->> (jdbc/execute! tx
                                     ["SELECT * FROM get_jobs(?, ?, ?, ?)"
                                      worker-id
                                      (when (seq task-identifiers)
                                        (into-array String task-identifiers))
                                      (when (seq forbidden-flags)
                                        (into-array String forbidden-flags))
                                      (int batch-size)]
                                     result-opts)
                      (mapv with-status))]
        (mapv (fn [job]
                (let [cid (random-uuid)
                      ret (or history-retention "30 days")]
                  (record-job-start tx job worker-id cid ret)
                  (assoc job :correlation-id cid)))
              jobs))))))

(defn complete-jobs
  "Mark jobs as completed and remove from queue."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string [:vector :uuid]]
                   [:vector schema/Job]]]}
  [pool worker-id job-ids]
  (jdbc/execute! pool
                 ["SELECT * FROM complete_jobs(?, ?)"
                  worker-id
                  (into-array java.util.UUID job-ids)]
                 result-opts))

(defn- fail-one!
  [tx worker-id {:keys [job-id error-message next-run-at target-attempts]}]
  (when-let
    [job
     (jdbc/execute-one!
      tx
      (if target-attempts
        ["UPDATE jobs
                       SET last_error       = ?,
                           run_at           = COALESCE(?, GREATEST(now(), run_at)),
                           locked_by        = NULL,
                           locked_at        = NULL,
                           attempts         = GREATEST(attempts, ?),
                           revision         = revision + 1
                       WHERE id = ? AND locked_by = ?
                       RETURNING *"
         error-message next-run-at target-attempts job-id worker-id]
        ["UPDATE jobs
                       SET last_error       = ?,
                           run_at           = COALESCE(?, GREATEST(now(), run_at)),
                           locked_by        = NULL,
                           locked_at        = NULL,
                           revision         = revision + 1
                       WHERE id = ? AND locked_by = ?
                       RETURNING *"
         error-message next-run-at job-id worker-id])
      result-opts)]
    (when (:queue-name job)
      (jdbc/execute!
       tx
       ["UPDATE job_queues SET locked_by = NULL, locked_at = NULL
          WHERE queue_name = ? AND locked_by = ?"
        (:queue-name job) worker-id]))
    (execute! tx (dsl/upsert-task-identifier (:task-identifier job)))
    (with-status job)))

(defn fail-jobs
  "Mark jobs as failed with error details."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string [:vector schema/JobFailure]]
                   [:vector schema/Job]]]}
  [pool worker-id job-failures]
  (with-transaction pool
                    (fn [tx]
                      (vec (keep #(fail-one! tx worker-id %) job-failures)))))

(defn add-job
  "Add single job to queue."
  {:malli/schema [:function [:=> [:cat schema/Pool :string :map] schema/Job]
                  [:=> [:cat schema/Pool :string :map :map] schema/Job]]}
  ([pool task-identifier payload] (add-job pool task-identifier payload {}))
  ([pool task-identifier payload opts]
   (let [{:keys [queue-name run-at priority max-attempts job-key job-key-mode
                 flags rate-limit-key]
          :or   {flags        []
                 job-key-mode "replace"
                 max-attempts 25
                 priority     0}}
         opts]
     (when (and (contains? opts :job-key-mode) (nil? job-key))
       (throw (ex-info "job-key is required when job-key-mode is set"
                       {:job-key-mode job-key-mode})))
     (jdbc/execute-one!
      pool
      ["SELECT * FROM add_job(?, ?::jsonb, ?, COALESCE(?::timestamptz, now()), ?, ?, ?, ?, ?::text[], ?)"
       task-identifier
       (json/generate-string payload)
       queue-name
       run-at
       (int priority)
       (int max-attempts)
       job-key
       (some-> job-key-mode
               name
               (str/replace \- \_))
       (into-array String flags)
       rate-limit-key]
      result-opts))))

(defn add-jobs
  "Add multiple jobs to queue in transaction."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:vector schema/JobSpec]]
                   [:vector schema/Job]]]}
  [pool job-specs]
  (with-transaction pool
                    (fn [tx]
                      (mapv (fn [spec]
                              (add-job tx
                                       (:task-identifier spec)
                                       (:payload spec)
                                       (dissoc spec :task-identifier :payload)))
                            job-specs))))

(defn reschedule-jobs
  "Update run-at for jobs."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:vector :uuid] :map]
                   [:vector schema/Job]]]}
  [pool job-ids opts]
  (if-let [q (dsl/reschedule job-ids opts)]
    (jdbc/execute! pool (dsl/sqlvec q) result-opts)
    []))

(defn force-unlock-jobs
  "Clear locks on locked jobs. When worker-ids is provided, restricted to those workers."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:maybe [:vector :string]]]
                   [:vector schema/Job]]]}
  [pool worker-ids]
  (with-transaction
   pool
   (fn [tx]
     (let [queue-rows  (execute! tx
                                 (dsl/locked-job-queue-names worker-ids)
                                 result-opts)
           queue-names (into [] (distinct (keep :queue-name queue-rows)))
           jobs        (execute! tx (dsl/unlock-jobs worker-ids) result-opts)]
       (when (seq queue-names)
         (execute! tx (dsl/unlock-queues queue-names) result-opts))
       (mapv with-status jobs)))))

(defn force-unlock-queues
  "Clear locks on job queues. When queue-names is provided, restricted to those queues."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:maybe [:vector :string]]]
                   [:vector :map]]]}
  [pool queue-names]
  (execute! pool (dsl/unlock-queues queue-names) result-opts))

(defn query-job-history
  "Return job history records matching criteria map."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :map] [:vector schema/HistoryRecord]]]}
  [pool criteria]
  (execute! pool (dsl/query-history criteria) result-opts))

(defn permanently-fail-jobs
  "Set jobs to exhausted status with reason. Unlocks any queues affected."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool [:vector :uuid] :string]
                   [:vector schema/Job]]]}
  [pool job-ids reason]
  (with-transaction
   pool
   (fn [tx]
     (let [queue-rows  (execute! tx
                                 (-> (h/select :queue_name)
                                     (h/from :jobs)
                                     (h/where (dsl/any-ids job-ids)
                                              [:is-not :locked_at nil]
                                              [:is-not :queue_name nil]))
                                 result-opts)
           queue-names (into [] (distinct (keep :queue-name queue-rows)))
           jobs        (execute! tx
                                 (-> (h/update :jobs)
                                     (h/set {:attempts   [:raw "max_attempts"]
                                             :last_error reason
                                             :locked_at  nil
                                             :locked_by  nil
                                             :revision   [:+ :revision 1]})
                                     (h/where (dsl/any-ids job-ids))
                                     dsl/returning-all)
                                 result-opts)]
       (when (seq queue-names)
         (execute! tx (dsl/unlock-queues queue-names) result-opts))
       (mapv with-status jobs)))))

;;; Maintenance Operations

(defn reset-locked-jobs
  "Reset locked jobs exceeding timeout."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]
                  [:=> [:cat schema/Pool :map] :int]]}
  ([pool] (reset-locked-jobs pool {}))
  ([pool opts]
   (let [expiry (or (:job-expiry opts)
                    (when-let [h (:timeout-hours opts)]
                      (str h " hours"))
                    "4 hours")]
     (-> (jdbc/execute-one! pool
                            ["SELECT reset_locked_jobs(?::interval)" expiry])
         vals
         first))))

(defn gc-task-identifiers
  "Remove unused task identifiers."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]
                  [:=> [:cat schema/Pool :map] :int]]}
  ([pool] (gc-task-identifiers pool {}))
  ([pool opts]
   (let [since (or (:keep-since opts)
                   (when-let [d (:keep-days opts)]
                     (str d " days"))
                   "7 days")]
     (-> (jdbc/execute-one! pool
                            ["SELECT gc_task_identifiers(?::interval)" since])
         vals
         first))))

(defn gc-job-queues
  "Remove empty job queues."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]]}
  [pool]
  (-> (jdbc/execute-one! pool ["SELECT gc_job_queues()"])
      vals
      first))

(defn gc-job-history
  "Remove expired job history records."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]]}
  [pool]
  (-> (jdbc/execute-one! pool ["SELECT gc_job_history()"])
      vals
      first))

;;; Crontab Operations

(defn upsert-crontab!
  "Insert known_crontabs entry if identifier is not already present."
  {:malli/schema [:function [:=> [:cat schema/Pool :string] :any]]}
  [pool identifier]
  (execute! pool (dsl/upsert-crontab identifier) result-opts))

(defn load-crontab-state
  "Return last_execution and known_since for identifier, or nil if not found."
  {:malli/schema [:function [:=> [:cat schema/Pool :string] [:maybe :map]]]}
  [pool identifier]
  (execute-one! pool (dsl/crontab-state identifier) result-opts))

(defn update-last-execution!
  "Set last_execution = now() for identifier in known_crontabs."
  {:malli/schema [:function [:=> [:cat schema/Pool :string] :any]]}
  [pool identifier]
  (execute! pool (dsl/touch-last-execution identifier) result-opts))

;;; Job History

(defn record-job-start
  "Record job execution start."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool schema/Job :string :uuid]
                   schema/HistoryRecord]
                  [:=> [:cat schema/Pool schema/Job :string :uuid :string]
                   schema/HistoryRecord]]}
  ([pool job worker-id correlation-id]
   (record-job-start pool job worker-id correlation-id "30 days"))
  ([pool job worker-id correlation-id retention]
   (jdbc/execute-one!
    pool
    ["INSERT INTO job_history
       (job_id, correlation_id, task_identifier, payload,
        worker_id, status, started_at, attempt_number, queue_time_ms, expires_at)
       VALUES (?, ?, ?, ?::jsonb, ?, 'started', now(), ?,
               LEAST(GREATEST(0, EXTRACT(EPOCH FROM (now() - ?::timestamptz)) * 1000), 2147483647)::integer,
               now() + ?::interval)
       RETURNING *"
     (:id job)
     correlation-id
     (:task-identifier job)
     (json/generate-string (:payload job))
     worker-id
     (:attempts job)
     (:run-at job)
     retention]
    result-opts)))

(defn record-job-completion
  "Record successful job completion."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :uuid :string :uuid :int [:maybe :map]]
                   schema/HistoryRecord]]}
  [pool job-id worker-id correlation-id execution-time-ms _results]
  (jdbc/execute-one!
   pool
   (dsl/sqlvec
    (dsl/mark-job-completed job-id worker-id correlation-id execution-time-ms))
   result-opts))

(defn- throwable->stack
  [^Throwable t]
  (let [sw (StringWriter.)
        pw (PrintWriter. sw)]
    (.printStackTrace t pw)
    (str sw)))

(defn record-job-failure
  "Record job failure with error details."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :uuid :string :uuid :int :any]
                   schema/HistoryRecord]]}
  [pool job-id worker-id correlation-id execution-time-ms error]
  (let [err-msg   (if (instance? Throwable error)
                    (ex-message error)
                    (or (:message error) (str error)))
        err-stack (if (instance? Throwable error)
                    (throwable->stack error)
                    (:stack error))]
    (jdbc/execute-one! pool
                       (dsl/sqlvec (dsl/mark-job-failed job-id
                                                        worker-id
                                                        correlation-id
                                                        execution-time-ms
                                                        err-msg
                                                        err-stack))
                       result-opts)))

(defn record-partial-success
  "Record partial job success with step details."
  {:malli/schema
   [:function
    [:=> [:cat schema/Pool :uuid :string :uuid :int schema/PartialResults]
     schema/HistoryRecord]]}
  [pool job-id worker-id correlation-id execution-time-ms partial-results]
  (jdbc/execute-one! pool
                     (dsl/sqlvec (dsl/mark-job-partial job-id
                                                       worker-id
                                                       correlation-id
                                                       execution-time-ms
                                                       partial-results))
                     result-opts))

(defn get-job-history
  "Get history records for job-id."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :uuid]
                   [:vector schema/HistoryRecord]]]}
  [pool job-id]
  (jdbc/execute! pool
                 (dsl/sqlvec (dsl/job-history-by-job-id job-id))
                 result-opts))

(defn get-correlation-history
  "Get history records for correlation-id."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :uuid]
                   [:vector schema/HistoryRecord]]]}
  [pool correlation-id]
  (jdbc/execute! pool
                 (dsl/sqlvec (dsl/job-history-by-correlation correlation-id))
                 result-opts))

(defn replay-failed-jobs
  "Replay failed jobs matching criteria."
  {:malli/schema [:function [:=> [:cat schema/Pool :map] [:vector schema/Job]]]}
  [pool criteria]
  (let [from (or (:from-time criteria) (:from criteria))
        to   (or (:to-time criteria) (:to criteria))
        task (:task-identifier criteria)]
    (jdbc/execute!
     pool
     ["SELECT * FROM replay_failed_jobs(?::timestamptz, ?::timestamptz, ?, NULL, ?)"
      from to task (int (or (:max-attempts criteria) 25))]
     result-opts)))

;;; Rate Limit Operations

(defn register-rate-limit
  "Register or update a rate limit. Creates a new limit with full capacity, or
  updates capacity and interval on an existing one (preserving current tokens)."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string :int :string]
                   schema/RateLimit]]}
  [pool key capacity interval]
  (jdbc/execute-one! pool
                     ["SELECT * FROM register_rate_limit(?, ?, ?::interval)"
                      key (int capacity) interval]
                     result-opts))

(defn refill-rate-limits
  "Reset tokens to capacity for all rate limits whose window has expired.
  Returns count of limits refilled."
  {:malli/schema [:function [:=> [:cat schema/Pool] :int]]}
  [pool]
  (-> (jdbc/execute-one! pool ["SELECT refill_rate_limits()"])
      vals
      first))

(defn get-rate-limit
  "Retrieve rate limit state by key. Returns nil if not found."
  {:malli/schema [:function
                  [:=> [:cat schema/Pool :string] [:maybe schema/RateLimit]]]}
  [pool key]
  (execute-one! pool
                (-> (h/select :*)
                    (h/from :rate_limits)
                    (h/where [:= :key key]))
                result-opts))
