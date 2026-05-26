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

(ns dev.skivi.skivi.core
  "Unified entry point that wires all skivi components into a single system map.

  Typical usage:

    (def system
      (-> (skivi/create-system config {\"send-email\" handle-send-email})
          (skivi/start!)))

    (skivi/add-job system \"send-email\" {:to \"x@y.com\"})

    ;; On shutdown
    (skivi/stop! system)"
  (:require [dev.skivi.config.interface :as config]
            [dev.skivi.database.interface :as database]
            [dev.skivi.job-history.interface :as job-history]
            [dev.skivi.job-manager.interface :as job-manager]
            [dev.skivi.maintenance.interface :as maintenance]
            [dev.skivi.migration.interface :as migration]
            [dev.skivi.monitoring.interface :as monitoring]
            [dev.skivi.scheduler.interface :as scheduler]
            [dev.skivi.validation.interface :as validation]
            [dev.skivi.worker-pool.interface :as worker-pool]))

(defn load-task-registry
  "Loads task handlers from Clojure files in task-directory.
  Each file must evaluate to a {\"task-id\" handler-fn} map as its last expression.
  file-extensions is a sequence of file suffixes to include (default: [\".clj\"]).
  Returns a merged registry map; files are processed in alphabetical order."
  ([task-directory] (load-task-registry task-directory [".clj"]))
  ([task-directory file-extensions]
   (let [exts (set (if (seq file-extensions) file-extensions [".clj"]))
         dir  (java.io.File. ^String task-directory)]
     (when-not (.isDirectory dir)
       (throw (ex-info (str
                        "task-directory does not exist or is not a directory: "
                        task-directory)
                       {:task-directory task-directory})))
     (->> (.listFiles dir)
          (filter (fn [^java.io.File f]
                    (some #(.endsWith (.getName f) ^String %) exts)))
          (sort-by #(.getName ^java.io.File %))
          (reduce (fn [registry ^java.io.File f]
                    (let [result (load-file (.getAbsolutePath f))]
                      (when-not (map? result)
                        (throw (ex-info (str "Task file must return a map: "
                                             (.getName f))
                                        {:file (.getAbsolutePath f)})))
                      (merge registry result)))
                  {})))))

(defn create-system
  "Creates a system map from config, task-registry, and optional crontabs.
  Does not start any background processes - call start! to begin processing.

  If config contains [:worker :task-directory], Clojure files there are loaded
  at startup and merged with task-registry. The programmatic task-registry wins
  on identifier conflicts.

  config        - validated Config map (see config/load-config)
  task-registry - map of task-identifier string to handler fn
  crontabs      - vector of CrontabEntry maps (see scheduler/create-scheduler)"
  ([config] (create-system config {} []))
  ([config task-registry] (create-system config task-registry []))
  ([config task-registry crontabs]
   (let [worker-cfg      (config/worker-config config)
         file-registry   (when-let [dir (:task-directory worker-cfg)]
                           (load-task-registry dir
                                               (:file-extensions worker-cfg)))
         merged-registry (merge file-registry task-registry)
         emitter         (monitoring/create-emitter
                          (get-in config [:monitoring :events]))
         pool            (database/create-pool (config/database-config config))
         validator       (validation/create-validator config)
         job-system      {:pool         pool
                          :retry-config (config/retry-config config)
                          :validator    validator}
         pool-cfg        {:concurrency      (:concurrency worker-cfg)
                          :graceful-shutdown-timeout-ms
                          (:graceful-shutdown-timeout worker-cfg)
                          :max-job-execution-time-ms (:max-job-execution-time
                                                      worker-cfg)
                          :poll-interval-ms (:poll-interval worker-cfg)
                          :queue-size       (get-in config
                                                    [:queue :local-queue :size])
                          :queue-ttl-ms     (get-in config
                                                    [:queue :local-queue :ttl])}
         wp              (worker-pool/create-pool job-system
                                                  merged-registry
                                                  emitter
                                                  pool-cfg)
         history         (job-history/create-store pool
                                                   {:buffer-size
                                                    (get-in config
                                                            [:monitoring :events
                                                             :buffer-size]
                                                            1000)})
         sched           (when (seq crontabs)
                           (scheduler/create-scheduler job-system
                                                       crontabs
                                                       emitter
                                                       (config/scheduler-config
                                                        config)))
         cleanup-cfg     (config/cleanup-config config)
         maint           (when (get cleanup-cfg :enabled true)
                           (maintenance/create-scheduler
                            job-system
                            {:maintenance-interval-ms
                             (get cleanup-cfg :maintenance-interval-ms 60000)
                             :retention-periods
                             (get cleanup-cfg :retention-periods {})
                             :schedule (get cleanup-cfg :schedule "0 3 * * *")
                             :tasks    (get cleanup-cfg
                                            :tasks
                                            [:gc-task-identifiers
                                             :gc-job-queues
                                             :gc-job-history])
                             :timezone (get-in config
                                               [:cron :timezone]
                                               "UTC")}))]
     (cond-> {:config      config
              :emitter     emitter
              :history     history
              :job-system  job-system
              :pool        pool
              :validator   validator
              :worker-pool wp}
       sched (assoc :scheduler sched)
       maint (assoc :maintenance maint)))))

(defn start!
  "Runs pending migrations unless :migrate? is false, then starts the worker
  pool and scheduler (when present). Returns system.

  opts keys: :migrate? (default true)"
  ([system] (start! system {}))
  ([system opts]
   (when (get opts :migrate? true)
     (let [cfg    (:config system)
           db-cfg (config/database-config cfg)]
       (migration/migrate! {:connection-string (:connection-string db-cfg)
                            :schema-name       (config/schema-name cfg)})))
   (worker-pool/start! (:worker-pool system))
   (when-let [sched (:scheduler system)]
     (scheduler/start! sched))
   (when-let [maint (:maintenance system)]
     (maintenance/start! maint))
   system))

(defn stop!
  "Gracefully stops maintenance, scheduler, worker pool, and closes the connection pool.
  Returns system.

  opts keys: :maintenance-timeout-ms (default 5000), :scheduler-timeout-ms (default 5000),
             :worker-timeout-ms (default 15000)"
  ([system] (stop! system {}))
  ([system opts]
   (when-let [maint (:maintenance system)]
     (maintenance/stop! maint (get opts :maintenance-timeout-ms 5000)))
   (when-let [sched (:scheduler system)]
     (scheduler/stop! sched (get opts :scheduler-timeout-ms 5000)))
   (let [cfg-timeout
         (get-in system [:config :worker :graceful-shutdown-timeout] 30000)]
     (worker-pool/stop! (:worker-pool system)
                        (get opts :worker-timeout-ms cfg-timeout)))
   (database/close-pool (:pool system))
   system))

;;; Job enqueueing

(defn add-job
  "Validates payload and enqueues a single job. Returns the created job."
  ([system task-identifier payload]
   (job-manager/add-job (:job-system system) task-identifier payload))
  ([system task-identifier payload opts]
   (job-manager/add-job (:job-system system) task-identifier payload opts)))

(defn add-jobs
  "Validates all payloads and enqueues multiple jobs atomically. Returns created jobs."
  [system job-specs]
  (job-manager/add-jobs (:job-system system) job-specs))

(defn reschedule-jobs
  "Updates run-at, priority, or max-attempts for job-ids. Returns updated jobs."
  [system job-ids opts]
  (job-manager/reschedule-jobs (:job-system system) job-ids opts))

(defn replay-failed-jobs
  "Creates new jobs from failed history records matching criteria map."
  [system criteria]
  (job-manager/replay-failed-jobs (:job-system system) criteria))

(defn permanently-fail-jobs
  "Sets jobs to exhausted status with reason as last-error."
  [system job-ids reason]
  (job-manager/permanently-fail-jobs (:job-system system) job-ids reason))

(defn force-unlock-jobs
  "Clears locks on locked jobs. With no worker-ids, unlocks all locked jobs."
  ([system] (job-manager/force-unlock-jobs (:job-system system)))
  ([system worker-ids]
   (job-manager/force-unlock-jobs (:job-system system) worker-ids)))

(defn force-unlock-queues
  "Clears locks on named job queues. With no queue-names, unlocks all locked queues."
  ([system] (job-manager/force-unlock-queues (:job-system system)))
  ([system queue-names]
   (job-manager/force-unlock-queues (:job-system system) queue-names)))

(defn register-rate-limit
  "Registers or updates a rate limit. capacity is max tokens per interval.
  interval is a SQL interval string (e.g. \"1 minute\")."
  [system key capacity interval]
  (job-manager/register-rate-limit (:job-system system) key capacity interval))

;;; Job history

(defn recent-history
  "Returns recent job execution records from the in-memory ring buffer.
  With n, returns the last n records. No database access."
  ([system] (job-history/recent (:history system)))
  ([system n] (job-history/recent (:history system) n)))

(defn query-history
  "Returns history records matching criteria map from the database.
  Criteria keys (all optional): :from, :to, :task-identifier, :status, :limit."
  [system criteria]
  (job-history/query (:history system) criteria))

;;; Observability

(defn worker-pool-stats
  "Returns a snapshot of worker pool operational metrics."
  [system]
  (worker-pool/stats (:worker-pool system)))

(defn scheduler-stats
  "Returns a snapshot of scheduler operational metrics, or nil if no scheduler."
  [system]
  (some-> (:scheduler system)
          scheduler/stats))

(defn maintenance-stats
  "Returns a snapshot of maintenance scheduler metrics, or nil if disabled."
  [system]
  (some-> (:maintenance system)
          maintenance/stats))

(defn emitter-stats
  "Returns a snapshot of monitoring emitter metrics."
  [system]
  (monitoring/stats (:emitter system)))

(defn health-check
  "Returns true if the database is accessible, false otherwise."
  [system]
  (database/health-check (:pool system)))
