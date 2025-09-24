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

(ns dev.skivi.worker-pool.core
  "Thread-pool-based worker pool that claims jobs from the queue and dispatches
  them to registered task functions.

  All worker threads share one queue and one worker-id, so jobs are claimed
  atomically in batches and distributed via take-job!. The pool does not own the
  job-system or emitter; callers supply those at creation time.

  Task functions receive a context map with :job, :job-system, and :worker-id.
  They signal outcomes as follows:
    • return any value (including nil) - job succeeds (WorkerCompletesJob)
    • return (partial-success m)       - partial success (WorkerReportsPartialSuccess)
    • throw any Throwable              - job fails   (WorkerFailsJob)

  Events emitted:
    :pool/start          - pool is started (includes :concurrency)
    :pool/stop           - pool has stopped (includes :forced? when force-stop!)
    :job/completed       - task fn returned normally
    :job/failed          - task fn threw; job retry scheduled
    :job/exhausted       - task fn threw on the final allowed attempt
    :job/partial-success - task fn returned a partial-success value
    :worker/error        - infrastructure exception outside task execution"
  (:require [dev.skivi.job-manager.interface :as job-manager]
            [dev.skivi.monitoring.interface :as monitoring]
            [dev.skivi.queue.interface :as queue]
            [dev.skivi.worker-pool.schema :as schema]))

(def ^:private partial-success-key ::partial-success)

(def ^:private defaults
  {:concurrency      10
   :graceful-shutdown-timeout-ms 30000
   :poll-interval-ms 2000
   :queue-size       50
   :queue-ttl-ms     60000})

(def ^:private take-timeout-ms
  "How long each worker waits on take-job! before re-checking :running?."
  1000)

(defn partial-success
  "Wraps partial-results so the pool treats the task return as a partial success.
  partial-results keys: :completed-steps, :failed-steps, :retry-from-step, :results."
  {:malli/schema [:function [:=> [:cat :map] :map]]}
  [partial-results]
  (assoc partial-results partial-success-key true))

(defn partial-success?
  "Returns true if v was produced by partial-success."
  {:malli/schema [:function [:=> [:cat :any] :boolean]]}
  [v]
  (boolean (and (map? v) (get v partial-success-key))))

(defn- partial-results->map [result] (dissoc result partial-success-key))

(defn- execute-job!
  "Resolves and invokes the task fn for job. Throws if no fn is registered."
  [pool job]
  (let [{:keys [task-registry job-system queue]} pool
        task-id (:task-identifier job)
        task-fn (get task-registry task-id)]
    (when-not task-fn
      (throw (ex-info (str "No handler registered for task: " task-id)
                      {::missing-handler true
                       :job-id (:id job)
                       :task-identifier task-id})))
    (task-fn {:job        job
              :job-system job-system
              :worker-id  (queue/worker-id queue)})))

(defn- process-job!
  "Executes job and reports outcome to job-manager. Updates pool stats."
  [pool job]
  (let [{:keys [emitter job-system queue state]} pool
        wid   (queue/worker-id queue)
        start (System/currentTimeMillis)]
    (swap! state update-in [:stats :active] inc)
    (try
      (let [result  (execute-job! pool job)
            elapsed (- (System/currentTimeMillis) start)]
        (if (partial-success? result)
          (let [exhausted? (>= (:attempts job) (:max-attempts job))]
            (job-manager/report-partial-success job-system
                                                wid
                                                job
                                                (partial-results->map result)
                                                elapsed)
            (monitoring/emit! emitter
                              :job/partial-success
                              {:job-id    (:id job)
                               :worker-id wid})
            (when exhausted?
              (monitoring/emit! emitter
                                :job/exhausted
                                {:job-id    (:id job)
                                 :worker-id wid})))
          (do (job-manager/complete-jobs job-system wid [job] elapsed)
              (monitoring/emit! emitter
                                :job/completed
                                {:job-id    (:id job)
                                 :worker-id wid})))
        (swap! state update-in [:stats :completed] inc))
      (catch Throwable ex
        (let [elapsed       (- (System/currentTimeMillis) start)
              missing?      (boolean (::missing-handler (ex-data ex)))
              ;; Missing handler is a config error; bypass the retry cycle
              ;; by presenting max-attempts as already reached so fail-jobs
              ;; exhausts the job rather than scheduling a retry.
              effective-job (if missing?
                              (assoc job :attempts (:max-attempts job))
                              job)
              exhausted?    (or missing?
                                (>= (:attempts job) (:max-attempts job)))]
          (try (job-manager/fail-jobs job-system
                                      wid
                                      [{:error ex
                                        :job   effective-job}]
                                      (if missing? 0 elapsed))
               (if exhausted?
                 (monitoring/emit! emitter
                                   :job/exhausted
                                   {:error     (ex-message ex)
                                    :job-id    (:id job)
                                    :worker-id wid})
                 (monitoring/emit! emitter
                                   :job/failed
                                   {:error     (ex-message ex)
                                    :job-id    (:id job)
                                    :worker-id wid}))
               (catch Exception fail-ex
                 (monitoring/emit! emitter
                                   :worker/error
                                   {:cause     :fail-record-error
                                    :error     (ex-message fail-ex)
                                    :job-id    (:id job)
                                    :worker-id wid})
                 (swap! state update-in [:stats :errors] inc)))
          (swap! state update-in [:stats :failed] inc)))
      (finally (swap! state update-in [:stats :active] dec)))))

(defn- run-worker!
  "Worker loop: claims and processes jobs until pool is stopped."
  [pool]
  (try (loop []
         (when (:running? @(:state pool))
           (try (when-let [job (queue/take-job! (:queue pool) take-timeout-ms)]
                  (process-job! pool job))
                (catch InterruptedException ex (throw ex))
                (catch Exception ex
                  (monitoring/emit! (:emitter pool)
                                    :worker/error
                                    {:error     (ex-message ex)
                                     :worker-id (queue/worker-id (:queue
                                                                  pool))})
                  (swap! (:state pool) update-in [:stats :errors] inc)))
           (recur)))
       (catch InterruptedException _ (.interrupt (Thread/currentThread)))))

(defn create-pool
  "Creates a worker pool. Call start! to begin processing jobs.
  task-registry is a map of task-identifier strings to handler functions.
  Each handler fn receives {:job job :job-system sys :worker-id wid}."
  {:malli/schema [:function
                  [:=> [:cat :any schema/TaskRegistry :any] schema/WorkerPool]
                  [:=>
                   [:cat :any schema/TaskRegistry :any schema/WorkerPoolConfig]
                   schema/WorkerPool]]}
  ([job-system task-registry emitter]
   (create-pool job-system task-registry emitter {}))
  ([job-system task-registry emitter config]
   (let [cfg (merge defaults config)
         q   (queue/create-queue
              job-system
              (str "skivi-pool-" (random-uuid))
              (cond-> {:poll-interval-ms (:poll-interval-ms cfg)
                       :size   (:queue-size cfg)
                       :ttl-ms (:queue-ttl-ms cfg)}
                (:task-identifiers cfg) (assoc :task-identifiers
                                               (:task-identifiers cfg))
                (:forbidden-flags cfg) (assoc :forbidden-flags
                                              (:forbidden-flags cfg))))]
     {:config        cfg
      :emitter       emitter
      :job-system    job-system
      :queue         q
      :state         (atom {:running? false
                            :stats    {:active    0
                                       :completed 0
                                       :errors    0
                                       :failed    0}
                            :threads  []})
      :task-registry task-registry})))

(defn start!
  "Starts the worker pool. Throws if already running. Returns pool."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] schema/WorkerPool]]}
  [pool]
  (let [{:keys [config emitter state queue]} pool
        concurrency (:concurrency config)]
    (when (:running? @state) (throw (ex-info "Pool already started" {})))
    (queue/start! queue)
    (swap! state assoc :running? true)
    (let [threads (mapv (fn [i]
                          (doto (Thread. ^Runnable #(run-worker! pool)
                                         (str "skivi-worker-" i))
                            (.setDaemon true)
                            (.start)))
                        (range concurrency))]
      (swap! state assoc :threads threads)
      (monitoring/emit! emitter :pool/start {:concurrency concurrency})
      pool)))

(defn stop!
  "Gracefully stops the pool. Waits up to timeout-ms for in-flight jobs.
  Remaining buffered-but-unclaimed jobs stay locked in the DB and are
  recovered by ResetOverdueJobs after config.lock_timeout. Returns pool."
  {:malli/schema [:function
                  [:=> [:cat schema/WorkerPool] schema/WorkerPool]
                  [:=> [:cat schema/WorkerPool pos-int?] schema/WorkerPool]]}
  ([pool] (stop! pool (get-in pool [:config :graceful-shutdown-timeout-ms])))
  ([pool timeout-ms]
   (let [{:keys [emitter state queue]} pool]
     (swap! state assoc :running? false)
     (queue/stop! queue (min 5000 (long timeout-ms)))
     (let [threads    (:threads @state)
           deadline   (+ (System/currentTimeMillis) (long timeout-ms))
           all-joined (volatile! true)]
       (doseq [^Thread t threads]
         (let [remaining (- deadline (System/currentTimeMillis))]
           (if (pos? remaining)
             (.join t remaining)
             (vreset! all-joined false))))
       (when-not @all-joined
         (doseq [^Thread t threads]
           (when (.isAlive t) (.interrupt t))))
       (monitoring/emit! emitter :pool/stop {:forced? (not @all-joined)}))
     pool)))

(defn force-stop!
  "Immediately interrupts all worker threads and stops the pool. Returns pool."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] schema/WorkerPool]]}
  [pool]
  (let [{:keys [emitter state queue]} pool]
    (swap! state assoc :running? false)
    (queue/stop! queue 100)
    (doseq [^Thread t (:threads @state)]
      (.interrupt ^Thread t))
    (monitoring/emit! emitter :pool/stop {:forced? true})
    pool))

(defn running?
  "Returns true if the pool has been started and not yet stopped."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] :boolean]]}
  [pool]
  (boolean (:running? @(:state pool))))

(defn active-workers
  "Returns the number of worker threads currently executing a task."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] nat-int?]]}
  [pool]
  (get-in @(:state pool) [:stats :active] 0))

(defn stats
  "Returns a snapshot of pool operational metrics.
  Keys: :active, :completed, :failed, :errors."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] schema/PoolStats]]}
  [pool]
  (:stats @(:state pool)))

(defn worker-id
  "Returns the worker-id used for all job claims from this pool."
  {:malli/schema [:function [:=> [:cat schema/WorkerPool] :string]]}
  [pool]
  (queue/worker-id (:queue pool)))
