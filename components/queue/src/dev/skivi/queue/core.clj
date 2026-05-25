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

(ns dev.skivi.queue.core
  "In-process job buffer with background polling from job-manager.

  Jobs are claimed from the database in batches and held in a
  LinkedBlockingQueue until a worker calls take-job!. The poll loop runs
  as a daemon thread and automatically refetches when the buffer depth
  drops below the configured threshold."
  (:require [dev.skivi.job-manager.interface :as job-manager]
            [dev.skivi.queue.schema :as schema])
  (:import [java.util ArrayList]
           [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(def ^:private defaults
  {:forbidden-flags nil
   :poll-interval-ms 2000
   :refetch-delay-ms 1000
   :refetch-threshold 10
   :size 50
   :task-identifiers nil
   :ttl-ms 60000})

(defn- fetch-once!
  "Claims a batch from job-manager into buffer.
  Returns :above-threshold when buffer is full, :no-jobs when none available,
  or the count of jobs added."
  [job-system worker-id ^LinkedBlockingQueue buffer config state]
  (let [{:keys [size refetch-threshold task-identifiers forbidden-flags]} config
        depth (.size buffer)]
    (if (>= depth refetch-threshold)
      :above-threshold
      (let [needed (- size depth)
            jobs   (job-manager/get-jobs
                    job-system
                    worker-id
                    (cond-> {:batch-size needed}
                      task-identifiers (assoc :task-identifiers
                                              task-identifiers)
                      forbidden-flags (assoc :forbidden-flags
                                             forbidden-flags)))]
        (when (seq jobs)
          (let [now (System/currentTimeMillis)]
            (doseq [job jobs]
              (.offer buffer
                      {:buffered-at now
                       :job         job})))
          (swap! state update
            :stats
            #(-> %
                 (update :fetched + (count jobs))
                 (update :refetch-count inc))))
        (if (seq jobs) (count jobs) :no-jobs)))))

(defn- run-poll-loop!
  "Runs until :running? is false in state."
  [job-system worker-id ^LinkedBlockingQueue buffer config state]
  (let [{:keys [poll-interval-ms refetch-delay-ms]} config]
    (loop []
      (when (:running? @state)
        (let [result (try (fetch-once! job-system worker-id buffer config state)
                          (catch Exception _
                            (swap! state update-in [:stats :errors] inc)
                            :error))]
          (Thread/sleep (long (case result
                                :no-jobs poll-interval-ms
                                :above-threshold refetch-delay-ms
                                :error poll-interval-ms
                                refetch-delay-ms))))
        (recur)))))

(defn create-queue
  "Creates a local queue. Call start! to begin polling.
  worker-id is used for all job claims from this queue."
  {:malli/schema
   [:function
    [:=> [:cat schema/JobManagerSystem :string] schema/LocalQueue]
    [:=> [:cat schema/JobManagerSystem :string schema/QueueTuningConfig]
     schema/LocalQueue]]}
  ([job-system worker-id] (create-queue job-system worker-id {}))
  ([job-system worker-id config]
   {:config     (merge defaults config)
    :job-system job-system
    :state      (atom {:buffer      (LinkedBlockingQueue.)
                       :poll-thread nil
                       :running?    false
                       :stats       {:dispatched    0
                                     :errors        0
                                     :fetched       0
                                     :refetch-count 0
                                     :stale-dropped 0}})
    :worker-id  worker-id}))

(defn start!
  "Starts the background polling loop. Throws if already running."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] schema/LocalQueue]]}
  [queue]
  (let [{:keys [job-system worker-id config state]} queue]
    (when (:running? @state)
      (throw (ex-info "Queue already started" {:worker-id worker-id})))
    (swap! state assoc :running? true)
    (let [buf    ^LinkedBlockingQueue (:buffer @state)
          thread (doto (Thread.
                        ^Runnable
                        #(run-poll-loop! job-system worker-id buf config state)
                        "skivi-queue-poll")
                   (.setDaemon true)
                   (.start))]
      (swap! state assoc :poll-thread thread)
      queue)))

(defn stop!
  "Stops polling, waits for the thread, and drains the buffer.
  Returns a vector of jobs that were buffered but not dispatched."
  {:malli/schema [:function
                  [:=> [:cat schema/LocalQueue] [:vector :map]]
                  [:=> [:cat schema/LocalQueue :int] [:vector :map]]]}
  ([queue] (stop! queue 5000))
  ([queue join-timeout-ms]
   (let [{:keys [state]} queue]
     (swap! state assoc :running? false)
     (when-let [thread (:poll-thread @state)]
       (.join ^Thread thread (long join-timeout-ms)))
     (let [buf  ^LinkedBlockingQueue (:buffer @state)
           sink (ArrayList.)]
       (.drainTo buf sink)
       (mapv :job sink)))))

(defn take-job!
  "Blocks until a job is available or timeout-ms elapses.
  Entries older than config :ttl-ms are discarded silently.
  Returns the job map or nil on timeout."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue :int] [:maybe :map]]]}
  [queue timeout-ms]
  (let [{:keys [config state]} queue
        ttl-ms (:ttl-ms config)
        buf    ^LinkedBlockingQueue (:buffer @state)
        end-ms (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [remaining (- end-ms (System/currentTimeMillis))]
        (when (pos? remaining)
          (when-let [entry (.poll buf remaining TimeUnit/MILLISECONDS)]
            (let [age (- (System/currentTimeMillis) ^long (:buffered-at entry))]
              (if (> age ttl-ms)
                (do (swap! state update-in [:stats :stale-dropped] inc) (recur))
                (do (swap! state update-in [:stats :dispatched] inc)
                    (:job entry))))))))))

(defn offer-jobs!
  "Places jobs directly into the buffer without claiming them from the database.
  Useful for re-dispatch or testing."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue [:vector :map]] :nil]]}
  [queue jobs]
  (let [buf ^LinkedBlockingQueue (:buffer @(:state queue))
        now (System/currentTimeMillis)]
    (doseq [job jobs]
      (.offer buf
              {:buffered-at now
               :job         job}))
    nil))

(defn depth
  "Returns the number of jobs currently buffered locally."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] :int]]}
  [queue]
  (.size ^LinkedBlockingQueue (:buffer @(:state queue))))

(defn running?
  "Returns true if the background polling loop is active."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] :boolean]]}
  [queue]
  (boolean (:running? @(:state queue))))

(defn stats
  "Returns a snapshot of queue operational metrics."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] schema/QueueStats]]}
  [queue]
  (:stats @(:state queue)))

(defn worker-id
  "Returns the worker-id used for database job locking."
  {:malli/schema [:function [:=> [:cat schema/LocalQueue] :string]]}
  [queue]
  (:worker-id queue))
