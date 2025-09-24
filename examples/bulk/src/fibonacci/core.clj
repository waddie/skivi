(ns fibonacci.core
  (:require [dev.skivi.config.interface :as config]
            [dev.skivi.monitoring.interface :as monitoring]
            [dev.skivi.skivi.core :as skivi]
            [fibonacci.tasks :as tasks]))

(defonce system (atom nil))

(def ^:private job-count 3000)
(def ^:private first-n 3)
(def ^:private last-n (+ first-n job-count))

(defn- attach-progress-listener!
  [sys]
  (let [counter (atom 0)
        started (atom nil)]
    (monitoring/on
     (:emitter sys)
     :job/completed
     (fn [_]
       (let [n (swap! counter inc)]
         (when (= n 1) (reset! started (System/currentTimeMillis)))
         (when (zero? (mod n 1000))
           (let [elapsed (/ (- (System/currentTimeMillis) @started) 1000.0)
                 rate    (/ n elapsed)]
             (println (format "  %,7d / %,d  (%.0f jobs/s, %.1fs elapsed)"
                              n
                              job-count
                              rate
                              elapsed))))))))
  sys)

(defn start!
  []
  (reset! system (-> (config/load-config)
                     (skivi/create-system tasks/registry)
                     skivi/start!
                     attach-progress-listener!)))

(defn stop!
  []
  (when-let [s @system]
    (skivi/stop! s)
    (reset! system nil)))

(defn stats
  []
  (when-let [s @system]
    (skivi/worker-pool-stats s)))

(defn- enqueue!
  [sys output-dir rate-limit-key]
  (println (format "Enqueueing %,d jobs → %s" job-count output-dir))
  (let [start     (System/currentTimeMillis)
        job-specs (for [n (range first-n last-n)]
                    (cond-> {:payload         {:n n
                                               :output-dir output-dir}
                             :task-identifier "write-fibonacci"}
                      rate-limit-key (assoc :rate-limit-key rate-limit-key)))]
    (doseq [batch (partition-all 500 job-specs)]
      (skivi/add-jobs sys (vec batch)))
    (println (format "Enqueued in %.1fs. Workers are processing..."
                     (/ (- (System/currentTimeMillis) start) 1000.0)))))

(defn run-fast!
  "Enqueues 3,000 Fibonacci jobs with no rate limiting."
  ([] (run-fast! "output/fast"))
  ([output-dir]
   (when-not @system (throw (ex-info "System not started" {})))
   (enqueue! @system output-dir nil)))

(defn run-throttled!
  "Enqueues 3,000 Fibonacci jobs limited to 600 per minute."
  ([] (run-throttled! "output/throttled"))
  ([output-dir]
   (when-not @system (throw (ex-info "System not started" {})))
   (skivi/register-rate-limit @system "fibonacci-throttle" 600 "1 minute")
   (enqueue! @system output-dir "fibonacci-throttle")))
