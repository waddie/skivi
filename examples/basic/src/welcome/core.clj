(ns welcome.core
  (:require [dev.skivi.config.interface :as config]
            [dev.skivi.skivi.core :as skivi]
            [welcome.tasks :as tasks]))

(defonce system (atom nil))

(defn start!
  []
  (reset! system (-> (config/load-config)
                     (skivi/create-system tasks/registry)
                     skivi/start!)))

(defn stop!
  []
  (when-let [s @system]
    (skivi/stop! s)
    (reset! system nil)))

(defn on-user-registered!
  "Enqueues a welcome email job. Call this after inserting the user row."
  [{:keys [email name]}]
  (when-not @system
    (throw (ex-info "System not started - call start! first" {})))
  (skivi/add-job @system
                 "send-welcome-email"
                 {:name name
                  :to   email}))
