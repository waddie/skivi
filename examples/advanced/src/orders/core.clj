(ns orders.core
  (:require [dev.skivi.config.interface :as config]
            [dev.skivi.database.interface :as database]
            [dev.skivi.monitoring.interface :as monitoring]
            [dev.skivi.skivi.core :as skivi]
            [orders.cron :as cron]
            [orders.specs]
            [orders.tasks :as tasks]))

(defonce system (atom nil))

(defn- setup-rate-limits
  [sys]
  ;; Payment processor allows 100 calls per minute.
  (database/register-rate-limit (:pool sys) "payment-processor" 100 "1 minute")
  sys)

(defn- setup-monitoring
  [sys]
  (monitoring/on (:emitter sys)
                 :job/exhausted
                 (fn [{:keys [data]}]
                   (when (= "process-payment" (:task-identifier data))
                     ;; Replace with your alerting call.
                     (println (format "ALERT: Payment exhausted for order %s"
                                      (get-in data [:payload :order-id]))))))
  (monitoring/on (:emitter sys)
                 :all
                 (fn [{:keys [type data]}]
                   (println (format "[%s] %s" (name type) data))))
  sys)

(defn start!
  []
  (reset! system (-> (config/load-config)
                     (skivi/create-system tasks/registry cron/tabs)
                     skivi/start!
                     setup-rate-limits
                     setup-monitoring)))

(defn stop!
  []
  (when-let [s @system]
    (skivi/stop! s)
    (reset! system nil)))

(defn place-order!
  "Atomically enqueues payment processing and confirmation email for an order.
  Both jobs are added in a single transaction - either both are queued or neither."
  [{:as   order
    :keys [id customer-id email amount items payment-method]}]
  (when-not @system
    (throw (ex-info "System not started - call start! first" {:order order})))
  (skivi/add-jobs @system
                  [{:job-key         id
                    :job-key-mode    :unsafe-dedupe
                    :payload         {:amount         amount
                                      :customer-id    customer-id
                                      :order-id       id
                                      :payment-method payment-method}
                    :queue-name      "payments"
                    :rate-limit-key  "payment-processor"
                    :task-identifier "process-payment"}
                   {:payload         {:email    email
                                      :items    items
                                      :order-id id}
                    :task-identifier "send-confirmation"}]))
