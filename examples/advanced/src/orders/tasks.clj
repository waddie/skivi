(ns orders.tasks)

(defn process-payment
  [{:keys [job]}]
  (let [{:keys [order-id amount customer-id]} (:payload job)]
    ;; Replace with your actual payment processor call.
    (println (format "Charging %s for order %s (£%.2f)"
                     customer-id
                     order-id
                     (/ amount 100.0)))))

(defn send-confirmation
  [{:keys [job]}]
  (let [{:keys [order-id email]} (:payload job)]
    ;; Replace with your actual email delivery call.
    (println (format "Confirmation sent to %s for order %s" email order-id))))

(defn reconcile-daily
  [{:keys [job]}]
  (let [{:keys [date]} (:payload job)]
    ;; Fetch yesterday's orders and compare with payment processor records.
    (println (format "Reconciling orders for %s" date))))

(def registry
  {"process-payment"   process-payment
   "reconcile-daily"   reconcile-daily
   "send-confirmation" send-confirmation})
