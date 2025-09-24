(ns orders.cron)

(def tabs
  [{:identifier "reconcile-daily"
    :schedule   "0 2 * * *"
    :spec       {:max-attempts 3
                 :queue-name   "maintenance"}}])
