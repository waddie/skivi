(ns welcome.tasks)

(defn send-welcome-email
  [{:keys [job]}]
  (let [{:keys [to name]} (:payload job)]
    ;; Replace with your actual email delivery call.
    (println (format "Sending welcome email to %s (%s)" to name))))

(def registry {"send-welcome-email" send-welcome-email})
