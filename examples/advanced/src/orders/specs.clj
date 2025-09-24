(ns orders.specs
  (:require [clojure.spec.alpha :as s]))

(s/def ::order-id (s/and string? seq))
(s/def ::customer-id (s/and string? seq))
(s/def ::amount (s/and int? pos?))
(s/def ::email (s/and string? #(re-matches #".+@.+\..+" %)))
(s/def ::payment-method (s/and string? seq))
(s/def ::date (s/and string? #(re-matches #"\d{4}-\d{2}-\d{2}" %)))
(s/def ::items (s/and coll? seq))

(s/def ::process-payment-payload
  (s/keys :req-un [::order-id ::amount ::customer-id ::payment-method]))

(s/def ::send-confirmation-payload
  (s/keys :req-un [::order-id ::email ::items]))

(s/def ::reconcile-daily-payload (s/keys :req-un [::date]))
