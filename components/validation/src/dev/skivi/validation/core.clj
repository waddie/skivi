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

(ns dev.skivi.validation.core
  (:require [clojure.spec.alpha :as s]
            [malli.core :as m]
            [malli.error :as me]))

(defprotocol PayloadValidator
  "Protocol for validating job payloads against task-specific schemas."
  (validate-payload [this task-identifier payload]
   "Validates payload for task-identifier. Returns payload on success, throws
    ex-info with :task-identifier, :errors, and :payload keys on failure.")
  (valid-payload? [this task-identifier payload]
   "Returns true if payload is valid for task-identifier, false otherwise.")
  (explain-payload [this task-identifier payload]
   "Returns nil if payload is valid, or a humanised error map otherwise."))

(defrecord MalliValidator [schemas]
  PayloadValidator
    (validate-payload [_ task-identifier payload]
      (if-let [schema (get schemas (keyword task-identifier))]
        (if (m/validate schema payload)
          payload
          (throw (ex-info "Invalid payload"
                          {:errors          (-> (m/explain schema payload)
                                                me/humanize)
                           :payload         payload
                           :task-identifier task-identifier})))
        payload))
    (valid-payload? [_ task-identifier payload]
      (if-let [schema (get schemas (keyword task-identifier))]
        (m/validate schema payload)
        true))
    (explain-payload [_ task-identifier payload]
      (when-let [schema (get schemas (keyword task-identifier))]
        (when-not (m/validate schema payload)
          (-> (m/explain schema payload)
              me/humanize)))))

(defrecord SpecValidator [specs]
  PayloadValidator
    (validate-payload [_ task-identifier payload]
      (if-let [spec (get specs (keyword task-identifier))]
        (if (s/valid? spec payload)
          payload
          (throw (ex-info "Invalid payload"
                          {:errors          (s/explain-data spec payload)
                           :payload         payload
                           :task-identifier task-identifier})))
        payload))
    (valid-payload? [_ task-identifier payload]
      (if-let [spec (get specs (keyword task-identifier))]
        (s/valid? spec payload)
        true))
    (explain-payload [_ task-identifier payload]
      (when-let [spec (get specs (keyword task-identifier))]
        (when-not (s/valid? spec payload) (s/explain-data spec payload)))))

(defrecord CompositeValidator [validators]
  PayloadValidator
    (validate-payload [_ task-identifier payload]
      (reduce (fn [p v] (validate-payload v task-identifier p))
              payload
              validators))
    (valid-payload? [_ task-identifier payload]
      (every? #(valid-payload? % task-identifier payload) validators))
    (explain-payload [_ task-identifier payload]
      (first (keep #(explain-payload % task-identifier payload) validators))))

(defrecord NoOpValidator []
  PayloadValidator
    (validate-payload [_ _ payload] payload)
    (valid-payload? [_ _ _] true)
    (explain-payload [_ _ _] nil))

(defn malli-validator
  "Creates a validator that uses malli schemas keyed by task-identifier keyword."
  {:malli/schema [:function [:=> [:cat [:map-of :keyword :any]] :any]]}
  [schemas]
  (->MalliValidator schemas))

(defn spec-validator
  "Creates a validator that uses clojure.spec specs keyed by task-identifier keyword."
  {:malli/schema [:function [:=> [:cat [:map-of :keyword :any]] :any]]}
  [specs]
  (->SpecValidator specs))

(defn composite-validator
  "Creates a validator that applies validators in order, threading payload through each."
  {:malli/schema [:function [:=> [:cat [:sequential :any]] :any]]}
  [validators]
  (->CompositeValidator validators))

(defn noop-validator
  "Creates a no-op validator that accepts all payloads without validation."
  {:malli/schema [:function [:=> [:cat] :any]]}
  []
  (->NoOpValidator))

(defn create-validator
  "Creates a validator from config. Reads [:schema :job-schemas], partitioning
  entries by value type: keyword values are treated as clojure.spec spec names,
  all other values are treated as malli schemas. Returns a no-op validator when
  :validate-payloads is false or no schemas are defined."
  {:malli/schema [:function [:=> [:cat :map] :any]]}
  [config]
  (let [validate?   (get-in config [:schema :validate-payloads] true)
        job-schemas (get-in config [:schema :job-schemas] {})]
    (if (or (not validate?) (empty? job-schemas))
      (->NoOpValidator)
      (let [{malli-schemas false
             spec-schemas  true}
            (group-by (comp keyword? val) job-schemas)
            validators (cond-> []
                         (seq malli-schemas) (conj (->MalliValidator
                                                    (into {} malli-schemas)))
                         (seq spec-schemas) (conj (->SpecValidator
                                                   (into {} spec-schemas))))]
        (if (= 1 (count validators))
          (first validators)
          (->CompositeValidator validators))))))
