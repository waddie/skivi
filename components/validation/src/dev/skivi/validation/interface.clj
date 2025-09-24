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

(ns dev.skivi.validation.interface
  "Job payload validation interface supporting malli schemas, clojure.spec specs,
  and composite chains of validators.

  Typical usage:

    ;; From config (malli schemas or clojure.spec keywords in [:schema :job-schemas])
    (def v (create-validator config))

    ;; Directly
    (def v (malli-validator {:send-email [:map [:to :string] [:subject :string]]}))

    ;; Validate
    (validate-payload v :send-email {:to \"x@y.com\" :subject \"Hi\"})

  To enable malli instrumentation of this namespace:

    (require '[malli.instrument :as mi])
    (mi/instrument!)
    (mi/unstrument!)"
  (:require [dev.skivi.validation.core :as core]))

(defn validate-payload
  "Validates payload for task-identifier. Returns payload on success, throws
  ex-info with :task-identifier, :errors, and :payload keys on failure.
  Passes payload through unchanged when no schema is registered for the task."
  {:malli/schema [:=> [:cat :any :any :any] :any]}
  [validator task-identifier payload]
  (core/validate-payload validator task-identifier payload))

(defn valid-payload?
  "Returns true if payload is valid for task-identifier, false otherwise.
  Returns true when no schema is registered for the task."
  {:malli/schema [:=> [:cat :any :any :any] :boolean]}
  [validator task-identifier payload]
  (core/valid-payload? validator task-identifier payload))

(defn explain-payload
  "Returns nil if payload is valid, or a humanised error map otherwise.
  Returns nil when no schema is registered for the task."
  {:malli/schema [:=> [:cat :any :any :any] [:maybe :any]]}
  [validator task-identifier payload]
  (core/explain-payload validator task-identifier payload))

(defn malli-validator
  "Creates a validator that uses malli schemas keyed by task-identifier keyword.
  schemas is a map of keyword to malli schema, e.g. {:my-task [:map [:id :int]]}."
  {:malli/schema [:=> [:cat [:map-of :keyword :any]] :any]}
  [schemas]
  (core/malli-validator schemas))

(defn spec-validator
  "Creates a validator that uses clojure.spec specs keyed by task-identifier keyword.
  specs is a map of keyword to spec, e.g. {:my-task ::my-spec}."
  {:malli/schema [:=> [:cat [:map-of :keyword :any]] :any]}
  [specs]
  (core/spec-validator specs))

(defn composite-validator
  "Creates a validator that applies validators in order, threading payload through
  each. Fails on the first validator that rejects the payload."
  {:malli/schema [:=> [:cat [:sequential :any]] :any]}
  [validators]
  (core/composite-validator validators))

(defn noop-validator
  "Creates a no-op validator that accepts all payloads without validation."
  {:malli/schema [:=> [:cat] :any]}
  []
  (core/noop-validator))

(defn create-validator
  "Creates a validator from config. Reads [:schema :job-schemas], treating keyword
  values as clojure.spec spec names and all other values as malli schemas.
  Returns a no-op validator when :validate-payloads is false or no schemas are defined."
  {:malli/schema [:=> [:cat :map] :any]}
  [config]
  (core/create-validator config))
