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

(ns dev.skivi.config.interface
  "Configuration management interface with malli schema metadata.

  All functions have :malli/schema metadata for runtime validation.
  To enable instrumentation:

    (require '[malli.instrument :as mi])
    (mi/instrument!)  ; instruments all functions with :malli/schema metadata
    (mi/unstrument!)  ; removes instrumentation"
  (:require [dev.skivi.config.schema :as schema]
            [dev.skivi.config.core :as core]))

;; Re-export schema for consumers that need it
(def Config schema/Config)

(defn load-config
  "Reads and validates configuration from filename using aero. With no arguments,
  loads configuration with the following precedence:

  1. Checks for 'skivi-config.edn' (dedicated file, root-level config)
  2. Falls back to :skivi section in 'config.edn' (embedded in shared config)

  Opts map may include :profile for environment-specific config.
  Throws if configuration is invalid or not found."
  {:malli/schema [:function
                  [:=> [:cat] schema/Config]
                  [:=> [:cat :string] schema/Config]
                  [:=> [:cat :string :map] schema/Config]]}
  ([] (core/load-config))
  ([filename] (core/load-config filename))
  ([filename opts] (core/load-config filename opts)))

(defn validate-config
  "Returns config if it conforms to the Config schema, otherwise throws ex-info
  with humanised validation errors."
  {:malli/schema [:=> [:cat :any] schema/Config]}
  [config]
  (core/validate-config config))

(defn reload-config!
  "Reloads configuration and updates system. System can be:
   - Map with :config atom key (returns updated system map)
   - Atom directly (returns dereferenced new config)
   - nil (returns new config without updating anything)

  Optional filename parameter specifies which config file to reload."
  {:malli/schema [:function
                  [:=> [:cat :any] :any]
                  [:=> [:cat :any :string] :any]]}
  ([system] (core/reload-config! system))
  ([system filename] (core/reload-config! system filename)))

(defn get-config
  "Returns the current configuration from the dynamic var. Throws if configuration
  has not been loaded."
  {:malli/schema [:=> [:cat] schema/Config]}
  []
  (core/get-config))

(defn with-config
  "Evaluates f with config bound to the dynamic configuration var. Returns the
  result of calling f."
  {:malli/schema [:=> [:cat schema/Config ifn?] :any]}
  [config f]
  (core/with-config config f))

;; Explicit-first API - primary library functions
(defn database-config
  "Returns the database configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/DatabaseConfig]}
  [config]
  (core/database-config config))

(defn worker-config
  "Returns the worker configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/WorkerConfig]}
  [config]
  (core/worker-config config))

(defn queue-config
  "Returns the queue configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/QueueConfig]}
  [config]
  (core/queue-config config))

(defn scheduler-config
  "Returns the scheduler/cron configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/CronConfig]}
  [config]
  (core/scheduler-config config))

(defn monitoring-config
  "Returns the monitoring configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/MonitoringConfig]}
  [config]
  (core/monitoring-config config))

(defn schema-name
  "Returns the database schema name string from config."
  {:malli/schema [:=> [:cat schema/Config] :string]}
  [config]
  (core/schema-name config))

(defn logging-config
  "Returns the logging configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/LoggingConfig]}
  [config]
  (core/logging-config config))

(defn retry-config
  "Returns the retry configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/RetryConfig]}
  [config]
  (core/retry-config config))

(defn cleanup-config
  "Returns the cleanup configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/CleanupConfig]}
  [config]
  (core/cleanup-config config))

;; Dynamic var convenience API - for REPL/app use
(defn database-config*
  "Returns the database configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/DatabaseConfig]}
  []
  (core/database-config*))

(defn worker-config*
  "Returns the worker configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/WorkerConfig]}
  []
  (core/worker-config*))

(defn queue-config*
  "Returns the queue configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/QueueConfig]}
  []
  (core/queue-config*))

(defn scheduler-config*
  "Returns the scheduler/cron configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/CronConfig]}
  []
  (core/scheduler-config*))

(defn monitoring-config*
  "Returns the monitoring configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/MonitoringConfig]}
  []
  (core/monitoring-config*))

(defn schema-name*
  "Returns the database schema name string from the dynamic var."
  {:malli/schema [:=> [:cat] :string]}
  []
  (core/schema-name*))

(defn logging-config*
  "Returns the logging configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/LoggingConfig]}
  []
  (core/logging-config*))

(defn retry-config*
  "Returns the retry configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/RetryConfig]}
  []
  (core/retry-config*))

(defn cleanup-config*
  "Returns the cleanup configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/CleanupConfig]}
  []
  (core/cleanup-config*))
