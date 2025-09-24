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

(ns dev.skivi.config.core
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [dev.skivi.config.schema :as schema]
            [malli.core :as m]
            [malli.error :as me]))

;; Default configuration values
(def default-config
  "Default configuration values for optional sections. These are merged with
  user-provided config to ensure sensible defaults."
  {:cleanup    {:enabled  true
                :maintenance-interval-ms 60000
                :retention-periods {:failed-jobs      "30 days"
                                    :task-identifiers "7 days"}
                :schedule "0 3 * * *"
                :tasks    [:gc-task-identifiers :gc-job-queues
                           :gc-job-history]}
   :cron       {:default-backfill-period 3600000
                :enabled  false
                :file     "crontab.edn"
                :timezone "UTC"}
   :logging    {:format      :json
                :include-mdc true
                :level       :info}
   :monitoring {:events       {:buffer-size 1000
                               :enabled     true}
                :health-check {:enabled true
                               :port    3000}
                :metrics      {:enabled  false
                               :registry nil}}
   :queue      {:local-queue {:size 50
                              :ttl  60000}}
   :retry      {:base-delay   1000
                :jitter       0.1
                :max-attempts 25
                :max-delay    3600000
                :multiplier   2.0
                :strategy     :exponential-backoff}
   :schema     {:name "skivi"}
   :worker     {:concurrency   10
                :poll-interval 2000}})

(defn merge-with-defaults
  "Merges user config with default config. Required sections (database, worker,
  queue, schema) must be provided by user. Optional sections are merged with defaults."
  [config]
  (let [;; Deep merge function for nested maps
        deep-merge
        (fn deep-merge [a b]
          (if (and (map? a) (map? b)) (merge-with deep-merge a b) b))]
    (deep-merge default-config config)))

(defn load-config
  "Reads and validates configuration from filename using aero. With no arguments,
  loads configuration with the following precedence:

  1. Checks for 'skivi-config.edn' (dedicated file, root-level config)
  2. Falls back to :skivi section in 'config.edn' (embedded in shared config)

  Opts map may include :profile for environment-specific config.
  Throws if configuration is invalid or not found."
  ([]
   ;; Try dedicated file first, fall back to shared config
   (if-let [_ (io/resource "skivi-config.edn")]
     (load-config "skivi-config.edn" {})
     (load-config "config.edn" {:extract-skivi-section true})))
  ([filename] (load-config filename {}))
  ([filename opts]
   (let [profile     (or (:profile opts)
                         (keyword (System/getProperty "skivi.profile" "dev")))
         config-opts (merge {:profile profile} opts)
         resource    (io/resource filename)]
     (when-not resource
       (throw
        (ex-info
         "Configuration file not found"
         {:filename filename
          :note
          "For embedded use, ensure config.edn contains :skivi section"})))
     (let
       [raw-config (aero/read-config resource config-opts)
        ;; Extract :skivi section if this is a shared config file
        extracted-config
        (if (:extract-skivi-section opts)
          (or
           (:skivi raw-config)
           (throw
            (ex-info
             "No :skivi section found in config file"
             {:filename filename
              :note
              "Add {:skivi {...}} section or use dedicated skivi-config.edn"})))
          raw-config)
        ;; Merge with defaults
        config (merge-with-defaults extracted-config)]
       (if (m/validate schema/Config config)
         config
         (throw (ex-info "Invalid configuration"
                         {:errors   (-> (m/explain schema/Config config)
                                        (me/humanize))
                          :filename filename
                          :profile  profile})))))))

(defn validate-config
  "Returns config if it conforms to the Config schema, otherwise throws ex-info
  with humanised validation errors."
  {:malli/schema [:=> [:cat :any] schema/Config]}
  [config]
  (if (m/validate schema/Config config)
    config
    (throw (ex-info "Invalid configuration"
                    {:errors (-> (m/explain schema/Config config)
                                 (me/humanize))}))))

(defn reload-config!
  "Reloads configuration and updates system. System can be:
   - Map with :config atom key — the :config value must be a clojure.lang.Atom;
     returns the updated system map. Note: create-system does not wrap config
     in an atom by default. Callers who want hot-reload must store config in an
     atom themselves and pass the system map here.
   - Atom directly (returns dereferenced new config)
   - nil (returns new config without updating anything)

  Optional filename parameter specifies which config file to reload."
  {:malli/schema [:function
                  [:=> [:cat :any] :any]
                  [:=> [:cat :any :string] :any]]}
  ([system] (reload-config! system nil))
  ([system filename]
   (let [new-config (if filename (load-config filename) (load-config))]
     (cond
       ;; System map with :config atom key
       (and (map? system) (instance? clojure.lang.Atom (:config system)))
       (do (reset! (:config system) new-config) system)
       ;; Direct atom
       (instance? clojure.lang.Atom system) (do (reset! system new-config)
                                                @system)
       ;; No system - just return new config
       :else new-config))))

(def ^:dynamic *config* nil)

(defn get-config
  "Returns the current configuration from the dynamic var. Throws if configuration
  has not been loaded."
  {:malli/schema [:=> [:cat] schema/Config]}
  []
  (or *config* (throw (ex-info "Configuration not loaded" {}))))

(defn with-config
  "Evaluates f with config bound to the dynamic configuration var. Returns the
  result of calling f."
  {:malli/schema [:=> [:cat schema/Config ifn?] :any]}
  [config f]
  (binding [*config* config]
    (f)))

;; Explicit-first API - primary library functions
(defn database-config
  "Returns the database configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/DatabaseConfig]}
  [config]
  (:database config))

(defn worker-config
  "Returns the worker configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/WorkerConfig]}
  [config]
  (:worker config))

(defn queue-config
  "Returns the queue configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/QueueConfig]}
  [config]
  (:queue config))

(defn scheduler-config
  "Returns the scheduler/cron configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/CronConfig]}
  [config]
  (:cron config))

(defn monitoring-config
  "Returns the monitoring configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/MonitoringConfig]}
  [config]
  (:monitoring config))

(defn schema-name
  "Returns the database schema name string from config."
  {:malli/schema [:=> [:cat schema/Config] :string]}
  [config]
  (get-in config [:schema :name] "skivi"))

(defn logging-config
  "Returns the logging configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/LoggingConfig]}
  [config]
  (:logging config))

(defn retry-config
  "Returns the retry configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/RetryConfig]}
  [config]
  (:retry config))

(defn cleanup-config
  "Returns the cleanup configuration map from config."
  {:malli/schema [:=> [:cat schema/Config] schema/CleanupConfig]}
  [config]
  (:cleanup config))

;; Dynamic var convenience API - for REPL/app use
(defn database-config*
  "Returns the database configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/DatabaseConfig]}
  []
  (database-config (get-config)))

(defn worker-config*
  "Returns the worker configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/WorkerConfig]}
  []
  (worker-config (get-config)))

(defn queue-config*
  "Returns the queue configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/QueueConfig]}
  []
  (queue-config (get-config)))

(defn scheduler-config*
  "Returns the scheduler/cron configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/CronConfig]}
  []
  (scheduler-config (get-config)))

(defn monitoring-config*
  "Returns the monitoring configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/MonitoringConfig]}
  []
  (monitoring-config (get-config)))

(defn schema-name*
  "Returns the database schema name string from the dynamic var."
  {:malli/schema [:=> [:cat] :string]}
  []
  (schema-name (get-config)))

(defn logging-config*
  "Returns the logging configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/LoggingConfig]}
  []
  (logging-config (get-config)))

(defn retry-config*
  "Returns the retry configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/RetryConfig]}
  []
  (retry-config (get-config)))

(defn cleanup-config*
  "Returns the cleanup configuration map from the dynamic var."
  {:malli/schema [:=> [:cat] schema/CleanupConfig]}
  []
  (cleanup-config (get-config)))
