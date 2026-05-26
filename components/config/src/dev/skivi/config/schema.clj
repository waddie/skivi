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

(ns dev.skivi.config.schema "Malli schemas for configuration validation")

;; Sub-schemas
(def DatabasePoolConfig
  [:map
   [:maximum-pool-size pos-int?]
   [:minimum-idle nat-int?]
   [:connection-timeout pos-int?]
   [:idle-timeout pos-int?]
   [:max-lifetime pos-int?]])

(def DatabaseConfig
  [:map
   [:connection-string string?]
   [:username string?]
   [:password string?]
   [:pool-config DatabasePoolConfig]])

(def WorkerConfig
  [:map
   [:concurrency pos-int?]
   [:poll-interval pos-int?]
   [:task-directory {:optional true} string?]
   [:file-extensions {:optional true} [:vector string?]]])

(def LocalQueueConfig
  [:map
   [:size pos-int?]
   [:ttl pos-int?]])

(def QueueConfig
  [:map
   [:local-queue LocalQueueConfig]])

(def SchemaConfig
  [:map
   [:name string?]])

(def CronConfig
  [:map
   [:enabled boolean?]
   [:file string?]
   [:timezone string?]
   [:default-backfill-period pos-int?]])

(def EventsConfig
  [:map
   [:enabled boolean?]
   [:buffer-size pos-int?]])

(def MetricsConfig
  [:map
   [:enabled boolean?]
   [:registry [:maybe any?]]])

(def HealthCheckConfig
  [:map
   [:enabled boolean?]
   [:port pos-int?]])

(def MonitoringConfig
  [:map
   [:events EventsConfig]
   [:metrics MetricsConfig]
   [:health-check HealthCheckConfig]])

(def LoggingConfig
  [:map
   [:level keyword?]
   [:format keyword?]
   [:include-mdc boolean?]])

(def RetryConfig
  [:map
   [:strategy keyword?]
   [:base-delay pos-int?]
   [:max-delay pos-int?]
   [:multiplier pos?]
   [:jitter [:and number? [:>= 0] [:<= 1]]]
   [:max-attempts pos-int?]])

(def CleanupConfig
  [:map
   [:enabled boolean?]
   [:maintenance-interval-ms pos-int?]
   [:schedule string?]
   [:tasks [:vector keyword?]]
   [:retention-periods [:map-of keyword? string?]]])

;; Main configuration schema
(def Config
  "Schema for validating the entire system configuration.

  Required sections: database, worker, queue, schema
  Optional sections: cron, monitoring, logging, retry, cleanup (defaults provided)"
  [:map
   ;; Required sections - must be provided by user
   [:database DatabaseConfig]
   [:worker WorkerConfig]
   [:queue QueueConfig]
   [:schema SchemaConfig]

   ;; Optional sections - defaults provided if not specified
   [:cron {:optional true} CronConfig]
   [:monitoring {:optional true} MonitoringConfig]
   [:logging {:optional true} LoggingConfig]
   [:retry {:optional true} RetryConfig]
   [:cleanup {:optional true} CleanupConfig]])
