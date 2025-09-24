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

(ns dev.skivi.config.interface-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.config.interface :as config]
            [dev.skivi.config.core :as core]
            [malli.instrument :as mi]))

;; Enable malli instrumentation for all tests
(use-fixtures :once (fn [f] (mi/instrument!) (f) (mi/unstrument!)))

;; =============================================================================
;; File Resolution Tests
;; =============================================================================

(deftest load-config-standalone-mode-test
  (testing "Loading valid standalone config file"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/valid-standalone.edn")]
      (is (map? cfg))
      (is (= "postgresql://localhost:5432/test_db"
             (get-in cfg [:database :connection-string])))
      (is (= 4 (get-in cfg [:worker :concurrency])))
      (is (= "job_system" (get-in cfg [:schema :name]))))))

(deftest load-config-embedded-mode-test
  (testing "Loading valid embedded config with :skivi section"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/valid-embedded.edn"
               {:extract-skivi-section true})]
      (is (map? cfg))
      (is (= "postgresql://localhost:5432/test_db"
             (get-in cfg [:database :connection-string])))
      (is (= 4 (get-in cfg [:worker :concurrency])))
      ;; Should not have app-level keys
      (is (nil? (:app/database cfg)))
      (is (nil? (:app/web-server cfg))))))

(deftest load-config-missing-file-test
  (testing "Loading non-existent config file throws error"
    (is (thrown-with-msg? Exception
                          #"Configuration file not found"
                          (config/load-config "non-existent.edn")))))

(deftest load-config-missing-skivi-section-test
  (testing "Loading embedded config without :skivi section throws error"
    (is (thrown-with-msg?
         Exception
         #"No :skivi section found"
         (config/load-config
          "dev/skivi/config/test_resources/embedded-no-skivi.edn"
          {:extract-skivi-section true})))))

;; =============================================================================
;; Schema Validation Tests
;; =============================================================================

(deftest validate-config-valid-test
  (testing "Validating a valid configuration succeeds"
    (let [valid-cfg {:database {:connection-string
                                "postgresql://localhost:5432/test"
                                :pool-config {:connection-timeout 30000
                                              :idle-timeout       600000
                                              :max-lifetime       1800000
                                              :maximum-pool-size  10
                                              :minimum-idle       2}}
                     :queue    {:batch-complete-delay 50
                                :batch-fail-delay     50
                                :local-queue          {:enabled true
                                                       :refetch-threshold 10
                                                       :size    50
                                                       :ttl     60000}}
                     :schema   {:job-schemas {}
                                :name        "test_schema"
                                :validate-payloads true}
                     :worker   {:concurrency            4
                                :file-extensions        [".clj"]
                                :graceful-shutdown-timeout 30000
                                :max-job-execution-time 300000
                                :poll-interval          2000
                                :task-directory         "tasks"}}]
      (is (= valid-cfg (config/validate-config valid-cfg))))))

(deftest validate-config-invalid-test
  (testing "Validating an invalid configuration throws error"
    (is (thrown-with-msg?
         Exception
         #"Invalid configuration"
         (config/load-config
          "dev/skivi/config/test_resources/invalid-config.edn")))))

(deftest validate-config-missing-required-sections-test
  (testing "Configuration missing required sections is invalid"
    (let [incomplete-cfg {:database {:connection-string
                                     "postgresql://localhost:5432/test"
                                     :pool-config {:connection-timeout 30000
                                                   :idle-timeout       600000
                                                   :max-lifetime       1800000
                                                   :maximum-pool-size  10
                                                   :minimum-idle       2}}}]
      (is (thrown-with-msg? Exception
                            #"Invalid configuration"
                            (config/validate-config incomplete-cfg))))))

(deftest validate-config-optional-sections-test
  (testing "Configuration with only required sections is valid"
    (let [minimal-cfg (config/load-config
                       "dev/skivi/config/test_resources/minimal-config.edn")]
      (is (map? minimal-cfg))
      ;; Should have defaults for optional sections
      (is (contains? minimal-cfg :cron))
      (is (contains? minimal-cfg :monitoring))
      (is (contains? minimal-cfg :logging))
      (is (contains? minimal-cfg :retry))
      (is (contains? minimal-cfg :cleanup)))))

;; =============================================================================
;; Default Merging Tests
;; =============================================================================

(deftest merge-with-defaults-full-config-test
  (testing "Full config with all sections preserves user values"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/config-with-optional.edn")]
      ;; User-specified values should be preserved
      (is (= true (get-in cfg [:cron :enabled])))
      (is (= "custom-crontab.edn" (get-in cfg [:cron :file])))
      (is (= "America/New_York" (get-in cfg [:cron :timezone])))
      (is (= false (get-in cfg [:monitoring :events :enabled])))
      (is (= 2000 (get-in cfg [:monitoring :events :buffer-size])))
      (is (= :debug (get-in cfg [:logging :level])))
      (is (= :pretty (get-in cfg [:logging :format])))
      (is (= :linear (get-in cfg [:retry :strategy])))
      (is (= 500 (get-in cfg [:retry :base-delay])))
      (is (= false (get-in cfg [:cleanup :enabled]))))))

(deftest merge-with-defaults-minimal-config-test
  (testing "Minimal config gets sensible defaults for optional sections"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/minimal-config.edn")]
      ;; Defaults should be applied
      (is (= false (get-in cfg [:cron :enabled])))
      (is (= "crontab.edn" (get-in cfg [:cron :file])))
      (is (= "UTC" (get-in cfg [:cron :timezone])))
      (is (= true (get-in cfg [:monitoring :events :enabled])))
      (is (= 1000 (get-in cfg [:monitoring :events :buffer-size])))
      (is (= false (get-in cfg [:monitoring :metrics :enabled])))
      (is (= :info (get-in cfg [:logging :level])))
      (is (= :json (get-in cfg [:logging :format])))
      (is (= :exponential-backoff (get-in cfg [:retry :strategy])))
      (is (= 1000 (get-in cfg [:retry :base-delay])))
      (is (= 3600000 (get-in cfg [:retry :max-delay])))
      (is (= true (get-in cfg [:cleanup :enabled]))))))

(deftest merge-with-defaults-deep-merge-test
  (testing "Deep merge preserves nested user values while adding defaults"
    (let [partial-monitoring {:monitoring {:events {:enabled false}}}
          base-cfg {:database {:connection-string
                               "postgresql://localhost:5432/test"
                               :pool-config {:connection-timeout 30000
                                             :idle-timeout       600000
                                             :max-lifetime       1800000
                                             :maximum-pool-size  10
                                             :minimum-idle       2}}
                    :queue    {:batch-complete-delay 50
                               :batch-fail-delay     50
                               :local-queue          {:enabled true
                                                      :refetch-threshold 10
                                                      :size    50
                                                      :ttl     60000}}
                    :schema   {:job-schemas {}
                               :name        "test"
                               :validate-payloads true}
                    :worker   {:concurrency            4
                               :file-extensions        [".clj"]
                               :graceful-shutdown-timeout 30000
                               :max-job-execution-time 300000
                               :poll-interval          2000
                               :task-directory         "tasks"}}
          merged   (core/merge-with-defaults (merge base-cfg
                                                    partial-monitoring))]
      ;; User's monitoring.events.enabled should be preserved
      (is (= false (get-in merged [:monitoring :events :enabled])))
      ;; But default buffer-size should be added
      (is (= 1000 (get-in merged [:monitoring :events :buffer-size])))
      ;; Other monitoring defaults should be present
      (is (= false (get-in merged [:monitoring :metrics :enabled])))
      (is (= true (get-in merged [:monitoring :health-check :enabled]))))))

;; =============================================================================
;; API Accessor Tests
;; =============================================================================

(deftest explicit-api-accessors-test
  (testing "All explicit config accessor functions work correctly"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/valid-standalone.edn")]
      (is (= "postgresql://localhost:5432/test_db"
             (:connection-string (config/database-config cfg))))
      (is (= 4 (:concurrency (config/worker-config cfg))))
      (is (= true (get-in (config/queue-config cfg) [:local-queue :enabled])))
      (is (= false (:enabled (config/scheduler-config cfg))))
      (is (= true (get-in (config/monitoring-config cfg) [:events :enabled])))
      (is (= "job_system" (config/schema-name cfg)))
      (is (= :info (:level (config/logging-config cfg))))
      (is (= :exponential-backoff (:strategy (config/retry-config cfg))))
      (is (= true (:enabled (config/cleanup-config cfg)))))))

(deftest dynamic-var-api-test
  (testing "Dynamic var convenience API works with with-config"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/valid-standalone.edn")]
      (config/with-config
       cfg
       (fn []
         (is (= cfg (config/get-config)))
         (is (= "postgresql://localhost:5432/test_db"
                (:connection-string (config/database-config*))))
         (is (= 4 (:concurrency (config/worker-config*))))
         (is (= "job_system" (config/schema-name*)))
         (is (= :info (:level (config/logging-config*))))
         (is (= :exponential-backoff (:strategy (config/retry-config*))))
         (is (= true (:enabled (config/cleanup-config*)))))))))

(deftest dynamic-var-not-loaded-test
  (testing "Dynamic var functions throw when config not loaded"
    (is (thrown-with-msg? Exception
                          #"Configuration not loaded"
                          (config/get-config)))))

(deftest reload-config-with-atom-test
  (testing "reload-config! with atom updates the atom"
    (let [cfg-atom (atom (config/load-config
                          "dev/skivi/config/test_resources/minimal-config.edn"))
          original-conn-str (get-in @cfg-atom [:database :connection-string])]
      (is (= "postgresql://localhost:5432/minimal_db" original-conn-str))
      ;; Reload with different config
      (config/reload-config!
       cfg-atom
       "dev/skivi/config/test_resources/valid-standalone.edn")
      (is (= "postgresql://localhost:5432/test_db"
             (get-in @cfg-atom [:database :connection-string]))))))

(deftest reload-config-with-system-map-test
  (testing "reload-config! with system map updates :config atom"
    (let [cfg-atom (atom (config/load-config
                          "dev/skivi/config/test_resources/minimal-config.edn"))
          system   {:config cfg-atom}]
      (config/reload-config!
       system
       "dev/skivi/config/test_resources/valid-standalone.edn")
      (is (= "postgresql://localhost:5432/test_db"
             (get-in @cfg-atom [:database :connection-string]))))))

(deftest reload-config-without-system-test
  (testing "reload-config! without system just returns new config"
    (let [new-cfg (config/reload-config!
                   nil
                   "dev/skivi/config/test_resources/valid-standalone.edn")]
      (is (map? new-cfg))
      (is (= "postgresql://localhost:5432/test_db"
             (get-in new-cfg [:database :connection-string]))))))

;; =============================================================================
;; Aero Features Tests
;; =============================================================================

(deftest aero-env-reader-test
  (testing "Aero #env reader uses default when env var not set"
    ;; Without env var, should use default from #or
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/aero-features.edn")]
      (is (= "postgresql://localhost:5432/default_db"
             (get-in cfg [:database :connection-string]))))))

(deftest aero-or-reader-with-default-test
  (testing "Aero #or reader uses default when env var not set"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/aero-features.edn")]
      (is (= "postgresql://localhost:5432/default_db"
             (get-in cfg [:database :connection-string]))))))

(deftest aero-long-reader-test
  (testing "Aero #long reader with #or default"
    ;; Without env var, should use default and convert to long
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/aero-features.edn")]
      (is (= 10 (get-in cfg [:database :pool-config :maximum-pool-size])))
      (is (instance? Long
                     (get-in cfg
                             [:database :pool-config :maximum-pool-size]))))))

(deftest aero-boolean-reader-test
  (testing "Aero #boolean reader with #or default"
    ;; Without env var, should use default and convert to boolean
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/aero-features.edn")]
      (is (= true (get-in cfg [:queue :local-queue :enabled])))
      (is (instance? Boolean (get-in cfg [:queue :local-queue :enabled]))))))

;; =============================================================================
;; Profile-Based Configuration Tests
;; =============================================================================

(deftest profile-dev-config-test
  (testing "Loading config with :dev profile"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/profile-config.edn"
               {:profile :dev})]
      (is (= "postgresql://localhost:5432/dev_db"
             (get-in cfg [:database :connection-string])))
      (is (= 5 (get-in cfg [:database :pool-config :maximum-pool-size])))
      (is (= :debug (get-in cfg [:logging :level])))
      (is (= :pretty (get-in cfg [:logging :format]))))))

(deftest profile-test-config-test
  (testing "Loading config with :test profile"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/profile-config.edn"
               {:profile :test})]
      (is (= "postgresql://localhost:5432/test_db"
             (get-in cfg [:database :connection-string])))
      (is (= 3 (get-in cfg [:database :pool-config :maximum-pool-size])))
      (is (= :warn (get-in cfg [:logging :level])))
      (is (= :json (get-in cfg [:logging :format])))
      (is (= false (get-in cfg [:logging :include-mdc]))))))

(deftest profile-prod-config-test
  (testing "Loading config with :prod profile"
    (let [cfg (config/load-config
               "dev/skivi/config/test_resources/profile-config.edn"
               {:profile :prod})]
      (is (= "postgresql://localhost:5432/prod_db"
             (get-in cfg [:database :connection-string])))
      (is (= 20 (get-in cfg [:database :pool-config :maximum-pool-size])))
      (is (= :info (get-in cfg [:logging :level])))
      (is (= :json (get-in cfg [:logging :format]))))))

;; =============================================================================
;; Multiple Instance Isolation Tests
;; =============================================================================

(deftest multiple-instances-isolation-test
  (testing "Multiple config instances are independent"
    (let [cfg1 (config/load-config
                "dev/skivi/config/test_resources/minimal-config.edn")
          cfg2 (config/load-config
                "dev/skivi/config/test_resources/valid-standalone.edn")]
      ;; Configs should be different
      (is (not= cfg1 cfg2))
      (is (= "postgresql://localhost:5432/minimal_db"
             (get-in cfg1 [:database :connection-string])))
      (is (= "postgresql://localhost:5432/test_db"
             (get-in cfg2 [:database :connection-string])))
      ;; Modifying one should not affect the other
      (let [modified-cfg1 (assoc-in cfg1 [:worker :concurrency] 100)]
        (is (= 100 (get-in modified-cfg1 [:worker :concurrency])))
        (is (= 2 (get-in cfg1 [:worker :concurrency])))
        (is (= 4 (get-in cfg2 [:worker :concurrency])))))))

(deftest multiple-instances-dynamic-var-isolation-test
  (testing "Dynamic var binding is thread-local and isolated"
    (let [cfg1    (config/load-config
                   "dev/skivi/config/test_resources/minimal-config.edn")
          cfg2    (config/load-config
                   "dev/skivi/config/test_resources/valid-standalone.edn")
          result1 (atom nil)
          result2 (atom nil)
          ;; Run two concurrent operations with different configs
          f1      (future (config/with-config cfg1
                                              (fn []
                                                (Thread/sleep 50)
                                                (reset! result1
                                                  (config/schema-name*)))))
          f2      (future (config/with-config cfg2
                                              (fn []
                                                (Thread/sleep 50)
                                                (reset! result2
                                                  (config/schema-name*)))))]
      @f1
      @f2
      ;; Each thread should have seen its own config
      (is (= "minimal_schema" @result1))
      (is (= "job_system" @result2)))))

(deftest explicit-api-supports-multiple-instances-test
  (testing "Explicit API naturally supports multiple instances"
    (let [cfg1 (config/load-config
                "dev/skivi/config/test_resources/minimal-config.edn")
          cfg2 (config/load-config
                "dev/skivi/config/test_resources/valid-standalone.edn")]
      ;; Can use both configs simultaneously without conflict
      (is (= "minimal_schema" (config/schema-name cfg1)))
      (is (= "job_system" (config/schema-name cfg2)))
      (is (= 2 (:concurrency (config/worker-config cfg1))))
      (is (= 4 (:concurrency (config/worker-config cfg2)))))))
