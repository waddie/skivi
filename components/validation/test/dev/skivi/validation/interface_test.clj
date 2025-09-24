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

(ns dev.skivi.validation.interface-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.validation.interface :as validation]
            [malli.instrument :as mi]))

(use-fixtures :once (fn [f] (mi/instrument!) (f) (mi/unstrument!)))

;; Spec definitions used by SpecValidator tests
(s/def ::to string?)
(s/def ::subject string?)
(s/def ::send-email (s/keys :req-un [::to ::subject]))
(s/def ::count pos-int?)
(s/def ::batch-job (s/keys :req-un [::count]))

;; =============================================================================
;; MalliValidator tests
;; =============================================================================

(def email-schemas
  {:resize-image [:map [:url :string] [:width :int] [:height :int]]
   :send-email   [:map [:to :string] [:subject :string]]})

(deftest malli-validator-valid-payload-test
  (testing "Valid payload passes through unchanged"
    (let [v       (validation/malli-validator email-schemas)
          payload {:subject "Hello"
                   :to      "user@example.com"}]
      (is (= payload (validation/validate-payload v :send-email payload))))))

(deftest malli-validator-invalid-payload-test
  (testing "Invalid payload throws ex-info"
    (let [v (validation/malli-validator email-schemas)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid payload"
           (validation/validate-payload v :send-email {:to 42}))))))

(deftest malli-validator-error-contains-task-id-test
  (testing "Error includes task-identifier and errors"
    (let [v (validation/malli-validator email-schemas)]
      (try (validation/validate-payload v :send-email {:to 42})
           (is false "Expected exception")
           (catch clojure.lang.ExceptionInfo e
             (is (= :send-email (:task-identifier (ex-data e))))
             (is (some? (:errors (ex-data e))))
             (is (= {:to 42} (:payload (ex-data e)))))))))

(deftest malli-validator-unknown-task-passes-through-test
  (testing "Unknown task identifier passes payload through without validation"
    (let [v       (validation/malli-validator email-schemas)
          payload "not-a-map"]
      (is (= payload (validation/validate-payload v :unknown-task payload))))))

(deftest malli-validator-string-task-identifier-test
  (testing "String task-identifier is coerced to keyword for lookup"
    (let [v       (validation/malli-validator email-schemas)
          payload {:subject "Hi"
                   :to      "x@y.com"}]
      (is (= payload (validation/validate-payload v "send-email" payload))))))

;; =============================================================================
;; SpecValidator tests
;; =============================================================================

(def spec-schemas
  {:batch-job  ::batch-job
   :send-email ::send-email})

(deftest spec-validator-valid-payload-test
  (testing "Valid payload passes through unchanged"
    (let [v       (validation/spec-validator spec-schemas)
          payload {:subject "Hello"
                   :to      "user@example.com"}]
      (is (= payload (validation/validate-payload v :send-email payload))))))

(deftest spec-validator-invalid-payload-test
  (testing "Invalid payload throws ex-info"
    (let [v (validation/spec-validator spec-schemas)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid payload"
           (validation/validate-payload v :send-email {:to 42}))))))

(deftest spec-validator-error-contains-explain-data-test
  (testing "Error includes spec explain-data"
    (let [v (validation/spec-validator spec-schemas)]
      (try (validation/validate-payload v :send-email {:to 42})
           (is false "Expected exception")
           (catch clojure.lang.ExceptionInfo e
             (is (= :send-email (:task-identifier (ex-data e))))
             (is (some? (:errors (ex-data e)))))))))

(deftest spec-validator-unknown-task-passes-through-test
  (testing "Unknown task identifier passes payload through without validation"
    (let [v (validation/spec-validator spec-schemas)]
      (is (= "anything"
             (validation/validate-payload v :no-such-task "anything"))))))

;; =============================================================================
;; CompositeValidator tests
;; =============================================================================

(deftest composite-validator-all-pass-test
  (testing "Payload passing all validators is returned unchanged"
    (let [v1      (validation/malli-validator {:my-task [:map [:x :int]]})
          v2      (validation/malli-validator {:my-task [:map
                                                         [:x [:int {:min 0}]]]})
          v       (validation/composite-validator [v1 v2])
          payload {:x 5}]
      (is (= payload (validation/validate-payload v :my-task payload))))))

(deftest composite-validator-first-fails-test
  (testing "Fails on first validator that rejects the payload"
    (let [v1 (validation/malli-validator {:my-task [:map [:x :int]]})
          v2 (validation/malli-validator {:my-task [:map [:x :string]]})
          v  (validation/composite-validator [v1 v2])]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid payload"
           (validation/validate-payload v :my-task {:x "not-an-int"}))))))

(deftest composite-validator-second-fails-test
  (testing "Fails when second validator rejects the payload"
    (let [v1 (validation/malli-validator {:my-task [:map [:x :int]]})
          v2 (validation/malli-validator {:my-task [:map
                                                    [:x [:int {:min 10}]]]})
          v  (validation/composite-validator [v1 v2])]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Invalid payload"
                            (validation/validate-payload v :my-task {:x 1}))))))

(deftest composite-validator-unknown-task-passes-through-test
  (testing "Unknown task passes through all validators"
    (let [v1      (validation/malli-validator {:my-task [:map [:x :int]]})
          v2      (validation/spec-validator {:my-task ::batch-job})
          v       (validation/composite-validator [v1 v2])
          payload "any value"]
      (is (= payload (validation/validate-payload v :other-task payload))))))

;; =============================================================================
;; NoOpValidator tests
;; =============================================================================

(deftest noop-validator-always-passes-test
  (testing "No-op validator accepts any payload for any task"
    (let [v (validation/noop-validator)]
      (is (= {:x 1} (validation/validate-payload v :any-task {:x 1})))
      (is (= "string" (validation/validate-payload v :task "string")))
      (is (nil? (validation/validate-payload v :task nil))))))

;; =============================================================================
;; valid-payload? tests
;; =============================================================================

(deftest valid-payload-predicate-true-test
  (testing "Returns true for valid payloads"
    (let [v (validation/malli-validator {:task [:map [:id :int]]})]
      (is (true? (validation/valid-payload? v :task {:id 1})))
      (is (true? (validation/valid-payload? v :unknown {:id "anything"}))))))

(deftest valid-payload-predicate-false-test
  (testing "Returns false for invalid payloads"
    (let [v (validation/malli-validator {:task [:map [:id :int]]})]
      (is (false? (validation/valid-payload? v :task {:id "not-an-int"}))))))

(deftest valid-payload-noop-always-true-test
  (testing "No-op validator always returns true"
    (let [v (validation/noop-validator)]
      (is (true? (validation/valid-payload? v :task "garbage"))))))

;; =============================================================================
;; explain-payload tests
;; =============================================================================

(deftest explain-payload-nil-when-valid-test
  (testing "Returns nil for valid payloads"
    (let [v (validation/malli-validator {:task [:map [:id :int]]})]
      (is (nil? (validation/explain-payload v :task {:id 1}))))))

(deftest explain-payload-errors-when-invalid-test
  (testing "Returns humanised errors for invalid payloads"
    (let [v      (validation/malli-validator {:task [:map [:id :int]]})
          errors (validation/explain-payload v :task {:id "not-an-int"})]
      (is (some? errors))
      (is (map? errors)))))

(deftest explain-payload-nil-for-unknown-task-test
  (testing "Returns nil for unknown task identifiers"
    (let [v (validation/malli-validator {:task [:map [:id :int]]})]
      (is (nil? (validation/explain-payload v :unknown-task "anything"))))))

(deftest explain-payload-noop-always-nil-test
  (testing "No-op validator always returns nil"
    (let [v (validation/noop-validator)]
      (is (nil? (validation/explain-payload v :task "garbage"))))))

;; =============================================================================
;; create-validator tests
;; =============================================================================

(def minimal-config
  {:database {:connection-string "postgresql://localhost/test"
              :pool-config       {:connection-timeout 30000
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
   :schema   {:job-schemas {:my-task [:map [:id :int]]}
              :name        "test_schema"
              :validate-payloads true}
   :worker   {:concurrency            4
              :file-extensions        [".clj"]
              :graceful-shutdown-timeout 30000
              :max-job-execution-time 300000
              :poll-interval          2000
              :task-directory         "tasks"}})

(deftest create-validator-with-schemas-test
  (testing "Creates malli validator when schemas are configured"
    (let [v (validation/create-validator minimal-config)]
      (is (= {:id 1} (validation/validate-payload v :my-task {:id 1})))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid payload"
           (validation/validate-payload v :my-task {:id "bad"}))))))

(deftest create-validator-validate-payloads-false-test
  (testing "Returns no-op validator when :validate-payloads is false"
    (let [config (assoc-in minimal-config [:schema :validate-payloads] false)
          v      (validation/create-validator config)]
      (is (= {:id "bad"}
             (validation/validate-payload v :my-task {:id "bad"}))))))

(deftest create-validator-empty-schemas-test
  (testing "Returns no-op validator when no schemas are defined"
    (let [config (assoc-in minimal-config [:schema :job-schemas] {})
          v      (validation/create-validator config)]
      (is (= "anything" (validation/validate-payload v :my-task "anything"))))))

(deftest create-validator-unknown-task-passes-through-test
  (testing "Unknown task identifier passes through even with schemas configured"
    (let [v (validation/create-validator minimal-config)]
      (is (= "untyped"
             (validation/validate-payload v :unregistered-task "untyped"))))))

(deftest create-validator-with-spec-schemas-test
  (testing
    "Creates spec validator when all job-schemas values are spec keywords"
    (let [config (assoc-in minimal-config
                  [:schema :job-schemas]
                  {:send-email ::send-email})
          v      (validation/create-validator config)]
      (is (= {:subject "Hi"
              :to      "a@b.com"}
             (validation/validate-payload v
                                          :send-email
                                          {:subject "Hi"
                                           :to      "a@b.com"})))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid payload"
           (validation/validate-payload v :send-email {:to 42}))))))

(deftest create-validator-with-mixed-schemas-test
  (testing "Mixed malli and spec schemas each validate their own task"
    (let [config (assoc-in minimal-config
                  [:schema :job-schemas]
                  {:my-task    [:map [:id :int]]
                   :send-email ::send-email})
          v      (validation/create-validator config)]
      (is (= {:id 1} (validation/validate-payload v :my-task {:id 1})))
      (is (= {:subject "Hi"
              :to      "a@b.com"}
             (validation/validate-payload v
                                          :send-email
                                          {:subject "Hi"
                                           :to      "a@b.com"})))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid payload"
           (validation/validate-payload v :my-task {:id "bad"})))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid payload"
           (validation/validate-payload v :send-email {:to 42}))))))
