;; Copyright (c) 2025-2026 Tom Waddington
;;
;; This software is dual-licensed:
;;
;; 1. GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later)
;;    This program is free software: you can redistribute it and/or modify
;;    it under the terms of the GNU Affero General Public License as published
;;    by the Free Software Foundation, either version 3 of the License, or
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

(ns dev.skivi.skivi.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [dev.skivi.database.interface :as database]
            [dev.skivi.job-history.interface :as job-history]
            [dev.skivi.skivi.core :as core]
            [dev.skivi.worker-pool.interface :as worker-pool]))

(def ^:private stub-config
  {:cleanup    {:enabled  true
                :retention-periods {:failed-jobs      "30 days"
                                    :task-identifiers "7 days"}
                :schedule "0 3 * * *"
                :tasks    [:gc-task-identifiers :gc-job-queues]}
   :cron       {:default-backfill-period 3600000
                :enabled  false
                :timezone "UTC"}
   :database   {:connection-string "jdbc:postgresql://localhost:5432/test"
                :pool-size         2
                :schema-name       "skivi"}
   :logging    {:format      :json
                :include-mdc true
                :level       :info}
   :monitoring {:events       {:buffer-size 10
                               :enabled     false}
                :health-check {:enabled true
                               :port    3000}
                :metrics      {:enabled  false
                               :registry nil}}
   :queue      {:local-queue {:size 10
                              :ttl  30000}}
   :retry      {:base-delay   1000
                :jitter       0.1
                :max-attempts 25
                :max-delay    3600000
                :multiplier   2.0}
   :schema     {:name "skivi"}
   :worker     {:concurrency      2
                :poll-interval-ms 1000}})

(deftest create-system-returns-expected-keys
  (testing "all required keys are present"
    (with-redefs [database/create-pool     (constantly ::mock-pool)
                  job-history/create-store (fn [_ _] ::mock-history)]
      (let [sys (core/create-system stub-config)]
        (is (map? sys))
        (is (= stub-config (:config sys)))
        (is (= ::mock-pool (:pool sys)))
        (is (= ::mock-history (:history sys)))
        (is (contains? sys :emitter))
        (is (contains? sys :validator))
        (is (contains? sys :job-system))
        (is (contains? sys :worker-pool)))))
  (testing "job-system contains pool and validator"
    (with-redefs [database/create-pool     (constantly ::mock-pool)
                  job-history/create-store (fn [_ _] ::mock-history)]
      (let [{:keys [job-system pool]} (core/create-system stub-config)]
        (is (= pool (:pool job-system)))
        (is (contains? job-system :validator))))))

(deftest create-system-without-crontabs-has-no-scheduler
  (with-redefs [database/create-pool     (constantly ::mock-pool)
                job-history/create-store (fn [_ _] ::mock-history)]
    (let [sys (core/create-system stub-config)]
      (is (not (contains? sys :scheduler))))))

(deftest create-system-with-crontabs-includes-scheduler
  (with-redefs [database/create-pool     (constantly ::mock-pool)
                job-history/create-store (fn [_ _] ::mock-history)]
    (let [crontabs [{:identifier "test-job"
                     :schedule   "0 * * * *"}]
          sys      (core/create-system stub-config {} crontabs)]
      (is (contains? sys :scheduler)))))

(deftest stop-closes-pool
  (let [closed? (atom false)]
    (with-redefs [database/create-pool     (constantly ::mock-pool)
                  database/close-pool      (fn [_] (reset! closed? true) nil)
                  job-history/create-store (fn [_ _] ::mock-history)
                  worker-pool/stop!        (fn ([p] p) ([p _] p))]
      (let [sys (core/create-system stub-config)]
        (core/stop! sys)
        (is @closed?)))))

;;;; ── load-task-registry
;;;; ─────────────────────────────────────────────────────

(defn- make-temp-dir
  []
  (let [d (java.io.File. (System/getProperty "java.io.tmpdir")
                         (str "skivi-test-" (random-uuid)))]
    (.mkdirs d)
    d))

(defn- write-file
  [^java.io.File dir name content]
  (let [f (io/file dir name)]
    (spit f content)
    f))

(deftest load-task-registry-returns-empty-map-for-empty-dir
  (let [dir (make-temp-dir)]
    (try (is (= {} (core/load-task-registry (.getAbsolutePath dir))))
         (finally (doseq [f (.listFiles dir)]
                    (.delete f))
                  (.delete dir)))))

(deftest load-task-registry-loads-single-file
  (let [dir (make-temp-dir)]
    (try (write-file dir "tasks.clj" "{\"send-email\" (fn [_] nil)}")
         (let [reg (core/load-task-registry (.getAbsolutePath dir))]
           (is (contains? reg "send-email"))
           (is (ifn? (get reg "send-email"))))
         (finally (doseq [f (.listFiles dir)]
                    (.delete f))
                  (.delete dir)))))

(deftest load-task-registry-merges-multiple-files-alphabetically
  (let [dir (make-temp-dir)]
    (try (write-file dir "b-tasks.clj" "{\"task-b\" (fn [_] :b)}")
         (write-file dir "a-tasks.clj" "{\"task-a\" (fn [_] :a)}")
         (let [reg (core/load-task-registry (.getAbsolutePath dir))]
           (is (contains? reg "task-a"))
           (is (contains? reg "task-b")))
         (finally (doseq [f (.listFiles dir)]
                    (.delete f))
                  (.delete dir)))))

(deftest load-task-registry-later-file-wins-on-conflict
  (let [dir (make-temp-dir)]
    (try (write-file dir "a-tasks.clj" "{\"same-task\" (fn [_] :from-a)}")
         (write-file dir "b-tasks.clj" "{\"same-task\" (fn [_] :from-b)}")
         (let [reg (core/load-task-registry (.getAbsolutePath dir))]
           (is (= :from-b ((get reg "same-task") nil))))
         (finally (doseq [f (.listFiles dir)]
                    (.delete f))
                  (.delete dir)))))

(deftest load-task-registry-respects-file-extensions
  (let [dir (make-temp-dir)]
    (try (write-file dir "tasks.clj" "{\"clj-task\"  (fn [_] nil)}")
         (write-file dir "tasks.cljc" "{\"cljc-task\" (fn [_] nil)}")
         (let [reg (core/load-task-registry (.getAbsolutePath dir) [".cljc"])]
           (is (contains? reg "cljc-task"))
           (is (not (contains? reg "clj-task"))))
         (finally (doseq [f (.listFiles dir)]
                    (.delete f))
                  (.delete dir)))))

(deftest load-task-registry-throws-for-missing-directory
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"does not exist or is not a directory"
                        (core/load-task-registry "/no/such/directory/ever"))))

(deftest load-task-registry-throws-when-file-returns-non-map
  (let [dir (make-temp-dir)]
    (try (write-file dir "bad.clj" "42")
         (is (thrown-with-msg? clojure.lang.ExceptionInfo
                               #"Task file must return a map"
                               (core/load-task-registry (.getAbsolutePath
                                                         dir))))
         (finally (doseq [f (.listFiles dir)]
                    (.delete f))
                  (.delete dir)))))
