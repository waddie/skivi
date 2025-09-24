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

;; Spec compliance tests derived from docs/spec/skivi.allium.
;;
;; Scope note: the allium spec explicitly excludes "monitoring and metrics
;; wiring (implementation)" from its coverage. These tests therefore do not
;; map to spec rule obligations. Instead they verify the behavioural
;; contracts that the monitoring component must satisfy in order for spec-
;; compliant components (worker-pool, queue, job-manager) to correctly emit
;; the events described in the spec's @guidance annotations. Specifically:
;;
;;   • Event types that correspond 1-to-1 with spec rules can be emitted and
;;     received (the emitter is a correct routing layer).
;;   • The config defaults for the monitoring subsystem match the values
;;     declared in the spec's config block (:events :enabled true,
;;     :events :buffer-size 1000).
;;   • The noop-emitter satisfies the "monitoring disabled" path required by
;;     config.monitoring.events.enabled = false.
;;   • The collecting-emitter enables spec-level integration tests in other
;;     components to assert that correct events are emitted during rule
;;     execution.
;;
;; These tests are pure (no database required).

(ns dev.skivi.monitoring.spec-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.monitoring.interface :as monitoring]
            [malli.instrument :as mi]))

(use-fixtures :once (fn [f] (mi/instrument!) (f) (mi/unstrument!)))

;;;; ── Config defaults ───────────────────────────────────────────────────────
;;
;; The monitoring component's default emitter config should align with the
;; defaults declared in the full system config (components/config).
;; Values come from config.default-config's :monitoring :events section:
;;   :enabled true  :buffer-size 1000

(deftest default-emitter-enabled-test
  ;; [config-default.monitoring.events.enabled] default = true
  (testing "create-emitter with no args produces an enabled emitter"
    (let [em (monitoring/create-emitter)]
      (is (monitoring/enabled? em)
          "events enabled by default, matching spec config default"))))

(deftest default-buffer-size-test
  ;; [config-default.monitoring.events.buffer_size] default = 1000
  (testing "default emitter retains up to 1000 events before dropping"
    (let [em (monitoring/create-emitter)]
      (dotimes [n 1001]
        (monitoring/emit! em :probe {:n n}))
      (let [evts (monitoring/events em)]
        (is (= 1000 (count evts)) "buffer capped at 1000 matching spec default")
        (is (= 1 (get-in (first evts) [:data :n]))
            "oldest retained event is index 1 (index 0 dropped)")))))

;;;; ── Disabled emitter (monitoring.events.enabled = false) ─────────────────

(deftest noop-emitter-satisfies-disabled-config-test
  ;; When config.monitoring.events.enabled = false the emitter must be a
  ;; complete no-op: no events stored, no handlers invoked, stats
  ;; unchanged.
  (testing "noop-emitter honours the disabled monitoring path"
    (let [em     (monitoring/noop-emitter)
          called (atom false)]
      (monitoring/on em :all (fn [_] (reset! called true)))
      (monitoring/emit! em :job/claimed {:job-id "x"})
      (is (not @called) "no handler called on disabled emitter")
      (is (empty? (monitoring/events em)) "no events stored")
      (is (= {:dropped 0
              :emitted 0}
             (monitoring/stats em))
          "stats not incremented")))
  (testing "disabled emitter created via config map"
    (let [em (monitoring/create-emitter {:buffer-size 100
                                         :enabled     false})]
      (is (not (monitoring/enabled? em)))
      (monitoring/emit! em :job/claimed {})
      (is (empty? (monitoring/events em))))))

;;;; ── Spec-defined event types ──────────────────────────────────────────────
;;
;; The allium spec's @guidance annotations describe which events worker-pool,
;; queue, and job-manager emit at each spec rule boundary. These tests verify
;; that the emitter correctly routes each of those event types to registered
;; handlers. The event type keywords match the standard set documented in
;; the interface namespace.

(deftest job-lifecycle-event-routing-test
  ;; Covers events emitted by spec rules:
  ;;   WorkerClaimsJob  -> :job/claimed. WorkerCompletesJob ->
  ;;   :job/completed. WorkerFailsJob   -> :job/failed. WorkerExhaustsJob
  ;;   -> :job/exhausted. WorkerReportsPartialSuccess ->
  ;;   :job/partial-success
  (testing "job lifecycle events are correctly routed to handlers"
    (let [em       (monitoring/collecting-emitter)
          received (atom [])]
      (monitoring/on em :all (fn [evt] (swap! received conj (:type evt))))
      (monitoring/emit! em
                        :job/claimed
                        {:job-id    "j1"
                         :worker-id "w1"})
      (monitoring/emit! em
                        :job/completed
                        {:job-id    "j1"
                         :worker-id "w1"})
      (monitoring/emit! em
                        :job/failed
                        {:job-id    "j2"
                         :worker-id "w2"})
      (monitoring/emit! em
                        :job/exhausted
                        {:job-id    "j3"
                         :worker-id "w3"})
      (monitoring/emit! em
                        :job/partial-success
                        {:job-id    "j4"
                         :worker-id "w4"})
      (is (= [:job/claimed :job/completed :job/failed
              :job/exhausted :job/partial-success]
             @received)
          "all job lifecycle event types routed in order"))))

(deftest queue-lifecycle-event-routing-test
  ;; Covers events emitted by spec rules:
  ;;   LockJobQueue / CreateAndLockJobQueue -> :queue/locked
  ;;   UnlockJobQueue -> :queue/unlocked
  (testing "queue lifecycle events are correctly routed to handlers"
    (let [em       (monitoring/collecting-emitter)
          received (atom [])]
      (monitoring/on em :queue/locked (fn [_] (swap! received conj :locked)))
      (monitoring/on em
                     :queue/unlocked
                     (fn [_] (swap! received conj :unlocked)))
      (monitoring/emit! em
                        :queue/locked
                        {:queue-name "q1"
                         :worker-id  "w1"})
      (monitoring/emit! em :queue/unlocked {:queue-name "q1"})
      (is (= [:locked :unlocked] @received)
          "queue lock/unlock events routed to specific handlers"))))

(deftest cron-event-routing-test
  ;; Covers CronFiresJob -> :cron/fired
  (testing ":cron/fired event is routed to handler"
    (let [em       (monitoring/collecting-emitter)
          received (atom nil)]
      (monitoring/on em :cron/fired (fn [evt] (reset! received (:data evt))))
      (monitoring/emit! em :cron/fired {:cron-id "daily-report"})
      (is (= {:cron-id "daily-report"} @received)
          "cron fired event delivers data to handler"))))

(deftest worker-error-event-routing-test
  ;; Covers :worker/error emitted when an unhandled exception occurs
  (testing ":worker/error event is routed to handler"
    (let [em       (monitoring/collecting-emitter)
          received (atom nil)]
      (monitoring/on em :worker/error (fn [evt] (reset! received (:data evt))))
      (monitoring/emit! em
                        :worker/error
                        {:error     "oops"
                         :worker-id "w1"})
      (is (= {:error     "oops"
              :worker-id "w1"}
             @received)
          "worker error event delivers data correctly"))))

;;;; ── collecting-emitter as integration test helper ─────────────────────────

(deftest collecting-emitter-as-test-spy-test
  ;; collecting-emitter is the mechanism by which spec integration tests in
  ;; other components can assert that spec-defined rules emit the correct
  ;; events.
  (testing "collecting-emitter captures events emitted during a simulated rule"
    (let [em (monitoring/collecting-emitter)]
      (letfn [(simulate-worker-claims-job [em job-id worker-id]
                (monitoring/emit! em
                                  :job/claimed
                                  {:attempts  1
                                   :job-id    job-id
                                   :worker-id worker-id}))]
        (simulate-worker-claims-job em "j1" "w1")
        (simulate-worker-claims-job em "j2" "w1")
        (let [evts (monitoring/events em)]
          (is (= 2 (count evts)) "both claim events captured")
          (is (every? #(= :job/claimed (:type %)) evts)
              "all events have correct type")
          (is (= 1 (get-in (first evts) [:data :attempts]))
              "attempts field preserved in event data"))))))

;;;; ── Handler isolation
;;;; ──────────────────────────────────────────────────────

(deftest handler-error-does-not-prevent-other-handlers-test
  ;; Spec compliance depends on other components being able to observe
  ;; events even when one handler (e.g. a metrics handler) throws.
  (testing "a throwing handler does not prevent other handlers from running"
    (let [em      (monitoring/create-emitter)
          reached (atom false)]
      (monitoring/on em :job/claimed (fn [_] (throw (ex-info "bad" {}))))
      (monitoring/on em :job/claimed (fn [_] (reset! reached true)))
      (monitoring/emit! em :job/claimed {:job-id "x"})
      (is @reached "second handler runs even when first handler threw")
      (is (= 1 (:dropped (monitoring/stats em))) "error counted as dropped"))))

;;;; ── Event timestamp precision ─────────────────────────────────────────────

(deftest emitted-at-is-instant-test
  ;; Events carry :emitted-at as a java.time.Instant, enabling correlation
  ;; with spec-defined timestamps (started_at, completed_at, etc.).
  (testing ":emitted-at is a java.time.Instant on every event"
    (let [em (monitoring/collecting-emitter)]
      (monitoring/emit! em :job/claimed {:job-id "x"})
      (monitoring/emit! em :job/completed {:job-id "x"})
      (is (every? #(instance? java.time.Instant (:emitted-at %))
                  (monitoring/events em))
          "all buffered events have java.time.Instant :emitted-at"))))
