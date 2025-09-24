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

(ns dev.skivi.monitoring.interface-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [dev.skivi.monitoring.interface :as monitoring]
            [malli.instrument :as mi]))

(use-fixtures :once (fn [f] (mi/instrument!) (f) (mi/unstrument!)))

;;;; ── create-emitter ────────────────────────────────────────────────────────

(deftest create-emitter-defaults-test
  (testing "create-emitter with no args returns an enabled emitter"
    (let [em (monitoring/create-emitter)]
      (is (monitoring/enabled? em))
      (is (empty? (monitoring/events em)))
      (is (= {:dropped 0
              :emitted 0}
             (monitoring/stats em)))))
  (testing "create-emitter with explicit config respects :enabled flag"
    (let [em (monitoring/create-emitter {:buffer-size 100
                                         :enabled     true})]
      (is (monitoring/enabled? em)))
    (let [em (monitoring/create-emitter {:buffer-size 100
                                         :enabled     false})]
      (is (not (monitoring/enabled? em))))))

;;;; ── noop-emitter ──────────────────────────────────────────────────────────

(deftest noop-emitter-test
  (testing "noop-emitter is not enabled"
    (let [em (monitoring/noop-emitter)]
      (is (not (monitoring/enabled? em)))))
  (testing "emit! on noop-emitter has no effect"
    (let [em     (monitoring/noop-emitter)
          called (atom false)]
      (monitoring/on em :job/claimed (fn [_] (reset! called true)))
      (monitoring/emit! em :job/claimed {:job-id "x"})
      (is (not @called) "handler not invoked on disabled emitter")
      (is (empty? (monitoring/events em)) "no events buffered")
      (is (= {:dropped 0
              :emitted 0}
             (monitoring/stats em))
          "stats unchanged"))))

;;;; ── collecting-emitter ────────────────────────────────────────────────────

(deftest collecting-emitter-buffers-all-test
  (testing "collecting-emitter buffers every event without dropping"
    (let [em (monitoring/collecting-emitter)]
      (monitoring/emit! em :job/claimed {:job-id "a"})
      (monitoring/emit! em :job/completed {:job-id "b"})
      (monitoring/emit! em :job/failed {:job-id "c"})
      (is (= 3 (count (monitoring/events em)))
          "all emitted events are buffered")
      (is (= [:job/claimed :job/completed :job/failed]
             (mapv :type (monitoring/events em)))
          "events are in emission order"))))

;;;; ── emit! and on ──────────────────────────────────────────────────────────

(deftest emit-delivers-to-specific-handler-test
  (testing "handler registered for event-type receives matching events"
    (let [em       (monitoring/create-emitter)
          received (atom [])]
      (monitoring/on em :job/claimed (fn [evt] (swap! received conj evt)))
      (monitoring/emit! em :job/claimed {:job-id "x"})
      (monitoring/emit! em :job/completed {:job-id "y"})
      (is (= 1 (count @received)) "only one event delivered")
      (is (= :job/claimed (:type (first @received))))))
  (testing "handler registered for :all receives every event"
    (let [em       (monitoring/create-emitter)
          received (atom [])]
      (monitoring/on em :all (fn [evt] (swap! received conj evt)))
      (monitoring/emit! em :job/claimed {:job-id "a"})
      (monitoring/emit! em :job/failed {:job-id "b"})
      (is (= 2 (count @received)) ":all handler receives both events")))
  (testing "multiple handlers for same event-type all receive the event"
    (let [em (monitoring/create-emitter)
          c1 (atom 0)
          c2 (atom 0)]
      (monitoring/on em :job/claimed (fn [_] (swap! c1 inc)))
      (monitoring/on em :job/claimed (fn [_] (swap! c2 inc)))
      (monitoring/emit! em :job/claimed {:job-id "x"})
      (is (= 1 @c1) "first handler invoked")
      (is (= 1 @c2) "second handler invoked"))))

;;;; ── off ───────────────────────────────────────────────────────────────────

(deftest off-removes-handler-test
  (testing "off deregisters handler so it no longer receives events"
    (let [em       (monitoring/create-emitter)
          received (atom [])
          hid      (monitoring/on em
                                  :job/claimed
                                  (fn [evt] (swap! received conj evt)))]
      (monitoring/emit! em :job/claimed {:job-id "first"})
      (monitoring/off em hid)
      (monitoring/emit! em :job/claimed {:job-id "second"})
      (is (= 1 (count @received)) "handler not called after off")))
  (testing "off with unknown handler-id is a no-op"
    (let [em (monitoring/create-emitter)]
      (is (nil? (monitoring/off em (random-uuid)))
          "returns nil without throwing"))))

;;;; ── events / ring buffer ──────────────────────────────────────────────────

(deftest ring-buffer-bounded-test
  (testing "buffer drops oldest events when at capacity"
    (let [em (monitoring/create-emitter {:buffer-size 3
                                         :enabled     true})]
      (monitoring/emit! em :a {:n 1})
      (monitoring/emit! em :b {:n 2})
      (monitoring/emit! em :c {:n 3})
      (monitoring/emit! em :d {:n 4})
      (let [evts (monitoring/events em)]
        (is (= 3 (count evts)) "at most buffer-size events retained")
        (is (= [:b :c :d] (mapv :type evts))
            "oldest event dropped when at capacity")))))

(deftest events-includes-required-keys-test
  (testing "each event has :type, :data, and :emitted-at"
    (let [em (monitoring/create-emitter)]
      (monitoring/emit! em
                        :job/claimed
                        {:job-id    "x"
                         :worker-id "w"})
      (let [evt (first (monitoring/events em))]
        (is (= :job/claimed (:type evt)))
        (is (= {:job-id    "x"
                :worker-id "w"}
               (:data evt)))
        (is (instance? java.time.Instant (:emitted-at evt)))))))

;;;; ── stats ─────────────────────────────────────────────────────────────────

(deftest stats-counts-emitted-test
  (testing ":emitted increments with each emit!"
    (let [em (monitoring/create-emitter)]
      (monitoring/emit! em :job/claimed {})
      (monitoring/emit! em :job/completed {})
      (is (= 2 (:emitted (monitoring/stats em))))))
  (testing ":dropped increments when a handler throws"
    (let [em (monitoring/create-emitter)]
      (monitoring/on em
                     :job/claimed
                     (fn [_] (throw (ex-info "handler error" {}))))
      (monitoring/emit! em :job/claimed {})
      (is (= 1 (:dropped (monitoring/stats em)))
          "thrown handler counted as dropped")
      (is (= 1 (:emitted (monitoring/stats em)))
          "event still counted as emitted"))))

;;;; ── on returns nil when disabled ─────────────────────────────────────────

(deftest on-returns-nil-when-disabled-test
  (testing "on returns nil when emitter is disabled"
    (let [em (monitoring/noop-emitter)]
      (is (nil? (monitoring/on em :job/claimed (fn [_])))
          "returns nil from disabled emitter")))
  (testing "on returns a UUID handler-id when emitter is enabled"
    (let [em  (monitoring/create-emitter)
          hid (monitoring/on em :job/claimed (fn [_]))]
      (is (uuid? hid) "returns a UUID handler-id"))))

;;;; ── surface - all declared functions are accessible ──────────────────────

(deftest surface-functions-accessible-test
  (testing "all interface functions are callable"
    (is (ifn? monitoring/create-emitter))
    (is (ifn? monitoring/noop-emitter))
    (is (ifn? monitoring/collecting-emitter))
    (is (ifn? monitoring/enabled?))
    (is (ifn? monitoring/on))
    (is (ifn? monitoring/off))
    (is (ifn? monitoring/emit!))
    (is (ifn? monitoring/events))
    (is (ifn? monitoring/stats))))
